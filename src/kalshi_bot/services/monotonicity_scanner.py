"""Monotonicity Arb Scanner — Addition 3, §4.3.

For any station/day, P(high > T) must be non-increasing in T. When the orderbook
violates that (higher threshold YES bid > lower threshold YES ask), a risk-free arb
exists: buy YES T_i + buy NO T_j. Payoff is ≥ $1 in all scenarios; cost is < $1.

Scanner runs every N seconds over open KXHIGH* markets, groups by (station, event_date),
and emits MonotonicityArbProposal objects for each detected violation.

Atomicity contract (P1-2)
-------------------------
The "risk-free" claim depends on BOTH legs of the pair filling at the quoted
prices. Kalshi does not provide native atomic multi-leg orders, so a live
implementation must:

1. Place leg 1 (buy YES on the lower threshold).
2. Wait for fill confirmation — on failure/timeout, cancel and abort.
3. Place leg 2 (buy NO on the higher threshold) only after leg 1 filled.
4. If leg 2 fails to fill within a short timeout, **immediately unwind leg 1
   by selling back to the resting bid**. Document and account for unwind
   slippage and timing risk.
5. Persist the leg-pair as a single logical unit (both leg order_ids + the
   terminal state: both_filled / leg1_unwound / stuck_partial).

Until that executor exists, ``evaluate_arb_risk`` returns ``"risk_blocked"``
for any configuration that would attempt live execution. Scanner remains free
to run in shadow mode, which never calls ExecutionService; the operator sees
proposals in the control room and can monitor detection quality without
exposure.

Flag sequence to eventually enable live:
- ``monotonicity_arb_enabled = True``
- ``monotonicity_arb_shadow_only = False``
- ``monotonicity_arb_atomic_execution_ready = True``  (acknowledges the above)

Even with all three set, today the gate still returns ``"risk_blocked"``
because the atomic executor is a separate follow-up.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fee calculation (§4.3.3)
# ---------------------------------------------------------------------------

def kalshi_fee_cents(price_dollars: float, contracts: int = 1) -> float:
    """Compute Kalshi taker fee: ceil(0.07 * C * P * (1-P) * 100) / 100 * 100.

    Returns fee in cents per the given number of contracts.
    Delegates to the shared fee model so arb detection stays aligned with the
    directional risk gate.
    """
    price = Decimal(str(max(0.0, min(1.0, price_dollars))))
    fee_dollars = estimate_kalshi_taker_fee_dollars(
        price_dollars=price,
        count=Decimal(str(contracts)),
        fee_rate=Decimal("0.07"),
    )
    return float(fee_dollars * Decimal("100"))


def _safe_side_fee_cents_per_leg() -> float:
    """Conservative 2¢/leg floor for rapid detection pass. See §4.3.3."""
    return 2.0


# ---------------------------------------------------------------------------
# Market grouping helpers
# ---------------------------------------------------------------------------

def _parse_station_date_threshold(ticker: str) -> tuple[str, date, float] | None:
    """Extract (station, event_date, threshold_f) from KXHIGH<STATION>-<YYMONDD>-T<N> tickers.

    Returns None if the ticker does not match the expected format.
    """
    try:
        # e.g. KXHIGHTBOS-26APR22-T58  →  parts = ['KXHIGHTBOS', '26APR22', 'T58']
        parts = ticker.split("-")
        if len(parts) < 3:
            return None
        prefix = parts[0]  # e.g. KXHIGHTBOS
        if not prefix.startswith("KXHIGH"):
            return None
        station = prefix[len("KXHIGH"):]  # e.g. TBOS
        date_str = parts[1]  # e.g. 26APR22
        event_date = datetime.strptime(date_str, "%y%b%d").date()
        threshold_str = parts[2]  # e.g. T58
        if not threshold_str.startswith("T"):
            return None
        threshold_f = float(threshold_str[1:])
        return station, event_date, threshold_f
    except (ValueError, IndexError):
        return None


def group_markets_by_station_date(
    markets: list[dict[str, Any]],
) -> dict[tuple[str, date], list[dict[str, Any]]]:
    """Group open KXHIGH* market snapshots by (station, event_date).

    Each entry in the returned dict is sorted by threshold_f ascending.
    Markets without a parseable ticker are silently skipped.
    """
    groups: dict[tuple[str, date], list[tuple[float, dict[str, Any]]]] = {}
    for market in markets:
        ticker = market.get("ticker", "")
        parsed = _parse_station_date_threshold(ticker)
        if parsed is None:
            continue
        station, event_date, threshold_f = parsed
        key = (station, event_date)
        groups.setdefault(key, []).append((threshold_f, market))

    return {
        key: [m for _, m in sorted(entries, key=lambda x: x[0])]
        for key, entries in groups.items()
    }


# ---------------------------------------------------------------------------
# Violation detection
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class MonotonicityViolation:
    """A detected monotonicity violation between two thresholds."""
    station: str
    event_date: date
    ticker_low: str
    ticker_high: str
    threshold_low_f: float
    threshold_high_f: float
    # Prices in dollars at detection time
    ask_yes_low: Decimal   # price to buy YES on lower threshold
    bid_yes_high: Decimal  # detection-only: bid_yes on higher threshold (cheaper NO proxy)
    ask_no_high: Decimal   # re-validated NO ask at proposal time
    gross_edge_cents: float
    fee_estimate_cents: float
    net_edge_cents: float


def detect_violations(
    group: list[dict[str, Any]],
    *,
    station: str,
    event_date: date,
    min_net_edge_cents: float,
) -> list[MonotonicityViolation]:
    """Walk sorted threshold sequence, find all pairs where bid_yes(T_j) > ask_yes(T_i).

    Two-step detection (§4.3.3):
    1. Cheap pass: bid_yes(T_j) - ask_yes(T_i) > 2*fee_floor + min_edge_cents
    2. Re-validate with actual ask_no(T_j) to get real net edge

    Returns only pairs that still pass after re-validation.
    """
    violations: list[MonotonicityViolation] = []
    fee_floor = _safe_side_fee_cents_per_leg() * 2  # 4¢ total conservative estimate

    for i, market_low in enumerate(group):
        ask_low = _price_cents_or_none(market_low.get("yes_ask_dollars"))
        if ask_low is None:
            continue
        ticker_low = market_low.get("ticker", "")
        parsed_low = _parse_station_date_threshold(ticker_low)
        if parsed_low is None:
            continue
        threshold_low_f = parsed_low[2]

        for market_high in group[i + 1:]:
            ticker_high = market_high.get("ticker", "")
            parsed_high = _parse_station_date_threshold(ticker_high)
            if parsed_high is None:
                continue
            threshold_high_f = parsed_high[2]

            bid_high = _price_cents_or_none(market_high.get("yes_bid_dollars"))
            if bid_high is None:
                continue

            # Step 1: cheap detection using bid_yes as NO-side proxy
            raw_edge = bid_high - ask_low  # in cents
            if raw_edge <= fee_floor + min_net_edge_cents:
                continue

            # Step 2: re-validate with actual ask_no_high
            ask_no_high_raw = market_high.get("no_ask_dollars")
            if ask_no_high_raw is None:
                # Fall back to complement only if actual no_ask is unavailable
                ask_no_high_cents = 100.0 - float(market_high.get("yes_bid_dollars", 0)) * 100.0
            else:
                ask_no_high_cents = float(ask_no_high_raw) * 100.0

            ask_yes_low_cents = ask_low
            total_cost_cents = ask_yes_low_cents + ask_no_high_cents

            # Gross edge: guaranteed payout is 100¢; cost is total_cost_cents
            gross_edge_cents = 100.0 - total_cost_cents

            # Actual fees: 2 legs at their respective prices
            fee_low_cents = kalshi_fee_cents(ask_yes_low_cents / 100.0)
            fee_high_cents = kalshi_fee_cents(ask_no_high_cents / 100.0)
            fee_total_cents = fee_low_cents + fee_high_cents

            net_edge_cents = gross_edge_cents - fee_total_cents

            if net_edge_cents <= min_net_edge_cents:
                continue

            violations.append(MonotonicityViolation(
                station=station,
                event_date=event_date,
                ticker_low=ticker_low,
                ticker_high=ticker_high,
                threshold_low_f=threshold_low_f,
                threshold_high_f=threshold_high_f,
                ask_yes_low=Decimal(str(round(ask_yes_low_cents / 100, 4))),
                bid_yes_high=Decimal(str(round(bid_high / 100, 4))),
                ask_no_high=Decimal(str(round(ask_no_high_cents / 100, 4))),
                gross_edge_cents=gross_edge_cents,
                fee_estimate_cents=fee_total_cents,
                net_edge_cents=net_edge_cents,
            ))

    return violations


def _price_cents_or_none(raw: Any) -> float | None:
    """Convert a dollars field (str or float) to cents, returning None if missing/zero."""
    if raw is None:
        return None
    try:
        cents = float(raw) * 100.0
        return cents if cents > 0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Proposal dataclass
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ArbProposal:
    """A fully evaluated arb proposal ready for persistence and optional execution."""
    station: str
    event_date: date
    ticker_low: str
    ticker_high: str
    threshold_low_f: float
    threshold_high_f: float
    ask_yes_low_cents: float
    ask_no_high_cents: float
    total_cost_cents: float
    gross_edge_cents: float
    fee_estimate_cents: float
    net_edge_cents: float
    contracts_proposed: int
    execution_outcome: str
    suppression_reason: str | None
    detected_at: datetime


# ---------------------------------------------------------------------------
# Risk gate for monotonicity arb
# ---------------------------------------------------------------------------

def evaluate_arb_risk(
    violation: MonotonicityViolation,
    *,
    control: DeploymentControl,
    settings: Settings,
) -> tuple[str, str | None]:
    """Return (execution_outcome, suppression_reason) for a monotonicity arb violation.

    Applies: kill switch, enabled flag, shadow-only flag, notional cap.
    Returns one of ``"shadow"``, ``"risk_blocked"`` with an optional reason.

    Live execution is not yet available — see the module docstring for the
    atomicity contract. When ``monotonicity_arb_shadow_only=False`` the gate
    returns ``"risk_blocked"`` instead of silently downgrading to shadow, so
    an operator flipping the flag without a two-leg executor sees an explicit
    refusal rather than a misleading green light.
    """
    if control.kill_switch_enabled:
        return "risk_blocked", "Global kill switch is enabled."

    if not settings.monotonicity_arb_enabled:
        return "risk_blocked", "monotonicity_arb_enabled=False."

    total_dollars = violation.ask_yes_low + violation.ask_no_high
    max_notional = Decimal(str(settings.monotonicity_arb_max_notional_dollars))
    if total_dollars > max_notional:
        return "risk_blocked", (
            f"Pair cost {float(total_dollars):.2f} exceeds "
            f"monotonicity_arb_max_notional_dollars={settings.monotonicity_arb_max_notional_dollars}."
        )

    if settings.monotonicity_arb_shadow_only:
        return "shadow", None

    # Live execution demands an atomic two-leg executor with rollback on
    # leg-2 failure. Until that lands, block rather than silently downgrade.
    if not settings.monotonicity_arb_atomic_execution_ready:
        return "risk_blocked", (
            "Live monotonicity arb requires monotonicity_arb_atomic_execution_ready=True "
            "and a two-leg executor that unwinds leg 1 on leg-2 failure. "
            "Flip shadow_only back on or implement the atomic executor first."
        )

    # Even with the readiness flag set, the atomic executor is not yet built.
    # Refuse live execution so a misconfigured environment cannot place naked
    # legs. Remove this gate only in the same commit that ships the executor.
    return "risk_blocked", (
        "monotonicity_arb_atomic_execution_ready=True but atomic executor is not yet "
        "implemented. See services/monotonicity_scanner.py docstring."
    )


def size_proposal(
    violation: MonotonicityViolation,
    *,
    settings: Settings,
) -> int:
    """Compute number of contract pairs to propose, capped by max_notional."""
    pair_cost_dollars = float(violation.ask_yes_low + violation.ask_no_high)
    if pair_cost_dollars <= 0:
        return 0
    return max(1, int(settings.monotonicity_arb_max_notional_dollars / pair_cost_dollars))


# ---------------------------------------------------------------------------
# Scan entry point (pure function — no I/O)
# ---------------------------------------------------------------------------

def scan_for_violations(
    markets: list[dict[str, Any]],
    *,
    control: DeploymentControl,
    settings: Settings,
    reference_time: datetime | None = None,
) -> list[ArbProposal]:
    """Run the full monotonicity arb scan over a list of market snapshots.

    Returns one ArbProposal per detected violation (suppressed or actionable).
    Pure function: no DB writes, no network calls.
    """
    now = reference_time or datetime.now(UTC)
    proposals: list[ArbProposal] = []

    groups = group_markets_by_station_date(markets)
    for (station, event_date), group in groups.items():
        if len(group) < 2:
            continue
        violations = detect_violations(
            group,
            station=station,
            event_date=event_date,
            min_net_edge_cents=settings.monotonicity_arb_min_net_edge_cents,
        )
        for violation in violations:
            outcome, reason = evaluate_arb_risk(
                violation, control=control, settings=settings
            )
            contracts = size_proposal(violation, settings=settings) if outcome == "shadow" else 0
            proposals.append(ArbProposal(
                station=violation.station,
                event_date=violation.event_date,
                ticker_low=violation.ticker_low,
                ticker_high=violation.ticker_high,
                threshold_low_f=violation.threshold_low_f,
                threshold_high_f=violation.threshold_high_f,
                ask_yes_low_cents=float(violation.ask_yes_low) * 100,
                ask_no_high_cents=float(violation.ask_no_high) * 100,
                total_cost_cents=float(violation.ask_yes_low + violation.ask_no_high) * 100,
                gross_edge_cents=violation.gross_edge_cents,
                fee_estimate_cents=violation.fee_estimate_cents,
                net_edge_cents=violation.net_edge_cents,
                contracts_proposed=contracts,
                execution_outcome=outcome,
                suppression_reason=reason,
                detected_at=now,
            ))

    return proposals
