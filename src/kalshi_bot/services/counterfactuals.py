from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


@dataclass(frozen=True)
class CounterfactualTradeOutcome:
    settlement_value_dollars: Decimal
    pnl_dollars: Decimal
    settlement_result: str


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, InvalidOperation, ValueError):
        return None


def _settlement_value(settlement: Mapping[str, Any] | None) -> Decimal | None:
    if settlement is None:
        return None
    direct_value = _decimal_or_none(settlement.get("settlement_value_dollars"))
    if direct_value is not None:
        return direct_value
    result = str(settlement.get("kalshi_result") or settlement.get("result") or "").strip().lower()
    if result in {"yes", "win"}:
        return Decimal("1")
    if result in {"no", "loss"}:
        return Decimal("0")
    return None


def score_counterfactual_trade(
    *,
    trade_ticket: Mapping[str, Any] | None,
    settlement: Mapping[str, Any] | None,
) -> CounterfactualTradeOutcome | None:
    if trade_ticket is None or settlement is None:
        return None
    settlement_value = _settlement_value(settlement)
    side = str(trade_ticket.get("side") or "").strip().lower()
    yes_price = _decimal_or_none(trade_ticket.get("yes_price_dollars"))
    count_fp = _decimal_or_none(trade_ticket.get("count_fp"))
    if settlement_value is None or yes_price is None or count_fp is None or side not in {"yes", "no"}:
        return None
    pnl = (settlement_value - yes_price) * count_fp if side == "yes" else (yes_price - settlement_value) * count_fp
    settled_yes = settlement_value >= Decimal("0.5")
    settlement_result = "win" if (side == "yes" and settled_yes) or (side == "no" and not settled_yes) else "loss"
    return CounterfactualTradeOutcome(
        settlement_value_dollars=settlement_value.quantize(Decimal("0.0001")),
        pnl_dollars=pnl.quantize(Decimal("0.0001")),
        settlement_result=settlement_result,
    )


# ---------------------------------------------------------------------------
# Strategy C discount sensitivity helpers (P1-3)
# ---------------------------------------------------------------------------


def _is_locked_yes(resolution_state: str) -> bool:
    return str(resolution_state).lower().endswith("yes")


def strategy_c_target_cents(
    *, resolution_state: str, discount_cents: float
) -> float:
    """Entry-price target (in cents, 0..100) for a Strategy C signal.

    LOCKED_YES → 100 - discount (bid on YES just below settlement $1.00).
    LOCKED_NO  → discount      (bid on NO just above settlement $0.00, priced on
                                 the YES-axis as the complement).

    ``discount_cents`` may be fractional (e.g. 0.5).
    """
    if _is_locked_yes(resolution_state):
        return 100.0 - discount_cents
    return discount_cents


def strategy_c_gross_edge_cents(
    *, resolution_state: str, discount_cents: float
) -> float:
    """Gross edge per filled contract assuming fill at target, in cents.

    Settlement pays $1 to the correct side. For LOCKED_YES buying YES at
    (100 - d) cents the gross edge is d cents. For LOCKED_NO buying NO at
    d cents the gross edge is (100 - d) cents — larger per-contract edge but
    historically a much tighter market near $0.00 so fill rate collapses.
    """
    target = strategy_c_target_cents(
        resolution_state=resolution_state, discount_cents=discount_cents
    )
    # Settlement payoff is 100 cents; entry cost is the target, so gross = 100 - target.
    return 100.0 - target


def strategy_c_fee_cents(entry_price_dollars: float) -> float:
    """Kalshi taker fee per contract at ``entry_price_dollars`` (0..1).

    Formula (§ fee schedule): ceil(0.07 * price * (1-price) * 100) cents.
    Matches kalshi_fee_cents in services/monotonicity_scanner.py and is copied
    here rather than imported to keep the counterfactual module dependency-free.
    """
    price = max(0.0, min(1.0, entry_price_dollars))
    raw_dollars = 0.07 * price * (1.0 - price) * 100
    fee_dollars = math.ceil(raw_dollars) / 100.0
    return fee_dollars * 100.0  # cents


def strategy_c_net_ev_per_fill_cents(
    *, resolution_state: str, discount_cents: float
) -> float:
    """Net expected value per filled contract: gross edge minus entry fee."""
    gross = strategy_c_gross_edge_cents(
        resolution_state=resolution_state, discount_cents=discount_cents
    )
    target = strategy_c_target_cents(
        resolution_state=resolution_state, discount_cents=discount_cents
    )
    fee = strategy_c_fee_cents(target / 100.0)
    return gross - fee
