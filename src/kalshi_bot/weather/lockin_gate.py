"""Lock-in fee/edge gate for the weather lock-in MVP.

A pure decision predicate: given a deterministically-locked weather contract
(``WeatherResolutionState.LOCKED_YES`` / ``LOCKED_NO``) and the current order-book
ask for each side, decide whether the cheap winning side still offers positive
fee-adjusted edge. Trades nothing — callers feed the verdict to the order path.

Lock-in economics: when the contract is locked, the winning side pays out $1 at
settlement with certainty. Buying it at ``entry`` costs ``entry`` and returns $1, so
the per-contract cushion is ``(1 - entry)``. The gate requires that cushion to clear
both a minimum-edge floor and the Kalshi taker fee. Because a locked contract's
winning side is priced near $1, ``price*(1-price)`` is small → fees are least punishing
exactly here, which is the structural reason the lock-in can clear fees where mid-day
forecasts cannot. See docs/research/2026-06-22-weather-lockin-mvp-spec.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.services.fee_model import (
    KALSHI_DEFAULT_TAKER_FEE_RATE,
    estimate_kalshi_taker_fee_dollars,
)

DEFAULT_MIN_LOCKIN_EDGE_DOLLARS = Decimal("0.02")


@dataclass(frozen=True, slots=True)
class LockInGateResult:
    should_trade: bool
    reason: str
    side: str | None = None
    entry_price_dollars: Decimal | None = None
    gross_edge_dollars: Decimal | None = None
    fee_dollars: Decimal | None = None
    net_edge_dollars: Decimal | None = None


def evaluate_lockin_fee_edge_gate(
    *,
    resolution_state: WeatherResolutionState,
    yes_ask_dollars: Decimal | None,
    no_ask_dollars: Decimal | None,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal = KALSHI_DEFAULT_TAKER_FEE_RATE,
    min_edge_dollars: Decimal = DEFAULT_MIN_LOCKIN_EDGE_DOLLARS,
) -> LockInGateResult:
    """Decide whether to take the cheap winning side of a locked weather contract."""
    if resolution_state == WeatherResolutionState.LOCKED_YES:
        side, entry = "yes", yes_ask_dollars
    elif resolution_state == WeatherResolutionState.LOCKED_NO:
        side, entry = "no", no_ask_dollars
    else:
        return LockInGateResult(should_trade=False, reason="not_locked")

    if entry is None:
        return LockInGateResult(should_trade=False, reason="no_quote", side=side)

    entry = Decimal(str(entry))
    if entry <= Decimal("0") or entry > Decimal("1"):
        return LockInGateResult(
            should_trade=False, reason="invalid_price", side=side, entry_price_dollars=entry
        )

    contracts = Decimal(str(count))
    edge_per_contract = Decimal("1") - entry
    gross_edge = edge_per_contract * contracts
    fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry, count=contracts, fee_rate=fee_rate)
    net_edge = gross_edge - fee

    base = dict(
        side=side,
        entry_price_dollars=entry,
        gross_edge_dollars=gross_edge,
        fee_dollars=fee,
        net_edge_dollars=net_edge,
    )
    if edge_per_contract < min_edge_dollars:
        return LockInGateResult(should_trade=False, reason="below_min_edge", **base)
    if net_edge <= Decimal("0"):
        return LockInGateResult(should_trade=False, reason="fee_exceeds_edge", **base)
    return LockInGateResult(should_trade=True, reason="ok", **base)
