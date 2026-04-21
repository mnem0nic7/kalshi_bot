from __future__ import annotations

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
