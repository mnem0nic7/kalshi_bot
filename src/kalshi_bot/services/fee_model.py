"""Kalshi fee-model helpers.

Current scope: standard taker fee for single-side trades.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING

KALSHI_TAKER_FEE_V1 = "kalshi_taker_fee_v1"
KALSHI_TAKER_FEE_V2 = "kalshi_taker_fee_v2_cent_ceiling"


def current_fee_model_version() -> str:
    return KALSHI_TAKER_FEE_V2


def estimate_kalshi_taker_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal,
    round_up_to_cent: bool = True,
) -> Decimal:
    """Compute Kalshi taker fee in dollars for a single-side trade.

    Formula: ``round up(fee_rate * count * price * (1 - price))``.

    The formula is symmetric: ``fee(0.30) == fee(0.70)``, so callers may pass
    either the YES price or the NO price. ``price_dollars`` must be in dollars
    on the ``[0, 1]`` interval, not cents.
    """
    price = Decimal(str(price_dollars))
    contracts = Decimal(str(count))
    rate = Decimal(str(fee_rate))
    if price < Decimal("0") or price > Decimal("1"):
        raise ValueError("price_dollars must be between 0 and 1")
    if contracts < Decimal("0"):
        raise ValueError("count must be non-negative")
    raw_fee = rate * contracts * price * (Decimal("1") - price)
    if not round_up_to_cent or raw_fee <= Decimal("0"):
        return raw_fee
    return (raw_fee * Decimal("100")).to_integral_value(rounding=ROUND_CEILING) / Decimal("100")
