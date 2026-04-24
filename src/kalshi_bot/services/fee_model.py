"""Kalshi fee-model helpers.

Current scope: standard taker fee for single-side trades.

Known hardcoded fee call sites to migrate opportunistically when touched:
- ``src/kalshi_bot/services/monotonicity_scanner.py``: arb profit calculation
- ``src/kalshi_bot/services/counterfactuals.py``: Strategy C shadow fill math

Do not sweep live trading paths as part of the decision-corpus PR. Migrate a
call site only when the surrounding subsystem is already under review.
"""
from __future__ import annotations

from decimal import Decimal

KALSHI_TAKER_FEE_V1 = "kalshi_taker_fee_v1"


def current_fee_model_version() -> str:
    return KALSHI_TAKER_FEE_V1


def estimate_kalshi_taker_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal,
) -> Decimal:
    """Compute Kalshi taker fee in dollars for a single-side trade.

    Formula: ``fee_rate * count * price * (1 - price)``.

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
    return rate * contracts * price * (Decimal("1") - price)
