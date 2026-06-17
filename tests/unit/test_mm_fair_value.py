"""Analytic fair value on the MM tick schema (S, K, σ, τ) + σ estimator.

This is where the σ work lives — crypto-vol-eval showed the analytic edge stands
or falls on the volatility estimate. fair_up_normal is the digital-option
probability; realized_vol is the simple v1 σ̂ to be iterated (EWMA/HAR later).
"""
from __future__ import annotations

import math
from decimal import Decimal

from kalshi_bot.mm.fair_value import fair_up_normal, realized_vol


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def test_fair_up_half_at_the_money():
    f = fair_up_normal(spot=Decimal("100"), strike=Decimal("100"), sigma=Decimal("0.001"), seconds_to_close=300)
    assert abs(float(f) - 0.5) < 1e-9


def test_fair_up_in_the_money_above_half_and_sharpens_near_close():
    far = fair_up_normal(spot=Decimal("100.1"), strike=Decimal("100"), sigma=Decimal("0.001"), seconds_to_close=240)
    near = fair_up_normal(spot=Decimal("100.1"), strike=Decimal("100"), sigma=Decimal("0.001"), seconds_to_close=15)
    assert float(near) > float(far) > 0.5


def test_fair_up_none_on_bad_inputs():
    assert fair_up_normal(spot=Decimal("100"), strike=Decimal("0"), sigma=Decimal("0.001"), seconds_to_close=60) is None
    assert fair_up_normal(spot=Decimal("100"), strike=Decimal("100"), sigma=Decimal("0"), seconds_to_close=60) is None
    assert fair_up_normal(spot=Decimal("100"), strike=Decimal("100"), sigma=Decimal("0.001"), seconds_to_close=0) is None


def test_realized_vol_positive_for_varying_series_and_none_when_flat():
    prices = [Decimal("100"), Decimal("101"), Decimal("100"), Decimal("102"), Decimal("101")]
    v = realized_vol(prices)
    assert v is not None and v > 0
    assert realized_vol([Decimal("100")]) is None
