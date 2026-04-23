"""Edge-scaled position sizing (P2-2).

Implements fractional-Kelly sizing for Strategy A based on signal edge and
confidence. Gated behind ``Settings.risk_edge_scaled_sizing_enabled``; off by
default until empirical calibration (PR #7's ``/api/strategies/calibration``)
confirms the fair-value signal is well-calibrated.

**Design rule:** the Kelly notional is always capped by the existing flat-
percentage caps. Edge-scaled sizing can only ever *reduce* risk vs. the flat
default — never increase it. That preserves the portfolio-level risk envelope
the operator already tunes via ``risk_order_pct``.
"""
from __future__ import annotations

from decimal import Decimal


def kelly_fraction_yes_side(
    *, fair_yes: Decimal, target_price: Decimal
) -> Decimal:
    """Full Kelly fraction for a YES-side bet.

    Derivation: payoff per dollar staked is (1 - P) / P where P is the entry
    price; loss per dollar is 1 (you lose your stake). With true probability p
    that YES wins, the classical Kelly fraction is::

        f* = (p*(1-P)/P - (1-p)*1) / ((1-P)/P)
           = (p - P) / (1 - P)

    Returns 0 when there's no edge (fair_yes ≤ target_price) or when the entry
    price leaves no room (target_price ≥ 1).
    """
    if target_price >= Decimal("1.00") or fair_yes <= target_price:
        return Decimal("0")
    return (fair_yes - target_price) / (Decimal("1") - target_price)


def kelly_fraction_no_side(
    *, fair_yes: Decimal, target_price: Decimal
) -> Decimal:
    """Full Kelly fraction for a NO-side bet.

    NO contract costs (1 - target_price); wins $1 if the market resolves NO
    (probability 1 - p). Equivalently, let p' = 1 - p and P' = 1 - target_price
    and reuse the YES-side formula::

        f* = (p' - P') / (1 - P') = (target_price - fair_yes) / target_price

    Returns 0 when there's no edge (fair_yes ≥ target_price) or when the
    target price is non-positive.
    """
    if target_price <= Decimal("0") or fair_yes >= target_price:
        return Decimal("0")
    return (target_price - fair_yes) / target_price


def _clamp_unit(value: float) -> float:
    """Clamp to [0, 1]."""
    return max(0.0, min(1.0, value))


def _clamp_non_negative(value: float) -> float:
    return max(0.0, value)


def edge_scaled_notional_dollars(
    *,
    total_capital_dollars: Decimal,
    fair_yes: Decimal,
    target_price: Decimal,
    side: str,
    confidence: float,
    kelly_multiplier: float,
) -> Decimal:
    """Fractional-Kelly notional in dollars.

    ``confidence`` ∈ [0, 1] and ``kelly_multiplier`` (≥ 0) are multiplicative
    shrinkers on top of the full Kelly fraction. Typical operating points:
    ``kelly_multiplier = 0.25`` (quarter-Kelly) with the signal's confidence
    as an additional adjustment so low-confidence signals size down.

    Caller is responsible for capping the result against the flat-percentage
    order cap — this function returns the Kelly-suggested notional regardless
    of portfolio limits.
    """
    side_lower = (side or "").strip().lower()
    if side_lower == "yes":
        f_star = kelly_fraction_yes_side(fair_yes=fair_yes, target_price=target_price)
    elif side_lower == "no":
        f_star = kelly_fraction_no_side(fair_yes=fair_yes, target_price=target_price)
    else:
        return Decimal("0")
    if f_star <= Decimal("0") or total_capital_dollars <= Decimal("0"):
        return Decimal("0")
    shrink = Decimal(str(_clamp_unit(confidence))) * Decimal(
        str(_clamp_non_negative(kelly_multiplier))
    )
    if shrink <= Decimal("0"):
        return Decimal("0")
    return (f_star * shrink * total_capital_dollars).quantize(Decimal("0.01"))
