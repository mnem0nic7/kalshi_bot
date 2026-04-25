from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

RateLike = Decimal | float | int | str

BPS_PER_RATE_UNIT = Decimal("10000")

# Schema-defined upper bounds used as the delta-cap reference when current value is zero.
# Ratios are bounded by the schema [0, 1]. Bps/seconds fields have no declared max, so
# these ceilings represent the outer edge of a sane auto-evolve configuration.
DEFAULT_THRESHOLD_FIELD_CEILINGS: dict[str, Decimal] = {
    "risk_min_edge_bps": Decimal("10000"),
    "risk_max_order_notional_dollars": Decimal("0"),
    "risk_max_position_notional_dollars": Decimal("0"),
    "trigger_max_spread_bps": Decimal("10000"),
    "trigger_cooldown_seconds": Decimal("86400"),
    "strategy_quality_edge_buffer_bps": Decimal("10000"),
    "strategy_min_remaining_payout_bps": Decimal("0"),
    "risk_safe_capital_reserve_ratio": Decimal("1"),
    "risk_risky_capital_max_ratio": Decimal("1"),
}

_MISSING = object()


def rate_to_bps(rate: RateLike | None) -> int | None:
    """Convert a fractional rate to basis points, rounded half-up."""
    if rate is None:
        return None
    parsed = _to_decimal(rate, label="rate")
    if parsed < 0 or parsed > 1:
        raise ValueError("rate must be between 0.0 and 1.0")
    return int((parsed * BPS_PER_RATE_UNIT).to_integral_value(rounding=ROUND_HALF_UP))


def delta_to_bps(
    candidate_rate: RateLike | None,
    reference_rate: RateLike | None | object = _MISSING,
) -> int | None:
    """Convert a rate delta, or candidate-minus-reference rates, to basis points."""
    if candidate_rate is None:
        return None
    if reference_rate is _MISSING:
        delta = _to_decimal(candidate_rate, label="delta")
        return int((delta * BPS_PER_RATE_UNIT).to_integral_value(rounding=ROUND_HALF_UP))
    if reference_rate is None:
        return None
    delta = _to_decimal(candidate_rate, label="candidate_rate") - _to_decimal(reference_rate, label="reference_rate")
    return int((delta * BPS_PER_RATE_UNIT).to_integral_value(rounding=ROUND_HALF_UP))


def validate_delta_cap(
    current_thresholds: Mapping[str, Any],
    proposed_thresholds: Mapping[str, Any],
    *,
    max_delta_pct: RateLike,
    field_ceilings: Mapping[str, RateLike] | None = None,
) -> list[dict[str, Any]]:
    """Return threshold changes that exceed the allowed relative delta cap."""
    cap = _to_decimal(max_delta_pct, label="max_delta_pct")
    if cap < 0 or cap > 1:
        raise ValueError("max_delta_pct must be between 0.0 and 1.0")

    ceilings = _normalize_field_ceilings(field_ceilings or DEFAULT_THRESHOLD_FIELD_CEILINGS)
    violations: list[dict[str, Any]] = []
    for field, ceiling in ceilings.items():
        if field not in current_thresholds or field not in proposed_thresholds:
            continue
        if current_thresholds[field] is None or proposed_thresholds[field] is None:
            continue
        current = _to_decimal(current_thresholds[field], label=f"{field}.current")
        proposed = _to_decimal(proposed_thresholds[field], label=f"{field}.proposed")
        if current == 0:
            if ceiling == 0:
                continue
            allowed_min = Decimal("0")
            allowed_max = ceiling * cap
        else:
            reference = abs(current)
            allowed_min = current - reference * cap
            allowed_max = current + reference * cap

        if proposed < allowed_min or proposed > allowed_max:
            violations.append({
                "field": field,
                "current": float(current),
                "proposed": float(proposed),
                "allowed_min": _rounded_float(allowed_min),
                "allowed_max": _rounded_float(allowed_max),
            })
    return violations


def delta_cap_error(
    current_thresholds: Mapping[str, Any],
    proposed_thresholds: Mapping[str, Any],
    *,
    max_delta_pct: RateLike,
    field_ceilings: Mapping[str, RateLike] | None = None,
    stage: str = "accept",
) -> dict[str, Any] | None:
    """Return the auto-evolve error payload for a delta-cap breach, or None."""
    violations = validate_delta_cap(
        current_thresholds,
        proposed_thresholds,
        max_delta_pct=max_delta_pct,
        field_ceilings=field_ceilings,
    )
    if not violations:
        return None
    return {
        "stage": stage,
        "reason": "delta_cap_exceeded",
        "cap_pct": float(_to_decimal(max_delta_pct, label="max_delta_pct")),
        "violations": violations,
    }


def _normalize_field_ceilings(field_ceilings: Mapping[str, RateLike]) -> dict[str, Decimal]:
    return {
        str(field): _to_decimal(value, label=f"{field}.ceiling")
        for field, value in field_ceilings.items()
    }


def _to_decimal(value: RateLike, *, label: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{label} must be finite")
    return parsed


def _rounded_float(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


__all__ = [
    "BPS_PER_RATE_UNIT",
    "DEFAULT_THRESHOLD_FIELD_CEILINGS",
    "delta_cap_error",
    "delta_to_bps",
    "rate_to_bps",
    "validate_delta_cap",
]
