from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def probability_midband_required_edge_bps(
    *,
    fair_yes: Any,
    base_min_edge_bps: int,
    extremity_pct: float,
    max_extra_edge_bps: int,
) -> int | None:
    """Return the edge required by the midband probability guard, or None if outside/disabled."""
    fair = _decimal_or_none(fair_yes)
    if fair is None:
        return None
    extremity = Decimal(str(extremity_pct)) / Decimal("100")
    if extremity <= 0:
        return None

    lower = extremity
    upper = Decimal("1.0000") - extremity
    if not (lower < fair < upper):
        return None

    half_width = Decimal("0.5000") - lower
    if half_width <= 0:
        return None

    distance_from_midpoint = abs(fair - Decimal("0.5000"))
    midband_fraction = (half_width - distance_from_midpoint) / half_width
    midband_fraction = max(Decimal("0"), min(Decimal("1"), midband_fraction))
    extra_edge = Decimal(str(max_extra_edge_bps)) * midband_fraction
    required_edge = Decimal(str(base_min_edge_bps)) + extra_edge
    return int(required_edge.to_integral_value(rounding=ROUND_CEILING))


def probability_midband_block_reason(
    *,
    fair_yes: Any,
    edge_bps: int | None,
    base_min_edge_bps: int,
    extremity_pct: float,
    max_extra_edge_bps: int,
) -> str | None:
    required_edge = probability_midband_required_edge_bps(
        fair_yes=fair_yes,
        base_min_edge_bps=base_min_edge_bps,
        extremity_pct=extremity_pct,
        max_extra_edge_bps=max_extra_edge_bps,
    )
    if required_edge is None or edge_bps is None or edge_bps >= required_edge:
        return None
    fair = _decimal_or_none(fair_yes)
    fair_display = f"{fair:.2f}" if fair is not None else "unknown"
    return (
        f"Fair probability {fair_display} is in the noisy midband and requires "
        f"{required_edge}bps edge; actual edge is {edge_bps}bps."
    )
