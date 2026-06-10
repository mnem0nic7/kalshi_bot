"""Empirical edge-shrinkage calibration.

Live fills show the model's predicted entry edge (``decision_edge_bps``)
overstates realized P&L per contract by a large factor. This module fits a
single shrinkage coefficient ``beta`` (least squares through the origin) from
(predicted edge, realized P&L) pairs so decision-time gates can discount
predicted edges to an empirically supported level.
"""
from __future__ import annotations

import math

EDGE_SHRINKAGE_STATUS_OK = "ok"
EDGE_SHRINKAGE_STATUS_INSUFFICIENT_DATA = "insufficient_data"

DEFAULT_EDGE_SHRINKAGE_BETA_FLOOR = 0.2


def fit_edge_shrinkage(
    observations: list[tuple[float, float]],
    *,
    beta_floor: float = DEFAULT_EDGE_SHRINKAGE_BETA_FLOOR,
) -> dict:
    """Fit predicted-edge -> realized-P&L shrinkage through the origin.

    ``observations`` are ``(predicted_edge_dollars_per_contract,
    realized_pnl_dollars_per_contract)`` pairs. The fit is least squares
    through the origin: ``beta = sum(x*y) / sum(x*x)``, clamped to
    ``[beta_floor, 1.0]``. Empty or degenerate input (no finite pairs, or all
    predicted edges zero) returns ``beta = 1.0`` with status
    ``insufficient_data`` so callers fail open.
    """
    floor = min(max(0.0, float(beta_floor)), 1.0)
    clean: list[tuple[float, float]] = []
    for pair in observations or []:
        try:
            predicted, realized = float(pair[0]), float(pair[1])
        except (TypeError, ValueError, IndexError):
            continue
        if not (math.isfinite(predicted) and math.isfinite(realized)):
            continue
        clean.append((predicted, realized))
    sum_xx = sum(predicted * predicted for predicted, _ in clean)
    if not clean or sum_xx <= 0.0:
        return {
            "beta": 1.0,
            "raw_beta": None,
            "sample_count": len(clean),
            "status": EDGE_SHRINKAGE_STATUS_INSUFFICIENT_DATA,
        }
    sum_xy = sum(predicted * realized for predicted, realized in clean)
    raw_beta = sum_xy / sum_xx
    beta = min(1.0, max(floor, raw_beta))
    return {
        "beta": beta,
        "raw_beta": raw_beta,
        "sample_count": len(clean),
        "status": EDGE_SHRINKAGE_STATUS_OK,
    }
