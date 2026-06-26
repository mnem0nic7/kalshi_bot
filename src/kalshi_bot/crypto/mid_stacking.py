"""Mid-anchored stacking for crypto binary probabilities.

Our trained crypto models predict from spot features and never see the market mid, so their
Brier is worse than the mid on every asset. Stacking fits a logistic of the label on
``[logit(model_prob), logit(mid_prob)]`` on holdout data: the optimizer can always recover
"just the mid" (model coefficient -> 0), so the stacked forecast is ~never worse than the mid,
and it up-weights the model only where it carries genuine incremental signal (e.g. once the
cross-venue basis feature matures). With no fitted stack we fall back to the mid — the better
forecast — which guarantees never-worse-than-mid behavior by default.

``apply_mid_stack`` is pure (no sklearn) so it is safe on the live decision path; ``fit_mid_stack``
imports sklearn lazily (training only).
"""

from __future__ import annotations

import math

_EPS = 1e-6


def _clamp01(p: float) -> float:
    return max(0.0, min(1.0, float(p)))


def _logit(p: float) -> float:
    p = min(1.0 - _EPS, max(_EPS, float(p)))
    return math.log(p / (1.0 - p))


def apply_mid_stack(model_prob: float, mid_prob: float, stack: dict | None) -> float:
    """Blend the model probability with the mid via the fitted stack.

    Falls back to the mid (not the raw model) when there is no usable stack, so the deployed
    forecast is never worse than the mid by default.
    """

    mid = _clamp01(mid_prob)
    if not stack or stack.get("method") != "mid_stack":
        return mid
    try:
        c0 = float(stack.get("c0") or 0.0)
        c_model = float(stack.get("c_model") or 0.0)
        c_mid = float(stack.get("c_mid") or 0.0)
        z = c0 + c_model * _logit(model_prob) + c_mid * _logit(mid)
        z = max(-35.0, min(35.0, z))
        return _clamp01(1.0 / (1.0 + math.exp(-z)))
    except (TypeError, ValueError, OverflowError):
        return mid


def fit_mid_stack(
    model_probs: list[float],
    mid_probs: list[float],
    labels: list[int],
    *,
    min_samples: int = 200,
) -> dict | None:
    """Fit the 2-feature logistic stack [logit(model), logit(mid)] -> label.

    Returns the coefficient dict, or ``None`` when there is too little data or a single class
    (caller then falls back to the mid).
    """

    rows = [
        (float(mp), float(md), int(y))
        for mp, md, y in zip(model_probs, mid_probs, labels, strict=False)
        if mp is not None and md is not None and y is not None
    ]
    if len(rows) < int(min_samples) or len({y for _, _, y in rows}) < 2:
        return None

    import numpy as np
    from sklearn.linear_model import LogisticRegression

    x = np.asarray([[_logit(mp), _logit(md)] for mp, md, _ in rows], dtype=float)
    y = np.asarray([yy for _, _, yy in rows], dtype=int)
    # Light L2 keeps coefficients sane on small/correlated samples; the mid feature anchors it.
    model = LogisticRegression(max_iter=1000, solver="lbfgs", C=1.0)
    model.fit(x, y)
    return {
        "method": "mid_stack",
        "c0": float(model.intercept_[0]),
        "c_model": float(model.coef_[0][0]),
        "c_mid": float(model.coef_[0][1]),
        "n": len(rows),
    }
