"""Mid-anchored convex blend for crypto binary probabilities.

Our crypto model's Brier is worse than the market mid out-of-sample (walk-forward), because it
predicts from spot features and never sees the mid. The blend corrects that:

    p_final = w * model_prob + (1 - w) * mid_prob,   w in [0, 1]

A single convex weight (not a free 2-feature logistic — that earlier design overfit the collinear
model/mid inputs to a negative mid coefficient and failed OOS). ``w`` is chosen on a chronological
fit split and then **gated on a held-out tail**: ``w>0`` is accepted only if the blend beats the
mid there, else ``w=0`` (pure mid). So "never worse than the mid" is enforced by the fit's own
holdout gate, and a convex combination can never blow up. Callers should pass OOS predictions
(walk-forward folds) so ``w`` reflects out-of-sample value, not in-sample overfit.

``apply_mid_stack`` is pure (live-path safe). Legacy ``method == "mid_stack"`` (the abandoned
logistic) is ignored -> falls back to the mid.
"""

from __future__ import annotations

_W_GRID = [i / 20.0 for i in range(21)]  # 0.00, 0.05, ... 1.00


def _clamp01(p: float) -> float:
    return max(0.0, min(1.0, float(p)))


def apply_mid_stack(model_prob: float, mid_prob: float, stack: dict | None) -> float:
    """Apply the convex blend; fall back to the mid when there is no usable blend."""

    mid = _clamp01(mid_prob)
    if not stack or stack.get("method") != "mid_blend":
        return mid  # no blend, or legacy/unknown method -> never worse than the mid
    try:
        w = max(0.0, min(1.0, float(stack.get("w") or 0.0)))
    except (TypeError, ValueError):
        return mid
    return _clamp01(w * _clamp01(model_prob) + (1.0 - w) * mid)


def _brier(probs: list[float], labels: list[int]) -> float:
    return sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(probs)


def fit_mid_stack(
    model_probs: list[float],
    mid_probs: list[float],
    labels: list[int],
    *,
    min_samples: int = 200,
    holdout_frac: float = 0.4,
) -> dict | None:
    """Fit the convex blend weight w in [0,1], gated on a held-out tail.

    Pick w minimizing Brier on the fit split, then accept it only if the blend beats the mid on the
    held-out tail; otherwise w=0 (pure mid). Returns ``None`` for too-few/single-class data.
    Pass predictions in chronological order (and ideally out-of-sample) so the holdout is a genuine
    forward check.
    """

    rows = [
        (_clamp01(mp), _clamp01(md), int(y))
        for mp, md, y in zip(model_probs, mid_probs, labels, strict=False)
        if mp is not None and md is not None and y is not None
    ]
    n = len(rows)
    if n < int(min_samples) or len({y for _, _, y in rows}) < 2:
        return None

    cut = int(round(n * (1.0 - holdout_frac)))
    cut = max(1, min(n - 1, cut))
    fit_rows, hold_rows = rows[:cut], rows[cut:]

    def _blend(rs: list[tuple[float, float, int]], w: float) -> list[float]:
        return [w * mp + (1.0 - w) * md for mp, md, _ in rs]

    fit_labels = [y for _, _, y in fit_rows]
    best_w, best_b = 0.0, _brier(_blend(fit_rows, 0.0), fit_labels)
    for w in _W_GRID:
        b = _brier(_blend(fit_rows, w), fit_labels)
        if b < best_b - 1e-12:
            best_w, best_b = w, b

    # Holdout gate: keep w>0 only if it beats the mid (w=0) on the unseen tail by a margin large
    # enough to exclude noise-level wins (a spurious tiny improvement must not earn weight).
    hold_labels = [y for _, _, y in hold_rows]
    mid_hold = _brier(_blend(hold_rows, 0.0), hold_labels)
    blend_hold = _brier(_blend(hold_rows, best_w), hold_labels)
    margin = max(1e-3, 0.01 * mid_hold)
    accepted_w = best_w if blend_hold <= mid_hold - margin else 0.0

    return {"method": "mid_blend", "w": float(accepted_w), "n": n}
