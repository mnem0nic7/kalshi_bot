"""CORP-style proper-score decomposition for probabilistic binary forecasts.

The mean Brier score decomposes as ``S = MCB - DSC + UNC`` (Dimitriadis, Gneiting & Jordan
2021, arXiv 2108.03210):

- ``UNC`` = base-rate variance ``p(1-p)`` — irreducible uncertainty of the event.
- ``MCB`` (miscalibration) = how much the score improves under optimal (PAV/isotonic)
  recalibration — the part of the error that better calibration would remove.
- ``DSC`` (discrimination) = how much the recalibrated forecast beats a constant base-rate
  forecast — the genuine predictive signal.

Both MCB and DSC are nonnegative by construction (isotonic recalibration can only help, and is
at least as good as a constant). Reporting them separates "badly calibrated" (high MCB, fixable
by calibration) from "no signal" (low DSC, needs a better model) — which a single Brier number
conflates. Pure-python (no sklearn) so it is safe to call on the live decision path.
"""

from __future__ import annotations


def pav_isotonic(x: list[float], y: list[float]) -> list[float]:
    """Isotonic regression of ``y`` on ``x`` via pool-adjacent-violators (PAV).

    Returns fitted values aligned to the input order. Points with equal ``x`` are pooled to a
    common value first (isotonicity forces equal-x points to share a fitted value), then a
    weighted PAV sweep enforces a non-decreasing fit. Preserves the overall mean.
    """

    n = len(x)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: x[i])

    # Pre-pool equal-x points into weighted groups (mean y, weight = group size).
    groups: list[tuple[float, list[int]]] = []
    for i in order:
        if groups and groups[-1][0] == float(x[i]):
            groups[-1][1].append(i)
        else:
            groups.append((float(x[i]), [i]))
    means = [sum(float(y[j]) for j in idx) / len(idx) for _, idx in groups]
    weights = [float(len(idx)) for _, idx in groups]

    # Weighted PAV: stack of (mean, weight, num_groups_covered).
    stack: list[list[float]] = []
    for m, w in zip(means, weights, strict=True):
        cur_mean, cur_weight, cur_groups = m, w, 1.0
        while stack and stack[-1][0] > cur_mean:
            pm, pw, pg = stack.pop()
            cur_mean = (pm * pw + cur_mean * cur_weight) / (pw + cur_weight)
            cur_weight += pw
            cur_groups += pg
        stack.append([cur_mean, cur_weight, cur_groups])

    group_fit: list[float] = []
    for mean, _weight, ngroups in stack:
        group_fit.extend([mean] * int(ngroups))

    fitted = [0.0] * n
    for (_xval, idx), fitv in zip(groups, group_fit, strict=True):
        for j in idx:
            fitted[j] = fitv
    return fitted


def brier_score_decomposition(
    forecasts: list[float], outcomes: list[float]
) -> dict[str, float | None]:
    """Decompose the mean Brier score into MCB / DSC / UNC (and report S_recal, base rate)."""

    n = len(forecasts)
    if n == 0 or len(outcomes) != n:
        return {"brier": None, "mcb": None, "dsc": None, "unc": None, "s_recal": None, "base_rate": None, "n": n}
    p = [float(v) for v in forecasts]
    y = [float(v) for v in outcomes]
    base = sum(y) / n
    brier = sum((pi - yi) ** 2 for pi, yi in zip(p, y, strict=True)) / n
    recal = pav_isotonic(p, y)
    s_recal = sum((ci - yi) ** 2 for ci, yi in zip(recal, y, strict=True)) / n
    unc = base * (1.0 - base)
    return {
        "brier": brier,
        "mcb": max(0.0, brier - s_recal),
        "dsc": max(0.0, unc - s_recal),
        "unc": unc,
        "s_recal": s_recal,
        "base_rate": base,
        "n": n,
    }
