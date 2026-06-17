"""Analytic fair value + volatility estimator (plan §4.2).

`fair_up_normal` is the digital-option probability Φ(ln(S/K)/(σ√τ)) on the MM
tick schema. `realized_vol` is the simple v1 σ̂ (trailing realized vol). The
σ estimate is the one place where added modeling sophistication pays off
(EWMA/HAR, intraday-seasonality) — crypto-vol-eval showed the analytic edge
stands or falls on it — so it lives here, isolated and swappable.

The raw formula is NOT trusted on its own: the loop fits an isotonic calibration
map (plan 4.2) on logged settlements before any P&L is claimed.
"""
from __future__ import annotations

import math
from decimal import Decimal
from statistics import median


def infer_step_seconds(timestamps: list[float]) -> float | None:
    """Median gap between consecutive spot observations.

    The σ̂ is a per-step realized vol, so the per-step→remaining-horizon scaling
    needs the *actual* cadence — the live spot feed is ~5s, not the 60s the
    candidate assumed. Returns None when there aren't enough timestamps.
    """
    ts = sorted(float(t) for t in timestamps if t is not None)
    if len(ts) < 2:
        return None
    deltas = [b - a for a, b in zip(ts, ts[1:]) if b > a]
    if not deltas:
        return None
    return float(median(deltas))


def realized_vol(prices: list[Decimal]) -> Decimal | None:
    """Std of simple returns over the supplied price window (per-step σ̂)."""
    valid = [p for p in prices if p is not None and p > 0]
    if len(valid) < 3:
        return None
    rets = [float((b - a) / a) for a, b in zip(valid, valid[1:]) if a > 0]
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    if var <= 0:
        return None
    return Decimal(str(math.sqrt(var)))


def ewma_vol(prices: list[Decimal], *, lam: float = 0.94) -> Decimal | None:
    """Exponentially weighted std of simple returns (RiskMetrics-style σ̂).

    σ²_t = λ·σ²_{t-1} + (1−λ)·r²_t, seeded at the first squared return. A higher
    ``lam`` lengthens the effective memory (smoother, less per-row ranking noise —
    the "stable" estimate); a lower ``lam`` reacts faster to recent moves. Same
    minimum-data contract as ``realized_vol`` (needs ≥3 usable prices); None when
    the series is degenerate or flat.
    """
    valid = [p for p in prices if p is not None and p > 0]
    if len(valid) < 3:
        return None
    rets = [float((b - a) / a) for a, b in zip(valid, valid[1:]) if a > 0]
    if len(rets) < 2:
        return None
    decay = min(max(float(lam), 0.0), 0.999999)
    var = rets[0] * rets[0]
    for r in rets[1:]:
        var = decay * var + (1.0 - decay) * (r * r)
    if var <= 0:
        return None
    return Decimal(str(math.sqrt(var)))


def fair_up_normal(
    *,
    spot: Decimal,
    strike: Decimal,
    sigma: Decimal,
    seconds_to_close: int,
    step_interval_seconds: float = 60.0,
    max_abs_z: float | None = 3.0,
) -> Decimal | None:
    """Φ(z) with z = (ln(S/K)/σ_step)·√(step/τ); None on degenerate inputs.

    Uses the simple-return moneyness (S−K)/K as the ln(S/K) proxy. As τ→0 the
    √(step/τ) term grows and the probability snaps toward 0/1 (terminal regime);
    ``max_abs_z`` caps |z| so a tiny per-step σ near close doesn't saturate Φ
    past the point where calibration can recover it (see crypto-vol-eval finding).
    """
    if strike <= 0 or sigma <= 0 or seconds_to_close <= 0:
        return None
    step = float(step_interval_seconds) or 60.0
    moneyness = (spot - strike) / strike
    z = (float(moneyness) / float(sigma)) * math.sqrt(step / float(seconds_to_close))
    if max_abs_z is not None and max_abs_z > 0:
        z = max(-float(max_abs_z), min(float(max_abs_z), z))
    return Decimal(str(0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))))
