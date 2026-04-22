"""Per-station σ calibration — two-stage fit.

Stage 1: σ_base(station, season) — fit per station/season across all lead times.
Stage 2: lead_factor(lead_bucket) — fit once globally across all stations/seasons.

At query time: σ_effective = σ_base(station, season) × lead_factor(lead_bucket).

See docs/strategy/kalshi_build_spec.md §4.2 for full architecture rationale.
"""

from __future__ import annotations

import math
import statistics
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

SEASON_BUCKETS: dict[int, str] = {
    12: "DJF", 1: "DJF", 2: "DJF",
    3: "MAM", 4: "MAM", 5: "MAM",
    6: "JJA", 7: "JJA", 8: "JJA",
    9: "SON", 10: "SON", 11: "SON",
}

LEAD_BUCKETS: dict[str, tuple[float, float]] = {
    "D-0":  (0.0,  18.0),
    "D-1":  (18.0, 42.0),
    "D-2+": (42.0, float("inf")),
}


def season_for_month(month: int) -> str:
    return SEASON_BUCKETS[month]


def lead_bucket_for_hours(lead_hours: float) -> str:
    for bucket, (lo, hi) in LEAD_BUCKETS.items():
        if lo <= lead_hours < hi:
            return bucket
    return "D-2+"


def _crps_normal(sigma: float, bias: float, residuals: list[float]) -> float:
    """Mean CRPS for a N(bias, sigma²) forecast against observed residuals."""
    if sigma <= 0 or not residuals:
        return float("inf")
    sqrt2 = math.sqrt(2)
    sqrtpi = math.sqrt(math.pi)
    total = 0.0
    for r in residuals:
        z = (r - bias) / sigma
        phi_z = math.exp(-0.5 * z * z) / math.sqrt(2 * math.pi)
        Phi_z = 0.5 * (1.0 + math.erf(z / sqrt2))
        total += sigma * (z * (2 * Phi_z - 1) + 2 * phi_z - 1 / sqrtpi)
    return total / len(residuals)


def fit_sigma_base(
    residuals: list[float],
    *,
    held_out: list[float] | None = None,
    global_sigma: float = 3.5,
    global_bias: float = 0.0,
) -> dict[str, Any]:
    """Fit σ_base and mean_bias from residuals (crosscheck_high_f - forecast_high_f).

    Returns a dict matching the StationSigmaParams column set (minus DB fields).
    held_out is used for CRPS evaluation; defaults to a 20% split of residuals.
    """
    n = len(residuals)
    if n < 2:
        return {}

    # Train/held-out split
    if held_out is None:
        split = max(1, n // 5)
        train = residuals[:-split]
        held_out = residuals[-split:]
    else:
        train = residuals

    if len(train) < 2:
        return {}

    mean_bias = statistics.mean(train)
    sigma_base = statistics.stdev(train)
    sigma_se = sigma_base / math.sqrt(2 * len(train))

    try:
        skewness = _skewness(train)
    except Exception:
        skewness = None

    crps_fit = _crps_normal(sigma_base, mean_bias, held_out)
    crps_global = _crps_normal(global_sigma, global_bias, held_out)
    crps_improvement = crps_global - crps_fit  # positive = fit beats global

    return {
        "sigma_base_f": sigma_base,
        "mean_bias_f": mean_bias,
        "sample_count": n,
        "sigma_se_f": sigma_se,
        "residual_skewness": skewness,
        "crps_improvement_vs_global": crps_improvement,
    }


def fit_lead_factors(
    residuals_by_lead: dict[str, list[float]],
) -> dict[str, float]:
    """Fit per-lead-bucket σ scaling from residuals, normalised so D-0 = 1.0.

    Uses log(|residual|) OLS per bucket as a proxy for lead-dependent σ.
    Returns {lead_bucket: factor} — only populated buckets are returned.
    """
    bucket_sigma: dict[str, float] = {}
    for bucket, residuals in residuals_by_lead.items():
        if len(residuals) < 10:
            continue
        sigma = statistics.stdev(residuals)
        if sigma > 0:
            bucket_sigma[bucket] = sigma

    if "D-0" not in bucket_sigma or bucket_sigma["D-0"] == 0:
        return {b: 1.0 for b in bucket_sigma}

    baseline = bucket_sigma["D-0"]
    return {b: s / baseline for b, s in bucket_sigma.items()}


def _skewness(data: list[float]) -> float:
    n = len(data)
    if n < 3:
        return 0.0
    m = statistics.mean(data)
    s = statistics.stdev(data)
    if s == 0:
        return 0.0
    return sum((x - m) ** 3 for x in data) / (n * s ** 3)


async def persist_sigma_params(
    session: "AsyncSession",
    station: str,
    season_bucket: str,
    params: dict[str, Any],
    version: str,
) -> None:
    """Write a StationSigmaParams row; deactivate prior active rows for same cell."""
    from sqlalchemy import update
    from kalshi_bot.db.models import StationSigmaParams

    await session.execute(
        update(StationSigmaParams)
        .where(
            StationSigmaParams.station == station,
            StationSigmaParams.season_bucket == season_bucket,
            StationSigmaParams.is_active.is_(True),
        )
        .values(is_active=False)
    )
    row = StationSigmaParams(
        station=station,
        season_bucket=season_bucket,
        fitted_at=datetime.now(UTC),
        version=version,
        is_active=True,
        **params,
    )
    session.add(row)


async def persist_lead_factors(
    session: "AsyncSession",
    factors: dict[str, float],
    sample_counts: dict[str, int],
    version: str,
) -> None:
    """Write GlobalLeadFactor rows; deactivate prior active rows."""
    from sqlalchemy import update
    from kalshi_bot.db.models import GlobalLeadFactor

    await session.execute(
        update(GlobalLeadFactor)
        .where(GlobalLeadFactor.is_active.is_(True))
        .values(is_active=False)
    )
    for bucket, factor in factors.items():
        row = GlobalLeadFactor(
            lead_bucket=bucket,
            factor=factor,
            sample_count=sample_counts.get(bucket, 0),
            fitted_at=datetime.now(UTC),
            version=version,
            is_active=True,
        )
        session.add(row)


async def load_active_sigma_params(
    session: "AsyncSession",
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load all active StationSigmaParams rows keyed by (station, season_bucket)."""
    from sqlalchemy import select
    from kalshi_bot.db.models import StationSigmaParams

    rows = (await session.execute(
        select(StationSigmaParams).where(StationSigmaParams.is_active.is_(True))
    )).scalars().all()

    return {
        (r.station, r.season_bucket): {
            "sigma_base_f": r.sigma_base_f,
            "mean_bias_f": r.mean_bias_f,
            "sample_count": r.sample_count,
            "crps_improvement_vs_global": r.crps_improvement_vs_global,
        }
        for r in rows
    }


async def load_active_lead_factors(
    session: "AsyncSession",
) -> dict[str, float]:
    """Load all active GlobalLeadFactor rows keyed by lead_bucket."""
    from sqlalchemy import select
    from kalshi_bot.db.models import GlobalLeadFactor

    rows = (await session.execute(
        select(GlobalLeadFactor).where(GlobalLeadFactor.is_active.is_(True))
    )).scalars().all()

    return {r.lead_bucket: r.factor for r in rows}
