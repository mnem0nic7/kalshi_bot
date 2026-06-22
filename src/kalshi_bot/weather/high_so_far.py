"""Observed-high-so-far reconstruction + intraday terminal-certainty probability.

Core of the 2026-06-22 weather rework. Deep research verdict: the only verified,
fee-survivable edge in Kalshi daily-temperature binaries is the intraday
HIGH-SO-FAR lock-in — trade the observation, not the afternoon forecast (forecast
skill is worst at the afternoon peak when the daily high is set). This module
provides the two pure primitives the reworked strategy is built on; the adaptive
diurnal "remaining rise" distribution (HRRR/climatology) is supplied by the caller
so this core stays pure and testable.

See docs/research/2026-06-22-weather-strategy-rework.md.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any

# Fallback uncertainty (°F) for an unseen (season, hour) bucket: stay non-degenerate
# so the terminal probability never over-commits where we have no climatology.
_DEFAULT_FALLBACK_SIGMA_F = 5.0


def reconstruct_high_so_far(
    hourly_temps: list[tuple[datetime, float]],
) -> list[tuple[datetime, float]]:
    """Running max of temperature up to and including each timestamp.

    Input: (timestamp, temperature_f) samples for a single local market day.
    Output: (timestamp, high_so_far_f) sorted ascending; the final element's value
    is the realized daily high. Used to build the high-so-far training/backtest
    substrate from Open-Meteo archive hourly temps, and at decision time from live
    observations.
    """
    ordered = sorted(hourly_temps, key=lambda item: item[0])
    series: list[tuple[datetime, float]] = []
    running_max: float | None = None
    for ts, temp in ordered:
        running_max = temp if running_max is None else max(running_max, temp)
        series.append((ts, running_max))
    return series


def _phi(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def terminal_high_ge_probability(
    *,
    high_so_far_f: float,
    strike_f: float,
    remaining_rise_mean_f: float,
    remaining_rise_sigma_f: float,
) -> float:
    """P(daily_high >= strike) given the observed high-so-far and an (adaptive)
    distribution for the remaining intraday rise.

    - If high-so-far already meets the strike, YES is deterministic (lock-in) -> 1.0.
    - Otherwise the daily high = high_so_far + remaining_rise, so
      P(daily_high >= strike) = P(remaining_rise >= strike - high_so_far). As the
      diurnal peak passes, the remaining-rise mean and sigma collapse to 0 and the
      probability collapses to 0 (locked NO). Gaussian remaining-rise model:
      P = 1 - Phi((needed - mean) / sigma), clamped to [0, 1].
    """
    if high_so_far_f >= strike_f:
        return 1.0
    needed = strike_f - high_so_far_f
    if remaining_rise_sigma_f <= 0.0:
        # Deterministic remaining rise (e.g. peak passed -> mean 0): step function.
        return 1.0 if remaining_rise_mean_f >= needed else 0.0
    z = (needed - remaining_rise_mean_f) / remaining_rise_sigma_f
    return min(1.0, max(0.0, 1.0 - _phi(z)))


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n  # population variance
    return mean, math.sqrt(var)


def fit_remaining_rise_model(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Fit the empirical remaining-rise distribution per (season, hour) bucket.

    Each sample: {season, hour, high_so_far_f, daily_high_f}. The observed remaining
    rise = max(0, daily_high - high_so_far). Data-driven (NOT a hard-coded diurnal
    clock — peak timing varies by season/cloud, which the research refuted as fixed).
    Returns {"buckets": {(season,hour): (mean, sigma, n)}, "global": (mean, sigma)}.
    """
    grouped: dict[tuple[str, int], list[float]] = {}
    all_rises: list[float] = []
    for s in samples:
        try:
            season = str(s["season"])
            hour = int(s["hour"])
            rise = max(0.0, float(s["daily_high_f"]) - float(s["high_so_far_f"]))
        except (KeyError, TypeError, ValueError):
            continue
        grouped.setdefault((season, hour), []).append(rise)
        all_rises.append(rise)
    buckets = {key: (*_mean_std(vals), len(vals)) for key, vals in grouped.items()}
    return {"buckets": buckets, "global": _mean_std(all_rises)}


def remaining_rise(model: dict[str, Any], *, season: str, hour: int) -> tuple[float, float]:
    """(expected remaining rise °F, sigma °F) for a (season, hour); safe fallback if unseen."""
    bucket = (model.get("buckets") or {}).get((str(season), int(hour)))
    if bucket is not None:
        mean, sigma, _n = bucket
        return float(mean), float(sigma)
    g_mean, g_sigma = model.get("global", (0.0, 0.0))
    # Unknown bucket: keep the (climatological) mean but inflate sigma so we never
    # over-commit where we lack data.
    return float(g_mean), float(max(g_sigma, _DEFAULT_FALLBACK_SIGMA_F))
