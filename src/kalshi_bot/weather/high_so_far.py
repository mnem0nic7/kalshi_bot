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
