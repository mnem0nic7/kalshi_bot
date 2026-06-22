"""Core of the weather rework (2026-06-22): observed-high-so-far reconstruction +
the intraday terminal-certainty probability that captures the ONLY verified,
fee-survivable edge (deep-research result): the high-so-far lock-in.

- Once observed high-so-far >= strike, YES is deterministic (P=1).
- Otherwise P(daily_high >= strike) = P(remaining_rise >= strike - high_so_far),
  and as the diurnal peak passes the remaining rise (mean, sigma) -> 0, so P -> 0
  (locked NO). The remaining-rise distribution is supplied by an adaptive diurnal
  model (HRRR/climatology) — kept as an input so this core is pure + testable.

Replaces the crude `max(forecast, high_so_far)` in weather/scoring.py.
See docs/research/2026-06-22-weather-strategy-rework.md.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.weather.high_so_far import (
    reconstruct_high_so_far,
    terminal_high_ge_probability,
)


def _h(hour: int, temp: float) -> tuple[datetime, float]:
    return (datetime(2026, 6, 22, hour, 0, tzinfo=UTC), temp)


def test_reconstruct_high_so_far_is_running_max() -> None:
    hourly = [_h(6, 60.0), _h(9, 68.0), _h(12, 75.0), _h(14, 74.0), _h(16, 77.0), _h(18, 72.0)]
    series = reconstruct_high_so_far(hourly)
    # running max up to and including each timestamp
    assert [round(v, 1) for _, v in series] == [60.0, 68.0, 75.0, 75.0, 77.0, 77.0]
    # daily high = final running max
    assert series[-1][1] == 77.0


def test_reconstruct_high_so_far_handles_unsorted_and_empty() -> None:
    hourly = [_h(16, 77.0), _h(6, 60.0), _h(12, 75.0)]
    series = reconstruct_high_so_far(hourly)
    # sorted ascending by time, then running max
    assert [round(v, 1) for _, v in series] == [60.0, 75.0, 77.0]
    assert reconstruct_high_so_far([]) == []


def test_terminal_prob_locked_yes_when_high_so_far_meets_strike() -> None:
    # observed high already >= strike -> YES is deterministic regardless of remaining rise
    assert terminal_high_ge_probability(
        high_so_far_f=76.0, strike_f=75.0, remaining_rise_mean_f=0.0, remaining_rise_sigma_f=0.0
    ) == 1.0
    assert terminal_high_ge_probability(
        high_so_far_f=75.0, strike_f=75.0, remaining_rise_mean_f=2.0, remaining_rise_sigma_f=3.0
    ) == 1.0


def test_terminal_prob_locked_no_when_peak_passed_and_below_strike() -> None:
    # past the peak: no remaining rise possible, still below strike -> NO is deterministic
    assert terminal_high_ge_probability(
        high_so_far_f=72.0, strike_f=75.0, remaining_rise_mean_f=0.0, remaining_rise_sigma_f=0.0
    ) == 0.0


def test_terminal_prob_intermediate_is_gaussian_tail() -> None:
    # need +3F more; expected remaining rise +2F, sigma 2F -> P(rise >= 3) = 1 - Phi((3-2)/2) = 1 - Phi(0.5)
    p = terminal_high_ge_probability(
        high_so_far_f=72.0, strike_f=75.0, remaining_rise_mean_f=2.0, remaining_rise_sigma_f=2.0
    )
    assert 0.30 < p < 0.32  # 1 - 0.6915 = 0.3085
    # more expected rise -> higher probability
    p_hi = terminal_high_ge_probability(
        high_so_far_f=72.0, strike_f=75.0, remaining_rise_mean_f=4.0, remaining_rise_sigma_f=2.0
    )
    assert p_hi > p


def test_terminal_prob_clamped_unit_interval() -> None:
    p = terminal_high_ge_probability(
        high_so_far_f=60.0, strike_f=75.0, remaining_rise_mean_f=20.0, remaining_rise_sigma_f=0.0
    )
    assert p == 1.0  # deterministic rise exceeds need
    p0 = terminal_high_ge_probability(
        high_so_far_f=60.0, strike_f=75.0, remaining_rise_mean_f=1.0, remaining_rise_sigma_f=0.0
    )
    assert p0 == 0.0  # deterministic rise far short
