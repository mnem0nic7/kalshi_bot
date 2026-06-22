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
    fit_remaining_rise_model,
    reconstruct_high_so_far,
    remaining_rise,
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


# --- adaptive diurnal remaining-rise model (data-driven, fit from samples) ---

def _sample(season: str, hour: int, high_so_far: float, daily_high: float) -> dict:
    return {"season": season, "hour": hour, "high_so_far_f": high_so_far, "daily_high_f": daily_high}


def test_fit_remaining_rise_recovers_bucket_mean() -> None:
    # summer, 9am: daily high is consistently +5F above the 9am high-so-far
    samples = [_sample("summer", 9, 65.0 + i, 70.0 + i) for i in range(20)]
    model = fit_remaining_rise_model(samples)
    mean, sigma = remaining_rise(model, season="summer", hour=9)
    assert abs(mean - 5.0) < 1e-6
    assert sigma >= 0.0 and sigma < 0.01  # no variance in the synthetic rise


def test_fit_remaining_rise_locks_after_peak() -> None:
    # summer, 18:00 (after peak): daily high already equals high-so-far -> no remaining rise
    samples = [_sample("summer", 18, 80.0 + i, 80.0 + i) for i in range(15)]
    model = fit_remaining_rise_model(samples)
    mean, sigma = remaining_rise(model, season="summer", hour=18)
    assert mean == 0.0 and sigma == 0.0  # locked: nothing more to come


def test_remaining_rise_fallback_for_unseen_bucket() -> None:
    model = fit_remaining_rise_model([_sample("summer", 9, 65.0, 70.0)])
    # unseen (winter, 3am): must return a safe fallback, not crash
    mean, sigma = remaining_rise(model, season="winter", hour=3)
    assert isinstance(mean, float) and isinstance(sigma, float)
    assert sigma > 0.0  # unknown -> non-degenerate uncertainty
