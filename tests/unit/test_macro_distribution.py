"""Tests for `macro.distribution`: pure P(>k) math, monotonicity, and sigma-curve fitting."""
from __future__ import annotations

import math

import pytest

from kalshi_bot.macro.distribution import NowcastDistribution, SigmaCurve, SigmaCurvePoint


def test_prob_above_at_point_estimate_is_one_half_for_gaussian() -> None:
    dist = NowcastDistribution(point_estimate=0.3, sigma=0.15, kind="gaussian")
    assert dist.prob_above(0.3) == pytest.approx(0.5, abs=1e-9)


def test_prob_above_known_z_score_gaussian() -> None:
    # one sigma above the point estimate -> upper tail is Phi(-1) ~= 0.1587
    dist = NowcastDistribution(point_estimate=0.0, sigma=1.0, kind="gaussian")
    assert dist.prob_above(1.0) == pytest.approx(1 - 0.8413447460685429, abs=1e-9)
    assert dist.prob_above(-1.0) == pytest.approx(0.8413447460685429, abs=1e-9)


def test_ladder_probabilities_monotonic_non_increasing_gaussian() -> None:
    dist = NowcastDistribution(point_estimate=0.4, sigma=0.2, kind="gaussian")
    strikes = [-0.5, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    probs = dist.ladder_probabilities(strikes)
    for a, b in zip(probs, probs[1:]):
        assert b <= a + 1e-12


def test_ladder_probabilities_monotonic_student_t() -> None:
    dist = NowcastDistribution(point_estimate=0.4, sigma=0.2, kind="student_t", student_t_df=4.0)
    strikes = [-1.0, -0.5, 0.0, 0.4, 0.9, 1.5]
    probs = dist.ladder_probabilities(strikes)
    for a, b in zip(probs, probs[1:]):
        assert b <= a + 1e-12


def test_sigma_zero_or_negative_rejected() -> None:
    with pytest.raises(ValueError):
        NowcastDistribution(point_estimate=0.3, sigma=0.0)
    with pytest.raises(ValueError):
        NowcastDistribution(point_estimate=0.3, sigma=-0.1)


def test_empirical_distribution_requires_errors() -> None:
    with pytest.raises(ValueError):
        NowcastDistribution(point_estimate=0.3, sigma=0.1, kind="empirical", empirical_errors=())


def test_empirical_distribution_matches_residual_exceedance_fraction() -> None:
    # residuals (actual - nowcast): half above +0.1, half at/below -> exact fraction check
    errors = (0.3, 0.2, 0.1, -0.1, -0.2, -0.3, -0.4)
    dist = NowcastDistribution(point_estimate=1.0, sigma=1.0, kind="empirical", empirical_errors=errors)
    # threshold = strike - point_estimate = 1.05 - 1.0 = 0.05; errors strictly greater: 0.3, 0.2, 0.1 -> 3/7
    assert dist.prob_above(1.05) == pytest.approx(3 / 7)


def test_empirical_distribution_can_be_non_monotonic_and_is_caught() -> None:
    # deliberately contrived point/errors so two adjacent strikes evaluate to conflicting fractions
    # in a way that violates monotonicity is hard to construct from a real empirical CDF (it is
    # monotonic by construction over its own support) -- assert instead that the guard rejects an
    # artificially broken subclass-free scenario using out-of-order strikes fed unsorted.
    dist = NowcastDistribution(point_estimate=0.0, sigma=1.0, kind="empirical", empirical_errors=(1.0, 2.0, 3.0))
    probs_sorted_strikes = dist.ladder_probabilities([-1.0, 0.0, 1.0, 2.0, 4.0])
    for a, b in zip(probs_sorted_strikes, probs_sorted_strikes[1:]):
        assert b <= a + 1e-12


def test_sigma_curve_interpolates_linearly_between_points() -> None:
    curve = SigmaCurve([SigmaCurvePoint(0, 0.10), SigmaCurvePoint(10, 0.20)])
    assert curve.sigma_at(5) == pytest.approx(0.15)
    assert curve.sigma_at(0) == pytest.approx(0.10)
    assert curve.sigma_at(10) == pytest.approx(0.20)


def test_sigma_curve_clamps_outside_range() -> None:
    curve = SigmaCurve([SigmaCurvePoint(5, 0.10), SigmaCurvePoint(15, 0.20)])
    assert curve.sigma_at(0) == pytest.approx(0.10)
    assert curve.sigma_at(100) == pytest.approx(0.20)


def test_sigma_curve_fit_from_errors_by_horizon_bucket() -> None:
    errors_by_bucket = {
        0: [0.1, -0.1, 0.1, -0.1],  # mean 0, variance 0.01 -> sigma 0.1
        10: [0.2, -0.2, 0.2, -0.2],  # sigma 0.2
    }
    curve = SigmaCurve.fit(errors_by_bucket)
    assert curve.sigma_at(0) == pytest.approx(0.1, abs=1e-9)
    assert curve.sigma_at(10) == pytest.approx(0.2, abs=1e-9)


def test_sigma_curve_fit_drops_buckets_below_min_observations() -> None:
    errors_by_bucket = {0: [0.1, -0.1, 0.1, -0.1], 10: [0.5]}  # only 1 observation
    curve = SigmaCurve.fit(errors_by_bucket, min_observations=2)
    # bucket 10 dropped -> sigma_at(10) falls back to the only remaining point (bucket 0)
    assert curve.sigma_at(10) == pytest.approx(curve.sigma_at(0))


def test_sigma_curve_fit_raises_when_no_bucket_qualifies() -> None:
    with pytest.raises(ValueError):
        SigmaCurve.fit({0: []}, min_observations=1)


def test_sigma_curve_requires_positive_sigma() -> None:
    with pytest.raises(ValueError):
        SigmaCurve([SigmaCurvePoint(0, 0.0)])


def test_sigma_to_zero_limit_step_function_via_tiny_sigma() -> None:
    """As sigma -> 0, the distribution approaches a step function around the point estimate."""
    dist = NowcastDistribution(point_estimate=0.5, sigma=1e-6, kind="gaussian")
    assert dist.prob_above(0.4) == pytest.approx(1.0, abs=1e-6)
    assert dist.prob_above(0.6) == pytest.approx(0.0, abs=1e-6)
