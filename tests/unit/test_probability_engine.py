from __future__ import annotations

import pytest

from kalshi_bot.forecast.probability_engine import (
    ProbabilityEngineConfig,
    TemperatureBucket,
    estimate_bucket_probability,
    estimate_mapping_probability,
    fit_gumbel_from_mean_sigma,
    gumbel_bucket_probability,
    kde_bucket_probability,
)
from kalshi_bot.weather.models import WeatherMarketMapping


def test_gumbel_fit_matches_mean_sigma_bucket_probability() -> None:
    fit = fit_gumbel_from_mean_sigma(mean_f=82.0, sigma_f=4.0)
    bucket = TemperatureBucket(low_f=80.0, high_f=None)

    p_yes = gumbel_bucket_probability(bucket, fit)

    assert fit.beta_f == pytest.approx(3.1185, rel=1e-3)
    assert fit.mu_f == pytest.approx(80.2, rel=1e-2)
    assert 0.55 < p_yes < 0.75


def test_shrinkage_pulls_model_probability_toward_climatology() -> None:
    estimate = estimate_bucket_probability(
        mean_f=85.0,
        sigma_f=3.0,
        bucket=TemperatureBucket(low_f=80.0, high_f=None),
        p_climo=0.25,
        effective_member_count=1,
        config=ProbabilityEngineConfig(pseudo_count=8),
    )

    assert estimate.shrinkage_alpha == pytest.approx(1 / 9)
    assert estimate.p_bucket_yes < estimate.p_model_yes
    assert estimate.p_bucket_yes > estimate.p_climo_yes


def test_kde_path_blends_when_two_sources_and_enough_members_available() -> None:
    gfs = [82.0 + ((idx % 5) - 2) * 0.4 for idx in range(31)]
    ecmwf = [79.0 + ((idx % 5) - 2) * 0.3 for idx in range(51)]

    estimate = estimate_bucket_probability(
        mean_f=80.5,
        sigma_f=3.5,
        bucket=TemperatureBucket(low_f=80.0, high_f=None),
        p_climo=0.50,
        source_members_by_source={"GFS": gfs, "ECMWF": ecmwf},
        config=ProbabilityEngineConfig(pseudo_count=8, kde_min_members=30),
    )

    assert estimate.p_kde_yes is not None
    assert estimate.effective_member_count == 82
    assert estimate.disagreement > 0
    assert estimate.trace["kde_available"] is True
    assert set(estimate.source_set_used) == {"GFS", "ECMWF"}


def test_boundary_mass_increases_near_bucket_edge() -> None:
    bucket = TemperatureBucket(low_f=80.0, high_f=None)

    near = estimate_bucket_probability(mean_f=80.0, sigma_f=2.0, bucket=bucket, p_climo=0.5)
    far = estimate_bucket_probability(mean_f=92.0, sigma_f=2.0, bucket=bucket, p_climo=0.5)

    assert near.boundary_mass > far.boundary_mass
    assert near.boundary_mass > 0.2


def test_mapping_probability_converts_less_than_contract_to_lower_tail() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHCHI-26APR27-T69",
        station_id="KORD",
        location_name="Chicago",
        latitude=41.0,
        longitude=-87.0,
        threshold_f=69.0,
        operator="<",
    )

    estimate = estimate_mapping_probability(
        mapping=mapping,
        mean_f=65.0,
        sigma_f=3.0,
        p_climo=0.5,
        effective_member_count=50,
    )

    assert estimate.trace["bucket"] == {"low_f": None, "high_f": 69.0}
    assert estimate.p_bucket_yes > 0.5


def test_invalid_probability_inputs_fail_closed() -> None:
    with pytest.raises(ValueError):
        estimate_bucket_probability(
            mean_f=80.0,
            sigma_f=0.0,
            bucket=TemperatureBucket(low_f=80.0, high_f=None),
            p_climo=0.5,
        )


def test_kde_bucket_probability_accepts_open_bounds() -> None:
    p = kde_bucket_probability(TemperatureBucket(low_f=None, high_f=70.0), [68.0, 69.0, 72.0])

    assert 0 < p < 1
