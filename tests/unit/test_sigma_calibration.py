"""Unit tests for per-station σ calibration (Addition 2, §4.2).

Coverage:
- fit_sigma_base: synthetic residuals with known σ recovers within 5%
- fit_lead_factors: D-0 normalisation, monotonicity, insufficient-data guard
- sigma_f_for_mapping resolver: correct layer selected for all (DB/YAML/global) × (n, CRPS) combos
- lead_correction kill switch (SigmaContext.lead_correction_enabled)
- _crps_normal: baseline values + global < fit comparison
- season_for_month / lead_bucket_for_hours: boundary conditions
- persist/load round-trip via SQLite in-memory DB
"""
from __future__ import annotations

import math
import random
import statistics
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from kalshi_bot.weather.scoring import SigmaContext, sigma_f_for_mapping, nws_forecast_sigma_f
from kalshi_bot.weather.sigma_calibration import (
    LEAD_BUCKETS,
    SEASON_BUCKETS,
    fit_lead_factors,
    fit_sigma_base,
    lead_bucket_for_hours,
    load_active_lead_factors,
    load_active_sigma_params,
    persist_lead_factors,
    persist_sigma_params,
    season_for_month,
    _crps_normal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normal_residuals(n: int, sigma: float, bias: float = 0.0, seed: int = 42) -> list[float]:
    rng = random.Random(seed)
    return [bias + sigma * _box_muller(rng) for _ in range(n)]


def _box_muller(rng: random.Random) -> float:
    u1, u2 = rng.random(), rng.random()
    return math.sqrt(-2 * math.log(u1 + 1e-12)) * math.cos(2 * math.pi * u2)


# ---------------------------------------------------------------------------
# season_for_month
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("month,expected", [
    (12, "DJF"), (1, "DJF"), (2, "DJF"),
    (3, "MAM"), (4, "MAM"), (5, "MAM"),
    (6, "JJA"), (7, "JJA"), (8, "JJA"),
    (9, "SON"), (10, "SON"), (11, "SON"),
])
def test_season_for_month(month: int, expected: str) -> None:
    assert season_for_month(month) == expected


def test_season_for_month_covers_all_calendar_months() -> None:
    for m in range(1, 13):
        assert season_for_month(m) in {"DJF", "MAM", "JJA", "SON"}


# ---------------------------------------------------------------------------
# lead_bucket_for_hours
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hours,expected", [
    (0.0,   "D-0"),
    (17.9,  "D-0"),
    (18.0,  "D-1"),
    (41.9,  "D-1"),
    (42.0,  "D-2+"),
    (120.0, "D-2+"),
])
def test_lead_bucket_for_hours(hours: float, expected: str) -> None:
    assert lead_bucket_for_hours(hours) == expected


def test_lead_bucket_covers_all_defined_buckets() -> None:
    midpoints = {"D-0": 9.0, "D-1": 30.0, "D-2+": 72.0}
    for bucket, hours in midpoints.items():
        assert lead_bucket_for_hours(hours) == bucket


# ---------------------------------------------------------------------------
# _crps_normal
# ---------------------------------------------------------------------------

def test_crps_normal_perfect_forecast_near_zero() -> None:
    residuals = [0.0] * 100
    assert _crps_normal(0.01, 0.0, residuals) < 0.01


def test_crps_normal_infinite_sigma_returns_inf() -> None:
    assert math.isinf(_crps_normal(0.0, 0.0, [1.0, 2.0]))


def test_crps_normal_empty_residuals_returns_inf() -> None:
    assert math.isinf(_crps_normal(1.0, 0.0, []))


def test_crps_normal_larger_sigma_worse_for_residuals_near_zero() -> None:
    residuals = _normal_residuals(200, sigma=1.0, bias=0.0)
    crps_tight = _crps_normal(1.0, 0.0, residuals)
    crps_loose = _crps_normal(5.0, 0.0, residuals)
    assert crps_tight < crps_loose


def test_crps_normal_bias_increases_crps() -> None:
    residuals = _normal_residuals(200, sigma=2.0, bias=0.0)
    crps_unbiased = _crps_normal(2.0, 0.0, residuals)
    crps_biased = _crps_normal(2.0, 5.0, residuals)
    assert crps_unbiased < crps_biased


# ---------------------------------------------------------------------------
# fit_sigma_base — synthetic recovery
# ---------------------------------------------------------------------------

def test_fit_sigma_base_recovers_sigma_within_5_percent() -> None:
    true_sigma = 4.2
    residuals = _normal_residuals(500, sigma=true_sigma, bias=0.0)
    params = fit_sigma_base(residuals, global_sigma=3.5, global_bias=0.0)

    assert params, "fit_sigma_base returned empty dict"
    fitted = params["sigma_base_f"]
    assert abs(fitted - true_sigma) / true_sigma < 0.05, (
        f"Expected σ≈{true_sigma:.2f}, got {fitted:.2f} (error {abs(fitted-true_sigma)/true_sigma:.1%})"
    )


def test_fit_sigma_base_recovers_bias_within_tolerance() -> None:
    true_bias = 1.5
    residuals = _normal_residuals(500, sigma=3.0, bias=true_bias)
    params = fit_sigma_base(residuals, global_sigma=3.5, global_bias=0.0)
    assert abs(params["mean_bias_f"] - true_bias) < 0.3


def test_fit_sigma_base_returns_all_required_keys() -> None:
    residuals = _normal_residuals(100, sigma=2.5)
    params = fit_sigma_base(residuals)
    required = {
        "sigma_base_f", "mean_bias_f", "sample_count",
        "sigma_se_f", "residual_skewness", "crps_improvement_vs_global",
    }
    assert required <= set(params.keys())


def test_fit_sigma_base_sample_count_matches_input() -> None:
    residuals = _normal_residuals(123, sigma=2.0)
    params = fit_sigma_base(residuals)
    assert params["sample_count"] == 123


def test_fit_sigma_base_fewer_than_2_returns_empty() -> None:
    assert fit_sigma_base([]) == {}
    assert fit_sigma_base([1.0]) == {}


def test_fit_sigma_base_crps_improvement_positive_for_accurate_fit() -> None:
    """A tight local fit should beat the 3.5°F global fallback on residuals ≈ 1°F."""
    residuals = _normal_residuals(300, sigma=1.0, bias=0.0)
    params = fit_sigma_base(residuals, global_sigma=3.5, global_bias=0.0)
    assert params["crps_improvement_vs_global"] > 0, (
        "Expected tight fit to beat global fallback on held-out set"
    )


def test_fit_sigma_base_crps_improvement_negative_when_global_wins() -> None:
    """When local σ is inflated, global σ=3.5 wins on held-out residuals near zero."""
    # Train on high-variance data (fitted σ ≈ 20); evaluate on residuals near zero.
    # CRPS(σ=20, 0) >> CRPS(σ=3.5, 0), so improvement = global - fit < 0.
    train = _normal_residuals(200, sigma=20.0, bias=0.0)
    held_out_near_zero = [0.0] * 40
    params = fit_sigma_base(train, held_out=held_out_near_zero, global_sigma=3.5, global_bias=0.0)
    assert params["crps_improvement_vs_global"] < 0


def test_fit_sigma_base_explicit_held_out() -> None:
    train = _normal_residuals(200, sigma=2.0)
    held_out = _normal_residuals(50, sigma=2.0, seed=99)
    params = fit_sigma_base(train, held_out=held_out, global_sigma=3.5, global_bias=0.0)
    assert params["sample_count"] == 200  # train size
    assert params["sigma_base_f"] > 0


def test_fit_sigma_base_sigma_se_scales_with_sqrt_n() -> None:
    """σ_SE should scale as 1/√n."""
    r100 = _normal_residuals(100, sigma=2.0)
    r400 = _normal_residuals(400, sigma=2.0)
    p100 = fit_sigma_base(r100)
    p400 = fit_sigma_base(r400)
    ratio = p100["sigma_se_f"] / p400["sigma_se_f"]
    assert 1.5 < ratio < 3.5, f"Expected SE ratio ≈ 2, got {ratio:.2f}"


# ---------------------------------------------------------------------------
# fit_lead_factors
# ---------------------------------------------------------------------------

def test_fit_lead_factors_d0_normalised_to_one() -> None:
    residuals = {
        "D-0":  _normal_residuals(50, sigma=1.0),
        "D-1":  _normal_residuals(50, sigma=1.5),
        "D-2+": _normal_residuals(50, sigma=2.0),
    }
    factors = fit_lead_factors(residuals)
    assert math.isclose(factors["D-0"], 1.0, abs_tol=1e-9)


def test_fit_lead_factors_monotone_increase_with_lead() -> None:
    residuals = {
        "D-0":  _normal_residuals(50, sigma=1.0),
        "D-1":  _normal_residuals(50, sigma=1.8),
        "D-2+": _normal_residuals(50, sigma=2.8),
    }
    factors = fit_lead_factors(residuals)
    assert factors["D-0"] < factors["D-1"] < factors["D-2+"]


def test_fit_lead_factors_insufficient_data_excluded() -> None:
    """Buckets with < 10 samples should be silently dropped."""
    residuals = {
        "D-0":  _normal_residuals(50, sigma=1.0),
        "D-1":  _normal_residuals(5, sigma=1.5),   # too few
    }
    factors = fit_lead_factors(residuals)
    assert "D-1" not in factors
    assert "D-0" in factors


def test_fit_lead_factors_all_insufficient_returns_empty() -> None:
    residuals = {"D-0": [1.0, 2.0], "D-1": [3.0]}
    assert fit_lead_factors(residuals) == {}


def test_fit_lead_factors_missing_d0_falls_back_to_unit_factors() -> None:
    """Without D-0 baseline all returned factors are 1.0."""
    residuals = {
        "D-1":  _normal_residuals(50, sigma=1.5),
        "D-2+": _normal_residuals(50, sigma=2.0),
    }
    factors = fit_lead_factors(residuals)
    for f in factors.values():
        assert math.isclose(f, 1.0, abs_tol=1e-9)


def test_fit_lead_factors_all_factors_positive() -> None:
    residuals = {b: _normal_residuals(30, sigma=2.0) for b in LEAD_BUCKETS}
    factors = fit_lead_factors(residuals)
    for b, f in factors.items():
        assert f > 0, f"factor for {b} is non-positive: {f}"


# ---------------------------------------------------------------------------
# persist / load round-trip (mocked AsyncSession)
# ---------------------------------------------------------------------------

def _mock_session() -> MagicMock:
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))))
    session.add = MagicMock()
    return session


@pytest.mark.asyncio
async def test_persist_sigma_params_deactivates_then_adds() -> None:
    session = _mock_session()
    params = {
        "sigma_base_f": 2.3,
        "mean_bias_f": 0.1,
        "sample_count": 150,
        "sigma_se_f": 0.12,
        "residual_skewness": -0.2,
        "crps_improvement_vs_global": 0.05,
    }
    await persist_sigma_params(session, "KBOS", "DJF", params, version="test_v1")
    assert session.execute.called   # deactivation UPDATE
    assert session.add.called       # new row


@pytest.mark.asyncio
async def test_persist_lead_factors_deactivates_then_adds_per_bucket() -> None:
    session = _mock_session()
    factors = {"D-0": 1.0, "D-1": 1.4, "D-2+": 1.9}
    counts = {"D-0": 80, "D-1": 60, "D-2+": 40}
    await persist_lead_factors(session, factors, counts, version="test_v1")
    assert session.execute.called
    assert session.add.call_count == 3  # one row per bucket


@pytest.mark.asyncio
async def test_load_active_sigma_params_empty_db_returns_empty_dict() -> None:
    session = _mock_session()
    result = await load_active_sigma_params(session)
    assert result == {}


@pytest.mark.asyncio
async def test_load_active_lead_factors_empty_db_returns_empty_dict() -> None:
    session = _mock_session()
    result = await load_active_lead_factors(session)
    assert result == {}


@pytest.mark.asyncio
async def test_load_active_sigma_params_maps_correctly() -> None:
    from kalshi_bot.db.models import StationSigmaParams

    row = MagicMock(spec=StationSigmaParams)
    row.station = "KLAX"
    row.season_bucket = "JJA"
    row.sigma_base_f = 1.8
    row.mean_bias_f = -0.5
    row.sample_count = 200
    row.crps_improvement_vs_global = 0.03

    session = MagicMock()
    session.execute = AsyncMock(
        return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[row]))))
    )

    result = await load_active_sigma_params(session)
    assert ("KLAX", "JJA") in result
    assert result[("KLAX", "JJA")]["sigma_base_f"] == 1.8
    assert result[("KLAX", "JJA")]["mean_bias_f"] == -0.5


@pytest.mark.asyncio
async def test_load_active_lead_factors_maps_correctly() -> None:
    from kalshi_bot.db.models import GlobalLeadFactor

    row = MagicMock(spec=GlobalLeadFactor)
    row.lead_bucket = "D-1"
    row.factor = 1.35

    session = MagicMock()
    session.execute = AsyncMock(
        return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[row]))))
    )

    result = await load_active_lead_factors(session)
    assert result == {"D-1": 1.35}


# ---------------------------------------------------------------------------
# sigma_f_for_mapping — three-layer resolver (§4.2.3)
# ---------------------------------------------------------------------------

class _FakeMapping:
    """Minimal mapping stub for resolver tests."""
    def __init__(self, station_id: str = "KBOS", sigma_f_by_month: dict | None = None) -> None:
        self.station_id = station_id
        self.sigma_f_by_month = sigma_f_by_month or {}


def _db_params(n: int, crps_improvement: float, sigma: float = 2.0) -> dict:
    return {
        "sigma_base_f": sigma,
        "mean_bias_f": 0.0,
        "sample_count": n,
        "crps_improvement_vs_global": crps_improvement,
    }


# --- Layer 3 (global fallback) ---

def test_resolver_no_ctx_returns_global() -> None:
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7)
    assert result == nws_forecast_sigma_f(7)


def test_resolver_no_ctx_yaml_anchor_wins() -> None:
    mapping = _FakeMapping(sigma_f_by_month={7: 1.5})
    result = sigma_f_for_mapping(mapping, month=7)
    assert result == 1.5


def test_resolver_empty_sigma_params_falls_back_to_global() -> None:
    ctx = SigmaContext(station="KBOS", season_bucket="JJA", sigma_params={}, lead_factors={})
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


# --- n < 100: DB-fit too shallow even with positive CRPS ---

def test_resolver_db_fit_n_below_100_falls_back_to_global() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=80, crps_improvement=0.05)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


def test_resolver_db_fit_n_below_100_yaml_still_wins() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=80, crps_improvement=0.05)},
    )
    mapping = _FakeMapping(sigma_f_by_month={7: 1.5})
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == 1.5


# --- 100 ≤ n < 200: DB-fit beats global but not YAML ---

def test_resolver_db_fit_n_100_beats_global() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=100, crps_improvement=0.05, sigma=2.1)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.1)


def test_resolver_db_fit_n_150_yaml_still_beats_db() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=150, crps_improvement=0.05, sigma=2.1)},
    )
    mapping = _FakeMapping(sigma_f_by_month={7: 1.5})
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == 1.5


# --- n ≥ 200: DB-fit beats both global and YAML ---

def test_resolver_db_fit_n_200_beats_yaml() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.1)},
    )
    mapping = _FakeMapping(sigma_f_by_month={7: 1.5})
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.1)


# --- CRPS gate: negative improvement blocks DB-fit regardless of n ---

def test_resolver_db_fit_crps_negative_n_200_falls_back_to_yaml() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=300, crps_improvement=-0.01, sigma=2.1)},
    )
    mapping = _FakeMapping(sigma_f_by_month={7: 1.5})
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == 1.5


def test_resolver_db_fit_crps_negative_no_yaml_falls_back_to_global() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=300, crps_improvement=-0.01, sigma=2.1)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


def test_resolver_db_fit_crps_zero_blocked() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.0, sigma=2.1)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


# --- Lead factor application ---

def test_resolver_lead_factor_applied_when_correction_enabled() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
        lead_factors={"D-0": 1.0, "D-1": 1.5, "D-2+": 2.0},
        lead_hours=30.0,   # falls into D-1 bucket
        lead_correction_enabled=True,
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.0 * 1.5)  # sigma_base × D-1 factor


def test_resolver_lead_factor_not_applied_when_correction_disabled() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
        lead_factors={"D-0": 1.0, "D-1": 1.5, "D-2+": 2.0},
        lead_hours=30.0,
        lead_correction_enabled=False,   # kill switch
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.0)  # no lead factor applied


def test_resolver_lead_factor_missing_bucket_defaults_to_one() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
        lead_factors={"D-0": 1.0},   # D-1 missing
        lead_hours=30.0,              # falls into D-1
        lead_correction_enabled=True,
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.0)  # missing factor → 1.0 default


def test_resolver_lead_hours_none_skips_lead_factor() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
        lead_factors={"D-0": 1.0, "D-1": 1.5},
        lead_hours=None,              # no lead info
        lead_correction_enabled=True,
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == pytest.approx(2.0)


# --- Station / season mismatch ---

def test_resolver_wrong_station_falls_back_to_global() -> None:
    ctx = SigmaContext(
        station="KLAX", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
    )
    mapping = _FakeMapping(station_id="KLAX")
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


def test_resolver_wrong_season_falls_back_to_global() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="DJF",   # wrong season for July
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=2.0)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result == nws_forecast_sigma_f(7)


# --- Minimum sigma floor ---

def test_resolver_sigma_clamped_to_minimum() -> None:
    ctx = SigmaContext(
        station="KBOS", season_bucket="JJA",
        sigma_params={("KBOS", "JJA"): _db_params(n=200, crps_improvement=0.05, sigma=0.001)},
    )
    mapping = _FakeMapping()
    result = sigma_f_for_mapping(mapping, month=7, ctx=ctx)
    assert result >= 0.5
