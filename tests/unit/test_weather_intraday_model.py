from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.weather_prediction import IntradayTrainingRow, WeatherPredictionService
from kalshi_bot.weather.intraday import IntradayFeatureInput, intraday_feature_values


def _settings(**overrides) -> Settings:
    values = {
        "database_url": "sqlite+aiosqlite:///./test.db",
        "weather_intraday_min_train_rows": 50,
        "weather_intraday_min_holdout_rows": 10,
        "weather_intraday_min_brier_improvement_pct": 0.01,
        "weather_intraday_min_calibration_bucket_rows": 999,
        "weather_intraday_min_series_holdout_rows": 999,
    }
    values.update(overrides)
    return Settings(**values)


def _row(day: int, hour: int, *, outcome_yes: int) -> IntradayTrainingRow:
    market_day = f"2026-05-{day:02d}"
    observed_high = 88.0 if outcome_yes else 76.0
    return IntradayTrainingRow(
        market_ticker=f"KXHIGHNY-26MAY{day:02d}-T82",
        series_ticker="KXHIGHNY",
        station_id="KNYC",
        local_market_day=market_day,
        asof_ts=datetime(2026, 5, day, hour, tzinfo=UTC),
        settlement_ts=datetime(2026, 5, day + 1, 12, tzinfo=UTC),
        forecast_updated_ts=datetime(2026, 5, day, 10, tzinfo=UTC),
        source_kind="archived_weather_bundle",
        threshold_f=82.0,
        operator=">",
        outcome_yes=outcome_yes,
        forecast_high_f=82.0,
        current_temp_f=observed_high,
        observed_high_so_far_f=observed_high,
        source_disagreement_f=0.0,
        timezone_name="America/New_York",
        payload={},
    )


def test_intraday_features_use_dst_safe_local_hour() -> None:
    values = intraday_feature_values(
        IntradayFeatureInput(
            market_ticker="KXHIGHLAX-26MAR08-T70",
            series_ticker="KXHIGHLAX",
            station_id="KLAX",
            timezone_name="America/Los_Angeles",
            local_market_day="2026-03-08",
            threshold_f=70.0,
            operator=">",
            asof_ts=datetime(2026, 3, 8, 10, 30, tzinfo=UTC),
            forecast_updated_ts=None,
            settlement_ts=None,
            forecast_high_f=72.0,
            current_temp_f=61.0,
            observed_high_so_far_f=61.0,
            source_disagreement_f=0.0,
        )
    )

    assert values["local_hour"] == pytest.approx(3.5)
    assert values["heating_hours_remaining"] > 0


def test_intraday_training_persists_only_passing_artifact_shape() -> None:
    rows = []
    for day in range(1, 11):
        outcome = 1 if day % 2 else 0
        for hour in range(12, 22):
            rows.append(_row(day, hour, outcome_yes=outcome))
    service = WeatherPredictionService(_settings(), None)  # type: ignore[arg-type]

    artifact = service._fit_intraday_artifact(rows, version="test")

    assert artifact["active"] is True
    assert artifact["reason"] == "gates_passed"
    assert artifact["model_type"] == "weather_intraday_logistic_v1"
    assert artifact["metrics"]["model_brier"] < artifact["metrics"]["baseline_brier"]
    assert artifact["feature_names"]
    assert len(artifact["coefficients"]) == len(artifact["feature_names"])


def test_intraday_training_rejects_weak_holdout_gate() -> None:
    rows = []
    for day in range(1, 11):
        outcome = 1 if day % 2 else 0
        for hour in range(12, 22):
            rows.append(_row(day, hour, outcome_yes=outcome))
    service = WeatherPredictionService(
        _settings(weather_intraday_min_brier_improvement_pct=1.01),
        None,  # type: ignore[arg-type]
    )

    artifact = service._fit_intraday_artifact(rows, version="test")

    assert artifact["active"] is False
    assert artifact["reason"] == "gates_not_passed"
    assert artifact["gates"]["brier_improvement"]["passed"] is False
