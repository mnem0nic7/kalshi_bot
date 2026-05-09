from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import score_weather_market


def _mapping(*, threshold: float = 82.0) -> WeatherMarketMapping:
    return WeatherMarketMapping(
        market_ticker="KXHIGHNY-26MAY09-T82",
        station_id="KNYC",
        series_ticker="KXHIGHNY",
        location_name="New York City",
        timezone_name="America/New_York",
        latitude=40.7,
        longitude=-74.0,
        threshold_f=threshold,
        operator=">",
    )


def _forecast(high_f: int = 80) -> dict:
    return {
        "properties": {
            "updated": "2026-05-09T10:00:00+00:00",
            "periods": [
                {
                    "isDaytime": True,
                    "startTime": "2026-05-09T06:00:00-04:00",
                    "temperature": high_f,
                    "temperatureUnit": "F",
                }
            ],
        }
    }


def _grid(high_c: float = 29.4) -> dict:
    return {
        "properties": {
            "maxTemperature": {
                "uom": "wmoUnit:degC",
                "values": [
                    {
                        "validTime": "2026-05-09T06:00:00+00:00/PT24H",
                        "value": high_c,
                    }
                ],
            }
        }
    }


def _observation(temp_c: float = 20.0) -> dict:
    return {
        "properties": {
            "timestamp": "2026-05-09T14:00:00+00:00",
            "temperature": {"value": temp_c},
        }
    }


def test_source_ensemble_sets_forecast_and_provenance() -> None:
    signal = score_weather_market(
        _mapping(),
        _forecast(high_f=80),
        _observation(),
        forecast_grid_payload=_grid(high_c=29.4),
        prediction_enabled=True,
        source_ensemble_enabled=True,
    )

    assert signal.prediction_provenance is not None
    assert signal.prediction_provenance["ensemble"]["status"] == "ok"
    assert set(signal.prediction_provenance["ensemble"]["source_set"]) == {"nws_daily_period", "nws_gridpoint"}
    assert signal.source_disagreement_f == pytest.approx(4.92, abs=0.1)
    assert 82.0 < signal.forecast_high_f < 83.5


def test_source_disagreement_can_stand_down() -> None:
    signal = score_weather_market(
        _mapping(),
        _forecast(high_f=70),
        _observation(),
        forecast_grid_payload=_grid(high_c=35.0),
        prediction_enabled=True,
        source_ensemble_enabled=True,
        source_disagreement_stand_down_f=5.0,
    )

    assert signal.stand_down_reason == StandDownReason.FORECAST_SOURCE_DISAGREEMENT
    assert signal.fair_yes_dollars == Decimal("0.5000")
    assert signal.confidence == 0.0


def test_nowcast_high_so_far_is_lower_bound_for_forecast() -> None:
    signal = score_weather_market(
        _mapping(threshold=90),
        _forecast(high_f=80),
        _observation(temp_c=29.4),
        forecast_grid_payload={},
        prediction_enabled=True,
        nowcast_high_so_far_enabled=True,
    )

    assert signal.observed_high_so_far_f == pytest.approx(84.92, abs=0.1)
    assert signal.forecast_high_f == pytest.approx(signal.observed_high_so_far_f, abs=0.1)


def test_explicit_observed_high_so_far_bounds_future_observation_bundle() -> None:
    signal = score_weather_market(
        _mapping(threshold=90),
        _forecast(high_f=80),
        _observation(temp_c=20.0),
        forecast_grid_payload={},
        prediction_enabled=True,
        nowcast_high_so_far_enabled=True,
        observed_high_so_far_f=86.0,
    )

    assert signal.forecast_high_f == pytest.approx(86.0)
    assert signal.prediction_provenance["observed_high_so_far_f"] == pytest.approx(86.0)


def test_active_residual_model_adjusts_forecast() -> None:
    model = {
        "active": True,
        "version": "test",
        "feature_names": ["station:KNYC"],
        "coefficients": [2.0],
        "intercept": 0.0,
        "max_abs_adjustment_f": 5.0,
    }
    signal = score_weather_market(
        _mapping(threshold=85),
        _forecast(high_f=80),
        _observation(),
        forecast_grid_payload={},
        prediction_enabled=True,
        residual_model=model,
    )

    assert signal.residual_adjustment_f == pytest.approx(2.0)
    assert signal.forecast_high_f == pytest.approx(82.0)
    assert signal.prediction_provenance["residual_model"]["status"] == "applied"


def test_signal_engine_carries_prediction_provenance() -> None:
    engine = WeatherSignalEngine(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            weather_prediction_enabled=True,
            risk_min_edge_bps=50,
        )
    )
    signal = engine.evaluate(
        _mapping(),
        {"market": {"yes_bid_dollars": "0.4000", "yes_ask_dollars": "0.4300", "no_ask_dollars": "0.5700"}},
        {
            "forecast": _forecast(high_f=80),
            "forecast_grid": _grid(high_c=29.4),
            "observation": _observation(),
            "forecast_archive": {
                "provider": "open_meteo_forecast_archive",
                "forecast_high_f": 83,
                "run_ts": "2026-05-09T10:00:00+00:00",
            },
            "observed_high_so_far_f": 81.0,
        },
    )

    assert signal.prediction_provenance["prediction_enabled"] is True
    assert signal.prediction_provenance["ensemble"]["status"] == "ok"
    assert "open_meteo_forecast_archive" in signal.prediction_provenance["ensemble"]["source_set"]
    assert signal.weather.observed_high_so_far_f == pytest.approx(81.0)
