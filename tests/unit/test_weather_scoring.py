from decimal import Decimal

from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import extract_forecast_high_f, score_weather_market


def test_score_weather_market_above_threshold_is_bullish() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-TEST",
        station_id="KNYC",
        location_name="NYC",
        latitude=40.0,
        longitude=-73.0,
        threshold_f=80,
    )
    forecast = {
        "properties": {
            "updated": "2026-04-10T00:00:00+00:00",
            "periods": [
                {"isDaytime": True, "temperature": 86, "temperatureUnit": "F"},
                {"isDaytime": False, "temperature": 71, "temperatureUnit": "F"},
            ],
        }
    }
    observation = {"properties": {"temperature": {"value": 25.0}, "timestamp": "2026-04-10T01:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.forecast_high_f == 86
    assert signal.current_temp_f is not None
    assert signal.fair_yes_dollars > 0.5
    assert signal.confidence > 0.45
    assert signal.resolution_state == WeatherResolutionState.UNRESOLVED


def test_score_weather_market_locks_yes_when_above_threshold_is_already_met() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-LOCK-YES",
        station_id="KNYC",
        location_name="NYC",
        latitude=40.0,
        longitude=-73.0,
        threshold_f=80,
        operator=">",
    )
    forecast = {"properties": {"updated": "2026-04-10T00:00:00+00:00", "periods": []}}
    observation = {"properties": {"temperature": {"value": 27.0}, "timestamp": "2026-04-10T18:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.fair_yes_dollars == 1
    assert signal.confidence == 1.0
    assert signal.resolution_state == WeatherResolutionState.LOCKED_YES


def test_score_weather_market_locks_no_when_below_threshold_is_already_breached() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-LOCK-NO",
        station_id="KORD",
        location_name="Chicago",
        latitude=41.0,
        longitude=-87.0,
        threshold_f=51,
        operator="<",
    )
    forecast = {
        "properties": {
            "updated": "2026-04-10T00:00:00+00:00",
            "periods": [{"isDaytime": True, "temperature": 80, "temperatureUnit": "F"}],
        }
    }
    observation = {"properties": {"temperature": {"value": 11.0}, "timestamp": "2026-04-10T18:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.fair_yes_dollars == 0
    assert signal.confidence == 1.0
    assert signal.resolution_state == WeatherResolutionState.LOCKED_NO


def test_score_weather_market_marks_near_threshold_regime_and_shrinks_edge() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-NEAR",
        station_id="KSEA",
        location_name="Seattle",
        latitude=47.0,
        longitude=-122.0,
        threshold_f=70,
        operator=">",
    )
    forecast = {
        "properties": {
            "updated": "2026-04-10T00:00:00+00:00",
            "periods": [{"isDaytime": True, "temperature": 72, "temperatureUnit": "F"}],
        }
    }
    observation = {"properties": {"temperature": {"value": 16.0}, "timestamp": "2026-04-10T18:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.forecast_delta_f == 2.0
    assert signal.trade_regime == "near_threshold"
    assert signal.fair_yes_dollars == Decimal("0.5593")


def test_score_weather_market_penalizes_longshot_yes_setup() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-LONG-YES",
        station_id="KDCA",
        location_name="Washington",
        latitude=38.0,
        longitude=-77.0,
        threshold_f=92,
        operator=">",
    )
    forecast = {
        "properties": {
            "updated": "2026-04-10T00:00:00+00:00",
            "periods": [{"isDaytime": True, "temperature": 83, "temperatureUnit": "F"}],
        }
    }
    observation = {"properties": {"temperature": {"value": 18.0}, "timestamp": "2026-04-10T18:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.trade_regime == "longshot_yes"
    assert signal.fair_yes_dollars == Decimal("0.0560")


def test_score_weather_market_penalizes_longshot_no_setup() -> None:
    mapping = WeatherMarketMapping(
        market_ticker="WX-LONG-NO",
        station_id="KLAS",
        location_name="Las Vegas",
        latitude=36.0,
        longitude=-115.0,
        threshold_f=73,
        operator="<",
    )
    forecast = {
        "properties": {
            "updated": "2026-04-10T00:00:00+00:00",
            "periods": [{"isDaytime": True, "temperature": 62, "temperatureUnit": "F"}],
        }
    }
    observation = {"properties": {"temperature": {"value": 18.0}, "timestamp": "2026-04-10T18:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.trade_regime == "longshot_no"
    assert signal.fair_yes_dollars == Decimal("0.9736")


def test_extract_forecast_high_f_returns_target_date_not_hottest_day() -> None:
    """Regression: must return today's period, not max across the full 7-day forecast."""
    forecast = {
        "properties": {
            "periods": [
                {
                    "isDaytime": True,
                    "startTime": "2026-04-21T06:00:00-04:00",
                    "temperature": 47,
                    "temperatureUnit": "F",
                },
                {
                    "isDaytime": False,
                    "startTime": "2026-04-21T18:00:00-04:00",
                    "temperature": 42,
                    "temperatureUnit": "F",
                },
                {
                    "isDaytime": True,
                    "startTime": "2026-04-22T06:00:00-04:00",
                    "temperature": 59,
                    "temperatureUnit": "F",
                },
            ]
        }
    }
    from datetime import date

    result = extract_forecast_high_f(forecast, target_date=date(2026, 4, 21))
    assert result == 47, f"Expected today's 47°F, got {result}"


def test_score_weather_market_uses_observation_date_not_hottest_period() -> None:
    """Integration: score_weather_market must use today's forecast, not tomorrow's."""
    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHTBOS-26APR21-T55",
        station_id="KBOS",
        location_name="Boston",
        latitude=42.4,
        longitude=-71.0,
        threshold_f=55,
        operator=">",
    )
    # Forecast has today (Apr 21) at 47°F and tomorrow at 59°F.
    forecast = {
        "properties": {
            "updated": "2026-04-21T12:00:00+00:00",
            "periods": [
                {
                    "isDaytime": True,
                    "startTime": "2026-04-21T06:00:00-04:00",
                    "temperature": 47,
                    "temperatureUnit": "F",
                },
                {
                    "isDaytime": True,
                    "startTime": "2026-04-22T06:00:00-04:00",
                    "temperature": 59,
                    "temperatureUnit": "F",
                },
            ],
        }
    }
    observation = {
        "properties": {
            "temperature": {"value": 8.0},  # ~46°F
            "timestamp": "2026-04-21T17:00:00+00:00",
        }
    }
    signal = score_weather_market(mapping, forecast, observation)

    assert signal.forecast_high_f == 47, f"Expected 47°F (today), got {signal.forecast_high_f}"
    # With today's 47°F (8°F below 55°F threshold), fair_yes should be very low — not near 0.76.
    assert signal.fair_yes_dollars < Decimal("0.15")
    # The buggy version would have used tomorrow's 59°F and returned fair_yes ≈ 0.76.
    assert signal.fair_yes_dollars < Decimal("0.50")
