from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import score_weather_market


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
