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
    observation = {"properties": {"temperature": {"value": 28.0}, "timestamp": "2026-04-10T01:00:00+00:00"}}

    signal = score_weather_market(mapping, forecast, observation)

    assert signal.forecast_high_f == 86
    assert signal.current_temp_f is not None
    assert signal.fair_yes_dollars > 0.5
    assert signal.confidence > 0.45

