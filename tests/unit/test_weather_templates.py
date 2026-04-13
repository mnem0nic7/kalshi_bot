from pathlib import Path

from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherSeriesTemplate


def test_weather_series_template_resolves_greater_and_less_markets() -> None:
    template = WeatherSeriesTemplate(
        series_ticker="KXHIGHNY",
        display_name="NYC Daily High",
        location_name="New York City",
        station_id="KNYC",
        latitude=40.7146,
        longitude=-74.0071,
    )

    greater_mapping = template.resolve_market(
        {
            "ticker": "KXHIGHNY-26APR11-T68",
            "title": "Will the **high temp in NYC** be >68° on Apr 11, 2026?",
            "subtitle": "69° or above",
            "strike_type": "greater",
            "floor_strike": 68,
        }
    )
    less_mapping = template.resolve_market(
        {
            "ticker": "KXHIGHNY-26APR11-T61",
            "title": "Will the **high temp in NYC** be <61° on Apr 11, 2026?",
            "subtitle": "60° or below",
            "strike_type": "less",
            "cap_strike": 61,
        }
    )

    assert greater_mapping is not None
    assert greater_mapping.market_ticker == "KXHIGHNY-26APR11-T68"
    assert greater_mapping.operator == ">"
    assert greater_mapping.threshold_f == 68
    assert greater_mapping.display_name == "Will the high temp in NYC be >68° on Apr 11, 2026?"
    assert greater_mapping.series_ticker == "KXHIGHNY"

    assert less_mapping is not None
    assert less_mapping.operator == "<"
    assert less_mapping.threshold_f == 61


def test_weather_directory_supports_and_resolves_series_template_market() -> None:
    template = WeatherSeriesTemplate(
        series_ticker="KXHIGHCHI",
        location_name="Chicago",
        station_id="KMDW",
        latitude=41.7868,
        longitude=-87.7522,
    )
    directory = WeatherMarketDirectory({}, {"KXHIGHCHI": template})

    assert directory.supports_market_ticker("KXHIGHCHI-26APR11-T58")
    assert not directory.supports_market_ticker("UNRELATED-26APR11-T58")

    resolved = directory.resolve_market(
        "KXHIGHCHI-26APR11-T58",
        {
            "ticker": "KXHIGHCHI-26APR11-T58",
            "title": "Will the high temp in Chicago be >58° on Apr 11, 2026?",
            "subtitle": "59° or above",
            "strike_type": "greater",
            "floor_strike": 58,
        },
    )

    assert resolved is not None
    assert resolved.market_ticker == "KXHIGHCHI-26APR11-T58"
    assert resolved.station_id == "KMDW"


def test_example_weather_market_map_covers_all_current_high_temp_series() -> None:
    directory = WeatherMarketDirectory.from_file(Path("docs/examples/weather_markets.example.yaml"))
    series = {template.series_ticker for template in directory.templates()}

    assert series == {
        "KXHIGHAUS",
        "KXHIGHCHI",
        "KXHIGHDEN",
        "KXHIGHLAX",
        "KXHIGHMIA",
        "KXHIGHNY",
        "KXHIGHPHIL",
        "KXHIGHTATL",
        "KXHIGHTBOS",
        "KXHIGHTDAL",
        "KXHIGHTDC",
        "KXHIGHTHOU",
        "KXHIGHTLV",
        "KXHIGHTMIN",
        "KXHIGHTNOLA",
        "KXHIGHTOKC",
        "KXHIGHTPHX",
        "KXHIGHTSATX",
        "KXHIGHTSEA",
        "KXHIGHTSFO",
    }

    resolved = directory.resolve_market(
        "KXHIGHTSEA-26APR11-T58",
        {
            "ticker": "KXHIGHTSEA-26APR11-T58",
            "title": "Will the maximum temperature be >58 on Apr 11, 2026?",
            "subtitle": "59 or above",
            "strike_type": "greater",
            "floor_strike": 58,
        },
    )

    assert resolved is not None
    assert resolved.station_id == "KSEA"
    assert resolved.daily_summary_station_id == "USW00024233"
