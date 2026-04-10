from __future__ import annotations

import pytest

from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherSeriesTemplate


class FakeKalshi:
    async def list_markets(self, **params):
        assert params["series_ticker"] == "KXHIGHNY"
        return {
            "markets": [
                {
                    "ticker": "KXHIGHNY-26APR11-T68",
                    "title": "Will the high temp in NYC be >68° on Apr 11, 2026?",
                    "subtitle": "69° or above",
                    "strike_type": "greater",
                    "floor_strike": 68,
                    "status": "active",
                    "yes_bid_dollars": "0.3500",
                    "yes_ask_dollars": "0.5600",
                    "no_ask_dollars": "0.6500",
                    "rules_primary": "NWS Climatological Report (Daily) for Central Park, New York.",
                },
                {
                    "ticker": "KXHIGHNY-26APR11-B67.5",
                    "title": "Will the high temp in NYC be 67-68° on Apr 11, 2026?",
                    "subtitle": "67° to 68°",
                    "strike_type": "between",
                    "floor_strike": 67,
                    "cap_strike": 68,
                    "status": "active",
                },
            ]
        }

    async def get_market(self, ticker: str):
        raise AssertionError(f"unexpected exact market lookup for {ticker}")


@pytest.mark.asyncio
async def test_discovery_service_expands_series_templates_into_tradeable_markets() -> None:
    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHNY": WeatherSeriesTemplate(
                series_ticker="KXHIGHNY",
                display_name="NYC Daily High Temperature",
                location_name="New York City",
                station_id="KNYC",
                latitude=40.7146,
                longitude=-74.0071,
            )
        },
    )
    service = DiscoveryService(FakeKalshi(), directory)  # type: ignore[arg-type]

    discoveries = await service.discover_configured_markets()
    stream_markets = await service.list_stream_markets()

    assert len(discoveries) == 1
    assert discoveries[0].mapping.market_ticker == "KXHIGHNY-26APR11-T68"
    assert discoveries[0].mapping.operator == ">"
    assert discoveries[0].can_trade
    assert stream_markets == ["KXHIGHNY-26APR11-T68"]
