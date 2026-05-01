from __future__ import annotations

import httpx
import pytest

from kalshi_bot.config import Settings
from kalshi_bot.integrations.kalshi import KalshiClient


@pytest.mark.asyncio
async def test_public_market_data_reads_do_not_require_credentials() -> None:
    seen_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert "KALSHI-ACCESS-KEY" not in request.headers
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"series": []})

    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        kalshi_read_api_key_id=None,
        kalshi_read_private_key_path=None,
        kalshi_write_api_key_id=None,
        kalshi_write_private_key_path=None,
        demo_kalshi_api_key=None,
        live_kalshi_api_key=None,
    )
    client = KalshiClient(settings)
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=settings.kalshi_rest_base_url)

    response = await client.list_series(category="Crypto")

    assert response == {"series": []}
    assert seen_paths == ["/trade-api/v2/series"]
    await client.close()


@pytest.mark.asyncio
async def test_historical_candlestick_endpoint_uses_ticker_only_path() -> None:
    seen_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"candlesticks": []})

    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        kalshi_read_api_key_id=None,
        kalshi_read_private_key_path=None,
        demo_kalshi_api_key=None,
        live_kalshi_api_key=None,
    )
    client = KalshiClient(settings)
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=settings.kalshi_rest_base_url)

    await client.get_historical_market_candlesticks("KXBTC15M", "KXBTC15M-TEST", period_interval=1)

    assert seen_paths == ["/trade-api/v2/historical/markets/KXBTC15M-TEST/candlesticks"]
    await client.close()
