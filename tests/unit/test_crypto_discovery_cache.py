"""Discovery TTL cache: the no-sleep active autonomy loop re-discovers series and
markets every iteration; uncached that was ~1,900 Kalshi requests/minute of
identical listings, starving live quote polls and nightly crawls behind 429s."""
from __future__ import annotations

from types import SimpleNamespace

from kalshi_bot.crypto.services import CryptoMarketService


class _FakeKalshi:
    def __init__(self) -> None:
        self.series_calls = 0
        self.market_calls = 0

    async def list_series(self, **params):
        self.series_calls += 1
        return {
            "series": [
                {
                    "ticker": "KXBTC15M",
                    "title": "BTC 15 minute",
                    "category": "Crypto",
                    "frequency": "15m",
                },
            ],
            "cursor": None,
        }

    async def list_markets(self, **params):
        self.market_calls += 1
        return {"markets": [], "cursor": None}


def _service(ttl: float) -> tuple[CryptoMarketService, _FakeKalshi]:
    kalshi = _FakeKalshi()
    settings = SimpleNamespace(
        crypto_enabled=True,
        kalshi_env="production",
        crypto_market_discovery_cache_seconds=ttl,
    )
    service = CryptoMarketService(
        settings=settings,  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        kalshi=kalshi,  # type: ignore[arg-type]
        agent_pack_service=None,  # type: ignore[arg-type]
        asset_control_service=None,  # type: ignore[arg-type]
    )
    return service, kalshi


async def test_series_discovery_cached_within_ttl() -> None:
    service, kalshi = _service(ttl=60.0)

    first = await service.discover_series(frequency="15m")
    second = await service.discover_series(frequency="15m")

    assert kalshi.series_calls == 1
    assert [s.series_ticker for s in second] == [s.series_ticker for s in first]


async def test_series_cache_disabled_with_zero_ttl() -> None:
    service, kalshi = _service(ttl=0.0)

    await service.discover_series(frequency="15m")
    await service.discover_series(frequency="15m")

    assert kalshi.series_calls == 2


async def test_market_discovery_cached_within_ttl() -> None:
    service, kalshi = _service(ttl=60.0)

    await service.discover_markets(frequency="15m", status="open", persist=False)
    await service.discover_markets(frequency="15m", status="open", persist=False)

    assert kalshi.series_calls == 1
    assert kalshi.market_calls == 1


async def test_market_cache_key_separates_status_and_assets() -> None:
    service, kalshi = _service(ttl=60.0)

    await service.discover_markets(frequency="15m", status="open", persist=False)
    await service.discover_markets(frequency="15m", status="settled", persist=False)
    await service.discover_markets(frequency="15m", status="open", persist=False, asset_symbols=["BTC"])

    assert kalshi.market_calls == 3


async def test_market_cache_bypassed_for_close_time_windows() -> None:
    from datetime import UTC, datetime

    service, kalshi = _service(ttl=60.0)
    bound = datetime.now(UTC)

    await service.discover_markets(frequency="15m", status="open", persist=False, min_close_time=bound)
    await service.discover_markets(frequency="15m", status="open", persist=False, min_close_time=bound)

    assert kalshi.market_calls == 2


async def test_cached_market_list_is_a_copy() -> None:
    service, kalshi = _service(ttl=60.0)

    first = await service.discover_markets(frequency="15m", status="open", persist=False)
    first.append("mutated")  # type: ignore[arg-type]
    second = await service.discover_markets(frequency="15m", status="open", persist=False)

    assert "mutated" not in second
