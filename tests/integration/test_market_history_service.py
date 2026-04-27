from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MarketPriceHistory
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.market_history import MarketHistoryService


class FakeDiscoveryService:
    async def list_stream_markets(self) -> list[str]:
        return ["WX-DISCOVERED"]


class FakeKalshiClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.requests: list[str] = []

    async def get_market(self, ticker: str) -> dict:
        self.requests.append(ticker)
        return {
            "market": {
                "ticker": ticker,
                "yes_bid_dollars": "0.4000",
                "yes_ask_dollars": "0.5000",
                "last_price_dollars": "0.4500",
                "volume": 123,
            }
        }


@pytest.mark.asyncio
async def test_market_history_snapshots_open_position_markets_even_when_discovery_rolls_forward(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/market-history-open-position.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.upsert_position(
            market_ticker="WX-HELD",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="no",
            count_fp=Decimal("2.00"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        await session.commit()

    kalshi = FakeKalshiClient(settings)
    service = MarketHistoryService(
        session_factory,
        kalshi,  # type: ignore[arg-type]
        FakeDiscoveryService(),  # type: ignore[arg-type]
        retention_hours=24,
    )

    written = await service.snapshot_once()

    async with session_factory() as session:
        rows = list((await session.execute(select(MarketPriceHistory))).scalars())

    assert written == 2
    assert kalshi.requests == ["WX-DISCOVERED", "WX-HELD"]
    assert {row.market_ticker for row in rows} == {"WX-DISCOVERED", "WX-HELD"}

    await engine.dispose()
