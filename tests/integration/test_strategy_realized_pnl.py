from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.mark.asyncio
async def test_strategy_daily_realized_pnl_includes_fees(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/realized-pnl.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.upsert_fill(
            market_ticker="KXHIGHCHI-26MAY13-T60",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.40"),
            count_fp=Decimal("10.00"),
            raw={"fee_cost": "0.10"},
            trade_id="buy-1",
            kalshi_env="production",
            strategy_code="A",
        )
        await repo.upsert_fill(
            market_ticker="KXHIGHCHI-26MAY13-T60",
            side="yes",
            action="sell",
            yes_price_dollars=Decimal("0.45"),
            count_fp=Decimal("10.00"),
            raw={"fee_cost": "0.20"},
            trade_id="sell-1",
            kalshi_env="production",
            strategy_code="A",
        )
        pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code="A",
            kalshi_env="production",
        )
        await session.commit()

    assert pnl == Decimal("0.20")
    await engine.dispose()
