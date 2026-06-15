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


@pytest.mark.asyncio
async def test_crypto_pnl_reports_share_gross_fee_net_economics(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/crypto-pnl.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        fill = await repo.save_fill(
            market_ticker="KXBTC15M-26MAY13-B50000",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.40"),
            count_fp=Decimal("10.00"),
            raw={"taker_fees_dollars": "0.10"},
            trade_id="crypto-buy-1",
            is_taker=True,
            kalshi_env="production",
            strategy_code="CRYPTO_15M",
        )
        fill.settlement_result = "win"
        await session.flush()

        attribution = await repo.build_crypto_pnl_attribution_report(
            kalshi_env="production",
            days=7,
        )
        cell = await repo.get_crypto_live_pnl_cell_stats(
            kalshi_env="production",
            strategy_code="CRYPTO_15M",
            asset_symbol="BTC",
            frequency="15m",
            side="yes",
            contract_price_dollars=Decimal("0.40"),
            liquidity="taker",
            lookback_days=7,
        )
        await session.commit()

    assert attribution["totals"]["gross_pnl_dollars"] == "6.0000"
    assert attribution["totals"]["fees_dollars"] == "0.1000"
    assert attribution["totals"]["net_pnl_dollars"] == "5.9000"
    assert attribution["totals"]["missing_fee_count"] == 0
    assert attribution["totals"]["fee_sources"] == {"exchange_raw:taker_fees_dollars": 1}

    assert cell["gross_pnl_dollars"] == "6.0000"
    assert cell["fees_dollars"] == "0.1000"
    assert cell["net_pnl_dollars"] == "5.9000"
    assert cell["missing_fee_count"] == 0
    assert cell["fee_sources"] == {"exchange_raw:taker_fees_dollars": 1}
    await engine.dispose()
