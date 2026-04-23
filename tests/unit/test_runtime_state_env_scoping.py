from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.mark.asyncio
async def test_positions_and_market_state_are_scoped_by_env(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/runtime_state.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.upsert_position(
            market_ticker="KXHIGHNY-26APR21-T68",
            subaccount=0,
            kalshi_env="demo",
            side="yes",
            count_fp=Decimal("10.00"),
            average_price_dollars=Decimal("0.4200"),
            raw={"source": "demo"},
        )
        await repo.upsert_position(
            market_ticker="KXHIGHNY-26APR21-T68",
            subaccount=0,
            kalshi_env="production",
            side="no",
            count_fp=Decimal("4.00"),
            average_price_dollars=Decimal("0.6100"),
            raw={"source": "production"},
        )
        await repo.upsert_market_state(
            "KXHIGHNY-26APR21-T68",
            kalshi_env="demo",
            snapshot={"market_ticker": "KXHIGHNY-26APR21-T68", "source": "demo"},
            yes_bid_dollars=Decimal("0.4100"),
            yes_ask_dollars=Decimal("0.4300"),
            last_trade_dollars=Decimal("0.4200"),
        )
        await repo.upsert_market_state(
            "KXHIGHNY-26APR21-T68",
            kalshi_env="production",
            snapshot={"market_ticker": "KXHIGHNY-26APR21-T68", "source": "production"},
            yes_bid_dollars=Decimal("0.6100"),
            yes_ask_dollars=Decimal("0.6300"),
            last_trade_dollars=Decimal("0.6200"),
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        demo_positions = await repo.list_positions(limit=10, kalshi_env="demo")
        production_positions = await repo.list_positions(limit=10, kalshi_env="production")
        demo_market = await repo.get_market_state("KXHIGHNY-26APR21-T68", kalshi_env="demo")
        production_market = await repo.get_market_state("KXHIGHNY-26APR21-T68", kalshi_env="production")
        await session.commit()

    assert [position.side for position in demo_positions] == ["yes"]
    assert [position.side for position in production_positions] == ["no"]
    assert demo_market is not None and demo_market.snapshot["source"] == "demo"
    assert production_market is not None and production_market.snapshot["source"] == "production"
    await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_checkpoints_and_fill_metrics_are_env_scoped(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/runtime_metrics.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        now = datetime.now(UTC).isoformat()
        await repo.set_checkpoint(
            "reconcile:demo",
            None,
            {"balance": {"balance": 50000, "portfolio_value": 1200}, "reconciled_at": now},
        )
        await repo.set_checkpoint(
            "reconcile:production",
            None,
            {"balance": {"balance": 90000, "portfolio_value": 2500}, "reconciled_at": now},
        )
        await repo.set_daily_portfolio_baseline_dollars(
            Decimal("512.00"),
            kalshi_env="demo",
            pacific_date="2026-04-21",
        )
        await repo.set_daily_portfolio_baseline_dollars(
            Decimal("925.00"),
            kalshi_env="production",
            pacific_date="2026-04-21",
        )
        await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR21-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4200"),
            count_fp=Decimal("2.00"),
            raw={"source": "demo"},
            trade_id="demo-trade",
            kalshi_env="demo",
        )
        await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR21-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.6200"),
            count_fp=Decimal("3.00"),
            raw={"source": "production"},
            trade_id="prod-trade",
            kalshi_env="production",
        )
        await repo.settle_fills(
            [{"market_ticker": "KXHIGHNY-26APR21-T68", "market_result": "yes"}],
            kalshi_env="demo",
        )
        await repo.settle_fills(
            [{"market_ticker": "KXHIGHNY-26APR21-T68", "market_result": "no"}],
            kalshi_env="production",
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        demo_capital = await repo.get_total_capital_dollars(kalshi_env="demo")
        production_capital = await repo.get_total_capital_dollars(kalshi_env="production")
        demo_baseline = await repo.get_daily_portfolio_baseline_dollars(
            kalshi_env="demo",
            pacific_date="2026-04-21",
        )
        production_baseline = await repo.get_daily_portfolio_baseline_dollars(
            kalshi_env="production",
            pacific_date="2026-04-21",
        )
        demo_win_rate = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        production_win_rate = await repo.get_fill_win_rate_30d(kalshi_env="production")
        await session.commit()

    assert demo_capital == Decimal("512")
    assert production_capital == Decimal("925")
    assert demo_baseline == Decimal("512.00")
    assert production_baseline == Decimal("925.00")
    assert demo_win_rate["won_contracts"] == 2.0
    assert demo_win_rate["total_contracts"] == 2.0
    assert production_win_rate["won_contracts"] == 0
    assert production_win_rate["total_contracts"] == 3.0
    await engine.dispose()


@pytest.mark.asyncio
async def test_ops_events_are_filtered_by_env(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/runtime_events.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.log_ops_event(
            severity="warning",
            summary="Demo warning",
            source="watchdog",
            payload={"kalshi_env": "demo"},
            kalshi_env="demo",
        )
        await repo.log_ops_event(
            severity="error",
            summary="Production error",
            source="watchdog",
            payload={"kalshi_env": "production"},
            kalshi_env="production",
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        demo_events = await repo.list_ops_events(limit=10, kalshi_env="demo")
        production_events = await repo.list_ops_events(limit=10, kalshi_env="production")
        await session.commit()

    assert [event.summary for event in demo_events] == ["Demo warning"]
    assert [event.summary for event in production_events] == ["Production error"]
    await engine.dispose()
