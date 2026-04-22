from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.reconcile import ReconciliationService


class FakeKalshiForReconcile:
    async def get_historical_cutoff(self) -> dict:
        return {"cutoff_ts": 1234567890}

    async def get_balance(self) -> dict:
        return {"balance": 10000, "portfolio_value": 10250}

    async def get_positions(self, **params) -> dict:
        return {
            "market_positions": [
                {
                    "market_ticker": "WX-TEST",
                    "subaccount": 0,
                    "side": "yes",
                    "position_fp": "12.00",
                    "average_price_dollars": "0.5400",
                }
            ]
        }

    async def get_orders(self, **params) -> dict:
        return {
            "orders": [
                {
                    "order_id": "ord-1",
                    "client_order_id": "client-1",
                    "market_ticker": "WX-TEST",
                    "status": "resting",
                    "side": "yes",
                    "action": "buy",
                    "yes_price_dollars": "0.5500",
                    "count_fp": "4.00",
                }
            ]
        }

    async def get_fills(self, **params) -> dict:
        return {
            "fills": [
                {
                    "trade_id": "trade-1",
                    "market_ticker": "WX-TEST",
                    "side": "yes",
                    "action": "buy",
                    "yes_price_dollars": "0.5500",
                    "count_fp": "2.00",
                    "is_taker": True,
                }
            ]
        }

    async def get_settlements(self, **params) -> dict:
        return {"settlements": [{"market_ticker": "OLD-WX", "realized_pnl_dollars": "3.2500"}]}


class FakeKalshiNegativePosition(FakeKalshiForReconcile):
    async def get_positions(self, **params) -> dict:
        return {
            "market_positions": [
                {
                    "ticker": "WX-NO",
                    "subaccount": 0,
                    "position_fp": "-24.00",
                    "market_exposure_dollars": "13.440000",
                }
            ]
        }


@pytest.mark.asyncio
async def test_reconciliation_service_persists_exchange_state(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    service = ReconciliationService(FakeKalshiForReconcile())  # type: ignore[arg-type]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        summary = await service.reconcile(repo, kalshi_env="demo")
        positions = await repo.list_positions()
        checkpoint = await repo.get_checkpoint("reconcile:demo")
        await session.commit()

    assert summary.positions_count == 1
    assert summary.orders_count == 1
    assert summary.fills_count == 1
    assert checkpoint is not None
    assert checkpoint.payload["orders_count"] == 1
    assert positions[0].market_ticker == "WX-TEST"

    await engine.dispose()


@pytest.mark.asyncio
async def test_reconciliation_service_normalizes_negative_positions_to_no_side(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile-negative.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    service = ReconciliationService(FakeKalshiNegativePosition())  # type: ignore[arg-type]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        summary = await service.reconcile(repo)
        positions = await repo.list_positions()
        await session.commit()

    assert summary.positions_count == 1
    assert len(positions) == 1
    assert positions[0].market_ticker == "WX-NO"
    assert positions[0].side == "no"
    assert str(positions[0].count_fp) == "24.00"
    assert str(positions[0].average_price_dollars) == "0.5600"

    await engine.dispose()


@pytest.mark.asyncio
async def test_reconciliation_service_persists_position_environment(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile-env.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    service = ReconciliationService(FakeKalshiForReconcile())  # type: ignore[arg-type]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await service.reconcile(repo, kalshi_env="demo")
        positions = await repo.list_positions(kalshi_env="demo")
        await session.commit()

    assert len(positions) == 1
    assert positions[0].kalshi_env == "demo"

    await engine.dispose()
