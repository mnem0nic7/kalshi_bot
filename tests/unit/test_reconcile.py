from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.reconcile import ReconciliationService


class FakeKalshi:
    async def get_historical_cutoff(self):
        return {"cutoff": "ok"}

    async def get_balance(self):
        return {"balance": "100.00"}

    async def get_positions(self, *, subaccount: int = 0):
        return {
            "market_positions": [
                {
                    "ticker": "KXHIGHNY-26APR24-T67",
                    "position_fp": "3.00",
                    "side": "yes",
                    "average_price_dollars": "0.4500",
                    "subaccount": subaccount,
                }
            ]
        }

    async def get_orders(self):
        return {"orders": []}

    async def get_fills(self):
        return {"fills": []}

    async def get_settlements(self):
        return {"settlements": []}


@pytest.mark.asyncio
async def test_reconcile_checkpoint_records_live_tickers_and_time(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            summary = await ReconciliationService(FakeKalshi(), settings).reconcile(
                repo,
                subaccount=0,
                kalshi_env="demo",
            )
            await session.commit()

            checkpoint = await repo.get_checkpoint("reconcile:demo")

        assert summary.live_tickers == ["KXHIGHNY-26APR24-T67"]
        assert summary.reconciled_at
        assert summary.positions_count == 1
        assert checkpoint is not None
        assert checkpoint.payload["live_tickers"] == ["KXHIGHNY-26APR24-T67"]
        assert checkpoint.payload["reconciled_at"]
    finally:
        await engine.dispose()


class CryptoFillKalshi(FakeKalshi):
    """Models the order-write race: a crypto buy fill arrives with no pre-attributed
    order, so attribution must come deterministically from the ticker."""

    async def get_positions(self, *, subaccount: int = 0):
        return {"market_positions": []}

    async def get_orders(self):
        return {"orders": []}

    async def get_fills(self):
        return {
            "fills": [
                {
                    "market_ticker": "KXBTC15M-26MAY251800-00",
                    "side": "no",
                    "action": "buy",
                    "yes_price_dollars": "0.4000",
                    "count_fp": "5.00",
                    "trade_id": "trade-crypto-1",
                    "is_taker": True,
                }
            ]
        }


@pytest.mark.asyncio
async def test_reconcile_attributes_crypto_buy_fill_from_ticker(tmp_path) -> None:
    from sqlalchemy import select

    from kalshi_bot.core.enums import StrategyCode
    from kalshi_bot.db.models import FillRecord

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile_crypto.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            await ReconciliationService(CryptoFillKalshi(), settings).reconcile(
                repo,
                subaccount=0,
                kalshi_env="demo",
            )
            await session.commit()
            fill = (
                await session.execute(select(FillRecord).where(FillRecord.trade_id == "trade-crypto-1"))
            ).scalar_one()
        assert fill.strategy_code == StrategyCode.CRYPTO_15M.value
    finally:
        await engine.dispose()


class CutoffUnavailableKalshi(FakeKalshi):
    async def get_historical_cutoff(self):
        raise RuntimeError("historical cutoff unavailable")


@pytest.mark.asyncio
async def test_reconcile_continues_when_historical_cutoff_is_unavailable(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/reconcile_cutoff.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            summary = await ReconciliationService(CutoffUnavailableKalshi(), settings).reconcile(
                repo,
                subaccount=0,
                kalshi_env="demo",
            )
            await session.commit()

            checkpoint = await repo.get_checkpoint("reconcile:demo")

        assert summary.historical_cutoff_seen is False
        assert summary.live_tickers == ["KXHIGHNY-26APR24-T67"]
        assert checkpoint is not None
        assert checkpoint.payload["historical_cutoff"]["unavailable"] is True
        assert checkpoint.payload["live_tickers"] == ["KXHIGHNY-26APR24-T67"]
    finally:
        await engine.dispose()
