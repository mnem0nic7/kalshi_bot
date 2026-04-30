from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from kalshi_bot.config import Settings
from kalshi_bot.db.aiosqlite_wakeup import install_aiosqlite_wakeup_patch
from kalshi_bot.db.base import Base
from kalshi_bot.db import models as _models  # noqa: F401

install_aiosqlite_wakeup_patch()


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_models(engine: AsyncEngine) -> None:
    # Import models before create_all so metadata is fully populated in standalone scripts.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        existing_strategy_columns = set(
            await conn.run_sync(lambda sync_conn: [column["name"] for column in inspect(sync_conn).get_columns("strategies")])
        )
        if "source" not in existing_strategy_columns:
            await conn.execute(text("ALTER TABLE strategies ADD COLUMN source VARCHAR(64) NOT NULL DEFAULT 'builtin'"))
        if "metadata" not in existing_strategy_columns:
            await conn.execute(text("ALTER TABLE strategies ADD COLUMN metadata JSON NOT NULL DEFAULT '{}'"))
        existing_codex_columns = set(
            await conn.run_sync(lambda sync_conn: [column["name"] for column in inspect(sync_conn).get_columns("strategy_codex_runs")])
        )
        if "trigger_source" not in existing_codex_columns:
            await conn.execute(text("ALTER TABLE strategy_codex_runs ADD COLUMN trigger_source VARCHAR(32) NOT NULL DEFAULT 'manual'"))
        existing_arb_columns = set(
            await conn.run_sync(lambda sync_conn: [column["name"] for column in inspect(sync_conn).get_columns("monotonicity_arb_proposals")])
        )
        arb_columns = {
            "pair_id": "VARCHAR(64)",
            "leg1_client_order_id": "VARCHAR(128)",
            "leg2_client_order_id": "VARCHAR(128)",
            "unwind_client_order_id": "VARCHAR(128)",
            "leg1_order_id": "VARCHAR(128)",
            "leg2_order_id": "VARCHAR(128)",
            "unwind_order_id": "VARCHAR(128)",
        }
        for column_name, column_type in arb_columns.items():
            if column_name not in existing_arb_columns:
                await conn.execute(text(f"ALTER TABLE monotonicity_arb_proposals ADD COLUMN {column_name} {column_type}"))
        if "execution_payload" not in existing_arb_columns:
            await conn.execute(text("ALTER TABLE monotonicity_arb_proposals ADD COLUMN execution_payload JSON NOT NULL DEFAULT '{}'"))


async def session_scope(factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with factory() as session:
        yield session
