from __future__ import annotations

import asyncio

import aiosqlite
import pytest
from sqlalchemy import text

from kalshi_bot.config import Settings
from kalshi_bot.db.session import create_engine


@pytest.mark.asyncio
async def test_aiosqlite_file_connect_execute_and_close_completes(tmp_path) -> None:
    db = await asyncio.wait_for(aiosqlite.connect(tmp_path / "driver.db"), timeout=2)
    try:
        cursor = await asyncio.wait_for(db.execute("select 1"), timeout=2)
        try:
            row = await asyncio.wait_for(cursor.fetchone(), timeout=2)
        finally:
            await asyncio.wait_for(cursor.close(), timeout=2)
        assert row[0] == 1
    finally:
        await asyncio.wait_for(db.close(), timeout=2)


@pytest.mark.asyncio
async def test_sqlalchemy_aiosqlite_file_engine_executes(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'sqlalchemy.db'}")
    engine = create_engine(settings)
    try:
        async with engine.connect() as conn:
            result = await asyncio.wait_for(conn.execute(text("select 1")), timeout=5)
            assert result.scalar_one() == 1
    finally:
        await engine.dispose()
