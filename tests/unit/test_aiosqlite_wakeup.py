from __future__ import annotations

import asyncio
import queue
import threading

import aiosqlite
import aiosqlite.core as aiosqlite_core
import pytest
from sqlalchemy import text

from kalshi_bot.config import Settings
from kalshi_bot.db.aiosqlite_wakeup import install_aiosqlite_wakeup_patch
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


def test_aiosqlite_worker_drops_late_result_when_loop_is_closed() -> None:
    install_aiosqlite_wakeup_patch()
    loop = asyncio.new_event_loop()
    future = loop.create_future()
    loop.close()

    tx: queue.Queue = queue.Queue()
    tx.put((future, lambda: 1))
    tx.put((None, lambda: aiosqlite_core._STOP_RUNNING_SENTINEL))
    thread_errors: list[BaseException] = []
    original_excepthook = threading.excepthook

    def capture_thread_error(args: threading.ExceptHookArgs) -> None:
        thread_errors.append(args.exc_value)

    threading.excepthook = capture_thread_error
    try:
        worker = threading.Thread(target=aiosqlite_core._connection_worker_thread, args=(tx,))
        worker.start()
        worker.join(timeout=2)
    finally:
        threading.excepthook = original_excepthook

    assert not worker.is_alive()
    assert thread_errors == []
