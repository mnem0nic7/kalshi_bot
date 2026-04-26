from __future__ import annotations

import asyncio
import logging

import pytest

from kalshi_bot.web.background_tasks import schedule_logged_task


@pytest.mark.asyncio
async def test_schedule_logged_task_records_background_failures(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("kalshi_bot.tests.background_tasks")

    async def fail() -> None:
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        task = schedule_logged_task(fail(), name="failing-task", logger=logger)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert task.done()
    assert any(record.message == "Background task failed: failing-task" for record in caplog.records)
    assert any(record.exc_info and record.exc_info[0] is RuntimeError for record in caplog.records)


@pytest.mark.asyncio
async def test_schedule_logged_task_returns_successful_task() -> None:
    logger = logging.getLogger("kalshi_bot.tests.background_tasks")

    async def succeed() -> str:
        return "ok"

    task = schedule_logged_task(succeed(), name="successful-task", logger=logger)
    await task

    assert task.result() == "ok"
