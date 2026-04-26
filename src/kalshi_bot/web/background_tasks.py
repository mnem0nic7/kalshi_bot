from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any


def schedule_logged_task(
    coro: Coroutine[Any, Any, Any],
    *,
    name: str,
    logger: logging.Logger,
) -> asyncio.Task[Any]:
    task = asyncio.create_task(coro, name=name)

    def log_result(completed_task: asyncio.Task[Any]) -> None:
        try:
            completed_task.result()
        except asyncio.CancelledError:
            logger.debug("Background task cancelled: %s", name)
        except Exception:
            logger.exception("Background task failed: %s", name)

    task.add_done_callback(log_result)
    return task
