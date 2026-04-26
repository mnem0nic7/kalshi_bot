from __future__ import annotations

import asyncio
from functools import partial
import logging

logger = logging.getLogger(__name__)

# Keep the poll tight: SQLAlchemy schema inspection can issue hundreds of tiny
# sqlite calls, so a larger interval turns valid DB tests into minute-long runs.
_WAKEUP_POLL_SECONDS = 0.001


def install_aiosqlite_wakeup_patch() -> None:
    """Work around event-loop wakeups lost after sqlite work in worker threads.

    In this Python/runtime combination, a thread that runs SQLite work can call
    ``loop.call_soon_threadsafe`` successfully without reliably waking the
    selector that is awaiting the scheduled future.  ``aiosqlite`` then appears
    to hang even though its worker completed the operation.  We both nudge the
    loop from the worker and poll awaited futures so callbacks cannot remain
    stranded indefinitely.
    """
    try:
        import aiosqlite.core as aiosqlite_core
    except Exception:
        logger.debug("aiosqlite unavailable; wakeup patch not installed", exc_info=True)
        return

    if getattr(aiosqlite_core, "_kalshi_wakeup_patch_installed", False):
        return

    async def await_with_periodic_wakeup(future: asyncio.Future):
        while True:
            try:
                return await asyncio.wait_for(asyncio.shield(future), timeout=_WAKEUP_POLL_SECONDS)
            except TimeoutError:
                continue

    def wake_loop(loop: asyncio.AbstractEventLoop) -> None:
        writer = getattr(loop, "_write_to_self", None)
        if writer is None:
            return
        try:
            writer()
        except Exception:
            logger.debug("failed to wake asyncio loop after aiosqlite work", exc_info=True)

    def deliver_to_future(future: asyncio.Future | None, callback, value) -> None:
        if future is None or future.done():
            return
        loop = future.get_loop()
        if loop.is_closed():
            logger.debug("dropped late aiosqlite worker result because event loop is closed")
            return
        try:
            loop.call_soon_threadsafe(callback, future, value)
            wake_loop(loop)
        except RuntimeError:
            logger.debug("dropped late aiosqlite worker result during event-loop shutdown", exc_info=True)

    def patched_connection_worker_thread(tx):
        while True:
            future, function = tx.get()
            try:
                result = function()
                deliver_to_future(future, aiosqlite_core.set_result, result)
                if result is aiosqlite_core._STOP_RUNNING_SENTINEL:
                    break
            except BaseException as exc:
                deliver_to_future(future, aiosqlite_core.set_exception, exc)

    async def patched_connect(self):
        if self._connection is None:
            try:
                future = asyncio.get_event_loop().create_future()
                self._tx.put_nowait((future, self._connector))
                self._connection = await await_with_periodic_wakeup(future)
            except BaseException:
                self.stop()
                self._connection = None
                raise
        return self

    async def patched_execute(self, fn, *args, **kwargs):
        if not self._running or not self._connection:
            raise ValueError("Connection closed")
        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()
        self._tx.put_nowait((future, function))
        return await await_with_periodic_wakeup(future)

    async def patched_close(self) -> None:
        if self._connection is None:
            return
        try:
            await self._execute(self._conn.close)
        except Exception:
            aiosqlite_core.LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._connection = None
            future = self.stop()
            if future:
                await await_with_periodic_wakeup(future)

    aiosqlite_core._connection_worker_thread = patched_connection_worker_thread
    aiosqlite_core.Connection._connect = patched_connect
    aiosqlite_core.Connection._execute = patched_execute
    aiosqlite_core.Connection.close = patched_close
    aiosqlite_core._kalshi_wakeup_patch_installed = True
