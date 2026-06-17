"""Continuous market-making research loop (plan §3, non-trading).

Each iteration logs the data spine (every tick); the fair-value + backtest eval
runs only when its interval has elapsed. All IO — quote collection, persistence,
evaluation — is injected, so the orchestration is unit-testable and the same loop
drives the live service with real collaborators wired in the entrypoint.

This loop NEVER places orders. It only logs and evaluates.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MmLoopConfig:
    eval_interval_seconds: float = 900.0
    idle_seconds: float = 5.0


class MarketMakingLoop:
    def __init__(
        self,
        *,
        collect_ticks: Callable[[], Awaitable[list[dict[str, Any]]]],
        store_ticks: Callable[[list[dict[str, Any]]], Awaitable[None]],
        run_eval: Callable[[], Awaitable[Any]],
        clock: Callable[[], float],
        config: MmLoopConfig | None = None,
    ) -> None:
        self._collect_ticks = collect_ticks
        self._store_ticks = store_ticks
        self._run_eval = run_eval
        self._clock = clock
        self._config = config or MmLoopConfig()
        self._last_eval_ts: float | None = None

    def _eval_due(self, now: float) -> bool:
        return self._last_eval_ts is None or (now - self._last_eval_ts) >= self._config.eval_interval_seconds

    async def run_once(self) -> dict[str, Any]:
        now = self._clock()
        ticks = await self._collect_ticks()
        if ticks:
            await self._store_ticks(ticks)
        evaluation: Any = None
        if self._eval_due(now):
            evaluation = await self._run_eval()
            self._last_eval_ts = now
        return {"ticks": len(ticks), "stored": bool(ticks), "evaluated": evaluation is not None, "evaluation": evaluation}

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while not (stop_event is not None and stop_event.is_set()):
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # research loop must never crash the service
                logger.exception("market-making research loop iteration failed")
            await asyncio.sleep(self._config.idle_seconds)
