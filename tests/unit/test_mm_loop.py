"""Continuous MM research loop orchestration.

Each iteration collects ticks and logs them (the data spine); the fair-value +
backtest eval runs only when its interval has elapsed. IO is injected so the
orchestration is testable without network or DB.
"""
from __future__ import annotations

import pytest

from kalshi_bot.mm.loop import MarketMakingLoop, MmLoopConfig


def _loop(clock, *, collected, eval_interval=900.0):
    stored: list = []
    evals: list = []

    async def collect():
        return list(collected)

    async def store(ticks):
        stored.append(list(ticks))

    async def run_eval():
        evals.append(clock["t"])
        return {"ran_at": clock["t"]}

    loop = MarketMakingLoop(
        collect_ticks=collect,
        store_ticks=store,
        run_eval=run_eval,
        clock=lambda: clock["t"],
        config=MmLoopConfig(eval_interval_seconds=eval_interval),
    )
    return loop, stored, evals


@pytest.mark.asyncio
async def test_iteration_logs_every_tick_and_evals_first_time():
    clock = {"t": 1000.0}
    loop, stored, evals = _loop(clock, collected=[{"market_ticker": "X"}])
    result = await loop.run_once()
    assert result["ticks"] == 1
    assert stored == [[{"market_ticker": "X"}]]
    assert result["evaluated"] is True  # first iteration: eval is always due
    assert evals == [1000.0]


@pytest.mark.asyncio
async def test_eval_respects_interval():
    clock = {"t": 1000.0}
    loop, stored, evals = _loop(clock, collected=[{"m": 1}], eval_interval=900.0)
    await loop.run_once()                      # eval at 1000
    clock["t"] = 1100.0
    r2 = await loop.run_once()                 # only 100s later → no eval
    assert r2["evaluated"] is False
    clock["t"] = 1000.0 + 900.0
    r3 = await loop.run_once()                 # interval elapsed → eval again
    assert r3["evaluated"] is True
    assert evals == [1000.0, 1900.0]
    assert len(stored) == 3                    # logged on every iteration


@pytest.mark.asyncio
async def test_empty_collection_skips_store():
    clock = {"t": 1000.0}
    loop, stored, evals = _loop(clock, collected=[])
    result = await loop.run_once()
    assert result["ticks"] == 0
    assert stored == []
