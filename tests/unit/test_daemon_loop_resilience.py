"""Tests for reconcile/heartbeat loop resilience in DaemonService.

Incident 2026-07-02: a Postgres crash mid-await left the green daemon's
reconcile and heartbeat-follow-up paths dead for ~7 hours while every other
loop (and the container health check) stayed green.  Two distinct failure
modes must be survivable:

1. The loop body RAISES (DB refuses connections) — the loop must log and
   continue, not die (previously the task died and, by design, should have
   crashed run(); in practice nothing restarted it).
2. The loop body HANGS (await on a dead socket) — a stall timeout must cancel
   the wedged iteration so the next one can run.

Same stub-borrowing pattern as test_training_node_supervision.py: exercise
the real loop methods on a minimal object.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from kalshi_bot.services.daemon import DaemonService


class _ReconcileStub:
    def __init__(self, *, behaviors):
        # behaviors: list of "ok" | "raise" | "hang"; last entry repeats.
        self.settings = SimpleNamespace(
            daemon_reconcile_interval_seconds=0.01,
            daemon_heartbeat_interval_seconds=0.01,
            daemon_loop_stall_timeout_seconds=0.05,
        )
        self.behaviors = behaviors
        self.calls = 0
        self.completed = 0

    async def reconcile_once(self, *, run_settlement_gate_tuning: bool = True):
        behavior = self.behaviors[min(self.calls, len(self.behaviors) - 1)]
        self.calls += 1
        if behavior == "raise":
            raise ConnectionError("db down")
        if behavior == "hang":
            await asyncio.sleep(3600)
        self.completed += 1
        return {}

    _periodic_reconcile_loop = DaemonService._periodic_reconcile_loop


class _HeartbeatStub:
    def __init__(self, *, behaviors):
        self.settings = SimpleNamespace(
            daemon_reconcile_interval_seconds=0.01,
            daemon_heartbeat_interval_seconds=0.01,
            daemon_loop_stall_timeout_seconds=0.05,
        )
        self.behaviors = behaviors
        self.calls = 0
        self.follow_ups: list[dict] = []

    async def heartbeat_once(self, *, run_follow_up: bool = True):
        behavior = self.behaviors[min(self.calls, len(self.behaviors) - 1)]
        self.calls += 1
        if behavior == "raise":
            raise ConnectionError("db down")
        if behavior == "hang":
            await asyncio.sleep(3600)
        return {"seq": self.calls}

    def _schedule_heartbeat_follow_up(self, payload: dict) -> None:
        self.follow_ups.append(payload)

    _periodic_heartbeat_loop = DaemonService._periodic_heartbeat_loop


async def _run_loop_until(coro, predicate, timeout: float = 2.0) -> None:
    task = asyncio.create_task(coro)
    try:
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_reconcile_loop_survives_exception():
    stub = _ReconcileStub(behaviors=["raise", "ok"])
    await _run_loop_until(stub._periodic_reconcile_loop(), lambda: stub.completed >= 1)
    assert stub.calls >= 2
    assert stub.completed >= 1


async def test_reconcile_loop_survives_hang():
    stub = _ReconcileStub(behaviors=["hang", "ok"])
    await _run_loop_until(stub._periodic_reconcile_loop(), lambda: stub.completed >= 1)
    assert stub.calls >= 2
    assert stub.completed >= 1


async def test_heartbeat_loop_survives_exception_and_keeps_scheduling():
    stub = _HeartbeatStub(behaviors=["raise", "ok"])
    await _run_loop_until(stub._periodic_heartbeat_loop(), lambda: len(stub.follow_ups) >= 1)
    assert stub.calls >= 2
    # No follow-up scheduled for the failed iteration; one for the good one.
    assert stub.follow_ups[0]["seq"] >= 2


async def test_heartbeat_loop_survives_hang():
    stub = _HeartbeatStub(behaviors=["hang", "ok"])
    await _run_loop_until(stub._periodic_heartbeat_loop(), lambda: len(stub.follow_ups) >= 1)
    assert stub.calls >= 2


async def test_loop_cancellation_still_propagates():
    stub = _ReconcileStub(behaviors=["ok"])
    task = asyncio.create_task(stub._periodic_reconcile_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class _FollowUpStub:
    """Follow-up runner whose error handler hits a dead DB: the ops-event
    write must not surface a second unhandled exception (previously it
    produced 'Task exception was never retrieved' and lost the error)."""

    def __init__(self):
        self.settings = SimpleNamespace(kalshi_env="live", app_color="green")
        self._heartbeat_follow_up_task = None

    async def _run_heartbeat_follow_up(self, payload):
        raise ConnectionError("db down")

    def session_factory(self):
        raise ConnectionError("db still down")

    _heartbeat_follow_up_runner = DaemonService._heartbeat_follow_up_runner


async def test_follow_up_runner_survives_dead_db_in_error_handler():
    stub = _FollowUpStub()
    task = asyncio.create_task(stub._heartbeat_follow_up_runner({}))
    stub._heartbeat_follow_up_task = task
    await task  # must not raise
    assert stub._heartbeat_follow_up_task is None
