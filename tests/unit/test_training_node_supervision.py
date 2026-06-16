"""Tests for training-node supervision logic in DaemonService.

Two concerns:
1. _make_training_node_task — unit-tested directly via a minimal stub that
   exposes only the two loop methods the helper calls.
2. Supervision loop behaviour — tested by patching _run_training_node's key
   dependencies on a stub object so we can verify that a one-shot loop return
   does NOT cause the daemon to exit prematurely.  The supervision loop is
   exercised by running _run_training_node with run_seconds set so the test
   terminates, while the monitored loops are patched to return once then block.
"""
from __future__ import annotations

import asyncio
import types
from types import SimpleNamespace

import pytest

from kalshi_bot.services.daemon import DaemonService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _LoopController:
    """Coroutine factory that returns once on the first call, then blocks
    forever (simulating a loop that exits unexpectedly then is restarted).

    call_count tracks how many times the coroutine was created (== restarts + 1).
    """

    def __init__(self, *, raise_on_first: Exception | None = None):
        self.call_count = 0
        self._raise_on_first = raise_on_first

    async def __call__(self) -> None:
        self.call_count += 1
        if self.call_count == 1:
            if self._raise_on_first is not None:
                raise self._raise_on_first
            return  # exits normally on first invocation
        # Subsequent calls block indefinitely (the "running forever" state).
        await asyncio.sleep(3600)


class _TrainingNodeStub:
    """Minimal stand-in for DaemonService exposing only what _run_training_node
    and _make_training_node_task touch.

    We inherit from nothing; instead we borrow the two methods directly from
    DaemonService using unbound-method calls so we exercise the real logic
    without constructing the full service graph.
    """

    def __init__(
        self,
        *,
        nightly_enabled: bool = True,
        heartbeat_controller: _LoopController | None = None,
        nightly_controller: _LoopController | None = None,
    ):
        self.settings = SimpleNamespace(
            crypto_model_nightly_auto_enabled=nightly_enabled,
            crypto_continuous_train_enabled=False,
            kalshi_env="demo",
            app_color="blue",
        )
        self._heartbeat_role = "trainer"
        self._hb_ctrl = heartbeat_controller or _LoopController()
        self._nl_ctrl = nightly_controller or _LoopController()
        # Track whether the threaded liveness heartbeat was started/stopped.
        self.liveness_started = False
        self.liveness_stopped = False
        # Startup delay bypassed in tests.
        self._startup_grace_called = False

    # --- methods called by _run_training_node ---

    async def heartbeat_once(self, *, run_follow_up: bool = True) -> dict:
        return {"ok": True}

    def _start_threaded_liveness_heartbeat(self) -> None:
        self.liveness_started = True

    def _stop_threaded_liveness_heartbeat(self) -> None:
        self.liveness_stopped = True

    def _startup_delay_seconds(self) -> float:
        return 0.0

    async def _periodic_heartbeat_loop(self) -> None:
        await self._hb_ctrl()

    async def _periodic_crypto_model_nightly_loop(self) -> None:
        await self._nl_ctrl()

    # Borrow the real implementations from DaemonService.
    _run_training_node = DaemonService._run_training_node
    _make_training_node_task = DaemonService._make_training_node_task


# ---------------------------------------------------------------------------
# Tests: _make_training_node_task
# ---------------------------------------------------------------------------

class _MakeTaskStub:
    """Even simpler stub just for _make_training_node_task."""

    async def _periodic_heartbeat_loop(self) -> None:
        pass  # pragma: no cover

    async def _periodic_crypto_model_nightly_loop(self) -> None:
        pass  # pragma: no cover

    _make_training_node_task = DaemonService._make_training_node_task


class _AsyncCallRecorder:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def __call__(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"status": "ok"}


class _ContinuousTrainStub:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(
            crypto_training_preflight_enabled=False,
            crypto_continuous_train_replay_days=10,
            crypto_continuous_train_replay_limit=80_000,
        )
        self.crypto_training_backfill_service = None
        self.crypto_spot_service = None
        self.crypto_history_service = SimpleNamespace(
            collect_settled=_AsyncCallRecorder(),
            bootstrap=_AsyncCallRecorder(),
        )
        self.crypto_forecast_service = SimpleNamespace(train=_AsyncCallRecorder())
        self.crypto_replay_service = SimpleNamespace(
            run=_AsyncCallRecorder(),
            gate=_AsyncCallRecorder(),
        )

    _train_one_crypto_asset = DaemonService._train_one_crypto_asset


def test_make_training_node_task_heartbeat_returns_coroutine() -> None:
    stub = _MakeTaskStub()
    coro = stub._make_training_node_task("heartbeat")
    assert asyncio.iscoroutine(coro), "expected a coroutine for 'heartbeat'"
    coro.close()  # clean up without running


def test_make_training_node_task_nightly_returns_coroutine() -> None:
    stub = _MakeTaskStub()
    coro = stub._make_training_node_task("crypto_model_nightly")
    assert asyncio.iscoroutine(coro), "expected a coroutine for 'crypto_model_nightly'"
    coro.close()


def test_make_training_node_task_unknown_raises() -> None:
    stub = _MakeTaskStub()
    with pytest.raises(ValueError, match="unknown training-node task"):
        stub._make_training_node_task("bogus_task")


@pytest.mark.asyncio
async def test_continuous_train_bounds_per_asset_replay() -> None:
    stub = _ContinuousTrainStub()

    status = await stub._train_one_crypto_asset(frequency="15m", asset="BTC")

    assert status == "refreshed"
    assert stub.crypto_forecast_service.train.calls[-1] == {
        "frequency": "15m",
        "asset_symbols": ["BTC"],
        "use_feature_store": False,
        "feature_store_only": False,
    }
    assert stub.crypto_replay_service.run.calls[-1] == {
        "frequency": "15m",
        "asset_symbols": ["BTC"],
        "days": 10,
        "limit": 80_000,
    }
    assert stub.crypto_replay_service.gate.calls[-1] == {
        "frequency": "15m",
        "asset_symbols": ["BTC"],
    }


# ---------------------------------------------------------------------------
# Tests: supervision loop — loop exit does NOT kill the daemon
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_heartbeat_loop_return_is_restarted_not_fatal() -> None:
    """If _periodic_heartbeat_loop returns after the first call, the supervision
    loop must restart it.  The daemon should only exit when run_seconds fires,
    not when the loop returns.

    We use run_seconds=0.1 so the test completes quickly.  The heartbeat
    controller returns on call 1 (simulating the mysterious clean exit), then
    blocks on call 2 (restarted).  If supervision were broken the daemon would
    return immediately with completed='heartbeat'; with the fix it must return
    completed='timer'.
    """
    hb = _LoopController()  # returns on call 1, blocks on call 2+
    nl = _LoopController()  # same, but nightly is also created
    stub = _TrainingNodeStub(nightly_enabled=True, heartbeat_controller=hb, nightly_controller=nl)

    # run_seconds gives us a termination point after a short wall-clock delay.
    result = await stub._run_training_node(run_seconds=0.1)

    assert result["completed"] == "timer", (
        f"expected 'timer' (timer-driven exit) but got {result['completed']!r}; "
        "the supervision loop failed to restart the heartbeat task"
    )
    # heartbeat must have been created at least twice (original + 1 restart).
    assert hb.call_count >= 2, (
        f"heartbeat_controller was only called {hb.call_count} time(s); "
        "expected at least 2 (initial + after restart)"
    )


@pytest.mark.asyncio
async def test_heartbeat_loop_raise_is_restarted_not_fatal() -> None:
    """If _periodic_heartbeat_loop raises (e.g. DB error), the supervision loop
    must log and restart it, not propagate the exception.
    """
    db_error = OSError("simulated DB connection reset")
    hb = _LoopController(raise_on_first=db_error)
    nl = _LoopController()
    stub = _TrainingNodeStub(nightly_enabled=True, heartbeat_controller=hb, nightly_controller=nl)

    result = await stub._run_training_node(run_seconds=0.1)

    assert result["completed"] == "timer"
    assert hb.call_count >= 2, (
        f"heartbeat raised once but was only invoked {hb.call_count} time(s); "
        "expected restart"
    )


@pytest.mark.asyncio
async def test_nightly_loop_return_is_restarted_not_fatal() -> None:
    """If _periodic_crypto_model_nightly_loop returns, the daemon must restart
    it rather than exiting.
    """
    hb = _LoopController()
    nl = _LoopController()  # returns on first call
    stub = _TrainingNodeStub(nightly_enabled=True, heartbeat_controller=hb, nightly_controller=nl)

    result = await stub._run_training_node(run_seconds=0.1)

    assert result["completed"] == "timer"
    assert nl.call_count >= 2, (
        f"nightly controller was only called {nl.call_count} time(s); "
        "expected restart after unexpected return"
    )


@pytest.mark.asyncio
async def test_timer_exit_returns_timer_completed() -> None:
    """When run_seconds fires before any loop exits, result['completed'] == 'timer'.
    This is the test/CLI one-off path.
    """
    hb = _LoopController()  # will block forever on call 1 (asyncio.sleep(3600))
    # Override so first call blocks (never returns).
    async def _block() -> None:
        await asyncio.sleep(3600)
    hb_block = types.SimpleNamespace(call_count=0)

    # Patch the stub so heartbeat blocks immediately.
    class _TimerStub(_TrainingNodeStub):
        async def _periodic_heartbeat_loop(self) -> None:
            hb_block.call_count += 1
            await asyncio.sleep(3600)
        async def _periodic_crypto_model_nightly_loop(self) -> None:
            await asyncio.sleep(3600)

    stub = _TimerStub(nightly_enabled=True)
    result = await stub._run_training_node(run_seconds=0.05)

    assert result["completed"] == "timer"
    assert result["mode"] == "training_node"


@pytest.mark.asyncio
async def test_nightly_disabled_no_restart_task_created_for_it() -> None:
    """When crypto_model_nightly_auto_enabled=False the nightly task is never
    created, so _make_training_node_task should never be asked to restart it.
    The daemon should still exit cleanly via the timer.
    """
    hb = _LoopController()  # blocks forever
    # Override heartbeat to block — nightly is not created.
    class _NoNightlyStub(_TrainingNodeStub):
        async def _periodic_heartbeat_loop(self) -> None:
            await asyncio.sleep(3600)

    stub = _NoNightlyStub(nightly_enabled=False, heartbeat_controller=hb)
    result = await stub._run_training_node(run_seconds=0.05)

    assert result["completed"] == "timer"


@pytest.mark.asyncio
async def test_finally_stops_liveness_heartbeat_even_after_loop_restart() -> None:
    """The finally block must call _stop_threaded_liveness_heartbeat regardless
    of how many times loops were restarted.  run_seconds=None path.

    Because run_seconds is None the liveness heartbeat is started; we pass a
    short timer via run_seconds to keep the test fast, so we use run_seconds
    here to bound the test but verify the flag is set.
    """
    hb = _LoopController()  # returns on first call → triggers restart
    nl = _LoopController()
    stub = _TrainingNodeStub(nightly_enabled=True, heartbeat_controller=hb, nightly_controller=nl)

    # Use run_seconds to bound the test; note run_seconds=None would start the
    # threaded liveness heartbeat but never return — use a very small value.
    await stub._run_training_node(run_seconds=0.12)

    # The test stub's _stop_threaded_liveness_heartbeat records the call.
    assert stub.liveness_stopped, "finally block must call _stop_threaded_liveness_heartbeat"
