"""The heartbeat follow-up (training corpus, historical intelligence, gate
tuning, strategy jobs, rollout monitoring) must run ONLY on the main daemon
role. Before 2026-07-03 it gated on active COLOR alone, so the crypto-only
1h daemon (same color as the main daemon) ran the entire heavy suite every
heartbeat from a second container — duplicating stateful jobs and decoding
GBs of room-bundle JSON (the crypto_1h 8g OOM-cycle burst), and the trainer
node triplicated it.

Stub-borrowing pattern per test_training_node_supervision.py.
"""
from __future__ import annotations

from types import SimpleNamespace

from kalshi_bot.services.daemon import DaemonService


class _FollowUpStub:
    def __init__(self, role: str):
        self._heartbeat_role = role
        self.settings = SimpleNamespace(
            app_color="green",
            training_campaign_enabled=False,
            kalshi_env="production",
        )
        self.shadow_campaign_service = None
        self.autonomous_gate_tuning_service = None
        self.calls: list[str] = []
        self.self_improve_service = SimpleNamespace(monitor_rollouts=self._monitor_rollouts)

    async def _monitor_rollouts(self):
        self.calls.append("monitor_rollouts")
        return SimpleNamespace(status="idle", payload={})

    async def _maybe_capture_checkpoint_archives(self):
        self.calls.append("checkpoint_archives")
        return None

    async def _maybe_run_settlement_follow_up(self):
        self.calls.append("settlement_follow_up")
        return None

    async def _maybe_run_strategy_regression(self):
        self.calls.append("strategy_regression")
        return None

    async def _maybe_run_strategy_promotion_watchdog(self):
        self.calls.append("strategy_promotion_watchdog")
        return None

    async def _maybe_run_strategy_codex_nightly(self):
        self.calls.append("strategy_codex_nightly")
        return None

    async def _maybe_run_momentum_calibration_nightly(self):
        self.calls.append("momentum_calibration")
        return None

    async def _maybe_run_historical_pipeline(self):
        self.calls.append("historical_pipeline")
        return {"status": "ok"}

    async def _maybe_run_decision_corpus_promotion(self):
        self.calls.append("decision_corpus_promotion")
        return None

    async def _maybe_run_autonomous_gate_tuning(self):
        self.calls.append("autonomous_gate_tuning")
        return None

    _run_heartbeat_follow_up = DaemonService._run_heartbeat_follow_up


async def test_main_daemon_role_runs_follow_up():
    stub = _FollowUpStub(role="daemon")
    await stub._run_heartbeat_follow_up({"active_color": "green"})
    assert "settlement_follow_up" in stub.calls
    assert "monitor_rollouts" in stub.calls


async def test_crypto_only_role_skips_follow_up_entirely():
    stub = _FollowUpStub(role="crypto_1h")
    await stub._run_heartbeat_follow_up({"active_color": "green"})
    assert stub.calls == []


async def test_trainer_role_skips_follow_up_entirely():
    stub = _FollowUpStub(role="trainer")
    await stub._run_heartbeat_follow_up({"active_color": "green"})
    assert stub.calls == []


async def test_inactive_color_main_daemon_still_monitors_rollouts():
    # Pre-existing behavior: the color gate skips the heavy block on the
    # inactive color, but rollout monitoring runs on both colors (the canary
    # target color may be the inactive one).
    stub = _FollowUpStub(role="daemon")
    await stub._run_heartbeat_follow_up({"active_color": "blue"})
    assert stub.calls == ["monitor_rollouts"]
