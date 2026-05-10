from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.reconcile import ReconcileSummary


class FakeReconciliationService:
    async def reconcile(self, *_args, **_kwargs) -> ReconcileSummary:
        return ReconcileSummary(
            balances_seen=True,
            positions_count=0,
            orders_count=0,
            fills_count=0,
            settlements_count=1,
            historical_cutoff_seen=True,
            reconciled_at="2026-05-10T00:00:00+00:00",
        )


class FakeAutonomousGateTuningService:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, **kwargs):
        self.calls += 1
        return {"status": "staged", "triggered_by": kwargs.get("triggered_by")}


class FakeStrategyEvalService:
    def __init__(self) -> None:
        self.calls = 0

    async def maybe_adjust(self):
        self.calls += 1
        return {"status": "legacy_adjustment"}


@pytest.mark.asyncio
async def test_daemon_reconcile_triggers_autonomous_gate_tuning_after_settlements(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-autonomous-gates.db",
        autonomous_gate_tuning_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    autonomous = FakeAutonomousGateTuningService()
    legacy = FakeStrategyEvalService()
    daemon = DaemonService(
        settings,
        session_factory,
        weather_directory=None,
        discovery_service=None,
        stream_service=None,
        reconciliation_service=FakeReconciliationService(),
        research_coordinator=None,
        auto_trigger_service=None,
        shadow_training_service=None,
        shadow_campaign_service=None,
        self_improve_service=None,
        strategy_eval_service=legacy,
        autonomous_gate_tuning_service=autonomous,
    )

    result = await daemon.reconcile_once()

    assert result["autonomous_gate_tuning"] == {
        "status": "staged",
        "triggered_by": "settlement_reconcile",
    }
    assert autonomous.calls == 1
    assert legacy.calls == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_inactive_color_reconcile_skips_autonomous_gate_tuning(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-autonomous-gates-inactive.db",
        app_color="blue",
        autonomous_gate_tuning_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.set_active_color("green")
        await session.commit()
    autonomous = FakeAutonomousGateTuningService()
    daemon = DaemonService(
        settings,
        session_factory,
        weather_directory=None,
        discovery_service=None,
        stream_service=None,
        reconciliation_service=FakeReconciliationService(),
        research_coordinator=None,
        auto_trigger_service=None,
        shadow_training_service=None,
        shadow_campaign_service=None,
        self_improve_service=None,
        autonomous_gate_tuning_service=autonomous,
    )

    result = await daemon.reconcile_once()

    assert result["autonomous_gate_tuning"] == {"status": "skipped", "reason": "inactive_color"}
    assert autonomous.calls == 0

    await engine.dispose()
