from __future__ import annotations

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, OpsEvent
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.reconcile import ReconcileSummary
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeStreamService:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def stream(self, *, market_tickers, include_private, max_messages, on_market_update=None):
        self.calls.append(list(market_tickers))
        return 3


class FakeReconciliationService:
    async def reconcile(self, repo, *, subaccount=0):
        return ReconcileSummary(
            balances_seen=True,
            positions_count=0,
            orders_count=0,
            fills_count=0,
            settlements_count=0,
            historical_cutoff_seen=True,
        )


class FakeAutoTriggerService:
    async def handle_market_update(self, market_ticker: str) -> None:
        return None

    async def wait_for_tasks(self) -> None:
        return None


class FakeResearchCoordinator:
    async def handle_market_update(self, market_ticker: str) -> None:
        return None

    async def wait_for_tasks(self) -> None:
        return None


class FakeDiscoveryService:
    async def list_stream_markets(self) -> list[str]:
        return ["WX-DISCOVERED"]


class FakeShadowTrainingService:
    async def run_shadow_sweep(self, *, limit=None, reason="shadow_sweep", markets=None):
        return []


class FakeSelfImproveService:
    async def monitor_rollouts(self):
        from kalshi_bot.services.self_improve import SelfImproveResult

        return SelfImproveResult(status="idle", payload={})


class FakeTrainingCorpusService:
    def __init__(self, *, status_counts: dict[str, int] | None = None) -> None:
        self.status_counts = status_counts or {}

    async def get_settlement_focus_summary(self, *, limit: int = 10):
        return {
            "unsettled_count": sum(self.status_counts.values()),
            "near_settlement_count": 0,
            "status_counts": self.status_counts,
            "backlog_by_market": {},
            "backlog_by_day": {},
            "settled_label_velocity": {"24h": 0, "7d": 0},
            "backlog": [],
        }


@pytest.mark.asyncio
async def test_daemon_service_runs_startup_reconcile_and_heartbeat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon.db",
        daemon_start_with_reconcile=True,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                market_type="weather",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    stream_service = FakeStreamService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    result = await daemon.run(max_messages=3)

    async with session_factory() as session:
        heartbeat = (
            await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.desc()))
        ).scalar_one()
        heartbeat_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:blue"))
        ).scalar_one()
        reconcile_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_reconcile:blue"))
        ).scalar_one()

    assert result["completed"] == "stream"
    assert result["processed_messages"] == 3
    assert stream_service.calls == [["WX-DISCOVERED"]]
    assert heartbeat.summary == "Daemon heartbeat"
    assert heartbeat_checkpoint.payload["app_color"] == "blue"
    assert "heartbeat_at" in heartbeat_checkpoint.payload
    assert "reconciled_at" in reconcile_checkpoint.payload

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_service_runs_settlement_follow_up_reconcile(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-followup.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    stream_service = FakeStreamService()

    class CountingReconciliationService(FakeReconciliationService):
        def __init__(self) -> None:
            self.calls = 0

        async def reconcile(self, repo, *, subaccount=0):
            self.calls += 1
            return await super().reconcile(repo, subaccount=subaccount)

    reconcile_service = CountingReconciliationService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        reconcile_service,  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(status_counts={"awaiting_settlement": 1}),  # type: ignore[arg-type]
    )

    result = await daemon.run(max_messages=1)

    async with session_factory() as session:
        followup_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_settlement_followup:blue"))
        ).scalar_one()
        await session.commit()

    assert result["completed"] == "stream"
    assert reconcile_service.calls == 1
    assert followup_checkpoint.payload["summary"]["status_counts"]["awaiting_settlement"] == 1

    await engine.dispose()
