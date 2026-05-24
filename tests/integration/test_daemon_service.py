from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, CryptoModelArtifactRecord, OpsEvent
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.reconcile import ReconcileSummary
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeStreamService:
    def __init__(self, *, updates: list[str] | None = None) -> None:
        self.calls: list[list[str]] = []
        self.updates = updates or []

    async def stream(self, *, market_tickers, include_private, max_messages, on_market_update=None):
        self.calls.append(list(market_tickers))
        if on_market_update is not None:
            for ticker in self.updates:
                await on_market_update(ticker)
        return 3


class FakeReconciliationService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def reconcile(self, repo, *, subaccount=0, kalshi_env=""):
        self.calls.append({"subaccount": subaccount, "kalshi_env": kalshi_env})
        return ReconcileSummary(
            balances_seen=True,
            positions_count=0,
            orders_count=0,
            fills_count=0,
            settlements_count=0,
            historical_cutoff_seen=True,
        )


class FailingReconciliationService(FakeReconciliationService):
    async def reconcile(self, repo, *, subaccount=0, kalshi_env=""):
        self.calls.append({"subaccount": subaccount, "kalshi_env": kalshi_env})
        raise RuntimeError("Kalshi API timeout")


class FakeAutoTriggerService:
    def __init__(self) -> None:
        self.market_updates: list[str] = []

    async def handle_market_update(self, market_ticker: str) -> None:
        self.market_updates.append(market_ticker)
        return None

    async def wait_for_tasks(self) -> None:
        return None


class FakeResearchCoordinator:
    def __init__(self) -> None:
        self.market_updates: list[str] = []
        self.live_weather_refreshes: list[dict[str, object]] = []

    async def handle_market_update(self, market_ticker: str) -> None:
        self.market_updates.append(market_ticker)
        return None

    async def wait_for_tasks(self) -> None:
        return None

    async def refresh_live_weather_dossiers(self, market_tickers, **kwargs):
        tickers = list(market_tickers)
        self.live_weather_refreshes.append({"market_tickers": tickers, **kwargs})
        return {"status": "ok", "considered": len(tickers), "selected": 0, "refreshed": 0, "failed": 0}


class FakeDiscoveryService:
    async def list_stream_markets(self) -> list[str]:
        return ["WX-DISCOVERED"]


class CountingDiscoveryService:
    def __init__(self) -> None:
        self.calls = 0

    async def list_stream_markets(self) -> list[str]:
        self.calls += 1
        return ["WX-DISCOVERED"]


class BlockingDiscoveryService:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def list_stream_markets(self) -> list[str]:
        self.started.set()
        await self.release.wait()
        return ["WX-DISCOVERED"]


class FakeShadowTrainingService:
    async def run_shadow_sweep(self, *, limit=None, reason="shadow_sweep", markets=None):
        return []


class FakeSelfImproveService:
    async def monitor_rollouts(self):
        from kalshi_bot.services.self_improve import SelfImproveResult

        return SelfImproveResult(status="idle", payload={})

    async def apply_pending_pack_promotion(self, *, app_color: str) -> None:
        return None


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


class FakeCryptoSpotService:
    def __init__(self) -> None:
        self.collect_current_calls: list[dict[str, object]] = []
        self.collected = asyncio.Event()

    async def collect_current(self, **kwargs: object) -> dict[str, object]:
        self.collect_current_calls.append(kwargs)
        self.collected.set()
        return {"status": "ok", "stored": 1}


class FakeCryptoAutonomyService:
    def __init__(self) -> None:
        self.run_once_calls: list[dict[str, object]] = []
        self.first_finished = asyncio.Event()
        self.second_started = asyncio.Event()
        self.overlapped = False
        self._running = False

    async def run_once(self, **kwargs: object) -> dict[str, object]:
        if self._running:
            self.overlapped = True
        self._running = True
        self.run_once_calls.append(kwargs)
        try:
            if len(self.run_once_calls) == 1:
                await asyncio.sleep(0.01)
                self.first_finished.set()
            elif len(self.run_once_calls) == 2:
                if self.first_finished.is_set():
                    self.second_started.set()
        finally:
            self._running = False
        return {"status": "ok"}


class SlowCryptoAutonomyService:
    def __init__(self) -> None:
        self.run_once_calls: list[dict[str, object]] = []

    async def run_once(self, **kwargs: object) -> dict[str, object]:
        self.run_once_calls.append(kwargs)
        await asyncio.sleep(0.02)
        return {"status": "ok"}


class FakeHistoricalTrainingService:
    def __init__(self) -> None:
        self.capture_calls = 0
        self.backfill_calls = 0

    async def capture_checkpoint_archives_once(self, *, due_only=True, source_kind="daemon_checkpoint_capture", series=None, reference_time=None):
        self.capture_calls += 1
        return {"captured_checkpoint_count": 1, "source_kind": source_kind}

    async def backfill_settlements(self, *, date_from, date_to, series=None):
        self.backfill_calls += 1
        return {"backfilled_count": 1, "date_from": str(date_from), "date_to": str(date_to)}


class FakeHistoricalPipelineService:
    def __init__(self) -> None:
        self.daily_calls = 0

    async def daily(self):
        self.daily_calls += 1
        return {"status": "completed", "pipeline_kind": "daily"}


class FakeDecisionCorpusService:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def nightly_auto_promote(self, *, kalshi_env: str, actor: str = "daemon"):
        self.calls.append({"kalshi_env": kalshi_env, "actor": actor})
        return {"status": "skipped", "reason": "new_resolved_rooms"}


class FakeStrategyRegressionService:
    def __init__(self, session_factory, *, now_fn) -> None:
        self.session_factory = session_factory
        self.now_fn = now_fn
        self.calls = 0

    async def run_regression(self):
        self.calls += 1
        now = self.now_fn()
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(
                "strategy_regression",
                None,
                {
                    "ran_at": now.isoformat(),
                    "rooms_scanned": 84,
                    "series_evaluated": 2,
                    "window_days": 180,
                },
            )
            await session.commit()
        return {"status": "ok", "ran_at": now.isoformat()}


class FakeStrategyDashboardService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def build_dashboard(self, *, window_days=180, series_ticker=None, strategy_name=None, include_codex_lab=False):
        self.calls.append(
            {
                "window_days": window_days,
                "series_ticker": series_ticker,
                "strategy_name": strategy_name,
                "include_codex_lab": include_codex_lab,
            }
        )
        return {
            "summary": {"window_days": window_days, "window_display": f"{window_days}d"},
            "leaderboard": [],
            "city_matrix": [],
            "detail_context": {"type": "empty", "selected_series_ticker": None, "selected_strategy_name": None},
            "recent_promotions": [],
            "methodology": {"points": []},
        }


class FakeStrategyCodexService:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.calls: list[dict[str, object]] = []

    def is_available(self) -> bool:
        return self.available

    async def execute_modes_for_snapshot(
        self,
        *,
        modes,
        dashboard_snapshot,
        window_days,
        trigger_source="manual",
        series_ticker=None,
        strategy_name=None,
        operator_brief=None,
    ):
        self.calls.append(
            {
                "modes": list(modes),
                "dashboard_snapshot": dashboard_snapshot,
                "window_days": window_days,
                "trigger_source": trigger_source,
                "series_ticker": series_ticker,
                "strategy_name": strategy_name,
                "operator_brief": operator_brief,
            }
        )
        return [
            {"id": "nightly-evaluate", "mode": "evaluate", "status": "completed", "trigger_source": trigger_source},
            {"id": "nightly-suggest", "mode": "suggest", "status": "completed", "trigger_source": trigger_source},
        ]


class FakeStrategyAutoEvolveService:
    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory
        self.calls: list[dict[str, object]] = []

    async def run_once(self, *, trigger_source: str = "manual") -> dict[str, object]:
        self.calls.append({"trigger_source": trigger_source})
        payload = {
            "status": "completed",
            "mode": "auto_evolve",
            "trigger_source": trigger_source,
            "run_ids": ["nightly-evaluate", "nightly-suggest"],
            "accepted_strategy": "balanced-plus",
            "activated_strategy": "balanced-plus",
            "assignment_changes": [{"series_ticker": "KXHIGHNY", "new_strategy": "balanced-plus"}],
        }
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint("daemon_strategy_auto_evolve:demo", None, payload)
            await session.commit()
        return payload


class BlockingShadowCampaignService:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, request) -> dict:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return {"status": "completed", "reason": request.reason}


@pytest.mark.asyncio
async def test_weather_market_update_is_ignored_when_weather_runtime_disabled(tmp_path) -> None:
    research = FakeResearchCoordinator()
    auto_trigger = FakeAutoTriggerService()
    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-weather-disabled.db",
        weather_prediction_enabled=False,
        weather_residual_model_enabled=False,
        weather_intraday_model_enabled=False,
        weather_research_refresh_interval_seconds=0,
        weather_rejected_opportunity_scorer_enabled=False,
    )
    daemon.research_coordinator = research
    daemon.auto_trigger_service = auto_trigger
    daemon._auto_trigger_enabled_for_run = False
    daemon._market_update_dispatch_due = lambda market_ticker: True  # type: ignore[method-assign]

    async def active_color() -> bool:
        return True

    daemon._is_active_color = active_color  # type: ignore[method-assign]

    await daemon._handle_market_update("KXHIGHNY-26MAY24-T80")

    assert research.market_updates == []
    assert auto_trigger.market_updates == []


@pytest.mark.asyncio
async def test_non_weather_market_update_still_dispatches_when_weather_runtime_disabled(tmp_path) -> None:
    research = FakeResearchCoordinator()
    auto_trigger = FakeAutoTriggerService()
    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-non-weather.db",
        weather_prediction_enabled=False,
        weather_residual_model_enabled=False,
        weather_intraday_model_enabled=False,
        weather_research_refresh_interval_seconds=0,
        weather_rejected_opportunity_scorer_enabled=False,
    )
    daemon.research_coordinator = research
    daemon.auto_trigger_service = auto_trigger
    daemon._auto_trigger_enabled_for_run = False
    daemon._market_update_dispatch_due = lambda market_ticker: True  # type: ignore[method-assign]

    async def active_color() -> bool:
        return True

    daemon._is_active_color = active_color  # type: ignore[method-assign]

    await daemon._handle_market_update("KXBTC-26MAY24-B100000")

    assert research.market_updates == ["KXBTC-26MAY24-B100000"]
    assert auto_trigger.market_updates == []


@pytest.mark.asyncio
async def test_stream_market_discovery_is_skipped_when_weather_runtime_disabled(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-discovery-disabled.db",
        weather_prediction_enabled=False,
        weather_residual_model_enabled=False,
        weather_intraday_model_enabled=False,
        weather_research_refresh_interval_seconds=0,
        weather_rejected_opportunity_scorer_enabled=False,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    discovery = CountingDiscoveryService()
    daemon = object.__new__(DaemonService)
    daemon.settings = settings
    daemon.session_factory = session_factory
    daemon.discovery_service = discovery

    selected = await daemon._select_stream_markets(None)

    assert selected == []
    assert discovery.calls == 0


@pytest.mark.asyncio
async def test_daemon_service_runs_startup_reconcile_and_heartbeat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon.db",
        daemon_start_with_reconcile=True,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        weather_research_refresh_interval_seconds=0,
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
    reconciliation_service = FakeReconciliationService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        reconciliation_service,  # type: ignore[arg-type]
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
            await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.desc()).limit(1))
        ).scalar_one()
        heartbeat_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:demo:blue"))
        ).scalar_one()
        reconcile_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_reconcile:demo:blue"))
        ).scalar_one()

    assert result["completed"] == "stream"
    assert result["processed_messages"] == 3
    assert stream_service.calls == [["WX-DISCOVERED"]]
    assert reconciliation_service.calls == [{"subaccount": 0, "kalshi_env": "demo"}]
    assert heartbeat.summary == "Daemon heartbeat"
    assert heartbeat_checkpoint.payload["app_color"] == "blue"
    assert "heartbeat_at" in heartbeat_checkpoint.payload
    assert "reconciled_at" in reconcile_checkpoint.payload

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_run_writes_heartbeat_before_startup_warmup(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-startup-warmup.db",
        daemon_start_with_reconcile=True,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=60,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    stream_service = FakeStreamService()
    reconciliation_service = FakeReconciliationService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        reconciliation_service,  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    async def wait_for_heartbeat_checkpoint() -> Checkpoint:
        while True:
            async with session_factory() as session:
                checkpoint = (
                    await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:demo:blue"))
                ).scalar_one_or_none()
            if checkpoint is not None:
                return checkpoint
            await asyncio.sleep(0.01)

    task = asyncio.create_task(daemon.run(max_messages=1))
    try:
        checkpoint = await asyncio.wait_for(wait_for_heartbeat_checkpoint(), timeout=10)
        assert checkpoint.payload["app_color"] == "blue"
        assert "heartbeat_at" in checkpoint.payload
        assert reconciliation_service.calls == []
        assert stream_service.calls == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_restarts_unbounded_stream_instead_of_stopping_heartbeat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-stream-supervision.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        weather_research_refresh_interval_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    stream_service = FakeStreamService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
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

    result = await daemon.run(run_seconds=0.1)

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:demo:blue"))
        ).scalar_one()

    assert result["completed"] == "timer"
    assert stream_service.calls == [["WX-DISCOVERED"]]
    assert checkpoint.payload["reason"] == "market_stream_restart"
    assert checkpoint.payload["details"]["restart_count"] == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_stays_fresh_during_startup_discovery(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-startup-discovery.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=1,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    discovery_service = BlockingDiscoveryService()
    stream_service = FakeStreamService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        discovery_service,  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    async def wait_for_heartbeat_count(expected_count: int) -> list[OpsEvent]:
        while True:
            async with session_factory() as session:
                events = (
                    await session.execute(
                        select(OpsEvent)
                        .where(OpsEvent.source == "daemon", OpsEvent.summary == "Daemon heartbeat")
                        .order_by(OpsEvent.created_at.asc())
                    )
                ).scalars().all()
            if len(events) >= expected_count:
                return list(events)
            await asyncio.sleep(0.05)

    task = asyncio.create_task(daemon.run(max_messages=1))
    try:
        await asyncio.wait_for(discovery_service.started.wait(), timeout=2)
        events = await asyncio.wait_for(wait_for_heartbeat_count(2), timeout=3)
        assert len(events) >= 2
        assert stream_service.calls == []
        discovery_service.release.set()
        result = await asyncio.wait_for(task, timeout=2)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()

    assert result["completed"] == "stream"
    assert stream_service.calls == [["WX-DISCOVERED"]]


@pytest.mark.asyncio
async def test_daemon_heartbeat_uses_settings_kalshi_env_for_control_state(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-prod-control.db",
        kalshi_env="production",
        app_color="blue",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "green",
            kalshi_env="demo",
            initial_active_color="green",
            initial_kill_switch_enabled=True,
        )
        await repo.ensure_deployment_control(
            "blue",
            kalshi_env="production",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once(run_follow_up=False)

    async with session_factory() as session:
        checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:production:blue")
            )
        ).scalar_one()
        await session.commit()

    assert payload["active_color"] == "blue"
    assert payload["kill_switch_enabled"] is False
    assert checkpoint.payload["kalshi_env"] == "production"
    assert checkpoint.payload["active_color"] == "blue"

    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_spot_current_loop_collects_immediately_for_active_color(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-spot-current.db",
        kalshi_env="production",
        app_color="blue",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        crypto_spot_current_auto_enabled=True,
        crypto_spot_current_interval_seconds=30,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "blue",
            kalshi_env="production",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    spot_service = FakeCryptoSpotService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        crypto_spot_service=spot_service,  # type: ignore[arg-type]
    )

    task = asyncio.create_task(daemon._periodic_crypto_spot_current_loop())
    try:
        await asyncio.wait_for(spot_service.collected.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert spot_service.collect_current_calls == [{"frequency": "15m"}]

    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_loop_runs_next_pass_as_soon_as_previous_finishes(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-autonomy-continuous.db",
        kalshi_env="production",
        app_color="blue",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        crypto_autonomy_interval_seconds=30,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "blue",
            kalshi_env="production",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    autonomy_service = FakeCryptoAutonomyService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        crypto_autonomy_service=autonomy_service,  # type: ignore[arg-type]
    )

    task = asyncio.create_task(daemon._periodic_crypto_autonomy_loop())
    try:
        await asyncio.wait_for(autonomy_service.second_started.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert autonomy_service.overlapped is False
    assert autonomy_service.run_once_calls[:2] == [{"frequency": "15m"}, {"frequency": "15m"}]

    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_only_daemon_runs_crypto_loops_with_role_scoped_heartbeat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-only.db",
        kalshi_env="production",
        app_color="blue",
        daemon_start_with_reconcile=True,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        weather_research_refresh_interval_seconds=0,
        crypto_auto_frequencies="1h",
        crypto_spot_current_auto_enabled=True,
        crypto_spot_current_interval_seconds=30,
        crypto_autonomy_interval_seconds=30,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "blue",
            kalshi_env="production",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    stream_service = FakeStreamService()
    reconciliation_service = FakeReconciliationService()
    spot_service = FakeCryptoSpotService()
    autonomy_service = SlowCryptoAutonomyService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        reconciliation_service,  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        crypto_spot_service=spot_service,  # type: ignore[arg-type]
        crypto_autonomy_service=autonomy_service,  # type: ignore[arg-type]
    )

    result = await daemon.run(run_seconds=0.08, crypto_only=True, heartbeat_role="crypto_1h")

    async with session_factory() as session:
        role_checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:production:blue:crypto_1h")
            )
        ).scalar_one()
        default_checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:production:blue")
            )
        ).scalar_one_or_none()
        default_reconcile = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_reconcile:production:blue")
            )
        ).scalar_one_or_none()

    assert result["mode"] == "crypto_only"
    assert result["daemon_role"] == "crypto_1h"
    assert result["crypto_auto_frequencies"] == ["1h"]
    assert stream_service.calls == []
    assert reconciliation_service.calls == []
    assert spot_service.collect_current_calls[:1] == [{"frequency": "1h"}]
    assert autonomy_service.run_once_calls
    assert all(call == {"frequency": "1h"} for call in autonomy_service.run_once_calls)
    assert role_checkpoint.payload["daemon_role"] == "crypto_1h"
    assert default_checkpoint is None
    assert default_reconcile is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_inactive_daemon_color_does_not_run_market_update_side_effects(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-inactive-color.db",
        kalshi_env="demo",
        app_color="green",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        trigger_enable_auto_rooms=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "green",
            kalshi_env="demo",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    stream_service = FakeStreamService(updates=["WX-TEST"])
    research = FakeResearchCoordinator()
    auto_trigger = FakeAutoTriggerService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        research,  # type: ignore[arg-type]
        auto_trigger,  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    result = await daemon.run(max_messages=1)

    assert result["completed"] == "stream"
    assert stream_service.calls == [["WX-DISCOVERED"]]
    assert research.market_updates == []
    assert auto_trigger.market_updates == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_throttles_repeated_market_update_side_effects(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-update-throttle.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
        daemon_market_update_throttle_seconds=60.0,
        trigger_enable_auto_rooms=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    stream_service = FakeStreamService(updates=["WX-TEST", "WX-TEST"])
    research = FakeResearchCoordinator()
    auto_trigger = FakeAutoTriggerService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        stream_service,  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        research,  # type: ignore[arg-type]
        auto_trigger,  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    result = await daemon.run(max_messages=1)

    assert result["completed"] == "stream"
    assert research.market_updates == ["WX-TEST"]
    assert auto_trigger.market_updates == ["WX-TEST"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_streams_open_position_markets_even_when_discovery_rolls_forward(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-open-position-stream.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.upsert_position(
            market_ticker="WX-HELD",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="no",
            count_fp=Decimal("2.00"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        await session.commit()

    stream_service = FakeStreamService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
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

    result = await daemon.run(max_messages=1)

    assert result["completed"] == "stream"
    assert stream_service.calls == [["WX-DISCOVERED", "WX-HELD"]]

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_runs_checkpoint_capture_without_rooms(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-checkpoint.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    historical_training = FakeHistoricalTrainingService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        historical_training,  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once()

    assert historical_training.capture_calls == 1
    assert payload["checkpoint_capture"]["captured_checkpoint_count"] == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_service_runs_settlement_follow_up_reconcile(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-followup.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        daemon_startup_grace_seconds=0,
        daemon_startup_jitter_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    stream_service = FakeStreamService()

    class CountingReconciliationService(FakeReconciliationService):
        def __init__(self) -> None:
            super().__init__()
            self.call_count = 0

        async def reconcile(self, repo, *, subaccount=0, kalshi_env=""):
            self.call_count += 1
            return ReconcileSummary(
                balances_seen=True,
                positions_count=0,
                orders_count=0,
                fills_count=0,
                settlements_count=0,
                historical_cutoff_seen=True,
            )

    reconcile_service = CountingReconciliationService()
    historical_training = FakeHistoricalTrainingService()
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
        historical_training,  # type: ignore[arg-type]
    )

    await daemon.heartbeat_once()

    async with session_factory() as session:
        followup_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_settlement_followup:demo:blue"))
        ).scalar_one()
        await session.commit()

    assert reconcile_service.call_count == 1
    assert historical_training.backfill_calls == 1
    assert followup_checkpoint.payload["summary"]["status_counts"]["awaiting_settlement"] == 1
    assert followup_checkpoint.payload["settlement_backfill"]["backfilled_count"] == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_runs_historical_pipeline_when_available(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-pipeline.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        historical_pipeline_daily_run_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    pipeline = FakeHistoricalPipelineService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        FakeHistoricalTrainingService(),  # type: ignore[arg-type]
        None,
        pipeline,  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once()

    assert pipeline.daily_calls == 1
    assert payload["historical_pipeline"]["pipeline_kind"] == "daily"

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_promotes_decision_corpus_after_historical_pipeline(tmp_path) -> None:
    events: list[str] = []

    class RecordingHistoricalPipelineService(FakeHistoricalPipelineService):
        async def daily(self):
            events.append("historical_pipeline")
            return await super().daily()

    class RecordingDecisionCorpusService(FakeDecisionCorpusService):
        async def nightly_auto_promote(self, *, kalshi_env: str, actor: str = "daemon"):
            events.append("decision_corpus_promotion")
            return await super().nightly_auto_promote(kalshi_env=kalshi_env, actor=actor)

    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-pipeline-promotion.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        historical_pipeline_daily_run_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    pipeline = RecordingHistoricalPipelineService()
    decision_corpus = RecordingDecisionCorpusService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        FakeHistoricalTrainingService(),  # type: ignore[arg-type]
        historical_pipeline_service=pipeline,  # type: ignore[arg-type]
        decision_corpus_service=decision_corpus,  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once()

    assert events == ["historical_pipeline", "decision_corpus_promotion"]
    assert payload["historical_pipeline"]["pipeline_kind"] == "daily"
    assert payload["decision_corpus_promotion"]["reason"] == "new_resolved_rooms"

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_runs_decision_corpus_promotion_path(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-decision-corpus.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    decision_corpus = FakeDecisionCorpusService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        decision_corpus_service=decision_corpus,  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once()

    assert decision_corpus.calls == [{"kalshi_env": "demo", "actor": "daemon:blue"}]
    assert payload["decision_corpus_promotion"]["reason"] == "new_resolved_rooms"
    async with session_factory() as session:
        checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_decision_corpus_promotion:demo:blue")
            )
        ).scalar_one()
        await session.commit()
    assert checkpoint.payload["result"]["reason"] == "new_resolved_rooms"

    second_payload = await daemon.heartbeat_once()

    assert decision_corpus.calls == [{"kalshi_env": "demo", "actor": "daemon:blue"}]
    assert "decision_corpus_promotion" not in second_payload

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_checkpoint_stays_fresh_while_follow_up_is_running(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-follow-up-scheduling.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        training_campaign_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    shadow_campaign = BlockingShadowCampaignService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        shadow_campaign,  # type: ignore[arg-type]
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    first = await daemon.heartbeat_once(run_follow_up=False)
    daemon._schedule_heartbeat_follow_up(first)
    await asyncio.wait_for(shadow_campaign.started.wait(), timeout=1.0)

    second = await daemon.heartbeat_once(run_follow_up=False)
    daemon._schedule_heartbeat_follow_up(second)

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:demo:blue"))
        ).scalar_one()

    assert shadow_campaign.calls == 1
    assert checkpoint.payload["heartbeat_at"] == second["heartbeat_at"]

    shadow_campaign.release.set()
    await daemon._await_heartbeat_follow_up()

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_lightweight_heartbeat_updates_checkpoint_without_follow_up(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-lightweight-heartbeat.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        training_campaign_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    shadow_campaign = BlockingShadowCampaignService()
    daemon = DaemonService(
        settings,
        session_factory,
        WeatherMarketDirectory({}),
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        shadow_campaign,  # type: ignore[arg-type]
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_liveness_tick(
        reason="unit_test",
        details={"phase": "before_batch", "batch_index": 1},
    )

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_heartbeat:demo:blue"))
        ).scalar_one()
        heartbeat_events = (
            await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon", OpsEvent.summary == "Daemon heartbeat"))
        ).scalars().all()

    assert payload["lightweight"] is True
    assert payload["reason"] == "unit_test"
    assert checkpoint.payload["lightweight"] is True
    assert checkpoint.payload["reason"] == "unit_test"
    assert checkpoint.payload["details"]["phase"] == "before_batch"
    assert "heartbeat_at" in checkpoint.payload
    assert heartbeat_events == []
    assert shadow_campaign.calls == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_settlement_follow_up_reconcile_failure_logs_specific_warning(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-follow-up-error.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    historical_training = FakeHistoricalTrainingService()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FailingReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(status_counts={"awaiting_settlement": 1}),  # type: ignore[arg-type]
        historical_training,  # type: ignore[arg-type]
    )

    payload = await daemon.heartbeat_once(run_follow_up=False)
    daemon._schedule_heartbeat_follow_up(payload)
    await daemon._await_heartbeat_follow_up()

    async with session_factory() as session:
        summaries = [
            row.summary
            for row in (
                await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.asc()))
            ).scalars()
        ]
        followup_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_settlement_followup:demo:blue"))
        ).scalar_one()

    assert historical_training.backfill_calls == 1
    assert followup_checkpoint.payload["summary"]["status_counts"]["awaiting_settlement"] == 1
    assert "Settlement follow-up reconcile triggered" in summaries
    assert "Settlement follow-up reconcile failed" in summaries
    assert "Daemon heartbeat follow-up error" not in summaries

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_runs_nightly_strategy_codex_once_per_pacific_date(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-codex-nightly.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        strategy_codex_nightly_enabled=True,
        strategy_codex_nightly_timezone="America/Los_Angeles",
        strategy_codex_nightly_hour_local=1,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    now_holder = {"value": datetime(2026, 4, 22, 10, 30, tzinfo=UTC)}
    regression_service = FakeStrategyRegressionService(session_factory, now_fn=lambda: now_holder["value"])
    dashboard_service = FakeStrategyDashboardService()
    codex_service = FakeStrategyCodexService()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            "strategy_regression",
            None,
            {"ran_at": "2026-04-22T07:30:00+00:00", "window_days": 180},
        )
        await session.commit()

    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        strategy_regression_service=regression_service,  # type: ignore[arg-type]
        strategy_codex_service=codex_service,  # type: ignore[arg-type]
        strategy_dashboard_service=dashboard_service,  # type: ignore[arg-type]
    )
    daemon._utc_now = lambda: now_holder["value"]  # type: ignore[method-assign]

    payload = await daemon.heartbeat_once()

    assert regression_service.calls == 1
    assert len(codex_service.calls) == 1
    assert codex_service.calls[0]["modes"] == ["evaluate", "suggest"]
    assert codex_service.calls[0]["trigger_source"] == "nightly"
    assert dashboard_service.calls == [
        {
            "window_days": 180,
            "series_ticker": None,
            "strategy_name": None,
            "include_codex_lab": False,
        }
    ]
    assert payload["strategy_codex_nightly"]["status"] == "completed"
    assert payload["strategy_codex_nightly"]["run_ids"] == ["nightly-evaluate", "nightly-suggest"]

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_strategy_codex_nightly:demo:blue"))
        ).scalar_one()
        await session.commit()

    assert checkpoint.payload["local_date"] == "2026-04-22"

    second_payload = await daemon.heartbeat_once()

    assert len(codex_service.calls) == 1
    assert "strategy_codex_nightly" not in second_payload

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_delegates_nightly_strategy_auto_evolve_when_enabled(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-auto-evolve-nightly.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        strategy_codex_nightly_enabled=True,
        strategy_codex_nightly_timezone="UTC",
        strategy_codex_nightly_hour_local=1,
        strategy_auto_evolve_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    now_holder = {"value": datetime(2026, 4, 22, 2, 30, tzinfo=UTC)}
    regression_service = FakeStrategyRegressionService(session_factory, now_fn=lambda: now_holder["value"])
    dashboard_service = FakeStrategyDashboardService()
    codex_service = FakeStrategyCodexService()
    auto_evolve_service = FakeStrategyAutoEvolveService(session_factory)

    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        strategy_regression_service=regression_service,  # type: ignore[arg-type]
        strategy_codex_service=codex_service,  # type: ignore[arg-type]
        strategy_dashboard_service=dashboard_service,  # type: ignore[arg-type]
        strategy_auto_evolve_service=auto_evolve_service,  # type: ignore[arg-type]
    )
    daemon._utc_now = lambda: now_holder["value"]  # type: ignore[method-assign]

    payload = await daemon.heartbeat_once()

    assert auto_evolve_service.calls == [{"trigger_source": "nightly"}]
    assert codex_service.calls == []
    assert "strategy_codex_nightly" not in payload
    assert payload["strategy_auto_evolve"]["status"] == "completed"
    assert payload["strategy_auto_evolve"]["accepted_strategy"] == "balanced-plus"

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_strategy_auto_evolve:demo"))
        ).scalar_one()
        await session.commit()

    assert checkpoint.payload["trigger_source"] == "nightly"
    assert checkpoint.payload["mode"] == "auto_evolve"

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_catches_up_nightly_strategy_codex_after_local_target_time(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-codex-catchup.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        strategy_codex_nightly_enabled=True,
        strategy_codex_nightly_timezone="America/Los_Angeles",
        strategy_codex_nightly_hour_local=1,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    now_holder = {"value": datetime(2026, 4, 22, 7, 30, tzinfo=UTC)}
    regression_service = FakeStrategyRegressionService(session_factory, now_fn=lambda: now_holder["value"])
    dashboard_service = FakeStrategyDashboardService()
    codex_service = FakeStrategyCodexService()
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            "strategy_regression",
            None,
            {"ran_at": "2026-04-22T07:00:00+00:00", "window_days": 180},
        )
        await session.commit()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        strategy_regression_service=regression_service,  # type: ignore[arg-type]
        strategy_codex_service=codex_service,  # type: ignore[arg-type]
        strategy_dashboard_service=dashboard_service,  # type: ignore[arg-type]
    )
    daemon._utc_now = lambda: now_holder["value"]  # type: ignore[method-assign]

    first_payload = await daemon.heartbeat_once()
    assert "strategy_codex_nightly" not in first_payload
    assert regression_service.calls == 0
    assert codex_service.calls == []

    now_holder["value"] = datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    second_payload = await daemon.heartbeat_once()

    assert regression_service.calls == 1
    assert len(codex_service.calls) == 1
    assert second_payload["strategy_codex_nightly"]["status"] == "completed"

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_heartbeat_skips_nightly_strategy_codex_when_codex_unavailable(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-codex-unavailable.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        strategy_codex_nightly_enabled=True,
        strategy_codex_nightly_timezone="America/Los_Angeles",
        strategy_codex_nightly_hour_local=1,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory({})
    now_holder = {"value": datetime(2026, 4, 22, 10, 30, tzinfo=UTC)}
    regression_service = FakeStrategyRegressionService(session_factory, now_fn=lambda: now_holder["value"])
    dashboard_service = FakeStrategyDashboardService()
    codex_service = FakeStrategyCodexService(available=False)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            "strategy_regression",
            None,
            {"ran_at": "2026-04-22T10:00:00+00:00", "window_days": 180},
        )
        await session.commit()
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),  # type: ignore[arg-type]
        strategy_regression_service=regression_service,  # type: ignore[arg-type]
        strategy_codex_service=codex_service,  # type: ignore[arg-type]
        strategy_dashboard_service=dashboard_service,  # type: ignore[arg-type]
    )
    daemon._utc_now = lambda: now_holder["value"]  # type: ignore[method-assign]

    payload = await daemon.heartbeat_once()

    assert regression_service.calls == 0
    assert codex_service.calls == []
    assert dashboard_service.calls == []
    assert payload["strategy_codex_nightly"]["reason"] == "codex_unavailable"

    async with session_factory() as session:
        checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_strategy_codex_nightly:demo:blue"))
        ).scalar_one()
        summaries = [
            row.summary
            for row in (
                await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.asc()))
            ).scalars()
        ]
        await session.commit()

    assert checkpoint.payload["reason"] == "codex_unavailable"
    assert "Nightly strategy Codex skipped: Codex provider unavailable" in summaries

    await engine.dispose()


# ---------------------------------------------------------------------------
# Fake services for crypto_model_nightly tests
# ---------------------------------------------------------------------------


class FakeCryptoHistoryServiceForNightly:
    def __init__(self, *, strict_rows_by_asset: dict[str, int] | None = None) -> None:
        self.collect_settled_calls: list[dict] = []
        self.bootstrap_calls: list[dict] = []
        self.strict_rows_by_asset = strict_rows_by_asset or {}

    async def collect_settled(self, **kwargs: object) -> dict:
        self.collect_settled_calls.append(dict(kwargs))
        return {"status": "ok"}

    async def bootstrap(self, **kwargs: object) -> dict:
        self.bootstrap_calls.append(dict(kwargs))
        return {"status": "ok"}

    async def status(self, **kwargs: object) -> dict:
        return {
            "status": "ok",
            "quote_evidence": {
                "trade_candidate_support_by_asset": {
                    asset: {"strict_trade_eligible_rows": rows}
                    for asset, rows in self.strict_rows_by_asset.items()
                }
            },
        }


class FakeCryptoForecastServiceForNightly:
    def __init__(self, *, fail_assets: list[str] | None = None) -> None:
        self.train_calls: list[dict] = []
        self.fail_assets = set(fail_assets or [])

    async def train(self, **kwargs: object) -> dict:
        self.train_calls.append(dict(kwargs))
        for asset in (kwargs.get("asset_symbols") or []):
            if asset in self.fail_assets:
                raise RuntimeError(f"fake train failure: {asset}")
        return {"status": "ok"}


class FakeCryptoReplayServiceForNightly:
    def __init__(self) -> None:
        self.run_calls: list[dict] = []
        self.gate_calls: list[dict] = []

    async def run(self, **kwargs: object) -> dict:
        self.run_calls.append(dict(kwargs))
        return {"status": "ok"}

    async def gate(self, **kwargs: object) -> dict:
        self.gate_calls.append(dict(kwargs))
        return {"status": "ok"}


def _make_crypto_nightly_daemon(settings, session_factory, *, history_svc, forecast_svc, replay_svc, spot_svc=None):
    directory = WeatherMarketDirectory({})
    return DaemonService(
        settings,
        session_factory,
        directory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
        FakeShadowTrainingService(),  # type: ignore[arg-type]
        None,
        FakeSelfImproveService(),  # type: ignore[arg-type]
        FakeTrainingCorpusService(),
        crypto_history_service=history_svc,  # type: ignore[arg-type]
        crypto_spot_service=spot_svc,  # type: ignore[arg-type]
        crypto_forecast_service=forecast_svc,  # type: ignore[arg-type]
        crypto_replay_service=replay_svc,  # type: ignore[arg-type]
    )


def _crypto_nightly_settings(tmp_path, *, db_name: str, assets: str = "BTC,ETH") -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/{db_name}",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
        crypto_model_nightly_auto_enabled=True,
        crypto_model_nightly_timezone="UTC",
        crypto_model_nightly_hour_local=3,
        crypto_model_nightly_min_new_strict_rows=60,
        crypto_model_nightly_max_age_hours=336,
        crypto_model_nightly_assets=assets,
        crypto_auto_frequencies="15m",
        crypto_entry_policy_optimizer_auto_enabled=False,
    )


@pytest.mark.asyncio
async def test_daemon_crypto_model_nightly_refreshes_missing_assets(tmp_path) -> None:
    settings = _crypto_nightly_settings(tmp_path, db_name="crypto-nightly-missing.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    now = datetime(2026, 5, 17, 4, 0, tzinfo=UTC)
    history_svc = FakeCryptoHistoryServiceForNightly(strict_rows_by_asset={"BTC": 0, "ETH": 0})
    forecast_svc = FakeCryptoForecastServiceForNightly()
    replay_svc = FakeCryptoReplayServiceForNightly()
    spot_svc = FakeCryptoSpotService()

    daemon = _make_crypto_nightly_daemon(settings, session_factory, history_svc=history_svc, forecast_svc=forecast_svc, replay_svc=replay_svc, spot_svc=spot_svc)
    daemon._utc_now = lambda: now  # type: ignore[method-assign]

    result = await daemon._maybe_run_crypto_model_nightly_if_active()

    assert result is not None
    assert result["refreshed_count"] == 2
    assert result["asset_decisions"] == {"15m": {"BTC": "refreshed", "ETH": "refreshed"}}
    assert len(history_svc.collect_settled_calls) == 2
    assert len(history_svc.bootstrap_calls) == 2
    assert len(forecast_svc.train_calls) == 2
    assert len(replay_svc.run_calls) == 2
    # 2 per-asset gates + 1 final pooled gate
    assert len(replay_svc.gate_calls) == 3

    async with session_factory() as session:
        checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "daemon_crypto_model_nightly:demo:blue")
            )
        ).scalar_one()
        await session.commit()
    assert checkpoint.payload["ran_at"] is not None
    assert checkpoint.payload["refreshed_count"] == 2

    # Second nightly check is idempotent.
    second_result = await daemon._maybe_run_crypto_model_nightly_if_active()
    assert second_result is None
    assert len(forecast_svc.train_calls) == 2

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_crypto_model_nightly_skips_fresh_assets(tmp_path) -> None:
    settings = _crypto_nightly_settings(tmp_path, db_name="crypto-nightly-fresh.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    now = datetime(2026, 5, 17, 4, 0, tzinfo=UTC)
    recent_trained_at = now - timedelta(hours=2)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for asset in ("BTC", "ETH"):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=f"model:{asset}",
                version="v1",
                # Real models are recorded with status "trained" (see
                # CryptoForecastService), never "ready" — the gate must treat a
                # fresh "trained" model as present and skip it.
                status="trained",
                sample_count=1000,
                metrics={},
                payload={},
                kalshi_env="demo",
                trained_at=recent_trained_at,
            )
        await session.commit()

    # < min_new_strict_rows=60 → no new data trigger
    history_svc = FakeCryptoHistoryServiceForNightly(strict_rows_by_asset={"BTC": 5, "ETH": 3})
    forecast_svc = FakeCryptoForecastServiceForNightly()
    replay_svc = FakeCryptoReplayServiceForNightly()

    daemon = _make_crypto_nightly_daemon(settings, session_factory, history_svc=history_svc, forecast_svc=forecast_svc, replay_svc=replay_svc)
    daemon._utc_now = lambda: now  # type: ignore[method-assign]

    result = await daemon._maybe_run_crypto_model_nightly_if_active()
    assert result is not None
    assert result["refreshed_count"] == 0
    assert result["asset_decisions"] == {"15m": {"BTC": "skipped_fresh", "ETH": "skipped_fresh"}}
    assert len(forecast_svc.train_calls) == 0
    assert len(replay_svc.run_calls) == 0
    assert len(replay_svc.gate_calls) == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_crypto_model_nightly_does_not_retry_data_starved_assets(tmp_path) -> None:
    """A model that failed to train for lack of data (status "insufficient_data")
    must stay dormant on subsequent nights — re-attempted only when genuinely new
    strict-as-of rows arrive. This is what keeps assets like BNB 1h (no settled
    markets) from failing training every single night."""
    settings = _crypto_nightly_settings(tmp_path, db_name="crypto-nightly-starved.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    now = datetime(2026, 5, 17, 4, 0, tzinfo=UTC)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for asset in ("BTC", "ETH"):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=f"model:{asset}",
                version="v1",
                status="insufficient_data",
                sample_count=0,
                metrics={},
                payload={},
                kalshi_env="demo",
                trained_at=now - timedelta(days=15),  # well past max_age, but no data
            )
        await session.commit()

    # BTC still has no new strict rows -> stays dormant; ETH crossed the threshold
    # -> gets a fresh attempt.
    history_svc = FakeCryptoHistoryServiceForNightly(strict_rows_by_asset={"BTC": 4, "ETH": 75})
    forecast_svc = FakeCryptoForecastServiceForNightly()
    replay_svc = FakeCryptoReplayServiceForNightly()

    daemon = _make_crypto_nightly_daemon(settings, session_factory, history_svc=history_svc, forecast_svc=forecast_svc, replay_svc=replay_svc)
    daemon._utc_now = lambda: now  # type: ignore[method-assign]

    result = await daemon._maybe_run_crypto_model_nightly_if_active()
    assert result is not None
    # BTC dormant despite being "aged out" — no new data; ETH refreshed on new data.
    assert result["asset_decisions"] == {"15m": {"BTC": "skipped_fresh", "ETH": "refreshed"}}
    assert result["refreshed_count"] == 1
    assert [c.get("asset_symbols") for c in forecast_svc.train_calls] == [["ETH"]]

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_crypto_model_nightly_aged_out_triggers_refresh(tmp_path) -> None:
    settings = _crypto_nightly_settings(tmp_path, db_name="crypto-nightly-aged.db", assets="BTC")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    now = datetime(2026, 5, 17, 4, 0, tzinfo=UTC)
    stale_trained_at = now - timedelta(days=15)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="model:BTC",
            version="v1",
            status="trained",
            sample_count=1000,
            metrics={},
            payload={},
            kalshi_env="demo",
            trained_at=stale_trained_at,
        )
        await session.commit()

    history_svc = FakeCryptoHistoryServiceForNightly(strict_rows_by_asset={"BTC": 0})
    forecast_svc = FakeCryptoForecastServiceForNightly()
    replay_svc = FakeCryptoReplayServiceForNightly()

    daemon = _make_crypto_nightly_daemon(settings, session_factory, history_svc=history_svc, forecast_svc=forecast_svc, replay_svc=replay_svc)
    daemon._utc_now = lambda: now  # type: ignore[method-assign]

    result = await daemon._maybe_run_crypto_model_nightly_if_active()
    assert result is not None
    assert result["asset_decisions"] == {"15m": {"BTC": "refreshed"}}
    assert len(forecast_svc.train_calls) == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_daemon_crypto_model_nightly_error_isolation(tmp_path) -> None:
    settings = _crypto_nightly_settings(tmp_path, db_name="crypto-nightly-err.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    now = datetime(2026, 5, 17, 4, 0, tzinfo=UTC)
    history_svc = FakeCryptoHistoryServiceForNightly(strict_rows_by_asset={"BTC": 0, "ETH": 0})
    # BTC train will fail, ETH succeeds
    forecast_svc = FakeCryptoForecastServiceForNightly(fail_assets=["BTC"])
    replay_svc = FakeCryptoReplayServiceForNightly()

    daemon = _make_crypto_nightly_daemon(settings, session_factory, history_svc=history_svc, forecast_svc=forecast_svc, replay_svc=replay_svc)
    daemon._utc_now = lambda: now  # type: ignore[method-assign]

    result = await daemon._maybe_run_crypto_model_nightly_if_active()
    assert result is not None
    assert result["asset_decisions"]["15m"]["BTC"] == "error"
    assert result["asset_decisions"]["15m"]["ETH"] == "refreshed"
    assert result["refreshed_count"] == 1
    # Final pooled gate still runs for the refreshed asset
    assert len(replay_svc.gate_calls) >= 2

    await engine.dispose()
