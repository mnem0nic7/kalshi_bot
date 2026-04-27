from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, OpsEvent
from kalshi_bot.db.repositories import PlatformRepository
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
            await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.desc()))
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
async def test_daemon_heartbeat_uses_settings_kalshi_env_for_control_state(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-prod-control.db",
        kalshi_env="production",
        app_color="blue",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
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
async def test_daemon_streams_open_position_markets_even_when_discovery_rolls_forward(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-open-position-stream.db",
        daemon_start_with_reconcile=False,
        daemon_reconcile_interval_seconds=60,
        daemon_heartbeat_interval_seconds=60,
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

    result = await daemon.run(max_messages=1)

    async with session_factory() as session:
        followup_checkpoint = (
            await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "daemon_settlement_followup:demo:blue"))
        ).scalar_one()
        await session.commit()

    assert result["completed"] == "stream"
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
