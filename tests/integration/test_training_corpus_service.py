from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomStage
from kalshi_bot.core.schemas import RoomCreate, StrategyAuditResult, TrainingBuildRequest, TrainingRoomBundle, TrainingRoomOutcome
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping, WeatherSeriesTemplate


class FakeProviders:
    async def rewrite_with_metadata(self, *, role, fallback_text: str, system_prompt: str, user_prompt: str, role_config=None):
        return fallback_text, {"provider": "fake", "model": "fake-model", "temperature": 0.0, "fallback_used": False}

    async def maybe_rewrite(self, *, role, fallback_text: str, system_prompt: str, user_prompt: str) -> str:
        return fallback_text

    async def complete_json_with_metadata(self, *, role, fallback_payload: dict, system_prompt: str, user_prompt: str, role_config=None, schema_model=None):
        return fallback_payload, {"provider": "fake", "model": "fake-model", "temperature": 0.0, "fallback_used": False}

    async def maybe_complete_json(self, *, role, fallback_payload: dict, system_prompt: str, user_prompt: str, role_config=None, schema_model=None) -> dict:
        return fallback_payload

    def embed_text(self, text: str) -> list[float]:
        return [0.1] * 16

    async def close(self) -> None:
        return None


class FakeKalshi:
    write_credentials = object()

    async def get_market(self, ticker: str) -> dict:
        base = {
            "WX-ONE": {"yes_bid_dollars": "0.5400", "yes_ask_dollars": "0.5600", "no_ask_dollars": "0.4500", "last_price_dollars": "0.5500"},
            "WX-TWO": {"yes_bid_dollars": "0.4300", "yes_ask_dollars": "0.4500", "no_ask_dollars": "0.5600", "last_price_dollars": "0.4400"},
        }[ticker]
        return {"market": {"ticker": ticker, "settlement_sources": ["Official source"], **base}}

    async def create_order(self, payload: dict) -> dict:
        return {"order": {"order_id": f"order-{payload['client_order_id']}", "status": "submitted"}, "echo": payload}

    async def close(self) -> None:
        return None


class FakeWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        temp = 88 if mapping.market_ticker == "WX-ONE" else 74
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "properties": {
                    "updated": "2026-04-10T00:00:00+00:00",
                    "periods": [{"isDaytime": True, "temperature": temp, "temperatureUnit": "F"}],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": (temp - 1 - 32) * 5 / 9},
                    "timestamp": "2026-04-10T01:00:00+00:00",
                }
            },
            "points": {},
        }

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_training_corpus_service_builds_reproducible_weather_dataset(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/training-corpus.db",
        app_color="blue",
        app_shadow_mode=True,
        training_min_complete_rooms=2,
        training_min_market_diversity=2,
        training_min_settled_rooms=1,
        training_min_trade_positive_rooms=1,
        training_good_research_threshold=0.5,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    providers = FakeProviders()
    agent_pack_service = AgentPackService(settings)
    agents = AgentSuite(settings, providers)  # type: ignore[arg-type]
    signal_engine = WeatherSignalEngine(settings)
    risk_engine = DeterministicRiskEngine(settings)
    execution_service = ExecutionService(settings, FakeKalshi())  # type: ignore[arg-type]
    memory_service = MemoryService(providers)  # type: ignore[arg-type]
    directory = WeatherMarketDirectory(
        {
            "WX-ONE": WeatherMarketMapping(
                market_ticker="WX-ONE",
                market_type="weather",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            ),
            "WX-TWO": WeatherMarketMapping(
                market_ticker="WX-TWO",
                market_type="weather",
                station_id="KORD",
                location_name="Chicago",
                latitude=41.0,
                longitude=-87.0,
                threshold_f=78,
            ),
        }
    )
    kalshi = FakeKalshi()
    weather = FakeWeather()
    training_export_service = TrainingExportService(session_factory)
    corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(kalshi, directory),  # type: ignore[arg-type]
        training_export_service,
        directory,
    )
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        kalshi,  # type: ignore[arg-type]
        weather,  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=kalshi,  # type: ignore[arg-type]
        weather=weather,  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=memory_service,
        research_coordinator=research_coordinator,
        training_corpus_service=corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_kill_switch_enabled=False)
        room_one = await repo.create_room(
            RoomCreate(name="Room One", market_ticker="WX-ONE"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        room_two = await repo.create_room(
            RoomCreate(name="Room Two", market_ticker="WX-TWO"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room_one.id, reason="training_corpus_test")
    await supervisor.run_room(room_two.id, reason="training_corpus_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.log_exchange_event(
            "reconcile",
            "settlements",
            {
                "settlements": [
                    {"market_ticker": "WX-ONE", "realized_pnl_dollars": "2.5000"},
                    {"market_ticker": "WX-TWO", "realized_pnl_dollars": "-1.2500"},
                ]
            },
        )
        await session.commit()

    status = await corpus_service.get_status(persist_readiness=True)
    assert status["room_count"] == 2
    assert status["unsettled_complete_room_count"] == 0
    assert status["oldest_unsettled_room_age_seconds"] is None
    assert status["settled_label_velocity"]["24h"] == 2
    assert status["readiness"]["ready_for_sft_export"] is True
    assert status["readiness"]["ready_for_critique"] is True
    assert status["readiness"]["ready_for_evaluation"] is True

    request = TrainingBuildRequest(mode="room-bundles", limit=10, days=30, good_research_only=True)
    build_one = await corpus_service.build_dataset(request)
    build_two = await corpus_service.build_dataset(request)

    assert build_one["build"]["room_count"] == 2
    assert build_one["build"]["label_stats"]["settlement_seen"] == 2
    assert build_one["build"]["label_stats"]["good_research"] == 2

    async with session_factory() as session:
        repo = PlatformRepository(session)
        build_one_items = await repo.list_training_dataset_build_items(build_one["build"]["id"])
        build_two_items = await repo.list_training_dataset_build_items(build_two["build"]["id"])
        audit_one = await repo.get_room_strategy_audit(room_one.id)
        audit_two = await repo.get_room_strategy_audit(room_two.id)
        await session.commit()

    assert [item.room_id for item in build_one_items] == [item.room_id for item in build_two_items]
    assert {item.room_id for item in build_one_items} == {room_one.id, room_two.id}
    assert audit_one is not None
    assert audit_two is not None

    await engine.dispose()


@pytest.mark.asyncio
async def test_training_status_separates_active_and_legacy_failure_noise(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/training-status.db",
        app_color="blue",
        app_shadow_mode=True,
        training_status_room_limit=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHNY": WeatherSeriesTemplate(
                series_ticker="KXHIGHNY",
                display_name="NYC Daily High Temperature",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
            )
        },
    )

    class NoopDiscoveryService:
        async def discover_configured_markets(self):
            return []

    corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        NoopDiscoveryService(),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_kill_switch_enabled=True)
        room = await repo.create_room(
            RoomCreate(name="shadow room", market_ticker="KXHIGHNY-26APR12-T70"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env=settings.kalshi_env,
        )
        await repo.update_room_stage(room.id, RoomStage.COMPLETE)
        await repo.save_artifact(
            room_id=room.id,
            artifact_type="market_snapshot",
            source="test",
            title="market snapshot",
            payload={"market": {"ticker": "KXHIGHNY-26APR12-T70", "close_ts": int((datetime.now(UTC) - timedelta(hours=3)).timestamp())}},
        )
        active_run = await repo.create_research_run(
            market_ticker="KXHIGHNY-26APR12-T70",
            trigger_reason="test_active_failure",
        )
        await repo.complete_research_run(active_run.id, status="failed", error_text="404 market not found")
        legacy_run = await repo.create_research_run(
            market_ticker="WEATHER-NYC-HIGH-80F",
            trigger_reason="test_legacy_failure",
        )
        await repo.complete_research_run(legacy_run.id, status="failed", error_text="404 market not found")
        stale_audit = StrategyAuditResult(
            room_id=room.id,
            market_ticker="KXHIGHNY-26APR12-T70",
            thesis_correctness="correct",
            trade_quality="weak_trade",
            block_correctness="correct_block",
            stale_data_mismatch=True,
            audit_source="live_forward",
            audit_version="weather-quality-v1",
            trainable_default=False,
            exclude_reason="stale_data_mismatch",
            quality_warnings=["stale_data_mismatch", "weak_trade"],
        )
        await repo.upsert_room_strategy_audit(
            room_id=stale_audit.room_id,
            market_ticker=stale_audit.market_ticker,
            audit_source=stale_audit.audit_source or "live_forward",
            audit_version=stale_audit.audit_version or "weather-quality-v1",
            thesis_correctness=stale_audit.thesis_correctness,
            trade_quality=stale_audit.trade_quality,
            block_correctness=stale_audit.block_correctness,
            missed_stand_down=stale_audit.missed_stand_down,
            stale_data_mismatch=stale_audit.stale_data_mismatch,
            effective_freshness_agreement=stale_audit.effective_freshness_agreement,
            resolution_state=stale_audit.resolution_state,
            eligibility_passed=stale_audit.eligibility_passed,
            stand_down_reason=stale_audit.stand_down_reason,
            trainable_default=stale_audit.trainable_default,
            exclude_reason=stale_audit.exclude_reason,
            quality_warnings=stale_audit.quality_warnings,
            payload=stale_audit.model_dump(mode="json"),
        )
        await session.commit()

    status = await corpus_service.get_status(persist_readiness=False)

    assert status["room_count"] == 1
    assert status["unsettled_complete_room_count"] == 1
    assert status["oldest_unsettled_room_age_seconds"] is not None
    assert status["active_failed_research_reasons"] == {"market lookup failures": 1}
    assert status["legacy_failed_research_reasons"] == {"market lookup failures": 1}
    assert status["failed_research_reasons"] == {"market lookup failures": 1}
    assert status["campaign_settings"]["rooms_per_run"] == settings.training_campaign_rooms_per_run
    assert status["campaign_settings"]["daemon_reconcile_interval_seconds"] == settings.daemon_reconcile_interval_seconds
    assert status["recent_exclusion_memory"]["by_market"][0]["market_ticker"] == "KXHIGHNY-26APR12-T70"
    assert status["quality_debt_summary"]["recent_stale_mismatch_count"] == 1
    assert status["settlement_maturity"]["status_counts"]["possible_ingestion_gap"] == 1
    assert status["unsettled_backlog_by_market"] == {"KXHIGHNY-26APR12-T70": 1}

    await engine.dispose()


@pytest.mark.asyncio
async def test_strategy_audit_classifies_correct_thesis_but_weak_trade(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-audit.db")
    corpus_service = TrainingCorpusService(
        settings,
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        WeatherMarketDirectory({}),
    )

    bundle = TrainingRoomBundle(
        room={"id": "a29a4ded-e1c0-4d16-ba48-1d432b415476", "market_ticker": "KXHIGHCHI-26APR11-T51"},
        signal={
            "fair_yes_dollars": "0.0003",
            "payload": {
                "resolution_state": "locked_no",
                "eligibility": {
                    "eligible": False,
                    "stand_down_reason": "resolved_contract",
                    "remaining_payout_dollars": "0.0100",
                    "market_spread_bps": 4500,
                },
            },
        },
        trade_ticket={"market_ticker": "KXHIGHCHI-26APR11-T51", "side": "no", "yes_price_dollars": "0.0100"},
        risk_verdict={"status": "blocked", "reasons": ["Research data is stale."]},
        weather_bundle={
            "mapping": {"operator": "<", "threshold_f": 51},
            "observation": {"properties": {"temperature": {"value": 11.0}}},
        },
        outcome=TrainingRoomOutcome(
            final_status="blocked",
            room_stage="complete",
            shadow_mode=True,
            kill_switch_enabled=True,
            research_gate_passed=True,
            risk_status="blocked",
            resolution_state="locked_no",
            eligibility_passed=False,
            stand_down_reason="resolved_contract",
            blocked_by="risk",
            ticket_generated=True,
            orders_submitted=0,
            fills_observed=0,
        ),
    )

    audit = corpus_service._audit_bundle(bundle)

    assert audit.thesis_correctness == "correct"
    assert audit.trade_quality == "weak_trade"
    assert audit.missed_stand_down is True


@pytest.mark.asyncio
async def test_strategy_audit_classifies_incorrect_locked_yes_thesis(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-audit-incorrect.db")
    corpus_service = TrainingCorpusService(
        settings,
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        WeatherMarketDirectory({}),
    )

    bundle = TrainingRoomBundle(
        room={"id": "room-incorrect", "market_ticker": "KXHIGHNY-26APR11-T80"},
        signal={"fair_yes_dollars": "0.1200", "payload": {"resolution_state": "locked_yes"}},
        weather_bundle={
            "mapping": {"operator": ">", "threshold_f": 80},
            "observation": {"properties": {"temperature": {"value": 27.0}}},
        },
        outcome=TrainingRoomOutcome(
            final_status="stand_down",
            room_stage="complete",
            shadow_mode=True,
            kill_switch_enabled=True,
            research_gate_passed=True,
            resolution_state="locked_yes",
            eligibility_passed=False,
            stand_down_reason="resolved_contract",
            blocked_by="eligibility",
            ticket_generated=False,
        ),
    )

    audit = corpus_service._audit_bundle(bundle)

    assert audit.thesis_correctness == "incorrect"


@pytest.mark.asyncio
async def test_strategy_audit_backfill_is_idempotent(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-audit-backfill.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    directory = WeatherMarketDirectory(
        {
            "WX-BACKFILL": WeatherMarketMapping(
                market_ticker="WX-BACKFILL",
                market_type="weather",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )

    class NoopDiscoveryService:
        async def discover_configured_markets(self):
            return []

    corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        NoopDiscoveryService(),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_kill_switch_enabled=True)
        room = await repo.create_room(
            RoomCreate(name="backfill room", market_ticker="WX-BACKFILL"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env=settings.kalshi_env,
        )
        await repo.update_room_stage(room.id, RoomStage.COMPLETE)
        await session.commit()

    first = await corpus_service.backfill_strategy_audits(days=30, limit=10)
    second = await corpus_service.backfill_strategy_audits(days=30, limit=10)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        audit = await repo.get_room_strategy_audit(room.id)
        await session.commit()

    assert first["created_count"] == 1
    assert second["updated_count"] == 1
    assert audit is not None
    assert audit.audit_source == "historical_backfill"

    await engine.dispose()


@pytest.mark.asyncio
async def test_quality_cleaned_dataset_excludes_bad_audits(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/quality-cleaned.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    class NoopDiscoveryService:
        async def discover_configured_markets(self):
            return []

    corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        NoopDiscoveryService(),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        WeatherMarketDirectory({}),
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_kill_switch_enabled=True)
        good_room = await repo.create_room(
            RoomCreate(name="good room", market_ticker="WX-GOOD"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env=settings.kalshi_env,
        )
        bad_room = await repo.create_room(
            RoomCreate(name="bad room", market_ticker="WX-BAD"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env=settings.kalshi_env,
        )
        await repo.update_room_stage(good_room.id, RoomStage.COMPLETE)
        await repo.update_room_stage(bad_room.id, RoomStage.COMPLETE)
        good_audit = StrategyAuditResult(
            room_id=good_room.id,
            market_ticker="WX-GOOD",
            thesis_correctness="unresolved",
            trade_quality="stand_down",
            block_correctness="not_applicable",
            audit_source="live_forward",
            audit_version="weather-quality-v1",
            trainable_default=True,
        )
        bad_audit = StrategyAuditResult(
            room_id=bad_room.id,
            market_ticker="WX-BAD",
            thesis_correctness="correct",
            trade_quality="weak_trade",
            block_correctness="correct_block",
            stale_data_mismatch=True,
            audit_source="historical_backfill",
            audit_version="weather-quality-v1",
            trainable_default=False,
            exclude_reason="stale_data_mismatch",
            quality_warnings=["stale_data_mismatch", "weak_trade"],
        )
        await repo.upsert_room_strategy_audit(
            room_id=good_audit.room_id,
            market_ticker=good_audit.market_ticker,
            audit_source=good_audit.audit_source or "live_forward",
            audit_version=good_audit.audit_version or "weather-quality-v1",
            thesis_correctness=good_audit.thesis_correctness,
            trade_quality=good_audit.trade_quality,
            block_correctness=good_audit.block_correctness,
            missed_stand_down=good_audit.missed_stand_down,
            stale_data_mismatch=good_audit.stale_data_mismatch,
            effective_freshness_agreement=good_audit.effective_freshness_agreement,
            resolution_state=good_audit.resolution_state,
            eligibility_passed=good_audit.eligibility_passed,
            stand_down_reason=good_audit.stand_down_reason,
            trainable_default=good_audit.trainable_default,
            exclude_reason=good_audit.exclude_reason,
            quality_warnings=good_audit.quality_warnings,
            payload=good_audit.model_dump(mode="json"),
        )
        await repo.upsert_room_strategy_audit(
            room_id=bad_audit.room_id,
            market_ticker=bad_audit.market_ticker,
            audit_source=bad_audit.audit_source or "historical_backfill",
            audit_version=bad_audit.audit_version or "weather-quality-v1",
            thesis_correctness=bad_audit.thesis_correctness,
            trade_quality=bad_audit.trade_quality,
            block_correctness=bad_audit.block_correctness,
            missed_stand_down=bad_audit.missed_stand_down,
            stale_data_mismatch=bad_audit.stale_data_mismatch,
            effective_freshness_agreement=bad_audit.effective_freshness_agreement,
            resolution_state=bad_audit.resolution_state,
            eligibility_passed=bad_audit.eligibility_passed,
            stand_down_reason=bad_audit.stand_down_reason,
            trainable_default=bad_audit.trainable_default,
            exclude_reason=bad_audit.exclude_reason,
            quality_warnings=bad_audit.quality_warnings,
            payload=bad_audit.model_dump(mode="json"),
        )
        await session.commit()

    build = await corpus_service.build_dataset(TrainingBuildRequest(mode="room-bundles", limit=10, days=30))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        items = await repo.list_training_dataset_build_items(build["build"]["id"])
        await session.commit()

    assert build["build"]["room_count"] == 1
    assert build["build"]["label_stats"]["excluded_stale_mismatches"] == 1
    assert [item.room_id for item in items] == [good_room.id]

    await engine.dispose()
