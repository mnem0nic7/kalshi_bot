from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine
from kalshi_bot.services.decision_trace import replay_decision_trace
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.services.discovery import DiscoveryService
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
        return {
            "market": {
                "ticker": ticker,
                "yes_bid_dollars": "0.5400",
                "yes_ask_dollars": "0.5600",
                "no_ask_dollars": "0.4500",
                "last_price_dollars": "0.5500",
                "volume": 200,
            }
        }

    async def create_order(self, payload: dict) -> dict:
        return {"order": {"order_id": "order-123", "status": "submitted"}, "echo": payload}

    async def close(self) -> None:
        return None


class FakeWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "properties": {
                    "updated": "2026-04-10T00:00:00+00:00",
                    "periods": [
                        {"isDaytime": True, "temperature": 88, "temperatureUnit": "F"},
                    ],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": 25.0},
                    "timestamp": "2026-04-10T01:00:00+00:00",
                }
            },
            "points": {},
        }

    async def close(self) -> None:
        return None


class NearThresholdWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "properties": {
                    "updated": "2026-04-10T00:00:00+00:00",
                    "periods": [
                        {"isDaytime": True, "temperature": 80, "temperatureUnit": "F"},
                    ],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": 20.0},
                    "timestamp": "2026-04-10T01:00:00+00:00",
                }
            },
            "points": {},
        }

    async def close(self) -> None:
        return None


class ResolvedNoWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "properties": {
                    "updated": "2026-04-10T00:00:00+00:00",
                    "periods": [{"isDaytime": True, "temperature": 80, "temperatureUnit": "F"}],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": 11.0},
                    "timestamp": "2026-04-10T18:00:00+00:00",
                }
            },
            "points": {},
        }

    async def close(self) -> None:
        return None


class WideChicagoKalshi:
    write_credentials = object()

    async def get_market(self, ticker: str) -> dict:
        return {
            "market": {
                "ticker": ticker,
                "yes_bid_dollars": "0.0100",
                "yes_ask_dollars": "0.4600",
                "no_ask_dollars": "0.9900",
                "last_price_dollars": "0.9000",
            }
        }

    async def create_order(self, payload: dict) -> dict:
        return {"order": {"order_id": "order-should-not-exist", "status": "submitted"}, "echo": payload}

    async def close(self) -> None:
        return None


class ExtremeEdgeKalshi:
    write_credentials = object()

    async def get_market(self, ticker: str) -> dict:
        return {
            "market": {
                "ticker": ticker,
                "title": "Will the high temp in Austin be >83 on Apr 29, 2026?",
                "subtitle": "84 or above",
                "strike_type": "greater",
                "floor_strike": 0.000083,
                "yes_bid_dollars": "0.2300",
                "yes_ask_dollars": "0.2500",
                "no_ask_dollars": "0.7700",
                "last_price_dollars": "0.2400",
                "volume": 200,
            }
        }

    async def create_order(self, payload: dict) -> dict:
        raise AssertionError("extreme-edge diagnostic should stand down before order creation")

    async def close(self) -> None:
        return None


class ExtremeEdgeWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "properties": {
                    "updated": "2026-04-29T11:45:00+00:00",
                    "periods": [
                        {
                            "isDaytime": True,
                            "temperature": 95,
                            "temperatureUnit": "F",
                            "startTime": "2026-04-29T07:00:00-05:00",
                        },
                    ],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": 23.8889},
                    "timestamp": "2026-04-29T12:00:00+00:00",
                }
            },
            "points": {},
        }

    async def close(self) -> None:
        return None


async def _seed_reconcile_balance(
    repo: PlatformRepository,
    *,
    kalshi_env: str,
    total_capital_dollars: Decimal = Decimal("1000.00"),
) -> None:
    cash_cents = int(total_capital_dollars * Decimal("100"))
    await repo.set_checkpoint(
        f"reconcile:{kalshi_env}",
        None,
        {
            "balance": {
                "balance": cash_cents,
                "portfolio_value": 0,
            },
            "reconciled_at": "2026-04-10T00:00:00+00:00",
        },
    )


@pytest.mark.asyncio
async def test_supervisor_completes_room_workflow(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/app.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
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
    memory_service = MemoryService()  # type: ignore[arg-type]
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
                settlement_source="NWS station observation",
            )
        }
    )
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(FakeKalshi(), directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=FakeKalshi(),  # type: ignore[arg-type]
        weather=FakeWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=memory_service,
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        room = await repo.create_room(
            RoomCreate(name="Test Room", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        stored_room = await repo.get_room(room.id)
        messages = await repo.list_messages(room.id)
        audit = await repo.get_room_strategy_audit(room.id)
        weather_snapshots = await repo.list_historical_weather_snapshots(station_id="KNYC")
        await session.commit()

    assert stored_room is not None
    assert stored_room.stage == "complete"
    assert any(message.role == "trader" for message in messages)
    assert any(message.kind == "ExecReceipt" for message in messages)
    assert audit is not None
    assert audit.audit_source == "live_forward"
    assert weather_snapshots
    assert weather_snapshots[0].source_hash is not None

    await engine.dispose()


@pytest.mark.asyncio
async def test_deterministic_fast_path_persists_replayable_decision_trace(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/deterministic_trace.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=False,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
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
    directory = WeatherMarketDirectory(
        {
            "WX-TRACE": WeatherMarketMapping(
                market_ticker="WX-TRACE",
                market_type="weather",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
                settlement_source="NWS station observation",
            )
        }
    )
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(FakeKalshi(), directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=FakeKalshi(),  # type: ignore[arg-type]
        weather=FakeWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=MemoryService(),  # type: ignore[arg-type]
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        room = await repo.create_room(
            RoomCreate(name="Deterministic Trace", market_ticker="WX-TRACE"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="deterministic_trace_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        decision_trace = await repo.get_latest_decision_trace_for_room(room.id)
        trade_ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        market_artifact = await repo.get_latest_artifact(room_id=room.id, artifact_type="market_snapshot")
        supervisor_messages = [
            message
            for message in await repo.list_messages(room.id)
            if message.role == "supervisor" and message.stage == "complete"
        ]
        await session.commit()

    assert decision_trace is not None
    assert trade_ticket is not None
    assert decision_trace.ticket_id == trade_ticket.id
    assert decision_trace.decision_kind == "entry"
    assert decision_trace.path_version == "deterministic-fast-path.v1"
    assert decision_trace.trace["normalized_intent"]["risk_status"] == "approved"
    assert market_artifact is not None
    assert market_artifact.source == "kalshi_rest"
    assert market_artifact.payload["market"]["observed_at"] is not None
    assert decision_trace.source_snapshot_ids["market_snapshot_artifact_id"] == market_artifact.id
    assert replay_decision_trace(decision_trace.trace, expected_trace_hash=decision_trace.trace_hash).ok
    assert supervisor_messages[-1].payload["decision_trace_id"] == decision_trace.id

    await engine.dispose()


@pytest.mark.asyncio
async def test_extreme_edge_diagnostic_stands_down_before_ticket_creation(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/extreme_edge_diagnostic.db"
    settings = Settings(
        database_url=database_url,
        historical_weather_archive_path=str(tmp_path / "weather_archive"),
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=False,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    providers = FakeProviders()
    agent_pack_service = AgentPackService(settings)
    agents = AgentSuite(settings, providers)  # type: ignore[arg-type]
    signal_engine = WeatherSignalEngine(settings)
    risk_engine = DeterministicRiskEngine(settings)
    kalshi = ExtremeEdgeKalshi()
    weather = ExtremeEdgeWeather()
    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHAUS": WeatherSeriesTemplate(
                series_ticker="KXHIGHAUS",
                location_name="Austin",
                station_id="KAUS",
                timezone_name="America/Chicago",
                latitude=30.1975,
                longitude=-97.6664,
            )
        },
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
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(kalshi, directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
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
        execution_service=ExecutionService(settings, kalshi),  # type: ignore[arg-type]
        memory_service=MemoryService(),  # type: ignore[arg-type]
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        await repo.upsert_historical_weather_snapshot(
            station_id="KAUS",
            series_ticker="KXHIGHAUS",
            local_market_day="2026-04-29",
            asof_ts=datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
            source_kind="archived_weather_bundle",
            source_id="seed:prior",
            source_hash="seed",
            observation_ts=datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
            forecast_updated_ts=datetime(2026, 4, 29, 9, 45, tzinfo=UTC),
            forecast_high_f=Decimal("83.00"),
            current_temp_f=Decimal("74.00"),
            payload={"source": "prior_station_day_snapshot"},
        )
        room = await repo.create_room(
            RoomCreate(name="Extreme Edge", market_ticker="KXHIGHAUS-26APR29-T83"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="extreme_edge_diagnostic_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        decision_trace = await repo.get_latest_decision_trace_for_room(room.id)
        trade_ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        signal = await repo.get_latest_signal_for_room(room.id)
        await session.commit()

    assert trade_ticket is None
    assert risk_verdict is None
    assert decision_trace is not None
    assert decision_trace.decision_kind == "stand_down"
    trace_candidate = decision_trace.trace["candidate_trace"]
    diagnostic = trace_candidate["extreme_edge_diagnostic"]
    assert trace_candidate["eligibility_stand_down_reason"] == "extreme_edge_diagnostic_failed"
    assert diagnostic["passed"] is False
    assert "station_daily_high_source_disagreement" in diagnostic["reason_codes"]
    assert diagnostic["checks"]["current_observed_temp_sanity"]["passed"] is True
    assert diagnostic["checks"]["market_metadata_polarity_verification"]["passed"] is True
    assert signal is not None
    assert signal.payload["eligibility"]["eligible"] is False

    await engine.dispose()


@pytest.mark.asyncio
async def test_supervisor_stands_down_on_resolved_contract_before_risk(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/resolved.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    providers = FakeProviders()
    agent_pack_service = AgentPackService(settings)
    agents = AgentSuite(settings, providers)  # type: ignore[arg-type]
    signal_engine = WeatherSignalEngine(settings)
    risk_engine = DeterministicRiskEngine(settings)
    execution_service = ExecutionService(settings, WideChicagoKalshi())  # type: ignore[arg-type]
    memory_service = MemoryService()  # type: ignore[arg-type]
    directory = WeatherMarketDirectory(
        {
            "KXHIGHCHI-26APR10-T51": WeatherMarketMapping(
                market_ticker="KXHIGHCHI-26APR10-T51",
                market_type="weather",
                station_id="KMDW",
                location_name="Chicago",
                timezone_name="America/Chicago",
                latitude=41.7868,
                longitude=-87.7522,
                threshold_f=51,
                operator="<",
                settlement_source="NWS station observation",
            )
        }
    )
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        WideChicagoKalshi(),  # type: ignore[arg-type]
        ResolvedNoWeather(),  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(WideChicagoKalshi(), directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=WideChicagoKalshi(),  # type: ignore[arg-type]
        weather=ResolvedNoWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=memory_service,
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        room = await repo.create_room(
            RoomCreate(name="Resolved Room", market_ticker="KXHIGHCHI-26APR10-T51"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="resolved_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        messages = await repo.list_messages(room.id)
        trade_ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        signal = await repo.get_latest_signal_for_room(room.id)
        await session.commit()

    trader_messages = [message for message in messages if message.role == "trader"]
    assert trade_ticket is None
    assert risk_verdict is None
    assert trader_messages
    assert trader_messages[-1].payload["decision"] == "stand_down"
    assert trader_messages[-1].payload["stand_down_reason"] == "resolved_contract"
    assert signal is not None
    assert signal.payload["resolution_state"] == "locked_no"
    assert signal.payload["eligibility"]["eligible"] is False

    await engine.dispose()


@pytest.mark.asyncio
async def test_supervisor_executes_bucket_resized_trade_size(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/bucketed.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=True,
        risk_min_edge_bps=10,
        risk_order_pct=1.0,
        risk_position_pct=1.0,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
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
    memory_service = MemoryService()  # type: ignore[arg-type]
    directory = WeatherMarketDirectory(
        {
            "WX-NEAR": WeatherMarketMapping(
                market_ticker="WX-NEAR",
                market_type="weather",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
                settlement_source="NWS station observation",
            )
        }
    )
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(FakeKalshi(), directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=FakeKalshi(),  # type: ignore[arg-type]
        weather=FakeWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=memory_service,
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(
            repo,
            kalshi_env=settings.kalshi_env,
            total_capital_dollars=Decimal("20.00"),
        )
        seed_room = await repo.create_room(
            RoomCreate(name="Seed Risky", market_ticker="OTHER-RISK"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await repo.save_signal(
            room_id=seed_room.id,
            market_ticker="OTHER-RISK",
            fair_yes_dollars=Decimal("0.5000"),
            edge_bps=80,
            confidence=0.6,
            summary="Seed safe signal",
            payload={"trade_regime": "standard", "capital_bucket": "safe"},
        )
        await repo.upsert_position(
            market_ticker="OTHER-RISK",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("43.00"),
            average_price_dollars=Decimal("0.4000"),
            raw={"seeded": True},
        )
        room = await repo.create_room(
            RoomCreate(name="Bucketed Room", market_ticker="WX-NEAR"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="bucket_resize_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        trade_ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        signal = await repo.get_latest_signal_for_room(room.id)
        orders = await repo.list_orders_for_room(room.id)
        await session.commit()

    assert trade_ticket is not None
    assert risk_verdict is not None
    assert signal is not None
    assert orders
    assert trade_ticket.count_fp == Decimal("20.00")
    assert risk_verdict.approved_count_fp == Decimal("5.00")
    assert risk_verdict.payload["resized_by_bucket"] is True
    assert risk_verdict.payload["capital_bucket"] == "safe"
    assert orders[0].count_fp == Decimal("5.00")
    assert signal.payload["capital_bucket"] == "safe"

    await engine.dispose()


# ---------------------------------------------------------------------------
# Opposite-side guard integration tests
# ---------------------------------------------------------------------------

async def _make_supervisor_for_opposite_side_test(tmp_path):
    """Return (settings, engine, session_factory, supervisor) wired with FakeWeather/FakeKalshi."""
    database_url = f"sqlite+aiosqlite:///{tmp_path}/opp_side.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=500,
        risk_max_order_count_fp=50,
        risk_allow_position_add_ons=True,
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
                settlement_source="NWS station observation",
            )
        }
    )
    providers = FakeProviders()
    agent_pack_service = AgentPackService(settings)
    agents = AgentSuite(settings, providers)  # type: ignore[arg-type]
    signal_engine = WeatherSignalEngine(settings)
    risk_engine = DeterministicRiskEngine(settings)
    execution_service = ExecutionService(settings, FakeKalshi())  # type: ignore[arg-type]
    memory_service = MemoryService()  # type: ignore[arg-type]
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        providers,  # type: ignore[arg-type]
        signal_engine,
        agent_pack_service,
    )
    training_corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(FakeKalshi(), directory),  # type: ignore[arg-type]
        TrainingExportService(session_factory),
        directory,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=FakeKalshi(),  # type: ignore[arg-type]
        weather=FakeWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=risk_engine,
        execution_service=execution_service,
        memory_service=memory_service,
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=agents,
    )
    return settings, engine, session_factory, supervisor


@pytest.mark.asyncio
async def test_supervisor_blocks_opposite_side_entry_end_to_end(tmp_path) -> None:
    """Seeding a NO position and running the supervisor with a BUY-YES signal produces
    a BLOCKED risk verdict due to the opposite-side guard."""
    settings, engine, session_factory, supervisor = await _make_supervisor_for_opposite_side_test(tmp_path)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        await repo.upsert_position(
            market_ticker="WX-TEST",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="no",
            count_fp=Decimal("5.00"),
            average_price_dollars=Decimal("0.4200"),
            raw={"seeded": True},
        )
        room = await repo.create_room(
            RoomCreate(name="Opp Side Test", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="opp_side_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        orders = await repo.list_orders_for_room(room.id)
        await session.commit()

    assert risk_verdict is not None
    assert risk_verdict.status == "blocked"
    assert any("opposite-side" in r for r in risk_verdict.reasons)
    assert len(orders) == 0, "No orders should be placed when the opposite-side guard fires"

    await engine.dispose()


@pytest.mark.asyncio
async def test_supervisor_guard_fires_when_both_sides_already_held(tmp_path) -> None:
    """When list_positions_for_ticker returns two rows (multi-row anomaly), the supervisor
    selects the canonical row by max count_fp, logs a data_inconsistency ops_event,
    and the opposite-side guard blocks the new entry."""
    settings, engine, session_factory, supervisor = await _make_supervisor_for_opposite_side_test(tmp_path)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await _seed_reconcile_balance(repo, kalshi_env=settings.kalshi_env)
        room = await repo.create_room(
            RoomCreate(name="Multi-Row Test", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    # Simulate the impossible multi-row DB state: NO(8) canonical, YES(3) stale
    no_pos = MagicMock()
    no_pos.count_fp = Decimal("8.00")
    no_pos.side = "no"
    yes_pos = MagicMock()
    yes_pos.count_fp = Decimal("3.00")
    yes_pos.side = "yes"

    with patch(
        "kalshi_bot.db.repositories.PlatformRepository.list_positions_for_ticker",
        new_callable=AsyncMock,
        return_value=[no_pos, yes_pos],
    ):
        await supervisor.run_room(room.id, reason="multi_row_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        ops_events = await repo.list_ops_events(sources=["supervisor"])
        orders = await repo.list_orders_for_room(room.id)
        await session.commit()

    assert risk_verdict is not None
    assert risk_verdict.status == "blocked"
    assert any("opposite-side" in r for r in risk_verdict.reasons)
    assert any("data_inconsistency" in e.summary for e in ops_events), (
        "Expected a data_inconsistency ops_event for the multi-row position anomaly"
    )
    assert len(orders) == 0

    await engine.dispose()
