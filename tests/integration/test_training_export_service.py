from __future__ import annotations

from decimal import Decimal

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
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


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
                "settlement_sources": ["Official source"],
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


@pytest.mark.asyncio
async def test_training_export_service_builds_room_bundle_and_role_examples(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/app.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=False,
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
    memory_service = MemoryService()
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
    training_service = TrainingExportService(session_factory)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        room = await repo.create_room(
            RoomCreate(name="Training Room", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="training_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.log_exchange_event(
            "reconcile",
            "settlements",
            {"settlements": [{"market_ticker": "WX-TEST", "realized_pnl_dollars": "2.5000"}]},
        )
        await session.commit()

    bundle = await training_service.build_room_bundle(room.id)

    assert bundle.room["market_ticker"] == "WX-TEST"
    assert bundle.signal is not None
    assert bundle.research_dossier is not None
    assert bundle.strategy_audit is not None
    assert bundle.trade_ticket is not None
    assert bundle.risk_verdict is not None
    assert bundle.orders
    assert bundle.outcome.final_status == "submitted"
    assert bundle.outcome.research_gate_passed is True
    assert bundle.outcome.orders_submitted == 1
    assert bundle.outcome.settlement_seen is True
    assert bundle.outcome.settlement_pnl_dollars == Decimal("2.5000")

    examples = training_service.build_role_training_examples(bundle)
    roles = {example.role for example in examples}

    assert roles == {"researcher", "president", "trader", "memory_librarian"}
    trader_example = next(example for example in examples if example.role == "trader")
    assert trader_example.target["payload"]["market_ticker"] == "WX-TEST"
    assert trader_example.input_context["research_dossier"] is not None
    assert trader_example.messages[0]["role"] == "system"
    assert trader_example.messages[1]["role"] == "user"
    assert trader_example.messages[2]["role"] == "assistant"

    memory_example = next(example for example in examples if example.role == "memory_librarian")
    assert memory_example.input_context["room_outcome"]["final_status"] == "submitted"

    await engine.dispose()
