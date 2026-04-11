from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate, TrainingBuildRequest
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
        agents=agents,
    )
    training_export_service = TrainingExportService(session_factory)
    corpus_service = TrainingCorpusService(
        settings,
        session_factory,
        DiscoveryService(kalshi, directory),  # type: ignore[arg-type]
        training_export_service,
        directory,
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
        await session.commit()

    assert [item.room_id for item in build_one_items] == [item.room_id for item in build_two_items]
    assert {item.room_id for item in build_one_items} == {room_one.id, room_two.id}

    await engine.dispose()
