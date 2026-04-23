from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.risk import DeterministicRiskEngine
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeProviders:
    async def rewrite_with_metadata(self, *, role, fallback_text, system_prompt, user_prompt, role_config=None):
        return fallback_text, {"provider": "fake", "model": "fake", "temperature": 0.0, "fallback_used": False}

    async def maybe_rewrite(self, *, role, fallback_text, system_prompt, user_prompt):
        return fallback_text

    async def complete_json_with_metadata(self, *, role, fallback_payload, system_prompt, user_prompt, role_config=None, schema_model=None):
        return fallback_payload, {"provider": "fake", "model": "fake", "temperature": 0.0, "fallback_used": False}

    async def maybe_complete_json(self, *, role, fallback_payload, system_prompt, user_prompt, role_config=None, schema_model=None):
        return fallback_payload

    def embed_text(self, text):
        return [0.1] * 16

    async def close(self):
        return None


class FakeWeather:
    async def build_market_snapshot(self, mapping):
        return {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {"properties": {"updated": "2026-04-10T00:00:00+00:00", "periods": []}},
            "observation": {"properties": {"temperature": {"value": 25.0}, "timestamp": "2026-04-10T01:00:00+00:00"}},
            "points": {},
        }

    async def close(self):
        return None


def _make_kalshi(close_time_iso: str | None):
    class FakeKalshi:
        write_credentials = object()

        async def get_market(self, ticker: str) -> dict:
            market: dict = {
                "ticker": ticker,
                "yes_bid_dollars": "0.5400",
                "yes_ask_dollars": "0.5600",
                "no_ask_dollars": "0.4500",
                "last_price_dollars": "0.5500",
                "settlement_sources": ["Official source"],
            }
            if close_time_iso is not None:
                market["close_time"] = close_time_iso
            return {"market": market}

        async def create_order(self, payload):
            return {"order": {"order_id": "order-123", "status": "submitted"}, "echo": payload}

        async def close(self):
            return None

    return FakeKalshi()


def _make_supervisor(settings, session_factory, kalshi):
    providers = FakeProviders()
    agent_pack_service = AgentPackService(settings)
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
    signal_engine = WeatherSignalEngine(settings)
    research_coordinator = ResearchCoordinator(
        settings,
        session_factory,
        kalshi,
        FakeWeather(),  # type: ignore[arg-type]
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
    return WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=kalshi,  # type: ignore[arg-type]
        weather=FakeWeather(),  # type: ignore[arg-type]
        weather_directory=directory,
        agent_pack_service=agent_pack_service,
        signal_engine=signal_engine,
        risk_engine=DeterministicRiskEngine(settings),
        execution_service=ExecutionService(settings, kalshi),  # type: ignore[arg-type]
        memory_service=MemoryService(),
        research_coordinator=research_coordinator,
        training_corpus_service=training_corpus_service,
        agents=AgentSuite(settings, providers),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_supervisor_skips_room_when_market_already_closed(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/app.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    past_close = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    kalshi = _make_kalshi(past_close)
    supervisor = _make_supervisor(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        room = await repo.create_room(
            RoomCreate(name="Close Guard Test", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="close_guard_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        updated_room = await repo.get_room(room.id)
        messages = await repo.list_messages(room.id)

    assert updated_room is not None
    assert updated_room.stage == "complete"
    supervisor_messages = [m for m in messages if m.payload.get("final_status") == "market_closed"]
    assert len(supervisor_messages) == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_supervisor_proceeds_when_market_not_yet_closed(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/app.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    future_close = (datetime.now(UTC) + timedelta(hours=4)).isoformat()
    kalshi = _make_kalshi(future_close)
    supervisor = _make_supervisor(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        room = await repo.create_room(
            RoomCreate(name="Open Market Test", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="open_market_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        updated_room = await repo.get_room(room.id)
        messages = await repo.list_messages(room.id)

    assert updated_room is not None
    # Room ran past the close guard — it should have more messages than just the trigger
    market_closed_messages = [m for m in messages if m.payload.get("final_status") == "market_closed"]
    assert len(market_closed_messages) == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_supervisor_does_not_log_flip_alert_for_zero_count_position(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/app.db"
    settings = Settings(
        database_url=database_url,
        app_color="blue",
        app_shadow_mode=True,
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50,
        risk_max_position_notional_dollars=100,
        risk_max_order_count_fp=20,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    future_close = (datetime.now(UTC) + timedelta(hours=4)).isoformat()
    kalshi = _make_kalshi(future_close)
    supervisor = _make_supervisor(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        await repo.upsert_position(
            market_ticker="WX-TEST",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="no",
            count_fp=Decimal("0.00"),
            average_price_dollars=Decimal("0.5500"),
            raw={"seeded": True},
        )
        room = await repo.create_room(
            RoomCreate(name="Zero Count Position Test", market_ticker="WX-TEST"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    await supervisor.run_room(room.id, reason="zero_count_position_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        ops_events = await repo.list_ops_events(limit=20, kalshi_env=settings.kalshi_env)

    assert all("Latest signal flipped away from held side" not in event.summary for event in ops_events)

    await engine.dispose()
