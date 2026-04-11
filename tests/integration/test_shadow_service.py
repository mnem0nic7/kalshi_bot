from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class FakeDiscoveryService:
    async def list_stream_markets(self) -> list[str]:
        return ["KXHIGHNY-26APR11-T68", "KXHIGHCHI-26APR11-T58"]


class FakeSupervisor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        self.calls.append((room_id, reason))


@pytest.mark.asyncio
async def test_shadow_service_runs_sweep_and_creates_rooms(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/shadow.db",
        app_shadow_mode=True,
        app_enable_kill_switch=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await session.commit()

    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    service = ShadowTrainingService(
        settings,
        session_factory,
        FakeDiscoveryService(),  # type: ignore[arg-type]
        agent_pack_service,
        supervisor,
    )

    results = await service.run_shadow_sweep(limit=1, reason="shadow_test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        await session.commit()

    assert len(results) == 1
    assert results[0].market_ticker == "KXHIGHNY-26APR11-T68"
    assert len(rooms) == 1
    assert rooms[0].market_ticker == "KXHIGHNY-26APR11-T68"
    assert supervisor.calls == [(rooms[0].id, "shadow_test")]

    await engine.dispose()
