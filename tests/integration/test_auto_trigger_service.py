from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeSupervisor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        self.calls.append((room_id, reason))


@pytest.mark.asyncio
async def test_auto_trigger_creates_one_room_for_actionable_market(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_max_spread_bps=1200,
        trigger_max_concurrent_rooms=4,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot={"market_ticker": "WX-TEST"},
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()
    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        checkpoint = await repo.get_checkpoint("auto_trigger:WX-TEST")
        await session.commit()

    assert len(rooms) == 1
    assert checkpoint is not None
    assert len(supervisor.calls) == 1
    assert supervisor.calls[0][1] == "auto_trigger"

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_skips_when_color_is_inactive(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_inactive.db",
        trigger_enable_auto_rooms=True,
        app_color="blue",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control("green")
        control.active_color = "green"
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot={"market_ticker": "WX-TEST"},
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []

    await engine.dispose()
