from __future__ import annotations

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import OpsEvent
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.reconcile import ReconcileSummary
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeStreamService:
    async def stream(self, *, market_tickers, include_private, max_messages, on_market_update=None):
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
    daemon = DaemonService(
        settings,
        session_factory,
        directory,
        FakeStreamService(),  # type: ignore[arg-type]
        FakeReconciliationService(),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        FakeAutoTriggerService(),  # type: ignore[arg-type]
    )

    result = await daemon.run(markets=["WX-TEST"], max_messages=3)

    async with session_factory() as session:
        heartbeat = (
            await session.execute(select(OpsEvent).where(OpsEvent.source == "daemon").order_by(OpsEvent.updated_at.desc()))
        ).scalar_one()

    assert result["completed"] == "stream"
    assert result["processed_messages"] == 3
    assert heartbeat.summary == "Daemon heartbeat"

    await engine.dispose()
