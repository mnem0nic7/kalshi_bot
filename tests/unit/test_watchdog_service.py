from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.watchdog import WatchdogService


async def _seed_daemon_checkpoint(repo: PlatformRepository, *, color: str, age_seconds: int) -> None:
    observed_at = (datetime.now(UTC) - timedelta(seconds=age_seconds)).isoformat()
    await repo.set_checkpoint(
        f"daemon_heartbeat:{color}",
        None,
        {
            "heartbeat_at": observed_at,
        },
    )


@pytest.mark.asyncio
async def test_watchdog_marks_daemon_unhealthy_when_heartbeat_is_stale(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/watchdog.db",
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = WatchdogService(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await _seed_daemon_checkpoint(repo, color="blue", age_seconds=200)
        health = await service.daemon_health(repo, color="blue")
        await session.commit()

    assert health["healthy"] is False
    assert health["reason"] == "heartbeat stale"
    await engine.dispose()


@pytest.mark.asyncio
async def test_watchdog_requests_restart_for_inactive_color_only(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/watchdog.db",
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = WatchdogService(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        await _seed_daemon_checkpoint(repo, color="blue", age_seconds=0)
        await _seed_daemon_checkpoint(repo, color="green", age_seconds=200)
        payload = await service.run_once(
            repo,
            app_statuses={"blue": "healthy", "green": "healthy"},
            source="test_watchdog",
        )
        await session.commit()

    assert payload["action"] == "restart_color"
    assert payload["target_color"] == "green"
    await engine.dispose()


@pytest.mark.asyncio
async def test_watchdog_fails_over_after_active_restart_attempt(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/watchdog.db",
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = WatchdogService(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        await _seed_daemon_checkpoint(repo, color="blue", age_seconds=200)
        await _seed_daemon_checkpoint(repo, color="green", age_seconds=0)
        first = await service.run_once(
            repo,
            app_statuses={"blue": "unhealthy", "green": "healthy"},
            source="test_watchdog",
        )
        second = await service.run_once(
            repo,
            app_statuses={"blue": "unhealthy", "green": "healthy"},
            source="test_watchdog",
        )
        control = await repo.get_deployment_control()
        await session.commit()

    assert first["action"] == "restart_color"
    assert first["target_color"] == "blue"
    assert second["action"] == "failover"
    assert second["target_color"] == "green"
    assert control.active_color == "green"
    await engine.dispose()


@pytest.mark.asyncio
async def test_watchdog_requests_stack_restart_when_both_colors_unhealthy(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/watchdog.db",
        daemon_heartbeat_interval_seconds=60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = WatchdogService(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        await _seed_daemon_checkpoint(repo, color="blue", age_seconds=200)
        await _seed_daemon_checkpoint(repo, color="green", age_seconds=200)
        payload = await service.run_once(
            repo,
            app_statuses={"blue": "unhealthy", "green": "unhealthy"},
            source="test_watchdog",
        )
        await session.commit()

    assert payload["action"] == "restart_stack"
    await engine.dispose()
