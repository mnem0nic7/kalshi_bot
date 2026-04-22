from __future__ import annotations

import sqlite3

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_session_factory
from kalshi_bot.db.session import create_engine, init_models


@pytest.mark.asyncio
async def test_init_models_creates_core_tables(tmp_path) -> None:
    db_path = tmp_path / "bootstrap.db"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{db_path}")
    engine = create_engine(settings)

    await init_models(engine)
    await engine.dispose()

    conn = sqlite3.connect(db_path)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert "rooms" in tables
    assert "deployment_control" in tables
    assert "ops_events" in tables
    assert "strategy_codex_runs" in tables


@pytest.mark.asyncio
async def test_deployment_control_bootstrap_defaults_are_deterministic(tmp_path) -> None:
    db_path = tmp_path / "bootstrap-control.db"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{db_path}")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control(
            "green",
            initial_active_color="blue",
            initial_kill_switch_enabled=True,
        )
        await session.commit()

    assert control.active_color == "blue"
    assert control.shadow_color == "green"
    assert control.kill_switch_enabled is True

    await engine.dispose()
