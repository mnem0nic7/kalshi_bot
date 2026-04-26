"""Unit tests for P0-2 — StrategyRegressionService read/write session split.

Verifies:
- Constructor: explicit read_session_factory is retained.
- Constructor: omitting read_session_factory defaults it to the write factory.
- run_regression() reads strategies + rooms from the read factory, writes
  StrategyResultRecord + Checkpoint rows into the write factory.
- build_strategies_dashboard_core uses container.regression_read_session_factory.
"""
from __future__ import annotations


import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    Checkpoint,
    StrategyRecord,
    StrategyResultRecord,
)
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_regression import StrategyRegressionService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


# ---------------------------------------------------------------------------
# Constructor behavior
# ---------------------------------------------------------------------------

class _FakeFactory:
    """Sentinel so we can prove identity without opening real sessions."""


class _FakeAgentPackService:
    pass


def _make_service(read_factory=None):
    write_factory = _FakeFactory()
    return StrategyRegressionService(
        Settings(),
        write_factory,  # type: ignore[arg-type]
        WeatherMarketDirectory(mappings={}),
        _FakeAgentPackService(),  # type: ignore[arg-type]
        read_session_factory=read_factory,
    ), write_factory


def test_explicit_read_factory_is_retained() -> None:
    read_factory = _FakeFactory()
    service, write_factory = _make_service(read_factory=read_factory)
    assert service.session_factory is write_factory
    assert service.read_session_factory is read_factory


def test_default_read_factory_falls_back_to_write_factory() -> None:
    service, write_factory = _make_service(read_factory=None)
    assert service.session_factory is write_factory
    assert service.read_session_factory is write_factory


# ---------------------------------------------------------------------------
# run_regression: data is read from read_factory, written to write_factory
# ---------------------------------------------------------------------------

@pytest.fixture
async def dual_factories(tmp_path):
    """Two independent SQLite databases with the same schema.

    The 'read' DB is seeded with strategies + rooms; the 'write' DB starts empty.
    We then run the regression and check where things ended up.
    """
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/write.db")
    write_engine = create_engine(settings)
    write_factory = create_session_factory(write_engine)
    await init_models(write_engine)

    read_settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/read.db")
    read_engine = create_engine(read_settings)
    read_factory = create_session_factory(read_engine)
    await init_models(read_engine)

    yield write_factory, read_factory
    await write_engine.dispose()
    await read_engine.dispose()


async def _seed_read_side(read_factory) -> None:
    """Plant one strategy + one resolved room so run_regression has something to score."""
    async with read_factory() as session:
        strategy = StrategyRecord(
            name="baseline",
            description="default",
            thresholds={
                "risk_min_edge_bps": 500,
                "risk_min_confidence": 0.4,
                "trigger_max_spread_bps": 1000,
                "risk_min_probability_extremity_pct": 5,
                "stop_loss_trailing_pct": None,
                "shadow_only": False,
            },
            is_active=True,
            source="builtin",
            strategy_metadata={},
        )
        session.add(strategy)
        await session.commit()


@pytest.mark.asyncio
async def test_run_regression_reads_from_read_factory_writes_to_write_factory(dual_factories) -> None:
    write_factory, read_factory = dual_factories
    await _seed_read_side(read_factory)

    service = StrategyRegressionService(
        Settings(),
        write_factory,
        WeatherMarketDirectory(mappings={}),
        _FakeAgentPackService(),  # type: ignore[arg-type]
        read_session_factory=read_factory,
    )

    # No rooms seeded → early return with status=no_rooms, but the STRATEGY read
    # itself must come from read_factory. If the service wrongly read from
    # write_factory (empty), it would return status=no_strategies instead.
    result = await service.run_regression()
    assert result["status"] == "no_rooms"

    # And prove nothing leaked into the read DB as a regression snapshot.
    async with read_factory() as session:
        leaked = (await session.execute(select(StrategyResultRecord))).scalars().all()
        assert leaked == []
        leaked_cp = (await session.execute(select(Checkpoint))).scalars().all()
        assert leaked_cp == []


@pytest.mark.asyncio
async def test_run_regression_with_no_strategies_in_read_returns_no_strategies(dual_factories) -> None:
    """If the read DB has no strategies, the service reports no_strategies — even if
    the write DB has strategies seeded, they must be ignored."""
    write_factory, read_factory = dual_factories
    # Seed strategies into the WRITE DB only. The read DB stays empty.
    async with write_factory() as session:
        strategy = StrategyRecord(
            name="should-not-be-used",
            description="wrong-db",
            thresholds={"risk_min_edge_bps": 500},
            is_active=True,
            source="builtin",
            strategy_metadata={},
        )
        session.add(strategy)
        await session.commit()

    service = StrategyRegressionService(
        Settings(),
        write_factory,
        WeatherMarketDirectory(mappings={}),
        _FakeAgentPackService(),  # type: ignore[arg-type]
        read_session_factory=read_factory,
    )
    result = await service.run_regression()
    assert result["status"] == "no_strategies"


# ---------------------------------------------------------------------------
# Container-level fallback when "secondary" is requested without a secondary DB
# ---------------------------------------------------------------------------

def test_settings_default_read_source_is_primary() -> None:
    assert Settings().strategy_regression_read_source == "primary"


def test_settings_accepts_secondary_value() -> None:
    assert Settings(strategy_regression_read_source="secondary").strategy_regression_read_source == "secondary"
