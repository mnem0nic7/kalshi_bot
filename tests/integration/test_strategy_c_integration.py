"""Integration tests for Strategy C sweep(), risk gate wiring, and
counterfactual fill-rate harness (Session 8)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MarketPriceHistory, StrategyCRoom
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_cleanup_service import StrategyCleanupService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

TICKER = f"KXHIGHTBOS-{(datetime.now(UTC) + timedelta(days=7)).strftime('%y%b%d').upper()}-T58"
STATION = "KBOS"


def _settings(**overrides) -> Settings:
    base = {
        "database_url": "sqlite+aiosqlite:///:memory:",
        "strategy_c_enabled": True,
        "strategy_c_shadow_only": True,
        "strategy_c_required_consecutive_confirmations": 1,
        "strategy_c_max_observation_age_minutes": 30,
        "strategy_c_max_forecast_residual_f": 8.0,
        "strategy_c_max_cli_variance_degf": 5.0,
        "strategy_c_min_edge_cents": 1,
        "strategy_c_max_book_age_seconds": 60,
        "strategy_c_max_order_notional_dollars": 50.0,
        "strategy_c_max_position_notional_dollars": 100.0,
    }
    base.update(overrides)
    return Settings(**base)


def _directory(
    *,
    ticker: str = TICKER,
    station: str = STATION,
    threshold_f: float = 58.0,
    operator: str = ">=",
) -> WeatherMarketDirectory:
    mapping = WeatherMarketMapping(
        market_ticker=ticker,
        station_id=station,
        threshold_f=threshold_f,
        operator=operator,
        timezone_name="America/New_York",
    )
    return WeatherMarketDirectory({ticker: mapping})


class _FakeWeather:
    """Returns a fresh observation with configurable temp and age."""

    def __init__(self, temp_f: float = 60.0, age_minutes: float = 0.0) -> None:
        self._temp_f = temp_f
        self._age_minutes = age_minutes

    async def get_latest_observation(self, station_id: str) -> dict[str, Any]:
        obs_ts = datetime.now(UTC) - timedelta(minutes=self._age_minutes)
        return {
            "properties": {
                "timestamp": obs_ts.isoformat(),
                "temperature": {
                    "value": (self._temp_f - 32) * 5 / 9,  # celsius
                    "unitCode": "wmoUnit:degC",
                },
            }
        }


class _FakeKalshi:
    """Returns a fresh market snapshot with configurable prices."""

    def __init__(
        self,
        yes_ask: float = 0.97,
        last_price: float = 0.95,
    ) -> None:
        self._yes_ask = yes_ask
        self._last_price = last_price

    async def get_market(self, ticker: str) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        yes_bid = self._yes_ask - 0.02
        return {
            "ticker": ticker,
            "status": "active",
            "yes_ask_dollars": self._yes_ask,
            "no_ask_dollars": round(1.0 - yes_bid, 4),
            "last_price": self._last_price,
            "yes_bid_dollars": yes_bid,
            "observed_at": now,
        }


async def _setup_db(settings: Settings):
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(
            "blue",
            initial_active_color="blue",
            initial_kill_switch_enabled=False,
        )
        await session.commit()
    return session_factory


def _make_service(
    settings: Settings,
    session_factory,
    *,
    weather: _FakeWeather | None = None,
    kalshi: _FakeKalshi | None = None,
    directory: WeatherMarketDirectory | None = None,
) -> StrategyCleanupService:
    return StrategyCleanupService(
        settings=settings,
        session_factory=session_factory,
        kalshi=kalshi or _FakeKalshi(),
        weather=weather or _FakeWeather(),
        weather_directory=directory or _directory(),
    )


# ---------------------------------------------------------------------------
# Kill-switch integration — sweep() must gate on risk when kill switch is on
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_risk_blocked_when_kill_switch_on() -> None:
    """With kill switch enabled, non-suppressed signals get execution_outcome='risk_blocked'."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    # Enable kill switch in DB
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_kill_switch(True)
        await session.commit()

    svc = _make_service(settings, session_factory)
    signals = await svc.sweep()

    # Should have at least one signal (temp 60°F ≥ threshold 58°F → LOCKED_YES)
    assert len(signals) >= 1

    async with session_factory() as session:
        rows = (await session.execute(select(StrategyCRoom))).scalars().all()

    assert len(rows) >= 1
    # All non-suppressed signals must be risk_blocked (kill switch was on)
    for row in rows:
        if row.execution_outcome != "suppressed":
            assert row.execution_outcome == "risk_blocked", (
                f"Expected risk_blocked, got {row.execution_outcome}"
            )


@pytest.mark.asyncio
async def test_sweep_shadow_when_kill_switch_off() -> None:
    """With kill switch off, a passing signal gets execution_outcome='shadow'."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    svc = _make_service(settings, session_factory)
    signals = await svc.sweep()

    async with session_factory() as session:
        rows = (await session.execute(select(StrategyCRoom))).scalars().all()

    shadow_rows = [r for r in rows if r.execution_outcome == "shadow"]
    assert len(shadow_rows) >= 1, "Expected at least one shadow signal with kill switch off"


# ---------------------------------------------------------------------------
# Stale-observation handling — Part C gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_observation_suppressed() -> None:
    """Observation older than max_age_minutes → suppression_reason set, outcome='suppressed'."""
    settings = _settings(strategy_c_max_observation_age_minutes=10)
    session_factory = await _setup_db(settings)

    # Observation is 45 minutes old — exceeds the 10-minute limit
    svc = _make_service(
        settings,
        session_factory,
        weather=_FakeWeather(temp_f=60.0, age_minutes=45.0),
    )
    signals = await svc.sweep()

    assert len(signals) >= 1
    signal = signals[0]
    assert signal.suppression_reason is not None
    assert "freshness" in signal.suppression_reason or "part_c" in signal.suppression_reason.lower()

    async with session_factory() as session:
        rows = (await session.execute(select(StrategyCRoom))).scalars().all()

    assert all(r.execution_outcome == "suppressed" for r in rows)


@pytest.mark.asyncio
async def test_fresh_observation_passes_part_c() -> None:
    """Observation within max_age_minutes → Part C passes."""
    settings = _settings(strategy_c_max_observation_age_minutes=30)
    session_factory = await _setup_db(settings)

    svc = _make_service(
        settings,
        session_factory,
        weather=_FakeWeather(temp_f=60.0, age_minutes=5.0),
    )
    signals = await svc.sweep()

    # Part C should pass; result is shadow or risk_blocked but not suppressed-for-staleness
    non_suppressed = [s for s in signals if s.suppression_reason is None or "freshness" not in s.suppression_reason]
    assert len(non_suppressed) >= 1


# ---------------------------------------------------------------------------
# Divergence event simulation — Part B gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_divergence_suppresses_when_forecast_residual_exceeded() -> None:
    """When the lock tracker carries a gridpoint forecast that diverges greatly,
    Part B should fire. We simulate this by patching LockStateTracker.observe()."""
    from unittest.mock import patch
    from kalshi_bot.services.strategy_cleanup import LockState

    settings = _settings(strategy_c_max_forecast_residual_f=3.0)
    session_factory = await _setup_db(settings)

    divergent_lock_state = LockState(
        station=STATION,
        observation_ts=datetime.now(UTC),
        observed_max_f=60.0,
        consecutive_confirmations=3,
        gridpoint_forecast_f=50.0,  # 10°F divergence > 3°F limit
        cli_variance_degf=None,
    )

    svc = _make_service(settings, session_factory)
    with patch.object(svc._lock_tracker, "observe", return_value=divergent_lock_state):
        signals = await svc.sweep()

    assert len(signals) >= 1
    signal = signals[0]
    assert signal.suppression_reason is not None
    assert "part_b" in signal.suppression_reason.lower() or "forecast" in signal.suppression_reason.lower()


# ---------------------------------------------------------------------------
# Sweep persists StrategyCRoom records
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_persists_strategy_c_room_records() -> None:
    """Every evaluated signal (suppressed or actionable) generates a DB record."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    svc = _make_service(settings, session_factory)
    signals = await svc.sweep()

    async with session_factory() as session:
        rows = (await session.execute(select(StrategyCRoom))).scalars().all()

    assert len(rows) == len(signals)
    for row in rows:
        assert row.ticker == TICKER
        assert row.station == STATION
        assert row.execution_outcome in ("shadow", "suppressed", "risk_blocked")


# ---------------------------------------------------------------------------
# Counterfactual fill-rate harness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_rate_returns_none_when_insufficient_data() -> None:
    """Returns None (not 0%) when fewer than 10 shadow signal records exist."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    svc = _make_service(settings, session_factory)
    # No records seeded → should return None, not 0.0
    result = await svc.compute_counterfactual_fill_rate()
    assert result is None


@pytest.mark.asyncio
async def test_fill_rate_yes_side_fills_when_ask_within_target() -> None:
    """YES-side signals fill when a MarketPriceHistory snapshot has yes_ask <= target."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    now = datetime.now(UTC)
    target_price_cents = 95.0
    target_dollars = Decimal("0.9500")

    async with session_factory() as session:
        # Seed 10 shadow rooms (minimum threshold)
        for i in range(10):
            room = StrategyCRoom(
                ticker=TICKER,
                station=STATION,
                decision_time=now - timedelta(hours=i),
                resolution_state="locked_yes",
                observed_max_at_decision=60.0,
                threshold=58.0,
                fair_value_dollars=Decimal("0.99"),
                modeled_edge_cents=4.0,
                target_price_cents=target_price_cents,
                contracts_requested=1,
                execution_outcome="shadow",
            )
            session.add(room)

        # For the first 8 rooms, add a price snapshot at or below target → fills
        # For the last 2, add no snapshot → no fill
        rooms_snapshot = (
            await session.execute(
                select(StrategyCRoom).order_by(StrategyCRoom.decision_time.desc())
            )
        ).scalars().all()

        for i, room in enumerate(rooms_snapshot[:8]):
            snap = MarketPriceHistory(
                kalshi_env="demo",
                market_ticker=room.ticker,
                yes_ask_dollars=target_dollars - Decimal("0.01"),  # below target → fills
                yes_bid_dollars=target_dollars - Decimal("0.03"),
                mid_dollars=target_dollars,
                observed_at=room.decision_time + timedelta(seconds=2),
            )
            session.add(snap)

        await session.commit()

    svc = _make_service(settings, session_factory)
    rate = await svc.compute_counterfactual_fill_rate(latency_budget_seconds=10)

    assert rate is not None
    assert 0.75 <= rate <= 0.90, f"Expected ~0.8 fill rate, got {rate}"


@pytest.mark.asyncio
async def test_fill_rate_no_side_fills_when_no_ask_within_target() -> None:
    """NO-side signals use no_ask_dollars for fill-rate computation."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    now = datetime.now(UTC)
    target_price_cents = 5.0
    target_dollars = Decimal("0.0500")

    async with session_factory() as session:
        for i in range(10):
            room = StrategyCRoom(
                ticker=TICKER,
                station=STATION,
                decision_time=now - timedelta(hours=i),
                resolution_state="locked_no",
                observed_max_at_decision=55.0,
                threshold=58.0,
                fair_value_dollars=Decimal("0.01"),
                modeled_edge_cents=3.0,
                target_price_cents=target_price_cents,
                contracts_requested=1,
                execution_outcome="shadow",
            )
            session.add(room)

        rooms_snapshot = (
            await session.execute(
                select(StrategyCRoom).order_by(StrategyCRoom.decision_time.desc())
            )
        ).scalars().all()

        # NO ask ≈ 1 - yes_bid. target_dollars=0.05; we want 1 - yes_bid <= 0.05
        # → yes_bid >= 0.95. Set yes_bid=0.96 → NO ask = 0.04 ≤ 0.05 → fills
        for room in rooms_snapshot[:10]:
            snap = MarketPriceHistory(
                kalshi_env="demo",
                market_ticker=room.ticker,
                yes_ask_dollars=Decimal("0.97"),
                yes_bid_dollars=Decimal("0.96"),
                mid_dollars=Decimal("0.05"),
                observed_at=room.decision_time + timedelta(seconds=2),
            )
            session.add(snap)

        await session.commit()

    svc = _make_service(settings, session_factory)
    rate = await svc.compute_counterfactual_fill_rate(latency_budget_seconds=10)

    assert rate is not None
    assert rate == 1.0, f"All 10 should fill, got {rate}"


@pytest.mark.asyncio
async def test_fill_rate_ignores_snapshots_outside_latency_window() -> None:
    """Snapshots arriving after latency_budget_seconds are not counted as fills."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    now = datetime.now(UTC)
    target_price_cents = 95.0
    target_dollars = Decimal("0.9500")

    async with session_factory() as session:
        for i in range(10):
            room = StrategyCRoom(
                ticker=TICKER,
                station=STATION,
                decision_time=now - timedelta(hours=i + 1),
                resolution_state="locked_yes",
                observed_max_at_decision=60.0,
                threshold=58.0,
                fair_value_dollars=Decimal("0.99"),
                modeled_edge_cents=4.0,
                target_price_cents=target_price_cents,
                contracts_requested=1,
                execution_outcome="shadow",
            )
            session.add(room)

        rooms_snapshot = (
            await session.execute(
                select(StrategyCRoom).order_by(StrategyCRoom.decision_time.desc())
            )
        ).scalars().all()

        for room in rooms_snapshot:
            snap = MarketPriceHistory(
                kalshi_env="demo",
                market_ticker=room.ticker,
                yes_ask_dollars=target_dollars - Decimal("0.01"),
                yes_bid_dollars=target_dollars - Decimal("0.03"),
                mid_dollars=target_dollars,
                # 60 seconds after decision — outside 5-second latency budget
                observed_at=room.decision_time + timedelta(seconds=60),
            )
            session.add(snap)

        await session.commit()

    svc = _make_service(settings, session_factory)
    rate = await svc.compute_counterfactual_fill_rate(latency_budget_seconds=5)

    # All snapshots outside window → 0 fills → rate == 0.0
    assert rate is not None
    assert rate == 0.0, f"Snapshots outside window should not count, got {rate}"
