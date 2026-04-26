"""Integration tests for MonotonicityArbScannerService (Addition 3, §4.3)."""
from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MonotonicityArbProposal
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.monotonicity_scanner_service import MonotonicityArbScannerService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

STATION = "TBOS"
EVENT_DATE = date(2026, 4, 22)


def _ticker(threshold_f: int) -> str:
    return f"KXHIGH{STATION}-26APR22-T{threshold_f}"


def _market(
    ticker: str,
    yes_ask_dollars: float,
    yes_bid_dollars: float,
    no_ask_dollars: float | None = None,
    status: str = "open",
) -> dict[str, Any]:
    m: dict[str, Any] = {
        "ticker": ticker,
        "status": status,
        "yes_ask_dollars": yes_ask_dollars,
        "yes_bid_dollars": yes_bid_dollars,
    }
    if no_ask_dollars is not None:
        m["no_ask_dollars"] = no_ask_dollars
    return m


def _settings(**overrides) -> Settings:
    base = {
        "database_url": "sqlite+aiosqlite:///:memory:",
        "monotonicity_arb_enabled": True,
        "monotonicity_arb_shadow_only": True,
        "monotonicity_arb_min_net_edge_cents": 2,
        "monotonicity_arb_max_notional_dollars": 25.0,
        "monotonicity_arb_max_proposals_per_minute": 5,
    }
    base.update(overrides)
    return Settings(**base)


class _FakeKalshi:
    """Returns a configurable list of markets from list_markets()."""

    def __init__(self, markets: list[dict[str, Any]]) -> None:
        self._markets = markets

    async def list_markets(self, **_kwargs: Any) -> dict[str, Any]:
        return {"markets": self._markets}


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
    markets: list[dict[str, Any]],
) -> MonotonicityArbScannerService:
    svc = MonotonicityArbScannerService(
        settings=settings,
        session_factory=session_factory,
        kalshi=_FakeKalshi(markets),
    )
    return svc


# Violation fixture: T80 ask=35¢, T85 bid=48¢ → raw edge 13¢ > 6¢ threshold → violation
VIOLATION_MARKETS = [
    _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33, no_ask_dollars=0.67),
    _market(_ticker(85), yes_ask_dollars=0.50, yes_bid_dollars=0.48, no_ask_dollars=0.52),
]

# Clean markets: T80 ask=35¢, T85 bid=30¢ → monotonicity holds
CLEAN_MARKETS = [
    _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33),
    _market(_ticker(85), yes_ask_dollars=0.32, yes_bid_dollars=0.30),
]


# ---------------------------------------------------------------------------
# sweep() — shadow mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_detects_violation_and_persists() -> None:
    """sweep() finds a violation and writes a MonotonicityArbProposal record."""
    settings = _settings()
    session_factory = await _setup_db(settings)
    svc = _make_service(settings, session_factory, VIOLATION_MARKETS)

    proposals = await svc.sweep()

    assert len(proposals) >= 1
    assert proposals[0].execution_outcome == "shadow"
    assert proposals[0].net_edge_cents > 0

    async with session_factory() as session:
        rows = (await session.execute(select(MonotonicityArbProposal))).scalars().all()

    assert len(rows) >= 1
    row = rows[0]
    assert row.ticker_low == _ticker(80)
    assert row.ticker_high == _ticker(85)
    assert row.execution_outcome == "shadow"
    assert row.net_edge_cents > 0


@pytest.mark.asyncio
async def test_sweep_no_violation_on_clean_markets() -> None:
    """Clean orderbook (monotonicity holds) → no proposals emitted."""
    settings = _settings()
    session_factory = await _setup_db(settings)
    svc = _make_service(settings, session_factory, CLEAN_MARKETS)

    proposals = await svc.sweep()

    assert proposals == []

    async with session_factory() as session:
        rows = (await session.execute(select(MonotonicityArbProposal))).scalars().all()

    assert rows == []


@pytest.mark.asyncio
async def test_sweep_risk_blocked_when_kill_switch_on() -> None:
    """Kill switch enabled → proposals get execution_outcome='risk_blocked'."""
    settings = _settings()
    session_factory = await _setup_db(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_kill_switch(True)
        await session.commit()

    svc = _make_service(settings, session_factory, VIOLATION_MARKETS)
    proposals = await svc.sweep()

    assert len(proposals) >= 1
    assert all(p.execution_outcome == "risk_blocked" for p in proposals)

    async with session_factory() as session:
        rows = (await session.execute(select(MonotonicityArbProposal))).scalars().all()

    assert all(r.execution_outcome == "risk_blocked" for r in rows)


@pytest.mark.asyncio
async def test_sweep_disabled_returns_empty() -> None:
    """monotonicity_arb_enabled=False → sweep() returns [] without hitting DB."""
    settings = _settings(monotonicity_arb_enabled=False)
    session_factory = await _setup_db(settings)
    svc = _make_service(settings, session_factory, VIOLATION_MARKETS)

    proposals = await svc.sweep()

    assert proposals == []

    async with session_factory() as session:
        rows = (await session.execute(select(MonotonicityArbProposal))).scalars().all()

    assert rows == []


# ---------------------------------------------------------------------------
# DB persistence fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_proposal_fields_populated() -> None:
    """All required fields on MonotonicityArbProposal are populated correctly."""
    settings = _settings()
    session_factory = await _setup_db(settings)
    svc = _make_service(settings, session_factory, VIOLATION_MARKETS)

    await svc.sweep()

    async with session_factory() as session:
        row = (await session.execute(
            select(MonotonicityArbProposal).limit(1)
        )).scalar_one()

    assert row.station == STATION
    assert row.event_date == EVENT_DATE
    assert row.threshold_low_f == 80.0
    assert row.threshold_high_f == 85.0
    assert row.ask_yes_low_cents == pytest.approx(35.0)
    assert row.ask_no_high_cents == pytest.approx(52.0)
    assert row.total_cost_cents == pytest.approx(87.0)
    assert row.gross_edge_cents > 0
    assert row.fee_estimate_cents > 0
    assert row.net_edge_cents > 0
    assert row.contracts_proposed >= 1
    assert row.detected_at is not None


# ---------------------------------------------------------------------------
# get_status()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_status_returns_counts() -> None:
    """get_status() returns total/shadow counts and enabled flags."""
    settings = _settings()
    session_factory = await _setup_db(settings)
    svc = _make_service(settings, session_factory, VIOLATION_MARKETS)

    await svc.sweep()
    status = await svc.get_status()

    assert status["enabled"] is True
    assert status["shadow_only"] is True
    assert status["total_proposals"] >= 1
    assert status["shadow_proposals"] >= 1
    assert isinstance(status["recent"], list)
