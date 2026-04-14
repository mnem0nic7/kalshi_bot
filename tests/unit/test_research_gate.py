from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import (
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchSourceCard,
    ResearchSummary,
    ResearchTraderContext,
)
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.weather.mapping import WeatherMarketDirectory


def make_coordinator() -> ResearchCoordinator:
    return ResearchCoordinator(  # type: ignore[arg-type]
        Settings(database_url="sqlite+aiosqlite:///./test.db"),
        None,
        None,
        None,
        WeatherMarketDirectory({}),
        None,
        WeatherSignalEngine(Settings(database_url="sqlite+aiosqlite:///./test.db")),
        AgentPackService(Settings(database_url="sqlite+aiosqlite:///./test.db")),
    )


def test_research_gate_blocks_missing_fair_value_and_settlement_gap() -> None:
    coordinator = make_coordinator()
    now = datetime.now(UTC)

    verdict = coordinator._gate_dossier(
        sources=[
            ResearchSourceCard(
                source_key="src-1",
                source_class="web_search",
                trust_tier="reputable",
                publisher="reuters.com",
                title="Example source",
                url="https://reuters.com/example",
                snippet="Example snippet",
                retrieved_at=now,
            )
        ],
        summary=ResearchSummary(
            narrative="Example",
            bullish_case="Bull",
            bearish_case="Bear",
            unresolved_uncertainties=["Settlement timing is unclear."],
            settlement_mechanics="Unknown",
            current_numeric_facts={},
            source_coverage="1 source",
            research_confidence=0.2,
        ),
        trader_context=ResearchTraderContext(
            fair_yes_dollars=None,
            confidence=0.2,
            thesis="Incomplete thesis",
        ),
        freshness=ResearchFreshness(
            refreshed_at=now,
            expires_at=now + timedelta(minutes=5),
            stale=False,
            max_source_age_seconds=0,
        ),
        settlement_covered=False,
    )

    assert verdict.passed is False
    assert any("settlement" in reason.lower() for reason in verdict.reasons)
    assert any("fair-value" in reason.lower() for reason in verdict.reasons)


def _make_stale_dossier(now: datetime, minutes_past_expiry: int) -> ResearchDossier:
    return ResearchDossier(
        market_ticker="API-STALE",
        status="ready",
        mode="structured",
        summary=ResearchSummary(
            narrative="Fresh when written",
            bullish_case="Bull",
            bearish_case="Bear",
            unresolved_uncertainties=[],
            settlement_mechanics="Official rules",
            current_numeric_facts={},
            source_coverage="1 source",
            research_confidence=0.8,
        ),
        freshness=ResearchFreshness(
            refreshed_at=now - timedelta(minutes=30),
            expires_at=now - timedelta(minutes=minutes_past_expiry),
            stale=False,
            max_source_age_seconds=0,
        ),
        trader_context=ResearchTraderContext(
            fair_yes_dollars="0.6100",
            confidence=0.8,
            thesis="Thesis",
            structured_source_used=True,
            autonomous_ready=True,
        ),
        gate=ResearchGateVerdict(
            passed=True,
            reasons=["Research gate passed."],
            cited_source_keys=["src-1"],
        ),
        sources=[
            ResearchSourceCard(
                source_key="src-1",
                source_class="weather_structured",
                trust_tier="primary",
                publisher="NWS",
                title="Structured weather",
                url="https://api.weather.gov",
                snippet="Structured source",
                retrieved_at=now - timedelta(minutes=30),
            )
        ],
        claims=[],
        contradiction_count=0,
        unresolved_count=0,
        settlement_covered=True,
    )


def test_runtime_hydration_recomputes_staleness_from_expires_at() -> None:
    """A dossier expired well beyond the grace window is hard-blocked."""
    coordinator = make_coordinator()
    now = datetime.now(UTC)
    # Default: stale_seconds=900 (15 min), grace_factor=2.0 → grace window = 30 min.
    # Expired 35 minutes ago → beyond grace → gate blocks.
    dossier = _make_stale_dossier(now, minutes_past_expiry=35)

    hydrated = coordinator._hydrate_runtime_fields(dossier)

    assert hydrated.freshness.stale is True
    assert hydrated.freshness.stale_grace is False
    assert hydrated.gate.passed is False
    assert any("stale" in reason.lower() for reason in hydrated.gate.reasons)


def test_runtime_hydration_stale_within_grace_passes_with_tolerance() -> None:
    """A dossier stale but within the grace window passes with stale_tolerance_active=True."""
    coordinator = make_coordinator()
    now = datetime.now(UTC)
    # Expired 5 minutes ago → within the 30-min grace window → passes with tolerance.
    dossier = _make_stale_dossier(now, minutes_past_expiry=5)

    hydrated = coordinator._hydrate_runtime_fields(dossier)

    assert hydrated.freshness.stale is True
    assert hydrated.freshness.stale_grace is True
    assert hydrated.gate.passed is True
    assert hydrated.gate.stale_tolerance_active is True
    assert any("stale tolerance" in reason.lower() for reason in hydrated.gate.reasons)
