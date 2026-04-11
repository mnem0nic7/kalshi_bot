from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ResearchFreshness, ResearchSourceCard, ResearchSummary, ResearchTraderContext
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
