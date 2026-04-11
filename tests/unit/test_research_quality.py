from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import (
    ResearchClaim,
    ResearchFreshness,
    ResearchSourceCard,
    ResearchSummary,
    ResearchTraderContext,
)
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


def test_research_quality_summary_rewards_structured_complete_weather_research() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", research_stale_seconds=3600)
    coordinator = ResearchCoordinator(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        WeatherMarketDirectory({}),
        None,
        None,
        None,
    )
    mapping = WeatherMarketMapping(
        market_ticker="WX-TEST",
        market_type="weather",
        station_id="KNYC",
        location_name="NYC",
        latitude=40.0,
        longitude=-73.0,
        threshold_f=80,
    )
    now = datetime.now(UTC)
    sources = [
        ResearchSourceCard(
            source_key="kalshi",
            source_class="kalshi_market",
            trust_tier="primary",
            publisher="Kalshi",
            title="Kalshi market",
            snippet="Market structure and settlement.",
            retrieved_at=now,
        ),
        ResearchSourceCard(
            source_key="weather",
            source_class="weather_structured",
            trust_tier="primary",
            publisher="NWS/NOAA",
            title="Structured weather",
            url="https://api.weather.gov",
            snippet="Forecast supports a warm day.",
            retrieved_at=now - timedelta(minutes=5),
        ),
    ]
    claims = [
        ResearchClaim(source_key="kalshi", claim="Settlement uses official weather observations.", citations=["kalshi"]),
        ResearchClaim(source_key="weather", claim="Forecast high is above the threshold.", stance="supports", citations=["weather"]),
    ]
    summary = ResearchSummary(
        narrative="Structured weather research is available.",
        bullish_case="Forecast favors a yes outcome.",
        bearish_case="A cooler day is still possible.",
        settlement_mechanics="Settlement uses official weather observations.",
        current_numeric_facts={"forecast_high_f": 84, "current_temp_f": 77},
        source_coverage="2 total sources, structured adapter",
        research_confidence=0.82,
    )
    trader_context = ResearchTraderContext(
        fair_yes_dollars=Decimal("0.6800"),
        confidence=0.82,
        thesis="Structured forecast supports yes.",
        source_keys=["kalshi", "weather"],
        numeric_facts={"forecast_high_f": 84, "current_temp_f": 77},
        structured_source_used=True,
        autonomous_ready=True,
    )
    freshness = ResearchFreshness(
        refreshed_at=now,
        expires_at=now + timedelta(hours=1),
        stale=False,
        max_source_age_seconds=300,
    )

    quality = coordinator._quality_summary(
        mapping=mapping,
        sources=sources,
        claims=claims,
        summary=summary,
        trader_context=trader_context,
        freshness=freshness,
        settlement_covered=True,
        contradiction_count=0,
    )

    assert quality.citation_coverage_score == 1.0
    assert quality.settlement_clarity_score == 1.0
    assert quality.structured_completeness_score == 1.0
    assert quality.fair_value_score == 1.0
    assert quality.overall_score >= 0.9
    assert quality.issues == []
