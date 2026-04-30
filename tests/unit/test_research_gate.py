from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason
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
from kalshi_bot.weather.models import WeatherMarketMapping


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


# ---------------------------------------------------------------------------
# Dossier freshness gate in build_signal_from_dossier
# ---------------------------------------------------------------------------

_MARKET_SNAPSHOT = {
    "yes_ask_dollars": "0.55",
    "yes_bid_dollars": "0.50",
    "no_ask_dollars": "0.45",
    "volume": 1000,
}


def _make_dossier_aged(now: datetime, age_seconds: float) -> ResearchDossier:
    created = now - timedelta(seconds=age_seconds)
    return ResearchDossier(
        market_ticker="API-FRESH-TEST",
        status="ready",
        mode="structured",
        summary=ResearchSummary(
            narrative="Test",
            bullish_case="Bull",
            bearish_case="Bear",
            unresolved_uncertainties=[],
            settlement_mechanics="Official",
            current_numeric_facts={},
            source_coverage="1 source",
            research_confidence=0.9,
        ),
        freshness=ResearchFreshness(
            refreshed_at=created,
            expires_at=created + timedelta(seconds=900),
            stale=False,
            max_source_age_seconds=0,
        ),
        trader_context=ResearchTraderContext(
            fair_yes_dollars="0.6500",
            confidence=0.9,
            thesis="Thesis",
            structured_source_used=True,
            autonomous_ready=True,
        ),
        gate=ResearchGateVerdict(passed=True, reasons=["OK"], cited_source_keys=[]),
        created_at=created,
    )


def test_build_signal_from_dossier_fresh_dossier_passes() -> None:
    coordinator = make_coordinator()
    now = datetime.now(UTC)
    dossier = _make_dossier_aged(now, age_seconds=60)  # 1 minute old — well within 900s

    signal = coordinator.build_signal_from_dossier(dossier, _MARKET_SNAPSHOT)

    assert signal.stand_down_reason != StandDownReason.DOSSIER_STALE
    assert signal.edge_bps > 0


def test_build_signal_from_dossier_derives_missing_forecast_delta_from_facts() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    ticker = "KXHIGHNY-26APR24-T67"
    coordinator = ResearchCoordinator(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        WeatherMarketDirectory(
            {
                ticker: WeatherMarketMapping(
                    market_ticker=ticker,
                    station_id="KNYC",
                    location_name="New York",
                    latitude=40.0,
                    longitude=-73.0,
                    threshold_f=67.0,
                    series_ticker="KXHIGHNY",
                    operator=">",
                )
            }
        ),
        None,
        WeatherSignalEngine(settings),
        AgentPackService(settings),
    )
    now = datetime.now(UTC)
    dossier = _make_dossier_aged(now, age_seconds=60).model_copy(
        update={
            "market_ticker": ticker,
            "forecast_delta_f": None,
            "summary": _make_dossier_aged(now, age_seconds=60).summary.model_copy(
                update={"current_numeric_facts": {"forecast_high_f": 74.0}}
            ),
            "trader_context": _make_dossier_aged(now, age_seconds=60).trader_context.model_copy(
                update={"forecast_delta_f": None}
            ),
        }
    )

    signal = coordinator.build_signal_from_dossier(
        dossier,
        {
            "market": {
                **_MARKET_SNAPSHOT,
                "ticker": ticker,
            }
        },
    )

    assert signal.forecast_delta_f == 7.0
    assert signal.candidate_trace["forecast_delta_fallback"]["derived"] is True
    assert signal.candidate_trace["forecast_delta_fallback"]["source"] == "numeric_facts_and_market_mapping"


def test_build_signal_from_dossier_derives_delta_from_new_high_keys_and_preserves_zero() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    ticker = "KXHIGHNY-26APR24-T0"
    coordinator = ResearchCoordinator(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        WeatherMarketDirectory(
            {
                ticker: WeatherMarketMapping(
                    market_ticker=ticker,
                    station_id="KNYC",
                    location_name="New York",
                    latitude=40.0,
                    longitude=-73.0,
                    threshold_f=0.0,
                    series_ticker="KXHIGHNY",
                    operator=">",
                )
            }
        ),
        None,
        WeatherSignalEngine(settings),
        AgentPackService(settings),
    )
    now = datetime.now(UTC)
    base = _make_dossier_aged(now, age_seconds=60)
    dossier = base.model_copy(
        update={
            "market_ticker": ticker,
            "forecast_delta_f": None,
            "summary": base.summary.model_copy(
                update={"current_numeric_facts": {"predicted_high_f": 0.0}}
            ),
            "trader_context": base.trader_context.model_copy(update={"forecast_delta_f": None}),
        }
    )

    signal = coordinator.build_signal_from_dossier(
        dossier,
        {"market": {**_MARKET_SNAPSHOT, "ticker": ticker}},
    )

    assert signal.forecast_delta_f == 0.0
    fallback = signal.candidate_trace["forecast_delta_fallback"]
    assert fallback["derived"] is True
    assert fallback["forecast_high_source"] == "predicted_high_f"


def test_build_signal_from_dossier_derives_less_than_delta_with_correct_polarity() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    ticker = "KXLOWCHI-26APR24-T60"
    coordinator = ResearchCoordinator(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        WeatherMarketDirectory(
            {
                ticker: WeatherMarketMapping(
                    market_ticker=ticker,
                    station_id="KMDW",
                    location_name="Chicago",
                    latitude=41.0,
                    longitude=-87.0,
                    threshold_f=60.0,
                    series_ticker="KXLOWCHI",
                    operator="<",
                )
            }
        ),
        None,
        WeatherSignalEngine(settings),
        AgentPackService(settings),
    )
    now = datetime.now(UTC)
    base = _make_dossier_aged(now, age_seconds=60)
    dossier = base.model_copy(
        update={
            "market_ticker": ticker,
            "forecast_delta_f": None,
            "summary": base.summary.model_copy(
                update={"current_numeric_facts": {"nws_forecast_high_f": 55.0}}
            ),
            "trader_context": base.trader_context.model_copy(update={"forecast_delta_f": None}),
        }
    )

    signal = coordinator.build_signal_from_dossier(
        dossier,
        {"market": {**_MARKET_SNAPSHOT, "ticker": ticker}},
    )

    assert signal.forecast_delta_f == 5.0


def test_build_signal_from_dossier_stale_dossier_blocked() -> None:
    coordinator = make_coordinator()
    now = datetime.now(UTC)
    dossier = _make_dossier_aged(now, age_seconds=901)  # just over the 900s limit

    signal = coordinator.build_signal_from_dossier(dossier, _MARKET_SNAPSHOT)

    assert signal.stand_down_reason == StandDownReason.DOSSIER_STALE
    assert signal.recommended_action is None
    assert signal.edge_bps == 0


def test_build_signal_from_dossier_sentinel_fair_yes_blocked() -> None:
    """LLM returning fair_yes=0.5000 (the 'I don't know' sentinel) must be blocked."""
    coordinator = make_coordinator()
    now = datetime.now(UTC)
    dossier = _make_dossier_aged(now, age_seconds=60)
    # Overwrite fair_yes_dollars with the sentinel value
    from decimal import Decimal

    dossier.trader_context.fair_yes_dollars = Decimal("0.5000")

    signal = coordinator.build_signal_from_dossier(dossier, _MARKET_SNAPSHOT)

    assert signal.stand_down_reason == StandDownReason.FORECAST_UNAVAILABLE
    assert signal.recommended_action is None
    assert signal.edge_bps == 0
