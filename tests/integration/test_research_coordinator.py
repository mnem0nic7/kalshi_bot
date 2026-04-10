from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ResearchSourceCard
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeKalshi:
    async def get_market(self, ticker: str) -> dict:
        return {
            "market": {
                "ticker": ticker,
                "title": f"{ticker} title",
                "subtitle": "Test settlement",
                "yes_bid_dollars": "0.4200",
                "yes_ask_dollars": "0.4400",
                "no_ask_dollars": "0.5800",
                "last_price_dollars": "0.4300",
                "settlement_sources": ["Official settlement source"],
            }
        }


class FakeWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        return {
            "forecast": {
                "properties": {
                    "updated": "2026-04-10T00:00:00+00:00",
                    "periods": [{"isDaytime": True, "temperature": 86, "temperatureUnit": "F"}],
                }
            },
            "observation": {
                "properties": {
                    "temperature": {"value": 27.0},
                    "timestamp": "2026-04-10T01:00:00+00:00",
                }
            },
            "points": {},
        }


class FakeProviders:
    async def maybe_rewrite(self, *, role, fallback_text: str, system_prompt: str, user_prompt: str) -> str:
        return fallback_text

    async def maybe_complete_json(self, *, role, fallback_payload: dict, system_prompt: str, user_prompt: str) -> dict:
        payload = dict(fallback_payload)
        payload.update(
            {
                "narrative": "Web dossier supports a probabilistic view.",
                "bullish_case": "Recent coverage supports the thesis.",
                "bearish_case": "The event could still resolve the other way.",
                "unresolved_uncertainties": [],
                "fair_yes_dollars": "0.6400",
                "confidence": 0.62,
                "thesis": "Web-backed fair value favors yes.",
            }
        )
        return payload


@pytest.mark.asyncio
async def test_research_refresh_persists_weather_dossier(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/research_weather.db")
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
                settlement_source="NWS station observation",
            )
        }
    )
    coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        FakeProviders(),  # type: ignore[arg-type]
        WeatherSignalEngine(settings),
    )

    dossier = await coordinator.refresh_market_dossier("WX-TEST", trigger_reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        record = await repo.get_research_dossier("WX-TEST")
        runs = await repo.list_research_runs(market_ticker="WX-TEST")
        sources = await repo.list_research_sources(run_id=runs[0].id)
        await session.commit()

    assert dossier.gate.passed is True
    assert dossier.trader_context.structured_source_used is True
    assert record is not None
    assert runs[0].status == "completed"
    assert len(sources) >= 2

    await engine.dispose()


@pytest.mark.asyncio
async def test_research_refresh_supports_generic_web_dossier(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/research_web.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    directory = WeatherMarketDirectory(
        {
            "GEN-TEST": WeatherMarketMapping(
                market_ticker="GEN-TEST",
                market_type="generic",
                display_name="Generic Test Market",
                research_queries=["generic test market latest"],
                settlement_source="Kalshi official rules",
            )
        }
    )
    coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FakeWeather(),  # type: ignore[arg-type]
        directory,
        FakeProviders(),  # type: ignore[arg-type]
        WeatherSignalEngine(settings),
    )

    async def fake_web_sources(mapping, market):
        return [
            ResearchSourceCard(
                source_key="web-1",
                source_class="web_search",
                trust_tier="reputable",
                publisher="reuters.com",
                title="Generic test market coverage",
                url="https://reuters.com/example",
                snippet="Recent reporting supports the monitored event outcome.",
            )
        ]

    coordinator._web_sources = fake_web_sources  # type: ignore[method-assign]

    dossier = await coordinator.refresh_market_dossier("GEN-TEST", trigger_reason="test")

    assert dossier.trader_context.web_source_used is True
    assert dossier.trader_context.fair_yes_dollars == Decimal("0.6400")
    assert dossier.gate.passed is True

    await engine.dispose()
