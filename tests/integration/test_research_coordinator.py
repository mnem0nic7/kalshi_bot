from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ResearchSourceCard
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.weather import NWSWeatherClient, WeatherProviderError
from kalshi_bot.services.agent_packs import AgentPackService
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


class FailingWeather:
    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict:
        request = httpx.Request("GET", f"https://api.weather.gov/stations/{mapping.station_id}/observations/latest")
        raise httpx.ConnectTimeout("", request=request)


class FakeProviders:
    async def rewrite_with_metadata(self, *, role, fallback_text: str, system_prompt: str, user_prompt: str, role_config=None):
        return fallback_text, {"provider": "fake", "model": "fake-model", "temperature": 0.0, "fallback_used": False}

    async def maybe_rewrite(self, *, role, fallback_text: str, system_prompt: str, user_prompt: str) -> str:
        return fallback_text

    async def complete_json_with_metadata(self, *, role, fallback_payload: dict, system_prompt: str, user_prompt: str, role_config=None, schema_model=None):
        payload = await self.maybe_complete_json(
            role=role,
            fallback_payload=fallback_payload,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        return payload, {"provider": "fake", "model": "fake-model", "temperature": 0.0, "fallback_used": False}

    async def maybe_complete_json(self, *, role, fallback_payload: dict, system_prompt: str, user_prompt: str, role_config=None, schema_model=None) -> dict:
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


def _nws_transport(*, timeout_once: bool = False, always_503: bool = False) -> tuple[httpx.MockTransport, dict[str, int]]:
    calls = {"observation": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/points/25.791,-80.316":
            return httpx.Response(
                200,
                json={
                    "properties": {
                        "forecast": "https://api.weather.gov/gridpoints/MFL/105,51/forecast",
                        "forecastGridData": "https://api.weather.gov/gridpoints/MFL/105,51",
                    }
                },
            )
        if path == "/gridpoints/MFL/105,51/forecast":
            return httpx.Response(
                200,
                json={
                    "properties": {
                        "updated": "2026-04-10T00:00:00+00:00",
                        "periods": [{"isDaytime": True, "temperature": 86, "temperatureUnit": "F"}],
                    }
                },
            )
        if path == "/gridpoints/MFL/105,51":
            return httpx.Response(200, json={"properties": {}})
        if path == "/stations/KMIA/observations/latest":
            calls["observation"] += 1
            if always_503:
                return httpx.Response(503, request=request, json={"title": "Service Unavailable"})
            if timeout_once and calls["observation"] == 1:
                raise httpx.ConnectTimeout("", request=request)
            return httpx.Response(
                200,
                json={
                    "properties": {
                        "temperature": {"value": 27.0},
                        "timestamp": "2026-04-10T01:00:00+00:00",
                    }
                },
            )
        return httpx.Response(404, request=request, json={"title": "Not Found"})

    return httpx.MockTransport(handler), calls


def _miami_directory() -> WeatherMarketDirectory:
    return WeatherMarketDirectory(
        {
            "WX-MIA": WeatherMarketMapping(
                market_ticker="WX-MIA",
                market_type="weather",
                station_id="KMIA",
                location_name="Miami",
                latitude=25.791,
                longitude=-80.316,
                threshold_f=80,
                settlement_source="NWS station observation",
            )
        }
    )


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
        AgentPackService(settings),
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
    assert dossier.trade_regime == "standard"
    assert dossier.capital_bucket == "safe"
    assert dossier.forecast_delta_f == 6.0
    assert dossier.confidence_band == "high"
    assert dossier.model_quality_status == "pass"
    assert dossier.model_quality_reasons == []
    assert dossier.recommended_size_cap_fp is None
    assert record is not None
    assert runs[0].status == "completed"
    assert len(sources) >= 2

    await engine.dispose()


@pytest.mark.asyncio
async def test_research_refresh_retries_transient_nws_timeout_without_failed_run(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/research_weather_retry.db",
        weather_retry_base_delay_seconds=0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    weather = NWSWeatherClient(settings)
    transport, calls = _nws_transport(timeout_once=True)
    await weather.close()
    weather.client = httpx.AsyncClient(
        timeout=30.0,
        transport=transport,
        headers={"User-Agent": settings.weather_user_agent, "Accept": "application/geo+json, application/json"},
    )
    coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        weather,
        _miami_directory(),
        FakeProviders(),  # type: ignore[arg-type]
        WeatherSignalEngine(settings),
        AgentPackService(settings),
    )

    dossier = await coordinator.refresh_market_dossier("WX-MIA", trigger_reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        runs = await repo.list_research_runs(market_ticker="WX-MIA", limit=10)
        await session.commit()

    assert dossier.gate.passed is True
    assert calls["observation"] == 2
    assert [run.status for run in runs] == ["completed"]

    await weather.close()
    await engine.dispose()


@pytest.mark.asyncio
async def test_weather_client_reports_endpoint_and_status_after_retries() -> None:
    settings = Settings(weather_retry_attempts=2, weather_retry_base_delay_seconds=0)
    weather = NWSWeatherClient(settings)
    transport, calls = _nws_transport(always_503=True)
    await weather.close()
    weather.client = httpx.AsyncClient(
        timeout=30.0,
        transport=transport,
        headers={"User-Agent": settings.weather_user_agent, "Accept": "application/geo+json, application/json"},
    )

    with pytest.raises(WeatherProviderError) as exc_info:
        await weather.get_latest_observation("KMIA")

    error = exc_info.value
    assert calls["observation"] == 2
    assert error.error_type == "HTTPStatusError"
    assert error.status_code == 503
    assert "/stations/KMIA/observations/latest" in error.endpoint
    assert "503" in str(error)

    await weather.close()


@pytest.mark.asyncio
async def test_background_research_failure_is_recorded_without_escaping_task(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/research_background_failure.db")
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
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot={"ticker": "WX-TEST"},
            yes_bid_dollars=Decimal("0.4200"),
            yes_ask_dollars=Decimal("0.4400"),
            last_trade_dollars=Decimal("0.4300"),
        )
        await session.commit()

    coordinator = ResearchCoordinator(
        settings,
        session_factory,
        FakeKalshi(),  # type: ignore[arg-type]
        FailingWeather(),  # type: ignore[arg-type]
        directory,
        FakeProviders(),  # type: ignore[arg-type]
        WeatherSignalEngine(settings),
        AgentPackService(settings),
    )

    await coordinator.handle_market_update("WX-TEST")
    await coordinator.wait_for_tasks()
    await coordinator.handle_market_update("WX-TEST")
    await coordinator.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        runs = await repo.list_research_runs(market_ticker="WX-TEST", limit=10)
        events = await repo.list_ops_events(sources=["research"], limit=10)
        failed_checkpoint = await repo.get_checkpoint("research_refresh_failed:demo:WX-TEST")
        await session.commit()

    assert coordinator._inflight_markets == set()
    assert [run.status for run in runs] == ["failed"]
    assert runs[0].error_text
    assert "ConnectTimeout" in runs[0].error_text
    assert events[0].payload["error"]
    assert events[0].payload["error_type"] == "ConnectTimeout"
    assert failed_checkpoint is not None

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
        AgentPackService(settings),
    )

    async def fake_web_sources(mapping, market, *, pack):
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


@pytest.mark.asyncio
async def test_room_delta_uses_same_weather_units_as_dossier(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/research_delta.db")
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
        AgentPackService(settings),
    )

    dossier = await coordinator.refresh_market_dossier("WX-TEST", trigger_reason="test")
    delta = coordinator.build_room_delta(
        dossier=dossier,
        market_response=await FakeKalshi().get_market("WX-TEST"),
        weather_bundle=await FakeWeather().build_market_snapshot(directory.require("WX-TEST")),
    )

    assert delta.changed_fields == []
    assert delta.numeric_fact_updates == {}

    await engine.dispose()
