from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
from kalshi_bot.core.schemas import (
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchSourceCard,
    ResearchSummary,
    ResearchTraderContext,
    RoomCreate,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.web.app import create_app


def test_research_api_serves_dossier_and_history(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                run = await repo.create_research_run(market_ticker="API-TEST", trigger_reason="seed")
                dossier = ResearchDossier(
                    market_ticker="API-TEST",
                    status="ready",
                    mode="web",
                    summary=ResearchSummary(
                        narrative="Seed dossier",
                        bullish_case="Bull case",
                        bearish_case="Bear case",
                        unresolved_uncertainties=[],
                        settlement_mechanics="Official rules",
                        current_numeric_facts={"yes_bid_dollars": "0.4200"},
                        source_coverage="1 source",
                        research_confidence=0.55,
                    ),
                    freshness=ResearchFreshness(
                        refreshed_at=datetime.now(UTC),
                        expires_at=datetime.now(UTC) + timedelta(minutes=15),
                        stale=False,
                        max_source_age_seconds=0,
                    ),
                    trader_context=ResearchTraderContext(
                        fair_yes_dollars="0.6100",
                        confidence=0.55,
                        thesis="Seed thesis",
                        source_keys=["seed-src"],
                        web_source_used=True,
                        autonomous_ready=True,
                    ),
                    gate=ResearchGateVerdict(
                        passed=True,
                        reasons=["Research gate passed."],
                        cited_source_keys=["seed-src"],
                    ),
                    sources=[
                        ResearchSourceCard(
                            source_key="seed-src",
                            source_class="web_search",
                            trust_tier="reputable",
                            publisher="reuters.com",
                            title="Seed source",
                            url="https://reuters.com/example",
                            snippet="Seed snippet",
                        )
                    ],
                    claims=[],
                    contradiction_count=0,
                    unresolved_count=0,
                    settlement_covered=True,
                    last_run_id=run.id,
                )
                await repo.upsert_research_dossier(dossier)
                await repo.complete_research_run(run.id, status="completed", payload={"seeded": True})
                await session.commit()

        asyncio.run(seed())

        response = client.get("/api/research/API-TEST")
        assert response.status_code == 200
        assert response.json()["market_ticker"] == "API-TEST"

        history = client.get("/api/research/API-TEST/history")
        assert history.status_code == 200
        assert history.json()["runs"][0]["status"] == "completed"

        called = {"value": False}

        async def fake_refresh(market_ticker: str, *, trigger_reason: str, force: bool = True):
            called["value"] = True
            return None

        container.research_coordinator.refresh_market_dossier = fake_refresh  # type: ignore[method-assign]
        refresh = client.post("/api/research/API-TEST/refresh")
        assert refresh.status_code == 200
        assert refresh.json()["status"] == "scheduled"

    get_settings.cache_clear()


def test_web_pages_render_index_and_room_detail(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        (
            "markets:\n"
            "  - market_ticker: WX-UI\n"
            "    station_id: KNYC\n"
            "    location_name: New York City\n"
            "    latitude: 40.7146\n"
            "    longitude: -74.0071\n"
            "    threshold_f: 80\n"
            "    settlement_source: NWS station observation\n"
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "ui.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> str:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                pack = await container.agent_pack_service.get_pack_for_color(repo, container.settings.app_color)
                room = await repo.create_room(
                    room=RoomCreate(name="UI Room", market_ticker="WX-UI"),
                    active_color=container.settings.app_color,
                    shadow_mode=True,
                    kill_switch_enabled=control.kill_switch_enabled,
                    kalshi_env=container.settings.kalshi_env,
                    agent_pack_version=pack.version,
                )
                await session.commit()
                return room.id

        room_id = asyncio.run(seed())

        index_response = client.get("/")
        assert index_response.status_code == 200
        assert "WX-UI" in index_response.text
        assert "Runtime Health" in index_response.text

        room_response = client.get(f"/rooms/{room_id}")
        assert room_response.status_code == 200
        assert "UI Room" in room_response.text

    get_settings.cache_clear()


def test_status_api_includes_runtime_health(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "status.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.ensure_deployment_control("blue", initial_active_color="blue")
                await repo.set_checkpoint(
                    "daemon_heartbeat:blue",
                    None,
                    {"heartbeat_at": datetime.now(UTC).isoformat()},
                )
                await repo.set_checkpoint(
                    "daemon_heartbeat:green",
                    None,
                    {"heartbeat_at": (datetime.now(UTC) - timedelta(seconds=500)).isoformat()},
                )
                await container.watchdog_service.record_boot(
                    repo,
                    status="success",
                    reason="seed_boot",
                )
                await session.commit()

        asyncio.run(seed())

        response = client.get("/api/status")
        assert response.status_code == 200
        body = response.json()
        assert body["runtime_health"]["active_color"] == "blue"
        assert body["runtime_health"]["colors"]["blue"]["daemon"]["healthy"] is True
        assert body["runtime_health"]["last_boot_recovery"]["reason"] == "seed_boot"

    get_settings.cache_clear()
