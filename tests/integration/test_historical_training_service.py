from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from kalshi_bot.config import Settings, get_settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RoomOrigin, RoomStage
from kalshi_bot.core.schemas import RoomCreate, RoomMessageCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.web.app import create_app


@pytest.mark.asyncio
async def test_list_rooms_for_learning_excludes_historical_replay_by_default(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/learning.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        shadow_room = await repo.create_room(
            RoomCreate(name="Shadow Room", market_ticker="WX-SHADOW"),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env="demo",
            room_origin=RoomOrigin.SHADOW.value,
        )
        live_room = await repo.create_room(
            RoomCreate(name="Live Room", market_ticker="WX-LIVE"),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="demo",
            room_origin=RoomOrigin.LIVE.value,
        )
        historical_room = await repo.create_room(
            RoomCreate(name="Historical Room", market_ticker="WX-HISTORY"),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="demo",
            room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
        )
        await repo.update_room_stage(shadow_room.id, RoomStage.COMPLETE)
        await repo.update_room_stage(live_room.id, RoomStage.COMPLETE)
        await repo.update_room_stage(historical_room.id, RoomStage.COMPLETE)
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms_for_learning(
            since=datetime.now(UTC) - timedelta(days=1),
            limit=10,
        )
        await session.commit()

    assert {room.id for room in rooms} == {shadow_room.id, live_room.id}
    await engine.dispose()


def test_historical_status_api_and_build_route(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "historical-api.db"
    output_path = tmp_path / "historical_bundles.jsonl"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> tuple[str, str]:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                shadow_room = await repo.create_room(
                    RoomCreate(name="Visible Room", market_ticker="WX-SHADOW"),
                    active_color=container.settings.app_color,
                    shadow_mode=True,
                    kill_switch_enabled=control.kill_switch_enabled,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.SHADOW.value,
                )
                historical_room = await repo.create_room(
                    RoomCreate(name="Historical Replay Room", market_ticker="WX-HISTORY"),
                    active_color=container.settings.app_color,
                    shadow_mode=False,
                    kill_switch_enabled=False,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                    agent_pack_version="builtin-gemini-v1",
                    role_models={"researcher": {"provider": "gemini", "model": "gemini-2.5-pro"}},
                )
                await repo.update_room_stage(shadow_room.id, RoomStage.COMPLETE)
                await repo.update_room_stage(historical_room.id, RoomStage.COMPLETE)
                for role, kind, content in (
                    (AgentRole.RESEARCHER, MessageKind.OBSERVATION, "Research trace"),
                    (AgentRole.PRESIDENT, MessageKind.POLICY_MEMO, "Posture trace"),
                    (AgentRole.TRADER, MessageKind.TRADE_IDEA, "Stand down"),
                    (AgentRole.MEMORY_LIBRARIAN, MessageKind.MEMORY_NOTE, "Memory trace"),
                ):
                    await repo.append_message(
                        historical_room.id,
                        RoomMessageCreate(role=role, kind=kind, stage=RoomStage.COMPLETE, content=content, payload={}),
                    )
                await repo.upsert_historical_settlement_label(
                    market_ticker="WX-HISTORY",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=datetime(2026, 4, 10, 23, 0, tzinfo=UTC),
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={"market": {"ticker": "WX-HISTORY"}},
                )
                await repo.create_historical_replay_run(
                    room_id=historical_room.id,
                    market_ticker="WX-HISTORY",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    checkpoint_label="checkpoint_1",
                    checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
                    status="completed",
                    agent_pack_version="builtin-gemini-v1",
                    payload={
                        "historical_provenance": {
                            "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                            "local_market_day": "2026-04-10",
                            "checkpoint_label": "checkpoint_1",
                            "checkpoint_ts": "2026-04-10T13:00:00+00:00",
                            "timezone_name": "America/New_York",
                            "market_snapshot_source_id": "market-snapshot-1",
                            "weather_snapshot_source_id": "weather-snapshot-1",
                            "settlement_label_id": "settlement-1",
                            "source_coverage": {
                                "market_snapshot": True,
                                "weather_snapshot": True,
                                "settlement_label": True,
                            },
                        }
                    },
                )
                await session.commit()
                return shadow_room.id, historical_room.id

        shadow_room_id, historical_room_id = asyncio.run(seed())

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        assert historical_status.json()["replayed_checkpoint_count"] == 1
        assert "full_checkpoint_coverage_count" in historical_status.json()
        assert "draft_training_ready" in historical_status.json()

        status_response = client.get("/api/status")
        assert status_response.status_code == 200
        status_body = status_response.json()
        assert any(room["market_ticker"] == "WX-SHADOW" for room in status_body["rooms"])
        assert all(room["market_ticker"] != "WX-HISTORY" for room in status_body["rooms"])
        assert status_body["training"]["historical"]["replayed_checkpoint_count"] == 1

        index_response = client.get("/")
        assert index_response.status_code == 200
        assert "Historical Corpus" in index_response.text
        assert "Visible Room" in index_response.text
        assert "Historical Replay Room" not in index_response.text

        build_response = client.post(
            "/api/training/historical/build",
            json={
                "mode": "bundles",
                "date_from": "2026-04-10",
                "date_to": "2026-04-10",
                "output": str(output_path),
            },
        )
        assert build_response.status_code == 200
        payload = build_response.json()
        assert payload["build"]["room_count"] == 1
        assert payload["build"]["draft_only"] is False
        assert payload["build"]["training_ready"] is True
        lines = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert lines[0]["room_origin"] == "historical_replay"
        assert lines[0]["historical_provenance"]["local_market_day"] == "2026-04-10"
        assert lines[0]["settlement_label"]["crosscheck_status"] == "match"
        assert lines[0]["draft_only"] is False
        assert historical_room_id == lines[0]["room"]["id"]

    get_settings.cache_clear()
