from __future__ import annotations

from threading import Event

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
from kalshi_bot.web.app import create_app


def test_run_room_endpoint_rejects_malformed_json(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    called = Event()

    with TestClient(app) as client:
        async def fake_run_room(room_id: str, *, reason: str) -> None:
            called.set()

        client.app.state.container.supervisor.run_room = fake_run_room  # type: ignore[method-assign]
        response = client.post(
            "/api/rooms/room-123/run",
            content=b'{"reason": ',
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Malformed JSON body"
    assert not called.is_set()
    get_settings.cache_clear()


def test_run_room_endpoint_allows_empty_body_with_default_reason(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    called = Event()
    captured: dict[str, str] = {}

    with TestClient(app) as client:
        async def fake_run_room(room_id: str, *, reason: str) -> None:
            captured["room_id"] = room_id
            captured["reason"] = reason
            called.set()

        client.app.state.container.supervisor.run_room = fake_run_room  # type: ignore[method-assign]
        response = client.post("/api/rooms/room-123/run", content=b"")

    assert response.status_code == 200
    assert response.json() == {"status": "scheduled", "room_id": "room-123"}
    assert called.wait(timeout=1.0)
    assert captured == {"room_id": "room-123", "reason": "manual"}
    get_settings.cache_clear()


def test_shadow_run_endpoint_creates_room_and_schedules_workflow(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    called = Event()
    captured: dict[str, str] = {}

    with TestClient(app) as client:
        async def fake_run_room(room_id: str, *, reason: str) -> None:
            captured["room_id"] = room_id
            captured["reason"] = reason
            called.set()

        client.app.state.container.supervisor.run_room = fake_run_room  # type: ignore[method-assign]
        response = client.post(
            "/api/markets/KXHIGHNY-26APR11-T68/shadow-run",
            json={"name": "shadow nyc", "reason": "ui_shadow_run"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "scheduled"
    assert body["market_ticker"] == "KXHIGHNY-26APR11-T68"
    assert body["redirect"].startswith("/rooms/")
    assert called.wait(timeout=1.0)
    assert captured["room_id"] == body["room_id"]
    assert captured["reason"] == "ui_shadow_run"
    get_settings.cache_clear()
