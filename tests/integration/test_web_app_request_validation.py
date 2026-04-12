from __future__ import annotations

from threading import Event

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
from kalshi_bot.core.schemas import ResearchAuditIssue
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


def test_training_status_and_research_audit_endpoints_return_payloads(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()

    with TestClient(app) as client:
        async def fake_training_status(*, persist_readiness: bool = False):
            return {
                "room_count": 3,
                "readiness": {"ready_for_sft_export": True, "ready_for_critique": False},
                "top_missing_data": ["not enough settled rooms"],
            }

        async def fake_research_audit(*, limit: int = 50):
            return [
                ResearchAuditIssue(
                    market_ticker="WX-TEST",
                    severity="high",
                    code="market_lookup_error",
                    summary="Configured weather market could not be discovered.",
                )
            ]

        client.app.state.container.training_corpus_service.get_status = fake_training_status  # type: ignore[method-assign]
        client.app.state.container.training_corpus_service.research_audit = fake_research_audit  # type: ignore[method-assign]

        status_response = client.get("/api/training/status")
        audit_response = client.get("/api/research-audit")

    assert status_response.status_code == 200
    assert status_response.json()["room_count"] == 3
    assert audit_response.status_code == 200
    assert audit_response.json()["issues"][0]["market_ticker"] == "WX-TEST"
    get_settings.cache_clear()


def test_historical_intelligence_and_heuristic_pack_endpoints_return_payloads(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()

    with TestClient(app) as client:
        async def fake_status():
            return {
                "active_pack_version": "heuristic-baseline-v1",
                "candidate_pack_version": "heuristic-candidate-v1",
                "intelligence_window_days": 30,
                "latest_run": {"status": "completed", "row_count": 12},
            }

        async def fake_run(payload):
            return {
                "status": "completed",
                "date_from": payload.date_from,
                "date_to": payload.date_to,
                "candidate_pack_version": "heuristic-candidate-v1",
            }

        async def fake_explain(*, series=None):
            return {"series": series or [], "agent_summary": "summary"}

        async def fake_promote(*, candidate_version=None, reason: str):
            return {"status": "promoted", "candidate_version": candidate_version, "reason": reason}

        async def fake_rollback(*, reason: str):
            return {"status": "rolled_back", "reason": reason}

        client.app.state.container.historical_intelligence_service.get_status = fake_status  # type: ignore[method-assign]
        client.app.state.container.historical_intelligence_service.run = fake_run  # type: ignore[method-assign]
        client.app.state.container.historical_intelligence_service.explain = fake_explain  # type: ignore[method-assign]
        client.app.state.container.historical_intelligence_service.promote = fake_promote  # type: ignore[method-assign]
        client.app.state.container.historical_intelligence_service.rollback = fake_rollback  # type: ignore[method-assign]

        status_response = client.get("/api/historical/intelligence/status")
        run_response = client.post(
            "/api/historical/intelligence/run",
            json={"date_from": "2026-04-01", "date_to": "2026-04-10", "origins": ["historical_replay"]},
        )
        explain_response = client.get("/api/historical/intelligence/explain?series=KXHIGHCHI")
        pack_status_response = client.get("/api/heuristic-pack/status")
        promote_response = client.post("/api/heuristic-pack/promote", json={"candidate_version": "heuristic-candidate-v1"})
        rollback_response = client.post("/api/heuristic-pack/rollback", json={"reason": "manual"})

    assert status_response.status_code == 200
    assert status_response.json()["active_pack_version"] == "heuristic-baseline-v1"
    assert run_response.status_code == 200
    assert run_response.json()["candidate_pack_version"] == "heuristic-candidate-v1"
    assert explain_response.status_code == 200
    assert explain_response.json()["series"] == ["KXHIGHCHI"]
    assert pack_status_response.status_code == 200
    assert pack_status_response.json()["candidate_pack_version"] == "heuristic-candidate-v1"
    assert promote_response.status_code == 200
    assert promote_response.json()["status"] == "promoted"
    assert rollback_response.status_code == 200
    assert rollback_response.json()["status"] == "rolled_back"
    get_settings.cache_clear()


def test_faq_page_and_header_link_render(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()

    with TestClient(app) as client:
        index_response = client.get("/")
        faq_response = client.get("/faq")

    assert index_response.status_code == 200
    assert 'href="/faq"' in index_response.text
    assert faq_response.status_code == 200
    assert "What is a room?" in faq_response.text
    assert "Shadow mode" in faq_response.text
    get_settings.cache_clear()
