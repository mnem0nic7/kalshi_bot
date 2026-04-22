from __future__ import annotations

import asyncio
from threading import Event

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
from kalshi_bot.core.schemas import ResearchAuditIssue
from kalshi_bot.db.repositories import PlatformRepository
import kalshi_bot.web.app as web_app_module
from kalshi_bot.web.app import create_app


def _strategy_assignment_payload(
    *,
    series_ticker: str = "KXHIGHNY",
    strategy_name: str = "moderate",
    recommendation_status: str = "strong_recommendation",
    recommendation_label: str = "Strong recommendation",
    approval_eligible: bool = True,
    assignment_strategy: str | None = None,
) -> dict[str, object]:
    city_row = {
        "series_ticker": series_ticker,
        "city_label": "New York City",
        "location_name": "New York City",
        "selected": True,
        "assignment": {
            "strategy_name": assignment_strategy,
            "assigned_at": "2026-04-21T20:00:00+00:00" if assignment_strategy else None,
            "assigned_by": "manual_seed" if assignment_strategy else None,
        },
        "assignment_context_status": (
            "matches_recommendation"
            if assignment_strategy == strategy_name and assignment_strategy is not None
            else "differs_from_recommendation"
            if assignment_strategy is not None
            else "unassigned"
        ),
        "best_strategy": strategy_name,
        "best_strategy_win_rate": 0.75,
        "best_strategy_win_rate_display": "75%",
        "best_outcome_coverage_display": "24/24 scored",
        "runner_up_strategy": "aggressive" if strategy_name != "aggressive" else "moderate",
        "runner_up_win_rate_display": "60%",
        "gap_to_runner_up": 0.15,
        "gap_to_runner_up_display": "15%",
        "gap_to_assignment": 0.15 if assignment_strategy and assignment_strategy != strategy_name else None,
        "gap_to_assignment_display": "15%" if assignment_strategy and assignment_strategy != strategy_name else "—",
        "evidence_status": "strong" if recommendation_status == "strong_recommendation" else "low_sample",
        "evidence_label": recommendation_label,
        "trade_count_sufficient": True,
        "resolved_trade_count_sufficient": True,
        "outcome_coverage_sufficient": True,
        "gap_threshold_sufficient": recommendation_status == "strong_recommendation",
        "lean_gap_sufficient": recommendation_status in {"strong_recommendation", "lean_recommendation"},
        "assignment_gap_sufficient": True,
        "assignment_status": recommendation_status,
        "assignment_status_label": recommendation_label,
        "recommendation": {
            "strategy_name": strategy_name,
            "status": recommendation_status,
            "label": recommendation_label,
            "resolved_trade_count": 24 if recommendation_status != "no_outcomes" else 0,
            "resolved_trade_count_display": "24" if recommendation_status != "no_outcomes" else "0",
            "outcome_coverage_rate": 1.0 if recommendation_status != "no_outcomes" else 0.0,
            "outcome_coverage_rate_display": "100%" if recommendation_status != "no_outcomes" else "0%",
            "gap_to_runner_up": 0.15 if recommendation_status != "too_close" else 0.005,
            "gap_to_runner_up_display": "15%" if recommendation_status != "too_close" else "0.5%",
            "writes_assignment": False,
        },
        "can_promote": False,
        "approval_eligible": approval_eligible,
        "approval_label": "Ready to approve" if approval_eligible else recommendation_label,
        "approval_window_days": 180,
        "approval_requires_note": True,
        "approval_reason": "Manual approval validates against the latest stored 180d snapshot.",
        "metrics": [
            {
                "strategy_name": strategy_name,
                "selected": False,
                "is_assigned": assignment_strategy == strategy_name,
                "is_best": True,
                "is_runner_up": False,
                "rooms_evaluated": 40,
                "trade_count": 24,
                "resolved_trade_count": 24,
                "resolved_trade_count_display": "24",
                "unscored_trade_count": 0,
                "unscored_trade_count_display": "0",
                "outcome_coverage_rate": 1.0,
                "outcome_coverage_rate_display": "100%",
                "outcome_coverage_display": "24/24 scored",
                "trade_rate": 0.6,
                "trade_rate_display": "60%",
                "win_rate": 0.75,
                "win_rate_display": "75%",
                "win_rate_interval_lower": 0.55,
                "win_rate_interval_upper": 0.88,
                "win_rate_interval_display": "55%-88%",
                "total_pnl_dollars": 8.4,
                "total_pnl_display": "+$8.40",
                "avg_edge_bps": 68.0,
                "avg_edge_bps_display": "68bps",
                "has_data": True,
            }
        ],
        "sort_priority": 0,
    }
    detail_context = {
        "type": "city",
        "selected_series_ticker": series_ticker,
        "selected_strategy_name": None,
        "city": city_row,
        "ranking": city_row["metrics"],
        "recommendation_rationale": {
            "best_strategy": strategy_name,
            "best_trade_count_display": "24",
            "best_resolved_trade_count_display": "24",
            "best_unscored_trade_count_display": "0",
            "best_outcome_coverage_display": "24/24 scored",
            "gap_to_runner_up_display": city_row["gap_to_runner_up_display"],
            "gap_to_current_assignment_display": city_row["gap_to_assignment_display"],
            "winner_wilson_display": "55%-88%",
            "runner_up_wilson_display": "41%-77%",
            "recommendation_status": recommendation_status,
            "recommendation_label": recommendation_label,
            "writes_assignment": False,
            "meets_trade_threshold": True,
            "meets_coverage_threshold": True,
            "meets_gap_threshold": recommendation_status == "strong_recommendation",
            "meets_lean_gap_threshold": recommendation_status in {"strong_recommendation", "lean_recommendation"},
        },
        "promotion_rationale": {},
        "approval": {
            "eligible": approval_eligible,
            "label": "Ready to approve" if approval_eligible else recommendation_label,
            "window_days": 180,
            "requires_note": True,
            "reason": "Manual approval validates against the latest stored 180d snapshot.",
            "strategy_name": strategy_name,
            "recommendation_status": recommendation_status,
            "recommendation_label": recommendation_label,
            "assignment_context_status": city_row["assignment_context_status"],
        },
        "threshold_comparison": [],
        "trend": {"title": "Stored regression history", "window_days": 180, "series": [], "note": "Stored regression snapshots."},
        "recent_events": [],
    }
    return {
        "summary": {
            "window_days": 180,
            "window_display": "180d",
            "window_options": [30, 90, 180],
            "source_mode": "stored_snapshot",
            "recommendation_mode": "recommendation_only",
            "manual_approval_enabled": True,
            "approval_window_days": 180,
            "last_regression_run": "2026-04-21T20:35:59+00:00",
            "rooms_scanned": 84,
            "rooms_scanned_display": "84",
            "cities_evaluated": 1,
            "cities_evaluated_display": "1",
            "best_strategy_name": strategy_name,
            "best_strategy_win_rate": 0.75,
            "best_strategy_win_rate_display": "75%",
            "strong_recommendations_count": 1 if recommendation_status == "strong_recommendation" else 0,
            "lean_recommendations_count": 1 if recommendation_status == "lean_recommendation" else 0,
            "recent_promotions_count": 0,
            "recent_approvals_count": 0,
            "assignments_covered": 0 if assignment_strategy is None else 1,
            "assignments_total": 1,
            "assignments_covered_display": "0 / 1" if assignment_strategy is None else "1 / 1",
            "methodology_note": "Canonical outcomes, manual approval",
        },
        "leaderboard": [],
        "city_matrix": [city_row],
        "detail_context": detail_context,
        "codex_lab": {
            "available": False,
            "provider": "unavailable",
            "model": None,
            "recent_runs": [],
            "inactive_codex_strategies": [],
            "creation_window_days": 180,
        },
        "recent_promotions": [],
        "methodology": {"points": []},
    }


def _list_city_strategy_assignments(app) -> list[dict[str, object]]:
    async def _load() -> list[dict[str, object]]:
        async with app.state.container.session_factory() as session:
            repo = PlatformRepository(session)
            rows = await repo.list_city_strategy_assignments()
            await session.commit()
        return [
            {
                "series_ticker": row.series_ticker,
                "strategy_name": row.strategy_name,
                "assigned_by": row.assigned_by,
                "assigned_at": row.assigned_at.isoformat(),
            }
            for row in rows
        ]

    return asyncio.run(_load())


def _list_strategy_review_events(app) -> list[dict[str, object]]:
    async def _load() -> list[dict[str, object]]:
        async with app.state.container.session_factory() as session:
            repo = PlatformRepository(session)
            rows = await repo.list_ops_events(limit=20, sources=["strategy_review"])
            await session.commit()
        return [
            {
                "summary": row.summary,
                "source": row.source,
                "payload": row.payload or {},
            }
            for row in rows
        ]

    return asyncio.run(_load())


def _seed_city_strategy_assignment(app, *, series_ticker: str, strategy_name: str, assigned_by: str = "manual_seed") -> None:
    async def _seed() -> None:
        async with app.state.container.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_city_strategy_assignment(series_ticker, strategy_name, assigned_by=assigned_by)
            await session.commit()

    asyncio.run(_seed())


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

    async def fake_build_env_dashboard(_container, kalshi_env: str):
        return {
            "portfolio": {"env_label": kalshi_env.title()},
            "daily_pnl_display": "—",
            "daily_pnl_line_display": "—",
            "daily_pnl_tone": "neutral",
            "active_rooms": [],
            "alerts": [],
            "positions": [],
            "positions_summary": {},
        }

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(series_ticker="KXHIGHNY")

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
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


def test_control_room_page_and_tab_endpoints_render_payloads(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_bootstrap(_container):
        return {
            "initial_tab": "overview",
            "tabs": [
                {"id": "overview", "label": "Overview"},
                {"id": "training", "label": "Training & Historical"},
                {"id": "research", "label": "Research"},
                {"id": "rooms", "label": "Rooms"},
                {"id": "operations", "label": "Operations"},
            ],
            "summary": {
                "as_of": "2026-04-13T16:00:00+00:00",
                "system_status": {
                    "level": "critical",
                    "label": "Kill Switch On",
                    "detail": "Trading is disabled.",
                    "active_color": "green",
                },
                "active_deployment": {
                    "active_color": "green",
                    "watchdog_updated_at": "2026-04-13T15:59:45+00:00",
                    "last_action": {"action": "heartbeat"},
                },
                "open_positions": {"count": 0, "total_contracts": "0.00"},
                "research_confidence": {"average": 0.91, "count": 3, "sparkline": [0.82, 0.91, 0.96]},
                "room_outcomes": {"succeeded": 0, "total": 6, "window_hours": 24, "blocked": 3, "stand_down": 2, "failed": 1},
                "quality_debt": {
                    "total": 12,
                    "stale_mismatch_count": 2,
                    "missed_stand_down_count": 1,
                    "weak_resolved_trade_count": 9,
                    "recent_stale_mismatch_count": 1,
                    "recent_missed_stand_down_count": 0,
                },
            },
            "initial_tab_payload": {
                "runtime_health": {
                    "colors": {
                        "blue": {
                            "combined_healthy": True,
                            "app": {"status": "healthy"},
                            "daemon": {"healthy": True, "heartbeat_age_seconds": 19},
                        }
                    }
                },
                "top_blockers": ["not enough settled rooms"],
                "next_actions": ["backfill weather archives"],
                "ops_events": [{"severity": "info", "summary": "Daemon heartbeat", "source": "daemon"}],
                "self_improve": {"agent_packs": {"champion_version": "champion-v1", "candidate_version": None, "blue_version": "blue-v1", "green_version": "green-v1"}},
            },
        }

    async def fake_summary(_container):
        return {"as_of": "2026-04-13T16:00:00+00:00", "system_status": {"level": "healthy", "label": "Healthy", "detail": "ok", "active_color": "green"}}

    async def fake_tab(_container, tab_name: str):
        return {"tab": tab_name, "rooms": [], "markets": [], "ops_events": []}

    async def fake_build_env_dashboard(_container, kalshi_env: str):
        return {
            "portfolio": {"env_label": kalshi_env.title()},
            "daily_pnl_display": "—",
            "daily_pnl_line_display": "—",
            "daily_pnl_tone": "neutral",
            "active_rooms": [],
            "alerts": [],
            "positions": [],
            "positions_summary": {},
        }

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(series_ticker="KXHIGHNY")

    monkeypatch.setattr(web_app_module, "build_control_room_bootstrap", fake_bootstrap)
    monkeypatch.setattr(web_app_module, "build_control_room_summary", fake_summary)
    monkeypatch.setattr(web_app_module, "build_control_room_tab", fake_tab)
    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)

    app = create_app()

    with TestClient(app) as client:
        index_response = client.get("/")
        summary_response = client.get("/api/control-room/summary")
        tab_response = client.get("/api/control-room/tab/rooms")
        missing_tab_response = client.get("/api/control-room/tab/not-a-tab")

    assert index_response.status_code == 200
    assert "Demo" in index_response.text
    assert "Production" in index_response.text
    assert "Strategies" in index_response.text
    assert "/static/dashboard.js" in index_response.text
    assert summary_response.status_code == 200
    assert summary_response.json()["system_status"]["label"] == "Healthy"
    assert tab_response.status_code == 200
    assert tab_response.json()["tab"] == "rooms"
    assert missing_tab_response.status_code == 404
    get_settings.cache_clear()


def test_dashboard_strategies_endpoint_accepts_window_and_selection_params(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    captured: dict[str, object] = {}

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        captured["window_days"] = window_days
        captured["series_ticker"] = series_ticker
        captured["strategy_name"] = strategy_name
        return {
            "summary": {"window_days": window_days, "window_options": [30, 90, 180]},
            "leaderboard": [],
            "city_matrix": [],
            "detail_context": {"selected_series_ticker": series_ticker, "selected_strategy_name": strategy_name, "type": "empty"},
            "recent_promotions": [],
            "methodology": {"points": []},
        }

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        response = client.get("/api/dashboard/strategies?window_days=90&series_ticker=KXHIGHNY&strategy_name=moderate")

    assert response.status_code == 200
    assert captured == {"window_days": 90, "series_ticker": "KXHIGHNY", "strategy_name": "moderate"}
    assert response.json()["summary"]["window_days"] == 90
    get_settings.cache_clear()


def test_dashboard_strategies_endpoint_rejects_invalid_window(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()

    with TestClient(app) as client:
        response = client.get("/api/dashboard/strategies?window_days=15")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid window_days"}
    get_settings.cache_clear()


def test_approve_strategy_assignment_creates_assignment_and_audit_event(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        assert window_days == 180
        assert series_ticker == "KXHIGHNY"
        return _strategy_assignment_payload(series_ticker="KXHIGHNY")

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/strategies/assignments/KXHIGHNY/approve",
            json={
                "expected_strategy_name": "moderate",
                "expected_recommendation_status": "strong_recommendation",
                "note": "Approving the current winner after reviewing the latest 180d evidence.",
            },
        )

    assert response.status_code == 200
    assert response.json()["status"] == "approved"
    assignments = _list_city_strategy_assignments(app)
    assert assignments == [
        {
            "series_ticker": "KXHIGHNY",
            "strategy_name": "moderate",
            "assigned_by": "strategies_approval",
            "assigned_at": assignments[0]["assigned_at"],
        }
    ]
    review_events = _list_strategy_review_events(app)
    assert len(review_events) == 1
    assert review_events[0]["payload"]["event_kind"] == "assignment_approval"
    assert review_events[0]["payload"]["new_strategy"] == "moderate"
    assert review_events[0]["payload"]["note"].startswith("Approving the current winner")
    get_settings.cache_clear()


def test_approve_strategy_assignment_replaces_existing_assignment(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(series_ticker="KXHIGHNY", assignment_strategy="aggressive")

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        _seed_city_strategy_assignment(client.app, series_ticker="KXHIGHNY", strategy_name="aggressive")
        response = client.post(
            "/api/strategies/assignments/KXHIGHNY/approve",
            json={
                "expected_strategy_name": "moderate",
                "expected_recommendation_status": "strong_recommendation",
                "note": "Replacing the older assignment with the current approved winner.",
            },
        )

    assert response.status_code == 200
    assignments = _list_city_strategy_assignments(app)
    assert assignments[0]["strategy_name"] == "moderate"
    assert assignments[0]["assigned_by"] == "strategies_approval"
    review_events = _list_strategy_review_events(app)
    assert review_events[0]["payload"]["previous_strategy"] == "aggressive"
    assert review_events[0]["payload"]["new_strategy"] == "moderate"
    get_settings.cache_clear()


def test_approve_strategy_assignment_rejects_stale_recommendation(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(series_ticker="KXHIGHNY", strategy_name="moderate")

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/strategies/assignments/KXHIGHNY/approve",
            json={
                "expected_strategy_name": "aggressive",
                "expected_recommendation_status": "strong_recommendation",
                "note": "This should fail because the strategy changed.",
            },
        )

    assert response.status_code == 409
    assert response.json()["error"] == "stale_recommendation"
    assert _list_city_strategy_assignments(app) == []
    assert _list_strategy_review_events(app) == []
    get_settings.cache_clear()


def test_approve_strategy_assignment_rejects_ineligible_status(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(
            series_ticker="KXHIGHNY",
            recommendation_status="low_sample",
            recommendation_label="Low sample",
            approval_eligible=False,
        )

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/strategies/assignments/KXHIGHNY/approve",
            json={
                "expected_strategy_name": "moderate",
                "expected_recommendation_status": "low_sample",
                "note": "This should not be approvable.",
            },
        )

    assert response.status_code == 409
    assert response.json()["error"] == "approval_not_eligible"
    assert _list_city_strategy_assignments(app) == []
    assert _list_strategy_review_events(app) == []
    get_settings.cache_clear()


def test_approve_strategy_assignment_requires_note(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        return _strategy_assignment_payload(series_ticker="KXHIGHNY")

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/strategies/assignments/KXHIGHNY/approve",
            json={
                "expected_strategy_name": "moderate",
                "expected_recommendation_status": "strong_recommendation",
                "note": "   ",
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"][0]["msg"] == "Value error, Note is required"
    get_settings.cache_clear()


def test_create_strategy_codex_run_endpoint_schedules_background_execution(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    captured: dict[str, object] = {}

    async def fake_build_strategies_dashboard(
        _container,
        *,
        window_days: int = 180,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ):
        captured["window_days"] = window_days
        captured["series_ticker"] = series_ticker
        captured["strategy_name"] = strategy_name
        return _strategy_assignment_payload(series_ticker=series_ticker or "KXHIGHNY")

    class FakeStrategyCodexService:
        async def close(self) -> None:
            return None

        def is_available(self) -> bool:
            return True

        async def create_run(self, *, request, dashboard_snapshot, trigger_source="manual"):
            captured["request"] = request
            captured["dashboard_snapshot"] = dashboard_snapshot
            captured["trigger_source"] = trigger_source
            return {"run_id": "run-123", "status": "queued"}

        async def execute_run(self, run_id: str) -> None:
            captured["executed_run_id"] = run_id

    real_create_task = asyncio.create_task

    def fake_create_task(coro):
        captured["scheduled_coro_name"] = coro.cr_code.co_name
        return real_create_task(coro)

    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    monkeypatch.setattr(web_app_module.asyncio, "create_task", fake_create_task)
    app = create_app()

    with TestClient(app) as client:
        client.app.state.container.strategy_codex_service = FakeStrategyCodexService()  # type: ignore[assignment]
        response = client.post(
            "/api/strategies/codex/runs",
            json={
                "mode": "evaluate",
                "window_days": 180,
                "series_ticker": "KXHIGHNY",
                "strategy_name": "moderate",
                "operator_brief": "Focus on mismatches and weak coverage.",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"run_id": "run-123", "status": "queued"}
    assert captured["window_days"] == 180
    assert captured["series_ticker"] == "KXHIGHNY"
    assert captured["strategy_name"] == "moderate"
    assert captured["trigger_source"] == "manual"
    assert captured["scheduled_coro_name"] == "execute_run"
    request_payload = captured["request"]
    assert request_payload.mode == "evaluate"
    assert request_payload.window_days == 180
    assert request_payload.series_ticker == "KXHIGHNY"
    assert request_payload.strategy_name == "moderate"
    assert request_payload.operator_brief == "Focus on mismatches and weak coverage."
    assert captured["dashboard_snapshot"]["summary"]["window_days"] == 180
    get_settings.cache_clear()


def test_create_strategy_codex_run_endpoint_handles_unavailable_and_invalid_window(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    class FakeStrategyCodexService:
        async def close(self) -> None:
            return None

        def is_available(self) -> bool:
            return False

    app = create_app()

    with TestClient(app) as client:
        client.app.state.container.strategy_codex_service = FakeStrategyCodexService()  # type: ignore[assignment]
        unavailable_response = client.post("/api/strategies/codex/runs", json={"mode": "evaluate", "window_days": 180})
        invalid_window_response = client.post("/api/strategies/codex/runs", json={"mode": "evaluate", "window_days": 15})

    assert unavailable_response.status_code == 503
    assert unavailable_response.json() == {"error": "codex_unavailable"}
    assert invalid_window_response.status_code == 400
    assert invalid_window_response.json() == {"error": "invalid_window_days"}
    get_settings.cache_clear()


def test_get_strategy_codex_run_endpoint_returns_payload_or_404(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    class FakeStrategyCodexService:
        async def close(self) -> None:
            return None

        async def get_run_view(self, run_id: str):
            if run_id == "run-123":
                return {"id": run_id, "status": "completed", "mode": "evaluate"}
            return None

    app = create_app()

    with TestClient(app) as client:
        client.app.state.container.strategy_codex_service = FakeStrategyCodexService()  # type: ignore[assignment]
        found_response = client.get("/api/strategies/codex/runs/run-123")
        missing_response = client.get("/api/strategies/codex/runs/missing-run")

    assert found_response.status_code == 200
    assert found_response.json()["id"] == "run-123"
    assert missing_response.status_code == 404
    assert missing_response.json() == {"error": "unknown_run_id", "run_id": "missing-run"}
    get_settings.cache_clear()


def test_accept_strategy_codex_run_endpoint_maps_service_errors(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    class FakeStrategyCodexService:
        async def close(self) -> None:
            return None

        async def accept_run(self, run_id: str):
            if run_id == "missing":
                raise KeyError(run_id)
            if run_id == "invalid":
                raise ValueError("Only completed suggestion runs can be accepted")
            return {"status": "accepted", "strategy_name": "balanced-plus", "is_active": False}

    app = create_app()

    with TestClient(app) as client:
        client.app.state.container.strategy_codex_service = FakeStrategyCodexService()  # type: ignore[assignment]
        success_response = client.post("/api/strategies/codex/runs/run-123/accept")
        invalid_response = client.post("/api/strategies/codex/runs/invalid/accept")
        missing_response = client.post("/api/strategies/codex/runs/missing/accept")

    assert success_response.status_code == 200
    assert success_response.json()["strategy_name"] == "balanced-plus"
    assert invalid_response.status_code == 400
    assert invalid_response.json() == {
        "error": "invalid_run_state",
        "message": "Only completed suggestion runs can be accepted",
    }
    assert missing_response.status_code == 404
    assert missing_response.json() == {"error": "unknown_run_id", "run_id": "missing"}
    get_settings.cache_clear()


def test_activate_strategy_endpoint_maps_service_errors(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    class FakeStrategyCodexService:
        async def close(self) -> None:
            return None

        async def activate_strategy(self, strategy_name: str):
            if strategy_name == "missing":
                raise KeyError(strategy_name)
            return {"status": "activated", "strategy_name": strategy_name, "is_active": True}

    app = create_app()

    with TestClient(app) as client:
        client.app.state.container.strategy_codex_service = FakeStrategyCodexService()  # type: ignore[assignment]
        success_response = client.post("/api/strategies/balanced-plus/activate")
        missing_response = client.post("/api/strategies/missing/activate")

    assert success_response.status_code == 200
    assert success_response.json() == {
        "status": "activated",
        "strategy_name": "balanced-plus",
        "is_active": True,
    }
    assert missing_response.status_code == 404
    assert missing_response.json() == {"error": "unknown_strategy", "strategy_name": "missing"}
    get_settings.cache_clear()
