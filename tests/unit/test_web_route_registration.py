from __future__ import annotations

from starlette.routing import Match

from kalshi_bot.web.app import create_app


def test_extracted_web_routers_preserve_registered_paths() -> None:
    app = create_app()
    paths = {route.path for route in app.routes}

    expected_paths = {
        "/api/control-room/summary",
        "/api/control-room/tab/{tab_name}",
        "/api/research/{market_ticker}",
        "/api/research/{market_ticker}/history",
        "/api/research/{market_ticker}/refresh",
        "/api/self-improve/status",
        "/api/training/status",
        "/api/training/build",
        "/api/historical/status",
        "/api/historical/pipeline/status",
        "/api/historical/intelligence/status",
        "/api/historical/intelligence/run",
        "/api/historical/intelligence/explain",
        "/api/heuristic-pack/status",
        "/api/heuristic-pack/promote",
        "/api/heuristic-pack/rollback",
        "/api/historical/import",
        "/api/historical/replay",
        "/api/training/historical/build",
        "/api/training/builds",
        "/api/research-audit",
        "/api/research/audit",
        "/api/strategy-audit/rooms/{room_id}",
        "/api/strategy-audit/summary",
        "/api/shadow-campaign/run",
        "/api/self-improve/critique",
        "/api/self-improve/eval/{candidate_version}",
        "/api/self-improve/promote",
        "/api/self-improve/rollback",
        "/api/dashboard/strategies",
        "/api/strategies/codex/runs",
        "/api/strategies/codex/runs/{run_id}",
        "/api/strategies/codex/runs/{run_id}/accept",
        "/api/strategies/auto-evolve/run",
        "/api/strategies/calibration",
        "/api/strategies/cleanup/discount-sweep",
        "/api/strategies/promotions",
        "/api/strategies/{strategy_name}/activate",
        "/api/strategies/assignments/{series_ticker}/approve",
        "/faq",
        "/",
        "/api/dashboard/{kalshi_env}",
        "/api/rooms",
        "/api/rooms/{room_id}/run",
        "/api/markets/{market_ticker}/shadow-run",
        "/api/rooms/{room_id}/snapshot",
        "/rooms/{room_id}",
        "/rooms/{room_id}/events",
        "/api/control/kill-switch/{enabled}",
        "/api/control/promote/{color}",
    }

    assert expected_paths <= paths


def test_research_audit_static_route_is_not_shadowed_by_market_ticker_route() -> None:
    scope = {"type": "http", "method": "GET", "path": "/api/research/audit"}

    for route in create_app().routes:
        match, _ = route.matches(scope)
        if match in {Match.FULL, Match.PARTIAL}:
            assert route.path == "/api/research/audit"
            return

    raise AssertionError("No route matched /api/research/audit")


def test_static_routes_are_not_shadowed_by_earlier_parameter_routes() -> None:
    routes = [route for route in create_app().routes if getattr(route, "path", "").startswith("/")]
    shadowed: list[tuple[str, str]] = []

    for index, route in enumerate(routes):
        path = route.path
        if "{" in path:
            continue
        method = next(iter(getattr(route, "methods", {"GET"})))
        scope = {"type": "http", "method": method, "path": path}
        for earlier_route in routes[:index]:
            methods = getattr(earlier_route, "methods", None)
            if methods is not None and method not in methods:
                continue
            match, _ = earlier_route.matches(scope)
            if match is Match.FULL and earlier_route.path != path:
                shadowed.append((path, earlier_route.path))
                break

    assert shadowed == []
