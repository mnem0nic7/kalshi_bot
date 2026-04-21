from __future__ import annotations

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
import kalshi_bot.web.app as web_app_module
from kalshi_bot.web.app import create_app


def _create_site_mode_app(tmp_path, monkeypatch, *, site_kind: str):
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "site-mode.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    monkeypatch.setenv("WEB_SITE_KIND", site_kind)
    get_settings.cache_clear()
    return create_app()


def _env_dashboard_payload(kalshi_env: str) -> dict[str, object]:
    return {
        "portfolio": {
            "portfolio_display": f"{kalshi_env.title()} Portfolio",
            "cash_display": "$0.00",
            "positions_value_display": "$0.00",
            "gain_loss_display": "—",
            "gain_loss_tone": "neutral",
        },
        "daily_pnl_display": "—",
        "daily_pnl_line_display": "—",
        "daily_pnl_tone": "neutral",
        "win_rate_display": "—",
        "win_rate_contracts": "0 contracts",
        "active_rooms": [],
        "alerts": [],
        "positions": [],
        "positions_summary": {},
    }


def test_demo_site_mode_renders_only_demo_panel(tmp_path, monkeypatch) -> None:
    env_calls: list[str] = []

    async def fake_build_env_dashboard(_container, kalshi_env: str):
        env_calls.append(kalshi_env)
        return _env_dashboard_payload(kalshi_env)

    async def fail_build_strategies_dashboard(*_args, **_kwargs):
        raise AssertionError("Strategies dashboard should not be built for demo-only site mode")

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fail_build_strategies_dashboard)
    app = _create_site_mode_app(tmp_path, monkeypatch, site_kind="demo")

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-dashboard-mode="single_site"' in response.text
    assert 'data-active-env="demo"' in response.text
    assert 'id="panel-demo"' in response.text
    assert 'id="panel-production"' not in response.text
    assert 'id="panel-strategies"' not in response.text
    assert 'class="dash-shell-site-label">Demo<' in response.text
    assert "<title>Kalshi Bot Demo</title>" in response.text
    assert 'href="https://prod.ai-al.site"' not in response.text
    assert 'href="https://strategy.ai-al.site"' not in response.text
    assert 'data-tab-mode="local"' not in response.text
    assert env_calls == ["demo"]
    get_settings.cache_clear()


def test_production_site_mode_renders_only_production_panel(tmp_path, monkeypatch) -> None:
    env_calls: list[str] = []

    async def fake_build_env_dashboard(_container, kalshi_env: str):
        env_calls.append(kalshi_env)
        return _env_dashboard_payload(kalshi_env)

    async def fail_build_strategies_dashboard(*_args, **_kwargs):
        raise AssertionError("Strategies dashboard should not be built for production-only site mode")

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fail_build_strategies_dashboard)
    app = _create_site_mode_app(tmp_path, monkeypatch, site_kind="production")

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-dashboard-mode="single_site"' in response.text
    assert 'data-active-env="production"' in response.text
    assert 'id="panel-production"' in response.text
    assert 'id="panel-demo"' not in response.text
    assert 'id="panel-strategies"' not in response.text
    assert 'class="dash-shell-site-label">Production<' in response.text
    assert "<title>Kalshi Bot Production</title>" in response.text
    assert 'href="https://demo.ai-al.site"' not in response.text
    assert 'href="https://strategy.ai-al.site"' not in response.text
    assert 'data-tab-mode="local"' not in response.text
    assert env_calls == ["production"]
    get_settings.cache_clear()


def test_strategies_site_mode_renders_only_strategies_panel(tmp_path, monkeypatch) -> None:
    async def fail_build_env_dashboard(*_args, **_kwargs):
        raise AssertionError("Environment dashboards should not be built for strategies-only site mode")

    async def fake_build_strategies_dashboard(_container, *, window_days: int = 180, series_ticker: str | None = None, strategy_name: str | None = None):
        assert window_days == 180
        assert series_ticker is None
        assert strategy_name is None
        return {
            "summary": {"window_days": 180, "window_options": [30, 90, 180]},
            "leaderboard": [],
            "city_matrix": [],
            "detail_context": {"type": "empty", "message": "No strategy data"},
            "recent_promotions": [],
            "methodology": {"points": []},
        }

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fail_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = _create_site_mode_app(tmp_path, monkeypatch, site_kind="strategies")

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-dashboard-mode="single_site"' in response.text
    assert 'data-active-env="strategies"' in response.text
    assert 'id="panel-strategies"' in response.text
    assert 'id="panel-demo"' not in response.text
    assert 'id="panel-production"' not in response.text
    assert 'class="dash-shell-site-label">Strategies<' in response.text
    assert "<title>Kalshi Bot Strategies</title>" in response.text
    assert 'href="https://demo.ai-al.site"' not in response.text
    assert 'href="https://prod.ai-al.site"' not in response.text
    assert 'data-tab-mode="local"' not in response.text
    get_settings.cache_clear()


def test_combined_mode_keeps_local_dashboard_tabs(tmp_path, monkeypatch) -> None:
    env_calls: list[str] = []
    strategies_calls = 0

    async def fake_build_env_dashboard(_container, kalshi_env: str):
        env_calls.append(kalshi_env)
        return _env_dashboard_payload(kalshi_env)

    async def fake_build_strategies_dashboard(
        _container,
        *,
        window_days: int = 180,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ):
        nonlocal strategies_calls
        strategies_calls += 1
        return {
            "summary": {"window_days": window_days, "window_options": [30, 90, 180]},
            "leaderboard": [],
            "city_matrix": [],
            "detail_context": {"type": "empty", "message": "No strategy data"},
            "recent_promotions": [],
            "methodology": {"points": []},
        }

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = _create_site_mode_app(tmp_path, monkeypatch, site_kind="combined")

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-dashboard-mode="combined"' in response.text
    assert 'data-tab-mode="local"' in response.text
    assert 'id="panel-demo"' in response.text
    assert 'id="panel-production"' in response.text
    assert 'id="panel-strategies"' in response.text
    assert 'dash-shell-site-label' not in response.text
    assert "<title>Kalshi Bot Control Room</title>" in response.text
    assert env_calls == ["demo", "production"]
    assert strategies_calls == 1
    get_settings.cache_clear()
