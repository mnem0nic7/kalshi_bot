from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request

import pytest
import uvicorn

from kalshi_bot.config import get_settings
import kalshi_bot.web.app as web_app_module
from kalshi_bot.web.app import create_app

playwright_sync_api = pytest.importorskip("playwright.sync_api")
Page = playwright_sync_api.Page
sync_playwright = playwright_sync_api.sync_playwright

VIEWPORTS = [
    pytest.param("desktop", {"width": 1280, "height": 1400}, id="desktop"),
    pytest.param("narrow", {"width": 760, "height": 1400}, id="narrow"),
]


def _build_positions(count: int) -> list[dict[str, object]]:
    positions: list[dict[str, object]] = []
    for idx in range(count):
        positions.append(
            {
                "market_ticker": f"KXTEST-{idx:02d}",
                "model_quality_status": "ok",
                "trade_regime": "normal",
                "warn_only_blocked": False,
                "recommended_size_cap_fp": None,
                "count_fp": "10",
                "side": "yes",
                "average_price_display": "$0.45",
                "current_price_display": "$0.48",
                "notional_display": "$4.80",
                "unrealized_pnl_tone": "good",
                "unrealized_pnl_display": "+$0.30",
                "model_quality_reasons": [],
            }
        )
    return positions


def _build_env_payload(
    cash_display: str,
    positions_count: int,
    *,
    daily_pnl_display: str = "+$42.00",
    daily_pnl_tone: str = "good",
    daily_pnl_line_display: str = "+$42.00 (0.42%) today (PT)",
    total_value_dollars: str | None = "$115.20",
    total_value_display: str = "$115.20",
    total_value_label: str = "Current",
    total_value_is_marked: bool = True,
    total_notional_display: str = "$108.00",
    total_unrealized_pnl_display: str = "+$7.20",
    total_unrealized_pnl_tone: str = "good",
    has_pnl_summary: bool = True,
) -> dict[str, object]:
    return {
        "portfolio": {
            "cash_display": cash_display,
            "portfolio_display": "$9,999.00",
            "positions_value_display": "$1,250.00",
            "gain_loss_display": "+$42.00",
            "gain_loss_tone": "good",
        },
        "daily_pnl_dollars": None,
        "daily_pnl_display": daily_pnl_display,
        "daily_pnl_tone": daily_pnl_tone,
        "daily_pnl_line_display": daily_pnl_line_display,
        "alerts": [],
        "active_rooms": [],
        "positions": _build_positions(positions_count),
        "positions_summary": {
            "capital_buckets": None,
            "total_current_value_dollars": total_value_dollars if total_value_is_marked else None,
            "total_current_value_display": total_value_display if total_value_is_marked else "$0.00",
            "total_value_dollars": total_value_dollars,
            "total_value_display": total_value_display,
            "total_value_label": total_value_label,
            "total_value_is_marked": total_value_is_marked,
            "total_notional_display": total_notional_display,
            "total_unrealized_pnl_display": total_unrealized_pnl_display,
            "total_unrealized_pnl_tone": total_unrealized_pnl_tone,
            "has_pnl_summary": has_pnl_summary,
        },
    }


def _build_strategies_payload(
    window_days: int = 180,
    *,
    selected_series_ticker: str | None = None,
    selected_strategy_name: str | None = None,
) -> dict[str, object]:
    leaderboard = [
        {
            "name": "moderate",
            "description": "Balanced filters matching current live settings.",
            "selected": selected_series_ticker is None and selected_strategy_name != "aggressive",
            "threshold_groups": [
                {"label": "Risk", "items": [{"label": "Risk Min Edge Bps", "value": "40"}]},
                {"label": "Strategy", "items": [{"label": "Strategy Quality Edge Buffer Bps", "value": "20"}]},
            ],
            "overall_win_rate": 0.73,
            "overall_win_rate_display": "73%",
            "overall_trade_rate": 0.58,
            "overall_trade_rate_display": "58%",
            "total_pnl_dollars": 18.4,
            "total_pnl_display": "+$18.40",
            "avg_edge_bps": 62.0,
            "avg_edge_bps_display": "62bps",
            "total_rooms_evaluated": 44,
            "total_rooms_evaluated_display": "44",
            "total_trade_count": 26,
            "total_trade_count_display": "26",
            "cities_led": 1,
            "assigned_city_count": 1,
        },
        {
            "name": "aggressive",
            "description": "Loose filters, higher trade frequency.",
            "selected": selected_strategy_name == "aggressive",
            "threshold_groups": [
                {"label": "Risk", "items": [{"label": "Risk Min Edge Bps", "value": "20"}]},
                {"label": "Strategy", "items": [{"label": "Strategy Quality Edge Buffer Bps", "value": "0"}]},
            ],
            "overall_win_rate": 0.60,
            "overall_win_rate_display": "60%",
            "overall_trade_rate": 0.50,
            "overall_trade_rate_display": "50%",
            "total_pnl_dollars": 5.0,
            "total_pnl_display": "+$5.00",
            "avg_edge_bps": 75.0,
            "avg_edge_bps_display": "75bps",
            "total_rooms_evaluated": 44,
            "total_rooms_evaluated_display": "44",
            "total_trade_count": 20,
            "total_trade_count_display": "20",
            "cities_led": 0,
            "assigned_city_count": 1,
        },
    ]
    city_matrix = [
        {
            "series_ticker": "KXHIGHNY",
            "city_label": "New York City",
            "location_name": "New York City",
            "selected": selected_series_ticker == "KXHIGHNY",
            "assignment": {"strategy_name": "aggressive", "assigned_at": "2026-04-21T18:00:00+00:00", "assigned_by": "auto_regression"},
            "best_strategy": "moderate",
            "best_strategy_win_rate": 0.75,
            "best_strategy_win_rate_display": "75%",
            "runner_up_strategy": "aggressive",
            "runner_up_win_rate_display": "60%",
            "gap_to_runner_up": 0.15,
            "gap_to_runner_up_display": "15%",
            "gap_to_assignment": 0.15,
            "gap_to_assignment_display": "15%",
            "evidence_status": "strong",
            "evidence_label": "Strong",
            "trade_count_sufficient": True,
            "assignment_status": "promotion_candidate",
            "assignment_status_label": "Promote",
            "can_promote": True,
            "sort_priority": 0,
            "metrics": [
                {"strategy_name": "moderate", "selected": selected_strategy_name == "moderate", "is_assigned": False, "is_best": True, "is_runner_up": False, "rooms_evaluated": 20, "trade_count": 12, "trade_rate": 0.60, "trade_rate_display": "60%", "win_rate": 0.75, "win_rate_display": "75%", "total_pnl_dollars": 8.4, "total_pnl_display": "+$8.40", "avg_edge_bps": 68.0, "avg_edge_bps_display": "68bps", "has_data": True},
                {"strategy_name": "aggressive", "selected": selected_strategy_name == "aggressive", "is_assigned": True, "is_best": False, "is_runner_up": True, "rooms_evaluated": 20, "trade_count": 10, "trade_rate": 0.50, "trade_rate_display": "50%", "win_rate": 0.60, "win_rate_display": "60%", "total_pnl_dollars": 5.0, "total_pnl_display": "+$5.00", "avg_edge_bps": 75.0, "avg_edge_bps_display": "75bps", "has_data": True},
            ],
        },
        {
            "series_ticker": "KXHIGHCHI",
            "city_label": "Chicago",
            "location_name": "Chicago",
            "selected": selected_series_ticker == "KXHIGHCHI",
            "assignment": {"strategy_name": "moderate", "assigned_at": "2026-04-21T18:00:00+00:00", "assigned_by": "auto_regression"},
            "best_strategy": "moderate",
            "best_strategy_win_rate": 0.58,
            "best_strategy_win_rate_display": "58%",
            "runner_up_strategy": "aggressive",
            "runner_up_win_rate_display": "40%",
            "gap_to_runner_up": 0.18,
            "gap_to_runner_up_display": "18%",
            "gap_to_assignment": None,
            "gap_to_assignment_display": "—",
            "evidence_status": "strong",
            "evidence_label": "Strong",
            "trade_count_sufficient": True,
            "assignment_status": "aligned",
            "assignment_status_label": "Aligned",
            "can_promote": False,
            "sort_priority": 3,
            "metrics": [
                {"strategy_name": "moderate", "selected": selected_strategy_name == "moderate", "is_assigned": True, "is_best": True, "is_runner_up": False, "rooms_evaluated": 24, "trade_count": 14, "trade_rate": 0.58, "trade_rate_display": "58%", "win_rate": 0.58, "win_rate_display": "58%", "total_pnl_dollars": 2.8, "total_pnl_display": "+$2.80", "avg_edge_bps": 52.0, "avg_edge_bps_display": "52bps", "has_data": True},
                {"strategy_name": "aggressive", "selected": selected_strategy_name == "aggressive", "is_assigned": False, "is_best": False, "is_runner_up": True, "rooms_evaluated": 24, "trade_count": 10, "trade_rate": 0.42, "trade_rate_display": "42%", "win_rate": 0.40, "win_rate_display": "40%", "total_pnl_dollars": -1.2, "total_pnl_display": "-$1.20", "avg_edge_bps": 58.0, "avg_edge_bps_display": "58bps", "has_data": True},
            ],
        },
    ]
    if selected_series_ticker == "KXHIGHNY":
        detail_context = {
            "type": "city",
            "selected_series_ticker": "KXHIGHNY",
            "selected_strategy_name": None,
            "city": city_matrix[0],
            "ranking": city_matrix[0]["metrics"],
            "promotion_rationale": {"best_strategy": "moderate", "best_trade_count_display": "12", "gap_to_runner_up_display": "15%", "gap_to_current_assignment_display": "15%", "meets_trade_threshold": True, "meets_gap_threshold": True, "clears_promotion_rule": True},
            "threshold_comparison": [
                {"strategy_name": "moderate", "role": "best", "threshold_groups": leaderboard[0]["threshold_groups"]},
                {"strategy_name": "aggressive", "role": "runner_up", "threshold_groups": leaderboard[1]["threshold_groups"]},
            ],
            "trend": {
                "title": "Stored regression history",
                "window_days": 180,
                "note": "Trend history uses stored 180d regression snapshots.",
                "series": [
                    {"strategy_name": "moderate", "points": [{"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.67}, {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.75}]},
                    {"strategy_name": "aggressive", "points": [{"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.52}, {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.60}]},
                ],
            },
            "recent_events": [{"kind": "promotion", "summary": "Strategy auto-promoted for KXHIGHNY: aggressive -> moderate", "source": "strategy_regression", "created_at": "2026-04-21T18:00:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12}],
        }
    else:
        selected = leaderboard[0] if selected_strategy_name != "aggressive" else leaderboard[1]
        detail_context = {
            "type": "strategy",
            "selected_series_ticker": None,
            "selected_strategy_name": selected["name"],
            "strategy": {**selected, "threshold_groups": selected["threshold_groups"]},
            "strongest_cities": [{"series_ticker": "KXHIGHNY", "city_label": "New York City", "win_rate_display": "75%", "trade_count_display": "12", "total_pnl_display": "+$8.40"}],
            "weakest_cities": [{"series_ticker": "KXHIGHCHI", "city_label": "Chicago", "win_rate_display": "58%", "trade_count_display": "14", "total_pnl_display": "+$2.80"}],
            "city_distribution": [
                {"series_ticker": "KXHIGHNY", "city_label": "New York City", "win_rate_display": "75%", "trade_rate_display": "60%", "trade_count_display": "12", "trade_count": 12, "total_pnl_display": "+$8.40", "total_pnl_dollars": 8.4, "is_best": True, "is_assigned": False},
                {"series_ticker": "KXHIGHCHI", "city_label": "Chicago", "win_rate_display": "58%", "trade_rate_display": "58%", "trade_count_display": "14", "trade_count": 14, "total_pnl_display": "+$2.80", "total_pnl_dollars": 2.8, "is_best": True, "is_assigned": True},
            ],
            "trend": {
                "title": "Stored regression history",
                "note": "Current leaderboard metrics reflect the selected window.",
                "points": [
                    {"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.66, "trade_rate": 0.52, "total_pnl_dollars": 12.4},
                    {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.73, "trade_rate": 0.58, "total_pnl_dollars": 18.4},
                ],
            },
            "recent_events": [{"kind": "threshold_adjustment", "summary": "Auto-adjusted risk_min_edge_bps 50->40", "source": "strategy_eval", "created_at": "2026-04-21T18:00:00+00:00", "change_display": "50bps -> 40bps", "win_rate_display": "63%", "trade_count": 82}],
        }

    return {
        "summary": {
            "window_days": window_days,
            "window_display": f"{window_days}d",
            "window_options": [30, 90, 180],
            "source_mode": "stored_snapshot" if window_days == 180 else "live_eval",
            "last_regression_run": "2026-04-21T18:00:00+00:00",
            "rooms_scanned": 84,
            "rooms_scanned_display": "84",
            "cities_evaluated": 2,
            "cities_evaluated_display": "2",
            "best_strategy_name": "moderate",
            "best_strategy_win_rate": 0.73,
            "best_strategy_win_rate_display": "73%",
            "recent_promotions_count": 1,
            "assignments_covered": 2,
            "assignments_total": 2,
            "assignments_covered_display": "2 / 2",
            "methodology_note": "Historical replay evidence only",
        },
        "leaderboard": leaderboard,
        "city_matrix": city_matrix,
        "detail_context": detail_context,
        "recent_promotions": [
            {"kind": "promotion", "summary": "Strategy auto-promoted for KXHIGHNY: aggressive -> moderate", "source": "strategy_regression", "created_at": "2026-04-21T18:00:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12},
            {"kind": "threshold_adjustment", "summary": "Auto-adjusted risk_min_edge_bps 50->40", "source": "strategy_eval", "created_at": "2026-04-21T18:00:00+00:00", "change_display": "50bps -> 40bps", "win_rate_display": "63%", "trade_count": 82},
        ],
        "methodology": {
            "title": "How to read this tab",
            "points": [
                "Data comes from historical replay rooms, not live forward testing.",
                "Default view uses a rolling 180d regression snapshot.",
            ],
            "promotion_trade_threshold": 20,
            "promotion_gap_threshold": 0.05,
        },
    }


def _artifact_root() -> Path:
    root = os.getenv("PLAYWRIGHT_ARTIFACTS_DIR")
    if root:
        return Path(root)
    return Path(tempfile.gettempdir()) / "kalshi_bot_playwright"


@contextmanager
def _serve_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payloads: dict[str, object] | None = None,
    strategies_payload_factory=None,
):
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "browser.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    if payloads is None:
        payloads = {
            "demo": _build_env_payload("$1,250.00", positions_count=24),
            "production": _build_env_payload("$2,500.00", positions_count=18),
        }

    async def fake_build_env_dashboard(_container, kalshi_env: str) -> dict[str, object]:
        return payloads[kalshi_env]

    async def fake_build_strategies_dashboard(
        _container,
        *,
        window_days: int = 180,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ) -> dict[str, object]:
        factory = strategies_payload_factory or _build_strategies_payload
        return factory(window_days, selected_series_ticker=series_ticker, selected_strategy_name=strategy_name)

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}/"
    deadline = time.time() + 10
    try:
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(base_url, timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.HTTPError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            raise RuntimeError("Timed out waiting for the dashboard test server to start")
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        get_settings.cache_clear()


def _collect_layout_metrics(page: Page, env_key: str) -> dict[str, object]:
    if env_key == "production":
        page.locator('.dash-tab[data-env="production"]').click(timeout=15_000)
        page.wait_for_function("() => !document.querySelector('#panel-production').hidden", timeout=15_000)

    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const grid = panel?.querySelector('.dash-grid');
          const card = panel?.querySelector('.dash-card-alerts');
          const header = card?.querySelector('.dash-card-header');
          const rooms = card?.querySelector('.active-rooms-section');
          const positions = panel?.querySelector('.dash-card-positions');
          if (!panel || !grid || !card || !header || !rooms || !positions) {
            return null;
          }
          const rect = (node) => {
            const box = node.getBoundingClientRect();
            return { top: box.top, bottom: box.bottom, height: box.height };
          };
          const cardRect = rect(card);
          const headerRect = rect(header);
          const roomsRect = rect(rooms);
          const positionsRect = rect(positions);
          return {
            activeTabEnv: document.querySelector('.dash-tab.is-active')?.dataset.env ?? null,
            panelHidden: panel.hidden,
            gridAlignItems: getComputedStyle(grid).alignItems,
            cardAlignContent: getComputedStyle(card).alignContent,
            cardHeight: Math.round(cardRect.height),
            positionsHeight: Math.round(positionsRect.height),
            cardToHeaderGap: Math.round(headerRect.top - cardRect.top),
            headerToRoomsGap: Math.round(roomsRect.top - headerRect.bottom),
          };
        }
        """,
        env_key,
    )


def _assert_layout(page: Page, env_key: str, viewport_name: str) -> None:
    artifact_path = _artifact_root() / f"dashboard-layout-{viewport_name}-{env_key}.png"
    metrics = _collect_layout_metrics(page, env_key)
    try:
        assert metrics is not None, f"Missing layout metrics for {env_key}"
        assert metrics["activeTabEnv"] == env_key, metrics
        assert metrics["panelHidden"] is False, metrics
        assert metrics["gridAlignItems"] == "start", metrics
        assert metrics["cardAlignContent"] == "start", metrics
        assert metrics["cardHeight"] < metrics["positionsHeight"], metrics
        assert metrics["cardToHeaderGap"] <= 24, metrics
        assert metrics["headerToRoomsGap"] <= 24, metrics
    except AssertionError as exc:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(artifact_path), full_page=True)
        raise AssertionError(f"{exc}\nScreenshot: {artifact_path}") from exc


@pytest.mark.parametrize(("viewport_name", "viewport"), VIEWPORTS)
def test_dashboard_alerts_card_stays_top_aligned(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, viewport_name: str, viewport: dict[str, int]
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as playwright_context:
            browser = playwright_context.chromium.launch(headless=True)
            page = browser.new_page(viewport=viewport, device_scale_factor=1)
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .dash-card-alerts", timeout=15_000)
                _assert_layout(page, "demo", viewport_name)
                _assert_layout(page, "production", viewport_name)
            finally:
                browser.close()


def _positions_total_row(page: Page, env_key: str = "demo") -> dict[str, object] | None:
    """Return label and value text from the positions tfoot row, or None if absent."""
    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const tfoot = panel?.querySelector('.positions-totals');
          if (!tfoot) return null;
          const cells = tfoot.querySelectorAll('td');
          return {
            label: cells[0]?.textContent?.trim() ?? null,
            value: cells[1]?.textContent?.trim() ?? null,
            pnl:   cells[2]?.textContent?.trim() ?? null,
          };
        }
        """,
        env_key,
    )


def _portfolio_recent_line(page: Page, env_key: str = "demo") -> str | None:
    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const line = panel?.querySelector('.dash-stat-portfolio .dash-stat-detail');
          return line?.textContent?.trim() ?? null;
        }
        """,
        env_key,
    )


def test_positions_total_row_shows_notional_and_pnl_when_marked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            total_notional_display="$108.00",
            total_unrealized_pnl_display="+$7.20",
            total_unrealized_pnl_tone="good",
            has_pnl_summary=True,
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .positions-table", timeout=15_000)
                row = _positions_total_row(page)
                assert row is not None, "positions tfoot not rendered"
                assert row["label"] == "Totals", row
                assert row["value"] == "$108.00", row
                assert row["pnl"] == "+$7.20", row
            finally:
                browser.close()


def test_portfolio_card_shows_recent_pnl_line(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            daily_pnl_display="+$65.30",
            daily_pnl_tone="good",
            daily_pnl_line_display="+$65.30 (9.96%) today (PT)",
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .dash-stat-portfolio .dash-stat-detail", timeout=15_000)
                assert _portfolio_recent_line(page) == "+$65.30 (9.96%) today (PT)"
            finally:
                browser.close()


def test_positions_total_row_shows_dash_pnl_when_unmarked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            total_notional_display="$108.00",
            total_unrealized_pnl_display="—",
            total_unrealized_pnl_tone="neutral",
            has_pnl_summary=False,
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .positions-table", timeout=15_000)
                row = _positions_total_row(page)
                assert row is not None, "positions tfoot not rendered"
                assert row["label"] == "Totals", row
                assert row["value"] == "$108.00", row
                assert row["pnl"] == "—", row
            finally:
                browser.close()


@pytest.mark.parametrize(("viewport_name", "viewport"), VIEWPORTS)
def test_strategies_tab_renders_filters_and_drilldowns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, viewport_name: str, viewport: dict[str, int]
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport=viewport, device_scale_factor=1)
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.locator('.dash-tab[data-env="strategies"]').click(timeout=15_000)
                page.wait_for_selector("#strategies-summary", timeout=15_000)
                page.wait_for_selector("#strategies-leaderboard .strategy-card", timeout=15_000)
                assert "180d" in (page.locator("#strategies-summary").text_content(timeout=15_000) or "")
                assert "moderate" in (page.locator("#strategies-leaderboard").text_content(timeout=15_000) or "")

                page.locator('#strategies-window-filter button[data-window-days="90"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-summary')?.textContent?.includes('90d')",
                    timeout=15_000,
                )

                page.locator('#strategies-city-matrix button[data-series-ticker="KXHIGHNY"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-detail h3')?.textContent?.includes('KXHIGHNY')",
                    timeout=15_000,
                )
                detail_text = page.locator("#strategies-detail").text_content(timeout=15_000) or ""
                assert "Promotion Rationale" in detail_text
                assert "Stored regression history" in detail_text
            finally:
                browser.close()
