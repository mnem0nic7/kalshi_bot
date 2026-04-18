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


def _build_env_payload(cash_display: str, positions_count: int) -> dict[str, object]:
    return {
        "portfolio": {
            "cash_display": cash_display,
            "portfolio_display": "$9,999.00",
            "positions_value_display": "$1,250.00",
            "gain_loss_display": "+$42.00",
            "gain_loss_tone": "good",
        },
        "daily_pnl_dollars": None,
        "daily_pnl_display": "—",
        "daily_pnl_tone": "neutral",
        "alerts": [],
        "active_rooms": [],
        "positions": _build_positions(positions_count),
        "positions_summary": {
            "capital_buckets": None,
            "total_current_value_dollars": None,
            "total_current_value_display": "$0.00",
            "total_unrealized_pnl_display": "$0.00",
            "total_unrealized_pnl_tone": "neutral",
        },
    }


def _artifact_root() -> Path:
    root = os.getenv("PLAYWRIGHT_ARTIFACTS_DIR")
    if root:
        return Path(root)
    return Path(tempfile.gettempdir()) / "kalshi_bot_playwright"


@contextmanager
def _serve_dashboard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "browser.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    payloads = {
        "demo": _build_env_payload("$1,250.00", positions_count=24),
        "production": _build_env_payload("$2,500.00", positions_count=18),
    }

    async def fake_build_env_dashboard(_container, kalshi_env: str) -> dict[str, object]:
        return payloads[kalshi_env]

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
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
