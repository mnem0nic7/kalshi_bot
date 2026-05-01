from __future__ import annotations

import json
from pathlib import Path

import pytest

playwright_sync_api = pytest.importorskip("playwright.sync_api")
sync_playwright = playwright_sync_api.sync_playwright


VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 390, "height": 844},
]


def _crypto_payload() -> dict[str, object]:
    return {
        "updated_at": "2026-05-01T12:00:00Z",
        "settings": {
            "crypto_trading_enabled": False,
            "crypto_autonomy_enabled": True,
        },
        "replay_gate": {"status": "missing"},
        "asset_modes": {"BTC": "shadow", "ETH": "live"},
        "asset_mode_counts": {"off": 0, "shadow": 1, "live": 1},
        "markets": [
            {
                "market_ticker": "KXBTC15M-TEST",
                "asset_symbol": "BTC",
                "status": "open",
                "close_time": "2026-05-01T12:15:00Z",
                "target_price_dollars": "76468.89",
                "yes_ask_dollars": "0.4900",
                "yes_bid_dollars": "0.4700",
                "no_ask_dollars": "0.5300",
                "volume": 105941,
                "asset_mode": "shadow",
                "live_eligible": False,
                "live_blockers": ["Asset BTC mode is shadow; set it to live to allow live orders."],
                "signal": {"recommended_side": "yes", "edge_bps": 120},
            },
            {
                "market_ticker": "KXETH15M-TEST",
                "asset_symbol": "ETH",
                "status": "open",
                "close_time": "2026-05-01T12:15:00Z",
                "target_price_dollars": "2263.74",
                "yes_ask_dollars": "0.4400",
                "yes_bid_dollars": "0.4200",
                "no_ask_dollars": "0.5800",
                "volume": 5293,
                "asset_mode": "live",
                "live_eligible": False,
                "live_blockers": ["crypto_trading_enabled is false"],
                "signal": {"recommended_side": "no", "edge_bps": 95},
            },
        ],
    }


def _crypto_html(payload: dict[str, object]) -> str:
    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <link rel="stylesheet" href="/static/crypto.css" />
      </head>
      <body>
        <main class="crypto-main">
          <script id="crypto-bootstrap" type="application/json">{json.dumps(payload)}</script>
          <section class="crypto-toolbar" aria-label="Crypto controls">
            <div>
              <p class="crypto-eyebrow">15 Minute Crypto</p>
              <h1>Crypto Markets</h1>
            </div>
            <div class="crypto-actions">
              <select id="crypto-sort" aria-label="Sort crypto markets">
                <option value="trending">Trending</option>
                <option value="closing">Closing</option>
                <option value="volume">Volume</option>
                <option value="asset">Asset</option>
              </select>
              <button id="crypto-refresh" type="button">Refresh</button>
            </div>
          </section>
          <div id="crypto-alert" class="crypto-alert" role="alert" hidden></div>
          <section class="crypto-status-strip" id="crypto-status-strip" aria-label="Crypto status"></section>
          <section class="crypto-grid" id="crypto-grid" aria-label="15 minute crypto markets"></section>
        </main>
        <script src="/static/crypto.js"></script>
      </body>
    </html>
    """


def _install_crypto_routes(page, payload: dict[str, object]) -> None:
    crypto_js = Path("src/kalshi_bot/web/static/crypto.js").read_text(encoding="utf-8")
    crypto_css = Path("src/kalshi_bot/web/static/crypto.css").read_text(encoding="utf-8")

    def route_handler(route) -> None:
        url = route.request.url
        if url.endswith("/crypto"):
            route.fulfill(status=200, content_type="text/html", body=_crypto_html(payload))
        elif url.endswith("/static/crypto.js"):
            route.fulfill(status=200, content_type="application/javascript", body=crypto_js)
        elif url.endswith("/static/crypto.css"):
            route.fulfill(status=200, content_type="text/css", body=crypto_css)
        elif "/api/crypto/assets/BTC/mode" in url:
            route.fulfill(
                status=500,
                content_type="application/json",
                body=json.dumps({"detail": "Mode service unavailable"}),
            )
        elif "/api/crypto/markets" in url:
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        else:
            route.abort()

    page.route("**/*", route_handler)


@pytest.mark.parametrize("viewport", VIEWPORTS)
def test_crypto_dashboard_modes_render_and_failed_mode_reverts(viewport: dict[str, int]) -> None:
    payload = _crypto_payload()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport=viewport, device_scale_factor=1)
        try:
            _install_crypto_routes(page, payload)
            page.goto("http://crypto.test/crypto", wait_until="load", timeout=15_000)
            page.wait_for_selector(".crypto-card", timeout=15_000)

            assert page.locator(".crypto-card").count() == 2
            assert page.locator(".crypto-live-blockers").count() == 1
            btc_select = page.locator('[data-asset-mode="BTC"]')
            assert btc_select.input_value() == "shadow"

            btc_select.select_option("live")
            page.wait_for_selector("#crypto-alert:not([hidden])", timeout=5_000)

            assert "Mode service unavailable" in page.locator("#crypto-alert").inner_text()
            assert page.locator('[data-asset-mode="BTC"]').input_value() == "shadow"
            assert page.evaluate(
                """
                () => [...document.querySelectorAll('.crypto-card')].every((card) => {
                  const bounds = card.getBoundingClientRect();
                  return [...card.querySelectorAll('select, button, a, h2, .crypto-live-blockers')]
                    .every((element) => {
                      const rect = element.getBoundingClientRect();
                      return rect.left >= bounds.left - 1 && rect.right <= bounds.right + 1;
                    });
                })
                """
            )
        finally:
            browser.close()
