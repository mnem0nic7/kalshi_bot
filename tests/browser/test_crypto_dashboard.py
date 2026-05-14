from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

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
        "replay_gate": {"status": "passed", "scope": "live_assets"},
        "asset_modes": {"BTC": "shadow", "ETH": "live"},
        "asset_mode_counts": {"off": 0, "shadow": 1, "live": 1},
        "markets": [
            {
                "market_ticker": "KXBTC15M-TEST",
                "asset_symbol": "BTC",
                "status": "open",
                "close_time": "2026-05-01T12:15:00Z",
                "target_price_dollars": "76468.89",
                "yes_ask_dollars": "0.5700",
                "yes_bid_dollars": "0.5600",
                "no_ask_dollars": "0.4400",
                "volume": 105941,
                "asset_mode": "shadow",
                "replay_gate": {"status": "passed"},
                "live_eligible": False,
                "live_blockers": ["Asset BTC mode is shadow; set it to live to allow live orders."],
                "signal": {
                    "recommended_side": "no",
                    "edge_bps": 1736,
                    "candidate_trace": {
                        "selected_side": "no",
                        "min_edge_bps": 500,
                        "candidates": [
                            {
                                "side": "no",
                                "candidate_status": "blocked_fee_edge",
                                "reason": "contract_price_below_crypto_min",
                                "live_eligible": False,
                                "rank": 1,
                                "edge_bps": 1736,
                                "expected_net_edge": "0.1536",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                            {
                                "side": "yes",
                                "candidate_status": "blocked_fee_edge",
                                "reason": "fee_adjusted_edge_below_live_min",
                                "live_eligible": False,
                                "rank": 2,
                                "edge_bps": -1736,
                                "expected_net_edge": "-0.1936",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                        ],
                    },
                },
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
                "replay_gate": {"status": "blocked"},
                "live_eligible": False,
                "live_blockers": ["crypto_trading_enabled is false"],
                "signal": {
                    "recommended_side": "no",
                    "edge_bps": 420,
                    "candidate_trace": {
                        "selected_side": "no",
                        "min_edge_bps": 500,
                        "candidates": [
                            {
                                "side": "no",
                                "rank": 1,
                                "edge_bps": 420,
                                "expected_net_edge": "-0.0050",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                            {
                                "side": "yes",
                                "rank": 2,
                                "edge_bps": -177,
                                "expected_net_edge": "-0.0388",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                        ],
                    },
                },
            },
            {
                "market_ticker": "KXSOL15M-TEST",
                "asset_symbol": "SOL",
                "status": "open",
                "close_time": "2026-05-01T12:15:00Z",
                "target_price_dollars": "91.20",
                "yes_ask_dollars": "0.1900",
                "yes_bid_dollars": "0.1800",
                "no_ask_dollars": "0.8200",
                "volume": 42,
                "asset_mode": "shadow",
                "replay_gate": {"status": "passed"},
                "live_eligible": False,
                "live_blockers": [],
                "signal": {
                    "recommended_side": "no",
                    "edge_bps": 686,
                    "candidate_trace": {
                        "selected_side": "no",
                        "min_edge_bps": 500,
                        "candidates": [
                            {
                                "side": "no",
                                "candidate_status": "exploratory_shadow",
                                "reason": "broad_shadow_exploration",
                                "live_eligible": False,
                                "rank": 1,
                                "edge_bps": 686,
                                "expected_net_edge": "0.0486",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                            {
                                "side": "yes",
                                "candidate_status": "blocked_fee_edge",
                                "reason": "contract_price_below_crypto_min",
                                "live_eligible": False,
                                "rank": 2,
                                "edge_bps": -1386,
                                "expected_net_edge": "-0.1186",
                                "runtime_thresholds": {"min_fee_adjusted_edge_bps": 500},
                            },
                        ],
                    },
                },
            },
        ],
    }


def _crypto_html(payload: dict[str, object]) -> str:
    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <link rel="stylesheet" href="/static/crypto.css?v=test" />
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
        <script src="/static/crypto.js?v=test"></script>
      </body>
    </html>
    """


def _install_crypto_routes(page, payload: dict[str, object]) -> None:
    crypto_js = Path("src/kalshi_bot/web/static/crypto.js").read_text(encoding="utf-8")
    crypto_css = Path("src/kalshi_bot/web/static/crypto.css").read_text(encoding="utf-8")

    def route_handler(route) -> None:
        url = route.request.url
        path = urlparse(url).path
        if path == "/crypto":
            route.fulfill(status=200, content_type="text/html", body=_crypto_html(payload))
        elif path == "/static/crypto.js":
            route.fulfill(status=200, content_type="application/javascript", body=crypto_js)
        elif path == "/static/crypto.css":
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

            assert page.locator(".crypto-card").count() == 3
            assert page.locator(".crypto-live-blockers").count() == 1
            assert "passed" in page.locator(".crypto-status-pill", has_text="Replay Gate").inner_text()
            assert page.locator(".crypto-card").first.get_attribute("data-market") == "KXBTC15M-TEST"
            assert page.locator('[data-market="KXBTC15M-TEST"] .crypto-gate').inner_text() == "blocked"
            assert page.locator('[data-market="KXETH15M-TEST"] .crypto-gate').inner_text() == "blocked"
            assert (
                page.locator('[data-market="KXBTC15M-TEST"] .crypto-edge-summary').inner_text()
                == "raw +1736bps / net +1536bps / need +500bps"
            )
            assert (
                page.locator('[data-market="KXBTC15M-TEST"] .crypto-signal-blocker').inner_text()
                == "Signal blocked on Down: contract price below crypto min"
            )
            btc_side_details = page.locator('[data-market="KXBTC15M-TEST"] .crypto-side-detail').all_inner_texts()
            assert "net -1936bps / need +500bps · blocked: net edge below live min" in btc_side_details
            assert "net +1536bps / need +500bps · blocked: contract price below crypto min" in btc_side_details
            assert (
                page.locator('[data-market="KXETH15M-TEST"] .crypto-edge-summary').inner_text()
                == "raw +420bps / net -50bps / need +500bps"
            )
            assert (
                page.locator('[data-market="KXSOL15M-TEST"] .crypto-signal-blocker').inner_text()
                == "Signal blocked on Down: net edge below live min; shadow only"
            )
            assert (
                page.locator('[data-market="KXSOL15M-TEST"] .crypto-side-selected .crypto-side-detail').inner_text()
                == "net +486bps / need +500bps · blocked: net edge below live min; shadow only"
            )
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
	                  return [...card.querySelectorAll('select, button, a, h2, .crypto-live-blockers, .crypto-signal-blocker, .crypto-edge-summary')]
                    .every((element) => {
                      const rect = element.getBoundingClientRect();
                      return rect.left >= bounds.left - 1 && rect.right <= bounds.right + 1;
                    });
                })
                """
            )
        finally:
            browser.close()
