from __future__ import annotations

from pathlib import Path

from kalshi_bot.config import get_settings
from kalshi_bot.web.app import create_app
from tests.integration.asgi_sync_client import SameThreadASGITestClient as TestClient


def test_crypto_asset_mode_patch_route_persists_modes(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "crypto_asset_mode.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.request(
            "PATCH", "/api/crypto/assets/btc/mode", json={"mode": "live"}
        )
        invalid = client.request(
            "PATCH", "/api/crypto/assets/btc/mode", json={"mode": "rocket"}
        )
        status = client.get("/api/crypto/status")

    assert response.status_code == 200
    assert response.json()["asset_symbol"] == "BTC"
    assert response.json()["mode"] == "live"
    assert invalid.status_code == 400
    assert status.status_code == 200
    assert status.json()["asset_modes"]["BTC"] == "live"
    get_settings.cache_clear()


def test_crypto_static_asset_mode_controls_are_present() -> None:
    crypto_js = Path("src/kalshi_bot/web/static/crypto.js").read_text(encoding="utf-8")
    crypto_css = Path("src/kalshi_bot/web/static/crypto.css").read_text(
        encoding="utf-8"
    )
    crypto_template = Path("src/kalshi_bot/web/templates/crypto.html").read_text(
        encoding="utf-8"
    )

    assert "crypto-mode-select" in crypto_js
    assert "data-current-mode" in crypto_js
    assert "crypto-alert" in crypto_js
    assert 'method: "PATCH"' in crypto_js
    assert "Could not set" in crypto_js
    assert "crypto-alert" in crypto_css
    assert "crypto-live-blockers" in crypto_css
    assert "/static/crypto.js?v={{ static_asset_version }}" in crypto_template
    assert "/static/crypto.css?v={{ static_asset_version }}" in crypto_template
