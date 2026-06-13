"""Spot backfill interval override: a one-time historical densification can fetch
finer candles (60s) than the frequency default (900s for 15m). Coinbase retains
~1-min candles ~7 months back; the stored history was coarse only because the
backfill requested the frequency interval. interval_seconds is part of the
spot-OHLC unique key and the training lookup reads by frequency, so finer rows
coexist with and densify the same series."""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from kalshi_bot.crypto.services import CryptoSpotService


def _service() -> CryptoSpotService:
    settings = SimpleNamespace(
        kalshi_env="production",
        crypto_history_lookback_days=180,
        crypto_spot_proxy_fallback_enabled=False,
        crypto_spot_request_timeout_seconds=20,
        coinbase_advanced_trade_authenticated_enabled=False,
        crypto_replay_min_spot_coverage_pct=0.0,
    )
    return CryptoSpotService(settings=settings, session_factory=None)  # type: ignore[arg-type]


def test_interval_override_passed_to_provider(monkeypatch) -> None:
    service = _service()
    captured: dict = {}

    class _FakeCoinbase:
        async def fetch_ohlc(self, asset, *, start, end, interval_seconds):
            captured["interval"] = interval_seconds
            return []

        async def aclose(self):
            pass

    monkeypatch.setattr(service, "_coinbase_client", lambda: _FakeCoinbase())
    monkeypatch.setattr(service, "_kraken_client", lambda: None)

    async def _no_kraken(*a, **k):
        return 0

    monkeypatch.setattr(service, "_collect_kraken_rows", _no_kraken)

    async def _fake_asset_symbols(*, asset_symbols, frequency):
        return ["BTC"]

    monkeypatch.setattr(service, "_asset_symbols", _fake_asset_symbols)

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def commit(self):
            pass

        async def execute(self, *a, **k):
            class _R:
                def scalars(self):
                    return []

            return _R()

    monkeypatch.setattr(service, "session_factory", lambda: _FakeSession())

    import asyncio

    asyncio.run(service.backfill(days=200, frequency="15m", asset_symbols=["BTC"], interval_seconds=60))
    assert captured["interval"] == 60


def test_interval_default_falls_back_to_frequency(monkeypatch) -> None:
    service = _service()
    captured: dict = {}

    class _FakeCoinbase:
        async def fetch_ohlc(self, asset, *, start, end, interval_seconds):
            captured["interval"] = interval_seconds
            return []

        async def aclose(self):
            pass

    monkeypatch.setattr(service, "_coinbase_client", lambda: _FakeCoinbase())
    monkeypatch.setattr(service, "_kraken_client", lambda: None)

    async def _no_kraken(*a, **k):
        return 0

    monkeypatch.setattr(service, "_collect_kraken_rows", _no_kraken)

    async def _fake_asset_symbols(*, asset_symbols, frequency):
        return ["BTC"]

    monkeypatch.setattr(service, "_asset_symbols", _fake_asset_symbols)

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def commit(self):
            pass

        async def execute(self, *a, **k):
            class _R:
                def scalars(self):
                    return []

            return _R()

    monkeypatch.setattr(service, "session_factory", lambda: _FakeSession())

    import asyncio

    asyncio.run(service.backfill(days=200, frequency="15m", asset_symbols=["BTC"]))
    assert captured["interval"] == 900  # 15m default
