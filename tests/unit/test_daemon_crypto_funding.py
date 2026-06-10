"""Unit tests for the periodic crypto funding-rate collection cycle.

These exercise the one-iteration cycle helper and the ops-event escalation
predicate directly, not the infinite loop (same approach as
test_daemon_crypto_history.py).
"""
from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.daemon import DaemonService


class _SpotService:
    def __init__(self, result=None):
        self.calls: list[dict[str, object]] = []
        self.result = result if result is not None else {"stored": 7, "assets": [], "errors": []}

    async def collect_funding_rates(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        return self.result


def make_daemon(tmp_path, *, enabled=True, active=True, spot_service=None, assets="BTC,ETH"):
    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-funding.db",
        crypto_funding_collect_enabled=enabled,
        crypto_model_nightly_assets=assets,
    )
    daemon.crypto_spot_service = spot_service

    async def active_color() -> bool:
        return active

    daemon._is_active_color = active_color  # type: ignore[method-assign]
    return daemon


@pytest.mark.asyncio
async def test_funding_cycle_collects_for_configured_assets(tmp_path) -> None:
    spot = _SpotService()
    daemon = make_daemon(tmp_path, spot_service=spot)

    result = await daemon._run_crypto_funding_rate_cycle()

    assert result == spot.result
    assert spot.calls == [{"asset_symbols": ["BTC", "ETH"]}]


@pytest.mark.asyncio
async def test_funding_cycle_skips_when_disabled(tmp_path) -> None:
    spot = _SpotService()
    daemon = make_daemon(tmp_path, enabled=False, spot_service=spot)

    assert await daemon._run_crypto_funding_rate_cycle() is None
    assert spot.calls == []


@pytest.mark.asyncio
async def test_funding_cycle_skips_on_inactive_color(tmp_path) -> None:
    spot = _SpotService()
    daemon = make_daemon(tmp_path, active=False, spot_service=spot)

    assert await daemon._run_crypto_funding_rate_cycle() is None
    assert spot.calls == []


@pytest.mark.asyncio
async def test_funding_cycle_skips_without_spot_service(tmp_path) -> None:
    daemon = make_daemon(tmp_path, spot_service=None)

    assert await daemon._run_crypto_funding_rate_cycle() is None


@pytest.mark.asyncio
async def test_funding_cycle_raises_when_all_assets_fail(tmp_path) -> None:
    spot = _SpotService(
        result={"stored": 0, "assets": ["BTC", "ETH"], "errors": [{"asset_symbol": "BTC", "error": "boom"}]}
    )
    daemon = make_daemon(tmp_path, spot_service=spot)

    with pytest.raises(RuntimeError, match="failed for all assets"):
        await daemon._run_crypto_funding_rate_cycle()


@pytest.mark.asyncio
async def test_funding_cycle_tolerates_partial_asset_errors(tmp_path) -> None:
    spot = _SpotService(
        result={"stored": 5, "assets": ["BTC", "ETH"], "errors": [{"asset_symbol": "ETH", "error": "boom"}]}
    )
    daemon = make_daemon(tmp_path, spot_service=spot)

    result = await daemon._run_crypto_funding_rate_cycle()
    assert result is not None
    assert result["stored"] == 5


def test_funding_interval_setting_plumbs_into_settings(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-funding-interval.db",
        crypto_funding_collect_interval_seconds=900,
    )
    assert settings.crypto_funding_collect_interval_seconds == 900
    # default
    default_settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/d2.db")
    assert default_settings.crypto_funding_collect_interval_seconds == 1800
    assert default_settings.crypto_funding_collect_enabled is True


def test_ops_event_escalation_fires_only_on_repeated_failures() -> None:
    should = DaemonService._crypto_funding_failure_should_log_ops_event
    assert [n for n in range(1, 10) if should(n)] == [3, 6, 9]
    assert should(0) is False
