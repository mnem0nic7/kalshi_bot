"""The decision-row build never reads a spot row's JSON ``payload`` (only the
structured OHLC/timestamp columns). During materialize the spot rows are loaded
once and then broadcast (pickled) to every parallel build worker, so carrying the
payload bloats both the parent and each worker copy; it OOMed the 32g trainer
even at 2 workers. ``_list_crypto_spot_rows_with_cross_assets`` must be able to
defer the payload so the broadcast stays lean.
"""
from __future__ import annotations

import pytest

from kalshi_bot.crypto.services import _list_crypto_spot_rows_with_cross_assets


class _RecordingRepo:
    def __init__(self):
        self.calls: list[dict] = []

    async def list_crypto_spot_ohlc(self, **kwargs):
        self.calls.append(kwargs)
        return []


@pytest.mark.asyncio
async def test_cross_asset_spot_loader_propagates_defer_payload():
    repo = _RecordingRepo()
    await _list_crypto_spot_rows_with_cross_assets(
        repo,
        frequency="15m",
        kalshi_env="production",
        requested_assets=["BTC"],
        since=None,
        limit=1000,
        defer_payload=True,
    )
    # Both the primary and cross-asset loads must defer the payload.
    assert repo.calls, "expected at least one spot load"
    assert all(call.get("defer_payload") is True for call in repo.calls)


@pytest.mark.asyncio
async def test_cross_asset_spot_loader_defaults_payload_loaded():
    repo = _RecordingRepo()
    await _list_crypto_spot_rows_with_cross_assets(
        repo,
        frequency="15m",
        kalshi_env="production",
        requested_assets=["BTC"],
        since=None,
        limit=1000,
    )
    assert all(call.get("defer_payload") is False for call in repo.calls)
