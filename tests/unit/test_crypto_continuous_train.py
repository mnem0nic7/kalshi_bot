"""Continuous one-asset-at-a-time training loop (replaces the nightly on the
dedicated trainer).

The trainer must train one market at a time via the feature-store path
(preflight prepare -> train(feature_store_only=True) -> replay run -> per-asset
gate), isolate a single asset's failure so the rest of the cycle still runs, and
refresh the pooled gate + sizing policy after each full pass. Stub services
follow the pattern in test_crypto_model_nightly_pooled.py.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.daemon import DaemonService


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def commit(self):
        return None


def _make_trainer(tmp_path, calls, *, preflight: bool, failing_asset: str | None = None):
    class _Recorder:
        def __init__(self, name):
            self.name = name

        def __getattr__(self, method):
            async def call(**kwargs):
                calls.append((f"{self.name}.{method}", kwargs))
                if (
                    self.name == "forecast"
                    and method == "train"
                    and failing_asset is not None
                    and kwargs.get("asset_symbols") == [failing_asset]
                ):
                    raise RuntimeError(f"boom training {failing_asset}")
                return {"status": "ok"}

            return call

    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/continuous.db",
        crypto_training_preflight_enabled=preflight,
    )
    daemon.session_factory = lambda: _Session()
    daemon.crypto_history_service = _Recorder("history")
    daemon.crypto_spot_service = _Recorder("spot")
    daemon.crypto_training_backfill_service = _Recorder("preflight")
    daemon.crypto_forecast_service = _Recorder("forecast")
    daemon.crypto_replay_service = _Recorder("replay")

    sizing_calls: list[dict] = []

    async def _fake_sizing(*, frequency, assets, evaluated_at):
        sizing_calls.append({"frequency": frequency, "assets": list(assets)})
        return {"status": "updated"}

    daemon._update_crypto_pnl_sizing_policy = _fake_sizing  # type: ignore[assignment]
    return daemon, sizing_calls


@pytest.mark.asyncio
async def test_continuous_cycle_trains_each_asset_via_feature_store(tmp_path):
    calls: list[tuple[str, dict]] = []
    daemon, sizing_calls = _make_trainer(tmp_path, calls, preflight=True)

    decisions = await daemon._run_one_crypto_train_cycle(
        frequency="15m",
        assets=["BTC", "ETH"],
        now=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )

    assert decisions == {"BTC": "refreshed", "ETH": "refreshed"}
    # Feature-store path: preflight prepare then train with feature_store_only=True.
    assert ("preflight.prepare", {"frequency": "15m", "asset_symbols": ["BTC"], "run_source_backfill": True}) in calls
    assert ("forecast.train", {
        "frequency": "15m",
        "asset_symbols": ["BTC"],
        "use_feature_store": True,
        "feature_store_only": True,
    }) in calls
    # Per-asset replay run (bounded by the continuous-train replay window/limit)
    # + per-asset gate for each asset.
    assert (
        "replay.run",
        {"frequency": "15m", "asset_symbols": ["BTC"], "days": 10, "limit": 40_000},
    ) in calls
    assert ("replay.gate", {"frequency": "15m", "asset_symbols": ["ETH"]}) in calls
    # Pooled gate over all assets + sizing policy refresh once per cycle.
    assert ("replay.gate", {"frequency": "15m", "asset_symbols": ["BTC", "ETH"]}) in calls
    assert sizing_calls == [{"frequency": "15m", "assets": ["BTC", "ETH"]}]


@pytest.mark.asyncio
async def test_continuous_cycle_isolates_per_asset_failure(tmp_path):
    calls: list[tuple[str, dict]] = []
    daemon, sizing_calls = _make_trainer(tmp_path, calls, preflight=True, failing_asset="BTC")

    decisions = await daemon._run_one_crypto_train_cycle(
        frequency="15m",
        assets=["BTC", "ETH"],
        now=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )

    # BTC errored but ETH still trained — the cycle is not aborted.
    assert decisions["BTC"] == "error"
    assert decisions["ETH"] == "refreshed"
    assert ("forecast.train", {
        "frequency": "15m",
        "asset_symbols": ["ETH"],
        "use_feature_store": True,
        "feature_store_only": True,
    }) in calls
    # Sizing policy still refreshed (ETH did refresh).
    assert sizing_calls == [{"frequency": "15m", "assets": ["BTC", "ETH"]}]


@pytest.mark.asyncio
async def test_continuous_cycle_non_preflight_uses_collect_path(tmp_path):
    calls: list[tuple[str, dict]] = []
    daemon, _ = _make_trainer(tmp_path, calls, preflight=False)

    await daemon._run_one_crypto_train_cycle(
        frequency="15m",
        assets=["BTC"],
        now=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )

    # No preflight prepare; raw collect path + train(feature_store_only=False).
    assert not any(name == "preflight.prepare" for name, _ in calls)
    assert ("history.collect_settled", {"frequency": "15m", "asset_symbols": ["BTC"]}) in calls
    assert ("forecast.train", {
        "frequency": "15m",
        "asset_symbols": ["BTC"],
        "use_feature_store": False,
        "feature_store_only": False,
    }) in calls
