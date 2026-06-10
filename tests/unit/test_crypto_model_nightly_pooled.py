"""Unit tests for pooled-model nightly training.

Covers the stateless rotation helper in both modes, the pooled-vs-per-asset
artifact resolution preference order, and the pooled per-frequency nightly flow
(pooled train + replay run + per-asset gates). Stub repos/services follow the
patterns in test_daemon_crypto_history.py and test_crypto_model_nightly_gate.py.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import _latest_crypto_artifact_for_asset
from kalshi_bot.services.daemon import DaemonService

ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"]
FREQS = ["15m", "1h"]


# ---------------------------------------------------------------------------
# Rotation slot computation
# ---------------------------------------------------------------------------


def rotate(local_date: date, *, pooled_only: bool, assets=None, freqs=None):
    return DaemonService._crypto_model_nightly_rotation(
        local_date=local_date,
        configured_assets=list(ASSETS if assets is None else assets),
        training_freqs=list(FREQS if freqs is None else freqs),
        pooled_only=pooled_only,
    )


def test_pooled_rotation_has_two_slots_alternating_by_date_ordinal():
    day = date(2026, 6, 10)
    seen_freqs = []
    for offset in range(4):
        assets, frequencies, rotation = rotate(day + timedelta(days=offset), pooled_only=True)
        assert assets == ASSETS  # pooled mode trains the full configured asset list
        assert len(frequencies) == 1
        assert rotation["slot_count"] == 2
        assert rotation["slot"] == (day + timedelta(days=offset)).toordinal() % 2
        seen_freqs.append(frequencies[0])
    # strict alternation: consecutive nights flip frequency, period 2
    assert seen_freqs[0] != seen_freqs[1]
    assert seen_freqs[0] == seen_freqs[2]
    assert seen_freqs[1] == seen_freqs[3]
    assert set(seen_freqs) == set(FREQS)


def test_pooled_rotation_slot_count_follows_configured_frequencies():
    assets, frequencies, rotation = rotate(date(2026, 6, 10), pooled_only=True, freqs=["15m"])
    assert frequencies == ["15m"]
    assert rotation == {"slot": 0, "slot_count": 1}
    assert assets == ASSETS


def test_per_asset_rotation_keeps_fourteen_slots_and_covers_every_pair():
    day = date(2026, 6, 1)
    seen_pairs = set()
    for offset in range(14):
        assets, frequencies, rotation = rotate(day + timedelta(days=offset), pooled_only=False)
        assert len(assets) == 1
        assert len(frequencies) == 1
        assert rotation["slot_count"] == 14
        seen_pairs.add((assets[0], frequencies[0]))
    assert seen_pairs == {(a, f) for f in FREQS for a in ASSETS}


def test_per_asset_rotation_slot_zero_is_first_asset_first_frequency():
    # find a date whose ordinal is a multiple of 14
    day = date.fromordinal((date(2026, 6, 1).toordinal() // 14) * 14)
    assets, frequencies, _ = rotate(day, pooled_only=False)
    assert assets == ["BTC"]
    assert frequencies == ["15m"]


def test_rotation_handles_empty_configuration():
    assert rotate(date(2026, 6, 10), pooled_only=False, assets=[]) == ([], [], {"slot": 0, "slot_count": 0})
    assert rotate(date(2026, 6, 10), pooled_only=True, freqs=[]) == ([], [], {"slot": 0, "slot_count": 0})
    # pooled mode with no configured assets still rotates frequencies (trains unrestricted)
    assets, frequencies, rotation = rotate(date(2026, 6, 10), pooled_only=True, assets=[])
    assert assets == []
    assert len(frequencies) == 1
    assert rotation["slot_count"] == 2


# ---------------------------------------------------------------------------
# Artifact resolution preference order
# ---------------------------------------------------------------------------


class _ArtifactRepo:
    """Stub repo keyed by artifact_type; records lookup order."""

    def __init__(self, artifacts: dict[str, object]):
        self.artifacts = artifacts
        self.lookups: list[str] = []

    async def get_latest_crypto_model_artifact(self, *, frequency, artifact_type, kalshi_env):
        self.lookups.append(artifact_type)
        return self.artifacts.get(artifact_type)


def artifact(name: str, status: str = "trained"):
    return SimpleNamespace(name=name, status=status)


async def resolve(repo, *, prefer_generic: bool, allow_generic_fallback: bool = True):
    return await _latest_crypto_artifact_for_asset(
        repo,
        frequency="15m",
        artifact_type="model",
        kalshi_env="live",
        asset_symbol="BTC",
        allow_generic_fallback=allow_generic_fallback,
        prefer_generic=prefer_generic,
    )


@pytest.mark.asyncio
async def test_default_order_prefers_per_asset_artifact():
    repo = _ArtifactRepo({"model": artifact("pooled"), "model:BTC": artifact("per_asset")})
    found = await resolve(repo, prefer_generic=False)
    assert found.name == "per_asset"
    assert repo.lookups == ["model:BTC"]


@pytest.mark.asyncio
async def test_default_order_falls_back_to_generic_when_per_asset_missing():
    repo = _ArtifactRepo({"model": artifact("pooled")})
    found = await resolve(repo, prefer_generic=False)
    assert found.name == "pooled"
    assert repo.lookups == ["model:BTC", "model"]


@pytest.mark.asyncio
async def test_pooled_mode_prefers_trained_generic_artifact():
    repo = _ArtifactRepo({"model": artifact("pooled"), "model:BTC": artifact("per_asset")})
    found = await resolve(repo, prefer_generic=True)
    assert found.name == "pooled"
    assert repo.lookups == ["model"]


@pytest.mark.asyncio
async def test_pooled_mode_falls_back_to_per_asset_when_generic_missing():
    # Backward safety: live assets run on June-5 per-asset artifacts before the
    # first pooled artifact exists.
    repo = _ArtifactRepo({"model:BTC": artifact("per_asset")})
    found = await resolve(repo, prefer_generic=True)
    assert found.name == "per_asset"
    assert repo.lookups == ["model", "model:BTC"]


@pytest.mark.asyncio
async def test_pooled_mode_skips_untrained_generic_artifact():
    repo = _ArtifactRepo(
        {
            "model": artifact("pooled", status="insufficient_data"),
            "model:BTC": artifact("per_asset"),
        }
    )
    found = await resolve(repo, prefer_generic=True)
    assert found.name == "per_asset"


@pytest.mark.asyncio
async def test_pooled_mode_returns_untrained_generic_as_last_resort():
    repo = _ArtifactRepo({"model": artifact("pooled", status="insufficient_data")})
    found = await resolve(repo, prefer_generic=True)
    assert found.name == "pooled"


@pytest.mark.asyncio
async def test_prefer_generic_respects_allow_generic_fallback():
    repo = _ArtifactRepo({"model": artifact("pooled"), "model:BTC": artifact("per_asset")})
    found = await resolve(repo, prefer_generic=True, allow_generic_fallback=False)
    assert found.name == "per_asset"
    assert repo.lookups == ["model:BTC"]


# ---------------------------------------------------------------------------
# Pooled per-frequency nightly flow
# ---------------------------------------------------------------------------


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def commit(self):
        return None


class _NightlyRepo:
    artifact = None

    def __init__(self, session, kalshi_env=None):
        pass

    async def get_latest_crypto_model_artifact(self, **kwargs):
        return _NightlyRepo.artifact


def make_pooled_daemon(tmp_path, monkeypatch, calls, *, existing_artifact=None):
    monkeypatch.setattr("kalshi_bot.services.daemon.PlatformRepository", _NightlyRepo)
    _NightlyRepo.artifact = existing_artifact

    class _Recorder:
        def __init__(self, name):
            self.name = name

        def __getattr__(self, method):
            async def call(**kwargs):
                calls.append((f"{self.name}.{method}", kwargs))
                return {"status": "ok"}

            return call

    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/nightly-pooled.db",
        crypto_model_nightly_pooled_only=True,
        crypto_training_preflight_enabled=False,
        crypto_model_nightly_min_new_strict_rows=60,
    )
    daemon.session_factory = lambda: _Session()
    daemon.crypto_history_service = _Recorder("history")
    daemon.crypto_spot_service = None
    daemon.crypto_training_backfill_service = None
    daemon.crypto_forecast_service = _Recorder("forecast")
    daemon.crypto_replay_service = _Recorder("replay")
    return daemon


@pytest.mark.asyncio
async def test_pooled_nightly_trains_pooled_model_and_gates_per_asset(tmp_path, monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []
    daemon = make_pooled_daemon(tmp_path, monkeypatch, calls)

    decisions = await daemon._run_crypto_model_nightly_pooled_frequency(
        frequency="15m",
        assets=["BTC", "ETH"],
        by_asset_rows={"BTC": 100, "ETH": 100},
        now=datetime(2026, 6, 10, 12, 0, tzinfo=UTC),
        max_age=timedelta(days=14),
    )

    assert decisions == {"pooled": "refreshed", "BTC": "gated", "ETH": "gated"}
    # Multi-asset train → generic pooled "model" artifact (no asset suffix).
    assert ("forecast.train", {
        "frequency": "15m",
        "asset_symbols": ["BTC", "ETH"],
        "use_feature_store": False,
        "feature_store_only": False,
    }) in calls
    gate_calls = [kwargs for name, kwargs in calls if name == "replay.gate"]
    # one gate per asset (per-asset go/no-go) plus the pooled gate
    assert {"frequency": "15m", "asset_symbols": ["BTC"]} in gate_calls
    assert {"frequency": "15m", "asset_symbols": ["ETH"]} in gate_calls
    assert {"frequency": "15m", "asset_symbols": ["BTC", "ETH"]} in gate_calls
    assert ("replay.run", {"frequency": "15m", "asset_symbols": ["BTC", "ETH"]}) in calls


@pytest.mark.asyncio
async def test_pooled_nightly_skips_when_pooled_artifact_is_fresh(tmp_path, monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []
    fresh = SimpleNamespace(status="trained", trained_at=datetime(2026, 6, 10, 3, 0, tzinfo=UTC))
    daemon = make_pooled_daemon(tmp_path, monkeypatch, calls, existing_artifact=fresh)

    decisions = await daemon._run_crypto_model_nightly_pooled_frequency(
        frequency="15m",
        assets=["BTC", "ETH"],
        by_asset_rows={"BTC": 10, "ETH": 10},  # 20 pooled rows < 60 min new rows
        now=datetime(2026, 6, 10, 12, 0, tzinfo=UTC),
        max_age=timedelta(days=14),
    )

    assert decisions == {"pooled": "skipped_fresh"}
    assert calls == []
