"""Prediction-scaled take-profit threshold (spec:
docs/superpowers/specs/2026-07-04-prediction-scaled-take-profit-design.md).

The effective threshold scales with the model's shrunk remaining edge:
no edge -> min_multiplier * base, edge >= ref -> max_multiplier * base,
linear in between. No usable model view -> static base (today's behavior).
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from kalshi_bot.services.crypto_take_profit import (
    CryptoTakeProfitService,
    _prediction_scaled_threshold,
)


# --- pure scaling math ---


def test_scaled_threshold_floors_at_zero_edge():
    assert _prediction_scaled_threshold(
        0.5, edge_remaining_dollars=0.0, edge_ref_dollars=0.05,
        min_multiplier=0.5, max_multiplier=2.0,
    ) == pytest.approx(0.25)


def test_scaled_threshold_floors_at_negative_edge():
    assert _prediction_scaled_threshold(
        0.5, edge_remaining_dollars=-0.10, edge_ref_dollars=0.05,
        min_multiplier=0.5, max_multiplier=2.0,
    ) == pytest.approx(0.25)


def test_scaled_threshold_caps_at_reference_edge():
    assert _prediction_scaled_threshold(
        0.5, edge_remaining_dollars=0.05, edge_ref_dollars=0.05,
        min_multiplier=0.5, max_multiplier=2.0,
    ) == pytest.approx(1.0)


def test_scaled_threshold_caps_beyond_reference_edge():
    assert _prediction_scaled_threshold(
        0.5, edge_remaining_dollars=0.50, edge_ref_dollars=0.05,
        min_multiplier=0.5, max_multiplier=2.0,
    ) == pytest.approx(1.0)


def test_scaled_threshold_linear_midpoint():
    assert _prediction_scaled_threshold(
        0.5, edge_remaining_dollars=0.025, edge_ref_dollars=0.05,
        min_multiplier=0.5, max_multiplier=2.0,
    ) == pytest.approx(0.5 * (0.5 + 1.5 * 0.5))


# --- service-level threshold resolution ---


def _settings(**overrides):
    base = dict(
        crypto_take_profit_prediction_scaling_enabled=True,
        crypto_take_profit_edge_ref_cents=5,
        crypto_take_profit_min_multiplier=0.5,
        crypto_take_profit_max_multiplier=2.0,
        crypto_take_profit_prediction_refresh_seconds=30.0,
        crypto_take_profit_prediction_max_age_seconds=120.0,
        crypto_edge_shrinkage_beta_floor=0.125,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _Forecaster:
    def __init__(self, fair_yes: str | None, model_type: str | None = "sklearn_logistic", raise_exc: bool = False):
        self.calls = 0
        self.fair_yes = fair_yes
        self.model_type = model_type
        self.raise_exc = raise_exc

    async def forecast(self, market):
        self.calls += 1
        if self.raise_exc:
            raise RuntimeError("boom")
        sig = MagicMock()
        sig.fair_yes_dollars = Decimal(self.fair_yes) if self.fair_yes is not None else None
        sig.candidate_trace = {"prediction_model": {"model_type": self.model_type}}
        return sig


def _service(settings, forecaster):
    svc = CryptoTakeProfitService.__new__(CryptoTakeProfitService)
    svc.settings = settings
    svc.forecast_service = forecaster
    svc._prediction_cache = {}
    svc._market_for_snapshot = lambda snapshot: MagicMock()
    return svc


def _pos(side="yes"):
    pos = MagicMock()
    pos.side = side
    pos.market_ticker = "KXHYPE15M-26JUL041500-00"
    return pos


def _snap(yes_bid="0.58", yes_ask="0.62"):
    snap = MagicMock()
    snap.yes_bid_dollars = Decimal(yes_bid)
    snap.yes_ask_dollars = Decimal(yes_ask)
    return snap


NOW = datetime(2026, 7, 4, 18, 0, tzinfo=UTC)


async def test_prediction_threshold_scales_with_remaining_edge():
    # fair_yes 0.70 vs mid_yes 0.60 -> raw 0.10, beta 0.125 -> 0.0125 shrunk
    # frac = 0.0125/0.05 = 0.25 -> mult = 0.5 + 1.5*0.25 = 0.875
    forecaster = _Forecaster("0.70")
    svc = _service(_settings(), forecaster)
    threshold, meta = await svc._prediction_threshold(_pos("yes"), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.5 * 0.875)
    assert meta["threshold_mode"] == "scaled"
    assert forecaster.calls == 1


async def test_prediction_threshold_no_side_sign_convention():
    # no-side: fair_no = 0.30, mid_no = 0.40 -> edge -0.10 -> floor multiplier
    forecaster = _Forecaster("0.70")
    svc = _service(_settings(), forecaster)
    threshold, meta = await svc._prediction_threshold(_pos("no"), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.25)
    assert meta["threshold_mode"] == "scaled"


async def test_prediction_threshold_throttles_forecast_calls():
    forecaster = _Forecaster("0.70")
    svc = _service(_settings(), forecaster)
    await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    assert forecaster.calls == 1


async def test_prediction_threshold_static_when_baseline_model():
    forecaster = _Forecaster("0.60", model_type="market_mid_baseline")
    svc = _service(_settings(), forecaster)
    threshold, meta = await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.5)
    assert meta["threshold_mode"] == "static"


async def test_prediction_threshold_static_when_forecast_raises():
    forecaster = _Forecaster("0.70", raise_exc=True)
    svc = _service(_settings(), forecaster)
    threshold, meta = await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.5)
    assert meta["threshold_mode"] == "static"


async def test_prediction_threshold_static_when_disabled():
    forecaster = _Forecaster("0.70")
    svc = _service(_settings(crypto_take_profit_prediction_scaling_enabled=False), forecaster)
    threshold, meta = await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.5)
    assert meta["threshold_mode"] == "static"
    assert forecaster.calls == 0


async def test_prediction_threshold_static_without_forecast_service():
    svc = _service(_settings(), None)
    threshold, meta = await svc._prediction_threshold(_pos(), _snap(), 0.5, NOW)
    assert threshold == pytest.approx(0.5)
    assert meta["threshold_mode"] == "static"


# --- _evaluate integration: profit compared against the SCALED threshold ---


class _FakeSession:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def commit(self): pass


async def test_evaluate_uses_prediction_scaled_threshold(monkeypatch):
    import kalshi_bot.services.crypto_take_profit as mod
    from datetime import timedelta

    snapshot = MagicMock()
    snapshot.yes_bid_dollars = Decimal("0.70")
    snapshot.yes_ask_dollars = Decimal("0.72")
    snapshot.status = "open"
    snapshot.observed_at = NOW

    class _Repo:
        def __init__(self, session): pass
        async def get_deployment_control(self, kalshi_env=None):
            return SimpleNamespace(active_color="green", kill_switch_enabled=False)
        async def get_latest_crypto_market_snapshot(self, ticker, kalshi_env=None):
            return snapshot

    async def _no_checkpoint(repo, **kw): return None

    monkeypatch.setattr(mod, "PlatformRepository", _Repo)
    monkeypatch.setattr(mod, "get_position_exit_submit_checkpoint", _no_checkpoint)

    svc = CryptoTakeProfitService.__new__(CryptoTakeProfitService)
    svc.settings = SimpleNamespace(
        kalshi_env="production", kalshi_taker_fee_rate=0.07,
    )
    svc.session_factory = lambda: _FakeSession()

    # entry 0.50, sell 0.70 -> net profit ratio ~ +0.31: below base 0.5,
    # above the scaled 0.20 the stubbed prediction returns.
    position = MagicMock()
    position.side = "yes"
    position.market_ticker = "KXHYPE15M-26JUL041500-00"
    position.average_price_dollars = Decimal("0.50")
    position.count_fp = Decimal("1.00")

    meta = {"threshold_mode": "scaled", "effective_threshold": 0.20}
    async def _fake_pred(pos, snap, base, now):
        return 0.20, meta
    svc._prediction_threshold = _fake_pred

    submitted = {}
    async def _fake_submit(**kwargs):
        submitted.update(kwargs)
        return {"ok": True}
    svc._submit = _fake_submit

    result = await svc._evaluate(position, NOW, timedelta(seconds=120), 0.5)
    assert result == {"ok": True}
    assert submitted.get("threshold_meta") == meta
