"""Cross-venue settlement-basis feature.

Kalshi crypto markets settle on a multi-venue index (CF Benchmarks), but our spot
moneyness is single-venue (Coinbase). When venues disagree near a strike, our
single-venue moneyness is a noisy proxy for the settlement basis. This feature
exposes, per decision, how far our reference venue sits from the cross-venue mean
(signed basis) and how much the venues disagree (spread) — computed from the
PRE-dedup multi-venue rows, so it survives the momentum-protecting dedup that
collapses to one provider per period.

Plumbing only: the feature is computed + accrues into training rows now; it is NOT
validated/retrained until ~30d of dense multi-venue overlap exists.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
    _crypto_cross_venue_basis_features,
    _crypto_feature_schema,
    _crypto_raw_feature_vector,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


class _Row:
    def __init__(self, provider: str, close: str):
        self.kalshi_env = "production"
        self.provider = provider
        self.asset_symbol = "BTC"
        self.quote_currency = "USD"
        self.frequency = "15m"
        self.interval_seconds = 900
        self.start_ts = NOW - timedelta(minutes=15)
        self.end_ts = NOW
        self.open_dollars = Decimal(close)
        self.high_dollars = Decimal(close)
        self.low_dollars = Decimal(close)
        self.close_dollars = Decimal(close)
        self.volume = Decimal("1")
        self.source_kind = "spot_ohlc"
        self.source_id = f"{provider}-1"
        self.observed_at = NOW
        self.payload = {}


def test_cross_venue_basis_pure_function() -> None:
    feats = _crypto_cross_venue_basis_features({"coinbase": 70100.0, "kraken": 70000.0, "gemini": 70000.0})
    assert feats["spot_cross_venue_count"] == 3
    mean = (70100.0 + 70000.0 + 70000.0) / 3
    # stored as fractions (matching spot_moneyness_pct convention)
    assert feats["spot_cross_venue_basis_pct"] == pytest.approx((70100.0 - mean) / mean)
    assert feats["spot_cross_venue_spread_pct"] == pytest.approx((70100.0 - 70000.0) / mean)


def test_cross_venue_basis_single_venue_is_zero_spread() -> None:
    feats = _crypto_cross_venue_basis_features({"coinbase": 70000.0})
    assert feats["spot_cross_venue_count"] == 1
    assert feats["spot_cross_venue_spread_pct"] == 0.0
    assert feats["spot_cross_venue_basis_pct"] == 0.0


def test_cross_venue_basis_empty_is_none() -> None:
    feats = _crypto_cross_venue_basis_features({})
    assert feats["spot_cross_venue_count"] == 0
    assert feats["spot_cross_venue_spread_pct"] is None
    assert feats["spot_cross_venue_basis_pct"] is None


def test_cross_venue_basis_excludes_ticks_uses_aligned_candles_only() -> None:
    # Coinbase publishes instantaneous ticks (interval_seconds=0) every 15s; Kraken/Gemini
    # publish 15-min candles (interval=900). Ticks must be excluded from the basis index so a
    # lone coinbase tick at decision time doesn't yield count=1 — the basis must come from the
    # aligned candle period where multiple venues share (end_ts, interval).
    from kalshi_bot.crypto.services import _crypto_basis_index

    tick = _Row("coinbase", "70100")
    tick.interval_seconds = 0
    tick.end_ts = NOW + timedelta(seconds=30)  # most recent, but tick -> excluded
    cb_candle = _Row("coinbase", "70100")
    kr_candle = _Row("kraken", "70000")
    gm_candle = _Row("gemini", "70000")  # all interval_seconds=900, end_ts=NOW

    end_times, features = _crypto_basis_index([tick, cb_candle, kr_candle, gm_candle])
    # only the one aligned candle period survives; the tick is dropped
    assert len(end_times) == 1
    assert features[0]["spot_cross_venue_count"] == 3


def test_cross_venue_basis_flows_through_spot_context() -> None:
    rows = [_Row("coinbase", "70100"), _Row("kraken", "70000"), _Row("gemini", "70000")]
    prepared = _prepare_spot_context_series(rows)
    ctx = _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )
    # dedup collapses to coinbase for moneyness, but the basis sees all 3 venues
    mean = (70100.0 + 70000.0 + 70000.0) / 3
    assert ctx["spot_cross_venue_count"] == 3
    assert ctx["spot_cross_venue_spread_pct"] == pytest.approx(100.0 / mean, abs=1e-6)
    assert ctx["spot_cross_venue_basis_pct"] > 0


def test_cross_venue_basis_in_schema_and_feature_vector() -> None:
    row = {
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5"),
        "spot_feature_status": "available",
        "spot_cross_venue_basis_pct": 0.001,  # 0.1% basis (fraction)
        "spot_cross_venue_spread_pct": 0.002,
        "spot_cross_venue_count": 3,
    }
    schema = _crypto_feature_schema([row])
    for name in ("spot_cross_venue_basis_pct", "spot_cross_venue_spread_pct", "spot_cross_venue_count"):
        assert name in schema["numeric_feature_names"]
    vector = _crypto_raw_feature_vector(row, schema)
    # extractor clamps ±0.005 and scales ×200 -> 0.001 maps to 0.2; count/3 -> 1.0
    assert vector[schema["numeric_feature_names"].index("spot_cross_venue_basis_pct")] == pytest.approx(0.2)
    assert vector[schema["numeric_feature_names"].index("spot_cross_venue_spread_pct")] == pytest.approx(0.4)
    assert vector[schema["numeric_feature_names"].index("spot_cross_venue_count")] == pytest.approx(1.0)


def test_cross_venue_basis_defaults_to_zero_when_absent() -> None:
    row = {"asset_symbol": "BTC", "mid_yes_dollars": Decimal("0.5"), "spot_feature_status": "missing"}
    schema = _crypto_feature_schema([row])
    vector = _crypto_raw_feature_vector(row, schema)
    for name in ("spot_cross_venue_basis_pct", "spot_cross_venue_spread_pct", "spot_cross_venue_count"):
        idx = schema["numeric_feature_names"].index(name)
        assert vector[idx] == 0.0
