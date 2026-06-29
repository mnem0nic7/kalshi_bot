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


def test_cross_venue_basis_buckets_ticks_across_venues() -> None:
    # All venues publish instantaneous ticks (interval=0) at ~15s, with slightly different
    # timestamps. They must be bucketed (60s) so the basis aligns them — a single bucket with
    # all 3 venues -> count=3, not three lone count=1 periods.
    from kalshi_bot.crypto.services import _crypto_basis_index

    cb = _Row("coinbase", "70100"); cb.interval_seconds = 0; cb.end_ts = NOW + timedelta(seconds=2)
    kr = _Row("kraken", "70000"); kr.interval_seconds = 0; kr.end_ts = NOW + timedelta(seconds=9)
    gm = _Row("gemini", "70050"); gm.interval_seconds = 0; gm.end_ts = NOW + timedelta(seconds=14)

    end_times, features = _crypto_basis_index([cb, kr, gm])
    assert len(end_times) == 1  # one 60s tick bucket
    assert features[0]["spot_cross_venue_count"] == 3


def test_cross_venue_basis_averages_multiple_ticks_per_venue_in_bucket() -> None:
    # Settlement-alignment (v14): Kalshi settles on a 60s TIME-WEIGHTED average of a multi-venue
    # index. When a venue publishes several ticks within the 60s bucket, the basis should use their
    # AVERAGE (a 60s-TWAP proxy), not just the latest tick — less tick-noise, closer to settlement.
    from kalshi_bot.crypto.services import _crypto_basis_index

    cb1 = _Row("coinbase", "70000"); cb1.interval_seconds = 0; cb1.end_ts = NOW + timedelta(seconds=2)
    cb2 = _Row("coinbase", "70200"); cb2.interval_seconds = 0; cb2.end_ts = NOW + timedelta(seconds=20)
    kr = _Row("kraken", "70000"); kr.interval_seconds = 0; kr.end_ts = NOW + timedelta(seconds=10)

    end_times, features = _crypto_basis_index([cb1, cb2, kr])
    assert len(end_times) == 1
    assert features[0]["spot_cross_venue_count"] == 2  # coinbase + kraken
    # coinbase = mean(70000, 70200) = 70100 (TWAP), NOT the latest tick 70200
    mean = (70100.0 + 70000.0) / 2
    assert features[0]["spot_cross_venue_basis_pct"] == pytest.approx((70100.0 - mean) / mean)


def test_cross_venue_basis_for_decision_prefers_freshest_multi_venue() -> None:
    # A lone coinbase tick AFTER a multi-venue bucket must not blank the basis: the decision
    # should fall back to the most recent multi-venue period <= decision.
    from kalshi_bot.crypto.services import _crypto_basis_for_decision

    cb1 = _Row("coinbase", "70100"); cb1.interval_seconds = 0; cb1.end_ts = NOW
    kr1 = _Row("kraken", "70000"); kr1.interval_seconds = 0; kr1.end_ts = NOW + timedelta(seconds=3)
    lone = _Row("coinbase", "70080"); lone.interval_seconds = 0; lone.end_ts = NOW + timedelta(seconds=75)

    feats = _crypto_basis_for_decision(
        [cb1, kr1, lone], decision_ts=NOW + timedelta(seconds=80)
    )
    assert feats["spot_cross_venue_count"] == 2  # used the multi-venue bucket, not the lone tick


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


def test_settlement_aligned_moneyness_uses_consensus_not_single_venue() -> None:
    # #3 (v14): Kalshi settles on the multi-venue CONSENSUS, not our single reference venue.
    # Moneyness must be computed off the consensus (close / (1 + basis)), so when coinbase sits
    # ABOVE the consensus, consensus-moneyness is smaller than single-venue moneyness. Momentum
    # keeps using the clean single series; only moneyness shifts to the settlement basis.
    rows = [_Row("coinbase", "70100"), _Row("kraken", "70000"), _Row("gemini", "70000")]
    prepared = _prepare_spot_context_series(rows)
    ctx = _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )
    consensus = (70100.0 + 70000.0 + 70000.0) / 3  # 70033.33
    # moneyness off consensus ≈ consensus - target (~33), NOT single-venue 70100-70000 = 100
    assert float(ctx["spot_moneyness_dollars"]) == pytest.approx(consensus - 70000.0, abs=0.6)
    assert float(ctx["spot_moneyness_dollars"]) < 90.0
    # close itself (for momentum) stays the single-venue reference, unchanged
    assert float(ctx["spot_close_dollars"]) == pytest.approx(70100.0)


def test_moneyness_falls_back_to_single_venue_when_one_venue() -> None:
    # Only one venue -> no consensus -> moneyness uses the single close (unchanged behavior).
    rows = [_Row("coinbase", "70100")]
    prepared = _prepare_spot_context_series(rows)
    ctx = _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )
    assert float(ctx["spot_moneyness_dollars"]) == pytest.approx(100.0)  # 70100 - 70000


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
