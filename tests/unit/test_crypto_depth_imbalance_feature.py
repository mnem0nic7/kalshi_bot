"""Order-book depth imbalance as a model feature.

Depth imbalance = (bid_size - ask_size)/(bid_size + ask_size) is the classic
short-horizon DIRECTIONAL microstructure signal. The spot payload carries
best_bid_size/best_ask_size in best_bid_ask (preserved by microstructure
compaction), and _materialize_spot_microstructure already computes the same
imbalance — but only writes it to an unused table. It was never a model feature,
so the model had spread + trade-count (non-directional) but no order-flow
direction. This wires it into the decision-row spot context and feature vector.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
    _crypto_feature_schema,
    _crypto_raw_feature_vector,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


class _Row:
    def __init__(self, best_bid_ask: dict | None):
        self.kalshi_env = "production"
        self.provider = "coinbase"
        self.asset_symbol = "BTC"
        self.quote_currency = "USD"
        self.frequency = "15m"
        self.interval_seconds = 900
        self.start_ts = NOW - timedelta(minutes=15)
        self.end_ts = NOW
        self.open_dollars = Decimal("70000")
        self.high_dollars = Decimal("70100")
        self.low_dollars = Decimal("69900")
        self.close_dollars = Decimal("70050")
        self.volume = Decimal("1")
        self.source_kind = "spot_ohlc"
        self.source_id = "s1"
        self.observed_at = NOW
        micro = {"market_microstructure": {"recent_trade_count": 10}}
        if best_bid_ask is not None:
            micro["market_microstructure"]["best_bid_ask"] = best_bid_ask
        self.payload = micro


def _context(best_bid_ask: dict | None) -> dict:
    rows = [_Row(best_bid_ask)]
    prepared = _prepare_spot_context_series(rows)
    return _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )


def test_depth_imbalance_computed_from_sizes() -> None:
    ctx = _context(
        {
            "best_bid_dollars": "70049.00",
            "best_ask_dollars": "70051.00",
            "spread_bps": 3,
            "best_bid_size": "3.0",
            "best_ask_size": "1.0",
        }
    )
    # (3 - 1) / (3 + 1) = 0.5 (bid-heavy -> positive)
    assert ctx["spot_exchange_depth_imbalance"] == pytest.approx(0.5)


def test_depth_imbalance_none_without_sizes() -> None:
    ctx = _context({"best_bid_dollars": "70049.00", "best_ask_dollars": "70051.00", "spread_bps": 3})
    assert ctx["spot_exchange_depth_imbalance"] is None


def test_depth_imbalance_in_schema_and_feature_vector() -> None:
    row = {
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5"),
        "spot_feature_status": "available",
        "spot_exchange_depth_imbalance": 0.5,
    }
    schema = _crypto_feature_schema([row])
    assert "spot_exchange_depth_imbalance" in schema["numeric_feature_names"]
    vector = _crypto_raw_feature_vector(row, schema)
    idx = schema["numeric_feature_names"].index("spot_exchange_depth_imbalance")
    assert vector[idx] == pytest.approx(0.5)


def test_depth_imbalance_feature_defaults_to_zero_when_absent() -> None:
    row = {"asset_symbol": "BTC", "mid_yes_dollars": Decimal("0.5"), "spot_feature_status": "missing"}
    schema = _crypto_feature_schema([row])
    vector = _crypto_raw_feature_vector(row, schema)
    idx = schema["numeric_feature_names"].index("spot_exchange_depth_imbalance")
    assert vector[idx] == 0.0
