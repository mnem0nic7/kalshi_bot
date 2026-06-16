"""Memory-safe microstructure feature population for crypto training.

The model's microstructure features (spot_exchange_spread_bps,
spot_exchange_recent_trade_count, exchange bid/ask) are computed during the
feature build from each spot row's ``market_microstructure`` payload. To save
memory the materialize path loaded spot with ``defer_payload=True``, which
stripped that payload entirely — so those features were dead-constant and the
model could not beat market-mid on BTC 15m (it had no signal the mid doesn't
already price).

Loading the full payload and broadcasting it to the parallel build workers OOMs
the trainer (full payload ~8.7KB/row; the ``recent_trades`` array alone is
~3.9KB). The fix: compact each spot row's payload to ONLY the microstructure
scalars the feature builder needs (best_bid_ask + recent_trade_count), dropping
the heavy recent_trades array and everything else (~35x smaller), before the
build broadcast.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
    _compact_spot_microstructure_payload,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


def _full_payload() -> dict:
    return {
        "market_microstructure": {
            "product_id": "BTC-USD",
            "best_bid_ask": {
                "best_bid_dollars": "70049.00",
                "best_ask_dollars": "70051.00",
                "mid_dollars": "70050.00",
                "spread_bps": 3,
                "best_bid_size": "1.5",
                "best_ask_size": "2.0",
            },
            "recent_trade_count": 12,
            # The heavy array we DON'T need for features (only the count).
            "recent_trades": [{"trade_id": f"t{i}", "price": "70050", "size": "0.1"} for i in range(40)],
        },
        "some_other_heavy_key": list(range(200)),
    }


def test_compaction_keeps_microstructure_scalars_drops_heavy() -> None:
    compact = _compact_spot_microstructure_payload(_full_payload())
    micro = compact["market_microstructure"]
    assert micro["best_bid_ask"]["spread_bps"] == 3
    assert micro["best_bid_ask"]["best_bid_dollars"] == "70049.00"
    assert micro["recent_trade_count"] == 12
    # Heavy / irrelevant content dropped.
    assert "recent_trades" not in micro
    assert "some_other_heavy_key" not in compact
    # Compaction is a real shrink.
    assert len(str(compact)) < len(str(_full_payload())) / 3


def test_compaction_derives_count_from_recent_trades_when_count_absent() -> None:
    payload = _full_payload()
    del payload["market_microstructure"]["recent_trade_count"]
    compact = _compact_spot_microstructure_payload(payload)
    assert compact["market_microstructure"]["recent_trade_count"] == 40
    assert "recent_trades" not in compact["market_microstructure"]


def test_compaction_returns_empty_without_microstructure() -> None:
    assert _compact_spot_microstructure_payload({"foo": "bar"}) == {}
    assert _compact_spot_microstructure_payload({}) == {}
    assert _compact_spot_microstructure_payload(None) == {}


def test_compacted_payload_yields_live_microstructure_features() -> None:
    """A spot row carrying a COMPACTED payload must still produce non-None
    microstructure features (the whole point — restore the dead signal)."""

    class _Row:
        # Lightweight stand-in matching the attributes the feature builder reads.
        def __init__(self, payload):
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
            self.payload = payload

    compact = _compact_spot_microstructure_payload(_full_payload())
    rows = [_Row(compact)]
    prepared = _prepare_spot_context_series(rows)
    context = _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )

    assert context["spot_feature_status"] == "available"
    assert context["spot_exchange_spread_bps"] == 3
    assert context["spot_exchange_recent_trade_count"] == 12
    assert context["spot_exchange_bid_dollars"] == Decimal("70049.00")
