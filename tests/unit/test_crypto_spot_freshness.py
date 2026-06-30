"""Historical-mode spot context must use fresh point-in-time ticks (the fix).

ROOT CAUSE (2026-06-30): training/replay rows are built in HISTORICAL mode, which
dropped `spot_tick` rows and computed moneyness from the up-to-900s-stale 15-min
candle. On a 15-min market that ~7.6-min-stale spot makes moneyness a coin flip,
so the analytic vol fair-value (and every spot feature) collapsed to base rate
(Brier ~0.25). LIVE mode already uses the fresh ~20s ticks. These tests pin that
HISTORICAL mode uses the freshest point-in-time spot (tick OR candle), matching
live, so the model trains on the same fresh-spot features it is served.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_HISTORICAL,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

NOW = datetime(2026, 6, 30, 12, 0, tzinfo=UTC)
# Production passes settings (coinbase max_stale=180s); without it the fallback
# constant (5s) would mark even a 10s tick stale. Reflect production here.
SETTINGS = Settings()


class _Spot:
    def __init__(self, *, source_kind: str, interval_seconds: int, age_seconds: int, close: str):
        self.kalshi_env = "production"
        self.provider = "coinbase"
        self.asset_symbol = "HYPE"
        self.quote_currency = "USD"
        self.frequency = "15m"
        self.interval_seconds = interval_seconds
        self.end_ts = NOW - timedelta(seconds=age_seconds)
        self.start_ts = self.end_ts - timedelta(seconds=interval_seconds or 10)
        self.open_dollars = Decimal(close)
        self.high_dollars = Decimal(close)
        self.low_dollars = Decimal(close)
        self.close_dollars = Decimal(close)
        self.volume = Decimal("1")
        self.source_kind = source_kind
        self.source_id = f"{source_kind}-{age_seconds}"
        self.observed_at = self.end_ts
        self.payload = {}


def _rows() -> list[_Spot]:
    # A stale 15-min candle (600s old) plus several fresh ticks, the freshest 10s old.
    return [
        _Spot(source_kind="spot_ohlc", interval_seconds=900, age_seconds=600, close="100"),
        _Spot(source_kind="spot_tick", interval_seconds=0, age_seconds=40, close="101"),
        _Spot(source_kind="spot_tick", interval_seconds=0, age_seconds=25, close="102"),
        _Spot(source_kind="spot_tick", interval_seconds=0, age_seconds=10, close="103"),
    ]


def _ctx(rows, *, prepared):
    return _spot_context_for_decision(
        rows,
        prepared=_prepare_spot_context_series(rows) if prepared else None,
        decision_ts=NOW,
        target_price=Decimal("100"),
        mid_yes=Decimal("0.50"),
        settings=SETTINGS,
        mode=CRYPTO_SPOT_CONTEXT_HISTORICAL,
    )


def test_historical_uses_fresh_tick_prepared_path() -> None:
    # The trainer/replay path passes a prepared series; it must surface the 10s-old
    # tick (close=103), not the 600s-old candle (close=100).
    ctx = _ctx(_rows(), prepared=True)
    assert ctx["spot_feature_status"] == "available"
    assert int(ctx["spot_stale_seconds"]) <= 15
    assert float(ctx["spot_close_dollars"]) == 103.0


def test_historical_uses_fresh_tick_unprepared_path() -> None:
    # The non-prepared branch (also historical) must likewise prefer the fresh tick.
    ctx = _ctx(_rows(), prepared=False)
    assert int(ctx["spot_stale_seconds"]) <= 15
    assert float(ctx["spot_close_dollars"]) == 103.0


def test_historical_falls_back_to_candle_when_no_ticks() -> None:
    # Pre-tick-era data (only candles): unchanged behaviour, uses the candle.
    rows = [_Spot(source_kind="spot_ohlc", interval_seconds=900, age_seconds=300, close="100")]
    ctx = _ctx(rows, prepared=True)
    assert float(ctx["spot_close_dollars"]) == 100.0
    assert int(ctx["spot_stale_seconds"]) == 300
