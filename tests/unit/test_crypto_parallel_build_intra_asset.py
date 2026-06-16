"""Intra-asset parallel decision-row build.

The asset-partitioned build gives no speedup for one-asset-at-a-time training
(the continuous trainer): a single asset falls back to the serial, single-
threaded build, leaving 7 of 8 cores idle (~2.75h/asset on densified data).

Decision rows are per-market independent (candles, prior-snapshot, settlement
and the `seen` set are all keyed by market_ticker; spot is broadcast per asset),
so the build can be split by MARKET across workers and concatenated with no
change to the output row set. These tests drive the market-balancing helper and
assert byte-identical parity for a single asset spread over many markets.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    _balance_markets_into_bins,
    _crypto_decision_rows,
    _crypto_decision_rows_parallel,
    _crypto_training_build_id,
    _crypto_training_json_ready,
)
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, CryptoSpotOHLCRecord

NOW = datetime(2026, 6, 16, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Pure market-balancing helper (greedy longest-processing-time)
# ---------------------------------------------------------------------------

def test_balance_markets_covers_every_market_exactly_once():
    bins = _balance_markets_into_bins([("a", 5), ("b", 3), ("c", 2), ("d", 1)], 2)
    flat = sorted(m for b in bins for m in b)
    assert flat == ["a", "b", "c", "d"]
    assert len(bins) == 2
    # disjoint
    assert sum(len(b) for b in bins) == 4


def test_balance_markets_greedy_puts_heavy_alone():
    # a dominates; with 2 bins it sits alone and b+c share the other.
    bins = _balance_markets_into_bins([("a", 10), ("b", 1), ("c", 1)], 2)
    assert ["a"] in bins
    other = next(b for b in bins if "a" not in b)
    assert sorted(other) == ["b", "c"]


def test_balance_markets_one_per_bin_when_bins_exceed_markets():
    bins = _balance_markets_into_bins([("a", 1), ("b", 1)], 5)
    assert sorted(sorted(b) for b in bins) == [["a"], ["b"]]


def test_balance_markets_empty_input():
    assert _balance_markets_into_bins([], 4) == []


# ---------------------------------------------------------------------------
# Parity: one asset, many markets, split across workers == serial
# ---------------------------------------------------------------------------

def _snapshot(market_ticker, *, observed_at, settlement_result, close_time, target):
    return CryptoMarketSnapshotRecord(
        kalshi_env="production",
        series_ticker="KXBTC15M",
        market_ticker=market_ticker,
        asset_symbol="BTC",
        frequency="15m",
        status="closed" if settlement_result else "active",
        close_time=close_time,
        expected_expiration_time=close_time,
        target_price_dollars=target,
        yes_bid_dollars=Decimal("0.3000"),
        yes_ask_dollars=Decimal("0.3100"),
        no_bid_dollars=Decimal("0.6900"),
        no_ask_dollars=Decimal("0.7000"),
        last_price_dollars=Decimal("0.3050"),
        volume=100,
        open_interest=200,
        settlement_result=settlement_result,
        observed_at=observed_at,
        source_kind="test",
        payload={},
    )


def _spot(end_ts, close):
    return CryptoSpotOHLCRecord(
        kalshi_env="production",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=end_ts - timedelta(minutes=15),
        end_ts=end_ts,
        open_dollars=close,
        high_dollars=close + Decimal("1"),
        low_dollars=close - Decimal("1"),
        close_dollars=close,
        volume=Decimal("1.0"),
        observed_at=end_ts,
        source_kind="spot_ohlc",
        source_id="BTC-USD",
        payload={},
    )


def _single_asset_many_markets(n_markets: int = 6):
    snapshots: list[CryptoMarketSnapshotRecord] = []
    for m in range(n_markets):
        close_ts = NOW - timedelta(hours=m + 1)
        decision_ts = close_ts - timedelta(minutes=10)
        market = f"KXBTC15M-M{m}"
        snapshots.append(
            _snapshot(market, observed_at=decision_ts, settlement_result=None,
                      close_time=close_ts, target=Decimal("70000"))
        )
        snapshots.append(
            _snapshot(market, observed_at=close_ts, settlement_result="yes",
                      close_time=close_ts, target=Decimal("70000"))
        )
    spot_rows = [
        _spot(end_ts=NOW - timedelta(hours=n_markets + 1) - timedelta(minutes=10)
              + timedelta(minutes=i), close=Decimal("70000") + Decimal(i))
        for i in range((n_markets + 2) * 60)
    ]
    return snapshots, spot_rows


def _fingerprint(rows):
    out = []
    for r in rows:
        feature_hash = _crypto_training_build_id(_crypto_training_json_ready(r))
        out.append((r["market_ticker"], r.get("row_id"), r.get("label_yes"), feature_hash))
    return sorted(out)


def test_single_asset_many_markets_parallel_matches_serial() -> None:
    settings = Settings(kalshi_env="production")
    snapshots, spot_rows = _single_asset_many_markets(n_markets=6)
    candles: list = []

    serial = _crypto_decision_rows(snapshots, candles, spot_rows, settings=settings)
    parallel = _crypto_decision_rows_parallel(
        snapshots, candles, spot_rows, settings=settings, workers=4
    )

    assert serial, "serial build produced no rows; dataset is degenerate"
    assert {r["asset_symbol"] for r in serial} == {"BTC"}
    # 6 markets > 1 -> the build splits across workers (no longer serial-only).
    assert _fingerprint(parallel) == _fingerprint(serial)
