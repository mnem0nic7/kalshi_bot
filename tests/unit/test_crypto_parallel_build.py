"""F2: asset-parallel decision-row build parity.

The cold full rebuild's COMPUTE phase runs `_crypto_decision_rows` single-threaded.
`_crypto_decision_rows_parallel` partitions by asset across a process pool and must
produce a byte-identical row set. These tests assert that parity on an in-memory
(no DB) multi-asset dataset, plus the serial-delegation fast paths.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    _crypto_decision_rows,
    _crypto_decision_rows_parallel,
    _crypto_training_build_id,
    _crypto_training_json_ready,
)
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, CryptoSpotOHLCRecord


NOW = datetime(2026, 5, 20, 0, 0, tzinfo=UTC)


def _snapshot(
    market_ticker: str,
    *,
    asset_symbol: str,
    series_ticker: str,
    observed_at: datetime,
    settlement_result: str | None,
    close_time: datetime,
    target_price: Decimal,
) -> CryptoMarketSnapshotRecord:
    # Fully populate the price columns so the payload-fallback path
    # (which would touch the deferred .payload column) is never hit.
    return CryptoMarketSnapshotRecord(
        kalshi_env="production",
        series_ticker=series_ticker,
        market_ticker=market_ticker,
        asset_symbol=asset_symbol,
        frequency="15m",
        status="closed" if settlement_result else "active",
        close_time=close_time,
        expected_expiration_time=close_time,
        target_price_dollars=target_price,
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


def _spot(
    asset_symbol: str,
    *,
    end_ts: datetime,
    close: Decimal,
) -> CryptoSpotOHLCRecord:
    return CryptoSpotOHLCRecord(
        kalshi_env="production",
        provider="coinbase",
        asset_symbol=asset_symbol,
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
        source_id=f"{asset_symbol}-USD",
        payload={},
    )


def _build_dataset(assets: dict[str, tuple[str, str, Decimal, Decimal]]):
    """Build an in-memory multi-asset dataset.

    assets maps asset_symbol -> (series_ticker, market_prefix, target_price, spot_close).
    Each asset gets one settled market with a pre-close decision snapshot + a
    settlement snapshot, plus a dense run of spot OHLC rows (so cross-asset spot
    features have data to consume).
    """
    close_ts = NOW - timedelta(hours=1)
    decision_ts = close_ts - timedelta(minutes=10)
    snapshots: list[CryptoMarketSnapshotRecord] = []
    spot_rows: list[CryptoSpotOHLCRecord] = []
    for asset, (series, prefix, target, spot_close) in assets.items():
        market = f"{prefix}-SETTLED"
        snapshots.append(
            _snapshot(
                market,
                asset_symbol=asset,
                series_ticker=series,
                observed_at=decision_ts,
                settlement_result=None,
                close_time=close_ts,
                target_price=target,
            )
        )
        snapshots.append(
            _snapshot(
                market,
                asset_symbol=asset,
                series_ticker=series,
                observed_at=close_ts,
                settlement_result="yes",
                close_time=close_ts,
                target_price=target,
            )
        )
        # ~3h of 60s spot rows leading up to the decision time.
        for i in range(180):
            end_ts = decision_ts - timedelta(minutes=180) + timedelta(minutes=i)
            spot_rows.append(_spot(asset, end_ts=end_ts, close=spot_close + Decimal(i)))
    return snapshots, spot_rows


def _fingerprint(rows):
    out = []
    for r in rows:
        feature_hash = _crypto_training_build_id(_crypto_training_json_ready(r))
        out.append((r["market_ticker"], r.get("row_id"), r.get("label_yes"), feature_hash))
    return sorted(out)


def test_parallel_matches_serial_two_assets() -> None:
    settings = Settings(kalshi_env="production")
    snapshots, spot_rows = _build_dataset(
        {
            "BTC": ("KXBTC15M", "KXBTC15M", Decimal("70000"), Decimal("70000")),
            "ETH": ("KXETH15M", "KXETH15M", Decimal("3500"), Decimal("3500")),
        }
    )
    candles: list = []

    serial = _crypto_decision_rows(snapshots, candles, spot_rows, settings=settings)
    parallel = _crypto_decision_rows_parallel(
        snapshots, candles, spot_rows, settings=settings, workers=2
    )

    # Guard: the dataset actually produced rows for both assets.
    assert serial, "serial build produced no rows; dataset is degenerate"
    assert {r["asset_symbol"] for r in serial} == {"BTC", "ETH"}

    assert _fingerprint(parallel) == _fingerprint(serial)


def test_workers_one_delegates_to_serial() -> None:
    settings = Settings(kalshi_env="production")
    snapshots, spot_rows = _build_dataset(
        {
            "BTC": ("KXBTC15M", "KXBTC15M", Decimal("70000"), Decimal("70000")),
            "ETH": ("KXETH15M", "KXETH15M", Decimal("3500"), Decimal("3500")),
        }
    )
    candles: list = []

    serial = _crypto_decision_rows(snapshots, candles, spot_rows, settings=settings)
    delegated = _crypto_decision_rows_parallel(
        snapshots, candles, spot_rows, settings=settings, workers=1
    )

    assert _fingerprint(delegated) == _fingerprint(serial)


def test_single_asset_with_many_workers_delegates_to_serial() -> None:
    settings = Settings(kalshi_env="production")
    snapshots, spot_rows = _build_dataset(
        {"BTC": ("KXBTC15M", "KXBTC15M", Decimal("70000"), Decimal("70000"))}
    )
    candles: list = []

    serial = _crypto_decision_rows(snapshots, candles, spot_rows, settings=settings)
    parallel = _crypto_decision_rows_parallel(
        snapshots, candles, spot_rows, settings=settings, workers=4
    )

    assert serial, "serial build produced no rows; dataset is degenerate"
    assert {r["asset_symbol"] for r in serial} == {"BTC"}
    assert _fingerprint(parallel) == _fingerprint(serial)
