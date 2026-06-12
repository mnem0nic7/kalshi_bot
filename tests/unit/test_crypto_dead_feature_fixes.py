from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.crypto.services import (
    _crypto_quote_sequence_context,
    _crypto_spot_feature_asset_scope,
    CRYPTO_CROSS_ASSET_FEATURE_ASSETS,
)


NOW = datetime(2026, 6, 10, 16, 5, tzinfo=UTC)


def test_quote_sequence_context_computes_change_velocity_and_gap() -> None:
    ctx = _crypto_quote_sequence_context(
        Decimal("0.4000"),
        120.0,
        NOW,
        Decimal("0.3500"),
        200.0,
        NOW - timedelta(seconds=30),
    )

    assert ctx["market_mid_change_1"] == Decimal("0.0500")
    assert ctx["quote_observation_gap_seconds"] == 30.0
    assert ctx["spread_change_bps_1"] == -80.0
    # 5 cents over half a minute = 10 cents/minute
    assert abs(float(ctx["market_mid_velocity_per_min"]) - 0.10) < 1e-9


def test_quote_sequence_context_empty_without_prior() -> None:
    assert _crypto_quote_sequence_context(Decimal("0.4"), 100.0, NOW, None, None, None) == {}


def test_quote_sequence_context_empty_on_non_positive_gap() -> None:
    assert (
        _crypto_quote_sequence_context(
            Decimal("0.4"), 100.0, NOW, Decimal("0.3"), 100.0, NOW
        )
        == {}
    )


def test_quote_sequence_context_omits_spread_when_unknown() -> None:
    ctx = _crypto_quote_sequence_context(
        Decimal("0.4000"),
        None,
        NOW,
        Decimal("0.3500"),
        None,
        NOW - timedelta(seconds=15),
    )

    assert "spread_change_bps_1" not in ctx
    assert ctx["market_mid_change_1"] == Decimal("0.0500")


def test_spot_feature_asset_scope_unions_cross_assets() -> None:
    scope = _crypto_spot_feature_asset_scope(["ETH"])

    assert scope is not None
    assert set(scope) == {"ETH", *CRYPTO_CROSS_ASSET_FEATURE_ASSETS}


def test_spot_feature_asset_scope_passthrough_when_unscoped() -> None:
    assert _crypto_spot_feature_asset_scope(None) is None
    assert _crypto_spot_feature_asset_scope([]) is None


def _spot_row(asset: str, end_ts: datetime, close: str):
    from unittest.mock import MagicMock

    row = MagicMock()
    row.asset_symbol = asset
    row.end_ts = end_ts
    row.close_dollars = Decimal(close)
    return row


def test_cross_asset_context_emits_returns_for_all_seven_assets() -> None:
    from kalshi_bot.crypto.services import _cross_asset_context

    spot_by_asset = {
        asset: [
            _spot_row(asset, NOW - timedelta(minutes=3), "100"),
            _spot_row(asset, NOW - timedelta(minutes=2), "101"),
            _spot_row(asset, NOW - timedelta(minutes=1), "102"),
            _spot_row(asset, NOW, "103"),
        ]
        for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS
    }

    ctx = _cross_asset_context(spot_by_asset, decision_ts=NOW)

    for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS:
        key = asset.lower()
        assert ctx[f"{key}_return_1_pct"] is not None, f"{key}_return_1_pct missing"
        assert ctx[f"{key}_return_3_pct"] is not None, f"{key}_return_3_pct missing"


def test_cross_asset_context_excludes_self_asset() -> None:
    from kalshi_bot.crypto.services import _cross_asset_context

    spot_by_asset = {
        asset: [
            _spot_row(asset, NOW - timedelta(minutes=1), "100"),
            _spot_row(asset, NOW, "101"),
        ]
        for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS
    }

    ctx = _cross_asset_context(spot_by_asset, decision_ts=NOW, exclude_asset="SOL")

    assert ctx["sol_return_1_pct"] is None
    assert ctx["sol_return_3_pct"] is None
    assert ctx["btc_return_1_pct"] is not None
    assert ctx["doge_return_1_pct"] is not None


def test_cross_asset_context_missing_asset_rows_yield_none() -> None:
    from kalshi_bot.crypto.services import _cross_asset_context

    ctx = _cross_asset_context({}, decision_ts=NOW)

    for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS:
        assert ctx[f"{asset.lower()}_return_1_pct"] is None


def test_cross_asset_context_handles_descending_live_lists() -> None:
    # The live path fetches most-recent-first lists from the repo; the
    # bisect path must normalize ordering before computing returns.
    from kalshi_bot.crypto.services import _cross_asset_context

    descending = [
        _spot_row("BTC", NOW, "103"),
        _spot_row("BTC", NOW - timedelta(minutes=1), "102"),
        _spot_row("BTC", NOW - timedelta(minutes=2), "101"),
    ]
    ctx = _cross_asset_context({"BTC": descending}, decision_ts=NOW)

    assert ctx["btc_return_1_pct"] == (Decimal("103") - Decimal("102")) / Decimal("102")


def test_cross_asset_context_respects_as_of_cutoff() -> None:
    from kalshi_bot.crypto.services import _cross_asset_context

    rows = [
        _spot_row("BTC", NOW - timedelta(minutes=2), "100"),
        _spot_row("BTC", NOW - timedelta(minutes=1), "110"),
        _spot_row("BTC", NOW + timedelta(minutes=1), "999"),  # future row must be ignored
    ]
    ctx = _cross_asset_context({"BTC": rows}, decision_ts=NOW)

    assert ctx["btc_return_1_pct"] == (Decimal("110") - Decimal("100")) / Decimal("100")


def _ohlc_row(end_ts, close, source_kind="ohlc", provider="coinbase", payload=None):
    from unittest.mock import MagicMock

    row = MagicMock()
    row.end_ts = end_ts
    row.close_dollars = Decimal(close) if close is not None else None
    row.source_kind = source_kind
    row.provider = provider
    row.payload = payload or {}
    row.interval_seconds = 60
    row.open_dollars = row.high_dollars = row.low_dollars = row.close_dollars
    return row


def test_prepared_spot_context_matches_legacy_path() -> None:
    from kalshi_bot.crypto.services import (
        _as_utc_datetime,
        _prepare_spot_context_series,
        _spot_context_for_decision,
    )

    base = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    rows = []
    for i in range(60):  # one candle per minute
        rows.append(_ohlc_row(base + timedelta(minutes=i), str(100 + i)))
    for i in range(0, 60, 7):  # kraken duplicates on some timestamps
        rows.append(_ohlc_row(base + timedelta(minutes=i), str(200 + i), provider="kraken"))
    for i in range(40, 60):  # ticks near the end, some with microstructure
        payload = {"market_microstructure": {"best_bid": "1.0"}} if i % 3 == 0 else {}
        rows.append(
            _ohlc_row(base + timedelta(minutes=i, seconds=30), str(150 + i), source_kind="spot_tick", payload=payload)
        )
    rows.append(_ohlc_row(base + timedelta(minutes=30, seconds=10), None))  # null close dropped
    rows.sort(key=lambda r: r.end_ts)
    end_times = [_as_utc_datetime(r.end_ts) for r in rows]
    prepared = _prepare_spot_context_series(rows)

    for cutoff in (
        base + timedelta(minutes=5),
        base + timedelta(minutes=35),
        base + timedelta(minutes=50, seconds=45),
        base + timedelta(hours=2),
        base - timedelta(minutes=1),
    ):
        legacy = _spot_context_for_decision(
            rows, spot_end_times=end_times, decision_ts=cutoff,
            target_price=Decimal("120"), mid_yes=Decimal("0.50"),
        )
        fast = _spot_context_for_decision(
            rows, spot_end_times=end_times, prepared=prepared, decision_ts=cutoff,
            target_price=Decimal("120"), mid_yes=Decimal("0.50"),
        )
        assert fast == legacy, f"divergence at cutoff {cutoff}"
