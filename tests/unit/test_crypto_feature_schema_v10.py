"""Schema v10: trade-count log scaling + per-corpus constant pruning.

- spot_exchange_recent_trade_count used min(count, 50)/50 — permanently
  saturated for liquid assets (BTC always has 50+ recent trades), flattening
  the feature to a constant. Now log1p-scaled with a ~500-trade ceiling.
- Schema build prunes zero-variance columns on real-size corpora (>= 1000
  rows): expected-constant availability flags quietly, anything else loudly
  (WARNING) so pipeline regressions stay visible.
"""
from __future__ import annotations

import logging

from kalshi_bot.crypto.services import (
    _CRYPTO_CONSTANT_PRUNE_MIN_ROWS,
    _crypto_feature_schema,
    _crypto_raw_feature_vector,
)


def _row(asset: str = "BTC", **overrides) -> dict:
    base = {
        "asset_symbol": asset,
        "frequency": "15m",
        "spot_feature_status": "available",
        "settlement_window_status": "available",
    }
    base.update(overrides)
    return base


def _varied_rows(n: int, **constant_overrides) -> list[dict]:
    """Rows varying every feature input so that only availability flags (and any
    explicit constant_overrides) end up zero-variance."""
    from datetime import UTC, datetime, timedelta

    base_ts = datetime(2026, 6, 1, tzinfo=UTC)
    rows = []
    for i in range(n):
        row = _row(
            mid_yes_dollars=f"0.{30 + (i % 40):02d}",
            time_to_close_seconds=60 + (i % 800),
            spread_bps=100 + (i % 300),
            volume=i % 70,
            open_interest=i % 55,
            target_price_dollars=str(100 + (i % 9)),
            candle_momentum_dollars=f"0.0{i % 8}",
            spot_moneyness_pct=f"0.{i % 7:02d}",
            spot_momentum_pct=f"0.0{i % 4}",
            spot_return_1_pct=f"0.0{i % 5}",
            spot_return_3_pct=f"0.0{i % 6}",
            spot_return_6_pct=f"0.0{i % 7}",
            spot_return_12_pct=f"0.0{i % 8}",
            spot_return_24_pct=f"0.0{i % 9}",
            spot_realized_volatility=f"0.0{1 + i % 5}",
            spot_realized_volatility_32=f"0.0{1 + i % 6}",
            spot_target_distance_volatility=f"{(i % 11) - 5}",
            kalshi_mid_spot_gap=f"0.0{i % 6}",
            spot_stale_seconds=i % 900,
            asset_recent_yes_rate=f"0.{40 + (i % 20):02d}",
            asset_recent_mid_error=f"0.0{i % 5}",
            quote_source="snapshot_quotes" if i % 3 else "candlestick_close_proxy",
            strict_trade_eligible=bool(i % 2),
            market_age_seconds=i % 700,
            spot_exchange_spread_bps=i % 90,
            spot_exchange_recent_trade_count=i % 400,
            market_mid_change_1=f"0.0{i % 7}",
            market_mid_velocity_per_min=f"0.0{i % 5}",
            spread_change_bps_1=(i % 41) - 20,
            quote_observation_gap_seconds=10 + (i % 200),
            settlement_window_sample_count=i % 50,
            settlement_twap_target_distance_pct=f"0.0{i % 6}",
            settlement_window_return_pct=f"0.0{i % 4}",
            settlement_window_volatility=f"0.0{1 + i % 3}",
            settlement_ts=base_ts + timedelta(hours=i % 168),
            funding_rate_current=f"0.000{i % 9}",
            funding_rate_delta=f"0.000{i % 7}",
            yes_bid_dollars=f"0.{25 + (i % 45):02d}",
            **{
                f"{sym}_return_{p}_pct": f"0.0{(i + offset) % 5}"
                for offset, sym in enumerate(("btc", "eth", "sol", "xrp", "doge", "bnb", "hype"))
                for p in (1, 3)
            },
        )
        row.update(constant_overrides)
        rows.append(row)
    return rows


def test_trade_count_no_longer_saturates_for_liquid_assets() -> None:
    schema = _crypto_feature_schema([_row(), _row(asset="ETH")])
    index = schema["numeric_feature_names"].index("spot_exchange_recent_trade_count")

    busy = _crypto_raw_feature_vector(_row(spot_exchange_recent_trade_count=60), schema)
    busier = _crypto_raw_feature_vector(_row(spot_exchange_recent_trade_count=400), schema)

    assert busy[index] != busier[index]
    assert 0.0 < busy[index] < busier[index] <= 1.0


def test_tiny_corpora_skip_constant_pruning() -> None:
    schema = _crypto_feature_schema([_row(), _row(asset="ETH")])
    assert "pruned_constant_features" not in schema
    assert "spot_available" in schema["numeric_feature_names"]


def test_expected_constants_pruned_quietly_on_real_corpora(caplog) -> None:
    rows = _varied_rows(_CRYPTO_CONSTANT_PRUNE_MIN_ROWS)
    with caplog.at_level(logging.INFO, logger="kalshi_bot.crypto.services"):
        schema = _crypto_feature_schema(rows)

    pruned = schema.get("pruned_constant_features") or []
    assert "spot_available" in pruned
    assert "settlement_window_available" in pruned
    assert "spot_available" not in schema["numeric_feature_names"]
    assert not any(r.levelno == logging.WARNING and "pruned_unexpected" in r.message for r in caplog.records)


def test_unexpected_constant_pruned_loudly(caplog) -> None:
    # candle_momentum frozen across a real-size corpus is a regression signal.
    rows = _varied_rows(_CRYPTO_CONSTANT_PRUNE_MIN_ROWS, candle_momentum_dollars="0")
    with caplog.at_level(logging.WARNING, logger="kalshi_bot.crypto.services"):
        schema = _crypto_feature_schema(rows)

    assert "candle_momentum" in (schema.get("pruned_constant_features") or [])
    assert any("crypto_schema_pruned_unexpected_constant_features" in r.message for r in caplog.records)


def test_vector_matches_pruned_feature_names() -> None:
    rows = _varied_rows(_CRYPTO_CONSTANT_PRUNE_MIN_ROWS)
    schema = _crypto_feature_schema(rows)
    vector = _crypto_raw_feature_vector(rows[0], schema)
    assert len(vector) == len(schema["feature_names"])
