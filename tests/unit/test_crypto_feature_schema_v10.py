"""Schema v10: trade-count log scaling + data-density dead-feature classification.

- spot_exchange_recent_trade_count used min(count, 50)/50, saturating at 1.0 for
  any liquid window. Now log1p-scaled with a ~500 ceiling so live-era trade
  intensity stays resolvable.
- The schema builder does NOT auto-prune corpus-constant columns: it runs per
  walk-forward fold, so per-fold pruning produced inconsistent schemas and
  false-alarm warnings. Constants are reported once on the full corpus by
  _crypto_dead_feature_report, which now separates data-density constants
  (microstructure features with no historical coverage -> INFO) from genuine
  regressions (-> WARNING).
"""
from __future__ import annotations

import logging

from kalshi_bot.crypto.services import (
    _crypto_dead_feature_report,
    _crypto_feature_schema,
    _crypto_raw_feature_vector,
)


def _row(asset: str = "BTC", **overrides) -> dict:
    base = {"asset_symbol": asset, "frequency": "15m"}
    base.update(overrides)
    return base


def test_trade_count_no_longer_saturates_for_liquid_assets() -> None:
    schema = _crypto_feature_schema([_row(), _row(asset="ETH")])
    index = schema["numeric_feature_names"].index("spot_exchange_recent_trade_count")

    busy = _crypto_raw_feature_vector(_row(spot_exchange_recent_trade_count=60), schema)
    busier = _crypto_raw_feature_vector(_row(spot_exchange_recent_trade_count=400), schema)

    # Old min(count,50)/50 returned 1.0 for both; log scaling keeps them distinct.
    assert busy[index] != busier[index]
    assert 0.0 < busy[index] < busier[index] <= 1.0


def test_schema_is_stable_not_pruned_per_corpus() -> None:
    # Two corpora differing only in row count must produce identical schemas:
    # the builder must not prune based on per-corpus variance (it runs per fold).
    small = _crypto_feature_schema([_row(), _row(asset="ETH")])
    big = _crypto_feature_schema([_row() for _ in range(2000)] + [_row(asset="ETH")])
    assert small["feature_names"] == big["feature_names"]
    assert "pruned_constant_features" not in big


def test_microstructure_constants_reported_as_data_density_not_warning(caplog) -> None:
    # All rows identical => every feature is constant. The microstructure/
    # availability features must be classified as data-density (INFO), and the
    # rest as unexpected dead (WARNING).
    rows = [_row(spot_feature_status="available") for _ in range(5)]
    with caplog.at_level(logging.INFO, logger="kalshi_bot.crypto.services"):
        report = _crypto_dead_feature_report(rows)

    assert "spot_exchange_recent_trade_count" in report["data_density_dead_feature_names"]
    assert "spot_available" in report["data_density_dead_feature_names"]
    # data-density names are excluded from the WARNING bucket
    assert "spot_exchange_recent_trade_count" not in report["unexpected_dead_feature_names"]
    assert any(
        r.levelno == logging.INFO and "data_density_constant_features" in r.message
        for r in caplog.records
    )


def test_dead_feature_report_counts_all_constants() -> None:
    rows = [_row() for _ in range(5)]
    report = _crypto_dead_feature_report(rows)
    # union of the two buckets equals the full dead list
    assert set(report["data_density_dead_feature_names"]) | set(
        report["unexpected_dead_feature_names"]
    ) == set(report["dead_feature_names"])
    assert report["dead_feature_count"] == len(report["dead_feature_names"])
