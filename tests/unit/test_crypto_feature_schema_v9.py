"""Schema v9: structural dead-feature fixes.

- spread_change_bps_1 was dead everywhere: market snapshots carry no spread_bps
  column and the prior-quote call sites never extracted bid/ask, so the prior
  spread was always None. _crypto_snapshot_spread_bps now self-derives.
- Single-asset training corpora pruned the lone asset one-hot (constant 1.0)
  and the self cross-asset return columns (constant default, excluded by the
  train/live skew guard).
- time_to_close_bucket_15m_plus was constant-zero for both frequencies (15m
  markets never reach it; 1h bucketing uses different names) — removed.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from kalshi_bot.crypto.services import (
    _crypto_feature_schema,
    _crypto_quote_sequence_context,
    _crypto_raw_feature_vector,
    _crypto_snapshot_spread_bps,
)


def _row(asset: str) -> dict:
    return {"asset_symbol": asset, "frequency": "15m"}


def _snapshot(yes_bid: str | None, yes_ask: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        yes_bid_dollars=Decimal(yes_bid) if yes_bid is not None else None,
        yes_ask_dollars=Decimal(yes_ask) if yes_ask is not None else None,
        payload=None,
        raw=None,
    )


def test_snapshot_spread_self_derives_from_quotes() -> None:
    spread = _crypto_snapshot_spread_bps(_snapshot("0.40", "0.44"), None, None)
    assert spread is not None and abs(spread - 400.0) < 1e-6


def test_snapshot_spread_none_without_quotes() -> None:
    assert _crypto_snapshot_spread_bps(_snapshot(None, None), None, None) is None


def test_quote_sequence_spread_change_now_populates() -> None:
    now = datetime.now(UTC)
    prior = now - timedelta(seconds=60)
    context = _crypto_quote_sequence_context(
        Decimal("0.52"),
        _crypto_snapshot_spread_bps(_snapshot("0.50", "0.54"), None, None),
        now,
        Decimal("0.50"),
        _crypto_snapshot_spread_bps(_snapshot("0.49", "0.51"), None, None),
        prior,
    )
    assert abs(context["spread_change_bps_1"] - 200.0) < 1e-6


def test_single_asset_schema_prunes_structural_constants() -> None:
    schema = _crypto_feature_schema([_row("BNB")])

    assert "asset=BNB" not in schema["feature_names"]
    assert "bnb_return_1_pct" not in schema["numeric_feature_names"]
    assert "bnb_return_3_pct" not in schema["numeric_feature_names"]
    assert "time_to_close_bucket_15m_plus" not in schema["numeric_feature_names"]
    # Other assets' cross returns stay.
    assert "btc_return_1_pct" in schema["numeric_feature_names"]
    assert schema["asset_categories"] == ["BNB"]


def test_pooled_schema_keeps_onehots_and_cross_returns() -> None:
    schema = _crypto_feature_schema([_row("BNB"), _row("BTC")])

    assert "asset=BNB" in schema["feature_names"]
    assert "asset=BTC" in schema["feature_names"]
    assert "bnb_return_1_pct" in schema["numeric_feature_names"]
    assert "btc_return_3_pct" in schema["numeric_feature_names"]
    assert "time_to_close_bucket_15m_plus" not in schema["numeric_feature_names"]


def test_vector_length_matches_feature_names_single_and_pooled() -> None:
    for rows in ([_row("BNB")], [_row("BNB"), _row("BTC")]):
        schema = _crypto_feature_schema(rows)
        vector = _crypto_raw_feature_vector(rows[0], schema)
        assert len(vector) == len(schema["feature_names"])


def test_vector_backward_compatible_with_pre_v9_artifact_schema() -> None:
    """Artifacts trained before the pruning still list the lone one-hot and the
    15m_plus bucket; their stored schema must keep producing matching vectors."""
    old_numeric = [
        "market_mid_logit",
        "time_to_close_bucket_15m_plus",
        "bnb_return_1_pct",
    ]
    old_schema = {
        "feature_names": [*old_numeric, "asset=BNB"],
        "numeric_feature_names": old_numeric,
        "asset_categories": ["BNB"],
    }
    vector = _crypto_raw_feature_vector(_row("BNB"), old_schema)
    assert len(vector) == len(old_schema["feature_names"])
    assert vector[-1] == 1.0  # the lone one-hot still emitted for old artifacts
