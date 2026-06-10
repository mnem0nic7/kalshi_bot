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
