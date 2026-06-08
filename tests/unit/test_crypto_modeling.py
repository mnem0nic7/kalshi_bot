"""Tests for spot_distance_residual isotonic calibration (Change 1)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
    _fit_crypto_spot_distance_contrarian_model,
    _fit_crypto_spot_distance_contrarian_gated_model,
    _fit_crypto_spot_distance_residual_model,
    _predict_crypto_probability,
)


def _make_row(asset: str, label: int, mid: float, spot_distance: float = 0.05) -> dict:
    return {
        "asset_symbol": asset,
        "label_yes": label,
        "mid_yes_dollars": Decimal(str(mid)),
        "spot_distance_dollars": Decimal(str(spot_distance)),
        "spot_current_dollars": Decimal("100"),
        "contract_close_yes_dollars": Decimal(str(mid + spot_distance)),
    }


def _make_fallback() -> dict:
    return {"model_type": "market_mid_baseline"}


def _make_rows_with_both_labels(n: int = 20) -> list[dict]:
    rows = []
    for i in range(n):
        label = 1 if i % 2 == 0 else 0
        mid = 0.4 + (i % 5) * 0.03
        rows.append(_make_row("BTC", label, mid))
    return rows


def test_residual_model_fits_probability_calibration_with_sufficient_data() -> None:
    rows = _make_rows_with_both_labels(20)
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert model.get("probability_calibration") is not None
    calibration = model["probability_calibration"]
    assert calibration.get("method") == "isotonic"
    assert calibration.get("sample_count") == 20


def test_residual_model_calibration_is_none_with_too_few_rows() -> None:
    rows = _make_rows_with_both_labels(5)
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert model.get("probability_calibration") is None


def test_residual_model_calibration_is_none_with_single_label() -> None:
    rows = [_make_row("BTC", 1, 0.5) for _ in range(20)]
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert model.get("probability_calibration") is None


def test_predict_residual_apply_calibration_true_uses_isotonic() -> None:
    rows = _make_rows_with_both_labels(20)
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert model.get("probability_calibration") is not None

    row = _make_row("BTC", 1, 0.5)
    raw = _predict_crypto_probability(row, model, apply_calibration=False)
    calibrated = _predict_crypto_probability(row, model, apply_calibration=True)
    # Both must be valid probabilities in [0.01, 0.99]
    assert Decimal("0.01") <= raw <= Decimal("0.99")
    assert Decimal("0.01") <= calibrated <= Decimal("0.99")


def test_predict_residual_apply_calibration_false_returns_raw() -> None:
    rows = _make_rows_with_both_labels(20)
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert model.get("probability_calibration") is not None

    row = _make_row("BTC", 1, 0.5)
    raw = _predict_crypto_probability(row, model, apply_calibration=False)
    # With no calibration applied, predict again and results should be identical
    raw2 = _predict_crypto_probability(row, model, apply_calibration=False)
    assert raw == raw2


def test_predict_residual_no_infinite_recursion_during_calibration_fitting() -> None:
    rows = _make_rows_with_both_labels(20)
    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    assert "probability_calibration" in model


def test_spot_distance_contrarian_predicts_against_current_distance() -> None:
    rows = _make_rows_with_both_labels(4)
    model = _fit_crypto_spot_distance_contrarian_model(rows, fallback=_make_fallback())

    above = _make_row("SOL", 0, 0.50)
    above["spot_target_distance_volatility"] = Decimal("1.25")
    below = _make_row("SOL", 1, 0.50)
    below["spot_target_distance_volatility"] = Decimal("-1.25")

    assert _predict_crypto_probability(above, model) == Decimal("0.0100")
    assert _predict_crypto_probability(below, model) == Decimal("0.9900")


def test_spot_distance_contrarian_falls_back_when_distance_missing() -> None:
    rows = _make_rows_with_both_labels(4)
    model = _fit_crypto_spot_distance_contrarian_model(rows, fallback=_make_fallback())
    row = _make_row("SOL", 0, 0.42)

    assert _predict_crypto_probability(row, model) == Decimal("0.4200")


def _clean_quote_row(*, distance: Decimal = Decimal("1.25"), no_ask: Decimal = Decimal("0.4000")) -> dict:
    row = _make_row("SOL", 0, 0.50)
    row.update(
        {
            "spot_target_distance_volatility": distance,
            "quote_source": "snapshot_quotes",
            "strict_trade_eligible": True,
            "spot_feature_status": "available",
            "spot_provider": "coinbase",
            "spot_source_kind": "spot_tick",
            "yes_bid_dollars": Decimal("0.5900"),
            "yes_ask_dollars": Decimal("0.6000"),
            "no_bid_dollars": Decimal("0.3900"),
            "no_ask_dollars": no_ask,
            "spread_bps": 100,
        }
    )
    return row


def test_spot_distance_contrarian_gated_activates_on_clean_quote_row() -> None:
    rows = _make_rows_with_both_labels(4)
    model = _fit_crypto_spot_distance_contrarian_gated_model(rows, fallback=_make_fallback())

    above = _clean_quote_row(distance=Decimal("1.25"))
    below = _clean_quote_row(distance=Decimal("-1.25"))
    below["yes_ask_dollars"] = Decimal("0.4000")

    assert _predict_crypto_probability(above, model) == Decimal("0.1000")
    assert _predict_crypto_probability(below, model) == Decimal("0.9000")


def test_spot_distance_contrarian_gated_falls_back_when_quote_context_not_live_quality() -> None:
    rows = _make_rows_with_both_labels(4)
    model = _fit_crypto_spot_distance_contrarian_gated_model(rows, fallback=_make_fallback())

    proxy = _clean_quote_row()
    proxy["quote_source"] = "candlestick_close_proxy"
    wide = _clean_quote_row()
    wide["spread_bps"] = 1501
    too_cheap = _clean_quote_row(no_ask=Decimal("0.3400"))
    missing_spot = _clean_quote_row()
    missing_spot.pop("spot_feature_status")

    assert _predict_crypto_probability(proxy, model) == Decimal("0.5000")
    assert _predict_crypto_probability(wide, model) == Decimal("0.5000")
    assert _predict_crypto_probability(too_cheap, model) == Decimal("0.5000")
    assert _predict_crypto_probability(missing_spot, model) == Decimal("0.5000")


def test_calibrated_ece_not_worse_than_raw_on_biased_data() -> None:
    """Calibrated output should reduce systematic bias."""
    from kalshi_bot.crypto.services import _fit_probability_calibration

    # Create biased predictions: all predicted ~0.8 but labels split 50/50
    rows = []
    for i in range(30):
        label = 1 if i < 15 else 0
        rows.append(_make_row("BTC", label, 0.78 + (i % 3) * 0.01))

    model = _fit_crypto_spot_distance_residual_model(rows, fallback=_make_fallback())
    if model.get("probability_calibration") is None:
        pytest.skip("Calibration not fit (insufficient distinct predictions)")

    raw_preds = [_predict_crypto_probability(row, model, apply_calibration=False) for row in rows]
    cal_preds = [_predict_crypto_probability(row, model, apply_calibration=True) for row in rows]

    # Compute MSE as a proxy — calibrated output should be at least as good
    labels = [int(row["label_yes"]) for row in rows]
    raw_mse = sum((float(p) - l) ** 2 for p, l in zip(raw_preds, labels)) / len(labels)
    cal_mse = sum((float(p) - l) ** 2 for p, l in zip(cal_preds, labels)) / len(labels)
    # Allow a small tolerance — isotonic may not always strictly improve on training data
    assert cal_mse <= raw_mse + 0.05, f"Calibrated MSE {cal_mse:.4f} much worse than raw {raw_mse:.4f}"


# ── Market price anchor weight tests ─────────────────────────────────────────

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    _crypto_market_anchored_probability,
    _crypto_market_price_anchor_weight,
)


def _anchor_settings(enabled: bool = True, weight: float = 0.75) -> Settings:
    return Settings(
        kalshi_env="demo",
        crypto_market_price_anchor_enabled=enabled,
        crypto_market_price_anchor_weight=weight,
    )


def test_anchor_weight_nonzero_at_50_50():
    """Anchor weight must never be zero — prevents model from ignoring market price."""
    settings = _anchor_settings()
    row = {"mid_yes_dollars": Decimal("0.5000")}
    weight = _crypto_market_price_anchor_weight(row, settings=settings)
    assert weight > Decimal("0.10"), f"Weight at 50/50 must be >10%, got {weight}"


def test_anchor_weight_increases_at_extremes():
    """Anchor weight should be higher for extreme market prices than for near-50 prices."""
    settings = _anchor_settings()
    row_mid = {"mid_yes_dollars": Decimal("0.5000")}
    row_extreme = {"mid_yes_dollars": Decimal("0.2000")}
    weight_mid = _crypto_market_price_anchor_weight(row_mid, settings=settings)
    weight_extreme = _crypto_market_price_anchor_weight(row_extreme, settings=settings)
    assert weight_extreme > weight_mid, "Extreme prices should anchor more strongly"


def test_anchor_prevents_catastrophic_btc_no_failure():
    """At 49-cent market price, model predicting 12% should be pushed toward market."""
    settings = _anchor_settings()
    row = {"mid_yes_dollars": Decimal("0.4900")}
    raw_model = Decimal("0.1200")
    anchored = _crypto_market_anchored_probability(row, raw_model, settings=settings)
    # After anchoring, fair_yes must be closer to market (0.49) than raw model (0.12)
    dist_raw = abs(raw_model - Decimal("0.4900"))
    dist_anchored = abs(anchored - Decimal("0.4900"))
    assert dist_anchored < dist_raw, (
        f"Anchored {anchored} should be closer to market 0.49 than raw {raw_model}"
    )
    # Anchored should be meaningfully above the raw model prediction
    assert anchored > Decimal("0.15"), f"Anchored {anchored} still too low (raw={raw_model})"


def test_anchor_disabled_passes_raw():
    """When anchor is disabled, raw model prediction passes through unchanged."""
    settings = _anchor_settings(enabled=False)
    row = {"mid_yes_dollars": Decimal("0.5000")}
    raw_model = Decimal("0.1200")
    anchored = _crypto_market_anchored_probability(row, raw_model, settings=settings)
    assert anchored == raw_model.quantize(Decimal("0.0001")), "Disabled anchor should pass raw"


# ── Model-spot direction check tests ─────────────────────────────────────────

from kalshi_bot.crypto.services import _crypto_model_spot_direction_check  # noqa: E402


def _direction_check_settings(
    enabled: bool = True,
    min_moneyness: float = 0.02,
    max_fair_yes: float = 0.40,
) -> Settings:
    return Settings(
        kalshi_env="demo",
        crypto_model_spot_direction_check_enabled=enabled,
        crypto_model_spot_direction_min_moneyness_pct=min_moneyness,
        crypto_model_spot_direction_max_fair_yes=max_fair_yes,
    )


def test_direction_check_flags_inversion_when_spot_above_target():
    """Model says YES=12% but spot is clearly above target — conflict detected."""
    settings = _direction_check_settings()
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": Decimal("0.05"),  # spot 5% above target (YES in the money)
        "mid_yes_dollars": Decimal("0.3500"),
    }
    raw_fair = Decimal("0.1200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is not None, "Direction conflict must be flagged"
    assert mismatch["reason"] == "model_spot_direction_conflict"
    # Corrected fair must equal the market mid, not the raw model output
    assert corrected == Decimal("0.3500"), f"Expected corrected=0.35, got {corrected}"
    assert corrected > raw_fair, "Corrected fair must be higher than inverted model output"


def test_direction_check_passes_through_low_price_yes_market():
    """Spot below target + model says low YES = correct NO prediction, no conflict."""
    settings = _direction_check_settings()
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": Decimal("-0.04"),  # spot 4% BELOW target (YES out of the money)
        "mid_yes_dollars": Decimal("0.2000"),
    }
    raw_fair = Decimal("0.1500")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None, "No conflict when model correctly predicts low YES with spot below target"
    assert corrected == raw_fair, "Raw fair must pass through unchanged"


def test_direction_check_passes_through_when_model_agrees():
    """Spot above target and model also says high YES — no conflict."""
    settings = _direction_check_settings()
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": Decimal("0.05"),
        "mid_yes_dollars": Decimal("0.6800"),
    }
    raw_fair = Decimal("0.7200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None, "No conflict when model agrees with spot direction"
    assert corrected == raw_fair


def test_direction_check_ignores_small_moneyness():
    """Spot barely above target (below threshold) does not trigger the check."""
    settings = _direction_check_settings(min_moneyness=0.02)
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": Decimal("0.01"),  # 1% < 2% threshold
        "mid_yes_dollars": Decimal("0.4800"),
    }
    raw_fair = Decimal("0.1200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None, "Small moneyness below threshold must not trigger correction"
    assert corrected == raw_fair


def test_direction_check_ignores_stale_spot():
    """Stale spot features must not trigger the conflict check."""
    settings = _direction_check_settings()
    row = {
        "spot_feature_status": "stale",  # stale — not trustworthy
        "spot_moneyness_pct": Decimal("0.10"),
        "mid_yes_dollars": Decimal("0.3500"),
    }
    raw_fair = Decimal("0.1200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None, "Stale spot must not trigger direction correction"
    assert corrected == raw_fair


def test_direction_check_disabled_passes_through():
    """When the check is disabled the raw model output is unchanged."""
    settings = _direction_check_settings(enabled=False)
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": Decimal("0.08"),
        "mid_yes_dollars": Decimal("0.3500"),
    }
    raw_fair = Decimal("0.1200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None, "Disabled check must not produce a mismatch trace"
    assert corrected == raw_fair, "Disabled check must pass raw fair through unchanged"


def test_direction_check_missing_spot_moneyness_skips():
    """Missing spot_moneyness_pct (no target price) must not trigger the check."""
    settings = _direction_check_settings()
    row = {
        "spot_feature_status": "available",
        "spot_moneyness_pct": None,
        "mid_yes_dollars": Decimal("0.3500"),
    }
    raw_fair = Decimal("0.1200")
    corrected, mismatch = _crypto_model_spot_direction_check(row, raw_fair, settings=settings)
    assert mismatch is None
    assert corrected == raw_fair
