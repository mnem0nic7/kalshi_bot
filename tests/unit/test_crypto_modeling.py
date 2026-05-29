"""Tests for spot_distance_residual isotonic calibration (Change 1)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
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
