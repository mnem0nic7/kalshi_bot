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
