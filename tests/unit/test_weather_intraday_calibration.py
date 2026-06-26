"""Small-sample calibration selection for the weather intraday model.

Deep-research finding (3-0 confirmed, Niculescu-Mizil & Caruana ICML'05): isotonic
regression overfits when calibration data is small (<~200-1000 cases); Platt (sigmoid)
scaling outperforms it there. The intraday model calibrates on per-series buckets as
small as 30-50 rows, so it should pick Platt below a configurable row threshold and
isotonic only with sufficient data.
"""

from __future__ import annotations

import math

import pytest

from kalshi_bot.weather.intraday import (
    calibrate_intraday_probability,
    fit_intraday_calibrator,
)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def test_calibrate_applies_platt_artifact() -> None:
    artifact = {"method": "platt", "coef": 2.0, "intercept": -1.0}
    # sigmoid(2*0.5 - 1) = sigmoid(0) = 0.5
    assert calibrate_intraday_probability(0.5, artifact) == pytest.approx(0.5, abs=1e-9)
    # monotone increasing in p
    low = calibrate_intraday_probability(0.2, artifact)
    high = calibrate_intraday_probability(0.8, artifact)
    assert low < high
    assert high == pytest.approx(_sigmoid(2.0 * 0.8 - 1.0), abs=1e-9)


def test_calibrate_clamps_unknown_method() -> None:
    assert calibrate_intraday_probability(1.5, {"method": "identity"}) == 1.0
    assert calibrate_intraday_probability(-0.3, {"method": "identity"}) == 0.0


def test_fit_uses_platt_below_threshold() -> None:
    # 40 calibration points (well under threshold) with a clear monotone signal.
    raw = [i / 40.0 for i in range(40)]
    outcomes = [0 if p < 0.5 else 1 for p in raw]
    artifact = fit_intraday_calibrator(raw, outcomes, isotonic_min_rows=1000)
    assert artifact["method"] == "platt"
    assert "coef" in artifact and "intercept" in artifact
    assert artifact["row_count"] == 40
    # Learned map must be monotone increasing (positive slope) given the signal.
    assert artifact["coef"] > 0.0
    assert calibrate_intraday_probability(0.1, artifact) < calibrate_intraday_probability(0.9, artifact)


def test_fit_uses_isotonic_at_or_above_threshold() -> None:
    raw = [i / 60.0 for i in range(60)]
    outcomes = [0 if p < 0.5 else 1 for p in raw]
    artifact = fit_intraday_calibrator(raw, outcomes, isotonic_min_rows=20)
    assert artifact["method"] == "isotonic"
    assert artifact["x_thresholds"] and artifact["y_thresholds"]
    assert len(artifact["x_thresholds"]) == len(artifact["y_thresholds"])
    assert artifact["row_count"] == 60


def test_fit_falls_back_to_identity_for_single_class() -> None:
    raw = [0.3, 0.4, 0.5, 0.6]
    outcomes = [0, 0, 0, 0]
    artifact = fit_intraday_calibrator(raw, outcomes, isotonic_min_rows=1000)
    assert artifact["method"] == "identity"
    # identity passthrough (clamped)
    assert calibrate_intraday_probability(0.42, artifact) == pytest.approx(0.42, abs=1e-9)


def test_fit_empty_is_identity() -> None:
    assert fit_intraday_calibrator([], [], isotonic_min_rows=1000)["method"] == "identity"
