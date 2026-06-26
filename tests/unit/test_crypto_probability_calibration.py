"""Small-sample Platt calibration for crypto champion probability calibration.

Carryover of the weather finding (Niculescu-Mizil & Caruana, ICML'05; deep-research
3-0 confirmed): isotonic overfits on scarce calibration data (<~200-1000 cases) where
Platt/sigmoid wins. Crypto per-asset single-holdout calibration routinely runs on a few
hundred rows (OOF only attempts at >=2000), so it should pick Platt below a row threshold.
"""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import (
    _apply_probability_calibration,
    _fit_probability_calibration,
)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def test_apply_platt_calibration() -> None:
    cal = {"method": "platt", "coef": 2.0, "intercept": -1.0}
    # sigmoid(2*0.5 - 1) = 0.5
    assert float(_apply_probability_calibration(Decimal("0.5"), cal)) == pytest.approx(0.5, abs=1e-3)
    low = _apply_probability_calibration(Decimal("0.2"), cal)
    high = _apply_probability_calibration(Decimal("0.8"), cal)
    assert low < high
    assert float(high) == pytest.approx(_sigmoid(2.0 * 0.8 - 1.0), abs=1e-3)


def test_fit_selects_platt_below_threshold() -> None:
    preds = [Decimal(str(i / 40.0)) for i in range(40)]
    labels = [0 if i < 20 else 1 for i in range(40)]
    cal = _fit_probability_calibration(preds, labels, isotonic_min_rows=1000)
    assert cal is not None
    assert cal["method"] == "platt"
    assert "coef" in cal and "intercept" in cal
    assert cal["sample_count"] == 40
    # monotone increasing learned map
    assert _apply_probability_calibration(Decimal("0.1"), cal) < _apply_probability_calibration(
        Decimal("0.9"), cal
    )


def test_fit_selects_isotonic_at_or_above_threshold() -> None:
    preds = [Decimal(str(i / 60.0)) for i in range(60)]
    labels = [0 if i < 30 else 1 for i in range(60)]
    cal = _fit_probability_calibration(preds, labels, isotonic_min_rows=20)
    assert cal is not None
    assert cal["method"] == "isotonic"
    assert cal["thresholds_x"] and cal["thresholds_y"]


def test_fit_too_few_rows_returns_none() -> None:
    preds = [Decimal(str(i / 8.0)) for i in range(8)]
    labels = [0, 0, 0, 0, 1, 1, 1, 1]
    assert _fit_probability_calibration(preds, labels, isotonic_min_rows=1000) is None
