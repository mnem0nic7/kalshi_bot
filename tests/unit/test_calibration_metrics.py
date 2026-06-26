"""CORP-style Brier score decomposition: S = MCB - DSC + UNC.

Deep-research (3-0, Dimitriadis/Gneiting/Jordan 2021, arXiv 2108.03210): the mean of a
proper score decomposes into nonnegative miscalibration (MCB), discrimination (DSC), and
uncertainty (UNC) components via PAV recalibration. Surfacing MCB/DSC in the gate separates
"badly calibrated" from "no discrimination" — Brier alone conflates them.
"""

from __future__ import annotations

import math

import pytest

from kalshi_bot.forecast.calibration_metrics import (
    brier_score_decomposition,
    pav_isotonic,
)


def _brier(p, y):
    return sum((pi - yi) ** 2 for pi, yi in zip(p, y)) / len(p)


def test_pav_is_monotone_nondecreasing_and_averages():
    # PAV on a non-monotone target must produce a non-decreasing fit.
    x = [0.1, 0.2, 0.3, 0.4, 0.5]
    y = [0.0, 1.0, 0.0, 1.0, 1.0]
    fit = pav_isotonic(x, y)
    assert all(fit[i] <= fit[i + 1] + 1e-12 for i in range(len(fit) - 1))
    # PAV preserves the overall mean.
    assert sum(fit) / len(fit) == pytest.approx(sum(y) / len(y), abs=1e-9)


def test_decomposition_identity_holds():
    # S = MCB - DSC + UNC must hold exactly for any forecasts/outcomes.
    forecasts = [0.1, 0.2, 0.25, 0.4, 0.55, 0.6, 0.7, 0.85, 0.9, 0.95]
    outcomes = [0, 0, 1, 0, 1, 0, 1, 1, 1, 1]
    d = brier_score_decomposition(forecasts, outcomes)
    assert d["brier"] == pytest.approx(_brier(forecasts, outcomes), abs=1e-12)
    assert d["mcb"] - d["dsc"] + d["unc"] == pytest.approx(d["brier"], abs=1e-9)
    # components are nonnegative
    assert d["mcb"] >= -1e-12 and d["dsc"] >= -1e-12 and d["unc"] >= -1e-12


def test_uncertainty_is_base_rate_variance():
    outcomes = [0, 0, 0, 1, 1, 1, 1, 1, 1, 1]  # base rate 0.7
    d = brier_score_decomposition([0.5] * 10, outcomes)
    assert d["unc"] == pytest.approx(0.7 * 0.3, abs=1e-9)


def test_perfectly_calibrated_has_zero_mcb():
    # Forecasts equal to the PAV recalibration of themselves => MCB ~ 0.
    # Two groups, empirical rates match the forecast values exactly.
    forecasts = [0.25, 0.25, 0.25, 0.25, 0.75, 0.75, 0.75, 0.75]
    outcomes = [0, 1, 0, 0, 1, 1, 1, 0]  # group rates: 0.25 and 0.75
    d = brier_score_decomposition(forecasts, outcomes)
    assert d["mcb"] == pytest.approx(0.0, abs=1e-9)


def test_no_discrimination_has_zero_dsc():
    # Constant forecast => recalibration is the base rate everywhere => DSC = 0.
    outcomes = [0, 1, 0, 1, 1, 0, 1, 0]
    d = brier_score_decomposition([0.5] * 8, outcomes)
    assert d["dsc"] == pytest.approx(0.0, abs=1e-9)


def test_empty_is_safe():
    d = brier_score_decomposition([], [])
    assert d["brier"] is None and d["mcb"] is None
