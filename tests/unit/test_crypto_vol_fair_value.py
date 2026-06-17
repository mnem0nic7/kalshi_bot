"""Analytic vol-based fair-value candidate model.

From docs/research/kalshi_15m_market_making_plan.md (sec 2 + 4.2): over a short
horizon, a 15m crypto binary's fair probability is the mechanism-based
    fair_up = Phi( ln(S/K) / (sigma * sqrt(tau)) )
where S=spot, K=strike, sigma=per-step realized vol, tau=remaining horizon.

The system already logs the ingredients (spot_moneyness_pct ~ ln(S/K),
spot_realized_volatility[_32] = sigma, time_to_close_seconds = tau) but never
formed the analytic probability. This adds a `vol_normal_fair_value` candidate so
a calibrated analytic fair value competes in champion selection against the
learned heads (whose live edge is largely phantom, beta=0.125).
"""
from __future__ import annotations

import math
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CRYPTO_MODEL_CANDIDATE_NAMES,
    _fit_crypto_model_candidates,
    _predict_crypto_probability,
)


def _row(*, moneyness_pct, sigma, tau, label_yes=1, **overrides) -> dict[str, object]:
    values: dict[str, object] = {
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "label_yes": label_yes,
        "spot_moneyness_pct": Decimal(str(moneyness_pct)) if moneyness_pct is not None else None,
        "spot_realized_volatility_32": Decimal(str(sigma)) if sigma is not None else None,
        "spot_realized_volatility": Decimal(str(sigma)) if sigma is not None else None,
        "time_to_close_seconds": tau,
    }
    values.update(overrides)
    return values


def _model(**overrides) -> dict[str, object]:
    model: dict[str, object] = {
        "model_type": "vol_normal_fair_value",
        "step_interval_seconds": 60,
        "fallback_model": {"model_type": "market_mid_baseline"},
    }
    model.update(overrides)
    return model


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


# --- analytic probability ------------------------------------------------------


def test_fair_value_is_half_at_the_money() -> None:
    # S == K -> z == 0 -> Phi(0) == 0.5, regardless of sigma/tau.
    prob = _predict_crypto_probability(
        _row(moneyness_pct=0, sigma=0.001, tau=300), _model(), apply_calibration=False
    )
    assert abs(float(prob) - 0.5) < 1e-6


def test_fair_value_matches_normal_cdf() -> None:
    # moneyness/sigma = 1.0; tau == step -> z = 1.0 -> Phi(1) ~ 0.8413.
    prob = _predict_crypto_probability(
        _row(moneyness_pct=0.001, sigma=0.001, tau=60), _model(step_interval_seconds=60), apply_calibration=False
    )
    assert abs(float(prob) - _phi(1.0)) < 1e-4


def test_fair_value_sharpens_toward_terminal() -> None:
    # Same in-the-money distance, less time left -> z grows -> fair closer to 1.
    far = _predict_crypto_probability(
        _row(moneyness_pct=0.001, sigma=0.001, tau=240), _model(), apply_calibration=False
    )
    near = _predict_crypto_probability(
        _row(moneyness_pct=0.001, sigma=0.001, tau=15), _model(), apply_calibration=False
    )
    assert float(near) > float(far) > 0.5


def test_fair_value_monotonic_in_moneyness() -> None:
    lo = _predict_crypto_probability(_row(moneyness_pct=0.0005, sigma=0.001, tau=60), _model(), apply_calibration=False)
    hi = _predict_crypto_probability(_row(moneyness_pct=0.0020, sigma=0.001, tau=60), _model(), apply_calibration=False)
    assert float(hi) > float(lo) > 0.5


def test_fair_value_z_clamp_desaturates_tail() -> None:
    # Deep in-the-money, tiny vol, near close → raw z explodes → Phi saturates to
    # the price ceiling. max_abs_z caps z so the tail stays calibratable.
    saturating = _row(moneyness_pct=0.02, sigma=0.0005, tau=10)
    uncapped = _predict_crypto_probability(saturating, _model(), apply_calibration=False)
    capped = _predict_crypto_probability(saturating, _model(max_abs_z=2.0), apply_calibration=False)
    # The cap pulls the saturated prediction back, to exactly Phi(cap).
    assert float(capped) < float(uncapped)
    assert float(capped) == pytest.approx(_phi(2.0), abs=1e-4)


def test_fair_value_z_clamp_preserves_unsaturated_values() -> None:
    # Within the cap, predictions are unchanged.
    row = _row(moneyness_pct=0.001, sigma=0.001, tau=60)  # z = 1.0
    capped = _predict_crypto_probability(row, _model(max_abs_z=2.5), apply_calibration=False)
    assert abs(float(capped) - _phi(1.0)) < 1e-4


def test_fair_value_falls_back_when_inputs_missing() -> None:
    # No vol -> cannot form z -> fall back to market mid (0.50).
    prob = _predict_crypto_probability(
        _row(moneyness_pct=0.001, sigma=None, tau=60), _model(), apply_calibration=False
    )
    assert abs(float(prob) - 0.5) < 1e-6


# --- registration in the champion pool ----------------------------------------


def test_vol_fair_value_registered_as_candidate(tmp_path) -> None:
    assert "vol_normal_fair_value" in CRYPTO_MODEL_CANDIDATE_NAMES
    rows = [
        _row(moneyness_pct=0.002, sigma=0.001, tau=120, label_yes=1),
        _row(moneyness_pct=-0.002, sigma=0.001, tau=120, label_yes=0),
        _row(moneyness_pct=0.001, sigma=0.001, tau=200, label_yes=1),
        _row(moneyness_pct=-0.001, sigma=0.001, tau=200, label_yes=0),
    ]
    candidates = _fit_crypto_model_candidates(rows)
    assert candidates["vol_normal_fair_value"]["status"] == "available"
    assert candidates["vol_normal_fair_value"]["model"]["model_type"] == "vol_normal_fair_value"
