"""Mid-anchored convex blend for crypto probabilities.

Our crypto model's Brier is worse than the market mid OOS (walk-forward), because it predicts from
spot features and never sees the mid. The first design (a free 2-feature logistic of label on
[logit(model), logit(mid)]) overfit on collinear inputs (negative mid coefficient) and did NOT
generalize OOS. This robust version fits a single convex weight w in [0,1] for p = w*model +
(1-w)*mid, accepting w>0 ONLY if it beats the mid on a held-out tail (else w=0 = pure mid). That
makes "never worse than the mid" enforced by the fit's own holdout gate, and a convex combination
can never blow up the way the free logistic did. With no usable blend we fall back to the mid.
"""

from __future__ import annotations

import random

import pytest

from kalshi_bot.crypto.mid_stacking import apply_mid_stack, fit_mid_stack


def _brier(ps, ys):
    return sum((p - y) ** 2 for p, y in zip(ps, ys)) / len(ps)


def test_apply_none_blend_falls_back_to_mid():
    assert apply_mid_stack(0.9, 0.4, None) == pytest.approx(0.4, abs=1e-9)
    assert apply_mid_stack(0.1, 0.62, {}) == pytest.approx(0.62, abs=1e-9)


def test_apply_is_convex_combination():
    blend = {"method": "mid_blend", "w": 0.25}
    # 0.25*model + 0.75*mid
    assert apply_mid_stack(1.0, 0.0, blend) == pytest.approx(0.25, abs=1e-9)
    assert apply_mid_stack(0.8, 0.4, blend) == pytest.approx(0.25 * 0.8 + 0.75 * 0.4, abs=1e-9)


def test_apply_is_bounded_between_inputs():
    blend = {"method": "mid_blend", "w": 0.5}
    v = apply_mid_stack(0.9, 0.3, blend)
    assert 0.3 <= v <= 0.9


def test_apply_ignores_legacy_logistic_method():
    # Old "mid_stack" logistic artifacts are abandoned -> fall back to the mid (never-worse).
    legacy = {"method": "mid_stack", "c0": 0.0, "c_model": 1.5, "c_mid": -0.4}
    assert apply_mid_stack(0.9, 0.42, legacy) == pytest.approx(0.42, abs=1e-9)


def test_fit_noise_model_gives_zero_weight():
    rng = random.Random(0)
    mids, models, labels = [], [], []
    for _ in range(2000):
        mid = rng.uniform(0.1, 0.9)
        labels.append(1 if rng.random() < mid else 0)
        mids.append(mid)
        models.append(rng.uniform(0.05, 0.95))  # noise, uncorrelated with label
    blend = fit_mid_stack(models, mids, labels, min_samples=200)
    assert blend is not None
    assert blend["w"] == pytest.approx(0.0, abs=1e-9)  # model doesn't beat mid on holdout
    stacked = [apply_mid_stack(m, md, blend) for m, md in zip(models, mids)]
    assert _brier(stacked, labels) <= _brier(mids, labels) + 1e-9  # never worse than mid


def test_fit_signal_model_gets_positive_weight_and_beats_mid():
    rng = random.Random(1)
    mids, models, labels = [], [], []
    for _ in range(4000):
        mid = rng.uniform(0.2, 0.8)
        signal = rng.uniform(0.0, 1.0)
        true_p = max(0.02, min(0.98, 0.5 * mid + 0.5 * signal))
        labels.append(1 if rng.random() < true_p else 0)
        mids.append(mid)
        models.append(signal)
    blend = fit_mid_stack(models, mids, labels, min_samples=200)
    assert blend["w"] > 0.0
    stacked = [apply_mid_stack(m, md, blend) for m, md in zip(models, mids)]
    assert _brier(stacked, labels) < _brier(mids, labels) - 1e-3


def test_fit_worse_model_gives_zero_weight():
    # Model is systematically MISCALIBRATED (worse than mid) -> w must be 0 (don't blend it in).
    rng = random.Random(2)
    mids, models, labels = [], [], []
    for _ in range(2000):
        mid = rng.uniform(0.2, 0.8)
        labels.append(1 if rng.random() < mid else 0)
        mids.append(mid)
        models.append(max(0.01, min(0.99, 1.0 - mid)))  # inverted -> harmful
    blend = fit_mid_stack(models, mids, labels, min_samples=200)
    assert blend["w"] == pytest.approx(0.0, abs=1e-9)


def test_fit_returns_none_below_min_samples_or_single_class():
    assert fit_mid_stack([0.5, 0.6], [0.4, 0.5], [1, 0], min_samples=200) is None
    assert fit_mid_stack([0.5] * 300, [0.4] * 300, [1] * 300, min_samples=200) is None
