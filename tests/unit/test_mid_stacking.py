"""Mid-anchored stacking for crypto probabilities.

Measured: our crypto model's Brier is WORSE than the market mid on every asset (+1..+76%),
because the model predicts from spot features and never sees the mid. Stacking blends the model
with the mid (logistic of label on [logit(model), logit(mid)], fit on holdout). By construction
the optimizer can recover "just the mid" (model coef -> 0), so the stack is ~never worse than the
mid, and it up-weights the model only where it adds genuine signal. With no fitted stack we fall
back to the mid (the better forecast), guaranteeing never-worse behavior.
"""

from __future__ import annotations

import math
import random

import pytest

from kalshi_bot.crypto.mid_stacking import apply_mid_stack, fit_mid_stack


def _brier(ps, ys):
    return sum((p - y) ** 2 for p, y in zip(ps, ys)) / len(ps)


def test_apply_none_stack_falls_back_to_mid():
    # No fitted stack -> use the mid (the better forecast), never the raw model.
    assert apply_mid_stack(0.9, 0.4, None) == pytest.approx(0.4, abs=1e-9)
    assert apply_mid_stack(0.1, 0.62, {}) == pytest.approx(0.62, abs=1e-9)


def test_apply_is_bounded():
    stack = {"method": "mid_stack", "c0": 0.0, "c_model": 50.0, "c_mid": 50.0}
    v = apply_mid_stack(0.999999, 0.999999, stack)
    assert 0.0 <= v <= 1.0


def test_fit_noise_model_recovers_the_mid():
    rng = random.Random(0)
    # mid is well-calibrated and informative; model is pure noise (uncorrelated with label).
    mids, models, labels = [], [], []
    for _ in range(2000):
        mid = rng.uniform(0.1, 0.9)
        y = 1 if rng.random() < mid else 0
        mids.append(mid)
        models.append(rng.uniform(0.05, 0.95))  # noise
        labels.append(y)
    stack = fit_mid_stack(models, mids, labels, min_samples=200)
    assert stack is not None
    # Stack should track the mid (noise model contributes ~nothing).
    stacked = [apply_mid_stack(m, md, stack) for m, md in zip(models, mids)]
    b_stack = _brier(stacked, labels)
    b_mid = _brier(mids, labels)
    b_model = _brier(models, labels)
    assert b_model > b_mid  # model really is worse (sanity)
    assert b_stack <= b_mid + 0.01  # stack is not meaningfully worse than the mid


def test_fit_signal_model_helps_when_real():
    rng = random.Random(1)
    # Label depends on BOTH mid and an independent model signal -> stack should beat mid alone.
    mids, models, labels = [], [], []
    for _ in range(3000):
        mid = rng.uniform(0.2, 0.8)
        signal = rng.uniform(0.0, 1.0)
        true_p = max(0.02, min(0.98, 0.5 * mid + 0.5 * signal))
        y = 1 if rng.random() < true_p else 0
        mids.append(mid)
        models.append(signal)  # model carries the extra signal
        labels.append(y)
    stack = fit_mid_stack(models, mids, labels, min_samples=200)
    stacked = [apply_mid_stack(m, md, stack) for m, md in zip(models, mids)]
    assert _brier(stacked, labels) < _brier(mids, labels) - 1e-3  # genuinely better


def test_fit_returns_none_below_min_samples_or_single_class():
    assert fit_mid_stack([0.5, 0.6], [0.4, 0.5], [1, 0], min_samples=200) is None
    assert fit_mid_stack([0.5] * 300, [0.4] * 300, [1] * 300, min_samples=200) is None
