"""Out-of-fold (cross-fitted) isotonic calibration for crypto tree candidates.

Single-holdout isotonic (fit booster on first 85%, calibrate on last 15%) leaves
tree models tail-overconfident: the calibrator never sees the in-sample rows the
booster memorized, so the candidate's reported ECE regresses vs market-mid and
the model-selection guardrail vetoes it — even when it is the most profitable
candidate (live evidence 2026-06-16: lightgbm +$98 OOS net, 203 trades, rejected
for ``ece_regressed_vs_market_mid``).

Out-of-fold calibration fits the isotonic map on predictions produced by boosters
that did NOT train on the row being predicted, over the FULL training set, so the
calibrator corrects the honest score->frequency relationship.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.crypto.services import _fit_crypto_oof_probability_calibration


def test_oof_calibration_covers_every_row_without_leakage() -> None:
    n = 60
    labels = [i % 2 for i in range(n)]
    seen_train_indices: list[set[int]] = []

    def fit_predict(train_idx: list[int], eval_idx: list[int]) -> list[float]:
        # No leakage: the rows we are scoring must never appear in training.
        assert not (set(train_idx) & set(eval_idx)), "OOF fold leaked eval rows into training"
        seen_train_indices.append(set(train_idx))
        # Graded monotone "model" (>=3 distinct values so isotonic can fit).
        return [0.15 + 0.6 * labels[i] + 0.01 * (i % 5) for i in eval_idx]

    calibration = _fit_crypto_oof_probability_calibration(
        labels,
        fit_predict=fit_predict,
        n_splits=3,
        min_rows=12,
    )

    assert calibration is not None
    assert calibration.get("method") == "isotonic"
    # Calibrator trained on OOF predictions for ALL rows, not just a 15% tail.
    assert calibration.get("sample_count") == n
    # One fit per fold.
    assert len(seen_train_indices) == 3


def _candidate_rows(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        label = 1 if (i * 7) % 10 < 5 else 0
        mid = 0.35 + (i % 7) * 0.03
        rows.append(
            {
                "asset_symbol": "BTC",
                "label_yes": label,
                "mid_yes_dollars": Decimal(str(round(mid, 4))),
                "spot_distance_dollars": Decimal(str(0.01 * (i % 9 - 4))),
                "spot_current_dollars": Decimal("100"),
                "contract_close_yes_dollars": Decimal(str(round(mid + 0.05, 4))),
                "time_to_close_seconds": 600 + (i % 50),
            }
        )
    return rows


@pytest.mark.parametrize("candidate", ["lightgbm_classifier", "xgboost_classifier"])
def test_tree_candidate_uses_out_of_fold_calibration_over_full_set(candidate, monkeypatch) -> None:
    from kalshi_bot.crypto.services import _fit_crypto_model_candidates

    monkeypatch.setenv("CRYPTO_CALIBRATION_OOF_SPLITS", "3")
    rows = _candidate_rows(2200)
    result = _fit_crypto_model_candidates(rows)[candidate]
    assert result["status"] == "available", result.get("reason")
    calibration = result["model"].get("probability_calibration")
    assert calibration is not None, "tree candidate must attach a calibration"
    assert calibration.get("scope") == "out_of_fold"
    # OOF calibrates over the whole training set, not just the 15% holdout.
    assert calibration.get("sample_count") == len(rows)


@pytest.mark.parametrize("candidate", ["lightgbm_classifier", "xgboost_classifier"])
def test_tree_candidate_oof_disabled_falls_back_to_single_holdout(candidate, monkeypatch) -> None:
    from kalshi_bot.crypto.services import _fit_crypto_model_candidates

    monkeypatch.setenv("CRYPTO_CALIBRATION_OOF_SPLITS", "0")  # disable OOF
    rows = _candidate_rows(2200)
    result = _fit_crypto_model_candidates(rows)[candidate]
    assert result["status"] == "available", result.get("reason")
    calibration = result["model"].get("probability_calibration")
    assert calibration is not None
    assert calibration.get("scope") != "out_of_fold"
    # Single-holdout calibrates only on the most-recent ~15%.
    assert calibration.get("sample_count") == len(rows) - int(len(rows) * 0.85)


def test_oof_calibration_returns_none_below_min_rows() -> None:
    labels = [i % 2 for i in range(10)]

    def fit_predict(train_idx: list[int], eval_idx: list[int]) -> list[float]:
        return [0.5 for _ in eval_idx]

    assert (
        _fit_crypto_oof_probability_calibration(labels, fit_predict=fit_predict, n_splits=3, min_rows=2000)
        is None
    )


def test_oof_calibration_lowers_ece_vs_single_holdout_under_regime_shift() -> None:
    """The model's raw score->frequency relationship differs across the series
    (a regime shift). Single-holdout calibration fits isotonic on only the last
    15% (one regime) and mis-maps the rest; OOF spans every fold, so its
    calibrator reflects the full distribution and has lower aggregate ECE."""
    from kalshi_bot.crypto.services import _apply_probability_calibration, _fit_probability_calibration

    n = 600
    split = int(n * 0.85)
    # Fixed raw score per row (5 graded levels), independent of which fold trains
    # — this isolates the *calibration-fitting set* effect from model variance.
    raw = [0.1 + 0.2 * (i % 5) for i in range(n)]
    labels: list[int] = []
    for i in range(n):
        if i < split:
            # Bulk regime: higher raw score -> higher hit rate.
            labels.append(1 if (i % 5) >= 2 else 0)
        else:
            # Tail regime: relationship collapses (mostly negatives).
            labels.append(1 if (i % 5) == 4 else 0)

    def fit_predict(train_idx: list[int], eval_idx: list[int]) -> list[float]:
        # No leakage by construction (fixed scores), model independent of fold.
        assert not (set(train_idx) & set(eval_idx))
        return [raw[i] for i in eval_idx]

    oof_cal = _fit_crypto_oof_probability_calibration(labels, fit_predict=fit_predict, n_splits=5, min_rows=12)
    assert oof_cal is not None

    # Single-holdout: isotonic fit on the last 15% only (the tail regime).
    holdout_cal = _fit_probability_calibration(
        [Decimal(str(raw[i])) for i in range(split, n)], labels[split:]
    )
    assert holdout_cal is not None

    def ece(cal: dict) -> float:
        calibrated = [float(_apply_probability_calibration(Decimal(str(p)), cal)) for p in raw]
        bins = 10
        total = 0.0
        for b in range(bins):
            lo, hi = b / bins, (b + 1) / bins
            members = [
                (c, labels[i])
                for i, c in enumerate(calibrated)
                if (lo <= c < hi or (b == bins - 1 and c >= hi))
            ]
            if not members:
                continue
            avg_conf = sum(c for c, _ in members) / len(members)
            avg_acc = sum(l for _, l in members) / len(members)
            total += (len(members) / n) * abs(avg_conf - avg_acc)
        return total

    assert ece(oof_cal) < ece(holdout_cal), (
        f"OOF ECE {ece(oof_cal):.4f} should beat single-holdout {ece(holdout_cal):.4f}"
    )
