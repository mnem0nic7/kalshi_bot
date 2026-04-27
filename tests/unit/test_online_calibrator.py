from __future__ import annotations

import pytest

from kalshi_bot.forecast.online_calibrator import OnlineLogisticCalibrator, calibration_features


def test_online_calibrator_predicts_and_updates_deterministically() -> None:
    calibrator = OnlineLogisticCalibrator()
    features = calibration_features(
        p_catboost_yes=0.70,
        source_health_aggregate=0.95,
        days_since_last_settle=1.0,
        recent_brier_trailing=0.18,
    )

    before = calibrator.predict(features)
    calibrator.update(features, observed_yes=True)
    after = calibrator.predict(features)

    assert before == 0.5
    assert after > before
    assert calibrator.update_count == 1


def test_online_calibrator_round_trips_state() -> None:
    calibrator = OnlineLogisticCalibrator()
    features = calibration_features(
        p_catboost_yes=0.20,
        source_health_aggregate=0.80,
        days_since_last_settle=2.0,
        recent_brier_trailing=0.21,
    )
    calibrator.update(features, observed_yes=False)

    restored = OnlineLogisticCalibrator.from_dict(calibrator.to_dict())

    assert restored.to_dict() == calibrator.to_dict()
    assert restored.predict(features) == pytest.approx(calibrator.predict(features))


def test_online_calibrator_rejects_non_finite_features() -> None:
    calibrator = OnlineLogisticCalibrator()

    with pytest.raises(ValueError):
        calibrator.predict({"p_catboost_yes": float("nan")})
