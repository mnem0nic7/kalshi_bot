from __future__ import annotations

from dataclasses import dataclass, field
from math import exp, isfinite
from typing import Any, Mapping


DEFAULT_CALIBRATOR_FEATURES = (
    "p_catboost_yes",
    "source_health_aggregate",
    "days_since_last_settle",
    "recent_brier_trailing",
)


@dataclass(frozen=True, slots=True)
class OnlineCalibratorConfig:
    learning_rate: float = 0.05
    l2: float = 0.001
    feature_names: tuple[str, ...] = DEFAULT_CALIBRATOR_FEATURES


@dataclass(slots=True)
class OnlineLogisticCalibrator:
    config: OnlineCalibratorConfig = field(default_factory=OnlineCalibratorConfig)
    intercept: float = 0.0
    weights: dict[str, float] = field(default_factory=dict)
    update_count: int = 0

    def predict(self, features: Mapping[str, float]) -> float:
        normalized = self._normalized_features(features)
        logit = self.intercept
        for name, value in normalized.items():
            logit += self.weights.get(name, 0.0) * value
        return _sigmoid(logit)

    def update(self, features: Mapping[str, float], *, observed_yes: bool) -> float:
        normalized = self._normalized_features(features)
        prediction = self.predict(normalized)
        target = 1.0 if observed_yes else 0.0
        error = prediction - target
        lr = max(0.0, float(self.config.learning_rate))
        l2 = max(0.0, float(self.config.l2))
        self.intercept -= lr * error
        for name, value in normalized.items():
            current = self.weights.get(name, 0.0)
            self.weights[name] = current - lr * ((error * value) + (l2 * current))
        self.update_count += 1
        return prediction

    def to_dict(self) -> dict[str, Any]:
        return {
            "intercept": self.intercept,
            "weights": dict(sorted(self.weights.items())),
            "update_count": self.update_count,
            "config": {
                "learning_rate": self.config.learning_rate,
                "l2": self.config.l2,
                "feature_names": list(self.config.feature_names),
            },
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "OnlineLogisticCalibrator":
        config_payload = dict(payload.get("config") or {})
        config = OnlineCalibratorConfig(
            learning_rate=float(config_payload.get("learning_rate", 0.05)),
            l2=float(config_payload.get("l2", 0.001)),
            feature_names=tuple(config_payload.get("feature_names") or DEFAULT_CALIBRATOR_FEATURES),
        )
        calibrator = cls(
            config=config,
            intercept=float(payload.get("intercept", 0.0)),
            weights={str(k): float(v) for k, v in dict(payload.get("weights") or {}).items()},
            update_count=int(payload.get("update_count", 0)),
        )
        calibrator._assert_finite_state()
        return calibrator

    def _normalized_features(self, features: Mapping[str, float]) -> dict[str, float]:
        normalized: dict[str, float] = {}
        for name in self.config.feature_names:
            value = float(features.get(name, 0.0))
            if not isfinite(value):
                raise ValueError(f"calibrator feature {name} must be finite")
            normalized[name] = value
        return normalized

    def _assert_finite_state(self) -> None:
        if not isfinite(self.intercept):
            raise ValueError("calibrator intercept must be finite")
        for name, value in self.weights.items():
            if not isfinite(value):
                raise ValueError(f"calibrator weight {name} must be finite")


def calibration_features(
    *,
    p_catboost_yes: float,
    source_health_aggregate: float,
    days_since_last_settle: float,
    recent_brier_trailing: float,
) -> dict[str, float]:
    values = {
        "p_catboost_yes": p_catboost_yes,
        "source_health_aggregate": source_health_aggregate,
        "days_since_last_settle": days_since_last_settle,
        "recent_brier_trailing": recent_brier_trailing,
    }
    for key, value in values.items():
        if not isfinite(float(value)):
            raise ValueError(f"calibration feature {key} must be finite")
    return {key: float(value) for key, value in values.items()}


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = exp(-value)
        return 1.0 / (1.0 + z)
    z = exp(value)
    return z / (1.0 + z)
