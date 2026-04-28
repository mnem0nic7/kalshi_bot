from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Mapping


STRUCTURED_FEATURE_NAMES = (
    "forecast_mean_f",
    "forecast_sigma_f",
    "bucket_low_f",
    "bucket_high_f",
    "lead_time_hours",
    "city_id",
    "season",
    "source_health_aggregate",
    "spread_cents",
    "bid_volume",
    "ask_volume",
    "regime_label",
)

TEXT_FEATURE_NAMES = {
    "discussion_text",
    "raw_text",
    "afd_text",
    "forecast_discussion",
    "nws_discussion",
}


@dataclass(frozen=True, slots=True)
class StructuredForecastFeatures:
    forecast_mean_f: float
    forecast_sigma_f: float
    bucket_low_f: float | None
    bucket_high_f: float | None
    lead_time_hours: float
    city_id: str
    season: str
    source_health_aggregate: float
    spread_cents: float
    bid_volume: float
    ask_volume: float
    regime_label: str

    def to_dict(self) -> dict[str, float | str | None]:
        return {
            "forecast_mean_f": self.forecast_mean_f,
            "forecast_sigma_f": self.forecast_sigma_f,
            "bucket_low_f": self.bucket_low_f,
            "bucket_high_f": self.bucket_high_f,
            "lead_time_hours": self.lead_time_hours,
            "city_id": self.city_id,
            "season": self.season,
            "source_health_aggregate": self.source_health_aggregate,
            "spread_cents": self.spread_cents,
            "bid_volume": self.bid_volume,
            "ask_volume": self.ask_volume,
            "regime_label": self.regime_label,
        }


@dataclass(frozen=True, slots=True)
class LearnedHeadManifest:
    model_version: str
    model_kind: str = "catboost"
    seed: int = 0
    feature_names: tuple[str, ...] = STRUCTURED_FEATURE_NAMES
    training_data_hash: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_version": self.model_version,
            "model_kind": self.model_kind,
            "seed": self.seed,
            "feature_names": list(self.feature_names),
            "training_data_hash": self.training_data_hash,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True, slots=True)
class LearnedHeadValidation:
    valid: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class LearnedHeadHoldoutMetrics:
    brier: float
    ece: float
    sharpe: float
    invalid_probability_count: int = 0

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "LearnedHeadHoldoutMetrics":
        return cls(
            brier=float(payload.get("brier", 1.0)),
            ece=float(payload.get("ece", 1.0)),
            sharpe=float(payload.get("sharpe", 0.0)),
            invalid_probability_count=int(payload.get("invalid_probability_count", 0)),
        )


@dataclass(frozen=True, slots=True)
class LearnedHeadGateResult:
    passed: bool
    learned_weight: float
    failures: list[str]
    comparisons: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "learned_weight": self.learned_weight,
            "failures": list(self.failures),
            "comparisons": dict(self.comparisons),
        }


@dataclass(frozen=True, slots=True)
class ProbabilityBlend:
    p_final: float
    learned_weight: float
    p_learned: float
    p_closed_form: float
    reason: str

    def to_dict(self) -> dict[str, float | str]:
        return {
            "p_final": self.p_final,
            "learned_weight": self.learned_weight,
            "p_learned": self.p_learned,
            "p_closed_form": self.p_closed_form,
            "reason": self.reason,
        }


def validate_learned_head_manifest(manifest: LearnedHeadManifest) -> LearnedHeadValidation:
    reasons: list[str] = []
    if manifest.model_kind != "catboost":
        reasons.append("model_kind_must_be_catboost")
    if manifest.seed is None:
        reasons.append("seed_required")
    names = tuple(str(name) for name in manifest.feature_names)
    missing = [name for name in STRUCTURED_FEATURE_NAMES if name not in names]
    extra_text = sorted(TEXT_FEATURE_NAMES & set(names))
    if missing:
        reasons.append("missing_structured_features")
    if extra_text:
        reasons.append("text_features_not_allowed")
    if not manifest.model_version.strip():
        reasons.append("model_version_required")
    return LearnedHeadValidation(valid=not reasons, reasons=reasons)


def evaluate_learned_head_gate(
    *,
    closed_form: LearnedHeadHoldoutMetrics,
    learned: LearnedHeadHoldoutMetrics,
    requested_weight: float,
    max_weight: float = 0.5,
    min_sharpe_improvement: float = 0.05,
) -> LearnedHeadGateResult:
    failures: list[str] = []
    comparisons: dict[str, Any] = {}

    comparisons["brier"] = {"closed_form": closed_form.brier, "learned": learned.brier, "must_be_lower": True}
    if learned.brier >= closed_form.brier:
        failures.append("brier_not_improved")

    comparisons["ece"] = {"closed_form": closed_form.ece, "learned": learned.ece, "must_be_lower": True}
    if learned.ece >= closed_form.ece:
        failures.append("ece_not_improved")

    min_sharpe = _minimum_learned_sharpe(closed_form.sharpe, min_sharpe_improvement)
    comparisons["sharpe"] = {"closed_form": closed_form.sharpe, "learned": learned.sharpe, "minimum": min_sharpe}
    if learned.sharpe < min_sharpe:
        failures.append("sharpe_not_improved")

    comparisons["invalid_probability_count"] = learned.invalid_probability_count
    if learned.invalid_probability_count:
        failures.append("invalid_probability")

    passed = not failures
    return LearnedHeadGateResult(
        passed=passed,
        learned_weight=max(0.0, min(float(requested_weight), float(max_weight), 0.5)) if passed else 0.0,
        failures=failures,
        comparisons=comparisons,
    )


def stable_feature_payload(features: StructuredForecastFeatures) -> dict[str, Any]:
    payload = features.to_dict()
    for key in ("city_id", "season", "regime_label"):
        payload[f"{key}_hash"] = stable_category_hash(str(payload[key]))
    return payload


def stable_feature_hash(features: StructuredForecastFeatures) -> str:
    encoded = json.dumps(
        stable_feature_payload(features),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def blend_learned_probability(
    *,
    p_closed_form: float,
    p_learned: float | None,
    learned_weight: float,
    learned_available: bool,
    max_weight: float = 0.5,
) -> ProbabilityBlend:
    closed_form = clamp_probability(p_closed_form)
    if not learned_available or p_learned is None:
        return ProbabilityBlend(
            p_final=closed_form,
            learned_weight=0.0,
            p_learned=closed_form,
            p_closed_form=closed_form,
            reason="learned_head_unavailable",
        )
    learned = clamp_probability(p_learned)
    weight = max(0.0, min(float(learned_weight), float(max_weight), 0.5))
    final = (weight * learned) + ((1.0 - weight) * closed_form)
    return ProbabilityBlend(
        p_final=clamp_probability(final),
        learned_weight=weight,
        p_learned=learned,
        p_closed_form=closed_form,
        reason="blended" if weight > 0 else "zero_weight",
    )


def numeric_feature_vector(payload: Mapping[str, Any]) -> dict[str, float]:
    vector: dict[str, float] = {}
    for key, value in payload.items():
        if value is None:
            vector[key] = 0.0
        elif isinstance(value, bool):
            vector[key] = 1.0 if value else 0.0
        elif isinstance(value, (int, float)):
            vector[key] = float(value) if isfinite(float(value)) else 0.0
        else:
            vector[key] = stable_category_hash(str(value))
    return vector


def stable_category_hash(value: str) -> float:
    digest = hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def clamp_probability(value: float) -> float:
    if not isfinite(float(value)):
        raise ValueError("probability must be finite")
    return max(0.0, min(1.0, float(value)))


def _minimum_learned_sharpe(closed_form_sharpe: float, improvement: float) -> float:
    if closed_form_sharpe > 0:
        return closed_form_sharpe * (1.0 + improvement)
    return closed_form_sharpe + abs(closed_form_sharpe) * improvement
