from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping


CONFIDENCE_VALUES = {"low", "medium", "high"}
DIRECTION_VALUES = {"warmer", "cooler", "neutral"}
MAX_KEYWORDS = 12
MAX_KEYWORD_LENGTH = 48


@dataclass(frozen=True, slots=True)
class NwsDiscussionFeatures:
    forecaster_confidence: str = "medium"
    regime_keywords: tuple[str, ...] = field(default_factory=tuple)
    anomaly_flag: bool = False
    discussion_direction: str = "neutral"
    valid: bool = True
    discarded_reason: str | None = None

    @property
    def neutral(self) -> bool:
        return (
            self.forecaster_confidence == "medium"
            and not self.regime_keywords
            and not self.anomaly_flag
            and self.discussion_direction == "neutral"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "forecaster_confidence": self.forecaster_confidence,
            "regime_keywords": list(self.regime_keywords),
            "anomaly_flag": self.anomaly_flag,
            "discussion_direction": self.discussion_direction,
            "valid": self.valid,
            "discarded_reason": self.discarded_reason,
        }


@dataclass(frozen=True, slots=True)
class NwsParserHealthWindow:
    attempts: int
    successful_parses: int
    schema_failures: int

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "NwsParserHealthWindow":
        return cls(
            attempts=int(payload.get("attempts", 0)),
            successful_parses=int(payload.get("successful_parses", payload.get("available_count", 0))),
            schema_failures=int(payload.get("schema_failures", payload.get("schema_failure_count", 0))),
        )


@dataclass(frozen=True, slots=True)
class NwsParserHealthGate:
    passed: bool
    feature_weight: float
    failures: list[str]
    comparisons: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "feature_weight": self.feature_weight,
            "failures": list(self.failures),
            "comparisons": dict(self.comparisons),
        }


def neutral_discussion_features(*, reason: str | None = None) -> NwsDiscussionFeatures:
    return NwsDiscussionFeatures(valid=reason is None, discarded_reason=reason)


def evaluate_nws_parser_health(
    window: NwsParserHealthWindow,
    *,
    requested_feature_weight: float = 0.0,
    min_availability: float = 0.95,
    max_schema_failure_rate: float = 0.01,
) -> NwsParserHealthGate:
    failures: list[str] = []
    attempts = max(0, window.attempts)
    availability = (window.successful_parses / attempts) if attempts else 0.0
    schema_failure_rate = (window.schema_failures / attempts) if attempts else 0.0
    comparisons = {
        "attempts": attempts,
        "availability": {
            "observed": availability,
            "minimum": min_availability,
        },
        "schema_failure_rate": {
            "observed": schema_failure_rate,
            "maximum": max_schema_failure_rate,
        },
    }

    if attempts <= 0:
        failures.append("no_parser_attempts")
    if availability < min_availability:
        failures.append("parser_availability_below_minimum")
    if schema_failure_rate > max_schema_failure_rate:
        failures.append("schema_failure_rate_above_maximum")

    passed = not failures
    return NwsParserHealthGate(
        passed=passed,
        feature_weight=max(0.0, min(1.0, float(requested_feature_weight))) if passed else 0.0,
        failures=failures,
        comparisons=comparisons,
    )


def parse_nws_discussion_json(raw: str | bytes | Mapping[str, Any]) -> NwsDiscussionFeatures:
    try:
        if isinstance(raw, Mapping):
            payload = dict(raw)
        else:
            payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return neutral_discussion_features(reason="invalid_json")
    if not isinstance(payload, dict):
        return neutral_discussion_features(reason="payload_not_object")
    return validate_nws_discussion_payload(payload)


def validate_nws_discussion_payload(payload: Mapping[str, Any]) -> NwsDiscussionFeatures:
    allowed_keys = {
        "forecaster_confidence",
        "regime_keywords",
        "anomaly_flag",
        "discussion_direction",
    }
    extra_keys = set(payload) - allowed_keys
    if extra_keys:
        return neutral_discussion_features(reason="unexpected_fields")

    confidence = str(payload.get("forecaster_confidence", "")).strip().lower()
    if confidence not in CONFIDENCE_VALUES:
        return neutral_discussion_features(reason="invalid_confidence")

    direction = str(payload.get("discussion_direction", "")).strip().lower()
    if direction not in DIRECTION_VALUES:
        return neutral_discussion_features(reason="invalid_direction")

    keywords_raw = payload.get("regime_keywords")
    if not isinstance(keywords_raw, list):
        return neutral_discussion_features(reason="keywords_not_list")
    keywords: list[str] = []
    for raw_keyword in keywords_raw[:MAX_KEYWORDS]:
        if not isinstance(raw_keyword, str):
            return neutral_discussion_features(reason="keyword_not_string")
        keyword = " ".join(raw_keyword.strip().lower().split())
        if not keyword:
            continue
        if len(keyword) > MAX_KEYWORD_LENGTH:
            return neutral_discussion_features(reason="keyword_too_long")
        if keyword not in keywords:
            keywords.append(keyword)

    anomaly = payload.get("anomaly_flag")
    if not isinstance(anomaly, bool):
        return neutral_discussion_features(reason="anomaly_not_boolean")

    return NwsDiscussionFeatures(
        forecaster_confidence=confidence,
        regime_keywords=tuple(keywords),
        anomaly_flag=anomaly,
        discussion_direction=direction,
    )
