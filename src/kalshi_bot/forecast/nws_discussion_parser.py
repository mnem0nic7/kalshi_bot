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


def neutral_discussion_features(*, reason: str | None = None) -> NwsDiscussionFeatures:
    return NwsDiscussionFeatures(valid=reason is None, discarded_reason=reason)


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
