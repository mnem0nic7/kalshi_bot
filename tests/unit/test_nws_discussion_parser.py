from __future__ import annotations

from kalshi_bot.forecast.nws_discussion_parser import parse_nws_discussion_json


def test_nws_discussion_parser_accepts_strict_schema() -> None:
    features = parse_nws_discussion_json(
        {
            "forecaster_confidence": "High",
            "regime_keywords": ["Heat Ridge", " heat ridge ", "sea breeze"],
            "anomaly_flag": True,
            "discussion_direction": "Warmer",
        }
    )

    assert features.valid is True
    assert features.forecaster_confidence == "high"
    assert features.regime_keywords == ("heat ridge", "sea breeze")
    assert features.anomaly_flag is True
    assert features.discussion_direction == "warmer"


def test_nws_discussion_parser_discards_unexpected_fields_to_neutral() -> None:
    features = parse_nws_discussion_json(
        {
            "forecaster_confidence": "high",
            "regime_keywords": ["heat"],
            "anomaly_flag": True,
            "discussion_direction": "warmer",
            "probability_boost": 0.2,
        }
    )

    assert features.valid is False
    assert features.neutral is True
    assert features.discarded_reason == "unexpected_fields"


def test_nws_discussion_parser_discards_invalid_json_to_neutral() -> None:
    features = parse_nws_discussion_json("{")

    assert features.valid is False
    assert features.neutral is True
    assert features.discarded_reason == "invalid_json"
