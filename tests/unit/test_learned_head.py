from __future__ import annotations

import pytest

from kalshi_bot.forecast.learned_head import (
    LearnedHeadManifest,
    StructuredForecastFeatures,
    blend_learned_probability,
    stable_feature_hash,
    validate_learned_head_manifest,
)


def _features() -> StructuredForecastFeatures:
    return StructuredForecastFeatures(
        forecast_mean_f=72.0,
        forecast_sigma_f=3.2,
        bucket_low_f=69.0,
        bucket_high_f=None,
        lead_time_hours=18.0,
        city_id="NY",
        season="spring",
        source_health_aggregate=0.92,
        spread_cents=4.0,
        bid_volume=120.0,
        ask_volume=160.0,
        regime_label="standard",
    )


def test_learned_head_manifest_accepts_structured_features_only() -> None:
    result = validate_learned_head_manifest(LearnedHeadManifest(model_version="catboost-v1", seed=42))

    assert result.valid is True
    assert result.reasons == []


def test_learned_head_manifest_rejects_text_features() -> None:
    manifest = LearnedHeadManifest(
        model_version="catboost-v1",
        seed=42,
        feature_names=("forecast_mean_f", "discussion_text"),
    )

    result = validate_learned_head_manifest(manifest)

    assert result.valid is False
    assert "text_features_not_allowed" in result.reasons
    assert "missing_structured_features" in result.reasons


def test_stable_feature_hash_is_order_independent_for_same_dataclass() -> None:
    assert stable_feature_hash(_features()) == stable_feature_hash(_features())


def test_learned_probability_blend_caps_weight_at_half() -> None:
    blend = blend_learned_probability(
        p_closed_form=0.40,
        p_learned=0.80,
        learned_weight=0.90,
        learned_available=True,
    )

    assert blend.learned_weight == 0.5
    assert blend.p_final == pytest.approx(0.60)


def test_learned_probability_blend_falls_back_to_closed_form_when_unavailable() -> None:
    blend = blend_learned_probability(
        p_closed_form=0.40,
        p_learned=0.90,
        learned_weight=0.50,
        learned_available=False,
    )

    assert blend.p_final == 0.40
    assert blend.learned_weight == 0.0
    assert blend.reason == "learned_head_unavailable"
