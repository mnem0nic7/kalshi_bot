from __future__ import annotations

import pytest

from kalshi_bot.forecast.learned_head import (
    LearnedHeadHoldoutMetrics,
    LearnedHeadManifest,
    StructuredForecastFeatures,
    blend_learned_probability,
    evaluate_learned_head_gate,
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
    assert result.to_dict() == {"valid": True, "reasons": []}


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


def test_learned_head_gate_allows_weight_only_when_holdout_beats_closed_form() -> None:
    result = evaluate_learned_head_gate(
        closed_form=LearnedHeadHoldoutMetrics(brier=0.20, ece=0.05, sharpe=1.0),
        learned=LearnedHeadHoldoutMetrics(brier=0.19, ece=0.04, sharpe=1.06),
        requested_weight=0.90,
    )

    assert result.passed is True
    assert result.learned_weight == 0.5
    assert result.failures == []


def test_learned_head_gate_forces_zero_weight_on_partial_regression() -> None:
    result = evaluate_learned_head_gate(
        closed_form=LearnedHeadHoldoutMetrics(brier=0.20, ece=0.05, sharpe=1.0),
        learned=LearnedHeadHoldoutMetrics(brier=0.19, ece=0.06, sharpe=1.01, invalid_probability_count=1),
        requested_weight=0.25,
    )

    assert result.passed is False
    assert result.learned_weight == 0.0
    assert result.failures == ["ece_not_improved", "sharpe_not_improved", "invalid_probability"]
