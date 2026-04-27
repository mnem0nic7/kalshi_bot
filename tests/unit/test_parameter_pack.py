from __future__ import annotations

from kalshi_bot.learning.parameter_pack import (
    HARD_CAP_PARAMETER_NAMES,
    ParameterPack,
    default_parameter_pack,
    parameter_pack_hash,
    sanitize_parameter_pack,
)


def test_default_parameter_pack_excludes_hard_caps() -> None:
    pack = default_parameter_pack()

    assert pack.parameters["pseudo_count"] == 8
    assert not (set(pack.parameters) & HARD_CAP_PARAMETER_NAMES)
    assert "max_position_usd" in pack.metadata["hard_caps_excluded"]


def test_parameter_pack_sanitization_clamps_ranges_and_drops_hard_caps() -> None:
    base = default_parameter_pack()
    candidate = ParameterPack(
        version="candidate-v1",
        status="candidate",
        parent_version=base.version,
        source="test",
        description="test candidate",
        specs=base.specs,
        parameters={
            **base.parameters,
            "pseudo_count": 999,
            "kelly_fraction": -1.0,
            "catboost_blend_weight": 0.9,
            "max_position_usd": 10_000,
        },
    )

    sanitized = sanitize_parameter_pack(candidate)

    assert sanitized.parameters["pseudo_count"] == 32
    assert sanitized.parameters["kelly_fraction"] == 0.01
    assert sanitized.parameters["catboost_blend_weight"] == 0.5
    assert "max_position_usd" not in sanitized.parameters
    assert sanitized.metadata["dropped_hard_cap_parameters"] == ["max_position_usd"]


def test_parameter_pack_hash_is_stable_across_parameter_order() -> None:
    base = default_parameter_pack()
    reordered = ParameterPack(
        version=base.version,
        status=base.status,
        source=base.source,
        description=base.description,
        parameters=dict(reversed(list(base.parameters.items()))),
        specs=base.specs,
        metadata=base.metadata,
    )

    assert parameter_pack_hash(base) == parameter_pack_hash(reordered)
