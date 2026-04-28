from __future__ import annotations

import pytest

from kalshi_bot.learning.hard_caps import hard_caps_from_mapping, load_hard_caps, validate_hard_caps


def test_hard_caps_file_loads_and_hashes_stably() -> None:
    caps = load_hard_caps("infra/config/hard_caps.yaml")

    assert caps.operator_only is True
    assert caps.schema_version == "hard-caps-v1"
    assert caps.hard_caps["max_position_pct"] == 0.10
    assert len(caps.config_hash) == 64
    assert caps.config_hash == load_hard_caps("infra/config/hard_caps.yaml").config_hash


def test_hard_caps_reject_non_operator_owned_config() -> None:
    caps = hard_caps_from_mapping(
        {
            "schema_version": "hard-caps-v1",
            "operator_only": False,
            "hard_caps": {
                "max_position_pct": 0.10,
                "max_total_exposure_pct": 0.25,
                "daily_max_loss_pct": 0.20,
                "max_drawdown_pct": 0.20,
                "max_position_usd": None,
            },
        }
    )

    with pytest.raises(ValueError, match="operator_only"):
        validate_hard_caps(caps)
