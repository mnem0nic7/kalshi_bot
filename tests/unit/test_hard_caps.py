from __future__ import annotations

import pytest

from kalshi_bot.learning.hard_caps import hard_caps_from_mapping, load_hard_caps, validate_hard_caps


def test_hard_caps_file_loads_and_hashes_stably() -> None:
    caps = load_hard_caps("infra/config/hard_caps.yaml")

    assert caps.operator_only is True
    assert caps.schema_version == "hard-caps-v1"
    assert caps.hard_caps["max_position_pct"] == 0.10
    assert caps.hard_caps["daily_max_loss_pct"] == 0.02
    assert caps.hard_caps["max_drawdown_pct"] == 0.05
    assert caps.hard_caps["max_order_count_fp"] == 200.0
    assert caps.hard_caps["max_position_count_fp_per_ticker"] == 200.0
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


def test_hard_caps_reject_order_count_above_position_count() -> None:
    caps = hard_caps_from_mapping(
        {
            "schema_version": "hard-caps-v1",
            "operator_only": True,
            "hard_caps": {
                "max_position_pct": 0.10,
                "max_total_exposure_pct": 0.25,
                "daily_max_loss_pct": 0.02,
                "max_drawdown_pct": 0.05,
                "max_position_usd": None,
                "max_order_count_fp": 201.0,
                "max_position_count_fp_per_ticker": 200.0,
            },
        }
    )

    with pytest.raises(ValueError, match="max_order_count_fp"):
        validate_hard_caps(caps)
