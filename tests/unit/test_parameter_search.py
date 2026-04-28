from __future__ import annotations

import pytest

from kalshi_bot.learning.hard_caps import load_hard_caps
from kalshi_bot.learning.parameter_search import generate_parameter_pack_grid, select_parameter_pack_candidate


def _current_report() -> dict[str, object]:
    return {
        "coverage": 0.99,
        "brier": 0.20,
        "ece": 0.05,
        "sharpe": 1.00,
        "max_drawdown": 0.10,
        "city_win_rates": {"NY": 0.58},
        "pack_hash": "current",
        "rerun_pack_hash": "current",
    }


def _passing_holdout() -> dict[str, object]:
    return {
        "coverage": 0.98,
        "brier": 0.19,
        "ece": 0.04,
        "sharpe": 1.01,
        "max_drawdown": 0.09,
        "city_win_rates": {"NY": 0.57},
        "hard_cap_touches": 0,
    }


def test_select_parameter_pack_candidate_stamps_hashes_and_selects_first_passing_candidate() -> None:
    result = select_parameter_pack_candidate(
        search_payload={
            "candidates": [
                {
                    "parameters": {"pseudo_count": 10},
                    "holdout_report": _passing_holdout(),
                }
            ]
        },
        current_report=_current_report(),
        hard_caps=load_hard_caps("infra/config/hard_caps.yaml"),
    )

    assert result.selected is not None
    assert result.selected.pack.parameters["pseudo_count"] == 10
    assert result.selected.pack.version.startswith("candidate-1-")
    assert result.selected.holdout_report["pack_hash"] == result.selected.pack.pack_hash
    assert result.selected.holdout_report["rerun_pack_hash"] == result.selected.pack.pack_hash
    assert result.selected.failures == []


def test_select_parameter_pack_candidate_rejects_hard_cap_overrides_and_uses_next_candidate() -> None:
    result = select_parameter_pack_candidate(
        search_payload=[
            {
                "version": "bad-hard-cap",
                "parameters": {"max_position_usd": 10_000},
                "holdout_report": _passing_holdout(),
            },
            {
                "version": "clean-candidate",
                "parameters": {"pseudo_count": 12},
                "holdout_report": _passing_holdout(),
            },
        ],
        current_report=_current_report(),
        hard_caps=load_hard_caps("infra/config/hard_caps.yaml"),
    )

    assert result.selected is not None
    assert result.selected.pack.version == "clean-candidate"
    assert result.evaluated[0].passed is False
    assert "candidate_contains_hard_cap_parameters" in result.evaluated[0].failures


def test_select_parameter_pack_candidate_preserves_idempotency_failures() -> None:
    result = select_parameter_pack_candidate(
        search_payload=[
            {
                "version": "hash-mismatch",
                "parameters": {"pseudo_count": 10},
                "holdout_report": {**_passing_holdout(), "pack_hash": "candidate", "rerun_pack_hash": "different"},
            }
        ],
        current_report=_current_report(),
        hard_caps=load_hard_caps("infra/config/hard_caps.yaml"),
    )

    assert result.selected is None
    assert result.evaluated[0].failures == ["pack_hash_not_idempotent"]


def test_generate_parameter_pack_grid_is_bounded_and_deterministic() -> None:
    result = generate_parameter_pack_grid(
        {
            "parameters": {
                "kelly_fraction": [0.20, 0.25],
                "pseudo_count": [4, 999],
            }
        }
    )

    assert result.to_dict()["count"] == 4
    assert [candidate.pack.parameters["pseudo_count"] for candidate in result.candidates] == [4, 32, 4, 32]
    assert [candidate.pack.parameters["kelly_fraction"] for candidate in result.candidates] == [0.20, 0.20, 0.25, 0.25]
    assert result.candidates[0].pack.version.startswith("grid-1-")
    assert result.candidates[0].pack.metadata["grid_parameters"] == ["kelly_fraction", "pseudo_count"]


def test_generate_parameter_pack_grid_rejects_unknown_and_hard_cap_parameters() -> None:
    with pytest.raises(ValueError, match="hard cap"):
        generate_parameter_pack_grid({"parameters": {"max_position_usd": [100]}})

    with pytest.raises(ValueError, match="unknown"):
        generate_parameter_pack_grid({"parameters": {"not_a_parameter": [1]}})
