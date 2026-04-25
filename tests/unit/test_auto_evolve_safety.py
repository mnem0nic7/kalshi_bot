from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.services.core.auto_evolve_safety import (
    delta_cap_error,
    delta_to_bps,
    rate_to_bps,
    validate_delta_cap,
)


def test_rate_to_bps_handles_common_rate_shapes() -> None:
    assert rate_to_bps(Decimal("0.12345")) == 1235
    assert rate_to_bps("0.10") == 1000
    assert rate_to_bps(None) is None


def test_delta_to_bps_accepts_delta_or_candidate_and_reference() -> None:
    assert delta_to_bps(Decimal("0.025")) == 250
    assert delta_to_bps(Decimal("0.64"), Decimal("0.615")) == 250
    assert delta_to_bps(None, Decimal("0.615")) is None
    assert delta_to_bps(Decimal("0.64"), None) is None


@pytest.mark.parametrize("value", [True, "not-a-number", "NaN"])
def test_rate_to_bps_rejects_non_numeric_values(value: object) -> None:
    with pytest.raises(ValueError, match="rate"):
        rate_to_bps(value)  # type: ignore[arg-type]


def test_validate_delta_cap_allows_changes_within_relative_cap() -> None:
    current = {
        "risk_min_edge_bps": 100,
        "trigger_max_spread_bps": 500,
        "risk_safe_capital_reserve_ratio": 0.5,
    }
    proposed = {
        "risk_min_edge_bps": 120,
        "trigger_max_spread_bps": 600,
        "risk_safe_capital_reserve_ratio": 0.6,
    }

    assert validate_delta_cap(current, proposed, max_delta_pct=0.30) == []


def test_validate_delta_cap_reports_changes_over_relative_cap() -> None:
    current = {"risk_min_edge_bps": 100}
    proposed = {"risk_min_edge_bps": 200}

    violations = validate_delta_cap(current, proposed, max_delta_pct=0.30)

    assert violations == [
        {
            "field": "risk_min_edge_bps",
            "current": 100.0,
            "proposed": 200.0,
            "allowed_min": 70.0,
            "allowed_max": 130.0,
        }
    ]


def test_validate_delta_cap_uses_field_ceiling_when_current_value_is_zero() -> None:
    current = {"risk_safe_capital_reserve_ratio": 0}

    assert validate_delta_cap(
        current,
        {"risk_safe_capital_reserve_ratio": 0.25},
        max_delta_pct=0.30,
    ) == []

    violations = validate_delta_cap(
        current,
        {"risk_safe_capital_reserve_ratio": 0.40},
        max_delta_pct=0.30,
    )
    assert violations[0] == {
        "field": "risk_safe_capital_reserve_ratio",
        "current": 0.0,
        "proposed": 0.4,
        "allowed_min": 0.0,
        "allowed_max": 0.3,
    }


def test_validate_delta_cap_ignores_unset_threshold_values() -> None:
    violations = validate_delta_cap(
        {"risk_min_edge_bps": None},
        {"risk_min_edge_bps": 200},
        max_delta_pct=0.30,
    )

    assert violations == []


def test_delta_cap_error_matches_auto_evolve_error_payload_shape() -> None:
    error = delta_cap_error(
        {"risk_min_edge_bps": 100},
        {"risk_min_edge_bps": 200},
        max_delta_pct=0.30,
    )

    assert error is not None
    assert error["stage"] == "accept"
    assert error["reason"] == "delta_cap_exceeded"
    assert error["cap_pct"] == 0.30
    assert error["violations"][0]["field"] == "risk_min_edge_bps"


def test_validate_delta_cap_rejects_out_of_range_cap() -> None:
    with pytest.raises(ValueError, match="max_delta_pct"):
        validate_delta_cap({}, {}, max_delta_pct=1.1)
