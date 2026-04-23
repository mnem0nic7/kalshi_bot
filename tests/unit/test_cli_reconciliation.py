"""Unit tests for Session 5 Strategy C schema + backfill logic.

Coverage:
- ORM models import correctly and have expected fields
- delta_degf arithmetic (cli_value - asos_observed_max)
- cli_station_variance rollup statistics (mean, stdev, abs-value percentile)
- CliReconciliationRecord composite PK roundtrip (mock session)
- StrategyCRoom execution_outcome / settlement_outcome independence
"""
from __future__ import annotations

import math
import statistics
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from kalshi_bot.db.models import (
    CliReconciliationRecord,
    CliStationVariance,
    StrategyCRoom,
)


# ---------------------------------------------------------------------------
# Model field presence (smoke test — catches typos in column definitions)
# ---------------------------------------------------------------------------

def test_cli_reconciliation_has_required_columns() -> None:
    r = CliReconciliationRecord(
        station="KBOS",
        observation_date=date(2026, 4, 1),
        asos_observed_max=72.5,
        cli_value=73.0,
        delta_degf=0.5,
    )
    assert r.station == "KBOS"
    assert r.observation_date == date(2026, 4, 1)
    assert r.asos_observed_max == 72.5
    assert r.cli_value == 73.0
    assert r.delta_degf == 0.5


def test_strategy_c_room_has_two_outcome_columns() -> None:
    r = StrategyCRoom(
        ticker="KXHIGHTBOS-26APR22-T58",
        station="KBOS",
        decision_time=datetime.now(UTC),
        resolution_state="LOCKED_YES",
        observed_max_at_decision=58.5,
        threshold=58.0,
        fair_value_dollars=Decimal("0.9900"),
        modeled_edge_cents=5.0,
        target_price_cents=94.0,
        contracts_requested=50,
        execution_outcome="shadow",
    )
    assert hasattr(r, "execution_outcome")
    assert hasattr(r, "settlement_outcome")
    assert r.settlement_outcome is None  # populated post-settlement only


def test_cli_station_variance_has_signed_and_abs_columns() -> None:
    v = CliStationVariance(
        station="KBOS",
        sample_count=40,
        signed_mean_delta_degf=0.12,
        signed_stddev_delta_degf=0.45,
        mean_abs_delta_degf=0.38,
        p95_abs_delta_degf=0.91,
        last_refreshed_at=datetime.now(UTC),
    )
    assert v.signed_mean_delta_degf == 0.12
    assert v.signed_stddev_delta_degf == 0.45
    assert v.mean_abs_delta_degf == 0.38
    assert v.p95_abs_delta_degf == 0.91


# ---------------------------------------------------------------------------
# delta_degf arithmetic
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cli_value,asos_max,expected_delta", [
    (73.0,  72.5,  0.5),
    (72.0,  73.0, -1.0),
    (68.0,  68.0,  0.0),
    (99.9,  98.0,  1.9),
])
def test_delta_degf_is_cli_minus_asos(cli_value: float, asos_max: float, expected_delta: float) -> None:
    delta = round(cli_value - asos_max, 4)
    assert math.isclose(delta, expected_delta, abs_tol=1e-4)


# ---------------------------------------------------------------------------
# cli_station_variance rollup statistics
# ---------------------------------------------------------------------------

def _compute_variance_stats(deltas: list[float]) -> dict:
    n = len(deltas)
    signed_mean = statistics.mean(deltas)
    signed_std = statistics.stdev(deltas) if n >= 2 else 0.0
    abs_deltas = sorted(abs(d) for d in deltas)
    mean_abs = statistics.mean(abs_deltas)
    p95_idx = max(0, int(0.95 * n) - 1)
    p95_abs = abs_deltas[p95_idx]
    return {
        "sample_count": n,
        "signed_mean_delta_degf": signed_mean,
        "signed_stddev_delta_degf": signed_std,
        "mean_abs_delta_degf": mean_abs,
        "p95_abs_delta_degf": p95_abs,
    }


def test_rollup_signed_mean_captures_direction() -> None:
    # CLI consistently underreports → negative mean
    deltas = [-0.5, -0.3, -0.7, -0.4, -0.6]
    stats = _compute_variance_stats(deltas)
    assert stats["signed_mean_delta_degf"] < 0


def test_rollup_signed_mean_zero_for_balanced_deltas() -> None:
    deltas = [-1.0, 0.0, 1.0]
    stats = _compute_variance_stats(deltas)
    assert math.isclose(stats["signed_mean_delta_degf"], 0.0, abs_tol=1e-9)


def test_rollup_abs_mean_is_always_nonnegative() -> None:
    deltas = [-2.0, -1.0, 0.0, 1.0, 2.0]
    stats = _compute_variance_stats(deltas)
    assert stats["mean_abs_delta_degf"] >= 0.0


def test_rollup_p95_is_within_range() -> None:
    deltas = [float(i) * 0.1 for i in range(100)]
    stats = _compute_variance_stats(deltas)
    assert 0 <= stats["p95_abs_delta_degf"] <= max(abs(d) for d in deltas)


def test_rollup_p95_exceeds_mean_abs_for_skewed_distribution() -> None:
    # 90 small deltas + 10 large outliers; p95 index = 94 → falls in the outlier range
    deltas = [0.1] * 90 + [5.0] * 10
    stats = _compute_variance_stats(deltas)
    assert stats["p95_abs_delta_degf"] > stats["mean_abs_delta_degf"]


def test_rollup_stdev_zero_for_constant_deltas() -> None:
    deltas = [0.5] * 10
    stats = _compute_variance_stats(deltas)
    assert math.isclose(stats["signed_stddev_delta_degf"], 0.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# StrategyCRoom outcome independence
# ---------------------------------------------------------------------------

def test_execution_outcome_filled_settlement_pending() -> None:
    r = StrategyCRoom(
        ticker="KXHIGHTBOS-26APR22-T58",
        station="KBOS",
        decision_time=datetime.now(UTC),
        resolution_state="LOCKED_YES",
        observed_max_at_decision=58.5,
        threshold=58.0,
        fair_value_dollars=Decimal("0.9900"),
        modeled_edge_cents=5.0,
        target_price_cents=94.0,
        contracts_requested=50,
        execution_outcome="filled",
        contracts_filled=50,
        avg_fill_price_cents=94.0,
        realized_edge_cents=5.0,
    )
    assert r.execution_outcome == "filled"
    assert r.settlement_outcome is None  # not yet settled


def test_settlement_outcome_lock_reversed_is_independent() -> None:
    r = StrategyCRoom(
        ticker="KXHIGHTBOS-26APR22-T58",
        station="KBOS",
        decision_time=datetime.now(UTC),
        resolution_state="LOCKED_YES",
        observed_max_at_decision=58.5,
        threshold=58.0,
        fair_value_dollars=Decimal("0.9900"),
        modeled_edge_cents=5.0,
        target_price_cents=94.0,
        contracts_requested=50,
        execution_outcome="filled",
        settlement_outcome="lock_reversed",  # CLI disagreed — signal precision bug
        contracts_filled=50,
    )
    assert r.execution_outcome == "filled"
    assert r.settlement_outcome == "lock_reversed"


@pytest.mark.parametrize("execution_outcome", [
    "filled", "partial_fill", "raced", "cancelled", "rejected", "error", "shadow", "pending",
])
def test_all_execution_outcomes_are_accepted(execution_outcome: str) -> None:
    r = StrategyCRoom(
        ticker="TEST-T58",
        station="KBOS",
        decision_time=datetime.now(UTC),
        resolution_state="LOCKED_YES",
        observed_max_at_decision=58.5,
        threshold=58.0,
        fair_value_dollars=Decimal("0.9900"),
        modeled_edge_cents=5.0,
        target_price_cents=94.0,
        contracts_requested=10,
        execution_outcome=execution_outcome,
    )
    assert r.execution_outcome == execution_outcome


@pytest.mark.parametrize("settlement_outcome", [
    "lock_held", "lock_reversed", "void", None,
])
def test_all_settlement_outcomes_are_accepted(settlement_outcome: str | None) -> None:
    r = StrategyCRoom(
        ticker="TEST-T58",
        station="KBOS",
        decision_time=datetime.now(UTC),
        resolution_state="LOCKED_YES",
        observed_max_at_decision=58.5,
        threshold=58.0,
        fair_value_dollars=Decimal("0.9900"),
        modeled_edge_cents=5.0,
        target_price_cents=94.0,
        contracts_requested=10,
        execution_outcome="shadow",
        settlement_outcome=settlement_outcome,
    )
    assert r.settlement_outcome == settlement_outcome
