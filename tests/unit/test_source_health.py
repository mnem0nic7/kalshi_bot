from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.forecast.source_health import (
    SourceHealthConfig,
    SourceHealthLabel,
    SourceHealthObservation,
    aggregate_source_health,
    health_size_multiplier,
    score_source_health,
    should_pause_new_entries,
)


def test_source_health_scores_fresh_complete_consistent_source_as_healthy() -> None:
    now = datetime(2026, 4, 27, 20, tzinfo=UTC)

    score = score_source_health(
        SourceHealthObservation(
            source="GFS",
            success=True,
            observed_at=now - timedelta(minutes=10),
            expected_member_count=31,
            observed_member_count=31,
            value_mean_f=71.0,
            rolling_mean_f=70.0,
        ),
        now=now,
    )

    assert score.label == SourceHealthLabel.HEALTHY
    assert score.score >= 0.95


def test_source_health_marks_failed_source_broken_with_zero_components() -> None:
    score = score_source_health(
        SourceHealthObservation(
            source="ECMWF",
            success=False,
            observed_at=None,
            expected_member_count=51,
            observed_member_count=0,
        ),
        now=datetime(2026, 4, 27, 20, tzinfo=UTC),
    )

    assert score.label == SourceHealthLabel.BROKEN
    assert score.score == 0.0
    assert score.success_score == 0.0
    assert score.freshness_score == 0.0
    assert score.completeness_score == 0.0


def test_source_health_degrades_for_stale_or_incomplete_members() -> None:
    now = datetime(2026, 4, 27, 20, tzinfo=UTC)

    score = score_source_health(
        SourceHealthObservation(
            source="AIFS",
            success=True,
            observed_at=now - timedelta(hours=9),
            expected_member_count=50,
            observed_member_count=25,
            value_mean_f=80.0,
            rolling_mean_f=70.0,
        ),
        now=now,
        config=SourceHealthConfig(expected_run_cadence_seconds=6 * 60 * 60),
    )

    assert score.label == SourceHealthLabel.DEGRADED
    assert 0.55 <= score.score < 0.85


def test_aggregate_source_health_drives_sizing_multiplier() -> None:
    now = datetime(2026, 4, 27, 20, tzinfo=UTC)
    healthy = score_source_health(
        SourceHealthObservation("GFS", True, now, 31, 31),
        now=now,
    )
    degraded = score_source_health(
        SourceHealthObservation(
            "ECMWF",
            True,
            now - timedelta(hours=9),
            51,
            26,
            value_mean_f=80.0,
            rolling_mean_f=70.0,
        ),
        now=now,
        config=SourceHealthConfig(expected_run_cadence_seconds=6 * 60 * 60),
    )

    aggregate = aggregate_source_health([healthy, degraded])

    assert aggregate.label == SourceHealthLabel.DEGRADED
    assert aggregate.health_size_mult == 0.5
    assert aggregate.degraded_sources == ["ECMWF"]
    assert health_size_multiplier(SourceHealthLabel.BROKEN) == 0.0


def test_consecutive_broken_labels_pause_new_entries() -> None:
    assert should_pause_new_entries(["BROKEN", "BROKEN"], consecutive_broken_cycles=2) is True
    assert should_pause_new_entries(["BROKEN", "DEGRADED"], consecutive_broken_cycles=2) is False
