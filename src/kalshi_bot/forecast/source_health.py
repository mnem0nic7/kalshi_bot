from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from math import isfinite
from typing import Any, Iterable


class SourceHealthLabel(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    BROKEN = "BROKEN"


@dataclass(frozen=True, slots=True)
class SourceHealthConfig:
    success_weight: float = 0.45
    freshness_weight: float = 0.25
    completeness_weight: float = 0.20
    consistency_weight: float = 0.10
    healthy_threshold: float = 0.85
    degraded_threshold: float = 0.55
    expected_run_cadence_seconds: int = 21_600
    consistency_deviation_scale_f: float = 12.0


@dataclass(frozen=True, slots=True)
class SourceHealthObservation:
    source: str
    success: bool
    observed_at: datetime | None
    expected_member_count: int
    observed_member_count: int
    value_mean_f: float | None = None
    rolling_mean_f: float | None = None
    expected_run_cadence_seconds: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SourceHealthScore:
    source: str
    label: SourceHealthLabel
    score: float
    success_score: float
    freshness_score: float
    completeness_score: float
    consistency_score: float
    observed_at: datetime | None
    expected_member_count: int
    observed_member_count: int
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "label": self.label.value,
            "score": self.score,
            "success_score": self.success_score,
            "freshness_score": self.freshness_score,
            "completeness_score": self.completeness_score,
            "consistency_score": self.consistency_score,
            "observed_at": self.observed_at.isoformat() if self.observed_at is not None else None,
            "expected_member_count": self.expected_member_count,
            "observed_member_count": self.observed_member_count,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class AggregateSourceHealth:
    label: SourceHealthLabel
    score: float
    source_count: int
    source_scores: dict[str, SourceHealthScore]

    @property
    def health_size_mult(self) -> float:
        return health_size_multiplier(self.label)

    @property
    def broken_sources(self) -> list[str]:
        return sorted(
            source
            for source, score in self.source_scores.items()
            if score.label == SourceHealthLabel.BROKEN
        )

    @property
    def degraded_sources(self) -> list[str]:
        return sorted(
            source
            for source, score in self.source_scores.items()
            if score.label == SourceHealthLabel.DEGRADED
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label.value,
            "score": self.score,
            "source_count": self.source_count,
            "health_size_mult": self.health_size_mult,
            "broken_sources": self.broken_sources,
            "degraded_sources": self.degraded_sources,
            "sources": {source: score.to_dict() for source, score in self.source_scores.items()},
        }


def clamp_unit(value: float) -> float:
    if not isfinite(value):
        return 0.0
    return max(0.0, min(1.0, float(value)))


def source_health_label(score: float, cfg: SourceHealthConfig | None = None) -> SourceHealthLabel:
    active = cfg or SourceHealthConfig()
    normalized = clamp_unit(score)
    if normalized >= active.healthy_threshold:
        return SourceHealthLabel.HEALTHY
    if normalized >= active.degraded_threshold:
        return SourceHealthLabel.DEGRADED
    return SourceHealthLabel.BROKEN


def freshness_score(
    observed_at: datetime | None,
    *,
    now: datetime | None = None,
    expected_run_cadence_seconds: int,
) -> float:
    if observed_at is None or expected_run_cadence_seconds <= 0:
        return 0.0
    active_now = _as_utc(now or datetime.now(UTC))
    observed = _as_utc(observed_at)
    age_seconds = max(0.0, (active_now - observed).total_seconds())
    if age_seconds <= expected_run_cadence_seconds:
        return 1.0
    return clamp_unit(1.0 - ((age_seconds - expected_run_cadence_seconds) / expected_run_cadence_seconds))


def completeness_score(observed_member_count: int, expected_member_count: int, *, success: bool = True) -> float:
    if not success:
        return 0.0
    if expected_member_count <= 0:
        return 1.0 if observed_member_count > 0 else 0.0
    return clamp_unit(observed_member_count / expected_member_count)


def consistency_score(
    value_mean_f: float | None,
    rolling_mean_f: float | None,
    *,
    deviation_scale_f: float,
    success: bool = True,
) -> float:
    if not success:
        return 0.0
    if value_mean_f is None or rolling_mean_f is None:
        return 1.0
    if not isfinite(value_mean_f) or not isfinite(rolling_mean_f) or deviation_scale_f <= 0:
        return 0.0
    return clamp_unit(1.0 - (abs(value_mean_f - rolling_mean_f) / deviation_scale_f))


def score_source_health(
    observation: SourceHealthObservation,
    *,
    now: datetime | None = None,
    config: SourceHealthConfig | None = None,
) -> SourceHealthScore:
    cfg = config or SourceHealthConfig()
    cadence = observation.expected_run_cadence_seconds or cfg.expected_run_cadence_seconds
    success = 1.0 if observation.success else 0.0
    freshness = freshness_score(
        observation.observed_at,
        now=now,
        expected_run_cadence_seconds=cadence,
    )
    completeness = completeness_score(
        observation.observed_member_count,
        observation.expected_member_count,
        success=observation.success,
    )
    consistency = consistency_score(
        observation.value_mean_f,
        observation.rolling_mean_f,
        deviation_scale_f=cfg.consistency_deviation_scale_f,
        success=observation.success,
    )
    total_weight = cfg.success_weight + cfg.freshness_weight + cfg.completeness_weight + cfg.consistency_weight
    if total_weight <= 0:
        score = 0.0
    else:
        score = (
            cfg.success_weight * success
            + cfg.freshness_weight * freshness
            + cfg.completeness_weight * completeness
            + cfg.consistency_weight * consistency
        ) / total_weight
    return SourceHealthScore(
        source=observation.source,
        label=source_health_label(score, cfg),
        score=clamp_unit(score),
        success_score=success,
        freshness_score=freshness,
        completeness_score=completeness,
        consistency_score=consistency,
        observed_at=observation.observed_at,
        expected_member_count=observation.expected_member_count,
        observed_member_count=observation.observed_member_count,
        details=dict(observation.details),
    )


def aggregate_source_health(
    scores: Iterable[SourceHealthScore],
    *,
    config: SourceHealthConfig | None = None,
) -> AggregateSourceHealth:
    cfg = config or SourceHealthConfig()
    by_source = {score.source: score for score in scores}
    if not by_source:
        return AggregateSourceHealth(
            label=SourceHealthLabel.BROKEN,
            score=0.0,
            source_count=0,
            source_scores={},
        )
    aggregate = sum(score.score for score in by_source.values()) / len(by_source)
    return AggregateSourceHealth(
        label=source_health_label(aggregate, cfg),
        score=clamp_unit(aggregate),
        source_count=len(by_source),
        source_scores=by_source,
    )


def health_size_multiplier(label: SourceHealthLabel | str) -> float:
    normalized = _normalize_label(label)
    if normalized == SourceHealthLabel.HEALTHY:
        return 1.0
    if normalized == SourceHealthLabel.DEGRADED:
        return 0.5
    return 0.0


def should_pause_new_entries(
    aggregate_labels: Iterable[SourceHealthLabel | str],
    *,
    consecutive_broken_cycles: int,
) -> bool:
    required = max(1, int(consecutive_broken_cycles))
    labels = [_normalize_label(label) for label in aggregate_labels]
    if len(labels) < required:
        return False
    return all(label == SourceHealthLabel.BROKEN for label in labels[:required])


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _normalize_label(label: SourceHealthLabel | str) -> SourceHealthLabel:
    if isinstance(label, SourceHealthLabel):
        return label
    raw = str(label).strip().upper()
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return SourceHealthLabel(raw)
