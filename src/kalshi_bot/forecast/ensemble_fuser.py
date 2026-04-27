from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence


HEALTHY = "HEALTHY"


@dataclass(frozen=True, slots=True)
class EnsembleMember:
    source: str
    run_id: str
    station_id: str
    valid_time: datetime
    high_temp_f: float
    weight: float = 1.0

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "run_id": self.run_id,
            "station_id": self.station_id,
            "valid_time": self.valid_time.isoformat(),
            "high_temp_f": self.high_temp_f,
            "weight": self.weight,
        }


@dataclass(frozen=True, slots=True)
class SourceEnsemble:
    source: str
    members: Sequence[EnsembleMember]
    health: str = HEALTHY
    expected_member_count: int | None = None
    source_weight: float = 1.0

    def healthy(self) -> bool:
        return self.health.upper() == HEALTHY


@dataclass(frozen=True, slots=True)
class FusedEnsemble:
    members: list[EnsembleMember]
    rejected_members: list[dict[str, object]]
    source_set_used: list[str]
    mean_f: float
    sigma_f: float
    member_count: int
    source_member_counts: dict[str, int] = field(default_factory=dict)

    def source_members_by_source(self) -> dict[str, list[float]]:
        grouped: dict[str, list[float]] = {}
        for member in self.members:
            grouped.setdefault(member.source, []).append(member.high_temp_f)
        return grouped

    def to_dict(self) -> dict[str, object]:
        return {
            "mean_f": self.mean_f,
            "sigma_f": self.sigma_f,
            "member_count": self.member_count,
            "source_set_used": list(self.source_set_used),
            "source_member_counts": dict(self.source_member_counts),
            "rejected_members": list(self.rejected_members),
            "members": [member.to_dict() for member in self.members],
        }


def _finite_member(member: EnsembleMember) -> bool:
    return math.isfinite(float(member.high_temp_f)) and math.isfinite(float(member.weight)) and member.weight > 0


def _reject_source_outliers(
    source: SourceEnsemble,
    *,
    outlier_sigma: float,
) -> tuple[list[EnsembleMember], list[dict[str, object]]]:
    finite = [member for member in source.members if _finite_member(member)]
    rejected = [
        {"source": member.source, "run_id": member.run_id, "reason": "non_finite_or_non_positive_weight"}
        for member in source.members
        if not _finite_member(member)
    ]
    if len(finite) < 3:
        return finite, rejected
    temps = [member.high_temp_f for member in finite]
    sigma = statistics.pstdev(temps)
    if sigma <= 0:
        return finite, rejected
    median = statistics.median(temps)
    kept: list[EnsembleMember] = []
    for member in finite:
        if abs(member.high_temp_f - median) > outlier_sigma * sigma:
            rejected.append(
                {
                    "source": member.source,
                    "run_id": member.run_id,
                    "valid_time": member.valid_time.isoformat(),
                    "high_temp_f": member.high_temp_f,
                    "reason": "source_outlier",
                    "median_f": median,
                    "sigma_f": sigma,
                    "outlier_sigma": outlier_sigma,
                }
            )
        else:
            kept.append(member)
    return kept, rejected


def fuse_ensembles(
    sources: Sequence[SourceEnsemble],
    *,
    outlier_sigma: float = 4.0,
) -> FusedEnsemble:
    healthy_sources = [source for source in sources if source.healthy()]
    members: list[EnsembleMember] = []
    rejected: list[dict[str, object]] = []
    source_counts: dict[str, int] = {}
    for source in healthy_sources:
        kept, source_rejected = _reject_source_outliers(source, outlier_sigma=outlier_sigma)
        rejected.extend(source_rejected)
        weighted = [
            EnsembleMember(
                source=member.source,
                run_id=member.run_id,
                station_id=member.station_id,
                valid_time=member.valid_time,
                high_temp_f=member.high_temp_f,
                weight=member.weight * source.source_weight,
            )
            for member in kept
        ]
        members.extend(weighted)
        source_counts[source.source] = len(weighted)

    if not members:
        raise ValueError("cannot fuse ensemble without healthy finite members")

    total_weight = sum(member.weight for member in members)
    mean_f = sum(member.high_temp_f * member.weight for member in members) / total_weight
    variance = sum(member.weight * ((member.high_temp_f - mean_f) ** 2) for member in members) / total_weight
    sigma_f = max(math.sqrt(max(variance, 0.0)), 0.25)

    return FusedEnsemble(
        members=members,
        rejected_members=rejected,
        source_set_used=[source.source for source in healthy_sources if source_counts.get(source.source, 0) > 0],
        mean_f=mean_f,
        sigma_f=sigma_f,
        member_count=len(members),
        source_member_counts=source_counts,
    )
