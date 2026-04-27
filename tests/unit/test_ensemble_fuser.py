from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_bot.forecast.ensemble_fuser import EnsembleMember, SourceEnsemble, fuse_ensembles


def _member(source: str, temp_f: float, idx: int = 0) -> EnsembleMember:
    return EnsembleMember(
        source=source,
        run_id=f"{source}-00z",
        station_id="KNYC",
        valid_time=datetime(2026, 4, 27, 18, tzinfo=UTC),
        high_temp_f=temp_f,
        weight=1.0,
    )


def test_fuse_ensembles_uses_only_healthy_sources() -> None:
    fused = fuse_ensembles(
        [
            SourceEnsemble(source="GFS", members=[_member("GFS", 80 + idx) for idx in range(3)]),
            SourceEnsemble(source="ECMWF", members=[_member("ECMWF", 60)], health="BROKEN"),
        ]
    )

    assert fused.source_set_used == ["GFS"]
    assert fused.member_count == 3
    assert fused.mean_f == pytest.approx(81.0)


def test_fuse_ensembles_rejects_source_outliers() -> None:
    members = [_member("GFS", 80.0 + (idx % 3) * 0.1, idx) for idx in range(30)]
    members.append(_member("GFS", 130.0, 99))

    fused = fuse_ensembles([SourceEnsemble(source="GFS", members=members)], outlier_sigma=3.0)

    assert fused.member_count == 30
    assert len(fused.rejected_members) == 1
    assert fused.rejected_members[0]["reason"] == "source_outlier"


def test_fused_ensemble_groups_members_for_probability_engine() -> None:
    fused = fuse_ensembles(
        [
            SourceEnsemble(source="GFS", members=[_member("GFS", 80), _member("GFS", 81)]),
            SourceEnsemble(source="ECMWF", members=[_member("ECMWF", 79), _member("ECMWF", 78)]),
        ]
    )

    grouped = fused.source_members_by_source()

    assert grouped == {"GFS": [80.0, 81.0], "ECMWF": [79.0, 78.0]}
    assert fused.sigma_f > 0


def test_fuse_ensembles_requires_healthy_members() -> None:
    with pytest.raises(ValueError):
        fuse_ensembles([SourceEnsemble(source="AIFS", members=[_member("AIFS", float("nan"))])])
