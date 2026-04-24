"""
Integration-test clusters for services/momentum_calibration.py (Stage/Promote workflow).

CA  – sanity bounds: scale OOB, veto < 0, CI too wide → ok=False, no checkpoint written.
CB  – stage→promote round-trip: ops events emitted, checkpoint schema fields present.
CC  – promote with no pending → ok=False (non-zero exit).
CD  – reject idempotent: exit 0 regardless of whether pending exists.
CE1 – get_momentum_calibration_state fallback: no checkpoint, active-only, both.
CE2 – get_active_momentum_calibration_async fallback: partial checkpoint → per-field fallback.
CH  – stale-pending: >=24h without --force → refused; >=24h with --force → succeeds.
CI  – corpus filter: signals below edge_bps threshold excluded from corpus.
CP  – min-observations abort: stage exits ok=False, no checkpoint, no ops event.
CS  – preview is read-only: no Checkpoint or OpsEvent rows written.
"""
from __future__ import annotations

import unittest.mock
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    Checkpoint,
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    OpsEvent,
    Room,
    Signal,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.momentum_calibration import (
    MomentumCalibrationService,
    get_active_momentum_calibration_async,
    get_momentum_calibration_state,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _settings(**overrides) -> Settings:
    base: dict = {"database_url": "sqlite+aiosqlite:///:memory:"}
    base.update(overrides)
    return Settings(**base)


class _FakeKalshi:
    async def close(self) -> None:
        pass


@pytest.fixture
async def sf(tmp_path):
    settings = _settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/mc_test.db")
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    await init_models(engine)
    yield factory, settings
    await engine.dispose()


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


async def _seed_room(
    session_factory,
    *,
    room_id: str,
    market_ticker: str,
    slope_cpmin: float = 1.0,
    edge_bps: int = 1000,
    fair_yes: float = 0.95,
    settlement_value: float = 1.0,
    recommended_side: str = "yes",
    local_market_day: str = "2026-04-10",
    checkpoint_ts: datetime | None = None,
) -> None:
    if checkpoint_ts is None:
        checkpoint_ts = datetime(2026, 4, 10, 12, 0, 0, tzinfo=UTC)
    async with session_factory() as session:
        session.add(Room(id=room_id, name=f"room-{room_id}", market_ticker=market_ticker, kalshi_env="demo"))
        session.add(Signal(
            room_id=room_id,
            market_ticker=market_ticker,
            fair_yes_dollars=Decimal(str(fair_yes)),
            edge_bps=edge_bps,
            confidence=0.7,
            summary="test",
            payload={
                "momentum_slope_cents_per_min": slope_cpmin,
                "recommended_side": recommended_side,
            },
        ))
        session.add(HistoricalReplayRunRecord(
            room_id=room_id,
            market_ticker=market_ticker,
            series_ticker="KXHIGHBOS",
            local_market_day=local_market_day,
            checkpoint_label="test",
            checkpoint_ts=checkpoint_ts,
            status="completed",
            payload={},
        ))
        session.add(HistoricalSettlementLabelRecord(
            market_ticker=market_ticker,
            series_ticker="KXHIGHBOS",
            local_market_day=local_market_day,
            kalshi_result="yes" if settlement_value == 1.0 else "no",
            settlement_value_dollars=Decimal(str(settlement_value)),
            crosscheck_status="ok",
        ))
        await session.commit()


async def _seed_valid_corpus(session_factory, *, n: int = 15) -> None:
    """Seed n rooms that produce a valid fit (scale ≈ 2.0, CI narrow).

    Design: YES recommendation, slope=1.0, fyd=0.95, sv=1.0, edge_bps=1000.
      settlement_pnl = 1.0 - 0.95 = 0.05
      edge_dollars   = 1000/10000 = 0.10
      ratio          = 0.5
      slope_against  = 1.0  (adverse cohort)
      scale (OLS)    = Σx²/Σx(1-y) = n / n*0.5 = 2.0
    """
    for i in range(n):
        await _seed_room(
            session_factory,
            room_id=f"rv{i}",
            market_ticker=f"KXHIGHBOS-26APR10-T{58 + i * 2}",
            checkpoint_ts=datetime(2026, 4, 10, 12, i, 0, tzinfo=UTC),
        )


# ---------------------------------------------------------------------------
# CA: sanity bounds enforcement
# ---------------------------------------------------------------------------

_SANITY_CASES = [
    ("scale_ub", {"scale_fit": {"scale_fit": 15.0, "ci_width_fraction": 0.1, "ci_95_lo": 14.0, "ci_95_hi": 16.0}, "veto_candidates": []}, "outside bounds"),
    ("scale_lb", {"scale_fit": {"scale_fit": 0.05, "ci_width_fraction": 0.1, "ci_95_lo": 0.04, "ci_95_hi": 0.06}, "veto_candidates": []}, "outside bounds"),
    ("ci_wide", {"scale_fit": {"scale_fit": 2.0, "ci_width_fraction": 0.8, "ci_95_lo": 0.5, "ci_95_hi": 3.5}, "veto_candidates": []}, "CI width"),
    ("veto_neg", {"scale_fit": {"scale_fit": 2.0, "ci_width_fraction": 0.1, "ci_95_lo": 1.5, "ci_95_hi": 2.5}, "veto_candidates": [{"slope_against_cents_per_min": -0.5}]}, "non-negative"),
]


@pytest.mark.parametrize("case_id,fake_analysis,expected_error_fragment", _SANITY_CASES)
async def test_stage_sanity_bounds_rejects_and_writes_no_checkpoint(sf, case_id, fake_analysis, expected_error_fragment) -> None:
    session_factory, settings = sf
    await _seed_room(
        session_factory,
        room_id=f"rca-{case_id}",
        market_ticker=f"KXHIGHBOS-26APR10-T{58}",
        checkpoint_ts=datetime(2026, 4, 10, 12, 0, 0, tzinfo=UTC),
    )
    # Inject the analysis results we want to test against.
    base_analysis = {
        "corpus": {"n_total": 1, "n_with_slope": 1, "n_with_settlement": 1, "n_usable": 1, "null_slope_rate": 0},
        "slope_distribution": {},
        "buckets": [],
        "cohort_comparison": {},
        "scale_fit": {},
        "veto_candidates": [],
        "run_at": datetime.now(UTC).isoformat(),
    }
    base_analysis.update(fake_analysis)

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    with unittest.mock.patch(
        "kalshi_bot.services.momentum_calibration._deploy_analysis",
        return_value=base_analysis,
    ):
        result = await svc.stage(
            "2026-04-10",
            "2026-04-10",
            min_observations=1,
            staged_by="test",
        )

    assert result.get("ok") is False
    assert expected_error_fragment.lower() in result["error"].lower()

    async with session_factory() as session:
        n_cp = (await session.execute(select(func.count()).select_from(Checkpoint))).scalar_one()
    assert n_cp == 0


# ---------------------------------------------------------------------------
# CB: stage → promote round-trip
# ---------------------------------------------------------------------------


async def test_stage_promote_round_trip(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory)

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)

    # --- stage ---
    stage_result = await svc.stage(
        "2026-04-10",
        "2026-04-10",
        min_observations=10,
        staged_by="alice",
    )
    assert stage_result.get("ok") is True

    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"
    active_key = f"momentum_calibration:{settings.kalshi_env}"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        pending = await repo.get_checkpoint(pending_key)

    assert pending is not None
    p = pending.payload
    # Required schema fields.
    for field in [
        "momentum_weight_scale_cents_per_min",
        "momentum_slope_veto_cents_per_min",
        "momentum_weight_floor",
        "momentum_veto_staleness_gate",
        "corpus_n_usable",
        "corpus_date_from",
        "corpus_date_to",
        "ci_95_lo",
        "ci_95_hi",
        "ci_width_fraction",
        "staged_at",
        "staged_by",
        "provenance",
        "calibration_script_version",
    ]:
        assert field in p, f"missing field: {field}"
    assert p["staged_by"] == "alice"
    assert p["provenance"] == "manual"
    assert 0.1 <= p["momentum_weight_scale_cents_per_min"] <= 10.0

    # Ops event was emitted.
    async with session_factory() as session:
        events = (await session.execute(
            select(OpsEvent).where(OpsEvent.summary.like("%momentum_calibration%"))
        )).scalars().all()
    assert any("staged" in e.summary.lower() for e in events)

    # --- promote ---
    promote_result = await svc.promote(activated_by="bob")
    assert promote_result.get("ok") is True

    async with session_factory() as session:
        repo = PlatformRepository(session)
        active = await repo.get_checkpoint(active_key)
        gone = await repo.get_checkpoint(pending_key)

    assert active is not None
    assert gone is None
    assert active.payload.get("activated_by") == "bob"
    assert "activated_at" in active.payload

    async with session_factory() as session:
        events = (await session.execute(
            select(OpsEvent).where(OpsEvent.summary.like("%momentum_calibration%"))
        )).scalars().all()
    assert any("activated" in e.summary.lower() for e in events)


# ---------------------------------------------------------------------------
# CC: promote with no pending → ok=False
# ---------------------------------------------------------------------------


async def test_promote_no_pending_returns_error(sf) -> None:
    session_factory, settings = sf
    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.promote()
    assert result.get("ok") is False
    assert "no pending" in result["error"].lower()


# ---------------------------------------------------------------------------
# CD: reject idempotent
# ---------------------------------------------------------------------------


async def test_reject_idempotent_no_pending(sf) -> None:
    session_factory, settings = sf
    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.reject()
    assert result.get("ok") is True
    assert result.get("action") == "noop"


async def test_reject_removes_pending_then_noop(sf) -> None:
    session_factory, settings = sf
    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(pending_key, cursor=None, payload={"staged_at": datetime.now(UTC).isoformat()})
        await session.commit()

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)

    first = await svc.reject()
    assert first.get("ok") is True
    assert first.get("action") == "rejected"

    second = await svc.reject()
    assert second.get("ok") is True
    assert second.get("action") == "noop"


# ---------------------------------------------------------------------------
# CE1: get_momentum_calibration_state fallback
# ---------------------------------------------------------------------------


async def test_state_no_checkpoints_returns_nones(sf) -> None:
    session_factory, settings = sf
    async with session_factory() as session:
        repo = PlatformRepository(session)
        state = await get_momentum_calibration_state(repo, settings.kalshi_env)

    assert state["active"] is None
    assert state["pending"] is None


async def test_state_active_only(sf) -> None:
    session_factory, settings = sf
    active_key = f"momentum_calibration:{settings.kalshi_env}"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(active_key, cursor=None, payload={"momentum_weight_scale_cents_per_min": 1.5})
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        state = await get_momentum_calibration_state(repo, settings.kalshi_env)

    assert state["active"] is not None
    assert state["active"]["momentum_weight_scale_cents_per_min"] == 1.5
    assert state["pending"] is None


async def test_state_both_active_and_pending(sf) -> None:
    session_factory, settings = sf
    active_key = f"momentum_calibration:{settings.kalshi_env}"
    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"
    now_iso = datetime.now(UTC).isoformat()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(active_key, cursor=None, payload={"momentum_weight_scale_cents_per_min": 1.0})
        await repo.set_checkpoint(pending_key, cursor=None, payload={"staged_at": now_iso, "momentum_weight_scale_cents_per_min": 1.3})
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        state = await get_momentum_calibration_state(repo, settings.kalshi_env)

    assert state["active"]["momentum_weight_scale_cents_per_min"] == 1.0
    assert state["pending"]["momentum_weight_scale_cents_per_min"] == 1.3
    assert "pending_age_hours" in state
    assert state["pending_is_stale"] is False  # just created


# ---------------------------------------------------------------------------
# CE2: get_active_momentum_calibration_async fallback
# ---------------------------------------------------------------------------


async def test_active_calibration_no_checkpoint_uses_settings_defaults(sf) -> None:
    session_factory, settings = sf
    async with session_factory() as session:
        repo = PlatformRepository(session)
        params, checkpoint_exists = await get_active_momentum_calibration_async(repo, settings)

    assert not checkpoint_exists
    assert params.momentum_weight_scale_cents_per_min == settings.momentum_weight_scale_cents_per_min
    assert params.momentum_slope_veto_cents_per_min == settings.momentum_slope_veto_cents_per_min
    assert params.momentum_weight_floor == settings.momentum_weight_floor
    assert params.momentum_veto_staleness_gate == settings.momentum_veto_staleness_gate


async def test_active_calibration_partial_checkpoint_falls_back_per_field(sf) -> None:
    session_factory, settings = sf
    active_key = f"momentum_calibration:{settings.kalshi_env}"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            active_key,
            cursor=None,
            payload={"momentum_weight_scale_cents_per_min": 2.5},  # only scale, no other fields
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        params, checkpoint_exists = await get_active_momentum_calibration_async(repo, settings)

    assert checkpoint_exists
    # scale comes from checkpoint
    assert params.momentum_weight_scale_cents_per_min == 2.5
    # others fall back to Settings
    assert params.momentum_weight_floor == settings.momentum_weight_floor
    assert params.momentum_veto_staleness_gate == settings.momentum_veto_staleness_gate


async def test_active_calibration_full_checkpoint_uses_all_checkpoint_values(sf) -> None:
    session_factory, settings = sf
    active_key = f"momentum_calibration:{settings.kalshi_env}"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            active_key,
            cursor=None,
            payload={
                "momentum_weight_scale_cents_per_min": 1.8,
                "momentum_slope_veto_cents_per_min": 0.75,
                "momentum_weight_floor": 0.25,
                "momentum_veto_staleness_gate": 0.4,
            },
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        params, checkpoint_exists = await get_active_momentum_calibration_async(repo, settings)

    assert checkpoint_exists
    assert params.momentum_weight_scale_cents_per_min == 1.8
    assert params.momentum_slope_veto_cents_per_min == 0.75
    assert params.momentum_weight_floor == 0.25
    assert params.momentum_veto_staleness_gate == 0.4


# ---------------------------------------------------------------------------
# CH: stale-pending behavior
# ---------------------------------------------------------------------------


async def test_stage_refuses_stale_pending_without_force(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory)
    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"
    stale_ts = (datetime.now(UTC) - timedelta(hours=25)).isoformat()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(pending_key, cursor=None, payload={"staged_at": stale_ts})
        await session.commit()

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.stage("2026-04-10", "2026-04-10", min_observations=10)
    assert result.get("ok") is False
    assert "stale" in result["error"].lower()


async def test_stage_overwrites_stale_pending_with_force(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory)
    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"
    stale_ts = (datetime.now(UTC) - timedelta(hours=25)).isoformat()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(pending_key, cursor=None, payload={"staged_at": stale_ts})
        await session.commit()

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.stage("2026-04-10", "2026-04-10", min_observations=10, force=True)
    assert result.get("ok") is True


async def test_stage_overwrites_fresh_pending_without_force(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory)
    pending_key = f"pending_momentum_calibration:{settings.kalshi_env}"
    fresh_ts = (datetime.now(UTC) - timedelta(hours=2)).isoformat()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(pending_key, cursor=None, payload={"staged_at": fresh_ts})
        await session.commit()

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.stage("2026-04-10", "2026-04-10", min_observations=10)
    assert result.get("ok") is True  # fresh pending is overwritten without --force


# ---------------------------------------------------------------------------
# CI: corpus filter — edge_bps threshold
# ---------------------------------------------------------------------------


async def test_corpus_excludes_low_edge_signals(sf) -> None:
    session_factory, settings = sf
    # 3 rooms above threshold, 2 below.
    for i in range(3):
        await _seed_room(
            session_factory,
            room_id=f"ri-hi-{i}",
            market_ticker=f"KXHIGHBOS-26APR10-T{60 + i * 2}",
            edge_bps=settings.risk_min_edge_bps + 100,
            checkpoint_ts=datetime(2026, 4, 10, 12, i, 0, tzinfo=UTC),
        )
    for i in range(2):
        await _seed_room(
            session_factory,
            room_id=f"ri-lo-{i}",
            market_ticker=f"KXHIGHBOS-26APR10-T{70 + i * 2}",
            edge_bps=settings.risk_min_edge_bps - 100,
            checkpoint_ts=datetime(2026, 4, 10, 12, 10 + i, 0, tzinfo=UTC),
        )

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.preview("2026-04-10", "2026-04-10")
    assert result["corpus"]["n_total"] == 3


# ---------------------------------------------------------------------------
# CP: min-observations abort
# ---------------------------------------------------------------------------


async def test_stage_aborts_below_min_observations(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory, n=5)

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    result = await svc.stage("2026-04-10", "2026-04-10", min_observations=100)

    assert result.get("ok") is False
    assert "insufficient" in result["error"].lower()

    # No checkpoint written.
    async with session_factory() as session:
        n_cp = (await session.execute(select(func.count()).select_from(Checkpoint))).scalar_one()
    assert n_cp == 0

    # No ops event emitted.
    async with session_factory() as session:
        n_ev = (await session.execute(select(func.count()).select_from(OpsEvent))).scalar_one()
    assert n_ev == 0


# ---------------------------------------------------------------------------
# CS: preview is read-only
# ---------------------------------------------------------------------------


async def test_preview_writes_no_checkpoint_and_no_ops_event(sf) -> None:
    session_factory, settings = sf
    await _seed_valid_corpus(session_factory, n=5)

    svc = MomentumCalibrationService(session_factory, _FakeKalshi(), settings)
    await svc.preview("2026-04-10", "2026-04-10")

    async with session_factory() as session:
        n_cp = (await session.execute(select(func.count()).select_from(Checkpoint))).scalar_one()
        n_ev = (await session.execute(select(func.count()).select_from(OpsEvent))).scalar_one()

    assert n_cp == 0
    assert n_ev == 0
