"""Unit tests for services/signal_calibration.py.

Covers the pure math (Brier, log-loss, reliability curve) and the DB-aware
service that joins signals, replay runs, and settlement labels.
"""
from __future__ import annotations

import math
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    Room,
    Signal,
)
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.signal_calibration import (
    SignalCalibrationService,
    brier_score,
    calibration_from_pairs,
    log_loss,
    reliability_curve,
)


# ---------------------------------------------------------------------------
# Pure math
# ---------------------------------------------------------------------------

def test_brier_score_perfect_prediction_is_zero() -> None:
    assert brier_score([1.0, 0.0, 1.0, 0.0], [1, 0, 1, 0]) == 0.0


def test_brier_score_constant_half_on_balanced_set_is_0_25() -> None:
    assert brier_score([0.5, 0.5, 0.5, 0.5], [1, 0, 1, 0]) == 0.25


def test_brier_score_worst_case_is_one() -> None:
    assert brier_score([0.0, 1.0], [1, 0]) == 1.0


def test_brier_score_empty_returns_none() -> None:
    assert brier_score([], []) is None


def test_brier_score_mismatched_lengths_raises() -> None:
    with pytest.raises(ValueError):
        brier_score([0.5], [1, 0])


def test_log_loss_perfect_prediction_is_near_zero() -> None:
    # With eps clipping, log(1 - 1e-15) is ~1.1e-15 (not exactly zero).
    assert log_loss([1.0, 0.0], [1, 0]) < 1e-10


def test_log_loss_handles_edge_predictions_without_infinity() -> None:
    # A wrong confident prediction should not return inf due to clipping.
    result = log_loss([1.0, 0.0], [0, 1])
    assert result is not None
    assert math.isfinite(result)
    assert result > 0


def test_log_loss_empty_returns_none() -> None:
    assert log_loss([], []) is None


def test_reliability_curve_groups_predictions_into_buckets() -> None:
    # Predictions 0.05, 0.15, 0.25, ..., 0.95 → 10 separate buckets
    preds = [0.05 + i * 0.1 for i in range(10)]
    outcomes = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
    buckets = reliability_curve(preds, outcomes, n_buckets=10)
    assert len(buckets) == 10
    # Bucket 0 (0-0.1): one prediction 0.05, outcome 0 → observed_frequency 0.0
    assert buckets[0].count == 1
    assert buckets[0].observed_frequency == 0.0
    # Bucket 9 (0.9-1.0): one prediction 0.95, outcome 1 → observed_frequency 1.0
    assert buckets[9].count == 1
    assert buckets[9].observed_frequency == 1.0


def test_reliability_curve_omits_empty_buckets() -> None:
    # All predictions fall into the first two buckets.
    buckets = reliability_curve([0.05, 0.15], [0, 1], n_buckets=10)
    assert [b.lower for b in buckets] == [0.0, 0.1]


def test_reliability_curve_handles_p_exactly_one() -> None:
    # p=1.0 should land in the last bucket, not off the end.
    buckets = reliability_curve([1.0, 0.0], [1, 0], n_buckets=10)
    assert any(b.upper == 1.0 and b.count == 1 for b in buckets)
    assert any(b.lower == 0.0 and b.count == 1 for b in buckets)


def test_reliability_curve_invalid_n_buckets() -> None:
    with pytest.raises(ValueError):
        reliability_curve([0.5], [0], n_buckets=0)


def test_calibration_from_pairs_returns_summary() -> None:
    summary = calibration_from_pairs(
        [0.1, 0.5, 0.9], [0, 1, 1], bucket_label="KXHIGHNY"
    )
    assert summary.n == 3
    assert summary.brier_score is not None
    assert summary.log_loss is not None
    assert summary.bucket_label == "KXHIGHNY"
    assert len(summary.reliability) > 0


# ---------------------------------------------------------------------------
# DB-backed service
# ---------------------------------------------------------------------------

@pytest.fixture
async def session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/calib.db")
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    await init_models(engine)
    yield factory
    await engine.dispose()


async def _seed_signal_and_settlement(
    session_factory,
    *,
    room_id: str,
    market_ticker: str,
    series_ticker: str,
    local_market_day: str,
    fair_yes: Decimal,
    kalshi_result: str,
) -> None:
    """Plant one signal + settlement + replay-run row keyed by room_id."""
    async with session_factory() as session:
        room = Room(
            id=room_id,
            name=f"room-{room_id}",
            market_ticker=market_ticker,
            kalshi_env="demo",
        )
        signal = Signal(
            room_id=room_id,
            market_ticker=market_ticker,
            fair_yes_dollars=fair_yes,
            edge_bps=0,
            confidence=0.7,
            summary="test",
            payload={},
        )
        replay_run = HistoricalReplayRunRecord(
            room_id=room_id,
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            checkpoint_label="test",
            checkpoint_ts=datetime.now(UTC),
            status="completed",
            agent_pack_version=None,
            payload={},
        )
        settlement = HistoricalSettlementLabelRecord(
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            kalshi_result=kalshi_result,
            settlement_value_dollars=Decimal("1.00") if kalshi_result == "yes" else Decimal("0.00"),
            crosscheck_status="ok",
        )
        session.add(room)
        session.add(signal)
        session.add(replay_run)
        session.add(settlement)
        await session.commit()


@pytest.mark.asyncio
async def test_service_computes_overall_calibration(session_factory) -> None:
    # Seed four perfect predictions over two series.
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r1",
        market_ticker="KXHIGHNY-26APR23-T68",
        series_ticker="KXHIGHNY",
        local_market_day="2026-04-23",
        fair_yes=Decimal("1.0000"),
        kalshi_result="yes",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r2",
        market_ticker="KXHIGHNY-26APR24-T70",
        series_ticker="KXHIGHNY",
        local_market_day="2026-04-24",
        fair_yes=Decimal("0.0000"),
        kalshi_result="no",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r3",
        market_ticker="KXHIGHCHI-26APR23-T82",
        series_ticker="KXHIGHCHI",
        local_market_day="2026-04-23",
        fair_yes=Decimal("1.0000"),
        kalshi_result="yes",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r4",
        market_ticker="KXHIGHCHI-26APR24-T84",
        series_ticker="KXHIGHCHI",
        local_market_day="2026-04-24",
        fair_yes=Decimal("0.0000"),
        kalshi_result="no",
    )

    service = SignalCalibrationService(session_factory)
    summary = await service.compute_overall()
    assert summary.n == 4
    assert summary.brier_score == 0.0
    assert summary.log_loss is not None
    assert summary.log_loss < 1e-10


@pytest.mark.asyncio
async def test_service_filters_by_series_ticker(session_factory) -> None:
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r1",
        market_ticker="KXHIGHNY-26APR23-T68",
        series_ticker="KXHIGHNY",
        local_market_day="2026-04-23",
        fair_yes=Decimal("0.5000"),
        kalshi_result="yes",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r2",
        market_ticker="KXHIGHCHI-26APR23-T82",
        series_ticker="KXHIGHCHI",
        local_market_day="2026-04-23",
        fair_yes=Decimal("0.0000"),
        kalshi_result="no",
    )

    service = SignalCalibrationService(session_factory)
    ny_only = await service.compute_overall(series_ticker="KXHIGHNY")
    assert ny_only.n == 1
    chi_only = await service.compute_overall(series_ticker="KXHIGHCHI")
    assert chi_only.n == 1
    assert chi_only.brier_score == 0.0


@pytest.mark.asyncio
async def test_service_per_series_returns_one_summary_per_series(session_factory) -> None:
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r1",
        market_ticker="KXHIGHNY-26APR23-T68",
        series_ticker="KXHIGHNY",
        local_market_day="2026-04-23",
        fair_yes=Decimal("0.8000"),
        kalshi_result="yes",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r2",
        market_ticker="KXHIGHCHI-26APR23-T82",
        series_ticker="KXHIGHCHI",
        local_market_day="2026-04-23",
        fair_yes=Decimal("0.4000"),
        kalshi_result="no",
    )
    service = SignalCalibrationService(session_factory)
    summaries = await service.compute_per_series()
    labels = {s.bucket_label for s in summaries}
    assert labels == {"KXHIGHNY", "KXHIGHCHI"}


@pytest.mark.asyncio
async def test_service_per_month_buckets_by_local_market_day(session_factory) -> None:
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r1",
        market_ticker="KXHIGHNY-26MAR15-T68",
        series_ticker="KXHIGHNY",
        local_market_day="2026-03-15",
        fair_yes=Decimal("0.7000"),
        kalshi_result="yes",
    )
    await _seed_signal_and_settlement(
        session_factory,
        room_id="r2",
        market_ticker="KXHIGHNY-26APR23-T68",
        series_ticker="KXHIGHNY",
        local_market_day="2026-04-23",
        fair_yes=Decimal("0.3000"),
        kalshi_result="no",
    )
    service = SignalCalibrationService(session_factory)
    summaries = await service.compute_per_month()
    months = {s.bucket_label for s in summaries}
    assert months == {"2026-03", "2026-04"}


@pytest.mark.asyncio
async def test_service_excludes_unresolved_markets(session_factory) -> None:
    # Plant a signal with an unresolved (NULL kalshi_result) settlement label — should be excluded.
    async with session_factory() as session:
        room = Room(id="r1", name="r1", market_ticker="KXHIGHNY-26APR23-T68", kalshi_env="demo")
        signal = Signal(
            room_id="r1",
            market_ticker="KXHIGHNY-26APR23-T68",
            fair_yes_dollars=Decimal("0.5000"),
            edge_bps=0, confidence=0.5, summary="", payload={},
        )
        replay_run = HistoricalReplayRunRecord(
            room_id="r1",
            market_ticker="KXHIGHNY-26APR23-T68",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-23",
            checkpoint_label="test",
            checkpoint_ts=datetime.now(UTC),
            status="completed",
            agent_pack_version=None,
            payload={},
        )
        settlement = HistoricalSettlementLabelRecord(
            market_ticker="KXHIGHNY-26APR23-T68",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-23",
            kalshi_result=None,
            settlement_value_dollars=None,
            crosscheck_status="missing",
        )
        session.add_all([room, signal, replay_run, settlement])
        await session.commit()

    service = SignalCalibrationService(session_factory)
    summary = await service.compute_overall()
    assert summary.n == 0
    assert summary.brier_score is None
