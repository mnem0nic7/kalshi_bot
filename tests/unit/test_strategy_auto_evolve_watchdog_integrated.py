from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_auto_evolve import StrategyAutoEvolveService


PROMOTED_STRATEGY = "auto-lab"
INCUMBENT_STRATEGY = "incumbent"
MANUAL_STRATEGY = "operator-choice"


@pytest.fixture
async def watchdog_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-watchdog.db",
        kalshi_env="demo",
        strategy_auto_evolve_watchdog_min_resolved_live_fills=5,
        strategy_auto_evolve_watchdog_win_rate_degradation_bps=1000,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = StrategyAutoEvolveService(
        settings=settings,
        session_factory=session_factory,
        strategy_regression_service=object(),
        strategy_codex_service=object(),
        strategy_dashboard_service=object(),
    )

    yield SimpleNamespace(settings=settings, session_factory=session_factory, service=service)
    await engine.dispose()


@pytest.fixture
async def secondary_watchdog_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-watchdog-primary.db",
        kalshi_env="demo",
        strategy_auto_evolve_watchdog_min_resolved_live_fills=5,
        strategy_auto_evolve_watchdog_win_rate_degradation_bps=1000,
    )
    secondary_settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-watchdog-secondary.db",
        kalshi_env="demo",
    )
    engine = create_engine(settings)
    secondary_engine = create_engine(secondary_settings)
    session_factory = create_session_factory(engine)
    secondary_session_factory = create_session_factory(secondary_engine)
    await init_models(engine)
    await init_models(secondary_engine)
    service = StrategyAutoEvolveService(
        settings=settings,
        session_factory=session_factory,
        secondary_session_factory=secondary_session_factory,
        strategy_regression_service=object(),
        strategy_codex_service=object(),
        strategy_dashboard_service=object(),
    )

    yield SimpleNamespace(
        settings=settings,
        session_factory=session_factory,
        secondary_session_factory=secondary_session_factory,
        service=service,
    )
    await engine.dispose()
    await secondary_engine.dispose()


async def _seed_strategies(repo: PlatformRepository) -> None:
    for name in (INCUMBENT_STRATEGY, PROMOTED_STRATEGY, MANUAL_STRATEGY):
        await repo.create_strategy(
            name=name,
            description=f"{name} fixture",
            thresholds={"risk_min_edge_bps": 100},
            source="fixture",
        )


def _previous_assignment(*, baseline_win_rate: float = 0.50) -> dict:
    return {
        "strategy_name": INCUMBENT_STRATEGY,
        "baseline_win_rate": baseline_win_rate,
    }


def _new_assignment() -> dict:
    return {"strategy_name": PROMOTED_STRATEGY}


def _settled_fill(
    trade_id: str,
    *,
    series_ticker: str,
    strategy_name: str = PROMOTED_STRATEGY,
    settlement_result: str,
    created_at: datetime,
    yes_price: str = "0.4000",
) -> FillRecord:
    return FillRecord(
        trade_id=trade_id,
        kalshi_env="demo",
        market_ticker=f"{series_ticker}-26APR25-T70",
        side="yes",
        action="buy",
        yes_price_dollars=Decimal(yes_price),
        count_fp=Decimal("1.00"),
        strategy_code=strategy_name,
        settlement_result=settlement_result,
        raw={},
        created_at=created_at,
        updated_at=created_at,
    )


def _fills(prefix: str, *, series_ticker: str, results: list[str], created_at: datetime) -> list[FillRecord]:
    return [
        _settled_fill(
            f"{prefix}-{index}",
            series_ticker=series_ticker,
            settlement_result=result,
            created_at=created_at,
        )
        for index, result in enumerate(results, start=1)
    ]


@pytest.mark.asyncio
async def test_watchdog_extends_pending_promotion_when_followup_fills_are_insufficient(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment()},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=8),
            watchdog_due_at=now - timedelta(minutes=5),
            watchdog_extended_due_at=now + timedelta(days=6),
            trigger_source="test",
        )
        session.add_all(
            _fills(
                "extend",
                series_ticker="KXHIGHNY",
                results=["win", "loss"],
                created_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["due_count"] == 1
    assert result["processed"] == [
        {"promotion_id": promotion_id, "status": "extended", "reason": "insufficient_fills"}
    ]
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    assert record is not None
    assert record.watchdog_status == "extended"
    assert record.watchdog_extended_reason == "insufficient_fills"
    assert record.watchdog_last_eval_reason == "insufficient_fills"
    assert record.rollback_metrics["total_fills"] == 2


@pytest.mark.asyncio
async def test_watchdog_marks_extended_promotion_insufficient_data_at_t14(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment()},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=14),
            watchdog_due_at=now - timedelta(days=7),
            watchdog_extended_due_at=now - timedelta(minutes=5),
            trigger_source="test",
        )
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="extended",
            watchdog_extended_reason="insufficient_fills",
        )
        session.add_all(
            _fills(
                "insufficient",
                series_ticker="KXHIGHNY",
                results=["win", "loss"],
                created_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["due_count"] == 1
    assert result["processed"] == [
        {"promotion_id": promotion_id, "status": "insufficient_data", "reason": "insufficient_fills"}
    ]
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    assert record is not None
    assert record.watchdog_status == "insufficient_data"
    assert record.watchdog_last_eval_reason == "insufficient_data:insufficient_fills"
    assert record.rollback_metrics["total_fills"] == 2


@pytest.mark.asyncio
async def test_watchdog_insufficient_data_resolution_approve_requires_operator_audit_and_live_snapshot(
    watchdog_harness,
) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment()},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=14),
            watchdog_due_at=now - timedelta(days=7),
            watchdog_extended_due_at=now - timedelta(minutes=5),
            trigger_source="test",
        )
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="insufficient_data",
            watchdog_last_eval_reason="insufficient_data:insufficient_fills",
            rollback_metrics={"total_fills": 2},
        )
        await session.commit()
        promotion_id = promotion.id

    with pytest.raises(ValueError, match="resolved_by"):
        await watchdog_harness.service.resolve_promotion_watchdog_insufficient_data(
            promotion_id=promotion_id,
            action="approve",
            resolved_by="",
            note="Operator accepts the limited live sample.",
        )
    with pytest.raises(ValueError, match="note"):
        await watchdog_harness.service.resolve_promotion_watchdog_insufficient_data(
            promotion_id=promotion_id,
            action="approve",
            resolved_by="ops@example.com",
            note="",
        )

    result = await watchdog_harness.service.resolve_promotion_watchdog_insufficient_data(
        promotion_id=promotion_id,
        action="approve",
        resolved_by="ops@example.com",
        note="Operator accepts the limited live sample after dashboard review.",
    )

    assert result["promotion_id"] == promotion_id
    assert result["action"] == "approve"
    assert result["status"] == "passed"
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")

    assert record is not None
    assert record.watchdog_status == "passed"
    assert record.rollback_trigger is None
    assert record.resolution_data is not None
    assert record.resolution_data["action"] == "approve"
    assert record.resolution_data["resolved_by"] == "ops@example.com"
    assert record.resolution_data["note"] == "Operator accepts the limited live sample after dashboard review."
    assert "resolved_at" in record.resolution_data
    assert record.resolution_data["live_snapshot"]["watchdog_status"] == "insufficient_data"
    assert record.resolution_data["live_snapshot"]["assignments"]["KXHIGHNY"]["strategy_name"] == PROMOTED_STRATEGY
    assert assignment is not None
    assert assignment.strategy_name == PROMOTED_STRATEGY


@pytest.mark.asyncio
async def test_watchdog_insufficient_data_resolution_rollback_restores_assignments_and_stores_note(
    watchdog_harness,
) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        await repo.set_city_strategy_assignment("KXHIGHCHI", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={
                "KXHIGHNY": _previous_assignment(),
                "KXHIGHCHI": {"strategy_name": None, "baseline_win_rate": 0.50},
            },
            new_city_assignments={
                "KXHIGHNY": _new_assignment(),
                "KXHIGHCHI": _new_assignment(),
            },
            promoted_at=now - timedelta(days=14),
            watchdog_due_at=now - timedelta(days=7),
            watchdog_extended_due_at=now - timedelta(minutes=5),
            trigger_source="test",
        )
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="insufficient_data",
            watchdog_last_eval_reason="insufficient_data:insufficient_fills",
            rollback_metrics={"total_fills": 2},
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.resolve_promotion_watchdog_insufficient_data(
        promotion_id=promotion_id,
        action="rollback",
        resolved_by="ops@example.com",
        note="Operator rejected the promotion because live evidence remained too thin.",
    )

    assert result["promotion_id"] == promotion_id
    assert result["action"] == "rollback"
    assert result["status"] == "rolled_back"
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)
        restored_assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        deleted_assignment = await repo.get_city_strategy_assignment("KXHIGHCHI")
        events = await repo.list_city_assignment_events(
            promotion_id=promotion_id,
        )

    assert record is not None
    assert record.watchdog_status == "rolled_back"
    assert record.rollback_trigger == "manual_insufficient_data_review"
    assert record.rollback_details["restored"] == [
        {"series_ticker": "KXHIGHNY", "restored_strategy": INCUMBENT_STRATEGY},
        {"series_ticker": "KXHIGHCHI", "restored_strategy": None},
    ]
    assert record.resolution_data is not None
    assert record.resolution_data["action"] == "rollback"
    assert record.resolution_data["resolved_by"] == "ops@example.com"
    assert record.resolution_data["note"] == "Operator rejected the promotion because live evidence remained too thin."
    assert record.resolution_data["live_snapshot"]["assignments"]["KXHIGHNY"]["strategy_name"] == PROMOTED_STRATEGY
    assert restored_assignment is not None
    assert restored_assignment.strategy_name == INCUMBENT_STRATEGY
    assert deleted_assignment is None
    assert {event.series_ticker for event in events} == {"KXHIGHNY", "KXHIGHCHI"}
    assert {event.event_type for event in events} == {"rollback_restore", "rollback_delete"}


@pytest.mark.asyncio
async def test_watchdog_passes_when_post_promotion_metrics_clear_gates(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment(baseline_win_rate=0.50)},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=8),
            watchdog_due_at=now - timedelta(minutes=5),
            watchdog_extended_due_at=now + timedelta(days=6),
            trigger_source="test",
        )
        session.add_all(
            _fills(
                "pass",
                series_ticker="KXHIGHNY",
                results=["win", "win", "win", "win", "loss"],
                created_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["due_count"] == 1
    assert result["processed"] == [{"promotion_id": promotion_id, "status": "passed", "reason": "passed"}]
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")

    assert record is not None
    assert record.watchdog_status == "passed"
    assert record.rollback_metrics["aggregate"]["resolved_live_fills"] == 5
    assert record.rollback_metrics["aggregate"]["post_win_rate"] == pytest.approx(0.80)
    assert assignment is not None
    assert assignment.strategy_name == PROMOTED_STRATEGY


@pytest.mark.asyncio
async def test_watchdog_ignores_unrelated_execution_strategy_fills(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment(baseline_win_rate=0.50)},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=8),
            watchdog_due_at=now - timedelta(minutes=5),
            watchdog_extended_due_at=now + timedelta(days=6),
            trigger_source="test",
        )
        session.add_all(
            [
                _settled_fill(
                    f"arb-{index}",
                    series_ticker="KXHIGHNY",
                    strategy_name="ARB",
                    settlement_result="win",
                    created_at=now - timedelta(hours=1),
                )
                for index in range(5)
            ]
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["processed"] == [
        {"promotion_id": promotion_id, "status": "extended", "reason": "insufficient_fills"}
    ]
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    assert record is not None
    assert record.rollback_metrics["total_fills"] == 0


@pytest.mark.asyncio
async def test_watchdog_rollback_restores_assignment_and_writes_rollback_event(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={"KXHIGHNY": _previous_assignment(baseline_win_rate=0.90)},
            new_city_assignments={"KXHIGHNY": _new_assignment()},
            promoted_at=now - timedelta(days=8),
            watchdog_due_at=now - timedelta(minutes=5),
            watchdog_extended_due_at=now + timedelta(days=6),
            trigger_source="test",
        )
        session.add_all(
            _fills(
                "rollback",
                series_ticker="KXHIGHNY",
                results=["loss", "loss", "loss", "loss", "loss"],
                created_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["due_count"] == 1
    assert result["processed"][0]["status"] == "rolled_back"
    assert result["processed"][0]["reason"] == "aggregate_win_rate_breach"
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        events = await repo.list_city_assignment_events(
            promotion_id=promotion_id,
            event_type="rollback_restore",
        )

    assert record is not None
    assert record.watchdog_status == "rolled_back"
    assert record.rollback_trigger == "aggregate_win_rate_breach"
    assert record.rollback_details["restored"] == [
        {"series_ticker": "KXHIGHNY", "restored_strategy": INCUMBENT_STRATEGY}
    ]
    assert assignment is not None
    assert assignment.strategy_name == INCUMBENT_STRATEGY
    assert assignment.assigned_by == "watchdog_rollback"
    assert len(events) == 1
    assert events[0].series_ticker == "KXHIGHNY"
    assert events[0].previous_strategy == PROMOTED_STRATEGY
    assert events[0].new_strategy == INCUMBENT_STRATEGY
    assert events[0].actor == "strategy_auto_evolve"


@pytest.mark.asyncio
async def test_watchdog_rollback_skips_city_with_manual_override(watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        await _seed_strategies(repo)
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        await repo.set_city_strategy_assignment("KXHIGHCHI", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={
                "KXHIGHNY": _previous_assignment(baseline_win_rate=0.90),
                "KXHIGHCHI": _previous_assignment(baseline_win_rate=0.90),
            },
            new_city_assignments={
                "KXHIGHNY": _new_assignment(),
                "KXHIGHCHI": _new_assignment(),
            },
            promoted_at=now - timedelta(days=8),
            watchdog_due_at=now - timedelta(minutes=5),
            watchdog_extended_due_at=now + timedelta(days=6),
            trigger_source="test",
        )
        await repo.set_city_strategy_assignment("KXHIGHCHI", MANUAL_STRATEGY, assigned_by="operator")
        await repo.record_city_assignment_event(
            series_ticker="KXHIGHCHI",
            previous_strategy=PROMOTED_STRATEGY,
            new_strategy=MANUAL_STRATEGY,
            event_type="manual_override",
            actor="operator",
            promotion_id=promotion.id,
            kalshi_env=watchdog_harness.settings.kalshi_env,
            note="Operator replaced the auto-evolve assignment.",
        )
        session.add_all(
            _fills(
                "manual-skip",
                series_ticker="KXHIGHNY",
                results=["loss", "loss", "loss", "loss", "loss"],
                created_at=now - timedelta(hours=1),
            )
        )
        await session.commit()
        promotion_id = promotion.id

    result = await watchdog_harness.service.run_promotion_watchdog_once(trigger_source="test")

    assert result["due_count"] == 1
    assert result["processed"][0]["status"] == "rolled_back"
    async with watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)
        restored_assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        manual_assignment = await repo.get_city_strategy_assignment("KXHIGHCHI")
        rollback_events = await repo.list_city_assignment_events(
            promotion_id=promotion_id,
            event_type="rollback_restore",
        )

    assert record is not None
    assert record.watchdog_status == "rolled_back"
    assert record.rollback_details["restored"] == [
        {"series_ticker": "KXHIGHNY", "restored_strategy": INCUMBENT_STRATEGY}
    ]
    assert record.rollback_skipped_cities == [
        {
            "series_ticker": "KXHIGHCHI",
            "reason": "rollback_skipped_manual_override",
            "current_strategy": MANUAL_STRATEGY,
            "expected_strategy": PROMOTED_STRATEGY,
        }
    ]
    assert restored_assignment is not None
    assert restored_assignment.strategy_name == INCUMBENT_STRATEGY
    assert manual_assignment is not None
    assert manual_assignment.strategy_name == MANUAL_STRATEGY
    assert manual_assignment.assigned_by == "operator"
    assert {event.series_ticker for event in rollback_events} == {"KXHIGHNY"}


@pytest.mark.asyncio
async def test_secondary_assignment_sweep_skips_primary_manual_override(secondary_watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={
                "KXHIGHNY": _previous_assignment(),
                "KXHIGHCHI": _previous_assignment(),
            },
            new_city_assignments={
                "KXHIGHNY": {"new_strategy": PROMOTED_STRATEGY},
                "KXHIGHCHI": {"new_strategy": PROMOTED_STRATEGY},
            },
            promoted_at=now,
            trigger_source="test",
            secondary_sync_status="failed",
        )
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        await repo.set_city_strategy_assignment("KXHIGHCHI", MANUAL_STRATEGY, assigned_by="operator")
        await session.commit()
        promotion_id = promotion.id

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        await repo.set_city_strategy_assignment("KXHIGHNY", INCUMBENT_STRATEGY, assigned_by="stale_secondary")
        await repo.set_city_strategy_assignment("KXHIGHCHI", INCUMBENT_STRATEGY, assigned_by="stale_secondary")
        await session.commit()

    result = await secondary_watchdog_harness.service.sweep_secondary_strategy_promotion_syncs(trigger_source="test")

    assert result["due_count"] == 1
    assert result["assignment_syncs"][0]["status"] == "synced"
    assert result["assignment_syncs"][0]["applied_count"] == 1
    assert result["assignment_syncs"][0]["skipped"] == [
        {
            "series_ticker": "KXHIGHCHI",
            "reason": "primary_assignment_changed",
            "current_strategy": MANUAL_STRATEGY,
            "expected_strategy": PROMOTED_STRATEGY,
        }
    ]
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        synced_assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        skipped_assignment = await repo.get_city_strategy_assignment("KXHIGHCHI")

    assert record is not None
    assert record.secondary_sync_status == "synced"
    assert record.secondary_sync_error is None
    assert record.secondary_sync_resolution["applied_count"] == 1
    assert record.secondary_sync_resolution["skipped_count"] == 1
    assert record.promotion_details["secondary_sync_skipped_cities"] == result["assignment_syncs"][0]["skipped"]
    assert synced_assignment is not None
    assert synced_assignment.strategy_name == PROMOTED_STRATEGY
    assert skipped_assignment is not None
    assert skipped_assignment.strategy_name == INCUMBENT_STRATEGY


@pytest.mark.asyncio
async def test_secondary_assignment_sweep_skips_secondary_manual_override(secondary_watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={
                "KXHIGHNY": _previous_assignment(),
                "KXHIGHCHI": _previous_assignment(),
            },
            new_city_assignments={
                "KXHIGHNY": {"new_strategy": PROMOTED_STRATEGY},
                "KXHIGHCHI": {"new_strategy": PROMOTED_STRATEGY},
            },
            promoted_at=now,
            trigger_source="test",
            secondary_sync_status="failed",
        )
        await repo.set_city_strategy_assignment("KXHIGHNY", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        await repo.set_city_strategy_assignment("KXHIGHCHI", PROMOTED_STRATEGY, assigned_by="auto_evolve")
        await session.commit()
        promotion_id = promotion.id

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        await repo.set_city_strategy_assignment("KXHIGHNY", INCUMBENT_STRATEGY, assigned_by="stale_secondary")
        await repo.set_city_strategy_assignment("KXHIGHCHI", MANUAL_STRATEGY, assigned_by="operator")
        await session.commit()

    result = await secondary_watchdog_harness.service.sweep_secondary_strategy_promotion_syncs(trigger_source="test")

    assert result["due_count"] == 1
    sync = result["assignment_syncs"][0]
    assert sync["status"] == "synced"
    assert sync["applied_count"] == 1
    assert sync["skipped"] == [
        {
            "series_ticker": "KXHIGHCHI",
            "reason": "secondary_assignment_changed",
            "current_strategy": MANUAL_STRATEGY,
            "expected_strategy": PROMOTED_STRATEGY,
            "previous_strategy": INCUMBENT_STRATEGY,
        }
    ]
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        synced_assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        skipped_assignment = await repo.get_city_strategy_assignment("KXHIGHCHI")

    assert record is not None
    assert record.secondary_sync_status == "synced"
    assert record.secondary_sync_resolution["applied_count"] == 1
    assert record.secondary_sync_resolution["skipped_count"] == 1
    assert record.promotion_details["secondary_sync_skipped_cities"] == sync["skipped"]
    assert synced_assignment is not None
    assert synced_assignment.strategy_name == PROMOTED_STRATEGY
    assert skipped_assignment is not None
    assert skipped_assignment.strategy_name == MANUAL_STRATEGY


@pytest.mark.asyncio
async def test_secondary_rollback_sweep_uses_only_restored_entries(secondary_watchdog_harness) -> None:
    now = datetime.now(UTC)
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name=PROMOTED_STRATEGY,
            previous_city_assignments={
                "KXHIGHNY": _previous_assignment(),
                "KXHIGHCHI": _previous_assignment(),
                "KXHIGHAUS": _previous_assignment(),
            },
            new_city_assignments={
                "KXHIGHNY": _new_assignment(),
                "KXHIGHCHI": _new_assignment(),
                "KXHIGHAUS": _new_assignment(),
            },
            promoted_at=now,
            trigger_source="test",
        )
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="rolled_back",
            rollback_details={
                "restored": [
                    {"series_ticker": "KXHIGHNY", "restored_strategy": INCUMBENT_STRATEGY},
                    {"series_ticker": "KXHIGHCHI", "restored_strategy": None},
                ],
                "skipped": [
                    {
                        "series_ticker": "KXHIGHAUS",
                        "reason": "rollback_skipped_manual_override",
                    }
                ],
            },
            secondary_rollback_status="pending",
        )
        await session.commit()
        promotion_id = promotion.id

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        for ticker in ("KXHIGHNY", "KXHIGHAUS"):
            await repo.set_city_strategy_assignment(ticker, PROMOTED_STRATEGY, assigned_by="stale_secondary")
        await repo.set_city_strategy_assignment("KXHIGHCHI", MANUAL_STRATEGY, assigned_by="operator")
        await session.commit()

    result = await secondary_watchdog_harness.service.sweep_secondary_strategy_promotion_syncs(trigger_source="test")

    assert result["due_count"] == 1
    assert result["rollback_syncs"][0]["promotion_id"] == promotion_id
    assert result["rollback_syncs"][0]["status"] == "synced"
    assert result["rollback_syncs"][0]["restored_count"] == 1
    assert result["rollback_syncs"][0]["skipped_count"] == 1
    assert result["rollback_syncs"][0]["skipped"] == [
        {
            "series_ticker": "KXHIGHCHI",
            "reason": "secondary_rollback_skipped_manual_override",
            "current_strategy": MANUAL_STRATEGY,
            "expected_strategy": PROMOTED_STRATEGY,
        }
    ]
    async with secondary_watchdog_harness.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        record = await repo.get_strategy_promotion(promotion_id)

    async with secondary_watchdog_harness.secondary_session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=secondary_watchdog_harness.settings.kalshi_env)
        restored_assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        deleted_assignment = await repo.get_city_strategy_assignment("KXHIGHCHI")
        skipped_assignment = await repo.get_city_strategy_assignment("KXHIGHAUS")

    assert record is not None
    assert record.secondary_rollback_status == "synced"
    assert record.secondary_rollback_error is None
    assert record.secondary_rollback_resolution["restored_count"] == 1
    assert record.secondary_rollback_resolution["skipped_count"] == 1
    assert record.rollback_details["secondary_rollback_skipped_cities"] == result["rollback_syncs"][0]["skipped"]
    assert restored_assignment is not None
    assert restored_assignment.strategy_name == INCUMBENT_STRATEGY
    assert deleted_assignment is not None
    assert deleted_assignment.strategy_name == MANUAL_STRATEGY
    assert skipped_assignment is not None
    assert skipped_assignment.strategy_name == PROMOTED_STRATEGY
