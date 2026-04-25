from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.fixture
async def repo_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-promotions.db")
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    await init_models(engine)

    async def _make():
        return factory()

    yield _make
    await engine.dispose()


@pytest.mark.asyncio
async def test_create_and_update_strategy_promotion_record(repo_factory) -> None:
    session_ctx = await repo_factory()
    promoted_at = datetime.now(UTC)
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name="moderate",
            previous_city_assignments={"KXHIGHNY": "aggressive"},
            new_city_assignments={"KXHIGHNY": "moderate"},
            baseline_metrics={"trade_count": 24, "win_rate": 0.75},
            promotion_details={"note": "180d winner cleared approval gates"},
            promoted_at=promoted_at,
            trigger_source="strategy_review",
        )
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="passed",
            watchdog_last_eval_at=promoted_at,
            watchdog_last_eval_reason="enough_followup_data",
            secondary_sync_status="synced",
            secondary_sync_resolution={"detail": "replicated to secondary"},
        )
        await session.commit()

        records = await repo.list_strategy_promotion_records(
            kalshi_env="demo",
            promoted_strategy_name="moderate",
        )

    assert len(records) == 1
    assert records[0].promoted_strategy_name == "moderate"
    assert records[0].previous_city_assignments == {"KXHIGHNY": "aggressive"}
    assert records[0].new_city_assignments == {"KXHIGHNY": "moderate"}
    assert records[0].watchdog_status == "passed"
    assert records[0].watchdog_last_eval_reason == "enough_followup_data"
    assert records[0].secondary_sync_status == "synced"
    assert records[0].secondary_sync_resolution == {"detail": "replicated to secondary"}
    assert records[0].promotion_details["note"].startswith("180d winner")
    assert records[0].baseline_metrics["trade_count"] == 24


@pytest.mark.asyncio
async def test_record_city_assignment_event_links_to_promotion(repo_factory) -> None:
    session_ctx = await repo_factory()
    basis_run_at = datetime.now(UTC)
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name="balanced",
            previous_city_assignments={"KXHIGHCHI": None},
            new_city_assignments={"KXHIGHCHI": "balanced"},
            trigger_source="strategy_auto_evolve",
        )
        event = await repo.record_city_assignment_event(
            series_ticker="KXHIGHCHI",
            previous_strategy=None,
            new_strategy="balanced",
            event_type="auto_evolve_assign",
            actor="auto_evolve",
            promotion_id=promotion.id,
            note="Auto-evolve accepted a stable winner.",
            event_metadata={
                "basis_run_at": basis_run_at.isoformat(),
                "window_days": 180,
                "recommendation_status": "strong_recommendation",
            },
        )
        await session.commit()

        events = await repo.list_city_assignment_events(
            kalshi_env="demo",
            series_ticker="KXHIGHCHI",
            promotion_id=promotion.id,
        )

    assert [row.id for row in events] == [event.id]
    assert events[0].promotion_id == promotion.id
    assert events[0].actor == "auto_evolve"
    assert events[0].event_type == "auto_evolve_assign"
    assert events[0].event_metadata["window_days"] == 180
    assert events[0].event_metadata["recommendation_status"] == "strong_recommendation"


@pytest.mark.asyncio
async def test_set_city_strategy_assignment_with_event_records_previous_assignment(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.set_city_strategy_assignment("KXHIGHAUS", "aggressive", assigned_by="manual_seed")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name="moderate",
            previous_city_assignments={"KXHIGHAUS": "aggressive"},
            new_city_assignments={"KXHIGHAUS": "moderate"},
        )
        assignment, event = await repo.set_city_strategy_assignment_with_event(
            "KXHIGHAUS",
            "moderate",
            assigned_by="strategies_approval",
            promotion_id=promotion.id,
            event_type="manual_assign",
            note="Operator approved latest snapshot.",
            event_metadata={"recommendation_label": "Strong recommendation"},
        )
        await session.commit()

        events = await repo.list_city_assignment_events(promotion_id=promotion.id)

    assert assignment.strategy_name == "moderate"
    assert assignment.assigned_by == "strategies_approval"
    assert event.previous_strategy == "aggressive"
    assert event.new_strategy == "moderate"
    assert event.note == "Operator approved latest snapshot."
    assert events[0].id == event.id


@pytest.mark.asyncio
async def test_city_strategy_assignments_are_scoped_by_kalshi_env(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        demo_repo = PlatformRepository(session, kalshi_env="demo")
        prod_repo = PlatformRepository(session, kalshi_env="prod")
        await demo_repo.set_city_strategy_assignment("KXHIGHNY", "demo-strategy", assigned_by="demo")
        await prod_repo.set_city_strategy_assignment("KXHIGHNY", "prod-strategy", assigned_by="prod")
        await session.commit()

        demo_assignment = await demo_repo.get_city_strategy_assignment("KXHIGHNY")
        prod_assignment = await prod_repo.get_city_strategy_assignment("KXHIGHNY")
        demo_rows = await demo_repo.list_city_strategy_assignments()
        prod_rows = await prod_repo.list_city_strategy_assignments()

    assert demo_assignment is not None
    assert demo_assignment.kalshi_env == "demo"
    assert demo_assignment.strategy_name == "demo-strategy"
    assert prod_assignment is not None
    assert prod_assignment.kalshi_env == "prod"
    assert prod_assignment.strategy_name == "prod-strategy"
    assert [row.strategy_name for row in demo_rows] == ["demo-strategy"]
    assert [row.strategy_name for row in prod_rows] == ["prod-strategy"]


@pytest.mark.asyncio
async def test_strategy_results_can_be_scoped_to_decision_corpus(repo_factory) -> None:
    session_ctx = await repo_factory()
    run_at = datetime.now(UTC)
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        strategy = await repo.create_strategy(
            name="moderate",
            description="fixture",
            thresholds={"risk_min_edge_bps": 100},
            source="fixture",
        )
        first = await repo.create_decision_corpus_build(
            version="corpus-a",
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 10),
            source={"kind": "test"},
            filters={},
        )
        second = await repo.create_decision_corpus_build(
            version="corpus-b",
            date_from=date(2026, 4, 11),
            date_to=date(2026, 4, 20),
            source={"kind": "test"},
            filters={},
        )
        await repo.mark_decision_corpus_build_successful(first.id, row_count=10)
        await repo.mark_decision_corpus_build_successful(second.id, row_count=10)
        await repo.save_strategy_results(
            [
                {
                    "strategy_id": strategy.id,
                    "corpus_build_id": first.id,
                    "run_at": run_at,
                    "date_from": first.date_from,
                    "date_to": first.date_to,
                    "series_ticker": "KXHIGHNY",
                    "rooms_evaluated": 10,
                    "trade_count": 10,
                    "resolved_trade_count": 10,
                    "win_count": 6,
                    "total_pnl_dollars": Decimal("1.00"),
                    "trade_rate": Decimal("1.0000"),
                    "win_rate": Decimal("0.6000"),
                    "avg_edge_bps": Decimal("50.0"),
                },
                {
                    "strategy_id": strategy.id,
                    "corpus_build_id": second.id,
                    "run_at": run_at,
                    "date_from": second.date_from,
                    "date_to": second.date_to,
                    "series_ticker": "KXHIGHNY",
                    "rooms_evaluated": 10,
                    "trade_count": 10,
                    "resolved_trade_count": 10,
                    "win_count": 2,
                    "total_pnl_dollars": Decimal("-1.00"),
                    "trade_rate": Decimal("1.0000"),
                    "win_rate": Decimal("0.2000"),
                    "avg_edge_bps": Decimal("50.0"),
                },
            ]
        )
        await session.commit()

        first_latest = await repo.get_latest_strategy_results(corpus_build_id=first.id)
        second_history = await repo.list_strategy_results_history(corpus_build_id=second.id)

    assert len(first_latest) == 1
    assert first_latest[0].corpus_build_id == first.id
    assert first_latest[0].win_rate == Decimal("0.6000")
    assert len(second_history) == 1
    assert second_history[0].corpus_build_id == second.id
    assert second_history[0].win_rate == Decimal("0.2000")


@pytest.mark.asyncio
async def test_record_city_assignment_event_rejects_unknown_promotion(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        with pytest.raises(KeyError, match="Strategy promotion"):
            await repo.record_city_assignment_event(
                series_ticker="KXHIGHNY",
                event_type="manual_assign",
                actor="strategies_approval",
                new_strategy="moderate",
                promotion_id=404,
            )


@pytest.mark.asyncio
async def test_strategy_promotion_rejects_invalid_status_values(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name="moderate",
            previous_city_assignments={"KXHIGHNY": None},
            new_city_assignments={"KXHIGHNY": "moderate"},
        )
        with pytest.raises(ValueError, match="watchdog_status"):
            await repo.update_strategy_promotion(promotion.id, watchdog_status="mystery")
        with pytest.raises(ValueError, match="event_type"):
            await repo.record_city_assignment_event(
                series_ticker="KXHIGHNY",
                event_type="mystery",
                actor="strategies_approval",
                new_strategy="moderate",
                promotion_id=promotion.id,
            )
