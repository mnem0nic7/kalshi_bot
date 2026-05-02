from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.trade_behavior import evaluate_empirical_gate
from kalshi_bot.services.trade_behavior_validation import build_trade_behavior_validation_report


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
TICKER = "KXHIGHNY-26APR24-T67"


@pytest.fixture
async def trade_behavior_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior.db",
        trade_behavior_production_entry_freeze_enabled=False,
        trade_behavior_empirical_gate_min_settled_fills=2,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    yield settings, session_factory
    await engine.dispose()


def _fill(idx: int, *, settlement_result: str, yes_price: str = "0.5000") -> FillRecord:
    created_at = NOW - timedelta(days=idx)
    return FillRecord(
        id=f"fill-{idx}",
        kalshi_env="production",
        trade_id=f"trade-{idx}",
        market_ticker=TICKER,
        side="yes",
        action="buy",
        yes_price_dollars=Decimal(yes_price),
        count_fp=Decimal("10.00"),
        settlement_result=settlement_result,
        strategy_code="A",
        raw={"fee_cost": "0.2500"},
        created_at=created_at,
        updated_at=created_at,
    )


@pytest.mark.asyncio
async def test_empirical_gate_blocks_under_sampled_production_live_entries(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="production",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=False,
            yes_price_dollars=Decimal("0.5000"),
            now=NOW,
        )

    assert decision.status == "blocked"
    assert decision.reason == "empirical_gate_under_sampled"
    assert decision.blocks_live_entries is True


@pytest.mark.asyncio
async def test_empirical_gate_keeps_under_sampled_demo_shadow_report_only(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="demo",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=True,
            yes_price_dollars=Decimal("0.5000"),
            now=NOW,
        )

    assert decision.status == "shadow_only"
    assert decision.reason == "empirical_gate_under_sampled"
    assert decision.blocks_live_entries is False


@pytest.mark.asyncio
async def test_empirical_gate_blocks_negative_actual_settled_evidence(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        session.add_all([
            _fill(1, settlement_result="loss"),
            _fill(2, settlement_result="loss"),
        ])
        await session.commit()

        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="production",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=False,
            yes_price_dollars=Decimal("0.5000"),
            now=NOW,
        )

    assert decision.status == "blocked"
    assert decision.reason == "empirical_gate_negative_actual_net_pnl"
    assert decision.actual_sample_count == 2
    assert decision.actual_net_pnl is not None
    assert decision.actual_net_pnl < Decimal("0")


class FakeWatchdog:
    async def get_status(self, repo, *, kalshi_env: str | None = None):
        return {
            "kalshi_env": kalshi_env,
            "active_color": "blue",
            "kill_switch_enabled": False,
            "colors": {
                "blue": {"combined_healthy": True},
                "green": {"combined_healthy": True},
            },
        }


class FakeUnhealthyWatchdog:
    async def get_status(self, repo, *, kalshi_env: str | None = None):
        return {
            "kalshi_env": kalshi_env,
            "active_color": "blue",
            "kill_switch_enabled": False,
            "colors": {
                "blue": {"combined_healthy": False, "reason": "no heartbeat checkpoint"},
                "green": {"combined_healthy": False, "reason": "no heartbeat checkpoint"},
            },
        }


class FakeAudit:
    async def build_report(self, *, kalshi_env: str, days: int, focus: str = "money-safety"):
        return {
            "issues": [],
            "lifecycle": {
                "worst_buckets": [
                    {
                        "bucket_key": "KXHIGHNY|NY|yes|A|50-59c|delta:unknown|conf:unknown|spread:unknown",
                        "lifecycle_net_pnl": "1.0000",
                        "bucket_win_rate": 1.0,
                    }
                ]
            },
        }


class FakeAnalysis:
    async def build_report(self, *, kalshi_env: str, days: int, buckets: bool = False):
        return {
            "row_count": 1,
            "training_eligible_count": 1,
            "excluded_count": 0,
            "data_defect_count": 0,
            "top_exclusion_reasons": [],
            "buckets": [],
        }


class FakeLegacyDebtAnalysis:
    async def build_report(self, *, kalshi_env: str, days: int, buckets: bool = False):
        return {
            "row_count": 100,
            "training_eligible_count": 10,
            "excluded_count": 90,
            "data_defect_count": 90,
            "current_missing_market_snapshot_count": 0,
            "current_data_defect_count": 0,
            "legacy_coverage_debt_count": 90,
            "top_exclusion_reasons": [("missing_market_snapshot", 60), ("market_snapshot_high_leakage", 30)],
            "buckets": [],
        }


@pytest.mark.asyncio
async def test_trade_behavior_validation_aggregates_runtime_audit_analysis_and_freeze(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-validation.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="production")
            await repo.ensure_deployment_control("blue", kalshi_env="production")
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=FakeAudit(),
            trade_analysis_service=FakeAnalysis(),
            kalshi_env="production",
            days=7,
            since_hours=24,
            now=NOW,
        )
    finally:
        await engine.dispose()

    assert report["status"] == "pass"
    assert report["freeze"]["production_entry_freeze_enabled"] is True
    assert report["empirical_gate"]["readiness"]["status"] == "freeze_active"
    assert report["buy_entry_bypass"]["ticket_count"] == 0
    assert report["analysis"]["training_eligible_count"] == 1


@pytest.mark.asyncio
async def test_trade_behavior_validation_does_not_fail_unobserved_production_runtime(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-validation-runtime.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="production")
            await repo.ensure_deployment_control("blue", kalshi_env="production")
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeUnhealthyWatchdog(),
            trading_audit_service=FakeAudit(),
            trade_analysis_service=FakeAnalysis(),
            kalshi_env="production",
            days=7,
            since_hours=24,
            now=NOW,
        )
    finally:
        await engine.dispose()

    assert report["status"] == "pass"
    assert report["runtime_observed"] is False
    assert report["runtime_not_observed"] is True
    assert "active_runtime_unhealthy" not in {issue["code"] for issue in report["issues"]}


@pytest.mark.asyncio
async def test_trade_behavior_validation_treats_legacy_explained_exclusions_as_coverage_debt(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-validation-legacy.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="production")
            await repo.ensure_deployment_control("blue", kalshi_env="production")
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=FakeAudit(),
            trade_analysis_service=FakeLegacyDebtAnalysis(),
            kalshi_env="production",
            days=30,
            since_hours=24,
            now=NOW,
        )
    finally:
        await engine.dispose()

    assert report["status"] == "pass"
    assert report["analysis"]["legacy_coverage_debt_count"] == 90
    assert not [issue for issue in report["issues"] if issue["code"].startswith("analysis:")]
