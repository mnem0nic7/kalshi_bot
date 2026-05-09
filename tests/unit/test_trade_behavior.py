from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord, OpsEvent, OrderRecord, Room, Signal, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.trade_analysis import TradeAnalysisDataset
from kalshi_bot.services.trade_behavior import evaluate_empirical_gate
from kalshi_bot.services.trade_behavior_quality import build_trade_behavior_quality_report
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
async def test_empirical_gate_labels_demo_live_entries_without_blocking(trade_behavior_harness) -> None:
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
            shadow_mode=False,
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


class FakeFullBucketAudit:
    async def build_report(self, *, kalshi_env: str, days: int, focus: str = "money-safety"):
        return {
            "issues": [],
            "lifecycle": {
                "bucket_count": 4,
                "buckets": [
                    {
                        "bucket_key": "KXHIGHNY|NY|yes|A|50-59c|delta:>=5f|conf:high|spread:000-099bps",
                        "lifecycle_net_pnl": "4.0000",
                        "bucket_net_pnl": "4.0000",
                        "bucket_sample_count": 3,
                        "bucket_win_rate": 1.0,
                    },
                    {
                        "bucket_key": "KXHIGHNY|NY|no|A|40-49c|delta:0-2f|conf:medium|spread:250-499bps",
                        "lifecycle_net_pnl": "-3.0000",
                        "bucket_net_pnl": "-3.0000",
                        "bucket_sample_count": 3,
                        "bucket_win_rate": 0.0,
                    },
                    {
                        "bucket_key": "KXHIGHTDAL|TDAL|yes|A|20-29c|delta:2-5f|conf:medium|spread:100-249bps",
                        "lifecycle_net_pnl": "0.5000",
                        "bucket_net_pnl": "0.5000",
                        "bucket_sample_count": 1,
                        "bucket_win_rate": 1.0,
                    },
                    {
                        "bucket_key": "KXHIGHCHI|CHI|yes|A|30-39c|delta:unknown|conf:unknown|spread:unknown",
                        "lifecycle_net_pnl": None,
                        "bucket_net_pnl": None,
                        "bucket_sample_count": 0,
                        "bucket_win_rate": None,
                    },
                ],
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


class FakeQualityAnalysis(FakeAnalysis):
    async def build_dataset(self, *, kalshi_env: str, days: int, now=None, limit=None):
        rows = [
            {
                "training_eligible": True,
                "lifecycle_net_pnl": "-2.0000",
                "label_win": False,
                "confidence": 0.55,
                "forecast_delta_f": 1.0,
                "market_spread_bps": 450,
                "side": "yes",
                "series_ticker": "KXHIGHNY",
                "city": "NY",
                "entry_price_band": "50-59c",
                "forecast_delta_band": "0-2f",
                "confidence_band": "medium",
                "spread_band": "250-499bps",
            },
            {
                "training_eligible": True,
                "lifecycle_net_pnl": "-1.0000",
                "label_win": False,
                "confidence": 0.70,
                "forecast_delta_f": 4.0,
                "market_spread_bps": 300,
                "side": "no",
                "series_ticker": "KXHIGHNY",
                "city": "NY",
                "entry_price_band": "40-49c",
                "forecast_delta_band": "2-5f",
                "confidence_band": "medium",
                "spread_band": "250-499bps",
            },
            {
                "training_eligible": True,
                "lifecycle_net_pnl": "3.0000",
                "label_win": True,
                "confidence": 0.82,
                "forecast_delta_f": 9.0,
                "market_spread_bps": 80,
                "side": "yes",
                "series_ticker": "KXHIGHCHI",
                "city": "CHI",
                "entry_price_band": "30-39c",
                "forecast_delta_band": ">=5f",
                "confidence_band": "high",
                "spread_band": "000-099bps",
            },
            {
                "training_eligible": True,
                "lifecycle_net_pnl": "2.0000",
                "label_win": True,
                "confidence": 0.90,
                "forecast_delta_f": 12.0,
                "market_spread_bps": 50,
                "side": "yes",
                "series_ticker": "KXHIGHCHI",
                "city": "CHI",
                "entry_price_band": "20-29c",
                "forecast_delta_band": ">=5f",
                "confidence_band": "high",
                "spread_band": "000-099bps",
            },
        ]
        return TradeAnalysisDataset(rows=rows, summary={"row_count": len(rows)})


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


class ExplodingAudit:
    async def build_report(self, **kwargs):
        raise AssertionError("fast validation must not call trading_audit_service")


class ExplodingAnalysis:
    async def build_report(self, **kwargs):
        raise AssertionError("fast validation must not call trade_analysis_service")


async def _seed_fast_reconcile(session_factory, *, reconciled_at: datetime = NOW) -> None:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.ensure_deployment_control("blue", kalshi_env="production")
        await repo.set_checkpoint("daemon_reconcile:production:blue", None, {"reconciled_at": reconciled_at.isoformat()})
        await session.commit()


def _fast_room(*, room_id: str = "fast-room", shadow_mode: bool = True) -> Room:
    return Room(
        id=room_id,
        name="Fast Gate Room",
        market_ticker=TICKER,
        kalshi_env="production",
        shadow_mode=shadow_mode,
        room_origin="auto",
        stage="complete",
        created_at=NOW - timedelta(hours=1),
        updated_at=NOW - timedelta(hours=1),
    )


@pytest.mark.asyncio
async def test_trade_behavior_fast_validation_uses_indexed_rows_without_slow_services(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-fast.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        await _seed_fast_reconcile(session_factory)
        async with session_factory() as session:
            room = _fast_room()
            session.add(room)
            session.add(
                Signal(
                    id="fast-signal",
                    room_id=room.id,
                    market_ticker=TICKER,
                    fair_yes_dollars=Decimal("0.6000"),
                    edge_bps=1000,
                    confidence=0.9,
                    summary="fast gate signal",
                    created_at=NOW - timedelta(hours=1),
                    updated_at=NOW - timedelta(hours=1),
                )
            )
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=ExplodingAudit(),
            trade_analysis_service=ExplodingAnalysis(),
            kalshi_env="production",
            days=7,
            since_hours=24,
            mode="fast",
            now=NOW,
        )
    finally:
        await engine.dispose()

    assert report["status"] == "pass"
    assert report["mode"] == "fast"
    assert report["freeze"]["production_entry_freeze_enabled"] is True
    assert report["fast_gate"]["counts"]["rooms"] == 1
    assert report["fast_gate"]["counts"]["signals"] == 1
    assert report["buy_entry_bypass"]["ticket_count"] == 0
    assert report["audit"]["mode"] == "fast"
    assert report["analysis"]["mode"] == "fast"


@pytest.mark.asyncio
async def test_trade_behavior_fast_validation_fails_live_buy_bypass_during_freeze(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-fast-bypass.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        await _seed_fast_reconcile(session_factory)
        async with session_factory() as session:
            room = _fast_room(shadow_mode=False)
            session.add(room)
            session.add(
                TradeTicketRecord(
                    id="fast-ticket",
                    room_id=room.id,
                    market_ticker=TICKER,
                    action="buy",
                    side="yes",
                    yes_price_dollars=Decimal("0.5000"),
                    count_fp=Decimal("1.00"),
                    time_in_force="immediate_or_cancel",
                    client_order_id="fast-ticket-client",
                    status="approved",
                    created_at=NOW - timedelta(hours=1),
                    updated_at=NOW - timedelta(hours=1),
                )
            )
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=ExplodingAudit(),
            trade_analysis_service=ExplodingAnalysis(),
            kalshi_env="production",
            since_hours=24,
            mode="fast",
            now=NOW,
        )
    finally:
        await engine.dispose()

    codes = {issue["code"] for issue in report["issues"]}
    assert report["status"] == "fail"
    assert "production_buy_entry_bypass" in codes
    assert report["buy_entry_bypass"]["ticket_count"] == 1


@pytest.mark.asyncio
async def test_trade_behavior_fast_validation_fails_failed_orders_critical_ops_and_stale_reconcile(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-fast-failures.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        await _seed_fast_reconcile(session_factory, reconciled_at=NOW - timedelta(minutes=10))
        async with session_factory() as session:
            session.add(
                OrderRecord(
                    id="fast-order",
                    kalshi_env="production",
                    client_order_id="fast-order-client",
                    market_ticker=TICKER,
                    status="rejected_400",
                    side="yes",
                    action="buy",
                    yes_price_dollars=Decimal("0.5000"),
                    count_fp=Decimal("1.00"),
                    created_at=NOW - timedelta(hours=1),
                    updated_at=NOW - timedelta(hours=1),
                )
            )
            session.add(
                OpsEvent(
                    id="fast-critical",
                    kalshi_env="production",
                    severity="critical",
                    source="watchdog",
                    summary="critical reconcile fault",
                    payload={},
                    created_at=NOW - timedelta(hours=1),
                    updated_at=NOW - timedelta(hours=1),
                )
            )
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=ExplodingAudit(),
            trade_analysis_service=ExplodingAnalysis(),
            kalshi_env="production",
            since_hours=24,
            mode="fast",
            now=NOW,
        )
    finally:
        await engine.dispose()

    codes = {issue["code"] for issue in report["issues"]}
    assert report["status"] == "fail"
    assert "fast_gate_failed_orders" in codes
    assert "fast_gate_critical_ops_events" in codes
    assert "fast_gate_unexpected_live_activity" in codes
    assert "daemon_reconcile_stale" in codes


@pytest.mark.asyncio
async def test_trade_behavior_fast_validation_warns_on_stale_ops_noise(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-fast-warn.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        await _seed_fast_reconcile(session_factory)
        async with session_factory() as session:
            for idx in range(10):
                session.add(
                    OpsEvent(
                        id=f"fast-warning-{idx}",
                        kalshi_env="production",
                        severity="warning",
                        source="daemon",
                        summary="stale market data warning",
                        payload={},
                        created_at=NOW - timedelta(minutes=idx),
                        updated_at=NOW - timedelta(minutes=idx),
                    )
                )
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=ExplodingAudit(),
            trade_analysis_service=ExplodingAnalysis(),
            kalshi_env="production",
            since_hours=24,
            mode="fast",
            now=NOW,
        )
    finally:
        await engine.dispose()

    codes = {issue["code"] for issue in report["issues"]}
    assert report["status"] == "warn"
    assert "fast_gate_stale_data_noise" in codes
    assert "fast_gate_ops_warning_error_noise" in codes
    assert not [issue for issue in report["issues"] if issue["severity"] == "fail"]


@pytest.mark.asyncio
async def test_trade_behavior_fast_validation_ignores_known_benign_ops_noise(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-fast-benign.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        await _seed_fast_reconcile(session_factory)
        async with session_factory() as session:
            for idx in range(12):
                session.add(
                    OpsEvent(
                        id=f"research-404-{idx}",
                        kalshi_env="production",
                        severity="error",
                        source="research",
                        summary=f"Research refresh failed for {TICKER}",
                        payload={
                            "market_ticker": TICKER,
                            "trigger_reason": "market_event",
                            "error_type": "HTTPStatusError",
                            "error": "Client error '404 Not Found' for url https://example.test/markets",
                        },
                        created_at=NOW - timedelta(minutes=idx),
                        updated_at=NOW - timedelta(minutes=idx),
                    )
                )
                session.add(
                    OpsEvent(
                        id=f"stream-keepalive-{idx}",
                        kalshi_env="production",
                        severity="error",
                        source="stream",
                        summary=f"Kalshi websocket stream error repeated {idx + 1} times",
                        payload={
                            "error_type": "ConnectionClosedError",
                            "message": "sent 1011 (internal error) keepalive ping timeout; no close frame received",
                        },
                        created_at=NOW - timedelta(minutes=idx),
                        updated_at=NOW - timedelta(minutes=idx),
                    )
                )
            session.add(
                OpsEvent(
                    id="watchdog-heartbeat-stale",
                    kalshi_env="production",
                    severity="warning",
                    source="watchdog",
                    summary="Watchdog requested recovery action",
                    payload={
                        "reason": "active color unhealthy",
                        "colors": {
                            "blue": {
                                "daemon": {
                                    "healthy": False,
                                    "reason": "heartbeat stale",
                                }
                            }
                        },
                    },
                    created_at=NOW - timedelta(minutes=1),
                    updated_at=NOW - timedelta(minutes=1),
                )
            )
            await session.commit()

        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=ExplodingAudit(),
            trade_analysis_service=ExplodingAnalysis(),
            kalshi_env="production",
            since_hours=24,
            mode="fast",
            now=NOW,
        )
    finally:
        await engine.dispose()

    assert report["status"] == "pass"
    assert report["issues"] == []
    ops = report["fast_gate"]["ops"]
    assert ops["stale_event_count"] == 0
    assert ops["noisy_sources"] == []
    assert ops["ignored_benign_event_counts"] == {
        "research_market_event_404": 12,
        "stream_transient_reconnect": 12,
        "watchdog_heartbeat_stale_recovery": 1,
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


@pytest.mark.asyncio
async def test_trade_behavior_validation_uses_full_bucket_matrix_for_readiness(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-validation-buckets.db",
        trade_behavior_production_entry_freeze_enabled=False,
        trade_behavior_empirical_gate_min_settled_fills=2,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        report = await build_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=FakeWatchdog(),
            trading_audit_service=FakeFullBucketAudit(),
            trade_analysis_service=FakeAnalysis(),
            kalshi_env="demo",
            days=30,
            since_hours=24,
            now=NOW,
        )
    finally:
        await engine.dispose()

    readiness = report["empirical_gate"]["readiness"]
    assert readiness["reported_bucket_rows_evaluated"] == 4
    assert readiness["eligible_reported_bucket_count"] == 1
    assert readiness["negative_reported_bucket_count"] == 1
    assert readiness["under_sampled_reported_bucket_count"] == 1
    assert readiness["unscored_reported_bucket_count"] == 1
    assert readiness["evidence_status_counts"] == {
        "eligible_for_live_review": 1,
        "negative_actual_pnl": 1,
        "under_sampled": 1,
        "unscored": 1,
    }


@pytest.mark.asyncio
async def test_trade_behavior_quality_reports_demo_exploration_and_filter_sweeps() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        trade_behavior_production_entry_freeze_enabled=False,
        trade_behavior_empirical_gate_min_settled_fills=2,
    )

    report = await build_trade_behavior_quality_report(
        settings=settings,
        trading_audit_service=FakeFullBucketAudit(),
        trade_analysis_service=FakeQualityAnalysis(),
        kalshi_env="demo",
        days=30,
        min_samples=2,
        limit=20,
    )

    summary = report["bucket_matrix_summary"]
    assert summary["eligible_for_live_review_count"] == 1
    assert summary["negative_actual_pnl_count"] == 1
    assert summary["under_sampled_count"] == 1
    assert summary["unscored_count"] == 1
    negative = next(row for row in report["worst_losing_cohorts"] if row["evidence_status"] == "negative_actual_pnl")
    assert negative["demo_treatment"] == "exploratory_evidence_only"
    assert report["candidate_live_review_buckets"][0]["evidence_status"] == "eligible_for_live_review"
    confidence_sweep = next(row for row in report["threshold_sweeps"] if row["filter"] == "confidence_min_0.80")
    assert confidence_sweep["sample_count"] == 2
    assert confidence_sweep["net_pnl"] == "5.0000"
    assert confidence_sweep["blocked_loss_dollars"] == "3.0000"
    assert confidence_sweep["missed_profit_dollars"] == "0.0000"
