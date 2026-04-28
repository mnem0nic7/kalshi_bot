from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, DeploymentColor, RoomOrigin, TradeAction
from kalshi_bot.core.schemas import RoomCreate, TradeTicket
from kalshi_bot.db.models import DecisionCorpusRowRecord
from kalshi_bot.db.models import OpsEvent
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.decision_corpus import DecisionCorpusService
from kalshi_bot.services.fee_model import KALSHI_TAKER_FEE_V2


async def _setup(tmp_path) -> SimpleNamespace:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'decision_corpus.db'}",
        kalshi_taker_fee_rate=0.07,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    return SimpleNamespace(
        settings=settings,
        session_factory=session_factory,
        service=DecisionCorpusService(settings, session_factory),
    )


async def _seed_historical_decision(
    session_factory,
    *,
    market_ticker: str,
    series_ticker: str = "KXHIGHNY",
    station_id: str = "KNYC",
    local_market_day: str = "2026-04-20",
    checkpoint_ts: datetime = datetime(2026, 4, 20, 17, 0, tzinfo=UTC),
    recommended_side: str | None = "yes",
    target_yes_price: Decimal | None = Decimal("0.6000"),
    fair_yes: Decimal = Decimal("0.5500"),
    settlement_result: str | None = "yes",
    coverage_class: str = "full_checkpoint_coverage",
    source_kind: str = "checkpoint_archive",
    kalshi_env: str = "demo",
    forecast_delta_f: float | None = None,
    stand_down_reason: str | None = None,
) -> str:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        room = await repo.create_room(
            RoomCreate(name=f"Replay {market_ticker}", market_ticker=market_ticker),
            active_color=DeploymentColor.BLUE.value,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=kalshi_env,
            room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
            agent_pack_version="model-v1",
        )
        resolved_stand_down_reason = (
            stand_down_reason
            if stand_down_reason is not None
            else (None if recommended_side else "below_min_edge")
        )
        signal_payload = {
            "trade_regime": "standard",
            "eligibility": {
                "eligible": bool(recommended_side and target_yes_price is not None and resolved_stand_down_reason is None),
                "stand_down_reason": resolved_stand_down_reason,
            },
            "agent_pack_version": "model-v1",
            "heuristic_pack_version": "policy-v1",
        }
        if resolved_stand_down_reason is not None:
            signal_payload["stand_down_reason"] = resolved_stand_down_reason
        if recommended_side is not None:
            signal_payload["recommended_side"] = recommended_side
        if target_yes_price is not None:
            signal_payload["target_yes_price_dollars"] = str(target_yes_price)
        if forecast_delta_f is not None:
            signal_payload["forecast_delta_f"] = forecast_delta_f
        await repo.save_signal(
            room_id=room.id,
            market_ticker=market_ticker,
            fair_yes_dollars=fair_yes,
            edge_bps=700,
            confidence=0.82,
            summary="synthetic corpus fixture",
            payload=signal_payload,
        )
        if recommended_side is not None and target_yes_price is not None:
            await repo.save_trade_ticket(
                room_id=room.id,
                ticket=TradeTicket(
                    market_ticker=market_ticker,
                    action=TradeAction.BUY,
                    side=ContractSide.YES if recommended_side == "yes" else ContractSide.NO,
                    yes_price_dollars=target_yes_price,
                    count_fp=Decimal("1.00"),
                ),
                client_order_id=f"ticket-{market_ticker}",
            )
        await repo.save_artifact(
            room_id=room.id,
            artifact_type="market_snapshot",
            source="fixture",
            title="fixture market snapshot",
            payload={
                "mapping": {"station_id": station_id, "timezone_name": "America/New_York"},
                "market": {
                    "yes_bid_dollars": "0.5000",
                    "yes_ask_dollars": "0.6000",
                    "observed_at": checkpoint_ts.isoformat(),
                },
            },
        )
        await repo.save_artifact(
            room_id=room.id,
            artifact_type="weather_bundle",
            source="fixture",
            title="fixture weather bundle",
            payload={
                "mapping": {"station_id": station_id, "timezone_name": "America/New_York"},
            },
        )
        if settlement_result is not None:
            await repo.upsert_historical_settlement_label(
                market_ticker=market_ticker,
                series_ticker=series_ticker,
                local_market_day=local_market_day,
                source_kind="cli_daily",
                kalshi_result=settlement_result,
                settlement_value_dollars=Decimal("1.0000") if settlement_result == "yes" else Decimal("0.0000"),
                settlement_ts=datetime.fromisoformat(f"{local_market_day}T23:59:00+00:00"),
                crosscheck_status="matched",
                crosscheck_high_f=None,
                crosscheck_result=settlement_result,
                payload={"fixture": True},
            )
        await repo.create_historical_replay_run(
            room_id=room.id,
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            checkpoint_label="1300",
            checkpoint_ts=checkpoint_ts,
            status="completed",
            agent_pack_version="model-v1",
            payload={
                "historical_provenance": {
                    "coverage_class": coverage_class,
                    "market_source_kind": source_kind,
                    "weather_source_kind": source_kind,
                    "checkpoint_label": "1300",
                    "checkpoint_ts": checkpoint_ts.isoformat(),
                    "asof_ts": checkpoint_ts.isoformat(),
                    "station_id": station_id,
                    "timezone_name": "America/New_York",
                }
            },
        )
        await session.commit()
        return room.id


async def _seed_historical_decisions(
    session_factory,
    *,
    count: int,
    start_day: date,
    ticker_prefix: str = "KXHIGHNY",
    kalshi_env: str = "demo",
    coverage_class: str = "full_checkpoint_coverage",
    source_kind: str = "checkpoint_archive",
) -> None:
    for idx in range(count):
        day = start_day.fromordinal(start_day.toordinal() + idx)
        await _seed_historical_decision(
            session_factory,
            market_ticker=f"{ticker_prefix}-26{day.month:02d}{day.day:02d}-T{idx:03d}",
            local_market_day=day.isoformat(),
            checkpoint_ts=datetime(day.year, day.month, day.day, 17, 0, tzinfo=UTC),
            kalshi_env=kalshi_env,
            coverage_class=coverage_class,
            source_kind=source_kind,
        )


@pytest.mark.asyncio
async def test_build_creates_rows_with_pnl_nulls_support_and_provenance(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T80",
        recommended_side="yes",
        target_yes_price=Decimal("0.6000"),
        settlement_result="yes",
        forecast_delta_f=2.0,
    )
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T85",
        recommended_side="no",
        target_yes_price=Decimal("0.4000"),
        settlement_result="no",
        checkpoint_ts=datetime(2026, 4, 20, 17, 1, tzinfo=UTC),
    )
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T90",
        recommended_side=None,
        target_yes_price=None,
        settlement_result="no",
        checkpoint_ts=datetime(2026, 4, 20, 17, 2, tzinfo=UTC),
    )
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T95",
        recommended_side="yes",
        target_yes_price=Decimal("0.7000"),
        settlement_result=None,
        checkpoint_ts=datetime(2026, 4, 20, 17, 3, tzinfo=UTC),
    )

    result = await harness.service.build(
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 20),
        source="historical-replay",
        notes="fixture build",
    )

    assert result["status"] == "successful"
    assert result["row_count"] == 3
    assert result["support_distribution"] == {"supported": 0, "exploratory": 0, "insufficient": 3}

    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        rows = await repo.list_decision_corpus_rows(build_id=result["build_id"])

    by_ticker = {row.market_ticker: row for row in rows}
    yes_row = by_ticker["KXHIGHNY-26APR20-T80"]
    assert yes_row.pnl_counterfactual_target_frictionless == Decimal("0.400000")
    assert yes_row.fee_counterfactual_dollars == Decimal("0.020000")
    assert yes_row.pnl_counterfactual_target_with_fees == Decimal("0.380000")
    assert yes_row.pnl_model_fair_frictionless == Decimal("0.450000")
    assert yes_row.fee_model_version == KALSHI_TAKER_FEE_V2
    assert yes_row.source_provenance == "historical_replay_full_checkpoint"
    assert yes_row.station_id == "KNYC"
    assert yes_row.time_to_settlement_at_checkpoint_minutes is not None
    assert yes_row.diagnostics["forecast_delta_f"] == 2.0
    assert yes_row.diagnostics["abs_forecast_delta_f"] == 2.0
    assert yes_row.diagnostics["configured_min_abs_delta_f"] == float(harness.settings.strategy_min_abs_delta_f)
    assert yes_row.diagnostics["forecast_delta_gap_f"] == float(harness.settings.strategy_min_abs_delta_f) - 2.0
    assert yes_row.support_level == "L5_global"
    assert yes_row.support_status == "insufficient"
    assert yes_row.backoff_path[-1]["failed_on"] == ["n", "market_days"]

    no_row = by_ticker["KXHIGHNY-26APR20-T85"]
    assert no_row.recommended_side == "no"
    assert no_row.pnl_counterfactual_target_frictionless == Decimal("0.400000")
    assert no_row.pnl_counterfactual_target_with_fees == Decimal("0.380000")
    assert no_row.pnl_model_fair_frictionless == Decimal("0.550000")

    stand_down = by_ticker["KXHIGHNY-26APR20-T90"]
    assert stand_down.recommended_side is None
    assert stand_down.pnl_counterfactual_target_frictionless is None
    assert stand_down.pnl_counterfactual_target_with_fees is None
    assert stand_down.pnl_model_fair_frictionless is None
    assert stand_down.fee_counterfactual_dollars is None
    assert stand_down.fee_model_version is None


@pytest.mark.asyncio
async def test_inspect_build_reports_source_diagnostics(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decisions(
        harness.session_factory,
        count=30,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHPRIMARY",
        source_kind="checkpoint_archive",
    )
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26MAR31-REPAIR",
        local_market_day="2026-03-31",
        checkpoint_ts=datetime(2026, 3, 31, 17, 0, tzinfo=UTC),
        coverage_class="full_checkpoint_coverage",
        source_kind="external_forecast_archive_weather_bundle",
        forecast_delta_f=2.0,
        stand_down_reason="insufficient_forecast_separation",
    )

    result = await harness.service.build(
        date_from=date(2026, 3, 1),
        date_to=date(2026, 3, 31),
    )

    inspect = await harness.service.inspect_build(result["build_id"])
    diagnostics = inspect["source_diagnostics"]

    assert diagnostics["by_source_provenance"] == {
        "historical_replay_external_forecast_repair": 1,
        "historical_replay_full_checkpoint": 30,
    }
    assert diagnostics["by_coverage_class"] == {"full_checkpoint_coverage": 31}
    assert diagnostics["by_market_source_kind"] == {
        "checkpoint_archive": 30,
        "external_forecast_archive_weather_bundle": 1,
    }
    assert diagnostics["by_weather_source_kind"] == {
        "checkpoint_archive": 30,
        "external_forecast_archive_weather_bundle": 1,
    }
    assert diagnostics["clean_primary_rows"] == 30
    assert diagnostics["clean_primary_market_days"] == 30
    assert diagnostics["degraded_rows"] == 1
    assert diagnostics["gap_to_exploratory"] == {"clean_primary_rows": 0, "clean_primary_market_days": 0}
    assert diagnostics["gap_to_supported"] == {"clean_primary_rows": 70, "clean_primary_market_days": 0}
    assert diagnostics["forecast_separation"] == {
        "rows_with_delta": 1,
        "avg_abs_delta_f": 2.0,
        "avg_gap_f": 6.0,
        "insufficient_separation_rows": 1,
        "insufficient_separation_avg_gap_f": 6.0,
    }
    assert "historical-status --verbose" in diagnostics["next_check"]


@pytest.mark.asyncio
async def test_build_filters_historical_replay_rows_by_kalshi_env(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-DEMO",
        kalshi_env="demo",
    )
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-LIVE",
        kalshi_env="live",
        checkpoint_ts=datetime(2026, 4, 20, 17, 1, tzinfo=UTC),
    )

    result = await harness.service.build(
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 20),
        kalshi_env="demo",
    )

    assert result["row_count"] == 1
    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        rows = await repo.list_decision_corpus_rows(build_id=result["build_id"])

    assert [row.market_ticker for row in rows] == ["KXHIGHNY-26APR20-DEMO"]
    assert {row.kalshi_env for row in rows} == {"demo"}


@pytest.mark.asyncio
async def test_dry_run_is_deterministic_and_does_not_write_build_rows(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T80",
    )

    first = await harness.service.build(
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 20),
        source="historical-replay",
        dry_run=True,
    )
    second = await harness.service.build(
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 20),
        source="historical-replay",
        dry_run=True,
    )

    assert first["status"] == "dry_run"
    assert first["row_count"] == second["row_count"] == 1
    assert first["support_distribution"] == second["support_distribution"]
    assert await harness.service.list_builds() == []


@pytest.mark.asyncio
async def test_build_rejects_empty_date_range(tmp_path) -> None:
    harness = await _setup(tmp_path)

    with pytest.raises(ValueError, match="no eligible historical replay"):
        await harness.service.build(
            date_from=date(2026, 4, 20),
            date_to=date(2026, 4, 20),
            source="historical-replay",
            dry_run=True,
        )


@pytest.mark.asyncio
async def test_build_rows_are_append_only_after_completion(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T80",
    )
    result = await harness.service.build(
        date_from=date(2026, 4, 20),
        date_to=date(2026, 4, 20),
    )

    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        with pytest.raises(ValueError, match="in-progress"):
            await repo.add_decision_corpus_row(corpus_build_id=result["build_id"])


@pytest.mark.asyncio
async def test_promote_pointer_is_env_scoped_and_current_rows_follow_pointer(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decisions(
        harness.session_factory,
        count=30,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHDEMO",
    )
    await _seed_historical_decisions(
        harness.session_factory,
        count=30,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHLIVE",
        kalshi_env="live",
    )
    first = await harness.service.build(date_from=date(2026, 3, 1), date_to=date(2026, 3, 30), kalshi_env="demo")
    second = await harness.service.build(date_from=date(2026, 3, 1), date_to=date(2026, 3, 30), kalshi_env="live")

    assert (await harness.service.current(kalshi_env="demo"))["status"] == "missing"
    await harness.service.promote(first["build_id"], kalshi_env="demo", actor="test")
    await harness.service.promote(second["build_id"], kalshi_env="live", actor="test")

    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        demo = await repo.get_current_decision_corpus_build(kalshi_env="demo")
        live = await repo.get_current_decision_corpus_build(kalshi_env="live")
        demo_rows = await repo.list_current_decision_corpus_rows(kalshi_env="demo")
        live_rows = await repo.list_current_decision_corpus_rows(kalshi_env="live")

    assert demo is not None and demo.id == first["build_id"]
    assert live is not None and live.id == second["build_id"]
    assert {row.corpus_build_id for row in demo_rows} == {first["build_id"]}
    assert {row.corpus_build_id for row in live_rows} == {second["build_id"]}

    with pytest.raises(ValueError, match="kalshi_env_mismatch"):
        await harness.service.promote(first["build_id"], kalshi_env="live", actor="test")
    with pytest.raises(ValueError, match="already current"):
        await harness.service.promote(first["build_id"], kalshi_env="demo", actor="test")


@pytest.mark.asyncio
async def test_nightly_auto_promote_builds_and_promotes_when_triggers_and_gates_pass(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decisions(
        harness.session_factory,
        count=50,
        start_day=date(2026, 3, 1),
    )
    await _seed_historical_decisions(
        harness.session_factory,
        count=3,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHLIVE",
        kalshi_env="live",
    )

    result = await harness.service.nightly_auto_promote(
        kalshi_env="demo",
        now=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
    )

    assert result["status"] == "promoted"
    assert result["trigger"]["new_resolved_rooms"] == 50
    assert result["gates"]["ok"] is True
    assert result["build"]["row_count"] == 50

    current = await harness.service.current(kalshi_env="demo")
    assert current["status"] == "ok"
    assert current["build"]["id"] == result["build"]["build_id"]
    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        rows = await repo.list_current_decision_corpus_rows(kalshi_env="demo")
    assert len(rows) == 50
    assert {row.kalshi_env for row in rows} == {"demo"}


@pytest.mark.asyncio
async def test_nightly_auto_promotion_rejects_non_full_replay_provenance(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decisions(
        harness.session_factory,
        count=50,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHPARTIAL",
        coverage_class="partial_checkpoint_coverage",
    )

    result = await harness.service.nightly_auto_promote(
        kalshi_env="demo",
        now=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "source_provenance"
    assert result["trigger"]["source_provenance"]["disallowed_rows"] == 50
    assert result["trigger"]["source_provenance"]["disallowed_by_source_provenance"] == {
        "historical_replay_partial_checkpoint": 50
    }
    assert (await harness.service.current(kalshi_env="demo"))["status"] == "missing"


@pytest.mark.asyncio
async def test_nightly_auto_promotion_gate_failure_retains_current_corpus_and_logs_event(tmp_path) -> None:
    harness = await _setup(tmp_path)
    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        current = await repo.create_decision_corpus_build(
            version="current-fixture",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            source={"type": "fixture"},
            filters={},
        )
        await repo.mark_decision_corpus_build_successful(current.id, row_count=0)
        await repo.set_checkpoint(
            repo.decision_corpus_current_checkpoint_name(kalshi_env="demo"),
            current.id,
            {
                "build_id": current.id,
                "kalshi_env": "demo",
                "promoted_at": "2026-03-01T00:00:00+00:00",
            },
        )
        await repo.set_checkpoint(
            "decision_corpus_excluded_date_windows:demo",
            None,
            {
                "windows": [
                    {
                        "date_from": "2026-03-15",
                        "date_to": "2026-03-16",
                        "reason": "bad source archive",
                    }
                ]
            },
        )
        await session.commit()

    await _seed_historical_decisions(
        harness.session_factory,
        count=50,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHCHI",
    )

    result = await harness.service.nightly_auto_promote(
        kalshi_env="demo",
        now=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
    )

    assert result["status"] == "failed"
    assert result["reason"] == "promotion_gates_failed"
    assert result["retained_build_id"] == current.id
    assert result["gates"]["failed"] == ["excluded_date_windows"]

    after = await harness.service.current(kalshi_env="demo")
    assert after["build"]["id"] == current.id
    async with harness.session_factory() as session:
        events = list(
            (
                await session.execute(
                    select(OpsEvent)
                    .where(OpsEvent.source == "decision_corpus")
                    .order_by(OpsEvent.created_at.desc())
                )
            ).scalars()
        )
    assert events[0].severity == "warning"
    assert events[0].payload["event_kind"] == "decision_corpus_auto_promotion_failed"


@pytest.mark.asyncio
async def test_manual_promotion_rejects_excluded_window_overlap(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decisions(
        harness.session_factory,
        count=30,
        start_day=date(2026, 3, 1),
        ticker_prefix="KXHIGHMANUAL",
        kalshi_env="demo",
    )
    build = await harness.service.build(
        date_from=date(2026, 3, 1),
        date_to=date(2026, 3, 30),
        kalshi_env="demo",
    )
    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            "decision_corpus_excluded_date_windows:demo",
            None,
            {
                "windows": [
                    {
                        "date_from": "2026-03-15",
                        "date_to": "2026-03-16",
                        "reason": "bad source archive",
                    }
                ]
            },
        )
        await session.commit()

    with pytest.raises(ValueError, match="excluded_date_windows"):
        await harness.service.promote(build["build_id"], kalshi_env="demo", actor="test")

    assert (await harness.service.current(kalshi_env="demo"))["status"] == "missing"


@pytest.mark.asyncio
async def test_promotion_gates_require_strategy_code_attribution_threshold(tmp_path) -> None:
    harness = await _setup(tmp_path)

    class FakeAuditService:
        async def build_report(self, **kwargs):
            return {
                "fill_summary": {"total_fills": 100},
                "attribution": {"missing_fill_strategy_count": 2},
                "issues": [
                    {
                        "severity": "high",
                        "code": "missing_fill_strategy_attribution",
                        "summary": "covered by explicit attribution gate",
                    }
                ],
            }

    service = DecisionCorpusService(harness.settings, harness.session_factory, trading_audit_service=FakeAuditService())

    gates = await service._promotion_gates(
        kalshi_env="demo",
        now=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
        metrics={
            "resolved_rooms": 50,
            "settlement_coverage": 1.0,
        },
        excluded_windows=[],
    )

    assert gates["ok"] is False
    assert gates["failed"] == ["strategy_code_attribution"]
    assert gates["checks"]["strategy_code_attribution"]["actual"] == 0.98


@pytest.mark.asyncio
async def test_failed_or_in_progress_builds_cannot_promote(tmp_path) -> None:
    harness = await _setup(tmp_path)
    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        failed = await repo.create_decision_corpus_build(
            version="failed-fixture",
            date_from=date(2026, 4, 20),
            date_to=date(2026, 4, 20),
            source={"type": "fixture"},
            filters={},
        )
        await repo.mark_decision_corpus_build_failed(failed.id, failure_reason="fixture")
        in_progress = await repo.create_decision_corpus_build(
            version="in-progress-fixture",
            date_from=date(2026, 4, 20),
            date_to=date(2026, 4, 20),
            source={"type": "fixture"},
            filters={},
        )
        await repo.set_checkpoint(
            repo.decision_corpus_current_checkpoint_name(kalshi_env="demo"),
            failed.id,
            {"build_id": failed.id},
        )
        await session.commit()

    assert (await harness.service.current(kalshi_env="demo"))["status"] == "missing"
    with pytest.raises(ValueError, match="successful"):
        await harness.service.promote(failed.id, kalshi_env="demo")
    with pytest.raises(ValueError, match="successful"):
        await harness.service.promote(in_progress.id, kalshi_env="demo")


@pytest.mark.asyncio
async def test_validate_detects_target_pnl_mismatch(tmp_path) -> None:
    harness = await _setup(tmp_path)
    await _seed_historical_decision(
        harness.session_factory,
        market_ticker="KXHIGHNY-26APR20-T80",
    )
    result = await harness.service.build(date_from=date(2026, 4, 20), date_to=date(2026, 4, 20))

    async with harness.session_factory() as session:
        repo = PlatformRepository(session)
        rows = await repo.list_decision_corpus_rows(build_id=result["build_id"])
        rows[0].pnl_counterfactual_target_frictionless = Decimal("-99.000000")
        await session.commit()

    validation = await harness.service.validate_build(result["build_id"])
    assert validation["ok"] is False
    assert validation["errors"][0]["code"] == "target_pnl_mismatch"


def test_corpus_schema_has_no_win_rate_columns() -> None:
    column_names = {column.name for column in DecisionCorpusRowRecord.__table__.columns}

    assert not {"win_rate", "win_count", "loss_count"} & column_names


def test_corpus_schema_has_required_integrity_constraints() -> None:
    table = DecisionCorpusRowRecord.__table__
    constraint_names = {constraint.name for constraint in table.constraints}

    assert table.c.source_provenance.nullable is False
    assert "ck_decision_corpus_source_provenance" in constraint_names
    assert "uq_decision_corpus_row_identity" in constraint_names


def test_platform_repository_exposes_no_decision_corpus_row_mutators() -> None:
    dangerous_names = {
        "update_decision_corpus_row",
        "delete_decision_corpus_row",
        "upsert_decision_corpus_row",
        "modify_decision_corpus_pnl",
        "rewrite_decision_corpus_row",
    }

    assert all(not hasattr(PlatformRepository, name) for name in dangerous_names)
