from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ExecReceiptPayload
from kalshi_bot.db.models import Checkpoint, OpsEvent, OrderRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.stop_loss import StopLossService


class FakeExecutionService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def close_position(
        self,
        *,
        market_ticker: str,
        side: str,
        count_fp: Decimal,
        yes_price_dollars: Decimal,
        client_order_id: str,
        kill_switch_enabled: bool,
        active_color: str,
        subaccount: int | None = None,
    ) -> ExecReceiptPayload:
        payload: dict[str, object] = {
            "ticker": market_ticker,
            "side": side,
            "action": "sell",
            "client_order_id": client_order_id,
            "count_fp": f"{count_fp:.2f}",
            "yes_price_dollars": f"{yes_price_dollars:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
        if subaccount:
            payload["subaccount"] = subaccount
        self.calls.append(payload)
        return ExecReceiptPayload(
            status="submitted",
            external_order_id="stop-loss-order",
            client_order_id=client_order_id,
            details={"order": {"status": "submitted", "order_id": "stop-loss-order"}},
        )


class FailingExecutionService:
    async def close_position(
        self,
        *,
        market_ticker: str,
        side: str,
        count_fp: Decimal,
        yes_price_dollars: Decimal,
        client_order_id: str,
        kill_switch_enabled: bool,
        active_color: str,
        subaccount: int | None = None,
    ) -> ExecReceiptPayload:
        return ExecReceiptPayload(
            status="rejected_500",
            client_order_id=client_order_id,
            details={"error": "stop-loss submit failed"},
        )


class CancelledExecutionService(FakeExecutionService):
    async def close_position(
        self,
        *,
        market_ticker: str,
        side: str,
        count_fp: Decimal,
        yes_price_dollars: Decimal,
        client_order_id: str,
        kill_switch_enabled: bool,
        active_color: str,
        subaccount: int | None = None,
    ) -> ExecReceiptPayload:
        self.calls.append({
            "ticker": market_ticker,
            "side": side,
            "action": "sell",
            "client_order_id": client_order_id,
            "count_fp": f"{count_fp:.2f}",
            "yes_price_dollars": f"{yes_price_dollars:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        })
        return ExecReceiptPayload(
            status="canceled",
            external_order_id="cancelled-stop-loss-order",
            client_order_id=client_order_id,
            details={"order": {"status": "canceled", "order_id": "cancelled-stop-loss-order"}},
        )


@pytest.mark.asyncio
async def test_stop_loss_skips_when_color_is_inactive(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_inactive.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FakeExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control("green", initial_active_color="green")
        control.active_color = "green"
        await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T70",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("10.00"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T70",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T70"},
            yes_bid_dollars=Decimal("0.1200"),
            yes_ask_dollars=Decimal("0.1400"),
            last_trade_dollars=Decimal("0.1300"),
        )
        base = datetime.now(UTC) - timedelta(minutes=5)
        for offset_minutes, mid in enumerate(["0.9200", "0.9100", "0.9000", "0.8900", "0.8800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T70",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    triggered = await service.check_once()

    async with session_factory() as session:
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        checkpoints = list((await session.execute(select(Checkpoint))).scalars())
        await session.commit()

    assert triggered == []
    assert kalshi.calls == []
    assert ops_events == []
    assert checkpoints == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_stop_loss_uses_fractional_count_fp_payload_and_sets_reentry_checkpoint(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_payload.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FakeExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        entry_at = datetime.now(UTC) - timedelta(minutes=10)
        position = await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T70",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("10.50"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        position.created_at = entry_at
        entry_fill = await repo.save_fill(
            market_ticker="KXHIGHTSFO-26APR23-T70",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.8000"),
            count_fp=Decimal("10.50"),
            raw={},
            trade_id="entry-stop-loss-local-order",
            kalshi_env=settings.kalshi_env,
            strategy_code="A",
        )
        entry_fill.created_at = entry_at
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T70",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T70"},
            yes_bid_dollars=Decimal("0.1200"),
            yes_ask_dollars=Decimal("0.1400"),
            last_trade_dollars=Decimal("0.1300"),
        )
        base = entry_at + timedelta(minutes=1)
        for offset_minutes, mid in enumerate(["0.9200", "0.9100", "0.9000", "0.8900", "0.8800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T70",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    triggered = await service.check_once()
    triggered_again = await service.check_once()

    async with session_factory() as session:
        checkpoints = {
            cp.stream_name: cp
            for cp in (await session.execute(select(Checkpoint))).scalars()
        }
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    assert len(triggered) == 1
    assert triggered_again == []
    assert len(kalshi.calls) == 1
    assert kalshi.calls == [
        {
            "ticker": "KXHIGHTSFO-26APR23-T70",
            "side": "yes",
            "action": "sell",
            "client_order_id": kalshi.calls[0]["client_order_id"],
            "count_fp": "10.50",
            "yes_price_dollars": "0.1200",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
    ]
    assert "count" not in kalshi.calls[0]
    assert "stop_loss_submit:demo:KXHIGHTSFO-26APR23-T70" in checkpoints
    assert "stop_loss_reentry:demo:KXHIGHTSFO-26APR23-T70" in checkpoints
    assert checkpoints["stop_loss_submit:demo:KXHIGHTSFO-26APR23-T70"].payload["outcome_status"] == "submitted_pending_fill"
    assert checkpoints["stop_loss_reentry:demo:KXHIGHTSFO-26APR23-T70"].payload["client_order_id"] == kalshi.calls[0]["client_order_id"]
    sell_orders = [order for order in orders if order.action == "sell"]
    assert len(sell_orders) == 1
    assert sell_orders[0].strategy_code == "A"
    assert sell_orders[0].kalshi_order_id == "stop-loss-order"
    assert any(
        event.summary == "Stop loss triggered [trailing_stop]: KXHIGHTSFO-26APR23-T70 yes loss=86% peak=0.9200 mark=0.1300 sell=0.1200"
        for event in ops_events
    )

    await engine.dispose()


@pytest.mark.asyncio
async def test_stop_loss_terminal_unfilled_sets_retry_and_suppresses_repeat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_cancelled.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = CancelledExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        entry_at = datetime.now(UTC) - timedelta(minutes=10)
        position = await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T73",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("2.00"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        position.created_at = entry_at
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T73",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T73"},
            yes_bid_dollars=Decimal("0.1200"),
            yes_ask_dollars=Decimal("0.1400"),
            last_trade_dollars=Decimal("0.1300"),
        )
        base = entry_at + timedelta(minutes=1)
        for offset_minutes, mid in enumerate(["0.9200", "0.9100", "0.9000", "0.8900", "0.8800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T73",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    first = await service.check_once()
    second = await service.check_once()

    async with session_factory() as session:
        checkpoints = {
            cp.stream_name: cp
            for cp in (await session.execute(select(Checkpoint))).scalars()
        }
        await session.commit()

    assert len(first) == 1
    assert second == []
    assert len(kalshi.calls) == 1
    submit_cp = checkpoints["stop_loss_submit:demo:KXHIGHTSFO-26APR23-T73"]
    assert submit_cp.payload["outcome_status"] == "cancelled_or_unfilled"
    assert submit_cp.payload["next_retry_at"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_stop_loss_trailing_peak_starts_when_position_is_opened(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_position_window.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FakeExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        opened_at = datetime.now(UTC) - timedelta(minutes=5)
        position = await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T72",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("10.00"),
            average_price_dollars=Decimal("0.7000"),
            raw={},
        )
        position.created_at = opened_at - timedelta(minutes=15)
        fill = await repo.save_fill(
            market_ticker="KXHIGHTSFO-26APR23-T72",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.7000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="entry-fill-position-window",
            kalshi_env=settings.kalshi_env,
        )
        fill.created_at = opened_at
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T72",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T72"},
            yes_bid_dollars=Decimal("0.6900"),
            yes_ask_dollars=Decimal("0.7100"),
            last_trade_dollars=Decimal("0.7000"),
        )
        for observed_at, mid in [
            (opened_at - timedelta(minutes=3), "0.9200"),
            (opened_at, "0.7000"),
            (opened_at + timedelta(minutes=1), "0.7400"),
            (opened_at + timedelta(minutes=2), "0.7300"),
            (opened_at + timedelta(minutes=3), "0.7100"),
            (opened_at + timedelta(minutes=4), "0.7000"),
        ]:
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T72",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=observed_at,
            )
        await session.commit()

    triggered = await service.check_once()

    async with session_factory() as session:
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        checkpoints = list((await session.execute(select(Checkpoint))).scalars())
        await session.commit()

    assert triggered == []
    assert kalshi.calls == []
    assert ops_events == []
    assert checkpoints == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_stop_loss_submit_failure_sets_retry_without_reentry_checkpoint(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_failure.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FailingExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        entry_at = datetime.now(UTC) - timedelta(minutes=10)
        position = await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T71",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("12.25"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        position.created_at = entry_at
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T71",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T71"},
            yes_bid_dollars=Decimal("0.1200"),
            yes_ask_dollars=Decimal("0.1400"),
            last_trade_dollars=Decimal("0.1300"),
        )
        base = entry_at + timedelta(minutes=1)
        for offset_minutes, mid in enumerate(["0.9200", "0.9100", "0.9000", "0.8900", "0.8800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T71",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    triggered = await service.check_once()

    async with session_factory() as session:
        checkpoints = {
            cp.stream_name: cp
            for cp in (await session.execute(select(Checkpoint))).scalars()
        }
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        await session.commit()

    assert len(triggered) == 1
    submit_cp = checkpoints["stop_loss_submit:demo:KXHIGHTSFO-26APR23-T71"]
    assert submit_cp.payload["next_retry_at"]
    assert submit_cp.payload["outcome_status"] == "submit_failed"
    assert "stop_loss_reentry:demo:KXHIGHTSFO-26APR23-T71" not in checkpoints
    assert any("stop-loss submit failed" in str(event.payload.get("submit_error")) for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_stop_loss_no_side_uses_no_mark_in_trailing_ratio_and_summary(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_no_side.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FakeExecutionService()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue", initial_active_color="blue")
        entry_at = datetime.now(UTC) - timedelta(minutes=10)
        position = await repo.upsert_position(
            market_ticker="KXHIGHTOKC-26APR23-T83",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="no",
            count_fp=Decimal("13.31"),
            average_price_dollars=Decimal("0.7489"),
            raw={},
        )
        position.created_at = entry_at
        await repo.upsert_market_state(
            "KXHIGHTOKC-26APR23-T83",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTOKC-26APR23-T83"},
            yes_bid_dollars=Decimal("0.2800"),
            yes_ask_dollars=Decimal("0.3200"),
            last_trade_dollars=Decimal("0.3000"),
        )
        base = entry_at + timedelta(minutes=1)
        for offset_minutes, mid in enumerate(["0.2000", "0.2200", "0.2400", "0.2600", "0.2800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTOKC-26APR23-T83",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    triggered = await service.check_once()

    async with session_factory() as session:
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        await session.commit()

    assert len(triggered) == 1
    assert triggered[0]["trailing_loss_ratio"] == pytest.approx(0.125, abs=0.0001)
    assert triggered[0]["mid_mark"] == "0.7000"
    assert triggered[0]["sell_price"] == "0.3200"
    assert any(
        event.summary == "Stop loss triggered [trailing_stop]: KXHIGHTOKC-26APR23-T83 no loss=12% peak=0.8000 mark=0.7000 sell=0.3200"
        for event in ops_events
    )

    await engine.dispose()
