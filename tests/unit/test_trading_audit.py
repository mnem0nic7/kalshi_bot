from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    FillRecord,
    MarketState,
    OpsEvent,
    OrderRecord,
    PositionRecord,
    RiskVerdictRecord,
    Room,
    Signal,
    TradeTicketRecord,
)
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.trading_audit import TradingAuditService


NOW = datetime(2026, 4, 24, 15, 0, tzinfo=UTC)


@pytest.fixture
async def audit_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/trading-audit.db",
        risk_stale_market_seconds=60,
        stop_loss_submit_cooldown_seconds=300,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    yield settings, session_factory
    await engine.dispose()


def _room(room_id: str = "room-1", ticker: str = "KXHIGHNY-26APR24-T67") -> Room:
    return Room(
        id=room_id,
        name=room_id,
        market_ticker=ticker,
        kalshi_env="production",
        shadow_mode=False,
        created_at=NOW - timedelta(hours=2),
        updated_at=NOW - timedelta(hours=2),
    )


def _fill(
    trade_id: str,
    *,
    ticker: str,
    side: str,
    action: str,
    yes_price: str,
    count: str,
    strategy_code: str | None = "A",
    settlement_result: str | None = None,
    raw: dict | None = None,
    created_at: datetime | None = None,
) -> FillRecord:
    return FillRecord(
        trade_id=trade_id,
        kalshi_env="production",
        market_ticker=ticker,
        side=side,
        action=action,
        yes_price_dollars=Decimal(yes_price),
        count_fp=Decimal(count),
        strategy_code=strategy_code,
        settlement_result=settlement_result,
        raw=raw or {"fee_cost": "0.0100"},
        created_at=created_at or NOW - timedelta(hours=1),
        updated_at=created_at or NOW - timedelta(hours=1),
    )


@pytest.mark.asyncio
async def test_trading_audit_scores_settled_and_exit_pnl(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        session.add_all([
            _room(),
            # YES buy -> sell exit: (0.70 - 0.40) * 10 = +3.00
            _fill("yes-buy", ticker="KXHIGHNY-26APR24-T67", side="yes", action="buy", yes_price="0.4000", count="10.00"),
            _fill("yes-sell", ticker="KXHIGHNY-26APR24-T67", side="yes", action="sell", yes_price="0.7000", count="10.00"),
            # NO settled win: cost is 1 - yes_price = 0.30; payout = 1.00; +3.50
            _fill(
                "no-win",
                ticker="KXHIGHCHI-26APR24-T78",
                side="no",
                action="buy",
                yes_price="0.7000",
                count="5.00",
                settlement_result="win",
            ),
            # YES unsettled buy should not affect gross realized/settled P&L.
            _fill("open-buy", ticker="KXHIGHPHIL-26APR24-T74", side="yes", action="buy", yes_price="0.5000", count="2.00"),
        ])
        await session.commit()

    report = await TradingAuditService(settings, session_factory).build_report(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    assert report["pnl"]["gross_pnl_dollars"] == "6.5000"
    assert report["pnl"]["net_pnl_dollars"] == "6.4600"
    assert report["pnl"]["unsettled_open_contracts"] == "2.00"
    assert report["fill_summary"]["total_fills"] == 4


@pytest.mark.asyncio
async def test_trading_audit_flags_money_safety_issues(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        room = _room()
        ticket = TradeTicketRecord(
            id="ticket-1",
            room_id=room.id,
            market_ticker=room.market_ticker,
            action="buy",
            side="yes",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            time_in_force="immediate_or_cancel",
            client_order_id="coid-1",
            status="proposed",
            strategy_code="A",
            created_at=NOW - timedelta(hours=2),
            updated_at=NOW - timedelta(hours=2),
        )
        order = OrderRecord(
            id="order-1",
            trade_ticket_id=ticket.id,
            kalshi_env="production",
            kalshi_order_id="kord-1",
            client_order_id="coid-1",
            market_ticker=room.market_ticker,
            status="order_id_missing",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            strategy_code="A",
            raw={},
            created_at=NOW - timedelta(hours=2),
            updated_at=NOW - timedelta(hours=2),
        )
        session.add_all([
            room,
            ticket,
            RiskVerdictRecord(
                room_id=room.id,
                ticket_id=ticket.id,
                status="approved",
                reasons=["All deterministic checks passed."],
                approved_notional_dollars=Decimal("4.0000"),
                approved_count_fp=Decimal("10.00"),
                payload={},
                created_at=NOW - timedelta(hours=2),
                updated_at=NOW - timedelta(hours=2),
            ),
            order,
            _fill(
                "missing-strategy",
                ticker=room.market_ticker,
                side="yes",
                action="buy",
                yes_price="0.4000",
                count="10.00",
                strategy_code=None,
                raw={"order_id": "kord-1", "fee_cost": "0.0100"},
            ),
            PositionRecord(
                market_ticker=room.market_ticker,
                kalshi_env="production",
                subaccount=0,
                side="yes",
                count_fp=Decimal("10.00"),
                average_price_dollars=Decimal("0.4000"),
                raw={},
                created_at=NOW - timedelta(hours=2),
                updated_at=NOW - timedelta(hours=2),
            ),
            MarketState(
                kalshi_env="production",
                market_ticker=room.market_ticker,
                yes_bid_dollars=Decimal("0.3500"),
                yes_ask_dollars=Decimal("0.4500"),
                observed_at=NOW - timedelta(minutes=10),
                snapshot={},
                created_at=NOW - timedelta(minutes=10),
                updated_at=NOW - timedelta(minutes=10),
            ),
        ])
        for idx in range(5):
            session.add(
                OpsEvent(
                    kalshi_env="production",
                    severity="warning",
                    source="stop_loss",
                    summary="Stop loss triggered",
                    payload={"market_ticker": room.market_ticker, "trigger": "trailing_stop"},
                    created_at=NOW - timedelta(minutes=idx),
                    updated_at=NOW - timedelta(minutes=idx),
                )
            )
        await session.commit()

    report = await TradingAuditService(settings, session_factory).build_report(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    issue_codes = {issue["code"] for issue in report["issues"]}
    assert "missing_fill_strategy_attribution" in issue_codes
    assert "open_positions_stale_or_missing_market_state" in issue_codes
    assert "repeated_stop_loss_events" in issue_codes
    assert "approved_trade_execution_gaps" in issue_codes
    assert "unlinked_fills_with_recoverable_order_attribution" in issue_codes
    assert report["execution_funnel"]["failed_order_count"] == 1


@pytest.mark.asyncio
async def test_trading_audit_reports_selected_signal_funnel_gaps(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        room_selected = _room("room-selected", "KXHIGHNY-26APR24-T67")
        room_stand_down = _room("room-stand-down", "KXHIGHCHI-26APR24-T78")
        room_blocked_candidate = _room("room-blocked-candidate", "KXHIGHNY-26APR24-T69")
        room_terminal_blocked = _room("room-terminal-blocked", "KXHIGHLAX-26APR24-T66")
        session.add_all([
            room_selected,
            room_stand_down,
            room_blocked_candidate,
            room_terminal_blocked,
            Signal(
                room_id=room_selected.id,
                market_ticker=room_selected.market_ticker,
                fair_yes_dollars=Decimal("0.7200"),
                edge_bps=420,
                confidence=0.82,
                summary="Selected YES",
                payload={
                    "evaluation_outcome": "candidate_selected",
                    "recommended_side": "yes",
                    "candidate_trace": {"outcome": "candidate_selected", "selected_side": "yes"},
                },
                created_at=NOW - timedelta(minutes=20),
                updated_at=NOW - timedelta(minutes=20),
            ),
            Signal(
                room_id=room_stand_down.id,
                market_ticker=room_stand_down.market_ticker,
                fair_yes_dollars=Decimal("0.5100"),
                edge_bps=40,
                confidence=0.61,
                summary="Stand down",
                payload={
                    "evaluation_outcome": "pre_risk_filtered",
                    "recommended_side": None,
                    "stand_down_reason": "spread_too_wide",
                },
                created_at=NOW - timedelta(minutes=10),
                updated_at=NOW - timedelta(minutes=10),
            ),
            Signal(
                room_id=room_blocked_candidate.id,
                market_ticker=room_blocked_candidate.market_ticker,
                fair_yes_dollars=Decimal("0.6500"),
                edge_bps=900,
                confidence=0.74,
                summary="Selected but blocked",
                payload={
                    "evaluation_outcome": "pre_risk_filtered",
                    "recommended_side": "yes",
                    "stand_down_reason": "insufficient_forecast_separation",
                    "forecast_delta_f": 1.5,
                    "candidate_trace": {
                        "outcome": "candidate_selected",
                        "selected_side": "yes",
                        "selected_edge_bps": 900,
                        "yes": {
                            "status": "selected",
                            "side": "yes",
                            "edge_bps": 900,
                            "quality_adjusted_edge_bps": 875,
                            "spread_bps": 400,
                        },
                    },
                },
                created_at=NOW - timedelta(minutes=5),
                updated_at=NOW - timedelta(minutes=5),
            ),
            Signal(
                room_id=room_terminal_blocked.id,
                market_ticker=room_terminal_blocked.market_ticker,
                fair_yes_dollars=Decimal("0.1200"),
                edge_bps=1200,
                confidence=0.91,
                summary="Selected but terminal",
                payload={
                    "evaluation_outcome": "pre_risk_filtered",
                    "recommended_side": "no",
                    "stand_down_reason": "resolved_contract",
                    "forecast_delta_f": -60.0,
                    "candidate_trace": {
                        "outcome": "candidate_selected",
                        "selected_side": "no",
                        "selected_edge_bps": 1200,
                        "no": {
                            "status": "selected",
                            "side": "no",
                            "edge_bps": 1200,
                            "quality_adjusted_edge_bps": 1175,
                            "spread_bps": 100,
                        },
                    },
                },
                created_at=NOW - timedelta(minutes=4),
                updated_at=NOW - timedelta(minutes=4),
            ),
        ])
        await session.commit()

    report = await TradingAuditService(settings, session_factory).build_report(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    assert report["signal_funnel"]["signals"] == 4
    assert report["signal_funnel"]["candidate_selected"] == 1
    assert report["signal_funnel"]["selected_without_ticket_count"] == 1
    assert report["signal_funnel"]["outcome_counts"] == {
        "candidate_selected": 1,
        "pre_risk_filtered": 3,
    }
    assert report["signal_funnel"]["recommended_side_counts"]["yes"] == 2
    assert report["signal_funnel"]["recommended_side_counts"]["no"] == 1
    assert report["signal_funnel"]["recommended_side_counts"]["none"] == 1
    assert {row["reason"]: row["count"] for row in report["signal_funnel"]["top_stand_down_reasons"]} == {
        "spread_too_wide": 1,
        "insufficient_forecast_separation": 1,
        "resolved_contract": 1,
    }
    assert report["signal_funnel"]["top_markets"][0]["market_ticker"] == room_selected.market_ticker
    assert report["signal_funnel"]["blocked_candidate_count"] == 2
    blocked = report["signal_funnel"]["top_blocked_candidates"][0]
    assert blocked["market_ticker"] == room_terminal_blocked.market_ticker
    assert blocked["stand_down_reason"] == "resolved_contract"
    assert report["signal_funnel"]["terminal_blocked_candidate_count"] == 1
    assert report["signal_funnel"]["non_terminal_blocked_candidate_count"] == 1
    blocked = report["signal_funnel"]["top_non_terminal_blocked_candidates"][0]
    assert blocked["market_ticker"] == room_blocked_candidate.market_ticker
    assert blocked["stand_down_reason"] == "insufficient_forecast_separation"
    assert blocked["selected_edge_bps"] == 900
    assert blocked["quality_adjusted_edge_bps"] == 875
    assert blocked["forecast_delta_f"] == 1.5
    assert report["signal_funnel"]["non_terminal_blocked_reason_rollups"] == [
        {
            "reason": "insufficient_forecast_separation",
            "count": 1,
            "max_selected_edge_bps": 900,
            "avg_selected_edge_bps": 900.0,
            "avg_quality_adjusted_edge_bps": 875.0,
            "avg_spread_bps": 400.0,
            "avg_abs_forecast_delta_f": 1.5,
        }
    ]
    assert report["signal_funnel"]["recent_selected_without_ticket"][0]["room_id"] == room_selected.id
    issue_codes = {issue["code"] for issue in report["issues"]}
    assert "selected_signal_without_trade_ticket" in issue_codes


@pytest.mark.asyncio
async def test_trading_audit_is_non_mutating(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        session.add_all([
            _room(),
            _fill("open-buy", ticker="KXHIGHNY-26APR24-T67", side="yes", action="buy", yes_price="0.5000", count="1.00"),
        ])
        await session.commit()

    async def counts() -> tuple[int, int]:
        async with session_factory() as session:
            fill_count = len(list((await session.execute(select(FillRecord))).scalars()))
            ops_count = len(list((await session.execute(select(OpsEvent))).scalars()))
            return fill_count, ops_count

    before = await counts()
    await TradingAuditService(settings, session_factory).build_report(kalshi_env="production", days=7, now=NOW)
    after = await counts()

    assert after == before


@pytest.mark.asyncio
async def test_trading_audit_repair_dry_run_reports_without_mutating(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        room = _room()
        order = OrderRecord(
            id="order-raw",
            kalshi_env="production",
            kalshi_order_id="kord-raw",
            client_order_id="coid-raw",
            market_ticker=room.market_ticker,
            status="executed",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("1.00"),
            strategy_code="A",
            raw={},
            created_at=NOW - timedelta(hours=1),
            updated_at=NOW - timedelta(hours=1),
        )
        session.add_all([
            room,
            order,
            _fill(
                "repair-raw",
                ticker=room.market_ticker,
                side="yes",
                action="buy",
                yes_price="0.4000",
                count="1.00",
                strategy_code=None,
                raw={"order_id": "kord-raw"},
            ),
        ])
        await session.commit()

    result = await TradingAuditService(settings, session_factory).repair_attribution(
        kalshi_env="production",
        days=7,
        dry_run=True,
        now=NOW,
    )

    async with session_factory() as session:
        fill = (await session.execute(select(FillRecord).where(FillRecord.trade_id == "repair-raw"))).scalar_one()

    assert result["candidate_count"] == 1
    assert result["updated_count"] == 0
    assert fill.strategy_code is None
    assert fill.order_id is None


@pytest.mark.asyncio
async def test_trading_audit_repair_apply_updates_evidence_backed_rows(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        room = _room()
        session.add_all([
            room,
            _fill(
                "repair-entry",
                ticker=room.market_ticker,
                side="yes",
                action="buy",
                yes_price="0.5000",
                count="2.00",
                strategy_code="A",
                created_at=NOW - timedelta(hours=2),
            ),
            _fill(
                "repair-exit",
                ticker=room.market_ticker,
                side="yes",
                action="sell",
                yes_price="0.3000",
                count="2.00",
                strategy_code=None,
                raw={},
                created_at=NOW - timedelta(hours=1),
            ),
        ])
        await session.commit()

    result = await TradingAuditService(settings, session_factory).repair_attribution(
        kalshi_env="production",
        days=7,
        dry_run=False,
        now=NOW,
    )

    async with session_factory() as session:
        fill = (await session.execute(select(FillRecord).where(FillRecord.trade_id == "repair-exit"))).scalar_one()

    assert result["candidate_count"] == 1
    assert result["updated_count"] == 1
    assert result["candidates"][0]["reason"] == "same_ticker_side_buy_lot"
    assert fill.strategy_code == "A"


@pytest.mark.asyncio
async def test_trading_audit_repair_recovers_orphaned_bot_room_order(audit_harness) -> None:
    settings, session_factory = audit_harness
    async with session_factory() as session:
        room = _room()
        order = OrderRecord(
            id="order-orphaned-room",
            kalshi_env="production",
            kalshi_order_id="kord-orphaned-room",
            client_order_id="room:abcdef123456",
            market_ticker=room.market_ticker,
            status="executed",
            side="no",
            action="buy",
            yes_price_dollars=Decimal("0.2000"),
            count_fp=Decimal("2.44"),
            strategy_code=None,
            raw={},
            created_at=NOW - timedelta(hours=1),
            updated_at=NOW - timedelta(hours=1),
        )
        session.add_all([
            room,
            order,
            _fill(
                "repair-orphaned-room",
                ticker=room.market_ticker,
                side="no",
                action="buy",
                yes_price="0.2000",
                count="2.44",
                strategy_code=None,
                raw={"order_id": "kord-orphaned-room"},
            ),
        ])
        await session.commit()

    result = await TradingAuditService(settings, session_factory).repair_attribution(
        kalshi_env="production",
        days=7,
        dry_run=False,
        now=NOW,
    )

    async with session_factory() as session:
        fill = (
            await session.execute(
                select(FillRecord).where(FillRecord.trade_id == "repair-orphaned-room")
            )
        ).scalar_one()
        order = (
            await session.execute(
                select(OrderRecord).where(OrderRecord.id == "order-orphaned-room")
            )
        ).scalar_one()

    assert result["candidate_count"] == 1
    assert result["updated_count"] == 1
    assert result["candidates"][0]["reason"] == "raw_order_id_match"
    assert fill.strategy_code == "A"
    assert fill.order_id == order.id
    assert order.strategy_code == "A"
