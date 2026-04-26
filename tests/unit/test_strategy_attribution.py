"""Unit tests for strategy_code attribution on TradeTicket / Order / Fill records.

Verifies:
- save_trade_ticket stores strategy_code
- upsert_order copies strategy_code from the matching ticket when caller omits it
- upsert_fill copies strategy_code from the matching order when caller omits it
- get_fill_win_rate_30d(strategy_code=...) segregates results per strategy
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StrategyCode, TradeAction
from kalshi_bot.db.models import OrderRecord, Room
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


def _ticket(market_ticker: str, side: ContractSide = ContractSide.YES) -> TradeTicket:
    return TradeTicket(
        market_ticker=market_ticker,
        action=TradeAction.BUY,
        side=side,
        yes_price_dollars=Decimal("0.4000"),
        count_fp=Decimal("10.00"),
        capital_bucket="risky",
        time_in_force="immediate_or_cancel",
        nonce="nonce-1",
    )


@pytest.fixture
async def repo_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/attr.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async def _make():
        return session_factory()

    yield _make
    await engine.dispose()


@pytest.fixture
async def room_id(repo_factory):
    """Create a Room row so TradeTicketRecord's FK is valid."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        room = Room(
            id="room-1",
            name="test-room",
            market_ticker="KXHIGHNY-26APR23-T68",
            kalshi_env="demo",
        )
        session.add(room)
        await session.commit()
    return "room-1"


@pytest.mark.asyncio
async def test_save_trade_ticket_records_strategy_code(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        record = await repo.save_trade_ticket(
            room_id,
            _ticket("KXHIGHNY-26APR23-T68"),
            client_order_id="coid-A1",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        assert record.strategy_code == "A"


@pytest.mark.asyncio
async def test_upsert_order_inherits_strategy_code_from_ticket(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.save_trade_ticket(
            room_id,
            _ticket("KXHIGHNY-26APR23-T68"),
            client_order_id="coid-A1",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        # Caller (streaming/reconcile) does NOT pass strategy_code; repo should look it up.
        order = await repo.upsert_order(
            client_order_id="coid-A1",
            market_ticker="KXHIGHNY-26APR23-T68",
            status="submitted",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-1",
            kalshi_env="demo",
        )
        assert order.strategy_code == "A"


@pytest.mark.asyncio
async def test_save_order_repairs_stream_placeholder_with_ticket_attribution(repo_factory, room_id):
    """Execution persistence can race websocket/reconcile order ingestion.

    The stream may insert a same-client-order placeholder before the supervisor
    records its execution receipt. Saving the execution receipt must enrich that
    row, not raise a duplicate-key error and roll back the room's ticket/risk
    context.
    """
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        client_order_id = "coid-race"
        await repo.upsert_order(
            client_order_id=client_order_id,
            market_ticker="KXHIGHNY-26APR23-T68",
            status="executed",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={"source": "stream"},
            kalshi_order_id="kord-race",
            kalshi_env="demo",
        )
        ticket = await repo.save_trade_ticket(
            room_id,
            _ticket("KXHIGHNY-26APR23-T68"),
            client_order_id=client_order_id,
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )

        order = await repo.save_order(
            ticket_id=ticket.id,
            client_order_id=client_order_id,
            market_ticker="KXHIGHNY-26APR23-T68",
            status="executed",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={"source": "supervisor"},
            kalshi_order_id="kord-race",
            kalshi_env="demo",
        )
        rows = list(
            (
                await session.execute(
                    select(OrderRecord).where(
                        OrderRecord.kalshi_env == "demo",
                        OrderRecord.client_order_id == client_order_id,
                    )
                )
            ).scalars()
        )

        assert len(rows) == 1
        assert order.id == rows[0].id
        assert order.trade_ticket_id == ticket.id
        assert order.strategy_code == StrategyCode.DIRECTIONAL.value


@pytest.mark.asyncio
async def test_upsert_fill_inherits_strategy_code_from_kalshi_order(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.save_trade_ticket(
            room_id,
            _ticket("KXHIGHNY-26APR23-T68"),
            client_order_id="coid-A1",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        await repo.upsert_order(
            client_order_id="coid-A1",
            market_ticker="KXHIGHNY-26APR23-T68",
            status="submitted",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-1",
            kalshi_env="demo",
        )
        # Fill arrives via websocket — only kalshi_order_id is known, via raw payload.
        fill = await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={"order_id": "kord-1"},
            trade_id="trade-1",
            kalshi_env="demo",
        )
        assert fill.strategy_code == "A"
        assert fill.order_id is not None


@pytest.mark.asyncio
async def test_upsert_sell_fill_inherits_strategy_from_latest_same_side_buy(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="entry-fill",
            kalshi_env="demo",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )

        exit_fill = await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="sell",
            yes_price_dollars=Decimal("0.3000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="exit-fill",
            kalshi_env="demo",
        )

        assert exit_fill.strategy_code == StrategyCode.DIRECTIONAL.value


@pytest.mark.asyncio
async def test_upsert_fill_tolerates_duplicate_kalshi_order_ids(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.upsert_order(
            client_order_id="coid-A1",
            market_ticker="KXHIGHNY-26APR23-T68",
            status="submitted",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-1",
            kalshi_env="demo",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        await repo.upsert_order(
            client_order_id="coid-C1",
            market_ticker="KXHIGHNY-26APR23-T68",
            status="submitted",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-1",
            kalshi_env="demo",
            strategy_code=StrategyCode.CLEANUP.value,
        )

        fill = await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={"order_id": "kord-1"},
            trade_id="trade-1",
            kalshi_env="demo",
        )

        assert fill.strategy_code in {StrategyCode.DIRECTIONAL.value, StrategyCode.CLEANUP.value}


@pytest.mark.asyncio
async def test_daily_realized_pnl_by_strategy_settled_buys_only(repo_factory, room_id):
    """Conservative daily P&L counts BUY fills whose settlement is known plus
    matched BUY→SELL round-trips; unsettled open BUYs contribute zero."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")

        # Strategy A: one settled loss buy (paid $0.40, settled NO → lost $4.00 for 10 contracts)
        loser = await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="trade-A-loss",
            kalshi_env="demo",
            strategy_code="A",
        )
        loser.settlement_result = "loss"
        # Strategy A: one settled winning buy ($0.30 × 5 → receive $1 × 5 → +$3.50 P&L)
        winner = await repo.upsert_fill(
            market_ticker="KXHIGHCHI-26APR23-T82",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.3000"),
            count_fp=Decimal("5.00"),
            raw={},
            trade_id="trade-A-win",
            kalshi_env="demo",
            strategy_code="A",
        )
        winner.settlement_result = "win"
        # Strategy A: one unsettled open buy — must NOT contribute
        await repo.upsert_fill(
            market_ticker="KXHIGHAUS-26APR23-T90",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.6000"),
            count_fp=Decimal("20.00"),
            raw={},
            trade_id="trade-A-open",
            kalshi_env="demo",
            strategy_code="A",
        )
        # Strategy C: losing settlement — should not leak into A's number
        c_loser = await repo.upsert_fill(
            market_ticker="KXHIGHCHI-26APR23-T84",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.9800"),
            count_fp=Decimal("5.00"),
            raw={},
            trade_id="trade-C-loss",
            kalshi_env="demo",
            strategy_code="C",
        )
        c_loser.settlement_result = "loss"
        await session.flush()

        a_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code="A", kalshi_env="demo"
        )
        # A: -$4.00 (loser) + $3.50 (winner) = -$0.50
        assert a_pnl == Decimal("-0.50")

        c_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code="C", kalshi_env="demo"
        )
        # C: -$4.90 (0.98 × 5 = $4.90 cost, $0 back on loss)
        assert c_pnl == Decimal("-4.90")

        arb_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code="ARB", kalshi_env="demo"
        )
        assert arb_pnl == Decimal("0.00")


@pytest.mark.asyncio
async def test_daily_realized_pnl_by_strategy_nets_matched_buy_sell_pair(repo_factory, room_id):
    """When a BUY and SELL for the same ticker/side both exist within the window,
    the method nets them instead of waiting for settlement."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # Bought YES at $0.40, sold YES at $0.55 — realized gain of $0.15 × 10 = $1.50
        await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="trade-buy",
            kalshi_env="demo",
            strategy_code="A",
        )
        await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="sell",
            yes_price_dollars=Decimal("0.5500"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="trade-sell",
            kalshi_env="demo",
            strategy_code="A",
        )
        await session.flush()

        pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code="A", kalshi_env="demo"
        )
        assert pnl == Decimal("1.50")


@pytest.mark.asyncio
async def test_win_rate_segregates_by_strategy_code(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # Strategy A: one winning settlement buy.
        a_fill = await repo.upsert_fill(
            market_ticker="KXHIGHNY-26APR23-T68",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            trade_id="trade-A",
            kalshi_env="demo",
            strategy_code="A",
        )
        a_fill.settlement_result = "win"
        # Strategy C: one losing settlement buy.
        c_fill = await repo.upsert_fill(
            market_ticker="KXHIGHCHI-26APR23-T82",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.9800"),
            count_fp=Decimal("5.00"),
            raw={},
            trade_id="trade-C",
            kalshi_env="demo",
            strategy_code="C",
        )
        c_fill.settlement_result = "loss"
        await session.flush()

        overall = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert overall["won_contracts"] == 10.0
        assert overall["total_contracts"] == 15.0

        only_a = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="A")
        assert only_a["won_contracts"] == 10.0
        assert only_a["total_contracts"] == 10.0

        only_c = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="C")
        assert only_c["won_contracts"] == 0.0
        assert only_c["total_contracts"] == 5.0

        no_arb_yet = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="ARB")
        assert no_arb_yet["won_contracts"] == 0.0
        assert no_arb_yet["total_contracts"] == 0.0


# ---------------------------------------------------------------------------
# P2-1: loss-magnitude + Sharpe fields on the win-rate card
# ---------------------------------------------------------------------------


async def _seed_buy_with_settlement(
    repo,
    *,
    market_ticker: str,
    yes_price: str,
    count: str,
    trade_id: str,
    settlement_result: str,
    side: str = "yes",
):
    fill = await repo.upsert_fill(
        market_ticker=market_ticker,
        side=side,
        action="buy",
        yes_price_dollars=Decimal(yes_price),
        count_fp=Decimal(count),
        raw={},
        trade_id=trade_id,
        kalshi_env="demo",
        strategy_code="A",
    )
    fill.settlement_result = settlement_result
    return fill


@pytest.mark.asyncio
async def test_win_rate_empty_has_null_magnitudes(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        result = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert result["trade_count"] == 0
        assert result["avg_win_dollars"] is None
        assert result["avg_loss_dollars"] is None
        assert result["stdev_dollars"] is None
        assert result["sharpe_per_trade"] is None


@pytest.mark.asyncio
async def test_win_rate_all_wins_have_no_loss_magnitude(repo_factory, room_id):
    """When every trade wins, avg_loss_dollars is None, not zero."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # Two YES buys at $0.40 × 10 each, both settle YES → P&L = (1-0.40)*10 = $6.00 each
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHNY-26APR23-T68",
            yes_price="0.4000", count="10.00", trade_id="t1", settlement_result="win",
        )
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHCHI-26APR23-T82",
            yes_price="0.4000", count="10.00", trade_id="t2", settlement_result="win",
        )
        await session.flush()

        result = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert result["trade_count"] == 2
        assert result["win_count"] == 2
        assert result["loss_count"] == 0
        assert result["avg_win_dollars"] == pytest.approx(6.0)
        assert result["avg_loss_dollars"] is None
        # Stdev of [6.0, 6.0] is zero → Sharpe is None (guard against div-by-zero)
        assert result["stdev_dollars"] == pytest.approx(0.0)
        assert result["sharpe_per_trade"] is None


@pytest.mark.asyncio
async def test_win_rate_all_losses_have_no_win_magnitude(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # Two YES buys at $0.60 × 5, both settle NO → P&L = (0 - 0.60) * 5 = -$3.00 each
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHNY-26APR23-T68",
            yes_price="0.6000", count="5.00", trade_id="t1", settlement_result="loss",
        )
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHCHI-26APR23-T82",
            yes_price="0.6000", count="5.00", trade_id="t2", settlement_result="loss",
        )
        await session.flush()

        result = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert result["trade_count"] == 2
        assert result["win_count"] == 0
        assert result["loss_count"] == 2
        assert result["avg_win_dollars"] is None
        assert result["avg_loss_dollars"] == pytest.approx(-3.0)


@pytest.mark.asyncio
async def test_win_rate_mixed_outcomes_report_both_magnitudes_and_sharpe(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # Trade 1: YES buy at $0.40 × 10, wins → P&L = +$6.00
        # Trade 2: YES buy at $0.80 × 5, loses → P&L = -$4.00
        # Trade 3: YES buy at $0.50 × 8, wins → P&L = +$4.00
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHNY-26APR23-T68",
            yes_price="0.4000", count="10.00", trade_id="t1", settlement_result="win",
        )
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHCHI-26APR23-T82",
            yes_price="0.8000", count="5.00", trade_id="t2", settlement_result="loss",
        )
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHAUS-26APR23-T90",
            yes_price="0.5000", count="8.00", trade_id="t3", settlement_result="win",
        )
        await session.flush()

        result = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert result["trade_count"] == 3
        assert result["win_count"] == 2
        assert result["loss_count"] == 1
        # avg win = (6 + 4) / 2 = 5.0
        assert result["avg_win_dollars"] == pytest.approx(5.0)
        # avg loss = -4.0 / 1 = -4.0
        assert result["avg_loss_dollars"] == pytest.approx(-4.0)
        # stdev over [6, -4, 4]: mean=2, var=((6-2)^2+(-4-2)^2+(4-2)^2)/3 = (16+36+4)/3 = 56/3 ≈ 18.667
        # stdev ≈ sqrt(18.667) ≈ 4.320
        assert result["stdev_dollars"] == pytest.approx(4.320, abs=0.01)
        # sharpe = mean / stdev = 2.0 / 4.320 ≈ 0.463
        assert result["sharpe_per_trade"] == pytest.approx(0.463, abs=0.01)


@pytest.mark.asyncio
async def test_win_rate_for_no_side_accounts_for_complement_price(repo_factory, room_id):
    """NO side stores yes_price_dollars; entry cost is (1 - yes_price).
    A NO buy at yes_price=0.40 that settles NO should report P&L = 0.40 × count."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # NO buy at yes_price=0.40 means we paid $0.60 per NO contract.
        # Settles NO (our side wins) → payoff $1, P&L = (1 - 0.60) * 10 = $4.00
        await _seed_buy_with_settlement(
            repo, market_ticker="KXHIGHNY-26APR23-T68",
            yes_price="0.4000", count="10.00", trade_id="t1",
            settlement_result="win", side="no",
        )
        await session.flush()

        result = await repo.get_fill_win_rate_30d(kalshi_env="demo")
        assert result["trade_count"] == 1
        assert result["win_count"] == 1
        assert result["avg_win_dollars"] == pytest.approx(4.0)
