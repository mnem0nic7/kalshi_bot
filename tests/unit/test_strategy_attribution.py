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

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StrategyCode, TradeAction
from kalshi_bot.db.models import Room
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
        assert overall == {"won_contracts": 10.0, "total_contracts": 15.0}

        only_a = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="A")
        assert only_a == {"won_contracts": 10.0, "total_contracts": 10.0}

        only_c = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="C")
        assert only_c == {"won_contracts": 0.0, "total_contracts": 5.0}

        no_arb_yet = await repo.get_fill_win_rate_30d(kalshi_env="demo", strategy_code="ARB")
        assert no_arb_yet == {"won_contracts": 0.0, "total_contracts": 0.0}
