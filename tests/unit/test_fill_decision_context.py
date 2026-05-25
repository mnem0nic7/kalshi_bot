"""Unit tests for decision-context lineage persisted onto fills.

Verifies:
- a fill linked through order->ticket->room->signal carries decision_edge_bps /
  decision_confidence / decision_fair_yes / decision_spread_bps / decision_ts;
- a fill with no signal/order leaves decision_* columns NULL and does not crash;
- slippage (fill price - decision_price) is computable from persisted values.
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StrategyCode, TradeAction
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.db.models import FillRecord, Room
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
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/fill_ctx.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async def _make():
        return session_factory()

    yield _make
    await engine.dispose()


@pytest.fixture
async def room_id(repo_factory):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        room = Room(
            id="room-1",
            name="test-room",
            market_ticker="KXBTC15M-26MAY151445-45",
            kalshi_env="demo",
        )
        session.add(room)
        await session.commit()
    return "room-1"


@pytest.mark.asyncio
async def test_fill_carries_decision_context_from_signal(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.save_signal(
            room_id=room_id,
            market_ticker="KXBTC15M-26MAY151445-45",
            fair_yes_dollars=Decimal("0.6200"),
            edge_bps=750,
            confidence=0.83,
            summary="crypto directional",
            payload={"market_domain": "crypto", "spread_bps": 120},
        )
        ticket = await repo.save_trade_ticket(
            room_id,
            _ticket("KXBTC15M-26MAY151445-45"),
            client_order_id="coid-D1",
            strategy_code=StrategyCode.CRYPTO_15M.value,
        )
        order = await repo.upsert_order(
            client_order_id="coid-D1",
            market_ticker="KXBTC15M-26MAY151445-45",
            status="executed",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-D1",
            kalshi_env="demo",
        )
        assert order.trade_ticket_id == ticket.id

        fill = await repo.upsert_fill(
            market_ticker="KXBTC15M-26MAY151445-45",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4200"),
            count_fp=Decimal("10.00"),
            raw={"order_id": "kord-D1"},
            trade_id="trade-D1",
            kalshi_env="demo",
        )

        assert fill.order_id == order.id
        assert fill.decision_edge_bps == 750
        assert fill.decision_confidence == pytest.approx(0.83)
        assert fill.decision_fair_yes == Decimal("0.6200")
        assert fill.decision_spread_bps == 120
        assert fill.decision_price == Decimal("0.4000")
        assert fill.decision_ts is not None
        # Slippage = fill price - decision price.
        slippage = fill.yes_price_dollars - fill.decision_price
        assert slippage == Decimal("0.0200")


@pytest.mark.asyncio
async def test_fill_without_order_or_signal_leaves_decision_columns_null(repo_factory, room_id):
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        # No matching order, no signal — must not crash and decision_* must be NULL.
        fill = await repo.upsert_fill(
            market_ticker="KXBTC15M-26MAY151445-45",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.3000"),
            count_fp=Decimal("5.00"),
            raw={"order_id": "no-such-order"},
            trade_id="trade-orphan",
            kalshi_env="demo",
        )

        assert fill.order_id is None
        assert fill.decision_edge_bps is None
        assert fill.decision_confidence is None
        assert fill.decision_spread_bps is None
        assert fill.decision_fair_yes is None
        assert fill.decision_price is None
        assert fill.decision_ts is None

        rows = list((await session.execute(select(FillRecord))).scalars())
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_order_without_signal_populates_decision_price_only(repo_factory, room_id):
    """Order present but no Signal for the room: decision_price comes from the order,
    the rest stay NULL (graceful partial resolution)."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        ticket = await repo.save_trade_ticket(
            room_id,
            _ticket("KXBTC15M-26MAY151445-45"),
            client_order_id="coid-P1",
            strategy_code=StrategyCode.CRYPTO_15M.value,
        )
        order = await repo.upsert_order(
            client_order_id="coid-P1",
            market_ticker="KXBTC15M-26MAY151445-45",
            status="executed",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4500"),
            count_fp=Decimal("10.00"),
            raw={},
            kalshi_order_id="kord-P1",
            kalshi_env="demo",
        )
        assert order.trade_ticket_id == ticket.id

        fill = await repo.upsert_fill(
            market_ticker="KXBTC15M-26MAY151445-45",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4600"),
            count_fp=Decimal("10.00"),
            raw={"order_id": "kord-P1"},
            trade_id="trade-P1",
            kalshi_env="demo",
        )

        assert fill.order_id == order.id
        assert fill.decision_price == Decimal("0.4500")
        assert fill.decision_edge_bps is None
        assert fill.decision_confidence is None
        assert fill.decision_fair_yes is None
        assert fill.decision_ts is None
