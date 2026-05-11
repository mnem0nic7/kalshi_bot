from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StrategyCode, TradeAction
from kalshi_bot.core.schemas import RoomCreate, TradeTicket
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.mark.asyncio
async def test_bootstrap_order_fill_settlement_is_mirrored_as_live_evidence(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/bootstrap-settlement.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        market_ticker = "KXHIGHTSFO-26MAY11-T70"
        room = await repo.create_room(
            RoomCreate(name="bootstrap", market_ticker=market_ticker),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="production",
        )
        ticket = await repo.save_trade_ticket(
            room.id,
            TradeTicket(
                market_ticker=market_ticker,
                action=TradeAction.BUY,
                side=ContractSide.YES,
                yes_price_dollars=Decimal("0.4000"),
                count_fp=Decimal("2.00"),
            ),
            client_order_id="client-bootstrap-1",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        order = await repo.save_order(
            ticket_id=ticket.id,
            client_order_id="client-bootstrap-1",
            kalshi_order_id="kalshi-bootstrap-1",
            market_ticker=market_ticker,
            status="filled",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("2.00"),
            raw={"order_id": "kalshi-bootstrap-1"},
            kalshi_env="production",
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        await repo.save_weather_bootstrap_event(
            kalshi_env="production",
            market_ticker=market_ticker,
            series_ticker="KXHIGHTSFO",
            local_market_day="26MAY11",
            bucket_key="sfo-cold",
            policy_key="weather/a/SFO/entry_gate",
            tier="cold",
            event_type="order",
            status="filled",
            side="yes",
            confidence=0.94,
            edge_bps=4200,
            size_factor=0.10,
            count_fp=Decimal("2.00"),
            notional_dollars=Decimal("0.8000"),
            source="live_forward",
            room_id=room.id,
            order_id=order.id,
            payload={"bootstrap": True},
        )
        await repo.upsert_fill(
            market_ticker=market_ticker,
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.4000"),
            count_fp=Decimal("2.00"),
            raw={"order_id": "kalshi-bootstrap-1", "fee_dollars": "0.0100"},
            trade_id="trade-bootstrap-1",
            kalshi_env="production",
        )
        await repo.settle_fills(
            [{"market_ticker": market_ticker, "market_result": "yes"}],
            kalshi_env="production",
        )

        created = await repo.sync_weather_bootstrap_settlement_events(kalshi_env="production")
        second_created = await repo.sync_weather_bootstrap_settlement_events(kalshi_env="production")
        events = await repo.list_weather_bootstrap_events(
            kalshi_env="production",
            event_types=["settlement"],
            limit=10,
        )
        await session.commit()

    assert created == 1
    assert second_created == 0
    assert len(events) == 1
    event = events[0]
    assert event.status == "settled_win"
    assert event.source == "live_settlement"
    assert event.fill_id is not None
    assert event.policy_key == "weather/a/SFO/entry_gate"
    assert event.pnl_dollars == Decimal("1.1900")
    assert event.payload["trade_id"] == "trade-bootstrap-1"

    await engine.dispose()
