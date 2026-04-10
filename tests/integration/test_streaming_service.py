from __future__ import annotations

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, FillRecord, MarketState, OrderRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.streaming import MarketStreamService


class DummyWebSocketClient:
    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_streaming_service_processes_messages_and_persists_state(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/stream.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    messages = [
        {"type": "subscribed", "sid": 4, "msg": {"channel": "orderbook_delta"}},
        {
            "type": "orderbook_snapshot",
            "sid": 4,
            "seq": 1,
            "msg": {
                "market_ticker": "WX-TEST",
                "yes_dollars_fp": [["0.4300", "4.00"]],
                "no_dollars_fp": [["0.5200", "6.00"]],
            },
        },
        {
            "type": "orderbook_delta",
            "sid": 4,
            "seq": 2,
            "msg": {
                "market_ticker": "WX-TEST",
                "side": "yes",
                "price_dollars": "0.4400",
                "delta_fp": "3.00",
            },
        },
        {
            "type": "user_order",
            "sid": 8,
            "msg": {
                "order_id": "ord-1",
                "client_order_id": "client-1",
                "ticker": "WX-TEST",
                "status": "resting",
                "side": "yes",
                "action": "buy",
                "yes_price_dollars": "0.4400",
                "remaining_count_fp": "3.00",
            },
        },
        {
            "type": "fill",
            "sid": 9,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "WX-TEST",
                "side": "yes",
                "action": "buy",
                "yes_price_dollars": "0.4400",
                "count_fp": "1.00",
                "is_taker": True,
            },
        },
    ]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        for message in messages:
            await service.process_message(repo, message)
        await session.commit()

        market_state = (await session.execute(select(MarketState).where(MarketState.market_ticker == "WX-TEST"))).scalar_one()
        order = (await session.execute(select(OrderRecord).where(OrderRecord.client_order_id == "client-1"))).scalar_one()
        fill = (await session.execute(select(FillRecord).where(FillRecord.trade_id == "trade-1"))).scalar_one()
        checkpoint = (await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "kalshi_ws:blue:4"))).scalar_one()

    assert str(market_state.yes_bid_dollars) == "0.4400"
    assert str(market_state.yes_ask_dollars) == "0.4800"
    assert order.status == "resting"
    assert fill.market_ticker == "WX-TEST"
    assert checkpoint.cursor == "2"

    await engine.dispose()
