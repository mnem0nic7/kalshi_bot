from __future__ import annotations

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, FillRecord, MarketState, OpsEvent, OrderRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.streaming import MarketStreamService


class DummyWebSocketClient:
    async def close(self) -> None:
        return None


class FailingWebSocketClient:
    async def connect(self) -> None:
        return None

    async def subscribe(self, channels: list[str], market_tickers: list[str] | None = None) -> None:
        return None

    async def iter_messages(self):
        raise TimeoutError("ws timeout")
        yield {}

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
        checkpoint = (await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "kalshi_ws:demo:blue:4"))).scalar_one()

    assert str(market_state.yes_bid_dollars) == "0.4400"
    assert str(market_state.yes_ask_dollars) == "0.4800"
    assert order.status == "resting"
    assert fill.market_ticker == "WX-TEST"
    assert checkpoint.cursor == "2"

    await engine.dispose()


@pytest.mark.asyncio
async def test_streaming_service_preserves_user_order_when_remaining_count_is_zero(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/stream-zero-remaining.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    message = {
        "type": "user_order",
        "sid": 8,
        "msg": {
            "order_id": "ord-2",
            "client_order_id": "client-2",
            "ticker": "WX-TEST",
            "status": "canceled",
            "side": "no",
            "action": "buy",
            "yes_price_dollars": "0.4400",
            "remaining_count_fp": "0.00",
            "initial_count_fp": "25.00",
        },
    }

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await service.process_message(repo, message)
        await session.commit()

        order = (await session.execute(select(OrderRecord).where(OrderRecord.client_order_id == "client-2"))).scalar_one()

    assert order.status == "canceled"
    assert str(order.count_fp) == "25.00"

    await engine.dispose()


@pytest.mark.asyncio
async def test_streaming_service_deduplicates_fill_events_by_trade_id(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/stream-duplicate-fills.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    first = {
        "type": "fill",
        "sid": 9,
        "msg": {
            "trade_id": "trade-duplicate",
            "market_ticker": "WX-TEST",
            "side": "yes",
            "action": "buy",
            "yes_price_dollars": "0.4400",
            "count_fp": "1.00",
            "is_taker": True,
        },
    }
    second = {
        "type": "fill",
        "sid": 9,
        "msg": {
            "trade_id": "trade-duplicate",
            "market_ticker": "WX-TEST",
            "side": "yes",
            "action": "buy",
            "yes_price_dollars": "0.4500",
            "count_fp": "1.00",
            "is_taker": False,
        },
    }

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await service.process_message(repo, first)
        await service.process_message(repo, second)
        await session.commit()

        fills = list((await session.execute(select(FillRecord).where(FillRecord.trade_id == "trade-duplicate"))).scalars())

    assert len(fills) == 1
    assert str(fills[0].yes_price_dollars) == "0.4500"
    assert fills[0].is_taker is False

    await engine.dispose()


@pytest.mark.asyncio
async def test_streaming_service_persists_private_events_in_active_settings_env(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stream-env.db",
        kalshi_env="production",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    messages = [
        {
            "type": "orderbook_snapshot",
            "sid": 4,
            "seq": 1,
            "msg": {
                "market_ticker": "WX-ENV",
                "yes_dollars_fp": [["0.4300", "4.00"]],
                "no_dollars_fp": [["0.5200", "6.00"]],
            },
        },
        {
            "type": "user_order",
            "sid": 8,
            "msg": {
                "order_id": "ord-env",
                "client_order_id": "client-env",
                "ticker": "WX-ENV",
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
                "trade_id": "trade-env",
                "market_ticker": "WX-ENV",
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

        market_state = (await session.execute(select(MarketState).where(MarketState.market_ticker == "WX-ENV"))).scalar_one()
        order = (await session.execute(select(OrderRecord).where(OrderRecord.client_order_id == "client-env"))).scalar_one()
        fill = (await session.execute(select(FillRecord).where(FillRecord.trade_id == "trade-env"))).scalar_one()
        checkpoint = (await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "kalshi_ws:production:blue:4"))).scalar_one()

    assert market_state.kalshi_env == "production"
    assert order.kalshi_env == "production"
    assert fill.kalshi_env == "production"
    assert checkpoint.cursor == "1"

    await engine.dispose()


@pytest.mark.asyncio
async def test_streaming_service_handles_interleaved_market_sequences_by_sid(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/stream-interleaved.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    messages = [
        {
            "type": "orderbook_snapshot",
            "sid": 4,
            "seq": 1,
            "msg": {
                "market_ticker": "WX-ALPHA",
                "yes_dollars_fp": [["0.4300", "4.00"]],
                "no_dollars_fp": [["0.5200", "6.00"]],
            },
        },
        {
            "type": "orderbook_snapshot",
            "sid": 4,
            "seq": 2,
            "msg": {
                "market_ticker": "WX-BETA",
                "yes_dollars_fp": [["0.3300", "7.00"]],
                "no_dollars_fp": [["0.6200", "5.00"]],
            },
        },
        {
            "type": "orderbook_delta",
            "sid": 4,
            "seq": 3,
            "msg": {
                "market_ticker": "WX-ALPHA",
                "side": "yes",
                "price_dollars": "0.4400",
                "delta_fp": "2.00",
            },
        },
    ]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        for message in messages:
            await service.process_message(repo, message)
        await session.commit()

        alpha = (await session.execute(select(MarketState).where(MarketState.market_ticker == "WX-ALPHA"))).scalar_one()
        beta = (await session.execute(select(MarketState).where(MarketState.market_ticker == "WX-BETA"))).scalar_one()
        checkpoint = (await session.execute(select(Checkpoint).where(Checkpoint.stream_name == "kalshi_ws:demo:blue:4"))).scalar_one()

    assert str(alpha.yes_bid_dollars) == "0.4400"
    assert str(beta.yes_bid_dollars) == "0.3300"
    assert checkpoint.cursor == "3"

    await engine.dispose()


@pytest.mark.asyncio
async def test_streaming_service_rate_limits_repeated_stream_errors(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stream-errors.db",
        stream_error_log_cooldown_seconds=900,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, FailingWebSocketClient())  # type: ignore[arg-type]

    for _ in range(2):
        with pytest.raises(TimeoutError):
            await service.stream(market_tickers=["WX-TEST"], include_private=False, max_messages=1)

    async with session_factory() as session:
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        checkpoint = (
            await session.execute(
                select(Checkpoint).where(Checkpoint.stream_name == "kalshi_ws_error:demo:blue:TimeoutError")
            )
        ).scalar_one()

    stream_errors = [event for event in ops_events if event.source == "stream" and "websocket stream error" in event.summary]
    assert len(stream_errors) == 1
    assert stream_errors[0].payload["error_type"] == "TimeoutError"
    assert stream_errors[0].payload["message"] == "ws timeout"
    assert checkpoint.payload["occurrence_count"] == 2
    assert checkpoint.payload["last_seen_at"] is not None

    await engine.dispose()
