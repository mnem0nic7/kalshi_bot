from decimal import Decimal

import pytest

from kalshi_bot.services.streaming import MarketStreamService, OrderBookState, SequenceGapError
from kalshi_bot.config import Settings
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


class DummyWebSocketClient:
    async def close(self) -> None:
        return None


def test_orderbook_state_derives_best_prices_from_snapshot() -> None:
    state = OrderBookState.from_snapshot(
        {
            "market_ticker": "WX-TEST",
            "yes_dollars_fp": [["0.4200", "10.00"], ["0.4400", "5.00"]],
            "no_dollars_fp": [["0.5300", "7.00"], ["0.5500", "3.00"]],
        },
        seq=10,
    )

    assert state.best_yes_bid == Decimal("0.4400")
    assert state.best_yes_ask == Decimal("0.4500")
    assert state.best_no_ask == Decimal("0.5600")


def test_orderbook_state_tracks_last_seen_message_seq_without_gap_validation() -> None:
    state = OrderBookState.from_snapshot(
        {
            "market_ticker": "WX-TEST",
            "yes_dollars_fp": [["0.4200", "10.00"]],
            "no_dollars_fp": [],
        },
        seq=3,
    )

    state.apply_delta({"side": "yes", "price_dollars": "0.4200", "delta_fp": "1.00"}, seq=7)

    assert state.yes_levels[Decimal("0.4200")] == Decimal("11.00")
    assert state.seq == 7


@pytest.mark.asyncio
async def test_streaming_service_validates_sid_sequences(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/sid-gap.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = MarketStreamService(settings, session_factory, DummyWebSocketClient())  # type: ignore[arg-type]

    async with session_factory() as session:
        from kalshi_bot.db.repositories import PlatformRepository

        repo = PlatformRepository(session)
        await service.process_message(
            repo,
            {
                "type": "orderbook_snapshot",
                "sid": 4,
                "seq": 10,
                "msg": {
                    "market_ticker": "WX-TEST",
                    "yes_dollars_fp": [["0.4200", "10.00"]],
                    "no_dollars_fp": [["0.5200", "2.00"]],
                },
            },
        )
        with pytest.raises(SequenceGapError):
            await service.process_message(
                repo,
                {
                    "type": "orderbook_delta",
                    "sid": 4,
                    "seq": 12,
                    "msg": {
                        "market_ticker": "WX-TEST",
                        "side": "yes",
                        "price_dollars": "0.4200",
                        "delta_fp": "1.00",
                    },
                },
            )

    await engine.dispose()
