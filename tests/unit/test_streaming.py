from decimal import Decimal

import pytest

from kalshi_bot.services.streaming import OrderBookState, SequenceGapError


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


def test_orderbook_state_raises_on_sequence_gap() -> None:
    state = OrderBookState.from_snapshot(
        {
            "market_ticker": "WX-TEST",
            "yes_dollars_fp": [["0.4200", "10.00"]],
            "no_dollars_fp": [],
        },
        seq=3,
    )

    with pytest.raises(SequenceGapError):
        state.apply_delta({"side": "yes", "price_dollars": "0.4200", "delta_fp": "1.00"}, seq=7)

