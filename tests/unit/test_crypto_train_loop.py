"""Pure logic for the continuous-train work order + resume cursor.

The continuous trainer was starving 1h: it ran the whole 15m pass (all assets)
before 1h, and a restart reset it to the first frequency, so 1h never completed
(and 15m kept getting re-trained from scratch). These helpers (a) interleave so
1h is attempted right after each asset's 15m, and (b) resume at the next item
after the last completed one so a restart picks up where it left off instead of
re-training.
"""
from __future__ import annotations

from kalshi_bot.crypto.train_loop import (
    build_train_work_order,
    frequency_last_indices,
    resume_index,
)


def test_work_order_is_asset_major_interleave():
    order = build_train_work_order(["BTC", "ETH"], ["15m", "1h"])
    assert order == [("BTC", "15m"), ("BTC", "1h"), ("ETH", "15m"), ("ETH", "1h")]


def test_work_order_attempts_1h_before_all_15m_done():
    # Fairness: the first 1h item must come before the last 15m item, so a
    # short uptime window still trains some 1h.
    order = build_train_work_order(["BTC", "ETH", "SOL"], ["15m", "1h"])
    first_1h = next(i for i, (_, f) in enumerate(order) if f == "1h")
    last_15m = max(i for i, (_, f) in enumerate(order) if f == "15m")
    assert first_1h < last_15m


def test_resume_index_picks_up_after_last_completed():
    order = build_train_work_order(["BTC", "ETH"], ["15m", "1h"])
    assert resume_index(order, ("BTC", "1h")) == 2  # next is ("ETH","15m")


def test_resume_index_wraps_after_final_item():
    order = build_train_work_order(["BTC", "ETH"], ["15m", "1h"])
    assert resume_index(order, ("ETH", "1h")) == 0


def test_resume_index_restarts_safely_on_unknown_or_empty_cursor():
    order = build_train_work_order(["BTC", "ETH"], ["15m", "1h"])
    assert resume_index(order, None) == 0
    assert resume_index(order, ("DOGE", "15m")) == 0  # config changed -> safe restart
    assert resume_index([], ("BTC", "15m")) == 0


def test_frequency_last_indices_marks_each_frequencys_final_item():
    order = build_train_work_order(["BTC", "ETH"], ["15m", "1h"])
    # last 15m is ("ETH","15m") at idx 2; last 1h is ("ETH","1h") at idx 3
    assert frequency_last_indices(order) == {2: "15m", 3: "1h"}
