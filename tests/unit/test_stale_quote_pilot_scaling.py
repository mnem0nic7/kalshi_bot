"""Leg-1 scaling (spec 2026-07-06-profitability-push-2): per-12h-window trade
budget (stops overnight burning the whole day), contract count on tickets.
Flat daily cap remains as the belt on top of the window cap."""
from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.crypto.stale_quote_pilot import (
    PilotConfig, PilotState, build_pilot_ticket, evaluate_guards,
)


def _cfg(**kw):
    base = dict(enabled=True, assets=("BNB",), max_trades_per_day=10,
                max_open_positions=2, daily_loss_stop_dollars=6.0,
                max_entry_dollars=0.75, max_trades_per_window=5, window_hours=12)
    base.update(kw)
    return PilotConfig(**base)


def _at(hour):
    return datetime(2026, 7, 6, hour, 30, tzinfo=UTC)


def test_window_cap_binds_within_window():
    state = PilotState()
    cfg = _cfg()
    for _ in range(5):
        ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(3))
        assert ok, why
        state.trades_today += 1
        state.trades_this_window += 1
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(4))
    assert not ok and why == "window_trade_cap"


def test_window_rollover_resets_window_counter_not_daily():
    state = PilotState()
    cfg = _cfg()
    state.day = _at(3).date()
    state.trades_today = 5
    state.window_index = 0
    state.trades_this_window = 5
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(13))
    assert ok, why                      # second UTC window: fresh window budget
    assert state.trades_this_window == 0
    assert state.trades_today == 5      # daily belt untouched by rollover


def test_daily_belt_still_binds_across_windows():
    state = PilotState()
    cfg = _cfg(max_trades_per_day=6)
    state.day = _at(13).date()
    state.trades_today = 6
    state.window_index = 1
    state.trades_this_window = 1
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(13))
    assert not ok and why == "daily_trade_cap"


def test_flat_daily_only_when_window_cap_unset():
    state = PilotState()
    cfg = _cfg(max_trades_per_window=0)   # window feature off -> legacy behavior
    for _ in range(7):
        ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(3))
        assert ok, why
        state.trades_today += 1


def test_ticket_carries_contract_count():
    t1 = build_pilot_ticket(market_ticker="X", side="yes",
                            yes_bid=Decimal("0.40"), yes_ask=Decimal("0.42"))
    assert t1.count_fp == Decimal("1")
    t2 = build_pilot_ticket(market_ticker="X", side="no",
                            yes_bid=Decimal("0.40"), yes_ask=Decimal("0.42"), count=2)
    assert t2.count_fp == Decimal("2")
