"""Rebuild of pilot daily counters from JSONL after a restart.

A pilot restart previously zeroed trades_today / realized_pnl_today /
trades_this_window, silently re-arming the $-loss stop and trade budgets
mid-day (2026-07-07: true day P&L was -$2.34 while the in-process counter
said +$0.60 after two same-day restarts).
"""
from datetime import UTC, datetime

from kalshi_bot.crypto.stale_quote_pilot import (
    PilotConfig,
    order_consumed_budget,
    rebuild_state_from_records,
)

NOW = datetime(2026, 7, 7, 15, 30, tzinfo=UTC)  # window index 1 (12-24 UTC)
CFG = PilotConfig(enabled=True, assets=("BNB",), max_trades_per_day=10,
                  max_open_positions=2, daily_loss_stop_dollars=6.0,
                  max_entry_dollars=0.75, max_trades_per_window=5)


def test_settles_today_rebuild_pnl_and_yesterday_ignored():
    records = [
        {"type": "settle", "ts": "2026-07-06T23:50:00+00:00", "net": -3.0},
        {"type": "settle", "ts": "2026-07-07T10:00:00+00:00", "net": -1.25},
        {"type": "settle", "ts": "2026-07-07T14:00:00+00:00", "net": 0.5},
    ]
    state = rebuild_state_from_records(records, CFG, NOW)
    assert state.day == NOW.date()
    assert state.realized_pnl_today == -0.75


def test_submitted_orders_count_toward_budgets_but_skips_do_not():
    records = [
        # counted: executed this window
        {"type": "signal", "ts": "2026-07-07T13:00:00+00:00", "order_status": "executed"},
        # counted toward day only: executed in the earlier 00-12 window
        {"type": "signal", "ts": "2026-07-07T09:00:00+00:00", "order_status": "canceled"},
        # not counted: shadow / rejected / no order at all
        {"type": "signal", "ts": "2026-07-07T13:05:00+00:00", "order_status": "shadow_skipped"},
        {"type": "signal", "ts": "2026-07-07T13:06:00+00:00", "order_status": "rejected_insufficient_balance"},
        {"type": "signal", "ts": "2026-07-07T13:07:00+00:00", "guard": "live_edge_too_small"},
    ]
    state = rebuild_state_from_records(records, CFG, NOW)
    assert state.trades_today == 2
    assert state.trades_this_window == 1
    assert state.window_index == 1


def test_empty_and_malformed_records_yield_fresh_state():
    state = rebuild_state_from_records(
        [{"type": "settle"}, {"ts": "not-a-date", "type": "settle", "net": 1.0}], CFG, NOW
    )
    assert state.trades_today == 0
    assert state.realized_pnl_today == 0.0
    assert state.window_index == 1
    assert state.open_positions == 0  # open positions are deliberately NOT rebuilt


def test_order_consumed_budget_matches_submission_rule():
    assert order_consumed_budget("executed")
    assert order_consumed_budget("canceled")
    assert not order_consumed_budget("shadow_skipped")
    assert not order_consumed_budget("kill_switch_blocked")
    assert not order_consumed_budget("inactive_color_skipped")
    assert not order_consumed_budget("write_credentials_missing")
    assert not order_consumed_budget("rejected_anything")
    assert not order_consumed_budget(None)
