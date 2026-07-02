"""Guard + ticket logic for the stale-quote micro-pilot (TDD).

The pilot places REAL 1-contract IOC taker orders only when every guard passes;
defaults must refuse. Order submission itself goes through ExecutionService and
is not under test here — only the pure decision logic."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.crypto.stale_quote_pilot import PilotConfig, PilotState, build_pilot_ticket, evaluate_guards


def _cfg(**kw) -> PilotConfig:
    base = dict(enabled=True, assets=("BTC", "BNB"), max_trades_per_day=10,
                max_open_positions=1, daily_loss_stop_dollars=3.0, max_entry_dollars=0.75)
    base.update(kw)
    return PilotConfig(**base)


NOW = datetime(2026, 7, 2, 15, 0, tzinfo=UTC)


class TestGuards:
    def test_disabled_by_default_refuses(self):
        cfg = PilotConfig()  # all defaults
        assert cfg.enabled is False
        ok, reason = evaluate_guards(cfg, PilotState(), asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is False and reason == "pilot_disabled"

    def test_enabled_allows_within_all_caps(self):
        ok, reason = evaluate_guards(_cfg(), PilotState(), asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is True and reason == "ok"

    def test_asset_not_allowlisted_refuses(self):
        ok, reason = evaluate_guards(_cfg(), PilotState(), asset="DOGE", entry_dollars=0.50, now=NOW)
        assert ok is False and reason == "asset_not_allowed"

    def test_daily_trade_cap_refuses(self):
        st = PilotState(trades_today=10, day=NOW.date())
        ok, reason = evaluate_guards(_cfg(), st, asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is False and reason == "daily_trade_cap"

    def test_trade_count_resets_on_new_day(self):
        st = PilotState(trades_today=10, day=(NOW - timedelta(days=1)).date())
        ok, reason = evaluate_guards(_cfg(), st, asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is True

    def test_open_position_cap_refuses(self):
        st = PilotState(open_positions=1)
        ok, reason = evaluate_guards(_cfg(), st, asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is False and reason == "open_position_cap"

    def test_daily_loss_stop_refuses(self):
        st = PilotState(realized_pnl_today=-3.0, day=NOW.date())
        ok, reason = evaluate_guards(_cfg(), st, asset="BTC", entry_dollars=0.50, now=NOW)
        assert ok is False and reason == "daily_loss_stop"

    def test_entry_price_cap_refuses(self):
        ok, reason = evaluate_guards(_cfg(), PilotState(), asset="BTC", entry_dollars=0.80, now=NOW)
        assert ok is False and reason == "entry_above_max"


class TestTicket:
    def test_buy_yes_ioc_at_ask(self):
        t = build_pilot_ticket(market_ticker="KXBTC15M-26JUL021100-00", side="yes",
                               yes_bid=Decimal("0.46"), yes_ask=Decimal("0.51"))
        assert t.market_ticker == "KXBTC15M-26JUL021100-00"
        assert t.side == ContractSide.YES
        assert t.action == TradeAction.BUY
        assert t.count_fp == Decimal("1")
        assert t.yes_price_dollars == Decimal("0.51")   # cross the ask
        assert t.time_in_force == "immediate_or_cancel"

    def test_buy_no_ioc_at_bid(self):
        t = build_pilot_ticket(market_ticker="KXBNB15M-26JUL021100-00", side="no",
                               yes_bid=Decimal("0.46"), yes_ask=Decimal("0.51"))
        assert t.side == ContractSide.NO
        assert t.action == TradeAction.BUY
        # crossing the NO side means transacting at the yes_bid (paying 1 - bid for no)
        assert t.yes_price_dollars == Decimal("0.46")
        assert t.count_fp == Decimal("1")
