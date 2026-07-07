"""Guard + ticket logic for the stale-quote micro-pilot (research; default OFF).

Pure decision logic for the operator-gated live pilot of the stale-quote taker
edge (docs/research/2026-07-02-stale-quote-taker-edge.md). Order submission is
NOT here — the pilot script routes tickets through the existing ExecutionService
(kill switch, deployment color, shadow mode, write creds), per the architecture
rule that Kalshi writes only happen there. These guards are the pilot's own hard
caps ON TOP of those rails; every default refuses to trade.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import TradeTicket


@dataclass(frozen=True)
class PilotConfig:
    """Hard caps for the micro-pilot. Defaults refuse to trade (enabled=False)."""

    enabled: bool = False
    assets: tuple[str, ...] = ()
    max_trades_per_day: int = 0
    max_open_positions: int = 0
    daily_loss_stop_dollars: float = 0.0
    max_entry_dollars: float = 0.0
    contracts: int = 1
    max_trades_per_window: int = 0
    window_hours: int = 12


@dataclass
class PilotState:
    """Mutable per-run accounting the guards evaluate against."""

    day: date | None = None
    trades_today: int = 0
    realized_pnl_today: float = 0.0
    open_positions: int = 0
    window_index: int | None = None
    trades_this_window: int = 0


# Order receipt statuses that do NOT consume the daily/window trade budget
# (mirrors the submission-count rule in scripts/stale_quote_pilot.py).
NON_BUDGET_ORDER_STATUSES: tuple[str, ...] = (
    "shadow_skipped",
    "kill_switch_blocked",
    "inactive_color_skipped",
    "write_credentials_missing",
)


def order_consumed_budget(status: str | None) -> bool:
    """True when an order receipt status counted against the trade budgets."""
    return bool(status) and status not in NON_BUDGET_ORDER_STATUSES and not status.startswith("rejected")


def rebuild_state_from_records(
    records: Iterable[dict], config: PilotConfig, now: datetime
) -> PilotState:
    """Rebuild daily counters from the pilot's own JSONL after a restart.

    Restarts previously zeroed trades_today / realized_pnl_today /
    trades_this_window, silently re-arming the daily loss stop and trade
    budgets mid-day. Open positions are deliberately NOT rebuilt: they settle
    within ~15 minutes, so a restart orphans at most one settlement cycle,
    and reconstructing entry/settle_by from records is not worth the risk of
    double-counting a settlement that raced the restart.
    """
    state = PilotState(day=now.date())
    window_hours = max(1, config.window_hours)
    state.window_index = now.hour // window_hours
    for rec in records:
        ts = rec.get("ts")
        if not isinstance(ts, str):
            continue
        try:
            when = datetime.fromisoformat(ts)
        except ValueError:
            continue
        if when.date() != state.day:
            continue
        if rec.get("type") == "settle":
            try:
                state.realized_pnl_today += float(rec.get("net", 0.0))
            except (TypeError, ValueError):
                continue
        elif order_consumed_budget(rec.get("order_status")):
            state.trades_today += 1
            if when.hour // window_hours == state.window_index:
                state.trades_this_window += 1
    return state


def evaluate_guards(
    config: PilotConfig,
    state: PilotState,
    *,
    asset: str,
    entry_dollars: float,
    now: datetime,
) -> tuple[bool, str]:
    """Return (allowed, reason). Order matters: cheapest/most-decisive first."""
    if not config.enabled:
        return False, "pilot_disabled"
    if asset not in config.assets:
        return False, "asset_not_allowed"
    if state.day != now.date():
        # new (or first) day: daily counters reset
        state.day = now.date()
        state.trades_today = 0
        state.realized_pnl_today = 0.0
        state.window_index = None
        state.trades_this_window = 0
    if state.trades_today >= config.max_trades_per_day:
        return False, "daily_trade_cap"
    if config.max_trades_per_window > 0:
        idx = now.hour // max(1, config.window_hours)
        if state.window_index != idx:
            state.window_index = idx
            state.trades_this_window = 0
        if state.trades_this_window >= config.max_trades_per_window:
            return False, "window_trade_cap"
    if state.open_positions >= config.max_open_positions:
        return False, "open_position_cap"
    if state.realized_pnl_today <= -abs(config.daily_loss_stop_dollars):
        return False, "daily_loss_stop"
    if entry_dollars > config.max_entry_dollars:
        return False, "entry_above_max"
    return True, "ok"


def build_pilot_ticket(
    *,
    market_ticker: str,
    side: str,
    yes_bid: Decimal,
    yes_ask: Decimal,
    count: int = 1,
) -> TradeTicket:
    """IOC taker ticket (count contracts) crossing the current top of book.

    Buying YES crosses at the ask; buying NO crosses at the bid (the yes-price at
    which the NO side transacts — you pay 1 - yes_bid per NO contract).
    """
    contract_side = ContractSide.YES if side == "yes" else ContractSide.NO
    yes_price = yes_ask if contract_side == ContractSide.YES else yes_bid
    return TradeTicket(
        market_ticker=market_ticker,
        action=TradeAction.BUY,
        side=contract_side,
        yes_price_dollars=yes_price,
        count_fp=Decimal(count),
        time_in_force="immediate_or_cancel",
        note="stale_quote_pilot",
    )
