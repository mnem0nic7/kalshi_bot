"""Guard + ticket logic for the stale-quote micro-pilot (research; default OFF).

Pure decision logic for the operator-gated live pilot of the stale-quote taker
edge (docs/research/2026-07-02-stale-quote-taker-edge.md). Order submission is
NOT here — the pilot script routes tickets through the existing ExecutionService
(kill switch, deployment color, shadow mode, write creds), per the architecture
rule that Kalshi writes only happen there. These guards are the pilot's own hard
caps ON TOP of those rails; every default refuses to trade.
"""
from __future__ import annotations

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


@dataclass
class PilotState:
    """Mutable per-run accounting the guards evaluate against."""

    day: date | None = None
    trades_today: int = 0
    realized_pnl_today: float = 0.0
    open_positions: int = 0


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
    if state.trades_today >= config.max_trades_per_day:
        return False, "daily_trade_cap"
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
) -> TradeTicket:
    """1-contract IOC taker ticket crossing the current top of book.

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
        count_fp=Decimal("1"),
        time_in_force="immediate_or_cancel",
        note="stale_quote_pilot",
    )
