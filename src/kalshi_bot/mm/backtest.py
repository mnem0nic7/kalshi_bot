"""Market-making backtest core (plan §4.4 entry rule, §5 realistic fills).

Pure functions: decide a maker buy from the fair value, model a queue-aware fill
(only when the market trades to the resting limit), and settle P&L from the real
outcome — never from mid — so adverse selection is reflected. Kalshi pays no
maker rebate; the fee is `rate · price · (1−price)`.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

_AVOID_BAND = (Decimal("0.45"), Decimal("0.55"))


def maker_fee(price: Decimal, *, fee_rate: Decimal) -> Decimal:
    """Kalshi fee for one contract at ``price`` (no maker rebate)."""
    return fee_rate * price * (Decimal("1") - price)


def maker_entry(
    fair_up: Decimal,
    *,
    edge_buffer: Decimal,
    fee_rate: Decimal,
    min_edge: Decimal = Decimal("0"),
    avoid_band: tuple[Decimal, Decimal] = _AVOID_BAND,
) -> dict[str, Any] | None:
    """Decide a maker buy from the analytic fair value.

    Picks the favored side (yes if ``fair_up >= 0.5`` else no), posts a buy a
    ``edge_buffer`` below that side's fair probability, and requires the residual
    edge (fair − limit − fee) to clear ``min_edge``. Stands down inside the
    peak-fee/max-uncertainty avoid band. Returns ``None`` when no order qualifies.
    """
    side = "yes" if fair_up >= Decimal("0.5") else "no"
    side_prob = fair_up if side == "yes" else Decimal("1") - fair_up
    limit = side_prob - edge_buffer
    if limit <= Decimal("0") or limit >= Decimal("1"):
        return None
    if avoid_band[0] <= limit <= avoid_band[1]:
        return None
    fee = maker_fee(limit, fee_rate=fee_rate)
    edge = side_prob - limit - fee
    if edge < max(min_edge, Decimal("0")) or edge <= Decimal("0"):
        return None
    return {"side": side, "limit": limit, "fee": fee, "edge": edge, "side_prob": side_prob}


def maker_fills(limit: Decimal, *, future_side_asks: list[Decimal]) -> bool:
    """A resting buy at ``limit`` fills only if a later side ask trades at/through it."""
    return any(ask <= limit for ask in future_side_asks)


def settle_net_pnl(*, side: str, limit: Decimal, label_yes: int, fee: Decimal) -> Decimal:
    """Net P&L of a filled maker buy, settled from the real outcome."""
    payoff = Decimal(int(label_yes)) if side == "yes" else Decimal(1 - int(label_yes))
    return payoff - limit - fee
