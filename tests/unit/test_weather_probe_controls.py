from __future__ import annotations

from decimal import Decimal

from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.orchestration.supervisor import (
    _cap_ticket_notional,
    _weather_balance_discontinuity,
    _weather_realized_loss_cap_dollars,
)
from kalshi_bot.services.signal import estimate_notional_dollars


def test_weather_realized_loss_cap_uses_one_dollar_floor() -> None:
    cap = _weather_realized_loss_cap_dollars(
        total_capital=Decimal("29.26"),
        cap_pct=0.02,
        min_loss_dollars=1.0,
    )

    assert cap == Decimal("1.0")


def test_balance_drop_without_realized_loss_is_discontinuity_not_trading_loss() -> None:
    assert _weather_balance_discontinuity(
        portfolio_loss_ratio=2.3399,
        realized_loss_dollars=Decimal("0"),
        realized_loss_cap_dollars=Decimal("1.00"),
        discontinuity_ratio=0.50,
    )


def test_probe_ticket_notional_is_capped_to_tiny_size() -> None:
    ticket = TradeTicket(
        market_ticker="KXHIGHCHI-26MAY13-T60",
        action=TradeAction.BUY,
        side=ContractSide.YES,
        yes_price_dollars=Decimal("0.5800"),
        count_fp=Decimal("10.00"),
    )

    capped, trace = _cap_ticket_notional(ticket, max_notional_dollars=0.25)

    assert capped is not None
    assert trace["cap_applied"] is True
    assert estimate_notional_dollars(capped.side, capped.yes_price_dollars, capped.count_fp) <= Decimal("0.25")
