from __future__ import annotations

from decimal import Decimal

from kalshi_bot.db.repositories import _total_capital_dollars_from_balance_payload


def test_total_capital_treats_smaller_portfolio_value_as_positions_value() -> None:
    assert _total_capital_dollars_from_balance_payload(
        {"balance": 50000, "portfolio_value": 1200}
    ) == Decimal("512")


def test_total_capital_treats_larger_portfolio_value_as_total_equity() -> None:
    assert _total_capital_dollars_from_balance_payload(
        {"balance": 10000, "portfolio_value": 25000}
    ) == Decimal("250")


def test_total_capital_falls_back_to_cash_when_portfolio_missing() -> None:
    assert _total_capital_dollars_from_balance_payload({"cash_balance": 9000}) == Decimal("90")
