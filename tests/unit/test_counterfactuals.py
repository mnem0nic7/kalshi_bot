from __future__ import annotations

from decimal import Decimal

from kalshi_bot.services.counterfactuals import score_counterfactual_trade


def test_score_counterfactual_trade_uses_ticket_and_settlement_inputs() -> None:
    outcome = score_counterfactual_trade(
        trade_ticket={
            "side": "no",
            "yes_price_dollars": "0.62",
            "count_fp": "3.00",
        },
        settlement={
            "settlement_value_dollars": "1.0000",
            "kalshi_result": "yes",
        },
    )

    assert outcome is not None
    assert outcome.settlement_result == "loss"
    assert outcome.settlement_value_dollars == Decimal("1.0000")
    assert outcome.pnl_dollars == Decimal("-1.1400")
