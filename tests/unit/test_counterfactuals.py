from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.services.counterfactuals import (
    score_counterfactual_trade,
    strategy_c_fee_cents,
    strategy_c_gross_edge_cents,
    strategy_c_net_ev_per_fill_cents,
    strategy_c_target_cents,
)


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


# ---------------------------------------------------------------------------
# Strategy C discount-sensitivity helpers (P1-3)
# ---------------------------------------------------------------------------


def test_target_cents_locked_yes_is_upper_complement() -> None:
    assert strategy_c_target_cents(resolution_state="locked_yes", discount_cents=1) == 99.0
    assert strategy_c_target_cents(resolution_state="locked_yes", discount_cents=0.5) == 99.5
    assert strategy_c_target_cents(resolution_state="locked_yes", discount_cents=0) == 100.0
    assert strategy_c_target_cents(resolution_state="LOCKED_YES", discount_cents=2) == 98.0


def test_target_cents_locked_no_is_discount_itself() -> None:
    assert strategy_c_target_cents(resolution_state="locked_no", discount_cents=1) == 1.0
    assert strategy_c_target_cents(resolution_state="locked_no", discount_cents=0.5) == 0.5
    assert strategy_c_target_cents(resolution_state="locked_no", discount_cents=0) == 0.0


def test_gross_edge_cents_is_asymmetric_between_locked_yes_and_locked_no() -> None:
    # LOCKED_YES: buy YES at (100-d) cents, settlement pays 100 → edge = d cents.
    assert strategy_c_gross_edge_cents(resolution_state="locked_yes", discount_cents=1) == 1.0
    # LOCKED_NO: buy NO at d cents, settlement pays 100 → edge = (100-d) cents.
    # This asymmetry is the whole point of the sensitivity sweep.
    assert strategy_c_gross_edge_cents(resolution_state="locked_no", discount_cents=1) == 99.0


def test_fee_cents_matches_kalshi_formula() -> None:
    # Fee at p=0.50 is maximal: ceil(0.07*0.5*0.5*100)/100 = ceil(1.75)/100 = 2/100 = $0.02
    assert strategy_c_fee_cents(0.50) == 2.0
    # Fee at p=0.99 is tiny: ceil(0.07*0.99*0.01*100)/100 = ceil(0.0693)/100 = 1/100 = $0.01
    assert strategy_c_fee_cents(0.99) == 1.0
    # Fee at p=0.01 is tiny: ceil(0.07*0.01*0.99*100)/100 = ceil(0.0693)/100 = 1/100 = $0.01
    assert strategy_c_fee_cents(0.01) == 1.0


def test_fee_cents_clamps_inputs_outside_0_1() -> None:
    # Protects against callers passing price_cents by mistake.
    assert strategy_c_fee_cents(-0.5) == 0.0
    assert strategy_c_fee_cents(1.5) == 0.0


def test_net_ev_per_fill_subtracts_fee_from_gross() -> None:
    # LOCKED_YES, d=1¢: gross=1¢, fee at target=0.99 is 1¢ → net=0. Break-even.
    assert strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=1) == 0.0
    # LOCKED_YES, d=2¢: gross=2¢, fee at target=0.98 is ceil(0.07*0.98*0.02*100)/100*100 = 2¢ → net=0.
    # ceil(0.1372)=1 → 1/100=$0.01 → 1¢. So net = 2 - 1 = 1¢.
    assert strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=2) == 1.0
    # LOCKED_YES, d=0.5¢: gross=0.5¢, fee at target=0.995 still 1¢ → net = -0.5¢ (subsidy).
    assert strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=0.5) == pytest.approx(-0.5)
    # LOCKED_YES, d=0¢: gross=0, fee at target=1.00 is 0 (0.07*1*0*100=0, ceil(0)=0) → net=0.
    assert strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=0) == 0.0


def test_net_ev_is_non_decreasing_within_reasonable_discount_range() -> None:
    """Wider discounts should never strictly reduce net EV per fill for
    LOCKED_YES — gross grows linearly with discount while the fee is bounded
    by the same formula."""
    prev = strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=1)
    for d in [1.5, 2.0, 3.0, 5.0]:
        current = strategy_c_net_ev_per_fill_cents(resolution_state="locked_yes", discount_cents=d)
        assert current >= prev, f"net EV went down at d={d}"
        prev = current
