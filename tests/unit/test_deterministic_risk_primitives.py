from __future__ import annotations

import pytest

from kalshi_bot.risk.exit_score import ExitInputs, choose_exit_action, score_exit_risk
from kalshi_bot.risk.sizing import (
    SizingConfig,
    fee_aware_binary_kelly_fraction,
    side_cost_from_yes_price,
    side_probability,
    size_fee_aware_binary_trade,
)
from kalshi_bot.risk.survival import SurvivalConfig, apply_survival_mode
from kalshi_bot.risk.uncertainty import UncertaintyConfig, score_uncertainty


def test_uncertainty_score_raises_min_ev_and_tapers_size() -> None:
    result = score_uncertainty(boundary_mass=0.25, disagreement=0.425)

    assert result.boundary_component == 1.0
    assert result.disagreement_component == pytest.approx(0.5)
    assert result.uncertainty_score == pytest.approx(0.8)
    assert result.dynamic_min_ev == pytest.approx(0.036)
    assert result.size_mult == pytest.approx(0.52)


def test_uncertainty_score_respects_size_floor() -> None:
    result = score_uncertainty(
        boundary_mass=10,
        disagreement=10,
        config=UncertaintyConfig(size_floor=0.45),
    )

    assert result.uncertainty_score == 1.0
    assert result.size_mult == 0.45


def test_fee_aware_binary_kelly_rejects_when_fee_erases_edge() -> None:
    no_fee = fee_aware_binary_kelly_fraction(p_win=0.62, cost=0.55, fee=0.0)
    with_fee = fee_aware_binary_kelly_fraction(p_win=0.62, cost=0.55, fee=0.08)

    assert no_fee > 0
    assert with_fee == 0


def test_sizing_maps_no_side_probability_and_cost_from_yes_price() -> None:
    assert side_probability(p_yes=0.30, side="no") == pytest.approx(0.70)
    assert side_cost_from_yes_price(yes_price=0.30, side="no") == pytest.approx(0.70)


def test_fee_aware_sizing_applies_multipliers_and_caps() -> None:
    result = size_fee_aware_binary_trade(
        balance=1000.0,
        p_yes=0.70,
        yes_price=0.50,
        side="yes",
        fee_per_contract=0.01,
        current_total_exposure_dollars=100.0,
        config=SizingConfig(
            kelly_fraction=0.25,
            spread_penalty=0.8,
            uncertainty_mult=0.5,
            health_size_mult=1.0,
            max_position_pct=0.20,
            max_position_usd=30.0,
            max_total_exposure_pct=0.50,
        ),
    )

    assert result.accepted
    assert result.full_kelly > 0
    assert result.requested_size_dollars > 30.0
    assert result.capped_size_dollars == 30.0
    assert result.cap_bound == "max_position_usd"


def test_survival_mode_switches_kelly_and_ev_buffer_below_threshold() -> None:
    config = SurvivalConfig(starting_balance=1000.0)

    normal = apply_survival_mode(balance=400.0, dynamic_min_ev=0.02, config=config)
    survival = apply_survival_mode(balance=200.0, dynamic_min_ev=0.02, config=config)

    assert normal.active is False
    assert normal.kelly_fraction == pytest.approx(0.25)
    assert survival.active is True
    assert survival.kelly_fraction == pytest.approx(0.10)
    assert survival.dynamic_min_ev == pytest.approx(0.05)


def test_exit_risk_tightens_hold_buffer() -> None:
    risk = score_exit_risk(
        boundary_mass=0.25,
        hours_to_event=1.0,
        disagreement=0.425,
        spread_cents=10.0,
        hold_buffer=4.0,
    )

    assert risk.risk_score == pytest.approx(0.675)
    assert risk.effective_hold_buffer == pytest.approx(2.65)


def test_exit_decision_precedence_closeout_before_take_profit() -> None:
    decision = choose_exit_action(
        ExitInputs(
            hours_to_event=1.0,
            mark_to_market_pnl_cents=100.0,
            current_p_bucket_yes=0.9,
            entry_p_bucket_yes=0.7,
            held_side="yes",
            ev_now=0.5,
            take_profit_cents=10.0,
            stop_loss_cents=20.0,
            prob_drift_threshold=0.1,
            ev_gone_threshold=0.0,
            hold_buffer=2.0,
            boundary_mass=0.0,
            disagreement=0.0,
            spread_cents=1.0,
        )
    )

    assert decision.action == "close"
    assert decision.reason == "closeout_window"


def test_exit_decision_probability_drift_is_side_aware() -> None:
    yes_decision = choose_exit_action(
        ExitInputs(
            hours_to_event=5.0,
            mark_to_market_pnl_cents=0.0,
            current_p_bucket_yes=0.50,
            entry_p_bucket_yes=0.70,
            held_side="yes",
            ev_now=0.1,
            take_profit_cents=10.0,
            stop_loss_cents=20.0,
            prob_drift_threshold=0.1,
            ev_gone_threshold=0.0,
            hold_buffer=2.0,
            boundary_mass=0.0,
            disagreement=0.0,
            spread_cents=1.0,
        )
    )
    no_decision = choose_exit_action(
        ExitInputs(
            hours_to_event=5.0,
            mark_to_market_pnl_cents=0.0,
            current_p_bucket_yes=0.70,
            entry_p_bucket_yes=0.50,
            held_side="no",
            ev_now=0.1,
            take_profit_cents=10.0,
            stop_loss_cents=20.0,
            prob_drift_threshold=0.1,
            ev_gone_threshold=0.0,
            hold_buffer=2.0,
            boundary_mass=0.0,
            disagreement=0.0,
            spread_cents=1.0,
        )
    )

    assert yes_decision.reason == "probability_drift"
    assert no_decision.reason == "probability_drift"
