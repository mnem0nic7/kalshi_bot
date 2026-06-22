from decimal import Decimal

from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.weather.lockin_gate import evaluate_lockin_fee_edge_gate


def test_unresolved_state_never_trades() -> None:
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.UNRESOLVED,
        yes_ask_dollars=Decimal("0.10"),
        no_ask_dollars=Decimal("0.90"),
    )
    assert result.should_trade is False
    assert result.reason == "not_locked"


def test_locked_yes_with_cheap_yes_ask_trades() -> None:
    # high already crossed the strike → YES is certain; market still offers YES at 0.90.
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_YES,
        yes_ask_dollars=Decimal("0.90"),
        no_ask_dollars=Decimal("0.12"),
        count=Decimal("1"),
        min_edge_dollars=Decimal("0.02"),
    )
    assert result.should_trade is True
    assert result.side == "yes"
    assert result.entry_price_dollars == Decimal("0.90")
    assert result.gross_edge_dollars == Decimal("0.10")
    # fee = ceil(0.07 * 1 * 0.90 * 0.10) cents = ceil(0.63c) = 0.01
    assert result.fee_dollars == Decimal("0.01")
    assert result.net_edge_dollars == Decimal("0.09")
    assert result.reason == "ok"


def test_locked_no_picks_the_no_side() -> None:
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_NO,
        yes_ask_dollars=Decimal("0.88"),
        no_ask_dollars=Decimal("0.85"),
        min_edge_dollars=Decimal("0.02"),
    )
    assert result.should_trade is True
    assert result.side == "no"
    assert result.entry_price_dollars == Decimal("0.85")
    assert result.gross_edge_dollars == Decimal("0.15")
    assert result.reason == "ok"


def test_edge_below_min_threshold_does_not_trade() -> None:
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_YES,
        yes_ask_dollars=Decimal("0.99"),
        no_ask_dollars=Decimal("0.02"),
        min_edge_dollars=Decimal("0.03"),
    )
    assert result.should_trade is False
    assert result.reason == "below_min_edge"


def test_fee_exceeds_edge_does_not_trade() -> None:
    # tiny cushion (0.001) is wiped out by the 1-cent fee ceiling; isolate via min_edge=0.
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_YES,
        yes_ask_dollars=Decimal("0.999"),
        no_ask_dollars=Decimal("0.001"),
        min_edge_dollars=Decimal("0"),
    )
    assert result.should_trade is False
    assert result.reason == "fee_exceeds_edge"


def test_missing_quote_does_not_trade() -> None:
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_YES,
        yes_ask_dollars=None,
        no_ask_dollars=Decimal("0.10"),
    )
    assert result.should_trade is False
    assert result.reason == "no_quote"


def test_count_scales_net_edge() -> None:
    result = evaluate_lockin_fee_edge_gate(
        resolution_state=WeatherResolutionState.LOCKED_YES,
        yes_ask_dollars=Decimal("0.90"),
        no_ask_dollars=Decimal("0.12"),
        count=Decimal("5"),
        min_edge_dollars=Decimal("0.02"),
    )
    # gross = 0.10 * 5 = 0.50 ; fee = ceil(0.07*5*0.9*0.1)=ceil(3.15c)=0.04 ; net = 0.46
    assert result.gross_edge_dollars == Decimal("0.50")
    assert result.fee_dollars == Decimal("0.04")
    assert result.net_edge_dollars == Decimal("0.46")
    assert result.should_trade is True
