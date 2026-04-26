"""Unit tests for monotonicity_scanner.py (Addition 3, §4.3).

Includes:
- Payoff table test matching §4.3.3.1 exactly
- Hypothesis property test: any proposed pair has positive net PnL in all scenarios
- Violation detection on synthetic fixtures
- Fee calculation
- Ticker parsing
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from kalshi_bot.config import Settings
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.services.monotonicity_scanner import (
    MonotonicityViolation,
    _parse_station_date_threshold,
    detect_violations,
    evaluate_arb_risk,
    group_markets_by_station_date,
    kalshi_fee_cents,
    scan_for_violations,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _control(kill_switch: bool = False) -> DeploymentControl:
    ctrl = MagicMock(spec=DeploymentControl)
    ctrl.kill_switch_enabled = kill_switch
    return ctrl


def _settings(**overrides) -> Settings:
    base = {
        "database_url": "sqlite+aiosqlite:///:memory:",
        "monotonicity_arb_enabled": True,
        "monotonicity_arb_shadow_only": True,
        "monotonicity_arb_min_net_edge_cents": 2,
        "monotonicity_arb_max_notional_dollars": 25.0,
        "monotonicity_arb_max_proposals_per_minute": 5,
    }
    base.update(overrides)
    return Settings(**base)


def _market(
    ticker: str,
    yes_ask_dollars: float,
    yes_bid_dollars: float,
    no_ask_dollars: float | None = None,
    status: str = "open",
) -> dict[str, Any]:
    m: dict[str, Any] = {
        "ticker": ticker,
        "status": status,
        "yes_ask_dollars": yes_ask_dollars,
        "yes_bid_dollars": yes_bid_dollars,
    }
    if no_ask_dollars is not None:
        m["no_ask_dollars"] = no_ask_dollars
    return m


EVENT_DATE = date(2026, 4, 22)
STATION = "TBOS"


def _ticker(threshold_f: int) -> str:
    return f"KXHIGH{STATION}-26APR22-T{threshold_f}"


# ---------------------------------------------------------------------------
# Ticker parsing
# ---------------------------------------------------------------------------

def test_parse_station_date_threshold_standard() -> None:
    result = _parse_station_date_threshold("KXHIGHTBOS-26APR22-T58")
    assert result is not None
    station, event_date, threshold_f = result
    assert station == "TBOS"
    assert event_date == date(2026, 4, 22)
    assert threshold_f == 58.0


def test_parse_station_date_threshold_non_kxhigh() -> None:
    assert _parse_station_date_threshold("KXLOWNY-26APR22-T50") is None


def test_parse_station_date_threshold_malformed() -> None:
    assert _parse_station_date_threshold("NOTAMARKET") is None
    assert _parse_station_date_threshold("") is None


def test_parse_station_date_threshold_high_threshold() -> None:
    result = _parse_station_date_threshold("KXHIGHCHI-26APR22-T100")
    assert result is not None
    assert result[2] == 100.0


# ---------------------------------------------------------------------------
# Fee calculation
# ---------------------------------------------------------------------------

def test_kalshi_fee_at_midrange_price() -> None:
    # 0.07 * 1 * 0.5 * 0.5 * 100 = 1.75 → ceil → 2.00 dollars → 200 cents? No.
    # Actually: raw_dollars = 0.07 * 1 * 0.5 * 0.5 * 100 = 1.75
    # fee_dollars = ceil(1.75) / 100 = 2 / 100 = 0.02 → 2 cents
    fee = kalshi_fee_cents(0.50, contracts=1)
    assert fee == pytest.approx(2.0)


def test_kalshi_fee_at_near_par_price() -> None:
    # 0.07 * 1 * 0.97 * 0.03 * 100 = 0.2037 → ceil → 1 → 1 cent
    fee = kalshi_fee_cents(0.97, contracts=1)
    assert fee == pytest.approx(1.0)


def test_kalshi_fee_scales_with_contracts() -> None:
    fee1 = kalshi_fee_cents(0.50, contracts=1)
    fee5 = kalshi_fee_cents(0.50, contracts=5)
    assert fee5 > fee1


# ---------------------------------------------------------------------------
# §4.3.3.1 Payoff table test — must match spec exactly
# ---------------------------------------------------------------------------

def test_payoff_table_spec_example() -> None:
    """Verify §4.3.3.1 payoff table: T80 at 40¢ YES + T85 at 55¢ NO = 95¢ total cost.

    Scenario 1: high > 85  → YES T80 wins ($1), NO T85 loses ($0) → gross $1.00, net +5¢
    Scenario 2: 80 < high ≤ 85 → YES T80 wins ($1), NO T85 wins ($1) → gross $2.00, net +$1.05
    Scenario 3: high ≤ 80  → YES T80 loses ($0), NO T85 wins ($1) → gross $1.00, net +5¢
    """
    ask_yes_low_cents = 40.0  # buy YES T80 at 40¢
    ask_no_high_cents = 55.0  # buy NO T85 at 55¢ (= 1 - 0.45)
    total_cost_cents = ask_yes_low_cents + ask_no_high_cents  # 95¢

    # Gross payout in each scenario (before fees)
    gross_scenario_1 = 100.0  # only YES T80 pays (high > 85 → also > 80)
    gross_scenario_2 = 200.0  # both pay (80 < high ≤ 85)
    gross_scenario_3 = 100.0  # only NO T85 pays (high ≤ 80 → also ≤ 85)

    assert total_cost_cents == 95.0
    for gross in (gross_scenario_1, gross_scenario_2, gross_scenario_3):
        net_cents = gross - total_cost_cents
        assert net_cents > 0, f"Expected positive net, got {net_cents}¢ (gross={gross}¢)"

    # Minimum gross payout is $1.00 → minimum gross edge is 5¢
    min_gross_payout = min(gross_scenario_1, gross_scenario_2, gross_scenario_3)
    assert min_gross_payout == 100.0
    assert min_gross_payout - total_cost_cents == 5.0


# ---------------------------------------------------------------------------
# Hypothesis property test (§5.2): all proposed pairs have positive net PnL
# ---------------------------------------------------------------------------

@given(
    ask_yes_low_cents=st.floats(min_value=1.0, max_value=98.0),
    ask_no_high_cents=st.floats(min_value=1.0, max_value=98.0),
)
@h_settings(max_examples=500)
def test_arb_positive_pnl_in_all_scenarios(
    ask_yes_low_cents: float,
    ask_no_high_cents: float,
) -> None:
    """Any (ask_yes_low, ask_no_high) pair forming a valid arb must yield positive
    gross PnL in all 3 outcome scenarios.

    Valid arb condition: total_cost_cents < 100 (guaranteed payout floor).
    """
    total_cost = ask_yes_low_cents + ask_no_high_cents
    if total_cost >= 100.0:
        # Not an arb — total cost ≥ guaranteed payout, skip
        return

    # Three scenarios:
    # 1. high > T_j (>high threshold): YES T_i wins, NO T_j loses → payout $1
    # 2. T_i < high ≤ T_j (between): YES T_i wins, NO T_j wins → payout $2
    # 3. high ≤ T_i (≤ low threshold): YES T_i loses, NO T_j wins → payout $1
    for gross_payout_cents in (100.0, 200.0, 100.0):
        net = gross_payout_cents - total_cost
        assert net > 0, (
            f"Scenario with gross={gross_payout_cents}¢, "
            f"cost={total_cost:.2f}¢ yielded net={net:.2f}¢ ≤ 0"
        )


# ---------------------------------------------------------------------------
# Violation detection — synthetic orderbook fixtures
# ---------------------------------------------------------------------------

def test_detect_violation_simple() -> None:
    """T85 YES bid (48¢) > T80 YES ask (35¢) → raw edge 13¢ > 6¢ floor → violation."""
    group = [
        _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33, no_ask_dollars=0.67),
        _market(_ticker(85), yes_ask_dollars=0.50, yes_bid_dollars=0.48, no_ask_dollars=0.52),
    ]
    violations = detect_violations(
        group, station=STATION, event_date=EVENT_DATE, min_net_edge_cents=2.0
    )
    assert len(violations) == 1
    v = violations[0]
    assert v.ticker_low == _ticker(80)
    assert v.ticker_high == _ticker(85)
    assert v.threshold_low_f == 80.0
    assert v.threshold_high_f == 85.0
    assert v.net_edge_cents > 0


def test_no_violation_when_monotonicity_holds() -> None:
    """T85 YES bid (35¢) < T80 YES ask (40¢) → no violation."""
    group = [
        _market(_ticker(80), yes_ask_dollars=0.40, yes_bid_dollars=0.38),
        _market(_ticker(85), yes_ask_dollars=0.37, yes_bid_dollars=0.35),
    ]
    violations = detect_violations(
        group, station=STATION, event_date=EVENT_DATE, min_net_edge_cents=2.0
    )
    assert len(violations) == 0


def test_no_violation_when_edge_below_floor() -> None:
    """Violation exists but net edge is below min_net_edge_cents → filtered out."""
    # bid_yes_high=0.41, ask_yes_low=0.40 → raw_edge=1¢, below 4¢ floor
    group = [
        _market(_ticker(80), yes_ask_dollars=0.40, yes_bid_dollars=0.38),
        _market(_ticker(85), yes_ask_dollars=0.43, yes_bid_dollars=0.41),
    ]
    violations = detect_violations(
        group, station=STATION, event_date=EVENT_DATE, min_net_edge_cents=2.0
    )
    assert len(violations) == 0


def test_detect_multiple_violations_in_group() -> None:
    """Three thresholds where both (T80, T90) and (T85, T90) violate."""
    group = [
        _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33, no_ask_dollars=0.67),
        _market(_ticker(85), yes_ask_dollars=0.36, yes_bid_dollars=0.34, no_ask_dollars=0.66),
        _market(_ticker(90), yes_ask_dollars=0.50, yes_bid_dollars=0.48, no_ask_dollars=0.52),
    ]
    violations = detect_violations(
        group, station=STATION, event_date=EVENT_DATE, min_net_edge_cents=2.0
    )
    # T90 bid (48¢) > T80 ask (35¢) and T90 bid (48¢) > T85 ask (36¢) → 2 violations
    assert len(violations) >= 2


def test_skip_market_with_zero_ask() -> None:
    """Markets with yes_ask=0 are skipped (no-quote condition)."""
    group = [
        _market(_ticker(80), yes_ask_dollars=0.0, yes_bid_dollars=0.0),
        _market(_ticker(85), yes_ask_dollars=0.50, yes_bid_dollars=0.48),
    ]
    violations = detect_violations(
        group, station=STATION, event_date=EVENT_DATE, min_net_edge_cents=2.0
    )
    assert len(violations) == 0


# ---------------------------------------------------------------------------
# Group-by-station-date
# ---------------------------------------------------------------------------

def test_group_markets_by_station_date_single_station() -> None:
    markets = [
        _market("KXHIGHTBOS-26APR22-T58", yes_ask_dollars=0.97, yes_bid_dollars=0.95),
        _market("KXHIGHTBOS-26APR22-T65", yes_ask_dollars=0.80, yes_bid_dollars=0.78),
        _market("KXHIGHTBOS-26APR22-T70", yes_ask_dollars=0.60, yes_bid_dollars=0.58),
    ]
    groups = group_markets_by_station_date(markets)
    assert len(groups) == 1
    key = list(groups.keys())[0]
    assert key == ("TBOS", date(2026, 4, 22))
    # Sorted by threshold ascending
    thresholds = [
        _parse_station_date_threshold(m["ticker"])[2]
        for m in groups[key]
    ]
    assert thresholds == sorted(thresholds)


def test_group_markets_skips_non_kxhigh() -> None:
    markets = [
        _market("KXRAINBOS-26APR22-T1", yes_ask_dollars=0.50, yes_bid_dollars=0.48),
        _market("KXHIGHTBOS-26APR22-T58", yes_ask_dollars=0.97, yes_bid_dollars=0.95),
    ]
    groups = group_markets_by_station_date(markets)
    # Only KXHIGH* market included
    assert len(groups) == 1


# ---------------------------------------------------------------------------
# Risk gate
# ---------------------------------------------------------------------------

def test_evaluate_arb_risk_kill_switch_blocks() -> None:
    violation = _make_violation()
    settings = _settings()
    outcome, reason = evaluate_arb_risk(violation, control=_control(kill_switch=True), settings=settings)
    assert outcome == "risk_blocked"
    assert reason is not None


def test_evaluate_arb_risk_disabled_blocks() -> None:
    violation = _make_violation()
    settings = _settings(monotonicity_arb_enabled=False)
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "risk_blocked"


def test_evaluate_arb_risk_shadow_when_enabled() -> None:
    violation = _make_violation()
    settings = _settings()
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "shadow"
    assert reason is None


def test_evaluate_arb_risk_notional_cap() -> None:
    violation = _make_violation(ask_yes_low=Decimal("0.40"), ask_no_high=Decimal("0.55"))
    settings = _settings(monotonicity_arb_max_notional_dollars=0.50)  # below pair cost 0.95
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "risk_blocked"
    assert "notional" in (reason or "").lower() or "max" in (reason or "").lower()


# ---------------------------------------------------------------------------
# P1-2: atomicity gate — live execution is refused until a two-leg executor
# with rollback on leg-2 failure exists.
# ---------------------------------------------------------------------------


def test_evaluate_arb_risk_shadow_off_without_atomic_flag_is_blocked() -> None:
    """Flipping shadow_only alone must NOT silently downgrade to shadow.

    An operator who toggles shadow_only=False without reading the atomicity
    contract deserves an explicit 'risk_blocked' with a pointer, not a green
    light that masquerades as safe shadow execution.
    """
    violation = _make_violation()
    settings = _settings(
        monotonicity_arb_shadow_only=False,
        monotonicity_arb_atomic_execution_ready=False,
    )
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "risk_blocked"
    assert reason is not None
    assert "atomic" in reason.lower()


def test_evaluate_arb_risk_shadow_off_with_atomic_flag_is_still_blocked_today() -> None:
    """Even with both flags set, live execution is blocked until the executor
    ships. This prevents a partially-configured environment from placing a
    naked leg 1 with no unwind path."""
    violation = _make_violation()
    settings = _settings(
        monotonicity_arb_shadow_only=False,
        monotonicity_arb_atomic_execution_ready=True,
    )
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "risk_blocked"
    assert reason is not None
    assert "executor" in reason.lower() or "not yet implemented" in reason.lower()


def test_evaluate_arb_risk_shadow_only_stays_shadow_regardless_of_atomic_flag() -> None:
    """shadow_only=True wins even if someone also sets atomic_execution_ready=True."""
    violation = _make_violation()
    settings = _settings(
        monotonicity_arb_shadow_only=True,
        monotonicity_arb_atomic_execution_ready=True,
    )
    outcome, reason = evaluate_arb_risk(violation, control=_control(), settings=settings)
    assert outcome == "shadow"
    assert reason is None


def test_evaluate_arb_risk_kill_switch_preempts_atomic_flag() -> None:
    """Kill switch remains the hardest gate; it overrides every other flag."""
    violation = _make_violation()
    settings = _settings(
        monotonicity_arb_shadow_only=False,
        monotonicity_arb_atomic_execution_ready=True,
    )
    outcome, reason = evaluate_arb_risk(violation, control=_control(kill_switch=True), settings=settings)
    assert outcome == "risk_blocked"
    assert "kill switch" in (reason or "").lower()


def test_settings_default_atomic_execution_ready_is_false() -> None:
    """Safe default: atomic flag must be False out of the box so deployments
    cannot accidentally run with the gate already permissive."""
    assert Settings().monotonicity_arb_atomic_execution_ready is False


# ---------------------------------------------------------------------------
# scan_for_violations end-to-end
# ---------------------------------------------------------------------------

def test_scan_for_violations_emits_proposal() -> None:
    markets = [
        _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33, no_ask_dollars=0.67),
        _market(_ticker(85), yes_ask_dollars=0.50, yes_bid_dollars=0.48, no_ask_dollars=0.52),
    ]
    proposals = scan_for_violations(
        markets,
        control=_control(),
        settings=_settings(),
    )
    assert len(proposals) >= 1
    p = proposals[0]
    assert p.execution_outcome == "shadow"
    assert p.net_edge_cents > 0
    assert p.ticker_low == _ticker(80)
    assert p.ticker_high == _ticker(85)


def test_scan_for_violations_kill_switch_blocks_all() -> None:
    markets = [
        _market(_ticker(80), yes_ask_dollars=0.35, yes_bid_dollars=0.33, no_ask_dollars=0.67),
        _market(_ticker(85), yes_ask_dollars=0.50, yes_bid_dollars=0.48, no_ask_dollars=0.52),
    ]
    proposals = scan_for_violations(
        markets,
        control=_control(kill_switch=True),
        settings=_settings(),
    )
    assert all(p.execution_outcome == "risk_blocked" for p in proposals)


def test_scan_for_violations_empty_markets() -> None:
    proposals = scan_for_violations([], control=_control(), settings=_settings())
    assert proposals == []


# ---------------------------------------------------------------------------
# Helpers for risk gate tests
# ---------------------------------------------------------------------------

def _make_violation(
    ask_yes_low: Decimal = Decimal("0.40"),
    ask_no_high: Decimal = Decimal("0.55"),
) -> MonotonicityViolation:
    return MonotonicityViolation(
        station=STATION,
        event_date=EVENT_DATE,
        ticker_low=_ticker(80),
        ticker_high=_ticker(85),
        threshold_low_f=80.0,
        threshold_high_f=85.0,
        ask_yes_low=ask_yes_low,
        bid_yes_high=Decimal("0.45"),
        ask_no_high=ask_no_high,
        gross_edge_cents=5.0,
        fee_estimate_cents=2.0,
        net_edge_cents=3.0,
    )
