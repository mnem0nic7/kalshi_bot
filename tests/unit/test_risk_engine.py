from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, DeploymentColor, RiskStatus, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import PortfolioBucketSnapshot, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.weather.scoring import WeatherSignalSnapshot


def make_signal(edge_bps: int = 300, *, capital_bucket: str = "safe", trade_regime: str = "standard") -> StrategySignal:
    weather = WeatherSignalSnapshot(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        forecast_high_f=86,
        current_temp_f=78,
        forecast_delta_f=6.0,
        confidence_band="medium",
        trade_regime=trade_regime,
        resolution_state=WeatherResolutionState.UNRESOLVED,
        observation_time=datetime.now(UTC),
        forecast_updated_time=datetime.now(UTC),
        summary="test signal",
    )
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        edge_bps=edge_bps,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5800"),
        summary="test",
        weather=weather,
        trade_regime=trade_regime,
        capital_bucket=capital_bucket,
    )
    return signal


def make_room() -> Room:
    return Room(
        name="Test",
        market_ticker="WX-TEST",
        prompt=None,
        stage="triggered",
        active_color=DeploymentColor.BLUE.value,
        shadow_mode=False,
        kill_switch_enabled=False,
    )


def test_risk_engine_approves_fresh_small_trade() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )
    assert verdict.status == RiskStatus.APPROVED


def test_risk_engine_blocks_new_entries_when_source_health_pause_is_active() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(
            id="default",
            active_color="blue",
            kill_switch_enabled=False,
            notes={
                "source_health": {
                    "pause_new_entries": True,
                    "aggregate_label": "BROKEN",
                    "pause_reason": "aggregate source health BROKEN for 2 consecutive cycles",
                }
            },
        ),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert any("Source health pause is active" in reason for reason in verdict.reasons)


def test_risk_engine_blocks_when_position_limit_would_be_breached() -> None:
    """current_position_notional_dollars must be passed from the DB, not hardcoded to 0."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=50,  # tight cap
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    # Existing open position of $40 notional; new order adds $5.80 → total $45.80 < $50 → passes
    context_open = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_notional_dollars=Decimal("40.00"),
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=context_open,
    )
    assert verdict.status == RiskStatus.APPROVED

    # Existing open position of $46 notional; new order adds $5.80 → total $51.80 > $50 → blocked
    context_over = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_notional_dollars=Decimal("46.00"),
    )
    verdict_over = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=context_over,
    )
    assert verdict_over.status == RiskStatus.BLOCKED
    assert any("position" in r.lower() for r in verdict_over.reasons)


def test_risk_engine_blocks_stale_data() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_edge_bps=50)
    engine = DeterministicRiskEngine(settings)
    stale = datetime(2020, 1, 1, tzinfo=UTC)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(market_observed_at=stale, research_observed_at=stale),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("stale" in reason.lower() for reason in verdict.reasons)


def test_risk_engine_blocks_same_ticker_add_ons_by_default() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=False,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("5.00"),
            current_position_side="yes",
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert any("no pyramiding" in reason.lower() for reason in verdict.reasons)


def test_risk_engine_blocks_near_threshold_regime() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=20,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-RISKY",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(capital_bucket="risky", trade_regime="near_threshold"),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            portfolio_bucket_snapshot=PortfolioBucketSnapshot(
                total_capital_dollars="20.0000",
                overall_used_dollars="8.0000",
                overall_remaining_dollars="12.0000",
                safe_used_dollars="4.0000",
                safe_remaining_dollars="12.0000",
                safe_reserve_target_dollars="14.0000",
                risky_used_dollars="4.0000",
                risky_limit_dollars="6.0000",
                risky_remaining_dollars="2.0000",
            ),
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert any("near_threshold" in reason for reason in verdict.reasons)


def test_risk_engine_blocks_risky_trade_when_bucket_has_no_room() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=20,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-RISKY",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(capital_bucket="risky", trade_regime="near_threshold"),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            portfolio_bucket_snapshot=PortfolioBucketSnapshot(
                total_capital_dollars="20.0000",
                overall_used_dollars="19.5000",
                overall_remaining_dollars="0.5000",
                safe_used_dollars="13.5000",
                safe_remaining_dollars="0.5000",
                safe_reserve_target_dollars="14.0000",
                risky_used_dollars="6.0000",
                risky_limit_dollars="6.0000",
                risky_remaining_dollars="0.0000",
            ),
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert verdict.approved_count_fp is None
    assert any("capital bucket is full" in reason.lower() for reason in verdict.reasons)


def test_risk_engine_safe_trade_is_not_blocked_when_risky_bucket_is_full() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=20,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-SAFE",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("4.00"),
        ),
        signal=make_signal(capital_bucket="safe", trade_regime="standard"),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            portfolio_bucket_snapshot=PortfolioBucketSnapshot(
                total_capital_dollars="20.0000",
                overall_used_dollars="10.0000",
                overall_remaining_dollars="10.0000",
                safe_used_dollars="4.0000",
                safe_remaining_dollars="10.0000",
                safe_reserve_target_dollars="14.0000",
                risky_used_dollars="6.0000",
                risky_limit_dollars="6.0000",
                risky_remaining_dollars="0.0000",
            ),
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.capital_bucket == "safe"
    assert verdict.resized_by_bucket is False


def test_risk_engine_blocks_when_per_ticker_count_cap_reached() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=500,
        risk_max_position_count_fp_per_ticker=200,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("200.00"),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("200" in r for r in verdict.reasons)


def test_risk_engine_blocks_low_confidence_signal() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_confidence=0.70,
    )
    engine = DeterministicRiskEngine(settings)
    low_conf_signal = replace(make_signal(edge_bps=200), confidence=0.65)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=low_conf_signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("confidence" in r for r in verdict.reasons)


def test_risk_engine_blocks_below_minimum_contract_price() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_contract_price_dollars=0.25,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.2000"),  # 20 cents — still below the tightened 25-cent floor
            count_fp=Decimal("500.00"),
        ),
        signal=make_signal(edge_bps=300),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("nearly impossible" in r for r in verdict.reasons)
    assert any("0.25" in r for r in verdict.reasons)


def test_risk_engine_blocks_runaway_edge_as_model_error() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_credible_edge_bps=5000,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.0200"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(edge_bps=7382),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("credibility" in r for r in verdict.reasons)


def test_risk_engine_blocks_when_taker_fee_erases_min_edge() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(edge_bps=200),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert verdict.gross_edge_bps == 200
    assert verdict.fee_edge_bps is not None
    assert verdict.net_edge_bps is not None and verdict.net_edge_bps < 50
    assert any("Fee-adjusted edge" in reason for reason in verdict.reasons)


def test_risk_engine_allows_when_under_per_ticker_count_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=500,
        risk_max_position_count_fp_per_ticker=200,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=True,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("199.00"),
        ),
    )
    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("1.00")
    assert verdict.resized_by_count_cap is True


def test_risk_engine_blocks_when_resized_ticket_loses_fee_adjusted_edge() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=500,
        risk_max_position_count_fp_per_ticker=200,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=True,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(edge_bps=240),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("199.00"),
            current_position_side="yes",
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert verdict.net_edge_bps == 40
    assert verdict.fee_edge_bps == 200
    assert any("Fee-adjusted edge 40bps" in reason for reason in verdict.reasons)


def test_risk_engine_blocks_midband_probability_with_weak_edge() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
        risk_probability_midband_max_extra_edge_bps=500,
    )
    engine = DeterministicRiskEngine(settings)
    # fair_yes=0.64 needs about 270bps under the edge-scaled midband policy.
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(edge_bps=200),
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("requires 270bps edge" in r and "actual edge is 20bps" in r for r in verdict.reasons)


def test_risk_engine_approves_midband_probability_with_strong_edge() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
        risk_probability_midband_max_extra_edge_bps=500,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(edge_bps=500),
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )

    assert verdict.status == RiskStatus.APPROVED


def test_risk_engine_probability_boundaries_pass() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
        risk_probability_midband_max_extra_edge_bps=500,
    )
    engine = DeterministicRiskEngine(settings)

    for fair_yes in (Decimal("0.2500"), Decimal("0.7500")):
        signal = replace(make_signal(edge_bps=300), fair_yes_dollars=fair_yes)
        verdict = engine.evaluate(
            room=make_room(),
            control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
            ticket=TradeTicket(
                market_ticker="WX-TEST",
                action=TradeAction.BUY,
                side=ContractSide.YES,
                yes_price_dollars=Decimal("0.5800"),
                count_fp=Decimal("10.00"),
            ),
            signal=signal,
            context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
        )
        assert verdict.status == RiskStatus.APPROVED


def test_risk_engine_midpoint_requires_full_extra_edge() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
        risk_probability_midband_max_extra_edge_bps=500,
    )
    engine = DeterministicRiskEngine(settings)
    signal = replace(make_signal(edge_bps=549), fair_yes_dollars=Decimal("0.5000"))
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=signal,
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert any("requires 550bps edge" in r and "actual edge is 369bps" in r for r in verdict.reasons)


def test_risk_engine_approves_extreme_probability() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
    )
    engine = DeterministicRiskEngine(settings)
    # fair_yes=0.80 is > 0.75 → passes the extremity filter
    extreme_signal = replace(make_signal(edge_bps=300), fair_yes_dollars=Decimal("0.8000"))
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=extreme_signal,
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )
    assert verdict.status == RiskStatus.APPROVED


def test_risk_engine_blocks_when_pending_orders_reach_cap() -> None:
    """Concurrent rooms with in-flight orders must not collectively exceed the position cap."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_position_count_fp_per_ticker=200,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    # Filled position is 0 (no fills yet), but in-flight resting orders total 200 → at cap → block.
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("0"),
            pending_order_count_fp=Decimal("200.00"),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("in-flight" in r for r in verdict.reasons)


def test_risk_engine_downsizes_when_projected_count_would_exceed_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_position_count_fp_per_ticker=200,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=True,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("195.00"),
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("5.00")
    assert verdict.resized_by_count_cap is True


def test_risk_engine_combines_filled_and_pending_for_cap() -> None:
    """Position cap check sums filled position and in-flight orders."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_position_count_fp_per_ticker=200,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    # 100 filled + 100 pending = 200 → at cap → blocked
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("100.00"),
            pending_order_count_fp=Decimal("100.00"),
        ),
    )
    assert verdict.status == RiskStatus.BLOCKED


# ---------------------------------------------------------------------------
# Per-strategy daily-loss envelope (P1-1)
# ---------------------------------------------------------------------------


def _base_settings(**overrides):
    return Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        **overrides,
    )


def _base_eval(engine, context):
    return engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("10.00"),
        ),
        signal=make_signal(),
        context=context,
    )


def test_per_strategy_cap_blocks_when_realized_loss_meets_cap() -> None:
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 100.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=Decimal("-100.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.BLOCKED
    assert any("daily cap" in reason for reason in verdict.reasons)


def test_per_strategy_cap_blocks_when_realized_loss_exceeds_cap() -> None:
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 50.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=Decimal("-125.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.BLOCKED


def test_per_strategy_cap_allows_when_realized_loss_under_cap() -> None:
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 100.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=Decimal("-50.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


def test_per_strategy_cap_allows_positive_pnl() -> None:
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 100.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=Decimal("42.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


def test_per_strategy_cap_for_strategy_a_does_not_block_strategy_c() -> None:
    """A cap configured for A must not impact a Strategy-C trade, even if C is
    currently bleeding hard. Each strategy gets its own envelope."""
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 100.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="C",
        strategy_daily_realized_pnl_dollars=Decimal("-500.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


def test_per_strategy_cap_no_op_when_no_cap_configured() -> None:
    settings = _base_settings()  # no risk_daily_loss_dollars_by_strategy
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=Decimal("-1000.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


def test_per_strategy_cap_no_op_when_pnl_is_none() -> None:
    """If the caller couldn't compute P&L (e.g. fresh DB with no fills yet),
    the cap doesn't trigger — absence of data is not a blocker."""
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 1.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code="A",
        strategy_daily_realized_pnl_dollars=None,
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


def test_per_strategy_cap_no_op_when_strategy_code_is_none() -> None:
    """Replay/training callers that don't tag a strategy_code must not be
    blocked by the per-strategy cap."""
    settings = _base_settings(risk_daily_loss_dollars_by_strategy={"A": 1.0})
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        strategy_code=None,
        strategy_daily_realized_pnl_dollars=Decimal("-1000.00"),
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED


# ---------------------------------------------------------------------------
# Opposite-side guard tests
# ---------------------------------------------------------------------------

def _opp_side_settings(**overrides) -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=True,
        **overrides,
    )


def test_blocks_no_entry_when_yes_position_open() -> None:
    """Buying NO while a YES position is open must be BLOCKED by the opposite-side guard."""
    settings = _opp_side_settings()
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("5.00"),
        current_position_side="yes",
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.4200"),
            count_fp=Decimal("5.00"),
        ),
        signal=make_signal(),
        context=context,
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("opposite-side" in r for r in verdict.reasons)


def test_blocks_yes_entry_when_no_position_open() -> None:
    """Buying YES while a NO position is open must be BLOCKED."""
    settings = _opp_side_settings()
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("8.00"),
        current_position_side="no",
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("5.00"),
        ),
        signal=make_signal(),
        context=context,
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("opposite-side" in r for r in verdict.reasons)


def test_same_side_pyramid_still_allowed() -> None:
    """Adding to an existing YES position (pyramid) must be APPROVED when add-ons are enabled."""
    settings = _opp_side_settings()
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("5.00"),
        current_position_side="yes",
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("5.00"),
        ),
        signal=make_signal(),
        context=context,
    )
    assert verdict.status == RiskStatus.APPROVED


def test_no_position_no_guard_fires() -> None:
    """With no open position the opposite-side guard must not fire."""
    settings = _opp_side_settings()
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("0"),
        current_position_side=None,
    )
    verdict = _base_eval(engine, context)
    assert verdict.status == RiskStatus.APPROVED
    assert not any("opposite-side" in r for r in verdict.reasons)


def test_guard_and_addons_disabled_produce_distinct_reasons() -> None:
    """When add-ons are disabled AND opposite side is held, both reasons appear separately."""
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=False,
    )
    engine = DeterministicRiskEngine(settings)
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("5.00"),
        current_position_side="no",
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("5.00"),
        ),
        signal=make_signal(),
        context=context,
    )
    assert verdict.status == RiskStatus.BLOCKED
    reason_text = " ".join(verdict.reasons)
    assert "add-on" in reason_text or "add_on" in reason_text or "existing" in reason_text.lower()
    assert "opposite-side" in reason_text


def test_guard_reads_position_side_for_queried_row() -> None:
    """The guard uses current_position_side from RiskContext, not the ticket side."""
    settings = _opp_side_settings()
    engine = DeterministicRiskEngine(settings)
    # side in context is "yes", ticket is buying NO — guard must fire
    context = RiskContext(
        market_observed_at=datetime.now(UTC),
        research_observed_at=datetime.now(UTC),
        current_position_count_fp=Decimal("3.00"),
        current_position_side="yes",
    )
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.4200"),
            count_fp=Decimal("3.00"),
        ),
        signal=make_signal(),
        context=context,
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("opposite-side" in r for r in verdict.reasons)
