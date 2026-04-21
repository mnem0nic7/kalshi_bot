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


def make_signal(edge_bps: int = 100, *, capital_bucket: str = "safe", trade_regime: str = "standard") -> StrategySignal:
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
        risk_min_contract_price_dollars=0.05,
    )
    engine = DeterministicRiskEngine(settings)
    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.0200"),  # 2 cents — market says nearly impossible
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


def test_risk_engine_allows_when_under_per_ticker_count_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=500,
        risk_max_position_count_fp_per_ticker=200,
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
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("199.00"),
        ),
    )
    assert verdict.status == RiskStatus.APPROVED


def test_risk_engine_blocks_near_50_pct_probability() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
    )
    engine = DeterministicRiskEngine(settings)
    # fair_yes=0.64 is between 0.25 and 0.75 → blocked
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
    assert any("50%" in r for r in verdict.reasons)


def test_risk_engine_approves_extreme_probability() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=25.0,
    )
    engine = DeterministicRiskEngine(settings)
    # fair_yes=0.80 is > 0.75 → passes the extremity filter
    extreme_signal = replace(make_signal(edge_bps=200), fair_yes_dollars=Decimal("0.8000"))
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
