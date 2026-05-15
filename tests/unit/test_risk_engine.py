from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, DeploymentColor, RiskStatus, StrategyCode, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import PortfolioBucketSnapshot, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.decision_policy_variants import LOW_PRICE_HIGH_EDGE, REMAINING_PAYOUT_RELAXATION
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


def make_sell_signal(edge_bps: int = 300) -> StrategySignal:
    return replace(make_signal(edge_bps), recommended_action=TradeAction.SELL)


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


def make_production_room() -> Room:
    room = make_room()
    room.kalshi_env = "production"
    return room


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


def test_risk_engine_allows_late_high_confidence_crypto_edge_bypass() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=500,
        risk_min_probability_extremity_pct=0.0,
    )
    signal = make_signal(edge_bps=-200)
    signal.recommended_side = ContractSide.NO
    signal.target_yes_price_dollars = Decimal("0.0300")
    signal.fair_yes_dollars = Decimal("0.0500")
    signal.confidence = 0.95
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "late_high_confidence_directional_entry": True,
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-LATE-SURE",
            action=TradeAction.BUY,
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.0300"),
            count_fp=Decimal("1.00"),
        ),
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("1000.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
        thresholds=RuntimeThresholds(
            risk_min_edge_bps=500,
            risk_max_order_notional_dollars=100,
            risk_max_position_notional_dollars=300,
            trigger_max_spread_bps=250,
            trigger_cooldown_seconds=300,
            strategy_quality_edge_buffer_bps=25,
            strategy_min_remaining_payout_bps=0,
            risk_min_confidence=0.80,
            risk_min_contract_price_dollars=0.50,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert "late_high_confidence_edge_bypass" in verdict.reason_codes
    assert "late_high_confidence_fee_adjusted_edge_bypass" in verdict.reason_codes


def test_risk_engine_allows_last_minute_passive_edge_bypass_and_normal_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=500,
        risk_min_probability_extremity_pct=0.0,
        risk_position_pct=0.10,
        risk_max_order_notional_dollars=100,
        strategy_min_remaining_payout_bps=0,
    )
    signal = make_signal(edge_bps=-100)
    signal.target_yes_price_dollars = Decimal("0.5500")
    signal.fair_yes_dollars = Decimal("0.2000")
    signal.confidence = 0.90
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "candidate_status": "live_quality",
        "last_minute_passive_market_confidence": True,
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-LAST-MINUTE",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5500"),
            count_fp=Decimal("100.00"),
        ),
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("100.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("18.18")
    assert "last_minute_passive_edge_bypass" in verdict.reason_codes
    assert "last_minute_passive_fee_adjusted_edge_bypass" in verdict.reason_codes
    assert "position_notional_cap_resized" in verdict.reason_codes


def test_risk_engine_blocks_duplicate_last_minute_passive_open_order() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=500,
        risk_min_probability_extremity_pct=0.0,
        strategy_min_remaining_payout_bps=0,
    )
    signal = make_signal(edge_bps=100)
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "candidate_status": "live_quality",
        "last_minute_passive_market_confidence": True,
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-LAST-MINUTE",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5500"),
            count_fp=Decimal("1.00"),
        ),
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("100.00"),
            pending_order_count_fp=Decimal("1.00"),
            pending_order_notional_dollars=Decimal("0.5500"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "last_minute_passive_duplicate_open_order" in verdict.reason_codes


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


def test_risk_engine_blocks_production_entries_during_trade_behavior_freeze() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        trade_behavior_production_entry_freeze_enabled=True,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_production_room(),
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

    assert verdict.status == RiskStatus.BLOCKED
    assert any("trade_behavior_retraining_freeze" in reason for reason in verdict.reasons)


def test_risk_engine_applies_weather_live_order_count_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=1000,
        risk_max_position_notional_dollars=1000,
        risk_max_order_count_fp=500,
        risk_max_position_count_fp_per_ticker=1000,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        trade_behavior_production_entry_freeze_enabled=True,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_production_room(),
        control=DeploymentControl(
            id="default",
            active_color="blue",
            kill_switch_enabled=False,
            notes={
                "weather_live": {
                    "enabled": True,
                    "policy_state": "live",
                    "max_order_count_fp": 200,
                }
            },
        ),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("250.00"),
        ),
        signal=make_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            strategy_code="A",
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert any("Ticket size exceeds max order count" in reason for reason in verdict.reasons)
    assert verdict.diagnostics["max_order_count_fp"]["configured"] == 500.0
    assert verdict.diagnostics["max_order_count_fp"]["effective"] == 200.0
    assert not any("trade_behavior_retraining_freeze" in reason for reason in verdict.reasons)


def test_risk_engine_allows_risk_reducing_sell_during_freeze_and_kill_switch() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_production_room(),
        control=DeploymentControl(
            id="default",
            active_color="blue",
            kill_switch_enabled=True,
            notes={
                "source_health": {
                    "pause_new_entries": True,
                    "pause_reason": "source outage",
                }
            },
        ),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.SELL,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("5.00"),
        ),
        signal=make_sell_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_count_fp=Decimal("10.00"),
            current_position_side="yes",
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert any("risk-reducing exit is allowed" in reason for reason in verdict.reasons)
    assert not any("trade_behavior_retraining_freeze" in reason for reason in verdict.reasons)
    assert not any("Source health pause" in reason for reason in verdict.reasons)


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


def test_risk_engine_downsizes_below_one_contract_to_fit_capital_position_pct() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_position_pct=0.10,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("1.00"),
        ),
        signal=replace(
            make_signal(edge_bps=500),
            target_yes_price_dollars=Decimal("0.5000"),
            fair_yes_dollars=Decimal("0.6500"),
        ),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("3.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("0.60")
    assert verdict.approved_notional_dollars == Decimal("0.3000")
    assert "position_notional_cap_resized" in verdict.reason_codes


def test_risk_engine_counts_pending_notional_against_capital_position_pct() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_position_pct=0.10,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("1.00"),
        ),
        signal=replace(
            make_signal(edge_bps=500),
            target_yes_price_dollars=Decimal("0.5000"),
            fair_yes_dollars=Decimal("0.6500"),
        ),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("5.00"),
            pending_order_count_fp=Decimal("0.60"),
            pending_order_notional_dollars=Decimal("0.3000"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("0.40")
    assert verdict.approved_notional_dollars == Decimal("0.2000")
    assert verdict.diagnostics["position_notional_cap"]["pending_order_notional_dollars"] == "0.3000"


def test_risk_engine_downsizes_dynamic_crypto_ticket_to_position_pct_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_position_pct=0.10,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("30.00"),
        ),
        signal=replace(
            make_signal(edge_bps=500),
            target_yes_price_dollars=Decimal("0.5000"),
            fair_yes_dollars=Decimal("0.6500"),
        ),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("100.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("20.00")
    assert verdict.approved_notional_dollars == Decimal("10.0000")
    assert "position_notional_cap_resized" in verdict.reason_codes


def test_risk_engine_blocks_when_capital_position_pct_has_no_room() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_allow_position_add_ons=True,
        risk_position_pct=0.10,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("1.00"),
        ),
        signal=replace(
            make_signal(edge_bps=500),
            target_yes_price_dollars=Decimal("0.5000"),
            fair_yes_dollars=Decimal("0.6500"),
        ),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("3.00"),
            current_position_notional_dollars=Decimal("0.3000"),
            current_position_count_fp=Decimal("0.60"),
            current_position_side="yes",
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "position_notional_cap_reached" in verdict.reason_codes


def test_risk_engine_blocks_crypto_entry_when_total_capital_is_missing() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_position_pct=0.10,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("1.00"),
        ),
        signal=replace(
            make_signal(edge_bps=500),
            target_yes_price_dollars=Decimal("0.5000"),
            fair_yes_dollars=Decimal("0.6500"),
        ),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "position_notional_cap_missing_capital" in verdict.reason_codes


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


def _crypto_add_on_signal(*, asset: str = "BTC", side: ContractSide = ContractSide.YES) -> StrategySignal:
    signal = make_signal(edge_bps=1200)
    signal.recommended_side = side
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "asset_symbol": asset,
        "candidate_status": "live_quality",
        "bucket_key": f"{asset}|{side.value}|0.50-0.75|tight|5_10m",
        "empirical_bucket_gate": {"status": "allowed", "allowed": True},
    }
    return signal


def test_risk_engine_allows_one_btc_same_side_add_on_under_cap() -> None:
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
            market_ticker="KXBTC15M-ADDON",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5800"),
            count_fp=Decimal("1.00"),
        ),
        signal=_crypto_add_on_signal(),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("1000.00"),
            current_position_count_fp=Decimal("1.00"),
            current_position_side="yes",
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert "crypto_position_add_on_allowed" in verdict.reason_codes


def test_risk_engine_downsizes_live_crypto_same_side_add_on_to_position_pct_cap() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=False,
        crypto_position_add_on_assets="live",
        risk_max_order_notional_dollars=None,
        risk_max_position_notional_dollars=None,
        risk_position_pct=0.10,
    )
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXETH15M-ADDON",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("10.00"),
        ),
        signal=_crypto_add_on_signal(asset="ETH", side=ContractSide.YES),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("100.00"),
            current_position_count_fp=Decimal("18.00"),
            current_position_notional_dollars=Decimal("9.0000"),
            current_position_side="yes",
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("2.00")
    assert verdict.approved_notional_dollars == Decimal("1.0000")
    assert "crypto_position_add_on_allowed" in verdict.reason_codes
    assert "position_notional_cap_resized" in verdict.reason_codes
    assert verdict.diagnostics["position_notional_cap"]["projected_position_notional_dollars"] == "10.0000"


def test_risk_engine_blocks_second_opposite_non_btc_and_oversized_crypto_add_ons() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_allow_position_add_ons=False,
        crypto_position_add_on_assets="BTC",
        crypto_position_add_on_max_position_count_fp=2.0,
        crypto_position_add_on_max_ticket_count_fp=1.0,
    )
    engine = DeterministicRiskEngine(settings)

    def evaluate(*, count: str, current: str, side: ContractSide, current_side: str, signal_asset: str = "BTC"):
        return engine.evaluate(
            room=make_room(),
            control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
            ticket=TradeTicket(
                market_ticker=f"KX{signal_asset}15M-ADDON",
                action=TradeAction.BUY,
                side=side,
                yes_price_dollars=Decimal("0.5800") if side == ContractSide.YES else Decimal("0.4200"),
                count_fp=Decimal(count),
            ),
            signal=_crypto_add_on_signal(asset=signal_asset, side=side),
            context=RiskContext(
                market_observed_at=datetime.now(UTC),
                research_observed_at=datetime.now(UTC),
                total_capital_dollars=Decimal("1000.00"),
                current_position_count_fp=Decimal(current),
                current_position_side=current_side,
                strategy_code=StrategyCode.CRYPTO_15M.value,
            ),
        )

    second = evaluate(count="1.00", current="2.00", side=ContractSide.YES, current_side="yes")
    opposite = evaluate(count="1.00", current="1.00", side=ContractSide.NO, current_side="yes")
    non_btc = evaluate(count="1.00", current="1.00", side=ContractSide.YES, current_side="yes", signal_asset="ETH")
    oversized = evaluate(count="2.00", current="0.50", side=ContractSide.YES, current_side="yes")

    assert second.status == RiskStatus.BLOCKED
    assert second.diagnostics["crypto_position_add_on"]["reason"] == "projected_position_above_crypto_add_on_cap"
    assert opposite.status == RiskStatus.BLOCKED
    assert any("opposite-side" in reason for reason in opposite.reasons)
    assert non_btc.status == RiskStatus.BLOCKED
    assert non_btc.diagnostics["crypto_position_add_on"]["reason"] == "crypto_position_add_on_asset_not_configured"
    assert oversized.status == RiskStatus.BLOCKED
    assert oversized.diagnostics["crypto_position_add_on"]["reason"] == "ticket_count_above_crypto_add_on_cap"


def test_risk_engine_blocks_high_cost_side_with_tiny_remaining_payout() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    signal = make_signal(edge_bps=2300)
    signal.recommended_side = ContractSide.NO
    signal.target_yes_price_dollars = Decimal("0.1900")
    signal.fair_yes_dollars = Decimal("0.0000")
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.1900"),
            count_fp=Decimal("2.31"),
        ),
        signal=signal,
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
        thresholds=RuntimeThresholds(
            risk_min_edge_bps=50,
            risk_max_order_notional_dollars=100,
            risk_max_position_notional_dollars=300,
            trigger_max_spread_bps=1200,
            trigger_cooldown_seconds=300,
            strategy_quality_edge_buffer_bps=25,
            strategy_min_remaining_payout_bps=2500,
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "insufficient_remaining_payout" in verdict.reason_codes
    assert any("Remaining payout 0.1900" in reason for reason in verdict.reasons)
    assert verdict.diagnostics["remaining_payout"]["minimum_remaining_payout_bps"] == 2500


def test_risk_engine_honors_remaining_payout_policy_variant_floor() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_fee_aware_edge_enabled=False,
        remaining_payout_relaxation_live_enabled=True,
    )
    signal = make_signal(edge_bps=1900)
    signal.fair_yes_dollars = Decimal("0.9900")
    signal.candidate_trace = {
        "policy_variant_applied": REMAINING_PAYOUT_RELAXATION,
        "policy_variants": {REMAINING_PAYOUT_RELAXATION: {"applied": True}},
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.8000"),
            count_fp=Decimal("2.00"),
        ),
        signal=signal,
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert "remaining_payout_policy_variant_applied" in verdict.reason_codes
    assert verdict.diagnostics["remaining_payout"]["minimum_remaining_payout_bps"] == 1000


def test_risk_engine_caps_crypto_late_empirical_override_ticket_to_one_contract() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_position_pct=0.10,
    )
    signal = make_signal(edge_bps=5000)
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "candidate_status": "live_quality",
        "late_high_confidence_directional_entry": True,
        "empirical_bucket_gate": {
            "status": "override_allowed",
            "allowed": True,
            "override_allowed": True,
            "override_reason": "late_high_confidence_empirical_bucket_missing_override",
            "original_reason": "empirical_bucket_missing",
        },
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXBTC15M-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("10.00"),
        ),
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("1000.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("1.00")
    assert "crypto_late_empirical_override_count_cap" in verdict.reason_codes
    assert verdict.diagnostics["crypto_empirical_late_override"]["original_count_fp"] == "10.0000"
    assert verdict.diagnostics["crypto_empirical_late_override"]["approved_count_fp"] == "1.0000"


def test_risk_engine_late_high_confidence_uses_existing_position_caps() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_credible_edge_bps=10000,
        risk_min_probability_extremity_pct=0.0,
        risk_max_order_notional_dollars=100,
        risk_max_position_notional_dollars=300,
        risk_position_pct=0.10,
        strategy_min_remaining_payout_bps=0,
    )
    signal = make_signal(edge_bps=300)
    signal.recommended_side = ContractSide.NO
    signal.candidate_trace = {
        "strategy_code": StrategyCode.CRYPTO_15M.value,
        "candidate_status": "live_quality",
        "late_high_confidence_directional_entry": True,
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="KXETH15M-LATE-POSITION-BUDGET",
            action=TradeAction.BUY,
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.1300"),
            count_fp=Decimal("10.00"),
        ),
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            total_capital_dollars=Decimal("1000.00"),
            strategy_code=StrategyCode.CRYPTO_15M.value,
        ),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert verdict.approved_count_fp == Decimal("10.00")
    assert "crypto_late_high_confidence_max_loss_cap" not in verdict.reason_codes
    assert "crypto_late_high_confidence_max_loss" not in verdict.diagnostics


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
        signal=make_signal(capital_bucket="risky", trade_regime="standard"),
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


def test_risk_engine_honors_low_price_policy_variant_floor() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_contract_price_dollars=0.25,
        risk_min_probability_extremity_pct=0.0,
        risk_fee_aware_edge_enabled=False,
        low_price_high_edge_live_enabled=True,
    )
    signal = make_signal(edge_bps=2400)
    signal.target_yes_price_dollars = Decimal("0.0600")
    signal.candidate_trace = {
        "policy_variant_applied": LOW_PRICE_HIGH_EDGE,
        "policy_variants": {LOW_PRICE_HIGH_EDGE: {"applied": True}},
    }
    engine = DeterministicRiskEngine(settings)

    verdict = engine.evaluate(
        room=make_room(),
        control=DeploymentControl(id="default", active_color="blue", kill_switch_enabled=False, notes={}),
        ticket=TradeTicket(
            market_ticker="WX-TEST",
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.0600"),
            count_fp=Decimal("10.00"),
        ),
        signal=signal,
        context=RiskContext(market_observed_at=datetime.now(UTC), research_observed_at=datetime.now(UTC)),
    )

    assert verdict.status == RiskStatus.APPROVED
    assert "low_price_policy_variant_applied" in verdict.reason_codes
    assert verdict.diagnostics["minimum_contract_price"]["effective_min_price_dollars"] == "0.0500"


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
    assert "max_credible_edge" in verdict.reason_codes
    diagnostics = verdict.diagnostics["max_credible_edge"]
    assert diagnostics["selected_side"] == "yes"
    assert diagnostics["fair_yes_dollars"] == "0.6400"
    assert diagnostics["side_fair_dollars"] == "0.6400"
    assert diagnostics["traded_price_dollars"] == "0.0200"
    assert diagnostics["forecast_high_f"] == 86
    assert diagnostics["forecast_delta_f"] == 6.0


def test_risk_engine_allows_validated_extreme_edge_without_lifting_other_gates() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_max_credible_edge_bps=5000,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = DeterministicRiskEngine(settings)
    signal = make_signal(edge_bps=7382)
    signal.candidate_trace = {
        "validated_extreme_edge": True,
        "extreme_edge_diagnostic": {"passed": True, "reason_codes": []},
    }

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
        signal=signal,
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "max_credible_edge" not in verdict.reason_codes
    assert "max_credible_edge_validated" in verdict.reason_codes
    assert any("nearly impossible" in reason for reason in verdict.reasons)


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


def test_non_standard_regime_does_not_emit_zero_risky_bucket_noise() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_risky_capital_max_ratio=0.0,
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
        signal=make_signal(edge_bps=6000, capital_bucket="risky", trade_regime="near_threshold"),
        context=RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            portfolio_bucket_snapshot=PortfolioBucketSnapshot(
                total_capital_dollars=Decimal("10.00"),
                overall_remaining_dollars=Decimal("10.00"),
                risky_limit_dollars=Decimal("0.00"),
                risky_remaining_dollars=Decimal("0.00"),
                risky_capital_max_ratio=0.0,
            ),
        ),
    )

    assert verdict.status == RiskStatus.BLOCKED
    assert "non_standard_regime" in verdict.reason_codes
    assert not any("Risky capital bucket is full" in reason for reason in verdict.reasons)
