from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, DeploymentColor, RiskStatus, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.weather.scoring import WeatherSignalSnapshot


def make_signal(edge_bps: int = 100) -> StrategySignal:
    weather = WeatherSignalSnapshot(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        forecast_high_f=86,
        current_temp_f=78,
        resolution_state=WeatherResolutionState.UNRESOLVED,
        observation_time=datetime.now(UTC),
        forecast_updated_time=datetime.now(UTC),
        summary="test signal",
    )
    return StrategySignal(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        edge_bps=edge_bps,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5800"),
        summary="test",
        weather=weather,
    )


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
