from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import ResearchFreshness
from kalshi_bot.services.signal import StrategySignal, evaluate_trade_eligibility
from kalshi_bot.weather.scoring import WeatherSignalSnapshot


def _signal(*, resolution_state: WeatherResolutionState = WeatherResolutionState.UNRESOLVED) -> StrategySignal:
    return StrategySignal(
        fair_yes_dollars=Decimal("0.0003") if resolution_state == WeatherResolutionState.LOCKED_NO else Decimal("0.6400"),
        confidence=1.0 if resolution_state != WeatherResolutionState.UNRESOLVED else 0.9,
        edge_bps=97,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.NO if resolution_state == WeatherResolutionState.LOCKED_NO else ContractSide.YES,
        target_yes_price_dollars=Decimal("0.0100") if resolution_state == WeatherResolutionState.LOCKED_NO else Decimal("0.5800"),
        summary="strategy signal",
        weather=WeatherSignalSnapshot(
            fair_yes_dollars=Decimal("0.0000") if resolution_state == WeatherResolutionState.LOCKED_NO else Decimal("0.6400"),
            confidence=1.0 if resolution_state != WeatherResolutionState.UNRESOLVED else 0.9,
            forecast_high_f=80,
            current_temp_f=51.8,
            resolution_state=resolution_state,
            observation_time=datetime.now(UTC),
            forecast_updated_time=datetime.now(UTC),
            summary="weather summary",
        ),
        resolution_state=resolution_state,
        strategy_mode=(
            StrategyMode.RESOLVED_CLEANUP_CANDIDATE
            if resolution_state != WeatherResolutionState.UNRESOLVED
            else StrategyMode.DIRECTIONAL_UNRESOLVED
        ),
    )


def _freshness(*, stale: bool) -> ResearchFreshness:
    now = datetime.now(UTC)
    return ResearchFreshness(
        refreshed_at=now - timedelta(seconds=30),
        expires_at=now - timedelta(seconds=1) if stale else now + timedelta(minutes=5),
        stale=stale,
        max_source_age_seconds=30,
    )


def _thresholds() -> SimpleNamespace:
    return SimpleNamespace(
        risk_min_edge_bps=50,
        trigger_max_spread_bps=1200,
    )


def test_trade_eligibility_prioritizes_stale_research_then_market() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.5500", "yes_ask_dollars": "0.5800", "no_ask_dollars": "0.4200"}}

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=_signal(),
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC) - timedelta(minutes=10),
        research_freshness=_freshness(stale=True),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.stand_down_reason == StandDownReason.RESEARCH_STALE


def test_trade_eligibility_blocks_resolved_contract_before_ticketing() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.0100", "yes_ask_dollars": "0.4600", "no_ask_dollars": "0.9900"}}

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=_signal(resolution_state=WeatherResolutionState.LOCKED_NO),
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.resolution_state == WeatherResolutionState.LOCKED_NO
    assert verdict.stand_down_reason == StandDownReason.RESOLVED_CONTRACT


def test_trade_eligibility_blocks_tiny_remaining_payout() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.9600", "yes_ask_dollars": "0.9700", "no_ask_dollars": "0.0300"}}
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.9900"),
        confidence=0.8,
        edge_bps=75,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.9700"),
        summary="tiny payout",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.stand_down_reason == StandDownReason.INSUFFICIENT_REMAINING_PAYOUT


def test_trade_eligibility_blocks_wide_spread() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.0100", "yes_ask_dollars": "0.4600", "no_ask_dollars": "0.5400"}}
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.7000"),
        confidence=0.7,
        edge_bps=150,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.4600"),
        summary="wide spread",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.stand_down_reason == StandDownReason.SPREAD_TOO_WIDE
