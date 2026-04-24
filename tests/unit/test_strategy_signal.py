from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import ResearchFreshness
from kalshi_bot.services.signal import (
    StrategySignal,
    _trade_recommendation_with_trace,
    apply_heuristic_application_to_signal,
    annotate_signal_quality,
    base_strategy_summary,
    evaluate_trade_eligibility,
)
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
            forecast_delta_f=4.0,
            confidence_band="high" if resolution_state != WeatherResolutionState.UNRESOLVED else "medium",
            trade_regime="standard",
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


def test_trade_recommendation_selects_no_when_yes_is_below_price_floor() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_contract_price_dollars=0.25)

    action, side, target_yes, edge_bps, trace = _trade_recommendation_with_trace(
        fair_yes_dollars=Decimal("0.3500"),
        market_snapshot={"market": {"yes_bid_dollars": "0.0050", "yes_ask_dollars": "0.0100", "no_ask_dollars": "0.4500"}},
        min_edge_bps=50,
        settings=settings,
    )

    assert action == TradeAction.BUY
    assert side == ContractSide.NO
    assert target_yes == Decimal("0.5500")
    assert edge_bps == 2000
    assert trace["yes"]["reason"] == "below_min_contract_price"
    assert trace["no"]["status"] == "selected"
    assert trace["outcome"] == "candidate_selected"


def test_trade_recommendation_ranks_best_eligible_side() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_contract_price_dollars=0.25)

    _action, side, target_yes, edge_bps, trace = _trade_recommendation_with_trace(
        fair_yes_dollars=Decimal("0.5500"),
        market_snapshot={"market": {"yes_bid_dollars": "0.4400", "yes_ask_dollars": "0.4600", "no_ask_dollars": "0.3000"}},
        min_edge_bps=50,
        settings=settings,
    )

    assert side == ContractSide.NO
    assert target_yes == Decimal("0.7000")
    assert edge_bps == 1500
    assert trace["yes"]["status"] == "eligible"
    assert trace["no"]["reason"] == "selected_best_quality_adjusted_edge"


def test_trade_recommendation_filters_both_sides_below_price_floor() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_contract_price_dollars=0.25)

    action, side, target_yes, edge_bps, trace = _trade_recommendation_with_trace(
        fair_yes_dollars=Decimal("0.5000"),
        market_snapshot={"market": {"yes_bid_dollars": "0.0050", "yes_ask_dollars": "0.0100", "no_ask_dollars": "0.0100"}},
        min_edge_bps=50,
        settings=settings,
    )

    assert action is None
    assert side is None
    assert target_yes is None
    assert edge_bps == 4950
    assert trace["outcome"] == "pre_risk_filtered"
    assert trace["yes"]["reason"] == "below_min_contract_price"
    assert trace["no"]["reason"] == "below_min_contract_price"


def test_trade_recommendation_checks_no_when_yes_fails_remaining_payout() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_contract_price_dollars=0.25)

    _action, side, target_yes, edge_bps, trace = _trade_recommendation_with_trace(
        fair_yes_dollars=Decimal("0.5000"),
        market_snapshot={"market": {"yes_bid_dollars": "0.3300", "yes_ask_dollars": "0.3500", "no_ask_dollars": "0.2500"}},
        min_edge_bps=50,
        settings=settings,
        minimum_remaining_payout_bps=7000,
    )

    assert side == ContractSide.NO
    assert target_yes == Decimal("0.7500")
    assert edge_bps == 2500
    assert trace["yes"]["reason"] == "insufficient_remaining_payout"
    assert trace["no"]["status"] == "selected"


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


def test_trade_eligibility_labels_broken_book_when_no_trade_quotes_are_extreme() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6391"),
        confidence=0.75,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="broken book",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )
    market_snapshot = {"market": {"yes_bid_dollars": "0.0400", "yes_ask_dollars": "1.0000", "no_ask_dollars": "0.9600"}}

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.stand_down_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN


def test_trade_eligibility_labels_wide_spread_when_no_trade_book_is_bad_but_not_broken() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.5200"),
        confidence=0.75,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="wide spread no trade",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )
    market_snapshot = {"market": {"yes_bid_dollars": "0.2000", "yes_ask_dollars": "0.4500", "no_ask_dollars": "0.8000"}}

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


def test_trade_eligibility_labels_no_actionable_edge_when_book_is_normal_but_edge_isnt() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.5100"),
        confidence=0.6,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="no edge",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )
    market_snapshot = {"market": {"yes_bid_dollars": "0.4900", "yes_ask_dollars": "0.5200", "no_ask_dollars": "0.4900"}}

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.stand_down_reason == StandDownReason.NO_ACTIONABLE_EDGE
    assert verdict.evaluation_outcome == "no_candidate"


def test_base_strategy_summary_strips_old_trade_suffixes() -> None:
    summary = (
        "Forecast high 86.0F versus threshold 84.0F implies fair yes near 0.6391 with confidence 0.77. "
        "No taker trade clears the configured edge threshold. Stand down: Something else."
    )

    assert base_strategy_summary(summary) == (
        "Forecast high 86.0F versus threshold 84.0F implies fair yes near 0.6391 with confidence 0.77"
    )

    broken_book_summary = (
        "Forecast high 86.0F versus threshold 84.0F implies fair yes near 0.6391 with confidence 0.77. "
        "Order book is effectively broken at current quotes (yes bid 0.0400, yes ask 1.0000, no ask 0.9600)."
    )
    assert base_strategy_summary(broken_book_summary) == (
        "Forecast high 86.0F versus threshold 84.0F implies fair yes near 0.6391 with confidence 0.77"
    )


def test_apply_heuristic_application_adjusts_signal_and_summary() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.5200"),
        confidence=0.7,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="Forecast high 81F versus threshold 80F implies fair yes near 0.5200.",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
        heuristic_application={
            "fair_yes_adjust_bps": 300,
            "adjusted_fair_yes_dollars": "0.5500",
            "thresholds": {"risk_min_edge_bps": 50, "trigger_max_spread_bps": 1200},
            "heuristic_pack_version": "heuristic-1",
        },
    )
    market_snapshot = {"market": {"yes_bid_dollars": "0.5200", "yes_ask_dollars": "0.5300", "no_ask_dollars": "0.4700"}}

    adjusted = apply_heuristic_application_to_signal(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        min_edge_bps=50,
        spread_limit_bps=1200,
    )

    assert adjusted.fair_yes_dollars == Decimal("0.5500")
    assert adjusted.recommended_side == ContractSide.YES
    assert adjusted.recommended_action == TradeAction.BUY
    assert adjusted.target_yes_price_dollars == Decimal("0.5300")
    assert adjusted.evaluation_outcome == "candidate_selected"
    assert adjusted.candidate_trace["yes"]["status"] == "selected"
    assert "Historical heuristics adjusted fair yes by +300bps" in adjusted.summary


def test_trade_eligibility_respects_heuristic_forced_stand_down() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.5500", "yes_ask_dollars": "0.5800", "no_ask_dollars": "0.4200"}}
    signal = _signal()
    signal.heuristic_application = {
        "recommended_strategy_mode": StrategyMode.LATE_DAY_AVOID.value,
        "force_stand_down_reason": StandDownReason.NO_ACTIONABLE_EDGE.value,
    }

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert verdict.eligible is False
    assert verdict.strategy_mode == StrategyMode.LATE_DAY_AVOID
    assert verdict.stand_down_reason == StandDownReason.NO_ACTIONABLE_EDGE


def test_trade_eligibility_uses_decision_time_for_historical_checks() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    market_snapshot = {"market": {"yes_bid_dollars": "0.5500", "yes_ask_dollars": "0.5800", "no_ask_dollars": "0.4200"}}
    observed_at = datetime(2026, 4, 10, 18, 0, tzinfo=UTC)

    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=_signal(),
        market_snapshot=market_snapshot,
        market_observed_at=observed_at,
        research_freshness=ResearchFreshness(
            refreshed_at=observed_at - timedelta(seconds=30),
            expires_at=observed_at + timedelta(minutes=5),
            stale=False,
            max_source_age_seconds=30,
        ),
        thresholds=_thresholds(),
        decision_time=observed_at + timedelta(seconds=30),
    )

    assert verdict.market_stale is False
    assert verdict.research_stale is False


def test_annotate_signal_quality_assigns_safe_bucket_for_standard_trade() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        edge_bps=90,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5800"),
        summary="Standard setup.",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
        forecast_delta_f=4.0,
    )

    annotated = annotate_signal_quality(
        settings=settings,
        signal=signal,
        market_snapshot={"market": {"yes_bid_dollars": "0.5600", "yes_ask_dollars": "0.5800", "no_ask_dollars": "0.4200"}},
    )

    assert annotated.trade_regime == "standard"
    assert annotated.capital_bucket == "safe"


def test_annotate_signal_quality_warns_on_near_threshold_low_confidence_and_oversize() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6200"),
        confidence=0.55,
        edge_bps=90,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5800"),
        summary="Forecast high 81F versus threshold 80F implies fair yes near 0.6200.",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
        forecast_delta_f=1.0,
    )

    annotated = annotate_signal_quality(
        settings=settings,
        signal=signal,
        market_snapshot={"market": {"yes_bid_dollars": "0.5600", "yes_ask_dollars": "0.5800", "no_ask_dollars": "0.4200"}},
        max_order_notional_dollars=20.0,
    )

    assert annotated.trade_regime == "near_threshold"
    assert annotated.capital_bucket == "risky"
    assert annotated.model_quality_status == "warn"
    assert annotated.recommended_size_cap_fp == Decimal("10.00")
    assert annotated.warn_only_blocked is False
    assert any("low confidence" in reason.lower() for reason in annotated.model_quality_reasons)
    assert any("exceeds advisory cap" in reason.lower() for reason in annotated.model_quality_reasons)
    assert "Model-quality review:" in annotated.summary


def test_annotate_signal_quality_warns_on_longshot_yes_setup() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.0400"),
        confidence=0.62,
        edge_bps=30,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.0100"),
        summary="Cheap tail setup.",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
        forecast_delta_f=6.0,
    )

    annotated = annotate_signal_quality(
        settings=settings,
        signal=signal,
        market_snapshot={"market": {"yes_bid_dollars": "0.0000", "yes_ask_dollars": "0.0100", "no_ask_dollars": "0.9900"}},
    )

    assert annotated.trade_regime == "longshot_yes"
    assert annotated.capital_bucket == "risky"
    assert annotated.model_quality_status == "warn"
    assert annotated.recommended_size_cap_fp == Decimal("10.00")
    assert any("longshot" in reason.lower() for reason in annotated.model_quality_reasons)


def test_annotate_signal_quality_marks_broken_book_as_warn_only_block() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6300"),
        confidence=0.75,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="Broken book setup.",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
        forecast_delta_f=4.0,
    )

    annotated = annotate_signal_quality(
        settings=settings,
        signal=signal,
        market_snapshot={"market": {"yes_bid_dollars": "0.0400", "yes_ask_dollars": "1.0000", "no_ask_dollars": "0.9600"}},
    )
    verdict = evaluate_trade_eligibility(
        settings=settings,
        signal=annotated,
        market_snapshot={"market": {"yes_bid_dollars": "0.0400", "yes_ask_dollars": "1.0000", "no_ask_dollars": "0.9600"}},
        market_observed_at=datetime.now(UTC),
        research_freshness=_freshness(stale=False),
        thresholds=_thresholds(),
    )

    assert annotated.warn_only_blocked is True
    assert annotated.model_quality_status == "warn"
    assert verdict.stand_down_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN
