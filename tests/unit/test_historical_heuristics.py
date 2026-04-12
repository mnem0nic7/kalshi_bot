from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason, StrategyMode, WeatherResolutionState
from kalshi_bot.core.schemas import (
    HeuristicPolicyAction,
    HeuristicPolicyCondition,
    HeuristicPolicyNode,
    HistoricalHeuristicPack,
)
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.weather.models import WeatherMarketMapping


def _mapping() -> WeatherMarketMapping:
    return WeatherMarketMapping(
        market_ticker="KXHIGHCHI-26APR11-T81",
        market_type="weather",
        series_ticker="KXHIGHCHI",
        station_id="KMDW",
        location_name="Chicago",
        latitude=41.7868,
        longitude=-87.7522,
        threshold_f=81,
        settlement_source="NWS station observation",
    )


def test_compile_pack_clamps_invalid_actions() -> None:
    service = HistoricalHeuristicService(Settings(database_url="sqlite+aiosqlite:///./test.db"))
    pack = HistoricalHeuristicPack(
        version="heuristic-test",
        thresholds={
            "risk_min_edge_bps": 9999,
            "trigger_max_spread_bps": 1,
            "strategy_quality_edge_buffer_bps": 9999,
            "strategy_min_remaining_payout_bps": 1,
        },
        policy_graph=[
            HeuristicPolicyNode(
                rule_id="bad-rule",
                description="bad inputs",
                condition=HeuristicPolicyCondition(series_tickers=["KXHIGHCHI"]),
                action=HeuristicPolicyAction(
                    fair_yes_adjust_bps=9999,
                    risk_min_edge_bps=9999,
                    trigger_max_spread_bps=1,
                    strategy_quality_edge_buffer_bps=9999,
                    strategy_min_remaining_payout_bps=1,
                    recommended_strategy_mode=StrategyMode.RESOLVED_CLEANUP_CANDIDATE,
                    force_stand_down_reason=StandDownReason.RESEARCH_STALE,
                ),
            )
        ],
    )

    compiled = service.compile_pack(pack)

    assert compiled.thresholds.risk_min_edge_bps == 500
    assert compiled.thresholds.trigger_max_spread_bps == 50
    assert compiled.thresholds.strategy_quality_edge_buffer_bps == 500
    assert compiled.thresholds.strategy_min_remaining_payout_bps == 50
    assert compiled.policy_graph[0].action.recommended_strategy_mode == StrategyMode.LATE_DAY_AVOID
    assert compiled.policy_graph[0].action.force_stand_down_reason == StandDownReason.NO_ACTIONABLE_EDGE


def test_apply_to_signal_matches_rule_and_adjusts_thresholds() -> None:
    service = HistoricalHeuristicService(Settings(database_url="sqlite+aiosqlite:///./test.db"))
    pack = HistoricalHeuristicPack(
        version="heuristic-live",
        thresholds={
            "risk_min_edge_bps": 40,
            "trigger_max_spread_bps": 900,
            "strategy_quality_edge_buffer_bps": 80,
            "strategy_min_remaining_payout_bps": 700,
        },
        calibration_entries=[],
        policy_graph=[
            HeuristicPolicyNode(
                rule_id="broken-book",
                description="stand down on broken books",
                priority=10,
                support_count=12,
                condition=HeuristicPolicyCondition(
                    series_tickers=["KXHIGHCHI"],
                    spread_regimes=["broken"],
                ),
                action=HeuristicPolicyAction(
                    recommended_strategy_mode=StrategyMode.LATE_DAY_AVOID,
                    force_stand_down_reason=StandDownReason.BOOK_EFFECTIVELY_BROKEN,
                ),
            )
        ],
        agent_summary="Stay conservative when books are broken.",
        metadata={"intelligence_run_id": "run-1", "support_window": {"window_days": 30}},
    )
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.8,
        edge_bps=0,
        recommended_action=None,
        recommended_side=None,
        target_yes_price_dollars=None,
        summary="summary",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
    )
    application = service.apply_to_signal(
        pack=pack,
        mapping=_mapping(),
        signal=signal,
        market_snapshot={"market": {"yes_bid_dollars": "0.0100", "yes_ask_dollars": "1.0000", "no_ask_dollars": "0.9600"}},
        reference_time=datetime.now(UTC),
        base_thresholds=RuntimeThresholds(
            risk_min_edge_bps=30,
            risk_max_order_notional_dollars=50.0,
            risk_max_position_notional_dollars=200.0,
            trigger_max_spread_bps=1200,
            trigger_cooldown_seconds=300,
            strategy_quality_edge_buffer_bps=50,
            strategy_min_remaining_payout_bps=400,
        ),
    )

    assert application["heuristic_pack_version"] == "heuristic-live"
    assert application["recommended_strategy_mode"] == StrategyMode.LATE_DAY_AVOID.value
    assert application["force_stand_down_reason"] == StandDownReason.BOOK_EFFECTIVELY_BROKEN.value
    assert application["thresholds"]["risk_min_edge_bps"] == 40
    assert application["rule_trace"][0]["rule_id"] == "broken-book"
