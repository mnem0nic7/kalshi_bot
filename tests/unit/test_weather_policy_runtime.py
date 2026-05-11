from __future__ import annotations

from decimal import Decimal

from kalshi_bot.core.enums import ContractSide, StandDownReason, TradeAction
from kalshi_bot.core.schemas import TradeEligibilityVerdict
from kalshi_bot.orchestration.supervisor import _apply_weather_policy_mode, _policy_side_for_signal
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.services.weather_policy import ResolvedWeatherPolicy


def _signal() -> StrategySignal:
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.7500"),
        confidence=0.91,
        edge_bps=3200,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.4000"),
        summary="test",
    )
    signal.eligibility = TradeEligibilityVerdict(
        eligible=True,
        evaluation_outcome="candidate_selected",
        candidate_trace={},
        reasons=["Trade passed marketability checks."],
    )
    return signal


def _resolution(mode: str) -> ResolvedWeatherPolicy:
    thresholds = RuntimeThresholds(
        risk_min_edge_bps=500,
        risk_max_order_notional_dollars=50.0,
        risk_max_position_notional_dollars=100.0,
        trigger_max_spread_bps=250,
        trigger_cooldown_seconds=60,
        strategy_quality_edge_buffer_bps=25,
        strategy_min_remaining_payout_bps=2000,
    )
    return ResolvedWeatherPolicy(
        policy_key="weather/a/global/any/yes/all_season/all_month/any/any/entry_gate",
        fallback_policy_key_used="weather/a/global/any/any/all_season/all_month/any/any/entry_gate",
        mode=mode,
        action="pause",
        lane="entry_gate",
        thresholds=thresholds,
        policy=None,
        candidate_keys=(),
        binding_policy_lane="entry_gate",
        policy_disagreement=False,
        reason_codes=("unit_policy_mode",),
        deterministic_summary="unit",
    )


def test_paused_weather_policy_blocks_candidate_before_empirical_bootstrap_can_revive_it() -> None:
    signal = _apply_weather_policy_mode(_signal(), _resolution("paused"))

    assert signal.eligibility is not None
    assert signal.eligibility.eligible is False
    assert signal.eligibility.stand_down_reason == StandDownReason.WEATHER_POLICY_PAUSED
    assert signal.eligibility.candidate_trace["weather_policy_mode_block"]["mode"] == "paused"


def test_policy_side_uses_best_candidate_when_recommended_side_is_not_selected() -> None:
    signal = _signal()
    signal.recommended_side = None
    signal.candidate_trace = {
        "candidates": [
            {"side": "yes", "quality_adjusted_edge_bps": 400, "edge_bps": 500},
            {"side": "no", "quality_adjusted_edge_bps": 1800, "edge_bps": 1900},
        ]
    }

    assert _policy_side_for_signal(signal) == "no"
