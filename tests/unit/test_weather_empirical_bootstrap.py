from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.core.schemas import AgentPackWeatherBootstrapPolicy
from kalshi_bot.db.models import WeatherBootstrapEventRecord, WeatherBootstrapHistoricalEvidenceRecord
from kalshi_bot.services.trade_behavior import coarse_empirical_bucket_key_from_legacy
from kalshi_bot.services.weather_empirical_bootstrap import (
    WeatherEmpiricalBootstrapContext,
    WeatherEmpiricalBootstrapService,
    confidence_source_from_trace,
    fair_value_source_from_provenance,
    stale_signal_evidence_from_trace,
)


NOW = datetime(2026, 5, 11, 16, 0, tzinfo=UTC)


def _context(**overrides):
    payload = {
        "kalshi_env": "production",
        "market_ticker": "KXHIGHTSFO-26MAY11-T70",
        "side": "no",
        "confidence": 0.91,
        "edge_bps_after_buffer": 3200,
        "fair_yes_dollars": Decimal("0.0450"),
        "fair_value_source": "intraday_model",
        "confidence_source": "calibrated",
        "bucket_key": "KXHIGHTSFO|SFO|no|A|50-59c|delta:5|conf:high|spread:100-249bps",
        "actual_sample_count": 0,
        "policy_key": "weather/weather_baseline/global/any/any/all_season/all_month/any/any/entry_gate",
    }
    payload.update(overrides)
    return WeatherEmpiricalBootstrapContext(**payload)


def _event(**overrides):
    payload = {
        "kalshi_env": "production",
        "market_ticker": "KXHIGHTSFO-26MAY11-T70",
        "series_ticker": "KXHIGHTSFO",
        "local_market_day": "26MAY11",
        "bucket_key": "bucket",
        "policy_key": "policy-a",
        "tier": "cold",
        "event_type": "decision",
        "status": "shadow_allow",
        "occurred_at": NOW,
        "payload": {"matched": True, "fair_value_source": "intraday_model", "data_stale": False},
    }
    payload.update(overrides)
    return WeatherBootstrapEventRecord(**payload)


def _historical(**overrides):
    payload = {
        "kalshi_env": "production",
        "market_ticker": "KXHIGHTSFO-26MAY11-T70",
        "series_ticker": "KXHIGHTSFO",
        "local_market_day": "26MAY11",
        "bucket_key": "KXHIGHTSFO|SFO|no|A|50-59c|delta:5|conf:high|spread:100-249bps",
        "policy_key": "policy-a",
        "tier": "cold",
        "replay_version": "strict",
        "source_fingerprint": "fingerprint",
        "strict_replay": True,
        "side": "no",
        "confidence": 0.91,
        "edge_bps": 3200,
        "count_fp": Decimal("1.00"),
        "pnl_dollars": Decimal("1.0000"),
        "evidence_weight": 1.0,
        "outcome": "win",
        "observed_at": NOW,
        "payload": {"fair_value_source": "intraday_model"},
    }
    payload.update(overrides)
    return WeatherBootstrapHistoricalEvidenceRecord(**payload)


def test_cold_tier_matches_shadow_without_live_application() -> None:
    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(),
        policy=AgentPackWeatherBootstrapPolicy(),
        now=NOW,
    )

    assert decision.matched is True
    assert decision.would_have_entered is True
    assert decision.allowed_live is False
    assert decision.outcome == "shadow_allow"
    assert decision.tier == "cold"
    assert decision.to_trace()["size_factor"] == 0.10


def test_coarse_bucket_key_maps_old_spread_and_price_bands_together() -> None:
    first = coarse_empirical_bucket_key_from_legacy(
        "KXHIGHTSFO|SFO|no|A|50-59c|delta:5|conf:high|spread:000-099bps"
    )
    second = coarse_empirical_bucket_key_from_legacy(
        "KXHIGHTSFO|SFO|no|A|40-49c|delta:5|conf:high|spread:500bps+"
    )

    assert first == second
    assert first == "KXHIGHTSFO|SFO|no|A|delta:5|quality:high"


def test_raw_confidence_gets_cold_only_penalty() -> None:
    service = WeatherEmpiricalBootstrapService()
    policy = AgentPackWeatherBootstrapPolicy()

    blocked = service.evaluate(
        context=_context(confidence_source="raw", confidence=0.94, edge_bps_after_buffer=3900),
        policy=policy,
        now=NOW,
    )
    allowed = service.evaluate(
        context=_context(confidence_source="raw", confidence=0.96, edge_bps_after_buffer=4100),
        policy=policy,
        now=NOW,
    )

    assert blocked.reason == "bootstrap_cold_confidence_below_threshold"
    assert allowed.matched is True
    assert allowed.min_confidence_required == 0.95
    assert allowed.min_edge_bps_required == 4000


def test_fallback_fair_value_is_disqualified() -> None:
    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(fair_value_source="fallback"),
        policy=AgentPackWeatherBootstrapPolicy(),
        now=NOW,
    )

    assert decision.matched is False
    assert decision.reason == "fair_value_source_disqualified"
    assert "fair_value_source_disqualified" in decision.reason_codes


def test_static_fresh_repetition_is_persistence_but_stale_repetition_blocks() -> None:
    assert stale_signal_evidence_from_trace({"static_signal_guard": {"pattern": "static_edge"}}) == {
        "stale": False,
        "reason_codes": [],
        "static_repetition_is_persistence": True,
    }

    stale = stale_signal_evidence_from_trace({"market_stale": True, "static_signal_guard": {"pattern": "static_edge"}})
    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(data_stale=stale["stale"], source_stale_reasons=tuple(stale["reason_codes"])),
        policy=AgentPackWeatherBootstrapPolicy(),
        now=NOW,
    )

    assert decision.reason == "bootstrap_stale_data_blocked"
    assert "market_stale" in decision.reason_codes


def test_live_enabled_cold_tier_applies_size_factor_and_caps() -> None:
    policy = AgentPackWeatherBootstrapPolicy(rollout_state="promoted_low")
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(),
        policy=policy,
        now=NOW,
    )

    assert decision.allowed_live is True
    assert decision.applied is True
    assert decision.size_factor == 0.10
    assert decision.daily_notional_cap_dollars == 100.0
    assert decision.max_concurrent_positions == 1


def test_close_strike_probe_unlocks_low_confidence_delta_two_with_tiny_cap() -> None:
    policy = AgentPackWeatherBootstrapPolicy(
        rollout_state="promoted_low",
        local_overrides={
            "close_strike_probe": {
                "enabled": True,
                "max_order_notional_dollars": 0.25,
                "tiers": [
                    {"name": "conf60_delta2_edge3500", "min_confidence": 0.60, "min_abs_delta_f": 2.0, "min_edge_bps": 3500}
                ],
            }
        },
    )
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(
            confidence=0.64,
            edge_bps_after_buffer=3600,
            forecast_delta_f=2.0,
            trade_regime="near_threshold",
            pre_empirical_stand_down_reason="insufficient_forecast_separation",
        ),
        policy=policy,
        now=NOW,
    )

    assert decision.allowed_live is True
    assert decision.max_order_notional_dollars == 0.25
    assert "close_strike_probe_unlocked" in decision.reason_codes
    assert decision.thresholds["close_strike_probe"]["tier"] == "conf60_delta2_edge3500"


def test_close_strike_probe_blocks_below_confidence_floor() -> None:
    policy = AgentPackWeatherBootstrapPolicy(
        rollout_state="promoted_low",
        local_overrides={"close_strike_probe": {"enabled": True}},
    )
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(
            confidence=0.59,
            edge_bps_after_buffer=8000,
            forecast_delta_f=5.0,
            pre_empirical_stand_down_reason="insufficient_forecast_separation",
        ),
        policy=policy,
        now=NOW,
    )

    assert decision.allowed_live is False
    assert decision.reason == "close_strike_probe_tier_not_matched"
    assert "close_strike_probe_evidence_failed" in decision.reason_codes


def test_close_strike_probe_respects_cooldown() -> None:
    policy = AgentPackWeatherBootstrapPolicy(
        rollout_state="promoted_low",
        local_overrides={
            "close_strike_probe": {
                "enabled": True,
                "max_order_notional_dollars": 0.25,
                "cooldown_seconds": 3600,
            }
        },
    )
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})
    recent_probe = _event(
        event_type="risk",
        status="approved",
        occurred_at=NOW - timedelta(minutes=30),
        payload={"reason_codes": ["close_strike_probe_unlocked"]},
    )

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(
            confidence=0.94,
            edge_bps_after_buffer=6000,
            forecast_delta_f=4.0,
            pre_empirical_stand_down_reason="insufficient_forecast_separation",
        ),
        policy=policy,
        recent_events=[recent_probe],
        now=NOW,
    )

    assert decision.allowed_live is False
    assert decision.reason == "close_strike_probe_cooldown_active"
    assert "probe_cooldown_active" in decision.reason_codes
    assert decision.thresholds["close_strike_probe"]["cooldown"]["active"] is True


def test_shadow_gate_counts_scoped_market_episodes_not_repeated_rows() -> None:
    policy = AgentPackWeatherBootstrapPolicy()
    events = [
        _event(market_ticker="KXHIGHTSFO-26MAY11-T70", occurred_at=NOW),
        _event(market_ticker="KXHIGHTSFO-26MAY11-T70", occurred_at=NOW.replace(minute=1)),
        _event(market_ticker="KXHIGHTSFO-26MAY12-T70", local_market_day="26MAY12", occurred_at=NOW),
        _event(market_ticker="KXHIGHTSFO-26MAY13-T70", local_market_day="26MAY13", occurred_at=NOW),
        _event(market_ticker="KXHIGHTSFO-26MAY14-T70", local_market_day="26MAY14", occurred_at=NOW),
        _event(market_ticker="KXHIGHTSFO-26MAY15-T70", local_market_day="26MAY15", occurred_at=NOW),
        _event(policy_key="other-policy", market_ticker="KXHIGHTLAX-26MAY11-T72", occurred_at=NOW),
    ]

    status = WeatherEmpiricalBootstrapService().shadow_gate_status(
        policy=policy,
        events=events,
        policy_key="policy-a",
        now=NOW.replace(day=12, hour=17),
    )

    assert status.eligible_for_promotion is True
    assert status.market_episodes == 5
    assert status.intended_matches == 5
    assert status.actual_matches == 5


def test_terminal_order_event_releases_bootstrap_caps() -> None:
    policy = AgentPackWeatherBootstrapPolicy(rollout_state="promoted_low")
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})
    events = [
        _event(
            event_type="risk",
            status="approved",
            room_id="room-1",
            notional_dollars=Decimal("100.0000"),
            occurred_at=NOW.replace(hour=10),
        ),
        _event(
            event_type="order",
            status="write_credentials_missing",
            room_id="room-1",
            notional_dollars=Decimal("100.0000"),
            occurred_at=NOW.replace(hour=10, minute=1),
        ),
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(),
        policy=policy,
        recent_events=events,
        now=NOW,
    )

    assert decision.allowed_live is True
    assert decision.daily_notional_used_dollars == 0.0
    assert decision.concurrent_positions == 0


def test_settled_trade_still_consumes_daily_bootstrap_cap() -> None:
    policy = AgentPackWeatherBootstrapPolicy(rollout_state="promoted_low")
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})
    events = [
        _event(
            event_type="risk",
            status="approved",
            room_id="room-1",
            order_id="order-1",
            notional_dollars=Decimal("100.0000"),
            occurred_at=NOW.replace(hour=10),
        ),
        _event(
            event_type="settlement",
            status="settled_win",
            room_id="room-1",
            order_id="order-1",
            pnl_dollars=Decimal("20.0000"),
            occurred_at=NOW.replace(hour=11),
        ),
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(),
        policy=policy,
        recent_events=events,
        now=NOW,
    )

    assert decision.allowed_live is False
    assert decision.reason == "bootstrap_daily_notional_cap_reached"


def test_mature_bucket_requires_positive_net_pnl() -> None:
    service = WeatherEmpiricalBootstrapService()
    policy = AgentPackWeatherBootstrapPolicy()
    promoted_policy = AgentPackWeatherBootstrapPolicy(
        rollout_state="promoted_normal",
        evidence_ready=True,
    )

    blocked = service.evaluate(
        context=_context(actual_sample_count=20, actual_net_pnl=Decimal("-0.01")),
        policy=policy,
        now=NOW,
    )
    shadow = service.evaluate(
        context=_context(actual_sample_count=20, actual_net_pnl=Decimal("1.00")),
        policy=policy,
        now=NOW,
    )
    allowed = service.evaluate(
        context=_context(actual_sample_count=20, actual_net_pnl=Decimal("1.00")),
        policy=promoted_policy,
        now=NOW,
    )

    assert blocked.reason == "bootstrap_mature_negative_net_pnl"
    assert shadow.allowed_live is False
    assert shadow.reason == "empirical_gate_mature_shadow_matched"
    assert allowed.allowed_live is True
    assert allowed.size_factor == 1.0


def test_weighted_shadow_evidence_contributes_to_mature_graduation() -> None:
    historical = [
        _historical(
            market_ticker=f"KXHIGHTSFO-26MAY{11 + idx}-T70",
            local_market_day=f"26MAY{11 + idx}",
            source_fingerprint=f"shadow-{idx}",
            pnl_dollars=Decimal("1.0000"),
        )
        for idx in range(4)
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(actual_sample_count=19, actual_net_pnl=Decimal("0.0000")),
        policy=AgentPackWeatherBootstrapPolicy(evidence_ready=True, rollout_state="promoted_normal"),
        historical_evidence=historical,
        now=NOW,
    )

    trace = decision.to_trace()
    assert decision.tier == "mature"
    assert decision.allowed_live is True
    assert trace["raw_live_sample_count"] == 19
    assert trace["shadow_weighted_sample_count"] == 1.0
    assert trace["weighted_sample_count"] == 20.0
    assert trace["weighted_net_pnl"] == 1.0


def test_fallback_shadow_evidence_does_not_contribute_to_graduation() -> None:
    historical = [
        _historical(
            market_ticker=f"KXHIGHTSFO-26MAY{11 + idx}-T70",
            local_market_day=f"26MAY{11 + idx}",
            source_fingerprint=f"fallback-{idx}",
            payload={"fair_value_source": "fallback"},
        )
        for idx in range(4)
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(actual_sample_count=19, actual_net_pnl=Decimal("1.0000")),
        policy=AgentPackWeatherBootstrapPolicy(evidence_ready=True, rollout_state="promoted_normal"),
        historical_evidence=historical,
        now=NOW,
    )

    trace = decision.to_trace()
    assert decision.tier == "maturing"
    assert trace["shadow_weighted_sample_count"] == 0.0
    assert trace["weighted_sample_count"] == 19.0


def test_live_bootstrap_evidence_uses_size_factor_for_weighted_graduation() -> None:
    events = [
        _event(
            event_type="settlement",
            status="settled_win",
            source="live_settlement",
            market_ticker=f"KXHIGHTSFO-26MAY{11 + idx}-T70",
            local_market_day=f"26MAY{11 + idx}",
            bucket_key="KXHIGHTSFO|SFO|no|A|50-59c|delta:5|conf:high|spread:100-249bps",
            size_factor=0.5,
            pnl_dollars=Decimal("1.0000"),
        )
        for idx in range(2)
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(actual_sample_count=19, actual_net_pnl=Decimal("0.0000")),
        policy=AgentPackWeatherBootstrapPolicy(evidence_ready=True, rollout_state="promoted_normal"),
        recent_events=events,
        now=NOW,
    )

    trace = decision.to_trace()
    assert decision.tier == "mature"
    assert trace["bootstrap_weighted_sample_count"] == 1.0
    assert trace["weighted_sample_count"] == 20.0
    assert trace["weighted_net_pnl"] == 1.0


def test_three_consecutive_bootstrap_losses_trip_kill_switch() -> None:
    policy = AgentPackWeatherBootstrapPolicy(rollout_state="promoted_low")
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})
    events = [
        _event(
            event_type="settlement",
            status="settled_loss",
            source="live_settlement",
            market_ticker=f"KXHIGHTSFO-26MAY{11 + idx}-T70",
            local_market_day=f"26MAY{11 + idx}",
            pnl_dollars=Decimal("-1.0000"),
            occurred_at=NOW.replace(minute=idx),
        )
        for idx in range(3)
    ]

    decision = WeatherEmpiricalBootstrapService().evaluate(
        context=_context(),
        policy=policy,
        recent_events=events,
        now=NOW,
    )

    assert decision.allowed_live is False
    assert decision.kill_switch_active is True
    assert decision.reason == "bootstrap_kill_switch_consecutive_losses"


def test_persisted_bootstrap_kill_switch_resets_only_after_operator_reset_or_policy_change() -> None:
    service = WeatherEmpiricalBootstrapService()
    policy = AgentPackWeatherBootstrapPolicy(
        rollout_state="promoted_low",
        kill_switch_state={
            "active": True,
            "reason": "bootstrap_kill_switch_consecutive_losses",
            "tripped_at": NOW.isoformat(),
            "policy_pack_version": "pack-v1",
        },
    )
    policy.tiers["cold"] = policy.tiers["cold"].model_copy(update={"live_enabled": True})

    blocked = service.evaluate(
        context=_context(policy_pack_version="pack-v1"),
        policy=policy,
        now=NOW,
    )
    reset_policy = policy.model_copy(
        update={
            "kill_switch_state": {
                **policy.kill_switch_state,
                "operator_reset_at": NOW.replace(minute=1).isoformat(),
            }
        }
    )
    reset = service.evaluate(
        context=_context(policy_pack_version="pack-v1"),
        policy=reset_policy,
        now=NOW,
    )
    changed_version = service.evaluate(
        context=_context(policy_pack_version="pack-v2"),
        policy=policy,
        now=NOW,
    )

    assert blocked.allowed_live is False
    assert blocked.kill_switch_active is True
    assert blocked.reason == "bootstrap_kill_switch_consecutive_losses"
    assert reset.allowed_live is True
    assert reset.kill_switch_active is False
    assert changed_version.allowed_live is True
    assert changed_version.kill_switch_active is False


def test_confidence_and_fair_value_source_helpers_are_deterministic() -> None:
    provenance = {"intraday_model": {"status": "fallback", "fallback_reason": "model_unavailable"}}
    assert fair_value_source_from_provenance(provenance, fair_yes_dollars=Decimal("0.5000")) == "fallback"
    assert fair_value_source_from_provenance({"intraday_model": {"status": "used"}}) == "intraday_model"
    assert confidence_source_from_trace({"calibrated_confidence": 0.92}, {}) == "calibrated"
    assert confidence_source_from_trace({}, {}) == "raw"
