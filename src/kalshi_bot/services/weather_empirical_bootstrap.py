from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Iterable

from kalshi_bot.core.schemas import AgentPackWeatherBootstrapPolicy, AgentPackWeatherBootstrapTier
from kalshi_bot.db.models import WeatherBootstrapEventRecord, WeatherBootstrapHistoricalEvidenceRecord
from kalshi_bot.services.trade_behavior import (
    EMPIRICAL_BUCKET_VERSION,
    coarse_empirical_bucket_key_from_legacy,
)


BOOTSTRAP_POLICY_VERSION = "weather-empirical-bootstrap-v2"
STRICT_REPLAY_SHADOW_WEIGHT = Decimal("0.25")
BOOTSTRAP_ACTIVE_STATUSES = {
    "live_allowed",
    "approved",
    "ordered",
    "submitted",
    "resting",
    "filled",
    "executed",
}
BOOTSTRAP_TERMINAL_NON_POSITION_STATUSES = {
    "blocked",
    "risk_blocked",
    "rejected",
    "write_credentials_missing",
    "kill_switch_blocked",
    "inactive_color_skipped",
    "shadow_skipped",
    "lock_denied",
    "canceled",
    "cancelled",
    "unfilled_cancelled",
}
BOOTSTRAP_SETTLED_STATUSES = {"settled_win", "settled_loss", "settled"}
DISQUALIFIED_FAIR_VALUE_SOURCES = {"fallback", "unavailable", "dark", "none"}


def _as_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]


def market_day_from_ticker(market_ticker: str | None) -> str | None:
    parts = str(market_ticker or "").split("-")
    return parts[1] if len(parts) > 1 else None


def _bucket_identity(value: str | None) -> str | None:
    return coarse_empirical_bucket_key_from_legacy(value)


def _is_disqualified_fair_value_source(value: Any) -> bool:
    return str(value or "").strip().lower() in DISQUALIFIED_FAIR_VALUE_SOURCES


def _event_fair_value_source(event: WeatherBootstrapEventRecord) -> str | None:
    payload = event.payload if isinstance(event.payload, dict) else {}
    return str(payload.get("fair_value_source") or "").strip().lower() or None


def _historical_fair_value_source(row: WeatherBootstrapHistoricalEvidenceRecord) -> str | None:
    payload = row.payload if isinstance(row.payload, dict) else {}
    return str(payload.get("fair_value_source") or "").strip().lower() or None


def fair_value_source_from_provenance(
    provenance: dict[str, Any] | None,
    *,
    fair_yes_dollars: Decimal | None = None,
) -> str:
    data = dict(provenance or {})
    explicit = str(
        data.get("fair_value_source")
        or data.get("source")
        or data.get("model_source")
        or ""
    ).strip().lower()
    if explicit in {"fallback", "unavailable", "dark", "none"}:
        return "fallback"
    if explicit in {"calibrated", "intraday", "intraday_model", "model", "ensemble"}:
        return explicit

    intraday = data.get("intraday_model")
    if isinstance(intraday, dict):
        status = str(intraday.get("status") or "").strip().lower()
        reason = str(intraday.get("fallback_reason") or intraday.get("reason") or "").strip().lower()
        if status in {"used", "active", "ok"}:
            return "intraday_model"
        if status in {"fallback", "unavailable", "disabled", "missing"}:
            if fair_yes_dollars is not None and fair_yes_dollars != Decimal("0.5000"):
                return "model"
            if reason in {"", "model_unavailable", "unavailable", "missing", "stale_artifact", "series_regression_fallback"}:
                return "fallback"

    model = data.get("model")
    if isinstance(model, dict) and str(model.get("status") or "").lower() in {"ok", "used", "active"}:
        return "model"
    if data.get("prediction_enabled") is True or data.get("calibration") is not None:
        return "model"
    if fair_yes_dollars is not None and fair_yes_dollars != Decimal("0.5000"):
        return "model"
    return "unknown"


def confidence_source_from_trace(
    trace: dict[str, Any] | None,
    provenance: dict[str, Any] | None,
) -> str:
    values = [dict(trace or {}), dict(provenance or {})]
    for data in values:
        explicit = str(
            data.get("confidence_source")
            or data.get("calibrated_confidence_source")
            or ""
        ).strip().lower()
        if explicit in {"calibrated", "evidence", "evidence_backed", "calibration"}:
            return "calibrated"
        if explicit == "raw":
            return "raw"
        if data.get("calibrated_confidence") is not None or data.get("confidence_calibrated") is True:
            return "calibrated"
    return "raw"


def stale_signal_evidence_from_trace(trace: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(trace or {})
    freshness = dict(data.get("freshness") or data.get("source_freshness") or {})
    stale_reasons: list[str] = []
    for key in ("market_stale", "research_stale", "weather_stale", "model_stale", "source_stale"):
        if data.get(key) is True or freshness.get(key) is True:
            stale_reasons.append(key)
    static_guard = data.get("static_signal_guard") or (data.get("trading_improvement") or {}).get("static_signal_guard")
    if isinstance(static_guard, dict):
        stale_hint = static_guard.get("stale") or static_guard.get("stale_data") or static_guard.get("source_stale")
        if stale_hint is True:
            stale_reasons.append("static_guard_stale")
    return {
        "stale": bool(stale_reasons),
        "reason_codes": list(dict.fromkeys(stale_reasons)),
        "static_repetition_is_persistence": not bool(stale_reasons),
    }


@dataclass(frozen=True, slots=True)
class WeatherEmpiricalBootstrapContext:
    kalshi_env: str
    market_ticker: str
    side: str | None
    confidence: float | None
    edge_bps_after_buffer: int | float | None
    fair_yes_dollars: Decimal | None
    fair_value_source: str
    confidence_source: str
    bucket_key: str | None
    legacy_bucket_key: str | None = None
    actual_sample_count: int = 0
    actual_net_pnl: Decimal | None = None
    current_stand_down_reason: str | None = None
    pre_empirical_stand_down_reason: str | None = None
    policy_key: str | None = None
    fallback_policy_key: str | None = None
    market_observed_at: datetime | None = None
    data_stale: bool = False
    source_stale_reasons: tuple[str, ...] = ()
    room_id: str | None = None
    policy_pack_version: str | None = None


@dataclass(frozen=True, slots=True)
class BootstrapEvidenceStats:
    bucket_key: str | None
    legacy_bucket_key: str | None
    raw_live_sample_count: int
    live_weighted_sample_count: Decimal
    bootstrap_weighted_sample_count: Decimal
    shadow_weighted_sample_count: Decimal
    weighted_sample_count: Decimal
    weighted_net_pnl: Decimal | None


@dataclass(frozen=True, slots=True)
class WeatherEmpiricalBootstrapDecision:
    matched: bool
    applied: bool
    would_have_entered: bool
    allowed_live: bool
    outcome: str
    reason: str
    tier: str | None
    sample_count: float
    bucket_key: str | None
    legacy_bucket_key: str | None
    bucket_key_version: str
    raw_live_sample_count: int
    live_weighted_sample_count: float
    bootstrap_weighted_sample_count: float
    shadow_weighted_sample_count: float
    weighted_sample_count: float
    weighted_net_pnl: float | None
    policy_key: str | None
    fallback_policy_key: str | None
    rollout_state: str
    confidence: float | None
    confidence_source: str
    min_confidence_required: float | None
    edge_bps_after_buffer: int | None
    min_edge_bps_required: int | None
    fair_value_source: str
    size_factor: float
    daily_notional_cap_dollars: float | None
    max_concurrent_positions: int | None
    daily_notional_used_dollars: float
    concurrent_positions: int
    kill_switch_active: bool
    kill_switch_reason: str | None
    evidence_source: str
    lineage_id: str
    thresholds: dict[str, Any]
    reason_codes: list[str]

    def to_trace(self) -> dict[str, Any]:
        return {
            "policy_version": BOOTSTRAP_POLICY_VERSION,
            "matched": self.matched,
            "applied": self.applied,
            "would_have_entered": self.would_have_entered,
            "allowed_live": self.allowed_live,
            "outcome": self.outcome,
            "reason": self.reason,
            "tier": self.tier,
            "sample_count": self.sample_count,
            "bucket_key": self.bucket_key,
            "coarse_bucket_key": self.bucket_key,
            "legacy_bucket_key": self.legacy_bucket_key,
            "bucket_key_version": self.bucket_key_version,
            "raw_live_sample_count": self.raw_live_sample_count,
            "live_weighted_sample_count": self.live_weighted_sample_count,
            "bootstrap_weighted_sample_count": self.bootstrap_weighted_sample_count,
            "shadow_weighted_sample_count": self.shadow_weighted_sample_count,
            "weighted_sample_count": self.weighted_sample_count,
            "weighted_net_pnl": self.weighted_net_pnl,
            "policy_key": self.policy_key,
            "fallback_policy_key_used": self.fallback_policy_key,
            "rollout_state": self.rollout_state,
            "confidence": self.confidence,
            "confidence_source": self.confidence_source,
            "min_confidence_required": self.min_confidence_required,
            "edge_bps_after_buffer": self.edge_bps_after_buffer,
            "min_edge_bps_required": self.min_edge_bps_required,
            "fair_value_source": self.fair_value_source,
            "size_factor": self.size_factor,
            "daily_notional_cap_dollars": self.daily_notional_cap_dollars,
            "max_concurrent_positions": self.max_concurrent_positions,
            "daily_notional_used_dollars": self.daily_notional_used_dollars,
            "concurrent_positions": self.concurrent_positions,
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_reason": self.kill_switch_reason,
            "evidence_source": self.evidence_source,
            "lineage_id": self.lineage_id,
            "thresholds": dict(self.thresholds),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class BootstrapShadowGateStatus:
    status: str
    eligible_for_promotion: bool
    observed_hours: float
    market_episodes: int
    intended_matches: int
    actual_matches: int
    match_rate: float
    fallback_matches: int
    stale_matches: int
    reason_codes: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "eligible_for_promotion": self.eligible_for_promotion,
            "observed_hours": round(self.observed_hours, 4),
            "market_episodes": self.market_episodes,
            "intended_matches": self.intended_matches,
            "actual_matches": self.actual_matches,
            "match_rate": round(self.match_rate, 6),
            "fallback_matches": self.fallback_matches,
            "stale_matches": self.stale_matches,
            "reason_codes": list(self.reason_codes),
        }


class WeatherEmpiricalBootstrapService:
    """Deterministic bootstrap evaluator for weather empirical-history cold starts."""

    def evaluate(
        self,
        *,
        context: WeatherEmpiricalBootstrapContext,
        policy: AgentPackWeatherBootstrapPolicy,
        recent_events: Iterable[WeatherBootstrapEventRecord] = (),
        historical_evidence: Iterable[WeatherBootstrapHistoricalEvidenceRecord] = (),
        now: datetime | None = None,
    ) -> WeatherEmpiricalBootstrapDecision:
        now_utc = _as_utc(now) or datetime.now(UTC)
        rollout = str(policy.rollout_state or "shadow").strip().lower()
        policy_key = context.policy_key
        evidence_stats = self._evidence_stats(context, recent_events, historical_evidence)
        sample_count = evidence_stats.weighted_sample_count
        tier_name, tier = self._tier_for(sample_count, policy)
        thresholds = self._threshold_payload(tier_name, tier, policy)
        lineage_id = _json_hash(
            {
                "policy_version": BOOTSTRAP_POLICY_VERSION,
                "policy_key": policy_key,
                "market_ticker": context.market_ticker,
                "bucket_key": evidence_stats.bucket_key,
                "legacy_bucket_key": evidence_stats.legacy_bucket_key,
                "tier": tier_name,
                "weighted_sample_count": str(sample_count),
                "rollout_state": rollout,
                "thresholds": thresholds,
            }
        )

        base_kwargs = {
            "sample_count": float(sample_count),
            "bucket_key": evidence_stats.bucket_key,
            "legacy_bucket_key": evidence_stats.legacy_bucket_key,
            "bucket_key_version": EMPIRICAL_BUCKET_VERSION,
            "raw_live_sample_count": evidence_stats.raw_live_sample_count,
            "live_weighted_sample_count": float(evidence_stats.live_weighted_sample_count),
            "bootstrap_weighted_sample_count": float(evidence_stats.bootstrap_weighted_sample_count),
            "shadow_weighted_sample_count": float(evidence_stats.shadow_weighted_sample_count),
            "weighted_sample_count": float(evidence_stats.weighted_sample_count),
            "weighted_net_pnl": float(evidence_stats.weighted_net_pnl) if evidence_stats.weighted_net_pnl is not None else None,
            "policy_key": policy_key,
            "fallback_policy_key": context.fallback_policy_key,
            "rollout_state": rollout,
            "confidence": context.confidence,
            "confidence_source": context.confidence_source,
            "fair_value_source": context.fair_value_source,
            "size_factor": 0.0,
            "daily_notional_cap_dollars": None,
            "max_concurrent_positions": None,
            "daily_notional_used_dollars": 0.0,
            "concurrent_positions": 0,
            "kill_switch_active": False,
            "kill_switch_reason": None,
            "evidence_source": "live_forward",
            "lineage_id": lineage_id,
            "thresholds": thresholds,
        }

        def blocked(reason: str, *codes: str) -> WeatherEmpiricalBootstrapDecision:
            return WeatherEmpiricalBootstrapDecision(
                matched=False,
                applied=False,
                would_have_entered=False,
                allowed_live=False,
                outcome="block",
                reason=reason,
                tier=tier_name,
                min_confidence_required=thresholds.get("min_confidence"),
                edge_bps_after_buffer=self._edge_int(context.edge_bps_after_buffer),
                min_edge_bps_required=thresholds.get("min_edge_bps"),
                reason_codes=list(codes or (reason,)),
                **base_kwargs,
            )

        if not policy.enabled:
            return blocked("weather_empirical_bootstrap_disabled", "policy_disabled")
        if tier_name is None or (tier is None and tier_name != "mature"):
            return blocked("weather_empirical_bootstrap_tier_unavailable", "tier_unavailable")
        if policy.fair_value_fallback_disqualifies and context.fair_value_source in DISQUALIFIED_FAIR_VALUE_SOURCES:
            return blocked("fair_value_source_disqualified", "fair_value_source_disqualified")
        if context.data_stale:
            return blocked("bootstrap_stale_data_blocked", "stale_data", *context.source_stale_reasons)

        mature_pnl = evidence_stats.weighted_net_pnl
        kill_active, kill_reason = self._kill_switch(policy, recent_events, policy_pack_version=context.policy_pack_version)
        if tier_name == "mature":
            mature_live = bool(
                policy.evidence_ready
                and rollout in {"live", "canary", "promoted", "promoted_normal", "expanded"}
            )
            if mature_pnl is None or mature_pnl <= Decimal("0"):
                return blocked("bootstrap_mature_negative_net_pnl", "mature_negative_net_pnl")
            if kill_active:
                decision = blocked(kill_reason or "bootstrap_kill_switch_active", "kill_switch_active")
                return replace(decision, kill_switch_active=True, kill_switch_reason=kill_reason)
            return WeatherEmpiricalBootstrapDecision(
                matched=True,
                applied=mature_live,
                would_have_entered=True,
                allowed_live=mature_live,
                outcome="allow" if mature_live else "shadow_allow",
                reason="empirical_gate_mature_passed" if mature_live else "empirical_gate_mature_shadow_matched",
                tier=tier_name,
                min_confidence_required=None,
                edge_bps_after_buffer=self._edge_int(context.edge_bps_after_buffer),
                min_edge_bps_required=None,
                reason_codes=[
                    "mature_empirical_gate_passed" if mature_live else "mature_requires_promoted_policy",
                ],
                **{**base_kwargs, "size_factor": 1.0},
            )

        confidence_required = float(tier.min_confidence)
        edge_required = int(tier.min_edge_bps)
        if context.confidence_source != "calibrated":
            if policy.raw_confidence_cold_only and tier_name != "cold":
                return blocked("raw_confidence_only_allowed_for_cold_tier", "raw_confidence_non_cold")
            confidence_required = round(confidence_required + float(policy.raw_confidence_min_confidence_penalty), 6)
            edge_required += int(policy.raw_confidence_min_edge_bps_penalty)
        if context.confidence is None or float(context.confidence) < confidence_required:
            return blocked(
                f"bootstrap_{tier_name}_confidence_below_threshold",
                "confidence_below_threshold",
            )
        edge_int = self._edge_int(context.edge_bps_after_buffer)
        if edge_int is None or edge_int < edge_required:
            return blocked(f"bootstrap_{tier_name}_edge_below_threshold", "edge_below_threshold")
        if kill_active:
            decision = blocked(kill_reason or "bootstrap_kill_switch_active", "kill_switch_active")
            return replace(decision, kill_switch_active=True, kill_switch_reason=kill_reason)

        cap_dollars, concurrent_cap = self._caps(policy, rollout=rollout)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_used = self._daily_notional_used(recent_events, since=day_start)
        concurrent = self._concurrent_positions(recent_events)
        if cap_dollars is not None and daily_used >= Decimal(str(cap_dollars)):
            return blocked("bootstrap_daily_notional_cap_reached", "daily_notional_cap")
        if concurrent_cap is not None and concurrent >= concurrent_cap:
            return blocked("bootstrap_concurrent_position_cap_reached", "concurrent_position_cap")

        live_enabled = bool(tier.live_enabled and rollout in {"live", "canary", "promoted", "promoted_low", "promoted_normal"})
        return WeatherEmpiricalBootstrapDecision(
            matched=True,
            applied=live_enabled,
            would_have_entered=True,
            allowed_live=live_enabled,
            outcome="allow" if live_enabled else "shadow_allow",
            reason=f"bootstrap_{tier_name}_{'live_allowed' if live_enabled else 'shadow_matched'}",
            tier=tier_name,
            min_confidence_required=confidence_required,
            edge_bps_after_buffer=edge_int,
            min_edge_bps_required=edge_required,
            reason_codes=[
                f"{tier_name}_tier_matched",
                "live_enabled" if live_enabled else "shadow_only",
            ],
            **{
                **base_kwargs,
                "size_factor": float(tier.size_factor),
                "daily_notional_cap_dollars": cap_dollars,
                "max_concurrent_positions": concurrent_cap,
                "daily_notional_used_dollars": float(daily_used),
                "concurrent_positions": concurrent,
                "kill_switch_active": False,
                "kill_switch_reason": None,
                "thresholds": {
                    **thresholds,
                    "min_confidence": confidence_required,
                    "min_edge_bps": edge_required,
                },
            },
        )

    def shadow_gate_status(
        self,
        *,
        policy: AgentPackWeatherBootstrapPolicy,
        events: Iterable[WeatherBootstrapEventRecord],
        policy_key: str | None = None,
        now: datetime | None = None,
    ) -> BootstrapShadowGateStatus:
        event_list = [
            event for event in events
            if policy_key is None or event.policy_key == policy_key
        ]
        now_utc = _as_utc(now) or datetime.now(UTC)
        first_seen = min((_as_utc(event.occurred_at) for event in event_list if event.occurred_at is not None), default=None)
        observed_hours = (now_utc - first_seen).total_seconds() / 3600 if first_seen is not None else 0.0
        cold_episodes: dict[tuple[str | None, str | None, str | None], list[WeatherBootstrapEventRecord]] = {}
        for event in event_list:
            if event.event_type != "decision" or event.tier != "cold":
                continue
            bucket_key = _bucket_identity(event.bucket_key)
            key = (
                event.market_ticker,
                event.local_market_day or market_day_from_ticker(event.market_ticker),
                bucket_key,
            )
            cold_episodes.setdefault(key, []).append(event)

        intended: list[WeatherBootstrapEventRecord] = []
        actual: list[WeatherBootstrapEventRecord] = []
        for episode_events in cold_episodes.values():
            latest = max(
                episode_events,
                key=lambda event: _as_utc(event.occurred_at) or datetime.min.replace(tzinfo=UTC),
            )
            matched = any(
                bool((event.payload or {}).get("matched"))
                or bool((event.payload or {}).get("would_have_entered"))
                or event.status in {"shadow_allow", "live_allowed"}
                for event in episode_events
            )
            if not matched:
                continue
            intended.append(latest)
            if any(event.status in {"shadow_allow", "live_allowed"} for event in episode_events):
                actual.append(latest)
        fallback = [
            event for event in actual
            if _is_disqualified_fair_value_source((event.payload or {}).get("fair_value_source"))
        ]
        stale = [
            event for event in actual
            if bool((event.payload or {}).get("data_stale"))
        ]
        match_rate = len(actual) / len(intended) if intended else 0.0
        reason_codes: list[str] = []
        if observed_hours < policy.caps.shadow_min_hours:
            reason_codes.append("shadow_min_hours_not_met")
        if len(intended) < policy.caps.shadow_min_market_episodes:
            reason_codes.append("shadow_market_episode_support_not_met")
        if intended and match_rate < policy.caps.shadow_required_match_rate:
            reason_codes.append("shadow_match_rate_below_threshold")
        if fallback:
            reason_codes.append("fallback_fair_value_matched")
        if stale:
            reason_codes.append("stale_data_matched")
        eligible = not reason_codes and bool(intended)
        return BootstrapShadowGateStatus(
            status="eligible" if eligible else "pending",
            eligible_for_promotion=eligible,
            observed_hours=observed_hours,
            market_episodes=len(intended),
            intended_matches=len(intended),
            actual_matches=len(actual),
            match_rate=match_rate,
            fallback_matches=len(fallback),
            stale_matches=len(stale),
            reason_codes=tuple(reason_codes),
        )

    def _sample_count(
        self,
        context: WeatherEmpiricalBootstrapContext,
        events: Iterable[WeatherBootstrapEventRecord],
        historical_evidence: Iterable[WeatherBootstrapHistoricalEvidenceRecord],
    ) -> int:
        return int(self._evidence_stats(context, events, historical_evidence).weighted_sample_count)

    def _evidence_stats(
        self,
        context: WeatherEmpiricalBootstrapContext,
        events: Iterable[WeatherBootstrapEventRecord],
        historical_evidence: Iterable[WeatherBootstrapHistoricalEvidenceRecord],
    ) -> BootstrapEvidenceStats:
        bucket_key = _bucket_identity(context.bucket_key)
        legacy_bucket_key = context.legacy_bucket_key
        if legacy_bucket_key is None and context.bucket_key != bucket_key:
            legacy_bucket_key = context.bucket_key

        live_count = max(0, int(context.actual_sample_count or 0))
        live_weight = Decimal(live_count)
        weighted_values: list[Decimal] = []
        if context.actual_net_pnl is not None:
            weighted_values.append(Decimal(str(context.actual_net_pnl)))

        event_episodes: dict[tuple[str | None, str | None, str | None], WeatherBootstrapEventRecord] = {}
        for event in events:
            event_bucket_key = _bucket_identity(event.bucket_key)
            if bucket_key is not None and event_bucket_key != bucket_key:
                continue
            status = str(event.status or "").lower()
            if status not in BOOTSTRAP_SETTLED_STATUSES and event.pnl_dollars is None:
                continue
            if _is_disqualified_fair_value_source(_event_fair_value_source(event)):
                continue
            episode_key = (
                event.market_ticker,
                event.local_market_day or market_day_from_ticker(event.market_ticker),
                event_bucket_key,
            )
            prior = event_episodes.get(episode_key)
            if prior is None or (_as_utc(event.occurred_at) or datetime.min.replace(tzinfo=UTC)) >= (
                _as_utc(prior.occurred_at) or datetime.min.replace(tzinfo=UTC)
            ):
                event_episodes[episode_key] = event

        bootstrap_weight = Decimal("0")
        shadow_weight = Decimal("0")
        for event in event_episodes.values():
            source = str(event.source or "").lower()
            if source in {"shadow", "decision_trace_backfill", "strict_replay", "historical_replay"}:
                weight = STRICT_REPLAY_SHADOW_WEIGHT
                shadow_weight += weight
            else:
                weight = Decimal(str(event.size_factor if event.size_factor is not None else event.evidence_weight or 1.0))
                bootstrap_weight += weight
            if event.pnl_dollars is not None:
                weighted_values.append(Decimal(str(event.pnl_dollars)) * weight)

        historical_episodes: dict[tuple[str | None, str | None, str | None], WeatherBootstrapHistoricalEvidenceRecord] = {}
        for row in historical_evidence:
            if not row.strict_replay:
                continue
            row_bucket_key = _bucket_identity(row.bucket_key)
            if bucket_key is not None and row_bucket_key != bucket_key:
                continue
            if row.pnl_dollars is None and row.outcome not in {"win", "loss", "settled"}:
                continue
            if _is_disqualified_fair_value_source(_historical_fair_value_source(row)):
                continue
            episode_key = (
                row.market_ticker,
                row.local_market_day or market_day_from_ticker(row.market_ticker),
                row_bucket_key,
            )
            prior = historical_episodes.get(episode_key)
            if prior is None or (_as_utc(row.observed_at) or datetime.min.replace(tzinfo=UTC)) >= (
                _as_utc(prior.observed_at) or datetime.min.replace(tzinfo=UTC)
            ):
                historical_episodes[episode_key] = row

        for row in historical_episodes.values():
            shadow_weight += STRICT_REPLAY_SHADOW_WEIGHT
            if row.pnl_dollars is not None:
                weighted_values.append(Decimal(str(row.pnl_dollars)) * STRICT_REPLAY_SHADOW_WEIGHT)

        weighted_count = live_weight + bootstrap_weight + shadow_weight
        return BootstrapEvidenceStats(
            bucket_key=bucket_key,
            legacy_bucket_key=legacy_bucket_key,
            raw_live_sample_count=live_count,
            live_weighted_sample_count=live_weight,
            bootstrap_weighted_sample_count=bootstrap_weight,
            shadow_weighted_sample_count=shadow_weight,
            weighted_sample_count=weighted_count,
            weighted_net_pnl=sum(weighted_values, Decimal("0")) if weighted_values else None,
        )

    def _tier_for(
        self,
        sample_count: Decimal,
        policy: AgentPackWeatherBootstrapPolicy,
    ) -> tuple[str | None, AgentPackWeatherBootstrapTier | None]:
        if sample_count >= Decimal("20"):
            return "mature", None
        for name in ("cold", "warming", "maturing"):
            tier = policy.tiers.get(name)
            if tier is None:
                continue
            high = Decimal(str(tier.max_samples)) if tier.max_samples is not None else Decimal("1000000000")
            if Decimal(str(tier.min_samples)) <= sample_count <= high:
                return name, tier
        return None, None

    def _caps(
        self,
        policy: AgentPackWeatherBootstrapPolicy,
        *,
        rollout: str,
    ) -> tuple[float | None, int | None]:
        if rollout in {"promoted", "promoted_normal", "expanded"}:
            return float(policy.caps.expanded_daily_notional_usd), int(policy.caps.expanded_max_concurrent_positions)
        return float(policy.caps.initial_daily_notional_usd), int(policy.caps.initial_max_concurrent_positions)

    def _daily_notional_used(
        self,
        events: Iterable[WeatherBootstrapEventRecord],
        *,
        since: datetime,
    ) -> Decimal:
        since_utc = _as_utc(since) or since
        active_by_key: dict[str, Decimal] = {}
        for event in sorted(
            events,
            key=lambda item: _as_utc(item.occurred_at) or datetime.min.replace(tzinfo=UTC),
        ):
            event_time = _as_utc(event.occurred_at)
            if event_time is None or event_time < since_utc:
                continue
            key = event.order_id or event.room_id or f"{event.market_ticker}:{event.bucket_key}"
            status = str(event.status or "").lower()
            if status in BOOTSTRAP_TERMINAL_NON_POSITION_STATUSES or status.startswith("rejected_"):
                active_by_key.pop(key, None)
                continue
            if status not in BOOTSTRAP_ACTIVE_STATUSES and status not in BOOTSTRAP_SETTLED_STATUSES:
                continue
            if event.notional_dollars is not None:
                active_by_key[key] = Decimal(str(event.notional_dollars))
        return sum(active_by_key.values(), Decimal("0"))

    def _concurrent_positions(self, events: Iterable[WeatherBootstrapEventRecord]) -> int:
        active: set[str] = set()
        settled: set[str] = set()
        for event in sorted(
            events,
            key=lambda item: _as_utc(item.occurred_at) or datetime.min.replace(tzinfo=UTC),
        ):
            key = event.market_ticker
            status = str(event.status or "").lower()
            if status in BOOTSTRAP_SETTLED_STATUSES:
                settled.add(key)
                active.discard(key)
            elif status in BOOTSTRAP_TERMINAL_NON_POSITION_STATUSES or status.startswith("rejected_"):
                active.discard(key)
            elif status in BOOTSTRAP_ACTIVE_STATUSES:
                active.add(key)
        return len(active - settled)

    def _kill_switch(
        self,
        policy: AgentPackWeatherBootstrapPolicy,
        events: Iterable[WeatherBootstrapEventRecord],
        *,
        policy_pack_version: str | None = None,
    ) -> tuple[bool, str | None]:
        state = dict(policy.kill_switch_state or {})
        if state.get("active") or state.get("tripped"):
            tripped_at = _as_utc(state.get("tripped_at"))
            reset_at = _as_utc(state.get("operator_reset_at") or state.get("reset_at"))
            state_policy_version = str(state.get("policy_pack_version") or "")
            policy_version_changed = bool(policy_pack_version and state_policy_version and state_policy_version != policy_pack_version)
            operator_reset = reset_at is not None and (tripped_at is None or reset_at >= tripped_at)
            if not operator_reset and not policy_version_changed:
                return True, str(state.get("reason") or "bootstrap_kill_switch_active")

        resolved = [
            event
            for event in events
            if event.pnl_dollars is not None or event.status in BOOTSTRAP_SETTLED_STATUSES
        ]
        resolved = sorted(resolved, key=lambda event: _as_utc(event.occurred_at) or datetime.min.replace(tzinfo=UTC), reverse=True)
        consecutive_loss_threshold = max(1, int(getattr(policy.caps, "consecutive_loss_kill_switch_threshold", 3)))
        consecutive_losses = 0
        for event in resolved:
            pnl = Decimal(str(event.pnl_dollars or "0"))
            if pnl < Decimal("0") or str(event.status or "").lower() == "settled_loss":
                consecutive_losses += 1
                if consecutive_losses >= consecutive_loss_threshold:
                    return True, "bootstrap_kill_switch_consecutive_losses"
                continue
            if pnl > Decimal("0") or str(event.status or "").lower() == "settled_win":
                break
        recent = resolved[: max(1, int(policy.caps.kill_switch_lookback))]
        if len(recent) < int(policy.caps.kill_switch_min_rows):
            return False, None
        wins = sum(1 for event in recent if Decimal(str(event.pnl_dollars or "0")) > 0)
        net = sum((Decimal(str(event.pnl_dollars or "0")) for event in recent), Decimal("0"))
        win_rate = wins / len(recent)
        if win_rate < float(policy.caps.kill_switch_min_win_rate):
            return True, "bootstrap_kill_switch_win_rate"
        if net < -Decimal(str(policy.caps.kill_switch_drawdown_usd)):
            return True, "bootstrap_kill_switch_drawdown"
        return False, None

    def _mature_net_pnl(
        self,
        context: WeatherEmpiricalBootstrapContext,
        events: Iterable[WeatherBootstrapEventRecord],
        historical_evidence: Iterable[WeatherBootstrapHistoricalEvidenceRecord],
    ) -> Decimal | None:
        return self._evidence_stats(context, events, historical_evidence).weighted_net_pnl

    def _threshold_payload(
        self,
        tier_name: str | None,
        tier: AgentPackWeatherBootstrapTier | None,
        policy: AgentPackWeatherBootstrapPolicy,
    ) -> dict[str, Any]:
        if tier is None:
            return {
                "tier": tier_name,
                "mature_min_samples": 20,
                "requires_positive_net_pnl": True,
                "requires_non_worse_drawdown": True,
            }
        return {
            "tier": tier_name,
            "min_samples": tier.min_samples,
            "max_samples": tier.max_samples,
            "min_confidence": tier.min_confidence,
            "min_edge_bps": tier.min_edge_bps,
            "size_factor": tier.size_factor,
            "live_enabled": tier.live_enabled,
            "raw_confidence_cold_only": policy.raw_confidence_cold_only,
            "raw_confidence_min_confidence_penalty": policy.raw_confidence_min_confidence_penalty,
            "raw_confidence_min_edge_bps_penalty": policy.raw_confidence_min_edge_bps_penalty,
        }

    def _edge_int(self, value: int | float | None) -> int | None:
        if value is None:
            return None
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None
