from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import AgentPack, AgentPackCryptoEntryPolicy, AgentPackCryptoPolicy, AgentPackWeatherPolicy
from kalshi_bot.crypto.services import (
    _crypto_decision_rows,
    _crypto_artifact_type,
    _crypto_walk_forward_folds,
    _decimal,
    _filter_crypto_snapshot_rows,
    _fit_crypto_calibration,
    _predict_crypto_probability,
    _simulate_crypto_trade,
    normalize_asset_symbol,
    normalize_asset_symbols,
)
from kalshi_bot.db.models import DecisionTraceRecord, WeatherBootstrapEventRecord, WeatherBootstrapHistoricalEvidenceRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeCryptoPolicy, RuntimeThresholds
from kalshi_bot.services.backtesting import build_backtesting_report
from kalshi_bot.services.gate_learning import (
    GateLearningRow,
    GateLearningService,
    decision_corpus_row_to_gate_learning_row,
)
from kalshi_bot.services.modeling import build_modeling_report
from kalshi_bot.services.trade_behavior import coarse_empirical_bucket_key_from_legacy, series_from_ticker
from kalshi_bot.services.weather_empirical_bootstrap import (
    STRICT_REPLAY_SHADOW_WEIGHT,
    WeatherEmpiricalBootstrapContext,
    WeatherEmpiricalBootstrapService,
    confidence_source_from_trace,
    fair_value_source_from_provenance,
    market_day_from_ticker,
    stale_signal_evidence_from_trace,
)
from kalshi_bot.services.weather_policy import (
    WeatherPolicyContext,
    build_weather_policy_key,
    weather_policy_context_from_market,
)


CHECKPOINT_PREFIX = "autonomous_gate_tuning"
OPS_SOURCE = "autonomous_gate_tuning"

TUNABLE_GATE_FIELDS = (
    "risk_min_contract_price_dollars",
    "strategy_min_remaining_payout_bps",
    "trigger_max_spread_bps",
    "risk_min_confidence",
    "risk_min_edge_bps",
    "strategy_min_abs_delta_f",
    "risk_max_credible_edge_bps",
)

BacktestingBuilder = Callable[..., Awaitable[dict[str, Any]]]
ModelingBuilder = Callable[..., Awaitable[dict[str, Any]]]

_UNKNOWN_ORIGINAL_GATE = "__unknown_original_gate__"
_EMPIRICAL_VISIBLE_REASONS = {
    "empirical_gate_block",
    "empirical_gate_under_sampled",
    "trade_behavior_gate_blocked",
}
_ALLOWED_BOOTSTRAP_ORIGINAL_REASONS = {None, "insufficient_forecast_separation"}
_REASON_ALIASES = {
    "forecast_too_close_to_threshold": "insufficient_forecast_separation",
    "forecast_separation": "insufficient_forecast_separation",
    "min_entry_price": "below_min_contract_price",
    "contract_price_too_low": "below_min_contract_price",
    "price_below_minimum_entry": "below_min_contract_price",
    "remaining_payout_min": "insufficient_remaining_payout",
    "below_min_remaining_payout": "insufficient_remaining_payout",
    "min_edge": "no_actionable_edge",
}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    numeric = _float_or_none(value)
    return int(round(numeric)) if numeric is not None else None


def _selected_trace_candidate(candidate_trace: dict[str, Any]) -> dict[str, Any]:
    side = str(candidate_trace.get("selected_side") or "").lower()
    if side in {"yes", "no"} and isinstance(candidate_trace.get(side), dict):
        return dict(candidate_trace[side])
    selected = candidate_trace.get("selected_candidate")
    if isinstance(selected, dict):
        return dict(selected)
    candidates = [row for row in candidate_trace.get("candidates") or [] if isinstance(row, dict)]
    if candidates:
        return max(
            candidates,
            key=lambda row: (
                _int_or_none(row.get("quality_adjusted_edge_bps")) or -1_000_000,
                _int_or_none(row.get("edge_bps")) or -1_000_000,
            ),
        )
    return {}


def _empirical_reason(empirical: dict[str, Any]) -> str:
    return _normalize_block_reason(empirical.get("underlying_reason") or empirical.get("reason")) or ""


def _trace_side(signal: dict[str, Any], candidate_trace: dict[str, Any], selected: dict[str, Any]) -> str | None:
    side = signal.get("recommended_side") or candidate_trace.get("selected_side") or selected.get("side")
    text = str(side or "").lower()
    return text if text in {"yes", "no"} else None


def _str_or_none(value: Any) -> str | None:
    return str(value).strip() if value not in (None, "") else None


def _normalize_block_reason(value: Any) -> str | None:
    text = _str_or_none(value)
    if text is None:
        return None
    normalized = text.lower().strip()
    return _REASON_ALIASES.get(normalized, normalized)


def _trace_reason_candidates(
    *,
    trace: dict[str, Any],
    signal: dict[str, Any],
    eligibility: dict[str, Any],
    candidate_trace: dict[str, Any],
    selected: dict[str, Any],
) -> list[str]:
    nested_candidate = _dict(eligibility.get("candidate_trace"))
    raw_values = [
        candidate_trace.get("pre_empirical_stand_down_reason"),
        nested_candidate.get("pre_empirical_stand_down_reason"),
        candidate_trace.get("baseline_block_reason"),
        nested_candidate.get("baseline_block_reason"),
        trace.get("baseline_block_reason"),
        candidate_trace.get("eligibility_stand_down_reason"),
        nested_candidate.get("eligibility_stand_down_reason"),
        trace.get("eligibility_stand_down_reason"),
        eligibility.get("stand_down_reason"),
        candidate_trace.get("stand_down_reason"),
        candidate_trace.get("final_stand_down_reason"),
        trace.get("final_stand_down_reason"),
        signal.get("stand_down_reason"),
        trace.get("stand_down_reason"),
        selected.get("baseline_reason"),
        selected.get("reason"),
        selected.get("stand_down_reason"),
    ]
    reasons: list[str] = []
    for value in raw_values:
        reason = _normalize_block_reason(value)
        if reason is not None:
            reasons.append(reason)
    return reasons


def _trace_policy_original_reason(
    *,
    trace: dict[str, Any],
    signal: dict[str, Any],
    eligibility: dict[str, Any],
    candidate_trace: dict[str, Any],
    selected: dict[str, Any],
) -> str | None:
    reasons = _trace_reason_candidates(
        trace=trace,
        signal=signal,
        eligibility=eligibility,
        candidate_trace=candidate_trace,
        selected=selected,
    )
    non_empirical = [reason for reason in reasons if reason not in _EMPIRICAL_VISIBLE_REASONS]
    if non_empirical:
        return non_empirical[0]
    if any(reason in _EMPIRICAL_VISIBLE_REASONS for reason in reasons):
        return None
    return _UNKNOWN_ORIGINAL_GATE


def _bootstrap_original_reason_allowed(reason: str | None) -> bool:
    return reason in _ALLOWED_BOOTSTRAP_ORIGINAL_REASONS


class AutonomousGateTuningService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker,
        agent_pack_service: AgentPackService,
        decision_corpus_service: Any,
        trade_analysis_service: Any,
        trading_audit_service: Any,
        backtesting_builder: BacktestingBuilder = build_backtesting_report,
        modeling_builder: ModelingBuilder = build_modeling_report,
        gate_learning_service_factory: Callable[[Settings], GateLearningService] | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.agent_pack_service = agent_pack_service
        self.decision_corpus_service = decision_corpus_service
        self.trade_analysis_service = trade_analysis_service
        self.trading_audit_service = trading_audit_service
        self.backtesting_builder = backtesting_builder
        self.modeling_builder = modeling_builder
        self.gate_learning_service_factory = gate_learning_service_factory or GateLearningService

    async def status(
        self,
        *,
        kalshi_env: str | None = None,
        domain: str = "all",
        policy_scope: str = "global",
        series_ticker: str | None = None,
        side: str | None = None,
        month: int | str | None = None,
        lane: str = "entry_gate",
    ) -> dict[str, Any]:
        normalized_domain = _normalize_domain(domain)
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            await self.agent_pack_service.ensure_initialized(repo)
            checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
            crypto_checkpoint = await repo.get_checkpoint(_checkpoint_name(env, domain="crypto"))
            control = await repo.get_deployment_control()
            active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            active_thresholds = _threshold_values(self.agent_pack_service.runtime_thresholds(active_pack))
            weather_scope = _weather_scope_payload(
                policy_scope=policy_scope,
                series_ticker=series_ticker,
                side=side,
                month=month,
                lane=lane,
            )
            weather_resolution = self.agent_pack_service.resolve_weather_policy(
                active_pack,
                _weather_policy_context_from_scope(weather_scope),
            )
            bootstrap_policy = (
                weather_resolution.policy.bootstrap
                if weather_resolution.policy is not None
                else None
            )
            bootstrap_events = await repo.list_weather_bootstrap_events(
                kalshi_env=env,
                since=datetime.now(UTC) - timedelta(days=7),
                limit=2000,
            )
            bootstrap_historical_evidence = await repo.list_weather_bootstrap_historical_evidence(
                kalshi_env=env,
                strict_replay=True,
                limit=5000,
            )
            bootstrap_shadow = (
                WeatherEmpiricalBootstrapService().shadow_gate_status(
                    policy=bootstrap_policy,
                    events=bootstrap_events,
                    policy_key=weather_resolution.policy_key,
                    now=datetime.now(UTC),
                ).to_payload()
                if bootstrap_policy is not None
                else None
            )
            bootstrap_historical = (
                _historical_bootstrap_shadow_gate_status(
                    policy=bootstrap_policy,
                    evidence=bootstrap_historical_evidence,
                    policy_key=weather_resolution.policy_key,
                    now=datetime.now(UTC),
                )
                if bootstrap_policy is not None
                else None
            )
            active_crypto_policy = _crypto_policy_values(self.agent_pack_service.runtime_crypto_policy(active_pack))
            await session.commit()
        settings_thresholds = _settings_threshold_values(self.settings)
        checkpoint_payload = checkpoint.payload if checkpoint is not None else None
        loosened_fields = _loosened_threshold_fields(active_thresholds, settings_thresholds)
        bootstrap_blocks_live = False
        bootstrap_block_reasons: list[str] = []
        if bootstrap_policy is not None and loosened_fields:
            rollout_state = str(bootstrap_policy.rollout_state or "shadow").lower()
            if rollout_state == "shadow" or not bootstrap_policy.evidence_ready:
                bootstrap_blocks_live = True
                bootstrap_block_reasons.append("bootstrap_not_evidence_ready")
            if bootstrap_shadow and not bootstrap_shadow.get("eligible_for_promotion"):
                bootstrap_blocks_live = True
                bootstrap_block_reasons.extend(str(code) for code in bootstrap_shadow.get("reason_codes") or [])
            if bootstrap_historical and not bootstrap_historical.get("eligible_for_promotion"):
                bootstrap_blocks_live = True
                bootstrap_block_reasons.extend(str(code) for code in bootstrap_historical.get("reason_codes") or [])
        active_weather_valid_until = getattr(weather_resolution.policy, "valid_until", None) if weather_resolution.policy is not None else None
        payload = {
            "status": ((checkpoint_payload or {}).get("status") if checkpoint_payload is not None else "not_started"),
            "kalshi_env": env,
            "domain": normalized_domain,
            "llm_calls_enabled": bool(self.settings.llm_calls_enabled),
            "deterministic_runtime": not bool(self.settings.llm_calls_enabled),
            "active_color": control.active_color,
            "active_pack_version": active_pack.version,
            "active_thresholds": active_thresholds,
            "active_weather_policy": weather_resolution.provenance(),
            "weather_bootstrap": {
                "policy": bootstrap_policy.model_dump(mode="json") if bootstrap_policy is not None else None,
                "shadow_gate": bootstrap_shadow,
                "historical_gate": bootstrap_historical,
                "recent_event_count": len(bootstrap_events),
                "strict_historical_evidence_count": len(bootstrap_historical_evidence),
                "active_policy_valid_until": active_weather_valid_until.isoformat() if active_weather_valid_until is not None else None,
                "active_policy_expired": bool(active_weather_valid_until is not None and active_weather_valid_until < datetime.now(UTC)),
                "active_threshold_relaxation_blocked": bootstrap_blocks_live,
                "active_threshold_relaxation_block_reasons": list(dict.fromkeys(bootstrap_block_reasons)),
                "loosened_threshold_fields": loosened_fields,
            },
            "active_crypto_policy": active_crypto_policy,
            "settings_thresholds": settings_thresholds,
            "threshold_drift": _threshold_drift(active_thresholds, settings_thresholds),
            "checkpoint": checkpoint_payload,
            "agent_pack_notes": dict((control.notes or {}).get("agent_packs") or {}),
            "last_recommendation": (checkpoint_payload or {}).get("recommendation_summary")
            or (checkpoint_payload or {}).get("recommendation"),
            "last_stage": _stage_summary(checkpoint_payload),
            "last_canary": (checkpoint_payload or {}).get("canary") if checkpoint_payload else None,
            "last_promotion": _promotion_summary(checkpoint_payload),
        }
        if normalized_domain in {"crypto", "all"}:
            crypto_payload = crypto_checkpoint.payload if crypto_checkpoint is not None else None
            payload["crypto"] = {
                "status": (crypto_payload or {}).get("status") if crypto_payload is not None else "not_started",
                "checkpoint": crypto_payload,
                "last_stage": _stage_summary(crypto_payload),
                "last_canary": (crypto_payload or {}).get("canary") if crypto_payload else None,
                "last_promotion": _promotion_summary(crypto_payload),
            }
            if normalized_domain == "crypto":
                payload["status"] = payload["crypto"]["status"]
                payload["checkpoint"] = crypto_payload
                payload["last_stage"] = payload["crypto"]["last_stage"]
                payload["last_canary"] = payload["crypto"]["last_canary"]
                payload["last_promotion"] = payload["crypto"]["last_promotion"]
        return payload

    async def maybe_emit_drift_alert(
        self,
        *,
        kalshi_env: str | None = None,
        no_promotion_threshold_days: int = 14,
        alert_cooldown_hours: int = 24,
    ) -> dict[str, Any] | None:
        """Emit a warning ops_event when active thresholds drift from settings
        defaults and the autonomous tuner has not made a promotion recently.

        Debounced via a checkpoint so at most one alert fires per ``alert_cooldown_hours``."""
        env = kalshi_env or self.settings.kalshi_env
        alert_checkpoint_name = f"autonomous_gate_drift_alert:{env}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            await self.agent_pack_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            active_thresholds = _threshold_values(self.agent_pack_service.runtime_thresholds(active_pack))
            settings_thresholds = _settings_threshold_values(self.settings)
            drift = _threshold_drift(active_thresholds, settings_thresholds)
            if not drift:
                return None
            gate_checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
            checkpoint_payload = gate_checkpoint.payload if gate_checkpoint is not None else {}
            last_promotion = _promotion_summary(checkpoint_payload)
            now = datetime.now(UTC)
            promoted_at = _as_utc((last_promotion or {}).get("promoted_at"))
            if promoted_at is not None and now - promoted_at < timedelta(days=no_promotion_threshold_days):
                return None
            alert_checkpoint = await repo.get_checkpoint(alert_checkpoint_name)
            alert_payload = alert_checkpoint.payload if alert_checkpoint is not None else {}
            last_alerted = _as_utc((alert_payload or {}).get("alerted_at"))
            if last_alerted is not None and now - last_alerted < timedelta(hours=alert_cooldown_hours):
                return None
            await repo.log_ops_event(
                severity="warning",
                summary=(
                    f"Gate threshold drift: {len(drift)} tunable field(s) diverge from"
                    " settings defaults with no recent autonomous promotion"
                ),
                source=OPS_SOURCE,
                payload={"drift": drift, "last_promotion": last_promotion, "fields": list(drift.keys())},
            )
            await repo.set_checkpoint(
                alert_checkpoint_name,
                None,
                {"alerted_at": now.isoformat(), "drift_fields": list(drift.keys())},
            )
            await session.commit()
        return {"alerted": True, "drift_fields": list(drift.keys())}

    async def run(
        self,
        *,
        kalshi_env: str | None = None,
        source: str | None = None,
        days: int | None = None,
        min_support: int | None = None,
        dry_run: bool = False,
        triggered_by: str = "manual",
        now: datetime | None = None,
        domain: str = "weather",
        policy_scope: str = "global",
        series_ticker: str | None = None,
        side: str | None = None,
        month: int | str | None = None,
        lane: str = "entry_gate",
        bootstrap_promote_from_historical: bool = False,
        crypto_assets: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_domain = _normalize_domain(domain)
        if normalized_domain == "crypto":
            return await self._run_crypto(
                kalshi_env=kalshi_env,
                days=days,
                min_support=min_support,
                dry_run=dry_run,
                triggered_by=triggered_by,
                now=now,
                asset_symbols=crypto_assets,
            )
        if normalized_domain == "all":
            weather = await self.run(
                kalshi_env=kalshi_env,
                source=source,
                days=days,
                min_support=min_support,
                dry_run=dry_run,
                triggered_by=triggered_by,
                now=now,
                domain="weather",
                policy_scope=policy_scope,
                series_ticker=series_ticker,
                side=side,
                month=month,
                lane=lane,
                bootstrap_promote_from_historical=bootstrap_promote_from_historical,
            )
            crypto = await self._run_crypto(
                kalshi_env=kalshi_env,
                days=days,
                min_support=min_support,
                dry_run=dry_run,
                triggered_by=triggered_by,
                now=now,
                asset_symbols=crypto_assets,
            )
            return {
                "status": _combined_status(weather, crypto),
                "kalshi_env": kalshi_env or self.settings.kalshi_env,
                "domain": "all",
                "weather": weather,
                "crypto": crypto,
            }
        env = kalshi_env or self.settings.kalshi_env
        run_source = source or self.settings.autonomous_gate_tuning_source
        run_days = int(days if days is not None else self.settings.autonomous_gate_tuning_days)
        support = int(min_support if min_support is not None else self.settings.autonomous_gate_tuning_min_support)
        now_utc = _as_utc(now) or datetime.now(UTC)
        weather_scope = _weather_scope_payload(
            policy_scope=policy_scope,
            series_ticker=series_ticker,
            side=side,
            month=month,
            lane=lane,
        )

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
            payload = dict(checkpoint.payload if checkpoint is not None else {})
            if payload.get("status") == "staged":
                if bootstrap_promote_from_historical:
                    result = await self._evaluate_historical_bootstrap(
                        repo,
                        checkpoint_payload=payload,
                        kalshi_env=env,
                        source=run_source,
                        days=run_days,
                        min_support=support,
                        dry_run=dry_run,
                        now=now_utc,
                    )
                else:
                    result = await self._evaluate_canary(
                        repo,
                        checkpoint_payload=payload,
                        kalshi_env=env,
                        dry_run=dry_run,
                        now=now_utc,
                    )
                await session.commit()
                return result
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            weather_resolution = self.agent_pack_service.resolve_weather_policy(
                pack,
                _weather_policy_context_from_scope(weather_scope),
            )
            runtime_thresholds = weather_resolution.thresholds
            bootstrap_sync = await self._sync_weather_bootstrap_shadow_from_decision_traces(
                repo,
                current_pack=pack,
                kalshi_env=env,
                since=now_utc - timedelta(days=max(1, run_days)),
                limit=5000,
                dry_run=dry_run,
                now=now_utc,
            )
            historical_sync = await self._sync_weather_bootstrap_historical_evidence(
                repo,
                current_pack=pack,
                kalshi_env=env,
                source=run_source,
                since=now_utc - timedelta(days=max(1, run_days)),
                dry_run=dry_run,
                now=now_utc,
            )
            bootstrap_result = await self._maybe_promote_weather_bootstrap_shadow(
                repo,
                current_pack=pack,
                weather_resolution=weather_resolution,
                kalshi_env=env,
                dry_run=dry_run,
                triggered_by=triggered_by,
                now=now_utc,
            )
            bootstrap_result["decision_trace_sync"] = bootstrap_sync
            bootstrap_result["historical_evidence_sync"] = historical_sync
            await session.commit()

        active_settings = _settings_with_thresholds(self.settings, runtime_thresholds)
        gate_service = self.gate_learning_service_factory(active_settings)
        recommendation = await gate_service.build_recommendation_report(
            kalshi_env=env,
            days=run_days,
            source=run_source,
            min_support=support,
            now=now_utc,
            policy_scope=policy_scope,
            series_ticker=series_ticker,
            side=side,
            month=month,
            lane=lane,
            episode_level=True,
        )
        candidate = _candidate_threshold_values(recommendation, runtime_thresholds, settings=self.settings)
        if not candidate["changes"]:
            return {
                "status": "no_candidate",
                "kalshi_env": env,
                "reason": "no_gate_changes_promoted",
                "weather_bootstrap": bootstrap_result,
                "recommendation": recommendation,
            }
        evidence_fingerprint = _evidence_fingerprint(
            recommendation=recommendation,
            current_thresholds=candidate["current_thresholds"],
            candidate_thresholds=candidate["candidate_thresholds"],
        )
        if evidence_fingerprint in set(payload.get("evidence_fingerprint_history") or []):
            return {
                "status": "duplicate_evidence",
                "kalshi_env": env,
                "evidence_fingerprint": evidence_fingerprint,
                "weather_bootstrap": bootstrap_result,
                "changes": candidate["changes"],
            }

        candidate_settings = self.settings.model_copy(update=candidate["candidate_thresholds"])
        validation = await self._validate_candidate(
            settings=candidate_settings,
            kalshi_env=env,
            days=run_days,
            now=now_utc,
        )
        if dry_run or not validation["passed"]:
            return {
                "status": "dry_run" if dry_run else "validation_failed",
                "kalshi_env": env,
                "dry_run": dry_run,
                "changes": candidate["changes"],
                "candidate_thresholds": candidate["candidate_thresholds"],
                "evidence_fingerprint": evidence_fingerprint,
                "weather_bootstrap": bootstrap_result,
                "recommendation": recommendation,
                "validation": validation,
            }

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            staged = await self._stage_candidate(
                repo,
                current_pack=pack,
                candidate_thresholds=candidate["candidate_thresholds"],
                current_thresholds=candidate["current_thresholds"],
                changes=candidate["changes"],
                recommendation=recommendation,
                validation=validation,
                evidence_fingerprint=evidence_fingerprint,
                kalshi_env=env,
                source=run_source,
                days=run_days,
                min_support=support,
                triggered_by=triggered_by,
                now=now_utc,
                history=list(payload.get("evidence_fingerprint_history") or []),
                weather_scope=weather_scope,
            )
            if bootstrap_promote_from_historical:
                checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
                staged_payload = dict(checkpoint.payload if checkpoint is not None else {})
                result = await self._evaluate_historical_bootstrap(
                    repo,
                    checkpoint_payload=staged_payload,
                    kalshi_env=env,
                    source=run_source,
                    days=run_days,
                    min_support=support,
                    dry_run=dry_run,
                    now=now_utc,
                )
                await session.commit()
                return result
            await session.commit()
        return staged

    async def _sync_weather_bootstrap_shadow_from_decision_traces(
        self,
        repo: PlatformRepository,
        *,
        current_pack: AgentPack,
        kalshi_env: str,
        since: datetime,
        limit: int,
        dry_run: bool,
        now: datetime,
    ) -> dict[str, Any]:
        """Backfill shadow bootstrap events from persisted decision traces."""
        page_size = max(1, min(int(limit), 1000))
        max_scan = max(int(limit), 50_000 if int(limit) >= 5000 else int(limit) * 10)
        historical_cache: dict[str, list[Any]] = {}
        event_cache: dict[str, list[WeatherBootstrapEventRecord]] = {}
        base_thresholds = self.agent_pack_service.runtime_thresholds(current_pack)
        service = WeatherEmpiricalBootstrapService()
        skipped: dict[str, int] = {}
        scanned = 0
        evaluated = 0
        matched = 0
        created = 0
        would_create = 0
        exhausted = False
        last_cursor: dict[str, str] | None = None

        def skip(reason: str) -> None:
            skipped[reason] = skipped.get(reason, 0) + 1

        cursor_time: datetime | None = None
        cursor_id: str | None = None
        while scanned < max_scan:
            stmt = select(DecisionTraceRecord).where(
                DecisionTraceRecord.kalshi_env == kalshi_env,
                DecisionTraceRecord.decision_time >= since,
            )
            if cursor_time is not None and cursor_id is not None:
                stmt = stmt.where(
                    or_(
                        DecisionTraceRecord.decision_time > cursor_time,
                        (DecisionTraceRecord.decision_time == cursor_time) & (DecisionTraceRecord.id > cursor_id),
                    )
                )
            records = list(
                (
                    await repo.session.execute(
                        stmt.order_by(DecisionTraceRecord.decision_time.asc(), DecisionTraceRecord.id.asc()).limit(
                            min(page_size, max_scan - scanned)
                        )
                    )
                )
                .scalars()
                .all()
            )
            if not records:
                exhausted = True
                break
            scanned += len(records)
            cursor_time = _as_utc(records[-1].decision_time) or records[-1].decision_time
            cursor_id = records[-1].id
            last_cursor = {"decision_time": cursor_time.isoformat() if cursor_time is not None else "", "id": cursor_id}

            trace_ids = [record.id for record in records]
            existing_stmt = select(WeatherBootstrapEventRecord.decision_trace_id).where(
                WeatherBootstrapEventRecord.kalshi_env == kalshi_env,
                WeatherBootstrapEventRecord.event_type == "decision",
                WeatherBootstrapEventRecord.decision_trace_id.in_(trace_ids),
            )
            existing_ids = {
                trace_id
                for trace_id in (await repo.session.execute(existing_stmt)).scalars().all()
                if trace_id is not None
            }

            for record in records:
                if record.id in existing_ids:
                    skip("already_synced")
                    continue
                trace = _dict(record.trace)
                signal = _dict(trace.get("signal"))
                eligibility = _dict(signal.get("eligibility"))
                candidate_trace = _dict(trace.get("candidate_trace"))
                selected = _selected_trace_candidate(candidate_trace)
                empirical = _dict(eligibility.get("empirical_gate") or candidate_trace.get("empirical_gate"))
                if _empirical_reason(empirical) != "empirical_gate_under_sampled":
                    skip("not_empirical_under_sampled")
                    continue
                original_reason = _trace_policy_original_reason(
                    trace=trace,
                    signal=signal,
                    eligibility=eligibility,
                    candidate_trace=candidate_trace,
                    selected=selected,
                )
                if original_reason == _UNKNOWN_ORIGINAL_GATE:
                    skip("unknown_original_gate")
                    continue
                if not _bootstrap_original_reason_allowed(original_reason):
                    skip("original_gate_not_bootstrap_bypassable")
                    continue
                bucket_key = empirical.get("bucket_key") or selected.get("bucket_id") or selected.get("bucket_key")
                if not bucket_key:
                    skip("missing_bucket_key")
                    continue

                legacy_bucket_key_text = str(bucket_key)
                bucket_key_text = coarse_empirical_bucket_key_from_legacy(legacy_bucket_key_text) or legacy_bucket_key_text
                side = _trace_side(signal, candidate_trace, selected)
                policy_resolution = self.agent_pack_service.resolve_weather_policy(
                    current_pack,
                    weather_policy_context_from_market(
                        market_ticker=record.market_ticker,
                        strategy_code="A",
                        side=side,
                        local_market_day=market_day_from_ticker(record.market_ticker),
                        trade_regime=str(signal.get("trade_regime") or "") or None,
                        lane="entry_gate",
                    ),
                    fallback_thresholds=base_thresholds,
                )
                bootstrap_policy = (
                    policy_resolution.policy.bootstrap
                    if policy_resolution.policy is not None and policy_resolution.policy.bootstrap is not None
                    else None
                )
                if bootstrap_policy is None:
                    skip("bootstrap_policy_missing")
                    continue

                weather_payload = _dict(signal.get("weather") or trace.get("weather"))
                provenance = _dict(
                    signal.get("prediction_provenance")
                    or candidate_trace.get("prediction_provenance")
                    or weather_payload.get("prediction_provenance")
                )
                fair_yes = _decimal_or_none(signal.get("fair_yes_dollars") or trace.get("fair_yes_dollars"))
                stale = stale_signal_evidence_from_trace(candidate_trace)
                if bucket_key_text not in event_cache:
                    event_cache[bucket_key_text] = await repo.list_weather_bootstrap_events(
                        kalshi_env=kalshi_env,
                        series_ticker=series_from_ticker(record.market_ticker),
                        since=since,
                        limit=2000,
                    )
                if bucket_key_text not in historical_cache:
                    historical_cache[bucket_key_text] = await repo.list_weather_bootstrap_historical_evidence(
                        kalshi_env=kalshi_env,
                        series_ticker=series_from_ticker(record.market_ticker),
                        strict_replay=True,
                        limit=2000,
                    )
                context = WeatherEmpiricalBootstrapContext(
                    kalshi_env=kalshi_env,
                    market_ticker=record.market_ticker,
                    side=side,
                    confidence=_float_or_none(signal.get("confidence")),
                    edge_bps_after_buffer=(
                        _int_or_none(eligibility.get("edge_after_quality_buffer_bps"))
                        or _int_or_none(selected.get("quality_adjusted_edge_bps"))
                        or _int_or_none(signal.get("edge_bps"))
                    ),
                    fair_yes_dollars=fair_yes,
                    fair_value_source=fair_value_source_from_provenance(provenance, fair_yes_dollars=fair_yes),
                    confidence_source=confidence_source_from_trace(candidate_trace, provenance),
                    bucket_key=bucket_key_text,
                    legacy_bucket_key=legacy_bucket_key_text if legacy_bucket_key_text != bucket_key_text else None,
                    actual_sample_count=int(empirical.get("actual_sample_count") or 0),
                    actual_net_pnl=_decimal_or_none(empirical.get("actual_net_pnl")),
                    current_stand_down_reason=_str_or_none(
                        eligibility.get("stand_down_reason") or signal.get("stand_down_reason")
                    ),
                    pre_empirical_stand_down_reason=original_reason,
                    policy_key=policy_resolution.policy_key,
                    fallback_policy_key=policy_resolution.fallback_policy_key_used,
                    data_stale=bool(
                        eligibility.get("market_stale")
                        or eligibility.get("research_stale")
                        or stale.get("stale")
                    ),
                    source_stale_reasons=tuple(stale.get("reason_codes") or ()),
                    room_id=record.room_id,
                    policy_pack_version=current_pack.version,
                )
                decision = service.evaluate(
                    context=context,
                    policy=bootstrap_policy,
                    recent_events=event_cache[bucket_key_text],
                    historical_evidence=historical_cache[bucket_key_text],
                    now=_as_utc(record.decision_time) or now,
                )
                event_trace = decision.to_trace()
                event_trace["bucket_id"] = bucket_key_text
                event_trace["legacy_bucket_id"] = legacy_bucket_key_text
                event_trace["original_stand_down_reason"] = original_reason
                event_trace["empirical_reason"] = "empirical_gate_under_sampled"
                event_trace["stale_signal_evidence"] = stale
                event_trace["source_decision_trace_id"] = record.id
                evaluated += 1
                if event_trace.get("matched") or event_trace.get("would_have_entered"):
                    matched += 1
                if dry_run:
                    would_create += 1
                    continue
                event = await repo.save_weather_bootstrap_event(
                    kalshi_env=kalshi_env,
                    market_ticker=record.market_ticker,
                    series_ticker=series_from_ticker(record.market_ticker),
                    local_market_day=market_day_from_ticker(record.market_ticker),
                    bucket_key=bucket_key_text,
                    policy_key=event_trace.get("policy_key"),
                    tier=event_trace.get("tier"),
                    event_type="decision",
                    status=event_trace.get("outcome") or "block",
                    side=side,
                    confidence=context.confidence,
                    edge_bps=event_trace.get("edge_bps_after_buffer"),
                    size_factor=event_trace.get("size_factor"),
                    source="decision_trace_backfill",
                    occurred_at=_as_utc(record.decision_time) or now,
                    room_id=record.room_id,
                    decision_trace_id=record.id,
                    payload={
                        **event_trace,
                        "legacy_bucket_key": legacy_bucket_key_text,
                        "fair_value_source": context.fair_value_source,
                        "data_stale": context.data_stale,
                    },
                )
                event_cache[bucket_key_text].append(event)
                existing_ids.add(record.id)
                created += 1

        return {
            "status": "empty" if scanned == 0 else ("dry_run" if dry_run else ("synced" if created else "no_new_events")),
            "scanned": scanned,
            "evaluated": evaluated,
            "matched_evaluations": matched,
            "created": created,
            "would_create": would_create,
            "exhausted": exhausted,
            "last_cursor": last_cursor,
            "skipped": skipped,
        }

    async def _sync_weather_bootstrap_historical_evidence(
        self,
        repo: PlatformRepository,
        *,
        current_pack: AgentPack,
        kalshi_env: str,
        source: str,
        since: datetime,
        dry_run: bool,
        now: datetime,
    ) -> dict[str, Any]:
        """Persist strict replay-equivalent bundle rows as bootstrap historical evidence."""
        gate_service = self.gate_learning_service_factory(self.settings)
        rows = [
            row
            for row in gate_service.load_bundle_rows(source=source)
            if row.decision_time is None or row.decision_time >= since
        ]
        if not rows:
            return {
                "status": "empty",
                "source": source,
                "scanned": 0,
                "evaluated": 0,
                "matched_evaluations": 0,
                "created": 0,
                "would_create": 0,
                "skipped": {},
            }

        source_files = gate_service.source_files(source=source)
        base_thresholds = self.agent_pack_service.runtime_thresholds(current_pack)
        service = WeatherEmpiricalBootstrapService()
        skipped: dict[str, int] = {}
        evaluated = 0
        matched = 0
        created = 0
        would_create = 0

        def skip(reason: str) -> None:
            skipped[reason] = skipped.get(reason, 0) + 1

        for row in rows:
            if not row.labeled:
                skip("unlabeled")
                continue
            if row.current_gate_passed:
                skip("current_gate_already_passed")
                continue
            if str(row.evidence_quality or "").lower() != "executable":
                skip("non_executable_evidence")
                continue
            if row.data_stale or row.stale_data_mismatch:
                skip("stale_data")
                continue
            fair_value_source = _normalize_fair_value_source(row.fair_value_source)
            if fair_value_source in {None, "fallback", "unavailable", "dark", "none"}:
                skip("fair_value_source_disqualified")
                continue
            original_reason = _historical_row_original_reason(row)
            if original_reason == _UNKNOWN_ORIGINAL_GATE:
                skip("unknown_original_gate")
                continue
            if not _bootstrap_original_reason_allowed(original_reason):
                skip("original_gate_not_bootstrap_bypassable")
                continue
            if not row.bucket_key:
                skip("missing_bucket_key")
                continue
            if row.side not in {"yes", "no"}:
                skip("missing_side")
                continue

            policy_resolution = self.agent_pack_service.resolve_weather_policy(
                current_pack,
                weather_policy_context_from_market(
                    market_ticker=row.market_ticker,
                    strategy_code=row.strategy_code or "A",
                    side=row.side,
                    local_market_day=row.market_day or market_day_from_ticker(row.market_ticker),
                    trade_regime=row.trade_regime,
                    lane="entry_gate",
                ),
                fallback_thresholds=base_thresholds,
            )
            bootstrap_policy = (
                policy_resolution.policy.bootstrap
                if policy_resolution.policy is not None and policy_resolution.policy.bootstrap is not None
                else None
            )
            if bootstrap_policy is None:
                skip("bootstrap_policy_missing")
                continue

            legacy_bucket_key = str(row.bucket_key)
            bucket_key = coarse_empirical_bucket_key_from_legacy(legacy_bucket_key) or legacy_bucket_key
            context = WeatherEmpiricalBootstrapContext(
                kalshi_env=kalshi_env,
                market_ticker=row.market_ticker,
                side=row.side,
                confidence=row.confidence,
                edge_bps_after_buffer=row.quality_adjusted_edge_bps or row.edge_bps,
                fair_yes_dollars=row.fair_yes_dollars,
                fair_value_source=fair_value_source,
                confidence_source=row.confidence_source or "raw",
                bucket_key=bucket_key,
                legacy_bucket_key=legacy_bucket_key if legacy_bucket_key != bucket_key else None,
                actual_sample_count=0,
                actual_net_pnl=None,
                current_stand_down_reason="empirical_gate_block"
                if original_reason is None
                else original_reason,
                pre_empirical_stand_down_reason=original_reason,
                policy_key=policy_resolution.policy_key,
                fallback_policy_key=policy_resolution.fallback_policy_key_used,
                data_stale=False,
                source_stale_reasons=(),
                room_id=row.room_id,
                policy_pack_version=current_pack.version,
            )
            decision = service.evaluate(
                context=context,
                policy=bootstrap_policy,
                recent_events=(),
                historical_evidence=(),
                now=row.decision_time or now,
            )
            evaluated += 1
            if not decision.matched:
                skip(f"bootstrap_not_matched:{decision.reason}")
                continue
            matched += 1
            fingerprint = _historical_bootstrap_source_fingerprint(
                row=row,
                source_files=source_files,
                policy_key=policy_resolution.policy_key,
                decision_trace=decision.to_trace(),
            )
            existing_stmt = select(WeatherBootstrapHistoricalEvidenceRecord.id).where(
                WeatherBootstrapHistoricalEvidenceRecord.kalshi_env == kalshi_env,
                WeatherBootstrapHistoricalEvidenceRecord.source_fingerprint == fingerprint,
                WeatherBootstrapHistoricalEvidenceRecord.market_ticker == row.market_ticker,
                WeatherBootstrapHistoricalEvidenceRecord.bucket_key == bucket_key,
                WeatherBootstrapHistoricalEvidenceRecord.policy_key == policy_resolution.policy_key,
            )
            existing_id = (await repo.session.execute(existing_stmt)).scalar_one_or_none()
            if existing_id is not None:
                skip("already_synced")
                continue
            if dry_run:
                would_create += 1
                continue
            trace_payload = decision.to_trace()
            trace_payload["source_files"] = source_files
            trace_payload["primary_block_reason"] = row.primary_block_reason
            trace_payload["legacy_bucket_key"] = legacy_bucket_key
            trace_payload["fair_value_source"] = fair_value_source
            await repo.save_weather_bootstrap_historical_evidence(
                kalshi_env=kalshi_env,
                market_ticker=row.market_ticker,
                series_ticker=row.series_ticker or series_from_ticker(row.market_ticker),
                local_market_day=row.market_day or market_day_from_ticker(row.market_ticker),
                bucket_key=bucket_key,
                policy_key=policy_resolution.policy_key,
                tier=decision.tier,
                replay_version="gate-learning-strict-bootstrap-v1",
                source_fingerprint=fingerprint,
                strict_replay=True,
                side=row.side,
                confidence=row.confidence,
                edge_bps=decision.edge_bps_after_buffer,
                count_fp=Decimal("1.00"),
                pnl_dollars=row.counterfactual_pnl_dollars,
                evidence_weight=float(STRICT_REPLAY_SHADOW_WEIGHT),
                outcome=_historical_outcome(row.counterfactual_pnl_dollars),
                observed_at=row.settlement_ts or row.decision_time or now,
                payload=trace_payload,
            )
            created += 1

        return {
            "status": "dry_run" if dry_run else ("synced" if created else "no_new_evidence"),
            "source": source,
            "scanned": len(rows),
            "evaluated": evaluated,
            "matched_evaluations": matched,
            "created": created,
            "would_create": would_create,
            "skipped": skipped,
            "source_files": source_files,
        }

    async def _maybe_promote_weather_bootstrap_shadow(
        self,
        repo: PlatformRepository,
        *,
        current_pack: AgentPack,
        weather_resolution: Any,
        kalshi_env: str,
        dry_run: bool,
        triggered_by: str,
        now: datetime,
    ) -> dict[str, Any]:
        policy: AgentPackWeatherPolicy | None = getattr(weather_resolution, "policy", None)
        if policy is None:
            return {"status": "no_policy", "reason": "weather_bootstrap_policy_not_resolved"}
        bootstrap = policy.bootstrap
        events = await repo.list_weather_bootstrap_events(
            kalshi_env=kalshi_env,
            since=now - timedelta(days=7),
            limit=5000,
        )
        historical_evidence = await repo.list_weather_bootstrap_historical_evidence(
            kalshi_env=kalshi_env,
            strict_replay=True,
            limit=10_000,
        )
        shadow_gate = WeatherEmpiricalBootstrapService().shadow_gate_status(
            policy=bootstrap,
            events=events,
            policy_key=weather_resolution.policy_key,
            now=now,
        ).to_payload()
        historical_gate = _historical_bootstrap_shadow_gate_status(
            policy=bootstrap,
            evidence=historical_evidence,
            policy_key=weather_resolution.policy_key,
            now=now,
        )
        if bootstrap.rollout_state != "shadow":
            return {
                "status": "no_action",
                "reason": "bootstrap_not_in_shadow_rollout",
                "rollout_state": bootstrap.rollout_state,
                "shadow_gate": shadow_gate,
                "historical_gate": historical_gate,
            }
        if shadow_gate["eligible_for_promotion"]:
            evidence_source = "shadow_decision_events"
            evidence_gate = shadow_gate
        elif historical_gate["eligible_for_promotion"]:
            evidence_source = "strict_historical_startup"
            evidence_gate = historical_gate
        else:
            return {
                "status": "pending",
                "reason": "shadow_evidence_gate_not_met",
                "shadow_gate": shadow_gate,
                "historical_gate": historical_gate,
            }

        valid_until = now + timedelta(days=7)
        cold = bootstrap.tiers["cold"].model_copy(update={"live_enabled": True})
        promoted_bootstrap = bootstrap.model_copy(
            deep=True,
            update={
                "rollout_state": "promoted_low",
                "evidence_ready": True,
                "tiers": {**bootstrap.tiers, "cold": cold},
                "promotion": {
                    **dict(bootstrap.promotion or {}),
                    "cold_tier_promoted_at": now.isoformat(),
                    "triggered_by": triggered_by,
                    "shadow_gate": shadow_gate,
                    "historical_gate": historical_gate,
                    "evidence_source": evidence_source,
                    "valid_until": valid_until.isoformat(),
                },
            },
        )
        promoted_policy = policy.model_copy(
            deep=True,
            update={
                "bootstrap": promoted_bootstrap,
                "reason_codes": list(dict.fromkeys([*policy.reason_codes, "weather_bootstrap_cold_promoted"])),
                "valid_until": valid_until,
                "deterministic_summary": (
                    "Weather empirical bootstrap cold tier promoted from shadow evidence. "
                    "Cold tier is live at the initial $100/day, 1-position cap; other tiers remain shadow-only."
                ),
            },
        )
        fingerprint = _bootstrap_evidence_fingerprint(
            pack_version=current_pack.version,
            policy_key=policy.policy_key,
            evidence_gate=evidence_gate,
            evidence_source=evidence_source,
            target_rollout="promoted_low",
        )
        version = f"weather-bootstrap-cold-{fingerprint[:12]}"
        if dry_run:
            return {
                "status": "would_promote",
                "dry_run": True,
                "candidate_version": version,
                "evidence_fingerprint": fingerprint,
                "shadow_gate": shadow_gate,
                "historical_gate": historical_gate,
                "evidence_source": evidence_source,
            }

        book = current_pack.weather_policy.model_copy(deep=True) if current_pack.weather_policy is not None else None
        if book is None:
            return {"status": "failed", "reason": "weather_policy_book_missing", "shadow_gate": shadow_gate}
        book.policies[policy.policy_key] = promoted_policy
        candidate_pack = current_pack.model_copy(
            deep=True,
            update={
                "version": version,
                "status": "champion",
                "parent_version": current_pack.version,
                "source": OPS_SOURCE,
                "description": "Autonomous weather empirical bootstrap cold-tier promotion from deterministic shadow evidence.",
                "weather_policy": book,
                "metadata": {
                    **dict(current_pack.metadata or {}),
                    "weather_empirical_bootstrap": {
                        "promoted_at": now.isoformat(),
                        "triggered_by": triggered_by,
                        "kalshi_env": kalshi_env,
                        "policy_key": policy.policy_key,
                        "evidence_fingerprint": fingerprint,
                        "evidence_source": evidence_source,
                        "shadow_gate": shadow_gate,
                        "historical_gate": historical_gate,
                        "valid_until": valid_until.isoformat(),
                    },
                },
            },
        )
        candidate_pack = self.agent_pack_service.sanitize_candidate_pack(candidate_pack, parent_version=current_pack.version)
        await repo.update_agent_pack(candidate_pack)
        promotion = await repo.create_promotion_event(
            candidate_version=version,
            previous_version=current_pack.version,
            target_color=self.settings.app_color,
            evaluation_run_id=None,
            payload={
                "kind": "weather_empirical_bootstrap_cold_promotion",
                "policy_key": policy.policy_key,
                "evidence_fingerprint": fingerprint,
                "evidence_source": evidence_source,
                "shadow_gate": shadow_gate,
                "historical_gate": historical_gate,
            },
            status="promoted",
        )
        await self.agent_pack_service.assign_pack_to_color(
            repo,
            color=self.settings.app_color,
            version=version,
        )
        return {
            "status": "promoted",
            "candidate_version": version,
            "promotion_event_id": promotion.id,
            "evidence_fingerprint": fingerprint,
            "evidence_source": evidence_source,
            "shadow_gate": shadow_gate,
            "historical_gate": historical_gate,
        }

    async def _run_crypto(
        self,
        *,
        kalshi_env: str | None,
        days: int | None,
        min_support: int | None,
        dry_run: bool,
        triggered_by: str,
        now: datetime | None,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        requested_assets = normalize_asset_symbols(asset_symbols)
        run_days = int(days if days is not None else self.settings.autonomous_gate_tuning_days)
        support = int(min_support if min_support is not None else self.settings.autonomous_gate_tuning_min_support)
        now_utc = _as_utc(now) or datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            checkpoint = await repo.get_checkpoint(_checkpoint_name(env, domain="crypto"))
            payload = dict(checkpoint.payload if checkpoint is not None else {})
            if payload.get("status") == "staged":
                result = await self._evaluate_crypto_canary(
                    repo,
                    checkpoint_payload=payload,
                    kalshi_env=env,
                    dry_run=dry_run,
                    now=now_utc,
                )
                await session.commit()
                return result
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(pack)
            recommendation = await self._build_crypto_recommendation(
                repo,
                crypto_policy=crypto_policy,
                kalshi_env=env,
                days=run_days,
                min_support=support,
                now=now_utc,
                asset_symbols=requested_assets,
            )
            if recommendation.get("validation_short_circuit"):
                validation = recommendation.get("validation") or {
                    "passed": False,
                    "failures": ["crypto_validation_short_circuit"],
                    "row_counts": recommendation.get("row_counts") or {},
                    "artifact_checks": recommendation.get("artifact_checks") or {},
                    "data_gap_reason": recommendation.get("data_gap_reason"),
                }
                await session.commit()
                return {
                    "status": "validation_failed",
                    "kalshi_env": env,
                    "domain": "crypto",
                    "dry_run": dry_run,
                    "changes": {},
                    "recommendation": recommendation,
                    "validation": validation,
                }
            candidate = _candidate_crypto_policy_values(recommendation, pack)
            if not candidate["changes"]:
                await session.commit()
                return {
                    "status": "no_candidate",
                    "kalshi_env": env,
                    "domain": "crypto",
                    "reason": recommendation.get("data_gap_reason") or "no_crypto_asset_gate_changes_promoted",
                    "data_gap_reason": recommendation.get("data_gap_reason"),
                    "recommendation": recommendation,
                }
            evidence_fingerprint = _crypto_evidence_fingerprint(
                recommendation=recommendation,
                current_policy=candidate["current_crypto_policy"],
                candidate_policy=candidate["candidate_crypto_policy"],
            )
            if evidence_fingerprint in set(payload.get("evidence_fingerprint_history") or []):
                await session.commit()
                return {
                    "status": "duplicate_evidence",
                    "kalshi_env": env,
                    "domain": "crypto",
                    "evidence_fingerprint": evidence_fingerprint,
                    "changes": candidate["changes"],
                }
            artifact_checks = recommendation.get("artifact_checks") or {}
            validation_failures: list[str] = []
            if recommendation["row_counts"]["labeled_rows"] <= 0:
                validation_failures.append("crypto_zero_labeled_rows")
            if recommendation.get("data_gap_reason"):
                validation_failures.append(str(recommendation["data_gap_reason"]))
            if artifact_checks.get("model_status") != "trained":
                validation_failures.append("crypto_model_not_trained")
            if artifact_checks.get("backtest_status") in {None, "missing"}:
                validation_failures.append("crypto_backtest_missing")
            if artifact_checks.get("replay_gate_status") != "passed":
                validation_failures.append("crypto_replay_gate_not_passed")
            validation = {
                "passed": not validation_failures,
                "failures": validation_failures,
                "row_counts": recommendation["row_counts"],
                "artifact_checks": artifact_checks,
                "data_gap_reason": recommendation.get("data_gap_reason"),
            }
            if dry_run or not validation["passed"]:
                await session.commit()
                return {
                    "status": "dry_run" if dry_run else "validation_failed",
                    "kalshi_env": env,
                    "domain": "crypto",
                    "dry_run": dry_run,
                    "changes": candidate["changes"],
                    "candidate_crypto_policy": candidate["candidate_crypto_policy"],
                    "evidence_fingerprint": evidence_fingerprint,
                    "recommendation": recommendation,
                    "validation": validation,
                }
            staged = await self._stage_crypto_candidate(
                repo,
                current_pack=pack,
                current_crypto_policy=candidate["current_crypto_policy"],
                candidate_crypto_policy=candidate["candidate_crypto_policy"],
                changes=candidate["changes"],
                recommendation=recommendation,
                validation=validation,
                evidence_fingerprint=evidence_fingerprint,
                kalshi_env=env,
                days=run_days,
                min_support=support,
                triggered_by=triggered_by,
                now=now_utc,
                history=list(payload.get("evidence_fingerprint_history") or []),
            )
            await session.commit()
            return staged

    async def _build_crypto_recommendation(
        self,
        repo: PlatformRepository,
        *,
        crypto_policy: Any,
        kalshi_env: str,
        days: int,
        min_support: int,
        now: datetime,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        cutoff = now - timedelta(days=days)
        requested_assets = normalize_asset_symbols(asset_symbols)
        model = await repo.get_latest_crypto_model_artifact(
            frequency="15m",
            artifact_type=_crypto_artifact_type("model", requested_assets),
            kalshi_env=kalshi_env,
        )
        backtest = await repo.get_latest_crypto_model_artifact(
            frequency="15m",
            artifact_type=_crypto_artifact_type("backtest", requested_assets),
            kalshi_env=kalshi_env,
        )
        replay_gate = await repo.get_latest_crypto_model_artifact(
            frequency="15m",
            artifact_type=_crypto_artifact_type("replay_gate", requested_assets),
            kalshi_env=kalshi_env,
        )
        artifact_checks = {
            "model_status": _crypto_artifact_status(model),
            "model_version": getattr(model, "version", None),
            "backtest_status": _crypto_artifact_status(backtest),
            "backtest_version": getattr(backtest, "version", None),
            "replay_gate_status": _crypto_artifact_status(replay_gate),
            "replay_gate_version": getattr(replay_gate, "version", None),
        }
        validation_failures: list[str] = []
        if artifact_checks["model_status"] != "trained":
            validation_failures.append("crypto_model_not_trained")
        if artifact_checks["backtest_status"] in {None, "missing"}:
            validation_failures.append("crypto_backtest_missing")
        if artifact_checks["replay_gate_status"] != "passed":
            validation_failures.append("crypto_replay_gate_not_passed")
        if validation_failures:
            metrics = (
                dict(getattr(replay_gate, "metrics", None) or {})
                or dict(getattr(backtest, "metrics", None) or {})
                or dict(getattr(model, "metrics", None) or {})
            )
            if metrics:
                backtest_payload = dict(getattr(backtest, "payload", None) or {})
                dataset = backtest_payload.get("dataset") if isinstance(backtest_payload.get("dataset"), dict) else None
                row_counts = _crypto_row_counts_from_artifact_metrics(metrics, dataset=dataset)
                data_gap_reason = _crypto_data_gap_reason(**{key: int(row_counts[key]) for key in (
                    "snapshot_rows",
                    "candle_rows",
                    "spot_rows",
                    "decision_rows",
                    "labeled_rows",
                    "settled_snapshot_rows",
                )})
                if data_gap_reason is None:
                    return _crypto_validation_short_circuit_recommendation(
                        kalshi_env=kalshi_env,
                        days=days,
                        min_support=min_support,
                        row_counts=row_counts,
                        artifact_checks=artifact_checks,
                        validation_failures=validation_failures,
                        crypto_policy=crypto_policy,
                        asset_symbols=requested_assets,
                    )
            else:
                probe_snapshots = await repo.list_crypto_market_snapshots(
                    frequency="15m",
                    kalshi_env=kalshi_env,
                    since=cutoff,
                    limit=500,
                )
                probe_candles = await repo.list_crypto_market_candlesticks(
                    frequency="15m",
                    kalshi_env=kalshi_env,
                    since=cutoff,
                    limit=500,
                )
                probe_spot_rows = await repo.list_crypto_spot_ohlc(
                    frequency="15m",
                    kalshi_env=kalshi_env,
                    since=cutoff,
                    limit=500,
                )
                probe_snapshots = _filter_crypto_snapshot_rows(probe_snapshots, requested_assets)
                probe_candles = _filter_crypto_snapshot_rows(probe_candles, requested_assets)
                probe_spot_rows = _filter_crypto_snapshot_rows(probe_spot_rows, requested_assets)
                row_counts = {
                    "snapshot_rows": len(probe_snapshots),
                    "candle_rows": len(probe_candles),
                    "spot_rows": len(probe_spot_rows),
                    "settled_snapshot_rows": sum(1 for row in probe_snapshots if row.settlement_result in {"yes", "no"}),
                    "decision_rows": 0,
                    "labeled_rows": 0,
                    "assets": [],
                }
                data_gap_reason = _crypto_data_gap_reason(
                    snapshot_rows=row_counts["snapshot_rows"],
                    candle_rows=row_counts["candle_rows"],
                    spot_rows=row_counts["spot_rows"],
                    decision_rows=row_counts["decision_rows"],
                    labeled_rows=row_counts["labeled_rows"],
                    settled_snapshot_rows=row_counts["settled_snapshot_rows"],
                )
                if data_gap_reason is None:
                    return _crypto_validation_short_circuit_recommendation(
                        kalshi_env=kalshi_env,
                        days=days,
                        min_support=min_support,
                        row_counts=row_counts,
                        artifact_checks=artifact_checks,
                        validation_failures=validation_failures,
                        crypto_policy=crypto_policy,
                        asset_symbols=requested_assets,
                    )
                return {
                    "schema_version": "crypto-autonomous-gate-recommendations-v1",
                    "kalshi_env": kalshi_env,
                    "domain": "crypto",
                    "window_days": days,
                    "min_support": min_support,
                    "row_counts": row_counts,
                    "data_gap_reason": data_gap_reason,
                    "artifact_checks": artifact_checks,
                    "current_policy": _crypto_policy_values(crypto_policy),
                    "asset_diagnostics": [],
                    "promoted_assets": {},
                    "asset_symbols": requested_assets,
                }
        snapshots = await repo.list_crypto_market_snapshots(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=cutoff,
            limit=100_000,
        )
        candles = await repo.list_crypto_market_candlesticks(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=cutoff,
            limit=200_000,
        )
        spot_rows = await repo.list_crypto_spot_ohlc(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=cutoff,
            limit=500_000,
        )
        snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
        candles = _filter_crypto_snapshot_rows(candles, requested_assets)
        spot_rows = _filter_crypto_snapshot_rows(spot_rows, requested_assets)
        rows = _crypto_decision_rows(snapshots, candles, spot_rows)
        labeled = [row for row in rows if row.get("label_yes") in {0, 1}]
        assets = sorted({normalize_asset_symbol(str(row.get("asset_symbol") or "")) for row in labeled})
        settled_snapshot_rows = sum(1 for row in snapshots if row.settlement_result in {"yes", "no"})
        row_counts = {
            "snapshot_rows": len(snapshots),
            "candle_rows": len(candles),
            "spot_rows": len(spot_rows),
            "settled_snapshot_rows": settled_snapshot_rows,
            "decision_rows": len(rows),
            "labeled_rows": len(labeled),
            "assets": assets,
        }
        data_gap_reason = _crypto_data_gap_reason(
            snapshot_rows=len(snapshots),
            candle_rows=len(candles),
            spot_rows=len(spot_rows),
            decision_rows=len(rows),
            labeled_rows=len(labeled),
            settled_snapshot_rows=settled_snapshot_rows,
        )
        scored_labeled = _crypto_rows_with_policy_predictions(labeled, settings=self.settings)
        current_scores: dict[str, Any] = {}
        promoted: dict[str, Any] = {}
        diagnostics: list[dict[str, Any]] = []
        for asset in assets:
            asset_rows = [row for row in scored_labeled if normalize_asset_symbol(str(row.get("asset_symbol") or "")) == asset]
            current_score = _score_crypto_policy_rows(
                asset_rows,
                settings=self.settings,
                crypto_policy=crypto_policy,
                asset_symbol=asset,
            )
            current_scores[asset] = current_score
            best = _best_crypto_asset_candidate(
                asset_rows,
                settings=self.settings,
                crypto_policy=crypto_policy,
                asset_symbol=asset,
                min_support=min_support,
                current_score=current_score,
            )
            diagnostics.append(
                {
                    "asset_symbol": asset,
                    "row_count": len(asset_rows),
                    "current": _json_crypto_score(current_score),
                    "best_candidate": best,
                }
            )
            if best is not None and best["promotion_status"] == "promoted":
                promoted[asset] = best
        return {
            "schema_version": "crypto-autonomous-gate-recommendations-v1",
            "kalshi_env": kalshi_env,
            "domain": "crypto",
            "window_days": days,
            "min_support": min_support,
            "asset_symbols": requested_assets,
            "row_counts": row_counts,
            "data_gap_reason": data_gap_reason,
            "artifact_checks": artifact_checks,
            "current_policy": _crypto_policy_values(crypto_policy),
            "asset_diagnostics": diagnostics,
            "promoted_assets": promoted,
        }

    async def _stage_crypto_candidate(
        self,
        repo: PlatformRepository,
        *,
        current_pack: AgentPack,
        current_crypto_policy: dict[str, Any],
        candidate_crypto_policy: dict[str, Any],
        changes: dict[str, Any],
        recommendation: dict[str, Any],
        validation: dict[str, Any],
        evidence_fingerprint: str,
        kalshi_env: str,
        days: int,
        min_support: int,
        triggered_by: str,
        now: datetime,
        history: list[str],
    ) -> dict[str, Any]:
        version = f"crypto-gate-tuning-{now.strftime('%Y%m%dT%H%M%SZ')}"
        candidate_pack = current_pack.model_copy(
            update={
                "version": version,
                "status": "staged",
                "parent_version": current_pack.version,
                "source": OPS_SOURCE,
                "description": "Autonomous crypto gate tuning candidate from deterministic point-in-time replay evidence.",
                "crypto_policy": AgentPackCryptoPolicy.model_validate(candidate_crypto_policy),
                "metadata": {
                    **dict(current_pack.metadata or {}),
                    "autonomous_crypto_gate_tuning": {
                        "staged_at": now.isoformat(),
                        "triggered_by": triggered_by,
                        "kalshi_env": kalshi_env,
                        "days": days,
                        "min_support": min_support,
                        "evidence_fingerprint": evidence_fingerprint,
                        "changes": changes,
                    },
                },
            }
        )
        candidate_pack = self.agent_pack_service.sanitize_candidate_pack(candidate_pack, parent_version=current_pack.version)
        await repo.update_agent_pack(candidate_pack)
        promotion = await repo.create_promotion_event(
            candidate_version=version,
            previous_version=current_pack.version,
            target_color=self.settings.app_color,
            evaluation_run_id=None,
            payload={
                "kind": "autonomous_crypto_gate_tuning",
                "evidence_fingerprint": evidence_fingerprint,
                "current_crypto_policy": current_crypto_policy,
                "candidate_crypto_policy": candidate_crypto_policy,
                "changes": changes,
                "recommendation_summary": {
                    "row_counts": recommendation.get("row_counts"),
                    "promoted_assets": sorted(changes),
                },
                "validation": validation,
            },
            status="staged",
        )
        control = await repo.get_deployment_control()
        notes = self.agent_pack_service._notes(control)
        notes["autonomous_crypto_gate_tuning"] = {
            "status": "staged",
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "evidence_fingerprint": evidence_fingerprint,
            "staged_at": now.isoformat(),
            "changes": changes,
        }
        await repo.update_deployment_notes(self.agent_pack_service._replace_notes(control.notes, notes))
        history = list(dict.fromkeys([*history, evidence_fingerprint]))
        checkpoint_payload = {
            "status": "staged",
            "kalshi_env": kalshi_env,
            "domain": "crypto",
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "staged_at": now.isoformat(),
            "days": days,
            "min_support": min_support,
            "evidence_fingerprint": evidence_fingerprint,
            "evidence_fingerprint_history": history[-20:],
            "current_crypto_policy": current_crypto_policy,
            "candidate_crypto_policy": candidate_crypto_policy,
            "changes": changes,
            "recommendation_summary": {
                "row_counts": recommendation.get("row_counts"),
                "promoted_assets": sorted(changes),
            },
        }
        await repo.set_checkpoint(_checkpoint_name(kalshi_env, domain="crypto"), cursor=None, payload=checkpoint_payload)
        await repo.log_ops_event(
            severity="info",
            summary=f"Autonomous crypto gate tuning staged {version} for {len(changes)} asset(s)",
            source=OPS_SOURCE,
            payload=checkpoint_payload,
            kalshi_env=kalshi_env,
        )
        return {
            "status": "staged",
            "kalshi_env": kalshi_env,
            "domain": "crypto",
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "changes": changes,
            "evidence_fingerprint": evidence_fingerprint,
            "validation": validation,
        }

    async def _evaluate_crypto_canary(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        dry_run: bool,
        now: datetime,
    ) -> dict[str, Any]:
        staged_at = _as_utc(checkpoint_payload.get("staged_at")) or now
        snapshots = await repo.list_crypto_market_snapshots(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=staged_at,
            limit=100_000,
        )
        candles = await repo.list_crypto_market_candlesticks(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=staged_at - timedelta(hours=6),
            limit=200_000,
        )
        spot_rows = await repo.list_crypto_spot_ohlc(
            frequency="15m",
            kalshi_env=kalshi_env,
            since=staged_at - timedelta(hours=6),
            limit=500_000,
        )
        rows = [
            row
            for row in _crypto_decision_rows(snapshots, candles, spot_rows)
            if _as_utc(row.get("settlement_ts")) is not None and (_as_utc(row.get("settlement_ts")) or now) > staged_at
        ]
        changed_assets = set((checkpoint_payload.get("changes") or {}).keys())
        if changed_assets:
            rows = [
                row
                for row in rows
                if normalize_asset_symbol(str(row.get("asset_symbol") or "")) in changed_assets
            ]
        current_policy = _runtime_crypto_policy_from_payload(
            checkpoint_payload.get("current_crypto_policy") or {},
            service=self.agent_pack_service,
        )
        candidate_policy = _runtime_crypto_policy_from_payload(
            checkpoint_payload.get("candidate_crypto_policy") or {},
            service=self.agent_pack_service,
        )
        current_score = _score_crypto_policy_rows(rows, settings=self.settings, crypto_policy=current_policy)
        candidate_score = _score_crypto_policy_rows(rows, settings=self.settings, crypto_policy=candidate_policy)
        canary = {
            "settled_rows": len(rows),
            "evidence_source": "crypto_live_market_snapshots",
            "candidate_selected_rows": candidate_score["selected_count"],
            "current_selected_rows": current_score["selected_count"],
            "candidate_net_pnl": _money(candidate_score["net_pnl"]),
            "current_net_pnl": _money(current_score["net_pnl"]),
            "candidate_drawdown_proxy": _money(candidate_score["drawdown_proxy"]),
            "current_drawdown_proxy": _money(current_score["drawdown_proxy"]),
            "staged_at": staged_at.isoformat(),
            "evaluated_at": now.isoformat(),
            "assets": sorted(changed_assets),
        }
        min_rows = int(self.settings.autonomous_gate_tuning_canary_min_settled_rows)
        expired = now - staged_at > timedelta(hours=int(self.settings.autonomous_gate_tuning_canary_max_wait_hours))
        if candidate_score["selected_count"] < min_rows:
            if dry_run or not expired:
                return {
                    "status": "canary_pending",
                    "kalshi_env": kalshi_env,
                    "domain": "crypto",
                    "candidate_version": checkpoint_payload.get("candidate_version"),
                    "reason": "insufficient_canary_support",
                    "required_candidate_rows": min_rows,
                    "expired": expired,
                    "canary": canary,
                }
            return await self._reject_crypto_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="canary_support_timeout",
                canary=canary,
                now=now,
            )
        passed = (
            candidate_score["net_pnl"] > current_score["net_pnl"]
            and candidate_score["drawdown_proxy"] <= current_score["drawdown_proxy"]
        )
        if dry_run:
            return {
                "status": "canary_passed" if passed else "canary_failed",
                "kalshi_env": kalshi_env,
                "domain": "crypto",
                "dry_run": True,
                "candidate_version": checkpoint_payload.get("candidate_version"),
                "canary": canary,
            }
        if not passed:
            return await self._reject_crypto_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="canary_pnl_or_drawdown_regression",
                canary=canary,
                now=now,
            )
        return await self._promote_crypto_candidate(
            repo,
            checkpoint_payload=checkpoint_payload,
            kalshi_env=kalshi_env,
            canary=canary,
            now=now,
        )

    async def _promote_crypto_candidate(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        canary: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        result = await self._promote_candidate(
            repo,
            checkpoint_payload=checkpoint_payload,
            kalshi_env=kalshi_env,
            canary=canary,
            now=now,
            checkpoint_domain="crypto",
        )
        result["domain"] = "crypto"
        return result

    async def _reject_crypto_candidate(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        reason: str,
        canary: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        result = await self._reject_candidate(
            repo,
            checkpoint_payload=checkpoint_payload,
            kalshi_env=kalshi_env,
            reason=reason,
            canary=canary,
            now=now,
            checkpoint_domain="crypto",
        )
        result["domain"] = "crypto"
        return result

    async def _validate_candidate(
        self,
        *,
        settings: Settings,
        kalshi_env: str,
        days: int,
        now: datetime,
    ) -> dict[str, Any]:
        backtesting = await self.backtesting_builder(
            settings=settings,
            session_factory=self.session_factory,
            decision_corpus_service=self.decision_corpus_service,
            trade_analysis_service=self.trade_analysis_service,
            kalshi_env=kalshi_env,
            days=days,
            full_history=True,
            command="validate",
            dataset_source="gate-learning-bundles",
            row_limit=None,
            now=now,
        )
        modeling = await self.modeling_builder(
            settings=settings,
            decision_corpus_service=self.decision_corpus_service,
            trading_audit_service=self.trading_audit_service,
            trade_analysis_service=self.trade_analysis_service,
            kalshi_env=kalshi_env,
            days=days,
            command="validate",
            dataset_source="gate-learning-bundles",
            row_limit=None,
            now=now,
        )
        failures: list[str] = []
        backtesting_rows = int(((backtesting.get("dataset") or {}).get("row_count")) or 0)
        modeling_rows = int(((modeling.get("dataset") or {}).get("row_count")) or 0)
        if backtesting_rows <= 0:
            failures.append("backtesting_zero_rows")
        if modeling_rows <= 0:
            failures.append("modeling_zero_rows")
        backtesting_failures = _fail_issue_codes(
            backtesting,
            prefix="backtesting",
            ignored_codes={"production_entry_freeze_disabled"},
        )
        modeling_failures = _fail_issue_codes(
            modeling,
            prefix="modeling",
            ignored_codes={"production_entry_freeze_disabled"},
        )
        failures.extend(backtesting_failures)
        failures.extend(modeling_failures)
        if str(backtesting.get("status")) == "fail" and backtesting_failures:
            failures.append("backtesting_status_fail")
        if str(modeling.get("status")) == "fail" and modeling_failures:
            failures.append("modeling_status_fail")
        return {
            "passed": not failures,
            "failures": sorted(set(failures)),
            "backtesting_status": backtesting.get("status"),
            "modeling_status": modeling.get("status"),
            "backtesting_rows": backtesting_rows,
            "modeling_rows": modeling_rows,
            "backtesting": _compact_validation_report(backtesting),
            "modeling": _compact_validation_report(modeling),
        }

    async def _stage_candidate(
        self,
        repo: PlatformRepository,
        *,
        current_pack: AgentPack,
        candidate_thresholds: dict[str, Any],
        current_thresholds: dict[str, Any],
        changes: dict[str, dict[str, Any]],
        recommendation: dict[str, Any],
        validation: dict[str, Any],
        evidence_fingerprint: str,
        kalshi_env: str,
        source: str,
        days: int,
        min_support: int,
        triggered_by: str,
        now: datetime,
        history: list[str],
        weather_scope: dict[str, Any],
    ) -> dict[str, Any]:
        version = f"gate-tuning-{now.strftime('%Y%m%dT%H%M%SZ')}"
        valid_until = now + timedelta(hours=int(self.settings.autonomous_gate_tuning_canary_max_wait_hours))
        threshold_payload = current_pack.thresholds.model_dump(mode="json")
        if str(weather_scope.get("scope") or "global") == "global":
            threshold_payload.update(candidate_thresholds)
        weather_policy = current_pack.weather_policy.model_copy(deep=True)
        policy_key = str(weather_scope.get("policy_key") or "")
        if policy_key:
            policy_thresholds = current_pack.thresholds.model_copy(update=candidate_thresholds)
            weather_policy.policies[policy_key] = AgentPackWeatherPolicy(
                policy_key=policy_key,
                strategy_code=str(weather_scope.get("strategy_code") or "A"),
                cohort_id=str(weather_scope.get("cohort_id") or "global"),
                series_ticker=str(weather_scope.get("series_ticker") or "any"),
                side=str(weather_scope.get("side") or "any"),
                season=str(weather_scope.get("season") or "all_season"),
                month=str(weather_scope.get("month") or "all_month"),
                regime=str(weather_scope.get("regime") or "any"),
                threshold_band=str(weather_scope.get("threshold_band") or "any"),
                lane=str(weather_scope.get("lane") or "entry_gate"),
                mode="live",
                action="loosen" if _thresholds_are_relaxed(changes) else "tighten",
                thresholds=policy_thresholds,
                parent_policy_key=str(weather_scope.get("parent_policy_key") or ""),
                evidence={
                    "evidence_fingerprint": evidence_fingerprint,
                    "source": source,
                    "days": days,
                    "min_support": min_support,
                    "row_counts": recommendation.get("row_counts") or {},
                    "source_files": recommendation.get("source_files") or {},
                },
                scores={
                    "changes": changes,
                    "gate_evidence": [
                        row for row in recommendation.get("gate_evidence") or []
                        if row.get("promotion_status") == "promoted"
                    ],
                },
                reason_codes=["staged_by_autonomous_gate_tuning"],
                deterministic_summary=(
                    f"Scoped weather policy {policy_key} staged with {len(changes)} threshold change(s)."
                ),
                valid_until=valid_until,
                metadata={
                    "triggered_by": triggered_by,
                    "staged_at": now.isoformat(),
                    "valid_until": valid_until.isoformat(),
                },
            )
        candidate_pack = current_pack.model_copy(
            update={
                "version": version,
                "status": "staged",
                "parent_version": current_pack.version,
                "source": OPS_SOURCE,
                "description": "Autonomous gate tuning candidate from bundle-backed backtests and modeling.",
                "thresholds": current_pack.thresholds.model_copy(update=threshold_payload),
                "weather_policy": weather_policy,
                "metadata": {
                    **dict(current_pack.metadata or {}),
                    "autonomous_gate_tuning": {
                        "staged_at": now.isoformat(),
                        "triggered_by": triggered_by,
                        "kalshi_env": kalshi_env,
                        "source": source,
                        "days": days,
                        "min_support": min_support,
                        "evidence_fingerprint": evidence_fingerprint,
                        "changes": changes,
                        "weather_scope": weather_scope,
                        "valid_until": valid_until.isoformat(),
                    },
                },
            }
        )
        candidate_pack = self.agent_pack_service.sanitize_candidate_pack(candidate_pack, parent_version=current_pack.version)
        await repo.update_agent_pack(candidate_pack)
        promotion = await repo.create_promotion_event(
            candidate_version=version,
            previous_version=current_pack.version,
            target_color=self.settings.app_color,
            evaluation_run_id=None,
            payload={
                "kind": "autonomous_gate_tuning",
                "evidence_fingerprint": evidence_fingerprint,
                "current_thresholds": current_thresholds,
                "candidate_thresholds": candidate_thresholds,
                "changes": changes,
                "weather_scope": weather_scope,
                "recommendation_summary": {
                    "row_counts": recommendation.get("row_counts"),
                    "source_files": recommendation.get("source_files"),
                    "confidence_warnings": recommendation.get("confidence_warnings") or [],
                },
                "validation": validation,
            },
            status="staged",
        )
        control = await repo.get_deployment_control()
        notes = self.agent_pack_service._notes(control)
        notes["autonomous_gate_tuning"] = {
            "status": "staged",
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "evidence_fingerprint": evidence_fingerprint,
            "staged_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "changes": changes,
        }
        await repo.update_deployment_notes(self.agent_pack_service._replace_notes(control.notes, notes))
        history = list(dict.fromkeys([*history, evidence_fingerprint]))
        checkpoint_payload = {
            "status": "staged",
            "kalshi_env": kalshi_env,
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "staged_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "source": source,
            "days": days,
            "min_support": min_support,
            "evidence_fingerprint": evidence_fingerprint,
            "evidence_fingerprint_history": history[-20:],
            "current_thresholds": current_thresholds,
            "candidate_thresholds": candidate_thresholds,
            "changes": changes,
            "weather_scope": weather_scope,
            "recommendation_summary": {
                "row_counts": recommendation.get("row_counts"),
                "source_files": recommendation.get("source_files"),
                "confidence_warnings": recommendation.get("confidence_warnings") or [],
            },
        }
        await repo.set_checkpoint(_checkpoint_name(kalshi_env), cursor=None, payload=checkpoint_payload)
        await repo.log_ops_event(
            severity="info",
            summary=f"Autonomous gate tuning staged {version} with {len(changes)} threshold change(s)",
            source=OPS_SOURCE,
            payload=checkpoint_payload,
            kalshi_env=kalshi_env,
        )
        return {
            "status": "staged",
            "kalshi_env": kalshi_env,
            "candidate_version": version,
            "previous_version": current_pack.version,
            "promotion_event_id": promotion.id,
            "changes": changes,
            "weather_scope": weather_scope,
            "evidence_fingerprint": evidence_fingerprint,
            "valid_until": valid_until.isoformat(),
            "validation": validation,
        }

    async def _evaluate_canary(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        dry_run: bool,
        now: datetime,
    ) -> dict[str, Any]:
        staged_at = _as_utc(checkpoint_payload.get("staged_at")) or now
        valid_until = _as_utc(checkpoint_payload.get("valid_until"))
        if valid_until is not None and now > valid_until:
            return await self._reject_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="candidate_validity_expired",
                canary={
                    "evidence_source": "live_decision_corpus",
                    "staged_at": staged_at.isoformat(),
                    "valid_until": valid_until.isoformat(),
                    "evaluated_at": now.isoformat(),
                    "expired": True,
                },
                now=now,
            )
        records = await repo.list_current_decision_corpus_rows(kalshi_env=kalshi_env, limit=10000)
        rows = _canary_rows(
            [
                row
                for record in records
                if (row := decision_corpus_row_to_gate_learning_row(record)) is not None
            ],
            staged_at=staged_at,
        )
        rows = _filter_weather_scope_rows(rows, dict(checkpoint_payload.get("weather_scope") or {}))
        current_score = _score_threshold_rows(rows, dict(checkpoint_payload.get("current_thresholds") or {}))
        candidate_score = _score_threshold_rows(rows, dict(checkpoint_payload.get("candidate_thresholds") or {}))
        canary = {
            "settled_rows": len(rows),
            "evidence_source": "live_decision_corpus",
            "candidate_selected_rows": candidate_score["selected_count"],
            "current_selected_rows": current_score["selected_count"],
            "candidate_net_pnl": _money(candidate_score["net_pnl"]),
            "current_net_pnl": _money(current_score["net_pnl"]),
            "candidate_drawdown_proxy": _money(candidate_score["drawdown_proxy"]),
            "current_drawdown_proxy": _money(current_score["drawdown_proxy"]),
            "staged_at": staged_at.isoformat(),
            "valid_until": valid_until.isoformat() if valid_until is not None else None,
            "evaluated_at": now.isoformat(),
        }
        min_rows = int(self.settings.autonomous_gate_tuning_canary_min_settled_rows)
        expired = now - staged_at > timedelta(hours=int(self.settings.autonomous_gate_tuning_canary_max_wait_hours))
        if candidate_score["selected_count"] < min_rows:
            if dry_run or not expired:
                return {
                    "status": "canary_pending",
                    "kalshi_env": kalshi_env,
                    "candidate_version": checkpoint_payload.get("candidate_version"),
                    "reason": "insufficient_canary_support",
                    "required_candidate_rows": min_rows,
                    "expired": expired,
                    "canary": canary,
                }
            return await self._reject_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="canary_support_timeout",
                canary=canary,
                now=now,
            )
        passed = (
            candidate_score["net_pnl"] > current_score["net_pnl"]
            and candidate_score["drawdown_proxy"] <= current_score["drawdown_proxy"]
        )
        if dry_run:
            return {
                "status": "canary_passed" if passed else "canary_failed",
                "kalshi_env": kalshi_env,
                "dry_run": True,
                "candidate_version": checkpoint_payload.get("candidate_version"),
                "canary": canary,
            }
        if not passed:
            return await self._reject_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="canary_pnl_or_drawdown_regression",
                canary=canary,
                now=now,
            )
        return await self._promote_candidate(
            repo,
            checkpoint_payload=checkpoint_payload,
            kalshi_env=kalshi_env,
            canary=canary,
            now=now,
        )

    async def _evaluate_historical_bootstrap(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        source: str,
        days: int,
        min_support: int,
        dry_run: bool,
        now: datetime,
    ) -> dict[str, Any]:
        allowed, block_reason = await self._historical_bootstrap_allowed(
            repo,
            checkpoint_payload=checkpoint_payload,
            now=now,
        )
        if not allowed:
            return {
                "status": "historical_bootstrap_rejected",
                "kalshi_env": kalshi_env,
                "candidate_version": checkpoint_payload.get("candidate_version"),
                "reason": block_reason,
            }
        gate_service = self.gate_learning_service_factory(self.settings)
        cutoff = now - timedelta(days=days)
        rows = [
            row for row in gate_service.load_bundle_rows(source=source)
            if row.decision_time is None or row.decision_time >= cutoff
        ]
        rows = _filter_weather_scope_rows(rows, dict(checkpoint_payload.get("weather_scope") or {}))
        labeled = [row for row in rows if row.labeled]
        train, holdout = _walk_forward_threshold_split(labeled)
        current_thresholds = dict(checkpoint_payload.get("current_thresholds") or {})
        candidate_thresholds = dict(checkpoint_payload.get("candidate_thresholds") or {})
        current_train = _score_threshold_episode_rows(train, current_thresholds)
        current_holdout = _score_threshold_episode_rows(holdout, current_thresholds)
        candidate_train = _score_threshold_episode_rows(train, candidate_thresholds)
        candidate_holdout = _score_threshold_episode_rows(holdout, candidate_thresholds)
        total_candidate_support = candidate_train["selected_count"] + candidate_holdout["selected_count"]
        required_holdout_rows = max(
            10,
            (int(min_support) + 3) // 4,
            int(self.settings.autonomous_gate_tuning_canary_min_settled_rows),
        )
        failures: list[str] = []
        if not labeled:
            failures.append("historical_bootstrap_zero_labeled_rows")
        if total_candidate_support < int(min_support):
            failures.append("historical_bootstrap_insufficient_total_support")
        if candidate_holdout["selected_count"] < required_holdout_rows:
            failures.append("historical_bootstrap_insufficient_holdout_support")
        if candidate_holdout["net_pnl"] <= Decimal("0"):
            failures.append("historical_bootstrap_holdout_net_pnl_not_positive")
        if candidate_holdout["net_pnl"] <= current_holdout["net_pnl"]:
            failures.append("historical_bootstrap_holdout_net_pnl_not_improved")
        if candidate_holdout["drawdown_proxy"] > current_holdout["drawdown_proxy"]:
            failures.append("historical_bootstrap_holdout_drawdown_worse")
        canary = {
            "settled_rows": len(holdout),
            "evidence_source": "historical_bootstrap_holdout",
            "promotion_source": "historical_bootstrap",
            "source": source,
            "window_days": days,
            "labeled_rows": len(labeled),
            "train_rows": len(train),
            "holdout_rows": len(holdout),
            "required_total_support": int(min_support),
            "required_holdout_rows": required_holdout_rows,
            "candidate_total_selected_rows": total_candidate_support,
            "candidate_train_selected_rows": candidate_train["selected_count"],
            "current_train_selected_rows": current_train["selected_count"],
            "candidate_selected_rows": candidate_holdout["selected_count"],
            "current_selected_rows": current_holdout["selected_count"],
            "candidate_train_net_pnl": _money(candidate_train["net_pnl"]),
            "current_train_net_pnl": _money(current_train["net_pnl"]),
            "candidate_net_pnl": _money(candidate_holdout["net_pnl"]),
            "current_net_pnl": _money(current_holdout["net_pnl"]),
            "candidate_drawdown_proxy": _money(candidate_holdout["drawdown_proxy"]),
            "current_drawdown_proxy": _money(current_holdout["drawdown_proxy"]),
            "source_files": gate_service.source_files(source=source),
            "staged_at": checkpoint_payload.get("staged_at"),
            "evaluated_at": now.isoformat(),
            "failures": failures,
        }
        if failures:
            if not dry_run:
                return await self._reject_candidate(
                    repo,
                    checkpoint_payload=checkpoint_payload,
                    kalshi_env=kalshi_env,
                    reason=f"historical_bootstrap_failed:{','.join(failures)}",
                    canary=canary,
                    now=now,
                )
            return {
                "status": "historical_bootstrap_failed",
                "kalshi_env": kalshi_env,
                "candidate_version": checkpoint_payload.get("candidate_version"),
                "reason": ",".join(failures),
                "canary": canary,
            }
        if dry_run:
            return {
                "status": "historical_bootstrap_passed",
                "kalshi_env": kalshi_env,
                "dry_run": True,
                "candidate_version": checkpoint_payload.get("candidate_version"),
                "canary": canary,
            }
        result = await self._promote_candidate(
            repo,
            checkpoint_payload=checkpoint_payload,
            kalshi_env=kalshi_env,
            canary=canary,
            now=now,
        )
        result["promotion_source"] = "historical_bootstrap"
        return result

    async def _historical_bootstrap_allowed(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        now: datetime,
    ) -> tuple[bool, str | None]:
        if checkpoint_payload.get("status") != "staged":
            return False, "historical_bootstrap_requires_staged_candidate"
        valid_until = _as_utc(checkpoint_payload.get("valid_until"))
        if valid_until is not None and now > valid_until:
            return False, "historical_bootstrap_candidate_expired"
        candidate_version = str(checkpoint_payload.get("candidate_version") or "")
        if not candidate_version:
            return False, "historical_bootstrap_candidate_missing"
        previous_version = str(checkpoint_payload.get("previous_version") or "")
        if previous_version != "builtin-deterministic-v1":
            return False, "historical_bootstrap_requires_builtin_startup_pack"
        control = await repo.get_deployment_control()
        active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
        if active_pack.version != previous_version:
            return False, "historical_bootstrap_active_pack_changed"
        if checkpoint_payload.get("promoted_at"):
            return False, "historical_bootstrap_already_promoted"
        return True, None

    async def _promote_candidate(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        canary: dict[str, Any],
        now: datetime,
        checkpoint_domain: str = "weather",
    ) -> dict[str, Any]:
        candidate_version = str(checkpoint_payload["candidate_version"])
        previous_version = checkpoint_payload.get("previous_version")
        record = await repo.get_agent_pack(candidate_version)
        if record is None:
            return await self._reject_candidate(
                repo,
                checkpoint_payload=checkpoint_payload,
                kalshi_env=kalshi_env,
                reason="candidate_pack_missing",
                canary=canary,
                now=now,
            )
        pack = AgentPack.model_validate(record.payload)
        promoted = pack.model_copy(
            update={
                "status": "champion",
                "metadata": {
                    **dict(pack.metadata or {}),
                    "promoted_by": OPS_SOURCE,
                    "promoted_at": now.isoformat(),
                    "canary": canary,
                },
            }
        )
        await repo.update_agent_pack(promoted)
        if previous_version is not None:
            previous_record = await repo.get_agent_pack(str(previous_version))
            if previous_record is not None:
                previous_pack = AgentPack.model_validate(previous_record.payload)
                await repo.update_agent_pack(
                    previous_pack.model_copy(
                        update={
                            "status": "archived",
                            "metadata": {
                                **dict(previous_pack.metadata or {}),
                                "archived_by": candidate_version,
                                "archived_at": now.isoformat(),
                            },
                        }
                    )
                )
        await self.agent_pack_service.assign_pack_to_color(
            repo,
            color=self.settings.app_color,
            version=candidate_version,
        )
        promotion_id = checkpoint_payload.get("promotion_event_id")
        if promotion_id:
            promotion = await repo.get_promotion_event(str(promotion_id))
            payload = dict(promotion.payload if promotion is not None else {})
            payload["canary"] = canary
            payload["promoted_at"] = now.isoformat()
            await repo.update_promotion_event(str(promotion_id), status="stable", payload=payload)
        payload = {**checkpoint_payload, "status": "champion", "promoted_at": now.isoformat(), "canary": canary}
        await repo.set_checkpoint(_checkpoint_name(kalshi_env, domain=checkpoint_domain), cursor=None, payload=payload)
        await repo.log_ops_event(
            severity="info",
            summary=f"Autonomous gate tuning promoted {candidate_version}",
            source=OPS_SOURCE,
            payload=payload,
            kalshi_env=kalshi_env,
        )
        return {
            "status": "promoted",
            "kalshi_env": kalshi_env,
            "candidate_version": candidate_version,
            "previous_version": previous_version,
            "canary": canary,
        }

    async def _reject_candidate(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        reason: str,
        canary: dict[str, Any],
        now: datetime,
        checkpoint_domain: str = "weather",
    ) -> dict[str, Any]:
        candidate_version = str(checkpoint_payload.get("candidate_version") or "")
        if candidate_version:
            record = await repo.get_agent_pack(candidate_version)
            if record is not None:
                pack = AgentPack.model_validate(record.payload)
                await repo.update_agent_pack(
                    pack.model_copy(
                        update={
                            "status": "rejected",
                            "metadata": {
                                **dict(pack.metadata or {}),
                                "rejected_by": OPS_SOURCE,
                                "rejected_at": now.isoformat(),
                                "rejection_reason": reason,
                                "canary": canary,
                            },
                        }
                    )
                )
        promotion_id = checkpoint_payload.get("promotion_event_id")
        if promotion_id:
            promotion = await repo.get_promotion_event(str(promotion_id))
            payload = dict(promotion.payload if promotion is not None else {})
            payload["canary"] = canary
            payload["rejected_at"] = now.isoformat()
            await repo.update_promotion_event(str(promotion_id), status="rolled_back", payload=payload, rollback_reason=reason)
        payload = {**checkpoint_payload, "status": "rejected", "rejected_at": now.isoformat(), "rejection_reason": reason, "canary": canary}
        await repo.set_checkpoint(_checkpoint_name(kalshi_env, domain=checkpoint_domain), cursor=None, payload=payload)
        await repo.log_ops_event(
            severity="warning",
            summary=f"Autonomous gate tuning rejected {candidate_version or 'candidate'}: {reason}",
            source=OPS_SOURCE,
            payload=payload,
            kalshi_env=kalshi_env,
        )
        return {
            "status": "rejected",
            "kalshi_env": kalshi_env,
            "candidate_version": candidate_version,
            "reason": reason,
            "canary": canary,
        }


def _checkpoint_name(kalshi_env: str, *, domain: str = "weather") -> str:
    if domain == "crypto":
        return f"{CHECKPOINT_PREFIX}:crypto:{kalshi_env}"
    return f"{CHECKPOINT_PREFIX}:{kalshi_env}"


def _normalize_domain(domain: str | None) -> str:
    normalized = str(domain or "weather").strip().lower()
    if normalized not in {"weather", "crypto", "all"}:
        raise ValueError("domain must be one of: weather, crypto, all")
    return normalized


def _combined_status(weather: dict[str, Any], crypto: dict[str, Any]) -> str:
    statuses = {str(weather.get("status")), str(crypto.get("status"))}
    if "staged" in statuses:
        return "staged"
    if "promoted" in statuses:
        return "promoted"
    if "validation_failed" in statuses or "rejected" in statuses:
        return "attention"
    if statuses <= {"no_candidate", "duplicate_evidence", "canary_pending"}:
        return "no_candidate"
    return "ok"


def _as_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _settings_with_thresholds(settings: Settings, thresholds: RuntimeThresholds) -> Settings:
    return settings.model_copy(update=_threshold_values(thresholds))


def _threshold_values(thresholds: RuntimeThresholds) -> dict[str, Any]:
    return {field: getattr(thresholds, field) for field in TUNABLE_GATE_FIELDS}


def _settings_threshold_values(settings: Settings) -> dict[str, Any]:
    return {field: getattr(settings, field) for field in TUNABLE_GATE_FIELDS}


def _weather_scope_payload(
    *,
    policy_scope: str,
    series_ticker: str | None,
    side: str | None,
    month: int | str | None,
    lane: str,
) -> dict[str, Any]:
    context = WeatherPolicyContext(
        strategy_code="A",
        cohort_id="global",
        series_ticker=series_ticker if str(policy_scope or "global") == "city" else None,
        side=side,
        month=month,
        lane=lane,
    )
    policy_key = build_weather_policy_key(context)
    parts = policy_key.split("/")
    return {
        "scope": str(policy_scope or "global"),
        "policy_key": policy_key,
        "strategy_code": parts[1],
        "cohort_id": parts[2],
        "series_ticker": parts[3],
        "side": parts[4],
        "season": parts[5],
        "month": parts[6],
        "regime": parts[7],
        "threshold_band": parts[8],
        "lane": parts[9],
    }


def _weather_policy_context_from_scope(scope: dict[str, Any]) -> WeatherPolicyContext:
    return WeatherPolicyContext(
        strategy_code=str(scope.get("strategy_code") or "A"),
        cohort_id=str(scope.get("cohort_id") or "global"),
        series_ticker=str(scope.get("series_ticker") or "any"),
        side=str(scope.get("side") or "any"),
        season=str(scope.get("season") or "all_season"),
        month=str(scope.get("month") or "all_month"),
        regime=str(scope.get("regime") or "any"),
        threshold_band=str(scope.get("threshold_band") or "any"),
        lane=str(scope.get("lane") or "entry_gate"),
    )


def _thresholds_are_relaxed(changes: dict[str, dict[str, Any]]) -> bool:
    relaxing_lower_is_better = {
        "risk_min_contract_price_dollars",
        "strategy_min_remaining_payout_bps",
        "risk_min_confidence",
        "risk_min_edge_bps",
        "strategy_min_abs_delta_f",
    }
    relaxing_higher_is_better = {"trigger_max_spread_bps", "risk_max_credible_edge_bps"}
    for field, details in changes.items():
        current = _decimal_or_none(details.get("current"))
        recommended = _decimal_or_none(details.get("recommended"))
        if current is None or recommended is None:
            continue
        if field in relaxing_lower_is_better and recommended < current:
            return True
        if field in relaxing_higher_is_better and recommended > current:
            return True
    return False


def _crypto_policy_values(policy: RuntimeCryptoPolicy) -> dict[str, Any]:
    return {
        "entry": {
            "min_fee_adjusted_edge_bps": policy.min_fee_adjusted_edge_bps,
            "max_spread_bps": policy.max_spread_bps,
            "min_confidence": policy.min_confidence,
            "min_contract_price_dollars": policy.min_contract_price_dollars,
            "min_remaining_payout_bps": policy.min_remaining_payout_bps,
            "max_credible_edge_bps": policy.max_credible_edge_bps,
        },
        "replay": {
            "min_resolved_markets": policy.replay_min_resolved_markets,
            "min_trade_candidates": policy.replay_min_trade_candidates,
            "min_net_pl_dollars": policy.replay_min_net_pl_dollars,
            "max_hard_cap_breaches": policy.replay_max_hard_cap_breaches,
            "min_spot_coverage_pct": policy.replay_min_spot_coverage_pct,
            "require_calibration_better_than_mid": policy.replay_require_calibration_better_than_mid,
            "require_pnl_beats_market_mid": policy.replay_require_pnl_beats_market_mid,
            "min_pnl_advantage_dollars": policy.replay_min_pnl_advantage_dollars,
        },
        "live": {
            "trading_enabled": policy.trading_enabled,
            "production_autonomy_enabled": policy.production_autonomy_enabled,
            "asset_modes": dict(policy.asset_modes),
        },
        "asset_entry_overrides": {
            symbol: {key: value for key, value in values.items() if value is not None}
            for symbol, values in policy.asset_entry_overrides.items()
        },
    }


def _crypto_artifact_status(artifact: Any | None) -> str:
    return str(getattr(artifact, "status", None) or "missing")


def _crypto_row_counts_from_artifact_metrics(
    metrics: dict[str, Any],
    *,
    dataset: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_count = int(metrics.get("sample_count") or metrics.get("resolved_sample_count") or 0)
    resolved = int(metrics.get("resolved_sample_count") or sample_count)
    dataset = dataset or {}
    return {
        "snapshot_rows": int(dataset.get("snapshot_count") or metrics.get("snapshot_count") or (1 if sample_count > 0 else 0)),
        "candle_rows": int(metrics.get("candle_count") or dataset.get("candle_count") or 0),
        "spot_rows": int(metrics.get("spot_row_count") or dataset.get("spot_row_count") or 0),
        "settled_snapshot_rows": int(dataset.get("settled_snapshot_count") or resolved),
        "decision_rows": sample_count,
        "labeled_rows": resolved,
        "assets": sorted(str(asset) for asset in (dataset.get("assets") or metrics.get("assets") or [])),
    }


def _crypto_validation_short_circuit_recommendation(
    *,
    kalshi_env: str,
    days: int,
    min_support: int,
    row_counts: dict[str, Any],
    artifact_checks: dict[str, Any],
    validation_failures: list[str],
    crypto_policy: RuntimeCryptoPolicy,
    asset_symbols: list[str] | None = None,
) -> dict[str, Any]:
    requested_assets = normalize_asset_symbols(asset_symbols)
    return {
        "schema_version": "crypto-autonomous-gate-recommendations-v1",
        "kalshi_env": kalshi_env,
        "domain": "crypto",
        "window_days": days,
        "min_support": min_support,
        "asset_symbols": requested_assets,
        "row_counts": row_counts,
        "data_gap_reason": None,
        "artifact_checks": artifact_checks,
        "current_policy": _crypto_policy_values(crypto_policy),
        "asset_diagnostics": [],
        "promoted_assets": {},
        "validation_short_circuit": True,
        "validation": {
            "passed": False,
            "failures": sorted(set(validation_failures)),
            "row_counts": row_counts,
            "artifact_checks": artifact_checks,
            "data_gap_reason": None,
        },
    }


def _crypto_data_gap_reason(
    *,
    snapshot_rows: int,
    candle_rows: int,
    spot_rows: int,
    decision_rows: int,
    labeled_rows: int,
    settled_snapshot_rows: int,
) -> str | None:
    if snapshot_rows <= 0:
        return "no_crypto_market_snapshots_for_env_window"
    if candle_rows <= 0:
        return "no_crypto_candlesticks_for_env_window"
    if spot_rows <= 0:
        return "no_crypto_spot_rows_for_env_window"
    if settled_snapshot_rows <= 0:
        return "no_settled_crypto_snapshots_for_env_window"
    if decision_rows <= 0:
        return "no_crypto_decision_rows_after_snapshot_candle_spot_join"
    if labeled_rows <= 0:
        return "no_labeled_crypto_rows_for_env_window"
    return None


def _runtime_crypto_policy_from_payload(payload: dict[str, Any], *, service: AgentPackService) -> RuntimeCryptoPolicy:
    pack = service.default_pack().model_copy(
        update={"crypto_policy": AgentPackCryptoPolicy.model_validate(payload)}
    )
    return service.runtime_crypto_policy(pack)


def _threshold_drift(active_thresholds: dict[str, Any], settings_thresholds: dict[str, Any]) -> dict[str, dict[str, Any]]:
    drift: dict[str, dict[str, Any]] = {}
    for field in TUNABLE_GATE_FIELDS:
        active = active_thresholds.get(field)
        baseline = settings_thresholds.get(field)
        if active != baseline:
            drift[field] = {"settings": baseline, "active": active}
    return drift


def _loosened_threshold_fields(active_thresholds: dict[str, Any], settings_thresholds: dict[str, Any]) -> list[str]:
    loosened: list[str] = []
    lower_is_looser = {
        "risk_min_contract_price_dollars",
        "strategy_min_remaining_payout_bps",
        "risk_min_confidence",
        "risk_min_edge_bps",
        "strategy_min_abs_delta_f",
    }
    higher_is_looser = {"trigger_max_spread_bps", "risk_max_credible_edge_bps"}
    for field in TUNABLE_GATE_FIELDS:
        active = active_thresholds.get(field)
        baseline = settings_thresholds.get(field)
        if active in (None, "") or baseline in (None, ""):
            continue
        try:
            active_f = float(active)
            baseline_f = float(baseline)
        except (TypeError, ValueError):
            continue
        if field in lower_is_looser and active_f < baseline_f:
            loosened.append(field)
        if field in higher_is_looser and active_f > baseline_f:
            loosened.append(field)
    return loosened


def _stage_summary(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    if not payload.get("candidate_version"):
        return None
    return {
        "status": payload.get("status"),
        "candidate_version": payload.get("candidate_version"),
        "previous_version": payload.get("previous_version"),
        "staged_at": payload.get("staged_at"),
        "changes": payload.get("changes") or {},
        "evidence_fingerprint": payload.get("evidence_fingerprint"),
    }


def _promotion_summary(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload or payload.get("status") != "champion":
        return None
    return {
        "candidate_version": payload.get("candidate_version"),
        "previous_version": payload.get("previous_version"),
        "promoted_at": payload.get("promoted_at"),
    }


def _candidate_threshold_values(
    recommendation: dict[str, Any],
    runtime_thresholds: RuntimeThresholds,
    *,
    settings: Settings,
) -> dict[str, Any]:
    current = _threshold_values(runtime_thresholds)
    candidate = dict(current)
    changes: dict[str, dict[str, Any]] = {}
    for field, details in dict(recommendation.get("recommended_settings") or {}).items():
        if field not in TUNABLE_GATE_FIELDS or not bool(details.get("changed")):
            continue
        original_recommended = details.get("recommended")
        recommended, floor_applied = _apply_operator_threshold_floor(field, original_recommended, settings=settings)
        if recommended == current.get(field):
            continue
        candidate[field] = recommended
        changes[field] = {
            "current": current[field],
            "recommended": recommended,
            "candidate_policy": details.get("candidate_policy"),
            "reason": details.get("reason"),
        }
        if floor_applied:
            changes[field]["operator_floor_applied"] = True
            changes[field]["original_recommended"] = original_recommended
    return {
        "current_thresholds": current,
        "candidate_thresholds": candidate,
        "changes": changes,
    }


def _apply_operator_threshold_floor(field: str, value: Any, *, settings: Settings) -> tuple[Any, bool]:
    if field != "risk_min_contract_price_dollars":
        return value, False
    recommended = _decimal_or_none(value)
    floor = _decimal_or_none(settings.risk_min_contract_price_dollars)
    if recommended is None or floor is None or recommended >= floor:
        return value, False
    return float(floor), True


def _candidate_crypto_policy_values(recommendation: dict[str, Any], current_pack: AgentPack) -> dict[str, Any]:
    current_policy = AgentPackCryptoPolicy.model_validate(current_pack.crypto_policy.model_dump(mode="json"))
    candidate_policy = AgentPackCryptoPolicy.model_validate(current_pack.crypto_policy.model_dump(mode="json"))
    changes: dict[str, Any] = {}
    live_modes = dict(candidate_policy.live.asset_modes or {})
    overrides = dict(candidate_policy.asset_entry_overrides or {})
    for asset, details in dict(recommendation.get("promoted_assets") or {}).items():
        symbol = normalize_asset_symbol(asset)
        entry = AgentPackCryptoEntryPolicy.model_validate(details["entry"])
        overrides[symbol] = entry
        live_modes[symbol] = "live"
        changes[symbol] = {
            "entry": entry.model_dump(mode="json"),
            "current_score": details.get("current_score"),
            "candidate_score": details.get("candidate_score"),
            "candidate_policy": details.get("candidate_policy"),
            "reason": details.get("promotion_reason"),
            "live_mode": "live",
        }
    candidate_policy.asset_entry_overrides = overrides
    candidate_policy.live = candidate_policy.live.model_copy(
        update={
            "trading_enabled": True,
            "production_autonomy_enabled": True,
            "asset_modes": live_modes,
        }
    )
    return {
        "current_crypto_policy": current_policy.model_dump(mode="json"),
        "candidate_crypto_policy": candidate_policy.model_dump(mode="json"),
        "changes": changes,
    }


def _normalize_fair_value_source(value: Any) -> str | None:
    text = _str_or_none(value)
    return text.lower() if text is not None else None


def _historical_row_original_reason(row: GateLearningRow) -> str | None:
    reasons = [
        reason
        for reason in (
            _normalize_block_reason(row.primary_block_reason),
            _normalize_block_reason(row.blocked_by),
        )
        if reason is not None
    ]
    non_empirical = [reason for reason in reasons if reason not in _EMPIRICAL_VISIBLE_REASONS]
    if non_empirical:
        return non_empirical[0]
    if any(reason in _EMPIRICAL_VISIBLE_REASONS for reason in reasons):
        return None
    return _UNKNOWN_ORIGINAL_GATE


def _historical_outcome(pnl: Decimal | None) -> str | None:
    if pnl is None:
        return None
    if pnl > Decimal("0"):
        return "win"
    if pnl < Decimal("0"):
        return "loss"
    return "flat"


def _historical_bootstrap_source_fingerprint(
    *,
    row: GateLearningRow,
    source_files: dict[str, list[str]],
    policy_key: str,
    decision_trace: dict[str, Any],
) -> str:
    payload = {
        "schema_version": "weather-bootstrap-historical-evidence-v1",
        "source": row.source,
        "source_files": source_files,
        "room_id": row.room_id,
        "market_ticker": row.market_ticker,
        "decision_time": row.decision_time.isoformat() if row.decision_time is not None else None,
        "market_day": row.market_day,
        "side": row.side,
        "bucket_key": decision_trace.get("bucket_key") or row.bucket_key,
        "legacy_bucket_key": row.bucket_key,
        "policy_key": policy_key,
        "quality_adjusted_edge_bps": row.quality_adjusted_edge_bps,
        "confidence": row.confidence,
        "fair_value_source": row.fair_value_source,
        "counterfactual_pnl_dollars": _money(row.counterfactual_pnl_dollars),
        "decision": {
            "tier": decision_trace.get("tier"),
            "reason": decision_trace.get("reason"),
            "thresholds": decision_trace.get("thresholds"),
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _historical_bootstrap_shadow_gate_status(
    *,
    policy: Any,
    evidence: list[WeatherBootstrapHistoricalEvidenceRecord],
    policy_key: str | None,
    now: datetime,
) -> dict[str, Any]:
    records = [
        row
        for row in evidence
        if row.strict_replay
        and row.tier == "cold"
        and row.pnl_dollars is not None
        and (policy_key is None or row.policy_key == policy_key)
        and str((row.payload or {}).get("fair_value_source") or "").lower() not in {"fallback", "unavailable", "dark", "none"}
    ]
    episodes: dict[tuple[str, str | None, str | None], WeatherBootstrapHistoricalEvidenceRecord] = {}
    for row in sorted(records, key=lambda item: _as_utc(item.observed_at) or datetime.min.replace(tzinfo=UTC)):
        key = (
            row.market_ticker,
            row.local_market_day or market_day_from_ticker(row.market_ticker),
            coarse_empirical_bucket_key_from_legacy(row.bucket_key) or row.bucket_key,
        )
        episodes.setdefault(key, row)
    episode_rows = list(episodes.values())
    train, holdout = _split_historical_bootstrap_evidence(episode_rows)
    total_score = _score_historical_bootstrap_evidence(episode_rows)
    holdout_score = _score_historical_bootstrap_evidence(holdout)
    required_total = max(1, int(getattr(policy.caps, "shadow_min_market_episodes", 5)))
    required_holdout = max(1, (required_total + 3) // 4)
    reason_codes: list[str] = []
    if len(episode_rows) < required_total:
        reason_codes.append("historical_bootstrap_total_support_not_met")
    if len(holdout) < required_holdout:
        reason_codes.append("historical_bootstrap_holdout_support_not_met")
    if holdout_score["net_pnl"] <= Decimal("0"):
        reason_codes.append("historical_bootstrap_holdout_net_pnl_not_positive")
    if holdout_score["drawdown_proxy"] > Decimal("0"):
        reason_codes.append("historical_bootstrap_holdout_drawdown_worse")
    eligible = not reason_codes and bool(episode_rows)
    return {
        "status": "eligible" if eligible else "pending",
        "eligible_for_promotion": eligible,
        "evidence_source": "strict_historical_startup",
        "evaluated_at": now.isoformat(),
        "market_episodes": len(episode_rows),
        "train_episodes": len(train),
        "holdout_episodes": len(holdout),
        "required_market_episodes": required_total,
        "required_holdout_episodes": required_holdout,
        "candidate_net_pnl": _money(total_score["net_pnl"]),
        "candidate_holdout_net_pnl": _money(holdout_score["net_pnl"]),
        "current_holdout_net_pnl": _money(Decimal("0")),
        "candidate_drawdown_proxy": _money(total_score["drawdown_proxy"]),
        "candidate_holdout_drawdown_proxy": _money(holdout_score["drawdown_proxy"]),
        "current_holdout_drawdown_proxy": _money(Decimal("0")),
        "reason_codes": reason_codes,
    }


def _split_historical_bootstrap_evidence(
    rows: list[WeatherBootstrapHistoricalEvidenceRecord],
) -> tuple[list[WeatherBootstrapHistoricalEvidenceRecord], list[WeatherBootstrapHistoricalEvidenceRecord]]:
    ordered = sorted(rows, key=lambda item: _as_utc(item.observed_at) or datetime.min.replace(tzinfo=UTC))
    days = sorted({(_as_utc(row.observed_at) or datetime.min.replace(tzinfo=UTC)).date().isoformat() for row in ordered})
    if len(days) >= 3:
        holdout_start = days[max(1, int(len(days) * 0.70))]
        train = [
            row
            for row in ordered
            if (_as_utc(row.observed_at) or datetime.min.replace(tzinfo=UTC)).date().isoformat() < holdout_start
        ]
        holdout = [
            row
            for row in ordered
            if (_as_utc(row.observed_at) or datetime.min.replace(tzinfo=UTC)).date().isoformat() >= holdout_start
        ]
        if train and holdout:
            return train, holdout
    split_idx = max(1, int(len(ordered) * 0.70))
    return ordered[:split_idx], ordered[split_idx:]


def _score_historical_bootstrap_evidence(rows: list[WeatherBootstrapHistoricalEvidenceRecord]) -> dict[str, Decimal]:
    pnls = [row.pnl_dollars or Decimal("0") for row in rows if row.pnl_dollars is not None]
    net = sum(pnls, Decimal("0"))
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {"net_pnl": net, "drawdown_proxy": max_drawdown}


def _evidence_fingerprint(
    *,
    recommendation: dict[str, Any],
    current_thresholds: dict[str, Any],
    candidate_thresholds: dict[str, Any],
) -> str:
    payload = {
        "schema_version": recommendation.get("schema_version"),
        "source": recommendation.get("source"),
        "source_files": recommendation.get("source_files"),
        "row_counts": recommendation.get("row_counts"),
        "current_thresholds": current_thresholds,
        "candidate_thresholds": candidate_thresholds,
        "recommended_settings": recommendation.get("recommended_settings"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bootstrap_evidence_fingerprint(
    *,
    pack_version: str,
    policy_key: str,
    evidence_gate: dict[str, Any],
    evidence_source: str,
    target_rollout: str,
) -> str:
    payload = {
        "schema_version": "weather-empirical-bootstrap-promotion-v1",
        "pack_version": pack_version,
        "policy_key": policy_key,
        "evidence_gate": evidence_gate,
        "evidence_source": evidence_source,
        "target_rollout": target_rollout,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _crypto_evidence_fingerprint(
    *,
    recommendation: dict[str, Any],
    current_policy: dict[str, Any],
    candidate_policy: dict[str, Any],
) -> str:
    payload = {
        "schema_version": recommendation.get("schema_version"),
        "row_counts": recommendation.get("row_counts"),
        "current_crypto_policy": current_policy,
        "candidate_crypto_policy": candidate_policy,
        "promoted_assets": recommendation.get("promoted_assets"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fail_issue_codes(
    report: dict[str, Any],
    *,
    prefix: str,
    ignored_codes: set[str] | None = None,
) -> list[str]:
    ignored = ignored_codes or set()
    return [
        f"{prefix}:{issue.get('code') or 'unknown'}"
        for issue in report.get("issues") or []
        if isinstance(issue, dict) and issue.get("severity") == "fail" and str(issue.get("code") or "unknown") not in ignored
    ]


def _compact_validation_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "dataset": report.get("dataset"),
        "issues": report.get("issues") or [],
        "promotion_gates": report.get("promotion_gates"),
    }


def _canary_rows(rows: list[GateLearningRow], *, staged_at: datetime) -> list[GateLearningRow]:
    output: list[GateLearningRow] = []
    for row in rows:
        if not row.labeled:
            continue
        observed_at = row.settlement_ts or row.decision_time
        if observed_at is None or observed_at <= staged_at:
            continue
        output.append(row)
    return output


def _filter_weather_scope_rows(rows: list[GateLearningRow], scope: dict[str, Any]) -> list[GateLearningRow]:
    series = str(scope.get("series_ticker") or "any").upper()
    side = str(scope.get("side") or "any").lower()
    month = str(scope.get("month") or "all_month").lower()
    filtered: list[GateLearningRow] = []
    for row in rows:
        if series not in {"", "ANY"} and (row.series_ticker or "").upper() != series:
            continue
        if side not in {"", "any"} and (row.side or "").lower() != side:
            continue
        if month not in {"", "all_month", "any"} and _gate_row_month(row) != month:
            continue
        filtered.append(row)
    return filtered


def _gate_row_month(row: GateLearningRow) -> str:
    if row.market_day:
        parts = str(row.market_day).split("-")
        if len(parts) >= 2 and parts[1].isdigit():
            return f"m{int(parts[1]):02d}"
    return "all_month"


def _score_threshold_rows(rows: list[GateLearningRow], thresholds: dict[str, Any]) -> dict[str, Any]:
    selected_pnls: list[Decimal] = []
    for row in rows:
        if _row_passes_thresholds(row, thresholds):
            selected_pnls.append(row.counterfactual_pnl_dollars or Decimal("0"))
    net = sum(selected_pnls, Decimal("0"))
    cumulative = Decimal("0")
    min_cumulative = Decimal("0")
    for pnl in selected_pnls:
        cumulative += pnl
        min_cumulative = min(min_cumulative, cumulative)
    return {
        "selected_count": len(selected_pnls),
        "net_pnl": net,
        "drawdown_proxy": abs(min_cumulative),
    }


def _walk_forward_threshold_split(rows: list[GateLearningRow]) -> tuple[list[GateLearningRow], list[GateLearningRow]]:
    ordered = sorted(rows, key=_threshold_row_sort_key)
    days = sorted({row.market_day for row in ordered if row.market_day})
    if len(days) >= 3:
        holdout_start = days[max(1, int(len(days) * 0.70))]
        train = [row for row in ordered if (row.market_day or "") < holdout_start]
        holdout = [row for row in ordered if (row.market_day or "") >= holdout_start]
        if train and holdout:
            return train, holdout
    split_idx = max(1, int(len(ordered) * 0.70))
    return ordered[:split_idx], ordered[split_idx:]


def _score_threshold_episode_rows(rows: list[GateLearningRow], thresholds: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str, str, str], list[GateLearningRow]] = {}
    for row in rows:
        grouped.setdefault(_threshold_episode_key(row), []).append(row)
    selected: list[GateLearningRow] = []
    for episode_rows in grouped.values():
        ordered = sorted(episode_rows, key=_threshold_row_sort_key)
        first_selected = next((row for row in ordered if _row_passes_thresholds(row, thresholds)), None)
        if first_selected is not None:
            selected.append(first_selected)
    ordered_selected = sorted(selected, key=_threshold_row_sort_key)
    selected_pnls = [
        row.counterfactual_pnl_dollars or Decimal("0")
        for row in ordered_selected
        if row.counterfactual_pnl_dollars is not None
    ]
    net = sum(selected_pnls, Decimal("0"))
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for pnl in selected_pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "selected_count": len(selected_pnls),
        "episode_count": len(grouped),
        "net_pnl": net,
        "drawdown_proxy": max_drawdown,
    }


def _threshold_episode_key(row: GateLearningRow) -> tuple[str, str, str, str, str]:
    return (
        row.strategy_code or "A",
        row.series_ticker or _series_from_market_ticker(row.market_ticker) or "unknown_series",
        row.market_ticker,
        row.market_day or "unknown_day",
        row.side or "any",
    )


def _series_from_market_ticker(market_ticker: str | None) -> str | None:
    if not market_ticker:
        return None
    parts = str(market_ticker).split("-")
    return parts[0] if parts else None


def _threshold_row_sort_key(row: GateLearningRow) -> tuple[str, datetime, str]:
    return (
        row.market_day or "",
        row.decision_time or datetime.min.replace(tzinfo=UTC),
        row.room_id or "",
    )


def _row_passes_thresholds(row: GateLearningRow, thresholds: dict[str, Any]) -> bool:
    min_price = Decimal(str(thresholds.get("risk_min_contract_price_dollars", "0.25")))
    min_remaining = int(thresholds.get("strategy_min_remaining_payout_bps", 2500))
    max_spread = int(thresholds.get("trigger_max_spread_bps", 1200))
    min_confidence = float(thresholds.get("risk_min_confidence", 0.80))
    min_edge = int(thresholds.get("risk_min_edge_bps", 500))
    min_delta = float(thresholds.get("strategy_min_abs_delta_f", 8.0))
    max_edge = int(thresholds.get("risk_max_credible_edge_bps", 5000))
    return (
        row.entry_price is not None
        and row.entry_price >= min_price
        and row.remaining_payout_bps is not None
        and row.remaining_payout_bps >= min_remaining
        and row.spread_bps is not None
        and row.spread_bps <= max_spread
        and row.confidence is not None
        and row.confidence >= min_confidence
        and row.quality_adjusted_edge_bps is not None
        and row.quality_adjusted_edge_bps >= min_edge
        and row.forecast_delta_f is not None
        and abs(row.forecast_delta_f) >= min_delta
        and row.edge_bps is not None
        and row.edge_bps <= max_edge
    )


def _crypto_rows_with_policy_predictions(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    """Attach deterministic walk-forward predictions once for threshold sweeps."""
    if not rows:
        return []
    folds = _crypto_walk_forward_folds(rows, min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)))
    scored: list[dict[str, Any]] = []
    if folds:
        for fold in folds:
            model = _fit_crypto_calibration(fold["train_rows"], settings=settings, include_candidate_report=False)
            for row in fold["test_rows"]:
                scored.append({**row, "_autonomous_predicted_yes_dollars": _predict_crypto_probability(row, model)})
        return scored
    model = _fit_crypto_calibration(rows, settings=settings, include_candidate_report=False)
    return [{**row, "_autonomous_predicted_yes_dollars": _predict_crypto_probability(row, model)} for row in rows]


def _score_crypto_policy_rows(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy,
    asset_symbol: str | None = None,
) -> dict[str, Any]:
    filtered = [
        row for row in rows
        if asset_symbol is None or normalize_asset_symbol(str(row.get("asset_symbol") or "")) == normalize_asset_symbol(asset_symbol)
    ]
    if filtered and all("_autonomous_predicted_yes_dollars" in row for row in filtered):
        selected_pnls: list[Decimal] = []
        selected_rows = 0
        for row in filtered:
            fair = _decimal(row["_autonomous_predicted_yes_dollars"])
            simulated = _simulate_crypto_trade(row, fair, settings=settings, crypto_policy=crypto_policy)
            if simulated.get("status") == "fillable":
                selected_rows += 1
                selected_pnls.append(Decimal(str(simulated.get("net_pnl") or "0")))
        net = sum(selected_pnls, Decimal("0"))
        cumulative = Decimal("0")
        min_cumulative = Decimal("0")
        for pnl in selected_pnls:
            cumulative += pnl
            min_cumulative = min(min_cumulative, cumulative)
        return {
            "row_count": len(filtered),
            "selected_count": selected_rows,
            "net_pnl": net,
            "drawdown_proxy": abs(min_cumulative),
        }
    folds = _crypto_walk_forward_folds(filtered, min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)))
    selected_pnls: list[Decimal] = []
    selected_rows = 0
    if folds:
        for fold in folds:
            model = _fit_crypto_calibration(fold["train_rows"], settings=settings, include_candidate_report=False)
            for row in fold["test_rows"]:
                fair = _predict_crypto_probability(row, model)
                simulated = _simulate_crypto_trade(row, fair, settings=settings, crypto_policy=crypto_policy)
                if simulated.get("status") == "fillable":
                    selected_rows += 1
                    selected_pnls.append(Decimal(str(simulated.get("net_pnl") or "0")))
    else:
        model = _fit_crypto_calibration(filtered, settings=settings, include_candidate_report=False)
        for row in filtered:
            fair = _predict_crypto_probability(row, model)
            simulated = _simulate_crypto_trade(row, fair, settings=settings, crypto_policy=crypto_policy)
            if simulated.get("status") == "fillable":
                selected_rows += 1
                selected_pnls.append(Decimal(str(simulated.get("net_pnl") or "0")))
    net = sum(selected_pnls, Decimal("0"))
    cumulative = Decimal("0")
    min_cumulative = Decimal("0")
    for pnl in selected_pnls:
        cumulative += pnl
        min_cumulative = min(min_cumulative, cumulative)
    return {
        "row_count": len(filtered),
        "selected_count": selected_rows,
        "net_pnl": net,
        "drawdown_proxy": abs(min_cumulative),
    }


def _best_crypto_asset_candidate(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy,
    asset_symbol: str,
    min_support: int,
    current_score: dict[str, Any],
) -> dict[str, Any] | None:
    if len(rows) < min_support:
        return {
            "promotion_status": "insufficient_evidence",
            "reason": "asset_row_support_below_minimum",
            "support_count": len(rows),
        }
    current_entry = crypto_policy.entry_for_asset(asset_symbol)
    candidates: list[dict[str, Any]] = []
    for field, values in {
        "min_fee_adjusted_edge_bps": (750, 1000, 1500, 2500, 5000),
        "max_spread_bps": (100, 250, 500, 800, 1000, 1500, 2500),
        "min_confidence": (0.60, 0.70, 0.75, 0.80, 0.85, 0.90),
        "min_contract_price_dollars": (0.03, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25),
        "min_remaining_payout_bps": (500, 1000, 1500, 2000, 2500, 3000, 3500),
        "max_credible_edge_bps": (2500, 5000, 7500, 10000),
    }.items():
        for value in values:
            entry = dict(current_entry)
            entry[field] = value
            candidate_policy = _crypto_policy_with_asset_entry(
                crypto_policy,
                asset_symbol=asset_symbol,
                entry=entry,
            )
            score = _score_crypto_policy_rows(
                rows,
                settings=settings,
                crypto_policy=candidate_policy,
                asset_symbol=asset_symbol,
            )
            support = int(score["selected_count"])
            promoted = (
                support >= min_support
                and score["net_pnl"] > current_score["net_pnl"]
                and score["drawdown_proxy"] <= current_score["drawdown_proxy"]
            )
            candidates.append(
                {
                    "promotion_status": "promoted" if promoted else "not_promoted",
                    "candidate_policy": f"crypto.{asset_symbol}.{field}={value}",
                    "entry": entry,
                    "support_count": support,
                    "current_score": _json_crypto_score(current_score),
                    "candidate_score": _json_crypto_score(score),
                    "net_improvement_dollars": _money(score["net_pnl"] - current_score["net_pnl"]),
                    "promotion_reason": "walk_forward_pnl_improved_without_worse_drawdown"
                    if promoted
                    else "support_pnl_or_drawdown_rule_failed",
                }
            )
    promoted = [candidate for candidate in candidates if candidate["promotion_status"] == "promoted"]
    if promoted:
        return max(
            promoted,
            key=lambda item: (
                Decimal(str(item["net_improvement_dollars"])),
                int(item["support_count"]),
            ),
        )
    return max(
        candidates,
        key=lambda item: (
            Decimal(str(item["net_improvement_dollars"])),
            int(item["support_count"]),
        ),
        default=None,
    )


def _crypto_policy_with_asset_entry(
    crypto_policy: RuntimeCryptoPolicy,
    *,
    asset_symbol: str,
    entry: dict[str, Any],
) -> RuntimeCryptoPolicy:
    overrides = {**crypto_policy.asset_entry_overrides, normalize_asset_symbol(asset_symbol): dict(entry)}
    return RuntimeCryptoPolicy(
        min_fee_adjusted_edge_bps=crypto_policy.min_fee_adjusted_edge_bps,
        max_spread_bps=crypto_policy.max_spread_bps,
        min_confidence=crypto_policy.min_confidence,
        min_contract_price_dollars=crypto_policy.min_contract_price_dollars,
        min_remaining_payout_bps=crypto_policy.min_remaining_payout_bps,
        max_credible_edge_bps=crypto_policy.max_credible_edge_bps,
        replay_min_resolved_markets=crypto_policy.replay_min_resolved_markets,
        replay_min_trade_candidates=crypto_policy.replay_min_trade_candidates,
        replay_min_net_pl_dollars=crypto_policy.replay_min_net_pl_dollars,
        replay_max_hard_cap_breaches=crypto_policy.replay_max_hard_cap_breaches,
        replay_min_spot_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
        replay_require_calibration_better_than_mid=crypto_policy.replay_require_calibration_better_than_mid,
        replay_require_pnl_beats_market_mid=crypto_policy.replay_require_pnl_beats_market_mid,
        replay_min_pnl_advantage_dollars=crypto_policy.replay_min_pnl_advantage_dollars,
        trading_enabled=crypto_policy.trading_enabled,
        production_autonomy_enabled=crypto_policy.production_autonomy_enabled,
        asset_modes=dict(crypto_policy.asset_modes),
        asset_entry_overrides=overrides,
    )


def _json_crypto_score(score: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_count": int(score.get("row_count") or 0),
        "selected_count": int(score.get("selected_count") or 0),
        "net_pnl": _money(score.get("net_pnl") or 0),
        "drawdown_proxy": _money(score.get("drawdown_proxy") or 0),
    }


def _money(value: Any) -> str:
    decimal = Decimal(str(value))
    return str(decimal.quantize(Decimal("0.0001")))
