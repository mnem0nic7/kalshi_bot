from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import AgentPack
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.backtesting import build_backtesting_report
from kalshi_bot.services.gate_learning import (
    GateLearningRow,
    GateLearningService,
    decision_corpus_row_to_gate_learning_row,
)
from kalshi_bot.services.modeling import build_modeling_report


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

    async def status(self, *, kalshi_env: str | None = None) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            await self.agent_pack_service.ensure_initialized(repo)
            checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
            control = await repo.get_deployment_control()
            active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            active_thresholds = _threshold_values(self.agent_pack_service.runtime_thresholds(active_pack))
            await session.commit()
        settings_thresholds = _settings_threshold_values(self.settings)
        checkpoint_payload = checkpoint.payload if checkpoint is not None else None
        return {
            "status": ((checkpoint_payload or {}).get("status") if checkpoint_payload is not None else "not_started"),
            "kalshi_env": env,
            "llm_calls_enabled": bool(self.settings.llm_calls_enabled),
            "deterministic_runtime": not bool(self.settings.llm_calls_enabled),
            "active_color": control.active_color,
            "active_pack_version": active_pack.version,
            "active_thresholds": active_thresholds,
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
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        run_source = source or self.settings.autonomous_gate_tuning_source
        run_days = int(days if days is not None else self.settings.autonomous_gate_tuning_days)
        support = int(min_support if min_support is not None else self.settings.autonomous_gate_tuning_min_support)
        now_utc = _as_utc(now) or datetime.now(UTC)

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            checkpoint = await repo.get_checkpoint(_checkpoint_name(env))
            payload = dict(checkpoint.payload if checkpoint is not None else {})
            if payload.get("status") == "staged":
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
            runtime_thresholds = self.agent_pack_service.runtime_thresholds(pack)
            await session.commit()

        active_settings = _settings_with_thresholds(self.settings, runtime_thresholds)
        gate_service = self.gate_learning_service_factory(active_settings)
        recommendation = await gate_service.build_recommendation_report(
            kalshi_env=env,
            days=run_days,
            source=run_source,
            min_support=support,
            now=now_utc,
        )
        candidate = _candidate_threshold_values(recommendation, runtime_thresholds)
        if not candidate["changes"]:
            return {
                "status": "no_candidate",
                "kalshi_env": env,
                "reason": "no_gate_changes_promoted",
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
            )
            await session.commit()
        return staged

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
    ) -> dict[str, Any]:
        version = f"gate-tuning-{now.strftime('%Y%m%dT%H%M%SZ')}"
        threshold_payload = current_pack.thresholds.model_dump(mode="json")
        threshold_payload.update(candidate_thresholds)
        candidate_pack = current_pack.model_copy(
            update={
                "version": version,
                "status": "staged",
                "parent_version": current_pack.version,
                "source": OPS_SOURCE,
                "description": "Autonomous gate tuning candidate from bundle-backed backtests and modeling.",
                "thresholds": current_pack.thresholds.model_copy(update=threshold_payload),
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
                    },
                },
            }
        )
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
            "source": source,
            "days": days,
            "min_support": min_support,
            "evidence_fingerprint": evidence_fingerprint,
            "evidence_fingerprint_history": history[-20:],
            "current_thresholds": current_thresholds,
            "candidate_thresholds": candidate_thresholds,
            "changes": changes,
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
            "evidence_fingerprint": evidence_fingerprint,
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
        records = await repo.list_current_decision_corpus_rows(kalshi_env=kalshi_env, limit=10000)
        rows = _canary_rows(
            [
                row
                for record in records
                if (row := decision_corpus_row_to_gate_learning_row(record)) is not None
            ],
            staged_at=staged_at,
        )
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

    async def _promote_candidate(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_payload: dict[str, Any],
        kalshi_env: str,
        canary: dict[str, Any],
        now: datetime,
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
        await repo.set_checkpoint(_checkpoint_name(kalshi_env), cursor=None, payload=payload)
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
        await repo.set_checkpoint(_checkpoint_name(kalshi_env), cursor=None, payload=payload)
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


def _checkpoint_name(kalshi_env: str) -> str:
    return f"{CHECKPOINT_PREFIX}:{kalshi_env}"


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


def _settings_with_thresholds(settings: Settings, thresholds: RuntimeThresholds) -> Settings:
    return settings.model_copy(update=_threshold_values(thresholds))


def _threshold_values(thresholds: RuntimeThresholds) -> dict[str, Any]:
    return {field: getattr(thresholds, field) for field in TUNABLE_GATE_FIELDS}


def _settings_threshold_values(settings: Settings) -> dict[str, Any]:
    return {field: getattr(settings, field) for field in TUNABLE_GATE_FIELDS}


def _threshold_drift(active_thresholds: dict[str, Any], settings_thresholds: dict[str, Any]) -> dict[str, dict[str, Any]]:
    drift: dict[str, dict[str, Any]] = {}
    for field in TUNABLE_GATE_FIELDS:
        active = active_thresholds.get(field)
        baseline = settings_thresholds.get(field)
        if active != baseline:
            drift[field] = {"settings": baseline, "active": active}
    return drift


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
) -> dict[str, Any]:
    current = _threshold_values(runtime_thresholds)
    candidate = dict(current)
    changes: dict[str, dict[str, Any]] = {}
    for field, details in dict(recommendation.get("recommended_settings") or {}).items():
        if field not in TUNABLE_GATE_FIELDS or not bool(details.get("changed")):
            continue
        recommended = details.get("recommended")
        candidate[field] = recommended
        changes[field] = {
            "current": current[field],
            "recommended": recommended,
            "candidate_policy": details.get("candidate_policy"),
            "reason": details.get("reason"),
        }
    return {
        "current_thresholds": current,
        "candidate_thresholds": candidate,
        "changes": changes,
    }


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


def _money(value: Any) -> str:
    decimal = Decimal(str(value))
    return str(decimal.quantize(Decimal("0.0001")))
