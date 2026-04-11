from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, DeploymentColor
from kalshi_bot.core.schemas import (
    AgentPack,
    AgentPackThresholds,
    EvaluationMetrics,
    EvaluationSummary,
    SelfImproveCritiqueItem,
    TrainingRoomBundle,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService


@dataclass(slots=True)
class SelfImproveResult:
    status: str
    payload: dict[str, Any]


class SelfImproveService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        providers: ProviderRouter,
        training_export_service: TrainingExportService,
        training_corpus_service: TrainingCorpusService | None,
        agent_pack_service: AgentPackService,
        risk_engine: DeterministicRiskEngine,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.providers = providers
        self.training_export_service = training_export_service
        self.training_corpus_service = training_corpus_service
        self.agent_pack_service = agent_pack_service
        self.risk_engine = risk_engine

    async def get_status(self) -> dict[str, Any]:
        if self.training_corpus_service is None:
            raise RuntimeError("Training corpus service is not configured")
        readiness = await self.training_corpus_service.compute_readiness(persist=False)
        dataset_builds = [
            build.model_dump(mode="json")
            for build in await self.training_corpus_service.list_builds(limit=5)
        ]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            notes = dict(control.notes.get("agent_packs") or {})
            packs = [record.payload for record in await repo.list_agent_packs(limit=10)]
            critiques = await repo.list_critique_runs(limit=5)
            evaluations = await repo.list_evaluation_runs(limit=5)
            promotions = await repo.list_promotion_events(limit=5)
            await session.commit()
        return {
            "active_color": control.active_color,
            "agent_packs": notes,
            "recent_packs": packs,
            "recent_critiques": [
                {
                    "id": record.id,
                    "status": record.status,
                    "source_pack_version": record.source_pack_version,
                    "candidate_version": record.candidate_version,
                    "room_count": record.room_count,
                }
                for record in critiques
            ],
            "recent_evaluations": [
                {
                    "id": record.id,
                    "status": record.status,
                    "candidate_version": record.candidate_version,
                    "champion_version": record.champion_version,
                    "passed": record.passed,
                }
                for record in evaluations
            ],
            "recent_promotions": [
                {
                    "id": record.id,
                    "status": record.status,
                    "candidate_version": record.candidate_version,
                    "target_color": record.target_color,
                    "rollback_reason": record.rollback_reason,
                }
                for record in promotions
            ],
            "training_readiness": readiness.model_dump(mode="json"),
            "recent_dataset_builds": dataset_builds,
        }

    async def critique_recent_rooms(self, *, days: int | None = None, limit: int = 200) -> SelfImproveResult:
        days = days or self.settings.self_improve_window_days
        if self.training_corpus_service is None:
            raise RuntimeError("Training corpus service is not configured")
        readiness = await self.training_corpus_service.compute_readiness(persist=True)
        if not readiness.ready_for_critique:
            raise ValueError(f"Training corpus is not ready for critique: {', '.join(readiness.missing_indicators)}")
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            champion_pack = await self.agent_pack_service.get_active_pack(repo)
            room_ids = await self.training_corpus_service.select_learning_room_ids(
                days=days,
                limit=limit,
                settled_only=False,
                good_research_only=True,
            )
            critique_run = await repo.create_critique_run(
                source_pack_version=champion_pack.version,
                payload={"days": days, "room_ids": room_ids},
            )
            await session.commit()

        try:
            bundles = await self.training_export_service.export_room_bundles(room_ids=room_ids, limit=len(room_ids), include_non_complete=False)
            critiques = [await self._critique_bundle(champion_pack, bundle) for bundle in bundles]
            candidate = await self._build_candidate_pack(champion_pack, critiques, critique_run_id=critique_run.id)
            candidate = self.agent_pack_service.sanitize_candidate_pack(candidate, parent_version=champion_pack.version)
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.update_agent_pack(candidate)
                await repo.complete_critique_run(
                    critique_run.id,
                    status="completed",
                    payload={
                        "criticisms": [item.model_dump(mode="json") for item in critiques],
                        "candidate_version": candidate.version,
                    },
                    candidate_version=candidate.version,
                    room_count=len(bundles),
                )
                await session.commit()
            return SelfImproveResult(
                status="completed",
                payload={"critique_run_id": critique_run.id, "candidate_version": candidate.version, "room_count": len(bundles)},
            )
        except Exception as exc:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_critique_run(
                    critique_run.id,
                    status="failed",
                    payload={},
                    error_text=str(exc),
                )
                await session.commit()
            raise

    async def evaluate_candidate(
        self,
        *,
        candidate_version: str,
        days: int | None = None,
        limit: int = 200,
    ) -> SelfImproveResult:
        days = days or self.settings.self_improve_window_days
        if self.training_corpus_service is None:
            raise RuntimeError("Training corpus service is not configured")
        readiness = await self.training_corpus_service.compute_readiness(persist=True)
        if not readiness.ready_for_evaluation:
            raise ValueError(f"Training corpus is not ready for evaluation: {', '.join(readiness.missing_indicators)}")
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            champion_pack = await self.agent_pack_service.get_active_pack(repo)
            candidate_pack = await self.agent_pack_service.get_pack(repo, candidate_version)
            room_ids = await self.training_corpus_service.select_learning_room_ids(
                days=days,
                limit=limit,
                settled_only=False,
                good_research_only=True,
            )
            holdout_ids = self._holdout_room_ids(room_ids)
            evaluation = await repo.create_evaluation_run(
                champion_version=champion_pack.version,
                candidate_version=candidate_pack.version,
                payload={"days": days, "holdout_ids": holdout_ids},
            )
            await session.commit()

        try:
            bundles = await self.training_export_service.export_room_bundles(room_ids=holdout_ids, limit=len(holdout_ids), include_non_complete=False)
            champion_metrics = await self._evaluate_pack(champion_pack, bundles)
            candidate_metrics = await self._evaluate_pack(candidate_pack, bundles)
            summary = self._evaluation_summary(
                champion_version=champion_pack.version,
                candidate_version=candidate_pack.version,
                champion_metrics=champion_metrics,
                candidate_metrics=candidate_metrics,
            )
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_evaluation_run(
                    evaluation.id,
                    summary=summary,
                    holdout_room_count=len(bundles),
                )
                await session.commit()
            return SelfImproveResult(
                status="completed",
                payload={"evaluation_run_id": evaluation.id, **summary.model_dump(mode="json")},
            )
        except Exception as exc:
            failed_summary = EvaluationSummary(candidate_version=candidate_version, champion_version="unknown", reasons=[str(exc)])
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_evaluation_run(
                    evaluation.id,
                    summary=failed_summary,
                    holdout_room_count=0,
                    error_text=str(exc),
                )
                await session.commit()
            raise

    async def promote_candidate(self, *, evaluation_run_id: str, reason: str = "auto_promote") -> SelfImproveResult:
        if self.training_corpus_service is None:
            raise RuntimeError("Training corpus service is not configured")
        readiness = await self.training_corpus_service.compute_readiness(persist=True)
        if not readiness.ready_for_promotion:
            raise ValueError(f"Training corpus is not ready for promotion: {', '.join(readiness.missing_indicators)}")
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            evaluation = await repo.get_evaluation_run(evaluation_run_id)
            if evaluation is None:
                raise KeyError(f"Evaluation run {evaluation_run_id} not found")
            summary = EvaluationSummary.model_validate(evaluation.payload)
            if not evaluation.passed or not summary.passed:
                raise ValueError(f"Evaluation run {evaluation_run_id} did not pass")
            inactive_color = self.agent_pack_service.inactive_color(control)
            previous_pack = await self.agent_pack_service.get_active_pack(repo)
            promotion = await repo.create_promotion_event(
                candidate_version=summary.candidate_version,
                previous_version=previous_pack.version,
                target_color=inactive_color,
                evaluation_run_id=evaluation_run_id,
                payload={"reason": reason, "summary": summary.model_dump(mode="json")},
                status="staged",
            )
            notes = await self.agent_pack_service.stage_candidate(
                repo,
                candidate_version=summary.candidate_version,
                inactive_color=inactive_color,
                evaluation_run_id=evaluation_run_id,
                promotion_event_id=promotion.id,
                previous_version=previous_pack.version,
            )
            await session.commit()
        return SelfImproveResult(
            status="staged",
            payload={
                "promotion_event_id": promotion.id,
                "inactive_color": inactive_color,
                "candidate_version": summary.candidate_version,
                "notes": notes,
            },
        )

    async def rollback(self, *, reason: str = "manual_rollback") -> SelfImproveResult:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            notes = dict(control.notes.get("agent_packs") or {})
            live_monitor = dict(notes.get("live_monitor") or {})
            canary = dict(notes.get("canary") or {})
            previous_version = live_monitor.get("previous_version") or canary.get("previous_version") or notes.get("champion_version")
            if not previous_version:
                raise ValueError("No previous agent pack version is available for rollback")
            target_color = control.active_color
            await self.agent_pack_service.assign_pack_to_color(repo, color=target_color, version=previous_version)
            if canary.get("color"):
                await self.agent_pack_service.assign_pack_to_color(repo, color=canary["color"], version=previous_version)
            notes["candidate_version"] = None
            notes["canary"] = None
            notes["live_monitor"] = None
            notes["champion_version"] = previous_version
            notes["active_version"] = previous_version
            await repo.update_deployment_notes({"agent_packs": notes})
            if live_monitor.get("promotion_event_id"):
                await repo.update_promotion_event(
                    live_monitor["promotion_event_id"],
                    status="rolled_back",
                    rollback_reason=reason,
                )
            if canary.get("promotion_event_id"):
                await repo.update_promotion_event(
                    canary["promotion_event_id"],
                    status="rolled_back",
                    rollback_reason=reason,
                )
            await session.commit()
        return SelfImproveResult(status="rolled_back", payload={"version": previous_version, "reason": reason})

    async def monitor_rollouts(self) -> SelfImproveResult:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.agent_pack_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            notes = dict(control.notes.get("agent_packs") or {})
            canary = dict(notes.get("canary") or {})
            live_monitor = dict(notes.get("live_monitor") or {})
            await session.commit()

        if canary.get("status") == "running":
            return await self._monitor_canary(control.active_color, canary)
        if live_monitor.get("status") == "running":
            return await self._monitor_live(live_monitor)
        return SelfImproveResult(status="idle", payload={"message": "no active rollout"})

    async def _monitor_canary(self, active_color: str, canary: dict[str, Any]) -> SelfImproveResult:
        started_at = datetime.fromisoformat(canary["started_at"])
        target_color = canary["color"]
        candidate_version = canary["version"]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room_ids = [
                room.id
                for room in await repo.list_rooms_for_learning(
                    since=started_at,
                    limit=500,
                    pack_version=candidate_version,
                    color=target_color,
                )
            ]
            await session.commit()
        bundles = await self.training_export_service.export_room_bundles(room_ids=room_ids, limit=len(room_ids), include_non_complete=False)
        metrics = self._room_guardrail_metrics(bundles)
        enough_rooms = len(bundles) >= int(canary.get("required_rooms") or self.settings.self_improve_canary_min_rooms)
        enough_time = (datetime.now(UTC) - started_at).total_seconds() >= int(
            canary.get("min_seconds") or self.settings.self_improve_canary_min_seconds
        )
        failure_reason = self._guardrail_failure_reason(metrics)
        if failure_reason is not None:
            rollback = await self.rollback(reason=f"canary_failure: {failure_reason}")
            return SelfImproveResult(status="canary_failed", payload={"metrics": metrics, "rollback": rollback.payload})
        if enough_rooms and enough_time:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                previous_version = dict(control.notes.get("agent_packs") or {}).get("champion_version")
                await repo.set_active_color(target_color)
                await self.agent_pack_service.mark_live_monitor(
                    repo,
                    promoted_version=candidate_version,
                    previous_version=previous_version,
                    promotion_event_id=canary["promotion_event_id"],
                )
                await repo.update_promotion_event(canary["promotion_event_id"], status="live")
                await session.commit()
            return SelfImproveResult(status="canary_promoted", payload={"metrics": metrics, "candidate_version": candidate_version})
        return SelfImproveResult(
            status="canary_running",
            payload={"metrics": metrics, "completed_rooms": len(bundles), "color": target_color, "candidate_version": candidate_version},
        )

    async def _monitor_live(self, live_monitor: dict[str, Any]) -> SelfImproveResult:
        started_at = datetime.fromisoformat(live_monitor["started_at"])
        version = live_monitor["version"]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            room_ids = [
                room.id
                for room in await repo.list_rooms_for_learning(
                    since=started_at,
                    limit=500,
                    pack_version=version,
                    color=control.active_color,
                )
            ]
            await session.commit()
        bundles = await self.training_export_service.export_room_bundles(room_ids=room_ids, limit=len(room_ids), include_non_complete=False)
        metrics = self._room_guardrail_metrics(bundles)
        failure_reason = self._guardrail_failure_reason(metrics)
        ends_at = datetime.fromtimestamp(float(live_monitor["ends_at"]), tz=UTC)
        if failure_reason is not None:
            rollback = await self.rollback(reason=f"live_failure: {failure_reason}")
            return SelfImproveResult(status="live_failed", payload={"metrics": metrics, "rollback": rollback.payload})
        if datetime.now(UTC) >= ends_at:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                notes = dict(control.notes.get("agent_packs") or {})
                notes["live_monitor"] = None
                await repo.update_deployment_notes({"agent_packs": notes})
                await repo.update_promotion_event(live_monitor["promotion_event_id"], status="stable")
                await session.commit()
            return SelfImproveResult(status="live_stable", payload={"metrics": metrics, "version": version})
        return SelfImproveResult(status="live_monitoring", payload={"metrics": metrics, "version": version})

    async def _critique_bundle(self, base_pack: AgentPack, bundle: TrainingRoomBundle) -> SelfImproveCritiqueItem:
        fallback = self._fallback_critique(bundle)
        prompt = json.dumps(
            {
                "pack": base_pack.model_dump(mode="json"),
                "bundle": bundle.model_dump(mode="json"),
            },
            indent=2,
            default=str,
        )
        payload, _ = await self.providers.complete_json_with_metadata(
            role=AgentRole.RESEARCHER,
            fallback_payload=fallback.model_dump(mode="json"),
            system_prompt=base_pack.research.critique_system_prompt,
            user_prompt=prompt,
            role_config=base_pack.roles.get(AgentRole.RESEARCHER.value),
            schema_model=SelfImproveCritiqueItem,
        )
        return SelfImproveCritiqueItem.model_validate(payload)

    async def _build_candidate_pack(
        self,
        champion_pack: AgentPack,
        critiques: list[SelfImproveCritiqueItem],
        *,
        critique_run_id: str,
    ) -> AgentPack:
        fallback = self._fallback_candidate_pack(champion_pack, critiques, critique_run_id=critique_run_id)
        prompt = json.dumps(
            {
                "champion_pack": champion_pack.model_dump(mode="json"),
                "critiques": [item.model_dump(mode="json") for item in critiques],
                "instructions": {
                    "mutable_thresholds_only": [
                        "risk_min_edge_bps",
                        "risk_max_order_notional_dollars",
                        "risk_max_position_notional_dollars",
                        "trigger_max_spread_bps",
                        "trigger_cooldown_seconds",
                    ],
                    "never_change": [
                        "risk_daily_loss_limit_dollars",
                        "stale-data thresholds",
                        "kill-switch semantics",
                        "execution lock behavior",
                        "write credentials",
                        "order schema invariants",
                    ],
                },
            },
            indent=2,
            default=str,
        )
        payload = await self.providers.maybe_complete_json(
            role=AgentRole.RESEARCHER,
            fallback_payload=fallback.model_dump(mode="json"),
            system_prompt=(
                "You are designing the next Kalshi agent pack candidate. Return JSON only matching the AgentPack schema. "
                "Improve prompts and bounded thresholds for risk-safe decision quality. Do not change immutable safety controls."
            ),
            user_prompt=prompt,
            role_config=champion_pack.roles.get(AgentRole.RESEARCHER.value),
            schema_model=AgentPack,
        )
        return AgentPack.model_validate(payload)

    def _fallback_candidate_pack(
        self,
        champion_pack: AgentPack,
        critiques: list[SelfImproveCritiqueItem],
        *,
        critique_run_id: str,
    ) -> AgentPack:
        avg_direction = self._average([item.directional_agreement for item in critiques])
        avg_risk = self._average([item.risk_compliance for item in critiques])
        thresholds = champion_pack.thresholds.model_copy(deep=True)
        if avg_direction < 0.6:
            thresholds.risk_min_edge_bps = (thresholds.risk_min_edge_bps or self.settings.risk_min_edge_bps) + 10
        elif avg_direction > 0.8 and avg_risk > 0.9:
            thresholds.risk_min_edge_bps = max(5, (thresholds.risk_min_edge_bps or self.settings.risk_min_edge_bps) - 5)
        if avg_risk < 0.7:
            thresholds.risk_max_order_notional_dollars = max(
                5.0,
                float(thresholds.risk_max_order_notional_dollars or self.settings.risk_max_order_notional_dollars) - 5.0,
            )
        return champion_pack.model_copy(
            update={
                "version": self.agent_pack_service.next_candidate_version(),
                "status": "candidate",
                "source": "critique",
                "description": f"Auto-generated candidate from critique run {critique_run_id}.",
                "thresholds": thresholds,
                "metadata": {
                    **champion_pack.metadata,
                    "critique_run_id": critique_run_id,
                    "weakness_count": sum(len(item.weaknesses) for item in critiques),
                },
            }
        )

    async def _evaluate_pack(self, pack: AgentPack, bundles: list[TrainingRoomBundle]) -> EvaluationMetrics:
        scores: list[dict[str, float]] = []
        invalid_payloads = 0
        gate_violations = 0
        safety_violations = 0
        settled_scores: list[float] = []
        for bundle in bundles:
            score = await self._score_bundle(pack, bundle)
            scores.append(score)
            invalid_payloads += int(score["invalid_payload"])
            gate_violations += int(score["gate_violation"])
            safety_violations += int(score["safety_violation"])
            if "settled_pnl_score" in score:
                settled_scores.append(score["settled_pnl_score"])
        sample_size = len(scores)
        if sample_size == 0:
            return EvaluationMetrics()
        research_quality = self._average([item["research_quality"] for item in scores])
        directional_agreement = self._average([item["directional_agreement"] for item in scores])
        risk_compliance = self._average([item["risk_compliance"] for item in scores])
        memory_usefulness = self._average([item["memory_usefulness"] for item in scores])
        composite = (
            research_quality * 0.40
            + directional_agreement * 0.25
            + risk_compliance * 0.20
            + memory_usefulness * 0.15
        )
        return EvaluationMetrics(
            composite_score=composite,
            research_quality=research_quality,
            directional_agreement=directional_agreement,
            risk_compliance=risk_compliance,
            memory_usefulness=memory_usefulness,
            invalid_payload_rate=invalid_payloads / sample_size,
            gate_violation_count=gate_violations,
            safety_violation_count=safety_violations,
            settled_pnl_score=(self._average(settled_scores) if settled_scores else None),
            sample_size=sample_size,
        )

    async def _score_bundle(self, pack: AgentPack, bundle: TrainingRoomBundle) -> dict[str, float]:
        dossier = bundle.research_dossier or {}
        trader_context = dossier.get("trader_context", {}) if isinstance(dossier, dict) else {}
        gate = dossier.get("gate", {}) if isinstance(dossier, dict) else {}
        fair_yes = self._fair_yes_for_bundle(pack, bundle)
        signal = self._replay_signal(bundle.market_snapshot or {}, fair_yes, pack)
        ticket_generated = signal["recommended_side"] is not None and bool(gate.get("passed"))
        invalid_payload = 1.0 if ticket_generated and signal["target_yes_price_dollars"] is None else 0.0
        gate_violation = 1.0 if signal["recommended_side"] is not None and not bool(gate.get("passed")) else 0.0
        safety_violation = 0.0
        research_quality = self._average(
            [
                1.0 if bundle.research_sources else 0.0,
                1.0 if trader_context.get("fair_yes_dollars") not in (None, "") or fair_yes is not None else 0.0,
                1.0 if bool(dossier.get("settlement_covered")) else 0.0,
                1.0 if bool((dossier.get("summary") or {}).get("source_coverage")) else 0.0,
            ]
        )
        directional_agreement = self._directional_agreement(bundle, signal)
        risk_compliance = 1.0 if gate_violation == 0.0 and invalid_payload == 0.0 and safety_violation == 0.0 else 0.0
        memory_usefulness = await self._memory_usefulness(pack, bundle)
        result = {
            "research_quality": research_quality,
            "directional_agreement": directional_agreement,
            "risk_compliance": risk_compliance,
            "memory_usefulness": memory_usefulness,
            "invalid_payload": invalid_payload,
            "gate_violation": gate_violation,
            "safety_violation": safety_violation,
        }
        if bundle.outcome.settlement_seen and bundle.outcome.settlement_pnl_dollars is not None:
            result["settled_pnl_score"] = 1.0 if bundle.outcome.settlement_pnl_dollars >= 0 else 0.0
        return result

    def _evaluation_summary(
        self,
        *,
        champion_version: str,
        candidate_version: str,
        champion_metrics: EvaluationMetrics,
        candidate_metrics: EvaluationMetrics,
    ) -> EvaluationSummary:
        improvement = candidate_metrics.composite_score - champion_metrics.composite_score
        regressions = [
            champion_metrics.research_quality - candidate_metrics.research_quality,
            champion_metrics.directional_agreement - candidate_metrics.directional_agreement,
            champion_metrics.risk_compliance - candidate_metrics.risk_compliance,
            champion_metrics.memory_usefulness - candidate_metrics.memory_usefulness,
        ]
        max_critical_regression = max(0.0, *regressions)
        reasons: list[str] = []
        if candidate_metrics.invalid_payload_rate > 0:
            reasons.append("Candidate produced invalid payloads on holdout rooms.")
        if candidate_metrics.gate_violation_count > 0:
            reasons.append("Candidate proposed trades while research gate was false.")
        if candidate_metrics.safety_violation_count > 0:
            reasons.append("Candidate violated immutable safety controls.")
        if improvement < self.settings.self_improve_min_improvement:
            reasons.append("Candidate did not improve the composite score enough.")
        if max_critical_regression > self.settings.self_improve_max_critical_regression:
            reasons.append("Candidate regressed too far on a critical subscore.")
        return EvaluationSummary(
            candidate_version=candidate_version,
            champion_version=champion_version,
            passed=not reasons,
            improvement=improvement,
            max_critical_regression=max_critical_regression,
            candidate_metrics=candidate_metrics,
            champion_metrics=champion_metrics,
            reasons=reasons or ["Evaluation passed."],
        )

    def _holdout_room_ids(self, room_ids: list[str]) -> list[str]:
        if not room_ids:
            return []
        holdout_count = max(1, int(len(room_ids) * self.settings.self_improve_holdout_ratio))
        return room_ids[-holdout_count:]

    def _fair_yes_for_bundle(self, pack: AgentPack, bundle: TrainingRoomBundle) -> Decimal | None:
        dossier = bundle.research_dossier or {}
        trader_context = dossier.get("trader_context", {}) if isinstance(dossier, dict) else {}
        raw = trader_context.get("fair_yes_dollars")
        if raw in (None, ""):
            return None
        return Decimal(str(raw))

    def _replay_signal(self, market_snapshot: dict[str, Any], fair_yes: Decimal | None, pack: AgentPack) -> dict[str, Any]:
        market = market_snapshot.get("market", market_snapshot)
        if fair_yes is None:
            return {"recommended_side": None, "target_yes_price_dollars": None, "edge_bps": 0}
        min_edge = Decimal(str(self.agent_pack_service.runtime_thresholds(pack).risk_min_edge_bps)) / Decimal("10000")
        ask_yes = self._decimal_or_none(market.get("yes_ask_dollars"))
        ask_no = self._decimal_or_none(market.get("no_ask_dollars"))
        if ask_yes is not None and fair_yes - ask_yes >= min_edge:
            return {
                "recommended_side": "yes",
                "target_yes_price_dollars": str(ask_yes),
                "edge_bps": int(((fair_yes - ask_yes) * Decimal("10000")).to_integral_value()),
            }
        if ask_no is not None and (Decimal("1.0000") - fair_yes) - ask_no >= min_edge:
            target_yes = (Decimal("1.0000") - ask_no).quantize(Decimal("0.0001"))
            return {
                "recommended_side": "no",
                "target_yes_price_dollars": str(target_yes),
                "edge_bps": int((((Decimal("1.0000") - fair_yes) - ask_no) * Decimal("10000")).to_integral_value()),
            }
        return {"recommended_side": None, "target_yes_price_dollars": None, "edge_bps": 0}

    def _directional_agreement(self, bundle: TrainingRoomBundle, signal: dict[str, Any]) -> float:
        trade_ticket = bundle.trade_ticket or {}
        original_side = trade_ticket.get("side")
        candidate_side = signal.get("recommended_side")
        if original_side is None and candidate_side is None:
            return 1.0
        if original_side is None or candidate_side is None:
            return 0.0
        score = 1.0 if original_side == candidate_side else 0.0
        if bundle.outcome.settlement_seen and bundle.outcome.settlement_pnl_dollars is not None:
            if bundle.outcome.settlement_pnl_dollars > 0 and score == 1.0:
                return 1.0
            if bundle.outcome.settlement_pnl_dollars < 0 and score == 1.0:
                return 0.5
        return score

    async def _memory_usefulness(self, pack: AgentPack, bundle: TrainingRoomBundle) -> float:
        room = SimpleNamespace(
            name=bundle.room["name"],
            market_ticker=bundle.room["market_ticker"],
            stage=bundle.room["stage"],
            active_color=bundle.room["active_color"],
        )
        fallback = (
            f"Room {room.name} on {room.market_ticker} moved through {room.stage} with "
            f"{len(bundle.messages)} messages and a final outcome captured in the transcript."
        )
        text, _ = await self.providers.rewrite_with_metadata(
            role=AgentRole.MEMORY_LIBRARIAN,
            fallback_text=fallback,
            system_prompt=pack.memory.system_prompt,
            user_prompt=f"Summarize this room in {pack.memory.max_sentences} sentences.\n\n{fallback}",
            role_config=pack.roles.get(AgentRole.MEMORY_LIBRARIAN.value),
        )
        if not text.strip():
            return 0.0
        mentions_market = bundle.room["market_ticker"] in text
        mentions_stage = bundle.room["stage"] in text or bundle.outcome.final_status in text
        return self._average([1.0, 1.0 if mentions_market else 0.0, 1.0 if mentions_stage else 0.0])

    def _fallback_critique(self, bundle: TrainingRoomBundle) -> SelfImproveCritiqueItem:
        research_quality = 1.0 if bundle.research_dossier and bundle.research_sources else 0.5
        directional = 1.0 if bundle.trade_ticket is not None or bundle.outcome.research_gate_passed is False else 0.5
        risk = 1.0 if bundle.outcome.risk_status in {None, "approved", "blocked"} else 0.5
        memory = 1.0 if bundle.memory_note is not None else 0.5
        strengths: list[str] = []
        weaknesses: list[str] = []
        if bundle.outcome.research_gate_passed:
            strengths.append("Research gate passed with a stored dossier.")
        else:
            weaknesses.append("Research gate did not pass on this room.")
        if bundle.trade_ticket is not None:
            strengths.append("A machine-readable trade ticket was stored.")
        else:
            weaknesses.append("No trade ticket was generated.")
        return SelfImproveCritiqueItem(
            room_id=bundle.room["id"],
            market_ticker=bundle.room["market_ticker"],
            research_quality=research_quality,
            directional_agreement=directional,
            risk_compliance=risk,
            memory_usefulness=memory,
            strengths=strengths,
            weaknesses=weaknesses,
            suggested_prompt_changes={},
            suggested_thresholds=AgentPackThresholds(),
        )

    def _room_guardrail_metrics(self, bundles: list[TrainingRoomBundle]) -> dict[str, float]:
        if not bundles:
            return {"research_block_rate": 0.0, "blocked_rate": 0.0, "stale_rate": 0.0, "drawdown": 0.0}
        research_blocked = 0
        blocked = 0
        stale = 0
        drawdown = Decimal("0")
        for bundle in bundles:
            if bundle.outcome.final_status == "research_blocked":
                research_blocked += 1
            if bundle.outcome.final_status in {"blocked", "research_blocked"} or bundle.outcome.risk_status == "blocked":
                blocked += 1
            reasons = (bundle.risk_verdict or {}).get("reasons", []) if isinstance(bundle.risk_verdict, dict) else []
            if any("stale" in str(reason).lower() for reason in reasons):
                stale += 1
            if bundle.outcome.settlement_pnl_dollars is not None:
                drawdown += bundle.outcome.settlement_pnl_dollars
        count = len(bundles)
        return {
            "research_block_rate": research_blocked / count,
            "blocked_rate": blocked / count,
            "stale_rate": stale / count,
            "drawdown": float(drawdown),
        }

    def _guardrail_failure_reason(self, metrics: dict[str, float]) -> str | None:
        if metrics["research_block_rate"] > self.settings.self_improve_research_gate_failure_threshold:
            return "research gate regression spike"
        if metrics["blocked_rate"] > self.settings.self_improve_blocked_order_threshold:
            return "abnormal blocked-order spike"
        if metrics["stale_rate"] > 0.5:
            return "stale-data spike"
        if metrics["drawdown"] < -abs(self.settings.risk_daily_loss_limit_dollars):
            return "drawdown breach"
        return None

    @staticmethod
    def _average(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value)).quantize(Decimal("0.0001"))
