from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.models import PromotionEventRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.learning.hard_caps import HardCaps
from kalshi_bot.learning.parameter_pack import ParameterPack, default_parameter_pack, parameter_pack_from_dict
from kalshi_bot.learning.promotion_gates import (
    HoldoutMetrics,
    PromotionGateResult,
    evaluate_parameter_pack_promotion,
    promotion_gate_config_from_hard_caps,
)


@dataclass(frozen=True, slots=True)
class ParameterPackStageResult:
    status: str
    candidate_version: str
    previous_version: str
    target_color: str
    promotion_event_id: str
    gate: PromotionGateResult
    hard_caps_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "candidate_version": self.candidate_version,
            "previous_version": self.previous_version,
            "target_color": self.target_color,
            "promotion_event_id": self.promotion_event_id,
            "gate": self.gate.to_dict(),
            "hard_caps_hash": self.hard_caps_hash,
        }


@dataclass(frozen=True, slots=True)
class ParameterPackRollbackResult:
    status: str
    candidate_version: str
    previous_version: str | None
    promotion_event_id: str | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "candidate_version": self.candidate_version,
            "previous_version": self.previous_version,
            "promotion_event_id": self.promotion_event_id,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ParameterPackCanaryMetrics:
    completed_shadow_rooms: int
    elapsed_seconds: float
    brier: float
    risk_engine_bypasses: int = 0
    data_source_kill_events: int = 0
    hard_cap_touches: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ParameterPackCanaryMetrics":
        return cls(
            completed_shadow_rooms=int(payload.get("completed_shadow_rooms", payload.get("shadow_room_count", 0))),
            elapsed_seconds=float(payload.get("elapsed_seconds", 0.0)),
            brier=float(payload.get("brier", 1.0)),
            risk_engine_bypasses=int(payload.get("risk_engine_bypasses", 0)),
            data_source_kill_events=int(payload.get("data_source_kill_events", 0)),
            hard_cap_touches=int(payload.get("hard_cap_touches", 0)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed_shadow_rooms": self.completed_shadow_rooms,
            "elapsed_seconds": self.elapsed_seconds,
            "brier": self.brier,
            "risk_engine_bypasses": self.risk_engine_bypasses,
            "data_source_kill_events": self.data_source_kill_events,
            "hard_cap_touches": self.hard_cap_touches,
        }


@dataclass(frozen=True, slots=True)
class ParameterPackCanaryConfig:
    min_shadow_rooms: int = 25
    min_elapsed_seconds: int = 7200
    max_brier_ratio: float = 1.20


@dataclass(frozen=True, slots=True)
class ParameterPackCanaryResult:
    status: str
    candidate_version: str
    promotion_event_id: str | None
    passed: bool
    failures: list[str]
    comparisons: dict[str, Any]
    rollback: ParameterPackRollbackResult | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "candidate_version": self.candidate_version,
            "promotion_event_id": self.promotion_event_id,
            "passed": self.passed,
            "failures": list(self.failures),
            "comparisons": dict(self.comparisons),
            "rollback": self.rollback.to_dict() if self.rollback is not None else None,
        }


@dataclass(frozen=True, slots=True)
class ParameterPackPromotionResult:
    status: str
    candidate_version: str
    previous_version: str | None
    promotion_event_id: str | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "candidate_version": self.candidate_version,
            "previous_version": self.previous_version,
            "promotion_event_id": self.promotion_event_id,
            "reason": self.reason,
        }


class ParameterPackPromotionService:
    async def stage_candidate(
        self,
        repo: PlatformRepository,
        *,
        candidate_pack: ParameterPack,
        candidate_report: dict[str, Any],
        current_report: dict[str, Any],
        hard_caps: HardCaps,
        reason: str = "manual_parameter_pack_stage",
        target_color: str | None = None,
    ) -> ParameterPackStageResult:
        current_pack = await self._current_champion_pack(repo)
        candidate_metrics = HoldoutMetrics.from_dict(candidate_report)
        current_metrics = HoldoutMetrics.from_dict(current_report)
        self._validate_report_hashes(
            candidate_pack=candidate_pack,
            candidate_metrics=candidate_metrics,
            current_pack=current_pack,
            current_metrics=current_metrics,
        )
        gate = evaluate_parameter_pack_promotion(
            candidate=candidate_metrics,
            current=current_metrics,
            config=promotion_gate_config_from_hard_caps(hard_caps),
        )
        if not gate.passed:
            raise ValueError(f"parameter pack promotion gates failed: {', '.join(gate.failures)}")

        control = await repo.get_deployment_control()
        inactive_color = target_color or self._inactive_color(control.active_color)
        staged_pack = replace(
            candidate_pack,
            status="staged",
            parent_version=current_pack.version,
            metadata={
                **candidate_pack.metadata,
                "staged_at": datetime.now(UTC).isoformat(),
                "staged_reason": reason,
                "previous_version": current_pack.version,
                "hard_caps_hash": hard_caps.config_hash,
            },
        )
        await repo.update_parameter_pack(staged_pack, holdout_report=candidate_report)
        promotion = await repo.create_promotion_event(
            candidate_version=staged_pack.version,
            previous_version=current_pack.version,
            target_color=inactive_color,
            evaluation_run_id=None,
            payload={
                "kind": "parameter_pack",
                "reason": reason,
                "candidate_pack_hash": staged_pack.pack_hash,
                "previous_pack_hash": current_pack.pack_hash,
                "gate": gate.to_dict(),
                "candidate_report": candidate_report,
                "current_report": current_report,
                "hard_caps": {
                    "config_hash": hard_caps.config_hash,
                    "max_drawdown_pct": hard_caps.hard_caps["max_drawdown_pct"],
                },
            },
            status="staged",
        )
        await self._write_staged_notes(
            repo,
            candidate_pack=staged_pack,
            current_pack=current_pack,
            promotion=promotion,
            gate=gate,
            hard_caps=hard_caps,
            target_color=inactive_color,
        )
        return ParameterPackStageResult(
            status="staged",
            candidate_version=staged_pack.version,
            previous_version=current_pack.version,
            target_color=inactive_color,
            promotion_event_id=promotion.id,
            gate=gate,
            hard_caps_hash=hard_caps.config_hash,
        )

    async def rollback_staged(
        self,
        repo: PlatformRepository,
        *,
        reason: str = "manual_parameter_pack_rollback",
    ) -> ParameterPackRollbackResult:
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        staged = dict(notes.get("parameter_packs") or {})
        if staged.get("status") not in {"staged", "canary_pending", "canary_passed"}:
            raise ValueError("No staged parameter pack is available for rollback")
        candidate_version = str(staged.get("candidate_version") or "")
        if not candidate_version:
            raise ValueError("Staged parameter pack notes are missing candidate_version")
        promotion_event_id = staged.get("promotion_event_id")
        candidate_record = await repo.get_parameter_pack(candidate_version)
        if candidate_record is not None:
            candidate_pack = parameter_pack_from_dict(candidate_record.payload)
            rejected_pack = replace(
                candidate_pack,
                status="rejected",
                metadata={
                    **candidate_pack.metadata,
                    "rollback_reason": reason,
                    "rolled_back_at": datetime.now(UTC).isoformat(),
                },
            )
            await repo.update_parameter_pack(rejected_pack, holdout_report=candidate_record.holdout_report)
        if promotion_event_id:
            await repo.update_promotion_event(str(promotion_event_id), status="rolled_back", rollback_reason=reason)
        rolled_back_notes = {
            **staged,
            "status": "rolled_back",
            "rollback_reason": reason,
            "rolled_back_at": datetime.now(UTC).isoformat(),
        }
        notes["parameter_packs"] = rolled_back_notes
        await repo.update_deployment_notes(notes)
        return ParameterPackRollbackResult(
            status="rolled_back",
            candidate_version=candidate_version,
            previous_version=staged.get("previous_version"),
            promotion_event_id=str(promotion_event_id) if promotion_event_id else None,
            reason=reason,
        )

    async def evaluate_staged_canary(
        self,
        repo: PlatformRepository,
        *,
        canary_report: dict[str, Any],
        config: ParameterPackCanaryConfig | None = None,
    ) -> ParameterPackCanaryResult:
        cfg = config or ParameterPackCanaryConfig()
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        staged = dict(notes.get("parameter_packs") or {})
        if staged.get("status") not in {"staged", "canary_pending"}:
            raise ValueError("No staged parameter pack is available for canary evaluation")
        candidate_version = str(staged.get("candidate_version") or "")
        promotion_event_id = staged.get("promotion_event_id")
        promotion = await repo.get_promotion_event(str(promotion_event_id)) if promotion_event_id else None
        candidate_report = dict((promotion.payload if promotion is not None else {}).get("candidate_report") or {})
        holdout_brier = float(candidate_report.get("brier", 1.0))
        metrics = ParameterPackCanaryMetrics.from_dict(canary_report)
        failures, pending, comparisons = self._evaluate_canary_metrics(
            metrics,
            holdout_brier=holdout_brier,
            config=cfg,
        )

        payload = dict(promotion.payload if promotion is not None else {})
        payload["canary"] = {
            "metrics": metrics.to_dict(),
            "failures": failures,
            "pending": pending,
            "comparisons": comparisons,
            "evaluated_at": datetime.now(UTC).isoformat(),
        }
        if failures:
            if promotion is not None:
                await repo.update_promotion_event(promotion.id, status="canary_failed", payload=payload)
            rollback = await self.rollback_staged(repo, reason=f"parameter_pack_canary_failure:{','.join(failures)}")
            return ParameterPackCanaryResult(
                status="canary_failed",
                candidate_version=candidate_version,
                promotion_event_id=str(promotion_event_id) if promotion_event_id else None,
                passed=False,
                failures=failures,
                comparisons=comparisons,
                rollback=rollback,
            )
        if pending:
            if promotion is not None:
                await repo.update_promotion_event(promotion.id, status="canary_pending", payload=payload)
            staged["status"] = "canary_pending"
            staged["canary"] = payload["canary"]
            notes["parameter_packs"] = staged
            await repo.update_deployment_notes(notes)
            return ParameterPackCanaryResult(
                status="canary_pending",
                candidate_version=candidate_version,
                promotion_event_id=str(promotion_event_id) if promotion_event_id else None,
                passed=False,
                failures=pending,
                comparisons=comparisons,
            )

        if promotion is not None:
            await repo.update_promotion_event(promotion.id, status="canary_passed", payload=payload)
        candidate_record = await repo.get_parameter_pack(candidate_version)
        if candidate_record is not None:
            candidate_pack = parameter_pack_from_dict(candidate_record.payload)
            await repo.update_parameter_pack(
                replace(
                    candidate_pack,
                    status="canary_passed",
                    metadata={
                        **candidate_pack.metadata,
                        "canary_passed_at": datetime.now(UTC).isoformat(),
                    },
                ),
                holdout_report=candidate_record.holdout_report,
            )
        staged["status"] = "canary_passed"
        staged["canary"] = payload["canary"]
        notes["parameter_packs"] = staged
        await repo.update_deployment_notes(notes)
        return ParameterPackCanaryResult(
            status="canary_passed",
            candidate_version=candidate_version,
            promotion_event_id=str(promotion_event_id) if promotion_event_id else None,
            passed=True,
            failures=[],
            comparisons=comparisons,
        )

    async def promote_canary_passed(
        self,
        repo: PlatformRepository,
        *,
        reason: str = "manual_parameter_pack_promote",
    ) -> ParameterPackPromotionResult:
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        staged = dict(notes.get("parameter_packs") or {})
        if staged.get("status") != "canary_passed":
            raise ValueError("No canary-passed parameter pack is available for promotion")
        candidate_version = str(staged.get("candidate_version") or "")
        if not candidate_version:
            raise ValueError("Canary-passed parameter pack notes are missing candidate_version")
        previous_version = staged.get("previous_version")
        promotion_event_id = staged.get("promotion_event_id")
        candidate_record = await repo.get_parameter_pack(candidate_version)
        if candidate_record is None:
            raise KeyError(f"Parameter pack {candidate_version} not found")
        candidate_pack = parameter_pack_from_dict(candidate_record.payload)
        if previous_version is not None and previous_version != candidate_version:
            previous_record = await repo.get_parameter_pack(str(previous_version))
            if previous_record is not None and previous_record.status == "champion":
                previous_pack = parameter_pack_from_dict(previous_record.payload)
                await repo.update_parameter_pack(
                    replace(
                        previous_pack,
                        status="archived",
                        metadata={
                            **previous_pack.metadata,
                            "archived_at": datetime.now(UTC).isoformat(),
                            "archived_by_parameter_pack": candidate_version,
                        },
                    ),
                    holdout_report=previous_record.holdout_report,
                )
        promoted_pack = replace(
            candidate_pack,
            status="champion",
            metadata={
                **candidate_pack.metadata,
                "promoted_at": datetime.now(UTC).isoformat(),
                "promoted_reason": reason,
                "previous_version": previous_version,
            },
        )
        await repo.update_parameter_pack(promoted_pack, holdout_report=candidate_record.holdout_report)
        if promotion_event_id:
            payload = {}
            promotion = await repo.get_promotion_event(str(promotion_event_id))
            if promotion is not None:
                payload = dict(promotion.payload or {})
            payload["promoted"] = {
                "reason": reason,
                "promoted_at": datetime.now(UTC).isoformat(),
                "candidate_version": candidate_version,
                "previous_version": previous_version,
            }
            await repo.update_promotion_event(str(promotion_event_id), status="stable", payload=payload)
        promoted_notes = {
            **staged,
            "status": "champion",
            "champion_version": candidate_version,
            "champion_hash": promoted_pack.pack_hash,
            "promoted_reason": reason,
            "promoted_at": datetime.now(UTC).isoformat(),
        }
        notes["parameter_packs"] = promoted_notes
        await repo.update_deployment_notes(notes)
        return ParameterPackPromotionResult(
            status="champion",
            candidate_version=candidate_version,
            previous_version=str(previous_version) if previous_version is not None else None,
            promotion_event_id=str(promotion_event_id) if promotion_event_id else None,
            reason=reason,
        )

    async def _current_champion_pack(self, repo: PlatformRepository) -> ParameterPack:
        record = await repo.get_champion_parameter_pack()
        if record is None:
            pack = default_parameter_pack()
            await repo.update_parameter_pack(pack, holdout_report={})
            return pack
        return parameter_pack_from_dict(record.payload)

    async def _write_staged_notes(
        self,
        repo: PlatformRepository,
        *,
        candidate_pack: ParameterPack,
        current_pack: ParameterPack,
        promotion: PromotionEventRecord,
        gate: PromotionGateResult,
        hard_caps: HardCaps,
        target_color: str,
    ) -> None:
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        notes["parameter_packs"] = {
            "status": "staged",
            "candidate_version": candidate_pack.version,
            "candidate_hash": candidate_pack.pack_hash,
            "previous_version": current_pack.version,
            "previous_hash": current_pack.pack_hash,
            "target_color": target_color,
            "promotion_event_id": promotion.id,
            "staged_at": datetime.now(UTC).isoformat(),
            "gate": gate.to_dict(),
            "hard_caps": {
                "config_hash": hard_caps.config_hash,
                "max_drawdown_pct": hard_caps.hard_caps["max_drawdown_pct"],
            },
        }
        await repo.update_deployment_notes(notes)

    @staticmethod
    def _validate_report_hashes(
        *,
        candidate_pack: ParameterPack,
        candidate_metrics: HoldoutMetrics,
        current_pack: ParameterPack,
        current_metrics: HoldoutMetrics,
    ) -> None:
        if candidate_metrics.pack_hash != candidate_pack.pack_hash:
            raise ValueError("candidate_report_pack_hash_mismatch")
        if candidate_metrics.rerun_pack_hash != candidate_pack.pack_hash:
            raise ValueError("candidate_report_rerun_hash_mismatch")
        if current_metrics.pack_hash is not None and current_metrics.pack_hash != current_pack.pack_hash:
            raise ValueError("current_report_pack_hash_mismatch")
        if current_metrics.rerun_pack_hash is not None and current_metrics.rerun_pack_hash != current_pack.pack_hash:
            raise ValueError("current_report_rerun_hash_mismatch")

    @staticmethod
    def _evaluate_canary_metrics(
        metrics: ParameterPackCanaryMetrics,
        *,
        holdout_brier: float,
        config: ParameterPackCanaryConfig,
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        failures: list[str] = []
        pending: list[str] = []
        max_brier = holdout_brier * config.max_brier_ratio
        comparisons = {
            "shadow_rooms": {
                "observed": metrics.completed_shadow_rooms,
                "minimum": config.min_shadow_rooms,
            },
            "elapsed_seconds": {
                "observed": metrics.elapsed_seconds,
                "minimum": config.min_elapsed_seconds,
            },
            "brier": {
                "observed": metrics.brier,
                "holdout": holdout_brier,
                "maximum": max_brier,
            },
            "risk_engine_bypasses": metrics.risk_engine_bypasses,
            "data_source_kill_events": metrics.data_source_kill_events,
            "hard_cap_touches": metrics.hard_cap_touches,
        }
        if metrics.completed_shadow_rooms < config.min_shadow_rooms:
            pending.append("insufficient_shadow_rooms")
        if metrics.elapsed_seconds < config.min_elapsed_seconds:
            pending.append("insufficient_canary_duration")
        if metrics.brier > max_brier:
            failures.append("canary_brier_regression")
        if metrics.risk_engine_bypasses > 0:
            failures.append("risk_engine_bypass")
        if metrics.data_source_kill_events > 0:
            failures.append("data_source_kill_event")
        if metrics.hard_cap_touches > 0:
            failures.append("hard_cap_touch")
        return failures, pending, comparisons

    @staticmethod
    def _inactive_color(active_color: str) -> str:
        return DeploymentColor.GREEN.value if active_color == DeploymentColor.BLUE.value else DeploymentColor.BLUE.value
