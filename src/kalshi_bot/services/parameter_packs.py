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
    def _inactive_color(active_color: str) -> str:
        return DeploymentColor.GREEN.value if active_color == DeploymentColor.BLUE.value else DeploymentColor.BLUE.value
