from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, DeploymentColor
from kalshi_bot.core.schemas import (
    AgentPack,
    AgentPackMemoryConfig,
    AgentPackResearchConfig,
    AgentPackRoleConfig,
    AgentPackThresholds,
)
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository


@dataclass(slots=True)
class RuntimeThresholds:
    risk_min_edge_bps: int
    risk_max_order_notional_dollars: float | None
    risk_max_position_notional_dollars: float | None
    trigger_max_spread_bps: int
    trigger_cooldown_seconds: int
    strategy_quality_edge_buffer_bps: int
    strategy_min_remaining_payout_bps: int
    risk_safe_capital_reserve_ratio: float = 0.70
    risk_risky_capital_max_ratio: float = 0.30


class AgentPackService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def default_pack(self) -> AgentPack:
        return AgentPack(
            version=self.settings.active_agent_pack_version,
            status="champion",
            source="builtin",
            description="Built-in Gemini-first runtime pack.",
            roles={
                AgentRole.RESEARCHER.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_researcher,
                    temperature=0.2,
                    system_prompt="You are the researcher agent in a Kalshi trading room. Be factual, cite evidence, and stay concise.",
                ),
                AgentRole.PRESIDENT.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_president,
                    temperature=0.2,
                    system_prompt="You are an advisory president agent setting posture for a trading room.",
                ),
                AgentRole.TRADER.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_trader,
                    temperature=0.1,
                    system_prompt="You are the trader agent. Speak clearly and reference the deterministic rationale.",
                ),
                AgentRole.RISK_OFFICER.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_risk_officer,
                    temperature=0.1,
                    system_prompt="You are the risk officer explaining a deterministic verdict.",
                ),
                AgentRole.OPS_MONITOR.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_ops_monitor,
                    temperature=0.1,
                    system_prompt="You are the ops monitor. Report concrete issues, stale data, and operational state without embellishment.",
                ),
                AgentRole.MEMORY_LIBRARIAN.value: AgentPackRoleConfig(
                    provider="gemini",
                    model=self.settings.gemini_model_memory_librarian,
                    temperature=0.2,
                    system_prompt="You write concise trading memory notes for future retrieval.",
                ),
            },
            research=AgentPackResearchConfig(
                synthesis_system_prompt=(
                    "You are the research synthesis agent for a Kalshi trading system. "
                    "Return JSON only. Estimate fair_yes_dollars only if the cited sources support a reasoned probability view. "
                    "Do not fabricate citations. Keep unresolved_uncertainties concise."
                ),
                critique_system_prompt=(
                    "You are a strict reviewer of Kalshi agent room outputs. Score risk-safe decision quality, critique weaknesses, "
                    "and recommend prompt or bounded-threshold improvements without proposing safety bypasses."
                ),
                web_max_queries=self.settings.research_web_max_queries,
                web_max_results=self.settings.research_web_max_results,
            ),
            memory=AgentPackMemoryConfig(
                system_prompt="You write concise trading memory notes for future retrieval.",
                max_sentences=2,
            ),
            thresholds=AgentPackThresholds(
                risk_min_edge_bps=self.settings.risk_min_edge_bps,
                risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
                risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
                risk_safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
                risk_risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
                trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
                trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
            ),
        )

    async def ensure_initialized(self, repo: PlatformRepository) -> AgentPack:
        builtin = self.default_pack()
        existing = await repo.get_agent_pack(builtin.version)
        if existing is None:
            await repo.create_agent_pack(builtin)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes.setdefault("blue_version", builtin.version)
        notes.setdefault("green_version", builtin.version)
        notes.setdefault("champion_version", builtin.version)
        notes.setdefault("active_version", builtin.version)
        control.notes = self._replace_notes(control.notes, notes)
        await repo.update_deployment_notes(control.notes)
        return builtin

    async def get_pack(self, repo: PlatformRepository, version: str) -> AgentPack:
        record = await repo.get_agent_pack(version)
        if record is None:
            if version == self.settings.active_agent_pack_version:
                builtin = self.default_pack()
                await repo.update_agent_pack(builtin)
                return builtin
            raise KeyError(f"Agent pack {version} not found")
        return AgentPack.model_validate(record.payload)

    async def list_packs(self, repo: PlatformRepository, limit: int = 20) -> list[AgentPack]:
        await self.ensure_initialized(repo)
        return [AgentPack.model_validate(record.payload) for record in await repo.list_agent_packs(limit=limit)]

    async def get_pack_for_color(self, repo: PlatformRepository, color: str) -> AgentPack:
        await self.ensure_initialized(repo)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        version = notes["blue_version"] if color == DeploymentColor.BLUE.value else notes["green_version"]
        return await self.get_pack(repo, version)

    async def get_active_pack(self, repo: PlatformRepository) -> AgentPack:
        control = await repo.get_deployment_control()
        return await self.get_pack_for_color(repo, control.active_color)

    async def assign_pack_to_color(self, repo: PlatformRepository, *, color: str, version: str) -> dict[str, Any]:
        await self.get_pack(repo, version)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        if color == DeploymentColor.BLUE.value:
            notes["blue_version"] = version
        else:
            notes["green_version"] = version
        if control.active_color == color:
            notes["active_version"] = version
            notes["champion_version"] = version
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    async def stage_candidate(
        self,
        repo: PlatformRepository,
        *,
        candidate_version: str,
        inactive_color: str,
        evaluation_run_id: str | None,
        promotion_event_id: str,
        previous_version: str | None = None,
        kalshi_env: str = "demo",
    ) -> dict[str, Any]:
        # Write the pack assignment as a pending checkpoint rather than directly modifying
        # deployment_control.notes — the daemon on the inactive color applies it at startup,
        # making promotion safe across concurrent watchdog failovers.
        await repo.set_checkpoint(
            f"pending_pack_promotion:{kalshi_env}:{inactive_color}",
            cursor=None,
            payload={
                "candidate_version": candidate_version,
                "promotion_event_id": promotion_event_id,
                "staged_at": datetime.now(UTC).isoformat(),
            },
        )
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes["candidate_version"] = candidate_version
        started_at = datetime.now(UTC)
        notes["canary"] = {
            "status": "running",
            "color": inactive_color,
            "version": candidate_version,
            "started_at": started_at.isoformat(),
            "expires_at": (started_at + timedelta(seconds=self.settings.self_improve_canary_max_seconds)).isoformat(),
            "required_rooms": self.settings.self_improve_canary_min_rooms,
            "min_seconds": self.settings.self_improve_canary_min_seconds,
            "evaluation_run_id": evaluation_run_id,
            "promotion_event_id": promotion_event_id,
            "previous_version": previous_version,
        }
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    async def mark_live_monitor(
        self,
        repo: PlatformRepository,
        *,
        promoted_version: str,
        previous_version: str | None,
        promotion_event_id: str,
    ) -> dict[str, Any]:
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes["candidate_version"] = None
        notes["champion_version"] = promoted_version
        notes["active_version"] = promoted_version
        notes["live_monitor"] = {
            "status": "running",
            "version": promoted_version,
            "previous_version": previous_version,
            "promotion_event_id": promotion_event_id,
            "started_at": datetime.now(UTC).isoformat(),
            "ends_at": datetime.now(UTC).timestamp() + self.settings.self_improve_live_monitor_seconds,
        }
        notes["canary"] = None
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    def role_config(self, pack: AgentPack, role: AgentRole) -> AgentPackRoleConfig | None:
        return pack.roles.get(role.value)

    def runtime_thresholds(self, pack: AgentPack | None = None) -> RuntimeThresholds:
        thresholds = (pack.thresholds if pack is not None else AgentPackThresholds()) or AgentPackThresholds()
        return RuntimeThresholds(
            risk_min_edge_bps=thresholds.risk_min_edge_bps or self.settings.risk_min_edge_bps,
            risk_max_order_notional_dollars=(
                thresholds.risk_max_order_notional_dollars or self.settings.risk_max_order_notional_dollars
            ),
            risk_max_position_notional_dollars=(
                thresholds.risk_max_position_notional_dollars or self.settings.risk_max_position_notional_dollars
            ),
            risk_safe_capital_reserve_ratio=(
                thresholds.risk_safe_capital_reserve_ratio or self.settings.risk_safe_capital_reserve_ratio
            ),
            risk_risky_capital_max_ratio=(
                thresholds.risk_risky_capital_max_ratio or self.settings.risk_risky_capital_max_ratio
            ),
            trigger_max_spread_bps=thresholds.trigger_max_spread_bps or self.settings.trigger_max_spread_bps,
            trigger_cooldown_seconds=thresholds.trigger_cooldown_seconds or self.settings.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
        )

    def sanitize_candidate_pack(self, candidate: AgentPack, *, parent_version: str) -> AgentPack:
        thresholds = candidate.thresholds.model_copy(deep=True)
        thresholds.risk_min_edge_bps = self._clamp_int(thresholds.risk_min_edge_bps, 5, 500)
        thresholds.trigger_max_spread_bps = self._clamp_int(thresholds.trigger_max_spread_bps, 50, 2500)
        thresholds.trigger_cooldown_seconds = self._clamp_int(thresholds.trigger_cooldown_seconds, 30, 3600)
        thresholds.risk_max_order_notional_dollars = self._clamp_float(thresholds.risk_max_order_notional_dollars, 5.0, 250.0)
        thresholds.risk_max_position_notional_dollars = self._clamp_float(
            thresholds.risk_max_position_notional_dollars, 25.0, 1000.0
        )
        thresholds.risk_safe_capital_reserve_ratio = self._clamp_float(
            thresholds.risk_safe_capital_reserve_ratio, 0.0, 1.0
        )
        thresholds.risk_risky_capital_max_ratio = self._clamp_float(
            thresholds.risk_risky_capital_max_ratio, 0.0, 1.0
        )
        sanitized_roles = {
            role_name: role.model_copy(update={"temperature": max(0.0, min(1.0, role.temperature))})
            for role_name, role in candidate.roles.items()
        }
        return candidate.model_copy(
            update={
                "parent_version": parent_version,
                "thresholds": thresholds,
                "roles": sanitized_roles,
            }
        )

    def next_candidate_version(self) -> str:
        return datetime.now(UTC).strftime("candidate-%Y%m%dT%H%M%SZ")

    @staticmethod
    def inactive_color(control: DeploymentControl) -> str:
        return DeploymentColor.GREEN.value if control.active_color == DeploymentColor.BLUE.value else DeploymentColor.BLUE.value

    @staticmethod
    def _clamp_int(value: int | None, low: int, high: int) -> int | None:
        if value is None:
            return None
        return max(low, min(high, int(value)))

    @staticmethod
    def _clamp_float(value: float | None, low: float, high: float) -> float | None:
        if value is None:
            return None
        return max(low, min(high, float(value)))

    @staticmethod
    def _notes(control: DeploymentControl) -> dict[str, Any]:
        existing = dict(control.notes or {})
        agent_packs = dict(existing.get("agent_packs") or {})
        existing["agent_packs"] = agent_packs
        return agent_packs

    @staticmethod
    def _replace_notes(notes: dict[str, Any], agent_pack_notes: dict[str, Any]) -> dict[str, Any]:
        updated = dict(notes or {})
        updated["agent_packs"] = agent_pack_notes
        return updated
