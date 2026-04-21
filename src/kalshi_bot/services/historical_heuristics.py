from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason, StrategyMode
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.core.schemas import (
    HeuristicCalibrationEntry,
    HeuristicPolicyAction,
    HeuristicPolicyCondition,
    HeuristicPolicyNode,
    HeuristicThresholds,
    HistoricalHeuristicPack,
)
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.signal import StrategySignal, market_spread_bps
from kalshi_bot.weather.models import WeatherMarketMapping


class HistoricalHeuristicService:
    NOTE_KEY = "heuristic_packs"
    ALLOWED_FORCE_STAND_DOWN = {
        StandDownReason.NO_ACTIONABLE_EDGE.value,
        StandDownReason.SPREAD_TOO_WIDE.value,
        StandDownReason.BOOK_EFFECTIVELY_BROKEN.value,
        StandDownReason.INSUFFICIENT_REMAINING_PAYOUT.value,
    }
    ALLOWED_STRATEGY_MODES = {
        StrategyMode.DIRECTIONAL_UNRESOLVED.value,
        StrategyMode.LATE_DAY_AVOID.value,
    }

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def default_pack(self) -> HistoricalHeuristicPack:
        return HistoricalHeuristicPack(
            version=self.settings.active_heuristic_pack_version,
            status="champion",
            source="builtin",
            description="Baseline historical heuristic pack with no learned overrides.",
            thresholds=HeuristicThresholds(
                risk_min_edge_bps=self.settings.risk_min_edge_bps,
                trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
                strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
                strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
            ),
            calibration_entries=[],
            policy_graph=[],
            agent_summary="No promoted historical heuristics are active. Use the base weather strategy and deterministic safety rules.",
            metadata={"baseline": True},
        )

    async def ensure_initialized(self, repo: PlatformRepository) -> HistoricalHeuristicPack:
        builtin = self.default_pack()
        existing = await repo.get_heuristic_pack(builtin.version)
        if existing is None:
            await repo.create_heuristic_pack(builtin)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes.setdefault("active_version", builtin.version)
        notes.setdefault("champion_version", builtin.version)
        notes.setdefault("candidate_version", None)
        notes.setdefault("previous_version", None)
        control.notes = self._replace_notes(control.notes, notes)
        await repo.update_deployment_notes(control.notes)
        return builtin

    async def get_pack(self, repo: PlatformRepository, version: str) -> HistoricalHeuristicPack:
        record = await repo.get_heuristic_pack(version)
        if record is None:
            if version == self.settings.active_heuristic_pack_version:
                builtin = self.default_pack()
                await repo.update_heuristic_pack(builtin)
                return builtin
            raise KeyError(f"Heuristic pack {version} not found")
        return HistoricalHeuristicPack.model_validate(record.payload)

    async def list_packs(self, repo: PlatformRepository, limit: int = 20) -> list[HistoricalHeuristicPack]:
        await self.ensure_initialized(repo)
        return [HistoricalHeuristicPack.model_validate(record.payload) for record in await repo.list_heuristic_packs(limit=limit)]

    async def get_active_pack(self, repo: PlatformRepository) -> HistoricalHeuristicPack:
        await self.ensure_initialized(repo)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        return await self.get_pack(repo, notes.get("active_version") or self.settings.active_heuristic_pack_version)

    async def get_candidate_pack(self, repo: PlatformRepository) -> HistoricalHeuristicPack | None:
        await self.ensure_initialized(repo)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        candidate_version = notes.get("candidate_version")
        if not candidate_version:
            return None
        return await self.get_pack(repo, candidate_version)

    async def save_pack(self, repo: PlatformRepository, pack: HistoricalHeuristicPack) -> HistoricalHeuristicPack:
        compiled = self.compile_pack(pack)
        await repo.update_heuristic_pack(compiled)
        return compiled

    async def stage_candidate(
        self,
        repo: PlatformRepository,
        *,
        candidate_version: str,
        intelligence_run_id: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        candidate = await self.get_pack(repo, candidate_version)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes["candidate_version"] = candidate_version
        notes["last_intelligence_run_id"] = intelligence_run_id
        notes["last_candidate_payload"] = payload
        await repo.update_heuristic_pack(candidate.model_copy(update={"status": "candidate"}))
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    async def promote_candidate(
        self,
        repo: PlatformRepository,
        *,
        candidate_version: str,
        intelligence_run_id: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        candidate = await self.get_pack(repo, candidate_version)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        previous_version = notes.get("active_version") or self.settings.active_heuristic_pack_version
        previous_pack = await self.get_pack(repo, previous_version)
        notes["previous_version"] = previous_version
        notes["active_version"] = candidate_version
        notes["champion_version"] = candidate_version
        notes["candidate_version"] = None
        notes["last_intelligence_run_id"] = intelligence_run_id
        notes["last_promotion_payload"] = payload
        await repo.update_heuristic_pack(previous_pack.model_copy(update={"status": "historical"}))
        await repo.update_heuristic_pack(candidate.model_copy(update={"status": "champion"}))
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    async def rollback(
        self,
        repo: PlatformRepository,
        *,
        reason: str,
    ) -> dict[str, Any]:
        await self.ensure_initialized(repo)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        previous_version = notes.get("previous_version") or self.settings.active_heuristic_pack_version
        active_version = notes.get("active_version") or self.settings.active_heuristic_pack_version
        active_pack = await self.get_pack(repo, active_version)
        previous_pack = await self.get_pack(repo, previous_version)
        notes["active_version"] = previous_version
        notes["champion_version"] = previous_version
        notes["candidate_version"] = None
        notes["last_rollback_reason"] = reason
        await repo.update_heuristic_pack(active_pack.model_copy(update={"status": "historical"}))
        await repo.update_heuristic_pack(previous_pack.model_copy(update={"status": "champion"}))
        await repo.update_deployment_notes(self._replace_notes(control.notes, notes))
        return notes

    def compile_pack(self, pack: HistoricalHeuristicPack) -> HistoricalHeuristicPack:
        thresholds = pack.thresholds.model_copy(deep=True)
        thresholds.risk_min_edge_bps = self._clamp_int(thresholds.risk_min_edge_bps, 100, 500)
        thresholds.trigger_max_spread_bps = self._clamp_int(thresholds.trigger_max_spread_bps, 50, 2500)
        thresholds.strategy_quality_edge_buffer_bps = self._clamp_int(thresholds.strategy_quality_edge_buffer_bps, 0, 500)
        thresholds.strategy_min_remaining_payout_bps = self._clamp_int(thresholds.strategy_min_remaining_payout_bps, 300, 5000)
        calibrations = [self._compiled_calibration(entry) for entry in pack.calibration_entries]
        policy_graph = [self._compiled_policy_node(node) for node in pack.policy_graph]
        return pack.model_copy(update={"thresholds": thresholds, "calibration_entries": calibrations, "policy_graph": policy_graph})

    def apply_to_signal(
        self,
        *,
        pack: HistoricalHeuristicPack | None,
        mapping: WeatherMarketMapping,
        signal: StrategySignal,
        market_snapshot: dict[str, Any],
        reference_time: datetime,
        base_thresholds: RuntimeThresholds,
        market_stale: bool = False,
        research_stale: bool = False,
        coverage_class: str | None = None,
        candidate_pack_id: str | None = None,
    ) -> dict[str, Any]:
        active_pack = self.compile_pack(pack or self.default_pack())
        context = self._context(
            mapping=mapping,
            signal=signal,
            market_snapshot=market_snapshot,
            reference_time=reference_time,
            market_stale=market_stale,
            research_stale=research_stale,
            coverage_class=coverage_class,
        )
        effective_thresholds = {
            "risk_min_edge_bps": active_pack.thresholds.risk_min_edge_bps or base_thresholds.risk_min_edge_bps,
            "trigger_max_spread_bps": active_pack.thresholds.trigger_max_spread_bps or base_thresholds.trigger_max_spread_bps,
            "strategy_quality_edge_buffer_bps": (
                active_pack.thresholds.strategy_quality_edge_buffer_bps or base_thresholds.strategy_quality_edge_buffer_bps
            ),
            "strategy_min_remaining_payout_bps": (
                active_pack.thresholds.strategy_min_remaining_payout_bps or base_thresholds.strategy_min_remaining_payout_bps
            ),
        }
        fair_yes_adjust_bps = self._calibration_adjustment(active_pack.calibration_entries, context)
        rule_trace: list[dict[str, Any]] = []
        strategy_mode = signal.strategy_mode.value
        force_stand_down_reason: str | None = None

        for node in sorted(active_pack.policy_graph, key=lambda item: (item.priority, item.rule_id)):
            if not self._node_matches(node, context):
                continue
            action = node.action
            if action.fair_yes_adjust_bps is not None:
                fair_yes_adjust_bps += action.fair_yes_adjust_bps
            if action.risk_min_edge_bps is not None:
                effective_thresholds["risk_min_edge_bps"] = action.risk_min_edge_bps
            if action.trigger_max_spread_bps is not None:
                effective_thresholds["trigger_max_spread_bps"] = action.trigger_max_spread_bps
            if action.strategy_quality_edge_buffer_bps is not None:
                effective_thresholds["strategy_quality_edge_buffer_bps"] = action.strategy_quality_edge_buffer_bps
            if action.strategy_min_remaining_payout_bps is not None:
                effective_thresholds["strategy_min_remaining_payout_bps"] = action.strategy_min_remaining_payout_bps
            if action.recommended_strategy_mode is not None:
                strategy_mode = action.recommended_strategy_mode.value
            if action.force_stand_down_reason is not None:
                force_stand_down_reason = action.force_stand_down_reason.value
            rule_trace.append(
                {
                    "rule_id": node.rule_id,
                    "description": node.description,
                    "priority": node.priority,
                    "support_count": node.support_count,
                    "action": node.action.model_dump(mode="json"),
                }
            )

        adjusted = quantize_price(
            min(
                Decimal("1.0000"),
                max(Decimal("0.0000"), signal.fair_yes_dollars + (Decimal(fair_yes_adjust_bps) / Decimal("10000"))),
            )
        )
        return {
            "heuristic_pack_version": active_pack.version,
            "intelligence_run_id": str(active_pack.metadata.get("intelligence_run_id") or ""),
            "candidate_pack_id": candidate_pack_id or (active_pack.version if active_pack.status == "candidate" else None),
            "support_window": dict(active_pack.metadata.get("support_window") or {}),
            "agent_summary": active_pack.agent_summary,
            "fair_yes_adjust_bps": int(fair_yes_adjust_bps),
            "adjusted_fair_yes_dollars": str(adjusted),
            "thresholds": effective_thresholds,
            "recommended_strategy_mode": strategy_mode,
            "force_stand_down_reason": force_stand_down_reason,
            "rule_trace": rule_trace,
            "context": context,
        }

    def runtime_thresholds(
        self,
        *,
        base_thresholds: RuntimeThresholds,
        application: dict[str, Any] | None,
    ) -> RuntimeThresholds:
        payload = dict((application or {}).get("thresholds") or {})
        return RuntimeThresholds(
            risk_min_edge_bps=int(payload.get("risk_min_edge_bps") or base_thresholds.risk_min_edge_bps),
            risk_max_order_notional_dollars=base_thresholds.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=base_thresholds.risk_max_position_notional_dollars,
            risk_safe_capital_reserve_ratio=base_thresholds.risk_safe_capital_reserve_ratio,
            risk_risky_capital_max_ratio=base_thresholds.risk_risky_capital_max_ratio,
            trigger_max_spread_bps=int(payload.get("trigger_max_spread_bps") or base_thresholds.trigger_max_spread_bps),
            trigger_cooldown_seconds=base_thresholds.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=int(
                payload.get("strategy_quality_edge_buffer_bps") or base_thresholds.strategy_quality_edge_buffer_bps
            ),
            strategy_min_remaining_payout_bps=int(
                payload.get("strategy_min_remaining_payout_bps") or base_thresholds.strategy_min_remaining_payout_bps
            ),
        )

    def status_payload(
        self,
        *,
        control: DeploymentControl,
        active_pack: HistoricalHeuristicPack,
        candidate_pack: HistoricalHeuristicPack | None,
        recent_promotions: list[Any],
        recent_runs: list[Any],
        patch_suggestions: list[Any],
    ) -> dict[str, Any]:
        notes = self._notes(control)
        return {
            "active_version": notes.get("active_version"),
            "candidate_version": notes.get("candidate_version"),
            "previous_version": notes.get("previous_version"),
            "last_intelligence_run_id": notes.get("last_intelligence_run_id"),
            "active_pack": active_pack.model_dump(mode="json"),
            "candidate_pack": candidate_pack.model_dump(mode="json") if candidate_pack is not None else None,
            "recent_promotions": [record.payload for record in recent_promotions],
            "recent_runs": [record.payload for record in recent_runs],
            "patch_suggestions": [record.payload for record in patch_suggestions],
        }

    def next_candidate_version(self) -> str:
        return datetime.now(UTC).strftime("heuristic-%Y%m%dT%H%M%SZ")

    def _compiled_calibration(self, entry: HeuristicCalibrationEntry) -> HeuristicCalibrationEntry:
        return entry.model_copy(
            update={
                "fair_yes_adjust_bps": self._clamp_int(entry.fair_yes_adjust_bps, -2500, 2500) or 0,
                "support_count": max(0, int(entry.support_count)),
            }
        )

    def _compiled_policy_node(self, node: HeuristicPolicyNode) -> HeuristicPolicyNode:
        action = node.action.model_copy(deep=True)
        action.fair_yes_adjust_bps = self._clamp_int(action.fair_yes_adjust_bps, -2500, 2500)
        action.risk_min_edge_bps = self._clamp_int(action.risk_min_edge_bps, 100, 500)
        action.trigger_max_spread_bps = self._clamp_int(action.trigger_max_spread_bps, 50, 2500)
        action.strategy_quality_edge_buffer_bps = self._clamp_int(action.strategy_quality_edge_buffer_bps, 0, 500)
        action.strategy_min_remaining_payout_bps = self._clamp_int(action.strategy_min_remaining_payout_bps, 300, 5000)
        if action.recommended_strategy_mode is not None and action.recommended_strategy_mode.value not in self.ALLOWED_STRATEGY_MODES:
            action.recommended_strategy_mode = StrategyMode.LATE_DAY_AVOID
        if action.force_stand_down_reason is not None and action.force_stand_down_reason.value not in self.ALLOWED_FORCE_STAND_DOWN:
            action.force_stand_down_reason = StandDownReason.NO_ACTIONABLE_EDGE
        return node.model_copy(update={"priority": max(0, int(node.priority)), "support_count": max(0, int(node.support_count)), "action": action})

    def _calibration_adjustment(self, entries: list[HeuristicCalibrationEntry], context: dict[str, Any]) -> int:
        matches = [entry for entry in entries if self._calibration_matches(entry, context)]
        if not matches:
            return 0
        weighted = sum(entry.fair_yes_adjust_bps * max(1, entry.support_count) for entry in matches)
        total_weight = sum(max(1, entry.support_count) for entry in matches)
        return int(weighted / total_weight) if total_weight else 0

    def _calibration_matches(self, entry: HeuristicCalibrationEntry, context: dict[str, Any]) -> bool:
        return all(
            (
                entry.series_ticker in (None, "", context["series_ticker"]),
                entry.city_bucket in (None, "", context["city_bucket"]),
                entry.threshold_bucket in (None, "", context["threshold_bucket"]),
                entry.daypart in (None, "", context["daypart"]),
                entry.forecast_delta_bucket in (None, "", context["forecast_delta_bucket"]),
            )
        )

    def _node_matches(self, node: HeuristicPolicyNode, context: dict[str, Any]) -> bool:
        condition = node.condition
        return all(
            (
                self._matches_list(condition.market_tickers, context["market_ticker"]),
                self._matches_list(condition.series_tickers, context["series_ticker"]),
                self._matches_list(condition.city_buckets, context["city_bucket"]),
                self._matches_list(condition.dayparts, context["daypart"]),
                self._matches_list(condition.threshold_buckets, context["threshold_bucket"]),
                self._matches_list(condition.forecast_delta_buckets, context["forecast_delta_bucket"]),
                self._matches_list(condition.spread_regimes, context["spread_regime"]),
                self._matches_list(condition.coverage_classes, context["coverage_class"]),
                self._matches_list(condition.resolution_states, context["resolution_state"]),
                self._matches_list(condition.market_stale_values, context["market_stale"]),
                self._matches_list(condition.research_stale_values, context["research_stale"]),
            )
        )

    def _context(
        self,
        *,
        mapping: WeatherMarketMapping,
        signal: StrategySignal,
        market_snapshot: dict[str, Any],
        reference_time: datetime,
        market_stale: bool,
        research_stale: bool,
        coverage_class: str | None,
    ) -> dict[str, Any]:
        forecast_high = getattr(signal.weather, "forecast_high_f", None) if signal.weather is not None else None
        threshold_f = mapping.threshold_f
        forecast_delta = (
            float(forecast_high) - float(threshold_f)
            if forecast_high is not None and threshold_f is not None
            else None
        )
        return {
            "market_ticker": mapping.market_ticker,
            "series_ticker": mapping.series_ticker,
            "city_bucket": mapping.location_name,
            "daypart": self._daypart(mapping, reference_time),
            "threshold_bucket": self._threshold_bucket(mapping.threshold_f),
            "forecast_delta_bucket": self._forecast_delta_bucket(forecast_delta),
            "spread_regime": self._spread_regime(market_spread_bps(market_snapshot)),
            "coverage_class": coverage_class or "live_runtime",
            "resolution_state": signal.resolution_state.value,
            "market_stale": bool(market_stale),
            "research_stale": bool(research_stale),
        }

    def _daypart(self, mapping: WeatherMarketMapping, reference_time: datetime) -> str:
        timezone = ZoneInfo(mapping.timezone_name or "UTC")
        local_time = reference_time.astimezone(timezone)
        if local_time.hour < 12:
            return "morning"
        if local_time.hour < 16:
            return "midday"
        return "late"

    @staticmethod
    def _threshold_bucket(threshold_f: float | int | None) -> str:
        if threshold_f is None:
            return "unknown"
        value = float(threshold_f)
        if value < 60:
            return "lt60"
        if value < 70:
            return "60s"
        if value < 80:
            return "70s"
        return "80plus"

    @staticmethod
    def _forecast_delta_bucket(delta: float | None) -> str:
        if delta is None:
            return "unknown"
        if delta <= -8:
            return "minus_8_plus"
        if delta <= -3:
            return "minus_3_to_7"
        if delta < 3:
            return "flat"
        if delta < 8:
            return "plus_3_to_7"
        return "plus_8_plus"

    @staticmethod
    def _spread_regime(spread_bps: int | None) -> str:
        if spread_bps is None:
            return "unknown"
        if spread_bps <= 150:
            return "tight"
        if spread_bps <= 500:
            return "tradable"
        if spread_bps <= 1200:
            return "wide"
        return "broken"

    @staticmethod
    def _matches_list(values: list[Any], current: Any) -> bool:
        return not values or current in values

    @staticmethod
    def _clamp_int(value: int | None, low: int, high: int) -> int | None:
        if value is None:
            return None
        return max(low, min(high, int(value)))

    @classmethod
    def _notes(cls, control: DeploymentControl) -> dict[str, Any]:
        existing = dict(control.notes or {})
        heuristic_notes = dict(existing.get(cls.NOTE_KEY) or {})
        existing[cls.NOTE_KEY] = heuristic_notes
        return heuristic_notes

    @classmethod
    def _replace_notes(cls, notes: dict[str, Any], heuristic_notes: dict[str, Any]) -> dict[str, Any]:
        updated = dict(notes or {})
        updated[cls.NOTE_KEY] = heuristic_notes
        return updated
