from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.constants import CRYPTO_MIN_REMAINING_PAYOUT_BPS
from kalshi_bot.core.enums import AgentRole, DeploymentColor
from kalshi_bot.core.schemas import (
    AgentPack,
    AgentPackCryptoEntryPolicy,
    AgentPackCryptoLivePolicy,
    AgentPackCryptoPolicy,
    AgentPackCryptoReplayPolicy,
    AgentPackMemoryConfig,
    AgentPackResearchConfig,
    AgentPackRoleConfig,
    AgentPackThresholds,
    AgentPackWeatherBootstrapCaps,
    AgentPackWeatherBootstrapGridEnvelope,
    AgentPackWeatherBootstrapPolicy,
    AgentPackWeatherBootstrapTier,
    AgentPackWeatherPolicy,
    AgentPackWeatherPolicyBook,
)
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository


LEGACY_BUILTIN_AGENT_PACK_VERSION = "builtin-gemini-v1"
DETERMINISTIC_BUILTIN_AGENT_PACK_VERSION = "builtin-deterministic-v1"


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
    risk_max_credible_edge_bps: int = 10000
    risk_min_confidence: float = 0.80
    risk_min_contract_price_dollars: float = 0.25
    strategy_min_abs_delta_f: float = 8.0


@dataclass(slots=True)
class RuntimeCryptoPolicy:
    min_fee_adjusted_edge_bps: int
    max_spread_bps: int
    min_confidence: float
    min_contract_price_dollars: float
    min_remaining_payout_bps: int
    max_credible_edge_bps: int
    replay_min_resolved_markets: int
    replay_min_trade_candidates: int
    replay_min_net_pl_dollars: float
    replay_max_hard_cap_breaches: int
    replay_min_spot_coverage_pct: float
    replay_require_calibration_better_than_mid: bool
    replay_require_pnl_beats_market_mid: bool
    replay_min_pnl_advantage_dollars: float
    trading_enabled: bool
    production_autonomy_enabled: bool
    asset_modes: dict[str, str]
    asset_entry_overrides: dict[str, dict[str, Any]]

    def entry_for_asset(self, asset_symbol: str | None) -> dict[str, Any]:
        symbol = _normalize_crypto_asset_symbol(asset_symbol or "")
        base = {
            "min_fee_adjusted_edge_bps": self.min_fee_adjusted_edge_bps,
            "max_spread_bps": self.max_spread_bps,
            "min_confidence": self.min_confidence,
            "min_contract_price_dollars": self.min_contract_price_dollars,
            "min_remaining_payout_bps": self.min_remaining_payout_bps,
            "max_credible_edge_bps": self.max_credible_edge_bps,
        }
        override = self.asset_entry_overrides.get(symbol) or {}
        return {**base, **{key: value for key, value in override.items() if value is not None}}


class AgentPackService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def default_pack(self) -> AgentPack:
        return AgentPack(
            version=DETERMINISTIC_BUILTIN_AGENT_PACK_VERSION,
            status="champion",
            source="builtin_deterministic",
            description="Built-in deterministic runtime pack. All role text is generated without LLM calls.",
            roles={
                AgentRole.RESEARCHER.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic researcher role. Use structured weather, market, and trace data only.",
                ),
                AgentRole.PRESIDENT.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic president role. Summarize posture from configured gates only.",
                ),
                AgentRole.TRADER.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic trader role. Propose tickets only from the signal engine.",
                ),
                AgentRole.RISK_OFFICER.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic risk role. Report the risk engine verdict.",
                ),
                AgentRole.OPS_MONITOR.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic ops role. Report concrete operational state.",
                ),
                AgentRole.MEMORY_LIBRARIAN.value: AgentPackRoleConfig(
                    provider="none",
                    model=None,
                    temperature=0.0,
                    system_prompt="Deterministic memory role. Persist concise rule-based notes.",
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
                risk_max_credible_edge_bps=self.settings.risk_max_credible_edge_bps,
                risk_min_confidence=self.settings.risk_min_confidence,
                risk_min_contract_price_dollars=self.settings.risk_min_contract_price_dollars,
                risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
                risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
                risk_safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
                risk_risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
                trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
                trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
                strategy_min_abs_delta_f=self.settings.strategy_min_abs_delta_f,
                strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
            ),
            weather_policy=AgentPackWeatherPolicyBook(
                policies={
                    "weather/a/global/any/any/all_season/all_month/any/any/entry_gate": AgentPackWeatherPolicy(
                        policy_key="weather/a/global/any/any/all_season/all_month/any/any/entry_gate",
                        domain="weather",
                        strategy_code="A",
                        cohort_id="global",
                        series_ticker="any",
                        side="any",
                        season="all_season",
                        month="all_month",
                        regime="any",
                        threshold_band="any",
                        lane="entry_gate",
                        mode="live",
                        action="keep_current",
                        reason_codes=["builtin_weather_bootstrap_shadow_policy"],
                        deterministic_summary=(
                            "Built-in deterministic weather entry policy. Bootstrap starts shadow-only "
                            "and can change live behavior only after evidence promotion."
                        ),
                    ),
                    "weather/weather_baseline/global/any/any/all_season/all_month/any/any/entry_gate": AgentPackWeatherPolicy(
                        policy_key="weather/weather_baseline/global/any/any/all_season/all_month/any/any/entry_gate",
                        domain="weather",
                        strategy_code="weather_baseline",
                        cohort_id="global",
                        series_ticker="any",
                        side="any",
                        season="all_season",
                        month="all_month",
                        regime="any",
                        threshold_band="any",
                        lane="entry_gate",
                        mode="live",
                        action="keep_current",
                        reason_codes=["builtin_weather_bootstrap_shadow_policy"],
                        deterministic_summary=(
                            "Built-in deterministic weather entry policy. Bootstrap starts shadow-only "
                            "and can change live behavior only after evidence promotion."
                        ),
                    )
                }
            ),
            crypto_policy=AgentPackCryptoPolicy(
                entry=AgentPackCryptoEntryPolicy(
                    min_fee_adjusted_edge_bps=self.settings.risk_min_edge_bps,
                    max_spread_bps=self.settings.trigger_max_spread_bps,
                    min_confidence=self.settings.risk_min_confidence,
                    min_contract_price_dollars=self.settings.risk_min_contract_price_dollars,
                    min_remaining_payout_bps=CRYPTO_MIN_REMAINING_PAYOUT_BPS,
                    max_credible_edge_bps=self.settings.risk_max_credible_edge_bps,
                ),
                replay=AgentPackCryptoReplayPolicy(
                    min_resolved_markets=self.settings.crypto_replay_min_resolved_markets,
                    min_trade_candidates=self.settings.crypto_replay_min_trade_candidates,
                    min_net_pl_dollars=self.settings.crypto_replay_min_net_pl_dollars,
                    max_hard_cap_breaches=self.settings.crypto_replay_max_hard_cap_breaches,
                    min_spot_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
                    require_calibration_better_than_mid=self.settings.crypto_replay_require_calibration_better_than_mid,
                    require_pnl_beats_market_mid=self.settings.crypto_replay_require_pnl_beats_market_mid,
                    min_pnl_advantage_dollars=self.settings.crypto_replay_min_pnl_advantage_dollars,
                ),
                live=AgentPackCryptoLivePolicy(
                    trading_enabled=self.settings.crypto_trading_enabled,
                    production_autonomy_enabled=self.settings.crypto_production_autonomy_enabled,
                ),
            ),
        )

    async def ensure_initialized(self, repo: PlatformRepository) -> AgentPack:
        builtin = self.default_pack()
        existing = await repo.get_agent_pack(builtin.version)
        if existing is None:
            await repo.create_agent_pack(builtin)
        else:
            await repo.update_agent_pack(builtin)
        control = await repo.get_deployment_control()
        notes = self._notes(control)
        notes.setdefault("blue_version", builtin.version)
        notes.setdefault("green_version", builtin.version)
        notes.setdefault("champion_version", builtin.version)
        notes.setdefault("active_version", builtin.version)
        for key in ("blue_version", "green_version", "champion_version", "active_version"):
            if notes.get(key) == LEGACY_BUILTIN_AGENT_PACK_VERSION:
                notes[key] = builtin.version
        updated_notes = self._replace_notes(control.notes, notes)
        if control.notes != updated_notes:
            control.notes = updated_notes
            await repo.update_deployment_notes(control.notes)
        return builtin

    async def get_pack(self, repo: PlatformRepository, version: str) -> AgentPack:
        record = await repo.get_agent_pack(version)
        if record is None:
            if version in {self.settings.active_agent_pack_version, DETERMINISTIC_BUILTIN_AGENT_PACK_VERSION}:
                builtin = self.default_pack()
                await repo.update_agent_pack(builtin)
                return builtin
            if version == LEGACY_BUILTIN_AGENT_PACK_VERSION:
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
        def value_or_settings(value: Any, fallback: Any) -> Any:
            return fallback if value is None else value

        return RuntimeThresholds(
            risk_min_edge_bps=value_or_settings(thresholds.risk_min_edge_bps, self.settings.risk_min_edge_bps),
            risk_max_credible_edge_bps=value_or_settings(
                thresholds.risk_max_credible_edge_bps,
                self.settings.risk_max_credible_edge_bps,
            ),
            risk_min_confidence=value_or_settings(thresholds.risk_min_confidence, self.settings.risk_min_confidence),
            risk_min_contract_price_dollars=self._contract_price_floor(
                value_or_settings(
                    thresholds.risk_min_contract_price_dollars,
                    self.settings.risk_min_contract_price_dollars,
                )
            ),
            risk_max_order_notional_dollars=(
                value_or_settings(
                    thresholds.risk_max_order_notional_dollars,
                    self.settings.risk_max_order_notional_dollars,
                )
            ),
            risk_max_position_notional_dollars=(
                value_or_settings(
                    thresholds.risk_max_position_notional_dollars,
                    self.settings.risk_max_position_notional_dollars,
                )
            ),
            risk_safe_capital_reserve_ratio=(
                value_or_settings(
                    thresholds.risk_safe_capital_reserve_ratio,
                    self.settings.risk_safe_capital_reserve_ratio,
                )
            ),
            risk_risky_capital_max_ratio=(
                value_or_settings(
                    thresholds.risk_risky_capital_max_ratio,
                    self.settings.risk_risky_capital_max_ratio,
                )
            ),
            trigger_max_spread_bps=value_or_settings(thresholds.trigger_max_spread_bps, self.settings.trigger_max_spread_bps),
            trigger_cooldown_seconds=value_or_settings(
                thresholds.trigger_cooldown_seconds,
                self.settings.trigger_cooldown_seconds,
            ),
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_abs_delta_f=value_or_settings(
                thresholds.strategy_min_abs_delta_f,
                self.settings.strategy_min_abs_delta_f,
            ),
            strategy_min_remaining_payout_bps=(
                value_or_settings(
                    thresholds.strategy_min_remaining_payout_bps,
                    self.settings.strategy_min_remaining_payout_bps,
                )
            ),
        )

    def resolve_weather_policy(self, pack: AgentPack | None, context: Any, fallback_thresholds: RuntimeThresholds | None = None):
        from kalshi_bot.services.weather_policy import WeatherPolicyResolver

        active_pack = pack or self.default_pack()
        resolved = WeatherPolicyResolver().resolve(
            pack=active_pack,
            context=context,
            fallback_thresholds=fallback_thresholds or self.runtime_thresholds(active_pack),
        )
        return replace(resolved, thresholds=self._floor_runtime_thresholds(resolved.thresholds))

    def runtime_weather_thresholds(self, pack: AgentPack | None, context: Any) -> RuntimeThresholds:
        return self.resolve_weather_policy(pack, context).thresholds

    def runtime_crypto_policy(self, pack: AgentPack | None = None) -> RuntimeCryptoPolicy:
        policy = (pack.crypto_policy if pack is not None else AgentPackCryptoPolicy()) or AgentPackCryptoPolicy()
        entry = policy.entry or AgentPackCryptoEntryPolicy()
        replay = policy.replay or AgentPackCryptoReplayPolicy()
        live = policy.live or AgentPackCryptoLivePolicy()

        def value_or_settings(value: Any, fallback: Any) -> Any:
            return fallback if value is None else value

        overrides: dict[str, dict[str, Any]] = {}
        for raw_symbol, raw_entry in (policy.asset_entry_overrides or {}).items():
            override = {
                "min_fee_adjusted_edge_bps": raw_entry.min_fee_adjusted_edge_bps,
                "max_spread_bps": raw_entry.max_spread_bps,
                "min_confidence": raw_entry.min_confidence,
                "min_contract_price_dollars": raw_entry.min_contract_price_dollars,
                "min_remaining_payout_bps": CRYPTO_MIN_REMAINING_PAYOUT_BPS,
                "max_credible_edge_bps": raw_entry.max_credible_edge_bps,
            }
            if override["min_contract_price_dollars"] is not None:
                override["min_contract_price_dollars"] = self._contract_price_floor(
                    override["min_contract_price_dollars"]
                )
            if override["min_fee_adjusted_edge_bps"] is not None:
                override["min_fee_adjusted_edge_bps"] = max(
                    int(override["min_fee_adjusted_edge_bps"]),
                    int(self.settings.risk_min_edge_bps),
                )
            overrides[_normalize_crypto_asset_symbol(raw_symbol)] = override

        return RuntimeCryptoPolicy(
            min_fee_adjusted_edge_bps=max(
                int(value_or_settings(entry.min_fee_adjusted_edge_bps, self.settings.risk_min_edge_bps)),
                int(self.settings.risk_min_edge_bps),
            ),
            max_spread_bps=int(value_or_settings(entry.max_spread_bps, self.settings.crypto_live_max_spread_bps)),
            min_confidence=float(value_or_settings(entry.min_confidence, self.settings.risk_min_confidence)),
            min_contract_price_dollars=self._contract_price_floor(
                value_or_settings(entry.min_contract_price_dollars, self.settings.risk_min_contract_price_dollars)
            ),
            min_remaining_payout_bps=CRYPTO_MIN_REMAINING_PAYOUT_BPS,
            max_credible_edge_bps=int(
                value_or_settings(entry.max_credible_edge_bps, self.settings.risk_max_credible_edge_bps)
            ),
            replay_min_resolved_markets=int(
                value_or_settings(replay.min_resolved_markets, self.settings.crypto_replay_min_resolved_markets)
            ),
            replay_min_trade_candidates=int(
                value_or_settings(replay.min_trade_candidates, self.settings.crypto_replay_min_trade_candidates)
            ),
            replay_min_net_pl_dollars=float(
                value_or_settings(replay.min_net_pl_dollars, self.settings.crypto_replay_min_net_pl_dollars)
            ),
            replay_max_hard_cap_breaches=int(
                value_or_settings(replay.max_hard_cap_breaches, self.settings.crypto_replay_max_hard_cap_breaches)
            ),
            replay_min_spot_coverage_pct=float(
                value_or_settings(replay.min_spot_coverage_pct, self.settings.crypto_replay_min_spot_coverage_pct)
            ),
            replay_require_calibration_better_than_mid=bool(
                value_or_settings(
                    replay.require_calibration_better_than_mid,
                    self.settings.crypto_replay_require_calibration_better_than_mid,
                )
            ),
            replay_require_pnl_beats_market_mid=bool(
                value_or_settings(
                    replay.require_pnl_beats_market_mid,
                    self.settings.crypto_replay_require_pnl_beats_market_mid,
                )
            ),
            replay_min_pnl_advantage_dollars=float(
                value_or_settings(
                    replay.min_pnl_advantage_dollars,
                    self.settings.crypto_replay_min_pnl_advantage_dollars,
                )
            ),
            trading_enabled=bool(value_or_settings(live.trading_enabled, self.settings.crypto_trading_enabled)),
            production_autonomy_enabled=bool(
                value_or_settings(live.production_autonomy_enabled, self.settings.crypto_production_autonomy_enabled)
            ),
            asset_modes={
                _normalize_crypto_asset_symbol(symbol): _normalize_crypto_asset_mode(mode)
                for symbol, mode in (live.asset_modes or {}).items()
            },
            asset_entry_overrides=overrides,
        )

    def runtime_crypto_thresholds(self, policy: RuntimeCryptoPolicy, *, asset_symbol: str | None = None) -> RuntimeThresholds:
        entry = policy.entry_for_asset(asset_symbol)
        return RuntimeThresholds(
            risk_min_edge_bps=int(entry["min_fee_adjusted_edge_bps"]),
            risk_max_credible_edge_bps=int(entry["max_credible_edge_bps"]),
            risk_min_confidence=float(entry["min_confidence"]),
            risk_min_contract_price_dollars=float(entry["min_contract_price_dollars"]),
            risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
            risk_safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
            risk_risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
            trigger_max_spread_bps=int(entry["max_spread_bps"]),
            trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_abs_delta_f=self.settings.strategy_min_abs_delta_f,
            strategy_min_remaining_payout_bps=CRYPTO_MIN_REMAINING_PAYOUT_BPS,
        )

    def sanitize_candidate_pack(self, candidate: AgentPack, *, parent_version: str) -> AgentPack:
        thresholds = candidate.thresholds.model_copy(deep=True)
        weather_policy = candidate.weather_policy.model_copy(deep=True)
        crypto_policy = candidate.crypto_policy.model_copy(deep=True)
        thresholds.risk_min_edge_bps = self._clamp_int(thresholds.risk_min_edge_bps, 250, 5000)
        thresholds.risk_max_credible_edge_bps = self._clamp_int(thresholds.risk_max_credible_edge_bps, 2500, 10000)
        thresholds.risk_min_confidence = self._clamp_float(thresholds.risk_min_confidence, 0.60, 0.90)
        thresholds.risk_min_contract_price_dollars = self._clamp_float(
            thresholds.risk_min_contract_price_dollars,
            self.settings.risk_min_contract_price_dollars,
            0.99,
        )
        thresholds.trigger_max_spread_bps = self._clamp_int(thresholds.trigger_max_spread_bps, 50, 2500)
        thresholds.trigger_cooldown_seconds = self._clamp_int(thresholds.trigger_cooldown_seconds, 30, 3600)
        thresholds.strategy_min_abs_delta_f = self._clamp_float(thresholds.strategy_min_abs_delta_f, 0.0, 10.0)
        thresholds.strategy_min_remaining_payout_bps = self._clamp_int(
            thresholds.strategy_min_remaining_payout_bps,
            500,
            3500,
        )
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
        crypto_policy.entry = self._sanitize_crypto_entry(crypto_policy.entry)
        crypto_policy.replay = AgentPackCryptoReplayPolicy(
            min_resolved_markets=self._clamp_int(crypto_policy.replay.min_resolved_markets, 10, 5000),
            min_trade_candidates=self._clamp_int(crypto_policy.replay.min_trade_candidates, 1, 1000),
            min_net_pl_dollars=self._clamp_float(crypto_policy.replay.min_net_pl_dollars, 0.0, 1000.0),
            max_hard_cap_breaches=self._clamp_int(crypto_policy.replay.max_hard_cap_breaches, 0, 100),
            min_spot_coverage_pct=self._clamp_float(crypto_policy.replay.min_spot_coverage_pct, 0.50, 1.0),
            require_calibration_better_than_mid=crypto_policy.replay.require_calibration_better_than_mid,
            require_pnl_beats_market_mid=crypto_policy.replay.require_pnl_beats_market_mid,
            min_pnl_advantage_dollars=self._clamp_float(crypto_policy.replay.min_pnl_advantage_dollars, 0.0, 1000.0),
        )
        crypto_policy.live = AgentPackCryptoLivePolicy(
            trading_enabled=crypto_policy.live.trading_enabled,
            production_autonomy_enabled=crypto_policy.live.production_autonomy_enabled,
            asset_modes={
                _normalize_crypto_asset_symbol(symbol): _normalize_crypto_asset_mode(mode)
                for symbol, mode in (crypto_policy.live.asset_modes or {}).items()
            },
        )
        crypto_policy.asset_entry_overrides = {
            _normalize_crypto_asset_symbol(symbol): self._sanitize_crypto_entry(entry)
            for symbol, entry in (crypto_policy.asset_entry_overrides or {}).items()
        }
        weather_policy.policies = self._sanitize_weather_policies(weather_policy.policies)
        sanitized_roles = {
            role_name: role.model_copy(update={"temperature": max(0.0, min(1.0, role.temperature))})
            for role_name, role in candidate.roles.items()
        }
        return candidate.model_copy(
            update={
                "parent_version": parent_version,
                "thresholds": thresholds,
                "weather_policy": weather_policy,
                "crypto_policy": crypto_policy,
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

    def _sanitize_crypto_entry(self, entry: AgentPackCryptoEntryPolicy) -> AgentPackCryptoEntryPolicy:
        return AgentPackCryptoEntryPolicy(
            min_fee_adjusted_edge_bps=self._clamp_int(entry.min_fee_adjusted_edge_bps, 250, 5000),
            max_spread_bps=self._clamp_int(entry.max_spread_bps, 50, 2500),
            min_confidence=self._clamp_float(entry.min_confidence, 0.50, 0.99),
            min_contract_price_dollars=self._clamp_float(
                entry.min_contract_price_dollars,
                self.settings.risk_min_contract_price_dollars,
                0.99,
            ),
            min_remaining_payout_bps=CRYPTO_MIN_REMAINING_PAYOUT_BPS,
            max_credible_edge_bps=self._clamp_int(entry.max_credible_edge_bps, 2500, 10000),
        )

    def _contract_price_floor(self, value: Any) -> float:
        if value is None:
            return float(self.settings.risk_min_contract_price_dollars)
        return max(float(value), float(self.settings.risk_min_contract_price_dollars))

    def _floor_runtime_thresholds(self, thresholds: RuntimeThresholds) -> RuntimeThresholds:
        return replace(
            thresholds,
            risk_min_contract_price_dollars=self._contract_price_floor(
                thresholds.risk_min_contract_price_dollars
            ),
        )

    def _sanitize_weather_policies(self, policies: dict[str, Any]) -> dict[str, Any]:
        from kalshi_bot.services.weather_policy import sanitize_weather_policy

        sanitized: dict[str, Any] = {}
        for key, policy in (policies or {}).items():
            normalized = sanitize_weather_policy(policy)
            thresholds = normalized.thresholds.model_copy(deep=True)
            thresholds.risk_min_edge_bps = self._clamp_int(thresholds.risk_min_edge_bps, 250, 5000)
            thresholds.risk_max_credible_edge_bps = self._clamp_int(thresholds.risk_max_credible_edge_bps, 2500, 10000)
            thresholds.risk_min_confidence = self._clamp_float(thresholds.risk_min_confidence, 0.60, 0.90)
            thresholds.risk_min_contract_price_dollars = self._clamp_float(
                thresholds.risk_min_contract_price_dollars,
                self.settings.risk_min_contract_price_dollars,
                0.99,
            )
            thresholds.trigger_max_spread_bps = self._clamp_int(thresholds.trigger_max_spread_bps, 50, 2500)
            thresholds.trigger_cooldown_seconds = self._clamp_int(thresholds.trigger_cooldown_seconds, 30, 3600)
            thresholds.strategy_min_abs_delta_f = self._clamp_float(thresholds.strategy_min_abs_delta_f, 0.0, 10.0)
            thresholds.strategy_min_remaining_payout_bps = self._clamp_int(
                thresholds.strategy_min_remaining_payout_bps,
                500,
                3500,
            )
            bootstrap = self._sanitize_weather_bootstrap(normalized.bootstrap)
            sanitized[normalized.policy_key or str(key)] = normalized.model_copy(
                update={"thresholds": thresholds, "bootstrap": bootstrap}
            )
        return sanitized

    def _sanitize_weather_bootstrap(self, policy: AgentPackWeatherBootstrapPolicy) -> AgentPackWeatherBootstrapPolicy:
        envelope = policy.grid_envelope or AgentPackWeatherBootstrapGridEnvelope()
        envelope = AgentPackWeatherBootstrapGridEnvelope(
            min_confidence=self._clamp_float(envelope.min_confidence, 0.0, 0.99),
            max_confidence=self._clamp_float(envelope.max_confidence, 0.01, 1.0),
            min_edge_bps=self._clamp_int(envelope.min_edge_bps, 0, 10000),
            max_edge_bps=self._clamp_int(envelope.max_edge_bps, 250, 20000),
            min_size_factor=self._clamp_float(envelope.min_size_factor, 0.0, 1.0),
            max_size_factor=self._clamp_float(envelope.max_size_factor, 0.01, 1.0),
            min_daily_notional_usd=self._clamp_float(envelope.min_daily_notional_usd, 0.0, 1000.0),
            max_daily_notional_usd=self._clamp_float(envelope.max_daily_notional_usd, 1.0, 5000.0),
            min_entry_price_dollars=self._clamp_float(envelope.min_entry_price_dollars, 0.01, 0.99),
            min_remaining_payout_bps=self._clamp_int(envelope.min_remaining_payout_bps, 100, 9900),
        )
        if envelope.max_confidence < envelope.min_confidence:
            envelope.max_confidence = envelope.min_confidence
        if envelope.max_edge_bps < envelope.min_edge_bps:
            envelope.max_edge_bps = envelope.min_edge_bps
        if envelope.max_size_factor < envelope.min_size_factor:
            envelope.max_size_factor = envelope.min_size_factor
        if envelope.max_daily_notional_usd < envelope.min_daily_notional_usd:
            envelope.max_daily_notional_usd = envelope.min_daily_notional_usd

        tiers: dict[str, AgentPackWeatherBootstrapTier] = {}
        for name, tier in (policy.tiers or {}).items():
            tiers[str(name)] = AgentPackWeatherBootstrapTier(
                min_samples=max(0, int(tier.min_samples)),
                max_samples=max(0, int(tier.max_samples)) if tier.max_samples is not None else None,
                min_confidence=self._clamp_float(
                    tier.min_confidence,
                    float(envelope.min_confidence),
                    float(envelope.max_confidence),
                ),
                min_edge_bps=self._clamp_int(
                    tier.min_edge_bps,
                    int(envelope.min_edge_bps),
                    int(envelope.max_edge_bps),
                ),
                size_factor=self._clamp_float(
                    tier.size_factor,
                    float(envelope.min_size_factor),
                    float(envelope.max_size_factor),
                ),
                live_enabled=bool(tier.live_enabled),
            )
        caps = policy.caps or AgentPackWeatherBootstrapCaps()
        sanitized_caps = AgentPackWeatherBootstrapCaps(
            shadow_min_hours=max(0, int(caps.shadow_min_hours)),
            shadow_min_market_episodes=max(1, int(caps.shadow_min_market_episodes)),
            shadow_required_match_rate=self._clamp_float(caps.shadow_required_match_rate, 0.0, 1.0),
            initial_daily_notional_usd=self._clamp_float(
                caps.initial_daily_notional_usd,
                float(envelope.min_daily_notional_usd),
                float(envelope.max_daily_notional_usd),
            ),
            initial_max_concurrent_positions=max(1, int(caps.initial_max_concurrent_positions)),
            expanded_daily_notional_usd=self._clamp_float(
                caps.expanded_daily_notional_usd,
                float(envelope.min_daily_notional_usd),
                float(envelope.max_daily_notional_usd),
            ),
            expanded_max_concurrent_positions=max(1, int(caps.expanded_max_concurrent_positions)),
            kill_switch_lookback=max(1, int(caps.kill_switch_lookback)),
            kill_switch_min_rows=max(1, int(caps.kill_switch_min_rows)),
            kill_switch_min_win_rate=self._clamp_float(caps.kill_switch_min_win_rate, 0.0, 1.0),
            kill_switch_drawdown_usd=max(0.0, float(caps.kill_switch_drawdown_usd)),
            consecutive_loss_kill_switch_threshold=max(
                1,
                int(getattr(caps, "consecutive_loss_kill_switch_threshold", 3)),
            ),
        )
        return policy.model_copy(
            deep=True,
            update={
                "grid_envelope": envelope,
                "tiers": tiers,
                "caps": sanitized_caps,
            },
        )

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


def _normalize_crypto_asset_symbol(asset_symbol: str) -> str:
    normalized = "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())
    return normalized or "UNKNOWN"


def _normalize_crypto_asset_mode(mode: Any) -> str:
    normalized = str(mode or "shadow").strip().lower()
    return normalized if normalized in {"off", "shadow", "live"} else "shadow"
