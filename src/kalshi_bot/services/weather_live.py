from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StrategyCode
from kalshi_bot.core.schemas import (
    AgentPackThresholds,
    AgentPackWeatherBootstrapCaps,
    AgentPackWeatherBootstrapPolicy,
    AgentPackWeatherBootstrapTier,
    AgentPackWeatherPolicy,
    AgentPackWeatherPolicyBook,
)
from kalshi_bot.db.models import OpsEvent, Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.weather_policy import WeatherPolicyContext, build_weather_policy_key


WEATHER_LIVE_POLICY_VALID_DAYS = 14
WEATHER_LIVE_POLICY_KEY = build_weather_policy_key(
    WeatherPolicyContext(
        strategy_code=StrategyCode.DIRECTIONAL.value,
        cohort_id="global",
        series_ticker="any",
        side="any",
        season="all_season",
        month="all_month",
        regime="any",
        threshold_band="any",
        lane="entry_gate",
    )
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _is_weather_ticker(ticker: Any) -> bool:
    return str(ticker or "").upper().startswith("KXHIGH")


@dataclass(frozen=True, slots=True)
class WeatherLivePreflight:
    result: dict[str, Any]
    blockers: list[dict[str, Any]]

    @property
    def live_capable(self) -> bool:
        return not self.blockers


class WeatherLiveService:
    """Operator control surface for production weather live eligibility."""

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        agent_pack_service: AgentPackService,
        watchdog_service: Any,
        weather_prediction_service: Any,
        trading_audit_service: Any,
        has_write_credentials: bool,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.agent_pack_service = agent_pack_service
        self.watchdog_service = watchdog_service
        self.weather_prediction_service = weather_prediction_service
        self.trading_audit_service = trading_audit_service
        self.has_write_credentials = has_write_credentials

    async def status(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 1,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            preflight = await self._status(repo, kalshi_env=kalshi_env, days=days, now=now)
        return preflight.result

    async def activate(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 1,
        actor: str = "cli",
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            preflight = await self._status(repo, kalshi_env=kalshi_env, days=days, now=now)
            health_blockers = [
                blocker
                for blocker in preflight.blockers
                if blocker["code"]
                not in {"weather_policy_expired", "weather_policy_missing", "weather_policy_threshold_drift"}
            ]
            if health_blockers:
                return {
                    **preflight.result,
                    "action": "activate",
                    "activated": False,
                    "reason": "preflight_blocked",
                }

            control = await repo.get_deployment_control(kalshi_env=kalshi_env)
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            staged_pack = self._weather_live_pack(
                active_pack,
                now=now,
                actor=actor,
                kalshi_env=kalshi_env,
            )
            await repo.update_agent_pack(staged_pack)
            await self.agent_pack_service.assign_pack_to_color(
                repo,
                color=control.active_color,
                version=staged_pack.version,
            )

            notes = dict(control.notes or {})
            notes["weather_live"] = {
                "enabled": True,
                "policy_state": "live",
                "strategy_code": StrategyCode.DIRECTIONAL.value,
                "activated_at": now.isoformat(),
                "activated_by": actor,
                "active_policy_pack_version": staged_pack.version,
                "previous_agent_pack_version": active_pack.version,
                "valid_until": (now + timedelta(days=WEATHER_LIVE_POLICY_VALID_DAYS)).isoformat(),
                "daily_realized_loss_cap_pct": 0.02,
                "max_order_count_fp": 200,
                "bootstrap": {
                    "live_tiers": ["cold"],
                    "max_concurrent_positions": 1,
                    "max_daily_notional_usd": 100,
                    "cold_min_confidence": 0.90,
                    "cold_min_edge_bps_after_buffer": 3000,
                    "cold_size_factor": 0.10,
                },
            }
            await repo.update_deployment_notes(notes, kalshi_env=kalshi_env)
            await repo.log_ops_event(
                severity="info",
                summary="Weather live policy activated",
                source="weather_live",
                kalshi_env=kalshi_env,
                payload={
                    "actor": actor,
                    "policy_pack_version": staged_pack.version,
                    "policy_key": WEATHER_LIVE_POLICY_KEY,
                    "cold_bootstrap_live": True,
                },
            )
            await session.commit()

        post = await self.status(kalshi_env=kalshi_env, days=days, now=now)
        return {
            **post,
            "action": "activate",
            "activated": bool(post.get("live_capable")),
            "policy_pack_version": post.get("active_weather_policy", {}).get("pack_version"),
        }

    async def rollback(
        self,
        *,
        kalshi_env: str = "production",
        actor: str = "cli",
        reason: str = "manual_weather_live_rollback",
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            rollback_pack = self._weather_rollback_pack(
                active_pack,
                now=now,
                actor=actor,
                reason=reason,
            )
            await repo.update_agent_pack(rollback_pack)
            control = await repo.set_kill_switch(
                True,
                kalshi_env=kalshi_env,
                source="weather_live",
                reason=reason,
                payload={"actor": actor, "rolled_back_at": now.isoformat()},
            )
            await self.agent_pack_service.assign_pack_to_color(
                repo,
                color=control.active_color,
                version=rollback_pack.version,
            )
            notes = dict(control.notes or {})
            previous = dict(notes.get("weather_live") or {})
            notes["weather_live"] = {
                **previous,
                "enabled": False,
                "policy_state": "rollback",
                "rolled_back_at": now.isoformat(),
                "rolled_back_by": actor,
                "rollback_reason": reason,
                "rollback_policy_pack_version": rollback_pack.version,
            }
            await repo.update_deployment_notes(notes, kalshi_env=kalshi_env)
            await repo.log_ops_event(
                severity="warning",
                summary="Weather live policy rolled back",
                source="weather_live",
                kalshi_env=kalshi_env,
                payload={"actor": actor, "reason": reason},
            )
            await session.commit()

        post = await self.status(kalshi_env=kalshi_env, days=1, now=now)
        return {**post, "action": "rollback", "rolled_back": True}

    async def _status(
        self,
        repo: PlatformRepository,
        *,
        kalshi_env: str,
        days: int,
        now: datetime,
    ) -> WeatherLivePreflight:
        blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        control = await repo.get_deployment_control(kalshi_env=kalshi_env)
        active_pack = await self.agent_pack_service.get_active_pack(repo)
        fallback_thresholds = self.agent_pack_service.runtime_thresholds(active_pack)
        policy_resolution = self.agent_pack_service.resolve_weather_policy(
            active_pack,
            WeatherPolicyContext(
                strategy_code=StrategyCode.DIRECTIONAL.value,
                cohort_id="global",
                series_ticker="any",
                side="any",
                season="all_season",
                month="all_month",
                regime="any",
                threshold_band="any",
                lane="entry_gate",
            ),
            fallback_thresholds=fallback_thresholds,
        )
        policy = policy_resolution.policy
        policy_valid_until = _as_utc(policy.valid_until) if policy is not None else None
        threshold_consistency = self._threshold_consistency(policy_resolution.thresholds)
        daemon = await self.watchdog_service.daemon_health(
            repo,
            color=control.active_color,
            kalshi_env=kalshi_env,
        )
        watchdog_status = (
            await self.watchdog_service.get_status(repo, kalshi_env=kalshi_env)
            if hasattr(self.watchdog_service, "get_status")
            else {}
        )
        active_color_health = dict(
            (watchdog_status.get("colors") or {}).get(control.active_color) or {}
        )
        reconcile = await self._reconcile_status(repo, control=control, kalshi_env=kalshi_env, now=now)
        source_health = dict((control.notes or {}).get("source_health") or {})
        intraday = await self.weather_prediction_service.status(kalshi_env=kalshi_env)
        audit = await self.trading_audit_service.build_report(
            kalshi_env=kalshi_env,
            days=days,
            focus="weather-live",
            now=now,
        )
        weather_metrics = self._weather_metrics_from_audit(audit)
        shadow = await self._shadow_room_status(repo, kalshi_env=kalshi_env, now=now)
        ops_noise = await self._ops_noise(repo, kalshi_env=kalshi_env, now=now)

        def block(code: str, message: str, **details: Any) -> None:
            blockers.append({"code": code, "message": message, **details})

        def warn(code: str, message: str, **details: Any) -> None:
            warnings.append({"code": code, "message": message, **details})

        if self.settings.app_shadow_mode:
            block("app_shadow_mode_enabled", "APP_SHADOW_MODE is enabled.")
        if not self.has_write_credentials:
            block("write_credentials_missing", "Kalshi write credentials are missing.")
        if bool(control.kill_switch_enabled):
            block("kill_switch_enabled", "Global kill switch is enabled.")
        if str(control.active_color) != str(self.settings.app_color):
            block(
                "inactive_runtime_color",
                "Runtime app color is not the active deployment color.",
                active_color=control.active_color,
                runtime_color=self.settings.app_color,
            )
        if not bool(daemon.get("healthy")):
            block("daemon_unhealthy", "Active daemon heartbeat is not healthy.", daemon=daemon)
        if active_color_health and not bool(active_color_health.get("combined_healthy")):
            block(
                "active_color_unhealthy",
                "Active deployment color is not healthy.",
                active_color_health=active_color_health,
            )
        if not reconcile["fresh"]:
            block("reconcile_stale", "Post-clear reconcile is missing or stale.", reconcile=reconcile)
        if reconcile.get("post_clear_pending"):
            block(
                "post_clear_reconcile_pending",
                "Kill switch was cleared after the last active-color reconcile.",
                reconcile=reconcile,
            )
        if bool(source_health.get("pause_new_entries")):
            block(
                "source_health_pause_active",
                "Source-health pause is blocking new entries.",
                source_health=source_health,
            )
        if not self.settings.weather_intraday_model_enabled:
            block("intraday_model_disabled", "WEATHER_INTRADAY_MODEL_ENABLED is false.")
        elif not bool(intraday.get("intraday_model_usable")):
            block("intraday_model_unusable", "Intraday weather model is not usable.", intraday_model=intraday)
        if policy is None:
            block("weather_policy_missing", "No explicit Strategy A weather entry policy is active.")
        elif policy_valid_until is not None and policy_valid_until < now:
            block(
                "weather_policy_expired",
                "Active weather live policy has expired.",
                valid_until=policy_valid_until.isoformat(),
            )
        if not threshold_consistency["consistent"]:
            block(
                "weather_policy_threshold_drift",
                "Active weather policy thresholds disagree with runtime risk/trigger settings.",
                threshold_consistency=threshold_consistency,
            )
        if shadow["fresh_unexpected_shadow_rooms"] > 0:
            block(
                "fresh_weather_shadow_leakage",
                "Fresh production weather rooms are shadowed while app shadow mode is false.",
                shadow=shadow,
            )
        if shadow["stale_unexpected_shadow_rooms"] > 0:
            warn(
                "stale_weather_shadow_rooms",
                "Existing production weather shadow rooms remain shadow-only and will age out.",
                shadow=shadow,
            )
        if policy_resolution.reason_codes == ("fallback_flat_agent_pack_thresholds",):
            warn(
                "weather_policy_fallback_thresholds",
                "Weather policy resolver fell back to flat agent-pack thresholds.",
            )

        result = {
            "schema": "weather-live-v1",
            "kalshi_env": kalshi_env,
            "checked_at": now.isoformat(),
            "status": "ready" if not blockers else "blocked",
            "live_capable": not blockers,
            "blockers": blockers,
            "warnings": warnings,
            "deployment": {
                "app_shadow_mode": self.settings.app_shadow_mode,
                "runtime_color": self.settings.app_color,
                "active_color": control.active_color,
                "kill_switch_enabled": bool(control.kill_switch_enabled),
                "write_credentials_present": self.has_write_credentials,
                "weather_live": dict((control.notes or {}).get("weather_live") or {}),
            },
            "daemon": daemon,
            "active_color_health": active_color_health,
            "watchdog": watchdog_status,
            "reconcile": reconcile,
            "source_health": source_health,
            "intraday_model": intraday,
            "shadow": shadow,
            "active_weather_policy": {
                "pack_version": active_pack.version,
                "policy_key": policy_resolution.policy_key,
                "matched_policy_key": policy.policy_key if policy is not None else None,
                "mode": policy_resolution.mode,
                "action": policy_resolution.action,
                "valid_until": policy_valid_until.isoformat() if policy_valid_until is not None else None,
                "threshold_source": policy_resolution.reason_codes,
                "thresholds": self._threshold_payload(policy_resolution.thresholds),
                "threshold_consistency": threshold_consistency,
                "bootstrap": policy.bootstrap.model_dump(mode="json") if policy is not None else None,
            },
            "weather_metrics": weather_metrics,
            "top_weather_blockers": weather_metrics.get("top_blockers", []),
            "ops_event_groups": ops_noise,
        }
        return WeatherLivePreflight(result=result, blockers=blockers)

    def _weather_live_pack(
        self,
        active_pack: Any,
        *,
        now: datetime,
        actor: str,
        kalshi_env: str,
    ) -> Any:
        valid_until = now + timedelta(days=WEATHER_LIVE_POLICY_VALID_DAYS)
        version = f"weather-full-live-{now:%Y%m%dT%H%M%SZ}"
        thresholds = AgentPackThresholds(
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
        )
        bootstrap = AgentPackWeatherBootstrapPolicy(
            enabled=True,
            rollout_state="promoted_low",
            evidence_ready=True,
            allow_forecast_separation_bootstrap=True,
            fair_value_fallback_disqualifies=True,
            tiers={
                "cold": AgentPackWeatherBootstrapTier(
                    min_samples=0,
                    max_samples=0,
                    min_confidence=0.90,
                    min_edge_bps=3000,
                    size_factor=0.10,
                    live_enabled=True,
                ),
                "warming": AgentPackWeatherBootstrapTier(
                    min_samples=1,
                    max_samples=9,
                    min_confidence=0.80,
                    min_edge_bps=1500,
                    size_factor=0.25,
                    live_enabled=False,
                ),
                "maturing": AgentPackWeatherBootstrapTier(
                    min_samples=10,
                    max_samples=19,
                    min_confidence=0.70,
                    min_edge_bps=800,
                    size_factor=0.50,
                    live_enabled=False,
                ),
            },
            caps=AgentPackWeatherBootstrapCaps(
                initial_daily_notional_usd=100.0,
                initial_max_concurrent_positions=1,
                expanded_daily_notional_usd=500.0,
                expanded_max_concurrent_positions=3,
                kill_switch_lookback=10,
                kill_switch_min_rows=5,
                kill_switch_min_win_rate=0.30,
                kill_switch_drawdown_usd=100.0,
                consecutive_loss_kill_switch_threshold=3,
            ),
            promotion={
                "activated_by": actor,
                "activated_at": now.isoformat(),
                "kalshi_env": kalshi_env,
                "live_tiers": ["cold"],
            },
        )
        policy = AgentPackWeatherPolicy(
            policy_key=WEATHER_LIVE_POLICY_KEY,
            domain="weather",
            strategy_code=StrategyCode.DIRECTIONAL.value,
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
            thresholds=thresholds,
            bootstrap=bootstrap,
            reason_codes=["operator_weather_full_live_cold_bootstrap"],
            deterministic_summary=(
                "Weather full-live staged: mature empirical buckets remain deterministic; "
                "cold bootstrap is live at 10% size with one concurrent position and $100/day notional."
            ),
            valid_until=valid_until,
            metadata={
                "daily_realized_loss_cap_pct": 0.02,
                "max_order_count_fp": 200,
                "warming_maturing_shadow_only": True,
            },
        )
        weather_policy = active_pack.weather_policy.model_copy(deep=True)
        weather_policy.policies[WEATHER_LIVE_POLICY_KEY] = policy
        metadata = dict(active_pack.metadata or {})
        metadata["weather_full_live"] = {
            "activated_by": actor,
            "activated_at": now.isoformat(),
            "valid_until": valid_until.isoformat(),
            "policy_key": WEATHER_LIVE_POLICY_KEY,
        }
        return active_pack.model_copy(
            deep=True,
            update={
                "version": version,
                "status": "active",
                "parent_version": active_pack.version,
                "source": "operator_weather_live",
                "description": "Weather full-live policy: mature deterministic + cold bootstrap only.",
                "weather_policy": AgentPackWeatherPolicyBook(
                    policies=weather_policy.policies,
                    cohort_memberships=weather_policy.cohort_memberships,
                    grid_versions=weather_policy.grid_versions,
                ),
                "metadata": metadata,
                "created_at": now,
            },
        )

    def _weather_rollback_pack(
        self,
        active_pack: Any,
        *,
        now: datetime,
        actor: str,
        reason: str,
    ) -> Any:
        version = f"weather-full-live-rollback-{now:%Y%m%dT%H%M%SZ}"
        weather_policy = active_pack.weather_policy.model_copy(deep=True)
        policy = weather_policy.policies.get(WEATHER_LIVE_POLICY_KEY)
        if policy is not None:
            tiers = {
                name: tier.model_copy(update={"live_enabled": False})
                for name, tier in policy.bootstrap.tiers.items()
            }
            bootstrap = policy.bootstrap.model_copy(
                deep=True,
                update={
                    "rollout_state": "rollback",
                    "tiers": tiers,
                    "promotion": {
                        **dict(policy.bootstrap.promotion or {}),
                        "rolled_back_by": actor,
                        "rolled_back_at": now.isoformat(),
                        "rollback_reason": reason,
                    },
                },
            )
            metadata = dict(policy.metadata or {})
            metadata["rollback"] = {
                "rolled_back_by": actor,
                "rolled_back_at": now.isoformat(),
                "reason": reason,
            }
            weather_policy.policies[WEATHER_LIVE_POLICY_KEY] = policy.model_copy(
                deep=True,
                update={
                    "mode": "paused",
                    "action": "rollback",
                    "bootstrap": bootstrap,
                    "valid_until": now,
                    "reason_codes": ["operator_weather_live_rollback"],
                    "metadata": metadata,
                },
            )
        metadata = dict(active_pack.metadata or {})
        metadata["weather_full_live_rollback"] = {
            "rolled_back_by": actor,
            "rolled_back_at": now.isoformat(),
            "reason": reason,
            "policy_key": WEATHER_LIVE_POLICY_KEY,
        }
        return active_pack.model_copy(
            deep=True,
            update={
                "version": version,
                "status": "rollback",
                "parent_version": active_pack.version,
                "source": "operator_weather_live",
                "description": "Weather full-live rollback policy; global kill switch enabled.",
                "weather_policy": AgentPackWeatherPolicyBook(
                    policies=weather_policy.policies,
                    cohort_memberships=weather_policy.cohort_memberships,
                    grid_versions=weather_policy.grid_versions,
                ),
                "metadata": metadata,
                "created_at": now,
            },
        )

    async def _reconcile_status(
        self,
        repo: PlatformRepository,
        *,
        control: Any,
        kalshi_env: str,
        now: datetime,
    ) -> dict[str, Any]:
        checkpoint = await repo.get_checkpoint(f"daemon_reconcile:{kalshi_env}:{control.active_color}")
        payload = dict(checkpoint.payload or {}) if checkpoint is not None else {}
        reconciled_at = _parse_utc(payload.get("reconciled_at"))
        age = int((now - reconciled_at).total_seconds()) if reconciled_at is not None else None
        max_age = max(1, int(self.settings.daemon_reconcile_stale_kill_switch_seconds))
        fresh = age is not None and age <= max_age
        cleared_at = _parse_utc((control.notes or {}).get("kill_switch_cleared_at"))
        post_clear_pending = (
            cleared_at is not None
            and (reconciled_at is None or reconciled_at < cleared_at)
        )
        return {
            "fresh": fresh,
            "reconciled_at": reconciled_at.isoformat() if reconciled_at is not None else None,
            "age_seconds": age,
            "max_age_seconds": max_age,
            "post_clear_pending": post_clear_pending,
            "kill_switch_cleared_at": cleared_at.isoformat() if cleared_at is not None else None,
        }

    async def _shadow_room_status(
        self,
        repo: PlatformRepository,
        *,
        kalshi_env: str,
        now: datetime,
    ) -> dict[str, Any]:
        freshness_seconds = max(1800, int(self.settings.trigger_active_room_stale_seconds))
        fresh_cutoff = now - timedelta(seconds=freshness_seconds)
        result = await repo.session.execute(
            select(Room).where(
                Room.kalshi_env == kalshi_env,
                Room.shadow_mode.is_(True),
                Room.created_at >= now - timedelta(days=1),
            )
        )
        shadow_rooms = [room for room in result.scalars() if _is_weather_ticker(room.market_ticker)]
        fresh = [room for room in shadow_rooms if (_as_utc(room.created_at) or now) >= fresh_cutoff]
        stale = [room for room in shadow_rooms if (_as_utc(room.created_at) or now) < fresh_cutoff]
        return {
            "app_shadow_mode": self.settings.app_shadow_mode,
            "fresh_window_seconds": freshness_seconds,
            "fresh_unexpected_shadow_rooms": 0 if self.settings.app_shadow_mode else len(fresh),
            "stale_unexpected_shadow_rooms": 0 if self.settings.app_shadow_mode else len(stale),
            "fresh_room_ids": [room.id for room in fresh[:10]],
            "stale_room_ids": [room.id for room in stale[:10]],
        }

    async def _ops_noise(
        self,
        repo: PlatformRepository,
        *,
        kalshi_env: str,
        now: datetime,
    ) -> list[dict[str, Any]]:
        cutoff = now - timedelta(hours=6)
        result = await repo.session.execute(
            select(OpsEvent)
            .where(OpsEvent.kalshi_env == kalshi_env, OpsEvent.created_at >= cutoff)
            .order_by(OpsEvent.created_at.asc())
        )
        groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        for event in result.scalars():
            event_at = _as_utc(event.created_at)
            if event_at is None:
                continue
            source = str(event.source or "unknown")
            severity = str(event.severity or "unknown")
            summary = str(event.summary or "unknown")
            key = (source, severity, summary[:160])
            group = groups.setdefault(
                key,
                {
                    "source": source,
                    "severity": severity,
                    "summary": summary,
                    "count": 0,
                    "first_at": None,
                    "last_at": None,
                },
            )
            group["count"] += 1
            group["first_at"] = group["first_at"] or event_at.isoformat()
            group["last_at"] = event_at.isoformat()
        return sorted(groups.values(), key=lambda row: (-int(row["count"]), str(row["summary"])))[:20]

    def _weather_metrics_from_audit(self, audit: dict[str, Any]) -> dict[str, Any]:
        weather = dict((audit.get("domains") or {}).get("weather") or {})
        if weather:
            return weather
        return {
            "orders": 0,
            "fills": 0,
            "positions": 0,
            "pnl": {"gross_pnl_dollars": "0.0000", "net_pnl_dollars": None},
            "top_blockers": [],
        }

    def _threshold_payload(self, thresholds: RuntimeThresholds) -> dict[str, Any]:
        return {
            "risk_min_edge_bps": thresholds.risk_min_edge_bps,
            "trigger_max_spread_bps": thresholds.trigger_max_spread_bps,
            "risk_min_confidence": thresholds.risk_min_confidence,
            "risk_min_contract_price_dollars": thresholds.risk_min_contract_price_dollars,
            "strategy_min_abs_delta_f": thresholds.strategy_min_abs_delta_f,
            "strategy_min_remaining_payout_bps": thresholds.strategy_min_remaining_payout_bps,
        }

    def _threshold_consistency(self, thresholds: RuntimeThresholds) -> dict[str, Any]:
        expected = {
            "risk_min_edge_bps": self.settings.risk_min_edge_bps,
            "trigger_max_spread_bps": self.settings.trigger_max_spread_bps,
            "risk_min_confidence": self.settings.risk_min_confidence,
            "risk_min_contract_price_dollars": self.settings.risk_min_contract_price_dollars,
            "strategy_min_abs_delta_f": self.settings.strategy_min_abs_delta_f,
            "strategy_min_remaining_payout_bps": self.settings.strategy_min_remaining_payout_bps,
        }
        actual = self._threshold_payload(thresholds)
        mismatches = {
            key: {"expected": expected[key], "actual": actual[key]}
            for key in expected
            if actual[key] != expected[key]
        }
        return {
            "consistent": not mismatches,
            "expected": expected,
            "actual": actual,
            "mismatches": mismatches,
        }
