from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.forecast.source_health import SourceHealthLabel, should_pause_new_entries

logger = logging.getLogger(__name__)


class WatchdogService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def daemon_unhealthy_after_seconds(self) -> int:
        return (self.settings.daemon_heartbeat_interval_seconds * 2) + 15

    @property
    def active_restart_wait_seconds(self) -> int:
        return max(30, self.settings.daemon_heartbeat_interval_seconds + 15)

    async def app_health(
        self,
        *,
        color: str,
        kalshi_env: str,
        timeout_seconds: float = 5.0,
    ) -> dict[str, Any]:
        url = f"http://app_{kalshi_env}_{color}:8000/readyz"
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
            except Exception as exc:
                return {
                    "kalshi_env": kalshi_env,
                    "color": color,
                    "healthy": False,
                    "status": "unhealthy",
                    "detail": str(exc),
                    "url": url,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
        return {
            "kalshi_env": kalshi_env,
            "color": color,
            "healthy": True,
            "status": "healthy",
            "detail": "readyz_ok",
            "url": url,
            "observed_at": datetime.now(UTC).isoformat(),
        }

    async def daemon_health(
        self,
        repo: PlatformRepository,
        *,
        color: str,
        kalshi_env: str | None = None,
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        now = datetime.now(UTC)
        heartbeat = await repo.get_checkpoint(f"daemon_heartbeat:{env}:{color}")
        reconcile = await repo.get_checkpoint(f"daemon_reconcile:{env}:{color}")
        heartbeat_at = self._checkpoint_time(heartbeat, "heartbeat_at")
        reconcile_at = self._checkpoint_time(reconcile, "reconciled_at")
        heartbeat_age = self._age_seconds(now, heartbeat_at)
        reconcile_age = self._age_seconds(now, reconcile_at)
        healthy = heartbeat_age is not None and heartbeat_age <= self.daemon_unhealthy_after_seconds
        if heartbeat_at is None:
            reason = "no heartbeat checkpoint"
        elif healthy:
            reason = "heartbeat fresh"
        else:
            reason = "heartbeat stale"
        return {
            "kalshi_env": env,
            "color": color,
            "healthy": healthy,
            "reason": reason,
            "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
            "heartbeat_age_seconds": heartbeat_age,
            "last_reconcile_at": reconcile_at.isoformat() if reconcile_at is not None else None,
            "last_reconcile_age_seconds": reconcile_age,
            "threshold_seconds": self.daemon_unhealthy_after_seconds,
        }

    async def get_status(self, repo: PlatformRepository, *, kalshi_env: str | None = None) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        control = await repo.get_deployment_control(kalshi_env=env)
        watchdog = dict(control.notes.get("watchdog") or {})
        colors: dict[str, Any] = {}
        note_colors = dict(watchdog.get("colors") or {})
        for color in (DeploymentColor.BLUE.value, DeploymentColor.GREEN.value):
            app = dict(note_colors.get(color, {}).get("app") or {})
            daemon = await self.daemon_health(repo, color=color, kalshi_env=env)
            app_healthy = app.get("healthy")
            colors[color] = {
                "app": {
                    "healthy": app_healthy,
                    "status": app.get("status", "unknown"),
                    "detail": app.get("detail", "not yet observed by watchdog"),
                    "observed_at": app.get("observed_at"),
                },
                "daemon": daemon,
                "combined_healthy": bool(app_healthy) and daemon["healthy"],
            }
        return {
            "kalshi_env": env,
            "active_color": control.active_color,
            "kill_switch_enabled": control.kill_switch_enabled,
            "colors": colors,
            "last_action": watchdog.get("last_action"),
            "last_failover": watchdog.get("last_failover"),
            "last_boot_recovery": watchdog.get("last_boot_recovery"),
            "pending_recovery": watchdog.get("pending_recovery"),
            "updated_at": watchdog.get("updated_at"),
            "daemon_unhealthy_after_seconds": self.daemon_unhealthy_after_seconds,
        }

    async def run_once(
        self,
        repo: PlatformRepository,
        *,
        app_statuses: dict[str, str],
        source: str = "watchdog_timer",
    ) -> dict[str, Any]:
        env = self.settings.kalshi_env
        now = datetime.now(UTC)
        control = await repo.get_deployment_control(kalshi_env=env)
        notes = dict(control.notes or {})
        watchdog = dict(notes.get("watchdog") or {})
        pending = dict(watchdog.get("pending_recovery") or {})

        colors: dict[str, Any] = {}
        for color in (DeploymentColor.BLUE.value, DeploymentColor.GREEN.value):
            app = self._normalize_app_status(app_statuses.get(color, "unknown"))
            app["observed_at"] = now.isoformat()
            daemon = await self.daemon_health(repo, color=color, kalshi_env=env)
            colors[color] = {
                "app": app,
                "daemon": daemon,
                "combined_healthy": app["healthy"] and daemon["healthy"],
            }

        active_color = control.active_color
        inactive_color = self._other_color(active_color)
        action = "none"
        target_color: str | None = None
        failed_color: str | None = None
        reason = "all colors healthy"
        wait_seconds = 0

        active_healthy = self._recovery_healthy(colors[active_color])
        inactive_healthy = self._recovery_healthy(colors[inactive_color])

        if active_healthy:
            if pending.get("color") == active_color:
                pending = {}
            if not inactive_healthy:
                action = "restart_color"
                target_color = inactive_color
                reason = "inactive color unhealthy"
        else:
            failed_color = active_color
            if not inactive_healthy:
                action = "restart_stack"
                reason = "both colors unhealthy"
                pending = {}
            elif pending.get("color") == active_color and pending.get("step") == "restart_active":
                await repo.set_active_color(inactive_color, kalshi_env=env)
                control = await repo.get_deployment_control(kalshi_env=env)
                notes = dict(control.notes or {})
                watchdog = dict(notes.get("watchdog") or {})
                action = "failover"
                target_color = inactive_color
                reason = "active color remained unhealthy after restart; failed over"
                pending = {}
                watchdog["last_failover"] = {
                    "from_color": active_color,
                    "to_color": inactive_color,
                    "reason": reason,
                    "observed_at": now.isoformat(),
                }
            else:
                action = "restart_color"
                target_color = active_color
                reason = "active color unhealthy"
                wait_seconds = self.active_restart_wait_seconds
                pending = {
                    "color": active_color,
                    "step": "restart_active",
                    "started_at": now.isoformat(),
                    "reason": reason,
                }

        # Auto-enable kill switch if active color's reconcile is stale.
        reconcile_stale_threshold = self.settings.daemon_reconcile_stale_kill_switch_seconds
        active_daemon = colors[active_color]["daemon"]
        reconcile_age = active_daemon.get("last_reconcile_age_seconds")
        if (
            reconcile_stale_threshold > 0
            and reconcile_age is not None
            and reconcile_age > reconcile_stale_threshold
            and not control.kill_switch_enabled
        ):
            await repo.set_kill_switch(True, kalshi_env=env)
            control = await repo.get_deployment_control(kalshi_env=env)
            logger.critical(
                "Reconcile stale for %.0fs (threshold %ds) — kill switch auto-enabled",
                reconcile_age,
                reconcile_stale_threshold,
            )
            await repo.log_ops_event(
                severity="critical",
                summary=(
                    f"Kill switch auto-enabled: reconcile stale for {reconcile_age:.0f}s "
                    f"(threshold {reconcile_stale_threshold}s)"
                ),
                source="watchdog",
                payload={
                    "kalshi_env": env,
                    "active_color": active_color,
                    "reconcile_age_seconds": reconcile_age,
                    "threshold_seconds": reconcile_stale_threshold,
                },
            )

        source_health_pause = await self._evaluate_source_health_pause(repo, env=env, now=now, notes=notes)
        notes = source_health_pause.pop("notes")

        action_payload = {
            "kalshi_env": env,
            "action": action,
            "target_color": target_color,
            "failed_color": failed_color,
            "active_color": control.active_color,
            "inactive_color": self._other_color(control.active_color),
            "reason": reason,
            "wait_seconds": wait_seconds,
            "source": source,
            "colors": colors,
            "source_health": source_health_pause,
            "observed_at": now.isoformat(),
        }
        watchdog["updated_at"] = now.isoformat()
        watchdog["colors"] = colors
        watchdog["pending_recovery"] = pending or None
        watchdog["last_action"] = {
            "action": action,
            "target_color": target_color,
            "failed_color": failed_color,
            "reason": reason,
            "source": source,
            "outcome": "planned" if action != "none" else "noop",
            "observed_at": now.isoformat(),
        }
        notes["watchdog"] = watchdog
        await repo.update_deployment_notes(notes, kalshi_env=env)
        if action != "none":
            await repo.log_ops_event(
                severity="critical" if action == "restart_stack" else "warning",
                summary="Watchdog requested recovery action",
                source="watchdog",
                payload=action_payload,
            )
        return action_payload

    async def _evaluate_source_health_pause(
        self,
        repo: PlatformRepository,
        *,
        env: str,
        now: datetime,
        notes: dict[str, Any],
    ) -> dict[str, Any]:
        current = dict(notes.get("source_health") or {})
        if not self.settings.source_health_pause_new_entries_enabled:
            current["pause_new_entries_enabled"] = False
            notes["source_health"] = current
            return {"enabled": False, "pause_new_entries": bool(current.get("pause_new_entries")), "notes": notes}

        required = max(1, int(self.settings.source_health_broken_pause_consecutive_cycles))
        recent = await repo.list_recent_source_health_logs(
            kalshi_env=env,
            aggregate_only=True,
            limit=required,
        )
        labels = [record.label for record in recent]
        latest = recent[0] if recent else None
        should_pause = should_pause_new_entries(labels, consecutive_broken_cycles=required)
        was_paused = bool(current.get("pause_new_entries"))
        changed = False

        current.update(
            {
                "pause_new_entries_enabled": True,
                "required_broken_cycles": required,
                "recent_aggregate_labels": labels,
                "latest_observed_at": latest.observed_at.isoformat() if latest is not None else None,
                "latest_log_id": latest.id if latest is not None else None,
                "updated_at": now.isoformat(),
            }
        )
        if latest is not None:
            current["aggregate_label"] = latest.label
            current["aggregate_score"] = latest.score

        if should_pause:
            current["pause_new_entries"] = True
            current["pause_reason"] = f"aggregate source health BROKEN for {required} consecutive cycles"
            current["paused_at"] = current.get("paused_at") or now.isoformat()
            changed = not was_paused
            if changed:
                await repo.log_ops_event(
                    severity="warning",
                    summary="Source health paused new entries",
                    source="watchdog",
                    payload={
                        "kalshi_env": env,
                        "required_broken_cycles": required,
                        "recent_aggregate_labels": labels,
                        "latest_log_id": latest.id if latest is not None else None,
                    },
                    kalshi_env=env,
                )
        elif was_paused and latest is not None and latest.label == SourceHealthLabel.HEALTHY.value:
            current["pause_new_entries"] = False
            current["pause_reason"] = None
            current["resumed_at"] = now.isoformat()
            changed = True
            await repo.log_ops_event(
                severity="info",
                summary="Source health resumed new entries",
                source="watchdog",
                payload={
                    "kalshi_env": env,
                    "latest_log_id": latest.id,
                    "aggregate_label": latest.label,
                    "aggregate_score": latest.score,
                },
                kalshi_env=env,
            )
        else:
            current["pause_new_entries"] = was_paused

        notes["source_health"] = current
        return {
            "enabled": True,
            "pause_new_entries": bool(current.get("pause_new_entries")),
            "changed": changed,
            "aggregate_label": current.get("aggregate_label"),
            "aggregate_score": current.get("aggregate_score"),
            "recent_aggregate_labels": labels,
            "latest_log_id": current.get("latest_log_id"),
            "notes": notes,
        }

    async def record_action(
        self,
        repo: PlatformRepository,
        *,
        action: str,
        outcome: str,
        reason: str,
        target_color: str | None = None,
        failed_color: str | None = None,
        source: str = "watchdog_timer",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        env = self.settings.kalshi_env
        control = await repo.get_deployment_control(kalshi_env=env)
        notes = dict(control.notes or {})
        watchdog = dict(notes.get("watchdog") or {})
        last_action = {
            "action": action,
            "target_color": target_color,
            "failed_color": failed_color,
            "reason": reason,
            "source": source,
            "outcome": outcome,
            "completed_at": now.isoformat(),
        }
        watchdog["last_action"] = last_action
        if action == "failover" and outcome == "succeeded":
            watchdog["last_failover"] = {
                "from_color": failed_color,
                "to_color": target_color,
                "reason": reason,
                "outcome": outcome,
                "completed_at": now.isoformat(),
            }
        notes["watchdog"] = watchdog
        await repo.update_deployment_notes(notes, kalshi_env=env)
        await repo.log_ops_event(
            severity="info" if outcome == "succeeded" else "error",
            summary="Watchdog action outcome",
            source="watchdog",
            payload={
                "kalshi_env": env,
                "action": action,
                "outcome": outcome,
                "reason": reason,
                "target_color": target_color,
                "failed_color": failed_color,
                "source": source,
                **(payload or {}),
            },
        )
        return last_action

    async def record_boot(
        self,
        repo: PlatformRepository,
        *,
        status: str,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        env = self.settings.kalshi_env
        control = await repo.get_deployment_control(kalshi_env=env)
        notes = dict(control.notes or {})
        watchdog = dict(notes.get("watchdog") or {})
        watchdog["last_boot_recovery"] = {
            "status": status,
            "reason": reason,
            "observed_at": now.isoformat(),
            **(payload or {}),
        }
        notes["watchdog"] = watchdog
        await repo.update_deployment_notes(notes, kalshi_env=env)
        await repo.log_ops_event(
            severity="info" if status == "success" else "error",
            summary="Boot recovery status recorded",
            source="watchdog",
            payload={
                "kalshi_env": env,
                "status": status,
                "reason": reason,
                **(payload or {}),
            },
        )
        return watchdog["last_boot_recovery"]

    @staticmethod
    def _other_color(color: str) -> str:
        return DeploymentColor.GREEN.value if color == DeploymentColor.BLUE.value else DeploymentColor.BLUE.value

    @staticmethod
    def _normalize_app_status(status: str) -> dict[str, Any]:
        raw = (status or "unknown").strip().lower()
        healthy = raw == "healthy"
        detail = {
            "healthy": "app ready",
            "unhealthy": "container healthcheck failing",
            "starting": "container still starting",
            "missing": "container missing",
            "running": "container running without health result",
            "exited": "container exited",
            "unknown": "watchdog has no app status",
        }.get(raw, f"container status {raw}")
        return {"status": raw, "healthy": healthy, "detail": detail}

    @staticmethod
    def _recovery_healthy(color_status: dict[str, Any]) -> bool:
        app = dict(color_status.get("app") or {})
        daemon = dict(color_status.get("daemon") or {})
        if not daemon.get("healthy"):
            return False
        return app.get("status") in {"healthy", "starting"}

    @staticmethod
    def _age_seconds(now: datetime, observed_at: datetime | None) -> float | None:
        if observed_at is None:
            return None
        return max(0.0, (now - observed_at).total_seconds())

    @staticmethod
    def _checkpoint_time(checkpoint: Any | None, payload_key: str) -> datetime | None:
        if checkpoint is None:
            return None
        payload = dict(checkpoint.payload or {})
        raw = payload.get(payload_key)
        if raw:
            try:
                return datetime.fromisoformat(str(raw))
            except ValueError:
                return checkpoint.updated_at
        return checkpoint.updated_at
