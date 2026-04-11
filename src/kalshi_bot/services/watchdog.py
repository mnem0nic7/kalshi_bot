from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.repositories import PlatformRepository


class WatchdogService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def daemon_unhealthy_after_seconds(self) -> int:
        return (self.settings.daemon_heartbeat_interval_seconds * 2) + 15

    @property
    def active_restart_wait_seconds(self) -> int:
        return max(30, self.settings.daemon_heartbeat_interval_seconds + 15)

    async def app_health(self, *, color: str, timeout_seconds: float = 5.0) -> dict[str, Any]:
        url = f"http://app_{color}:8000/readyz"
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
            except Exception as exc:
                return {
                    "color": color,
                    "healthy": False,
                    "status": "unhealthy",
                    "detail": str(exc),
                    "url": url,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
        return {
            "color": color,
            "healthy": True,
            "status": "healthy",
            "detail": "readyz_ok",
            "url": url,
            "observed_at": datetime.now(UTC).isoformat(),
        }

    async def daemon_health(self, repo: PlatformRepository, *, color: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        heartbeat = await repo.get_checkpoint(f"daemon_heartbeat:{color}")
        reconcile = await repo.get_checkpoint(f"daemon_reconcile:{color}")
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
            "color": color,
            "healthy": healthy,
            "reason": reason,
            "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
            "heartbeat_age_seconds": heartbeat_age,
            "last_reconcile_at": reconcile_at.isoformat() if reconcile_at is not None else None,
            "last_reconcile_age_seconds": reconcile_age,
            "threshold_seconds": self.daemon_unhealthy_after_seconds,
        }

    async def get_status(self, repo: PlatformRepository) -> dict[str, Any]:
        control = await repo.get_deployment_control()
        watchdog = dict(control.notes.get("watchdog") or {})
        colors: dict[str, Any] = {}
        note_colors = dict(watchdog.get("colors") or {})
        for color in (DeploymentColor.BLUE.value, DeploymentColor.GREEN.value):
            app = dict(note_colors.get(color, {}).get("app") or {})
            daemon = await self.daemon_health(repo, color=color)
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
        now = datetime.now(UTC)
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        watchdog = dict(notes.get("watchdog") or {})
        pending = dict(watchdog.get("pending_recovery") or {})

        colors: dict[str, Any] = {}
        for color in (DeploymentColor.BLUE.value, DeploymentColor.GREEN.value):
            app = self._normalize_app_status(app_statuses.get(color, "unknown"))
            app["observed_at"] = now.isoformat()
            daemon = await self.daemon_health(repo, color=color)
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
                await repo.set_active_color(inactive_color)
                control = await repo.get_deployment_control()
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

        action_payload = {
            "action": action,
            "target_color": target_color,
            "failed_color": failed_color,
            "active_color": control.active_color,
            "inactive_color": self._other_color(control.active_color),
            "reason": reason,
            "wait_seconds": wait_seconds,
            "source": source,
            "colors": colors,
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
        await repo.update_deployment_notes(notes)
        if action != "none":
            await repo.log_ops_event(
                severity="critical" if action == "restart_stack" else "warning",
                summary="Watchdog requested recovery action",
                source="watchdog",
                payload=action_payload,
            )
        return action_payload

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
        control = await repo.get_deployment_control()
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
        await repo.update_deployment_notes(notes)
        await repo.log_ops_event(
            severity="info" if outcome == "succeeded" else "error",
            summary="Watchdog action outcome",
            source="watchdog",
            payload={
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
        control = await repo.get_deployment_control()
        notes = dict(control.notes or {})
        watchdog = dict(notes.get("watchdog") or {})
        watchdog["last_boot_recovery"] = {
            "status": status,
            "reason": reason,
            "observed_at": now.isoformat(),
            **(payload or {}),
        }
        notes["watchdog"] = watchdog
        await repo.update_deployment_notes(notes)
        await repo.log_ops_event(
            severity="info" if status == "success" else "error",
            summary="Boot recovery status recorded",
            source="watchdog",
            payload={
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
