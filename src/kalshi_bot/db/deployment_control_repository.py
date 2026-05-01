from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.models import DeploymentControl


class DeploymentControlRepositoryMixin:
    session: AsyncSession

    def _resolved_kalshi_env(self, kalshi_env: str | None = None) -> str:
        raise NotImplementedError

    async def ensure_deployment_control(
        self,
        color: str,
        *,
        kalshi_env: str | None = None,
        initial_active_color: str | None = None,
        initial_kill_switch_enabled: bool | None = None,
    ) -> DeploymentControl:
        env = self._resolved_kalshi_env(kalshi_env)
        control = await self.session.get(DeploymentControl, env)
        if control is None:
            control = DeploymentControl(
                id=env,
                active_color=initial_active_color or DeploymentColor.BLUE.value,
                shadow_color=color,
                kill_switch_enabled=bool(initial_kill_switch_enabled),
            )
            self.session.add(control)
            await self.session.flush()
        return control

    async def get_deployment_control(self, *, kalshi_env: str | None = None) -> DeploymentControl:
        return await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=kalshi_env,
        )

    async def set_active_color(
        self,
        color: DeploymentColor | str,
        *,
        kalshi_env: str | None = None,
    ) -> DeploymentControl:
        control = await self.ensure_deployment_control(str(color), kalshi_env=kalshi_env)
        if control.active_color != str(color):
            control.execution_lock_holder = None
        control.active_color = str(color)
        notes = dict(control.notes or {})
        agent_pack_notes = dict(notes.get("agent_packs") or {})
        if agent_pack_notes:
            active_version = (
                agent_pack_notes.get("blue_version")
                if str(color) == DeploymentColor.BLUE.value
                else agent_pack_notes.get("green_version")
            )
            if active_version is not None:
                agent_pack_notes["active_version"] = active_version
                agent_pack_notes["champion_version"] = active_version
                notes["agent_packs"] = agent_pack_notes
                control.notes = notes
        await self.session.flush()
        return control

    async def set_kill_switch(
        self,
        enabled: bool,
        *,
        kalshi_env: str | None = None,
        source: str | None = None,
        reason: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> DeploymentControl:
        control = await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=kalshi_env,
        )
        now = datetime.now(UTC)
        control.kill_switch_enabled = enabled
        notes = dict(control.notes or {})
        kill_switch_notes = dict(notes.get("kill_switch") or {})
        if enabled:
            control.execution_lock_holder = None
            if source or reason:
                notes["kill_switch"] = {
                    "mode": "auto" if source == "watchdog" else "external",
                    "source": source,
                    "reason": reason,
                    "enabled_at": now.isoformat(),
                    "payload": payload or {},
                }
            elif kill_switch_notes.get("mode") == "auto":
                notes["kill_switch"] = {
                    "mode": "manual_or_external",
                    "enabled_at": now.isoformat(),
                    "previous_auto_source": kill_switch_notes.get("source"),
                    "previous_auto_reason": kill_switch_notes.get("reason"),
                    "previous_auto_enabled_at": kill_switch_notes.get("enabled_at"),
                }
        else:
            # Record when the kill switch was cleared so execution can require a
            # post-clear reconcile before the first live order goes out.
            notes["kill_switch_cleared_at"] = now.isoformat()
            if source or reason:
                notes["kill_switch"] = {
                    **kill_switch_notes,
                    "mode": "auto_cleared" if source == "watchdog" else "external_cleared",
                    "source": source,
                    "reason": reason,
                    "cleared_at": now.isoformat(),
                    "clear_payload": payload or {},
                }
            elif kill_switch_notes.get("mode") == "auto":
                notes["kill_switch"] = {
                    **kill_switch_notes,
                    "mode": "manual_or_external_cleared",
                    "cleared_at": now.isoformat(),
                }
        control.notes = notes
        await self.session.flush()
        return control

    async def acquire_execution_lock(
        self,
        holder: str,
        color: str,
        *,
        kalshi_env: str | None = None,
    ) -> bool:
        control = await self.ensure_deployment_control(color, kalshi_env=kalshi_env)
        if control.active_color != color or control.kill_switch_enabled:
            return False
        if control.execution_lock_holder not in (None, holder):
            return False
        control.execution_lock_holder = holder
        await self.session.flush()
        return True

    async def release_execution_lock(self, holder: str, *, kalshi_env: str | None = None) -> None:
        control = await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=kalshi_env,
        )
        if control.execution_lock_holder == holder:
            control.execution_lock_holder = None
            await self.session.flush()

    async def update_deployment_notes(
        self,
        notes: dict[str, Any],
        *,
        kalshi_env: str | None = None,
    ) -> DeploymentControl:
        control = await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=kalshi_env,
        )
        control.notes = notes
        await self.session.flush()
        return control

    async def update_deployment_note_key(
        self,
        key: str,
        updater: Callable[[Any], Any],
        *,
        kalshi_env: str | None = None,
    ) -> tuple[DeploymentControl, Any]:
        """Update one deployment-control notes key from a fresh locked row."""
        env = self._resolved_kalshi_env(kalshi_env)
        await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=env,
        )
        result = await self.session.execute(
            select(DeploymentControl)
            .where(DeploymentControl.id == env)
            .with_for_update()
        )
        control = result.scalar_one()
        notes = dict(control.notes or {})
        previous_value = notes.get(key)
        notes[key] = updater(previous_value)
        control.notes = notes
        await self.session.flush()
        return control, previous_value
