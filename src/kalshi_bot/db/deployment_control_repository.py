from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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

    async def set_kill_switch(self, enabled: bool, *, kalshi_env: str | None = None) -> DeploymentControl:
        control = await self.ensure_deployment_control(
            DeploymentColor.BLUE.value,
            kalshi_env=kalshi_env,
        )
        control.kill_switch_enabled = enabled
        if enabled:
            control.execution_lock_holder = None
        else:
            # Record when the kill switch was cleared so execution can require a
            # post-clear reconcile before the first live order goes out.
            notes = dict(control.notes or {})
            notes["kill_switch_cleared_at"] = datetime.now(UTC).isoformat()
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
