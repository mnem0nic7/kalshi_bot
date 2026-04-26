from __future__ import annotations

import pytest

from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository


class FakeDeploymentControlSession:
    def __init__(self) -> None:
        self.controls: dict[str, DeploymentControl] = {}
        self.flush_count = 0

    async def get(self, model: type[DeploymentControl], key: str) -> DeploymentControl | None:
        assert model is DeploymentControl
        return self.controls.get(key)

    def add(self, record: DeploymentControl) -> None:
        self.controls[record.id] = record

    async def flush(self) -> None:
        self.flush_count += 1


@pytest.mark.asyncio
async def test_deployment_control_repository_slice_preserves_lock_and_notes_behavior() -> None:
    session = FakeDeploymentControlSession()
    repo = PlatformRepository(session, kalshi_env="demo")  # type: ignore[arg-type]

    control = await repo.ensure_deployment_control("blue", initial_active_color="blue")
    await repo.update_deployment_notes(
        {
            "agent_packs": {
                "blue_version": "blue-pack",
                "green_version": "green-pack",
            }
        }
    )

    assert await repo.acquire_execution_lock("worker-a", "blue") is True

    promoted = await repo.set_active_color("green")

    assert promoted is control
    assert promoted.active_color == "green"
    assert promoted.execution_lock_holder is None
    assert promoted.notes["agent_packs"]["active_version"] == "green-pack"
    assert promoted.notes["agent_packs"]["champion_version"] == "green-pack"
    assert await repo.acquire_execution_lock("worker-a", "blue") is False
    assert await repo.acquire_execution_lock("worker-a", "green") is True

    killed = await repo.set_kill_switch(True)
    assert killed.kill_switch_enabled is True
    assert killed.execution_lock_holder is None

    cleared = await repo.set_kill_switch(False)
    assert cleared.kill_switch_enabled is False
    assert "kill_switch_cleared_at" in cleared.notes


@pytest.mark.asyncio
async def test_deployment_control_repository_slice_preserves_env_scoping() -> None:
    session = FakeDeploymentControlSession()
    repo = PlatformRepository(session, kalshi_env="demo")  # type: ignore[arg-type]

    demo = await repo.ensure_deployment_control("blue", initial_active_color="blue")
    production = await repo.ensure_deployment_control(
        "green",
        kalshi_env="production",
        initial_active_color="green",
        initial_kill_switch_enabled=True,
    )

    await repo.set_kill_switch(True)

    assert demo.id == "demo"
    assert demo.kill_switch_enabled is True
    assert production.id == "production"
    assert production.active_color == "green"
    assert production.shadow_color == "green"
    assert production.kill_switch_enabled is True

    fetched_production = await repo.get_deployment_control(kalshi_env="production")
    assert fetched_production is production
