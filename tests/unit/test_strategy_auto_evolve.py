from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_auto_evolve import StrategyAutoEvolveService


class FakeRegressionService:
    def __init__(self) -> None:
        self.calls = 0

    async def run_regression(self):
        self.calls += 1
        return {"status": "ok"}


class FakeDashboardService:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls = 0

    async def build_dashboard(self, **_kwargs):
        self.calls += 1
        return self.payload


class FakeCodexService:
    def __init__(self, *, available: bool = True, backtest_status: str = "ok") -> None:
        self.available = available
        self.backtest_status = backtest_status
        self.execute_calls = 0
        self.accept_calls = 0
        self.activate_calls = 0

    def is_available(self) -> bool:
        return self.available

    async def execute_modes_for_snapshot(self, **_kwargs):
        self.execute_calls += 1
        return [
            {
                "id": "eval-run",
                "mode": "evaluate",
                "status": "completed",
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "result": {"kind": "evaluate", "evaluation": {"summary": "ok"}},
            },
            {
                "id": "suggest-run",
                "mode": "suggest",
                "status": "completed",
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "result": {"kind": "suggest", "backtest": {"status": self.backtest_status}},
                "can_accept": self.backtest_status == "ok",
            },
        ]

    async def accept_run(self, _run_id: str):
        self.accept_calls += 1
        return {"status": "accepted", "strategy_name": "auto-lab", "is_active": False}

    async def activate_strategy(self, strategy_name: str):
        self.activate_calls += 1
        return {"status": "activated", "strategy_name": strategy_name, "is_active": True}


def _dashboard_payload(*, assigned: str | None = None) -> dict:
    return {
        "summary": {"window_days": 180},
        "leaderboard": [],
        "city_matrix": [
            {
                "series_ticker": "KXHIGHNY",
                "assignment": {"strategy_name": assigned} if assigned else {},
                "approval_eligible": True,
                "recommendation": {
                    "strategy_name": "auto-lab",
                    "status": "strong_recommendation",
                    "label": "Strong recommendation",
                },
                "gap_to_runner_up": 0.2,
            }
        ],
    }


@pytest.fixture
async def auto_evolve_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto-evolve.db",
        strategy_codex_nightly_timezone="UTC",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint("strategy_regression", None, {"ran_at": datetime.now(UTC).isoformat()})
        await session.commit()

    async def build(*, dashboard=None, codex=None):
        service = StrategyAutoEvolveService(
            settings=settings,
            session_factory=session_factory,
            strategy_regression_service=FakeRegressionService(),
            strategy_codex_service=codex or FakeCodexService(),
            strategy_dashboard_service=FakeDashboardService(dashboard or _dashboard_payload()),
        )
        return service

    yield SimpleNamespace(settings=settings, session_factory=session_factory, build=build)
    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_evolve_accepts_activates_and_assigns(auto_evolve_harness) -> None:
    codex = FakeCodexService()
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed"
    assert result["accepted_strategy"] == "auto-lab"
    assert result["activated_strategy"] == "auto-lab"
    assert result["assignment_changes"][0]["series_ticker"] == "KXHIGHNY"
    assert codex.accept_calls == 1
    assert codex.activate_calls == 1

    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        checkpoint = await repo.get_checkpoint(service.checkpoint_name)
        await session.commit()

    assert assignment is not None
    assert assignment.strategy_name == "auto-lab"
    assert assignment.assigned_by == "auto_evolve"
    assert checkpoint is not None
    assert checkpoint.payload["status"] == "completed"


@pytest.mark.asyncio
async def test_auto_evolve_same_day_is_idempotent(auto_evolve_harness) -> None:
    codex = FakeCodexService()
    service = await auto_evolve_harness.build(codex=codex)

    first = await service.run_once(trigger_source="manual")
    second = await service.run_once(trigger_source="manual")

    assert first["status"] == "completed"
    assert second["status"] == "already_completed"
    assert codex.execute_calls == 1
    assert codex.accept_calls == 1
    assert codex.activate_calls == 1


@pytest.mark.asyncio
async def test_auto_evolve_provider_unavailable_skips_without_assignment(auto_evolve_harness) -> None:
    service = await auto_evolve_harness.build(codex=FakeCodexService(available=False))

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "skipped"
    assert result["reason"] == "codex_unavailable"
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        await session.commit()
    assert assignment is None


@pytest.mark.asyncio
async def test_auto_evolve_failed_backtest_does_not_activate_or_assign(auto_evolve_harness) -> None:
    codex = FakeCodexService(backtest_status="failed")
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert result["activated_strategy"] is None
    assert result["assignment_changes"] == []
    assert codex.accept_calls == 0
    assert codex.activate_calls == 0
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        await session.commit()
    assert assignment is None
