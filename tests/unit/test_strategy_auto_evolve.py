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


class FakeAuditService:
    def __init__(self, issues: list[dict] | None = None) -> None:
        self.issues = issues or []
        self.calls = 0

    async def build_report(self, **_kwargs):
        self.calls += 1
        return {
            "issues": self.issues,
            "counts": {"fills": 1},
            "pnl": {"net_pnl_dollars": "0.0000"},
            "attribution": {"missing_fill_strategy_count": 0},
            "execution_funnel": {"failed_order_count": 0},
            "stop_loss": {"event_count": 0, "clusters": []},
        }


class FakeCodexService:
    def __init__(self, *, available: bool = True, backtest_status: str = "ok") -> None:
        self.available = available
        self.backtest_status = backtest_status
        self.execute_calls = 0
        self.accept_calls = 0
        self.activate_calls = 0
        self.snapshots: list[dict] = []

    def is_available(self) -> bool:
        return self.available

    async def execute_modes_for_snapshot(self, **kwargs):
        self.execute_calls += 1
        self.snapshots.append(kwargs.get("dashboard_snapshot") or {})
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
        strategy_auto_evolve_activate_suggestions=True,
        strategy_auto_evolve_assign_eligible=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint("strategy_regression", None, {"ran_at": datetime.now(UTC).isoformat()})
        await session.commit()

    async def build(*, dashboard=None, codex=None, audit=None):
        service = StrategyAutoEvolveService(
            settings=settings,
            session_factory=session_factory,
            strategy_regression_service=FakeRegressionService(),
            strategy_codex_service=codex or FakeCodexService(),
            strategy_dashboard_service=FakeDashboardService(dashboard or _dashboard_payload()),
            trading_audit_service=audit or FakeAuditService(),
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


@pytest.mark.asyncio
async def test_auto_evolve_trading_audit_blocker_skips_before_codex(auto_evolve_harness) -> None:
    codex = FakeCodexService()
    audit = FakeAuditService([
        {
            "severity": "critical",
            "code": "missing_fill_strategy_attribution",
            "summary": "Fills without strategy attribution",
        }
    ])
    service = await auto_evolve_harness.build(codex=codex, audit=audit)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "skipped"
    assert result["reason"] == "trading_audit_blocked"
    assert result["trading_audit"]["blocked"] is True
    assert codex.execute_calls == 0
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        checkpoint = await repo.get_checkpoint(service.checkpoint_name)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        await session.commit()
    assert checkpoint is not None
    assert checkpoint.payload["reason"] == "trading_audit_blocked"
    assert assignment is None


@pytest.mark.asyncio
async def test_auto_evolve_medium_audit_issue_is_context_only(auto_evolve_harness) -> None:
    codex = FakeCodexService()
    audit = FakeAuditService([
        {
            "severity": "medium",
            "code": "ops_warning_error_noise",
            "summary": "Noisy ops events",
        }
    ])
    service = await auto_evolve_harness.build(codex=codex, audit=audit)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed"
    assert result["trading_audit"]["blocked"] is False
    assert codex.execute_calls == 1
    assert codex.snapshots[0]["trading_audit"]["issue_count"] == 1


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_settings_assign_eligible_requires_activate_suggestions() -> None:
    """assign_eligible=True with activate_suggestions=False must raise at construction time."""
    import pytest
    with pytest.raises(Exception, match="activate_suggestions"):
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            strategy_auto_evolve_assign_eligible=True,
            strategy_auto_evolve_activate_suggestions=False,
        )


# ---------------------------------------------------------------------------
# Delta cap
# ---------------------------------------------------------------------------


class FakeCodexServiceWithThresholds(FakeCodexService):
    """FakeCodexService that includes threshold data in the suggestion result."""

    def __init__(self, *, thresholds: dict | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._thresholds = thresholds

    async def execute_modes_for_snapshot(self, **kwargs):
        self.execute_calls += 1
        self.snapshots.append(kwargs.get("dashboard_snapshot") or {})
        candidate: dict = {}
        if self._thresholds is not None:
            candidate = {"thresholds": self._thresholds}
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
                "result": {
                    "kind": "suggest",
                    "backtest": {"status": self.backtest_status},
                    "candidate": candidate,
                },
                "can_accept": self.backtest_status == "ok",
            },
        ]


_BASE_THRESHOLDS = {
    "risk_min_edge_bps": 100,
    "risk_max_order_notional_dollars": 50.0,
    "risk_max_position_notional_dollars": 200.0,
    "trigger_max_spread_bps": 500,
    "trigger_cooldown_seconds": 300,
    "strategy_quality_edge_buffer_bps": 20,
    "strategy_min_remaining_payout_bps": 100,
    "risk_safe_capital_reserve_ratio": 0.5,
    "risk_risky_capital_max_ratio": 0.5,
}


@pytest.mark.asyncio
async def test_delta_cap_rejects_over_cap_suggestion(auto_evolve_harness) -> None:
    """Suggestion that moves a threshold >30% from active baseline is rejected before accept_run."""
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.seed_strategies([{
            "name": "baseline-strategy",
            "description": "Active baseline for delta cap test",
            "thresholds": _BASE_THRESHOLDS,
            "is_active": True,
            "source": "builtin",
        }])
        await session.commit()

    # Propose risk_min_edge_bps = 200 (100% increase from 100, exceeds 30% cap)
    over_cap_thresholds = {**_BASE_THRESHOLDS, "risk_min_edge_bps": 200}
    codex = FakeCodexServiceWithThresholds(thresholds=over_cap_thresholds)
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert codex.accept_calls == 0
    assert codex.activate_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "delta_cap_exceeded")
    assert any(v["field"] == "risk_min_edge_bps" for v in error["violations"])


@pytest.mark.asyncio
async def test_delta_cap_passes_within_cap_suggestion(auto_evolve_harness) -> None:
    """Suggestion that moves thresholds within the 30% cap proceeds to accept_run."""
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.seed_strategies([{
            "name": "baseline-strategy",
            "description": "Active baseline for delta cap test",
            "thresholds": _BASE_THRESHOLDS,
            "is_active": True,
            "source": "builtin",
        }])
        await session.commit()

    # Propose risk_min_edge_bps = 120 (20% increase from 100, within 30% cap)
    within_cap_thresholds = {**_BASE_THRESHOLDS, "risk_min_edge_bps": 120}
    codex = FakeCodexServiceWithThresholds(thresholds=within_cap_thresholds)
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert codex.accept_calls == 1


# ---------------------------------------------------------------------------
# Per-cycle city cap
# ---------------------------------------------------------------------------


def _multi_city_dashboard(cities: list[tuple[str, float, float]]) -> dict:
    """Build a dashboard snapshot with multiple eligible cities.

    cities: list of (series_ticker, gap_to_assignment, gap_to_runner_up)
    """
    return {
        "summary": {"window_days": 180},
        "leaderboard": [],
        "city_matrix": [
            {
                "series_ticker": ticker,
                "assignment": {},
                "approval_eligible": True,
                "recommendation": {
                    "strategy_name": "auto-lab",
                    "status": "strong_recommendation",
                    "label": "Strong recommendation",
                },
                "gap_to_assignment": gap_assign,
                "gap_to_runner_up": gap_runner,
            }
            for ticker, gap_assign, gap_runner in cities
        ],
    }


@pytest.mark.asyncio
async def test_city_cap_limits_assignments_to_top_n(auto_evolve_harness) -> None:
    """With 5 eligible cities and max_cities_per_cycle=3, only the top 3 by gap_to_assignment are assigned."""
    cities = [
        ("CITY-A", 0.10, 0.05),  # 4th best
        ("CITY-B", 0.25, 0.10),  # 2nd best
        ("CITY-C", 0.05, 0.02),  # 5th best
        ("CITY-D", 0.30, 0.15),  # 1st best
        ("CITY-E", 0.20, 0.08),  # 3rd best
    ]
    dashboard = _multi_city_dashboard(cities)
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assigned = {c["series_ticker"] for c in result["assignment_changes"]}
    assert assigned == {"CITY-D", "CITY-B", "CITY-E"}

    capped = [s for s in result["assignment_skips"] if s.get("reason") == "cycle_cap_exceeded"]
    assert {s["series_ticker"] for s in capped} == {"CITY-A", "CITY-C"}


@pytest.mark.asyncio
async def test_city_cap_assigns_all_when_below_limit(auto_evolve_harness) -> None:
    """With 2 eligible cities and max_cities_per_cycle=3, all 2 are assigned."""
    cities = [
        ("CITY-X", 0.15, 0.08),
        ("CITY-Y", 0.10, 0.05),
    ]
    dashboard = _multi_city_dashboard(cities)
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert len(result["assignment_changes"]) == 2
    assert not any(s.get("reason") == "cycle_cap_exceeded" for s in result["assignment_skips"])
