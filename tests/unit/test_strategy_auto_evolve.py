from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord
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
    def __init__(
        self,
        *,
        available: bool = True,
        backtest_status: str = "ok",
        corpus_build_id: str | None = None,
        baseline_corpus_build_id: str | None = None,
        bind_current_corpus: bool = True,
        finished_at: str | datetime | None = None,
        resolved_regression_rooms: int | None = 30,
        candidate_hypothetical_trades: int | None = 12,
        candidate_win_rate: float | None = 0.62,
        baseline_win_rate: float | None = 0.50,
        promotion_candidate: bool = True,
        cluster_count: int | None = 30,
        sortino: float | None = 0.75,
        total_pnl_dollars: float | None = 2.5,
        below_support_floor: bool = False,
        insufficient_for_ranking: bool = False,
        suggestion_status: str = "completed",
    ) -> None:
        self.available = available
        self.backtest_status = backtest_status
        self.corpus_build_id = corpus_build_id
        self.baseline_corpus_build_id = baseline_corpus_build_id
        self.bind_current_corpus = bind_current_corpus
        self.finished_at = finished_at
        self.resolved_regression_rooms = resolved_regression_rooms
        self.candidate_hypothetical_trades = candidate_hypothetical_trades
        self.candidate_win_rate = candidate_win_rate
        self.baseline_win_rate = baseline_win_rate
        self.promotion_candidate = promotion_candidate
        self.cluster_count = cluster_count
        self.sortino = sortino
        self.total_pnl_dollars = total_pnl_dollars
        self.below_support_floor = below_support_floor
        self.insufficient_for_ranking = insufficient_for_ranking
        self.suggestion_status = suggestion_status
        self.execute_calls = 0
        self.accept_calls = 0
        self.activate_calls = 0
        self.snapshots: list[dict] = []

    def is_available(self) -> bool:
        return self.available

    async def execute_modes_for_snapshot(self, **kwargs):
        self.execute_calls += 1
        self.snapshots.append(kwargs.get("dashboard_snapshot") or {})
        finished_at = self.finished_at or datetime.now(UTC)
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
                "status": self.suggestion_status,
                "finished_at": finished_at.isoformat() if isinstance(finished_at, datetime) else finished_at,
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "result": {
                    "kind": "suggest",
                    "backtest": self._backtest_payload(),
                },
                "can_accept": self.backtest_status == "ok",
            },
        ]

    def _backtest_payload(self) -> dict:
        backtest = {
            "status": self.backtest_status,
            "corpus_build_id": self.corpus_build_id,
            "resolved_regression_rooms": self.resolved_regression_rooms,
            "candidate_hypothetical_trades": self.candidate_hypothetical_trades,
            "candidate_metrics": {
                "overall_win_rate": self.candidate_win_rate,
                "total_resolved_trade_count": self.candidate_hypothetical_trades,
                "total_pnl_dollars": self.total_pnl_dollars,
                "cluster_count": self.cluster_count,
                "sortino": self.sortino,
                "promotion_candidate": self.promotion_candidate,
                "below_support_floor": self.below_support_floor,
                "insufficient_for_ranking": self.insufficient_for_ranking,
            },
            "candidate_result_rows": [
                {
                    "strategy_name": "auto-lab",
                    "total_rows_contributing": self.candidate_hypothetical_trades,
                    "total_net_pnl_dollars": self.total_pnl_dollars,
                    "cluster_count": self.cluster_count,
                    "sortino": self.sortino,
                    "promotion_candidate": self.promotion_candidate,
                    "below_support_floor": self.below_support_floor,
                    "insufficient_for_ranking": self.insufficient_for_ranking,
                }
            ],
            "assignment_weighted_baseline": {
                "corpus_build_id": self.baseline_corpus_build_id or self.corpus_build_id,
                "overall_win_rate": self.baseline_win_rate,
            },
        }
        return backtest

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
                    "win_rate": 0.62,
                    "resolved_trade_count": 12,
                    "total_pnl_dollars": 2.5,
                },
                "city_corpus_days": 21,
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
        strategy_auto_evolve_greenfield_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint("strategy_regression", None, {"ran_at": datetime.now(UTC).isoformat()})
        corpus = await repo.create_decision_corpus_build(
            version="test-corpus",
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 24),
            source={"kind": "test"},
            filters={},
        )
        await repo.mark_decision_corpus_build_successful(corpus.id, row_count=30)
        await repo.promote_decision_corpus_build(corpus.id, kalshi_env=settings.kalshi_env, actor="test")
        await session.commit()

    async def build(*, dashboard=None, codex=None, audit=None):
        codex_service = codex or FakeCodexService()
        if (
            isinstance(codex_service, FakeCodexService)
            and codex_service.bind_current_corpus
            and codex_service.corpus_build_id is None
        ):
            codex_service.corpus_build_id = corpus.id
        dashboard_payload = dashboard or _dashboard_payload()
        dashboard_payload.setdefault("summary", {}).setdefault("corpus_build_id", corpus.id)
        dashboard_payload["summary"].setdefault("last_regression_run", datetime.now(UTC).isoformat())
        service = StrategyAutoEvolveService(
            settings=settings,
            session_factory=session_factory,
            strategy_regression_service=FakeRegressionService(),
            strategy_codex_service=codex_service,
            strategy_dashboard_service=FakeDashboardService(dashboard_payload),
            trading_audit_service=audit or FakeAuditService(),
        )
        return service

    yield SimpleNamespace(settings=settings, session_factory=session_factory, build=build, corpus_build_id=corpus.id)
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
        promotion = await repo.get_strategy_promotion(result["promotion_id"])
        events = await repo.list_city_assignment_events(
            series_ticker="KXHIGHNY",
            promotion_id=result["promotion_id"],
        )
        await session.commit()

    assert assignment is not None
    assert assignment.strategy_name == "auto-lab"
    assert assignment.assigned_by == "auto_evolve"
    assert assignment.evidence_corpus_build_id == auto_evolve_harness.corpus_build_id
    assert assignment.evidence_run_at is not None
    assert promotion is not None
    assert promotion.watchdog_status == "pending"
    assert promotion.watchdog_due_at is not None
    assert promotion.watchdog_extended_due_at is not None
    assert promotion.new_city_assignments["KXHIGHNY"]["new_strategy"] == "auto-lab"
    assert events[0].actor == "strategy_auto_evolve"
    assert events[0].event_type == "auto_evolve_assign"
    assert events[0].new_strategy == "auto-lab"
    assert events[0].event_metadata["corpus_build_id"] == auto_evolve_harness.corpus_build_id
    assert events[0].event_metadata["basis_run_at"] is not None
    assert checkpoint is not None
    assert checkpoint.payload["status"] == "completed"


@pytest.mark.asyncio
async def test_auto_evolve_does_not_assign_when_regression_refresh_leaves_checkpoint_stale(
    auto_evolve_harness,
) -> None:
    codex = FakeCodexService()
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            "strategy_regression",
            None,
            {"ran_at": (datetime.now(UTC) - timedelta(days=3)).isoformat()},
        )
        await session.commit()
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "skipped"
    assert result["reason"] == "fresh_regression_unavailable"
    assert result["regression"]["refreshed"] is True
    assert codex.execute_calls == 0
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        assignment = await repo.get_city_strategy_assignment("KXHIGHNY")
        promotions = await repo.list_strategy_promotion_records(kalshi_env=auto_evolve_harness.settings.kalshi_env)
        await session.commit()

    assert assignment is None
    assert promotions == []


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
async def test_auto_evolve_same_day_already_completed_state_stays_idempotent(auto_evolve_harness) -> None:
    codex = FakeCodexService(suggestion_status="failed")
    service = await auto_evolve_harness.build(codex=codex)

    first = await service.run_once(trigger_source="nightly")
    second = await service.run_once(trigger_source="nightly")
    third = await service.run_once(trigger_source="nightly")

    assert first["status"] == "completed_with_failures"
    assert first["errors"] == [{"stage": "accept", "reason": "suggestion_not_completed", "status": "failed"}]
    assert second["status"] == "already_completed"
    assert third["status"] == "already_completed"
    assert codex.execute_calls == 1
    assert codex.accept_calls == 0
    assert codex.activate_calls == 0


@pytest.mark.asyncio
async def test_inactive_color_skip_does_not_overwrite_shared_checkpoint(auto_evolve_harness) -> None:
    codex = FakeCodexService()
    service = await auto_evolve_harness.build(codex=codex)

    first = await service.run_once(trigger_source="manual")
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_active_color("green")
        await session.commit()

    skipped = await service.run_once(trigger_source="manual")

    assert first["status"] == "completed"
    assert skipped["status"] == "skipped"
    assert skipped["reason"] == "inactive_color"
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        checkpoint = await repo.get_checkpoint(service.checkpoint_name)
        await session.commit()

    assert checkpoint is not None
    assert checkpoint.payload["status"] == "completed"
    assert checkpoint.payload["assignment_changes"][0]["series_ticker"] == "KXHIGHNY"


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
async def test_auto_evolve_rejects_when_no_promoted_corpus(auto_evolve_harness) -> None:
    codex = FakeCodexService(corpus_build_id=auto_evolve_harness.corpus_build_id)
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            repo.decision_corpus_current_checkpoint_name(kalshi_env=auto_evolve_harness.settings.kalshi_env),
            None,
            {},
        )
        await session.commit()
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    assert result["errors"][0]["reason"] == "insufficient_corpus"


@pytest.mark.asyncio
async def test_auto_evolve_rejects_backtest_on_stale_corpus(auto_evolve_harness) -> None:
    codex = FakeCodexService(corpus_build_id="stale-corpus", baseline_corpus_build_id="stale-corpus")
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "stale_backtest_corpus")
    assert error["backtest_corpus_build_id"] == "stale-corpus"
    assert error["current_corpus_build_id"] == auto_evolve_harness.corpus_build_id


@pytest.mark.asyncio
async def test_auto_evolve_rejects_backtest_without_corpus_link(auto_evolve_harness) -> None:
    codex = FakeCodexService(bind_current_corpus=False)
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "backtest_corpus_missing")
    assert error["current_corpus_build_id"] == auto_evolve_harness.corpus_build_id


@pytest.mark.asyncio
async def test_auto_evolve_rejects_backtest_below_resolved_room_floor(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        resolved_regression_rooms=29,
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "insufficient_corpus")
    assert error["resolved_regression_rooms"] == 29


@pytest.mark.asyncio
async def test_auto_evolve_rejects_backtest_below_candidate_trade_floor(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        candidate_hypothetical_trades=9,
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "insufficient_backtest_trades")
    assert error["candidate_hypothetical_trades"] == 9


@pytest.mark.asyncio
async def test_auto_evolve_rejects_backtest_without_promotion_quality_evidence(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        promotion_candidate=False,
        cluster_count=29,
        sortino=0.25,
        total_pnl_dollars=-0.01,
        below_support_floor=True,
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "failed_quality_floor")
    assert set(error["floor_failures"]) >= {
        "promotion_candidate",
        "cluster_count",
        "sortino",
        "total_net_pnl",
        "below_support_floor",
    }


@pytest.mark.asyncio
async def test_auto_evolve_rejects_candidate_that_does_not_beat_assignment_weighted_baseline(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        candidate_win_rate=0.50,
        baseline_win_rate=0.50,
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "failed_quality_floor")
    assert error["candidate_win_rate"] == 0.50
    assert error["baseline_win_rate"] == 0.50
    assert error["min_improvement_bps"] == auto_evolve_harness.settings.strategy_auto_evolve_min_improvement_bps


@pytest.mark.asyncio
async def test_auto_evolve_rejects_candidate_below_min_assignment_weighted_improvement(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        candidate_win_rate=0.505,
        baseline_win_rate=0.50,
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "failed_quality_floor")
    assert error["improvement_bps"] == 50
    assert error["min_improvement_bps"] == auto_evolve_harness.settings.strategy_auto_evolve_min_improvement_bps


@pytest.mark.asyncio
async def test_auto_evolve_rejects_stale_suggestion_run(auto_evolve_harness) -> None:
    codex = FakeCodexService(
        corpus_build_id=auto_evolve_harness.corpus_build_id,
        finished_at=datetime.now(UTC)
        - timedelta(seconds=auto_evolve_harness.settings.strategy_auto_evolve_accept_max_run_age_seconds + 5),
    )
    service = await auto_evolve_harness.build(codex=codex)

    result = await service.run_once(trigger_source="manual")

    assert result["status"] == "completed_with_failures"
    assert codex.accept_calls == 0
    error = next(e for e in result["errors"] if e.get("reason") == "stale_backtest_run")
    assert error["max_age_seconds"] == auto_evolve_harness.settings.strategy_auto_evolve_accept_max_run_age_seconds


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
        finished_at = self.finished_at or datetime.now(UTC)
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
                "finished_at": finished_at.isoformat() if isinstance(finished_at, datetime) else finished_at,
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "result": {
                    "kind": "suggest",
                    "backtest": self._backtest_payload(),
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

    await service.run_once(trigger_source="manual")

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
                    "win_rate": 0.50 + gap_assign,
                    "resolved_trade_count": 12,
                    "total_pnl_dollars": 2.5,
                },
                "city_corpus_days": 21,
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


@pytest.mark.asyncio
async def test_city_cap_orders_ranking_rows_by_sortino(auto_evolve_harness) -> None:
    dashboard = _multi_city_dashboard([
        ("CITY-A", 0.30, 0.15),
        ("CITY-B", 0.05, 0.02),
        ("CITY-C", 0.20, 0.08),
        ("CITY-D", 0.10, 0.04),
    ])
    sortinos = {
        "CITY-A": 0.55,
        "CITY-B": 1.90,
        "CITY-C": 1.20,
        "CITY-D": 1.60,
    }
    for row in dashboard["city_matrix"]:
        row["ranking_version"] = "clustered_sortino_v1"
        row["candidate_sortino"] = sortinos[row["series_ticker"]]
        row["candidate_cluster_count"] = 36
        row["candidate_total_net_pnl_dollars"] = 4.0
        row["promotion_candidate"] = True
        row["recommendation"].update({
            "ranking_version": "clustered_sortino_v1",
            "sortino": sortinos[row["series_ticker"]],
            "cluster_count": 36,
            "total_net_pnl_dollars": 4.0,
            "promotion_candidate": True,
            "below_support_floor": False,
            "insufficient_for_ranking": False,
        })
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assigned = [change["series_ticker"] for change in result["assignment_changes"]]
    assert assigned == ["CITY-B", "CITY-D", "CITY-C"]
    capped = next(skip for skip in result["assignment_skips"] if skip.get("reason") == "cycle_cap_exceeded")
    assert capped["series_ticker"] == "CITY-A"


@pytest.mark.asyncio
async def test_ranking_candidate_below_quality_floor_is_skipped(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload()
    row = dashboard["city_matrix"][0]
    row["ranking_version"] = "clustered_sortino_v1"
    row["candidate_sortino"] = 0.1
    row["candidate_cluster_count"] = 36
    row["candidate_total_net_pnl_dollars"] = 4.0
    row["promotion_candidate"] = False
    row["recommendation"].update({
        "ranking_version": "clustered_sortino_v1",
        "sortino": 0.1,
        "cluster_count": 36,
        "total_net_pnl_dollars": 4.0,
        "promotion_candidate": False,
        "below_support_floor": False,
        "insufficient_for_ranking": False,
        "win_rate": 0.90,
        "resolved_trade_count": 40,
        "total_pnl_dollars": 10.0,
    })
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"] == []
    skip = next(s for s in result["assignment_skips"] if s["series_ticker"] == "KXHIGHNY")
    assert skip["reason"] == "below_improvement_floor"
    assert set(skip["floor_failures"]) == {"promotion_candidate", "sortino"}


@pytest.mark.asyncio
async def test_greenfield_candidate_missing_evidence_is_skipped(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload()
    city = dashboard["city_matrix"][0]
    city["recommendation"].pop("win_rate")
    city["recommendation"].pop("resolved_trade_count")
    city["recommendation"].pop("total_pnl_dollars")
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"] == []
    skip = next(s for s in result["assignment_skips"] if s["series_ticker"] == "KXHIGHNY")
    assert skip["reason"] == "below_improvement_floor"
    assert set(skip["floor_failures"]) == {
        "greenfield_win_rate",
        "greenfield_resolved_trades",
        "greenfield_pnl",
    }


@pytest.mark.asyncio
async def test_auto_evolve_only_assigns_activated_candidate_strategy(auto_evolve_harness) -> None:
    dashboard = _multi_city_dashboard([
        ("CITY-A", 0.30, 0.15),
        ("CITY-B", 0.25, 0.10),
    ])
    dashboard["city_matrix"][1]["recommendation"]["strategy_name"] = "other-active"
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert {change["series_ticker"] for change in result["assignment_changes"]} == {"CITY-A"}
    skip = next(s for s in result["assignment_skips"] if s["series_ticker"] == "CITY-B")
    assert skip["reason"] == "not_recommended"
    assert skip["candidate_strategy"] == "auto-lab"


@pytest.mark.asyncio
async def test_reassignment_promotion_snapshots_incumbent_baseline_metrics(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload(assigned="incumbent")
    row = dashboard["city_matrix"][0]
    row["expected_improvement"] = 0.08
    row["incumbent_strategy_live_fill_count_30d"] = 7
    row["incumbent_health"] = {
        "status": "degraded",
        "win_rate_30d": 0.52,
        "realized_pnl_30d": 18.4,
        "fill_count_30d": 7,
    }
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"][0]["assignment_type"] == "reassignment"
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        promotion = await repo.get_strategy_promotion(result["promotion_id"])
        await session.commit()

    assert promotion is not None
    previous = promotion.previous_city_assignments["KXHIGHNY"]
    assert previous["strategy_name"] == "incumbent"
    assert previous["incumbent_win_rate_30d"] == 0.52
    assert previous["incumbent_realized_pnl_30d"] == 18.4
    assert previous["incumbent_live_fill_count_30d"] == 7


@pytest.mark.asyncio
async def test_reassignment_skips_incumbent_healthy_from_metric_fallback(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload(assigned="incumbent")
    row = dashboard["city_matrix"][0]
    row["expected_improvement"] = 0.08
    row["incumbent_strategy_live_fill_count_30d"] = 7
    row["incumbent_health"] = {
        "win_rate_30d": 0.46,
        "realized_pnl_30d": 0.01,
        "fill_count_30d": 7,
    }
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"] == []
    skip = next(s for s in result["assignment_skips"] if s["series_ticker"] == "KXHIGHNY")
    assert skip["reason"] == "incumbent_healthy"
    assert skip["incumbent_health"]["status"] == "healthy"
    assert skip["incumbent_health"]["health_source"] == "metrics_fallback"


@pytest.mark.asyncio
async def test_assignment_rejects_stale_dashboard_corpus(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload()
    dashboard["summary"]["corpus_build_id"] = "stale-corpus"
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"] == []
    assert any(
        error.get("stage") == "assign" and error.get("reason") == "stale_assignment_snapshot"
        for error in result["errors"]
    )


@pytest.mark.asyncio
async def test_reassignment_uses_recent_live_fills_when_dashboard_omits_count(auto_evolve_harness) -> None:
    dashboard = _dashboard_payload(assigned="incumbent")
    row = dashboard["city_matrix"][0]
    row["expected_improvement"] = 0.08
    row["incumbent_health"] = {"status": "degraded", "win_rate_30d": 0.48}
    row.pop("incumbent_strategy_live_fill_count_30d", None)
    now = datetime.now(UTC)
    async with auto_evolve_harness.session_factory() as session:
        repo = PlatformRepository(session)
        assignment = await repo.set_city_strategy_assignment("KXHIGHNY", "incumbent", assigned_by="operator")
        assignment.assigned_at = now - timedelta(days=3)
        session.add_all(
            [
                FillRecord(
                    trade_id=f"fallback-incumbent-live-{idx}",
                    kalshi_env=auto_evolve_harness.settings.kalshi_env,
                    market_ticker=f"KXHIGHNY-26APR{20 + idx}-T70",
                    side="yes",
                    action="buy",
                    yes_price_dollars=Decimal("0.4000"),
                    count_fp=Decimal("1.00"),
                    strategy_code="incumbent",
                    settlement_result="win",
                    raw={},
                    created_at=now - timedelta(hours=idx),
                    updated_at=now - timedelta(hours=idx),
                )
                for idx in range(1, 6)
            ]
            + [
                FillRecord(
                    trade_id=f"fallback-legacy-a-live-{idx}",
                    kalshi_env=auto_evolve_harness.settings.kalshi_env,
                    market_ticker=f"KXHIGHNY-26APR{30 + idx}-T70",
                    side="yes",
                    action="buy",
                    yes_price_dollars=Decimal("0.4000"),
                    count_fp=Decimal("1.00"),
                    strategy_code="A",
                    settlement_result="win",
                    raw={},
                    created_at=now - timedelta(hours=idx),
                    updated_at=now - timedelta(hours=idx),
                )
                for idx in range(1, 5)
            ]
        )
        await session.commit()
    service = await auto_evolve_harness.build(dashboard=dashboard)

    result = await service.run_once(trigger_source="manual")

    assert result["assignment_changes"][0]["series_ticker"] == "KXHIGHNY"
    assert not any(skip.get("reason") == "insufficient_live_fills" for skip in result["assignment_skips"])
