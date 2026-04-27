from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, RiskStatus, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import RiskVerdictPayload, RoomCreate, TradeEligibilityVerdict
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.decision_trace import replay_decision_trace
from kalshi_bot.services.signal import StrategySignal


class NoopTrainingCorpusService:
    async def persist_strategy_audit_for_room(self, *args, **kwargs) -> None:
        return None


class BlockingRiskEngine:
    def evaluate(self, **kwargs) -> RiskVerdictPayload:
        return RiskVerdictPayload(status=RiskStatus.BLOCKED, reasons=["test block"])


def _thresholds() -> RuntimeThresholds:
    return RuntimeThresholds(
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50.0,
        risk_max_position_notional_dollars=100.0,
        trigger_max_spread_bps=1200,
        trigger_cooldown_seconds=0,
        strategy_quality_edge_buffer_bps=0,
        strategy_min_remaining_payout_bps=100,
    )


async def _seed_balance(repo: PlatformRepository, *, kalshi_env: str, dollars: Decimal = Decimal("1000.00")) -> None:
    await repo.set_checkpoint(
        f"reconcile:{kalshi_env}",
        None,
        {
            "balance": {"balance": int(dollars * Decimal("100")), "portfolio_value": 0},
            "reconciled_at": "2026-04-27T00:00:00+00:00",
        },
    )


def _supervisor(settings: Settings, session_factory, risk_engine=SimpleNamespace()) -> WorkflowSupervisor:
    return WorkflowSupervisor(
        settings=settings,
        session_factory=session_factory,
        kalshi=SimpleNamespace(),
        weather=SimpleNamespace(),
        weather_directory=SimpleNamespace(),
        agent_pack_service=SimpleNamespace(),
        signal_engine=SimpleNamespace(),
        risk_engine=risk_engine,
        execution_service=SimpleNamespace(),
        memory_service=SimpleNamespace(),
        research_coordinator=SimpleNamespace(),
        training_corpus_service=NoopTrainingCorpusService(),
        agents=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_deterministic_stand_down_persists_decision_trace(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/stand_down.db", llm_trading_enabled=False)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = _supervisor(settings, session_factory)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        room = await repo.create_room(
            RoomCreate(name="Stand Down Trace", market_ticker="WX-STAND"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        signal = StrategySignal(
            fair_yes_dollars=Decimal("0.5100"),
            confidence=0.5,
            edge_bps=5,
            recommended_action=None,
            recommended_side=None,
            target_yes_price_dollars=None,
            summary="No candidate clears edge.",
            eligibility=TradeEligibilityVerdict(
                eligible=False,
                strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
                resolution_state=WeatherResolutionState.UNRESOLVED,
                stand_down_reason=StandDownReason.NO_ACTIONABLE_EDGE,
                evaluation_outcome="pre_risk_filtered",
                candidate_trace={
                    "outcome": "no_candidate",
                    "eligibility_outcome": "pre_risk_filtered",
                    "eligibility_stand_down_reason": "no_actionable_edge",
                },
            ),
            evaluation_outcome="pre_risk_filtered",
        )
        await supervisor._run_deterministic_fast_path(  # noqa: SLF001
            repo=repo,
            session=session,
            room=room,
            control=SimpleNamespace(),
            signal=signal,
            thresholds=_thresholds(),
            market_observed_at=datetime(2026, 4, 27, 18, 0, tzinfo=UTC),
            source_snapshot_ids={"market_state": {"market_ticker": "WX-STAND"}},
        )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        trace = await repo.get_latest_decision_trace_for_room(room.id)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        await session.commit()

    assert ticket is None
    assert trace is not None
    assert trace.decision_kind == "stand_down"
    assert trace.trace["normalized_intent"]["stand_down_reason"] == "no_actionable_edge"
    assert replay_decision_trace(trace.trace, expected_trace_hash=trace.trace_hash).ok
    await engine.dispose()


@pytest.mark.asyncio
async def test_deterministic_risk_block_persists_decision_trace(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/risk_block.db",
        llm_trading_enabled=False,
        risk_order_pct=1.0,
        risk_position_pct=1.0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = _supervisor(settings, session_factory, risk_engine=BlockingRiskEngine())

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await _seed_balance(repo, kalshi_env=settings.kalshi_env)
        room = await repo.create_room(
            RoomCreate(name="Risk Block Trace", market_ticker="WX-BLOCK"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        signal = StrategySignal(
            fair_yes_dollars=Decimal("0.9000"),
            confidence=0.9,
            edge_bps=7000,
            recommended_action=TradeAction.BUY,
            recommended_side=ContractSide.YES,
            target_yes_price_dollars=Decimal("0.2000"),
            summary="Candidate clears signal.",
            eligibility=TradeEligibilityVerdict(
                eligible=True,
                strategy_mode=StrategyMode.DIRECTIONAL_UNRESOLVED,
                resolution_state=WeatherResolutionState.UNRESOLVED,
                evaluation_outcome="candidate_selected",
                candidate_trace={"outcome": "candidate_selected", "selected_side": "yes"},
            ),
            evaluation_outcome="candidate_selected",
            candidate_trace={"outcome": "candidate_selected", "selected_side": "yes"},
        )
        await supervisor._run_deterministic_fast_path(  # noqa: SLF001
            repo=repo,
            session=session,
            room=room,
            control=SimpleNamespace(),
            signal=signal,
            thresholds=_thresholds(),
            market_observed_at=datetime(2026, 4, 27, 18, 0, tzinfo=UTC),
            source_snapshot_ids={"market_state": {"market_ticker": "WX-BLOCK"}},
        )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        trace = await repo.get_latest_decision_trace_for_room(room.id)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        await session.commit()

    assert ticket is not None
    assert risk_verdict is not None
    assert trace is not None
    assert trace.ticket_id == ticket.id
    assert trace.decision_kind == "risk_block"
    assert trace.trace["normalized_intent"]["risk_status"] == "blocked"
    assert trace.trace["normalized_intent"]["risk_reasons"] == ["test block"]
    assert replay_decision_trace(trace.trace, expected_trace_hash=trace.trace_hash).ok
    await engine.dispose()
