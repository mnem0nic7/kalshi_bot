from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.trade_behavior import evaluate_empirical_gate


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
TICKER = "KXHIGHNY-26APR24-T67"


@pytest.fixture
async def trade_behavior_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-behavior.db",
        trade_behavior_production_entry_freeze_enabled=False,
        trade_behavior_empirical_gate_min_settled_fills=2,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    yield settings, session_factory
    await engine.dispose()


def _fill(idx: int, *, settlement_result: str, yes_price: str = "0.5000") -> FillRecord:
    created_at = NOW - timedelta(days=idx)
    return FillRecord(
        id=f"fill-{idx}",
        kalshi_env="production",
        trade_id=f"trade-{idx}",
        market_ticker=TICKER,
        side="yes",
        action="buy",
        yes_price_dollars=Decimal(yes_price),
        count_fp=Decimal("10.00"),
        settlement_result=settlement_result,
        strategy_code="A",
        raw={"fee_cost": "0.2500"},
        created_at=created_at,
        updated_at=created_at,
    )


@pytest.mark.asyncio
async def test_empirical_gate_blocks_under_sampled_production_live_entries(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="production",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=False,
            now=NOW,
        )

    assert decision.status == "blocked"
    assert decision.reason == "empirical_gate_under_sampled"
    assert decision.blocks_live_entries is True


@pytest.mark.asyncio
async def test_empirical_gate_keeps_under_sampled_demo_shadow_report_only(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="demo",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=True,
            now=NOW,
        )

    assert decision.status == "shadow_only"
    assert decision.reason == "empirical_gate_under_sampled"
    assert decision.blocks_live_entries is False


@pytest.mark.asyncio
async def test_empirical_gate_blocks_negative_actual_settled_evidence(trade_behavior_harness) -> None:
    settings, session_factory = trade_behavior_harness
    async with session_factory() as session:
        session.add_all([
            _fill(1, settlement_result="loss"),
            _fill(2, settlement_result="loss"),
        ])
        await session.commit()

        decision = await evaluate_empirical_gate(
            session=session,
            settings=settings,
            kalshi_env="production",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            strategy_code="A",
            shadow_mode=False,
            now=NOW,
        )

    assert decision.status == "blocked"
    assert decision.reason == "empirical_gate_negative_actual_net_pnl"
    assert decision.actual_sample_count == 2
    assert decision.actual_net_pnl is not None
    assert decision.actual_net_pnl < Decimal("0")
