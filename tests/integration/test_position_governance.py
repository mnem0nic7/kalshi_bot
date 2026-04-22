from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
    refresh_stop_loss_checkpoints,
)


def _checkpoint_payload(*, stopped_side: str, submitted_at: datetime, client_order_id: str) -> dict[str, str]:
    iso = submitted_at.isoformat()
    return {
        "submitted_at": iso,
        "stopped_at": iso,
        "stopped_side": stopped_side,
        "client_order_id": client_order_id,
        "outcome_status": STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
    }


@pytest.mark.asyncio
async def test_refresh_stop_loss_checkpoints_accepts_opposite_side_sell_fill_for_stop_loss_exit(tmp_path) -> None:
    ticker = "KXHIGHTSFO-26APR22-T67"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/position_governance_fill_side.db", kalshi_env="demo")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    submitted_at = datetime.now(UTC) - timedelta(minutes=2)
    client_order_id = "stop-loss-order-1"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            f"stop_loss_submit:{settings.kalshi_env}:{ticker}",
            cursor=None,
            payload=_checkpoint_payload(
                stopped_side="yes",
                submitted_at=submitted_at,
                client_order_id=client_order_id,
            ),
        )
        await repo.set_checkpoint(
            f"stop_loss_reentry:{settings.kalshi_env}:{ticker}",
            cursor=None,
            payload=_checkpoint_payload(
                stopped_side="yes",
                submitted_at=submitted_at,
                client_order_id=client_order_id,
            ),
        )
        await repo.save_order(
            ticket_id=None,
            client_order_id=client_order_id,
            market_ticker=ticker,
            status="executed",
            side="yes",
            action="sell",
            yes_price_dollars=Decimal("0.0200"),
            count_fp=Decimal("5.57"),
            raw={},
            kalshi_env=settings.kalshi_env,
        )
        await repo.save_fill(
            market_ticker=ticker,
            side="no",
            action="sell",
            yes_price_dollars=Decimal("0.0200"),
            count_fp=Decimal("5.57"),
            raw={},
            trade_id="trade-opposite-side",
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        refreshed = await refresh_stop_loss_checkpoints(
            repo,
            settings=settings,
            kalshi_env=settings.kalshi_env,
            subaccount=settings.kalshi_subaccount,
            market_tickers=[ticker],
        )
        submit_cp = await repo.get_checkpoint(f"stop_loss_submit:{settings.kalshi_env}:{ticker}")
        reentry_cp = await repo.get_checkpoint(f"stop_loss_reentry:{settings.kalshi_env}:{ticker}")
        await session.commit()

    assert len(refreshed) == 1
    assert refreshed[0].market_ticker == ticker
    assert refreshed[0].outcome_status == STOP_LOSS_OUTCOME_FILLED_EXIT
    assert refreshed[0].repaired is False
    assert submit_cp is not None
    assert reentry_cp is not None
    assert submit_cp.payload["outcome_status"] == STOP_LOSS_OUTCOME_FILLED_EXIT
    assert reentry_cp.payload["outcome_status"] == STOP_LOSS_OUTCOME_FILLED_EXIT
    assert submit_cp.payload["exit_fill_trade_id"] == "trade-opposite-side"
    assert reentry_cp.payload["exit_fill_trade_id"] == "trade-opposite-side"
    assert submit_cp.payload["order_status"] == "executed"

    await engine.dispose()


@pytest.mark.asyncio
async def test_refresh_stop_loss_checkpoints_marks_executed_sell_order_as_filled_exit_when_position_is_closed(tmp_path) -> None:
    ticker = "KXHIGHTOKC-26APR23-T83"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/position_governance_executed_order.db", kalshi_env="demo")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    submitted_at = datetime.now(UTC) - timedelta(minutes=2)
    client_order_id = "stop-loss-order-2"

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.set_checkpoint(
            f"stop_loss_submit:{settings.kalshi_env}:{ticker}",
            cursor=None,
            payload=_checkpoint_payload(
                stopped_side="no",
                submitted_at=submitted_at,
                client_order_id=client_order_id,
            ),
        )
        await repo.set_checkpoint(
            f"stop_loss_reentry:{settings.kalshi_env}:{ticker}",
            cursor=None,
            payload=_checkpoint_payload(
                stopped_side="no",
                submitted_at=submitted_at,
                client_order_id=client_order_id,
            ),
        )
        await repo.save_order(
            ticket_id=None,
            client_order_id=client_order_id,
            market_ticker=ticker,
            status="executed",
            side="no",
            action="sell",
            yes_price_dollars=Decimal("0.7900"),
            count_fp=Decimal("13.31"),
            raw={},
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await refresh_stop_loss_checkpoints(
            repo,
            settings=settings,
            kalshi_env=settings.kalshi_env,
            subaccount=settings.kalshi_subaccount,
            market_tickers=[ticker],
        )
        submit_cp = await repo.get_checkpoint(f"stop_loss_submit:{settings.kalshi_env}:{ticker}")
        reentry_cp = await repo.get_checkpoint(f"stop_loss_reentry:{settings.kalshi_env}:{ticker}")
        await session.commit()

    assert submit_cp is not None
    assert reentry_cp is not None
    assert submit_cp.payload["outcome_status"] == STOP_LOSS_OUTCOME_FILLED_EXIT
    assert reentry_cp.payload["outcome_status"] == STOP_LOSS_OUTCOME_FILLED_EXIT
    assert submit_cp.payload["order_status"] == "executed"
    assert "exit_fill_trade_id" not in submit_cp.payload

    await engine.dispose()
