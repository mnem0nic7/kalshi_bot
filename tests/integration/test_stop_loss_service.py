from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, OpsEvent
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.stop_loss import StopLossService


class FakeKalshiClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def create_order(self, payload: dict[str, str]) -> dict[str, object]:
        self.calls.append(payload)
        return {"order": {"status": "submitted", "order_id": "stop-loss-order"}}


@pytest.mark.asyncio
async def test_stop_loss_skips_when_color_is_inactive(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/stop_loss_inactive.db",
        app_color="blue",
        kalshi_env="demo",
        stop_loss_threshold_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    kalshi = FakeKalshiClient()
    service = StopLossService(settings, session_factory, kalshi)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control("green", initial_active_color="green")
        control.active_color = "green"
        await repo.upsert_position(
            market_ticker="KXHIGHTSFO-26APR23-T70",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("10.00"),
            average_price_dollars=Decimal("0.8000"),
            raw={},
        )
        await repo.upsert_market_state(
            "KXHIGHTSFO-26APR23-T70",
            kalshi_env=settings.kalshi_env,
            snapshot={"market_ticker": "KXHIGHTSFO-26APR23-T70"},
            yes_bid_dollars=Decimal("0.1200"),
            yes_ask_dollars=Decimal("0.1400"),
            last_trade_dollars=Decimal("0.1300"),
        )
        base = datetime.now(UTC) - timedelta(minutes=5)
        for offset_minutes, mid in enumerate(["0.9200", "0.9100", "0.9000", "0.8900", "0.8800"]):
            await repo.record_market_price_snapshot(
                market_ticker="KXHIGHTSFO-26APR23-T70",
                kalshi_env=settings.kalshi_env,
                yes_bid_dollars=Decimal(mid) - Decimal("0.0100"),
                yes_ask_dollars=Decimal(mid) + Decimal("0.0100"),
                mid_dollars=Decimal(mid),
                last_trade_dollars=Decimal(mid),
                volume=10,
                observed_at=base + timedelta(minutes=offset_minutes),
            )
        await session.commit()

    triggered = await service.check_once()

    async with session_factory() as session:
        ops_events = list((await session.execute(select(OpsEvent))).scalars())
        checkpoints = list((await session.execute(select(Checkpoint))).scalars())
        await session.commit()

    assert triggered == []
    assert kalshi.calls == []
    assert ops_events == []
    assert checkpoints == []

    await engine.dispose()
