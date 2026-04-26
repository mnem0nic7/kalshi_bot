from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.execution import ExecutionService


def _settings(risk_min_edge_bps: int = 50) -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=risk_min_edge_bps,
    )


def _ticket(side: str = "yes", price: str = "0.5800", tif: str = "gtc") -> TradeTicket:
    return TradeTicket(
        market_ticker="WX-TEST",
        action=TradeAction.BUY,
        side=ContractSide(side),
        yes_price_dollars=Decimal(price),
        count_fp=Decimal("10.00"),
        time_in_force=tif,
    )


def _room() -> Room:
    room = MagicMock(spec=Room)
    room.shadow_mode = False
    room.market_ticker = "WX-TEST"
    return room


def _control() -> DeploymentControl:
    ctrl = MagicMock(spec=DeploymentControl)
    ctrl.active_color = "blue"
    return ctrl


def _kalshi(*, order_statuses: list[str], market_ask: str = "0.5900", no_ask: str | None = None) -> MagicMock:
    kalshi = MagicMock()
    kalshi.write_credentials = object()
    kalshi.create_order = AsyncMock(return_value={"order": {"order_id": "ord-1", "status": "resting"}})
    kalshi.cancel_order = AsyncMock(return_value={})
    poll_responses = [{"order": {"status": s}} for s in order_statuses]
    kalshi.get_order = AsyncMock(side_effect=poll_responses)
    market_resp: dict = {"market": {"yes_ask_dollars": market_ask}}
    if no_ask is not None:
        market_resp["market"]["no_ask_dollars"] = no_ask
    kalshi.get_market = AsyncMock(return_value=market_resp)
    return kalshi


@pytest.mark.asyncio
async def test_limit_order_fills_on_first_attempt():
    kalshi = _kalshi(order_statuses=["resting", "resting", "filled"])
    svc = ExecutionService(_settings(), kalshi)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        receipt = await svc.execute(
            room=_room(),
            control=_control(),
            ticket=_ticket(),
            client_order_id="coid-1",
            fair_yes_dollars=Decimal("0.64"),
        )

    assert receipt.status == "filled"
    kalshi.cancel_order.assert_not_called()


@pytest.mark.asyncio
async def test_limit_order_requotes_when_edge_holds():
    # First attempt times out (10 polls, all resting), second attempt fills immediately.
    first_timeout = ["resting"] * 10
    second_fill = ["filled"]
    kalshi = _kalshi(order_statuses=first_timeout + second_fill, market_ask="0.5900")
    kalshi.create_order = AsyncMock(side_effect=[
        {"order": {"order_id": "ord-1", "status": "resting"}},
        {"order": {"order_id": "ord-2", "status": "resting"}},
    ])
    kalshi.get_order = AsyncMock(side_effect=[
        *[{"order": {"status": "resting"}}] * 10,
        {"order": {"status": "filled"}},
    ])
    svc = ExecutionService(_settings(), kalshi)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        receipt = await svc.execute(
            room=_room(),
            control=_control(),
            ticket=_ticket(price="0.5800"),
            client_order_id="coid-1",
            fair_yes_dollars=Decimal("0.6400"),  # edge vs 0.59 ask = 500bps, above 50bps min
        )

    assert receipt.status == "filled"
    assert kalshi.cancel_order.call_count == 1


@pytest.mark.asyncio
async def test_limit_order_aborts_requote_when_edge_lost():
    kalshi = _kalshi(order_statuses=["resting"] * 10, market_ask="0.6350")
    # fair = 0.64, new_ask = 0.635 → edge = 0.005 = 50bps = exactly at min
    # With min_edge_bps=51, edge is below threshold
    svc = ExecutionService(_settings(51), kalshi)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        receipt = await svc.execute(
            room=_room(),
            control=_control(),
            ticket=_ticket(price="0.5800"),
            client_order_id="coid-1",
            fair_yes_dollars=Decimal("0.6400"),
        )

    assert receipt.status == "requote_edge_lost"
    assert kalshi.create_order.call_count == 1  # only the first attempt was placed


@pytest.mark.asyncio
async def test_limit_order_returns_unfilled_after_max_requotes():
    kalshi = _kalshi(order_statuses=["resting"] * 30, market_ask="0.5800")
    kalshi.create_order = AsyncMock(side_effect=[
        {"order": {"order_id": f"ord-{i}", "status": "resting"}} for i in range(1, 4)
    ])
    kalshi.get_order = AsyncMock(return_value={"order": {"status": "resting"}})
    svc = ExecutionService(_settings(), kalshi)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        receipt = await svc.execute(
            room=_room(),
            control=_control(),
            ticket=_ticket(price="0.5800"),
            client_order_id="coid-1",
            fair_yes_dollars=Decimal("0.6400"),
        )

    assert receipt.status == "unfilled_cancelled"
    assert kalshi.create_order.call_count == 3
    assert kalshi.cancel_order.call_count == 3


@pytest.mark.asyncio
async def test_ioc_order_bypasses_limit_state_machine():
    kalshi = _kalshi(order_statuses=[])
    kalshi.create_order = AsyncMock(return_value={"order": {"order_id": "ord-1", "status": "filled"}})
    svc = ExecutionService(_settings(), kalshi)

    receipt = await svc.execute(
        room=_room(),
        control=_control(),
        ticket=_ticket(tif="immediate_or_cancel"),
        client_order_id="coid-1",
    )

    assert receipt.status == "filled"
    kalshi.get_order.assert_not_called()
    kalshi.cancel_order.assert_not_called()


@pytest.mark.asyncio
async def test_shadow_mode_skips_before_any_api_call():
    kalshi = _kalshi(order_statuses=[])
    svc = ExecutionService(_settings(), kalshi)
    room = _room()
    room.shadow_mode = True

    receipt = await svc.execute(
        room=room,
        control=_control(),
        ticket=_ticket(),
        client_order_id="coid-1",
    )

    assert receipt.status == "shadow_skipped"
    kalshi.create_order.assert_not_called()
