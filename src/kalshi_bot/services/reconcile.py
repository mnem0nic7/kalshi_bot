from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from kalshi_bot.core.fixed_point import as_decimal, quantize_count, quantize_price
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient


def _first_present(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _stringish(value: Any, default: str) -> str:
    return str(value) if value is not None else default


@dataclass(slots=True)
class ReconcileSummary:
    balances_seen: bool
    positions_count: int
    orders_count: int
    fills_count: int
    settlements_count: int
    historical_cutoff_seen: bool


class ReconciliationService:
    def __init__(self, kalshi: KalshiClient) -> None:
        self.kalshi = kalshi

    async def reconcile(self, repo: PlatformRepository, *, subaccount: int = 0) -> ReconcileSummary:
        historical_cutoff = await self.kalshi.get_historical_cutoff()
        balance = await self.kalshi.get_balance()
        positions_payload = await self.kalshi.get_positions(subaccount=subaccount)
        orders_payload = await self.kalshi.get_orders()
        fills_payload = await self.kalshi.get_fills()
        settlements_payload = await self.kalshi.get_settlements()

        await repo.log_exchange_event("reconcile", "historical_cutoff", historical_cutoff)
        await repo.log_exchange_event("reconcile", "balance", balance)
        await repo.log_exchange_event("reconcile", "positions", positions_payload)
        await repo.log_exchange_event("reconcile", "orders", orders_payload)
        await repo.log_exchange_event("reconcile", "fills", fills_payload)
        await repo.log_exchange_event("reconcile", "settlements", settlements_payload)

        positions = _first_present(positions_payload, "market_positions", "positions")
        orders = _first_present(orders_payload, "orders")
        fills = _first_present(fills_payload, "fills", "trades")
        settlements = _first_present(settlements_payload, "settlements")

        for position in positions:
            market_ticker = _stringish(position.get("ticker") or position.get("market_ticker"), "unknown")
            count = position.get("position_fp") or position.get("count_fp") or position.get("net_position_fp") or "0.00"
            if Decimal(str(count)) == Decimal("0"):
                continue
            avg_price = position.get("average_price_dollars") or position.get("avg_price_dollars") or "0.5000"
            await repo.upsert_position(
                market_ticker=market_ticker,
                subaccount=int(position.get("subaccount", subaccount)),
                side=_stringish(position.get("side") or position.get("position_side"), "yes"),
                count_fp=quantize_count(count),
                average_price_dollars=quantize_price(avg_price),
                raw=position,
            )

        for order in orders:
            client_order_id = _stringish(order.get("client_order_id"), _stringish(order.get("order_id"), "unknown"))
            market_ticker = _stringish(order.get("ticker") or order.get("market_ticker"), "unknown")
            yes_price = order.get("yes_price_dollars") or order.get("price_dollars") or "0.5000"
            count = order.get("count_fp") or order.get("remaining_count_fp") or "1.00"
            await repo.upsert_order(
                client_order_id=client_order_id,
                market_ticker=market_ticker,
                status=_stringish(order.get("status"), "unknown"),
                side=_stringish(order.get("side"), "yes"),
                action=_stringish(order.get("action"), "buy"),
                yes_price_dollars=quantize_price(yes_price),
                count_fp=quantize_count(count),
                raw=order,
                kalshi_order_id=order.get("order_id"),
            )

        for fill in fills:
            yes_price = fill.get("yes_price_dollars") or fill.get("price_dollars") or "0.5000"
            count = fill.get("count_fp") or "1.00"
            await repo.upsert_fill(
                market_ticker=_stringish(fill.get("market_ticker") or fill.get("ticker"), "unknown"),
                side=_stringish(fill.get("side"), "yes"),
                action=_stringish(fill.get("action") or fill.get("user_action"), "buy"),
                yes_price_dollars=quantize_price(yes_price),
                count_fp=quantize_count(count),
                raw=fill,
                trade_id=fill.get("trade_id"),
                is_taker=bool(fill.get("is_taker", True)),
            )

        await repo.set_checkpoint(
            "reconcile",
            cursor=None,
            payload={
                "historical_cutoff": historical_cutoff,
                "balance": balance,
                "positions_count": len(positions),
                "orders_count": len(orders),
                "fills_count": len(fills),
                "settlements_count": len(settlements),
            },
        )
        await repo.log_ops_event(
            severity="info",
            summary="Reconciliation completed",
            source="reconcile",
            payload={
                "positions_count": len(positions),
                "orders_count": len(orders),
                "fills_count": len(fills),
                "settlements_count": len(settlements),
            },
        )

        return ReconcileSummary(
            balances_seen=bool(balance),
            positions_count=len(positions),
            orders_count=len(orders),
            fills_count=len(fills),
            settlements_count=len(settlements),
            historical_cutoff_seen=bool(historical_cutoff),
        )

