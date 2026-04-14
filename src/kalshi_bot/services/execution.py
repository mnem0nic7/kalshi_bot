from __future__ import annotations

from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.integrations.kalshi import KalshiClient


class ExecutionService:
    def __init__(self, settings: Settings, kalshi: KalshiClient) -> None:
        self.settings = settings
        self.kalshi = kalshi

    async def execute(
        self,
        *,
        room: Room,
        control: DeploymentControl,
        ticket: TradeTicket,
        client_order_id: str,
    ) -> ExecReceiptPayload:
        if room.shadow_mode:
            return ExecReceiptPayload(
                status="shadow_skipped",
                client_order_id=client_order_id,
                details={"reason": "room is in shadow mode"},
            )
        if control.active_color != self.settings.app_color:
            return ExecReceiptPayload(
                status="inactive_color_skipped",
                client_order_id=client_order_id,
                details={"active_color": control.active_color, "app_color": self.settings.app_color},
            )
        if self.kalshi.write_credentials is None:
            return ExecReceiptPayload(
                status="write_credentials_missing",
                client_order_id=client_order_id,
                details={"reason": "write credentials were not configured"},
            )

        payload = {
            "ticker": ticket.market_ticker,
            "side": ticket.side.value,
            "action": ticket.action.value,
            "client_order_id": client_order_id,
            "count_fp": f"{ticket.count_fp:.2f}",
            # Kalshi requires exactly one price field; always send yes_price_dollars.
            "yes_price_dollars": f"{ticket.yes_price_dollars:.4f}",
            "time_in_force": ticket.time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
        }
        if self.settings.kalshi_subaccount:
            payload["subaccount"] = self.settings.kalshi_subaccount
        response = await self.kalshi.create_order(payload)
        order = response.get("order", {})
        return ExecReceiptPayload(
            status=order.get("status", "submitted"),
            external_order_id=order.get("order_id"),
            client_order_id=client_order_id,
            details=response,
        )
