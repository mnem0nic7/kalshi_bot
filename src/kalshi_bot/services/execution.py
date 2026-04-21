from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.integrations.kalshi import KalshiClient

logger = logging.getLogger(__name__)

_FILL_TERMINAL = {"filled", "executed", "cancelled", "canceled", "expired"}
_FILL_DONE = {"filled", "executed"}
_LIMIT_TIF = "gtc"
_POLL_INTERVAL = 3
_FILL_TIMEOUT = 30
_MAX_REQUOTES = 3


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
        fair_yes_dollars: Decimal | None = None,
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

        if ticket.time_in_force == _LIMIT_TIF:
            return await self._execute_limit(
                ticket=ticket,
                client_order_id=client_order_id,
                fair_yes_dollars=fair_yes_dollars,
            )

        return await self._place_order(ticket, client_order_id)

    async def _place_order(self, ticket: TradeTicket, client_order_id: str) -> ExecReceiptPayload:
        payload: dict[str, Any] = {
            "ticker": ticket.market_ticker,
            "side": ticket.side.value,
            "action": ticket.action.value,
            "client_order_id": client_order_id,
            "count_fp": f"{ticket.count_fp:.2f}",
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

    async def _execute_limit(
        self,
        *,
        ticket: TradeTicket,
        client_order_id: str,
        fair_yes_dollars: Decimal | None,
    ) -> ExecReceiptPayload:
        min_edge = Decimal(str(self.settings.risk_min_edge_bps)) / Decimal("10000")
        current_ticket = ticket

        for attempt in range(1, _MAX_REQUOTES + 1):
            attempt_coid = f"{client_order_id}_q{attempt}"
            receipt = await self._place_order(current_ticket, attempt_coid)
            order_id = receipt.external_order_id

            if order_id is None:
                logger.warning(
                    "limit order for %s returned no order_id on attempt %d",
                    ticket.market_ticker, attempt,
                )
                return ExecReceiptPayload(
                    status="order_id_missing",
                    client_order_id=client_order_id,
                    details=receipt.details,
                )

            filled = await self._wait_for_fill(order_id)

            if filled:
                logger.info(
                    "limit order filled: %s attempt=%d price=%s",
                    ticket.market_ticker, attempt, current_ticket.yes_price_dollars,
                )
                return ExecReceiptPayload(
                    status="filled",
                    external_order_id=order_id,
                    client_order_id=client_order_id,
                    details=receipt.details,
                )

            # Timed out — cancel the resting order.
            try:
                await self.kalshi.cancel_order(order_id)
            except Exception:
                logger.warning("cancel failed for %s order %s", ticket.market_ticker, order_id, exc_info=True)

            if attempt == _MAX_REQUOTES:
                break

            # Re-check edge at new touch before requoting.
            if fair_yes_dollars is None:
                break

            new_price = await self._fresh_touch(ticket.market_ticker, ticket.side.value)
            if new_price is None:
                logger.info("requote aborted for %s: no fresh quote", ticket.market_ticker)
                break

            if ticket.side.value == "yes":
                new_edge = fair_yes_dollars - new_price
            else:
                new_edge = (Decimal("1") - fair_yes_dollars) - (Decimal("1") - new_price)

            if new_edge < min_edge:
                logger.info(
                    "requote aborted for %s: edge %.0fbps below min",
                    ticket.market_ticker, float(new_edge) * 10000,
                )
                return ExecReceiptPayload(
                    status="requote_edge_lost",
                    client_order_id=client_order_id,
                    details={"attempts": attempt, "new_edge_bps": round(float(new_edge) * 10000)},
                )

            current_ticket = current_ticket.model_copy(update={"yes_price_dollars": new_price})
            logger.info(
                "requoting %s attempt=%d new_price=%s edge=%.0fbps",
                ticket.market_ticker, attempt + 1, new_price, float(new_edge) * 10000,
            )

        return ExecReceiptPayload(
            status="unfilled_cancelled",
            client_order_id=client_order_id,
            details={"attempts": min(attempt, _MAX_REQUOTES)},
        )

    async def _wait_for_fill(self, order_id: str) -> bool:
        elapsed = 0
        while elapsed < _FILL_TIMEOUT:
            await asyncio.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL
            try:
                resp = await self.kalshi.get_order(order_id)
                status = resp.get("order", {}).get("status", "")
                if status in _FILL_DONE:
                    return True
                if status in _FILL_TERMINAL:
                    return False
            except Exception:
                logger.warning("poll failed for order %s", order_id, exc_info=True)
        return False

    async def _fresh_touch(self, market_ticker: str, side: str) -> Decimal | None:
        try:
            resp = await self.kalshi.get_market(market_ticker)
            market = resp.get("market", resp)
            if side == "yes":
                raw = market.get("yes_ask_dollars")
                return Decimal(str(raw)) if raw is not None else None
            else:
                raw = market.get("no_ask_dollars")
                if raw is None:
                    return None
                # Convert no_ask to yes_price: yes_price = 1 - no_ask
                return (Decimal("1") - Decimal(str(raw))).quantize(Decimal("0.0001"))
        except Exception:
            logger.warning("fresh touch fetch failed for %s", market_ticker, exc_info=True)
            return None
