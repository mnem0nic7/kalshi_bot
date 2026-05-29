from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars

logger = logging.getLogger(__name__)

_FILL_TERMINAL = {"filled", "executed", "cancelled", "canceled", "expired"}
_FILL_DONE = {"filled", "executed"}
KALSHI_GTC_TIME_IN_FORCE = "good_till_canceled"
_LIMIT_TIFS = {"gtc", KALSHI_GTC_TIME_IN_FORCE}
_POLL_INTERVAL = 3
_FILL_TIMEOUT = 30
_MAX_REQUOTES = 3


def _kalshi_time_in_force(time_in_force: str) -> str:
    if str(time_in_force).strip().lower() == "gtc":
        return KALSHI_GTC_TIME_IN_FORCE
    return time_in_force


def _fee_adjusted_edge_dollars(
    *,
    settings: Settings,
    edge_dollars: Decimal,
    contract_price_dollars: Decimal,
    count_fp: Decimal,
) -> Decimal:
    if not settings.risk_fee_aware_edge_enabled:
        return edge_dollars
    fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=contract_price_dollars,
        count=count_fp,
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    fee_per_contract = fee / count_fp if count_fp > Decimal("0") else Decimal("0")
    return edge_dollars - fee_per_contract


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
        min_edge_bps: int | None = None,
    ) -> ExecReceiptPayload:
        if self.settings.app_shadow_mode:
            return ExecReceiptPayload(
                status="shadow_skipped",
                client_order_id=client_order_id,
                details={"reason": "app shadow mode"},
            )
        if room.shadow_mode:
            return ExecReceiptPayload(
                status="shadow_skipped",
                client_order_id=client_order_id,
                details={"reason": "room is in shadow mode"},
            )
        if control.kill_switch_enabled:
            return ExecReceiptPayload(
                status="kill_switch_blocked",
                client_order_id=client_order_id,
                details={"reason": "kill switch enabled"},
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

        if str(ticket.time_in_force).strip().lower() in _LIMIT_TIFS:
            return await self._execute_limit(
                ticket=ticket,
                client_order_id=client_order_id,
                fair_yes_dollars=fair_yes_dollars,
                min_edge_bps=min_edge_bps,
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
            "time_in_force": _kalshi_time_in_force(ticket.time_in_force),
            "self_trade_prevention_type": "taker_at_cross",
        }
        if self.settings.kalshi_subaccount:
            payload["subaccount"] = self.settings.kalshi_subaccount
        try:
            response = await self.kalshi.create_order(payload)
        except httpx.HTTPStatusError as exc:
            try:
                body = exc.response.json()
            except Exception:
                body = exc.response.text
            logger.error(
                "order rejected for %s status=%d body=%s payload=%s",
                ticket.market_ticker, exc.response.status_code, body, payload,
            )
            return ExecReceiptPayload(
                status=f"rejected_{exc.response.status_code}",
                client_order_id=client_order_id,
                details={"http_status": exc.response.status_code, "body": body, "payload": payload},
            )
        order = response.get("order", {})
        return ExecReceiptPayload(
            status=order.get("status", "submitted"),
            external_order_id=order.get("order_id"),
            client_order_id=client_order_id,
            details={**response, "request_payload": payload},
        )

    async def _execute_limit(
        self,
        *,
        ticket: TradeTicket,
        client_order_id: str,
        fair_yes_dollars: Decimal | None,
        min_edge_bps: int | None = None,
    ) -> ExecReceiptPayload:
        threshold_bps = min_edge_bps if min_edge_bps is not None else self.settings.risk_min_edge_bps
        min_edge = Decimal(str(threshold_bps)) / Decimal("10000")
        current_ticket = ticket
        already_filled_fp = Decimal("0")
        order_ids: list[str] = []
        last_receipt_details: dict[str, Any] = {}

        for attempt in range(1, _MAX_REQUOTES + 1):
            attempt_coid = f"{client_order_id}_q{attempt}"
            receipt = await self._place_order(current_ticket, attempt_coid)
            order_id = receipt.external_order_id
            last_receipt_details = receipt.details if isinstance(receipt.details, dict) else {}

            if order_id is None:
                if receipt.status.startswith("rejected_"):
                    return ExecReceiptPayload(
                        status=receipt.status,
                        client_order_id=client_order_id,
                        details=receipt.details,
                    )
                logger.warning(
                    "limit order for %s returned no order_id on attempt %d",
                    ticket.market_ticker, attempt,
                )
                return ExecReceiptPayload(
                    status="order_id_missing",
                    client_order_id=client_order_id,
                    details=receipt.details,
                )

            order_ids.append(order_id)
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
                    details={**last_receipt_details, "order_ids": order_ids},
                )

            # Timed out — cancel the resting order.
            try:
                await self.kalshi.cancel_order(order_id)
            except Exception:
                logger.warning("cancel failed for %s order %s", ticket.market_ticker, order_id, exc_info=True)

            # Guard against maker fills that landed during the cancel window.
            # The cancel response drops remaining_count_fp to 0 regardless of partial fills,
            # so we query the fills endpoint to learn the actual filled quantity.
            already_filled_fp += await self._get_filled_fp(order_id)
            remaining_fp = ticket.count_fp - already_filled_fp
            if remaining_fp <= Decimal("0"):
                logger.info(
                    "limit order fully covered by race fill for %s attempt=%d filled=%.2f",
                    ticket.market_ticker, attempt, float(already_filled_fp),
                )
                return ExecReceiptPayload(
                    status="filled",
                    external_order_id=order_id,
                    client_order_id=client_order_id,
                    details={
                        **last_receipt_details,
                        "race_filled": True,
                        "attempts": attempt,
                        "filled_count_fp": f"{already_filled_fp:.2f}",
                        "order_ids": order_ids,
                    },
                )

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
                contract_price = new_price
            else:
                new_edge = (Decimal("1") - fair_yes_dollars) - (Decimal("1") - new_price)
                contract_price = Decimal("1") - new_price

            fee_adjusted_edge = _fee_adjusted_edge_dollars(
                settings=self.settings,
                edge_dollars=new_edge,
                contract_price_dollars=contract_price,
                count_fp=remaining_fp,
            )

            if fee_adjusted_edge < min_edge:
                logger.info(
                    "requote aborted for %s: fee-adjusted edge %.0fbps below min",
                    ticket.market_ticker, float(fee_adjusted_edge) * 10000,
                )
                return ExecReceiptPayload(
                    status="requote_edge_lost",
                    external_order_id=order_ids[-1] if order_ids else None,
                    client_order_id=client_order_id,
                    details={
                        "attempts": attempt,
                        "new_edge_bps": round(float(new_edge) * 10000),
                        "fee_adjusted_edge_bps": round(float(fee_adjusted_edge) * 10000),
                        "filled_count_fp": f"{already_filled_fp:.2f}",
                        "partial_fill": already_filled_fp > Decimal("0"),
                        "order_ids": order_ids,
                    },
                )

            current_ticket = current_ticket.model_copy(update={
                "yes_price_dollars": new_price,
                "count_fp": remaining_fp,
            })
            logger.info(
                "requoting %s attempt=%d new_price=%s count=%.2f edge=%.0fbps fee_adjusted=%.0fbps",
                ticket.market_ticker,
                attempt + 1,
                new_price,
                float(remaining_fp),
                float(new_edge) * 10000,
                float(fee_adjusted_edge) * 10000,
            )

        return ExecReceiptPayload(
            status="partially_filled_cancelled" if already_filled_fp > Decimal("0") else "unfilled_cancelled",
            external_order_id=order_ids[-1] if order_ids else None,
            client_order_id=client_order_id,
            details={
                **last_receipt_details,
                "attempts": min(attempt, _MAX_REQUOTES),
                "filled_count_fp": f"{already_filled_fp:.2f}",
                "partial_fill": already_filled_fp > Decimal("0"),
                "order_ids": order_ids,
            },
        )

    async def execute_fixed_limit_until_close(
        self,
        *,
        ticket: TradeTicket,
        client_order_id: str,
        close_time: datetime | None,
        cancel_grace_seconds: int = 2,
        poll_interval_seconds: float = _POLL_INTERVAL,
    ) -> ExecReceiptPayload:
        """Submit one fixed GTC limit and leave it resting until close/cancel."""
        limit_ticket = ticket.model_copy(update={"time_in_force": KALSHI_GTC_TIME_IN_FORCE})
        receipt = await self._place_order(limit_ticket, client_order_id)
        order_id = receipt.external_order_id
        if order_id is None:
            if receipt.status.startswith("rejected_"):
                return receipt
            return ExecReceiptPayload(
                status="order_id_missing",
                client_order_id=client_order_id,
                details=receipt.details,
            )

        deadline = close_time
        if deadline is None:
            deadline = datetime.now(UTC) + timedelta(seconds=_FILL_TIMEOUT)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        deadline = deadline.astimezone(UTC) + timedelta(seconds=max(0, cancel_grace_seconds))
        last_order: dict[str, Any] | None = None
        while datetime.now(UTC) < deadline:
            try:
                resp = await self.kalshi.get_order(order_id)
                order = resp.get("order", {})
                last_order = order if isinstance(order, dict) else None
                status = str(order.get("status", "") if isinstance(order, dict) else "")
                if status in _FILL_DONE:
                    return ExecReceiptPayload(
                        status="filled",
                        external_order_id=order_id,
                        client_order_id=client_order_id,
                        details={
                            **(receipt.details if isinstance(receipt.details, dict) else {}),
                            "last_poll": resp,
                            "fixed_limit_until_close": True,
                        },
                    )
                if status in _FILL_TERMINAL:
                    return ExecReceiptPayload(
                        status=status or "closed",
                        external_order_id=order_id,
                        client_order_id=client_order_id,
                        details={
                            **(receipt.details if isinstance(receipt.details, dict) else {}),
                            "last_poll": resp,
                            "fixed_limit_until_close": True,
                        },
                    )
            except Exception:
                logger.warning("poll failed for order %s", order_id, exc_info=True)
            remaining = max(0.0, (deadline - datetime.now(UTC)).total_seconds())
            if remaining <= 0:
                break
            await asyncio.sleep(min(max(0.01, poll_interval_seconds), remaining))

        try:
            await self.kalshi.cancel_order(order_id)
        except Exception:
            logger.warning("cancel failed for %s order %s", ticket.market_ticker, order_id, exc_info=True)
        filled_count_fp = await self._get_filled_fp(order_id)
        status = "unfilled_cancelled"
        if filled_count_fp >= ticket.count_fp:
            status = "filled"
        elif filled_count_fp > Decimal("0"):
            status = "partially_filled_cancelled"
        return ExecReceiptPayload(
            status=status,
            external_order_id=order_id,
            client_order_id=client_order_id,
            details={
                **(receipt.details if isinstance(receipt.details, dict) else {}),
                "fixed_limit_until_close": True,
                "cancel_after_close": True,
                "last_order_status": (last_order or {}).get("status"),
                "filled_count_fp": f"{filled_count_fp:.2f}",
                "partial_fill": Decimal("0") < filled_count_fp < ticket.count_fp,
            },
        )

    async def close_position(
        self,
        *,
        market_ticker: str,
        side: str,
        count_fp: Decimal,
        yes_price_dollars: Decimal,
        client_order_id: str,
        kill_switch_enabled: bool,
        active_color: str,
        subaccount: int | None = None,
        allow_risk_reducing_exit: bool = False,
    ) -> ExecReceiptPayload:
        """Submit an IOC sell order for an existing position.

        Returns a sentinel status without hitting the API in three cases:
        - ``shadow_skipped``: app is in shadow mode
        - ``kill_switch_blocked``: kill switch is enabled and the caller has not
          explicitly marked this as a risk-reducing exit
        - ``inactive_color_skipped``: this deployment color is not active
        """
        if self.settings.app_shadow_mode:
            return ExecReceiptPayload(
                status="shadow_skipped",
                client_order_id=client_order_id,
                details={"reason": "shadow mode"},
            )
        if kill_switch_enabled and not allow_risk_reducing_exit:
            return ExecReceiptPayload(
                status="kill_switch_blocked",
                client_order_id=client_order_id,
                details={"reason": "kill switch enabled"},
            )
        if active_color != self.settings.app_color:
            return ExecReceiptPayload(
                status="inactive_color_skipped",
                client_order_id=client_order_id,
                details={"active_color": active_color, "app_color": self.settings.app_color},
            )
        if self.kalshi.write_credentials is None:
            return ExecReceiptPayload(
                status="write_credentials_missing",
                client_order_id=client_order_id,
                details={"reason": "write credentials were not configured"},
            )
        payload: dict[str, Any] = {
            "ticker": market_ticker,
            "side": side,
            "action": "sell",
            "client_order_id": client_order_id,
            "count_fp": f"{count_fp:.2f}",
            "yes_price_dollars": f"{yes_price_dollars:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
        if subaccount:
            payload["subaccount"] = subaccount
        try:
            response = await self.kalshi.create_order(payload)
        except httpx.HTTPStatusError as exc:
            try:
                body = exc.response.json()
            except Exception:
                body = exc.response.text
            logger.error(
                "close_position rejected for %s status=%d body=%s",
                market_ticker,
                exc.response.status_code,
                body,
            )
            return ExecReceiptPayload(
                status=f"rejected_{exc.response.status_code}",
                client_order_id=client_order_id,
                details={"http_status": exc.response.status_code, "body": body, "payload": payload},
            )
        order = response.get("order", {})
        return ExecReceiptPayload(
            status=order.get("status", "submitted"),
            external_order_id=order.get("order_id"),
            client_order_id=client_order_id,
            details={**response, "request_payload": payload},
        )

    async def _get_filled_fp(self, order_id: str) -> Decimal:
        """Return total filled quantity for a Kalshi order ID via the fills endpoint."""
        try:
            resp = await self.kalshi.get_fills(order_id=order_id)
            return sum(
                (Decimal(str(f.get("count_fp", "0"))) for f in resp.get("fills", [])),
                Decimal("0"),
            )
        except Exception:
            logger.warning("fill query failed for order %s", order_id, exc_info=True)
            return Decimal("0")

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
