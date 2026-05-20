from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StrategyCode
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, PositionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED,
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
)

logger = logging.getLogger(__name__)


def _crypto_mid(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    yes_bid = snapshot.yes_bid_dollars
    yes_ask = snapshot.yes_ask_dollars
    if yes_bid is None or yes_ask is None:
        return None
    mid_yes = (yes_bid + yes_ask) / Decimal("2")
    return mid_yes if side == "yes" else Decimal("1") - mid_yes


def _crypto_sell_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    price = snapshot.yes_bid_dollars if side == "yes" else snapshot.yes_ask_dollars
    if price is None or price <= Decimal("0") or price >= Decimal("1"):
        return None
    return price


def _profit_ratio(position: PositionRecord, mid: Decimal) -> float | None:
    avg = position.average_price_dollars
    if position.count_fp <= 0 or avg <= 0:
        return None
    return float((position.count_fp * mid - position.count_fp * avg) / (position.count_fp * avg))


class CryptoTakeProfitService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        execution_service: ExecutionService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.execution_service = execution_service

    async def check_once(self) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []
        if not self.settings.crypto_take_profit_enabled:
            return triggered

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            if control.active_color != self.settings.app_color:
                await session.commit()
                return triggered
            positions = await repo.list_positions(
                limit=500,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
            )

        if not positions:
            return triggered

        now = datetime.now(UTC)
        stale_cutoff = timedelta(seconds=self.settings.crypto_take_profit_stale_snapshot_seconds)
        threshold = self.settings.crypto_take_profit_threshold_pct

        for position in positions:
            result = await self._evaluate(position, now, stale_cutoff, threshold)
            if result is not None:
                triggered.append(result)

        return triggered

    async def _evaluate(
        self,
        position: PositionRecord,
        now: datetime,
        stale_cutoff: timedelta,
        threshold: float,
    ) -> dict[str, Any] | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            kill_switch_enabled = bool(control.kill_switch_enabled)
            active_color = control.active_color

            submit_key = f"stop_loss_submit:{self.settings.kalshi_env}:{position.market_ticker}"
            submit_cp = await repo.get_checkpoint(submit_key)
            if submit_cp is not None:
                outcome_status = str((submit_cp.payload or {}).get("outcome_status") or "")
                if outcome_status == STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL:
                    return None
                next_retry = (submit_cp.payload or {}).get("next_retry_at")
                if next_retry is not None and now < datetime.fromisoformat(next_retry):
                    return None

            snapshot = await repo.get_latest_crypto_market_snapshot(
                position.market_ticker,
                kalshi_env=self.settings.kalshi_env,
            )

        if snapshot is None:
            return None

        observed_at = snapshot.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        if (now - observed_at) > stale_cutoff:
            logger.debug(
                "crypto_take_profit skipping %s: snapshot stale (observed_at=%s)",
                position.market_ticker,
                observed_at,
            )
            return None

        if snapshot.status and snapshot.status not in {"open", "active"}:
            return None

        mid = _crypto_mid(snapshot, position.side)
        if mid is None:
            return None

        profit = _profit_ratio(position, mid)
        if profit is None or profit < threshold:
            return None

        sell_px = _crypto_sell_price(snapshot, position.side)
        if sell_px is None or sell_px <= 0:
            return None

        return await self._submit(
            position=position,
            sell_price=sell_px,
            mid=mid,
            profit=profit,
            now=now,
            kill_switch_enabled=kill_switch_enabled,
            active_color=active_color,
        )

    async def _submit(
        self,
        *,
        position: PositionRecord,
        sell_price: Decimal,
        mid: Decimal,
        profit: float,
        now: datetime,
        kill_switch_enabled: bool,
        active_color: str,
    ) -> dict[str, Any]:
        market_ticker = position.market_ticker
        client_order_id = str(uuid4())

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            strategy_code = await repo.get_latest_fill_strategy_for_market_side(
                market_ticker=market_ticker,
                side=position.side,
                kalshi_env=self.settings.kalshi_env,
                before=now,
            )
            if strategy_code is None:
                strategy_code = StrategyCode.DIRECTIONAL

        receipt = None
        submit_failed = False
        submit_error: str | None = None
        try:
            receipt = await self.execution_service.close_position(
                market_ticker=market_ticker,
                side=position.side,
                count_fp=position.count_fp,
                yes_price_dollars=sell_price,
                client_order_id=client_order_id,
                kill_switch_enabled=kill_switch_enabled,
                active_color=active_color,
                subaccount=self.settings.kalshi_subaccount or None,
                allow_risk_reducing_exit=True,
            )
        except Exception as exc:
            logger.warning("crypto_take_profit submit failed for %s: %s", market_ticker, exc)
            submit_failed = True
            submit_error = str(exc)

        receipt_status = receipt.status if receipt is not None else "submit_exception"
        receipt_details = receipt.details if receipt is not None else {"error": submit_error}
        shadow = receipt_status == "shadow_skipped"
        order_data = dict((receipt_details or {}).get("order") or {})
        order_status = str(order_data.get("status") or receipt_status)
        normalized = order_status.strip().lower()
        terminal_filled = normalized in {"filled", "executed"}
        terminal_unfilled = normalized in {"cancelled", "canceled", "expired"}
        if receipt is not None:
            submit_failed = (
                not shadow
                and receipt.external_order_id is None
                and not terminal_filled
                and not terminal_unfilled
            )
            if submit_failed:
                submit_error = str(receipt_details)

        action = f"crypto_take_profit_shadow" if shadow else "crypto_take_profit"

        event_payload: dict[str, Any] = {
            "market_ticker": market_ticker,
            "side": position.side,
            "count_fp": str(position.count_fp),
            "average_price_dollars": str(position.average_price_dollars),
            "mid_mark": str(mid),
            "sell_price": str(sell_price),
            "profit_ratio": round(profit, 4),
            "shadow_mode": shadow,
            "action": action,
            "trigger": "take_profit",
            "exec_status": receipt_status,
            "strategy_code": strategy_code,
        }

        submit_payload: dict[str, Any] = {
            "submitted_at": now.isoformat(),
            "trigger": "take_profit",
            "profit_ratio": round(profit, 4),
        }

        if shadow:
            submit_payload["client_order_id"] = client_order_id
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_FILLED_EXIT
        elif not submit_failed:
            submit_payload.update(
                {
                    "client_order_id": client_order_id,
                    "order_status": order_status,
                    "kalshi_order_id": receipt.external_order_id if receipt is not None else None,
                    "outcome_status": (
                        STOP_LOSS_OUTCOME_FILLED_EXIT
                        if terminal_filled
                        else (
                            STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED
                            if terminal_unfilled
                            else STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL
                        )
                    ),
                }
            )
            if terminal_unfilled:
                submit_payload["next_retry_at"] = (now + timedelta(minutes=30)).isoformat()
            event_payload["order_response"] = receipt_details
        else:
            event_payload["submit_error"] = submit_error
            submit_payload["submit_error"] = submit_error
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_SUBMIT_FAILED
            submit_payload["next_retry_at"] = (now + timedelta(minutes=30)).isoformat()

        async with self.session_factory() as session:
            repo = PlatformRepository(session)

            if not shadow and receipt_status != "inactive_color_skipped":
                await repo.save_order(
                    ticket_id=None,
                    client_order_id=client_order_id,
                    market_ticker=market_ticker,
                    status=order_status,
                    side=position.side,
                    action="sell",
                    yes_price_dollars=sell_price,
                    count_fp=position.count_fp,
                    raw=receipt_details or {},
                    kalshi_order_id=receipt.external_order_id if receipt is not None else None,
                    kalshi_env=self.settings.kalshi_env,
                    strategy_code=strategy_code,
                )

            await repo.log_ops_event(
                severity="info",
                summary=(
                    f"Crypto take-profit {'(shadow) ' if shadow else ''}triggered: "
                    f"{market_ticker} {position.side} profit={profit:.0%} "
                    f"mark={mid} sell={sell_price}"
                ),
                source="crypto_take_profit",
                payload=event_payload,
            )

            await repo.set_checkpoint(
                f"stop_loss_submit:{self.settings.kalshi_env}:{market_ticker}",
                cursor=None,
                payload=submit_payload,
            )
            await session.commit()

        logger.info(
            "Crypto take-profit %s: %s %s profit=%s mark=%s sell=%s",
            "shadow" if shadow else ("submit_failed" if submit_failed else "submitted"),
            market_ticker,
            position.side,
            f"{profit:.0%}",
            str(mid),
            str(sell_price),
        )
        return event_payload
