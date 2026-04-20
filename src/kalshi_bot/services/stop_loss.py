from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MarketState, PositionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient

logger = logging.getLogger(__name__)


def _midpoint(market_state: MarketState, side: str) -> Decimal | None:
    yes_bid = market_state.yes_bid_dollars
    yes_ask = market_state.yes_ask_dollars
    if yes_bid is None or yes_ask is None:
        return None
    mid_yes = (yes_bid + yes_ask) / Decimal("2")
    if side == "yes":
        return mid_yes
    return Decimal("1") - mid_yes


def _sell_price(market_state: MarketState, side: str) -> Decimal | None:
    if side == "yes":
        return market_state.yes_bid_dollars
    yes_ask = market_state.yes_ask_dollars
    if yes_ask is None:
        return None
    return (Decimal("1") - yes_ask).quantize(Decimal("0.0001"))


def _loss_ratio(position: PositionRecord, mid: Decimal) -> float | None:
    count = position.count_fp
    avg = position.average_price_dollars
    if count <= 0 or avg <= 0:
        return None
    cost_basis = count * avg
    mark_value = count * mid
    return float((cost_basis - mark_value) / cost_basis)


class StopLossService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi

    async def check_once(self) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            positions = await repo.list_positions(
                limit=500,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
            )
            if not positions:
                await session.commit()
                return triggered

            tickers = [p.market_ticker for p in positions]
            market_states = {ms.market_ticker: ms for ms in await repo.list_market_states(tickers)}

            now = datetime.now(UTC)
            for position in positions:
                ms = market_states.get(position.market_ticker)
                if ms is None:
                    continue

                mid = _midpoint(ms, position.side)
                if mid is None:
                    continue

                ratio = _loss_ratio(position, mid)
                if ratio is None or ratio < self.settings.stop_loss_threshold_pct:
                    continue

                submit_key = f"stop_loss_submit:{position.market_ticker}"
                submit_cp = await repo.get_checkpoint(submit_key)
                if submit_cp is not None:
                    last = submit_cp.payload.get("submitted_at")
                    if last is not None:
                        last_dt = datetime.fromisoformat(last)
                        if now - last_dt < timedelta(seconds=self.settings.stop_loss_submit_cooldown_seconds):
                            continue

                sell_px = _sell_price(ms, position.side)
                if sell_px is None or sell_px <= 0:
                    continue

                result = await self._submit(repo, position, sell_px, mid, ratio, now)
                triggered.append(result)

            await session.commit()
        return triggered

    async def _submit(
        self,
        repo: PlatformRepository,
        position: PositionRecord,
        sell_price: Decimal,
        mid: Decimal,
        loss_ratio: float,
        now: datetime,
    ) -> dict[str, Any]:
        market_ticker = position.market_ticker
        shadow = self.settings.app_shadow_mode
        action = "stop_loss_shadow" if shadow else "stop_loss_sell"

        event_payload: dict[str, Any] = {
            "market_ticker": market_ticker,
            "side": position.side,
            "count_fp": str(position.count_fp),
            "average_price_dollars": str(position.average_price_dollars),
            "mid_mark": str(mid),
            "sell_price": str(sell_price),
            "loss_ratio": round(loss_ratio, 4),
            "shadow_mode": shadow,
            "action": action,
        }

        if not shadow:
            try:
                order_resp = await self.kalshi.create_order({
                    "ticker": market_ticker,
                    "side": position.side,
                    "action": "sell",
                    "yes_price_dollars": f"{sell_price:.4f}",
                    "count": str(int(position.count_fp)),
                    "time_in_force": "ioc",
                    "client_order_id": str(uuid4()),
                })
                event_payload["order_response"] = order_resp
            except Exception as exc:
                logger.warning("stop_loss order submit failed for %s: %s", market_ticker, exc)
                event_payload["submit_error"] = str(exc)

        await repo.log_ops_event(
            severity="warning",
            summary=(
                f"Stop loss {'(shadow) ' if shadow else ''}triggered: "
                f"{market_ticker} {position.side} loss={loss_ratio:.0%}"
            ),
            source="stop_loss",
            payload=event_payload,
        )

        await repo.set_checkpoint(
            f"stop_loss_submit:{market_ticker}",
            cursor=None,
            payload={"submitted_at": now.isoformat(), "loss_ratio": round(loss_ratio, 4)},
        )
        await repo.set_checkpoint(
            f"stop_loss_reentry:{market_ticker}",
            cursor=None,
            payload={
                "stopped_at": now.isoformat(),
                "loss_ratio": round(loss_ratio, 4),
                "reentry_blocked_until": (
                    now + timedelta(seconds=self.settings.stop_loss_reentry_cooldown_seconds)
                ).isoformat(),
            },
        )

        logger.warning(
            "Stop loss %s: %s %s loss=%.0f%%",
            "shadow" if shadow else "executed",
            market_ticker,
            position.side,
            loss_ratio * 100,
        )
        return event_payload
