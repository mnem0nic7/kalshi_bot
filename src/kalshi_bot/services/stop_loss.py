from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MarketPriceHistory, MarketState, PositionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient

logger = logging.getLogger(__name__)


def _midpoint(market_state: MarketState, side: str) -> Decimal | None:
    yes_bid = market_state.yes_bid_dollars
    if yes_bid is None:
        return None
    yes_ask = market_state.yes_ask_dollars
    # When ask side is dry, use bid as the conservative mark rather than skipping entirely.
    mid_yes = (yes_bid + yes_ask) / Decimal("2") if yes_ask is not None else yes_bid
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


def _profit_ratio(position: PositionRecord, mid: Decimal) -> float | None:
    ratio = _loss_ratio(position, mid)
    return -ratio if ratio is not None else None


def _momentum_slope(prices: list[MarketPriceHistory]) -> float | None:
    """Return YES midpoint slope in ¢/min via linear regression, or None if < 5 valid points."""
    points = [
        (row.observed_at.timestamp(), float(row.mid_dollars))
        for row in prices
        if row.mid_dollars is not None
    ]
    if len(points) < 5:
        return None
    xs = np.array([p[0] for p in points])
    ys = np.array([p[1] for p in points])
    xs = xs - xs[0]
    slope_per_second = float(np.polyfit(xs, ys, 1)[0])
    return slope_per_second * 100 * 60  # $/s → ¢/min


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

        # Load positions and market states in one read session
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            positions = await repo.list_positions(
                limit=500,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
            )
            if not positions:
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

            result = await self._evaluate_and_submit(position, ms, mid, now)
            if result is not None:
                triggered.append(result)

        return triggered

    async def _evaluate_and_submit(
        self,
        position: PositionRecord,
        ms: MarketState,
        mid: Decimal,
        now: datetime,
    ) -> dict[str, Any] | None:
        """Evaluate one position in its own committed transaction to make the cooldown checkpoint immediately visible."""
        async with self.session_factory() as session:
            repo = PlatformRepository(session)

            # shared submit cooldown — read with committed isolation
            submit_key = f"stop_loss_submit:{position.market_ticker}"
            submit_cp = await repo.get_checkpoint(submit_key)
            if submit_cp is not None:
                # On order failure the checkpoint stores next_retry_at (absolute); prefer that.
                next_retry = submit_cp.payload.get("next_retry_at")
                if next_retry is not None:
                    if now < datetime.fromisoformat(next_retry):
                        return None
                else:
                    last = submit_cp.payload.get("submitted_at")
                    if last is not None:
                        last_dt = datetime.fromisoformat(last)
                        if now - last_dt < timedelta(seconds=self.settings.stop_loss_submit_cooldown_seconds):
                            return None

            # Trigger 1: loss-ratio threshold
            ratio = _loss_ratio(position, mid)
            if ratio is not None and ratio >= self.settings.stop_loss_threshold_pct:
                sell_px = _sell_price(ms, position.side)
                if sell_px is not None and sell_px > 0:
                    result = await self._submit(repo, position, sell_px, mid, ratio, now, trigger="loss_ratio")
                    await session.commit()
                    return result

            # Trigger 2: adverse momentum (no P&L requirement)
            created_at = position.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            hold_minutes = (now - created_at).total_seconds() / 60
            if hold_minutes < self.settings.stop_loss_momentum_min_hold_minutes:
                return None

            prices = await repo.fetch_recent_prices(position.market_ticker, window=timedelta(minutes=60))
            slope = _momentum_slope(prices)
            if slope is None:
                return None

            # adverse slope: YES falling for YES holder, YES rising for NO holder
            slope_against = slope if position.side == "yes" else -slope
            if slope_against >= self.settings.stop_loss_momentum_slope_threshold_cents_per_min:
                return None

            sell_px = _sell_price(ms, position.side)
            if sell_px is None or sell_px <= 0:
                return None

            profit = _profit_ratio(position, mid)
            trigger = "profit_protection" if (profit is not None and profit >= self.settings.stop_loss_profit_protection_threshold_pct) else "momentum"
            result = await self._submit(repo, position, sell_px, mid, ratio, now, trigger=trigger, slope=slope)
            await session.commit()
            return result

    async def _submit(
        self,
        repo: PlatformRepository,
        position: PositionRecord,
        sell_price: Decimal,
        mid: Decimal,
        loss_ratio: float | None,
        now: datetime,
        *,
        trigger: str = "loss_ratio",
        slope: float | None = None,
    ) -> dict[str, Any]:
        market_ticker = position.market_ticker
        shadow = self.settings.app_shadow_mode
        action = f"stop_loss_{trigger}_shadow" if shadow else f"stop_loss_{trigger}"

        event_payload: dict[str, Any] = {
            "market_ticker": market_ticker,
            "side": position.side,
            "count_fp": str(position.count_fp),
            "average_price_dollars": str(position.average_price_dollars),
            "mid_mark": str(mid),
            "sell_price": str(sell_price),
            "loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "shadow_mode": shadow,
            "action": action,
            "trigger": trigger,
        }
        if slope is not None:
            event_payload["momentum_slope_cents_per_min"] = round(slope, 4)

        submit_failed = False
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
                submit_failed = True

        await repo.log_ops_event(
            severity="warning",
            summary=(
                f"Stop loss {'(shadow) ' if shadow else ''}triggered [{trigger}]: "
                f"{market_ticker} {position.side}"
                + (f" loss={loss_ratio:.0%}" if loss_ratio is not None else "")
                + (f" slope={slope:.3f}¢/min" if slope is not None else "")
            ),
            source="stop_loss",
            payload=event_payload,
        )

        submit_payload: dict[str, Any] = {
            "submitted_at": now.isoformat(),
            "loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "trigger": trigger,
        }
        if submit_failed:
            # Back off 30 min on order failure to avoid spamming an illiquid book.
            submit_payload["next_retry_at"] = (now + timedelta(minutes=30)).isoformat()
        await repo.set_checkpoint(
            f"stop_loss_submit:{market_ticker}",
            cursor=None,
            payload=submit_payload,
        )
        await repo.set_checkpoint(
            f"stop_loss_reentry:{market_ticker}",
            cursor=None,
            payload={
                "stopped_at": now.isoformat(),
                "loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
                "trigger": trigger,
            },
        )

        logger.warning(
            "Stop loss %s [%s]: %s %s loss=%s slope=%s",
            "shadow" if shadow else "executed",
            trigger,
            market_ticker,
            position.side,
            f"{loss_ratio:.0%}" if loss_ratio is not None else "n/a",
            f"{slope:.3f}¢/min" if slope is not None else "n/a",
        )
        return event_payload
