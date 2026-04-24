from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord, MarketPriceHistory, MarketState, PositionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED,
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
)

logger = logging.getLogger(__name__)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _midpoint(market_state: MarketState, side: str) -> Decimal | None:
    yes_bid = market_state.yes_bid_dollars
    yes_ask = market_state.yes_ask_dollars
    # Require both sides: a missing ask means the book is broken (market maker withdrew,
    # likely near settlement). A stale $0.01 bid next to a missing ask would produce a
    # fake 99% loss signal on a position that's actually winning.
    if yes_bid is None or yes_ask is None:
        return None
    mid_yes = (yes_bid + yes_ask) / Decimal("2")
    if side == "yes":
        return mid_yes
    return Decimal("1") - mid_yes


def _sell_price(market_state: MarketState, side: str) -> Decimal | None:
    if side == "yes":
        return market_state.yes_bid_dollars
    # For NO sell: yes_price_dollars = yes_ask.
    # SELL NO at P means min acceptable NO price = 1-P; fills when NO bid ≥ 1-P.
    # yes_ask = 1 - no_bid, so P = yes_ask → 1-P = no_bid → fills at the NO bid. ✓
    # The old formula (1 - yes_ask) inverted this: min NO = 0.82 when market NO = 0.18 → never fills.
    return market_state.yes_ask_dollars


def _side_price(mid_yes: Decimal, side: str) -> Decimal:
    return mid_yes if side == "yes" else Decimal("1") - mid_yes


def _price_history_since(
    prices: list[MarketPriceHistory],
    opened_at: datetime | None,
) -> list[MarketPriceHistory]:
    opened_at = _as_utc(opened_at)
    if opened_at is None:
        return prices
    return [
        row
        for row in prices
        if (_as_utc(row.observed_at) or datetime.min.replace(tzinfo=UTC)) >= opened_at
    ]


def _peak_price_from_history(
    prices: list[MarketPriceHistory],
    side: str,
    *,
    opened_at: datetime | None = None,
) -> Decimal | None:
    """Return the highest side-appropriate mid price while this position was held."""
    candidates = [
        _side_price(row.mid_dollars, side)
        for row in _price_history_since(prices, opened_at)
        if row.mid_dollars is not None
    ]
    return max(candidates) if candidates else None


def _position_opened_at_from_fills(position: PositionRecord, fills: list[FillRecord]) -> datetime | None:
    """Infer when the current open lot began by replaying same-side fills."""
    running = Decimal("0")
    opened_at: datetime | None = None
    for fill in sorted(fills, key=lambda item: _as_utc(item.created_at) or datetime.min.replace(tzinfo=UTC)):
        if fill.market_ticker != position.market_ticker or str(fill.side) != str(position.side):
            continue
        delta = Decimal(str(fill.count_fp or "0"))
        if str(fill.action).lower() == "sell":
            delta = -delta
        if running <= 0 and delta > 0:
            opened_at = _as_utc(fill.created_at)
        running += delta
        if running <= 0:
            opened_at = None
    return opened_at


def _position_opened_at(position: PositionRecord, fills: list[FillRecord]) -> datetime | None:
    return _position_opened_at_from_fills(position, fills) or _as_utc(position.created_at)


def _trailing_loss_ratio(peak: Decimal, current: Decimal) -> float:
    """Drop from peak as a fraction of peak: (peak - current) / peak."""
    if peak <= 0:
        return 0.0
    return float((peak - current) / peak)


def _profit_ratio(position: PositionRecord, mid: Decimal) -> float | None:
    avg = position.average_price_dollars
    if position.count_fp <= 0 or avg <= 0:
        return None
    cost_basis = position.count_fp * avg
    mark_value = position.count_fp * mid
    return float((mark_value - cost_basis) / cost_basis)


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
        execution_service: ExecutionService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.execution_service = execution_service

    async def check_once(self) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []

        # Load positions and market states in one read session
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
            tickers = [p.market_ticker for p in positions]
            market_states = {
                ms.market_ticker: ms
                for ms in await repo.list_market_states(tickers, kalshi_env=self.settings.kalshi_env)
            }

        now = datetime.now(UTC)
        # Fetch the retained price and fill history for each held ticker.
        today_window = timedelta(hours=24)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            price_histories = {}
            for ticker in tickers:
                price_histories[ticker] = await repo.fetch_recent_prices(
                    ticker,
                    kalshi_env=self.settings.kalshi_env,
                    window=today_window,
                )
            fills_by_ticker: dict[str, list[FillRecord]] = {ticker: [] for ticker in tickers}
            for fill in await repo.list_fills_for_markets(tickers, kalshi_env=self.settings.kalshi_env):
                fills_by_ticker.setdefault(fill.market_ticker, []).append(fill)

        stale_cutoff = timedelta(seconds=self.settings.risk_stale_market_seconds)
        for position in positions:
            ms = market_states.get(position.market_ticker)
            if ms is None:
                continue

            observed_at = _as_utc(ms.observed_at)
            if observed_at is None or (now - observed_at) > stale_cutoff:
                logger.warning(
                    "stop_loss skipping %s: market state stale (observed_at=%s)",
                    position.market_ticker,
                    observed_at,
                )
                continue

            mid = _midpoint(ms, position.side)
            if mid is None:
                continue

            prices = price_histories.get(position.market_ticker, [])
            opened_at = _position_opened_at(position, fills_by_ticker.get(position.market_ticker, []))
            result = await self._evaluate_and_submit(position, ms, mid, prices, now, opened_at)
            if result is not None:
                triggered.append(result)

        return triggered

    async def _evaluate_and_submit(
        self,
        position: PositionRecord,
        ms: MarketState,
        mid: Decimal,
        prices: list[MarketPriceHistory],
        now: datetime,
        opened_at: datetime | None,
    ) -> dict[str, Any] | None:
        """Evaluate one position in its own committed transaction to make the cooldown checkpoint immediately visible."""
        async with self.session_factory() as session:
            repo = PlatformRepository(session)

            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            kill_switch_enabled: bool = bool(control.kill_switch_enabled)
            active_color: str = control.active_color

            # shared submit cooldown — read with committed isolation
            submit_key = f"stop_loss_submit:{self.settings.kalshi_env}:{position.market_ticker}"
            submit_cp = await repo.get_checkpoint(submit_key)
            if submit_cp is not None:
                outcome_status = str((submit_cp.payload or {}).get("outcome_status") or "")
                if outcome_status == STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL:
                    return None
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

            held_prices = _price_history_since(prices, opened_at)

            # Trigger 1: trailing stop — 10% drop from the peak seen while held.
            peak = _peak_price_from_history(held_prices, position.side)
            trailing_ratio: float | None = None
            if peak is not None:
                trailing_ratio = _trailing_loss_ratio(peak, mid)
                if trailing_ratio >= self.settings.stop_loss_threshold_pct:
                    sell_px = _sell_price(ms, position.side)
                    if sell_px is not None and sell_px > 0:
                        result = await self._submit(
                            repo, position, sell_px, mid, trailing_ratio, now,
                            kill_switch_enabled=kill_switch_enabled,
                            active_color=active_color,
                            trigger="trailing_stop", peak=peak, opened_at=opened_at,
                        )
                        await session.commit()
                        return result

            # Trigger 2: adverse momentum (no P&L requirement — catches slow bleeds
            # that haven't yet hit the trailing stop threshold).
            hold_start = opened_at or _as_utc(position.created_at) or now
            hold_minutes = (now - hold_start).total_seconds() / 60
            if hold_minutes < self.settings.stop_loss_momentum_min_hold_minutes:
                return None

            slope = _momentum_slope(held_prices)
            if slope is None:
                return None

            slope_against = slope if position.side == "yes" else -slope
            if slope_against >= self.settings.stop_loss_momentum_slope_threshold_cents_per_min:
                return None

            sell_px = _sell_price(ms, position.side)
            if sell_px is None or sell_px <= 0:
                return None

            profit = _profit_ratio(position, mid)
            trigger = "profit_protection" if (profit is not None and profit >= self.settings.stop_loss_profit_protection_threshold_pct) else "momentum"
            result = await self._submit(
                repo, position, sell_px, mid, trailing_ratio, now,
                kill_switch_enabled=kill_switch_enabled,
                active_color=active_color,
                trigger=trigger, slope=slope, peak=peak, opened_at=opened_at,
            )
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
        kill_switch_enabled: bool,
        active_color: str,
        trigger: str = "trailing_stop",
        slope: float | None = None,
        peak: Decimal | None = None,
        opened_at: datetime | None = None,
    ) -> dict[str, Any]:
        market_ticker = position.market_ticker
        peak_display = str(peak) if peak is not None else "n/a"
        mark_display = str(mid)
        sell_display = str(sell_price)

        hold_start = opened_at or _as_utc(position.created_at) or now
        hold_minutes = (now - hold_start).total_seconds() / 60
        possible_model_error = (
            loss_ratio is not None
            and loss_ratio > 0.30
            and hold_minutes < 120
        )

        client_order_id = str(uuid4())
        strategy_code = await repo.get_latest_fill_strategy_for_market_side(
            market_ticker=market_ticker,
            side=position.side,
            kalshi_env=self.settings.kalshi_env,
            before=now,
        )
        receipt = await self.execution_service.close_position(
            market_ticker=market_ticker,
            side=position.side,
            count_fp=position.count_fp,
            yes_price_dollars=sell_price,
            client_order_id=client_order_id,
            kill_switch_enabled=kill_switch_enabled,
            active_color=active_color,
            subaccount=self.settings.kalshi_subaccount or None,
        )

        # Kill switch: clean noop — no submit/reentry checkpoints, rate-limited warning.
        if receipt.status == "kill_switch_blocked":
            ks_cp_key = f"stop_loss_kill_switch_suppressed:{self.settings.kalshi_env}:{market_ticker}"
            ks_cp = await repo.get_checkpoint(ks_cp_key)
            rate_limited = (
                ks_cp is not None
                and ks_cp.payload.get("suppressed_at") is not None
                and now - datetime.fromisoformat(ks_cp.payload["suppressed_at"]) < timedelta(minutes=10)
            )
            if not rate_limited:
                await repo.log_ops_event(
                    severity="warning",
                    summary=(
                        f"Stop-loss suppressed by kill switch [{trigger}]: "
                        f"{market_ticker} {position.side}"
                        + (f" loss={loss_ratio:.0%}" if loss_ratio is not None else "")
                    ),
                    source="stop_loss",
                    payload={
                        "market_ticker": market_ticker,
                        "side": position.side,
                        "trigger": trigger,
                        "mid_mark": str(mid),
                        "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
                        "peak_price": str(peak) if peak is not None else None,
                        "action": "stop_loss_kill_switch_suppressed",
                    },
                )
                await repo.set_checkpoint(
                    ks_cp_key,
                    cursor=None,
                    payload={"suppressed_at": now.isoformat(), "trigger": trigger},
                )
            logger.warning(
                "Stop-loss suppressed by kill switch [%s]: %s %s loss=%s",
                trigger,
                market_ticker,
                position.side,
                f"{loss_ratio:.0%}" if loss_ratio is not None else "n/a",
            )
            return {
                "market_ticker": market_ticker,
                "side": position.side,
                "action": "stop_loss_kill_switch_suppressed",
                "trigger": trigger,
                "kill_switch_blocked": True,
                "rate_limited_event": rate_limited,
            }

        shadow = receipt.status == "shadow_skipped"
        order_data = dict((receipt.details or {}).get("order") or {})
        order_status = str(order_data.get("status") or receipt.status)
        normalized_order_status = order_status.strip().lower()
        terminal_filled = normalized_order_status in {"filled", "executed"}
        terminal_unfilled = normalized_order_status in {"cancelled", "canceled", "expired"}
        submit_failed = not shadow and receipt.external_order_id is None and not terminal_filled and not terminal_unfilled
        action = f"stop_loss_{trigger}_shadow" if shadow else f"stop_loss_{trigger}"

        event_payload: dict[str, Any] = {
            "market_ticker": market_ticker,
            "side": position.side,
            "count_fp": str(position.count_fp),
            "average_price_dollars": str(position.average_price_dollars),
            "mid_mark": str(mid),
            "sell_price": str(sell_price),
            "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "peak_price": str(peak) if peak is not None else None,
            "hold_minutes": round(hold_minutes, 1),
            "shadow_mode": shadow,
            "action": action,
            "trigger": trigger,
            "possible_model_error": possible_model_error,
            "exec_status": receipt.status,
            "strategy_code": strategy_code,
            "position_opened_at": hold_start.isoformat(),
        }
        if slope is not None:
            event_payload["momentum_slope_cents_per_min"] = round(slope, 4)

        submit_payload: dict[str, Any] = {
            "submitted_at": now.isoformat(),
            "stopped_at": now.isoformat(),
            "stopped_side": position.side,
            "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "peak_price": str(peak) if peak is not None else None,
            "trigger": trigger,
            "position_opened_at": hold_start.isoformat(),
        }

        if shadow:
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_FILLED_EXIT
            submit_payload["client_order_id"] = client_order_id
        elif not submit_failed:
            submit_payload.update(
                {
                    "client_order_id": client_order_id,
                    "order_status": order_status,
                    "kalshi_order_id": receipt.external_order_id,
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
            event_payload["order_response"] = receipt.details
        else:
            logger.warning("stop_loss order submit failed for %s: %s", market_ticker, receipt.details)
            event_payload["submit_error"] = str(receipt.details)
            submit_payload["submit_error"] = str(receipt.details)
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_SUBMIT_FAILED
            # Back off on outright submit errors to avoid spamming an illiquid book.
            submit_payload["next_retry_at"] = (now + timedelta(minutes=30)).isoformat()

        if not shadow and receipt.status != "inactive_color_skipped":
            await repo.save_order(
                ticket_id=None,
                client_order_id=client_order_id,
                market_ticker=market_ticker,
                status=order_status,
                side=position.side,
                action="sell",
                yes_price_dollars=sell_price,
                count_fp=position.count_fp,
                raw=receipt.details or {},
                kalshi_order_id=receipt.external_order_id,
                kalshi_env=self.settings.kalshi_env,
                strategy_code=strategy_code,
            )

        await repo.log_ops_event(
            severity="warning",
            summary=(
                f"Stop loss {'(shadow) ' if shadow else ''}triggered [{trigger}]: "
                f"{market_ticker} {position.side}"
                + (f" loss={loss_ratio:.0%}" if loss_ratio is not None else "")
                + f" peak={peak_display} mark={mark_display} sell={sell_display}"
                + (f" slope={slope:.3f}¢/min" if slope is not None else "")
            ),
            source="stop_loss",
            payload=event_payload,
        )

        await repo.set_checkpoint(
            f"stop_loss_submit:{self.settings.kalshi_env}:{market_ticker}",
            cursor=None,
            payload=submit_payload,
        )
        if not submit_failed:
            await repo.set_checkpoint(
                f"stop_loss_reentry:{self.settings.kalshi_env}:{market_ticker}",
                cursor=None,
                payload={
                    "stopped_at": now.isoformat(),
                    "stopped_side": position.side,
                    "client_order_id": submit_payload.get("client_order_id"),
                    "kalshi_order_id": submit_payload.get("kalshi_order_id"),
                    "order_status": submit_payload.get("order_status"),
                    "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
                    "peak_price": str(peak) if peak is not None else None,
                    "trigger": trigger,
                    "position_opened_at": hold_start.isoformat(),
                    "outcome_status": submit_payload.get("outcome_status"),
                    "reverse_evaluated": False,
                },
            )

        logger.warning(
            "Stop loss %s [%s]: %s %s trailing_loss=%s peak=%s mark=%s sell=%s slope=%s hold=%.0fmin%s",
            "shadow" if shadow else ("submit_failed" if submit_failed else "submitted"),
            trigger,
            market_ticker,
            position.side,
            f"{loss_ratio:.0%}" if loss_ratio is not None else "n/a",
            peak_display,
            mark_display,
            sell_display,
            f"{slope:.3f}¢/min" if slope is not None else "n/a",
            hold_minutes,
            " [POSSIBLE MODEL ERROR]" if possible_model_error else "",
        )
        return event_payload
