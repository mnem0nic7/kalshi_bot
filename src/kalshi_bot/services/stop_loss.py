from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import numpy as np
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StrategyCode
from kalshi_bot.db.models import MarketPriceHistory, MarketState, PositionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED,
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
    get_position_exit_submit_checkpoint,
    set_position_exit_submit_checkpoint,
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
        price = market_state.yes_bid_dollars
    else:
        # Kalshi's order API prices every order with yes_price_dollars. For a
        # NO sell at the current NO bid, the corresponding YES price is yes_ask.
        price = market_state.yes_ask_dollars
    if price is None or price <= Decimal("0") or price >= Decimal("1"):
        return None
    return price


def _side_value_from_yes_price(yes_price: Decimal, side: str) -> Decimal:
    return yes_price if side == "yes" else Decimal("1") - yes_price


def _side_price(mid_yes: Decimal, side: str) -> Decimal:
    return mid_yes if side == "yes" else Decimal("1") - mid_yes


def _peak_price_from_history(
    prices: list[MarketPriceHistory],
    side: str,
    since: datetime | None = None,
    opened_at: datetime | None = None,
) -> Decimal | None:
    """Return highest side-appropriate mid price after `since` (post-entry peak)."""
    threshold = since if since is not None else opened_at
    candidates = [
        _side_price(row.mid_dollars, side)
        for row in prices
        if row.mid_dollars is not None
        and (threshold is None or _as_utc(row.observed_at) >= threshold)
    ]
    return max(candidates) if candidates else None


def _position_opened_at_from_fills(position: PositionRecord, fills: list[Any]) -> datetime | None:
    running = Decimal("0")
    opened_at: datetime | None = None
    relevant = sorted(
        (
            fill
            for fill in fills
            if getattr(fill, "market_ticker", None) == position.market_ticker
            and getattr(fill, "side", None) == position.side
        ),
        key=lambda fill: _as_utc(getattr(fill, "created_at", None)) or datetime.min.replace(tzinfo=UTC),
    )
    for fill in relevant:
        count = Decimal(str(getattr(fill, "count_fp", Decimal("0")) or Decimal("0")))
        action = str(getattr(fill, "action", "") or "").strip().lower()
        created_at = _as_utc(getattr(fill, "created_at", None))
        if action == "buy":
            if running <= 0 and count > 0:
                opened_at = created_at
            running += count
        elif action == "sell":
            running -= count
            if running <= 0:
                running = Decimal("0")
                opened_at = None
    return opened_at


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


def _round_trip_net_return_ratio(
    position: PositionRecord,
    *,
    sell_yes_price: Decimal,
    fee_rate: Decimal,
) -> float | None:
    avg = position.average_price_dollars
    count = position.count_fp
    if count <= 0 or avg <= 0:
        return None
    sell_value = _side_value_from_yes_price(sell_yes_price, position.side)
    entry_notional = count * avg
    entry_fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=avg,
        count=count,
        fee_rate=fee_rate,
    )
    exit_fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=sell_value,
        count=count,
        fee_rate=fee_rate,
    )
    denominator = entry_notional + entry_fee
    if denominator <= 0:
        return None
    net_return = (count * sell_value) - entry_notional - entry_fee - exit_fee
    return float(net_return / denominator)


def _strategy_for_market_ticker(market_ticker: str) -> str | None:
    ticker = str(market_ticker or "").upper()
    if "15M" in ticker:
        return "CRYPTO_15M"
    if "1H" in ticker:
        return "CRYPTO_1H"
    series = ticker.split("-", 1)[0]
    if series.startswith("KX") and series.endswith("D") and len(series) > 3:
        return "CRYPTO_1H"
    return None


def exit_retry_delay_seconds(settings: Settings, market_ticker: str) -> int:
    """Retry delay for failed/unfilled exit orders, per strategy.

    15m markets live ~900s, so the retry must be much shorter than the old
    fixed 30 minutes or the market expires before the retry ever runs.
    Shared by StopLossService and CryptoTakeProfitService.
    """
    strategy = _strategy_for_market_ticker(market_ticker)
    by_strategy = settings.stop_loss_retry_seconds_by_strategy
    if strategy and strategy in by_strategy:
        return max(10, int(by_strategy[strategy]))
    return max(10, int(settings.stop_loss_retry_seconds))


def _within_filled_exit_cooldown(payload: dict | None, now: datetime, settings: Settings) -> bool:
    """True while a just-filled exit should suppress further exit submissions.

    Shared by StopLossService and CryptoTakeProfitService. The local positions
    row lags the exchange by up to a reconcile cycle after an exit fills; any
    exit submitted from that stale row sells contracts we no longer hold and
    flips the position to the opposite side.
    """
    cooldown = int(getattr(settings, "position_exit_filled_cooldown_seconds", 0) or 0)
    if cooldown <= 0:
        return False
    raw = (payload or {}).get("submitted_at") or (payload or {}).get("stopped_at")
    if raw is None:
        return False
    try:
        submitted = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    if submitted.tzinfo is None:
        submitted = submitted.replace(tzinfo=UTC)
    return now - submitted < timedelta(seconds=cooldown)


def _strategy_csv(value: str | None) -> set[str]:
    return {
        raw.strip().upper()
        for raw in str(value or "").replace(";", ",").split(",")
        if raw.strip()
    }


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
        # Keyed by position id → last 2 (mid, observed_at) readings for rapid-drop detection
        self._prev_mids: dict[int, list[tuple[Decimal, datetime]]] = {}

    async def _refresh_stale_market_state(self, market_ticker: str, now: datetime) -> MarketState | None:
        """Fetch a live quote from Kalshi when the stored market state is stale.

        Near settlement the streamed book often goes quiet, which previously left
        open positions without stop coverage for the rest of the market's life.
        Returns a transient (never persisted) MarketState, or None if the market
        is closed or the live book is one-sided.
        """
        try:
            payload = await self.execution_service.kalshi.get_market(market_ticker)
        except Exception as exc:
            logger.warning("stop_loss stale refresh failed for %s: %s", market_ticker, exc)
            return None
        market = dict(payload.get("market") or {})
        status = str(market.get("status") or "").strip().lower()
        if status and status not in {"active", "open"}:
            return None

        def _quote_dollars(key: str) -> Decimal | None:
            # Fractional-price markets (price_level_structure tapered_deci_cent,
            # e.g. the 15m crypto series) omit the legacy integer-cent fields and
            # publish dollar-string fields instead.
            raw = market.get(f"{key}_dollars")
            if raw is not None:
                value = Decimal(str(raw))
            else:
                raw = market.get(key)
                if raw is None:
                    return None
                value = Decimal(str(raw)) / Decimal("100")
            if value <= Decimal("0") or value >= Decimal("1"):
                return None
            return value

        yes_bid = _quote_dollars("yes_bid")
        yes_ask = _quote_dollars("yes_ask")
        if yes_bid is None or yes_ask is None:
            logger.warning(
                "stop_loss stale refresh for %s returned no two-sided book (status=%s)",
                market_ticker,
                status or "unknown",
            )
            return None
        return MarketState(
            kalshi_env=self.settings.kalshi_env,
            market_ticker=market_ticker,
            source="stop_loss_stale_refresh",
            yes_bid_dollars=yes_bid,
            yes_ask_dollars=yes_ask,
            snapshot={},
            observed_at=now,
        )

    async def check_once(self) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []
        if not self.settings.stop_loss_enabled:
            return triggered

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
        # Fetch a full trading day of price history for each held ticker.
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

        stale_cutoff = timedelta(seconds=self.settings.risk_stale_market_seconds)
        for position in positions:
            ms = market_states.get(position.market_ticker)

            observed_at = _as_utc(ms.observed_at) if ms is not None else None
            needs_refresh = (
                ms is None
                or observed_at is None
                or (now - observed_at) > stale_cutoff
                # A fresh state row without a two-sided book (e.g. fractional-price
                # 15m markets whose stream writer records no quotes) still leaves
                # the position uncovered — treat it like a stale state.
                or _midpoint(ms, position.side) is None
            )
            if needs_refresh:
                refreshed = (
                    await self._refresh_stale_market_state(position.market_ticker, now)
                    if self.settings.stop_loss_stale_refresh_enabled
                    else None
                )
                if refreshed is None:
                    logger.warning(
                        "stop_loss skipping %s: no usable market state (observed_at=%s)",
                        position.market_ticker,
                        observed_at,
                    )
                    continue
                ms = refreshed

            mid = _midpoint(ms, position.side)
            if mid is None:
                logger.warning(
                    "stop_loss skipping %s: one-sided book after refresh",
                    position.market_ticker,
                )
                continue

            prices = price_histories.get(position.market_ticker, [])
            prev_readings = list(self._prev_mids.get(position.id, []))
            result = await self._evaluate_and_submit(position, ms, mid, prices, now, prev_mids=prev_readings)
            # Always update, even if trigger fired (orphaned key is harmless after position closes)
            readings = self._prev_mids.setdefault(position.id, [])
            readings.append((mid, now))
            if len(readings) > 2:
                readings.pop(0)
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
        prev_mids: list[tuple[Decimal, datetime]] = (),
    ) -> dict[str, Any] | None:
        """Evaluate one position in its own committed transaction to make the cooldown checkpoint immediately visible."""
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            kill_switch_enabled = bool(control.kill_switch_enabled)
            active_color = control.active_color

            # Shared exit-submit cooldown — read with committed isolation.
            submit_cp = await get_position_exit_submit_checkpoint(
                repo,
                kalshi_env=self.settings.kalshi_env,
                market_ticker=position.market_ticker,
            )
            if submit_cp is not None:
                outcome_status = str((submit_cp.payload or {}).get("outcome_status") or "")
                if outcome_status == STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL:
                    return None
                if outcome_status == STOP_LOSS_OUTCOME_FILLED_EXIT and _within_filled_exit_cooldown(
                    submit_cp.payload, now, self.settings
                ):
                    # The position row is stale until reconciliation lands the
                    # fill; re-submitting here sells a position we no longer
                    # hold, which opens the OPPOSITE side.
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

            inferred_strategy = _strategy_for_market_ticker(position.market_ticker)
            enabled_strategies = _strategy_csv(self.settings.stop_loss_enabled_strategies)
            if enabled_strategies and (inferred_strategy or "") not in enabled_strategies:
                return None
            effective_threshold = (
                self.settings.stop_loss_threshold_pct_by_strategy.get(inferred_strategy)
                if inferred_strategy else None
            ) or self.settings.stop_loss_threshold_pct
            hard_stop_disabled = (inferred_strategy or "") in _strategy_csv(
                self.settings.stop_loss_hard_stop_disabled_strategies
            )
            # Minimum hold before hard stop and trailing stop may fire.
            # Rapid adverse bypasses this gate intentionally — it is the crash
            # circuit-breaker and already carries an implicit ~60-90s delay
            # from requiring two consecutive prev_mids readings.
            min_hard_hold_s = (
                self.settings.stop_loss_min_hard_stop_hold_seconds_by_strategy.get(inferred_strategy)
                if inferred_strategy else None
            ) or self.settings.stop_loss_min_hard_stop_hold_seconds
            pos_created = _as_utc(position.created_at)
            hard_stop_hold_ready = (
                pos_created is None
                or (now - pos_created).total_seconds() >= min_hard_hold_s
            )
            sell_px = _sell_price(ms, position.side)
            net_return_ratio = (
                _round_trip_net_return_ratio(
                    position,
                    sell_yes_price=sell_px,
                    fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
                )
                if sell_px is not None
                else None
            )
            if hard_stop_hold_ready and not hard_stop_disabled and net_return_ratio is not None and net_return_ratio <= -effective_threshold:
                result = await self._submit(
                    repo, position, sell_px, mid, abs(net_return_ratio), now,
                    kill_switch_enabled=kill_switch_enabled,
                    active_color=active_color,
                    trigger="hard_stop",
                    net_return_ratio=net_return_ratio,
                )
                await session.commit()
                return result
            if (
                self.settings.crypto_touch_strategy_enabled
                and not self.settings.crypto_model_trained_replay_only
                and inferred_strategy in {"CRYPTO_15M", "CRYPTO_1H"}
            ):
                return None

            # Trigger 1: trailing stop — drop from post-entry peak.
            # Filtering to prices after entry makes the stop entry-relative, not
            # intraday-relative (avoids firing immediately on a position entered below
            # the prior intraday high).
            peak = _peak_price_from_history(prices, position.side, since=_as_utc(position.created_at))
            trailing_ratio: float | None = None
            if peak is not None:
                trailing_ratio = _trailing_loss_ratio(peak, mid)
                if hard_stop_hold_ready and not hard_stop_disabled and trailing_ratio >= effective_threshold:
                    if sell_px is not None and sell_px > 0:
                        result = await self._submit(
                            repo, position, sell_px, mid, trailing_ratio, now,
                            kill_switch_enabled=kill_switch_enabled,
                            active_color=active_color,
                            trigger="trailing_stop", peak=peak,
                        )
                        await session.commit()
                        return result

            # Trigger 3: rapid adverse move — 2 consecutive per-check drops ≥ threshold,
            # bypasses the hold gate to catch fast crashes inside the min-hold window.
            if not hard_stop_disabled and self.settings.stop_loss_rapid_adverse_enabled and len(prev_mids) >= 2:
                threshold = Decimal(str(self.settings.stop_loss_rapid_adverse_dollars))
                drop1 = prev_mids[-2][0] - prev_mids[-1][0]  # older→recent (adverse = positive)
                drop2 = prev_mids[-1][0] - mid               # recent→current
                if drop1 >= threshold and drop2 >= threshold:
                    if sell_px is not None and sell_px > 0:
                        result = await self._submit(
                            repo, position, sell_px, mid, trailing_ratio, now,
                            kill_switch_enabled=kill_switch_enabled,
                            active_color=active_color,
                            trigger="rapid_adverse_move",
                            peak=peak,
                            rapid_drop1=drop1,
                            rapid_drop2=drop2,
                        )
                        await session.commit()
                        return result

            # Trigger 2: adverse momentum (no P&L requirement — catches slow bleeds
            # that haven't yet hit the trailing stop threshold).
            created_at = position.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            hold_minutes = (now - created_at).total_seconds() / 60
            min_hold = (
                self.settings.stop_loss_momentum_min_hold_minutes_by_strategy.get(inferred_strategy)
                if inferred_strategy else None
            ) or self.settings.stop_loss_momentum_min_hold_minutes
            if hold_minutes < min_hold:
                return None

            slope = _momentum_slope(prices)
            if slope is None:
                return None

            slope_against = slope if position.side == "yes" else -slope
            if slope_against >= self.settings.stop_loss_momentum_slope_threshold_cents_per_min:
                return None

            if sell_px is None or sell_px <= 0:
                return None

            profit = _profit_ratio(position, mid)
            profit_threshold = (
                self.settings.stop_loss_profit_protection_threshold_pct_by_strategy.get(inferred_strategy)
                if inferred_strategy else None
            ) or self.settings.stop_loss_profit_protection_threshold_pct
            profit_protection_ready = profit is not None and profit >= profit_threshold
            if hard_stop_disabled and not profit_protection_ready:
                return None
            trigger = "profit_protection" if profit_protection_ready else "momentum"
            result = await self._submit(
                repo, position, sell_px, mid, trailing_ratio, now,
                kill_switch_enabled=kill_switch_enabled,
                active_color=active_color,
                trigger=trigger, slope=slope, peak=peak,
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
        rapid_drop1: Decimal | None = None,
        rapid_drop2: Decimal | None = None,
        net_return_ratio: float | None = None,
    ) -> dict[str, Any]:
        market_ticker = position.market_ticker
        peak_display = str(peak) if peak is not None else "n/a"
        mark_display = str(mid)
        sell_display = str(sell_price)

        created_at = position.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        hold_minutes = (now - created_at).total_seconds() / 60
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
            logger.warning("stop_loss order submit failed for %s: %s", market_ticker, exc)
            submit_failed = True
            submit_error = str(exc)

        receipt_status = receipt.status if receipt is not None else "submit_exception"
        receipt_details = receipt.details if receipt is not None else {"error": submit_error}
        shadow = receipt_status == "shadow_skipped"
        order_data = dict((receipt_details or {}).get("order") or {})
        order_status = str(order_data.get("status") or receipt_status)
        normalized_order_status = order_status.strip().lower()
        terminal_filled = normalized_order_status in {"filled", "executed"}
        terminal_unfilled = normalized_order_status in {"cancelled", "canceled", "expired"}
        if receipt is not None:
            submit_failed = (
                not shadow
                and receipt.external_order_id is None
                and not terminal_filled
                and not terminal_unfilled
            )
            if submit_failed:
                submit_error = str(receipt_details)
        action = f"stop_loss_{trigger}_shadow" if shadow else f"stop_loss_{trigger}"

        event_payload: dict[str, Any] = {
            "market_ticker": market_ticker,
            "side": position.side,
            "count_fp": str(position.count_fp),
            "average_price_dollars": str(position.average_price_dollars),
            "mid_mark": str(mid),
            "sell_price": str(sell_price),
            "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "net_return_ratio": round(net_return_ratio, 4) if net_return_ratio is not None else None,
            "peak_price": str(peak) if peak is not None else None,
            "hold_minutes": round(hold_minutes, 1),
            "shadow_mode": shadow,
            "action": action,
            "trigger": trigger,
            "possible_model_error": possible_model_error,
            "exec_status": receipt_status,
            "strategy_code": strategy_code,
        }
        if slope is not None:
            event_payload["momentum_slope_cents_per_min"] = round(slope, 4)
        if rapid_drop1 is not None:
            event_payload["rapid_adverse_drop1"] = str(rapid_drop1)
        if rapid_drop2 is not None:
            event_payload["rapid_adverse_drop2"] = str(rapid_drop2)

        submit_payload: dict[str, Any] = {
            "submitted_at": now.isoformat(),
            "stopped_at": now.isoformat(),
            "stopped_side": position.side,
            "trailing_loss_ratio": round(loss_ratio, 4) if loss_ratio is not None else None,
            "net_return_ratio": round(net_return_ratio, 4) if net_return_ratio is not None else None,
            "peak_price": str(peak) if peak is not None else None,
            "trigger": trigger,
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
                submit_payload["next_retry_at"] = (
                    now + timedelta(seconds=exit_retry_delay_seconds(self.settings, market_ticker))
                ).isoformat()
            event_payload["order_response"] = receipt_details
        else:
            event_payload["submit_error"] = submit_error
            submit_payload["submit_error"] = submit_error
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_SUBMIT_FAILED
            # Back off on outright submit errors to avoid spamming an illiquid book.
            submit_payload["next_retry_at"] = (
                now + timedelta(seconds=exit_retry_delay_seconds(self.settings, market_ticker))
            ).isoformat()

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

        await set_position_exit_submit_checkpoint(
            repo,
            kalshi_env=self.settings.kalshi_env,
            market_ticker=market_ticker,
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
                    "net_return_ratio": round(net_return_ratio, 4) if net_return_ratio is not None else None,
                    "peak_price": str(peak) if peak is not None else None,
                    "trigger": trigger,
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
