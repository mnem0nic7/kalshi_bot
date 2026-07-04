from __future__ import annotations

import logging
import re
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
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.stop_loss import exit_retry_delay_seconds, _within_filled_exit_cooldown
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED,
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
    get_position_exit_submit_checkpoint,
    set_position_exit_submit_checkpoint,
)

logger = logging.getLogger(__name__)


def _crypto_market_identity(market_ticker: str) -> tuple[str | None, str | None]:
    match = re.match(r"^KX([A-Z]+)(15M|1H)", str(market_ticker or "").upper())
    if match is None:
        hourly_match = re.match(r"^KX([A-Z]+)D-", str(market_ticker or "").upper())
        if hourly_match is None:
            return None, None
        return hourly_match.group(1), "1h"
    frequency = "15m" if match.group(2) == "15M" else "1h"
    return match.group(1), frequency


def _crypto_take_profit_frequencies(raw: str | None) -> set[str]:
    aliases = {
        "15": "15m",
        "15m": "15m",
        "15min": "15m",
        "15minute": "15m",
        "15minutes": "15m",
        "1": "1h",
        "1h": "1h",
        "1hr": "1h",
        "1hour": "1h",
        "1hours": "1h",
        "hour": "1h",
        "hourly": "1h",
    }
    values: set[str] = set()
    for item in str(raw or "").replace(";", ",").split(","):
        normalized = item.strip().lower().replace("_", "").replace("-", "")
        if not normalized:
            continue
        if normalized in {"all", "*"}:
            return {"15m", "1h"}
        value = aliases.get(normalized)
        if value is not None:
            values.add(value)
    return values or {"15m", "1h"}



def _resolve_take_profit_threshold(
    asset: str | None,
    frequency: str | None,
    *,
    global_threshold: float,
    by_asset: dict[str, float],
    by_frequency: dict[str, float],
) -> float:
    if frequency is not None and frequency in by_frequency:
        return by_frequency[frequency]
    if asset is not None and asset in by_asset:
        return by_asset[asset]
    return global_threshold


def _prediction_scaled_threshold(
    base: float,
    *,
    edge_remaining_dollars: float,
    edge_ref_dollars: float,
    min_multiplier: float,
    max_multiplier: float,
) -> float:
    """Scale the take-profit threshold by the model's shrunk remaining edge.

    No remaining edge -> min_multiplier * base (take profits sooner: the model
    says the move is done). Edge at/above the reference -> max_multiplier *
    base (let it ride). Linear in between.
    """
    if edge_ref_dollars <= 0:
        return base
    frac = min(1.0, max(0.0, edge_remaining_dollars / edge_ref_dollars))
    return base * (min_multiplier + (max_multiplier - min_multiplier) * frac)


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


def _side_value_from_yes_price(yes_price: Decimal, side: str) -> Decimal:
    return yes_price if side == "yes" else Decimal("1") - yes_price


def _profit_ratio(position: PositionRecord, mid: Decimal) -> float | None:
    avg = position.average_price_dollars
    if position.count_fp <= 0 or avg <= 0:
        return None
    return float((position.count_fp * mid - position.count_fp * avg) / (position.count_fp * avg))


def _round_trip_net_profit_ratio(
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
    net_profit = (count * sell_value) - entry_notional - entry_fee - exit_fee
    return float(net_profit / denominator)


class CryptoTakeProfitService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        execution_service: ExecutionService,
        forecast_service: Any | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.execution_service = execution_service
        # Optional CryptoForecastService for prediction-scaled thresholds;
        # None -> static thresholds only.
        self.forecast_service = forecast_service
        # ticker -> (fetched_at_utc, StrategySignal | None). Throttles inline
        # forecasts: the exit loop can run sub-second while a position is open
        # and forecast() loads model artifacts.
        self._prediction_cache: dict[str, tuple[datetime, Any | None]] = {}
        # Open-position count from the last check; the daemon loop runs hot
        # (sub-interval) while this is non-zero.
        self.last_position_count: int = 0

    @staticmethod
    def _market_for_snapshot(snapshot: CryptoMarketSnapshotRecord) -> Any:
        from kalshi_bot.crypto.services import _market_from_snapshot

        return _market_from_snapshot(snapshot)

    async def _prediction_threshold(
        self,
        position: PositionRecord,
        snapshot: CryptoMarketSnapshotRecord,
        base_threshold: float,
        now: datetime,
    ) -> tuple[float, dict[str, Any]]:
        """Resolve the effective take-profit threshold for a position.

        Returns (threshold, meta). Any missing/unusable model view falls back
        to the static base threshold (meta["threshold_mode"] == "static").
        """
        static = (base_threshold, {"threshold_mode": "static"})
        if (
            self.forecast_service is None
            or not self.settings.crypto_take_profit_prediction_scaling_enabled
        ):
            return static

        ticker = position.market_ticker
        cached = self._prediction_cache.get(ticker)
        refresh = timedelta(seconds=float(self.settings.crypto_take_profit_prediction_refresh_seconds))
        max_age = timedelta(seconds=float(self.settings.crypto_take_profit_prediction_max_age_seconds))
        if cached is None or (now - cached[0]) >= refresh:
            try:
                market = self._market_for_snapshot(snapshot)
                signal = await self.forecast_service.forecast(market)
            except Exception:
                logger.warning("crypto_take_profit forecast failed for %s", ticker, exc_info=True)
                signal = None
            cached = (now, signal)
            self._prediction_cache[ticker] = cached
        fetched_at, signal = cached
        if signal is None or (now - fetched_at) > max_age:
            return static

        trace = getattr(signal, "candidate_trace", None) or {}
        model_type = (trace.get("prediction_model") or {}).get("model_type")
        fair_yes = getattr(signal, "fair_yes_dollars", None)
        if fair_yes is None or model_type in (None, "market_mid_baseline"):
            return static

        mid = _crypto_mid(snapshot, position.side)
        if mid is None:
            return static
        fair_side = _side_value_from_yes_price(Decimal(fair_yes), position.side)
        beta = float(self.settings.crypto_edge_shrinkage_beta_floor)
        edge_remaining = beta * float(fair_side - mid)
        threshold = _prediction_scaled_threshold(
            base_threshold,
            edge_remaining_dollars=edge_remaining,
            edge_ref_dollars=float(self.settings.crypto_take_profit_edge_ref_cents) / 100.0,
            min_multiplier=float(self.settings.crypto_take_profit_min_multiplier),
            max_multiplier=float(self.settings.crypto_take_profit_max_multiplier),
        )
        return threshold, {
            "threshold_mode": "scaled",
            "model_type": model_type,
            "edge_remaining_dollars": round(edge_remaining, 4),
            "base_threshold": base_threshold,
            "effective_threshold": round(threshold, 4),
        }

    async def check_once(self) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []
        if not self.settings.crypto_take_profit_enabled:
            return triggered

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            if control.active_color != self.settings.app_color:
                self.last_position_count = 0
                await session.commit()
                return triggered
            positions = await repo.list_positions(
                limit=500,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
            )
        self.last_position_count = len(positions)

        if not positions:
            return triggered

        now = datetime.now(UTC)
        stale_cutoff = timedelta(seconds=self.settings.crypto_take_profit_stale_snapshot_seconds)
        global_threshold = self.settings.crypto_take_profit_threshold_pct
        by_asset = self.settings.crypto_take_profit_threshold_pct_by_asset
        by_frequency = self.settings.crypto_take_profit_threshold_pct_by_frequency
        enabled_frequencies = _crypto_take_profit_frequencies(self.settings.crypto_take_profit_frequencies)

        for position in positions:
            asset, frequency = _crypto_market_identity(position.market_ticker)
            if frequency not in enabled_frequencies:
                continue
            threshold = _resolve_take_profit_threshold(
                asset,
                frequency,
                global_threshold=global_threshold,
                by_asset=by_asset,
                by_frequency=by_frequency,
            )
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
                    # Stale position row until reconciliation lands the fill;
                    # re-submitting sells contracts we no longer hold and opens
                    # the opposite side.
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

        sell_px = _crypto_sell_price(snapshot, position.side)
        if sell_px is None or sell_px <= 0:
            return None

        mid = _crypto_mid(snapshot, position.side)
        if mid is None:
            return None

        profit = _round_trip_net_profit_ratio(
            position,
            sell_yes_price=sell_px,
            fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
        )
        if profit is None:
            return None

        effective_threshold, threshold_meta = await self._prediction_threshold(
            position, snapshot, threshold, now
        )
        if profit < effective_threshold:
            return None

        return await self._submit(
            position=position,
            sell_price=sell_px,
            mid=mid,
            profit=profit,
            now=now,
            kill_switch_enabled=kill_switch_enabled,
            active_color=active_color,
            threshold_meta=threshold_meta,
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
        threshold_meta: dict[str, Any] | None = None,
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
        if threshold_meta:
            event_payload["take_profit_threshold"] = threshold_meta

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
                submit_payload["next_retry_at"] = (
                    now + timedelta(seconds=exit_retry_delay_seconds(self.settings, market_ticker))
                ).isoformat()
            event_payload["order_response"] = receipt_details
        else:
            event_payload["submit_error"] = submit_error
            submit_payload["submit_error"] = submit_error
            submit_payload["outcome_status"] = STOP_LOSS_OUTCOME_SUBMIT_FAILED
            submit_payload["next_retry_at"] = (
                now + timedelta(seconds=exit_retry_delay_seconds(self.settings, market_ticker))
            ).isoformat()

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

            await set_position_exit_submit_checkpoint(
                repo,
                kalshi_env=self.settings.kalshi_env,
                market_ticker=market_ticker,
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
