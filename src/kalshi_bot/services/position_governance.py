from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kalshi_bot.config import Settings
    from kalshi_bot.db.models import FillRecord, OrderRecord
    from kalshi_bot.db.repositories import PlatformRepository

from kalshi_bot.services.risk_policy import probability_midband_block_reason

STOP_LOSS_OUTCOME_SUBMIT_FAILED = "submit_failed"
STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL = "submitted_pending_fill"
STOP_LOSS_OUTCOME_FILLED_EXIT = "filled_exit"
STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED = "cancelled_or_unfilled"

STOP_LOSS_OUTCOME_LABELS = {
    STOP_LOSS_OUTCOME_SUBMIT_FAILED: "Stop-Loss Failed",
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL: "Stop-Loss Pending",
    STOP_LOSS_OUTCOME_FILLED_EXIT: "Exited",
    STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED: "Unfilled Exit",
}

POSITION_CLASSIFICATION_COMPLIANT = "compliant"
POSITION_CLASSIFICATION_LEGACY_BELOW_PRICE_FLOOR = "legacy_below_price_floor"
POSITION_CLASSIFICATION_THESIS_DRIFTED = "thesis_drifted"
POSITION_CLASSIFICATION_STOP_LOSS_IMPAIRED = "stop_loss_impaired"
POSITION_CLASSIFICATION_FULLY_PRICED_NO_ADD = "fully_priced_no_add"

POSITION_CLASSIFICATION_LABELS = {
    POSITION_CLASSIFICATION_COMPLIANT: "Compliant",
    POSITION_CLASSIFICATION_LEGACY_BELOW_PRICE_FLOOR: "Legacy",
    POSITION_CLASSIFICATION_THESIS_DRIFTED: "Thesis Drift",
    POSITION_CLASSIFICATION_STOP_LOSS_IMPAIRED: "Stop-Loss Impaired",
    POSITION_CLASSIFICATION_FULLY_PRICED_NO_ADD: "Fully Priced",
}

_TERMINAL_UNFILLED_ORDER_STATUSES = {"cancelled", "canceled", "expired", "rejected", "failed"}
_TERMINAL_FILLED_ORDER_STATUSES = {"filled", "executed"}


def _normalize_side(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"yes", "no"} else None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _datetime_on_or_after(value: Any, threshold: datetime | None) -> bool:
    if threshold is None:
        return False
    parsed = _parse_dt(value)
    return parsed is not None and parsed >= threshold


def stop_loss_outcome_from_payloads(
    submit_payload: dict[str, Any] | None,
    reentry_payload: dict[str, Any] | None,
) -> str | None:
    for payload in (reentry_payload or {}, submit_payload or {}):
        status = str(payload.get("outcome_status") or "").strip().lower()
        if status in STOP_LOSS_OUTCOME_LABELS:
            return status
    if (submit_payload or {}).get("submit_error"):
        return STOP_LOSS_OUTCOME_SUBMIT_FAILED
    if reentry_payload:
        return STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL
    return None


def stop_loss_stopped_at_from_payloads(
    submit_payload: dict[str, Any] | None,
    reentry_payload: dict[str, Any] | None,
) -> datetime | None:
    return _parse_dt((reentry_payload or {}).get("stopped_at")) or _parse_dt((submit_payload or {}).get("stopped_at"))


def stop_loss_outcome_label(status: str | None) -> str | None:
    if status is None:
        return None
    return STOP_LOSS_OUTCOME_LABELS.get(status, status.replace("_", " ").title())


def stop_loss_is_impaired(status: str | None) -> bool:
    return status in {STOP_LOSS_OUTCOME_SUBMIT_FAILED, STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL}


def stop_loss_reentry_blocked(
    status: str | None,
    *,
    stopped_at: datetime | None,
    cooldown_seconds: int,
    now: datetime | None = None,
) -> bool:
    if status in {STOP_LOSS_OUTCOME_SUBMIT_FAILED, STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL}:
        return True
    if status != STOP_LOSS_OUTCOME_FILLED_EXIT or stopped_at is None:
        return False
    now_utc = now or datetime.now(UTC)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=UTC)
    return now_utc < (stopped_at + timedelta(seconds=cooldown_seconds))


def latest_signal_fair_yes_dollars(signal_payload: dict[str, Any] | None) -> Decimal | None:
    payload = dict(signal_payload or {})
    direct = _decimal_or_none(payload.get("fair_yes_dollars"))
    if direct is not None:
        return direct
    trader_context = payload.get("trader_context") or {}
    if isinstance(trader_context, dict):
        return _decimal_or_none(trader_context.get("fair_yes_dollars"))
    return None


def latest_signal_model_side(signal_payload: dict[str, Any] | None) -> str | None:
    payload = dict(signal_payload or {})
    recommended = _normalize_side(payload.get("recommended_side"))
    if recommended is not None:
        return recommended
    fair_yes = latest_signal_fair_yes_dollars(payload)
    if fair_yes is None:
        return None
    if fair_yes > Decimal("0.5000"):
        return "yes"
    if fair_yes < Decimal("0.5000"):
        return "no"
    return None


def latest_signal_side_fair_dollars(signal_payload: dict[str, Any] | None, side: str) -> Decimal | None:
    fair_yes = latest_signal_fair_yes_dollars(signal_payload)
    if fair_yes is None:
        return None
    if side == "yes":
        return fair_yes.quantize(Decimal("0.0001"))
    return (Decimal("1.0000") - fair_yes).quantize(Decimal("0.0001"))


def classify_position_health(
    *,
    settings: Settings,
    position_side: str,
    average_price_dollars: Decimal,
    current_price_dollars: Decimal | None,
    signal_payload: dict[str, Any] | None,
    stop_loss_outcome_status: str | None,
    stop_loss_stopped_at: datetime | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    latest_model_side = latest_signal_model_side(signal_payload)
    fair_yes = latest_signal_fair_yes_dollars(signal_payload)
    side_fair = latest_signal_side_fair_dollars(signal_payload, position_side)
    trade_regime = str((signal_payload or {}).get("trade_regime") or "standard").strip().lower() or "standard"
    current_price = current_price_dollars if current_price_dollars is not None else average_price_dollars
    implied_edge_bps: int | None = None
    if current_price is not None and side_fair is not None:
        implied_edge_bps = int(((side_fair - current_price) * Decimal("10000")).to_integral_value())

    fresh_entry_reasons: list[str] = []
    min_price = Decimal(str(settings.risk_min_contract_price_dollars))
    if current_price is None:
        fresh_entry_reasons.append("Current side mark unavailable.")
    elif current_price < min_price:
        fresh_entry_reasons.append(f"Current side price {current_price:.4f} is below floor {min_price:.2f}.")
    if fair_yes is None:
        fresh_entry_reasons.append("Latest model fair value unavailable.")
    else:
        probability_reason = probability_midband_block_reason(
            fair_yes=fair_yes,
            edge_bps=implied_edge_bps,
            base_min_edge_bps=settings.risk_min_edge_bps,
            extremity_pct=settings.risk_min_probability_extremity_pct,
            max_extra_edge_bps=getattr(settings, "risk_probability_midband_max_extra_edge_bps", 500),
        )
        if probability_reason is not None:
            fresh_entry_reasons.append(probability_reason)
    if trade_regime != "standard":
        fresh_entry_reasons.append(f"Trade regime '{trade_regime}' is no longer permitted for fresh entries.")
    if implied_edge_bps is None:
        fresh_entry_reasons.append("Current edge cannot be computed from the latest model.")
    elif implied_edge_bps < settings.risk_min_edge_bps:
        fresh_entry_reasons.append(
            f"Current edge {implied_edge_bps}bps is below the {settings.risk_min_edge_bps}bps minimum."
        )

    fresh_entry_allowed = not fresh_entry_reasons
    reentry_blocked = stop_loss_reentry_blocked(
        stop_loss_outcome_status,
        stopped_at=stop_loss_stopped_at,
        cooldown_seconds=settings.stop_loss_reentry_cooldown_seconds,
        now=now,
    )
    add_on_blocked = not settings.risk_allow_position_add_ons

    if stop_loss_is_impaired(stop_loss_outcome_status):
        classification = POSITION_CLASSIFICATION_STOP_LOSS_IMPAIRED
    elif average_price_dollars < min_price:
        classification = POSITION_CLASSIFICATION_LEGACY_BELOW_PRICE_FLOOR
    elif latest_model_side is not None and latest_model_side != position_side:
        classification = POSITION_CLASSIFICATION_THESIS_DRIFTED
    elif trade_regime != "standard":
        classification = POSITION_CLASSIFICATION_THESIS_DRIFTED
    elif fresh_entry_allowed:
        classification = POSITION_CLASSIFICATION_COMPLIANT
    else:
        classification = POSITION_CLASSIFICATION_FULLY_PRICED_NO_ADD

    badges: list[str] = []
    if classification == POSITION_CLASSIFICATION_LEGACY_BELOW_PRICE_FLOOR:
        badges.append("Legacy")
    if classification == POSITION_CLASSIFICATION_THESIS_DRIFTED:
        badges.append("Thesis Drift")
    if classification == POSITION_CLASSIFICATION_STOP_LOSS_IMPAIRED:
        badges.append(
            "Stop-Loss Failed"
            if stop_loss_outcome_status == STOP_LOSS_OUTCOME_SUBMIT_FAILED
            else "Stop-Loss Pending"
        )
    if classification == POSITION_CLASSIFICATION_FULLY_PRICED_NO_ADD:
        badges.append("Fully Priced")
    if reentry_blocked:
        badges.append("Re-entry Blocked")
    if add_on_blocked:
        badges.append("No Add-Ons")

    return {
        "classification": classification,
        "classification_label": POSITION_CLASSIFICATION_LABELS[classification],
        "latest_model_side": latest_model_side,
        "latest_model_fair_yes_dollars": str(fair_yes.quantize(Decimal("0.0001"))) if fair_yes is not None else None,
        "latest_model_side_fair_dollars": str(side_fair.quantize(Decimal("0.0001"))) if side_fair is not None else None,
        "implied_edge_bps": implied_edge_bps,
        "fresh_entry_allowed": fresh_entry_allowed,
        "fresh_entry_reasons": fresh_entry_reasons,
        "stop_loss_outcome_status": stop_loss_outcome_status,
        "stop_loss_outcome_label": stop_loss_outcome_label(stop_loss_outcome_status),
        "reentry_blocked": reentry_blocked,
        "add_on_blocked": add_on_blocked,
        "badges": badges,
    }


def _stop_loss_payload_for_status(
    *,
    payload: dict[str, Any],
    status: str,
    order: OrderRecord | None,
    fill: FillRecord | None,
    stopped_at: datetime | None,
    cooldown_seconds: int,
    repair_reason: str | None = None,
) -> dict[str, Any]:
    updated = dict(payload)
    updated["outcome_status"] = status
    updated["stopped_at"] = _iso_or_none(stopped_at) or updated.get("stopped_at")
    if order is not None:
        updated["client_order_id"] = order.client_order_id
        updated["order_status"] = order.status
        updated["kalshi_order_id"] = order.kalshi_order_id
    if fill is not None:
        updated["exit_fill_trade_id"] = fill.trade_id
        updated["exit_fill_order_id"] = fill.order_id
        updated["exit_fill_at"] = _iso_or_none(fill.created_at)
        effective_stopped_at = stopped_at or fill.created_at
        updated["stopped_at"] = _iso_or_none(effective_stopped_at)
        if effective_stopped_at is not None:
            updated["cooldown_expires_at"] = _iso_or_none(
                effective_stopped_at + timedelta(seconds=cooldown_seconds)
            )
    if repair_reason:
        updated["repair_reason"] = repair_reason
        updated["repaired_at"] = datetime.now(UTC).isoformat()
    return updated


@dataclass(slots=True)
class StopLossCheckpointRefresh:
    market_ticker: str
    outcome_status: str | None
    repaired: bool = False


async def refresh_stop_loss_checkpoints(
    repo: PlatformRepository,
    *,
    settings: Settings,
    kalshi_env: str,
    subaccount: int,
    market_tickers: list[str] | None = None,
    log_repairs: bool = False,
) -> list[StopLossCheckpointRefresh]:
    submit_prefix = f"stop_loss_submit:{kalshi_env}:"
    reentry_prefix = f"stop_loss_reentry:{kalshi_env}:"
    submit_checkpoints = await repo.list_checkpoints(prefix=submit_prefix)
    reentry_checkpoints = await repo.list_checkpoints(prefix=reentry_prefix)
    submit_by_ticker = {
        checkpoint.stream_name.removeprefix(submit_prefix): checkpoint
        for checkpoint in submit_checkpoints
    }
    reentry_by_ticker = {
        checkpoint.stream_name.removeprefix(reentry_prefix): checkpoint
        for checkpoint in reentry_checkpoints
    }
    tickers = set(submit_by_ticker) | set(reentry_by_ticker)
    if market_tickers is not None:
        tickers &= set(market_tickers)
    if not tickers:
        return []

    positions_by_ticker = {
        position.market_ticker: position
        for position in await repo.list_positions(
            limit=max(len(tickers) * 5, 100),
            kalshi_env=kalshi_env,
            subaccount=subaccount,
        )
    }
    orders = await repo.list_orders_for_markets(sorted(tickers), kalshi_env=kalshi_env)
    fills = await repo.list_fills_for_markets(sorted(tickers), kalshi_env=kalshi_env)
    orders_by_ticker: dict[str, list[OrderRecord]] = {}
    fills_by_ticker: dict[str, list[FillRecord]] = {}
    for order in orders:
        orders_by_ticker.setdefault(order.market_ticker, []).append(order)
    for fill in fills:
        fills_by_ticker.setdefault(fill.market_ticker, []).append(fill)

    refreshed: list[StopLossCheckpointRefresh] = []
    for ticker in sorted(tickers):
        submit_cp = submit_by_ticker.get(ticker)
        reentry_cp = reentry_by_ticker.get(ticker)
        submit_payload = dict(submit_cp.payload or {}) if submit_cp is not None else {}
        reentry_payload = dict(reentry_cp.payload or {}) if reentry_cp is not None else {}
        position = positions_by_ticker.get(ticker)
        open_position = position is not None and Decimal(str(position.count_fp)) > Decimal("0")
        stopped_side = _normalize_side(reentry_payload.get("stopped_side") or submit_payload.get("stopped_side"))
        submitted_at = _parse_dt(submit_payload.get("submitted_at")) or _parse_dt(reentry_payload.get("stopped_at"))
        client_order_id = str(
            reentry_payload.get("client_order_id")
            or submit_payload.get("client_order_id")
            or ""
        ).strip()
        kalshi_order_id = str(
            reentry_payload.get("kalshi_order_id")
            or submit_payload.get("kalshi_order_id")
            or ""
        ).strip()

        relevant_orders = [
            order
            for order in orders_by_ticker.get(ticker, [])
            if order.action == "sell"
            and (
                (client_order_id and order.client_order_id == client_order_id)
                or (kalshi_order_id and order.kalshi_order_id == kalshi_order_id)
                or (
                    not client_order_id
                    and not kalshi_order_id
                    and submitted_at is not None
                    and _datetime_on_or_after(order.created_at, submitted_at - timedelta(seconds=5))
                    and (stopped_side is None or order.side == stopped_side)
                )
            )
        ]
        relevant_orders.sort(key=lambda item: item.created_at, reverse=True)
        latest_order = relevant_orders[0] if relevant_orders else None
        latest_order_status = str(getattr(latest_order, "status", "") or "").strip().lower()

        relevant_fills = [
            fill
            for fill in fills_by_ticker.get(ticker, [])
            if fill.action == "sell"
            and (
                (latest_order is not None and fill.order_id == latest_order.id)
                or (
                    latest_order is not None
                    and latest_order.kalshi_order_id
                    and isinstance(fill.raw, dict)
                    and fill.raw.get("order_id") == latest_order.kalshi_order_id
                )
                or (
                    submitted_at is not None
                    and _datetime_on_or_after(fill.created_at, submitted_at - timedelta(seconds=5))
                )
            )
        ]
        relevant_fills.sort(key=lambda item: item.created_at, reverse=True)
        latest_fill = relevant_fills[0] if relevant_fills else None
        current_status = stop_loss_outcome_from_payloads(submit_payload, reentry_payload)
        repair_reason: str | None = None
        order_exit_confirmed = (
            latest_order is not None
            and latest_order_status in _TERMINAL_FILLED_ORDER_STATUSES
            and not open_position
        )

        if latest_fill is not None or order_exit_confirmed:
            next_status = STOP_LOSS_OUTCOME_FILLED_EXIT
        elif submit_payload.get("submit_error"):
            next_status = STOP_LOSS_OUTCOME_SUBMIT_FAILED
        elif current_status in {
            STOP_LOSS_OUTCOME_SUBMIT_FAILED,
            STOP_LOSS_OUTCOME_FILLED_EXIT,
            STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED,
        }:
            next_status = current_status
        elif latest_order is not None and latest_order_status in _TERMINAL_UNFILLED_ORDER_STATUSES:
            next_status = STOP_LOSS_OUTCOME_CANCELLED_OR_UNFILLED
        elif reentry_cp is not None and submit_cp is None and not client_order_id and not kalshi_order_id and open_position:
            next_status = STOP_LOSS_OUTCOME_SUBMIT_FAILED
            repair_reason = "stale_reentry_without_accepted_order"
        elif reentry_cp is not None:
            next_status = STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL
        else:
            next_status = current_status

        stopped_at = (
            _parse_dt(reentry_payload.get("stopped_at"))
            or _parse_dt(submit_payload.get("stopped_at"))
            or (latest_fill.created_at if latest_fill is not None else None)
        )
        repaired = repair_reason is not None
        if next_status is not None and submit_cp is not None:
            submit_cp.payload = _stop_loss_payload_for_status(
                payload=submit_payload,
                status=next_status,
                order=latest_order,
                fill=latest_fill,
                stopped_at=stopped_at,
                cooldown_seconds=settings.stop_loss_reentry_cooldown_seconds,
                repair_reason=repair_reason,
            )
        if next_status is not None and reentry_cp is not None:
            reentry_cp.payload = _stop_loss_payload_for_status(
                payload=reentry_payload,
                status=next_status,
                order=latest_order,
                fill=latest_fill,
                stopped_at=stopped_at,
                cooldown_seconds=settings.stop_loss_reentry_cooldown_seconds,
                repair_reason=repair_reason,
            )
        if repaired and log_repairs:
            await repo.log_ops_event(
                severity="warning",
                summary=f"Ignored stale stop-loss re-entry checkpoint for {ticker}",
                source="position_governance",
                payload={
                    "market_ticker": ticker,
                    "repair_reason": repair_reason,
                    "open_position": open_position,
                },
                kalshi_env=kalshi_env,
            )
        refreshed.append(
            StopLossCheckpointRefresh(
                market_ticker=ticker,
                outcome_status=next_status,
                repaired=repaired,
            )
        )
    return refreshed
