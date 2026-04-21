from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import logging
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from kalshi_bot.core.enums import RoomOrigin
from kalshi_bot.db.models import FillRecord, OrderRecord, RiskVerdictRecord, Room, RoomResearchHealthRecord, RoomStrategyAuditRecord, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.strategy_regression import (
    LEAN_RECOMMENDATION_MIN_GAP as STRATEGY_LEAN_RECOMMENDATION_GAP,
    MIN_TRADE_COUNT as STRATEGY_MIN_TRADE_COUNT,
    MIN_WIN_RATE_GAP as STRATEGY_LEGACY_PROMOTION_GAP,
    RECOMMENDATION_MIN_OUTCOME_COVERAGE_RATE as STRATEGY_MIN_OUTCOME_COVERAGE_RATE,
    RECOMMENDATION_MODE as STRATEGY_RECOMMENDATION_MODE,
    STRONG_RECOMMENDATION_MIN_GAP as STRATEGY_STRONG_RECOMMENDATION_GAP,
    WINDOW_DAYS as DEFAULT_STRATEGY_WINDOW_DAYS,
    _group_strategy_rooms_by_series,
    _recommendation_decision,
    _recommendation_sort_priority,
    _rank_scored_strategy_rows,
    _score_strategy_room_outcome,
    _strategy_result_rank_key,
    _thresholds_from_dict,
    _wilson_confidence_interval,
    _would_have_traded,
)

if TYPE_CHECKING:
    from kalshi_bot.services.container import AppContainer


logger = logging.getLogger(__name__)

CONTROL_ROOM_TABS = ("overview", "training", "research", "rooms", "operations")
SUMMARY_ROOM_WINDOW_HOURS = 24
SUMMARY_ROOM_LIMIT = 60
SUMMARY_ROOM_OUTCOME_LIMIT = 500
ROOM_TAB_LIMIT = 40
POSITION_LIMIT = 100
OPS_EVENT_LIMIT = 40
RESEARCH_ACTIVE_STATUSES = {"active", "open"}
STRATEGY_WINDOW_OPTIONS = (30, 90, DEFAULT_STRATEGY_WINDOW_DAYS)
STRATEGY_EVENT_LIMIT = 80
STRATEGY_EVENT_LOOKBACK_DAYS = 14
STRATEGY_RESULT_HISTORY_LIMIT = 600
STRATEGY_RESULT_TREND_POINTS = 12
STRATEGY_APPROVAL_SOURCE = "strategy_review"
STRATEGY_APPROVAL_EVENT_KIND = "assignment_approval"
STRATEGY_APPROVAL_ASSIGNED_BY = "strategies_approval"
STRATEGY_APPROVAL_WINDOW_DAYS = DEFAULT_STRATEGY_WINDOW_DAYS
STRATEGY_APPROVAL_ELIGIBLE_STATUSES = frozenset({"strong_recommendation", "lean_recommendation"})


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_zero(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return None


def _quote_or_none(value: Any) -> Decimal | None:
    quote = _decimal_or_none(value)
    if quote is None:
        return None
    quote = quote.quantize(Decimal("0.0001"))
    return quote if quote > 0 else None


def _cents_to_dollars(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    amount = _decimal_or_none(value)
    if amount is None:
        return None
    raw = str(value).strip().lower()
    if "." not in raw and "e" not in raw:
        amount = amount / Decimal("100")
    return amount.quantize(Decimal("0.01"))


def _money_display(value: Decimal | None, *, signed: bool = False) -> str:
    if value is None:
        return "—"
    amount = value.quantize(Decimal("0.01"))
    if signed:
        if amount > 0:
            return f"+${amount}"
        if amount < 0:
            return f"-${abs(amount)}"
    return f"${amount}"


def _percent_display(value: Decimal | None, *, signed: bool = False) -> str | None:
    if value is None:
        return None
    amount = value.quantize(Decimal("0.01"))
    if signed and amount > 0:
        return f"+{amount}%"
    return f"{amount}%"


def _ratio_range_display(lower: float | None, upper: float | None) -> str:
    if lower is None or upper is None:
        return "—"
    return f"{lower:.0%}-{upper:.0%}"


def _price_display(value: Decimal | None) -> str:
    if value is None:
        return "—"
    return f"${value.quantize(Decimal('0.0001'))}"


def _pnl_tone(value: Decimal | None) -> str:
    if value is None:
        return "neutral"
    if value > 0:
        return "good"
    if value < 0:
        return "bad"
    return "neutral"


def _win_rate_display(win_rate_data: dict) -> str:
    total = win_rate_data.get("total_contracts", 0)
    if not total:
        return "—"
    won = win_rate_data.get("won_contracts", 0)
    pct = int(round(100 * won / total))
    return f"{pct}%"


def _percent_change(change: Decimal | None, baseline: Decimal | None) -> Decimal | None:
    if change is None or baseline is None or baseline <= 0:
        return None
    return ((change / baseline) * Decimal("100")).quantize(Decimal("0.01"))


def _daily_pnl_line_display(daily_pnl: Decimal | None, daily_pnl_percent: Decimal | None) -> str:
    money_text = _money_display(daily_pnl, signed=True)
    if daily_pnl is None:
        return money_text
    percent_text = _percent_display(daily_pnl_percent)
    if percent_text is None:
        return f"{money_text} today (PT)"
    return f"{money_text} ({percent_text}) today (PT)"


def _capital_bucket_from_signal_payload(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return "risky"
    explicit = str(payload.get("capital_bucket") or "").strip().lower()
    if explicit in {"safe", "risky"}:
        return explicit
    trade_regime = str(payload.get("trade_regime") or "").strip().lower()
    if trade_regime in {"near_threshold", "longshot_yes", "longshot_no"}:
        return "risky"
    if trade_regime == "standard":
        return "safe"
    return "risky"


def _dossier_value(dossier: Any | None, key: str, *, fallback_key: str | None = None, default: Any = None) -> Any:
    if dossier is None:
        return default
    if isinstance(dossier, dict):
        if key in dossier:
            return dossier.get(key, default)
        trader_context = dossier.get("trader_context") or {}
        if fallback_key is not None:
            return trader_context.get(fallback_key, default)
        return trader_context.get(key, default)
    if hasattr(dossier, key):
        return getattr(dossier, key)
    trader_context = getattr(dossier, "trader_context", None)
    if trader_context is not None:
        return getattr(trader_context, fallback_key or key, default)
    return default


def _midpoint(lower: Decimal | None, upper: Decimal | None) -> Decimal | None:
    if lower is not None and upper is not None:
        return ((lower + upper) / Decimal("2")).quantize(Decimal("0.0001"))
    return lower if lower is not None else upper


def _market_field(source: Any | None, field: str) -> Any | None:
    if source is None:
        return None
    if isinstance(source, dict):
        payload = source.get("market", source)
        return payload.get(field)
    return getattr(source, field, None)


def _position_mark_price(position_side: str, market_state: Any | None) -> tuple[Decimal | None, str | None]:
    if market_state is None:
        return None, None

    yes_bid = _quote_or_none(_market_field(market_state, "yes_bid_dollars"))
    yes_ask = _quote_or_none(_market_field(market_state, "yes_ask_dollars"))
    no_bid = _quote_or_none(_market_field(market_state, "no_bid_dollars"))
    no_ask = _quote_or_none(_market_field(market_state, "no_ask_dollars"))
    last_trade = (
        _quote_or_none(_market_field(market_state, "last_price_dollars"))
        or _quote_or_none(_market_field(market_state, "last_trade_dollars"))
    )

    if position_side == "no":
        if last_trade is not None:
            return (Decimal("1.0000") - last_trade).quantize(Decimal("0.0001")), "last_trade"
        if no_bid is not None and no_ask is not None:
            mark = _midpoint(no_bid, no_ask)
            if mark is not None:
                return mark, "midpoint"
        if yes_bid is not None and yes_ask is not None:
            derived_no_bid = (Decimal("1.0000") - yes_ask).quantize(Decimal("0.0001"))
            derived_no_ask = (Decimal("1.0000") - yes_bid).quantize(Decimal("0.0001"))
            mark = _midpoint(derived_no_bid, derived_no_ask)
            if mark is not None:
                return mark, "midpoint"
        return None, None

    if last_trade is not None:
        return last_trade, "last_trade"
    if yes_bid is not None and yes_ask is not None:
        mark = _midpoint(yes_bid, yes_ask)
        if mark is not None:
            return mark, "midpoint"
    if no_bid is not None and no_ask is not None:
        derived_yes_bid = (Decimal("1.0000") - no_ask).quantize(Decimal("0.0001"))
        derived_yes_ask = (Decimal("1.0000") - no_bid).quantize(Decimal("0.0001"))
        mark = _midpoint(derived_yes_bid, derived_yes_ask)
        if mark is not None:
            return mark, "midpoint"
    return None, None


def _balance_summary(balance_checkpoint: Any | None, position_views: list[dict[str, Any]]) -> dict[str, Any]:
    checkpoint_payload = dict(getattr(balance_checkpoint, "payload", {}) or {})
    balance_payload = dict(checkpoint_payload.get("balance") or {})

    cash = None
    for key in ("balance", "cash_balance", "cash"):
        cash = _cents_to_dollars(balance_payload.get(key))
        if cash is not None:
            break

    raw_portfolio = None
    for key in ("portfolio_value", "portfolioValue", "portfolio"):
        raw_portfolio = _cents_to_dollars(balance_payload.get(key))
        if raw_portfolio is not None:
            break

    total_notional = sum((_decimal_or_zero(view.get("notional_dollars")) for view in position_views), Decimal("0.00"))
    total_marked_value = sum((_decimal_or_zero(view.get("current_value_dollars")) for view in position_views), Decimal("0.00"))
    all_marked = all(view.get("current_value_dollars") is not None for view in position_views) if position_views else True

    positions_value = raw_portfolio
    portfolio = None
    if cash is not None and raw_portfolio is not None:
        if raw_portfolio < cash:
            positions_value = raw_portfolio.quantize(Decimal("0.01"))
            portfolio = (cash + positions_value).quantize(Decimal("0.01"))
        else:
            portfolio = raw_portfolio.quantize(Decimal("0.01"))
            positions_value = (portfolio - cash).quantize(Decimal("0.01"))
    elif all_marked:
        positions_value = total_marked_value.quantize(Decimal("0.01"))
        if cash is not None:
            portfolio = (cash + positions_value).quantize(Decimal("0.01"))

    gain_loss = None
    if positions_value is not None:
        gain_loss = (positions_value - total_notional).quantize(Decimal("0.01"))
    elif all(view.get("unrealized_pnl_dollars") is not None for view in position_views):
        gain_loss = sum(
            (_decimal_or_zero(view.get("unrealized_pnl_dollars")) for view in position_views),
            Decimal("0.00"),
        ).quantize(Decimal("0.01"))

    return {
        "cash_dollars": str(cash) if cash is not None else None,
        "cash_display": _money_display(cash),
        "portfolio_dollars": str(portfolio) if portfolio is not None else None,
        "portfolio_display": _money_display(portfolio),
        "positions_value_dollars": str(positions_value) if positions_value is not None else None,
        "positions_value_display": _money_display(positions_value),
        "gain_loss_dollars": str(gain_loss) if gain_loss is not None else None,
        "gain_loss_display": _money_display(gain_loss, signed=True),
        "gain_loss_tone": _pnl_tone(gain_loss),
        "updated_at": _iso_or_none(getattr(balance_checkpoint, "updated_at", None)),
    }


def _confidence_band(value: float | None) -> str:
    if value is None:
        return "none"
    if value >= 0.90:
        return "high"
    if value >= 0.75:
        return "medium"
    return "low"


def _system_status(
    *,
    control: dict[str, Any],
    runtime_health: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    updated_at = _parse_iso(runtime_health.get("updated_at"))
    watchdog_age_seconds = (now - updated_at).total_seconds() if updated_at is not None else None
    active_color = str(control.get("active_color") or runtime_health.get("active_color") or "unknown")
    active_health = dict((runtime_health.get("colors") or {}).get(active_color) or {})
    colors = runtime_health.get("colors") or {}
    unhealthy_colors = [name for name, payload in colors.items() if not bool((payload or {}).get("combined_healthy"))]

    if bool(control.get("kill_switch_enabled")):
        level = "critical"
        label = "Kill Switch On"
        detail = "Trading is disabled by the operator kill switch."
    elif watchdog_age_seconds is None or watchdog_age_seconds > 300 or not bool(active_health.get("combined_healthy")):
        level = "critical"
        label = "Critical"
        detail = "Active deployment is unhealthy or watchdog freshness is critical."
    elif watchdog_age_seconds > 60 or unhealthy_colors:
        level = "warning"
        label = "Degraded"
        detail = "Runtime is up, but one or more deployments need attention."
    else:
        level = "healthy"
        label = "Healthy"
        detail = "Kill switch is off and the active deployment looks healthy."

    return {
        "level": level,
        "label": label,
        "detail": detail,
        "watchdog_updated_at": runtime_health.get("updated_at"),
        "watchdog_age_seconds": round(watchdog_age_seconds, 1) if watchdog_age_seconds is not None else None,
        "active_color": active_color,
        "unhealthy_colors": unhealthy_colors,
    }


def _classify_room(bundle: Any) -> dict[str, str]:
    outcome = bundle.outcome
    if outcome.fills_observed > 0 or outcome.orders_submitted > 0 or (
        outcome.ticket_generated and outcome.risk_status == "approved"
    ):
        return {"status": "succeeded", "label": "Succeeded", "tone": "good"}
    if outcome.blocked_by == "eligibility" or outcome.final_status in {"stand_down", "no_trade"} or outcome.stand_down_reason:
        return {"status": "stand_down", "label": "Stand Down", "tone": "warning"}
    if outcome.blocked_by in {"risk", "research_gate"} or outcome.final_status in {"blocked", "research_blocked"}:
        return {"status": "blocked", "label": "Blocked", "tone": "bad"}
    if outcome.final_status == "failed" or outcome.room_stage == "failed":
        return {"status": "failed", "label": "Failed", "tone": "bad"}
    if outcome.room_stage != "complete":
        return {"status": "running", "label": "Running", "tone": "neutral"}
    return {"status": "failed", "label": "Failed", "tone": "bad"}


def _room_reason(bundle: Any) -> str | None:
    outcome = bundle.outcome
    return (
        outcome.stand_down_reason
        or outcome.blocked_by
        or outcome.risk_status
        or outcome.final_status
        or outcome.room_stage
    )


def _compact_ticket(ticket: Any) -> dict[str, Any] | None:
    """Extract the 4 display-relevant fields from a trade ticket dict or None."""
    if not ticket:
        return None
    d = dict(ticket) if not isinstance(ticket, dict) else ticket
    return {
        "action": d.get("action"),
        "side": d.get("side"),
        "yes_price_dollars": str(d.get("yes_price_dollars") or ""),
        "count_fp": str(d.get("count_fp") or ""),
    }


def _room_view(bundle: Any) -> dict[str, Any]:
    room = dict(bundle.room)
    classification = _classify_room(bundle)
    return {
        "id": room["id"],
        "url": f"/rooms/{room['id']}",
        "name": room["name"],
        "market_ticker": room["market_ticker"],
        "room_origin": bundle.room_origin or room.get("room_origin"),
        "stage": room["stage"],
        "updated_at": room["updated_at"],
        "created_at": room["created_at"],
        "agent_pack_version": room.get("agent_pack_version"),
        "shadow_mode": bool(room.get("shadow_mode")),
        "status": classification["status"],
        "status_label": classification["label"],
        "status_tone": classification["tone"],
        "blocked_by": bundle.outcome.blocked_by,
        "stand_down_reason": bundle.outcome.stand_down_reason,
        "final_status": bundle.outcome.final_status,
        "risk_status": bundle.outcome.risk_status,
        "ticket_generated": bundle.outcome.ticket_generated,
        "orders_submitted": bundle.outcome.orders_submitted,
        "fills_observed": bundle.outcome.fills_observed,
        "reason": _room_reason(bundle),
        "ticket": _compact_ticket(getattr(bundle, "trade_ticket", None)),
    }


def _position_view(
    position: Any,
    market_state: Any | None = None,
    live_market: Any | None = None,
    dossier: Any | None = None,
    signal_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    count = _decimal_or_zero(position.count_fp)
    avg_price = _decimal_or_zero(position.average_price_dollars)
    notional = (count * avg_price).quantize(Decimal("0.01"))
    mark_price, mark_source = _position_mark_price(str(position.side), live_market)
    if mark_price is None:
        mark_price, mark_source = _position_mark_price(str(position.side), market_state)
    current_value = (count * mark_price).quantize(Decimal("0.01")) if mark_price is not None else None
    unrealized_pnl = (current_value - notional).quantize(Decimal("0.01")) if current_value is not None else None
    recommended_size_cap_fp = _decimal_or_none(_dossier_value(dossier, "recommended_size_cap_fp"))
    model_quality_reasons = [str(reason) for reason in (_dossier_value(dossier, "model_quality_reasons", default=[]) or [])]
    return {
        "market_ticker": position.market_ticker,
        "side": position.side,
        "count_fp": str(count.quantize(Decimal("0.01"))),
        "average_price_dollars": str(avg_price.quantize(Decimal("0.0001"))),
        "average_price_display": _price_display(avg_price),
        "notional_dollars": str(notional),
        "notional_display": _money_display(notional),
        "current_price_dollars": str(mark_price) if mark_price is not None else None,
        "current_price_display": _price_display(mark_price),
        "current_value_dollars": str(current_value) if current_value is not None else None,
        "current_value_display": _money_display(current_value),
        "unrealized_pnl_dollars": str(unrealized_pnl) if unrealized_pnl is not None else None,
        "unrealized_pnl_display": _money_display(unrealized_pnl, signed=True),
        "unrealized_pnl_tone": _pnl_tone(unrealized_pnl),
        "mark_source": mark_source,
        "mark_observed_at": _iso_or_none(getattr(market_state, "observed_at", None)),
        "trade_regime": str(_dossier_value(dossier, "trade_regime", default="standard") or "standard"),
        "model_quality_status": str(_dossier_value(dossier, "model_quality_status", default="pass") or "pass"),
        "model_quality_reasons": model_quality_reasons,
        "recommended_size_cap_fp": str(recommended_size_cap_fp) if recommended_size_cap_fp is not None else None,
        "warn_only_blocked": bool(_dossier_value(dossier, "warn_only_blocked", default=False)),
        "capital_bucket": _capital_bucket_from_signal_payload(signal_payload),
        "updated_at": _iso_or_none(position.updated_at),
    }


async def _load_live_position_markets(container: AppContainer, positions: list[Any]) -> dict[str, dict[str, Any]]:
    kalshi_client = getattr(container, "kalshi", None)
    if kalshi_client is None or not positions:
        return {}

    tickers = list(dict.fromkeys(str(position.market_ticker) for position in positions if getattr(position, "market_ticker", None)))
    if not tickers:
        return {}

    async def fetch_market(ticker: str) -> tuple[str, dict[str, Any] | None]:
        try:
            return ticker, await kalshi_client.get_market(ticker)
        except Exception:
            logger.warning("Failed to load live market quote for dashboard position %s", ticker, exc_info=True)
            return ticker, None

    results = await asyncio.gather(*(fetch_market(ticker) for ticker in tickers))
    return {
        ticker: payload
        for ticker, payload in results
        if payload is not None
    }


def _positions_summary(positions: list[Any], position_views: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    total_contracts = sum((abs(_decimal_or_zero(position.count_fp)) for position in positions), Decimal("0.00"))
    total_notional = sum(
        (abs(_decimal_or_zero(position.count_fp)) * _decimal_or_zero(position.average_price_dollars) for position in positions),
        Decimal("0.00"),
    )
    total_unrealized = None
    total_value = None
    total_value_is_marked = False
    if position_views is not None and positions:
        if all(item.get("unrealized_pnl_dollars") is not None for item in position_views):
            total_unrealized = sum(
                (_decimal_or_zero(item.get("unrealized_pnl_dollars")) for item in position_views),
                Decimal("0.00"),
            ).quantize(Decimal("0.01"))
        all_marked = all(item.get("current_value_dollars") is not None for item in position_views)
        total_value = sum(
            (
                _decimal_or_zero(item.get("current_value_dollars") or item.get("notional_dollars"))
                for item in position_views
            ),
            Decimal("0.00"),
        ).quantize(Decimal("0.01"))
        total_value_is_marked = all_marked
    # keep legacy keys for callers that still reference them
    total_current_value = total_value if total_value_is_marked else None
    total_notional_q = total_notional.quantize(Decimal("0.01")) if positions else Decimal("0.00")
    return {
        "count": len(positions),
        "total_contracts": str(total_contracts.quantize(Decimal("0.01"))) if positions else "0.00",
        "total_notional_dollars": str(total_notional_q),
        "total_notional_display": _money_display(total_notional_q),
        "total_current_value_dollars": str(total_current_value) if total_current_value is not None else None,
        "total_current_value_display": _money_display(total_current_value),
        "total_value_dollars": str(total_value) if total_value is not None else None,
        "total_value_display": _money_display(total_value),
        "total_value_label": "Current" if total_value_is_marked else "Cost",
        "total_value_is_marked": total_value_is_marked,
        "total_unrealized_pnl_dollars": str(total_unrealized) if total_unrealized is not None else None,
        "total_unrealized_pnl_display": _money_display(total_unrealized, signed=True),
        "total_unrealized_pnl_tone": _pnl_tone(total_unrealized),
        "has_pnl_summary": total_unrealized is not None,
    }


def _capital_bucket_summary(snapshot: Any) -> dict[str, Any]:
    return {
        "safe_used_dollars": str(snapshot.safe_used_dollars),
        "safe_used_display": _money_display(snapshot.safe_used_dollars),
        "safe_remaining_dollars": str(snapshot.safe_remaining_dollars),
        "safe_remaining_display": _money_display(snapshot.safe_remaining_dollars),
        "safe_reserve_target_dollars": str(snapshot.safe_reserve_target_dollars),
        "safe_reserve_target_display": _money_display(snapshot.safe_reserve_target_dollars),
        "risky_used_dollars": str(snapshot.risky_used_dollars),
        "risky_used_display": _money_display(snapshot.risky_used_dollars),
        "risky_limit_dollars": str(snapshot.risky_limit_dollars),
        "risky_limit_display": _money_display(snapshot.risky_limit_dollars),
        "risky_remaining_dollars": str(snapshot.risky_remaining_dollars),
        "risky_remaining_display": _money_display(snapshot.risky_remaining_dollars),
        "overall_used_dollars": str(snapshot.overall_used_dollars),
        "overall_used_display": _money_display(snapshot.overall_used_dollars),
        "overall_remaining_dollars": str(snapshot.overall_remaining_dollars),
        "overall_remaining_display": _money_display(snapshot.overall_remaining_dollars),
    }


def _active_room_view(room: Any) -> dict[str, Any]:
    return {
        "id": room.id,
        "market_ticker": room.market_ticker,
        "stage": room.stage,
        "active_color": room.active_color,
        "created_at": _iso_or_none(room.created_at),
        "updated_at": _iso_or_none(room.updated_at),
    }


def _ops_event_view(event: Any) -> dict[str, Any]:
    payload = dict(event.payload or {})
    return {
        "id": event.id,
        "severity": event.severity,
        "summary": event.summary,
        "source": event.source,
        "created_at": _iso_or_none(event.created_at),
        "updated_at": _iso_or_none(event.updated_at),
        "details": payload,
    }


def _research_market_view(item: dict[str, Any]) -> dict[str, Any]:
    dossier = item.get("dossier") or {}
    summary = dict(dossier.get("summary") or {})
    freshness = dict(dossier.get("freshness") or {})
    confidence = _float_or_none(summary.get("research_confidence"))
    close_ts = item.get("close_ts")
    close_at = datetime.fromtimestamp(close_ts, UTC).isoformat() if close_ts else None
    status = str(item.get("status") or "unknown")
    return {
        "market_ticker": item["market_ticker"],
        "label": item["label"],
        "market_type": item["market_type"],
        "status": status,
        "status_group": "active" if status in RESEARCH_ACTIVE_STATUSES else "closed",
        "series_ticker": item.get("series_ticker"),
        "can_trade": bool(item.get("can_trade")),
        "notes": list(item.get("notes") or []),
        "close_ts": close_ts,
        "close_at": close_at,
        "confidence": confidence,
        "confidence_band": _confidence_band(confidence),
        "gate_passed": bool(((dossier.get("gate") or {}).get("passed"))),
        "gate_reasons": list((dossier.get("gate") or {}).get("reasons") or []),
        "mode": dossier.get("mode"),
        "source_coverage": summary.get("source_coverage"),
        "refreshed_at": freshness.get("refreshed_at"),
        "expires_at": freshness.get("expires_at"),
        "has_dossier": bool(dossier),
        "json_url": f"/api/research/{item['market_ticker']}",
    }


def _series_filter_options(
    market_views: list[dict[str, Any]],
    *,
    templates: list[Any],
) -> list[dict[str, str]]:
    label_by_series: dict[str, str] = {}
    for template in templates:
        series_ticker = str(getattr(template, "series_ticker", "") or "").strip()
        if not series_ticker:
            continue
        label = str(
            getattr(template, "location_name", None)
            or getattr(template, "display_name", None)
            or series_ticker
        ).strip()
        label_by_series[series_ticker] = label

    for item in market_views:
        series_ticker = str(item.get("series_ticker") or "").strip()
        if not series_ticker or series_ticker in label_by_series:
            continue
        label_by_series[series_ticker] = str(item.get("label") or series_ticker).strip()

    options = [{"id": "all", "label": "All Series"}]
    options.extend(
        {"id": series_ticker, "label": label_by_series[series_ticker]}
        for series_ticker in sorted(label_by_series, key=lambda item: (label_by_series[item].lower(), item))
    )
    return options


def _recent_room_outcomes(room_views: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    window_start = now - timedelta(hours=SUMMARY_ROOM_WINDOW_HOURS)
    recent = []
    for room in room_views:
        updated_at = _parse_iso(room.get("updated_at"))
        if updated_at is None or updated_at < window_start:
            continue
        recent.append(room)
    counts = Counter(room["status"] for room in recent)
    total = len(recent)
    succeeded = counts.get("succeeded", 0)
    resolved_total = (
        succeeded
        + counts.get("blocked", 0)
        + counts.get("stand_down", 0)
        + counts.get("failed", 0)
    )
    return {
        "window_hours": SUMMARY_ROOM_WINDOW_HOURS,
        "total": total,
        "resolved_total": resolved_total,
        "succeeded": succeeded,
        "blocked": counts.get("blocked", 0),
        "stand_down": counts.get("stand_down", 0),
        "failed": counts.get("failed", 0),
        "running": counts.get("running", 0),
        "success_rate": round(succeeded / resolved_total, 4) if resolved_total else 0.0,
    }


async def _configured_markets(container: AppContainer) -> list[dict[str, Any]]:
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        dossier_records = await repo.list_research_dossiers(limit=200)
        await session.commit()
    dossiers_by_market = {record.market_ticker: record.payload for record in dossier_records}
    configured_markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    try:
        discoveries = await container.discovery_service.discover_configured_markets()
    except Exception:
        logger.exception("Failed to load configured markets for control room")
        discoveries = []
    for discovery in discoveries:
        configured_markets.append(
            {
                "market_ticker": discovery.mapping.market_ticker,
                "label": discovery.mapping.label,
                "market_type": discovery.mapping.market_type,
                "status": discovery.status,
                "can_trade": discovery.can_trade,
                "notes": discovery.notes,
                "series_ticker": discovery.mapping.series_ticker,
                "close_ts": discovery.close_ts,
                "dossier": dossiers_by_market.get(discovery.mapping.market_ticker),
            }
        )
        seen.add(discovery.mapping.market_ticker)
    for mapping in container.weather_directory.all():
        if mapping.market_ticker in seen:
            continue
        configured_markets.append(
            {
                "market_ticker": mapping.market_ticker,
                "label": mapping.label,
                "market_type": mapping.market_type,
                "status": "configured",
                "can_trade": False,
                "notes": ["No live market snapshot loaded yet."],
                "series_ticker": mapping.series_ticker,
                "close_ts": None,
                "dossier": dossiers_by_market.get(mapping.market_ticker),
            }
        )
    configured_markets.sort(
        key=lambda item: (
            0 if str(item.get("status") or "") in RESEARCH_ACTIVE_STATUSES else 1,
            item.get("close_ts") or 2**31,
            _float_or_none(((item.get("dossier") or {}).get("summary") or {}).get("research_confidence")) or 2.0,
            item["market_ticker"],
        )
    )
    return configured_markets


async def _research_confidence_summary(container: AppContainer) -> dict[str, Any]:
    configured_market_tickers = {
        str(mapping.market_ticker)
        for mapping in container.weather_directory.all()
        if getattr(mapping, "market_ticker", None)
    }
    if not configured_market_tickers:
        return {"average": None, "count": 0, "sparkline": []}

    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        dossier_records = await repo.list_research_dossiers(limit=max(len(configured_market_tickers) * 4, 200))
        await session.commit()

    confidences = [
        confidence
        for record in dossier_records
        if record.market_ticker in configured_market_tickers
        for confidence in [_float_or_none(record.confidence)]
        if confidence is not None
    ]
    return {
        "average": round(sum(confidences) / len(confidences), 2) if confidences else None,
        "count": len(confidences),
        "sparkline": list(reversed(confidences[:12])),
    }


async def _current_intel_board(container: AppContainer, *, limit: int = 8) -> list[dict[str, Any]]:
    """Cross-market snapshot: what should the operator look at right now?

    Returns up to `limit` rows sorted gate-passed first, then by confidence
    descending. Each row carries enough data to answer: is this market
    actionable, what's the fair value / edge, and if blocked, why?
    """
    configured_tickers = {
        str(mapping.market_ticker)
        for mapping in container.weather_directory.all()
        if getattr(mapping, "market_ticker", None)
    }
    if not configured_tickers:
        return []

    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        records = await repo.list_research_dossiers(limit=max(len(configured_tickers) * 4, 200))
        await session.commit()

    # Keep only the latest record per ticker (list_research_dossiers is ordered
    # by updated_at desc, so the first hit per ticker is freshest).
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    for record in records:
        ticker = record.market_ticker
        if ticker not in configured_tickers or ticker in seen:
            continue
        seen.add(ticker)
        d = record.payload or {}
        gate = d.get("gate") or {}
        freshness = d.get("freshness") or {}
        tc = d.get("trader_context") or {}
        summary_d = d.get("summary") or {}
        age_seconds: int | None = None
        refreshed_at_str = freshness.get("refreshed_at")
        if refreshed_at_str:
            try:
                dt = datetime.fromisoformat(refreshed_at_str.replace("Z", "+00:00"))
                age_seconds = round((now - dt.astimezone(UTC)).total_seconds())
            except ValueError:
                pass
        rows.append({
            "ticker": ticker,
            "gate_passed": bool(gate.get("passed")),
            "gate_reasons": list(gate.get("reasons") or []),
            "fair_yes_dollars": str(tc.get("fair_yes_dollars") or ""),
            "confidence": _float_or_none(summary_d.get("research_confidence")),
            "age_seconds": age_seconds,
            "stale": bool(freshness.get("stale")),
        })

    rows.sort(key=lambda r: (
        0 if r["gate_passed"] else 1,
        -(r["confidence"] or 0.0),
        r["ticker"],
    ))
    return rows[:limit]


async def _recent_room_bundles(container: AppContainer, *, limit: int) -> list[Any]:
    return await container.training_export_service.export_room_bundles(
        limit=limit,
        include_non_complete=True,
        origins=[RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value],
    )


async def _recent_room_outcome_bundles(container: AppContainer, *, now: datetime) -> list[Any]:
    return await container.training_export_service.export_room_bundles(
        limit=SUMMARY_ROOM_OUTCOME_LIMIT,
        include_non_complete=True,
        origins=[RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value],
        updated_since=now - timedelta(hours=SUMMARY_ROOM_WINDOW_HOURS),
    )


async def _recent_room_outcome_views(container: AppContainer, *, now: datetime) -> list[dict[str, Any]]:
    window_start = now - timedelta(hours=SUMMARY_ROOM_WINDOW_HOURS)
    ticket_count_sq = select(func.count(TradeTicketRecord.id)).where(TradeTicketRecord.room_id == Room.id).scalar_subquery()
    order_count_sq = (
        select(func.count(OrderRecord.id))
        .select_from(OrderRecord)
        .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
        .where(TradeTicketRecord.room_id == Room.id)
        .scalar_subquery()
    )
    fill_count_sq = (
        select(func.count(FillRecord.id))
        .select_from(FillRecord)
        .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
        .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
        .where(TradeTicketRecord.room_id == Room.id)
        .scalar_subquery()
    )
    risk_status_sq = (
        select(RiskVerdictRecord.status)
        .select_from(RiskVerdictRecord)
        .join(TradeTicketRecord, RiskVerdictRecord.ticket_id == TradeTicketRecord.id)
        .where(TradeTicketRecord.room_id == Room.id)
        .order_by(RiskVerdictRecord.updated_at.desc())
        .limit(1)
        .scalar_subquery()
    )
    gate_passed_sq = (
        select(RoomResearchHealthRecord.gate_passed)
        .where(RoomResearchHealthRecord.room_id == Room.id)
        .limit(1)
        .scalar_subquery()
    )
    eligibility_passed_sq = (
        select(RoomStrategyAuditRecord.eligibility_passed)
        .where(RoomStrategyAuditRecord.room_id == Room.id)
        .limit(1)
        .scalar_subquery()
    )
    stand_down_reason_sq = (
        select(RoomStrategyAuditRecord.stand_down_reason)
        .where(RoomStrategyAuditRecord.room_id == Room.id)
        .limit(1)
        .scalar_subquery()
    )

    async with container.session_factory() as session:
        result = await session.execute(
            select(
                Room,
                ticket_count_sq.label("ticket_count"),
                order_count_sq.label("order_count"),
                fill_count_sq.label("fill_count"),
                risk_status_sq.label("risk_status"),
                gate_passed_sq.label("gate_passed"),
                eligibility_passed_sq.label("eligibility_passed"),
                stand_down_reason_sq.label("stand_down_reason"),
            )
            .where(
                Room.room_origin.in_([RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value]),
                Room.updated_at >= window_start,
            )
            .order_by(Room.updated_at.desc())
        )
        rows = list(result.all())
        await session.commit()

    room_views: list[dict[str, Any]] = []
    for room, ticket_count, order_count, fill_count, risk_status, gate_passed, eligibility_passed, stand_down_reason in rows:
        blocked_by = None
        if gate_passed is False:
            blocked_by = "research_gate"
        elif eligibility_passed is False:
            blocked_by = "eligibility"
        elif risk_status == "blocked":
            blocked_by = "risk"

        bundle = SimpleNamespace(
            room={
                "id": room.id,
                "name": room.name,
                "market_ticker": room.market_ticker,
                "stage": room.stage,
                "updated_at": _iso_or_none(room.updated_at),
                "created_at": _iso_or_none(room.created_at),
                "agent_pack_version": room.agent_pack_version,
                "shadow_mode": room.shadow_mode,
            },
            room_origin=room.room_origin,
            outcome=SimpleNamespace(
                fills_observed=int(fill_count or 0),
                orders_submitted=int(order_count or 0),
                ticket_generated=bool(ticket_count),
                risk_status=risk_status,
                blocked_by=blocked_by,
                final_status=room.stage,
                stand_down_reason=stand_down_reason,
                room_stage=room.stage,
            ),
        )
        room_views.append(_room_view(bundle))
    return room_views


def _error_alert_summary(ops_events: list[Any]) -> dict[str, Any]:
    errors = [e for e in ops_events if e.severity == "error"]
    warnings = [e for e in ops_events if e.severity == "warning"]
    return {
        "error_count": len(errors),
        "warning_count": len(warnings),
        "most_recent": errors[0].summary if errors else (warnings[0].summary if warnings else None),
    }


def _summary_payload(
    *,
    now: datetime,
    control: Any,
    runtime_health: dict[str, Any],
    positions: list[Any],
    training_status: dict[str, Any],
    research_confidence: dict[str, Any],
    room_views: list[dict[str, Any]],
    intel_board: list[dict[str, Any]] | None = None,
    ops_events: list[Any] | None = None,
) -> dict[str, Any]:
    room_outcomes = _recent_room_outcomes(room_views, now=now)

    return {
        "as_of": now.isoformat(),
        "system_status": _system_status(
            control={
                "active_color": control.active_color,
                "kill_switch_enabled": control.kill_switch_enabled,
            },
            runtime_health=runtime_health,
            now=now,
        ),
        "active_deployment": {
            "active_color": control.active_color,
            "kill_switch_enabled": control.kill_switch_enabled,
            "watchdog_updated_at": runtime_health.get("updated_at"),
            "watchdog_age_seconds": (
                round((now - _parse_iso(runtime_health.get("updated_at"))).total_seconds(), 1)
                if _parse_iso(runtime_health.get("updated_at")) is not None
                else None
            ),
            "last_action": runtime_health.get("last_action"),
            "last_failover": runtime_health.get("last_failover"),
            "last_boot_recovery": runtime_health.get("last_boot_recovery"),
        },
        "open_positions": _positions_summary(positions),
        "research_confidence": research_confidence,
        "current_intel_board": intel_board or [],
        "room_outcomes": room_outcomes,
        "error_alert": _error_alert_summary(ops_events or []),
    }


def _overview_payload(
    *,
    now: datetime,
    control: Any,
    runtime_health: dict[str, Any],
    ops_events: list[Any],
    positions: list[Any],
    training_status: dict[str, Any],
    self_improve_status: dict[str, Any],
    heuristic_status: dict[str, Any],
) -> dict[str, Any]:
    return {
        "tab": "overview",
        "as_of": now.isoformat(),
        "control": {
            "active_color": control.active_color,
            "kill_switch_enabled": control.kill_switch_enabled,
            "execution_lock_holder": control.execution_lock_holder,
        },
        "system_status": _system_status(
            control={
                "active_color": control.active_color,
                "kill_switch_enabled": control.kill_switch_enabled,
            },
            runtime_health=runtime_health,
            now=now,
        ),
        "runtime_health": runtime_health,
        "top_blockers": list(training_status.get("top_blockers") or []),
        "next_actions": list(training_status.get("next_actions") or []),
        "ops_events": [_ops_event_view(event) for event in ops_events],
        "positions_summary": _positions_summary(positions),
        "positions": [_position_view(p) for p in positions],
        "self_improve": self_improve_status,
        "heuristics": heuristic_status,
    }


async def build_control_room_summary(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        runtime_health = await container.watchdog_service.get_status(repo)
        positions = await repo.list_positions(limit=POSITION_LIMIT)
        ops_events = await repo.list_ops_events(limit=20)
        daily_pnl = await repo.get_daily_pnl_dollars()
        await session.commit()

    research_confidence, room_bundles, room_outcome_views, intel_board = await asyncio.gather(
        _research_confidence_summary(container),
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
        _recent_room_outcome_views(container, now=now),
        _current_intel_board(container),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    payload = _summary_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        positions=positions,
        training_status=training_status,
        research_confidence=research_confidence,
        room_views=room_outcome_views,
        intel_board=intel_board,
        ops_events=ops_events,
    )
    payload["daily_pnl_display"] = _money_display(daily_pnl, signed=True)
    payload["daily_pnl_tone"] = _pnl_tone(daily_pnl)
    return payload


async def build_control_room_tab(container: AppContainer, tab: str) -> dict[str, Any]:
    if tab not in CONTROL_ROOM_TABS:
        raise ValueError(f"Unsupported control room tab: {tab}")
    if tab == "overview":
        return await _build_overview_tab(container)
    if tab == "training":
        return await _build_training_tab(container)
    if tab == "research":
        return await _build_research_tab(container)
    if tab == "rooms":
        return await _build_rooms_tab(container)
    return await _build_operations_tab(container)


async def build_control_room_bootstrap(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        runtime_health = await container.watchdog_service.get_status(repo)
        positions = await repo.list_positions(limit=POSITION_LIMIT)
        ops_events = await repo.list_ops_events(limit=8)
        await session.commit()

    research_confidence, room_bundles, room_outcome_views, self_improve_status, heuristic_status, intel_board = await asyncio.gather(
        _research_confidence_summary(container),
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
        _recent_room_outcome_views(container, now=now),
        container.self_improve_service.get_dashboard_status(),
        container.historical_intelligence_service.get_dashboard_status(),
        _current_intel_board(container),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    summary = _summary_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        positions=positions,
        training_status=training_status,
        research_confidence=research_confidence,
        room_views=room_outcome_views,
        intel_board=intel_board,
    )
    overview = _overview_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        ops_events=ops_events,
        positions=positions[:12],
        training_status=training_status,
        self_improve_status=self_improve_status,
        heuristic_status=heuristic_status,
    )
    return {
        "summary": summary,
        "initial_tab": "overview",
        "initial_tab_payload": overview,
        "tabs": [
            {"id": "overview", "label": "Overview"},
            {"id": "training", "label": "Training & Historical"},
            {"id": "research", "label": "Research"},
            {"id": "rooms", "label": "Rooms"},
            {"id": "operations", "label": "Operations"},
        ],
        "refresh_interval_seconds": 15,
    }


async def _build_overview_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        runtime_health = await container.watchdog_service.get_status(repo)
        ops_events = await repo.list_ops_events(limit=8)
        positions = await repo.list_positions(limit=12)
        await session.commit()
    room_bundles, self_improve_status, heuristic_status = await asyncio.gather(
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
        container.self_improve_service.get_dashboard_status(),
        container.historical_intelligence_service.get_dashboard_status(),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    return _overview_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        ops_events=ops_events,
        positions=positions,
        training_status=training_status,
        self_improve_status=self_improve_status,
        heuristic_status=heuristic_status,
    )


async def _build_training_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    training_status, historical_status, heuristic_status = await asyncio.gather(
        container.training_corpus_service.get_status(persist_readiness=False),
        container.historical_training_service.get_status(),
        container.historical_intelligence_service.get_status(),
    )
    return {
        "tab": "training",
        "as_of": now.isoformat(),
        "quality": {
            "summary": training_status.get("quality_debt_summary") or {},
            "exclusion_reasons": training_status.get("quality_exclusion_reasons") or {},
            "recent_exclusion_memory": (training_status.get("recent_exclusion_memory") or {}).get("by_market") or [],
            "top_blockers": training_status.get("top_blockers") or [],
            "next_actions": training_status.get("next_actions") or [],
            "recent_builds": training_status.get("recent_dataset_builds") or [],
        },
        "historical": {
            "corpus": {
                "imported_market_days": historical_status.get("imported_market_days"),
                "imported_market_count": historical_status.get("imported_market_count"),
                "replayed_checkpoint_count": historical_status.get("replayed_checkpoint_count"),
                "clean_historical_trainable_count": historical_status.get("clean_historical_trainable_count"),
                "settlement_mismatch_count": historical_status.get("settlement_mismatch_count"),
                "settlement_mismatch_breakdown": historical_status.get("settlement_mismatch_breakdown") or {},
                "source_replay_coverage": historical_status.get("source_replay_coverage") or {},
                "checkpoint_archive_coverage": historical_status.get("checkpoint_archive_coverage") or {},
                "external_archive_coverage": historical_status.get("external_archive_coverage") or {},
                "external_archive_recovery": historical_status.get("external_archive_recovery") or {},
                "replay_corpus": historical_status.get("replay_corpus") or {},
                "coverage_repair_summary": historical_status.get("coverage_repair_summary") or {},
                "checkpoint_archive_promotion_count": historical_status.get("checkpoint_archive_promotion_count") or 0,
            },
            "samples": {
                "source_replay_coverage": (historical_status.get("source_replay_coverage") or {}).get("market_day_coverage") or [],
                "checkpoint_archive_coverage": (historical_status.get("checkpoint_archive_coverage") or {}).get("market_day_coverage") or [],
                "external_archive_coverage": (historical_status.get("external_archive_coverage") or {}).get("market_day_coverage") or [],
                "replay_corpus": (historical_status.get("replay_corpus") or {}).get("market_day_coverage") or [],
                "coverage_backlog": (historical_status.get("coverage_backlog") or {}).get("samples") or [],
            },
            "readiness": historical_status.get("historical_build_readiness") or {},
            "confidence_progress": historical_status.get("confidence_progress") or {},
            "heuristics": heuristic_status,
        },
        "pipeline": {
            "recent_import_runs": historical_status.get("recent_import_runs") or [],
            "recent_pipeline_runs": historical_status.get("recent_pipeline_runs") or [],
            "latest_pipeline_run": historical_status.get("latest_pipeline_run"),
            "bootstrap_progress": historical_status.get("bootstrap_progress"),
            "replay_refresh_counts_by_cause": historical_status.get("replay_refresh_counts_by_cause") or {},
            "stale_build_count": historical_status.get("stale_build_count") or 0,
        },
        "backlog": {
            "settlement_maturity": training_status.get("settlement_maturity") or {},
            "unsettled_backlog_by_market": training_status.get("unsettled_backlog_by_market") or {},
            "promotable_market_day_counts": historical_status.get("promotable_market_day_counts") or {},
            "coverage_backlog": historical_status.get("coverage_backlog") or {},
            "recent_exclusion_memory": training_status.get("recent_exclusion_memory") or {},
        },
    }


async def _build_research_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    configured_markets = await _configured_markets(container)
    market_views = [_research_market_view(item) for item in configured_markets]
    active_count = sum(1 for item in market_views if item["status_group"] == "active")
    closed_count = sum(1 for item in market_views if item["status_group"] == "closed")
    confidences = [item["confidence"] for item in market_views if item["confidence"] is not None]
    return {
        "tab": "research",
        "as_of": now.isoformat(),
        "counts": {
            "active": active_count,
            "closed": closed_count,
            "tracked": len(market_views),
            "average_confidence": round(sum(confidences) / len(confidences), 2) if confidences else None,
        },
        "series_filters": _series_filter_options(
            market_views,
            templates=container.weather_directory.templates(),
        ),
        "markets": market_views,
    }


async def _build_rooms_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    room_bundles, room_outcome_bundles, configured_markets = await asyncio.gather(
        _recent_room_bundles(container, limit=ROOM_TAB_LIMIT),
        _recent_room_outcome_bundles(container, now=now),
        _configured_markets(container),
    )
    room_views = [_room_view(bundle) for bundle in room_bundles]
    room_outcome_views = [_room_view(bundle) for bundle in room_outcome_bundles]
    return {
        "tab": "rooms",
        "as_of": now.isoformat(),
        "rooms": room_views,
        "room_outcomes": _recent_room_outcomes(room_outcome_views, now=now),
        "quick_create_markets": [
            {
                "market_ticker": item["market_ticker"],
                "label": item["label"],
                "series_ticker": item.get("series_ticker"),
                "status_group": "active" if str(item.get("status") or "") in RESEARCH_ACTIVE_STATUSES else "inactive",
            }
            for item in configured_markets[:30]
        ],
    }


async def build_env_dashboard(container: AppContainer, kalshi_env: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        positions = await repo.list_positions(limit=100, kalshi_env=kalshi_env)
        ops_events = await repo.list_ops_events(limit=50)
        active_rooms = await repo.list_active_rooms(
            kalshi_env=kalshi_env,
            updated_within_seconds=container.settings.trigger_active_room_stale_seconds,
            limit=20,
        )
        market_states = await repo.list_market_states([position.market_ticker for position in positions])
        balance_checkpoint = await repo.get_checkpoint("reconcile")
        runtime_health = await container.watchdog_service.get_status(repo)
        pack = await container.agent_pack_service.get_pack_for_color(repo, container.settings.app_color)
        thresholds = container.agent_pack_service.runtime_thresholds(pack)
        signal_payload_by_ticker = await repo.latest_signal_payloads_for_markets(
            market_tickers=[position.market_ticker for position in positions],
            kalshi_env=kalshi_env,
        )
        total_capital = await repo.get_total_capital_dollars()
        daily_pnl_baseline = await repo.get_daily_portfolio_baseline_dollars()
        win_rate_data = await repo.get_fill_win_rate_30d()
        capital_buckets = await repo.portfolio_bucket_snapshot(
            kalshi_env=kalshi_env,
            subaccount=container.settings.kalshi_subaccount,
            total_capital_dollars=total_capital if total_capital is not None else Decimal(str(thresholds.risk_max_position_notional_dollars)),
            safe_capital_reserve_ratio=thresholds.risk_safe_capital_reserve_ratio,
            risky_capital_max_ratio=thresholds.risk_risky_capital_max_ratio,
        )
        dossier_by_ticker = {
            position.market_ticker: await repo.get_research_dossier(position.market_ticker)
            for position in positions
        }
        await session.commit()

    severity_rank = {"error": 0, "warning": 1, "info": 2}
    alerts = sorted(
        [e for e in ops_events if e.severity in ("error", "warning")],
        key=lambda e: severity_rank.get(e.severity, 3),
    )
    market_state_by_ticker = {item.market_ticker: item for item in market_states}
    live_market_by_ticker = await _load_live_position_markets(container, positions)
    position_views = [
        _position_view(
            position,
            market_state_by_ticker.get(position.market_ticker),
            live_market_by_ticker.get(position.market_ticker),
            dossier_by_ticker.get(position.market_ticker).payload if dossier_by_ticker.get(position.market_ticker) is not None else None,
            signal_payload_by_ticker.get(position.market_ticker),
        )
        for position in positions
    ]
    portfolio_summary = _balance_summary(balance_checkpoint, position_views)
    current_portfolio_dollars = _decimal_or_none(portfolio_summary.get("portfolio_dollars")) or total_capital
    daily_pnl = None
    if current_portfolio_dollars is not None and daily_pnl_baseline is not None:
        daily_pnl = (current_portfolio_dollars - daily_pnl_baseline).quantize(Decimal("0.01"))
    positions_summary = _positions_summary(positions, position_views)
    positions_summary["capital_buckets"] = _capital_bucket_summary(capital_buckets)
    daily_pnl_percent = _percent_change(daily_pnl, daily_pnl_baseline)
    return {
        "kalshi_env": kalshi_env,
        "as_of": now.isoformat(),
        "portfolio": portfolio_summary,
        "daily_pnl_dollars": str(daily_pnl) if daily_pnl is not None else None,
        "daily_pnl_display": _money_display(daily_pnl, signed=True),
        "daily_pnl_percent": str(daily_pnl_percent) if daily_pnl_percent is not None else None,
        "daily_pnl_percent_display": _percent_display(daily_pnl_percent),
        "daily_pnl_line_display": _daily_pnl_line_display(daily_pnl, daily_pnl_percent),
        "daily_pnl_tone": _pnl_tone(daily_pnl),
        "win_rate_display": _win_rate_display(win_rate_data),
        "win_rate_contracts": f"{int(win_rate_data.get('won_contracts', 0))}W / {int(win_rate_data.get('total_contracts', 0))}T",
        "positions_summary": positions_summary,
        "positions": position_views,
        "alerts": [_ops_event_view(e) for e in alerts],
        "active_rooms": [_active_room_view(r) for r in active_rooms],
        "runtime_health": runtime_health,
    }


async def _build_operations_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        positions = await repo.list_positions(limit=POSITION_LIMIT)
        ops_events = await repo.list_ops_events(limit=OPS_EVENT_LIMIT)
        runtime_health = await container.watchdog_service.get_status(repo)
        await session.commit()
    self_improve_status, heuristic_status = await asyncio.gather(
        container.self_improve_service.get_dashboard_status(),
        container.historical_intelligence_service.get_dashboard_status(),
    )
    return {
        "tab": "operations",
        "as_of": now.isoformat(),
        "control": {
            "active_color": control.active_color,
            "kill_switch_enabled": control.kill_switch_enabled,
            "execution_lock_holder": control.execution_lock_holder,
        },
        "runtime_health": runtime_health,
        "positions": [
            {
                "market_ticker": position.market_ticker,
                "side": position.side,
                "count_fp": str(position.count_fp),
                "average_price_dollars": str(position.average_price_dollars),
                "updated_at": _iso_or_none(position.updated_at),
            }
            for position in positions
        ],
        "ops_events": [_ops_event_view(event) for event in ops_events],
        "self_improve": self_improve_status,
        "heuristics": heuristic_status,
    }


def _series_metadata_index(container: AppContainer) -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    for template in container.weather_directory.templates():
        metadata[template.series_ticker] = {
            "label": template.label,
            "location_name": template.location_name,
        }
    for mapping in container.weather_directory.all():
        if mapping.series_ticker and mapping.series_ticker not in metadata:
            metadata[mapping.series_ticker] = {
                "label": mapping.label,
                "location_name": mapping.location_name or mapping.label,
            }
    return metadata


def _strategy_result_value(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _strategy_window_display(window_days: int) -> str:
    return f"{window_days}d"


def _ratio_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.0%}"


def _bps_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.0f}bps"


def _compact_number(value: int) -> str:
    return f"{value:,d}"


def _coverage_display(resolved_trade_count: int, trade_count: int) -> str:
    if trade_count <= 0:
        return "—"
    return f"{resolved_trade_count}/{trade_count} scored"


def _promotion_event_has_scored_evidence(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("clears_promotion_rule") is True:
        return True
    resolved_trade_count = int(payload.get("resolved_trade_count") or 0)
    gap_to_runner_up = _float_or_none(payload.get("gap_to_runner_up"))
    return resolved_trade_count >= STRATEGY_MIN_TRADE_COUNT and (
        gap_to_runner_up is not None and gap_to_runner_up >= STRATEGY_LEGACY_PROMOTION_GAP
    )


def _normalize_strategy_result_rows(rows: list[Any], strategies_by_id: dict[int, Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        strategy_id = _strategy_result_value(row, "strategy_id")
        strategy = strategies_by_id.get(int(strategy_id)) if strategy_id is not None else None
        strategy_name = strategy.name if strategy is not None else str(_strategy_result_value(row, "strategy_name") or "unknown")
        total_pnl = _decimal_or_none(_strategy_result_value(row, "total_pnl_dollars"))
        trade_rate = _float_or_none(_strategy_result_value(row, "trade_rate"))
        win_rate = _float_or_none(_strategy_result_value(row, "win_rate"))
        avg_edge_bps = _float_or_none(_strategy_result_value(row, "avg_edge_bps"))
        run_at = _strategy_result_value(row, "run_at")
        trade_count = int(_strategy_result_value(row, "trade_count") or 0)
        resolved_trade_count = int(_strategy_result_value(row, "resolved_trade_count") or 0)
        unscored_trade_count = int(_strategy_result_value(row, "unscored_trade_count") or 0)
        outcome_coverage_rate = (resolved_trade_count / trade_count) if trade_count > 0 else None
        win_count = int(_strategy_result_value(row, "win_count") or 0)
        win_rate_interval_lower, win_rate_interval_upper = _wilson_confidence_interval(
            win_count,
            resolved_trade_count,
        )
        normalized.append({
            "strategy_id": int(strategy_id) if strategy_id is not None else None,
            "strategy_name": strategy_name,
            "series_ticker": str(_strategy_result_value(row, "series_ticker") or ""),
            "rooms_evaluated": int(_strategy_result_value(row, "rooms_evaluated") or 0),
            "trade_count": trade_count,
            "resolved_trade_count": resolved_trade_count,
            "resolved_trade_count_display": _compact_number(resolved_trade_count),
            "unscored_trade_count": unscored_trade_count,
            "unscored_trade_count_display": _compact_number(unscored_trade_count),
            "outcome_coverage_rate": outcome_coverage_rate,
            "outcome_coverage_rate_display": _ratio_display(outcome_coverage_rate),
            "outcome_coverage_display": _coverage_display(resolved_trade_count, trade_count),
            "win_count": win_count,
            "trade_rate": trade_rate,
            "trade_rate_display": _ratio_display(trade_rate),
            "win_rate": win_rate,
            "win_rate_display": _ratio_display(win_rate),
            "win_rate_interval_lower": win_rate_interval_lower,
            "win_rate_interval_upper": win_rate_interval_upper,
            "win_rate_interval_display": _ratio_range_display(
                win_rate_interval_lower,
                win_rate_interval_upper,
            ),
            "total_pnl_dollars": total_pnl,
            "total_pnl_value": float(total_pnl) if total_pnl is not None else None,
            "total_pnl_display": _money_display(total_pnl, signed=True),
            "avg_edge_bps": avg_edge_bps,
            "avg_edge_bps_display": _bps_display(avg_edge_bps),
            "run_at": _iso_or_none(run_at),
            "has_outcomes": resolved_trade_count > 0,
        })
    return normalized


def _evaluate_strategy_city_rows(
    *,
    strategies: list[Any],
    rooms: list[dict[str, Any]],
    container: AppContainer,
    run_at: datetime,
    date_from: datetime,
    date_to: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rooms_by_series, diagnostics = _group_strategy_rooms_by_series(rooms, container.weather_directory)

    result_rows: list[dict[str, Any]] = []
    for strategy in strategies:
        thresholds = _thresholds_from_dict(strategy.thresholds)
        for series_ticker, city_rooms in rooms_by_series.items():
            trade_count = 0
            resolved_trade_count = 0
            win_count = 0
            total_pnl = Decimal("0")
            edge_sum = 0.0

            for room in city_rooms:
                if not _would_have_traded(room, thresholds):
                    continue
                trade_count += 1
                edge_sum += float(room["edge_bps"])

                outcome = _score_strategy_room_outcome(room)
                if outcome is None:
                    continue
                resolved_trade_count += 1
                if outcome.settlement_result == "win":
                    win_count += 1
                total_pnl += outcome.pnl_dollars

            rooms_evaluated = len(city_rooms)
            unscored_trade_count = max(0, trade_count - resolved_trade_count)
            trade_rate = (trade_count / rooms_evaluated) if rooms_evaluated > 0 else None
            win_rate = (win_count / resolved_trade_count) if resolved_trade_count > 0 else None
            avg_edge_bps = (edge_sum / trade_count) if trade_count > 0 else None
            result_rows.append({
                "strategy_id": strategy.id,
                "strategy_name": strategy.name,
                "run_at": run_at,
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "series_ticker": series_ticker,
                "rooms_evaluated": rooms_evaluated,
                "trade_count": trade_count,
                "resolved_trade_count": resolved_trade_count,
                "unscored_trade_count": unscored_trade_count,
                "win_count": win_count,
                "total_pnl_dollars": (
                    total_pnl.quantize(Decimal("0.0001")) if resolved_trade_count > 0 else None
                ),
                "trade_rate": trade_rate,
                "win_rate": win_rate,
                "avg_edge_bps": avg_edge_bps,
            })
    return _normalize_strategy_result_rows(result_rows, {strategy.id: strategy for strategy in strategies}), diagnostics


def _threshold_value_display(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


def _threshold_label(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _group_thresholds(thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    groups: list[tuple[str, str, list[tuple[str, Any]]]] = [
        ("risk", "Risk", []),
        ("trigger", "Trigger", []),
        ("strategy", "Strategy", []),
        ("capital", "Capital", []),
        ("other", "Other", []),
    ]
    bucket_index = {key: idx for idx, (key, _, _) in enumerate(groups)}
    for threshold_key, threshold_value in thresholds.items():
        if threshold_key.startswith("risk_"):
            target = bucket_index["risk"]
        elif threshold_key.startswith("trigger_"):
            target = bucket_index["trigger"]
        elif threshold_key.startswith("strategy_"):
            target = bucket_index["strategy"]
        elif "capital" in threshold_key:
            target = bucket_index["capital"]
        else:
            target = bucket_index["other"]
        groups[target][2].append((threshold_key, threshold_value))

    grouped: list[dict[str, Any]] = []
    for _, label, items in groups:
        if not items:
            continue
        grouped.append({
            "label": label,
            "items": [
                {
                    "key": key,
                    "label": _threshold_label(key),
                    "value": _threshold_value_display(value),
                }
                for key, value in sorted(items, key=lambda pair: pair[0])
            ],
        })
    return grouped


def _city_evidence_status(
    *,
    recommendation_status: str,
) -> tuple[str, str]:
    if recommendation_status == "strong_recommendation":
        return "strong", "Strong recommendation"
    if recommendation_status == "lean_recommendation":
        return "lean", "Lean recommendation"
    if recommendation_status == "too_close":
        return "too_close", "Too close"
    if recommendation_status == "low_sample":
        return "low_sample", "Low sample"
    return "no_outcomes", "No outcomes"


def _strategy_event_view(event: Any) -> dict[str, Any]:
    payload = dict(getattr(event, "payload", None) or {})
    updated_at = _iso_or_none(getattr(event, "updated_at", None))
    source = getattr(event, "source", "unknown")
    if source == "strategy_regression" and (
        payload.get("event_kind") == "promotion" or payload.get("new_strategy") is not None
    ) and _promotion_event_has_scored_evidence(payload):
        trade_count = int(payload.get("trade_count") or 0)
        resolved_trade_count = int(payload.get("resolved_trade_count") or 0)
        outcome_coverage_rate = (resolved_trade_count / trade_count) if trade_count > 0 else None
        return {
            "kind": "promotion",
            "summary": getattr(event, "summary", "Strategy promotion"),
            "source": source,
            "created_at": updated_at,
            "series_ticker": payload.get("series_ticker"),
            "previous_strategy": payload.get("previous_strategy"),
            "new_strategy": payload.get("new_strategy"),
            "win_rate": _float_or_none(payload.get("new_win_rate")),
            "win_rate_display": _ratio_display(_float_or_none(payload.get("new_win_rate"))),
            "trade_count": trade_count,
            "resolved_trade_count": resolved_trade_count,
            "resolved_trade_count_display": _compact_number(resolved_trade_count),
            "unscored_trade_count": int(payload.get("unscored_trade_count") or 0),
            "gap_to_runner_up": _float_or_none(payload.get("gap_to_runner_up")),
            "gap_to_runner_up_display": _ratio_display(_float_or_none(payload.get("gap_to_runner_up"))),
            "outcome_coverage_rate": outcome_coverage_rate,
            "outcome_coverage_display": _coverage_display(resolved_trade_count, trade_count),
            "direction": "promoted",
        }
    if source == STRATEGY_APPROVAL_SOURCE and payload.get("event_kind") == STRATEGY_APPROVAL_EVENT_KIND:
        trade_count = int(payload.get("trade_count") or 0)
        resolved_trade_count = int(payload.get("resolved_trade_count") or 0)
        return {
            "kind": "assignment_approval",
            "summary": getattr(event, "summary", "Strategy assignment approved"),
            "source": source,
            "created_at": updated_at,
            "series_ticker": payload.get("series_ticker"),
            "previous_strategy": payload.get("previous_strategy"),
            "new_strategy": payload.get("new_strategy"),
            "recommendation_status": payload.get("recommendation_status"),
            "recommendation_label": payload.get("recommendation_label"),
            "win_rate": _float_or_none(payload.get("new_win_rate")),
            "win_rate_display": _ratio_display(_float_or_none(payload.get("new_win_rate"))),
            "trade_count": trade_count,
            "resolved_trade_count": resolved_trade_count,
            "resolved_trade_count_display": _compact_number(resolved_trade_count),
            "unscored_trade_count": int(payload.get("unscored_trade_count") or 0),
            "gap_to_runner_up": _float_or_none(payload.get("gap_to_runner_up")),
            "gap_to_runner_up_display": _ratio_display(_float_or_none(payload.get("gap_to_runner_up"))),
            "outcome_coverage_rate": _float_or_none(payload.get("outcome_coverage_rate")),
            "outcome_coverage_display": _coverage_display(resolved_trade_count, trade_count),
            "note": payload.get("note"),
            "basis_run_at": payload.get("basis_run_at"),
            "direction": "approved",
        }
    if source == "strategy_eval":
        old_bps = _float_or_none(payload.get("old_bps"))
        new_bps = _float_or_none(payload.get("new_bps"))
        return {
            "kind": "threshold_adjustment",
            "summary": getattr(event, "summary", "Threshold adjustment"),
            "source": source,
            "created_at": updated_at,
            "direction": payload.get("direction"),
            "old_bps": old_bps,
            "new_bps": new_bps,
            "change_display": (
                f"{int(old_bps)}bps -> {int(new_bps)}bps"
                if old_bps is not None and new_bps is not None
                else "—"
            ),
            "win_rate": _float_or_none(payload.get("win_rate")),
            "win_rate_display": _ratio_display(_float_or_none(payload.get("win_rate"))),
            "trade_count": int(payload.get("total_contracts") or 0),
        }
    return {
        "kind": "event",
        "summary": getattr(event, "summary", "Strategy event"),
        "source": source,
        "created_at": updated_at,
        "payload": payload,
    }


def _checkpoint_promotion_views(checkpoint: Any) -> list[dict[str, Any]]:
    if checkpoint is None or not isinstance(getattr(checkpoint, "payload", None), dict):
        return []
    promotions = list(checkpoint.payload.get("promotions") or [])
    created_at = _iso_or_none(getattr(checkpoint, "updated_at", None))
    views: list[dict[str, Any]] = []
    for promotion in promotions:
        if not _promotion_event_has_scored_evidence(promotion):
            continue
        win_rate = _float_or_none(promotion.get("win_rate"))
        trade_count = int(promotion.get("trade_count") or 0)
        resolved_trade_count = int(promotion.get("resolved_trade_count") or 0)
        views.append({
            "kind": "promotion",
            "summary": (
                f"Strategy auto-promoted for {promotion.get('series_ticker')}: "
                f"{promotion.get('previous') or 'none'} -> {promotion.get('promoted_to')}"
            ),
            "source": "strategy_regression",
            "created_at": created_at,
            "series_ticker": promotion.get("series_ticker"),
            "previous_strategy": promotion.get("previous"),
            "new_strategy": promotion.get("promoted_to"),
            "win_rate": win_rate,
            "win_rate_display": _ratio_display(win_rate),
            "trade_count": trade_count,
            "resolved_trade_count": resolved_trade_count,
            "resolved_trade_count_display": _compact_number(resolved_trade_count),
            "unscored_trade_count": int(promotion.get("unscored_trade_count") or 0),
            "gap_to_runner_up": _float_or_none(promotion.get("gap_to_runner_up")),
            "gap_to_runner_up_display": _ratio_display(_float_or_none(promotion.get("gap_to_runner_up"))),
            "outcome_coverage_display": _coverage_display(resolved_trade_count, trade_count),
            "direction": "promoted",
        })
    return views


def _city_sort_priority(city_row: dict[str, Any]) -> int:
    recommendation = city_row.get("recommendation") or {}
    return _recommendation_sort_priority(str(recommendation.get("status") or "no_outcomes"))


def _city_approval_state(
    *,
    window_days: int,
    recommendation: dict[str, Any],
    assignment_context_status: str,
) -> dict[str, Any]:
    recommendation_status = str(recommendation.get("status") or "no_outcomes")
    recommendation_name = recommendation.get("strategy_name")
    if window_days != STRATEGY_APPROVAL_WINDOW_DAYS:
        return {
            "approval_eligible": False,
            "approval_label": f"{STRATEGY_APPROVAL_WINDOW_DAYS}d only",
            "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
            "approval_requires_note": True,
            "approval_reason": "Manual approval only validates against the latest stored 180d snapshot.",
        }
    if not recommendation_name:
        return {
            "approval_eligible": False,
            "approval_label": "No recommendation",
            "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
            "approval_requires_note": True,
            "approval_reason": "No current winner is available to approve for this city.",
        }
    if assignment_context_status == "matches_recommendation":
        return {
            "approval_eligible": False,
            "approval_label": "Already assigned",
            "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
            "approval_requires_note": True,
            "approval_reason": "Canonical assignment already matches the current recommendation.",
        }
    if recommendation_status == "strong_recommendation":
        return {
            "approval_eligible": True,
            "approval_label": "Ready to approve",
            "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
            "approval_requires_note": True,
            "approval_reason": "Strong recommendations can be manually approved from the latest 180d snapshot.",
        }
    if recommendation_status == "lean_recommendation":
        return {
            "approval_eligible": True,
            "approval_label": "Ready to approve",
            "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
            "approval_requires_note": True,
            "approval_reason": "Lean recommendations can be approved with an operator note, but the evidence gap is narrower.",
        }
    if recommendation_status == "too_close":
        label = "Too close"
        reason = "The current winner does not separate enough from the runner-up to be manually approved."
    elif recommendation_status == "low_sample":
        label = "Low sample"
        reason = "This city needs more resolved trades before a recommendation can be approved."
    else:
        label = "No outcomes"
        reason = "This city does not have enough scored outcome evidence for approval."
    return {
        "approval_eligible": False,
        "approval_label": label,
        "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
        "approval_requires_note": True,
        "approval_reason": reason,
    }


def _build_strategy_research_sections(
    *,
    strategies: list[Any],
    normalized_rows: list[dict[str, Any]],
    assignments: list[Any],
    series_metadata: dict[str, dict[str, str]],
    selected_series_ticker: str | None,
    selected_strategy_name: str | None,
    window_days: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    strategy_lookup = {strategy.name: strategy for strategy in strategies}
    assignment_index = {assignment.series_ticker: assignment for assignment in assignments}
    results_index: dict[str, dict[str, dict[str, Any]]] = {}
    for row in normalized_rows:
        results_index.setdefault(row["series_ticker"], {})[row["strategy_name"]] = row

    all_series = sorted(set(results_index) | set(assignment_index))
    city_rows: list[dict[str, Any]] = []
    strategy_leaders: Counter[str] = Counter()
    strategy_assignment_counts: Counter[str] = Counter(assignment.strategy_name for assignment in assignments)

    for series_ticker in all_series:
        city_results = results_index.get(series_ticker, {})
        ranked_rows = _rank_scored_strategy_rows(list(city_results.values()))
        best_row = ranked_rows[0] if ranked_rows else None
        runner_up_row = ranked_rows[1] if len(ranked_rows) > 1 else None
        if best_row is not None:
            strategy_leaders[best_row["strategy_name"]] += 1

        assignment = assignment_index.get(series_ticker)
        assigned_name = assignment.strategy_name if assignment is not None else None
        decision = _recommendation_decision(results_by_strategy=city_results, current_name=assigned_name)
        recommendation = {
            **decision["recommendation"],
            "resolved_trade_count_display": _compact_number(
                decision["recommendation"]["resolved_trade_count"]
            ),
            "outcome_coverage_rate_display": _ratio_display(
                decision["recommendation"]["outcome_coverage_rate"]
            ),
            "gap_to_runner_up_display": _ratio_display(decision["recommendation"]["gap_to_runner_up"]),
        }
        gap_to_runner_up = decision["gap_to_runner_up"]
        gap_to_assignment = decision["gap_to_current_assignment"]

        evidence_status, evidence_label = _city_evidence_status(
            recommendation_status=recommendation["status"],
        )
        assignment_context_status = (
            "unassigned"
            if assigned_name is None
            else "matches_recommendation"
            if assigned_name == recommendation["strategy_name"]
            else "differs_from_recommendation"
        )
        approval_state = _city_approval_state(
            window_days=window_days,
            recommendation=recommendation,
            assignment_context_status=assignment_context_status,
        )

        metric_cells: list[dict[str, Any]] = []
        for strategy in strategies:
            metric_row = city_results.get(strategy.name)
            trade_count = metric_row["trade_count"] if metric_row is not None else 0
            win_rate = metric_row["win_rate"] if metric_row is not None else None
            metric_cells.append({
                "strategy_name": strategy.name,
                "selected": selected_strategy_name == strategy.name,
                "is_assigned": assigned_name == strategy.name,
                "is_best": best_row is not None and best_row["strategy_name"] == strategy.name,
                "is_runner_up": runner_up_row is not None and runner_up_row["strategy_name"] == strategy.name,
                "rooms_evaluated": metric_row["rooms_evaluated"] if metric_row is not None else 0,
                "trade_count": trade_count,
                "resolved_trade_count": metric_row["resolved_trade_count"] if metric_row is not None else 0,
                "resolved_trade_count_display": (
                    metric_row["resolved_trade_count_display"] if metric_row is not None else "0"
                ),
                "unscored_trade_count": metric_row["unscored_trade_count"] if metric_row is not None else 0,
                "unscored_trade_count_display": (
                    metric_row["unscored_trade_count_display"] if metric_row is not None else "0"
                ),
                "outcome_coverage_rate": metric_row["outcome_coverage_rate"] if metric_row is not None else None,
                "outcome_coverage_rate_display": (
                    metric_row["outcome_coverage_rate_display"] if metric_row is not None else "—"
                ),
                "outcome_coverage_display": metric_row["outcome_coverage_display"] if metric_row is not None else "—",
                "trade_rate": metric_row["trade_rate"] if metric_row is not None else None,
                "trade_rate_display": metric_row["trade_rate_display"] if metric_row is not None else "—",
                "win_rate": win_rate,
                "win_rate_display": metric_row["win_rate_display"] if metric_row is not None else "—",
                "win_rate_interval_lower": (
                    metric_row["win_rate_interval_lower"] if metric_row is not None else None
                ),
                "win_rate_interval_upper": (
                    metric_row["win_rate_interval_upper"] if metric_row is not None else None
                ),
                "win_rate_interval_display": (
                    metric_row["win_rate_interval_display"] if metric_row is not None else "—"
                ),
                "total_pnl_dollars": metric_row["total_pnl_value"] if metric_row is not None else None,
                "total_pnl_display": metric_row["total_pnl_display"] if metric_row is not None else "—",
                "avg_edge_bps": metric_row["avg_edge_bps"] if metric_row is not None else None,
                "avg_edge_bps_display": metric_row["avg_edge_bps_display"] if metric_row is not None else "—",
                "has_data": metric_row is not None and (metric_row["rooms_evaluated"] > 0 or metric_row["trade_count"] > 0),
            })

        meta = series_metadata.get(series_ticker, {})
        city_rows.append({
            "series_ticker": series_ticker,
            "city_label": meta.get("label") or series_ticker,
            "location_name": meta.get("location_name") or meta.get("label") or series_ticker,
            "selected": selected_series_ticker == series_ticker,
            "assignment": {
                "strategy_name": assigned_name,
                "assigned_at": _iso_or_none(assignment.assigned_at) if assignment is not None else None,
                "assigned_by": assignment.assigned_by if assignment is not None else None,
            },
            "assignment_context_status": assignment_context_status,
            "best_strategy": best_row["strategy_name"] if best_row is not None else None,
            "best_strategy_win_rate": best_row["win_rate"] if best_row is not None else None,
            "best_strategy_win_rate_display": best_row["win_rate_display"] if best_row is not None else "—",
            "best_resolved_trade_count": best_row["resolved_trade_count"] if best_row is not None else 0,
            "best_resolved_trade_count_display": (
                best_row["resolved_trade_count_display"] if best_row is not None else "0"
            ),
            "best_outcome_coverage_display": best_row["outcome_coverage_display"] if best_row is not None else "—",
            "runner_up_strategy": runner_up_row["strategy_name"] if runner_up_row is not None else None,
            "runner_up_win_rate_display": runner_up_row["win_rate_display"] if runner_up_row is not None else "—",
            "gap_to_runner_up": gap_to_runner_up,
            "gap_to_runner_up_display": _ratio_display(gap_to_runner_up),
            "gap_to_assignment": gap_to_assignment,
            "gap_to_assignment_display": _ratio_display(gap_to_assignment),
            "evidence_status": evidence_status,
            "evidence_label": evidence_label,
            "trade_count_sufficient": decision["clears_trade_threshold"],
            "resolved_trade_count_sufficient": decision["clears_trade_threshold"],
            "outcome_coverage_sufficient": decision["clears_coverage_threshold"],
            "gap_threshold_sufficient": decision["clears_strong_gap"],
            "lean_gap_sufficient": decision["clears_lean_gap"],
            "assignment_gap_sufficient": (
                assigned_name is None
                or gap_to_assignment is None
                or gap_to_assignment >= STRATEGY_STRONG_RECOMMENDATION_GAP
            ),
            "assignment_status": recommendation["status"],
            "assignment_status_label": recommendation["label"],
            "recommendation": recommendation,
            "can_promote": False,
            **approval_state,
            "metrics": metric_cells,
            "sort_priority": _city_sort_priority({"recommendation": recommendation}),
        })

    city_rows.sort(
        key=lambda row: (
            row["sort_priority"],
            -(row["gap_to_assignment"] if row["gap_to_assignment"] is not None else -1.0),
            -(row["gap_to_runner_up"] if row["gap_to_runner_up"] is not None else -1.0),
            row["series_ticker"],
        )
    )

    leaderboard: list[dict[str, Any]] = []
    rows_by_strategy: dict[str, list[dict[str, Any]]] = {strategy.name: [] for strategy in strategies}
    for row in normalized_rows:
        rows_by_strategy.setdefault(row["strategy_name"], []).append(row)

    for strategy in strategies:
        strategy_rows = rows_by_strategy.get(strategy.name, [])
        total_rooms = sum(row["rooms_evaluated"] for row in strategy_rows)
        total_trades = sum(row["trade_count"] for row in strategy_rows)
        total_resolved_trades = sum(row["resolved_trade_count"] for row in strategy_rows)
        total_unscored_trades = sum(row["unscored_trade_count"] for row in strategy_rows)
        total_wins = sum(row["win_count"] for row in strategy_rows)
        total_pnl = sum((row["total_pnl_dollars"] or Decimal("0")) for row in strategy_rows)
        edge_numerator = sum((row["avg_edge_bps"] or 0.0) * row["trade_count"] for row in strategy_rows)
        overall_win_rate = (total_wins / total_resolved_trades) if total_resolved_trades > 0 else None
        overall_trade_rate = (total_trades / total_rooms) if total_rooms > 0 else None
        outcome_coverage_rate = (total_resolved_trades / total_trades) if total_trades > 0 else None
        overall_avg_edge = (edge_numerator / total_trades) if total_trades > 0 else None
        total_pnl_value = float(total_pnl) if total_resolved_trades > 0 else None
        leaderboard.append({
            "name": strategy.name,
            "description": strategy.description,
            "selected": selected_strategy_name == strategy.name,
            "thresholds": strategy.thresholds,
            "threshold_groups": _group_thresholds(strategy.thresholds),
            "overall_win_rate": overall_win_rate,
            "overall_win_rate_display": _ratio_display(overall_win_rate),
            "overall_trade_rate": overall_trade_rate,
            "overall_trade_rate_display": _ratio_display(overall_trade_rate),
            "total_pnl_dollars": total_pnl_value,
            "total_pnl_display": _money_display(total_pnl if total_resolved_trades > 0 else None, signed=True),
            "avg_edge_bps": overall_avg_edge,
            "avg_edge_bps_display": _bps_display(overall_avg_edge),
            "total_rooms_evaluated": total_rooms,
            "total_rooms_evaluated_display": _compact_number(total_rooms),
            "total_trade_count": total_trades,
            "total_trade_count_display": _compact_number(total_trades),
            "total_resolved_trade_count": total_resolved_trades,
            "total_resolved_trade_count_display": _compact_number(total_resolved_trades),
            "total_unscored_trade_count": total_unscored_trades,
            "total_unscored_trade_count_display": _compact_number(total_unscored_trades),
            "outcome_coverage_rate": outcome_coverage_rate,
            "outcome_coverage_rate_display": _ratio_display(outcome_coverage_rate),
            "outcome_coverage_display": _coverage_display(total_resolved_trades, total_trades),
            "cities_led": strategy_leaders.get(strategy.name, 0),
            "assigned_city_count": strategy_assignment_counts.get(strategy.name, 0),
        })

    leaderboard.sort(
        key=lambda row: (
            row["overall_win_rate"] if row["overall_win_rate"] is not None else -1.0,
            row["total_resolved_trade_count"],
            row["total_pnl_dollars"] if row["total_pnl_dollars"] is not None else float("-inf"),
        ),
        reverse=True,
    )

    best_strategy = next((row for row in leaderboard if row["overall_win_rate"] is not None), None)
    recommendation_counts = Counter(
        (row.get("recommendation") or {}).get("status") or "no_outcomes"
        for row in city_rows
    )
    return leaderboard, city_rows, {
        "best_strategy": best_strategy,
        "recommendation_counts": dict(recommendation_counts),
        "strategy_lookup": strategy_lookup,
    }


def _trim_trend_points(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(series) <= STRATEGY_RESULT_TREND_POINTS:
        return series
    return series[-STRATEGY_RESULT_TREND_POINTS:]


def _strategy_history_series(history_rows: list[dict[str, Any]], *, strategy_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in history_rows:
        if row["strategy_name"] != strategy_name or row["run_at"] is None:
            continue
        bucket = grouped.setdefault(row["run_at"], {
            "run_at": row["run_at"],
            "trade_count": 0,
            "resolved_trade_count": 0,
            "unscored_trade_count": 0,
            "win_count": 0,
            "rooms_evaluated": 0,
            "total_pnl_dollars": Decimal("0"),
            "edge_numerator": 0.0,
        })
        bucket["trade_count"] += row["trade_count"]
        bucket["resolved_trade_count"] += row["resolved_trade_count"]
        bucket["unscored_trade_count"] += row["unscored_trade_count"]
        bucket["win_count"] += row["win_count"]
        bucket["rooms_evaluated"] += row["rooms_evaluated"]
        bucket["total_pnl_dollars"] += row["total_pnl_dollars"] or Decimal("0")
        bucket["edge_numerator"] += (row["avg_edge_bps"] or 0.0) * row["trade_count"]

    points: list[dict[str, Any]] = []
    for run_at, bucket in sorted(grouped.items(), key=lambda item: item[0]):
        trade_count = bucket["trade_count"]
        resolved_trade_count = bucket["resolved_trade_count"]
        rooms_evaluated = bucket["rooms_evaluated"]
        win_rate = (bucket["win_count"] / resolved_trade_count) if resolved_trade_count > 0 else None
        trade_rate = (trade_count / rooms_evaluated) if rooms_evaluated > 0 else None
        outcome_coverage_rate = (resolved_trade_count / trade_count) if trade_count > 0 else None
        avg_edge_bps = (bucket["edge_numerator"] / trade_count) if trade_count > 0 else None
        total_pnl = bucket["total_pnl_dollars"].quantize(Decimal("0.0001")) if resolved_trade_count > 0 else None
        points.append({
            "run_at": run_at,
            "win_rate": win_rate,
            "trade_rate": trade_rate,
            "resolved_trade_count": resolved_trade_count,
            "unscored_trade_count": bucket["unscored_trade_count"],
            "outcome_coverage_rate": outcome_coverage_rate,
            "outcome_coverage_display": _coverage_display(resolved_trade_count, trade_count),
            "avg_edge_bps": avg_edge_bps,
            "total_pnl_dollars": float(total_pnl) if total_pnl is not None else None,
            "total_pnl_display": _money_display(total_pnl, signed=True),
        })
    return _trim_trend_points(points)


def _city_history_series(history_rows: list[dict[str, Any]], *, strategies: list[Any], series_ticker: str) -> list[dict[str, Any]]:
    trend_series: list[dict[str, Any]] = []
    for strategy in strategies:
        points = [
            {
                "run_at": row["run_at"],
                "win_rate": row["win_rate"],
                "trade_rate": row["trade_rate"],
                "resolved_trade_count": row["resolved_trade_count"],
                "unscored_trade_count": row["unscored_trade_count"],
                "outcome_coverage_rate": row["outcome_coverage_rate"],
                "outcome_coverage_display": row["outcome_coverage_display"],
                "avg_edge_bps": row["avg_edge_bps"],
                "total_pnl_dollars": row["total_pnl_value"],
                "total_pnl_display": row["total_pnl_display"],
            }
            for row in sorted(history_rows, key=lambda item: item["run_at"] or "")
            if row["series_ticker"] == series_ticker and row["strategy_name"] == strategy.name and row["run_at"] is not None
        ]
        if not points:
            continue
        trend_series.append({
            "strategy_name": strategy.name,
            "points": _trim_trend_points(points),
        })
    return trend_series


def _city_detail_context(
    *,
    selected_city: dict[str, Any],
    strategies: list[Any],
    strategy_lookup: dict[str, Any],
    history_rows: list[dict[str, Any]],
    strategy_events: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_series_ticker = selected_city["series_ticker"]
    ranking = sorted(list(selected_city["metrics"]), key=_strategy_result_rank_key, reverse=True)
    best_strategy_name = selected_city.get("best_strategy")
    runner_up_name = selected_city.get("runner_up_strategy")
    assigned_name = (selected_city.get("assignment") or {}).get("strategy_name")
    assigned_metric = next((row for row in ranking if row["strategy_name"] == assigned_name), None)
    best_metric = next((row for row in ranking if row["strategy_name"] == best_strategy_name), None)
    runner_up_metric = next((row for row in ranking if row["strategy_name"] == runner_up_name), None)
    threshold_comparison: list[dict[str, Any]] = []
    for role, strategy_name in (("best", best_strategy_name), ("runner_up", runner_up_name)):
        strategy = strategy_lookup.get(strategy_name) if strategy_name else None
        if strategy is None:
            continue
        threshold_comparison.append({
            "role": role,
            "strategy_name": strategy.name,
            "threshold_groups": _group_thresholds(strategy.thresholds),
        })

    city_events = [
        event
        for event in strategy_events
        if event.get("series_ticker") == selected_series_ticker
    ][:8]
    gap_to_assignment = selected_city.get("gap_to_assignment")
    recommendation = selected_city.get("recommendation") or {}
    recommendation_rationale = {
        "best_strategy": best_strategy_name,
        "runner_up_strategy": runner_up_name,
        "current_assignment": assigned_name,
        "best_trade_count": best_metric.get("trade_count") if best_metric is not None else 0,
        "best_trade_count_display": _compact_number(best_metric.get("trade_count") if best_metric is not None else 0),
        "best_resolved_trade_count": best_metric.get("resolved_trade_count") if best_metric is not None else 0,
        "best_resolved_trade_count_display": _compact_number(best_metric.get("resolved_trade_count") if best_metric is not None else 0),
        "best_unscored_trade_count": best_metric.get("unscored_trade_count") if best_metric is not None else 0,
        "best_unscored_trade_count_display": _compact_number(best_metric.get("unscored_trade_count") if best_metric is not None else 0),
        "best_outcome_coverage_display": best_metric.get("outcome_coverage_display") if best_metric is not None else "—",
        "gap_to_runner_up": selected_city.get("gap_to_runner_up"),
        "gap_to_runner_up_display": selected_city.get("gap_to_runner_up_display"),
        "gap_to_current_assignment": gap_to_assignment,
        "gap_to_current_assignment_display": selected_city.get("gap_to_assignment_display"),
        "winner_wilson_lower": best_metric.get("win_rate_interval_lower") if best_metric is not None else None,
        "winner_wilson_upper": best_metric.get("win_rate_interval_upper") if best_metric is not None else None,
        "winner_wilson_display": best_metric.get("win_rate_interval_display") if best_metric is not None else "—",
        "runner_up_wilson_lower": (
            runner_up_metric.get("win_rate_interval_lower") if runner_up_metric is not None else None
        ),
        "runner_up_wilson_upper": (
            runner_up_metric.get("win_rate_interval_upper") if runner_up_metric is not None else None
        ),
        "runner_up_wilson_display": (
            runner_up_metric.get("win_rate_interval_display") if runner_up_metric is not None else "—"
        ),
        "recommendation_status": recommendation.get("status"),
        "recommendation_label": recommendation.get("label"),
        "writes_assignment": False,
        "meets_trade_threshold": bool(best_metric is not None and best_metric["resolved_trade_count"] >= STRATEGY_MIN_TRADE_COUNT),
        "meets_coverage_threshold": bool(selected_city.get("outcome_coverage_sufficient")),
        "meets_gap_threshold": bool(selected_city.get("gap_threshold_sufficient")),
        "meets_lean_gap_threshold": bool(selected_city.get("lean_gap_sufficient")),
        "meets_assignment_gap_threshold": (
            assigned_name is None
            or assigned_metric is None
            or assigned_metric["win_rate"] is None
            or (gap_to_assignment is not None and gap_to_assignment >= STRATEGY_STRONG_RECOMMENDATION_GAP)
        ),
        "clears_promotion_rule": False,
        "rule_trade_threshold": STRATEGY_MIN_TRADE_COUNT,
        "rule_coverage_threshold": STRATEGY_MIN_OUTCOME_COVERAGE_RATE,
        "rule_gap_threshold": STRATEGY_STRONG_RECOMMENDATION_GAP,
        "rule_lean_gap_threshold": STRATEGY_LEAN_RECOMMENDATION_GAP,
    }
    approval_context = {
        "eligible": bool(selected_city.get("approval_eligible")),
        "label": selected_city.get("approval_label"),
        "window_days": selected_city.get("approval_window_days"),
        "requires_note": bool(selected_city.get("approval_requires_note")),
        "reason": selected_city.get("approval_reason"),
        "strategy_name": recommendation.get("strategy_name"),
        "recommendation_status": recommendation.get("status"),
        "recommendation_label": recommendation.get("label"),
        "assignment_context_status": selected_city.get("assignment_context_status"),
    }
    return {
        "type": "city",
        "selected_series_ticker": selected_series_ticker,
        "selected_strategy_name": None,
        "city": selected_city,
        "ranking": ranking,
        "promotion_rationale": recommendation_rationale,
        "recommendation_rationale": recommendation_rationale,
        "approval": approval_context,
        "threshold_comparison": threshold_comparison,
        "trend": {
            "title": "Stored regression history",
            "available": True,
            "window_days": DEFAULT_STRATEGY_WINDOW_DAYS,
            "note": (
                f"Trend history uses stored {DEFAULT_STRATEGY_WINDOW_DAYS}d regression snapshots. "
                "Current city metrics above reflect the selected window."
            ),
            "series": _city_history_series(history_rows, strategies=strategies, series_ticker=selected_series_ticker),
        },
        "recent_events": city_events,
    }


def _strategy_detail_context(
    *,
    selected_strategy: dict[str, Any],
    city_rows: list[dict[str, Any]],
    strategy_lookup: dict[str, Any],
    history_rows: list[dict[str, Any]],
    strategy_events: list[dict[str, Any]],
) -> dict[str, Any]:
    strategy = strategy_lookup[selected_strategy["name"]]
    city_distribution = []
    for city_row in city_rows:
        metric = next((row for row in city_row["metrics"] if row["strategy_name"] == strategy.name), None)
        if metric is None or not metric["has_data"]:
            continue
        city_distribution.append({
            "series_ticker": city_row["series_ticker"],
            "city_label": city_row["city_label"],
            "win_rate": metric["win_rate"],
            "win_rate_display": metric["win_rate_display"],
            "trade_rate": metric["trade_rate"],
            "trade_rate_display": metric["trade_rate_display"],
            "trade_count": metric["trade_count"],
            "trade_count_display": _compact_number(metric["trade_count"]),
            "resolved_trade_count": metric["resolved_trade_count"],
            "resolved_trade_count_display": metric["resolved_trade_count_display"],
            "unscored_trade_count": metric["unscored_trade_count"],
            "outcome_coverage_display": metric["outcome_coverage_display"],
            "total_pnl_dollars": metric["total_pnl_dollars"],
            "total_pnl_display": metric["total_pnl_display"],
            "is_assigned": metric["is_assigned"],
            "is_best": metric["is_best"],
        })
    city_distribution.sort(
        key=lambda row: (
            row["win_rate"] if row["win_rate"] is not None else -1.0,
            row["resolved_trade_count"],
            row["total_pnl_dollars"] if row["total_pnl_dollars"] is not None else -10**9,
        ),
        reverse=True,
    )
    strongest = city_distribution[:3]
    weakest = list(reversed(city_distribution[-3:])) if city_distribution else []
    related_events = [
        event
        for event in strategy_events
        if event.get("new_strategy") == strategy.name or event.get("previous_strategy") == strategy.name
    ][:8]
    return {
        "type": "strategy",
        "selected_series_ticker": None,
        "selected_strategy_name": strategy.name,
        "strategy": {
            **selected_strategy,
            "threshold_groups": _group_thresholds(strategy.thresholds),
        },
        "strongest_cities": strongest,
        "weakest_cities": weakest,
        "city_distribution": city_distribution,
        "trend": {
            "title": "Stored regression history",
            "available": True,
            "window_days": DEFAULT_STRATEGY_WINDOW_DAYS,
            "note": (
                f"Trend history uses stored {DEFAULT_STRATEGY_WINDOW_DAYS}d regression snapshots. "
                "Current leaderboard metrics above reflect the selected window."
            ),
            "points": _strategy_history_series(history_rows, strategy_name=strategy.name),
        },
        "recent_events": related_events,
    }


async def build_strategies_dashboard(
    container: AppContainer,
    *,
    window_days: int = DEFAULT_STRATEGY_WINDOW_DAYS,
    series_ticker: str | None = None,
    strategy_name: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    if window_days not in STRATEGY_WINDOW_OPTIONS:
        window_days = DEFAULT_STRATEGY_WINDOW_DAYS

    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        strategies = await repo.list_strategies(active_only=True)
        assignments = await repo.list_city_strategy_assignments()
        regression_checkpoint = await repo.get_checkpoint("strategy_regression")
        strategy_events_raw = await repo.list_ops_events(
            limit=STRATEGY_EVENT_LIMIT,
            sources=["strategy_regression", "strategy_eval", STRATEGY_APPROVAL_SOURCE],
            created_after=now - timedelta(days=STRATEGY_EVENT_LOOKBACK_DAYS),
        )

        snapshot_meta = {
            "rooms_scanned": 0,
            "series_evaluated": 0,
            "source_mode": "stored_snapshot",
        }
        strategies_by_id = {strategy.id: strategy for strategy in strategies}
        if window_days == DEFAULT_STRATEGY_WINDOW_DAYS:
            latest_results = await repo.get_latest_strategy_results()
            normalized_rows = _normalize_strategy_result_rows(latest_results, strategies_by_id)
            if regression_checkpoint is not None and isinstance(regression_checkpoint.payload, dict):
                snapshot_meta["rooms_scanned"] = int(regression_checkpoint.payload.get("rooms_scanned") or 0)
                snapshot_meta["series_evaluated"] = int(regression_checkpoint.payload.get("series_evaluated") or 0)
        else:
            date_from = now - timedelta(days=window_days)
            rooms = await repo.get_strategy_regression_rooms(date_from, now)
            normalized_rows, live_meta = _evaluate_strategy_city_rows(
                strategies=strategies,
                rooms=rooms,
                container=container,
                run_at=now,
                date_from=date_from,
                date_to=now,
            )
            snapshot_meta.update(live_meta)
            snapshot_meta["source_mode"] = "live_eval"

        series_metadata = _series_metadata_index(container)
        leaderboard, city_matrix, leaderboard_context = _build_strategy_research_sections(
            strategies=strategies,
            normalized_rows=normalized_rows,
            assignments=assignments,
            series_metadata=series_metadata,
            selected_series_ticker=series_ticker,
            selected_strategy_name=strategy_name,
            window_days=window_days,
        )
        strategy_events = [_strategy_event_view(event) for event in strategy_events_raw]

        best_strategy = leaderboard_context["best_strategy"]
        selected_city = next((row for row in city_matrix if row["series_ticker"] == series_ticker), None)
        selected_strategy = next((row for row in leaderboard if row["name"] == strategy_name), None)

        history_rows: list[dict[str, Any]] = []
        if selected_city is not None:
            history = await repo.list_strategy_results_history(
                series_ticker=selected_city["series_ticker"],
                limit=STRATEGY_RESULT_HISTORY_LIMIT,
            )
            history_rows = _normalize_strategy_result_rows(history, strategies_by_id)
            detail_context = _city_detail_context(
                selected_city=selected_city,
                strategies=strategies,
                strategy_lookup=leaderboard_context["strategy_lookup"],
                history_rows=history_rows,
                strategy_events=strategy_events,
            )
        else:
            if selected_strategy is None:
                selected_strategy = best_strategy
            if selected_strategy is not None:
                history = await repo.list_strategy_results_history(
                    strategy_ids=[strategy_lookup.id for strategy_lookup in strategies if strategy_lookup.name == selected_strategy["name"]],
                    limit=STRATEGY_RESULT_HISTORY_LIMIT,
                )
                history_rows = _normalize_strategy_result_rows(history, strategies_by_id)
                detail_context = _strategy_detail_context(
                    selected_strategy=selected_strategy,
                    city_rows=city_matrix,
                    strategy_lookup=leaderboard_context["strategy_lookup"],
                    history_rows=history_rows,
                    strategy_events=strategy_events,
                )
            else:
                detail_context = {
                    "type": "empty",
                    "selected_series_ticker": None,
                    "selected_strategy_name": None,
                    "message": "No strategy data available yet.",
                }

        await session.commit()

    recent_promotions = [
        event for event in strategy_events if event["kind"] in {"promotion", "threshold_adjustment", "assignment_approval"}
    ][:10]
    if not any(event["kind"] == "promotion" for event in recent_promotions):
        recent_promotions = (_checkpoint_promotion_views(regression_checkpoint) + recent_promotions)[:10]

    configured_series = set(series_metadata) or {row["series_ticker"] for row in city_matrix}
    assigned_series = {assignment.series_ticker for assignment in assignments if assignment.series_ticker in configured_series}
    last_regression_run = None
    if regression_checkpoint is not None and isinstance(regression_checkpoint.payload, dict):
        last_regression_run = regression_checkpoint.payload.get("ran_at")

    methodology_points = [
        "Canonical replay outcomes come from persisted trade tickets and settlement labels, not raw signal payloads.",
        f"Default view uses a rolling {DEFAULT_STRATEGY_WINDOW_DAYS}d regression snapshot.",
        "Regression stays recommendation-only; auto-assignment remains paused during calibration.",
        "Manual approval is available only for the latest 180d strong or lean recommendation, and it always requires an operator note.",
        "Recommendation tiers require resolved-trade evidence, strong outcome coverage, and a measurable gap to the runner-up.",
        "Resolved-contract, longshot, and effectively-broken-book stand-down cases are excluded from regression.",
        "Missing data means not enough evidence, not that a strategy failed.",
    ]
    recommendation_counts = leaderboard_context.get("recommendation_counts") or {}

    summary = {
        "window_days": window_days,
        "window_display": _strategy_window_display(window_days),
        "window_options": list(STRATEGY_WINDOW_OPTIONS),
        "source_mode": snapshot_meta["source_mode"],
        "recommendation_mode": STRATEGY_RECOMMENDATION_MODE,
        "manual_approval_enabled": True,
        "approval_window_days": STRATEGY_APPROVAL_WINDOW_DAYS,
        "last_regression_run": last_regression_run,
        "rooms_scanned": snapshot_meta["rooms_scanned"],
        "rooms_scanned_display": _compact_number(snapshot_meta["rooms_scanned"]),
        "cities_evaluated": snapshot_meta["series_evaluated"] or len(city_matrix),
        "cities_evaluated_display": _compact_number(
            snapshot_meta["series_evaluated"] or len(city_matrix)
        ),
        "best_strategy_name": best_strategy["name"] if best_strategy is not None else "—",
        "best_strategy_win_rate": best_strategy["overall_win_rate"] if best_strategy is not None else None,
        "best_strategy_win_rate_display": best_strategy["overall_win_rate_display"] if best_strategy is not None else "—",
        "strong_recommendations_count": int(recommendation_counts.get("strong_recommendation") or 0),
        "lean_recommendations_count": int(recommendation_counts.get("lean_recommendation") or 0),
        "recent_promotions_count": sum(1 for event in strategy_events if event["kind"] == "promotion"),
        "recent_approvals_count": sum(1 for event in strategy_events if event["kind"] == "assignment_approval"),
        "assignments_covered": len(assigned_series),
        "assignments_total": len(configured_series),
        "assignments_covered_display": f"{len(assigned_series)} / {len(configured_series) if configured_series else 0}",
        "methodology_note": "Canonical outcomes, manual approval",
    }

    return {
        "summary": summary,
        "leaderboard": leaderboard,
        "city_matrix": city_matrix,
        "detail_context": detail_context,
        "recent_promotions": recent_promotions,
        "methodology": {
            "title": "How to read this tab",
            "points": methodology_points,
            "window_default_days": DEFAULT_STRATEGY_WINDOW_DAYS,
            "recommendation_trade_threshold": STRATEGY_MIN_TRADE_COUNT,
            "recommendation_outcome_coverage_threshold": STRATEGY_MIN_OUTCOME_COVERAGE_RATE,
            "recommendation_lean_gap_threshold": STRATEGY_LEAN_RECOMMENDATION_GAP,
            "recommendation_strong_gap_threshold": STRATEGY_STRONG_RECOMMENDATION_GAP,
            "promotion_trade_threshold": STRATEGY_MIN_TRADE_COUNT,
            "promotion_gap_threshold": STRATEGY_STRONG_RECOMMENDATION_GAP,
        },
    }
