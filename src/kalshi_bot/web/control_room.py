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
    if position_views is not None and all(item.get("unrealized_pnl_dollars") is not None for item in position_views):
        total_unrealized = sum(
            (_decimal_or_zero(item.get("unrealized_pnl_dollars")) for item in position_views),
            Decimal("0.00"),
        ).quantize(Decimal("0.01"))
    return {
        "count": len(positions),
        "total_contracts": str(total_contracts.quantize(Decimal("0.01"))) if positions else "0.00",
        "total_notional_dollars": str(total_notional.quantize(Decimal("0.01"))) if positions else "0.00",
        "total_unrealized_pnl_dollars": str(total_unrealized) if total_unrealized is not None else None,
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
        await session.commit()

    research_confidence, room_bundles, room_outcome_views, intel_board = await asyncio.gather(
        _research_confidence_summary(container),
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
        _recent_room_outcome_views(container, now=now),
        _current_intel_board(container),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    return _summary_payload(
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
    positions_summary = _positions_summary(positions, position_views)
    positions_summary["capital_buckets"] = _capital_bucket_summary(capital_buckets)
    return {
        "kalshi_env": kalshi_env,
        "as_of": now.isoformat(),
        "portfolio": _balance_summary(balance_checkpoint, position_views),
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
