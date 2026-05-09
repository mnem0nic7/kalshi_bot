from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Checkpoint, FillRecord, OpsEvent, OrderRecord, RiskVerdictRecord, Room, Signal, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.trade_behavior_quality import annotate_bucket_matrix, bucket_status_summary
from kalshi_bot.services.trade_behavior import production_entry_freeze_enabled


LIVE_BUY_TICKET_STATUSES = {"approved", "submitted", "resting", "filled", "executed"}
LIVE_BUY_ORDER_STATUSES = {"accepted", "open", "submitted", "resting", "filled", "executed"}
FAILED_ORDER_STATUSES = {"failed", "rejected", "order_id_missing", "lock_denied", "write_credentials_missing"}
NOISY_OPS_SEVERITIES = {"warning", "error", "critical"}


def _event_payload(event: OpsEvent) -> dict[str, Any]:
    return event.payload if isinstance(event.payload, dict) else {}


def _event_text(event: OpsEvent) -> str:
    return f"{event.summary or ''} {_event_payload(event)}".lower()


def _is_background_research_market_not_found(event: OpsEvent) -> bool:
    payload = _event_payload(event)
    if str(event.source or "").lower() != "research":
        return False
    if str(payload.get("trigger_reason") or "").lower() != "market_event":
        return False
    text = _event_text(event)
    status_code = payload.get("status_code")
    return status_code in (404, "404") or ("404" in text and "not found" in text)


def _is_watchdog_heartbeat_stale_recovery(event: OpsEvent) -> bool:
    if str(event.source or "").lower() != "watchdog":
        return False
    text = _event_text(event)
    return "watchdog requested recovery action" in text and "heartbeat stale" in text


def _is_transient_stream_reconnect(event: OpsEvent) -> bool:
    payload = _event_payload(event)
    if str(event.source or "").lower() != "stream":
        return False
    if payload.get("classification") == "transient_reconnect" or payload.get("transient") is True:
        return True
    error_type = str(payload.get("error_type") or "")
    if error_type != "ConnectionClosedError":
        return False
    message = str(payload.get("message") or "")
    text = f"{event.summary or ''} {message}".lower()
    return "keepalive ping timeout" in text and ("no close frame" in text or "1011" in text)


def _ignored_fast_gate_ops_category(event: OpsEvent) -> str | None:
    if _is_background_research_market_not_found(event):
        return "research_market_event_404"
    if _is_watchdog_heartbeat_stale_recovery(event):
        return "watchdog_heartbeat_stale_recovery"
    if _is_transient_stream_reconnect(event):
        return "stream_transient_reconnect"
    return None


def _batches(values: list[str], size: int = 500) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def _production_buy_entry_bypass(
    session,
    *,
    kalshi_env: str,
    since: datetime,
    limit: int = 20,
) -> dict[str, Any]:
    ticket_rows = list(
        (
            await session.execute(
                select(TradeTicketRecord, Room)
                .join(Room, TradeTicketRecord.room_id == Room.id)
                .where(
                    Room.kalshi_env == kalshi_env,
                    Room.shadow_mode.is_(False),
                    TradeTicketRecord.action == "buy",
                    TradeTicketRecord.status.in_(sorted(LIVE_BUY_TICKET_STATUSES)),
                    TradeTicketRecord.created_at >= since,
                )
                .order_by(TradeTicketRecord.created_at.desc())
                .limit(limit)
            )
        ).all()
    )
    order_rows = list(
        (
            await session.execute(
                select(OrderRecord)
                .where(
                    OrderRecord.kalshi_env == kalshi_env,
                    OrderRecord.action == "buy",
                    OrderRecord.status.in_(sorted(LIVE_BUY_ORDER_STATUSES)),
                    OrderRecord.created_at >= since,
                )
                .order_by(OrderRecord.created_at.desc())
                .limit(limit)
            )
        ).scalars()
    )
    return {
        "since": since.isoformat(),
        "ticket_count": len(ticket_rows),
        "order_count": len(order_rows),
        "tickets": [
            {
                "ticket_id": ticket.id,
                "room_id": room.id,
                "market_ticker": ticket.market_ticker,
                "status": ticket.status,
                "created_at": _as_utc(ticket.created_at).isoformat() if ticket.created_at else None,
            }
            for ticket, room in ticket_rows
        ],
        "orders": [
            {
                "order_id": order.id,
                "market_ticker": order.market_ticker,
                "status": order.status,
                "client_order_id": order.client_order_id,
                "created_at": _as_utc(order.created_at).isoformat() if order.created_at else None,
            }
            for order in order_rows
        ],
    }


async def _env_trading_activity_observed(
    session,
    *,
    kalshi_env: str,
    since: datetime,
) -> bool:
    room_rows = list(
        (
            await session.execute(
                select(Room)
                .where(Room.kalshi_env == kalshi_env, Room.created_at >= since)
                .limit(1)
            )
        ).scalars()
    )
    if room_rows:
        return True
    ticket_rows = list(
        (
            await session.execute(
                select(TradeTicketRecord, Room)
                .join(Room, TradeTicketRecord.room_id == Room.id)
                .where(Room.kalshi_env == kalshi_env, TradeTicketRecord.created_at >= since)
                .limit(1)
            )
        ).all()
    )
    if ticket_rows:
        return True
    order_rows = list(
        (
            await session.execute(
                select(OrderRecord)
                .where(OrderRecord.kalshi_env == kalshi_env, OrderRecord.created_at >= since)
                .limit(1)
            )
        ).scalars()
    )
    return bool(order_rows)


def _active_runtime_issue(watchdog_status: dict[str, Any]) -> dict[str, Any] | None:
    active = str(watchdog_status.get("active_color") or "")
    color = dict((watchdog_status.get("colors") or {}).get(active) or {})
    if not color:
        return {"severity": "fail", "code": "active_runtime_missing", "summary": "Active runtime color is missing from watchdog status."}
    if not bool(color.get("combined_healthy")):
        return {"severity": "fail", "code": "active_runtime_unhealthy", "summary": "Active runtime is not healthy."}
    return None


def _issue(severity: str, code: str, summary: str) -> dict[str, str]:
    return {"severity": severity, "code": code, "summary": summary}


def _report_status(issues: list[dict[str, Any]]) -> str:
    if any(issue.get("severity") == "fail" for issue in issues):
        return "fail"
    if issues:
        return "warn"
    return "pass"


def _order_failed(order: OrderRecord) -> bool:
    status = str(order.status or "").lower()
    return status in FAILED_ORDER_STATUSES or status.startswith("rejected_")


def _parse_checkpoint_time(checkpoint: Checkpoint | None) -> datetime | None:
    if checkpoint is None:
        return None
    payload = checkpoint.payload if isinstance(checkpoint.payload, dict) else {}
    raw = payload.get("reconciled_at") or payload.get("ran_at") or payload.get("updated_at")
    if not raw:
        return _as_utc(checkpoint.updated_at)
    try:
        return _as_utc(datetime.fromisoformat(str(raw)))
    except ValueError:
        return None


def _reconcile_evidence(checkpoint: Checkpoint | None, *, now: datetime) -> dict[str, Any]:
    reconciled_at = _parse_checkpoint_time(checkpoint)
    age_seconds = None
    if reconciled_at is not None:
        age_seconds = max(0, int((now - reconciled_at).total_seconds()))
    return {
        "stream_name": checkpoint.stream_name if checkpoint is not None else None,
        "present": checkpoint is not None,
        "reconciled_at": reconciled_at.isoformat() if reconciled_at is not None else None,
        "age_seconds": age_seconds,
    }


def _ops_summary(ops_events: list[OpsEvent]) -> dict[str, Any]:
    ignored_categories = Counter(
        category
        for event in ops_events
        if (category := _ignored_fast_gate_ops_category(event)) is not None
    )
    actionable_events = [
        event
        for event in ops_events
        if _ignored_fast_gate_ops_category(event) is None
    ]
    source_counts = Counter((str(event.severity), str(event.source)) for event in ops_events)
    actionable_source_counts = Counter((str(event.severity), str(event.source)) for event in actionable_events)
    stale_count = sum(
        1
        for event in actionable_events
        if "stale" in _event_text(event)
    )
    critical_events = [event for event in ops_events if str(event.severity).lower() == "critical"]
    noisy_sources = [
        {"severity": severity, "source": source, "count": count}
        for (severity, source), count in actionable_source_counts.most_common(20)
        if severity in NOISY_OPS_SEVERITIES and count >= 10
    ]
    return {
        "event_count": len(ops_events),
        "actionable_event_count": len(actionable_events),
        "ignored_benign_event_count": sum(ignored_categories.values()),
        "ignored_benign_event_counts": dict(ignored_categories),
        "stale_event_count": stale_count,
        "critical_event_count": len(critical_events),
        "top_sources": [
            {"severity": severity, "source": source, "count": count}
            for (severity, source), count in source_counts.most_common(20)
        ],
        "actionable_top_sources": [
            {"severity": severity, "source": source, "count": count}
            for (severity, source), count in actionable_source_counts.most_common(20)
        ],
        "noisy_sources": noisy_sources,
        "critical_events": [
            {
                "id": event.id,
                "source": event.source,
                "summary": event.summary,
                "created_at": _as_utc(event.created_at).isoformat() if event.created_at else None,
            }
            for event in critical_events[:20]
        ],
    }


def _position_discrepancy_evidence(ops_events: list[OpsEvent]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    for event in ops_events:
        payload = event.payload if isinstance(event.payload, dict) else {}
        text = f"{event.summary or ''} {payload}".lower()
        count = payload.get("discrepancy_count") or payload.get("position_discrepancy_count")
        flagged = "exchange_position_discrepancy" in text or "position discrepancy" in text
        if not flagged and count in (None, 0, "0"):
            continue
        evidence.append(
            {
                "id": event.id,
                "severity": event.severity,
                "source": event.source,
                "summary": event.summary,
                "discrepancy_count": count,
                "created_at": _as_utc(event.created_at).isoformat() if event.created_at else None,
            }
        )
    return {"discrepancy_count": len(evidence), "events": evidence[:20]}


def _fast_order_payload(order: OrderRecord) -> dict[str, Any]:
    return {
        "order_id": order.id,
        "client_order_id": order.client_order_id,
        "market_ticker": order.market_ticker,
        "status": order.status,
        "side": order.side,
        "action": order.action,
        "strategy_code": order.strategy_code,
        "created_at": _as_utc(order.created_at).isoformat() if order.created_at else None,
    }


def _empirical_gate_readiness(
    *,
    settings: Settings,
    kalshi_env: str,
    freeze_enabled: bool,
    audit: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = dict(audit.get("lifecycle") or {})
    rows_by_key: dict[str, dict[str, Any]] = {}
    source_rows = list(lifecycle.get("buckets") or [])
    if not source_rows:
        source_rows = list(lifecycle.get("worst_buckets") or []) + list(lifecycle.get("best_buckets") or [])
    for row in source_rows:
        if isinstance(row, dict) and row.get("bucket_key"):
            rows_by_key[str(row["bucket_key"])] = row
    annotated_rows = annotate_bucket_matrix(
        list(rows_by_key.values()),
        settings=settings,
        kalshi_env=kalshi_env,
        freeze_enabled=freeze_enabled,
    )
    summary = bucket_status_summary(annotated_rows)
    under_sampled = int(summary.get("under_sampled_count") or 0)
    negative = int(summary.get("negative_actual_pnl_count") or 0)
    eligible = int(summary.get("eligible_for_live_review_count") or 0)
    unscored = int(summary.get("unscored_count") or 0)

    if not settings.trade_behavior_empirical_gate_enabled:
        status = "disabled"
        reason = "empirical_gate_disabled"
    elif str(kalshi_env).lower() == "production" and freeze_enabled:
        status = "freeze_active"
        reason = settings.trade_behavior_entry_freeze_reason
    elif eligible:
        status = "ready_with_positive_sampled_buckets"
        reason = "some_reported_buckets_have_enough_positive_settled_evidence"
    else:
        status = "collecting_evidence"
        reason = "no_reported_bucket_has_enough_positive_settled_evidence"

    return {
        "status": status,
        "reason": reason,
        "reported_bucket_count": int(lifecycle.get("bucket_count") or len(rows_by_key)),
        "reported_bucket_rows_evaluated": len(rows_by_key),
        "eligible_reported_bucket_count": eligible,
        "negative_reported_bucket_count": negative,
        "under_sampled_reported_bucket_count": under_sampled,
        "unscored_reported_bucket_count": unscored,
        "evidence_status_counts": summary.get("evidence_status_counts") or {},
        "bucket_status_counts": summary.get("bucket_status_counts") or {},
        "bucket_matrix_sample": annotated_rows[:20],
    }


async def _build_fast_trade_behavior_validation_report(
    *,
    settings: Settings,
    session_factory: async_sessionmaker,
    watchdog_service: Any,
    kalshi_env: str,
    days: int,
    since_hours: int,
    now: datetime,
) -> dict[str, Any]:
    since = now - timedelta(hours=max(1, int(since_hours)))
    activity_cutoff = now - timedelta(days=max(1, int(days)))
    freeze_enabled = production_entry_freeze_enabled(settings, kalshi_env)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        watchdog_status = await watchdog_service.get_status(repo, kalshi_env=kalshi_env)
        control = await repo.get_deployment_control(kalshi_env=kalshi_env)
        active_color = str(getattr(control, "active_color", "") or settings.app_color)
        reconcile_checkpoint = await repo.get_checkpoint(f"daemon_reconcile:{kalshi_env}:{active_color}")
        buy_bypass = await _production_buy_entry_bypass(session, kalshi_env=kalshi_env, since=since)
        runtime_observed = await _env_trading_activity_observed(
            session,
            kalshi_env=kalshi_env,
            since=activity_cutoff,
        )

        rooms = list(
            (
                await session.execute(
                    select(Room)
                    .where(Room.kalshi_env == kalshi_env, Room.created_at >= since)
                    .order_by(Room.created_at.desc(), Room.id.desc())
                )
            ).scalars()
        )
        room_ids = [room.id for room in rooms]

        signals: list[Signal] = []
        tickets: list[TradeTicketRecord] = []
        risk_verdicts: list[RiskVerdictRecord] = []
        if room_ids:
            for batch in _batches(room_ids):
                signals.extend(
                    list((await session.execute(select(Signal).where(Signal.room_id.in_(batch)))).scalars())
                )
                tickets.extend(
                    list((await session.execute(select(TradeTicketRecord).where(TradeTicketRecord.room_id.in_(batch)))).scalars())
                )
                risk_verdicts.extend(
                    list((await session.execute(select(RiskVerdictRecord).where(RiskVerdictRecord.room_id.in_(batch)))).scalars())
                )

        orders = list(
            (
                await session.execute(
                    select(OrderRecord)
                    .where(OrderRecord.kalshi_env == kalshi_env, OrderRecord.created_at >= since)
                    .order_by(OrderRecord.created_at.desc(), OrderRecord.id.desc())
                )
            ).scalars()
        )
        fills = list(
            (
                await session.execute(
                    select(FillRecord)
                    .where(FillRecord.kalshi_env == kalshi_env, FillRecord.created_at >= since)
                    .order_by(FillRecord.created_at.desc(), FillRecord.id.desc())
                )
            ).scalars()
        )
        ops_events = list(
            (
                await session.execute(
                    select(OpsEvent)
                    .where(OpsEvent.kalshi_env == kalshi_env, OpsEvent.created_at >= since)
                    .order_by(OpsEvent.created_at.desc(), OpsEvent.id.desc())
                )
            ).scalars()
        )

    runtime_issue = _active_runtime_issue(watchdog_status)
    runtime_not_observed = kalshi_env == "production" and not runtime_observed
    failed_orders = [order for order in orders if _order_failed(order)]
    live_activity_blocked_by_safety = kalshi_env == "production" and (settings.app_shadow_mode or freeze_enabled)
    reconcile = _reconcile_evidence(reconcile_checkpoint, now=now)
    reconcile_age = reconcile.get("age_seconds")
    ops = _ops_summary(ops_events)
    discrepancy = _position_discrepancy_evidence(ops_events)

    issues: list[dict[str, Any]] = []
    if runtime_issue is not None and not runtime_not_observed:
        issues.append(runtime_issue)
    if kalshi_env == "production" and not freeze_enabled:
        issues.append(_issue("fail", "production_entry_freeze_disabled", "Production entry freeze is disabled."))
    if kalshi_env == "production" and (buy_bypass["ticket_count"] or buy_bypass["order_count"]):
        issues.append(_issue("fail", "production_buy_entry_bypass", "Production buy entries were observed after the validation cutoff."))
    if failed_orders:
        issues.append(_issue("fail", "fast_gate_failed_orders", "Failed or rejected orders were observed inside the fast validation window."))
    if live_activity_blocked_by_safety and (orders or fills):
        issues.append(_issue("fail", "fast_gate_unexpected_live_activity", "Live orders or fills were observed while production safety blockers are active."))
    if ops["critical_event_count"]:
        issues.append(_issue("fail", "fast_gate_critical_ops_events", "Critical ops events were observed inside the fast validation window."))
    if discrepancy["discrepancy_count"]:
        issues.append(_issue("fail", "exchange_position_discrepancy_evidence", "Exchange/local position discrepancy evidence was observed."))
    if not reconcile["present"]:
        issues.append(_issue("fail", "daemon_reconcile_missing", "No daemon reconcile checkpoint exists for the active production color."))
    elif reconcile_age is None:
        issues.append(_issue("fail", "daemon_reconcile_invalid", "Daemon reconcile checkpoint does not contain a usable timestamp."))
    elif reconcile_age > max(1, int(settings.daemon_reconcile_stale_kill_switch_seconds)):
        issues.append(_issue("fail", "daemon_reconcile_stale", "Daemon reconcile checkpoint is stale."))

    if ops["stale_event_count"]:
        issues.append(_issue("warn", "fast_gate_stale_data_noise", "Recent ops events include stale-data noise."))
    if ops["noisy_sources"]:
        issues.append(_issue("warn", "fast_gate_ops_warning_error_noise", "Recent warning/error event volume is elevated."))

    fast_gate = {
        "mode": "fast",
        "since": since.isoformat(),
        "counts": {
            "rooms": len(rooms),
            "signals": len(signals),
            "trade_tickets": len(tickets),
            "risk_verdicts": len(risk_verdicts),
            "orders": len(orders),
            "fills": len(fills),
            "ops_events": len(ops_events),
        },
        "room_stage_counts": dict(Counter(str(room.stage or "<unknown>") for room in rooms)),
        "room_origin_counts": dict(Counter(str(room.room_origin or "<unknown>") for room in rooms)),
        "shadow_mode_counts": dict(Counter("shadow" if room.shadow_mode else "live" for room in rooms)),
        "ticket_status_counts": dict(Counter(str(ticket.status or "<unknown>") for ticket in tickets)),
        "risk_status_counts": dict(Counter(str(verdict.status or "<unknown>") for verdict in risk_verdicts)),
        "order_status_counts": dict(Counter(str(order.status or "<unknown>") for order in orders)),
        "failed_orders": [_fast_order_payload(order) for order in failed_orders[:20]],
        "reconcile": reconcile,
        "ops": ops,
        "exchange_position_discrepancy": discrepancy,
    }

    return {
        "status": _report_status(issues),
        "mode": "fast",
        "kalshi_env": kalshi_env,
        "window_days": days,
        "since_hours": since_hours,
        "generated_at": now.isoformat(),
        "freeze": {
            "production_entry_freeze_enabled": freeze_enabled,
            "reason": settings.trade_behavior_entry_freeze_reason,
            "min_edge_floor_bps": settings.trade_behavior_freeze_min_edge_bps,
        },
        "empirical_gate": {
            "enabled": settings.trade_behavior_empirical_gate_enabled,
            "readiness": {
                "status": "not_evaluated_fast_mode",
                "reason": "fast_mode_uses_indexed_runtime_evidence_only",
            },
        },
        "runtime_health": watchdog_status,
        "runtime_observed": runtime_observed,
        "runtime_not_observed": runtime_not_observed,
        "buy_entry_bypass": buy_bypass,
        "audit": {
            "mode": "fast",
            "issue_count": None,
            "critical_count": None,
            "high_count": None,
            "issues": [],
            "worst_lifecycle_buckets": [],
        },
        "analysis": {
            "mode": "fast",
            "rows": None,
            "training_eligible_count": None,
            "excluded_count": None,
            "data_defect_count": None,
            "top_exclusion_reasons": [],
            "new_row_scoreability": {},
            "worst_buckets": [],
        },
        "fast_gate": fast_gate,
        "issues": issues,
    }


async def build_trade_behavior_validation_report(
    *,
    settings: Settings,
    session_factory: async_sessionmaker,
    watchdog_service: Any,
    trading_audit_service: Any,
    trade_analysis_service: Any,
    kalshi_env: str = "production",
    days: int = 7,
    since_hours: int = 24,
    mode: str = "detailed",
    now: datetime | None = None,
) -> dict[str, Any]:
    if mode not in {"fast", "detailed"}:
        raise ValueError("mode must be fast or detailed")
    now_utc = _as_utc(now) or datetime.now(UTC)
    if mode == "fast":
        return await _build_fast_trade_behavior_validation_report(
            settings=settings,
            session_factory=session_factory,
            watchdog_service=watchdog_service,
            kalshi_env=kalshi_env,
            days=days,
            since_hours=since_hours,
            now=now_utc,
        )

    since = now_utc - timedelta(hours=max(1, int(since_hours)))
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        watchdog_status = await watchdog_service.get_status(repo, kalshi_env=kalshi_env)
        buy_bypass = await _production_buy_entry_bypass(
            session,
            kalshi_env=kalshi_env,
            since=since,
        )
        runtime_observed = await _env_trading_activity_observed(
            session,
            kalshi_env=kalshi_env,
            since=now_utc - timedelta(days=max(1, int(days))),
        )

    audit = await trading_audit_service.build_report(kalshi_env=kalshi_env, days=days)
    analysis = await trade_analysis_service.build_report(kalshi_env=kalshi_env, days=days, buckets=True)
    freeze_enabled = production_entry_freeze_enabled(settings, kalshi_env)
    gate_readiness = _empirical_gate_readiness(
        settings=settings,
        kalshi_env=kalshi_env,
        freeze_enabled=freeze_enabled,
        audit=audit,
    )
    issues: list[dict[str, Any]] = []
    runtime_issue = _active_runtime_issue(watchdog_status)
    runtime_not_observed = kalshi_env == "production" and not runtime_observed
    if runtime_issue is not None and not runtime_not_observed:
        issues.append(runtime_issue)
    if kalshi_env == "production" and not freeze_enabled:
        issues.append(_issue("fail", "production_entry_freeze_disabled", "Production entry freeze is disabled."))
    if kalshi_env == "production" and (buy_bypass["ticket_count"] or buy_bypass["order_count"]):
        issues.append(_issue("fail", "production_buy_entry_bypass", "Production buy entries were observed after the validation cutoff."))

    audit_issues = list(audit.get("issues") or [])
    critical_audit_issues = [item for item in audit_issues if str(item.get("severity")).lower() == "critical"]
    high_audit_issues = [item for item in audit_issues if str(item.get("severity")).lower() == "high"]
    for item in critical_audit_issues:
        issues.append(_issue("fail", f"audit:{item.get('code')}", str(item.get("summary") or item.get("code"))))
    for item in high_audit_issues:
        issues.append(_issue("warn", f"audit:{item.get('code')}", str(item.get("summary") or item.get("code"))))

    exclusion_counts = dict(analysis.get("top_exclusion_reasons") or [])
    current_missing = int(
        analysis.get("current_missing_market_snapshot_count")
        if analysis.get("current_missing_market_snapshot_count") is not None
        else exclusion_counts.get("missing_market_snapshot", 0)
    )
    current_defects = int(
        analysis.get("current_data_defect_count")
        if analysis.get("current_data_defect_count") is not None
        else analysis.get("data_defect_count") or 0
    )
    if current_missing:
        issues.append(_issue("warn", "analysis:missing_market_snapshot", "Trade-analysis rows still lack recoverable market snapshots."))
    new_scoreability = dict(analysis.get("new_row_scoreability") or {})
    if int(new_scoreability.get("missing_market_snapshot_count") or 0):
        issues.append(_issue("warn", "analysis:new_rows_missing_market_snapshot", "New trade-analysis rows lack point-in-time market snapshots."))
    if current_defects:
        issues.append(_issue("warn", "analysis:data_defects", "Trade-analysis data defects remain."))

    status = "pass"
    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif issues:
        status = "warn"

    return {
        "status": status,
        "mode": "detailed",
        "kalshi_env": kalshi_env,
        "window_days": days,
        "since_hours": since_hours,
        "generated_at": now_utc.isoformat(),
        "freeze": {
            "production_entry_freeze_enabled": freeze_enabled,
            "reason": settings.trade_behavior_entry_freeze_reason,
            "min_edge_floor_bps": settings.trade_behavior_freeze_min_edge_bps,
        },
        "empirical_gate": {
            "enabled": settings.trade_behavior_empirical_gate_enabled,
            "min_settled_fills": settings.trade_behavior_empirical_gate_min_settled_fills,
            "min_net_pnl_dollars": settings.trade_behavior_empirical_gate_min_net_pnl_dollars,
            "lookback_days": settings.trade_behavior_empirical_gate_lookback_days,
            "readiness": gate_readiness,
        },
        "runtime_health": watchdog_status,
        "runtime_observed": runtime_observed,
        "runtime_not_observed": runtime_not_observed,
        "buy_entry_bypass": buy_bypass,
        "audit": {
            "issue_count": len(audit_issues),
            "critical_count": len(critical_audit_issues),
            "high_count": len(high_audit_issues),
            "issues": audit_issues[:10],
            "worst_lifecycle_buckets": (audit.get("lifecycle") or {}).get("worst_buckets", [])[:10],
        },
        "analysis": {
            "rows": analysis.get("row_count") or analysis.get("rows") or 0,
            "training_eligible_count": analysis.get("training_eligible_count"),
            "excluded_count": analysis.get("excluded_count"),
            "data_defect_count": analysis.get("data_defect_count"),
            "current_missing_market_snapshot_count": analysis.get("current_missing_market_snapshot_count"),
            "current_data_defect_count": analysis.get("current_data_defect_count"),
            "legacy_coverage_debt_count": analysis.get("legacy_coverage_debt_count"),
            "top_exclusion_reasons": analysis.get("top_exclusion_reasons") or [],
            "snapshot_source_counts": analysis.get("snapshot_source_counts") or {},
            "snapshot_source_kind_counts": analysis.get("snapshot_source_kind_counts") or {},
            "new_row_scoreability": new_scoreability,
            "worst_buckets": (analysis.get("buckets") or [])[:10],
        },
        "issues": issues,
    }


def format_trade_behavior_validation_report(report: dict[str, Any]) -> str:
    freeze = report.get("freeze") or {}
    empirical_gate = report.get("empirical_gate") or {}
    gate_readiness = empirical_gate.get("readiness") or {}
    audit = report.get("audit") or {}
    analysis = report.get("analysis") or {}
    bypass = report.get("buy_entry_bypass") or {}
    fast_gate = report.get("fast_gate") or {}
    fast_counts = fast_gate.get("counts") or {}
    lines = [
        "Trade Behavior Validation",
        (
            f"env={report.get('kalshi_env')} status={str(report.get('status')).upper()} "
            f"mode={report.get('mode', 'detailed')} window={report.get('window_days')}d since={report.get('since_hours')}h"
        ),
        "",
        (
            "Freeze: "
            f"enabled={freeze.get('production_entry_freeze_enabled')} "
            f"reason={freeze.get('reason')} floor={freeze.get('min_edge_floor_bps')}bps"
        ),
        f"Buy-entry bypass: tickets={bypass.get('ticket_count', 0)} orders={bypass.get('order_count', 0)}",
    ]
    if report.get("mode") == "fast":
        reconcile = fast_gate.get("reconcile") or {}
        ops = fast_gate.get("ops") or {}
        lines.extend(
            [
                (
                    "Fast gate: "
                    f"rooms={fast_counts.get('rooms', 0)} "
                    f"signals={fast_counts.get('signals', 0)} "
                    f"tickets={fast_counts.get('trade_tickets', 0)} "
                    f"orders={fast_counts.get('orders', 0)} "
                    f"fills={fast_counts.get('fills', 0)}"
                ),
                (
                    "Reconcile: "
                    f"present={reconcile.get('present')} "
                    f"age_seconds={reconcile.get('age_seconds')} "
                    f"stream={reconcile.get('stream_name')}"
                ),
                (
                    "Ops: "
                    f"events={ops.get('event_count', 0)} "
                    f"critical={ops.get('critical_event_count', 0)} "
                    f"stale={ops.get('stale_event_count', 0)}"
                ),
            ]
        )
    else:
        lines.extend(
            [
                (
                    "Empirical gate: "
                    f"status={gate_readiness.get('status')} "
                    f"eligible_buckets={gate_readiness.get('eligible_reported_bucket_count')} "
                    f"negative_buckets={gate_readiness.get('negative_reported_bucket_count')} "
                    f"under_sampled={gate_readiness.get('under_sampled_reported_bucket_count')} "
                    f"unscored={gate_readiness.get('unscored_reported_bucket_count')}"
                ),
                f"Audit: issues={audit.get('issue_count', 0)} critical={audit.get('critical_count', 0)} high={audit.get('high_count', 0)}",
                (
                    "Analysis: "
                    f"eligible={analysis.get('training_eligible_count')} "
                    f"excluded={analysis.get('excluded_count')} "
                    f"defects={analysis.get('data_defect_count')}"
                ),
            ]
        )
    if analysis.get("new_row_scoreability"):
        scoreability = analysis["new_row_scoreability"]
        lines.append(
            "New-row scoreability: "
            f"since={scoreability.get('since')} "
            f"rows={scoreability.get('row_count')} "
            f"missing_snapshots={scoreability.get('missing_market_snapshot_count')}"
        )
    if analysis.get("top_exclusion_reasons"):
        lines.extend(["", "Top exclusions:"])
        for reason, count in analysis["top_exclusion_reasons"][:5]:
            lines.append(f"- {count}: {reason}")
    if audit.get("worst_lifecycle_buckets"):
        lines.extend(["", "Worst lifecycle buckets:"])
        for row in audit["worst_lifecycle_buckets"][:5]:
            lines.append(f"- {row['bucket_key']}: net={row['lifecycle_net_pnl']} win_rate={row['bucket_win_rate']}")
    if report.get("issues"):
        lines.extend(["", "Validation issues:"])
        for issue in report["issues"][:10]:
            lines.append(f"- {str(issue.get('severity')).upper()} {issue.get('code')}: {issue.get('summary')}")
    else:
        lines.extend(["", "Validation issues: none"])
    return "\n".join(lines)
