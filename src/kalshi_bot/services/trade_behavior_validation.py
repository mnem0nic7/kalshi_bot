from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import OrderRecord, Room, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.trade_behavior import production_entry_freeze_enabled


LIVE_BUY_TICKET_STATUSES = {"approved", "submitted", "resting", "filled", "executed"}
LIVE_BUY_ORDER_STATUSES = {"accepted", "open", "submitted", "resting", "filled", "executed"}


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


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _empirical_gate_readiness(
    *,
    settings: Settings,
    kalshi_env: str,
    freeze_enabled: bool,
    audit: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = dict(audit.get("lifecycle") or {})
    rows_by_key: dict[str, dict[str, Any]] = {}
    for row in list(lifecycle.get("worst_buckets") or []) + list(lifecycle.get("best_buckets") or []):
        if isinstance(row, dict) and row.get("bucket_key"):
            rows_by_key[str(row["bucket_key"])] = row
    min_samples = int(settings.trade_behavior_empirical_gate_min_settled_fills)
    min_net = Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars))
    under_sampled = 0
    negative = 0
    eligible = 0
    for row in rows_by_key.values():
        sample_count = int(row.get("bucket_sample_count") or 0)
        net = _decimal_or_none(row.get("bucket_net_pnl") or row.get("lifecycle_net_pnl"))
        if sample_count < min_samples:
            under_sampled += 1
        if net is not None and net <= min_net:
            negative += 1
        if sample_count >= min_samples and net is not None and net > min_net:
            eligible += 1

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
    now: datetime | None = None,
) -> dict[str, Any]:
    now_utc = _as_utc(now) or datetime.now(UTC)
    since = now_utc - timedelta(hours=max(1, int(since_hours)))
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        control = await repo.get_deployment_control(kalshi_env=kalshi_env)
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
    if current_defects:
        issues.append(_issue("warn", "analysis:data_defects", "Trade-analysis data defects remain."))

    status = "pass"
    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif issues:
        status = "warn"

    return {
        "status": status,
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
    lines = [
        "Trade Behavior Validation",
        f"env={report.get('kalshi_env')} status={str(report.get('status')).upper()} window={report.get('window_days')}d since={report.get('since_hours')}h",
        "",
        (
            "Freeze: "
            f"enabled={freeze.get('production_entry_freeze_enabled')} "
            f"reason={freeze.get('reason')} floor={freeze.get('min_edge_floor_bps')}bps"
        ),
        f"Buy-entry bypass: tickets={bypass.get('ticket_count', 0)} orders={bypass.get('order_count', 0)}",
        (
            "Empirical gate: "
            f"status={gate_readiness.get('status')} "
            f"eligible_buckets={gate_readiness.get('eligible_reported_bucket_count')} "
            f"negative_buckets={gate_readiness.get('negative_reported_bucket_count')} "
            f"under_sampled={gate_readiness.get('under_sampled_reported_bucket_count')}"
        ),
        f"Audit: issues={audit.get('issue_count', 0)} critical={audit.get('critical_count', 0)} high={audit.get('high_count', 0)}",
        (
            "Analysis: "
            f"eligible={analysis.get('training_eligible_count')} "
            f"excluded={analysis.get('excluded_count')} "
            f"defects={analysis.get('data_defect_count')}"
        ),
    ]
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
