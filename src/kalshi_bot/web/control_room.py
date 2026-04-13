from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import logging
from typing import TYPE_CHECKING, Any

from kalshi_bot.core.enums import RoomOrigin
from kalshi_bot.db.repositories import PlatformRepository

if TYPE_CHECKING:
    from kalshi_bot.services.container import AppContainer


logger = logging.getLogger(__name__)

CONTROL_ROOM_TABS = ("overview", "training", "research", "rooms", "operations")
SUMMARY_ROOM_WINDOW_HOURS = 24
SUMMARY_ROOM_LIMIT = 60
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
    }


def _positions_summary(positions: list[Any]) -> dict[str, Any]:
    total_contracts = sum(abs(_decimal_or_zero(position.count_fp)) for position in positions)
    return {
        "count": len(positions),
        "total_contracts": str(total_contracts.quantize(Decimal("0.01"))) if positions else "0.00",
        "has_pnl_summary": False,
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
        "mode": dossier.get("mode"),
        "source_coverage": summary.get("source_coverage"),
        "refreshed_at": freshness.get("refreshed_at"),
        "expires_at": freshness.get("expires_at"),
        "has_dossier": bool(dossier),
        "json_url": f"/api/research/{item['market_ticker']}",
    }


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


async def _recent_room_bundles(container: AppContainer, *, limit: int) -> list[Any]:
    return await container.training_export_service.export_room_bundles(
        limit=limit,
        include_non_complete=True,
        origins=[RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value],
    )


def _summary_payload(
    *,
    now: datetime,
    control: Any,
    runtime_health: dict[str, Any],
    positions: list[Any],
    training_status: dict[str, Any],
    configured_markets: list[dict[str, Any]],
    room_bundles: list[Any],
) -> dict[str, Any]:
    room_views = [_room_view(bundle) for bundle in room_bundles]
    research_views = [_research_market_view(item) for item in configured_markets]
    research_confidences = [item["confidence"] for item in research_views if item["confidence"] is not None]
    quality_debt = dict(training_status.get("quality_debt_summary") or {})
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
        "research_confidence": {
            "average": round(sum(research_confidences) / len(research_confidences), 2) if research_confidences else None,
            "count": len(research_confidences),
            "sparkline": research_confidences[-12:],
        },
        "room_outcomes": room_outcomes,
        "quality_debt": {
            "total": int(
                quality_debt.get("stale_mismatch_count", 0)
                + quality_debt.get("missed_stand_down_count", 0)
                + quality_debt.get("weak_resolved_trade_count", 0)
            ),
            "stale_mismatch_count": int(quality_debt.get("stale_mismatch_count", 0)),
            "missed_stand_down_count": int(quality_debt.get("missed_stand_down_count", 0)),
            "weak_resolved_trade_count": int(quality_debt.get("weak_resolved_trade_count", 0)),
            "recent_stale_mismatch_count": int(quality_debt.get("recent_stale_mismatch_count", 0)),
            "recent_missed_stand_down_count": int(quality_debt.get("recent_missed_stand_down_count", 0)),
            "cleaned_trainable_room_count": int(quality_debt.get("cleaned_trainable_room_count", 0)),
        },
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
        await session.commit()

    configured_markets, room_bundles = await asyncio.gather(
        _configured_markets(container),
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    return _summary_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        positions=positions,
        training_status=training_status,
        configured_markets=configured_markets,
        room_bundles=room_bundles,
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

    configured_markets, room_bundles, self_improve_status, heuristic_status = await asyncio.gather(
        _configured_markets(container),
        _recent_room_bundles(container, limit=SUMMARY_ROOM_LIMIT),
        container.self_improve_service.get_dashboard_status(),
        container.historical_intelligence_service.get_dashboard_status(),
    )
    training_status = await container.training_corpus_service.get_dashboard_status(bundles=room_bundles)
    summary = _summary_payload(
        now=now,
        control=control,
        runtime_health=runtime_health,
        positions=positions,
        training_status=training_status,
        configured_markets=configured_markets,
        room_bundles=room_bundles,
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
                "replay_corpus": historical_status.get("replay_corpus") or {},
                "coverage_repair_summary": historical_status.get("coverage_repair_summary") or {},
                "checkpoint_archive_promotion_count": historical_status.get("checkpoint_archive_promotion_count") or 0,
            },
            "samples": {
                "source_replay_coverage": (historical_status.get("source_replay_coverage") or {}).get("market_day_coverage") or [],
                "checkpoint_archive_coverage": (historical_status.get("checkpoint_archive_coverage") or {}).get("market_day_coverage") or [],
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
        "markets": market_views,
    }


async def _build_rooms_tab(container: AppContainer) -> dict[str, Any]:
    now = datetime.now(UTC)
    room_bundles, configured_markets = await asyncio.gather(
        _recent_room_bundles(container, limit=ROOM_TAB_LIMIT),
        _configured_markets(container),
    )
    room_views = [_room_view(bundle) for bundle in room_bundles]
    return {
        "tab": "rooms",
        "as_of": now.isoformat(),
        "rooms": room_views,
        "room_outcomes": _recent_room_outcomes(room_views, now=now),
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
