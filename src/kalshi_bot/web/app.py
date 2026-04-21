from __future__ import annotations

import asyncio
from collections import Counter
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import AsyncIterator, TypeVar

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ValidationError

from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import (
    HeuristicPackPromoteRequest,
    HeuristicPackRollbackRequest,
    HistoricalDateRangeRequest,
    HistoricalIntelligenceRunRequest,
    HistoricalTrainingBuildRequest,
    RoomCreate,
    ShadowCampaignRequest,
    ShadowRunRequest,
    SelfImprovePromoteRequest,
    SelfImproveRollbackRequest,
    TrainingBuildRequest,
    TriggerRequest,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.control_room import (
    CONTROL_ROOM_TABS,
    DEFAULT_STRATEGY_WINDOW_DAYS,
    STRATEGY_WINDOW_OPTIONS,
    build_control_room_bootstrap,
    build_control_room_summary,
    build_control_room_tab,
    build_env_dashboard,
    build_strategies_dashboard,
)
from kalshi_bot.web.faq_content import FAQ_SECTIONS
from kalshi_bot.weather.scoring import extract_current_temp_f, extract_forecast_high_f

templates = Jinja2Templates(directory="src/kalshi_bot/web/templates")
ModelT = TypeVar("ModelT", bound=BaseModel)
logger = logging.getLogger(__name__)
ROOM_STAGE_FLOW = [
    "triggered",
    "researching",
    "posture",
    "proposing",
    "risk",
    "executing",
    "auditing",
    "memory",
]


def _enum_value(value):
    if isinstance(value, Enum):
        return value.value
    return value


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _stage_label(stage: str) -> str:
    return stage.replace("_", " ").title()


def _serialize_room(room) -> dict:
    return {
        "id": room.id,
        "name": room.name,
        "market_ticker": room.market_ticker,
        "room_origin": room.room_origin,
        "prompt": room.prompt,
        "kalshi_env": room.kalshi_env,
        "stage": room.stage,
        "active_color": room.active_color,
        "shadow_mode": room.shadow_mode,
        "kill_switch_enabled": room.kill_switch_enabled,
        "agent_pack_version": room.agent_pack_version,
        "evaluation_run_id": room.evaluation_run_id,
        "role_models": room.role_models or {},
        "created_at": room.created_at.isoformat(),
        "updated_at": room.updated_at.isoformat(),
    }


def _serialize_message(message) -> dict:
    return {
        "id": message.id,
        "room_id": message.room_id,
        "sequence": message.sequence,
        "role": _enum_value(message.role),
        "kind": _enum_value(message.kind),
        "stage": _enum_value(message.stage),
        "content": message.content,
        "payload": message.payload or {},
        "created_at": message.created_at.isoformat(),
    }


def _serialize_signal(signal) -> dict | None:
    if signal is None:
        return None
    return {
        "id": signal.id,
        "room_id": signal.room_id,
        "market_ticker": signal.market_ticker,
        "fair_yes_dollars": str(signal.fair_yes_dollars),
        "edge_bps": signal.edge_bps,
        "confidence": signal.confidence,
        "summary": signal.summary,
        "payload": signal.payload,
        "created_at": signal.created_at.isoformat(),
    }


def _serialize_trade_ticket(ticket) -> dict | None:
    if ticket is None:
        return None
    return {
        "id": ticket.id,
        "room_id": ticket.room_id,
        "message_id": ticket.message_id,
        "market_ticker": ticket.market_ticker,
        "action": ticket.action,
        "side": ticket.side,
        "yes_price_dollars": str(ticket.yes_price_dollars),
        "count_fp": str(ticket.count_fp),
        "time_in_force": ticket.time_in_force,
        "client_order_id": ticket.client_order_id,
        "status": ticket.status,
        "payload": ticket.payload,
        "created_at": ticket.created_at.isoformat(),
    }


def _serialize_risk_verdict(verdict) -> dict | None:
    if verdict is None:
        return None
    return {
        "id": verdict.id,
        "room_id": verdict.room_id,
        "ticket_id": verdict.ticket_id,
        "status": verdict.status,
        "reasons": verdict.reasons,
        "approved_notional_dollars": (
            str(verdict.approved_notional_dollars) if verdict.approved_notional_dollars is not None else None
        ),
        "approved_count_fp": str(verdict.approved_count_fp) if verdict.approved_count_fp is not None else None,
        "capital_bucket": (verdict.payload or {}).get("capital_bucket"),
        "bucket_limit_dollars": (verdict.payload or {}).get("bucket_limit_dollars"),
        "bucket_used_dollars_before": (verdict.payload or {}).get("bucket_used_dollars_before"),
        "bucket_used_dollars_after": (verdict.payload or {}).get("bucket_used_dollars_after"),
        "resized_by_bucket": bool((verdict.payload or {}).get("resized_by_bucket", False)),
        "payload": verdict.payload,
        "created_at": verdict.created_at.isoformat(),
    }


def _serialize_order(order) -> dict:
    return {
        "id": order.id,
        "trade_ticket_id": order.trade_ticket_id,
        "kalshi_order_id": order.kalshi_order_id,
        "client_order_id": order.client_order_id,
        "market_ticker": order.market_ticker,
        "status": order.status,
        "side": order.side,
        "action": order.action,
        "yes_price_dollars": str(order.yes_price_dollars),
        "count_fp": str(order.count_fp),
        "raw": order.raw,
        "created_at": order.created_at.isoformat(),
    }


def _serialize_fill(fill) -> dict:
    return {
        "id": fill.id,
        "order_id": fill.order_id,
        "trade_id": fill.trade_id,
        "market_ticker": fill.market_ticker,
        "side": fill.side,
        "action": fill.action,
        "yes_price_dollars": str(fill.yes_price_dollars),
        "count_fp": str(fill.count_fp),
        "is_taker": fill.is_taker,
        "raw": fill.raw,
        "created_at": fill.created_at.isoformat(),
    }


def _serialize_memory_note(note) -> dict | None:
    if note is None:
        return None
    return {
        "id": note.id,
        "room_id": note.room_id,
        "title": note.title,
        "summary": note.summary,
        "tags": note.tags,
        "linked_message_ids": note.linked_message_ids,
        "created_at": note.created_at.isoformat(),
    }


def _serialize_campaign(campaign) -> dict | None:
    if campaign is None:
        return None
    return {
        "id": campaign.id,
        "room_id": campaign.room_id,
        "campaign_id": campaign.campaign_id,
        "trigger_source": campaign.trigger_source,
        "city_bucket": campaign.city_bucket,
        "market_regime_bucket": campaign.market_regime_bucket,
        "difficulty_bucket": campaign.difficulty_bucket,
        "outcome_bucket": campaign.outcome_bucket,
        "dossier_artifact_id": campaign.dossier_artifact_id,
        "payload": campaign.payload,
        "created_at": campaign.created_at.isoformat(),
        "updated_at": campaign.updated_at.isoformat(),
    }


def _serialize_research_health(record) -> dict | None:
    if record is None:
        return None
    return {
        "room_id": record.room_id,
        "market_ticker": record.market_ticker,
        "dossier_status": record.dossier_status,
        "gate_passed": record.gate_passed,
        "valid_dossier": record.valid_dossier,
        "good_for_training": record.good_for_training,
        "quality_score": record.quality_score,
        "citation_coverage_score": record.citation_coverage_score,
        "settlement_clarity_score": record.settlement_clarity_score,
        "freshness_score": record.freshness_score,
        "contradiction_count": record.contradiction_count,
        "structured_completeness_score": record.structured_completeness_score,
        "fair_value_score": record.fair_value_score,
        "dossier_artifact_id": record.dossier_artifact_id,
        "payload": record.payload,
        "updated_at": record.updated_at.isoformat(),
    }


def _serialize_research_run(run) -> dict:
    return {
        "id": run.id,
        "market_ticker": run.market_ticker,
        "trigger_reason": run.trigger_reason,
        "status": run.status,
        "payload": run.payload,
        "error_text": run.error_text,
        "started_at": run.started_at.isoformat(),
        "finished_at": _isoformat(run.finished_at),
    }


def _latest_message_by_role(messages: list[dict], role: str) -> dict | None:
    for message in reversed(messages):
        if message["role"] == role:
            return message
    return None


def _build_stage_timeline(messages: list[dict], room_stage: str, room_updated_at: str) -> list[dict]:
    seen_at: dict[str, str] = {}
    for message in messages:
        stage = message.get("stage")
        if stage and stage not in seen_at:
            seen_at[stage] = message["created_at"]

    timeline_stages = list(ROOM_STAGE_FLOW)
    if room_stage in {"complete", "failed"}:
        timeline_stages.append(room_stage)

    current_index = timeline_stages.index(room_stage) if room_stage in timeline_stages else -1
    timeline: list[dict] = []
    for index, stage in enumerate(timeline_stages):
        if room_stage in {"complete", "failed"}:
            if stage == room_stage:
                status = "current"
            elif stage in seen_at:
                status = "complete"
            else:
                status = "pending"
        else:
            if stage == room_stage:
                status = "current"
            elif current_index >= 0 and index < current_index:
                status = "complete"
            else:
                status = "pending"
        timeline.append(
            {
                "stage": stage,
                "label": _stage_label(stage),
                "status": status,
                "at": seen_at.get(stage) or (room_updated_at if stage == room_stage else None),
            }
        )
    return timeline


def _source_summary(sources: list[dict]) -> dict:
    by_class = Counter(str(source.get("source_class") or "unknown") for source in sources)
    by_trust = Counter(str(source.get("trust_tier") or "unknown") for source in sources)
    return {
        "count": len(sources),
        "by_class": dict(by_class.most_common()),
        "by_trust": dict(by_trust.most_common()),
    }


def _pricing_summary(signal: dict | None, market_snapshot: dict | None, trade_ticket: dict | None) -> dict:
    market = (market_snapshot or {}).get("market", market_snapshot or {})
    return {
        "yes_bid_dollars": market.get("yes_bid_dollars"),
        "yes_ask_dollars": market.get("yes_ask_dollars"),
        "no_ask_dollars": market.get("no_ask_dollars"),
        "last_price_dollars": market.get("last_price_dollars"),
        "fair_yes_dollars": (signal or {}).get("fair_yes_dollars"),
        "edge_bps": (signal or {}).get("edge_bps"),
        "confidence": (signal or {}).get("confidence"),
        "ticket_yes_price_dollars": (trade_ticket or {}).get("yes_price_dollars"),
        "ticket_count_fp": (trade_ticket or {}).get("count_fp"),
    }


def _weather_summary(research_dossier: dict | None, weather_bundle: dict | None) -> dict:
    numeric_facts = ((research_dossier or {}).get("summary") or {}).get("current_numeric_facts") or {}
    mapping = (weather_bundle or {}).get("mapping") or {}
    forecast = extract_forecast_high_f((weather_bundle or {}).get("forecast", {}))
    current = extract_current_temp_f((weather_bundle or {}).get("observation", {}))
    return {
        "threshold_f": mapping.get("threshold_f") or numeric_facts.get("threshold_f"),
        "operator": mapping.get("operator"),
        "forecast_high_f": forecast if forecast is not None else numeric_facts.get("forecast_high_f"),
        "current_temp_f": current if current is not None else numeric_facts.get("current_temp_f"),
        "station_id": mapping.get("station_id"),
        "location_name": mapping.get("location_name"),
        "forecast_updated_at": ((weather_bundle or {}).get("forecast", {}).get("properties", {}).get("updated")),
        "observation_at": ((weather_bundle or {}).get("observation", {}).get("properties", {}).get("timestamp")),
    }


def _research_quality_summary(research_dossier: dict | None, research_health: dict | None) -> dict:
    quality = (research_dossier or {}).get("quality") or {}
    return {
        "overall_score": (research_health or {}).get("quality_score") or quality.get("overall_score"),
        "citation_coverage_score": (research_health or {}).get("citation_coverage_score") or quality.get("citation_coverage_score"),
        "settlement_clarity_score": (research_health or {}).get("settlement_clarity_score") or quality.get("settlement_clarity_score"),
        "freshness_score": (research_health or {}).get("freshness_score") or quality.get("freshness_score"),
        "structured_completeness_score": (
            (research_health or {}).get("structured_completeness_score") or quality.get("structured_completeness_score")
        ),
        "fair_value_score": (research_health or {}).get("fair_value_score") or quality.get("fair_value_score"),
        "contradiction_count": (research_health or {}).get("contradiction_count") or (research_dossier or {}).get("contradiction_count"),
        "unresolved_count": (research_dossier or {}).get("unresolved_count"),
        "issues": quality.get("issues") or [],
    }


def _decision_summary(
    room: dict,
    signal: dict | None,
    research_dossier: dict | None,
    trade_ticket: dict | None,
    risk_verdict: dict | None,
    orders: list[dict],
    fills: list[dict],
    messages: list[dict],
) -> dict:
    latest_order = orders[-1] if orders else None
    latest_fill = fills[-1] if fills else None
    latest_exec = _latest_message_by_role(messages, AgentRole.EXECUTION_CLERK.value)
    latest_auditor = _latest_message_by_role(messages, AgentRole.AUDITOR.value)
    latest_ops = _latest_message_by_role(messages, AgentRole.OPS_MONITOR.value)

    if latest_fill is not None:
        execution_status = "filled"
    elif latest_order is not None:
        execution_status = latest_order.get("status")
    elif latest_exec is not None:
        execution_status = (latest_exec.get("payload") or {}).get("status") or "recorded"
    elif risk_verdict is not None and risk_verdict.get("status") == "blocked":
        execution_status = "blocked"
    elif research_dossier is not None and not ((research_dossier.get("gate") or {}).get("passed", False)):
        execution_status = "research_blocked"
    elif trade_ticket is not None:
        execution_status = "pending"
    else:
        execution_status = "stand_down"

    signal_payload = (signal or {}).get("payload") or {}
    eligibility = signal_payload.get("eligibility") if isinstance(signal_payload, dict) else None
    blocked_by = None
    if ((research_dossier or {}).get("gate") or {}).get("passed") is False:
        blocked_by = "research_gate"
    elif isinstance(eligibility, dict) and eligibility.get("eligible") is False:
        blocked_by = "eligibility"
    elif (risk_verdict or {}).get("status") == "blocked":
        blocked_by = "risk"

    return {
        "room_stage": room.get("stage"),
        "research_gate_passed": ((research_dossier or {}).get("gate") or {}).get("passed"),
        "research_gate_reasons": ((research_dossier or {}).get("gate") or {}).get("reasons") or [],
        "trade_proposed": trade_ticket is not None,
        "resolution_state": signal_payload.get("resolution_state") if isinstance(signal_payload, dict) else None,
        "strategy_mode": signal_payload.get("strategy_mode") if isinstance(signal_payload, dict) else None,
        "heuristic_pack_version": signal_payload.get("heuristic_pack_version") if isinstance(signal_payload, dict) else None,
        "intelligence_run_id": signal_payload.get("intelligence_run_id") if isinstance(signal_payload, dict) else None,
        "candidate_pack_id": signal_payload.get("candidate_pack_id") if isinstance(signal_payload, dict) else None,
        "rule_trace": signal_payload.get("rule_trace") if isinstance(signal_payload, dict) else [],
        "eligibility": eligibility,
        "stand_down_reason": signal_payload.get("stand_down_reason") if isinstance(signal_payload, dict) else None,
        "blocked_by": blocked_by,
        "risk_status": (risk_verdict or {}).get("status"),
        "execution_status": execution_status,
        "order_count": len(orders),
        "fill_count": len(fills),
        "latest_order_status": latest_order.get("status") if latest_order is not None else None,
        "latest_fill_price": latest_fill.get("yes_price_dollars") if latest_fill is not None else None,
        "audit_rationale_ids": ((latest_auditor or {}).get("payload") or {}).get("rationale_ids") or [],
        "latest_ops_summary": latest_ops.get("content") if latest_ops is not None else None,
    }


async def _load_room_snapshot(app_container: AppContainer, room_id: str, *, include_messages: bool = False) -> dict:
    async with app_container.session_factory() as session:
        repo = PlatformRepository(session)
        room = await repo.get_room(room_id)
        if room is None:
            raise KeyError(room_id)
        messages = await repo.list_messages(room_id)
        signal = await repo.get_latest_signal_for_room(room_id)
        trade_ticket = await repo.get_latest_trade_ticket_for_room(room_id)
        risk_verdict = await repo.get_latest_risk_verdict_for_room(room_id)
        orders = await repo.list_orders_for_room(room_id)
        fills = await repo.list_fills_for_room(room_id)
        memory_note = await repo.get_latest_memory_note_for_room(room_id)
        campaign = await repo.get_room_campaign(room_id)
        research_health = await repo.get_room_research_health(room_id)
        strategy_audit = await repo.get_room_strategy_audit(room_id)
        dossier_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_dossier_snapshot")
        delta_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_delta")
        market_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="market_snapshot")
        weather_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="weather_bundle")
        source_artifacts = await repo.list_artifacts(room_id=room_id, artifact_type="research_source", limit=24)
        latest_dossier = await repo.get_research_dossier(room.market_ticker)
        research_runs = await repo.list_research_runs(market_ticker=room.market_ticker, limit=8)
        await session.commit()

    serialized_room = _serialize_room(room)
    serialized_messages = [_serialize_message(message) for message in messages]
    serialized_signal = _serialize_signal(signal)
    serialized_ticket = _serialize_trade_ticket(trade_ticket)
    serialized_verdict = _serialize_risk_verdict(risk_verdict)
    serialized_orders = [_serialize_order(order) for order in orders]
    serialized_fills = [_serialize_fill(fill) for fill in fills]
    serialized_memory = _serialize_memory_note(memory_note)
    serialized_campaign = _serialize_campaign(campaign)
    serialized_research_health = _serialize_research_health(research_health)
    serialized_strategy_audit = dict(strategy_audit.payload or {}) if strategy_audit is not None else None
    serialized_sources = [artifact.payload for artifact in source_artifacts]
    research_dossier = (
        dossier_artifact.payload
        if dossier_artifact is not None
        else latest_dossier.payload if latest_dossier is not None else None
    )
    research_delta = delta_artifact.payload if delta_artifact is not None else None
    market_snapshot = market_artifact.payload if market_artifact is not None else None
    weather_bundle = weather_artifact.payload if weather_artifact is not None else None

    snapshot = {
        "room": serialized_room,
        "stage_timeline": _build_stage_timeline(serialized_messages, serialized_room["stage"], serialized_room["updated_at"]),
        "signal": serialized_signal,
        "trade_ticket": serialized_ticket,
        "risk_verdict": serialized_verdict,
        "orders": serialized_orders,
        "fills": serialized_fills,
        "memory_note": serialized_memory,
        "campaign": serialized_campaign,
        "research_health": serialized_research_health,
        "strategy_audit": serialized_strategy_audit,
        "research_dossier": research_dossier,
        "research_delta": research_delta,
        "research_sources": serialized_sources,
        "research_runs": [_serialize_research_run(run) for run in research_runs],
        "market_snapshot": market_snapshot,
        "weather_bundle": weather_bundle,
        "analytics": {
            "pricing": _pricing_summary(serialized_signal, market_snapshot, serialized_ticket),
            "weather": _weather_summary(research_dossier, weather_bundle),
            "research_quality": _research_quality_summary(research_dossier, serialized_research_health),
            "decision": _decision_summary(
                serialized_room,
                serialized_signal,
                research_dossier,
                serialized_ticket,
                serialized_verdict,
                serialized_orders,
                serialized_fills,
                serialized_messages,
            ),
            "source_summary": _source_summary(serialized_sources),
        },
    }
    if include_messages:
        snapshot["messages"] = serialized_messages
    return snapshot


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await AppContainer.build()
    app.state.container = container
    try:
        yield
    finally:
        await container.close()


def create_app() -> FastAPI:
    app = FastAPI(title="Kalshi Bot Control Room", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory="src/kalshi_bot/web/static"), name="static")

    def container(request: Request) -> AppContainer:
        return request.app.state.container

    async def parse_json_model(
        request: Request,
        model_cls: type[ModelT],
        *,
        default_on_empty: bool = False,
    ) -> ModelT:
        raw_body = await request.body()
        if not raw_body.strip():
            if default_on_empty:
                return model_cls()
            raise HTTPException(status_code=400, detail="Request body is required")
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Malformed JSON body") from exc
        try:
            return model_cls.model_validate(payload)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors(include_url=False)) from exc

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            await session.commit()
        return JSONResponse({"status": "ready", "active_color": control.active_color, "kill_switch": control.kill_switch_enabled})

    @app.get("/api/status")
    async def status(request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            positions = await repo.list_positions(limit=10)
            ops_events = await repo.list_ops_events(limit=10)
            dossiers = [
                {"market_ticker": room.market_ticker, "stage": room.stage}
                for room in await repo.list_rooms(limit=10, origins=["shadow", "live"])
            ]
            runtime_health = await app_container.watchdog_service.get_status(repo)
            await session.commit()
        training_status_payload = await app_container.training_corpus_service.get_status(persist_readiness=False)
        historical_status_payload = await app_container.historical_training_service.get_status()
        heuristic_status_payload = await app_container.historical_intelligence_service.get_status()
        training_status_payload["historical"] = historical_status_payload
        return JSONResponse(
            {
                "active_color": control.active_color,
                "kill_switch_enabled": control.kill_switch_enabled,
                "execution_lock_holder": control.execution_lock_holder,
                "self_improve": dict(control.notes.get("agent_packs") or {}),
                "training": training_status_payload,
                "heuristics": heuristic_status_payload,
                "runtime_health": runtime_health,
                "rooms": dossiers,
                "positions": [
                    {
                        "market_ticker": position.market_ticker,
                        "side": position.side,
                        "count_fp": str(position.count_fp),
                        "average_price_dollars": str(position.average_price_dollars),
                    }
                    for position in positions
                ],
                "ops_events": [
                    {
                        "severity": event.severity,
                        "summary": event.summary,
                        "source": event.source,
                    }
                    for event in ops_events
                ],
            }
        )

    @app.get("/api/control-room/summary")
    async def control_room_summary(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await build_control_room_summary(app_container)
        return JSONResponse(jsonable_encoder(payload))

    @app.get("/api/control-room/tab/{tab_name}")
    async def control_room_tab(tab_name: str, request: Request) -> JSONResponse:
        if tab_name not in CONTROL_ROOM_TABS:
            raise HTTPException(status_code=404, detail="Unknown control room tab")
        app_container = container(request)
        payload = await build_control_room_tab(app_container, tab_name)
        return JSONResponse(jsonable_encoder(payload))

    @app.get("/api/research/{market_ticker}")
    async def research_dossier(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        dossier = await app_container.research_coordinator.get_latest_dossier(market_ticker)
        if dossier is None:
            raise HTTPException(status_code=404, detail="Research dossier not found")
        return JSONResponse(dossier.model_dump(mode="json"))

    @app.get("/api/research/{market_ticker}/history")
    async def research_history(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        runs = await app_container.research_coordinator.list_recent_runs(market_ticker, limit=10)
        return JSONResponse({"market_ticker": market_ticker, "runs": runs})

    @app.post("/api/research/{market_ticker}/refresh")
    async def refresh_research(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        asyncio.create_task(
            app_container.research_coordinator.refresh_market_dossier(
                market_ticker,
                trigger_reason="api_refresh",
                force=True,
            )
        )
        return JSONResponse({"status": "scheduled", "market_ticker": market_ticker})

    @app.get("/api/self-improve/status")
    async def self_improve_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.self_improve_service.get_status())

    @app.get("/api/training/status")
    async def training_status(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await app_container.training_corpus_service.get_status(persist_readiness=True)
        payload["historical"] = await app_container.historical_training_service.get_status()
        payload["heuristics"] = await app_container.historical_intelligence_service.get_status()
        return JSONResponse(payload)

    @app.post("/api/training/build")
    async def training_build(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, TrainingBuildRequest, default_on_empty=True)
        return JSONResponse(await app_container.training_corpus_service.build_dataset(payload))

    @app.get("/api/historical/status")
    async def historical_status(request: Request, verbose: bool = False) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_training_service.get_status(verbose=verbose))

    @app.get("/api/historical/pipeline/status")
    async def historical_pipeline_status(request: Request, verbose: bool = False) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_pipeline_service.status(verbose=verbose))

    @app.get("/api/historical/intelligence/status")
    async def historical_intelligence_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_intelligence_service.get_status())

    @app.post("/api/historical/intelligence/run")
    async def historical_intelligence_run(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalIntelligenceRunRequest)
        return JSONResponse(await app_container.historical_intelligence_service.run(payload))

    @app.get("/api/historical/intelligence/explain")
    async def historical_intelligence_explain(request: Request, series: str | None = None) -> JSONResponse:
        app_container = container(request)
        series_values = [item for item in (series or "").split(",") if item]
        return JSONResponse(await app_container.historical_intelligence_service.explain(series=series_values or None))

    @app.get("/api/heuristic-pack/status")
    async def heuristic_pack_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_intelligence_service.get_status())

    @app.post("/api/heuristic-pack/promote")
    async def heuristic_pack_promote(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HeuristicPackPromoteRequest, default_on_empty=True)
        return JSONResponse(
            await app_container.historical_intelligence_service.promote(
                candidate_version=payload.candidate_version,
                reason=payload.reason,
            )
        )

    @app.post("/api/heuristic-pack/rollback")
    async def heuristic_pack_rollback(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HeuristicPackRollbackRequest, default_on_empty=True)
        return JSONResponse(await app_container.historical_intelligence_service.rollback(reason=payload.reason))

    @app.post("/api/historical/import")
    async def historical_import(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalDateRangeRequest)
        result = await app_container.historical_training_service.import_weather_history(
            date_from=datetime.fromisoformat(payload.date_from).date(),
            date_to=datetime.fromisoformat(payload.date_to).date(),
            series=payload.series or None,
        )
        return JSONResponse(result)

    @app.post("/api/historical/replay")
    async def historical_replay(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalDateRangeRequest)
        result = await app_container.historical_training_service.replay_weather_history(
            date_from=datetime.fromisoformat(payload.date_from).date(),
            date_to=datetime.fromisoformat(payload.date_to).date(),
            series=payload.series or None,
        )
        return JSONResponse(result)

    @app.post("/api/training/historical/build")
    async def historical_training_build(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalTrainingBuildRequest)
        return JSONResponse(await app_container.historical_training_service.build_historical_dataset(payload))

    @app.get("/api/training/builds")
    async def training_builds(request: Request) -> JSONResponse:
        app_container = container(request)
        builds = await app_container.training_corpus_service.list_builds(limit=10)
        return JSONResponse({"builds": [build.model_dump(mode="json") for build in builds]})

    @app.get("/api/research-audit")
    async def research_audit_alias(request: Request) -> JSONResponse:
        app_container = container(request)
        issues = await app_container.training_corpus_service.research_audit(limit=50)
        return JSONResponse({"issues": [issue.model_dump(mode="json") for issue in issues]})

    @app.get("/api/research/audit")
    async def research_audit(request: Request) -> JSONResponse:
        app_container = container(request)
        issues = await app_container.training_corpus_service.research_audit(limit=50)
        return JSONResponse({"issues": [issue.model_dump(mode="json") for issue in issues]})

    @app.get("/api/strategy-audit/rooms/{room_id}")
    async def strategy_audit_room(room_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse((await app_container.training_corpus_service.strategy_audit_room(room_id)).model_dump(mode="json"))

    @app.get("/api/strategy-audit/summary")
    async def strategy_audit_summary(request: Request, days: int | None = None, limit: int = 100) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(
            (await app_container.training_corpus_service.strategy_audit_summary(days=days, limit=limit)).model_dump(mode="json")
        )

    @app.post("/api/shadow-campaign/run")
    async def shadow_campaign_run(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, ShadowCampaignRequest, default_on_empty=True)
        results = await app_container.shadow_campaign_service.run(payload)
        return JSONResponse(
            {
                "status": "completed",
                "count": len(results),
                "rooms": [
                    {
                        "room_id": result.room_id,
                        "market_ticker": result.market_ticker,
                        "redirect": f"/rooms/{result.room_id}",
                    }
                    for result in results
                ],
            }
        )

    @app.post("/api/self-improve/critique")
    async def self_improve_critique(request: Request) -> JSONResponse:
        app_container = container(request)
        result = await app_container.self_improve_service.critique_recent_rooms()
        return JSONResponse(result.payload)

    @app.post("/api/self-improve/eval/{candidate_version}")
    async def self_improve_eval(candidate_version: str, request: Request) -> JSONResponse:
        app_container = container(request)
        result = await app_container.self_improve_service.evaluate_candidate(candidate_version=candidate_version)
        return JSONResponse(result.payload)

    @app.post("/api/self-improve/promote")
    async def self_improve_promote(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, SelfImprovePromoteRequest)
        result = await app_container.self_improve_service.promote_candidate(
            evaluation_run_id=payload.evaluation_run_id,
            reason=payload.reason,
        )
        return JSONResponse(result.payload)

    @app.post("/api/self-improve/rollback")
    async def self_improve_rollback(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, SelfImproveRollbackRequest, default_on_empty=True)
        result = await app_container.self_improve_service.rollback(reason=payload.reason)
        return JSONResponse(result.payload)

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    @app.get("/faq", response_class=HTMLResponse)
    async def faq(request: Request) -> HTMLResponse:
        app_container = container(request)
        return templates.TemplateResponse(
            request,
            "faq.html",
            {
                "faq_sections": FAQ_SECTIONS,
                "settings": app_container.settings,
            },
        )

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        app_container = container(request)
        demo_data, prod_data, strategies_data = await asyncio.gather(
            build_env_dashboard(app_container, "demo"),
            build_env_dashboard(app_container, "production"),
            build_strategies_dashboard(app_container),
        )
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "demo": jsonable_encoder(demo_data),
                "production": jsonable_encoder(prod_data),
                "strategies": jsonable_encoder(strategies_data),
                "settings": app_container.settings,
            },
        )

    @app.get("/api/dashboard/strategies")
    async def dashboard_strategies(
        request: Request,
        window_days: int = DEFAULT_STRATEGY_WINDOW_DAYS,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ) -> JSONResponse:
        if window_days not in STRATEGY_WINDOW_OPTIONS:
            return JSONResponse({"error": "invalid window_days"}, status_code=400)
        app_container = container(request)
        payload = await build_strategies_dashboard(
            app_container,
            window_days=window_days,
            series_ticker=series_ticker,
            strategy_name=strategy_name,
        )
        return JSONResponse(jsonable_encoder(payload))

    @app.get("/api/dashboard/{kalshi_env}")
    async def dashboard_env(kalshi_env: str, request: Request) -> JSONResponse:
        if kalshi_env not in ("demo", "production"):
            return JSONResponse({"error": "invalid env"}, status_code=400)
        app_container = container(request)
        payload = await build_env_dashboard(app_container, kalshi_env)
        return JSONResponse(jsonable_encoder(payload))

    @app.post("/api/rooms")
    async def create_room_endpoint(request: Request) -> JSONResponse:
        payload = await parse_json_model(request, RoomCreate)
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.ensure_deployment_control(app_container.settings.app_color)
            pack = await app_container.agent_pack_service.get_pack_for_color(repo, app_container.settings.app_color)
            room = await repo.create_room(
                payload,
                active_color=app_container.settings.app_color,
                shadow_mode=app_container.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=app_container.settings.kalshi_env,
                agent_pack_version=pack.version,
            )
            await session.commit()
        return JSONResponse({"id": room.id, "redirect": f"/rooms/{room.id}"})

    @app.post("/api/rooms/{room_id}/run")
    async def run_room_endpoint(room_id: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, TriggerRequest, default_on_empty=True)
        app_container = container(request)
        asyncio.create_task(app_container.supervisor.run_room(room_id, reason=payload.reason))
        return JSONResponse({"status": "scheduled", "room_id": room_id})

    @app.post("/api/markets/{market_ticker}/shadow-run")
    async def shadow_run_market_endpoint(market_ticker: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, ShadowRunRequest, default_on_empty=True)
        app_container = container(request)
        result = await app_container.shadow_training_service.create_shadow_room(
            market_ticker,
            name=payload.name,
            prompt=payload.prompt,
        )
        asyncio.create_task(app_container.supervisor.run_room(result.room_id, reason=payload.reason))
        return JSONResponse(
            {
                "status": "scheduled",
                "room_id": result.room_id,
                "market_ticker": market_ticker,
                "redirect": f"/rooms/{result.room_id}",
            }
        )

    @app.get("/api/rooms/{room_id}/snapshot")
    async def room_snapshot_endpoint(room_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            snapshot = await _load_room_snapshot(app_container, room_id, include_messages=False)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Room not found") from exc
        return JSONResponse(snapshot)

    @app.get("/rooms/{room_id}", response_class=HTMLResponse)
    async def room_detail(room_id: str, request: Request) -> HTMLResponse:
        app_container = container(request)
        try:
            snapshot = await _load_room_snapshot(app_container, room_id, include_messages=True)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Room not found") from exc
        messages = snapshot.get("messages", [])
        room = snapshot["room"]
        room_bootstrap = {
            "roomId": room_id,
            "snapshotUrl": f"/api/rooms/{room_id}/snapshot",
            "eventsUrl": f"/rooms/{room_id}/events?after={messages[-1]['sequence'] if messages else 0}",
            "initialSnapshot": snapshot,
            "initialMessages": messages,
        }
        return templates.TemplateResponse(
            request,
            "room.html",
            {
                "room": room,
                "messages": messages,
                "snapshot": snapshot,
                "room_bootstrap": room_bootstrap,
                "settings": app_container.settings,
            },
        )

    @app.get("/rooms/{room_id}/events")
    async def room_events(room_id: str, request: Request, after: int = 0, once: bool = False) -> StreamingResponse:
        app_container = container(request)

        async def event_stream() -> AsyncIterator[str]:
            last_sequence = after
            while True:
                if await request.is_disconnected():
                    break
                async with app_container.session_factory() as session:
                    repo = PlatformRepository(session)
                    messages = await repo.list_messages(room_id, after_sequence=last_sequence)
                    await session.commit()
                if messages:
                    serialized = []
                    for message in messages:
                        serialized.append(_serialize_message(message))
                    last_sequence = messages[-1].sequence
                    yield f"data: {json.dumps(serialized)}\n\n"
                    if once:
                        break
                elif once:
                    break
                await asyncio.sleep(app_container.settings.sse_poll_interval_seconds)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/api/control/kill-switch/{enabled}")
    async def set_kill_switch(enabled: bool, request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.set_kill_switch(enabled)
            await session.commit()
        return JSONResponse({"status": "ok", "kill_switch_enabled": control.kill_switch_enabled})

    @app.post("/api/control/promote/{color}")
    async def promote_color(color: str, request: Request) -> JSONResponse:
        if color not in {"blue", "green"}:
            raise HTTPException(status_code=400, detail="Color must be blue or green")
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.set_active_color(color)
            await session.commit()
        return JSONResponse({"status": "ok", "active_color": control.active_color})

    @app.get("/favicon.ico")
    async def favicon() -> RedirectResponse:
        return RedirectResponse(url="/")

    return app
