from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, TypeVar

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ValidationError

from kalshi_bot.core.schemas import RoomCreate, TriggerRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer

templates = Jinja2Templates(directory="src/kalshi_bot/web/templates")
ModelT = TypeVar("ModelT", bound=BaseModel)
logger = logging.getLogger(__name__)


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
                for room in await repo.list_rooms(limit=10)
            ]
            await session.commit()
        return JSONResponse(
            {
                "active_color": control.active_color,
                "kill_switch_enabled": control.kill_switch_enabled,
                "execution_lock_holder": control.execution_lock_holder,
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

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            rooms = await repo.list_rooms()
            control = await repo.get_deployment_control()
            positions = await repo.list_positions(limit=8)
            ops_events = await repo.list_ops_events(limit=8)
            dossier_records = await repo.list_research_dossiers(limit=100)
            await session.commit()
        dossiers_by_market = {record.market_ticker: record.payload for record in dossier_records}
        configured_markets = []
        try:
            discoveries = await app_container.discovery_service.discover_configured_markets()
        except Exception:
            logger.exception("Failed to load live market discovery for index page")
            discoveries = []
        seen_markets: set[str] = set()
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
                    "dossier": dossiers_by_market.get(discovery.mapping.market_ticker),
                }
            )
            seen_markets.add(discovery.mapping.market_ticker)
        for mapping in app_container.weather_directory.all():
            if mapping.market_ticker in seen_markets:
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
                    "dossier": dossiers_by_market.get(mapping.market_ticker),
                }
            )
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "rooms": rooms,
                "control": control,
                "positions": positions,
                "ops_events": ops_events,
                "configured_markets": configured_markets,
                "settings": app_container.settings,
            },
        )

    @app.post("/api/rooms")
    async def create_room_endpoint(request: Request) -> JSONResponse:
        payload = await parse_json_model(request, RoomCreate)
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.ensure_deployment_control(app_container.settings.app_color)
            room = await repo.create_room(
                payload,
                active_color=control.active_color,
                shadow_mode=app_container.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
            )
            await session.commit()
        return JSONResponse({"id": room.id, "redirect": f"/rooms/{room.id}"})

    @app.post("/api/rooms/{room_id}/run")
    async def run_room_endpoint(room_id: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, TriggerRequest, default_on_empty=True)
        app_container = container(request)
        asyncio.create_task(app_container.supervisor.run_room(room_id, reason=payload.reason))
        return JSONResponse({"status": "scheduled", "room_id": room_id})

    @app.get("/rooms/{room_id}", response_class=HTMLResponse)
    async def room_detail(room_id: str, request: Request) -> HTMLResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            room = await repo.get_room(room_id)
            if room is None:
                raise HTTPException(status_code=404, detail="Room not found")
            messages = await repo.list_messages(room_id)
            dossier_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_dossier_snapshot")
            delta_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_delta")
            source_artifacts = await repo.list_artifacts(room_id=room_id, artifact_type="research_source", limit=12)
            latest_dossier = await repo.get_research_dossier(room.market_ticker)
            research_runs = await repo.list_research_runs(market_ticker=room.market_ticker, limit=5)
            await session.commit()
        return templates.TemplateResponse(
            request,
            "room.html",
            {
                "room": room,
                "messages": messages,
                "research_dossier": (dossier_artifact.payload if dossier_artifact is not None else latest_dossier.payload if latest_dossier is not None else None),
                "research_delta": delta_artifact.payload if delta_artifact is not None else None,
                "research_sources": [artifact.payload for artifact in source_artifacts],
                "research_runs": research_runs,
                "settings": app_container.settings,
            },
        )

    @app.get("/rooms/{room_id}/events")
    async def room_events(room_id: str, request: Request, after: int = 0) -> StreamingResponse:
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
                        serialized.append(
                            {
                                "id": message.id,
                                "sequence": message.sequence,
                                "role": message.role,
                                "kind": message.kind,
                                "content": message.content,
                                "created_at": message.created_at.isoformat(),
                            }
                        )
                    last_sequence = messages[-1].sequence
                    yield f"data: {json.dumps(serialized)}\n\n"
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
