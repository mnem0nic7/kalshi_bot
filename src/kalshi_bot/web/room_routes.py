from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from kalshi_bot.core.schemas import RoomCreate, ShadowRunRequest, TriggerRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.background_tasks import schedule_logged_task
from kalshi_bot.web.request_parsing import ParseJsonModel
from kalshi_bot.web.room_snapshot import load_room_snapshot, serialize_message


def create_room_router(
    *,
    container: Callable[[Request], AppContainer],
    parse_json_model: ParseJsonModel,
    templates: Jinja2Templates,
    template_context: Callable[..., dict[str, Any]],
    logger: logging.Logger,
    repository_cls: type[PlatformRepository] = PlatformRepository,
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/rooms")
    async def create_room_endpoint(request: Request) -> JSONResponse:
        payload = await parse_json_model(request, RoomCreate)
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            control = await repo.ensure_deployment_control(app_container.settings.app_color)
            active_color = control.active_color
            pack = await app_container.agent_pack_service.get_pack_for_color(repo, active_color)
            room = await repo.create_room(
                payload,
                active_color=active_color,
                shadow_mode=app_container.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=app_container.settings.kalshi_env,
                agent_pack_version=pack.version,
            )
            await session.commit()
        return JSONResponse({"id": room.id, "redirect": f"/rooms/{room.id}"})

    @router.post("/api/rooms/{room_id}/run")
    async def run_room_endpoint(room_id: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, TriggerRequest, default_on_empty=True)
        app_container = container(request)
        schedule_logged_task(
            app_container.supervisor.run_room(room_id, reason=payload.reason),
            name=f"room_run:{room_id}",
            logger=logger,
        )
        return JSONResponse({"status": "scheduled", "room_id": room_id})

    @router.post("/api/markets/{market_ticker}/shadow-run")
    async def shadow_run_market_endpoint(market_ticker: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, ShadowRunRequest, default_on_empty=True)
        app_container = container(request)
        result = await app_container.shadow_training_service.create_shadow_room(
            market_ticker,
            name=payload.name,
            prompt=payload.prompt,
        )
        schedule_logged_task(
            app_container.supervisor.run_room(result.room_id, reason=payload.reason),
            name=f"shadow_room_run:{result.room_id}",
            logger=logger,
        )
        return JSONResponse(
            {
                "status": "scheduled",
                "room_id": result.room_id,
                "market_ticker": market_ticker,
                "redirect": f"/rooms/{result.room_id}",
            }
        )

    @router.get("/api/rooms/{room_id}/snapshot")
    async def room_snapshot_endpoint(room_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            snapshot = await load_room_snapshot(app_container, room_id, include_messages=False)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Room not found") from exc
        return JSONResponse(snapshot)

    @router.get("/rooms/{room_id}", response_class=HTMLResponse)
    async def room_detail(room_id: str, request: Request) -> HTMLResponse:
        app_container = container(request)
        try:
            snapshot = await load_room_snapshot(app_container, room_id, include_messages=True)
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
            template_context(
                request,
                room=room,
                messages=messages,
                snapshot=snapshot,
                room_bootstrap=room_bootstrap,
            ),
        )

    @router.get("/rooms/{room_id}/events")
    async def room_events(room_id: str, request: Request, after: int = 0, once: bool = False) -> StreamingResponse:
        app_container = container(request)

        async def event_stream() -> AsyncIterator[str]:
            last_sequence = after
            while True:
                if await request.is_disconnected():
                    break
                async with app_container.session_factory() as session:
                    repo = repository_cls(session)
                    messages = await repo.list_messages(room_id, after_sequence=last_sequence)
                    await session.commit()
                if messages:
                    serialized = []
                    for message in messages:
                        serialized.append(serialize_message(message))
                    last_sequence = messages[-1].sequence
                    yield f"data: {json.dumps(serialized)}\n\n"
                    if once:
                        break
                elif once:
                    break
                await asyncio.sleep(app_container.settings.sse_poll_interval_seconds)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    return router
