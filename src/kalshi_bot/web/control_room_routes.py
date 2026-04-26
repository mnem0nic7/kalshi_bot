from __future__ import annotations

from typing import Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.control_room import CONTROL_ROOM_TABS, build_control_room_summary, build_control_room_tab


def create_control_room_router(
    *,
    container: Callable[[Request], AppContainer],
    build_control_room_summary_func: Callable[[AppContainer], object] = build_control_room_summary,
    build_control_room_tab_func: Callable[[AppContainer, str], object] = build_control_room_tab,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/control-room/summary")
    async def control_room_summary(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await build_control_room_summary_func(app_container)
        return JSONResponse(jsonable_encoder(payload))

    @router.get("/api/control-room/tab/{tab_name}")
    async def control_room_tab(tab_name: str, request: Request) -> JSONResponse:
        if tab_name not in CONTROL_ROOM_TABS:
            raise HTTPException(status_code=404, detail="Unknown control room tab")
        app_container = container(request)
        payload = await build_control_room_tab_func(app_container, tab_name)
        return JSONResponse(jsonable_encoder(payload))

    return router
