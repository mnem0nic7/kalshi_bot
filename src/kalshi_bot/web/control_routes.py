from __future__ import annotations

from typing import Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer


def create_control_router(
    *,
    container: Callable[[Request], AppContainer],
    repository_cls: type[PlatformRepository] = PlatformRepository,
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/control/kill-switch/{enabled}")
    async def set_kill_switch(enabled: bool, request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            control = await repo.set_kill_switch(enabled)
            await session.commit()
        return JSONResponse({"status": "ok", "kill_switch_enabled": control.kill_switch_enabled})

    @router.post("/api/control/promote/{color}")
    async def promote_color(color: str, request: Request) -> JSONResponse:
        if color not in {"blue", "green"}:
            raise HTTPException(status_code=400, detail="Color must be blue or green")
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            control = await repo.set_active_color(color)
            await session.commit()
        return JSONResponse({"status": "ok", "active_color": control.active_color})

    return router
