from __future__ import annotations

import asyncio
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.control_room import build_env_dashboard, build_strategies_dashboard
from kalshi_bot.web.faq_content import FAQ_SECTIONS

SITE_LABELS = {
    "demo": "Demo",
    "production": "Production",
    "strategies": "Strategies",
}


def create_page_router(
    *,
    container: Callable[[Request], AppContainer],
    templates: Jinja2Templates,
    template_context: Callable[..., dict[str, Any]],
    normalized_site_kind: Callable[[], str],
    dashboard_shell: Callable[[], dict[str, Any]],
    build_env_dashboard_func: Callable[[AppContainer, str], Any] = build_env_dashboard,
    build_strategies_dashboard_func: Callable[[AppContainer], Any] = build_strategies_dashboard,
) -> APIRouter:
    router = APIRouter()

    @router.get("/faq", response_class=HTMLResponse)
    async def faq(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "faq.html",
            template_context(
                request,
                faq_sections=FAQ_SECTIONS,
            ),
        )

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        app_container = container(request)
        site_kind = normalized_site_kind()
        shell = dashboard_shell()
        tasks: list[tuple[str, Any]] = []
        if site_kind in {"combined", "demo"}:
            tasks.append(("demo", build_env_dashboard_func(app_container, "demo")))
        if site_kind in {"combined", "production"}:
            tasks.append(("production", build_env_dashboard_func(app_container, "production")))
        if site_kind in {"combined", "strategies"}:
            tasks.append(("strategies", build_strategies_dashboard_func(app_container)))

        results = await asyncio.gather(*(task for _, task in tasks))
        payloads = {key: jsonable_encoder(result) for (key, _), result in zip(tasks, results, strict=False)}
        env_panels = [
            {
                "key": env_key,
                "label": SITE_LABELS[env_key],
                "data": payloads[env_key],
                "active": shell["active_env"] == env_key,
            }
            for env_key in ("demo", "production")
            if env_key in payloads
        ]
        strategies_panel = payloads.get("strategies")
        return templates.TemplateResponse(
            request,
            "index.html",
            template_context(
                request,
                env_panels=env_panels,
                strategies=strategies_panel,
            ),
        )

    @router.get("/api/dashboard/{kalshi_env}")
    async def dashboard_env(kalshi_env: str, request: Request) -> JSONResponse:
        if kalshi_env not in ("demo", "production"):
            return JSONResponse({"error": "invalid env"}, status_code=400)
        app_container = container(request)
        payload = await build_env_dashboard_func(app_container, kalshi_env)
        return JSONResponse(jsonable_encoder(payload))

    return router
