from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.auth import hash_session_token
from kalshi_bot.web.auth_routes import create_auth_router
from kalshi_bot.web.background_tasks import schedule_logged_task
from kalshi_bot.web.control_routes import create_control_router
from kalshi_bot.web.control_room_routes import create_control_room_router
from kalshi_bot.web.control_room import (
    build_control_room_summary,  # noqa: F401 - tests monkeypatch this app-module seam.
    build_control_room_tab,  # noqa: F401 - tests monkeypatch this app-module seam.
    build_env_dashboard,  # noqa: F401 - tests monkeypatch this app-module seam.
    build_strategies_dashboard,  # noqa: F401 - tests monkeypatch this app-module seam.
    build_control_room_bootstrap,  # noqa: F401 - tests monkeypatch this app-module seam.
)
from kalshi_bot.web.learning_routes import create_learning_router
from kalshi_bot.web.page_routes import SITE_LABELS, create_page_router
from kalshi_bot.web.research_routes import create_research_router
from kalshi_bot.web.request_parsing import parse_json_model
from kalshi_bot.web.room_routes import create_room_router
from kalshi_bot.web.strategy_routes import create_strategy_router
from kalshi_bot.web.system_routes import create_system_router

templates = Jinja2Templates(directory="src/kalshi_bot/web/templates")
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
    container_type_settings = get_settings()

    def container(request: Request) -> AppContainer:
        return request.app.state.container

    def request_path_with_query(request: Request) -> str:
        return f"{request.url.path}?{request.url.query}" if request.url.query else request.url.path

    def as_utc_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def safe_next_path(next_path: str | None) -> str:
        if not next_path:
            return "/"
        if not next_path.startswith("/") or next_path.startswith("//"):
            return "/"
        return next_path

    def auth_redirect_url(next_path: str | None) -> str:
        target = safe_next_path(next_path)
        if target == "/":
            return "/login"
        return f"/login?next={quote(target, safe='/?=&')}"

    def next_query_suffix(next_path: str) -> str:
        if next_path == "/":
            return ""
        return f"?next={quote(next_path, safe='/?=&')}"

    def normalized_site_kind() -> str:
        site_kind = container_type_settings.web_site_kind.strip().lower()
        return site_kind if site_kind in SITE_LABELS else "combined"

    def dashboard_shell() -> dict[str, Any]:
        site_kind = normalized_site_kind()
        combined = site_kind == "combined"
        active_env = "demo" if combined else site_kind
        active_label = SITE_LABELS[active_env]
        tabs = []
        if combined:
            for key, label in SITE_LABELS.items():
                tabs.append(
                    {
                        "id": key,
                        "label": label,
                        "active": key == active_env,
                        "mode": "local",
                    }
                )
        return {
            "mode": "combined" if combined else "single_site",
            "active_env": active_env,
            "active_label": active_label,
            "brand_label": "Kalshi Bot Control Room" if combined else f"Kalshi Bot {active_label}",
            "browser_title": "Kalshi Bot Control Room" if combined else f"Kalshi Bot {active_label}",
            "tabs": tabs,
        }

    def configured_cookie_domain() -> str | None:
        domain = container_type_settings.web_auth_cookie_domain
        return domain.strip() if isinstance(domain, str) and domain.strip() else None

    def set_session_cookie(response: Response, request: Request, token: str) -> None:
        app_container = container(request)
        cookie_kwargs: dict[str, Any] = {
            "key": app_container.settings.web_auth_cookie_name,
            "value": token,
            "max_age": app_container.settings.web_auth_session_ttl_seconds,
            "httponly": True,
            "samesite": "lax",
            "secure": request.url.scheme == "https",
            "path": "/",
        }
        cookie_domain = configured_cookie_domain()
        if cookie_domain is not None:
            cookie_kwargs["domain"] = cookie_domain
        response.set_cookie(**cookie_kwargs)

    def clear_session_cookie(response: RedirectResponse | JSONResponse | HTMLResponse, request: Request) -> None:
        app_container = container(request)
        cookie_kwargs: dict[str, Any] = {
            "key": app_container.settings.web_auth_cookie_name,
            "httponly": True,
            "samesite": "lax",
            "secure": request.url.scheme == "https",
            "path": "/",
        }
        cookie_domain = configured_cookie_domain()
        if cookie_domain is not None:
            cookie_kwargs["domain"] = cookie_domain
        response.delete_cookie(**cookie_kwargs)

    def template_context(request: Request, **extra: Any) -> dict[str, Any]:
        app_container = container(request)
        current_user = getattr(request.state, "current_user", None)
        return {
            "request": request,
            "settings": app_container.settings,
            "current_user": current_user,
            "current_user_email": getattr(current_user, "email", None),
            "dashboard_shell": dashboard_shell(),
            **extra,
        }

    def auth_required_response(request: Request, *, clear_cookie: bool = False):
        if request.url.path.startswith("/api/"):
            response: JSONResponse | RedirectResponse = JSONResponse(
                {"error": "auth_required", "login_url": auth_redirect_url(request_path_with_query(request))},
                status_code=401,
            )
        else:
            response = RedirectResponse(url=auth_redirect_url(request_path_with_query(request)), status_code=303)
        if clear_cookie:
            clear_session_cookie(response, request)
        return response

    @app.middleware("http")
    async def require_authenticated_session(request: Request, call_next):
        request.state.current_user = None
        request.state.current_session = None

        app_container = container(request)
        settings = app_container.settings
        if not settings.web_auth_enabled:
            return await call_next(request)

        path = request.url.path
        if path.startswith("/static/") or path in {"/healthz", "/readyz", "/metrics", "/favicon.ico"}:
            return await call_next(request)

        cookie_token = request.cookies.get(settings.web_auth_cookie_name)
        clear_cookie = False
        refresh_cookie_token: str | None = None
        if cookie_token:
            token_hash = hash_session_token(cookie_token)
            async with app_container.session_factory() as session:
                repo = PlatformRepository(session)
                session_record = await repo.get_web_session_by_token_hash(token_hash)
                now = datetime.now(UTC)
                if session_record is None:
                    clear_cookie = True
                elif as_utc_datetime(session_record.expires_at) <= now:
                    await repo.delete_web_session(session_record.id)
                    clear_cookie = True
                else:
                    user = await repo.get_web_user(session_record.user_id)
                    if user is None or not user.is_active:
                        await repo.delete_web_session(session_record.id)
                        clear_cookie = True
                    else:
                        refreshed_expires_at = now + timedelta(seconds=settings.web_auth_session_ttl_seconds)
                        session_record = await repo.touch_web_session(
                            session_record.id,
                            seen_at=now,
                            expires_at=refreshed_expires_at,
                        )
                        request.state.current_user = user
                        request.state.current_session = session_record
                        refresh_cookie_token = cookie_token
                await session.commit()

        if path in {"/login", "/register"}:
            if request.method == "GET" and getattr(request.state, "current_user", None) is not None:
                response = RedirectResponse(
                    url=safe_next_path(request.query_params.get("next")),
                    status_code=303,
                )
            else:
                response = await call_next(request)
            if refresh_cookie_token is not None and getattr(request.state, "current_user", None) is not None:
                set_session_cookie(response, request, refresh_cookie_token)
            if clear_cookie:
                clear_session_cookie(response, request)
            return response

        if path == "/logout":
            response = await call_next(request)
            if clear_cookie:
                clear_session_cookie(response, request)
            return response

        if getattr(request.state, "current_user", None) is None:
            return auth_required_response(request, clear_cookie=clear_cookie)

        response = await call_next(request)
        if refresh_cookie_token is not None:
            set_session_cookie(response, request, refresh_cookie_token)
        if clear_cookie:
            clear_session_cookie(response, request)
        return response

    app.include_router(create_system_router(container=container, repository_cls=PlatformRepository))
    app.include_router(
        create_control_room_router(
            container=container,
            build_control_room_summary_func=build_control_room_summary,
            build_control_room_tab_func=build_control_room_tab,
        )
    )
    app.include_router(create_learning_router(container=container, parse_json_model=parse_json_model))
    app.include_router(create_research_router(container=container))

    app.include_router(
        create_auth_router(
            container=container,
            templates=templates,
            template_context=template_context,
            safe_next_path=safe_next_path,
            next_query_suffix=next_query_suffix,
            set_session_cookie=set_session_cookie,
            clear_session_cookie=clear_session_cookie,
            repository_cls=PlatformRepository,
        )
    )

    app.include_router(
        create_strategy_router(
            container=container,
            parse_json_model=parse_json_model,
            build_strategies_dashboard_func=build_strategies_dashboard,
            schedule_task=schedule_logged_task,
        )
    )
    app.include_router(
        create_page_router(
            container=container,
            templates=templates,
            template_context=template_context,
            normalized_site_kind=normalized_site_kind,
            dashboard_shell=dashboard_shell,
            build_env_dashboard_func=build_env_dashboard,
            build_strategies_dashboard_func=build_strategies_dashboard,
        )
    )
    app.include_router(
        create_room_router(
            container=container,
            parse_json_model=parse_json_model,
            templates=templates,
            template_context=template_context,
            logger=logger,
            repository_cls=PlatformRepository,
        )
    )
    app.include_router(create_control_router(container=container, repository_cls=PlatformRepository))

    return app
