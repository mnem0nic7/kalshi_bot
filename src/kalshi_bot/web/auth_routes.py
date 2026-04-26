from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Callable, TypeVar
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError

from kalshi_bot.core.schemas import WebLoginRequest, WebRegisterRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.auth import (
    hash_password,
    hash_session_token,
    is_registration_email_allowed,
    new_session_token,
    normalize_auth_email,
    verify_password,
)

ModelT = TypeVar("ModelT", bound=BaseModel)


async def _parse_form_model(request: Request, model_cls: type[ModelT]) -> ModelT:
    raw_body = await request.body()
    form_data = parse_qs(raw_body.decode("utf-8"), keep_blank_values=True) if raw_body else {}
    payload = {key: values[-1] if values else "" for key, values in form_data.items()}
    return model_cls.model_validate(payload)


def _validation_error_message(exc: ValidationError) -> str:
    error = exc.errors(include_url=False)[0] if exc.errors(include_url=False) else {}
    return str(error.get("msg") or "Invalid input")


def create_auth_router(
    *,
    container: Callable[[Request], AppContainer],
    templates: Jinja2Templates,
    template_context: Callable[..., dict[str, Any]],
    safe_next_path: Callable[[str | None], str],
    next_query_suffix: Callable[[str], str],
    set_session_cookie: Callable[[Response, Request, str], None],
    clear_session_cookie: Callable[[RedirectResponse | JSONResponse | HTMLResponse, Request], None],
    repository_cls: type[PlatformRepository] = PlatformRepository,
) -> APIRouter:
    router = APIRouter()

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request) -> HTMLResponse:
        next_path = safe_next_path(request.query_params.get("next"))
        return templates.TemplateResponse(
            request,
            "login.html",
            template_context(
                request,
                next_path=next_path,
                next_query=next_query_suffix(next_path),
                form_values={"email": ""},
            ),
        )

    @router.post("/login", response_class=HTMLResponse, response_model=None)
    async def login_submit(request: Request) -> HTMLResponse | RedirectResponse:
        app_container = container(request)
        next_path = safe_next_path(request.query_params.get("next"))
        try:
            payload = await _parse_form_model(request, WebLoginRequest)
        except ValidationError as exc:
            return templates.TemplateResponse(
                request,
                "login.html",
                template_context(
                    request,
                    next_path=next_path,
                    next_query=next_query_suffix(next_path),
                    form_values={"email": ""},
                    error_message=_validation_error_message(exc),
                ),
                status_code=422,
            )

        normalized_email = normalize_auth_email(payload.email)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            await repo.prune_expired_web_sessions()
            user = await repo.get_web_user_by_email(normalized_email)
            if user is None or not user.is_active or not verify_password(
                payload.password,
                expected_hash=user.password_hash if user is not None else "",
                salt_hex=user.password_salt if user is not None else "",
            ):
                await session.commit()
                return templates.TemplateResponse(
                    request,
                    "login.html",
                    template_context(
                        request,
                        next_path=next_path,
                        next_query=next_query_suffix(next_path),
                        form_values={"email": normalized_email},
                        error_message="Invalid email or password.",
                    ),
                    status_code=401,
                )

            now = datetime.now(UTC)
            session_token = new_session_token()
            token_hash = hash_session_token(session_token)
            expires_at = now + timedelta(seconds=app_container.settings.web_auth_session_ttl_seconds)
            await repo.record_web_user_login(user.id, logged_in_at=now)
            await repo.create_web_session(
                user_id=user.id,
                token_hash=token_hash,
                expires_at=expires_at,
                last_seen_at=now,
            )
            await session.commit()

        response = RedirectResponse(url=next_path, status_code=303)
        set_session_cookie(response, request, session_token)
        return response

    @router.get("/register", response_class=HTMLResponse)
    async def register_page(request: Request) -> HTMLResponse:
        next_path = safe_next_path(request.query_params.get("next"))
        return templates.TemplateResponse(
            request,
            "register.html",
            template_context(
                request,
                next_path=next_path,
                next_query=next_query_suffix(next_path),
                form_values={"email": ""},
            ),
        )

    @router.post("/register", response_class=HTMLResponse, response_model=None)
    async def register_submit(request: Request) -> HTMLResponse | RedirectResponse:
        app_container = container(request)
        next_path = safe_next_path(request.query_params.get("next"))
        try:
            payload = await _parse_form_model(request, WebRegisterRequest)
        except ValidationError as exc:
            return templates.TemplateResponse(
                request,
                "register.html",
                template_context(
                    request,
                    next_path=next_path,
                    next_query=next_query_suffix(next_path),
                    form_values={"email": ""},
                    error_message=_validation_error_message(exc),
                ),
                status_code=422,
            )

        normalized_email = normalize_auth_email(payload.email)
        if not is_registration_email_allowed(app_container.settings, normalized_email):
            return templates.TemplateResponse(
                request,
                "register.html",
                template_context(
                    request,
                    next_path=next_path,
                    next_query=next_query_suffix(next_path),
                    form_values={"email": normalized_email},
                    error_message="This email is not eligible to register for this site.",
                ),
                status_code=403,
            )

        password_hash_value, password_salt = hash_password(payload.password)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            await repo.prune_expired_web_sessions()
            existing_user = await repo.get_web_user_by_email(normalized_email)
            if existing_user is not None:
                await session.commit()
                return templates.TemplateResponse(
                    request,
                    "register.html",
                    template_context(
                        request,
                        next_path=next_path,
                        next_query=next_query_suffix(next_path),
                        form_values={"email": normalized_email},
                        error_message="That account already exists. Sign in instead.",
                    ),
                    status_code=409,
                )

            user = await repo.create_web_user(
                email=normalized_email,
                password_hash=password_hash_value,
                password_salt=password_salt,
            )
            now = datetime.now(UTC)
            session_token = new_session_token()
            token_hash = hash_session_token(session_token)
            expires_at = now + timedelta(seconds=app_container.settings.web_auth_session_ttl_seconds)
            await repo.record_web_user_login(user.id, logged_in_at=now)
            await repo.create_web_session(
                user_id=user.id,
                token_hash=token_hash,
                expires_at=expires_at,
                last_seen_at=now,
            )
            await session.commit()

        response = RedirectResponse(url=next_path, status_code=303)
        set_session_cookie(response, request, session_token)
        return response

    @router.post("/logout")
    async def logout(request: Request) -> RedirectResponse:
        app_container = container(request)
        cookie_token = request.cookies.get(app_container.settings.web_auth_cookie_name)
        if cookie_token:
            async with app_container.session_factory() as session:
                repo = repository_cls(session)
                await repo.delete_web_session_by_token_hash(hash_session_token(cookie_token))
                await session.commit()
        response = RedirectResponse(url="/login", status_code=303)
        clear_session_cookie(response, request)
        return response

    return router
