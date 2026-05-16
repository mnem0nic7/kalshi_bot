from __future__ import annotations

import logging
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.background_tasks import schedule_logged_task


class CryptoAssetModeRequest(BaseModel):
    mode: str


def create_crypto_router(
    *,
    container: Callable[[Request], AppContainer],
    templates: Jinja2Templates,
    template_context: Callable[..., dict[str, Any]],
    logger: logging.Logger,
) -> APIRouter:
    router = APIRouter()

    @router.get("/crypto", response_class=HTMLResponse)
    async def crypto_dashboard(request: Request, frequency: str = "15m") -> HTMLResponse:
        app_container = container(request)
        payload = await app_container.crypto_market_service.dashboard_payload(frequency=frequency)
        return templates.TemplateResponse(
            request,
            "crypto.html",
            template_context(
                request,
                crypto_bootstrap=jsonable_encoder(payload),
            ),
        )

    @router.get("/api/crypto/status")
    async def crypto_status(request: Request, frequency: str = "15m") -> JSONResponse:
        app_container = container(request)
        return JSONResponse(jsonable_encoder(await app_container.crypto_market_service.status(frequency=frequency)))

    @router.get("/api/crypto/markets")
    async def crypto_markets(request: Request, frequency: str = "15m", current_only: bool = True) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(
            jsonable_encoder(
                await app_container.crypto_market_service.dashboard_payload(
                    frequency=frequency,
                    current_only=current_only,
                )
            )
        )

    @router.get("/api/crypto/markets/{market_ticker}")
    async def crypto_market_detail(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            market = await app_container.crypto_market_service.get_market(market_ticker)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Crypto market not found") from exc
        return JSONResponse(jsonable_encoder({"market": market.to_payload()}))

    @router.post("/api/crypto/markets/{market_ticker}/rooms")
    async def create_crypto_room(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            result = await app_container.crypto_market_service.create_room_for_market(market_ticker)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Crypto market not found") from exc
        schedule_logged_task(
            app_container.run_room(result["room_id"], reason="crypto_dashboard"),
            name=f"crypto_room_run:{result['room_id']}",
            logger=logger,
        )
        return JSONResponse({"status": "scheduled", **result})

    @router.patch("/api/crypto/assets/{asset_symbol}/mode")
    async def set_crypto_asset_mode(
        asset_symbol: str,
        payload: CryptoAssetModeRequest,
        request: Request,
    ) -> JSONResponse:
        app_container = container(request)
        actor = "operator"
        current_user = getattr(request.state, "current_user", None)
        if current_user is not None:
            actor = str(getattr(current_user, "email", None) or getattr(current_user, "id", None) or actor)
        try:
            result = await app_container.crypto_asset_control_service.set_asset_mode(
                asset_symbol,
                payload.mode,
                actor=actor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(jsonable_encoder(result))

    return router
