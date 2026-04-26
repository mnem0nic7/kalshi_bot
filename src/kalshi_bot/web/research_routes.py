from __future__ import annotations

import logging
from typing import Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.background_tasks import schedule_logged_task

logger = logging.getLogger(__name__)


def create_research_router(*, container: Callable[[Request], AppContainer]) -> APIRouter:
    router = APIRouter()

    @router.get("/api/research/{market_ticker}")
    async def research_dossier(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        dossier = await app_container.research_coordinator.get_latest_dossier(market_ticker)
        if dossier is None:
            raise HTTPException(status_code=404, detail="Research dossier not found")
        return JSONResponse(dossier.model_dump(mode="json"))

    @router.get("/api/research/{market_ticker}/history")
    async def research_history(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        runs = await app_container.research_coordinator.list_recent_runs(market_ticker, limit=10)
        return JSONResponse({"market_ticker": market_ticker, "runs": runs})

    @router.post("/api/research/{market_ticker}/refresh")
    async def refresh_research(market_ticker: str, request: Request) -> JSONResponse:
        app_container = container(request)
        schedule_logged_task(
            app_container.research_coordinator.refresh_market_dossier(
                market_ticker,
                trigger_reason="api_refresh",
                force=True,
            ),
            name=f"research_refresh:{market_ticker}",
            logger=logger,
        )
        return JSONResponse({"status": "scheduled", "market_ticker": market_ticker})

    return router
