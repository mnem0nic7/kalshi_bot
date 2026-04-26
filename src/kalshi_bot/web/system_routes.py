from __future__ import annotations

from typing import Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer


def create_system_router(
    *,
    container: Callable[[Request], AppContainer],
    repository_cls: type[PlatformRepository] = PlatformRepository,
) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @router.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            control = await repo.get_deployment_control()
            await session.commit()
        return JSONResponse(
            {
                "status": "ready",
                "active_color": control.active_color,
                "kill_switch": control.kill_switch_enabled,
            }
        )

    @router.get("/api/status")
    async def status(request: Request) -> JSONResponse:
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = repository_cls(session)
            control = await repo.get_deployment_control()
            positions = await repo.list_positions(limit=10, kalshi_env=app_container.settings.kalshi_env)
            ops_events = await repo.list_ops_events(limit=10, kalshi_env=app_container.settings.kalshi_env)
            dossiers = [
                {"market_ticker": room.market_ticker, "stage": room.stage}
                for room in await repo.list_rooms(limit=20, origins=["shadow", "live"])
                if room.kalshi_env == app_container.settings.kalshi_env
            ]
            runtime_health = await app_container.watchdog_service.get_status(
                repo,
                kalshi_env=app_container.settings.kalshi_env,
            )
            await session.commit()
        training_status_payload = await app_container.training_corpus_service.get_status(persist_readiness=False)
        historical_status_payload = await app_container.historical_training_service.get_status()
        heuristic_status_payload = await app_container.historical_intelligence_service.get_status()
        training_status_payload["historical"] = historical_status_payload
        return JSONResponse(
            {
                "kalshi_env": app_container.settings.kalshi_env,
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

    @router.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    @router.get("/favicon.ico")
    async def favicon() -> RedirectResponse:
        return RedirectResponse(url="/")

    return router
