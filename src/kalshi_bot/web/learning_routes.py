from __future__ import annotations

from datetime import datetime
from typing import Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from kalshi_bot.core.schemas import (
    HeuristicPackPromoteRequest,
    HeuristicPackRollbackRequest,
    HistoricalDateRangeRequest,
    HistoricalIntelligenceRunRequest,
    HistoricalTrainingBuildRequest,
    ShadowCampaignRequest,
    SelfImprovePromoteRequest,
    SelfImproveRollbackRequest,
    TrainingBuildRequest,
)
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.request_parsing import ParseJsonModel


def create_learning_router(
    *,
    container: Callable[[Request], AppContainer],
    parse_json_model: ParseJsonModel,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/self-improve/status")
    async def self_improve_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.self_improve_service.get_status())

    @router.get("/api/training/status")
    async def training_status(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await app_container.training_corpus_service.get_status(persist_readiness=True)
        payload["historical"] = await app_container.historical_training_service.get_status()
        payload["heuristics"] = await app_container.historical_intelligence_service.get_status()
        return JSONResponse(payload)

    @router.post("/api/training/build")
    async def training_build(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, TrainingBuildRequest, default_on_empty=True)
        return JSONResponse(await app_container.training_corpus_service.build_dataset(payload))

    @router.get("/api/historical/status")
    async def historical_status(request: Request, verbose: bool = False) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_training_service.get_status(verbose=verbose))

    @router.get("/api/historical/pipeline/status")
    async def historical_pipeline_status(request: Request, verbose: bool = False) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_pipeline_service.status(verbose=verbose))

    @router.get("/api/historical/intelligence/status")
    async def historical_intelligence_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_intelligence_service.get_status())

    @router.post("/api/historical/intelligence/run")
    async def historical_intelligence_run(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalIntelligenceRunRequest)
        return JSONResponse(await app_container.historical_intelligence_service.run(payload))

    @router.get("/api/historical/intelligence/explain")
    async def historical_intelligence_explain(request: Request, series: str | None = None) -> JSONResponse:
        app_container = container(request)
        series_values = [item for item in (series or "").split(",") if item]
        return JSONResponse(await app_container.historical_intelligence_service.explain(series=series_values or None))

    @router.get("/api/heuristic-pack/status")
    async def heuristic_pack_status(request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(await app_container.historical_intelligence_service.get_status())

    @router.post("/api/heuristic-pack/promote")
    async def heuristic_pack_promote(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HeuristicPackPromoteRequest, default_on_empty=True)
        return JSONResponse(
            await app_container.historical_intelligence_service.promote(
                candidate_version=payload.candidate_version,
                reason=payload.reason,
            )
        )

    @router.post("/api/heuristic-pack/rollback")
    async def heuristic_pack_rollback(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HeuristicPackRollbackRequest, default_on_empty=True)
        return JSONResponse(await app_container.historical_intelligence_service.rollback(reason=payload.reason))

    @router.post("/api/historical/import")
    async def historical_import(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalDateRangeRequest)
        result = await app_container.historical_training_service.import_weather_history(
            date_from=datetime.fromisoformat(payload.date_from).date(),
            date_to=datetime.fromisoformat(payload.date_to).date(),
            series=payload.series or None,
        )
        return JSONResponse(result)

    @router.post("/api/historical/replay")
    async def historical_replay(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalDateRangeRequest)
        result = await app_container.historical_training_service.replay_weather_history(
            date_from=datetime.fromisoformat(payload.date_from).date(),
            date_to=datetime.fromisoformat(payload.date_to).date(),
            series=payload.series or None,
        )
        return JSONResponse(result)

    @router.post("/api/training/historical/build")
    async def historical_training_build(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, HistoricalTrainingBuildRequest)
        return JSONResponse(await app_container.historical_training_service.build_historical_dataset(payload))

    @router.get("/api/training/builds")
    async def training_builds(request: Request) -> JSONResponse:
        app_container = container(request)
        builds = await app_container.training_corpus_service.list_builds(limit=10)
        return JSONResponse({"builds": [build.model_dump(mode="json") for build in builds]})

    @router.get("/api/research-audit")
    async def research_audit_alias(request: Request) -> JSONResponse:
        app_container = container(request)
        issues = await app_container.training_corpus_service.research_audit(limit=50)
        return JSONResponse({"issues": [issue.model_dump(mode="json") for issue in issues]})

    @router.get("/api/research/audit")
    async def research_audit(request: Request) -> JSONResponse:
        app_container = container(request)
        issues = await app_container.training_corpus_service.research_audit(limit=50)
        return JSONResponse({"issues": [issue.model_dump(mode="json") for issue in issues]})

    @router.get("/api/strategy-audit/rooms/{room_id}")
    async def strategy_audit_room(room_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        return JSONResponse((await app_container.training_corpus_service.strategy_audit_room(room_id)).model_dump(mode="json"))

    @router.get("/api/strategy-audit/summary")
    async def strategy_audit_summary(request: Request, days: int | None = None, limit: int = 100) -> JSONResponse:
        app_container = container(request)
        return JSONResponse(
            (await app_container.training_corpus_service.strategy_audit_summary(days=days, limit=limit)).model_dump(mode="json")
        )

    @router.post("/api/shadow-campaign/run")
    async def shadow_campaign_run(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, ShadowCampaignRequest, default_on_empty=True)
        results = await app_container.shadow_campaign_service.run(payload)
        return JSONResponse(
            {
                "status": "completed",
                "count": len(results),
                "rooms": [
                    {
                        "room_id": result.room_id,
                        "market_ticker": result.market_ticker,
                        "redirect": f"/rooms/{result.room_id}",
                    }
                    for result in results
                ],
            }
        )

    @router.post("/api/self-improve/critique")
    async def self_improve_critique(request: Request) -> JSONResponse:
        app_container = container(request)
        result = await app_container.self_improve_service.critique_recent_rooms()
        return JSONResponse(result.payload)

    @router.post("/api/self-improve/eval/{candidate_version}")
    async def self_improve_eval(candidate_version: str, request: Request) -> JSONResponse:
        app_container = container(request)
        result = await app_container.self_improve_service.evaluate_candidate(candidate_version=candidate_version)
        return JSONResponse(result.payload)

    @router.post("/api/self-improve/promote")
    async def self_improve_promote(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, SelfImprovePromoteRequest)
        result = await app_container.self_improve_service.promote_candidate(
            evaluation_run_id=payload.evaluation_run_id,
            reason=payload.reason,
        )
        return JSONResponse(result.payload)

    @router.post("/api/self-improve/rollback")
    async def self_improve_rollback(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, SelfImproveRollbackRequest, default_on_empty=True)
        result = await app_container.self_improve_service.rollback(reason=payload.reason)
        return JSONResponse(result.payload)

    return router
