from __future__ import annotations

import logging
from collections.abc import Coroutine
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from kalshi_bot.core.schemas import StrategyAssignmentApprovalRequest, StrategyCodexRunRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.container import AppContainer
from kalshi_bot.web.background_tasks import schedule_logged_task
from kalshi_bot.web.control_room import (
    DEFAULT_STRATEGY_WINDOW_DAYS,
    STRATEGY_APPROVAL_ASSIGNED_BY,
    STRATEGY_APPROVAL_EVENT_KIND,
    STRATEGY_APPROVAL_SOURCE,
    STRATEGY_WINDOW_OPTIONS,
    build_strategies_dashboard,
)
from kalshi_bot.web.request_parsing import ParseJsonModel

logger = logging.getLogger(__name__)


def create_strategy_router(
    *,
    container: Callable[[Request], AppContainer],
    parse_json_model: ParseJsonModel,
    build_strategies_dashboard_func: Callable[..., Coroutine[Any, Any, dict[str, Any]]] = build_strategies_dashboard,
    schedule_task: Callable[..., object] = schedule_logged_task,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/dashboard/strategies")
    async def dashboard_strategies(
        request: Request,
        window_days: int = DEFAULT_STRATEGY_WINDOW_DAYS,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ) -> JSONResponse:
        if window_days not in STRATEGY_WINDOW_OPTIONS:
            return JSONResponse({"error": "invalid window_days"}, status_code=400)
        app_container = container(request)
        payload = await build_strategies_dashboard_func(
            app_container,
            window_days=window_days,
            series_ticker=series_ticker,
            strategy_name=strategy_name,
        )
        return JSONResponse(jsonable_encoder(payload))

    @router.post("/api/strategies/codex/runs")
    async def create_strategy_codex_run(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await parse_json_model(request, StrategyCodexRunRequest)
        if payload.window_days not in STRATEGY_WINDOW_OPTIONS:
            return JSONResponse({"error": "invalid_window_days"}, status_code=400)
        if not app_container.strategy_codex_service.is_available():
            return JSONResponse({"error": "codex_unavailable"}, status_code=503)
        snapshot = await build_strategies_dashboard_func(
            app_container,
            window_days=payload.window_days,
            series_ticker=payload.series_ticker,
            strategy_name=payload.strategy_name,
        )
        try:
            run = await app_container.strategy_codex_service.create_run(
                request=payload,
                dashboard_snapshot=snapshot,
                trigger_source="manual",
            )
        except ValueError as exc:
            return JSONResponse({"error": "invalid_provider_config", "message": str(exc)}, status_code=400)
        schedule_task(
            app_container.strategy_codex_service.execute_run(run["run_id"]),
            name=f"strategy_codex_run:{run['run_id']}",
            logger=logger,
        )
        return JSONResponse(run)

    @router.get("/api/strategies/codex/runs/{run_id}")
    async def get_strategy_codex_run(run_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await app_container.strategy_codex_service.get_run_view(run_id)
        if payload is None:
            return JSONResponse({"error": "unknown_run_id", "run_id": run_id}, status_code=404)
        return JSONResponse(jsonable_encoder(payload))

    @router.post("/api/strategies/codex/runs/{run_id}/accept")
    async def accept_strategy_codex_run(run_id: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            payload = await app_container.strategy_codex_service.accept_run(run_id)
        except KeyError:
            return JSONResponse({"error": "unknown_run_id", "run_id": run_id}, status_code=404)
        except ValueError as exc:
            return JSONResponse({"error": "invalid_run_state", "message": str(exc)}, status_code=400)
        return JSONResponse(jsonable_encoder(payload))

    @router.post("/api/strategies/auto-evolve/run")
    async def run_strategy_auto_evolve(request: Request) -> JSONResponse:
        app_container = container(request)
        payload = await app_container.strategy_auto_evolve_service.run_once(trigger_source="manual")
        return JSONResponse(jsonable_encoder(payload))

    @router.get("/api/strategies/calibration")
    async def strategies_calibration(
        request: Request,
        series_ticker: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        bucket_by: str = "overall",
        n_buckets: int = 10,
    ) -> JSONResponse:
        from datetime import date as _date

        if bucket_by not in {"overall", "series", "month"}:
            return JSONResponse({"error": "invalid_bucket_by"}, status_code=400)
        if n_buckets < 2 or n_buckets > 50:
            return JSONResponse({"error": "invalid_n_buckets"}, status_code=400)
        try:
            df = _date.fromisoformat(date_from) if date_from else None
            dt = _date.fromisoformat(date_to) if date_to else None
        except ValueError:
            return JSONResponse({"error": "invalid_date_format"}, status_code=400)

        app_container = container(request)
        service = app_container.signal_calibration_service
        if bucket_by == "overall":
            summary = await service.compute_overall(
                date_from=df,
                date_to=dt,
                series_ticker=series_ticker,
                n_buckets=n_buckets,
            )
            return JSONResponse(jsonable_encoder({"bucket_by": "overall", "result": summary.to_dict()}))
        if bucket_by == "series":
            summaries = await service.compute_per_series(
                date_from=df,
                date_to=dt,
                n_buckets=n_buckets,
            )
        else:
            summaries = await service.compute_per_month(
                date_from=df,
                date_to=dt,
                series_ticker=series_ticker,
                n_buckets=n_buckets,
            )
        return JSONResponse(
            jsonable_encoder({"bucket_by": bucket_by, "results": [summary.to_dict() for summary in summaries]})
        )

    @router.get("/api/strategies/cleanup/discount-sweep")
    async def strategies_cleanup_discount_sweep(
        request: Request,
        discounts: str = "0,0.5,1,2",
        lookback_days: int = 30,
        latency_budget_seconds: int = 10,
    ) -> JSONResponse:
        try:
            candidates = [float(value) for value in discounts.split(",") if value.strip()]
        except ValueError:
            return JSONResponse({"error": "invalid_discounts"}, status_code=400)
        if not candidates:
            return JSONResponse({"error": "empty_discounts"}, status_code=400)
        if any(discount < 0 or discount > 100 for discount in candidates):
            return JSONResponse({"error": "discount_out_of_range"}, status_code=400)
        if lookback_days < 1 or lookback_days > 365:
            return JSONResponse({"error": "invalid_lookback_days"}, status_code=400)
        if latency_budget_seconds < 1 or latency_budget_seconds > 600:
            return JSONResponse({"error": "invalid_latency_budget_seconds"}, status_code=400)

        app_container = container(request)
        payload = await app_container.strategy_cleanup_service.sweep_discount_sensitivity(
            discount_cents_candidates=candidates,
            lookback_days=lookback_days,
            latency_budget_seconds=latency_budget_seconds,
        )
        return JSONResponse(jsonable_encoder(payload))

    @router.get("/api/strategies/promotions")
    async def strategies_promotions(
        request: Request,
        strategy: str | None = None,
        limit: int = 25,
    ) -> JSONResponse:
        if limit < 1 or limit > 500:
            return JSONResponse({"error": "invalid_limit"}, status_code=400)
        app_container = container(request)
        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            events = await repo.list_strategy_promotions(
                strategy=strategy,
                kalshi_env=app_container.settings.kalshi_env,
                limit=limit,
            )
        return JSONResponse(
            jsonable_encoder(
                {
                    "events": [
                        {
                            "id": event.id,
                            "strategy": event.strategy,
                            "from_state": event.from_state,
                            "to_state": event.to_state,
                            "actor": event.actor,
                            "evidence_ref": event.evidence_ref,
                            "notes": event.notes,
                            "kalshi_env": event.kalshi_env,
                            "created_at": event.created_at.isoformat(),
                        }
                        for event in events
                    ],
                }
            )
        )

    @router.post("/api/strategies/{strategy_name}/activate")
    async def activate_strategy_preset(strategy_name: str, request: Request) -> JSONResponse:
        app_container = container(request)
        try:
            payload = await app_container.strategy_codex_service.activate_strategy(strategy_name)
        except KeyError:
            return JSONResponse({"error": "unknown_strategy", "strategy_name": strategy_name}, status_code=404)
        return JSONResponse(jsonable_encoder(payload))

    @router.post("/api/strategies/assignments/{series_ticker}/approve")
    async def approve_strategy_assignment(series_ticker: str, request: Request) -> JSONResponse:
        payload = await parse_json_model(request, StrategyAssignmentApprovalRequest)
        app_container = container(request)
        snapshot = await build_strategies_dashboard_func(
            app_container,
            window_days=DEFAULT_STRATEGY_WINDOW_DAYS,
            series_ticker=series_ticker,
        )
        city_row = next(
            (
                row
                for row in snapshot.get("city_matrix", [])
                if row.get("series_ticker") == series_ticker
            ),
            None,
        )
        detail_context = snapshot.get("detail_context")
        fresh_context = {
            "city_row": city_row,
            "detail_context": (
                detail_context
                if isinstance(detail_context, dict)
                and detail_context.get("selected_series_ticker") == series_ticker
                else None
            ),
        }
        if city_row is None:
            return JSONResponse(
                {"error": "unknown_series_ticker", "series_ticker": series_ticker},
                status_code=404,
            )

        recommendation = dict(city_row.get("recommendation") or {})
        approved_strategy_name = recommendation.get("strategy_name")
        approved_status = recommendation.get("status")
        snapshot_summary = dict(snapshot.get("summary") or {})
        current_corpus_build_id = snapshot_summary.get("corpus_build_id")
        if (
            approved_strategy_name != payload.expected_strategy_name
            or approved_status != payload.expected_recommendation_status
            or payload.expected_corpus_build_id != current_corpus_build_id
        ):
            return JSONResponse(
                {
                    "error": "stale_recommendation",
                    "series_ticker": series_ticker,
                    "message": "Recommendation changed. Reload the latest 180d snapshot before approving.",
                    **fresh_context,
                },
                status_code=409,
            )
        if not city_row.get("approval_eligible"):
            return JSONResponse(
                {
                    "error": "approval_not_eligible",
                    "series_ticker": series_ticker,
                    "message": "Only strong and lean 180d recommendations can be approved.",
                    **fresh_context,
                },
                status_code=409,
            )

        winning_metric = next(
            (
                row
                for row in city_row.get("metrics", [])
                if row.get("strategy_name") == approved_strategy_name
            ),
            None,
        )

        async with app_container.session_factory() as session:
            repo = PlatformRepository(session)
            previous_assignment = await repo.get_city_strategy_assignment(
                series_ticker,
                kalshi_env=app_container.settings.kalshi_env,
            )
            previous_strategy_name = previous_assignment.strategy_name if previous_assignment is not None else None
            await repo.set_city_strategy_assignment(
                series_ticker,
                str(approved_strategy_name),
                assigned_by=STRATEGY_APPROVAL_ASSIGNED_BY,
                kalshi_env=app_container.settings.kalshi_env,
                evidence_corpus_build_id=current_corpus_build_id,
                evidence_run_at=snapshot_summary.get("last_regression_run"),
            )
            await repo.record_city_assignment_event(
                series_ticker=series_ticker,
                previous_strategy=previous_strategy_name,
                new_strategy=str(approved_strategy_name),
                event_type="manual_assign",
                actor=STRATEGY_APPROVAL_ASSIGNED_BY,
                kalshi_env=app_container.settings.kalshi_env,
                note=payload.note,
                metadata={
                    "recommendation_status": approved_status,
                    "recommendation_label": recommendation.get("label"),
                    "basis_run_at": snapshot_summary.get("last_regression_run"),
                    "corpus_build_id": current_corpus_build_id,
                    "source": STRATEGY_APPROVAL_SOURCE,
                },
            )
            await repo.log_ops_event(
                severity="info",
                summary=(
                    f"Approved strategy assignment for {series_ticker}: "
                    f"{previous_strategy_name or 'unassigned'} -> {approved_strategy_name}"
                ),
                source=STRATEGY_APPROVAL_SOURCE,
                payload={
                    "event_kind": STRATEGY_APPROVAL_EVENT_KIND,
                    "series_ticker": series_ticker,
                    "previous_strategy": previous_strategy_name,
                    "new_strategy": approved_strategy_name,
                    "recommendation_status": approved_status,
                    "recommendation_label": recommendation.get("label"),
                    "trade_count": int(winning_metric.get("trade_count") or 0) if winning_metric is not None else 0,
                    "resolved_trade_count": int(winning_metric.get("resolved_trade_count") or 0)
                    if winning_metric is not None
                    else 0,
                    "unscored_trade_count": int(winning_metric.get("unscored_trade_count") or 0)
                    if winning_metric is not None
                    else 0,
                    "outcome_coverage_rate": winning_metric.get("outcome_coverage_rate")
                    if winning_metric is not None
                    else None,
                    "gap_to_runner_up": city_row.get("gap_to_runner_up"),
                    "new_win_rate": winning_metric.get("win_rate") if winning_metric is not None else None,
                    "note": payload.note,
                    "basis_run_at": snapshot_summary.get("last_regression_run"),
                    "corpus_build_id": current_corpus_build_id,
                    "assigned_by": STRATEGY_APPROVAL_ASSIGNED_BY,
                },
            )
            assignment = await repo.get_city_strategy_assignment(
                series_ticker,
                kalshi_env=app_container.settings.kalshi_env,
            )
            await session.commit()

        if app_container.secondary_session_factory is not None:
            try:
                async with app_container.secondary_session_factory() as sec_session:
                    sec_repo = PlatformRepository(sec_session, kalshi_env=app_container.settings.kalshi_env)
                    await sec_repo.set_city_strategy_assignment(
                        series_ticker,
                        str(approved_strategy_name),
                        assigned_by=STRATEGY_APPROVAL_ASSIGNED_BY,
                        kalshi_env=app_container.settings.kalshi_env,
                        evidence_corpus_build_id=current_corpus_build_id,
                        evidence_run_at=snapshot_summary.get("last_regression_run"),
                    )
                    await sec_session.commit()
            except Exception:
                logger.warning(
                    "Secondary DB write failed for strategy assignment %s -> %s",
                    series_ticker,
                    approved_strategy_name,
                    exc_info=True,
                )

        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "approved",
                    "series_ticker": series_ticker,
                    "strategy_name": assignment.strategy_name if assignment is not None else approved_strategy_name,
                    "assigned_by": assignment.assigned_by if assignment is not None else STRATEGY_APPROVAL_ASSIGNED_BY,
                    "assigned_at": assignment.assigned_at if assignment is not None else None,
                }
            )
        )

    return router
