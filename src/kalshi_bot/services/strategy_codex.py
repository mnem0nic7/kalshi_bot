from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.codex_cli import CodexCLIProvider
from kalshi_bot.agents.providers import (
    NativeGeminiProvider,
    ProviderRouter,
)
from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import (
    StrategyCodexEvaluationPayload,
    StrategyCodexRunRequest,
    StrategyCodexSuggestionPayload,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.strategy_regression import RegressionStrategySpec, StrategyRegressionService, _rank_scored_strategy_rows

logger = logging.getLogger(__name__)

CODEX_RUN_STALE_AFTER = timedelta(minutes=10)
CODEX_RECENT_RUN_LIMIT = 8
CODEX_CREATION_WINDOW_DAYS = 180
CODEX_TRIGGER_SOURCES = {"manual", "nightly"}
STRATEGY_LAB_SOURCE = "strategy_lab"
LEGACY_STRATEGY_LAB_SOURCES = {"codex_cli", STRATEGY_LAB_SOURCE}
STRATEGY_PROVIDER_PREFERENCE = ("gemini", "codex")
STRATEGY_PROVIDER_LABELS = {
    "gemini": "Gemini",
    "codex": "Codex",
}


def _ratio_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{round(value * 100)}%"


def _money_display(value: float | Decimal | None) -> str:
    if value is None:
        return "—"
    amount = float(value)
    return f"{'+' if amount >= 0 else '-'}${abs(amount):.2f}"


def _bps_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{round(value)}bps"


def _coverage_display(resolved: int, trades: int) -> str:
    return f"{resolved}/{trades} scored"


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def _ordered_unique(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = _clean_text(value)
        if cleaned is None or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _json_safe(value: Any) -> Any:
    return jsonable_encoder(value)


class StrategyCodexService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        strategy_regression_service: StrategyRegressionService,
        providers: ProviderRouter | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.strategy_regression_service = strategy_regression_service
        self.providers = providers or ProviderRouter(settings)
        self._owns_providers = providers is None

    async def close(self) -> None:
        if self._owns_providers:
            await self.providers.close()

    def is_available(self) -> bool:
        return bool(self._provider_options())

    @staticmethod
    def _normalize_provider_id(provider_id: str | None) -> str | None:
        if not provider_id:
            return None
        lowered = provider_id.strip().lower()
        if lowered in {"codex", "codex-cli"}:
            return "codex"
        if lowered == "gemini":
            return lowered
        return None

    def _provider_object(
        self,
        provider_id: str | None,
    ) -> NativeGeminiProvider | CodexCLIProvider | None:
        normalized = self._normalize_provider_id(provider_id)
        if normalized == "gemini":
            return self.providers.gemini
        if normalized == "codex":
            return self.providers.codex
        if normalized == "hosted":
            return self.providers.hosted
        return None

    def _default_model_for_provider(self, provider_id: str | None) -> str | None:
        normalized = self._normalize_provider_id(provider_id)
        if normalized == "gemini":
            return self.settings.gemini_model_president
        if normalized == "codex":
            return self.settings.codex_model
        if normalized == "hosted":
            return self.settings.llm_hosted_model
        return None

    def _suggested_models_for_provider(self, provider_id: str | None) -> list[str]:
        normalized = self._normalize_provider_id(provider_id)
        if normalized == "gemini":
            return _ordered_unique(
                [
                    self.settings.gemini_model_president,
                    self.settings.gemini_model_trader,
                    self.settings.gemini_model_researcher,
                    self.settings.gemini_model_risk_officer,
                    self.settings.gemini_model_ops_monitor,
                    self.settings.gemini_model_memory_librarian,
                    "gemini-2.5-pro",
                    "gemini-2.5-flash",
                ]
            )
        if normalized == "codex":
            return _ordered_unique([self.settings.codex_model])
        if normalized == "hosted":
            return _ordered_unique([self.settings.llm_hosted_model])
        return []

    def _provider_options(self) -> list[dict[str, Any]]:
        options: list[dict[str, Any]] = []
        for provider_id in STRATEGY_PROVIDER_PREFERENCE:
            if self._provider_object(provider_id) is None:
                continue
            options.append(
                {
                    "id": provider_id,
                    "label": STRATEGY_PROVIDER_LABELS[provider_id],
                    "default_model": self._default_model_for_provider(provider_id),
                    "suggested_models": self._suggested_models_for_provider(provider_id),
                }
            )
        return options

    def _preferred_provider_id(self) -> str | None:
        options = self._provider_options()
        if not options:
            return None
        return str(options[0]["id"])

    def _resolve_provider_config(
        self,
        *,
        requested_provider: str | None,
        requested_model: str | None,
    ) -> tuple[str, NativeGeminiProvider | CodexCLIProvider, str]:
        provider_id = self._normalize_provider_id(requested_provider) or self._preferred_provider_id()
        if provider_id is None:
            raise RuntimeError("No strategy lab provider is configured")
        provider = self._provider_object(provider_id)
        if provider is None:
            raise ValueError(f"Strategy provider {provider_id} is unavailable")
        model = _clean_text(requested_model) or self._default_model_for_provider(provider_id)
        if model is None:
            raise ValueError(f"No default model configured for {provider_id}")
        return provider_id, provider, model

    async def mark_stale_runs_failed(self) -> int:
        stale_before = datetime.now(UTC) - CODEX_RUN_STALE_AFTER
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            rows = await repo.fail_stale_strategy_codex_runs(
                stale_before=stale_before,
                error_text="Strategy lab run expired before completion.",
            )
            await session.commit()
        return len(rows)

    async def dashboard_payload(self) -> dict[str, Any]:
        await self.mark_stale_runs_failed()
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            recent_runs = await repo.list_strategy_codex_runs(limit=CODEX_RECENT_RUN_LIMIT)
            strategies = await repo.list_strategies(active_only=False)
            await session.commit()

        inactive_codex_strategies = [
            {
                "name": strategy.name,
                "description": strategy.description,
                "created_at": strategy.created_at.isoformat(),
                "labels": list((strategy.strategy_metadata or {}).get("labels") or []),
                "rationale": (strategy.strategy_metadata or {}).get("rationale"),
                "source_run_id": (strategy.strategy_metadata or {}).get("source_run_id"),
            }
            for strategy in strategies
            if not strategy.is_active and strategy.source in LEGACY_STRATEGY_LAB_SOURCES
        ]

        provider_options = self._provider_options()
        default_provider = provider_options[0] if provider_options else None
        return {
            "available": bool(default_provider),
            "provider": default_provider["id"] if default_provider else "unavailable",
            "provider_label": default_provider["label"] if default_provider else "Unavailable",
            "model": default_provider["default_model"] if default_provider else None,
            "provider_options": provider_options,
            "creation_window_days": CODEX_CREATION_WINDOW_DAYS,
            "recent_runs": [self._compact_run_view(record) for record in recent_runs],
            "inactive_codex_strategies": inactive_codex_strategies,
        }

    async def create_run(
        self,
        *,
        request: StrategyCodexRunRequest,
        dashboard_snapshot: dict[str, Any],
        trigger_source: str = "manual",
    ) -> dict[str, Any]:
        if not self.is_available():
            raise RuntimeError("Strategy lab provider is not available")
        if trigger_source not in CODEX_TRIGGER_SOURCES:
            raise ValueError(f"Unsupported codex trigger source: {trigger_source}")
        provider_id, _provider, model = self._resolve_provider_config(
            requested_provider=request.provider,
            requested_model=request.model,
        )
        request_payload = request.model_dump(mode="json")
        request_payload["provider"] = provider_id
        request_payload["model"] = model
        compact_snapshot = self._compact_dashboard_snapshot(dashboard_snapshot)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.create_strategy_codex_run(
                mode=request.mode,
                status="queued",
                trigger_source=trigger_source,
                window_days=request.window_days,
                series_ticker=request.series_ticker,
                strategy_name=request.strategy_name,
                operator_brief=request.operator_brief,
                provider=provider_id,
                model=model,
                payload={
                    "request": request_payload,
                    "snapshot": compact_snapshot,
                },
            )
            await session.commit()
        return {"run_id": run.id, "status": run.status}

    async def create_and_execute_run(
        self,
        *,
        request: StrategyCodexRunRequest,
        dashboard_snapshot: dict[str, Any],
        trigger_source: str = "manual",
    ) -> dict[str, Any] | None:
        run = await self.create_run(
            request=request,
            dashboard_snapshot=dashboard_snapshot,
            trigger_source=trigger_source,
        )
        await self.execute_run(run["run_id"])
        return await self.get_run_view(run["run_id"])

    async def execute_modes_for_snapshot(
        self,
        *,
        modes: list[str],
        dashboard_snapshot: dict[str, Any],
        window_days: int,
        trigger_source: str = "manual",
        series_ticker: str | None = None,
        strategy_name: str | None = None,
        operator_brief: str | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for mode in modes:
            request = StrategyCodexRunRequest(
                mode=mode,
                window_days=window_days,
                series_ticker=series_ticker,
                strategy_name=strategy_name,
                operator_brief=operator_brief,
            )
            run_view = await self.create_and_execute_run(
                request=request,
                dashboard_snapshot=dashboard_snapshot,
                trigger_source=trigger_source,
            )
            if run_view is not None:
                results.append(run_view)
        return results

    async def execute_run(self, run_id: str) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.get_strategy_codex_run(run_id)
            if run is None or run.status != "queued":
                await session.commit()
                return
            payload = dict(run.payload or {})
            await repo.update_strategy_codex_run(
                run_id,
                status="running",
                payload=payload,
                error_text=None,
                started_at=datetime.now(UTC),
            )
            await session.commit()

        try:
            result = await self._run_mode(run_id)
        except Exception as exc:
            logger.warning("strategy codex run failed", exc_info=True, extra={"run_id": run_id})
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                run = await repo.get_strategy_codex_run(run_id)
                if run is not None:
                    payload = dict(run.payload or {})
                    await repo.update_strategy_codex_run(
                        run_id,
                        status="failed",
                        payload=payload,
                        error_text=str(exc),
                        finished_at=datetime.now(UTC),
                    )
                await session.commit()
            return

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.get_strategy_codex_run(run_id)
            if run is None:
                await session.commit()
                return
            payload = dict(run.payload or {})
            payload["result"] = _json_safe(result)
            await repo.update_strategy_codex_run(
                run_id,
                status="completed",
                payload=payload,
                error_text=None,
                finished_at=datetime.now(UTC),
            )
            await session.commit()

    async def get_run_view(self, run_id: str) -> dict[str, Any] | None:
        await self.mark_stale_runs_failed()
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.get_strategy_codex_run(run_id)
            if run is None:
                await session.commit()
                return None
            payload = dict(run.payload or {})
            saved_name = payload.get("saved_strategy_name")
            saved_strategy = await repo.get_strategy_by_name(saved_name) if isinstance(saved_name, str) and saved_name else None
            await session.commit()

        result = payload.get("result")
        creation_allowed = run.window_days == CODEX_CREATION_WINDOW_DAYS
        backtest_ok = isinstance(result, dict) and ((result.get("backtest") or {}).get("status") == "ok")
        return {
            "id": run.id,
            "mode": run.mode,
            "status": run.status,
            "trigger_source": run.trigger_source,
            "window_days": run.window_days,
            "series_ticker": run.series_ticker,
            "strategy_name": run.strategy_name,
            "operator_brief": run.operator_brief,
            "provider": self._normalize_provider_id(run.provider) or run.provider,
            "model": run.model,
            "created_at": run.created_at.isoformat(),
            "updated_at": run.updated_at.isoformat(),
            "started_at": run.started_at.isoformat() if run.started_at is not None else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
            "error_text": run.error_text,
            "result": result,
            "saved_strategy_name": saved_strategy.name if saved_strategy is not None else saved_name,
            "saved_strategy_active": saved_strategy.is_active if saved_strategy is not None else False,
            "can_accept": (
                run.mode == "suggest"
                and run.status == "completed"
                and creation_allowed
                and backtest_ok
                and saved_strategy is None
            ),
            "accept_disabled_reason": None
            if creation_allowed
            else f"Saving new presets is limited to the {CODEX_CREATION_WINDOW_DAYS}d window.",
            "can_activate": saved_strategy is not None and not saved_strategy.is_active,
        }

    async def accept_run(self, run_id: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.get_strategy_codex_run(run_id)
            if run is None:
                raise KeyError(f"Strategy codex run {run_id} not found")
            if run.mode != "suggest" or run.status != "completed":
                raise ValueError("Only completed suggestion runs can be accepted")
            if run.window_days != CODEX_CREATION_WINDOW_DAYS:
                raise ValueError(f"Suggestion saving is limited to {CODEX_CREATION_WINDOW_DAYS}d runs")
            payload = dict(run.payload or {})
            if payload.get("saved_strategy_name"):
                saved_strategy = await repo.get_strategy_by_name(str(payload["saved_strategy_name"]))
                await session.commit()
                return {
                    "status": "already_accepted",
                    "strategy_name": saved_strategy.name if saved_strategy is not None else payload["saved_strategy_name"],
                    "is_active": bool(saved_strategy.is_active) if saved_strategy is not None else False,
                }

            result = dict(payload.get("result") or {})
            candidate = dict(result.get("candidate") or {})
            backtest = dict(result.get("backtest") or {})
            if backtest.get("status") != "ok":
                raise ValueError("Only suggestion runs with a deterministic backtest can be accepted")
            try:
                candidate_payload = StrategyCodexSuggestionPayload.model_validate(candidate)
            except ValidationError as exc:
                raise ValueError("Completed run does not contain a valid suggestion candidate") from exc

            requested_name = _clean_text(candidate_payload.name) or "strategy-lab"
            strategy_name = await self._unique_strategy_name(repo, requested_name)
            strategy = await repo.create_strategy(
                name=strategy_name,
                description=_clean_text(candidate_payload.description),
                thresholds=candidate_payload.thresholds.model_dump(mode="json"),
                is_active=False,
                source=STRATEGY_LAB_SOURCE,
                metadata={
                    "labels": list(candidate_payload.labels),
                    "rationale": candidate_payload.rationale,
                    "source_run_id": run.id,
                    "backtest_summary": backtest.get("summary"),
                },
            )
            candidate_rows = [
                {**row, "strategy_id": strategy.id}
                for row in list(backtest.get("candidate_result_rows") or [])
            ]
            if candidate_rows:
                await repo.save_strategy_results(candidate_rows)

            payload["saved_strategy_name"] = strategy.name
            await repo.update_strategy_codex_run(run.id, payload=payload)
            await session.commit()

        return {"status": "accepted", "strategy_name": strategy.name, "is_active": False}

    async def activate_strategy(self, strategy_name: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            strategy = await repo.set_strategy_active(strategy_name, is_active=True)
            if strategy is None:
                raise KeyError(f"Strategy {strategy_name} not found")
            await session.commit()
        return {"status": "activated", "strategy_name": strategy.name, "is_active": True}

    async def _run_mode(self, run_id: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.get_strategy_codex_run(run_id)
            if run is None:
                raise KeyError(f"Strategy codex run {run_id} not found")
            payload = dict(run.payload or {})
            snapshot = dict(payload.get("snapshot") or {})
            active_rows = await repo.list_strategies(active_only=True)
            await session.commit()

        if run.mode == "evaluate":
            _provider_id, provider, model = self._resolve_provider_config(
                requested_provider=run.provider,
                requested_model=run.model,
            )
            return await self._evaluate_snapshot(run=run, snapshot=snapshot, provider=provider, model=model)
        _provider_id, provider, model = self._resolve_provider_config(
            requested_provider=run.provider,
            requested_model=run.model,
        )
        return await self._suggest_strategy(run=run, snapshot=snapshot, active_rows=active_rows, provider=provider, model=model)

    async def _evaluate_snapshot(self, *, run, snapshot: dict[str, Any], provider, model: str) -> dict[str, Any]:
        payload = await provider.complete_json(
            system_prompt=(
                "You are a strategy evaluator for a Kalshi trading dashboard. "
                "Review only the supplied strategy snapshot. "
                "Do not invent data, do not propose code changes, and stay grounded in the given metrics."
            ),
            user_prompt=json.dumps(
                {
                    "task": "Evaluate the current strategy landscape and explain the most important strengths, risks, and next actions.",
                    "window_days": run.window_days,
                    "selected_series_ticker": run.series_ticker,
                    "selected_strategy_name": run.strategy_name,
                    "operator_brief": run.operator_brief,
                    "snapshot": snapshot,
                },
                indent=2,
            ),
            model=model,
            temperature=0.2,
            schema_model=StrategyCodexEvaluationPayload,
        )
        return {
            "kind": "evaluate",
            "evaluation": payload,
        }

    async def _suggest_strategy(self, *, run, snapshot: dict[str, Any], active_rows: list[Any], provider, model: str) -> dict[str, Any]:
        suggestion = await provider.complete_json(
            system_prompt=(
                "You design one new threshold-based strategy preset for a Kalshi strategy regression dashboard. "
                "Use only the current threshold schema. "
                "Return one candidate only, do not propose code changes, and do not modify approval or assignment rules."
            ),
            user_prompt=json.dumps(
                {
                    "task": "Suggest one new strategy preset and explain why it could complement the current presets.",
                    "window_days": run.window_days,
                    "selected_series_ticker": run.series_ticker,
                    "selected_strategy_name": run.strategy_name,
                    "operator_brief": run.operator_brief,
                    "creation_window_days": CODEX_CREATION_WINDOW_DAYS,
                    "snapshot": snapshot,
                },
                indent=2,
            ),
            model=model,
            temperature=0.2,
            schema_model=StrategyCodexSuggestionPayload,
        )
        candidate = StrategyCodexSuggestionPayload.model_validate(suggestion)
        candidate_spec = RegressionStrategySpec(
            id=None,
            name=_clean_text(candidate.name) or "strategy-candidate",
            description=_clean_text(candidate.description),
            thresholds=candidate.thresholds.model_dump(mode="json"),
        )
        backtest = await self.strategy_regression_service.evaluate_strategy_specs(
            strategies=[
                *[
                    RegressionStrategySpec(
                        id=row.id,
                        name=row.name,
                        description=row.description,
                        thresholds=row.thresholds,
                    )
                    for row in active_rows
                ],
                candidate_spec,
            ],
            window_days=run.window_days,
        )
        return {
            "kind": "suggest",
            "candidate": {
                "name": candidate_spec.name,
                "description": candidate_spec.description,
                "labels": list(candidate.labels),
                "rationale": candidate.rationale,
                "thresholds": candidate_spec.thresholds,
            },
            "backtest": self._summarize_backtest(
                evaluation=backtest,
                candidate_name=candidate_spec.name,
                series_ticker=run.series_ticker,
                compare_strategy_name=run.strategy_name,
            ),
        }

    def _summarize_backtest(
        self,
        *,
        evaluation: dict[str, Any],
        candidate_name: str,
        series_ticker: str | None,
        compare_strategy_name: str | None,
    ) -> dict[str, Any]:
        if evaluation.get("status") != "ok":
            return {
                "status": evaluation.get("status") or "failed",
                "summary": "No deterministic backtest was available for this candidate in the selected window.",
                "candidate_result_rows": [],
            }

        leaderboard = list(evaluation.get("leaderboard") or [])
        candidate_row = next((row for row in leaderboard if row.get("name") == candidate_name), None)
        candidate_rank = next(
            (index + 1 for index, row in enumerate(leaderboard) if row.get("name") == candidate_name),
            None,
        )
        city_results = dict(evaluation.get("city_results") or {})
        candidate_city_rows: list[dict[str, Any]] = []
        for city_key, results in city_results.items():
            candidate_city = dict((results or {}).get(candidate_name) or {})
            if not candidate_city:
                continue
            candidate_city_rows.append({
                "series_ticker": city_key,
                "win_rate": candidate_city.get("win_rate"),
                "win_rate_display": _ratio_display(candidate_city.get("win_rate")),
                "resolved_trade_count": candidate_city.get("resolved_trade_count") or 0,
                "resolved_trade_count_display": str(candidate_city.get("resolved_trade_count") or 0),
                "trade_count": candidate_city.get("trade_count") or 0,
                "trade_count_display": str(candidate_city.get("trade_count") or 0),
                "outcome_coverage_display": _coverage_display(
                    int(candidate_city.get("resolved_trade_count") or 0),
                    int(candidate_city.get("trade_count") or 0),
                ),
                "total_pnl_dollars": candidate_city.get("total_pnl_dollars"),
                "total_pnl_display": _money_display(candidate_city.get("total_pnl_dollars")),
            })
        candidate_city_rows.sort(
            key=lambda row: (
                row["win_rate"] if row["win_rate"] is not None else -1.0,
                row["resolved_trade_count"],
                row["total_pnl_dollars"] if row["total_pnl_dollars"] is not None else float("-inf"),
            ),
            reverse=True,
        )

        selected_city = None
        if series_ticker and series_ticker in city_results:
            ranked_rows = _rank_scored_strategy_rows(list((city_results.get(series_ticker) or {}).values()))
            candidate_city = dict((city_results.get(series_ticker) or {}).get(candidate_name) or {})
            if candidate_city:
                candidate_city_rank = next(
                    (index + 1 for index, row in enumerate(ranked_rows) if row.get("strategy_name") == candidate_name),
                    None,
                )
                selected_city = {
                    "series_ticker": series_ticker,
                    "candidate_rank": candidate_city_rank,
                    "leader": ranked_rows[0]["strategy_name"] if ranked_rows else None,
                    "candidate_win_rate": candidate_city.get("win_rate"),
                    "candidate_win_rate_display": _ratio_display(candidate_city.get("win_rate")),
                    "candidate_resolved_trade_count": candidate_city.get("resolved_trade_count") or 0,
                    "candidate_outcome_coverage_display": _coverage_display(
                        int(candidate_city.get("resolved_trade_count") or 0),
                        int(candidate_city.get("trade_count") or 0),
                    ),
                }

        compare_strategy = next((row for row in leaderboard if row.get("name") == compare_strategy_name), None)
        candidate_result_rows = [
            row for row in list(evaluation.get("result_rows") or []) if row.get("strategy_name") == candidate_name
        ]
        return {
            "status": "ok",
            "summary": (
                f"{candidate_name} ranks #{candidate_rank or '—'} of {len(leaderboard)} strategies "
                f"on the {evaluation.get('window_days')}d backtest."
            ),
            "candidate_rank": candidate_rank,
            "strategy_count": len(leaderboard),
            "candidate_metrics": {
                "overall_win_rate": candidate_row.get("overall_win_rate") if candidate_row is not None else None,
                "overall_win_rate_display": _ratio_display(candidate_row.get("overall_win_rate") if candidate_row is not None else None),
                "overall_trade_rate": candidate_row.get("overall_trade_rate") if candidate_row is not None else None,
                "overall_trade_rate_display": _ratio_display(candidate_row.get("overall_trade_rate") if candidate_row is not None else None),
                "outcome_coverage_rate": candidate_row.get("outcome_coverage_rate") if candidate_row is not None else None,
                "outcome_coverage_rate_display": _ratio_display(candidate_row.get("outcome_coverage_rate") if candidate_row is not None else None),
                "total_pnl_dollars": candidate_row.get("total_pnl_dollars") if candidate_row is not None else None,
                "total_pnl_display": _money_display(candidate_row.get("total_pnl_dollars") if candidate_row is not None else None),
                "avg_edge_bps": candidate_row.get("avg_edge_bps") if candidate_row is not None else None,
                "avg_edge_bps_display": _bps_display(candidate_row.get("avg_edge_bps") if candidate_row is not None else None),
                "cities_led": candidate_row.get("cities_led") if candidate_row is not None else 0,
                "total_resolved_trade_count": candidate_row.get("total_resolved_trade_count") if candidate_row is not None else 0,
            },
            "top_strategies": leaderboard[:5],
            "compare_strategy": compare_strategy,
            "selected_city": selected_city,
            "strongest_cities": candidate_city_rows[:3],
            "weakest_cities": list(reversed(candidate_city_rows[-3:])) if candidate_city_rows else [],
            "candidate_result_rows": candidate_result_rows,
            "diagnostics": evaluation.get("diagnostics") or {},
            "window_days": evaluation.get("window_days"),
        }

    async def _unique_strategy_name(self, repo: PlatformRepository, requested_name: str) -> str:
        base_name = requested_name.strip()[:64] or "strategy-lab"
        candidate = base_name
        suffix = 2
        while await repo.get_strategy_by_name(candidate) is not None:
            suffix_text = f"-{suffix}"
            candidate = f"{base_name[: 64 - len(suffix_text)]}{suffix_text}"
            suffix += 1
        return candidate

    def _compact_dashboard_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        summary = dict(snapshot.get("summary") or {})
        leaderboard = list(snapshot.get("leaderboard") or [])
        city_matrix = list(snapshot.get("city_matrix") or [])
        detail_context = dict(snapshot.get("detail_context") or {})
        recent_promotions = list(snapshot.get("recent_promotions") or [])
        methodology = dict(snapshot.get("methodology") or {})

        compact_rows = [
            {
                "series_ticker": row.get("series_ticker"),
                "city_label": row.get("city_label"),
                "location_name": row.get("location_name"),
                "assignment": (row.get("assignment") or {}).get("strategy_name"),
                "assignment_context_status": row.get("assignment_context_status"),
                "best_strategy": row.get("best_strategy"),
                "runner_up_strategy": row.get("runner_up_strategy"),
                "gap_to_runner_up": row.get("gap_to_runner_up"),
                "gap_to_assignment": row.get("gap_to_assignment"),
                "recommendation": {
                    "strategy_name": ((row.get("recommendation") or {}).get("strategy_name")),
                    "status": ((row.get("recommendation") or {}).get("status")),
                    "resolved_trade_count": ((row.get("recommendation") or {}).get("resolved_trade_count")),
                },
                "review": {
                    "status": ((row.get("review") or {}).get("status")),
                    "needs_review": ((row.get("review") or {}).get("needs_review")),
                },
            }
            for row in city_matrix
        ]
        compact_leaderboard = [
            {
                "name": row.get("name"),
                "description": row.get("description"),
                "overall_win_rate": row.get("overall_win_rate"),
                "overall_trade_rate": row.get("overall_trade_rate"),
                "outcome_coverage_rate": row.get("outcome_coverage_rate"),
                "total_pnl_dollars": row.get("total_pnl_dollars"),
                "avg_edge_bps": row.get("avg_edge_bps"),
                "cities_led": row.get("cities_led"),
                "assigned_city_count": row.get("assigned_city_count"),
                "thresholds": row.get("thresholds"),
            }
            for row in leaderboard
        ]
        compact_detail = {
            "type": detail_context.get("type"),
            "selected_series_ticker": detail_context.get("selected_series_ticker"),
            "selected_strategy_name": detail_context.get("selected_strategy_name"),
            "city": (
                {
                    "series_ticker": ((detail_context.get("city") or {}).get("series_ticker")),
                    "assignment": (((detail_context.get("city") or {}).get("assignment") or {}).get("strategy_name")),
                    "best_strategy": ((detail_context.get("city") or {}).get("best_strategy")),
                    "runner_up_strategy": ((detail_context.get("city") or {}).get("runner_up_strategy")),
                    "gap_to_runner_up": ((detail_context.get("city") or {}).get("gap_to_runner_up")),
                    "gap_to_assignment": ((detail_context.get("city") or {}).get("gap_to_assignment")),
                }
                if detail_context.get("type") == "city"
                else None
            ),
            "strategy": (
                {
                    "name": ((detail_context.get("strategy") or {}).get("name")),
                    "overall_win_rate": ((detail_context.get("strategy") or {}).get("overall_win_rate")),
                    "outcome_coverage_rate": ((detail_context.get("strategy") or {}).get("outcome_coverage_rate")),
                    "total_pnl_dollars": ((detail_context.get("strategy") or {}).get("total_pnl_dollars")),
                    "thresholds": ((detail_context.get("strategy") or {}).get("thresholds")),
                }
                if detail_context.get("type") == "strategy"
                else None
            ),
        }
        return {
            "summary": {
                "window_days": summary.get("window_days"),
                "window_display": summary.get("window_display"),
                "best_strategy_name": summary.get("best_strategy_name"),
                "best_strategy_win_rate": summary.get("best_strategy_win_rate"),
                "last_regression_run": summary.get("last_regression_run"),
                "assignments_covered_display": summary.get("assignments_covered_display"),
            },
            "leaderboard": compact_leaderboard,
            "city_matrix": compact_rows,
            "detail_context": compact_detail,
            "recent_promotions": recent_promotions[:6],
            "methodology": {
                "points": list(methodology.get("points") or [])[:6],
                "recommendation_trade_threshold": methodology.get("recommendation_trade_threshold"),
                "recommendation_outcome_coverage_threshold": methodology.get("recommendation_outcome_coverage_threshold"),
                "recommendation_lean_gap_threshold": methodology.get("recommendation_lean_gap_threshold"),
                "recommendation_strong_gap_threshold": methodology.get("recommendation_strong_gap_threshold"),
            },
        }

    def _compact_run_view(self, record) -> dict[str, Any]:
        payload = dict(record.payload or {})
        result = dict(payload.get("result") or {})
        summary = None
        if record.mode == "evaluate":
            summary = (result.get("evaluation") or {}).get("summary")
        elif record.mode == "suggest":
            if payload.get("saved_strategy_name"):
                summary = f"Saved as inactive preset {payload['saved_strategy_name']}."
            else:
                summary = (result.get("backtest") or {}).get("summary")
        return {
            "id": record.id,
            "mode": record.mode,
            "status": record.status,
            "trigger_source": record.trigger_source,
            "window_days": record.window_days,
            "series_ticker": record.series_ticker,
            "strategy_name": record.strategy_name,
            "provider": self._normalize_provider_id(record.provider) or record.provider,
            "model": record.model,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
            "summary": summary,
            "saved_strategy_name": payload.get("saved_strategy_name"),
        }
