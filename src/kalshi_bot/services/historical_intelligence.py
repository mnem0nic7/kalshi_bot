from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import (
    ContractSide,
    RoomOrigin,
    StandDownReason,
    StrategyMode,
    WeatherResolutionState,
)
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.core.schemas import (
    HeuristicCalibrationEntry,
    HeuristicPolicyAction,
    HeuristicPolicyCondition,
    HeuristicPolicyNode,
    HistoricalHeuristicPack,
    HistoricalIntelligenceRunRequest,
    ResearchDossier,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.historical_training import HistoricalTrainingService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.signal import (
    StrategySignal,
    apply_heuristic_application_to_signal,
    evaluate_trade_eligibility,
    is_market_stale,
    market_quotes,
    market_spread_bps,
    remaining_payout_dollars,
)
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


@dataclass(slots=True)
class IntelligenceFeatureRow:
    room_id: str
    market_ticker: str
    series_ticker: str
    local_market_day: str | None
    daypart: str
    city_bucket: str
    threshold_bucket: str
    forecast_delta_bucket: str
    spread_regime: str
    payout_bucket: str
    quote_freshness: str
    liquidity_regime: str
    coverage_class: str
    settlement_yes_dollars: Decimal | None
    fair_yes_dollars: Decimal
    calibration_error_bps: int | None
    stand_down_reason: str | None
    eligible: bool
    blocked_by: str | None
    stale_data_mismatch: bool
    missed_stand_down: bool
    weak_trade: bool
    rule_trace: list[dict[str, Any]]
    counterfactual_pnl_dollars: Decimal | None
    profitability_flag: bool | None


class HistoricalIntelligenceService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        heuristic_service: HistoricalHeuristicService,
        research_coordinator: ResearchCoordinator,
        training_export_service: TrainingExportService,
        historical_training_service: HistoricalTrainingService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.heuristic_service = heuristic_service
        self.research_coordinator = research_coordinator
        self.training_export_service = training_export_service
        self.historical_training_service = historical_training_service

    async def get_status(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.heuristic_service.ensure_initialized(repo)
            control = await repo.get_deployment_control()
            active_pack = await self.heuristic_service.get_active_pack(repo)
            candidate_pack = await self.heuristic_service.get_candidate_pack(repo)
            recent_runs = await repo.list_historical_intelligence_runs(limit=10)
            recent_promotions = await repo.list_heuristic_pack_promotions(limit=10)
            patch_suggestions = await repo.list_heuristic_patch_suggestions(limit=10)
            await session.commit()
        latest_run = recent_runs[0].payload if recent_runs else None
        historical_status = await self.historical_training_service.get_status()
        fallback_confidence = self.historical_training_service._confidence_story(  # noqa: SLF001
            latest_run_payload=latest_run,
            historical_build_readiness=historical_status.get("historical_build_readiness") or {},
            source_replay_coverage=historical_status.get("source_replay_coverage") or {},
        )
        return {
            "active_pack_version": active_pack.version,
            "candidate_pack_version": candidate_pack.version if candidate_pack is not None else None,
            "intelligence_window_days": self.settings.historical_intelligence_window_days,
            "latest_run": latest_run,
            "confidence_state": (latest_run or {}).get("confidence_state") or fallback_confidence["confidence_state"],
            "confidence_scorecard": (latest_run or {}).get("confidence_scorecard") or fallback_confidence["confidence_scorecard"],
            "confidence_progress": historical_status.get("confidence_progress") or fallback_confidence["confidence_progress"],
            "historical_build_readiness": historical_status.get("historical_build_readiness"),
            "settlement_mismatch_breakdown": historical_status.get("settlement_mismatch_breakdown") or {},
            "coverage_repair_summary": historical_status.get("coverage_repair_summary") or {},
            "checkpoint_archive_promotion_count": historical_status.get("checkpoint_archive_promotion_count") or 0,
            "replay_refresh_counts_by_cause": historical_status.get("replay_refresh_counts_by_cause") or {},
            "heuristics": self.heuristic_service.status_payload(
                control=control,
                active_pack=active_pack,
                candidate_pack=candidate_pack,
                recent_promotions=recent_promotions,
                recent_runs=recent_runs,
                patch_suggestions=patch_suggestions,
            ),
        }

    async def get_dashboard_status(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.heuristic_service.ensure_initialized(repo)
            active_pack = await self.heuristic_service.get_active_pack(repo)
            candidate_pack = await self.heuristic_service.get_candidate_pack(repo)
            recent_runs = await repo.list_historical_intelligence_runs(limit=1)
            await session.commit()

        latest_run = recent_runs[0].payload if recent_runs else None
        return {
            "active_pack_version": active_pack.version,
            "candidate_pack_version": candidate_pack.version if candidate_pack is not None else None,
            "latest_run": latest_run,
            "confidence_state": (latest_run or {}).get("confidence_state") or "unknown",
        }

    async def explain(self, *, series: list[str] | None = None) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            active_pack = await self.heuristic_service.get_active_pack(repo)
            latest_runs = await repo.list_historical_intelligence_runs(limit=5)
            await session.commit()
        calibrations = [
            entry.model_dump(mode="json")
            for entry in active_pack.calibration_entries
            if not series or entry.series_ticker in set(series)
        ]
        policy_graph = [
            node.model_dump(mode="json")
            for node in active_pack.policy_graph
            if not series or not node.condition.series_tickers or any(item in set(series) for item in node.condition.series_tickers)
        ]
        return {
            "active_pack_version": active_pack.version,
            "series": series or [],
            "agent_summary": active_pack.agent_summary,
            "calibration_entries": calibrations[:20],
            "policy_graph": policy_graph[:20],
            "recent_runs": [record.payload for record in latest_runs],
        }

    async def promote(self, *, candidate_version: str | None, reason: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.heuristic_service.ensure_initialized(repo)
            candidate = (
                await self.heuristic_service.get_pack(repo, candidate_version)
                if candidate_version
                else await self.heuristic_service.get_candidate_pack(repo)
            )
            if candidate is None:
                raise KeyError("No candidate heuristic pack available")
            promotion = await repo.create_heuristic_pack_promotion(
                candidate_version=candidate.version,
                previous_version=(await self.heuristic_service.get_active_pack(repo)).version,
                intelligence_run_id=str(candidate.metadata.get("intelligence_run_id") or "") or None,
                payload={"reason": reason, "manual": True},
                status="staged",
            )
            notes = await self.heuristic_service.promote_candidate(
                repo,
                candidate_version=candidate.version,
                intelligence_run_id=str(candidate.metadata.get("intelligence_run_id") or "") or None,
                payload={"reason": reason, "manual": True},
            )
            await repo.update_heuristic_pack_promotion(
                promotion.id,
                status="promoted",
                payload={"reason": reason, "notes": notes},
            )
            await session.commit()
        return {"status": "promoted", "candidate_version": candidate.version, "reason": reason}

    async def rollback(self, *, reason: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            notes = await self.heuristic_service.rollback(repo, reason=reason)
            promotion = await repo.create_heuristic_pack_promotion(
                candidate_version=notes.get("active_version") or self.settings.active_heuristic_pack_version,
                previous_version=notes.get("previous_version"),
                intelligence_run_id=notes.get("last_intelligence_run_id"),
                payload={"reason": reason, "rollback": True},
                status="rolled_back",
            )
            await repo.update_heuristic_pack_promotion(
                promotion.id,
                status="rolled_back",
                payload={"reason": reason, "notes": notes},
                rollback_reason=reason,
            )
            await session.commit()
        return {"status": "rolled_back", "reason": reason}

    async def run(self, request: HistoricalIntelligenceRunRequest) -> dict[str, Any]:
        date_from = date.fromisoformat(request.date_from)
        date_to = date.fromisoformat(request.date_to)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await self.heuristic_service.ensure_initialized(repo)
            active_pack = await self.heuristic_service.get_active_pack(repo)
            run = await repo.create_historical_intelligence_run(
                date_from=request.date_from,
                date_to=request.date_to,
                active_pack_version=active_pack.version,
                payload={
                    "status": "running",
                    "date_from": request.date_from,
                    "date_to": request.date_to,
                    "origins": request.origins,
                },
            )
            await session.commit()

        try:
            bundles = await self._load_bundles(
                date_from=date_from,
                date_to=date_to,
                origins=request.origins,
            )
            intelligence = self._build_intelligence(bundles)
            split = self.historical_training_service._split_historical_bundles(bundles)  # noqa: SLF001
            sufficiency = self._support_diagnostics(intelligence, bundles=bundles, split=split)
            candidate_pack: HistoricalHeuristicPack | None = None
            candidate_evaluation: dict[str, Any] | None = None
            promoted_pack_version: str | None = None
            patch_payload: dict[str, Any] | None = None

            if intelligence["row_count"] > 0:
                candidate_pack = await self._build_candidate_pack(
                    run_id=run.id,
                    active_pack=active_pack,
                    intelligence=intelligence,
                    sufficiency=sufficiency,
                )
                if candidate_pack is not None:
                    async with self.session_factory() as session:
                        repo = PlatformRepository(session)
                        await self.heuristic_service.save_pack(repo, candidate_pack)
                        await self.heuristic_service.stage_candidate(
                            repo,
                            candidate_version=candidate_pack.version,
                            intelligence_run_id=run.id,
                            payload={"source_window": {"date_from": request.date_from, "date_to": request.date_to}},
                        )
                        await session.commit()
                    candidate_evaluation = await self._evaluate_candidate(
                        bundles=bundles,
                        active_pack=active_pack,
                        candidate_pack=candidate_pack,
                    )
                    patch_payload = self._patch_suggestion_payload(
                        active_pack=active_pack,
                        candidate_pack=candidate_pack,
                        intelligence=intelligence,
                        evaluation=candidate_evaluation,
                    )
                    async with self.session_factory() as session:
                        repo = PlatformRepository(session)
                        promotion = await repo.create_heuristic_pack_promotion(
                            candidate_version=candidate_pack.version,
                            previous_version=active_pack.version,
                            intelligence_run_id=run.id,
                            payload={"evaluation": candidate_evaluation},
                            status="staged",
                        )
                        await repo.create_heuristic_patch_suggestion(
                            heuristic_pack_version=candidate_pack.version,
                            intelligence_run_id=run.id,
                            status="candidate",
                            payload=patch_payload,
                        )
                        if request.auto_promote and self.settings.historical_intelligence_auto_promote and candidate_evaluation["promotable"]:
                            await self.heuristic_service.promote_candidate(
                                repo,
                                candidate_version=candidate_pack.version,
                                intelligence_run_id=run.id,
                                payload={"evaluation": candidate_evaluation, "source": "auto_promote"},
                            )
                            await repo.update_heuristic_pack_promotion(
                                promotion.id,
                                status="promoted",
                                payload={"evaluation": candidate_evaluation, "source": "auto_promote"},
                            )
                            promoted_pack_version = candidate_pack.version
                        else:
                            await repo.update_heuristic_pack_promotion(
                                promotion.id,
                                status="rejected" if not candidate_evaluation["promotable"] else "staged",
                                payload={"evaluation": candidate_evaluation},
                            )
                        await session.commit()

            payload = {
                "status": "completed",
                "date_from": request.date_from,
                "date_to": request.date_to,
                "origins": request.origins,
                "row_count": intelligence["row_count"],
                "support_diagnostics": sufficiency,
                "execution_intelligence": intelligence["execution_intelligence"],
                "directional_intelligence": intelligence["directional_intelligence"],
                "rule_synthesis_candidates": intelligence["rule_synthesis_candidates"],
                "confidence_scorecard": sufficiency["confidence_scorecard"],
                "confidence_state": sufficiency["confidence_state"],
                "candidate_pack_version": candidate_pack.version if candidate_pack is not None else None,
                "promoted_pack_version": promoted_pack_version,
                "evaluation": candidate_evaluation,
                "patch_suggestion": patch_payload,
            }
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_intelligence_run(
                    run.id,
                    status="completed",
                    payload=payload,
                    room_count=intelligence["row_count"],
                    candidate_pack_version=candidate_pack.version if candidate_pack is not None else None,
                    promoted_pack_version=promoted_pack_version,
                )
                await session.commit()
            return payload
        except Exception as exc:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_intelligence_run(
                    run.id,
                    status="failed",
                    payload={"error": str(exc)},
                    room_count=0,
                    error_text=str(exc),
                )
                await session.commit()
            raise

    async def _load_bundles(
        self,
        *,
        date_from: date,
        date_to: date,
        origins: list[str],
    ) -> list[Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            replay_runs = await repo.list_historical_replay_runs(
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                status="completed",
                limit=5000,
            )
            await session.commit()
        room_ids = [record.room_id for record in replay_runs if record.room_id]
        bundles = await self.training_export_service.export_room_bundles(
            room_ids=room_ids,
            limit=len(room_ids),
            include_non_complete=False,
            origins=origins or [RoomOrigin.HISTORICAL_REPLAY.value],
        )
        bundles = await self.historical_training_service._hydrate_historical_bundle_coverage(bundles)
        return self.historical_training_service._filter_historical_bundles(
            bundles,
            quality_cleaned_only=True,
            include_pathology_examples=False,
            require_full_checkpoints=False,
            late_only_ok=True,
            mode="outcome-eval",
        )

    def _build_intelligence(self, bundles: list[Any]) -> dict[str, Any]:
        rows = [row for row in (self._feature_row(bundle) for bundle in bundles) if row is not None]
        calibration_rollups: dict[tuple[str, str, str, str, str], list[int]] = defaultdict(list)
        spread_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        payout_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        freshness_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        liquidity_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        stand_down_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        regime_rollups: dict[str, list[IntelligenceFeatureRow]] = defaultdict(list)
        recurrence_rollups: dict[str, Counter[str]] = defaultdict(Counter)
        brier_rollups: dict[tuple[str, str, str], list[float]] = defaultdict(list)

        for row in rows:
            if row.calibration_error_bps is not None and row.coverage_class == HistoricalTrainingService.COVERAGE_FULL:
                calibration_rollups[
                    (
                        row.city_bucket,
                        row.series_ticker,
                        row.threshold_bucket,
                        row.daypart,
                        row.forecast_delta_bucket,
                    )
                ].append(row.calibration_error_bps)
            if row.settlement_yes_dollars is not None and row.coverage_class == HistoricalTrainingService.COVERAGE_FULL:
                brier_rollups[(row.city_bucket, row.threshold_bucket, row.daypart)].append(
                    float((row.fair_yes_dollars - row.settlement_yes_dollars) ** 2)
                )
            spread_rollups[row.spread_regime].append(row)
            payout_rollups[row.payout_bucket].append(row)
            freshness_rollups[row.quote_freshness].append(row)
            liquidity_rollups[row.liquidity_regime].append(row)
            if row.stand_down_reason:
                stand_down_rollups[row.stand_down_reason].append(row)
            regime_rollups[f"{row.daypart}:{row.spread_regime}:{row.coverage_class}"].append(row)
            if row.stale_data_mismatch:
                recurrence_rollups[row.series_ticker]["stale_data_mismatch"] += 1
            if row.missed_stand_down:
                recurrence_rollups[row.series_ticker]["missed_stand_down"] += 1

        execution_intelligence = {
            "spread_regimes": {
                regime: self._execution_regime_summary(items)
                for regime, items in spread_rollups.items()
            },
            "payout_buckets": {
                bucket: self._execution_regime_summary(items)
                for bucket, items in payout_rollups.items()
            },
            "freshness_regimes": {
                regime: self._execution_regime_summary(items)
                for regime, items in freshness_rollups.items()
            },
            "liquidity_regimes": {
                regime: self._execution_regime_summary(items)
                for regime, items in liquidity_rollups.items()
            },
            "stand_down_reasons": {
                reason: self._stand_down_summary(items)
                for reason, items in stand_down_rollups.items()
            },
            "recurrence_patterns": {
                series_ticker: dict(counter)
                for series_ticker, counter in recurrence_rollups.items()
            },
            "regime_outcome_curves": {
                regime: self._regime_outcome_summary(items)
                for regime, items in regime_rollups.items()
            },
        }
        directional_entries = []
        for key, errors in calibration_rollups.items():
            city_bucket, series_ticker, threshold_bucket, daypart, forecast_delta_bucket = key
            support_count = len(errors)
            mean_error_bps = sum(errors) / support_count if support_count else 0.0
            directional_entries.append(
                {
                    "city_bucket": city_bucket,
                    "series_ticker": series_ticker,
                    "threshold_bucket": threshold_bucket,
                    "daypart": daypart,
                    "forecast_delta_bucket": forecast_delta_bucket,
                    "support_count": support_count,
                    "mean_error_bps": mean_error_bps,
                    "fair_yes_adjust_bps": int(round(-mean_error_bps)),
                }
            )
        directional_entries.sort(key=lambda item: (-item["support_count"], abs(item["mean_error_bps"])))
        brier_entries = [
            {
                "city_bucket": city_bucket,
                "threshold_bucket": threshold_bucket,
                "daypart": daypart,
                "support_count": len(values),
                "mean_brier_error": (sum(values) / len(values)) if values else 0.0,
            }
            for (city_bucket, threshold_bucket, daypart), values in brier_rollups.items()
        ]
        brier_entries.sort(key=lambda item: (-item["support_count"], item["mean_brier_error"]))
        rule_synthesis_candidates = self._rule_synthesis_candidates(rows)

        return {
            "row_count": len(rows),
            "rows": rows,
            "execution_intelligence": execution_intelligence,
            "directional_intelligence": {
                "calibration_entries": directional_entries[:50],
                "brier_scorecard": brier_entries[:50],
                "full_coverage_row_count": sum(
                    1 for row in rows if row.coverage_class == HistoricalTrainingService.COVERAGE_FULL
                ),
            },
            "rule_synthesis_candidates": rule_synthesis_candidates,
        }

    def _support_diagnostics(
        self,
        intelligence: dict[str, Any],
        *,
        bundles: list[Any],
        split,
    ) -> dict[str, Any]:
        rows: list[IntelligenceFeatureRow] = intelligence["rows"]
        distinct_full_market_days = {
            row.local_market_day for row in rows
            if row.local_market_day and row.coverage_class == HistoricalTrainingService.COVERAGE_FULL
        }
        distinct_execution_market_days = {
            row.local_market_day
            for row in rows
            if row.local_market_day and row.coverage_class in {
                HistoricalTrainingService.COVERAGE_FULL,
                HistoricalTrainingService.COVERAGE_LATE_ONLY,
                HistoricalTrainingService.COVERAGE_PARTIAL,
            }
        }
        holdout_ids = set(split.holdout or [])
        holdout_full_market_days = {
            str((bundle.historical_provenance or {}).get("local_market_day") or "")
            for bundle in bundles
            if bundle.room["id"] in holdout_ids
            and (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == HistoricalTrainingService.COVERAGE_FULL
            and (bundle.historical_provenance or {}).get("local_market_day")
        }
        coverage_counts = Counter(
            row.coverage_class or HistoricalTrainingService.COVERAGE_NONE
            for row in rows
        )
        execution_confident = (
            len(distinct_execution_market_days) >= self.settings.historical_execution_confidence_min_market_days
        )
        directional_confident = (
            execution_confident
            and
            len(distinct_full_market_days) >= self.settings.historical_directional_confidence_min_full_market_days
            and len(holdout_full_market_days) >= self.settings.historical_directional_confidence_min_holdout_market_days
        )
        confidence_state = (
            "directional_confident"
            if directional_confident
            else "execution_confident_only"
            if execution_confident
            else "insufficient_support"
        )
        confidence_scorecard = {
            "confidence_state": confidence_state,
            "support_counts_by_coverage_class": {
                HistoricalTrainingService.COVERAGE_FULL: coverage_counts.get(HistoricalTrainingService.COVERAGE_FULL, 0),
                HistoricalTrainingService.COVERAGE_LATE_ONLY: coverage_counts.get(HistoricalTrainingService.COVERAGE_LATE_ONLY, 0),
                HistoricalTrainingService.COVERAGE_PARTIAL: coverage_counts.get(HistoricalTrainingService.COVERAGE_PARTIAL, 0),
                HistoricalTrainingService.COVERAGE_OUTCOME_ONLY: 0,
                HistoricalTrainingService.COVERAGE_NONE: coverage_counts.get(HistoricalTrainingService.COVERAGE_NONE, 0),
            },
            "distinct_full_market_days": len(distinct_full_market_days),
            "distinct_execution_market_days": len(distinct_execution_market_days),
            "full_coverage_holdout_market_days": len(holdout_full_market_days),
            "execution_confident": execution_confident,
            "directional_confident": directional_confident,
            "execution_confidence_threshold_market_days": self.settings.historical_execution_confidence_min_market_days,
            "directional_confidence_threshold_market_days": self.settings.historical_directional_confidence_min_full_market_days,
            "directional_confidence_threshold_holdout_market_days": self.settings.historical_directional_confidence_min_holdout_market_days,
            "learning_lane_support": {
                "directional_market_days": len(distinct_full_market_days),
                "execution_market_days": len(distinct_execution_market_days),
            },
        }
        return {
            "row_count": len(rows),
            "distinct_full_market_days": len(distinct_full_market_days),
            "distinct_execution_market_days": len(distinct_execution_market_days),
            "full_coverage_holdout_market_days": len(holdout_full_market_days),
            "min_directional_support_met": len(distinct_full_market_days) >= self.settings.historical_intelligence_min_full_market_days,
            "min_segment_support": self.settings.historical_intelligence_min_segment_support,
            "directional_candidate_allowed": len(distinct_full_market_days) >= self.settings.historical_intelligence_min_full_market_days,
            "execution_confident": execution_confident,
            "directional_confident": directional_confident,
            "confidence_state": confidence_state,
            "confidence_scorecard": confidence_scorecard,
        }

    async def _build_candidate_pack(
        self,
        *,
        run_id: str,
        active_pack: HistoricalHeuristicPack,
        intelligence: dict[str, Any],
        sufficiency: dict[str, Any],
    ) -> HistoricalHeuristicPack | None:
        rows: list[IntelligenceFeatureRow] = intelligence["rows"]
        if not rows:
            return None
        thresholds = active_pack.thresholds.model_copy(deep=True)
        spread_stats = intelligence["execution_intelligence"]["spread_regimes"]
        wide_stats = spread_stats.get("wide") or {}
        broken_stats = spread_stats.get("broken") or {}
        if (wide_stats.get("weak_trade_rate") or 0) >= 0.50 or (broken_stats.get("weak_trade_rate") or 0) >= 0.30:
            thresholds.strategy_quality_edge_buffer_bps = min(
                300,
                int((thresholds.strategy_quality_edge_buffer_bps or self.settings.strategy_quality_edge_buffer_bps) + 25),
            )
        if (broken_stats.get("count") or 0) >= self.settings.historical_intelligence_min_segment_support:
            thresholds.trigger_max_spread_bps = min(
                int(thresholds.trigger_max_spread_bps or self.settings.trigger_max_spread_bps),
                1200,
            )
        if any(row.weak_trade for row in rows):
            thresholds.strategy_min_remaining_payout_bps = max(
                int(thresholds.strategy_min_remaining_payout_bps or self.settings.strategy_min_remaining_payout_bps),
                500,
            )

        calibration_entries: list[HeuristicCalibrationEntry] = []
        if sufficiency["directional_candidate_allowed"]:
            for entry in intelligence["directional_intelligence"]["calibration_entries"]:
                if entry["support_count"] < self.settings.historical_intelligence_min_segment_support:
                    continue
                calibration_entries.append(
                    HeuristicCalibrationEntry(
                        series_ticker=entry["series_ticker"],
                        city_bucket=entry["city_bucket"],
                        threshold_bucket=entry["threshold_bucket"],
                        daypart=entry["daypart"],
                        forecast_delta_bucket=entry["forecast_delta_bucket"],
                        fair_yes_adjust_bps=entry["fair_yes_adjust_bps"],
                        support_count=entry["support_count"],
                        mean_error_bps=entry["mean_error_bps"],
                    )
                )

        policy_graph: list[HeuristicPolicyNode] = []
        if sufficiency["directional_candidate_allowed"]:
            for brier_entry in intelligence["directional_intelligence"]["brier_scorecard"]:
                if brier_entry["support_count"] < self.settings.historical_intelligence_min_segment_support:
                    continue
                if brier_entry["mean_brier_error"] <= 0.12:
                    continue
                buffer_add = min(100, int(brier_entry["mean_brier_error"] * 500))
                policy_graph.append(
                    HeuristicPolicyNode(
                        rule_id=(
                            f"high-brier-{brier_entry['city_bucket']}-"
                            f"{brier_entry['threshold_bucket']}-{brier_entry['daypart']}"
                        ),
                        description=(
                            f"High Brier error ({brier_entry['mean_brier_error']:.3f}) in "
                            f"{brier_entry['city_bucket']} {brier_entry['threshold_bucket']} "
                            f"{brier_entry['daypart']}; require extra edge buffer."
                        ),
                        priority=40,
                        support_count=int(brier_entry["support_count"]),
                        condition=HeuristicPolicyCondition(
                            city_buckets=[brier_entry["city_bucket"]],
                            threshold_buckets=[brier_entry["threshold_bucket"]],
                            dayparts=[brier_entry["daypart"]],
                        ),
                        action=HeuristicPolicyAction(
                            strategy_quality_edge_buffer_bps=buffer_add,
                        ),
                    )
                )

        for candidate in intelligence["rule_synthesis_candidates"]:
            if candidate["support_count"] < self.settings.historical_intelligence_min_segment_support:
                continue
            condition = HeuristicPolicyCondition(
                series_tickers=[candidate["series_ticker"]] if candidate.get("series_ticker") else [],
                spread_regimes=[candidate["spread_regime"]] if candidate.get("spread_regime") else [],
                dayparts=[candidate["daypart"]] if candidate.get("daypart") else [],
                forecast_delta_buckets=[candidate["forecast_delta_bucket"]] if candidate.get("forecast_delta_bucket") else [],
                coverage_classes=[candidate["coverage_class"]] if candidate.get("coverage_class") else [],
            )
            action = HeuristicPolicyAction(
                recommended_strategy_mode=StrategyMode.LATE_DAY_AVOID
                if candidate["recommended_strategy_mode"] == StrategyMode.LATE_DAY_AVOID.value
                else StrategyMode.DIRECTIONAL_UNRESOLVED,
                force_stand_down_reason=StandDownReason(candidate["force_stand_down_reason"])
                if candidate.get("force_stand_down_reason")
                else None,
                strategy_quality_edge_buffer_bps=candidate.get("strategy_quality_edge_buffer_bps"),
            )
            policy_graph.append(
                HeuristicPolicyNode(
                    rule_id=candidate["rule_id"],
                    description=candidate["description"],
                    priority=int(candidate["priority"]),
                    support_count=int(candidate["support_count"]),
                    condition=condition,
                    action=action,
                )
            )

        agent_summary = self._build_agent_summary(
            rows=rows,
            thresholds=thresholds,
            policy_graph=policy_graph,
            sufficiency=sufficiency,
        )
        if not calibration_entries and not policy_graph and thresholds == active_pack.thresholds:
            return None
        candidate = HistoricalHeuristicPack(
            version=self.heuristic_service.next_candidate_version(),
            status="candidate",
            parent_version=active_pack.version,
            source="historical_intelligence",
            description="Candidate heuristic pack synthesized from historical replay intelligence.",
            thresholds=thresholds,
            calibration_entries=calibration_entries,
            policy_graph=policy_graph,
            agent_summary=agent_summary,
            metadata={
                "intelligence_run_id": run_id,
                "support_window": {
                    "window_days": self.settings.historical_intelligence_window_days,
                    "row_count": len(rows),
                    "full_market_days": sufficiency["distinct_full_market_days"],
                },
                "sufficiency": sufficiency,
            },
        )
        return self.heuristic_service.compile_pack(candidate)

    async def _evaluate_candidate(
        self,
        *,
        bundles: list[Any],
        active_pack: HistoricalHeuristicPack,
        candidate_pack: HistoricalHeuristicPack,
    ) -> dict[str, Any]:
        split = self.historical_training_service._split_historical_bundles(bundles)
        holdout_ids = set(split.holdout or [])
        evaluation_bundles = [bundle for bundle in bundles if bundle.room["id"] in holdout_ids] or bundles
        baseline_results = [self._simulate_bundle(bundle, active_pack) for bundle in evaluation_bundles]
        candidate_results = [self._simulate_bundle(bundle, candidate_pack) for bundle in evaluation_bundles]
        baseline_scorecard = self._score_simulations(baseline_results)
        candidate_scorecard = self._score_simulations(candidate_results)
        composite_improvement = candidate_scorecard["composite_score"] - baseline_scorecard["composite_score"]
        directional_change_requested = bool(candidate_pack.calibration_entries)
        promotable = (
            candidate_scorecard["immutable_boundary_violations"] == 0
            and candidate_scorecard["stale_data_mismatch_regressions"] <= baseline_scorecard["stale_data_mismatch_regressions"]
            and candidate_scorecard["missed_stand_down_regressions"] <= baseline_scorecard["missed_stand_down_regressions"]
            and candidate_scorecard["resolved_case_regressions"] <= baseline_scorecard["resolved_case_regressions"]
                    and composite_improvement >= self.settings.historical_intelligence_min_composite_improvement
                    and (
                        (not directional_change_requested)
                        or candidate_pack.metadata.get("sufficiency", {}).get("directional_confident", False)
                    )
        )
        return {
            "promotable": promotable,
            "composite_improvement": composite_improvement,
            "directional_change_requested": directional_change_requested,
            "confidence_state": candidate_pack.metadata.get("sufficiency", {}).get("confidence_state"),
            "baseline": baseline_scorecard,
            "candidate": candidate_scorecard,
            "holdout_room_count": len(evaluation_bundles),
        }

    def _simulate_bundle(self, bundle: Any, pack: HistoricalHeuristicPack) -> dict[str, Any]:
        market_snapshot = dict(bundle.market_snapshot or {})
        research_dossier = bundle.research_dossier or {}
        if not market_snapshot or not research_dossier:
            return {
                "room_id": bundle.room["id"],
                "composite_score": 0.0,
                "immutable_boundary_violation": 1,
                "stale_data_mismatch_regression": 1,
                "missed_stand_down_regression": 1,
                "resolved_case_regression": 1,
                "calibration_mae_bps": None,
                "would_trade": False,
                "profitable_trade": None,
            }
        mapping = self._mapping_for_bundle(bundle)
        if mapping is None:
            return {
                "room_id": bundle.room["id"],
                "composite_score": 0.0,
                "immutable_boundary_violation": 1,
                "stale_data_mismatch_regression": 1,
                "missed_stand_down_regression": 1,
                "resolved_case_regression": 1,
                "calibration_mae_bps": None,
                "would_trade": False,
                "profitable_trade": None,
            }
        base_pack_thresholds = self._base_thresholds()
        reference_time = self._bundle_reference_time(bundle)
        dossier = self.research_coordinator._hydrate_runtime_fields(  # noqa: SLF001
            ResearchDossier.model_validate(research_dossier),
            reference_time=reference_time,
        )
        base_signal = self.research_coordinator.build_signal_from_dossier(
            dossier,
            market_snapshot,
            min_edge_bps=pack.thresholds.risk_min_edge_bps or base_pack_thresholds.risk_min_edge_bps,
        )
        market_observed_at = reference_time
        market_stale = is_market_stale(
            observed_at=market_observed_at,
            stale_after_seconds=self.settings.risk_stale_market_seconds,
            reference_time=reference_time,
        )
        application = self.heuristic_service.apply_to_signal(
            pack=pack,
            mapping=mapping,
            signal=base_signal,
            market_snapshot=market_snapshot,
            reference_time=reference_time,
            base_thresholds=base_pack_thresholds,
            market_stale=market_stale,
            research_stale=dossier.freshness.stale,
            coverage_class=bundle.coverage_class,
            candidate_pack_id=pack.version if pack.status == "candidate" else None,
        )
        threshold_overrides = self.heuristic_service.runtime_thresholds(
            base_thresholds=base_pack_thresholds,
            application=application,
        )
        signal = apply_heuristic_application_to_signal(
            settings=self.settings,
            signal=StrategySignal(
                fair_yes_dollars=base_signal.fair_yes_dollars,
                confidence=base_signal.confidence,
                edge_bps=base_signal.edge_bps,
                recommended_action=base_signal.recommended_action,
                recommended_side=base_signal.recommended_side,
                target_yes_price_dollars=base_signal.target_yes_price_dollars,
                summary=base_signal.summary,
                weather=base_signal.weather,
                resolution_state=base_signal.resolution_state,
                strategy_mode=base_signal.strategy_mode,
                heuristic_application=application,
            ),
            market_snapshot=market_snapshot,
            min_edge_bps=threshold_overrides.risk_min_edge_bps,
            spread_limit_bps=threshold_overrides.trigger_max_spread_bps,
        )
        signal.heuristic_application = application
        signal.eligibility = evaluate_trade_eligibility(
            settings=self.settings,
            signal=signal,
            market_snapshot=market_snapshot,
            market_observed_at=market_observed_at,
            research_freshness=dossier.freshness,
            thresholds=threshold_overrides,
            decision_time=reference_time,
            market_stale_after_seconds=self.settings.historical_replay_market_stale_seconds,
        )
        settlement = self._settlement_yes(bundle)
        calibration_mae_bps = (
            abs(int(((signal.fair_yes_dollars - settlement) * Decimal("10000")).to_integral_value()))
            if settlement is not None
            else None
        )
        would_trade = bool(
            signal.eligibility.eligible
            and signal.recommended_action is not None
            and signal.recommended_side is not None
            and signal.target_yes_price_dollars is not None
        )
        profitable_trade = None
        if would_trade and settlement is not None and signal.target_yes_price_dollars is not None and signal.recommended_side is not None:
            payout = settlement - signal.target_yes_price_dollars if signal.recommended_side.value == "yes" else signal.target_yes_price_dollars - settlement
            profitable_trade = payout > Decimal("0")
        strategy_audit = dict(bundle.strategy_audit or {})
        immutable_boundary_violation = 1 if (signal.eligibility.eligible and (dossier.freshness.stale or signal.resolution_state != WeatherResolutionState.UNRESOLVED)) else 0
        stale_regression = 1 if (signal.eligibility.eligible and dossier.freshness.stale) else 0
        missed_stand_down_regression = 1 if (signal.eligibility.eligible and bool(strategy_audit.get("missed_stand_down"))) else 0
        resolved_case_regression = 1 if (
            signal.eligibility.eligible and signal.resolution_state != WeatherResolutionState.UNRESOLVED
        ) else 0
        return {
            "room_id": bundle.room["id"],
            "calibration_mae_bps": calibration_mae_bps,
            "would_trade": would_trade,
            "profitable_trade": profitable_trade,
            "immutable_boundary_violation": immutable_boundary_violation,
            "stale_data_mismatch_regression": stale_regression,
            "missed_stand_down_regression": missed_stand_down_regression,
            "resolved_case_regression": resolved_case_regression,
        }

    def _score_simulations(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        calibration_rows = [row["calibration_mae_bps"] for row in rows if row["calibration_mae_bps"] is not None]
        trade_rows = [row for row in rows if row["would_trade"]]
        profitable_trades = [row for row in trade_rows if row["profitable_trade"] is True]
        calibration_score = 1.0 - (sum(calibration_rows) / len(calibration_rows) / 10000.0 if calibration_rows else 1.0)
        trade_quality_score = (len(profitable_trades) / len(trade_rows)) if trade_rows else 0.0
        cleanliness_penalty = sum(row["immutable_boundary_violation"] for row in rows) / max(len(rows), 1)
        composite_score = (0.45 * calibration_score) + (0.30 * trade_quality_score) + (0.25 * (1.0 - cleanliness_penalty))
        return {
            "calibration_mae_bps": (sum(calibration_rows) / len(calibration_rows)) if calibration_rows else None,
            "would_trade_count": len(trade_rows),
            "profitable_trade_count": len(profitable_trades),
            "immutable_boundary_violations": sum(row["immutable_boundary_violation"] for row in rows),
            "stale_data_mismatch_regressions": sum(row["stale_data_mismatch_regression"] for row in rows),
            "missed_stand_down_regressions": sum(row["missed_stand_down_regression"] for row in rows),
            "resolved_case_regressions": sum(row["resolved_case_regression"] for row in rows),
            "composite_score": composite_score,
        }

    def _feature_row(self, bundle: Any) -> IntelligenceFeatureRow | None:
        mapping = self._mapping_for_bundle(bundle)
        if mapping is None:
            return None
        signal = dict(bundle.signal or {})
        signal_payload = dict(signal.get("payload") or {})
        fair_yes_raw = signal.get("fair_yes_dollars")
        if fair_yes_raw in (None, ""):
            return None
        fair_yes = quantize_price(fair_yes_raw)
        reference_time = self._bundle_reference_time(bundle)
        weather_bundle = dict(bundle.weather_bundle or {})
        forecast_high = self._extract_float(
            (((weather_bundle.get("forecast") or {}).get("summary") or {}).get("forecast_high_f"))
            or (((bundle.research_dossier or {}).get("summary") or {}).get("current_numeric_facts") or {}).get("forecast_high_f")
        )
        current_temp = self._extract_float(
            (((weather_bundle.get("observation") or {}).get("summary") or {}).get("current_temp_f"))
            or (((bundle.research_dossier or {}).get("summary") or {}).get("current_numeric_facts") or {}).get("current_temp_f")
        )
        threshold_f = self._extract_float(
            ((weather_bundle.get("mapping") or {}).get("threshold_f"))
            or (((bundle.research_dossier or {}).get("summary") or {}).get("current_numeric_facts") or {}).get("threshold_f")
            or mapping.threshold_f
        )
        forecast_delta = (forecast_high - threshold_f) if forecast_high is not None and threshold_f is not None else None
        settlement_yes = self._settlement_yes(bundle)
        calibration_error = (
            int(((fair_yes - settlement_yes) * Decimal("10000")).to_integral_value())
            if settlement_yes is not None
            else None
        )
        quotes = market_quotes(bundle.market_snapshot or {})
        liquidity_regime = self._liquidity_regime(quotes)
        remaining_payout = None
        side = (bundle.trade_ticket or {}).get("side")
        yes_price = (bundle.trade_ticket or {}).get("yes_price_dollars")
        if side in {"yes", "no"} and yes_price not in (None, ""):
            remaining_payout = remaining_payout_dollars(
                side=ContractSide(side),
                yes_price_dollars=quantize_price(yes_price),
            )
        stand_down_reason = signal_payload.get("stand_down_reason") or ((signal_payload.get("eligibility") or {}).get("stand_down_reason"))
        outcome = bundle.outcome.model_dump(mode="json") if hasattr(bundle.outcome, "model_dump") else (bundle.outcome or {})
        strategy_audit = dict(bundle.strategy_audit or {})
        profitability_flag = None
        if bundle.counterfactual_pnl_dollars is not None:
            profitability_flag = bundle.counterfactual_pnl_dollars > Decimal("0")
        return IntelligenceFeatureRow(
            room_id=bundle.room["id"],
            market_ticker=bundle.room["market_ticker"],
            series_ticker=mapping.series_ticker,
            local_market_day=(bundle.historical_provenance or {}).get("local_market_day"),
            daypart=self._daypart(mapping, reference_time),
            city_bucket=mapping.location_name,
            threshold_bucket=self._threshold_bucket(threshold_f),
            forecast_delta_bucket=self._forecast_delta_bucket(forecast_delta),
            spread_regime=self._spread_regime(market_spread_bps(bundle.market_snapshot or {})),
            payout_bucket=self._payout_bucket(remaining_payout),
            quote_freshness="stale" if ((signal_payload.get("eligibility") or {}).get("market_stale")) else "fresh",
            liquidity_regime=liquidity_regime,
            coverage_class=bundle.coverage_class or HistoricalTrainingService.COVERAGE_NONE,
            settlement_yes_dollars=settlement_yes,
            fair_yes_dollars=fair_yes,
            calibration_error_bps=calibration_error,
            stand_down_reason=str(stand_down_reason) if stand_down_reason else None,
            eligible=bool((signal_payload.get("eligibility") or {}).get("eligible")),
            blocked_by=outcome.get("blocked_by"),
            stale_data_mismatch=bool(strategy_audit.get("stale_data_mismatch")),
            missed_stand_down=bool(strategy_audit.get("missed_stand_down")),
            weak_trade=(strategy_audit.get("trade_quality") == "weak_trade"),
            rule_trace=list(bundle.rule_trace or []),
            counterfactual_pnl_dollars=bundle.counterfactual_pnl_dollars,
            profitability_flag=profitability_flag,
        )

    def _rule_synthesis_candidates(self, rows: list[IntelligenceFeatureRow]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str, str], list[IntelligenceFeatureRow]] = defaultdict(list)
        for row in rows:
            grouped[(row.series_ticker, row.spread_regime, row.daypart)].append(row)
        candidates: list[dict[str, Any]] = []
        for (series_ticker, spread_regime, daypart), items in grouped.items():
            support_count = len(items)
            weak_trade_rate = sum(1 for item in items if item.weak_trade) / support_count if support_count else 0.0
            broken_rate = sum(1 for item in items if item.stand_down_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN.value) / support_count if support_count else 0.0
            if spread_regime == "broken" and support_count >= self.settings.historical_intelligence_min_segment_support:
                candidates.append(
                    {
                        "rule_id": f"broken-book-{series_ticker}-{daypart}",
                        "description": f"Stand down early on broken books for {series_ticker} during {daypart}.",
                        "series_ticker": series_ticker,
                        "spread_regime": spread_regime,
                        "daypart": daypart,
                        "coverage_class": HistoricalTrainingService.COVERAGE_LATE_ONLY,
                        "priority": 10,
                        "support_count": support_count,
                        "recommended_strategy_mode": StrategyMode.LATE_DAY_AVOID.value,
                        "force_stand_down_reason": StandDownReason.BOOK_EFFECTIVELY_BROKEN.value,
                        "strategy_quality_edge_buffer_bps": None,
                    }
                )
            elif spread_regime == "wide" and weak_trade_rate >= 0.50 and support_count >= self.settings.historical_intelligence_min_segment_support:
                candidates.append(
                    {
                        "rule_id": f"wide-late-avoid-{series_ticker}-{daypart}",
                        "description": f"Use late-day avoid posture on persistently wide books for {series_ticker}.",
                        "series_ticker": series_ticker,
                        "spread_regime": spread_regime,
                        "daypart": daypart,
                        "coverage_class": HistoricalTrainingService.COVERAGE_LATE_ONLY,
                        "priority": 30,
                        "support_count": support_count,
                        "recommended_strategy_mode": StrategyMode.LATE_DAY_AVOID.value,
                        "force_stand_down_reason": StandDownReason.SPREAD_TOO_WIDE.value if broken_rate >= 0.10 else None,
                        "strategy_quality_edge_buffer_bps": 25,
                    }
                )
        # Flat-delta (near-threshold) quality-buffer rules.
        # Group by (series_ticker, forecast_delta_bucket, daypart) and emit a
        # quality-buffer rule for flat regimes that are underperforming.
        delta_grouped: dict[tuple[str, str, str], list[IntelligenceFeatureRow]] = defaultdict(list)
        for row in rows:
            delta_grouped[(row.series_ticker, row.forecast_delta_bucket, row.daypart)].append(row)

        for (series_ticker, forecast_delta_bucket, daypart), items in delta_grouped.items():
            if forecast_delta_bucket != "flat":
                continue
            support_count = len(items)
            if support_count < self.settings.historical_intelligence_min_segment_support:
                continue
            profitable = sum(1 for item in items if item.profitability_flag is True)
            known_outcome = sum(1 for item in items if item.profitability_flag is not None)
            profitable_rate = profitable / known_outcome if known_outcome else 0.0
            if profitable_rate >= 0.55:
                continue  # flat regime is performing adequately, no intervention needed
            candidates.append(
                {
                    "rule_id": f"flat-delta-quality-{series_ticker}-{daypart}",
                    "description": (
                        f"Near-threshold forecasts for {series_ticker} during {daypart} "
                        f"show low profitability ({profitable_rate:.0%}); require higher edge."
                    ),
                    "series_ticker": series_ticker,
                    "spread_regime": None,
                    "forecast_delta_bucket": "flat",
                    "daypart": daypart,
                    "coverage_class": None,
                    "priority": 20,
                    "support_count": support_count,
                    "recommended_strategy_mode": StrategyMode.DIRECTIONAL_UNRESOLVED.value,
                    "force_stand_down_reason": None,
                    "strategy_quality_edge_buffer_bps": 50,
                }
            )

        candidates.sort(key=lambda item: (-item["support_count"], item["priority"], item["rule_id"]))
        return candidates

    def _execution_regime_summary(self, rows: list[IntelligenceFeatureRow]) -> dict[str, Any]:
        count = len(rows)
        profitable = sum(1 for row in rows if row.profitability_flag is True)
        eligible = sum(1 for row in rows if row.eligible)
        weak = sum(1 for row in rows if row.weak_trade)
        return {
            "count": count,
            "eligible_count": eligible,
            "profitable_count": profitable,
            "profitable_rate": profitable / count if count else 0.0,
            "weak_trade_rate": weak / count if count else 0.0,
            "average_counterfactual_pnl_dollars": (
                float(sum((row.counterfactual_pnl_dollars or Decimal("0")) for row in rows) / count)
                if count
                else 0.0
            ),
        }

    def _stand_down_summary(self, rows: list[IntelligenceFeatureRow]) -> dict[str, Any]:
        count = len(rows)
        regret = sum(1 for row in rows if row.profitability_flag is True)
        return {
            "count": count,
            "regret_rate": regret / count if count else 0.0,
        }

    def _regime_outcome_summary(self, rows: list[IntelligenceFeatureRow]) -> dict[str, Any]:
        count = len(rows)
        blocked = sum(1 for row in rows if row.blocked_by)
        would_trade = sum(1 for row in rows if row.eligible)
        profitable = sum(1 for row in rows if row.profitability_flag is True)
        return {
            "count": count,
            "blocked_count": blocked,
            "would_trade_count": would_trade,
            "profitable_count": profitable,
        }

    def _patch_suggestion_payload(
        self,
        *,
        active_pack: HistoricalHeuristicPack,
        candidate_pack: HistoricalHeuristicPack,
        intelligence: dict[str, Any],
        evaluation: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "heuristic_pack_version": candidate_pack.version,
            "parent_version": active_pack.version,
            "summary": "Historical intelligence suggests runtime threshold and routing updates.",
            "files": [
                "src/kalshi_bot/services/signal.py",
                "src/kalshi_bot/services/research.py",
                "src/kalshi_bot/services/historical_heuristics.py",
            ],
            "threshold_changes": {
                "from": active_pack.thresholds.model_dump(mode="json"),
                "to": candidate_pack.thresholds.model_dump(mode="json"),
            },
            "policy_rule_count": len(candidate_pack.policy_graph),
            "calibration_entry_count": len(candidate_pack.calibration_entries),
            "support_window": candidate_pack.metadata.get("support_window"),
            "evaluation": evaluation,
            "rule_synthesis_candidates": intelligence["rule_synthesis_candidates"][:10],
        }

    def _build_agent_summary(
        self,
        *,
        rows: list[IntelligenceFeatureRow],
        thresholds,
        policy_graph: list[HeuristicPolicyNode],
        sufficiency: dict[str, Any],
    ) -> str:
        spread_counts = Counter(row.spread_regime for row in rows)
        blocker_counts = Counter(row.stand_down_reason for row in rows if row.stand_down_reason)
        top_spread = spread_counts.most_common(2)
        top_blockers = blocker_counts.most_common(3)
        spread_text = ", ".join(f"{name}={count}" for name, count in top_spread) or "no dominant spread regime"
        blocker_text = ", ".join(f"{name}={count}" for name, count in top_blockers) or "no dominant stand-down blocker"
        rule_text = f"{len(policy_graph)} policy rules staged" if policy_graph else "no policy routing changes staged"
        directional_text = (
            "Directional calibration updates are supported by the current full-checkpoint confidence window."
            if sufficiency["directional_confident"]
            else "Directional calibration remains conservative because full-checkpoint support is still thin."
        )
        return (
            f"Historical intelligence window analyzed {len(rows)} clean replay checkpoints. "
            f"Primary spread regimes: {spread_text}. "
            f"Primary blockers: {blocker_text}. "
            f"Threshold bias now favors edge buffer {thresholds.strategy_quality_edge_buffer_bps}bps and minimum remaining payout "
            f"{thresholds.strategy_min_remaining_payout_bps}bps. {rule_text}. "
            f"Confidence state is {sufficiency['confidence_state']}. {directional_text}"
        )

    def _base_thresholds(self) -> RuntimeThresholds:
        return RuntimeThresholds(
            risk_min_edge_bps=self.settings.risk_min_edge_bps,
            risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
            trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
            trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
        )

    def _mapping_for_bundle(self, bundle: Any) -> WeatherMarketMapping | None:
        market_snapshot = bundle.market_snapshot or {}
        market = market_snapshot.get("market", market_snapshot)
        mapping = self.weather_directory.resolve_market(bundle.room["market_ticker"], market)
        if mapping is not None:
            return mapping
        weather_mapping = (bundle.weather_bundle or {}).get("mapping") or {}
        series_ticker = weather_mapping.get("series_ticker")
        if series_ticker:
            for candidate in self.weather_directory.all():
                if candidate.series_ticker == series_ticker and candidate.market_ticker == bundle.room["market_ticker"]:
                    return candidate
        return None

    def _bundle_reference_time(self, bundle: Any) -> datetime:
        raw = bundle.replay_checkpoint_ts or (bundle.historical_provenance or {}).get("checkpoint_ts") or bundle.room.get("updated_at")
        if isinstance(raw, datetime):
            return raw.astimezone(UTC)
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
        return datetime.now(UTC)

    def _settlement_yes(self, bundle: Any) -> Decimal | None:
        settlement_label = bundle.settlement_label or {}
        value = settlement_label.get("settlement_value_dollars")
        if value in (None, ""):
            settlement = bundle.settlement or {}
            value = settlement.get("settlement_value_dollars")
        return quantize_price(value) if value not in (None, "") else None

    def _daypart(self, mapping: WeatherMarketMapping, reference_time: datetime) -> str:
        local_time = reference_time.astimezone(ZoneInfo(mapping.timezone_name or "UTC"))
        if local_time.hour < 12:
            return "morning"
        if local_time.hour < 16:
            return "midday"
        return "late"

    @staticmethod
    def _threshold_bucket(threshold_f: float | None) -> str:
        if threshold_f is None:
            return "unknown"
        if threshold_f < 60:
            return "lt60"
        if threshold_f < 70:
            return "60s"
        if threshold_f < 80:
            return "70s"
        return "80plus"

    @staticmethod
    def _forecast_delta_bucket(delta: float | None) -> str:
        if delta is None:
            return "unknown"
        if delta <= -8:
            return "minus_8_plus"
        if delta <= -3:
            return "minus_3_to_7"
        if delta < 3:
            return "flat"
        if delta < 8:
            return "plus_3_to_7"
        return "plus_8_plus"

    @staticmethod
    def _spread_regime(spread_bps: int | None) -> str:
        if spread_bps is None:
            return "unknown"
        if spread_bps <= 150:
            return "tight"
        if spread_bps <= 500:
            return "tradable"
        if spread_bps <= 1200:
            return "wide"
        return "broken"

    @staticmethod
    def _payout_bucket(remaining_payout: Decimal | None) -> str:
        if remaining_payout is None:
            return "unknown"
        if remaining_payout < Decimal("0.1000"):
            return "lt_0_10"
        if remaining_payout < Decimal("0.2500"):
            return "0_10_to_0_25"
        if remaining_payout < Decimal("0.5000"):
            return "0_25_to_0_50"
        return "0_50_plus"

    @staticmethod
    def _liquidity_regime(quotes: dict[str, Decimal | None]) -> str:
        yes_bid = quotes.get("yes_bid")
        yes_ask = quotes.get("yes_ask")
        no_ask = quotes.get("no_ask")
        if yes_ask is None or no_ask is None:
            return "missing"
        if yes_bid is None:
            return "one_sided"
        if yes_bid <= Decimal("0.0200") and yes_ask >= Decimal("0.9800"):
            return "dead"
        if yes_bid >= Decimal("0.2000") and yes_ask <= Decimal("0.8000"):
            return "active"
        return "thin"

    @staticmethod
    def _extract_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
