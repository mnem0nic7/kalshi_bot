from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import case, func, select

from kalshi_bot.core.schemas import AgentPack, EvaluationSummary, HistoricalHeuristicPack, TrainingReadiness
from kalshi_bot.db.models import (
    AgentPackRecord,
    CritiqueRunRecord,
    DecisionCorpusBuildRecord,
    DecisionCorpusRowRecord,
    EvaluationRunRecord,
    HeuristicPackPromotionRecord,
    HeuristicPackRecord,
    HeuristicPatchSuggestionRecord,
    HistoricalCheckpointArchiveRecord,
    HistoricalImportRunRecord,
    HistoricalIntelligenceRunRecord,
    HistoricalMarketSnapshotRecord,
    HistoricalPipelineRunRecord,
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    HistoricalWeatherSnapshotRecord,
    ParameterPackRecord,
    PromotionEventRecord,
    Room,
    TrainingDatasetBuildItemRecord,
    TrainingDatasetBuildRecord,
    TrainingReadinessRecord,
)
from kalshi_bot.learning.parameter_pack import ParameterPack, parameter_pack_hash


class LearningRepositoryMixin:
    session: Any

    def _resolved_kalshi_env(self, kalshi_env: str | None = None) -> str:
        raise NotImplementedError

    def _env_stream_name(self, prefix: str, *, kalshi_env: str | None = None, suffix: str | None = None) -> str:
        raise NotImplementedError

    async def create_agent_pack(self, pack: AgentPack) -> AgentPackRecord:
        record = AgentPackRecord(
            version=pack.version,
            status=pack.status,
            parent_version=pack.parent_version,
            source=pack.source,
            description=pack.description,
            payload=pack.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_agent_pack(self, pack: AgentPack) -> AgentPackRecord:
        record = await self.get_agent_pack(pack.version)
        if record is None:
            return await self.create_agent_pack(pack)
        record.status = pack.status
        record.parent_version = pack.parent_version
        record.source = pack.source
        record.description = pack.description
        record.payload = pack.model_dump(mode="json")
        await self.session.flush()
        return record

    async def get_agent_pack(self, version: str) -> AgentPackRecord | None:
        stmt = select(AgentPackRecord).where(AgentPackRecord.version == version).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_agent_packs(self, limit: int = 20) -> list[AgentPackRecord]:
        result = await self.session.execute(select(AgentPackRecord).order_by(AgentPackRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def create_parameter_pack(
        self,
        pack: ParameterPack,
        *,
        holdout_report: dict[str, Any] | None = None,
    ) -> ParameterPackRecord:
        record = ParameterPackRecord(
            version=pack.version,
            status=pack.status,
            parent_version=pack.parent_version,
            source=pack.source,
            description=pack.description,
            pack_hash=parameter_pack_hash(pack),
            payload=pack.to_dict(),
            holdout_report=holdout_report or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_parameter_pack(
        self,
        pack: ParameterPack,
        *,
        holdout_report: dict[str, Any] | None = None,
    ) -> ParameterPackRecord:
        record = await self.get_parameter_pack(pack.version)
        if record is None:
            return await self.create_parameter_pack(pack, holdout_report=holdout_report)
        record.status = pack.status
        record.parent_version = pack.parent_version
        record.source = pack.source
        record.description = pack.description
        record.pack_hash = parameter_pack_hash(pack)
        record.payload = pack.to_dict()
        if holdout_report is not None:
            record.holdout_report = holdout_report
        await self.session.flush()
        return record

    async def get_parameter_pack(self, version: str) -> ParameterPackRecord | None:
        stmt = select(ParameterPackRecord).where(ParameterPackRecord.version == version).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_parameter_packs(self, limit: int = 20) -> list[ParameterPackRecord]:
        result = await self.session.execute(
            select(ParameterPackRecord).order_by(ParameterPackRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def create_historical_intelligence_run(
        self,
        *,
        date_from: str,
        date_to: str,
        active_pack_version: str | None,
        payload: dict[str, Any],
    ) -> HistoricalIntelligenceRunRecord:
        record = HistoricalIntelligenceRunRecord(
            date_from=date_from,
            date_to=date_to,
            active_pack_version=active_pack_version,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_historical_intelligence_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any],
        room_count: int,
        candidate_pack_version: str | None = None,
        promoted_pack_version: str | None = None,
        error_text: str | None = None,
    ) -> HistoricalIntelligenceRunRecord:
        record = await self.session.get(HistoricalIntelligenceRunRecord, run_id)
        if record is None:
            raise KeyError(f"Historical intelligence run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.room_count = room_count
        record.candidate_pack_version = candidate_pack_version
        record.promoted_pack_version = promoted_pack_version
        record.payload = payload
        record.error_text = error_text
        await self.session.flush()
        return record

    async def get_historical_intelligence_run(self, run_id: str) -> HistoricalIntelligenceRunRecord | None:
        return await self.session.get(HistoricalIntelligenceRunRecord, run_id)

    async def list_historical_intelligence_runs(self, limit: int = 20) -> list[HistoricalIntelligenceRunRecord]:
        result = await self.session.execute(
            select(HistoricalIntelligenceRunRecord)
            .order_by(HistoricalIntelligenceRunRecord.started_at.desc())
            .limit(limit)
        )
        return list(result.scalars())

    async def create_historical_pipeline_run(
        self,
        *,
        pipeline_kind: str,
        date_from: str,
        date_to: str,
        rolling_days: int,
        payload: dict[str, Any],
    ) -> HistoricalPipelineRunRecord:
        record = HistoricalPipelineRunRecord(
            pipeline_kind=pipeline_kind,
            date_from=date_from,
            date_to=date_to,
            rolling_days=rolling_days,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_historical_pipeline_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any],
        error_text: str | None = None,
    ) -> HistoricalPipelineRunRecord:
        record = await self.session.get(HistoricalPipelineRunRecord, run_id)
        if record is None:
            raise KeyError(f"Historical pipeline run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.payload = payload
        record.error_text = error_text
        await self.session.flush()
        return record

    async def update_historical_pipeline_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        payload: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> HistoricalPipelineRunRecord:
        record = await self.session.get(HistoricalPipelineRunRecord, run_id)
        if record is None:
            raise KeyError(f"Historical pipeline run {run_id} not found")
        if status is not None:
            record.status = status
        if payload is not None:
            record.payload = payload
        if error_text is not None:
            record.error_text = error_text
        await self.session.flush()
        return record

    async def get_historical_pipeline_run(self, run_id: str) -> HistoricalPipelineRunRecord | None:
        return await self.session.get(HistoricalPipelineRunRecord, run_id)

    async def list_historical_pipeline_runs(
        self,
        *,
        pipeline_kind: str | None = None,
        limit: int = 20,
    ) -> list[HistoricalPipelineRunRecord]:
        stmt = select(HistoricalPipelineRunRecord)
        if pipeline_kind is not None:
            stmt = stmt.where(HistoricalPipelineRunRecord.pipeline_kind == pipeline_kind)
        result = await self.session.execute(
            stmt.order_by(HistoricalPipelineRunRecord.started_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def create_heuristic_pack(self, pack: HistoricalHeuristicPack) -> HeuristicPackRecord:
        record = HeuristicPackRecord(
            version=pack.version,
            status=pack.status,
            parent_version=pack.parent_version,
            source=pack.source,
            description=pack.description,
            payload=pack.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_heuristic_pack(self, pack: HistoricalHeuristicPack) -> HeuristicPackRecord:
        record = await self.get_heuristic_pack(pack.version)
        if record is None:
            return await self.create_heuristic_pack(pack)
        record.status = pack.status
        record.parent_version = pack.parent_version
        record.source = pack.source
        record.description = pack.description
        record.payload = pack.model_dump(mode="json")
        await self.session.flush()
        return record

    async def get_heuristic_pack(self, version: str) -> HeuristicPackRecord | None:
        stmt = select(HeuristicPackRecord).where(HeuristicPackRecord.version == version).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_heuristic_packs(self, limit: int = 20) -> list[HeuristicPackRecord]:
        result = await self.session.execute(select(HeuristicPackRecord).order_by(HeuristicPackRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def create_heuristic_pack_promotion(
        self,
        *,
        candidate_version: str,
        previous_version: str | None,
        intelligence_run_id: str | None,
        payload: dict[str, Any],
        status: str = "staged",
    ) -> HeuristicPackPromotionRecord:
        record = HeuristicPackPromotionRecord(
            candidate_version=candidate_version,
            previous_version=previous_version,
            intelligence_run_id=intelligence_run_id,
            payload=payload,
            status=status,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_heuristic_pack_promotion(
        self,
        promotion_id: str,
        *,
        status: str,
        payload: dict[str, Any] | None = None,
        rollback_reason: str | None = None,
    ) -> HeuristicPackPromotionRecord:
        record = await self.session.get(HeuristicPackPromotionRecord, promotion_id)
        if record is None:
            raise KeyError(f"Heuristic pack promotion {promotion_id} not found")
        record.status = status
        if payload is not None:
            record.payload = payload
        if rollback_reason is not None:
            record.rollback_reason = rollback_reason
        await self.session.flush()
        return record

    async def get_heuristic_pack_promotion(self, promotion_id: str) -> HeuristicPackPromotionRecord | None:
        return await self.session.get(HeuristicPackPromotionRecord, promotion_id)

    async def list_heuristic_pack_promotions(self, limit: int = 20) -> list[HeuristicPackPromotionRecord]:
        result = await self.session.execute(
            select(HeuristicPackPromotionRecord)
            .order_by(HeuristicPackPromotionRecord.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars())

    async def create_heuristic_patch_suggestion(
        self,
        *,
        heuristic_pack_version: str,
        intelligence_run_id: str | None,
        status: str,
        payload: dict[str, Any],
    ) -> HeuristicPatchSuggestionRecord:
        record = HeuristicPatchSuggestionRecord(
            heuristic_pack_version=heuristic_pack_version,
            intelligence_run_id=intelligence_run_id,
            status=status,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_heuristic_patch_suggestions(
        self,
        *,
        heuristic_pack_version: str | None = None,
        intelligence_run_id: str | None = None,
        limit: int = 20,
    ) -> list[HeuristicPatchSuggestionRecord]:
        stmt = select(HeuristicPatchSuggestionRecord)
        if heuristic_pack_version is not None:
            stmt = stmt.where(HeuristicPatchSuggestionRecord.heuristic_pack_version == heuristic_pack_version)
        if intelligence_run_id is not None:
            stmt = stmt.where(HeuristicPatchSuggestionRecord.intelligence_run_id == intelligence_run_id)
        result = await self.session.execute(
            stmt.order_by(HeuristicPatchSuggestionRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def create_critique_run(
        self,
        *,
        source_pack_version: str,
        payload: dict[str, Any],
    ) -> CritiqueRunRecord:
        record = CritiqueRunRecord(source_pack_version=source_pack_version, payload=payload)
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_critique_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any],
        candidate_version: str | None = None,
        room_count: int | None = None,
        error_text: str | None = None,
    ) -> CritiqueRunRecord:
        record = await self.session.get(CritiqueRunRecord, run_id)
        if record is None:
            raise KeyError(f"Critique run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.payload = payload
        record.candidate_version = candidate_version
        if room_count is not None:
            record.room_count = room_count
        record.error_text = error_text
        await self.session.flush()
        return record

    async def get_critique_run(self, run_id: str) -> CritiqueRunRecord | None:
        return await self.session.get(CritiqueRunRecord, run_id)

    async def list_critique_runs(self, limit: int = 20) -> list[CritiqueRunRecord]:
        result = await self.session.execute(select(CritiqueRunRecord).order_by(CritiqueRunRecord.started_at.desc()).limit(limit))
        return list(result.scalars())

    async def create_evaluation_run(
        self,
        *,
        champion_version: str,
        candidate_version: str,
        payload: dict[str, Any],
    ) -> EvaluationRunRecord:
        record = EvaluationRunRecord(
            champion_version=champion_version,
            candidate_version=candidate_version,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_evaluation_run(
        self,
        run_id: str,
        *,
        summary: EvaluationSummary,
        holdout_room_count: int,
        error_text: str | None = None,
    ) -> EvaluationRunRecord:
        record = await self.session.get(EvaluationRunRecord, run_id)
        if record is None:
            raise KeyError(f"Evaluation run {run_id} not found")
        record.status = "completed" if error_text is None else "failed"
        record.finished_at = datetime.now(UTC)
        record.holdout_room_count = holdout_room_count
        record.passed = summary.passed if error_text is None else False
        record.payload = summary.model_dump(mode="json")
        record.error_text = error_text
        await self.session.flush()
        return record

    async def get_evaluation_run(self, run_id: str) -> EvaluationRunRecord | None:
        return await self.session.get(EvaluationRunRecord, run_id)

    async def list_evaluation_runs(self, limit: int = 20) -> list[EvaluationRunRecord]:
        result = await self.session.execute(select(EvaluationRunRecord).order_by(EvaluationRunRecord.started_at.desc()).limit(limit))
        return list(result.scalars())

    async def create_promotion_event(
        self,
        *,
        candidate_version: str,
        previous_version: str | None,
        target_color: str,
        evaluation_run_id: str | None,
        payload: dict[str, Any],
        status: str = "staged",
    ) -> PromotionEventRecord:
        record = PromotionEventRecord(
            candidate_version=candidate_version,
            previous_version=previous_version,
            target_color=target_color,
            evaluation_run_id=evaluation_run_id,
            payload=payload,
            status=status,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_promotion_event(
        self,
        promotion_event_id: str,
        *,
        status: str,
        payload: dict[str, Any] | None = None,
        rollback_reason: str | None = None,
    ) -> PromotionEventRecord:
        record = await self.session.get(PromotionEventRecord, promotion_event_id)
        if record is None:
            raise KeyError(f"Promotion event {promotion_event_id} not found")
        record.status = status
        if payload is not None:
            record.payload = payload
        if rollback_reason is not None:
            record.rollback_reason = rollback_reason
        await self.session.flush()
        return record

    async def get_promotion_event(self, promotion_event_id: str) -> PromotionEventRecord | None:
        return await self.session.get(PromotionEventRecord, promotion_event_id)

    async def list_promotion_events(self, limit: int = 20) -> list[PromotionEventRecord]:
        result = await self.session.execute(select(PromotionEventRecord).order_by(PromotionEventRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def create_training_dataset_build(
        self,
        *,
        build_version: str,
        mode: str,
        status: str,
        selection_window_start: datetime | None,
        selection_window_end: datetime | None,
        room_count: int,
        filters: dict[str, Any],
        label_stats: dict[str, Any],
        pack_versions: list[str],
        payload: dict[str, Any],
        completed_at: datetime | None = None,
    ) -> TrainingDatasetBuildRecord:
        record = TrainingDatasetBuildRecord(
            build_version=build_version,
            mode=mode,
            status=status,
            selection_window_start=selection_window_start,
            selection_window_end=selection_window_end,
            room_count=room_count,
            filters=filters,
            label_stats=label_stats,
            pack_versions=pack_versions,
            payload=payload,
            completed_at=completed_at,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def set_training_dataset_build_items(
        self,
        *,
        dataset_build_id: str,
        items: list[dict[str, Any]],
    ) -> list[TrainingDatasetBuildItemRecord]:
        existing = await self.session.execute(
            select(TrainingDatasetBuildItemRecord).where(TrainingDatasetBuildItemRecord.dataset_build_id == dataset_build_id)
        )
        for record in existing.scalars():
            await self.session.delete(record)
        created: list[TrainingDatasetBuildItemRecord] = []
        for sequence, item in enumerate(items, start=1):
            record = TrainingDatasetBuildItemRecord(
                dataset_build_id=dataset_build_id,
                room_id=item["room_id"],
                sequence=sequence,
                payload=item,
            )
            self.session.add(record)
            created.append(record)
        await self.session.flush()
        return created

    async def get_training_dataset_build(self, build_id: str) -> TrainingDatasetBuildRecord | None:
        return await self.session.get(TrainingDatasetBuildRecord, build_id)

    async def list_training_dataset_builds(
        self,
        limit: int = 20,
        *,
        mode_prefix: str | None = None,
        statuses: list[str] | None = None,
        exclude_statuses: list[str] | None = None,
    ) -> list[TrainingDatasetBuildRecord]:
        stmt = select(TrainingDatasetBuildRecord)
        if mode_prefix is not None:
            stmt = stmt.where(TrainingDatasetBuildRecord.mode.like(f"{mode_prefix}%"))
        if statuses:
            stmt = stmt.where(TrainingDatasetBuildRecord.status.in_(statuses))
        if exclude_statuses:
            stmt = stmt.where(TrainingDatasetBuildRecord.status.not_in(exclude_statuses))
        result = await self.session.execute(
            stmt.order_by(TrainingDatasetBuildRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def list_training_dataset_builds_for_room_ids(
        self,
        room_ids: list[str],
        *,
        mode_prefix: str | None = None,
        limit: int = 1000,
    ) -> list[TrainingDatasetBuildRecord]:
        if not room_ids:
            return []
        stmt = (
            select(TrainingDatasetBuildRecord)
            .join(
                TrainingDatasetBuildItemRecord,
                TrainingDatasetBuildItemRecord.dataset_build_id == TrainingDatasetBuildRecord.id,
            )
            .where(TrainingDatasetBuildItemRecord.room_id.in_(room_ids))
            .distinct()
        )
        if mode_prefix is not None:
            stmt = stmt.where(TrainingDatasetBuildRecord.mode.like(f"{mode_prefix}%"))
        result = await self.session.execute(
            stmt.order_by(TrainingDatasetBuildRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def update_training_dataset_build(
        self,
        build_id: str,
        *,
        status: str | None = None,
        payload_updates: dict[str, Any] | None = None,
        completed_at: datetime | None = None,
    ) -> TrainingDatasetBuildRecord | None:
        record = await self.get_training_dataset_build(build_id)
        if record is None:
            return None
        if status is not None:
            record.status = status
        if payload_updates:
            record.payload = {**(record.payload or {}), **payload_updates}
        if completed_at is not None:
            record.completed_at = completed_at
        await self.session.flush()
        return record

    async def list_training_dataset_build_items(self, build_id: str) -> list[TrainingDatasetBuildItemRecord]:
        result = await self.session.execute(
            select(TrainingDatasetBuildItemRecord)
            .where(TrainingDatasetBuildItemRecord.dataset_build_id == build_id)
            .order_by(TrainingDatasetBuildItemRecord.sequence.asc())
        )
        return list(result.scalars())

    async def create_training_readiness_snapshot(self, readiness: TrainingReadiness) -> TrainingReadinessRecord:
        record = TrainingReadinessRecord(
            ready_for_sft_export=readiness.ready_for_sft_export,
            ready_for_critique=readiness.ready_for_critique,
            ready_for_evaluation=readiness.ready_for_evaluation,
            ready_for_promotion=readiness.ready_for_promotion,
            complete_room_count=readiness.complete_room_count,
            market_diversity_count=readiness.market_diversity_count,
            settled_room_count=readiness.settled_room_count,
            trade_positive_room_count=readiness.trade_positive_room_count,
            payload=readiness.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_latest_training_readiness(self) -> TrainingReadinessRecord | None:
        stmt = select(TrainingReadinessRecord).order_by(TrainingReadinessRecord.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def create_historical_import_run(
        self,
        *,
        import_kind: str,
        source: str,
        payload: dict[str, Any],
    ) -> HistoricalImportRunRecord:
        record = HistoricalImportRunRecord(
            import_kind=import_kind,
            source=source,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_historical_import_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any],
        error_text: str | None = None,
    ) -> HistoricalImportRunRecord:
        record = await self.session.get(HistoricalImportRunRecord, run_id)
        if record is None:
            raise KeyError(f"Historical import run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.payload = payload
        record.error_text = error_text
        await self.session.flush()
        return record

    async def list_historical_import_runs(
        self,
        *,
        import_kind: str | None = None,
        limit: int = 20,
    ) -> list[HistoricalImportRunRecord]:
        stmt = select(HistoricalImportRunRecord)
        if import_kind is not None:
            stmt = stmt.where(HistoricalImportRunRecord.import_kind == import_kind)
        result = await self.session.execute(stmt.order_by(HistoricalImportRunRecord.started_at.desc()).limit(limit))
        return list(result.scalars())

    async def upsert_historical_market_snapshot(
        self,
        *,
        market_ticker: str,
        series_ticker: str | None,
        station_id: str | None,
        local_market_day: str,
        asof_ts: datetime,
        source_kind: str,
        source_id: str,
        source_hash: str | None,
        close_ts: datetime | None,
        settlement_ts: datetime | None,
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        no_ask_dollars: Decimal | None,
        last_price_dollars: Decimal | None,
        payload: dict[str, Any],
    ) -> HistoricalMarketSnapshotRecord:
        stmt = select(HistoricalMarketSnapshotRecord).where(
            HistoricalMarketSnapshotRecord.market_ticker == market_ticker,
            HistoricalMarketSnapshotRecord.source_kind == source_kind,
            HistoricalMarketSnapshotRecord.source_id == source_id,
        )
        record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = HistoricalMarketSnapshotRecord(
                market_ticker=market_ticker,
                series_ticker=series_ticker,
                station_id=station_id,
                local_market_day=local_market_day,
                asof_ts=asof_ts,
                source_kind=source_kind,
                source_id=source_id,
                source_hash=source_hash,
                close_ts=close_ts,
                settlement_ts=settlement_ts,
                yes_bid_dollars=yes_bid_dollars,
                yes_ask_dollars=yes_ask_dollars,
                no_ask_dollars=no_ask_dollars,
                last_price_dollars=last_price_dollars,
                payload=payload,
            )
            self.session.add(record)
        else:
            record.series_ticker = series_ticker
            record.station_id = station_id
            record.local_market_day = local_market_day
            record.asof_ts = asof_ts
            record.source_hash = source_hash
            record.close_ts = close_ts
            record.settlement_ts = settlement_ts
            record.yes_bid_dollars = yes_bid_dollars
            record.yes_ask_dollars = yes_ask_dollars
            record.no_ask_dollars = no_ask_dollars
            record.last_price_dollars = last_price_dollars
            record.payload = payload
        await self.session.flush()
        return record

    async def list_historical_market_snapshots(
        self,
        *,
        market_ticker: str | None = None,
        series_ticker: str | None = None,
        source_kind: str | None = None,
        local_market_day: str | None = None,
        before_asof: datetime | None = None,
        limit: int = 500,
    ) -> list[HistoricalMarketSnapshotRecord]:
        stmt = select(HistoricalMarketSnapshotRecord)
        if market_ticker is not None:
            stmt = stmt.where(HistoricalMarketSnapshotRecord.market_ticker == market_ticker)
        if series_ticker is not None:
            stmt = stmt.where(HistoricalMarketSnapshotRecord.series_ticker == series_ticker)
        if source_kind is not None:
            stmt = stmt.where(HistoricalMarketSnapshotRecord.source_kind == source_kind)
        if local_market_day is not None:
            stmt = stmt.where(HistoricalMarketSnapshotRecord.local_market_day == local_market_day)
        if before_asof is not None:
            stmt = stmt.where(HistoricalMarketSnapshotRecord.asof_ts <= before_asof)
        result = await self.session.execute(
            stmt.order_by(
                HistoricalMarketSnapshotRecord.asof_ts.desc(),
                HistoricalMarketSnapshotRecord.source_id.desc(),
                HistoricalMarketSnapshotRecord.id.desc(),
            ).limit(limit)
        )
        return list(result.scalars())

    async def get_latest_historical_market_snapshot(
        self,
        *,
        market_ticker: str,
        before_asof: datetime,
        source_kind: str | None = None,
        local_market_day: str | None = None,
    ) -> HistoricalMarketSnapshotRecord | None:
        records = await self.list_historical_market_snapshots(
            market_ticker=market_ticker,
            source_kind=source_kind,
            local_market_day=local_market_day,
            before_asof=before_asof,
            limit=1,
        )
        return records[0] if records else None

    async def upsert_historical_weather_snapshot(
        self,
        *,
        station_id: str,
        series_ticker: str | None,
        local_market_day: str,
        asof_ts: datetime,
        source_kind: str,
        source_id: str,
        source_hash: str | None,
        observation_ts: datetime | None,
        forecast_updated_ts: datetime | None,
        forecast_high_f: Decimal | None,
        current_temp_f: Decimal | None,
        payload: dict[str, Any],
    ) -> HistoricalWeatherSnapshotRecord:
        stmt = select(HistoricalWeatherSnapshotRecord).where(
            HistoricalWeatherSnapshotRecord.station_id == station_id,
            HistoricalWeatherSnapshotRecord.source_kind == source_kind,
            HistoricalWeatherSnapshotRecord.source_id == source_id,
        )
        record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = HistoricalWeatherSnapshotRecord(
                station_id=station_id,
                series_ticker=series_ticker,
                local_market_day=local_market_day,
                asof_ts=asof_ts,
                source_kind=source_kind,
                source_id=source_id,
                source_hash=source_hash,
                observation_ts=observation_ts,
                forecast_updated_ts=forecast_updated_ts,
                forecast_high_f=forecast_high_f,
                current_temp_f=current_temp_f,
                payload=payload,
            )
            self.session.add(record)
        else:
            record.series_ticker = series_ticker
            record.local_market_day = local_market_day
            record.asof_ts = asof_ts
            record.source_hash = source_hash
            record.observation_ts = observation_ts
            record.forecast_updated_ts = forecast_updated_ts
            record.forecast_high_f = forecast_high_f
            record.current_temp_f = current_temp_f
            record.payload = payload
        await self.session.flush()
        return record

    async def list_historical_weather_snapshots(
        self,
        *,
        station_id: str | None = None,
        series_ticker: str | None = None,
        local_market_day: str | None = None,
        before_asof: datetime | None = None,
        limit: int = 500,
    ) -> list[HistoricalWeatherSnapshotRecord]:
        stmt = select(HistoricalWeatherSnapshotRecord)
        if station_id is not None:
            stmt = stmt.where(HistoricalWeatherSnapshotRecord.station_id == station_id)
        if series_ticker is not None:
            stmt = stmt.where(HistoricalWeatherSnapshotRecord.series_ticker == series_ticker)
        if local_market_day is not None:
            stmt = stmt.where(HistoricalWeatherSnapshotRecord.local_market_day == local_market_day)
        if before_asof is not None:
            stmt = stmt.where(HistoricalWeatherSnapshotRecord.asof_ts <= before_asof)
        result = await self.session.execute(
            stmt.order_by(
                HistoricalWeatherSnapshotRecord.asof_ts.desc(),
                HistoricalWeatherSnapshotRecord.source_id.desc(),
                HistoricalWeatherSnapshotRecord.id.desc(),
            ).limit(limit)
        )
        return list(result.scalars())

    async def get_latest_historical_weather_snapshot(
        self,
        *,
        station_id: str,
        before_asof: datetime,
        local_market_day: str | None = None,
    ) -> HistoricalWeatherSnapshotRecord | None:
        records = await self.list_historical_weather_snapshots(
            station_id=station_id,
            local_market_day=local_market_day,
            before_asof=before_asof,
            limit=1,
        )
        return records[0] if records else None

    async def get_historical_weather_snapshot_by_source(
        self,
        *,
        station_id: str,
        source_kind: str,
        source_id: str,
    ) -> HistoricalWeatherSnapshotRecord | None:
        stmt = select(HistoricalWeatherSnapshotRecord).where(
            HistoricalWeatherSnapshotRecord.station_id == station_id,
            HistoricalWeatherSnapshotRecord.source_kind == source_kind,
            HistoricalWeatherSnapshotRecord.source_id == source_id,
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def upsert_historical_checkpoint_archive(
        self,
        *,
        series_ticker: str,
        market_ticker: str | None,
        station_id: str,
        local_market_day: str,
        checkpoint_label: str,
        checkpoint_ts: datetime,
        captured_at: datetime,
        source_kind: str,
        source_id: str,
        source_hash: str | None,
        observation_ts: datetime | None,
        forecast_updated_ts: datetime | None,
        archive_path: str | None,
        payload: dict[str, Any],
    ) -> HistoricalCheckpointArchiveRecord:
        stmt = select(HistoricalCheckpointArchiveRecord).where(
            HistoricalCheckpointArchiveRecord.series_ticker == series_ticker,
            HistoricalCheckpointArchiveRecord.local_market_day == local_market_day,
            HistoricalCheckpointArchiveRecord.checkpoint_label == checkpoint_label,
        )
        record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = HistoricalCheckpointArchiveRecord(
                series_ticker=series_ticker,
                market_ticker=market_ticker,
                station_id=station_id,
                local_market_day=local_market_day,
                checkpoint_label=checkpoint_label,
                checkpoint_ts=checkpoint_ts,
                captured_at=captured_at,
                source_kind=source_kind,
                source_id=source_id,
                source_hash=source_hash,
                observation_ts=observation_ts,
                forecast_updated_ts=forecast_updated_ts,
                archive_path=archive_path,
                payload=payload,
            )
            self.session.add(record)
        else:
            record.market_ticker = market_ticker
            record.station_id = station_id
            record.checkpoint_ts = checkpoint_ts
            record.captured_at = captured_at
            record.source_kind = source_kind
            record.source_id = source_id
            record.source_hash = source_hash
            record.observation_ts = observation_ts
            record.forecast_updated_ts = forecast_updated_ts
            record.archive_path = archive_path
            record.payload = payload
        await self.session.flush()
        return record

    async def get_historical_checkpoint_archive(
        self,
        *,
        series_ticker: str,
        local_market_day: str,
        checkpoint_label: str,
    ) -> HistoricalCheckpointArchiveRecord | None:
        stmt = select(HistoricalCheckpointArchiveRecord).where(
            HistoricalCheckpointArchiveRecord.series_ticker == series_ticker,
            HistoricalCheckpointArchiveRecord.local_market_day == local_market_day,
            HistoricalCheckpointArchiveRecord.checkpoint_label == checkpoint_label,
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_historical_checkpoint_archives(
        self,
        *,
        series_tickers: list[str] | None = None,
        local_market_day: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        checkpoint_label: str | None = None,
        limit: int = 1000,
    ) -> list[HistoricalCheckpointArchiveRecord]:
        stmt = select(HistoricalCheckpointArchiveRecord)
        if series_tickers:
            stmt = stmt.where(HistoricalCheckpointArchiveRecord.series_ticker.in_(series_tickers))
        if local_market_day is not None:
            stmt = stmt.where(HistoricalCheckpointArchiveRecord.local_market_day == local_market_day)
        if date_from is not None:
            stmt = stmt.where(HistoricalCheckpointArchiveRecord.local_market_day >= date_from)
        if date_to is not None:
            stmt = stmt.where(HistoricalCheckpointArchiveRecord.local_market_day <= date_to)
        if checkpoint_label is not None:
            stmt = stmt.where(HistoricalCheckpointArchiveRecord.checkpoint_label == checkpoint_label)
        result = await self.session.execute(
            stmt.order_by(
                HistoricalCheckpointArchiveRecord.local_market_day.asc(),
                HistoricalCheckpointArchiveRecord.series_ticker.asc(),
                HistoricalCheckpointArchiveRecord.checkpoint_ts.asc(),
            ).limit(limit)
        )
        return list(result.scalars())

    async def upsert_historical_settlement_label(
        self,
        *,
        market_ticker: str,
        series_ticker: str | None,
        local_market_day: str,
        source_kind: str,
        kalshi_result: str | None,
        settlement_value_dollars: Decimal | None,
        settlement_ts: datetime | None,
        crosscheck_status: str,
        crosscheck_high_f: Decimal | None,
        crosscheck_result: str | None,
        payload: dict[str, Any],
    ) -> HistoricalSettlementLabelRecord:
        stmt = select(HistoricalSettlementLabelRecord).where(HistoricalSettlementLabelRecord.market_ticker == market_ticker)
        record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = HistoricalSettlementLabelRecord(
                market_ticker=market_ticker,
                series_ticker=series_ticker,
                local_market_day=local_market_day,
                source_kind=source_kind,
                kalshi_result=kalshi_result,
                settlement_value_dollars=settlement_value_dollars,
                settlement_ts=settlement_ts,
                crosscheck_status=crosscheck_status,
                crosscheck_high_f=crosscheck_high_f,
                crosscheck_result=crosscheck_result,
                payload=payload,
            )
            self.session.add(record)
        else:
            record.series_ticker = series_ticker
            record.local_market_day = local_market_day
            record.source_kind = source_kind
            record.kalshi_result = kalshi_result
            record.settlement_value_dollars = settlement_value_dollars
            record.settlement_ts = settlement_ts
            record.crosscheck_status = crosscheck_status
            record.crosscheck_high_f = crosscheck_high_f
            record.crosscheck_result = crosscheck_result
            record.payload = payload
        await self.session.flush()
        return record

    async def get_historical_settlement_label(self, market_ticker: str) -> HistoricalSettlementLabelRecord | None:
        stmt = select(HistoricalSettlementLabelRecord).where(HistoricalSettlementLabelRecord.market_ticker == market_ticker).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_historical_settlement_labels(
        self,
        *,
        series_tickers: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 1000,
    ) -> list[HistoricalSettlementLabelRecord]:
        stmt = select(HistoricalSettlementLabelRecord)
        if series_tickers:
            stmt = stmt.where(HistoricalSettlementLabelRecord.series_ticker.in_(series_tickers))
        if date_from is not None:
            stmt = stmt.where(HistoricalSettlementLabelRecord.local_market_day >= date_from)
        if date_to is not None:
            stmt = stmt.where(HistoricalSettlementLabelRecord.local_market_day <= date_to)
        result = await self.session.execute(
            stmt.order_by(HistoricalSettlementLabelRecord.local_market_day.asc(), HistoricalSettlementLabelRecord.market_ticker.asc()).limit(limit)
        )
        return list(result.scalars())

    async def create_historical_replay_run(
        self,
        *,
        room_id: str,
        market_ticker: str,
        series_ticker: str | None,
        local_market_day: str,
        checkpoint_label: str,
        checkpoint_ts: datetime,
        status: str,
        agent_pack_version: str | None,
        payload: dict[str, Any],
    ) -> HistoricalReplayRunRecord:
        record = HistoricalReplayRunRecord(
            room_id=room_id,
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            checkpoint_label=checkpoint_label,
            checkpoint_ts=checkpoint_ts,
            status=status,
            agent_pack_version=agent_pack_version,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_historical_replay_run_by_room(self, room_id: str) -> HistoricalReplayRunRecord | None:
        stmt = select(HistoricalReplayRunRecord).where(HistoricalReplayRunRecord.room_id == room_id).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_historical_replay_runs(
        self,
        *,
        market_tickers: list[str] | None = None,
        series_tickers: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        status: str | None = None,
        kalshi_env: str | None = None,
        limit: int = 1000,
    ) -> list[HistoricalReplayRunRecord]:
        stmt = select(HistoricalReplayRunRecord)
        if kalshi_env is not None:
            stmt = stmt.join(Room, HistoricalReplayRunRecord.room_id == Room.id).where(
                Room.kalshi_env == self._resolved_kalshi_env(kalshi_env)
            )
        if market_tickers:
            stmt = stmt.where(HistoricalReplayRunRecord.market_ticker.in_(market_tickers))
        if series_tickers:
            stmt = stmt.where(HistoricalReplayRunRecord.series_ticker.in_(series_tickers))
        if date_from is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.local_market_day >= date_from)
        if date_to is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.local_market_day <= date_to)
        if status is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.status == status)
        result = await self.session.execute(
            stmt.order_by(HistoricalReplayRunRecord.checkpoint_ts.asc(), HistoricalReplayRunRecord.market_ticker.asc()).limit(limit)
        )
        return list(result.scalars())

    async def decision_corpus_replay_date_bounds(
        self,
        *,
        kalshi_env: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, str] | None:
        stmt = (
            select(
                func.min(HistoricalReplayRunRecord.local_market_day),
                func.max(HistoricalReplayRunRecord.local_market_day),
            )
            .join(Room, HistoricalReplayRunRecord.room_id == Room.id)
            .where(
                HistoricalReplayRunRecord.status == "completed",
                HistoricalReplayRunRecord.room_id.is_not(None),
                Room.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            )
        )
        if date_to is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.local_market_day <= date_to)
        start, end = (await self.session.execute(stmt)).one()
        if start is None or end is None:
            return None
        return {"date_from": str(start), "date_to": str(end)}

    async def decision_corpus_promotion_source_metrics(
        self,
        *,
        date_from: str,
        date_to: str,
        kalshi_env: str | None = None,
        new_after_date: str | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        resolved_count = func.sum(
            case((HistoricalSettlementLabelRecord.kalshi_result.in_(["yes", "no"]), 1), else_=0)
        )
        stmt = (
            select(
                func.count(HistoricalReplayRunRecord.id),
                func.coalesce(resolved_count, 0),
            )
            .join(Room, HistoricalReplayRunRecord.room_id == Room.id)
            .outerjoin(
                HistoricalSettlementLabelRecord,
                HistoricalSettlementLabelRecord.market_ticker == HistoricalReplayRunRecord.market_ticker,
            )
            .where(
                HistoricalReplayRunRecord.status == "completed",
                HistoricalReplayRunRecord.room_id.is_not(None),
                HistoricalReplayRunRecord.local_market_day >= date_from,
                HistoricalReplayRunRecord.local_market_day <= date_to,
                Room.kalshi_env == env,
            )
        )
        total_rooms, resolved_rooms = (await self.session.execute(stmt)).one()
        new_resolved_rooms = int(resolved_rooms or 0)
        if new_after_date is not None:
            new_stmt = stmt.where(HistoricalReplayRunRecord.local_market_day > new_after_date)
            _new_total, new_resolved_rooms = (await self.session.execute(new_stmt)).one()
        total = int(total_rooms or 0)
        resolved = int(resolved_rooms or 0)
        return {
            "date_from": date_from,
            "date_to": date_to,
            "kalshi_env": env,
            "completed_rooms": total,
            "resolved_rooms": resolved,
            "new_resolved_rooms": int(new_resolved_rooms or 0),
            "settlement_coverage": (resolved / total) if total else 0.0,
        }

    async def delete_historical_replay_run(self, run_id: str) -> bool:
        record = await self.session.get(HistoricalReplayRunRecord, run_id)
        if record is None:
            return False
        await self.session.delete(record)
        await self.session.flush()
        return True

    async def create_decision_corpus_build(
        self,
        *,
        version: str,
        date_from: date,
        date_to: date,
        source: dict[str, Any],
        filters: dict[str, Any],
        git_sha: str | None = None,
        parent_build_id: str | None = None,
        notes: str | None = None,
    ) -> DecisionCorpusBuildRecord:
        record = DecisionCorpusBuildRecord(
            version=version,
            status="in_progress",
            git_sha=git_sha,
            source=source,
            filters=filters,
            date_from=date_from,
            date_to=date_to,
            parent_build_id=parent_build_id,
            notes=notes,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_decision_corpus_build(self, build_id: str) -> DecisionCorpusBuildRecord | None:
        return await self.session.get(DecisionCorpusBuildRecord, build_id)

    async def list_decision_corpus_builds(
        self,
        *,
        status: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 20,
    ) -> list[DecisionCorpusBuildRecord]:
        stmt = select(DecisionCorpusBuildRecord)
        if status is not None:
            stmt = stmt.where(DecisionCorpusBuildRecord.status == status)
        if date_from is not None:
            stmt = stmt.where(DecisionCorpusBuildRecord.date_to >= date_from)
        if date_to is not None:
            stmt = stmt.where(DecisionCorpusBuildRecord.date_from <= date_to)
        result = await self.session.execute(
            stmt.order_by(DecisionCorpusBuildRecord.created_at.desc(), DecisionCorpusBuildRecord.id.desc()).limit(limit)
        )
        return list(result.scalars())

    async def mark_decision_corpus_build_successful(
        self,
        build_id: str,
        *,
        row_count: int,
    ) -> DecisionCorpusBuildRecord:
        record = await self.get_decision_corpus_build(build_id)
        if record is None:
            raise KeyError(f"Decision corpus build {build_id} not found")
        record.status = "successful"
        record.row_count = row_count
        record.finished_at = datetime.now(UTC)
        record.failure_reason = None
        await self.session.flush()
        return record

    async def mark_decision_corpus_build_failed(
        self,
        build_id: str,
        *,
        failure_reason: str,
        row_count: int | None = None,
    ) -> DecisionCorpusBuildRecord:
        record = await self.get_decision_corpus_build(build_id)
        if record is None:
            raise KeyError(f"Decision corpus build {build_id} not found")
        record.status = "failed"
        record.row_count = row_count
        record.finished_at = datetime.now(UTC)
        record.failure_reason = failure_reason
        await self.session.flush()
        return record

    async def mark_decision_corpus_build_stale(
        self,
        build_id: str,
        *,
        reason: str,
    ) -> DecisionCorpusBuildRecord:
        record = await self.get_decision_corpus_build(build_id)
        if record is None:
            raise KeyError(f"Decision corpus build {build_id} not found")
        record.status = "stale"
        record.failure_reason = reason
        await self.session.flush()
        return record

    async def add_decision_corpus_row(self, **values: Any) -> DecisionCorpusRowRecord:
        build_id = str(values.get("corpus_build_id") or "")
        build = await self.get_decision_corpus_build(build_id)
        if build is None:
            raise KeyError(f"Decision corpus build {build_id} not found")
        if build.status != "in_progress":
            raise ValueError("Decision corpus rows can only be inserted into in-progress builds")
        record = DecisionCorpusRowRecord(**values)
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_decision_corpus_rows(
        self,
        *,
        build_id: str,
        limit: int | None = None,
    ) -> list[DecisionCorpusRowRecord]:
        stmt = (
            select(DecisionCorpusRowRecord)
            .where(DecisionCorpusRowRecord.corpus_build_id == build_id)
            .order_by(
                DecisionCorpusRowRecord.local_market_day.asc(),
                DecisionCorpusRowRecord.checkpoint_ts.asc(),
                DecisionCorpusRowRecord.market_ticker.asc(),
                DecisionCorpusRowRecord.id.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def count_decision_corpus_rows(self, *, build_id: str) -> int:
        stmt = select(func.count()).select_from(DecisionCorpusRowRecord).where(DecisionCorpusRowRecord.corpus_build_id == build_id)
        return int((await self.session.execute(stmt)).scalar_one())

    def decision_corpus_current_checkpoint_name(self, *, kalshi_env: str | None = None) -> str:
        return self._env_stream_name("current_decision_corpus_build", kalshi_env=kalshi_env)

    async def get_current_decision_corpus_build(
        self,
        *,
        kalshi_env: str | None = None,
    ) -> DecisionCorpusBuildRecord | None:
        checkpoint = await self.get_checkpoint(self.decision_corpus_current_checkpoint_name(kalshi_env=kalshi_env))
        build_id = ((checkpoint.payload or {}).get("build_id") if checkpoint is not None else None)
        if not build_id:
            return None
        build = await self.get_decision_corpus_build(str(build_id))
        if build is None or build.status != "successful":
            return None
        return build

    async def promote_decision_corpus_build(
        self,
        build_id: str,
        *,
        kalshi_env: str | None = None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        build = await self.get_decision_corpus_build(build_id)
        if build is None:
            raise KeyError(f"Decision corpus build {build_id} not found")
        if build.status != "successful":
            raise ValueError("Only successful decision corpus builds can be promoted")
        env = self._resolved_kalshi_env(kalshi_env)
        previous = await self.get_current_decision_corpus_build(kalshi_env=env)
        if previous is not None and previous.id == build_id:
            raise ValueError("Decision corpus build is already current for this environment")
        await self.set_checkpoint(
            self.decision_corpus_current_checkpoint_name(kalshi_env=env),
            build_id,
            {
                "build_id": build_id,
                "kalshi_env": env,
                "promoted_at": datetime.now(UTC).isoformat(),
                "previous_build_id": previous.id if previous is not None else None,
                "actor": actor,
            },
        )
        await self.log_ops_event(
            severity="info",
            summary=f"Decision corpus build promoted for {env}",
            source="decision_corpus",
            kalshi_env=env,
            payload={
                "event_kind": "decision_corpus_build_promoted",
                "build_id": build_id,
                "previous_build_id": previous.id if previous is not None else None,
                "kalshi_env": env,
                "actor": actor,
            },
        )
        return {"build_id": build_id, "previous_build_id": previous.id if previous is not None else None, "kalshi_env": env}

    async def list_current_decision_corpus_rows(
        self,
        *,
        kalshi_env: str | None = None,
        limit: int | None = None,
    ) -> list[DecisionCorpusRowRecord]:
        build = await self.get_current_decision_corpus_build(kalshi_env=kalshi_env)
        if build is None:
            return []
        return await self.list_decision_corpus_rows(build_id=build.id, limit=limit)
