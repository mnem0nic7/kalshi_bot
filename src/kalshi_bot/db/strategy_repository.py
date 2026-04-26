from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from kalshi_bot.db.models import (
    CityAssignmentEventRecord,
    CityStrategyAssignment,
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    Signal,
    StrategyCodexRunRecord,
    StrategyPromotionEvent,
    StrategyPromotionRecord,
    StrategyRecord,
    StrategyResultRecord,
    TradeTicketRecord,
)

STRATEGY_PROMOTION_WATCHDOG_STATUSES = {
    "pending",
    "extended",
    "passed",
    "rolled_back",
    "insufficient_data",
}
STRATEGY_PROMOTION_SECONDARY_STATUSES = {
    "pending",
    "failed",
    "synced",
    "ignored_by_operator",
    "not_applicable",
}
CITY_ASSIGNMENT_EVENT_TYPES = {
    "auto_evolve_assign",
    "manual_assign",
    "manual_override",
    "rollback_restore",
    "rollback_delete",
}


class StrategyRepositoryMixin:
    session: Any

    def _resolved_kalshi_env(self, kalshi_env: str | None = None) -> str:
        raise NotImplementedError

    # Strategy presets

    async def seed_strategies(self, presets: list[dict[str, Any]]) -> None:
        for preset in presets:
            stmt = (
                pg_insert(StrategyRecord)
                .values(
                    name=preset["name"],
                    description=preset.get("description"),
                    thresholds=preset["thresholds"],
                    is_active=preset.get("is_active", True),
                    source=preset.get("source", "builtin"),
                    strategy_metadata=preset.get("metadata", {}),
                    created_at=datetime.now(UTC),
                )
                .on_conflict_do_nothing(index_elements=["name"])
            )
            await self.session.execute(stmt)
        await self.session.flush()

    async def create_strategy(
        self,
        *,
        name: str,
        description: str | None,
        thresholds: dict[str, Any],
        is_active: bool = True,
        source: str = "manual",
        metadata: dict[str, Any] | None = None,
    ) -> StrategyRecord:
        record = StrategyRecord(
            name=name,
            description=description,
            thresholds=thresholds,
            is_active=is_active,
            source=source,
            strategy_metadata=dict(metadata or {}),
            created_at=datetime.now(UTC),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_strategies(self, *, active_only: bool = True) -> list[StrategyRecord]:
        stmt = select(StrategyRecord)
        if active_only:
            stmt = stmt.where(StrategyRecord.is_active.is_(True))
        stmt = stmt.order_by(StrategyRecord.id)
        return list((await self.session.execute(stmt)).scalars())

    async def get_strategy_by_name(self, name: str) -> StrategyRecord | None:
        stmt = select(StrategyRecord).where(StrategyRecord.name == name)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def set_strategy_active(self, name: str, *, is_active: bool) -> StrategyRecord | None:
        record = await self.get_strategy_by_name(name)
        if record is None:
            return None
        record.is_active = is_active
        await self.session.flush()
        return record

    async def save_strategy_results(self, results: list[dict[str, Any]]) -> None:
        for row in results:
            run_at = row["run_at"]
            if isinstance(run_at, str):
                run_at = datetime.fromisoformat(run_at)
            date_from = row["date_from"]
            if isinstance(date_from, str):
                date_from = date.fromisoformat(date_from)
            date_to = row["date_to"]
            if isinstance(date_to, str):
                date_to = date.fromisoformat(date_to)
            record = StrategyResultRecord(
                strategy_id=row["strategy_id"],
                corpus_build_id=row.get("corpus_build_id"),
                run_at=run_at,
                date_from=date_from,
                date_to=date_to,
                series_ticker=row["series_ticker"],
                rooms_evaluated=row["rooms_evaluated"],
                trade_count=row["trade_count"],
                resolved_trade_count=row.get("resolved_trade_count", 0),
                unscored_trade_count=row.get("unscored_trade_count", 0),
                win_count=row["win_count"],
                total_pnl_dollars=(
                    Decimal(str(row["total_pnl_dollars"])) if row.get("total_pnl_dollars") is not None else None
                ),
                trade_rate=Decimal(str(row["trade_rate"])) if row.get("trade_rate") is not None else None,
                win_rate=Decimal(str(row["win_rate"])) if row.get("win_rate") is not None else None,
                avg_edge_bps=Decimal(str(row["avg_edge_bps"])) if row.get("avg_edge_bps") is not None else None,
            )
            self.session.add(record)
        await self.session.flush()

    async def get_latest_strategy_results(self, *, corpus_build_id: str | None = None) -> list[StrategyResultRecord]:
        """Return the most recent result per (strategy_id, series_ticker)."""
        base = select(
            StrategyResultRecord.strategy_id,
            StrategyResultRecord.series_ticker,
            func.max(StrategyResultRecord.run_at).label("max_run_at"),
        )
        if corpus_build_id is not None:
            base = base.where(StrategyResultRecord.corpus_build_id == corpus_build_id)
        subq = (
            base
            .group_by(StrategyResultRecord.strategy_id, StrategyResultRecord.series_ticker)
            .subquery()
        )
        stmt = select(StrategyResultRecord).join(
            subq,
            (StrategyResultRecord.strategy_id == subq.c.strategy_id)
            & (StrategyResultRecord.series_ticker == subq.c.series_ticker)
            & (StrategyResultRecord.run_at == subq.c.max_run_at),
        )
        if corpus_build_id is not None:
            stmt = stmt.where(StrategyResultRecord.corpus_build_id == corpus_build_id)
        return list((await self.session.execute(stmt)).scalars())

    async def get_latest_strategy_results_unscoped(self) -> list[StrategyResultRecord]:
        """Compatibility wrapper for callers that intentionally read legacy snapshots."""
        subq = (
            select(
                StrategyResultRecord.strategy_id,
                StrategyResultRecord.series_ticker,
                func.max(StrategyResultRecord.run_at).label("max_run_at"),
            )
            .group_by(StrategyResultRecord.strategy_id, StrategyResultRecord.series_ticker)
            .subquery()
        )
        stmt = select(StrategyResultRecord).join(
            subq,
            (StrategyResultRecord.strategy_id == subq.c.strategy_id)
            & (StrategyResultRecord.series_ticker == subq.c.series_ticker)
            & (StrategyResultRecord.run_at == subq.c.max_run_at),
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_strategy_results_history(
        self,
        *,
        strategy_ids: list[int] | None = None,
        series_ticker: str | None = None,
        run_after: datetime | None = None,
        corpus_build_id: str | None = None,
        limit: int = 500,
    ) -> list[StrategyResultRecord]:
        stmt = select(StrategyResultRecord)
        if corpus_build_id is not None:
            stmt = stmt.where(StrategyResultRecord.corpus_build_id == corpus_build_id)
        if strategy_ids:
            stmt = stmt.where(StrategyResultRecord.strategy_id.in_(strategy_ids))
        if series_ticker is not None:
            stmt = stmt.where(StrategyResultRecord.series_ticker == series_ticker)
        if run_after is not None:
            stmt = stmt.where(StrategyResultRecord.run_at >= run_after)
        stmt = stmt.order_by(StrategyResultRecord.run_at.desc(), StrategyResultRecord.strategy_id.asc(), StrategyResultRecord.series_ticker.asc())
        return list((await self.session.execute(stmt.limit(limit))).scalars())

    async def get_city_strategy_assignment(
        self,
        series_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> CityStrategyAssignment | None:
        stmt = select(CityStrategyAssignment).where(
            CityStrategyAssignment.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CityStrategyAssignment.series_ticker == series_ticker,
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def set_city_strategy_assignment(
        self,
        series_ticker: str,
        strategy_name: str,
        assigned_by: str = "auto_regression",
        *,
        kalshi_env: str | None = None,
        evidence_corpus_build_id: str | None = None,
        evidence_run_at: datetime | str | None = None,
    ) -> CityStrategyAssignment:
        env = self._resolved_kalshi_env(kalshi_env)
        assigned_at = datetime.now(UTC)
        if isinstance(evidence_run_at, str):
            evidence_run_at = datetime.fromisoformat(evidence_run_at)
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        insert_fn = sqlite_insert if dialect_name == "sqlite" else pg_insert
        stmt = (
            insert_fn(CityStrategyAssignment)
            .values(
                kalshi_env=env,
                series_ticker=series_ticker,
                strategy_name=strategy_name,
                assigned_at=assigned_at,
                assigned_by=assigned_by,
                evidence_corpus_build_id=evidence_corpus_build_id,
                evidence_run_at=evidence_run_at,
            )
            .on_conflict_do_update(
                index_elements=["kalshi_env", "series_ticker"],
                set_={
                    "strategy_name": strategy_name,
                    "assigned_at": assigned_at,
                    "assigned_by": assigned_by,
                    "evidence_corpus_build_id": evidence_corpus_build_id,
                    "evidence_run_at": evidence_run_at,
                },
            )
        )
        await self.session.execute(stmt)
        await self.session.flush()
        record = await self.get_city_strategy_assignment(series_ticker, kalshi_env=env)
        if record is None:
            raise RuntimeError(f"City strategy assignment {series_ticker} was not persisted")
        record.kalshi_env = env
        record.strategy_name = strategy_name
        record.assigned_at = assigned_at
        record.assigned_by = assigned_by
        record.evidence_corpus_build_id = evidence_corpus_build_id
        record.evidence_run_at = evidence_run_at
        return record

    async def list_city_strategy_assignments(self, *, kalshi_env: str | None = None) -> list[CityStrategyAssignment]:
        stmt = (
            select(CityStrategyAssignment)
            .where(CityStrategyAssignment.kalshi_env == self._resolved_kalshi_env(kalshi_env))
            .order_by(CityStrategyAssignment.series_ticker)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def delete_city_strategy_assignment(self, series_ticker: str, *, kalshi_env: str | None = None) -> bool:
        from sqlalchemy import delete as sa_delete

        stmt = sa_delete(CityStrategyAssignment).where(
            CityStrategyAssignment.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CityStrategyAssignment.series_ticker == series_ticker,
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return bool(result.rowcount)

    async def count_city_assignments_for_strategy(self, strategy_name: str, *, kalshi_env: str | None = None) -> int:
        stmt = select(func.count()).select_from(CityStrategyAssignment).where(
            CityStrategyAssignment.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CityStrategyAssignment.strategy_name == strategy_name
        )
        return int((await self.session.execute(stmt)).scalar_one())

    async def get_cities_assigned_to_strategy(
        self,
        strategy_name: str,
        *,
        kalshi_env: str | None = None,
    ) -> list[CityStrategyAssignment]:
        stmt = (
            select(CityStrategyAssignment)
            .where(
                CityStrategyAssignment.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CityStrategyAssignment.strategy_name == strategy_name,
            )
            .order_by(CityStrategyAssignment.series_ticker)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def create_strategy_promotion(
        self,
        *,
        promoted_strategy_name: str,
        previous_city_assignments: dict[str, Any] | None = None,
        new_city_assignments: dict[str, Any] | None = None,
        watchdog_due_at: datetime | None = None,
        watchdog_extended_due_at: datetime | None = None,
        kalshi_env: str | None = None,
        promoted_at: datetime | None = None,
        baseline_metrics: dict[str, Any] | None = None,
        promotion_details: dict[str, Any] | None = None,
        trigger_source: str | None = None,
        secondary_sync_status: str = "not_applicable",
    ) -> StrategyPromotionRecord:
        previous_city_assignments = dict(previous_city_assignments or {})
        new_city_assignments = dict(new_city_assignments or {})
        if set(previous_city_assignments) != set(new_city_assignments):
            raise ValueError("previous_city_assignments and new_city_assignments must have identical city keys")
        if not promoted_strategy_name or not promoted_strategy_name.strip():
            raise ValueError("promoted_strategy_name must be non-empty")
        if secondary_sync_status not in STRATEGY_PROMOTION_SECONDARY_STATUSES:
            raise ValueError(f"Invalid secondary_sync_status: {secondary_sync_status}")
        record = StrategyPromotionRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            promoted_strategy_name=promoted_strategy_name.strip(),
            promoted_at=promoted_at or datetime.now(UTC),
            previous_city_assignments=dict(previous_city_assignments),
            new_city_assignments=dict(new_city_assignments),
            baseline_metrics=dict(baseline_metrics or {}),
            promotion_details=dict(promotion_details or {}),
            watchdog_due_at=watchdog_due_at or datetime.now(UTC) + timedelta(days=7),
            watchdog_extended_due_at=watchdog_extended_due_at or datetime.now(UTC) + timedelta(days=14),
            watchdog_status="pending",
            trigger_source=trigger_source,
            secondary_sync_status=secondary_sync_status,
            secondary_rollback_status="not_applicable",
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_strategy_promotion(self, promotion_id: int) -> StrategyPromotionRecord | None:
        if not isinstance(promotion_id, int):
            raise TypeError("promotion_id must be an int")
        return await self.session.get(StrategyPromotionRecord, promotion_id)

    async def update_strategy_promotion(self, promotion_id: int, **values: Any) -> StrategyPromotionRecord:
        if not isinstance(promotion_id, int):
            raise TypeError("promotion_id must be an int")
        record = await self.session.get(StrategyPromotionRecord, promotion_id)
        if record is None:
            raise KeyError(f"Strategy promotion {promotion_id} not found")
        for key, value in values.items():
            if not hasattr(record, key):
                raise AttributeError(f"Unknown strategy promotion field {key}")
            if key == "watchdog_status" and value not in STRATEGY_PROMOTION_WATCHDOG_STATUSES:
                raise ValueError(f"Invalid watchdog_status: {value}")
            if key in {"secondary_sync_status", "secondary_rollback_status"} and value not in STRATEGY_PROMOTION_SECONDARY_STATUSES:
                raise ValueError(f"Invalid {key}: {value}")
            setattr(record, key, value)
        record.updated_at = datetime.now(UTC)
        await self.session.flush()
        return record

    async def list_strategy_promotion_records(
        self,
        *,
        kalshi_env: str | None = None,
        watchdog_status: str | None = None,
        promoted_strategy_name: str | None = None,
        limit: int = 50,
    ) -> list[StrategyPromotionRecord]:
        stmt = select(StrategyPromotionRecord)
        if kalshi_env is not None:
            stmt = stmt.where(StrategyPromotionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env))
        if watchdog_status is not None:
            stmt = stmt.where(StrategyPromotionRecord.watchdog_status == watchdog_status)
        if promoted_strategy_name is not None:
            stmt = stmt.where(StrategyPromotionRecord.promoted_strategy_name == promoted_strategy_name)
        stmt = stmt.order_by(StrategyPromotionRecord.promoted_at.desc(), StrategyPromotionRecord.id.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_strategy_promotions_due_for_watchdog(
        self,
        *,
        now: datetime,
        kalshi_env: str | None = None,
        limit: int = 50,
    ) -> list[StrategyPromotionRecord]:
        stmt = (
            select(StrategyPromotionRecord)
            .where(
                StrategyPromotionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                StrategyPromotionRecord.watchdog_status.in_(["pending", "extended"]),
                StrategyPromotionRecord.watchdog_due_at <= now,
            )
            .order_by(StrategyPromotionRecord.watchdog_due_at.asc(), StrategyPromotionRecord.id.asc())
            .limit(limit)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_strategy_promotions_due_for_secondary_sync(
        self,
        *,
        kalshi_env: str | None = None,
        limit: int = 50,
    ) -> list[StrategyPromotionRecord]:
        stmt = (
            select(StrategyPromotionRecord)
            .where(
                StrategyPromotionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                or_(
                    StrategyPromotionRecord.secondary_sync_status.in_(["pending", "failed"]),
                    StrategyPromotionRecord.secondary_rollback_status.in_(["pending", "failed"]),
                ),
            )
            .order_by(StrategyPromotionRecord.updated_at.asc(), StrategyPromotionRecord.id.asc())
            .limit(limit)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_auto_evolve_locked_city_tickers(self, *, kalshi_env: str | None = None) -> set[str]:
        stmt = select(StrategyPromotionRecord).where(
            StrategyPromotionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            or_(
                StrategyPromotionRecord.watchdog_status.in_(["pending", "extended", "insufficient_data"]),
                (
                    StrategyPromotionRecord.watchdog_status == "passed"
                )
                & StrategyPromotionRecord.secondary_sync_status.in_(["pending", "failed"]),
                (
                    StrategyPromotionRecord.watchdog_status == "rolled_back"
                )
                & StrategyPromotionRecord.secondary_rollback_status.in_(["pending", "failed"]),
            ),
        )
        locked: set[str] = set()
        for promotion in (await self.session.execute(stmt)).scalars():
            locked.update(str(key) for key in dict(promotion.new_city_assignments or {}).keys())
        return locked

    async def record_city_assignment_event(
        self,
        *,
        series_ticker: str,
        new_strategy: str | None,
        previous_strategy: str | None = None,
        promotion_id: int | None = None,
        kalshi_env: str | None = None,
        event_type: str = "manual_assign",
        actor: str = "operator",
        note: str | None = None,
        metadata: dict[str, Any] | None = None,
        event_metadata: dict[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> CityAssignmentEventRecord:
        if not series_ticker or not series_ticker.strip():
            raise ValueError("series_ticker must be non-empty")
        if not actor or not actor.strip():
            raise ValueError("actor must be non-empty")
        if not event_type or event_type.strip() not in CITY_ASSIGNMENT_EVENT_TYPES:
            raise ValueError(f"Invalid city assignment event_type: {event_type}")
        if promotion_id is not None and not isinstance(promotion_id, int):
            raise TypeError("promotion_id must be an int")
        if promotion_id is not None and await self.session.get(StrategyPromotionRecord, promotion_id) is None:
            raise KeyError(f"Strategy promotion {promotion_id} not found")
        record = CityAssignmentEventRecord(
            promotion_id=promotion_id,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            series_ticker=series_ticker.strip(),
            previous_strategy=previous_strategy.strip() if previous_strategy and previous_strategy.strip() else None,
            new_strategy=new_strategy.strip() if isinstance(new_strategy, str) and new_strategy.strip() else None,
            event_type=event_type.strip(),
            actor=actor.strip(),
            note=note,
            event_metadata=dict(event_metadata if event_metadata is not None else (metadata or {})),
            created_at=created_at or datetime.now(UTC),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_city_assignment_events(
        self,
        *,
        kalshi_env: str | None = None,
        series_ticker: str | None = None,
        promotion_id: int | None = None,
        event_type: str | None = None,
        limit: int = 50,
    ) -> list[CityAssignmentEventRecord]:
        stmt = select(CityAssignmentEventRecord)
        if kalshi_env is not None:
            stmt = stmt.where(CityAssignmentEventRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env))
        if series_ticker is not None:
            stmt = stmt.where(CityAssignmentEventRecord.series_ticker == series_ticker)
        if promotion_id is not None:
            stmt = stmt.where(CityAssignmentEventRecord.promotion_id == promotion_id)
        if event_type is not None:
            stmt = stmt.where(CityAssignmentEventRecord.event_type == event_type)
        stmt = stmt.order_by(CityAssignmentEventRecord.created_at.desc(), CityAssignmentEventRecord.id.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def latest_city_assignment_event(
        self,
        *,
        series_ticker: str,
        kalshi_env: str | None = None,
        event_types: set[str] | None = None,
    ) -> CityAssignmentEventRecord | None:
        stmt = select(CityAssignmentEventRecord).where(
            CityAssignmentEventRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CityAssignmentEventRecord.series_ticker == series_ticker,
        )
        if event_types:
            stmt = stmt.where(CityAssignmentEventRecord.event_type.in_(sorted(event_types)))
        stmt = stmt.order_by(CityAssignmentEventRecord.created_at.desc(), CityAssignmentEventRecord.id.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def set_city_strategy_assignment_with_event(
        self,
        series_ticker: str,
        strategy_name: str,
        *,
        assigned_by: str = "auto_regression",
        promotion_id: int | None = None,
        kalshi_env: str | None = None,
        event_type: str = "manual_assign",
        note: str | None = None,
        metadata: dict[str, Any] | None = None,
        event_metadata: dict[str, Any] | None = None,
        evidence_corpus_build_id: str | None = None,
        evidence_run_at: datetime | str | None = None,
    ) -> tuple[CityStrategyAssignment, CityAssignmentEventRecord]:
        previous_assignment = await self.get_city_strategy_assignment(series_ticker, kalshi_env=kalshi_env)
        previous_strategy_name = previous_assignment.strategy_name if previous_assignment is not None else None
        assignment = await self.set_city_strategy_assignment(
            series_ticker,
            strategy_name,
            assigned_by=assigned_by,
            kalshi_env=kalshi_env,
            evidence_corpus_build_id=evidence_corpus_build_id,
            evidence_run_at=evidence_run_at,
        )
        event = await self.record_city_assignment_event(
            series_ticker=series_ticker,
            previous_strategy=previous_strategy_name,
            new_strategy=strategy_name,
            promotion_id=promotion_id,
            kalshi_env=kalshi_env,
            event_type=event_type,
            actor=assigned_by,
            note=note,
            metadata=event_metadata if event_metadata is not None else metadata,
        )
        return assignment, event

    async def record_strategy_promotion(
        self,
        *,
        strategy: str,
        from_state: str,
        to_state: str,
        actor: str,
        evidence_ref: str | None = None,
        notes: str | None = None,
        kalshi_env: str | None = None,
    ) -> StrategyPromotionEvent:
        """Append one row to the strategy promotion audit log (P2-3)."""
        if not strategy or not strategy.strip():
            raise ValueError("strategy must be non-empty")
        if not actor or not actor.strip():
            raise ValueError("actor must be non-empty")
        if from_state == to_state:
            raise ValueError("from_state and to_state must differ")
        record = StrategyPromotionEvent(
            id=str(uuid4()),
            strategy=strategy.strip(),
            from_state=from_state.strip(),
            to_state=to_state.strip(),
            actor=actor.strip(),
            evidence_ref=evidence_ref,
            notes=notes,
            kalshi_env=self._resolved_kalshi_env(kalshi_env) if kalshi_env is not None else None,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_strategy_promotions(
        self,
        *,
        strategy: str | None = None,
        kalshi_env: str | None = None,
        limit: int = 50,
    ) -> list[StrategyPromotionEvent]:
        stmt = select(StrategyPromotionEvent)
        if strategy is not None:
            stmt = stmt.where(StrategyPromotionEvent.strategy == strategy)
        if kalshi_env is not None:
            stmt = stmt.where(
                StrategyPromotionEvent.kalshi_env == self._resolved_kalshi_env(kalshi_env)
            )
        stmt = stmt.order_by(StrategyPromotionEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def clear_city_strategy_assignments(self, *, assigned_by: str | None = None) -> int:
        from sqlalchemy import delete as sa_delete

        stmt = sa_delete(CityStrategyAssignment)
        if assigned_by is not None:
            stmt = stmt.where(CityStrategyAssignment.assigned_by == assigned_by)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0

    async def get_strategy_regression_rooms(
        self, date_from: datetime, date_to: datetime
    ) -> list[dict[str, Any]]:
        """Return canonical replay rows with the latest signal, ticket, and settlement evidence."""
        latest_signal = (
            select(
                Signal.room_id.label("room_id"),
                Signal.market_ticker.label("signal_market_ticker"),
                Signal.edge_bps.label("edge_bps"),
                Signal.fair_yes_dollars.label("fair_yes_dollars"),
                Signal.payload.label("signal_payload"),
                func.row_number().over(
                    partition_by=Signal.room_id,
                    order_by=(Signal.created_at.desc(), Signal.id.desc()),
                ).label("rn"),
            ).subquery()
        )
        latest_ticket = (
            select(
                TradeTicketRecord.room_id.label("room_id"),
                TradeTicketRecord.side.label("ticket_side"),
                TradeTicketRecord.yes_price_dollars.label("ticket_yes_price_dollars"),
                TradeTicketRecord.count_fp.label("ticket_count_fp"),
                TradeTicketRecord.status.label("ticket_status"),
                func.row_number().over(
                    partition_by=TradeTicketRecord.room_id,
                    order_by=(TradeTicketRecord.created_at.desc(), TradeTicketRecord.id.desc()),
                ).label("rn"),
            ).subquery()
        )
        stmt = (
            select(
                HistoricalReplayRunRecord.room_id.label("room_id"),
                HistoricalReplayRunRecord.market_ticker.label("market_ticker"),
                HistoricalReplayRunRecord.series_ticker.label("series_ticker"),
                latest_signal.c.edge_bps,
                latest_signal.c.fair_yes_dollars,
                latest_signal.c.signal_payload,
                latest_ticket.c.ticket_side,
                latest_ticket.c.ticket_yes_price_dollars,
                latest_ticket.c.ticket_count_fp,
                latest_ticket.c.ticket_status,
                HistoricalSettlementLabelRecord.settlement_value_dollars,
                HistoricalSettlementLabelRecord.kalshi_result,
            )
            .join(
                latest_signal,
                (latest_signal.c.room_id == HistoricalReplayRunRecord.room_id) & (latest_signal.c.rn == 1),
            )
            .outerjoin(
                latest_ticket,
                (latest_ticket.c.room_id == HistoricalReplayRunRecord.room_id) & (latest_ticket.c.rn == 1),
            )
            .outerjoin(
                HistoricalSettlementLabelRecord,
                HistoricalSettlementLabelRecord.market_ticker == HistoricalReplayRunRecord.market_ticker,
            )
            .where(
                HistoricalReplayRunRecord.status == "completed",
                HistoricalReplayRunRecord.room_id.is_not(None),
                HistoricalReplayRunRecord.checkpoint_ts >= date_from,
                HistoricalReplayRunRecord.checkpoint_ts <= date_to,
            )
            .order_by(HistoricalReplayRunRecord.checkpoint_ts.asc(), HistoricalReplayRunRecord.market_ticker.asc())
        )
        rows = list((await self.session.execute(stmt)).mappings())
        return [dict(r) for r in rows]

    async def create_strategy_codex_run(
        self,
        *,
        mode: str,
        status: str,
        trigger_source: str,
        window_days: int,
        series_ticker: str | None,
        strategy_name: str | None,
        operator_brief: str | None,
        provider: str,
        model: str | None,
        payload: dict[str, Any],
    ) -> StrategyCodexRunRecord:
        record = StrategyCodexRunRecord(
            mode=mode,
            status=status,
            trigger_source=trigger_source,
            window_days=window_days,
            series_ticker=series_ticker,
            strategy_name=strategy_name,
            operator_brief=operator_brief,
            provider=provider,
            model=model,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_strategy_codex_run(self, run_id: str) -> StrategyCodexRunRecord | None:
        return await self.session.get(StrategyCodexRunRecord, run_id)

    async def update_strategy_codex_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        payload: dict[str, Any] | None = None,
        error_text: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> StrategyCodexRunRecord:
        record = await self.session.get(StrategyCodexRunRecord, run_id)
        if record is None:
            raise KeyError(f"Strategy codex run {run_id} not found")
        if status is not None:
            record.status = status
        if payload is not None:
            record.payload = payload
        record.error_text = error_text
        if started_at is not None:
            record.started_at = started_at
        if finished_at is not None:
            record.finished_at = finished_at
        await self.session.flush()
        return record

    async def list_strategy_codex_runs(self, *, limit: int = 10) -> list[StrategyCodexRunRecord]:
        result = await self.session.execute(
            select(StrategyCodexRunRecord).order_by(StrategyCodexRunRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

    async def fail_stale_strategy_codex_runs(
        self,
        *,
        stale_before: datetime,
        error_text: str,
    ) -> list[StrategyCodexRunRecord]:
        result = await self.session.execute(
            select(StrategyCodexRunRecord).where(
                StrategyCodexRunRecord.status.in_(("queued", "running")),
                or_(
                    StrategyCodexRunRecord.started_at < stale_before,
                    StrategyCodexRunRecord.started_at.is_(None) & (StrategyCodexRunRecord.created_at < stale_before),
                ),
            )
        )
        records = list(result.scalars())
        if not records:
            return []
        finished_at = datetime.now(UTC)
        for record in records:
            record.status = "failed"
            record.error_text = error_text
            record.finished_at = finished_at
        await self.session.flush()
        return records
