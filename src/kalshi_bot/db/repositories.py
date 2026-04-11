from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.core.enums import DeploymentColor, MessageKind, RiskStatus, RoomStage
from kalshi_bot.core.schemas import (
    AgentPack,
    EvaluationSummary,
    MemoryNotePayload,
    ResearchClaim,
    ResearchDossier,
    ResearchSourceCard,
    RoomCreate,
    RoomMessageCreate,
    SelfImproveCritiqueItem,
    TrainingReadiness,
    TradeTicket,
)
from kalshi_bot.db.models import (
    AgentPackRecord,
    Artifact,
    Checkpoint,
    CritiqueRunRecord,
    DeploymentControl,
    EvaluationRunRecord,
    FillRecord,
    MarketState,
    MemoryEmbedding,
    MemoryNoteRecord,
    OpsEvent,
    OrderRecord,
    PositionRecord,
    PromotionEventRecord,
    ResearchClaimRecord,
    ResearchDossierRecord,
    ResearchRunRecord,
    ResearchSourceRecord,
    RawExchangeEvent,
    RawWeatherEvent,
    RiskVerdictRecord,
    Room,
    RoomCampaignRecord,
    RoomMessage,
    RoomResearchHealthRecord,
    Signal,
    TrainingDatasetBuildItemRecord,
    TrainingDatasetBuildRecord,
    TrainingReadinessRecord,
    TradeTicketRecord,
)


class PlatformRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ensure_deployment_control(
        self,
        color: str,
        *,
        initial_active_color: str | None = None,
        initial_kill_switch_enabled: bool | None = None,
    ) -> DeploymentControl:
        control = await self.session.get(DeploymentControl, "default")
        if control is None:
            control = DeploymentControl(
                id="default",
                active_color=initial_active_color or DeploymentColor.BLUE.value,
                shadow_color=color,
                kill_switch_enabled=bool(initial_kill_switch_enabled),
            )
            self.session.add(control)
            await self.session.flush()
        return control

    async def get_deployment_control(self) -> DeploymentControl:
        return await self.ensure_deployment_control(DeploymentColor.BLUE.value)

    async def set_active_color(self, color: DeploymentColor | str) -> DeploymentControl:
        control = await self.ensure_deployment_control(str(color))
        if control.active_color != str(color):
            control.execution_lock_holder = None
        control.active_color = str(color)
        notes = dict(control.notes or {})
        agent_pack_notes = dict(notes.get("agent_packs") or {})
        if agent_pack_notes:
            active_version = agent_pack_notes.get("blue_version") if str(color) == DeploymentColor.BLUE.value else agent_pack_notes.get("green_version")
            if active_version is not None:
                agent_pack_notes["active_version"] = active_version
                agent_pack_notes["champion_version"] = active_version
                notes["agent_packs"] = agent_pack_notes
                control.notes = notes
        await self.session.flush()
        return control

    async def set_kill_switch(self, enabled: bool) -> DeploymentControl:
        control = await self.ensure_deployment_control(DeploymentColor.BLUE.value)
        control.kill_switch_enabled = enabled
        if enabled:
            control.execution_lock_holder = None
        await self.session.flush()
        return control

    async def acquire_execution_lock(self, holder: str, color: str) -> bool:
        control = await self.ensure_deployment_control(color)
        if control.active_color != color or control.kill_switch_enabled:
            return False
        if control.execution_lock_holder not in (None, holder):
            return False
        control.execution_lock_holder = holder
        await self.session.flush()
        return True

    async def release_execution_lock(self, holder: str) -> None:
        control = await self.ensure_deployment_control(DeploymentColor.BLUE.value)
        if control.execution_lock_holder == holder:
            control.execution_lock_holder = None
            await self.session.flush()

    async def update_deployment_notes(self, notes: dict[str, Any]) -> DeploymentControl:
        control = await self.ensure_deployment_control(DeploymentColor.BLUE.value)
        control.notes = notes
        await self.session.flush()
        return control

    async def create_room(
        self,
        room: RoomCreate,
        *,
        active_color: str,
        shadow_mode: bool,
        kill_switch_enabled: bool,
        kalshi_env: str,
        agent_pack_version: str | None = None,
        evaluation_run_id: str | None = None,
        role_models: dict[str, Any] | None = None,
    ) -> Room:
        record = Room(
            name=room.name,
            market_ticker=room.market_ticker,
            prompt=room.prompt,
            kalshi_env=kalshi_env,
            stage=RoomStage.TRIGGERED.value,
            active_color=active_color,
            shadow_mode=shadow_mode,
            kill_switch_enabled=kill_switch_enabled,
            agent_pack_version=agent_pack_version,
            evaluation_run_id=evaluation_run_id,
            role_models=role_models or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def save_room_campaign(
        self,
        *,
        room_id: str,
        campaign_id: str,
        trigger_source: str,
        city_bucket: str | None = None,
        market_regime_bucket: str | None = None,
        difficulty_bucket: str | None = None,
        outcome_bucket: str | None = None,
        dossier_artifact_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RoomCampaignRecord:
        record = await self.get_room_campaign(room_id)
        if record is None:
            record = RoomCampaignRecord(
                room_id=room_id,
                campaign_id=campaign_id,
                trigger_source=trigger_source,
                city_bucket=city_bucket,
                market_regime_bucket=market_regime_bucket,
                difficulty_bucket=difficulty_bucket,
                outcome_bucket=outcome_bucket,
                dossier_artifact_id=dossier_artifact_id,
                payload=payload or {},
            )
            self.session.add(record)
        else:
            record.campaign_id = campaign_id
            record.trigger_source = trigger_source
            record.city_bucket = city_bucket
            record.market_regime_bucket = market_regime_bucket
            record.difficulty_bucket = difficulty_bucket
            record.outcome_bucket = outcome_bucket
            record.dossier_artifact_id = dossier_artifact_id
            record.payload = payload or record.payload
        await self.session.flush()
        return record

    async def update_room_campaign(
        self,
        room_id: str,
        *,
        dossier_artifact_id: str | None = None,
        payload_updates: dict[str, Any] | None = None,
    ) -> RoomCampaignRecord | None:
        record = await self.get_room_campaign(room_id)
        if record is None:
            return None
        if dossier_artifact_id is not None:
            record.dossier_artifact_id = dossier_artifact_id
        if payload_updates:
            record.payload = {**(record.payload or {}), **payload_updates}
        await self.session.flush()
        return record

    async def get_room_campaign(self, room_id: str) -> RoomCampaignRecord | None:
        stmt = select(RoomCampaignRecord).where(RoomCampaignRecord.room_id == room_id).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_room_campaigns(
        self,
        *,
        limit: int = 200,
        campaign_id: str | None = None,
    ) -> list[RoomCampaignRecord]:
        stmt = select(RoomCampaignRecord)
        if campaign_id is not None:
            stmt = stmt.where(RoomCampaignRecord.campaign_id == campaign_id)
        result = await self.session.execute(stmt.order_by(RoomCampaignRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms(self, limit: int = 25) -> list[Room]:
        result = await self.session.execute(select(Room).order_by(Room.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms_for_export(
        self,
        *,
        limit: int = 100,
        market_ticker: str | None = None,
        include_non_complete: bool = False,
    ) -> list[Room]:
        stmt = select(Room)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        if not include_non_complete:
            stmt = stmt.where(Room.stage == RoomStage.COMPLETE.value)
        result = await self.session.execute(stmt.order_by(Room.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms_for_learning(
        self,
        *,
        since: datetime,
        limit: int = 500,
        pack_version: str | None = None,
        color: str | None = None,
        market_ticker: str | None = None,
    ) -> list[Room]:
        stmt = (
            select(Room)
            .where(
                Room.stage == RoomStage.COMPLETE.value,
                Room.created_at >= since,
                (Room.shadow_mode.is_(True)) | (Room.kalshi_env != "production"),
            )
            .order_by(Room.created_at.asc())
        )
        if pack_version is not None:
            stmt = stmt.where(Room.agent_pack_version == pack_version)
        if color is not None:
            stmt = stmt.where(Room.active_color == color)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        result = await self.session.execute(stmt.limit(limit))
        return list(result.scalars())

    async def count_active_rooms(self) -> int:
        stmt = select(func.count()).select_from(Room).where(Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]))
        return int((await self.session.execute(stmt)).scalar_one())

    async def get_room(self, room_id: str) -> Room | None:
        return await self.session.get(Room, room_id)

    async def get_latest_active_room_for_market(self, market_ticker: str) -> Room | None:
        stmt = (
            select(Room)
            .where(Room.market_ticker == market_ticker, Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]))
            .order_by(Room.updated_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def update_room_stage(self, room_id: str, stage: RoomStage) -> None:
        room = await self.get_room(room_id)
        if room is not None:
            room.stage = stage.value
            room.updated_at = datetime.now(UTC)
            await self.session.flush()

    async def update_room_runtime(
        self,
        room_id: str,
        *,
        agent_pack_version: str | None = None,
        evaluation_run_id: str | None = None,
        role_models: dict[str, Any] | None = None,
    ) -> Room | None:
        room = await self.get_room(room_id)
        if room is None:
            return None
        if agent_pack_version is not None:
            room.agent_pack_version = agent_pack_version
        if evaluation_run_id is not None:
            room.evaluation_run_id = evaluation_run_id
        if role_models is not None:
            room.role_models = role_models
        room.updated_at = datetime.now(UTC)
        await self.session.flush()
        return room

    async def append_message(self, room_id: str, message: RoomMessageCreate) -> RoomMessage:
        sequence_query: Select[tuple[int]] = select(func.coalesce(func.max(RoomMessage.sequence), 0) + 1).where(
            RoomMessage.room_id == room_id
        )
        next_sequence = (await self.session.execute(sequence_query)).scalar_one()
        record = RoomMessage(
            room_id=room_id,
            role=message.role.value,
            kind=message.kind.value,
            stage=message.stage.value if message.stage else None,
            sequence=next_sequence,
            content=message.content,
            payload=message.payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_messages(self, room_id: str, after_sequence: int = 0) -> list[RoomMessage]:
        stmt = (
            select(RoomMessage)
            .where(RoomMessage.room_id == room_id, RoomMessage.sequence > after_sequence)
            .order_by(RoomMessage.sequence.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars())

    async def save_artifact(
        self,
        *,
        room_id: str,
        artifact_type: str,
        source: str,
        title: str,
        payload: dict[str, Any],
        message_id: str | None = None,
        url: str | None = None,
        external_id: str | None = None,
    ) -> Artifact:
        record = Artifact(
            room_id=room_id,
            message_id=message_id,
            artifact_type=artifact_type,
            source=source,
            title=title,
            url=url,
            external_id=external_id,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_artifacts(self, *, room_id: str, artifact_type: str | None = None, limit: int = 50) -> list[Artifact]:
        stmt = select(Artifact).where(Artifact.room_id == room_id)
        if artifact_type is not None:
            stmt = stmt.where(Artifact.artifact_type == artifact_type)
        result = await self.session.execute(stmt.order_by(Artifact.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def get_latest_artifact(self, *, room_id: str, artifact_type: str) -> Artifact | None:
        stmt = (
            select(Artifact)
            .where(Artifact.room_id == room_id, Artifact.artifact_type == artifact_type)
            .order_by(Artifact.updated_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def log_exchange_event(self, stream_name: str, event_type: str, payload: dict[str, Any], market_ticker: str | None = None) -> None:
        self.session.add(
            RawExchangeEvent(stream_name=stream_name, event_type=event_type, payload=payload, market_ticker=market_ticker)
        )
        await self.session.flush()

    async def log_weather_event(self, station_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self.session.add(RawWeatherEvent(station_id=station_id, event_type=event_type, payload=payload))
        await self.session.flush()

    async def upsert_market_state(
        self,
        market_ticker: str,
        *,
        snapshot: dict[str, Any],
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        last_trade_dollars: Decimal | None,
    ) -> MarketState:
        observed_at = datetime.now(UTC)
        insert_values = {
            "market_ticker": market_ticker,
            "source": "kalshi",
            "snapshot": snapshot,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "last_trade_dollars": last_trade_dollars,
            "observed_at": observed_at,
            "created_at": observed_at,
            "updated_at": observed_at,
        }
        update_values = {
            "snapshot": snapshot,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "last_trade_dollars": last_trade_dollars,
            "observed_at": observed_at,
            "updated_at": observed_at,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(MarketState).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(MarketState).values(**insert_values)
        else:
            record = await self.session.get(MarketState, market_ticker)
            if record is None:
                record = MarketState(market_ticker=market_ticker, snapshot={})
                self.session.add(record)
            record.snapshot = snapshot
            record.yes_bid_dollars = yes_bid_dollars
            record.yes_ask_dollars = yes_ask_dollars
            record.last_trade_dollars = last_trade_dollars
            record.observed_at = observed_at
            await self.session.flush()
            return record

        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[MarketState.market_ticker],
                set_=update_values,
            )
        )
        await self.session.flush()
        return await self.session.get(MarketState, market_ticker)

    async def get_market_state(self, market_ticker: str) -> MarketState | None:
        return await self.session.get(MarketState, market_ticker)

    async def get_latest_signal_for_room(self, room_id: str) -> Signal | None:
        stmt = select(Signal).where(Signal.room_id == room_id).order_by(Signal.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_signal(
        self,
        *,
        room_id: str,
        market_ticker: str,
        fair_yes_dollars: Decimal,
        edge_bps: int,
        confidence: float,
        summary: str,
        payload: dict[str, Any],
    ) -> Signal:
        record = Signal(
            room_id=room_id,
            market_ticker=market_ticker,
            fair_yes_dollars=fair_yes_dollars,
            edge_bps=edge_bps,
            confidence=confidence,
            summary=summary,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_latest_trade_ticket_for_room(self, room_id: str) -> TradeTicketRecord | None:
        stmt = (
            select(TradeTicketRecord)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(TradeTicketRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_trade_ticket(self, room_id: str, ticket: TradeTicket, client_order_id: str, message_id: str | None = None) -> TradeTicketRecord:
        record = TradeTicketRecord(
            room_id=room_id,
            message_id=message_id,
            market_ticker=ticket.market_ticker,
            action=ticket.action.value,
            side=ticket.side.value,
            yes_price_dollars=ticket.yes_price_dollars,
            count_fp=ticket.count_fp,
            time_in_force=ticket.time_in_force,
            client_order_id=client_order_id,
            payload=ticket.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_latest_risk_verdict_for_room(self, room_id: str) -> RiskVerdictRecord | None:
        stmt = (
            select(RiskVerdictRecord)
            .where(RiskVerdictRecord.room_id == room_id)
            .order_by(RiskVerdictRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_risk_verdict(
        self,
        *,
        room_id: str,
        ticket_id: str,
        status: RiskStatus,
        reasons: list[str],
        approved_notional_dollars: Decimal | None,
        approved_count_fp: Decimal | None,
        payload: dict[str, Any],
    ) -> RiskVerdictRecord:
        record = RiskVerdictRecord(
            room_id=room_id,
            ticket_id=ticket_id,
            status=status.value,
            reasons=reasons,
            approved_notional_dollars=approved_notional_dollars,
            approved_count_fp=approved_count_fp,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def save_order(
        self,
        *,
        ticket_id: str | None,
        client_order_id: str,
        market_ticker: str,
        status: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        kalshi_order_id: str | None = None,
    ) -> OrderRecord:
        record = OrderRecord(
            trade_ticket_id=ticket_id,
            client_order_id=client_order_id,
            market_ticker=market_ticker,
            status=status,
            side=side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            raw=raw,
            kalshi_order_id=kalshi_order_id,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def upsert_order(
        self,
        *,
        client_order_id: str,
        market_ticker: str,
        status: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        ticket_id: str | None = None,
        kalshi_order_id: str | None = None,
    ) -> OrderRecord:
        stmt = select(OrderRecord).where(OrderRecord.client_order_id == client_order_id)
        record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = OrderRecord(
                trade_ticket_id=ticket_id,
                client_order_id=client_order_id,
                market_ticker=market_ticker,
                status=status,
                side=side,
                action=action,
                yes_price_dollars=yes_price_dollars,
                count_fp=count_fp,
                raw=raw,
                kalshi_order_id=kalshi_order_id,
            )
            self.session.add(record)
        else:
            record.trade_ticket_id = ticket_id or record.trade_ticket_id
            record.market_ticker = market_ticker
            record.status = status
            record.side = side
            record.action = action
            record.yes_price_dollars = yes_price_dollars
            record.count_fp = count_fp
            record.raw = raw
            record.kalshi_order_id = kalshi_order_id or record.kalshi_order_id
        await self.session.flush()
        return record

    async def list_orders_for_room(self, room_id: str) -> list[OrderRecord]:
        stmt = (
            select(OrderRecord)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(OrderRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def save_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        order_id: str | None = None,
        trade_id: str | None = None,
        is_taker: bool = True,
    ) -> FillRecord:
        record = FillRecord(
            order_id=order_id,
            trade_id=trade_id,
            market_ticker=market_ticker,
            side=side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            raw=raw,
            is_taker=is_taker,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def upsert_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        order_id: str | None = None,
        trade_id: str | None = None,
        is_taker: bool = True,
    ) -> FillRecord:
        record: FillRecord | None = None
        if trade_id is not None:
            stmt = select(FillRecord).where(FillRecord.trade_id == trade_id)
            record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = FillRecord(
                order_id=order_id,
                trade_id=trade_id,
                market_ticker=market_ticker,
                side=side,
                action=action,
                yes_price_dollars=yes_price_dollars,
                count_fp=count_fp,
                raw=raw,
                is_taker=is_taker,
            )
            self.session.add(record)
        else:
            record.order_id = order_id or record.order_id
            record.market_ticker = market_ticker
            record.side = side
            record.action = action
            record.yes_price_dollars = yes_price_dollars
            record.count_fp = count_fp
            record.raw = raw
            record.is_taker = is_taker
        await self.session.flush()
        return record

    async def list_fills_for_room(self, room_id: str) -> list[FillRecord]:
        stmt = (
            select(FillRecord)
            .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(FillRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def upsert_position(
        self,
        *,
        market_ticker: str,
        subaccount: int,
        side: str,
        count_fp: Decimal,
        average_price_dollars: Decimal,
        raw: dict[str, Any],
    ) -> PositionRecord:
        stmt = select(PositionRecord).where(
            PositionRecord.market_ticker == market_ticker, PositionRecord.subaccount == subaccount
        )
        existing = (await self.session.execute(stmt)).scalar_one_or_none()
        if existing is None:
            existing = PositionRecord(
                market_ticker=market_ticker,
                subaccount=subaccount,
                side=side,
                count_fp=count_fp,
                average_price_dollars=average_price_dollars,
                raw=raw,
            )
            self.session.add(existing)
        else:
            existing.side = side
            existing.count_fp = count_fp
            existing.average_price_dollars = average_price_dollars
            existing.raw = raw
        await self.session.flush()
        return existing

    async def log_ops_event(self, *, severity: str, summary: str, source: str, payload: dict[str, Any], room_id: str | None = None) -> OpsEvent:
        record = OpsEvent(room_id=room_id, severity=severity, summary=summary, source=source, payload=payload)
        self.session.add(record)
        await self.session.flush()
        return record

    async def create_research_run(self, *, market_ticker: str, trigger_reason: str, payload: dict[str, Any] | None = None) -> ResearchRunRecord:
        record = ResearchRunRecord(
            market_ticker=market_ticker,
            trigger_reason=trigger_reason,
            status="running",
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_research_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> ResearchRunRecord:
        record = await self.session.get(ResearchRunRecord, run_id)
        if record is None:
            raise KeyError(f"Research run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.error_text = error_text
        if payload is not None:
            record.payload = payload
        await self.session.flush()
        return record

    async def save_research_sources(self, *, run_id: str, market_ticker: str, sources: list[ResearchSourceCard]) -> dict[str, ResearchSourceRecord]:
        created: dict[str, ResearchSourceRecord] = {}
        for source in sources:
            record = ResearchSourceRecord(
                research_run_id=run_id,
                market_ticker=market_ticker,
                source_key=source.source_key,
                source_class=source.source_class,
                trust_tier=source.trust_tier,
                publisher=source.publisher,
                title=source.title,
                url=source.url,
                snippet=source.snippet,
                retrieved_at=source.retrieved_at,
                payload=source.model_dump(mode="json"),
            )
            self.session.add(record)
            await self.session.flush()
            created[source.source_key] = record
        return created

    async def save_research_claims(
        self,
        *,
        run_id: str,
        market_ticker: str,
        claims: list[ResearchClaim],
        source_records: dict[str, ResearchSourceRecord],
    ) -> list[ResearchClaimRecord]:
        created: list[ResearchClaimRecord] = []
        for claim in claims:
            record = ResearchClaimRecord(
                research_run_id=run_id,
                research_source_id=source_records.get(claim.source_key).id if claim.source_key in source_records else None,
                market_ticker=market_ticker,
                source_key=claim.source_key,
                claim_text=claim.claim,
                stance=claim.stance,
                settlement_critical=claim.settlement_critical,
                freshness_seconds=claim.freshness_seconds,
                payload=claim.model_dump(mode="json"),
            )
            self.session.add(record)
            await self.session.flush()
            created.append(record)
        return created

    async def upsert_research_dossier(self, dossier: ResearchDossier) -> ResearchDossierRecord:
        record = await self.session.get(ResearchDossierRecord, dossier.market_ticker)
        if record is None:
            record = ResearchDossierRecord(market_ticker=dossier.market_ticker, payload={})
            self.session.add(record)
        record.status = dossier.status
        record.mode = dossier.mode
        record.confidence = dossier.summary.research_confidence
        record.source_count = len(dossier.sources)
        record.contradiction_count = dossier.contradiction_count
        record.unresolved_count = dossier.unresolved_count
        record.settlement_covered = dossier.settlement_covered
        record.last_run_id = dossier.last_run_id
        record.expires_at = dossier.freshness.expires_at
        record.payload = dossier.model_dump(mode="json")
        await self.session.flush()
        return record

    async def get_research_dossier(self, market_ticker: str) -> ResearchDossierRecord | None:
        return await self.session.get(ResearchDossierRecord, market_ticker)

    async def list_research_dossiers(self, limit: int = 100) -> list[ResearchDossierRecord]:
        result = await self.session.execute(select(ResearchDossierRecord).order_by(ResearchDossierRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_research_runs(
        self,
        *,
        market_ticker: str | None = None,
        status: str | None = None,
        limit: int = 20,
    ) -> list[ResearchRunRecord]:
        stmt = select(ResearchRunRecord)
        if market_ticker is not None:
            stmt = stmt.where(ResearchRunRecord.market_ticker == market_ticker)
        if status is not None:
            stmt = stmt.where(ResearchRunRecord.status == status)
        result = await self.session.execute(stmt.order_by(ResearchRunRecord.started_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_research_sources(self, *, run_id: str) -> list[ResearchSourceRecord]:
        result = await self.session.execute(
            select(ResearchSourceRecord).where(ResearchSourceRecord.research_run_id == run_id).order_by(ResearchSourceRecord.retrieved_at.desc())
        )
        return list(result.scalars())

    async def list_research_claims(self, *, run_id: str) -> list[ResearchClaimRecord]:
        result = await self.session.execute(
            select(ResearchClaimRecord).where(ResearchClaimRecord.research_run_id == run_id).order_by(ResearchClaimRecord.created_at.asc())
        )
        return list(result.scalars())

    async def upsert_room_research_health(
        self,
        *,
        room_id: str,
        market_ticker: str,
        dossier_status: str,
        gate_passed: bool,
        valid_dossier: bool,
        good_for_training: bool,
        quality_score: float,
        citation_coverage_score: float,
        settlement_clarity_score: float,
        freshness_score: float,
        contradiction_count: int,
        structured_completeness_score: float,
        fair_value_score: float,
        dossier_artifact_id: str | None,
        payload: dict[str, Any],
    ) -> RoomResearchHealthRecord:
        record = await self.session.get(RoomResearchHealthRecord, room_id)
        if record is None:
            record = RoomResearchHealthRecord(room_id=room_id, market_ticker=market_ticker, payload={})
            self.session.add(record)
        record.market_ticker = market_ticker
        record.dossier_status = dossier_status
        record.gate_passed = gate_passed
        record.valid_dossier = valid_dossier
        record.good_for_training = good_for_training
        record.quality_score = quality_score
        record.citation_coverage_score = citation_coverage_score
        record.settlement_clarity_score = settlement_clarity_score
        record.freshness_score = freshness_score
        record.contradiction_count = contradiction_count
        record.structured_completeness_score = structured_completeness_score
        record.fair_value_score = fair_value_score
        record.dossier_artifact_id = dossier_artifact_id
        record.payload = payload
        await self.session.flush()
        return record

    async def get_room_research_health(self, room_id: str) -> RoomResearchHealthRecord | None:
        return await self.session.get(RoomResearchHealthRecord, room_id)

    async def list_room_research_health(
        self,
        *,
        limit: int = 200,
        good_for_training: bool | None = None,
    ) -> list[RoomResearchHealthRecord]:
        stmt = select(RoomResearchHealthRecord)
        if good_for_training is not None:
            stmt = stmt.where(RoomResearchHealthRecord.good_for_training.is_(good_for_training))
        result = await self.session.execute(stmt.order_by(RoomResearchHealthRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

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

    async def list_training_dataset_builds(self, limit: int = 20) -> list[TrainingDatasetBuildRecord]:
        result = await self.session.execute(
            select(TrainingDatasetBuildRecord).order_by(TrainingDatasetBuildRecord.created_at.desc()).limit(limit)
        )
        return list(result.scalars())

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

    async def save_memory_note(self, *, room_id: str | None, payload: MemoryNotePayload, embedding: list[float] | None, provider: str) -> MemoryNoteRecord:
        note = MemoryNoteRecord(
            room_id=room_id,
            title=payload.title,
            summary=payload.summary,
            tags=payload.tags,
            linked_message_ids=payload.linked_message_ids,
        )
        self.session.add(note)
        await self.session.flush()
        self.session.add(
            MemoryEmbedding(memory_note_id=note.id, provider=provider, embedding=embedding, payload={"tags": payload.tags})
        )
        await self.session.flush()
        return note

    async def list_recent_memory_notes(self, limit: int = 10) -> list[MemoryNoteRecord]:
        result = await self.session.execute(select(MemoryNoteRecord).order_by(MemoryNoteRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def get_latest_memory_note_for_room(self, room_id: str) -> MemoryNoteRecord | None:
        stmt = (
            select(MemoryNoteRecord)
            .where(MemoryNoteRecord.room_id == room_id)
            .order_by(MemoryNoteRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_positions(self, limit: int = 50) -> list[PositionRecord]:
        result = await self.session.execute(select(PositionRecord).order_by(PositionRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_ops_events(self, limit: int = 50) -> list[OpsEvent]:
        result = await self.session.execute(select(OpsEvent).order_by(OpsEvent.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def set_checkpoint(self, stream_name: str, cursor: str | None, payload: dict[str, Any]) -> Checkpoint:
        stmt = select(Checkpoint).where(Checkpoint.stream_name == stream_name)
        checkpoint = (await self.session.execute(stmt)).scalar_one_or_none()
        if checkpoint is None:
            checkpoint = Checkpoint(stream_name=stream_name, cursor=cursor, payload=payload)
            self.session.add(checkpoint)
        else:
            checkpoint.cursor = cursor
            checkpoint.payload = payload
        await self.session.flush()
        return checkpoint

    async def get_checkpoint(self, stream_name: str) -> Checkpoint | None:
        stmt = select(Checkpoint).where(Checkpoint.stream_name == stream_name)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_exchange_events(
        self,
        *,
        stream_name: str | None = None,
        event_type: str | None = None,
        market_ticker: str | None = None,
        limit: int = 50,
    ) -> list[RawExchangeEvent]:
        stmt = select(RawExchangeEvent)
        if stream_name is not None:
            stmt = stmt.where(RawExchangeEvent.stream_name == stream_name)
        if event_type is not None:
            stmt = stmt.where(RawExchangeEvent.event_type == event_type)
        if market_ticker is not None:
            stmt = stmt.where(RawExchangeEvent.market_ticker == market_ticker)
        stmt = stmt.order_by(RawExchangeEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())
