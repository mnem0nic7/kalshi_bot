from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import defer
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.config import get_settings
from kalshi_bot.core.enums import RiskStatus, RoomOrigin, RoomStage
from kalshi_bot.core.fixed_point import as_decimal
from kalshi_bot.core.signal_payload import capital_bucket_from_signal_payload as _capital_bucket_from_signal_payload
from kalshi_bot.core.schemas import (
    MemoryNotePayload,
    PortfolioBucketSnapshot,
    ResearchClaim,
    ResearchDossier,
    ResearchSourceCard,
    RoomCreate,
    RoomMessageCreate,
    TradeTicket,
)
from kalshi_bot.db.deployment_control_repository import DeploymentControlRepositoryMixin
from kalshi_bot.db.learning_repository import LearningRepositoryMixin
from kalshi_bot.db.strategy_repository import StrategyRepositoryMixin
from kalshi_bot.db.web_auth_repository import WebAuthRepositoryMixin
from kalshi_bot.db.models import (
    Artifact,
    Checkpoint,
    ClimatologyPriorRecord,
    CryptoMarketCandlestickRecord,
    CryptoMarketSnapshotRecord,
    CryptoModelArtifactRecord,
    CryptoSpotOHLCRecord,
    DecisionTraceRecord,
    FillRecord,
    ForecastSnapshotRecord,
    MarketPriceHistory,
    MarketState,
    MemoryEmbedding,
    MemoryNoteRecord,
    OpsEvent,
    OrderRecord,
    PositionRecord,
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
    RoomStrategyAuditRecord,
    Signal,
    SourceHealthLogRecord,
    TradeTicketRecord,
    WeatherBootstrapEventRecord,
    WeatherBootstrapHistoricalEvidenceRecord,
)


def _quantize_money(value: Any) -> Decimal:
    return as_decimal(value).quantize(Decimal("0.0001"))


def _fill_fee_dollars(fill: FillRecord) -> Decimal:
    raw = fill.raw if isinstance(fill.raw, dict) else {}
    for key in ("fee_cost", "fee_dollars", "fee"):
        value = raw.get(key)
        if value not in (None, ""):
            try:
                return _quantize_money(value)
            except Exception:
                return Decimal("0")
    return Decimal("0")


def _candidate_trade_ticket_client_order_ids(client_order_id: str) -> list[str]:
    candidates: list[str] = []

    def add(value: str) -> None:
        if value and value not in candidates:
            candidates.append(value)

    raw = str(client_order_id).strip()
    add(raw)
    quote_base, sep, quote_num = raw.rpartition("_q")
    if sep and quote_num.isdigit():
        add(quote_base)
    else:
        quote_base = raw
    for base in list(candidates):
        for suffix in (":maker", ":taker"):
            if base.endswith(suffix):
                add(base[: -len(suffix)])
    return candidates


def _settled_buy_fill_pnl(fill: FillRecord) -> Decimal | None:
    if fill.action != "buy" or fill.settlement_result not in {"win", "loss"}:
        return None
    yes_price = _quantize_money(fill.yes_price_dollars)
    count = as_decimal(fill.count_fp)
    won = fill.settlement_result == "win"
    if fill.side == "yes":
        contract_price = yes_price
    elif fill.side == "no":
        contract_price = Decimal("1.0000") - yes_price
    else:
        return None
    gross = ((Decimal("1.0000") if won else Decimal("0")) - contract_price) * count
    return _quantize_money(gross - _fill_fee_dollars(fill))


class PlatformRepository(DeploymentControlRepositoryMixin, WebAuthRepositoryMixin, StrategyRepositoryMixin, LearningRepositoryMixin):
    def __init__(self, session: AsyncSession, *, kalshi_env: str | None = None) -> None:
        self.session = session
        self.kalshi_env = kalshi_env if kalshi_env is not None else get_settings().kalshi_env

    def _resolved_kalshi_env(self, kalshi_env: str | None = None) -> str:
        env = (kalshi_env or self.kalshi_env or "demo").strip()
        return env or "demo"

    def _env_stream_name(self, prefix: str, *, kalshi_env: str | None = None, suffix: str | None = None) -> str:
        parts = [prefix, self._resolved_kalshi_env(kalshi_env)]
        if suffix:
            parts.append(suffix)
        return ":".join(parts)

    @staticmethod
    def _fill_pnl_metrics(all_fills: list[FillRecord]) -> dict[str, Any]:
        """Summarize buy-fill trade P&L from a bounded fill set."""
        buys: dict[tuple[str, str], list[FillRecord]] = {}
        sells: dict[tuple[str, str], list[FillRecord]] = {}
        for fill in all_fills:
            key = (fill.market_ticker, fill.side)
            if fill.action == "buy":
                buys.setdefault(key, []).append(fill)
            elif fill.action == "sell":
                sells.setdefault(key, []).append(fill)

        def _fill_time(fill: FillRecord) -> datetime:
            value = fill.created_at
            if value is None:
                return datetime.min.replace(tzinfo=UTC)
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)

        won = 0.0
        total = 0.0
        scored_contracts = 0.0
        unresolved_contracts = 0.0
        unresolved_trade_count = 0
        trade_pnls_with_time: list[tuple[datetime, float]] = []
        for key, buy_fills in buys.items():
            _ticker, side = key
            sell_fills = sells.get(key, [])
            avg_sell: float | None = None
            if sell_fills:
                sell_count = sum(float(s.count_fp) for s in sell_fills)
                if sell_count > 0:
                    avg_sell = (
                        sum(float(s.yes_price_dollars) * float(s.count_fp) for s in sell_fills)
                        / sell_count
                    )
            for buy_fill in buy_fills:
                count = float(buy_fill.count_fp)
                buy_px = float(buy_fill.yes_price_dollars)
                total += count
                pnl: float | None = None
                profitable = False
                if avg_sell is not None:
                    if side == "yes":
                        pnl = (avg_sell - buy_px) * count
                    else:
                        pnl = (buy_px - avg_sell) * count
                    profitable = pnl > 0
                elif buy_fill.settlement_result is not None:
                    won_leg = buy_fill.settlement_result == "win"
                    if side == "yes":
                        pnl = ((1.0 if won_leg else 0.0) - buy_px) * count
                    else:
                        pnl = ((1.0 if won_leg else 0.0) - (1.0 - buy_px)) * count
                    profitable = won_leg
                if profitable:
                    won += count
                if pnl is None:
                    unresolved_trade_count += 1
                    unresolved_contracts += count
                else:
                    scored_contracts += count
                    trade_pnls_with_time.append((_fill_time(buy_fill), pnl))

        ordered_pnls = [
            pnl for _created_at, pnl in sorted(trade_pnls_with_time, key=lambda item: item[0])
        ]
        trade_count = len(ordered_pnls)
        wins_pnl = [p for p in ordered_pnls if p > 0]
        losses_pnl = [p for p in ordered_pnls if p < 0]
        total_pnl = sum(ordered_pnls)
        avg_win_dollars = (sum(wins_pnl) / len(wins_pnl)) if wins_pnl else None
        avg_loss_dollars = (sum(losses_pnl) / len(losses_pnl)) if losses_pnl else None
        mean_return_dollars = (total_pnl / trade_count) if trade_count else None

        stdev_dollars: float | None = None
        sharpe_per_trade: float | None = None
        if trade_count >= 2 and mean_return_dollars is not None:
            variance = sum((p - mean_return_dollars) ** 2 for p in ordered_pnls) / trade_count
            stdev = variance ** 0.5
            stdev_dollars = stdev
            if stdev > 0:
                sharpe_per_trade = mean_return_dollars / stdev

        running_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown = 0.0
        for pnl in ordered_pnls:
            running_pnl += pnl
            peak_pnl = max(peak_pnl, running_pnl)
            max_drawdown = max(max_drawdown, peak_pnl - running_pnl)

        return {
            "won_contracts": won,
            "total_contracts": total,
            "scored_contracts": scored_contracts,
            "unresolved_contracts": unresolved_contracts,
            "trade_count": trade_count,
            "win_count": len(wins_pnl),
            "loss_count": len(losses_pnl),
            "unresolved_trade_count": unresolved_trade_count,
            "avg_win_dollars": avg_win_dollars,
            "avg_loss_dollars": avg_loss_dollars,
            "total_pnl_dollars": total_pnl,
            "mean_return_dollars": mean_return_dollars,
            "max_drawdown_dollars": max_drawdown if trade_count else None,
            "stdev_dollars": stdev_dollars,
            "sharpe_per_trade": sharpe_per_trade,
        }

    async def create_room(
        self,
        room: RoomCreate,
        *,
        active_color: str,
        shadow_mode: bool,
        kill_switch_enabled: bool,
        kalshi_env: str,
        room_origin: str | None = None,
        agent_pack_version: str | None = None,
        evaluation_run_id: str | None = None,
        role_models: dict[str, Any] | None = None,
    ) -> Room:
        record = Room(
            name=room.name,
            market_ticker=room.market_ticker,
            room_origin=room_origin or (RoomOrigin.SHADOW.value if shadow_mode else RoomOrigin.LIVE.value),
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

    async def list_rooms(
        self,
        limit: int = 25,
        *,
        origins: list[str] | None = None,
    ) -> list[Room]:
        stmt = select(Room)
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        result = await self.session.execute(stmt.order_by(Room.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms_for_export(
        self,
        *,
        limit: int = 100,
        market_ticker: str | None = None,
        include_non_complete: bool = False,
        origins: list[str] | None = None,
        updated_since: datetime | None = None,
    ) -> list[Room]:
        stmt = select(Room)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        if updated_since is not None:
            stmt = stmt.where(Room.updated_at >= updated_since)
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
        origins: list[str] | None = None,
        include_frozen_production_live: bool = False,
    ) -> list[Room]:
        stmt = (
            select(Room)
            .where(
                Room.stage == RoomStage.COMPLETE.value,
                Room.created_at >= since,
            )
            .order_by(Room.created_at.asc())
        )
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        else:
            learning_room_filter = (Room.shadow_mode.is_(True)) | (Room.kalshi_env != "production")
            if include_frozen_production_live:
                learning_room_filter = learning_room_filter | (
                    (Room.kalshi_env == "production")
                    & (Room.room_origin == RoomOrigin.LIVE.value)
                    & (Room.shadow_mode.is_(False))
                )
            stmt = stmt.where(Room.room_origin.in_([RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value]), learning_room_filter)
        if pack_version is not None:
            stmt = stmt.where(Room.agent_pack_version == pack_version)
        if color is not None:
            stmt = stmt.where(Room.active_color == color)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        result = await self.session.execute(stmt.limit(limit))
        return list(result.scalars())

    async def count_active_rooms(
        self,
        *,
        color: str | None = None,
        kalshi_env: str | None = None,
        updated_within_seconds: int | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value])
        )
        if color is not None:
            stmt = stmt.where(Room.active_color == color)
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        if updated_within_seconds is not None:
            cutoff = datetime.now(UTC) - timedelta(seconds=updated_within_seconds)
            stmt = stmt.where(Room.updated_at >= cutoff)
        return int((await self.session.execute(stmt)).scalar_one())

    async def list_active_rooms(
        self,
        *,
        kalshi_env: str | None = None,
        updated_within_seconds: int | None = None,
        limit: int = 20,
    ) -> list[Room]:
        stmt = select(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value])
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        if updated_within_seconds is not None:
            cutoff = datetime.now(UTC) - timedelta(seconds=updated_within_seconds)
            stmt = stmt.where(Room.updated_at >= cutoff)
        stmt = stmt.order_by(Room.updated_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_room(self, room_id: str) -> Room | None:
        return await self.session.get(Room, room_id)

    async def delete_room(self, room_id: str) -> bool:
        room = await self.get_room(room_id)
        if room is None:
            return False
        await self.session.delete(room)
        await self.session.flush()
        return True

    async def get_latest_active_room_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> Room | None:
        stmt = (
            select(Room)
            .where(Room.market_ticker == market_ticker, Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]))
            .order_by(Room.updated_at.desc())
            .limit(1)
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_room_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> Room | None:
        stmt = select(Room).where(Room.market_ticker == market_ticker).order_by(Room.updated_at.desc()).limit(1)
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def reap_orphaned_rooms(self, *, color: str, kalshi_env: str | None = None) -> list[str]:
        """Mark all non-terminal rooms for *color* as failed. Returns IDs reaped."""
        stmt = select(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]),
            Room.active_color == color,
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        rooms = list((await self.session.execute(stmt)).scalars())
        now = datetime.now(UTC)
        for room in rooms:
            room.stage = RoomStage.FAILED.value
            room.updated_at = now
        await self.session.flush()
        return [room.id for room in rooms]

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
        kalshi_env: str | None = None,
        snapshot: dict[str, Any],
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        last_trade_dollars: Decimal | None,
    ) -> MarketState:
        observed_at = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "kalshi_env": env,
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
            record = await self.session.get(MarketState, (env, market_ticker))
            if record is None:
                record = MarketState(kalshi_env=env, market_ticker=market_ticker, snapshot={})
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
                index_elements=[MarketState.kalshi_env, MarketState.market_ticker],
                set_=update_values,
            )
        )
        await self.session.flush()
        return await self.session.get(MarketState, (env, market_ticker))

    async def get_market_state(self, market_ticker: str, *, kalshi_env: str | None = None) -> MarketState | None:
        env = self._resolved_kalshi_env(kalshi_env)
        return await self.session.get(MarketState, (env, market_ticker))

    async def list_market_states(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[MarketState]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(MarketState).where(
            MarketState.kalshi_env == env,
            MarketState.market_ticker.in_(market_tickers),
        )
        return list((await self.session.execute(stmt)).scalars())

    async def get_latest_signal_for_room(self, room_id: str) -> Signal | None:
        stmt = select(Signal).where(Signal.room_id == room_id).order_by(Signal.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def latest_signal_payloads_for_markets(
        self,
        *,
        market_tickers: list[str],
        kalshi_env: str,
    ) -> dict[str, dict[str, Any]]:
        if not market_tickers:
            return {}
        stmt = (
            select(Signal.market_ticker, Signal.payload, Signal.fair_yes_dollars, Signal.edge_bps, Signal.confidence)
            .join(Room, Signal.room_id == Room.id)
            .where(
                Signal.market_ticker.in_(market_tickers),
                Room.kalshi_env == kalshi_env,
            )
            .order_by(Signal.market_ticker.asc(), Signal.created_at.desc())
        )
        results = await self.session.execute(stmt)
        payloads: dict[str, dict[str, Any]] = {}
        for market_ticker, payload, fair_yes_dollars, edge_bps, confidence in results:
            if market_ticker not in payloads:
                payloads[str(market_ticker)] = {
                    "fair_yes_dollars": str(fair_yes_dollars),
                    "edge_bps": edge_bps,
                    "confidence": confidence,
                    **dict(payload or {}),
                }
        return payloads

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

    async def record_market_price_snapshot(
        self,
        *,
        market_ticker: str,
        kalshi_env: str | None = None,
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        mid_dollars: Decimal | None,
        last_trade_dollars: Decimal | None,
        volume: int | None,
        observed_at: datetime,
    ) -> MarketPriceHistory:
        record = MarketPriceHistory(
            id=str(uuid4()),
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            yes_bid_dollars=yes_bid_dollars,
            yes_ask_dollars=yes_ask_dollars,
            mid_dollars=mid_dollars,
            last_trade_dollars=last_trade_dollars,
            volume=volume,
            observed_at=observed_at,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def fetch_recent_prices(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
        window: timedelta,
    ) -> list[MarketPriceHistory]:
        cutoff = datetime.now(UTC) - window
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(MarketPriceHistory)
            .where(
                MarketPriceHistory.kalshi_env == env,
                MarketPriceHistory.market_ticker == market_ticker,
                MarketPriceHistory.observed_at >= cutoff,
            )
            .order_by(MarketPriceHistory.observed_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def record_crypto_market_snapshot(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        asset_symbol: str,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        event_ticker: str | None = None,
        title: str | None = None,
        status: str | None = None,
        open_time: datetime | None = None,
        close_time: datetime | None = None,
        expected_expiration_time: datetime | None = None,
        target_price_dollars: Decimal | None = None,
        yes_bid_dollars: Decimal | None = None,
        yes_ask_dollars: Decimal | None = None,
        no_bid_dollars: Decimal | None = None,
        no_ask_dollars: Decimal | None = None,
        last_price_dollars: Decimal | None = None,
        volume: int | None = None,
        open_interest: int | None = None,
        settlement_result: str | None = None,
        observed_at: datetime | None = None,
        source_kind: str = "live",
        payload: dict[str, Any] | None = None,
    ) -> CryptoMarketSnapshotRecord:
        now = datetime.now(UTC)
        observed = observed_at or now
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "series_ticker": series_ticker,
            "market_ticker": market_ticker,
            "event_ticker": event_ticker,
            "asset_symbol": asset_symbol,
            "frequency": frequency,
            "title": title,
            "status": status,
            "open_time": open_time,
            "close_time": close_time,
            "expected_expiration_time": expected_expiration_time,
            "target_price_dollars": target_price_dollars,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "no_bid_dollars": no_bid_dollars,
            "no_ask_dollars": no_ask_dollars,
            "last_price_dollars": last_price_dollars,
            "volume": volume,
            "open_interest": open_interest,
            "settlement_result": settlement_result,
            "observed_at": observed,
            "source_kind": source_kind,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketSnapshotRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketSnapshotRecord).values(**insert_values)
        else:
            record = CryptoMarketSnapshotRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record

        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketSnapshotRecord.kalshi_env,
                    CryptoMarketSnapshotRecord.market_ticker,
                    CryptoMarketSnapshotRecord.observed_at,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoMarketSnapshotRecord).where(
                    CryptoMarketSnapshotRecord.kalshi_env == env,
                    CryptoMarketSnapshotRecord.market_ticker == market_ticker,
                    CryptoMarketSnapshotRecord.observed_at == observed,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_market_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        status: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoMarketSnapshotRecord]:
        stmt = select(CryptoMarketSnapshotRecord).where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if status is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.status == status)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        if defer_payload:
            # Skip the large raw-response JSON for analytics callers that never
            # read it — it dominates row width and TOAST/heap I/O on this table.
            stmt = stmt.options(defer(CryptoMarketSnapshotRecord.payload))
        stmt = stmt.order_by(CryptoMarketSnapshotRecord.observed_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_crypto_settled_market_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[CryptoMarketSnapshotRecord]:
        """Return snapshots only for markets that actually settled.

        A market counts as settled when any of its snapshots carries a
        ``settlement_result`` of ``yes``/``no``. Unlike ``list_crypto_market_snapshots``,
        which caps the most-recent N *raw* snapshots (mostly still-open markets),
        this scopes to settled markets first so the ``limit`` governs the number
        of trainable decision points rather than raw rows.
        """
        env = self._resolved_kalshi_env(kalshi_env)
        settled_markets = select(CryptoMarketSnapshotRecord.market_ticker).where(
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
        )
        if frequency is not None:
            settled_markets = settled_markets.where(CryptoMarketSnapshotRecord.frequency == frequency)
        stmt = select(CryptoMarketSnapshotRecord).where(
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.market_ticker.in_(settled_markets),
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        stmt = stmt.order_by(CryptoMarketSnapshotRecord.observed_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_crypto_snapshot_asset_symbols(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        since: datetime | None = None,
    ) -> list[str]:
        stmt = select(CryptoMarketSnapshotRecord.asset_symbol).distinct().where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        rows = (await self.session.execute(stmt)).scalars().all()
        return sorted({row for row in rows if row})

    async def list_latest_crypto_market_snapshots(
        self,
        *,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        limit: int = 200,
    ) -> list[CryptoMarketSnapshotRecord]:
        rows = await self.list_crypto_market_snapshots(
            frequency=frequency,
            kalshi_env=kalshi_env,
            asset_symbols=asset_symbols,
            limit=max(limit * 6, limit),
        )
        latest: list[CryptoMarketSnapshotRecord] = []
        seen: set[str] = set()
        for row in rows:
            if row.market_ticker in seen:
                continue
            seen.add(row.market_ticker)
            latest.append(row)
            if len(latest) >= limit:
                break
        return latest

    async def get_latest_crypto_market_snapshot(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> CryptoMarketSnapshotRecord | None:
        stmt = (
            select(CryptoMarketSnapshotRecord)
            .where(
                CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoMarketSnapshotRecord.market_ticker == market_ticker,
            )
            .order_by(CryptoMarketSnapshotRecord.observed_at.desc(), CryptoMarketSnapshotRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def upsert_crypto_market_candlestick(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        asset_symbol: str,
        end_period_ts: datetime,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        period_interval: int = 1,
        open_dollars: Decimal | None = None,
        high_dollars: Decimal | None = None,
        low_dollars: Decimal | None = None,
        close_dollars: Decimal | None = None,
        volume: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> CryptoMarketCandlestickRecord:
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "series_ticker": series_ticker,
            "market_ticker": market_ticker,
            "asset_symbol": asset_symbol,
            "frequency": frequency,
            "period_interval": period_interval,
            "end_period_ts": end_period_ts,
            "open_dollars": open_dollars,
            "high_dollars": high_dollars,
            "low_dollars": low_dollars,
            "close_dollars": close_dollars,
            "volume": volume,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        else:
            record = CryptoMarketCandlestickRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketCandlestickRecord.kalshi_env,
                    CryptoMarketCandlestickRecord.market_ticker,
                    CryptoMarketCandlestickRecord.period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoMarketCandlestickRecord).where(
                    CryptoMarketCandlestickRecord.kalshi_env == env,
                    CryptoMarketCandlestickRecord.market_ticker == market_ticker,
                    CryptoMarketCandlestickRecord.period_interval == period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts == end_period_ts,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_market_candlesticks(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        market_ticker: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoMarketCandlestickRecord]:
        stmt = select(CryptoMarketCandlestickRecord).where(
            CryptoMarketCandlestickRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.frequency == frequency)
        if market_ticker is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.market_ticker == market_ticker)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.end_period_ts >= since)
        if defer_payload:
            stmt = stmt.options(defer(CryptoMarketCandlestickRecord.payload))
        stmt = stmt.order_by(CryptoMarketCandlestickRecord.end_period_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def upsert_crypto_spot_ohlc(
        self,
        *,
        provider: str,
        asset_symbol: str,
        end_ts: datetime,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        quote_currency: str = "USD",
        interval_seconds: int = 900,
        start_ts: datetime | None = None,
        open_dollars: Decimal | None = None,
        high_dollars: Decimal | None = None,
        low_dollars: Decimal | None = None,
        close_dollars: Decimal | None = None,
        volume: Decimal | None = None,
        observed_at: datetime | None = None,
        source_kind: str = "spot_ohlc",
        source_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> CryptoSpotOHLCRecord:
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "provider": provider,
            "asset_symbol": asset_symbol,
            "quote_currency": quote_currency,
            "frequency": frequency,
            "interval_seconds": interval_seconds,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "open_dollars": open_dollars,
            "high_dollars": high_dollars,
            "low_dollars": low_dollars,
            "close_dollars": close_dollars,
            "volume": volume,
            "observed_at": observed_at or now,
            "source_kind": source_kind,
            "source_id": source_id,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoSpotOHLCRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoSpotOHLCRecord).values(**insert_values)
        else:
            record = CryptoSpotOHLCRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoSpotOHLCRecord.kalshi_env,
                    CryptoSpotOHLCRecord.provider,
                    CryptoSpotOHLCRecord.asset_symbol,
                    CryptoSpotOHLCRecord.quote_currency,
                    CryptoSpotOHLCRecord.interval_seconds,
                    CryptoSpotOHLCRecord.end_ts,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoSpotOHLCRecord).where(
                    CryptoSpotOHLCRecord.kalshi_env == env,
                    CryptoSpotOHLCRecord.provider == provider,
                    CryptoSpotOHLCRecord.asset_symbol == asset_symbol,
                    CryptoSpotOHLCRecord.quote_currency == quote_currency,
                    CryptoSpotOHLCRecord.interval_seconds == interval_seconds,
                    CryptoSpotOHLCRecord.end_ts == end_ts,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_spot_ohlc(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        provider: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoSpotOHLCRecord]:
        stmt = select(CryptoSpotOHLCRecord).where(
            CryptoSpotOHLCRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.frequency == frequency)
        if provider is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.provider == provider)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoSpotOHLCRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.end_ts >= since)
        if until is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.end_ts <= until)
        if defer_payload:
            stmt = stmt.options(defer(CryptoSpotOHLCRecord.payload))
        stmt = stmt.order_by(CryptoSpotOHLCRecord.end_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def record_crypto_model_artifact(
        self,
        *,
        frequency: str,
        artifact_type: str,
        version: str,
        status: str,
        sample_count: int,
        metrics: dict[str, Any],
        payload: dict[str, Any],
        kalshi_env: str | None = None,
        trained_at: datetime | None = None,
    ) -> CryptoModelArtifactRecord:
        record = CryptoModelArtifactRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            frequency=frequency,
            artifact_type=artifact_type,
            version=version,
            status=status,
            trained_at=trained_at,
            sample_count=sample_count,
            metrics=metrics,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_latest_crypto_model_artifact(
        self,
        *,
        frequency: str = "15m",
        artifact_type: str,
        kalshi_env: str | None = None,
    ) -> CryptoModelArtifactRecord | None:
        stmt = (
            select(CryptoModelArtifactRecord)
            .where(
                CryptoModelArtifactRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoModelArtifactRecord.frequency == frequency,
                CryptoModelArtifactRecord.artifact_type == artifact_type,
            )
            .order_by(CryptoModelArtifactRecord.created_at.desc(), CryptoModelArtifactRecord.id.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_momentum_shadow_metrics(
        self,
        *,
        kalshi_env: str,
        window_hours: int = 24,
        veto_threshold_cents_per_min: float | None = None,
    ) -> dict[str, Any]:
        """Return shadow-mode outcome counts and slope/weight averages for the rolling window.

        Aggregates momentum_post_processor_outcome from signal payloads.
        avg_slope_cents_per_min includes all rows that have a slope stamped (any outcome).
        avg_weight is restricted to 'success' rows.
        veto_fraction is the share of 'success' rows where |slope| exceeds the threshold.
        """
        cutoff = datetime.now(UTC) - timedelta(hours=window_hours)
        stmt = (
            select(Signal.payload)
            .join(Room, Signal.room_id == Room.id)
            .where(
                Room.kalshi_env == kalshi_env,
                Signal.created_at >= cutoff,
            )
        )
        payloads = list((await self.session.execute(stmt)).scalars())

        by_outcome: dict[str, int] = {
            "success": 0,
            "calibration_missing": 0,
            "insufficient_points": 0,
            "price_history_error": 0,
            "unknown": 0,
        }
        slopes: list[float] = []
        weights: list[float] = []
        success_with_slope = 0
        veto_count = 0

        for payload in payloads:
            if not isinstance(payload, dict):
                by_outcome["unknown"] += 1
                continue
            outcome = str(payload.get("momentum_post_processor_outcome") or "unknown")
            if outcome not in by_outcome:
                outcome = "unknown"
            by_outcome[outcome] += 1

            raw_slope = payload.get("momentum_slope_cents_per_min")
            raw_weight = payload.get("momentum_weight")

            if raw_slope is not None:
                try:
                    slopes.append(float(raw_slope))
                except (TypeError, ValueError):
                    pass

            if outcome == "success" and raw_weight is not None:
                try:
                    weights.append(float(raw_weight))
                except (TypeError, ValueError):
                    pass

            if outcome == "success" and raw_slope is not None and veto_threshold_cents_per_min is not None:
                try:
                    success_with_slope += 1
                    if abs(float(raw_slope)) > veto_threshold_cents_per_min:
                        veto_count += 1
                except (TypeError, ValueError):
                    pass

        return {
            "window_hours": window_hours,
            "total": len(payloads),
            "by_outcome": by_outcome,
            "avg_slope_cents_per_min": (sum(slopes) / len(slopes)) if slopes else None,
            "avg_weight": (sum(weights) / len(weights)) if weights else None,
            "veto_fraction": (veto_count / success_with_slope) if success_with_slope > 0 else None,
        }

    async def purge_market_price_history(
        self,
        *,
        older_than: timedelta,
        kalshi_env: str | None = None,
    ) -> int:
        from sqlalchemy import delete as sa_delete
        cutoff = datetime.now(UTC) - older_than
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = sa_delete(MarketPriceHistory).where(
            MarketPriceHistory.kalshi_env == env,
            MarketPriceHistory.observed_at < cutoff,
        )
        result = await self.session.execute(stmt)
        return result.rowcount or 0

    async def vacuum_memory_notes(self, *, older_than_days: int) -> int:
        """Delete memory notes (and their cascade-linked embeddings) older than the retention window."""
        from sqlalchemy import delete as sa_delete
        cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
        stmt = sa_delete(MemoryNoteRecord).where(MemoryNoteRecord.created_at < cutoff)
        result = await self.session.execute(stmt)
        return result.rowcount or 0

    async def get_latest_trade_ticket_for_room(self, room_id: str) -> TradeTicketRecord | None:
        stmt = (
            select(TradeTicketRecord)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(TradeTicketRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_trade_ticket(
        self,
        room_id: str,
        ticket: TradeTicket,
        client_order_id: str,
        message_id: str | None = None,
        *,
        strategy_code: str | None = None,
    ) -> TradeTicketRecord:
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
            strategy_code=strategy_code,
            payload=ticket.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_trade_ticket_status(self, ticket_id: str, status: str) -> TradeTicketRecord | None:
        record = await self.session.get(TradeTicketRecord, ticket_id)
        if record is None:
            return None
        record.status = status
        await self.session.flush()
        return record

    async def save_decision_trace(
        self,
        *,
        room_id: str | None,
        ticket_id: str | None,
        market_ticker: str,
        kalshi_env: str | None,
        decision_kind: str,
        path_version: str,
        source_snapshot_ids: dict[str, Any],
        input_hash: str,
        trace_hash: str,
        trace: dict[str, Any],
        decision_time: datetime | None = None,
        agent_pack_version: str | None = None,
        parameter_pack_version: str | None = None,
    ) -> DecisionTraceRecord:
        record = DecisionTraceRecord(
            room_id=room_id,
            ticket_id=ticket_id,
            market_ticker=market_ticker,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            decision_kind=decision_kind,
            decision_time=decision_time or datetime.now(UTC),
            path_version=path_version,
            agent_pack_version=agent_pack_version,
            parameter_pack_version=parameter_pack_version,
            source_snapshot_ids=source_snapshot_ids,
            input_hash=input_hash,
            trace_hash=trace_hash,
            trace=trace,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_decision_trace(self, decision_trace_id: str) -> DecisionTraceRecord | None:
        stmt = select(DecisionTraceRecord).where(DecisionTraceRecord.id == decision_trace_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_decision_trace_for_room(self, room_id: str) -> DecisionTraceRecord | None:
        stmt = (
            select(DecisionTraceRecord)
            .where(DecisionTraceRecord.room_id == room_id)
            .order_by(DecisionTraceRecord.created_at.desc(), DecisionTraceRecord.decision_time.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_decision_trace_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> DecisionTraceRecord | None:
        stmt = (
            select(DecisionTraceRecord)
            .where(
                DecisionTraceRecord.market_ticker == market_ticker,
                DecisionTraceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            )
            .order_by(DecisionTraceRecord.decision_time.desc(), DecisionTraceRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_forecast_snapshot(
        self,
        *,
        market_ticker: str,
        kalshi_env: str | None,
        source_members: dict[str, Any],
        fused_pdf: dict[str, Any],
        probability_output: dict[str, Any],
        source_set_used: list[str],
        fetched_at: datetime | None = None,
        parameter_pack_version: str | None = None,
    ) -> ForecastSnapshotRecord:
        record = ForecastSnapshotRecord(
            market_ticker=market_ticker,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            fetched_at=fetched_at or datetime.now(UTC),
            parameter_pack_version=parameter_pack_version,
            source_members=source_members,
            fused_pdf=fused_pdf,
            probability_output=probability_output,
            source_set_used=source_set_used,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_climatology_prior(
        self,
        *,
        station_id: str,
        day_of_year: int,
        bucket_low_f: float | None,
        bucket_high_f: float | None,
        series_ticker: str | None = None,
    ) -> ClimatologyPriorRecord | None:
        stmt = select(ClimatologyPriorRecord).where(
            ClimatologyPriorRecord.station_id == station_id,
            ClimatologyPriorRecord.day_of_year == day_of_year,
        )
        if series_ticker is not None:
            stmt = stmt.where(ClimatologyPriorRecord.series_ticker == series_ticker)
        if bucket_low_f is None:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_low_f.is_(None))
        else:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_low_f == bucket_low_f)
        if bucket_high_f is None:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_high_f.is_(None))
        else:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_high_f == bucket_high_f)
        stmt = stmt.order_by(ClimatologyPriorRecord.updated_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_source_health_log(
        self,
        *,
        source: str,
        label: str,
        score: float,
        success_score: float,
        freshness_score: float,
        completeness_score: float,
        consistency_score: float,
        kalshi_env: str | None = None,
        market_ticker: str | None = None,
        station_id: str | None = None,
        observed_at: datetime | None = None,
        is_aggregate: bool = False,
        payload: dict[str, Any] | None = None,
    ) -> SourceHealthLogRecord:
        record = SourceHealthLogRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            source=source,
            is_aggregate=is_aggregate,
            market_ticker=market_ticker,
            station_id=station_id,
            observed_at=observed_at or datetime.now(UTC),
            label=label,
            score=score,
            success_score=success_score,
            freshness_score=freshness_score,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_recent_source_health_logs(
        self,
        *,
        kalshi_env: str | None = None,
        aggregate_only: bool | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> list[SourceHealthLogRecord]:
        stmt = select(SourceHealthLogRecord).where(
            SourceHealthLogRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if aggregate_only is not None:
            stmt = stmt.where(SourceHealthLogRecord.is_aggregate.is_(aggregate_only))
        if source is not None:
            stmt = stmt.where(SourceHealthLogRecord.source == source)
        stmt = stmt.order_by(SourceHealthLogRecord.observed_at.desc(), SourceHealthLogRecord.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

    async def save_weather_bootstrap_event(
        self,
        *,
        kalshi_env: str | None = None,
        market_ticker: str,
        event_type: str,
        status: str,
        series_ticker: str | None = None,
        local_market_day: str | None = None,
        bucket_key: str | None = None,
        policy_key: str | None = None,
        tier: str | None = None,
        side: str | None = None,
        confidence: float | None = None,
        edge_bps: int | None = None,
        size_factor: float | None = None,
        count_fp: Decimal | None = None,
        notional_dollars: Decimal | None = None,
        pnl_dollars: Decimal | None = None,
        evidence_weight: float = 1.0,
        source: str = "live",
        occurred_at: datetime | None = None,
        room_id: str | None = None,
        decision_trace_id: str | None = None,
        order_id: str | None = None,
        fill_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WeatherBootstrapEventRecord:
        record = WeatherBootstrapEventRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            bucket_key=bucket_key,
            policy_key=policy_key,
            tier=tier,
            event_type=event_type,
            status=status,
            side=side,
            confidence=confidence,
            edge_bps=edge_bps,
            size_factor=size_factor,
            count_fp=count_fp,
            notional_dollars=notional_dollars,
            pnl_dollars=pnl_dollars,
            evidence_weight=evidence_weight,
            source=source,
            occurred_at=occurred_at or datetime.now(UTC),
            room_id=room_id,
            decision_trace_id=decision_trace_id,
            order_id=order_id,
            fill_id=fill_id,
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_weather_bootstrap_events(
        self,
        *,
        kalshi_env: str | None = None,
        bucket_key: str | None = None,
        series_ticker: str | None = None,
        since: datetime | None = None,
        event_types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 1000,
    ) -> list[WeatherBootstrapEventRecord]:
        stmt = select(WeatherBootstrapEventRecord).where(
            WeatherBootstrapEventRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if bucket_key:
            stmt = stmt.where(WeatherBootstrapEventRecord.bucket_key == bucket_key)
        if series_ticker:
            stmt = stmt.where(WeatherBootstrapEventRecord.series_ticker == series_ticker)
        if since is not None:
            stmt = stmt.where(WeatherBootstrapEventRecord.occurred_at >= since)
        if event_types:
            stmt = stmt.where(WeatherBootstrapEventRecord.event_type.in_(event_types))
        if statuses:
            stmt = stmt.where(WeatherBootstrapEventRecord.status.in_(statuses))
        stmt = stmt.order_by(WeatherBootstrapEventRecord.occurred_at.desc(), WeatherBootstrapEventRecord.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

    async def save_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        market_ticker: str,
        replay_version: str,
        source_fingerprint: str,
        series_ticker: str | None = None,
        local_market_day: str | None = None,
        bucket_key: str | None = None,
        policy_key: str | None = None,
        tier: str | None = None,
        strict_replay: bool = True,
        side: str | None = None,
        confidence: float | None = None,
        edge_bps: int | None = None,
        count_fp: Decimal | None = None,
        notional_dollars: Decimal | None = None,
        pnl_dollars: Decimal | None = None,
        evidence_weight: float = 1.0,
        outcome: str | None = None,
        observed_at: datetime | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WeatherBootstrapHistoricalEvidenceRecord:
        record = WeatherBootstrapHistoricalEvidenceRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            bucket_key=bucket_key,
            policy_key=policy_key,
            tier=tier,
            replay_version=replay_version,
            source_fingerprint=source_fingerprint,
            strict_replay=strict_replay,
            side=side,
            confidence=confidence,
            edge_bps=edge_bps,
            count_fp=count_fp,
            notional_dollars=notional_dollars,
            pnl_dollars=pnl_dollars,
            evidence_weight=evidence_weight,
            outcome=outcome,
            observed_at=observed_at or datetime.now(UTC),
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        source_fingerprint: str,
        market_ticker: str,
        bucket_key: str | None = None,
        policy_key: str | None = None,
    ) -> WeatherBootstrapHistoricalEvidenceRecord | None:
        stmt = select(WeatherBootstrapHistoricalEvidenceRecord).where(
            WeatherBootstrapHistoricalEvidenceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            WeatherBootstrapHistoricalEvidenceRecord.source_fingerprint == source_fingerprint,
            WeatherBootstrapHistoricalEvidenceRecord.market_ticker == market_ticker,
            WeatherBootstrapHistoricalEvidenceRecord.policy_key == policy_key,
        )
        if bucket_key is None:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key.is_(None))
        else:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key == bucket_key)
        return (await self.session.execute(stmt.limit(1))).scalar_one_or_none()

    async def list_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        bucket_key: str | None = None,
        series_ticker: str | None = None,
        strict_replay: bool | None = True,
        limit: int = 1000,
    ) -> list[WeatherBootstrapHistoricalEvidenceRecord]:
        stmt = select(WeatherBootstrapHistoricalEvidenceRecord).where(
            WeatherBootstrapHistoricalEvidenceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if bucket_key:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key == bucket_key)
        if series_ticker:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.series_ticker == series_ticker)
        if strict_replay is not None:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.strict_replay.is_(strict_replay))
        stmt = stmt.order_by(
            WeatherBootstrapHistoricalEvidenceRecord.observed_at.desc(),
            WeatherBootstrapHistoricalEvidenceRecord.created_at.desc(),
        ).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

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
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> OrderRecord:
        return await self.upsert_order(
            ticket_id=ticket_id,
            client_order_id=client_order_id,
            market_ticker=market_ticker,
            status=status,
            side=side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            raw=raw,
            kalshi_order_id=kalshi_order_id,
            kalshi_env=kalshi_env,
            strategy_code=strategy_code,
        )

    async def _resolve_strategy_code_for_order(
        self,
        *,
        strategy_code: str | None,
        ticket_id: str | None,
        client_order_id: str | None,
    ) -> str | None:
        """Strategy code flows from the ticket if caller didn't specify one."""
        if strategy_code is not None:
            return strategy_code
        if ticket_id is not None:
            stmt = select(TradeTicketRecord.strategy_code).where(TradeTicketRecord.id == ticket_id)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        if client_order_id is not None:
            stmt = select(TradeTicketRecord.strategy_code).where(
                TradeTicketRecord.client_order_id.in_(_candidate_trade_ticket_client_order_ids(client_order_id))
            )
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        return None

    async def _resolve_ticket_id_for_order(
        self,
        *,
        ticket_id: str | None,
        client_order_id: str | None,
    ) -> str | None:
        if ticket_id is not None:
            return ticket_id
        if not client_order_id:
            return None
        stmt = select(TradeTicketRecord.id).where(
            TradeTicketRecord.client_order_id.in_(_candidate_trade_ticket_client_order_ids(client_order_id))
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    @staticmethod
    def _ticket_status_rank(status: str | None) -> int:
        normalized = str(status or "").strip().lower()
        if normalized in {"filled", "executed"}:
            return 50
        if (
            normalized
            in {
                "blocked",
                "canceled",
                "cancelled",
                "expired",
                "lock_denied",
                "order_id_missing",
                "rejected",
                "requote_edge_lost",
                "unfilled_cancelled",
                "write_credentials_missing",
            }
            or normalized.startswith("rejected_")
        ):
            return 40
        if normalized in {"submitted", "resting", "accepted", "open", "pending"}:
            return 20
        if normalized == "approved":
            return 10
        if normalized == "proposed":
            return 0
        return 20

    async def _sync_trade_ticket_status_from_order(self, ticket_id: str | None, status: str) -> None:
        if ticket_id is None:
            return
        ticket = await self.session.get(TradeTicketRecord, ticket_id)
        if ticket is None:
            return
        if self._ticket_status_rank(status) >= self._ticket_status_rank(ticket.status):
            ticket.status = status
            await self.session.flush()

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
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> OrderRecord:
        from kalshi_bot.db.models import OrderRecord as _OR
        resolved_ticket_id = await self._resolve_ticket_id_for_order(
            ticket_id=ticket_id,
            client_order_id=client_order_id,
        )
        resolved_strategy = await self._resolve_strategy_code_for_order(
            strategy_code=strategy_code,
            ticket_id=resolved_ticket_id,
            client_order_id=client_order_id,
        )
        record_id = str(uuid4())
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": record_id,
            "trade_ticket_id": resolved_ticket_id,
            "client_order_id": client_order_id,
            "kalshi_env": env,
            "market_ticker": market_ticker,
            "status": status,
            "side": side,
            "action": action,
            "yes_price_dollars": yes_price_dollars,
            "count_fp": count_fp,
            "strategy_code": resolved_strategy,
            "raw": raw,
            "kalshi_order_id": kalshi_order_id,
            "created_at": now,
            "updated_at": now,
        }
        update_values = {
            "status": status,
            "market_ticker": market_ticker,
            "side": side,
            "action": action,
            "yes_price_dollars": yes_price_dollars,
            "count_fp": count_fp,
            "raw": raw,
            "updated_at": now,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(_OR).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(_OR).values(**insert_values)
        else:
            # fallback: SELECT then mutate
            existing = (
                await self.session.execute(
                    select(_OR).where(
                        _OR.kalshi_env == env,
                        _OR.client_order_id == client_order_id,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                existing = _OR(**insert_values)
                self.session.add(existing)
            else:
                for k, v in update_values.items():
                    setattr(existing, k, v)
                if resolved_ticket_id and not existing.trade_ticket_id:
                    existing.trade_ticket_id = resolved_ticket_id
                if kalshi_order_id and not existing.kalshi_order_id:
                    existing.kalshi_order_id = kalshi_order_id
                if resolved_strategy and not existing.strategy_code:
                    existing.strategy_code = resolved_strategy
            await self.session.flush()
            await self._sync_trade_ticket_status_from_order(existing.trade_ticket_id, existing.status)
            return existing

        # COALESCE lets later, richer execution records repair placeholder rows
        # inserted first by websocket/reconcile without overwriting with NULL.
        coalesce_ticket_id = func.coalesce(stmt.excluded.trade_ticket_id, _OR.trade_ticket_id)
        coalesce_kalshi_id = func.coalesce(stmt.excluded.kalshi_order_id, _OR.kalshi_order_id)
        coalesce_strategy = func.coalesce(stmt.excluded.strategy_code, _OR.strategy_code)
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=["kalshi_env", "client_order_id"],
                set_={
                    **update_values,
                    "trade_ticket_id": coalesce_ticket_id,
                    "kalshi_order_id": coalesce_kalshi_id,
                    "strategy_code": coalesce_strategy,
                },
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(_OR).where(
                    _OR.kalshi_env == env,
                    _OR.client_order_id == client_order_id,
                )
            )
        ).scalar_one()
        await self._sync_trade_ticket_status_from_order(result.trade_ticket_id, result.status)
        return result

    async def list_orders_for_room(self, room_id: str) -> list[OrderRecord]:
        stmt = (
            select(OrderRecord)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(OrderRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_orders_for_markets(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[OrderRecord]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(OrderRecord)
            .where(
                OrderRecord.kalshi_env == env,
                OrderRecord.market_ticker.in_(market_tickers),
            )
            .order_by(OrderRecord.created_at.desc())
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
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> FillRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        raw_order_id = raw.get("order_id") if isinstance(raw, dict) else None
        resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
            strategy_code=strategy_code,
            order_id=order_id,
            kalshi_order_id=raw_order_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=side,
            action=action,
        )
        economic_side = resolved_side or side
        record = FillRecord(
            order_id=resolved_order_id,
            trade_id=trade_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=economic_side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            strategy_code=resolved_strategy,
            raw=raw,
            is_taker=is_taker,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def _resolve_order_for_fill(
        self,
        *,
        order_id: str | None,
        kalshi_order_id: str | None,
        kalshi_env: str | None,
    ) -> OrderRecord | None:
        if order_id is not None:
            stmt = select(OrderRecord).where(OrderRecord.id == order_id)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        if kalshi_order_id is not None and kalshi_env is not None:
            attribution_rank = case(
                (
                    OrderRecord.strategy_code.is_not(None)
                    & OrderRecord.trade_ticket_id.is_not(None),
                    0,
                ),
                (OrderRecord.strategy_code.is_not(None), 1),
                (OrderRecord.trade_ticket_id.is_not(None), 2),
                else_=3,
            )
            stmt = select(OrderRecord).where(
                OrderRecord.kalshi_env == kalshi_env,
                OrderRecord.kalshi_order_id == kalshi_order_id,
            ).order_by(attribution_rank, OrderRecord.updated_at.desc(), OrderRecord.created_at.desc()).limit(1)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        return None

    async def _latest_attributed_buy_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        kalshi_env: str,
        before: datetime | None = None,
    ) -> FillRecord | None:
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == kalshi_env,
            FillRecord.market_ticker == market_ticker,
            FillRecord.side == side,
            FillRecord.action == "buy",
            FillRecord.strategy_code.is_not(None),
        )
        if before is not None:
            stmt = stmt.where(FillRecord.created_at <= before)
        stmt = stmt.order_by(FillRecord.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_fill_strategy_for_market_side(
        self,
        *,
        market_ticker: str,
        side: str,
        kalshi_env: str | None = None,
        before: datetime | None = None,
    ) -> str | None:
        found = await self._latest_attributed_buy_fill(
            market_ticker=market_ticker,
            side=side,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            before=before,
        )
        return found.strategy_code if found is not None else None

    async def _resolve_fill_links(
        self,
        *,
        strategy_code: str | None,
        order_id: str | None,
        kalshi_order_id: str | None,
        kalshi_env: str,
        market_ticker: str,
        side: str,
        action: str,
        before: datetime | None = None,
    ) -> tuple[str | None, str | None, str | None]:
        """Return (order_id, strategy_code, economic_side) for a fill using bounded evidence."""
        if strategy_code is not None:
            matched = await self._resolve_order_for_fill(
                order_id=order_id,
                kalshi_order_id=kalshi_order_id,
                kalshi_env=kalshi_env,
            )
            return (
                order_id or (matched.id if matched is not None else None),
                strategy_code,
                matched.side if matched is not None else None,
            )

        matched_order = await self._resolve_order_for_fill(
            order_id=order_id,
            kalshi_order_id=kalshi_order_id,
            kalshi_env=kalshi_env,
        )
        if matched_order is not None:
            matched_strategy = matched_order.strategy_code
            if matched_strategy is None:
                matched_strategy = await self._resolve_strategy_code_for_order(
                    strategy_code=None,
                    ticket_id=matched_order.trade_ticket_id,
                    client_order_id=matched_order.client_order_id,
                )
                if matched_strategy is not None:
                    matched_order.strategy_code = matched_strategy
            return matched_order.id, matched_strategy, matched_order.side

        if action == "sell":
            latest_buy = await self._latest_attributed_buy_fill(
                market_ticker=market_ticker,
                side=side,
                kalshi_env=kalshi_env,
                before=before,
            )
            if latest_buy is not None:
                return order_id, latest_buy.strategy_code, latest_buy.side

        return order_id, None, None

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
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> FillRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        raw_order_id = raw.get("order_id") if isinstance(raw, dict) else None
        resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
            strategy_code=strategy_code,
            order_id=order_id,
            kalshi_order_id=raw_order_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=side,
            action=action,
        )
        economic_side = resolved_side or side
        if trade_id is not None:
            observed_at = datetime.now(UTC)
            insert_values = {
                "id": str(uuid4()),
                "order_id": resolved_order_id,
                "trade_id": trade_id,
                "kalshi_env": env,
                "market_ticker": market_ticker,
                "side": economic_side,
                "action": action,
                "yes_price_dollars": yes_price_dollars,
                "count_fp": count_fp,
                "strategy_code": resolved_strategy,
                "raw": raw,
                "is_taker": is_taker,
                "created_at": observed_at,
                "updated_at": observed_at,
            }
            dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
            if dialect_name == "postgresql":
                stmt = pg_insert(FillRecord).values(**insert_values)
            elif dialect_name == "sqlite":
                stmt = sqlite_insert(FillRecord).values(**insert_values)
            else:
                stmt = None
            if stmt is not None:
                excluded = stmt.excluded
                await self.session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[FillRecord.kalshi_env, FillRecord.trade_id],
                        set_={
                            "order_id": func.coalesce(excluded.order_id, FillRecord.order_id),
                            "market_ticker": excluded.market_ticker,
                            "side": excluded.side,
                            "action": excluded.action,
                            "yes_price_dollars": excluded.yes_price_dollars,
                            "count_fp": excluded.count_fp,
                            "strategy_code": func.coalesce(excluded.strategy_code, FillRecord.strategy_code),
                            "raw": excluded.raw,
                            "is_taker": excluded.is_taker,
                            "updated_at": observed_at,
                        },
                    )
                )
                await self.session.flush()
                stmt = select(FillRecord).where(
                    FillRecord.kalshi_env == env,
                    FillRecord.trade_id == trade_id,
                )
                return (await self.session.execute(stmt)).scalar_one()

        record: FillRecord | None = None
        if trade_id is not None:
            stmt = select(FillRecord).where(
                FillRecord.kalshi_env == env,
                FillRecord.trade_id == trade_id,
            )
            record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = FillRecord(
                order_id=resolved_order_id,
                trade_id=trade_id,
                kalshi_env=env,
                market_ticker=market_ticker,
                side=economic_side,
                action=action,
                yes_price_dollars=yes_price_dollars,
                count_fp=count_fp,
                strategy_code=resolved_strategy,
                raw=raw,
                is_taker=is_taker,
            )
            self.session.add(record)
        else:
            record.order_id = resolved_order_id or record.order_id
            record.market_ticker = market_ticker
            record.side = economic_side
            record.action = action
            record.yes_price_dollars = yes_price_dollars
            record.count_fp = count_fp
            if resolved_strategy and not record.strategy_code:
                record.strategy_code = resolved_strategy
            record.raw = raw
            record.is_taker = is_taker
        await self.session.flush()
        return record

    async def repair_fill_attribution(
        self,
        *,
        kalshi_env: str | None = None,
        days: int = 7,
        limit: int = 500,
        dry_run: bool = True,
        strategy_code: str | None = None,
        market_prefix: str | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(days)))
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.created_at >= cutoff,
                or_(FillRecord.strategy_code.is_(None), FillRecord.strategy_code == "", FillRecord.order_id.is_(None)),
            )
            .order_by(FillRecord.created_at.desc(), FillRecord.id.desc())
            .limit(max(1, int(limit)))
        )
        if market_prefix:
            stmt = stmt.where(FillRecord.market_ticker.like(f"{market_prefix}%"))
        fills = list((await self.session.execute(stmt)).scalars())
        repaired: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for fill in fills:
            raw = fill.raw if isinstance(fill.raw, dict) else {}
            raw_order_id = raw.get("order_id")
            resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
                strategy_code=None,
                order_id=None if raw_order_id else fill.order_id,
                kalshi_order_id=raw_order_id,
                kalshi_env=env,
                market_ticker=fill.market_ticker,
                side=fill.side,
                action=fill.action,
                before=fill.created_at,
            )
            if strategy_code and resolved_strategy != strategy_code:
                skipped.append(
                    {
                        "fill_id": fill.id,
                        "trade_id": fill.trade_id,
                        "reason": "resolved_strategy_mismatch",
                        "resolved_strategy_code": resolved_strategy,
                    }
                )
                continue
            if not resolved_order_id and not resolved_strategy:
                skipped.append(
                    {
                        "fill_id": fill.id,
                        "trade_id": fill.trade_id,
                        "reason": "no_attributed_order_match",
                        "kalshi_order_id": raw_order_id,
                    }
                )
                continue
            change = {
                "fill_id": fill.id,
                "trade_id": fill.trade_id,
                "market_ticker": fill.market_ticker,
                "old_order_id": fill.order_id,
                "new_order_id": resolved_order_id or fill.order_id,
                "old_strategy_code": fill.strategy_code,
                "new_strategy_code": resolved_strategy or fill.strategy_code,
                "old_side": fill.side,
                "new_side": resolved_side or fill.side,
            }
            if (
                change["old_order_id"] == change["new_order_id"]
                and change["old_strategy_code"] == change["new_strategy_code"]
                and change["old_side"] == change["new_side"]
            ):
                skipped.append({**change, "reason": "no_change"})
                continue
            repaired.append(change)
            if not dry_run:
                fill.order_id = resolved_order_id or fill.order_id
                fill.strategy_code = resolved_strategy or fill.strategy_code
                fill.side = resolved_side or fill.side
        if not dry_run:
            await self.session.flush()
        return {
            "kalshi_env": env,
            "dry_run": dry_run,
            "days": days,
            "scanned": len(fills),
            "repairable": len(repaired),
            "skipped": len(skipped),
            "changes": repaired,
            "skipped_samples": skipped[:20],
        }

    async def list_fills_for_room(self, room_id: str) -> list[FillRecord]:
        stmt = (
            select(FillRecord)
            .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(FillRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_fills_for_markets(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[FillRecord]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.market_ticker.in_(market_tickers),
            )
            .order_by(FillRecord.created_at.desc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def settle_fills(self, settlements: list[dict[str, Any]], *, kalshi_env: str | None = None) -> int:
        """Mark fills as win/loss based on settlement results. Returns number of fills updated."""
        settled = 0
        env = self._resolved_kalshi_env(kalshi_env)
        for s in settlements:
            ticker = s.get("ticker") or s.get("market_ticker")
            result = s.get("market_result")
            if not ticker or result not in ("yes", "no"):
                continue
            stmt = select(FillRecord).where(
                FillRecord.kalshi_env == env,
                FillRecord.market_ticker == ticker,
                FillRecord.settlement_result.is_(None),
            )
            fills = list((await self.session.execute(stmt)).scalars())
            for fill in fills:
                fill.settlement_result = "win" if fill.side == result else "loss"
                settled += 1
        if settled:
            await self.session.flush()
        return settled

    async def sync_weather_bootstrap_settlement_events(self, *, kalshi_env: str | None = None) -> int:
        """Mirror settled bootstrap-attributed fills into bootstrap evidence events."""
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord, WeatherBootstrapEventRecord)
            .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
            .join(WeatherBootstrapEventRecord, WeatherBootstrapEventRecord.order_id == OrderRecord.id)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.settlement_result.is_not(None),
                WeatherBootstrapEventRecord.kalshi_env == env,
                WeatherBootstrapEventRecord.event_type == "order",
            )
            .order_by(FillRecord.created_at.asc(), WeatherBootstrapEventRecord.created_at.asc())
        )
        rows = list((await self.session.execute(stmt)).all())
        created = 0
        seen_fill_ids: set[str] = set()
        for fill, event in rows:
            if fill.id in seen_fill_ids:
                continue
            seen_fill_ids.add(fill.id)
            existing_stmt = select(WeatherBootstrapEventRecord.id).where(
                WeatherBootstrapEventRecord.kalshi_env == env,
                WeatherBootstrapEventRecord.event_type == "settlement",
                WeatherBootstrapEventRecord.fill_id == fill.id,
            ).limit(1)
            existing = (await self.session.execute(existing_stmt)).scalar_one_or_none()
            if existing is not None:
                continue
            pnl = _settled_buy_fill_pnl(fill)
            if pnl is None:
                continue
            status = "settled_win" if fill.settlement_result == "win" else "settled_loss"
            await self.save_weather_bootstrap_event(
                kalshi_env=env,
                market_ticker=fill.market_ticker,
                series_ticker=event.series_ticker,
                local_market_day=event.local_market_day,
                bucket_key=event.bucket_key,
                policy_key=event.policy_key,
                tier=event.tier,
                event_type="settlement",
                status=status,
                side=fill.side,
                confidence=event.confidence,
                edge_bps=event.edge_bps,
                size_factor=event.size_factor,
                count_fp=fill.count_fp,
                notional_dollars=event.notional_dollars,
                pnl_dollars=pnl,
                evidence_weight=1.0,
                source="live_settlement",
                occurred_at=datetime.now(UTC),
                room_id=event.room_id,
                order_id=event.order_id,
                fill_id=fill.id,
                payload={
                    **dict(event.payload or {}),
                    "settlement_result": fill.settlement_result,
                    "fill_id": fill.id,
                    "trade_id": fill.trade_id,
                    "pnl_dollars": str(pnl) if pnl is not None else None,
                },
            )
            created += 1
        if created:
            await self.session.flush()
        return created

    async def get_fill_win_rate_30d(
        self,
        *,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> dict[str, Any]:
        """Return 30-day rolling realized P&L metrics.

        A position is a win if:
        - It was sold (stop-loss or manual exit) at a better price than entry, OR
        - It was held to settlement and the market resolved on our side.
        Settlement-based result is used only when no sell fill exists for the ticker+side.

        When ``strategy_code`` is provided, only fills attributed to that strategy
        are counted. Fills with a NULL ``strategy_code`` are excluded from filtered
        queries (treat as unknown-attribution).

        Returned keys:
        - ``won_contracts``, ``total_contracts``: legacy count-weighted win/loss totals.
        - ``trade_count``, ``win_count``, ``loss_count``: per-trade counts (each
          buy fill = one trade observation, regardless of contract count).
        - ``avg_win_dollars``, ``avg_loss_dollars``: mean P&L of winning / losing
          trades, each weighted by contract count inside the trade. None when
          there are no trades of that kind.
        - ``stdev_dollars``: population stdev of per-trade P&L (unweighted).
          None when fewer than two trades.
        - ``sharpe_per_trade``: mean(p&l) / stdev(p&l) over the sample (P2-1
          rolling Sharpe proxy). None when stdev is zero or fewer than two trades.
        """
        cutoff = datetime.now(UTC) - timedelta(days=30)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == env,
            FillRecord.created_at >= cutoff,
        )
        if strategy_code is not None:
            stmt = stmt.where(FillRecord.strategy_code == strategy_code)
        all_fills = list((await self.session.execute(stmt)).scalars())

        return self._fill_pnl_metrics(all_fills)

    async def get_session_fill_pnl_summary(
        self,
        *,
        kalshi_env: str | None = None,
        pacific_date: str | None = None,
    ) -> dict[str, Any]:
        """Return scored fill P&L metrics for one Pacific trading day."""
        import zoneinfo
        from datetime import date as date_cls
        from datetime import time

        pacific_zone = zoneinfo.ZoneInfo("America/Los_Angeles")
        local_date = (
            date_cls.fromisoformat(pacific_date)
            if pacific_date is not None
            else datetime.now(pacific_zone).date()
        )
        start_local = datetime.combine(local_date, time.min, tzinfo=pacific_zone)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(UTC)
        end_utc = end_local.astimezone(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.created_at >= start_utc,
                FillRecord.created_at < end_utc,
            )
            .order_by(FillRecord.created_at.asc())
        )
        all_fills = list((await self.session.execute(stmt)).scalars())
        metrics = self._fill_pnl_metrics(all_fills)
        metrics.update(
            {
                "date": local_date.isoformat(),
                "window_start": start_utc.isoformat(),
                "window_end": end_utc.isoformat(),
                "fill_count": len(all_fills),
                "buy_fill_count": sum(1 for fill in all_fills if fill.action == "buy"),
                "sell_fill_count": sum(1 for fill in all_fills if fill.action == "sell"),
            }
        )
        return metrics

    async def get_strategy_city_fill_metrics_since(
        self,
        *,
        series_ticker: str,
        strategy_name: str,
        since: datetime,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        effective_strategy_code = strategy_code if strategy_code is not None else strategy_name
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == env,
            FillRecord.created_at >= since,
            FillRecord.market_ticker.like(f"{series_ticker}%"),
            FillRecord.strategy_code == effective_strategy_code,
        )
        all_fills = list((await self.session.execute(stmt)).scalars())
        buys: dict[tuple[str, str], list[FillRecord]] = {}
        sells: dict[tuple[str, str], list[FillRecord]] = {}
        for fill in all_fills:
            key = (fill.market_ticker, fill.side)
            if fill.action == "buy":
                buys.setdefault(key, []).append(fill)
            elif fill.action == "sell":
                sells.setdefault(key, []).append(fill)

        trade_pnls: list[float] = []
        for key, buy_fills in buys.items():
            _ticker, side = key
            sell_fills = sells.get(key, [])
            avg_sell: float | None = None
            if sell_fills:
                sell_count = sum(float(s.count_fp) for s in sell_fills)
                if sell_count > 0:
                    avg_sell = sum(float(s.yes_price_dollars) * float(s.count_fp) for s in sell_fills) / sell_count
            for buy_fill in buy_fills:
                count = float(buy_fill.count_fp)
                buy_px = float(buy_fill.yes_price_dollars)
                pnl: float | None = None
                if avg_sell is not None:
                    pnl = (avg_sell - buy_px) * count if side == "yes" else (buy_px - avg_sell) * count
                elif buy_fill.settlement_result is not None:
                    won_leg = buy_fill.settlement_result == "win"
                    pnl = ((1.0 if won_leg else 0.0) - buy_px) * count if side == "yes" else ((1.0 if won_leg else 0.0) - (1.0 - buy_px)) * count
                if pnl is not None:
                    trade_pnls.append(pnl)

        fill_count = len(trade_pnls)
        win_count = len([pnl for pnl in trade_pnls if pnl > 0])
        total_pnl = sum(trade_pnls)
        return {
            "series_ticker": series_ticker,
            "strategy_name": strategy_name,
            "resolved_live_fills": fill_count,
            "win_count": win_count,
            "win_rate": (win_count / fill_count) if fill_count else None,
            "realized_pnl": total_pnl,
        }

    async def get_daily_realized_pnl_dollars_by_strategy(
        self,
        *,
        strategy_code: str,
        kalshi_env: str | None = None,
        now: datetime | None = None,
    ) -> Decimal:
        """Conservative realized daily P&L for one strategy in the last 24 hours.

        Powers the per-strategy hard-loss cap. Intentionally narrow: counts only
        BUY fills whose settlement is already known, matched-pair BUY→SELL
        (stop-loss exits) on the same rolling window, and standalone SELL fills
        (treated as pure proceeds — the offsetting BUY is assumed to be older
        than the 24-hour window and therefore already accounted for).

        Open unsettled BUYs contribute zero so the cap cannot be tripped by
        unrealized marks. A negative return means losses; compare magnitude
        against the configured cap.
        """
        cutoff = (now or datetime.now(UTC)) - timedelta(hours=24)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == env,
            FillRecord.strategy_code == strategy_code,
            FillRecord.created_at >= cutoff,
        )
        fills = list((await self.session.execute(stmt)).scalars())
        pnl = Decimal("0")

        # Index sells per (ticker, side) so matched-pair exits can net against
        # a buy from the same window.
        sells_by_key: dict[tuple[str, str], list[FillRecord]] = {}
        matched_sell_ids: set[str] = set()
        for fill in fills:
            if fill.action == "sell":
                sells_by_key.setdefault((fill.market_ticker, fill.side), []).append(fill)

        for buy in fills:
            if buy.action != "buy":
                continue
            cost_per_contract = (
                buy.yes_price_dollars
                if buy.side == "yes"
                else Decimal("1") - buy.yes_price_dollars
            )
            cost_total = cost_per_contract * buy.count_fp
            buy_fee = _fill_fee_dollars(buy)

            key = (buy.market_ticker, buy.side)
            matched_sells = [s for s in sells_by_key.get(key, []) if s.id not in matched_sell_ids]
            if matched_sells:
                # Match the earliest unused sell. Partial matching is rare in practice
                # and would only skew the figure by a fraction of a cent.
                sell = matched_sells[0]
                matched_sell_ids.add(sell.id)
                sell_per_contract = (
                    sell.yes_price_dollars
                    if sell.side == "yes"
                    else Decimal("1") - sell.yes_price_dollars
                )
                pnl += sell_per_contract * sell.count_fp - _fill_fee_dollars(sell) - cost_total - buy_fee
            elif buy.settlement_result == "win":
                pnl += buy.count_fp - cost_total - buy_fee
            elif buy.settlement_result == "loss":
                pnl -= cost_total + buy_fee
            # else: unsettled + unmatched buy → unrealized, contributes nothing

        # Standalone sells whose corresponding buy fell out of the 24h window
        # still produced proceeds today. Treat those as pure positive cashflow.
        for sells in sells_by_key.values():
            for sell in sells:
                if sell.id in matched_sell_ids:
                    continue
                sell_per_contract = (
                    sell.yes_price_dollars
                    if sell.side == "yes"
                    else Decimal("1") - sell.yes_price_dollars
                )
                pnl += sell_per_contract * sell.count_fp - _fill_fee_dollars(sell)

        return pnl.quantize(Decimal("0.01"))

    async def get_broken_book_rate_30d(self, *, kalshi_env: str | None = None) -> dict[str, Any]:
        cutoff = datetime.now(UTC) - timedelta(days=30)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(
                func.count().filter(
                    RoomStrategyAuditRecord.stand_down_reason == "book_effectively_broken"
                ).label("broken_count"),
                func.count().label("total_count"),
            )
            .join(Room, Room.id == RoomStrategyAuditRecord.room_id)
            .where(
                Room.kalshi_env == env,
                RoomStrategyAuditRecord.created_at >= cutoff,
            )
        )
        row = (await self.session.execute(stmt)).one()
        return {"broken_count": int(row.broken_count), "total_count": int(row.total_count)}

    async def get_position(
        self,
        market_ticker: str,
        subaccount: int = 0,
        *,
        kalshi_env: str | None = None,
        include_closed: bool = False,
    ) -> PositionRecord | None:
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == env,
            PositionRecord.market_ticker == market_ticker,
            PositionRecord.subaccount == subaccount,
        )
        if not include_closed:
            stmt = stmt.where(PositionRecord.count_fp > 0)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_positions_for_ticker(
        self,
        market_ticker: str,
        subaccount: int = 0,
        *,
        kalshi_env: str | None = None,
        include_closed: bool = False,
    ) -> list[PositionRecord]:
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == env,
            PositionRecord.market_ticker == market_ticker,
            PositionRecord.subaccount == subaccount,
        )
        if not include_closed:
            stmt = stmt.where(PositionRecord.count_fp > 0)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_pending_buy_count_fp(
        self,
        market_ticker: str,
        side: str,
        *,
        kalshi_env: str | None = None,
    ) -> Decimal:
        """Sum count_fp of resting/submitted buy orders for this ticker+side (in-flight exposure)."""
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(func.coalesce(func.sum(OrderRecord.count_fp), 0)).where(
            OrderRecord.kalshi_env == env,
            OrderRecord.market_ticker == market_ticker,
            OrderRecord.side == side,
            OrderRecord.action == "buy",
            OrderRecord.status.in_(["resting", "submitted"]),
        )
        result = await self.session.execute(stmt)
        return Decimal(str(result.scalar() or 0))

    async def get_pending_buy_notional_dollars(
        self,
        *,
        kalshi_env: str | None = None,
        strategy_codes: list[str] | None = None,
        market_ticker_prefixes: list[str] | None = None,
    ) -> Decimal:
        """Sum notional exposure of resting/submitted buy orders for portfolio caps."""
        env = self._resolved_kalshi_env(kalshi_env)
        unit_notional = case(
            (OrderRecord.side == "yes", OrderRecord.yes_price_dollars),
            else_=Decimal("1.0000") - OrderRecord.yes_price_dollars,
        )
        stmt = select(func.coalesce(func.sum(OrderRecord.count_fp * unit_notional), 0)).where(
            OrderRecord.kalshi_env == env,
            OrderRecord.action == "buy",
            OrderRecord.status.in_(["resting", "submitted"]),
        )
        exposure_filters = []
        if strategy_codes:
            exposure_filters.append(OrderRecord.strategy_code.in_([str(code) for code in strategy_codes]))
        if market_ticker_prefixes:
            prefix_filters = [
                OrderRecord.market_ticker.ilike(f"{str(prefix).upper()}%")
                for prefix in market_ticker_prefixes
                if str(prefix).strip()
            ]
            if prefix_filters:
                exposure_filters.append(or_(*prefix_filters))
        if exposure_filters:
            stmt = stmt.where(or_(*exposure_filters))
        result = await self.session.execute(stmt)
        return _quantize_money(result.scalar() or 0)

    async def zero_settled_positions(
        self,
        *,
        kalshi_env: str,
        subaccount: int,
        live_tickers: set[str],
    ) -> int:
        """Zero out DB positions not present in the live Kalshi response (i.e. settled)."""
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == kalshi_env,
            PositionRecord.subaccount == subaccount,
            PositionRecord.count_fp != 0,
        )
        rows = list((await self.session.execute(stmt)).scalars())
        zeroed = 0
        for row in rows:
            if row.market_ticker not in live_tickers:
                row.count_fp = Decimal("0")
                zeroed += 1
        if zeroed:
            await self.session.flush()
        return zeroed

    async def upsert_position(
        self,
        *,
        market_ticker: str,
        subaccount: int,
        kalshi_env: str | None = None,
        side: str,
        count_fp: Decimal,
        average_price_dollars: Decimal,
        raw: dict[str, Any],
    ) -> PositionRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        now = datetime.now(UTC)
        insert_values = {
            "id": str(uuid4()),
            "market_ticker": market_ticker,
            "subaccount": subaccount,
            "kalshi_env": env,
            "side": side,
            "count_fp": count_fp,
            "average_price_dollars": average_price_dollars,
            "raw": raw,
            "created_at": now,
            "updated_at": now,
        }
        update_values = {
            "side": side,
            "count_fp": count_fp,
            "average_price_dollars": average_price_dollars,
            "raw": raw,
            "updated_at": now,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(PositionRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(PositionRecord).values(**insert_values)
        else:
            stmt = None
        if stmt is not None:
            await self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["kalshi_env", "market_ticker", "subaccount"],
                    set_=update_values,
                )
            )
            await self.session.flush()
            return (
                await self.session.execute(
                    select(PositionRecord).where(
                        PositionRecord.kalshi_env == env,
                        PositionRecord.market_ticker == market_ticker,
                        PositionRecord.subaccount == subaccount,
                    )
                )
            ).scalar_one()

        existing = (
            await self.session.execute(
                select(PositionRecord).where(
                    PositionRecord.kalshi_env == env,
                    PositionRecord.market_ticker == market_ticker,
                    PositionRecord.subaccount == subaccount,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = PositionRecord(**insert_values)
            self.session.add(existing)
        else:
            for key, value in update_values.items():
                setattr(existing, key, value)
        await self.session.flush()
        return existing

    async def log_ops_event(
        self,
        *,
        severity: str,
        summary: str,
        source: str,
        payload: dict[str, Any],
        room_id: str | None = None,
        kalshi_env: str | None = None,
    ) -> OpsEvent:
        record = OpsEvent(
            room_id=room_id,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            severity=severity,
            summary=summary,
            source=source,
            payload=payload,
        )
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

    async def upsert_room_strategy_audit(
        self,
        *,
        room_id: str,
        market_ticker: str,
        audit_source: str,
        audit_version: str,
        thesis_correctness: str,
        trade_quality: str,
        block_correctness: str,
        missed_stand_down: bool,
        stale_data_mismatch: bool,
        effective_freshness_agreement: bool,
        resolution_state: str | None,
        eligibility_passed: bool | None,
        stand_down_reason: str | None,
        trainable_default: bool,
        exclude_reason: str | None,
        quality_warnings: list[str],
        payload: dict[str, Any],
    ) -> RoomStrategyAuditRecord:
        record = await self.session.get(RoomStrategyAuditRecord, room_id)
        if record is None:
            record = RoomStrategyAuditRecord(room_id=room_id, market_ticker=market_ticker, payload={})
            self.session.add(record)
        record.market_ticker = market_ticker
        record.audit_source = audit_source
        record.audit_version = audit_version
        record.thesis_correctness = thesis_correctness
        record.trade_quality = trade_quality
        record.block_correctness = block_correctness
        record.missed_stand_down = missed_stand_down
        record.stale_data_mismatch = stale_data_mismatch
        record.effective_freshness_agreement = effective_freshness_agreement
        record.resolution_state = resolution_state
        record.eligibility_passed = eligibility_passed
        record.stand_down_reason = stand_down_reason
        record.trainable_default = trainable_default
        record.exclude_reason = exclude_reason
        record.quality_warnings = quality_warnings
        record.payload = payload
        await self.session.flush()
        return record

    async def get_room_strategy_audit(self, room_id: str) -> RoomStrategyAuditRecord | None:
        return await self.session.get(RoomStrategyAuditRecord, room_id)

    async def list_room_strategy_audits(
        self,
        *,
        limit: int = 200,
        since: datetime | None = None,
        market_ticker: str | None = None,
        audit_source: str | None = None,
        trainable_default: bool | None = None,
    ) -> list[RoomStrategyAuditRecord]:
        stmt = select(RoomStrategyAuditRecord)
        if since is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.updated_at >= since)
        if market_ticker is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.market_ticker == market_ticker)
        if audit_source is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.audit_source == audit_source)
        if trainable_default is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.trainable_default.is_(trainable_default))
        result = await self.session.execute(stmt.order_by(RoomStrategyAuditRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

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

    async def list_positions(
        self,
        limit: int = 50,
        kalshi_env: str | None = None,
        subaccount: int | None = None,
    ) -> list[PositionRecord]:
        stmt = select(PositionRecord).where(PositionRecord.count_fp != 0)
        if kalshi_env is not None:
            stmt = stmt.where(PositionRecord.kalshi_env == kalshi_env)
        if subaccount is not None:
            stmt = stmt.where(PositionRecord.subaccount == subaccount)
        stmt = stmt.order_by(PositionRecord.updated_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def portfolio_bucket_snapshot(
        self,
        *,
        kalshi_env: str,
        subaccount: int,
        total_capital_dollars: Decimal,
        safe_capital_reserve_ratio: float,
        risky_capital_max_ratio: float,
    ) -> PortfolioBucketSnapshot:
        positions = await self.list_positions(limit=5000, kalshi_env=kalshi_env, subaccount=subaccount)
        signal_payloads = await self.latest_signal_payloads_for_markets(
            market_tickers=[position.market_ticker for position in positions],
            kalshi_env=kalshi_env,
        )

        overall_used = Decimal("0.0000")
        safe_used = Decimal("0.0000")
        risky_used = Decimal("0.0000")
        for position in positions:
            notional = _quantize_money(abs(Decimal(str(position.count_fp))) * Decimal(str(position.average_price_dollars)))
            overall_used += notional
            bucket = _capital_bucket_from_signal_payload(signal_payloads.get(position.market_ticker))
            if bucket == "safe":
                safe_used += notional
            else:
                risky_used += notional

        total_capital = _quantize_money(total_capital_dollars)
        risky_limit = _quantize_money(total_capital * Decimal(str(risky_capital_max_ratio)))
        safe_reserve_target = _quantize_money(total_capital * Decimal(str(safe_capital_reserve_ratio)))
        overall_remaining = _quantize_money(max(Decimal("0.0000"), total_capital - overall_used))
        risky_remaining = _quantize_money(max(Decimal("0.0000"), min(overall_remaining, risky_limit - risky_used)))
        safe_remaining = overall_remaining

        return PortfolioBucketSnapshot(
            total_capital_dollars=total_capital,
            overall_used_dollars=overall_used,
            overall_remaining_dollars=overall_remaining,
            safe_used_dollars=safe_used,
            safe_remaining_dollars=safe_remaining,
            safe_reserve_target_dollars=safe_reserve_target,
            risky_used_dollars=risky_used,
            risky_limit_dollars=risky_limit,
            risky_remaining_dollars=risky_remaining,
            safe_capital_reserve_ratio=safe_capital_reserve_ratio,
            risky_capital_max_ratio=risky_capital_max_ratio,
        )

    async def list_ops_events(
        self,
        *,
        limit: int = 50,
        sources: list[str] | None = None,
        created_after: datetime | None = None,
        kalshi_env: str | None = None,
    ) -> list[OpsEvent]:
        stmt = select(OpsEvent)
        if kalshi_env is not None:
            stmt = stmt.where(OpsEvent.kalshi_env == self._resolved_kalshi_env(kalshi_env))
        if sources:
            stmt = stmt.where(OpsEvent.source.in_(sources))
        if created_after is not None:
            stmt = stmt.where(OpsEvent.updated_at >= created_after)
        result = await self.session.execute(stmt.order_by(OpsEvent.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def set_checkpoint(self, stream_name: str, cursor: str | None, payload: dict[str, Any]) -> Checkpoint:
        bind = self.session.get_bind()
        dialect_name = bind.dialect.name if bind is not None else ""
        now = datetime.now(UTC)
        insert_values = {
            "id": str(uuid4()),
            "stream_name": stream_name,
            "cursor": cursor,
            "payload": payload,
            "created_at": now,
            "updated_at": now,
        }
        if dialect_name in {"postgresql", "sqlite"}:
            insert_stmt = (pg_insert if dialect_name == "postgresql" else sqlite_insert)(Checkpoint).values(
                **insert_values
            )
            stmt = (
                insert_stmt.on_conflict_do_update(
                    index_elements=["stream_name"],
                    set_={
                        "cursor": insert_stmt.excluded.cursor,
                        "payload": insert_stmt.excluded.payload,
                        "updated_at": now,
                    },
                )
                .returning(Checkpoint.id)
            )
            checkpoint_id = (await self.session.execute(stmt)).scalar_one()
            checkpoint = await self.session.get(Checkpoint, checkpoint_id, populate_existing=True)
            if checkpoint is None:
                raise RuntimeError(f"checkpoint upsert returned missing id for {stream_name}")
            return checkpoint

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

    async def list_checkpoints(
        self,
        *,
        prefix: str | None = None,
        limit: int = 500,
    ) -> list[Checkpoint]:
        stmt = select(Checkpoint)
        if prefix is not None:
            stmt = stmt.where(Checkpoint.stream_name.like(f"{prefix}%"))
        stmt = stmt.order_by(Checkpoint.stream_name.asc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_total_capital_dollars(self, *, kalshi_env: str | None = None) -> Decimal | None:
        """Return total portfolio value (cash + positions) from the latest reconcile checkpoint."""
        checkpoint = await self.get_checkpoint(self._env_stream_name("reconcile", kalshi_env=kalshi_env))
        if checkpoint is None:
            return None
        balance_payload = dict((checkpoint.payload or {}).get("balance") or {})
        cash_cents = None
        for key in ("balance", "cash_balance", "cash"):
            raw = balance_payload.get(key)
            if raw is not None:
                try:
                    cash_cents = Decimal(str(raw))
                    break
                except ArithmeticError:
                    pass
        positions_cents = None
        for key in ("portfolio_value", "portfolioValue"):
            raw = balance_payload.get(key)
            if raw is not None:
                try:
                    positions_cents = Decimal(str(raw))
                    break
                except ArithmeticError:
                    pass
        if cash_cents is None:
            return None
        # Kalshi returns portfolio_value as positions market value when it's < cash
        effective_positions = positions_cents if (positions_cents is not None and positions_cents < cash_cents) else Decimal("0")
        return (cash_cents + effective_positions) / Decimal("100")

    @staticmethod
    def _pacific_today() -> str:
        """Return today's date string in Pacific Time (YYYY-MM-DD)."""
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")

    async def get_daily_portfolio_baseline_dollars(
        self,
        *,
        pacific_date: str | None = None,
        kalshi_env: str | None = None,
    ) -> Decimal | None:
        date = pacific_date or self._pacific_today()
        checkpoint = await self.get_checkpoint(self._env_stream_name("daily_portfolio", kalshi_env=kalshi_env, suffix=date))
        if checkpoint is None:
            return None
        raw = (checkpoint.payload or {}).get("total_capital_dollars")
        if raw is None:
            return None
        try:
            return Decimal(str(raw))
        except ArithmeticError:
            return None

    async def set_daily_portfolio_baseline_dollars(
        self,
        total_capital_dollars: Decimal,
        *,
        pacific_date: str | None = None,
        kalshi_env: str | None = None,
    ) -> None:
        date = pacific_date or self._pacific_today()
        await self.set_checkpoint(
            self._env_stream_name("daily_portfolio", kalshi_env=kalshi_env, suffix=date),
            cursor=None,
            payload={"total_capital_dollars": str(total_capital_dollars), "date": date},
        )

    async def get_daily_pnl_dollars(self, *, kalshi_env: str | None = None) -> Decimal | None:
        """Return today's P&L: current portfolio value minus start-of-day baseline (Pacific Time)."""
        current = await self.get_total_capital_dollars(kalshi_env=kalshi_env)
        baseline = await self.get_daily_portfolio_baseline_dollars(kalshi_env=kalshi_env)
        if current is None or baseline is None:
            return None
        return (current - baseline).quantize(Decimal("0.01"))

    async def list_exchange_events(
        self,
        *,
        stream_name: str | None = None,
        event_type: str | None = None,
        market_ticker: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 50,
    ) -> list[RawExchangeEvent]:
        stmt = select(RawExchangeEvent)
        if stream_name is not None:
            stmt = stmt.where(RawExchangeEvent.stream_name == stream_name)
        if event_type is not None:
            stmt = stmt.where(RawExchangeEvent.event_type == event_type)
        if market_ticker is not None:
            stmt = stmt.where(RawExchangeEvent.market_ticker == market_ticker)
        if created_after is not None:
            stmt = stmt.where(RawExchangeEvent.created_at >= created_after)
        if created_before is not None:
            stmt = stmt.where(RawExchangeEvent.created_at <= created_before)
        stmt = stmt.order_by(RawExchangeEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_weather_events(
        self,
        *,
        station_id: str | None = None,
        event_type: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 200,
    ) -> list[RawWeatherEvent]:
        stmt = select(RawWeatherEvent)
        if station_id is not None:
            stmt = stmt.where(RawWeatherEvent.station_id == station_id)
        if event_type is not None:
            stmt = stmt.where(RawWeatherEvent.event_type == event_type)
        if created_after is not None:
            stmt = stmt.where(RawWeatherEvent.created_at >= created_after)
        if created_before is not None:
            stmt = stmt.where(RawWeatherEvent.created_at <= created_before)
        stmt = stmt.order_by(RawWeatherEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())
