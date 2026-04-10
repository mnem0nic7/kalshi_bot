from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kalshi_bot.core.enums import DeploymentColor, RiskStatus, RoomStage
from kalshi_bot.db.base import Base, IdMixin, TimestampMixin
from kalshi_bot.db.types import EmbeddingType


class Room(Base, IdMixin, TimestampMixin):
    __tablename__ = "rooms"

    name: Mapped[str] = mapped_column(String(255))
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    prompt: Mapped[str | None] = mapped_column(Text(), nullable=True)
    stage: Mapped[str] = mapped_column(String(32), default=RoomStage.TRIGGERED.value)
    active_color: Mapped[str] = mapped_column(String(16), default=DeploymentColor.BLUE.value)
    shadow_mode: Mapped[bool] = mapped_column(Boolean, default=True)
    kill_switch_enabled: Mapped[bool] = mapped_column(Boolean, default=False)

    messages: Mapped[list["RoomMessage"]] = relationship(back_populates="room", cascade="all, delete-orphan")


class RoomMessage(Base, IdMixin):
    __tablename__ = "room_messages"
    __table_args__ = (UniqueConstraint("room_id", "sequence", name="uq_room_message_sequence"),)

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    role: Mapped[str] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(64), index=True)
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sequence: Mapped[int] = mapped_column(Integer, index=True)
    content: Mapped[str] = mapped_column(Text())
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    room: Mapped[Room] = relationship(back_populates="messages")


class Artifact(Base, IdMixin, TimestampMixin):
    __tablename__ = "artifacts"

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    message_id: Mapped[str | None] = mapped_column(ForeignKey("room_messages.id", ondelete="SET NULL"), nullable=True)
    artifact_type: Mapped[str] = mapped_column(String(64))
    source: Mapped[str] = mapped_column(String(128))
    title: Mapped[str] = mapped_column(String(255))
    url: Mapped[str | None] = mapped_column(Text(), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fingerprint: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class RawExchangeEvent(Base, IdMixin):
    __tablename__ = "raw_exchange_events"

    stream_name: Mapped[str] = mapped_column(String(64), index=True)
    market_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class RawWeatherEvent(Base, IdMixin):
    __tablename__ = "raw_weather_events"

    station_id: Mapped[str] = mapped_column(String(32), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class MarketState(Base, TimestampMixin):
    __tablename__ = "market_state"

    market_ticker: Mapped[str] = mapped_column(String(128), primary_key=True)
    source: Mapped[str] = mapped_column(String(64), default="kalshi")
    yes_bid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_trade_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class Signal(Base, IdMixin, TimestampMixin):
    __tablename__ = "signals"

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    fair_yes_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    edge_bps: Mapped[int] = mapped_column(Integer)
    confidence: Mapped[float] = mapped_column()
    summary: Mapped[str] = mapped_column(Text())
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class TradeTicketRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "trade_tickets"
    __table_args__ = (UniqueConstraint("client_order_id", name="uq_trade_tickets_client_order_id"),)

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    message_id: Mapped[str | None] = mapped_column(ForeignKey("room_messages.id", ondelete="SET NULL"), nullable=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    action: Mapped[str] = mapped_column(String(16))
    side: Mapped[str] = mapped_column(String(16))
    yes_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    time_in_force: Mapped[str] = mapped_column(String(64))
    client_order_id: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="proposed")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class RiskVerdictRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "risk_verdicts"

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    ticket_id: Mapped[str] = mapped_column(ForeignKey("trade_tickets.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(32), default=RiskStatus.REVIEW.value)
    reasons: Mapped[list[str]] = mapped_column(JSON, default=list)
    approved_notional_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    approved_count_fp: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class OrderRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "orders"
    __table_args__ = (UniqueConstraint("client_order_id", name="uq_orders_client_order_id"),)

    trade_ticket_id: Mapped[str | None] = mapped_column(ForeignKey("trade_tickets.id", ondelete="SET NULL"), nullable=True)
    kalshi_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    client_order_id: Mapped[str] = mapped_column(String(64))
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    action: Mapped[str] = mapped_column(String(16))
    yes_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class FillRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "fills"
    __table_args__ = (UniqueConstraint("trade_id", name="uq_fills_trade_id"),)

    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    trade_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    side: Mapped[str] = mapped_column(String(16))
    action: Mapped[str] = mapped_column(String(16))
    yes_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    is_taker: Mapped[bool] = mapped_column(Boolean, default=True)
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class PositionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("market_ticker", "subaccount", name="uq_positions_market_subaccount"),)

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    subaccount: Mapped[int] = mapped_column(Integer, default=0)
    side: Mapped[str] = mapped_column(String(16))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    average_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class OpsEvent(Base, IdMixin, TimestampMixin):
    __tablename__ = "ops_events"

    room_id: Mapped[str | None] = mapped_column(ForeignKey("rooms.id", ondelete="SET NULL"), nullable=True, index=True)
    severity: Mapped[str] = mapped_column(String(16))
    summary: Mapped[str] = mapped_column(Text())
    source: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class ResearchDossierRecord(Base, TimestampMixin):
    __tablename__ = "research_dossiers"

    market_ticker: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), default="ready")
    mode: Mapped[str] = mapped_column(String(32), default="mixed")
    confidence: Mapped[float] = mapped_column(default=0.0)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    contradiction_count: Mapped[int] = mapped_column(Integer, default=0)
    unresolved_count: Mapped[int] = mapped_column(Integer, default=0)
    settlement_covered: Mapped[bool] = mapped_column(Boolean, default=False)
    last_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class ResearchRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "research_runs"

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    trigger_reason: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class ResearchSourceRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "research_sources"

    research_run_id: Mapped[str] = mapped_column(ForeignKey("research_runs.id", ondelete="CASCADE"), index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    source_key: Mapped[str] = mapped_column(String(255), index=True)
    source_class: Mapped[str] = mapped_column(String(64))
    trust_tier: Mapped[str] = mapped_column(String(32))
    publisher: Mapped[str] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(255))
    url: Mapped[str | None] = mapped_column(Text(), nullable=True)
    snippet: Mapped[str] = mapped_column(Text())
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class ResearchClaimRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "research_claims"

    research_run_id: Mapped[str] = mapped_column(ForeignKey("research_runs.id", ondelete="CASCADE"), index=True)
    research_source_id: Mapped[str | None] = mapped_column(ForeignKey("research_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    source_key: Mapped[str] = mapped_column(String(255), index=True)
    claim_text: Mapped[str] = mapped_column(Text())
    stance: Mapped[str] = mapped_column(String(32), default="context")
    settlement_critical: Mapped[bool] = mapped_column(Boolean, default=False)
    freshness_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class MemoryNoteRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "memory_notes"

    room_id: Mapped[str | None] = mapped_column(ForeignKey("rooms.id", ondelete="SET NULL"), nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(Text())
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    linked_message_ids: Mapped[list[str]] = mapped_column(JSON, default=list)


class MemoryEmbedding(Base, IdMixin, TimestampMixin):
    __tablename__ = "memory_embeddings"

    memory_note_id: Mapped[str] = mapped_column(ForeignKey("memory_notes.id", ondelete="CASCADE"), unique=True, index=True)
    provider: Mapped[str] = mapped_column(String(64))
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingType(16), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class Checkpoint(Base, IdMixin, TimestampMixin):
    __tablename__ = "checkpoints"
    __table_args__ = (UniqueConstraint("stream_name", name="uq_checkpoint_stream_name"),)

    stream_name: Mapped[str] = mapped_column(String(128))
    cursor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class DeploymentControl(Base):
    __tablename__ = "deployment_control"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default="default")
    active_color: Mapped[str] = mapped_column(String(16), default=DeploymentColor.BLUE.value)
    kill_switch_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    execution_lock_holder: Mapped[str | None] = mapped_column(String(64), nullable=True)
    shadow_color: Mapped[str | None] = mapped_column(String(16), nullable=True)
    notes: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


Index("ix_room_messages_room_created", RoomMessage.room_id, RoomMessage.created_at)
Index("ix_raw_exchange_events_stream_created", RawExchangeEvent.stream_name, RawExchangeEvent.created_at)
