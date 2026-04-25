from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import JSON, Boolean, CheckConstraint, Date, DateTime, Float, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kalshi_bot.core.enums import DeploymentColor, RiskStatus, RoomOrigin, RoomStage
from kalshi_bot.db.base import Base, IdMixin, TimestampMixin
from kalshi_bot.db.types import EmbeddingType


def _jsonb() -> JSON:
    return JSON().with_variant(JSONB(), "postgresql")


class Room(Base, IdMixin, TimestampMixin):
    __tablename__ = "rooms"

    name: Mapped[str] = mapped_column(String(255))
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    room_origin: Mapped[str] = mapped_column(String(32), default=RoomOrigin.SHADOW.value, index=True)
    prompt: Mapped[str | None] = mapped_column(Text(), nullable=True)
    kalshi_env: Mapped[str] = mapped_column(String(32), default="demo", index=True)
    stage: Mapped[str] = mapped_column(String(32), default=RoomStage.TRIGGERED.value)
    active_color: Mapped[str] = mapped_column(String(16), default=DeploymentColor.BLUE.value)
    shadow_mode: Mapped[bool] = mapped_column(Boolean, default=True)
    kill_switch_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    agent_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    evaluation_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    role_models: Mapped[dict] = mapped_column(JSON, default=dict)

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

    kalshi_env: Mapped[str] = mapped_column(String(16), primary_key=True, default="demo")
    market_ticker: Mapped[str] = mapped_column(String(128), primary_key=True)
    source: Mapped[str] = mapped_column(String(64), default="kalshi")
    yes_bid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_trade_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class MarketPriceHistory(Base, IdMixin):
    __tablename__ = "market_price_history"

    kalshi_env: Mapped[str] = mapped_column(String(16), nullable=False, default="demo", index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False)
    yes_bid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    mid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_trade_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


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
    strategy_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
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
    __table_args__ = (UniqueConstraint("kalshi_env", "client_order_id", name="uq_orders_env_client_order_id"),)

    trade_ticket_id: Mapped[str | None] = mapped_column(ForeignKey("trade_tickets.id", ondelete="SET NULL"), nullable=True)
    kalshi_env: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    kalshi_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    client_order_id: Mapped[str] = mapped_column(String(64))
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    action: Mapped[str] = mapped_column(String(16))
    yes_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    strategy_code: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class FillRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "fills"
    __table_args__ = (UniqueConstraint("kalshi_env", "trade_id", name="uq_fills_env_trade_id"),)

    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    kalshi_env: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    trade_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    side: Mapped[str] = mapped_column(String(16))
    action: Mapped[str] = mapped_column(String(16))
    yes_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    is_taker: Mapped[bool] = mapped_column(Boolean, default=True)
    settlement_result: Mapped[str | None] = mapped_column(String(8), nullable=True)
    strategy_code: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class PositionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("kalshi_env", "market_ticker", "subaccount", name="uq_positions_env_market_subaccount"),)

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    subaccount: Mapped[int] = mapped_column(Integer, default=0)
    kalshi_env: Mapped[str] = mapped_column(String(16), default="demo", index=True)
    side: Mapped[str] = mapped_column(String(16))
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    average_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class OpsEvent(Base, IdMixin, TimestampMixin):
    __tablename__ = "ops_events"

    room_id: Mapped[str | None] = mapped_column(ForeignKey("rooms.id", ondelete="SET NULL"), nullable=True, index=True)
    kalshi_env: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    severity: Mapped[str] = mapped_column(String(16))
    summary: Mapped[str] = mapped_column(Text())
    source: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class WebUser(Base, IdMixin, TimestampMixin):
    __tablename__ = "web_users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    password_salt: Mapped[str] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    sessions: Mapped[list["WebSession"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class WebSession(Base, IdMixin, TimestampMixin):
    __tablename__ = "web_sessions"

    user_id: Mapped[str] = mapped_column(ForeignKey("web_users.id", ondelete="CASCADE"), index=True)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    user: Mapped[WebUser] = relationship(back_populates="sessions")


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


class RoomCampaignRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "room_campaigns"
    __table_args__ = (UniqueConstraint("room_id", name="uq_room_campaign_room_id"),)

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    campaign_id: Mapped[str] = mapped_column(String(64), index=True)
    trigger_source: Mapped[str] = mapped_column(String(64), index=True)
    city_bucket: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    market_regime_bucket: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    difficulty_bucket: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    outcome_bucket: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    dossier_artifact_id: Mapped[str | None] = mapped_column(ForeignKey("artifacts.id", ondelete="SET NULL"), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class RoomResearchHealthRecord(Base, TimestampMixin):
    __tablename__ = "room_research_health"

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), primary_key=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    dossier_status: Mapped[str] = mapped_column(String(32), default="missing", index=True)
    gate_passed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    valid_dossier: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    good_for_training: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    quality_score: Mapped[float] = mapped_column(default=0.0, index=True)
    citation_coverage_score: Mapped[float] = mapped_column(default=0.0)
    settlement_clarity_score: Mapped[float] = mapped_column(default=0.0)
    freshness_score: Mapped[float] = mapped_column(default=0.0)
    contradiction_count: Mapped[int] = mapped_column(Integer, default=0)
    structured_completeness_score: Mapped[float] = mapped_column(default=0.0)
    fair_value_score: Mapped[float] = mapped_column(default=0.0)
    dossier_artifact_id: Mapped[str | None] = mapped_column(ForeignKey("artifacts.id", ondelete="SET NULL"), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class RoomStrategyAuditRecord(Base, TimestampMixin):
    __tablename__ = "room_strategy_audits"

    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), primary_key=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    audit_source: Mapped[str] = mapped_column(String(32), default="live_forward", index=True)
    audit_version: Mapped[str] = mapped_column(String(64), default="weather-quality-v1", index=True)
    thesis_correctness: Mapped[str] = mapped_column(String(32), default="unresolved", index=True)
    trade_quality: Mapped[str] = mapped_column(String(32), default="stand_down", index=True)
    block_correctness: Mapped[str] = mapped_column(String(32), default="not_applicable", index=True)
    missed_stand_down: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    stale_data_mismatch: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    effective_freshness_agreement: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    resolution_state: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    eligibility_passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True, index=True)
    stand_down_reason: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    trainable_default: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    exclude_reason: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    quality_warnings: Mapped[list[str]] = mapped_column(JSON, default=list)
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


class AgentPackRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "agent_packs"
    __table_args__ = (UniqueConstraint("version", name="uq_agent_packs_version"),)

    version: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), default="candidate", index=True)
    parent_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(64), default="builtin")
    description: Mapped[str] = mapped_column(Text(), default="")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class CritiqueRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "critique_runs"

    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    source_pack_version: Mapped[str] = mapped_column(String(128), index=True)
    candidate_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    room_count: Mapped[int] = mapped_column(Integer, default=0)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)


class EvaluationRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "evaluation_runs"

    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    champion_version: Mapped[str] = mapped_column(String(128), index=True)
    candidate_version: Mapped[str] = mapped_column(String(128), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    holdout_room_count: Mapped[int] = mapped_column(Integer, default=0)
    passed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)


class PromotionEventRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "promotion_events"

    status: Mapped[str] = mapped_column(String(32), default="staged", index=True)
    candidate_version: Mapped[str] = mapped_column(String(128), index=True)
    previous_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    target_color: Mapped[str] = mapped_column(String(16), index=True)
    evaluation_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    rollback_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalIntelligenceRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_intelligence_runs"

    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    date_from: Mapped[str] = mapped_column(String(16), index=True)
    date_to: Mapped[str] = mapped_column(String(16), index=True)
    active_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    candidate_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    promoted_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    room_count: Mapped[int] = mapped_column(Integer, default=0)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)


class HistoricalPipelineRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_pipeline_runs"

    pipeline_kind: Mapped[str] = mapped_column(String(32), default="daily", index=True)
    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    date_from: Mapped[str] = mapped_column(String(16), index=True)
    date_to: Mapped[str] = mapped_column(String(16), index=True)
    rolling_days: Mapped[int] = mapped_column(Integer, default=365)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)


class HeuristicPackRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "heuristic_packs"
    __table_args__ = (UniqueConstraint("version", name="uq_heuristic_packs_version"),)

    version: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), default="candidate", index=True)
    parent_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    source: Mapped[str] = mapped_column(String(64), default="historical_intelligence")
    description: Mapped[str] = mapped_column(Text(), default="")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HeuristicPackPromotionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "heuristic_pack_promotions"

    status: Mapped[str] = mapped_column(String(32), default="staged", index=True)
    candidate_version: Mapped[str] = mapped_column(String(128), index=True)
    previous_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    intelligence_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    rollback_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HeuristicPatchSuggestionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "heuristic_patch_suggestions"

    heuristic_pack_version: Mapped[str] = mapped_column(String(128), index=True)
    intelligence_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="candidate", index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class TrainingDatasetBuildRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "training_dataset_builds"
    __table_args__ = (UniqueConstraint("build_version", name="uq_training_dataset_builds_version"),)

    build_version: Mapped[str] = mapped_column(String(128), index=True)
    mode: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="completed", index=True)
    selection_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    selection_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    room_count: Mapped[int] = mapped_column(Integer, default=0)
    filters: Mapped[dict] = mapped_column(JSON, default=dict)
    label_stats: Mapped[dict] = mapped_column(JSON, default=dict)
    pack_versions: Mapped[list[str]] = mapped_column(JSON, default=list)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class TrainingDatasetBuildItemRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "training_dataset_build_items"
    __table_args__ = (UniqueConstraint("dataset_build_id", "room_id", name="uq_training_dataset_build_items_room"),)

    dataset_build_id: Mapped[str] = mapped_column(ForeignKey("training_dataset_builds.id", ondelete="CASCADE"), index=True)
    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), index=True)
    sequence: Mapped[int] = mapped_column(Integer, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class TrainingReadinessRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "training_readiness"

    ready_for_sft_export: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ready_for_critique: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ready_for_evaluation: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ready_for_promotion: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    complete_room_count: Mapped[int] = mapped_column(Integer, default=0)
    market_diversity_count: Mapped[int] = mapped_column(Integer, default=0)
    settled_room_count: Mapped[int] = mapped_column(Integer, default=0)
    trade_positive_room_count: Mapped[int] = mapped_column(Integer, default=0)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalImportRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_import_runs"

    import_kind: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    source: Mapped[str] = mapped_column(String(64), default="kalshi_history")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalMarketSnapshotRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_market_snapshots"
    __table_args__ = (UniqueConstraint("market_ticker", "source_kind", "source_id", name="uq_historical_market_snapshot_source"),)

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    station_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), index=True)
    asof_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_kind: Mapped[str] = mapped_column(String(64), index=True)
    source_id: Mapped[str] = mapped_column(String(255), index=True)
    source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    close_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    settlement_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    yes_bid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    no_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_price_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalWeatherSnapshotRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_weather_snapshots"
    __table_args__ = (UniqueConstraint("station_id", "source_kind", "source_id", name="uq_historical_weather_snapshot_source"),)

    station_id: Mapped[str] = mapped_column(String(32), index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), index=True)
    asof_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_kind: Mapped[str] = mapped_column(String(64), index=True)
    source_id: Mapped[str] = mapped_column(String(255), index=True)
    source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    observation_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    forecast_updated_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    forecast_high_f: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    current_temp_f: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalCheckpointArchiveRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_checkpoint_archives"
    __table_args__ = (
        UniqueConstraint("series_ticker", "local_market_day", "checkpoint_label", name="uq_historical_checkpoint_archive_slot"),
    )

    series_ticker: Mapped[str] = mapped_column(String(128), index=True)
    market_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    station_id: Mapped[str] = mapped_column(String(32), index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), index=True)
    checkpoint_label: Mapped[str] = mapped_column(String(32), index=True)
    checkpoint_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_kind: Mapped[str] = mapped_column(String(64), index=True)
    source_id: Mapped[str] = mapped_column(String(255), index=True)
    source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    observation_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    forecast_updated_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    archive_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalSettlementLabelRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_settlement_labels"
    __table_args__ = (UniqueConstraint("market_ticker", name="uq_historical_settlement_labels_market_ticker"),)

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), index=True)
    source_kind: Mapped[str] = mapped_column(String(64), default="kalshi_primary", index=True)
    kalshi_result: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    settlement_value_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    settlement_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    crosscheck_status: Mapped[str] = mapped_column(String(32), default="missing", index=True)
    crosscheck_high_f: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    crosscheck_result: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class HistoricalReplayRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "historical_replay_runs"
    __table_args__ = (
        UniqueConstraint("room_id", name="uq_historical_replay_runs_room"),
        UniqueConstraint("market_ticker", "checkpoint_ts", name="uq_historical_replay_runs_checkpoint"),
    )

    room_id: Mapped[str | None] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), nullable=True, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), index=True)
    checkpoint_label: Mapped[str] = mapped_column(String(32))
    checkpoint_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(32), default="completed", index=True)
    agent_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class DecisionCorpusBuildRecord(Base, IdMixin, TimestampMixin):
    """Build-level metadata for immutable decision corpus row sets.

    Rows are inserted while a build is in progress. Once a build leaves
    ``in_progress``, application code creates a new build rather than mutating
    rows in the completed one.
    """

    __tablename__ = "decision_corpus_builds"

    version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="in_progress", index=True)
    git_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source: Mapped[dict] = mapped_column(JSON, default=dict)
    filters: Mapped[dict] = mapped_column(JSON, default=dict)
    date_from: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    date_to: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parent_build_id: Mapped[str | None] = mapped_column(
        ForeignKey("decision_corpus_builds.id"),
        nullable=True,
        index=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class DecisionCorpusRowRecord(Base, IdMixin):
    """Per-decision historical corpus row.

    The repository intentionally exposes insert/list/get paths only. If a row is
    wrong, the normal-operation fix is to create a new build with corrected
    derivation logic.
    """

    __tablename__ = "decision_corpus_rows"
    __table_args__ = (
        CheckConstraint(
            "support_status IN ('supported', 'exploratory', 'insufficient')",
            name="ck_decision_corpus_support_status",
        ),
        CheckConstraint(
            "support_level IN ("
            "'L1_station_season_lead_regime', "
            "'L2_station_season_lead', "
            "'L3_station_season', "
            "'L4_season_lead', "
            "'L5_global')",
            name="ck_decision_corpus_support_level",
        ),
        CheckConstraint(
            "source_provenance IN ("
            "'historical_replay_full_checkpoint', "
            "'historical_replay_partial_checkpoint', "
            "'historical_replay_late_only', "
            "'historical_replay_external_forecast_repair', "
            "'historical_replay_unknown')",
            name="ck_decision_corpus_source_provenance",
        ),
        UniqueConstraint(
            "corpus_build_id",
            "room_id",
            "market_ticker",
            "checkpoint_ts",
            "policy_version",
            "model_version",
            name="uq_decision_corpus_row_identity",
        ),
    )

    corpus_build_id: Mapped[str] = mapped_column(
        ForeignKey("decision_corpus_builds.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    room_id: Mapped[str] = mapped_column(ForeignKey("rooms.id", ondelete="CASCADE"), nullable=False, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    station_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    local_market_day: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    checkpoint_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    kalshi_env: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    deployment_color: Mapped[str | None] = mapped_column(String(16), nullable=True)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    policy_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_asof_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    quote_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    quote_captured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    time_to_settlement_at_checkpoint_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fair_yes_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    edge_bps: Mapped[int | None] = mapped_column(Integer, nullable=True)
    recommended_side: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    target_yes_price_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    eligibility_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    stand_down_reason: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    trade_regime: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    liquidity_regime: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    support_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    support_level: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    support_n: Mapped[int] = mapped_column(Integer, nullable=False)
    support_market_days: Mapped[int] = mapped_column(Integer, nullable=False)
    support_recency_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    backoff_path: Mapped[list] = mapped_column(JSON, default=list)
    settlement_result: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    settlement_value_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    pnl_counterfactual_target_frictionless: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    pnl_counterfactual_target_with_fees: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    pnl_model_fair_frictionless: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    pnl_executed_realized: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    fee_counterfactual_dollars: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    counterfactual_count: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    executed_count: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    fee_model_version: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_provenance: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_details: Mapped[dict] = mapped_column(JSON, default=dict)
    signal_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    quote_snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    settlement_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    diagnostics: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        nullable=False,
        index=True,
    )


class Checkpoint(Base, IdMixin, TimestampMixin):
    __tablename__ = "checkpoints"
    __table_args__ = (UniqueConstraint("stream_name", name="uq_checkpoint_stream_name"),)

    stream_name: Mapped[str] = mapped_column(String(128))
    cursor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class DeploymentControl(Base):
    __tablename__ = "deployment_control"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default="demo")
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
Index(
    "ix_decision_corpus_rows_day_env_policy",
    DecisionCorpusRowRecord.local_market_day,
    DecisionCorpusRowRecord.kalshi_env,
    DecisionCorpusRowRecord.policy_version,
)
Index(
    "ix_decision_corpus_rows_series_day",
    DecisionCorpusRowRecord.series_ticker,
    DecisionCorpusRowRecord.local_market_day,
)


class StrategyRecord(Base):
    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(64), default="builtin", index=True)
    strategy_metadata: Mapped[dict] = mapped_column("metadata", JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class StrategyResultRecord(Base):
    __tablename__ = "strategy_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), index=True)
    corpus_build_id: Mapped[str | None] = mapped_column(
        ForeignKey("decision_corpus_builds.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    date_from: Mapped[date] = mapped_column(Date)
    date_to: Mapped[date] = mapped_column(Date)
    series_ticker: Mapped[str] = mapped_column(String(64), index=True)
    rooms_evaluated: Mapped[int] = mapped_column(Integer, default=0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    resolved_trade_count: Mapped[int] = mapped_column(Integer, default=0)
    unscored_trade_count: Mapped[int] = mapped_column(Integer, default=0)
    win_count: Mapped[int] = mapped_column(Integer, default=0)
    total_pnl_dollars: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    trade_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    win_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    avg_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), nullable=True)


class CityStrategyAssignment(Base):
    __tablename__ = "city_strategy_assignments"

    kalshi_env: Mapped[str] = mapped_column(String(16), primary_key=True, default="demo")
    series_ticker: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(64))
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    assigned_by: Mapped[str] = mapped_column(String(64), default="auto_regression")
    evidence_corpus_build_id: Mapped[str | None] = mapped_column(
        ForeignKey("decision_corpus_builds.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    evidence_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class StrategyPromotionRecord(Base):
    __tablename__ = "strategy_promotions"
    __table_args__ = (
        CheckConstraint(
            "watchdog_status IN ('pending', 'extended', 'passed', 'rolled_back', 'insufficient_data')",
            name="ck_strategy_promotions_watchdog_status",
        ),
        CheckConstraint(
            "secondary_sync_status IN ('pending', 'failed', 'synced', 'ignored_by_operator', 'not_applicable')",
            name="ck_strategy_promotions_secondary_sync_status",
        ),
        CheckConstraint(
            "secondary_rollback_status IN ('pending', 'failed', 'synced', 'ignored_by_operator', 'not_applicable')",
            name="ck_strategy_promotions_secondary_rollback_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kalshi_env: Mapped[str] = mapped_column(String(16), nullable=False)
    promoted_strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    promoted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        nullable=False,
    )
    previous_city_assignments: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    new_city_assignments: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    baseline_metrics: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    rollback_metrics: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    promotion_details: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    rollback_details: Mapped[dict] = mapped_column(_jsonb(), default=dict, server_default=text("'{}'"))
    rollback_skipped_cities: Mapped[list] = mapped_column(_jsonb(), default=list, server_default=text("'[]'"))
    watchdog_due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    watchdog_extended_due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    watchdog_status: Mapped[str] = mapped_column(String(32), default="pending", server_default="pending")
    watchdog_extended_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    watchdog_extended_detail: Mapped[str | None] = mapped_column(Text(), nullable=True)
    watchdog_last_eval_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    watchdog_last_eval_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    rollback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rollback_trigger: Mapped[str | None] = mapped_column(String(128), nullable=True)
    trigger_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolution_data: Mapped[dict | None] = mapped_column(_jsonb(), nullable=True)
    secondary_sync_status: Mapped[str] = mapped_column(String(32), default="not_applicable", server_default="not_applicable")
    secondary_sync_error: Mapped[str | None] = mapped_column(Text(), nullable=True)
    secondary_sync_resolution: Mapped[dict | None] = mapped_column(_jsonb(), nullable=True)
    secondary_rollback_status: Mapped[str] = mapped_column(String(32), default="not_applicable", server_default="not_applicable")
    secondary_rollback_error: Mapped[str | None] = mapped_column(Text(), nullable=True)
    secondary_rollback_resolution: Mapped[dict | None] = mapped_column(_jsonb(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    assignment_events: Mapped[list["CityAssignmentEventRecord"]] = relationship(back_populates="promotion")


class CityAssignmentEventRecord(Base):
    __tablename__ = "city_assignment_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('auto_evolve_assign', 'manual_assign', 'manual_override', 'rollback_restore', 'rollback_delete')",
            name="ck_city_assignment_events_event_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kalshi_env: Mapped[str] = mapped_column(String(16), nullable=False)
    series_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_strategy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    new_strategy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    note: Mapped[str | None] = mapped_column(Text(), nullable=True)
    promotion_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_promotions.id", ondelete="SET NULL"),
        nullable=True,
    )
    event_metadata: Mapped[dict] = mapped_column("metadata", _jsonb(), default=dict, server_default=text("'{}'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    promotion: Mapped[StrategyPromotionRecord | None] = relationship(back_populates="assignment_events")


Index(
    "ix_strategy_promotions_env_status_due",
    StrategyPromotionRecord.kalshi_env,
    StrategyPromotionRecord.watchdog_status,
    StrategyPromotionRecord.watchdog_due_at,
)
Index("ix_strategy_promotions_strategy", StrategyPromotionRecord.promoted_strategy_name)
Index(
    "ix_city_assignment_events_env_city_created",
    CityAssignmentEventRecord.kalshi_env,
    CityAssignmentEventRecord.series_ticker,
    CityAssignmentEventRecord.created_at,
)
Index("ix_city_assignment_events_promotion", CityAssignmentEventRecord.promotion_id)


class StrategyPromotionEvent(Base):
    """Audit log row for a strategy shadow→live (or live→shadow) transition.

    P2-3 — inserted via the ``record-strategy-promotion`` CLI so the operator's
    intent, identity, and evidence reference are captured alongside the
    environment change. Intentionally not a per-row foreign key to
    ``strategies.name`` — the strategy column accepts short codes (A, C, ARB)
    and may reference strategies that were never persisted as StrategyRecord.
    """

    __tablename__ = "strategy_promotion_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    from_state: Mapped[str] = mapped_column(String(32), nullable=False)
    to_state: Mapped[str] = mapped_column(String(32), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    evidence_ref: Mapped[str | None] = mapped_column(Text(), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    kalshi_env: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        nullable=False,
        index=True,
    )


class StrategyCodexRunRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "strategy_codex_runs"

    mode: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    trigger_source: Mapped[str] = mapped_column(String(32), default="manual", index=True)
    window_days: Mapped[int] = mapped_column(Integer, index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    strategy_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    operator_brief: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider: Mapped[str] = mapped_column(String(64), default="codex-cli")
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class StationSigmaParams(Base):
    """Per-(station, season) sigma_base fit. Lead correction lives in GlobalLeadFactor."""

    __tablename__ = "station_sigma_params"
    __table_args__ = (
        UniqueConstraint("station", "season_bucket", "version", name="uq_station_sigma_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    station: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    season_bucket: Mapped[str] = mapped_column(String(4), nullable=False)  # DJF/MAM/JJA/SON
    sigma_base_f: Mapped[float] = mapped_column(Float, nullable=False)
    mean_bias_f: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    sigma_se_f: Mapped[float] = mapped_column(Float, nullable=False)
    residual_skewness: Mapped[float | None] = mapped_column(Float, nullable=True)
    crps_improvement_vs_global: Mapped[float | None] = mapped_column(Float, nullable=True)
    fitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class GlobalLeadFactor(Base):
    """Global lead-time σ scaling factor, fit across all stations and seasons."""

    __tablename__ = "global_lead_factor"
    __table_args__ = (
        UniqueConstraint("lead_bucket", "version", name="uq_lead_factor_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_bucket: Mapped[str] = mapped_column(String(8), nullable=False)  # D-0, D-1, D-2+
    factor: Mapped[float] = mapped_column(Float, nullable=False)  # normalised: D-0 = 1.0
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    fitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


# ---------------------------------------------------------------------------
# Strategy C — Addition 1 tables (§4.1.5)
# ---------------------------------------------------------------------------

class CliReconciliationRecord(Base):
    """Daily CLI vs ASOS observed max per station. Composite PK (station, date).

    Populated by backfill_cli_reconciliation.py and extended daily post-settlement.
    Source of truth for cli_station_variance rollup.
    """
    __tablename__ = "cli_reconciliation"

    station: Mapped[str] = mapped_column(String(32), primary_key=True)
    observation_date: Mapped[date] = mapped_column(Date(), primary_key=True)
    asos_observed_max: Mapped[float] = mapped_column(Float, nullable=False)
    asos_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cli_value: Mapped[float] = mapped_column(Float, nullable=False)
    cli_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delta_degf: Mapped[float] = mapped_column(Float, nullable=False)   # cli_value - asos_observed_max
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )


class StrategyCRoom(Base):
    """Per-decision record for Strategy C (lock-confirmation) trades.

    execution_outcome: what happened between signal and order resolution (stage 1).
    settlement_outcome: did CLI agree with the asserted lock (stage 2, diagnostic).
    Do not collapse these into a single outcome column.
    """
    __tablename__ = "strategy_c_rooms"

    room_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(__import__("uuid").uuid4()))
    ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    station: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    decision_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    resolution_state: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_max_at_decision: Mapped[float] = mapped_column(Float, nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    fair_value_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    modeled_edge_cents: Mapped[float] = mapped_column(Float, nullable=False)
    target_price_cents: Mapped[float] = mapped_column(Float, nullable=False)
    contracts_requested: Mapped[int] = mapped_column(Integer, nullable=False)
    contracts_filled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_fill_price_cents: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_edge_cents: Mapped[float | None] = mapped_column(Float, nullable=True)
    execution_outcome: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    settlement_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    outcome_pnl_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=lambda: datetime.now(UTC),
    )


class CliStationVariance(Base):
    """Per-station CLI/ASOS variance rollup. Recomputed periodically from cli_reconciliation.

    Signed columns: retained for future parametric calibration (not consumed by default pricing).
    Abs-value columns: used for dashboards and anomaly detection only.
    """
    __tablename__ = "cli_station_variance"

    station: Mapped[str] = mapped_column(String(32), primary_key=True)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    signed_mean_delta_degf: Mapped[float] = mapped_column(Float, nullable=False)
    signed_stddev_delta_degf: Mapped[float] = mapped_column(Float, nullable=False)
    mean_abs_delta_degf: Mapped[float] = mapped_column(Float, nullable=False)
    p95_abs_delta_degf: Mapped[float] = mapped_column(Float, nullable=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)


class MonotonicityArbProposal(Base):
    """Per-proposal record for the Monotonicity Arb Scanner (Addition 3, §4.3).

    Each row represents a detected monotonicity violation: a pair of thresholds
    for the same station/date where bid_yes(T_j) > ask_yes(T_i).

    execution_outcome: 'shadow', 'risk_blocked', 'suppressed', or 'live'.
    """
    __tablename__ = "monotonicity_arb_proposals"

    proposal_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(__import__("uuid").uuid4()))
    station: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_date: Mapped[date] = mapped_column(Date(), nullable=False, index=True)
    ticker_low: Mapped[str] = mapped_column(String(128), nullable=False)
    ticker_high: Mapped[str] = mapped_column(String(128), nullable=False)
    threshold_low_f: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_high_f: Mapped[float] = mapped_column(Float, nullable=False)
    ask_yes_low_cents: Mapped[float] = mapped_column(Float, nullable=False)
    ask_no_high_cents: Mapped[float] = mapped_column(Float, nullable=False)
    total_cost_cents: Mapped[float] = mapped_column(Float, nullable=False)
    gross_edge_cents: Mapped[float] = mapped_column(Float, nullable=False)
    fee_estimate_cents: Mapped[float] = mapped_column(Float, nullable=False)
    net_edge_cents: Mapped[float] = mapped_column(Float, nullable=False)
    contracts_proposed: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_outcome: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    suppression_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
