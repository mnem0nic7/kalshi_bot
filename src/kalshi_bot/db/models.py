from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kalshi_bot.core.enums import DeploymentColor, RiskStatus, RoomOrigin, RoomStage
from kalshi_bot.db.base import Base, IdMixin, TimestampMixin
from kalshi_bot.db.types import EmbeddingType


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

    market_ticker: Mapped[str] = mapped_column(String(128), primary_key=True)
    source: Mapped[str] = mapped_column(String(64), default="kalshi")
    yes_bid_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    yes_ask_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    last_trade_dollars: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True)


class MarketPriceHistory(Base, IdMixin):
    __tablename__ = "market_price_history"

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
    settlement_result: Mapped[str | None] = mapped_column(String(8), nullable=True)
    raw: Mapped[dict] = mapped_column(JSON, default=dict)


class PositionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("market_ticker", "subaccount", name="uq_positions_market_subaccount"),)

    market_ticker: Mapped[str] = mapped_column(String(128), index=True)
    subaccount: Mapped[int] = mapped_column(Integer, default=0)
    kalshi_env: Mapped[str] = mapped_column(String(16), default="")
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


class StrategyRecord(Base):
    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class StrategyResultRecord(Base):
    __tablename__ = "strategy_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), index=True)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    date_from: Mapped[str] = mapped_column(String(16))
    date_to: Mapped[str] = mapped_column(String(16))
    series_ticker: Mapped[str] = mapped_column(String(64), index=True)
    rooms_evaluated: Mapped[int] = mapped_column(Integer, default=0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    win_count: Mapped[int] = mapped_column(Integer, default=0)
    total_pnl_dollars: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"))
    trade_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    win_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    avg_edge_bps: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), nullable=True)


class CityStrategyAssignment(Base):
    __tablename__ = "city_strategy_assignments"

    series_ticker: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(64))
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    assigned_by: Mapped[str] = mapped_column(String(64), default="auto_regression")
