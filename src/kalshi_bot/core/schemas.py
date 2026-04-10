from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from kalshi_bot.core.enums import AgentRole, ContractSide, MessageKind, RiskStatus, RoomStage, TradeAction
from kalshi_bot.core.fixed_point import quantize_count, quantize_price


class RoomCreate(BaseModel):
    name: str
    market_ticker: str
    prompt: str | None = None


class ObservationPayload(BaseModel):
    thesis: str
    evidence_ids: list[str] = Field(default_factory=list)
    fair_yes_dollars: Decimal | None = None
    edge_bps: int | None = None


class EvidenceArtifactPayload(BaseModel):
    source: str
    title: str
    url: str | None = None
    content: dict[str, Any] = Field(default_factory=dict)


class PolicyMemoPayload(BaseModel):
    posture: str
    capital_tone: str
    constraints: list[str] = Field(default_factory=list)


class TradeIdeaPayload(BaseModel):
    thesis: str
    target_market_ticker: str
    fair_yes_dollars: Decimal
    edge_bps: int


class TradeTicket(BaseModel):
    market_ticker: str
    action: TradeAction
    side: ContractSide
    yes_price_dollars: Decimal
    count_fp: Decimal
    time_in_force: str = "immediate_or_cancel"
    rationale_message_ids: list[str] = Field(default_factory=list)
    nonce: str = Field(default_factory=lambda: uuid4().hex[:12])
    note: str | None = None

    @field_validator("yes_price_dollars", mode="before")
    @classmethod
    def validate_price(cls, value: Any) -> Decimal:
        return quantize_price(value)

    @field_validator("count_fp", mode="before")
    @classmethod
    def validate_count(cls, value: Any) -> Decimal:
        return quantize_count(value)


class RiskVerdictPayload(BaseModel):
    status: RiskStatus
    reasons: list[str] = Field(default_factory=list)
    approved_notional_dollars: Decimal | None = None
    approved_count_fp: Decimal | None = None


class ExecReceiptPayload(BaseModel):
    status: str
    external_order_id: str | None = None
    client_order_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class OpsAlertPayload(BaseModel):
    severity: str
    summary: str
    details: dict[str, Any] = Field(default_factory=dict)


class MemoryNotePayload(BaseModel):
    title: str
    summary: str
    tags: list[str] = Field(default_factory=list)
    linked_message_ids: list[str] = Field(default_factory=list)


class ResearchSourceCard(BaseModel):
    source_key: str
    source_class: str
    trust_tier: str
    publisher: str
    title: str
    url: str | None = None
    snippet: str
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    content: dict[str, Any] = Field(default_factory=dict)


class ResearchClaim(BaseModel):
    source_key: str
    claim: str
    stance: str = "context"
    settlement_critical: bool = False
    freshness_seconds: int | None = None
    citations: list[str] = Field(default_factory=list)


class ResearchSummary(BaseModel):
    narrative: str
    bullish_case: str
    bearish_case: str
    unresolved_uncertainties: list[str] = Field(default_factory=list)
    settlement_mechanics: str
    current_numeric_facts: dict[str, Any] = Field(default_factory=dict)
    source_coverage: str
    research_confidence: float


class ResearchTraderContext(BaseModel):
    fair_yes_dollars: Decimal | None = None
    confidence: float = 0.0
    thesis: str
    source_keys: list[str] = Field(default_factory=list)
    numeric_facts: dict[str, Any] = Field(default_factory=dict)
    structured_source_used: bool = False
    web_source_used: bool = False
    autonomous_ready: bool = False

    @field_validator("fair_yes_dollars", mode="before")
    @classmethod
    def validate_research_price(cls, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return quantize_price(value)


class ResearchFreshness(BaseModel):
    refreshed_at: datetime
    expires_at: datetime
    stale: bool
    max_source_age_seconds: int = 0


class ResearchGateVerdict(BaseModel):
    passed: bool
    reasons: list[str] = Field(default_factory=list)
    cited_source_keys: list[str] = Field(default_factory=list)


class ResearchDelta(BaseModel):
    summary: str
    changed_fields: list[str] = Field(default_factory=list)
    numeric_fact_updates: dict[str, Any] = Field(default_factory=dict)
    source_keys: list[str] = Field(default_factory=list)


class ResearchDossier(BaseModel):
    market_ticker: str
    status: str
    mode: str
    summary: ResearchSummary
    freshness: ResearchFreshness
    trader_context: ResearchTraderContext
    gate: ResearchGateVerdict
    sources: list[ResearchSourceCard] = Field(default_factory=list)
    claims: list[ResearchClaim] = Field(default_factory=list)
    contradiction_count: int = 0
    unresolved_count: int = 0
    settlement_covered: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_run_id: str | None = None


class TrainingRoomOutcome(BaseModel):
    final_status: str
    room_stage: str
    shadow_mode: bool
    kill_switch_enabled: bool
    research_gate_passed: bool | None = None
    risk_status: str | None = None
    ticket_generated: bool = False
    orders_submitted: int = 0
    fills_observed: int = 0
    settlement_seen: bool = False
    settlement_pnl_dollars: Decimal | None = None

    @field_validator("settlement_pnl_dollars", mode="before")
    @classmethod
    def validate_settlement_price(cls, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value)).quantize(Decimal("0.0001"))


class TrainingRoomBundle(BaseModel):
    export_version: str = "room-bundle.v1"
    exported_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    room: dict[str, Any]
    messages: list[RoomMessageRead] = Field(default_factory=list)
    signal: dict[str, Any] | None = None
    research_dossier: dict[str, Any] | None = None
    research_delta: dict[str, Any] | None = None
    market_snapshot: dict[str, Any] | None = None
    weather_bundle: dict[str, Any] | None = None
    research_sources: list[dict[str, Any]] = Field(default_factory=list)
    trade_ticket: dict[str, Any] | None = None
    risk_verdict: dict[str, Any] | None = None
    orders: list[dict[str, Any]] = Field(default_factory=list)
    fills: list[dict[str, Any]] = Field(default_factory=list)
    memory_note: dict[str, Any] | None = None
    settlement: dict[str, Any] | None = None
    outcome: TrainingRoomOutcome


class RoleTrainingExample(BaseModel):
    export_version: str = "role-sft.v1"
    exported_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    room_id: str
    market_ticker: str
    role: str
    task: str
    messages: list[dict[str, str]] = Field(default_factory=list)
    input_context: dict[str, Any]
    target: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)


class RoomMessageCreate(BaseModel):
    role: AgentRole
    kind: MessageKind
    content: str
    payload: dict[str, Any] = Field(default_factory=dict)
    stage: RoomStage | None = None


class RoomMessageRead(BaseModel):
    id: str
    room_id: str
    role: AgentRole
    kind: MessageKind
    content: str
    payload: dict[str, Any]
    sequence: int
    stage: RoomStage | None = None
    created_at: datetime


class RoomState(BaseModel):
    id: str
    name: str
    market_ticker: str
    stage: RoomStage
    active_color: str
    shadow_mode: bool
    kill_switch_enabled: bool
    created_at: datetime
    updated_at: datetime


class TriggerRequest(BaseModel):
    reason: str = "manual"


class TradeDecisionContext(BaseModel):
    room_id: UUID
    market_ticker: str
    messages: list[RoomMessageRead] = Field(default_factory=list)
    latest_market_state: dict[str, Any] = Field(default_factory=dict)
    latest_weather_state: dict[str, Any] = Field(default_factory=dict)
    risk_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
