from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from kalshi_bot.core.enums import (
    AgentRole,
    ContractSide,
    MessageKind,
    RiskStatus,
    RoomOrigin,
    RoomStage,
    StandDownReason,
    StrategyMode,
    TradeAction,
    WeatherResolutionState,
)
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
    resolution_state: WeatherResolutionState = WeatherResolutionState.UNRESOLVED
    strategy_mode: StrategyMode = StrategyMode.DIRECTIONAL_UNRESOLVED
    heuristic_application: dict[str, Any] = Field(default_factory=dict)

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
    stale_grace: bool = False  # stale but within grace window; gate may still pass at reduced size


class ResearchGateVerdict(BaseModel):
    passed: bool
    reasons: list[str] = Field(default_factory=list)
    cited_source_keys: list[str] = Field(default_factory=list)
    stale_tolerance_active: bool = False  # passed via grace window; supervisor applies reduced notional cap


class TradeEligibilityVerdict(BaseModel):
    eligible: bool
    strategy_mode: StrategyMode = StrategyMode.DIRECTIONAL_UNRESOLVED
    resolution_state: WeatherResolutionState = WeatherResolutionState.UNRESOLVED
    stand_down_reason: StandDownReason | None = None
    reasons: list[str] = Field(default_factory=list)
    market_stale: bool = False
    research_stale: bool = False
    remaining_payout_dollars: Decimal | None = None
    market_spread_bps: int | None = None
    edge_after_quality_buffer_bps: int | None = None
    blocked_upstream: bool = False

    @field_validator("remaining_payout_dollars", mode="before")
    @classmethod
    def validate_remaining_payout(cls, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return quantize_price(value)


class ResearchDelta(BaseModel):
    summary: str
    changed_fields: list[str] = Field(default_factory=list)
    numeric_fact_updates: dict[str, Any] = Field(default_factory=dict)
    source_keys: list[str] = Field(default_factory=list)


class ResearchQualitySummary(BaseModel):
    citation_coverage_score: float = 0.0
    settlement_clarity_score: float = 0.0
    freshness_score: float = 0.0
    contradiction_score: float = 0.0
    structured_completeness_score: float = 0.0
    fair_value_score: float = 0.0
    overall_score: float = 0.0
    issues: list[str] = Field(default_factory=list)


class ResearchDossier(BaseModel):
    market_ticker: str
    status: str
    mode: str
    summary: ResearchSummary
    freshness: ResearchFreshness
    quality: ResearchQualitySummary = Field(default_factory=ResearchQualitySummary)
    trader_context: ResearchTraderContext
    gate: ResearchGateVerdict
    sources: list[ResearchSourceCard] = Field(default_factory=list)
    claims: list[ResearchClaim] = Field(default_factory=list)
    contradiction_count: int = 0
    unresolved_count: int = 0
    settlement_covered: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_run_id: str | None = None


class AgentRoleRuntime(BaseModel):
    provider: str = "gemini"
    model: str | None = None
    temperature: float = 0.2

    @field_validator("temperature")
    @classmethod
    def validate_temperature(cls, value: float) -> float:
        return max(0.0, min(1.0, float(value)))


class AgentPackRoleConfig(AgentRoleRuntime):
    system_prompt: str


class AgentPackResearchConfig(BaseModel):
    synthesis_system_prompt: str
    critique_system_prompt: str
    web_max_queries: int | None = None
    web_max_results: int | None = None


class AgentPackMemoryConfig(BaseModel):
    system_prompt: str
    max_sentences: int = 2


class AgentPackThresholds(BaseModel):
    risk_min_edge_bps: int | None = None
    risk_max_order_notional_dollars: float | None = None
    risk_max_position_notional_dollars: float | None = None
    trigger_max_spread_bps: int | None = None
    trigger_cooldown_seconds: int | None = None


class AgentPack(BaseModel):
    version: str
    status: str = "builtin"
    parent_version: str | None = None
    source: str = "builtin"
    description: str = ""
    roles: dict[str, AgentPackRoleConfig] = Field(default_factory=dict)
    research: AgentPackResearchConfig
    memory: AgentPackMemoryConfig
    thresholds: AgentPackThresholds = Field(default_factory=AgentPackThresholds)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SelfImproveCritiqueItem(BaseModel):
    room_id: str
    market_ticker: str
    research_quality: float = 0.0
    directional_agreement: float = 0.0
    risk_compliance: float = 0.0
    memory_usefulness: float = 0.0
    strengths: list[str] = Field(default_factory=list)
    weaknesses: list[str] = Field(default_factory=list)
    suggested_prompt_changes: dict[str, str] = Field(default_factory=dict)
    suggested_thresholds: AgentPackThresholds = Field(default_factory=AgentPackThresholds)


class EvaluationMetrics(BaseModel):
    composite_score: float = 0.0
    research_quality: float = 0.0
    directional_agreement: float = 0.0
    risk_compliance: float = 0.0
    memory_usefulness: float = 0.0
    invalid_payload_rate: float = 0.0
    gate_violation_count: int = 0
    safety_violation_count: int = 0
    settled_pnl_score: float | None = None
    sample_size: int = 0


class EvaluationSummary(BaseModel):
    candidate_version: str
    champion_version: str
    passed: bool = False
    improvement: float = 0.0
    max_critical_regression: float = 0.0
    candidate_metrics: EvaluationMetrics = Field(default_factory=EvaluationMetrics)
    champion_metrics: EvaluationMetrics = Field(default_factory=EvaluationMetrics)
    reasons: list[str] = Field(default_factory=list)


class TrainingRoomOutcome(BaseModel):
    final_status: str
    room_stage: str
    shadow_mode: bool
    kill_switch_enabled: bool
    research_gate_passed: bool | None = None
    risk_status: str | None = None
    resolution_state: str | None = None
    eligibility_passed: bool | None = None
    stand_down_reason: str | None = None
    blocked_by: str | None = None
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
    room_origin: str | None = None
    room: dict[str, Any]
    campaign: dict[str, Any] | None = None
    research_health: dict[str, Any] | None = None
    strategy_audit: dict[str, Any] | None = None
    audit_source: str | None = None
    audit_version: str | None = None
    trainable_default: bool | None = None
    exclude_reason: str | None = None
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
    historical_provenance: dict[str, Any] | None = None
    market_source_kind: str | None = None
    weather_source_kind: str | None = None
    coverage_class: str | None = None
    draft_only: bool = False
    replay_checkpoint_ts: datetime | None = None
    settlement: dict[str, Any] | None = None
    settlement_label: dict[str, Any] | None = None
    counterfactual_pnl_dollars: Decimal | None = None
    heuristic_pack_version: str | None = None
    intelligence_run_id: str | None = None
    candidate_pack_id: str | None = None
    rule_trace: list[dict[str, Any]] = Field(default_factory=list)
    support_window: dict[str, Any] | None = None
    heuristic_summary: str | None = None
    outcome: TrainingRoomOutcome

    @field_validator("counterfactual_pnl_dollars", mode="before")
    @classmethod
    def validate_counterfactual_pnl(cls, value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value)).quantize(Decimal("0.0001"))


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


class ResearchAuditIssue(BaseModel):
    market_ticker: str
    severity: str
    code: str
    summary: str
    details: dict[str, Any] = Field(default_factory=dict)


class StrategyAuditResult(BaseModel):
    room_id: str
    market_ticker: str
    thesis_correctness: str
    trade_quality: str
    block_correctness: str
    missed_stand_down: bool = False
    stale_data_mismatch: bool = False
    effective_freshness_agreement: bool = True
    resolution_state: str | None = None
    eligibility_passed: bool | None = None
    stand_down_reason: str | None = None
    blocked_by: str | None = None
    audit_source: str | None = None
    audit_version: str | None = None
    trainable_default: bool = True
    exclude_reason: str | None = None
    quality_warnings: list[str] = Field(default_factory=list)
    audited_at: datetime | None = None
    reasons: list[str] = Field(default_factory=list)


class StrategyAuditSummary(BaseModel):
    room_count: int = 0
    audited_room_count: int = 0
    forward_audit_count: int = 0
    backfilled_audit_count: int = 0
    stale_mismatch_count: int = 0
    low_upside_proposal_count: int = 0
    resolved_contract_proposal_count: int = 0
    missed_stand_down_count: int = 0
    cleaned_trainable_room_count: int = 0
    exclusion_reason_counts: dict[str, int] = Field(default_factory=dict)
    thesis_counts: dict[str, int] = Field(default_factory=dict)
    trade_quality_counts: dict[str, int] = Field(default_factory=dict)
    block_correctness_counts: dict[str, int] = Field(default_factory=dict)
    samples: list[StrategyAuditResult] = Field(default_factory=list)


class TrainingReadiness(BaseModel):
    complete_room_count: int = 0
    market_diversity_count: int = 0
    settled_room_count: int = 0
    trade_positive_room_count: int = 0
    ready_for_sft_export: bool = False
    ready_for_critique: bool = False
    ready_for_evaluation: bool = False
    ready_for_promotion: bool = False
    missing_indicators: list[str] = Field(default_factory=list)
    thresholds: dict[str, int] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)


class TrainingDatasetBuildSummary(BaseModel):
    id: str
    build_version: str
    mode: str
    status: str
    room_count: int = 0
    filters: dict[str, Any] = Field(default_factory=dict)
    label_stats: dict[str, Any] = Field(default_factory=dict)
    pack_versions: list[str] = Field(default_factory=list)
    created_at: datetime
    completed_at: datetime | None = None


class TrainingBuildRequest(BaseModel):
    mode: str = "room-bundles"
    limit: int = 200
    days: int = 30
    settled_only: bool = False
    include_non_complete: bool = False
    good_research_only: bool = False
    quality_cleaned_only: bool = True
    market_ticker: str | None = None
    origins: list[str] | None = None
    output: str | None = None


class HistoricalTrainingBuildRequest(BaseModel):
    mode: str = "bundles"
    limit: int = 1000
    date_from: str
    date_to: str
    series: list[str] = Field(default_factory=list)
    quality_cleaned_only: bool = True
    include_pathology_examples: bool = False
    require_full_checkpoints: bool = True
    late_only_ok: bool = False
    origins: list[str] = Field(default_factory=lambda: [RoomOrigin.HISTORICAL_REPLAY.value])
    output: str | None = None


class HeuristicThresholds(BaseModel):
    risk_min_edge_bps: int | None = None
    trigger_max_spread_bps: int | None = None
    strategy_quality_edge_buffer_bps: int | None = None
    strategy_min_remaining_payout_bps: int | None = None


class HeuristicCalibrationEntry(BaseModel):
    series_ticker: str | None = None
    city_bucket: str | None = None
    threshold_bucket: str | None = None
    daypart: str | None = None
    forecast_delta_bucket: str | None = None
    fair_yes_adjust_bps: int = 0
    support_count: int = 0
    mean_error_bps: float = 0.0


class HeuristicPolicyCondition(BaseModel):
    market_tickers: list[str] = Field(default_factory=list)
    series_tickers: list[str] = Field(default_factory=list)
    city_buckets: list[str] = Field(default_factory=list)
    dayparts: list[str] = Field(default_factory=list)
    threshold_buckets: list[str] = Field(default_factory=list)
    forecast_delta_buckets: list[str] = Field(default_factory=list)
    spread_regimes: list[str] = Field(default_factory=list)
    coverage_classes: list[str] = Field(default_factory=list)
    resolution_states: list[str] = Field(default_factory=list)
    market_stale_values: list[bool] = Field(default_factory=list)
    research_stale_values: list[bool] = Field(default_factory=list)


class HeuristicPolicyAction(BaseModel):
    fair_yes_adjust_bps: int | None = None
    risk_min_edge_bps: int | None = None
    trigger_max_spread_bps: int | None = None
    strategy_quality_edge_buffer_bps: int | None = None
    strategy_min_remaining_payout_bps: int | None = None
    recommended_strategy_mode: StrategyMode | None = None
    force_stand_down_reason: StandDownReason | None = None


class HeuristicPolicyNode(BaseModel):
    rule_id: str
    description: str
    priority: int = 100
    support_count: int = 0
    condition: HeuristicPolicyCondition = Field(default_factory=HeuristicPolicyCondition)
    action: HeuristicPolicyAction = Field(default_factory=HeuristicPolicyAction)


class HistoricalHeuristicPack(BaseModel):
    version: str
    status: str = "baseline"
    parent_version: str | None = None
    source: str = "historical_intelligence"
    description: str = ""
    thresholds: HeuristicThresholds = Field(default_factory=HeuristicThresholds)
    calibration_entries: list[HeuristicCalibrationEntry] = Field(default_factory=list)
    policy_graph: list[HeuristicPolicyNode] = Field(default_factory=list)
    agent_summary: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class HistoricalIntelligenceRunRequest(BaseModel):
    date_from: str
    date_to: str
    origins: list[str] = Field(default_factory=lambda: [RoomOrigin.HISTORICAL_REPLAY.value])
    auto_promote: bool = True


class HeuristicPackPromoteRequest(BaseModel):
    candidate_version: str | None = None
    reason: str = "manual_promote"


class HeuristicPackRollbackRequest(BaseModel):
    reason: str = "manual_rollback"


class HistoricalDateRangeRequest(BaseModel):
    date_from: str
    date_to: str
    series: list[str] = Field(default_factory=list)


class HistoricalReplayCheckpoint(BaseModel):
    checkpoint_label: str
    checkpoint_ts: datetime
    local_market_day: str
    timezone_name: str


class HistoricalProvenance(BaseModel):
    room_origin: RoomOrigin = RoomOrigin.HISTORICAL_REPLAY
    import_run_id: str | None = None
    replay_run_id: str | None = None
    local_market_day: str
    checkpoint_label: str
    checkpoint_ts: datetime
    timezone_name: str
    market_snapshot_source_id: str | None = None
    weather_snapshot_source_id: str | None = None
    market_source_kind: str | None = None
    weather_source_kind: str | None = None
    settlement_label_id: str | None = None
    coverage_class: str | None = None
    source_coverage: dict[str, Any] = Field(default_factory=dict)


class ShadowCampaignRequest(BaseModel):
    limit: int = 3
    reason: str = "shadow_campaign"


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


class ShadowRunRequest(BaseModel):
    reason: str = "shadow_run"
    name: str | None = None
    prompt: str | None = None


class SelfImprovePromoteRequest(BaseModel):
    evaluation_run_id: str
    reason: str = "auto_promote"


class SelfImproveRollbackRequest(BaseModel):
    reason: str = "manual_rollback"


class TradeDecisionContext(BaseModel):
    room_id: UUID
    market_ticker: str
    messages: list[RoomMessageRead] = Field(default_factory=list)
    latest_market_state: dict[str, Any] = Field(default_factory=dict)
    latest_weather_state: dict[str, Any] = Field(default_factory=dict)
    risk_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
