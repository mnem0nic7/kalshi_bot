from __future__ import annotations

from enum import StrEnum


class AgentRole(StrEnum):
    RESEARCHER = "researcher"
    TRADER = "trader"
    PRESIDENT = "president"
    RISK_OFFICER = "risk_officer"
    EXECUTION_CLERK = "execution_clerk"
    OPS_MONITOR = "ops_monitor"
    AUDITOR = "auditor"
    MEMORY_LIBRARIAN = "memory_librarian"
    SUPERVISOR = "supervisor"
    SYSTEM = "system"


class MessageKind(StrEnum):
    OBSERVATION = "Observation"
    EVIDENCE_ARTIFACT = "EvidenceArtifact"
    POLICY_MEMO = "PolicyMemo"
    TRADE_IDEA = "TradeIdea"
    TRADE_TICKET = "TradeTicket"
    RISK_VERDICT = "RiskVerdict"
    EXEC_RECEIPT = "ExecReceipt"
    OPS_ALERT = "OpsAlert"
    MEMORY_NOTE = "MemoryNote"
    INCIDENT_ACTION = "IncidentAction"


class RoomStage(StrEnum):
    TRIGGERED = "triggered"
    RESEARCHING = "researching"
    POSTURE = "posture"
    PROPOSING = "proposing"
    RISK = "risk"
    EXECUTING = "executing"
    AUDITING = "auditing"
    MEMORY = "memory"
    COMPLETE = "complete"
    FAILED = "failed"


class TradeAction(StrEnum):
    BUY = "buy"
    SELL = "sell"


class ContractSide(StrEnum):
    YES = "yes"
    NO = "no"


class WeatherResolutionState(StrEnum):
    UNRESOLVED = "unresolved"
    LOCKED_YES = "locked_yes"
    LOCKED_NO = "locked_no"


class StrategyMode(StrEnum):
    DIRECTIONAL_UNRESOLVED = "directional_unresolved"
    LATE_DAY_AVOID = "late_day_avoid"
    RESOLVED_CLEANUP_CANDIDATE = "resolved_cleanup_candidate"
    RESOLUTION_CLEANUP = "resolution_cleanup"
    MONOTONICITY_ARB = "monotonicity_arb"


class StandDownReason(StrEnum):
    RESEARCH_STALE = "research_stale"
    MARKET_STALE = "market_stale"
    RESOLVED_CONTRACT = "resolved_contract"
    NO_ACTIONABLE_EDGE = "no_actionable_edge"
    INSUFFICIENT_EDGE_QUALITY = "insufficient_edge_quality"
    SPREAD_TOO_WIDE = "spread_too_wide"
    BOOK_EFFECTIVELY_BROKEN = "book_effectively_broken"
    INSUFFICIENT_REMAINING_PAYOUT = "insufficient_remaining_payout"
    MARKET_SPREAD_OVER_60PCT = "market_spread_over_60pct"
    NEGATIVE_MARKET_EDGE = "negative_market_edge"
    MOMENTUM_AGAINST_TRADE = "momentum_against_trade"
    VOLUME_TOO_LOW = "volume_too_low"
    LONGSHOT_BET = "longshot_bet"
    # Strategy C lock-confirmation gate failures
    STRATEGY_C_LOCK_UNCONFIRMED = "strategy_c_lock_unconfirmed"
    STRATEGY_C_FORECAST_RESIDUAL_EXCEEDED = "strategy_c_forecast_residual_exceeded"
    STRATEGY_C_BOOK_STALE = "strategy_c_book_stale"
    STRATEGY_C_CLI_VARIANCE = "strategy_c_cli_variance"
    # Signal-level stand-downs (before risk engine)
    FORECAST_UNAVAILABLE = "forecast_unavailable"
    FORECAST_SOURCE_DISAGREEMENT = "forecast_source_disagreement"
    FAIR_VALUE_SOURCE_DISQUALIFIED = "fair_value_source_disqualified"
    INSUFFICIENT_FORECAST_SEPARATION = "insufficient_forecast_separation"
    CONFIDENCE_TOO_LOW = "confidence_too_low"
    DOSSIER_STALE = "dossier_stale"
    FORECAST_DELTA_MISSING = "forecast_delta_missing"
    EXTREME_EDGE_DIAGNOSTIC_FAILED = "extreme_edge_diagnostic_failed"
    SELECTED_SIDE_UNMARKETABLE = "selected_side_unmarketable"
    CONTRACT_PRICE_TOO_LOW = "contract_price_too_low"
    ENTRY_FREEZE = "entry_freeze"
    EMPIRICAL_GATE_BLOCK = "empirical_gate_block"
    STATIC_SIGNAL_STALE = "static_signal_stale"
    CRYPTO_DISABLED = "crypto_disabled"
    CRYPTO_MODEL_UNAVAILABLE = "crypto_model_unavailable"
    CRYPTO_DATA_INSUFFICIENT = "crypto_data_insufficient"
    CRYPTO_REPLAY_GATE_NOT_PASSED = "crypto_replay_gate_not_passed"


class RiskStatus(StrEnum):
    APPROVED = "approved"
    BLOCKED = "blocked"
    REVIEW = "review"


class DeploymentColor(StrEnum):
    BLUE = "blue"
    GREEN = "green"


class RoomOrigin(StrEnum):
    SHADOW = "shadow"
    LIVE = "live"
    HISTORICAL_REPLAY = "historical_replay"


class StrategyCode(StrEnum):
    DIRECTIONAL = "A"  # Strategy A — directional weather taker
    CLEANUP = "C"  # Strategy C — resolution-lag cleanup
    MONOTONICITY_ARB = "ARB"  # Monotonicity arbitrage scanner
    CRYPTO_15M = "CRYPTO_15M"  # Kalshi 15-minute crypto microstructure strategy
    UNMANAGED = "UNMANAGED"  # Exchange-side/manual fills with no bot decision linkage
