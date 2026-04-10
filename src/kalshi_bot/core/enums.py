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


class RiskStatus(StrEnum):
    APPROVED = "approved"
    BLOCKED = "blocked"
    REVIEW = "review"


class DeploymentColor(StrEnum):
    BLUE = "blue"
    GREEN = "green"

