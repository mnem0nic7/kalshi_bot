from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


TRACE_SCHEMA_VERSION = "decision_trace.v1"
DETERMINISTIC_PATH_VERSION = "deterministic-fast-path.v1"


def normalize_for_trace(value: Any) -> Any:
    """Convert trace inputs into stable JSON-compatible values."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return normalize_for_trace(asdict(value))
    if hasattr(value, "model_dump"):
        return normalize_for_trace(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(k): normalize_for_trace(value[k]) for k in sorted(value.keys(), key=str)}
    if isinstance(value, (list, tuple, set)):
        return [normalize_for_trace(item) for item in value]
    if value.__class__.__module__.startswith("numpy") and hasattr(value, "item"):
        return normalize_for_trace(value.item())
    if value.__class__.__module__ == "types" and value.__class__.__name__ == "SimpleNamespace":
        return normalize_for_trace(vars(value))
    return value


def stable_json(payload: Any) -> str:
    return json.dumps(normalize_for_trace(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()


def _object_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        return normalize_for_trace(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return normalize_for_trace(value)
    return normalize_for_trace(vars(value))


def _threshold_dict(thresholds: Any) -> dict[str, Any]:
    if is_dataclass(thresholds):
        return normalize_for_trace(asdict(thresholds))
    return {
        key: normalize_for_trace(value)
        for key, value in vars(thresholds).items()
        if not key.startswith("_")
    }


def _signal_dict(signal: Any) -> dict[str, Any]:
    eligibility = getattr(signal, "eligibility", None)
    weather = getattr(signal, "weather", None)
    return normalize_for_trace(
        {
            "fair_yes_dollars": getattr(signal, "fair_yes_dollars", None),
            "edge_bps": getattr(signal, "edge_bps", None),
            "confidence": getattr(signal, "confidence", None),
            "recommended_action": getattr(signal, "recommended_action", None),
            "recommended_side": getattr(signal, "recommended_side", None),
            "target_yes_price_dollars": getattr(signal, "target_yes_price_dollars", None),
            "capital_bucket": getattr(signal, "capital_bucket", None),
            "trade_regime": getattr(signal, "trade_regime", None),
            "strategy_mode": getattr(signal, "strategy_mode", None),
            "resolution_state": getattr(signal, "resolution_state", None),
            "stand_down_reason": getattr(signal, "stand_down_reason", None),
            "evaluation_outcome": getattr(signal, "evaluation_outcome", None),
            "size_factor": getattr(signal, "size_factor", None),
            "recommended_size_cap_fp": getattr(signal, "recommended_size_cap_fp", None),
            "summary": getattr(signal, "summary", None),
            "eligibility": eligibility,
            "weather": weather,
        }
    )


def _ticket_record_dict(ticket_record: Any) -> dict[str, Any] | None:
    if ticket_record is None:
        return None
    return normalize_for_trace(
        {
            "id": ticket_record.id,
            "market_ticker": ticket_record.market_ticker,
            "action": ticket_record.action,
            "side": ticket_record.side,
            "yes_price_dollars": ticket_record.yes_price_dollars,
            "count_fp": ticket_record.count_fp,
            "time_in_force": ticket_record.time_in_force,
            "client_order_id": ticket_record.client_order_id,
            "status": ticket_record.status,
            "strategy_code": ticket_record.strategy_code,
            "payload": ticket_record.payload,
        }
    )


def _risk_record_dict(risk_verdict_record: Any) -> dict[str, Any] | None:
    if risk_verdict_record is None:
        return None
    return normalize_for_trace(
        {
            "id": risk_verdict_record.id,
            "status": risk_verdict_record.status,
            "reasons": risk_verdict_record.reasons,
            "approved_notional_dollars": risk_verdict_record.approved_notional_dollars,
            "approved_count_fp": risk_verdict_record.approved_count_fp,
            "payload": risk_verdict_record.payload,
        }
    )


def infer_decision_kind(
    *,
    ticket_record: Any | None,
    final_status: str,
    evaluation_outcome: str,
) -> str:
    if evaluation_outcome == "risk_blocked" or final_status == "blocked":
        return "risk_block"
    if ticket_record is not None:
        return "entry"
    return "stand_down"


def normalized_intent_from_trace(trace: Mapping[str, Any]) -> dict[str, Any]:
    signal = trace.get("signal") if isinstance(trace.get("signal"), Mapping) else {}
    ticket = trace.get("ticket") if isinstance(trace.get("ticket"), Mapping) else {}
    risk = trace.get("risk") if isinstance(trace.get("risk"), Mapping) else {}
    execution = trace.get("execution") if isinstance(trace.get("execution"), Mapping) else {}
    candidate = trace.get("candidate_trace") if isinstance(trace.get("candidate_trace"), Mapping) else {}
    eligibility = signal.get("eligibility") if isinstance(signal.get("eligibility"), Mapping) else {}
    return normalize_for_trace(
        {
            "schema_version": trace.get("schema_version"),
            "path_version": trace.get("path_version"),
            "decision_kind": trace.get("decision_kind"),
            "market_ticker": trace.get("market_ticker"),
            "kalshi_env": trace.get("kalshi_env"),
            "final_status": trace.get("final_status"),
            "evaluation_outcome": trace.get("evaluation_outcome"),
            "recommended_action": signal.get("recommended_action"),
            "recommended_side": signal.get("recommended_side"),
            "target_yes_price_dollars": signal.get("target_yes_price_dollars"),
            "candidate_selected_side": candidate.get("selected_side"),
            "candidate_outcome": candidate.get("outcome"),
            "eligibility_outcome": candidate.get("eligibility_outcome") or eligibility.get("evaluation_outcome"),
            "stand_down_reason": candidate.get("eligibility_stand_down_reason") or eligibility.get("stand_down_reason"),
            "ticket_action": ticket.get("action"),
            "ticket_side": ticket.get("side"),
            "ticket_yes_price_dollars": ticket.get("yes_price_dollars"),
            "ticket_count_fp": ticket.get("count_fp"),
            "risk_status": risk.get("status"),
            "risk_reasons": risk.get("reasons"),
            "approved_notional_dollars": risk.get("approved_notional_dollars"),
            "approved_count_fp": risk.get("approved_count_fp"),
            "execution_status": execution.get("status"),
        }
    )


def build_deterministic_decision_trace(
    *,
    room: Any,
    signal: Any,
    thresholds: Any,
    candidate_trace: Mapping[str, Any],
    final_status: str,
    evaluation_outcome: str,
    ticket_record: Any | None = None,
    risk_verdict_record: Any | None = None,
    receipt: Any | None = None,
    market_observed_at: datetime | None = None,
    research_observed_at: datetime | None = None,
    source_snapshot_ids: Mapping[str, Any] | None = None,
    sizing: Mapping[str, Any] | None = None,
    loss_sensitivity_active: bool = False,
    parameter_pack_version: str | None = None,
    path_version: str = DETERMINISTIC_PATH_VERSION,
) -> tuple[str, str, dict[str, Any]]:
    signal_payload = _signal_dict(signal)
    inputs = normalize_for_trace(
        {
            "market_ticker": room.market_ticker,
            "kalshi_env": room.kalshi_env,
            "source_snapshot_ids": source_snapshot_ids or {},
            "market_observed_at": market_observed_at,
            "research_observed_at": research_observed_at,
            "thresholds": _threshold_dict(thresholds),
            "signal": signal_payload,
            "candidate_trace": candidate_trace,
            "sizing": sizing or {},
        }
    )
    input_hash = stable_hash(inputs)
    decision_kind = infer_decision_kind(
        ticket_record=ticket_record,
        final_status=final_status,
        evaluation_outcome=evaluation_outcome,
    )
    trace = normalize_for_trace(
        {
            "schema_version": TRACE_SCHEMA_VERSION,
            "path_version": path_version,
            "decision_kind": decision_kind,
            "room_id": room.id,
            "market_ticker": room.market_ticker,
            "kalshi_env": room.kalshi_env,
            "agent_pack_version": getattr(room, "agent_pack_version", None),
            "parameter_pack_version": parameter_pack_version,
            "source_snapshot_ids": source_snapshot_ids or {},
            "market_observed_at": market_observed_at,
            "research_observed_at": research_observed_at,
            "inputs": inputs,
            "signal": signal_payload,
            "candidate_trace": candidate_trace,
            "ticket": _ticket_record_dict(ticket_record),
            "risk": _risk_record_dict(risk_verdict_record),
            "execution": _object_dict(receipt),
            "sizing": sizing or {},
            "loss_sensitivity_active": loss_sensitivity_active,
            "final_status": final_status,
            "evaluation_outcome": evaluation_outcome,
        }
    )
    normalized_intent = normalized_intent_from_trace(trace)
    trace_hash = stable_hash(normalized_intent)
    trace["input_hash"] = input_hash
    trace["trace_hash"] = trace_hash
    trace["normalized_intent"] = normalized_intent
    return input_hash, trace_hash, trace


@dataclass(slots=True)
class DecisionTraceReplayResult:
    ok: bool
    expected_trace_hash: str | None
    actual_trace_hash: str
    expected_input_hash: str | None
    actual_input_hash: str
    mismatches: list[str]
    normalized_intent: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return normalize_for_trace(
            {
                "ok": self.ok,
                "expected_trace_hash": self.expected_trace_hash,
                "actual_trace_hash": self.actual_trace_hash,
                "expected_input_hash": self.expected_input_hash,
                "actual_input_hash": self.actual_input_hash,
                "mismatches": self.mismatches,
                "normalized_intent": self.normalized_intent,
            }
        )


def replay_decision_trace(trace: Mapping[str, Any], *, expected_trace_hash: str | None = None) -> DecisionTraceReplayResult:
    normalized_intent = normalized_intent_from_trace(trace)
    actual_trace_hash = stable_hash(normalized_intent)
    inputs = trace.get("inputs") if isinstance(trace.get("inputs"), Mapping) else {}
    actual_input_hash = stable_hash(inputs)
    expected_trace = expected_trace_hash or trace.get("trace_hash")
    expected_input = trace.get("input_hash")
    mismatches: list[str] = []
    if expected_trace != actual_trace_hash:
        mismatches.append("trace_hash")
    if expected_input != actual_input_hash:
        mismatches.append("input_hash")
    return DecisionTraceReplayResult(
        ok=not mismatches,
        expected_trace_hash=expected_trace if isinstance(expected_trace, str) else None,
        actual_trace_hash=actual_trace_hash,
        expected_input_hash=expected_input if isinstance(expected_input, str) else None,
        actual_input_hash=actual_input_hash,
        mismatches=mismatches,
        normalized_intent=normalized_intent,
    )


def decision_trace_record_to_dict(record: Any) -> dict[str, Any]:
    return normalize_for_trace(
        {
            "id": record.id,
            "room_id": record.room_id,
            "ticket_id": record.ticket_id,
            "market_ticker": record.market_ticker,
            "kalshi_env": record.kalshi_env,
            "decision_kind": record.decision_kind,
            "decision_time": record.decision_time,
            "path_version": record.path_version,
            "agent_pack_version": record.agent_pack_version,
            "parameter_pack_version": record.parameter_pack_version,
            "source_snapshot_ids": record.source_snapshot_ids,
            "input_hash": record.input_hash,
            "trace_hash": record.trace_hash,
            "trace": record.trace,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
    )
