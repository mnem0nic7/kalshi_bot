from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from kalshi_bot.learning.hard_caps import HardCaps
from kalshi_bot.learning.parameter_pack import ParameterPack, default_parameter_pack, sanitize_parameter_pack
from kalshi_bot.learning.promotion_gates import (
    HoldoutMetrics,
    PromotionGateResult,
    evaluate_parameter_pack_promotion,
    promotion_gate_config_from_hard_caps,
)


@dataclass(frozen=True, slots=True)
class ParameterPackCandidateResult:
    index: int
    pack: ParameterPack
    holdout_report: dict[str, Any]
    objective: float
    gate: PromotionGateResult
    passed: bool
    failures: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "version": self.pack.version,
            "pack_hash": self.pack.pack_hash,
            "objective": self.objective,
            "passed": self.passed,
            "failures": list(self.failures),
            "pack": self.pack.to_dict(),
            "holdout_report": dict(self.holdout_report),
            "gate": self.gate.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ParameterPackSearchSelection:
    selected: ParameterPackCandidateResult | None
    evaluated: list[ParameterPackCandidateResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected": self.selected is not None,
            "selected_candidate": self.selected.to_dict() if self.selected is not None else None,
            "evaluated": [candidate.to_dict() for candidate in self.evaluated],
        }


def select_parameter_pack_candidate(
    *,
    search_payload: dict[str, Any] | list[dict[str, Any]],
    current_report: dict[str, Any],
    hard_caps: HardCaps,
    base_pack: ParameterPack | None = None,
) -> ParameterPackSearchSelection:
    """Select the first passing replay candidate without mutating runtime state."""

    base = base_pack or default_parameter_pack()
    current = HoldoutMetrics.from_dict(current_report)
    gate_config = promotion_gate_config_from_hard_caps(hard_caps)
    evaluated: list[ParameterPackCandidateResult] = []

    for index, candidate_payload in enumerate(_candidate_payloads(search_payload)):
        pack = _candidate_pack_from_payload(candidate_payload, index=index, base_pack=base)
        holdout_report = _candidate_holdout_report(candidate_payload, pack=pack)
        metrics = HoldoutMetrics.from_dict(holdout_report)
        gate = evaluate_parameter_pack_promotion(candidate=metrics, current=current, config=gate_config)
        failures = list(gate.failures)
        dropped_hard_caps = pack.metadata.get("dropped_hard_cap_parameters", [])
        if dropped_hard_caps:
            failures.append("candidate_contains_hard_cap_parameters")
        passed = not failures
        evaluated.append(
            ParameterPackCandidateResult(
                index=index,
                pack=pack,
                holdout_report=holdout_report,
                objective=parameter_pack_objective(metrics, hard_max_drawdown=gate_config.hard_max_drawdown),
                gate=gate,
                passed=passed,
                failures=failures,
            )
        )

    selected = next((candidate for candidate in evaluated if candidate.passed), None)
    return ParameterPackSearchSelection(selected=selected, evaluated=evaluated)


def parameter_pack_objective(metrics: HoldoutMetrics, *, hard_max_drawdown: float | None = None) -> float:
    objective = 0.5 * metrics.sharpe - 0.3 * metrics.brier - 0.2 * metrics.ece
    if metrics.hard_cap_touches:
        objective -= 1_000.0
    if hard_max_drawdown is not None and metrics.max_drawdown > hard_max_drawdown:
        objective -= 1_000.0
    return objective


def _candidate_payloads(search_payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(search_payload, list):
        candidates = search_payload
    else:
        candidates = search_payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("parameter-pack search payload must include a non-empty candidates list")
    if not all(isinstance(candidate, dict) for candidate in candidates):
        raise ValueError("each parameter-pack candidate must be an object")
    return candidates


def _candidate_pack_from_payload(
    payload: dict[str, Any],
    *,
    index: int,
    base_pack: ParameterPack,
) -> ParameterPack:
    pack_payload = dict(payload.get("pack") or {})
    parameters = dict(pack_payload.get("parameters") or payload.get("parameters") or {})
    raw_version = pack_payload.get("version") or payload.get("version")
    pack = ParameterPack(
        version=str(raw_version or "candidate-pending"),
        status=str(pack_payload.get("status") or payload.get("status") or "candidate"),
        parent_version=pack_payload.get("parent_version") or payload.get("parent_version") or base_pack.version,
        source=str(pack_payload.get("source") or payload.get("source") or "parameter-search"),
        description=str(pack_payload.get("description") or payload.get("description") or "Replay-selected parameter candidate."),
        parameters={**base_pack.parameters, **parameters},
        specs=base_pack.specs,
        metadata={**dict(pack_payload.get("metadata") or {}), **dict(payload.get("metadata") or {})},
    )
    sanitized = sanitize_parameter_pack(pack, specs=base_pack.specs)
    if raw_version:
        return sanitized
    return replace(sanitized, version=f"candidate-{index + 1}-{sanitized.pack_hash[:12]}")


def _candidate_holdout_report(payload: dict[str, Any], *, pack: ParameterPack) -> dict[str, Any]:
    report = dict(payload.get("holdout_report") or payload.get("report") or {})
    report.setdefault("pack_hash", pack.pack_hash)
    report.setdefault("rerun_pack_hash", pack.pack_hash)
    return report
