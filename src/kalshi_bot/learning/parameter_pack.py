from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


ParameterValue = bool | int | float | str


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    name: str
    default: ParameterValue
    min_value: ParameterValue | None = None
    max_value: ParameterValue | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "default": self.default,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class ParameterPack:
    version: str
    status: str
    parameters: dict[str, ParameterValue]
    specs: dict[str, ParameterSpec]
    parent_version: str | None = None
    source: str = "builtin"
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def pack_hash(self) -> str:
        return parameter_pack_hash(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "parent_version": self.parent_version,
            "source": self.source,
            "description": self.description,
            "parameters": dict(sorted(self.parameters.items())),
            "specs": {name: spec.to_dict() for name, spec in sorted(self.specs.items())},
            "metadata": dict(sorted(self.metadata.items())),
        }


DEFAULT_PARAMETER_SPECS: dict[str, ParameterSpec] = {
    "pseudo_count": ParameterSpec("pseudo_count", 8, 2, 32, "Climatology shrinkage pseudo-count."),
    "gumbel_weight": ParameterSpec("gumbel_weight", 0.5, 0.0, 1.0, "Closed-form Gumbel blend weight."),
    "kde_weight": ParameterSpec("kde_weight", 0.5, 0.0, 1.0, "Ensemble KDE blend weight."),
    "boundary_threshold": ParameterSpec("boundary_threshold", 0.25, 0.05, 0.75, "Boundary mass threshold."),
    "disagreement_threshold": ParameterSpec("disagreement_threshold", 0.85, 0.10, 1.0, "Source disagreement threshold."),
    "base_min_ev": ParameterSpec("base_min_ev", 0.02, 0.0, 0.20, "Base minimum EV dollars per contract."),
    "uncertainty_min_ev_buffer": ParameterSpec(
        "uncertainty_min_ev_buffer",
        0.02,
        0.0,
        0.20,
        "Additional EV buffer at max uncertainty.",
    ),
    "uncertainty_size_taper": ParameterSpec("uncertainty_size_taper", 0.60, 0.0, 1.0, "Uncertainty size taper."),
    "kelly_fraction": ParameterSpec("kelly_fraction", 0.25, 0.01, 0.50, "Fractional Kelly multiplier."),
    "survival_kelly": ParameterSpec("survival_kelly", 0.10, 0.01, 0.25, "Survival-mode Kelly multiplier."),
    "survival_ev_buffer": ParameterSpec("survival_ev_buffer", 0.03, 0.0, 0.20, "Survival-mode EV buffer."),
    "health_degraded_size_mult": ParameterSpec(
        "health_degraded_size_mult",
        0.5,
        0.0,
        1.0,
        "Size multiplier when aggregate source health is DEGRADED.",
    ),
    "catboost_blend_weight": ParameterSpec(
        "catboost_blend_weight",
        0.0,
        0.0,
        0.5,
        "Optional learned-head probability blend weight.",
    ),
}


HARD_CAP_PARAMETER_NAMES = {
    "max_position_pct",
    "max_total_exposure_pct",
    "daily_max_loss_pct",
    "max_drawdown_pct",
    "max_position_usd",
    "risk_max_order_notional_dollars",
    "risk_max_position_notional_dollars",
}


def default_parameter_pack(*, version: str = "builtin-parameters-v1") -> ParameterPack:
    return ParameterPack(
        version=version,
        status="champion",
        source="builtin",
        description="Built-in deterministic parameter defaults.",
        parameters={name: spec.default for name, spec in DEFAULT_PARAMETER_SPECS.items()},
        specs=dict(DEFAULT_PARAMETER_SPECS),
        metadata={"hard_caps_excluded": sorted(HARD_CAP_PARAMETER_NAMES)},
    )


def sanitize_parameter_pack(
    pack: ParameterPack,
    *,
    specs: dict[str, ParameterSpec] | None = None,
) -> ParameterPack:
    active_specs = specs or pack.specs or DEFAULT_PARAMETER_SPECS
    sanitized: dict[str, ParameterValue] = {}
    for name, spec in active_specs.items():
        raw_value = pack.parameters.get(name, spec.default)
        sanitized[name] = _sanitize_value(raw_value, spec)
    forbidden = sorted(set(pack.parameters) & HARD_CAP_PARAMETER_NAMES)
    metadata = dict(pack.metadata)
    if forbidden:
        metadata["dropped_hard_cap_parameters"] = forbidden
    return ParameterPack(
        version=pack.version,
        status=pack.status,
        parent_version=pack.parent_version,
        source=pack.source,
        description=pack.description,
        parameters=sanitized,
        specs=dict(active_specs),
        metadata=metadata,
    )


def parameter_pack_from_dict(payload: dict[str, Any]) -> ParameterPack:
    specs_payload = dict(payload.get("specs") or {})
    specs = {
        name: ParameterSpec(
            name=str(spec.get("name") or name),
            default=spec.get("default"),
            min_value=spec.get("min_value"),
            max_value=spec.get("max_value"),
            description=str(spec.get("description") or ""),
        )
        for name, spec in specs_payload.items()
    }
    if not specs:
        specs = dict(DEFAULT_PARAMETER_SPECS)
    return ParameterPack(
        version=str(payload["version"]),
        status=str(payload.get("status") or "candidate"),
        parent_version=payload.get("parent_version"),
        source=str(payload.get("source") or "imported"),
        description=str(payload.get("description") or ""),
        parameters=dict(payload.get("parameters") or {}),
        specs=specs,
        metadata=dict(payload.get("metadata") or {}),
    )


def parameter_pack_hash(pack: ParameterPack) -> str:
    payload = canonical_parameter_payload(pack)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_parameter_payload(pack: ParameterPack) -> dict[str, Any]:
    sanitized = sanitize_parameter_pack(pack)
    return {
        "parameters": dict(sorted(sanitized.parameters.items())),
        "specs": {name: spec.to_dict() for name, spec in sorted(sanitized.specs.items())},
    }


def _sanitize_value(value: ParameterValue, spec: ParameterSpec) -> ParameterValue:
    if isinstance(spec.default, bool):
        return bool(value)
    if isinstance(spec.default, int) and not isinstance(spec.default, bool):
        numeric = int(round(float(value)))
        return int(_clamp(numeric, spec.min_value, spec.max_value))
    if isinstance(spec.default, float):
        numeric = float(value)
        return float(_clamp(numeric, spec.min_value, spec.max_value))
    return str(value)


def _clamp(value: float | int, min_value: ParameterValue | None, max_value: ParameterValue | None) -> float | int:
    result = value
    if min_value is not None:
        result = max(result, float(min_value))
    if max_value is not None:
        result = min(result, float(max_value))
    return result
