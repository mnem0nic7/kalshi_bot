from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


DEFAULT_HARD_CAPS_PATH = Path("infra/config/hard_caps.yaml")
HARD_CAPS_SCHEMA_VERSION = "hard-caps-v1"


@dataclass(frozen=True, slots=True)
class HardCaps:
    schema_version: str
    hard_caps: dict[str, float | int | bool | str | None]
    description: str = ""
    operator_only: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def config_hash(self) -> str:
        return hard_caps_hash(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "description": self.description,
            "operator_only": self.operator_only,
            "hard_caps": dict(sorted(self.hard_caps.items())),
            "metadata": dict(sorted(self.metadata.items())),
        }


def load_hard_caps(path: Path | str = DEFAULT_HARD_CAPS_PATH) -> HardCaps:
    source_path = Path(path)
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{source_path} must contain a YAML object")
    caps = hard_caps_from_mapping(payload)
    validate_hard_caps(caps)
    return caps


def hard_caps_from_mapping(payload: Mapping[str, Any]) -> HardCaps:
    raw_caps = payload.get("hard_caps")
    if not isinstance(raw_caps, Mapping):
        raise ValueError("hard_caps must be an object")
    return HardCaps(
        schema_version=str(payload.get("schema_version") or ""),
        description=str(payload.get("description") or ""),
        operator_only=bool(payload.get("operator_only", True)),
        hard_caps={str(key): _normalize_cap_value(value) for key, value in raw_caps.items()},
        metadata=dict(payload.get("metadata") or {}),
    )


def validate_hard_caps(caps: HardCaps) -> None:
    if caps.schema_version != HARD_CAPS_SCHEMA_VERSION:
        raise ValueError(f"hard_caps schema_version must be {HARD_CAPS_SCHEMA_VERSION}")
    if not caps.operator_only:
        raise ValueError("hard_caps must be operator_only")
    required = {
        "max_position_pct",
        "max_total_exposure_pct",
        "daily_max_loss_pct",
        "max_drawdown_pct",
        "max_position_usd",
    }
    missing = sorted(required - set(caps.hard_caps))
    if missing:
        raise ValueError(f"hard_caps missing required fields: {', '.join(missing)}")
    for name, value in caps.hard_caps.items():
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"hard cap {name} must be numeric or null")
        if float(value) < 0:
            raise ValueError(f"hard cap {name} must be non-negative")


def hard_caps_hash(caps: HardCaps) -> str:
    encoded = json.dumps(
        {
            "schema_version": caps.schema_version,
            "operator_only": caps.operator_only,
            "hard_caps": dict(sorted(caps.hard_caps.items())),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_cap_value(value: Any) -> float | int | bool | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return stripped
        try:
            return float(stripped)
        except ValueError:
            return stripped
    return value
