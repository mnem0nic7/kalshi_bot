from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from kalshi_bot.learning.hard_caps import HardCaps


@dataclass(frozen=True, slots=True)
class HoldoutMetrics:
    coverage: float
    brier: float
    ece: float
    sharpe: float
    max_drawdown: float
    city_win_rates: dict[str, float] = field(default_factory=dict)
    hard_cap_touches: int = 0
    pack_hash: str | None = None
    rerun_pack_hash: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "HoldoutMetrics":
        return cls(
            coverage=float(payload.get("coverage", 0.0)),
            brier=float(payload.get("brier", 1.0)),
            ece=float(payload.get("ece", 1.0)),
            sharpe=float(payload.get("sharpe", 0.0)),
            max_drawdown=float(payload.get("max_drawdown", 1.0)),
            city_win_rates={str(k): float(v) for k, v in dict(payload.get("city_win_rates") or {}).items()},
            hard_cap_touches=int(payload.get("hard_cap_touches", 0)),
            pack_hash=payload.get("pack_hash"),
            rerun_pack_hash=payload.get("rerun_pack_hash"),
        )


@dataclass(frozen=True, slots=True)
class PromotionGateConfig:
    min_coverage: float = 0.95
    max_brier_ratio: float = 1.02
    max_ece: float = 0.06
    min_sharpe_ratio: float = 0.95
    max_drawdown_ratio: float = 1.10
    hard_max_drawdown: float = 1.0
    max_city_win_rate_drop: float = 0.10


@dataclass(frozen=True, slots=True)
class PromotionGateResult:
    passed: bool
    failures: list[str]
    comparisons: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failures": list(self.failures),
            "comparisons": dict(self.comparisons),
        }


def evaluate_parameter_pack_promotion(
    *,
    candidate: HoldoutMetrics,
    current: HoldoutMetrics,
    config: PromotionGateConfig | None = None,
) -> PromotionGateResult:
    cfg = config or PromotionGateConfig()
    failures: list[str] = []
    comparisons: dict[str, Any] = {}

    comparisons["coverage"] = {"candidate": candidate.coverage, "minimum": cfg.min_coverage}
    if candidate.coverage < cfg.min_coverage:
        failures.append("coverage_below_minimum")

    max_brier = current.brier * cfg.max_brier_ratio
    comparisons["brier"] = {"candidate": candidate.brier, "current": current.brier, "maximum": max_brier}
    if candidate.brier > max_brier:
        failures.append("brier_regression")

    comparisons["ece"] = {"candidate": candidate.ece, "maximum": cfg.max_ece}
    if candidate.ece > cfg.max_ece:
        failures.append("ece_above_maximum")

    min_sharpe = _minimum_allowed_sharpe(current.sharpe, cfg.min_sharpe_ratio)
    comparisons["sharpe"] = {"candidate": candidate.sharpe, "current": current.sharpe, "minimum": min_sharpe}
    if candidate.sharpe < min_sharpe:
        failures.append("sharpe_regression")

    max_drawdown = min(current.max_drawdown * cfg.max_drawdown_ratio, cfg.hard_max_drawdown)
    comparisons["max_drawdown"] = {
        "candidate": candidate.max_drawdown,
        "current": current.max_drawdown,
        "maximum": max_drawdown,
    }
    if candidate.max_drawdown > max_drawdown:
        failures.append("drawdown_regression")

    city_drops = _city_win_rate_drops(candidate.city_win_rates, current.city_win_rates)
    comparisons["city_win_rate_drops"] = city_drops
    if any(drop > cfg.max_city_win_rate_drop for drop in city_drops.values()):
        failures.append("city_win_rate_regression")

    comparisons["hard_cap_touches"] = candidate.hard_cap_touches
    if candidate.hard_cap_touches != 0:
        failures.append("hard_cap_touch")

    comparisons["idempotent_hash"] = {
        "pack_hash": candidate.pack_hash,
        "rerun_pack_hash": candidate.rerun_pack_hash,
    }
    if not candidate.pack_hash or candidate.pack_hash != candidate.rerun_pack_hash:
        failures.append("pack_hash_not_idempotent")

    return PromotionGateResult(
        passed=not failures,
        failures=failures,
        comparisons=comparisons,
    )


def promotion_gate_config_from_hard_caps(
    hard_caps: HardCaps,
    *,
    base: PromotionGateConfig | None = None,
) -> PromotionGateConfig:
    max_drawdown = hard_caps.hard_caps.get("max_drawdown_pct")
    if max_drawdown is None:
        raise ValueError("hard_caps missing max_drawdown_pct")
    return replace(base or PromotionGateConfig(), hard_max_drawdown=float(max_drawdown))


def _minimum_allowed_sharpe(current_sharpe: float, ratio: float) -> float:
    if current_sharpe > 0:
        return current_sharpe * ratio
    return current_sharpe


def _city_win_rate_drops(candidate: dict[str, float], current: dict[str, float]) -> dict[str, float]:
    drops: dict[str, float] = {}
    for city, current_rate in current.items():
        if city not in candidate:
            continue
        drops[city] = max(0.0, current_rate - candidate[city])
    return drops
