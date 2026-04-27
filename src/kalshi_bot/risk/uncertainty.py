from __future__ import annotations

from dataclasses import dataclass


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


@dataclass(frozen=True, slots=True)
class UncertaintyConfig:
    boundary_threshold: float = 0.25
    disagreement_threshold: float = 0.85
    boundary_weight: float = 0.60
    disagreement_weight: float = 0.40
    base_min_ev: float = 0.02
    uncertainty_min_ev_buffer: float = 0.02
    uncertainty_size_taper: float = 0.60
    size_floor: float = 0.35


@dataclass(frozen=True, slots=True)
class UncertaintyResult:
    uncertainty_score: float
    boundary_component: float
    disagreement_component: float
    dynamic_min_ev: float
    size_mult: float

    def to_dict(self) -> dict[str, float]:
        return {
            "uncertainty_score": self.uncertainty_score,
            "boundary_component": self.boundary_component,
            "disagreement_component": self.disagreement_component,
            "dynamic_min_ev": self.dynamic_min_ev,
            "size_mult": self.size_mult,
        }


def score_uncertainty(
    *,
    boundary_mass: float,
    disagreement: float,
    config: UncertaintyConfig | None = None,
) -> UncertaintyResult:
    cfg = config or UncertaintyConfig()
    if cfg.boundary_threshold <= 0 or cfg.disagreement_threshold <= 0:
        raise ValueError("uncertainty thresholds must be positive")
    total_weight = cfg.boundary_weight + cfg.disagreement_weight
    if total_weight <= 0:
        raise ValueError("uncertainty weights must sum positive")

    boundary_component = clamp(boundary_mass / cfg.boundary_threshold)
    disagreement_component = clamp(disagreement / cfg.disagreement_threshold)
    score = (
        (cfg.boundary_weight * boundary_component)
        + (cfg.disagreement_weight * disagreement_component)
    ) / total_weight
    dynamic_min_ev = cfg.base_min_ev + score * cfg.uncertainty_min_ev_buffer
    size_mult = max(cfg.size_floor, 1.0 - cfg.uncertainty_size_taper * score)
    return UncertaintyResult(
        uncertainty_score=score,
        boundary_component=boundary_component,
        disagreement_component=disagreement_component,
        dynamic_min_ev=dynamic_min_ev,
        size_mult=size_mult,
    )
