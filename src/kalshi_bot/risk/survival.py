from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SurvivalConfig:
    starting_balance: float
    survival_threshold_ratio: float = 0.25
    standard_kelly: float = 0.25
    survival_kelly: float = 0.10
    survival_ev_buffer: float = 0.03

    @property
    def survival_threshold(self) -> float:
        return max(0.0, self.starting_balance * self.survival_threshold_ratio)


@dataclass(frozen=True, slots=True)
class SurvivalState:
    active: bool
    kelly_fraction: float
    dynamic_min_ev: float
    survival_threshold: float

    def to_dict(self) -> dict[str, float | bool]:
        return {
            "active": self.active,
            "kelly_fraction": self.kelly_fraction,
            "dynamic_min_ev": self.dynamic_min_ev,
            "survival_threshold": self.survival_threshold,
        }


def apply_survival_mode(
    *,
    balance: float,
    dynamic_min_ev: float,
    config: SurvivalConfig,
) -> SurvivalState:
    if config.starting_balance <= 0:
        raise ValueError("starting_balance must be positive")
    threshold = config.survival_threshold
    active = balance < threshold
    return SurvivalState(
        active=active,
        kelly_fraction=config.survival_kelly if active else config.standard_kelly,
        dynamic_min_ev=dynamic_min_ev + (config.survival_ev_buffer if active else 0.0),
        survival_threshold=threshold,
    )
