from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DriftWindow:
    rolling_7d_brier: float
    trailing_30d_brier: float
    rolling_ece: float
    predicted_win_rate: float
    realized_win_rate: float
    trade_count: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DriftWindow":
        return cls(
            rolling_7d_brier=float(payload.get("rolling_7d_brier", 0.0)),
            trailing_30d_brier=float(payload.get("trailing_30d_brier", 0.0)),
            rolling_ece=float(payload.get("rolling_ece", 0.0)),
            predicted_win_rate=float(payload.get("predicted_win_rate", 0.0)),
            realized_win_rate=float(payload.get("realized_win_rate", 0.0)),
            trade_count=int(payload.get("trade_count", 0)),
        )


@dataclass(frozen=True, slots=True)
class DriftDecision:
    pause_new_entries: bool
    trigger_pack_search: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pause_new_entries": self.pause_new_entries,
            "trigger_pack_search": self.trigger_pack_search,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class DriftWatcherConfig:
    brier_relative_pause: float = 1.15
    max_ece: float = 0.08
    max_win_rate_divergence: float = 0.05
    min_win_rate_trades: int = 100


def evaluate_calibration_drift(
    window: DriftWindow,
    *,
    config: DriftWatcherConfig | None = None,
) -> DriftDecision:
    cfg = config or DriftWatcherConfig()
    reasons: list[str] = []

    if window.trailing_30d_brier > 0 and window.rolling_7d_brier > window.trailing_30d_brier * cfg.brier_relative_pause:
        reasons.append("brier_relative_drift")
    if window.rolling_ece > cfg.max_ece:
        reasons.append("ece_above_limit")
    if window.trade_count >= cfg.min_win_rate_trades:
        divergence = abs(window.realized_win_rate - window.predicted_win_rate)
        if divergence > cfg.max_win_rate_divergence:
            reasons.append("win_rate_divergence")

    return DriftDecision(
        pause_new_entries=bool(reasons),
        trigger_pack_search=bool(reasons),
        reasons=reasons,
    )
