"""ThresholdProximityMonitor — adaptive polling cadence for Strategy C (§4.1.3 / §4.1.4).

classify_proximity is the stateless core: given current temp, threshold list, and a
peak-confirmed flag, it returns the proximity tier. ThresholdProximityMonitor wraps it with
per-station peak state so the caller only needs to call update() each polling cycle.

Tier priority (highest to lowest urgency):
  near_threshold → post_peak → approach → idle
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

ProximityTier = Literal["idle", "approach", "near_threshold", "post_peak"]

_PEAK_COOLING_GAP_F = 1.0  # temp must drop this far below station max to confirm peak


def classify_proximity(
    current_temp_f: float,
    thresholds_f: list[float],
    *,
    peak_confirmed: bool = False,
    near_threshold_margin_f: float = 2.0,
    approach_margin_f: float = 5.0,
) -> ProximityTier:
    """Return the proximity tier for a station given current temperature.

    Args:
        current_temp_f: Latest ASOS observed temperature.
        thresholds_f: Settlement thresholds for all open contracts at this station.
        peak_confirmed: True when the station has confirmed its diurnal peak and is cooling.
        near_threshold_margin_f: Distance threshold for near_threshold tier (°F).
        approach_margin_f: Distance threshold for approach tier (°F).

    Returns:
        ProximityTier string; determines polling cadence.
    """
    if not thresholds_f:
        return "idle"

    min_distance = min(abs(current_temp_f - t) for t in thresholds_f)

    if min_distance <= near_threshold_margin_f:
        return "near_threshold"
    if peak_confirmed:
        return "post_peak"
    if min_distance <= approach_margin_f:
        return "approach"
    return "idle"


@dataclass
class _StationPeakState:
    max_temp_f: float | None = None
    peak_confirmed: bool = False
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    def observe(self, current_temp_f: float) -> None:
        if self.max_temp_f is None:
            self.max_temp_f = current_temp_f
        elif current_temp_f > self.max_temp_f:
            self.max_temp_f = current_temp_f
            self.peak_confirmed = False
        elif current_temp_f < self.max_temp_f - _PEAK_COOLING_GAP_F:
            self.peak_confirmed = True
        self.last_updated = datetime.now(UTC)


class ThresholdProximityMonitor:
    """Tracks per-station diurnal peak state and emits proximity tier + cadence.

    One instance is shared across all stations for a given trading day. Call
    reset_station() at the start of each new market day to clear peak history.
    """

    def __init__(
        self,
        *,
        near_threshold_margin_f: float = 2.0,
        approach_margin_f: float = 5.0,
        cadence_idle_seconds: int = 3600,
        cadence_approach_seconds: int = 900,
        cadence_near_threshold_seconds: int = 150,
        cadence_post_peak_seconds: int = 900,
    ) -> None:
        self._near_margin = near_threshold_margin_f
        self._approach_margin = approach_margin_f
        self._cadence: dict[ProximityTier, int] = {
            "idle": cadence_idle_seconds,
            "approach": cadence_approach_seconds,
            "near_threshold": cadence_near_threshold_seconds,
            "post_peak": cadence_post_peak_seconds,
        }
        self._states: dict[str, _StationPeakState] = {}

    @classmethod
    def from_settings(cls, settings: object) -> "ThresholdProximityMonitor":
        return cls(
            near_threshold_margin_f=getattr(settings, "strategy_c_near_threshold_margin_f", 2.0),
            approach_margin_f=getattr(settings, "strategy_c_approach_margin_f", 5.0),
            cadence_idle_seconds=getattr(settings, "strategy_c_cadence_idle_seconds", 3600),
            cadence_approach_seconds=getattr(settings, "strategy_c_cadence_approach_seconds", 900),
            cadence_near_threshold_seconds=getattr(settings, "strategy_c_cadence_near_threshold_seconds", 150),
            cadence_post_peak_seconds=getattr(settings, "strategy_c_cadence_post_peak_seconds", 900),
        )

    def update(
        self,
        station: str,
        current_temp_f: float,
        thresholds_f: list[float],
    ) -> tuple[ProximityTier, int]:
        """Observe a new temperature reading and return (tier, cadence_seconds).

        Args:
            station: Station identifier (e.g. "KBOS").
            current_temp_f: Current ASOS observed temperature.
            thresholds_f: Settlement thresholds for all open contracts at this station.

        Returns:
            (tier, cadence_seconds) — cadence is how long to wait before the next poll.
        """
        state = self._states.setdefault(station, _StationPeakState())
        state.observe(current_temp_f)

        tier = classify_proximity(
            current_temp_f,
            thresholds_f,
            peak_confirmed=state.peak_confirmed,
            near_threshold_margin_f=self._near_margin,
            approach_margin_f=self._approach_margin,
        )
        return tier, self._cadence[tier]

    def peak_confirmed(self, station: str) -> bool:
        """Return whether the station has confirmed its diurnal peak."""
        state = self._states.get(station)
        return state.peak_confirmed if state is not None else False

    def reset_station(self, station: str) -> None:
        """Clear peak state for a station — call at the start of each new market day."""
        self._states.pop(station, None)

    def reset_all(self) -> None:
        """Clear all station state — call at the start of a new trading day."""
        self._states.clear()
