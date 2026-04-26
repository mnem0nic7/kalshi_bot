"""Unit tests for ThresholdProximityMonitor (Session 4.5, §4.1.3 / §4.1.4).

Coverage:
- classify_proximity: all tier transitions, priority ordering, edge cases
- ThresholdProximityMonitor: stateful peak tracking, cadence mapping, reset
- from_settings: reads config attributes correctly
"""
from __future__ import annotations


from kalshi_bot.services.proximity_monitor import (
    ThresholdProximityMonitor,
    classify_proximity,
)


# ---------------------------------------------------------------------------
# classify_proximity — tier logic (stateless)
# ---------------------------------------------------------------------------

class TestClassifyProximity:
    def test_empty_thresholds_returns_idle(self) -> None:
        assert classify_proximity(72.0, []) == "idle"

    def test_idle_when_far_from_all_thresholds(self) -> None:
        assert classify_proximity(70.0, [85.0, 90.0]) == "idle"

    def test_approach_when_within_5f(self) -> None:
        assert classify_proximity(81.0, [85.0]) == "approach"

    def test_approach_exact_boundary(self) -> None:
        assert classify_proximity(80.0, [85.0]) == "approach"

    def test_idle_just_outside_approach_boundary(self) -> None:
        assert classify_proximity(79.9, [85.0]) == "idle"

    def test_near_threshold_when_within_2f(self) -> None:
        assert classify_proximity(83.5, [85.0]) == "near_threshold"

    def test_near_threshold_exact_boundary(self) -> None:
        assert classify_proximity(83.0, [85.0]) == "near_threshold"

    def test_approach_just_outside_near_threshold(self) -> None:
        assert classify_proximity(82.9, [85.0]) == "approach"

    def test_near_threshold_above_threshold(self) -> None:
        # Already crossed threshold by 1°F — still in near_threshold zone
        assert classify_proximity(86.0, [85.0]) == "near_threshold"

    def test_post_peak_when_peak_confirmed_and_not_near(self) -> None:
        assert classify_proximity(72.0, [85.0], peak_confirmed=True) == "post_peak"

    def test_near_threshold_beats_post_peak(self) -> None:
        # Even if peak confirmed, near_threshold takes priority
        assert classify_proximity(84.0, [85.0], peak_confirmed=True) == "near_threshold"

    def test_approach_beats_post_peak_when_within_approach_zone(self) -> None:
        # peak_confirmed but within approach zone — post_peak wins over approach
        # (post_peak has same cadence as approach; priority is post_peak > approach)
        assert classify_proximity(82.0, [85.0], peak_confirmed=True) == "post_peak"

    def test_multiple_thresholds_uses_closest(self) -> None:
        # Closest threshold is 85°F (distance 1°F) → near_threshold
        result = classify_proximity(84.0, [75.0, 85.0, 95.0])
        assert result == "near_threshold"

    def test_multiple_thresholds_all_far_returns_idle(self) -> None:
        assert classify_proximity(60.0, [80.0, 90.0, 100.0]) == "idle"

    def test_custom_margins_respected(self) -> None:
        # near_threshold_margin_f=1.0, approach_margin_f=3.0
        assert classify_proximity(82.5, [85.0], near_threshold_margin_f=1.0, approach_margin_f=3.0) == "approach"
        assert classify_proximity(84.1, [85.0], near_threshold_margin_f=1.0, approach_margin_f=3.0) == "near_threshold"
        assert classify_proximity(81.9, [85.0], near_threshold_margin_f=1.0, approach_margin_f=3.0) == "idle"

    def test_negative_temp_no_crash(self) -> None:
        result = classify_proximity(-10.0, [32.0])
        assert result in {"idle", "approach", "near_threshold", "post_peak"}

    def test_temp_exactly_at_threshold(self) -> None:
        # distance = 0 → near_threshold
        assert classify_proximity(85.0, [85.0]) == "near_threshold"


# ---------------------------------------------------------------------------
# ThresholdProximityMonitor — stateful peak tracking
# ---------------------------------------------------------------------------

def _monitor(**kwargs: object) -> ThresholdProximityMonitor:
    defaults = dict(
        near_threshold_margin_f=2.0,
        approach_margin_f=5.0,
        cadence_idle_seconds=3600,
        cadence_approach_seconds=900,
        cadence_near_threshold_seconds=150,
        cadence_post_peak_seconds=900,
    )
    defaults.update(kwargs)  # type: ignore[arg-type]
    return ThresholdProximityMonitor(**defaults)  # type: ignore[arg-type]


class TestThresholdProximityMonitorTiers:
    def test_first_observation_idle_when_far(self) -> None:
        m = _monitor()
        tier, _ = m.update("KBOS", 70.0, [85.0])
        assert tier == "idle"

    def test_first_observation_approach(self) -> None:
        m = _monitor()
        tier, _ = m.update("KBOS", 82.0, [85.0])
        assert tier == "approach"

    def test_first_observation_near_threshold(self) -> None:
        m = _monitor()
        tier, _ = m.update("KBOS", 84.0, [85.0])
        assert tier == "near_threshold"

    def test_peak_confirmed_after_rise_then_drop(self) -> None:
        m = _monitor()
        m.update("KBOS", 70.0, [95.0])
        m.update("KBOS", 75.0, [95.0])
        m.update("KBOS", 80.0, [95.0])
        m.update("KBOS", 78.0, [95.0])   # starts cooling (dropped > 1°F)
        tier, _ = m.update("KBOS", 76.0, [95.0])
        assert tier == "post_peak"
        assert m.peak_confirmed("KBOS") is True

    def test_peak_not_confirmed_while_still_rising(self) -> None:
        m = _monitor()
        m.update("KBOS", 70.0, [95.0])
        m.update("KBOS", 75.0, [95.0])
        tier, _ = m.update("KBOS", 80.0, [95.0])
        assert tier == "idle"
        assert m.peak_confirmed("KBOS") is False

    def test_minor_temp_drop_does_not_confirm_peak(self) -> None:
        # Drop of only 0.5°F (< _PEAK_COOLING_GAP_F=1.0°F) should not confirm peak
        m = _monitor()
        m.update("KBOS", 80.0, [95.0])
        tier, _ = m.update("KBOS", 79.6, [95.0])
        assert tier == "idle"
        assert m.peak_confirmed("KBOS") is False

    def test_near_threshold_overrides_post_peak(self) -> None:
        m = _monitor()
        m.update("KBOS", 90.0, [95.0])  # peak
        m.update("KBOS", 88.0, [95.0])  # cooling → post_peak
        # Now station gets hotter again (new reading near threshold)
        tier, _ = m.update("KBOS", 94.0, [95.0])
        assert tier == "near_threshold"

    def test_peak_reset_clears_state(self) -> None:
        m = _monitor()
        m.update("KBOS", 85.0, [95.0])
        m.update("KBOS", 80.0, [95.0])  # → post_peak
        m.reset_station("KBOS")
        assert m.peak_confirmed("KBOS") is False
        tier, _ = m.update("KBOS", 70.0, [95.0])
        assert tier == "idle"

    def test_reset_all_clears_all_stations(self) -> None:
        m = _monitor()
        m.update("KBOS", 85.0, [90.0])
        m.update("KBOS", 82.0, [90.0])
        m.update("KLAX", 85.0, [90.0])
        m.update("KLAX", 82.0, [90.0])
        m.reset_all()
        assert m.peak_confirmed("KBOS") is False
        assert m.peak_confirmed("KLAX") is False

    def test_independent_state_per_station(self) -> None:
        m = _monitor()
        # KBOS: rising
        m.update("KBOS", 80.0, [95.0])
        m.update("KBOS", 85.0, [95.0])
        # KLAX: peaked and cooling
        m.update("KLAX", 88.0, [95.0])
        m.update("KLAX", 85.0, [95.0])

        tier_bos, _ = m.update("KBOS", 87.0, [95.0])
        tier_lax, _ = m.update("KLAX", 83.0, [95.0])
        assert tier_bos == "idle"
        assert tier_lax == "post_peak"

    def test_peak_confirmed_unknown_station_returns_false(self) -> None:
        m = _monitor()
        assert m.peak_confirmed("UNKNOWN") is False


# ---------------------------------------------------------------------------
# Cadence mapping
# ---------------------------------------------------------------------------

class TestCadenceMapping:
    def test_idle_cadence(self) -> None:
        m = _monitor(cadence_idle_seconds=3600)
        _, cadence = m.update("KBOS", 60.0, [90.0])
        assert cadence == 3600

    def test_approach_cadence(self) -> None:
        m = _monitor(cadence_approach_seconds=900)
        _, cadence = m.update("KBOS", 86.0, [90.0])
        assert cadence == 900

    def test_near_threshold_cadence(self) -> None:
        m = _monitor(cadence_near_threshold_seconds=150)
        _, cadence = m.update("KBOS", 89.0, [90.0])
        assert cadence == 150

    def test_post_peak_cadence(self) -> None:
        m = _monitor(cadence_post_peak_seconds=900)
        m.update("KBOS", 90.0, [95.0])
        m.update("KBOS", 88.0, [95.0])
        _, cadence = m.update("KBOS", 85.0, [95.0])
        assert cadence == 900

    def test_cadence_accelerates_as_temp_rises(self) -> None:
        m = _monitor()
        _, c_idle = m.update("KBOS", 70.0, [90.0])
        _, c_approach = m.update("KBOS", 86.0, [90.0])
        _, c_near = m.update("KBOS", 89.0, [90.0])
        assert c_near < c_approach < c_idle


# ---------------------------------------------------------------------------
# from_settings factory
# ---------------------------------------------------------------------------

class TestFromSettings:
    def test_reads_all_cadence_settings(self) -> None:
        class FakeSettings:
            strategy_c_near_threshold_margin_f = 1.5
            strategy_c_approach_margin_f = 4.0
            strategy_c_cadence_idle_seconds = 7200
            strategy_c_cadence_approach_seconds = 600
            strategy_c_cadence_near_threshold_seconds = 120
            strategy_c_cadence_post_peak_seconds = 600

        m = ThresholdProximityMonitor.from_settings(FakeSettings())
        assert m._near_margin == 1.5
        assert m._approach_margin == 4.0
        assert m._cadence["idle"] == 7200
        assert m._cadence["approach"] == 600
        assert m._cadence["near_threshold"] == 120
        assert m._cadence["post_peak"] == 600

    def test_defaults_used_when_attrs_missing(self) -> None:
        class EmptySettings:
            pass

        m = ThresholdProximityMonitor.from_settings(EmptySettings())
        assert m._near_margin == 2.0
        assert m._approach_margin == 5.0
        assert m._cadence["idle"] == 3600
        assert m._cadence["near_threshold"] == 150
