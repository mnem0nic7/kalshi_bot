"""Unit tests for strategy_cleanup.py (Session 6, §4.1.3).

Coverage:
- LockStateTracker: consecutive confirmation accumulation, reset
- check_part_a / check_part_b / check_part_c
- check_cli_variance / check_book_freshness / check_market_status
- check_time_to_settlement (local-timezone logic — critical correctness zone)
- _fair_value_dollars: locked-yes, locked-no, flat discount
- _edge_and_side: YES buy, NO buy, missing quote
- evaluate_cleanup_signal: gate short-circuit ordering, LOCKED_YES path, LOCKED_NO path,
  suppression reasons, shadow flag, non-locked returns None
"""
from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, WeatherResolutionState
from kalshi_bot.services.strategy_cleanup import (
    LockState,
    LockStateTracker,
    _edge_and_side,
    _fair_value_dollars,
    _market_date_from_ticker,
    check_book_freshness,
    check_cli_variance,
    check_market_status,
    check_part_a,
    check_part_b,
    check_part_c,
    check_time_to_settlement,
    evaluate_cleanup_signal,
)
from kalshi_bot.weather.models import WeatherMarketMapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(UTC)


def _lock(
    *,
    station: str = "KBOS",
    observation_ts: datetime | None = None,
    observed_max_f: float = 88.0,
    consecutive_confirmations: int = 2,
    gridpoint_forecast_f: float | None = 85.0,
    cli_variance_degf: float | None = 0.3,
) -> LockState:
    return LockState(
        station=station,
        observation_ts=observation_ts or _now(),
        observed_max_f=observed_max_f,
        consecutive_confirmations=consecutive_confirmations,
        gridpoint_forecast_f=gridpoint_forecast_f,
        cli_variance_degf=cli_variance_degf,
    )


def _mapping(*, threshold_f: float = 85.0, timezone_name: str = "America/New_York") -> WeatherMarketMapping:
    return WeatherMarketMapping(
        market_ticker="KXHIGHTBOS",
        station_id="KBOS",
        location_name="Boston",
        latitude=42.36,
        longitude=-71.01,
        timezone_name=timezone_name,
        threshold_f=threshold_f,
    )


def _snapshot(*, yes_ask: float = 0.94, no_ask: float = 0.07, status: str = "active", observed_at: datetime | None = None) -> dict:
    obs = (observed_at or _now()).isoformat()
    return {
        "market": {
            "yes_bid_dollars": yes_ask - 0.03,
            "yes_ask_dollars": yes_ask,
            "no_ask_dollars": no_ask,
            "status": status,
        },
        "observed_at": obs,
    }


def _settings(**overrides) -> Settings:
    base = dict(
        strategy_c_required_consecutive_confirmations=2,
        strategy_c_max_observation_age_minutes=30,
        strategy_c_max_forecast_residual_f=8.0,
        strategy_c_max_cli_variance_degf=1.5,
        strategy_c_min_time_to_settlement_minutes=60,
        strategy_c_locked_yes_discount_cents=1,
        strategy_c_locked_no_discount_cents=1,
        strategy_c_min_edge_cents=2,
        strategy_c_max_book_age_seconds=30,
        strategy_c_shadow_only=True,
        database_url="sqlite+aiosqlite:///:memory:",
    )
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# LockStateTracker
# ---------------------------------------------------------------------------

class TestLockStateTracker:
    def test_first_reading_above_threshold_gives_1_confirmation(self) -> None:
        tracker = LockStateTracker()
        state = tracker.observe(
            station="KBOS",
            observation_ts=_now(),
            observed_max_f=87.0,
            threshold_f=85.0,
            gridpoint_forecast_f=86.0,
            cli_variance_degf=0.3,
        )
        assert state.consecutive_confirmations == 1

    def test_second_consecutive_reading_gives_2(self) -> None:
        tracker = LockStateTracker()
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=87.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        state = tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=88.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        assert state.consecutive_confirmations == 2

    def test_reading_below_threshold_resets_to_zero(self) -> None:
        tracker = LockStateTracker()
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=87.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        state = tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=84.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        assert state.consecutive_confirmations == 0

    def test_reset_station_clears_accumulator(self) -> None:
        tracker = LockStateTracker()
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=87.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        tracker.reset_station("KBOS")
        assert tracker.consecutive_confirmations("KBOS") == 0

    def test_reset_all_clears_all_stations(self) -> None:
        tracker = LockStateTracker()
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=87.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        tracker.observe(station="KLAX", observation_ts=_now(), observed_max_f=92.0, threshold_f=90.0, gridpoint_forecast_f=91.0, cli_variance_degf=0.2)
        tracker.reset_all()
        assert tracker.consecutive_confirmations("KBOS") == 0
        assert tracker.consecutive_confirmations("KLAX") == 0

    def test_independent_accumulation_per_station(self) -> None:
        tracker = LockStateTracker()
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=87.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        tracker.observe(station="KBOS", observation_ts=_now(), observed_max_f=88.0, threshold_f=85.0, gridpoint_forecast_f=86.0, cli_variance_degf=0.3)
        tracker.observe(station="KLAX", observation_ts=_now(), observed_max_f=92.0, threshold_f=90.0, gridpoint_forecast_f=91.0, cli_variance_degf=0.2)
        assert tracker.consecutive_confirmations("KBOS") == 2
        assert tracker.consecutive_confirmations("KLAX") == 1

    def test_unknown_station_returns_zero(self) -> None:
        tracker = LockStateTracker()
        assert tracker.consecutive_confirmations("UNKNOWN") == 0


# ---------------------------------------------------------------------------
# Part A: persistence gate
# ---------------------------------------------------------------------------

class TestCheckPartA:
    def test_passes_when_confirmations_meet_required(self) -> None:
        state = _lock(consecutive_confirmations=2)
        passed, reason = check_part_a(state, required_consecutive=2)
        assert passed is True
        assert reason is None

    def test_passes_when_confirmations_exceed_required(self) -> None:
        state = _lock(consecutive_confirmations=5)
        passed, _ = check_part_a(state, required_consecutive=2)
        assert passed is True

    def test_fails_when_only_one_confirmation(self) -> None:
        state = _lock(consecutive_confirmations=1)
        passed, reason = check_part_a(state, required_consecutive=2)
        assert passed is False
        assert "1/2" in reason

    def test_fails_when_zero_confirmations(self) -> None:
        state = _lock(consecutive_confirmations=0)
        passed, _ = check_part_a(state, required_consecutive=2)
        assert passed is False


# ---------------------------------------------------------------------------
# Part B: cross-source sanity gate
# ---------------------------------------------------------------------------

class TestCheckPartB:
    def test_passes_when_residual_within_limit(self) -> None:
        state = _lock(observed_max_f=87.0, gridpoint_forecast_f=85.0)
        passed, _ = check_part_b(state, max_forecast_residual_f=8.0)
        assert passed is True

    def test_fails_when_residual_exceeds_limit(self) -> None:
        state = _lock(observed_max_f=97.0, gridpoint_forecast_f=85.0)
        passed, reason = check_part_b(state, max_forecast_residual_f=8.0)
        assert passed is False
        assert "12.0" in reason

    def test_passes_when_forecast_not_available(self) -> None:
        state = _lock(gridpoint_forecast_f=None)
        passed, _ = check_part_b(state, max_forecast_residual_f=8.0)
        assert passed is True

    def test_exact_boundary_passes(self) -> None:
        state = _lock(observed_max_f=93.0, gridpoint_forecast_f=85.0)
        passed, _ = check_part_b(state, max_forecast_residual_f=8.0)
        assert passed is True


# ---------------------------------------------------------------------------
# Part C: freshness gate
# ---------------------------------------------------------------------------

class TestCheckPartC:
    def test_passes_when_fresh(self) -> None:
        state = _lock(observation_ts=_now() - timedelta(minutes=15))
        passed, _ = check_part_c(state, max_age_minutes=30)
        assert passed is True

    def test_fails_when_stale(self) -> None:
        state = _lock(observation_ts=_now() - timedelta(minutes=45))
        passed, reason = check_part_c(state, max_age_minutes=30)
        assert passed is False
        assert "45" in reason or "44" in reason

    def test_passes_with_naive_datetime(self) -> None:
        naive_ts = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=10)
        state = _lock(observation_ts=naive_ts)
        passed, _ = check_part_c(state, max_age_minutes=30, reference_time=_now())
        assert passed is True

    def test_custom_reference_time(self) -> None:
        fixed_ref = datetime(2026, 4, 22, 14, 0, 0, tzinfo=UTC)
        state = _lock(observation_ts=datetime(2026, 4, 22, 13, 50, 0, tzinfo=UTC))
        passed, _ = check_part_c(state, max_age_minutes=30, reference_time=fixed_ref)
        assert passed is True


# ---------------------------------------------------------------------------
# CLI variance gate
# ---------------------------------------------------------------------------

class TestCheckCliVariance:
    def test_passes_when_variance_within_ceiling(self) -> None:
        state = _lock(cli_variance_degf=0.8)
        passed, _ = check_cli_variance(state, max_cli_variance_degf=1.5)
        assert passed is True

    def test_fails_when_variance_exceeds_ceiling(self) -> None:
        state = _lock(cli_variance_degf=2.0)
        passed, reason = check_cli_variance(state, max_cli_variance_degf=1.5)
        assert passed is False
        assert "2.00" in reason

    def test_passes_when_variance_not_available(self) -> None:
        state = _lock(cli_variance_degf=None)
        passed, _ = check_cli_variance(state, max_cli_variance_degf=1.5)
        assert passed is True

    def test_exact_ceiling_passes(self) -> None:
        state = _lock(cli_variance_degf=1.5)
        passed, _ = check_cli_variance(state, max_cli_variance_degf=1.5)
        assert passed is True


# ---------------------------------------------------------------------------
# Book freshness gate
# ---------------------------------------------------------------------------

class TestCheckBookFreshness:
    def test_passes_when_fresh(self) -> None:
        snap = _snapshot(observed_at=_now() - timedelta(seconds=10))
        passed, _ = check_book_freshness(snap, max_age_seconds=30)
        assert passed is True

    def test_fails_when_stale(self) -> None:
        snap = _snapshot(observed_at=_now() - timedelta(seconds=60))
        passed, reason = check_book_freshness(snap, max_age_seconds=30)
        assert passed is False
        assert "book_freshness" in reason

    def test_fails_when_observed_at_missing(self) -> None:
        snap = {"market": {"yes_ask_dollars": 0.94, "status": "active"}}
        passed, reason = check_book_freshness(snap, max_age_seconds=30)
        assert passed is False
        assert "no observed_at" in reason

    def test_passes_with_string_timestamp(self) -> None:
        recent = (_now() - timedelta(seconds=5)).isoformat()
        snap = {"market": {}, "observed_at": recent}
        passed, _ = check_book_freshness(snap, max_age_seconds=30)
        assert passed is True


# ---------------------------------------------------------------------------
# Market status gate
# ---------------------------------------------------------------------------

class TestCheckMarketStatus:
    @pytest.mark.parametrize("status", ["suspended", "paused", "closed_pending_settlement", ""])
    def test_fails_for_non_active_statuses(self, status: str) -> None:
        snap = _snapshot(status=status)
        passed, reason = check_market_status(snap)
        assert passed is False
        assert status in reason

    def test_passes_for_active(self) -> None:
        snap = _snapshot(status="active")
        passed, _ = check_market_status(snap)
        assert passed is True


# ---------------------------------------------------------------------------
# Time-to-settlement gate — critical timezone correctness zone
# ---------------------------------------------------------------------------

class TestCheckTimeToSettlement:
    def test_passes_with_ample_time(self) -> None:
        # Reference time at 14:00 ET, settlement 23:59 ET → ~9h59m remaining
        ref = datetime(2026, 4, 22, 18, 0, 0, tzinfo=UTC)  # 14:00 ET
        m = _mapping(timezone_name="America/New_York")
        passed, _ = check_time_to_settlement(m, date(2026, 4, 22), min_minutes=60, reference_time=ref)
        assert passed is True

    def test_fails_when_too_close_to_settlement(self) -> None:
        # Reference time at 23:30 ET → ~29 min to settlement
        ref = datetime(2026, 4, 23, 3, 30, 0, tzinfo=UTC)  # 23:30 ET
        m = _mapping(timezone_name="America/New_York")
        passed, reason = check_time_to_settlement(m, date(2026, 4, 22), min_minutes=60, reference_time=ref)
        assert passed is False
        assert "time_to_settlement" in reason

    def test_uses_local_timezone_not_utc(self) -> None:
        # 19:59 ET = 23:59 UTC — if we naively used 23:59 UTC as settlement,
        # this would report 0 minutes left; using local time correctly gives ~4h left
        ref = datetime(2026, 4, 22, 23, 59, 0, tzinfo=UTC)  # 19:59 ET
        m = _mapping(timezone_name="America/New_York")
        # Settlement = 23:59 ET = 03:59 UTC April 23
        passed, _ = check_time_to_settlement(m, date(2026, 4, 22), min_minutes=60, reference_time=ref)
        assert passed is True  # ~4h left in ET, well above 60 min

    def test_falls_back_to_utc_for_unknown_timezone(self) -> None:
        ref = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)
        m = _mapping(timezone_name="Invalid/Zone")
        # Should not raise; fallback to UTC
        passed, _ = check_time_to_settlement(m, date(2026, 4, 22), min_minutes=60, reference_time=ref)
        assert isinstance(passed, bool)

    def test_pacific_time_timezone(self) -> None:
        # PT station: settlement 23:59 PT = 06:59 UTC next day
        ref = datetime(2026, 4, 22, 18, 0, 0, tzinfo=UTC)  # 11:00 PT
        m = _mapping(timezone_name="America/Los_Angeles")
        passed, _ = check_time_to_settlement(m, date(2026, 4, 22), min_minutes=60, reference_time=ref)
        assert passed is True


# ---------------------------------------------------------------------------
# Fair value and edge computation
# ---------------------------------------------------------------------------

class TestFairValueDollars:
    def test_locked_yes_fair_value(self) -> None:
        fv = _fair_value_dollars(WeatherResolutionState.LOCKED_YES, locked_yes_discount_cents=1, locked_no_discount_cents=1)
        assert fv == Decimal("0.9900")

    def test_locked_no_fair_value(self) -> None:
        fv = _fair_value_dollars(WeatherResolutionState.LOCKED_NO, locked_yes_discount_cents=1, locked_no_discount_cents=1)
        assert fv == Decimal("0.0100")

    def test_locked_yes_two_cent_discount(self) -> None:
        fv = _fair_value_dollars(WeatherResolutionState.LOCKED_YES, locked_yes_discount_cents=2, locked_no_discount_cents=1)
        assert fv == Decimal("0.9800")

    def test_unresolved_raises(self) -> None:
        with pytest.raises(ValueError):
            _fair_value_dollars(WeatherResolutionState.UNRESOLVED, locked_yes_discount_cents=1, locked_no_discount_cents=1)


class TestEdgeAndSide:
    def test_locked_yes_positive_edge(self) -> None:
        quotes = {"yes_ask": Decimal("0.9400"), "yes_bid": Decimal("0.9100"), "no_ask": Decimal("0.0700")}
        fair = Decimal("0.9900")
        result = _edge_and_side(fair, quotes, WeatherResolutionState.LOCKED_YES)
        assert result is not None
        edge_cents, target, side = result
        assert math.isclose(edge_cents, 5.0, abs_tol=0.01)
        assert side == ContractSide.YES
        assert target == Decimal("0.9400")

    def test_locked_yes_negative_edge(self) -> None:
        quotes = {"yes_ask": Decimal("0.9950"), "yes_bid": Decimal("0.9900"), "no_ask": Decimal("0.0100")}
        fair = Decimal("0.9900")
        result = _edge_and_side(fair, quotes, WeatherResolutionState.LOCKED_YES)
        assert result is not None
        edge_cents, _, _ = result
        assert edge_cents < 0

    def test_locked_no_positive_edge(self) -> None:
        # fair_yes = 0.01 → fair_no = 0.99; no_ask = 0.94 → edge = 5¢
        fair_yes = Decimal("0.0100")
        quotes = {"yes_ask": Decimal("0.9800"), "yes_bid": Decimal("0.9500"), "no_ask": Decimal("0.9400")}
        result = _edge_and_side(fair_yes, quotes, WeatherResolutionState.LOCKED_NO)
        assert result is not None
        edge_cents, _, side = result
        assert side == ContractSide.NO
        assert math.isclose(edge_cents, 5.0, abs_tol=0.1)

    def test_missing_yes_ask_returns_none(self) -> None:
        quotes = {"yes_ask": None, "yes_bid": None, "no_ask": Decimal("0.07")}
        fair = Decimal("0.9900")
        result = _edge_and_side(fair, quotes, WeatherResolutionState.LOCKED_YES)
        assert result is None

    def test_missing_no_ask_returns_none(self) -> None:
        quotes = {"yes_ask": Decimal("0.94"), "yes_bid": Decimal("0.91"), "no_ask": None}
        fair = Decimal("0.0100")
        result = _edge_and_side(fair, quotes, WeatherResolutionState.LOCKED_NO)
        assert result is None


class TestMarketDateFromTicker:
    def test_boston_ticker(self) -> None:
        d = _market_date_from_ticker("KXHIGHTBOS-26APR22-T58")
        assert d == date(2026, 4, 22)

    def test_invalid_ticker_returns_none(self) -> None:
        assert _market_date_from_ticker("INVALID") is None

    def test_malformed_date_returns_none(self) -> None:
        assert _market_date_from_ticker("KXHIGHT-BADDATE-T58") is None


# ---------------------------------------------------------------------------
# evaluate_cleanup_signal — integration across gates
# ---------------------------------------------------------------------------

class TestEvaluateCleanupSignal:
    def _base_kwargs(self, *, lock_state: LockState | None = None, mapping: WeatherMarketMapping | None = None, snapshot: dict | None = None):
        ref_time = datetime(2026, 4, 22, 18, 0, 0, tzinfo=UTC)  # 14:00 ET — well before settlement
        return dict(
            ticker="KXHIGHTBOS-26APR22-T58",
            mapping=mapping or _mapping(),
            resolution_state=WeatherResolutionState.LOCKED_YES,
            lock_state=lock_state or _lock(),
            market_snapshot=snapshot or _snapshot(),
            settings=_settings(),
            reference_time=ref_time,
        )

    def test_returns_none_for_unresolved_state(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["resolution_state"] = WeatherResolutionState.UNRESOLVED
        result = evaluate_cleanup_signal(**kwargs)
        assert result is None

    def test_returns_clean_signal_for_valid_locked_yes(self) -> None:
        result = evaluate_cleanup_signal(**self._base_kwargs())
        assert result is not None
        assert result.suppression_reason is None
        assert result.resolution_state == WeatherResolutionState.LOCKED_YES
        assert result.side == ContractSide.YES
        assert result.shadow is True

    def test_returns_suppressed_signal_for_part_a_failure(self) -> None:
        state = _lock(consecutive_confirmations=1)
        result = evaluate_cleanup_signal(**self._base_kwargs(lock_state=state))
        assert result is not None
        assert "part_a" in result.suppression_reason

    def test_returns_suppressed_signal_for_part_b_failure(self) -> None:
        state = _lock(observed_max_f=99.0, gridpoint_forecast_f=85.0)
        result = evaluate_cleanup_signal(**self._base_kwargs(lock_state=state))
        assert result is not None
        assert "part_b" in result.suppression_reason

    def test_returns_suppressed_signal_for_part_c_stale_obs(self) -> None:
        ref_time = datetime(2026, 4, 22, 18, 0, 0, tzinfo=UTC)
        stale_obs = ref_time - timedelta(hours=2)
        state = _lock(observation_ts=stale_obs)
        result = evaluate_cleanup_signal(**self._base_kwargs(lock_state=state))
        assert result is not None
        assert "part_c" in result.suppression_reason

    def test_returns_suppressed_signal_for_high_cli_variance(self) -> None:
        state = _lock(cli_variance_degf=2.5)
        result = evaluate_cleanup_signal(**self._base_kwargs(lock_state=state))
        assert result is not None
        assert "cli_variance" in result.suppression_reason

    def test_returns_suppressed_signal_for_stale_book(self) -> None:
        ref_time = datetime(2026, 4, 22, 18, 0, 0, tzinfo=UTC)
        stale_snap = _snapshot(observed_at=ref_time - timedelta(seconds=90))
        result = evaluate_cleanup_signal(**self._base_kwargs(snapshot=stale_snap))
        assert result is not None
        assert "book_freshness" in result.suppression_reason

    def test_returns_suppressed_signal_for_non_active_market(self) -> None:
        snap = _snapshot(status="suspended")
        result = evaluate_cleanup_signal(**self._base_kwargs(snapshot=snap))
        assert result is not None
        assert "market_status" in result.suppression_reason

    def test_returns_suppressed_signal_for_insufficient_edge(self) -> None:
        # yes_ask at 98¢ → edge = 1¢ against fair 99¢ → below 2¢ minimum
        snap = _snapshot(yes_ask=0.98)
        result = evaluate_cleanup_signal(**self._base_kwargs(snapshot=snap))
        assert result is not None
        assert "insufficient_edge" in result.suppression_reason

    def test_returns_suppressed_signal_for_missing_quote(self) -> None:
        snap = {"market": {"yes_ask_dollars": 0, "no_ask_dollars": 0.07, "status": "active"}, "observed_at": _now().isoformat()}
        result = evaluate_cleanup_signal(**self._base_kwargs(snapshot=snap))
        assert result is not None
        assert "no_quote" in result.suppression_reason

    def test_locked_no_path_returns_no_side(self) -> None:
        kwargs = self._base_kwargs()
        # LOCKED_NO: fair_no = 0.99, no_ask = 0.03 → edge = 96¢ (above minimum)
        kwargs["resolution_state"] = WeatherResolutionState.LOCKED_NO
        snap = _snapshot(yes_ask=0.98, no_ask=0.03)
        kwargs["market_snapshot"] = snap
        result = evaluate_cleanup_signal(**kwargs)
        assert result is not None
        assert result.suppression_reason is None
        assert result.side == ContractSide.NO

    def test_shadow_flag_passed_through(self) -> None:
        s = _settings(strategy_c_shadow_only=True)
        kwargs = self._base_kwargs()
        kwargs["settings"] = s
        result = evaluate_cleanup_signal(**kwargs)
        assert result.shadow is True

    def test_suppressed_signal_for_no_threshold_in_mapping(self) -> None:
        m = WeatherMarketMapping(market_ticker="KXHIGHTBOS", station_id="KBOS", location_name="Boston", latitude=42.0, longitude=-71.0)
        result = evaluate_cleanup_signal(**self._base_kwargs(mapping=m))
        assert result is not None
        assert "no_threshold" in result.suppression_reason

    def test_edge_cents_in_clean_signal_is_positive(self) -> None:
        result = evaluate_cleanup_signal(**self._base_kwargs())
        assert result.suppression_reason is None
        assert result.edge_cents > 0

    def test_part_a_checked_before_book_freshness(self) -> None:
        # Part A fails AND book is stale — Part A reason should dominate
        stale_snap = _snapshot(observed_at=_now() - timedelta(seconds=90))
        state = _lock(consecutive_confirmations=0)
        result = evaluate_cleanup_signal(**self._base_kwargs(lock_state=state, snapshot=stale_snap))
        assert "part_a" in result.suppression_reason
