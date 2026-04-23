"""Strategy C signal engine: resolution-lag cleanup trading (§4.1.3).

Pure signal layer — takes a LockState as input, returns a CleanupSignal or None.
Does not call Kalshi APIs, does not mutate DB state. Supervisor consumes this.

Settlement time is computed as 23:59 local station time (mapping.timezone_name)
to avoid cutting off the late-afternoon ET trading window.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, WeatherResolutionState
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.services.signal import market_quotes
from kalshi_bot.weather.models import WeatherMarketMapping


# ---------------------------------------------------------------------------
# LockState — input snapshot passed in by the supervisor
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class LockState:
    """Snapshot of one station's lock status at a given observation time."""
    station: str
    observation_ts: datetime
    observed_max_f: float
    consecutive_confirmations: int
    gridpoint_forecast_f: float | None
    cli_variance_degf: float | None


# ---------------------------------------------------------------------------
# LockStateTracker — stateful per-station accumulator (not pure; lives in supervisor)
# ---------------------------------------------------------------------------

@dataclass
class _StationLockAccum:
    consecutive_confirmations: int = 0
    last_confirmed_max_f: float | None = None
    last_observation_ts: datetime | None = None


class LockStateTracker:
    """Tracks consecutive ASOS confirmations per station (Part A gate).

    Call observe() on each poll cycle. It returns an updated LockState.
    """

    def __init__(self) -> None:
        self._state: dict[str, _StationLockAccum] = {}

    def observe(
        self,
        *,
        station: str,
        observation_ts: datetime,
        observed_max_f: float,
        threshold_f: float,
        gridpoint_forecast_f: float | None,
        cli_variance_degf: float | None,
    ) -> LockState:
        accum = self._state.setdefault(station, _StationLockAccum())

        threshold_crossed = observed_max_f >= threshold_f
        if threshold_crossed:
            accum.consecutive_confirmations += 1
        else:
            accum.consecutive_confirmations = 0

        accum.last_confirmed_max_f = observed_max_f
        accum.last_observation_ts = observation_ts

        return LockState(
            station=station,
            observation_ts=observation_ts,
            observed_max_f=observed_max_f,
            consecutive_confirmations=accum.consecutive_confirmations,
            gridpoint_forecast_f=gridpoint_forecast_f,
            cli_variance_degf=cli_variance_degf,
        )

    def reset_station(self, station: str) -> None:
        self._state.pop(station, None)

    def reset_all(self) -> None:
        self._state.clear()

    def consecutive_confirmations(self, station: str) -> int:
        return self._state.get(station, _StationLockAccum()).consecutive_confirmations


# ---------------------------------------------------------------------------
# CleanupSignal — output of evaluate_cleanup_signal()
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CleanupSignal:
    ticker: str
    station: str
    resolution_state: WeatherResolutionState
    observed_max_f: float
    threshold_f: float
    fair_value_dollars: Decimal
    edge_cents: float
    target_price_cents: float
    side: ContractSide
    shadow: bool
    suppression_reason: str | None = None


# ---------------------------------------------------------------------------
# Gate check functions (pure; each returns (passed: bool, reason: str | None))
# ---------------------------------------------------------------------------

def check_part_a(lock_state: LockState, *, required_consecutive: int) -> tuple[bool, str | None]:
    """Part A: persistence — must hold across N consecutive ASOS transmissions."""
    if lock_state.consecutive_confirmations >= required_consecutive:
        return True, None
    return False, (
        f"part_a_persistence: only {lock_state.consecutive_confirmations}/"
        f"{required_consecutive} consecutive confirmations"
    )


def check_part_b(lock_state: LockState, *, max_forecast_residual_f: float) -> tuple[bool, str | None]:
    """Part B: cross-source sanity — observed max within N°F of gridpoint forecast."""
    if lock_state.gridpoint_forecast_f is None:
        return True, None
    residual = abs(lock_state.observed_max_f - lock_state.gridpoint_forecast_f)
    if residual <= max_forecast_residual_f:
        return True, None
    return False, (
        f"part_b_forecast_residual: {residual:.1f}°F > {max_forecast_residual_f:.1f}°F limit"
    )


def check_part_c(
    lock_state: LockState,
    *,
    max_age_minutes: int,
    reference_time: datetime | None = None,
) -> tuple[bool, str | None]:
    """Part C: freshness — observation timestamp within max_age_minutes."""
    now = reference_time or datetime.now(UTC)
    obs_ts = lock_state.observation_ts
    if obs_ts.tzinfo is None:
        obs_ts = obs_ts.replace(tzinfo=UTC)
    age_minutes = (now - obs_ts).total_seconds() / 60
    if age_minutes <= max_age_minutes:
        return True, None
    return False, f"part_c_freshness: observation {age_minutes:.1f} min old > {max_age_minutes} min limit"


def check_cli_variance(
    lock_state: LockState,
    *,
    max_cli_variance_degf: float,
) -> tuple[bool, str | None]:
    """Station must have ≤ max_cli_variance_degf historical CLI/ASOS variance."""
    if lock_state.cli_variance_degf is None:
        return True, None
    if lock_state.cli_variance_degf <= max_cli_variance_degf:
        return True, None
    return False, (
        f"cli_variance: station variance {lock_state.cli_variance_degf:.2f}°F "
        f"> {max_cli_variance_degf:.2f}°F ceiling"
    )


def check_book_freshness(
    market_snapshot: dict[str, Any],
    *,
    max_age_seconds: int,
    reference_time: datetime | None = None,
) -> tuple[bool, str | None]:
    """Market book must have been observed within max_age_seconds."""
    now = reference_time or datetime.now(UTC)
    raw_observed_at = (
        market_snapshot.get("observed_at")
        or market_snapshot.get("market", {}).get("observed_at")
    )
    if raw_observed_at is None:
        return False, "book_freshness: no observed_at in market snapshot"
    if isinstance(raw_observed_at, str):
        try:
            raw_observed_at = datetime.fromisoformat(raw_observed_at)
        except ValueError:
            return False, "book_freshness: unparseable observed_at"
    if raw_observed_at.tzinfo is None:
        raw_observed_at = raw_observed_at.replace(tzinfo=UTC)
    age = (now - raw_observed_at).total_seconds()
    if age <= max_age_seconds:
        return True, None
    return False, f"book_freshness: book {age:.0f}s old > {max_age_seconds}s limit"


def check_market_status(market_snapshot: dict[str, Any]) -> tuple[bool, str | None]:
    """Market must be 'active'; suspended/paused/closed_pending_settlement are not tradeable."""
    market = market_snapshot.get("market", market_snapshot)
    status = market.get("status", "")
    if status == "active":
        return True, None
    return False, f"market_status: status='{status}' is not tradeable"


def check_time_to_settlement(
    mapping: WeatherMarketMapping,
    market_date: date,
    *,
    min_minutes: int,
    reference_time: datetime | None = None,
) -> tuple[bool, str | None]:
    """Must be ≥ min_minutes before 23:59 local settlement time."""
    now = reference_time or datetime.now(UTC)

    tz_name = mapping.timezone_name or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")

    # Settlement = 23:59 local time on market_date
    settlement_local = datetime.combine(market_date, time(23, 59, 0), tzinfo=tz)
    settlement_utc = settlement_local.astimezone(UTC)

    minutes_left = (settlement_utc - now).total_seconds() / 60
    if minutes_left >= min_minutes:
        return True, None
    return False, f"time_to_settlement: {minutes_left:.1f} min remaining < {min_minutes} min minimum"


# ---------------------------------------------------------------------------
# Fair-value computation
# ---------------------------------------------------------------------------

def _fair_value_dollars(
    resolution_state: WeatherResolutionState,
    *,
    locked_yes_discount_cents: int,
    locked_no_discount_cents: int,
) -> Decimal:
    if resolution_state == WeatherResolutionState.LOCKED_YES:
        return quantize_price(Decimal("1.0000") - Decimal(locked_yes_discount_cents) / 100)
    if resolution_state == WeatherResolutionState.LOCKED_NO:
        return quantize_price(Decimal(locked_no_discount_cents) / 100)
    raise ValueError(f"Non-locked resolution state: {resolution_state!r}")


def _edge_and_side(
    fair_dollars: Decimal,
    quotes: dict[str, Decimal | None],
    resolution_state: WeatherResolutionState,
) -> tuple[float, Decimal | None, ContractSide] | None:
    """Return (edge_cents, target_price_dollars, side) or None if no actionable quote."""
    if resolution_state == WeatherResolutionState.LOCKED_YES:
        ask = quotes["yes_ask"]
        if ask is None:
            return None
        edge_cents = float((fair_dollars - ask) * 100)
        return edge_cents, ask, ContractSide.YES

    if resolution_state == WeatherResolutionState.LOCKED_NO:
        # Buying NO: fair_no = locked_no_discount_cents/100; market ask is no_ask
        no_ask = quotes["no_ask"]
        if no_ask is None:
            return None
        fair_no = quantize_price(Decimal("1.0000") - fair_dollars)
        edge_cents = float((fair_no - no_ask) * 100)
        return edge_cents, no_ask, ContractSide.NO

    return None


def _market_date_from_ticker(ticker: str) -> date | None:
    """Parse YYYYMMDD market date from a Kalshi weather ticker (e.g. KXHIGHTBOS-26APR22-T58)."""
    parts = ticker.split("-")
    if len(parts) < 2:
        return None
    try:
        return datetime.strptime(parts[1], "%y%b%d").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main evaluation entry point (pure function)
# ---------------------------------------------------------------------------

def evaluate_cleanup_signal(
    *,
    ticker: str,
    mapping: WeatherMarketMapping,
    resolution_state: WeatherResolutionState,
    lock_state: LockState,
    market_snapshot: dict[str, Any],
    settings: Settings,
    reference_time: datetime | None = None,
) -> CleanupSignal | None:
    """Evaluate all Strategy C gates and return a CleanupSignal or None.

    Returns None only when resolution_state is not locked. Suppressed signals
    are returned with suppression_reason set — callers log them; supervisors
    skip execution.
    """
    if resolution_state not in (WeatherResolutionState.LOCKED_YES, WeatherResolutionState.LOCKED_NO):
        return None

    threshold_f = mapping.threshold_f
    if threshold_f is None:
        return CleanupSignal(
            ticker=ticker,
            station=lock_state.station,
            resolution_state=resolution_state,
            observed_max_f=lock_state.observed_max_f,
            threshold_f=0.0,
            fair_value_dollars=Decimal("0"),
            edge_cents=0.0,
            target_price_cents=0.0,
            side=ContractSide.YES,
            shadow=settings.strategy_c_shadow_only,
            suppression_reason="no_threshold: mapping.threshold_f is None",
        )

    # --- lock-confirmation gates ---
    for passed, reason in [
        check_part_a(lock_state, required_consecutive=settings.strategy_c_required_consecutive_confirmations),
        check_part_b(lock_state, max_forecast_residual_f=settings.strategy_c_max_forecast_residual_f),
        check_part_c(lock_state, max_age_minutes=settings.strategy_c_max_observation_age_minutes, reference_time=reference_time),
        check_cli_variance(lock_state, max_cli_variance_degf=settings.strategy_c_max_cli_variance_degf),
    ]:
        if not passed:
            return CleanupSignal(
                ticker=ticker,
                station=lock_state.station,
                resolution_state=resolution_state,
                observed_max_f=lock_state.observed_max_f,
                threshold_f=threshold_f,
                fair_value_dollars=Decimal("0"),
                edge_cents=0.0,
                target_price_cents=0.0,
                side=ContractSide.YES,
                shadow=settings.strategy_c_shadow_only,
                suppression_reason=reason,
            )

    # --- book freshness and market status gates ---
    for passed, reason in [
        check_book_freshness(market_snapshot, max_age_seconds=settings.strategy_c_max_book_age_seconds, reference_time=reference_time),
        check_market_status(market_snapshot),
    ]:
        if not passed:
            return CleanupSignal(
                ticker=ticker,
                station=lock_state.station,
                resolution_state=resolution_state,
                observed_max_f=lock_state.observed_max_f,
                threshold_f=threshold_f,
                fair_value_dollars=Decimal("0"),
                edge_cents=0.0,
                target_price_cents=0.0,
                side=ContractSide.YES,
                shadow=settings.strategy_c_shadow_only,
                suppression_reason=reason,
            )

    # --- time-to-settlement gate ---
    market_date = _market_date_from_ticker(ticker)
    if market_date is not None:
        passed, reason = check_time_to_settlement(
            mapping,
            market_date,
            min_minutes=settings.strategy_c_min_time_to_settlement_minutes,
            reference_time=reference_time,
        )
        if not passed:
            return CleanupSignal(
                ticker=ticker,
                station=lock_state.station,
                resolution_state=resolution_state,
                observed_max_f=lock_state.observed_max_f,
                threshold_f=threshold_f,
                fair_value_dollars=Decimal("0"),
                edge_cents=0.0,
                target_price_cents=0.0,
                side=ContractSide.YES,
                shadow=settings.strategy_c_shadow_only,
                suppression_reason=reason,
            )

    # --- fair value and edge ---
    fair_dollars = _fair_value_dollars(
        resolution_state,
        locked_yes_discount_cents=settings.strategy_c_locked_yes_discount_cents,
        locked_no_discount_cents=settings.strategy_c_locked_no_discount_cents,
    )
    quotes = market_quotes(market_snapshot)
    result = _edge_and_side(fair_dollars, quotes, resolution_state)

    if result is None:
        return CleanupSignal(
            ticker=ticker,
            station=lock_state.station,
            resolution_state=resolution_state,
            observed_max_f=lock_state.observed_max_f,
            threshold_f=threshold_f,
            fair_value_dollars=fair_dollars,
            edge_cents=0.0,
            target_price_cents=0.0,
            side=ContractSide.YES,
            shadow=settings.strategy_c_shadow_only,
            suppression_reason="no_quote: required taker quote missing from book",
        )

    edge_cents, target_price_dollars, side = result

    if edge_cents < settings.strategy_c_min_edge_cents:
        return CleanupSignal(
            ticker=ticker,
            station=lock_state.station,
            resolution_state=resolution_state,
            observed_max_f=lock_state.observed_max_f,
            threshold_f=threshold_f,
            fair_value_dollars=fair_dollars,
            edge_cents=edge_cents,
            target_price_cents=float(target_price_dollars * 100) if target_price_dollars else 0.0,
            side=side,
            shadow=settings.strategy_c_shadow_only,
            suppression_reason=f"insufficient_edge: {edge_cents:.2f}¢ < {settings.strategy_c_min_edge_cents}¢ minimum",
        )

    return CleanupSignal(
        ticker=ticker,
        station=lock_state.station,
        resolution_state=resolution_state,
        observed_max_f=lock_state.observed_max_f,
        threshold_f=threshold_f,
        fair_value_dollars=fair_dollars,
        edge_cents=edge_cents,
        target_price_cents=float(target_price_dollars * 100) if target_price_dollars else 0.0,
        side=side,
        shadow=settings.strategy_c_shadow_only,
        suppression_reason=None,
    )
