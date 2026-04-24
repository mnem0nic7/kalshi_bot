from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.core.enums import StandDownReason, WeatherResolutionState
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.weather.models import WeatherMarketMapping

NEAR_THRESHOLD_DELTA_F = 2.0
LONGSHOT_FAIR_THRESHOLD = Decimal("0.0800")
NEAR_THRESHOLD_PENALTY = Decimal("0.0500")
LONGSHOT_PENALTY = Decimal("0.0150")


def celsius_to_fahrenheit(value_c: float | None) -> float | None:
    if value_c is None:
        return None
    return (value_c * 9 / 5) + 32


def extract_forecast_high_f(
    payload: dict[str, Any],
    target_date: date | None = None,
) -> float | None:
    """Return the forecast high for target_date from NWS /forecast periods.

    target_date is compared against each period's startTime (local date with tz offset).
    Falls back to the first daytime period when no period matches.
    """
    periods = payload.get("properties", {}).get("periods", [])
    use_date = target_date

    def _temp_f(period: dict[str, Any]) -> float | None:
        temperature = period.get("temperature")
        unit = period.get("temperatureUnit", "F")
        if temperature is None:
            return None
        return float(temperature) if unit == "F" else celsius_to_fahrenheit(float(temperature))

    # First pass: return the daytime period whose startTime matches target_date.
    for period in periods:
        if not period.get("isDaytime", False):
            continue
        start_time = period.get("startTime", "")
        if use_date is not None and start_time:
            try:
                dt = datetime.fromisoformat(start_time)
                if dt.date() != use_date:
                    continue
            except (ValueError, TypeError):
                pass
        val = _temp_f(period)
        if val is not None:
            return val

    # Second pass fallback: first available daytime period (target_date was set but unmatched).
    if use_date is not None:
        for period in periods:
            if not period.get("isDaytime", False):
                continue
            val = _temp_f(period)
            if val is not None:
                return val

    return None


def extract_current_temp_f(payload: dict[str, Any]) -> float | None:
    value_c = payload.get("properties", {}).get("temperature", {}).get("value")
    return celsius_to_fahrenheit(float(value_c)) if value_c is not None else None


def logistic_probability(delta_f: float, spread_f: float = 3.5) -> float:
    spread = max(abs(spread_f), 1.0)
    return 1.0 / (1.0 + math.exp(-delta_f / spread))


def _adaptive_spread_f(delta_f: float) -> float:
    """Widen logistic spread near threshold to avoid overconfident predictions.

    Historical backtests show flat-delta markets (|delta| < 3°F) are near
    coin-flip regardless of the NWS forecast. Widening the spread reduces
    overconfident fair-value estimates in these hard-to-call regimes.
    """
    if abs(delta_f) < 2.0:
        return 6.0   # very flat: ~0.43–0.57 range instead of ~0.36–0.64
    if abs(delta_f) < 4.0:
        return 4.5   # near-threshold zone
    return 3.5       # comfortable delta: standard spread


@dataclass(slots=True)
class WeatherSignalSnapshot:
    fair_yes_dollars: Decimal
    confidence: float
    forecast_high_f: float | None
    current_temp_f: float | None
    forecast_delta_f: float | None
    confidence_band: str
    trade_regime: str
    resolution_state: WeatherResolutionState
    observation_time: datetime | None
    forecast_updated_time: datetime | None
    summary: str
    stand_down_reason: StandDownReason | None = None


def gaussian_probability(delta_f: float, sigma_f: float = 3.5) -> float:
    """P(high > threshold) = Φ(delta_f / sigma_f) where delta_f = forecast_high - threshold."""
    sigma = max(abs(sigma_f), 0.5)
    return 0.5 * (1.0 + math.erf(delta_f / (sigma * math.sqrt(2))))


# NWS forecast uncertainty by month (σ in °F), derived from 2911 market-days of
# historical_weather_snapshots vs historical_settlement_labels crosscheck.
# Jan/Feb/Dec: NWS is more accurate than the original 4.5°F assumption (empirical ~2.5°F).
# Apr: NWS is significantly less accurate than assumed — spring variability is high,
#      empirical σ ranges 1.9–10.1°F across cities; using 6.0°F as conservative cover.
# May–Nov: insufficient settled data; prior assumptions retained.
_MONTHLY_SIGMA_F: dict[int, float] = {
    1: 3.0, 2: 3.5, 3: 4.0, 4: 6.0, 5: 3.5,
    6: 3.0, 7: 2.8, 8: 2.8, 9: 3.0, 10: 3.5,
    11: 4.0, 12: 3.0,
}


def nws_forecast_sigma_f(month: int) -> float:
    return _MONTHLY_SIGMA_F.get(month, 3.5)


@dataclass(slots=True)
class SigmaContext:
    """Pre-loaded DB-fit parameters for σ resolution. All fields optional — None → skip that layer."""
    station: str | None = None
    season_bucket: str | None = None
    lead_hours: float | None = None
    sigma_params: dict | None = None   # keyed by (station, season_bucket)
    lead_factors: dict | None = None   # keyed by lead_bucket
    lead_correction_enabled: bool = True


# Minimum sample counts for DB-fit to win over each fallback layer (§4.2.3).
_DB_BEATS_GLOBAL_MIN_N = 100
_DB_BEATS_YAML_MIN_N = 200


def sigma_f_for_mapping(
    mapping: Any,
    month: int,
    *,
    ctx: "SigmaContext | None" = None,
) -> float:
    """Return the calibrated σ for a market mapping, three-layer resolver.

    Layer priority:
    1. DB-fit (station, season_bucket): requires sample_count ≥ threshold AND CRPS improvement > 0.
       Threshold is 200 when a YAML anchor exists for this month, 100 otherwise.
    2. YAML anchor: mapping.sigma_f_by_month[month].
    3. Global fallback: nws_forecast_sigma_f(month).
    """
    overrides = getattr(mapping, "sigma_f_by_month", None)
    yaml_sigma = float(overrides[month]) if (overrides and month in overrides) else None

    if ctx is not None and ctx.sigma_params is not None and ctx.station and ctx.season_bucket:
        params = ctx.sigma_params.get((ctx.station, ctx.season_bucket))
        if params is not None:
            n = params.get("sample_count", 0)
            crps_ok = (params.get("crps_improvement_vs_global") or 0.0) > 0.0
            min_n = _DB_BEATS_YAML_MIN_N if yaml_sigma is not None else _DB_BEATS_GLOBAL_MIN_N
            if n >= min_n and crps_ok:
                sigma = float(params["sigma_base_f"])
                if ctx.lead_correction_enabled and ctx.lead_factors and ctx.lead_hours is not None:
                    from kalshi_bot.weather.sigma_calibration import lead_bucket_for_hours
                    bucket = lead_bucket_for_hours(ctx.lead_hours)
                    sigma *= ctx.lead_factors.get(bucket, 1.0)
                return max(sigma, 0.5)

    if yaml_sigma is not None:
        return yaml_sigma
    return nws_forecast_sigma_f(month)


def extract_gridpoint_max_temp_f(
    gridpoint_payload: dict[str, Any],
    target_date: date | None = None,
) -> float | None:
    """Parse maxTemperature from NWS forecastGridData for a target date (defaults to today UTC)."""
    values = (
        gridpoint_payload
        .get("properties", {})
        .get("maxTemperature", {})
        .get("values", [])
    )
    if not values:
        return None
    uom = gridpoint_payload.get("properties", {}).get("maxTemperature", {}).get("uom", "")
    use_date = target_date or datetime.now(UTC).date()
    for entry in values:
        valid_time_str = entry.get("validTime", "")
        if not valid_time_str:
            continue
        try:
            # validTime format: "2026-04-21T06:00:00+00:00/PT24H"
            dt_part = valid_time_str.split("/")[0]
            dt = datetime.fromisoformat(dt_part.replace("Z", "+00:00"))
            if dt.date() == use_date:
                value_c = entry.get("value")
                if value_c is None:
                    continue
                # NWS gridpoint temperatures are in Celsius (wmoUnit:degC)
                if "degC" in uom or uom == "":
                    return celsius_to_fahrenheit(float(value_c))
                return float(value_c)
        except (ValueError, TypeError):
            continue
    # Fall back to first available value if target date not matched
    try:
        first_value = values[0].get("value")
        if first_value is not None:
            if "degC" in uom or uom == "":
                return celsius_to_fahrenheit(float(first_value))
            return float(first_value)
    except (ValueError, TypeError, IndexError):
        pass
    return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def confidence_band_for(confidence: float) -> str:
    if confidence >= 0.85:
        return "high"
    if confidence >= 0.70:
        return "medium"
    return "low"


def classify_trade_regime(*, forecast_delta_f: float | None, fair_yes_dollars: Decimal) -> str:
    if forecast_delta_f is not None and abs(forecast_delta_f) <= NEAR_THRESHOLD_DELTA_F:
        return "near_threshold"
    if fair_yes_dollars <= LONGSHOT_FAIR_THRESHOLD:
        return "longshot_yes"
    if (Decimal("1.0000") - fair_yes_dollars) <= LONGSHOT_FAIR_THRESHOLD:
        return "longshot_no"
    return "standard"


def apply_trade_regime_penalty(*, fair_yes_dollars: Decimal, trade_regime: str) -> Decimal:
    if trade_regime == "near_threshold":
        edge = fair_yes_dollars - Decimal("0.5000")
        if edge == 0:
            return Decimal("0.5000")
        adjusted_edge = max(abs(edge) - NEAR_THRESHOLD_PENALTY, Decimal("0.0000"))
        direction = Decimal("1.0000") if edge > 0 else Decimal("-1.0000")
        return quantize_price(Decimal("0.5000") + (direction * adjusted_edge))
    if trade_regime == "longshot_yes":
        return quantize_price(max(Decimal("0.0000"), fair_yes_dollars - LONGSHOT_PENALTY))
    if trade_regime == "longshot_no":
        fair_no = max(Decimal("0.0000"), (Decimal("1.0000") - fair_yes_dollars) - LONGSHOT_PENALTY)
        return quantize_price(Decimal("1.0000") - fair_no)
    return quantize_price(fair_yes_dollars)


def score_weather_market(
    mapping: WeatherMarketMapping,
    forecast_payload: dict[str, Any],
    observation_payload: dict[str, Any],
    forecast_grid_payload: dict[str, Any] | None = None,
    *,
    sigma_ctx: "SigmaContext | None" = None,
) -> WeatherSignalSnapshot:
    if not mapping.supports_structured_weather or mapping.threshold_f is None:
        raise RuntimeError(f"{mapping.market_ticker} is missing structured weather configuration")

    # Derive settlement date from the observation timestamp so date comparisons remain
    # consistent whether called live or in historical replay.
    obs_ts = parse_iso_datetime(observation_payload.get("properties", {}).get("timestamp"))
    settlement_date: date | None = obs_ts.date() if obs_ts is not None else None

    # The Kalshi ticker encodes the settlement date (e.g. KXHIGHTBOS-26APR23-T58 → 2026-04-23).
    # For D-1 rooms (run the day before settlement), obs_ts is today — using it as
    # settlement_date means the model scores today's weather instead of tomorrow's.
    # Override with the ticker-encoded date whenever it parses successfully.
    _ticker_parts = mapping.market_ticker.split("-")
    if len(_ticker_parts) >= 2:
        try:
            settlement_date = datetime.strptime(_ticker_parts[1], "%y%b%d").date()
        except ValueError:
            pass

    # Layer 2: precise gridpoint max temp (unrounded Celsius→F) via forecastGridData.
    # Falls back to Layer 1 (rounded daily period) when unavailable.
    gridpoint_max_f = (
        extract_gridpoint_max_temp_f(forecast_grid_payload, target_date=settlement_date)
        if forecast_grid_payload
        else None
    )
    forecast_high_f = gridpoint_max_f if gridpoint_max_f is not None else extract_forecast_high_f(forecast_payload, target_date=settlement_date)
    using_gridpoint = gridpoint_max_f is not None

    current_temp_f = extract_current_temp_f(observation_payload)
    resolution_state = WeatherResolutionState.UNRESOLVED
    if current_temp_f is not None:
        if mapping.operator in (">", ">=") and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator in ("<", "<=") and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO

    snapshot_stand_down_reason: StandDownReason | None = None
    if resolution_state == WeatherResolutionState.LOCKED_YES:
        fair = Decimal("1.0000")
        confidence = 1.0
        summary = (
            f"Current observed temperature {current_temp_f:.1f}F has already met or exceeded "
            f"the {mapping.threshold_f:.1f}F threshold, so the contract is locked yes."
        )
    elif resolution_state == WeatherResolutionState.LOCKED_NO:
        fair = Decimal("0.0000")
        confidence = 1.0
        summary = (
            f"Current observed temperature {current_temp_f:.1f}F has already exceeded "
            f"the {mapping.threshold_f:.1f}F ceiling, so the contract is locked no."
        )
    elif forecast_high_f is None:
        fair = Decimal("0.5000")
        confidence = 0.0
        summary = "Forecast high was unavailable; standing down."
        snapshot_stand_down_reason = StandDownReason.FORECAST_UNAVAILABLE
    else:
        delta_f = forecast_high_f - mapping.threshold_f
        if mapping.operator in ("<", "<="):
            delta_f = -delta_f
        if using_gridpoint:
            # Layer 2: Gaussian CDF with calibrated monthly σ.
            # asof_ts = max(obs_ts, forecast_updated_ts) — matches the training definition.
            now_utc = datetime.now(UTC)
            month = (settlement_date or now_utc.date()).month
            forecast_updated_ts = parse_iso_datetime(forecast_payload.get("properties", {}).get("updated"))
            candidates = [t for t in (obs_ts, forecast_updated_ts) if t is not None]
            asof_ts = max(candidates) if candidates else now_utc
            resolved_ctx = sigma_ctx
            if resolved_ctx is None:
                from kalshi_bot.weather.sigma_calibration import season_for_month
                resolved_ctx = SigmaContext(
                    station=getattr(mapping, "station_id", None),
                    season_bucket=season_for_month(month),
                )
            elif resolved_ctx.season_bucket is None and resolved_ctx.station is None:
                from kalshi_bot.weather.sigma_calibration import season_for_month
                resolved_ctx = SigmaContext(
                    station=getattr(mapping, "station_id", None),
                    season_bucket=season_for_month(month),
                    lead_hours=resolved_ctx.lead_hours,
                    sigma_params=resolved_ctx.sigma_params,
                    lead_factors=resolved_ctx.lead_factors,
                    lead_correction_enabled=resolved_ctx.lead_correction_enabled,
                )
            if resolved_ctx.lead_hours is None and settlement_date is not None:
                settlement_noon = datetime(
                    settlement_date.year, settlement_date.month, settlement_date.day, 12, 0, 0, tzinfo=UTC
                )
                lead_h = (settlement_noon - asof_ts).total_seconds() / 3600
                resolved_ctx = SigmaContext(
                    station=resolved_ctx.station,
                    season_bucket=resolved_ctx.season_bucket,
                    lead_hours=lead_h,
                    sigma_params=resolved_ctx.sigma_params,
                    lead_factors=resolved_ctx.lead_factors,
                    lead_correction_enabled=resolved_ctx.lead_correction_enabled,
                )
            sigma = sigma_f_for_mapping(mapping, month, ctx=resolved_ctx)
            probability = gaussian_probability(delta_f, sigma_f=sigma)
        else:
            # Layer 1 fallback: logistic with adaptive spread.
            probability = logistic_probability(delta_f, spread_f=_adaptive_spread_f(delta_f))
        fair = quantize_price(probability)
        confidence = min(0.95, 0.45 + min(abs(delta_f) / 12, 0.35) + (0.15 if current_temp_f is not None else 0.0))

    forecast_delta_f = None
    if forecast_high_f is not None and mapping.threshold_f is not None:
        forecast_delta_f = forecast_high_f - mapping.threshold_f
        if mapping.operator in ("<", "<="):
            forecast_delta_f = -forecast_delta_f
    trade_regime = classify_trade_regime(
        forecast_delta_f=forecast_delta_f,
        fair_yes_dollars=fair,
    )
    fair = apply_trade_regime_penalty(
        fair_yes_dollars=fair,
        trade_regime=trade_regime,
    )
    confidence_band = confidence_band_for(confidence)
    if resolution_state in {WeatherResolutionState.LOCKED_YES, WeatherResolutionState.LOCKED_NO}:
        trade_regime = "standard"
    elif forecast_high_f is not None:
        layer_tag = "gridpoint" if using_gridpoint else "daily-period"
        summary = (
            f"Forecast high {forecast_high_f:.1f}F [{layer_tag}] versus threshold {mapping.threshold_f:.1f}F "
            f"implies fair yes near {fair} with confidence {confidence:.2f}."
        )
    return WeatherSignalSnapshot(
        fair_yes_dollars=fair,
        confidence=confidence,
        forecast_high_f=forecast_high_f,
        current_temp_f=current_temp_f,
        forecast_delta_f=forecast_delta_f,
        confidence_band=confidence_band,
        trade_regime=trade_regime,
        resolution_state=resolution_state,
        observation_time=obs_ts,
        forecast_updated_time=parse_iso_datetime(forecast_payload.get("properties", {}).get("updated")),
        summary=summary,
        stand_down_reason=snapshot_stand_down_reason,
    )
