from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from kalshi_bot.core.enums import StandDownReason, WeatherResolutionState
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.core.schemas import ResolvedSigma
from kalshi_bot.forecast.ensemble_fuser import EnsembleMember, SourceEnsemble, fuse_ensembles
from kalshi_bot.weather.models import WeatherMarketMapping

NEAR_THRESHOLD_DELTA_F = 2.0
LONGSHOT_FAIR_THRESHOLD = Decimal("0.0800")
NEAR_THRESHOLD_PENALTY = Decimal("0.0500")
LONGSHOT_PENALTY = Decimal("0.0150")
UNRESOLVED_FAIR_FLOOR = Decimal("0.0500")
UNRESOLVED_FAIR_CEILING = Decimal("0.9500")


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
    observed_high_so_far_f: float | None = None
    source_disagreement_f: float | None = None
    sigma_f: float | None = None
    sigma_layer: str | None = None
    residual_adjustment_f: float | None = None
    prediction_provenance: dict[str, Any] | None = None


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


# Minimum sample counts for DB-fit to win over each fallback layer (§4.2.3).
_DB_BEATS_GLOBAL_MIN_N = 100
_DB_BEATS_YAML_MIN_N = 200


@dataclass(slots=True)
class SigmaContext:
    """Pre-loaded DB-fit parameters for σ resolution. All fields optional — None → skip that layer."""
    station: str | None = None
    season_bucket: str | None = None
    lead_hours: float | None = None
    sigma_params: dict | None = None   # keyed by (station, season_bucket)
    lead_factors: dict | None = None   # keyed by lead_bucket
    lead_correction_enabled: bool = True
    sigma_calibration_enabled: bool = True
    min_samples_beats_global: int = _DB_BEATS_GLOBAL_MIN_N
    min_samples_beats_yaml: int = _DB_BEATS_YAML_MIN_N
    min_crps_improvement: float = 0.0

def sigma_f_for_mapping(
    mapping: Any,
    month: int,
    *,
    ctx: "SigmaContext | None" = None,
) -> float:
    return resolve_sigma_for_mapping(mapping, month, ctx=ctx).sigma_f


def resolve_sigma_for_mapping(
    mapping: Any,
    month: int,
    *,
    ctx: "SigmaContext | None" = None,
) -> ResolvedSigma:
    """Return the calibrated σ for a market mapping, three-layer resolver.

    Layer priority:
    1. DB-fit (station, season_bucket): requires sample_count ≥ threshold AND CRPS improvement > 0.
       Threshold is 200 when a YAML anchor exists for this month, 100 otherwise.
    2. YAML anchor: mapping.sigma_f_by_month[month].
    3. Global fallback: nws_forecast_sigma_f(month).
    """
    overrides = getattr(mapping, "sigma_f_by_month", None)
    yaml_sigma = float(overrides[month]) if (overrides and month in overrides) else None
    season_bucket = ctx.season_bucket if ctx is not None else None
    lead_bucket: str | None = None

    if (
        ctx is not None
        and ctx.sigma_calibration_enabled
        and ctx.sigma_params is not None
        and ctx.station
        and ctx.season_bucket
    ):
        params = ctx.sigma_params.get((ctx.station, ctx.season_bucket))
        if params is not None:
            n = params.get("sample_count", 0)
            crps_improvement = params.get("crps_improvement_vs_global")
            crps_ok = (crps_improvement or 0.0) > ctx.min_crps_improvement
            min_n = ctx.min_samples_beats_yaml if yaml_sigma is not None else ctx.min_samples_beats_global
            if n >= min_n and crps_ok:
                sigma = float(params["sigma_base_f"])
                lead_factor = 1.0
                if ctx.lead_correction_enabled and ctx.lead_factors and ctx.lead_hours is not None:
                    from kalshi_bot.weather.sigma_calibration import lead_bucket_for_hours
                    bucket = lead_bucket_for_hours(ctx.lead_hours)
                    lead_bucket = bucket
                    lead_factor = float(ctx.lead_factors.get(bucket, 1.0))
                    sigma *= lead_factor
                return ResolvedSigma(
                    sigma_f=max(sigma, 0.5),
                    mean_bias_f=float(params.get("mean_bias_f") or 0.0),
                    lead_factor=lead_factor,
                    source_layer="db_fit",
                    sample_count=int(n or 0),
                    crps_improvement_vs_global=(
                        float(crps_improvement) if crps_improvement is not None else None
                    ),
                    season_bucket=ctx.season_bucket,
                    lead_bucket=lead_bucket,
                )

    if yaml_sigma is not None:
        return ResolvedSigma(
            sigma_f=max(yaml_sigma, 0.5),
            source_layer="yaml",
            season_bucket=season_bucket,
            lead_bucket=lead_bucket,
        )
    return ResolvedSigma(
        sigma_f=nws_forecast_sigma_f(month),
        source_layer="global",
        season_bucket=season_bucket,
        lead_bucket=lead_bucket,
    )


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


def _ticker_settlement_date(market_ticker: str) -> date | None:
    parts = market_ticker.split("-")
    if len(parts) < 2:
        return None
    try:
        return datetime.strptime(parts[1], "%y%b%d").date()
    except ValueError:
        return None


def _observation_matches_settlement_date(
    *,
    obs_ts: datetime | None,
    settlement_date: date | None,
    timezone_name: str | None,
) -> bool:
    if obs_ts is None or settlement_date is None:
        return True
    try:
        timezone = ZoneInfo(timezone_name or "UTC")
    except ZoneInfoNotFoundError:
        timezone = UTC
    return obs_ts.astimezone(timezone).date() == settlement_date


def _forecast_run_time(forecast_payload: dict[str, Any], fallback: datetime) -> datetime:
    return parse_iso_datetime(forecast_payload.get("properties", {}).get("updated")) or fallback


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _build_forecast_ensemble(
    *,
    mapping: WeatherMarketMapping,
    target_date: date | None,
    forecast_payload: dict[str, Any],
    forecast_grid_payload: dict[str, Any] | None,
    external_forecast_members: list[dict[str, Any]] | None,
    fallback_time: datetime,
) -> tuple[float | None, dict[str, Any]]:
    members: list[EnsembleMember] = []
    run_time = _forecast_run_time(forecast_payload, fallback_time)
    station_id = mapping.station_id or mapping.series_ticker or mapping.market_ticker

    daily_high = extract_forecast_high_f(forecast_payload, target_date=target_date)
    if daily_high is not None:
        members.append(
            EnsembleMember(
                source="nws_daily_period",
                run_id=run_time.isoformat(),
                station_id=station_id,
                valid_time=run_time,
                high_temp_f=float(daily_high),
                weight=1.0,
            )
        )

    gridpoint_high = (
        extract_gridpoint_max_temp_f(forecast_grid_payload, target_date=target_date)
        if forecast_grid_payload
        else None
    )
    if gridpoint_high is not None:
        members.append(
            EnsembleMember(
                source="nws_gridpoint",
                run_id=run_time.isoformat(),
                station_id=station_id,
                valid_time=run_time,
                high_temp_f=float(gridpoint_high),
                weight=1.2,
            )
        )

    for index, raw in enumerate(external_forecast_members or []):
        high = _finite_float(raw.get("high_temp_f") or raw.get("forecast_high_f"))
        if high is None:
            continue
        valid_time = parse_iso_datetime(str(raw.get("valid_time") or raw.get("run_ts") or "")) or run_time
        source = str(raw.get("source") or raw.get("provider") or "external_forecast")
        members.append(
            EnsembleMember(
                source=source,
                run_id=str(raw.get("run_id") or raw.get("source_id") or f"{source}:{index}"),
                station_id=station_id,
                valid_time=valid_time,
                high_temp_f=high,
                weight=max(float(raw.get("weight") or 1.0), 0.01),
            )
        )

    if not members:
        return None, {
            "enabled": True,
            "status": "missing_members",
            "source_set": [],
            "source_disagreement_f": None,
        }

    source_groups = [
        SourceEnsemble(source=source, members=source_members)
        for source, source_members in _group_members_by_source(members).items()
    ]
    fused = fuse_ensembles(source_groups)
    member_values = [member.high_temp_f for member in fused.members]
    disagreement = max(member_values) - min(member_values) if len(member_values) > 1 else 0.0
    return fused.mean_f, {
        "enabled": True,
        "status": "ok",
        "mean_f": fused.mean_f,
        "ensemble_sigma_f": fused.sigma_f,
        "member_count": fused.member_count,
        "source_set": fused.source_set_used,
        "source_member_counts": fused.source_member_counts,
        "source_disagreement_f": disagreement,
        "rejected_members": fused.rejected_members,
    }


def _group_members_by_source(members: list[EnsembleMember]) -> dict[str, list[EnsembleMember]]:
    grouped: dict[str, list[EnsembleMember]] = {}
    for member in members:
        grouped.setdefault(member.source, []).append(member)
    return grouped


def _residual_model_adjustment(
    residual_model: dict[str, Any] | None,
    *,
    station_id: str | None,
    season_bucket: str | None,
    lead_hours: float | None,
    source_disagreement_f: float | None,
    forecast_revision_age_hours: float | None,
    current_temp_f: float | None,
    observed_high_so_far_f: float | None,
    forecast_high_f: float | None,
) -> tuple[float, dict[str, Any]]:
    if not residual_model or not residual_model.get("active"):
        return 0.0, {"status": "inactive"}
    feature_names = list(residual_model.get("feature_names") or [])
    coefficients = list(residual_model.get("coefficients") or [])
    if len(feature_names) != len(coefficients):
        return 0.0, {"status": "invalid_artifact"}
    values: dict[str, float] = {
        "intercept": 1.0,
        "lead_hours": float(lead_hours or 0.0),
        "source_disagreement_f": float(source_disagreement_f or 0.0),
        "forecast_revision_age_hours": float(forecast_revision_age_hours or 0.0),
        "current_minus_forecast_f": (
            float(current_temp_f - forecast_high_f)
            if current_temp_f is not None and forecast_high_f is not None
            else 0.0
        ),
        "observed_high_minus_forecast_f": (
            float(observed_high_so_far_f - forecast_high_f)
            if observed_high_so_far_f is not None and forecast_high_f is not None
            else 0.0
        ),
    }
    if station_id:
        values[f"station:{station_id}"] = 1.0
    if season_bucket:
        values[f"season:{season_bucket}"] = 1.0
    adjustment = float(residual_model.get("intercept") or 0.0)
    for name, coefficient in zip(feature_names, coefficients, strict=True):
        adjustment += values.get(str(name), 0.0) * float(coefficient)
    max_abs = float(residual_model.get("max_abs_adjustment_f") or 5.0)
    adjustment = max(-max_abs, min(max_abs, adjustment))
    return adjustment, {
        "status": "applied",
        "version": residual_model.get("version"),
        "adjustment_f": adjustment,
        "metrics": residual_model.get("metrics") or {},
    }


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


def clamp_unresolved_fair_yes(fair_yes_dollars: Decimal) -> Decimal:
    return quantize_price(min(UNRESOLVED_FAIR_CEILING, max(UNRESOLVED_FAIR_FLOOR, fair_yes_dollars)))


def score_weather_market(
    mapping: WeatherMarketMapping,
    forecast_payload: dict[str, Any],
    observation_payload: dict[str, Any],
    forecast_grid_payload: dict[str, Any] | None = None,
    *,
    sigma_ctx: "SigmaContext | None" = None,
    prediction_enabled: bool = False,
    source_ensemble_enabled: bool = True,
    source_disagreement_widen_f: float = 3.0,
    source_disagreement_stand_down_f: float = 8.0,
    source_disagreement_sigma_multiplier_max: float = 2.0,
    nowcast_high_so_far_enabled: bool = True,
    residual_model: dict[str, Any] | None = None,
    external_forecast_members: list[dict[str, Any]] | None = None,
    observed_high_so_far_f: float | None = None,
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
    ticker_settlement_date = _ticker_settlement_date(mapping.market_ticker)
    if ticker_settlement_date is not None:
        settlement_date = ticker_settlement_date

    now_utc = datetime.now(UTC)
    forecast_updated_ts = parse_iso_datetime(forecast_payload.get("properties", {}).get("updated"))
    candidates = [t for t in (obs_ts, forecast_updated_ts) if t is not None]
    asof_ts = max(candidates) if candidates else now_utc

    # Baseline: precise gridpoint max temp (unrounded Celsius→F), falling back
    # to rounded NWS daily-period high. Improved mode fuses all available sources.
    gridpoint_max_f = (
        extract_gridpoint_max_temp_f(forecast_grid_payload, target_date=settlement_date)
        if forecast_grid_payload
        else None
    )
    daily_period_high_f = extract_forecast_high_f(forecast_payload, target_date=settlement_date)
    forecast_high_f = gridpoint_max_f if gridpoint_max_f is not None else daily_period_high_f
    using_gridpoint = gridpoint_max_f is not None
    ensemble_provenance: dict[str, Any] = {
        "enabled": bool(prediction_enabled and source_ensemble_enabled),
        "status": "disabled",
        "source_disagreement_f": None,
    }
    if prediction_enabled and source_ensemble_enabled:
        ensemble_high, ensemble_provenance = _build_forecast_ensemble(
            mapping=mapping,
            target_date=settlement_date,
            forecast_payload=forecast_payload,
            forecast_grid_payload=forecast_grid_payload,
            external_forecast_members=external_forecast_members,
            fallback_time=asof_ts,
        )
        if ensemble_high is not None:
            forecast_high_f = ensemble_high
            using_gridpoint = "nws_gridpoint" in set(ensemble_provenance.get("source_set") or [])

    current_temp_f = extract_current_temp_f(observation_payload)
    current_observation_applies = _observation_matches_settlement_date(
        obs_ts=obs_ts,
        settlement_date=ticker_settlement_date,
        timezone_name=mapping.timezone_name,
    )
    effective_observed_high_so_far_f = observed_high_so_far_f
    if effective_observed_high_so_far_f is None and current_observation_applies:
        effective_observed_high_so_far_f = current_temp_f
    high_so_far_applies = observed_high_so_far_f is not None or current_observation_applies
    if (
        prediction_enabled
        and nowcast_high_so_far_enabled
        and forecast_high_f is not None
        and effective_observed_high_so_far_f is not None
        and high_so_far_applies
    ):
        forecast_high_f = max(forecast_high_f, effective_observed_high_so_far_f)
    resolution_state = WeatherResolutionState.UNRESOLVED
    if current_temp_f is not None and current_observation_applies:
        if mapping.operator == ">" and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator == ">=" and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator == "<" and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO
        elif mapping.operator == "<=" and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO

    snapshot_stand_down_reason: StandDownReason | None = None
    source_disagreement_f = ensemble_provenance.get("source_disagreement_f")
    if source_disagreement_f is not None:
        source_disagreement_f = float(source_disagreement_f)
    resolved_sigma: ResolvedSigma | None = None
    residual_adjustment_f: float | None = None
    residual_provenance: dict[str, Any] = {"status": "inactive"}
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
        locked_no_verb = "met or exceeded" if mapping.operator == "<" else "exceeded"
        summary = (
            f"Current observed temperature {current_temp_f:.1f}F has already {locked_no_verb} "
            f"the {mapping.threshold_f:.1f}F ceiling, so the contract is locked no."
        )
    elif forecast_high_f is None:
        fair = Decimal("0.5000")
        confidence = 0.0
        summary = "Forecast high was unavailable; standing down."
        snapshot_stand_down_reason = StandDownReason.FORECAST_UNAVAILABLE
    elif (
        prediction_enabled
        and source_disagreement_f is not None
        and source_disagreement_f > source_disagreement_stand_down_f
    ):
        fair = Decimal("0.5000")
        confidence = 0.0
        summary = (
            f"Forecast sources disagree by {source_disagreement_f:.1f}F, above the "
            f"{source_disagreement_stand_down_f:.1f}F stand-down threshold."
        )
        snapshot_stand_down_reason = StandDownReason.FORECAST_SOURCE_DISAGREEMENT
    else:
        month = (settlement_date or now_utc.date()).month
        from kalshi_bot.weather.sigma_calibration import season_for_month
        base_ctx = sigma_ctx or SigmaContext()
        resolved_ctx = SigmaContext(
            station=base_ctx.station or getattr(mapping, "station_id", None),
            season_bucket=base_ctx.season_bucket or season_for_month(month),
            lead_hours=base_ctx.lead_hours,
            sigma_params=base_ctx.sigma_params,
            lead_factors=base_ctx.lead_factors,
            lead_correction_enabled=base_ctx.lead_correction_enabled,
            sigma_calibration_enabled=base_ctx.sigma_calibration_enabled,
            min_samples_beats_global=base_ctx.min_samples_beats_global,
            min_samples_beats_yaml=base_ctx.min_samples_beats_yaml,
            min_crps_improvement=base_ctx.min_crps_improvement,
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
                sigma_calibration_enabled=resolved_ctx.sigma_calibration_enabled,
                min_samples_beats_global=resolved_ctx.min_samples_beats_global,
                min_samples_beats_yaml=resolved_ctx.min_samples_beats_yaml,
                min_crps_improvement=resolved_ctx.min_crps_improvement,
            )
        forecast_revision_age_hours = (
            (asof_ts - forecast_updated_ts).total_seconds() / 3600
            if forecast_updated_ts is not None
            else None
        )
        residual_delta, residual_provenance = _residual_model_adjustment(
            residual_model,
            station_id=getattr(mapping, "station_id", None),
            season_bucket=resolved_ctx.season_bucket,
            lead_hours=resolved_ctx.lead_hours,
            source_disagreement_f=source_disagreement_f,
            forecast_revision_age_hours=forecast_revision_age_hours,
            current_temp_f=current_temp_f,
            observed_high_so_far_f=effective_observed_high_so_far_f,
            forecast_high_f=forecast_high_f,
        )
        if residual_delta:
            residual_adjustment_f = residual_delta
            forecast_high_f += residual_delta
        delta_f = forecast_high_f - mapping.threshold_f
        if mapping.operator in ("<", "<="):
            delta_f = -delta_f
        if using_gridpoint:
            # Layer 2: Gaussian CDF with calibrated monthly σ.
            # asof_ts = max(obs_ts, forecast_updated_ts) — matches the training definition.
            resolved_sigma = resolve_sigma_for_mapping(mapping, month, ctx=resolved_ctx)
            sigma = resolved_sigma.sigma_f
            if prediction_enabled and source_disagreement_f is not None and source_disagreement_widen_f > 0:
                multiplier = 1.0 + max(0.0, source_disagreement_f / source_disagreement_widen_f)
                sigma *= min(source_disagreement_sigma_multiplier_max, multiplier)
            if resolved_sigma.source_layer == "db_fit" and resolved_sigma.mean_bias_f:
                delta_f -= resolved_sigma.mean_bias_f
            probability = gaussian_probability(delta_f, sigma_f=sigma)
        else:
            # Layer 1 fallback: logistic with adaptive spread.
            probability = logistic_probability(delta_f, spread_f=_adaptive_spread_f(delta_f))
        fair = quantize_price(probability)
        confidence = min(
            0.95,
            0.45 + min(abs(delta_f) / 12, 0.35) + (0.15 if current_temp_f is not None and current_observation_applies else 0.0),
        )

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
    if resolution_state == WeatherResolutionState.UNRESOLVED:
        fair = clamp_unresolved_fair_yes(fair)
    confidence_band = confidence_band_for(confidence)
    if resolution_state in {WeatherResolutionState.LOCKED_YES, WeatherResolutionState.LOCKED_NO}:
        trade_regime = "standard"
    elif forecast_high_f is not None:
        if prediction_enabled and source_ensemble_enabled and ensemble_provenance.get("status") == "ok":
            layer_tag = "ensemble"
        else:
            layer_tag = "gridpoint" if using_gridpoint else "daily-period"
        contract_phrase = "below-threshold" if mapping.operator in ("<", "<=") else "above-threshold"
        summary = (
            f"Forecast high {forecast_high_f:.1f}F [{layer_tag}] versus {contract_phrase} "
            f"{mapping.threshold_f:.1f}F contract "
            f"implies fair yes near {fair} with confidence {confidence:.2f}."
        )
    prediction_provenance = {
        "prediction_enabled": prediction_enabled,
        "forecast_high_f": forecast_high_f,
        "daily_period_high_f": daily_period_high_f,
        "gridpoint_high_f": gridpoint_max_f,
        "observed_high_so_far_f": effective_observed_high_so_far_f,
        "source_disagreement_f": source_disagreement_f,
        "ensemble": ensemble_provenance,
        "sigma": resolved_sigma.model_dump(mode="json") if resolved_sigma is not None else None,
        "residual_model": residual_provenance,
        "residual_adjustment_f": residual_adjustment_f,
        "asof_ts": asof_ts.isoformat(),
        "settlement_date": settlement_date.isoformat() if settlement_date is not None else None,
    }
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
        forecast_updated_time=forecast_updated_ts,
        summary=summary,
        stand_down_reason=snapshot_stand_down_reason,
        observed_high_so_far_f=effective_observed_high_so_far_f,
        source_disagreement_f=source_disagreement_f,
        sigma_f=resolved_sigma.sigma_f if resolved_sigma is not None else None,
        sigma_layer=resolved_sigma.source_layer if resolved_sigma is not None else None,
        residual_adjustment_f=residual_adjustment_f,
        prediction_provenance=prediction_provenance,
    )
