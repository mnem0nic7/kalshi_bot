from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


NUMERIC_INTRADAY_FEATURES: tuple[str, ...] = (
    "forecast_delta_f",
    "current_delta_f",
    "observed_high_delta_f",
    "forecast_minus_current_f",
    "forecast_minus_observed_high_f",
    "local_hour",
    "local_hour_sin",
    "local_hour_cos",
    "heating_hours_remaining",
    "after_peak_hours",
    "day_progress",
    "lead_hours",
    "forecast_revision_age_hours",
    "source_disagreement_f",
    "threshold_f",
)


@dataclass(slots=True)
class IntradayFeatureInput:
    market_ticker: str
    series_ticker: str | None
    station_id: str | None
    timezone_name: str | None
    local_market_day: str | None
    threshold_f: float
    operator: str
    asof_ts: datetime
    forecast_updated_ts: datetime | None
    settlement_ts: datetime | None
    forecast_high_f: float | None
    current_temp_f: float | None
    observed_high_so_far_f: float | None
    source_disagreement_f: float | None


def normalize_weather_operator(operator: str | None, *, market_payload: dict[str, Any] | None = None) -> str:
    raw = str(operator or "").strip().lower()
    strike_type = str(((market_payload or {}).get("market") or market_payload or {}).get("strike_type") or "").lower()
    title = str(((market_payload or {}).get("market") or market_payload or {}).get("title") or "").lower()
    rules = str(((market_payload or {}).get("market") or market_payload or {}).get("rules_primary") or "").lower()
    text = f"{title} {rules}"
    if raw in {"<", "<=", "less", "below"} or strike_type == "less" or " less than " in text or " be <" in text:
        return "<"
    if raw in {">", ">=", "greater", "above"} or strike_type == "greater" or " greater than " in text or " be >" in text:
        return ">"
    return ">"


def oriented_weather_delta(value_f: float | None, threshold_f: float, operator: str) -> float:
    if value_f is None:
        return 0.0
    if normalize_weather_operator(operator) == "<":
        return float(threshold_f) - float(value_f)
    return float(value_f) - float(threshold_f)


def threshold_bucket(threshold_f: float | int | None) -> str:
    if threshold_f is None:
        return "unknown"
    value = float(threshold_f)
    if value < 60:
        return "lt60"
    if value < 75:
        return "60s"
    if value < 85:
        return "75_84"
    if value < 95:
        return "85_94"
    return "95_plus"


def season_for_month_number(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "fall"


def daypart_for_local_hour(local_hour: float) -> str:
    if local_hour < 12:
        return "morning"
    if local_hour < 16:
        return "midday"
    return "late"


def _zoneinfo(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def parse_intraday_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def estimated_weather_settlement_ts(local_market_day: str | None, timezone_name: str | None) -> datetime | None:
    if not local_market_day:
        return None
    try:
        market_day = date.fromisoformat(local_market_day)
    except ValueError:
        return None
    timezone = _zoneinfo(timezone_name)
    # Weather markets generally resolve after the next morning's daily climate report.
    local_settlement = datetime.combine(market_day + timedelta(days=1), time(8, 0), tzinfo=timezone)
    return local_settlement.astimezone(UTC)


def intraday_feature_values(item: IntradayFeatureInput) -> dict[str, float]:
    timezone = _zoneinfo(item.timezone_name)
    asof = _as_utc(item.asof_ts) or datetime.now(UTC)
    local_dt = asof.astimezone(timezone)
    local_hour = local_dt.hour + local_dt.minute / 60 + local_dt.second / 3600
    radians = (local_hour / 24.0) * 2.0 * math.pi
    operator = normalize_weather_operator(item.operator)
    forecast_delta = oriented_weather_delta(item.forecast_high_f, item.threshold_f, operator)
    current_delta = oriented_weather_delta(item.current_temp_f, item.threshold_f, operator)
    observed_delta = oriented_weather_delta(item.observed_high_so_far_f, item.threshold_f, operator)
    forecast_revision_age = 0.0
    forecast_updated = _as_utc(item.forecast_updated_ts)
    if forecast_updated is not None:
        forecast_revision_age = max(0.0, (asof - forecast_updated).total_seconds() / 3600.0)
    settlement_ts = _as_utc(item.settlement_ts) or estimated_weather_settlement_ts(
        item.local_market_day,
        item.timezone_name,
    )
    lead_hours = max(0.0, (settlement_ts - asof).total_seconds() / 3600.0) if settlement_ts else 0.0
    market_month = local_dt.month
    if item.local_market_day:
        try:
            market_month = date.fromisoformat(item.local_market_day).month
        except ValueError:
            pass

    values: dict[str, float] = {
        "forecast_delta_f": forecast_delta,
        "current_delta_f": current_delta,
        "observed_high_delta_f": observed_delta,
        "forecast_minus_current_f": (
            float(item.forecast_high_f) - float(item.current_temp_f)
            if item.forecast_high_f is not None and item.current_temp_f is not None
            else 0.0
        ),
        "forecast_minus_observed_high_f": (
            float(item.forecast_high_f) - float(item.observed_high_so_far_f)
            if item.forecast_high_f is not None and item.observed_high_so_far_f is not None
            else 0.0
        ),
        "local_hour": local_hour,
        "local_hour_sin": math.sin(radians),
        "local_hour_cos": math.cos(radians),
        "heating_hours_remaining": max(0.0, 17.5 - local_hour),
        "after_peak_hours": max(0.0, local_hour - 16.0),
        "day_progress": min(1.0, max(0.0, local_hour / 24.0)),
        "lead_hours": lead_hours,
        "forecast_revision_age_hours": forecast_revision_age,
        "source_disagreement_f": float(item.source_disagreement_f or 0.0),
        "threshold_f": float(item.threshold_f),
        f"operator:{operator}": 1.0,
        f"series:{item.series_ticker or 'unknown'}": 1.0,
        f"station:{item.station_id or 'unknown'}": 1.0,
        f"season:{season_for_month_number(market_month)}": 1.0,
        f"threshold_bucket:{threshold_bucket(item.threshold_f)}": 1.0,
        f"daypart:{daypart_for_local_hour(local_hour)}": 1.0,
    }
    return values


def intraday_probability_from_artifact(
    artifact: dict[str, Any],
    values: dict[str, float],
) -> float | None:
    feature_names = list(artifact.get("feature_names") or [])
    coefficients = list(artifact.get("coefficients") or [])
    means = dict(artifact.get("feature_means") or {})
    scales = dict(artifact.get("feature_scales") or {})
    if not feature_names or len(feature_names) != len(coefficients):
        return None
    try:
        score = float(artifact.get("intercept") or 0.0)
        for name, coefficient in zip(feature_names, coefficients, strict=True):
            scale = float(scales.get(name) or 1.0)
            if not math.isfinite(scale) or scale == 0.0:
                scale = 1.0
            raw = float(values.get(name, 0.0))
            centered = (raw - float(means.get(name) or 0.0)) / scale
            score += float(coefficient) * centered
        probability = 1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, score))))
        return calibrate_intraday_probability(probability, artifact.get("calibration"))
    except (TypeError, ValueError, OverflowError):
        return None


def fit_intraday_calibrator(
    raw_probabilities: list[float],
    outcomes: list[int],
    *,
    isotonic_min_rows: int,
) -> dict[str, Any]:
    """Fit a probability calibrator, choosing the method by sample size.

    Deep-research finding (Niculescu-Mizil & Caruana, ICML'05; 3-0 confirmed): isotonic
    regression overfits when calibration data is scarce, while Platt (sigmoid) scaling is
    robust there. Below ``isotonic_min_rows`` calibration points we fit Platt; at/above it
    we fit isotonic. With fewer than two outcome classes neither is identifiable, so we
    return an identity passthrough.
    """

    pairs = [
        (float(p), int(o))
        for p, o in zip(raw_probabilities, outcomes, strict=False)
        if p is not None and o is not None
    ]
    n = len(pairs)
    if n == 0 or len({o for _, o in pairs}) < 2:
        return {"method": "identity", "row_count": n}

    xs = [p for p, _ in pairs]
    ys = [o for _, o in pairs]
    if n >= int(isotonic_min_rows):
        from sklearn.isotonic import IsotonicRegression

        calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        calibrator.fit(xs, ys)
        return {
            "method": "isotonic",
            "x_thresholds": [float(value) for value in calibrator.X_thresholds_],
            "y_thresholds": [float(value) for value in calibrator.y_thresholds_],
            "row_count": n,
        }

    # Platt scaling: a 1-D logistic of outcome on the raw probability.
    import numpy as np
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(max_iter=1000, solver="lbfgs")
    model.fit(np.asarray(xs, dtype=float).reshape(-1, 1), np.asarray(ys, dtype=int))
    return {
        "method": "platt",
        "coef": float(model.coef_[0][0]),
        "intercept": float(model.intercept_[0]),
        "row_count": n,
    }


def calibrate_intraday_probability(probability: float, calibration: Any) -> float:
    if not isinstance(calibration, dict):
        return max(0.0, min(1.0, float(probability)))
    method = calibration.get("method")
    if method == "platt":
        try:
            coef = float(calibration.get("coef") or 0.0)
            intercept = float(calibration.get("intercept") or 0.0)
            score = max(-35.0, min(35.0, coef * float(probability) + intercept))
            return max(0.0, min(1.0, 1.0 / (1.0 + math.exp(-score))))
        except (TypeError, ValueError, OverflowError):
            return max(0.0, min(1.0, float(probability)))
    if method != "isotonic":
        return max(0.0, min(1.0, float(probability)))
    xs = [float(value) for value in calibration.get("x_thresholds") or []]
    ys = [float(value) for value in calibration.get("y_thresholds") or []]
    if len(xs) != len(ys) or not xs:
        return max(0.0, min(1.0, float(probability)))
    p = max(0.0, min(1.0, float(probability)))
    if p <= xs[0]:
        return max(0.0, min(1.0, ys[0]))
    if p >= xs[-1]:
        return max(0.0, min(1.0, ys[-1]))
    for idx in range(1, len(xs)):
        if p > xs[idx]:
            continue
        left_x = xs[idx - 1]
        right_x = xs[idx]
        left_y = ys[idx - 1]
        right_y = ys[idx]
        if right_x == left_x:
            return max(0.0, min(1.0, right_y))
        weight = (p - left_x) / (right_x - left_x)
        return max(0.0, min(1.0, left_y + (right_y - left_y) * weight))
    return max(0.0, min(1.0, p))


def intraday_artifact_fallback_reason(
    artifact: dict[str, Any] | None,
    *,
    max_age_hours: int,
    now: datetime | None = None,
) -> str | None:
    if not artifact:
        return "missing_artifact"
    if not artifact.get("active"):
        return "inactive_artifact"
    if artifact.get("model_type") != "weather_intraday_logistic_v1":
        return "unsupported_artifact"
    if not artifact.get("feature_names") or not artifact.get("coefficients"):
        return "invalid_artifact"
    created_at = parse_intraday_datetime(artifact.get("created_at"))
    if created_at is not None:
        reference = _as_utc(now) or datetime.now(UTC)
        if reference - created_at > timedelta(hours=max(1, int(max_age_hours))):
            return "stale_artifact"
    return None
