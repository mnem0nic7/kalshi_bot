from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.weather.models import WeatherMarketMapping


def celsius_to_fahrenheit(value_c: float | None) -> float | None:
    if value_c is None:
        return None
    return (value_c * 9 / 5) + 32


def extract_forecast_high_f(payload: dict[str, Any]) -> float | None:
    periods = payload.get("properties", {}).get("periods", [])
    highs: list[float] = []
    for period in periods:
        if not period.get("isDaytime", False):
            continue
        temperature = period.get("temperature")
        unit = period.get("temperatureUnit", "F")
        if temperature is None:
            continue
        highs.append(float(temperature) if unit == "F" else celsius_to_fahrenheit(float(temperature)) or 0.0)
    return max(highs) if highs else None


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
    resolution_state: WeatherResolutionState
    observation_time: datetime | None
    forecast_updated_time: datetime | None
    summary: str


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def score_weather_market(mapping: WeatherMarketMapping, forecast_payload: dict[str, Any], observation_payload: dict[str, Any]) -> WeatherSignalSnapshot:
    if not mapping.supports_structured_weather or mapping.threshold_f is None:
        raise RuntimeError(f"{mapping.market_ticker} is missing structured weather configuration")
    forecast_high_f = extract_forecast_high_f(forecast_payload)
    current_temp_f = extract_current_temp_f(observation_payload)
    resolution_state = WeatherResolutionState.UNRESOLVED
    if current_temp_f is not None:
        if mapping.operator in (">", ">=") and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator in ("<", "<=") and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO

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
        confidence = 0.2
        summary = "Forecast high was unavailable, so the model defaulted to neutral pricing."
    else:
        delta_f = forecast_high_f - mapping.threshold_f
        if mapping.operator in ("<", "<="):
            delta_f = -delta_f
        adaptive_spread = _adaptive_spread_f(delta_f)
        probability = logistic_probability(delta_f, spread_f=adaptive_spread)
        fair = quantize_price(probability)
        confidence = min(0.95, 0.45 + min(abs(delta_f) / 12, 0.35) + (0.15 if current_temp_f is not None else 0.0))
        summary = (
            f"Forecast high {forecast_high_f:.1f}F versus threshold {mapping.threshold_f:.1f}F "
            f"implies fair yes near {fair} with confidence {confidence:.2f}."
        )
    return WeatherSignalSnapshot(
        fair_yes_dollars=fair,
        confidence=confidence,
        forecast_high_f=forecast_high_f,
        current_temp_f=current_temp_f,
        resolution_state=resolution_state,
        observation_time=parse_iso_datetime(observation_payload.get("properties", {}).get("timestamp")),
        forecast_updated_time=parse_iso_datetime(forecast_payload.get("properties", {}).get("updated")),
        summary=summary,
    )
