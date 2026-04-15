from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.weather.models import WeatherMarketMapping


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _quantize_two(value: float | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


@dataclass(slots=True)
class ForecastArchiveSnapshot:
    provider: str
    model: str
    run_ts: datetime
    checkpoint_ts: datetime
    local_market_day: str
    source_id: str
    forecast_high_f: Decimal | None
    current_temp_f: Decimal | None
    payload: dict[str, Any]


@dataclass(slots=True)
class ForecastArchiveLookupResult:
    snapshot: ForecastArchiveSnapshot | None
    failure_reason: str | None = None
    reason_counts: dict[str, int] = field(default_factory=dict)
    attempts: list[dict[str, Any]] = field(default_factory=list)


class OpenMeteoForecastArchiveClient:
    PROVIDER_NAME = "open_meteo_forecast_archive"
    DEFAULT_BASE_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
    DEFAULT_MODEL = "gfs_seamless"
    DEFAULT_MODEL_LABEL = "best_match"
    RUN_CYCLE_HOURS = (0, 6, 12, 18)
    RUN_LOOKBACK_HOURS = 24

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(
            timeout=max(1.0, settings.historical_forecast_archive_timeout_seconds),
            headers={
                "User-Agent": settings.weather_user_agent,
                "Accept": "application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch_point_in_time_forecast(
        self,
        mapping: WeatherMarketMapping,
        *,
        local_market_day: str,
        checkpoint_ts: datetime,
        checkpoint_label: str | None = None,
    ) -> ForecastArchiveSnapshot | None:
        result = await self.fetch_point_in_time_forecast_with_diagnostics(
            mapping,
            local_market_day=local_market_day,
            checkpoint_ts=checkpoint_ts,
            checkpoint_label=checkpoint_label,
        )
        return result.snapshot

    async def fetch_point_in_time_forecast_with_diagnostics(
        self,
        mapping: WeatherMarketMapping,
        *,
        local_market_day: str,
        checkpoint_ts: datetime,
        checkpoint_label: str | None = None,
    ) -> ForecastArchiveLookupResult:
        if not self.settings.historical_forecast_archive_provider_enabled:
            return ForecastArchiveLookupResult(
                snapshot=None,
                failure_reason="provider_disabled",
                reason_counts={"provider_disabled": 1},
            )
        if mapping.latitude is None or mapping.longitude is None:
            return ForecastArchiveLookupResult(
                snapshot=None,
                failure_reason="mapping_missing_coordinates",
                reason_counts={"mapping_missing_coordinates": 1},
            )
        checkpoint_utc = _as_utc(checkpoint_ts)
        if checkpoint_utc is None:
            return ForecastArchiveLookupResult(
                snapshot=None,
                failure_reason="checkpoint_missing_timestamp",
                reason_counts={"checkpoint_missing_timestamp": 1},
            )

        reason_counts: Counter[str] = Counter()
        attempts: list[dict[str, Any]] = []
        for model_param, model_label in self._candidate_models():
            for candidate in self._candidate_runs(checkpoint_utc, mapping.timezone_name):
                payload, fetch_reason = await self._fetch_run(
                    mapping,
                    local_market_day=local_market_day,
                    run_ts=candidate,
                    model=model_param,
                )
                attempt: dict[str, Any] = {
                    "run_ts": candidate.isoformat(),
                    "model": model_label,
                }
                if fetch_reason is not None:
                    reason_counts[fetch_reason] += 1
                    attempt["reason"] = fetch_reason
                    attempts.append(attempt)
                    continue
                normalized, normalize_reason = self._normalize_snapshot_with_reason(
                    mapping,
                    payload=payload or {},
                    local_market_day=local_market_day,
                    checkpoint_ts=checkpoint_utc,
                    checkpoint_label=checkpoint_label,
                    model=model_label,
                    run_ts=candidate,
                )
                if normalized is not None:
                    attempt["reason"] = "selected"
                    attempts.append(attempt)
                    return ForecastArchiveLookupResult(
                        snapshot=normalized,
                        reason_counts=dict(reason_counts),
                        attempts=attempts,
                    )
                failure_reason = normalize_reason or "normalization_rejected"
                reason_counts[failure_reason] += 1
                attempt["reason"] = failure_reason
                attempts.append(attempt)

        final_reason = self._prioritized_reason(reason_counts)
        if final_reason is None:
            final_reason = "no_valid_candidate_run"
            reason_counts[final_reason] += 1
        return ForecastArchiveLookupResult(
            snapshot=None,
            failure_reason=final_reason,
            reason_counts=dict(reason_counts),
            attempts=attempts,
        )

    async def _fetch_run(
        self,
        mapping: WeatherMarketMapping,
        *,
        local_market_day: str,
        run_ts: datetime,
        model: str | None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        timezone_name = mapping.timezone_name or "UTC"
        zone = ZoneInfo(timezone_name)
        run_local = run_ts.astimezone(zone)
        params = {
            "latitude": mapping.latitude,
            "longitude": mapping.longitude,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "timezone": timezone_name,
            "forecast_days": self._forecast_days_for_run(local_market_day, run_local=run_local),
            "run": run_local.strftime("%Y-%m-%dT%H:%M"),
        }
        if model:
            params["models"] = model
        if self.settings.historical_forecast_archive_api_key:
            params["apikey"] = self.settings.historical_forecast_archive_api_key

        last_error: Exception | None = None
        retries = max(0, int(self.settings.historical_forecast_archive_max_retries))
        for attempt in range(retries + 1):
            try:
                response = await self.client.get(
                    self.settings.historical_forecast_archive_base_url or self.DEFAULT_BASE_URL,
                    params=params,
                )
                if response.status_code in {400, 404, 422}:
                    reason_map = {
                        400: "request_bad_request",
                        404: "request_not_found",
                        422: "request_unprocessable",
                    }
                    return None, reason_map.get(response.status_code, "request_rejected")
                response.raise_for_status()
                return response.json(), None
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {400, 404, 422}:
                    reason_map = {
                        400: "request_bad_request",
                        404: "request_not_found",
                        422: "request_unprocessable",
                    }
                    return None, reason_map.get(exc.response.status_code, "request_rejected")
                last_error = exc
            except httpx.HTTPError as exc:
                last_error = exc
            except (ValueError, Exception) as exc:
                # Catches json.JSONDecodeError (subclass of ValueError) when the
                # server returns a 200 with an empty or non-JSON body.
                last_error = exc
            if attempt < retries:
                await asyncio.sleep(0.25 * (attempt + 1))
        if last_error is not None:
            return None, "request_http_error"
        return None, "request_unavailable"

    def _normalize_snapshot(
        self,
        mapping: WeatherMarketMapping,
        *,
        payload: dict[str, Any],
        local_market_day: str,
        checkpoint_ts: datetime,
        checkpoint_label: str | None,
        model: str,
        run_ts: datetime,
    ) -> ForecastArchiveSnapshot | None:
        snapshot, _ = self._normalize_snapshot_with_reason(
            mapping,
            payload=payload,
            local_market_day=local_market_day,
            checkpoint_ts=checkpoint_ts,
            checkpoint_label=checkpoint_label,
            model=model,
            run_ts=run_ts,
        )
        return snapshot

    def _normalize_snapshot_with_reason(
        self,
        mapping: WeatherMarketMapping,
        *,
        payload: dict[str, Any],
        local_market_day: str,
        checkpoint_ts: datetime,
        checkpoint_label: str | None,
        model: str,
        run_ts: datetime,
    ) -> tuple[ForecastArchiveSnapshot | None, str | None]:
        if run_ts > checkpoint_ts:
            return None, "future_run_rejected"
        timezone_name = mapping.timezone_name or str(payload.get("timezone") or "UTC")
        zone = ZoneInfo(timezone_name)
        local_day = date.fromisoformat(local_market_day)
        checkpoint_local = checkpoint_ts.astimezone(zone)
        hourly = dict(payload.get("hourly") or {})
        times = list(hourly.get("time") or [])
        temperatures = list(hourly.get("temperature_2m") or [])
        if not times or not temperatures or len(times) != len(temperatures):
            return None, "missing_hourly_series"

        periods: list[dict[str, Any]] = []
        forecast_high_f: float | None = None
        saw_temperature = False
        for index, (time_raw, temp_raw) in enumerate(zip(times, temperatures, strict=False), start=1):
            if temp_raw in (None, ""):
                continue
            saw_temperature = True
            local_time = self._parse_local_time(time_raw, zone)
            if local_time is None or local_time.date() != local_day:
                continue
            temp_f = float(temp_raw)
            forecast_high_f = temp_f if forecast_high_f is None else max(forecast_high_f, temp_f)
            next_time = local_time + timedelta(hours=1)
            periods.append(
                {
                    "number": index,
                    "name": local_time.strftime("%-I %p"),
                    "startTime": local_time.isoformat(),
                    "endTime": next_time.isoformat(),
                    "isDaytime": 6 <= local_time.hour < 18,
                    "temperature": temp_f,
                    "temperatureUnit": "F",
                }
            )
        if not saw_temperature:
            return None, "all_temperatures_null"
        if not periods or forecast_high_f is None:
            return None, "no_local_day_periods"

        checkpoint_key = (
            checkpoint_label
            or checkpoint_ts.astimezone(UTC).strftime("%Y%m%dT%H%MZ")
        )
        run_key = run_ts.astimezone(UTC).strftime("%Y%m%dT%H%MZ")
        source_id = (
            f"{self.PROVIDER_NAME}:{model}:{mapping.series_ticker or mapping.station_id}:{local_market_day}:{checkpoint_key}:{run_key}"
        )
        normalized_payload = {
            "mapping": mapping.model_dump(mode="json"),
            "forecast": {
                "type": "Feature",
                "properties": {
                    "updated": run_ts.astimezone(UTC).isoformat(),
                    "periods": periods,
                },
            },
            "observation": {
                "type": "Feature",
                "properties": {
                    "timestamp": checkpoint_local.isoformat(),
                    "temperature": {
                        "unitCode": "wmoUnit:degC",
                        "value": None,
                    },
                },
            },
            "_external_archive": {
                "provider": self.PROVIDER_NAME,
                "provider_label": "Open-Meteo Archived Forecast",
                "provider_url": "https://open-meteo.com/en/docs/single-runs-api",
                "base_url": self.settings.historical_forecast_archive_base_url or self.DEFAULT_BASE_URL,
                "model": model,
                "run_ts": run_ts.astimezone(UTC).isoformat(),
                "checkpoint_ts": checkpoint_ts.astimezone(UTC).isoformat(),
                "checkpoint_label": checkpoint_label,
                "local_market_day": local_market_day,
                "station_id": mapping.station_id,
                "series_ticker": mapping.series_ticker,
                "raw_payload": payload,
            },
        }
        return (
            ForecastArchiveSnapshot(
                provider=self.PROVIDER_NAME,
                model=model,
                run_ts=run_ts.astimezone(UTC),
                checkpoint_ts=checkpoint_ts.astimezone(UTC),
                local_market_day=local_market_day,
                source_id=source_id,
                forecast_high_f=_quantize_two(forecast_high_f),
                current_temp_f=None,
                payload=normalized_payload,
            ),
            None,
        )

    @classmethod
    def _candidate_runs(cls, checkpoint_ts: datetime, timezone_name: str | None) -> list[datetime]:
        zone = ZoneInfo(timezone_name or "UTC")
        checkpoint_local = checkpoint_ts.astimezone(zone).replace(minute=0, second=0, microsecond=0)
        lookback_start = checkpoint_local - timedelta(hours=cls.RUN_LOOKBACK_HOURS)
        candidates_local: list[datetime] = []
        cursor = checkpoint_local
        while cursor >= lookback_start:
            if cursor.hour in cls.RUN_CYCLE_HOURS:
                candidates_local.append(cursor)
            cursor -= timedelta(hours=1)

        seen: set[datetime] = set()
        candidates: list[datetime] = []
        for candidate_local in candidates_local:
            candidate_utc = candidate_local.astimezone(UTC)
            if candidate_utc in seen:
                continue
            seen.add(candidate_utc)
            candidates.append(candidate_utc)
        return candidates

    def _candidate_models(self) -> list[tuple[str | None, str]]:
        preferred = (self.settings.historical_forecast_archive_model_preference or "").strip()
        candidates: list[tuple[str | None, str]] = []
        if preferred and preferred.lower() != self.DEFAULT_MODEL_LABEL:
            candidates.append((preferred, preferred))
        candidates.append((None, self.DEFAULT_MODEL_LABEL))
        return candidates

    @staticmethod
    def _forecast_days_for_run(local_market_day: str, *, run_local: datetime) -> int:
        target_day = date.fromisoformat(local_market_day)
        delta_days = (target_day - run_local.date()).days
        return max(1, min(4, delta_days + 2))

    @staticmethod
    def _prioritized_reason(reason_counts: Counter[str]) -> str | None:
        for reason in (
            "request_bad_request",
            "request_not_found",
            "request_unprocessable",
            "request_http_error",
            "request_unavailable",
            "future_run_rejected",
            "missing_hourly_series",
            "all_temperatures_null",
            "no_local_day_periods",
        ):
            if reason_counts.get(reason):
                return reason
        return next(iter(reason_counts), None)

    @staticmethod
    def _parse_local_time(value: str, zone: ZoneInfo) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=zone)
        return parsed.astimezone(zone)
