from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
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


class OpenMeteoForecastArchiveClient:
    PROVIDER_NAME = "open_meteo_forecast_archive"
    DEFAULT_BASE_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
    DEFAULT_MODEL = "gfs_seamless"
    RUN_LOOKBACK_HOURS = (0, 1, 2, 3, 6, 12, 24)

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
        if not self.settings.historical_forecast_archive_provider_enabled:
            return None
        if mapping.latitude is None or mapping.longitude is None:
            return None
        checkpoint_utc = _as_utc(checkpoint_ts)
        if checkpoint_utc is None:
            return None
        model = (self.settings.historical_forecast_archive_model_preference or self.DEFAULT_MODEL).strip() or self.DEFAULT_MODEL
        for candidate in self._candidate_runs(checkpoint_utc):
            payload = await self._fetch_run(
                mapping,
                local_market_day=local_market_day,
                run_ts=candidate,
                model=model,
            )
            if payload is None:
                continue
            normalized = self._normalize_snapshot(
                mapping,
                payload=payload,
                local_market_day=local_market_day,
                checkpoint_ts=checkpoint_utc,
                checkpoint_label=checkpoint_label,
                model=model,
                run_ts=candidate,
            )
            if normalized is not None:
                return normalized
        return None

    async def _fetch_run(
        self,
        mapping: WeatherMarketMapping,
        *,
        local_market_day: str,
        run_ts: datetime,
        model: str,
    ) -> dict[str, Any] | None:
        params = {
            "latitude": mapping.latitude,
            "longitude": mapping.longitude,
            "hourly": "temperature_2m",
            "temperature_unit": "fahrenheit",
            "timezone": mapping.timezone_name or "UTC",
            "start_date": local_market_day,
            "end_date": local_market_day,
            "models": model,
            "run": run_ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M"),
        }
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
                    return None
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {400, 404, 422}:
                    return None
                last_error = exc
            except httpx.HTTPError as exc:
                last_error = exc
            if attempt < retries:
                await asyncio.sleep(0.25 * (attempt + 1))
        if last_error is not None:
            raise last_error
        return None

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
        if run_ts > checkpoint_ts:
            return None
        timezone_name = mapping.timezone_name or str(payload.get("timezone") or "UTC")
        zone = ZoneInfo(timezone_name)
        local_day = date.fromisoformat(local_market_day)
        checkpoint_local = checkpoint_ts.astimezone(zone)
        hourly = dict(payload.get("hourly") or {})
        times = list(hourly.get("time") or [])
        temperatures = list(hourly.get("temperature_2m") or [])
        if not times or not temperatures or len(times) != len(temperatures):
            return None

        periods: list[dict[str, Any]] = []
        forecast_high_f: float | None = None
        for index, (time_raw, temp_raw) in enumerate(zip(times, temperatures, strict=False), start=1):
            if temp_raw in (None, ""):
                continue
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
        if not periods or forecast_high_f is None:
            return None

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
        return ForecastArchiveSnapshot(
            provider=self.PROVIDER_NAME,
            model=model,
            run_ts=run_ts.astimezone(UTC),
            checkpoint_ts=checkpoint_ts.astimezone(UTC),
            local_market_day=local_market_day,
            source_id=source_id,
            forecast_high_f=_quantize_two(forecast_high_f),
            current_temp_f=None,
            payload=normalized_payload,
        )

    @classmethod
    def _candidate_runs(cls, checkpoint_ts: datetime) -> list[datetime]:
        seen: set[datetime] = set()
        candidates: list[datetime] = []
        for hours_back in cls.RUN_LOOKBACK_HOURS:
            candidate = (checkpoint_ts - timedelta(hours=hours_back)).replace(minute=0, second=0, microsecond=0)
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)
        return candidates

    @staticmethod
    def _parse_local_time(value: str, zone: ZoneInfo) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=zone)
        return parsed.astimezone(zone)
