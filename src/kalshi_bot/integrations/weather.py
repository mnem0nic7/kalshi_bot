from __future__ import annotations

import asyncio
from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.weather.models import WeatherMarketMapping


class WeatherProviderError(RuntimeError):
    def __init__(
        self,
        *,
        endpoint: str,
        error_type: str,
        message: str,
        status_code: int | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.error_type = error_type
        self.status_code = status_code
        detail = message.strip() or error_type
        if status_code is not None and str(status_code) not in detail:
            detail = f"{status_code} {detail}"
        super().__init__(f"{error_type} while fetching {endpoint}: {detail}")


class NWSWeatherClient:
    BASE_URL = "https://api.weather.gov"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(
            timeout=settings.weather_request_timeout_seconds,
            headers={
                "User-Agent": settings.weather_user_agent,
                "Accept": "application/geo+json, application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _get(self, path_or_url: str) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.BASE_URL}{path_or_url}"
        attempts = max(1, self.settings.weather_retry_attempts)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                status_code = exc.response.status_code
                if status_code < 500 or attempt >= attempts:
                    raise self._provider_error(url, exc) from exc
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout) as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise self._provider_error(url, exc) from exc

            delay = self.settings.weather_retry_base_delay_seconds * (2 ** (attempt - 1))
            if delay > 0:
                await asyncio.sleep(delay)

        if last_exc is not None:  # pragma: no cover - loop always raises on final failure
            raise self._provider_error(url, last_exc) from last_exc
        raise WeatherProviderError(endpoint=url, error_type="WeatherProviderError", message="request did not complete")

    def _provider_error(self, endpoint: str, exc: Exception) -> WeatherProviderError:
        status_code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
        message = str(exc).strip()
        if not message:
            message = type(exc).__name__
        return WeatherProviderError(
            endpoint=endpoint,
            error_type=type(exc).__name__,
            message=message,
            status_code=status_code,
        )

    async def get_points(self, latitude: float, longitude: float) -> dict[str, Any]:
        return await self._get(f"/points/{latitude},{longitude}")

    async def get_latest_observation(self, station_id: str) -> dict[str, Any]:
        return await self._get(f"/stations/{station_id}/observations/latest")

    async def get_forecast_grid_data(self, grid_data_url: str) -> dict[str, Any]:
        return await self._get(grid_data_url)

    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict[str, Any]:
        if not mapping.supports_structured_weather:
            raise RuntimeError(f"{mapping.market_ticker} does not have structured weather fields configured")
        points = await self.get_points(mapping.latitude, mapping.longitude)
        props = points.get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            raise RuntimeError(f"No forecast URL returned by NWS for {mapping.label}")
        grid_data_url = props.get("forecastGridData")
        forecast, observation = await asyncio.gather(
            self._get(forecast_url),
            self.get_latest_observation(mapping.station_id),
        )
        forecast_grid: dict[str, Any] = {}
        if grid_data_url:
            try:
                forecast_grid = await self.get_forecast_grid_data(grid_data_url)
            except Exception:
                pass  # non-fatal; Layer 2 falls back to Layer 1 inputs
        return {
            "mapping": mapping.model_dump(mode="json"),
            "points": points,
            "forecast": forecast,
            "forecast_grid": forecast_grid,
            "observation": observation,
        }
