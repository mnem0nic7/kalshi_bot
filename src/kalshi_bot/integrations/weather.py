from __future__ import annotations

from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.weather.models import WeatherMarketMapping


class NWSWeatherClient:
    BASE_URL = "https://api.weather.gov"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": settings.weather_user_agent,
                "Accept": "application/geo+json, application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _get(self, path_or_url: str) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.BASE_URL}{path_or_url}"
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    async def get_points(self, latitude: float, longitude: float) -> dict[str, Any]:
        return await self._get(f"/points/{latitude},{longitude}")

    async def get_latest_observation(self, station_id: str) -> dict[str, Any]:
        return await self._get(f"/stations/{station_id}/observations/latest")

    async def build_market_snapshot(self, mapping: WeatherMarketMapping) -> dict[str, Any]:
        if not mapping.supports_structured_weather:
            raise RuntimeError(f"{mapping.market_ticker} does not have structured weather fields configured")
        points = await self.get_points(mapping.latitude, mapping.longitude)
        forecast_url = points.get("properties", {}).get("forecast")
        if not forecast_url:
            raise RuntimeError(f"No forecast URL returned by NWS for {mapping.label}")
        forecast = await self._get(forecast_url)
        observation = await self.get_latest_observation(mapping.station_id)
        return {
            "mapping": mapping.model_dump(mode="json"),
            "points": points,
            "forecast": forecast,
            "observation": observation,
        }
