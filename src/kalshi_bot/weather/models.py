from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class WeatherMarketMapping(BaseModel):
    market_ticker: str
    market_type: Literal["weather", "generic"] = "weather"
    display_name: str | None = None
    description: str | None = None
    research_queries: list[str] = Field(default_factory=list)
    research_urls: list[str] = Field(default_factory=list)
    station_id: str | None = None
    location_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    threshold_f: float | None = None
    operator: Literal[">=", ">", "<=", "<"] = ">="
    metric: str = "daily_high_f"
    settlement_source: str = "Kalshi market rules"

    @property
    def label(self) -> str:
        return self.display_name or self.location_name or self.market_ticker

    @property
    def supports_structured_weather(self) -> bool:
        return (
            self.market_type == "weather"
            and self.station_id is not None
            and self.location_name is not None
            and self.latitude is not None
            and self.longitude is not None
            and self.threshold_f is not None
        )
