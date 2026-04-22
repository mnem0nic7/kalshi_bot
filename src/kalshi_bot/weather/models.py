from __future__ import annotations

import re
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
    daily_summary_station_id: str | None = None
    location_name: str | None = None
    timezone_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    threshold_f: float | None = None
    operator: Literal[">=", ">", "<=", "<"] = ">="
    metric: str = "daily_high_f"
    settlement_source: str = "Kalshi market rules"
    series_ticker: str | None = None
    sigma_f_by_month: dict[int, float] | None = None

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


def _clean_text(value: str | None) -> str | None:
    if not value:
        return value
    return re.sub(r"\*\*", "", value).strip()


class WeatherSeriesTemplate(BaseModel):
    series_ticker: str
    market_type: Literal["weather"] = "weather"
    display_name: str | None = None
    description: str | None = None
    research_queries: list[str] = Field(default_factory=list)
    research_urls: list[str] = Field(default_factory=list)
    station_id: str
    daily_summary_station_id: str | None = None
    location_name: str
    timezone_name: str | None = None
    latitude: float
    longitude: float
    metric: str = "daily_high_f"
    settlement_source: str = "NWS Climatological Report (Daily)"
    allowed_strike_types: list[Literal["greater", "less"]] = Field(default_factory=lambda: ["greater", "less"])
    sigma_f_by_month: dict[int, float] | None = None

    @property
    def label(self) -> str:
        return self.display_name or self.location_name or self.series_ticker

    def supports_market_ticker(self, market_ticker: str) -> bool:
        return market_ticker == self.series_ticker or market_ticker.startswith(f"{self.series_ticker}-")

    def resolve_market_stub(self, market_ticker: str) -> WeatherMarketMapping | None:
        if not self.supports_market_ticker(market_ticker):
            return None
        return WeatherMarketMapping(
            market_ticker=market_ticker,
            market_type="weather",
            display_name=self.label,
            description=self.description,
            research_queries=list(self.research_queries),
            research_urls=list(self.research_urls),
            station_id=self.station_id,
            daily_summary_station_id=self.daily_summary_station_id,
            location_name=self.location_name,
            timezone_name=self.timezone_name,
            latitude=self.latitude,
            longitude=self.longitude,
            threshold_f=None,
            operator=">=",
            metric=self.metric,
            settlement_source=self.settlement_source,
            series_ticker=self.series_ticker,
            sigma_f_by_month=self.sigma_f_by_month,
        )

    def resolve_market(self, market: dict) -> WeatherMarketMapping | None:
        ticker = str(market.get("ticker") or "")
        if not ticker or not self.supports_market_ticker(ticker):
            return None
        strike_type = str(market.get("strike_type") or "")
        if strike_type not in self.allowed_strike_types:
            return None

        threshold_f: float | None = None
        operator: Literal[">", "<"]
        if strike_type == "greater":
            raw = market.get("floor_strike")
            threshold_f = float(raw) if raw is not None else None
            operator = ">"
        else:
            raw = market.get("cap_strike")
            threshold_f = float(raw) if raw is not None else None
            operator = "<"
        if threshold_f is None:
            return None

        title = _clean_text(str(market.get("title") or "")) or self.label
        subtitle = _clean_text(str(market.get("subtitle") or "")) or self.description
        return WeatherMarketMapping(
            market_ticker=ticker,
            market_type="weather",
            display_name=title,
            description=subtitle,
            research_queries=list(self.research_queries),
            research_urls=list(self.research_urls),
            station_id=self.station_id,
            daily_summary_station_id=self.daily_summary_station_id,
            location_name=self.location_name,
            timezone_name=self.timezone_name,
            latitude=self.latitude,
            longitude=self.longitude,
            threshold_f=threshold_f,
            operator=operator,
            metric=self.metric,
            settlement_source=self.settlement_source,
            series_ticker=self.series_ticker,
            sigma_f_by_month=self.sigma_f_by_month,
        )
