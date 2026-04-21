from __future__ import annotations

from pathlib import Path

import yaml

from kalshi_bot.weather.models import WeatherMarketMapping, WeatherSeriesTemplate


class WeatherMarketDirectory:
    def __init__(
        self,
        mappings: dict[str, WeatherMarketMapping],
        series_templates: dict[str, WeatherSeriesTemplate] | None = None,
    ) -> None:
        self._mappings = mappings
        self._series_templates = series_templates or {}

    @classmethod
    def from_file(cls, path: Path) -> "WeatherMarketDirectory":
        if not path.exists():
            return cls({})
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(raw, list):
            items = raw
            templates = []
        else:
            items = raw.get("markets", [])
            templates = raw.get("series_templates", [])
        mappings = {item["market_ticker"]: WeatherMarketMapping.model_validate(item) for item in items}
        series_templates = {item["series_ticker"]: WeatherSeriesTemplate.model_validate(item) for item in templates}
        return cls(mappings, series_templates)

    def get(self, market_ticker: str) -> WeatherMarketMapping | None:
        return self._mappings.get(market_ticker)

    def require(self, market_ticker: str) -> WeatherMarketMapping:
        mapping = self.get(market_ticker)
        if mapping is None:
            raise KeyError(f"No configured market mapping for {market_ticker}")
        return mapping

    def all(self) -> list[WeatherMarketMapping]:
        return list(self._mappings.values())

    def templates(self) -> list[WeatherSeriesTemplate]:
        return list(self._series_templates.values())

    def template_for_market_ticker(self, market_ticker: str) -> WeatherSeriesTemplate | None:
        for template in self._series_templates.values():
            if template.supports_market_ticker(market_ticker):
                return template
        return None

    def supports_market_ticker(self, market_ticker: str) -> bool:
        if market_ticker in self._mappings:
            return True
        return self.template_for_market_ticker(market_ticker) is not None

    def resolve_market_stub(self, market_ticker: str) -> WeatherMarketMapping | None:
        mapping = self.get(market_ticker)
        if mapping is not None:
            return mapping
        template = self.template_for_market_ticker(market_ticker)
        if template is None:
            return None
        return template.resolve_market_stub(market_ticker)

    def resolve_market(self, market_ticker: str, market: dict | None = None) -> WeatherMarketMapping | None:
        mapping = self.get(market_ticker)
        if mapping is not None:
            return mapping
        if market is None:
            return None
        for template in self._series_templates.values():
            resolved = template.resolve_market(market)
            if resolved is not None:
                return resolved
        return None
