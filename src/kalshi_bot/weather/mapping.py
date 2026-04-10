from __future__ import annotations

from pathlib import Path

import yaml

from kalshi_bot.weather.models import WeatherMarketMapping


class WeatherMarketDirectory:
    def __init__(self, mappings: dict[str, WeatherMarketMapping]) -> None:
        self._mappings = mappings

    @classmethod
    def from_file(cls, path: Path) -> "WeatherMarketDirectory":
        if not path.exists():
            return cls({})
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(raw, list):
            items = raw
        else:
            items = raw.get("markets", [])
        mappings = {item["market_ticker"]: WeatherMarketMapping.model_validate(item) for item in items}
        return cls(mappings)

    def get(self, market_ticker: str) -> WeatherMarketMapping | None:
        return self._mappings.get(market_ticker)

    def require(self, market_ticker: str) -> WeatherMarketMapping:
        mapping = self.get(market_ticker)
        if mapping is None:
            raise KeyError(f"No configured market mapping for {market_ticker}")
        return mapping

    def all(self) -> list[WeatherMarketMapping]:
        return list(self._mappings.values())
