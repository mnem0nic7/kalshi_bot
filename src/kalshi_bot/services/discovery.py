from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping, WeatherSeriesTemplate


@dataclass(slots=True)
class MarketDiscovery:
    mapping: WeatherMarketMapping
    status: str
    close_ts: int | None
    yes_bid_dollars: Decimal | None
    yes_ask_dollars: Decimal | None
    no_ask_dollars: Decimal | None
    can_trade: bool
    notes: list[str]
    raw: dict


class DiscoveryService:
    def __init__(self, kalshi: KalshiClient, directory: WeatherMarketDirectory) -> None:
        self.kalshi = kalshi
        self.directory = directory

    async def discover_configured_markets(self) -> list[MarketDiscovery]:
        discoveries: list[MarketDiscovery] = []
        for mapping in self.directory.all():
            discoveries.append(await self._discover_exact_mapping(mapping))
        for template in self.directory.templates():
            discoveries.extend(await self._discover_series_template(template))
        return discoveries

    async def list_stream_markets(self) -> list[str]:
        discoveries = await self.discover_configured_markets()
        market_tickers: list[str] = []
        for item in discoveries:
            if item.status not in {"active", "open"}:
                continue
            if item.mapping.market_ticker not in market_tickers:
                market_tickers.append(item.mapping.market_ticker)
        if market_tickers:
            return market_tickers
        return [mapping.market_ticker for mapping in self.directory.all()]

    async def _discover_exact_mapping(self, mapping: WeatherMarketMapping) -> MarketDiscovery:
        try:
            response = await self.kalshi.get_market(mapping.market_ticker)
            market = response.get("market", response)
            return self._build_discovery(mapping, market=market, raw=response)
        except Exception as exc:
            return MarketDiscovery(
                mapping=mapping,
                status="error",
                close_ts=None,
                yes_bid_dollars=None,
                yes_ask_dollars=None,
                no_ask_dollars=None,
                can_trade=False,
                notes=[f"market lookup failed: {exc}"],
                raw={"error": str(exc)},
            )

    async def _discover_series_template(self, template: WeatherSeriesTemplate) -> list[MarketDiscovery]:
        try:
            response = await self.kalshi.list_markets(series_ticker=template.series_ticker, limit=100, status="open")
        except Exception:
            return []
        discoveries: list[MarketDiscovery] = []
        for market in response.get("markets", []):
            mapping = template.resolve_market(market)
            if mapping is None:
                continue
            discoveries.append(self._build_discovery(mapping, market=market, raw={"market": market, "source": "series_template"}))
        return discoveries

    def _build_discovery(self, mapping: WeatherMarketMapping, *, market: dict, raw: dict) -> MarketDiscovery:
        notes: list[str] = []
        status = market.get("status", "unknown")
        yes_bid = self._decimal_or_none(market.get("yes_bid_dollars"))
        yes_ask = self._decimal_or_none(market.get("yes_ask_dollars"))
        no_ask = self._decimal_or_none(market.get("no_ask_dollars"))
        if status not in {"active", "open"}:
            notes.append(f"market status is {status}")
        settlement_text = market.get("settlement_sources") or market.get("rules_primary") or mapping.settlement_source
        if not settlement_text:
            notes.append("settlement details missing from market response")
        if yes_ask is None and no_ask is None:
            notes.append("no ask liquidity detected")
        return MarketDiscovery(
            mapping=mapping,
            status=status,
            close_ts=market.get("close_ts"),
            yes_bid_dollars=yes_bid,
            yes_ask_dollars=yes_ask,
            no_ask_dollars=no_ask,
            can_trade=not notes,
            notes=notes,
            raw=raw,
        )

    @staticmethod
    def _decimal_or_none(value: str | None) -> Decimal | None:
        return Decimal(str(value)) if value is not None else None
