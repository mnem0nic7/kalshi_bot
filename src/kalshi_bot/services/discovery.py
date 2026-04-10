from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


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
            response = await self.kalshi.get_market(mapping.market_ticker)
            market = response.get("market", response)
            notes: list[str] = []
            status = market.get("status", "unknown")
            yes_bid = self._decimal_or_none(market.get("yes_bid_dollars"))
            yes_ask = self._decimal_or_none(market.get("yes_ask_dollars"))
            no_ask = self._decimal_or_none(market.get("no_ask_dollars"))
            if status not in {"active", "open"}:
                notes.append(f"market status is {status}")
            if not market.get("settlement_sources"):
                notes.append("settlement_sources missing from market response")
            if yes_ask is None and no_ask is None:
                notes.append("no ask liquidity detected")
            discoveries.append(
                MarketDiscovery(
                    mapping=mapping,
                    status=status,
                    close_ts=market.get("close_ts"),
                    yes_bid_dollars=yes_bid,
                    yes_ask_dollars=yes_ask,
                    no_ask_dollars=no_ask,
                    can_trade=not notes,
                    notes=notes,
                    raw=response,
                )
            )
        return discoveries

    @staticmethod
    def _decimal_or_none(value: str | None) -> Decimal | None:
        return Decimal(str(value)) if value is not None else None

