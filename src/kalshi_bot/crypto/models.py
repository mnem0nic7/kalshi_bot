from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class CryptoSeries:
    series_ticker: str
    title: str | None
    category: str | None
    frequency: str
    asset_symbol: str
    raw: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "series_ticker": self.series_ticker,
            "title": self.title,
            "category": self.category,
            "frequency": self.frequency,
            "asset_symbol": self.asset_symbol,
            "raw": self.raw,
        }


@dataclass(slots=True)
class CryptoMarket:
    market_ticker: str
    series_ticker: str
    asset_symbol: str
    frequency: str = "15m"
    event_ticker: str | None = None
    title: str | None = None
    subtitle: str | None = None
    status: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    expected_expiration_time: datetime | None = None
    target_price_dollars: Decimal | None = None
    yes_bid_dollars: Decimal | None = None
    yes_ask_dollars: Decimal | None = None
    no_bid_dollars: Decimal | None = None
    no_ask_dollars: Decimal | None = None
    last_price_dollars: Decimal | None = None
    volume: int | None = None
    open_interest: int | None = None
    settlement_result: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def mid_yes_dollars(self) -> Decimal | None:
        if self.yes_bid_dollars is None or self.yes_ask_dollars is None:
            return None
        return (self.yes_bid_dollars + self.yes_ask_dollars) / Decimal("2")

    @property
    def spread_bps(self) -> int | None:
        if self.yes_bid_dollars is None or self.yes_ask_dollars is None:
            return None
        return int(((self.yes_ask_dollars - self.yes_bid_dollars) * Decimal("10000")).to_integral_value())

    def to_payload(self) -> dict[str, Any]:
        return {
            "market_domain": "crypto",
            "strategy_code": "CRYPTO_15M",
            "market_ticker": self.market_ticker,
            "series_ticker": self.series_ticker,
            "event_ticker": self.event_ticker,
            "asset_symbol": self.asset_symbol,
            "frequency": self.frequency,
            "title": self.title,
            "subtitle": self.subtitle,
            "status": self.status,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "expected_expiration_time": (
                self.expected_expiration_time.isoformat() if self.expected_expiration_time else None
            ),
            "target_price_dollars": _decimal_text(self.target_price_dollars),
            "yes_bid_dollars": _decimal_text(self.yes_bid_dollars),
            "yes_ask_dollars": _decimal_text(self.yes_ask_dollars),
            "no_bid_dollars": _decimal_text(self.no_bid_dollars),
            "no_ask_dollars": _decimal_text(self.no_ask_dollars),
            "last_price_dollars": _decimal_text(self.last_price_dollars),
            "mid_yes_dollars": _decimal_text(self.mid_yes_dollars),
            "spread_bps": self.spread_bps,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "settlement_result": self.settlement_result,
            "raw": self.raw,
        }


def _decimal_text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
