from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx


OKX_ASSET_INST_IDS = {
    "BNB": "BNB-USDT-SWAP",
    "BTC": "BTC-USDT-SWAP",
    "DOGE": "DOGE-USDT-SWAP",
    "ETH": "ETH-USDT-SWAP",
    "HYPE": "HYPE-USDT-SWAP",
    "SOL": "SOL-USDT-SWAP",
    "XRP": "XRP-USDT-SWAP",
}

OKX_FUNDING_RATE_URL = "https://www.okx.com/api/v5/public/funding-rate-history"


@dataclass(slots=True)
class FundingRate:
    provider: str
    asset_symbol: str
    quote_currency: str
    settlement_ts: datetime
    funding_rate: Decimal
    realized_rate: Decimal
    payload: dict[str, Any] = field(default_factory=dict)


def _parse_okx_record(asset_symbol: str, raw: dict[str, Any]) -> FundingRate | None:
    try:
        settlement_ts = datetime.fromtimestamp(int(raw["fundingTime"]) / 1000, tz=UTC)
        funding_rate = Decimal(str(raw["fundingRate"]))
        realized_rate = Decimal(str(raw["realizedRate"]))
    except (KeyError, ValueError, TypeError):
        return None
    return FundingRate(
        provider="okx",
        asset_symbol=asset_symbol,
        quote_currency="USDT",
        settlement_ts=settlement_ts,
        funding_rate=funding_rate,
        realized_rate=realized_rate,
        payload=raw,
    )


class OkxFundingRateClient:
    def __init__(self, *, timeout_seconds: float = 20.0) -> None:
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"User-Agent": "kalshi-bot/1.0"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_history(
        self,
        asset_symbol: str,
        *,
        limit: int = 100,
        after_ts: datetime | None = None,
    ) -> list[FundingRate]:
        """Fetch historical settled funding rates.

        after_ts: if set, fetch records with settlement_ts < after_ts (paginate older).
        Returns most-recent-first order (OKX default).
        """
        inst_id = OKX_ASSET_INST_IDS.get(asset_symbol.upper())
        if not inst_id:
            return []

        params: dict[str, Any] = {"instId": inst_id, "limit": min(limit, 100)}
        if after_ts is not None:
            params["after"] = str(int(after_ts.timestamp() * 1000))

        try:
            resp = await self._client.get(OKX_FUNDING_RATE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return []

        if data.get("code") != "0":
            return []

        results = []
        for raw in data.get("data", []):
            record = _parse_okx_record(asset_symbol.upper(), raw)
            if record is not None:
                results.append(record)
        return results

    async def fetch_latest(self, asset_symbol: str) -> FundingRate | None:
        """Return the single most-recently settled funding rate for an asset."""
        records = await self.fetch_history(asset_symbol, limit=1)
        return records[0] if records else None
