from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.discovery import DiscoveryService

logger = logging.getLogger(__name__)

_SNAPSHOT_SEMAPHORE_LIMIT = 8


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        d = quantize_price(value)
        return d if d > Decimal("0") else None
    except Exception:
        return None


class MarketHistoryService:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
        discovery_service: DiscoveryService,
        *,
        retention_hours: int = 24,
    ) -> None:
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.discovery_service = discovery_service
        self.retention_hours = retention_hours
        self._last_purge_at: datetime | None = None

    async def snapshot_once(self) -> int:
        tickers = await self._snapshot_tickers()
        if not tickers:
            return 0

        sem = asyncio.Semaphore(_SNAPSHOT_SEMAPHORE_LIMIT)
        observed_at = datetime.now(UTC)

        async def _fetch(ticker: str) -> dict[str, Any] | None:
            async with sem:
                try:
                    response = await self.kalshi.get_market(ticker)
                    return response.get("market", response)
                except Exception:
                    logger.warning("market_history: failed to fetch %s", ticker, exc_info=True)
                    return None

        results = await asyncio.gather(*(_fetch(t) for t in tickers))

        written = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for market in results:
                if market is None:
                    continue
                ticker = market.get("ticker") or market.get("market_ticker") or ""
                if not ticker:
                    continue
                bid = _safe_decimal(market.get("yes_bid_dollars"))
                ask = _safe_decimal(market.get("yes_ask_dollars"))
                mid: Decimal | None = None
                if bid is not None and ask is not None:
                    mid = quantize_price((bid + ask) / Decimal("2"))
                last = _safe_decimal(market.get("last_price_dollars"))
                raw_volume = market.get("volume")
                volume = int(raw_volume) if raw_volume is not None else None
                await repo.record_market_price_snapshot(
                    market_ticker=ticker,
                    kalshi_env=self.kalshi.settings.kalshi_env,
                    yes_bid_dollars=bid,
                    yes_ask_dollars=ask,
                    mid_dollars=mid,
                    last_trade_dollars=last,
                    volume=volume,
                    observed_at=observed_at,
                )
                written += 1
            await session.commit()

        logger.info("market_history: snapshot_once wrote %d rows", written)
        return written

    async def _snapshot_tickers(self) -> list[str]:
        tickers = list(dict.fromkeys(await self.discovery_service.list_stream_markets()))
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            positions = await repo.list_positions(
                limit=5000,
                kalshi_env=self.kalshi.settings.kalshi_env,
                subaccount=self.kalshi.settings.kalshi_subaccount,
            )
            await session.commit()

        for position in positions:
            if position.market_ticker not in tickers:
                tickers.append(position.market_ticker)
        return tickers

    async def purge_once(self) -> int:
        now = datetime.now(UTC)
        if self._last_purge_at is not None and (now - self._last_purge_at) < timedelta(hours=1):
            return 0
        self._last_purge_at = now
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            deleted = await repo.purge_market_price_history(
                older_than=timedelta(hours=self.retention_hours),
            )
            await session.commit()
        if deleted:
            logger.info("market_history: purge_once removed %d rows older than %dh", deleted, self.retention_hours)
        return deleted
