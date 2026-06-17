"""Market-making research service: assembles the continuous loop with real IO.

NON-TRADING. Each iteration joins the data spine (active 15m markets anchored to
their floor strike + the latest consolidated spot) and logs it; the eval stage
periodically runs the analytic vol fair-value OOS evaluation (the proven
`evaluate_vol_fair_value`). Live multi-venue WS book reconstruction (plan §4.1)
is the documented Stage-1 enhancement; v1 reuses the already-collected spot so
the spine is verifiable without new WS infra.
"""
from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import CryptoForecastService, CryptoMarketService, normalize_asset_symbols
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.mm.data_spine import VenueQuote, build_market_tick, consolidate_spot, seconds_to_close
from kalshi_bot.mm.fair_value import fair_up_normal, infer_step_seconds, realized_vol
from kalshi_bot.mm.loop import MarketMakingLoop, MmLoopConfig
from kalshi_bot.mm.storage import TickStore

logger = logging.getLogger(__name__)

_VOL_WINDOW = 33
_DEFAULT_MM_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE")


def resolve_mm_assets(raw: str | list[str] | None) -> list[str]:
    """Parse the configured assets (a comma-separated string or a list) into
    normalized symbols, falling back to the default 7 when empty."""
    items = raw.split(",") if isinstance(raw, str) else list(raw or [])
    resolved = normalize_asset_symbols([str(item).strip() for item in items if str(item).strip()])
    return resolved or list(_DEFAULT_MM_ASSETS)


class MarketMakingResearchService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: Any,
        market_service: CryptoMarketService,
        forecast_service: CryptoForecastService,
        tick_store: TickStore | None = None,
        frequency: str = "15m",
        eval_interval_seconds: float = 900.0,
        idle_seconds: float = 5.0,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.forecast_service = forecast_service
        self.frequency = frequency
        self.assets = resolve_mm_assets(settings.crypto_model_nightly_assets)
        self.store = tick_store or TickStore(base_dir=settings.mm_data_dir)
        self._config = MmLoopConfig(eval_interval_seconds=eval_interval_seconds, idle_seconds=idle_seconds)

    async def _collect_ticks(self) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        now_ts = now.timestamp()
        try:
            markets = await self.market_service.discover_markets(
                frequency=self.frequency, status="open", asset_symbols=self.assets or None
            )
        except Exception:
            logger.exception("mm: market discovery failed")
            return []
        if not markets:
            return []
        spot_by_asset: dict[str, tuple] = {}
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for asset in {m.asset_symbol for m in markets}:
                try:
                    rows = await repo.list_crypto_spot_ohlc(
                        asset_symbol=asset, kalshi_env=self.settings.kalshi_env, limit=_VOL_WINDOW
                    )
                except Exception:
                    continue
                ordered = sorted(rows, key=lambda r: getattr(r, "end_ts", 0) or 0)
                closes = [r.close_dollars for r in ordered if r.close_dollars is not None]
                if not closes:
                    continue
                stamps = [getattr(r, "end_ts", None) for r in ordered]
                step = infer_step_seconds([s.timestamp() for s in stamps if s is not None]) or 60.0
                spot_by_asset[asset] = (closes[-1], realized_vol(closes), step)
        ticks: list[dict[str, Any]] = []
        for market in markets:
            strike = market.target_price_dollars
            spot_info = spot_by_asset.get(market.asset_symbol)
            if strike is None or strike <= 0 or spot_info is None:
                continue
            spot_price, sigma, step_seconds = spot_info
            spot = consolidate_spot([VenueQuote(venue="coinbase", price=spot_price, age_seconds=0.0)])
            if spot is None or market.close_time is None:
                continue
            ttc = seconds_to_close(now_ts=now_ts, close_ts=market.close_time.timestamp())
            tick = build_market_tick(
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                floor_strike=strike,
                spot=spot,
                yes_bid=market.yes_bid_dollars,
                yes_ask=market.yes_ask_dollars,
                bid_size=None,
                ask_size=None,
                seconds_to_close=ttc,
                observed_ts=now_ts,
            )
            tick["realized_vol"] = None if sigma is None else str(sigma)
            tick["step_interval_seconds"] = step_seconds
            fair = (
                fair_up_normal(
                    spot=spot.price,
                    strike=strike,
                    sigma=sigma,
                    seconds_to_close=ttc,
                    step_interval_seconds=step_seconds,
                )
                if sigma is not None
                else None
            )
            tick["fair_up"] = None if fair is None else str(fair)
            ticks.append(tick)
        return ticks

    async def _store_ticks(self, ticks: list[dict[str, Any]]) -> None:
        partition = datetime.now(UTC).strftime("%Y-%m-%d")
        self.store.append(ticks, partition=partition)

    async def _run_eval(self) -> Any:
        try:
            report = await self.forecast_service.evaluate_vol_fair_value(frequency=self.frequency)
            beats = sum(
                1 for a in (report.get("assets") or {}).values() if isinstance(a, dict) and a.get("beats_mid_brier")
            )
            logger.info("mm: vol-eval ran freq=%s assets=%d beats_mid=%d", self.frequency, len(report.get("assets") or {}), beats)
            return report
        except Exception:
            logger.exception("mm: vol-eval failed")
            return None

    def build_loop(self) -> MarketMakingLoop:
        return MarketMakingLoop(
            collect_ticks=self._collect_ticks,
            store_ticks=self._store_ticks,
            run_eval=self._run_eval,
            clock=time.monotonic,
            config=self._config,
        )

    async def run(self, *, stop_event=None) -> None:
        logger.info(
            "market-making research loop starting freq=%s assets=%s data_dir=%s (NON-TRADING)",
            self.frequency,
            self.assets,
            self.store.base_dir,
        )
        await self.build_loop().run_forever(stop_event=stop_event)
