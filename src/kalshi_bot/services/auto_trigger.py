from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

import numpy as np

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.models import MarketState
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.signal import market_quotes
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class SupervisorProtocol(Protocol):
    async def run_room(self, room_id: str, reason: str = "manual") -> None: ...


class AutoTriggerService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        supervisor: SupervisorProtocol,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.supervisor = supervisor
        self._inflight_markets: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    async def handle_market_update(self, market_ticker: str) -> None:
        if not self.settings.trigger_enable_auto_rooms:
            return
        if not self.weather_directory.supports_market_ticker(market_ticker):
            return
        if market_ticker in self._inflight_markets:
            return

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            if control.active_color != self.settings.app_color:
                await session.commit()
                return
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            thresholds = self.agent_pack_service.runtime_thresholds(pack)
            market_state = await repo.get_market_state(market_ticker)
            if market_state is None or not self._market_is_actionable(market_state, thresholds):
                await session.commit()
                return

            active_count = await repo.count_active_rooms(
                color=self.settings.app_color,
                updated_within_seconds=self.settings.trigger_active_room_stale_seconds,
            )
            if active_count >= self.settings.trigger_max_concurrent_rooms:
                await repo.log_ops_event(
                    severity="warning",
                    summary=f"Auto-trigger skipped for {market_ticker}: max concurrent rooms reached",
                    source="auto_trigger",
                    payload={"market_ticker": market_ticker},
                )
                await session.commit()
                return

            existing_room = await repo.get_latest_active_room_for_market(market_ticker)
            if existing_room is not None:
                await session.commit()
                return

            reentry_cp = await repo.get_checkpoint(f"stop_loss_reentry:{market_ticker}")
            if reentry_cp is not None:
                # Momentum-based re-entry: require sustained directional momentum
                # for stop_loss_momentum_reentry_window_seconds before allowing back in.
                prices = await repo.fetch_recent_prices(
                    market_ticker,
                    window=timedelta(seconds=self.settings.stop_loss_momentum_reentry_window_seconds),
                )
                points = [
                    (row.observed_at.timestamp(), float(row.mid_dollars))
                    for row in prices
                    if row.mid_dollars is not None
                ]
                if len(points) < 5:
                    await session.commit()
                    return
                xs = np.array([p[0] for p in points])
                ys = np.array([p[1] for p in points])
                xs = xs - xs[0]
                slope = float(np.polyfit(xs, ys, 1)[0]) * 100 * 60  # $/s → ¢/min
                if abs(slope) < abs(self.settings.stop_loss_momentum_slope_threshold_cents_per_min):
                    await session.commit()
                    return
                # Momentum is clear — allow re-entry (checkpoint remains; overwritten on next stop-loss)

            checkpoint = await repo.get_checkpoint(f"auto_trigger:{market_ticker}")
            if checkpoint is not None:
                last_triggered_at = checkpoint.payload.get("last_triggered_at")
                if last_triggered_at is not None:
                    last_trigger_time = datetime.fromisoformat(last_triggered_at)
                    cooldown = (
                        self.settings.trigger_broken_book_retry_seconds
                        if checkpoint.payload.get("book_broken")
                        else thresholds.trigger_cooldown_seconds
                    )
                    if datetime.now(UTC) - last_trigger_time < timedelta(seconds=cooldown):
                        await session.commit()
                        return

            if self._book_is_broken(market_state):
                await repo.set_checkpoint(
                    f"auto_trigger:{market_ticker}",
                    cursor=None,
                    payload={"last_triggered_at": datetime.now(UTC).isoformat(), "book_broken": True},
                )
                await session.commit()
                return

            spread_bps = self._spread_bps(market_state)
            room = await repo.create_room(
                RoomCreate(
                    name=f"auto {market_ticker}",
                    market_ticker=market_ticker,
                    prompt=f"Auto-triggered from live orderbook with spread {spread_bps}bps.",
                ),
                active_color=self.settings.app_color,
                shadow_mode=self.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                agent_pack_version=pack.version,
            )
            await repo.set_checkpoint(
                f"auto_trigger:{market_ticker}",
                cursor=None,
                payload={
                    "last_triggered_at": datetime.now(UTC).isoformat(),
                    "room_id": room.id,
                    "spread_bps": spread_bps,
                    "agent_pack_version": pack.version,
                },
            )
            await repo.log_ops_event(
                severity="info",
                summary=f"Auto-trigger launched room for {market_ticker}",
                source="auto_trigger",
                payload={"market_ticker": market_ticker, "room_id": room.id, "spread_bps": spread_bps, "agent_pack_version": pack.version},
                room_id=room.id,
            )
            await session.commit()

        self._inflight_markets.add(market_ticker)
        task = asyncio.create_task(self._run_room(market_ticker, room.id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_room(self, market_ticker: str, room_id: str) -> None:
        try:
            await self.supervisor.run_room(room_id, reason="auto_trigger")
        finally:
            self._inflight_markets.discard(market_ticker)

    async def wait_for_tasks(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    def _book_is_broken(self, market_state: MarketState) -> bool:
        quotes = market_quotes(market_state.snapshot)
        yes_ask = quotes.get("yes_ask")
        no_ask = quotes.get("no_ask")
        if yes_ask is None or no_ask is None:
            return True
        return (
            (yes_ask >= Decimal("0.9900") and no_ask >= Decimal("0.9400"))
            or (no_ask >= Decimal("0.9900") and yes_ask >= Decimal("0.9400"))
        )

    def _market_is_actionable(self, market_state: MarketState, thresholds: RuntimeThresholds) -> bool:
        yes_bid = market_state.yes_bid_dollars
        yes_ask = market_state.yes_ask_dollars
        if yes_bid is None or yes_ask is None:
            return False
        spread_bps = self._spread_bps(market_state)
        if spread_bps <= 0:
            return False
        return spread_bps <= thresholds.trigger_max_spread_bps

    @staticmethod
    def _spread_bps(market_state: MarketState) -> int:
        yes_bid = Decimal(str(market_state.yes_bid_dollars or 0))
        yes_ask = Decimal(str(market_state.yes_ask_dollars or 0))
        return int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value())
