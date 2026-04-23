"""StrategyCleanupService — orchestrates Strategy C shadow sweeps (§4.1.3).

Fetches live weather observations and Kalshi quotes for every configured market,
evaluates lock-confirmation gates via evaluate_cleanup_signal(), and persists
StrategyCRoom records for signal-level audit trails.

Not wired into the daemon yet (Session 8). CLI-only for Session 7.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.db.models import CliStationVariance, DeploymentControl, StrategyCRoom
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.strategy_cleanup import (
    CleanupSignal,
    LockState,
    LockStateTracker,
    evaluate_cleanup_signal,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import extract_current_temp_f

logger = logging.getLogger(__name__)


class StrategyCleanupService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        weather: NWSWeatherClient,
        weather_directory: WeatherMarketDirectory,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._kalshi = kalshi
        self._weather = weather
        self._weather_directory = weather_directory
        self._lock_tracker = LockStateTracker()

    async def sweep(self) -> list[CleanupSignal]:
        """Evaluate Strategy C gates across all configured markets.

        Returns the full list of CleanupSignal objects (suppressed and actionable).
        Writes StrategyCRoom records for every evaluated signal.
        """
        if not self._settings.strategy_c_enabled and not self._settings.strategy_c_shadow_only:
            logger.info("strategy_c: disabled and not in shadow mode — skipping sweep")
            return []

        cli_variances = await self._load_cli_variances()
        mappings = self._weather_directory.all()

        signals: list[CleanupSignal] = []
        for mapping in mappings:
            if not mapping.station_id or mapping.threshold_f is None:
                continue
            try:
                signal = await self._evaluate_one(mapping, cli_variances)
            except Exception:
                logger.exception("strategy_c: error evaluating %s", mapping.market_ticker)
                continue
            if signal is not None:
                signals.append(signal)
                await self._persist_signal(signal)

        logger.info(
            "strategy_c: sweep complete — %d evaluated, %d actionable",
            len(signals),
            sum(1 for s in signals if s.suppression_reason is None),
        )
        return signals

    async def get_status(self) -> dict[str, Any]:
        """Return aggregate Strategy C metrics from StrategyCRoom records."""
        async with self._session_factory() as session:
            total_stmt = select(func.count()).select_from(StrategyCRoom)
            total = (await session.execute(total_stmt)).scalar_one()

            actionable_stmt = select(func.count()).select_from(StrategyCRoom).where(
                StrategyCRoom.execution_outcome != "suppressed"
            )
            actionable = (await session.execute(actionable_stmt)).scalar_one()

            shadow_stmt = select(func.count()).select_from(StrategyCRoom).where(
                StrategyCRoom.execution_outcome == "shadow"
            )
            shadow = (await session.execute(shadow_stmt)).scalar_one()

            recent_stmt = (
                select(StrategyCRoom)
                .order_by(StrategyCRoom.decision_time.desc())
                .limit(10)
            )
            recent = (await session.execute(recent_stmt)).scalars().all()

        return {
            "total_rooms": total,
            "actionable_signals": actionable,
            "shadow_executions": shadow,
            "strategy_c_enabled": self._settings.strategy_c_enabled,
            "strategy_c_shadow_only": self._settings.strategy_c_shadow_only,
            "lock_tracker_state": {
                station: self._lock_tracker.consecutive_confirmations(station)
                for mapping in self._weather_directory.all()
                if (station := mapping.station_id)
            },
            "recent": [
                {
                    "ticker": r.ticker,
                    "station": r.station,
                    "decision_time": r.decision_time.isoformat(),
                    "resolution_state": r.resolution_state,
                    "modeled_edge_cents": r.modeled_edge_cents,
                    "execution_outcome": r.execution_outcome,
                }
                for r in recent
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_cli_variances(self) -> dict[str, float]:
        async with self._session_factory() as session:
            rows = (await session.execute(select(CliStationVariance))).scalars().all()
        return {row.station: row.p95_abs_delta_degf for row in rows}

    async def _evaluate_one(
        self,
        mapping: WeatherMarketMapping,
        cli_variances: dict[str, float],
    ) -> CleanupSignal | None:
        now = datetime.now(UTC)

        # Fetch weather observation and Kalshi market data concurrently.
        import asyncio
        observation_raw, market_raw = await asyncio.gather(
            self._weather.get_latest_observation(mapping.station_id),
            self._kalshi.get_market(mapping.market_ticker),
        )

        current_temp_f = extract_current_temp_f(observation_raw)
        if current_temp_f is None:
            logger.debug("strategy_c: no temperature reading for %s", mapping.station_id)
            return None

        # Compute resolution state from raw ASOS observation.
        if mapping.operator in (">", ">=") and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator in ("<", "<=") and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO
        else:
            resolution_state = WeatherResolutionState.UNRESOLVED

        lock_state = self._lock_tracker.observe(
            station=mapping.station_id,
            observation_ts=now,
            observed_max_f=current_temp_f,
            threshold_f=mapping.threshold_f,
            gridpoint_forecast_f=None,
            cli_variance_degf=cli_variances.get(mapping.station_id),
        )

        # Attach observed_at so check_book_freshness can assess staleness.
        market_snapshot: dict[str, Any] = dict(market_raw)
        market_snapshot["observed_at"] = now.isoformat()

        return evaluate_cleanup_signal(
            ticker=mapping.market_ticker,
            mapping=mapping,
            resolution_state=resolution_state,
            lock_state=lock_state,
            market_snapshot=market_snapshot,
            settings=self._settings,
            reference_time=now,
        )

    async def _persist_signal(self, signal: CleanupSignal) -> None:
        execution_outcome = "suppressed" if signal.suppression_reason else "shadow"
        contracts_requested = 0
        if not signal.suppression_reason and signal.target_price_cents > 0:
            unit_cost = signal.target_price_cents / 100.0
            contracts_requested = max(1, int(
                self._settings.strategy_c_max_order_notional_dollars / unit_cost
            ))

        record = StrategyCRoom(
            ticker=signal.ticker,
            station=signal.station,
            decision_time=datetime.now(UTC),
            resolution_state=signal.resolution_state.value,
            observed_max_at_decision=signal.observed_max_f,
            threshold=signal.threshold_f,
            fair_value_dollars=signal.fair_value_dollars,
            modeled_edge_cents=signal.edge_cents,
            target_price_cents=signal.target_price_cents,
            contracts_requested=contracts_requested,
            execution_outcome=execution_outcome,
        )
        async with self._session_factory() as session:
            session.add(record)
            await session.commit()

        logger.info(
            "strategy_c: %s %s edge=%.2f¢ outcome=%s suppression=%s",
            signal.ticker,
            signal.resolution_state.value,
            signal.edge_cents,
            execution_outcome,
            signal.suppression_reason or "none",
        )
