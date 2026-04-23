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
from kalshi_bot.db.models import CliStationVariance, DeploymentControl, MarketPriceHistory, StrategyCRoom
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.counterfactuals import (
    strategy_c_fee_cents,
    strategy_c_gross_edge_cents,
    strategy_c_target_cents,
)
from kalshi_bot.services.risk import evaluate_cleanup_risk
from kalshi_bot.services.strategy_cleanup import (
    CleanupSignal,
    LockState,
    LockStateTracker,
    evaluate_cleanup_signal,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import extract_current_temp_f, parse_iso_datetime

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
        control = await self._load_control()
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
            if signal is None:
                continue
            signals.append(signal)
            if signal.suppression_reason is None:
                verdict = evaluate_cleanup_risk(signal, control=control, settings=self._settings)
                outcome = "shadow" if verdict.status.value == "approved" else "risk_blocked"
            else:
                outcome = "suppressed"
            await self._persist_signal(signal, execution_outcome=outcome)

        logger.info(
            "strategy_c: sweep complete — %d evaluated, %d actionable",
            len(signals),
            sum(1 for s in signals if s.suppression_reason is None),
        )
        return signals

    async def compute_counterfactual_fill_rate(
        self,
        *,
        lookback_days: int = 30,
        latency_budget_seconds: int = 10,
    ) -> float | None:
        """Estimate the fraction of shadow signals that would have filled.

        A signal is counted as "would have filled" if at least one
        MarketPriceHistory snapshot within latency_budget_seconds of
        decision_time shows a price at or below the target:
          - YES side:  yes_ask_dollars <= target_price_cents / 100
          - NO  side:  no_ask_dollars  <= target_price_cents / 100

        Returns None when fewer than 10 shadow signals exist — avoids
        reporting 0% when there is simply no data yet.
        """
        from datetime import timedelta

        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        async with self._session_factory() as session:
            rooms_stmt = (
                select(StrategyCRoom)
                .where(
                    StrategyCRoom.execution_outcome == "shadow",
                    StrategyCRoom.decision_time >= cutoff,
                    StrategyCRoom.target_price_cents > 0,
                )
            )
            rooms = (await session.execute(rooms_stmt)).scalars().all()

            if len(rooms) < 10:
                return None

            filled = 0
            for room in rooms:
                target_dollars = room.target_price_cents / 100.0
                window_end = room.decision_time + timedelta(seconds=latency_budget_seconds)
                price_stmt = (
                    select(MarketPriceHistory)
                    .where(
                        MarketPriceHistory.market_ticker == room.ticker,
                        MarketPriceHistory.observed_at >= room.decision_time,
                        MarketPriceHistory.observed_at <= window_end,
                    )
                    .limit(50)
                )
                snapshots = (await session.execute(price_stmt)).scalars().all()
                for snap in snapshots:
                    if room.resolution_state.endswith("yes"):
                        ask = snap.yes_ask_dollars
                        if ask is not None and float(ask) <= target_dollars:
                            filled += 1
                            break
                    else:
                        # NO ask ≈ 1 - YES bid (NO contracts priced as complement)
                        bid = snap.yes_bid_dollars
                        if bid is not None and (1.0 - float(bid)) <= target_dollars:
                            filled += 1
                            break

        return filled / len(rooms)

    async def sweep_discount_sensitivity(
        self,
        *,
        discount_cents_candidates: list[float],
        lookback_days: int = 30,
        latency_budget_seconds: int = 10,
    ) -> dict[str, Any]:
        """Estimate fill rate × net EV per candidate discount (P1-3).

        For each candidate discount d ∈ ``discount_cents_candidates`` (fractional
        OK — e.g. 0.5), replays every shadow ``StrategyCRoom`` in the window
        against its persisted ``MarketPriceHistory`` with an *alternative* target
        price at d cents above/below settlement. A signal counts as filled when
        at least one price snapshot within ``latency_budget_seconds`` crossed
        the alt target on the correct side.

        Net EV per filled contract assumes fill at the alt target:
            gross = 100 - target (cents)
            fee   = kalshi taker fee at target
            net   = gross - fee

        Returns one row per candidate with fill_count, fill_rate, avg_net_ev,
        and total_net_ev_dollars (net_ev × fills — a rough total per-shadow-
        signal lower bound assuming 1 contract per fill). Also returns the
        sweep window metadata and a ``status`` marker so callers can detect
        "not enough data" explicitly.
        """
        from datetime import timedelta

        if not discount_cents_candidates:
            raise ValueError("discount_cents_candidates must be non-empty")

        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        async with self._session_factory() as session:
            rooms_stmt = (
                select(StrategyCRoom)
                .where(
                    StrategyCRoom.execution_outcome == "shadow",
                    StrategyCRoom.decision_time >= cutoff,
                    StrategyCRoom.target_price_cents > 0,
                )
            )
            rooms = (await session.execute(rooms_stmt)).scalars().all()

            if len(rooms) < 10:
                return {
                    "status": "insufficient_data",
                    "n_signals": len(rooms),
                    "lookback_days": lookback_days,
                    "latency_budget_seconds": latency_budget_seconds,
                    "rows": [],
                }

            # Preload the price snapshots window once per room to avoid running
            # one query per (room, candidate) — that's a quadratic blowup.
            snapshots_by_room: dict[str, list[MarketPriceHistory]] = {}
            for room in rooms:
                window_end = room.decision_time + timedelta(seconds=latency_budget_seconds)
                price_stmt = (
                    select(MarketPriceHistory)
                    .where(
                        MarketPriceHistory.market_ticker == room.ticker,
                        MarketPriceHistory.observed_at >= room.decision_time,
                        MarketPriceHistory.observed_at <= window_end,
                    )
                    .limit(50)
                )
                snapshots_by_room[room.room_id] = list(
                    (await session.execute(price_stmt)).scalars().all()
                )

        rows: list[dict[str, Any]] = []
        for discount_cents in discount_cents_candidates:
            filled = 0
            total_net_ev_cents = 0.0
            for room in rooms:
                target_cents = strategy_c_target_cents(
                    resolution_state=room.resolution_state,
                    discount_cents=discount_cents,
                )
                target_dollars = target_cents / 100.0
                is_yes = str(room.resolution_state).lower().endswith("yes")
                was_filled = False
                for snap in snapshots_by_room[room.room_id]:
                    if is_yes:
                        ask = snap.yes_ask_dollars
                        if ask is not None and float(ask) <= target_dollars:
                            was_filled = True
                            break
                    else:
                        bid = snap.yes_bid_dollars
                        # NO ask ≈ 1 - YES bid
                        if bid is not None and (1.0 - float(bid)) <= target_dollars:
                            was_filled = True
                            break
                if was_filled:
                    filled += 1
                    gross = strategy_c_gross_edge_cents(
                        resolution_state=room.resolution_state,
                        discount_cents=discount_cents,
                    )
                    fee = strategy_c_fee_cents(target_dollars)
                    total_net_ev_cents += gross - fee

            fill_rate = filled / len(rooms)
            avg_net_ev_cents = total_net_ev_cents / len(rooms)
            rows.append({
                "discount_cents": discount_cents,
                "n_signals": len(rooms),
                "fill_count": filled,
                "fill_rate": fill_rate,
                "avg_net_ev_cents": avg_net_ev_cents,
                "total_net_ev_dollars": total_net_ev_cents / 100.0,
            })

        return {
            "status": "ok",
            "n_signals": len(rooms),
            "lookback_days": lookback_days,
            "latency_budget_seconds": latency_budget_seconds,
            "rows": rows,
        }

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

    async def _load_control(self) -> DeploymentControl:
        async with self._session_factory() as session:
            repo = PlatformRepository(session)
            return await repo.get_deployment_control()

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

        # Use actual observation timestamp so Part C freshness gate is meaningful.
        obs_ts = parse_iso_datetime(
            observation_raw.get("properties", {}).get("timestamp")
        ) or now

        # Compute resolution state from raw ASOS observation.
        if mapping.operator in (">", ">=") and current_temp_f >= mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_YES
        elif mapping.operator in ("<", "<=") and current_temp_f > mapping.threshold_f:
            resolution_state = WeatherResolutionState.LOCKED_NO
        else:
            resolution_state = WeatherResolutionState.UNRESOLVED

        lock_state = self._lock_tracker.observe(
            station=mapping.station_id,
            observation_ts=obs_ts,
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

    async def _persist_signal(self, signal: CleanupSignal, *, execution_outcome: str) -> None:
        contracts_requested = 0
        if execution_outcome == "shadow" and signal.target_price_cents > 0:
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
