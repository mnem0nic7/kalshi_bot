"""StrategyCleanupService — orchestrates Strategy C shadow sweeps (§4.1.3).

Fetches live weather observations and Kalshi quotes for every configured market,
evaluates lock-confirmation gates via evaluate_cleanup_signal(), and persists
StrategyCRoom records for signal-level audit trails.

Wired into the daemon as an optional sweep loop. Live execution stays shadow
unless the Strategy C settings explicitly graduate it.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, RiskStatus, RoomOrigin, StrategyCode, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.fixed_point import make_client_order_id, quantize_count, quantize_price
from kalshi_bot.core.schemas import ExecReceiptPayload, RoomCreate, TradeTicket
from kalshi_bot.db.models import CliStationVariance, DeploymentControl, MarketPriceHistory, StrategyCRoom
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.decision_trace import build_deterministic_decision_trace
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.counterfactuals import (
    strategy_c_fee_cents,
    strategy_c_gross_edge_cents,
    strategy_c_target_cents,
)
from kalshi_bot.services.risk import evaluate_cleanup_risk
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.services.strategy_cleanup import (
    CleanupSignal,
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
        execution_service: ExecutionService | None = None,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._kalshi = kalshi
        self._weather = weather
        self._weather_directory = weather_directory
        self._execution_service = execution_service
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
                position_context = await self._position_context(signal)
                verdict = evaluate_cleanup_risk(
                    signal,
                    control=control,
                    settings=self._settings,
                    current_position_notional_dollars=position_context["notional"],
                    current_position_side=position_context["side"],
                )
                if verdict.status == RiskStatus.APPROVED:
                    outcome = "shadow" if self._settings.strategy_c_shadow_only else "live_pending"
                else:
                    outcome = "risk_blocked"
            else:
                verdict = None
                outcome = "suppressed"
            record = await self._persist_signal(signal, execution_outcome=outcome)
            if outcome == "live_pending":
                live_outcome = await self._execute_live_signal(signal, record, control, verdict)
                await self._update_record_outcome(record.room_id, live_outcome)

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
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            return await repo.get_deployment_control(kalshi_env=self._settings.kalshi_env)

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

    async def _position_context(self, signal: CleanupSignal) -> dict[str, Any]:
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            position = await repo.get_position(
                signal.ticker,
                self._settings.kalshi_subaccount,
                kalshi_env=self._settings.kalshi_env,
            )
            await session.commit()
        if position is None:
            return {"notional": Decimal("0"), "side": None}
        return {
            "notional": Decimal(str(position.average_price_dollars or 0)) * Decimal(str(position.count_fp or 0)),
            "side": position.side,
        }

    async def _persist_signal(self, signal: CleanupSignal, *, execution_outcome: str) -> StrategyCRoom:
        contracts_requested = 0
        if execution_outcome in {"shadow", "live_pending"} and signal.target_price_cents > 0:
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
        return record

    async def _update_record_outcome(self, strategy_c_room_id: str, outcome: str) -> None:
        async with self._session_factory() as session:
            record = await session.get(StrategyCRoom, strategy_c_room_id)
            if record is not None:
                record.execution_outcome = outcome
            await session.commit()

    async def _execute_live_signal(
        self,
        signal: CleanupSignal,
        strategy_c_record: StrategyCRoom,
        control: DeploymentControl,
        verdict: Any,
    ) -> str:
        if self._execution_service is None:
            return "risk_blocked"
        if self._settings.app_shadow_mode:
            return "shadow"

        count_fp = quantize_count(Decimal(str(strategy_c_record.contracts_requested or 0)))
        if count_fp <= Decimal("0"):
            return "risk_blocked"
        yes_price = self._yes_price_for_signal(signal)
        ticket = TradeTicket(
            market_ticker=signal.ticker,
            action=TradeAction.BUY,
            side=signal.side,
            yes_price_dollars=yes_price,
            count_fp=count_fp,
            capital_bucket="safe",
            time_in_force="immediate_or_cancel",
            note=f"strategy_c_room_id={strategy_c_record.room_id}",
        )
        unit_notional = ticket.yes_price_dollars if signal.side == ContractSide.YES else Decimal("1.0000") - ticket.yes_price_dollars
        approved_notional = (unit_notional * count_fp).quantize(Decimal("0.0001"))

        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            room = await repo.create_room(
                RoomCreate(
                    name=f"strategy C {signal.ticker}",
                    market_ticker=signal.ticker,
                    prompt=f"Strategy C cleanup signal linked to {strategy_c_record.room_id}.",
                ),
                active_color=self._settings.app_color,
                shadow_mode=False,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self._settings.kalshi_env,
                room_origin=RoomOrigin.LIVE.value,
                agent_pack_version="strategy-c-deterministic",
            )
            trace_signal = self._trace_signal(signal, yes_price)
            trace_signal.candidate_trace["strategy_c_room_id"] = strategy_c_record.room_id
            await repo.save_signal(
                room_id=room.id,
                market_ticker=signal.ticker,
                fair_yes_dollars=trace_signal.fair_yes_dollars,
                edge_bps=trace_signal.edge_bps,
                confidence=trace_signal.confidence,
                summary=trace_signal.summary,
                payload={
                    "strategy_code": StrategyCode.CLEANUP.value,
                    "strategy_c_room_id": strategy_c_record.room_id,
                    "resolution_state": signal.resolution_state.value,
                    "recommended_side": signal.side.value,
                    "candidate_trace": trace_signal.candidate_trace,
                },
            )
            client_order_id = make_client_order_id(room.id, signal.ticker, ticket.nonce)
            ticket_record = await repo.save_trade_ticket(
                room.id,
                ticket,
                client_order_id,
                strategy_code=StrategyCode.CLEANUP.value,
            )
            risk_record = await repo.save_risk_verdict(
                room_id=room.id,
                ticket_id=ticket_record.id,
                status=verdict.status,
                reasons=verdict.reasons,
                approved_notional_dollars=verdict.approved_notional_dollars or approved_notional,
                approved_count_fp=verdict.approved_count_fp or count_fp,
                payload={
                    **verdict.model_dump(mode="json"),
                    "strategy_c_room_id": strategy_c_record.room_id,
                    "approved_notional_dollars": str(approved_notional),
                    "approved_count_fp": str(count_fp),
                },
            )
            lock_acquired = await repo.acquire_execution_lock(
                holder=self._settings.app_color,
                color=self._settings.app_color,
                kalshi_env=self._settings.kalshi_env,
            )
            await session.commit()

        if not lock_acquired:
            receipt = ExecReceiptPayload(
                status="lock_denied",
                client_order_id=client_order_id,
                details={"reason": "execution lock held by another deployment color"},
            )
        else:
            receipt = await self._execution_service.execute(
                room=room,
                control=control,
                ticket=ticket,
                client_order_id=client_order_id,
                fair_yes_dollars=trace_signal.fair_yes_dollars,
            )

        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                await repo.save_order(
                    ticket_id=ticket_record.id,
                    client_order_id=client_order_id,
                    market_ticker=ticket.market_ticker,
                    status=receipt.status,
                    side=ticket.side.value,
                    action=ticket.action.value,
                    yes_price_dollars=ticket.yes_price_dollars,
                    count_fp=ticket.count_fp,
                    raw={**receipt.details, "strategy_c_room_id": strategy_c_record.room_id},
                    kalshi_order_id=receipt.external_order_id,
                    kalshi_env=self._settings.kalshi_env,
                    strategy_code=StrategyCode.CLEANUP.value,
                )
            input_hash, trace_hash, trace = build_deterministic_decision_trace(
                room=room,
                signal=trace_signal,
                thresholds=SimpleNamespace(
                    strategy_c_min_edge_cents=self._settings.strategy_c_min_edge_cents,
                    strategy_c_max_order_notional_dollars=self._settings.strategy_c_max_order_notional_dollars,
                    strategy_c_max_position_notional_dollars=self._settings.strategy_c_max_position_notional_dollars,
                    strategy_c_shadow_only=self._settings.strategy_c_shadow_only,
                ),
                candidate_trace=trace_signal.candidate_trace,
                final_status=receipt.status,
                evaluation_outcome="approved" if verdict.status == RiskStatus.APPROVED else "risk_blocked",
                ticket_record=ticket_record,
                risk_verdict_record=risk_record,
                receipt=receipt,
                market_observed_at=None,
                research_observed_at=datetime.now(UTC),
                source_snapshot_ids={"strategy_c_room_id": strategy_c_record.room_id},
            )
            await repo.save_decision_trace(
                room_id=room.id,
                ticket_id=ticket_record.id,
                market_ticker=ticket.market_ticker,
                kalshi_env=self._settings.kalshi_env,
                decision_kind=trace["decision_kind"],
                path_version="strategy-c-live.v1",
                agent_pack_version="strategy-c-deterministic",
                parameter_pack_version=None,
                source_snapshot_ids=trace["source_snapshot_ids"],
                input_hash=input_hash,
                trace_hash=trace_hash,
                trace=trace,
            )
            await session.commit()
        if receipt.status == "filled":
            return "live_filled"
        if receipt.status in {"submitted", "resting", "open"}:
            return "live_submitted"
        return f"live_{receipt.status}"

    @staticmethod
    def _yes_price_for_signal(signal: CleanupSignal) -> Decimal:
        target = Decimal(str(signal.target_price_cents)) / Decimal("100")
        if signal.side == ContractSide.YES:
            return quantize_price(target)
        return quantize_price(Decimal("1.0000") - target)

    @staticmethod
    def _trace_signal(signal: CleanupSignal, yes_price: Decimal) -> StrategySignal:
        fair_yes = signal.fair_value_dollars
        side_fair = fair_yes if signal.side == ContractSide.YES else Decimal("1.0000") - fair_yes
        target = Decimal(str(signal.target_price_cents)) / Decimal("100")
        edge_bps = int(((side_fair - target) * Decimal("10000")).to_integral_value())
        return StrategySignal(
            fair_yes_dollars=fair_yes,
            confidence=1.0,
            edge_bps=edge_bps,
            recommended_action=TradeAction.BUY,
            recommended_side=signal.side,
            target_yes_price_dollars=yes_price,
            summary=f"Strategy C cleanup {signal.side.value.upper()} for locked {signal.resolution_state.value}.",
            resolution_state=signal.resolution_state,
            strategy_mode=StrategyMode.RESOLUTION_CLEANUP,
            trade_regime="strategy_c_cleanup",
            capital_bucket="safe",
            forecast_delta_f=signal.observed_max_f - signal.threshold_f,
            confidence_band="high",
            candidate_trace={
                "strategy_code": StrategyCode.CLEANUP.value,
                "strategy_c_room_id": None,
                "resolution_state": signal.resolution_state.value,
                "selected_side": signal.side.value,
                "target_side_price_dollars": str(target.quantize(Decimal("0.0001"))),
                "target_yes_price_dollars": str(yes_price),
                "edge_cents": signal.edge_cents,
            },
        )
