from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, ContractSide, MessageKind, RiskStatus, RoomStage, StrategyCode
from kalshi_bot.core.fixed_point import as_decimal, make_client_order_id, quantize_count
from kalshi_bot.core.metrics import ACTIVE_ROOMS, ORDERS_TOTAL, ROOM_RUNS_TOTAL
from kalshi_bot.core.schemas import ExecReceiptPayload, RiskVerdictPayload, RoomMessageCreate, RoomMessageRead, TradeTicket
from kalshi_bot.db.models import Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.historical_archive import append_weather_bundle_archive, weather_bundle_archive_metadata
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext

from kalshi_bot.services.momentum_calibration import get_active_momentum_calibration_async
from kalshi_bot.services.signal import (
    StrategySignal,
    WeatherSignalEngine,
    apply_heuristic_application_to_signal,
    apply_momentum_weight_to_signal,
    estimate_notional_dollars,
    evaluate_trade_eligibility,
    is_market_stale,
    suggested_trade_count_fp,
)
from kalshi_bot.services.risk import approved_ticket_for_verdict
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]


def _room_message_read(record) -> RoomMessageRead:
    return RoomMessageRead(
        id=record.id,
        room_id=record.room_id,
        role=record.role,
        kind=record.kind,
        content=record.content,
        payload=record.payload,
        sequence=record.sequence,
        stage=record.stage,
        created_at=record.created_at,
    )


async def _pending_post_kill_switch_reconcile(
    repo: PlatformRepository,
    control: Any,
    app_color: str,
    kalshi_env: str,
) -> str | None:
    """Return a reason string if execution must wait for a post-kill-switch reconcile, else None."""
    cleared_at_raw = (control.notes or {}).get("kill_switch_cleared_at")
    if not cleared_at_raw:
        return None
    cleared_at = datetime.fromisoformat(cleared_at_raw)
    reconcile_cp = await repo.get_checkpoint(f"daemon_reconcile:{kalshi_env}:{app_color}")
    if reconcile_cp is None:
        return "Kill switch was recently cleared; waiting for first reconcile before executing."
    reconciled_at_raw = reconcile_cp.payload.get("reconciled_at") if isinstance(reconcile_cp.payload, dict) else None
    if not reconciled_at_raw:
        return "Kill switch was recently cleared; waiting for reconcile checkpoint to carry reconciled_at."
    reconciled_at = datetime.fromisoformat(reconciled_at_raw)
    if reconciled_at < cleared_at:
        return (
            f"Kill switch cleared at {cleared_at.isoformat()}; last reconcile was at "
            f"{reconciled_at.isoformat()} — waiting for a post-clear reconcile before executing."
        )
    return None


def _research_ref_time(
    signal: "StrategySignal",
    fallback: "datetime | None",
) -> "datetime | None":
    """Return the best available research reference timestamp.

    Uses signal.weather.observation_time when present (the actual moment the
    weather was observed, more precise than dossier refresh time). Falls back
    to the caller-supplied dossier freshness timestamp.
    """
    obs = signal.weather.observation_time if signal.weather is not None else None
    return obs or fallback


class WorkflowSupervisor:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
        weather: NWSWeatherClient,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        signal_engine: WeatherSignalEngine,
        risk_engine: DeterministicRiskEngine,
        execution_service: ExecutionService,
        memory_service: MemoryService,
        historical_heuristic_service: HistoricalHeuristicService | None = None,
        research_coordinator: ResearchCoordinator,
        training_corpus_service: TrainingCorpusService,
        agents: AgentSuite,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.weather = weather
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.signal_engine = signal_engine
        self.risk_engine = risk_engine
        self.execution_service = execution_service
        self.memory_service = memory_service
        self.historical_heuristic_service = historical_heuristic_service
        self.research_coordinator = research_coordinator
        self.training_corpus_service = training_corpus_service
        self.agents = agents
        self._momentum_post_processor_rate_limit: dict[tuple[str, str], datetime] = {}

    async def _try_apply_momentum_post_processor(
        self,
        signal: StrategySignal,
        *,
        repo: "PlatformRepository",
        market_ticker: str,
        bundle_age_reference: datetime | None,
    ) -> tuple[StrategySignal, str]:
        from datetime import timedelta

        from sqlalchemy.exc import DBAPIError
        try:
            params, checkpoint_exists = await get_active_momentum_calibration_async(repo, self.settings)
            price_history = await repo.fetch_recent_prices(
                market_ticker,
                kalshi_env=self.settings.kalshi_env,
                window=timedelta(minutes=60),
            )
        except (DBAPIError, asyncio.TimeoutError) as exc:
            key = (self.settings.kalshi_env, type(exc).__name__)
            now = datetime.now(UTC)
            last = self._momentum_post_processor_rate_limit.get(key)
            if last is None or (now - last).total_seconds() >= 300:
                self._momentum_post_processor_rate_limit[key] = now
                await repo.log_ops_event(
                    severity="warning",
                    summary=f"Momentum post-processor unavailable: {type(exc).__name__}",
                    source="momentum_post_processor",
                    payload={"market_ticker": market_ticker, "error": str(exc)},
                )
            return signal, "price_history_error"

        result = apply_momentum_weight_to_signal(
            signal,
            params=params,
            price_history=price_history,
            research_stale_seconds=self.settings.research_stale_seconds,
            bundle_age_reference=bundle_age_reference,
            shadow_mode=self.settings.momentum_weight_shadow_mode,
        )

        # priority: error → missing → insufficient → success
        if not checkpoint_exists:
            outcome = "calibration_missing"
        elif result.momentum_slope_cents_per_min is None:
            outcome = "insufficient_points"
        else:
            outcome = "success"

        return result, outcome

    async def _run_market_gates(
        self,
        repo: "PlatformRepository",
        signal: StrategySignal,
        market: dict[str, Any],
        market_ticker: str,
    ) -> bool:
        from kalshi_bot.core.enums import StandDownReason
        from kalshi_bot.core.fixed_point import quantize_price
        from kalshi_bot.core.schemas import TradeEligibilityVerdict

        def _d(key: str) -> Decimal | None:
            v = market.get(key)
            if v is None or v == "":
                return None
            try:
                d = quantize_price(v)
                return d if d > Decimal("0") else None
            except Exception:
                return None

        def _reject(reason: "StandDownReason", msg: str) -> bool:
            candidate_trace = dict(signal.candidate_trace or {})
            candidate_trace["eligibility_outcome"] = "pre_risk_filtered"
            candidate_trace["eligibility_stand_down_reason"] = reason.value
            if signal.eligibility is None:
                signal.eligibility = TradeEligibilityVerdict(
                    eligible=False,
                    reasons=[msg],
                    stand_down_reason=reason,
                    evaluation_outcome="pre_risk_filtered",
                    candidate_trace=candidate_trace,
                )
            else:
                signal.eligibility = signal.eligibility.model_copy(
                    update={
                        "eligible": False,
                        "reasons": list(signal.eligibility.reasons) + [msg],
                        "stand_down_reason": reason,
                        "evaluation_outcome": "pre_risk_filtered",
                        "candidate_trace": candidate_trace,
                    }
                )
            signal.stand_down_reason = reason
            signal.evaluation_outcome = "pre_risk_filtered"
            signal.candidate_trace = candidate_trace
            signal.summary = f"Stand down: {msg}"
            return False

        if signal.eligibility is not None and not signal.eligibility.eligible:
            return False

        bid = _d("yes_bid_dollars")
        ask = _d("yes_ask_dollars")

        # Gate 1: bid-ask spread > 60% of mid
        if bid is not None and ask is not None:
            mid = (bid + ask) / Decimal("2")
            if mid > Decimal("0") and (ask - bid) / mid > Decimal("0.60"):
                return _reject(
                    StandDownReason.MARKET_SPREAD_OVER_60PCT,
                    f"Bid-ask spread {((ask - bid) / mid * 100):.1f}% exceeds 60% threshold",
                )
        else:
            mid = None

        # Gate 2: edge recalculation vs market mid
        if mid is not None:
            side = signal.recommended_side
            if side is not None:
                from kalshi_bot.core.enums import ContractSide
                if side == ContractSide.YES:
                    market_edge_bps = int((signal.fair_yes_dollars - mid) * Decimal("10000"))
                else:
                    market_edge_bps = int((mid - signal.fair_yes_dollars) * Decimal("10000"))
                candidate_trace = dict(signal.candidate_trace or {})
                candidate_trace["market_mid_edge_bps"] = market_edge_bps
                signal.candidate_trace = candidate_trace
                if signal.eligibility is not None:
                    signal.eligibility = signal.eligibility.model_copy(
                        update={"candidate_trace": candidate_trace}
                    )
                if market_edge_bps <= 0:
                    return _reject(
                        StandDownReason.NEGATIVE_MARKET_EDGE,
                        f"Edge vs market mid is {market_edge_bps} bps (non-positive)",
                    )

        # Gate 3: momentum veto (reads slope pre-stamped by post-processor; None → pass per Q5)
        veto_threshold = self.settings.momentum_slope_veto_cents_per_min
        slope_cpmin = signal.momentum_slope_cents_per_min
        if (
            veto_threshold is not None
            and slope_cpmin is not None
            and signal.recommended_side is not None
        ):
            from kalshi_bot.core.enums import ContractSide
            slope_against = (
                -slope_cpmin if signal.recommended_side == ContractSide.YES else slope_cpmin
            )
            # Recompute staleness at veto time from signal — never cache on signal to stay current.
            obs_time = signal.weather.observation_time if signal.weather is not None else None
            _time_ref = obs_time.astimezone(UTC) if obs_time is not None else None
            if _time_ref is not None:
                _bundle_age_s = (datetime.now(UTC) - _time_ref).total_seconds()
                staleness_factor = min(1.0, _bundle_age_s / max(self.settings.research_stale_seconds, 1))
            else:
                staleness_factor = 0.0
            if staleness_factor >= self.settings.momentum_veto_staleness_gate and slope_against > veto_threshold:
                return _reject(
                    StandDownReason.MOMENTUM_AGAINST_TRADE,
                    f"Price momentum ({slope_cpmin:.3f} ¢/min) is against {signal.recommended_side.value.upper()} trade",
                )

        # Gate 4: volume check and size_factor — only gate if volume is explicitly reported
        raw_volume = market.get("volume")
        if raw_volume is not None:
            volume = int(raw_volume)
            if volume < 50:
                return _reject(
                    StandDownReason.VOLUME_TOO_LOW,
                    f"Market volume {volume} is below minimum threshold of 50",
                )
            signal.size_factor = min(Decimal(volume) / Decimal("50"), Decimal("1.00"))

        return True

    async def _run_deterministic_fast_path(
        self,
        *,
        repo: "PlatformRepository",
        session: Any,
        room: Room,
        control: Any,
        signal: StrategySignal,
        thresholds: Any,
        market_observed_at: datetime | None = None,
        research_fallback_time: datetime | None = None,
    ) -> None:
        research_observed_at = _research_ref_time(signal, research_fallback_time)
        receipt = ExecReceiptPayload(status="no_trade", details={})
        final_status = "no_trade"
        candidate_trace = dict(signal.candidate_trace or {})
        if signal.eligibility is not None and signal.eligibility.candidate_trace:
            candidate_trace = dict(signal.eligibility.candidate_trace)
        evaluation_outcome = (
            signal.evaluation_outcome
            or (signal.eligibility.evaluation_outcome if signal.eligibility is not None else None)
            or "no_candidate"
        )

        eligible = (
            signal.eligibility is not None
            and signal.eligibility.eligible
            and signal.recommended_action is not None
            and signal.recommended_side is not None
            and signal.target_yes_price_dollars is not None
        )

        if eligible:
            total_capital = await repo.get_total_capital_dollars(kalshi_env=room.kalshi_env)
            if total_capital is None or total_capital <= 0:
                eligible = False
            else:
                dynamic_order_cap = float(total_capital) * self.settings.risk_order_pct
                count_fp = suggested_trade_count_fp(
                    settings=self.settings,
                    signal=signal,
                    max_order_notional_dollars=dynamic_order_cap,
                    total_capital_dollars=total_capital,
                )
                if count_fp is None or count_fp <= Decimal("0"):
                    eligible = False

        loss_sensitivity_active = False
        if eligible:
            ticket = TradeTicket(
                market_ticker=room.market_ticker,
                action=signal.recommended_action,
                side=signal.recommended_side,
                yes_price_dollars=signal.target_yes_price_dollars,
                count_fp=count_fp,
                capital_bucket=signal.capital_bucket,
                time_in_force="immediate_or_cancel",
            )
            if signal.size_factor < Decimal("1.00"):
                scaled = quantize_count(ticket.count_fp * signal.size_factor)
                ticket = ticket.model_copy(update={"count_fp": scaled}) if scaled > Decimal("0") else None

            if ticket is not None:
                client_order_id = make_client_order_id(room.id, room.market_ticker, ticket.nonce)
                ticket_record = await repo.save_trade_ticket(
                    room.id,
                    ticket,
                    client_order_id,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                )
                ticker_positions = await repo.list_positions_for_ticker(
                    room.market_ticker,
                    self.settings.kalshi_subaccount,
                    kalshi_env=room.kalshi_env,
                )
                if len(ticker_positions) > 1:
                    await repo.log_ops_event(
                        severity="warning",
                        summary=f"data_inconsistency: multiple_positions_for_ticker {room.market_ticker}",
                        source="supervisor",
                        room_id=room.id,
                        kalshi_env=room.kalshi_env,
                        payload={
                            "market_ticker": room.market_ticker,
                            "position_count": len(ticker_positions),
                            "sides": [p.side for p in ticker_positions],
                        },
                    )
                open_position = max(ticker_positions, key=lambda p: p.count_fp) if ticker_positions else None
                current_position_notional = (
                    estimate_notional_dollars(
                        ContractSide(open_position.side),
                        open_position.average_price_dollars,
                        open_position.count_fp,
                    )
                    if open_position is not None
                    else Decimal("0")
                )
                position_cap = float(total_capital) * self.settings.risk_position_pct
                daily_pnl = await repo.get_daily_pnl_dollars(kalshi_env=room.kalshi_env)
                loss_sensitivity_active = False
                daily_loss_hard_blocked = False
                daily_loss_ratio = 0.0
                sensitivity_cp_key = f"loss_sensitivity_state:{room.kalshi_env}"
                prior_sensitivity_cp = await repo.get_checkpoint(sensitivity_cp_key)
                prior_sensitivity_active = (
                    prior_sensitivity_cp.payload.get("active") is True
                    if prior_sensitivity_cp is not None
                    else False
                )
                if daily_pnl is not None and float(total_capital) > 0:
                    daily_loss_ratio = float(-daily_pnl) / float(total_capital)
                    if self.settings.risk_daily_loss_pct > 0 and daily_loss_ratio >= self.settings.risk_daily_loss_pct:
                        daily_loss_hard_blocked = True
                    elif self.settings.risk_daily_loss_sensitivity_pct > 0:
                        loss_sensitivity_active = daily_loss_ratio >= self.settings.risk_daily_loss_sensitivity_pct

                if loss_sensitivity_active != prior_sensitivity_active:
                    await repo.log_ops_event(
                        severity="warning" if loss_sensitivity_active else "info",
                        summary=(
                            f"Loss sensitivity gate {'activated' if loss_sensitivity_active else 'deactivated'}: "
                            f"{daily_loss_ratio:.1%} vs {self.settings.risk_daily_loss_sensitivity_pct:.0%} threshold"
                        ),
                        source="supervisor",
                        room_id=room.id,
                        kalshi_env=room.kalshi_env,
                        payload={
                            "daily_loss_ratio": round(daily_loss_ratio, 4),
                            "sensitivity_pct": self.settings.risk_daily_loss_sensitivity_pct,
                            "active": loss_sensitivity_active,
                            "market_ticker": room.market_ticker,
                        },
                    )
                    await repo.set_checkpoint(
                        sensitivity_cp_key,
                        cursor=None,
                        payload={
                            "active": loss_sensitivity_active,
                            "changed_at": datetime.now(UTC).isoformat(),
                            "daily_loss_ratio": round(daily_loss_ratio, 4),
                        },
                    )
                    logger.warning(
                        "Loss sensitivity gate %s: %.1f%% daily loss vs %.0f%% threshold",
                        "activated" if loss_sensitivity_active else "deactivated",
                        daily_loss_ratio * 100,
                        self.settings.risk_daily_loss_sensitivity_pct * 100,
                    )

                effective_edge_bps = thresholds.risk_min_edge_bps
                effective_order_cap = dynamic_order_cap
                if loss_sensitivity_active:
                    effective_edge_bps = int(
                        effective_edge_bps * self.settings.risk_daily_loss_sensitivity_edge_multiplier
                    )
                    effective_order_cap = effective_order_cap * self.settings.risk_daily_loss_sensitivity_size_multiplier

                effective_thresholds = thresholds.__class__(
                    risk_min_edge_bps=effective_edge_bps,
                    risk_max_order_notional_dollars=effective_order_cap,
                    risk_max_position_notional_dollars=position_cap,
                    trigger_max_spread_bps=thresholds.trigger_max_spread_bps,
                    trigger_cooldown_seconds=thresholds.trigger_cooldown_seconds,
                    strategy_quality_edge_buffer_bps=thresholds.strategy_quality_edge_buffer_bps,
                    strategy_min_remaining_payout_bps=thresholds.strategy_min_remaining_payout_bps,
                    risk_safe_capital_reserve_ratio=thresholds.risk_safe_capital_reserve_ratio,
                    risk_risky_capital_max_ratio=thresholds.risk_risky_capital_max_ratio,
                )
                portfolio_bucket_snapshot = await repo.portfolio_bucket_snapshot(
                    kalshi_env=room.kalshi_env,
                    subaccount=self.settings.kalshi_subaccount,
                    total_capital_dollars=total_capital,
                    safe_capital_reserve_ratio=effective_thresholds.risk_safe_capital_reserve_ratio,
                    risky_capital_max_ratio=effective_thresholds.risk_risky_capital_max_ratio,
                )
                all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
                open_ticker_count = len({p.market_ticker for p in all_positions})
                pending_order_count_fp = await repo.get_pending_buy_count_fp(
                    room.market_ticker,
                    ticket.side.value,
                    kalshi_env=room.kalshi_env,
                )
                strategy_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    kalshi_env=room.kalshi_env,
                )
                risk_context = RiskContext(
                    market_observed_at=market_observed_at,
                    research_observed_at=research_observed_at,
                    current_position_notional_dollars=current_position_notional,
                    current_position_count_fp=open_position.count_fp if open_position is not None else Decimal("0"),
                    current_position_side=open_position.side if open_position is not None else None,
                    pending_order_count_fp=pending_order_count_fp,
                    portfolio_bucket_snapshot=portfolio_bucket_snapshot,
                    open_ticker_count=open_ticker_count,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    strategy_daily_realized_pnl_dollars=strategy_daily_pnl,
                )
                if daily_loss_hard_blocked:
                    await repo.log_ops_event(
                        severity="critical",
                        summary=(
                            f"Daily loss circuit breaker tripped: {daily_loss_ratio:.1%} loss "
                            f">= {self.settings.risk_daily_loss_pct:.0%} hard limit"
                        ),
                        source="supervisor",
                        room_id=room.id,
                        kalshi_env=room.kalshi_env,
                        payload={
                            "daily_loss_ratio": round(daily_loss_ratio, 4),
                            "hard_limit_pct": self.settings.risk_daily_loss_pct,
                            "market_ticker": room.market_ticker,
                        },
                    )
                    logger.critical(
                        "Daily loss circuit breaker tripped: %.1f%% loss >= %.0f%% hard limit (ticker=%s)",
                        daily_loss_ratio * 100,
                        self.settings.risk_daily_loss_pct * 100,
                        room.market_ticker,
                    )
                    verdict = RiskVerdictPayload(
                        status=RiskStatus.BLOCKED,
                        reasons=[
                            f"Daily loss circuit breaker: {daily_loss_ratio:.1%} loss "
                            f">= {self.settings.risk_daily_loss_pct:.0%} hard limit."
                        ],
                    )
                else:
                    verdict = self.risk_engine.evaluate(
                        room=room,
                        control=control,
                        ticket=ticket,
                        signal=signal,
                        context=risk_context,
                        thresholds=effective_thresholds,
                    )
                await repo.save_risk_verdict(
                    room_id=room.id,
                    ticket_id=ticket_record.id,
                    status=verdict.status,
                    reasons=verdict.reasons,
                    approved_notional_dollars=verdict.approved_notional_dollars,
                    approved_count_fp=verdict.approved_count_fp,
                    payload=verdict.model_dump(mode="json"),
                )
                if verdict.status == RiskStatus.APPROVED:
                    evaluation_outcome = "approved"
                    approved_ticket = approved_ticket_for_verdict(ticket, verdict)
                    await repo.update_room_stage(room.id, RoomStage.EXECUTING)
                    pending_reconcile = await _pending_post_kill_switch_reconcile(
                        repo, control, self.settings.app_color, room.kalshi_env
                    )
                    if pending_reconcile:
                        receipt = ExecReceiptPayload(
                            status="pending_reconcile_after_kill_switch_clear",
                            client_order_id=client_order_id,
                            details={"reason": pending_reconcile},
                        )
                    else:
                        lock_acquired = await repo.acquire_execution_lock(
                            holder=self.settings.app_color,
                            color=self.settings.app_color,
                            kalshi_env=room.kalshi_env,
                        )
                        if lock_acquired:
                            receipt = await self.execution_service.execute(
                                room=room,
                                control=control,
                                ticket=approved_ticket,
                                client_order_id=client_order_id,
                                fair_yes_dollars=signal.fair_yes_dollars,
                            )
                        else:
                            receipt = ExecReceiptPayload(
                                status="lock_denied",
                                client_order_id=client_order_id,
                                details={"reason": "execution lock held by another deployment color"},
                            )
                    ORDERS_TOTAL.labels(status=receipt.status).inc()
                    if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                        await repo.save_order(
                            ticket_id=ticket_record.id,
                            client_order_id=client_order_id,
                            market_ticker=approved_ticket.market_ticker,
                            status=receipt.status,
                            side=approved_ticket.side.value,
                            action=approved_ticket.action.value,
                            yes_price_dollars=approved_ticket.yes_price_dollars,
                            count_fp=approved_ticket.count_fp,
                            raw=receipt.details,
                            kalshi_order_id=receipt.external_order_id,
                            kalshi_env=room.kalshi_env,
                        )
                else:
                    evaluation_outcome = "risk_blocked"
                    receipt = ExecReceiptPayload(
                        status="blocked",
                        client_order_id=client_order_id,
                        details={
                            "reasons": verdict.reasons,
                            "evaluation_outcome": evaluation_outcome,
                            "candidate_trace": candidate_trace,
                        },
                    )
                    ORDERS_TOTAL.labels(status="blocked").inc()
                final_status = receipt.status
            else:
                final_status = "stand_down"
                evaluation_outcome = evaluation_outcome if evaluation_outcome in {"no_candidate", "pre_risk_filtered"} else "pre_risk_filtered"
        else:
            final_status = "stand_down"
            evaluation_outcome = evaluation_outcome if evaluation_outcome in {"no_candidate", "pre_risk_filtered"} else "pre_risk_filtered"
        candidate_trace["final_outcome"] = evaluation_outcome
        candidate_trace["final_status"] = final_status

        await repo.append_message(
            room.id,
            RoomMessageCreate(
                role=AgentRole.SUPERVISOR,
                kind=MessageKind.OBSERVATION,
                stage=RoomStage.COMPLETE,
                content=f"Deterministic path: {final_status}. {signal.summary}"
                + (" [loss sensitivity active: edge x2, size x0.5]" if loss_sensitivity_active else ""),
                payload={
                    "final_status": final_status,
                    "evaluation_outcome": evaluation_outcome,
                    "candidate_trace": candidate_trace,
                    "loss_sensitivity_active": loss_sensitivity_active,
                },
            ),
        )
        await repo.update_room_campaign(
            room.id,
            payload_updates={
                "final_status": final_status,
                "room_completed_at": datetime.now(UTC).isoformat(),
            },
        )
        await repo.update_room_stage(room.id, RoomStage.COMPLETE)
        ROOM_RUNS_TOTAL.labels(status="success").inc()
        await session.commit()
        try:
            await self.training_corpus_service.persist_strategy_audit_for_room(
                room.id,
                audit_source="live_forward",
            )
        except Exception:
            logger.exception("failed to persist strategy audit", extra={"room_id": room.id})

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        ACTIVE_ROOMS.inc()
        try:
            await self._run_room_inner(room_id=room_id, reason=reason)
        finally:
            ACTIVE_ROOMS.dec()

    async def _run_room_inner(self, *, room_id: str, reason: str) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.ensure_deployment_control(self.settings.app_color)
            room = await repo.get_room(room_id)
            if room is None:
                raise ValueError(f"Room {room_id} not found")

            await repo.append_message(
                room_id,
                RoomMessageCreate(
                    role=AgentRole.SUPERVISOR,
                    kind=MessageKind.OBSERVATION,
                    stage=RoomStage.TRIGGERED,
                    content=f"Supervisor started workflow for {room.market_ticker} because {reason}.",
                    payload={"reason": reason},
                ),
            )
            await session.commit()

            try:
                pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
                thresholds = self.agent_pack_service.runtime_thresholds(pack)
                heuristic_pack = (
                    await self.historical_heuristic_service.get_active_pack(repo)
                    if self.historical_heuristic_service is not None
                    else None
                )
                role_models: dict[str, Any] = {
                    role_name: {
                        "provider": config.provider,
                        "model": config.model,
                        "temperature": config.temperature,
                    }
                    for role_name, config in pack.roles.items()
                }
                await repo.update_room_runtime(
                    room.id,
                    agent_pack_version=pack.version,
                    role_models=role_models,
                )
                await session.commit()
                market_response, dossier = await asyncio.gather(
                    self.kalshi.get_market(room.market_ticker),
                    self.research_coordinator.ensure_fresh_dossier(room.market_ticker, reason="room_start"),
                )
                market = market_response.get("market", market_response)
                close_time_raw = market.get("close_time")
                if close_time_raw is not None:
                    try:
                        close_time = datetime.fromisoformat(str(close_time_raw).replace("Z", "+00:00"))
                        if close_time.tzinfo is None:
                            close_time = close_time.replace(tzinfo=UTC)
                        if datetime.now(UTC) >= close_time:
                            await repo.append_message(
                                room_id,
                                RoomMessageCreate(
                                    role=AgentRole.SUPERVISOR,
                                    kind=MessageKind.OBSERVATION,
                                    stage=RoomStage.COMPLETE,
                                    content=(
                                        f"Market {room.market_ticker} closed at {close_time.isoformat()}. "
                                        "Skipping room — no new entries after market close."
                                    ),
                                    payload={"close_time": close_time.isoformat(), "final_status": "market_closed"},
                                ),
                            )
                            await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                            await session.commit()
                            ROOM_RUNS_TOTAL.labels(status="success").inc()
                            return
                    except (ValueError, TypeError):
                        logger.warning(
                            "Could not parse close_time for %s: %r — proceeding without close guard",
                            room.market_ticker,
                            close_time_raw,
                        )
                mapping = self.weather_directory.resolve_market(room.market_ticker, market)
                weather_bundle = (
                    await self.weather.build_market_snapshot(mapping)
                    if mapping is not None and mapping.supports_structured_weather
                    else None
                )
                if mapping is not None and mapping.series_ticker:
                    city_assignment = await repo.get_city_strategy_assignment(mapping.series_ticker)
                    if city_assignment is not None:
                        strategy_record = await repo.get_strategy_by_name(city_assignment.strategy_name)
                        if strategy_record is not None:
                            d = strategy_record.thresholds
                            thresholds = RuntimeThresholds(
                                risk_min_edge_bps=int(d["risk_min_edge_bps"]),
                                risk_max_order_notional_dollars=float(d["risk_max_order_notional_dollars"]),
                                risk_max_position_notional_dollars=float(d["risk_max_position_notional_dollars"]),
                                trigger_max_spread_bps=int(d["trigger_max_spread_bps"]),
                                trigger_cooldown_seconds=int(d["trigger_cooldown_seconds"]),
                                strategy_quality_edge_buffer_bps=int(d["strategy_quality_edge_buffer_bps"]),
                                strategy_min_remaining_payout_bps=int(d["strategy_min_remaining_payout_bps"]),
                                risk_safe_capital_reserve_ratio=float(d["risk_safe_capital_reserve_ratio"]),
                                risk_risky_capital_max_ratio=float(d["risk_risky_capital_max_ratio"]),
                            )
                delta = self.research_coordinator.build_room_delta(
                    dossier=dossier,
                    market_response=market_response,
                    weather_bundle=weather_bundle,
                )

                await repo.log_exchange_event("rest_market", "market_snapshot", market_response, market_ticker=room.market_ticker)
                if mapping is not None and mapping.station_id is not None and weather_bundle is not None:
                    await repo.log_weather_event(mapping.station_id, "weather_bundle", weather_bundle)
                    archive_record = append_weather_bundle_archive(
                        self.settings,
                        weather_bundle,
                        source_id=f"room:{room.id}",
                        archive_source="room_supervisor",
                    )
                    archive_meta = weather_bundle_archive_metadata(weather_bundle)
                    if archive_meta is not None:
                        await repo.upsert_historical_weather_snapshot(
                            station_id=archive_meta["station_id"],
                            series_ticker=archive_meta["series_ticker"],
                            local_market_day=archive_meta["local_market_day"],
                            asof_ts=archive_meta["asof_ts"],
                            source_kind="archived_weather_bundle",
                            source_id=f"room:{room.id}",
                            source_hash=_hash_payload(weather_bundle),
                            observation_ts=archive_meta["observation_ts"],
                            forecast_updated_ts=archive_meta["forecast_updated_ts"],
                            forecast_high_f=archive_meta["forecast_high_f"],
                            current_temp_f=archive_meta["current_temp_f"],
                            payload={
                                **weather_bundle,
                                "_archive": {
                                    "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                                    "archive_source": "room_supervisor",
                                    "source_id": f"room:{room.id}",
                                },
                            },
                        )
                market_state = await repo.upsert_market_state(
                    room.market_ticker,
                    kalshi_env=room.kalshi_env,
                    snapshot=market,
                    yes_bid_dollars=as_decimal(market["yes_bid_dollars"]) if market.get("yes_bid_dollars") is not None else None,
                    yes_ask_dollars=as_decimal(market["yes_ask_dollars"]) if market.get("yes_ask_dollars") is not None else None,
                    last_trade_dollars=as_decimal(market["last_price_dollars"]) if market.get("last_price_dollars") is not None else None,
                )
                signal = self.research_coordinator.build_signal_from_dossier(
                    dossier,
                    market_response,
                    min_edge_bps=thresholds.risk_min_edge_bps,
                )
                if mapping is not None and mapping.supports_structured_weather and self.historical_heuristic_service is not None:
                    heuristic_application = self.historical_heuristic_service.apply_to_signal(
                        pack=heuristic_pack,
                        mapping=mapping,
                        signal=signal,
                        market_snapshot=market_response,
                        reference_time=datetime.now(UTC),
                        base_thresholds=thresholds,
                        market_stale=is_market_stale(
                            observed_at=market_state.observed_at,
                            stale_after_seconds=self.settings.risk_stale_market_seconds,
                        ),
                        research_stale=dossier.freshness.stale,
                    )
                    thresholds = self.historical_heuristic_service.runtime_thresholds(
                        base_thresholds=thresholds,
                        application=heuristic_application,
                    )
                    signal.heuristic_application = heuristic_application
                    signal = apply_heuristic_application_to_signal(
                        settings=self.settings,
                        signal=signal,
                        market_snapshot=market_response,
                        min_edge_bps=thresholds.risk_min_edge_bps,
                        spread_limit_bps=thresholds.trigger_max_spread_bps,
                        quality_buffer_bps=thresholds.strategy_quality_edge_buffer_bps,
                        minimum_remaining_payout_bps=thresholds.strategy_min_remaining_payout_bps,
                    )
                signal, momentum_outcome = await self._try_apply_momentum_post_processor(
                    signal,
                    repo=repo,
                    market_ticker=room.market_ticker,
                    bundle_age_reference=dossier.freshness.refreshed_at,
                )
                signal.eligibility = evaluate_trade_eligibility(
                    settings=self.settings,
                    signal=signal,
                    market_snapshot=market_response,
                    market_observed_at=market_state.observed_at,
                    research_freshness=dossier.freshness,
                    thresholds=thresholds,
                )
                signal.strategy_mode = signal.eligibility.strategy_mode
                signal.stand_down_reason = signal.eligibility.stand_down_reason
                signal.evaluation_outcome = signal.eligibility.evaluation_outcome
                signal.candidate_trace = signal.eligibility.candidate_trace or signal.candidate_trace
                if signal.eligibility.reasons and not signal.eligibility.eligible:
                    signal.summary = f"{signal.summary} Stand down: {' '.join(signal.eligibility.reasons)}"
                # Market structure gates mutate the signal in-place. Run them before
                # persistence so audit, dashboards, and corpus exports match execution.
                await self._run_market_gates(repo, signal, market, room.market_ticker)
                await repo.save_signal(
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    edge_bps=signal.edge_bps,
                    confidence=signal.confidence,
                    summary=signal.summary,
                    payload={
                        "research_mode": dossier.mode,
                        "research_gate_passed": dossier.gate.passed,
                        "research_last_run_id": dossier.last_run_id,
                        "research_delta": delta.model_dump(mode="json"),
                        "trader_context": dossier.trader_context.model_dump(mode="json"),
                        "research_freshness": dossier.freshness.model_dump(mode="json"),
                        "effective_research_freshness": dossier.freshness.model_dump(mode="json"),
                        "resolution_state": signal.resolution_state.value,
                        "strategy_mode": signal.strategy_mode.value,
                        "evaluation_outcome": signal.evaluation_outcome,
                        "candidate_trace": signal.candidate_trace,
                        "trade_regime": signal.trade_regime,
                        "capital_bucket": signal.capital_bucket,
                        "recommended_side": signal.recommended_side.value if signal.recommended_side is not None else None,
                        "forecast_delta_f": signal.forecast_delta_f,
                        "confidence_band": signal.confidence_band,
                        "model_quality_status": signal.model_quality_status,
                        "model_quality_reasons": signal.model_quality_reasons,
                        "recommended_size_cap_fp": (
                            str(signal.recommended_size_cap_fp) if signal.recommended_size_cap_fp is not None else None
                        ),
                        "size_factor": str(signal.size_factor),
                        "warn_only_blocked": signal.warn_only_blocked,
                        "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
                        "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason is not None else None,
                        "agent_pack_version": pack.version,
                        "heuristic_pack_version": (
                            (signal.heuristic_application or {}).get("heuristic_pack_version")
                            if signal.heuristic_application is not None
                            else None
                        ),
                        "intelligence_run_id": (
                            (signal.heuristic_application or {}).get("intelligence_run_id")
                            if signal.heuristic_application is not None
                            else None
                        ),
                        "candidate_pack_id": (
                            (signal.heuristic_application or {}).get("candidate_pack_id")
                            if signal.heuristic_application is not None
                            else None
                        ),
                        "heuristic_summary": (
                            (signal.heuristic_application or {}).get("agent_summary")
                            if signal.heuristic_application is not None
                            else None
                        ),
                        "rule_trace": (
                            list((signal.heuristic_application or {}).get("rule_trace") or [])
                            if signal.heuristic_application is not None
                            else []
                        ),
                        "support_window": (
                            dict((signal.heuristic_application or {}).get("support_window") or {})
                            if signal.heuristic_application is not None
                            else {}
                        ),
                        "momentum_slope_cents_per_min": signal.momentum_slope_cents_per_min,
                        "momentum_weight": signal.momentum_weight,
                        "edge_effective_bps": signal.edge_effective_bps,
                        "momentum_post_processor_outcome": momentum_outcome,
                    },
                )
                await session.commit()
                _governance_positions = await repo.list_positions_for_ticker(
                    room.market_ticker,
                    self.settings.kalshi_subaccount,
                    kalshi_env=room.kalshi_env,
                )
                held_position = max(_governance_positions, key=lambda p: p.count_fp) if _governance_positions else None
                if (
                    held_position is not None
                    and signal.recommended_side is not None
                    and held_position.side != signal.recommended_side.value
                ):
                    await repo.log_ops_event(
                        severity="warning",
                        summary=f"Latest signal flipped away from held side for {room.market_ticker}",
                        source="position_governance",
                        payload={
                            "market_ticker": room.market_ticker,
                            "held_side": held_position.side,
                            "recommended_side": signal.recommended_side.value,
                            "fair_yes_dollars": str(signal.fair_yes_dollars),
                        },
                        kalshi_env=room.kalshi_env,
                        room_id=room.id,
                    )
                    await session.commit()

                if not self.settings.llm_trading_enabled:
                    await self._run_deterministic_fast_path(
                        repo=repo,
                        session=session,
                        room=room,
                        control=control,
                        signal=signal,
                        thresholds=thresholds,
                        market_observed_at=market_state.observed_at,
                        research_fallback_time=dossier.freshness.refreshed_at,
                    )
                    return

                recent_memories = [note.summary for note in await repo.list_recent_memory_notes(limit=5)]
                await repo.update_room_stage(room.id, RoomStage.RESEARCHING)
                researcher_message, researcher_usage = await self.agents.researcher_message(
                    signal=signal,
                    dossier=dossier,
                    delta=delta,
                    room=room,
                    recent_memories=recent_memories,
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.RESEARCHER),
                )
                researcher_record = await repo.append_message(room.id, researcher_message)
                role_models[AgentRole.RESEARCHER.value] = researcher_usage
                dossier_artifact = await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="research_dossier_snapshot",
                    source="research",
                    title=f"Research dossier snapshot for {room.market_ticker}",
                    payload=dossier.model_dump(mode="json"),
                )
                await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="research_delta",
                    source="research",
                    title=f"Research delta for {room.market_ticker}",
                    payload=delta.model_dump(mode="json"),
                )
                await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="market_snapshot",
                    source="kalshi",
                    title=f"Market snapshot for {room.market_ticker}",
                    payload=market_response,
                )
                if weather_bundle is not None:
                    await repo.save_artifact(
                        room_id=room.id,
                        message_id=researcher_record.id,
                        artifact_type="weather_bundle",
                        source="nws",
                        title=f"Weather bundle for {room.market_ticker}",
                        payload=weather_bundle,
                    )
                for source in dossier.sources:
                    await repo.save_artifact(
                        room_id=room.id,
                        message_id=researcher_record.id,
                        artifact_type="research_source",
                        source=source.source_class,
                        title=source.title,
                        payload=source.model_dump(mode="json"),
                        url=source.url,
                        external_id=source.source_key,
                    )
                research_health = self.research_coordinator.training_quality_snapshot(dossier)
                await repo.upsert_room_research_health(
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    dossier_status=research_health["dossier_status"],
                    gate_passed=research_health["gate_passed"],
                    valid_dossier=research_health["valid_dossier"],
                    good_for_training=research_health["good_for_training"],
                    quality_score=research_health["quality_score"],
                    citation_coverage_score=research_health["citation_coverage_score"],
                    settlement_clarity_score=research_health["settlement_clarity_score"],
                    freshness_score=research_health["freshness_score"],
                    contradiction_count=research_health["contradiction_count"],
                    structured_completeness_score=research_health["structured_completeness_score"],
                    fair_value_score=research_health["fair_value_score"],
                    dossier_artifact_id=dossier_artifact.id,
                    payload=research_health["payload"],
                )
                await repo.update_room_campaign(
                    room.id,
                    dossier_artifact_id=dossier_artifact.id,
                    payload_updates={
                        "research_mode": dossier.mode,
                        "research_gate_passed": dossier.gate.passed,
                        "quality_score": dossier.quality.overall_score,
                    },
                )
                await session.commit()

                receipt = ExecReceiptPayload(status="no_trade", details={})
                final_status = "no_trade"
                rationale_ids = [researcher_record.id]

                if not dossier.gate.passed:
                    ops_record = await repo.append_message(
                        room.id,
                        await self.agents.ops_message(
                            summary=f"Research gate blocked the room: {' '.join(dossier.gate.reasons)}",
                            payload=dossier.gate.model_dump(mode='json'),
                        ),
                    )
                    rationale_ids.append(ops_record.id)
                    final_status = "research_blocked"
                    await session.commit()
                else:
                    total_capital_early = await repo.get_total_capital_dollars(kalshi_env=room.kalshi_env)
                    if total_capital_early is not None and total_capital_early > 0:
                        dynamic_order_cap = float(total_capital_early) * self.settings.risk_order_pct
                    else:
                        dynamic_order_cap = 0.0  # block trades until capital is reconciled

                    await repo.update_room_stage(room.id, RoomStage.POSTURE)
                    president_message, president_usage = await self.agents.president_message(
                        signal=signal,
                        role_config=self.agent_pack_service.role_config(pack, AgentRole.PRESIDENT),
                    )
                    president_record = await repo.append_message(room.id, president_message)
                    role_models[AgentRole.PRESIDENT.value] = president_usage
                    rationale_ids.append(president_record.id)
                    await session.commit()

                    await repo.update_room_stage(room.id, RoomStage.PROPOSING)
                    trader_message, ticket, client_order_id, trader_usage = await self.agents.trader_message(
                        signal=signal,
                        room_id=room.id,
                        market_ticker=room.market_ticker,
                        rationale_ids=rationale_ids.copy(),
                        role_config=self.agent_pack_service.role_config(pack, AgentRole.TRADER),
                        max_order_notional_dollars=dynamic_order_cap,
                    )
                    trader_record = await repo.append_message(room.id, trader_message)
                    role_models[AgentRole.TRADER.value] = trader_usage
                    rationale_ids.append(trader_record.id)
                    await session.commit()

                    if ticket is not None and client_order_id is not None:
                        if signal.size_factor < Decimal("1.00"):
                            from kalshi_bot.core.fixed_point import quantize_count
                            scaled = quantize_count(ticket.count_fp * signal.size_factor)
                            if scaled <= Decimal("0"):
                                ticket = None
                            else:
                                ticket = ticket.model_copy(update={"count_fp": scaled})
                    if ticket is not None and client_order_id is not None:
                        ticket_record = await repo.save_trade_ticket(
                            room.id,
                            ticket,
                            client_order_id,
                            message_id=trader_record.id,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                        )
                        _ticker_positions = await repo.list_positions_for_ticker(
                            room.market_ticker,
                            self.settings.kalshi_subaccount,
                            kalshi_env=room.kalshi_env,
                        )
                        if len(_ticker_positions) > 1:
                            await repo.log_ops_event(
                                severity="warning",
                                summary=f"data_inconsistency: multiple_positions_for_ticker {room.market_ticker}",
                                source="supervisor",
                                room_id=room.id,
                                kalshi_env=room.kalshi_env,
                                payload={
                                    "market_ticker": room.market_ticker,
                                    "position_count": len(_ticker_positions),
                                    "sides": [p.side for p in _ticker_positions],
                                },
                            )
                        open_position = max(_ticker_positions, key=lambda p: p.count_fp) if _ticker_positions else None
                        current_position_notional = (
                            estimate_notional_dollars(
                                ContractSide(open_position.side),
                                open_position.average_price_dollars,
                                open_position.count_fp,
                            )
                            if open_position is not None
                            else Decimal("0")
                        )
                        effective_thresholds = thresholds
                        total_capital = total_capital_early
                        _cap = float(total_capital) if total_capital is not None and total_capital > 0 else 0.0
                        order_cap = _cap * self.settings.risk_order_pct
                        position_cap = _cap * self.settings.risk_position_pct
                        if dossier.gate.stale_tolerance_active:
                            factor = self.settings.research_stale_tolerance_notional_factor
                            order_cap *= factor
                            position_cap *= factor
                        effective_thresholds = RuntimeThresholds(
                            risk_min_edge_bps=thresholds.risk_min_edge_bps,
                            risk_max_order_notional_dollars=order_cap,
                            risk_max_position_notional_dollars=position_cap,
                            risk_safe_capital_reserve_ratio=thresholds.risk_safe_capital_reserve_ratio,
                            risk_risky_capital_max_ratio=thresholds.risk_risky_capital_max_ratio,
                            trigger_max_spread_bps=thresholds.trigger_max_spread_bps,
                            trigger_cooldown_seconds=thresholds.trigger_cooldown_seconds,
                            strategy_quality_edge_buffer_bps=thresholds.strategy_quality_edge_buffer_bps,
                            strategy_min_remaining_payout_bps=thresholds.strategy_min_remaining_payout_bps,
                        )
                        portfolio_bucket_snapshot = await repo.portfolio_bucket_snapshot(
                            kalshi_env=room.kalshi_env,
                            subaccount=self.settings.kalshi_subaccount,
                            total_capital_dollars=total_capital or Decimal("0"),
                            safe_capital_reserve_ratio=effective_thresholds.risk_safe_capital_reserve_ratio,
                            risky_capital_max_ratio=effective_thresholds.risk_risky_capital_max_ratio,
                        )
                        all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
                        open_ticker_count = len({p.market_ticker for p in all_positions})
                        pending_order_count_fp = await repo.get_pending_buy_count_fp(
                            room.market_ticker,
                            ticket.side.value,
                            kalshi_env=room.kalshi_env,
                        )
                        strategy_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                            kalshi_env=room.kalshi_env,
                        )
                        risk_context = RiskContext(
                            market_observed_at=market_state.observed_at,
                            research_observed_at=_research_ref_time(signal, dossier.freshness.refreshed_at),
                            current_position_notional_dollars=current_position_notional,
                            current_position_count_fp=open_position.count_fp if open_position is not None else Decimal("0"),
                            current_position_side=open_position.side if open_position is not None else None,
                            pending_order_count_fp=pending_order_count_fp,
                            portfolio_bucket_snapshot=portfolio_bucket_snapshot,
                            open_ticker_count=open_ticker_count,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                            strategy_daily_realized_pnl_dollars=strategy_daily_pnl,
                        )
                        daily_pnl_llm = await repo.get_daily_pnl_dollars(kalshi_env=room.kalshi_env)
                        _daily_loss_ratio_llm = 0.0
                        _daily_hard_blocked_llm = False
                        if daily_pnl_llm is not None and _cap > 0 and self.settings.risk_daily_loss_pct > 0:
                            _daily_loss_ratio_llm = float(-daily_pnl_llm) / _cap
                            _daily_hard_blocked_llm = _daily_loss_ratio_llm >= self.settings.risk_daily_loss_pct
                        if _daily_hard_blocked_llm:
                            verdict = RiskVerdictPayload(
                                status=RiskStatus.BLOCKED,
                                reasons=[
                                    f"Daily loss circuit breaker: {_daily_loss_ratio_llm:.1%} loss "
                                    f">= {self.settings.risk_daily_loss_pct:.0%} hard limit."
                                ],
                            )
                        else:
                            verdict = self.risk_engine.evaluate(
                                room=room,
                                control=control,
                                ticket=ticket,
                                signal=signal,
                                context=risk_context,
                                thresholds=effective_thresholds,
                            )
                        await repo.save_risk_verdict(
                            room_id=room.id,
                            ticket_id=ticket_record.id,
                            status=verdict.status,
                            reasons=verdict.reasons,
                            approved_notional_dollars=verdict.approved_notional_dollars,
                            approved_count_fp=verdict.approved_count_fp,
                            payload=verdict.model_dump(mode="json"),
                        )
                        candidate_trace = dict(signal.candidate_trace or {})
                        if signal.eligibility is not None and signal.eligibility.candidate_trace:
                            candidate_trace = dict(signal.eligibility.candidate_trace)
                        risk_evaluation_outcome = (
                            "approved" if verdict.status == RiskStatus.APPROVED else "risk_blocked"
                        )
                        candidate_trace["final_outcome"] = risk_evaluation_outcome
                        risk_message, risk_usage = await self.agents.risk_message(
                            verdict=verdict,
                            role_config=self.agent_pack_service.role_config(pack, AgentRole.RISK_OFFICER),
                        )
                        risk_record = await repo.append_message(room.id, risk_message)
                        role_models[AgentRole.RISK_OFFICER.value] = risk_usage
                        rationale_ids.append(risk_record.id)
                        await session.commit()

                        if verdict.status == RiskStatus.APPROVED:
                            approved_ticket = approved_ticket_for_verdict(ticket, verdict)
                            await repo.update_room_stage(room.id, RoomStage.EXECUTING)
                            pending_reconcile = await _pending_post_kill_switch_reconcile(
                                repo, control, self.settings.app_color, room.kalshi_env
                            )
                            if pending_reconcile:
                                receipt = ExecReceiptPayload(
                                    status="pending_reconcile_after_kill_switch_clear",
                                    client_order_id=client_order_id,
                                    details={"reason": pending_reconcile},
                                )
                            else:
                                lock_acquired = await repo.acquire_execution_lock(
                                    holder=self.settings.app_color,
                                    color=self.settings.app_color,
                                    kalshi_env=room.kalshi_env,
                                )
                                if lock_acquired:
                                    receipt = await self.execution_service.execute(
                                        room=room,
                                        control=control,
                                        ticket=approved_ticket,
                                        client_order_id=client_order_id,
                                        fair_yes_dollars=signal.fair_yes_dollars,
                                    )
                                else:
                                    receipt = ExecReceiptPayload(
                                        status="lock_denied",
                                        client_order_id=client_order_id,
                                        details={"reason": "execution lock held by another deployment color"},
                                    )
                            ORDERS_TOTAL.labels(status=receipt.status).inc()
                            if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                                await repo.save_order(
                                    ticket_id=ticket_record.id,
                                    client_order_id=client_order_id,
                                    market_ticker=approved_ticket.market_ticker,
                                    status=receipt.status,
                                    side=approved_ticket.side.value,
                                    action=approved_ticket.action.value,
                                    yes_price_dollars=approved_ticket.yes_price_dollars,
                                    count_fp=approved_ticket.count_fp,
                                    raw=receipt.details,
                                    kalshi_order_id=receipt.external_order_id,
                                    kalshi_env=room.kalshi_env,
                                )
                        else:
                            receipt = ExecReceiptPayload(
                                status="blocked",
                                client_order_id=client_order_id,
                                details={
                                    "reasons": verdict.reasons,
                                    "evaluation_outcome": risk_evaluation_outcome,
                                    "candidate_trace": candidate_trace,
                                },
                            )
                            ORDERS_TOTAL.labels(status="blocked").inc()

                        execution_record = await repo.append_message(
                            room.id,
                            await self.agents.execution_message(receipt.status, receipt.model_dump(mode="json")),
                        )
                        rationale_ids.append(execution_record.id)
                        final_status = receipt.status
                        await session.commit()
                    else:
                        ops_record = await repo.append_message(
                            room.id,
                            await self.agents.ops_message(
                                summary=(
                                    "Ops monitor noted that the room stood down before risk or execution because "
                                    "the setup was not actionable."
                                ),
                                payload={
                                    "market_ticker": room.market_ticker,
                                    "status": "stand_down",
                                    "evaluation_outcome": (
                                        signal.evaluation_outcome
                                        or (
                                            signal.eligibility.evaluation_outcome
                                            if signal.eligibility is not None
                                            else None
                                        )
                                        or "pre_risk_filtered"
                                    ),
                                    "candidate_trace": (
                                        signal.eligibility.candidate_trace
                                        if signal.eligibility is not None and signal.eligibility.candidate_trace
                                        else signal.candidate_trace
                                    ),
                                    "eligibility": (
                                        signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None
                                    ),
                                },
                            ),
                        )
                        rationale_ids.append(ops_record.id)
                        final_status = "stand_down"
                        await session.commit()

                await repo.update_room_stage(room.id, RoomStage.AUDITING)
                auditor_record = await repo.append_message(
                    room.id,
                    await self.agents.auditor_message(final_status=final_status, rationale_ids=rationale_ids),
                )
                rationale_ids.append(auditor_record.id)
                await session.commit()

                all_messages = [_room_message_read(message) for message in await repo.list_messages(room.id)]
                memory_payload, memory_usage = await self.memory_service.build_note(
                    room,
                    all_messages,
                    memory_config=pack.memory,
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.MEMORY_LIBRARIAN),
                )
                await repo.update_room_stage(room.id, RoomStage.MEMORY)
                await repo.append_message(room.id, await self.agents.memory_message(memory_payload))
                role_models[AgentRole.MEMORY_LIBRARIAN.value] = memory_usage
                await repo.save_memory_note(
                    room_id=room.id,
                    payload=memory_payload,
                    embedding=self.agents.providers.embed_text(memory_payload.summary),
                    provider="hash-router-v1",
                )
                await repo.update_room_campaign(
                    room.id,
                    payload_updates={
                        "final_status": final_status,
                        "room_completed_at": datetime.now(UTC).isoformat(),
                    },
                )
                await repo.update_room_runtime(room.id, role_models=role_models)
                await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                ROOM_RUNS_TOTAL.labels(status="success").inc()
                await session.commit()
                try:
                    await self.training_corpus_service.persist_strategy_audit_for_room(
                        room.id,
                        audit_source="live_forward",
                    )
                except Exception:
                    logger.exception("failed to persist strategy audit", extra={"room_id": room.id})
            except Exception as exc:
                logger.exception("room workflow failed", extra={"room_id": room_id})
                await session.rollback()
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is not None:
                    await repo.update_room_stage(room.id, RoomStage.FAILED)
                    await repo.log_ops_event(
                        severity="error",
                        summary=f"Workflow failed for room {room.market_ticker}",
                        source="supervisor",
                        payload={"error": str(exc)},
                        room_id=room.id,
                    )
                    await repo.append_message(
                        room.id,
                        await self.agents.ops_message(
                            summary=f"Ops monitor saw a workflow failure: {exc}",
                            payload={"error": str(exc)},
                        ),
                    )
                    await session.commit()
                ROOM_RUNS_TOTAL.labels(status="failure").inc()
                raise
