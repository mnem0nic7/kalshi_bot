from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, ContractSide, MessageKind, RiskStatus, RoomStage, StandDownReason, StrategyCode
from kalshi_bot.core.fixed_point import as_decimal, make_client_order_id, quantize_count
from kalshi_bot.core.metrics import ACTIVE_ROOMS, ORDERS_TOTAL, ROOM_RUNS_TOTAL
from kalshi_bot.core.schemas import (
    AgentPackWeatherBootstrapPolicy,
    ExecReceiptPayload,
    RiskVerdictPayload,
    RoomMessageCreate,
    RoomMessageRead,
    TradeEligibilityVerdict,
    TradeTicket,
)
from kalshi_bot.db.models import Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.historical_archive import append_weather_bundle_archive, weather_bundle_archive_metadata
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.market_snapshot_archive import (
    DECISION_SIGNAL_MARKET_SOURCE_KIND,
    archive_point_in_time_market_snapshot,
)
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.modeling import build_shadow_modeling_payload
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.decision_trace import (
    DETERMINISTIC_PATH_VERSION,
    build_deterministic_decision_trace,
)
from kalshi_bot.services.signal_attention import SignalAttentionService, extract_decision_fields
from kalshi_bot.services.weather_empirical_bootstrap import (
    WeatherEmpiricalBootstrapContext,
    WeatherEmpiricalBootstrapService,
    confidence_source_from_trace,
    fair_value_source_from_provenance,
    market_day_from_ticker,
    stale_signal_evidence_from_trace,
)
from kalshi_bot.services.weather_policy import weather_policy_context_from_market

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
from kalshi_bot.services.trade_behavior import (
    apply_empirical_gate_to_eligibility,
    evaluate_empirical_gate,
    series_from_ticker,
    thresholds_with_production_freeze_floor,
    trade_behavior_context_payload,
    weather_live_daily_loss_cap_pct,
    weather_live_entry_freeze_bypassed,
)
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)

EXTREME_EDGE_DAILY_HIGH_AGREEMENT_TOLERANCE_F = 3.0
EXTREME_EDGE_CURRENT_TEMP_TOLERANCE_F = 1.0
PACIFIC = ZoneInfo("America/Los_Angeles")


def _policy_side_for_signal(signal: StrategySignal) -> str | None:
    if signal.recommended_side is not None:
        return signal.recommended_side.value
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    candidates = [candidate for candidate in trace.get("candidates") or [] if isinstance(candidate, dict)]
    if not candidates:
        candidates = [
            candidate
            for candidate in (trace.get("yes"), trace.get("no"))
            if isinstance(candidate, dict)
        ]
    candidates = [candidate for candidate in candidates if candidate.get("side") in {"yes", "no"}]
    if not candidates:
        return None
    best = max(
        candidates,
        key=lambda candidate: (
            int(candidate.get("quality_adjusted_edge_bps") or -1_000_000),
            int(candidate.get("edge_bps") or -1_000_000),
        ),
    )
    return str(best.get("side") or "") or None


def _runtime_threshold_payload(thresholds: RuntimeThresholds) -> dict[str, Any]:
    return {field: getattr(thresholds, field) for field in thresholds.__dataclass_fields__}


def _resolution_changes_thresholds(left: Any, right: Any) -> bool:
    return _runtime_threshold_payload(left.thresholds) != _runtime_threshold_payload(right.thresholds)


def _apply_weather_policy_mode(signal: StrategySignal, weather_policy_resolution: Any) -> StrategySignal:
    mode = str(getattr(weather_policy_resolution, "mode", "live") or "live").lower()
    if mode == "live" or signal.eligibility is None:
        return signal
    if mode not in {"paused", "shadow_only", "research_only", "retired"}:
        return signal
    candidate_trace = dict(signal.eligibility.candidate_trace or signal.candidate_trace or {})
    candidate_trace["weather_policy_mode_block"] = {
        "mode": mode,
        "policy_key": getattr(weather_policy_resolution, "policy_key", None),
        "lane": getattr(weather_policy_resolution, "lane", "entry_gate"),
        "reason": f"weather_policy_{mode}",
    }
    reason = (
        f"Weather policy {getattr(weather_policy_resolution, 'policy_key', 'unknown')} "
        f"is in {mode} mode for entry_gate, so live entry is blocked."
    )
    reasons = list(signal.eligibility.reasons or [])
    if reason not in reasons:
        reasons.append(reason)
    signal.eligibility = signal.eligibility.model_copy(
        update={
            "eligible": False,
            "stand_down_reason": StandDownReason.WEATHER_POLICY_PAUSED,
            "evaluation_outcome": "pre_risk_filtered",
            "candidate_trace": candidate_trace,
            "reasons": reasons,
            "blocked_upstream": True,
        }
    )
    signal.stand_down_reason = StandDownReason.WEATHER_POLICY_PAUSED
    signal.evaluation_outcome = "pre_risk_filtered"
    signal.candidate_trace = candidate_trace
    return signal


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]


def _apply_city_strategy_override(
    thresholds: RuntimeThresholds,
    strategy_thresholds: dict[str, Any],
) -> RuntimeThresholds:
    """Apply a city-strategy record's operational fields over the champion pack thresholds.

    Tunable gate fields (owned exclusively by AutonomousGateTuningService) are always
    taken from ``thresholds`` regardless of what the strategy record contains, ensuring
    city-strategy assignments cannot silently override values the autonomous tuner promotes.
    Strategy records may legitimately override sizing, cooldown, and quality-buffer fields.
    """
    d = strategy_thresholds
    return RuntimeThresholds(
        risk_min_edge_bps=thresholds.risk_min_edge_bps,
        risk_max_order_notional_dollars=float(d["risk_max_order_notional_dollars"]),
        risk_max_position_notional_dollars=float(d["risk_max_position_notional_dollars"]),
        trigger_max_spread_bps=thresholds.trigger_max_spread_bps,
        trigger_cooldown_seconds=int(d["trigger_cooldown_seconds"]),
        strategy_quality_edge_buffer_bps=int(d["strategy_quality_edge_buffer_bps"]),
        strategy_min_remaining_payout_bps=thresholds.strategy_min_remaining_payout_bps,
        risk_safe_capital_reserve_ratio=float(d["risk_safe_capital_reserve_ratio"]),
        risk_risky_capital_max_ratio=float(d["risk_risky_capital_max_ratio"]),
        risk_max_credible_edge_bps=thresholds.risk_max_credible_edge_bps,
        risk_min_confidence=thresholds.risk_min_confidence,
        risk_min_contract_price_dollars=thresholds.risk_min_contract_price_dollars,
        strategy_min_abs_delta_f=thresholds.strategy_min_abs_delta_f,
    )


def _payload_with_trade_behavior_context(payload: Any, context: dict[str, Any]) -> dict[str, Any]:
    base = dict(payload) if isinstance(payload, dict) else {}
    base["trade_behavior_context"] = context
    return base


def _market_snapshot_artifact_payload(
    market_response: dict[str, Any],
    *,
    observed_at: datetime | None,
    kalshi_env: str,
    market_ticker: str,
) -> dict[str, Any]:
    observed_at_iso = observed_at.astimezone(UTC).isoformat() if observed_at is not None else None
    payload = dict(market_response)
    market = payload.get("market")
    if isinstance(market, dict):
        payload["market"] = {
            **market,
            "observed_at": market.get("observed_at") or observed_at_iso,
        }
    else:
        payload["observed_at"] = payload.get("observed_at") or observed_at_iso
    payload["_snapshot_meta"] = {
        "source": "room_supervisor_rest_market",
        "kalshi_env": kalshi_env,
        "market_ticker": market_ticker,
        "observed_at": observed_at_iso,
    }
    return payload


def _signal_market_snapshot_payload(
    market_response: dict[str, Any],
    *,
    observed_at: datetime | None,
    kalshi_env: str,
    market_ticker: str,
) -> dict[str, Any]:
    observed_at_iso = observed_at.astimezone(UTC).isoformat() if observed_at is not None else None
    market = market_response.get("market") if isinstance(market_response.get("market"), dict) else market_response
    compact_market = {
        "ticker": market.get("ticker") or market.get("market_ticker") or market_ticker,
        "yes_bid_dollars": market.get("yes_bid_dollars"),
        "yes_ask_dollars": market.get("yes_ask_dollars"),
        "no_ask_dollars": market.get("no_ask_dollars"),
        "last_price_dollars": market.get("last_price_dollars"),
        "volume": market.get("volume"),
        "close_ts": market.get("close_ts"),
    }
    return {
        "market": compact_market,
        "observed_at": observed_at_iso,
        "snapshot_provenance": {
            "recovered": False,
            "source": "signal_payload_market_snapshot",
            "source_kind": "room_supervisor_rest_market",
            "source_id": _hash_payload({
                "kalshi_env": kalshi_env,
                "market_ticker": market_ticker,
                "observed_at": observed_at_iso,
                "market": compact_market,
            }),
            "leakage_risk": "none",
        },
    }


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


def _pacific_date(now: datetime | None = None) -> str:
    value = now or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(PACIFIC).strftime("%Y-%m-%d")


def _min_optional_cap(current: float | None, cap: float) -> float:
    if current is None or current <= 0:
        return cap
    return min(current, cap)


def _parse_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal_payload(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _cap_ticket_notional(ticket: TradeTicket, *, max_notional_dollars: float) -> tuple[TradeTicket | None, dict[str, Any]]:
    max_notional = Decimal(str(max_notional_dollars))
    unit_notional = estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, Decimal("1.00"))
    original_notional = estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp)
    trace = {
        "max_order_notional_dollars": str(max_notional),
        "original_count_fp": str(ticket.count_fp),
        "original_notional_dollars": str(original_notional),
        "unit_notional_dollars": str(unit_notional),
        "cap_applied": False,
    }
    if max_notional <= Decimal("0") or unit_notional <= Decimal("0"):
        return None, {**trace, "reason": "non_positive_probe_cap"}
    if original_notional <= max_notional:
        return ticket, trace
    capped_count = quantize_count(max_notional / unit_notional)
    trace.update({"cap_applied": True, "capped_count_fp": str(capped_count)})
    if capped_count <= Decimal("0"):
        return None, {**trace, "reason": "non_positive_probe_count"}
    return ticket.model_copy(update={"count_fp": capped_count}), trace


def _weather_realized_loss_cap_dollars(
    *,
    total_capital: Decimal,
    cap_pct: float,
    min_loss_dollars: float,
) -> Decimal:
    pct_cap = (Decimal(str(total_capital)) * Decimal(str(cap_pct))).quantize(Decimal("0.0001"))
    return max(pct_cap, Decimal(str(min_loss_dollars)))


def _weather_balance_discontinuity(
    *,
    portfolio_loss_ratio: float,
    realized_loss_dollars: Decimal,
    realized_loss_cap_dollars: Decimal,
    discontinuity_ratio: float,
) -> bool:
    return (
        portfolio_loss_ratio >= max(0.0, discontinuity_ratio)
        and realized_loss_dollars < realized_loss_cap_dollars
    )


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


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ticker_threshold_f(market_ticker: str) -> float | None:
    tail = market_ticker.rsplit("-T", 1)
    if len(tail) != 2:
        return None
    return _float_or_none(tail[1])


def _ticker_local_market_day(market_ticker: str) -> str | None:
    parts = market_ticker.split("-")
    if len(parts) < 2:
        return None
    try:
        return datetime.strptime(parts[1].upper(), "%y%b%d").date().isoformat()
    except ValueError:
        return None


def _normalised_strike_f(raw: Any) -> float | None:
    value = _float_or_none(raw)
    if value is None:
        return None
    return value * 1_000_000 if 0 < value < 1.0 else value


def _yes_contract_true_for_daily_high(operator: str | None, daily_high_f: float, threshold_f: float) -> bool | None:
    if operator == ">":
        return daily_high_f > threshold_f
    if operator == ">=":
        return daily_high_f >= threshold_f
    if operator == "<":
        return daily_high_f < threshold_f
    if operator == "<=":
        return daily_high_f <= threshold_f
    return None


def _selected_side_impossible_from_current(
    *,
    operator: str | None,
    selected_side: ContractSide | None,
    current_temp_f: float,
    threshold_f: float,
) -> bool:
    yes_now = _yes_contract_true_for_daily_high(operator, current_temp_f, threshold_f)
    if yes_now is True and selected_side == ContractSide.NO and operator in {">", ">="}:
        return True
    if yes_now is False and selected_side == ContractSide.YES and operator in {"<", "<="}:
        return True
    return False


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

    async def _apply_extreme_edge_diagnostic_gate(
        self,
        repo: "PlatformRepository",
        signal: StrategySignal,
        *,
        market_ticker: str,
        market: dict[str, Any] | None,
        kalshi_env: str,
        room_id: str | None,
        weather_archive_source_id: str | None,
    ) -> bool:
        if signal.edge_bps <= int(self.settings.risk_max_credible_edge_bps):
            return True
        if signal.eligibility is not None and not signal.eligibility.eligible:
            return False

        candidate_trace = dict(signal.candidate_trace or {})
        existing = candidate_trace.get("extreme_edge_diagnostic")
        if isinstance(existing, dict) and existing.get("passed") is False:
            return False

        diagnostic = await self._build_extreme_edge_diagnostic(
            repo=repo,
            signal=signal,
            market_ticker=market_ticker,
            market=market,
            weather_archive_source_id=weather_archive_source_id,
        )
        candidate_trace["extreme_edge_diagnostic"] = diagnostic
        reason_codes = list(candidate_trace.get("reason_codes") or [])
        if not diagnostic["passed"]:
            reason_codes.extend(["extreme_edge_diagnostic_failed", *diagnostic["reason_codes"]])
        candidate_trace["reason_codes"] = sorted({str(code) for code in reason_codes})
        signal.candidate_trace = candidate_trace

        if diagnostic["passed"]:
            candidate_trace["validated_extreme_edge"] = True
            reason_codes.append("max_credible_edge_validated")
            candidate_trace["reason_codes"] = sorted({str(code) for code in reason_codes})
            signal.candidate_trace = candidate_trace
            if signal.eligibility is not None:
                signal.eligibility = signal.eligibility.model_copy(update={"candidate_trace": candidate_trace})
            return True

        reason = StandDownReason.EXTREME_EDGE_DIAGNOSTIC_FAILED
        candidate_trace["eligibility_outcome"] = "pre_risk_filtered"
        candidate_trace["eligibility_stand_down_reason"] = reason.value
        signal.candidate_trace = candidate_trace
        msg = (
            f"Extreme edge {signal.edge_bps}bps exceeds credibility limit of "
            f"{self.settings.risk_max_credible_edge_bps}bps and failed diagnostic checks: "
            f"{', '.join(diagnostic['reason_codes'])}."
        )
        if signal.eligibility is None:
            signal.eligibility = TradeEligibilityVerdict(
                eligible=False,
                stand_down_reason=reason,
                evaluation_outcome="pre_risk_filtered",
                candidate_trace=candidate_trace,
                reasons=[msg],
            )
        else:
            signal.eligibility = signal.eligibility.model_copy(
                update={
                    "eligible": False,
                    "stand_down_reason": reason,
                    "evaluation_outcome": "pre_risk_filtered",
                    "candidate_trace": candidate_trace,
                    "reasons": [*list(signal.eligibility.reasons), msg],
                }
            )
        signal.stand_down_reason = reason
        signal.evaluation_outcome = "pre_risk_filtered"
        signal.summary = f"Stand down: {msg}"
        await repo.log_ops_event(
            severity="info",
            summary=f"Extreme-edge diagnostic failed for {market_ticker}",
            source="supervisor",
            room_id=room_id,
            kalshi_env=kalshi_env,
            payload={
                "market_ticker": market_ticker,
                "edge_bps": signal.edge_bps,
                "limit_bps": self.settings.risk_max_credible_edge_bps,
                "reason_codes": diagnostic["reason_codes"],
                "diagnostic": diagnostic,
            },
        )
        return False

    async def _build_extreme_edge_diagnostic(
        self,
        *,
        repo: "PlatformRepository",
        signal: StrategySignal,
        market_ticker: str,
        market: dict[str, Any] | None,
        weather_archive_source_id: str | None,
    ) -> dict[str, Any]:
        reason_codes: list[str] = []
        checks: dict[str, Any] = {}
        diagnostics: dict[str, Any] = {
            "diagnostic_class": "extreme_edge_over_5000bps",
            "edge_bps": signal.edge_bps,
            "limit_bps": self.settings.risk_max_credible_edge_bps,
            "selected_side": signal.recommended_side.value if signal.recommended_side is not None else None,
            "target_yes_price_dollars": str(signal.target_yes_price_dollars) if signal.target_yes_price_dollars is not None else None,
            "fair_yes_dollars": str(signal.fair_yes_dollars),
            "weather_archive_source_id": weather_archive_source_id,
        }

        mapping = self.weather_directory.resolve_market(market_ticker, market) if market is not None else self.weather_directory.resolve_market_stub(market_ticker)
        operator = getattr(mapping, "operator", None) if mapping is not None else None
        threshold_f = _float_or_none(getattr(mapping, "threshold_f", None)) if mapping is not None else None
        station_id = getattr(mapping, "station_id", None) if mapping is not None else None
        local_market_day = _ticker_local_market_day(market_ticker)
        diagnostics["market_metadata"] = {
            "resolved": mapping.model_dump(mode="json") if mapping is not None else None,
            "ticker_threshold_f": _ticker_threshold_f(market_ticker),
            "strike_type": market.get("strike_type") if isinstance(market, dict) else None,
            "floor_strike": market.get("floor_strike") if isinstance(market, dict) else None,
            "cap_strike": market.get("cap_strike") if isinstance(market, dict) else None,
            "title": market.get("title") if isinstance(market, dict) else None,
            "subtitle": market.get("subtitle") if isinstance(market, dict) else None,
        }

        metadata_ok = mapping is not None and operator is not None and threshold_f is not None
        strike_type = str(market.get("strike_type") or "") if isinstance(market, dict) else ""
        strike_operator = ">" if strike_type == "greater" else "<" if strike_type == "less" else None
        if strike_operator is not None and operator != strike_operator:
            metadata_ok = False
            reason_codes.append("market_metadata_polarity_conflict")
        ticker_threshold = _ticker_threshold_f(market_ticker)
        if ticker_threshold is not None and threshold_f is not None and abs(ticker_threshold - threshold_f) > 0.01:
            metadata_ok = False
            reason_codes.append("market_metadata_threshold_conflict")
        raw_strike = None
        if strike_operator == ">":
            raw_strike = market.get("floor_strike") if isinstance(market, dict) else None
        elif strike_operator == "<":
            raw_strike = market.get("cap_strike") if isinstance(market, dict) else None
        api_threshold = _normalised_strike_f(raw_strike)
        if api_threshold is not None and threshold_f is not None and abs(api_threshold - threshold_f) > 0.01:
            metadata_ok = False
            reason_codes.append("market_metadata_api_strike_conflict")
        if not metadata_ok:
            reason_codes.append("market_metadata_polarity_unverified")

        current_snapshot = None
        comparison_snapshot = None
        if station_id:
            if weather_archive_source_id:
                current_snapshot = await repo.get_historical_weather_snapshot_by_source(
                    station_id=str(station_id),
                    source_kind="archived_weather_bundle",
                    source_id=weather_archive_source_id,
                )
            if current_snapshot is None:
                current_snapshot = await repo.get_latest_historical_weather_snapshot(
                    station_id=str(station_id),
                    before_asof=datetime.now(UTC),
                    local_market_day=local_market_day,
                )
            if current_snapshot is not None:
                local_market_day = current_snapshot.local_market_day or local_market_day
                snapshots = await repo.list_historical_weather_snapshots(
                    station_id=str(station_id),
                    local_market_day=local_market_day,
                    before_asof=current_snapshot.asof_ts,
                    limit=8,
                )
                for snapshot in snapshots:
                    if snapshot.id == current_snapshot.id:
                        continue
                    if weather_archive_source_id and snapshot.source_id == weather_archive_source_id:
                        continue
                    if current_snapshot.source_hash and snapshot.source_hash == current_snapshot.source_hash:
                        continue
                    if (
                        snapshot.observation_ts == current_snapshot.observation_ts
                        and snapshot.forecast_updated_ts == current_snapshot.forecast_updated_ts
                        and snapshot.forecast_high_f == current_snapshot.forecast_high_f
                        and snapshot.current_temp_f == current_snapshot.current_temp_f
                    ):
                        continue
                    comparison_snapshot = snapshot
                    break

        current_high = _float_or_none(getattr(current_snapshot, "forecast_high_f", None))
        comparison_high = _float_or_none(getattr(comparison_snapshot, "forecast_high_f", None))
        current_temp = _float_or_none(getattr(current_snapshot, "current_temp_f", None))
        if current_temp is None and signal.weather is not None:
            current_temp = signal.weather.current_temp_f
        if current_high is None and signal.weather is not None:
            current_high = signal.weather.forecast_high_f
        diagnostics["station_daily_high_sources"] = {
            "station_id": station_id,
            "local_market_day": local_market_day,
            "current": self._historical_weather_snapshot_summary(current_snapshot),
            "comparison": self._historical_weather_snapshot_summary(comparison_snapshot),
            "agreement_tolerance_f": EXTREME_EDGE_DAILY_HIGH_AGREEMENT_TOLERANCE_F,
        }

        source_agreement_ok = current_high is not None and comparison_high is not None
        if not source_agreement_ok:
            reason_codes.append("station_daily_high_source_missing")
        else:
            high_delta = abs(current_high - comparison_high)
            source_agreement_ok = high_delta <= EXTREME_EDGE_DAILY_HIGH_AGREEMENT_TOLERANCE_F
            diagnostics["station_daily_high_sources"]["forecast_high_delta_f"] = round(high_delta, 3)
            if not source_agreement_ok:
                reason_codes.append("station_daily_high_source_disagreement")
        checks["station_daily_high_source_agreement"] = {
            "passed": source_agreement_ok,
            "current_forecast_high_f": current_high,
            "comparison_forecast_high_f": comparison_high,
        }

        current_sanity_ok = current_temp is not None and current_high is not None
        current_sanity_reasons: list[str] = []
        if not current_sanity_ok:
            current_sanity_reasons.append("current_observed_temp_missing")
        else:
            if current_temp > current_high + EXTREME_EDGE_CURRENT_TEMP_TOLERANCE_F:
                current_sanity_ok = False
                current_sanity_reasons.append("current_observed_temp_exceeds_forecast_high")
            if threshold_f is not None and _selected_side_impossible_from_current(
                operator=operator,
                selected_side=signal.recommended_side,
                current_temp_f=current_temp,
                threshold_f=threshold_f,
            ):
                current_sanity_ok = False
                current_sanity_reasons.append("current_observed_temp_contradicts_selected_side")
        reason_codes.extend(current_sanity_reasons)
        checks["current_observed_temp_sanity"] = {
            "passed": current_sanity_ok,
            "current_temp_f": current_temp,
            "forecast_high_f": current_high,
            "reasons": current_sanity_reasons,
        }

        polarity_ok = metadata_ok and current_high is not None and threshold_f is not None
        polarity_reasons: list[str] = []
        if not polarity_ok:
            polarity_reasons.append("market_metadata_polarity_unverified")
        else:
            yes_supported = _yes_contract_true_for_daily_high(operator, current_high, threshold_f)
            fair_yes = float(signal.fair_yes_dollars)
            if yes_supported is True and fair_yes < 0.45:
                polarity_ok = False
                polarity_reasons.append("market_metadata_polarity_forecast_conflict")
            elif yes_supported is False and fair_yes > 0.55:
                polarity_ok = False
                polarity_reasons.append("market_metadata_polarity_forecast_conflict")
        reason_codes.extend(polarity_reasons)
        checks["market_metadata_polarity_verification"] = {
            "passed": polarity_ok,
            "operator": operator,
            "threshold_f": threshold_f,
            "forecast_high_f": current_high,
            "fair_yes_dollars": str(signal.fair_yes_dollars),
            "reasons": polarity_reasons,
        }

        unique_reason_codes = sorted({code for code in reason_codes if code})
        return {
            "passed": not unique_reason_codes,
            "reason_codes": unique_reason_codes,
            "checks": checks,
            "diagnostics": diagnostics,
        }

    @staticmethod
    def _historical_weather_snapshot_summary(snapshot: Any | None) -> dict[str, Any] | None:
        if snapshot is None:
            return None
        return {
            "id": snapshot.id,
            "source_kind": snapshot.source_kind,
            "source_id": snapshot.source_id,
            "asof_ts": snapshot.asof_ts.isoformat() if snapshot.asof_ts is not None else None,
            "observation_ts": snapshot.observation_ts.isoformat() if snapshot.observation_ts is not None else None,
            "forecast_updated_ts": snapshot.forecast_updated_ts.isoformat() if snapshot.forecast_updated_ts is not None else None,
            "forecast_high_f": str(snapshot.forecast_high_f) if snapshot.forecast_high_f is not None else None,
            "current_temp_f": str(snapshot.current_temp_f) if snapshot.current_temp_f is not None else None,
        }

    async def _observed_high_so_far_for_bundle(
        self,
        repo: PlatformRepository,
        *,
        mapping: Any,
        weather_bundle: dict[str, Any],
    ) -> float | None:
        archive_meta = weather_bundle_archive_metadata(weather_bundle)
        if archive_meta is None:
            return None
        station_id = archive_meta.get("station_id") or getattr(mapping, "station_id", None)
        target_local_market_day = _ticker_local_market_day(getattr(mapping, "market_ticker", "") or "")
        local_market_day = target_local_market_day or archive_meta.get("local_market_day")
        asof_ts = archive_meta.get("asof_ts")
        if station_id is None or local_market_day is None or asof_ts is None:
            return _float_or_none(archive_meta.get("current_temp_f"))
        current_applies = archive_meta.get("local_market_day") == local_market_day
        snapshots = await repo.list_historical_weather_snapshots(
            station_id=str(station_id),
            local_market_day=str(local_market_day),
            before_asof=asof_ts,
            limit=5000,
        )
        highs = [
            value
            for snapshot in snapshots
            if (value := _float_or_none(getattr(snapshot, "current_temp_f", None))) is not None
        ]
        current = _float_or_none(archive_meta.get("current_temp_f"))
        if current is not None and current_applies:
            highs.append(current)
        return max(highs) if highs else None

    async def _recent_duplicate_risk_block_trace_id(
        self,
        *,
        repo: PlatformRepository,
        room: Room,
        ticket: TradeTicket,
        thresholds: RuntimeThresholds,
    ) -> str | None:
        latest_trace = await repo.get_latest_decision_trace_for_market(
            room.market_ticker,
            kalshi_env=room.kalshi_env,
        )
        if latest_trace is None or latest_trace.decision_kind != "risk_block":
            return None
        decision_time = latest_trace.decision_time
        if decision_time.tzinfo is None:
            decision_time = decision_time.replace(tzinfo=UTC)
        cooldown_seconds = max(int(thresholds.trigger_cooldown_seconds), 1800)
        if datetime.now(UTC) - decision_time >= timedelta(seconds=cooldown_seconds):
            return None

        trace = dict(latest_trace.trace or {})
        previous_ticket = trace.get("ticket") if isinstance(trace.get("ticket"), dict) else {}
        previous_side = previous_ticket.get("side")
        previous_price = previous_ticket.get("yes_price_dollars")
        previous_risk = trace.get("risk") if isinstance(trace.get("risk"), dict) else {}
        if previous_side != ticket.side.value:
            return None
        if previous_price in (None, ""):
            return None
        try:
            previous_bucket = int(Decimal(str(previous_price)) * Decimal("100"))
            current_bucket = int(ticket.yes_price_dollars * Decimal("100"))
        except Exception:
            return None
        if previous_bucket != current_bucket:
            return None
        if not previous_risk.get("reasons"):
            return None
        return latest_trace.id

    def _attention_row_for_signal(
        self,
        *,
        room: Room,
        signal: StrategySignal,
        market_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        numeric_facts: dict[str, Any] = {}
        if signal.weather is not None:
            numeric_facts.update(
                {
                    "forecast_high_f": signal.weather.forecast_high_f,
                    "forecast_delta_f": signal.weather.forecast_delta_f,
                    "current_temp_f": signal.weather.current_temp_f,
                    "observed_high_so_far_f": signal.weather.observed_high_so_far_f,
                    "source_disagreement_f": signal.weather.source_disagreement_f,
                    "prediction_provenance": dict(signal.prediction_provenance or {}),
                }
            )
        payload = {
            "candidate_trace": dict(signal.candidate_trace or {}),
            "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
            "empirical_gate": (
                signal.eligibility.empirical_gate
                if signal.eligibility is not None
                else (signal.candidate_trace or {}).get("empirical_gate")
            ),
            "fair_yes_dollars": str(signal.fair_yes_dollars),
            "confidence": signal.confidence,
            "recommended_side": signal.recommended_side.value if signal.recommended_side is not None else None,
            "target_yes_price_dollars": (
                str(signal.target_yes_price_dollars) if signal.target_yes_price_dollars is not None else None
            ),
            "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason is not None else None,
            "prediction_provenance": dict(signal.prediction_provenance or {}),
            "trader_context": {"numeric_facts": numeric_facts},
            "market_snapshot": market_snapshot or {},
            "model_quality_reasons": list(signal.model_quality_reasons or []),
            "recommended_size_cap_fp": (
                str(signal.recommended_size_cap_fp) if signal.recommended_size_cap_fp is not None else None
            ),
        }
        return extract_decision_fields(
            payload,
            settings=self.settings,
            room_id=room.id,
            market_ticker=room.market_ticker,
            updated_at=datetime.now(UTC),
            row_fair_yes=signal.fair_yes_dollars,
            row_edge_bps=signal.edge_bps,
            row_confidence=signal.confidence,
        )

    async def _apply_static_signal_guard(
        self,
        *,
        session: Any,
        room: Room,
        signal: StrategySignal,
        market_snapshot: dict[str, Any],
    ) -> StrategySignal:
        if (
            not bool(getattr(self.settings, "static_signal_guard_live_enabled", False))
            or signal.eligibility is None
            or not signal.eligibility.eligible
        ):
            return signal
        service = SignalAttentionService(self.settings)
        rows = await service.load_rows(
            session,
            kalshi_env=room.kalshi_env,
            lookback_hours=self.settings.signals_attention_lookback_hours,
            market_ticker=room.market_ticker,
        )
        rows.append(self._attention_row_for_signal(room=room, signal=signal, market_snapshot=market_snapshot))
        guard = service.static_signal_guard(rows)
        if guard is None:
            return signal
        candidate_trace = {
            **dict(signal.eligibility.candidate_trace or signal.candidate_trace or {}),
            "eligibility_stand_down_reason": StandDownReason.STATIC_SIGNAL_STALE.value,
            "trading_improvement": {
                **dict((signal.candidate_trace or {}).get("trading_improvement") or {}),
                "static_signal_guard": guard,
            },
        }
        reasons = list(signal.eligibility.reasons or [])
        reasons.append("Static signal guard blocked live entry because recent evaluations repeated unchanged fair value or edge.")
        signal.eligibility = signal.eligibility.model_copy(
            update={
                "eligible": False,
                "stand_down_reason": StandDownReason.STATIC_SIGNAL_STALE,
                "evaluation_outcome": "pre_risk_filtered",
                "candidate_trace": candidate_trace,
                "reasons": reasons,
                "blocked_upstream": True,
            }
        )
        signal.stand_down_reason = StandDownReason.STATIC_SIGNAL_STALE
        signal.evaluation_outcome = "pre_risk_filtered"
        signal.candidate_trace = candidate_trace
        return signal

    async def _maybe_apply_empirical_bootstrap(
        self,
        *,
        session: Any,
        room: Room,
        signal: StrategySignal,
        weather_policy_resolution: Any | None = None,
        pre_empirical_stand_down_reason: StandDownReason | None = None,
    ) -> StrategySignal:
        if signal.eligibility is None:
            return signal
        empirical = signal.eligibility.empirical_gate or (signal.candidate_trace or {}).get("empirical_gate") or {}
        if not isinstance(empirical, dict):
            return signal
        empirical_reason = str(empirical.get("underlying_reason") or empirical.get("reason") or "")
        if empirical_reason != "empirical_gate_under_sampled":
            return signal
        candidate_trace = dict(signal.eligibility.candidate_trace or signal.candidate_trace or {})
        original_reason_value = (
            pre_empirical_stand_down_reason.value
            if pre_empirical_stand_down_reason is not None
            else candidate_trace.get("pre_empirical_stand_down_reason")
        )
        allowed_original_reasons = {None, StandDownReason.INSUFFICIENT_FORECAST_SEPARATION.value}
        if original_reason_value not in allowed_original_reasons:
            candidate_trace["weather_empirical_bootstrap"] = {
                "matched": False,
                "applied": False,
                "reason": "bootstrap_does_not_bypass_original_gate",
                "original_stand_down_reason": original_reason_value,
            }
            signal.eligibility = signal.eligibility.model_copy(update={"candidate_trace": candidate_trace})
            signal.candidate_trace = candidate_trace
            return signal

        resolved_policy = getattr(weather_policy_resolution, "policy", None)
        bootstrap_policy = (
            resolved_policy.bootstrap
            if resolved_policy is not None and getattr(resolved_policy, "bootstrap", None) is not None
            else AgentPackWeatherBootstrapPolicy()
        )
        if (
            original_reason_value == StandDownReason.INSUFFICIENT_FORECAST_SEPARATION.value
            and not bootstrap_policy.allow_forecast_separation_bootstrap
        ):
            candidate_trace["weather_empirical_bootstrap"] = {
                "matched": False,
                "applied": False,
                "reason": "forecast_separation_bootstrap_disabled",
                "original_stand_down_reason": original_reason_value,
            }
            signal.eligibility = signal.eligibility.model_copy(update={"candidate_trace": candidate_trace})
            signal.candidate_trace = candidate_trace
            return signal

        provenance = dict(signal.prediction_provenance or {})
        if not provenance and signal.weather is not None and isinstance(signal.weather.prediction_provenance, dict):
            provenance = dict(signal.weather.prediction_provenance or {})
        fair_value_source = fair_value_source_from_provenance(
            provenance,
            fair_yes_dollars=signal.fair_yes_dollars,
        )
        confidence_source = confidence_source_from_trace(candidate_trace, provenance)
        stale_evidence = stale_signal_evidence_from_trace(candidate_trace)
        repo = PlatformRepository(session, kalshi_env=room.kalshi_env)
        now = datetime.now(UTC)
        since = now - timedelta(days=max(1, int(getattr(self.settings, "autonomous_gate_tuning_days", 3650))))
        bucket_key = empirical.get("bucket_key")
        recent_events = await repo.list_weather_bootstrap_events(
            kalshi_env=room.kalshi_env,
            bucket_key=bucket_key,
            since=since,
            limit=2000,
        )
        historical_evidence = await repo.list_weather_bootstrap_historical_evidence(
            kalshi_env=room.kalshi_env,
            bucket_key=bucket_key,
            strict_replay=True,
            limit=2000,
        )
        context = WeatherEmpiricalBootstrapContext(
            kalshi_env=room.kalshi_env,
            market_ticker=room.market_ticker,
            side=signal.recommended_side.value if signal.recommended_side is not None else None,
            confidence=signal.confidence,
            edge_bps_after_buffer=signal.eligibility.edge_after_quality_buffer_bps,
            fair_yes_dollars=signal.fair_yes_dollars,
            fair_value_source=fair_value_source,
            confidence_source=confidence_source,
            bucket_key=bucket_key,
            actual_sample_count=int(empirical.get("actual_sample_count") or 0),
            actual_net_pnl=(
                Decimal(str(empirical.get("actual_net_pnl")))
                if empirical.get("actual_net_pnl") not in (None, "")
                else None
            ),
            current_stand_down_reason=(
                signal.eligibility.stand_down_reason.value
                if signal.eligibility.stand_down_reason is not None
                else None
            ),
            pre_empirical_stand_down_reason=original_reason_value,
            policy_key=getattr(weather_policy_resolution, "policy_key", None),
            fallback_policy_key=getattr(weather_policy_resolution, "fallback_policy_key_used", None),
            market_observed_at=None,
            data_stale=bool(
                signal.eligibility.market_stale
                or signal.eligibility.research_stale
                or stale_evidence.get("stale")
            ),
            source_stale_reasons=tuple(stale_evidence.get("reason_codes") or ()),
            room_id=room.id,
            policy_pack_version=(
                (signal.candidate_trace or {}).get("active_policy_pack_version")
                or (signal.candidate_trace or {}).get("agent_pack_version")
            ),
        )
        decision = WeatherEmpiricalBootstrapService().evaluate(
            context=context,
            policy=bootstrap_policy,
            recent_events=recent_events,
            historical_evidence=historical_evidence,
            now=now,
        )
        trace = decision.to_trace()
        trace["bucket_id"] = bucket_key
        trace["original_stand_down_reason"] = original_reason_value
        trace["empirical_reason"] = empirical_reason
        trace["stale_signal_evidence"] = stale_evidence
        candidate_trace["weather_empirical_bootstrap"] = trace
        await repo.save_weather_bootstrap_event(
            kalshi_env=room.kalshi_env,
            market_ticker=room.market_ticker,
            series_ticker=series_from_ticker(room.market_ticker),
            local_market_day=market_day_from_ticker(room.market_ticker),
            bucket_key=bucket_key,
            policy_key=trace.get("policy_key"),
            tier=trace.get("tier"),
            event_type="decision",
            status=trace.get("outcome") or "block",
            side=signal.recommended_side.value if signal.recommended_side is not None else None,
            confidence=signal.confidence,
            edge_bps=trace.get("edge_bps_after_buffer"),
            size_factor=trace.get("size_factor"),
            source="live_forward",
            occurred_at=now,
            room_id=room.id,
            payload={
                **trace,
                "fair_value_source": fair_value_source,
                "data_stale": context.data_stale,
            },
        )
        if not decision.allowed_live:
            signal.eligibility = signal.eligibility.model_copy(update={"candidate_trace": candidate_trace})
            signal.candidate_trace = candidate_trace
            return signal

        candidate_trace["policy_variant_applied"] = "weather_empirical_bootstrap"
        candidate_trace["baseline_block_reason"] = signal.eligibility.stand_down_reason.value if signal.eligibility.stand_down_reason else None
        candidate_trace["eligibility_stand_down_reason"] = None
        candidate_trace["eligibility_outcome"] = "candidate_selected"
        signal.size_factor = min(signal.size_factor, Decimal(str(max(0.0, decision.size_factor))))
        signal.eligibility = signal.eligibility.model_copy(
            update={
                "eligible": True,
                "stand_down_reason": None,
                "evaluation_outcome": "candidate_selected",
                "candidate_trace": candidate_trace,
                "reasons": [
                    "Weather empirical bootstrap allowed an under-sampled high-edge entry to proceed to normal risk checks."
                ],
                "blocked_upstream": False,
            }
        )
        signal.stand_down_reason = None
        signal.evaluation_outcome = "candidate_selected"
        signal.candidate_trace = candidate_trace
        return signal

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
        source_snapshot_ids: dict[str, Any] | None = None,
        signal_record: Any | None = None,
    ) -> None:
        research_observed_at = _research_ref_time(signal, research_fallback_time)
        receipt = ExecReceiptPayload(status="no_trade", details={})
        final_status = "no_trade"
        ticket_record = None
        risk_verdict_record = None
        sizing_trace: dict[str, Any] = {}
        trace_thresholds = thresholds
        candidate_trace = dict(signal.candidate_trace or {})
        if signal.eligibility is not None and signal.eligibility.candidate_trace:
            candidate_trace = dict(signal.eligibility.candidate_trace)
        trade_behavior_context = trade_behavior_context_payload(
            market_ticker=room.market_ticker,
            side=signal.recommended_side.value if signal.recommended_side is not None else None,
            strategy_code=StrategyCode.DIRECTIONAL.value,
            yes_price_dollars=signal.target_yes_price_dollars,
            forecast_delta_f=signal.forecast_delta_f,
            confidence_band=signal.confidence_band,
            spread_bps=signal.eligibility.market_spread_bps if signal.eligibility is not None else None,
        )
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
            sizing_trace["total_capital_dollars"] = total_capital
            if total_capital is None or total_capital <= 0:
                eligible = False
                sizing_trace["stand_down_reason"] = "missing_total_capital"
            else:
                dynamic_order_cap = float(total_capital) * self.settings.risk_order_pct
                sizing_trace["dynamic_order_cap_dollars"] = dynamic_order_cap
                sizing_trace["risk_order_pct"] = self.settings.risk_order_pct
                count_fp = suggested_trade_count_fp(
                    settings=self.settings,
                    signal=signal,
                    max_order_notional_dollars=dynamic_order_cap,
                    total_capital_dollars=total_capital,
                )
                sizing_trace["suggested_count_fp"] = count_fp
                if count_fp is None or count_fp <= Decimal("0"):
                    eligible = False
                    sizing_trace["stand_down_reason"] = "non_positive_suggested_count"

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
                sizing_trace["size_factor"] = signal.size_factor
                sizing_trace["scaled_count_fp"] = scaled
                ticket = ticket.model_copy(update={"count_fp": scaled}) if scaled > Decimal("0") else None
            bootstrap_trace = (
                candidate_trace.get("weather_empirical_bootstrap")
                if isinstance(candidate_trace.get("weather_empirical_bootstrap"), dict)
                else {}
            )
            if ticket is not None and bootstrap_trace.get("applied") is True:
                cap_raw = bootstrap_trace.get("daily_notional_cap_dollars")
                used_raw = bootstrap_trace.get("daily_notional_used_dollars") or 0
                try:
                    remaining_cap = Decimal(str(cap_raw)) - Decimal(str(used_raw))
                except Exception:
                    remaining_cap = Decimal("0")
                unit_notional = estimate_notional_dollars(
                    ticket.side,
                    ticket.yes_price_dollars,
                    Decimal("1.00"),
                )
                ticket_notional = estimate_notional_dollars(
                    ticket.side,
                    ticket.yes_price_dollars,
                    ticket.count_fp,
                )
                sizing_trace["bootstrap_daily_notional_cap_dollars"] = cap_raw
                sizing_trace["bootstrap_daily_notional_used_dollars"] = used_raw
                sizing_trace["bootstrap_ticket_notional_dollars"] = ticket_notional
                if remaining_cap <= Decimal("0") or unit_notional <= Decimal("0"):
                    sizing_trace["stand_down_reason"] = "bootstrap_daily_notional_cap_reached"
                    ticket = None
                    eligible = False
                elif ticket_notional > remaining_cap:
                    capped_count = quantize_count(remaining_cap / unit_notional)
                    sizing_trace["bootstrap_capped_count_fp"] = capped_count
                    ticket = ticket.model_copy(update={"count_fp": capped_count}) if capped_count > Decimal("0") else None
                    if ticket is None:
                        sizing_trace["stand_down_reason"] = "bootstrap_daily_notional_cap_reached"
                        eligible = False

            if ticket is not None:
                duplicate_trace_id = await self._recent_duplicate_risk_block_trace_id(
                    repo=repo,
                    room=room,
                    ticket=ticket,
                    thresholds=thresholds,
                )
                if duplicate_trace_id is not None:
                    sizing_trace["stand_down_reason"] = "duplicate_suppressed"
                    sizing_trace["duplicate_decision_trace_id"] = duplicate_trace_id
                    evaluation_outcome = "duplicate_suppressed"
                    candidate_trace["final_outcome"] = "duplicate_suppressed"
                    candidate_trace["eligibility_stand_down_reason"] = "duplicate_suppressed"
                    ticket = None

            if ticket is not None:
                client_order_id = make_client_order_id(room.id, room.market_ticker, ticket.nonce)
                ticket_record = await repo.save_trade_ticket(
                    room.id,
                    ticket,
                    client_order_id,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                )
                ticket_record.payload = _payload_with_trade_behavior_context(
                    ticket_record.payload,
                    trade_behavior_context,
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
                sizing_trace["position_cap_dollars"] = position_cap
                sizing_trace["current_position_notional_dollars"] = current_position_notional
                daily_pnl = await repo.get_daily_pnl_dollars(kalshi_env=room.kalshi_env)
                strategy_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    kalshi_env=room.kalshi_env,
                )
                loss_sensitivity_active = False
                daily_loss_hard_blocked = False
                daily_loss_ratio = 0.0
                realized_loss_ratio = 0.0
                realized_loss_dollars = Decimal("0")
                realized_loss_cap_dollars = Decimal("0")
                weather_probe_active = False
                weather_balance_discontinuity = False
                weather_live_loss_guard = weather_live_entry_freeze_bypassed(
                    control=control,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                )
                daily_loss_cap_pct = weather_live_daily_loss_cap_pct(
                    control=control,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    default_pct=float(self.settings.risk_daily_loss_pct),
                )
                sizing_trace["daily_loss_cap_pct"] = daily_loss_cap_pct
                sensitivity_cp_key = f"loss_sensitivity_state:{room.kalshi_env}"
                prior_sensitivity_cp = await repo.get_checkpoint(sensitivity_cp_key)
                prior_sensitivity_active = (
                    prior_sensitivity_cp.payload.get("active") is True
                    if prior_sensitivity_cp is not None
                    else False
                )
                if daily_pnl is not None and float(total_capital) > 0:
                    daily_loss_ratio = float(-daily_pnl) / float(total_capital)
                if weather_live_loss_guard:
                    realized_loss_dollars = max(Decimal("0"), -Decimal(str(strategy_daily_pnl)))
                    realized_loss_cap_dollars = _weather_realized_loss_cap_dollars(
                        total_capital=total_capital,
                        cap_pct=daily_loss_cap_pct,
                        min_loss_dollars=self.settings.weather_live_probe_min_loss_dollars,
                    )
                    if float(total_capital) > 0:
                        realized_loss_ratio = float(realized_loss_dollars / Decimal(str(total_capital)))
                    weather_probe_active = daily_loss_cap_pct > 0 and realized_loss_dollars >= realized_loss_cap_dollars
                    weather_balance_discontinuity = _weather_balance_discontinuity(
                        portfolio_loss_ratio=daily_loss_ratio,
                        realized_loss_dollars=realized_loss_dollars,
                        realized_loss_cap_dollars=realized_loss_cap_dollars,
                        discontinuity_ratio=self.settings.weather_live_balance_discontinuity_ratio,
                    )
                    if weather_balance_discontinuity:
                        discontinuity_key = f"balance_discontinuity:{room.kalshi_env}:{_pacific_date()}"
                        if await repo.get_checkpoint(discontinuity_key) is None:
                            await repo.log_ops_event(
                                severity="warning",
                                summary="Weather daily-loss guard ignored portfolio balance discontinuity",
                                source="supervisor",
                                room_id=room.id,
                                kalshi_env=room.kalshi_env,
                                payload={
                                    "reason_code": "balance_discontinuity",
                                    "daily_pnl_dollars": str(daily_pnl),
                                    "portfolio_loss_ratio": round(daily_loss_ratio, 4),
                                    "strategy_daily_realized_pnl_dollars": str(strategy_daily_pnl),
                                    "realized_loss_dollars": str(realized_loss_dollars),
                                    "realized_loss_cap_dollars": str(realized_loss_cap_dollars),
                                    "market_ticker": room.market_ticker,
                                },
                            )
                            await repo.set_checkpoint(
                                discontinuity_key,
                                cursor=None,
                                payload={
                                    "observed_at": datetime.now(UTC).isoformat(),
                                    "reason_code": "balance_discontinuity",
                                    "daily_pnl_dollars": str(daily_pnl),
                                    "portfolio_loss_ratio": round(daily_loss_ratio, 4),
                                    "strategy_daily_realized_pnl_dollars": str(strategy_daily_pnl),
                                    "realized_loss_dollars": str(realized_loss_dollars),
                                    "realized_loss_cap_dollars": str(realized_loss_cap_dollars),
                                },
                            )
                    if self.settings.risk_daily_loss_sensitivity_pct > 0:
                        loss_sensitivity_active = realized_loss_ratio >= self.settings.risk_daily_loss_sensitivity_pct
                else:
                    if daily_loss_cap_pct > 0 and daily_loss_ratio >= daily_loss_cap_pct:
                        daily_loss_hard_blocked = True
                    elif self.settings.risk_daily_loss_sensitivity_pct > 0:
                        loss_sensitivity_active = daily_loss_ratio >= self.settings.risk_daily_loss_sensitivity_pct

                if loss_sensitivity_active != prior_sensitivity_active:
                    sensitivity_ratio = realized_loss_ratio if weather_live_loss_guard else daily_loss_ratio
                    await repo.log_ops_event(
                        severity="warning" if loss_sensitivity_active else "info",
                        summary=(
                            f"Loss sensitivity gate {'activated' if loss_sensitivity_active else 'deactivated'}: "
                            f"{sensitivity_ratio:.1%} vs {self.settings.risk_daily_loss_sensitivity_pct:.0%} threshold"
                        ),
                        source="supervisor",
                        room_id=room.id,
                        kalshi_env=room.kalshi_env,
                        payload={
                            "daily_loss_ratio": round(sensitivity_ratio, 4),
                            "sensitivity_pct": self.settings.risk_daily_loss_sensitivity_pct,
                            "active": loss_sensitivity_active,
                            "market_ticker": room.market_ticker,
                            "metric": "realized_strategy_loss" if weather_live_loss_guard else "portfolio_daily_pnl",
                        },
                    )
                    await repo.set_checkpoint(
                        sensitivity_cp_key,
                        cursor=None,
                        payload={
                            "active": loss_sensitivity_active,
                            "changed_at": datetime.now(UTC).isoformat(),
                            "daily_loss_ratio": round(sensitivity_ratio, 4),
                            "metric": "realized_strategy_loss" if weather_live_loss_guard else "portfolio_daily_pnl",
                        },
                    )
                    logger.warning(
                        "Loss sensitivity gate %s: %.1f%% daily loss vs %.0f%% threshold",
                        "activated" if loss_sensitivity_active else "deactivated",
                        sensitivity_ratio * 100,
                        self.settings.risk_daily_loss_sensitivity_pct * 100,
                    )

                effective_edge_bps = thresholds.risk_min_edge_bps
                effective_order_cap = dynamic_order_cap
                if loss_sensitivity_active:
                    effective_edge_bps = int(
                        effective_edge_bps * self.settings.risk_daily_loss_sensitivity_edge_multiplier
                    )
                    effective_order_cap = effective_order_cap * self.settings.risk_daily_loss_sensitivity_size_multiplier
                sizing_trace.update(
                    {
                        "daily_pnl_dollars": daily_pnl,
                        "daily_loss_ratio": daily_loss_ratio,
                        "daily_loss_hard_blocked": daily_loss_hard_blocked,
                        "weather_live_loss_guard": weather_live_loss_guard,
                        "weather_probe_active": weather_probe_active,
                        "weather_balance_discontinuity": weather_balance_discontinuity,
                        "realized_loss_ratio": realized_loss_ratio,
                        "realized_loss_dollars": str(realized_loss_dollars),
                        "realized_loss_cap_dollars": str(realized_loss_cap_dollars),
                        "loss_sensitivity_active": loss_sensitivity_active,
                        "effective_edge_bps": effective_edge_bps,
                        "effective_order_cap_dollars": effective_order_cap,
                    }
                )
                if weather_probe_active:
                    effective_edge_bps = max(effective_edge_bps, self.settings.weather_live_probe_min_net_edge_bps)
                    effective_order_cap = min(effective_order_cap, self.settings.weather_live_probe_max_order_notional_dollars)
                    position_cap = min(position_cap, self.settings.weather_live_probe_max_order_notional_dollars)

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
                    risk_max_credible_edge_bps=thresholds.risk_max_credible_edge_bps,
                    risk_min_confidence=(
                        max(thresholds.risk_min_confidence, self.settings.weather_live_probe_min_confidence)
                        if weather_probe_active
                        else thresholds.risk_min_confidence
                    ),
                    risk_min_contract_price_dollars=thresholds.risk_min_contract_price_dollars,
                    strategy_min_abs_delta_f=thresholds.strategy_min_abs_delta_f,
                )
                sizing_trace.update(
                    {
                        "effective_edge_bps": effective_edge_bps,
                        "effective_order_cap_dollars": effective_order_cap,
                        "position_cap_dollars": position_cap,
                    }
                )
                trace_thresholds = effective_thresholds
                portfolio_bucket_snapshot = await repo.portfolio_bucket_snapshot(
                    kalshi_env=room.kalshi_env,
                    subaccount=self.settings.kalshi_subaccount,
                    total_capital_dollars=total_capital,
                    safe_capital_reserve_ratio=effective_thresholds.risk_safe_capital_reserve_ratio,
                    risky_capital_max_ratio=effective_thresholds.risk_risky_capital_max_ratio,
                )
                all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
                open_ticker_count = len(
                    {p.market_ticker for p in all_positions if Decimal(str(p.count_fp)) > Decimal("0")}
                )
                pending_order_count_fp = await repo.get_pending_buy_count_fp(
                    room.market_ticker,
                    ticket.side.value,
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
                sizing_trace.update(
                    {
                        "open_ticker_count": open_ticker_count,
                        "pending_order_count_fp": pending_order_count_fp,
                        "portfolio_bucket_snapshot": portfolio_bucket_snapshot.model_dump(mode="json"),
                        "strategy_daily_realized_pnl_dollars": strategy_daily_pnl,
                    }
                )
                probe_checkpoint_key = f"weather_probe:{room.kalshi_env}:{StrategyCode.DIRECTIONAL.value}:{_pacific_date()}"
                probe_checkpoint = await repo.get_checkpoint(probe_checkpoint_key) if weather_probe_active else None
                probe_payload = dict(probe_checkpoint.payload or {}) if probe_checkpoint is not None else {}
                probe_daily_used = _decimal_payload(probe_payload.get("daily_notional_dollars"))
                probe_trace: dict[str, Any] = {}
                if weather_probe_active:
                    entered_pnl = _decimal_payload(probe_payload.get("entered_realized_pnl_dollars"), str(strategy_daily_pnl))
                    approved_probe_count = int(probe_payload.get("approved_probe_count") or 0)
                    if approved_probe_count > 0 and Decimal(str(strategy_daily_pnl)) < entered_pnl - Decimal("0.005"):
                        probe_payload["frozen"] = True
                        probe_payload["freeze_reason"] = "probe_realized_loss"
                        probe_payload["frozen_at"] = datetime.now(UTC).isoformat()
                    if not probe_payload:
                        await repo.log_ops_event(
                            severity="warning",
                            summary="Weather realized-loss probe mode active",
                            source="supervisor",
                            room_id=room.id,
                            kalshi_env=room.kalshi_env,
                            payload={
                                "reason_code": "realized_loss_probe_mode",
                                "strategy_code": StrategyCode.DIRECTIONAL.value,
                                "realized_loss_dollars": str(realized_loss_dollars),
                                "realized_loss_cap_dollars": str(realized_loss_cap_dollars),
                                "market_ticker": room.market_ticker,
                            },
                        )
                        probe_payload = {
                            "date": _pacific_date(),
                            "entered_at": datetime.now(UTC).isoformat(),
                            "entered_realized_pnl_dollars": str(strategy_daily_pnl),
                            "daily_notional_dollars": "0",
                            "approved_probe_count": 0,
                        }
                    last_probe_at = _parse_utc_datetime(probe_payload.get("last_probe_at"))
                    cooldown_active = (
                        last_probe_at is not None
                        and (datetime.now(UTC) - last_probe_at).total_seconds()
                        < self.settings.weather_live_probe_cooldown_seconds
                    )
                    probe_trace = {
                        "active": True,
                        "reason_code": "realized_loss_probe_mode",
                        "realized_loss_dollars": str(realized_loss_dollars),
                        "realized_loss_cap_dollars": str(realized_loss_cap_dollars),
                        "daily_notional_used_dollars": str(probe_daily_used),
                        "daily_notional_cap_dollars": str(self.settings.weather_live_probe_daily_notional_dollars),
                        "cooldown_active": cooldown_active,
                        "frozen": bool(probe_payload.get("frozen")),
                    }
                    sizing_trace["weather_live_probe"] = probe_trace

                probe_block_reasons: list[str] = []
                if weather_probe_active:
                    if probe_payload.get("frozen"):
                        probe_block_reasons.append("Weather probe mode is frozen until the next Pacific trading day.")
                    if open_ticker_count >= 1:
                        probe_block_reasons.append("Weather probe mode allows only one concurrent open position.")
                    if probe_daily_used >= Decimal(str(self.settings.weather_live_probe_daily_notional_dollars)):
                        probe_block_reasons.append("Weather probe daily notional cap is exhausted.")
                    if probe_trace.get("cooldown_active"):
                        probe_block_reasons.append("Weather probe cooldown is active.")
                    if not probe_block_reasons:
                        remaining_probe_notional = (
                            Decimal(str(self.settings.weather_live_probe_daily_notional_dollars))
                            - probe_daily_used
                        )
                        capped_ticket, cap_trace = _cap_ticket_notional(
                            ticket,
                            max_notional_dollars=float(
                                min(
                                    Decimal(str(self.settings.weather_live_probe_max_order_notional_dollars)),
                                    remaining_probe_notional,
                                )
                            ),
                        )
                        probe_trace["sizing"] = cap_trace
                        if capped_ticket is None:
                            probe_block_reasons.append("Weather probe cap produced a non-positive order size.")
                        else:
                            ticket = capped_ticket
                            sizing_trace["weather_live_probe"] = probe_trace

                if daily_loss_hard_blocked:
                    await repo.log_ops_event(
                        severity="critical",
                        summary=(
                            f"Daily loss circuit breaker tripped: {daily_loss_ratio:.1%} loss "
                            f">= {float(sizing_trace.get('daily_loss_cap_pct', self.settings.risk_daily_loss_pct)):.0%} hard limit"
                        ),
                        source="supervisor",
                        room_id=room.id,
                        kalshi_env=room.kalshi_env,
                        payload={
                            "daily_loss_ratio": round(daily_loss_ratio, 4),
                            "hard_limit_pct": sizing_trace.get("daily_loss_cap_pct", self.settings.risk_daily_loss_pct),
                            "market_ticker": room.market_ticker,
                        },
                    )
                    logger.critical(
                        "Daily loss circuit breaker tripped: %.1f%% loss >= %.0f%% hard limit (ticker=%s)",
                        daily_loss_ratio * 100,
                        float(sizing_trace.get("daily_loss_cap_pct", self.settings.risk_daily_loss_pct)) * 100,
                        room.market_ticker,
                    )
                    verdict = RiskVerdictPayload(
                        status=RiskStatus.BLOCKED,
                        reasons=[
                            f"Daily loss circuit breaker: {daily_loss_ratio:.1%} loss "
                            f">= {float(sizing_trace.get('daily_loss_cap_pct', self.settings.risk_daily_loss_pct)):.0%} hard limit."
                        ],
                        reason_codes=["daily_loss_circuit_breaker"],
                    )
                elif probe_block_reasons:
                    verdict = RiskVerdictPayload(
                        status=RiskStatus.BLOCKED,
                        reasons=probe_block_reasons,
                        reason_codes=["realized_loss_probe_mode"],
                        diagnostics={"weather_live_probe": probe_trace},
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
                    if weather_probe_active:
                        reason_codes = list(verdict.reason_codes)
                        if "realized_loss_probe_mode" not in reason_codes:
                            reason_codes.append("realized_loss_probe_mode")
                        if (probe_trace.get("sizing") or {}).get("cap_applied") and "probe_cap_applied" not in reason_codes:
                            reason_codes.append("probe_cap_applied")
                        if weather_balance_discontinuity and "balance_discontinuity" not in reason_codes:
                            reason_codes.append("balance_discontinuity")
                        reasons = list(verdict.reasons)
                        probe_reason = (
                            "Weather realized-loss probe mode active; entry must satisfy tiny-probe limits."
                        )
                        if probe_reason not in reasons:
                            reasons.insert(0, probe_reason)
                        diagnostics = dict(verdict.diagnostics or {})
                        diagnostics["weather_live_probe"] = probe_trace
                        verdict = verdict.model_copy(
                            update={
                                "reasons": reasons,
                                "reason_codes": reason_codes,
                                "diagnostics": diagnostics,
                            }
                        )
                        if verdict.status == RiskStatus.BLOCKED and "research_stale" in reason_codes:
                            probe_payload["frozen"] = True
                            probe_payload["freeze_reason"] = "probe_research_stale"
                            probe_payload["frozen_at"] = datetime.now(UTC).isoformat()
                if weather_balance_discontinuity and "balance_discontinuity" not in verdict.reason_codes:
                    reasons = list(verdict.reasons)
                    discontinuity_reason = (
                        "Portfolio balance discontinuity ignored by weather realized-loss guard."
                    )
                    if discontinuity_reason not in reasons:
                        reasons.append(discontinuity_reason)
                    verdict = verdict.model_copy(
                        update={
                            "reasons": reasons,
                            "reason_codes": [*verdict.reason_codes, "balance_discontinuity"],
                        }
                    )
                if weather_probe_active:
                    if verdict.status == RiskStatus.APPROVED:
                        approved_notional = verdict.approved_notional_dollars or estimate_notional_dollars(
                            ticket.side,
                            ticket.yes_price_dollars,
                            verdict.approved_count_fp or ticket.count_fp,
                        )
                        probe_payload.update(
                            {
                                "date": _pacific_date(),
                                "daily_notional_dollars": str(probe_daily_used + approved_notional),
                                "last_probe_at": datetime.now(UTC).isoformat(),
                                "approved_probe_count": int(probe_payload.get("approved_probe_count") or 0) + 1,
                                "entered_realized_pnl_dollars": probe_payload.get(
                                    "entered_realized_pnl_dollars",
                                    str(strategy_daily_pnl),
                                ),
                                "last_probe_notional_dollars": str(approved_notional),
                                "last_market_ticker": room.market_ticker,
                            }
                        )
                    if probe_payload:
                        await repo.set_checkpoint(probe_checkpoint_key, cursor=None, payload=probe_payload)
                risk_verdict_record = await repo.save_risk_verdict(
                    room_id=room.id,
                    ticket_id=ticket_record.id,
                    status=verdict.status,
                    reasons=verdict.reasons,
                    approved_notional_dollars=verdict.approved_notional_dollars,
                    approved_count_fp=verdict.approved_count_fp,
                    payload=verdict.model_dump(mode="json"),
                )
                if bootstrap_trace.get("applied") is True:
                    await repo.save_weather_bootstrap_event(
                        kalshi_env=room.kalshi_env,
                        market_ticker=room.market_ticker,
                        series_ticker=series_from_ticker(room.market_ticker),
                        local_market_day=market_day_from_ticker(room.market_ticker),
                        bucket_key=bootstrap_trace.get("bucket_id")
                        or (candidate_trace.get("empirical_gate") or {}).get("bucket_key"),
                        policy_key=bootstrap_trace.get("policy_key"),
                        tier=bootstrap_trace.get("tier"),
                        event_type="risk",
                        status=verdict.status.value,
                        side=ticket.side.value,
                        confidence=signal.confidence,
                        edge_bps=bootstrap_trace.get("edge_bps_after_buffer"),
                        size_factor=bootstrap_trace.get("size_factor"),
                        count_fp=ticket.count_fp,
                        notional_dollars=(
                            verdict.approved_notional_dollars
                            or estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp)
                        ),
                        source="live_forward",
                        occurred_at=datetime.now(UTC),
                        room_id=room.id,
                        payload={
                            **bootstrap_trace,
                            "risk_verdict_id": risk_verdict_record.id,
                            "risk_status": verdict.status.value,
                            "risk_reasons": verdict.reasons,
                        },
                    )
                if verdict.status == RiskStatus.APPROVED:
                    await repo.update_trade_ticket_status(ticket_record.id, "approved")
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
                                min_edge_bps=effective_thresholds.risk_min_edge_bps,
                            )
                        else:
                            receipt = ExecReceiptPayload(
                                status="lock_denied",
                                client_order_id=client_order_id,
                                details={"reason": "execution lock held by another deployment color"},
                            )
                    ORDERS_TOTAL.labels(status=receipt.status).inc()
                    if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                        order_raw = _payload_with_trade_behavior_context(receipt.details, trade_behavior_context)
                        order_record = await repo.save_order(
                            ticket_id=ticket_record.id,
                            client_order_id=client_order_id,
                            market_ticker=approved_ticket.market_ticker,
                            status=receipt.status,
                            side=approved_ticket.side.value,
                            action=approved_ticket.action.value,
                            yes_price_dollars=approved_ticket.yes_price_dollars,
                            count_fp=approved_ticket.count_fp,
                            raw=order_raw,
                            kalshi_order_id=receipt.external_order_id,
                            kalshi_env=room.kalshi_env,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                        )
                        if bootstrap_trace.get("applied") is True:
                            await repo.save_weather_bootstrap_event(
                                kalshi_env=room.kalshi_env,
                                market_ticker=room.market_ticker,
                                series_ticker=series_from_ticker(room.market_ticker),
                                local_market_day=market_day_from_ticker(room.market_ticker),
                                bucket_key=bootstrap_trace.get("bucket_id")
                                or (candidate_trace.get("empirical_gate") or {}).get("bucket_key"),
                                policy_key=bootstrap_trace.get("policy_key"),
                                tier=bootstrap_trace.get("tier"),
                                event_type="order",
                                status=receipt.status,
                                side=approved_ticket.side.value,
                                confidence=signal.confidence,
                                edge_bps=bootstrap_trace.get("edge_bps_after_buffer"),
                                size_factor=bootstrap_trace.get("size_factor"),
                                count_fp=approved_ticket.count_fp,
                                notional_dollars=estimate_notional_dollars(
                                    approved_ticket.side,
                                    approved_ticket.yes_price_dollars,
                                    approved_ticket.count_fp,
                                ),
                                source="live_forward",
                                occurred_at=datetime.now(UTC),
                                room_id=room.id,
                                order_id=order_record.id,
                                payload={
                                    **bootstrap_trace,
                                    "receipt_status": receipt.status,
                                    "external_order_id": receipt.external_order_id,
                                },
                            )
                    elif bootstrap_trace.get("applied") is True:
                        await repo.save_weather_bootstrap_event(
                            kalshi_env=room.kalshi_env,
                            market_ticker=room.market_ticker,
                            series_ticker=series_from_ticker(room.market_ticker),
                            local_market_day=market_day_from_ticker(room.market_ticker),
                            bucket_key=bootstrap_trace.get("bucket_id")
                            or (candidate_trace.get("empirical_gate") or {}).get("bucket_key"),
                            policy_key=bootstrap_trace.get("policy_key"),
                            tier=bootstrap_trace.get("tier"),
                            event_type="order",
                            status=receipt.status,
                            side=approved_ticket.side.value,
                            confidence=signal.confidence,
                            edge_bps=bootstrap_trace.get("edge_bps_after_buffer"),
                            size_factor=bootstrap_trace.get("size_factor"),
                            count_fp=approved_ticket.count_fp,
                            notional_dollars=estimate_notional_dollars(
                                approved_ticket.side,
                                approved_ticket.yes_price_dollars,
                                approved_ticket.count_fp,
                            ),
                            source="live_forward",
                            occurred_at=datetime.now(UTC),
                            room_id=room.id,
                            payload={
                                **bootstrap_trace,
                                "receipt_status": receipt.status,
                                "external_order_id": receipt.external_order_id,
                                "terminal_without_order_record": True,
                            },
                        )
                    await repo.update_trade_ticket_status(ticket_record.id, receipt.status)
                else:
                    await repo.update_trade_ticket_status(ticket_record.id, "blocked")
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
                evaluation_outcome = (
                    evaluation_outcome
                    if evaluation_outcome in {"no_candidate", "pre_risk_filtered", "duplicate_suppressed"}
                    else "pre_risk_filtered"
                )
        else:
            final_status = "stand_down"
            evaluation_outcome = (
                evaluation_outcome
                if evaluation_outcome in {"no_candidate", "pre_risk_filtered", "duplicate_suppressed"}
                else "pre_risk_filtered"
            )
        candidate_trace["final_outcome"] = evaluation_outcome
        candidate_trace["final_status"] = final_status

        input_hash, trace_hash, decision_trace_payload = build_deterministic_decision_trace(
            room=room,
            signal=signal,
            thresholds=trace_thresholds,
            candidate_trace=candidate_trace,
            final_status=final_status,
            evaluation_outcome=evaluation_outcome,
            ticket_record=ticket_record,
            risk_verdict_record=risk_verdict_record,
            receipt=receipt,
            market_observed_at=market_observed_at,
            research_observed_at=research_observed_at,
            source_snapshot_ids=source_snapshot_ids or {},
            sizing=sizing_trace,
            loss_sensitivity_active=loss_sensitivity_active,
        )
        decision_trace_record = await repo.save_decision_trace(
            room_id=room.id,
            ticket_id=ticket_record.id if ticket_record is not None else None,
            market_ticker=room.market_ticker,
            kalshi_env=room.kalshi_env,
            decision_kind=decision_trace_payload["decision_kind"],
            path_version=DETERMINISTIC_PATH_VERSION,
            agent_pack_version=room.agent_pack_version,
            parameter_pack_version=None,
            source_snapshot_ids=decision_trace_payload["source_snapshot_ids"],
            input_hash=input_hash,
            trace_hash=trace_hash,
            trace=decision_trace_payload,
        )
        if signal_record is not None:
            payload = dict(signal_record.payload or {})
            final_stand_down_reason = (
                sizing_trace.get("stand_down_reason")
                or candidate_trace.get("eligibility_stand_down_reason")
                or payload.get("stand_down_reason")
            )
            payload.update(
                {
                    "final_outcome": evaluation_outcome,
                    "final_status": final_status,
                    "final_stand_down_reason": final_stand_down_reason,
                    "decision_trace_id": decision_trace_record.id,
                    "decision_trace_hash": trace_hash,
                    "candidate_trace": candidate_trace,
                }
            )
            signal_record.payload = payload

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
                    "decision_trace_id": decision_trace_record.id,
                    "decision_trace_hash": trace_hash,
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
                "decision_trace_id": decision_trace_record.id,
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

    async def _apply_empirical_gate(
        self,
        *,
        session: Any,
        room: Room,
        signal: StrategySignal,
        weather_policy_resolution: Any | None = None,
    ) -> StrategySignal:
        if (
            signal.eligibility is None
            or signal.recommended_action is None
            or signal.recommended_side is None
            or signal.target_yes_price_dollars is None
        ):
            return signal
        if not hasattr(session, "execute"):
            return signal

        pre_empirical_stand_down_reason = signal.eligibility.stand_down_reason
        repo = PlatformRepository(session, kalshi_env=room.kalshi_env)
        control = await repo.get_deployment_control(kalshi_env=room.kalshi_env)
        production_freeze_bypass = weather_live_entry_freeze_bypassed(
            control=control,
            strategy_code=StrategyCode.DIRECTIONAL.value,
        )
        decision = await evaluate_empirical_gate(
            session=session,
            settings=self.settings,
            kalshi_env=room.kalshi_env,
            market_ticker=room.market_ticker,
            side=signal.recommended_side.value,
            action=signal.recommended_action.value,
            strategy_code=StrategyCode.DIRECTIONAL.value,
            shadow_mode=bool(getattr(room, "shadow_mode", self.settings.app_shadow_mode)),
            yes_price_dollars=signal.target_yes_price_dollars,
            forecast_delta_f=signal.forecast_delta_f,
            confidence_band=signal.confidence_band,
            spread_bps=signal.eligibility.market_spread_bps if signal.eligibility is not None else None,
            production_freeze_bypass=production_freeze_bypass,
        )
        payload = decision.to_payload()
        if decision.reason == self.settings.trade_behavior_entry_freeze_reason and hasattr(self.settings, "model_copy"):
            no_freeze_settings = self.settings.model_copy(
                update={"trade_behavior_production_entry_freeze_enabled": False}
            )
            underlying = await evaluate_empirical_gate(
                session=session,
                settings=no_freeze_settings,
                kalshi_env=room.kalshi_env,
                market_ticker=room.market_ticker,
                side=signal.recommended_side.value,
                action=signal.recommended_action.value,
                strategy_code=StrategyCode.DIRECTIONAL.value,
                shadow_mode=bool(getattr(room, "shadow_mode", self.settings.app_shadow_mode)),
                yes_price_dollars=signal.target_yes_price_dollars,
                forecast_delta_f=signal.forecast_delta_f,
                confidence_band=signal.confidence_band,
                spread_bps=signal.eligibility.market_spread_bps if signal.eligibility is not None else None,
            )
            payload.update(
                {
                    "underlying_status": underlying.status,
                    "underlying_reason": underlying.reason,
                    "underlying_actual_sample_count": underlying.actual_sample_count,
                    "underlying_blocks_live_entries": underlying.blocks_live_entries,
                }
            )
        signal.candidate_trace = {**dict(signal.candidate_trace or {}), "empirical_gate": payload}
        signal.candidate_trace["pre_empirical_stand_down_reason"] = (
            pre_empirical_stand_down_reason.value if pre_empirical_stand_down_reason is not None else None
        )
        signal.eligibility = apply_empirical_gate_to_eligibility(signal.eligibility, decision)
        signal.eligibility = signal.eligibility.model_copy(
            update={
                "candidate_trace": {
                    **dict(signal.eligibility.candidate_trace or {}),
                    "empirical_gate": payload,
                    "pre_empirical_stand_down_reason": (
                        pre_empirical_stand_down_reason.value
                        if pre_empirical_stand_down_reason is not None
                        else None
                    ),
                },
                "empirical_gate": payload,
            }
        )
        signal.stand_down_reason = signal.eligibility.stand_down_reason
        signal.evaluation_outcome = signal.eligibility.evaluation_outcome
        signal.candidate_trace = signal.eligibility.candidate_trace or signal.candidate_trace
        signal = await self._maybe_apply_empirical_bootstrap(
            session=session,
            room=room,
            signal=signal,
            weather_policy_resolution=weather_policy_resolution,
            pre_empirical_stand_down_reason=pre_empirical_stand_down_reason,
        )
        if decision.blocks_live_entries and signal.eligibility is not None and not signal.eligibility.eligible:
            signal.summary = f"{signal.summary} Stand down: empirical trade behavior gate blocked live entry ({decision.reason})."
        elif decision.blocks_live_entries:
            signal.summary = f"{signal.summary} Trading improvement: empirical bootstrap allowed an under-sampled high-edge entry to proceed to risk checks."
        return signal

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
                thresholds = thresholds_with_production_freeze_floor(
                    settings=self.settings,
                    kalshi_env=room.kalshi_env,
                    thresholds=thresholds,
                )
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
                if mapping is not None and mapping.supports_structured_weather and weather_bundle is not None:
                    observed_high_so_far = await self._observed_high_so_far_for_bundle(
                        repo,
                        mapping=mapping,
                        weather_bundle=weather_bundle,
                    )
                    if observed_high_so_far is not None:
                        weather_bundle = {**weather_bundle, "observed_high_so_far_f": observed_high_so_far}
                if mapping is not None and mapping.series_ticker:
                    city_assignment = await repo.get_city_strategy_assignment(mapping.series_ticker)
                    if city_assignment is not None:
                        strategy_record = await repo.get_strategy_by_name(city_assignment.strategy_name)
                        if strategy_record is not None:
                            thresholds = _apply_city_strategy_override(thresholds, strategy_record.thresholds)
                            thresholds = thresholds_with_production_freeze_floor(
                                settings=self.settings,
                                kalshi_env=room.kalshi_env,
                                thresholds=thresholds,
                            )
                weather_policy_context = weather_policy_context_from_market(
                    market_ticker=room.market_ticker,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    local_market_day=_ticker_local_market_day(room.market_ticker),
                    lane="entry_gate",
                )
                weather_policy_resolution = self.agent_pack_service.resolve_weather_policy(
                    pack,
                    weather_policy_context,
                    fallback_thresholds=thresholds,
                )
                thresholds = thresholds_with_production_freeze_floor(
                    settings=self.settings,
                    kalshi_env=room.kalshi_env,
                    thresholds=weather_policy_resolution.thresholds,
                )
                weather_policy_provenance = {
                    **weather_policy_resolution.provenance(),
                    "active_policy_pack_version": pack.version,
                }
                delta = self.research_coordinator.build_room_delta(
                    dossier=dossier,
                    market_response=market_response,
                    weather_bundle=weather_bundle,
                )

                await repo.log_exchange_event("rest_market", "market_snapshot", market_response, market_ticker=room.market_ticker)
                weather_archive_source_id = f"room:{room.id}" if weather_bundle is not None else None
                if mapping is not None and mapping.station_id is not None and weather_bundle is not None:
                    await repo.log_weather_event(mapping.station_id, "weather_bundle", weather_bundle)
                    archive_record = append_weather_bundle_archive(
                        self.settings,
                        weather_bundle,
                        source_id=weather_archive_source_id,
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
                                    "source_id": weather_archive_source_id,
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
                market_snapshot_artifact = None
                if mapping is not None and mapping.supports_structured_weather and weather_bundle is not None:
                    signal = self.signal_engine.evaluate(
                        mapping,
                        market_response,
                        weather_bundle,
                        min_edge_bps=thresholds.risk_min_edge_bps,
                        thresholds=thresholds,
                    )
                    signal.candidate_trace = {
                        **dict(signal.candidate_trace or {}),
                        "fresh_weather_signal": True,
                        "research_last_run_id": dossier.last_run_id,
                    }
                else:
                    signal = self.research_coordinator.build_signal_from_dossier(
                        dossier,
                        market_response,
                        min_edge_bps=thresholds.risk_min_edge_bps,
                    )
                def _resolve_policy_for_current_signal() -> Any:
                    scoped_policy_context = weather_policy_context_from_market(
                        market_ticker=room.market_ticker,
                        strategy_code=StrategyCode.DIRECTIONAL.value,
                        side=_policy_side_for_signal(signal),
                        local_market_day=_ticker_local_market_day(room.market_ticker),
                        trade_regime=signal.trade_regime,
                        lane="entry_gate",
                    )
                    return self.agent_pack_service.resolve_weather_policy(
                        pack,
                        scoped_policy_context,
                        fallback_thresholds=thresholds,
                    )

                for scoped_policy_attempt in range(3):
                    scoped_policy_resolution = _resolve_policy_for_current_signal()
                    scoped_policy_changed = (
                        scoped_policy_resolution.policy_key != weather_policy_resolution.policy_key
                        or scoped_policy_resolution.mode != weather_policy_resolution.mode
                        or scoped_policy_resolution.action != weather_policy_resolution.action
                    )
                    thresholds_changed = _resolution_changes_thresholds(
                        weather_policy_resolution,
                        scoped_policy_resolution,
                    )
                    if not scoped_policy_changed and not thresholds_changed:
                        break
                    weather_policy_resolution = scoped_policy_resolution
                    thresholds = thresholds_with_production_freeze_floor(
                        settings=self.settings,
                        kalshi_env=room.kalshi_env,
                        thresholds=weather_policy_resolution.thresholds,
                    )
                    weather_policy_provenance = {
                        **weather_policy_resolution.provenance(),
                        "active_policy_pack_version": pack.version,
                    }
                    if not thresholds_changed:
                        break
                    if mapping is not None and mapping.supports_structured_weather and weather_bundle is not None:
                        signal = self.signal_engine.evaluate(
                            mapping,
                            market_response,
                            weather_bundle,
                            min_edge_bps=thresholds.risk_min_edge_bps,
                            thresholds=thresholds,
                        )
                        signal.candidate_trace = {
                            **dict(signal.candidate_trace or {}),
                            "fresh_weather_signal": True,
                            "research_last_run_id": dossier.last_run_id,
                            "scoped_weather_policy_reapplied": True,
                            "scoped_weather_policy_attempt": scoped_policy_attempt + 1,
                        }
                    else:
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
                    thresholds = thresholds_with_production_freeze_floor(
                        settings=self.settings,
                        kalshi_env=room.kalshi_env,
                        thresholds=thresholds,
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
                signal.candidate_trace = {
                    **dict(signal.candidate_trace or {}),
                    "weather_policy": weather_policy_provenance,
                    "active_policy_pack_version": pack.version,
                    "policy_key": weather_policy_provenance["policy_key"],
                    "fallback_policy_key_used": weather_policy_provenance["fallback_policy_key_used"],
                    "binding_policy_lane": weather_policy_provenance["binding_policy_lane"],
                    "policy_disagreement": weather_policy_provenance["policy_disagreement"],
                }
                signal.eligibility = signal.eligibility.model_copy(update={"candidate_trace": signal.candidate_trace})
                signal = _apply_weather_policy_mode(signal, weather_policy_resolution)
                signal = await self._apply_static_signal_guard(
                    session=session,
                    room=room,
                    signal=signal,
                    market_snapshot=market_response,
                )
                if signal.eligibility.reasons and not signal.eligibility.eligible:
                    signal.summary = f"{signal.summary} Stand down: {' '.join(signal.eligibility.reasons)}"
                # Market structure gates mutate the signal in-place. Run them before
                # persistence so audit, dashboards, and corpus exports match execution.
                await self._run_market_gates(repo, signal, market, room.market_ticker)
                await self._apply_extreme_edge_diagnostic_gate(
                    repo,
                    signal,
                    market_ticker=room.market_ticker,
                    market=market,
                    kalshi_env=room.kalshi_env,
                    room_id=room.id,
                    weather_archive_source_id=weather_archive_source_id,
                )
                signal = await self._apply_empirical_gate(
                    session=session,
                    room=room,
                    signal=signal,
                    weather_policy_resolution=weather_policy_resolution,
                )
                signal_market_snapshot = _signal_market_snapshot_payload(
                    market_response,
                    observed_at=market_state.observed_at,
                    kalshi_env=room.kalshi_env,
                    market_ticker=room.market_ticker,
                )
                signal_trade_behavior_context = trade_behavior_context_payload(
                    market_ticker=room.market_ticker,
                    side=signal.recommended_side.value if signal.recommended_side is not None else None,
                    strategy_code=StrategyCode.DIRECTIONAL.value,
                    yes_price_dollars=signal.target_yes_price_dollars,
                    forecast_delta_f=signal.forecast_delta_f,
                    confidence_band=signal.confidence_band,
                    spread_bps=signal.eligibility.market_spread_bps if signal.eligibility is not None else None,
                )
                signal_modeling = build_shadow_modeling_payload(
                    signal=signal,
                    kalshi_env=room.kalshi_env,
                    shadow_mode=bool(getattr(room, "shadow_mode", self.settings.app_shadow_mode)),
                    bucket_key=signal_trade_behavior_context.get("bucket_key"),
                    empirical_gate=(
                        signal.eligibility.empirical_gate
                        if signal.eligibility is not None
                        else (signal.candidate_trace or {}).get("empirical_gate")
                    ),
                )
                signal.candidate_trace = {
                    **dict(signal.candidate_trace or {}),
                    "modeling": signal_modeling,
                    "prediction_model": signal_modeling["prediction_model"],
                    "trade_selection_model": signal_modeling["trade_selection_model"],
                }
                trader_context_payload = dossier.trader_context.model_dump(mode="json")
                if signal.weather is not None:
                    numeric_facts = dict(trader_context_payload.get("numeric_facts") or {})
                    numeric_facts.update(
                        {
                            "forecast_high_f": signal.weather.forecast_high_f,
                            "forecast_delta_f": signal.weather.forecast_delta_f,
                            "current_temp_f": signal.weather.current_temp_f,
                            "observed_high_so_far_f": signal.weather.observed_high_so_far_f,
                            "threshold_f": getattr(mapping, "threshold_f", None) if mapping is not None else None,
                            "operator": getattr(mapping, "operator", None) if mapping is not None else None,
                            "contract_direction": (
                                "below"
                                if mapping is not None and getattr(mapping, "operator", None) in {"<", "<="}
                                else "above"
                                if mapping is not None and getattr(mapping, "operator", None) in {">", ">="}
                                else None
                            ),
                            "source_disagreement_f": signal.weather.source_disagreement_f,
                            "sigma_f": signal.weather.sigma_f,
                            "sigma_layer": signal.weather.sigma_layer,
                            "residual_adjustment_f": signal.weather.residual_adjustment_f,
                            "prediction_provenance": dict(signal.prediction_provenance or {}),
                        }
                    )
                    trader_context_payload.update(
                        {
                            "fair_yes_dollars": str(signal.fair_yes_dollars),
                            "confidence": signal.confidence,
                            "forecast_delta_f": signal.forecast_delta_f,
                            "confidence_band": signal.confidence_band,
                            "trade_regime": signal.trade_regime,
                            "strategy_mode": signal.strategy_mode.value,
                            "resolution_state": signal.resolution_state.value,
                            "numeric_facts": numeric_facts,
                        }
                    )
                signal_record = await repo.save_signal(
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
                        "trader_context": trader_context_payload,
                        "research_freshness": dossier.freshness.model_dump(mode="json"),
                        "effective_research_freshness": dossier.freshness.model_dump(mode="json"),
                        "market_snapshot": signal_market_snapshot,
                        "trade_behavior_context": signal_trade_behavior_context,
                        "modeling": signal_modeling,
                        "prediction_model": signal_modeling["prediction_model"],
                        "trade_selection_model": signal_modeling["trade_selection_model"],
                        "resolution_state": signal.resolution_state.value,
                        "strategy_mode": signal.strategy_mode.value,
                        "evaluation_outcome": signal.evaluation_outcome,
                        "candidate_trace": signal.candidate_trace,
                        "trade_regime": signal.trade_regime,
                        "capital_bucket": signal.capital_bucket,
                        "recommended_side": signal.recommended_side.value if signal.recommended_side is not None else None,
                        "forecast_delta_f": signal.forecast_delta_f,
                        "confidence_band": signal.confidence_band,
                        "prediction_provenance": signal.prediction_provenance,
                        "model_quality_status": signal.model_quality_status,
                        "model_quality_reasons": signal.model_quality_reasons,
                        "recommended_size_cap_fp": (
                            str(signal.recommended_size_cap_fp) if signal.recommended_size_cap_fp is not None else None
                        ),
                        "size_factor": str(signal.size_factor),
                        "warn_only_blocked": signal.warn_only_blocked,
                        "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
                        "empirical_gate": (
                            signal.eligibility.empirical_gate
                            if signal.eligibility is not None
                            else (signal.candidate_trace or {}).get("empirical_gate")
                        ),
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
                signal_decision_ts = getattr(signal_record, "created_at", None) or market_state.observed_at
                decision_market_snapshot = await archive_point_in_time_market_snapshot(
                    repo,
                    market_response=market_response,
                    observed_at=market_state.observed_at,
                    kalshi_env=room.kalshi_env,
                    market_ticker=room.market_ticker,
                    source_kind=DECISION_SIGNAL_MARKET_SOURCE_KIND,
                    source_id=f"signal:{signal_record.id}",
                    recovered=False,
                    leakage_risk="none",
                    decision_ts=signal_decision_ts,
                    extra_payload={"signal_id": signal_record.id, "room_id": room.id},
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
                    market_snapshot_artifact = await repo.save_artifact(
                        room_id=room.id,
                        artifact_type="market_snapshot",
                        source="kalshi_rest",
                        title=f"Market snapshot for {room.market_ticker}",
                        payload=_market_snapshot_artifact_payload(
                            market_response,
                            observed_at=market_state.observed_at,
                            kalshi_env=room.kalshi_env,
                            market_ticker=room.market_ticker,
                        ),
                    )
                    await self._run_deterministic_fast_path(
                        repo=repo,
                        session=session,
                        room=room,
                        control=control,
                        signal=signal,
                        thresholds=thresholds,
                        market_observed_at=market_state.observed_at,
                        research_fallback_time=dossier.freshness.refreshed_at,
                        signal_record=signal_record,
                        source_snapshot_ids={
                            "market_state": {
                                "kalshi_env": room.kalshi_env,
                                "market_ticker": room.market_ticker,
                                "observed_at": market_state.observed_at,
                            },
                            "market_snapshot_artifact_id": market_snapshot_artifact.id,
                            "decision_market_snapshot_id": (
                                decision_market_snapshot.id if decision_market_snapshot is not None else None
                            ),
                            "weather_archive_source_id": weather_archive_source_id,
                            "research_run_id": dossier.last_run_id,
                        },
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
                        llm_trade_behavior_context = trade_behavior_context_payload(
                            market_ticker=room.market_ticker,
                            side=ticket.side.value,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                            yes_price_dollars=ticket.yes_price_dollars,
                            forecast_delta_f=signal.forecast_delta_f,
                            confidence_band=signal.confidence_band,
                            spread_bps=signal.eligibility.market_spread_bps if signal.eligibility is not None else None,
                        )
                        ticket_record.payload = _payload_with_trade_behavior_context(
                            ticket_record.payload,
                            llm_trade_behavior_context,
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
                            risk_max_credible_edge_bps=thresholds.risk_max_credible_edge_bps,
                            risk_min_confidence=thresholds.risk_min_confidence,
                            risk_min_contract_price_dollars=thresholds.risk_min_contract_price_dollars,
                            strategy_min_abs_delta_f=thresholds.strategy_min_abs_delta_f,
                        )
                        portfolio_bucket_snapshot = await repo.portfolio_bucket_snapshot(
                            kalshi_env=room.kalshi_env,
                            subaccount=self.settings.kalshi_subaccount,
                            total_capital_dollars=total_capital or Decimal("0"),
                            safe_capital_reserve_ratio=effective_thresholds.risk_safe_capital_reserve_ratio,
                            risky_capital_max_ratio=effective_thresholds.risk_risky_capital_max_ratio,
                        )
                        all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
                        open_ticker_count = len(
                            {p.market_ticker for p in all_positions if Decimal(str(p.count_fp)) > Decimal("0")}
                        )
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
                        daily_loss_cap_pct_llm = weather_live_daily_loss_cap_pct(
                            control=control,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                            default_pct=float(self.settings.risk_daily_loss_pct),
                        )
                        weather_live_guard_llm = weather_live_entry_freeze_bypassed(
                            control=control,
                            strategy_code=StrategyCode.DIRECTIONAL.value,
                        )
                        if daily_pnl_llm is not None and _cap > 0 and daily_loss_cap_pct_llm > 0:
                            _daily_loss_ratio_llm = float(-daily_pnl_llm) / _cap
                            _daily_hard_blocked_llm = (
                                _daily_loss_ratio_llm >= daily_loss_cap_pct_llm
                                and not weather_live_guard_llm
                            )
                        if weather_live_guard_llm:
                            realized_loss_dollars_llm = max(Decimal("0"), -Decimal(str(strategy_daily_pnl)))
                            realized_loss_cap_dollars_llm = _weather_realized_loss_cap_dollars(
                                total_capital=total_capital or Decimal("0"),
                                cap_pct=daily_loss_cap_pct_llm,
                                min_loss_dollars=self.settings.weather_live_probe_min_loss_dollars,
                            )
                            if realized_loss_dollars_llm >= realized_loss_cap_dollars_llm:
                                capped_ticket, _probe_cap_trace = _cap_ticket_notional(
                                    ticket,
                                    max_notional_dollars=self.settings.weather_live_probe_max_order_notional_dollars,
                                )
                                if capped_ticket is not None:
                                    ticket = capped_ticket
                                effective_thresholds = RuntimeThresholds(
                                    risk_min_edge_bps=max(
                                        effective_thresholds.risk_min_edge_bps,
                                        self.settings.weather_live_probe_min_net_edge_bps,
                                    ),
                                    risk_max_order_notional_dollars=_min_optional_cap(
                                        effective_thresholds.risk_max_order_notional_dollars,
                                        self.settings.weather_live_probe_max_order_notional_dollars,
                                    ),
                                    risk_max_position_notional_dollars=_min_optional_cap(
                                        effective_thresholds.risk_max_position_notional_dollars,
                                        self.settings.weather_live_probe_max_order_notional_dollars,
                                    ),
                                    risk_safe_capital_reserve_ratio=effective_thresholds.risk_safe_capital_reserve_ratio,
                                    risk_risky_capital_max_ratio=effective_thresholds.risk_risky_capital_max_ratio,
                                    trigger_max_spread_bps=effective_thresholds.trigger_max_spread_bps,
                                    trigger_cooldown_seconds=effective_thresholds.trigger_cooldown_seconds,
                                    strategy_quality_edge_buffer_bps=effective_thresholds.strategy_quality_edge_buffer_bps,
                                    strategy_min_remaining_payout_bps=effective_thresholds.strategy_min_remaining_payout_bps,
                                    risk_max_credible_edge_bps=effective_thresholds.risk_max_credible_edge_bps,
                                    risk_min_confidence=max(
                                        effective_thresholds.risk_min_confidence,
                                        self.settings.weather_live_probe_min_confidence,
                                    ),
                                    risk_min_contract_price_dollars=effective_thresholds.risk_min_contract_price_dollars,
                                    strategy_min_abs_delta_f=effective_thresholds.strategy_min_abs_delta_f,
                                )
                        if _daily_hard_blocked_llm:
                            verdict = RiskVerdictPayload(
                                status=RiskStatus.BLOCKED,
                                reasons=[
                                    f"Daily loss circuit breaker: {_daily_loss_ratio_llm:.1%} loss "
                                    f">= {daily_loss_cap_pct_llm:.0%} hard limit."
                                ],
                                reason_codes=["daily_loss_circuit_breaker"],
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
                        await repo.update_trade_ticket_status(
                            ticket_record.id,
                            "approved" if verdict.status == RiskStatus.APPROVED else "blocked",
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
                                        min_edge_bps=effective_thresholds.risk_min_edge_bps,
                                    )
                                else:
                                    receipt = ExecReceiptPayload(
                                        status="lock_denied",
                                        client_order_id=client_order_id,
                                        details={"reason": "execution lock held by another deployment color"},
                                    )
                            ORDERS_TOTAL.labels(status=receipt.status).inc()
                            if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                                order_raw = _payload_with_trade_behavior_context(
                                    receipt.details,
                                    llm_trade_behavior_context,
                                )
                                await repo.save_order(
                                    ticket_id=ticket_record.id,
                                    client_order_id=client_order_id,
                                    market_ticker=approved_ticket.market_ticker,
                                    status=receipt.status,
                                    side=approved_ticket.side.value,
                                    action=approved_ticket.action.value,
                                    yes_price_dollars=approved_ticket.yes_price_dollars,
                                    count_fp=approved_ticket.count_fp,
                                    raw=order_raw,
                                    kalshi_order_id=receipt.external_order_id,
                                    kalshi_env=room.kalshi_env,
                                    strategy_code=StrategyCode.DIRECTIONAL.value,
                                )
                            await repo.update_trade_ticket_status(ticket_record.id, receipt.status)
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
