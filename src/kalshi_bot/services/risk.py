from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RiskStatus, WeatherResolutionState
from kalshi_bot.core.fixed_point import as_decimal, quantize_count
from kalshi_bot.core.schemas import PortfolioBucketSnapshot, RiskVerdictPayload, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.decision_policy_variants import (
    LOW_PRICE_HIGH_EDGE,
    REMAINING_PAYOUT_RELAXATION,
    DecisionPolicyVariantService,
    policy_variant_applied,
)
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.risk_policy import probability_midband_block_reason
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars, remaining_payout_dollars
from kalshi_bot.services.strategy_cleanup import CleanupSignal
from kalshi_bot.services.trade_behavior import entry_pause_reason, weather_live_max_order_count_fp


@dataclass(slots=True)
class RiskContext:
    market_observed_at: datetime | None
    research_observed_at: datetime | None
    decision_time: datetime | None = None
    total_capital_dollars: Decimal | None = None
    current_position_notional_dollars: Decimal = Decimal("0")
    current_position_count_fp: Decimal = Decimal("0")
    current_position_side: str | None = None
    pending_order_count_fp: Decimal = Decimal("0")
    pending_order_notional_dollars: Decimal = Decimal("0")
    portfolio_bucket_snapshot: PortfolioBucketSnapshot | None = None
    open_ticker_count: int = 0
    strategy_code: str | None = None
    # Realized P&L over the last 24h for this strategy. Negative = loss.
    # Populated by the caller; risk engine gates on it against
    # Settings.risk_daily_loss_dollars_by_strategy[strategy_code].
    strategy_daily_realized_pnl_dollars: Decimal | None = None


def _quantize_money(value: Any) -> Decimal:
    return as_decimal(value).quantize(Decimal("0.0001"))


def _ticket_unit_notional(ticket: TradeTicket) -> Decimal:
    return ticket.yes_price_dollars if ticket.side.value == "yes" else Decimal("1.0000") - ticket.yes_price_dollars


def _fee_adjusted_edge_for_count(
    *,
    settings: Settings,
    contract_price: Decimal,
    count_fp: Decimal,
    gross_edge_bps: int,
) -> tuple[Decimal, int, int]:
    fee_estimate_dollars = estimate_kalshi_taker_fee_dollars(
        price_dollars=contract_price,
        count=count_fp,
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    fee_estimate_dollars_per_contract = (
        (fee_estimate_dollars / count_fp).quantize(
            Decimal("0.0001"),
            rounding=ROUND_CEILING,
        )
        if count_fp > Decimal("0")
        else Decimal("0.0000")
    )
    fee_edge_bps = int(
        (fee_estimate_dollars_per_contract * Decimal("10000")).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    return fee_estimate_dollars_per_contract, fee_edge_bps, gross_edge_bps - fee_edge_bps


def _close_strike_probe_unlocked(signal: StrategySignal) -> bool:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    bootstrap = trace.get("weather_empirical_bootstrap")
    if not isinstance(bootstrap, dict):
        bootstrap = (
            (signal.eligibility.candidate_trace or {}).get("weather_empirical_bootstrap")
            if signal.eligibility is not None and isinstance(signal.eligibility.candidate_trace, dict)
            else {}
        )
    if not isinstance(bootstrap, dict):
        return False
    return bool(bootstrap.get("applied")) and "close_strike_probe_unlocked" in {
        str(code) for code in bootstrap.get("reason_codes") or []
    }


def _crypto_late_sure_thing_edge_bypass(signal: StrategySignal, context: RiskContext) -> bool:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    strategy_code = str(context.strategy_code or trace.get("strategy_code") or "")
    return strategy_code == "CRYPTO_15M" and trace.get("late_high_confidence_directional_entry") is True


def _crypto_last_minute_passive_edge_bypass(signal: StrategySignal, context: RiskContext) -> bool:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    strategy_code = str(context.strategy_code or trace.get("strategy_code") or "")
    return strategy_code == "CRYPTO_15M" and trace.get("last_minute_passive_market_confidence") is True


def _asset_set(value: str | None) -> set[str]:
    assets: set[str] = set()
    for raw in str(value or "").replace(";", ",").split(","):
        stripped = raw.strip().upper()
        if not stripped:
            continue
        if stripped == "*":
            assets.add("*")
            continue
        normalized = "".join(ch for ch in stripped if ch.isalnum())
        if normalized:
            assets.add(normalized)
    return assets


def _asset_allowed_by_scope(asset: str | None, configured_assets: set[str]) -> bool:
    if asset is None:
        return False
    if not configured_assets:
        return True
    if configured_assets.intersection({"ALL", "ANY", "LIVE", "*"}):
        return True
    return asset in configured_assets


def _crypto_add_on_asset(ticket: TradeTicket, candidate_trace: dict[str, Any]) -> str | None:
    raw = candidate_trace.get("asset_symbol")
    if raw not in (None, ""):
        return "".join(ch for ch in str(raw).upper() if ch.isalnum()) or None
    features = candidate_trace.get("features")
    if isinstance(features, dict) and features.get("asset") not in (None, ""):
        return "".join(ch for ch in str(features["asset"]).upper() if ch.isalnum()) or None
    bucket_key = candidate_trace.get("bucket_key")
    if isinstance(bucket_key, str) and "|" in bucket_key:
        return "".join(ch for ch in bucket_key.split("|", 1)[0].upper() if ch.isalnum()) or None
    ticker = str(ticket.market_ticker or "").upper()
    if ticker.startswith("KX") and "15M" in ticker:
        between = ticker[2:ticker.index("15M")]
        return "".join(ch for ch in between if ch.isalnum()) or None
    return None


def _crypto_candidate_live_quality(candidate_trace: dict[str, Any]) -> bool:
    if candidate_trace.get("candidate_status") == "live_quality":
        return True
    selection = candidate_trace.get("trade_selection_model")
    if isinstance(selection, dict):
        return selection.get("candidate_status") == "live_quality" or selection.get("status") == "live_quality"
    return False


def _crypto_empirical_bucket_allowed(candidate_trace: dict[str, Any]) -> bool:
    gate = candidate_trace.get("empirical_bucket_gate")
    if not isinstance(gate, dict):
        selection = candidate_trace.get("trade_selection_model")
        if isinstance(selection, dict):
            gate = selection.get("empirical_bucket_gate")
    return isinstance(gate, dict) and (
        gate.get("allowed") is True or gate.get("status") in {"allowed", "override_allowed"}
    )


def _crypto_empirical_late_override_gate(candidate_trace: dict[str, Any]) -> dict[str, Any] | None:
    gate = candidate_trace.get("empirical_bucket_gate")
    if not isinstance(gate, dict):
        selection = candidate_trace.get("trade_selection_model")
        if isinstance(selection, dict):
            gate = selection.get("empirical_bucket_gate")
    if not isinstance(gate, dict):
        return None
    if gate.get("override_allowed") is True or gate.get("status") == "override_allowed":
        return gate
    late_override = gate.get("late_override")
    if isinstance(late_override, dict) and late_override.get("allowed") is True:
        return gate
    return None


def _crypto_position_add_on_verdict(
    *,
    settings: Settings,
    ticket: TradeTicket,
    signal: StrategySignal,
    context: RiskContext,
    candidate_trace: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    strategy_code = str(context.strategy_code or candidate_trace.get("strategy_code") or "")
    asset = _crypto_add_on_asset(ticket, candidate_trace)
    configured_assets = _asset_set(settings.crypto_position_add_on_assets)
    projected_count = context.current_position_count_fp + context.pending_order_count_fp + ticket.count_fp
    diagnostics = {
        "enabled": bool(settings.crypto_position_add_ons_enabled),
        "strategy_code": strategy_code,
        "asset_symbol": asset,
        "configured_assets": sorted(configured_assets),
        "current_position_side": context.current_position_side,
        "ticket_side": ticket.side.value,
        "current_position_count_fp": _decimal_text(context.current_position_count_fp),
        "pending_order_count_fp": _decimal_text(context.pending_order_count_fp),
        "ticket_count_fp": _decimal_text(ticket.count_fp),
        "projected_position_count_fp": _decimal_text(projected_count),
        "max_position_count_fp": _decimal_text(settings.crypto_position_add_on_max_position_count_fp),
        "max_ticket_count_fp": _decimal_text(settings.crypto_position_add_on_max_ticket_count_fp),
        "candidate_live_quality": _crypto_candidate_live_quality(candidate_trace),
        "empirical_bucket_allowed": _crypto_empirical_bucket_allowed(candidate_trace),
    }
    if not settings.crypto_position_add_ons_enabled:
        return False, {**diagnostics, "reason": "crypto_position_add_ons_disabled"}
    if strategy_code != "CRYPTO_15M":
        return False, {**diagnostics, "reason": "not_crypto_15m_strategy"}
    if not _asset_allowed_by_scope(asset, configured_assets):
        return False, {**diagnostics, "reason": "crypto_position_add_on_asset_not_configured"}
    if context.current_position_count_fp <= 0:
        return False, {**diagnostics, "reason": "no_existing_position"}
    if context.current_position_side != ticket.side.value:
        return False, {**diagnostics, "reason": "opposite_side_position"}
    if not _crypto_candidate_live_quality(candidate_trace):
        return False, {**diagnostics, "reason": "candidate_not_live_quality"}
    if not _crypto_empirical_bucket_allowed(candidate_trace):
        return False, {**diagnostics, "reason": "empirical_bucket_not_allowed"}
    if ticket.count_fp > Decimal(str(settings.crypto_position_add_on_max_ticket_count_fp)):
        return False, {**diagnostics, "reason": "ticket_count_above_crypto_add_on_cap"}
    if projected_count > Decimal(str(settings.crypto_position_add_on_max_position_count_fp)):
        return False, {**diagnostics, "reason": "projected_position_above_crypto_add_on_cap"}
    return True, {**diagnostics, "reason": "crypto_position_add_on_allowed"}


def _fit_count_for_notional(
    *,
    available_notional_dollars: Decimal,
    ticket: TradeTicket,
    minimum_count_fp: Decimal,
) -> Decimal | None:
    unit_notional = _ticket_unit_notional(ticket)
    if unit_notional <= Decimal("0"):
        return None
    raw_count = (available_notional_dollars / unit_notional).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if raw_count < minimum_count_fp:
        return None
    return quantize_count(raw_count)


def _bucket_fit_count(*, available_notional_dollars: Decimal, ticket: TradeTicket) -> Decimal | None:
    return _fit_count_for_notional(
        available_notional_dollars=available_notional_dollars,
        ticket=ticket,
        minimum_count_fp=Decimal("1.00"),
    )


def _decimal_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return str(Decimal(str(value)).quantize(Decimal("0.0001")))
    except Exception:
        return str(value)


def _candidate_for_side(candidate_trace: dict[str, Any], side: str | None) -> dict[str, Any]:
    if side in {"yes", "no"} and isinstance(candidate_trace.get(side), dict):
        return dict(candidate_trace[side])
    selected = candidate_trace.get("selected_candidate")
    if isinstance(selected, dict):
        return dict(selected)
    for candidate in candidate_trace.get("candidates") or []:
        if isinstance(candidate, dict) and (side is None or candidate.get("side") == side):
            return dict(candidate)
    return {}


def _max_credible_edge_diagnostics(
    *,
    ticket: TradeTicket,
    signal: StrategySignal,
    context: RiskContext,
) -> dict[str, Any]:
    selected_side = ticket.side.value
    candidate_trace = dict(signal.candidate_trace or {})
    selected = _candidate_for_side(candidate_trace, selected_side)
    weather = signal.weather
    side_fair = signal.fair_yes_dollars if selected_side == "yes" else Decimal("1.0000") - signal.fair_yes_dollars
    traded_price = _ticket_unit_notional(ticket)
    target_yes = signal.target_yes_price_dollars or ticket.yes_price_dollars
    return {
        "fair_yes_dollars": _decimal_text(signal.fair_yes_dollars),
        "selected_side": selected_side,
        "side_fair_dollars": _decimal_text(selected.get("fair_side_dollars") or side_fair),
        "target_yes_price_dollars": _decimal_text(target_yes),
        "ticket_yes_price_dollars": _decimal_text(ticket.yes_price_dollars),
        "traded_price_dollars": _decimal_text(selected.get("traded_price_dollars") or traded_price),
        "spread_bps": selected.get("spread_bps") if selected else candidate_trace.get("spread_bps"),
        "forecast_high_f": getattr(weather, "forecast_high_f", None) if weather is not None else None,
        "threshold_f": selected.get("threshold_f") or candidate_trace.get("threshold_f"),
        "operator": selected.get("operator") or candidate_trace.get("operator"),
        "forecast_delta_f": getattr(weather, "forecast_delta_f", None) if weather is not None else signal.forecast_delta_f,
        "market_observed_at": context.market_observed_at.isoformat() if context.market_observed_at else None,
        "research_observed_at": context.research_observed_at.isoformat() if context.research_observed_at else None,
        "source_snapshot_ids": candidate_trace.get("source_snapshot_ids") or {},
    }


def approved_ticket_for_verdict(ticket: TradeTicket, verdict: RiskVerdictPayload) -> TradeTicket:
    approved_count = verdict.approved_count_fp or ticket.count_fp
    return ticket.model_copy(update={"count_fp": approved_count})


class DeterministicRiskEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @staticmethod
    def _as_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def evaluate(
        self,
        *,
        room: Room,
        control: DeploymentControl,
        ticket: TradeTicket,
        signal: StrategySignal,
        context: RiskContext,
        thresholds: RuntimeThresholds | None = None,
    ) -> RiskVerdictPayload:
        reasons: list[str] = []
        blocking_reasons: list[str] = []
        reason_codes: list[str] = []
        diagnostics: dict[str, Any] = {}

        def note(reason: str) -> None:
            if reason not in reasons:
                reasons.append(reason)

        def block(reason: str) -> None:
            note(reason)
            if reason not in blocking_reasons:
                blocking_reasons.append(reason)

        def code(value: str) -> None:
            if value not in reason_codes:
                reason_codes.append(value)

        now = self._as_utc(context.decision_time) or datetime.now(UTC)
        market_observed_at = self._as_utc(context.market_observed_at)
        research_observed_at = self._as_utc(context.research_observed_at)
        active_thresholds = thresholds or RuntimeThresholds(
            risk_min_edge_bps=self.settings.risk_min_edge_bps,
            risk_max_credible_edge_bps=self.settings.risk_max_credible_edge_bps,
            risk_min_confidence=self.settings.risk_min_confidence,
            risk_min_contract_price_dollars=self.settings.risk_min_contract_price_dollars,
            risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
            risk_safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
            risk_risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
            trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
            trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_abs_delta_f=self.settings.strategy_min_abs_delta_f,
            strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
        )
        capital_bucket = signal.capital_bucket or "safe"
        order_notional = _quantize_money(estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp))
        approved_count = ticket.count_fp
        approved_notional = order_notional
        gross_edge_bps = signal.edge_bps
        candidate_trace = dict(signal.candidate_trace or {})
        last_minute_passive_edge_bypass = _crypto_last_minute_passive_edge_bypass(signal, context)
        # Late high-confidence crypto entries still need to clear the normal edge
        # floors; only the explicit last-minute passive path can bypass them.
        edge_floor_bypass = last_minute_passive_edge_bypass
        policy_service = DecisionPolicyVariantService(self.settings)
        applied_policy_variant = candidate_trace.get("policy_variant_applied")
        fee_estimate_dollars_per_contract: Decimal | None = None
        fee_edge_bps: int | None = None
        net_edge_bps: int | None = None
        bucket_limit_dollars: Decimal | None = None
        bucket_used_dollars_before: Decimal | None = None
        bucket_used_dollars_after: Decimal | None = None
        resized_by_bucket = False
        resized_by_count_cap = False
        is_buy_entry = ticket.action.value == "buy"
        is_sell_exit = ticket.action.value == "sell"
        risk_reducing_exit = (
            is_sell_exit
            and context.current_position_count_fp > 0
            and (
                context.current_position_side is None
                or context.current_position_side == ticket.side.value
            )
        )
        diagnostics["risk_reducing_exit"] = risk_reducing_exit
        diagnostics["policy_variants"] = {
            "policy_variant_applied": applied_policy_variant,
            "variants": candidate_trace.get("policy_variants") if isinstance(candidate_trace.get("policy_variants"), dict) else {},
        }
        diagnostics["active_thresholds"] = {
            "risk_min_edge_bps": active_thresholds.risk_min_edge_bps,
            "risk_max_credible_edge_bps": active_thresholds.risk_max_credible_edge_bps,
            "risk_min_confidence": active_thresholds.risk_min_confidence,
            "risk_min_contract_price_dollars": active_thresholds.risk_min_contract_price_dollars,
            "risk_max_order_notional_dollars": active_thresholds.risk_max_order_notional_dollars,
            "risk_max_position_notional_dollars": active_thresholds.risk_max_position_notional_dollars,
            "risk_position_pct": self.settings.risk_position_pct,
            "trigger_max_spread_bps": active_thresholds.trigger_max_spread_bps,
            "strategy_min_abs_delta_f": active_thresholds.strategy_min_abs_delta_f,
            "strategy_min_remaining_payout_bps": active_thresholds.strategy_min_remaining_payout_bps,
        }

        if is_sell_exit and not risk_reducing_exit:
            block("Sell ticket does not match an existing same-side position.")

        if control.kill_switch_enabled:
            if risk_reducing_exit:
                note("Global kill switch is enabled; risk-reducing exit is allowed.")
            else:
                block("Global kill switch is enabled.")
        pause_reason = (
            entry_pause_reason(
                settings=self.settings,
                control=control,
                kalshi_env=room.kalshi_env,
                strategy_code=context.strategy_code,
            )
            if is_buy_entry
            else None
        )
        if pause_reason is not None:
            block(pause_reason)
        if signal.recommended_action is None or signal.recommended_side is None:
            block("Signal engine did not recommend a live trade.")
        if signal.resolution_state != WeatherResolutionState.UNRESOLVED:
            block("Contract is already resolved under the base weather strategy.")
        if signal.eligibility is not None and not signal.eligibility.eligible:
            for reason in signal.eligibility.reasons:
                block(reason)
        if signal.edge_bps < active_thresholds.risk_min_edge_bps and not edge_floor_bypass:
            block(f"Edge {signal.edge_bps}bps is below configured minimum of {active_thresholds.risk_min_edge_bps}bps.")
        elif signal.edge_bps < active_thresholds.risk_min_edge_bps and last_minute_passive_edge_bypass:
            note(
                f"Last-minute passive market-confidence entry bypassed the model edge floor "
                f"({signal.edge_bps}bps versus {active_thresholds.risk_min_edge_bps}bps)."
            )
            code("last_minute_passive_edge_bypass")
        if signal.edge_bps > active_thresholds.risk_max_credible_edge_bps:
            extreme_diag = candidate_trace.get("extreme_edge_diagnostic")
            validated_extreme_edge = (
                candidate_trace.get("validated_extreme_edge") is True
                and isinstance(extreme_diag, dict)
                and extreme_diag.get("passed") is True
            )
            if validated_extreme_edge:
                note(
                    f"Edge {signal.edge_bps}bps exceeds credibility limit but passed "
                    "the extreme-edge diagnostic class."
                )
                code("max_credible_edge_validated")
                diagnostics["max_credible_edge"] = {
                    **_max_credible_edge_diagnostics(
                        ticket=ticket,
                        signal=signal,
                        context=context,
                    ),
                    "validated_extreme_edge": True,
                    "extreme_edge_diagnostic": extreme_diag,
                }
            else:
                block(
                    f"Edge {signal.edge_bps}bps exceeds credibility limit of "
                    f"{active_thresholds.risk_max_credible_edge_bps}bps; likely model error."
                )
                code("max_credible_edge")
                diagnostics["max_credible_edge"] = _max_credible_edge_diagnostics(
                    ticket=ticket,
                    signal=signal,
                    context=context,
                )
        if signal.confidence < active_thresholds.risk_min_confidence:
            block(
                f"Signal confidence {signal.confidence:.2f} is below minimum "
                f"{active_thresholds.risk_min_confidence:.2f}."
            )
        baseline_min_price = Decimal(str(active_thresholds.risk_min_contract_price_dollars))
        min_price = baseline_min_price
        if policy_variant_applied(candidate_trace, LOW_PRICE_HIGH_EDGE) and policy_service.live_enabled(LOW_PRICE_HIGH_EDGE):
            min_price = Decimal(str(self.settings.low_price_variant_min_entry_price_dollars))
        contract_price = (
            ticket.yes_price_dollars
            if ticket.side.value == "yes"
            else Decimal("1.0000") - ticket.yes_price_dollars
        )
        diagnostics["minimum_contract_price"] = {
            "contract_price_dollars": _decimal_text(contract_price),
            "baseline_min_price_dollars": _decimal_text(baseline_min_price),
            "effective_min_price_dollars": _decimal_text(min_price),
            "policy_variant_applied": LOW_PRICE_HIGH_EDGE
            if min_price != baseline_min_price
            else None,
        }
        if contract_price < min_price:
            block(
                f"Contract price {contract_price} is below minimum {min_price}; "
                f"market has priced this as nearly impossible."
            )
            code("contract_price_below_min")
        elif min_price != baseline_min_price:
            note(
                f"Policy variant {LOW_PRICE_HIGH_EDGE} lowered the minimum entry price "
                f"from {baseline_min_price} to {min_price}."
            )
            code("low_price_policy_variant_applied")
        remaining_payout = remaining_payout_dollars(ticket.side, ticket.yes_price_dollars)
        baseline_minimum_remaining_payout_bps = int(active_thresholds.strategy_min_remaining_payout_bps)
        if (
            policy_variant_applied(candidate_trace, REMAINING_PAYOUT_RELAXATION)
            and policy_service.live_enabled(REMAINING_PAYOUT_RELAXATION)
        ):
            minimum_remaining_payout_bps = int(self.settings.remaining_payout_variant_min_payout_bps)
        else:
            minimum_remaining_payout_bps = baseline_minimum_remaining_payout_bps
        minimum_remaining_payout = Decimal(minimum_remaining_payout_bps) / Decimal("10000")
        diagnostics["remaining_payout"] = {
            "remaining_payout_dollars": _decimal_text(remaining_payout),
            "minimum_remaining_payout_dollars": _decimal_text(minimum_remaining_payout),
            "minimum_remaining_payout_bps": minimum_remaining_payout_bps,
            "baseline_minimum_remaining_payout_bps": baseline_minimum_remaining_payout_bps,
            "policy_variant_applied": REMAINING_PAYOUT_RELAXATION
            if minimum_remaining_payout_bps != baseline_minimum_remaining_payout_bps
            else None,
        }
        if remaining_payout <= minimum_remaining_payout:
            block(
                f"Remaining payout {remaining_payout} is below configured minimum of "
                f"{minimum_remaining_payout:.4f}; payoff is too asymmetric for a new entry."
            )
            code("insufficient_remaining_payout")
        elif minimum_remaining_payout_bps != baseline_minimum_remaining_payout_bps:
            note(
                f"Policy variant {REMAINING_PAYOUT_RELAXATION} lowered the remaining-payout floor "
                f"from {baseline_minimum_remaining_payout_bps}bps to {minimum_remaining_payout_bps}bps."
            )
            code("remaining_payout_policy_variant_applied")
        if self.settings.risk_fee_aware_edge_enabled:
            (
                fee_estimate_dollars_per_contract,
                fee_edge_bps,
                net_edge_bps,
            ) = _fee_adjusted_edge_for_count(
                settings=self.settings,
                contract_price=contract_price,
                count_fp=ticket.count_fp,
                gross_edge_bps=gross_edge_bps,
            )
        probability_reason = probability_midband_block_reason(
            fair_yes=signal.fair_yes_dollars,
            edge_bps=net_edge_bps if net_edge_bps is not None else signal.edge_bps,
            base_min_edge_bps=active_thresholds.risk_min_edge_bps,
            extremity_pct=self.settings.risk_min_probability_extremity_pct,
            max_extra_edge_bps=getattr(self.settings, "risk_probability_midband_max_extra_edge_bps", 500),
        )
        if probability_reason is not None:
            block(probability_reason)

        if market_observed_at is None or (now - market_observed_at).total_seconds() > self.settings.risk_stale_market_seconds:
            block("Kalshi market data is stale.")
            code("market_stale")
        if research_observed_at is None or (now - research_observed_at).total_seconds() > self.settings.research_stale_seconds:
            block("Research data is stale.")
            code("research_stale")

        if is_buy_entry and last_minute_passive_edge_bypass and context.pending_order_count_fp > 0:
            block(
                f"Existing pending same-side order in {room.market_ticker} blocks duplicate "
                "last-minute passive entry."
            )
            code("last_minute_passive_duplicate_open_order")

        late_override_gate = _crypto_empirical_late_override_gate(candidate_trace)
        if is_buy_entry and context.strategy_code == "CRYPTO_15M" and late_override_gate is not None:
            late_override_cap = quantize_count(
                Decimal(str(self.settings.crypto_empirical_late_override_max_count_fp))
            )
            diagnostics["crypto_empirical_late_override"] = {
                "active": True,
                "gate_status": late_override_gate.get("status"),
                "override_reason": late_override_gate.get("override_reason"),
                "original_bucket_reason": late_override_gate.get("original_reason"),
                "max_count_fp": _decimal_text(late_override_cap),
                "original_count_fp": _decimal_text(approved_count),
            }
            if late_override_cap <= Decimal("0"):
                block("Crypto late empirical override count cap is non-positive.")
                code("crypto_late_empirical_override_invalid_cap")
            elif approved_count > late_override_cap:
                original_count = approved_count
                approved_count = late_override_cap
                approved_notional = _quantize_money(
                    estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, approved_count)
                )
                order_notional = approved_notional
                diagnostics["crypto_empirical_late_override"].update(
                    {
                        "resized": True,
                        "approved_count_fp": _decimal_text(approved_count),
                        "approved_order_notional_dollars": _decimal_text(order_notional),
                    }
                )
                note(
                    f"Late high-confidence empirical bucket override capped ticket from "
                    f"{original_count:.2f} to {approved_count:.2f} contracts."
                )
                code("crypto_late_empirical_override_count_cap")

        max_order_count_fp = weather_live_max_order_count_fp(
            control=control,
            strategy_code=context.strategy_code,
            default_count=float(self.settings.risk_max_order_count_fp),
        )
        diagnostics["max_order_count_fp"] = {
            "configured": float(self.settings.risk_max_order_count_fp),
            "effective": max_order_count_fp,
        }
        if float(ticket.count_fp) > max_order_count_fp:
            block("Ticket size exceeds max order count.")

        effective_position_count_fp = context.current_position_count_fp + context.pending_order_count_fp
        crypto_position_add_on_allowed = False
        if is_buy_entry and context.current_position_count_fp > 0 and not self.settings.risk_allow_position_add_ons:
            crypto_position_add_on_allowed, crypto_add_on_diag = _crypto_position_add_on_verdict(
                settings=self.settings,
                ticket=ticket,
                signal=signal,
                context=context,
                candidate_trace=candidate_trace,
            )
            diagnostics["crypto_position_add_on"] = crypto_add_on_diag
            if crypto_position_add_on_allowed:
                note(
                    f"Crypto same-side add-on is within capped add-on limits "
                    f"({crypto_add_on_diag['projected_position_count_fp']} contracts)."
                )
                code("crypto_position_add_on_allowed")
            else:
                block(
                    f"Existing live position in {room.market_ticker} blocks same-ticker add-ons; "
                    "no pyramiding is enabled outside approved crypto add-ons; "
                    f"crypto capped add-on check failed: {crypto_add_on_diag['reason']}."
                )
                code("crypto_position_add_on_blocked")
        if (
            is_buy_entry
            and context.current_position_count_fp > 0
            and context.current_position_side is not None
            and context.current_position_side != ticket.side.value
        ):
            block(
                f"Existing {context.current_position_side} position in {room.market_ticker} "
                f"blocks opposite-side {ticket.side.value} entry."
            )
        max_position_count_fp = Decimal(str(self.settings.risk_max_position_count_fp_per_ticker))
        if is_buy_entry and effective_position_count_fp >= max_position_count_fp:
            block(
                f"Position + in-flight orders in {room.market_ticker} at {effective_position_count_fp} contracts "
                f"(max {self.settings.risk_max_position_count_fp_per_ticker:.0f})."
            )
        elif is_buy_entry:
            projected_position_count_fp = effective_position_count_fp + approved_count
            if projected_position_count_fp > max_position_count_fp:
                fitted_count = quantize_count(
                    (max_position_count_fp - effective_position_count_fp).quantize(
                        Decimal("0.01"),
                        rounding=ROUND_DOWN,
                    )
                )
                if fitted_count < Decimal("1.00"):
                    block(
                        f"Projected position + in-flight orders in {room.market_ticker} would reach "
                        f"{projected_position_count_fp} contracts "
                        f"(max {self.settings.risk_max_position_count_fp_per_ticker:.0f})."
                    )
                else:
                    original_count = approved_count
                    approved_count = fitted_count
                    approved_notional = _quantize_money(
                        estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, approved_count)
                    )
                    order_notional = approved_notional
                    resized_by_count_cap = True
                    note(
                        f"Ticket downsized from {original_count:.2f} to {approved_count:.2f} contracts "
                        f"to fit the per-ticker count cap."
                    )

        opens_new_ticker = context.current_position_count_fp <= 0 and context.pending_order_count_fp <= 0
        if is_buy_entry and opens_new_ticker and context.open_ticker_count >= self.settings.risk_max_concurrent_tickers:
            block(
                f"Portfolio already has {context.open_ticker_count} open tickers "
                f"(max {self.settings.risk_max_concurrent_tickers})."
            )

        close_strike_probe = _close_strike_probe_unlocked(signal)
        non_standard_regime = signal.trade_regime in ("longshot_yes", "longshot_no") or (
            signal.trade_regime == "near_threshold" and not close_strike_probe
        )
        if is_buy_entry and non_standard_regime:
            block(
                f"Trade regime '{signal.trade_regime}' is not permitted; only standard-regime trades are allowed."
            )
            code("non_standard_regime")
        elif is_buy_entry and signal.trade_regime == "near_threshold" and close_strike_probe:
            note("Close-strike probe evidence permits this near-threshold entry under tiny-probe caps.")
            code("close_strike_probe_unlocked")

        if is_buy_entry and active_thresholds.risk_max_order_notional_dollars is not None and float(order_notional) > active_thresholds.risk_max_order_notional_dollars:
            block("Ticket notional exceeds max order notional.")
        current_position_notional = _quantize_money(context.current_position_notional_dollars)
        pending_position_notional = _quantize_money(context.pending_order_notional_dollars)
        dynamic_position_cap_dollars: Decimal | None = None
        total_capital = context.total_capital_dollars
        crypto_strategy = context.strategy_code == "CRYPTO_15M"
        if total_capital is None:
            if is_buy_entry and crypto_strategy:
                block("Total capital is unavailable; cannot enforce the crypto position capital cap.")
                code("position_notional_cap_missing_capital")
        else:
            total_capital_dec = _quantize_money(total_capital)
            if total_capital_dec <= Decimal("0"):
                if is_buy_entry and crypto_strategy:
                    block("Total capital is non-positive; cannot enforce the crypto position capital cap.")
                    code("position_notional_cap_missing_capital")
            elif self.settings.risk_position_pct > 0:
                dynamic_position_cap_dollars = _quantize_money(
                    total_capital_dec * Decimal(str(self.settings.risk_position_pct))
                )
        projected_position_notional = _quantize_money(
            current_position_notional + pending_position_notional + order_notional
        )
        diagnostics["position_notional_cap"] = {
            "total_capital_dollars": _decimal_text(total_capital),
            "risk_position_pct": self.settings.risk_position_pct,
            "dynamic_cap_dollars": _decimal_text(dynamic_position_cap_dollars),
            "configured_cap_dollars": _decimal_text(active_thresholds.risk_max_position_notional_dollars),
            "current_position_notional_dollars": _decimal_text(current_position_notional),
            "pending_order_notional_dollars": _decimal_text(pending_position_notional),
            "approved_order_notional_dollars": _decimal_text(order_notional),
            "projected_position_notional_dollars": _decimal_text(projected_position_notional),
        }
        if is_buy_entry and dynamic_position_cap_dollars is not None:
            existing_and_pending_notional = _quantize_money(current_position_notional + pending_position_notional)
            if existing_and_pending_notional >= dynamic_position_cap_dollars:
                block(
                    f"Position already uses {existing_and_pending_notional:.4f} of "
                    f"the {self.settings.risk_position_pct:.0%} capital cap "
                    f"({dynamic_position_cap_dollars:.4f})."
                )
                code("position_notional_cap_reached")
            elif projected_position_notional > dynamic_position_cap_dollars:
                available_notional = _quantize_money(dynamic_position_cap_dollars - existing_and_pending_notional)
                fitted_count = _fit_count_for_notional(
                    available_notional_dollars=available_notional,
                    ticket=ticket,
                    minimum_count_fp=Decimal("0.01"),
                )
                if fitted_count is None:
                    block(
                        f"Remaining position budget {available_notional:.4f} is below the minimum "
                        f"0.01 count_fp at the current contract price."
                    )
                    code("position_notional_cap_no_fit")
                else:
                    original_count = approved_count
                    approved_count = fitted_count
                    approved_notional = _quantize_money(
                        estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, approved_count)
                    )
                    order_notional = approved_notional
                    projected_position_notional = _quantize_money(
                        existing_and_pending_notional + order_notional
                    )
                    diagnostics["position_notional_cap"].update(
                        {
                            "available_notional_dollars": _decimal_text(available_notional),
                            "resized": True,
                            "original_count_fp": _decimal_text(original_count),
                            "approved_count_fp": _decimal_text(approved_count),
                            "approved_order_notional_dollars": _decimal_text(order_notional),
                            "projected_position_notional_dollars": _decimal_text(projected_position_notional),
                        }
                    )
                    note(
                        f"Ticket downsized from {original_count:.2f} to {approved_count:.2f} contracts "
                        f"so projected position notional stays within "
                        f"{self.settings.risk_position_pct:.0%} of capital."
                    )
                    code("position_notional_cap_resized")
        if (
            is_buy_entry
            and active_thresholds.risk_max_position_notional_dollars is not None
            and float(projected_position_notional) > active_thresholds.risk_max_position_notional_dollars
        ):
            block("Projected position exceeds max position notional.")

        # Per-strategy daily-loss envelope: hard stop distinct from the global
        # portfolio-wide daily-loss check in the supervisor. Each strategy can
        # have its own dollar cap so one bad strategy can't starve another of
        # capital.
        if (
            is_buy_entry
            and context.strategy_code is not None
            and context.strategy_daily_realized_pnl_dollars is not None
        ):
            cap_dollars = self.settings.risk_daily_loss_dollars_by_strategy.get(
                context.strategy_code
            )
            if cap_dollars is not None and cap_dollars > 0:
                realized_loss_dollars = -float(context.strategy_daily_realized_pnl_dollars)
                if realized_loss_dollars >= cap_dollars:
                    block(
                        f"Strategy {context.strategy_code} realized loss "
                        f"${realized_loss_dollars:.2f} has reached the "
                        f"${cap_dollars:.2f} daily cap."
                    )

        snapshot = context.portfolio_bucket_snapshot
        if is_buy_entry and snapshot is not None:
            bucket_limit_dollars = (
                snapshot.risky_limit_dollars
                if capital_bucket == "risky"
                else snapshot.total_capital_dollars
            )
            bucket_used_dollars_before = (
                snapshot.risky_used_dollars
                if capital_bucket == "risky"
                else snapshot.overall_used_dollars
            )
            available_notional = (
                snapshot.risky_remaining_dollars
                if capital_bucket == "risky"
                else snapshot.overall_remaining_dollars
            )
            bucket_used_dollars_after = _quantize_money(bucket_used_dollars_before + order_notional)
            if order_notional > available_notional:
                fitted_count = _bucket_fit_count(
                    available_notional_dollars=available_notional,
                    ticket=ticket,
                )
                if fitted_count is None:
                    if not non_standard_regime:
                        block(
                            f"{capital_bucket.capitalize()} capital bucket is full: "
                            f"used {bucket_used_dollars_before:.4f} of {bucket_limit_dollars:.4f}."
                        )
                        code(f"{capital_bucket}_capital_bucket_full")
                    bucket_used_dollars_after = bucket_used_dollars_before
                else:
                    approved_count = fitted_count
                    approved_notional = _quantize_money(
                        estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, approved_count)
                    )
                    bucket_used_dollars_after = _quantize_money(bucket_used_dollars_before + approved_notional)
                    resized_by_bucket = True
                    note(
                        f"Ticket downsized from {ticket.count_fp:.2f} to {approved_count:.2f} contracts "
                        f"to fit the {capital_bucket} capital bucket."
                    )

        if self.settings.risk_fee_aware_edge_enabled:
            (
                fee_estimate_dollars_per_contract,
                fee_edge_bps,
                net_edge_bps,
            ) = _fee_adjusted_edge_for_count(
                settings=self.settings,
                contract_price=contract_price,
                count_fp=approved_count,
                gross_edge_bps=gross_edge_bps,
            )
            if net_edge_bps < active_thresholds.risk_min_edge_bps and not edge_floor_bypass:
                block(
                    f"Fee-adjusted edge {net_edge_bps}bps is below configured minimum of "
                    f"{active_thresholds.risk_min_edge_bps}bps "
                    f"(gross {gross_edge_bps}bps, estimated taker fee {fee_edge_bps}bps)."
                )
                code("fee_adjusted_edge_below_min")
            elif net_edge_bps < active_thresholds.risk_min_edge_bps and last_minute_passive_edge_bypass:
                note(
                    f"Last-minute passive market-confidence entry bypassed the fee-adjusted edge floor "
                    f"({net_edge_bps}bps versus {active_thresholds.risk_min_edge_bps}bps)."
                )
                code("last_minute_passive_fee_adjusted_edge_bypass")
            else:
                note(
                    f"Fee-adjusted edge {net_edge_bps}bps clears the configured minimum "
                    f"of {active_thresholds.risk_min_edge_bps}bps."
                )
                code("fee_adjusted_edge_pass")
            final_probability_reason = probability_midband_block_reason(
                fair_yes=signal.fair_yes_dollars,
                edge_bps=net_edge_bps,
                base_min_edge_bps=active_thresholds.risk_min_edge_bps,
                extremity_pct=self.settings.risk_min_probability_extremity_pct,
                max_extra_edge_bps=getattr(self.settings, "risk_probability_midband_max_extra_edge_bps", 500),
            )
            if final_probability_reason is not None:
                block(final_probability_reason)

        if room.shadow_mode:
            note("Room is in shadow mode; execution will be simulated.")

        status = RiskStatus.APPROVED if not blocking_reasons else RiskStatus.BLOCKED
        return RiskVerdictPayload(
            status=status,
            reasons=reasons or ["All deterministic checks passed."],
            reason_codes=reason_codes,
            diagnostics=diagnostics,
            gross_edge_bps=gross_edge_bps,
            fee_edge_bps=fee_edge_bps,
            net_edge_bps=net_edge_bps,
            fee_estimate_dollars_per_contract=fee_estimate_dollars_per_contract,
            approved_notional_dollars=approved_notional if status == RiskStatus.APPROVED else None,
            approved_count_fp=approved_count if status == RiskStatus.APPROVED else None,
            capital_bucket=capital_bucket,
            bucket_limit_dollars=bucket_limit_dollars,
            bucket_used_dollars_before=bucket_used_dollars_before,
            bucket_used_dollars_after=(
                bucket_used_dollars_after if status == RiskStatus.APPROVED else bucket_used_dollars_before
            ),
            resized_by_bucket=resized_by_bucket if status == RiskStatus.APPROVED else False,
            resized_by_count_cap=resized_by_count_cap if status == RiskStatus.APPROVED else False,
        )


def evaluate_cleanup_risk(
    signal: CleanupSignal,
    *,
    control: DeploymentControl,
    settings: Settings,
    current_position_notional_dollars: Decimal = Decimal("0"),
    current_position_side: str | None = None,
) -> RiskVerdictPayload:
    """Deterministic risk gate for Strategy C cleanup signals.

    Skips all Strategy A gates (confidence, extremity, research staleness, etc.).
    Applies only: kill switch, Strategy C enabled flag, per-trade notional cap,
    per-position notional cap, and opposite-side guard.
    """
    blocking_reasons: list[str] = []
    reasons: list[str] = []

    def block(reason: str) -> None:
        reasons.append(reason)
        blocking_reasons.append(reason)

    if control.kill_switch_enabled:
        block("Global kill switch is enabled.")

    if not settings.strategy_c_enabled:
        block("Strategy C is not enabled (strategy_c_enabled=False).")

    order_notional = as_decimal(signal.target_price_cents / 100)
    max_order = Decimal(str(settings.strategy_c_max_order_notional_dollars))
    if order_notional > max_order:
        block(
            f"Target price {signal.target_price_cents:.2f}¢ implies notional {float(order_notional):.2f} "
            f"exceeds Strategy C order cap {settings.strategy_c_max_order_notional_dollars:.2f}."
        )

    max_position = Decimal(str(settings.strategy_c_max_position_notional_dollars))
    projected_position = current_position_notional_dollars + order_notional
    if projected_position > max_position:
        block(
            f"Projected position notional {float(projected_position):.2f} exceeds "
            f"Strategy C position cap {settings.strategy_c_max_position_notional_dollars:.2f}."
        )

    if current_position_side is not None and current_position_side != signal.side.value:
        block(
            f"Existing {current_position_side} position conflicts with new "
            f"{signal.side.value} cleanup signal; no opposite-side add-ons."
        )

    status = RiskStatus.APPROVED if not blocking_reasons else RiskStatus.BLOCKED
    return RiskVerdictPayload(
        status=status,
        reasons=reasons or ["Strategy C risk checks passed."],
    )
