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
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.risk_policy import probability_midband_block_reason
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars
from kalshi_bot.services.strategy_cleanup import CleanupSignal


@dataclass(slots=True)
class RiskContext:
    market_observed_at: datetime | None
    research_observed_at: datetime | None
    decision_time: datetime | None = None
    current_position_notional_dollars: Decimal = Decimal("0")
    current_position_count_fp: Decimal = Decimal("0")
    current_position_side: str | None = None
    pending_order_count_fp: Decimal = Decimal("0")
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


def _bucket_fit_count(*, available_notional_dollars: Decimal, ticket: TradeTicket) -> Decimal | None:
    unit_notional = _ticket_unit_notional(ticket)
    if unit_notional <= Decimal("0"):
        return None
    raw_count = (available_notional_dollars / unit_notional).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if raw_count < Decimal("1.00"):
        return None
    return quantize_count(raw_count)


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


def _source_health_pause_reason(control: DeploymentControl) -> str | None:
    notes = dict(control.notes or {})
    source_health = dict(notes.get("source_health") or {})
    if not source_health.get("pause_new_entries"):
        return None
    reason = source_health.get("pause_reason") or source_health.get("reason") or "source health degraded"
    label = source_health.get("aggregate_label")
    if label:
        return f"Source health pause is active ({label}): {reason}."
    return f"Source health pause is active: {reason}."


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
            risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
            risk_safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
            risk_risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
            trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
            trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
            strategy_quality_edge_buffer_bps=self.settings.strategy_quality_edge_buffer_bps,
            strategy_min_remaining_payout_bps=self.settings.strategy_min_remaining_payout_bps,
        )
        capital_bucket = signal.capital_bucket or "safe"
        order_notional = _quantize_money(estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp))
        approved_count = ticket.count_fp
        approved_notional = order_notional
        gross_edge_bps = signal.edge_bps
        fee_estimate_dollars_per_contract: Decimal | None = None
        fee_edge_bps: int | None = None
        net_edge_bps: int | None = None
        bucket_limit_dollars: Decimal | None = None
        bucket_used_dollars_before: Decimal | None = None
        bucket_used_dollars_after: Decimal | None = None
        resized_by_bucket = False
        resized_by_count_cap = False

        if control.kill_switch_enabled:
            block("Global kill switch is enabled.")
        pause_reason = _source_health_pause_reason(control)
        if pause_reason is not None:
            block(pause_reason)
        if signal.recommended_action is None or signal.recommended_side is None:
            block("Signal engine did not recommend a live trade.")
        if signal.resolution_state != WeatherResolutionState.UNRESOLVED:
            block("Contract is already resolved under the base weather strategy.")
        if signal.eligibility is not None and not signal.eligibility.eligible:
            for reason in signal.eligibility.reasons:
                block(reason)
        if signal.edge_bps < active_thresholds.risk_min_edge_bps:
            block(f"Edge {signal.edge_bps}bps is below configured minimum of {active_thresholds.risk_min_edge_bps}bps.")
        if signal.edge_bps > self.settings.risk_max_credible_edge_bps:
            candidate_trace = dict(signal.candidate_trace or {})
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
                    f"{self.settings.risk_max_credible_edge_bps}bps; likely model error."
                )
                code("max_credible_edge")
                diagnostics["max_credible_edge"] = _max_credible_edge_diagnostics(
                    ticket=ticket,
                    signal=signal,
                    context=context,
                )
        if signal.confidence < self.settings.risk_min_confidence:
            block(
                f"Signal confidence {signal.confidence:.2f} is below minimum "
                f"{self.settings.risk_min_confidence:.2f}."
            )
        min_price = Decimal(str(self.settings.risk_min_contract_price_dollars))
        contract_price = (
            ticket.yes_price_dollars
            if ticket.side.value == "yes"
            else Decimal("1.0000") - ticket.yes_price_dollars
        )
        if contract_price < min_price:
            block(
                f"Contract price {contract_price} is below minimum {min_price}; "
                f"market has priced this as nearly impossible."
            )
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
        if research_observed_at is None or (now - research_observed_at).total_seconds() > self.settings.research_stale_seconds:
            block("Research data is stale.")

        if float(ticket.count_fp) > self.settings.risk_max_order_count_fp:
            block("Ticket size exceeds max order count.")

        effective_position_count_fp = context.current_position_count_fp + context.pending_order_count_fp
        if context.current_position_count_fp > 0 and not self.settings.risk_allow_position_add_ons:
            block(
                f"Existing live position in {room.market_ticker} blocks same-ticker add-ons; "
                "no pyramiding is enabled."
            )
        if (
            context.current_position_count_fp > 0
            and context.current_position_side is not None
            and context.current_position_side != ticket.side.value
        ):
            block(
                f"Existing {context.current_position_side} position in {room.market_ticker} "
                f"blocks opposite-side {ticket.side.value} entry."
            )
        max_position_count_fp = Decimal(str(self.settings.risk_max_position_count_fp_per_ticker))
        if effective_position_count_fp >= max_position_count_fp:
            block(
                f"Position + in-flight orders in {room.market_ticker} at {effective_position_count_fp} contracts "
                f"(max {self.settings.risk_max_position_count_fp_per_ticker:.0f})."
            )
        else:
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
        if opens_new_ticker and context.open_ticker_count >= self.settings.risk_max_concurrent_tickers:
            block(
                f"Portfolio already has {context.open_ticker_count} open tickers "
                f"(max {self.settings.risk_max_concurrent_tickers})."
            )

        non_standard_regime = signal.trade_regime in ("near_threshold", "longshot_yes", "longshot_no")
        if non_standard_regime:
            block(
                f"Trade regime '{signal.trade_regime}' is not permitted; only standard-regime trades are allowed."
            )

        if active_thresholds.risk_max_order_notional_dollars is not None and float(order_notional) > active_thresholds.risk_max_order_notional_dollars:
            block("Ticket notional exceeds max order notional.")
        if active_thresholds.risk_max_position_notional_dollars is not None and float(context.current_position_notional_dollars + order_notional) > active_thresholds.risk_max_position_notional_dollars:
            block("Projected position exceeds max position notional.")

        # Per-strategy daily-loss envelope: hard stop distinct from the global
        # portfolio-wide daily-loss check in the supervisor. Each strategy can
        # have its own dollar cap so one bad strategy can't starve another of
        # capital.
        if (
            context.strategy_code is not None
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
        if snapshot is not None:
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
                    block(
                        f"{capital_bucket.capitalize()} capital bucket is full: "
                        f"used {bucket_used_dollars_before:.4f} of {bucket_limit_dollars:.4f}."
                    )
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
            if net_edge_bps < active_thresholds.risk_min_edge_bps:
                block(
                    f"Fee-adjusted edge {net_edge_bps}bps is below configured minimum of "
                    f"{active_thresholds.risk_min_edge_bps}bps "
                    f"(gross {gross_edge_bps}bps, estimated taker fee {fee_edge_bps}bps)."
                )
                code("fee_adjusted_edge_below_min")
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
