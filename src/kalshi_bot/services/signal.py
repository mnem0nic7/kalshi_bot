from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.fixed_point import as_decimal, quantize_price
from kalshi_bot.core.schemas import ResearchFreshness, TradeEligibilityVerdict
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import SigmaContext, WeatherSignalSnapshot, confidence_band_for, score_weather_market

NO_TRADE_SUMMARY_SENTENCE = "No taker trade clears the configured edge threshold."
ADVISORY_SIZE_CAP_FP = Decimal("10.00")
TAIL_PAYOUT_WARN_THRESHOLD = Decimal("0.9500")


@dataclass(slots=True)
class StrategySignal:
    fair_yes_dollars: Decimal
    confidence: float
    edge_bps: int
    recommended_action: TradeAction | None
    recommended_side: ContractSide | None
    target_yes_price_dollars: Decimal | None
    summary: str
    weather: WeatherSignalSnapshot | None = None
    resolution_state: WeatherResolutionState = WeatherResolutionState.UNRESOLVED
    strategy_mode: StrategyMode = StrategyMode.DIRECTIONAL_UNRESOLVED
    eligibility: TradeEligibilityVerdict | None = None
    stand_down_reason: StandDownReason | None = None
    heuristic_application: dict[str, Any] | None = None
    trade_regime: str = "standard"
    capital_bucket: str = "safe"
    forecast_delta_f: float | None = None
    confidence_band: str = "low"
    model_quality_status: str = "pass"
    model_quality_reasons: list[str] = field(default_factory=list)
    recommended_size_cap_fp: Decimal | None = None
    warn_only_blocked: bool = False
    size_factor: Decimal = field(default_factory=lambda: Decimal("1.00"))


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def is_market_stale(
    *,
    observed_at: datetime | None,
    stale_after_seconds: int,
    reference_time: datetime | None = None,
) -> bool:
    now = _as_utc(reference_time) or datetime.now(UTC)
    market_seen_at = _as_utc(observed_at)
    return market_seen_at is None or (now - market_seen_at).total_seconds() > stale_after_seconds


def market_spread_bps(market_snapshot: dict[str, Any]) -> int | None:
    market = market_snapshot.get("market", market_snapshot)
    raw_bid = market.get("yes_bid_dollars")
    raw_ask = market.get("yes_ask_dollars")
    yes_bid = quantize_price(raw_bid) if raw_bid is not None else None
    yes_ask = quantize_price(raw_ask) if raw_ask is not None else None
    # Kalshi returns 0 when no resting orders exist — treat as no quote.
    if yes_bid is not None and yes_bid <= Decimal("0"):
        yes_bid = None
    if yes_ask is not None and yes_ask <= Decimal("0"):
        yes_ask = None
    if yes_bid is None or yes_ask is None:
        return None
    return int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value())


def market_quotes(market_snapshot: dict[str, Any]) -> dict[str, Decimal | None]:
    market = market_snapshot.get("market", market_snapshot)

    def _price_or_none(raw: object) -> Decimal | None:
        if raw is None:
            return None
        price = quantize_price(raw)
        # Kalshi returns 0 when no resting orders exist on that side — treat as no quote.
        return price if price > Decimal("0") else None

    return {
        "yes_bid": _price_or_none(market.get("yes_bid_dollars")),
        "yes_ask": _price_or_none(market.get("yes_ask_dollars")),
        "no_ask": _price_or_none(market.get("no_ask_dollars")),
    }


def base_strategy_summary(summary: str) -> str:
    cleaned = summary.strip()
    for marker in (
        " Recommend ",
        f" {NO_TRADE_SUMMARY_SENTENCE}",
        " Market spread is too wide for the base strategy.",
        " Order book is effectively broken",
        " No actionable edge remains after applying the quality buffer to current quotes.",
        " Model-quality review:",
        " Stand down:",
    ):
        marker_index = cleaned.find(marker)
        if marker_index != -1:
            cleaned = cleaned[:marker_index].rstrip(" .")
    return cleaned.rstrip(".")


def non_trade_market_reason(
    market_snapshot: dict[str, Any],
    *,
    spread_limit_bps: int,
) -> tuple[StandDownReason, str]:
    quotes = market_quotes(market_snapshot)
    yes_bid = quotes["yes_bid"]
    yes_ask = quotes["yes_ask"]
    no_ask = quotes["no_ask"]
    spread_bps = market_spread_bps(market_snapshot)

    if yes_ask is None or no_ask is None:
        return (
            StandDownReason.BOOK_EFFECTIVELY_BROKEN,
            "Order book is effectively broken because one or more taker quotes are missing.",
        )

    if (
        (yes_ask >= Decimal("0.9900") and no_ask >= Decimal("0.9400"))
        or (no_ask >= Decimal("0.9900") and yes_ask >= Decimal("0.9400"))
        or (spread_bps is not None and spread_bps >= 9000)
        or (yes_bid is not None and yes_bid <= Decimal("0.0100") and yes_ask >= Decimal("0.9900") and no_ask >= Decimal("0.9400"))
    ):
        return (
            StandDownReason.BOOK_EFFECTIVELY_BROKEN,
            (
                f"Order book is effectively broken at current quotes "
                f"(yes bid {yes_bid or 'n/a'}, yes ask {yes_ask}, no ask {no_ask})."
            ),
        )

    if spread_bps is not None and spread_bps > spread_limit_bps:
        return (
            StandDownReason.SPREAD_TOO_WIDE,
            f"Market spread {spread_bps}bps is too wide for the base strategy.",
        )

    return (
        StandDownReason.NO_ACTIONABLE_EDGE,
        "No actionable edge remains after applying the quality buffer to current quotes.",
    )


def summarize_signal_action(
    base_summary: str,
    *,
    recommendation_action: TradeAction | None,
    recommendation_side: ContractSide | None,
    target_yes_price_dollars: Decimal | None,
    edge_bps: int,
    market_snapshot: dict[str, Any],
    spread_limit_bps: int,
) -> str:
    summary = base_strategy_summary(base_summary)
    if recommendation_action is not None and recommendation_side is not None and target_yes_price_dollars is not None:
        return (
            f"{summary}. Recommend {recommendation_action.value} {recommendation_side.value} "
            f"at yes price {target_yes_price_dollars} with edge {edge_bps} bps."
        )
    no_trade_reason, no_trade_text = non_trade_market_reason(
        market_snapshot,
        spread_limit_bps=spread_limit_bps,
    )
    if no_trade_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN:
        return f"{summary}. {no_trade_text}"
    if no_trade_reason == StandDownReason.SPREAD_TOO_WIDE:
        return f"{summary}. Market spread is too wide for the base strategy."
    return f"{summary}. {NO_TRADE_SUMMARY_SENTENCE}"


def remaining_payout_dollars(side: ContractSide, yes_price_dollars: Decimal) -> Decimal:
    if side == ContractSide.YES:
        return quantize_price(Decimal("1.0000") - yes_price_dollars)
    return quantize_price(yes_price_dollars)


def _confidence_size_factor(confidence: float) -> Decimal:
    """Three-tier position sizing: below 0.80 → 50%, 0.80–0.90 → 75%, 0.90+ → 100%."""
    if confidence >= 0.90:
        return Decimal("1.00")
    if confidence >= 0.80:
        return Decimal("0.75")
    return Decimal("0.50")


def suggested_trade_count_fp(
    *,
    settings: Settings,
    signal: StrategySignal,
    max_order_notional_dollars: float | None = None,
) -> Decimal | None:
    if signal.recommended_side is None or signal.target_yes_price_dollars is None:
        return None
    notional_cap = max_order_notional_dollars if max_order_notional_dollars is not None else settings.risk_max_order_notional_dollars
    if notional_cap is None:
        return None
    max_notional = Decimal(str(notional_cap)) * _confidence_size_factor(signal.confidence)
    unit_price = (
        signal.target_yes_price_dollars
        if signal.recommended_side == ContractSide.YES
        else Decimal("1.0000") - signal.target_yes_price_dollars
    )
    raw_count = (max_notional / max(unit_price, Decimal("0.01"))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    capped_count = min(raw_count, Decimal(str(settings.risk_max_order_count_fp)))
    return max(capped_count, Decimal("1.00"))


def signal_trade_regime(signal: StrategySignal) -> str:
    fair_yes = signal.fair_yes_dollars
    fair_no = Decimal("1.0000") - fair_yes
    if signal.recommended_side == ContractSide.YES and fair_yes <= Decimal("0.0800"):
        return "longshot_yes"
    if signal.recommended_side == ContractSide.NO and fair_no <= Decimal("0.0800"):
        return "longshot_no"
    if signal.forecast_delta_f is not None and abs(signal.forecast_delta_f) <= 2.0:
        return "near_threshold"
    return "standard"


def capital_bucket_for_trade_regime(trade_regime: str | None) -> str:
    if trade_regime in {"near_threshold", "longshot_yes", "longshot_no"}:
        return "risky"
    return "safe"


def _model_quality_review(
    *,
    settings: Settings,
    signal: StrategySignal,
    market_snapshot: dict[str, Any],
    current_ticket_count_fp: Decimal | None = None,
) -> tuple[str, list[str], Decimal | None, bool]:
    reasons: list[str] = []
    recommended_size_cap_fp: Decimal | None = None
    warn_only_blocked = False

    if signal.trade_regime == "near_threshold":
        recommended_size_cap_fp = ADVISORY_SIZE_CAP_FP
        if signal.confidence < settings.risk_min_confidence:
            reasons.append("Near-threshold setup carries low confidence and should be sized conservatively.")

    if signal.trade_regime in {"longshot_yes", "longshot_no"}:
        recommended_size_cap_fp = ADVISORY_SIZE_CAP_FP
        reasons.append("Longshot setup should be treated as small tail exposure.")
        if signal.recommended_side is not None and signal.target_yes_price_dollars is not None:
            remaining_payout = remaining_payout_dollars(signal.recommended_side, signal.target_yes_price_dollars)
            if remaining_payout < TAIL_PAYOUT_WARN_THRESHOLD:
                reasons.append(
                    f"Remaining payout {remaining_payout:.4f} is below the tail-trade comfort threshold of {TAIL_PAYOUT_WARN_THRESHOLD:.4f}."
                )

    if current_ticket_count_fp is None:
        current_ticket_count_fp = suggested_trade_count_fp(settings=settings, signal=signal)
    if (
        recommended_size_cap_fp is not None
        and current_ticket_count_fp is not None
        and current_ticket_count_fp > recommended_size_cap_fp
    ):
        reasons.append(
            f"Current default ticket size {current_ticket_count_fp:.2f} exceeds advisory cap {recommended_size_cap_fp:.2f}."
        )

    no_trade_reason, _ = non_trade_market_reason(
        market_snapshot,
        spread_limit_bps=settings.trigger_max_spread_bps,
    )
    if no_trade_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN:
        warn_only_blocked = True
        reasons.append("Strict quality review would block this setup because the order book is effectively broken.")

    status = "warn" if reasons else "pass"
    return status, reasons, recommended_size_cap_fp, warn_only_blocked


def _model_quality_summary_text(
    *,
    signal: StrategySignal,
    recommended_size_cap_fp: Decimal | None,
    reasons: list[str],
    warn_only_blocked: bool,
) -> str | None:
    details: list[str] = []
    if signal.trade_regime == "near_threshold":
        details.append("near-threshold setup")
    elif signal.trade_regime in {"longshot_yes", "longshot_no"}:
        details.append("longshot setup")
    if warn_only_blocked:
        details.append("strict mode would block the broken book")
    if recommended_size_cap_fp is not None:
        details.append(f"recommended cap is {recommended_size_cap_fp:.2f} contracts")
    if reasons:
        detail_reasons = [
            reason
            for reason in reasons
            if "Current default ticket size" in reason or "Remaining payout" in reason
        ]
        details.extend(detail_reasons)
    if not details:
        return None
    return "; ".join(details)


def annotate_signal_quality(
    *,
    settings: Settings,
    signal: StrategySignal,
    market_snapshot: dict[str, Any],
    max_order_notional_dollars: float | None = None,
) -> StrategySignal:
    signal.trade_regime = signal_trade_regime(signal)
    signal.capital_bucket = capital_bucket_for_trade_regime(signal.trade_regime)
    signal.confidence_band = confidence_band_for(signal.confidence)
    current_ticket_count_fp = suggested_trade_count_fp(
        settings=settings,
        signal=signal,
        max_order_notional_dollars=max_order_notional_dollars,
    )
    status, reasons, recommended_size_cap_fp, warn_only_blocked = _model_quality_review(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        current_ticket_count_fp=current_ticket_count_fp,
    )
    signal.model_quality_status = status
    signal.model_quality_reasons = reasons
    signal.recommended_size_cap_fp = recommended_size_cap_fp
    signal.warn_only_blocked = warn_only_blocked
    signal.summary = summarize_signal_action(
        base_strategy_summary(signal.summary),
        recommendation_action=signal.recommended_action,
        recommendation_side=signal.recommended_side,
        target_yes_price_dollars=signal.target_yes_price_dollars,
        edge_bps=signal.edge_bps,
        market_snapshot=market_snapshot,
        spread_limit_bps=settings.trigger_max_spread_bps,
    )
    quality_summary = _model_quality_summary_text(
        signal=signal,
        recommended_size_cap_fp=recommended_size_cap_fp,
        reasons=reasons,
        warn_only_blocked=warn_only_blocked,
    )
    if quality_summary:
        signal.summary = f"{signal.summary} Model-quality review: {quality_summary}."
    return signal


def _trade_recommendation(
    *,
    fair_yes_dollars: Decimal,
    market_snapshot: dict[str, Any],
    min_edge_bps: int,
) -> tuple[TradeAction | None, ContractSide | None, Decimal | None, int]:
    quotes = market_quotes(market_snapshot)
    ask_yes = quotes["yes_ask"]
    bid_yes = quotes["yes_bid"]
    ask_no = quotes["no_ask"]
    min_edge = Decimal(min_edge_bps) / Decimal("10000")

    recommendation_action: TradeAction | None = None
    recommendation_side: ContractSide | None = None
    target_yes: Decimal | None = None
    edge_bps = 0

    if ask_yes is not None:
        edge_yes = fair_yes_dollars - ask_yes
        if edge_yes >= min_edge:
            recommendation_action = TradeAction.BUY
            recommendation_side = ContractSide.YES
            target_yes = ask_yes
            edge_bps = int((edge_yes * Decimal("10000")).to_integral_value())

    if recommendation_action is None and ask_no is not None:
        fair_no = Decimal("1.0000") - fair_yes_dollars
        edge_no = fair_no - ask_no
        if edge_no >= min_edge:
            recommendation_action = TradeAction.BUY
            recommendation_side = ContractSide.NO
            target_yes = quantize_price(Decimal("1.0000") - ask_no)
            edge_bps = int((edge_no * Decimal("10000")).to_integral_value())

    if recommendation_action is None and bid_yes is not None and fair_yes_dollars >= bid_yes + min_edge:
        edge_bps = int(((fair_yes_dollars - bid_yes) * Decimal("10000")).to_integral_value())

    return recommendation_action, recommendation_side, target_yes, edge_bps


def apply_heuristic_application_to_signal(
    *,
    settings: Settings,
    signal: StrategySignal,
    market_snapshot: dict[str, Any],
    min_edge_bps: int,
    spread_limit_bps: int,
) -> StrategySignal:
    application = dict(signal.heuristic_application or {})
    adjusted_fair_raw = application.get("adjusted_fair_yes_dollars")
    adjusted_fair = quantize_price(adjusted_fair_raw) if adjusted_fair_raw not in (None, "") else signal.fair_yes_dollars
    recommendation_action, recommendation_side, target_yes, edge_bps = _trade_recommendation(
        fair_yes_dollars=adjusted_fair,
        market_snapshot=market_snapshot,
        min_edge_bps=min_edge_bps,
    )
    summary = base_strategy_summary(signal.summary)
    if adjusted_fair != signal.fair_yes_dollars:
        adjust_bps = int(application.get("fair_yes_adjust_bps") or 0)
        summary = f"{summary}. Historical heuristics adjusted fair yes by {adjust_bps:+d}bps"
    summary = summarize_signal_action(
        summary,
        recommendation_action=recommendation_action,
        recommendation_side=recommendation_side,
        target_yes_price_dollars=target_yes,
        edge_bps=edge_bps,
        market_snapshot=market_snapshot,
        spread_limit_bps=spread_limit_bps,
    )
    annotated = StrategySignal(
        fair_yes_dollars=adjusted_fair,
        confidence=signal.confidence,
        edge_bps=edge_bps,
        recommended_action=recommendation_action,
        recommended_side=recommendation_side,
        target_yes_price_dollars=target_yes,
        summary=summary,
        weather=signal.weather,
        resolution_state=signal.resolution_state,
        strategy_mode=signal.strategy_mode,
        heuristic_application=application,
        trade_regime=signal.trade_regime,
        capital_bucket=signal.capital_bucket,
        forecast_delta_f=signal.forecast_delta_f,
        confidence_band=signal.confidence_band,
    )
    return annotate_signal_quality(
        settings=settings,
        signal=annotated,
        market_snapshot=market_snapshot,
    )


def evaluate_trade_eligibility(
    *,
    settings: Settings,
    signal: StrategySignal,
    market_snapshot: dict[str, Any],
    market_observed_at: datetime | None,
    research_freshness: ResearchFreshness,
    thresholds: Any,
    decision_time: datetime | None = None,
    market_stale_after_seconds: int | None = None,
) -> TradeEligibilityVerdict:
    reasons: list[str] = []
    stand_down_reason: StandDownReason | None = None
    market_stale = is_market_stale(
        observed_at=market_observed_at,
        stale_after_seconds=market_stale_after_seconds or settings.risk_stale_market_seconds,
        reference_time=decision_time,
    )
    research_stale = bool(research_freshness.stale)
    spread_bps = market_spread_bps(market_snapshot)
    remaining_payout: Decimal | None = None
    quality_buffer_bps = getattr(thresholds, "strategy_quality_edge_buffer_bps", settings.strategy_quality_edge_buffer_bps)
    minimum_remaining_payout_bps = getattr(
        thresholds,
        "strategy_min_remaining_payout_bps",
        settings.strategy_min_remaining_payout_bps,
    )
    edge_after_quality_buffer_bps = signal.edge_bps - quality_buffer_bps
    no_trade_reason, no_trade_text = non_trade_market_reason(
        market_snapshot,
        spread_limit_bps=thresholds.trigger_max_spread_bps,
    )
    heuristic_application = dict(signal.heuristic_application or {})
    forced_strategy_mode = heuristic_application.get("recommended_strategy_mode")
    forced_stand_down_value = heuristic_application.get("force_stand_down_reason")

    strategy_mode = signal.strategy_mode
    if isinstance(forced_strategy_mode, str) and forced_strategy_mode:
        try:
            strategy_mode = StrategyMode(forced_strategy_mode)
        except ValueError:
            strategy_mode = signal.strategy_mode
    if signal.resolution_state != WeatherResolutionState.UNRESOLVED:
        strategy_mode = StrategyMode.RESOLVED_CLEANUP_CANDIDATE
    elif signal.recommended_action is None:
        strategy_mode = StrategyMode.LATE_DAY_AVOID

    if research_stale:
        reasons.append("Research context is stale at decision time.")
        stand_down_reason = StandDownReason.RESEARCH_STALE
    elif market_stale:
        reasons.append("Market quotes are stale at decision time.")
        stand_down_reason = StandDownReason.MARKET_STALE
    elif signal.resolution_state != WeatherResolutionState.UNRESOLVED:
        reasons.append("Contract is already resolved by observed weather state.")
        stand_down_reason = StandDownReason.RESOLVED_CONTRACT
    elif isinstance(forced_stand_down_value, str) and forced_stand_down_value:
        try:
            stand_down_reason = StandDownReason(forced_stand_down_value)
        except ValueError:
            stand_down_reason = None
        if stand_down_reason is not None:
            reasons.append(
                "Historical heuristic policy forced an early stand-down for this regime before order generation."
            )
    elif signal.trade_regime in {"longshot_yes", "longshot_no"}:
        reasons.append(f"Longshot bet blocked: trade regime is {signal.trade_regime}.")
        stand_down_reason = StandDownReason.LONGSHOT_BET
    elif signal.recommended_action is None or signal.recommended_side is None or signal.target_yes_price_dollars is None:
        reasons.append(no_trade_text)
        stand_down_reason = no_trade_reason
    else:
        remaining_payout = remaining_payout_dollars(signal.recommended_side, signal.target_yes_price_dollars)
        if remaining_payout <= (Decimal(minimum_remaining_payout_bps) / Decimal("10000")):
            reasons.append(
                f"Remaining payout {remaining_payout} is below configured minimum of "
                f"{Decimal(minimum_remaining_payout_bps) / Decimal('10000'):.4f}."
            )
            stand_down_reason = StandDownReason.INSUFFICIENT_REMAINING_PAYOUT
        elif no_trade_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN:
            reasons.append(no_trade_text)
            stand_down_reason = no_trade_reason
        elif spread_bps is not None and spread_bps > thresholds.trigger_max_spread_bps:
            reasons.append(
                f"Market spread {spread_bps}bps exceeds configured maximum of {thresholds.trigger_max_spread_bps}bps."
            )
            stand_down_reason = StandDownReason.SPREAD_TOO_WIDE
        elif edge_after_quality_buffer_bps < thresholds.risk_min_edge_bps:
            reasons.append(
                f"No actionable edge remains after the quality buffer: {edge_after_quality_buffer_bps}bps versus "
                f"required {thresholds.risk_min_edge_bps}bps."
            )
            stand_down_reason = StandDownReason.NO_ACTIONABLE_EDGE

    if stand_down_reason is not None and strategy_mode == StrategyMode.DIRECTIONAL_UNRESOLVED:
        strategy_mode = StrategyMode.LATE_DAY_AVOID

    return TradeEligibilityVerdict(
        eligible=stand_down_reason is None,
        strategy_mode=strategy_mode,
        resolution_state=signal.resolution_state,
        stand_down_reason=stand_down_reason,
        capital_bucket=signal.capital_bucket,
        reasons=reasons or ["Trade passed marketability checks."],
        market_stale=market_stale,
        research_stale=research_stale,
        remaining_payout_dollars=remaining_payout,
        market_spread_bps=spread_bps,
        edge_after_quality_buffer_bps=edge_after_quality_buffer_bps,
        blocked_upstream=stand_down_reason is not None,
        model_quality_status=signal.model_quality_status,
        model_quality_reasons=signal.model_quality_reasons,
        recommended_size_cap_fp=signal.recommended_size_cap_fp,
        warn_only_blocked=signal.warn_only_blocked,
    )


class WeatherSignalEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _market_price(self, market_snapshot: dict[str, Any], key: str) -> Decimal | None:
        market = market_snapshot.get("market", market_snapshot)
        raw = market.get(key)
        return quantize_price(raw) if raw is not None else None

    def evaluate(
        self,
        mapping: WeatherMarketMapping,
        market_snapshot: dict[str, Any],
        weather_bundle: dict[str, Any],
        *,
        min_edge_bps: int | None = None,
        sigma_params: dict | None = None,
        lead_factors: dict | None = None,
    ) -> StrategySignal:
        sigma_ctx: SigmaContext | None = None
        if sigma_params is not None or lead_factors is not None:
            from kalshi_bot.weather.sigma_calibration import season_for_month
            from datetime import UTC, datetime
            month = datetime.now(UTC).month
            sigma_ctx = SigmaContext(
                station=getattr(mapping, "station_id", None),
                season_bucket=season_for_month(month),
                sigma_params=sigma_params,
                lead_factors=lead_factors,
                lead_correction_enabled=self.settings.sigma_lead_correction_enabled,
            )
        weather = score_weather_market(
            mapping,
            weather_bundle.get("forecast", {}),
            weather_bundle.get("observation", {}),
            forecast_grid_payload=weather_bundle.get("forecast_grid") or None,
            sigma_ctx=sigma_ctx,
        )
        effective_min_edge_bps = min_edge_bps if min_edge_bps is not None else self.settings.risk_min_edge_bps
        recommendation_action, recommendation_side, target_yes, edge_bps = _trade_recommendation(
            fair_yes_dollars=weather.fair_yes_dollars,
            market_snapshot=market_snapshot,
            min_edge_bps=effective_min_edge_bps,
        )

        summary = summarize_signal_action(
            weather.summary,
            recommendation_action=recommendation_action,
            recommendation_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            edge_bps=edge_bps,
            market_snapshot=market_snapshot,
            spread_limit_bps=self.settings.trigger_max_spread_bps,
        )

        signal = StrategySignal(
            fair_yes_dollars=weather.fair_yes_dollars,
            confidence=weather.confidence,
            edge_bps=edge_bps,
            recommended_action=recommendation_action,
            recommended_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            weather=weather,
            resolution_state=weather.resolution_state,
            strategy_mode=(
                StrategyMode.RESOLVED_CLEANUP_CANDIDATE
                if weather.resolution_state != WeatherResolutionState.UNRESOLVED
                else StrategyMode.DIRECTIONAL_UNRESOLVED
            ),
            trade_regime=weather.trade_regime,
            capital_bucket=capital_bucket_for_trade_regime(weather.trade_regime),
            forecast_delta_f=weather.forecast_delta_f,
            confidence_band=weather.confidence_band,
        )
        return annotate_signal_quality(
            settings=self.settings,
            signal=signal,
            market_snapshot=market_snapshot,
        )


def estimate_notional_dollars(side: ContractSide, yes_price_dollars: Decimal, count_fp: Decimal) -> Decimal:
    unit_price = yes_price_dollars if side == ContractSide.YES else Decimal("1.0000") - yes_price_dollars
    return unit_price * count_fp
