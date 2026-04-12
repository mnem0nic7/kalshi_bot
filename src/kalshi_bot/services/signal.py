from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.fixed_point import as_decimal, quantize_price
from kalshi_bot.core.schemas import ResearchFreshness, TradeEligibilityVerdict
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import WeatherSignalSnapshot, score_weather_market

NO_TRADE_SUMMARY_SENTENCE = "No taker trade clears the configured edge threshold."


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


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def market_spread_bps(market_snapshot: dict[str, Any]) -> int | None:
    market = market_snapshot.get("market", market_snapshot)
    yes_bid = quantize_price(market.get("yes_bid_dollars")) if market.get("yes_bid_dollars") is not None else None
    yes_ask = quantize_price(market.get("yes_ask_dollars")) if market.get("yes_ask_dollars") is not None else None
    if yes_bid is None or yes_ask is None:
        return None
    return int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value())


def market_quotes(market_snapshot: dict[str, Any]) -> dict[str, Decimal | None]:
    market = market_snapshot.get("market", market_snapshot)
    return {
        "yes_bid": quantize_price(market.get("yes_bid_dollars")) if market.get("yes_bid_dollars") is not None else None,
        "yes_ask": quantize_price(market.get("yes_ask_dollars")) if market.get("yes_ask_dollars") is not None else None,
        "no_ask": quantize_price(market.get("no_ask_dollars")) if market.get("no_ask_dollars") is not None else None,
    }


def base_strategy_summary(summary: str) -> str:
    cleaned = summary.strip()
    for marker in (
        " Recommend ",
        f" {NO_TRADE_SUMMARY_SENTENCE}",
        " Market spread is too wide for the base strategy.",
        " Order book is effectively broken",
        " No actionable edge remains after applying the quality buffer to current quotes.",
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


def remaining_payout_dollars(side: ContractSide, yes_price_dollars: Decimal) -> Decimal:
    if side == ContractSide.YES:
        return quantize_price(Decimal("1.0000") - yes_price_dollars)
    return quantize_price(yes_price_dollars)


def evaluate_trade_eligibility(
    *,
    settings: Settings,
    signal: StrategySignal,
    market_snapshot: dict[str, Any],
    market_observed_at: datetime | None,
    research_freshness: ResearchFreshness,
    thresholds: Any,
) -> TradeEligibilityVerdict:
    reasons: list[str] = []
    stand_down_reason: StandDownReason | None = None
    now = datetime.now(UTC)
    market_seen_at = _as_utc(market_observed_at)
    market_stale = market_seen_at is None or (now - market_seen_at).total_seconds() > settings.risk_stale_market_seconds
    research_stale = bool(research_freshness.stale)
    spread_bps = market_spread_bps(market_snapshot)
    remaining_payout: Decimal | None = None
    edge_after_quality_buffer_bps = signal.edge_bps - settings.strategy_quality_edge_buffer_bps
    no_trade_reason, no_trade_text = non_trade_market_reason(
        market_snapshot,
        spread_limit_bps=thresholds.trigger_max_spread_bps,
    )

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
    elif signal.recommended_action is None or signal.recommended_side is None or signal.target_yes_price_dollars is None:
        reasons.append(no_trade_text)
        stand_down_reason = no_trade_reason
    else:
        remaining_payout = remaining_payout_dollars(signal.recommended_side, signal.target_yes_price_dollars)
        if remaining_payout <= (Decimal(settings.strategy_min_remaining_payout_bps) / Decimal("10000")):
            reasons.append(
                f"Remaining payout {remaining_payout} is below configured minimum of "
                f"{Decimal(settings.strategy_min_remaining_payout_bps) / Decimal('10000'):.4f}."
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
        reasons=reasons or ["Trade passed marketability checks."],
        market_stale=market_stale,
        research_stale=research_stale,
        remaining_payout_dollars=remaining_payout,
        market_spread_bps=spread_bps,
        edge_after_quality_buffer_bps=edge_after_quality_buffer_bps,
        blocked_upstream=stand_down_reason is not None,
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
    ) -> StrategySignal:
        weather = score_weather_market(mapping, weather_bundle.get("forecast", {}), weather_bundle.get("observation", {}))
        ask_yes = self._market_price(market_snapshot, "yes_ask_dollars")
        bid_yes = self._market_price(market_snapshot, "yes_bid_dollars")
        ask_no = self._market_price(market_snapshot, "no_ask_dollars")
        effective_min_edge_bps = min_edge_bps if min_edge_bps is not None else self.settings.risk_min_edge_bps
        min_edge = Decimal(effective_min_edge_bps) / Decimal("10000")

        recommendation_action = None
        recommendation_side = None
        target_yes = None
        edge_bps = 0

        if ask_yes is not None:
            edge_yes = weather.fair_yes_dollars - ask_yes
            if edge_yes >= min_edge:
                recommendation_action = TradeAction.BUY
                recommendation_side = ContractSide.YES
                target_yes = ask_yes
                edge_bps = int((edge_yes * Decimal("10000")).to_integral_value())

        if recommendation_action is None and ask_no is not None:
            fair_no = Decimal("1.0000") - weather.fair_yes_dollars
            edge_no = fair_no - ask_no
            if edge_no >= min_edge:
                recommendation_action = TradeAction.BUY
                recommendation_side = ContractSide.NO
                target_yes = quantize_price(Decimal("1.0000") - ask_no)
                edge_bps = int((edge_no * Decimal("10000")).to_integral_value())

        if recommendation_action is None and bid_yes is not None and weather.fair_yes_dollars >= bid_yes + min_edge:
            edge_bps = int(((weather.fair_yes_dollars - bid_yes) * Decimal("10000")).to_integral_value())

        summary = weather.summary
        if recommendation_action is not None and target_yes is not None:
            summary = (
                f"{summary} Recommend {recommendation_action.value} {recommendation_side.value} "
                f"at yes price {target_yes} with edge {edge_bps} bps."
            )
        else:
            no_trade_reason, no_trade_text = non_trade_market_reason(
                market_snapshot,
                spread_limit_bps=self.settings.trigger_max_spread_bps,
            )
            if no_trade_reason == StandDownReason.BOOK_EFFECTIVELY_BROKEN:
                summary = f"{summary} {no_trade_text}"
            elif no_trade_reason == StandDownReason.SPREAD_TOO_WIDE:
                summary = f"{summary} Market spread is too wide for the base strategy."
            else:
                summary = f"{summary} {NO_TRADE_SUMMARY_SENTENCE}"

        return StrategySignal(
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
        )


def estimate_notional_dollars(side: ContractSide, yes_price_dollars: Decimal, count_fp: Decimal) -> Decimal:
    unit_price = yes_price_dollars if side == ContractSide.YES else Decimal("1.0000") - yes_price_dollars
    return unit_price * count_fp
