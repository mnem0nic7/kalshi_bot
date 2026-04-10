from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.fixed_point import as_decimal, quantize_price
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import WeatherSignalSnapshot, score_weather_market


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


class WeatherSignalEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _market_price(self, market_snapshot: dict[str, Any], key: str) -> Decimal | None:
        market = market_snapshot.get("market", market_snapshot)
        raw = market.get(key)
        return quantize_price(raw) if raw is not None else None

    def evaluate(self, mapping: WeatherMarketMapping, market_snapshot: dict[str, Any], weather_bundle: dict[str, Any]) -> StrategySignal:
        weather = score_weather_market(mapping, weather_bundle.get("forecast", {}), weather_bundle.get("observation", {}))
        ask_yes = self._market_price(market_snapshot, "yes_ask_dollars")
        bid_yes = self._market_price(market_snapshot, "yes_bid_dollars")
        ask_no = self._market_price(market_snapshot, "no_ask_dollars")
        min_edge = Decimal(self.settings.risk_min_edge_bps) / Decimal("10000")

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
            summary = f"{summary} No taker trade clears the configured edge threshold."

        return StrategySignal(
            fair_yes_dollars=weather.fair_yes_dollars,
            confidence=weather.confidence,
            edge_bps=edge_bps,
            recommended_action=recommendation_action,
            recommended_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            weather=weather,
        )


def estimate_notional_dollars(side: ContractSide, yes_price_dollars: Decimal, count_fp: Decimal) -> Decimal:
    unit_price = yes_price_dollars if side == ContractSide.YES else Decimal("1.0000") - yes_price_dollars
    return unit_price * count_fp
