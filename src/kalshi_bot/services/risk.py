from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RiskStatus, WeatherResolutionState
from kalshi_bot.core.schemas import RiskVerdictPayload, TradeTicket
from kalshi_bot.db.models import DeploymentControl, Room
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars


@dataclass(slots=True)
class RiskContext:
    market_observed_at: datetime | None
    research_observed_at: datetime | None
    current_position_notional_dollars: Decimal = Decimal("0")


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
        now = datetime.now(UTC)
        market_observed_at = self._as_utc(context.market_observed_at)
        research_observed_at = self._as_utc(context.research_observed_at)
        active_thresholds = thresholds or RuntimeThresholds(
            risk_min_edge_bps=self.settings.risk_min_edge_bps,
            risk_max_order_notional_dollars=self.settings.risk_max_order_notional_dollars,
            risk_max_position_notional_dollars=self.settings.risk_max_position_notional_dollars,
            trigger_max_spread_bps=self.settings.trigger_max_spread_bps,
            trigger_cooldown_seconds=self.settings.trigger_cooldown_seconds,
        )

        if control.kill_switch_enabled:
            reasons.append("Global kill switch is enabled.")
        if signal.recommended_action is None or signal.recommended_side is None:
            reasons.append("Signal engine did not recommend a live trade.")
        if signal.resolution_state != WeatherResolutionState.UNRESOLVED:
            reasons.append("Contract is already resolved under the base weather strategy.")
        if signal.eligibility is not None and not signal.eligibility.eligible:
            reasons.extend(
                [reason for reason in signal.eligibility.reasons if reason not in reasons]
            )
        if signal.edge_bps < active_thresholds.risk_min_edge_bps:
            reasons.append(f"Edge {signal.edge_bps}bps is below configured minimum of {active_thresholds.risk_min_edge_bps}bps.")

        if market_observed_at is None or (now - market_observed_at).total_seconds() > self.settings.risk_stale_market_seconds:
            reasons.append("Kalshi market data is stale.")
        if research_observed_at is None or (now - research_observed_at).total_seconds() > self.settings.research_stale_seconds:
            reasons.append("Research data is stale.")

        if float(ticket.count_fp) > self.settings.risk_max_order_count_fp:
            reasons.append("Ticket size exceeds max order count.")

        order_notional = estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp)
        if float(order_notional) > active_thresholds.risk_max_order_notional_dollars:
            reasons.append("Ticket notional exceeds max order notional.")
        if float(context.current_position_notional_dollars + order_notional) > active_thresholds.risk_max_position_notional_dollars:
            reasons.append("Projected position exceeds max position notional.")

        if room.shadow_mode:
            reasons.append("Room is in shadow mode; execution will be simulated.")

        status = RiskStatus.APPROVED if not [reason for reason in reasons if "shadow mode" not in reason] else RiskStatus.BLOCKED
        approved_notional = order_notional if status == RiskStatus.APPROVED else None
        approved_count = ticket.count_fp if status == RiskStatus.APPROVED else None
        return RiskVerdictPayload(
            status=status,
            reasons=reasons or ["All deterministic checks passed."],
            approved_notional_dollars=approved_notional,
            approved_count_fp=approved_count,
        )
