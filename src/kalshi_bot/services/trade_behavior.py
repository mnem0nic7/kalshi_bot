from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason
from kalshi_bot.core.schemas import TradeEligibilityVerdict
from kalshi_bot.db.models import FillRecord
from kalshi_bot.services.agent_packs import RuntimeThresholds


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def series_from_ticker(market_ticker: str | None) -> str:
    ticker = str(market_ticker or "").strip()
    if "-" in ticker:
        return ticker.split("-", 1)[0]
    return ticker


def station_from_series(series_ticker: str | None) -> str | None:
    series = str(series_ticker or "").strip()
    if not series:
        return None
    for prefix in ("KXHIGH", "KXHIGHT"):
        if series.startswith(prefix):
            value = series[len(prefix):]
            return value or None
    return series


def side_price_from_yes_price(side: str | None, yes_price: Any) -> Decimal | None:
    if side not in {"yes", "no"} or yes_price in (None, ""):
        return None
    price = Decimal(str(yes_price))
    return price if side == "yes" else Decimal("1") - price


def price_band(price: Decimal | None) -> str:
    if price is None:
        return "unknown"
    cents = int((price * Decimal("100")).to_integral_value(rounding=ROUND_FLOOR))
    lower = max(0, min(99, (cents // 10) * 10))
    upper = min(100, lower + 9)
    return f"{lower:02d}-{upper:02d}c"


def bucket_key(
    *,
    market_ticker: str | None,
    side: str | None,
    strategy_code: str | None,
    yes_price_dollars: Any = None,
    include_price_band: bool = False,
) -> str:
    series = series_from_ticker(market_ticker)
    station = station_from_series(series) or "unknown"
    strategy = strategy_code or "<unknown>"
    side_value = side if side in {"yes", "no"} else "unknown"
    parts = [series or "unknown", station, side_value, strategy]
    if include_price_band:
        parts.append(price_band(side_price_from_yes_price(side, yes_price_dollars)))
    return "|".join(parts)


def _fee_dollars(fill: FillRecord) -> Decimal:
    raw = fill.raw if isinstance(fill.raw, dict) else {}
    for key in ("fee_cost", "fee_dollars", "fee"):
        value = raw.get(key)
        if value not in (None, ""):
            try:
                return Decimal(str(value))
            except Exception:
                return Decimal("0")
    return Decimal("0")


def _buy_settlement_pnl(fill: FillRecord) -> Decimal | None:
    price = side_price_from_yes_price(fill.side, fill.yes_price_dollars)
    if price is None:
        return None
    count = Decimal(fill.count_fp)
    if fill.settlement_result == "win":
        return (Decimal("1") - price) * count
    if fill.settlement_result == "loss":
        return -price * count
    return None


@dataclass(slots=True)
class EmpiricalGateDecision:
    status: str
    reason: str
    bucket_key: str
    actual_sample_count: int
    actual_net_pnl: Decimal | None
    actual_win_rate: float | None
    counterfactual_sample_count: int = 0
    blocks_live_entries: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "bucket_key": self.bucket_key,
            "actual_sample_count": self.actual_sample_count,
            "actual_net_pnl": (
                str(self.actual_net_pnl.quantize(Decimal("0.0001")))
                if self.actual_net_pnl is not None
                else None
            ),
            "actual_win_rate": self.actual_win_rate,
            "counterfactual_sample_count": self.counterfactual_sample_count,
            "blocks_live_entries": self.blocks_live_entries,
        }


def production_entry_freeze_enabled(settings: Settings, kalshi_env: str | None) -> bool:
    return (
        bool(settings.trade_behavior_production_entry_freeze_enabled)
        and str(kalshi_env or "").lower() == "production"
    )


def entry_pause_reason(
    *,
    settings: Settings,
    control: Any,
    kalshi_env: str | None,
) -> str | None:
    notes = dict(getattr(control, "notes", None) or {})
    source_health = dict(notes.get("source_health") or {})
    if source_health.get("pause_new_entries"):
        reason = source_health.get("pause_reason") or source_health.get("reason") or "source health degraded"
        label = source_health.get("aggregate_label")
        if label:
            return f"Source health pause is active ({label}): {reason}."
        return f"Source health pause is active: {reason}."

    entry_pause = dict(notes.get("entry_pause") or {})
    if entry_pause.get("pause_new_entries"):
        reason = entry_pause.get("pause_reason") or entry_pause.get("reason") or settings.trade_behavior_entry_freeze_reason
        return f"Entry pause is active: {reason}."

    if production_entry_freeze_enabled(settings, kalshi_env):
        return f"Entry pause is active: {settings.trade_behavior_entry_freeze_reason}."
    return None


def thresholds_with_production_freeze_floor(
    *,
    settings: Settings,
    kalshi_env: str | None,
    thresholds: RuntimeThresholds,
) -> RuntimeThresholds:
    if not production_entry_freeze_enabled(settings, kalshi_env):
        return thresholds
    floor = int(settings.trade_behavior_freeze_min_edge_bps)
    if int(thresholds.risk_min_edge_bps) >= floor:
        return thresholds
    return RuntimeThresholds(
        risk_min_edge_bps=floor,
        risk_max_order_notional_dollars=thresholds.risk_max_order_notional_dollars,
        risk_max_position_notional_dollars=thresholds.risk_max_position_notional_dollars,
        risk_safe_capital_reserve_ratio=thresholds.risk_safe_capital_reserve_ratio,
        risk_risky_capital_max_ratio=thresholds.risk_risky_capital_max_ratio,
        trigger_max_spread_bps=thresholds.trigger_max_spread_bps,
        trigger_cooldown_seconds=thresholds.trigger_cooldown_seconds,
        strategy_quality_edge_buffer_bps=thresholds.strategy_quality_edge_buffer_bps,
        strategy_min_remaining_payout_bps=thresholds.strategy_min_remaining_payout_bps,
    )


async def evaluate_empirical_gate(
    *,
    session: AsyncSession,
    settings: Settings,
    kalshi_env: str,
    market_ticker: str,
    side: str | None,
    action: str | None,
    strategy_code: str | None,
    shadow_mode: bool,
    now: datetime | None = None,
) -> EmpiricalGateDecision:
    gate_bucket_key = bucket_key(
        market_ticker=market_ticker,
        side=side,
        strategy_code=strategy_code,
    )
    if not settings.trade_behavior_empirical_gate_enabled:
        return EmpiricalGateDecision(
            status="disabled",
            reason="empirical_gate_disabled",
            bucket_key=gate_bucket_key,
            actual_sample_count=0,
            actual_net_pnl=None,
            actual_win_rate=None,
        )
    if action != "buy" or side not in {"yes", "no"}:
        return EmpiricalGateDecision(
            status="not_applicable",
            reason="not_new_entry",
            bucket_key=gate_bucket_key,
            actual_sample_count=0,
            actual_net_pnl=None,
            actual_win_rate=None,
        )

    now_utc = _as_utc(now) or datetime.now(UTC)
    cutoff = now_utc - timedelta(days=max(1, int(settings.trade_behavior_empirical_gate_lookback_days)))
    series = series_from_ticker(market_ticker)
    stmt = (
        select(FillRecord)
        .where(
            FillRecord.kalshi_env == kalshi_env,
            FillRecord.action == "buy",
            FillRecord.side == side,
            FillRecord.settlement_result.in_(["win", "loss"]),
            FillRecord.created_at >= cutoff,
        )
        .order_by(FillRecord.created_at.asc(), FillRecord.id.asc())
    )
    if series:
        stmt = stmt.where(FillRecord.market_ticker.like(f"{series}-%"))
    if strategy_code:
        stmt = stmt.where(FillRecord.strategy_code == strategy_code)
    fills = list((await session.execute(stmt)).scalars())

    sample_count = len(fills)
    wins = sum(1 for fill in fills if fill.settlement_result == "win")
    gross = Decimal("0")
    fees = Decimal("0")
    for fill in fills:
        pnl = _buy_settlement_pnl(fill)
        if pnl is not None:
            gross += pnl
        fees += _fee_dollars(fill)
    net = gross - fees if fills else None
    win_rate = round(wins / sample_count, 6) if sample_count else None

    min_samples = int(settings.trade_behavior_empirical_gate_min_settled_fills)
    min_net = Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars))
    reason = "empirical_gate_passed"
    passes = True
    if sample_count < min_samples:
        passes = False
        reason = "empirical_gate_under_sampled"
    if net is not None and net <= min_net:
        passes = False
        reason = "empirical_gate_negative_actual_net_pnl"

    production_live_entry = str(kalshi_env).lower() == "production" and not shadow_mode
    if production_live_entry and production_entry_freeze_enabled(settings, kalshi_env):
        return EmpiricalGateDecision(
            status="blocked",
            reason=settings.trade_behavior_entry_freeze_reason,
            bucket_key=gate_bucket_key,
            actual_sample_count=sample_count,
            actual_net_pnl=net,
            actual_win_rate=win_rate,
            blocks_live_entries=True,
        )
    if passes:
        return EmpiricalGateDecision(
            status="allowed",
            reason=reason,
            bucket_key=gate_bucket_key,
            actual_sample_count=sample_count,
            actual_net_pnl=net,
            actual_win_rate=win_rate,
        )
    return EmpiricalGateDecision(
        status="blocked" if production_live_entry else "shadow_only",
        reason=reason,
        bucket_key=gate_bucket_key,
        actual_sample_count=sample_count,
        actual_net_pnl=net,
        actual_win_rate=win_rate,
        blocks_live_entries=production_live_entry,
    )


def apply_empirical_gate_to_eligibility(
    eligibility: TradeEligibilityVerdict,
    decision: EmpiricalGateDecision,
) -> TradeEligibilityVerdict:
    payload = decision.to_payload()
    reasons = list(eligibility.reasons)
    candidate_trace = dict(eligibility.candidate_trace or {})
    candidate_trace["empirical_gate"] = payload
    stand_down_reason = eligibility.stand_down_reason
    eligible = eligibility.eligible
    evaluation_outcome = eligibility.evaluation_outcome

    if decision.blocks_live_entries:
        eligible = False
        evaluation_outcome = "pre_risk_filtered"
        stand_down_reason = (
            StandDownReason.ENTRY_FREEZE
            if decision.reason == "trade_behavior_retraining_freeze"
            else StandDownReason.EMPIRICAL_GATE_BLOCK
        )
        message = f"Empirical trade behavior gate blocked live entry: {decision.reason}."
        if message not in reasons:
            reasons.append(message)

    return eligibility.model_copy(
        update={
            "eligible": eligible,
            "stand_down_reason": stand_down_reason,
            "evaluation_outcome": evaluation_outcome,
            "candidate_trace": candidate_trace,
            "reasons": reasons,
            "blocked_upstream": eligibility.blocked_upstream or decision.blocks_live_entries,
            "empirical_gate": payload,
        }
    )
