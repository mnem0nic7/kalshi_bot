from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason, StrategyCode
from kalshi_bot.core.schemas import TradeEligibilityVerdict
from kalshi_bot.db.models import FillRecord, OrderRecord
from kalshi_bot.services.agent_packs import RuntimeThresholds


EMPIRICAL_BUCKET_VERSION = "empirical_bucket_v2"


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


def forecast_delta_band(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    try:
        delta = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if delta <= -5:
        return "<=-5f"
    if delta < -2:
        return "-5--2f"
    if delta < 0:
        return "-2-0f"
    if delta < 2:
        return "0-2f"
    if delta < 5:
        return "2-5f"
    return ">=5f"


def confidence_bucket(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized or "unknown"
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if confidence < 0.5:
        return "low"
    if confidence < 0.75:
        return "medium"
    return "high"


def _confidence_bucket_score(value: Any) -> float | None:
    bucket = confidence_bucket(value)
    if bucket == "high":
        return 0.85
    if bucket == "medium":
        return 0.65
    if bucket == "low":
        return 0.40
    return None


def _price_from_band(value: str | None) -> Decimal | None:
    text = str(value or "").strip().lower()
    if not text or text == "unknown" or not text.endswith("c") or "-" not in text:
        return None
    lower, _, upper = text[:-1].partition("-")
    try:
        midpoint_cents = (Decimal(lower) + Decimal(upper)) / Decimal("2")
    except Exception:
        return None
    return midpoint_cents / Decimal("100")


def trade_quality_bucket(
    *,
    entry_price_dollars: Any = None,
    entry_price_band: str | None = None,
    confidence: Any = None,
    confidence_band: Any = None,
) -> str:
    """Coarse empirical quality bucket combining price support and confidence."""
    price: Decimal | None
    try:
        price = Decimal(str(entry_price_dollars)) if entry_price_dollars not in (None, "") else None
    except Exception:
        price = None
    if price is None:
        price = _price_from_band(entry_price_band)

    confidence_value: float | None
    try:
        confidence_value = float(confidence) if confidence not in (None, "") else None
    except (TypeError, ValueError):
        confidence_value = None
    if confidence_value is None:
        confidence_value = _confidence_bucket_score(confidence_band)

    if price is None or confidence_value is None:
        return "low"
    if price < Decimal("0.10") or confidence_value < 0.60:
        return "low"
    if price >= Decimal("0.25") and confidence_value >= 0.75:
        return "high"
    return "medium"


def spread_band(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    try:
        spread = int(float(value))
    except (TypeError, ValueError):
        return "unknown"
    if spread < 100:
        return "000-099bps"
    if spread < 250:
        return "100-249bps"
    if spread < 500:
        return "250-499bps"
    return "500bps+"


def coarse_empirical_bucket_key(
    *,
    market_ticker: str | None,
    side: str | None,
    strategy_code: str | None,
    yes_price_dollars: Any = None,
    forecast_delta_f: Any = None,
    confidence: Any = None,
    confidence_band: Any = None,
) -> str:
    series = series_from_ticker(market_ticker)
    station = station_from_series(series) or "unknown"
    strategy = strategy_code or "<unknown>"
    side_value = side if side in {"yes", "no"} else "unknown"
    entry_price = side_price_from_yes_price(side, yes_price_dollars)
    quality = trade_quality_bucket(
        entry_price_dollars=entry_price,
        confidence=confidence,
        confidence_band=confidence_band,
    )
    return "|".join([
        series or "unknown",
        station,
        side_value,
        strategy,
        f"delta:{forecast_delta_band(forecast_delta_f)}",
        f"quality:{quality}",
    ])


def coarse_empirical_bucket_key_from_legacy(value: str | None) -> str | None:
    if value in (None, ""):
        return None
    parts = str(value).split("|")
    if len(parts) >= 6 and parts[4].startswith("delta:") and parts[5].startswith(("quality:", "trade_quality:")):
        return str(value)
    dimensions = bucket_dimensions_from_key(value)
    delta_band = dimensions["forecast_delta_band"]
    quality = trade_quality_bucket(
        entry_price_band=dimensions["entry_price_band"],
        confidence_band=dimensions["confidence_band"],
    )
    return "|".join([
        dimensions["series_ticker"],
        dimensions["station"],
        dimensions["side"],
        dimensions["strategy_code"],
        f"delta:{delta_band}",
        f"quality:{quality}",
    ])


def bucket_key(
    *,
    market_ticker: str | None,
    side: str | None,
    strategy_code: str | None,
    yes_price_dollars: Any = None,
    include_price_band: bool = False,
    forecast_delta_f: Any = None,
    confidence_band: Any = None,
    spread_bps: Any = None,
    include_context_bands: bool = False,
) -> str:
    series = series_from_ticker(market_ticker)
    station = station_from_series(series) or "unknown"
    strategy = strategy_code or "<unknown>"
    side_value = side if side in {"yes", "no"} else "unknown"
    parts = [series or "unknown", station, side_value, strategy]
    if include_price_band:
        parts.append(price_band(side_price_from_yes_price(side, yes_price_dollars)))
    if include_context_bands:
        parts.extend([
            f"delta:{forecast_delta_band(forecast_delta_f)}",
            f"conf:{confidence_bucket(confidence_band)}",
            f"spread:{spread_band(spread_bps)}",
        ])
    return "|".join(parts)


def bucket_dimensions_from_key(value: str | None) -> dict[str, str]:
    parts = str(value or "").split("|")
    series = parts[0] if len(parts) > 0 and parts[0] else "unknown"
    station = parts[1] if len(parts) > 1 and parts[1] else station_from_series(series) or "unknown"
    side = parts[2] if len(parts) > 2 and parts[2] else "unknown"
    strategy = parts[3] if len(parts) > 3 and parts[3] else "<unknown>"
    entry_price_band = "unknown"
    forecast = "unknown"
    confidence = "unknown"
    spread = "unknown"
    for part in parts[4:]:
        if part.startswith("delta:"):
            forecast = part.split(":", 1)[1] or "unknown"
        elif part.startswith("conf:"):
            confidence = part.split(":", 1)[1] or "unknown"
        elif part.startswith("spread:"):
            spread = part.split(":", 1)[1] or "unknown"
        elif entry_price_band == "unknown":
            entry_price_band = part or "unknown"
    return {
        "series_ticker": series,
        "station": station,
        "side": side if side in {"yes", "no"} else "unknown",
        "strategy_code": strategy,
        "entry_price_band": entry_price_band,
        "forecast_delta_band": forecast,
        "confidence_band": confidence,
        "spread_band": spread,
    }


def trade_behavior_context_payload(
    *,
    market_ticker: str | None,
    side: str | None,
    strategy_code: str | None,
    yes_price_dollars: Any = None,
    forecast_delta_f: Any = None,
    confidence_band: Any = None,
    spread_bps: Any = None,
) -> dict[str, Any]:
    try:
        forecast_delta_value = float(forecast_delta_f) if forecast_delta_f not in (None, "") else None
    except (TypeError, ValueError):
        forecast_delta_value = None
    try:
        spread_value = int(float(spread_bps)) if spread_bps not in (None, "") else None
    except (TypeError, ValueError):
        spread_value = None
    entry_price_band = price_band(side_price_from_yes_price(side, yes_price_dollars))
    delta_band = forecast_delta_band(forecast_delta_f)
    confidence = confidence_bucket(confidence_band)
    spread = spread_band(spread_bps)
    coarse_key = coarse_empirical_bucket_key(
        market_ticker=market_ticker,
        side=side,
        strategy_code=strategy_code,
        yes_price_dollars=yes_price_dollars,
        forecast_delta_f=forecast_delta_f,
        confidence_band=confidence_band,
    )
    return {
        "bucket_key": coarse_key,
        "coarse_bucket_key": coarse_key,
        "legacy_bucket_key": bucket_key(
            market_ticker=market_ticker,
            side=side,
            strategy_code=strategy_code,
            yes_price_dollars=yes_price_dollars,
            include_price_band=True,
            forecast_delta_f=forecast_delta_f,
            confidence_band=confidence_band,
            spread_bps=spread_bps,
            include_context_bands=True,
        ),
        "bucket_key_version": EMPIRICAL_BUCKET_VERSION,
        "series_ticker": series_from_ticker(market_ticker),
        "station": station_from_series(series_from_ticker(market_ticker)) or "unknown",
        "side": side if side in {"yes", "no"} else "unknown",
        "strategy_code": strategy_code or "<unknown>",
        "entry_price_band": entry_price_band,
        "forecast_delta_band": delta_band,
        "confidence_band": confidence,
        "spread_band": spread,
        "forecast_delta_f": forecast_delta_value,
        "market_spread_bps": spread_value,
    }


def _context_from_raw(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    for key in ("trade_behavior_context", "empirical_bucket_context"):
        value = raw.get(key)
        if isinstance(value, dict):
            return value
    return {}


def bucket_key_for_fill(fill: FillRecord, order: OrderRecord | None = None) -> str:
    context = _context_from_raw(fill.raw) or _context_from_raw(order.raw if order is not None else None)
    legacy_key = context.get("legacy_bucket_key")
    if legacy_key:
        return str(legacy_key)
    bucket_key_value = context.get("bucket_key")
    if bucket_key_value:
        parts = str(bucket_key_value).split("|")
        if not (len(parts) >= 6 and parts[4].startswith("delta:") and parts[5].startswith(("quality:", "trade_quality:"))):
            return str(bucket_key_value)
    return bucket_key(
        market_ticker=fill.market_ticker,
        side=fill.side,
        strategy_code=fill.strategy_code,
        yes_price_dollars=fill.yes_price_dollars,
        include_price_band=True,
        forecast_delta_f=context.get("forecast_delta_f"),
        confidence_band=context.get("confidence_band"),
        spread_bps=context.get("market_spread_bps"),
        include_context_bands=True,
    )


def coarse_bucket_key_for_fill(fill: FillRecord, order: OrderRecord | None = None) -> str:
    context = _context_from_raw(fill.raw) or _context_from_raw(order.raw if order is not None else None)
    coarse_key = context.get("coarse_bucket_key")
    if coarse_key:
        return str(coarse_key)
    legacy_key = context.get("legacy_bucket_key") or context.get("bucket_key")
    if legacy_key:
        coarse_from_legacy = coarse_empirical_bucket_key_from_legacy(str(legacy_key))
        if coarse_from_legacy:
            return coarse_from_legacy
    return coarse_empirical_bucket_key(
        market_ticker=fill.market_ticker,
        side=fill.side,
        strategy_code=fill.strategy_code,
        yes_price_dollars=fill.yes_price_dollars,
        forecast_delta_f=context.get("forecast_delta_f"),
        confidence_band=context.get("confidence_band"),
    )


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
    bucket_key_version: str = EMPIRICAL_BUCKET_VERSION
    legacy_bucket_key: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "bucket_key": self.bucket_key,
            "coarse_bucket_key": self.bucket_key,
            "legacy_bucket_key": self.legacy_bucket_key,
            "bucket_key_version": self.bucket_key_version,
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


def weather_live_entry_freeze_bypassed(*, control: Any, strategy_code: str | None) -> bool:
    notes = dict(getattr(control, "notes", None) or {})
    weather_live = dict(notes.get("weather_live") or {})
    return (
        strategy_code == StrategyCode.DIRECTIONAL.value
        and bool(weather_live.get("enabled"))
        and str(weather_live.get("policy_state") or "").lower() == "live"
    )


def weather_live_daily_loss_cap_pct(
    *,
    control: Any,
    strategy_code: str | None,
    default_pct: float,
) -> float:
    if not weather_live_entry_freeze_bypassed(control=control, strategy_code=strategy_code):
        return default_pct
    weather_live = dict((getattr(control, "notes", None) or {}).get("weather_live") or {})
    raw = weather_live.get("daily_realized_loss_cap_pct")
    if raw in (None, ""):
        return default_pct
    try:
        cap = float(raw)
    except (TypeError, ValueError):
        return default_pct
    if cap <= 0:
        return default_pct
    return min(default_pct, cap) if default_pct > 0 else cap


def weather_live_max_order_count_fp(
    *,
    control: Any,
    strategy_code: str | None,
    default_count: float,
) -> float:
    if not weather_live_entry_freeze_bypassed(control=control, strategy_code=strategy_code):
        return default_count
    weather_live = dict((getattr(control, "notes", None) or {}).get("weather_live") or {})
    raw = weather_live.get("max_order_count_fp")
    if raw in (None, ""):
        return default_count
    try:
        cap = float(raw)
    except (TypeError, ValueError):
        return default_count
    if cap <= 0:
        return default_count
    return min(default_count, cap)


def entry_pause_reason(
    *,
    settings: Settings,
    control: Any,
    kalshi_env: str | None,
    strategy_code: str | None = None,
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

    if production_entry_freeze_enabled(settings, kalshi_env) and not weather_live_entry_freeze_bypassed(
        control=control,
        strategy_code=strategy_code,
    ):
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
    return replace(thresholds, risk_min_edge_bps=floor)


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
    yes_price_dollars: Any = None,
    forecast_delta_f: Any = None,
    confidence_band: Any = None,
    spread_bps: Any = None,
    now: datetime | None = None,
    production_freeze_bypass: bool = False,
) -> EmpiricalGateDecision:
    legacy_bucket_key = bucket_key(
        market_ticker=market_ticker,
        side=side,
        strategy_code=strategy_code,
        yes_price_dollars=yes_price_dollars,
        include_price_band=True,
        forecast_delta_f=forecast_delta_f,
        confidence_band=confidence_band,
        spread_bps=spread_bps,
        include_context_bands=True,
    )
    gate_bucket_key = coarse_empirical_bucket_key(
        market_ticker=market_ticker,
        side=side,
        strategy_code=strategy_code,
        yes_price_dollars=yes_price_dollars,
        forecast_delta_f=forecast_delta_f,
        confidence_band=confidence_band,
    )
    if not settings.trade_behavior_empirical_gate_enabled:
        return EmpiricalGateDecision(
            status="disabled",
            reason="empirical_gate_disabled",
            bucket_key=gate_bucket_key,
            actual_sample_count=0,
            actual_net_pnl=None,
            actual_win_rate=None,
            legacy_bucket_key=legacy_bucket_key,
        )
    if action != "buy" or side not in {"yes", "no"}:
        return EmpiricalGateDecision(
            status="not_applicable",
            reason="not_new_entry",
            bucket_key=gate_bucket_key,
            actual_sample_count=0,
            actual_net_pnl=None,
            actual_win_rate=None,
            legacy_bucket_key=legacy_bucket_key,
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
    candidate_fills = list((await session.execute(stmt)).scalars())
    order_ids = [fill.order_id for fill in candidate_fills if fill.order_id is not None]
    orders_by_id: dict[str, OrderRecord] = {}
    if order_ids:
        order_rows = list((await session.execute(select(OrderRecord).where(OrderRecord.id.in_(order_ids)))).scalars())
        orders_by_id = {order.id: order for order in order_rows}
    fills = [
        fill
        for fill in candidate_fills
        if coarse_bucket_key_for_fill(fill, orders_by_id.get(str(fill.order_id))) == gate_bucket_key
    ]

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
    if (
        production_live_entry
        and production_entry_freeze_enabled(settings, kalshi_env)
        and not production_freeze_bypass
    ):
        return EmpiricalGateDecision(
            status="blocked",
            reason=settings.trade_behavior_entry_freeze_reason,
            bucket_key=gate_bucket_key,
            actual_sample_count=sample_count,
            actual_net_pnl=net,
            actual_win_rate=win_rate,
            blocks_live_entries=True,
            legacy_bucket_key=legacy_bucket_key,
        )
    if passes:
        return EmpiricalGateDecision(
            status="allowed",
            reason=reason,
            bucket_key=gate_bucket_key,
            actual_sample_count=sample_count,
            actual_net_pnl=net,
            actual_win_rate=win_rate,
            legacy_bucket_key=legacy_bucket_key,
        )
    return EmpiricalGateDecision(
        status="blocked" if production_live_entry else "shadow_only",
        reason=reason,
        bucket_key=gate_bucket_key,
        actual_sample_count=sample_count,
        actual_net_pnl=net,
        actual_win_rate=win_rate,
        blocks_live_entries=production_live_entry,
        legacy_bucket_key=legacy_bucket_key,
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
