from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.config import Settings
from kalshi_bot.db.models import DecisionCorpusRowRecord, DecisionTraceRecord, Room


SCHEMA_VERSION = "gate-learning-report-v1"
DEFAULT_HISTORICAL_BUNDLE_PATHS = (
    Path("data/training/historical_outcome_eval_latest.jsonl"),
    Path("data/training/historical_decision_eval_latest.jsonl"),
)
DEFAULT_FORWARD_SHADOW_PATHS = (
    Path("data/training/forward_shadow_uncurated_bundles.jsonl"),
    Path("data/training/forward_shadow_bundles.jsonl"),
)
SOURCE_CHOICES = {"historical", "forward-shadow", "combined"}


@dataclass(slots=True)
class GateLearningRow:
    source: str
    room_id: str | None
    market_ticker: str
    decision_time: datetime | None
    market_day: str | None
    fair_yes_dollars: Decimal | None = None
    settlement_result: str | None = None
    settlement_value_dollars: Decimal | None = None
    settlement_ts: datetime | None = None
    series_ticker: str | None = None
    city: str | None = None
    side: str | None = None
    entry_price: Decimal | None = None
    remaining_payout_bps: int | None = None
    spread_bps: int | None = None
    confidence: float | None = None
    edge_bps: int | None = None
    quality_adjusted_edge_bps: int | None = None
    forecast_delta_f: float | None = None
    trade_regime: str | None = None
    confidence_band: str | None = None
    spread_band: str | None = None
    forecast_delta_band: str | None = None
    primary_block_reason: str | None = None
    blocked_by: str | None = None
    current_gate_passed: bool = False
    stale_data_mismatch: bool = False
    counterfactual_pnl_dollars: Decimal | None = None

    @property
    def label_win(self) -> bool | None:
        if self.counterfactual_pnl_dollars is None:
            return None
        return self.counterfactual_pnl_dollars > Decimal("0")

    @property
    def labeled(self) -> bool:
        return self.counterfactual_pnl_dollars is not None


def _as_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _date_key(value: datetime | None, fallback: str | None = None) -> str | None:
    if fallback:
        return str(fallback)
    if value is None:
        return None
    return value.date().isoformat()


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    numeric = _float_or_none(value)
    return int(round(numeric)) if numeric is not None else None


def _nested(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _series_from_ticker(market_ticker: str | None) -> str | None:
    if not market_ticker:
        return None
    parts = str(market_ticker).split("-")
    return parts[0] if parts else str(market_ticker)


def _market_day_from_ticker(market_ticker: str | None) -> str | None:
    if not market_ticker:
        return None
    parts = str(market_ticker).split("-")
    return parts[1] if len(parts) >= 2 else None


def _selected_candidate(trace: dict[str, Any]) -> dict[str, Any]:
    side = str(trace.get("selected_side") or "").lower()
    if side in {"yes", "no"} and isinstance(trace.get(side), dict):
        return dict(trace[side])
    selected = trace.get("selected_candidate")
    if isinstance(selected, dict):
        return dict(selected)
    for candidate in trace.get("candidates") or []:
        if isinstance(candidate, dict) and candidate.get("status") == "selected":
            return dict(candidate)
    candidates = [candidate for candidate in trace.get("candidates") or [] if isinstance(candidate, dict)]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            _int_or_none(row.get("quality_adjusted_edge_bps")) or -1_000_000,
            _int_or_none(row.get("edge_bps")) or -1_000_000,
        ),
    )


def _entry_price_for_side(side: str | None, yes_price: Any, selected: dict[str, Any]) -> Decimal | None:
    selected_entry = _decimal_or_none(
        selected.get("traded_price_dollars")
        or selected.get("entry_price")
        or selected.get("entry_price_dollars")
    )
    if selected_entry is not None:
        return selected_entry
    price = _decimal_or_none(yes_price)
    if price is None:
        return None
    if str(side or "").lower() == "no":
        return Decimal("1.0000") - price
    return price


def _remaining_payout_bps(value: Any) -> int | None:
    decimal = _decimal_or_none(value)
    if decimal is None:
        numeric = _float_or_none(value)
        return int(round(numeric)) if numeric is not None else None
    if decimal <= Decimal("1.0000"):
        return int((decimal * Decimal("10000")).to_integral_value())
    return int(decimal.to_integral_value())


def _forecast_delta_bucket(value: float | None) -> str | None:
    if value is None:
        return None
    abs_value = abs(value)
    if abs_value < 2:
        return "flat"
    if abs_value < 5:
        return "near"
    if abs_value < 8:
        return "medium"
    return "wide"


def _spread_band(value: int | None) -> str | None:
    if value is None:
        return None
    if value <= 100:
        return "<=100bps"
    if value <= 500:
        return "101-500bps"
    if value <= 1200:
        return "501-1200bps"
    return "1200bps+"


def _confidence_band(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 0.70:
        return "low"
    if value < 0.80:
        return "medium"
    if value < 0.90:
        return "high"
    return "very_high"


def _jsonl_rows(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    payload = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    yield payload


def _row_from_bundle(payload: dict[str, Any], *, source: str) -> GateLearningRow | None:
    room = _dict(payload.get("room"))
    signal = _dict(payload.get("signal"))
    signal_payload = _dict(signal.get("payload"))
    eligibility = _dict(signal_payload.get("eligibility"))
    candidate_trace = _dict(signal_payload.get("candidate_trace") or _nested(signal_payload, "eligibility", "candidate_trace"))
    selected = _selected_candidate(candidate_trace)
    trade_ticket = _dict(payload.get("trade_ticket"))
    outcome = _dict(payload.get("outcome"))
    audit = _dict(payload.get("strategy_audit"))
    provenance = _dict(payload.get("historical_provenance"))
    settlement_label = _dict(payload.get("settlement_label"))
    settlement_payload = _dict(settlement_label.get("payload"))
    settlement_market = _dict(settlement_payload.get("market"))
    settlement_crosscheck = _dict(settlement_payload.get("crosscheck"))
    campaign = _dict(payload.get("campaign"))
    campaign_payload = _dict(campaign.get("payload"))
    research_dossier = _dict(payload.get("research_dossier"))
    trader_context = _dict(signal_payload.get("trader_context") or research_dossier.get("trader_context"))
    numeric_facts = _dict(trader_context.get("numeric_facts") or _nested(research_dossier, "summary", "current_numeric_facts"))

    market_ticker = (
        payload.get("market_ticker")
        or room.get("market_ticker")
        or signal.get("market_ticker")
        or trade_ticket.get("market_ticker")
    )
    if not market_ticker:
        return None
    market_ticker = str(market_ticker)
    decision_time = (
        _as_utc(payload.get("replay_checkpoint_ts"))
        or _as_utc(signal.get("created_at"))
        or _as_utc(room.get("created_at"))
    )
    side = (
        trade_ticket.get("side")
        or signal_payload.get("recommended_side")
        or candidate_trace.get("selected_side")
        or selected.get("side")
    )
    side = str(side).lower() if side else None
    yes_price = (
        trade_ticket.get("yes_price_dollars")
        or signal_payload.get("target_yes_price_dollars")
        or selected.get("target_yes_price_dollars")
    )
    forecast_high = _float_or_none(numeric_facts.get("forecast_high_f"))
    threshold = _float_or_none(
        numeric_facts.get("threshold_f")
        or campaign_payload.get("threshold_f")
        or _nested(payload, "weather_bundle", "mapping", "threshold_f")
    )
    forecast_delta = _float_or_none(signal_payload.get("forecast_delta_f") or numeric_facts.get("forecast_delta_f"))
    if forecast_delta is None and forecast_high is not None and threshold is not None:
        forecast_delta = forecast_high - threshold
    confidence = _float_or_none(signal.get("confidence") or signal_payload.get("confidence") or trader_context.get("confidence"))
    fair_yes = _decimal_or_none(
        payload.get("fair_yes_dollars")
        or signal.get("fair_yes_dollars")
        or signal_payload.get("fair_yes_dollars")
        or trader_context.get("fair_yes_dollars")
    )
    spread_bps = _int_or_none(eligibility.get("market_spread_bps") or selected.get("spread_bps"))
    qa_edge = _int_or_none(
        selected.get("quality_adjusted_edge_bps")
        or eligibility.get("edge_after_quality_buffer_bps")
    )
    edge_bps = _int_or_none(signal.get("edge_bps") or selected.get("edge_bps"))
    if qa_edge is None and edge_bps is not None:
        qa_edge = edge_bps - 25
    entry_price = _entry_price_for_side(side, yes_price, selected)
    cfpnl = _decimal_or_none(
        payload.get("counterfactual_pnl_dollars")
        or payload.get("lifecycle_net_pnl")
        or payload.get("lifecycle_net_pnl_dollars")
    )
    primary_block_reason = (
        candidate_trace.get("primary_block_reason")
        or candidate_trace.get("baseline_block_reason")
        or outcome.get("stand_down_reason")
        or audit.get("stand_down_reason")
        or eligibility.get("stand_down_reason")
        or signal_payload.get("stand_down_reason")
    )
    market_day = (
        provenance.get("local_market_day")
        or settlement_label.get("local_market_day")
        or payload.get("local_market_day")
        or _market_day_from_ticker(market_ticker)
    )
    series = settlement_label.get("series_ticker") or campaign_payload.get("series_ticker") or _series_from_ticker(market_ticker)
    settlement_result = (
        settlement_label.get("kalshi_result")
        or settlement_label.get("crosscheck_result")
        or settlement_crosscheck.get("result")
        or settlement_market.get("result")
    )
    settlement_ts = (
        _as_utc(settlement_label.get("settlement_ts"))
        or _as_utc(settlement_market.get("close_time"))
        or _as_utc(settlement_label.get("updated_at"))
    )
    current_gate_passed = bool(
        outcome.get("eligibility_passed")
        or audit.get("eligibility_passed")
        or outcome.get("ticket_generated")
        or trade_ticket
    )
    return GateLearningRow(
        source=source,
        room_id=str(payload.get("room_id") or room.get("id") or audit.get("room_id") or "") or None,
        market_ticker=market_ticker,
        decision_time=decision_time,
        market_day=str(market_day) if market_day else _date_key(decision_time),
        fair_yes_dollars=fair_yes,
        settlement_result=str(settlement_result).lower() if settlement_result not in (None, "") else None,
        settlement_value_dollars=_decimal_or_none(settlement_label.get("settlement_value_dollars")),
        settlement_ts=settlement_ts,
        series_ticker=str(series) if series else None,
        city=str(campaign_payload.get("city_bucket") or campaign.get("city_bucket") or "") or None,
        side=side,
        entry_price=entry_price,
        remaining_payout_bps=_remaining_payout_bps(
            eligibility.get("remaining_payout_dollars") or selected.get("remaining_payout_dollars")
        ),
        spread_bps=spread_bps,
        confidence=confidence,
        edge_bps=edge_bps,
        quality_adjusted_edge_bps=qa_edge,
        forecast_delta_f=forecast_delta,
        trade_regime=str(signal_payload.get("trade_regime") or audit.get("trade_regime") or "") or None,
        confidence_band=str(signal_payload.get("confidence_band") or _confidence_band(confidence) or "") or None,
        spread_band=_spread_band(spread_bps),
        forecast_delta_band=str(signal_payload.get("forecast_delta_band") or _forecast_delta_bucket(forecast_delta) or "") or None,
        primary_block_reason=str(primary_block_reason) if primary_block_reason not in (None, "") else None,
        blocked_by=str(outcome.get("blocked_by") or audit.get("blocked_by") or "") or None,
        current_gate_passed=current_gate_passed,
        stale_data_mismatch=bool(audit.get("stale_data_mismatch")),
        counterfactual_pnl_dollars=cfpnl,
    )


def _row_from_decision_trace(record: DecisionTraceRecord, room: Room | None = None) -> GateLearningRow | None:
    trace = _dict(record.trace)
    candidate_trace = _dict(trace.get("candidate_trace"))
    selected = _selected_candidate(candidate_trace)
    signal = _dict(trace.get("signal"))
    eligibility = _dict(signal.get("eligibility"))
    ticket = _dict(trace.get("ticket"))
    market_ticker = str(record.market_ticker or trace.get("market_ticker") or (room.market_ticker if room else "") or "")
    if not market_ticker:
        return None
    side = ticket.get("side") or signal.get("recommended_side") or candidate_trace.get("selected_side") or selected.get("side")
    side = str(side).lower() if side else None
    yes_price = ticket.get("yes_price_dollars") or signal.get("target_yes_price_dollars") or selected.get("target_yes_price_dollars")
    decision_time = _as_utc(record.decision_time) or _as_utc(record.created_at)
    forecast_delta = _float_or_none(signal.get("forecast_delta_f") or selected.get("forecast_threshold_delta_f"))
    confidence = _float_or_none(signal.get("confidence"))
    spread_bps = _int_or_none(candidate_trace.get("spread_bps") or selected.get("spread_bps") or eligibility.get("market_spread_bps"))
    edge_bps = _int_or_none(signal.get("edge_bps") or selected.get("edge_bps") or candidate_trace.get("selected_edge_bps"))
    qa_edge = _int_or_none(selected.get("quality_adjusted_edge_bps") or eligibility.get("edge_after_quality_buffer_bps"))
    return GateLearningRow(
        source="decision_trace",
        room_id=str(record.room_id or trace.get("room_id") or "") or None,
        market_ticker=market_ticker,
        decision_time=decision_time,
        market_day=_date_key(decision_time, _market_day_from_ticker(market_ticker)),
        fair_yes_dollars=_decimal_or_none(signal.get("fair_yes_dollars") or trace.get("fair_yes_dollars")),
        series_ticker=_series_from_ticker(market_ticker),
        side=side,
        entry_price=_entry_price_for_side(side, yes_price, selected),
        remaining_payout_bps=_remaining_payout_bps(eligibility.get("remaining_payout_dollars") or selected.get("remaining_payout_dollars")),
        spread_bps=spread_bps,
        confidence=confidence,
        edge_bps=edge_bps,
        quality_adjusted_edge_bps=qa_edge,
        forecast_delta_f=forecast_delta,
        trade_regime=str(signal.get("trade_regime") or "") or None,
        confidence_band=str(signal.get("confidence_band") or _confidence_band(confidence) or "") or None,
        spread_band=_spread_band(spread_bps),
        forecast_delta_band=_forecast_delta_bucket(forecast_delta),
        primary_block_reason=str(
            candidate_trace.get("primary_block_reason")
            or candidate_trace.get("baseline_block_reason")
            or candidate_trace.get("eligibility_stand_down_reason")
            or eligibility.get("stand_down_reason")
            or ""
        ) or None,
        blocked_by=str(trace.get("evaluation_outcome") or "") or None,
        current_gate_passed=bool(ticket),
        stale_data_mismatch=False,
        counterfactual_pnl_dollars=None,
    )


def _spread_bps_from_quote_snapshot(snapshot: dict[str, Any]) -> int | None:
    yes_bid = _decimal_or_none(snapshot.get("yes_bid_dollars") or snapshot.get("yes_bid"))
    yes_ask = _decimal_or_none(snapshot.get("yes_ask_dollars") or snapshot.get("yes_ask"))
    spread = _decimal_or_none(snapshot.get("spread_dollars"))
    spread_bps = _int_or_none(snapshot.get("spread_bps"))
    if spread_bps is not None:
        return spread_bps
    if spread is not None:
        return int((spread * Decimal("10000")).to_integral_value())
    if yes_bid is not None and yes_ask is not None:
        return int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value())
    return None


def decision_corpus_row_to_gate_learning_row(record: DecisionCorpusRowRecord) -> GateLearningRow | None:
    market_ticker = str(record.market_ticker or "")
    if not market_ticker:
        return None
    signal_payload = _dict(record.signal_payload)
    quote_snapshot = _dict(record.quote_snapshot)
    diagnostics = _dict(record.diagnostics)
    settlement_payload = _dict(record.settlement_payload)
    eligibility = _dict(signal_payload.get("eligibility"))
    candidate_trace = _dict(signal_payload.get("candidate_trace") or eligibility.get("candidate_trace"))
    selected = _selected_candidate(candidate_trace)

    side = str(record.recommended_side or selected.get("side") or candidate_trace.get("selected_side") or "").lower() or None
    yes_price = record.target_yes_price_dollars or signal_payload.get("target_yes_price_dollars") or selected.get("target_yes_price_dollars")
    entry_price = _entry_price_for_side(side, yes_price, selected)
    remaining_payout = _remaining_payout_bps(
        eligibility.get("remaining_payout_dollars")
        or selected.get("remaining_payout_dollars")
        or (Decimal("1.0000") - entry_price if entry_price is not None else None)
    )
    edge_bps = _int_or_none(record.edge_bps or selected.get("edge_bps") or signal_payload.get("edge_bps"))
    qa_edge = _int_or_none(
        selected.get("quality_adjusted_edge_bps")
        or eligibility.get("edge_after_quality_buffer_bps")
        or signal_payload.get("quality_adjusted_edge_bps")
    )
    if qa_edge is None:
        qa_edge = edge_bps
    spread_bps = _int_or_none(selected.get("spread_bps") or eligibility.get("market_spread_bps"))
    if spread_bps is None:
        spread_bps = _spread_bps_from_quote_snapshot(quote_snapshot)
    observed_at = (
        _as_utc(record.created_at)
        or _as_utc(settlement_payload.get("settlement_ts"))
        or _as_utc(settlement_payload.get("updated_at"))
        or _as_utc(record.checkpoint_ts)
    )
    pnl = _decimal_or_none(record.pnl_counterfactual_target_with_fees)
    if pnl is None:
        pnl = _decimal_or_none(record.pnl_counterfactual_target_frictionless)

    return GateLearningRow(
        source="decision_corpus",
        room_id=str(record.room_id or "") or None,
        market_ticker=market_ticker,
        decision_time=_as_utc(record.checkpoint_ts),
        market_day=str(record.local_market_day or "") or _market_day_from_ticker(market_ticker),
        fair_yes_dollars=_decimal_or_none(record.fair_yes_dollars or signal_payload.get("fair_yes_dollars")),
        settlement_result=str(record.settlement_result).lower() if record.settlement_result not in (None, "") else None,
        settlement_value_dollars=_decimal_or_none(record.settlement_value_dollars),
        settlement_ts=observed_at,
        series_ticker=record.series_ticker or _series_from_ticker(market_ticker),
        city=str(diagnostics.get("city") or diagnostics.get("city_bucket") or "") or None,
        side=side,
        entry_price=entry_price,
        remaining_payout_bps=remaining_payout,
        spread_bps=spread_bps,
        confidence=_float_or_none(record.confidence or signal_payload.get("confidence")),
        edge_bps=edge_bps,
        quality_adjusted_edge_bps=qa_edge,
        forecast_delta_f=_float_or_none(signal_payload.get("forecast_delta_f") or diagnostics.get("forecast_delta_f")),
        trade_regime=str(record.trade_regime or signal_payload.get("trade_regime") or "") or None,
        confidence_band=str(signal_payload.get("confidence_band") or _confidence_band(record.confidence) or "") or None,
        spread_band=str(record.liquidity_regime or _spread_band(spread_bps) or "") or None,
        forecast_delta_band=str(signal_payload.get("forecast_delta_band") or _forecast_delta_bucket(_float_or_none(signal_payload.get("forecast_delta_f"))) or "") or None,
        primary_block_reason=str(
            candidate_trace.get("primary_block_reason")
            or candidate_trace.get("baseline_block_reason")
            or record.stand_down_reason
            or ""
        ) or None,
        blocked_by=str(record.eligibility_status or record.stand_down_reason or "") or None,
        current_gate_passed=str(record.eligibility_status or "").lower() in {"eligible", "selected", "approved", "executed"},
        stale_data_mismatch=bool(diagnostics.get("stale_data_mismatch")),
        counterfactual_pnl_dollars=pnl,
    )


def _money(value: Decimal | None) -> str | None:
    return str(value.quantize(Decimal("0.0001"))) if value is not None else None


def _source_path_text(paths: Iterable[Path]) -> list[str]:
    return [str(path) for path in paths if path.exists()]


def _feature_richness(row: GateLearningRow) -> int:
    return sum(
        getattr(row, field) is not None
        for field in (
            "fair_yes_dollars",
            "settlement_result",
            "settlement_ts",
            "series_ticker",
            "city",
            "side",
            "entry_price",
            "remaining_payout_bps",
            "spread_bps",
            "confidence",
            "edge_bps",
            "quality_adjusted_edge_bps",
            "forecast_delta_f",
            "trade_regime",
            "confidence_band",
            "spread_band",
            "forecast_delta_band",
            "primary_block_reason",
        )
    )


def _merge_bundle_rows(existing: GateLearningRow, incoming: GateLearningRow) -> GateLearningRow:
    incoming_is_richer = _feature_richness(incoming) > _feature_richness(existing)
    rich_fields = (
        "fair_yes_dollars",
        "settlement_result",
        "settlement_value_dollars",
        "settlement_ts",
        "series_ticker",
        "city",
        "side",
        "entry_price",
        "remaining_payout_bps",
        "spread_bps",
        "confidence",
        "edge_bps",
        "quality_adjusted_edge_bps",
        "forecast_delta_f",
        "trade_regime",
        "confidence_band",
        "spread_band",
        "forecast_delta_band",
        "primary_block_reason",
        "blocked_by",
    )
    for field in rich_fields:
        existing_value = getattr(existing, field)
        incoming_value = getattr(incoming, field)
        if incoming_value is None:
            continue
        if existing_value is None or incoming_is_richer:
            setattr(existing, field, incoming_value)
    for field in ("decision_time", "market_day"):
        if getattr(existing, field) is None and getattr(incoming, field) is not None:
            setattr(existing, field, getattr(incoming, field))
    existing.current_gate_passed = existing.current_gate_passed or incoming.current_gate_passed
    existing.stale_data_mismatch = existing.stale_data_mismatch or incoming.stale_data_mismatch
    if existing.counterfactual_pnl_dollars is None and incoming.counterfactual_pnl_dollars is not None:
        existing.counterfactual_pnl_dollars = incoming.counterfactual_pnl_dollars
    return existing


def _score_rows(rows: list[GateLearningRow], predicate: Callable[[GateLearningRow], bool]) -> dict[str, Any]:
    kept = [row for row in rows if predicate(row)]
    kept_pnls = [row.counterfactual_pnl_dollars for row in kept if row.counterfactual_pnl_dollars is not None]
    blocked = [row for row in rows if not predicate(row)]
    blocked_pnls = [row.counterfactual_pnl_dollars for row in blocked if row.counterfactual_pnl_dollars is not None]
    net = sum(kept_pnls, Decimal("0"))
    wins = sum(1 for value in kept_pnls if value > Decimal("0"))
    missed_profit = sum((value for value in blocked_pnls if value > Decimal("0")), Decimal("0"))
    blocked_loss = sum((-value for value in blocked_pnls if value < Decimal("0")), Decimal("0"))
    return {
        "sample_count": len(kept_pnls),
        "net_pnl": net,
        "win_rate": (wins / len(kept_pnls)) if kept_pnls else None,
        "missed_profit": missed_profit,
        "blocked_loss": blocked_loss,
        "stale_regression_count": sum(1 for row in kept if row.stale_data_mismatch),
        "max_drawdown_proxy": _max_drawdown_proxy(kept),
    }


def _max_drawdown_proxy(rows: list[GateLearningRow]) -> Decimal:
    ordered = sorted(rows, key=lambda row: (row.market_day or "", row.decision_time or datetime.min.replace(tzinfo=UTC), row.room_id or ""))
    equity = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for row in ordered:
        if row.counterfactual_pnl_dollars is None:
            continue
        equity += row.counterfactual_pnl_dollars
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _walk_forward_split(rows: list[GateLearningRow]) -> tuple[list[GateLearningRow], list[GateLearningRow]]:
    ordered = sorted(rows, key=lambda row: (row.market_day or "", row.decision_time or datetime.min.replace(tzinfo=UTC), row.room_id or ""))
    days = sorted({row.market_day for row in ordered if row.market_day})
    if len(days) >= 3:
        holdout_start = days[max(1, int(len(days) * 0.70))]
        train = [row for row in ordered if (row.market_day or "") < holdout_start]
        holdout = [row for row in ordered if (row.market_day or "") >= holdout_start]
        if train and holdout:
            return train, holdout
    split_idx = max(1, int(len(ordered) * 0.70))
    return ordered[:split_idx], ordered[split_idx:]


def _numeric_min(field: str, threshold: float) -> Callable[[GateLearningRow], bool]:
    return lambda row: (value := getattr(row, field)) is not None and float(value) >= threshold


def _numeric_abs_min(field: str, threshold: float) -> Callable[[GateLearningRow], bool]:
    return lambda row: (value := getattr(row, field)) is not None and abs(float(value)) >= threshold


def _numeric_max(field: str, threshold: float) -> Callable[[GateLearningRow], bool]:
    return lambda row: (value := getattr(row, field)) is not None and float(value) <= threshold


def _categorical_not(field: str, value: str) -> Callable[[GateLearningRow], bool]:
    return lambda row: str(getattr(row, field) or "<unknown>") != value


@dataclass(slots=True)
class CandidatePolicy:
    gate: str
    candidate_policy: str
    predicate: Callable[[GateLearningRow], bool]


@dataclass(slots=True)
class BaselineGatePolicy:
    config_field: str
    gate: str
    candidate_policy: str
    threshold: Decimal | int | float
    field_name: str
    predicate: Callable[[GateLearningRow], bool]
    current_predicate: Callable[[GateLearningRow], bool]
    feature_predicate: Callable[[GateLearningRow], bool]


BASELINE_GATE_FIELDS = (
    "risk_min_contract_price_dollars",
    "strategy_min_remaining_payout_bps",
    "trigger_max_spread_bps",
    "risk_min_confidence",
    "risk_min_edge_bps",
    "strategy_min_abs_delta_f",
    "risk_max_credible_edge_bps",
)


def _candidate_policies(rows: list[GateLearningRow]) -> list[CandidatePolicy]:
    policies: list[CandidatePolicy] = []
    for threshold in (0.03, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25):
        policies.append(CandidatePolicy("entry_price", f"entry_price_min_{threshold:.2f}", _numeric_min("entry_price", threshold)))
    for threshold in (500, 1000, 1500, 2000, 2500):
        policies.append(CandidatePolicy("remaining_payout", f"remaining_payout_min_{threshold}bps", _numeric_min("remaining_payout_bps", threshold)))
    for threshold in (100, 250, 500, 800, 1000, 1200, 1500):
        policies.append(CandidatePolicy("spread", f"spread_max_{threshold}bps", _numeric_max("spread_bps", threshold)))
    for threshold in (0.60, 0.70, 0.75, 0.80, 0.85, 0.90):
        policies.append(CandidatePolicy("confidence", f"confidence_min_{threshold:.2f}", _numeric_min("confidence", threshold)))
    for threshold in (500, 1000, 1500, 2500, 5000):
        policies.append(CandidatePolicy("edge", f"edge_min_{threshold}bps", _numeric_min("edge_bps", threshold)))
        policies.append(CandidatePolicy("quality_adjusted_edge", f"qa_edge_min_{threshold}bps", _numeric_min("quality_adjusted_edge_bps", threshold)))
    for threshold in (0, 2, 5, 8, 10):
        policies.append(CandidatePolicy("forecast_separation", f"abs_forecast_delta_min_{threshold}f", _numeric_abs_min("forecast_delta_f", threshold)))
    for threshold in (2500, 5000, 7500):
        policies.append(CandidatePolicy("max_credible_edge", f"edge_max_{threshold}bps", _numeric_max("edge_bps", threshold)))

    policies.append(
        CandidatePolicy(
            "low_price_high_edge_interaction",
            "entry_price_min_0.05_and_qa_edge_min_1500_and_spread_max_1200",
            lambda row: (
                row.entry_price is not None
                and row.entry_price >= Decimal("0.0500")
                and (row.quality_adjusted_edge_bps or -1_000_000) >= 1500
                and (row.spread_bps is None or row.spread_bps <= 1200)
            ),
        )
    )
    policies.append(
        CandidatePolicy(
            "near_threshold_high_edge_interaction",
            "abs_forecast_delta_min_2_or_qa_edge_min_2500",
            lambda row: (
                row.forecast_delta_f is not None
                and (abs(row.forecast_delta_f) >= 2 or (row.quality_adjusted_edge_bps or -1_000_000) >= 2500)
            ),
        )
    )

    categorical_fields = (
        "series_ticker",
        "city",
        "side",
        "confidence_band",
        "spread_band",
        "forecast_delta_band",
        "trade_regime",
        "primary_block_reason",
    )
    for field in categorical_fields:
        counts = Counter(str(getattr(row, field) or "<unknown>") for row in rows)
        for value, count in counts.most_common(10):
            if count <= 0:
                continue
            policies.append(CandidatePolicy(field, f"exclude_{field}_{value}", _categorical_not(field, value)))
    return policies


def _baseline_gate_policies(settings: Settings) -> list[BaselineGatePolicy]:
    policies: list[BaselineGatePolicy] = []
    current_min_price = Decimal(str(settings.risk_min_contract_price_dollars))
    for threshold in (Decimal("0.03"), Decimal("0.05"), Decimal("0.08"), Decimal("0.10"), Decimal("0.15"), Decimal("0.20"), Decimal("0.25")):
        policies.append(
            BaselineGatePolicy(
                config_field="risk_min_contract_price_dollars",
                gate="entry_price",
                candidate_policy=f"risk_min_contract_price_dollars={threshold:.2f}",
                threshold=threshold,
                field_name="entry_price",
                predicate=lambda row, threshold=threshold: row.entry_price is not None and row.entry_price >= threshold,
                current_predicate=lambda row, current_min_price=current_min_price: (
                    row.entry_price is not None and row.entry_price >= current_min_price
                ),
                feature_predicate=lambda row: row.entry_price is not None,
            )
        )

    current_remaining = int(settings.strategy_min_remaining_payout_bps)
    for threshold in (500, 1000, 1500, 2000, 2500, 3000, 3500):
        policies.append(
            BaselineGatePolicy(
                config_field="strategy_min_remaining_payout_bps",
                gate="remaining_payout",
                candidate_policy=f"strategy_min_remaining_payout_bps={threshold}",
                threshold=threshold,
                field_name="remaining_payout_bps",
                predicate=lambda row, threshold=threshold: (
                    row.remaining_payout_bps is not None and row.remaining_payout_bps >= threshold
                ),
                current_predicate=lambda row, current_remaining=current_remaining: (
                    row.remaining_payout_bps is not None and row.remaining_payout_bps >= current_remaining
                ),
                feature_predicate=lambda row: row.remaining_payout_bps is not None,
            )
        )

    current_spread = int(settings.trigger_max_spread_bps)
    for threshold in (100, 250, 500, 800, 1000, 1200, 1500, 2000, 2500):
        policies.append(
            BaselineGatePolicy(
                config_field="trigger_max_spread_bps",
                gate="spread",
                candidate_policy=f"trigger_max_spread_bps={threshold}",
                threshold=threshold,
                field_name="spread_bps",
                predicate=lambda row, threshold=threshold: row.spread_bps is not None and row.spread_bps <= threshold,
                current_predicate=lambda row, current_spread=current_spread: row.spread_bps is not None and row.spread_bps <= current_spread,
                feature_predicate=lambda row: row.spread_bps is not None,
            )
        )

    current_confidence = float(settings.risk_min_confidence)
    for threshold in (0.60, 0.70, 0.75, 0.80, 0.85, 0.90):
        policies.append(
            BaselineGatePolicy(
                config_field="risk_min_confidence",
                gate="confidence",
                candidate_policy=f"risk_min_confidence={threshold:.2f}",
                threshold=threshold,
                field_name="confidence",
                predicate=lambda row, threshold=threshold: row.confidence is not None and row.confidence >= threshold,
                current_predicate=lambda row, current_confidence=current_confidence: (
                    row.confidence is not None and row.confidence >= current_confidence
                ),
                feature_predicate=lambda row: row.confidence is not None,
            )
        )

    current_min_edge = int(settings.risk_min_edge_bps)
    for threshold in (250, 500, 750, 1000, 1500, 2500, 5000):
        policies.append(
            BaselineGatePolicy(
                config_field="risk_min_edge_bps",
                gate="quality_adjusted_edge",
                candidate_policy=f"risk_min_edge_bps={threshold}",
                threshold=threshold,
                field_name="quality_adjusted_edge_bps",
                predicate=lambda row, threshold=threshold: (
                    row.quality_adjusted_edge_bps is not None and row.quality_adjusted_edge_bps >= threshold
                ),
                current_predicate=lambda row, current_min_edge=current_min_edge: (
                    row.quality_adjusted_edge_bps is not None and row.quality_adjusted_edge_bps >= current_min_edge
                ),
                feature_predicate=lambda row: row.quality_adjusted_edge_bps is not None,
            )
        )

    current_min_delta = float(settings.strategy_min_abs_delta_f)
    for threshold in (0.0, 2.0, 3.0, 5.0, 8.0, 10.0):
        policies.append(
            BaselineGatePolicy(
                config_field="strategy_min_abs_delta_f",
                gate="forecast_separation",
                candidate_policy=f"strategy_min_abs_delta_f={threshold:.1f}",
                threshold=threshold,
                field_name="forecast_delta_f",
                predicate=lambda row, threshold=threshold: row.forecast_delta_f is not None and abs(row.forecast_delta_f) >= threshold,
                current_predicate=lambda row, current_min_delta=current_min_delta: (
                    row.forecast_delta_f is not None and abs(row.forecast_delta_f) >= current_min_delta
                ),
                feature_predicate=lambda row: row.forecast_delta_f is not None,
            )
        )

    current_max_edge = int(settings.risk_max_credible_edge_bps)
    for threshold in (2500, 5000, 7500, 10000):
        policies.append(
            BaselineGatePolicy(
                config_field="risk_max_credible_edge_bps",
                gate="max_credible_edge",
                candidate_policy=f"risk_max_credible_edge_bps={threshold}",
                threshold=threshold,
                field_name="edge_bps",
                predicate=lambda row, threshold=threshold: row.edge_bps is not None and row.edge_bps <= threshold,
                current_predicate=lambda row, current_max_edge=current_max_edge: row.edge_bps is not None and row.edge_bps <= current_max_edge,
                feature_predicate=lambda row: row.edge_bps is not None,
            )
        )
    return policies


class GateLearningService:
    def __init__(
        self,
        settings: Settings,
        *,
        historical_paths: Iterable[Path] = DEFAULT_HISTORICAL_BUNDLE_PATHS,
        forward_shadow_paths: Iterable[Path] = DEFAULT_FORWARD_SHADOW_PATHS,
    ) -> None:
        self.settings = settings
        self.historical_paths = tuple(historical_paths)
        self.forward_shadow_paths = tuple(forward_shadow_paths)

    async def build_report(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 180,
        source: str = "combined",
        min_support: int | None = None,
        session: AsyncSession | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if source not in SOURCE_CHOICES:
            raise ValueError(f"unsupported gate-learning source {source!r}")
        min_support = int(min_support if min_support is not None else getattr(self.settings, "gate_learning_min_support", 30))
        now_utc = _as_utc(now) or datetime.now(UTC)
        cutoff = now_utc - timedelta(days=days)
        rows = self.load_bundle_rows(source=source)
        if session is not None and source == "combined":
            rows.extend(await self.load_decision_trace_rows(session, kalshi_env=kalshi_env, cutoff=cutoff))
        rows = [
            row for row in rows
            if row.decision_time is None or row.decision_time >= cutoff
        ]
        labeled = [row for row in rows if row.labeled]
        train, holdout = _walk_forward_split(labeled)
        current_train = _score_rows(train, lambda row: row.current_gate_passed)
        current_holdout = _score_rows(holdout, lambda row: row.current_gate_passed)
        recommendations = [
            self._score_policy(
                policy,
                train=train,
                holdout=holdout,
                current_holdout_net=current_holdout["net_pnl"],
                min_support=min_support,
            )
            for policy in _candidate_policies(labeled)
        ]
        recommendations.sort(
            key=lambda row: (
                row["recommendation"] != "insufficient_evidence",
                _decimal_or_none(row.get("net_improvement_dollars")) or Decimal("-999999"),
                _decimal_or_none(row.get("holdout_net_pnl")) or Decimal("-999999"),
                int(row.get("support_count") or 0),
            ),
            reverse=True,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "kalshi_env": kalshi_env,
            "window_days": days,
            "source": source,
            "source_files": self.source_files(source=source),
            "read_only": True,
            "objective": "net_counterfactual_pnl",
            "min_support": min_support,
            "row_counts": {
                "total_rows": len(rows),
                "labeled_rows": len(labeled),
                "unlabeled_rows": len(rows) - len(labeled),
                "train_rows": len(train),
                "holdout_rows": len(holdout),
                "sources": dict(Counter(row.source for row in rows)),
            },
            "current_gate_performance": {
                "train_net_pnl": _money(current_train["net_pnl"]),
                "train_sample_count": current_train["sample_count"],
                "holdout_net_pnl": _money(current_holdout["net_pnl"]),
                "holdout_sample_count": current_holdout["sample_count"],
                "holdout_win_rate": current_holdout["win_rate"],
            },
            "recommendations": recommendations[:50],
            "regret_rows": self._example_rows(
                [row for row in labeled if not row.current_gate_passed and (row.counterfactual_pnl_dollars or Decimal("0")) > 0],
                reverse=True,
            ),
            "saved_loss_rows": self._example_rows(
                [row for row in labeled if not row.current_gate_passed and (row.counterfactual_pnl_dollars or Decimal("0")) < 0],
                reverse=False,
            ),
            "confidence_warnings": self._confidence_warnings(labeled, holdout, min_support),
        }

    async def build_recommendation_report(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 180,
        source: str = "combined",
        min_support: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if source not in SOURCE_CHOICES:
            raise ValueError(f"unsupported gate-learning source {source!r}")
        min_support = int(min_support if min_support is not None else getattr(self.settings, "gate_learning_min_support", 30))
        now_utc = _as_utc(now) or datetime.now(UTC)
        cutoff = now_utc - timedelta(days=days)
        rows = [
            row for row in self.load_bundle_rows(source=source)
            if row.decision_time is None or row.decision_time >= cutoff
        ]
        labeled = [row for row in rows if row.labeled]
        train, holdout = _walk_forward_split(labeled)
        gate_evidence = [
            self._score_baseline_gate_policy(
                policy,
                train=train,
                holdout=holdout,
                min_support=min_support,
            )
            for policy in _baseline_gate_policies(self.settings)
        ]
        gate_evidence.sort(
            key=lambda row: (
                row["promotion_status"] == "promoted",
                _decimal_or_none(row.get("net_improvement_dollars")) or Decimal("-999999"),
                -(_decimal_or_none(row.get("candidate_holdout_drawdown_proxy")) or Decimal("999999")),
                int(row.get("support_count") or 0),
            ),
            reverse=True,
        )
        recommended_settings: dict[str, dict[str, Any]] = {}
        for field in BASELINE_GATE_FIELDS:
            current_value = getattr(self.settings, field)
            field_rows = [row for row in gate_evidence if row["config_field"] == field]
            promoted = [row for row in field_rows if row["promotion_status"] == "promoted"]
            best = self._best_gate_candidate(promoted or field_rows, current_value=current_value)
            recommended_settings[field] = {
                "current": current_value,
                "recommended": best["threshold"] if promoted and best is not None else current_value,
                "changed": bool(promoted),
                "candidate_policy": best["candidate_policy"] if best is not None else None,
                "reason": (
                    "promoted_by_walk_forward_pnl_and_drawdown"
                    if promoted
                    else (best["promotion_status"] if best is not None else "no_candidate_policy")
                ),
            }
        return {
            "schema_version": "gate-learning-recommendations-v1",
            "kalshi_env": kalshi_env,
            "window_days": days,
            "source": source,
            "source_files": self.source_files(source=source),
            "read_only": True,
            "objective": "replace_baseline_defaults_when_holdout_net_pnl_improves_without_worse_drawdown",
            "min_support": min_support,
            "promotion_holdout_support": max(10, math.ceil(min_support / 4)),
            "row_counts": {
                "total_rows": len(rows),
                "labeled_rows": len(labeled),
                "unlabeled_rows": len(rows) - len(labeled),
                "train_rows": len(train),
                "holdout_rows": len(holdout),
                "sources": dict(Counter(row.source for row in rows)),
            },
            "recommended_settings": recommended_settings,
            "gate_evidence": gate_evidence,
            "confidence_warnings": self._confidence_warnings(labeled, holdout, min_support),
        }

    @staticmethod
    def _best_gate_candidate(rows: list[dict[str, Any]], *, current_value: Any) -> dict[str, Any] | None:
        if not rows:
            return None
        current_decimal = _decimal_or_none(current_value) or Decimal("0")
        return max(
            rows,
            key=lambda row: (
                _decimal_or_none(row.get("net_improvement_dollars")) or Decimal("-999999"),
                -(_decimal_or_none(row.get("drawdown_delta_dollars")) or Decimal("999999")),
                -abs((_decimal_or_none(row.get("threshold")) or Decimal("0")) - current_decimal),
                int(row.get("support_count") or 0),
            ),
        )

    def load_bundle_rows(self, *, source: str) -> list[GateLearningRow]:
        paths: list[tuple[str, tuple[Path, ...]]] = []
        if source in {"historical", "combined"}:
            paths.append(("historical", self.historical_paths))
        if source in {"forward-shadow", "combined"}:
            paths.append(("forward-shadow", self.forward_shadow_paths))
        rows_by_key: dict[tuple[str, str, str, str], GateLearningRow] = {}
        for source_name, bundle_paths in paths:
            for payload in _jsonl_rows(bundle_paths):
                row = _row_from_bundle(payload, source=source_name)
                if row is None:
                    continue
                key = (
                    source_name,
                    row.room_id or "",
                    row.market_ticker,
                    row.decision_time.isoformat() if row.decision_time is not None else row.market_day or "",
                )
                existing = rows_by_key.get(key)
                rows_by_key[key] = row if existing is None else _merge_bundle_rows(existing, row)
        return list(rows_by_key.values())

    def source_files(self, *, source: str) -> dict[str, list[str]]:
        files: dict[str, list[str]] = {}
        if source in {"historical", "combined"}:
            files["historical"] = _source_path_text(self.historical_paths)
        if source in {"forward-shadow", "combined"}:
            files["forward-shadow"] = _source_path_text(self.forward_shadow_paths)
        return files

    async def load_decision_trace_rows(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        cutoff: datetime,
    ) -> list[GateLearningRow]:
        result = await session.execute(
            select(DecisionTraceRecord, Room)
            .join(Room, DecisionTraceRecord.room_id == Room.id)
            .where(
                DecisionTraceRecord.kalshi_env == kalshi_env,
                DecisionTraceRecord.decision_time >= cutoff,
            )
            .order_by(DecisionTraceRecord.decision_time.asc(), DecisionTraceRecord.id.asc())
        )
        rows: list[GateLearningRow] = []
        for record, room in result.all():
            row = _row_from_decision_trace(record, room)
            if row is not None:
                rows.append(row)
        return rows

    def _score_policy(
        self,
        policy: CandidatePolicy,
        *,
        train: list[GateLearningRow],
        holdout: list[GateLearningRow],
        current_holdout_net: Decimal,
        min_support: int,
    ) -> dict[str, Any]:
        train_score = _score_rows(train, policy.predicate)
        holdout_score = _score_rows(holdout, policy.predicate)
        total_support = train_score["sample_count"] + holdout_score["sample_count"]
        net_improvement = holdout_score["net_pnl"] - current_holdout_net
        recommendation = self._recommendation(
            support_count=total_support,
            holdout_support=holdout_score["sample_count"],
            net_improvement=net_improvement,
            stale_regression_count=holdout_score["stale_regression_count"],
            min_support=min_support,
        )
        return {
            "gate": policy.gate,
            "candidate_policy": policy.candidate_policy,
            "support_count": total_support,
            "train_net_pnl": _money(train_score["net_pnl"]),
            "holdout_net_pnl": _money(holdout_score["net_pnl"]),
            "holdout_win_rate": holdout_score["win_rate"],
            "missed_profit_dollars": _money(holdout_score["missed_profit"]),
            "blocked_loss_dollars": _money(holdout_score["blocked_loss"]),
            "net_improvement_dollars": _money(net_improvement),
            "max_drawdown_proxy": _money(holdout_score["max_drawdown_proxy"]),
            "stale_regression_count": holdout_score["stale_regression_count"],
            "recommendation": recommendation,
        }

    def _score_baseline_gate_policy(
        self,
        policy: BaselineGatePolicy,
        *,
        train: list[GateLearningRow],
        holdout: list[GateLearningRow],
        min_support: int,
    ) -> dict[str, Any]:
        feature_train = [row for row in train if policy.feature_predicate(row)]
        feature_holdout = [row for row in holdout if policy.feature_predicate(row)]
        current_train = _score_rows(feature_train, policy.current_predicate)
        current_holdout = _score_rows(feature_holdout, policy.current_predicate)
        candidate_train = _score_rows(feature_train, policy.predicate)
        candidate_holdout = _score_rows(feature_holdout, policy.predicate)
        support_count = candidate_train["sample_count"] + candidate_holdout["sample_count"]
        holdout_support = candidate_holdout["sample_count"]
        net_improvement = candidate_holdout["net_pnl"] - current_holdout["net_pnl"]
        drawdown_delta = candidate_holdout["max_drawdown_proxy"] - current_holdout["max_drawdown_proxy"]
        promotion_status = self._baseline_gate_promotion_status(
            support_count=support_count,
            holdout_support=holdout_support,
            net_improvement=net_improvement,
            drawdown_delta=drawdown_delta,
            min_support=min_support,
        )
        return {
            "config_field": policy.config_field,
            "gate": policy.gate,
            "candidate_policy": policy.candidate_policy,
            "threshold": self._json_threshold(policy.threshold),
            "field_name": policy.field_name,
            "feature_support_count": len(feature_train) + len(feature_holdout),
            "feature_holdout_count": len(feature_holdout),
            "support_count": support_count,
            "holdout_support": holdout_support,
            "current_train_net_pnl": _money(current_train["net_pnl"]),
            "current_holdout_net_pnl": _money(current_holdout["net_pnl"]),
            "current_holdout_drawdown_proxy": _money(current_holdout["max_drawdown_proxy"]),
            "current_holdout_sample_count": current_holdout["sample_count"],
            "candidate_train_net_pnl": _money(candidate_train["net_pnl"]),
            "candidate_holdout_net_pnl": _money(candidate_holdout["net_pnl"]),
            "candidate_holdout_drawdown_proxy": _money(candidate_holdout["max_drawdown_proxy"]),
            "candidate_holdout_win_rate": candidate_holdout["win_rate"],
            "missed_profit_dollars": _money(candidate_holdout["missed_profit"]),
            "blocked_loss_dollars": _money(candidate_holdout["blocked_loss"]),
            "net_improvement_dollars": _money(net_improvement),
            "drawdown_delta_dollars": _money(drawdown_delta),
            "promotion_status": promotion_status,
        }

    @staticmethod
    def _json_threshold(value: Decimal | int | float) -> int | float:
        if isinstance(value, Decimal):
            return float(value)
        return value

    @staticmethod
    def _recommendation(
        *,
        support_count: int,
        holdout_support: int,
        net_improvement: Decimal,
        stale_regression_count: int,
        min_support: int,
    ) -> str:
        if support_count < min_support or holdout_support < max(3, math.ceil(min_support / 4)):
            return "insufficient_evidence"
        if stale_regression_count > 0 and net_improvement > Decimal("0"):
            return "observe_only_stale_regression"
        if net_improvement > Decimal("0"):
            return "candidate_improves_holdout_net_pnl"
        return "keep_current_gate"

    @staticmethod
    def _baseline_gate_promotion_status(
        *,
        support_count: int,
        holdout_support: int,
        net_improvement: Decimal,
        drawdown_delta: Decimal,
        min_support: int,
    ) -> str:
        if support_count < min_support or holdout_support < max(10, math.ceil(min_support / 4)):
            return "insufficient_evidence"
        if net_improvement <= Decimal("0"):
            return "holdout_net_pnl_not_improved"
        if drawdown_delta > Decimal("0"):
            return "holdout_drawdown_worse"
        return "promoted"

    @staticmethod
    def _example_rows(rows: list[GateLearningRow], *, reverse: bool) -> list[dict[str, Any]]:
        ordered = sorted(
            rows,
            key=lambda row: row.counterfactual_pnl_dollars or Decimal("0"),
            reverse=reverse,
        )
        return [
            {
                "room_id": row.room_id,
                "market_ticker": row.market_ticker,
                "market_day": row.market_day,
                "source": row.source,
                "side": row.side,
                "entry_price": _money(row.entry_price),
                "quality_adjusted_edge_bps": row.quality_adjusted_edge_bps,
                "primary_block_reason": row.primary_block_reason,
                "counterfactual_pnl_dollars": _money(row.counterfactual_pnl_dollars),
            }
            for row in ordered[:20]
        ]

    @staticmethod
    def _confidence_warnings(rows: list[GateLearningRow], holdout: list[GateLearningRow], min_support: int) -> list[str]:
        warnings: list[str] = []
        if len(rows) < min_support:
            warnings.append(f"Only {len(rows)} labeled rows are available; minimum support is {min_support}.")
        if len(holdout) < max(3, min_support // 4):
            warnings.append(f"Only {len(holdout)} holdout rows are available; recommendations should stay read-only.")
        sparse_fields = [
            field for field in ("entry_price", "spread_bps", "confidence", "forecast_delta_f", "quality_adjusted_edge_bps")
            if sum(1 for row in rows if getattr(row, field) is not None) < min_support
        ]
        if sparse_fields:
            warnings.append("Sparse feature coverage: " + ", ".join(sparse_fields) + ".")
        return warnings


def _inferred_settlement_result(row: GateLearningRow) -> str | None:
    if row.settlement_result in {"yes", "no"}:
        return row.settlement_result
    if row.side not in {"yes", "no"} or row.counterfactual_pnl_dollars is None:
        return None
    side_won = row.counterfactual_pnl_dollars > Decimal("0")
    if row.side == "yes":
        return "yes" if side_won else "no"
    return "no" if side_won else "yes"


def _target_yes_price(row: GateLearningRow) -> Decimal | None:
    if row.entry_price is None:
        return None
    if row.side == "yes":
        return row.entry_price
    if row.side == "no":
        return Decimal("1.0000") - row.entry_price
    return None


def _fallback_settlement_ts(row: GateLearningRow) -> datetime | None:
    if row.settlement_ts is not None:
        return row.settlement_ts
    if row.decision_time is not None:
        return row.decision_time + timedelta(days=1)
    return None


def gate_learning_rows_to_backtesting_rows(
    rows: Iterable[GateLearningRow],
    *,
    kalshi_env: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if not row.labeled:
            continue
        target_yes = _target_yes_price(row)
        output.append(
            {
                "row_id": row.room_id or row.market_ticker,
                "source": "gate-learning-bundles",
                "source_ref": None,
                "kalshi_env": kalshi_env,
                "room_id": row.room_id,
                "market_ticker": row.market_ticker,
                "series_ticker": row.series_ticker,
                "city": row.city,
                "market_day": row.market_day,
                "decision_ts": row.decision_time,
                "settlement_ts": _fallback_settlement_ts(row),
                "strategy_code": "weather_baseline",
                "side": row.side,
                "target_yes_price_dollars": target_yes,
                "count_fp": Decimal("1.00"),
                "yes_bid_dollars": None,
                "yes_ask_dollars": None,
                "no_ask_dollars": None,
                "spread_dollars": None,
                "market_stale_seconds": None,
                "market_stale_threshold_seconds": None,
                "fair_yes_dollars": row.fair_yes_dollars,
                "confidence": row.confidence,
                "edge_bps": row.edge_bps,
                "forecast_delta_f": row.forecast_delta_f,
                "entry_price_band": None,
                "forecast_delta_band": row.forecast_delta_band,
                "confidence_band": row.confidence_band,
                "spread_band": row.spread_band,
                "bucket_key": None,
                "settlement_result": _inferred_settlement_result(row),
                "settlement_value_dollars": row.settlement_value_dollars,
                "label_win": row.label_win,
                "actual_net_pnl": None,
                "actual_gross_pnl": None,
                "actual_fees": None,
                "counterfactual_pnl": row.counterfactual_pnl_dollars,
                "leakage_status": "point_in_time",
                "exclusion_reason": None,
                "source_provenance": row.source,
                "decision_status": "selected" if row.current_gate_passed else row.primary_block_reason,
            }
        )
    return output


def format_gate_learning_report(report: dict[str, Any]) -> str:
    counts = report.get("row_counts") or {}
    current = report.get("current_gate_performance") or {}
    lines = [
        "# Gate Learning Report",
        "",
        f"env={report.get('kalshi_env')} source={report.get('source')} window={report.get('window_days')}d read_only={report.get('read_only')}",
        (
            "Rows: "
            f"total={counts.get('total_rows', 0)} "
            f"labeled={counts.get('labeled_rows', 0)} "
            f"train={counts.get('train_rows', 0)} "
            f"holdout={counts.get('holdout_rows', 0)}"
        ),
        (
            "Current gate holdout: "
            f"net={current.get('holdout_net_pnl')} "
            f"samples={current.get('holdout_sample_count')} "
            f"win_rate={current.get('holdout_win_rate')}"
        ),
    ]
    warnings = report.get("confidence_warnings") or []
    if warnings:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {warning}" for warning in warnings)
    lines.extend(["", "## Top Recommendations"])
    for row in (report.get("recommendations") or [])[:10]:
        lines.append(
            "- "
            f"{row.get('gate')} / {row.get('candidate_policy')}: "
            f"recommendation={row.get('recommendation')} "
            f"support={row.get('support_count')} "
            f"holdout_net={row.get('holdout_net_pnl')} "
            f"improvement={row.get('net_improvement_dollars')} "
            f"missed_profit={row.get('missed_profit_dollars')} "
            f"blocked_loss={row.get('blocked_loss_dollars')}"
        )
    if report.get("regret_rows"):
        lines.extend(["", "## Regret Rows"])
        for row in report["regret_rows"][:5]:
            lines.append(
                f"- {row.get('market_ticker')} {row.get('primary_block_reason')}: "
                f"pnl={row.get('counterfactual_pnl_dollars')} qa_edge={row.get('quality_adjusted_edge_bps')}"
            )
    if report.get("saved_loss_rows"):
        lines.extend(["", "## Saved Loss Rows"])
        for row in report["saved_loss_rows"][:5]:
            lines.append(
                f"- {row.get('market_ticker')} {row.get('primary_block_reason')}: "
                f"pnl={row.get('counterfactual_pnl_dollars')} qa_edge={row.get('quality_adjusted_edge_bps')}"
            )
    return "\n".join(lines)


def format_gate_recommendation_report(report: dict[str, Any]) -> str:
    counts = report.get("row_counts") or {}
    lines = [
        "# Gate Learning Recommendations",
        "",
        f"env={report.get('kalshi_env')} source={report.get('source')} window={report.get('window_days')}d read_only={report.get('read_only')}",
        (
            "Rows: "
            f"total={counts.get('total_rows', 0)} "
            f"labeled={counts.get('labeled_rows', 0)} "
            f"train={counts.get('train_rows', 0)} "
            f"holdout={counts.get('holdout_rows', 0)}"
        ),
        "",
        "## Recommended Settings",
    ]
    for field, item in (report.get("recommended_settings") or {}).items():
        marker = "change" if item.get("changed") else "unchanged"
        lines.append(
            f"- {field}: {item.get('current')} -> {item.get('recommended')} "
            f"({marker}; {item.get('reason')})"
        )
    promoted = [row for row in report.get("gate_evidence") or [] if row.get("promotion_status") == "promoted"]
    if promoted:
        lines.extend(["", "## Promoted Evidence"])
        for row in promoted[:10]:
            lines.append(
                "- "
                f"{row.get('config_field')} {row.get('candidate_policy')}: "
                f"support={row.get('support_count')} holdout={row.get('candidate_holdout_net_pnl')} "
                f"improvement={row.get('net_improvement_dollars')} drawdown_delta={row.get('drawdown_delta_dollars')}"
            )
    warnings = report.get("confidence_warnings") or []
    if warnings:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {warning}" for warning in warnings)
    return "\n".join(lines)
