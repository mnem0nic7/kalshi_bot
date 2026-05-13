from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.config import Settings
from kalshi_bot.db.models import Room, Signal
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.fee_model import current_fee_model_version, estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.trade_behavior import series_from_ticker
from kalshi_bot.services.weather_empirical_bootstrap import market_day_from_ticker


ATTENTION_EXPORT_COLUMNS = [
    "pattern",
    "market",
    "n_evaluations",
    "first_seen",
    "last_seen",
    "min_edge_bps",
    "max_edge_bps",
    "last_edge_bps",
    "bucket_id",
    "bucket_sample_count",
    "bucket_min_required",
    "forecast_threshold_delta_f",
    "high_so_far_f",
    "fair_yes_initial",
    "fair_yes_adjusted",
    "fair_yes_value",
    "edge_bps_value",
    "quality_adjusted_edge_bps",
    "time_span_minutes",
    "edge_trajectory",
    "room_ids",
]

REJECTED_WEATHER_SCORE_COLUMNS = [
    "market_ticker",
    "room_id",
    "updated_at",
    "recommended_side",
    "confidence",
    "quality_adjusted_edge_bps",
    "edge_bps",
    "forecast_threshold_delta_f",
    "settlement_result",
    "directional_win",
    "mid_price",
    "ask_price",
    "mid_net_pnl_dollars",
    "ask_net_pnl_dollars",
    "fee_dollars",
    "unlock_tier",
    "bucket_id",
]

WEATHER_POLICY_A_KEY = "weather/a/global/any/any/all_season/all_month/any/any/entry_gate"
REJECTED_WEATHER_REPLAY_VERSION = "rejected-weather-opportunity-v1"


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    numeric = _float_or_none(value)
    return int(numeric) if numeric is not None else None


def _decimal_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return f"{Decimal(str(value)):.4f}"
    except Exception:
        return str(value)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.0001"))
    except Exception:
        return None


def _money(value: Decimal | None) -> str | None:
    return f"{value.quantize(Decimal('0.0001'))}" if value is not None else None


def _iso_or_none(value: datetime | str | None) -> str | None:
    if isinstance(value, datetime):
        return _as_utc(value).isoformat() if _as_utc(value) is not None else None
    return str(value) if value not in (None, "") else None


def _nested(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _candidate_trace(payload: dict[str, Any]) -> dict[str, Any]:
    direct = payload.get("candidate_trace")
    if isinstance(direct, dict):
        return direct
    nested = _nested(payload, "eligibility", "candidate_trace")
    return nested if isinstance(nested, dict) else {}


def _candidate_for_side(trace: dict[str, Any], side: str | None) -> dict[str, Any] | None:
    if side not in {"yes", "no"}:
        return None
    candidate = trace.get(side)
    if isinstance(candidate, dict):
        return candidate
    for item in trace.get("candidates") or []:
        if isinstance(item, dict) and str(item.get("side") or "").lower() == side:
            return item
    return None


def _best_candidate(trace: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [item for item in trace.get("candidates") or [] if isinstance(item, dict)]
    if not candidates:
        candidates = [item for item in (trace.get("yes"), trace.get("no")) if isinstance(item, dict)]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            _int_or_none(item.get("quality_adjusted_edge_bps")) if item.get("quality_adjusted_edge_bps") is not None else -1_000_000,
            _int_or_none(item.get("edge_bps")) if item.get("edge_bps") is not None else -1_000_000,
        ),
    )


def _quotes(payload: dict[str, Any], trace: dict[str, Any]) -> dict[str, str | None]:
    market = _nested(payload, "market_snapshot", "market")
    if not isinstance(market, dict):
        market = payload.get("market_snapshot") if isinstance(payload.get("market_snapshot"), dict) else {}
    yes_bid = _decimal_text(market.get("yes_bid_dollars") or market.get("yes_bid"))
    yes_ask = _decimal_text(market.get("yes_ask_dollars") or market.get("yes_ask"))
    no_ask = _decimal_text(market.get("no_ask_dollars") or market.get("no_ask"))
    no_bid = _decimal_text(market.get("no_bid_dollars") or market.get("no_bid"))
    if no_ask is None and yes_bid is not None:
        no_ask = _decimal_text(Decimal("1.0000") - Decimal(yes_bid))
    if no_bid is None and yes_ask is not None:
        no_bid = _decimal_text(Decimal("1.0000") - Decimal(yes_ask))
    selected_side = str(trace.get("selected_side") or payload.get("recommended_side") or "").lower()
    selected_candidate = _candidate_for_side(trace, selected_side)
    if yes_ask is None:
        yes_ask = _decimal_text(_nested(trace, "yes", "traded_price_dollars"))
    if no_ask is None:
        no_ask = _decimal_text(_nested(trace, "no", "traded_price_dollars"))
    if selected_candidate is not None:
        if selected_side == "yes" and yes_ask is None:
            yes_ask = _decimal_text(selected_candidate.get("traded_price_dollars"))
        if selected_side == "no" and no_ask is None:
            no_ask = _decimal_text(selected_candidate.get("traded_price_dollars"))

    def mid(bid: str | None, ask: str | None) -> str | None:
        if bid is None or ask is None:
            return None
        return _decimal_text((Decimal(bid) + Decimal(ask)) / Decimal("2"))

    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_mid": mid(yes_bid, yes_ask),
        "no_bid": no_bid,
        "no_ask": no_ask,
        "no_mid": mid(no_bid, no_ask),
    }


_CANDIDATE_REASON_TO_GATE = {
    "below_min_edge": "min_edge",
    "below_quality_adjusted_edge": "quality_adjusted_edge",
    "below_min_contract_price": "min_entry_price",
    "insufficient_remaining_payout": "remaining_payout_min",
    "spread_too_wide": "max_spread",
    "longshot_regime": "longshot_regime",
    "missing_quote": "quote_available",
    "eligible": "candidate_marketability",
    "selected_best_quality_adjusted_edge": "candidate_marketability",
}


_STAND_DOWN_TO_PRIMARY = {
    "no_actionable_edge": "min_edge",
    "contract_price_too_low": "min_entry_price",
    "insufficient_forecast_separation": "forecast_too_close_to_threshold",
    "forecast_delta_missing": "forecast_delta_missing",
    "insufficient_remaining_payout": "remaining_payout_min",
    "selected_side_unmarketable": "selected_side_marketability",
    "spread_too_wide": "max_spread",
    "book_effectively_broken": "book_usable",
    "confidence_too_low": "min_confidence",
    "market_stale": "market_stale",
    "research_stale": "research_stale",
    "fair_value_source_disqualified": "fair_value_source_disqualified",
    "static_signal_stale": "static_signal_guard",
}


def _primary_block_reason(stand_down_reason: str | None, trace: dict[str, Any], empirical: dict[str, Any]) -> str | None:
    empirical_reason = str(empirical.get("reason") or "")
    if stand_down_reason == "empirical_gate_block" and empirical_reason:
        return empirical_reason
    if stand_down_reason in _STAND_DOWN_TO_PRIMARY:
        return _STAND_DOWN_TO_PRIMARY[stand_down_reason]
    best = _best_candidate(trace)
    candidate_reason = str((best or {}).get("reason") or "")
    return _CANDIDATE_REASON_TO_GATE.get(candidate_reason) or (stand_down_reason if stand_down_reason else None)


def _gate_lists(primary: str | None, trace: dict[str, Any], empirical: dict[str, Any]) -> tuple[list[str], list[str]]:
    failed: list[str] = []
    passed: list[str] = []
    for item in trace.get("candidates") or []:
        if not isinstance(item, dict):
            continue
        gate = _CANDIDATE_REASON_TO_GATE.get(str(item.get("reason") or ""))
        if not gate:
            continue
        if item.get("status") in {"eligible", "selected"}:
            passed.append(gate)
        else:
            failed.append(gate)
    if empirical:
        reason = str(empirical.get("reason") or "")
        status = str(empirical.get("status") or "")
        if reason == "empirical_gate_passed" or status == "allowed":
            passed.append("empirical_gate")
        elif reason:
            failed.append(reason)
    if primary:
        failed.append(primary)
    return sorted(set(failed)), sorted(set(passed))


def extract_decision_fields(
    payload: dict[str, Any] | None,
    *,
    settings: Settings | None = None,
    room_id: str | None = None,
    market_ticker: str | None = None,
    updated_at: datetime | str | None = None,
    row_fair_yes: Any = None,
    row_edge_bps: Any = None,
    row_confidence: Any = None,
    ticket_side: str | None = None,
    ticket_yes_price_dollars: Any = None,
    ticket_count_fp: Any = None,
) -> dict[str, Any]:
    payload = dict(payload or {})
    trace = _candidate_trace(payload)
    eligibility = payload.get("eligibility") if isinstance(payload.get("eligibility"), dict) else {}
    numeric_facts = _nested(payload, "trader_context", "numeric_facts")
    if not isinstance(numeric_facts, dict):
        numeric_facts = {}
    empirical = payload.get("empirical_gate")
    if not isinstance(empirical, dict):
        empirical = trace.get("empirical_gate") if isinstance(trace.get("empirical_gate"), dict) else {}
    policy_variants = trace.get("policy_variants")
    if not isinstance(policy_variants, dict):
        policy_variants = {}
    weather_policy = trace.get("weather_policy") if isinstance(trace.get("weather_policy"), dict) else {}
    bootstrap = trace.get("weather_empirical_bootstrap") if isinstance(trace.get("weather_empirical_bootstrap"), dict) else {}

    stand_down_reason = (
        payload.get("final_stand_down_reason")
        or payload.get("stand_down_reason")
        or eligibility.get("stand_down_reason")
        or trace.get("eligibility_stand_down_reason")
    )
    stand_down_reason = str(stand_down_reason) if stand_down_reason not in (None, "") else None
    primary = _primary_block_reason(stand_down_reason, trace, empirical)
    gates_failed, gates_passed = _gate_lists(primary, trace, empirical)

    selected_side = str(trace.get("selected_side") or payload.get("recommended_side") or ticket_side or "").lower()
    if selected_side not in {"yes", "no"}:
        selected_side = None
    selected_candidate = _candidate_for_side(trace, selected_side)
    best_candidate = selected_candidate or _best_candidate(trace) or {}
    quotes = _quotes(payload, trace)
    entry_price = (
        _decimal_text(best_candidate.get("traded_price_dollars"))
        or (_decimal_text(ticket_yes_price_dollars) if selected_side == "yes" else None)
        or (_decimal_text(Decimal("1.0000") - Decimal(str(ticket_yes_price_dollars))) if selected_side == "no" and ticket_yes_price_dollars not in (None, "") else None)
    )

    provenance = payload.get("prediction_provenance")
    if not isinstance(provenance, dict):
        provenance = numeric_facts.get("prediction_provenance") if isinstance(numeric_facts.get("prediction_provenance"), dict) else {}
    intraday = provenance.get("intraday_model") if isinstance(provenance.get("intraday_model"), dict) else {}
    fair_initial = (
        intraday.get("baseline_fair_yes")
        or provenance.get("baseline_fair_yes")
        or payload.get("fair_yes_initial")
    )
    fair_adjusted = row_fair_yes if row_fair_yes not in (None, "") else payload.get("fair_yes_dollars")
    operator = numeric_facts.get("operator") or numeric_facts.get("threshold_operator")
    contract_direction = numeric_facts.get("contract_direction")
    if contract_direction not in {"above", "below"} and operator in {">", ">="}:
        contract_direction = "above"
    elif contract_direction not in {"above", "below"} and operator in {"<", "<="}:
        contract_direction = "below"

    min_entry_price = (
        trace.get("min_contract_price_dollars")
        or (settings.risk_min_contract_price_dollars if settings is not None else None)
    )
    quality_buffer_bps = trace.get("quality_buffer_bps")
    if quality_buffer_bps is None and settings is not None:
        quality_buffer_bps = settings.strategy_quality_edge_buffer_bps
    min_separation_f = settings.strategy_min_abs_delta_f if settings is not None else None
    bucket_min_required = settings.trade_behavior_empirical_gate_min_settled_fills if settings is not None else None

    recommended_price = (
        _decimal_text(payload.get("target_yes_price_dollars"))
        or _decimal_text(best_candidate.get("target_yes_price_dollars"))
        or _decimal_text(ticket_yes_price_dollars)
    )
    recommended_size = _decimal_text(ticket_count_fp)
    recommended_size_cap = payload.get("recommended_size_cap_fp")
    model_quality_reasons = payload.get("model_quality_reasons") if isinstance(payload.get("model_quality_reasons"), list) else []
    updated_iso = _iso_or_none(updated_at)

    market_snapshot = payload.get("market_snapshot") if isinstance(payload.get("market_snapshot"), dict) else {}
    market = market_snapshot.get("market") if isinstance(market_snapshot.get("market"), dict) else market_snapshot
    market_close_time = market.get("close_time") if isinstance(market, dict) else None
    time_to_close_minutes = None
    if market_close_time and updated_iso:
        try:
            close_dt = datetime.fromisoformat(str(market_close_time).replace("Z", "+00:00"))
            updated_dt = datetime.fromisoformat(updated_iso.replace("Z", "+00:00"))
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=UTC)
            if updated_dt.tzinfo is None:
                updated_dt = updated_dt.replace(tzinfo=UTC)
            time_to_close_minutes = round((close_dt.astimezone(UTC) - updated_dt.astimezone(UTC)).total_seconds() / 60, 2)
        except Exception:
            time_to_close_minutes = None

    return {
        "room_id": room_id,
        "market_ticker": market_ticker,
        "updated_at": updated_iso,
        "edge_bps": _int_or_none(best_candidate.get("edge_bps")) if best_candidate.get("edge_bps") is not None else _int_or_none(row_edge_bps),
        "quality_adjusted_edge_bps": (
            _int_or_none(eligibility.get("edge_after_quality_buffer_bps"))
            if eligibility.get("edge_after_quality_buffer_bps") is not None
            else _int_or_none(best_candidate.get("quality_adjusted_edge_bps"))
        ),
        "fair_yes_adjusted": _decimal_text(fair_adjusted),
        "confidence": _float_or_none(row_confidence if row_confidence not in (None, "") else payload.get("confidence")),
        "gates_failed": gates_failed,
        "gates_passed": gates_passed,
        "primary_block_reason": primary,
        "market_stale": bool(eligibility.get("market_stale")),
        "research_stale": bool(eligibility.get("research_stale")),
        "policy_variants": policy_variants,
        "policy_variant_applied": trace.get("policy_variant_applied"),
        "baseline_block_reason": trace.get("baseline_block_reason"),
        "bootstrap_tier": bootstrap.get("tier"),
        "bootstrap_outcome": bootstrap.get("outcome"),
        "bootstrap_reason": bootstrap.get("reason"),
        "bootstrap_size_factor": bootstrap.get("size_factor"),
        "bootstrap_confidence_source": bootstrap.get("confidence_source"),
        "bootstrap_fair_value_source": bootstrap.get("fair_value_source"),
        "bootstrap_cap_applied": bootstrap.get("daily_notional_cap_dollars"),
        "bootstrap_evidence_source": bootstrap.get("evidence_source"),
        "bootstrap_lineage_id": bootstrap.get("lineage_id"),
        "bootstrap_policy_key": bootstrap.get("policy_key"),
        "bootstrap_rollout_state": bootstrap.get("rollout_state"),
        "active_policy_pack_version": trace.get("active_policy_pack_version") or weather_policy.get("active_policy_pack_version"),
        "policy_key": trace.get("policy_key") or weather_policy.get("policy_key"),
        "fallback_policy_key_used": trace.get("fallback_policy_key_used") or weather_policy.get("fallback_policy_key_used"),
        "gate_thresholds_used": weather_policy.get("gate_thresholds_used"),
        "capital_thresholds_used": weather_policy.get("capital_thresholds_used"),
        "model_policy_version": weather_policy.get("model_policy_version"),
        "binding_policy_lane": trace.get("binding_policy_lane") or weather_policy.get("binding_policy_lane"),
        "policy_disagreement": trace.get("policy_disagreement") if trace.get("policy_disagreement") is not None else weather_policy.get("policy_disagreement"),
        "counterfactual_parent_policy_decision": weather_policy.get("counterfactual_parent_policy_decision"),
        "why_allowed_or_blocked": weather_policy.get("deterministic_summary"),
        "forecast_high_f": _float_or_none(numeric_facts.get("forecast_high_f")),
        "threshold_f": _float_or_none(numeric_facts.get("threshold_f")),
        "forecast_threshold_delta_f": _float_or_none(numeric_facts.get("forecast_delta_f")),
        "high_so_far_f": _float_or_none(numeric_facts.get("observed_high_so_far_f")),
        "contract_direction": contract_direction,
        "min_separation_f": _float_or_none(min_separation_f),
        **quotes,
        "entry_price": entry_price,
        "min_entry_price": _decimal_text(min_entry_price),
        "quality_buffer_bps": _int_or_none(quality_buffer_bps),
        "fair_yes_initial": _decimal_text(fair_initial),
        "bucket_id": empirical.get("bucket_key"),
        "bucket_sample_count": _int_or_none(empirical.get("actual_sample_count")),
        "bucket_min_required": bucket_min_required,
        "bucket_recommended_size": _decimal_text(empirical.get("recommended_size")),
        "recommended_side": selected_side.upper() if selected_side else None,
        "recommended_price": recommended_price,
        "recommended_size": recommended_size,
        "recommended_size_cap": _decimal_text(recommended_size_cap),
        "recommended_size_cap_reason": "; ".join(str(item) for item in model_quality_reasons) if model_quality_reasons else None,
        "trade_regime": str(payload.get("trade_regime") or trace.get("trade_regime") or "") or None,
        "market_close_time": str(market_close_time) if market_close_time not in (None, "") else None,
        "time_to_close_minutes": time_to_close_minutes,
        "evaluation_n": None,
        "prev_room_id": None,
        "edge_delta_from_prev_bps": None,
    }


def add_lifecycle_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        market = str(row.get("market_ticker") or "")
        if market:
            grouped[market].append(row)
    for market_rows in grouped.values():
        market_rows.sort(key=lambda item: str(item.get("updated_at") or ""))
        prev: dict[str, Any] | None = None
        for index, row in enumerate(market_rows, start=1):
            row["evaluation_n"] = index
            if prev is not None:
                row["prev_room_id"] = prev.get("room_id")
                edge = _int_or_none(row.get("edge_bps"))
                prev_edge = _int_or_none(prev.get("edge_bps"))
                row["edge_delta_from_prev_bps"] = edge - prev_edge if edge is not None and prev_edge is not None else None
            prev = row
    return rows


@dataclass(slots=True)
class SignalAttentionService:
    settings: Settings

    async def load_rows(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        lookback_hours: int | None = None,
        market_ticker: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        hours = lookback_hours if lookback_hours is not None else self.settings.signals_attention_lookback_hours
        cutoff = now - timedelta(hours=max(1, int(hours)))
        stmt = (
            select(Signal, Room)
            .join(Room, Signal.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env, Signal.updated_at >= cutoff)
            .order_by(Signal.updated_at.asc(), Signal.id.asc())
        )
        if market_ticker:
            stmt = stmt.where(Signal.market_ticker == market_ticker)
        else:
            stmt = stmt.where(Signal.market_ticker.startswith("KXHIGH"))
        if limit:
            stmt = stmt.limit(limit)
        result = await session.execute(stmt)
        rows = [
            extract_decision_fields(
                signal.payload,
                settings=self.settings,
                room_id=room.id,
                market_ticker=signal.market_ticker,
                updated_at=signal.updated_at,
                row_fair_yes=signal.fair_yes_dollars,
                row_edge_bps=signal.edge_bps,
                row_confidence=signal.confidence,
            )
            for signal, room in result.all()
        ]
        return add_lifecycle_fields(rows)

    def detect_patterns(self, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        rows_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            market = str(row.get("market_ticker") or "")
            if market:
                rows_by_market[market].append(row)
        flagged: list[dict[str, Any]] = []
        for market, market_rows in rows_by_market.items():
            ordered = sorted(market_rows, key=lambda item: str(item.get("updated_at") or ""))
            flagged.extend(self._cold_start_trapped(market, ordered))
            flagged.extend(self._edge_growing(market, ordered))
            flagged.extend(self._intraday_resolved(market, ordered))
            flagged.extend(self._static_fair(market, ordered))
            flagged.extend(self._static_edge(market, ordered))
            flagged.extend(self._near_miss_edge_floor(market, ordered))
        return flagged

    def _base_row(self, pattern: str, market: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        edges = [_int_or_none(row.get("edge_bps")) for row in rows]
        edges = [edge for edge in edges if edge is not None]
        first_seen = rows[0].get("updated_at") if rows else None
        last_seen = rows[-1].get("updated_at") if rows else None
        return {
            "pattern": pattern,
            "market": market,
            "n_evaluations": len(rows),
            "first_seen": first_seen,
            "last_seen": last_seen,
            "min_edge_bps": min(edges) if edges else None,
            "max_edge_bps": max(edges) if edges else None,
            "last_edge_bps": edges[-1] if edges else None,
            "bucket_id": rows[-1].get("bucket_id") if rows else None,
            "bucket_sample_count": rows[-1].get("bucket_sample_count") if rows else None,
            "bucket_min_required": rows[-1].get("bucket_min_required") if rows else None,
            "room_ids": [row.get("room_id") for row in rows],
        }

    def _cold_start_trapped(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        matching = [
            row for row in rows
            if row.get("primary_block_reason") == "empirical_gate_under_sampled"
            and (_int_or_none(row.get("edge_bps")) or -1_000_000) >= self.settings.empirical_bootstrap_min_edge_bps
        ]
        if len(matching) < self.settings.empirical_bootstrap_min_evaluations:
            return []
        return [self._base_row("cold_start_trapped", market, matching)]

    def _edge_growing(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        values = [(row, _int_or_none(row.get("edge_bps"))) for row in rows]
        values = [(row, edge) for row, edge in values if edge is not None]
        if len(values) < 3:
            return []
        regressions = sum(1 for (_, prev), (_, current) in zip(values, values[1:], strict=False) if current < prev)
        first = values[0][1]
        last = values[-1][1]
        if regressions > 1 or (last < first * 2 and last < self.settings.empirical_bootstrap_last_edge_bps):
            return []
        row = self._base_row("edge_growing", market, [item[0] for item in values])
        row["edge_trajectory"] = [
            {"updated_at": item[0].get("updated_at"), "edge_bps": item[1]}
            for item in values
        ]
        return [row]

    def _intraday_resolved(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        low = self.settings.intraday_resolved_low_fair_yes
        high = self.settings.intraday_resolved_high_fair_yes
        matching = [
            row for row in rows
            if row.get("primary_block_reason") == "forecast_too_close_to_threshold"
            and (fair := _float_or_none(row.get("fair_yes_adjusted"))) is not None
            and (fair <= low or fair >= high)
        ]
        if not matching:
            return []
        latest = matching[-1]
        row = self._base_row("intraday_resolved_separation_block", market, matching)
        row.update(
            {
                "forecast_threshold_delta_f": latest.get("forecast_threshold_delta_f"),
                "high_so_far_f": latest.get("high_so_far_f"),
                "fair_yes_initial": latest.get("fair_yes_initial"),
                "fair_yes_adjusted": latest.get("fair_yes_adjusted"),
            }
        )
        return [row]

    def _static_fair(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        run, value = _latest_static_run(rows, "fair_yes_adjusted", lambda raw: _decimal_text(raw))
        if len(run) < self.settings.static_fair_min_evaluations or value is None:
            return []
        row = self._base_row("static_fair_value", market, run)
        row["fair_yes_value"] = value
        row["time_span_minutes"] = _time_span_minutes(run)
        return [row]

    def _static_edge(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        run, value = _latest_static_run(rows, "edge_bps", _int_or_none)
        if len(run) < self.settings.static_edge_min_evaluations or value is None:
            return []
        row = self._base_row("static_edge", market, run)
        row["edge_bps_value"] = value
        row["time_span_minutes"] = _time_span_minutes(run)
        return [row]

    def _near_miss_edge_floor(self, market: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        floor = int(self.settings.risk_min_edge_bps)
        matching = [
            row for row in rows
            if row.get("primary_block_reason") == "min_edge"
            and (edge := _int_or_none(row.get("quality_adjusted_edge_bps"))) is not None
            and floor - 100 <= edge < floor
        ]
        if len(matching) < 3:
            return []
        row = self._base_row("recurring_near_miss_edge_floor", market, matching)
        row["quality_adjusted_edge_bps"] = matching[-1].get("quality_adjusted_edge_bps")
        return [row]

    def empirical_bootstrap_override(self, rows: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
        patterns = self.detect_patterns(rows)
        cold = next((row for row in patterns if row["pattern"] == "cold_start_trapped"), None)
        if cold is None:
            return None
        edge = _int_or_none(cold.get("last_edge_bps"))
        if edge is not None and edge >= self.settings.empirical_bootstrap_last_edge_bps:
            return cold
        growing = next((row for row in patterns if row["pattern"] == "edge_growing"), None)
        return growing or None

    def static_signal_guard(self, rows: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
        row_list = list(rows)
        rows_by_id = {row.get("room_id"): row for row in row_list if row.get("room_id")}
        patterns = self.detect_patterns(row_list)
        pattern = next(
            (row for row in patterns if row["pattern"] in {"static_edge", "static_fair_value"}),
            None,
        )
        if pattern is None:
            return None
        pattern_rows = [rows_by_id.get(room_id) for room_id in pattern.get("room_ids") or []]
        stale = [
            row for row in pattern_rows
            if isinstance(row, dict)
            and (
                row.get("market_stale") is True
                or row.get("research_stale") is True
                or row.get("primary_block_reason") in {"market_stale", "research_stale"}
            )
        ]
        if not stale:
            return None
        pattern["stale"] = True
        pattern["stale_room_ids"] = [row.get("room_id") for row in stale if row.get("room_id")]
        return pattern

    def room_creation_backoff(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        current_mid_dollars: Decimal | None = None,
        current_edge_bps: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if not bool(getattr(self.settings, "weather_static_signal_backoff_enabled", True)):
            return None
        row_list = sorted(list(rows), key=lambda item: str(item.get("updated_at") or ""))
        if not row_list:
            return None
        now_utc = _as_utc(now) or datetime.now(UTC)
        latest = row_list[-1]
        final_minutes = max(0, int(getattr(self.settings, "weather_static_signal_backoff_final_minutes", 90)))
        time_to_close = _float_or_none(latest.get("time_to_close_minutes"))
        if time_to_close is not None and 0 <= time_to_close <= final_minutes:
            return None

        patterns = [
            pattern for pattern in self.detect_patterns(row_list)
            if pattern.get("pattern") in {"static_edge", "static_fair_value", "edge_growing"}
        ]
        if not patterns:
            return None

        min_evals = max(1, int(getattr(self.settings, "weather_static_signal_backoff_min_evaluations", 3)))
        window_minutes = max(1, int(getattr(self.settings, "weather_static_signal_backoff_window_minutes", 30)))
        cooldown_seconds = max(1, int(getattr(self.settings, "weather_static_signal_backoff_cooldown_seconds", 1800)))
        price_move = Decimal(str(getattr(self.settings, "weather_static_signal_backoff_price_move_dollars", 0.05)))
        edge_move = int(getattr(self.settings, "weather_static_signal_backoff_edge_move_bps", 500))
        rows_by_id = {row.get("room_id"): row for row in row_list if row.get("room_id")}

        for pattern in sorted(patterns, key=lambda item: str(item.get("last_seen") or ""), reverse=True):
            pattern_name = str(pattern.get("pattern") or "")
            if int(pattern.get("n_evaluations") or 0) < min_evals:
                continue
            span = _float_or_none(pattern.get("time_span_minutes"))
            if span is not None and span > window_minutes:
                continue
            last_seen = _parse_datetime(pattern.get("last_seen"))
            if last_seen is None or now_utc - last_seen >= timedelta(seconds=cooldown_seconds):
                continue
            pattern_rows = [rows_by_id.get(room_id) for room_id in pattern.get("room_ids") or []]
            pattern_rows = [row for row in pattern_rows if isinstance(row, dict)]
            latest_pattern_row = pattern_rows[-1] if pattern_rows else latest
            last_mid = _decimal_or_none(latest_pattern_row.get("yes_mid"))
            if current_mid_dollars is not None and last_mid is not None and abs(current_mid_dollars - last_mid) >= price_move:
                continue
            last_edge = _int_or_none(latest_pattern_row.get("edge_bps"))
            if current_edge_bps is not None and last_edge is not None and abs(int(current_edge_bps) - last_edge) >= edge_move:
                continue
            return {
                "reason": "edge_growing_cooloff" if pattern_name == "edge_growing" else "static_signal_backoff",
                "pattern": pattern_name,
                "market_ticker": latest.get("market_ticker"),
                "n_evaluations": pattern.get("n_evaluations"),
                "first_seen": pattern.get("first_seen"),
                "last_seen": pattern.get("last_seen"),
                "cooldown_seconds": cooldown_seconds,
                "cooldown_expires_at": (last_seen + timedelta(seconds=cooldown_seconds)).isoformat(),
                "release_conditions": [
                    f"price_move_gte_{price_move}",
                    f"edge_move_gte_{edge_move}bps",
                    f"final_{final_minutes}_minutes",
                ],
            }
        return None

    async def score_rejected_weather_opportunities(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        lookback_hours: int | None = None,
        dedupe: str = "first-qualifying",
        persist_bootstrap_evidence: bool = False,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if dedupe != "first-qualifying":
            raise ValueError("Only dedupe='first-qualifying' is supported")
        now_utc = _as_utc(now) or datetime.now(UTC)
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        rows = await self.load_rows(
            session,
            kalshi_env=kalshi_env,
            lookback_hours=lookback_hours,
        )
        qualifying: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        skipped_reasons: defaultdict[str, int] = defaultdict(int)
        for row in sorted(rows, key=lambda item: str(item.get("updated_at") or "")):
            tier = self._close_strike_score_tier(row)
            if tier is None:
                skipped_reasons["not_close_strike_candidate"] += 1
                continue
            market = str(row.get("market_ticker") or "")
            side = str(row.get("recommended_side") or "").lower()
            key = (market, side)
            if key in seen:
                skipped_reasons["duplicate_market_side"] += 1
                continue
            seen.add(key)
            qualifying.append(await self._score_rejected_weather_row(repo, row, tier=tier))

        settled = [row for row in qualifying if row.get("settlement_result") in {"yes", "no"}]
        ask_scored = [row for row in settled if row.get("ask_net_pnl_dollars") is not None]
        wins = [row for row in ask_scored if row.get("directional_win") is True]
        ask_net = sum(
            ((_decimal_or_none(row.get("ask_net_pnl_dollars")) or Decimal("0")) for row in ask_scored),
            Decimal("0"),
        )
        mid_net = sum(
            ((_decimal_or_none(row.get("mid_net_pnl_dollars")) or Decimal("0")) for row in settled),
            Decimal("0"),
        )
        fees = sum(
            ((_decimal_or_none(row.get("fee_dollars")) or Decimal("0")) for row in ask_scored),
            Decimal("0"),
        )
        settled_count = len(ask_scored)
        accuracy = (len(wins) / settled_count) if settled_count else 0.0
        min_settled = max(1, int(getattr(self.settings, "weather_rejected_opportunity_min_settled", 5)))
        min_accuracy = float(getattr(self.settings, "weather_rejected_opportunity_min_accuracy", 0.60))
        unlock_passed = settled_count >= min_settled and ask_net > Decimal("0") and accuracy >= min_accuracy
        unlock_reasons: list[str] = []
        if settled_count < min_settled:
            unlock_reasons.append("close_strike_probe_evidence_missing")
        if ask_net <= Decimal("0"):
            unlock_reasons.append("ask_net_pnl_not_positive")
        if accuracy < min_accuracy:
            unlock_reasons.append("directional_accuracy_below_min")
        unlock_reasons.append("close_strike_probe_unlocked" if unlock_passed else "close_strike_probe_evidence_failed")

        persistence = {"requested": persist_bootstrap_evidence, "dry_run": dry_run, "created": 0, "existing": 0, "would_create": 0}
        if persist_bootstrap_evidence:
            for row in ask_scored:
                created = await self._persist_rejected_weather_evidence(repo, row, dry_run=dry_run)
                if dry_run and created:
                    persistence["would_create"] += 1
                elif created:
                    persistence["created"] += 1
                else:
                    persistence["existing"] += 1

        report = {
            "schema": "rejected-weather-opportunity-score-v1",
            "kalshi_env": kalshi_env,
            "lookback_hours": lookback_hours if lookback_hours is not None else self.settings.signals_attention_lookback_hours,
            "dedupe": dedupe,
            "generated_at": now_utc.isoformat(),
            "candidate_count": len(qualifying),
            "settled_count": settled_count,
            "missing_label_count": sum(1 for row in qualifying if row.get("settlement_result") is None),
            "missing_ask_price_count": sum(1 for row in settled if row.get("ask_net_pnl_dollars") is None),
            "directional_win_count": len(wins),
            "directional_accuracy": round(accuracy, 6),
            "ask_net_pnl_dollars": _money(ask_net),
            "mid_net_pnl_dollars": _money(mid_net),
            "fee_dollars": _money(fees),
            "unlock": {
                "passed": unlock_passed,
                "reason_codes": list(dict.fromkeys(unlock_reasons)),
                "min_settled": min_settled,
                "min_accuracy": min_accuracy,
                "requires_ask_net_pnl_positive": True,
            },
            "persistence": persistence,
            "skipped_reasons": dict(skipped_reasons),
            "opportunities": qualifying,
        }
        if not dry_run:
            await repo.log_ops_event(
                severity="info" if unlock_passed else "warning",
                summary="weather_rejected_opportunity_score",
                source="signals-worth-attention",
                kalshi_env=kalshi_env,
                payload={key: value for key, value in report.items() if key != "opportunities"},
            )
        return report

    def _close_strike_score_tier(self, row: dict[str, Any]) -> dict[str, Any] | None:
        market = str(row.get("market_ticker") or "").upper()
        if not market.startswith("KXHIGH"):
            return None
        side = str(row.get("recommended_side") or "").lower()
        if side not in {"yes", "no"}:
            return None
        primary = str(row.get("primary_block_reason") or "")
        failed = {str(item) for item in row.get("gates_failed") or []}
        if primary not in {"forecast_too_close_to_threshold", "empirical_gate_under_sampled"} and not (
            {"forecast_too_close_to_threshold", "empirical_gate_under_sampled"} & failed
        ):
            return None
        if bool(row.get("market_stale")) or bool(row.get("research_stale")):
            return None
        confidence = _float_or_none(row.get("confidence"))
        edge = _int_or_none(row.get("quality_adjusted_edge_bps")) or _int_or_none(row.get("edge_bps"))
        delta = _float_or_none(row.get("forecast_threshold_delta_f"))
        if confidence is None or edge is None or delta is None:
            return None
        abs_delta = abs(delta)
        tiers = (
            {"name": "conf90_delta2_edge3000", "min_confidence": 0.90, "min_abs_delta_f": 2.0, "min_edge_bps": 3000},
            {"name": "conf75_delta3_edge4000", "min_confidence": 0.75, "min_abs_delta_f": 3.0, "min_edge_bps": 4000},
            {"name": "conf60_delta2_edge3500", "min_confidence": 0.60, "min_abs_delta_f": 2.0, "min_edge_bps": 3500},
        )
        for tier in tiers:
            if confidence >= tier["min_confidence"] and abs_delta >= tier["min_abs_delta_f"] and edge >= tier["min_edge_bps"]:
                return tier
        return None

    async def _score_rejected_weather_row(
        self,
        repo: PlatformRepository,
        row: dict[str, Any],
        *,
        tier: dict[str, Any],
    ) -> dict[str, Any]:
        market = str(row.get("market_ticker") or "")
        side = str(row.get("recommended_side") or "").lower()
        label = await repo.get_historical_settlement_label(market)
        settlement_result = None
        settlement_ts = None
        if label is not None:
            result = str(label.kalshi_result or label.crosscheck_result or "").lower()
            settlement_result = result if result in {"yes", "no"} else None
            settlement_ts = label.settlement_ts.isoformat() if label.settlement_ts is not None else None
        mid_price = _side_price(row, side, kind="mid")
        ask_price = _side_price(row, side, kind="ask")
        mid_sim = self._simulate_binary_pnl(side=side, price=mid_price, settlement_result=settlement_result)
        ask_sim = self._simulate_binary_pnl(side=side, price=ask_price, settlement_result=settlement_result)
        return {
            "market_ticker": market,
            "room_id": row.get("room_id"),
            "updated_at": row.get("updated_at"),
            "recommended_side": side.upper() if side else None,
            "confidence": row.get("confidence"),
            "quality_adjusted_edge_bps": row.get("quality_adjusted_edge_bps"),
            "edge_bps": row.get("edge_bps"),
            "forecast_threshold_delta_f": row.get("forecast_threshold_delta_f"),
            "primary_block_reason": row.get("primary_block_reason"),
            "gates_failed": row.get("gates_failed") or [],
            "settlement_result": settlement_result,
            "settlement_ts": settlement_ts,
            "directional_win": (side == settlement_result) if settlement_result in {"yes", "no"} and side in {"yes", "no"} else None,
            "mid_price": _money(mid_price),
            "ask_price": _money(ask_price),
            "mid_gross_pnl_dollars": _money(mid_sim["gross"]),
            "mid_net_pnl_dollars": _money(mid_sim["net"]),
            "ask_gross_pnl_dollars": _money(ask_sim["gross"]),
            "ask_net_pnl_dollars": _money(ask_sim["net"]),
            "fee_dollars": _money(ask_sim["fee"]),
            "fee_model_version": current_fee_model_version() if ask_sim["fee"] is not None else None,
            "unlock_tier": tier["name"],
            "bucket_id": row.get("bucket_id"),
            "policy_key": WEATHER_POLICY_A_KEY,
            "source_fingerprint": self._evidence_fingerprint(row, side=side, policy_key=WEATHER_POLICY_A_KEY),
            "trade_regime": row.get("trade_regime"),
        }

    def _simulate_binary_pnl(
        self,
        *,
        side: str,
        price: Decimal | None,
        settlement_result: str | None,
    ) -> dict[str, Decimal | None]:
        if price is None or settlement_result not in {"yes", "no"} or side not in {"yes", "no"}:
            return {"gross": None, "fee": None, "net": None}
        payout = Decimal("1.0000") if side == settlement_result else Decimal("0.0000")
        gross = (payout - price).quantize(Decimal("0.0001"))
        fee = estimate_kalshi_taker_fee_dollars(
            price_dollars=price,
            count=Decimal("1.00"),
            fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
        ).quantize(Decimal("0.0001"))
        return {"gross": gross, "fee": fee, "net": (gross - fee).quantize(Decimal("0.0001"))}

    async def _persist_rejected_weather_evidence(
        self,
        repo: PlatformRepository,
        row: dict[str, Any],
        *,
        dry_run: bool,
    ) -> bool:
        pnl = _decimal_or_none(row.get("ask_net_pnl_dollars"))
        if pnl is None:
            return False
        existing = await repo.get_weather_bootstrap_historical_evidence(
            kalshi_env=repo.kalshi_env,
            source_fingerprint=str(row["source_fingerprint"]),
            market_ticker=str(row["market_ticker"]),
            bucket_key=row.get("bucket_id"),
            policy_key=row.get("policy_key"),
        )
        if existing is not None:
            return False
        if dry_run:
            return True
        await repo.save_weather_bootstrap_historical_evidence(
            kalshi_env=repo.kalshi_env,
            market_ticker=str(row["market_ticker"]),
            replay_version=REJECTED_WEATHER_REPLAY_VERSION,
            source_fingerprint=str(row["source_fingerprint"]),
            series_ticker=series_from_ticker(str(row["market_ticker"])),
            local_market_day=market_day_from_ticker(str(row["market_ticker"])),
            bucket_key=row.get("bucket_id"),
            policy_key=row.get("policy_key"),
            tier="cold",
            strict_replay=True,
            side=str(row.get("recommended_side") or "").lower() or None,
            confidence=_float_or_none(row.get("confidence")),
            edge_bps=_int_or_none(row.get("quality_adjusted_edge_bps")) or _int_or_none(row.get("edge_bps")),
            count_fp=Decimal("1.00"),
            notional_dollars=_decimal_or_none(row.get("ask_price")),
            pnl_dollars=pnl,
            evidence_weight=1.0,
            outcome="win" if row.get("directional_win") is True else "loss",
            observed_at=_parse_datetime(row.get("updated_at")) or datetime.now(UTC),
            payload={
                "source": "rejected_weather_opportunity_score",
                "room_id": row.get("room_id"),
                "settlement_result": row.get("settlement_result"),
                "ask_price": row.get("ask_price"),
                "mid_price": row.get("mid_price"),
                "fee_dollars": row.get("fee_dollars"),
                "fee_model_version": row.get("fee_model_version"),
                "unlock_tier": row.get("unlock_tier"),
                "fair_value_source": "intraday_model",
            },
        )
        return True

    @staticmethod
    def _evidence_fingerprint(row: dict[str, Any], *, side: str, policy_key: str) -> str:
        payload = {
            "schema": REJECTED_WEATHER_REPLAY_VERSION,
            "market_ticker": row.get("market_ticker"),
            "room_id": row.get("room_id"),
            "updated_at": row.get("updated_at"),
            "side": side,
            "policy_key": policy_key,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:32]


def _time_span_minutes(rows: list[dict[str, Any]]) -> float | None:
    if len(rows) < 2:
        return None
    try:
        first = datetime.fromisoformat(str(rows[0].get("updated_at")).replace("Z", "+00:00"))
        last = datetime.fromisoformat(str(rows[-1].get("updated_at")).replace("Z", "+00:00"))
        if first.tzinfo is None:
            first = first.replace(tzinfo=UTC)
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        return round((last.astimezone(UTC) - first.astimezone(UTC)).total_seconds() / 60, 2)
    except Exception:
        return None


def _parse_datetime(value: Any) -> datetime | None:
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


def _side_price(row: dict[str, Any], side: str, *, kind: str) -> Decimal | None:
    if side == "yes":
        return _decimal_or_none(row.get(f"yes_{kind}"))
    if side == "no":
        return _decimal_or_none(row.get(f"no_{kind}"))
    return None


def _latest_static_run(
    rows: list[dict[str, Any]],
    key: str,
    normalise: Callable[[Any], Any],
) -> tuple[list[dict[str, Any]], Any]:
    if not rows:
        return [], None
    latest_value = normalise(rows[-1].get(key))
    if latest_value is None:
        return [], None
    run: list[dict[str, Any]] = []
    for row in reversed(rows):
        if normalise(row.get(key)) != latest_value:
            break
        run.append(row)
    return list(reversed(run)), latest_value


def attention_rows_to_csv(rows: Iterable[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=ATTENTION_EXPORT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_value(row.get(key)) for key in ATTENTION_EXPORT_COLUMNS})
    return output.getvalue()


def rejected_weather_score_rows_to_csv(rows: Iterable[dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=REJECTED_WEATHER_SCORE_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_value(row.get(key)) for key in REJECTED_WEATHER_SCORE_COLUMNS})
    return output.getvalue()


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    return str(value)
