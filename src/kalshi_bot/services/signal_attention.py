from __future__ import annotations

import csv
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


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    return str(value)
