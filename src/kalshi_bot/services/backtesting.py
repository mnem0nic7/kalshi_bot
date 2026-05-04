from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import OrderRecord, Room, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.fee_model import current_fee_model_version, estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.market_snapshot_archive import (
    DAEMON_MARKET_PRICE_SOURCE_KIND,
    DECISION_SIGNAL_MARKET_SOURCE_KIND,
    TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND,
)
from kalshi_bot.services.trade_behavior import production_entry_freeze_enabled
from kalshi_bot.services.trade_behavior_quality import bucket_evidence_status


SCHEMA_VERSION = "backtesting-report-v1"
SIMULATOR_VERSION = "canonical-taker-simulator-v1"
WALK_FORWARD_VERSION = "walk-forward-market-day-v1"
POINT_IN_TIME_DECISION_CORPUS_SOURCES = {
    "historical_replay_full_checkpoint",
    "historical_replay_partial_checkpoint",
    "historical_replay_late_only",
}
HIGH_LEAKAGE_TRADE_ANALYSIS_SOURCES = {
    "trade_analysis_backfill_final_market",
    "execution_touch",
    "final_market",
}
POINT_IN_TIME_TRADE_ANALYSIS_SOURCES = {
    DECISION_SIGNAL_MARKET_SOURCE_KIND,
    DAEMON_MARKET_PRICE_SOURCE_KIND,
    TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND,
    "trade_analysis_backfill_room_artifact",
    "trade_analysis_backfill_signal_payload",
    "trade_analysis_backfill_market_price_history",
    "trade_analysis_backfill_historical_snapshot",
}
LIVE_BUY_TICKET_STATUSES = {"approved", "submitted", "resting", "filled", "executed"}
LIVE_BUY_ORDER_STATUSES = {"accepted", "open", "submitted", "resting", "filled", "executed"}
DEFAULT_MIN_TRAIN_ROWS = 20
DEFAULT_MIN_TEST_ROWS = 1


def _as_utc(value: Any | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _date_key(value: Any | None) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _as_utc(value).date().isoformat() if _as_utc(value) is not None else None
    text = str(value)
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    parsed = _as_utc(value)
    return parsed.date().isoformat() if parsed is not None else None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, InvalidOperation, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _money(value: Any) -> str | None:
    decimal = _decimal_or_none(value)
    return str(decimal.quantize(Decimal("0.0001"))) if decimal is not None else None


def _rate(value: float | None) -> float | None:
    return round(value, 6) if value is not None and math.isfinite(value) else None


def _clip_probability(value: float) -> float:
    return min(0.999, max(0.001, value))


def _sigmoid(value: float) -> float:
    if value >= 35:
        return 1.0
    if value <= -35:
        return 0.0
    return 1.0 / (1.0 + math.exp(-value))


def _logit(value: float) -> float:
    clipped = _clip_probability(value)
    return math.log(clipped / (1.0 - clipped))


def _baseline_probability(row: dict[str, Any]) -> float | None:
    fair = _float_or_none(row.get("fair_yes_dollars"))
    return _clip_probability(fair) if fair is not None else None


def _yes_label(row: dict[str, Any]) -> int | None:
    result = str(row.get("settlement_result") or row.get("kalshi_result") or "").lower()
    if result == "yes":
        return 1
    if result == "no":
        return 0
    settlement_value = _decimal_or_none(row.get("settlement_value_dollars"))
    if settlement_value is not None:
        return 1 if settlement_value >= Decimal("0.5") else 0
    return None


def _win_label(row: dict[str, Any]) -> int | None:
    value = row.get("label_win")
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "true":
            return 1
        if lowered == "false":
            return 0
    if value in {True, False}:
        return 1 if value else 0
    side = str(row.get("side") or "").lower()
    yes = _yes_label(row)
    if side == "yes" and yes is not None:
        return yes
    if side == "no" and yes is not None:
        return 1 - yes
    return None


def _side_cost_from_yes_price(side: str | None, yes_price: Decimal | None) -> Decimal | None:
    if yes_price is None:
        return None
    side_value = str(side or "").lower()
    if side_value == "yes":
        return yes_price
    if side_value == "no":
        return Decimal("1") - yes_price
    return None


def _series_from_ticker(ticker: str | None) -> str | None:
    if not ticker:
        return None
    return str(ticker).split("-", 1)[0]


def _station_from_series(series: str | None) -> str | None:
    if not series:
        return None
    if series.startswith("KXHIGH"):
        return series.removeprefix("KXHIGH") or None
    return series


def _snapshot_source(row: dict[str, Any]) -> str | None:
    provenance = row.get("snapshot_provenance")
    if isinstance(provenance, dict):
        for key in ("source", "source_kind"):
            if provenance.get(key):
                return str(provenance[key])
    return (
        str(row.get("market_snapshot_source") or row.get("market_snapshot_source_kind"))
        if row.get("market_snapshot_source") or row.get("market_snapshot_source_kind")
        else None
    )


def _trade_analysis_leakage_status(row: dict[str, Any]) -> tuple[str, str | None]:
    training_eligible = row.get("training_eligible") is True
    reason = row.get("training_exclusion_reason")
    source = _snapshot_source(row)
    if source in HIGH_LEAKAGE_TRADE_ANALYSIS_SOURCES or reason == "market_snapshot_high_leakage":
        return "high_leakage", "market_snapshot_high_leakage"
    if reason == "missing_market_snapshot" or "missing_market_snapshot" in list(row.get("exclusion_reasons") or []):
        return "missing_point_in_time_snapshot", "missing_market_snapshot"
    if not training_eligible:
        return "excluded", str(reason or "not_training_eligible")
    return "point_in_time", None


def _decision_corpus_leakage_status(row: Any) -> tuple[str, str | None]:
    source = str(getattr(row, "source_provenance", None) or "")
    if source in POINT_IN_TIME_DECISION_CORPUS_SOURCES:
        return "point_in_time", None
    return "high_leakage", f"source_provenance:{source or 'unknown'}"


def normalize_trade_analysis_row(row: dict[str, Any]) -> dict[str, Any]:
    leakage_status, exclusion_reason = _trade_analysis_leakage_status(row)
    series = row.get("series_ticker") or _series_from_ticker(row.get("market_ticker"))
    market_day = row.get("market_day") or _date_key(row.get("decision_ts"))
    actual_net = _decimal_or_none(row.get("lifecycle_net_pnl") or row.get("lifecycle_net_pnl_dollars"))
    return {
        "row_id": row.get("signal_id") or row.get("room_id") or row.get("market_ticker"),
        "source": "trade-analysis",
        "source_ref": row,
        "kalshi_env": row.get("kalshi_env"),
        "room_id": row.get("room_id"),
        "market_ticker": row.get("market_ticker"),
        "series_ticker": series,
        "city": row.get("city") or row.get("bucket_station") or _station_from_series(series),
        "market_day": market_day,
        "decision_ts": _as_utc(row.get("decision_ts")),
        "settlement_ts": _as_utc(row.get("settlement_ts")),
        "strategy_code": row.get("strategy_code"),
        "side": str(row.get("side") or "").lower() or None,
        "target_yes_price_dollars": _decimal_or_none(row.get("ticket_yes_price_dollars")),
        "count_fp": _decimal_or_none(row.get("ticket_count_fp")) or _decimal_or_none(row.get("filled_contracts")) or Decimal("1.00"),
        "yes_bid_dollars": _decimal_or_none(row.get("yes_bid_dollars")),
        "yes_ask_dollars": _decimal_or_none(row.get("yes_ask_dollars")),
        "no_ask_dollars": _decimal_or_none(row.get("no_ask_dollars")),
        "spread_dollars": _decimal_or_none(row.get("spread_dollars")),
        "market_stale_seconds": _float_or_none(row.get("market_stale_seconds")),
        "market_stale_threshold_seconds": _float_or_none(row.get("market_stale_threshold_seconds")),
        "fair_yes_dollars": _decimal_or_none(row.get("fair_yes_dollars")),
        "confidence": _float_or_none(row.get("confidence")),
        "edge_bps": _int_or_none(row.get("edge_bps")),
        "forecast_delta_f": _float_or_none(row.get("forecast_delta_f")),
        "entry_price_band": row.get("entry_price_band"),
        "forecast_delta_band": row.get("forecast_delta_band"),
        "confidence_band": row.get("confidence_band"),
        "spread_band": row.get("spread_band"),
        "bucket_key": row.get("bucket_key"),
        "settlement_result": row.get("kalshi_result"),
        "settlement_value_dollars": _decimal_or_none(row.get("settlement_value_dollars")),
        "label_win": _win_label(row),
        "actual_net_pnl": actual_net,
        "actual_gross_pnl": _decimal_or_none(row.get("gross_pnl_dollars")),
        "actual_fees": _decimal_or_none(row.get("fees") or row.get("fee_dollars")),
        "counterfactual_pnl": actual_net,
        "leakage_status": leakage_status,
        "exclusion_reason": exclusion_reason,
        "source_provenance": _snapshot_source(row),
        "decision_status": row.get("decision_status"),
    }


def normalize_decision_corpus_row(row: Any) -> dict[str, Any]:
    leakage_status, exclusion_reason = _decision_corpus_leakage_status(row)
    quote = dict(getattr(row, "quote_snapshot", None) or {})
    market = quote.get("market") if isinstance(quote.get("market"), dict) else quote
    series = getattr(row, "series_ticker", None) or _series_from_ticker(getattr(row, "market_ticker", None))
    actual_net = _decimal_or_none(getattr(row, "pnl_executed_realized", None))
    counterfactual = _decimal_or_none(getattr(row, "pnl_counterfactual_target_with_fees", None))
    return {
        "row_id": getattr(row, "id", None) or getattr(row, "room_id", None),
        "source": "decision-corpus",
        "source_ref": row,
        "kalshi_env": getattr(row, "kalshi_env", None),
        "room_id": getattr(row, "room_id", None),
        "market_ticker": getattr(row, "market_ticker", None),
        "series_ticker": series,
        "city": _station_from_series(series) or getattr(row, "station_id", None),
        "market_day": getattr(row, "local_market_day", None) or _date_key(getattr(row, "checkpoint_ts", None)),
        "decision_ts": _as_utc(getattr(row, "checkpoint_ts", None)),
        "settlement_ts": _as_utc((getattr(row, "settlement_payload", None) or {}).get("settlement_ts")),
        "strategy_code": getattr(row, "policy_version", None),
        "side": str(getattr(row, "recommended_side", None) or "").lower() or None,
        "target_yes_price_dollars": _decimal_or_none(getattr(row, "target_yes_price_dollars", None)),
        "count_fp": _decimal_or_none(getattr(row, "counterfactual_count", None)) or Decimal("1.00"),
        "yes_bid_dollars": _decimal_or_none(market.get("yes_bid_dollars")),
        "yes_ask_dollars": _decimal_or_none(market.get("yes_ask_dollars")),
        "no_ask_dollars": _decimal_or_none(market.get("no_ask_dollars")),
        "spread_dollars": None,
        "market_stale_seconds": None,
        "market_stale_threshold_seconds": None,
        "fair_yes_dollars": _decimal_or_none(getattr(row, "fair_yes_dollars", None)),
        "confidence": _float_or_none(getattr(row, "confidence", None)),
        "edge_bps": _int_or_none(getattr(row, "edge_bps", None)),
        "forecast_delta_f": _float_or_none((getattr(row, "diagnostics", None) or {}).get("forecast_delta_f")),
        "entry_price_band": None,
        "forecast_delta_band": None,
        "confidence_band": None,
        "spread_band": getattr(row, "liquidity_regime", None),
        "bucket_key": None,
        "settlement_result": getattr(row, "settlement_result", None),
        "settlement_value_dollars": _decimal_or_none(getattr(row, "settlement_value_dollars", None)),
        "label_win": None,
        "actual_net_pnl": actual_net,
        "actual_gross_pnl": None,
        "actual_fees": None,
        "counterfactual_pnl": counterfactual,
        "leakage_status": leakage_status,
        "exclusion_reason": exclusion_reason,
        "source_provenance": getattr(row, "source_provenance", None),
        "decision_status": getattr(row, "eligibility_status", None),
    }


def _no_ask_from_quotes(row: dict[str, Any]) -> Decimal | None:
    no_ask = _decimal_or_none(row.get("no_ask_dollars"))
    if no_ask is not None and no_ask > 0:
        return no_ask
    yes_bid = _decimal_or_none(row.get("yes_bid_dollars"))
    if yes_bid is None or yes_bid <= 0:
        return None
    derived = Decimal("1") - yes_bid
    return derived if derived > 0 else None


def simulate_decision_row(row: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    side = str(row.get("side") or "").lower()
    target_yes = _decimal_or_none(row.get("target_yes_price_dollars"))
    count = _decimal_or_none(row.get("count_fp")) or Decimal("1.00")
    yes_label = _yes_label(row)
    stale_seconds = _float_or_none(row.get("market_stale_seconds"))
    stale_threshold = _float_or_none(row.get("market_stale_threshold_seconds"))
    if row.get("leakage_status") != "point_in_time":
        return {
            "execution_model_status": "diagnostics_only",
            "reason": row.get("exclusion_reason") or row.get("leakage_status"),
            "fillable": False,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
            "fee_model_version": current_fee_model_version(),
        }
    if side not in {"yes", "no"} or target_yes is None:
        return {
            "execution_model_status": "missing_trade_terms",
            "reason": "missing_side_or_target_price",
            "fillable": False,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
            "fee_model_version": current_fee_model_version(),
        }
    if yes_label is None:
        return {
            "execution_model_status": "unsettled",
            "reason": "missing_settlement",
            "fillable": False,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
            "fee_model_version": current_fee_model_version(),
        }
    if stale_seconds is not None and stale_threshold is not None and stale_seconds > stale_threshold:
        return {
            "execution_model_status": "stale_quote",
            "reason": "market_snapshot_stale",
            "fillable": False,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
            "fee_model_version": current_fee_model_version(),
        }

    yes_ask = _decimal_or_none(row.get("yes_ask_dollars"))
    no_ask = _no_ask_from_quotes(row)
    target_cost = _side_cost_from_yes_price(side, target_yes)
    if side == "yes":
        execution_cost = yes_ask if yes_ask is not None and yes_ask > 0 else None
        fillable = execution_cost is not None and target_cost is not None and execution_cost <= target_cost
    else:
        execution_cost = no_ask
        fillable = execution_cost is not None and target_cost is not None and execution_cost <= target_cost
    if not fillable or execution_cost is None or target_cost is None:
        return {
            "execution_model_status": "unfillable",
            "reason": "no_marketable_quote_at_target",
            "fillable": False,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
            "fee_model_version": current_fee_model_version(),
        }

    payoff = Decimal(yes_label) if side == "yes" else Decimal(1 - yes_label)
    gross = (payoff - execution_cost) * count
    fees = estimate_kalshi_taker_fee_dollars(
        price_dollars=execution_cost,
        count=count,
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    return {
        "execution_model_status": "fillable",
        "reason": None,
        "fillable": True,
        "execution_price_dollars": str(execution_cost.quantize(Decimal("0.0001"))),
        "gross_pnl": gross.quantize(Decimal("0.0001")),
        "fees": fees.quantize(Decimal("0.0001")),
        "net_pnl": (gross - fees).quantize(Decimal("0.0001")),
        "fee_model_version": current_fee_model_version(),
    }


def _actual_evidence_status(row: dict[str, Any], simulation: dict[str, Any]) -> str:
    actual = _decimal_or_none(row.get("actual_net_pnl"))
    simulated = _decimal_or_none(simulation.get("net_pnl"))
    counterfactual = _decimal_or_none(row.get("counterfactual_pnl"))
    if actual is not None and actual < 0 and ((simulated is not None and simulated > actual) or (counterfactual is not None and counterfactual > actual)):
        return "negative_actual_overrides_counterfactual"
    if actual is not None:
        return "actual_evidence_available"
    return "counterfactual_only" if simulated is not None or counterfactual is not None else "unscored"


def _effective_pnl(row: dict[str, Any], simulation: dict[str, Any]) -> Decimal | None:
    actual = _decimal_or_none(row.get("actual_net_pnl"))
    if actual is not None and actual < 0:
        return actual
    simulated = _decimal_or_none(simulation.get("net_pnl"))
    if simulated is not None:
        return simulated
    return _decimal_or_none(row.get("counterfactual_pnl"))


def row_diagnostic(row: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    simulation = simulate_decision_row(row, settings=settings)
    actual_status = _actual_evidence_status(row, simulation)
    effective = _effective_pnl(row, simulation)
    return {
        "row_id": row.get("row_id"),
        "source": row.get("source"),
        "market_ticker": row.get("market_ticker"),
        "market_day": row.get("market_day"),
        "bucket_key": row.get("bucket_key"),
        "leakage_status": row.get("leakage_status"),
        "exclusion_reason": row.get("exclusion_reason"),
        "execution_model_status": simulation.get("execution_model_status"),
        "actual_evidence_status": actual_status,
        "counterfactual_pnl": _money(row.get("counterfactual_pnl")),
        "actual_net_pnl": _money(row.get("actual_net_pnl")),
        "simulated_net_pnl": _money(simulation.get("net_pnl")),
        "effective_net_pnl": _money(effective),
        "fillable": bool(simulation.get("fillable")),
    }


def _pnl_values(rows: list[dict[str, Any]], *, settings: Settings) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        effective = _effective_pnl(row, simulate_decision_row(row, settings=settings))
        if effective is not None:
            values.append(effective)
    return values


def _max_drawdown(values: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    max_dd = Decimal("0")
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _population_stdev(values: list[float]) -> float | None:
    if not values:
        return None
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _sortino(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    downside = [value for value in floats if value < 0]
    downside_stdev = math.sqrt(sum(value * value for value in downside) / len(downside)) if downside else 0.0
    return mean / max(downside_stdev, 0.01)


def _sharpe(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    stdev = _population_stdev(floats)
    return mean / max(stdev or 0.0, 0.01)


def _policy_metrics(policy_name: str, rows: list[dict[str, Any]], *, all_rows_count: int, settings: Settings) -> dict[str, Any]:
    diagnostics = [row_diagnostic(row, settings=settings) for row in rows]
    scored = [item for item in diagnostics if _decimal_or_none(item.get("effective_net_pnl")) is not None]
    values = [_decimal_or_none(item["effective_net_pnl"]) or Decimal("0") for item in scored]
    gross_values = []
    fee_values = []
    fillable_count = 0
    wins = 0
    clusters: dict[tuple[str, str | None], Decimal] = defaultdict(lambda: Decimal("0"))
    for row, diagnostic in zip(rows, diagnostics, strict=True):
        simulation = simulate_decision_row(row, settings=settings)
        if simulation.get("fillable"):
            fillable_count += 1
        gross = _decimal_or_none(simulation.get("gross_pnl"))
        fee = _decimal_or_none(simulation.get("fees"))
        if gross is not None:
            gross_values.append(gross)
        if fee is not None:
            fee_values.append(fee)
        effective = _decimal_or_none(diagnostic.get("effective_net_pnl"))
        if effective is not None:
            clusters[(str(row.get("series_ticker") or row.get("city") or "unknown"), row.get("market_day"))] += effective
            if effective > 0:
                wins += 1
    worst_buckets = _worst_buckets(rows, settings=settings)
    return {
        "policy_name": policy_name,
        "selected_count": len(rows),
        "fillable_count": fillable_count,
        "coverage": _rate(len(rows) / all_rows_count) if all_rows_count else None,
        "gross_pnl": _money(sum(gross_values, Decimal("0"))),
        "fees": _money(sum(fee_values, Decimal("0"))),
        "net_pnl": _money(sum(values, Decimal("0"))),
        "max_drawdown": _money(_max_drawdown(values)),
        "sortino": _rate(_sortino(list(clusters.values()))),
        "sharpe": _rate(_sharpe(list(clusters.values()))),
        "win_rate": _rate(wins / len(values)) if values else None,
        "win_rate_display_only": True,
        "cluster_count": len(clusters),
        "worst_buckets": worst_buckets[:10],
    }


def _bucket_key(row: dict[str, Any]) -> str:
    if row.get("bucket_key"):
        return str(row["bucket_key"])
    return "|".join(
        str(row.get(key) or "unknown")
        for key in (
            "series_ticker",
            "city",
            "side",
            "strategy_code",
            "entry_price_band",
            "forecast_delta_band",
            "confidence_band",
            "spread_band",
        )
    )


def _worst_buckets(rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[_bucket_key(row)].append(row)
    output: list[dict[str, Any]] = []
    for key, bucket_rows in buckets.items():
        values = _pnl_values(bucket_rows, settings=settings)
        wins = sum(1 for value in values if value > 0)
        output.append({
            "bucket_key": key,
            "sample_count": len(values),
            "net_pnl": _money(sum(values, Decimal("0"))) if values else None,
            "win_rate": _rate(wins / len(values)) if values else None,
        })
    output.sort(key=lambda item: (_decimal_or_none(item.get("net_pnl")) or Decimal("0"), item["bucket_key"]))
    return output


def _bucket_matrix(rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    matrix: list[dict[str, Any]] = []
    for item in _worst_buckets(rows, settings=settings):
        key = item["bucket_key"]
        sample_rows = [row for row in rows if _bucket_key(row) == key]
        first = sample_rows[0] if sample_rows else {}
        matrix.append({
            **item,
            "env": first.get("kalshi_env"),
            "strategy": first.get("strategy_code"),
            "side": first.get("side"),
            "series_ticker": first.get("series_ticker"),
            "city": first.get("city"),
            "entry_price_band": first.get("entry_price_band"),
            "forecast_delta_band": first.get("forecast_delta_band"),
            "confidence_band": first.get("confidence_band"),
            "spread_band": first.get("spread_band"),
        })
    return matrix


def _fit_platt(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    samples = [(baseline, label) for row in rows if (baseline := _baseline_probability(row)) is not None and (label := _yes_label(row)) is not None]
    if len(samples) < DEFAULT_MIN_TRAIN_ROWS or len({label for _, label in samples}) < 2:
        return None
    xs = [_logit(probability) for probability, _ in samples]
    labels = [label for _, label in samples]
    mean = sum(xs) / len(xs)
    std = math.sqrt(sum((value - mean) ** 2 for value in xs) / max(1, len(xs) - 1)) or 1.0
    weights = [0.0, 0.0]
    for _ in range(800):
        grads = [0.0, 0.0]
        for x_raw, label in zip(xs, labels, strict=True):
            x = (x_raw - mean) / std
            err = _sigmoid(weights[0] + weights[1] * x) - label
            grads[0] += err
            grads[1] += err * x
        weights[0] -= 0.05 * (grads[0] / len(samples))
        weights[1] -= 0.05 * ((grads[1] / len(samples)) + 0.01 * weights[1])
    return {"weights": weights, "mean": mean, "std": std}


def _predict_platt(model: dict[str, Any] | None, baseline: float | None) -> float | None:
    if model is None or baseline is None:
        return baseline
    x = (_logit(baseline) - float(model["mean"])) / (float(model["std"]) or 1.0)
    return _clip_probability(_sigmoid(float(model["weights"][0]) + float(model["weights"][1]) * x))


def _probability_metrics(predictions: list[tuple[float, int]]) -> dict[str, Any]:
    if not predictions:
        return {"sample_count": 0, "brier": None, "log_loss": None, "ece": None}
    eps = 1e-9
    brier = sum((p - y) ** 2 for p, y in predictions) / len(predictions)
    log_loss = -sum(
        y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p))
        for p, y in predictions
    ) / len(predictions)
    bucketed: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for p, y in predictions:
        lo = min(9, int(p * 10)) / 10
        bucketed[f"{lo:.1f}-{lo + 0.1:.1f}"].append((p, y))
    ece = 0.0
    for values in bucketed.values():
        avg_prediction = sum(p for p, _ in values) / len(values)
        observed_rate = sum(y for _, y in values) / len(values)
        ece += (len(values) / len(predictions)) * abs(avg_prediction - observed_rate)
    return {
        "sample_count": len(predictions),
        "brier": _rate(brier),
        "log_loss": _rate(log_loss),
        "ece": _rate(ece),
    }


def _market_day_sort_key(row: dict[str, Any]) -> tuple[str, str]:
    day = str(row.get("market_day") or _date_key(row.get("decision_ts")) or "9999-99-99")
    return day, str(row.get("decision_ts") or "")


def _row_market_day(row: dict[str, Any]) -> str | None:
    value = row.get("market_day") or _date_key(row.get("decision_ts"))
    return str(value) if value else None


def _fold_cutoff_ts(day: str, test_rows: list[dict[str, Any]]) -> datetime:
    decision_times = [_as_utc(row.get("decision_ts")) for row in test_rows]
    decision_times = [value for value in decision_times if value is not None]
    if decision_times:
        return min(decision_times)
    parsed = _as_utc(f"{day}T00:00:00+00:00")
    return parsed or datetime.now(UTC)


def build_walk_forward_folds(
    rows: list[dict[str, Any]],
    *,
    min_train_rows: int = DEFAULT_MIN_TRAIN_ROWS,
    min_test_rows: int = DEFAULT_MIN_TEST_ROWS,
) -> list[dict[str, Any]]:
    eligible = [row for row in rows if row.get("leakage_status") == "point_in_time" and _yes_label(row) is not None]
    eligible.sort(key=_market_day_sort_key)
    days = sorted({day for row in eligible if (day := _row_market_day(row)) is not None})
    folds: list[dict[str, Any]] = []
    for day in days:
        test = [row for row in eligible if _row_market_day(row) == day]
        cutoff_ts = _fold_cutoff_ts(day, test)
        train = [
            row
            for row in eligible
            if (_row_market_day(row) or "") < day
            and (settlement_ts := _as_utc(row.get("settlement_ts"))) is not None
            and settlement_ts <= cutoff_ts
        ]
        if len(train) < min_train_rows or len(test) < min_test_rows:
            continue
        folds.append({
            "fold_id": f"wf-{len(folds) + 1}",
            "train_cutoff_market_day": day,
            "train_cutoff_ts": cutoff_ts.isoformat(),
            "train_rows": train,
            "test_rows": test,
        })
    return folds


def _selected_by_baseline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("leakage_status") == "point_in_time"
        and str(row.get("side") or "").lower() in {"yes", "no"}
        and _decimal_or_none(row.get("target_yes_price_dollars")) is not None
    ]


def _selected_by_calibrated(rows: list[dict[str, Any]], *, model: dict[str, Any] | None) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if row.get("leakage_status") != "point_in_time":
            continue
        side = str(row.get("side") or "").lower()
        if side not in {"yes", "no"}:
            continue
        baseline = _baseline_probability(row)
        probability = _predict_platt(model, baseline)
        if probability is None:
            continue
        target_yes = _decimal_or_none(row.get("target_yes_price_dollars"))
        cost = _side_cost_from_yes_price(side, target_yes)
        if cost is None:
            continue
        edge = (Decimal(str(probability)) if side == "yes" else Decimal("1") - Decimal(str(probability))) - cost
        if edge > Decimal("0"):
            selected.append(row)
    return selected


def _selected_by_bucket_policy(
    test_rows: list[dict[str, Any]],
    train_rows: list[dict[str, Any]],
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in train_rows:
        by_bucket[_bucket_key(row)].append(row)
    selected: list[dict[str, Any]] = []
    min_samples = int(settings.trade_behavior_empirical_gate_min_settled_fills)
    min_net = Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars))
    for row in test_rows:
        train_bucket = by_bucket.get(_bucket_key(row), [])
        values = _pnl_values(train_bucket, settings=settings)
        status = bucket_evidence_status(
            {
                "bucket_sample_count": len(values),
                "bucket_net_pnl": sum(values, Decimal("0")) if values else None,
            },
            min_samples=min_samples,
            min_net_pnl=min_net,
        )
        if status == "eligible_for_live_review":
            selected.append(row)
    return selected


def evaluate_walk_forward_backtest(rows: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any]:
    folds = build_walk_forward_folds(rows)
    if not folds:
        return {
            "status": "insufficient_data",
            "reason": "need_point_in_time_rows_across_market_days_for_walk_forward",
            "fold_count": 0,
            "folds": [],
            "prediction_metrics": {
                "baseline": _probability_metrics([]),
                "calibrated": _probability_metrics([]),
            },
            "baseline_policy": _policy_metrics("baseline_current_rule", [], all_rows_count=len(rows), settings=settings),
            "candidate_policies": [
                _policy_metrics("calibrated_prediction", [], all_rows_count=len(rows), settings=settings),
                _policy_metrics("trade_selection_policy", [], all_rows_count=len(rows), settings=settings),
            ],
        }

    all_test_rows: list[dict[str, Any]] = []
    all_baseline_selected: list[dict[str, Any]] = []
    all_calibrated_selected: list[dict[str, Any]] = []
    all_bucket_selected: list[dict[str, Any]] = []
    baseline_predictions: list[tuple[float, int]] = []
    calibrated_predictions: list[tuple[float, int]] = []
    fold_summaries: list[dict[str, Any]] = []
    for fold in folds:
        train_rows = list(fold["train_rows"])
        test_rows = list(fold["test_rows"])
        model = _fit_platt(train_rows)
        baseline_selected = _selected_by_baseline(test_rows)
        calibrated_selected = _selected_by_calibrated(test_rows, model=model)
        bucket_selected = _selected_by_bucket_policy(test_rows, train_rows, settings=settings)
        for row in test_rows:
            label = _yes_label(row)
            baseline = _baseline_probability(row)
            calibrated = _predict_platt(model, baseline)
            if label is not None and baseline is not None:
                baseline_predictions.append((baseline, label))
            if label is not None and calibrated is not None:
                calibrated_predictions.append((calibrated, label))
        all_test_rows.extend(test_rows)
        all_baseline_selected.extend(baseline_selected)
        all_calibrated_selected.extend(calibrated_selected)
        all_bucket_selected.extend(bucket_selected)
        fold_summaries.append({
            "fold_id": fold["fold_id"],
            "train_cutoff_market_day": fold["train_cutoff_market_day"],
            "train_rows": len(train_rows),
            "test_rows": len(test_rows),
            "baseline_selected_count": len(baseline_selected),
            "calibrated_selected_count": len(calibrated_selected),
            "trade_selection_selected_count": len(bucket_selected),
        })

    return {
        "status": "ok",
        "walk_forward_version": WALK_FORWARD_VERSION,
        "fold_count": len(folds),
        "folds": fold_summaries,
        "prediction_metrics": {
            "baseline": _probability_metrics(baseline_predictions),
            "calibrated": _probability_metrics(calibrated_predictions),
        },
        "baseline_policy": _policy_metrics(
            "baseline_current_rule",
            all_baseline_selected,
            all_rows_count=len(all_test_rows),
            settings=settings,
        ),
        "candidate_policies": [
            _policy_metrics(
                "calibrated_prediction",
                all_calibrated_selected,
                all_rows_count=len(all_test_rows),
                settings=settings,
            ),
            _policy_metrics(
                "trade_selection_policy",
                all_bucket_selected,
                all_rows_count=len(all_test_rows),
                settings=settings,
            ),
        ],
        "test_row_count": len(all_test_rows),
    }


def _data_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    leakage_counts = Counter(str(row.get("leakage_status") or "unknown") for row in rows)
    exclusions = Counter(str(row.get("exclusion_reason") or "none") for row in rows if row.get("exclusion_reason"))
    source_counts = Counter(str(row.get("source") or "unknown") for row in rows)
    snapshot_sources = Counter(str(row.get("source_provenance") or "<missing>") for row in rows)
    return {
        "row_count": len(rows),
        "point_in_time_rows": int(leakage_counts.get("point_in_time", 0)),
        "diagnostics_only_rows": len(rows) - int(leakage_counts.get("point_in_time", 0)),
        "leakage_status_counts": dict(leakage_counts),
        "top_exclusion_reasons": exclusions.most_common(10),
        "source_counts": dict(source_counts),
        "snapshot_source_counts": dict(snapshot_sources),
    }


def _promotion_gates(report: dict[str, Any], *, settings: Settings, kalshi_env: str, full_history: bool) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    data_quality = report.get("data_quality") or {}
    wf = report.get("walk_forward") or {}
    baseline = report.get("baseline_policy") or {}
    candidates = list(report.get("candidate_policies") or [])
    best_candidate_net = max(
        [_decimal_or_none(candidate.get("net_pnl")) or Decimal("-999999") for candidate in candidates],
        default=Decimal("-999999"),
    )
    baseline_net = _decimal_or_none(baseline.get("net_pnl")) or Decimal("0")
    if str(kalshi_env).lower() == "production" and not production_entry_freeze_enabled(settings, kalshi_env):
        failures.append("production_entry_freeze_disabled")
    if int(data_quality.get("point_in_time_rows") or 0) <= 0:
        if full_history:
            failures.append("no_point_in_time_rows")
        else:
            warnings.append("no_point_in_time_rows")
    if wf.get("status") != "ok":
        warnings.append("walk_forward_insufficient_data")
    if best_candidate_net <= baseline_net:
        warnings.append("no_candidate_beats_baseline_net_pnl")
    if full_history and (report.get("decision_corpus") or {}).get("status") != "ok":
        failures.append("current_decision_corpus_missing_for_full_history_validation")
    return {
        "status": "fail" if failures else ("warn" if warnings else "pass"),
        "failures": failures,
        "warnings": warnings,
    }


async def _production_buy_entry_bypass(
    session_factory: async_sessionmaker | None,
    *,
    kalshi_env: str,
    since: datetime,
) -> dict[str, Any]:
    if session_factory is None or str(kalshi_env).lower() != "production":
        return {"ticket_count": 0, "order_count": 0, "tickets": [], "orders": [], "since": since.isoformat()}
    async with session_factory() as session:
        tickets = list(
            (
                await session.execute(
                    select(TradeTicketRecord, Room)
                    .join(Room, TradeTicketRecord.room_id == Room.id)
                    .where(
                        Room.kalshi_env == kalshi_env,
                        Room.shadow_mode.is_(False),
                        TradeTicketRecord.action == "buy",
                        TradeTicketRecord.status.in_(sorted(LIVE_BUY_TICKET_STATUSES)),
                        TradeTicketRecord.created_at >= since,
                    )
                    .limit(20)
                )
            ).all()
        )
        orders = list(
            (
                await session.execute(
                    select(OrderRecord)
                    .where(
                        OrderRecord.kalshi_env == kalshi_env,
                        OrderRecord.action == "buy",
                        OrderRecord.status.in_(sorted(LIVE_BUY_ORDER_STATUSES)),
                        OrderRecord.created_at >= since,
                    )
                    .limit(20)
                )
            ).scalars()
        )
    return {
        "ticket_count": len(tickets),
        "order_count": len(orders),
        "since": since.isoformat(),
        "tickets": [{"ticket_id": ticket.id, "market_ticker": ticket.market_ticker} for ticket, _room in tickets],
        "orders": [{"order_id": order.id, "market_ticker": order.market_ticker} for order in orders],
    }


async def _decision_corpus_rows(
    *,
    session_factory: async_sessionmaker | None,
    kalshi_env: str,
    limit: int | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if session_factory is None:
        return {"status": "missing", "kalshi_env": kalshi_env, "build": None}, []
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=kalshi_env)
        build = await repo.get_current_decision_corpus_build(kalshi_env=kalshi_env)
        if build is None:
            return {"status": "missing", "kalshi_env": kalshi_env, "build": None}, []
        records = await repo.list_decision_corpus_rows(build_id=build.id)
    if limit is not None:
        records = records[:limit]
    return (
        {
            "status": "ok",
            "kalshi_env": kalshi_env,
            "build": {
                "id": build.id,
                "status": build.status,
                "row_count": build.row_count,
                "date_from": build.date_from.isoformat(),
                "date_to": build.date_to.isoformat(),
            },
        },
        [normalize_decision_corpus_row(row) for row in records],
    )


async def _load_rows(
    *,
    dataset_source: str,
    settings: Settings,
    session_factory: async_sessionmaker | None,
    decision_corpus_service: Any,
    trade_analysis_service: Any,
    kalshi_env: str,
    days: int,
    row_limit: int | None,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    corpus = await decision_corpus_service.current(kalshi_env=kalshi_env) if decision_corpus_service is not None else {"status": "missing", "kalshi_env": kalshi_env, "build": None}
    source = dataset_source
    if source == "auto":
        source = "decision-corpus" if corpus.get("status") == "ok" else "trade-analysis"
    if source == "decision-corpus":
        corpus, rows = await _decision_corpus_rows(session_factory=session_factory, kalshi_env=kalshi_env, limit=row_limit)
        dataset = {"source": "decision-corpus", "row_count": len(rows)}
        return dataset, corpus, rows
    dataset_obj = await trade_analysis_service.build_dataset(kalshi_env=kalshi_env, days=days, limit=row_limit)
    rows = [normalize_trade_analysis_row(row) for row in dataset_obj.rows]
    dataset = {"source": "trade-analysis", "row_count": len(rows), "window_days": days}
    return dataset, corpus, rows


def _issues(
    *,
    command: str,
    settings: Settings,
    kalshi_env: str,
    full_history: bool,
    corpus: dict[str, Any],
    data_quality: dict[str, Any],
    promotion_gates: dict[str, Any],
    buy_bypass: dict[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    def add_issue(*, severity: str, code: str, summary: str) -> None:
        existing = next((issue for issue in issues if issue["code"] == code), None)
        if existing is None:
            issues.append({"severity": severity, "code": code, "summary": summary})
            return
        if existing.get("severity") != "fail" and severity == "fail":
            existing["severity"] = "fail"
            existing["summary"] = summary

    if str(kalshi_env).lower() == "production" and not production_entry_freeze_enabled(settings, kalshi_env):
        add_issue(
            severity="fail",
            code="production_entry_freeze_disabled",
            summary="Production entry freeze is disabled.",
        )
    if str(kalshi_env).lower() == "production" and (buy_bypass.get("ticket_count") or buy_bypass.get("order_count")):
        add_issue(
            severity="fail",
            code="production_buy_entry_bypass",
            summary="Production buy entries were observed inside the validation window.",
        )
    if corpus.get("status") != "ok":
        severity = "fail" if command == "validate" and full_history else "warn"
        add_issue(
            severity=severity,
            code="decision_corpus_missing_or_stale",
            summary="No current decision corpus is promoted for this environment.",
        )
    if int(data_quality.get("point_in_time_rows") or 0) <= 0:
        add_issue(
            severity="warn",
            code="no_point_in_time_rows",
            summary="Backtest has no point-in-time-safe rows.",
        )
    for failure in promotion_gates.get("failures") or []:
        add_issue(severity="fail", code=failure, summary=failure)
    for warning in promotion_gates.get("warnings") or []:
        add_issue(severity="warn", code=warning, summary=warning)
    return issues


async def build_backtesting_report(
    *,
    settings: Settings,
    session_factory: async_sessionmaker | None,
    decision_corpus_service: Any,
    trade_analysis_service: Any,
    kalshi_env: str = "demo",
    days: int = 180,
    full_history: bool = False,
    command: str = "run",
    dataset_source: str = "auto",
    limit: int = 20,
    row_limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = (_as_utc(now) or datetime.now(UTC)).isoformat()
    dataset, corpus, rows = await _load_rows(
        dataset_source=dataset_source,
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=decision_corpus_service,
        trade_analysis_service=trade_analysis_service,
        kalshi_env=kalshi_env,
        days=days,
        row_limit=row_limit,
    )
    walk_forward = evaluate_walk_forward_backtest(rows, settings=settings)
    diagnostics = [row_diagnostic(row, settings=settings) for row in rows[:limit]]
    data_quality = _data_quality(rows)
    promotion_gates = _promotion_gates(
        {
            "data_quality": data_quality,
            "walk_forward": walk_forward,
            "baseline_policy": walk_forward.get("baseline_policy"),
            "candidate_policies": walk_forward.get("candidate_policies"),
            "decision_corpus": corpus,
        },
        settings=settings,
        kalshi_env=kalshi_env,
        full_history=full_history,
    )
    buy_bypass = await _production_buy_entry_bypass(
        session_factory,
        kalshi_env=kalshi_env,
        since=(_as_utc(now) or datetime.now(UTC)) - timedelta(hours=24),
    )
    issues = _issues(
        command=command,
        settings=settings,
        kalshi_env=kalshi_env,
        full_history=full_history,
        corpus=corpus,
        data_quality=data_quality,
        promotion_gates=promotion_gates,
        buy_bypass=buy_bypass,
    )
    status = "pass"
    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif command == "validate" and any(issue["severity"] == "warn" for issue in issues):
        status = "warn"
    return {
        "schema_version": SCHEMA_VERSION,
        "simulator_version": SIMULATOR_VERSION,
        "command": command,
        "kalshi_env": kalshi_env,
        "window_days": days,
        "full_history": full_history,
        "generated_at": generated_at,
        "read_only": True,
        "dataset": dataset,
        "decision_corpus": corpus,
        "data_quality": data_quality,
        "folds": walk_forward.get("folds") or [],
        "walk_forward": {
            "status": walk_forward.get("status"),
            "reason": walk_forward.get("reason"),
            "fold_count": walk_forward.get("fold_count"),
            "test_row_count": walk_forward.get("test_row_count"),
            "prediction_metrics": walk_forward.get("prediction_metrics"),
        },
        "baseline_policy": walk_forward.get("baseline_policy"),
        "candidate_policies": walk_forward.get("candidate_policies") or [],
        "bucket_matrix": _bucket_matrix(rows, settings=settings)[:limit],
        "promotion_gates": promotion_gates,
        "buy_entry_bypass": buy_bypass,
        "row_diagnostics": diagnostics,
        "issues": issues,
        "status": status,
    }


def format_backtesting_report(report: dict[str, Any]) -> str:
    dataset = report.get("dataset") or {}
    quality = report.get("data_quality") or {}
    baseline = report.get("baseline_policy") or {}
    gates = report.get("promotion_gates") or {}
    lines = [
        "Backtesting Report",
        (
            f"env={report.get('kalshi_env')} command={report.get('command')} "
            f"status={str(report.get('status')).upper()} source={dataset.get('source')}"
        ),
        (
            f"Rows: total={quality.get('row_count', 0)} "
            f"point_in_time={quality.get('point_in_time_rows', 0)} "
            f"diagnostics_only={quality.get('diagnostics_only_rows', 0)}"
        ),
        (
            f"Baseline: selected={baseline.get('selected_count')} net={baseline.get('net_pnl')} "
            f"drawdown={baseline.get('max_drawdown')} sortino={baseline.get('sortino')}"
        ),
        f"Promotion gates: {gates.get('status')} failures={gates.get('failures') or []} warnings={gates.get('warnings') or []}",
    ]
    candidates = list(report.get("candidate_policies") or [])
    if candidates:
        lines.extend(["", "Candidate policies:"])
        for policy in candidates:
            lines.append(
                f"- {policy.get('policy_name')}: selected={policy.get('selected_count')} "
                f"net={policy.get('net_pnl')} drawdown={policy.get('max_drawdown')} sortino={policy.get('sortino')}"
            )
    if report.get("issues"):
        lines.extend(["", "Issues:"])
        for issue in report["issues"][:10]:
            lines.append(f"- {str(issue.get('severity')).upper()} {issue.get('code')}: {issue.get('summary')}")
    return "\n".join(lines)


def write_backtesting_report(report: dict[str, Any], output: Path) -> dict[str, Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return {"output": str(output), "schema_version": report.get("schema_version"), "status": report.get("status")}
