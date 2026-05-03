from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any, Callable

from kalshi_bot.config import Settings
from kalshi_bot.services.trade_behavior import production_entry_freeze_enabled


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


def _money(value: Decimal | None) -> str | None:
    return str(value.quantize(Decimal("0.0001"))) if value is not None else None


def bucket_evidence_status(
    row: dict[str, Any],
    *,
    min_samples: int,
    min_net_pnl: Decimal,
) -> str:
    sample_count = int(row.get("bucket_sample_count") or row.get("settled_or_closed_count") or 0)
    net = _decimal_or_none(row.get("bucket_net_pnl") or row.get("lifecycle_net_pnl"))
    if sample_count <= 0 or net is None:
        return "unscored"
    if sample_count < min_samples:
        return "under_sampled"
    if net <= min_net_pnl:
        return "negative_actual_pnl"
    return "eligible_for_live_review"


def annotate_bucket_matrix(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    kalshi_env: str,
    freeze_enabled: bool | None = None,
) -> list[dict[str, Any]]:
    min_samples = int(settings.trade_behavior_empirical_gate_min_settled_fills)
    min_net = Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars))
    prod_frozen = production_entry_freeze_enabled(settings, kalshi_env) if freeze_enabled is None else freeze_enabled
    annotated: list[dict[str, Any]] = []
    for row in rows:
        evidence_status = bucket_evidence_status(row, min_samples=min_samples, min_net_pnl=min_net)
        status = "production_frozen" if str(kalshi_env).lower() == "production" and prod_frozen else evidence_status
        item = dict(row)
        item["evidence_status"] = evidence_status
        item["gate_status"] = status
        item["live_review_status"] = "blocked_by_freeze" if status == "production_frozen" else evidence_status
        if str(kalshi_env).lower() == "demo" and evidence_status != "eligible_for_live_review":
            item["demo_treatment"] = "exploratory_evidence_only"
        elif str(kalshi_env).lower() == "demo":
            item["demo_treatment"] = "positive_evidence_collection"
        elif status == "production_frozen":
            item["demo_treatment"] = None
        else:
            item["demo_treatment"] = None
        annotated.append(item)
    return annotated


def bucket_status_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evidence_counts = Counter(str(row.get("evidence_status") or "unknown") for row in rows)
    gate_counts = Counter(str(row.get("gate_status") or "unknown") for row in rows)
    return {
        "bucket_count": len(rows),
        "evidence_status_counts": dict(evidence_counts),
        "bucket_status_counts": dict(gate_counts),
        "eligible_for_live_review_count": int(evidence_counts.get("eligible_for_live_review", 0)),
        "negative_actual_pnl_count": int(evidence_counts.get("negative_actual_pnl", 0)),
        "under_sampled_count": int(evidence_counts.get("under_sampled", 0)),
        "unscored_count": int(evidence_counts.get("unscored", 0)),
        "production_frozen_count": int(gate_counts.get("production_frozen", 0)),
    }


def _pnl(row: dict[str, Any]) -> Decimal | None:
    return _decimal_or_none(row.get("lifecycle_net_pnl") or row.get("lifecycle_net_pnl_dollars"))


def _score_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [value for row in rows if (value := _pnl(row)) is not None]
    wins = sum(1 for row in rows if row.get("label_win") is True and _pnl(row) is not None)
    net = sum(values, Decimal("0"))
    return {
        "sample_count": len(values),
        "win_rate": round(wins / len(values), 6) if values else None,
        "net_pnl": _money(net) if values else None,
    }


def _score_filter(
    *,
    rows: list[dict[str, Any]],
    kept: list[dict[str, Any]],
    filter_name: str,
    filter_type: str,
    filter_field: str,
    filter_value: Any,
    description: str,
    min_samples: int,
) -> dict[str, Any]:
    kept_ids = {id(row) for row in kept}
    blocked = [row for row in rows if id(row) not in kept_ids]
    kept_values = [value for row in kept if (value := _pnl(row)) is not None]
    blocked_values = [value for row in blocked if (value := _pnl(row)) is not None]
    kept_net = sum(kept_values, Decimal("0"))
    baseline_net = sum((_pnl(row) or Decimal("0") for row in rows), Decimal("0"))
    blocked_loss = sum((-value for value in blocked_values if value < 0), Decimal("0"))
    missed_profit = sum((value for value in blocked_values if value > 0), Decimal("0"))
    wins = sum(1 for row in kept if row.get("label_win") is True and _pnl(row) is not None)
    return {
        "filter": filter_name,
        "filter_type": filter_type,
        "field": filter_field,
        "value": filter_value,
        "description": description,
        "sample_count": len(kept_values),
        "passes_min_samples": len(kept_values) >= min_samples,
        "coverage": round(len(kept_values) / len(rows), 6) if rows else None,
        "win_rate": round(wins / len(kept_values), 6) if kept_values else None,
        "net_pnl": _money(kept_net) if kept_values else None,
        "net_improvement_dollars": _money(kept_net - baseline_net) if rows else None,
        "blocked_loss_dollars": _money(blocked_loss),
        "missed_profit_dollars": _money(missed_profit),
    }


def _numeric_min_filter(field: str, threshold: float) -> Callable[[dict[str, Any]], bool]:
    return lambda row: (value := _float_or_none(row.get(field))) is not None and value >= threshold


def _numeric_abs_min_filter(field: str, threshold: float) -> Callable[[dict[str, Any]], bool]:
    return lambda row: (value := _float_or_none(row.get(field))) is not None and abs(value) >= threshold


def _numeric_max_filter(field: str, threshold: float) -> Callable[[dict[str, Any]], bool]:
    return lambda row: (value := _float_or_none(row.get(field))) is not None and value <= threshold


def build_threshold_sweeps(rows: list[dict[str, Any]], *, min_samples: int, limit: int) -> list[dict[str, Any]]:
    scored_rows = [row for row in rows if _pnl(row) is not None]
    if not scored_rows:
        return []
    sweeps: list[dict[str, Any]] = []
    numeric_filters: list[tuple[str, str, str, Any, Callable[[dict[str, Any]], bool], str]] = []
    for threshold in (0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90):
        numeric_filters.append((
            f"confidence_min_{threshold:.2f}",
            "minimum",
            "confidence",
            threshold,
            _numeric_min_filter("confidence", threshold),
            f"Keep rows with confidence >= {threshold:.2f}.",
        ))
    for threshold in (0, 2, 5, 8, 10, 12):
        numeric_filters.append((
            f"abs_forecast_delta_min_{threshold}",
            "minimum_abs",
            "forecast_delta_f",
            threshold,
            _numeric_abs_min_filter("forecast_delta_f", threshold),
            f"Keep rows with absolute forecast delta >= {threshold}F.",
        ))
    for threshold in (100, 250, 500, 800, 1000, 1200, 1500):
        numeric_filters.append((
            f"spread_max_{threshold}bps",
            "maximum",
            "market_spread_bps",
            threshold,
            _numeric_max_filter("market_spread_bps", threshold),
            f"Keep rows with market spread <= {threshold}bps.",
        ))
    for name, filter_type, field, value, predicate, description in numeric_filters:
        kept = [row for row in scored_rows if predicate(row)]
        sweeps.append(_score_filter(
            rows=scored_rows,
            kept=kept,
            filter_name=name,
            filter_type=filter_type,
            filter_field=field,
            filter_value=value,
            description=description,
            min_samples=min_samples,
        ))

    categorical_fields = (
        "side",
        "series_ticker",
        "city",
        "entry_price_band",
        "forecast_delta_band",
        "confidence_band",
        "spread_band",
    )
    for field in categorical_fields:
        values = [str(row.get(field) or "unknown") for row in scored_rows]
        for value, count in Counter(values).most_common():
            if count == 0:
                continue
            kept = [row for row in scored_rows if str(row.get(field) or "unknown") != value]
            sweeps.append(_score_filter(
                rows=scored_rows,
                kept=kept,
                filter_name=f"exclude_{field}_{value}",
                filter_type="exclude_value",
                filter_field=field,
                filter_value=value,
                description=f"Exclude rows where {field} is {value}.",
                min_samples=min_samples,
            ))

    sweeps.sort(
        key=lambda row: (
            bool(row.get("passes_min_samples")),
            _decimal_or_none(row.get("net_improvement_dollars")) or Decimal("-999999"),
            _decimal_or_none(row.get("net_pnl")) or Decimal("-999999"),
            int(row.get("sample_count") or 0),
        ),
        reverse=True,
    )
    return sweeps[:limit]


def _point_in_time_scored_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("training_eligible") is True
        and _pnl(row) is not None
    ]


async def build_trade_behavior_quality_report(
    *,
    settings: Settings,
    trading_audit_service: Any,
    trade_analysis_service: Any,
    kalshi_env: str = "production",
    days: int = 180,
    min_samples: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    original_min_samples = settings.trade_behavior_empirical_gate_min_settled_fills
    if min_samples is not None:
        settings = settings.model_copy(update={"trade_behavior_empirical_gate_min_settled_fills": int(min_samples)})
    audit = await trading_audit_service.build_report(kalshi_env=kalshi_env, days=days)
    dataset = await trade_analysis_service.build_dataset(kalshi_env=kalshi_env, days=days)
    lifecycle = dict(audit.get("lifecycle") or {})
    raw_buckets = list(lifecycle.get("buckets") or [])
    if not raw_buckets:
        by_key: dict[str, dict[str, Any]] = {}
        for row in list(lifecycle.get("worst_buckets") or []) + list(lifecycle.get("best_buckets") or []):
            if isinstance(row, dict) and row.get("bucket_key"):
                by_key[str(row["bucket_key"])] = row
        raw_buckets = list(by_key.values())
    freeze_enabled = production_entry_freeze_enabled(settings, kalshi_env)
    buckets = annotate_bucket_matrix(
        raw_buckets,
        settings=settings,
        kalshi_env=kalshi_env,
        freeze_enabled=freeze_enabled,
    )
    status_summary = bucket_status_summary(buckets)
    scored_rows = _point_in_time_scored_rows(dataset.rows)
    scored_summary = _score_rows(scored_rows)
    threshold_sweeps = build_threshold_sweeps(scored_rows, min_samples=int(settings.trade_behavior_empirical_gate_min_settled_fills), limit=limit)
    buckets_by_net = sorted(
        buckets,
        key=lambda row: (_decimal_or_none(row.get("bucket_net_pnl") or row.get("lifecycle_net_pnl")) or Decimal("0"), row.get("bucket_key") or ""),
    )
    candidates = [
        row for row in sorted(
            buckets,
            key=lambda item: (
                _decimal_or_none(item.get("bucket_net_pnl") or item.get("lifecycle_net_pnl")) or Decimal("-999999"),
                int(item.get("bucket_sample_count") or 0),
            ),
            reverse=True,
        )
        if row.get("evidence_status") == "eligible_for_live_review"
    ]
    experiments = [
        row for row in buckets_by_net
        if row.get("evidence_status") in {"negative_actual_pnl", "under_sampled", "unscored"}
    ]
    return {
        "schema_version": "trade-behavior-quality-v1",
        "kalshi_env": kalshi_env,
        "window_days": days,
        "read_only": True,
        "production_entry_freeze_enabled": freeze_enabled,
        "demo_exploratory": str(kalshi_env).lower() == "demo",
        "min_samples": int(settings.trade_behavior_empirical_gate_min_settled_fills),
        "min_net_pnl_dollars": settings.trade_behavior_empirical_gate_min_net_pnl_dollars,
        "bucket_matrix_summary": status_summary,
        "bucket_matrix": buckets[:limit],
        "worst_losing_cohorts": [row for row in buckets_by_net if (_decimal_or_none(row.get("bucket_net_pnl") or row.get("lifecycle_net_pnl")) or Decimal("0")) < 0][:limit],
        "candidate_live_review_buckets": candidates[:limit],
        "recommended_shadow_demo_experiment_buckets": experiments[:limit],
        "point_in_time_scored_rows": scored_summary,
        "threshold_sweeps": threshold_sweeps,
        "audit_issue_count": len(audit.get("issues") or []),
        "analysis_row_count": len(dataset.rows),
        "original_settings_min_samples": original_min_samples,
    }


def format_trade_behavior_quality_report(report: dict[str, Any]) -> str:
    summary = report.get("bucket_matrix_summary") or {}
    scored = report.get("point_in_time_scored_rows") or {}
    lines = [
        "Trade Behavior Quality",
        f"env={report.get('kalshi_env')} window={report.get('window_days')}d read_only={report.get('read_only')}",
        (
            "Buckets: "
            f"total={summary.get('bucket_count', 0)} "
            f"eligible={summary.get('eligible_for_live_review_count', 0)} "
            f"negative={summary.get('negative_actual_pnl_count', 0)} "
            f"under_sampled={summary.get('under_sampled_count', 0)} "
            f"unscored={summary.get('unscored_count', 0)} "
            f"production_frozen={summary.get('production_frozen_count', 0)}"
        ),
        (
            "Point-in-time scored rows: "
            f"samples={scored.get('sample_count', 0)} "
            f"net={scored.get('net_pnl')} "
            f"win_rate={scored.get('win_rate')}"
        ),
    ]
    if report.get("worst_losing_cohorts"):
        lines.extend(["", "Worst losing cohorts:"])
        for row in report["worst_losing_cohorts"][:5]:
            lines.append(
                f"- {row.get('bucket_key')}: net={row.get('bucket_net_pnl') or row.get('lifecycle_net_pnl')} "
                f"samples={row.get('bucket_sample_count')} status={row.get('evidence_status')}"
            )
    if report.get("candidate_live_review_buckets"):
        lines.extend(["", "Live-review candidates:"])
        for row in report["candidate_live_review_buckets"][:5]:
            lines.append(
                f"- {row.get('bucket_key')}: net={row.get('bucket_net_pnl') or row.get('lifecycle_net_pnl')} "
                f"samples={row.get('bucket_sample_count')} win_rate={row.get('bucket_win_rate')}"
            )
    if report.get("threshold_sweeps"):
        lines.extend(["", "Best filter sweeps:"])
        for row in report["threshold_sweeps"][:5]:
            lines.append(
                f"- {row.get('filter')}: net={row.get('net_pnl')} improvement={row.get('net_improvement_dollars')} "
                f"blocked_loss={row.get('blocked_loss_dollars')} missed_profit={row.get('missed_profit_dollars')} "
                f"coverage={row.get('coverage')}"
            )
    return "\n".join(lines)
