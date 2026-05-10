from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings
from kalshi_bot.services.backtesting import evaluate_walk_forward_backtest, normalize_trade_analysis_row
from kalshi_bot.services.gate_learning import GateLearningService, gate_learning_rows_to_backtesting_rows
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.services.trade_behavior import production_entry_freeze_enabled
from kalshi_bot.services.trade_behavior_quality import (
    annotate_bucket_matrix,
    bucket_evidence_status,
    bucket_status_summary,
)


SCHEMA_VERSION = "modeling-report-v1"
PREDICTION_MODEL_VERSION = "shadow-platt-v1"
TRADE_SELECTION_MODEL_VERSION = "shadow-bucket-ev-v1"
MIN_MODELING_ROWS = 20
TRAIN_FRACTION = 0.7


def _now_iso(now: datetime | None = None) -> str:
    value = now or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _money(value: Decimal | float | int | None) -> str | None:
    if value is None:
        return None
    return str(Decimal(str(value)).quantize(Decimal("0.0001")))


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
    p = _clip_probability(value)
    return math.log(p / (1.0 - p))


def _baseline_probability(row: dict[str, Any]) -> float | None:
    fair = _float_or_none(row.get("fair_yes_dollars"))
    if fair is None:
        return None
    return _clip_probability(fair)


def _yes_label(row: dict[str, Any]) -> int | None:
    result = str(row.get("kalshi_result") or "").lower()
    if result == "yes":
        return 1
    if result == "no":
        return 0
    return None


def _win_label(row: dict[str, Any]) -> int | None:
    value = row.get("label_win")
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "true":
            return 1
        if lowered == "false":
            return 0
        return None
    if value in (True, False):
        return 1 if value else 0
    return None


def _net_pnl(row: dict[str, Any]) -> Decimal | None:
    return _decimal_or_none(row.get("lifecycle_net_pnl") or row.get("lifecycle_net_pnl_dollars"))


def _chronological_split(
    rows: list[dict[str, Any]],
    *,
    min_rows: int = MIN_MODELING_ROWS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    if len(rows) < min_rows:
        return None
    split_idx = max(1, int(len(rows) * TRAIN_FRACTION))
    train = rows[:split_idx]
    test = rows[split_idx:]
    if not test:
        return None
    return train, test


def _prediction_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("training_eligible") is True
        and _baseline_probability(row) is not None
        and _yes_label(row) is not None
    ]
    eligible.sort(key=lambda row: (str(row.get("decision_ts") or ""), str(row.get("room_id") or "")))
    return eligible


def _trade_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("training_eligible") is True
        and _win_label(row) is not None
        and _net_pnl(row) is not None
        and row.get("bucket_key")
    ]
    eligible.sort(key=lambda row: (str(row.get("decision_ts") or ""), str(row.get("room_id") or "")))
    return eligible


def _fit_platt(rows: list[dict[str, Any]]) -> dict[str, Any]:
    xs = [_logit(_baseline_probability(row) or 0.5) for row in rows]
    labels = [_yes_label(row) or 0 for row in rows]
    mean = sum(xs) / len(xs)
    var = sum((x - mean) ** 2 for x in xs) / max(1, len(xs) - 1)
    std = math.sqrt(var) or 1.0
    weights = [0.0, 0.0]
    lr = 0.05
    reg = 0.01
    for _ in range(800):
        grads = [0.0, 0.0]
        for x_raw, label in zip(xs, labels, strict=True):
            x = (x_raw - mean) / std
            p = _sigmoid(weights[0] + weights[1] * x)
            err = p - label
            grads[0] += err
            grads[1] += err * x
        weights[0] -= lr * (grads[0] / len(rows))
        weights[1] -= lr * ((grads[1] / len(rows)) + reg * weights[1])
    return {
        "model_type": "builtin_platt_logistic",
        "version": PREDICTION_MODEL_VERSION,
        "weights": [round(weights[0], 12), round(weights[1], 12)],
        "baseline_logit_mean": round(mean, 12),
        "baseline_logit_std": round(std, 12),
    }


def _predict_platt(model: dict[str, Any], baseline: float) -> float:
    weights = list(model.get("weights") or [0.0, 1.0])
    mean = float(model.get("baseline_logit_mean") or 0.0)
    std = float(model.get("baseline_logit_std") or 1.0) or 1.0
    x = (_logit(baseline) - mean) / std
    return _clip_probability(_sigmoid(weights[0] + weights[1] * x))


def _probability_metrics(predictions: list[tuple[float, int]]) -> dict[str, Any]:
    if not predictions:
        return {"sample_count": 0, "brier": None, "log_loss": None, "ece": None, "reliability_buckets": []}
    eps = 1e-9
    brier = sum((p - y) ** 2 for p, y in predictions) / len(predictions)
    log_loss = -sum(
        y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p))
        for p, y in predictions
    ) / len(predictions)
    buckets: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for p, y in predictions:
        lo = min(9, int(p * 10)) / 10
        buckets[f"{lo:.1f}-{lo + 0.1:.1f}"].append((p, y))
    reliability = []
    ece = 0.0
    for key, values in sorted(buckets.items()):
        avg_prediction = sum(p for p, _ in values) / len(values)
        observed_rate = sum(y for _, y in values) / len(values)
        ece += (len(values) / len(predictions)) * abs(avg_prediction - observed_rate)
        reliability.append(
            {
                "bucket": key,
                "count": len(values),
                "avg_prediction": _rate(avg_prediction),
                "observed_rate": _rate(observed_rate),
            }
        )
    return {
        "sample_count": len(predictions),
        "brier": _rate(brier),
        "log_loss": _rate(log_loss),
        "ece": _rate(ece),
        "reliability_buckets": reliability,
    }


def _prediction_backtest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = _prediction_rows(rows)
    split = _chronological_split(eligible)
    if split is None:
        return {
            "status": "insufficient_data",
            "reason": "need_at_least_20_point_in_time_rows_with_yes_no_outcomes",
            "eligible_rows": len(eligible),
            "train_rows": 0,
            "test_rows": 0,
            "promotion_ready": False,
        }
    train, test = split
    if len({_yes_label(row) for row in train}) < 2:
        return {
            "status": "insufficient_data",
            "reason": "training_split_needs_both_yes_and_no_outcomes",
            "eligible_rows": len(eligible),
            "train_rows": len(train),
            "test_rows": len(test),
            "promotion_ready": False,
        }
    model = _fit_platt(train)
    baseline_predictions: list[tuple[float, int]] = []
    calibrated_predictions: list[tuple[float, int]] = []
    for row in test:
        baseline = _baseline_probability(row)
        label = _yes_label(row)
        if baseline is None or label is None:
            continue
        baseline_predictions.append((baseline, label))
        calibrated_predictions.append((_predict_platt(model, baseline), label))
    baseline_metrics = _probability_metrics(baseline_predictions)
    calibrated_metrics = _probability_metrics(calibrated_predictions)
    promotion_ready = all(
        calibrated_metrics.get(key) is not None
        and baseline_metrics.get(key) is not None
        and float(calibrated_metrics[key]) < float(baseline_metrics[key])
        for key in ("brier", "log_loss", "ece")
    )
    return {
        "status": "ok",
        "model": model,
        "eligible_rows": len(eligible),
        "train_rows": len(train),
        "test_rows": len(test),
        "train_window": {"start": train[0].get("decision_ts"), "end": train[-1].get("decision_ts")},
        "test_window": {"start": test[0].get("decision_ts"), "end": test[-1].get("decision_ts")},
        "baseline": baseline_metrics,
        "calibrated": calibrated_metrics,
        "promotion_ready": promotion_ready,
        "promotion_blockers": [] if promotion_ready else ["calibrated_metrics_do_not_improve_brier_log_loss_and_ece"],
    }


def _bucket_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get("bucket_key"))].append(row)
    stats: dict[str, dict[str, Any]] = {}
    for key, values in buckets.items():
        pnls = [_net_pnl(row) for row in values]
        pnls = [value for value in pnls if value is not None]
        wins = sum(1 for row in values if _win_label(row) == 1)
        stats[key] = {
            "bucket_key": key,
            "sample_count": len(pnls),
            "win_rate": round(wins / len(pnls), 6) if pnls else None,
            "net_pnl": sum(pnls, Decimal("0")),
            "expected_net_pnl": (sum(pnls, Decimal("0")) / Decimal(len(pnls))) if pnls else None,
        }
    return stats


def _max_drawdown(values: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    max_dd = Decimal("0")
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _pnl_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [_net_pnl(row) for row in rows]
    pnls = [value for value in pnls if value is not None]
    wins = sum(1 for row in rows if _win_label(row) == 1 and _net_pnl(row) is not None)
    return {
        "sample_count": len(pnls),
        "net_pnl": _money(sum(pnls, Decimal("0"))) if pnls else None,
        "win_rate": round(wins / len(pnls), 6) if pnls else None,
        "max_drawdown": _money(_max_drawdown(pnls)) if pnls else None,
    }


def _dataset_bucket_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("bucket_key"):
            grouped[str(row["bucket_key"])].append(row)
    matrix: list[dict[str, Any]] = []
    for key, bucket_rows in grouped.items():
        scored = [row for row in bucket_rows if _net_pnl(row) is not None]
        net = sum((_net_pnl(row) or Decimal("0") for row in scored), Decimal("0"))
        wins = sum(1 for row in scored if _win_label(row) == 1)
        first = bucket_rows[0]
        matrix.append(
            {
                "bucket_key": key,
                "env": first.get("kalshi_env"),
                "strategy": first.get("strategy_code"),
                "side": first.get("side"),
                "series_ticker": first.get("series_ticker"),
                "city": first.get("city"),
                "entry_price_band": first.get("entry_price_band"),
                "forecast_delta_band": first.get("forecast_delta_band"),
                "confidence_band": first.get("confidence_band"),
                "spread_band": first.get("spread_band"),
                "bucket_sample_count": len(scored),
                "settled_or_closed_count": len(scored),
                "bucket_win_rate": round(wins / len(scored), 6) if scored else None,
                "bucket_net_pnl": _money(net) if scored else None,
                "lifecycle_net_pnl": _money(net) if scored else None,
            }
        )
    matrix.sort(
        key=lambda row: (
            _decimal_or_none(row.get("bucket_net_pnl")) or Decimal("-999999"),
            int(row.get("bucket_sample_count") or 0),
            str(row.get("bucket_key") or ""),
        ),
        reverse=True,
    )
    return matrix


def _trade_selection_backtest(rows: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any]:
    eligible = _trade_rows(rows)
    split = _chronological_split(eligible)
    min_samples = int(settings.trade_behavior_empirical_gate_min_settled_fills)
    min_net = Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars))
    if split is None:
        return {
            "status": "insufficient_data",
            "reason": "need_at_least_20_point_in_time_rows_with_net_pnl",
            "eligible_rows": len(eligible),
            "train_rows": 0,
            "test_rows": 0,
            "promotion_ready": False,
            "bucket_status_counts": {},
        }
    train, test = split
    train_stats = _bucket_stats(train)
    selected: list[dict[str, Any]] = []
    scored_rows: list[dict[str, Any]] = []
    for row in test:
        bucket = train_stats.get(str(row.get("bucket_key")))
        if bucket is None:
            status = "unscored"
            expected = None
            sample_count = 0
        else:
            sample_count = int(bucket["sample_count"])
            expected = bucket["expected_net_pnl"]
            status = bucket_evidence_status(
                {
                    "bucket_sample_count": sample_count,
                    "bucket_net_pnl": bucket["net_pnl"],
                },
                min_samples=min_samples,
                min_net_pnl=min_net,
            )
        item = dict(row)
        item["trade_selection_status"] = "selected" if status == "eligible_for_live_review" else status
        item["expected_net_pnl"] = _money(expected) if expected is not None else None
        item["bucket_sample_count"] = sample_count
        scored_rows.append(item)
        if status == "eligible_for_live_review":
            selected.append(item)
    baseline_metrics = _pnl_metrics(test)
    selected_metrics = _pnl_metrics(selected)
    baseline_net = _decimal_or_none(baseline_metrics.get("net_pnl")) or Decimal("0")
    selected_net = _decimal_or_none(selected_metrics.get("net_pnl")) or Decimal("0")
    baseline_dd = _decimal_or_none(baseline_metrics.get("max_drawdown")) or Decimal("0")
    selected_dd = _decimal_or_none(selected_metrics.get("max_drawdown")) or Decimal("0")
    status_counts = Counter(str(row.get("trade_selection_status") or "unknown") for row in scored_rows)
    promotion_ready = bool(selected) and selected_net > baseline_net and selected_dd <= baseline_dd
    return {
        "status": "ok",
        "model": {
            "model_type": "bucket_expected_net_pnl",
            "version": TRADE_SELECTION_MODEL_VERSION,
            "min_samples": min_samples,
            "min_net_pnl_dollars": str(min_net),
            "bucket_count": len(train_stats),
        },
        "eligible_rows": len(eligible),
        "train_rows": len(train),
        "test_rows": len(test),
        "train_window": {"start": train[0].get("decision_ts"), "end": train[-1].get("decision_ts")},
        "test_window": {"start": test[0].get("decision_ts"), "end": test[-1].get("decision_ts")},
        "baseline_rule_path": baseline_metrics,
        "selected_path": selected_metrics,
        "bucket_status_counts": dict(status_counts),
        "selected_rows": selected[:20],
        "promotion_ready": promotion_ready,
        "promotion_blockers": [] if promotion_ready else ["selected_path_does_not_improve_net_pnl_and_drawdown"],
    }


def _modeling_issues(
    *,
    settings: Settings,
    kalshi_env: str,
    corpus: dict[str, Any],
    prediction: dict[str, Any],
    selection: dict[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if kalshi_env == "production" and not production_entry_freeze_enabled(settings, kalshi_env):
        issues.append({
            "severity": "fail",
            "code": "production_entry_freeze_disabled",
            "summary": "Production entry freeze is disabled.",
        })
    if corpus.get("status") != "ok":
        issues.append({
            "severity": "warn",
            "code": "decision_corpus_missing_or_stale",
            "summary": "No current decision corpus is promoted for this environment.",
        })
    if prediction.get("status") != "ok":
        issues.append({
            "severity": "warn",
            "code": "prediction_insufficient_data",
            "summary": str(prediction.get("reason") or "Prediction calibration cannot be evaluated yet."),
        })
    elif not prediction.get("promotion_ready"):
        issues.append({
            "severity": "warn",
            "code": "prediction_not_shadow_promotable",
            "summary": "Calibrated probabilities do not improve all promotion metrics.",
        })
    if selection.get("status") != "ok":
        issues.append({
            "severity": "warn",
            "code": "trade_selection_insufficient_data",
            "summary": str(selection.get("reason") or "Trade selection cannot be evaluated yet."),
        })
    elif not selection.get("promotion_ready"):
        issues.append({
            "severity": "warn",
            "code": "trade_selection_not_shadow_promotable",
            "summary": "Trade selection does not improve net P&L and drawdown versus current rules.",
        })
    return issues


def _prediction_from_walk_forward(walk_forward: dict[str, Any], *, eligible_rows: int) -> dict[str, Any]:
    prediction = dict((walk_forward.get("prediction_metrics") or {}))
    baseline = dict(prediction.get("baseline") or {})
    calibrated = dict(prediction.get("calibrated") or {})
    if walk_forward.get("status") != "ok":
        return {
            "status": "insufficient_data",
            "reason": walk_forward.get("reason") or "walk_forward_insufficient_data",
            "eligible_rows": eligible_rows,
            "train_rows": 0,
            "test_rows": 0,
            "fold_count": int(walk_forward.get("fold_count") or 0),
            "promotion_ready": False,
        }
    promotion_ready = all(
        calibrated.get(key) is not None
        and baseline.get(key) is not None
        and float(calibrated[key]) < float(baseline[key])
        for key in ("brier", "log_loss", "ece")
    )
    folds = list(walk_forward.get("folds") or [])
    return {
        "status": "ok",
        "model_type": "walk_forward_platt",
        "eligible_rows": eligible_rows,
        "train_rows": sum(int(fold.get("train_rows") or 0) for fold in folds),
        "test_rows": int(walk_forward.get("test_row_count") or 0),
        "fold_count": int(walk_forward.get("fold_count") or 0),
        "folds": folds,
        "baseline": baseline,
        "calibrated": calibrated,
        "promotion_ready": promotion_ready,
        "promotion_blockers": [] if promotion_ready else ["walk_forward_calibration_does_not_improve_brier_log_loss_and_ece"],
    }


def _selection_from_walk_forward(walk_forward: dict[str, Any], *, eligible_rows: int) -> dict[str, Any]:
    if walk_forward.get("status") != "ok":
        return {
            "status": "insufficient_data",
            "reason": walk_forward.get("reason") or "walk_forward_insufficient_data",
            "eligible_rows": eligible_rows,
            "train_rows": 0,
            "test_rows": 0,
            "fold_count": int(walk_forward.get("fold_count") or 0),
            "promotion_ready": False,
            "bucket_status_counts": {},
        }
    candidates = {
        str(policy.get("policy_name")): dict(policy)
        for policy in list(walk_forward.get("candidate_policies") or [])
    }
    selected_path = candidates.get("trade_selection_policy") or {}
    baseline = dict(walk_forward.get("baseline_policy") or {})
    baseline_net = _decimal_or_none(baseline.get("net_pnl")) or Decimal("0")
    selected_net = _decimal_or_none(selected_path.get("net_pnl")) or Decimal("0")
    baseline_dd = _decimal_or_none(baseline.get("max_drawdown")) or Decimal("0")
    selected_dd = _decimal_or_none(selected_path.get("max_drawdown")) or Decimal("0")
    promotion_ready = selected_net > baseline_net and selected_dd <= baseline_dd
    folds = list(walk_forward.get("folds") or [])
    return {
        "status": "ok",
        "model": {
            "model_type": "walk_forward_bucket_expected_net_pnl",
            "version": TRADE_SELECTION_MODEL_VERSION,
        },
        "eligible_rows": eligible_rows,
        "train_rows": sum(int(fold.get("train_rows") or 0) for fold in folds),
        "test_rows": int(walk_forward.get("test_row_count") or 0),
        "fold_count": int(walk_forward.get("fold_count") or 0),
        "folds": folds,
        "baseline_rule_path": baseline,
        "selected_path": selected_path,
        "candidate_policies": list(walk_forward.get("candidate_policies") or []),
        "bucket_status_counts": {},
        "promotion_ready": promotion_ready,
        "promotion_blockers": [] if promotion_ready else ["walk_forward_trade_selection_does_not_improve_net_pnl_and_drawdown"],
    }


def _normalized_prediction_eligible_rows(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if row.get("leakage_status") == "point_in_time"
        and _float_or_none(row.get("fair_yes_dollars")) is not None
        and str(row.get("settlement_result") or "").lower() in {"yes", "no"}
    )


def _normalized_selection_eligible_rows(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if row.get("leakage_status") == "point_in_time"
        and str(row.get("side") or "").lower() in {"yes", "no"}
        and (row.get("actual_net_pnl") is not None or row.get("counterfactual_pnl") is not None)
    )


def format_modeling_report(report: dict[str, Any]) -> str:
    prediction = report.get("prediction_calibration") or {}
    selection = report.get("trade_selection") or {}
    corpus = report.get("decision_corpus") or {}
    lines = [
        "Modeling And Prediction",
        (
            f"env={report.get('kalshi_env')} command={report.get('command')} "
            f"status={str(report.get('status')).upper()} window={report.get('window_days')}d"
        ),
        f"Decision corpus: status={corpus.get('status')} build_id={(corpus.get('build') or {}).get('id')}",
        (
            "Prediction: "
            f"status={prediction.get('status')} eligible={prediction.get('eligible_rows')} "
            f"train={prediction.get('train_rows')} test={prediction.get('test_rows')} "
            f"promotion_ready={prediction.get('promotion_ready')}"
        ),
        (
            "Trade selection: "
            f"status={selection.get('status')} eligible={selection.get('eligible_rows')} "
            f"train={selection.get('train_rows')} test={selection.get('test_rows')} "
            f"promotion_ready={selection.get('promotion_ready')}"
        ),
    ]
    if prediction.get("status") == "ok":
        baseline = prediction.get("baseline") or {}
        calibrated = prediction.get("calibrated") or {}
        lines.extend([
            "",
            "Prediction metrics:",
            f"- baseline brier={baseline.get('brier')} log_loss={baseline.get('log_loss')} ece={baseline.get('ece')}",
            (
                f"- calibrated brier={calibrated.get('brier')} "
                f"log_loss={calibrated.get('log_loss')} ece={calibrated.get('ece')}"
            ),
        ])
    if selection.get("status") == "ok":
        baseline = selection.get("baseline_rule_path") or {}
        selected = selection.get("selected_path") or {}
        lines.extend([
            "",
            "Selection metrics:",
            (
                f"- baseline net={baseline.get('net_pnl')} "
                f"drawdown={baseline.get('max_drawdown')} samples={baseline.get('sample_count')}"
            ),
            (
                f"- selected net={selected.get('net_pnl')} "
                f"drawdown={selected.get('max_drawdown')} samples={selected.get('sample_count')}"
            ),
        ])
    if report.get("bucket_matrix_summary"):
        summary = report["bucket_matrix_summary"]
        lines.extend([
            "",
            (
                "Bucket readiness: "
                f"eligible={summary.get('eligible_for_live_review_count', 0)} "
                f"negative={summary.get('negative_actual_pnl_count', 0)} "
                f"under_sampled={summary.get('under_sampled_count', 0)} "
                f"unscored={summary.get('unscored_count', 0)}"
            ),
        ])
    if report.get("issues"):
        lines.extend(["", "Issues:"])
        for issue in report["issues"][:10]:
            lines.append(f"- {str(issue.get('severity')).upper()} {issue.get('code')}: {issue.get('summary')}")
    return "\n".join(lines)


async def build_modeling_report(
    *,
    settings: Settings,
    decision_corpus_service: Any,
    trading_audit_service: Any,
    trade_analysis_service: Any,
    kalshi_env: str = "demo",
    days: int = 180,
    command: str = "backtest",
    dataset_source: str = "trade-analysis",
    limit: int = 20,
    row_limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _now_iso(now)
    try:
        corpus = await decision_corpus_service.current(kalshi_env=kalshi_env)
    except Exception as exc:
        corpus = {"status": "error", "kalshi_env": kalshi_env, "error": str(exc), "build": None}
    if dataset_source == "gate-learning-bundles":
        gate_service = GateLearningService(settings)
        now_utc = now or datetime.now(UTC)
        now_utc = now_utc.replace(tzinfo=UTC) if now_utc.tzinfo is None else now_utc.astimezone(UTC)
        cutoff = now_utc - timedelta(days=days)
        bundle_rows = [
            row for row in gate_service.load_bundle_rows(source="combined")
            if row.decision_time is None or row.decision_time >= cutoff
        ]
        normalized_rows = gate_learning_rows_to_backtesting_rows(bundle_rows, kalshi_env=kalshi_env)
        if row_limit is not None:
            normalized_rows = normalized_rows[:row_limit]
        dataset_rows: list[dict[str, Any]] = []
        dataset_summary = {
            "source": "gate-learning-bundles",
            "row_count": len(normalized_rows),
            "source_files": gate_service.source_files(source="combined"),
        }
    else:
        dataset = await trade_analysis_service.build_dataset(kalshi_env=kalshi_env, days=days, limit=row_limit)
        dataset_rows = list(dataset.rows)
        normalized_rows = [normalize_trade_analysis_row(row) for row in dataset.rows]
        dataset_summary = {"source": "trade-analysis", "row_count": len(dataset.rows)}
    audit = {"lifecycle": {}}
    if command != "status" and row_limit is None and dataset_source != "gate-learning-bundles":
        audit = await trading_audit_service.build_report(kalshi_env=kalshi_env, days=days)
    walk_forward = evaluate_walk_forward_backtest(normalized_rows, settings=settings)
    prediction = _prediction_from_walk_forward(
        walk_forward,
        eligible_rows=_normalized_prediction_eligible_rows(normalized_rows),
    )
    selection = _selection_from_walk_forward(
        walk_forward,
        eligible_rows=_normalized_selection_eligible_rows(normalized_rows),
    )
    lifecycle = dict(audit.get("lifecycle") or {})
    bucket_rows = list(lifecycle.get("buckets") or [])
    if not bucket_rows and dataset_rows:
        bucket_rows = _dataset_bucket_matrix(dataset_rows)
    annotated_buckets = annotate_bucket_matrix(
        bucket_rows,
        settings=settings,
        kalshi_env=kalshi_env,
        freeze_enabled=production_entry_freeze_enabled(settings, kalshi_env),
    )
    bucket_summary = bucket_status_summary(annotated_buckets)
    issues = _modeling_issues(
        settings=settings,
        kalshi_env=kalshi_env,
        corpus=corpus,
        prediction=prediction,
        selection=selection,
    )
    status = "pass"
    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif command == "validate" and any(issue["severity"] == "warn" for issue in issues):
        status = "warn"
    return {
        "schema_version": SCHEMA_VERSION,
        "command": command,
        "kalshi_env": kalshi_env,
        "window_days": days,
        "generated_at": generated_at,
        "read_only": True,
        "production_entry_freeze_enabled": production_entry_freeze_enabled(settings, kalshi_env),
        "demo_exploratory": str(kalshi_env).lower() == "demo",
        "decision_corpus": corpus,
        "dataset": {
            **dataset_summary,
            "prediction_eligible_rows": int(prediction.get("eligible_rows") or 0),
            "trade_selection_eligible_rows": int(selection.get("eligible_rows") or 0),
        },
        "prediction_calibration": prediction,
        "trade_selection": selection,
        "walk_forward": walk_forward,
        "bucket_matrix_summary": bucket_summary,
        "bucket_matrix": annotated_buckets[:limit],
        "issues": issues,
        "status": status,
    }


def build_shadow_modeling_payload(
    *,
    signal: StrategySignal,
    kalshi_env: str,
    shadow_mode: bool,
    bucket_key: str | None,
    empirical_gate: dict[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    baseline = _clip_probability(float(signal.fair_yes_dollars))
    empirical_gate = dict(empirical_gate or {})
    gate_reason = str(empirical_gate.get("reason") or "empirical_gate_unavailable")
    gate_status = str(empirical_gate.get("status") or "unscored")
    expected_net = empirical_gate.get("actual_net_pnl")
    actual_sample_count = int(empirical_gate.get("actual_sample_count") or 0)
    production_frozen = str(kalshi_env).lower() == "production"
    selection_status = "production_frozen" if production_frozen else "exploratory_evidence"
    if gate_reason == "empirical_gate_passed":
        selection_status = "eligible_for_live_review"
    elif "negative" in gate_reason:
        selection_status = "negative_actual_pnl"
    elif "under_sampled" in gate_reason:
        selection_status = "under_sampled"
    elif gate_status == "blocked" and production_frozen:
        selection_status = "production_frozen"
    return {
        "schema_version": "modeling-shadow-payload-v1",
        "generated_at": _now_iso(generated_at),
        "shadow_only": True,
        "prediction_model": {
            "version": PREDICTION_MODEL_VERSION,
            "status": "baseline_shadow_only",
            "reason": "no_shadow_calibration_promoted",
            "closed_form_fair_yes": round(baseline, 6),
            "calibrated_fair_yes": round(baseline, 6),
            "calibration_version": None,
        },
        "trade_selection_model": {
            "version": TRADE_SELECTION_MODEL_VERSION,
            "status": selection_status,
            "reason": gate_reason,
            "bucket_key": bucket_key,
            "actual_sample_count": actual_sample_count,
            "expected_net_pnl": str(expected_net) if expected_net is not None else None,
            "shadow_recommendation": (
                "score_only"
                if shadow_mode or str(kalshi_env).lower() == "demo"
                else "score_only_frozen_forward"
            ),
        },
    }
