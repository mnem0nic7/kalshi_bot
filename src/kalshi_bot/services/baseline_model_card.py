from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


MODEL_CARD_VERSION = "baseline-model-card-v1"
EXECUTION_MARKET_DAY_TARGET = 60
DIRECTIONAL_MARKET_DAY_TARGET = 30
HOLDOUT_MARKET_DAY_TARGET = 7


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _room(row: dict[str, Any]) -> dict[str, Any]:
    room = row.get("room")
    return room if isinstance(room, dict) else row


def _market_ticker(row: dict[str, Any]) -> str:
    return str(row.get("market_ticker") or _room(row).get("market_ticker") or "")


def _coverage_class(row: dict[str, Any]) -> str:
    provenance = row.get("historical_provenance") if isinstance(row.get("historical_provenance"), dict) else {}
    return str(row.get("coverage_class") or provenance.get("coverage_class") or "unknown")


def _split(row: dict[str, Any]) -> str:
    return str(row.get("split") or "unknown")


def _market_day(row: dict[str, Any]) -> str | None:
    provenance = row.get("historical_provenance") if isinstance(row.get("historical_provenance"), dict) else {}
    if provenance.get("local_market_day"):
        return str(provenance["local_market_day"])
    ticker = _market_ticker(row)
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def _counter(rows: list[dict[str, Any]], extractor) -> dict[str, int]:
    return dict(Counter(extractor(row) for row in rows).most_common())


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _signal_probability(row: dict[str, Any]) -> float | None:
    signal = row.get("signal") if isinstance(row.get("signal"), dict) else {}
    value = signal.get("fair_yes_dollars")
    parsed = _decimal(value)
    if parsed is None:
        return None
    return max(0.0, min(1.0, float(parsed)))


def _settlement_value(row: dict[str, Any]) -> float | None:
    for key in ("settlement_label", "settlement"):
        payload = row.get(key)
        if isinstance(payload, dict):
            value = _decimal(payload.get("settlement_value_dollars"))
            if value is not None:
                return 1.0 if value >= Decimal("0.5") else 0.0
    provenance = row.get("historical_provenance") if isinstance(row.get("historical_provenance"), dict) else {}
    signature = provenance.get("settlement_label_signature")
    if isinstance(signature, str):
        try:
            parsed = json.loads(signature)
        except json.JSONDecodeError:
            parsed = {}
        value = _decimal(parsed.get("settlement_value_dollars"))
        if value is not None:
            return 1.0 if value >= Decimal("0.5") else 0.0
        result = str(parsed.get("kalshi_result") or parsed.get("crosscheck_result") or "").lower()
        if result == "yes":
            return 1.0
        if result == "no":
            return 0.0
    return None


def _calibration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scored: list[tuple[float, float]] = []
    for row in rows:
        prediction = _signal_probability(row)
        label = _settlement_value(row)
        if prediction is None or label is None:
            continue
        scored.append((prediction, label))
    if not scored:
        return {"scored_count": 0, "brier": None, "ece": None}
    brier = sum((prediction - label) ** 2 for prediction, label in scored) / len(scored)
    ece = 0.0
    for bucket in range(10):
        lower = bucket / 10
        upper = 1.0 if bucket == 9 else (bucket + 1) / 10
        bucket_rows = [
            (prediction, label)
            for prediction, label in scored
            if prediction >= lower and (prediction <= upper if bucket == 9 else prediction < upper)
        ]
        if not bucket_rows:
            continue
        avg_prediction = sum(item[0] for item in bucket_rows) / len(bucket_rows)
        avg_label = sum(item[1] for item in bucket_rows) / len(bucket_rows)
        ece += (len(bucket_rows) / len(scored)) * abs(avg_prediction - avg_label)
    return {"scored_count": len(scored), "brier": round(brier, 6), "ece": round(ece, 6)}


def _policy_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def outcome(row: dict[str, Any]) -> dict[str, Any]:
        value = row.get("outcome")
        return value if isinstance(value, dict) else {}

    return {
        "final_status_counts": _counter(rows, lambda row: str(outcome(row).get("final_status") or "unknown")),
        "stand_down_reason_counts": _counter(rows, lambda row: str(outcome(row).get("stand_down_reason") or "none")),
        "risk_status_counts": _counter(rows, lambda row: str(outcome(row).get("risk_status") or "none")),
        "ticket_generated_count": sum(1 for row in rows if bool(outcome(row).get("ticket_generated")) or row.get("trade_ticket")),
        "risk_verdict_count": sum(1 for row in rows if row.get("risk_verdict")),
        "orders_submitted_count": sum(int(outcome(row).get("orders_submitted") or 0) for row in rows),
    }


def _pnl_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [
        value
        for value in (_decimal(row.get("counterfactual_pnl_dollars")) for row in rows)
        if value is not None
    ]
    if not values:
        return {"scored_count": 0, "total_counterfactual_pnl_dollars": None, "mean_counterfactual_pnl_dollars": None}
    total = sum(values, Decimal("0"))
    return {
        "scored_count": len(values),
        "total_counterfactual_pnl_dollars": str(total.quantize(Decimal("0.0001"))),
        "mean_counterfactual_pnl_dollars": str((total / Decimal(len(values))).quantize(Decimal("0.0001"))),
    }


def _support_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    execution_days = {
        _market_day(row)
        for row in rows
        if _market_day(row) is not None
        and _coverage_class(row) in {"full_checkpoint_coverage", "late_only_coverage", "partial_checkpoint_coverage"}
    }
    full_days = {
        _market_day(row)
        for row in rows
        if _market_day(row) is not None and _coverage_class(row) == "full_checkpoint_coverage"
    }
    holdout_days = {
        _market_day(row)
        for row in rows
        if _market_day(row) is not None and _coverage_class(row) == "full_checkpoint_coverage" and _split(row) == "holdout"
    }
    return {
        "execution_market_days": len(execution_days),
        "full_checkpoint_market_days": len(full_days),
        "full_checkpoint_holdout_market_days": len(holdout_days),
        "targets": {
            "execution_market_days": EXECUTION_MARKET_DAY_TARGET,
            "full_checkpoint_market_days": DIRECTIONAL_MARKET_DAY_TARGET,
            "full_checkpoint_holdout_market_days": HOLDOUT_MARKET_DAY_TARGET,
        },
    }


def _promotion_blockers(historical_rows: list[dict[str, Any]], shadow_rows: list[dict[str, Any]]) -> list[str]:
    support = _support_summary(historical_rows)
    blockers: list[str] = []
    if support["execution_market_days"] < EXECUTION_MARKET_DAY_TARGET:
        blockers.append("lack_of_execution_support")
    if support["full_checkpoint_market_days"] < DIRECTIONAL_MARKET_DAY_TARGET:
        blockers.append("lack_of_full_coverage_support")
    if support["full_checkpoint_holdout_market_days"] < HOLDOUT_MARKET_DAY_TARGET:
        blockers.append("lack_of_holdout_support")
    if shadow_rows and any(not row.get("decision_trace_id") for row in shadow_rows):
        blockers.append("shadow_trace_incomplete")
    if not shadow_rows:
        blockers.append("no_forward_shadow_rows")
    return blockers


def build_baseline_model_card(
    *,
    historical_path: Path,
    shadow_path: Path,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or _utc_now()
    historical_rows = _read_jsonl(historical_path)
    shadow_rows = _read_jsonl(shadow_path)
    all_rows = historical_rows + shadow_rows
    support = _support_summary(historical_rows)
    blockers = _promotion_blockers(historical_rows, shadow_rows)
    return {
        "model_card_version": MODEL_CARD_VERSION,
        "generated_at": generated.isoformat(),
        "status": "baseline_ready" if all_rows else "missing_artifacts",
        "read_only": True,
        "inputs": {
            "historical_path": str(historical_path),
            "shadow_path": str(shadow_path),
            "historical_file_exists": historical_path.exists(),
            "shadow_file_exists": shadow_path.exists(),
        },
        "row_counts": {
            "historical": len(historical_rows),
            "shadow": len(shadow_rows),
            "total": len(all_rows),
        },
        "historical": {
            "coverage_class_counts": _counter(historical_rows, _coverage_class),
            "split_counts": _counter(historical_rows, _split),
            "support": support,
            "calibration": _calibration(historical_rows),
            "policy": _policy_summary(historical_rows),
            "pnl": _pnl_summary(historical_rows),
        },
        "shadow": {
            "trace_count": sum(1 for row in shadow_rows if row.get("decision_trace_id")),
            "trace_coverage": round(
                sum(1 for row in shadow_rows if row.get("decision_trace_id")) / len(shadow_rows),
                4,
            )
            if shadow_rows
            else 0.0,
            "series_counts": _counter(shadow_rows, lambda row: _market_ticker(row).split("-")[0] or "unknown"),
            "calibration": _calibration(shadow_rows),
            "policy": _policy_summary(shadow_rows),
            "pnl": _pnl_summary(shadow_rows),
        },
        "promotion_blocked": bool(blockers),
        "promotion_blockers": blockers,
    }


def write_baseline_model_card(
    *,
    historical_path: Path,
    shadow_path: Path,
    output_path: Path,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    card = build_baseline_model_card(
        historical_path=historical_path,
        shadow_path=shadow_path,
        generated_at=generated_at,
    )
    _write_json(output_path, card)
    return {**card, "output": str(output_path)}
