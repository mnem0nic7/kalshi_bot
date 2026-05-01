from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from kalshi_bot.services.baseline_model_card import build_baseline_model_card, write_baseline_model_card


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")


def _historical_row(
    day: int,
    *,
    coverage_class: str = "full_checkpoint_coverage",
    split: str = "train",
    label: str | None = "yes",
    prediction: str = "0.7500",
) -> dict:
    signature = {}
    if label is not None:
        signature = {
            "kalshi_result": label,
            "settlement_value_dollars": "1.0000" if label == "yes" else "0.0000",
        }
    return {
        "room_id": f"hist-{day}",
        "market_ticker": f"KXHIGHNY-26APR{day:02d}-T70",
        "room_origin": "historical_replay",
        "split": split,
        "coverage_class": coverage_class,
        "historical_provenance": {
            "local_market_day": f"2026-04-{day:02d}",
            "coverage_class": coverage_class,
            "settlement_label_signature": json.dumps(signature) if signature else None,
        },
        "signal": {"fair_yes_dollars": prediction},
        "outcome": {
            "final_status": "stand_down",
            "stand_down_reason": "no_actionable_edge",
            "ticket_generated": False,
            "orders_submitted": 0,
        },
    }


def _shadow_row(idx: int, *, trace: bool = True, label: str | None = "no") -> dict:
    settlement = None if label is None else {"settlement_value_dollars": "1.0000" if label == "yes" else "0.0000"}
    return {
        "export_version": "room-bundle.v1",
        "room_origin": "shadow",
        "room": {
            "id": f"shadow-{idx}",
            "market_ticker": f"KXHIGHCHI-26MAY{idx:02d}-T66",
            "room_origin": "shadow",
            "shadow_mode": True,
            "stage": "complete",
        },
        "decision_trace_id": f"trace-{idx}" if trace else None,
        "signal": {"fair_yes_dollars": "0.2500"},
        "settlement": settlement,
        "counterfactual_pnl_dollars": "0.5000",
        "outcome": {
            "final_status": "stand_down",
            "stand_down_reason": "spread_too_wide",
            "risk_status": "blocked",
            "ticket_generated": True,
            "orders_submitted": 0,
        },
    }


def test_baseline_model_card_mixed_inputs_reports_counts_metrics_and_blockers(tmp_path) -> None:
    historical = tmp_path / "historical.jsonl"
    shadow = tmp_path / "shadow.jsonl"
    _write_jsonl(historical, [_historical_row(1, label="yes"), _historical_row(2, coverage_class="partial_checkpoint_coverage", label="no")])
    _write_jsonl(shadow, [_shadow_row(1, trace=True), _shadow_row(2, trace=False)])

    card = build_baseline_model_card(historical_path=historical, shadow_path=shadow, generated_at=NOW)

    assert card["status"] == "baseline_ready"
    assert card["row_counts"] == {"historical": 2, "shadow": 2, "total": 4}
    assert card["historical"]["coverage_class_counts"]["full_checkpoint_coverage"] == 1
    assert card["shadow"]["trace_coverage"] == 0.5
    assert card["historical"]["calibration"]["scored_count"] == 2
    assert card["shadow"]["pnl"]["total_counterfactual_pnl_dollars"] == "1.0000"
    assert card["promotion_blocked"] is True
    assert "shadow_trace_incomplete" in card["promotion_blockers"]
    assert "lack_of_execution_support" in card["promotion_blockers"]


def test_baseline_model_card_handles_historical_only(tmp_path) -> None:
    historical = tmp_path / "historical.jsonl"
    shadow = tmp_path / "missing-shadow.jsonl"
    _write_jsonl(historical, [_historical_row(1)])

    card = build_baseline_model_card(historical_path=historical, shadow_path=shadow, generated_at=NOW)

    assert card["row_counts"]["historical"] == 1
    assert card["row_counts"]["shadow"] == 0
    assert "no_forward_shadow_rows" in card["promotion_blockers"]


def test_baseline_model_card_handles_shadow_only(tmp_path) -> None:
    historical = tmp_path / "missing-historical.jsonl"
    shadow = tmp_path / "shadow.jsonl"
    _write_jsonl(shadow, [_shadow_row(1)])

    card = build_baseline_model_card(historical_path=historical, shadow_path=shadow, generated_at=NOW)

    assert card["row_counts"]["historical"] == 0
    assert card["row_counts"]["shadow"] == 1
    assert "lack_of_full_coverage_support" in card["promotion_blockers"]


def test_baseline_model_card_handles_missing_labels(tmp_path) -> None:
    historical = tmp_path / "historical.jsonl"
    shadow = tmp_path / "shadow.jsonl"
    _write_jsonl(historical, [_historical_row(1, label=None)])
    _write_jsonl(shadow, [_shadow_row(1, label=None)])

    card = build_baseline_model_card(historical_path=historical, shadow_path=shadow, generated_at=NOW)

    assert card["historical"]["calibration"] == {"scored_count": 0, "brier": None, "ece": None}
    assert card["shadow"]["calibration"] == {"scored_count": 0, "brier": None, "ece": None}


def test_baseline_model_card_allows_unblocked_status_when_support_targets_and_shadow_trace_pass(tmp_path) -> None:
    historical = tmp_path / "historical.jsonl"
    shadow = tmp_path / "shadow.jsonl"
    rows = []
    for day in range(1, 61):
        coverage = "full_checkpoint_coverage" if day <= 30 else "partial_checkpoint_coverage"
        split = "holdout" if day <= 7 else "train"
        rows.append(_historical_row(day, coverage_class=coverage, split=split))
    _write_jsonl(historical, rows)
    _write_jsonl(shadow, [_shadow_row(1, trace=True)])

    output = tmp_path / "card.json"
    card = write_baseline_model_card(
        historical_path=historical,
        shadow_path=shadow,
        output_path=output,
        generated_at=NOW,
    )

    assert output.exists()
    assert card["promotion_blocked"] is False
    assert card["promotion_blockers"] == []
    assert card["output"] == str(output)
