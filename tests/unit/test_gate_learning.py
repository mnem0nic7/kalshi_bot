from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from kalshi_bot.cli import build_parser
from kalshi_bot.config import Settings
from kalshi_bot.services.gate_learning import (
    GateLearningService,
    GateLearningRow,
    _score_episodes,
    _walk_forward_split,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _bundle(
    idx: int,
    *,
    pnl: str,
    market_day: str | None = None,
    entry_price: str = "0.0500",
    qa_edge: int = 2000,
    spread_bps: int = 100,
    confidence: float = 0.82,
    forecast_delta_f: float = 9.0,
    current_gate_passed: bool = False,
    stand_down_reason: str = "below_min_contract_price",
    stale_data_mismatch: bool = False,
) -> dict:
    day = market_day or f"2026-01-{idx:02d}"
    ts = f"{day}T18:00:00+00:00"
    ticker = f"KXHIGHNY-26JAN{idx:02d}-T70"
    return {
        "room_id": f"room-{idx}",
        "market_ticker": ticker,
        "replay_checkpoint_ts": ts,
        "historical_provenance": {"local_market_day": day},
        "settlement_label": {"series_ticker": "KXHIGHNY", "local_market_day": day},
        "counterfactual_pnl_dollars": pnl,
        "signal": {
            "market_ticker": ticker,
            "edge_bps": qa_edge + 25,
            "confidence": confidence,
            "payload": {
                "recommended_side": "yes",
                "target_yes_price_dollars": entry_price,
                "forecast_delta_f": forecast_delta_f,
                "trade_regime": "standard",
                "confidence_band": "high",
                "eligibility": {
                    "eligible": current_gate_passed,
                    "stand_down_reason": None if current_gate_passed else stand_down_reason,
                    "market_spread_bps": spread_bps,
                    "remaining_payout_dollars": "0.9500",
                    "edge_after_quality_buffer_bps": qa_edge,
                },
                "candidate_trace": {
                    "selected_side": "yes",
                    "baseline_block_reason": None if current_gate_passed else stand_down_reason,
                    "candidates": [
                        {
                            "side": "yes",
                            "status": "selected",
                            "reason": "selected_best_quality_adjusted_edge",
                            "traded_price_dollars": entry_price,
                            "target_yes_price_dollars": entry_price,
                            "edge_bps": qa_edge + 25,
                            "quality_adjusted_edge_bps": qa_edge,
                            "spread_bps": spread_bps,
                            "remaining_payout_dollars": "0.9500",
                        }
                    ],
                },
            },
        },
        "outcome": {
            "eligibility_passed": current_gate_passed,
            "ticket_generated": current_gate_passed,
            "stand_down_reason": None if current_gate_passed else stand_down_reason,
            "blocked_by": None if current_gate_passed else "eligibility",
        },
        "strategy_audit": {
            "eligibility_passed": current_gate_passed,
            "stand_down_reason": None if current_gate_passed else stand_down_reason,
            "blocked_by": None if current_gate_passed else "eligibility",
            "stale_data_mismatch": stale_data_mismatch,
            "trade_regime": "standard",
        },
    }


def _settings() -> Settings:
    return Settings(database_url="sqlite+aiosqlite:///./test.db")


def test_gate_learning_extracts_bundle_fields(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    _write_jsonl(path, [_bundle(1, pnl="4.2500", entry_price="0.0800", qa_edge=1775, forecast_delta_f=6.0)])

    service = GateLearningService(_settings(), historical_paths=(path,), forward_shadow_paths=())
    rows = service.load_bundle_rows(source="historical")

    assert len(rows) == 1
    row = rows[0]
    assert row.market_ticker == "KXHIGHNY-26JAN01-T70"
    assert row.source == "historical"
    assert row.entry_price is not None
    assert str(row.entry_price) == "0.0800"
    assert row.quality_adjusted_edge_bps == 1775
    assert row.remaining_payout_bps == 9500
    assert row.primary_block_reason == "below_min_contract_price"
    assert row.counterfactual_pnl_dollars is not None
    assert str(row.counterfactual_pnl_dollars) == "4.2500"


def test_episode_scoring_counts_repeated_market_snapshots_once() -> None:
    rows = [
        GateLearningRow(
            source="unit",
            room_id=f"room-{idx}",
            market_ticker="KXHIGHNY-26MAY10-T70",
            decision_time=datetime(2026, 5, 10, 12 + idx, tzinfo=UTC),
            market_day="2026-05-10",
            series_ticker="KXHIGHNY",
            side="yes",
            entry_price=None if idx == 0 else None,
            quality_adjusted_edge_bps=2000,
            counterfactual_pnl_dollars=Decimal("1.0000"),
        )
        for idx in range(3)
    ]

    score = _score_episodes(rows, lambda row: True)

    assert score["episode_count"] == 1
    assert score["sample_count"] == 1
    assert score["net_pnl"] == Decimal("1.0000")


def test_gate_learning_merges_sparse_outcome_with_rich_decision_row(tmp_path) -> None:
    sparse_path = tmp_path / "historical_outcome.jsonl"
    rich_path = tmp_path / "historical_decision.jsonl"
    sparse = {
        "room_id": "room-1",
        "market_ticker": "KXHIGHNY-26JAN01-T70",
        "replay_checkpoint_ts": "2026-01-01T18:00:00+00:00",
        "historical_provenance": {"local_market_day": "2026-01-01"},
        "settlement_label": {"series_ticker": "KXHIGHNY", "local_market_day": "2026-01-01", "crosscheck_result": "yes"},
        "counterfactual_pnl_dollars": "7.5000",
        "outcome": {"eligibility_passed": False, "stand_down_reason": "below_min_contract_price"},
        "strategy_audit": {"eligibility_passed": False, "stand_down_reason": "below_min_contract_price"},
    }
    rich = _bundle(1, pnl="", entry_price="0.0800", qa_edge=1775, forecast_delta_f=6.0)
    rich.pop("counterfactual_pnl_dollars")
    _write_jsonl(sparse_path, [sparse])
    _write_jsonl(rich_path, [rich])

    service = GateLearningService(_settings(), historical_paths=(sparse_path, rich_path), forward_shadow_paths=())
    rows = service.load_bundle_rows(source="historical")

    assert len(rows) == 1
    row = rows[0]
    assert row.counterfactual_pnl_dollars is not None
    assert str(row.counterfactual_pnl_dollars) == "7.5000"
    assert row.entry_price is not None
    assert str(row.entry_price) == "0.0800"
    assert row.quality_adjusted_edge_bps == 1775
    assert row.forecast_delta_f == 6.0
    assert row.settlement_result == "yes"


def test_walk_forward_split_uses_later_market_days_for_holdout() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        GateLearningRow(
            source="test",
            room_id=f"room-{idx}",
            market_ticker=f"M-{idx}",
            decision_time=base + timedelta(days=idx),
            market_day=f"2026-01-{idx + 1:02d}",
            counterfactual_pnl_dollars=None,
        )
        for idx in range(10)
    ]

    train, holdout = _walk_forward_split(rows)

    assert train
    assert holdout
    assert max(row.market_day for row in train if row.market_day) < min(row.market_day for row in holdout if row.market_day)


@pytest.mark.asyncio
async def test_threshold_sweep_recommends_profitable_low_price_high_edge_policy(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = [_bundle(idx, pnl="10.0000", entry_price="0.0500", qa_edge=2500) for idx in range(1, 11)]
    _write_jsonl(path, rows)
    service = GateLearningService(_settings(), historical_paths=(path,), forward_shadow_paths=())

    report = await service.build_report(source="historical", min_support=4, session=None, now=datetime(2026, 1, 20, tzinfo=UTC))
    policies = {row["candidate_policy"]: row for row in report["recommendations"]}

    policy = policies["entry_price_min_0.05_and_qa_edge_min_1500_and_spread_max_1200"]
    assert policy["recommendation"] == "candidate_improves_holdout_net_pnl"
    assert policy["support_count"] == 10
    assert policy["holdout_net_pnl"] == "30.0000"
    assert policy["net_improvement_dollars"] == "30.0000"
    assert report["regret_rows"]


@pytest.mark.asyncio
async def test_threshold_sweep_keeps_gate_when_candidate_loses_on_holdout(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = [_bundle(idx, pnl="-3.0000", entry_price="0.0500", qa_edge=2500) for idx in range(1, 11)]
    _write_jsonl(path, rows)
    service = GateLearningService(_settings(), historical_paths=(path,), forward_shadow_paths=())

    report = await service.build_report(source="historical", min_support=4, session=None, now=datetime(2026, 1, 20, tzinfo=UTC))
    policies = {row["candidate_policy"]: row for row in report["recommendations"]}

    policy = policies["entry_price_min_0.05_and_qa_edge_min_1500_and_spread_max_1200"]
    assert policy["recommendation"] == "keep_current_gate"
    assert policy["holdout_net_pnl"] == "-9.0000"
    assert policy["blocked_loss_dollars"] == "0.0000"
    assert report["saved_loss_rows"]


@pytest.mark.asyncio
async def test_low_support_produces_insufficient_evidence(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    _write_jsonl(path, [_bundle(1, pnl="10.0000", entry_price="0.0500", qa_edge=2500)])
    service = GateLearningService(_settings(), historical_paths=(path,), forward_shadow_paths=())

    report = await service.build_report(source="historical", min_support=30, session=None, now=datetime(2026, 1, 20, tzinfo=UTC))

    assert report["recommendations"][0]["recommendation"] == "insufficient_evidence"
    assert report["confidence_warnings"]


@pytest.mark.asyncio
async def test_gate_recommendation_promotes_only_comparable_supported_gate(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = [_bundle(idx, pnl="1.0000", entry_price="0.0500", qa_edge=900) for idx in range(1, 41)]
    _write_jsonl(path, rows)
    service = GateLearningService(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            risk_min_contract_price_dollars=0.25,
            risk_min_edge_bps=500,
        ),
        historical_paths=(path,),
        forward_shadow_paths=(),
    )

    report = await service.build_recommendation_report(
        source="historical",
        min_support=30,
        now=datetime(2026, 2, 20, tzinfo=UTC),
        days=3650,
    )

    price = report["recommended_settings"]["risk_min_contract_price_dollars"]
    assert price["changed"] is True
    assert price["recommended"] == 0.05
    evidence = next(
        row for row in report["gate_evidence"]
        if row["config_field"] == "risk_min_contract_price_dollars" and row["threshold"] == 0.05
    )
    assert evidence["feature_support_count"] == 40
    assert evidence["support_count"] == 40
    assert evidence["promotion_status"] == "promoted"


@pytest.mark.asyncio
async def test_gate_recommendation_scores_candidate_as_full_policy(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = [_bundle(idx, pnl="5.0000", entry_price="0.0500", qa_edge=400) for idx in range(1, 41)]
    _write_jsonl(path, rows)
    service = GateLearningService(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            risk_min_contract_price_dollars=0.25,
            risk_min_edge_bps=500,
        ),
        historical_paths=(path,),
        forward_shadow_paths=(),
    )

    report = await service.build_recommendation_report(
        source="historical",
        min_support=30,
        now=datetime(2026, 2, 20, tzinfo=UTC),
        days=3650,
    )

    price = report["recommended_settings"]["risk_min_contract_price_dollars"]
    evidence = next(
        row for row in report["gate_evidence"]
        if row["config_field"] == "risk_min_contract_price_dollars" and row["threshold"] == 0.05
    )
    assert price["changed"] is False
    assert evidence["support_count"] == 0
    assert evidence["promotion_status"] == "insufficient_evidence"


@pytest.mark.asyncio
async def test_gate_recommendation_keeps_gate_when_drawdown_worsens(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = []
    for idx in range(1, 41):
        pnl = "-5.0000" if idx == 32 else "2.0000"
        rows.append(_bundle(idx, pnl=pnl, entry_price="0.0500", qa_edge=900))
    _write_jsonl(path, rows)
    service = GateLearningService(
        Settings(database_url="sqlite+aiosqlite:///./test.db", risk_min_contract_price_dollars=0.25),
        historical_paths=(path,),
        forward_shadow_paths=(),
    )

    report = await service.build_recommendation_report(
        source="historical",
        min_support=30,
        now=datetime(2026, 2, 20, tzinfo=UTC),
        days=3650,
    )

    price = report["recommended_settings"]["risk_min_contract_price_dollars"]
    assert price["changed"] is False
    assert price["reason"] == "holdout_drawdown_worse"


@pytest.mark.asyncio
async def test_gate_recommendation_maps_all_baseline_fields(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    rows = [_bundle(idx, pnl="1.0000", entry_price="0.2500", qa_edge=900) for idx in range(1, 41)]
    _write_jsonl(path, rows)
    service = GateLearningService(_settings(), historical_paths=(path,), forward_shadow_paths=())

    report = await service.build_recommendation_report(
        source="historical",
        min_support=30,
        now=datetime(2026, 2, 20, tzinfo=UTC),
        days=3650,
    )

    assert set(report["recommended_settings"]) == {
        "risk_min_contract_price_dollars",
        "strategy_min_remaining_payout_bps",
        "trigger_max_spread_bps",
        "risk_min_confidence",
        "risk_min_edge_bps",
        "strategy_min_abs_delta_f",
        "risk_max_credible_edge_bps",
    }
    edge_rows = [row for row in report["gate_evidence"] if row["config_field"] == "risk_min_edge_bps"]
    assert edge_rows
    assert {row["field_name"] for row in edge_rows} == {"quality_adjusted_edge_bps"}


@pytest.mark.asyncio
async def test_gate_learning_smoke_reads_existing_training_files() -> None:
    report = await GateLearningService(_settings()).build_report(
        source="combined",
        min_support=30,
        session=None,
        now=datetime(2026, 5, 10, tzinfo=UTC),
    )

    assert report["schema_version"] == "gate-learning-report-v1"
    assert report["read_only"] is True
    assert report["row_counts"]["total_rows"] >= report["row_counts"]["labeled_rows"]
    assert "recommendations" in report


@pytest.mark.asyncio
async def test_current_training_files_do_not_promote_full_policy_gate_changes() -> None:
    report = await GateLearningService(_settings()).build_recommendation_report(
        source="combined",
        min_support=30,
        now=datetime(2026, 5, 10, tzinfo=UTC),
        days=3650,
        episode_level=True,
    )

    changed_settings = {
        field: details
        for field, details in report["recommended_settings"].items()
        if details["changed"]
    }
    promoted_evidence = [
        row for row in report["gate_evidence"]
        if row["promotion_status"] == "promoted"
    ]

    assert report["row_counts"]["labeled_rows"] == 266
    assert report["row_counts"]["episodes"] == 191
    assert changed_settings == {}
    assert promoted_evidence == []


@pytest.mark.asyncio
async def test_source_specific_report_does_not_load_decision_trace_context(tmp_path) -> None:
    path = tmp_path / "historical.jsonl"
    _write_jsonl(path, [_bundle(1, pnl="4.0000", current_gate_passed=True)])

    class GuardedService(GateLearningService):
        async def load_decision_trace_rows(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("source-specific reports should not load decision traces")

    report = await GuardedService(_settings(), historical_paths=(path,), forward_shadow_paths=()).build_report(
        source="historical",
        min_support=1,
        session=object(),  # type: ignore[arg-type]
        now=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert report["row_counts"]["sources"] == {"historical": 1}


def test_gate_learning_cli_parser_accepts_report_command() -> None:
    args = build_parser().parse_args([
        "gate-learning",
        "report",
        "--kalshi-env",
        "production",
        "--days",
        "180",
        "--format",
        "md",
        "--source",
        "combined",
    ])

    assert args.command == "gate-learning"
    assert args.gate_learning_command == "report"
    assert args.format == "md"


def test_gate_learning_cli_parser_accepts_recommend_command() -> None:
    args = build_parser().parse_args([
        "gate-learning",
        "recommend",
        "--kalshi-env",
        "production",
        "--days",
        "3650",
        "--format",
        "json",
        "--source",
        "combined",
    ])

    assert args.command == "gate-learning"
    assert args.gate_learning_command == "recommend"
    assert args.source == "combined"


def test_bundle_backed_validation_cli_options_parse() -> None:
    backtesting_args = build_parser().parse_args([
        "backtesting",
        "run",
        "--kalshi-env",
        "production",
        "--dataset-source",
        "gate-learning-bundles",
    ])
    modeling_args = build_parser().parse_args([
        "modeling",
        "backtest",
        "--kalshi-env",
        "production",
        "--source",
        "gate-learning-bundles",
    ])

    assert backtesting_args.dataset_source == "gate-learning-bundles"
    assert modeling_args.source == "gate-learning-bundles"
