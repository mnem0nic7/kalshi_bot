from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.services.modeling import build_modeling_report, build_shadow_modeling_payload
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.services.trade_analysis import TradeAnalysisDataset


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)


def _row(
    idx: int,
    *,
    fair_yes: float,
    kalshi_result: str,
    label_win: bool,
    net_pnl: str,
    bucket_key: str,
    side: str = "yes",
) -> dict:
    return {
        "training_eligible": True,
        "decision_ts": (NOW + timedelta(days=idx)).isoformat(),
        "room_id": f"room-{idx}",
        "market_ticker": "KXHIGHNY-26MAY03-T70",
        "series_ticker": "KXHIGHNY",
        "city": "NY",
        "market_day": (NOW + timedelta(days=idx)).date().isoformat(),
        "settlement_ts": (NOW + timedelta(days=idx, hours=1)).isoformat(),
        "fair_yes_dollars": f"{fair_yes:.4f}",
        "kalshi_result": kalshi_result,
        "label_win": label_win,
        "lifecycle_net_pnl": net_pnl,
        "lifecycle_net_pnl_dollars": net_pnl,
        "bucket_key": bucket_key,
        "side": side,
        "strategy_code": "A",
        "ticket_yes_price_dollars": "0.5000",
        "ticket_count_fp": "1.00",
        "yes_bid_dollars": "0.5500",
        "yes_ask_dollars": "0.4500",
        "market_stale_seconds": 5,
        "market_stale_threshold_seconds": 60,
        "confidence": 0.7,
        "forecast_delta_f": 5.0,
        "market_spread_bps": 100,
        "entry_price_band": "50-59c",
        "forecast_delta_band": ">=5f",
        "confidence_band": "high",
        "spread_band": "100-249bps",
    }


def _modeling_rows() -> list[dict]:
    rows: list[dict] = []
    good_bucket = "KXHIGHNY|NY|yes|A|50-59c|delta:>=5f|conf:high|spread:100-249bps"
    bad_bucket = "KXHIGHTDAL|TDAL|no|A|40-49c|delta:0-2f|conf:medium|spread:250-499bps"
    for idx in range(30):
        if idx < 21:
            bucket = good_bucket if idx % 2 == 0 else bad_bucket
            net = "2.0000" if bucket == good_bucket else "-1.5000"
            label_win = bucket == good_bucket
        else:
            bucket = good_bucket if idx % 2 == 0 else bad_bucket
            net = "1.5000" if bucket == good_bucket else "-3.0000"
            label_win = bucket == good_bucket
        yes_outcome = bucket == good_bucket
        rows.append(
            _row(
                idx,
                fair_yes=0.80,
                kalshi_result="yes" if yes_outcome else "no",
                label_win=label_win,
                net_pnl=net,
                bucket_key=bucket,
                side="yes" if bucket == good_bucket else "no",
            )
        )
    return rows


class FakeDecisionCorpus:
    async def current(self, *, kalshi_env: str):
        return {
            "status": "ok",
            "kalshi_env": kalshi_env,
            "build": {"id": "build-1", "status": "successful", "row_count": 30},
        }


class FakeMissingDecisionCorpus:
    async def current(self, *, kalshi_env: str):
        return {"status": "missing", "kalshi_env": kalshi_env, "build": None}


class FakeAnalysis:
    async def build_dataset(self, *, kalshi_env: str, days: int, now=None, limit=None):
        rows = _modeling_rows()
        return TradeAnalysisDataset(rows=rows, summary={"row_count": len(rows)})


class FakeSparseAnalysis:
    async def build_dataset(self, *, kalshi_env: str, days: int, now=None, limit=None):
        rows = _modeling_rows()[:5]
        return TradeAnalysisDataset(rows=rows, summary={"row_count": len(rows)})


class FakeAudit:
    async def build_report(self, *, kalshi_env: str, days: int, focus: str = "money-safety"):
        return {
            "issues": [],
            "lifecycle": {
                "buckets": [
                    {
                        "bucket_key": "KXHIGHNY|NY|yes|A|50-59c|delta:>=5f|conf:high|spread:100-249bps",
                        "bucket_sample_count": 12,
                        "bucket_net_pnl": "18.0000",
                        "lifecycle_net_pnl": "18.0000",
                        "bucket_win_rate": 1.0,
                    },
                    {
                        "bucket_key": "KXHIGHTDAL|TDAL|no|A|40-49c|delta:0-2f|conf:medium|spread:250-499bps",
                        "bucket_sample_count": 10,
                        "bucket_net_pnl": "-15.0000",
                        "lifecycle_net_pnl": "-15.0000",
                        "bucket_win_rate": 0.0,
                    },
                ],
            },
        }


@pytest.mark.asyncio
async def test_modeling_report_runs_two_stage_loop_and_marks_shadow_promotable() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./modeling-test.db",
        trade_behavior_production_entry_freeze_enabled=True,
        trade_behavior_empirical_gate_min_settled_fills=2,
    )

    report = await build_modeling_report(
        settings=settings,
        decision_corpus_service=FakeDecisionCorpus(),
        trading_audit_service=FakeAudit(),
        trade_analysis_service=FakeAnalysis(),
        kalshi_env="demo",
        days=30,
        command="validate",
    )

    assert report["status"] == "pass"
    prediction = report["prediction_calibration"]
    assert prediction["status"] == "ok"
    assert prediction["promotion_ready"] is True
    assert prediction["calibrated"]["brier"] < prediction["baseline"]["brier"]
    selection = report["trade_selection"]
    assert selection["status"] == "ok"
    assert selection["promotion_ready"] is True
    assert selection["selected_path"]["net_pnl"] == "2.6500"
    assert report["bucket_matrix_summary"]["eligible_for_live_review_count"] == 1
    assert report["bucket_matrix_summary"]["negative_actual_pnl_count"] == 1


@pytest.mark.asyncio
async def test_modeling_validate_warns_on_missing_corpus_and_sparse_data() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./modeling-sparse-test.db",
        trade_behavior_production_entry_freeze_enabled=True,
    )

    report = await build_modeling_report(
        settings=settings,
        decision_corpus_service=FakeMissingDecisionCorpus(),
        trading_audit_service=FakeAudit(),
        trade_analysis_service=FakeSparseAnalysis(),
        kalshi_env="demo",
        days=7,
        command="validate",
    )

    assert report["status"] == "warn"
    codes = {issue["code"] for issue in report["issues"]}
    assert "decision_corpus_missing_or_stale" in codes
    assert "prediction_insufficient_data" in codes
    assert "trade_selection_insufficient_data" in codes


@pytest.mark.asyncio
async def test_modeling_validate_fails_when_production_freeze_is_disabled() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./modeling-production-freeze-test.db",
        trade_behavior_production_entry_freeze_enabled=False,
    )

    report = await build_modeling_report(
        settings=settings,
        decision_corpus_service=FakeDecisionCorpus(),
        trading_audit_service=FakeAudit(),
        trade_analysis_service=FakeSparseAnalysis(),
        kalshi_env="production",
        days=7,
        command="validate",
    )

    assert report["status"] == "fail"
    assert "production_entry_freeze_disabled" in {issue["code"] for issue in report["issues"]}


def test_shadow_modeling_payload_is_score_only_and_does_not_block_demo() -> None:
    signal = StrategySignal(
        fair_yes_dollars="0.6200",
        confidence=0.7,
        edge_bps=300,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars="0.5000",
        summary="test",
    )

    payload = build_shadow_modeling_payload(
        signal=signal,
        kalshi_env="demo",
        shadow_mode=False,
        bucket_key="bucket-1",
        empirical_gate={
            "status": "shadow_only",
            "reason": "empirical_gate_under_sampled",
            "actual_sample_count": 1,
        },
        generated_at=NOW,
    )

    assert payload["shadow_only"] is True
    assert payload["prediction_model"]["closed_form_fair_yes"] == 0.62
    assert payload["prediction_model"]["calibrated_fair_yes"] == 0.62
    assert payload["trade_selection_model"]["status"] == "under_sampled"
    assert payload["trade_selection_model"]["shadow_recommendation"] == "score_only"


def test_shadow_modeling_payload_keeps_production_blocked_by_freeze_label() -> None:
    signal = StrategySignal(
        fair_yes_dollars="0.6200",
        confidence=0.7,
        edge_bps=300,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars="0.5000",
        summary="test",
    )

    payload = build_shadow_modeling_payload(
        signal=signal,
        kalshi_env="production",
        shadow_mode=False,
        bucket_key="bucket-1",
        empirical_gate={"status": "blocked", "reason": "trade_behavior_retraining_freeze"},
        generated_at=NOW,
    )

    assert payload["trade_selection_model"]["status"] == "production_frozen"
    assert payload["trade_selection_model"]["shadow_recommendation"] == "blocked_by_freeze"
