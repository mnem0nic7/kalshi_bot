from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.backtesting import (
    build_backtesting_report,
    build_walk_forward_folds,
    normalize_decision_corpus_row,
    normalize_trade_analysis_row,
    row_diagnostic,
    simulate_decision_row,
)
from kalshi_bot.services.trade_analysis import TradeAnalysisDataset


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)


def _settings(**overrides) -> Settings:
    values = {
        "database_url": "sqlite+aiosqlite:///./backtesting-test.db",
        "trade_behavior_production_entry_freeze_enabled": True,
        "trade_behavior_empirical_gate_min_settled_fills": 2,
    }
    values.update(overrides)
    return Settings(**values)


def _decision_row(
    idx: int = 0,
    *,
    side: str = "yes",
    target_yes: str = "0.6000",
    yes_bid: str = "0.4500",
    yes_ask: str = "0.5500",
    no_ask: str | None = None,
    settlement_result: str = "yes",
    leakage_status: str = "point_in_time",
    actual_net_pnl: str | None = None,
    counterfactual_pnl: str | None = None,
    fair_yes: str = "0.6500",
) -> dict:
    decision_ts = NOW + timedelta(days=idx)
    return {
        "row_id": f"row-{idx}",
        "source": "unit",
        "kalshi_env": "demo",
        "market_ticker": f"KXHIGHNY-26MAY{idx + 3:02d}-T70",
        "series_ticker": "KXHIGHNY",
        "city": "NY",
        "market_day": decision_ts.date().isoformat(),
        "decision_ts": decision_ts,
        "settlement_ts": decision_ts + timedelta(hours=1),
        "strategy_code": "unit",
        "side": side,
        "target_yes_price_dollars": Decimal(target_yes),
        "count_fp": Decimal("1.00"),
        "yes_bid_dollars": Decimal(yes_bid),
        "yes_ask_dollars": Decimal(yes_ask),
        "no_ask_dollars": Decimal(no_ask) if no_ask is not None else None,
        "market_stale_seconds": 5,
        "market_stale_threshold_seconds": 60,
        "fair_yes_dollars": Decimal(fair_yes),
        "settlement_result": settlement_result,
        "actual_net_pnl": Decimal(actual_net_pnl) if actual_net_pnl is not None else None,
        "counterfactual_pnl": Decimal(counterfactual_pnl) if counterfactual_pnl is not None else None,
        "leakage_status": leakage_status,
        "exclusion_reason": None if leakage_status == "point_in_time" else "market_snapshot_high_leakage",
    }


def test_simulator_accounts_yes_no_sides_with_fees() -> None:
    settings = _settings()

    yes_result = simulate_decision_row(_decision_row(side="yes"), settings=settings)
    no_result = simulate_decision_row(
        _decision_row(side="no", target_yes="0.4000", yes_bid="0.4500", settlement_result="no"),
        settings=settings,
    )

    assert yes_result["execution_model_status"] == "fillable"
    assert str(yes_result["gross_pnl"]) == "0.4500"
    assert str(yes_result["fees"]) == "0.0200"
    assert str(yes_result["net_pnl"]) == "0.4300"
    assert no_result["execution_model_status"] == "fillable"
    assert str(no_result["execution_price_dollars"]) == "0.5500"
    assert str(no_result["net_pnl"]) == "0.4300"


def test_simulator_rejects_unfillable_stale_and_high_leakage_rows() -> None:
    settings = _settings()
    unfillable = simulate_decision_row(
        _decision_row(side="yes", target_yes="0.5000", yes_ask="0.6000"),
        settings=settings,
    )
    stale_row = _decision_row()
    stale_row["market_stale_seconds"] = 90
    stale = simulate_decision_row(stale_row, settings=settings)
    high_leakage = simulate_decision_row(
        _decision_row(leakage_status="high_leakage"),
        settings=settings,
    )

    assert unfillable["execution_model_status"] == "unfillable"
    assert stale["execution_model_status"] == "stale_quote"
    assert high_leakage["execution_model_status"] == "diagnostics_only"


def test_negative_actual_evidence_overrides_optimistic_counterfactual() -> None:
    diagnostic = row_diagnostic(
        _decision_row(actual_net_pnl="-2.0000", counterfactual_pnl="1.2500"),
        settings=_settings(),
    )

    assert diagnostic["actual_evidence_status"] == "negative_actual_overrides_counterfactual"
    assert diagnostic["effective_net_pnl"] == "-2.0000"


def test_walk_forward_folds_require_point_in_time_rows_resolved_before_cutoff() -> None:
    rows = [_decision_row(idx) for idx in range(24)]
    unresolved_train = _decision_row(0)
    unresolved_train["row_id"] = "late-settlement"
    unresolved_train["market_day"] = (NOW + timedelta(days=0)).date().isoformat()
    unresolved_train["decision_ts"] = NOW
    unresolved_train["settlement_ts"] = NOW + timedelta(days=30)
    rows.append(unresolved_train)
    rows.append(_decision_row(24))

    folds = build_walk_forward_folds(rows, min_train_rows=20)

    assert folds
    first_fold = folds[0]
    assert first_fold["train_cutoff_market_day"] == (NOW + timedelta(days=20)).date().isoformat()
    assert all(row["settlement_ts"] <= datetime.fromisoformat(first_fold["train_cutoff_ts"]) for row in first_fold["train_rows"])
    assert "late-settlement" not in {row["row_id"] for row in first_fold["train_rows"]}


def test_normalizers_share_simulator_shape_for_trade_analysis_and_decision_corpus() -> None:
    trade_row = normalize_trade_analysis_row(
        {
            "training_eligible": True,
            "room_id": "room-1",
            "market_ticker": "KXHIGHNY-26MAY03-T70",
            "decision_ts": NOW.isoformat(),
            "settlement_ts": (NOW + timedelta(hours=1)).isoformat(),
            "kalshi_result": "yes",
            "side": "yes",
            "ticket_yes_price_dollars": "0.6000",
            "yes_bid_dollars": "0.5400",
            "yes_ask_dollars": "0.5500",
            "fair_yes_dollars": "0.6500",
            "lifecycle_net_pnl": "0.4300",
        }
    )
    corpus_row = normalize_decision_corpus_row(
        SimpleNamespace(
            id="corpus-row-1",
            room_id="room-2",
            market_ticker="KXHIGHNY-26MAY04-T70",
            series_ticker="KXHIGHNY",
            station_id="NY",
            local_market_day="2026-05-04",
            checkpoint_ts=NOW,
            kalshi_env="demo",
            policy_version="unit",
            recommended_side="yes",
            target_yes_price_dollars=Decimal("0.6000"),
            quote_snapshot={"yes_bid_dollars": "0.5400", "yes_ask_dollars": "0.5500"},
            fair_yes_dollars=Decimal("0.6500"),
            confidence=0.7,
            edge_bps=500,
            diagnostics={},
            settlement_result="yes",
            settlement_value_dollars=Decimal("1.0000"),
            pnl_executed_realized=Decimal("0.4300"),
            pnl_counterfactual_target_with_fees=Decimal("0.4300"),
            counterfactual_count=Decimal("1.00"),
            source_provenance="historical_replay_full_checkpoint",
            settlement_payload={"settlement_ts": (NOW + timedelta(hours=1)).isoformat()},
            eligibility_status="eligible",
            liquidity_regime="tight",
        )
    )

    for row in (trade_row, corpus_row):
        assert row["leakage_status"] == "point_in_time"
        assert row["side"] == "yes"
        assert row["target_yes_price_dollars"] == Decimal("0.6000")
        assert row["yes_ask_dollars"] == Decimal("0.5500")


class FakeCorpus:
    def __init__(self, status: str = "ok") -> None:
        self.status = status

    async def current(self, *, kalshi_env: str):
        if self.status != "ok":
            return {"status": "missing", "kalshi_env": kalshi_env, "build": None}
        return {"status": "ok", "kalshi_env": kalshi_env, "build": {"id": "build-1", "status": "successful"}}


class FakeAnalysis:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def build_dataset(self, *, kalshi_env: str, days: int, now=None, limit=None):
        rows = self.rows[:limit] if limit is not None else self.rows
        return TradeAnalysisDataset(rows=rows, summary={"row_count": len(rows)})


@pytest.mark.asyncio
async def test_backtesting_validate_fails_when_production_freeze_disabled() -> None:
    report = await build_backtesting_report(
        settings=_settings(trade_behavior_production_entry_freeze_enabled=False),
        session_factory=None,
        decision_corpus_service=FakeCorpus("ok"),
        trade_analysis_service=FakeAnalysis([_decision_row(idx) for idx in range(25)]),
        kalshi_env="production",
        days=30,
        command="validate",
        dataset_source="trade-analysis",
        row_limit=None,
    )

    assert report["status"] == "fail"
    assert "production_entry_freeze_disabled" in {issue["code"] for issue in report["issues"]}


@pytest.mark.asyncio
async def test_backtesting_full_history_validate_fails_on_missing_current_corpus() -> None:
    report = await build_backtesting_report(
        settings=_settings(),
        session_factory=None,
        decision_corpus_service=FakeCorpus("missing"),
        trade_analysis_service=FakeAnalysis([_decision_row(idx) for idx in range(25)]),
        kalshi_env="demo",
        days=3650,
        full_history=True,
        command="validate",
        dataset_source="trade-analysis",
        row_limit=None,
    )

    assert report["status"] == "fail"
    assert "decision_corpus_missing_or_stale" in {issue["code"] for issue in report["issues"]}
