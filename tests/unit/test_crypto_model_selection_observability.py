"""Observability-only tests for per-candidate model-selection projection.

These assert that the additive selection table / champion model_type are
captured in the saved-artifact ``metrics`` dict, and that recording this
projection does NOT change which champion is selected.
"""

from __future__ import annotations

from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    _crypto_champion_selection_reason,
    _crypto_model_metrics,
    _crypto_select_champion,
)


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto-sel-obs.db",
        "web_auth_enabled": False,
        "crypto_min_training_samples": 2,
    }
    values.update(overrides)
    return Settings(**values)


def _training_row(market_ticker: str, *, label: int = 1, mid: str = "0.5000") -> dict:
    return {
        "row_id": f"{market_ticker}:{label}",
        "market_ticker": market_ticker,
        "asset_symbol": "BTC",
        "frequency": "15m",
        "quote_source": "snapshot_quotes",
        "strict_trade_eligible": True,
        "spot_feature_status": "available",
        "label_yes": label,
        "mid_yes_dollars": Decimal(mid),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5100"),
        "spread_bps": 200,
        "time_to_close_seconds": 300,
        "market_day": "2026-05-01",
    }


def _market_mid_baseline_winning_candidates() -> list[dict]:
    """A market_mid_baseline fallback scenario: the only non-market candidate is
    guardrail-failed and has negative OOS P&L, so the champion is the baseline."""
    return [
        {
            "name": "market_mid_baseline",
            "status": "available",
            "metrics": {"brier": 0.0500, "log_loss": 0.20, "ece": 0.02},
            "policy_metrics": None,
            "reason": None,
        },
        {
            "name": "spot_distance_residual",
            "status": "guardrail_failed",
            "metrics": {"brier": 0.0600, "log_loss": 0.30, "ece": 0.05},
            "policy_metrics": {
                "selected_count": 4,
                "net_pnl": "-0.5000",
                "pnl_advantage_vs_market_mid_dollars": "-0.2500",
            },
            "reason": "log_loss_regressed_vs_market_mid",
        },
        {
            "name": "sklearn_logistic",
            "status": "unavailable",
            "metrics": None,
            "policy_metrics": None,
            "reason": "insufficient_rows",
        },
    ]


def _candidate_report(candidates: list[dict]) -> dict:
    min_selected_count = 1
    allow_guardrail_failed = False
    champion = _crypto_select_champion(
        candidates,
        min_selected_count=min_selected_count,
        allow_guardrail_failed_profit_candidates=allow_guardrail_failed,
    )
    champion_entry = next((c for c in candidates if c.get("name") == champion), None)
    return {
        "schema_version": "test",
        "status": "ok",
        "selection_scope": "in_sample_training_fallback",
        "min_policy_selected_count": min_selected_count,
        "guardrails": {
            "allow_guardrail_failed_profit_candidates": allow_guardrail_failed,
        },
        "fold_count": 0,
        "candidates": candidates,
        "champion_name": champion,
        "champion_status": champion_entry.get("status") if champion_entry else None,
        "champion_selection_reason": _crypto_champion_selection_reason(
            champion_entry,
            min_selected_count=min_selected_count,
            allow_guardrail_failed_profit_candidates=allow_guardrail_failed,
        ),
        "champion_validation_metrics": (champion_entry or {}).get("metrics") or {},
        "champion_policy_metrics": (champion_entry or {}).get("policy_metrics"),
    }


def test_selection_table_captured_for_market_mid_fallback(tmp_path) -> None:
    settings = _settings(tmp_path)
    rows = [_training_row("KXBTC15M-A", label=1) for _ in range(4)] + [
        _training_row("KXBTC15M-B", label=0)
    ]
    candidates = _market_mid_baseline_winning_candidates()
    report = _candidate_report(candidates)
    model = {"model_type": "market_mid_baseline", "candidate_report": report}

    metrics = _crypto_model_metrics(rows, model, settings=settings)

    # Champion is the market_mid baseline (the scenario we want self-explaining).
    assert report["champion_name"] == "market_mid_baseline"

    # New additive keys present.
    assert metrics["champion_model_type"] == "market_mid_baseline"
    assert metrics["champion_selection_reason"] == "fallback_market_mid_no_non_market_candidate"

    table = metrics["candidate_selection_table"]
    assert isinstance(table, list)
    assert len(table) == len(candidates)
    by_name = {row["name"]: row for row in table}
    assert set(by_name) == {"market_mid_baseline", "spot_distance_residual", "sklearn_logistic"}

    residual = by_name["spot_distance_residual"]
    assert residual["brier"] == 0.0600
    assert residual["policy_net"] == -0.5000
    assert residual["policy_advantage"] == -0.2500
    assert residual["selected_count"] == 4
    # Negative OOS pnl + guardrail_failed (not allowed) => not profit-deployable.
    assert residual["profit_deployable"] is False
    assert residual["status"] == "guardrail_failed"
    assert residual["reason"] == "log_loss_regressed_vs_market_mid"

    # Missing policy_metrics must not raise and yields None policy fields.
    logistic = by_name["sklearn_logistic"]
    assert logistic["policy_net"] is None
    assert logistic["selected_count"] is None
    assert logistic["profit_deployable"] is False

    # All floats / json-safe primitives.
    for row in table:
        assert row["brier"] is None or isinstance(row["brier"], float)
        assert row["policy_net"] is None or isinstance(row["policy_net"], float)
        assert row["policy_advantage"] is None or isinstance(row["policy_advantage"], float)
        assert isinstance(row["profit_deployable"], bool)
        assert isinstance(row["probability_deployable"], bool)


def test_selection_table_does_not_change_champion(tmp_path) -> None:
    """A profitable non-market candidate must still win, and the projection must
    record the same champion that _crypto_select_champion picks."""
    settings = _settings(tmp_path)
    rows = [_training_row("KXBTC15M-A", label=1) for _ in range(4)] + [
        _training_row("KXBTC15M-B", label=0)
    ]
    candidates = [
        {
            "name": "market_mid_baseline",
            "status": "available",
            "metrics": {"brier": 0.0300, "log_loss": 0.20, "ece": 0.02},
            "policy_metrics": None,
            "reason": None,
        },
        {
            "name": "spot_distance_residual",
            "status": "available",
            "metrics": {"brier": 0.0400, "log_loss": 0.19, "ece": 0.018},
            "policy_metrics": {
                "selected_count": 8,
                "net_pnl": "1.2500",
                "pnl_advantage_vs_market_mid_dollars": "0.7500",
            },
            "reason": None,
        },
    ]

    # Baseline behavior: who does the selector pick?
    expected_champion = _crypto_select_champion(
        candidates,
        min_selected_count=1,
        allow_guardrail_failed_profit_candidates=False,
    )
    assert expected_champion == "spot_distance_residual"

    report = _candidate_report(candidates)
    assert report["champion_name"] == expected_champion

    model = {"model_type": "spot_distance_residual", "candidate_report": report}
    metrics = _crypto_model_metrics(rows, model, settings=settings)

    # Projection records the unchanged champion + its model_type.
    assert metrics["champion_model"] == expected_champion
    assert metrics["champion_model_type"] == "spot_distance_residual"
    assert metrics["champion_selection_reason"] == "selected_positive_oos_pnl_non_market_candidate"

    by_name = {row["name"]: row for row in metrics["candidate_selection_table"]}
    winner = by_name["spot_distance_residual"]
    assert winner["profit_deployable"] is True
    assert winner["probability_deployable"] is True
    assert winner["policy_net"] == 1.2500
    assert winner["selected_count"] == 8


def test_model_metrics_without_candidate_report_omits_table(tmp_path) -> None:
    """No candidate_report (e.g. legacy/empty model) => no new keys, no raise."""
    settings = _settings(tmp_path)
    rows = [_training_row("KXBTC15M-A", label=1) for _ in range(4)]

    metrics = _crypto_model_metrics(rows, {}, settings=settings)

    assert "candidate_selection_table" not in metrics
    assert "champion_model_type" not in metrics
