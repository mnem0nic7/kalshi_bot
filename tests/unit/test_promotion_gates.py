from __future__ import annotations

from kalshi_bot.learning.promotion_gates import HoldoutMetrics, evaluate_parameter_pack_promotion


def test_parameter_pack_promotion_gates_pass_when_all_holdout_metrics_clear() -> None:
    current = HoldoutMetrics(
        coverage=0.98,
        brier=0.20,
        ece=0.05,
        sharpe=1.00,
        max_drawdown=0.10,
        city_win_rates={"NY": 0.58, "MIA": 0.55},
        pack_hash="current",
        rerun_pack_hash="current",
    )
    candidate = HoldoutMetrics(
        coverage=0.97,
        brier=0.19,
        ece=0.04,
        sharpe=0.99,
        max_drawdown=0.09,
        city_win_rates={"NY": 0.56, "MIA": 0.54},
        hard_cap_touches=0,
        pack_hash="candidate",
        rerun_pack_hash="candidate",
    )

    result = evaluate_parameter_pack_promotion(candidate=candidate, current=current)

    assert result.passed is True
    assert result.failures == []


def test_parameter_pack_promotion_gates_report_every_failure() -> None:
    current = HoldoutMetrics(
        coverage=0.99,
        brier=0.20,
        ece=0.04,
        sharpe=1.00,
        max_drawdown=0.10,
        city_win_rates={"NY": 0.60},
        pack_hash="current",
        rerun_pack_hash="current",
    )
    candidate = HoldoutMetrics(
        coverage=0.90,
        brier=0.23,
        ece=0.09,
        sharpe=0.80,
        max_drawdown=0.20,
        city_win_rates={"NY": 0.45},
        hard_cap_touches=1,
        pack_hash="candidate",
        rerun_pack_hash="different",
    )

    result = evaluate_parameter_pack_promotion(candidate=candidate, current=current)

    assert result.passed is False
    assert result.failures == [
        "coverage_below_minimum",
        "brier_regression",
        "ece_above_maximum",
        "sharpe_regression",
        "drawdown_regression",
        "city_win_rate_regression",
        "hard_cap_touch",
        "pack_hash_not_idempotent",
    ]
