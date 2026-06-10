from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import (
    _crypto_dead_feature_report,
    _crypto_decision_rows,
    _crypto_live_market_row,
    _crypto_model_metrics,
    _crypto_replay_gate_reasons,
    _crypto_training_sample_weights,
    _probability_metrics_market_weighted,
    _settlement_benchmark_context,
)
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.services.agent_packs import AgentPackService


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto-effn.db",
        "web_auth_enabled": False,
        "crypto_min_training_samples": 2,
    }
    values.update(overrides)
    return Settings(**values)


def _spot_row(end_ts: datetime, close: str, *, asset: str = "BTC") -> CryptoSpotOHLCRecord:
    return CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol=asset,
        quote_currency="USD",
        frequency="15m",
        interval_seconds=60,
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open_dollars=Decimal(close),
        high_dollars=Decimal(close),
        low_dollars=Decimal(close),
        close_dollars=Decimal(close),
        source_kind="spot_ohlc",
        observed_at=end_ts,
        payload={},
    )


def _snapshot(decision_ts: datetime, close_time: datetime, **overrides):
    attrs = {
        "market_ticker": "KXBTC15M-TEST",
        "series_ticker": "KXBTC15M",
        "asset_symbol": "BTC",
        "frequency": "15m",
        "source_kind": "historical",
        "settlement_result": "yes",
        "observed_at": decision_ts,
        "close_time": close_time,
        "expected_expiration_time": close_time,
        "target_price_dollars": Decimal("100.00000000"),
        "yes_bid_dollars": Decimal("0.4500"),
        "yes_ask_dollars": Decimal("0.4700"),
        "no_bid_dollars": Decimal("0.5300"),
        "no_ask_dollars": Decimal("0.5500"),
        "last_price_dollars": Decimal("0.4600"),
        "volume": 10,
        "open_interest": 5,
        "payload": {},
    }
    attrs.update(overrides)
    return type("_Snapshot", (), attrs)()


def test_settlement_benchmark_context_respects_as_of_cap() -> None:
    close_time = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    as_of = datetime(2026, 5, 1, 12, 5, tzinfo=UTC)
    before_cap = _spot_row(datetime(2026, 5, 1, 12, 4, tzinfo=UTC), "100.0")
    after_cap = _spot_row(datetime(2026, 5, 1, 12, 10, tzinfo=UTC), "999.0")

    capped = _settlement_benchmark_context(
        [before_cap, after_cap],
        close_time=close_time,
        target_price=Decimal("100.0"),
        frequency="15m",
        as_of=as_of,
    )
    uncapped = _settlement_benchmark_context(
        [before_cap, after_cap],
        close_time=close_time,
        target_price=Decimal("100.0"),
        frequency="15m",
    )

    assert capped["settlement_window_status"] == "available"
    assert capped["settlement_window_sample_count"] == 1
    assert capped["settlement_window_twap_dollars"] == Decimal("100.0")
    assert uncapped["settlement_window_sample_count"] == 2

    # Same cap behavior through the pre-sorted bisect path.
    capped_bisect = _settlement_benchmark_context(
        [before_cap, after_cap],
        spot_end_times=[before_cap.end_ts, after_cap.end_ts],
        close_time=close_time,
        target_price=Decimal("100.0"),
        frequency="15m",
        as_of=as_of,
    )
    assert capped_bisect["settlement_window_sample_count"] == 1
    assert capped_bisect["settlement_window_twap_dollars"] == Decimal("100.0")


def test_settlement_benchmark_context_unavailable_before_window_start() -> None:
    close_time = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    as_of = datetime(2026, 5, 1, 11, 58, tzinfo=UTC)
    in_window = _spot_row(datetime(2026, 5, 1, 12, 5, tzinfo=UTC), "101.0")

    context = _settlement_benchmark_context(
        [in_window],
        close_time=close_time,
        target_price=Decimal("100.0"),
        frequency="15m",
        as_of=as_of,
    )
    assert context["settlement_window_status"] == "missing_spot_window"
    assert context["settlement_window_sample_count"] == 0


def test_decision_rows_carry_point_in_time_settlement_window_features() -> None:
    decision_ts = datetime(2026, 5, 1, 12, 8, tzinfo=UTC)
    close_time = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    snapshot = _snapshot(decision_ts, close_time)
    in_window = _spot_row(datetime(2026, 5, 1, 12, 4, tzinfo=UTC), "102.0")
    after_decision = _spot_row(datetime(2026, 5, 1, 12, 12, tzinfo=UTC), "500.0")

    rows = _crypto_decision_rows([snapshot], [], [in_window, after_decision])  # type: ignore[list-item]

    assert rows
    row = rows[0]
    assert row["settlement_window_status"] == "available"
    assert row["settlement_window_sample_count"] == 1
    assert row["settlement_twap_target_distance_pct"] == Decimal("0.02")
    assert row["settlement_window_return_pct"] is not None
    assert row["settlement_window_volatility"] is None


def test_live_market_row_carries_settlement_window_features(tmp_path) -> None:
    settings = _settings(tmp_path)
    now = datetime.now(UTC)
    market = CryptoMarket(
        market_ticker="KXBTC15M-LIVE",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        frequency="15m",
        close_time=now + timedelta(minutes=5),
        target_price_dollars=Decimal("100.00000000"),
        yes_bid_dollars=Decimal("0.4500"),
        yes_ask_dollars=Decimal("0.4700"),
        volume=10,
        open_interest=5,
    )
    spot_rows = [
        _spot_row(now - timedelta(minutes=3), "101.0"),
        _spot_row(now - timedelta(minutes=1), "103.0"),
    ]

    row = _crypto_live_market_row(market, spot_rows=spot_rows, settings=settings)

    assert row["settlement_window_status"] == "available"
    assert row["settlement_window_sample_count"] == 2
    assert row["settlement_window_return_pct"] is not None
    assert row["settlement_twap_target_distance_pct"] == Decimal("0.02")


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


def test_dead_feature_detection_flags_constant_feature() -> None:
    rows = []
    for index in range(6):
        row = _training_row(f"KXBTC15M-{index}", label=index % 2, mid=f"0.4{index}00")
        row["spot_return_1_pct"] = Decimal(str(0.001 * (index + 1)))
        rows.append(row)

    report = _crypto_dead_feature_report(rows)

    # Settlement-window features are absent from these rows, so they are constant zero.
    assert "settlement_window_return_pct" in report["dead_feature_names"]
    assert "settlement_twap_target_distance_pct" in report["dead_feature_names"]
    assert "spot_return_1_pct" not in report["dead_feature_names"]
    assert "mid_yes" not in report["dead_feature_names"]
    assert report["dead_feature_count"] == len(report["dead_feature_names"])


def test_market_balanced_weights_equalize_market_totals(tmp_path) -> None:
    settings = _settings(tmp_path)
    rows = [_training_row("KXBTC15M-A") for _ in range(10)] + [_training_row("KXBTC15M-B")]

    weights = _crypto_training_sample_weights(rows, settings=settings)

    total_a = sum(weights[:10])
    total_b = weights[10]
    assert abs(total_a - total_b) < 1e-9
    assert abs(sum(weights) / len(weights) - 1.0) < 1e-9

    flat = _crypto_training_sample_weights(
        rows,
        settings=_settings(tmp_path, crypto_train_market_balanced_weights=False),
    )
    assert all(abs(weight - flat[0]) < 1e-9 for weight in flat)


def test_market_weighted_probability_metrics_average_per_market() -> None:
    predictions = [
        *[(Decimal("1.0"), 1, "MKT-A")] * 9,  # perfect, 9 rows
        (Decimal("0.0"), 1, "MKT-B"),  # worst case, 1 row
    ]
    metrics = _probability_metrics_market_weighted(predictions)
    assert metrics["market_count"] == 2
    # Each market contributes equally: (0.0 + 1.0) / 2.
    assert abs(metrics["brier"] - 0.5) < 1e-9


def test_model_metrics_include_market_grouped_effective_n(tmp_path) -> None:
    settings = _settings(tmp_path)
    rows = [_training_row("KXBTC15M-A", label=1) for _ in range(4)] + [
        _training_row("KXBTC15M-B", label=0)
    ]
    metrics = _crypto_model_metrics(rows, {}, settings=settings)

    assert metrics["independent_market_count"] == 2
    assert metrics["rows_per_market_mean"] == 2.5
    assert metrics["resolved_market_count"] == 2
    assert metrics["strict_trade_eligible_market_count"] == 2
    assert "calibration_brier_market_weighted" in metrics
    assert "market_mid_brier_market_weighted" in metrics
    assert "dead_feature_names" in metrics


_GATE_ROW_METRICS = {
    "resolved_sample_count": 600,
    "trade_candidate_count": 80,
    "current_model_live_quality_candidate_count": 80,
    "strict_trade_eligible_count": 120,
    "oos_trade_candidate_count": 80,
    "oos_fold_count": 3,
    "oos_evaluation_status": "ok",
    "oos_net_simulated_pl_dollars": 5.0,
    "oos_market_mid_net_simulated_pl_dollars": 0.0,
    "oos_pnl_advantage_vs_market_mid_dollars": 5.0,
    "net_simulated_pl_dollars": 5.0,
    "market_mid_net_simulated_pl_dollars": 0.0,
    "pnl_advantage_vs_market_mid_dollars": 5.0,
    "hard_cap_breaches": 0,
    "candle_count": 100,
    "spot_feature_coverage_pct": 1.0,
    "calibration_brier": 0.10,
    "market_mid_brier": 0.20,
    "calibration_log_loss": 0.30,
    "market_mid_log_loss": 0.40,
    "calibration_ece": 0.05,
    "market_mid_ece": 0.10,
}


def test_gate_reasons_unchanged_when_market_count_keys_absent(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=50,
        crypto_replay_min_trade_candidates=10,
    )
    policy = AgentPackService(settings).runtime_crypto_policy()

    reasons = _crypto_replay_gate_reasons(dict(_GATE_ROW_METRICS), crypto_policy=policy)

    assert reasons == []
    assert not any("Market-weighted" in reason for reason in reasons)


def test_gate_reasons_prefer_market_counts_when_present(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=50,
        crypto_replay_min_trade_candidates=10,
    )
    policy = AgentPackService(settings).runtime_crypto_policy()
    metrics = {
        **_GATE_ROW_METRICS,
        # Row counts pass every threshold, but distinct-market counts do not.
        "resolved_market_count": 12,
        "strict_trade_eligible_market_count": 4,
        "oos_trade_candidate_market_count": 3,
        "current_model_live_quality_market_count": 3,
    }

    reasons = _crypto_replay_gate_reasons(metrics, crypto_policy=policy)

    assert any("12 below minimum 50" in reason for reason in reasons)
    assert any("4 below minimum" in reason for reason in reasons)
    assert any("Out-of-sample trade candidate count 3" in reason for reason in reasons)
    assert any("Current model live-quality candidate count 3" in reason for reason in reasons)


def test_gate_market_weighted_calibration_comparison(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=50,
        crypto_replay_min_trade_candidates=10,
    )
    policy = AgentPackService(settings).runtime_crypto_policy()
    metrics = {
        **_GATE_ROW_METRICS,
        "calibration_brier_market_weighted": 0.30,
        "market_mid_brier_market_weighted": 0.20,
        "calibration_log_loss_market_weighted": 0.30,
        "market_mid_log_loss_market_weighted": 0.40,
        "calibration_ece_market_weighted": 0.05,
        "market_mid_ece_market_weighted": 0.10,
    }

    reasons = _crypto_replay_gate_reasons(metrics, crypto_policy=policy)

    assert any("Market-weighted calibration Brier" in reason for reason in reasons)
    assert not any("Market-weighted calibration log-loss" in reason for reason in reasons)
    assert not any("Market-weighted calibration ECE" in reason for reason in reasons)
