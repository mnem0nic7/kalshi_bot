from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from kalshi_bot.config import Settings
from kalshi_bot.crypto import btc15m_touch20_rules as rules
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, CryptoSpotOHLCRecord


def _settings(**overrides):
    values = {
        "database_url": "sqlite+aiosqlite:///./test.db",
        "risk_min_edge_bps": 0,
        "crypto_btc15m_touch20_min_rule_score": 0.50,
    }
    values.update(overrides)
    return Settings(
        **values,
    )


def _snapshot(**overrides):
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    values = {
        "kalshi_env": "production",
        "series_ticker": "KXBTC15M",
        "market_ticker": "KXBTC15M-26JUN011200-B100000-T100500",
        "event_ticker": "KXBTC15M-26JUN011200",
        "asset_symbol": "BTC",
        "frequency": "15m",
        "status": "open",
        "open_time": now - timedelta(seconds=300),
        "close_time": now + timedelta(seconds=600),
        "expected_expiration_time": now + timedelta(seconds=600),
        "yes_bid_dollars": Decimal("0.1900"),
        "yes_ask_dollars": Decimal("0.2000"),
        "no_bid_dollars": Decimal("0.7900"),
        "no_ask_dollars": Decimal("0.8100"),
        "observed_at": now,
        "source_kind": "live",
        "settlement_result": "yes",
        "payload": {},
    }
    values.update(overrides)
    return CryptoMarketSnapshotRecord(**values)


def _spot(**overrides):
    values = {
        "available": True,
        "reason": "available",
        "return_1": Decimal("0.0080"),
        "return_3": Decimal("0.0120"),
        "volatility": Decimal("0.0060"),
    }
    values.update(overrides)
    return values


def _gate(bucket_key: str = "BTC|yes|20_30c|le_1c|10_15m"):
    return {
        "allowed_bucket_keys": [bucket_key],
        "bucket_matrix": [
            {
                "bucket_key": bucket_key,
                "sample_count": 25,
                "touch_rate": 0.40,
                "net_pnl": "1.2500",
            }
        ],
    }


def _spot_row(ts: datetime, close: Decimal, **overrides):
    values = {
        "kalshi_env": "production",
        "provider": "coinbase",
        "asset_symbol": "BTC",
        "quote_currency": "USD",
        "frequency": "15m",
        "interval_seconds": 900,
        "start_ts": ts - timedelta(minutes=15),
        "end_ts": ts,
        "observed_at": ts,
        "open_dollars": close,
        "high_dollars": close,
        "low_dollars": close,
        "close_dollars": close,
        "source_kind": "ohlc",
    }
    values.update(overrides)
    return CryptoSpotOHLCRecord(**values)


def test_standalone_module_does_not_import_prohibited_strategy_dependencies():
    source = Path(rules.__file__).read_text()

    prohibited = (
        "AgentPackService",
        "RuntimeCryptoPolicy",
        "_crypto_touch_strategy_candidates",
        "_predict_crypto_probability",
        "_crypto_empirical_bucket_gate",
        "replay_gate_touch20",
        "crypto_touch_strategy_",
    )
    for token in prohibited:
        assert token not in source


def test_spot_feature_index_matches_latest_non_proxy_row():
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    rows = [
        _spot_row(now - timedelta(minutes=45), Decimal("100.00")),
        _spot_row(now - timedelta(minutes=30), Decimal("101.00")),
        _spot_row(now - timedelta(minutes=15), Decimal("103.00"), provider="coingecko", source_kind="proxy"),
        _spot_row(now, Decimal("102.00")),
    ]

    spot = rules._spot_features_from_index(
        rules._prepare_spot_index(rows),
        decision_ts=now,
        freshness_reference=now,
        max_age_seconds=180,
    )

    assert spot["available"] is True
    assert spot["provider"] == "coinbase"
    assert spot["close_dollars"] == "102.00"
    assert spot["return_1"] == Decimal("0.0099")
    assert spot["return_3"] == Decimal("0")


def test_spot_feature_index_is_asset_scoped_for_non_btc_lanes():
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    rows = [
        _spot_row(now - timedelta(minutes=15), Decimal("100.00"), asset_symbol="BTC"),
        _spot_row(now - timedelta(minutes=15), Decimal("3000.00"), asset_symbol="ETH"),
        _spot_row(now, Decimal("3030.00"), asset_symbol="ETH"),
    ]

    spot = rules._spot_features(
        rows,
        decision_ts=now,
        freshness_reference=now,
        max_age_seconds=180,
        asset_symbol="ETH",
    )

    assert spot["available"] is True
    assert spot["close_dollars"] == "3030.00"
    assert spot["return_1"] == Decimal("0.0100")


def test_rules_candidate_uses_explicit_score_and_20pct_objective():
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(),
        settings=_settings(),
        spot=_spot(),
        gate_metrics=_gate(),
    )

    selected = candidates[0]
    assert selected["candidate_status"] == "live_quality"
    assert selected["objective"] == "touch_20pct_before_close"
    assert selected["uses_trained_model"] is False
    assert selected["rule_score"] is not None
    assert selected["score_components"]["replay_touch"] == "0.4000"


def test_non_btc_assets_have_independent_identity_and_disabled_defaults():
    settings = _settings(
        crypto_btc15m_touch20_rules_enabled=True,
        crypto_btc15m_touch20_rules_trading_enabled=True,
    )

    assert rules._scope_supported("15m", "ETH") is True
    assert rules._strategy_code("ETH") == "eth15m_touch20_rules"
    assert rules._order_prefix("ETH") == "eth15t20r"
    assert rules._artifact_type(rules._artifact_base("gate", "ETH"), frequency="15m", asset_symbol="ETH") == "eth15m_touch20_rules_gate:15m:ETH"
    assert rules._asset_settings(settings, "BTC").rules_enabled is True
    assert rules._asset_settings(settings, "BTC").trading_enabled is True
    assert rules._asset_settings(settings, "ETH").rules_enabled is False
    assert rules._asset_settings(settings, "ETH").trading_enabled is False


def test_non_btc_asset_settings_override_candidate_rules():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "ETH": {
                "rules_enabled": True,
                "trading_enabled": False,
                "min_rule_score": 0.50,
                "max_open_notional_dollars": 7,
                "daily_loss_limit_dollars": 3,
            }
        }
    )

    cfg = rules._asset_settings(settings, "ETH")
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(asset_symbol="ETH", series_ticker="KXETH15M", market_ticker="KXETH15M-26JUN011200-B1000-T1005"),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("ETH|yes|20_30c|le_1c|10_15m"),
    )

    assert cfg.rules_enabled is True
    assert cfg.trading_enabled is False
    assert cfg.max_open_notional_dollars == Decimal("7")
    assert cfg.daily_loss_limit_dollars == Decimal("3")
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "ETH|yes|20_30c|le_1c|10_15m"


def test_entry_window_blocks_final_five_minutes_and_allows_boundary():
    settings = _settings()

    blocked = rules.rules_candidates_for_snapshot(
        _snapshot(open_time=datetime(2026, 6, 1, 12, 0, tzinfo=UTC), close_time=datetime(2026, 6, 1, 12, 9, 59, tzinfo=UTC)),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate(),
    )
    allowed = rules.rules_candidates_for_snapshot(
        _snapshot(open_time=datetime(2026, 6, 1, 12, 0, tzinfo=UTC), close_time=datetime(2026, 6, 1, 12, 10, tzinfo=UTC)),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|20_30c|le_1c|5_10m"),
    )

    assert next(candidate for candidate in blocked if candidate["side"] == "yes")["reason"] == "market_too_late"
    assert next(candidate for candidate in allowed if candidate["side"] == "yes")["candidate_status"] == "live_quality"


def test_rules_candidate_blocks_low_ask_target_spread_bucket_and_score():
    settings = _settings()
    low_price = rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.0800"), yes_ask_dollars=Decimal("0.0900")),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|under_10c|le_1c|10_15m"),
    )
    assert next(candidate for candidate in low_price if candidate["side"] == "yes")["reason"] == "entry_price_below_min"
    wide_spread = rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.1500"), yes_ask_dollars=Decimal("0.2500")),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|20_30c|gt_2c|10_15m"),
    )
    assert next(candidate for candidate in wide_spread if candidate["side"] == "yes")["reason"] == "spread_above_tier_max"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.9700"), yes_ask_dollars=Decimal("0.9800")),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|90c_plus|le_1c|10_15m"),
    )[0]["reason"] == "target_profit_impossible_after_fees"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(),
        settings=settings,
        spot=_spot(),
        gate_metrics={"allowed_bucket_keys": [], "bucket_matrix": []},
    )[0]["reason"] == "replay_bucket_not_allowed"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(),
        settings=_settings(crypto_btc15m_touch20_min_rule_score=0.99),
        spot=_spot(return_1=Decimal("-0.0100"), return_3=Decimal("-0.0100"), volatility=Decimal("0.0000")),
        gate_metrics=_gate(),
    )[0]["reason"] == "rule_score_below_min"


def test_gate_blocks_missing_negative_undersampled_low_touch_and_passes_supported():
    settings = _settings(
        crypto_btc15m_touch20_replay_min_candidates=50,
        crypto_btc15m_touch20_replay_min_touch_rate=0.25,
        crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars=0.01,
    )
    good_metrics = {
        "uses_trained_model": False,
        "real_quote_path_row_count": 500,
        "trade_candidate_count": 50,
        "net_simulated_pl_dollars": 1.00,
        "pnl_per_candidate_dollars": 0.02,
        "touch_rate": 0.25,
        "hard_cap_breaches": 0,
        "allowed_bucket_keys": ["BTC|yes|20_30c|le_1c|5_10m"],
    }

    assert rules.gate_reasons(good_metrics, settings=settings) == []
    assert "artifact is missing" in rules.gate_reasons({}, settings=settings)[0]
    assert any("candidate count" in reason for reason in rules.gate_reasons({**good_metrics, "trade_candidate_count": 49}, settings=settings))
    assert any("net P/L" in reason for reason in rules.gate_reasons({**good_metrics, "net_simulated_pl_dollars": -0.01}, settings=settings))
    assert any("touch rate" in reason for reason in rules.gate_reasons({**good_metrics, "touch_rate": 0.24}, settings=settings))
    assert any("hard-cap" in reason for reason in rules.gate_reasons({**good_metrics, "hard_cap_breaches": 1}, settings=settings))
    assert any("allowed bucket" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_bucket_keys": []}, settings=settings))
    assert any("trained model" in reason for reason in rules.gate_reasons({**good_metrics, "uses_trained_model": True}, settings=settings))


def test_future_quote_scan_detects_yes_and_no_touch():
    yes_touch = rules._first_touch(
        [
            _snapshot(yes_bid_dollars=Decimal("0.2300"), no_bid_dollars=Decimal("0.7100")),
            _snapshot(yes_bid_dollars=Decimal("0.2500"), no_bid_dollars=Decimal("0.7400")),
        ],
        side="yes",
        target_exit_side_price=Decimal("0.2400"),
    )
    no_touch = rules._first_touch(
        [
            _snapshot(yes_bid_dollars=Decimal("0.2300"), no_bid_dollars=Decimal("0.7100")),
            _snapshot(yes_bid_dollars=Decimal("0.2500"), no_bid_dollars=Decimal("0.7400")),
        ],
        side="no",
        target_exit_side_price=Decimal("0.7300"),
    )

    assert yes_touch is not None and yes_touch[1] == Decimal("0.2500")
    assert no_touch is not None and no_touch[1] == Decimal("0.7400")


def test_take_profit_uses_net_executable_after_fees():
    settings = _settings()
    target_exit = rules._target_exit_price_for_net_profit(
        Decimal("0.2000"),
        target_pct=Decimal("0.20"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )

    assert target_exit is not None
    at_target = rules.net_profit_pct(
        entry_side_price=Decimal("0.2000"),
        exit_side_price=target_exit,
        count_fp=Decimal("1.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    below_target = rules.net_profit_pct(
        entry_side_price=Decimal("0.2000"),
        exit_side_price=target_exit - Decimal("0.0100"),
        count_fp=Decimal("1.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )

    assert at_target is not None and at_target >= Decimal("0.2000")
    assert below_target is not None and below_target < Decimal("0.2000")


def test_profit_protection_arms_only_after_threshold_then_exits_on_floor():
    settings = _settings()
    entry = {"side": "yes", "profit_protection_armed": False, "max_net_profit_pct": "0", "quote_history": []}

    first = rules.profit_protection_review(entry, spot=_spot(), net_profit=Decimal("0.0900"), settings=settings, now=datetime.now(UTC))
    entry.update(first["entry_updates"])
    second = rules.profit_protection_review(entry, spot=_spot(), net_profit=Decimal("0.1100"), settings=settings, now=datetime.now(UTC))
    entry.update(second["entry_updates"])
    third = rules.profit_protection_review(entry, spot=_spot(), net_profit=Decimal("0.0500"), settings=settings, now=datetime.now(UTC))

    assert first["trigger"] is None
    assert first["entry_updates"]["profit_protection_armed"] is False
    assert second["trigger"] is None
    assert second["entry_updates"]["profit_protection_armed"] is True
    assert third["trigger"] == "profit_protection_floor"


def test_exit_trigger_includes_20pct_stop_loss_after_fees():
    settings = _settings()

    assert rules._exit_trigger_for_profit(
        Decimal("0.2000"),
        asset_symbol="BTC",
        settings=settings,
        protection_trigger=None,
    ) == "take_profit"
    assert rules._exit_trigger_for_profit(
        Decimal("-0.1999"),
        asset_symbol="BTC",
        settings=settings,
        protection_trigger=None,
    ) is None
    assert rules._exit_trigger_for_profit(
        Decimal("-0.2000"),
        asset_symbol="BTC",
        settings=settings,
        protection_trigger=None,
    ) == "stop_loss"


def test_exit_trigger_uses_asset_specific_stop_loss_override():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "ETH": {
                "rules_enabled": True,
                "stop_loss_pct": 0.15,
            }
        }
    )

    assert rules._exit_trigger_for_profit(
        Decimal("-0.1499"),
        asset_symbol="ETH",
        settings=settings,
        protection_trigger=None,
    ) is None
    assert rules._exit_trigger_for_profit(
        Decimal("-0.1500"),
        asset_symbol="ETH",
        settings=settings,
        protection_trigger=None,
    ) == "stop_loss"


def test_terminal_close_marks_expired_zero_price_position_closed_without_exit_fee():
    settings = _settings()
    now = datetime(2026, 6, 1, 12, 20, tzinfo=UTC)
    snapshot = _snapshot(
        status="closed",
        close_time=now - timedelta(minutes=10),
        expected_expiration_time=now - timedelta(minutes=5),
        yes_bid_dollars=Decimal("0.0000"),
        yes_ask_dollars=Decimal("0.0010"),
        no_bid_dollars=Decimal("0.9990"),
        no_ask_dollars=Decimal("1.0000"),
        settlement_result=None,
    )
    entry = {
        "status": "open",
        "side": "yes",
        "count_fp": "10.00",
        "entry_side_price_dollars": "0.5900",
        "close_time": (now - timedelta(minutes=5)).isoformat(),
    }

    assert rules._terminal_close_due(entry, snapshot, now=now) is True
    exit_side = rules._terminal_side_exit_price(snapshot, "yes")
    assert exit_side == Decimal("0.0000")

    result = rules._mark_entry_terminal_closed(
        entry,
        snapshot=snapshot,
        side="yes",
        exit_side_price=exit_side,
        now=now,
        settings=settings,
        trigger="terminal_close_after_market_close",
    )

    expected_realized = rules._realized_pnl_without_exit_fee(
        entry_side_price=Decimal("0.5900"),
        exit_side_price=Decimal("0.0000"),
        count_fp=Decimal("10.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    assert result["status"] == "terminal_closed"
    assert entry["status"] == "closed"
    assert entry["exit_order_status"] == "not_submitted_terminal_close"
    assert entry["exit_side_price_dollars"] == "0.0000"
    assert entry["realized_pnl_dollars"] == str(expected_realized)


def test_strategy_cap_uses_only_strategy_ledger_entries():
    ledger = {
        "positions": {
            "b15t20r:e:abc": {"status": "open", "entry_notional_dollars": "4.2500"},
            "b15t20r:e:def": {"status": "entry_submitted", "entry_notional_dollars": "1.7500"},
            "manual-or-other-bot": {"status": "open", "entry_notional_dollars": "100.0000"},
        }
    }

    assert rules._open_pending_notional(ledger) == Decimal("6.0000")


def test_zero_fill_terminal_entry_status_does_not_reserve_strategy_cap():
    assert rules._entry_ledger_decision("canceled", Decimal("0")) == (
        False,
        "entry_canceled_zero_fill",
    )
    assert rules._entry_ledger_decision("cancelled", None) == (
        False,
        "entry_canceled_zero_fill",
    )
    assert rules._entry_ledger_decision("expired", Decimal("0.00")) == (
        False,
        "entry_canceled_zero_fill",
    )


def test_filled_or_pending_entry_status_updates_strategy_ledger():
    assert rules._entry_ledger_decision("canceled", Decimal("0.25")) == (True, "open")
    assert rules._entry_ledger_decision("filled", Decimal("1.00")) == (True, "open")
    assert rules._entry_ledger_decision("executed", None) == (True, "open")
    assert rules._entry_ledger_decision("submitted", None) == (True, "entry_submitted")


def test_noop_execution_status_does_not_update_strategy_ledger():
    assert rules._entry_ledger_decision("shadow_skipped", None) == (False, "not_recorded")
    assert rules._entry_ledger_decision("kill_switch_blocked", Decimal("0")) == (False, "not_recorded")
    assert rules._entry_ledger_decision("inactive_color_skipped", None) == (False, "not_recorded")
    assert rules._entry_ledger_decision("write_credentials_missing", None) == (False, "not_recorded")


def test_operator_approval_must_match_gate_version():
    gate = type("Gate", (), {"version": "gate-v1", "status": "passed", "payload": {"passed": True}})()

    assert rules._approval_valid({}, gate) == (False, "operator_approval_missing")
    assert rules._approval_valid({"approved": True, "gate_version": "old"}, gate) == (
        False,
        "operator_approval_gate_version_mismatch",
    )
    assert rules._approval_valid({"approved": True, "gate_version": "gate-v1"}, gate) == (
        True,
        "operator_approval_valid",
    )
