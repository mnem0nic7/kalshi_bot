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
        "open_time": now - timedelta(seconds=60),
        "close_time": now + timedelta(seconds=840),
        "expected_expiration_time": now + timedelta(seconds=840),
        "yes_bid_dollars": Decimal("0.2900"),
        "yes_ask_dollars": Decimal("0.3000"),
        "no_bid_dollars": Decimal("0.6900"),
        "no_ask_dollars": Decimal("0.7100"),
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


def _gate(bucket_key: str = "BTC|yes|30_40c|le_1c|10_15m"):
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
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")
    assert selected["candidate_status"] == "live_quality"
    assert selected["objective"] == "touch_20pct_before_close"
    assert selected["uses_trained_model"] is False
    assert selected["rule_score"] is not None
    assert selected["score_components"]["replay_touch"] == "0.4000"
    assert selected["allowed_sides"] == ["yes"]
    assert no_candidate["reason"] == "side_not_allowed"


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
        gate_metrics=_gate("ETH|yes|30_40c|le_1c|10_15m"),
    )

    assert cfg.rules_enabled is True
    assert cfg.trading_enabled is False
    assert cfg.max_open_notional_dollars == Decimal("7")
    assert cfg.daily_loss_limit_dollars == Decimal("3")
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "ETH|yes|30_40c|le_1c|10_15m"


def test_entry_window_blocks_late_markets_and_allows_early_boundary():
    settings = _settings()

    blocked = rules.rules_candidates_for_snapshot(
        _snapshot(open_time=datetime(2026, 6, 1, 12, 0, tzinfo=UTC), close_time=datetime(2026, 6, 1, 12, 9, 59, tzinfo=UTC)),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate(),
    )
    allowed = rules.rules_candidates_for_snapshot(
        _snapshot(open_time=datetime(2026, 6, 1, 12, 0, tzinfo=UTC), close_time=datetime(2026, 6, 1, 12, 17, tzinfo=UTC)),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|30_40c|le_1c|10_15m"),
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
        _snapshot(yes_bid_dollars=Decimal("0.1500"), yes_ask_dollars=Decimal("0.3500")),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|30_40c|gt_2c|10_15m"),
    )
    assert next(candidate for candidate in wide_spread if candidate["side"] == "yes")["reason"] == "spread_above_tier_max"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.5400"), yes_ask_dollars=Decimal("0.5500")),
        settings=settings,
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|50_60c|le_1c|10_15m"),
    )[0]["reason"] == "entry_price_above_max"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.9700"), yes_ask_dollars=Decimal("0.9800")),
        settings=_settings(crypto_btc15m_touch20_max_contract_price_dollars=1.0),
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
        settings=settings,
        spot=_spot(return_1=Decimal("-0.0100"), return_3=Decimal("-0.0100"), volatility=Decimal("0.0000")),
        gate_metrics=_gate(),
    )[0]["reason"] == "side_aligned_momentum_below_min"
    assert rules.rules_candidates_for_snapshot(
        _snapshot(),
        settings=_settings(crypto_btc15m_touch20_min_rule_score=0.99),
        spot=_spot(),
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
        "stop_loss_rate": 0.10,
        "terminal_loss_rate": 0.05,
        "hard_cap_breaches": 0,
        "allowed_bucket_keys": ["BTC|yes|20_30c|le_1c|5_10m"],
        "simulator_version": rules.TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
        "bucket_matrix": [
            {
                "bucket_key": "BTC|yes|20_30c|le_1c|5_10m",
                "net_pnl": "1.0000",
                "stop_loss_rate": 0.10,
                "terminal_loss_rate": 0.05,
            }
        ],
    }

    assert rules.gate_reasons(good_metrics, settings=settings) == []
    assert "artifact is missing" in rules.gate_reasons({}, settings=settings)[0]
    assert any("candidate count" in reason for reason in rules.gate_reasons({**good_metrics, "trade_candidate_count": 49}, settings=settings))
    assert any("net P/L" in reason for reason in rules.gate_reasons({**good_metrics, "net_simulated_pl_dollars": -0.01}, settings=settings))
    assert any("touch rate" in reason for reason in rules.gate_reasons({**good_metrics, "touch_rate": 0.24}, settings=settings))
    assert any("hard-cap" in reason for reason in rules.gate_reasons({**good_metrics, "hard_cap_breaches": 1}, settings=settings))
    assert any("allowed bucket" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_bucket_keys": []}, settings=settings))
    assert any("trained model" in reason for reason in rules.gate_reasons({**good_metrics, "uses_trained_model": True}, settings=settings))
    assert any("simulator version" in reason for reason in rules.gate_reasons({**good_metrics, "simulator_version": "touch_only_v1"}, settings=settings))
    assert any("stop-loss rate" in reason for reason in rules.gate_reasons({**good_metrics, "stop_loss_rate": 0.36}, settings=settings))
    assert any("terminal-loss rate" in reason for reason in rules.gate_reasons({**good_metrics, "terminal_loss_rate": 0.16}, settings=settings))
    assert any(
        "negative P/L" in reason
        for reason in rules.gate_reasons(
            {
                **good_metrics,
                "net_simulated_pl_dollars": -0.01,
                "touch_rate": 0.90,
                "bucket_matrix": [{**good_metrics["bucket_matrix"][0], "net_pnl": "-0.0100"}],
            },
            settings=settings,
        )
    )


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


def _replay_candidate(side: str = "yes", *, entry: str = "0.5000") -> dict:
    return {
        "side": side,
        "execution_price_dollars": entry,
        "target_exit_side_price_dollars": "0.6200",
        "bucket_key": f"BTC|{side}|50_60c|le_1c|10_15m",
        "rule_score": "0.8000",
    }


def test_live_faithful_replay_exits_take_profit_before_later_stop():
    settings = _settings()
    row = _snapshot(yes_ask_dollars=Decimal("0.5000"), yes_bid_dollars=Decimal("0.4900"), settlement_result="no")
    future = [
        _snapshot(observed_at=row.observed_at + timedelta(seconds=60), yes_bid_dollars=Decimal("0.7000")),
        _snapshot(observed_at=row.observed_at + timedelta(seconds=120), yes_bid_dollars=Decimal("0.3000")),
    ]

    simulation = rules._simulate_replay_trade(row, future, _replay_candidate(), settings=settings)

    assert simulation["exit_reason"] == "take_profit"
    assert simulation["touched"] is True
    assert simulation["stopped"] is False
    assert simulation["exit_price_dollars"] == "0.7000"


def test_live_faithful_replay_exits_stop_loss_before_later_take_profit():
    settings = _settings()
    row = _snapshot(yes_ask_dollars=Decimal("0.5000"), yes_bid_dollars=Decimal("0.4900"), settlement_result="yes")
    future = [
        _snapshot(observed_at=row.observed_at + timedelta(seconds=60), yes_bid_dollars=Decimal("0.3000")),
        _snapshot(observed_at=row.observed_at + timedelta(seconds=120), yes_bid_dollars=Decimal("0.7000")),
    ]

    simulation = rules._simulate_replay_trade(row, future, _replay_candidate(), settings=settings)

    assert simulation["exit_reason"] == "stop_loss"
    assert simulation["touched"] is False
    assert simulation["stopped"] is True
    assert simulation["exit_price_dollars"] == "0.3000"


def test_live_faithful_replay_terminal_closes_without_executable_exit():
    settings = _settings()
    row = _snapshot(yes_ask_dollars=Decimal("0.5000"), yes_bid_dollars=Decimal("0.4900"), settlement_result="no")

    simulation = rules._simulate_replay_trade(row, [], _replay_candidate(), settings=settings)

    assert simulation["exit_reason"] == "terminal_close"
    assert simulation["terminal_closed"] is True
    assert simulation["exit_price_dollars"] == "0.0000"
    assert Decimal(simulation["net_pnl"]) < Decimal("0")


def test_live_faithful_replay_enters_only_first_eligible_row_per_market():
    settings = _settings(crypto_btc15m_touch20_min_rule_score=0.48)
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    first = _snapshot(observed_at=now, yes_bid_dollars=Decimal("0.2900"), yes_ask_dollars=Decimal("0.3000"))
    second = _snapshot(observed_at=now + timedelta(seconds=60), yes_bid_dollars=Decimal("0.3900"), yes_ask_dollars=Decimal("0.4000"))
    spot_rows = [_spot_row(now, Decimal("100.00"))]

    report = rules._evaluate_replay([first, second], spot_rows, settings=settings)

    assert report["metrics"]["entry_replay_mode"] == "first_eligible_per_market"
    assert report["metrics"]["trade_candidate_count"] == 1
    assert report["trade_sample"][0]["decision_ts"] == now.isoformat()


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


def test_live_bucket_controls_block_negative_and_consecutive_loss_buckets():
    settings = _settings()
    now = datetime(2026, 6, 1, 12, 30, tzinfo=UTC)
    ledger = {
        "asset_symbol": "BTC",
        "positions": {
            "b15t20r:e:loss1": {
                "status": "closed",
                "bucket_key": "BTC|yes|50_60c|le_1c|10_15m",
                "exit_trigger": "stop_loss",
                "realized_pnl_dollars": "-1.1000",
                "closed_at": (now - timedelta(minutes=20)).isoformat(),
            },
            "b15t20r:e:loss2": {
                "status": "closed",
                "bucket_key": "BTC|no|30_40c|le_1c|10_15m",
                "exit_trigger": "terminal_close_after_market_close",
                "realized_pnl_dollars": "-0.1000",
                "closed_at": (now - timedelta(minutes=10)).isoformat(),
            },
            "b15t20r:e:loss3": {
                "status": "closed",
                "bucket_key": "BTC|no|30_40c|le_1c|10_15m",
                "exit_trigger": "stop_loss",
                "realized_pnl_dollars": "-0.1000",
                "closed_at": now.isoformat(),
            },
            "other:e:ignored": {
                "status": "closed",
                "bucket_key": "BTC|yes|50_60c|le_1c|10_15m",
                "exit_trigger": "stop_loss",
                "realized_pnl_dollars": "-99.0000",
                "closed_at": now.isoformat(),
            },
        },
    }

    controls = rules._live_bucket_controls(ledger, settings=settings, asset_symbol="BTC")

    assert "BTC|yes|50_60c|le_1c|10_15m" in controls["blocked_bucket_keys"]
    assert "BTC|no|30_40c|le_1c|10_15m" in controls["blocked_bucket_keys"]
    no_bucket = next(bucket for bucket in controls["buckets"] if bucket["bucket_key"] == "BTC|no|30_40c|le_1c|10_15m")
    assert "bucket_consecutive_stop_or_terminal_losses" in no_bucket["block_reasons"]


def test_duplicate_market_exposure_and_loss_cooldown_are_strategy_local():
    now = datetime(2026, 6, 1, 12, 30, tzinfo=UTC)
    ledger = {
        "asset_symbol": "BTC",
        "positions": {
            "b15t20r:e:open": {
                "status": "open",
                "market_ticker": "KXBTC15M-DUP",
                "entry_notional_dollars": "5.0000",
            },
            "manual": {
                "status": "open",
                "market_ticker": "KXBTC15M-DUP",
                "entry_notional_dollars": "500.0000",
            },
            "b15t20r:e:loss": {
                "status": "closed",
                "market_ticker": "KXBTC15M-COOL",
                "exit_trigger": "stop_loss",
                "realized_pnl_dollars": "-0.5000",
                "closed_at": (now - timedelta(minutes=5)).isoformat(),
            },
        },
    }

    exposure = rules._market_strategy_exposure(ledger, "KXBTC15M-DUP")
    cooldown = rules._loss_cooldown_for_market(ledger, "KXBTC15M-COOL", now=now)

    assert len(exposure) == 1
    assert exposure[0]["client_order_id"] == "b15t20r:e:open"
    assert cooldown is not None
    assert cooldown["exit_trigger"] == "stop_loss"


def test_min_order_notional_blocks_dust_sized_remaining_cap():
    settings = _settings()
    cfg = rules._asset_settings(settings, "BTC")
    entry_price = Decimal("0.5000")
    count = rules._count_for_cap(Decimal("4.9900"), entry_price)

    assert count is not None
    assert entry_price * count < cfg.min_order_notional_dollars


def test_daily_loss_limit_input_is_still_strategy_local_and_utc_day_scoped():
    now = datetime(2026, 6, 1, 12, 30, tzinfo=UTC)
    ledger = {
        "asset_symbol": "BTC",
        "positions": {
            "b15t20r:e:today": {
                "status": "closed",
                "realized_pnl_dollars": "-10.0000",
                "closed_at": now.isoformat(),
            },
            "b15t20r:e:yesterday": {
                "status": "closed",
                "realized_pnl_dollars": "-10.0000",
                "closed_at": (now - timedelta(days=1)).isoformat(),
            },
            "manual": {
                "status": "closed",
                "realized_pnl_dollars": "-99.0000",
                "closed_at": now.isoformat(),
            },
        },
    }

    assert rules._daily_realized_pnl(ledger, now) == Decimal("-10.0000")


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
    stale_gate = type("Gate", (), {"version": "gate-v1", "status": "passed", "payload": {"passed": True}})()
    gate = type(
        "Gate",
        (),
        {
            "version": "gate-v1",
            "status": "passed",
            "payload": {
                "passed": True,
                "simulator_version": rules.TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
            },
        },
    )()

    assert rules._approval_valid({}, gate) == (False, "operator_approval_missing")
    assert rules._gate_passed(stale_gate) is False
    assert rules._approval_valid(
        {"approved": True, "gate_version": "gate-v1", "simulator_version": rules.TOUCH20_RULES_REPLAY_SIMULATOR_VERSION},
        stale_gate,
    ) == (False, "gate_simulator_version_stale_or_missing")
    assert rules._approval_valid({"approved": True, "gate_version": "gate-v1"}, gate) == (
        False,
        "operator_approval_simulator_version_mismatch",
    )
    assert rules._approval_valid(
        {"approved": True, "gate_version": "old", "simulator_version": rules.TOUCH20_RULES_REPLAY_SIMULATOR_VERSION},
        gate,
    ) == (
        False,
        "operator_approval_gate_version_mismatch",
    )
    assert rules._approval_valid(
        {"approved": True, "gate_version": "gate-v1", "simulator_version": rules.TOUCH20_RULES_REPLAY_SIMULATOR_VERSION},
        gate,
    ) == (
        True,
        "operator_approval_valid",
    )
