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
    assert rules._asset_settings(_settings(), "BTC").max_contract_price_dollars == Decimal("0.50")

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


def test_rules_candidate_distinguishes_non_executable_terminal_quotes():
    terminal_candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            yes_bid_dollars=Decimal("0.0000"),
            yes_ask_dollars=Decimal("0.0100"),
            no_bid_dollars=Decimal("0.9900"),
            no_ask_dollars=Decimal("1.0000"),
        ),
        settings=_settings(),
        spot=_spot(),
        gate_metrics=_gate(),
    )
    missing_candidates = rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=None, no_ask_dollars=None),
        settings=_settings(),
        spot=_spot(),
        gate_metrics=_gate(),
    )

    terminal_yes = next(candidate for candidate in terminal_candidates if candidate["side"] == "yes")
    missing_yes = next(candidate for candidate in missing_candidates if candidate["side"] == "yes")

    assert terminal_yes["reason"] == "non_executable_bid_ask"
    assert missing_yes["reason"] == "missing_real_bid_ask"


def test_rules_candidate_infers_entry_from_opposite_side_quote_pair():
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            yes_bid_dollars=None,
            yes_ask_dollars=None,
            no_bid_dollars=Decimal("0.7000"),
            no_ask_dollars=Decimal("0.7100"),
        ),
        settings=_settings(),
        spot=_spot(),
        gate_metrics=_gate(),
    )

    selected = candidates[0]

    assert selected["side"] == "yes"
    assert selected["candidate_status"] == "live_quality"
    assert selected["execution_price_dollars"] == "0.3000"
    assert selected["bid_price_dollars"] == "0.2900"
    assert selected["spread_dollars"] == "0.0100"
    assert selected["target_yes_price_dollars"] == "0.3000"


def test_rules_candidate_rejects_terminal_opposite_side_complement():
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            yes_bid_dollars=None,
            yes_ask_dollars=None,
            no_bid_dollars=Decimal("0.9900"),
            no_ask_dollars=Decimal("1.0000"),
        ),
        settings=_settings(),
        spot=_spot(),
        gate_metrics=_gate(),
    )

    selected = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert selected["reason"] == "non_executable_bid_ask"


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


def test_one_hour_assets_have_frequency_scoped_identity_and_settings():
    settings = _settings(
        crypto_btc15m_touch20_rules_enabled=True,
        crypto_btc15m_touch20_rules_trading_enabled=True,
        crypto_1h_touch20_rules_enabled=True,
        crypto_1h_touch20_rules_trading_enabled=False,
        crypto_1h_touch20_min_seconds_to_close=1200,
        crypto_1h_touch20_asset_settings={
            "ETH": {
                "rules_enabled": True,
                "trading_enabled": True,
                "bucket_time_band_minutes": 15,
            }
        },
    )

    assert rules._scope_supported("1h", "ETH") is True
    assert rules._strategy_code("BTC", frequency="1h") == "btc1h_touch20_rules"
    assert rules._strategy_code("ETH", frequency="1h") == "eth1h_touch20_rules"
    assert rules._order_prefix("BTC", frequency="1h") == "btc1ht20r"
    assert rules._artifact_type(rules._artifact_base("gate", "ETH", frequency="1h"), frequency="1h", asset_symbol="ETH") == "eth1h_touch20_rules_gate:1h:ETH"
    assert rules._asset_settings(settings, "BTC", frequency="15m").trading_enabled is True
    assert rules._asset_settings(settings, "BTC", frequency="1h").rules_enabled is True
    assert rules._asset_settings(settings, "BTC", frequency="1h").trading_enabled is False
    assert rules._asset_settings(settings, "BTC", frequency="1h").min_seconds_to_close == 1200
    assert rules._asset_settings(settings, "ETH", frequency="1h").rules_enabled is True
    assert rules._asset_settings(settings, "ETH", frequency="1h").trading_enabled is True
    assert rules._asset_settings(settings, "HYPE", frequency="1h").rules_enabled is True
    assert rules._asset_settings(settings, "HYPE", frequency="1h").trading_enabled is False
    assert rules._time_bucket(3600, width_minutes=15, interval_seconds=3600) == "45_60m"
    assert rules._time_bucket(1200, width_minutes=15, interval_seconds=3600) == "15_30m"
    assert rules._bucket_time_band_minutes(60) == 60
    assert rules._time_bucket(3600, width_minutes=60, interval_seconds=3600) == "0_60m"


def test_one_hour_default_asset_scope_is_all_supported_markets():
    settings = _settings(crypto_1h_touch20_rules_enabled=True)

    assert rules._configured_assets(settings, frequency="1h") == [
        "BTC",
        "HYPE",
        "ETH",
        "BNB",
        "SOL",
        "DOGE",
        "XRP",
    ]
    assert rules._asset_settings(settings, "XRP", frequency="1h").rules_enabled is True
    assert rules._asset_settings(settings, "XRP", frequency="1h").trading_enabled is False
    assert rules._asset_settings(settings, "XRP", frequency="15m").rules_enabled is False


def test_one_hour_asset_can_use_full_interval_bucket_for_sparse_replay():
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    settings = _settings(
        crypto_1h_touch20_asset_settings={
            "BTC": {
                "allowed_sides": "yes",
                "bucket_time_band_minutes": 60,
                "min_aligned_momentum": 0.0,
                "min_rule_score": 0.0,
            }
        }
    )
    gate = _gate("BTC|yes|40_50c|le_1c|0_60m")

    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            frequency="1h",
            series_ticker="KXBTC",
            market_ticker="KXBTC-26JUN011200-B100000-T100500",
            event_ticker="KXBTC-26JUN011200",
            open_time=now - timedelta(seconds=60),
            close_time=now + timedelta(seconds=3600),
            expected_expiration_time=now + timedelta(seconds=3600),
            yes_bid_dollars=Decimal("0.4400"),
            yes_ask_dollars=Decimal("0.4500"),
            no_bid_dollars=Decimal("0.5400"),
            no_ask_dollars=Decimal("0.5600"),
        ),
        settings=settings,
        spot=_spot(),
        gate_metrics=gate,
    )

    assert rules._asset_settings(settings, "BTC", frequency="1h").bucket_time_band_minutes == 60
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "BTC|yes|40_50c|le_1c|0_60m"


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


def test_non_btc_asset_can_use_20c_bucket_price_bands():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "BNB": {
                "rules_enabled": True,
                "allowed_sides": "yes",
                "max_contract_price_dollars": 0.85,
                "min_aligned_momentum": 0.0,
                "min_rule_score": 0.30,
                "bucket_price_band_cents": 20,
            }
        }
    )
    gate = _gate("BNB|yes|60_80c|le_1c|10_15m")
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            asset_symbol="BNB",
            series_ticker="KXBNB15M",
            market_ticker="KXBNB15M-26JUN011200-B600-T700",
            yes_bid_dollars=Decimal("0.6800"),
            yes_ask_dollars=Decimal("0.6900"),
            no_bid_dollars=Decimal("0.3000"),
            no_ask_dollars=Decimal("0.3100"),
        ),
        settings=settings,
        spot=_spot(),
        gate_metrics=gate,
    )

    assert rules._asset_settings(settings, "BNB").bucket_price_band_cents == 20
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "BNB|yes|60_80c|le_1c|10_15m"


def test_non_btc_asset_can_use_40c_bucket_price_bands():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "DOGE": {
                "rules_enabled": True,
                "allowed_sides": "yes",
                "max_contract_price_dollars": 0.85,
                "min_aligned_momentum": 0.0,
                "min_rule_score": 0.30,
                "bucket_price_band_cents": 40,
            }
        }
    )
    gate = _gate("DOGE|yes|40_80c|le_1c|10_15m")
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            asset_symbol="DOGE",
            series_ticker="KXDOGE15M",
            market_ticker="KXDOGE15M-26JUN011200-B1000-T1005",
            yes_bid_dollars=Decimal("0.6700"),
            yes_ask_dollars=Decimal("0.6800"),
            no_bid_dollars=Decimal("0.3100"),
            no_ask_dollars=Decimal("0.3200"),
        ),
        settings=settings,
        spot=_spot(),
        gate_metrics=gate,
    )

    assert rules._asset_settings(settings, "DOGE").bucket_price_band_cents == 40
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "DOGE|yes|40_80c|le_1c|10_15m"


def test_non_btc_asset_can_merge_spread_bands_for_sparse_assets():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "DOGE": {
                "rules_enabled": True,
                "allowed_sides": "yes",
                "max_contract_price_dollars": 0.85,
                "min_aligned_momentum": 0.0,
                "min_rule_score": 0.30,
                "bucket_spread_band_cents": 2,
            }
        }
    )
    gate = _gate("DOGE|yes|40_50c|le_2c|10_15m")
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            asset_symbol="DOGE",
            series_ticker="KXDOGE15M",
            market_ticker="KXDOGE15M-26JUN011200-B40-T50",
            yes_bid_dollars=Decimal("0.4300"),
            yes_ask_dollars=Decimal("0.4500"),
            no_bid_dollars=Decimal("0.5400"),
            no_ask_dollars=Decimal("0.5600"),
        ),
        settings=settings,
        spot=_spot(),
        gate_metrics=gate,
    )

    assert rules._asset_settings(settings, "DOGE").bucket_spread_band_cents == 2
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "DOGE|yes|40_50c|le_2c|10_15m"


def test_non_btc_asset_can_merge_time_buckets_for_sparse_assets():
    settings = _settings(
        crypto_15m_touch20_asset_settings={
            "XRP": {
                "rules_enabled": True,
                "allowed_sides": "yes",
                "max_contract_price_dollars": 0.85,
                "min_aligned_momentum": 0.0,
                "min_rule_score": 0.30,
                "bucket_time_band_minutes": 10,
            }
        }
    )
    gate = _gate("XRP|yes|40_50c|le_1c|5_15m")
    candidates = rules.rules_candidates_for_snapshot(
        _snapshot(
            asset_symbol="XRP",
            series_ticker="KXXRP15M",
            market_ticker="KXXRP15M-26JUN011200-B40-T50",
            yes_bid_dollars=Decimal("0.4400"),
            yes_ask_dollars=Decimal("0.4500"),
            no_bid_dollars=Decimal("0.5400"),
            no_ask_dollars=Decimal("0.5500"),
        ),
        settings=settings,
        spot=_spot(),
        gate_metrics=gate,
    )

    assert rules._asset_settings(settings, "XRP").bucket_time_band_minutes == 10
    assert candidates[0]["candidate_status"] == "live_quality"
    assert candidates[0]["bucket_key"] == "XRP|yes|40_50c|le_1c|5_15m"


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
    wide_spread_override = rules.rules_candidates_for_snapshot(
        _snapshot(yes_bid_dollars=Decimal("0.3100"), yes_ask_dollars=Decimal("0.3500")),
        settings=_settings(
            crypto_15m_touch20_asset_settings={
                "BTC": {
                    "max_spread_dollars": 0.05,
                    "min_aligned_momentum": 0.0,
                    "min_rule_score": 0.30,
                }
            }
        ),
        spot=_spot(),
        gate_metrics=_gate("BTC|yes|30_40c|gt_2c|10_15m"),
    )
    assert next(candidate for candidate in wide_spread_override if candidate["side"] == "yes")["candidate_status"] == "live_quality"
    assert next(candidate for candidate in wide_spread_override if candidate["side"] == "yes")["max_spread_dollars"] == "0.0500"
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
        "allowed_trade_candidate_count": 50,
        "gate_candidate_scope": "allowed_replay_buckets",
        "allowed_net_simulated_pl_dollars": 1.00,
        "allowed_pnl_per_candidate_dollars": 0.02,
        "allowed_touch_rate": 0.25,
        "allowed_stop_loss_rate": 0.10,
        "allowed_terminal_loss_rate": 0.05,
        "allowed_hard_cap_breaches": 0,
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
                "sample_count": 50,
                "allowed": True,
                "net_pnl": "1.0000",
                "touch_rate": 0.25,
                "stop_loss_rate": 0.10,
                "terminal_loss_rate": 0.05,
            },
            {
                "bucket_key": "BTC|yes|40_50c|le_2c|10_15m",
                "sample_count": 2,
                "allowed": False,
                "net_pnl": "-0.5000",
                "touch_rate": 0.00,
                "stop_loss_rate": 0.50,
                "terminal_loss_rate": 0.00,
            },
        ],
    }

    assert rules.gate_reasons(good_metrics, settings=settings) == []
    assert (
        rules.gate_reasons(
            {
                **good_metrics,
                "trade_candidate_count": 52,
                "net_simulated_pl_dollars": -10.00,
                "pnl_per_candidate_dollars": -0.1923,
                "touch_rate": 0.10,
                "stop_loss_rate": 0.90,
                "terminal_loss_rate": 0.90,
                "hard_cap_breaches": 5,
            },
            settings=settings,
        )
        == []
    )
    assert "artifact is missing" in rules.gate_reasons({}, settings=settings)[0]
    assert any(
        "candidate count" in reason
        for reason in rules.gate_reasons(
            {**good_metrics, "trade_candidate_count": 60, "allowed_trade_candidate_count": 49},
            settings=settings,
        )
    )
    assert any("net P/L" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_net_simulated_pl_dollars": -0.01}, settings=settings))
    assert any("touch rate" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_touch_rate": 0.24}, settings=settings))
    assert any("hard-cap" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_hard_cap_breaches": 1}, settings=settings))
    assert any("allowed bucket" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_bucket_keys": []}, settings=settings))
    assert any("trained model" in reason for reason in rules.gate_reasons({**good_metrics, "uses_trained_model": True}, settings=settings))
    assert any("simulator version" in reason for reason in rules.gate_reasons({**good_metrics, "simulator_version": "touch_only_v1"}, settings=settings))
    assert any("stop-loss rate" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_stop_loss_rate": 0.36}, settings=settings))
    assert any(
        "terminal-loss rate" in reason for reason in rules.gate_reasons({**good_metrics, "allowed_terminal_loss_rate": 0.16}, settings=settings)
    )
    assert any(
        "negative P/L" in reason
        for reason in rules.gate_reasons(
            {
                **good_metrics,
                "allowed_net_simulated_pl_dollars": -0.01,
                "allowed_touch_rate": 0.90,
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
    rules._target_exit_price_for_net_profit.cache_clear()
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
    assert rules._target_exit_price_for_net_profit.cache_info().misses == 1
    assert (
        rules._target_exit_price_for_net_profit(
            Decimal("0.2000"),
            target_pct=Decimal("0.20"),
            fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
        )
        == target_exit
    )
    assert rules._target_exit_price_for_net_profit.cache_info().hits == 1


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
    settings = _settings(crypto_btc15m_touch20_min_rule_score=0.48, crypto_btc15m_touch20_min_aligned_momentum=0.0)
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    first = _snapshot(observed_at=now, yes_bid_dollars=Decimal("0.2900"), yes_ask_dollars=Decimal("0.3000"))
    second = _snapshot(observed_at=now + timedelta(seconds=60), yes_bid_dollars=Decimal("0.3900"), yes_ask_dollars=Decimal("0.4000"))
    spot_rows = [_spot_row(now, Decimal("100.00"))]

    report = rules._evaluate_replay([first, second], spot_rows, settings=settings)

    assert report["metrics"]["entry_replay_mode"] == "first_eligible_per_market"
    assert report["metrics"]["trade_candidate_count"] == 1
    assert report["trade_sample"][0]["decision_ts"] == now.isoformat()


def test_live_faithful_replay_keeps_yes_only_quote_rows():
    settings = _settings(crypto_btc15m_touch20_min_rule_score=0.48, crypto_btc15m_touch20_min_aligned_momentum=0.0)
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    entry = _snapshot(
        observed_at=now,
        yes_bid_dollars=Decimal("0.2900"),
        yes_ask_dollars=Decimal("0.3000"),
        no_bid_dollars=None,
        no_ask_dollars=None,
    )
    touch_exit = _snapshot(
        observed_at=now + timedelta(seconds=60),
        yes_bid_dollars=Decimal("0.7000"),
        yes_ask_dollars=Decimal("0.7100"),
        no_bid_dollars=None,
        no_ask_dollars=None,
    )
    spot_rows = [_spot_row(now, Decimal("100.00"))]

    report = rules._evaluate_replay([entry, touch_exit], spot_rows, settings=settings)

    assert report["metrics"]["trade_candidate_count"] == 1
    assert report["trade_sample"][0]["simulation"]["touched"] is True
    assert report["trade_sample"][0]["simulation"]["exit_reason"] == "take_profit"


def test_live_faithful_replay_keeps_opposite_side_complement_quote_rows():
    settings = _settings(crypto_btc15m_touch20_min_rule_score=0.48, crypto_btc15m_touch20_min_aligned_momentum=0.0)
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    entry = _snapshot(
        observed_at=now,
        yes_bid_dollars=None,
        yes_ask_dollars=None,
        no_bid_dollars=Decimal("0.7000"),
        no_ask_dollars=Decimal("0.7100"),
    )
    touch_exit = _snapshot(
        observed_at=now + timedelta(seconds=60),
        yes_bid_dollars=None,
        yes_ask_dollars=None,
        no_bid_dollars=Decimal("0.2900"),
        no_ask_dollars=Decimal("0.3000"),
    )
    spot_rows = [_spot_row(now, Decimal("100.00"))]

    report = rules._evaluate_replay([entry, touch_exit], spot_rows, settings=settings)

    assert report["metrics"]["trade_candidate_count"] == 1
    assert report["metrics"]["input_diagnostics"]["side_filter_funnel"]["yes"]["quote_source_rows"] == 2
    assert "raw_bid_ask_rows" not in report["metrics"]["input_diagnostics"]["side_filter_funnel"]["yes"]
    assert report["trade_sample"][0]["candidate"]["execution_price_dollars"] == "0.3000"
    assert report["trade_sample"][0]["simulation"]["touched"] is True
    assert report["trade_sample"][0]["simulation"]["exit_price_dollars"] == "0.7000"


def test_replay_metrics_include_input_quote_diagnostics():
    settings = _settings()
    now = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    entry_row = _snapshot(observed_at=now)
    late_row = _snapshot(
        market_ticker="KXBTC15M-LATE",
        event_ticker="KXBTC15M-LATE-E",
        observed_at=now + timedelta(minutes=10),
        open_time=now,
        close_time=now + timedelta(minutes=12),
        expected_expiration_time=now + timedelta(minutes=12),
    )
    spot_rows = [_spot_row(now, Decimal("100.00"))]

    report = rules._evaluate_replay([entry_row, late_row], spot_rows, settings=settings)

    diagnostics = report["metrics"]["input_diagnostics"]
    assert diagnostics["entry_window_row_count"] == 1
    assert diagnostics["entry_window_market_count"] == 1
    assert diagnostics["side_quote_diagnostics"]["yes"]["entry_window_rows"] == 1
    assert diagnostics["side_quote_diagnostics"]["yes"]["quote_source_rows"] == 1
    assert diagnostics["side_quote_diagnostics"]["yes"]["executable_bid_ask_rows"] == 1
    assert diagnostics["side_quote_diagnostics"]["yes"]["configured_price_band_rows"] == 1
    assert diagnostics["side_quote_diagnostics"]["no"]["executable_bid_ask_rows"] == 1
    assert "configured_price_band_rows" not in diagnostics["side_quote_diagnostics"]["no"]
    assert diagnostics["side_filter_funnel"]["yes"]["total_rows"] == 2
    assert diagnostics["side_filter_funnel"]["yes"]["allowed_side_rows"] == 2
    assert diagnostics["side_filter_funnel"]["yes"]["entry_window_rows"] == 1
    assert diagnostics["side_filter_funnel"]["yes"]["quote_source_rows"] == 1
    assert diagnostics["side_filter_funnel"]["yes"]["executable_bid_ask_rows"] == 1
    assert diagnostics["side_filter_funnel"]["yes"]["configured_price_band_rows"] == 1
    assert diagnostics["side_filter_funnel"]["yes"]["target_exit_possible_rows"] == 1
    assert diagnostics["side_filter_funnel"]["yes"]["spread_within_tier_rows"] == 1
    assert diagnostics["side_filter_funnel"]["no"]["total_rows"] == 2
    assert "allowed_side_rows" not in diagnostics["side_filter_funnel"]["no"]
    assert "entry_window_rows" not in diagnostics["side_filter_funnel"]["no"]
    assert diagnostics["side_filter_market_funnel"]["yes"]["total_markets"] == 2
    assert diagnostics["side_filter_market_funnel"]["yes"]["allowed_side_markets"] == 2
    assert diagnostics["side_filter_market_funnel"]["yes"]["entry_window_markets"] == 1
    assert diagnostics["side_filter_market_funnel"]["yes"]["executable_bid_ask_markets"] == 1
    assert diagnostics["side_filter_market_funnel"]["yes"]["configured_price_band_markets"] == 1
    assert diagnostics["side_filter_market_funnel"]["yes"]["target_exit_possible_markets"] == 1
    assert diagnostics["side_filter_market_funnel"]["yes"]["spread_within_tier_markets"] == 1
    assert diagnostics["side_filter_market_funnel"]["no"]["total_markets"] == 2
    assert "allowed_side_markets" not in diagnostics["side_filter_market_funnel"]["no"]


def test_optimizer_profiles_rank_passed_replay_profile():
    settings = _settings(
        crypto_btc15m_touch20_min_rule_score=0.45,
        crypto_btc15m_touch20_replay_min_candidates=5,
    )
    start = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    snapshots = []
    spot_rows = [_spot_row(start - timedelta(minutes=15), Decimal("100.00"), asset_symbol="ETH")]
    for idx in range(6):
        decision_ts = start + timedelta(minutes=15 * idx)
        market_ticker = f"KXBTC15M-OPT-{idx}"
        snapshots.append(
            _snapshot(
                market_ticker=market_ticker,
                event_ticker=f"KXBTC15M-OPT-E{idx}",
                observed_at=decision_ts,
                open_time=decision_ts - timedelta(seconds=60),
                close_time=decision_ts + timedelta(seconds=840),
                expected_expiration_time=decision_ts + timedelta(seconds=840),
                yes_bid_dollars=Decimal("0.2900"),
                yes_ask_dollars=Decimal("0.3000"),
                no_bid_dollars=Decimal("0.6900"),
                no_ask_dollars=Decimal("0.7100"),
                settlement_result="yes",
            )
        )
        snapshots.append(
            _snapshot(
                market_ticker=market_ticker,
                event_ticker=f"KXBTC15M-OPT-E{idx}",
                observed_at=decision_ts + timedelta(seconds=60),
                open_time=decision_ts - timedelta(seconds=60),
                close_time=decision_ts + timedelta(seconds=840),
                expected_expiration_time=decision_ts + timedelta(seconds=840),
                yes_bid_dollars=Decimal("0.4200"),
                yes_ask_dollars=Decimal("0.4300"),
                no_bid_dollars=Decimal("0.5600"),
                no_ask_dollars=Decimal("0.5800"),
                settlement_result="yes",
            )
        )
        spot_rows.append(_spot_row(decision_ts - timedelta(minutes=15), Decimal("100.00") + Decimal(idx)))
        spot_rows.append(_spot_row(decision_ts, Decimal("101.00") + Decimal(idx)))

    result = rules.optimize_replay_profiles(
        snapshots,
        spot_rows,
        settings=settings,
        asset_symbol="BTC",
        top_n=3,
    )

    assert result["status"] == "passed_profile_found"
    assert result["profile_count"] >= 3
    assert len(result["profiles"]) == 3
    assert result["best_profile"]["passed"] is True
    assert result["best_profile"]["trade_candidate_count"] >= 5


def test_optimizer_profile_filter_limits_evaluated_profiles():
    settings = _settings()

    result = rules.optimize_replay_profiles(
        [],
        [],
        settings=settings,
        asset_symbol="BTC",
        frequency="1h",
        profile_names=["current", "yes_no_take10_maxspread10_open_s25"],
        top_n=5,
    )

    assert result["profile_filter"] == ["current", "yes_no_take10_maxspread10_open_s25"]
    assert result["profile_count"] == 2
    assert [profile["profile"] for profile in result["profiles"]] == [
        "current",
        "yes_no_take10_maxspread10_open_s25",
    ]


def test_optimizer_marks_replay_candidate_floor_relaxation_non_promotable():
    base = _settings(
        crypto_1h_touch20_replay_min_candidates=50,
        crypto_1h_touch20_asset_settings={"BTC": {"replay_min_candidates": 50}},
    )
    relaxed = _settings(
        crypto_1h_touch20_replay_min_candidates=50,
        crypto_1h_touch20_asset_settings={"BTC": {"replay_min_candidates": 15}},
    )

    reasons = rules._optimizer_non_promotable_reasons(
        base_settings=base,
        profile_settings=relaxed,
        asset_symbol="BTC",
        frequency="1h",
    )
    summary = rules._profile_summary(
        name="current_rules_min_candidates_only",
        settings_overrides={"replay_min_candidates": 15},
        metrics={
            "trade_candidate_count": 101,
            "allowed_trade_candidate_count": 17,
            "allowed_net_simulated_pl_dollars": 1.70,
            "allowed_pnl_per_candidate_dollars": 0.10,
        },
        reasons=[],
        non_promotable_reasons=reasons,
        settings=relaxed,
        asset_symbol="BTC",
        frequency="1h",
    )

    assert reasons == ["replay_min_candidates_relaxed_below_configured_gate"]
    assert summary["status"] == "diagnostic_passed"
    assert summary["passed"] is True
    assert summary["promotable"] is False
    assert summary["promotable_passed"] is False
    assert rules._optimizer_result_status([summary]) == "diagnostic_profile_found"


def test_optimizer_result_status_prefers_promotable_pass():
    diagnostic = {
        "passed": True,
        "promotable": False,
        "promotable_passed": False,
    }
    promotable = {
        "passed": True,
        "promotable": True,
        "promotable_passed": True,
    }

    assert rules._optimizer_result_status([diagnostic, promotable]) == "passed_profile_found"


def test_one_hour_optimizer_fetch_window_matches_loose_profile_entry_window():
    settings = _settings(
        crypto_1h_touch20_min_seconds_to_close=1200,
        crypto_1h_touch20_min_market_age_seconds=60,
    )

    min_seconds_to_close, min_market_age_seconds = rules._optimizer_replay_fetch_window(
        settings,
        asset_symbol="BTC",
        frequency="1h",
    )

    assert min_seconds_to_close == 300
    assert min_market_age_seconds == 60


def test_one_hour_entry_qualified_market_limit_uses_configured_cap():
    settings = _settings(crypto_1h_touch20_entry_qualified_market_limit=250)

    assert rules._entry_qualified_market_limit(settings, frequency="1h", row_limit=50_000) == 250
    assert rules._entry_qualified_market_limit(settings, frequency="1h", row_limit=100) == 100


def test_one_hour_optimizer_profiles_include_coarse_time_bucket_options():
    settings = _settings()

    one_hour_profiles = {
        profile["name"]: dict(profile.get("settings_overrides") or {})
        for profile in rules._optimizer_profile_specs(settings, asset_symbol="BTC", frequency="1h")
    }
    fifteen_minute_profile_names = {
        profile["name"]
        for profile in rules._optimizer_profile_specs(settings, asset_symbol="BTC", frequency="15m")
    }

    coarse = one_hour_profiles["yes_no_take15_maxspread10_time60_price40_open_s25"]
    assert coarse["bucket_time_band_minutes"] == 60
    assert coarse["bucket_price_band_cents"] == 40
    assert coarse["bucket_spread_band_cents"] == 2
    assert one_hour_profiles["yes_take10_maxspread10_time60_price40_open_s25"]["allowed_sides"] == "yes"
    assert one_hour_profiles["no_take10_maxspread10_time60_price40_open_s25"]["allowed_sides"] == "no"
    assert one_hour_profiles["yes_no_take10_stop40_maxspread10_time60_price40_open_s25"]["stop_loss_pct"] == 0.40
    assert "yes_no_take15_maxspread10_time60_price40_open_s25" not in fifteen_minute_profile_names
    assert "yes_take10_maxspread10_time60_price40_open_s25" not in fifteen_minute_profile_names


def test_optimizer_profiles_apply_non_btc_asset_overrides():
    settings = _settings(
        crypto_btc15m_touch20_min_rule_score=0.50,
        crypto_btc15m_touch20_replay_min_candidates=5,
    )
    start = datetime(2026, 6, 1, 12, 5, tzinfo=UTC)
    snapshots = []
    spot_rows = []
    for idx in range(6):
        decision_ts = start + timedelta(minutes=15 * idx)
        market_ticker = f"KXETH15M-OPT-{idx}"
        common = {
            "asset_symbol": "ETH",
            "series_ticker": "KXETH15M",
            "market_ticker": market_ticker,
            "event_ticker": f"KXETH15M-OPT-E{idx}",
            "open_time": decision_ts - timedelta(seconds=60),
            "close_time": decision_ts + timedelta(seconds=840),
            "expected_expiration_time": decision_ts + timedelta(seconds=840),
            "settlement_result": "no",
        }
        snapshots.append(
            _snapshot(
                **common,
                observed_at=decision_ts,
                yes_bid_dollars=Decimal("0.3700"),
                yes_ask_dollars=Decimal("0.3800"),
                no_bid_dollars=Decimal("0.6100"),
                no_ask_dollars=Decimal("0.6200"),
            )
        )
        snapshots.append(
            _snapshot(
                **common,
                observed_at=decision_ts + timedelta(seconds=60),
                yes_bid_dollars=Decimal("0.1800"),
                yes_ask_dollars=Decimal("0.2000"),
                no_bid_dollars=Decimal("0.8000"),
                no_ask_dollars=Decimal("0.8200"),
            )
        )
        spot_rows.append(_spot_row(decision_ts, Decimal("99.00") - Decimal(idx), asset_symbol="ETH"))

    result = rules.optimize_replay_profiles(
        snapshots,
        spot_rows,
        settings=settings,
        asset_symbol="ETH",
        top_n=5,
    )

    assert result["status"] == "passed_profile_found"
    assert result["best_profile"]["passed"] is True
    assert result["best_profile"]["allowed_trade_candidate_count"] >= 5
    assert result["best_profile"]["settings_overrides"]["allowed_sides"] in {"yes,no", "no"}
    assert rules._asset_settings(settings, "ETH").allowed_sides == ("yes",)


def test_optimizer_sort_prefers_profitable_clean_near_miss_over_losing_large_sample():
    profitable_near_miss = {
        "passed": False,
        "reason_count": 1,
        "trade_candidate_count": 12,
        "min_trade_candidates": 50,
        "net_simulated_pl_dollars": 0.88,
        "pnl_per_candidate_dollars": 0.0733,
        "touch_rate": 0.667,
        "allowed_bucket_keys": ["BTC|yes|30_40c|le_1c|10_15m"],
    }
    losing_large_sample = {
        "passed": False,
        "reason_count": 8,
        "trade_candidate_count": 69,
        "min_trade_candidates": 50,
        "net_simulated_pl_dollars": -1.24,
        "pnl_per_candidate_dollars": -0.0180,
        "touch_rate": 0.348,
        "allowed_bucket_keys": ["BTC|yes|30_40c|le_1c|10_15m"],
    }

    profiles = [losing_large_sample, profitable_near_miss]
    profiles.sort(key=rules._optimizer_profile_sort_key, reverse=True)

    assert profiles[0] is profitable_near_miss


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
    settings = _settings(crypto_btc15m_touch20_stop_loss_pct=0.20)

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
    assert rules._entry_ledger_decision("rejected_400", None) == (
        False,
        "entry_rejected_zero_fill",
    )
    assert rules._entry_ledger_decision("failed", Decimal("0")) == (
        False,
        "entry_rejected_zero_fill",
    )


def test_filled_or_pending_entry_status_updates_strategy_ledger():
    assert rules._entry_ledger_decision("canceled", Decimal("0.25")) == (True, "open")
    assert rules._entry_ledger_decision("rejected_400", Decimal("0.25")) == (True, "open")
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
