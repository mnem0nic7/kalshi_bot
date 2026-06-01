from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CRYPTO_LIVE_QUALITY,
    _btc15m_touch20_rules_net_profit_pct,
    _btc15m_touch20_rules_open_pending_notional,
    _btc15m_touch20_rules_profit_protection_review,
    _crypto_touch20_rules_replay_gate_reasons,
    _crypto_touch_replay_first_touch,
    _crypto_touch_strategy_candidates,
    _touch_strategy_exit_price_for_net_profit,
)


def _settings(**overrides):
    return Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_min_touch_probability=0.50,
        risk_min_edge_bps=0,
        **overrides,
    )


def _row(**overrides):
    base = {
        "market_ticker": "KXBTC15M-26JUN011200-B100000-T100500",
        "asset_symbol": "BTC",
        "frequency": "15m",
        "strict_trade_eligible": True,
        "execution_model_status": "real_quote_taker",
        "yes_bid_dollars": Decimal("0.1900"),
        "yes_ask_dollars": Decimal("0.2000"),
        "no_bid_dollars": Decimal("0.7900"),
        "no_ask_dollars": Decimal("0.8100"),
        "mid_yes_dollars": Decimal("0.1950"),
        "spread_bps": 100,
        "time_to_close_seconds": 300,
        "market_age_seconds": 60,
        "spot_feature_status": "available",
        "spot_proxy_only": False,
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "spot_return_1_pct": Decimal("0.0080"),
        "spot_return_3_pct": Decimal("0.0120"),
        "spot_realized_volatility": Decimal("0.0040"),
        "spot_realized_volatility_32": Decimal("0.0050"),
        "spot_target_distance_volatility": Decimal("1.0000"),
    }
    base.update(overrides)
    return base


def test_btc15m_touch20_rules_candidates_use_20pct_objective_and_no_hard_stop():
    candidates = _crypto_touch_strategy_candidates(
        _row(),
        settings=_settings(),
        btc_15m_touch20_rules_policy=True,
    )

    selected = candidates[0]
    assert selected["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert selected["objective"] == "touch_20pct_before_close"
    assert selected["reason"] == "touch_20pct_before_close_target"
    assert selected["touch_strategy"]["policy"] == "btc15m_touch20_rules"
    assert selected["touch_strategy"]["objective"] == "exitably_up_20pct_before_close"
    assert selected["touch_strategy"]["take_profit_pct"] == 0.20
    assert selected["touch_strategy"]["no_initial_hard_stop"] is True


def test_btc15m_touch20_rules_entry_window_blocks_final_five_minutes_but_allows_boundary():
    settings = _settings()

    blocked = _crypto_touch_strategy_candidates(
        _row(time_to_close_seconds=299),
        settings=settings,
        btc_15m_touch20_rules_policy=True,
    )
    allowed = _crypto_touch_strategy_candidates(
        _row(time_to_close_seconds=300),
        settings=settings,
        btc_15m_touch20_rules_policy=True,
    )

    assert next(candidate for candidate in blocked if candidate["side"] == "yes")["reason"] == "crypto_market_too_late_for_live_entry"
    assert "crypto_market_too_late_for_live_entry" not in {candidate["reason"] for candidate in allowed}


def test_btc15m_touch20_rules_gate_blocks_missing_negative_undersampled_and_passes_supported():
    settings = _settings(
        crypto_btc15m_touch20_replay_min_candidates=50,
        crypto_btc15m_touch20_replay_min_touch_rate=0.25,
        crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars=0.01,
    )
    good_metrics = {
        "uses_trained_model": False,
        "real_quote_path_row_count": 500,
        "trade_candidate_count": 50,
        "rules_live_quality_candidate_count": 50,
        "net_simulated_pl_dollars": 1.00,
        "pnl_per_candidate_dollars": 0.02,
        "touch_rate": 0.25,
        "hard_cap_breaches": 0,
        "allowed_bucket_keys": ["BTC|yes|20_30c|1c|5_10m"],
    }

    assert _crypto_touch20_rules_replay_gate_reasons(good_metrics, settings=settings) == []
    assert "artifact is missing" in _crypto_touch20_rules_replay_gate_reasons({}, settings=settings)[0]
    assert any("candidate count" in reason for reason in _crypto_touch20_rules_replay_gate_reasons({**good_metrics, "trade_candidate_count": 49, "rules_live_quality_candidate_count": 49}, settings=settings))
    assert any("net P/L" in reason for reason in _crypto_touch20_rules_replay_gate_reasons({**good_metrics, "net_simulated_pl_dollars": -0.01}, settings=settings))
    assert any("touch rate" in reason for reason in _crypto_touch20_rules_replay_gate_reasons({**good_metrics, "touch_rate": 0.24}, settings=settings))
    assert any("allowed bucket" in reason for reason in _crypto_touch20_rules_replay_gate_reasons({**good_metrics, "allowed_bucket_keys": []}, settings=settings))
    assert any("trained model" in reason for reason in _crypto_touch20_rules_replay_gate_reasons({**good_metrics, "uses_trained_model": True}, settings=settings))


def test_future_quote_scan_detects_yes_and_no_touch():
    yes_touch = _crypto_touch_replay_first_touch(
        [
            {"yes_bid_dollars": Decimal("0.2300"), "no_bid_dollars": Decimal("0.7100")},
            {"yes_bid_dollars": Decimal("0.2500"), "no_bid_dollars": Decimal("0.7400")},
        ],
        side="yes",
        target_exit_side_price=Decimal("0.2400"),
    )
    no_touch = _crypto_touch_replay_first_touch(
        [
            {"yes_bid_dollars": Decimal("0.2300"), "no_bid_dollars": Decimal("0.7100")},
            {"yes_bid_dollars": Decimal("0.2500"), "no_bid_dollars": Decimal("0.7400")},
        ],
        side="no",
        target_exit_side_price=Decimal("0.7300"),
    )

    assert yes_touch is not None and yes_touch[1] == Decimal("0.2500")
    assert no_touch is not None and no_touch[1] == Decimal("0.7400")


def test_btc15m_touch20_take_profit_uses_net_executable_after_fees():
    settings = _settings()
    target_exit = _touch_strategy_exit_price_for_net_profit(
        Decimal("0.2000"),
        target_pct=Decimal("0.20"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )

    assert target_exit is not None
    at_target = _btc15m_touch20_rules_net_profit_pct(
        entry_side_price=Decimal("0.2000"),
        exit_side_price=target_exit,
        count_fp=Decimal("1.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    below_target = _btc15m_touch20_rules_net_profit_pct(
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

    first = _btc15m_touch20_rules_profit_protection_review(
        entry,
        row=_row(),
        net_profit_pct=Decimal("0.0900"),
        settings=settings,
        now=datetime.now(UTC),
    )
    entry.update(first["entry_updates"])
    second = _btc15m_touch20_rules_profit_protection_review(
        entry,
        row=_row(),
        net_profit_pct=Decimal("0.1100"),
        settings=settings,
        now=datetime.now(UTC),
    )
    entry.update(second["entry_updates"])
    third = _btc15m_touch20_rules_profit_protection_review(
        entry,
        row=_row(),
        net_profit_pct=Decimal("0.0500"),
        settings=settings,
        now=datetime.now(UTC),
    )

    assert first["trigger"] is None
    assert first["entry_updates"]["profit_protection_armed"] is False
    assert second["trigger"] is None
    assert second["entry_updates"]["profit_protection_armed"] is True
    assert third["trigger"] == "profit_protection_floor"


def test_strategy_cap_uses_only_strategy_ledger_entries():
    ledger = {
        "positions": {
            "b15t20r:e:abc": {"status": "open", "entry_notional_dollars": "4.2500"},
            "b15t20r:e:def": {"status": "entry_submitted", "entry_notional_dollars": "1.7500"},
            "manual-or-other-bot": {"status": "open", "entry_notional_dollars": "100.0000"},
        }
    }

    assert _btc15m_touch20_rules_open_pending_notional(ledger) == Decimal("6.0000")
