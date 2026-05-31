from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CRYPTO_LIVE_QUALITY,
    _crypto_recommendation,
    _crypto_touch_strategy_candidates,
    _touch_strategy_exit_price_for_net_profit,
)


def _row(**overrides):
    base = {
        "market_ticker": "KXBTC15M-26MAY26-B100000-T100500",
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
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
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


def test_touch_target_price_requires_more_than_gross_30pct_after_fees():
    target = _touch_strategy_exit_price_for_net_profit(
        Decimal("0.2000"),
        target_pct=Decimal("0.30"),
        fee_rate=Decimal("0.07"),
    )

    assert target is not None
    assert target > Decimal("0.2600")


def test_touch_strategy_selects_live_candidate_when_probability_and_spread_clear():
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_enabled=True,
        crypto_model_trained_replay_only=False,
        crypto_touch_strategy_min_touch_probability=0.54,
        risk_min_edge_bps=200,
        crypto_live_min_market_age_seconds=180,
        crypto_autonomy_min_seconds_to_close=0,
    )

    candidates = _crypto_touch_strategy_candidates(_row(), settings=settings)

    assert candidates[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert candidates[0]["reason"] == "touch_30pct_target_before_stop"
    assert candidates[0]["touch_strategy"]["objective"] == "exitably_up_30pct_before_close"


def test_touch_strategy_supports_one_hour_frequency_without_training_artifact():
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_enabled=True,
        crypto_model_trained_replay_only=False,
        crypto_touch_strategy_min_touch_probability=0.54,
        risk_min_edge_bps=200,
        crypto_live_min_market_age_seconds=180,
        crypto_autonomy_min_seconds_to_close=0,
    )

    action, side, target_yes, _edge_bps, trace = _crypto_recommendation(
        market=SimpleNamespace(spread_bps=100),
        fair_yes=Decimal("0.5000"),
        settings=settings,
        row=_row(
            market_ticker="KXBTC-26MAY2615-B100000-T100500",
            frequency="1h",
            time_to_close_seconds=2400,
        ),
    )

    assert action is not None
    assert side is not None
    assert target_yes is not None
    assert trace["objective"] == "touch_30pct_before_close"
    assert trace["touch_strategy"]["objective"] == "exitably_up_30pct_before_close"


def test_touch_strategy_blocks_wide_low_price_spreads():
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_enabled=True,
        crypto_model_trained_replay_only=False,
        crypto_touch_strategy_min_touch_probability=0.50,
        risk_min_edge_bps=0,
        crypto_live_min_market_age_seconds=180,
        crypto_autonomy_min_seconds_to_close=0,
    )

    candidates = _crypto_touch_strategy_candidates(
        _row(
            yes_bid_dollars=Decimal("0.1700"),
            yes_ask_dollars=Decimal("0.1900"),
            mid_yes_dollars=Decimal("0.1800"),
            spread_bps=200,
        ),
        settings=settings,
    )

    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")
    assert yes_candidate["reason"] == "touch_spread_above_tier_max"


def test_touch_recommendation_blocked_trace_does_not_require_model_prediction():
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_enabled=True,
        crypto_model_trained_replay_only=False,
        crypto_touch_strategy_min_touch_probability=0.95,
        risk_min_edge_bps=0,
        crypto_live_min_market_age_seconds=180,
        crypto_autonomy_min_seconds_to_close=0,
    )

    action, side, target_yes, edge_bps, trace = _crypto_recommendation(
        market=SimpleNamespace(spread_bps=100),
        fair_yes=Decimal("0.5000"),
        settings=settings,
        row=_row(),
    )

    assert action is None
    assert side is None
    assert target_yes is None
    assert edge_bps >= 0
    assert trace["outcome"] == "touch_strategy_blocked"


def test_model_trained_replay_only_ignores_touch_recommendation_flag():
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        crypto_touch_strategy_enabled=True,
        risk_min_edge_bps=0,
        crypto_live_min_market_age_seconds=180,
        crypto_autonomy_min_seconds_to_close=0,
    )

    action, side, target_yes, edge_bps, trace = _crypto_recommendation(
        market=SimpleNamespace(spread_bps=100),
        fair_yes=Decimal("0.5000"),
        settings=settings,
        row=_row(),
    )

    assert action is None
    assert side is None
    assert target_yes is None
    assert isinstance(edge_bps, int)
    assert trace.get("objective") != "touch_30pct_before_close"
    assert "touch_strategy" not in trace
    assert trace["last_minute_passive"]["reason"] == "model_trained_replay_only"
