from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import _crypto_bootstrap_observed_at


def _market(close_time: datetime | None) -> CryptoMarket:
    return CryptoMarket(
        market_ticker="KXBTC-26MAY19-T70000",
        series_ticker="KXBTC",
        asset_symbol="BTC",
        frequency="1h",
        close_time=close_time,
    )


def test_past_close_time_is_returned_unchanged() -> None:
    now = datetime(2026, 5, 19, 20, 0, tzinfo=UTC)
    past = now - timedelta(hours=2)

    result = _crypto_bootstrap_observed_at(_market(past), now=now)

    assert result == past


def test_future_close_time_is_clamped_to_now() -> None:
    now = datetime(2026, 5, 19, 20, 0, tzinfo=UTC)
    future = now + timedelta(days=3)

    result = _crypto_bootstrap_observed_at(_market(future), now=now)

    assert result == now


def test_missing_close_time_falls_back_to_now() -> None:
    now = datetime(2026, 5, 19, 20, 0, tzinfo=UTC)

    result = _crypto_bootstrap_observed_at(_market(None), now=now)

    assert result == now
