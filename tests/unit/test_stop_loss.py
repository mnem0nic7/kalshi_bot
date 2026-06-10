from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from datetime import UTC, datetime, timedelta


from kalshi_bot.config import Settings
from kalshi_bot.services.stop_loss import (
    StopLossService, _midpoint, _momentum_slope, _peak_price_from_history,
    _position_opened_at_from_fills, _round_trip_net_return_ratio, _sell_price,
    _strategy_for_market_ticker, _trailing_loss_ratio, exit_retry_delay_seconds,
)


def _ms(yes_bid: str | None, yes_ask: str | None) -> MagicMock:
    ms = MagicMock()
    ms.yes_bid_dollars = Decimal(yes_bid) if yes_bid is not None else None
    ms.yes_ask_dollars = Decimal(yes_ask) if yes_ask is not None else None
    return ms


def _pos(side: str, count: str, avg: str) -> MagicMock:
    pos = MagicMock()
    pos.side = side
    pos.count_fp = Decimal(count)
    pos.average_price_dollars = Decimal(avg)
    return pos


# ── midpoint ────────────────────────────────────────────────────────────────

def test_midpoint_yes():
    ms = _ms("0.50", "0.60")
    assert _midpoint(ms, "yes") == Decimal("0.55")


def test_midpoint_no():
    ms = _ms("0.50", "0.60")
    # mid_yes = 0.55, mid_no = 1 - 0.55 = 0.45
    assert _midpoint(ms, "no") == Decimal("0.45")


def test_midpoint_none_when_bid_missing():
    ms = _ms(None, "0.60")
    assert _midpoint(ms, "yes") is None


def test_midpoint_returns_none_when_ask_missing_yes():
    # Broken book (ask withdrawn near settlement) — skip rather than use stale bid.
    ms = _ms("0.03", None)
    assert _midpoint(ms, "yes") is None


def test_midpoint_returns_none_when_ask_missing_no():
    ms = _ms("0.03", None)
    assert _midpoint(ms, "no") is None


# ── sell price ───────────────────────────────────────────────────────────────

def test_sell_price_yes_is_bid():
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "yes") == Decimal("0.22")


def test_sell_price_none_when_bid_boundary_for_yes():
    assert _sell_price(_ms("0.00", "0.02"), "yes") is None
    assert _sell_price(_ms("1.00", "1.00"), "yes") is None


def test_sell_price_no_is_yes_ask():
    # SELL NO at yes_price_dollars=yes_ask: "fill when NO bid ≥ 1-yes_ask = no_bid". ✓
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "no") == Decimal("0.30")


def test_sell_price_none_when_ask_missing_for_no():
    ms = _ms("0.22", None)
    assert _sell_price(ms, "no") is None


def test_sell_price_none_when_ask_boundary_for_no():
    assert _sell_price(_ms("0.00", "0.00"), "no") is None
    assert _sell_price(_ms("0.98", "1.00"), "no") is None


# -- hard stop return math ----------------------------------------------------

def test_round_trip_net_return_ratio_uses_executable_yes_bid():
    pos = _pos("yes", "1.00", "0.5000")

    ratio = _round_trip_net_return_ratio(
        pos,
        sell_yes_price=Decimal("0.3500"),
        fee_rate=Decimal("0"),
    )

    assert ratio == pytest.approx(-0.30)


def test_round_trip_net_return_ratio_converts_no_sell_yes_price():
    pos = _pos("no", "1.00", "0.5000")

    ratio = _round_trip_net_return_ratio(
        pos,
        sell_yes_price=Decimal("0.6500"),
        fee_rate=Decimal("0"),
    )

    assert ratio == pytest.approx(-0.30)


def test_strategy_for_market_ticker_detects_hourly_d_suffix():
    assert _strategy_for_market_ticker("KXBTCD-26MAY3121-T73799.99") == "CRYPTO_1H"


class _FakeStopLossSession:
    async def __aenter__(self) -> "_FakeStopLossSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def commit(self) -> None:
        return None


class _FakeStopLossRepo:
    def __init__(self, session) -> None:
        self.session = session

    async def get_deployment_control(self, *, kalshi_env: str):
        return MagicMock(kill_switch_enabled=False, active_color="blue")

    async def get_checkpoint(self, key: str):
        return None


@pytest.mark.asyncio
async def test_touch_strategy_crypto_stop_loss_is_hard_stop_only(monkeypatch):
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        crypto_touch_strategy_enabled=True,
        crypto_model_trained_replay_only=False,
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_threshold_pct_by_strategy={"CRYPTO_15M": 0.30},
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock()
    monkeypatch.setattr(service, "_submit", submit)
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    position = _pos("yes", "1.00", "0.3000")
    position.id = 1
    position.market_ticker = "KXBTCD-26MAY261200-15M"
    position.created_at = now - timedelta(minutes=10)
    ms = _ms("0.2300", "0.3700")
    prices = [
        _ph("0.6000", now - timedelta(minutes=9)),
        _ph("0.3000", now - timedelta(minutes=1)),
    ]

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3000"), prices, now)

    assert result is None
    submit.assert_not_called()


@pytest.mark.asyncio
async def test_model_trained_replay_only_keeps_crypto_trailing_stop_active(monkeypatch):
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        crypto_touch_strategy_enabled=True,
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_threshold_pct_by_strategy={"CRYPTO_15M": 0.30},
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock(return_value={"submitted": True})
    monkeypatch.setattr(service, "_submit", submit)
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    position = _pos("yes", "1.00", "0.3000")
    position.id = 1
    position.market_ticker = "KXBTCD-26MAY261200-15M"
    position.created_at = now - timedelta(minutes=10)
    ms = _ms("0.2300", "0.3700")
    prices = [
        _ph("0.6000", now - timedelta(minutes=9)),
        _ph("0.3000", now - timedelta(minutes=1)),
    ]

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3000"), prices, now)

    assert result == {"submitted": True}
    submit.assert_called_once()
    assert submit.call_args.kwargs["trigger"] == "trailing_stop"


@pytest.mark.asyncio
async def test_crypto_1h_profit_protection_requires_profit_threshold_and_adverse_momentum(monkeypatch):
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_enabled_strategies="CRYPTO_1H",
        stop_loss_hard_stop_disabled_strategies="CRYPTO_1H",
        stop_loss_profit_protection_threshold_pct_by_strategy={"CRYPTO_1H": 0.10},
        stop_loss_momentum_min_hold_minutes_by_strategy={"CRYPTO_1H": 5},
        stop_loss_momentum_slope_threshold_cents_per_min=-0.2,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock(return_value={"submitted": True})
    monkeypatch.setattr(service, "_submit", submit)
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    position = _pos("yes", "1.00", "0.5000")
    position.id = 1
    position.market_ticker = "KXBTCD-26JUN0112-T73000"
    position.created_at = now - timedelta(minutes=6)
    ms = _ms("0.5500", "0.5700")
    prices = [
        _ph("0.7000", now - timedelta(minutes=5)),
        _ph("0.6600", now - timedelta(minutes=4)),
        _ph("0.6200", now - timedelta(minutes=3)),
        _ph("0.5900", now - timedelta(minutes=2)),
        _ph("0.5600", now - timedelta(minutes=1)),
    ]

    result = await service._evaluate_and_submit(position, ms, Decimal("0.5600"), prices, now)

    assert result == {"submitted": True}
    submit.assert_called_once()
    assert submit.call_args.kwargs["trigger"] == "profit_protection"


@pytest.mark.asyncio
async def test_crypto_1h_profit_protection_does_not_fire_before_profit_threshold(monkeypatch):
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_enabled_strategies="CRYPTO_1H",
        stop_loss_hard_stop_disabled_strategies="CRYPTO_1H",
        stop_loss_profit_protection_threshold_pct_by_strategy={"CRYPTO_1H": 0.10},
        stop_loss_momentum_min_hold_minutes_by_strategy={"CRYPTO_1H": 5},
        stop_loss_momentum_slope_threshold_cents_per_min=-0.2,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock()
    monkeypatch.setattr(service, "_submit", submit)
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    position = _pos("yes", "1.00", "0.5000")
    position.id = 1
    position.market_ticker = "KXBTCD-26JUN0112-T73000"
    position.created_at = now - timedelta(minutes=6)
    ms = _ms("0.5200", "0.5400")
    prices = [
        _ph("0.6500", now - timedelta(minutes=5)),
        _ph("0.6100", now - timedelta(minutes=4)),
        _ph("0.5800", now - timedelta(minutes=3)),
        _ph("0.5500", now - timedelta(minutes=2)),
        _ph("0.5300", now - timedelta(minutes=1)),
    ]

    result = await service._evaluate_and_submit(position, ms, Decimal("0.5300"), prices, now)

    assert result is None
    submit.assert_not_called()


# ── trailing loss ratio ──────────────────────────────────────────────────────

def test_trailing_loss_ratio_no_drop():
    # peak == current → 0% trailing loss
    assert _trailing_loss_ratio(Decimal("0.80"), Decimal("0.80")) == pytest.approx(0.0)


def test_trailing_loss_ratio_10_pct():
    # peak=0.80, current=0.72 → (0.80-0.72)/0.80 = 0.10
    assert _trailing_loss_ratio(Decimal("0.80"), Decimal("0.72")) == pytest.approx(0.10)


def test_trailing_loss_ratio_exceeds_threshold():
    # peak=0.80, current=0.70 → 12.5% drop
    ratio = _trailing_loss_ratio(Decimal("0.80"), Decimal("0.70"))
    assert ratio > 0.10


def test_trailing_loss_ratio_zero_peak():
    assert _trailing_loss_ratio(Decimal("0.00"), Decimal("0.50")) == 0.0


# ── peak price from history ──────────────────────────────────────────────────

def _ph(mid: str, observed_at: datetime | None = None) -> MagicMock:
    row = MagicMock()
    row.mid_dollars = Decimal(mid)
    row.observed_at = observed_at or datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    return row


def test_peak_price_yes_side():
    rows = [_ph("0.70"), _ph("0.85"), _ph("0.78")]
    assert _peak_price_from_history(rows, "yes") == Decimal("0.85")


def test_peak_price_no_side():
    # NO side price = 1 - mid_yes; peak for NO is where mid_yes is lowest
    rows = [_ph("0.30"), _ph("0.20"), _ph("0.25")]
    # side prices: 0.70, 0.80, 0.75 → peak = 0.80
    assert _peak_price_from_history(rows, "no") == Decimal("0.80")


def test_peak_price_none_on_empty():
    assert _peak_price_from_history([], "yes") is None


def test_peak_price_skips_none_mid():
    rows = [_ph("0.70"), MagicMock(mid_dollars=None), _ph("0.85")]
    assert _peak_price_from_history(rows, "yes") == Decimal("0.85")


def test_peak_price_ignores_highs_before_position_opened():
    opened_at = datetime(2025, 1, 1, 12, 5, tzinfo=UTC)
    rows = [
        _ph("0.92", datetime(2025, 1, 1, 12, 0, tzinfo=UTC)),
        _ph("0.70", datetime(2025, 1, 1, 12, 5, tzinfo=UTC)),
        _ph("0.74", datetime(2025, 1, 1, 12, 6, tzinfo=UTC)),
    ]

    assert _peak_price_from_history(rows, "yes", opened_at=opened_at) == Decimal("0.74")


def _fill(action: str, count: str, created_at: datetime, *, side: str = "yes") -> MagicMock:
    fill = MagicMock()
    fill.market_ticker = "WX-TEST"
    fill.side = side
    fill.action = action
    fill.count_fp = Decimal(count)
    fill.created_at = created_at
    return fill


def test_position_opened_at_from_fills_uses_current_open_lot():
    position = MagicMock()
    position.market_ticker = "WX-TEST"
    position.side = "yes"
    first_open = datetime(2025, 1, 1, 10, 0, tzinfo=UTC)
    closed = datetime(2025, 1, 1, 10, 30, tzinfo=UTC)
    reopened = datetime(2025, 1, 1, 11, 15, tzinfo=UTC)

    opened_at = _position_opened_at_from_fills(
        position,
        [
            _fill("buy", "5.00", first_open),
            _fill("sell", "5.00", closed),
            _fill("buy", "3.00", reopened),
            _fill("buy", "2.00", reopened + timedelta(minutes=5)),
        ],
    )

    assert opened_at == reopened


# ── momentum slope ───────────────────────────────────────────────────────────

def _price_rows(slope_cents_per_min: float, n: int = 10, start_price: float = 0.50) -> list:
    """Build synthetic MarketPriceHistory-like mocks with a given slope in ¢/min."""
    base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(n):
        row = MagicMock()
        t = base + timedelta(seconds=i * 60)
        row.observed_at = t
        price_dollars = start_price + (slope_cents_per_min / 100) * i  # ¢/min → $/step (1 step = 1 min)
        row.mid_dollars = Decimal(str(round(price_dollars, 6)))
        rows.append(row)
    return rows


def test_momentum_slope_flat():
    rows = _price_rows(0.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert abs(slope) < 0.01  # flat ≈ 0 ¢/min


def test_momentum_slope_negative():
    rows = _price_rows(-1.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope == pytest.approx(-1.0, abs=0.05)


def test_momentum_slope_positive():
    rows = _price_rows(2.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope == pytest.approx(2.0, abs=0.05)


def test_momentum_slope_none_on_too_few_points():
    rows = _price_rows(-1.0, n=4)
    assert _momentum_slope(rows) is None


def test_momentum_slope_none_when_mid_dollars_missing():
    rows = _price_rows(-1.0, n=10)
    for row in rows:
        row.mid_dollars = None
    assert _momentum_slope(rows) is None


def test_momentum_slope_ignores_none_mid_dollars():
    rows = _price_rows(-1.0, n=10)
    # Null out 4 rows, leaving 6 valid — still enough
    for row in rows[:4]:
        row.mid_dollars = None
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope < 0


# ── minimum hard-stop hold window ────────────────────────────────────────────


def _pos_with_created(side: str, count: str, avg: str, created_at: datetime) -> MagicMock:
    pos = _pos(side, count, avg)
    pos.id = 99
    pos.market_ticker = "KXETH15M-26JUN091200-00"
    pos.created_at = created_at
    return pos


@pytest.mark.asyncio
async def test_hard_stop_blocked_within_hold_window(monkeypatch):
    """Hard stop must not fire when position age < min_hard_stop_hold_seconds."""
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_min_hard_stop_hold_seconds=90,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock()
    monkeypatch.setattr(service, "_submit", submit)

    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    # Position is only 60 seconds old — within the 90s hold window.
    position = _pos_with_created("yes", "1.00", "0.4600", now - timedelta(seconds=60))
    # Mid has crashed 35% below entry — would normally trigger hard stop.
    ms = _ms("0.2800", "0.3400")

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3100"), [], now)

    assert result is None
    submit.assert_not_called()


@pytest.mark.asyncio
async def test_hard_stop_fires_after_hold_window(monkeypatch):
    """Hard stop fires once position age >= min_hard_stop_hold_seconds."""
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_min_hard_stop_hold_seconds=90,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock(return_value={"submitted": True})
    monkeypatch.setattr(service, "_submit", submit)

    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    # Position is 91 seconds old — just past the 90s hold window.
    position = _pos_with_created("yes", "1.00", "0.4600", now - timedelta(seconds=91))
    ms = _ms("0.2800", "0.3400")

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3100"), [], now)

    assert result == {"submitted": True}
    submit.assert_called_once()
    assert submit.call_args.kwargs["trigger"] == "hard_stop"


@pytest.mark.asyncio
async def test_trailing_stop_blocked_within_hold_window(monkeypatch):
    """Trailing stop must not fire when position age < min_hard_stop_hold_seconds."""
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_min_hard_stop_hold_seconds=90,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock()
    monkeypatch.setattr(service, "_submit", submit)

    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    # Position is 60s old — within hold window.
    position = _pos_with_created("yes", "1.00", "0.3000", now - timedelta(seconds=60))
    ms = _ms("0.2300", "0.3700")
    # Price history shows a peak at 0.60 followed by a drop to 0.30 — 50% trailing drop.
    prices = [
        _ph("0.6000", now - timedelta(seconds=55)),
        _ph("0.3000", now - timedelta(seconds=5)),
    ]

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3000"), prices, now)

    assert result is None
    submit.assert_not_called()


@pytest.mark.asyncio
async def test_rapid_adverse_fires_within_hold_window(monkeypatch):
    """Rapid adverse bypasses the hold gate — it is the crash circuit-breaker."""
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_rapid_adverse_enabled=True,
        stop_loss_rapid_adverse_dollars=0.07,
        stop_loss_min_hard_stop_hold_seconds=90,
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock(return_value={"submitted": True})
    monkeypatch.setattr(service, "_submit", submit)

    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    # Position is only 65 seconds old — inside the 90s hold window.
    position = _pos_with_created("yes", "1.00", "0.4600", now - timedelta(seconds=65))
    # Sell price is valid and hard stop has NOT triggered (mid still close to entry).
    ms = _ms("0.3900", "0.4500")
    # Two consecutive 7¢ drops recorded in prev_mids.
    prev_mids = [
        (Decimal("0.5300"), now - timedelta(seconds=60)),
        (Decimal("0.4600"), now - timedelta(seconds=30)),
    ]
    # Current mid is 0.39 — drop2 = 0.46 - 0.39 = 0.07, drop1 = 0.53 - 0.46 = 0.07.

    result = await service._evaluate_and_submit(
        position, ms, Decimal("0.3900"), [], now, prev_mids=prev_mids
    )

    assert result == {"submitted": True}
    submit.assert_called_once()
    assert submit.call_args.kwargs["trigger"] == "rapid_adverse_move"


@pytest.mark.asyncio
async def test_per_strategy_hold_override_crypto_15m(monkeypatch):
    """Per-strategy hold takes precedence over the global default."""
    monkeypatch.setattr("kalshi_bot.services.stop_loss.PlatformRepository", _FakeStopLossRepo)
    settings = Settings(
        app_color="blue",
        kalshi_taker_fee_rate=0,
        stop_loss_threshold_pct=0.30,
        stop_loss_min_hard_stop_hold_seconds=0,           # global: no hold
        stop_loss_min_hard_stop_hold_seconds_by_strategy={"CRYPTO_15M": 90},  # 15m override
    )
    service = StopLossService(settings, lambda: _FakeStopLossSession(), MagicMock())
    submit = AsyncMock()
    monkeypatch.setattr(service, "_submit", submit)

    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    # 60s old CRYPTO_15M position — global hold=0 would allow stop, but 15m override=90 blocks it.
    position = _pos_with_created("yes", "1.00", "0.4600", now - timedelta(seconds=60))
    position.market_ticker = "KXBTC15M-26JUN091200-00"
    ms = _ms("0.2800", "0.3400")

    result = await service._evaluate_and_submit(position, ms, Decimal("0.3100"), [], now)

    assert result is None
    submit.assert_not_called()


# ── exit retry delay ─────────────────────────────────────────────────────────


def test_exit_retry_delay_uses_strategy_override():
    settings = Settings(
        stop_loss_retry_seconds=1800,
        stop_loss_retry_seconds_by_strategy={"CRYPTO_15M": 60},
    )
    assert exit_retry_delay_seconds(settings, "KXETH15M-26JUN091200-00") == 60


def test_exit_retry_delay_falls_back_to_base_for_unlisted_strategy():
    settings = Settings(
        stop_loss_retry_seconds=1800,
        stop_loss_retry_seconds_by_strategy={"CRYPTO_15M": 60},
    )
    # KXBTCD is the hourly series → CRYPTO_1H, not in the override dict.
    assert exit_retry_delay_seconds(settings, "KXBTCD-26JUN0917-T106000") == 1800


def test_exit_retry_delay_floors_at_10_seconds():
    settings = Settings(
        stop_loss_retry_seconds=0,
        stop_loss_retry_seconds_by_strategy={"CRYPTO_15M": 1},
    )
    assert exit_retry_delay_seconds(settings, "KXETH15M-26JUN091200-00") == 10
    assert exit_retry_delay_seconds(settings, "KXBTCD-26JUN0917-T106000") == 10


# ── stale market state refresh ───────────────────────────────────────────────


def _exec_with_market(payload):
    exec_svc = MagicMock()
    exec_svc.kalshi.get_market = AsyncMock(return_value=payload)
    return exec_svc


@pytest.mark.asyncio
async def test_stale_refresh_builds_state_from_live_quote():
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market({"market": {"status": "active", "yes_bid": 41, "yes_ask": 45}}),
    )
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    ms = await service._refresh_stale_market_state("KXETH15M-26JUN091200-00", now)

    assert ms is not None
    assert ms.yes_bid_dollars == Decimal("0.41")
    assert ms.yes_ask_dollars == Decimal("0.45")
    assert ms.observed_at == now
    assert ms.source == "stop_loss_stale_refresh"


@pytest.mark.asyncio
async def test_stale_refresh_returns_none_for_closed_market():
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market({"market": {"status": "settled", "yes_bid": 41, "yes_ask": 45}}),
    )
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    assert await service._refresh_stale_market_state("KXETH15M-26JUN091200-00", now) is None


@pytest.mark.asyncio
async def test_stale_refresh_returns_none_for_broken_book():
    # Missing ask = market maker withdrew; same skip semantics as _midpoint.
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market({"market": {"status": "active", "yes_bid": 41, "yes_ask": None}}),
    )
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    assert await service._refresh_stale_market_state("KXETH15M-26JUN091200-00", now) is None


@pytest.mark.asyncio
async def test_stale_refresh_returns_none_for_boundary_prices():
    # yes_bid=0 / yes_ask=100 cents is a degenerate book, not an exitable quote.
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market({"market": {"status": "active", "yes_bid": 0, "yes_ask": 100}}),
    )
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    assert await service._refresh_stale_market_state("KXETH15M-26JUN091200-00", now) is None


@pytest.mark.asyncio
async def test_stale_refresh_returns_none_on_api_error():
    exec_svc = MagicMock()
    exec_svc.kalshi.get_market = AsyncMock(side_effect=RuntimeError("api down"))
    service = StopLossService(Settings(app_color="blue"), MagicMock(), exec_svc)
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)

    assert await service._refresh_stale_market_state("KXETH15M-26JUN091200-00", now) is None


@pytest.mark.asyncio
async def test_stale_refresh_parses_fractional_dollar_quote_fields():
    # tapered_deci_cent markets (15m crypto) publish *_dollars strings and omit
    # the legacy integer-cent fields entirely — the 2026-06-10 BNB incident.
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market(
            {
                "market": {
                    "status": "active",
                    "price_level_structure": "tapered_deci_cent",
                    "yes_bid_dollars": "0.1500",
                    "yes_ask_dollars": "0.1900",
                }
            }
        ),
    )
    now = datetime(2026, 6, 10, 16, 5, tzinfo=UTC)

    ms = await service._refresh_stale_market_state("KXBNB15M-26JUN101215-15", now)

    assert ms is not None
    assert ms.yes_bid_dollars == Decimal("0.1500")
    assert ms.yes_ask_dollars == Decimal("0.1900")


@pytest.mark.asyncio
async def test_stale_refresh_prefers_dollar_fields_over_cent_fields():
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market(
            {
                "market": {
                    "status": "active",
                    "yes_bid_dollars": "0.3825",
                    "yes_ask_dollars": "0.4100",
                    "yes_bid": 38,
                    "yes_ask": 41,
                }
            }
        ),
    )
    now = datetime(2026, 6, 10, 16, 5, tzinfo=UTC)

    ms = await service._refresh_stale_market_state("KXBNB15M-26JUN101215-15", now)

    assert ms is not None
    assert ms.yes_bid_dollars == Decimal("0.3825")
    assert ms.yes_ask_dollars == Decimal("0.4100")


@pytest.mark.asyncio
async def test_stale_refresh_dollar_fields_boundary_returns_none():
    service = StopLossService(
        Settings(app_color="blue"),
        MagicMock(),
        _exec_with_market(
            {
                "market": {
                    "status": "active",
                    "yes_bid_dollars": "0.0000",
                    "yes_ask_dollars": "1.0000",
                }
            }
        ),
    )
    now = datetime(2026, 6, 10, 16, 5, tzinfo=UTC)

    assert await service._refresh_stale_market_state("KXBNB15M-26JUN101215-15", now) is None


# ── filled-exit cooldown (2026-06-10 BNB position-flip incidents) ────────────

from kalshi_bot.services.stop_loss import _within_filled_exit_cooldown


def test_filled_exit_cooldown_blocks_within_window():
    settings = Settings(app_color="blue", position_exit_filled_cooldown_seconds=180)
    now = datetime(2026, 6, 10, 18, 8, 0, tzinfo=UTC)
    payload = {"submitted_at": "2026-06-10T18:07:22+00:00", "outcome_status": "filled_exit"}

    assert _within_filled_exit_cooldown(payload, now, settings) is True


def test_filled_exit_cooldown_expires_after_window():
    settings = Settings(app_color="blue", position_exit_filled_cooldown_seconds=180)
    now = datetime(2026, 6, 10, 18, 11, 0, tzinfo=UTC)
    payload = {"submitted_at": "2026-06-10T18:07:22+00:00"}

    assert _within_filled_exit_cooldown(payload, now, settings) is False


def test_filled_exit_cooldown_disabled_when_zero():
    settings = Settings(app_color="blue", position_exit_filled_cooldown_seconds=0)
    now = datetime(2026, 6, 10, 18, 7, 30, tzinfo=UTC)
    payload = {"submitted_at": "2026-06-10T18:07:22+00:00"}

    assert _within_filled_exit_cooldown(payload, now, settings) is False


def test_filled_exit_cooldown_handles_missing_or_bad_timestamp():
    settings = Settings(app_color="blue", position_exit_filled_cooldown_seconds=180)
    now = datetime(2026, 6, 10, 18, 8, 0, tzinfo=UTC)

    assert _within_filled_exit_cooldown({}, now, settings) is False
    assert _within_filled_exit_cooldown({"submitted_at": "garbage"}, now, settings) is False
    assert _within_filled_exit_cooldown(None, now, settings) is False


def test_filled_exit_cooldown_falls_back_to_stopped_at():
    settings = Settings(app_color="blue", position_exit_filled_cooldown_seconds=180)
    now = datetime(2026, 6, 10, 18, 8, 0, tzinfo=UTC)
    payload = {"stopped_at": "2026-06-10T18:07:22+00:00"}

    assert _within_filled_exit_cooldown(payload, now, settings) is True


# ── hot exit loop: reading spacing + adaptive interval ───────────────────────

from kalshi_bot.services.stop_loss import _append_spaced_mid_reading


def test_spaced_reading_skipped_inside_spacing_window():
    now = datetime(2026, 6, 10, 18, 0, 0, tzinfo=UTC)
    readings = [(Decimal("0.40"), now)]

    _append_spaced_mid_reading(readings, Decimal("0.33"), now + timedelta(seconds=1), 15)

    assert len(readings) == 1  # 1s after the last reading: not appended


def test_spaced_reading_appended_after_spacing_and_capped_at_two():
    now = datetime(2026, 6, 10, 18, 0, 0, tzinfo=UTC)
    readings = [(Decimal("0.40"), now), (Decimal("0.38"), now + timedelta(seconds=15))]

    _append_spaced_mid_reading(readings, Decimal("0.30"), now + timedelta(seconds=30), 15)

    assert len(readings) == 2
    assert readings[-1][0] == Decimal("0.30")
    assert readings[0][0] == Decimal("0.38")


def test_spaced_reading_zero_spacing_appends_every_time():
    now = datetime(2026, 6, 10, 18, 0, 0, tzinfo=UTC)
    readings = [(Decimal("0.40"), now)]

    _append_spaced_mid_reading(readings, Decimal("0.39"), now + timedelta(milliseconds=100), 0)

    assert len(readings) == 2
