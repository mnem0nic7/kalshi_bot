from __future__ import annotations

import hashlib
import json
import logging
import math
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from functools import lru_cache
from typing import Any, Iterable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.fixed_point import quantize_count, quantize_price
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.crypto.parsing import normalize_frequency
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, CryptoSpotOHLCRecord, Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.fee_model import current_fee_model_version, estimate_kalshi_taker_fee_dollars

logger = logging.getLogger(__name__)

BTC15M_TOUCH20_RULES_STRATEGY = "btc15m_touch20_rules"
BTC15M_TOUCH20_RULES_ORDER_PREFIX = "b15t20r"
BTC15M_TOUCH20_RULES_BACKTEST_ARTIFACT = "btc15m_touch20_rules_backtest"
BTC15M_TOUCH20_RULES_GATE_ARTIFACT = "btc15m_touch20_rules_gate"
BTC15M_TOUCH20_RULES_FREQ = "15m"
BTC15M_TOUCH20_RULES_ASSET = "BTC"
BTC15M_TOUCH20_RULES_INTERVAL_SECONDS = 900
TOUCH20_RULES_SUPPORTED_FREQUENCIES = {
    BTC15M_TOUCH20_RULES_FREQ: BTC15M_TOUCH20_RULES_INTERVAL_SECONDS,
    "1h": 3600,
}
TOUCH20_RULES_SUPPORTED_ASSETS = frozenset({"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"})
TOUCH20_RULES_REPLAY_SIMULATOR_VERSION = "live_exit_v3"
TOUCH20_RULES_REMEDIATION_BLOCKED_BTC_BUCKETS = frozenset(
    {
        "BTC|yes|50_60c|le_1c|10_15m",
        "BTC|no|30_40c|le_1c|10_15m",
    }
)


@dataclass(frozen=True)
class Touch20AssetSettings:
    rules_enabled: bool
    trading_enabled: bool
    allowed_sides: tuple[str, ...]
    take_profit_pct: Decimal
    stop_loss_pct: Decimal
    min_market_age_seconds: int
    min_seconds_to_close: int
    replay_min_candidates: int
    replay_min_touch_rate: Decimal
    replay_min_net_pnl_dollars: Decimal
    replay_min_pnl_per_candidate_dollars: Decimal
    replay_max_hard_cap_breaches: int
    max_open_notional_dollars: Decimal
    daily_loss_limit_dollars: Decimal
    min_order_notional_dollars: Decimal
    max_bucket_live_loss_dollars: Decimal
    max_bucket_consecutive_losses: int
    max_replay_stop_loss_rate: Decimal
    max_replay_terminal_loss_rate: Decimal
    profit_protection_threshold_pct: Decimal
    profit_protection_floor_pct: Decimal
    loop_interval_seconds: int
    min_contract_price_dollars: Decimal
    max_contract_price_dollars: Decimal
    max_spread_dollars: Decimal
    min_aligned_momentum: Decimal
    min_rule_score: Decimal
    bucket_price_band_cents: int
    bucket_spread_band_cents: int
    bucket_time_band_minutes: int
    quote_fresh_seconds: int
    spot_fresh_seconds: int


def _version(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{digest}"


def _normalize_asset_symbol(asset_symbol: str | None) -> str:
    return "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())


def _normalize_touch_frequency(frequency: str | None) -> str:
    return normalize_frequency(frequency) or BTC15M_TOUCH20_RULES_FREQ


def _frequency_interval_seconds(frequency: str | None) -> int:
    return TOUCH20_RULES_SUPPORTED_FREQUENCIES.get(
        _normalize_touch_frequency(frequency),
        BTC15M_TOUCH20_RULES_INTERVAL_SECONDS,
    )


def _is_legacy_btc15m_scope(asset_symbol: str | None, frequency: str | None) -> bool:
    return (
        _normalize_asset_symbol(asset_symbol) == BTC15M_TOUCH20_RULES_ASSET
        and _normalize_touch_frequency(frequency) == BTC15M_TOUCH20_RULES_FREQ
    )


def _strategy_label(asset_symbol: str | None, frequency: str | None) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    return f"{asset} {freq} touch20 rules"


def _settings_prefix(frequency: str | None) -> str:
    return "crypto_1h_touch20" if _normalize_touch_frequency(frequency) == "1h" else "crypto_btc15m_touch20"


def _touch_setting(settings: Settings, frequency: str | None, name: str, default: Any) -> Any:
    return getattr(settings, f"{_settings_prefix(frequency)}_{name}", default)


def _configured_assets(settings: Settings, frequency: str | None = BTC15M_TOUCH20_RULES_FREQ) -> list[str]:
    freq = _normalize_touch_frequency(frequency)
    setting_name = "crypto_1h_touch20_rules_assets" if freq == "1h" else "crypto_15m_touch20_rules_assets"
    raw = str(getattr(settings, setting_name, "") or "").replace(";", ",")
    assets = [_normalize_asset_symbol(item) for item in raw.split(",") if _normalize_asset_symbol(item)]
    return assets or [BTC15M_TOUCH20_RULES_ASSET]


def _strategy_code(asset_symbol: str | None, frequency: str | None = BTC15M_TOUCH20_RULES_FREQ) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    if _is_legacy_btc15m_scope(asset, freq):
        return BTC15M_TOUCH20_RULES_STRATEGY
    return f"{asset.lower()}{freq}_touch20_rules"


def _order_prefix(asset_symbol: str | None, frequency: str | None = BTC15M_TOUCH20_RULES_FREQ) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    if _is_legacy_btc15m_scope(asset, freq):
        return BTC15M_TOUCH20_RULES_ORDER_PREFIX
    token = "15" if freq == BTC15M_TOUCH20_RULES_FREQ else freq
    return f"{asset.lower()}{token}t20r"


def _artifact_base(kind: str, asset_symbol: str | None, frequency: str | None = BTC15M_TOUCH20_RULES_FREQ) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    if _is_legacy_btc15m_scope(asset, freq):
        return BTC15M_TOUCH20_RULES_GATE_ARTIFACT if kind == "gate" else BTC15M_TOUCH20_RULES_BACKTEST_ARTIFACT
    return f"{_strategy_code(asset, frequency=freq)}_{kind}"


def _asset_overrides(
    settings: Settings,
    asset_symbol: str | None,
    frequency: str | None = BTC15M_TOUCH20_RULES_FREQ,
) -> dict[str, Any]:
    freq = _normalize_touch_frequency(frequency)
    setting_name = "crypto_1h_touch20_asset_settings" if freq == "1h" else "crypto_15m_touch20_asset_settings"
    raw = getattr(settings, setting_name, {}) or {}
    if not isinstance(raw, dict):
        return {}
    asset = _normalize_asset_symbol(asset_symbol)
    for key, value in raw.items():
        if _normalize_asset_symbol(str(key)) == asset and isinstance(value, dict):
            return dict(value)
    return {}


def _bool_override(overrides: dict[str, Any], key: str, default: bool) -> bool:
    if key not in overrides:
        return default
    value = overrides.get(key)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "live", "enabled"}


def _decimal_override(overrides: dict[str, Any], key: str, default: Decimal) -> Decimal:
    return _decimal(overrides.get(key), default) if key in overrides else default


def _int_override(overrides: dict[str, Any], key: str, default: int) -> int:
    if key not in overrides:
        return default
    try:
        return int(float(overrides[key]))
    except (TypeError, ValueError):
        return default


def _bucket_price_band_cents(value: int) -> int:
    return value if value in {10, 20, 30, 40} else 10


def _bucket_spread_band_cents(value: int) -> int:
    return 2 if value >= 2 else 1


def _bucket_time_band_minutes(value: int) -> int:
    if value >= 60:
        return 60
    if value >= 30:
        return 30
    if value >= 15:
        return 15
    if value >= 10:
        return 10
    return 5


def _side_tuple_override(overrides: dict[str, Any], key: str, default: str) -> tuple[str, ...]:
    raw = overrides.get(key, default)
    if isinstance(raw, (list, tuple, set)):
        values = raw
    else:
        values = str(raw or "").replace(";", ",").split(",")
    sides = tuple(side for side in (str(value).strip().lower() for value in values) if side in {"yes", "no"})
    return sides or tuple(
        side for side in (str(value).strip().lower() for value in default.replace(";", ",").split(",")) if side in {"yes", "no"}
    )


def _asset_settings(
    settings: Settings,
    asset_symbol: str | None,
    frequency: str | None = BTC15M_TOUCH20_RULES_FREQ,
) -> Touch20AssetSettings:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    overrides = _asset_overrides(settings, asset, frequency=freq)
    is_btc = asset == BTC15M_TOUCH20_RULES_ASSET
    enabled_default = bool(_touch_setting(settings, freq, "rules_enabled", settings.crypto_btc15m_touch20_rules_enabled)) if is_btc else False
    trading_default = (
        bool(_touch_setting(settings, freq, "rules_trading_enabled", settings.crypto_btc15m_touch20_rules_trading_enabled))
        if is_btc
        else False
    )
    return Touch20AssetSettings(
        rules_enabled=_bool_override(overrides, "rules_enabled", enabled_default),
        trading_enabled=_bool_override(overrides, "trading_enabled", trading_default),
        allowed_sides=_side_tuple_override(overrides, "allowed_sides", str(_touch_setting(settings, freq, "allowed_sides", settings.crypto_btc15m_touch20_allowed_sides))),
        take_profit_pct=_decimal_override(overrides, "take_profit_pct", Decimal(str(_touch_setting(settings, freq, "take_profit_pct", settings.crypto_btc15m_touch20_take_profit_pct)))),
        stop_loss_pct=_decimal_override(overrides, "stop_loss_pct", Decimal(str(_touch_setting(settings, freq, "stop_loss_pct", settings.crypto_btc15m_touch20_stop_loss_pct)))),
        min_market_age_seconds=_int_override(overrides, "min_market_age_seconds", int(_touch_setting(settings, freq, "min_market_age_seconds", settings.crypto_btc15m_touch20_min_market_age_seconds))),
        min_seconds_to_close=_int_override(overrides, "min_seconds_to_close", int(_touch_setting(settings, freq, "min_seconds_to_close", settings.crypto_btc15m_touch20_min_seconds_to_close))),
        replay_min_candidates=_int_override(overrides, "replay_min_candidates", int(_touch_setting(settings, freq, "replay_min_candidates", settings.crypto_btc15m_touch20_replay_min_candidates))),
        replay_min_touch_rate=_decimal_override(overrides, "replay_min_touch_rate", Decimal(str(_touch_setting(settings, freq, "replay_min_touch_rate", settings.crypto_btc15m_touch20_replay_min_touch_rate)))),
        replay_min_net_pnl_dollars=_decimal_override(overrides, "replay_min_net_pnl_dollars", Decimal(str(_touch_setting(settings, freq, "replay_min_net_pnl_dollars", settings.crypto_btc15m_touch20_replay_min_net_pnl_dollars)))),
        replay_min_pnl_per_candidate_dollars=_decimal_override(overrides, "replay_min_pnl_per_candidate_dollars", Decimal(str(_touch_setting(settings, freq, "replay_min_pnl_per_candidate_dollars", settings.crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars)))),
        replay_max_hard_cap_breaches=_int_override(overrides, "replay_max_hard_cap_breaches", int(_touch_setting(settings, freq, "replay_max_hard_cap_breaches", settings.crypto_btc15m_touch20_replay_max_hard_cap_breaches))),
        max_open_notional_dollars=_decimal_override(overrides, "max_open_notional_dollars", Decimal(str(_touch_setting(settings, freq, "max_open_notional_dollars", settings.crypto_btc15m_touch20_max_open_notional_dollars)))),
        daily_loss_limit_dollars=_decimal_override(overrides, "daily_loss_limit_dollars", Decimal(str(_touch_setting(settings, freq, "daily_loss_limit_dollars", settings.crypto_btc15m_touch20_daily_loss_limit_dollars)))),
        min_order_notional_dollars=_decimal_override(overrides, "min_order_notional_dollars", Decimal(str(_touch_setting(settings, freq, "min_order_notional_dollars", settings.crypto_btc15m_touch20_min_order_notional_dollars)))),
        max_bucket_live_loss_dollars=_decimal_override(overrides, "max_bucket_live_loss_dollars", Decimal(str(_touch_setting(settings, freq, "max_bucket_live_loss_dollars", settings.crypto_btc15m_touch20_max_bucket_live_loss_dollars)))),
        max_bucket_consecutive_losses=_int_override(overrides, "max_bucket_consecutive_losses", int(_touch_setting(settings, freq, "max_bucket_consecutive_losses", settings.crypto_btc15m_touch20_max_bucket_consecutive_losses))),
        max_replay_stop_loss_rate=_decimal_override(overrides, "max_replay_stop_loss_rate", Decimal(str(_touch_setting(settings, freq, "max_replay_stop_loss_rate", settings.crypto_btc15m_touch20_max_replay_stop_loss_rate)))),
        max_replay_terminal_loss_rate=_decimal_override(overrides, "max_replay_terminal_loss_rate", Decimal(str(_touch_setting(settings, freq, "max_replay_terminal_loss_rate", settings.crypto_btc15m_touch20_max_replay_terminal_loss_rate)))),
        profit_protection_threshold_pct=_decimal_override(overrides, "profit_protection_threshold_pct", Decimal(str(_touch_setting(settings, freq, "profit_protection_threshold_pct", settings.crypto_btc15m_touch20_profit_protection_threshold_pct)))),
        profit_protection_floor_pct=_decimal_override(overrides, "profit_protection_floor_pct", Decimal(str(_touch_setting(settings, freq, "profit_protection_floor_pct", settings.crypto_btc15m_touch20_profit_protection_floor_pct)))),
        loop_interval_seconds=_int_override(overrides, "loop_interval_seconds", int(_touch_setting(settings, freq, "loop_interval_seconds", settings.crypto_btc15m_touch20_loop_interval_seconds))),
        min_contract_price_dollars=_decimal_override(overrides, "min_contract_price_dollars", Decimal(str(_touch_setting(settings, freq, "min_contract_price_dollars", settings.crypto_btc15m_touch20_min_contract_price_dollars)))),
        max_contract_price_dollars=_decimal_override(overrides, "max_contract_price_dollars", Decimal(str(_touch_setting(settings, freq, "max_contract_price_dollars", settings.crypto_btc15m_touch20_max_contract_price_dollars)))),
        max_spread_dollars=_decimal_override(overrides, "max_spread_dollars", Decimal(str(_touch_setting(settings, freq, "max_spread_dollars", settings.crypto_btc15m_touch20_max_spread_dollars)))),
        min_aligned_momentum=_decimal_override(overrides, "min_aligned_momentum", Decimal(str(_touch_setting(settings, freq, "min_aligned_momentum", settings.crypto_btc15m_touch20_min_aligned_momentum)))),
        min_rule_score=_decimal_override(overrides, "min_rule_score", Decimal(str(_touch_setting(settings, freq, "min_rule_score", settings.crypto_btc15m_touch20_min_rule_score)))),
        bucket_price_band_cents=_bucket_price_band_cents(
            _int_override(overrides, "bucket_price_band_cents", int(_touch_setting(settings, freq, "bucket_price_band_cents", settings.crypto_btc15m_touch20_bucket_price_band_cents)))
        ),
        bucket_spread_band_cents=_bucket_spread_band_cents(
            _int_override(overrides, "bucket_spread_band_cents", int(_touch_setting(settings, freq, "bucket_spread_band_cents", settings.crypto_btc15m_touch20_bucket_spread_band_cents)))
        ),
        bucket_time_band_minutes=_bucket_time_band_minutes(
            _int_override(overrides, "bucket_time_band_minutes", int(_touch_setting(settings, freq, "bucket_time_band_minutes", settings.crypto_btc15m_touch20_bucket_time_band_minutes)))
        ),
        quote_fresh_seconds=_int_override(overrides, "quote_fresh_seconds", int(_touch_setting(settings, freq, "quote_fresh_seconds", settings.crypto_btc15m_touch20_quote_fresh_seconds))),
        spot_fresh_seconds=_int_override(overrides, "spot_fresh_seconds", int(_touch_setting(settings, freq, "spot_fresh_seconds", settings.crypto_btc15m_touch20_spot_fresh_seconds))),
    )


def _artifact_type(base: str, *, frequency: str = BTC15M_TOUCH20_RULES_FREQ, asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET) -> str:
    freq = normalize_frequency(frequency) or BTC15M_TOUCH20_RULES_FREQ
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    return f"{base}:{freq}:{asset}"


def _approval_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    asset = _normalize_asset_symbol(asset_symbol)
    freq = _normalize_touch_frequency(frequency)
    return f"{_strategy_code(asset, frequency=freq)}_approval:{kalshi_env}:{asset}:{freq}"


def _ledger_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    asset = _normalize_asset_symbol(asset_symbol)
    freq = _normalize_touch_frequency(frequency)
    return f"{_strategy_code(asset, frequency=freq)}:{kalshi_env}:{asset}:{freq}"


def _money_text(value: Decimal | None) -> str | None:
    return str(value.quantize(Decimal("0.0001"))) if isinstance(value, Decimal) else None


def _count_text(value: Decimal | None) -> str | None:
    return f"{value:.2f}" if isinstance(value, Decimal) else None


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _datetime_from_any(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _as_utc(parsed)


def _clamp(value: Decimal, low: Decimal = Decimal("0"), high: Decimal = Decimal("1")) -> Decimal:
    return min(high, max(low, value))


def _clamp_price(value: Decimal) -> Decimal:
    return quantize_price(min(Decimal("0.9900"), max(Decimal("0.0100"), value)))


def _ratio(value: float | Decimal | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _scope_supported(frequency: str, asset_symbol: str) -> bool:
    return _normalize_touch_frequency(frequency) in TOUCH20_RULES_SUPPORTED_FREQUENCIES and _normalize_asset_symbol(asset_symbol) in TOUCH20_RULES_SUPPORTED_ASSETS


def _snapshot_decision_time(snapshot: CryptoMarketSnapshotRecord) -> datetime:
    return _as_utc(snapshot.observed_at) or datetime.now(UTC)


def _market_timing(snapshot: CryptoMarketSnapshotRecord, decision_ts: datetime) -> dict[str, int | None]:
    close_ts = _as_utc(snapshot.close_time or snapshot.expected_expiration_time)
    open_ts = _as_utc(snapshot.open_time)
    interval_seconds = _frequency_interval_seconds(snapshot.frequency)
    time_to_close = int((close_ts - decision_ts).total_seconds()) if close_ts is not None else None
    market_age = int((decision_ts - open_ts).total_seconds()) if open_ts is not None else None
    if market_age is None and time_to_close is not None and time_to_close <= interval_seconds:
        market_age = max(0, interval_seconds - time_to_close)
    if time_to_close is None and market_age is not None and market_age <= interval_seconds:
        time_to_close = max(0, interval_seconds - market_age)
    return {
        "market_age_seconds": market_age,
        "time_to_close_seconds": time_to_close,
    }


def _complement_price(raw: Decimal | float | str | None) -> Decimal | None:
    if raw is None:
        return None
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return None
    complement = Decimal("1.0000") - price
    if complement <= Decimal("0") or complement >= Decimal("1"):
        return None
    return quantize_price(complement)


def _side_entry_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    raw = snapshot.yes_ask_dollars if side == "yes" else snapshot.no_ask_dollars
    complement_raw = snapshot.no_bid_dollars if side == "yes" else snapshot.yes_bid_dollars
    if raw is None:
        return _complement_price(complement_raw)
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return _complement_price(complement_raw)
    return quantize_price(price)


def _side_bid_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    raw = snapshot.yes_bid_dollars if side == "yes" else snapshot.no_bid_dollars
    complement_raw = snapshot.no_ask_dollars if side == "yes" else snapshot.yes_ask_dollars
    if raw is None:
        return _complement_price(complement_raw)
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return _complement_price(complement_raw)
    return quantize_price(price)


def _side_mid_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    bid = _side_bid_price(snapshot, side)
    ask = _side_entry_price(snapshot, side)
    if bid is None or ask is None:
        return None
    return quantize_price((bid + ask) / Decimal("2"))


def _target_yes_price_for_entry(side: str, side_price: Decimal) -> Decimal:
    return side_price if side == "yes" else Decimal("1.0000") - side_price


def _sell_yes_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    raw = snapshot.yes_bid_dollars if side == "yes" else snapshot.yes_ask_dollars
    if raw is None:
        return None
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return None
    return quantize_price(price)


def _sell_side_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    if side == "yes":
        return _side_bid_price(snapshot, "yes")
    no_bid = _side_bid_price(snapshot, "no")
    if no_bid is not None:
        return no_bid
    yes_ask = _sell_yes_price(snapshot, "no")
    return quantize_price(Decimal("1.0000") - yes_ask) if yes_ask is not None else None


def _terminal_price(raw: Decimal | float | str | None) -> Decimal | None:
    if raw is None:
        return None
    price = _decimal(raw)
    if price < Decimal("0") or price > Decimal("1"):
        return None
    return quantize_price(price)


def _terminal_side_exit_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    result = str(snapshot.settlement_result or "").strip().lower()
    if result in {"yes", "no"}:
        return _settlement_side_payout(snapshot, side)
    if side == "yes":
        direct = _terminal_price(snapshot.yes_bid_dollars)
        if direct is not None:
            return direct
        no_ask = _terminal_price(snapshot.no_ask_dollars)
        return quantize_price(Decimal("1.0000") - no_ask) if no_ask is not None else None
    direct = _terminal_price(snapshot.no_bid_dollars)
    if direct is not None:
        return direct
    yes_ask = _terminal_price(snapshot.yes_ask_dollars)
    return quantize_price(Decimal("1.0000") - yes_ask) if yes_ask is not None else None


def _side_spread(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    bid = _side_bid_price(snapshot, side)
    ask = _side_entry_price(snapshot, side)
    if bid is None or ask is None:
        return None
    spread = ask - bid
    if spread < Decimal("0"):
        return None
    return spread.quantize(Decimal("0.0001"))


def _side_has_raw_bid_ask(snapshot: CryptoMarketSnapshotRecord, side: str) -> bool:
    bid = snapshot.yes_bid_dollars if side == "yes" else snapshot.no_bid_dollars
    ask = snapshot.yes_ask_dollars if side == "yes" else snapshot.no_ask_dollars
    return bid is not None and ask is not None


def _side_has_quote_source(snapshot: CryptoMarketSnapshotRecord, side: str) -> bool:
    opposite = "no" if side == "yes" else "yes"
    return _side_has_raw_bid_ask(snapshot, side) or _side_has_raw_bid_ask(snapshot, opposite)


def _has_allowed_side_quote_source(snapshot: CryptoMarketSnapshotRecord, cfg: Touch20AssetSettings) -> bool:
    return any(_side_has_quote_source(snapshot, side) for side in cfg.allowed_sides)


def _max_spread_for_price(entry_price: Decimal) -> Decimal:
    return Decimal("0.0100") if entry_price < Decimal("0.2000") else Decimal("0.0200")


def _configured_max_spread(entry_price: Decimal, cfg: Touch20AssetSettings) -> Decimal:
    if cfg.max_spread_dollars > Decimal("0"):
        return cfg.max_spread_dollars.quantize(Decimal("0.0001"))
    return _max_spread_for_price(entry_price)


def _price_band(price: Decimal, *, width_cents: int = 10) -> str:
    width = _bucket_price_band_cents(int(width_cents or 10))
    cents = int((price * Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    if cents < 10:
        return "under_10c"
    if cents >= 90:
        return "90c_plus"
    low = (cents // width) * width
    high = min(low + width, 90)
    return f"{low}_{high}c"


def _spread_band(spread: Decimal | None, *, width_cents: int = 1) -> str:
    if spread is None:
        return "unknown_spread"
    if _bucket_spread_band_cents(int(width_cents or 1)) >= 2:
        return "le_2c" if spread <= Decimal("0.0200") else "gt_2c"
    if spread <= Decimal("0.0100"):
        return "le_1c"
    if spread <= Decimal("0.0200"):
        return "le_2c"
    return "gt_2c"


def _time_bucket(
    time_to_close_seconds: int | None,
    *,
    width_minutes: int = 5,
    interval_seconds: int = BTC15M_TOUCH20_RULES_INTERVAL_SECONDS,
) -> str:
    if time_to_close_seconds is None:
        return "unknown_time"
    width = max(1, _bucket_time_band_minutes(int(width_minutes or 5)))
    if interval_seconds == BTC15M_TOUCH20_RULES_INTERVAL_SECONDS and width >= 10:
        if time_to_close_seconds < 300:
            return "0_5m"
        if time_to_close_seconds <= BTC15M_TOUCH20_RULES_INTERVAL_SECONDS:
            return "5_15m"
        return "15m_plus"
    interval_minutes = max(width, int(math.ceil(max(1, interval_seconds) / 60)))
    if time_to_close_seconds > interval_seconds:
        return f"{interval_minutes}m_plus"
    width_seconds = width * 60
    max_idx = max(0, (interval_seconds - 1) // width_seconds)
    idx = min(max(0, time_to_close_seconds) // width_seconds, max_idx)
    low = idx * width
    high = min(low + width, interval_minutes)
    return f"{low}_{high}m"


def _bucket_key(
    *,
    asset_symbol: str,
    side: str,
    entry_price: Decimal,
    spread: Decimal | None,
    time_to_close_seconds: int | None,
    price_band_cents: int = 10,
    spread_band_cents: int = 1,
    time_band_minutes: int = 5,
    interval_seconds: int = BTC15M_TOUCH20_RULES_INTERVAL_SECONDS,
) -> str:
    return "|".join(
        [
            _normalize_asset_symbol(asset_symbol),
            side,
            _price_band(entry_price, width_cents=price_band_cents),
            _spread_band(spread, width_cents=spread_band_cents),
            _time_bucket(time_to_close_seconds, width_minutes=time_band_minutes, interval_seconds=interval_seconds),
        ]
    )


@lru_cache(maxsize=512)
def _target_exit_price_for_net_profit(entry_price: Decimal, *, target_pct: Decimal, fee_rate: Decimal) -> Decimal | None:
    count = Decimal("1.00")
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_price, count=count, fee_rate=fee_rate)
    denominator = (entry_price * count) + entry_fee
    if denominator <= Decimal("0"):
        return None

    def net_pct(exit_price: Decimal) -> Decimal:
        exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_price, count=count, fee_rate=fee_rate)
        pnl = ((exit_price - entry_price) * count) - entry_fee - exit_fee
        return pnl / denominator

    high = Decimal("0.9999")
    if net_pct(high) < target_pct:
        return None
    low = entry_price
    for _ in range(36):
        mid = (low + high) / Decimal("2")
        if net_pct(mid) >= target_pct:
            high = mid
        else:
            low = mid
    return quantize_price(high)


def _realized_pnl(
    *,
    entry_side_price: Decimal,
    exit_side_price: Decimal,
    count_fp: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_side_price, count=count_fp, fee_rate=fee_rate)
    return ((exit_side_price - entry_side_price) * count_fp - entry_fee - exit_fee).quantize(Decimal("0.0001"))


def _realized_pnl_without_exit_fee(
    *,
    entry_side_price: Decimal,
    exit_side_price: Decimal,
    count_fp: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    return ((exit_side_price - entry_side_price) * count_fp - entry_fee).quantize(Decimal("0.0001"))


def net_profit_pct(
    *,
    entry_side_price: Decimal,
    exit_side_price: Decimal,
    count_fp: Decimal,
    fee_rate: Decimal,
) -> Decimal | None:
    if count_fp <= Decimal("0") or entry_side_price <= Decimal("0"):
        return None
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    denominator = entry_side_price * count_fp + entry_fee
    if denominator <= Decimal("0"):
        return None
    return (_realized_pnl(
        entry_side_price=entry_side_price,
        exit_side_price=exit_side_price,
        count_fp=count_fp,
        fee_rate=fee_rate,
    ) / denominator).quantize(Decimal("0.0001"))


def _net_profit_pct_from_realized(
    *,
    realized_pnl: Decimal,
    entry_side_price: Decimal,
    count_fp: Decimal,
    fee_rate: Decimal,
) -> Decimal | None:
    if count_fp <= Decimal("0") or entry_side_price <= Decimal("0"):
        return None
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    denominator = entry_side_price * count_fp + entry_fee
    if denominator <= Decimal("0"):
        return None
    return (realized_pnl / denominator).quantize(Decimal("0.0001"))


def _non_proxy_spot(row: CryptoSpotOHLCRecord) -> bool:
    source_kind = str(row.source_kind or "").strip().lower()
    provider = str(row.provider or "").strip().lower()
    return source_kind not in {"spot_price_proxy", "proxy"} and provider not in {"coingecko"}


def _spot_time(row: CryptoSpotOHLCRecord) -> datetime | None:
    return _as_utc(row.observed_at or row.end_ts)


def _prepare_spot_index(
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
) -> dict[str, list[Any]]:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    entries: list[tuple[datetime, datetime, Decimal, CryptoSpotOHLCRecord]] = []
    for row in spot_rows:
        if (
            _normalize_asset_symbol(row.asset_symbol) != asset
            or not _non_proxy_spot(row)
            or row.close_dollars is None
        ):
            continue
        eligibility_ts = _as_utc(row.end_ts) or _spot_time(row)
        observed_ts = _spot_time(row) or _as_utc(row.end_ts)
        if eligibility_ts is None or observed_ts is None:
            continue
        entries.append((eligibility_ts, observed_ts, _decimal(row.close_dollars), row))
    entries.sort(key=lambda item: item[0])
    return {
        "eligibility_times": [item[0] for item in entries],
        "observed_times": [item[1] for item in entries],
        "closes": [item[2] for item in entries],
        "rows": [item[3] for item in entries],
    }


def _spot_features_from_index(
    spot_index: dict[str, list[Any]],
    *,
    decision_ts: datetime,
    freshness_reference: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    eligibility_times = spot_index.get("eligibility_times") or []
    if not eligibility_times:
        return {
            "available": False,
            "reason": "spot_data_missing_or_proxy_only",
            "return_1": Decimal("0"),
            "return_3": Decimal("0"),
            "volatility": Decimal("0"),
        }
    idx = bisect_right(eligibility_times, decision_ts) - 1
    if idx < 0:
        return {
            "available": False,
            "reason": "spot_data_missing_or_proxy_only",
            "return_1": Decimal("0"),
            "return_3": Decimal("0"),
            "volatility": Decimal("0"),
        }
    observed_times = spot_index["observed_times"]
    closes = spot_index["closes"]
    rows = spot_index["rows"]
    latest = rows[idx]
    latest_time = observed_times[idx]
    age_seconds = int((freshness_reference - latest_time).total_seconds())
    if age_seconds < 0 or age_seconds > max_age_seconds:
        return {
            "available": False,
            "reason": "spot_data_stale",
            "age_seconds": age_seconds,
            "latest_observed_at": latest_time.isoformat(),
            "return_1": Decimal("0"),
            "return_3": Decimal("0"),
            "volatility": Decimal("0"),
        }
    latest_close = closes[idx]

    def calc_return(back: int) -> Decimal:
        prior_idx = idx - back
        if prior_idx < 0 or closes[prior_idx] <= Decimal("0"):
            return Decimal("0")
        return ((latest_close / closes[prior_idx]) - Decimal("1")).quantize(Decimal("0.0001"))

    window = closes[max(0, idx - 8) : idx + 1]
    step_returns: list[Decimal] = []
    for left, right in zip(window[:-1], window[1:]):
        if left > Decimal("0"):
            step_returns.append((right / left) - Decimal("1"))
    if len(step_returns) >= 2:
        mean = sum(step_returns, Decimal("0")) / Decimal(len(step_returns))
        variance = sum((item - mean) * (item - mean) for item in step_returns) / Decimal(len(step_returns))
        volatility = Decimal(str(math.sqrt(float(variance)))).quantize(Decimal("0.0001"))
    elif step_returns:
        volatility = abs(step_returns[-1]).quantize(Decimal("0.0001"))
    else:
        volatility = Decimal("0")
    return {
        "available": True,
        "reason": "available",
        "age_seconds": age_seconds,
        "latest_observed_at": latest_time.isoformat(),
        "provider": latest.provider,
        "source_kind": latest.source_kind,
        "close_dollars": str(latest_close),
        "return_1": calc_return(1),
        "return_3": calc_return(3),
        "volatility": volatility,
    }


def _spot_features(
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    decision_ts: datetime,
    freshness_reference: datetime,
    max_age_seconds: int,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
) -> dict[str, Any]:
    return _spot_features_from_index(
        _prepare_spot_index(spot_rows, asset_symbol=asset_symbol),
        decision_ts=decision_ts,
        freshness_reference=freshness_reference,
        max_age_seconds=max_age_seconds,
    )


def _bucket_map(gate_metrics: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for item in (gate_metrics or {}).get("bucket_matrix") or []:
        if isinstance(item, dict) and item.get("bucket_key"):
            mapped[str(item["bucket_key"])] = item
    return mapped


def _allowed_bucket_keys(gate_metrics: dict[str, Any] | None) -> set[str]:
    return {str(key) for key in ((gate_metrics or {}).get("allowed_bucket_keys") or [])}


def _bucket_pnl_per_candidate(bucket: dict[str, Any]) -> Decimal:
    sample_count = max(1, int(bucket.get("sample_count") or 1))
    return _decimal(bucket.get("net_pnl") or "0") / Decimal(sample_count)


def _rule_score(
    *,
    side: str,
    entry_price: Decimal,
    current_mid: Decimal,
    target_exit_price: Decimal,
    spread: Decimal,
    max_spread: Decimal,
    time_to_close_seconds: int,
    spot: dict[str, Any],
    bucket: dict[str, Any],
) -> tuple[Decimal, dict[str, str]]:
    side_multiplier = Decimal("1") if side == "yes" else Decimal("-1")
    replay_touch = _clamp(_decimal(bucket.get("touch_rate") or "0"))
    replay_pnl = _clamp(_bucket_pnl_per_candidate(bucket) / Decimal("0.0500"))
    aligned_momentum = side_multiplier * ((_decimal(spot.get("return_1")) * Decimal("0.65")) + (_decimal(spot.get("return_3")) * Decimal("0.35")))
    momentum = _clamp((aligned_momentum + Decimal("0.0050")) / Decimal("0.0150"))
    volatility = _clamp(_decimal(spot.get("volatility")) / Decimal("0.0100"))
    time_score = _clamp((Decimal(time_to_close_seconds) - Decimal("300")) / Decimal("600"))
    target_gap = max(Decimal("0"), target_exit_price - current_mid)
    target_gap_score = _clamp(Decimal("1") - (target_gap / Decimal("0.2000")))
    spread_score = _clamp(Decimal("1") - (spread / max_spread)) if max_spread > Decimal("0") else Decimal("0")
    score = (
        replay_touch * Decimal("0.30")
        + replay_pnl * Decimal("0.20")
        + momentum * Decimal("0.15")
        + volatility * Decimal("0.10")
        + time_score * Decimal("0.10")
        + target_gap_score * Decimal("0.10")
        + spread_score * Decimal("0.05")
    ).quantize(Decimal("0.0001"))
    return score, {
        "replay_touch": str(replay_touch.quantize(Decimal("0.0001"))),
        "replay_pnl": str(replay_pnl.quantize(Decimal("0.0001"))),
        "momentum": str(momentum.quantize(Decimal("0.0001"))),
        "volatility": str(volatility.quantize(Decimal("0.0001"))),
        "time": str(time_score.quantize(Decimal("0.0001"))),
        "target_gap": str(target_gap_score.quantize(Decimal("0.0001"))),
        "spread": str(spread_score.quantize(Decimal("0.0001"))),
        "aligned_momentum": str(aligned_momentum.quantize(Decimal("0.0001"))),
    }


def rules_candidates_for_snapshot(
    snapshot: CryptoMarketSnapshotRecord,
    *,
    settings: Settings,
    spot: dict[str, Any],
    gate_metrics: dict[str, Any] | None,
    require_allowed_bucket: bool = True,
) -> list[dict[str, Any]]:
    decision_ts = _snapshot_decision_time(snapshot)
    timing = _market_timing(snapshot, decision_ts)
    market_age = timing["market_age_seconds"]
    time_to_close = timing["time_to_close_seconds"]
    allowed_keys = _allowed_bucket_keys(gate_metrics)
    buckets = _bucket_map(gate_metrics)
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    freq = _normalize_touch_frequency(snapshot.frequency)
    interval_seconds = _frequency_interval_seconds(freq)
    cfg = _asset_settings(settings, snapshot.asset_symbol, frequency=freq)
    target_pct = cfg.take_profit_pct
    min_price = cfg.min_contract_price_dollars
    max_price = cfg.max_contract_price_dollars
    min_score = cfg.min_rule_score
    candidates: list[dict[str, Any]] = []
    for side in ("yes", "no"):
        entry = _side_entry_price(snapshot, side)
        bid = _side_bid_price(snapshot, side)
        mid = _side_mid_price(snapshot, side)
        spread = _side_spread(snapshot, side)
        max_spread = _configured_max_spread(entry, cfg) if entry is not None else Decimal("0")
        target_exit = _target_exit_price_for_net_profit(entry, target_pct=target_pct, fee_rate=fee_rate) if entry is not None else None
        reason = "ok"
        status = "blocked"
        score: Decimal | None = None
        score_components: dict[str, str] = {}
        bucket_key = None
        bucket: dict[str, Any] = {}
        if side not in cfg.allowed_sides:
            reason = "side_not_allowed"
        elif snapshot.status and snapshot.status not in {"open", "active"}:
            reason = "market_not_open"
        elif not spot.get("available"):
            reason = str(spot.get("reason") or "spot_data_missing_or_stale")
        elif entry is None or bid is None or mid is None or spread is None:
            reason = "non_executable_bid_ask" if _side_has_quote_source(snapshot, side) else "missing_real_bid_ask"
        elif market_age is None or time_to_close is None:
            reason = "entry_window_unknown"
        elif market_age < cfg.min_market_age_seconds:
            reason = "market_too_early"
        elif time_to_close < cfg.min_seconds_to_close:
            reason = "market_too_late"
        elif entry < min_price:
            reason = "entry_price_below_min"
        elif max_price > Decimal("0") and entry >= max_price:
            reason = "entry_price_above_max"
        elif target_exit is None:
            reason = "target_profit_impossible_after_fees"
        elif target_exit >= Decimal("1.0000"):
            reason = "target_exit_above_max_payout"
        elif spread > max_spread:
            reason = "spread_above_tier_max"
        else:
            bucket_key = _bucket_key(
                asset_symbol=snapshot.asset_symbol,
                side=side,
                entry_price=entry,
                spread=spread,
                time_to_close_seconds=time_to_close,
                price_band_cents=cfg.bucket_price_band_cents,
                spread_band_cents=cfg.bucket_spread_band_cents,
                time_band_minutes=cfg.bucket_time_band_minutes,
                interval_seconds=interval_seconds,
            )
            bucket = buckets.get(bucket_key, {})
            if require_allowed_bucket and bucket_key not in allowed_keys:
                reason = "replay_bucket_not_allowed"
            else:
                score, score_components = _rule_score(
                    side=side,
                    entry_price=entry,
                    current_mid=mid,
                    target_exit_price=target_exit,
                    spread=spread,
                    max_spread=max_spread,
                    time_to_close_seconds=time_to_close,
                    spot=spot,
                    bucket=bucket
                    or (
                        {
                            "sample_count": 1,
                            "touch_rate": max(float(cfg.replay_min_touch_rate), 0.25),
                            "net_pnl": "0.0500",
                        }
                        if not require_allowed_bucket
                        else {}
                    ),
                )
                aligned_momentum = _decimal(score_components.get("aligned_momentum"))
                if aligned_momentum < cfg.min_aligned_momentum:
                    reason = "side_aligned_momentum_below_min"
                elif score < min_score:
                    reason = "rule_score_below_min"
                else:
                    status = "eligible"
        candidates.append(
            {
                "side": side,
                "status": status,
                "candidate_status": "live_quality" if status == "eligible" else "blocked",
                "reason": "touch_20pct_before_close_target" if status == "eligible" else reason,
                "objective": "touch_20pct_before_close",
                "execution_price_dollars": _money_text(entry),
                "bid_price_dollars": _money_text(bid),
                "target_yes_price_dollars": _money_text(_target_yes_price_for_entry(side, entry)) if entry is not None else None,
                "target_exit_side_price_dollars": _money_text(target_exit),
                "target_exit_yes_price_dollars": _money_text(_target_yes_price_for_entry(side, target_exit)) if target_exit is not None else None,
                "spread_dollars": _money_text(spread),
                "max_spread_dollars": _money_text(max_spread) if max_spread > Decimal("0") else None,
                "market_age_seconds": market_age,
                "time_to_close_seconds": time_to_close,
                "allowed_sides": list(cfg.allowed_sides),
                "bucket_key": bucket_key,
                "bucket": bucket,
                "rule_score": str(score) if score is not None else None,
                "score_components": score_components,
                "spot_features": {
                    key: (str(value) if isinstance(value, Decimal) else value)
                    for key, value in spot.items()
                    if key not in {"available"}
                },
                "uses_trained_model": False,
            }
        )
    candidates.sort(
        key=lambda item: (
            item.get("candidate_status") == "live_quality",
            _bucket_pnl_per_candidate(item.get("bucket") or {}),
            _decimal((item.get("bucket") or {}).get("touch_rate") or "0"),
            _decimal(item.get("rule_score") or "-1"),
            -_decimal(item.get("spread_dollars") or "999"),
            int(item.get("time_to_close_seconds") or 0),
        ),
        reverse=True,
    )
    for idx, candidate in enumerate(candidates, start=1):
        candidate["rank"] = idx
    return candidates


def _first_touch(
    future_rows: list[CryptoMarketSnapshotRecord],
    *,
    side: str,
    target_exit_side_price: Decimal,
) -> tuple[CryptoMarketSnapshotRecord, Decimal] | None:
    for row in future_rows:
        price = _side_bid_price(row, side)
        if price is not None and price >= target_exit_side_price:
            return row, price
    return None


def _settlement_side_payout(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal:
    result = str(snapshot.settlement_result or "").strip().lower()
    if result == "yes":
        return Decimal("1.0000") if side == "yes" else Decimal("0")
    if result == "no":
        return Decimal("1.0000") if side == "no" else Decimal("0")
    return Decimal("0")


def _simulate_replay_trade(
    row: CryptoMarketSnapshotRecord,
    future_rows: list[CryptoMarketSnapshotRecord],
    candidate: dict[str, Any],
    *,
    settings: Settings,
    spot_index: dict[str, list[Any]] | None = None,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(row.asset_symbol)
    freq = _normalize_touch_frequency(row.frequency)
    interval_seconds = _frequency_interval_seconds(freq)
    cfg = _asset_settings(settings, asset, frequency=freq)
    side = str(candidate["side"])
    entry = _decimal(candidate["execution_price_dollars"])
    target_exit = _decimal(candidate["target_exit_side_price_dollars"])
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry, count=Decimal("1.00"), fee_rate=fee_rate)
    exit_price: Decimal | None = None
    exit_reason = "terminal_close"
    exit_observed_at = _as_utc(row.close_time or row.expected_expiration_time) or _snapshot_decision_time(row)
    exit_fee = Decimal("0")
    max_drawdown_pct: Decimal | None = None
    min_exit_side_price: Decimal | None = None
    protection_armed = False
    previous_profit_pct: Decimal | None = None

    for future in future_rows:
        observed_at = _snapshot_decision_time(future)
        side_price = _sell_side_price(future, side)
        if side_price is None:
            continue
        profit_pct = net_profit_pct(
            entry_side_price=entry,
            exit_side_price=side_price,
            count_fp=Decimal("1.00"),
            fee_rate=fee_rate,
        )
        if profit_pct is None:
            continue
        max_drawdown_pct = profit_pct if max_drawdown_pct is None else min(max_drawdown_pct, profit_pct)
        min_exit_side_price = side_price if min_exit_side_price is None else min(min_exit_side_price, side_price)
        if profit_pct >= cfg.take_profit_pct:
            exit_price = side_price
            exit_reason = "take_profit"
            exit_observed_at = observed_at
            exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_price, count=Decimal("1.00"), fee_rate=fee_rate)
            break
        if cfg.stop_loss_pct > Decimal("0") and profit_pct <= -cfg.stop_loss_pct:
            exit_price = side_price
            exit_reason = "stop_loss"
            exit_observed_at = observed_at
            exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_price, count=Decimal("1.00"), fee_rate=fee_rate)
            break
        protection_armed = protection_armed or profit_pct >= cfg.profit_protection_threshold_pct
        protection_trigger: str | None = None
        if protection_armed and profit_pct <= cfg.profit_protection_floor_pct:
            protection_trigger = "profit_protection_floor"
        elif protection_armed and previous_profit_pct is not None and profit_pct < previous_profit_pct and spot_index:
            spot = _spot_features_from_index(
                spot_index,
                decision_ts=observed_at,
                freshness_reference=observed_at,
                max_age_seconds=max(cfg.spot_fresh_seconds, interval_seconds),
            )
            side_multiplier = Decimal("1") if side == "yes" else Decimal("-1")
            adverse_spot = (
                spot.get("available")
                and (side_multiplier * _decimal(spot.get("return_1"))) < Decimal("0")
                and (side_multiplier * _decimal(spot.get("return_3"))) < Decimal("0")
            )
            if adverse_spot:
                protection_trigger = "profit_protection_adverse_momentum"
        previous_profit_pct = profit_pct
        if protection_trigger:
            exit_price = side_price
            exit_reason = protection_trigger
            exit_observed_at = observed_at
            exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_price, count=Decimal("1.00"), fee_rate=fee_rate)
            break

    terminal = exit_price is None
    if terminal:
        exit_price = _settlement_side_payout(row, side)
        realized = _realized_pnl_without_exit_fee(
            entry_side_price=entry,
            exit_side_price=exit_price,
            count_fp=Decimal("1.00"),
            fee_rate=fee_rate,
        )
        terminal_profit_pct = _net_profit_pct_from_realized(
            realized_pnl=realized,
            entry_side_price=entry,
            count_fp=Decimal("1.00"),
            fee_rate=fee_rate,
        )
        if terminal_profit_pct is not None:
            max_drawdown_pct = terminal_profit_pct if max_drawdown_pct is None else min(max_drawdown_pct, terminal_profit_pct)
        min_exit_side_price = exit_price if min_exit_side_price is None else min(min_exit_side_price, exit_price)
    else:
        gross = exit_price - entry
        realized = (gross - entry_fee - exit_fee).quantize(Decimal("0.0001"))

    gross = exit_price - entry
    net = realized
    return {
        "side": side,
        "entry_price_dollars": _money_text(entry),
        "target_exit_side_price_dollars": _money_text(target_exit),
        "exit_price_dollars": _money_text(exit_price),
        "exit_reason": exit_reason,
        "exit_observed_at": exit_observed_at.isoformat(),
        "touched": exit_reason == "take_profit",
        "stopped": exit_reason == "stop_loss",
        "terminal_closed": terminal,
        "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
        "fees": str((entry_fee + exit_fee).quantize(Decimal("0.0001"))),
        "net_pnl": str(net.quantize(Decimal("0.0001"))),
        "max_drawdown_pct": str((max_drawdown_pct or Decimal("0")).quantize(Decimal("0.0001"))),
        "min_exit_side_price_dollars": _money_text(min_exit_side_price),
        "bucket_key": candidate.get("bucket_key"),
        "rule_score": candidate.get("rule_score"),
        "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
    }


def _bucket_matrix(
    trades: list[dict[str, Any]],
    *,
    settings: Settings,
    asset_symbol: str,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        simulation = trade.get("simulation") if isinstance(trade.get("simulation"), dict) else {}
        key = str(simulation.get("bucket_key") or "")
        if key:
            grouped[key].append(trade)
    matrix: list[dict[str, Any]] = []
    asset = _normalize_asset_symbol(asset_symbol)
    cfg = _asset_settings(settings, asset, frequency=frequency)
    min_touch_rate = cfg.replay_min_touch_rate
    min_pnl_per = cfg.replay_min_pnl_per_candidate_dollars
    max_stop_rate = cfg.max_replay_stop_loss_rate
    max_terminal_rate = cfg.max_replay_terminal_loss_rate
    for key, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        touch_count = sum(1 for row in rows if (row.get("simulation") or {}).get("touched") is True)
        stop_loss_count = sum(1 for row in rows if (row.get("simulation") or {}).get("exit_reason") == "stop_loss")
        terminal_loss_count = sum(
            1
            for row in rows
            if (row.get("simulation") or {}).get("terminal_closed") is True
            and _decimal((row.get("simulation") or {}).get("net_pnl")) < Decimal("0")
        )
        net = sum(values, Decimal("0"))
        sample_count = len(rows)
        touch_rate = Decimal(touch_count) / Decimal(sample_count) if sample_count else Decimal("0")
        stop_loss_rate = Decimal(stop_loss_count) / Decimal(sample_count) if sample_count else Decimal("0")
        terminal_loss_rate = Decimal(terminal_loss_count) / Decimal(sample_count) if sample_count else Decimal("0")
        pnl_per = net / Decimal(sample_count) if sample_count else Decimal("0")
        allowed = (
            sample_count >= 5
            and net > Decimal("0")
            and touch_rate >= min_touch_rate
            and pnl_per >= min_pnl_per
            and stop_loss_rate <= max_stop_rate
            and terminal_loss_rate <= max_terminal_rate
        )
        first = rows[0]
        candidate = first.get("candidate") if isinstance(first.get("candidate"), dict) else {}
        matrix.append(
            {
                "bucket_key": key,
                "asset_symbol": asset,
                "side": candidate.get("side"),
                "sample_count": sample_count,
                "touch_count": touch_count,
                "stop_loss_count": stop_loss_count,
                "terminal_loss_count": terminal_loss_count,
                "touch_rate": float(touch_rate),
                "stop_loss_rate": float(stop_loss_rate),
                "terminal_loss_rate": float(terminal_loss_rate),
                "net_pnl": str(net.quantize(Decimal("0.0001"))),
                "pnl_per_candidate": str(pnl_per.quantize(Decimal("0.0001"))),
                "allowed": allowed,
            }
        )
    matrix.sort(key=lambda item: (_decimal(item.get("pnl_per_candidate") or "0"), float(item.get("touch_rate") or 0.0)), reverse=True)
    return matrix


def _gate_requirements(
    settings: Settings,
    *,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> dict[str, Any]:
    freq = _normalize_touch_frequency(frequency)
    cfg = _asset_settings(settings, asset_symbol, frequency=freq)
    return {
        "asset_symbol": _normalize_asset_symbol(asset_symbol),
        "frequency": freq,
        "entry_replay_mode": "first_eligible_per_market",
        "allowed_sides": list(cfg.allowed_sides),
        "min_seconds_to_close": cfg.min_seconds_to_close,
        "min_contract_price_dollars": float(cfg.min_contract_price_dollars),
        "max_contract_price_dollars": float(cfg.max_contract_price_dollars),
        "max_spread_dollars": float(cfg.max_spread_dollars),
        "min_aligned_momentum": float(cfg.min_aligned_momentum),
        "min_rule_score": float(cfg.min_rule_score),
        "bucket_price_band_cents": cfg.bucket_price_band_cents,
        "bucket_spread_band_cents": cfg.bucket_spread_band_cents,
        "bucket_time_band_minutes": cfg.bucket_time_band_minutes,
        "min_trade_candidates": cfg.replay_min_candidates,
        "min_net_pl_dollars": float(cfg.replay_min_net_pnl_dollars),
        "min_pnl_per_candidate_dollars": float(cfg.replay_min_pnl_per_candidate_dollars),
        "max_hard_cap_breaches": cfg.replay_max_hard_cap_breaches,
        "min_touch_rate": float(cfg.replay_min_touch_rate),
        "max_stop_loss_rate": float(cfg.max_replay_stop_loss_rate),
        "max_terminal_loss_rate": float(cfg.max_replay_terminal_loss_rate),
        "gate_candidate_scope": "allowed_replay_buckets",
        "requires_allowed_bucket_support": True,
        "requires_real_quote_path_evidence": True,
        "uses_trained_model": False,
        "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
    }


def _allowed_bucket_candidate_count(metrics: dict[str, Any], *, exclude_bucket_keys: Iterable[str] = ()) -> int:
    excluded = {str(key) for key in exclude_bucket_keys}
    allowed_keys = {str(key) for key in (metrics.get("allowed_bucket_keys") or [])}
    count = 0
    for bucket in metrics.get("bucket_matrix") or []:
        if not isinstance(bucket, dict):
            continue
        key = str(bucket.get("bucket_key") or "")
        if bucket.get("allowed") is not True and key not in allowed_keys:
            continue
        if key in excluded:
            continue
        count += int(bucket.get("sample_count") or 0)
    return count


def _allowed_bucket_metric_summary(metrics: dict[str, Any]) -> dict[str, Decimal | int]:
    allowed_keys = {str(key) for key in (metrics.get("allowed_bucket_keys") or [])}
    count = 0
    net = Decimal("0")
    touch_weight = Decimal("0")
    stop_weight = Decimal("0")
    terminal_weight = Decimal("0")
    for bucket in metrics.get("bucket_matrix") or []:
        if not isinstance(bucket, dict):
            continue
        key = str(bucket.get("bucket_key") or "")
        if bucket.get("allowed") is not True and key not in allowed_keys:
            continue
        sample_count = int(bucket.get("sample_count") or 0)
        if sample_count <= 0:
            continue
        count += sample_count
        net += _decimal(bucket.get("net_pnl") or "0")
        if bucket.get("touch_count") is not None:
            touch_weight += Decimal(int(bucket.get("touch_count") or 0))
        else:
            touch_weight += Decimal(str(bucket.get("touch_rate") or "0")) * Decimal(sample_count)
        if bucket.get("stop_loss_count") is not None:
            stop_weight += Decimal(int(bucket.get("stop_loss_count") or 0))
        else:
            stop_weight += Decimal(str(bucket.get("stop_loss_rate") or "0")) * Decimal(sample_count)
        if bucket.get("terminal_loss_count") is not None:
            terminal_weight += Decimal(int(bucket.get("terminal_loss_count") or 0))
        else:
            terminal_weight += Decimal(str(bucket.get("terminal_loss_rate") or "0")) * Decimal(sample_count)
    denominator = Decimal(count) if count else Decimal("0")
    return {
        "count": count,
        "net": net,
        "pnl_per_candidate": net / denominator if denominator else Decimal("0"),
        "touch_rate": touch_weight / denominator if denominator else Decimal("0"),
        "stop_loss_rate": stop_weight / denominator if denominator else Decimal("0"),
        "terminal_loss_rate": terminal_weight / denominator if denominator else Decimal("0"),
    }


def _scoped_decimal_metric(
    metrics: dict[str, Any],
    *,
    scoped_key: str,
    total_key: str,
    summary_key: str | None = None,
    allowed_summary: dict[str, Decimal | int] | None = None,
    use_allowed_scope: bool,
) -> Decimal:
    if use_allowed_scope:
        if metrics.get(scoped_key) is not None:
            return Decimal(str(metrics.get(scoped_key) or "0"))
        if summary_key and allowed_summary is not None:
            return Decimal(str(allowed_summary.get(summary_key) or "0"))
    return Decimal(str(metrics.get(total_key) or "0"))


def _replay_trade_metric_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_decimal((trade.get("simulation") or {}).get("net_pnl")) for trade in trades]
    fees = [_decimal((trade.get("simulation") or {}).get("fees")) for trade in trades]
    trade_count = len(trades)
    touch_count = sum(1 for trade in trades if (trade.get("simulation") or {}).get("touched") is True)
    stop_loss_count = sum(1 for trade in trades if (trade.get("simulation") or {}).get("exit_reason") == "stop_loss")
    terminal_loss_count = sum(
        1
        for trade in trades
        if (trade.get("simulation") or {}).get("terminal_closed") is True
        and _decimal((trade.get("simulation") or {}).get("net_pnl")) < Decimal("0")
    )
    exit_reason_counts = Counter(str((trade.get("simulation") or {}).get("exit_reason") or "unknown") for trade in trades)
    net = sum(values, Decimal("0"))
    return {
        "trade_candidate_count": trade_count,
        "touch_count": touch_count,
        "touch_rate": float(Decimal(touch_count) / Decimal(trade_count)) if trade_count else 0.0,
        "stop_loss_count": stop_loss_count,
        "stop_loss_rate": float(Decimal(stop_loss_count) / Decimal(trade_count)) if trade_count else 0.0,
        "terminal_loss_count": terminal_loss_count,
        "terminal_loss_rate": float(Decimal(terminal_loss_count) / Decimal(trade_count)) if trade_count else 0.0,
        "settlement_hold_count": int(exit_reason_counts.get("terminal_close", 0)),
        "exit_reason_counts": dict(exit_reason_counts),
        "gross_simulated_pl_dollars": float(
            sum((_decimal((trade.get("simulation") or {}).get("gross_pnl")) for trade in trades), Decimal("0"))
        ),
        "net_simulated_pl_dollars": float(net),
        "pnl_per_candidate_dollars": float(net / Decimal(trade_count)) if trade_count else 0.0,
        "fees_dollars": float(sum(fees, Decimal("0"))),
        "max_trade_drawdown_pct": float(
            min((_decimal((trade.get("simulation") or {}).get("max_drawdown_pct")) for trade in trades), default=Decimal("0"))
        ),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
    }


def _replay_input_diagnostics(
    rows: list[CryptoMarketSnapshotRecord],
    *,
    settings: Settings,
    asset_symbol: str,
    frequency: str,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    cfg = _asset_settings(settings, asset, frequency=freq)
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_window_rows = 0
    entry_window_markets: set[str] = set()
    side_counts: dict[str, Counter[str]] = {"yes": Counter(), "no": Counter()}
    side_funnels: dict[str, Counter[str]] = {"yes": Counter(), "no": Counter()}
    side_market_funnels: dict[str, dict[str, set[str]]] = {
        "yes": defaultdict(set),
        "no": defaultdict(set),
    }
    for row in rows:
        market_key = row.market_ticker
        decision_ts = _snapshot_decision_time(row)
        timing = _market_timing(row, decision_ts)
        market_age = timing["market_age_seconds"]
        time_to_close = timing["time_to_close_seconds"]
        in_entry_window = (
            market_age is not None
            and time_to_close is not None
            and market_age >= cfg.min_market_age_seconds
            and time_to_close >= cfg.min_seconds_to_close
            and (not row.status or row.status in {"open", "active"})
        )
        if in_entry_window:
            entry_window_rows += 1
            entry_window_markets.add(row.market_ticker)
        for side in ("yes", "no"):
            counts = side_counts[side]
            funnel = side_funnels[side]
            market_funnel = side_market_funnels[side]
            funnel["total_rows"] += 1
            market_funnel["total_markets"].add(market_key)
            side_allowed = side in cfg.allowed_sides
            if side in cfg.allowed_sides:
                counts["allowed_side_rows"] += 1
                funnel["allowed_side_rows"] += 1
                market_funnel["allowed_side_markets"].add(market_key)
            if not in_entry_window:
                continue
            counts["entry_window_rows"] += 1
            if side_allowed:
                funnel["entry_window_rows"] += 1
                market_funnel["entry_window_markets"].add(market_key)
            if _side_has_raw_bid_ask(row, side):
                counts["raw_bid_ask_rows"] += 1
                if side_allowed:
                    funnel["raw_bid_ask_rows"] += 1
                    market_funnel["raw_bid_ask_markets"].add(market_key)
            if _side_has_quote_source(row, side):
                counts["quote_source_rows"] += 1
                if side_allowed:
                    funnel["quote_source_rows"] += 1
                    market_funnel["quote_source_markets"].add(market_key)
            entry = _side_entry_price(row, side)
            bid = _side_bid_price(row, side)
            spread = _side_spread(row, side)
            mid = _side_mid_price(row, side)
            executable = entry is not None and bid is not None and spread is not None and mid is not None
            if executable:
                counts["executable_bid_ask_rows"] += 1
                if side_allowed:
                    funnel["executable_bid_ask_rows"] += 1
                    market_funnel["executable_bid_ask_markets"].add(market_key)
            else:
                continue
            if entry is None:
                continue
            in_price_band = entry >= cfg.min_contract_price_dollars and (
                cfg.max_contract_price_dollars <= Decimal("0") or entry < cfg.max_contract_price_dollars
            )
            if in_price_band:
                counts["configured_price_band_rows"] += 1
                if side_allowed:
                    funnel["configured_price_band_rows"] += 1
                    market_funnel["configured_price_band_markets"].add(market_key)
            else:
                continue
            target_exit = _target_exit_price_for_net_profit(
                entry,
                target_pct=cfg.take_profit_pct,
                fee_rate=fee_rate,
            )
            target_possible = target_exit is not None and target_exit < Decimal("1.0000")
            if target_possible:
                counts["target_exit_possible_rows"] += 1
                if side_allowed:
                    funnel["target_exit_possible_rows"] += 1
                    market_funnel["target_exit_possible_markets"].add(market_key)
            else:
                continue
            max_spread = _configured_max_spread(entry, cfg)
            if spread is not None and spread <= max_spread:
                counts["spread_within_tier_rows"] += 1
                if side_allowed:
                    funnel["spread_within_tier_rows"] += 1
                    market_funnel["spread_within_tier_markets"].add(market_key)
    return {
        "entry_window_row_count": entry_window_rows,
        "entry_window_market_count": len(entry_window_markets),
        "side_quote_diagnostics": {
            side: dict(sorted(counts.items()))
            for side, counts in side_counts.items()
        },
        "side_filter_funnel": {
            side: dict(sorted(counts.items()))
            for side, counts in side_funnels.items()
        },
        "side_filter_market_funnel": {
            side: {
                stage: len(markets)
                for stage, markets in sorted(stages.items())
            }
            for side, stages in side_market_funnels.items()
        },
    }


def gate_reasons(
    metrics: dict[str, Any],
    *,
    settings: Settings,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> list[str]:
    asset = _normalize_asset_symbol(asset_symbol)
    freq = _normalize_touch_frequency(frequency)
    cfg = _asset_settings(settings, asset, frequency=freq)
    label = _strategy_label(asset, freq)
    if not metrics:
        return [f"{label} replay artifact is missing."]
    reasons: list[str] = []
    if metrics.get("backtest_missing"):
        reasons.append(f"{label} replay artifact is missing.")
    if metrics.get("uses_trained_model") is True:
        reasons.append(f"{label} replay must not use trained model predictions.")
    real_quote_rows = int(metrics.get("real_quote_path_row_count") or 0)
    if not metrics.get("backtest_missing") and real_quote_rows <= 0:
        reasons.append(f"{label} replay has no settled real quote-path evidence.")
    total_candidates = int(metrics.get("trade_candidate_count") or 0)
    use_allowed_scope = metrics.get("bucket_matrix") is not None
    allowed_summary = _allowed_bucket_metric_summary(metrics) if use_allowed_scope else {}
    candidates = (
        int(metrics.get("allowed_trade_candidate_count") or allowed_summary.get("count") or _allowed_bucket_candidate_count(metrics))
        if use_allowed_scope
        else total_candidates
    )
    min_candidates = cfg.replay_min_candidates
    net_pl = _scoped_decimal_metric(
        metrics,
        scoped_key="allowed_net_simulated_pl_dollars",
        total_key="net_simulated_pl_dollars",
        summary_key="net",
        allowed_summary=allowed_summary,
        use_allowed_scope=use_allowed_scope,
    )
    min_net = cfg.replay_min_net_pnl_dollars
    pnl_per = _scoped_decimal_metric(
        metrics,
        scoped_key="allowed_pnl_per_candidate_dollars",
        total_key="pnl_per_candidate_dollars",
        summary_key="pnl_per_candidate",
        allowed_summary=allowed_summary,
        use_allowed_scope=use_allowed_scope,
    )
    min_pnl_per = cfg.replay_min_pnl_per_candidate_dollars
    hard_cap_breaches = (
        int(metrics.get("allowed_hard_cap_breaches") or 0)
        if use_allowed_scope and metrics.get("allowed_hard_cap_breaches") is not None
        else int(metrics.get("hard_cap_breaches") or 0)
    )
    max_hard_cap = cfg.replay_max_hard_cap_breaches
    touch_rate = _scoped_decimal_metric(
        metrics,
        scoped_key="allowed_touch_rate",
        total_key="touch_rate",
        summary_key="touch_rate",
        allowed_summary=allowed_summary,
        use_allowed_scope=use_allowed_scope,
    )
    min_touch_rate = cfg.replay_min_touch_rate
    stop_loss_rate = _scoped_decimal_metric(
        metrics,
        scoped_key="allowed_stop_loss_rate",
        total_key="stop_loss_rate",
        summary_key="stop_loss_rate",
        allowed_summary=allowed_summary,
        use_allowed_scope=use_allowed_scope,
    )
    terminal_loss_rate = _scoped_decimal_metric(
        metrics,
        scoped_key="allowed_terminal_loss_rate",
        total_key="terminal_loss_rate",
        summary_key="terminal_loss_rate",
        allowed_summary=allowed_summary,
        use_allowed_scope=use_allowed_scope,
    )
    simulator_version = str(metrics.get("simulator_version") or "")
    if candidates < min_candidates:
        reasons.append(f"{label} allowed replay candidate count {candidates} below minimum {min_candidates}.")
    if net_pl <= min_net:
        reasons.append(f"{label} replay net P/L ${float(net_pl):.2f} does not clear required positive threshold.")
    if pnl_per < min_pnl_per:
        reasons.append(
            f"{label} replay P/L per candidate ${float(pnl_per):.4f} below minimum ${float(min_pnl_per):.4f}."
        )
    if hard_cap_breaches > max_hard_cap:
        reasons.append(f"{label} replay hard-cap breaches {hard_cap_breaches} exceed limit {max_hard_cap}.")
    if touch_rate < min_touch_rate:
        reasons.append(f"{label} replay touch rate {float(touch_rate):.1%} below minimum {float(min_touch_rate):.1%}.")
    if stop_loss_rate > cfg.max_replay_stop_loss_rate:
        reasons.append(
            f"{label} replay stop-loss rate {float(stop_loss_rate):.1%} exceeds maximum {float(cfg.max_replay_stop_loss_rate):.1%}."
        )
    if terminal_loss_rate > cfg.max_replay_terminal_loss_rate:
        reasons.append(
            f"{label} replay terminal-loss rate {float(terminal_loss_rate):.1%} exceeds maximum {float(cfg.max_replay_terminal_loss_rate):.1%}."
        )
    allowed_bucket_keys = {str(key) for key in (metrics.get("allowed_bucket_keys") or [])}
    for bucket in metrics.get("bucket_matrix") or []:
        if not isinstance(bucket, dict):
            continue
        key = str(bucket.get("bucket_key") or "unknown")
        if bucket.get("allowed") is not True and key not in allowed_bucket_keys:
            continue
        if _decimal(bucket.get("net_pnl") or "0") < Decimal("0"):
            reasons.append(f"{label} replay bucket {key} has negative P/L.")
        if Decimal(str(bucket.get("stop_loss_rate") or "0")) > cfg.max_replay_stop_loss_rate:
            reasons.append(f"{label} replay bucket {key} stop-loss rate exceeds maximum.")
        if Decimal(str(bucket.get("terminal_loss_rate") or "0")) > cfg.max_replay_terminal_loss_rate:
            reasons.append(f"{label} replay bucket {key} terminal-loss rate exceeds maximum.")
    if simulator_version != TOUCH20_RULES_REPLAY_SIMULATOR_VERSION:
        reasons.append(f"{label} replay simulator version is stale or missing.")
    if not (metrics.get("allowed_bucket_keys") or []):
        reasons.append(f"{label} replay has no allowed bucket support.")
    return reasons


def _evaluate_replay(
    snapshots: list[CryptoMarketSnapshotRecord],
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    settings: Settings,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    interval_seconds = _frequency_interval_seconds(freq)
    cfg = _asset_settings(settings, asset, frequency=freq)
    scoped_rows = [
        row
        for row in snapshots
        if _normalize_asset_symbol(row.asset_symbol) == asset
        and _normalize_touch_frequency(row.frequency) == freq
        and str(row.settlement_result or "").lower() in {"yes", "no"}
        and _has_allowed_side_quote_source(row, cfg)
    ]
    rows_by_market: dict[str, list[CryptoMarketSnapshotRecord]] = defaultdict(list)
    for row in scoped_rows:
        rows_by_market[row.market_ticker].append(row)
    spot_index = _prepare_spot_index(spot_rows, asset_symbol=asset)
    trades: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    for market_rows in rows_by_market.values():
        market_rows.sort(key=_snapshot_decision_time)
        for idx, row in enumerate(market_rows):
            decision_ts = _snapshot_decision_time(row)
            spot = _spot_features_from_index(
                spot_index,
                decision_ts=decision_ts,
                freshness_reference=decision_ts,
                max_age_seconds=max(cfg.spot_fresh_seconds, interval_seconds),
            )
            # Replay candidate generation is two-pass: first derive the bucket key
            # without bucket enforcement, then score with the bucket once matrix
            # support is known after all trades are collected. Use neutral bucket
            # support here to avoid leaking future bucket labels into candidacy.
            bootstrap_gate = {
                "allowed_bucket_keys": ["bootstrap"],
                "bucket_matrix": [
                    {
                        "bucket_key": "bootstrap",
                        "sample_count": 1,
                        "touch_rate": max(float(cfg.replay_min_touch_rate), 0.25),
                        "net_pnl": "0.0500",
                    }
                ],
            }
            candidates = rules_candidates_for_snapshot(
                row,
                settings=settings,
                spot=spot,
                gate_metrics=bootstrap_gate,
                require_allowed_bucket=False,
            )
            best = candidates[0] if candidates else {}
            status_counts[str(best.get("candidate_status") or "unknown")] += 1
            reason_counts[str(best.get("reason") or "unknown")] += 1
            selected = next((candidate for candidate in candidates if candidate.get("candidate_status") == "live_quality"), None)
            if selected is None:
                continue
            settlement_ts = _as_utc(row.close_time or row.expected_expiration_time) or decision_ts + timedelta(seconds=1)
            future_rows = [
                future
                for future in market_rows[idx + 1 :]
                if _snapshot_decision_time(future) > decision_ts and _snapshot_decision_time(future) < settlement_ts
            ]
            simulation = _simulate_replay_trade(row, future_rows, selected, settings=settings, spot_index=spot_index)
            trades.append(
                {
                    "market_ticker": row.market_ticker,
                    "decision_ts": decision_ts.isoformat(),
                    "settlement_result": row.settlement_result,
                    "candidate": selected,
                    "simulation": simulation,
                }
            )
            break
    bucket_matrix = _bucket_matrix(trades, settings=settings, asset_symbol=asset, frequency=freq)
    allowed_keys = [bucket["bucket_key"] for bucket in bucket_matrix if bucket.get("allowed")]
    blocked_keys = [bucket["bucket_key"] for bucket in bucket_matrix if not bucket.get("allowed")]
    total_trade_metrics = _replay_trade_metric_summary(trades)
    allowed_key_set = set(allowed_keys)
    allowed_trades = [
        trade
        for trade in trades
        if str((trade.get("simulation") or {}).get("bucket_key") or "") in allowed_key_set
    ]
    allowed_trade_metrics = _replay_trade_metric_summary(allowed_trades)
    allowed_trade_candidate_count = _allowed_bucket_candidate_count({"bucket_matrix": bucket_matrix})
    input_diagnostics = _replay_input_diagnostics(
        scoped_rows,
        settings=settings,
        asset_symbol=asset,
        frequency=freq,
    )
    metrics = {
        "objective": "touch_20pct_before_close",
        "strategy": _strategy_code(asset, frequency=freq),
        "frequency": freq,
        "uses_trained_model": False,
        "asset_symbols": [asset],
        "sample_count": len(scoped_rows),
        "real_quote_path_row_count": len(scoped_rows),
        "input_diagnostics": input_diagnostics,
        "entry_replay_mode": "first_eligible_per_market",
        "gate_candidate_scope": "allowed_replay_buckets",
        "bucket_price_band_cents": cfg.bucket_price_band_cents,
        "bucket_spread_band_cents": cfg.bucket_spread_band_cents,
        "bucket_time_band_minutes": cfg.bucket_time_band_minutes,
        "trade_candidate_count": total_trade_metrics["trade_candidate_count"],
        "allowed_trade_candidate_count": allowed_trade_candidate_count,
        "allowed_touch_count": allowed_trade_metrics["touch_count"],
        "allowed_touch_rate": allowed_trade_metrics["touch_rate"],
        "allowed_stop_loss_count": allowed_trade_metrics["stop_loss_count"],
        "allowed_stop_loss_rate": allowed_trade_metrics["stop_loss_rate"],
        "allowed_terminal_loss_count": allowed_trade_metrics["terminal_loss_count"],
        "allowed_terminal_loss_rate": allowed_trade_metrics["terminal_loss_rate"],
        "allowed_settlement_hold_count": allowed_trade_metrics["settlement_hold_count"],
        "allowed_exit_reason_counts": allowed_trade_metrics["exit_reason_counts"],
        "allowed_gross_simulated_pl_dollars": allowed_trade_metrics["gross_simulated_pl_dollars"],
        "allowed_net_simulated_pl_dollars": allowed_trade_metrics["net_simulated_pl_dollars"],
        "allowed_pnl_per_candidate_dollars": allowed_trade_metrics["pnl_per_candidate_dollars"],
        "allowed_fees_dollars": allowed_trade_metrics["fees_dollars"],
        "allowed_max_trade_drawdown_pct": allowed_trade_metrics["max_trade_drawdown_pct"],
        "allowed_hard_cap_breaches": allowed_trade_metrics["hard_cap_breaches"],
        "touch_count": total_trade_metrics["touch_count"],
        "touch_rate": total_trade_metrics["touch_rate"],
        "stop_loss_count": total_trade_metrics["stop_loss_count"],
        "stop_loss_rate": total_trade_metrics["stop_loss_rate"],
        "terminal_loss_count": total_trade_metrics["terminal_loss_count"],
        "terminal_loss_rate": total_trade_metrics["terminal_loss_rate"],
        "settlement_hold_count": total_trade_metrics["settlement_hold_count"],
        "exit_reason_counts": total_trade_metrics["exit_reason_counts"],
        "gross_simulated_pl_dollars": total_trade_metrics["gross_simulated_pl_dollars"],
        "net_simulated_pl_dollars": total_trade_metrics["net_simulated_pl_dollars"],
        "pnl_per_candidate_dollars": total_trade_metrics["pnl_per_candidate_dollars"],
        "fees_dollars": total_trade_metrics["fees_dollars"],
        "max_trade_drawdown_pct": total_trade_metrics["max_trade_drawdown_pct"],
        "hard_cap_breaches": total_trade_metrics["hard_cap_breaches"],
        "candidate_status_counts": dict(status_counts),
        "candidate_reason_counts": dict(reason_counts),
        "bucket_matrix": bucket_matrix,
        "allowed_bucket_keys": allowed_keys,
        "blocked_bucket_keys": blocked_keys,
        "bucket_live_blocked_keys": [],
        "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
        "fee_model_version": current_fee_model_version(),
    }
    return {
        "status": "ok" if trades else "warn",
        "metrics": metrics,
        "bucket_matrix": bucket_matrix,
        "trade_sample": trades[:100],
    }


def _optimizer_profile_specs(
    settings: Settings,
    *,
    asset_symbol: str,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> list[dict[str, Any]]:
    cfg = _asset_settings(settings, asset_symbol, frequency=frequency)

    def spec(name: str, **updates: Any) -> dict[str, Any]:
        return {"name": name, "settings_overrides": updates}

    profiles = [
        spec("current"),
        spec("cap_40c", max_contract_price_dollars=0.40),
        spec("cap_45c", max_contract_price_dollars=0.45),
        spec(
            "min_25c_cap_40c",
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "min_25c_cap_45c",
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.45,
        ),
        spec(
            "min_20c_cap_40c",
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "score_45_cap_40c",
            min_rule_score=0.45,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "score_45_min_25c_cap_40c",
            min_rule_score=0.45,
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "window_10m_min_25c_cap_40c",
            min_seconds_to_close=600,
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "take_15pct_min_25c_cap_40c",
            take_profit_pct=0.15,
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "stop_15pct_min_25c_cap_40c",
            stop_loss_pct=0.15,
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "yes_no_min_25c_cap_40c",
            allowed_sides="yes,no",
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "no_only_min_25c_cap_40c",
            allowed_sides="no",
            min_contract_price_dollars=0.25,
            max_contract_price_dollars=0.40,
        ),
        spec(
            "yes_no_open_s30",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "no_open_s30",
            allowed_sides="no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_open_s30",
            allowed_sides="yes",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_no_20_70_s20",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.70,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "yes_20_70_s20",
            allowed_sides="yes",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.70,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "no_20_70_s20",
            allowed_sides="no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.70,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "yes_no_60_80_s25",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.60,
            max_contract_price_dollars=0.80,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "yes_no_60_85_s25",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.60,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "yes_no_take15_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.15,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_take15_open_s30",
            allowed_sides="yes",
            take_profit_pct=0.15,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "no_take15_open_s30",
            allowed_sides="no",
            take_profit_pct=0.15,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_no_take10_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_take10_open_s30",
            allowed_sides="yes",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "no_take10_open_s30",
            allowed_sides="no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_no_take15_stop40_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.15,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_take15_stop40_open_s30",
            allowed_sides="yes",
            take_profit_pct=0.15,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "no_take15_stop40_open_s30",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "no_take15_stop40_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take15_stop40_open_s20",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "no_take15_stop45_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.45,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take15_stop50_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take15_stop50_open_s20",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "no_take10_stop40_open_s25",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take10_stop45_open_s25",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.45,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take10_stop50_open_s25",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "no_take10_stop50_open_s20",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "yes_no_take10_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
        ),
        spec(
            "yes_no_take10_maxspread5_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            max_spread_dollars=0.05,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take10_maxspread10_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            max_spread_dollars=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take15_maxspread5_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.15,
            max_spread_dollars=0.05,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take15_maxspread10_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.15,
            max_spread_dollars=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take10_open_s20",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
        ),
        spec(
            "yes_no_take10_stop40_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.40,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_no_take10_stop50_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
        ),
        spec(
            "yes_no_take10_stop50_time10_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_time_band_minutes=10,
        ),
        spec(
            "yes_no_take10_stop50_time10_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_time_band_minutes=10,
        ),
        spec(
            "yes_no_take10_stop50_spread2_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take10_stop50_spread2_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_price20_open_s30",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=20,
        ),
        spec(
            "yes_no_take10_price20_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=20,
        ),
        spec(
            "yes_no_take10_price20_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=20,
        ),
        spec(
            "yes_take10_price20_open_s30",
            allowed_sides="yes",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=20,
        ),
        spec(
            "no_take10_price20_open_s30",
            allowed_sides="no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=20,
        ),
        spec(
            "yes_no_price30_open_s30",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=30,
        ),
        spec(
            "yes_no_take10_price30_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=30,
        ),
        spec(
            "yes_no_take10_price30_spread2_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=30,
            bucket_spread_band_cents=2,
        ),
        spec(
            "yes_no_take10_price30_spread2_open_s25",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=30,
            bucket_spread_band_cents=2,
        ),
        spec(
            "no_take15_stop50_price30_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=30,
        ),
        spec(
            "yes_price30_open_s30",
            allowed_sides="yes",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=30,
        ),
        spec(
            "no_price30_open_s30",
            allowed_sides="no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=30,
        ),
        spec(
            "yes_no_price40_open_s30",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=40,
        ),
        spec(
            "yes_no_take10_price40_open_s30",
            allowed_sides="yes,no",
            take_profit_pct=0.10,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=40,
        ),
        spec(
            "no_take15_stop50_price40_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=40,
        ),
        spec(
            "yes_price40_open_s30",
            allowed_sides="yes",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=40,
        ),
        spec(
            "no_price40_open_s30",
            allowed_sides="no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=40,
        ),
        spec(
            "yes_no_price30_20_70_s20",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.70,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
            bucket_price_band_cents=30,
        ),
        spec(
            "yes_no_price40_20_70_s20",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.20,
            max_contract_price_dollars=0.70,
            min_aligned_momentum=0.0,
            min_rule_score=0.20,
            bucket_price_band_cents=40,
        ),
        spec(
            "yes_no_price20_60_80_s25",
            allowed_sides="yes,no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.60,
            max_contract_price_dollars=0.80,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=20,
        ),
        spec(
            "no_take15_stop50_price20_open_s25",
            allowed_sides="no",
            take_profit_pct=0.15,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=20,
        ),
        spec(
            "no_take10_stop50_price20_open_s25",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=20,
        ),
        spec(
            "no_take10_stop50_price20_spread2_open_s25",
            allowed_sides="no",
            take_profit_pct=0.10,
            stop_loss_pct=0.50,
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.25,
            bucket_price_band_cents=20,
            bucket_spread_band_cents=2,
        ),
        spec(
            "no_price20_open_s30",
            allowed_sides="no",
            min_seconds_to_close=300,
            min_contract_price_dollars=0.10,
            max_contract_price_dollars=0.85,
            min_aligned_momentum=0.0,
            min_rule_score=0.30,
            bucket_price_band_cents=20,
        ),
        spec(
            "current_rules_min_candidates_only",
            replay_min_candidates=min(cfg.replay_min_candidates, 15),
        ),
    ]
    if _normalize_touch_frequency(frequency) == "1h":
        profiles.extend(
            [
                spec(
                    "yes_no_take15_maxspread10_time60_price40_open_s25",
                    allowed_sides="yes,no",
                    take_profit_pct=0.15,
                    max_spread_dollars=0.10,
                    min_seconds_to_close=300,
                    min_contract_price_dollars=0.10,
                    max_contract_price_dollars=0.85,
                    min_aligned_momentum=0.0,
                    min_rule_score=0.25,
                    bucket_price_band_cents=40,
                    bucket_spread_band_cents=2,
                    bucket_time_band_minutes=60,
                ),
                spec(
                    "yes_no_take10_maxspread10_time60_price40_open_s25",
                    allowed_sides="yes,no",
                    take_profit_pct=0.10,
                    max_spread_dollars=0.10,
                    min_seconds_to_close=300,
                    min_contract_price_dollars=0.10,
                    max_contract_price_dollars=0.85,
                    min_aligned_momentum=0.0,
                    min_rule_score=0.25,
                    bucket_price_band_cents=40,
                    bucket_spread_band_cents=2,
                    bucket_time_band_minutes=60,
                ),
                spec(
                    "yes_no_take15_time60_price40_open_s30",
                    allowed_sides="yes,no",
                    take_profit_pct=0.15,
                    min_seconds_to_close=300,
                    min_contract_price_dollars=0.10,
                    max_contract_price_dollars=0.85,
                    min_aligned_momentum=0.0,
                    min_rule_score=0.30,
                    bucket_price_band_cents=40,
                    bucket_time_band_minutes=60,
                ),
            ]
        )
    return profiles


def _optimizer_replay_fetch_window(
    settings: Settings,
    *,
    asset_symbol: str,
    frequency: str,
) -> tuple[int, int]:
    cfg = _asset_settings(settings, asset_symbol, frequency=frequency)
    min_seconds_to_close = int(cfg.min_seconds_to_close)
    min_market_age_seconds = int(cfg.min_market_age_seconds)
    for profile in _optimizer_profile_specs(settings, asset_symbol=asset_symbol, frequency=frequency):
        overrides = dict(profile.get("settings_overrides") or {})
        min_seconds_to_close = min(
            min_seconds_to_close,
            _int_override(overrides, "min_seconds_to_close", int(cfg.min_seconds_to_close)),
        )
        min_market_age_seconds = min(
            min_market_age_seconds,
            _int_override(overrides, "min_market_age_seconds", int(cfg.min_market_age_seconds)),
        )
    return max(0, min_seconds_to_close), max(0, min_market_age_seconds)


def _entry_qualified_market_limit(settings: Settings, *, frequency: str, row_limit: int) -> int:
    if _normalize_touch_frequency(frequency) == "1h":
        configured = max(1, int(getattr(settings, "crypto_1h_touch20_entry_qualified_market_limit", 500) or 500))
        return max(1, min(int(row_limit), configured))
    return max(1, min(int(row_limit), max(10_000, int(row_limit) // 20)))


def _settings_with_optimizer_asset_overrides(
    settings: Settings,
    *,
    asset_symbol: str,
    overrides: dict[str, Any],
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> Settings:
    if not overrides:
        return settings
    freq = _normalize_touch_frequency(frequency)
    asset = _normalize_asset_symbol(asset_symbol)
    setting_name = "crypto_1h_touch20_asset_settings" if freq == "1h" else "crypto_15m_touch20_asset_settings"
    raw = dict(getattr(settings, setting_name, {}) or {})
    existing = _asset_overrides(settings, asset, frequency=freq)
    raw[asset] = {**existing, **overrides}
    return settings.model_copy(update={setting_name: raw})


def _profile_summary(
    *,
    name: str,
    settings_overrides: dict[str, Any],
    metrics: dict[str, Any],
    reasons: list[str],
    non_promotable_reasons: list[str] | None = None,
    settings: Settings,
    asset_symbol: str,
    frequency: str,
) -> dict[str, Any]:
    cfg = _asset_settings(settings, asset_symbol, frequency=frequency)
    bucket_matrix = list(metrics.get("bucket_matrix") or [])
    passed = not reasons
    non_promotable = list(non_promotable_reasons or [])
    promotable = not non_promotable
    status = "blocked"
    if passed:
        status = "passed" if promotable else "diagnostic_passed"
    return {
        "profile": name,
        "status": status,
        "passed": passed,
        "promotable": promotable,
        "promotable_passed": passed and promotable,
        "non_promotable_reasons": non_promotable,
        "settings_overrides": dict(settings_overrides),
        "requirements": _gate_requirements(settings, asset_symbol=asset_symbol, frequency=frequency),
        "reason_count": len(reasons),
        "gate_reasons": reasons,
        "trade_candidate_count": int(metrics.get("trade_candidate_count") or 0),
        "allowed_trade_candidate_count": int(
            metrics.get("allowed_trade_candidate_count") or _allowed_bucket_candidate_count({"bucket_matrix": bucket_matrix})
        ),
        "min_trade_candidates": cfg.replay_min_candidates,
        "gate_candidate_scope": metrics.get("gate_candidate_scope"),
        "allowed_net_simulated_pl_dollars": float(metrics.get("allowed_net_simulated_pl_dollars") or 0.0),
        "allowed_pnl_per_candidate_dollars": float(metrics.get("allowed_pnl_per_candidate_dollars") or 0.0),
        "allowed_touch_rate": float(metrics.get("allowed_touch_rate") or 0.0),
        "allowed_stop_loss_rate": float(metrics.get("allowed_stop_loss_rate") or 0.0),
        "allowed_terminal_loss_rate": float(metrics.get("allowed_terminal_loss_rate") or 0.0),
        "net_simulated_pl_dollars": float(metrics.get("net_simulated_pl_dollars") or 0.0),
        "pnl_per_candidate_dollars": float(metrics.get("pnl_per_candidate_dollars") or 0.0),
        "touch_rate": float(metrics.get("touch_rate") or 0.0),
        "stop_loss_rate": float(metrics.get("stop_loss_rate") or 0.0),
        "terminal_loss_rate": float(metrics.get("terminal_loss_rate") or 0.0),
        "exit_reason_counts": dict(metrics.get("exit_reason_counts") or {}),
        "allowed_bucket_keys": list(metrics.get("allowed_bucket_keys") or []),
        "blocked_bucket_keys": list(metrics.get("blocked_bucket_keys") or []),
        "bucket_matrix": bucket_matrix[:10],
        "simulator_version": metrics.get("simulator_version"),
    }


def _optimizer_non_promotable_reasons(
    *,
    base_settings: Settings,
    profile_settings: Settings,
    asset_symbol: str,
    frequency: str,
) -> list[str]:
    base_cfg = _asset_settings(base_settings, asset_symbol, frequency=frequency)
    profile_cfg = _asset_settings(profile_settings, asset_symbol, frequency=frequency)
    reasons: list[str] = []
    if int(profile_cfg.replay_min_candidates) < int(base_cfg.replay_min_candidates):
        reasons.append("replay_min_candidates_relaxed_below_configured_gate")
    return reasons


def _optimizer_result_status(profiles: list[dict[str, Any]]) -> str:
    if any(profile.get("promotable_passed") for profile in profiles):
        return "passed_profile_found"
    if any(profile.get("passed") and not profile.get("promotable") for profile in profiles):
        return "diagnostic_profile_found"
    return "no_passed_profile"


def optimize_replay_profiles(
    snapshots: list[CryptoMarketSnapshotRecord],
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    settings: Settings,
    asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
    top_n: int = 10,
    profile_names: Iterable[str] | None = None,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    freq = _normalize_touch_frequency(frequency)
    requested_profiles = {
        str(name).strip()
        for name in (profile_names or [])
        if str(name).strip()
    }
    profiles: list[dict[str, Any]] = []
    profile_specs = _optimizer_profile_specs(settings, asset_symbol=asset, frequency=freq)
    if requested_profiles:
        profile_specs = [
            profile
            for profile in profile_specs
            if str(profile.get("name") or "") in requested_profiles
        ]
    for profile in profile_specs:
        overrides = dict(profile.get("settings_overrides") or {})
        profile_settings = _settings_with_optimizer_asset_overrides(
            settings,
            asset_symbol=asset,
            overrides=overrides,
            frequency=freq,
        )
        replay = _evaluate_replay(snapshots, spot_rows, settings=profile_settings, asset_symbol=asset, frequency=freq)
        metrics = dict(replay.get("metrics") or {})
        reasons = gate_reasons(metrics, settings=profile_settings, asset_symbol=asset, frequency=freq)
        non_promotable_reasons = _optimizer_non_promotable_reasons(
            base_settings=settings,
            profile_settings=profile_settings,
            asset_symbol=asset,
            frequency=freq,
        )
        profiles.append(
            _profile_summary(
                name=str(profile.get("name") or "profile"),
                settings_overrides=overrides,
                metrics=metrics,
                reasons=reasons,
                non_promotable_reasons=non_promotable_reasons,
                settings=profile_settings,
                asset_symbol=asset,
                frequency=freq,
            )
        )

    profiles.sort(key=_optimizer_profile_sort_key, reverse=True)
    limit = max(0, int(top_n or 0))
    shown = profiles if limit <= 0 else profiles[:limit]
    promotable_profiles = [profile for profile in profiles if profile.get("promotable")]
    return {
        "status": _optimizer_result_status(profiles),
        "asset_symbol": asset,
        "frequency": freq,
        "profile_filter": sorted(requested_profiles),
        "profile_count": len(profiles),
        "top_n": limit,
        "best_profile": profiles[0] if profiles else None,
        "best_promotable_profile": promotable_profiles[0] if promotable_profiles else None,
        "profiles": shown,
    }


def _optimizer_profile_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    candidate_count = int(item.get("allowed_trade_candidate_count") or item.get("trade_candidate_count") or 0)
    min_candidates = max(1, int(item.get("min_trade_candidates") or 0))
    reason_count = int(item.get("reason_count") or 0)
    net_pnl = float(item.get("allowed_net_simulated_pl_dollars") or item.get("net_simulated_pl_dollars") or 0.0)
    pnl_per = float(item.get("allowed_pnl_per_candidate_dollars") or item.get("pnl_per_candidate_dollars") or 0.0)
    touch_rate = float(item.get("allowed_touch_rate") or item.get("touch_rate") or 0.0)
    allowed_bucket_count = len(item.get("allowed_bucket_keys") or [])
    candidate_ratio = min(1.0, candidate_count / float(min_candidates))
    profitable = net_pnl > 0.0 and pnl_per > 0.0
    return (
        bool(item.get("promotable_passed")),
        bool(item.get("passed")),
        bool(item.get("promotable", True)),
        profitable,
        allowed_bucket_count > 0,
        candidate_ratio,
        candidate_count,
        -reason_count,
        pnl_per,
        net_pnl,
        touch_rate,
    )


def _artifact_summary(artifact: Any | None) -> dict[str, Any] | None:
    if artifact is None:
        return None
    payload = getattr(artifact, "payload", None)
    metrics = getattr(artifact, "metrics", None)
    metrics = metrics if isinstance(metrics, dict) else {}
    return {
        "artifact_type": getattr(artifact, "artifact_type", None),
        "version": getattr(artifact, "version", None),
        "status": getattr(artifact, "status", None),
        "sample_count": getattr(artifact, "sample_count", None),
        "trade_candidate_count": metrics.get("trade_candidate_count"),
        "allowed_trade_candidate_count": metrics.get("allowed_trade_candidate_count"),
        "gate_candidate_scope": metrics.get("gate_candidate_scope"),
        "bucket_price_band_cents": metrics.get("bucket_price_band_cents"),
        "bucket_spread_band_cents": metrics.get("bucket_spread_band_cents"),
        "bucket_time_band_minutes": metrics.get("bucket_time_band_minutes"),
        "allowed_net_simulated_pl_dollars": metrics.get("allowed_net_simulated_pl_dollars"),
        "allowed_pnl_per_candidate_dollars": metrics.get("allowed_pnl_per_candidate_dollars"),
        "allowed_touch_rate": metrics.get("allowed_touch_rate"),
        "allowed_stop_loss_rate": metrics.get("allowed_stop_loss_rate"),
        "allowed_terminal_loss_rate": metrics.get("allowed_terminal_loss_rate"),
        "allowed_bucket_keys": list(metrics.get("allowed_bucket_keys") or []),
        "blocked_bucket_keys": list(metrics.get("blocked_bucket_keys") or []),
        "passed": payload.get("passed") if isinstance(payload, dict) else None,
        "simulator_version": payload.get("simulator_version") if isinstance(payload, dict) else None,
    }


def _gate_live_evidence(metrics: dict[str, Any], live_bucket_controls: dict[str, Any]) -> dict[str, Any]:
    blocked_keys = {str(key) for key in (live_bucket_controls.get("blocked_bucket_keys") or [])}
    allowed_keys = {str(key) for key in (metrics.get("allowed_bucket_keys") or [])}
    live_blocked_allowed_keys = sorted(allowed_keys & blocked_keys)
    return {
        "trade_candidate_count": int(metrics.get("trade_candidate_count") or 0),
        "allowed_trade_candidate_count": int(
            metrics.get("allowed_trade_candidate_count") or _allowed_bucket_candidate_count(metrics)
        ),
        "live_executable_candidate_count": _allowed_bucket_candidate_count(metrics, exclude_bucket_keys=blocked_keys),
        "live_blocked_allowed_bucket_keys": live_blocked_allowed_keys,
    }


def _gate_passed(gate: Any | None) -> bool:
    if gate is None:
        return False
    if str(getattr(gate, "status", "") or "").lower() != "passed":
        return False
    payload = getattr(gate, "payload", None)
    metrics = getattr(gate, "metrics", None)
    simulator_version = None
    if isinstance(payload, dict):
        requirements = payload.get("requirements") if isinstance(payload.get("requirements"), dict) else {}
        simulator_version = payload.get("simulator_version") or requirements.get("simulator_version")
    if simulator_version is None and isinstance(metrics, dict):
        simulator_version = metrics.get("simulator_version")
    if simulator_version != TOUCH20_RULES_REPLAY_SIMULATOR_VERSION:
        return False
    if isinstance(payload, dict) and "passed" in payload:
        return payload.get("passed") is True
    return True


def _approval_payload(checkpoint: Any | None) -> dict[str, Any]:
    return dict(getattr(checkpoint, "payload", None) or {})


def _approval_valid(approval: dict[str, Any], gate: Any | None) -> tuple[bool, str]:
    if not approval.get("approved"):
        return False, "operator_approval_missing"
    gate_version = getattr(gate, "version", None)
    if not gate_version:
        return False, "gate_version_missing"
    payload = getattr(gate, "payload", None)
    metrics = getattr(gate, "metrics", None)
    gate_simulator_version = None
    if isinstance(payload, dict):
        requirements = payload.get("requirements") if isinstance(payload.get("requirements"), dict) else {}
        gate_simulator_version = payload.get("simulator_version") or requirements.get("simulator_version")
    if gate_simulator_version is None and isinstance(metrics, dict):
        gate_simulator_version = metrics.get("simulator_version")
    if gate_simulator_version != TOUCH20_RULES_REPLAY_SIMULATOR_VERSION:
        return False, "gate_simulator_version_stale_or_missing"
    if str(approval.get("simulator_version") or "") != str(gate_simulator_version):
        return False, "operator_approval_simulator_version_mismatch"
    if str(approval.get("gate_version") or "") != str(gate_version):
        return False, "operator_approval_gate_version_mismatch"
    return True, "operator_approval_valid"


def _ledger_payload(
    checkpoint: Any | None,
    *,
    kalshi_env: str,
    asset_symbol: str,
    frequency: str,
) -> dict[str, Any]:
    payload = dict(getattr(checkpoint, "payload", None) or {})
    positions = payload.get("positions") if isinstance(payload.get("positions"), dict) else {}
    freq = _normalize_touch_frequency(frequency)
    return {
        "schema_version": "crypto-touch20-rules-ledger-v2",
        "strategy": _strategy_code(asset_symbol, frequency=freq),
        "kalshi_env": kalshi_env,
        "asset_symbol": _normalize_asset_symbol(asset_symbol),
        "frequency": freq,
        "positions": dict(positions),
        "updated_at": payload.get("updated_at"),
    }


def _open_entries(ledger: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""), frequency=str(ledger.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:"):
            continue
        if isinstance(entry, dict) and str(entry.get("status") or "") in {"open", "exit_submitted"}:
            entries.append((str(client_order_id), entry))
    return entries


def _open_pending_notional(ledger: dict[str, Any]) -> Decimal:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    total = Decimal("0")
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""), frequency=str(ledger.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:"):
            continue
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status") or "") in {"entry_submitted", "open", "exit_submitted"}:
            total += _decimal(entry.get("entry_notional_dollars") or "0")
    return total.quantize(Decimal("0.0001"))


def _daily_realized_pnl(ledger: dict[str, Any], now: datetime) -> Decimal:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    total = Decimal("0")
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""), frequency=str(ledger.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:"):
            continue
        if not isinstance(entry, dict) or not entry.get("closed_at"):
            continue
        try:
            closed_at = datetime.fromisoformat(str(entry["closed_at"]))
        except ValueError:
            continue
        closed_at = closed_at.replace(tzinfo=UTC) if closed_at.tzinfo is None else closed_at.astimezone(UTC)
        if closed_at.date() == now.astimezone(UTC).date():
            total += _decimal(entry.get("realized_pnl_dollars") or "0")
    return total.quantize(Decimal("0.0001"))


def _market_strategy_exposure(ledger: dict[str, Any], market_ticker: str) -> list[dict[str, Any]]:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""), frequency=str(ledger.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    exposure: list[dict[str, Any]] = []
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:") or not isinstance(entry, dict):
            continue
        if str(entry.get("market_ticker") or "") != market_ticker:
            continue
        status = str(entry.get("status") or "")
        if status in {"entry_submitted", "open", "exit_submitted"}:
            exposure.append(
                {
                    "client_order_id": str(client_order_id),
                    "status": status,
                    "entry_notional_dollars": entry.get("entry_notional_dollars"),
                }
            )
    return exposure


def _loss_cooldown_for_market(
    ledger: dict[str, Any],
    market_ticker: str,
    *,
    now: datetime,
    cooldown_seconds: int = BTC15M_TOUCH20_RULES_INTERVAL_SECONDS,
) -> dict[str, Any] | None:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""), frequency=str(ledger.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    latest_exit: tuple[datetime, str, dict[str, Any]] | None = None
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:") or not isinstance(entry, dict):
            continue
        if str(entry.get("market_ticker") or "") != market_ticker:
            continue
        trigger = str(entry.get("exit_trigger") or "")
        is_loss_exit = "stop_loss" in trigger or "terminal" in trigger
        is_tp_exit = "take_profit" in trigger
        if not is_loss_exit and not is_tp_exit:
            continue
        # Loss exits: only cooldown on negative P&L. TP exits: always cooldown.
        if is_loss_exit and _decimal(entry.get("realized_pnl_dollars") or "0") >= Decimal("0"):
            continue
        closed_at = _datetime_from_any(entry.get("closed_at"))
        if closed_at is None:
            continue
        if latest_exit is None or closed_at > latest_exit[0]:
            latest_exit = (closed_at, str(client_order_id), entry)
    if latest_exit is None:
        return None
    cooldown_until = latest_exit[0] + timedelta(seconds=cooldown_seconds)
    if now >= cooldown_until:
        return None
    return {
        "client_order_id": latest_exit[1],
        "exit_trigger": latest_exit[2].get("exit_trigger"),
        "realized_pnl_dollars": latest_exit[2].get("realized_pnl_dollars"),
        "closed_at": latest_exit[0].isoformat(),
        "cooldown_until": cooldown_until.isoformat(),
    }


def _bucket_proved_by_current_replay(bucket_key: str, gate_metrics: dict[str, Any] | None) -> bool:
    metrics = gate_metrics or {}
    if metrics.get("simulator_version") != TOUCH20_RULES_REPLAY_SIMULATOR_VERSION:
        return False
    if bucket_key not in {str(key) for key in (metrics.get("allowed_bucket_keys") or [])}:
        return False
    for bucket in metrics.get("bucket_matrix") or []:
        if not isinstance(bucket, dict) or str(bucket.get("bucket_key") or "") != bucket_key:
            continue
        return (
            bucket.get("allowed") is True
            and _decimal(bucket.get("net_pnl") or "0") > Decimal("0")
            and Decimal(str(bucket.get("stop_loss_rate") or "0")) <= Decimal(str(metrics.get("max_replay_stop_loss_rate") or "1"))
        )
    return False


def _live_bucket_controls(
    ledger: dict[str, Any],
    *,
    settings: Settings,
    asset_symbol: str,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
    gate_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol)
    freq = _normalize_touch_frequency(frequency)
    cfg = _asset_settings(settings, asset, frequency=freq)
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    prefix = _order_prefix(str(ledger.get("asset_symbol") or asset), frequency=str(ledger.get("frequency") or freq))
    grouped: dict[str, list[tuple[datetime, str, dict[str, Any]]]] = defaultdict(list)
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:") or not isinstance(entry, dict):
            continue
        if str(entry.get("status") or "") != "closed":
            continue
        bucket_key = str(entry.get("bucket_key") or "")
        closed_at = _datetime_from_any(entry.get("closed_at"))
        if not bucket_key or closed_at is None:
            continue
        grouped[bucket_key].append((closed_at, str(client_order_id), entry))

    seeded_bucket_keys = set(
        TOUCH20_RULES_REMEDIATION_BLOCKED_BTC_BUCKETS
        if asset == BTC15M_TOUCH20_RULES_ASSET and freq == BTC15M_TOUCH20_RULES_FREQ
        else frozenset()
    )
    for bucket_key in seeded_bucket_keys:
        grouped.setdefault(bucket_key, [])

    bucket_stats: list[dict[str, Any]] = []
    blocked_keys: list[str] = []
    for bucket_key, rows in grouped.items():
        rows.sort(key=lambda item: item[0])
        realized = sum((_decimal(entry.get("realized_pnl_dollars") or "0") for _, _, entry in rows), Decimal("0")).quantize(Decimal("0.0001"))
        loss_count = sum(1 for _, _, entry in rows if _decimal(entry.get("realized_pnl_dollars") or "0") < Decimal("0"))
        stop_terminal_losses = sum(
            1
            for _, _, entry in rows
            if _decimal(entry.get("realized_pnl_dollars") or "0") < Decimal("0")
            and ("stop_loss" in str(entry.get("exit_trigger") or "") or "terminal" in str(entry.get("exit_trigger") or ""))
        )
        consecutive = 0
        for _, _, entry in reversed(rows):
            trigger = str(entry.get("exit_trigger") or "")
            if _decimal(entry.get("realized_pnl_dollars") or "0") < Decimal("0") and ("stop_loss" in trigger or "terminal" in trigger):
                consecutive += 1
            else:
                break
        reasons: list[str] = []
        if realized <= -cfg.max_bucket_live_loss_dollars:
            reasons.append("bucket_live_loss_limit")
        if consecutive >= cfg.max_bucket_consecutive_losses:
            reasons.append("bucket_consecutive_stop_or_terminal_losses")
        if (
            bucket_key in seeded_bucket_keys
            and not rows
            and not _bucket_proved_by_current_replay(bucket_key, gate_metrics)
        ):
            reasons.append("seeded_remediation_block")
        if reasons:
            blocked_keys.append(bucket_key)
        bucket_stats.append(
            {
                "bucket_key": bucket_key,
                "trade_count": len(rows),
                "loss_count": loss_count,
                "stop_or_terminal_loss_count": stop_terminal_losses,
                "consecutive_stop_or_terminal_losses": consecutive,
                "realized_pnl_dollars": _money_text(realized),
                "blocked": bool(reasons),
                "block_reasons": reasons,
            }
        )
    bucket_stats.sort(key=lambda item: (item["blocked"], _decimal(item.get("realized_pnl_dollars") or "0")), reverse=True)
    return {
        "max_bucket_live_loss_dollars": _money_text(cfg.max_bucket_live_loss_dollars),
        "max_bucket_consecutive_losses": cfg.max_bucket_consecutive_losses,
        "seeded_blocked_bucket_keys": sorted(seeded_bucket_keys),
        "blocked_bucket_keys": sorted(blocked_keys),
        "buckets": bucket_stats,
    }


def _count_for_cap(remaining_cap: Decimal, entry_side_price: Decimal) -> Decimal | None:
    if remaining_cap <= Decimal("0") or entry_side_price <= Decimal("0"):
        return None
    raw_count = (remaining_cap / entry_side_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if raw_count <= Decimal("0"):
        return None
    try:
        return quantize_count(raw_count)
    except ValueError:
        return None


def _client_order_id(
    action: str,
    *,
    market_ticker: str,
    side: str,
    now: datetime,
    asset_symbol: str,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
) -> str:
    strategy = _strategy_code(asset_symbol, frequency=frequency)
    basis = f"{strategy}:{action}:{market_ticker}:{side}:{now.isoformat()}".encode("utf-8")
    digest = hashlib.blake2b(basis, digest_size=10).hexdigest()
    return f"{_order_prefix(asset_symbol, frequency=frequency)}:{action[:1]}:{digest}"


def _selection_summary(item: dict[str, Any]) -> dict[str, Any]:
    market = item.get("market")
    candidate = item.get("candidate") if isinstance(item.get("candidate"), dict) else {}
    bucket = candidate.get("bucket") if isinstance(candidate.get("bucket"), dict) else {}
    return {
        "market_ticker": getattr(market, "market_ticker", None),
        "asset_symbol": getattr(market, "asset_symbol", None),
        "frequency": getattr(market, "frequency", None),
        "side": candidate.get("side"),
        "execution_price_dollars": candidate.get("execution_price_dollars"),
        "target_yes_price_dollars": candidate.get("target_yes_price_dollars"),
        "target_exit_side_price_dollars": candidate.get("target_exit_side_price_dollars"),
        "rule_score": candidate.get("rule_score"),
        "score_components": candidate.get("score_components"),
        "spread_dollars": candidate.get("spread_dollars"),
        "time_to_close_seconds": candidate.get("time_to_close_seconds"),
        "bucket_key": candidate.get("bucket_key"),
        "bucket_touch_rate": bucket.get("touch_rate"),
        "bucket_net_pnl": bucket.get("net_pnl"),
        "bucket_sample_count": bucket.get("sample_count"),
        "objective": candidate.get("objective"),
        "uses_trained_model": False,
    }


def _entry_payload(
    *,
    market: CryptoMarketSnapshotRecord,
    candidate: dict[str, Any],
    count_fp: Decimal,
    side: str,
    target_yes: Decimal,
    entry_side_price: Decimal,
    status: str,
    client_order_id: str,
    receipt: ExecReceiptPayload,
    gate: Any,
    approval: dict[str, Any],
    now: datetime,
    settings: Settings,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(market.asset_symbol)
    freq = _normalize_touch_frequency(market.frequency)
    cfg = _asset_settings(settings, asset, frequency=freq)
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    return {
        "status": status,
        "strategy": _strategy_code(asset, frequency=freq),
        "client_order_id": client_order_id,
        "market_ticker": market.market_ticker,
        "asset_symbol": asset,
        "frequency": market.frequency,
        "side": side,
        "count_fp": _count_text(count_fp),
        "entry_yes_price_dollars": _money_text(target_yes),
        "entry_side_price_dollars": _money_text(entry_side_price),
        "entry_notional_dollars": _money_text(entry_side_price * count_fp),
        "entry_fee_dollars": _money_text(entry_fee),
        "opened_at": now.isoformat(),
        "close_time": (_as_utc(market.close_time or market.expected_expiration_time) or now).isoformat(),
        "bucket_key": candidate.get("bucket_key"),
        "bucket": candidate.get("bucket") or {},
        "candidate": _selection_summary({"market": market, "candidate": candidate}),
        "target_exit_side_price_dollars": candidate.get("target_exit_side_price_dollars"),
        "take_profit_pct": float(cfg.take_profit_pct),
        "stop_loss_pct": float(cfg.stop_loss_pct),
        "profit_protection_threshold_pct": float(cfg.profit_protection_threshold_pct),
        "profit_protection_floor_pct": float(cfg.profit_protection_floor_pct),
        "profit_protection_armed": False,
        "max_net_profit_pct": "0.0000",
        "quote_history": [],
        "entry_receipt": receipt.model_dump(mode="json"),
        "kalshi_order_id": receipt.external_order_id,
        "gate_version": getattr(gate, "version", None),
        "gate_artifact_type": getattr(gate, "artifact_type", None),
        "approval": {
            "gate_version": approval.get("gate_version"),
            "simulator_version": approval.get("simulator_version"),
            "approved_by": approval.get("approved_by"),
            "approved_at": approval.get("approved_at"),
            "max_notional_dollars": approval.get("max_notional_dollars"),
        },
    }


def _entry_ledger_decision(order_status: str, filled_count_fp: Decimal | None) -> tuple[bool, str]:
    filled_fp = filled_count_fp or Decimal("0")
    normalized = order_status.strip().lower()
    if normalized in {"shadow_skipped", "kill_switch_blocked", "inactive_color_skipped", "write_credentials_missing"}:
        return False, "not_recorded"
    if normalized in {"cancelled", "canceled", "expired", "unfilled_cancelled"} and filled_fp <= Decimal("0"):
        return False, "entry_canceled_zero_fill"
    if (normalized.startswith("rejected") or normalized in {"failed", "error"}) and filled_fp <= Decimal("0"):
        return False, "entry_rejected_zero_fill"
    if normalized in {"filled", "executed"} or filled_fp > Decimal("0"):
        return True, "open"
    return True, "entry_submitted"


def _entry_terminal_time(entry: dict[str, Any], snapshot: CryptoMarketSnapshotRecord) -> datetime | None:
    values = [
        _datetime_from_any(entry.get("close_time")),
        _datetime_from_any(snapshot.close_time),
        _datetime_from_any(snapshot.expected_expiration_time),
    ]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _terminal_close_due(entry: dict[str, Any], snapshot: CryptoMarketSnapshotRecord, *, now: datetime) -> bool:
    result = str(snapshot.settlement_result or "").strip().lower()
    if result in {"yes", "no"}:
        return True
    status = str(snapshot.status or "").strip().lower()
    if status in {"closed", "settled", "finalized"}:
        return True
    terminal_time = _entry_terminal_time(entry, snapshot)
    return terminal_time is not None and now >= terminal_time


def _terminal_exit_yes_price(side: str, exit_side_price: Decimal) -> Decimal:
    if side == "yes":
        return exit_side_price
    return quantize_price(Decimal("1.0000") - exit_side_price)


def _mark_entry_terminal_closed(
    entry: dict[str, Any],
    *,
    snapshot: CryptoMarketSnapshotRecord,
    side: str,
    exit_side_price: Decimal,
    now: datetime,
    settings: Settings,
    trigger: str,
    order_status: str = "not_submitted_terminal_close",
    receipt: ExecReceiptPayload | None = None,
    exit_client_order_id: str | None = None,
) -> dict[str, Any]:
    count_fp = _decimal(entry.get("count_fp") or "0")
    entry_side = _decimal(entry.get("entry_side_price_dollars") or "0")
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    realized = _realized_pnl_without_exit_fee(
        entry_side_price=entry_side,
        exit_side_price=exit_side_price,
        count_fp=count_fp,
        fee_rate=fee_rate,
    )
    profit_pct = _net_profit_pct_from_realized(
        realized_pnl=realized,
        entry_side_price=entry_side,
        count_fp=count_fp,
        fee_rate=fee_rate,
    )
    exit_yes = _terminal_exit_yes_price(side, exit_side_price)
    entry.update(
        {
            "status": "closed",
            "closed_at": now.isoformat(),
            "exit_trigger": trigger,
            "exit_yes_price_dollars": _money_text(exit_yes),
            "exit_side_price_dollars": _money_text(exit_side_price),
            "exit_order_status": order_status,
            "settlement_result": snapshot.settlement_result,
            "realized_pnl_dollars": _money_text(realized),
            "net_profit_pct": str(profit_pct) if profit_pct is not None else None,
        }
    )
    if exit_client_order_id:
        entry["exit_client_order_id"] = exit_client_order_id
        entry["exit_submitted_at"] = now.isoformat()
    if receipt is not None:
        entry["exit_receipt"] = receipt.model_dump(mode="json")
    return {
        "status": "terminal_closed",
        "trigger": trigger,
        "exit_order_status": order_status,
        "exit_side_price_dollars": _money_text(exit_side_price),
        "realized_pnl_dollars": _money_text(realized),
        "net_profit_pct": str(profit_pct) if profit_pct is not None else None,
    }


def _exit_trigger_for_profit(
    profit_pct: Decimal,
    *,
    asset_symbol: str,
    frequency: str = BTC15M_TOUCH20_RULES_FREQ,
    settings: Settings,
    protection_trigger: str | None,
) -> str | None:
    cfg = _asset_settings(settings, asset_symbol, frequency=frequency)
    if profit_pct >= cfg.take_profit_pct:
        return "take_profit"
    if cfg.stop_loss_pct > Decimal("0") and profit_pct <= -cfg.stop_loss_pct:
        return "stop_loss"
    return protection_trigger


def profit_protection_review(
    entry: dict[str, Any],
    *,
    spot: dict[str, Any],
    net_profit: Decimal,
    settings: Settings,
    now: datetime,
) -> dict[str, Any]:
    cfg = _asset_settings(settings, entry.get("asset_symbol"), frequency=str(entry.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
    threshold = cfg.profit_protection_threshold_pct
    floor = cfg.profit_protection_floor_pct
    history = list(entry.get("quote_history") or [])
    previous_profit = _decimal(history[-1].get("net_profit_pct")) if history and isinstance(history[-1], dict) else None
    armed = bool(entry.get("profit_protection_armed")) or net_profit >= threshold
    max_profit = max(_decimal(entry.get("max_net_profit_pct") or "0"), net_profit)
    side = str(entry.get("side") or "yes")
    side_multiplier = Decimal("1") if side == "yes" else Decimal("-1")
    adverse_spot = (side_multiplier * _decimal(spot.get("return_1"))) < Decimal("0") and (side_multiplier * _decimal(spot.get("return_3"))) < Decimal("0")
    adverse_quote = previous_profit is not None and net_profit < previous_profit
    trigger = None
    if armed and net_profit <= floor:
        trigger = "profit_protection_floor"
    elif armed and adverse_quote and adverse_spot:
        trigger = "profit_protection_adverse_momentum"
    history.append(
        {
            "observed_at": now.isoformat(),
            "net_profit_pct": str(net_profit.quantize(Decimal("0.0001"))),
            "spot_return_1": str(_decimal(spot.get("return_1")).quantize(Decimal("0.0001"))),
            "spot_return_3": str(_decimal(spot.get("return_3")).quantize(Decimal("0.0001"))),
            "adverse_quote": adverse_quote,
            "adverse_spot": adverse_spot,
        }
    )
    return {
        "trigger": trigger,
        "review": {
            "armed": armed,
            "threshold_pct": str(threshold),
            "floor_pct": str(floor),
            "max_net_profit_pct": str(max_profit.quantize(Decimal("0.0001"))),
            "previous_net_profit_pct": str(previous_profit.quantize(Decimal("0.0001"))) if previous_profit is not None else None,
            "adverse_quote": adverse_quote,
            "adverse_spot": adverse_spot,
        },
        "entry_updates": {
            "profit_protection_armed": armed,
            "max_net_profit_pct": str(max_profit.quantize(Decimal("0.0001"))),
            "last_net_profit_pct": str(net_profit.quantize(Decimal("0.0001"))),
            "quote_history": history[-5:],
        },
    }


class CryptoNonModelTouch20Service:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        base_execution_service: ExecutionService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.base_execution_service = base_execution_service

    async def replay(
        self,
        *,
        frequency: str = "15m",
        asset_symbol: str = "BTC",
        days: int = 30,
        limit: int = 0,
        persist: bool = True,
        include_joined_fallback: bool = True,
    ) -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset, frequency=freq)
        backtest_artifact_type = _artifact_type(_artifact_base("backtest", asset, frequency=freq), frequency=freq, asset_symbol=asset)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        row_limit = limit or 200_000
        quote_path_kwargs: dict[str, Any] = {}
        if freq == "1h":
            fetch_min_seconds, fetch_min_market_age = _optimizer_replay_fetch_window(
                self.settings,
                asset_symbol=asset,
                frequency=freq,
            )
            quote_path_kwargs = {
                "entry_min_seconds_to_close": fetch_min_seconds,
                "entry_min_market_age_seconds": fetch_min_market_age,
                "entry_qualified_market_limit": _entry_qualified_market_limit(
                    self.settings,
                    frequency=freq,
                    row_limit=row_limit,
                ),
            }
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_crypto_settled_live_quote_path_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=cutoff,
                limit=row_limit,
                defer_payload=True,
                include_joined_fallback=include_joined_fallback,
                **quote_path_kwargs,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                provider="coinbase",
                asset_symbols=[asset],
                since=cutoff,
                limit=200_000,
                defer_payload=True,
            )
            await session.commit()
        replay = _evaluate_replay(snapshots, spot_rows, settings=self.settings, asset_symbol=asset, frequency=freq)
        metrics = dict(replay["metrics"])
        metrics["dataset_source"] = "settled_live_quote_paths"
        metrics["joined_quote_path_fallback_enabled"] = bool(include_joined_fallback)
        metrics["quote_path_selection"] = "entry_qualified_markets" if quote_path_kwargs else "newest_quote_rows"
        if quote_path_kwargs:
            metrics["entry_min_seconds_to_close_filter"] = quote_path_kwargs["entry_min_seconds_to_close"]
            metrics["entry_min_market_age_seconds_filter"] = quote_path_kwargs["entry_min_market_age_seconds"]
            metrics["entry_qualified_market_limit"] = quote_path_kwargs["entry_qualified_market_limit"]
        reasons = gate_reasons(metrics, settings=self.settings, asset_symbol=asset, frequency=freq)
        report = {
            "schema_version": "crypto-touch20-rules-backtest-v2",
            "status": "pass" if not reasons else "warn",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbol": asset,
            "objective": "touch_20pct_before_close",
            "strategy": strategy,
            "uses_trained_model": False,
            "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
            "days": days,
            "metrics": metrics,
            "requirements": _gate_requirements(self.settings, asset_symbol=asset, frequency=freq),
            "gate_reasons": reasons,
            "trade_sample": replay["trade_sample"],
            "trade_sample_count": len(replay["trade_sample"]),
            "trade_count": int(metrics.get("trade_candidate_count") or 0),
        }
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                artifact = await repo.record_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=backtest_artifact_type,
                    version=_version(f"crypto-touch20-rules-backtest-{freq}-{asset}", report),
                    status=report["status"],
                    sample_count=int(metrics.get("trade_candidate_count") or 0),
                    metrics=metrics,
                    payload=report,
                    kalshi_env=self.settings.kalshi_env,
                    trained_at=datetime.now(UTC),
                )
                await session.commit()
            report["version"] = artifact.version
        return report

    async def optimize(
        self,
        *,
        frequency: str = "15m",
        asset_symbol: str = "BTC",
        days: int = 30,
        limit: int = 0,
        top_n: int = 10,
        include_joined_fallback: bool = True,
        profile_names: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        row_limit = limit or 200_000
        quote_path_kwargs: dict[str, Any] = {}
        if freq == "1h":
            fetch_min_seconds, fetch_min_market_age = _optimizer_replay_fetch_window(
                self.settings,
                asset_symbol=asset,
                frequency=freq,
            )
            quote_path_kwargs = {
                "entry_min_seconds_to_close": fetch_min_seconds,
                "entry_min_market_age_seconds": fetch_min_market_age,
                "entry_qualified_market_limit": _entry_qualified_market_limit(
                    self.settings,
                    frequency=freq,
                    row_limit=row_limit,
                ),
            }
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_crypto_settled_live_quote_path_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=cutoff,
                limit=row_limit,
                defer_payload=True,
                include_joined_fallback=include_joined_fallback,
                **quote_path_kwargs,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                provider="coinbase",
                asset_symbols=[asset],
                since=cutoff,
                limit=200_000,
                defer_payload=True,
            )
            await session.commit()
        result = optimize_replay_profiles(
            snapshots,
            spot_rows,
            settings=self.settings,
            asset_symbol=asset,
            frequency=freq,
            top_n=top_n,
            profile_names=profile_names,
        )
        result.update(
            {
                "kalshi_env": self.settings.kalshi_env,
                "frequency": freq,
                "days": days,
                "sample_count": len(snapshots),
                "spot_row_count": len(spot_rows),
                "persisted": False,
                "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
            }
        )
        if quote_path_kwargs:
            result["quote_path_selection"] = "optimizer_entry_window"
            result["optimizer_entry_min_seconds_to_close_filter"] = quote_path_kwargs["entry_min_seconds_to_close"]
            result["optimizer_entry_min_market_age_seconds_filter"] = quote_path_kwargs["entry_min_market_age_seconds"]
        return result

    async def gate(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset, frequency=freq)
        backtest_artifact_type = _artifact_type(_artifact_base("backtest", asset, frequency=freq), frequency=freq, asset_symbol=asset)
        gate_artifact_type = _artifact_type(_artifact_base("gate", asset, frequency=freq), frequency=freq, asset_symbol=asset)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=backtest_artifact_type,
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict(getattr(backtest, "metrics", None) or {})
            if backtest is None:
                metrics["backtest_missing"] = True
            reasons = gate_reasons(metrics, settings=self.settings, asset_symbol=asset, frequency=freq)
            payload = {
                "passed": not reasons,
                "reasons": reasons,
                "requirements": _gate_requirements(self.settings, asset_symbol=asset, frequency=freq),
                "objective": "touch_20pct_before_close",
                "strategy": strategy,
                "uses_trained_model": False,
                "simulator_version": metrics.get("simulator_version"),
                "expected_simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
                "backtest_version": getattr(backtest, "version", None),
                "backtest_simulator_version": metrics.get("simulator_version"),
            }
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type=gate_artifact_type,
                version=_version(f"crypto-touch20-rules-gate-{freq}-{asset}", payload),
                status="passed" if payload["passed"] else "blocked",
                sample_count=int(metrics.get("allowed_trade_candidate_count") or _allowed_bucket_candidate_count(metrics)),
                metrics=metrics,
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            await session.commit()
        return {
            "status": artifact.status,
            "passed": payload["passed"],
            "reasons": reasons,
            "version": artifact.version,
            "artifact_type": artifact.artifact_type,
            "metrics": metrics,
        }

    async def approve(
        self,
        *,
        frequency: str = "15m",
        asset_symbol: str = "BTC",
        approved_by: str,
        note: str | None = None,
        max_notional_dollars: Decimal | None = None,
    ) -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset, frequency=freq)
        cfg = _asset_settings(self.settings, asset, frequency=freq)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(_artifact_base("gate", asset, frequency=freq), frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            if not _gate_passed(gate):
                await session.commit()
                return {
                    "status": "gate_not_passed",
                    "gate": _artifact_summary(gate),
                    "reason": f"latest {_strategy_label(asset, freq)} gate is missing or blocked",
                }
            payload = {
                "schema_version": "crypto-touch20-rules-approval-v1",
                "strategy": strategy,
                "kalshi_env": self.settings.kalshi_env,
                "asset_symbol": asset,
                "frequency": freq,
                "approved": True,
                "gate_version": gate.version,
                "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
                "approved_by": approved_by,
                "approved_at": datetime.now(UTC).isoformat(),
                "max_notional_dollars": _money_text(max_notional_dollars or cfg.max_open_notional_dollars),
                "note": note,
            }
            await repo.set_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq), gate.version, payload)
            await repo.log_ops_event(
                severity="info",
                source="crypto_non_model_touch20",
                summary=f"{_strategy_label(asset, freq)} approved for gate {gate.version}",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {"status": "approved", "approval": payload}

    async def revoke(self, *, frequency: str = "15m", asset_symbol: str = "BTC", note: str | None = None) -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            checkpoint = await repo.get_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq))
            payload = _approval_payload(checkpoint)
            payload.update(
                {
                    "approved": False,
                    "revoked_at": datetime.now(UTC).isoformat(),
                    "revoke_note": note,
                }
            )
            await repo.set_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq), None, payload)
            await repo.log_ops_event(
                severity="warning",
                source="crypto_non_model_touch20",
                summary=f"{_strategy_label(asset, freq)} approval revoked",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {"status": "revoked", "approval": payload}

    async def status(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": _strategy_code(asset, frequency=freq), "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset, frequency=freq)
        cfg = _asset_settings(self.settings, asset, frequency=freq)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(_artifact_base("gate", asset, frequency=freq), frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            approval = _approval_payload(await repo.get_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq)))
            ledger = _ledger_payload(
                await repo.get_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq)),
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=asset,
                frequency=freq,
            )
            await session.commit()
        approval_valid, approval_reason = _approval_valid(approval, gate)
        gate_metrics = dict(getattr(gate, "metrics", None) or {})
        live_bucket_controls = _live_bucket_controls(ledger, settings=self.settings, asset_symbol=asset, frequency=freq, gate_metrics=gate_metrics)
        daily_pnl = _daily_realized_pnl(ledger, datetime.now(UTC))
        return {
            "status": "ok",
            "strategy": strategy,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbol": asset,
            "enabled": cfg.rules_enabled,
            "trading_enabled": cfg.trading_enabled,
            "settings": {
                "allowed_sides": list(cfg.allowed_sides),
                "take_profit_pct": str(cfg.take_profit_pct),
                "stop_loss_pct": str(cfg.stop_loss_pct),
                "min_seconds_to_close": cfg.min_seconds_to_close,
                "max_open_notional_dollars": _money_text(cfg.max_open_notional_dollars),
                "daily_loss_limit_dollars": _money_text(cfg.daily_loss_limit_dollars),
                "min_order_notional_dollars": _money_text(cfg.min_order_notional_dollars),
                "max_bucket_live_loss_dollars": _money_text(cfg.max_bucket_live_loss_dollars),
                "max_bucket_consecutive_losses": cfg.max_bucket_consecutive_losses,
                "max_replay_stop_loss_rate": str(cfg.max_replay_stop_loss_rate),
                "max_replay_terminal_loss_rate": str(cfg.max_replay_terminal_loss_rate),
                "min_contract_price_dollars": _money_text(cfg.min_contract_price_dollars),
                "max_contract_price_dollars": _money_text(cfg.max_contract_price_dollars),
                "min_aligned_momentum": str(cfg.min_aligned_momentum),
                "min_rule_score": str(cfg.min_rule_score),
                "bucket_price_band_cents": cfg.bucket_price_band_cents,
                "bucket_spread_band_cents": cfg.bucket_spread_band_cents,
                "bucket_time_band_minutes": cfg.bucket_time_band_minutes,
                "quote_fresh_seconds": cfg.quote_fresh_seconds,
                "spot_fresh_seconds": cfg.spot_fresh_seconds,
            },
            "gate": _artifact_summary(gate),
            "gate_live_evidence": _gate_live_evidence(gate_metrics, live_bucket_controls),
            "approval": approval,
            "approval_valid": approval_valid,
            "approval_reason": approval_reason,
            "open_pending_notional_dollars": _money_text(_open_pending_notional(ledger)),
            "open_strategy_positions": len(_open_entries(ledger)),
            "daily_realized_pnl_dollars": _money_text(daily_pnl),
            "live_bucket_controls": live_bucket_controls,
        }

    async def run_once(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        strategy = _strategy_code(asset, frequency=freq)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": strategy, "frequency": freq, "asset_symbol": asset}
        cfg = _asset_settings(self.settings, asset, frequency=freq)
        if not cfg.rules_enabled:
            return {
                "status": "disabled",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "reason": f"{strategy} rules_enabled is false",
            }

        now = datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(_artifact_base("gate", asset, frequency=freq), frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            approval = _approval_payload(await repo.get_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq)))
            snapshots = await repo.list_latest_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                limit=20,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=now - timedelta(hours=2),
                limit=1000,
            )
            ledger = _ledger_payload(
                await repo.get_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq)),
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=asset,
                frequency=freq,
            )
            await session.commit()

        gate_summary = _artifact_summary(gate)
        gate_metrics = dict(getattr(gate, "metrics", None) or {})
        live_bucket_controls = _live_bucket_controls(ledger, settings=self.settings, asset_symbol=asset, frequency=freq, gate_metrics=gate_metrics)
        live_bucket_blocks = {
            str(bucket.get("bucket_key")): bucket
            for bucket in live_bucket_controls.get("buckets", [])
            if isinstance(bucket, dict) and bucket.get("blocked")
        }
        if control.active_color != self.settings.app_color:
            return {
                "status": "inactive_color",
                "strategy": strategy,
                "frequency": freq,
                "active_color": control.active_color,
                "app_color": self.settings.app_color,
                "gate": gate_summary,
                "live_bucket_controls": live_bucket_controls,
            }
        if control.kill_switch_enabled:
            return {"status": "kill_switch_enabled", "strategy": strategy, "frequency": freq, "gate": gate_summary, "live_bucket_controls": live_bucket_controls}
        if not _gate_passed(gate):
            return {
                "status": "gate_blocked",
                "strategy": strategy,
                "frequency": freq,
                "gate": gate_summary,
                "reason": f"{_strategy_label(asset, freq)} gate missing, blocked, or simulator-stale",
                "live_bucket_controls": live_bucket_controls,
            }
        approval_valid, approval_reason = _approval_valid(approval, gate)
        if not approval_valid:
            return {
                "status": "approval_blocked",
                "strategy": strategy,
                "frequency": freq,
                "gate": gate_summary,
                "approval": approval,
                "reason": approval_reason,
                "live_bucket_controls": live_bucket_controls,
            }

        daily_pnl = _daily_realized_pnl(ledger, now)
        daily_loss_limit = cfg.daily_loss_limit_dollars
        if daily_loss_limit > Decimal("0") and daily_pnl <= -daily_loss_limit:
            return {
                "status": "daily_loss_limit_blocked",
                "strategy": strategy,
                "frequency": freq,
                "daily_realized_pnl_dollars": _money_text(daily_pnl),
                "daily_loss_limit_dollars": _money_text(daily_loss_limit),
                "live_bucket_controls": live_bucket_controls,
            }

        funnel: Counter[str] = Counter()
        skipped: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        quote_fresh_seconds = max(1, cfg.quote_fresh_seconds)
        for snapshot in snapshots:
            funnel["market_seen"] += 1
            observed_at = _snapshot_decision_time(snapshot)
            if now - observed_at > timedelta(seconds=quote_fresh_seconds):
                skipped.append({"market_ticker": snapshot.market_ticker, "reason": "stale_quote_snapshot"})
                continue
            funnel["quote_fresh"] += 1
            market_exposure = _market_strategy_exposure(ledger, snapshot.market_ticker)
            if market_exposure:
                funnel["strategy_market_overlap_blocked"] += 1
                skipped.append(
                    {
                        "market_ticker": snapshot.market_ticker,
                        "reason": "strategy_market_overlap_blocked",
                        "strategy_exposure": market_exposure,
                    }
                )
                continue
            market_cooldown = _loss_cooldown_for_market(
                ledger,
                snapshot.market_ticker,
                now=now,
                cooldown_seconds=_frequency_interval_seconds(freq),
            )
            if market_cooldown:
                funnel["strategy_market_cooldown"] += 1
                skipped.append(
                    {
                        "market_ticker": snapshot.market_ticker,
                        "reason": "strategy_market_loss_cooldown",
                        "cooldown": market_cooldown,
                    }
                )
                continue
            spot = _spot_features(
                spot_rows,
                decision_ts=observed_at,
                freshness_reference=now,
                max_age_seconds=cfg.spot_fresh_seconds,
                asset_symbol=asset,
            )
            if spot.get("available"):
                funnel["spot_fresh"] += 1
            row_candidates = rules_candidates_for_snapshot(
                snapshot,
                settings=self.settings,
                spot=spot,
                gate_metrics=gate_metrics,
                require_allowed_bucket=True,
            )
            best = row_candidates[0] if row_candidates else {}
            selected = next((candidate for candidate in row_candidates if candidate.get("candidate_status") == "live_quality"), None)
            if selected is None:
                skipped.append({"market_ticker": snapshot.market_ticker, "reason": best.get("reason") or "no_live_candidate", "candidate_status": best.get("candidate_status")})
                continue
            bucket_key = str(selected.get("bucket_key") or "")
            bucket_block = live_bucket_blocks.get(bucket_key)
            if bucket_block:
                funnel["live_bucket_blocked"] += 1
                skipped.append(
                    {
                        "market_ticker": snapshot.market_ticker,
                        "reason": "live_bucket_blocked",
                        "bucket_key": bucket_key,
                        "bucket_block": bucket_block,
                    }
                )
                continue
            funnel["live_bucket_pass"] += 1
            funnel["selected"] += 1
            candidates.append({"market": snapshot, "candidate": selected})

        candidates.sort(
            key=lambda item: (
                _bucket_pnl_per_candidate((item.get("candidate") or {}).get("bucket") or {}),
                _decimal(((item.get("candidate") or {}).get("bucket") or {}).get("touch_rate") or "0"),
                _decimal((item.get("candidate") or {}).get("rule_score") or "0"),
                -_decimal((item.get("candidate") or {}).get("spread_dollars") or "999"),
                int((item.get("candidate") or {}).get("time_to_close_seconds") or 0),
            ),
            reverse=True,
        )
        open_pending_notional = _open_pending_notional(ledger)
        cap = cfg.max_open_notional_dollars
        approval_cap = _decimal(approval.get("max_notional_dollars"), cap)
        if approval_cap > Decimal("0"):
            cap = min(cap, approval_cap)
        remaining_cap = max(Decimal("0"), cap - open_pending_notional)
        if not candidates:
            result = {
                "status": "no_candidate",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "gate": gate_summary,
                "approval": approval,
                "funnel": dict(funnel),
                "skipped": skipped[:25],
                "open_pending_notional_dollars": _money_text(open_pending_notional),
                "live_bucket_controls": live_bucket_controls,
            }
            await self._log_cycle(result)
            return result
        if remaining_cap <= Decimal("0"):
            result = {
                "status": "strategy_cap_blocked",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "selected": _selection_summary(candidates[0]),
                "open_pending_notional_dollars": _money_text(open_pending_notional),
                "max_open_notional_dollars": _money_text(cap),
                "live_bucket_controls": live_bucket_controls,
            }
            await self._log_cycle(result)
            return result

        selected_item = candidates[0]
        market = selected_item["market"]
        selected = selected_item["candidate"]
        side_text = str(selected["side"])
        side = ContractSide(side_text)
        entry_side_price = _decimal(selected["execution_price_dollars"])
        count_fp = _count_for_cap(remaining_cap, entry_side_price)
        if count_fp is None:
            result = {
                "status": "strategy_cap_too_small",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "selected": _selection_summary(selected_item),
                "remaining_cap_dollars": _money_text(remaining_cap),
                "min_order_notional_dollars": _money_text(cfg.min_order_notional_dollars),
                "live_bucket_controls": live_bucket_controls,
            }
            await self._log_cycle(result)
            return result
        order_notional = (entry_side_price * count_fp).quantize(Decimal("0.0001"))
        if order_notional < cfg.min_order_notional_dollars:
            result = {
                "status": "min_order_notional_blocked",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "selected": _selection_summary(selected_item),
                "order_notional_dollars": _money_text(order_notional),
                "min_order_notional_dollars": _money_text(cfg.min_order_notional_dollars),
                "remaining_cap_dollars": _money_text(remaining_cap),
                "live_bucket_controls": live_bucket_controls,
            }
            await self._log_cycle(result)
            return result
        target_yes = quantize_price(selected["target_yes_price_dollars"])
        client_order_id = _client_order_id(
            "entry",
            market_ticker=market.market_ticker,
            side=side_text,
            now=now,
            asset_symbol=asset,
            frequency=freq,
        )
        if not cfg.trading_enabled:
            result = {
                "status": "trading_disabled",
                "strategy": strategy,
                "frequency": freq,
                "asset_symbol": asset,
                "selected": _selection_summary(selected_item),
                "client_order_id": client_order_id,
                "funnel": dict(funnel),
                "gate": gate_summary,
                "approval": approval,
                "no_order_submitted": True,
                "live_bucket_controls": live_bucket_controls,
            }
            await self._log_cycle(result)
            return result

        ticket = TradeTicket(
            market_ticker=market.market_ticker,
            action=TradeAction.BUY,
            side=side,
            yes_price_dollars=target_yes,
            count_fp=count_fp,
            capital_bucket=strategy,
            note=f"{strategy} isolated non-model entry",
        )
        room = Room(name=f"{_strategy_label(asset, freq)} {market.market_ticker}", market_ticker=market.market_ticker, kalshi_env=self.settings.kalshi_env, shadow_mode=False)
        receipt = await self.base_execution_service.execute(room=room, control=control, ticket=ticket, client_order_id=client_order_id, fair_yes_dollars=None)
        order_status = str(receipt.status or "")
        filled_count_fp = await self._filled_count_fp(receipt.external_order_id)
        filled_fp = filled_count_fp or Decimal("0")
        should_update_ledger, ledger_status = _entry_ledger_decision(order_status, filled_count_fp)
        ledger_count_fp = filled_fp if filled_fp > Decimal("0") else count_fp
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            if receipt.external_order_id or order_status not in {"shadow_skipped", "kill_switch_blocked", "inactive_color_skipped", "write_credentials_missing"}:
                await repo.save_order(
                    ticket_id=None,
                    client_order_id=client_order_id,
                    market_ticker=ticket.market_ticker,
                    status=order_status,
                    side=ticket.side.value,
                    action=ticket.action.value,
                    yes_price_dollars=ticket.yes_price_dollars,
                    count_fp=ticket.count_fp,
                    raw=receipt.details if isinstance(receipt.details, dict) else {},
                    kalshi_order_id=receipt.external_order_id,
                    kalshi_env=self.settings.kalshi_env,
                    strategy_code=strategy,
                )
            if should_update_ledger:
                ledger["positions"][client_order_id] = _entry_payload(
                    market=market,
                    candidate=selected,
                    count_fp=ledger_count_fp,
                    side=side_text,
                    target_yes=target_yes,
                    entry_side_price=entry_side_price,
                    status=ledger_status,
                    client_order_id=client_order_id,
                    receipt=receipt,
                    gate=gate,
                    approval=approval,
                    now=now,
                    settings=self.settings,
                )
                ledger["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq), None, ledger)
            await repo.log_ops_event(
                severity="info" if order_status in {"filled", "executed", "submitted"} else "warning",
                source="crypto_non_model_touch20",
                summary=f"{_strategy_label(asset, freq)} entry {order_status}: {market.market_ticker} {side_text}",
                payload={
                    "strategy": strategy,
                    "client_order_id": client_order_id,
                    "ledger_recorded": should_update_ledger,
                    "ledger_status": ledger_status,
                    "receipt": receipt.model_dump(mode="json"),
                    "selected": _selection_summary(selected_item),
                },
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {
            "status": order_status,
            "strategy": strategy,
            "frequency": freq,
            "asset_symbol": asset,
            "client_order_id": client_order_id,
            "external_order_id": receipt.external_order_id,
            "filled_count_fp": _count_text(filled_count_fp) if filled_count_fp is not None else None,
            "ledger_recorded": should_update_ledger,
            "ledger_status": ledger_status,
            "selected": _selection_summary(selected_item),
            "funnel": dict(funnel),
            "gate": gate_summary,
            "approval": approval,
            "live_bucket_controls": live_bucket_controls,
        }

    async def exit_once(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = _normalize_touch_frequency(frequency)
        asset = _normalize_asset_symbol(asset_symbol)
        strategy = _strategy_code(asset, frequency=freq)
        cfg = _asset_settings(self.settings, asset, frequency=freq)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": strategy, "frequency": freq, "asset_symbol": asset}
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            ledger = _ledger_payload(
                await repo.get_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq)),
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=asset,
                frequency=freq,
            )
            await session.commit()
        open_entries = _open_entries(ledger)
        if not open_entries:
            return {"status": "no_open_strategy_positions", "strategy": strategy, "frequency": freq, "asset_symbol": asset}
        if control.active_color != self.settings.app_color:
            return {"status": "inactive_color", "strategy": strategy, "frequency": freq, "active_color": control.active_color, "app_color": self.settings.app_color, "open_entries": len(open_entries)}

        evaluated: list[dict[str, Any]] = []
        exits: list[dict[str, Any]] = []
        ledger_dirty = False
        for client_order_id, entry in open_entries:
            market_ticker = str(entry.get("market_ticker") or "")
            retry_at_raw = entry.get("next_exit_retry_at")
            if retry_at_raw:
                try:
                    retry_at = datetime.fromisoformat(str(retry_at_raw))
                    retry_at = retry_at.replace(tzinfo=UTC) if retry_at.tzinfo is None else retry_at.astimezone(UTC)
                    if now < retry_at:
                        evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "exit_retry_cooldown", "next_exit_retry_at": retry_at.isoformat()})
                        continue
                except ValueError:
                    pass
            if str(entry.get("status") or "") == "exit_submitted":
                try:
                    submitted_at = datetime.fromisoformat(str(entry.get("exit_submitted_at")))
                    submitted_at = submitted_at.replace(tzinfo=UTC) if submitted_at.tzinfo is None else submitted_at.astimezone(UTC)
                    if now - submitted_at < timedelta(seconds=60):
                        evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "exit_already_submitted"})
                        continue
                except (TypeError, ValueError):
                    pass
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                snapshot = await repo.get_latest_crypto_market_snapshot(market_ticker, kalshi_env=self.settings.kalshi_env)
                spot_rows = await repo.list_crypto_spot_ohlc(frequency=freq, kalshi_env=self.settings.kalshi_env, asset_symbols=[asset], since=now - timedelta(hours=2), limit=1000)
                await session.commit()
            if snapshot is None:
                evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "snapshot_missing"})
                continue
            side = str(entry.get("side") or "")
            sell_yes = _sell_yes_price(snapshot, side)
            exit_side = _sell_side_price(snapshot, side)
            if sell_yes is None or exit_side is None:
                if _terminal_close_due(entry, snapshot, now=now):
                    terminal_exit_side = _terminal_side_exit_price(snapshot, side)
                    if terminal_exit_side is not None:
                        close_result = _mark_entry_terminal_closed(
                            entry,
                            snapshot=snapshot,
                            side=side,
                            exit_side_price=terminal_exit_side,
                            now=now,
                            settings=self.settings,
                            trigger="terminal_close_after_market_close",
                        )
                        ledger_dirty = True
                        evaluated.append(
                            {
                                "client_order_id": client_order_id,
                                "market_ticker": market_ticker,
                                **close_result,
                            }
                        )
                        exits.append(
                            {
                                "entry_client_order_id": client_order_id,
                                "exit_client_order_id": None,
                                "market_ticker": market_ticker,
                                **close_result,
                            }
                        )
                        continue
                evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "sell_quote_missing"})
                continue
            count_fp = _decimal(entry.get("count_fp") or "0")
            entry_side = _decimal(entry.get("entry_side_price_dollars") or "0")
            profit_pct = net_profit_pct(entry_side_price=entry_side, exit_side_price=exit_side, count_fp=count_fp, fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)))
            if profit_pct is None:
                evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "profit_unavailable"})
                continue
            spot = _spot_features(
                spot_rows,
                decision_ts=_snapshot_decision_time(snapshot),
                freshness_reference=now,
                max_age_seconds=cfg.spot_fresh_seconds,
                asset_symbol=asset,
            )
            protection = profit_protection_review(entry, spot=spot, net_profit=profit_pct, settings=self.settings, now=now)
            entry.update(protection["entry_updates"])
            ledger_dirty = True
            trigger = _exit_trigger_for_profit(
                profit_pct,
                asset_symbol=asset,
                frequency=freq,
                settings=self.settings,
                protection_trigger=protection["trigger"],
            )
            evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "evaluated", "net_profit_pct": str(profit_pct), "trigger": trigger, "profit_protection": protection["review"]})
            if trigger is None:
                continue
            exit_client_order_id = _client_order_id(
                "exit",
                market_ticker=market_ticker,
                side=side,
                now=now,
                asset_symbol=asset,
                frequency=freq,
            )
            receipt = await self.base_execution_service.close_position(
                market_ticker=market_ticker,
                side=side,
                count_fp=count_fp,
                yes_price_dollars=sell_yes,
                client_order_id=exit_client_order_id,
                kill_switch_enabled=bool(control.kill_switch_enabled),
                active_color=control.active_color,
                subaccount=self.settings.kalshi_subaccount,
                allow_risk_reducing_exit=True,
            )
            status = str(receipt.status or "")
            realized = _realized_pnl(entry_side_price=entry_side, exit_side_price=exit_side, count_fp=count_fp, fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)))
            terminal_close_result: dict[str, Any] | None = None
            entry.update(
                {
                    "exit_client_order_id": exit_client_order_id,
                    "exit_submitted_at": now.isoformat(),
                    "exit_trigger": trigger,
                    "exit_yes_price_dollars": _money_text(sell_yes),
                    "exit_side_price_dollars": _money_text(exit_side),
                    "exit_order_status": status,
                    "exit_receipt": receipt.model_dump(mode="json"),
                }
            )
            if status in {"filled", "executed"}:
                entry["status"] = "closed"
                entry["closed_at"] = now.isoformat()
                entry["realized_pnl_dollars"] = _money_text(realized)
                entry["net_profit_pct"] = str(profit_pct)
            elif status in {"cancelled", "canceled", "expired", "unfilled_cancelled"}:
                entry["status"] = "open"
                retry_delay = 10 if "stop_loss" in trigger else 60
                entry["next_exit_retry_at"] = (now + timedelta(seconds=retry_delay)).isoformat()
            elif status.startswith("rejected") and _terminal_close_due(entry, snapshot, now=now):
                terminal_close_result = _mark_entry_terminal_closed(
                    entry,
                    snapshot=snapshot,
                    side=side,
                    exit_side_price=exit_side,
                    now=now,
                    settings=self.settings,
                    trigger=f"{trigger}_after_rejected_terminal_exit",
                    order_status=status,
                    receipt=receipt,
                    exit_client_order_id=exit_client_order_id,
                )
            else:
                entry["status"] = "exit_submitted"
            ledger_dirty = True
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                if receipt.external_order_id or status not in {"shadow_skipped", "inactive_color_skipped", "write_credentials_missing"}:
                    await repo.save_order(
                        ticket_id=None,
                        client_order_id=exit_client_order_id,
                        market_ticker=market_ticker,
                        status=status,
                        side=side,
                        action="sell",
                        yes_price_dollars=sell_yes,
                        count_fp=count_fp,
                        raw=receipt.details if isinstance(receipt.details, dict) else {},
                        kalshi_order_id=receipt.external_order_id,
                        kalshi_env=self.settings.kalshi_env,
                        strategy_code=strategy,
                    )
                ledger["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq), None, ledger)
                await repo.log_ops_event(
                    severity="info" if status in {"filled", "executed", "submitted"} else "warning",
                    source="crypto_non_model_touch20",
                    summary=f"{_strategy_label(asset, freq)} exit {status}: {market_ticker} {side} {trigger}",
                    payload={"strategy": strategy, "entry_client_order_id": client_order_id, "exit_client_order_id": exit_client_order_id, "trigger": trigger, "net_profit_pct": str(profit_pct), "receipt": receipt.model_dump(mode="json")},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
            exits.append(
                {
                    "entry_client_order_id": client_order_id,
                    "exit_client_order_id": exit_client_order_id,
                    "market_ticker": market_ticker,
                    "trigger": trigger,
                    "status": (terminal_close_result or {}).get("status") or status,
                    "net_profit_pct": (terminal_close_result or {}).get("net_profit_pct") or str(profit_pct),
                }
            )
        if ledger_dirty:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                ledger["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq), None, ledger)
                await session.commit()
        return {"status": "ok", "strategy": strategy, "frequency": freq, "asset_symbol": asset, "evaluated": evaluated, "exits": exits}

    async def _log_cycle(self, result: dict[str, Any]) -> None:
        try:
            asset = str(result.get("asset_symbol") or "").upper() or "UNKNOWN"
            freq = _normalize_touch_frequency(str(result.get("frequency") or BTC15M_TOUCH20_RULES_FREQ))
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.log_ops_event(
                    severity="info" if result.get("status") in {"trading_disabled", "no_candidate"} else "warning",
                    source="crypto_non_model_touch20",
                    summary=f"{_strategy_label(asset, freq)} cycle: {result.get('status')}",
                    payload=result,
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
        except Exception:
            logger.warning("failed to log touch20 rules cycle telemetry", exc_info=True)

    async def _filled_count_fp(self, external_order_id: str | None) -> Decimal | None:
        if not external_order_id:
            return None
        try:
            return await self.base_execution_service._get_filled_fp(external_order_id)
        except Exception:
            logger.warning("failed to fetch filled count for touch20 order %s", external_order_id, exc_info=True)
            return None


__all__ = [
    "BTC15M_TOUCH20_RULES_ORDER_PREFIX",
    "BTC15M_TOUCH20_RULES_STRATEGY",
    "CryptoNonModelTouch20Service",
    "gate_reasons",
    "net_profit_pct",
    "profit_protection_review",
    "rules_candidates_for_snapshot",
]
