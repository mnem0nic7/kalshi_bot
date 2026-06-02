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
from typing import Any

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
TOUCH20_RULES_SUPPORTED_ASSETS = frozenset({"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"})
TOUCH20_RULES_REPLAY_SIMULATOR_VERSION = "live_exit_v2"
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
    min_aligned_momentum: Decimal
    min_rule_score: Decimal
    quote_fresh_seconds: int
    spot_fresh_seconds: int


def _version(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{digest}"


def _normalize_asset_symbol(asset_symbol: str | None) -> str:
    return "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())


def _configured_assets(settings: Settings) -> list[str]:
    raw = str(getattr(settings, "crypto_15m_touch20_rules_assets", "") or "").replace(";", ",")
    assets = [_normalize_asset_symbol(item) for item in raw.split(",") if _normalize_asset_symbol(item)]
    return assets or [BTC15M_TOUCH20_RULES_ASSET]


def _strategy_code(asset_symbol: str | None) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    if asset == BTC15M_TOUCH20_RULES_ASSET:
        return BTC15M_TOUCH20_RULES_STRATEGY
    return f"{asset.lower()}15m_touch20_rules"


def _order_prefix(asset_symbol: str | None) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    if asset == BTC15M_TOUCH20_RULES_ASSET:
        return BTC15M_TOUCH20_RULES_ORDER_PREFIX
    return f"{asset.lower()}15t20r"


def _artifact_base(kind: str, asset_symbol: str | None) -> str:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    if asset == BTC15M_TOUCH20_RULES_ASSET:
        return BTC15M_TOUCH20_RULES_GATE_ARTIFACT if kind == "gate" else BTC15M_TOUCH20_RULES_BACKTEST_ARTIFACT
    return f"{_strategy_code(asset)}_{kind}"


def _asset_overrides(settings: Settings, asset_symbol: str | None) -> dict[str, Any]:
    raw = getattr(settings, "crypto_15m_touch20_asset_settings", {}) or {}
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


def _asset_settings(settings: Settings, asset_symbol: str | None) -> Touch20AssetSettings:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    overrides = _asset_overrides(settings, asset)
    is_btc = asset == BTC15M_TOUCH20_RULES_ASSET
    enabled_default = bool(settings.crypto_btc15m_touch20_rules_enabled) if is_btc else False
    trading_default = bool(settings.crypto_btc15m_touch20_rules_trading_enabled) if is_btc else False
    return Touch20AssetSettings(
        rules_enabled=_bool_override(overrides, "rules_enabled", enabled_default),
        trading_enabled=_bool_override(overrides, "trading_enabled", trading_default),
        allowed_sides=_side_tuple_override(overrides, "allowed_sides", str(settings.crypto_btc15m_touch20_allowed_sides)),
        take_profit_pct=_decimal_override(overrides, "take_profit_pct", Decimal(str(settings.crypto_btc15m_touch20_take_profit_pct))),
        stop_loss_pct=_decimal_override(overrides, "stop_loss_pct", Decimal(str(settings.crypto_btc15m_touch20_stop_loss_pct))),
        min_market_age_seconds=_int_override(overrides, "min_market_age_seconds", int(settings.crypto_btc15m_touch20_min_market_age_seconds)),
        min_seconds_to_close=_int_override(overrides, "min_seconds_to_close", int(settings.crypto_btc15m_touch20_min_seconds_to_close)),
        replay_min_candidates=_int_override(overrides, "replay_min_candidates", int(settings.crypto_btc15m_touch20_replay_min_candidates)),
        replay_min_touch_rate=_decimal_override(overrides, "replay_min_touch_rate", Decimal(str(settings.crypto_btc15m_touch20_replay_min_touch_rate))),
        replay_min_net_pnl_dollars=_decimal_override(overrides, "replay_min_net_pnl_dollars", Decimal(str(settings.crypto_btc15m_touch20_replay_min_net_pnl_dollars))),
        replay_min_pnl_per_candidate_dollars=_decimal_override(overrides, "replay_min_pnl_per_candidate_dollars", Decimal(str(settings.crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars))),
        replay_max_hard_cap_breaches=_int_override(overrides, "replay_max_hard_cap_breaches", int(settings.crypto_btc15m_touch20_replay_max_hard_cap_breaches)),
        max_open_notional_dollars=_decimal_override(overrides, "max_open_notional_dollars", Decimal(str(settings.crypto_btc15m_touch20_max_open_notional_dollars))),
        daily_loss_limit_dollars=_decimal_override(overrides, "daily_loss_limit_dollars", Decimal(str(settings.crypto_btc15m_touch20_daily_loss_limit_dollars))),
        min_order_notional_dollars=_decimal_override(overrides, "min_order_notional_dollars", Decimal(str(settings.crypto_btc15m_touch20_min_order_notional_dollars))),
        max_bucket_live_loss_dollars=_decimal_override(overrides, "max_bucket_live_loss_dollars", Decimal(str(settings.crypto_btc15m_touch20_max_bucket_live_loss_dollars))),
        max_bucket_consecutive_losses=_int_override(overrides, "max_bucket_consecutive_losses", int(settings.crypto_btc15m_touch20_max_bucket_consecutive_losses)),
        max_replay_stop_loss_rate=_decimal_override(overrides, "max_replay_stop_loss_rate", Decimal(str(settings.crypto_btc15m_touch20_max_replay_stop_loss_rate))),
        max_replay_terminal_loss_rate=_decimal_override(overrides, "max_replay_terminal_loss_rate", Decimal(str(settings.crypto_btc15m_touch20_max_replay_terminal_loss_rate))),
        profit_protection_threshold_pct=_decimal_override(overrides, "profit_protection_threshold_pct", Decimal(str(settings.crypto_btc15m_touch20_profit_protection_threshold_pct))),
        profit_protection_floor_pct=_decimal_override(overrides, "profit_protection_floor_pct", Decimal(str(settings.crypto_btc15m_touch20_profit_protection_floor_pct))),
        loop_interval_seconds=_int_override(overrides, "loop_interval_seconds", int(settings.crypto_btc15m_touch20_loop_interval_seconds)),
        min_contract_price_dollars=_decimal_override(overrides, "min_contract_price_dollars", Decimal(str(settings.crypto_btc15m_touch20_min_contract_price_dollars))),
        max_contract_price_dollars=_decimal_override(overrides, "max_contract_price_dollars", Decimal(str(settings.crypto_btc15m_touch20_max_contract_price_dollars))),
        min_aligned_momentum=_decimal_override(overrides, "min_aligned_momentum", Decimal(str(settings.crypto_btc15m_touch20_min_aligned_momentum))),
        min_rule_score=_decimal_override(overrides, "min_rule_score", Decimal(str(settings.crypto_btc15m_touch20_min_rule_score))),
        quote_fresh_seconds=_int_override(overrides, "quote_fresh_seconds", int(settings.crypto_btc15m_touch20_quote_fresh_seconds)),
        spot_fresh_seconds=_int_override(overrides, "spot_fresh_seconds", int(settings.crypto_btc15m_touch20_spot_fresh_seconds)),
    )


def _artifact_type(base: str, *, frequency: str = BTC15M_TOUCH20_RULES_FREQ, asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET) -> str:
    freq = normalize_frequency(frequency) or BTC15M_TOUCH20_RULES_FREQ
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    return f"{base}:{freq}:{asset}"


def _approval_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    asset = _normalize_asset_symbol(asset_symbol)
    return f"{_strategy_code(asset)}_approval:{kalshi_env}:{asset}:{normalize_frequency(frequency) or frequency}"


def _ledger_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    asset = _normalize_asset_symbol(asset_symbol)
    return f"{_strategy_code(asset)}:{kalshi_env}:{asset}:{normalize_frequency(frequency) or frequency}"


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
    return (normalize_frequency(frequency) or frequency) == BTC15M_TOUCH20_RULES_FREQ and _normalize_asset_symbol(asset_symbol) in TOUCH20_RULES_SUPPORTED_ASSETS


def _snapshot_decision_time(snapshot: CryptoMarketSnapshotRecord) -> datetime:
    return _as_utc(snapshot.observed_at) or datetime.now(UTC)


def _market_timing(snapshot: CryptoMarketSnapshotRecord, decision_ts: datetime) -> dict[str, int | None]:
    close_ts = _as_utc(snapshot.close_time or snapshot.expected_expiration_time)
    open_ts = _as_utc(snapshot.open_time)
    time_to_close = int((close_ts - decision_ts).total_seconds()) if close_ts is not None else None
    market_age = int((decision_ts - open_ts).total_seconds()) if open_ts is not None else None
    if market_age is None and time_to_close is not None and time_to_close <= BTC15M_TOUCH20_RULES_INTERVAL_SECONDS:
        market_age = max(0, BTC15M_TOUCH20_RULES_INTERVAL_SECONDS - time_to_close)
    if time_to_close is None and market_age is not None and market_age <= BTC15M_TOUCH20_RULES_INTERVAL_SECONDS:
        time_to_close = max(0, BTC15M_TOUCH20_RULES_INTERVAL_SECONDS - market_age)
    return {
        "market_age_seconds": market_age,
        "time_to_close_seconds": time_to_close,
    }


def _side_entry_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    raw = snapshot.yes_ask_dollars if side == "yes" else snapshot.no_ask_dollars
    if raw is None:
        return None
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return None
    return quantize_price(price)


def _side_bid_price(snapshot: CryptoMarketSnapshotRecord, side: str) -> Decimal | None:
    raw = snapshot.yes_bid_dollars if side == "yes" else snapshot.no_bid_dollars
    if raw is None:
        return None
    price = _decimal(raw)
    if price <= Decimal("0") or price >= Decimal("1"):
        return None
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


def _max_spread_for_price(entry_price: Decimal) -> Decimal:
    return Decimal("0.0100") if entry_price < Decimal("0.2000") else Decimal("0.0200")


def _price_band(price: Decimal) -> str:
    cents = int((price * Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    if cents < 10:
        return "under_10c"
    if cents >= 90:
        return "90c_plus"
    low = (cents // 10) * 10
    return f"{low}_{low + 10}c"


def _spread_band(spread: Decimal | None) -> str:
    if spread is None:
        return "unknown_spread"
    if spread <= Decimal("0.0100"):
        return "le_1c"
    if spread <= Decimal("0.0200"):
        return "le_2c"
    return "gt_2c"


def _time_bucket(time_to_close_seconds: int | None) -> str:
    if time_to_close_seconds is None:
        return "unknown_time"
    if time_to_close_seconds < 300:
        return "0_5m"
    if time_to_close_seconds < 600:
        return "5_10m"
    if time_to_close_seconds <= 900:
        return "10_15m"
    return "15m_plus"


def _bucket_key(
    *,
    asset_symbol: str,
    side: str,
    entry_price: Decimal,
    spread: Decimal | None,
    time_to_close_seconds: int | None,
) -> str:
    return "|".join(
        [
            _normalize_asset_symbol(asset_symbol),
            side,
            _price_band(entry_price),
            _spread_band(spread),
            _time_bucket(time_to_close_seconds),
        ]
    )


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
    cfg = _asset_settings(settings, snapshot.asset_symbol)
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
        max_spread = _max_spread_for_price(entry) if entry is not None else Decimal("0")
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
            reason = "missing_real_bid_ask"
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
    cfg = _asset_settings(settings, asset)
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
                max_age_seconds=max(cfg.spot_fresh_seconds, BTC15M_TOUCH20_RULES_INTERVAL_SECONDS),
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


def _bucket_matrix(trades: list[dict[str, Any]], *, settings: Settings, asset_symbol: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        simulation = trade.get("simulation") if isinstance(trade.get("simulation"), dict) else {}
        key = str(simulation.get("bucket_key") or "")
        if key:
            grouped[key].append(trade)
    matrix: list[dict[str, Any]] = []
    asset = _normalize_asset_symbol(asset_symbol)
    cfg = _asset_settings(settings, asset)
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


def _gate_requirements(settings: Settings, *, asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET) -> dict[str, Any]:
    cfg = _asset_settings(settings, asset_symbol)
    return {
        "asset_symbol": _normalize_asset_symbol(asset_symbol),
        "entry_replay_mode": "first_eligible_per_market",
        "allowed_sides": list(cfg.allowed_sides),
        "min_seconds_to_close": cfg.min_seconds_to_close,
        "min_contract_price_dollars": float(cfg.min_contract_price_dollars),
        "max_contract_price_dollars": float(cfg.max_contract_price_dollars),
        "min_aligned_momentum": float(cfg.min_aligned_momentum),
        "min_rule_score": float(cfg.min_rule_score),
        "min_trade_candidates": cfg.replay_min_candidates,
        "min_net_pl_dollars": float(cfg.replay_min_net_pnl_dollars),
        "min_pnl_per_candidate_dollars": float(cfg.replay_min_pnl_per_candidate_dollars),
        "max_hard_cap_breaches": cfg.replay_max_hard_cap_breaches,
        "min_touch_rate": float(cfg.replay_min_touch_rate),
        "max_stop_loss_rate": float(cfg.max_replay_stop_loss_rate),
        "max_terminal_loss_rate": float(cfg.max_replay_terminal_loss_rate),
        "requires_allowed_bucket_support": True,
        "requires_real_quote_path_evidence": True,
        "uses_trained_model": False,
        "simulator_version": TOUCH20_RULES_REPLAY_SIMULATOR_VERSION,
    }


def gate_reasons(metrics: dict[str, Any], *, settings: Settings, asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET) -> list[str]:
    asset = _normalize_asset_symbol(asset_symbol)
    cfg = _asset_settings(settings, asset)
    label = f"{asset} 15m touch20 rules"
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
    candidates = int(metrics.get("trade_candidate_count") or 0)
    min_candidates = cfg.replay_min_candidates
    net_pl = Decimal(str(metrics.get("net_simulated_pl_dollars") or "0"))
    min_net = cfg.replay_min_net_pnl_dollars
    pnl_per = Decimal(str(metrics.get("pnl_per_candidate_dollars") or "0"))
    min_pnl_per = cfg.replay_min_pnl_per_candidate_dollars
    hard_cap_breaches = int(metrics.get("hard_cap_breaches") or 0)
    max_hard_cap = cfg.replay_max_hard_cap_breaches
    touch_rate = Decimal(str(metrics.get("touch_rate") or "0"))
    min_touch_rate = cfg.replay_min_touch_rate
    stop_loss_rate = Decimal(str(metrics.get("stop_loss_rate") or "0"))
    terminal_loss_rate = Decimal(str(metrics.get("terminal_loss_rate") or "0"))
    simulator_version = str(metrics.get("simulator_version") or "")
    if candidates < min_candidates:
        reasons.append(f"{label} replay candidate count {candidates} below minimum {min_candidates}.")
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
    for bucket in metrics.get("bucket_matrix") or []:
        if not isinstance(bucket, dict):
            continue
        key = str(bucket.get("bucket_key") or "unknown")
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
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    cfg = _asset_settings(settings, asset)
    scoped_rows = [
        row
        for row in snapshots
        if _normalize_asset_symbol(row.asset_symbol) == asset
        and (normalize_frequency(row.frequency) or row.frequency) == BTC15M_TOUCH20_RULES_FREQ
        and str(row.settlement_result or "").lower() in {"yes", "no"}
        and all(
            value is not None
            for value in (row.yes_bid_dollars, row.yes_ask_dollars, row.no_bid_dollars, row.no_ask_dollars)
        )
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
                max_age_seconds=max(cfg.spot_fresh_seconds, BTC15M_TOUCH20_RULES_INTERVAL_SECONDS),
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
    bucket_matrix = _bucket_matrix(trades, settings=settings, asset_symbol=asset)
    allowed_keys = [bucket["bucket_key"] for bucket in bucket_matrix if bucket.get("allowed")]
    blocked_keys = [bucket["bucket_key"] for bucket in bucket_matrix if not bucket.get("allowed")]
    values = [_decimal((trade.get("simulation") or {}).get("net_pnl")) for trade in trades]
    fees = [_decimal((trade.get("simulation") or {}).get("fees")) for trade in trades]
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
    trade_count = len(trades)
    metrics = {
        "objective": "touch_20pct_before_close",
        "strategy": _strategy_code(asset),
        "uses_trained_model": False,
        "asset_symbols": [asset],
        "sample_count": len(scoped_rows),
        "real_quote_path_row_count": len(scoped_rows),
        "entry_replay_mode": "first_eligible_per_market",
        "trade_candidate_count": trade_count,
        "touch_count": touch_count,
        "touch_rate": float(Decimal(touch_count) / Decimal(trade_count)) if trade_count else 0.0,
        "stop_loss_count": stop_loss_count,
        "stop_loss_rate": float(Decimal(stop_loss_count) / Decimal(trade_count)) if trade_count else 0.0,
        "terminal_loss_count": terminal_loss_count,
        "terminal_loss_rate": float(Decimal(terminal_loss_count) / Decimal(trade_count)) if trade_count else 0.0,
        "settlement_hold_count": int(exit_reason_counts.get("terminal_close", 0)),
        "exit_reason_counts": dict(exit_reason_counts),
        "gross_simulated_pl_dollars": float(sum((_decimal((trade.get("simulation") or {}).get("gross_pnl")) for trade in trades), Decimal("0"))),
        "net_simulated_pl_dollars": float(net),
        "pnl_per_candidate_dollars": float(net / Decimal(trade_count)) if trade_count else 0.0,
        "fees_dollars": float(sum(fees, Decimal("0"))),
        "max_trade_drawdown_pct": float(
            min((_decimal((trade.get("simulation") or {}).get("max_drawdown_pct")) for trade in trades), default=Decimal("0"))
        ),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
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


def _artifact_summary(artifact: Any | None) -> dict[str, Any] | None:
    if artifact is None:
        return None
    payload = getattr(artifact, "payload", None)
    return {
        "artifact_type": getattr(artifact, "artifact_type", None),
        "version": getattr(artifact, "version", None),
        "status": getattr(artifact, "status", None),
        "sample_count": getattr(artifact, "sample_count", None),
        "passed": payload.get("passed") if isinstance(payload, dict) else None,
        "simulator_version": payload.get("simulator_version") if isinstance(payload, dict) else None,
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
    return {
        "schema_version": "crypto15m-touch20-rules-ledger-v2",
        "strategy": _strategy_code(asset_symbol),
        "kalshi_env": kalshi_env,
        "asset_symbol": _normalize_asset_symbol(asset_symbol),
        "frequency": normalize_frequency(frequency) or frequency,
        "positions": dict(positions),
        "updated_at": payload.get("updated_at"),
    }


def _open_entries(ledger: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""))
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:"):
            continue
        if isinstance(entry, dict) and str(entry.get("status") or "") in {"open", "exit_submitted"}:
            entries.append((str(client_order_id), entry))
    return entries


def _open_pending_notional(ledger: dict[str, Any]) -> Decimal:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    total = Decimal("0")
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""))
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
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""))
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
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""))
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
    prefix = _order_prefix(str(ledger.get("asset_symbol") or ""))
    latest_loss: tuple[datetime, str, dict[str, Any]] | None = None
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{prefix}:") or not isinstance(entry, dict):
            continue
        if str(entry.get("market_ticker") or "") != market_ticker:
            continue
        trigger = str(entry.get("exit_trigger") or "")
        if "stop_loss" not in trigger and "terminal" not in trigger:
            continue
        if _decimal(entry.get("realized_pnl_dollars") or "0") >= Decimal("0"):
            continue
        closed_at = _datetime_from_any(entry.get("closed_at"))
        if closed_at is None:
            continue
        if latest_loss is None or closed_at > latest_loss[0]:
            latest_loss = (closed_at, str(client_order_id), entry)
    if latest_loss is None:
        return None
    cooldown_until = latest_loss[0] + timedelta(seconds=cooldown_seconds)
    if now >= cooldown_until:
        return None
    return {
        "client_order_id": latest_loss[1],
        "exit_trigger": latest_loss[2].get("exit_trigger"),
        "realized_pnl_dollars": latest_loss[2].get("realized_pnl_dollars"),
        "closed_at": latest_loss[0].isoformat(),
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
    gate_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset = _normalize_asset_symbol(asset_symbol)
    cfg = _asset_settings(settings, asset)
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    prefix = _order_prefix(str(ledger.get("asset_symbol") or asset))
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

    seeded_bucket_keys = set(TOUCH20_RULES_REMEDIATION_BLOCKED_BTC_BUCKETS if asset == BTC15M_TOUCH20_RULES_ASSET else frozenset())
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


def _client_order_id(action: str, *, market_ticker: str, side: str, now: datetime, asset_symbol: str) -> str:
    strategy = _strategy_code(asset_symbol)
    basis = f"{strategy}:{action}:{market_ticker}:{side}:{now.isoformat()}".encode("utf-8")
    digest = hashlib.blake2b(basis, digest_size=10).hexdigest()
    return f"{_order_prefix(asset_symbol)}:{action[:1]}:{digest}"


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
    cfg = _asset_settings(settings, asset)
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    return {
        "status": status,
        "strategy": _strategy_code(asset),
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
    settings: Settings,
    protection_trigger: str | None,
) -> str | None:
    cfg = _asset_settings(settings, asset_symbol)
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
    cfg = _asset_settings(settings, entry.get("asset_symbol"))
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

    async def replay(self, *, frequency: str = "15m", asset_symbol: str = "BTC", days: int = 30, limit: int = 0, persist: bool = True) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset)
        backtest_artifact_type = _artifact_type(_artifact_base("backtest", asset), frequency=freq, asset_symbol=asset)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_crypto_settled_live_quote_path_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=cutoff,
                limit=limit or 200_000,
                defer_payload=True,
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
        replay = _evaluate_replay(snapshots, spot_rows, settings=self.settings, asset_symbol=asset)
        metrics = dict(replay["metrics"])
        metrics["dataset_source"] = "settled_live_quote_paths"
        reasons = gate_reasons(metrics, settings=self.settings, asset_symbol=asset)
        report = {
            "schema_version": "btc15m-touch20-rules-backtest-v2",
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
            "requirements": _gate_requirements(self.settings, asset_symbol=asset),
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
                    version=_version(f"btc15m-touch20-rules-backtest-{freq}-{asset}", report),
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

    async def gate(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset)
        backtest_artifact_type = _artifact_type(_artifact_base("backtest", asset), frequency=freq, asset_symbol=asset)
        gate_artifact_type = _artifact_type(_artifact_base("gate", asset), frequency=freq, asset_symbol=asset)
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
            reasons = gate_reasons(metrics, settings=self.settings, asset_symbol=asset)
            payload = {
                "passed": not reasons,
                "reasons": reasons,
                "requirements": _gate_requirements(self.settings, asset_symbol=asset),
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
                version=_version(f"btc15m-touch20-rules-gate-{freq}-{asset}", payload),
                status="passed" if payload["passed"] else "blocked",
                sample_count=int(metrics.get("trade_candidate_count") or 0),
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
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset)
        cfg = _asset_settings(self.settings, asset)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(_artifact_base("gate", asset), frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            if not _gate_passed(gate):
                await session.commit()
                return {
                    "status": "gate_not_passed",
                    "gate": _artifact_summary(gate),
                    "reason": f"latest {asset} 15m touch20 rules gate is missing or blocked",
                }
            payload = {
                "schema_version": "btc15m-touch20-rules-approval-v1",
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
                source="crypto_non_model_btc15m_touch20",
                summary=f"{asset} 15m touch20 rules approved for gate {gate.version}",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {"status": "approved", "approval": payload}

    async def revoke(self, *, frequency: str = "15m", asset_symbol: str = "BTC", note: str | None = None) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
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
                source="crypto_non_model_btc15m_touch20",
                summary=f"{asset} 15m touch20 rules approval revoked",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {"status": "revoked", "approval": payload}

    async def status(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": _strategy_code(asset), "frequency": freq, "asset_symbol": asset}
        strategy = _strategy_code(asset)
        cfg = _asset_settings(self.settings, asset)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(_artifact_base("gate", asset), frequency=freq, asset_symbol=asset),
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
        live_bucket_controls = _live_bucket_controls(ledger, settings=self.settings, asset_symbol=asset, gate_metrics=gate_metrics)
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
                "quote_fresh_seconds": cfg.quote_fresh_seconds,
                "spot_fresh_seconds": cfg.spot_fresh_seconds,
            },
            "gate": _artifact_summary(gate),
            "approval": approval,
            "approval_valid": approval_valid,
            "approval_reason": approval_reason,
            "open_pending_notional_dollars": _money_text(_open_pending_notional(ledger)),
            "open_strategy_positions": len(_open_entries(ledger)),
            "daily_realized_pnl_dollars": _money_text(daily_pnl),
            "live_bucket_controls": live_bucket_controls,
        }

    async def run_once(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        strategy = _strategy_code(asset)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": strategy, "frequency": freq, "asset_symbol": asset}
        cfg = _asset_settings(self.settings, asset)
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
                artifact_type=_artifact_type(_artifact_base("gate", asset), frequency=freq, asset_symbol=asset),
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
        live_bucket_controls = _live_bucket_controls(ledger, settings=self.settings, asset_symbol=asset, gate_metrics=gate_metrics)
        live_bucket_blocks = {
            str(bucket.get("bucket_key")): bucket
            for bucket in live_bucket_controls.get("buckets", [])
            if isinstance(bucket, dict) and bucket.get("blocked")
        }
        if control.active_color != self.settings.app_color:
            return {
                "status": "inactive_color",
                "strategy": strategy,
                "active_color": control.active_color,
                "app_color": self.settings.app_color,
                "gate": gate_summary,
                "live_bucket_controls": live_bucket_controls,
            }
        if control.kill_switch_enabled:
            return {"status": "kill_switch_enabled", "strategy": strategy, "gate": gate_summary, "live_bucket_controls": live_bucket_controls}
        if not _gate_passed(gate):
            return {
                "status": "gate_blocked",
                "strategy": strategy,
                "gate": gate_summary,
                "reason": f"{asset} 15m touch20 rules gate missing, blocked, or simulator-stale",
                "live_bucket_controls": live_bucket_controls,
            }
        approval_valid, approval_reason = _approval_valid(approval, gate)
        if not approval_valid:
            return {
                "status": "approval_blocked",
                "strategy": strategy,
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
            market_cooldown = _loss_cooldown_for_market(ledger, snapshot.market_ticker, now=now)
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
        client_order_id = _client_order_id("entry", market_ticker=market.market_ticker, side=side_text, now=now, asset_symbol=asset)
        if not cfg.trading_enabled:
            result = {
                "status": "trading_disabled",
                "strategy": strategy,
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
        room = Room(name=f"{asset} 15m touch20 rules {market.market_ticker}", market_ticker=market.market_ticker, kalshi_env=self.settings.kalshi_env, shadow_mode=False)
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
                source="crypto_non_model_btc15m_touch20",
                summary=f"{asset} 15m touch20 rules entry {order_status}: {market.market_ticker} {side_text}",
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
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        strategy = _strategy_code(asset)
        cfg = _asset_settings(self.settings, asset)
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
            return {"status": "inactive_color", "strategy": strategy, "active_color": control.active_color, "app_color": self.settings.app_color, "open_entries": len(open_entries)}

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
                settings=self.settings,
                protection_trigger=protection["trigger"],
            )
            evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "evaluated", "net_profit_pct": str(profit_pct), "trigger": trigger, "profit_protection": protection["review"]})
            if trigger is None:
                continue
            exit_client_order_id = _client_order_id("exit", market_ticker=market_ticker, side=side, now=now, asset_symbol=asset)
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
                entry["next_exit_retry_at"] = (now + timedelta(seconds=60)).isoformat()
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
                    source="crypto_non_model_btc15m_touch20",
                    summary=f"{asset} 15m touch20 rules exit {status}: {market_ticker} {side} {trigger}",
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
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.log_ops_event(
                    severity="info" if result.get("status") in {"trading_disabled", "no_candidate"} else "warning",
                    source="crypto_non_model_btc15m_touch20",
                    summary=f"{asset} 15m touch20 rules cycle: {result.get('status')}",
                    payload=result,
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
        except Exception:
            logger.warning("failed to log 15m touch20 rules cycle telemetry", exc_info=True)

    async def _filled_count_fp(self, external_order_id: str | None) -> Decimal | None:
        if not external_order_id:
            return None
        try:
            return await self.base_execution_service._get_filled_fp(external_order_id)
        except Exception:
            logger.warning("failed to fetch filled count for 15m touch20 order %s", external_order_id, exc_info=True)
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
