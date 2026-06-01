from __future__ import annotations

import hashlib
import json
import logging
import math
from bisect import bisect_right
from collections import Counter, defaultdict
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


def _version(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{digest}"


def _normalize_asset_symbol(asset_symbol: str | None) -> str:
    return "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())


def _artifact_type(base: str, *, frequency: str = BTC15M_TOUCH20_RULES_FREQ, asset_symbol: str = BTC15M_TOUCH20_RULES_ASSET) -> str:
    freq = normalize_frequency(frequency) or BTC15M_TOUCH20_RULES_FREQ
    asset = _normalize_asset_symbol(asset_symbol) or BTC15M_TOUCH20_RULES_ASSET
    return f"{base}:{freq}:{asset}"


def _approval_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    return f"{BTC15M_TOUCH20_RULES_STRATEGY}_approval:{kalshi_env}:{_normalize_asset_symbol(asset_symbol)}:{normalize_frequency(frequency) or frequency}"


def _ledger_stream(kalshi_env: str, asset_symbol: str, frequency: str) -> str:
    return f"{BTC15M_TOUCH20_RULES_STRATEGY}:{kalshi_env}:{_normalize_asset_symbol(asset_symbol)}:{normalize_frequency(frequency) or frequency}"


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
    return (normalize_frequency(frequency) or frequency) == BTC15M_TOUCH20_RULES_FREQ and _normalize_asset_symbol(asset_symbol) == BTC15M_TOUCH20_RULES_ASSET


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


def _non_proxy_spot(row: CryptoSpotOHLCRecord) -> bool:
    source_kind = str(row.source_kind or "").strip().lower()
    provider = str(row.provider or "").strip().lower()
    return source_kind not in {"spot_price_proxy", "proxy"} and provider not in {"coingecko"}


def _spot_time(row: CryptoSpotOHLCRecord) -> datetime | None:
    return _as_utc(row.observed_at or row.end_ts)


def _prepare_spot_index(spot_rows: list[CryptoSpotOHLCRecord]) -> dict[str, list[Any]]:
    entries: list[tuple[datetime, datetime, Decimal, CryptoSpotOHLCRecord]] = []
    for row in spot_rows:
        if (
            _normalize_asset_symbol(row.asset_symbol) != BTC15M_TOUCH20_RULES_ASSET
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
) -> dict[str, Any]:
    return _spot_features_from_index(
        _prepare_spot_index(spot_rows),
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
    target_pct = Decimal(str(settings.crypto_btc15m_touch20_take_profit_pct))
    min_price = Decimal(str(settings.crypto_btc15m_touch20_min_contract_price_dollars))
    min_score = Decimal(str(settings.crypto_btc15m_touch20_min_rule_score))
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
        if snapshot.status and snapshot.status not in {"open", "active"}:
            reason = "market_not_open"
        elif not spot.get("available"):
            reason = str(spot.get("reason") or "spot_data_missing_or_stale")
        elif entry is None or bid is None or mid is None or spread is None:
            reason = "missing_real_bid_ask"
        elif market_age is None or time_to_close is None:
            reason = "entry_window_unknown"
        elif market_age < int(settings.crypto_btc15m_touch20_min_market_age_seconds):
            reason = "market_too_early"
        elif time_to_close < int(settings.crypto_btc15m_touch20_min_seconds_to_close):
            reason = "market_too_late"
        elif entry < min_price:
            reason = "entry_price_below_min"
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
                            "touch_rate": max(float(settings.crypto_btc15m_touch20_replay_min_touch_rate), 0.25),
                            "net_pnl": "0.0500",
                        }
                        if not require_allowed_bucket
                        else {}
                    ),
                )
                if score < min_score:
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
) -> dict[str, Any]:
    side = str(candidate["side"])
    entry = _decimal(candidate["execution_price_dollars"])
    target_exit = _decimal(candidate["target_exit_side_price_dollars"])
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    touched = _first_touch(future_rows, side=side, target_exit_side_price=target_exit)
    if touched is not None:
        exit_row, exit_price = touched
        exit_reason = "take_profit"
        exit_observed_at = _snapshot_decision_time(exit_row)
    else:
        exit_price = _settlement_side_payout(row, side)
        exit_reason = "settlement_hold"
        exit_observed_at = _as_utc(row.close_time or row.expected_expiration_time) or _snapshot_decision_time(row)
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry, count=Decimal("1.00"), fee_rate=fee_rate)
    exit_fee = estimate_kalshi_taker_fee_dollars(price_dollars=exit_price, count=Decimal("1.00"), fee_rate=fee_rate)
    gross = exit_price - entry
    net = gross - entry_fee - exit_fee
    return {
        "side": side,
        "entry_price_dollars": _money_text(entry),
        "target_exit_side_price_dollars": _money_text(target_exit),
        "exit_price_dollars": _money_text(exit_price),
        "exit_reason": exit_reason,
        "exit_observed_at": exit_observed_at.isoformat(),
        "touched": touched is not None,
        "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
        "fees": str((entry_fee + exit_fee).quantize(Decimal("0.0001"))),
        "net_pnl": str(net.quantize(Decimal("0.0001"))),
        "bucket_key": candidate.get("bucket_key"),
        "rule_score": candidate.get("rule_score"),
    }


def _bucket_matrix(trades: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        simulation = trade.get("simulation") if isinstance(trade.get("simulation"), dict) else {}
        key = str(simulation.get("bucket_key") or "")
        if key:
            grouped[key].append(trade)
    matrix: list[dict[str, Any]] = []
    min_touch_rate = Decimal(str(settings.crypto_btc15m_touch20_replay_min_touch_rate))
    min_pnl_per = Decimal(str(settings.crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars))
    for key, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        touch_count = sum(1 for row in rows if (row.get("simulation") or {}).get("touched") is True)
        net = sum(values, Decimal("0"))
        sample_count = len(rows)
        touch_rate = Decimal(touch_count) / Decimal(sample_count) if sample_count else Decimal("0")
        pnl_per = net / Decimal(sample_count) if sample_count else Decimal("0")
        allowed = sample_count >= 5 and net > Decimal("0") and touch_rate >= min_touch_rate and pnl_per >= min_pnl_per
        first = rows[0]
        candidate = first.get("candidate") if isinstance(first.get("candidate"), dict) else {}
        matrix.append(
            {
                "bucket_key": key,
                "asset_symbol": BTC15M_TOUCH20_RULES_ASSET,
                "side": candidate.get("side"),
                "sample_count": sample_count,
                "touch_count": touch_count,
                "touch_rate": float(touch_rate),
                "net_pnl": str(net.quantize(Decimal("0.0001"))),
                "pnl_per_candidate": str(pnl_per.quantize(Decimal("0.0001"))),
                "allowed": allowed,
            }
        )
    matrix.sort(key=lambda item: (_decimal(item.get("pnl_per_candidate") or "0"), float(item.get("touch_rate") or 0.0)), reverse=True)
    return matrix


def _gate_requirements(settings: Settings) -> dict[str, Any]:
    return {
        "min_trade_candidates": settings.crypto_btc15m_touch20_replay_min_candidates,
        "min_net_pl_dollars": settings.crypto_btc15m_touch20_replay_min_net_pnl_dollars,
        "min_pnl_per_candidate_dollars": settings.crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars,
        "max_hard_cap_breaches": settings.crypto_btc15m_touch20_replay_max_hard_cap_breaches,
        "min_touch_rate": settings.crypto_btc15m_touch20_replay_min_touch_rate,
        "requires_allowed_bucket_support": True,
        "requires_real_quote_path_evidence": True,
        "uses_trained_model": False,
    }


def gate_reasons(metrics: dict[str, Any], *, settings: Settings) -> list[str]:
    if not metrics:
        return ["BTC 15m touch20 rules replay artifact is missing."]
    reasons: list[str] = []
    if metrics.get("backtest_missing"):
        reasons.append("BTC 15m touch20 rules replay artifact is missing.")
    if metrics.get("uses_trained_model") is True:
        reasons.append("BTC 15m touch20 rules replay must not use trained model predictions.")
    real_quote_rows = int(metrics.get("real_quote_path_row_count") or 0)
    if not metrics.get("backtest_missing") and real_quote_rows <= 0:
        reasons.append("BTC 15m touch20 rules replay has no settled real quote-path evidence.")
    candidates = int(metrics.get("trade_candidate_count") or 0)
    min_candidates = int(settings.crypto_btc15m_touch20_replay_min_candidates)
    net_pl = Decimal(str(metrics.get("net_simulated_pl_dollars") or "0"))
    min_net = Decimal(str(settings.crypto_btc15m_touch20_replay_min_net_pnl_dollars))
    pnl_per = Decimal(str(metrics.get("pnl_per_candidate_dollars") or "0"))
    min_pnl_per = Decimal(str(settings.crypto_btc15m_touch20_replay_min_pnl_per_candidate_dollars))
    hard_cap_breaches = int(metrics.get("hard_cap_breaches") or 0)
    max_hard_cap = int(settings.crypto_btc15m_touch20_replay_max_hard_cap_breaches)
    touch_rate = Decimal(str(metrics.get("touch_rate") or "0"))
    min_touch_rate = Decimal(str(settings.crypto_btc15m_touch20_replay_min_touch_rate))
    if candidates < min_candidates:
        reasons.append(f"BTC 15m touch20 rules replay candidate count {candidates} below minimum {min_candidates}.")
    if net_pl <= min_net:
        reasons.append(f"BTC 15m touch20 rules replay net P/L ${float(net_pl):.2f} does not clear required positive threshold.")
    if pnl_per < min_pnl_per:
        reasons.append(
            f"BTC 15m touch20 rules replay P/L per candidate ${float(pnl_per):.4f} below minimum ${float(min_pnl_per):.4f}."
        )
    if hard_cap_breaches > max_hard_cap:
        reasons.append(f"BTC 15m touch20 rules replay hard-cap breaches {hard_cap_breaches} exceed limit {max_hard_cap}.")
    if touch_rate < min_touch_rate:
        reasons.append(f"BTC 15m touch20 rules replay touch rate {float(touch_rate):.1%} below minimum {float(min_touch_rate):.1%}.")
    if not (metrics.get("allowed_bucket_keys") or []):
        reasons.append("BTC 15m touch20 rules replay has no allowed bucket support.")
    return reasons


def _evaluate_replay(
    snapshots: list[CryptoMarketSnapshotRecord],
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    settings: Settings,
) -> dict[str, Any]:
    scoped_rows = [
        row
        for row in snapshots
        if _normalize_asset_symbol(row.asset_symbol) == BTC15M_TOUCH20_RULES_ASSET
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
    spot_index = _prepare_spot_index(spot_rows)
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
                max_age_seconds=max(int(settings.crypto_btc15m_touch20_spot_fresh_seconds), BTC15M_TOUCH20_RULES_INTERVAL_SECONDS),
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
                        "touch_rate": max(float(settings.crypto_btc15m_touch20_replay_min_touch_rate), 0.25),
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
            simulation = _simulate_replay_trade(row, future_rows, selected, settings=settings)
            trades.append(
                {
                    "market_ticker": row.market_ticker,
                    "decision_ts": decision_ts.isoformat(),
                    "settlement_result": row.settlement_result,
                    "candidate": selected,
                    "simulation": simulation,
                }
            )
    bucket_matrix = _bucket_matrix(trades, settings=settings)
    allowed_keys = [bucket["bucket_key"] for bucket in bucket_matrix if bucket.get("allowed")]
    blocked_keys = [bucket["bucket_key"] for bucket in bucket_matrix if not bucket.get("allowed")]
    values = [_decimal((trade.get("simulation") or {}).get("net_pnl")) for trade in trades]
    fees = [_decimal((trade.get("simulation") or {}).get("fees")) for trade in trades]
    touch_count = sum(1 for trade in trades if (trade.get("simulation") or {}).get("touched") is True)
    net = sum(values, Decimal("0"))
    trade_count = len(trades)
    metrics = {
        "objective": "touch_20pct_before_close",
        "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
        "uses_trained_model": False,
        "asset_symbols": [BTC15M_TOUCH20_RULES_ASSET],
        "sample_count": len(scoped_rows),
        "real_quote_path_row_count": len(scoped_rows),
        "trade_candidate_count": trade_count,
        "touch_count": touch_count,
        "touch_rate": float(Decimal(touch_count) / Decimal(trade_count)) if trade_count else 0.0,
        "settlement_hold_count": trade_count - touch_count,
        "net_simulated_pl_dollars": float(net),
        "pnl_per_candidate_dollars": float(net / Decimal(trade_count)) if trade_count else 0.0,
        "fees_dollars": float(sum(fees, Decimal("0"))),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
        "candidate_status_counts": dict(status_counts),
        "candidate_reason_counts": dict(reason_counts),
        "bucket_matrix": bucket_matrix,
        "allowed_bucket_keys": allowed_keys,
        "blocked_bucket_keys": blocked_keys,
        "fee_model_version": current_fee_model_version(),
    }
    return {
        "status": "ok" if trades else "warn",
        "metrics": metrics,
        "bucket_matrix": bucket_matrix,
        "trades": trades[:100],
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
    }


def _gate_passed(gate: Any | None) -> bool:
    if gate is None:
        return False
    if str(getattr(gate, "status", "") or "").lower() != "passed":
        return False
    payload = getattr(gate, "payload", None)
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
        "schema_version": "btc15m-touch20-rules-ledger-v2",
        "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
        "kalshi_env": kalshi_env,
        "asset_symbol": _normalize_asset_symbol(asset_symbol),
        "frequency": normalize_frequency(frequency) or frequency,
        "positions": dict(positions),
        "updated_at": payload.get("updated_at"),
    }


def _open_entries(ledger: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{BTC15M_TOUCH20_RULES_ORDER_PREFIX}:"):
            continue
        if isinstance(entry, dict) and str(entry.get("status") or "") in {"open", "exit_submitted"}:
            entries.append((str(client_order_id), entry))
    return entries


def _open_pending_notional(ledger: dict[str, Any]) -> Decimal:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    total = Decimal("0")
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{BTC15M_TOUCH20_RULES_ORDER_PREFIX}:"):
            continue
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status") or "") in {"entry_submitted", "open", "exit_submitted"}:
            total += _decimal(entry.get("entry_notional_dollars") or "0")
    return total.quantize(Decimal("0.0001"))


def _daily_realized_pnl(ledger: dict[str, Any], now: datetime) -> Decimal:
    positions = ledger.get("positions") if isinstance(ledger.get("positions"), dict) else {}
    total = Decimal("0")
    for client_order_id, entry in positions.items():
        if not str(client_order_id).startswith(f"{BTC15M_TOUCH20_RULES_ORDER_PREFIX}:"):
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


def _client_order_id(action: str, *, market_ticker: str, side: str, now: datetime) -> str:
    basis = f"{BTC15M_TOUCH20_RULES_STRATEGY}:{action}:{market_ticker}:{side}:{now.isoformat()}".encode("utf-8")
    digest = hashlib.blake2b(basis, digest_size=10).hexdigest()
    return f"{BTC15M_TOUCH20_RULES_ORDER_PREFIX}:{action[:1]}:{digest}"


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
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_fee = estimate_kalshi_taker_fee_dollars(price_dollars=entry_side_price, count=count_fp, fee_rate=fee_rate)
    return {
        "status": status,
        "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
        "client_order_id": client_order_id,
        "market_ticker": market.market_ticker,
        "asset_symbol": market.asset_symbol,
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
        "take_profit_pct": float(settings.crypto_btc15m_touch20_take_profit_pct),
        "profit_protection_threshold_pct": float(settings.crypto_btc15m_touch20_profit_protection_threshold_pct),
        "profit_protection_floor_pct": float(settings.crypto_btc15m_touch20_profit_protection_floor_pct),
        "profit_protection_armed": False,
        "max_net_profit_pct": "0.0000",
        "quote_history": [],
        "entry_receipt": receipt.model_dump(mode="json"),
        "kalshi_order_id": receipt.external_order_id,
        "gate_version": getattr(gate, "version", None),
        "gate_artifact_type": getattr(gate, "artifact_type", None),
        "approval": {
            "gate_version": approval.get("gate_version"),
            "approved_by": approval.get("approved_by"),
            "approved_at": approval.get("approved_at"),
            "max_notional_dollars": approval.get("max_notional_dollars"),
        },
    }


def profit_protection_review(
    entry: dict[str, Any],
    *,
    spot: dict[str, Any],
    net_profit: Decimal,
    settings: Settings,
    now: datetime,
) -> dict[str, Any]:
    threshold = Decimal(str(settings.crypto_btc15m_touch20_profit_protection_threshold_pct))
    floor = Decimal(str(settings.crypto_btc15m_touch20_profit_protection_floor_pct))
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
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_crypto_settled_live_quote_path_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=cutoff,
                limit=limit or 200_000,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=[asset],
                since=cutoff,
                limit=1_000_000,
            )
            await session.commit()
        replay = _evaluate_replay(snapshots, spot_rows, settings=self.settings)
        metrics = dict(replay["metrics"])
        metrics["dataset_source"] = "settled_live_quote_paths"
        reasons = gate_reasons(metrics, settings=self.settings)
        report = {
            "schema_version": "btc15m-touch20-rules-backtest-v1",
            "status": "pass" if not reasons else "warn",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbol": asset,
            "objective": "touch_20pct_before_close",
            "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
            "uses_trained_model": False,
            "days": days,
            "metrics": metrics,
            "requirements": _gate_requirements(self.settings),
            "gate_reasons": reasons,
            "trade_sample": replay["trades"][:100],
            "trade_sample_count": min(100, len(replay["trades"])),
            "trade_count": len(replay["trades"]),
        }
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                artifact = await repo.record_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_BACKTEST_ARTIFACT, frequency=freq, asset_symbol=asset),
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
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_BACKTEST_ARTIFACT, frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict(getattr(backtest, "metrics", None) or {})
            if backtest is None:
                metrics["backtest_missing"] = True
            reasons = gate_reasons(metrics, settings=self.settings)
            payload = {
                "passed": not reasons,
                "reasons": reasons,
                "requirements": _gate_requirements(self.settings),
                "objective": "touch_20pct_before_close",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "uses_trained_model": False,
                "backtest_version": getattr(backtest, "version", None),
            }
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_GATE_ARTIFACT, frequency=freq, asset_symbol=asset),
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
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_GATE_ARTIFACT, frequency=freq, asset_symbol=asset),
                kalshi_env=self.settings.kalshi_env,
            )
            if not _gate_passed(gate):
                await session.commit()
                return {
                    "status": "gate_not_passed",
                    "gate": _artifact_summary(gate),
                    "reason": "latest btc15m touch20 rules gate is missing or blocked",
                }
            payload = {
                "schema_version": "btc15m-touch20-rules-approval-v1",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "kalshi_env": self.settings.kalshi_env,
                "asset_symbol": asset,
                "frequency": freq,
                "approved": True,
                "gate_version": gate.version,
                "approved_by": approved_by,
                "approved_at": datetime.now(UTC).isoformat(),
                "max_notional_dollars": _money_text(max_notional_dollars or Decimal(str(self.settings.crypto_btc15m_touch20_max_open_notional_dollars))),
                "note": note,
            }
            await repo.set_checkpoint(_approval_stream(self.settings.kalshi_env, asset, freq), gate.version, payload)
            await repo.log_ops_event(
                severity="info",
                source="crypto_non_model_btc15m_touch20",
                summary=f"BTC 15m touch20 rules approved for gate {gate.version}",
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
                summary="BTC 15m touch20 rules approval revoked",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {"status": "revoked", "approval": payload}

    async def status(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "frequency": freq, "asset_symbol": asset}
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_GATE_ARTIFACT, frequency=freq, asset_symbol=asset),
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
        return {
            "status": "ok",
            "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbol": asset,
            "enabled": bool(self.settings.crypto_btc15m_touch20_rules_enabled),
            "trading_enabled": bool(self.settings.crypto_btc15m_touch20_rules_trading_enabled),
            "gate": _artifact_summary(gate),
            "approval": approval,
            "approval_valid": approval_valid,
            "approval_reason": approval_reason,
            "open_pending_notional_dollars": _money_text(_open_pending_notional(ledger)),
            "open_strategy_positions": len(_open_entries(ledger)),
        }

    async def run_once(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "frequency": freq, "asset_symbol": asset}
        if not bool(self.settings.crypto_btc15m_touch20_rules_enabled):
            return {
                "status": "disabled",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "frequency": freq,
                "asset_symbol": asset,
                "reason": "CRYPTO_BTC15M_TOUCH20_RULES_ENABLED is false",
            }

        now = datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_artifact_type(BTC15M_TOUCH20_RULES_GATE_ARTIFACT, frequency=freq, asset_symbol=asset),
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
        if control.active_color != self.settings.app_color:
            return {"status": "inactive_color", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "active_color": control.active_color, "app_color": self.settings.app_color, "gate": gate_summary}
        if control.kill_switch_enabled:
            return {"status": "kill_switch_enabled", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "gate": gate_summary}
        if not _gate_passed(gate):
            return {"status": "gate_blocked", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "gate": gate_summary, "reason": "btc15m touch20 rules gate missing or blocked"}
        approval_valid, approval_reason = _approval_valid(approval, gate)
        if not approval_valid:
            return {"status": "approval_blocked", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "gate": gate_summary, "approval": approval, "reason": approval_reason}

        daily_pnl = _daily_realized_pnl(ledger, now)
        daily_loss_limit = Decimal(str(self.settings.crypto_btc15m_touch20_daily_loss_limit_dollars))
        if daily_loss_limit > Decimal("0") and daily_pnl <= -daily_loss_limit:
            return {
                "status": "daily_loss_limit_blocked",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "daily_realized_pnl_dollars": _money_text(daily_pnl),
                "daily_loss_limit_dollars": _money_text(daily_loss_limit),
            }

        funnel: Counter[str] = Counter()
        skipped: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        gate_metrics = dict(getattr(gate, "metrics", None) or {})
        quote_fresh_seconds = max(1, int(self.settings.crypto_btc15m_touch20_quote_fresh_seconds))
        for snapshot in snapshots:
            funnel["market_seen"] += 1
            observed_at = _snapshot_decision_time(snapshot)
            if now - observed_at > timedelta(seconds=quote_fresh_seconds):
                skipped.append({"market_ticker": snapshot.market_ticker, "reason": "stale_quote_snapshot"})
                continue
            funnel["quote_fresh"] += 1
            spot = _spot_features(
                spot_rows,
                decision_ts=observed_at,
                freshness_reference=now,
                max_age_seconds=int(self.settings.crypto_btc15m_touch20_spot_fresh_seconds),
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
        cap = Decimal(str(self.settings.crypto_btc15m_touch20_max_open_notional_dollars))
        approval_cap = _decimal(approval.get("max_notional_dollars"), cap)
        if approval_cap > Decimal("0"):
            cap = min(cap, approval_cap)
        remaining_cap = max(Decimal("0"), cap - open_pending_notional)
        if not candidates:
            result = {
                "status": "no_candidate",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "gate": gate_summary,
                "approval": approval,
                "funnel": dict(funnel),
                "skipped": skipped[:25],
                "open_pending_notional_dollars": _money_text(open_pending_notional),
            }
            await self._log_cycle(result)
            return result
        if remaining_cap <= Decimal("0"):
            result = {
                "status": "strategy_cap_blocked",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "selected": _selection_summary(candidates[0]),
                "open_pending_notional_dollars": _money_text(open_pending_notional),
                "max_open_notional_dollars": _money_text(cap),
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
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "selected": _selection_summary(selected_item),
                "remaining_cap_dollars": _money_text(remaining_cap),
            }
            await self._log_cycle(result)
            return result
        target_yes = quantize_price(selected["target_yes_price_dollars"])
        client_order_id = _client_order_id("entry", market_ticker=market.market_ticker, side=side_text, now=now)
        if not bool(self.settings.crypto_btc15m_touch20_rules_trading_enabled):
            result = {
                "status": "trading_disabled",
                "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
                "selected": _selection_summary(selected_item),
                "client_order_id": client_order_id,
                "funnel": dict(funnel),
                "gate": gate_summary,
                "approval": approval,
                "no_order_submitted": True,
            }
            await self._log_cycle(result)
            return result

        ticket = TradeTicket(
            market_ticker=market.market_ticker,
            action=TradeAction.BUY,
            side=side,
            yes_price_dollars=target_yes,
            count_fp=count_fp,
            capital_bucket=BTC15M_TOUCH20_RULES_STRATEGY,
            note="btc15m_touch20_rules isolated non-model entry",
        )
        room = Room(name=f"BTC 15m touch20 rules {market.market_ticker}", market_ticker=market.market_ticker, kalshi_env=self.settings.kalshi_env, shadow_mode=False)
        receipt = await self.base_execution_service.execute(room=room, control=control, ticket=ticket, client_order_id=client_order_id, fair_yes_dollars=None)
        order_status = str(receipt.status or "")
        filled_count_fp = await self._filled_count_fp(receipt.external_order_id)
        ledger_count_fp = filled_count_fp if filled_count_fp is not None and filled_count_fp > Decimal("0") else count_fp
        ledger_status = "open" if order_status in {"filled", "executed"} or (filled_count_fp or Decimal("0")) > Decimal("0") else "entry_submitted"
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
                    strategy_code=BTC15M_TOUCH20_RULES_STRATEGY,
                )
            if order_status not in {"shadow_skipped", "kill_switch_blocked", "inactive_color_skipped", "write_credentials_missing"}:
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
                summary=f"BTC 15m touch20 rules entry {order_status}: {market.market_ticker} {side_text}",
                payload={"strategy": BTC15M_TOUCH20_RULES_STRATEGY, "client_order_id": client_order_id, "receipt": receipt.model_dump(mode="json"), "selected": _selection_summary(selected_item)},
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return {
            "status": order_status,
            "strategy": BTC15M_TOUCH20_RULES_STRATEGY,
            "client_order_id": client_order_id,
            "external_order_id": receipt.external_order_id,
            "filled_count_fp": _count_text(filled_count_fp) if filled_count_fp is not None else None,
            "selected": _selection_summary(selected_item),
            "funnel": dict(funnel),
            "gate": gate_summary,
            "approval": approval,
        }

    async def exit_once(self, *, frequency: str = "15m", asset_symbol: str = "BTC") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        asset = _normalize_asset_symbol(asset_symbol)
        if not _scope_supported(freq, asset):
            return {"status": "unsupported_scope", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "frequency": freq, "asset_symbol": asset}
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
            return {"status": "no_open_strategy_positions", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "frequency": freq, "asset_symbol": asset}
        if control.active_color != self.settings.app_color:
            return {"status": "inactive_color", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "active_color": control.active_color, "app_color": self.settings.app_color, "open_entries": len(open_entries)}

        evaluated: list[dict[str, Any]] = []
        exits: list[dict[str, Any]] = []
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
                max_age_seconds=int(self.settings.crypto_btc15m_touch20_spot_fresh_seconds),
            )
            protection = profit_protection_review(entry, spot=spot, net_profit=profit_pct, settings=self.settings, now=now)
            entry.update(protection["entry_updates"])
            trigger = None
            if profit_pct >= Decimal(str(self.settings.crypto_btc15m_touch20_take_profit_pct)):
                trigger = "take_profit"
            elif protection["trigger"]:
                trigger = protection["trigger"]
            evaluated.append({"client_order_id": client_order_id, "market_ticker": market_ticker, "status": "evaluated", "net_profit_pct": str(profit_pct), "trigger": trigger, "profit_protection": protection["review"]})
            if trigger is None:
                continue
            exit_client_order_id = _client_order_id("exit", market_ticker=market_ticker, side=side, now=now)
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
            else:
                entry["status"] = "exit_submitted"
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
                        strategy_code=BTC15M_TOUCH20_RULES_STRATEGY,
                    )
                ledger["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq), None, ledger)
                await repo.log_ops_event(
                    severity="info" if status in {"filled", "executed", "submitted"} else "warning",
                    source="crypto_non_model_btc15m_touch20",
                    summary=f"BTC 15m touch20 rules exit {status}: {market_ticker} {side} {trigger}",
                    payload={"strategy": BTC15M_TOUCH20_RULES_STRATEGY, "entry_client_order_id": client_order_id, "exit_client_order_id": exit_client_order_id, "trigger": trigger, "net_profit_pct": str(profit_pct), "receipt": receipt.model_dump(mode="json")},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
            exits.append({"entry_client_order_id": client_order_id, "exit_client_order_id": exit_client_order_id, "market_ticker": market_ticker, "trigger": trigger, "status": status, "net_profit_pct": str(profit_pct)})
        if not exits:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                ledger["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(_ledger_stream(self.settings.kalshi_env, asset, freq), None, ledger)
                await session.commit()
        return {"status": "ok", "strategy": BTC15M_TOUCH20_RULES_STRATEGY, "frequency": freq, "asset_symbol": asset, "evaluated": evaluated, "exits": exits}

    async def _log_cycle(self, result: dict[str, Any]) -> None:
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.log_ops_event(
                    severity="info" if result.get("status") in {"trading_disabled", "no_candidate"} else "warning",
                    source="crypto_non_model_btc15m_touch20",
                    summary=f"BTC 15m touch20 rules cycle: {result.get('status')}",
                    payload=result,
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
        except Exception:
            logger.warning("failed to log BTC 15m touch20 rules cycle telemetry", exc_info=True)

    async def _filled_count_fp(self, external_order_id: str | None) -> Decimal | None:
        if not external_order_id:
            return None
        try:
            return await self.base_execution_service._get_filled_fp(external_order_id)
        except Exception:
            logger.warning("failed to fetch filled count for BTC 15m touch20 order %s", external_order_id, exc_info=True)
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
