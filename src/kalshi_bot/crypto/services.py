from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import importlib.metadata as importlib_metadata
import json
import logging
import math
import multiprocessing
import os
import time
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.constants import (
    CRYPTO_MAX_SPREAD_BPS,
    CRYPTO_MIN_REMAINING_PAYOUT_BPS,
    CRYPTO_MIN_SPREAD_BPS,
)
from kalshi_bot.core.enums import (
    AgentRole,
    ContractSide,
    MessageKind,
    RiskStatus,
    RoomOrigin,
    RoomStage,
    StandDownReason,
    StrategyCode,
    TradeAction,
    WeatherResolutionState,
)
from kalshi_bot.core.fixed_point import make_client_order_id, quantize_count, quantize_price
from kalshi_bot.core.schemas import ExecReceiptPayload, RoomCreate, RoomMessageCreate, TradeEligibilityVerdict, TradeTicket
from kalshi_bot.crypto.edge_shrinkage import EDGE_SHRINKAGE_STATUS_OK, fit_edge_shrinkage
from kalshi_bot.crypto.models import CryptoMarket, CryptoSeries
from kalshi_bot.crypto.parsing import (
    normalize_candlestick,
    normalize_frequency,
    parse_datetime,
    parse_crypto_market,
    parse_crypto_series,
    parse_price,
)
from kalshi_bot.db.models import (
    CryptoFundingRateRecord,
    CryptoMarketCandlestickRecord,
    CryptoMarketSnapshotRecord,
    CryptoSpotOHLCRecord,
    DecisionTraceRecord,
    FillRecord,
    OrderRecord,
    RiskVerdictRecord,
    Room,
    RoomMessage,
    Signal,
    TradeTicketRecord,
)
from kalshi_bot.db.repositories import PlatformRepository, _crypto_snapshot_matches_frequency_duration
from kalshi_bot.integrations.crypto_spot import (
    COINGECKO_IDS,
    COINBASE_PRODUCT_IDS,
    KRAKEN_PAIRS,
    CoinbaseSpotClient,
    CoinGeckoSpotClient,
    KrakenSpotClient,
    SpotOHLC,
    interval_seconds_for_frequency,
    load_coinbase_cdp_credentials,
)
from kalshi_bot.integrations.okx_funding_rates import OKX_ASSET_INST_IDS, OkxFundingRateClient
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeCryptoPolicy
from kalshi_bot.services.execution import KALSHI_GTC_TIME_IN_FORCE, ExecutionService
from kalshi_bot.services.fee_model import (
    current_fee_model_version,
    estimate_kalshi_maker_fee_dollars,
    estimate_kalshi_taker_fee_dollars,
)
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext, approved_ticket_for_verdict
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars

logger = logging.getLogger(__name__)

CRYPTO_ASSET_MODES_KEY = "crypto_asset_modes"
CRYPTO_ASSET_MODE_OFF = "off"
CRYPTO_ASSET_MODE_SHADOW = "shadow"
CRYPTO_ASSET_MODE_LIVE = "live"
CRYPTO_ASSET_MODES = {
    CRYPTO_ASSET_MODE_OFF,
    CRYPTO_ASSET_MODE_SHADOW,
    CRYPTO_ASSET_MODE_LIVE,
}
CRYPTO_LOGISTIC_FEATURE_SCHEMA_VERSION = "crypto-logistic-v2"
CRYPTO_RICH_FEATURE_SCHEMA_VERSION = "crypto-rich-v10"
CRYPTO_CANDIDATE_REGISTRY_VERSION = "crypto-candidate-registry-v1"
CRYPTO_AUTONOMY_CYCLE_OPS_SCHEMA_VERSION = "crypto-autonomy-cycle-v1"
CRYPTO_PROBABILITY_GUARDRAIL_TOLERANCE = 0.02
CRYPTO_EXPLORATORY_SHADOW = "exploratory_shadow"
CRYPTO_LIVE_QUALITY = "live_quality"
CRYPTO_SETTLEMENT_BENCHMARK_SOURCE = "cfb_rti_60s_average"
CRYPTO_SETTLEMENT_PROXY_REASON_CODE = "crypto_settlement_proxy_for_cfb_rti"
CRYPTO_ORDER_MODE_PASSIVE_ONLY = "passive_only"
CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER = "passive_then_taker"
CRYPTO_LAST_MINUTE_PASSIVE_REASON = "last_minute_passive_market_confidence"
CRYPTO_EDGE_SHRINKAGE_NOTE_PREFIX = "crypto_edge_shrinkage"
CRYPTO_SPOT_MAX_STALE_SECONDS_BY_PROVIDER = {
    "coinbase": 5,
    "coingecko": 90,
    # Kraken rows are completed OHLC candles (collected at 15m cadence), so a
    # fresh row can legitimately trail "now" by up to one interval.
    "kraken": 960,
}
# Preferred provider order when multiple venues report the same spot period.
CRYPTO_SPOT_PROVIDER_PREFERENCE = {
    "coinbase": 0,
    "kraken": 1,
    "coingecko": 2,
}
CRYPTO_ORDER_BOOK_TOP_LEVELS = 5
CRYPTO_SPOT_CONTEXT_HISTORICAL = "historical"
CRYPTO_SPOT_CONTEXT_LIVE = "live"
CRYPTO_MODEL_CANDIDATE_NAMES = (
    "market_mid_baseline",
    "current_heuristic",
    "sklearn_logistic",
    "spot_distance_residual",
    "spot_distance_contrarian",
    "spot_distance_contrarian_gated",
    "asset_time_calibration",
    "xgboost_classifier",
    "lightgbm_classifier",
)


def _settled_label_matches_requested_duration(market: CryptoMarket, frequency: str) -> bool:
    freq = normalize_frequency(frequency)
    if freq == "1h" and (market.open_time is None or market.close_time is None):
        return False
    return _crypto_snapshot_matches_frequency_duration(market, freq)


CRYPTO_MODEL_BASELINE_CANDIDATES = {"market_mid_baseline"}


def _resolve_crypto_shadow_evidence_mode(
    *,
    production_mode: bool,
    quote_evidence_enabled: bool,
    production_autonomy_enabled: bool,
    shadow_evidence_always: bool,
) -> bool:
    """Whether the shadow-evidence decision path should run this cycle.

    Historically shadow-evidence only ran when production autonomy was OFF and
    quote-evidence collection was ON (``quote_evidence_enabled and not
    production_autonomy_enabled``). That legacy branch is preserved exactly.

    ``shadow_evidence_always`` is an INDEPENDENT switch: when set (in production)
    it enables the shadow-evidence DECISION path regardless of the production
    autonomy state or the quote-evidence COLLECTION flag. This is deliberate —
    the live daemons disable quote-evidence collection (offloaded to dedicated
    collectors), but we still want SHADOW-mode assets to emit exploratory shadow
    decisions for pre-promotion evaluation. shadow_evidence_mode only gates which
    assets are evaluated; the decision uses normal market snapshots, so it does
    not depend on the collection loop. It NEVER enables live orders — execution
    stays blocked by asset_mode==LIVE / live-eligibility checks.
    """
    return bool(
        production_mode
        and (
            shadow_evidence_always
            or (quote_evidence_enabled and not production_autonomy_enabled)
        )
    )
CRYPTO_CROSS_ASSET_FEATURE_ASSETS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE")
CRYPTO_ENTRY_OPTIMIZER_GRID = {
    "min_fee_adjusted_edge_bps": (750, 1000, 1500, 2500, 5000),
    "max_spread_bps": (80, 150, 250, 400, 600, 1000),
    "min_contract_price_dollars": (0.50, 0.60, 0.70),
    "min_remaining_payout_bps": (0, 100, 300, 500),
}
CRYPTO_MICROSTRUCTURE_UPSERT_COMMIT_INTERVAL = 250
CRYPTO_TRAINING_STEP_RETRY_DELAYS_SECONDS = (5.0, 15.0, 45.0)
CRYPTO_TRAINING_DB_RETRY_DELAYS_SECONDS = (2.0, 5.0, 15.0, 30.0)
CRYPTO_STRATEGY_CODES = {
    "15m": StrategyCode.CRYPTO_15M.value,
    "1h": StrategyCode.CRYPTO_1H.value,
}
CRYPTO_FREQUENCY_LABELS = {
    "15m": "15m",
    "1h": "1h",
}


def _version(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{digest}"


def _money_text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _count_text(value: Decimal | None) -> str | None:
    return f"{value:.2f}" if value is not None else None


def _clamp_price(value: Decimal) -> Decimal:
    return quantize_price(min(Decimal("0.9900"), max(Decimal("0.0100"), value)))


CRYPTO_PASSIVE_PRICE_TICK = Decimal("0.01")


def _clamp_cent_price(value: Decimal) -> Decimal:
    return min(Decimal("0.99"), max(Decimal("0.01"), value)).quantize(CRYPTO_PASSIVE_PRICE_TICK)


def _rows_from_response(response: dict[str, Any], key: str) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [row for row in response if isinstance(row, dict)]
    rows = response.get(key)
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def normalize_asset_symbol(asset_symbol: str) -> str:
    normalized = "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())
    if not normalized:
        raise ValueError("asset_symbol is required")
    return normalized


def normalize_asset_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized not in CRYPTO_ASSET_MODES:
        raise ValueError(f"unsupported crypto asset mode: {mode}")
    return normalized


def _normalize_asset_mode_key(raw_key: str) -> str:
    """Normalize a mode dict key, preserving 'SYMBOL:FREQ' compound format."""
    parts = str(raw_key).split(":", 1)
    symbol = normalize_asset_symbol(parts[0])
    if len(parts) == 2:
        freq = normalize_frequency(parts[1]) or parts[1].strip().lower()
        return f"{symbol}:{freq}"
    return symbol


def normalize_asset_symbols(asset_symbols: list[str] | None) -> list[str]:
    return sorted({normalize_asset_symbol(symbol) for symbol in (asset_symbols or []) if str(symbol or "").strip()})


def crypto_entry_override_key(asset_symbol: str, frequency: str | None = None) -> str:
    asset = normalize_asset_symbol(asset_symbol)
    normalized_frequency = normalize_frequency(frequency) if frequency else None
    return f"{asset}:{normalized_frequency}" if normalized_frequency else asset


def crypto_strategy_code_for_frequency(frequency: object) -> str:
    normalized = normalize_frequency(frequency) or "15m"
    return CRYPTO_STRATEGY_CODES.get(normalized, f"CRYPTO_{normalized.upper().replace('-', '_')}")


def crypto_frequency_label(frequency: object) -> str:
    normalized = normalize_frequency(frequency) or "15m"
    return CRYPTO_FREQUENCY_LABELS.get(normalized, normalized)


def crypto_frequency_switch_enabled(settings: Settings, frequency: object) -> bool:
    normalized = normalize_frequency(frequency) or "15m"
    if normalized == "15m":
        return bool(settings.crypto_15m_enabled)
    if normalized == "1h":
        return bool(settings.crypto_1h_enabled)
    return False


def crypto_frequency_enabled(settings: Settings, frequency: object) -> bool:
    if not bool(settings.crypto_enabled):
        return False
    return crypto_frequency_switch_enabled(settings, frequency)


def crypto_autonomy_min_seconds_to_close_for_frequency(settings: Settings, frequency: object) -> int:
    normalized = normalize_frequency(frequency) or "15m"
    if normalized == "1h":
        return max(0, int(settings.crypto_1h_autonomy_min_seconds_to_close))
    return max(0, int(settings.crypto_autonomy_min_seconds_to_close))


def enabled_crypto_frequencies(settings: Settings) -> list[str]:
    frequencies: list[str] = []
    for raw in str(settings.crypto_auto_frequencies or "15m").replace(";", ",").split(","):
        normalized = normalize_frequency(raw)
        if normalized and normalized not in frequencies and crypto_frequency_enabled(settings, normalized):
            frequencies.append(normalized)
    return frequencies or (["15m"] if crypto_frequency_enabled(settings, "15m") else [])


def crypto_autonomy_15m_assets(settings: Settings) -> list[str]:
    """Sorted asset list for per-asset 15m autonomy loops, sourced from crypto_model_nightly_assets."""
    assets = sorted(_normalize_asset_csv(settings.crypto_model_nightly_assets))
    return assets or ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"]


def _normalize_asset_csv(value: str | None) -> set[str]:
    symbols: set[str] = set()
    for raw in str(value or "").replace(";", ",").split(","):
        raw = raw.strip()
        if not raw:
            continue
        symbols.add(normalize_asset_symbol(raw))
    return symbols


def _safe_normalize_asset_symbol(value: object, default: str = "UNKNOWN") -> str:
    try:
        return normalize_asset_symbol(str(value or default))
    except ValueError:
        return default


def _pct_objective_label(value: Decimal) -> str:
    pct = (value * Decimal("100")).quantize(Decimal("0.0001"))
    if pct == pct.to_integral_value():
        return f"{int(pct)}pct"
    return f"{str(pct).rstrip('0').rstrip('.').replace('.', '_')}pct"


def _crypto_touch_objective(value: Decimal) -> str:
    return f"touch_{_pct_objective_label(value)}_before_close"


def _crypto_touch_exit_objective(value: Decimal) -> str:
    return f"exitably_up_{_pct_objective_label(value)}_before_close"


def _crypto_btc_1h_touch_policy_configured_for_row(row: dict[str, Any], *, settings: Settings) -> bool:
    if not bool(settings.crypto_1h_touch_strategy_enabled):
        return False
    if _crypto_frequency_for_row(row) != "1h":
        return False
    asset = _safe_normalize_asset_symbol(row.get("asset_symbol"))
    return asset in (_normalize_asset_csv(settings.crypto_1h_touch_assets) or {"BTC"})


def _crypto_btc_1h_touch_policy_configured_for_market(market: CryptoMarket, *, settings: Settings) -> bool:
    if not bool(settings.crypto_1h_touch_strategy_enabled):
        return False
    if (normalize_frequency(market.frequency) or "15m") != "1h":
        return False
    asset = _safe_normalize_asset_symbol(market.asset_symbol)
    return asset in (_normalize_asset_csv(settings.crypto_1h_touch_assets) or {"BTC"})


def _crypto_touch_policy_context(
    row: dict[str, Any],
    *,
    settings: Settings,
    btc_1h_touch_policy: bool = False,
) -> dict[str, Any]:
    target_pct = Decimal(str(settings.crypto_touch_strategy_take_profit_pct))
    stop_pct = Decimal(str(settings.crypto_touch_strategy_stop_loss_pct))
    min_market_age_seconds: int | None = None
    min_seconds_to_close: int | None = None
    policy_name = "legacy_touch"
    no_initial_hard_stop = False
    if btc_1h_touch_policy:
        target_pct = Decimal(str(settings.crypto_1h_touch_take_profit_pct))
        stop_pct = Decimal("1.0000")
        min_market_age_seconds = max(0, int(settings.crypto_1h_touch_min_market_age_seconds))
        min_seconds_to_close = max(0, int(settings.crypto_1h_touch_min_seconds_to_close))
        policy_name = "btc_1h_touch20"
        no_initial_hard_stop = True
    return {
        "policy_name": policy_name,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "objective": _crypto_touch_objective(target_pct),
        "exit_objective": _crypto_touch_exit_objective(target_pct),
        "min_market_age_seconds": min_market_age_seconds,
        "min_seconds_to_close": min_seconds_to_close,
        "no_initial_hard_stop": no_initial_hard_stop,
    }


def _crypto_touch_entry_window_reason(
    row: dict[str, Any],
    *,
    settings: Settings,
    policy_context: dict[str, Any],
) -> str | None:
    min_market_age = policy_context.get("min_market_age_seconds")
    min_seconds_to_close = policy_context.get("min_seconds_to_close")
    if min_market_age is None and min_seconds_to_close is None:
        return _crypto_live_entry_window_reason(row, settings=settings)

    frequency = _crypto_frequency_for_row(row)
    try:
        interval_seconds = interval_seconds_for_frequency(frequency)
    except ValueError:
        interval_seconds = 3600 if frequency == "1h" else 900
    market_age = _optional_int(row.get("market_age_seconds"))
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    if market_age is None and time_to_close is not None and time_to_close <= interval_seconds:
        market_age = max(0, interval_seconds - time_to_close)
    if time_to_close is None and market_age is not None and market_age <= interval_seconds:
        time_to_close = max(0, interval_seconds - market_age)
    if market_age is None or time_to_close is None:
        return "crypto_entry_window_unknown"
    if min_market_age is not None and market_age < int(min_market_age):
        return "crypto_market_too_early_for_live_entry"
    if min_seconds_to_close is not None and time_to_close < int(min_seconds_to_close):
        return "crypto_market_too_late_for_live_entry"
    return None


def _crypto_touch_replay_gate_passed(replay_gate: Any | None) -> bool:
    if replay_gate is None:
        return False
    if str(getattr(replay_gate, "status", "") or "").strip().lower() != "passed":
        return False
    payload = getattr(replay_gate, "payload", None)
    if isinstance(payload, dict) and "passed" in payload:
        return payload.get("passed") is True
    return True


def _crypto_signal_uses_btc_1h_touch20_gate(
    signal: StrategySignal,
    market: CryptoMarket,
    *,
    settings: Settings,
) -> bool:
    if not _crypto_btc_1h_touch_policy_configured_for_market(market, settings=settings):
        return False
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    touch_strategy = trace.get("touch_strategy") if isinstance(trace.get("touch_strategy"), dict) else {}
    selection_touch_strategy = (
        selection.get("touch_strategy") if isinstance(selection.get("touch_strategy"), dict) else {}
    )
    objective = str(selection.get("objective") or trace.get("objective") or "").strip()
    touch_objective = str(touch_strategy.get("objective") or selection_touch_strategy.get("objective") or "").strip()
    touch_policy = str(touch_strategy.get("policy") or selection_touch_strategy.get("policy") or "").strip()
    return (
        objective == "touch_20pct_before_close"
        or touch_objective == "exitably_up_20pct_before_close"
        or touch_policy == "btc_1h_touch20"
        or isinstance(trace.get("touch_replay_gate"), dict)
        or isinstance(selection.get("touch_replay_gate"), dict)
    )


def _crypto_last_minute_passive_bid_by_asset(settings: Settings) -> dict[str, Decimal]:
    bids: dict[str, Decimal] = {}
    for raw in str(settings.crypto_last_minute_passive_bid_by_asset or "").replace(";", ",").split(","):
        if ":" not in raw:
            continue
        symbol_raw, price_raw = raw.split(":", 1)
        try:
            symbol = normalize_asset_symbol(symbol_raw)
            price = _clamp_cent_price(Decimal(str(price_raw).strip()))
        except Exception:
            continue
        bids[symbol] = price
    return bids


def _crypto_last_minute_passive_price_ladder(settings: Settings) -> list[Decimal]:
    raw_value = str(settings.crypto_last_minute_passive_price_ladder or "").strip()
    values: set[Decimal] = set()
    if raw_value.count(":") == 2:
        start_raw, end_raw, step_raw = raw_value.split(":", 2)
        try:
            current = _clamp_cent_price(Decimal(start_raw.strip()))
            end = _clamp_cent_price(Decimal(end_raw.strip()))
            step = abs(Decimal(step_raw.strip())).quantize(CRYPTO_PASSIVE_PRICE_TICK)
            if step > 0:
                while current <= end:
                    values.add(_clamp_cent_price(current))
                    current += step
        except Exception:
            values.clear()
    else:
        for raw in raw_value.replace(";", ",").split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                values.add(_clamp_cent_price(Decimal(raw)))
            except Exception:
                continue
    if not values:
        current = Decimal("0.01")
        while current <= Decimal("0.99"):
            values.add(_clamp_cent_price(current))
            current += Decimal("0.01")
    return sorted(values)


def _crypto_artifact_type(base: str, asset_symbols: list[str] | None = None) -> str:
    symbols = normalize_asset_symbols(asset_symbols)
    if len(symbols) == 1:
        return f"{base}:{symbols[0]}"
    return base


def _crypto_objective_is_touch20(objective: str | None) -> bool:
    value = str(objective or "settlement").strip().lower().replace("-", "_")
    return value in {"touch20", "touch_20", "touch_20pct", "touch_20pct_before_close"}


def _crypto_replay_gate_note_updates(
    *,
    frequency: str,
    asset_symbols: list[str] | None,
    status: str,
    version: str,
    reasons: list[str],
    updated_at: datetime,
) -> dict[str, dict[str, Any]]:
    freq = normalize_frequency(frequency) or str(frequency or "15m").strip().lower() or "15m"
    symbols = normalize_asset_symbols(asset_symbols)
    note = {
        "status": status,
        "version": version,
        "updated_at": updated_at.isoformat(),
        "reasons": list(reasons),
        "frequency": freq,
        "asset_symbols": symbols,
    }
    updates: dict[str, dict[str, Any]] = {f"crypto_replay_gate:{freq}": dict(note)}
    if symbols:
        updates[f"crypto_replay_gate:{freq}:{','.join(symbols)}"] = dict(note)
    if freq == "15m":
        updates["crypto_replay_gate"] = dict(note)
        if symbols:
            updates[f"crypto_replay_gate:{','.join(symbols)}"] = dict(note)
    return updates


def _crypto_touch_replay_gate_note_updates(
    *,
    frequency: str,
    asset_symbols: list[str] | None,
    status: str,
    version: str,
    reasons: list[str],
    updated_at: datetime,
) -> dict[str, dict[str, Any]]:
    freq = normalize_frequency(frequency) or str(frequency or "1h").strip().lower() or "1h"
    symbols = normalize_asset_symbols(asset_symbols)
    note = {
        "status": status,
        "version": version,
        "updated_at": updated_at.isoformat(),
        "reasons": list(reasons),
        "frequency": freq,
        "asset_symbols": symbols,
        "objective": "touch_20pct_before_close",
    }
    updates: dict[str, dict[str, Any]] = {f"crypto_replay_gate_touch20:{freq}": dict(note)}
    if symbols:
        updates[f"crypto_replay_gate_touch20:{freq}:{','.join(symbols)}"] = dict(note)
    return updates


async def _latest_crypto_artifact_for_asset(
    repo: PlatformRepository,
    *,
    frequency: str,
    artifact_type: str,
    kalshi_env: str,
    asset_symbol: str | None = None,
    allow_generic_fallback: bool = True,
    prefer_generic: bool = False,
) -> Any | None:
    if prefer_generic and allow_generic_fallback:
        # Pooled-model mode (crypto_model_nightly_pooled_only): a *trained*
        # generic pooled artifact wins over the per-asset one. Per-asset
        # artifacts stay as the fallback so assets trained before the first
        # pooled run keep working until a pooled artifact exists.
        generic = await repo.get_latest_crypto_model_artifact(
            frequency=frequency,
            artifact_type=artifact_type,
            kalshi_env=kalshi_env,
        )
        if generic is not None and str(getattr(generic, "status", "") or "") == "trained":
            return generic
        if asset_symbol:
            artifact = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type=_crypto_artifact_type(artifact_type, [asset_symbol]),
                kalshi_env=kalshi_env,
            )
            if artifact is not None:
                return artifact
        return generic
    if asset_symbol:
        artifact = await repo.get_latest_crypto_model_artifact(
            frequency=frequency,
            artifact_type=_crypto_artifact_type(artifact_type, [asset_symbol]),
            kalshi_env=kalshi_env,
        )
        if artifact is not None:
            return artifact
    if not allow_generic_fallback:
        return None
    return await repo.get_latest_crypto_model_artifact(
        frequency=frequency,
        artifact_type=artifact_type,
        kalshi_env=kalshi_env,
    )


def _filter_crypto_snapshot_rows(rows: list[Any], asset_symbols: list[str] | None) -> list[Any]:
    symbols = set(normalize_asset_symbols(asset_symbols))
    if not symbols:
        return rows
    return [row for row in rows if normalize_asset_symbol(str(getattr(row, "asset_symbol", "") or "")) in symbols]


def _crypto_spot_feature_asset_scope(asset_symbols: list[str] | None) -> list[str] | None:
    """Spot rows also feed the cross-asset return features, so a single-asset
    training/replay run must still see the cross-feature assets' spot history."""
    symbols = set(normalize_asset_symbols(asset_symbols))
    if not symbols:
        return None
    symbols.update(CRYPTO_CROSS_ASSET_FEATURE_ASSETS)
    return sorted(symbols)


# A re-upserted row's recency features look back over the last 20 prior settled
# markets (see _crypto_add_recent_asset_features). The read window must extend at
# least this far BEFORE the upsert floor so every re-upserted row's look-back is
# fully loaded and recomputes identically to a full rebuild. 24h comfortably holds
# 20 markets for both 15m (~192/24h) and 1h (~24/24h) frequencies.
_INCREMENTAL_MIN_RECENCY_CONTEXT_HOURS = 24


def _resolve_incremental_materialize_since(
    *,
    full_since: datetime,
    now: datetime,
    watermark: datetime | None,
    enabled: bool,
    warmup_hours: int,
    max_gap_hours: int,
    label_refresh_hours: int,
) -> tuple[datetime, datetime | None, str]:
    """Return ``(effective_since, upsert_floor, reason)``.

    ``reason`` is one of ``'full_disabled'``, ``'full_cold_cache'``,
    ``'full_gap_exceeds_max'``, ``'full_insufficient_recency_margin'``, or
    ``'incremental'``. The incremental tail reads from ``watermark - warmup`` but
    is clamped so it never reads earlier than ``full_since`` (never more than the
    configured lookback window).

    ``upsert_floor`` is ``None`` for full rebuilds (upsert everything); for
    incremental it is ``watermark - label_refresh_hours`` (only rows newer than
    this are re-upserted, so re-upserted rows always have their full recency
    look-back contained in the read window and recompute identically to a full
    rebuild). Because ``warmup_hours >> label_refresh_hours`` every re-upserted
    row has many hours of prior settled markets loaded ahead of it.

    Guard: if the read window does not extend at least
    ``_INCREMENTAL_MIN_RECENCY_CONTEXT_HOURS`` before the upsert floor (e.g. a
    misconfigured ``warmup_hours`` too close to / smaller than
    ``label_refresh_hours``), a partial incremental would re-upsert rows with a
    truncated recency look-back, silently corrupting features. In that case fall
    back to a full rebuild.
    """
    if not enabled:
        return full_since, None, "full_disabled"
    if watermark is None:
        # Cold cache or a feature-schema bump invalidated all persisted rows.
        return full_since, None, "full_cold_cache"
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=UTC)
    else:
        watermark = watermark.astimezone(UTC)
    if now - watermark > timedelta(hours=max_gap_hours):
        return full_since, None, "full_gap_exceeds_max"
    candidate = watermark - timedelta(hours=max(1, warmup_hours))
    effective_since = max(full_since, candidate)
    upsert_floor = watermark - timedelta(hours=max(1, label_refresh_hours))
    # The read window must hold a full recency look-back ahead of every
    # re-upserted row. If it does not, a partial incremental would corrupt
    # recency features — fall back to a full rebuild instead.
    if upsert_floor - effective_since < timedelta(hours=_INCREMENTAL_MIN_RECENCY_CONTEXT_HOURS):
        return full_since, None, "full_insufficient_recency_margin"
    return effective_since, upsert_floor, "incremental"


async def _list_crypto_spot_rows_with_cross_assets(
    repo: PlatformRepository,
    *,
    frequency: str,
    kalshi_env: str,
    requested_assets: list[str] | None,
    since: datetime | None,
    limit: int,
) -> list[Any]:
    """Load the requested assets' spot rows under their own row budget, then the
    remaining cross-feature assets under a second budget, so widening the scope
    for cross-asset returns can never truncate the primary asset's coverage."""
    primary = await repo.list_crypto_spot_ohlc(
        frequency=frequency,
        kalshi_env=kalshi_env,
        asset_symbols=requested_assets or None,
        since=since,
        limit=limit,
    )
    requested = set(normalize_asset_symbols(requested_assets))
    if not requested:
        return primary
    cross = sorted(set(CRYPTO_CROSS_ASSET_FEATURE_ASSETS) - requested)
    if not cross:
        return primary
    cross_rows = await repo.list_crypto_spot_ohlc(
        frequency=frequency,
        kalshi_env=kalshi_env,
        asset_symbols=cross,
        since=since,
        limit=limit,
    )
    return [*primary, *cross_rows]


def _dedupe_crypto_snapshot_rows(rows: list[Any]) -> list[Any]:
    deduped: list[Any] = []
    seen: set[Any] = set()
    for row in rows:
        key = getattr(row, "id", None)
        if key is None:
            key = (
                getattr(row, "market_ticker", None),
                getattr(row, "observed_at", None),
                getattr(row, "source_kind", None),
            )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _filter_crypto_dict_rows(rows: list[dict[str, Any]], asset_symbols: list[str] | None) -> list[dict[str, Any]]:
    symbols = set(normalize_asset_symbols(asset_symbols))
    if not symbols:
        return rows
    return [row for row in rows if normalize_asset_symbol(str(row.get("asset_symbol") or "")) in symbols]


class CryptoAssetControlService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    @staticmethod
    def normalize_symbol(asset_symbol: str) -> str:
        return normalize_asset_symbol(asset_symbol)

    @staticmethod
    def normalize_mode(mode: str) -> str:
        return normalize_asset_mode(mode)

    def modes_from_notes(self, notes: dict[str, Any] | None) -> dict[str, str]:
        raw_modes = (notes or {}).get(CRYPTO_ASSET_MODES_KEY) or {}
        if not isinstance(raw_modes, dict):
            return {}
        modes: dict[str, str] = {}
        for raw_key, raw_mode in raw_modes.items():
            try:
                key = _normalize_asset_mode_key(str(raw_key))
                mode = normalize_asset_mode(str(raw_mode))
            except ValueError:
                continue
            modes[key] = mode
        return modes

    def explicit_mode_for_control(self, control: Any, asset_symbol: str, *, frequency: str | None = None) -> str:
        symbol = normalize_asset_symbol(asset_symbol)
        modes = self.modes_from_notes(getattr(control, "notes", None))
        freq = normalize_frequency(frequency) if frequency else None
        if freq and f"{symbol}:{freq}" in modes:
            return modes[f"{symbol}:{freq}"]
        return modes.get(symbol, CRYPTO_ASSET_MODE_SHADOW)

    def mode_for_control(
        self,
        control: Any,
        asset_symbol: str,
        *,
        crypto_policy: RuntimeCryptoPolicy | None = None,
        frequency: str | None = None,
    ) -> str:
        symbol = normalize_asset_symbol(asset_symbol)
        modes = self.modes_from_notes(getattr(control, "notes", None))
        freq = normalize_frequency(frequency) if frequency else None
        freq_key = f"{symbol}:{freq}" if freq else None
        # Frequency-specific key takes precedence; fall back to plain symbol key
        note_mode = (modes.get(freq_key) if freq_key else None) or modes.get(symbol)
        if note_mode == CRYPTO_ASSET_MODE_OFF:
            return CRYPTO_ASSET_MODE_OFF
        if str(self.settings.kalshi_env or "").strip().lower() != "demo" and note_mode in CRYPTO_ASSET_MODES:
            return note_mode
        asset_modes = crypto_policy.asset_modes if crypto_policy is not None else {}
        policy_mode = (asset_modes.get(freq_key) if freq_key else None) or asset_modes.get(symbol)
        if policy_mode in CRYPTO_ASSET_MODES:
            return policy_mode
        return note_mode or CRYPTO_ASSET_MODE_SHADOW

    def asset_mode_summary(
        self,
        *,
        asset_symbols: list[str] | None,
        modes: dict[str, str],
    ) -> dict[str, Any]:
        symbols = {normalize_asset_symbol(symbol) for symbol in (asset_symbols or []) if str(symbol or "").strip()}
        symbols.update(modes)
        resolved = {symbol: modes.get(symbol, CRYPTO_ASSET_MODE_SHADOW) for symbol in sorted(symbols)}
        counts = {mode: 0 for mode in sorted(CRYPTO_ASSET_MODES)}
        for mode in resolved.values():
            counts[mode] = counts.get(mode, 0) + 1
        return {"modes": resolved, "counts": counts}

    def global_live_blockers(
        self,
        *,
        control: Any,
        replay_gate: Any | None,
        has_write_credentials: bool,
        frequency: str = "15m",
        crypto_policy: RuntimeCryptoPolicy | None = None,
    ) -> list[str]:
        blockers: list[str] = []
        normalized_frequency = normalize_frequency(frequency) or "15m"
        if not self.settings.crypto_enabled:
            blockers.append("Crypto is disabled.")
        if normalized_frequency == "15m" and not self.settings.crypto_15m_enabled:
            blockers.append("15-minute crypto is disabled.")
        if normalized_frequency == "1h" and not self.settings.crypto_1h_enabled:
            blockers.append("1-hour crypto is disabled.")
        trading_enabled = self.settings.crypto_trading_enabled or bool(
            crypto_policy.trading_enabled if crypto_policy is not None else False
        )
        if not trading_enabled:
            blockers.append("Global crypto trading is disabled.")
        if self.settings.app_shadow_mode:
            blockers.append("App shadow mode is enabled.")
        if getattr(control, "kill_switch_enabled", False):
            blockers.append("Kill switch is enabled.")
        active_color = str(getattr(control, "active_color", "") or "")
        if active_color and active_color != self.settings.app_color:
            blockers.append(f"Active color is {active_color}; this app is {self.settings.app_color}.")
        blockers.extend(_runtime_replay_gate_blockers(replay_gate, crypto_policy))
        if not has_write_credentials:
            blockers.append("Kalshi write credentials are missing.")
        return blockers

    def market_live_status(
        self,
        *,
        control: Any,
        replay_gate: Any | None,
        market: CryptoMarket,
        has_write_credentials: bool,
        crypto_policy: RuntimeCryptoPolicy | None = None,
    ) -> dict[str, Any]:
        mode = self.mode_for_control(control, market.asset_symbol, crypto_policy=crypto_policy, frequency=market.frequency)
        explicit_mode = self.explicit_mode_for_control(control, market.asset_symbol, frequency=market.frequency)
        global_blockers = self.global_live_blockers(
            control=control,
            replay_gate=replay_gate,
            has_write_credentials=has_write_credentials,
            frequency=market.frequency,
            crypto_policy=crypto_policy,
        )
        blockers = list(global_blockers)
        if mode != CRYPTO_ASSET_MODE_LIVE:
            blockers = [f"Asset {market.asset_symbol} mode is {mode}; set it to live to allow live orders."]
        elif str(self.settings.kalshi_env or "").strip().lower() != "demo" and explicit_mode != CRYPTO_ASSET_MODE_LIVE:
            blockers.append(
                f"Asset {market.asset_symbol} is not explicitly live in deployment control "
                f"(control mode {explicit_mode})."
            )
        return {
            "asset_mode": mode,
            "control_asset_mode": explicit_mode,
            "live_eligible": mode == CRYPTO_ASSET_MODE_LIVE and not blockers,
            "live_blockers": blockers,
            "global_live_blockers": global_blockers,
        }

    async def list_asset_modes(
        self,
        *,
        asset_symbols: list[str] | None = None,
        kalshi_env: str | None = None,
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            control = await repo.get_deployment_control(kalshi_env=env)
            modes = self.modes_from_notes(control.notes)
            await session.commit()
        return self.asset_mode_summary(asset_symbols=asset_symbols, modes=modes)

    async def set_asset_mode(
        self,
        asset_symbol: str,
        mode: str,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        actor: str = "operator",
    ) -> dict[str, Any]:
        symbol = normalize_asset_symbol(asset_symbol)
        freq = normalize_frequency(frequency) if frequency else None
        mode_key = f"{symbol}:{freq}" if freq else symbol
        normalized_mode = normalize_asset_mode(mode)
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            previous_mode = CRYPTO_ASSET_MODE_SHADOW

            def update_modes(previous_value: Any) -> dict[str, str]:
                nonlocal previous_mode
                modes = self.modes_from_notes({CRYPTO_ASSET_MODES_KEY: previous_value})
                previous_mode = modes.get(mode_key, CRYPTO_ASSET_MODE_SHADOW)
                modes[mode_key] = normalized_mode
                return modes

            control, _ = await repo.update_deployment_note_key(
                CRYPTO_ASSET_MODES_KEY,
                update_modes,
                kalshi_env=env,
            )
            await repo.log_ops_event(
                severity="info",
                summary=f"Crypto asset mode set: {mode_key} {normalized_mode}",
                source="crypto_asset_control",
                payload={
                    "asset_symbol": symbol,
                    "frequency": freq,
                    "mode_key": mode_key,
                    "mode": normalized_mode,
                    "previous_mode": previous_mode,
                    "actor": actor,
                    "kalshi_env": env,
                },
                kalshi_env=env,
            )
            await session.commit()
        return {
            "status": "ok",
            "asset_symbol": symbol,
            "frequency": freq,
            "mode_key": mode_key,
            "mode": normalized_mode,
            "previous_mode": previous_mode,
            "asset_modes": self.modes_from_notes(control.notes),
        }

    @staticmethod
    def _compute_win_rate(outcomes: list[Any]) -> tuple[int, int]:
        """Return (wins, total) where a win is realized_pnl_dollars > 0.

        None/zero/negative pnl rows count toward total but never as wins.
        """
        wins = 0
        for outcome in outcomes:
            pnl = getattr(outcome, "realized_pnl_dollars", None)
            if pnl is None:
                continue
            try:
                if float(pnl) > 0:
                    wins += 1
            except (TypeError, ValueError):
                continue
        return wins, len(outcomes)

    async def evaluate_winrate_guard(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
    ) -> dict[str, Any]:
        """Rolling per-asset win-rate kill-switch.

        For each asset currently explicitly LIVE in deployment-control notes,
        compute the win rate over the last `window` traded+settled outcomes. When
        the window is full AND the win rate is below breakeven, increment a
        persisted per-asset consecutive-breach counter; when the win rate is OK,
        reset it to 0; when the window is not yet full, leave it unchanged. After
        `consecutive_windows` consecutive breaches, flip the asset to shadow and
        reset its counter.

        This guard only ever flips LIVE -> shadow. Auto-resume (shadow -> live)
        is intentionally not implemented here even when
        ``crypto_winrate_guard_auto_resume`` is True; resuming remains a manual
        operator action by default per the safety spec.
        """
        env = kalshi_env or self.settings.kalshi_env
        normalized_frequency = normalize_frequency(frequency) or "15m"
        window = int(self.settings.crypto_winrate_guard_window)
        breakeven = float(self.settings.crypto_winrate_guard_breakeven_pct)
        consecutive_target = int(self.settings.crypto_winrate_guard_consecutive_windows)

        assessed: dict[str, Any] = {}
        paused: list[str] = []

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            control = await repo.get_deployment_control(kalshi_env=env)
            note_modes = self.modes_from_notes(control.notes)
            live_assets = [
                symbol for symbol, mode in note_modes.items() if mode == CRYPTO_ASSET_MODE_LIVE
            ]

            for symbol in sorted(live_assets):
                stream_name = f"crypto_winrate_guard:{env}:{symbol}"
                checkpoint = await repo.get_checkpoint(stream_name)
                counter = int((checkpoint.payload or {}).get("consecutive_breaches", 0)) if checkpoint else 0

                outcomes = await repo.list_recent_settled_crypto_outcomes(
                    frequency=normalized_frequency,
                    kalshi_env=env,
                    asset_symbol=symbol,
                    limit=window,
                )
                wins, total = self._compute_win_rate(outcomes)
                window_full = total >= window
                win_rate = (wins / total) if total else None
                breached = window_full and win_rate is not None and win_rate < breakeven

                action = "noop"
                if window_full:
                    if breached:
                        counter += 1
                    else:
                        counter = 0
                # window not full -> counter unchanged

                trip = breached and counter >= consecutive_target
                if trip:
                    action = "paused"
                    paused.append(symbol)

                # Persist the (non-reset) counter now. We do NOT reset on trip here:
                # if `set_asset_mode` (run after this session commits) were to fail,
                # resetting first would leave a losing asset LIVE with counter=0 —
                # the fail-OPEN direction, which is unsafe for a kill-switch. By
                # keeping the counter at/above threshold until the flip succeeds, a
                # failed flip re-trips next pass (fail-safe toward pausing). The
                # counter is reset only after a confirmed flip (see below).
                await repo.set_checkpoint(
                    stream_name,
                    None,
                    {
                        "consecutive_breaches": counter,
                        "last_win_rate": win_rate,
                        "last_window_size": total,
                        "updated_at": datetime.now(UTC).isoformat(),
                    },
                )
                assessed[symbol] = {
                    "wins": wins,
                    "total": total,
                    "window_full": window_full,
                    "win_rate": win_rate,
                    "breached": breached,
                    "consecutive_breaches": counter,
                    "action": action,
                }
            await session.commit()

        for symbol in paused:
            await self.set_asset_mode(
                symbol,
                CRYPTO_ASSET_MODE_SHADOW,
                kalshi_env=env,
                actor="winrate_guard",
            )
            # Flip confirmed -> reset the breach counter so the now-shadow asset
            # is not immediately re-evaluated/re-flipped on the next pass.
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=env)
                stream_name = f"crypto_winrate_guard:{env}:{symbol}"
                checkpoint = await repo.get_checkpoint(stream_name)
                payload = dict(checkpoint.payload or {}) if checkpoint else {}
                payload["consecutive_breaches"] = 0
                payload["updated_at"] = datetime.now(UTC).isoformat()
                await repo.set_checkpoint(stream_name, None, payload)
                await session.commit()

        return {
            "status": "ok",
            "frequency": normalized_frequency,
            "kalshi_env": env,
            "window": window,
            "breakeven_pct": breakeven,
            "consecutive_windows": consecutive_target,
            "assessed": assessed,
            "paused": paused,
        }


class CryptoMarketService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        agent_pack_service: AgentPackService,
        asset_control_service: CryptoAssetControlService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.agent_pack_service = agent_pack_service
        self.asset_control_service = asset_control_service
        # Discovery results barely change between autonomy iterations, but the
        # active-color loop has no sleep — uncached discovery re-pages the full
        # series + market listings dozens of times per second and saturates the
        # shared Kalshi API budget (429s on live quote polls, starved crawls).
        self._series_cache: dict[str, tuple[float, list[CryptoSeries]]] = {}
        self._markets_cache: dict[tuple[Any, ...], tuple[float, list[CryptoMarket]]] = {}

    def _discovery_cache_ttl(self) -> float:
        return max(0.0, float(getattr(self.settings, "crypto_market_discovery_cache_seconds", 0.0) or 0.0))

    async def discover_series(self, *, frequency: str = "15m") -> list[CryptoSeries]:
        if not self.settings.crypto_enabled:
            return []
        wanted = normalize_frequency(frequency) or "15m"
        ttl = self._discovery_cache_ttl()
        if ttl > 0:
            cached = self._series_cache.get(wanted)
            if cached is not None and (time.monotonic() - cached[0]) < ttl:
                return list(cached[1])
        cursor: str | None = None
        seen_cursors: set[str] = set()
        series: list[CryptoSeries] = []
        for _ in range(20):
            params: dict[str, Any] = {"category": "Crypto", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            response = await self.kalshi.list_series(**params)
            for row in _rows_from_response(response, "series"):
                parsed = parse_crypto_series(row, frequency=wanted)
                if parsed is not None:
                    series.append(parsed)
            cursor = response.get("cursor") or response.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        deduped: dict[str, CryptoSeries] = {item.series_ticker: item for item in series}
        result = sorted(deduped.values(), key=lambda item: item.asset_symbol)
        if ttl > 0:
            self._series_cache[wanted] = (time.monotonic(), list(result))
        return result

    async def discover_markets(
        self,
        *,
        frequency: str = "15m",
        status: str | None = "open",
        persist: bool = True,
        asset_symbols: list[str] | None = None,
        min_close_time: datetime | None = None,
        max_close_time: datetime | None = None,
    ) -> list[CryptoMarket]:
        requested_assets = set(normalize_asset_symbols(asset_symbols))
        ttl = self._discovery_cache_ttl()
        cache_key: tuple[Any, ...] | None = None
        if ttl > 0 and min_close_time is None and max_close_time is None:
            cache_key = (
                normalize_frequency(frequency) or "15m",
                status,
                tuple(sorted(requested_assets)),
                bool(persist),
            )
            cached = self._markets_cache.get(cache_key)
            if cached is not None and (time.monotonic() - cached[0]) < ttl:
                return list(cached[1])
        series_rows = await self.discover_series(frequency=frequency)
        if requested_assets:
            series_rows = [
                series for series in series_rows if normalize_asset_symbol(series.asset_symbol) in requested_assets
            ]
        markets: list[CryptoMarket] = []
        for series in series_rows:
            cursor: str | None = None
            seen_cursors: set[str] = set()
            for _ in range(20):
                params: dict[str, Any] = {
                    "series_ticker": series.series_ticker,
                    "limit": 1000,
                }
                if status:
                    params["status"] = status
                if min_close_time is not None:
                    params["min_close_ts"] = int(min_close_time.timestamp())
                if max_close_time is not None:
                    params["max_close_ts"] = int(max_close_time.timestamp())
                if cursor:
                    params["cursor"] = cursor
                response = await self.kalshi.list_markets(**params)
                for row in _rows_from_response(response, "markets"):
                    parsed = parse_crypto_market(row, series=series, frequency=frequency)
                    if parsed is not None:
                        markets.append(parsed)
                cursor = response.get("cursor") or response.get("next_cursor")
                if not cursor or cursor in seen_cursors:
                    break
                seen_cursors.add(cursor)
        markets.sort(key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol))
        if persist and markets:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                for market in markets:
                    await self.record_market_snapshot(repo, market, source_kind="live")
                await session.commit()
        if cache_key is not None:
            self._markets_cache[cache_key] = (time.monotonic(), list(markets))
        return markets

    async def get_market(self, market_ticker: str, *, persist: bool = True) -> CryptoMarket:
        response = await self.kalshi.get_market(market_ticker)
        market = parse_crypto_market(response, frequency="15m")
        if market is None:
            raise KeyError(market_ticker)
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await self.record_market_snapshot(repo, market, source_kind="live")
                await session.commit()
        return market

    async def record_market_snapshot(
        self,
        repo: PlatformRepository,
        market: CryptoMarket,
        *,
        source_kind: str,
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshotRecord:
        return await repo.record_crypto_market_snapshot(
            kalshi_env=self.settings.kalshi_env,
            series_ticker=market.series_ticker,
            market_ticker=market.market_ticker,
            event_ticker=market.event_ticker,
            asset_symbol=market.asset_symbol,
            frequency=market.frequency,
            title=market.title,
            status=market.status,
            open_time=market.open_time,
            close_time=market.close_time,
            expected_expiration_time=market.expected_expiration_time,
            target_price_dollars=market.target_price_dollars,
            yes_bid_dollars=market.yes_bid_dollars,
            yes_ask_dollars=market.yes_ask_dollars,
            no_bid_dollars=market.no_bid_dollars,
            no_ask_dollars=market.no_ask_dollars,
            last_price_dollars=market.last_price_dollars,
            volume=market.volume,
            open_interest=market.open_interest,
            settlement_result=market.settlement_result,
            observed_at=observed_at or datetime.now(UTC),
            source_kind=source_kind,
            payload=market.to_payload(),
        )

    async def dashboard_payload(self, *, frequency: str = "15m", current_only: bool = True) -> dict[str, Any]:
        dashboard_frequency = normalize_frequency(frequency) or "15m"
        try:
            markets = await self.discover_markets(frequency=dashboard_frequency, status="open", persist=True)
            source = "kalshi_live"
        except Exception:
            logger.warning("crypto market discovery failed; using stored snapshots", exc_info=True)
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                rows = await repo.list_latest_crypto_market_snapshots(frequency=dashboard_frequency)
                await session.commit()
            markets = [_market_from_snapshot(row) for row in rows]
            source = "stored_snapshots"
        total_open_markets = len(markets)
        if current_only:
            markets = _nearest_market_per_asset(markets)
        asset_symbols = sorted({normalize_asset_symbol(market.asset_symbol) for market in markets})
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            signal_payloads = await repo.latest_signal_payloads_for_markets(
                market_tickers=[market.market_ticker for market in markets],
                kalshi_env=self.settings.kalshi_env,
            )
            generic_gate = await repo.get_latest_crypto_model_artifact(
                frequency=dashboard_frequency,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            replay_gates_by_asset: dict[str, Any | None] = {}
            for asset_symbol in asset_symbols:
                replay_gates_by_asset[asset_symbol] = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=dashboard_frequency,
                    artifact_type="replay_gate",
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbol=asset_symbol,
                )
            active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            active_rooms: dict[str, dict[str, str]] = {}
            for market in markets:
                room = await repo.get_latest_active_room_for_market(
                    market.market_ticker,
                    kalshi_env=self.settings.kalshi_env,
                )
                if room is not None:
                    risk = await repo.get_latest_risk_verdict_for_room(room.id)
                    risk_payload = risk.payload if risk is not None and isinstance(risk.payload, dict) else {}
                    risk_diagnostics = risk_payload.get("diagnostics") if isinstance(risk_payload.get("diagnostics"), dict) else {}
                    active_rooms[market.market_ticker] = {
                        "id": room.id,
                        "stage": room.stage,
                        "latest_risk": (
                            {
                                "status": risk.status,
                                "reason_codes": risk_payload.get("reason_codes") or [],
                                "crypto_position_add_on": risk_diagnostics.get("crypto_position_add_on"),
                            }
                            if risk is not None
                            else None
                        ),
                    }
            await session.commit()
        mode_summary = self.asset_control_service.asset_mode_summary(
            asset_symbols=asset_symbols,
            modes=_resolved_crypto_asset_modes(
                asset_symbols=asset_symbols,
                note_modes=self.asset_control_service.modes_from_notes(control.notes),
                crypto_policy=crypto_policy,
            ),
        )
        replay_gate_summary = _crypto_replay_gate_dashboard_summary(
            gates_by_asset=replay_gates_by_asset,
            generic_gate=generic_gate,
            live_asset_symbols=[
                asset_symbol
                for asset_symbol, mode in mode_summary["modes"].items()
                if mode == CRYPTO_ASSET_MODE_LIVE
            ],
            displayed_asset_symbols=asset_symbols,
        )
        market_payloads: list[dict[str, Any]] = []
        for market in markets:
            asset_symbol = normalize_asset_symbol(market.asset_symbol)
            market_gate = replay_gates_by_asset.get(asset_symbol, generic_gate)
            signal_payload = _crypto_signal_payload_with_current_quote_metrics(
                signal_payloads.get(market.market_ticker),
                market=market,
                settings=self.settings,
                crypto_policy=crypto_policy,
            )
            live_status = self.asset_control_service.market_live_status(
                control=control,
                replay_gate=market_gate,
                market=market,
                has_write_credentials=self.kalshi.write_credentials is not None,
                crypto_policy=crypto_policy,
            )
            market_payloads.append(
                {
                    **market.to_payload(),
                    **live_status,
                    "replay_gate": _artifact_summary(market_gate),
                    "signal": signal_payload,
                    "active_room": active_rooms.get(market.market_ticker),
                }
            )
        global_live_blockers = sorted(
            {
                blocker
                for market_payload in market_payloads
                if market_payload.get("asset_mode") == CRYPTO_ASSET_MODE_LIVE
                for blocker in (market_payload.get("global_live_blockers") or [])
            }
        )
        signal_summary = _crypto_dashboard_signal_summary(market_payloads)
        return {
            "market_domain": "crypto",
            "frequency": dashboard_frequency,
            "source": source,
            "total_open_markets": total_open_markets,
            "current_only": current_only,
            "settings": {
                "crypto_enabled": self.settings.crypto_enabled,
                "crypto_15m_enabled": self.settings.crypto_15m_enabled,
                "crypto_1h_enabled": self.settings.crypto_1h_enabled,
                "crypto_trading_enabled": self.settings.crypto_trading_enabled,
                "crypto_autonomy_enabled": self.settings.crypto_autonomy_enabled,
                "crypto_order_mode": self.settings.crypto_order_mode,
                "runtime_crypto_trading_enabled": crypto_policy.trading_enabled,
                "runtime_crypto_production_autonomy_enabled": crypto_policy.production_autonomy_enabled,
            },
            "asset_modes": mode_summary["modes"],
            "asset_mode_counts": mode_summary["counts"],
            "global_live_blockers": global_live_blockers,
            "signal_summary": signal_summary,
            "replay_gate": replay_gate_summary,
            "generic_replay_gate": _artifact_summary(generic_gate),
            "markets": market_payloads,
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def create_room_for_market(self, market_ticker: str, *, reason: str = "crypto_dashboard") -> dict[str, Any]:
        market = await self.get_market(market_ticker, persist=True)
        frequency_label = crypto_frequency_label(market.frequency)
        strategy_code = crypto_strategy_code_for_frequency(market.frequency)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.ensure_deployment_control(self.settings.app_color)
            pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(pack)
            gate = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
            )
            live_status = self.asset_control_service.market_live_status(
                control=control,
                replay_gate=gate,
                market=market,
                has_write_credentials=self.kalshi.write_credentials is not None,
                crypto_policy=crypto_policy,
            )
            shadow_mode = self.settings.app_shadow_mode or not live_status["live_eligible"]
            room = await repo.create_room(
                RoomCreate(
                    name=f"{market.asset_symbol} {frequency_label} Crypto",
                    market_ticker=market.market_ticker,
                    prompt=(
                        f"Crypto {frequency_label} workflow. "
                        f"asset={market.asset_symbol} target={_money_text(market.target_price_dollars)} "
                        f"close_time={market.close_time.isoformat() if market.close_time else 'unknown'} "
                        f"asset_mode={live_status['asset_mode']} live_eligible={live_status['live_eligible']} "
                        f"control_asset_mode={live_status['control_asset_mode']} "
                        f"reason={reason}"
                    ),
                ),
                active_color=control.active_color,
                shadow_mode=shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                room_origin=RoomOrigin.SHADOW.value if shadow_mode else RoomOrigin.LIVE.value,
                agent_pack_version=pack.version,
            )
            await repo.save_artifact(
                room_id=room.id,
                artifact_type="market_snapshot",
                source="crypto_market_service",
                title=f"{market.asset_symbol} crypto market snapshot",
                payload={
                    "market_domain": "crypto",
                    "frequency": market.frequency,
                    "strategy_code": strategy_code,
                    "asset_mode": live_status["asset_mode"],
                    "control_asset_mode": live_status["control_asset_mode"],
                    "live_eligible": live_status["live_eligible"],
                    "live_blockers": live_status["live_blockers"],
                    "global_live_blockers": live_status["global_live_blockers"],
                    "reason": reason,
                    "market": market.to_payload(),
                },
            )
            await session.commit()
        return {
            "room_id": room.id,
            "redirect": f"/rooms/{room.id}",
            "market_ticker": market.market_ticker,
            "asset_symbol": market.asset_symbol,
            "asset_mode": live_status["asset_mode"],
            "live_eligible": live_status["live_eligible"],
            "live_blockers": live_status["live_blockers"],
        }

    async def status(self, *, frequency: str = "15m", asset_symbols: list[str] | None = None) -> dict[str, Any]:
        requested_assets = normalize_asset_symbols(asset_symbols)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_latest_crypto_market_snapshots(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
            )
            model = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="backtest",
                kalshi_env=self.settings.kalshi_env,
            )
            all_snapshots = await repo.list_crypto_market_snapshots(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                limit=100_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                limit=200_000,
            )
            spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                repo,
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                requested_assets=requested_assets,
                since=None,
                limit=500_000,
            )
            shadow_evidence = await _crypto_shadow_evidence_counts(
                session,
                kalshi_env=self.settings.kalshi_env,
                market_tickers={row.market_ticker for row in all_snapshots},
            )
            active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            await session.commit()
        snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
        all_snapshots = _filter_crypto_snapshot_rows(all_snapshots, requested_assets)
        candles = _filter_crypto_snapshot_rows(candles, requested_assets)
        spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
        quote_rows = _crypto_decision_rows(all_snapshots, candles, spot_rows, settings=self.settings)
        if len(requested_assets) == 1:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                model = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=normalize_frequency(frequency) or "15m",
                    artifact_type="model",
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbol=requested_assets[0],
                    prefer_generic=self.settings.crypto_model_nightly_pooled_only,
                )
                gate = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=normalize_frequency(frequency) or "15m",
                    artifact_type="replay_gate",
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbol=requested_assets[0],
                )
                backtest = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=normalize_frequency(frequency) or "15m",
                    artifact_type="backtest",
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbol=requested_assets[0],
                )
                await session.commit()
        elif model is None or gate is None or backtest is None:
            # Per-asset training mode: no aggregate artifact exists, fall back to per-asset lookup.
            # Iterate assets until all three are found (first asset that has a full set wins).
            _fallback_assets = requested_assets or sorted({s.asset_symbol for s in snapshots})
            if _fallback_assets:
                async with self.session_factory() as session:
                    repo = PlatformRepository(session)
                    for _asset in _fallback_assets:
                        if model is None:
                            model = await _latest_crypto_artifact_for_asset(
                                repo,
                                frequency=normalize_frequency(frequency) or "15m",
                                artifact_type="model",
                                kalshi_env=self.settings.kalshi_env,
                                asset_symbol=_asset,
                                allow_generic_fallback=False,
                            )
                        if gate is None:
                            gate = await _latest_crypto_artifact_for_asset(
                                repo,
                                frequency=normalize_frequency(frequency) or "15m",
                                artifact_type="replay_gate",
                                kalshi_env=self.settings.kalshi_env,
                                asset_symbol=_asset,
                                allow_generic_fallback=False,
                            )
                        if backtest is None:
                            backtest = await _latest_crypto_artifact_for_asset(
                                repo,
                                frequency=normalize_frequency(frequency) or "15m",
                                artifact_type="backtest",
                                kalshi_env=self.settings.kalshi_env,
                                asset_symbol=_asset,
                                allow_generic_fallback=False,
                            )
                        if model is not None and gate is not None and backtest is not None:
                            break
                    await session.commit()
        asset_symbols = sorted({snapshot.asset_symbol for snapshot in snapshots})
        mode_summary = self.asset_control_service.asset_mode_summary(
            asset_symbols=asset_symbols,
            modes=_resolved_crypto_asset_modes(
                asset_symbols=asset_symbols,
                note_modes=self.asset_control_service.modes_from_notes(control.notes),
                crypto_policy=crypto_policy,
            ),
        )
        data_quality = _crypto_data_quality(
            all_snapshots,
            candles,
            min_training_samples=self.settings.crypto_min_training_samples,
        )
        spot_quality = _crypto_spot_quality(
            spot_rows,
            expected_assets=(
                requested_assets
                if requested_assets
                else _crypto_expected_spot_assets(self.settings, observed_assets={row.asset_symbol for row in all_snapshots})
            ),
            min_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
            settings=self.settings,
        )
        latest_snapshot_at = max((row.observed_at for row in all_snapshots), default=None)
        latest_candle_at = max((row.end_period_ts for row in candles), default=None)
        latest_spot_at = max((_as_utc_datetime(row.end_ts) for row in spot_rows), default=None)
        quote_evidence = _crypto_quote_evidence_summary(all_snapshots, quote_rows, settings=self.settings)
        backtest_metrics = (backtest.metrics if backtest is not None else {}) or {}
        current_model_candidates = int(
            backtest_metrics.get("current_model_live_quality_candidate_count", backtest_metrics.get("trade_candidate_count")) or 0
        )
        quote_evidence["trade_candidate_count"] = current_model_candidates
        quote_evidence["current_model_live_quality_candidate_count"] = current_model_candidates
        quote_evidence["strict_trade_candidate_min_required"] = crypto_policy.replay_min_trade_candidates
        return {
            "market_domain": "crypto",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": normalize_frequency(frequency) or "15m",
            "app_color": self.settings.app_color,
            "active_color": control.active_color,
            "is_active_color": control.active_color == self.settings.app_color,
            "app_shadow_mode": self.settings.app_shadow_mode,
            "has_write_credentials": self.kalshi.write_credentials is not None,
            "active_pack_version": active_pack.version,
            "crypto_enabled": self.settings.crypto_enabled,
            "crypto_15m_enabled": self.settings.crypto_15m_enabled,
            "crypto_1h_enabled": self.settings.crypto_1h_enabled,
            "crypto_trading_enabled": self.settings.crypto_trading_enabled,
            "crypto_autonomy_enabled": self.settings.crypto_autonomy_enabled,
            "runtime_crypto_trading_enabled": crypto_policy.trading_enabled,
            "runtime_crypto_production_autonomy_enabled": crypto_policy.production_autonomy_enabled,
            "stored_market_count": len(snapshots),
            "asset_modes": mode_summary["modes"],
            "asset_mode_counts": mode_summary["counts"],
            "global_live_blockers": self.asset_control_service.global_live_blockers(
                control=control,
                replay_gate=gate,
                has_write_credentials=self.kalshi.write_credentials is not None,
                frequency=frequency,
                crypto_policy=crypto_policy,
            ),
            "model": _artifact_summary(model),
            "backtest": _artifact_summary(backtest),
            "replay_gate": _artifact_summary(gate),
            "data_quality": data_quality,
            "spot_quality": spot_quality,
            "quote_evidence": quote_evidence,
            "data_freshness": {
                "latest_snapshot_observed_at": latest_snapshot_at.isoformat() if latest_snapshot_at else None,
                "latest_candle_at": latest_candle_at.isoformat() if latest_candle_at else None,
                "latest_spot_end_ts": latest_spot_at.isoformat() if latest_spot_at else None,
                "stale_spot_assets": spot_quality.get("stale_assets") or [],
            },
            "shadow_evidence": shadow_evidence,
            "readiness_score": _crypto_readiness_score(
                settings=self.settings,
                data_quality=data_quality,
                spot_quality=spot_quality,
                shadow_evidence=shadow_evidence,
                model=_artifact_summary(model),
                backtest=_artifact_summary(backtest),
                gate=_artifact_summary(gate),
                global_live_blockers=self.asset_control_service.global_live_blockers(
                    control=control,
                    replay_gate=gate,
                    has_write_credentials=self.kalshi.write_credentials is not None,
                    frequency=frequency,
                    crypto_policy=crypto_policy,
                ),
                active_color=control.active_color,
            ),
        }

    async def is_crypto_room(self, room_id: str) -> bool:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room = await repo.get_room(room_id)
            if room is None:
                return False
            artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="market_snapshot")
            if artifact is not None and (artifact.payload or {}).get("market_domain") == "crypto":
                return True
            snapshot = await repo.get_latest_crypto_market_snapshot(room.market_ticker, kalshi_env=room.kalshi_env)
            await session.commit()
        return snapshot is not None


def _crypto_order_book_levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    levels: list[tuple[Decimal, Decimal]] = []
    if not isinstance(raw, list):
        return levels
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            price_cents = Decimal(str(item[0]))
            count = Decimal(str(item[1]))
        except Exception:
            continue
        if count <= 0:
            continue
        levels.append((price_cents, count))
    return levels


def _crypto_order_book_depth_metrics(
    orderbook: Any,
    *,
    top_levels: int = CRYPTO_ORDER_BOOK_TOP_LEVELS,
) -> dict[str, Any]:
    """Summarize a Kalshi order book from the YES-side perspective.

    Kalshi returns resting YES bids under ``yes`` and resting NO bids under
    ``no``; a NO bid at price ``p`` cents is a YES ask at ``100 - p`` cents.
    Depths are contract counts summed over the top ``top_levels`` levels of
    each side; depth_imbalance is None when both sides are empty.
    """
    yes_levels = _crypto_order_book_levels(orderbook.get("yes") if isinstance(orderbook, dict) else None)
    no_levels = _crypto_order_book_levels(orderbook.get("no") if isinstance(orderbook, dict) else None)
    yes_levels.sort(key=lambda level: level[0], reverse=True)
    no_levels.sort(key=lambda level: level[0], reverse=True)
    best_bid = yes_levels[0][0] / Decimal("100") if yes_levels else None
    best_ask = (Decimal("100") - no_levels[0][0]) / Decimal("100") if no_levels else None
    mid = (best_bid + best_ask) / Decimal("2") if best_bid is not None and best_ask is not None else None
    spread_bps = (
        int(((best_ask - best_bid) * Decimal("10000")).to_integral_value())
        if best_bid is not None and best_ask is not None
        else None
    )
    bid_depth = sum((count for _price, count in yes_levels[:top_levels]), Decimal("0"))
    ask_depth = sum((count for _price, count in no_levels[:top_levels]), Decimal("0"))
    total_depth = bid_depth + ask_depth
    depth_imbalance = (
        ((bid_depth - ask_depth) / total_depth).quantize(Decimal("0.00000001"))
        if total_depth > 0
        else None
    )
    return {
        "best_bid_dollars": best_bid,
        "best_ask_dollars": best_ask,
        "mid_dollars": mid,
        "spread_bps": spread_bps,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "depth_imbalance": depth_imbalance,
    }


class CryptoHistoryService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        market_service: CryptoMarketService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.market_service = market_service
        self._order_book_last_fetch_monotonic: dict[str, float] = {}

    async def bootstrap(
        self,
        *,
        days: int | None = None,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        lookback_days = days or self.settings.crypto_history_lookback_days
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        requested_assets = set(normalize_asset_symbols(asset_symbols))
        live_markets = await self.market_service.discover_markets(
            frequency=frequency,
            status="open",
            persist=True,
            asset_symbols=sorted(requested_assets) or None,
        )
        if requested_assets:
            live_markets = [
                market for market in live_markets if normalize_asset_symbol(market.asset_symbol) in requested_assets
            ]
        historical_markets: list[CryptoMarket] = []
        series_rows = await self.market_service.discover_series(frequency=frequency)
        if requested_assets:
            series_rows = [
                series for series in series_rows if normalize_asset_symbol(series.asset_symbol) in requested_assets
            ]
        errors: list[dict[str, str]] = []
        series_stats: list[dict[str, Any]] = []
        for series in series_rows:
            result = await self._list_historical_markets(
                series.series_ticker,
                cutoff=cutoff,
                frequency=frequency,
                series=series,
            )
            errors.extend({"series_ticker": series.series_ticker, "error": error} for error in result["errors"])
            markets_in_window = 0
            for row in result["rows"]:
                parsed = parse_crypto_market(row, series=series, frequency=frequency)
                if parsed is None:
                    continue
                if parsed.close_time is None or parsed.close_time >= cutoff:
                    markets_in_window += 1
                    historical_markets.append(parsed)
            series_stats.append(
                {
                    "series_ticker": series.series_ticker,
                    "asset_symbol": series.asset_symbol,
                    "pages_fetched": result["pages_fetched"],
                    "rows_seen": result["rows_seen"],
                    "markets_in_window": markets_in_window,
                    "errors": result["errors"],
                }
            )
        all_markets = {market.market_ticker: market for market in [*historical_markets, *live_markets]}
        historical_tickers = {market.market_ticker for market in historical_markets}
        candle_stats: dict[str, Any] = {
            "stored": 0,
            "markets_attempted": 0,
            "markets_skipped_existing": 0,
            "errors": [],
            "concurrency": max(1, int(self.settings.crypto_history_candle_concurrency)),
        }
        commit_batch_size = 250
        market_items = list(all_markets.values())
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for index, market in enumerate(market_items, start=1):
                await self.market_service.record_market_snapshot(
                    repo,
                    market,
                    source_kind="historical" if market.market_ticker in historical_tickers else "live",
                    observed_at=market.close_time or datetime.now(UTC),
                )
                if index % commit_batch_size == 0:
                    await session.commit()
            await session.commit()
            captures = await self._capture_candles_for_markets(
                session,
                repo,
                market_items,
                cutoff=cutoff,
                commit_batch_size=commit_batch_size,
                frequency=frequency,
            )
            for _market, capture in captures:
                candle_stats["stored"] += int(capture["stored"])
                if capture["status"] == "skipped_existing":
                    candle_stats["markets_skipped_existing"] += 1
                else:
                    candle_stats["markets_attempted"] += 1
                if capture.get("error"):
                    candle_stats["errors"].append({"market_ticker": _market.market_ticker, "error": capture["error"]})
            await session.commit()
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=100_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=200_000,
            )
            if requested_assets:
                snapshots = [
                    row for row in snapshots if normalize_asset_symbol(row.asset_symbol) in requested_assets
                ]
                candles = [row for row in candles if normalize_asset_symbol(row.asset_symbol) in requested_assets]
            await session.commit()
        return {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": normalize_frequency(frequency) or "15m",
            "asset_symbols": sorted(requested_assets),
            "lookback_days": lookback_days,
            "markets_stored": len(all_markets),
            "live_markets": len(live_markets),
            "historical_markets": len(historical_markets),
            "candles_stored": candle_stats["stored"],
            "candle_capture": {
                **candle_stats,
                "errors": candle_stats["errors"][:10],
                "error_count": len(candle_stats["errors"]),
            },
            "series": series_stats,
            "pages_fetched": sum(int(item["pages_fetched"]) for item in series_stats),
            "historical_rows_seen": sum(int(item["rows_seen"]) for item in series_stats),
            "data_quality": _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            ),
            "errors": errors[:10],
        }

    async def daily(self, *, frequency: str = "15m") -> dict[str, Any]:
        return await self.bootstrap(days=2, frequency=frequency)

    async def collect_settled(
        self,
        *,
        days: int | None = 2,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        capture_candles: bool | None = None,
        summarize_quality: bool = True,
        propagate_settlement_labels: bool = True,
    ) -> dict[str, Any]:
        """Collect recently settled crypto markets as immutable label snapshots."""
        freq = normalize_frequency(frequency) or "15m"
        lookback_days = days if days and days > 0 else 2
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        requested_assets = set(normalize_asset_symbols(asset_symbols))
        series_rows = await self.market_service.discover_series(frequency=freq)
        if requested_assets:
            series_rows = [
                series for series in series_rows if normalize_asset_symbol(series.asset_symbol) in requested_assets
            ]

        settled_markets: list[CryptoMarket] = []
        errors: list[dict[str, str]] = []
        series_stats: list[dict[str, Any]] = []
        expected_assets = sorted(requested_assets or {normalize_asset_symbol(series.asset_symbol) for series in series_rows})
        for series in series_rows:
            result = await self._list_settled_markets(series, cutoff=cutoff, frequency=freq)
            errors.extend({"series_ticker": series.series_ticker, "error": error} for error in result["errors"])
            settled_markets.extend(result["markets"])
            series_stats.append(
                {
                    "series_ticker": series.series_ticker,
                    "asset_symbol": series.asset_symbol,
                    "pages_fetched": result["pages_fetched"],
                    "rows_seen": result["rows_seen"],
                    "markets_in_window": len(result["markets"]),
                    "errors": result["errors"],
                }
            )

        capture_settled_candles = (
            bool(self.settings.crypto_collect_settled_candles_enabled)
            if capture_candles is None
            else bool(capture_candles)
        )
        candle_stats: dict[str, Any] = {
            "stored": 0,
            "markets_attempted": 0,
            "markets_skipped_existing": 0,
            "errors": [],
            "source_counts": {},
            "enabled": capture_settled_candles,
            "concurrency": max(1, int(self.settings.crypto_history_candle_concurrency)),
        }
        asset_counts: Counter[str] = Counter({asset: 0 for asset in expected_assets})
        settlement_labels_propagated = 0
        settlement_label_candidates = 0
        settlement_label_live_ticker_count = 0
        commit_batch_size = 250
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            settled_snapshot_batch: list[dict[str, Any]] = []
            for market in settled_markets:
                settled_snapshot_batch.append(
                    {
                        "kalshi_env": self.settings.kalshi_env,
                        "series_ticker": market.series_ticker,
                        "market_ticker": market.market_ticker,
                        "event_ticker": market.event_ticker,
                        "asset_symbol": market.asset_symbol,
                        "frequency": market.frequency,
                        "title": market.title,
                        "status": market.status,
                        "open_time": market.open_time,
                        "close_time": market.close_time,
                        "expected_expiration_time": market.expected_expiration_time,
                        "target_price_dollars": market.target_price_dollars,
                        "yes_bid_dollars": market.yes_bid_dollars,
                        "yes_ask_dollars": market.yes_ask_dollars,
                        "no_bid_dollars": market.no_bid_dollars,
                        "no_ask_dollars": market.no_ask_dollars,
                        "last_price_dollars": market.last_price_dollars,
                        "volume": market.volume,
                        "open_interest": market.open_interest,
                        "settlement_result": market.settlement_result,
                        "observed_at": _crypto_settlement_observed_at(market),
                        "source_kind": "settled_backfill",
                        "payload": market.to_payload(),
                    }
                )
                asset_counts[market.asset_symbol] += 1
                if len(settled_snapshot_batch) >= commit_batch_size:
                    await repo.bulk_record_crypto_market_snapshots(
                        settled_snapshot_batch,
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()
                    settled_snapshot_batch.clear()
            if settled_snapshot_batch:
                await repo.bulk_record_crypto_market_snapshots(
                    settled_snapshot_batch,
                    kalshi_env=self.settings.kalshi_env,
                )
            await session.commit()
            if propagate_settlement_labels:
                settlement_labels = {
                    market.market_ticker: str(market.settlement_result or "").lower()
                    for market in settled_markets
                    if str(market.settlement_result or "").lower() in {"yes", "no"}
                    and _settled_label_matches_requested_duration(market, freq)
                }
                settlement_label_items = list(settlement_labels.items())
                settlement_label_candidates = len(settlement_label_items)
                settlement_label_live_ticker_count = settlement_label_candidates
                label_propagation_batch_size = 250
                for offset in range(0, len(settlement_label_items), label_propagation_batch_size):
                    batch = dict(settlement_label_items[offset : offset + label_propagation_batch_size])
                    settlement_labels_propagated += await repo.update_crypto_snapshot_settlement_results(
                        batch,
                        kalshi_env=self.settings.kalshi_env,
                        frequency=freq,
                        observed_since=cutoff,
                        require_quote_path=True,
                    )
                    await session.commit()
                await session.commit()
            captures = []
            if capture_settled_candles:
                captures = await self._capture_candles_for_markets(
                    session,
                    repo,
                    settled_markets,
                    cutoff=cutoff,
                    commit_batch_size=commit_batch_size,
                    frequency=freq,
                )
            else:
                candle_stats["skipped_reason"] = "crypto_collect_settled_candles_disabled"
            for market, capture in captures:
                candle_stats["stored"] += int(capture.get("stored") or 0)
                if capture.get("status") == "skipped_existing":
                    candle_stats["markets_skipped_existing"] += 1
                else:
                    candle_stats["markets_attempted"] += 1
                source = str(capture.get("source") or "unknown")
                candle_stats["source_counts"][source] = int(candle_stats["source_counts"].get(source, 0)) + 1
                if capture.get("error"):
                    candle_stats["errors"].append(
                        {
                            "market_ticker": market.market_ticker,
                            "error": capture["error"],
                            "attempted_sources": capture.get("attempted_sources") or [],
                        }
                    )
            await session.commit()
            snapshots: list[CryptoMarketSnapshotRecord] = []
            candles: list[CryptoMarketCandlestickRecord] = []
            if summarize_quality:
                snapshots = await repo.list_crypto_market_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    since=cutoff,
                    limit=200_000,
                )
                candles = await repo.list_crypto_market_candlesticks(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    since=cutoff,
                    limit=500_000,
                )
                if requested_assets:
                    snapshots = [
                        row for row in snapshots if normalize_asset_symbol(row.asset_symbol) in requested_assets
                    ]
                    candles = [row for row in candles if normalize_asset_symbol(row.asset_symbol) in requested_assets]
            await session.commit()
        if summarize_quality:
            assets_missing_settled = _crypto_assets_missing_settled_markets(
                snapshots,
                expected_assets=expected_assets,
            )
            data_quality = _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            )
        else:
            assets_missing_settled = sorted(asset for asset in expected_assets if int(asset_counts.get(asset) or 0) <= 0)
            data_quality = _crypto_lightweight_settled_data_quality(
                expected_assets=expected_assets,
                asset_counts=asset_counts,
                settled_snapshot_count=len(settled_markets),
                candle_count=candle_stats["stored"],
            )
        return {
            "status": "ok" if settled_markets else "warn",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbols": expected_assets,
            "lookback_days": lookback_days,
            "settled_markets_stored": len(settled_markets),
            "settlement_labels_propagated": settlement_labels_propagated,
            "settlement_label_candidates": settlement_label_candidates,
            "settlement_label_live_ticker_count": settlement_label_live_ticker_count,
            "settlement_label_propagation_skipped": not propagate_settlement_labels,
            "asset_counts": dict(sorted(asset_counts.items())),
            "assets_missing_settled_markets": assets_missing_settled,
            "candles_stored": candle_stats["stored"],
            "candle_capture": {
                **candle_stats,
                "errors": candle_stats["errors"][:10],
                "error_count": len(candle_stats["errors"]),
            },
            "series": series_stats,
            "pages_fetched": sum(int(item["pages_fetched"]) for item in series_stats),
            "settled_rows_seen": sum(int(item["rows_seen"]) for item in series_stats),
            "data_quality": data_quality,
            "errors": errors[:10],
        }

    async def collect_open(self, *, frequency: str = "15m", asset_symbols: list[str] | None = None) -> dict[str, Any]:
        """Collect lightweight executable quote evidence for currently open crypto markets.

        This deliberately avoids historical pagination and candlestick capture. Strict
        replay evidence needs real bid/ask rows; candles stay prediction-only.
        """
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = set(normalize_asset_symbols(asset_symbols))
        observed_at = datetime.now(UTC)
        market_status = "open"
        min_close_time: datetime | None = None
        max_close_time: datetime | None = None
        if freq == "1h":
            market_status = None
            min_close_time = observed_at - timedelta(minutes=1)
            max_close_time = observed_at + timedelta(hours=1, minutes=5)
        markets = await self.market_service.discover_markets(
            frequency=freq,
            status=market_status,
            persist=False,
            asset_symbols=sorted(requested_assets) or None,
            min_close_time=min_close_time,
            max_close_time=max_close_time,
        )
        if requested_assets:
            markets = [market for market in markets if normalize_asset_symbol(market.asset_symbol) in requested_assets]
        stored = 0
        order_books_stored = 0
        order_book_errors: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        asset_counts: Counter[str] = Counter()
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            for market in markets:
                if market.open_time is not None and observed_at < market.open_time:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "market_not_open",
                        }
                    )
                    continue
                if market.close_time is not None and observed_at >= market.close_time:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "market_closed",
                        }
                    )
                    continue
                order_book_status = await self._maybe_capture_order_book(
                    repo,
                    market,
                    frequency=freq,
                    observed_at=observed_at,
                )
                if order_book_status == "stored":
                    order_books_stored += 1
                elif order_book_status.startswith("error"):
                    order_book_errors.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "error": order_book_status,
                        }
                    )
                if market.yes_bid_dollars is None or market.yes_ask_dollars is None:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "missing_real_bid_ask",
                        }
                    )
                    continue
                await self.market_service.record_market_snapshot(
                    repo,
                    market,
                    source_kind="live_quote_evidence",
                    observed_at=observed_at,
                )
                stored += 1
                asset_counts[market.asset_symbol] += 1
            await session.commit()
            recent_snapshots = await repo.list_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=observed_at - timedelta(minutes=30),
                limit=5000,
                match_frequency_duration=True,
            )
            await session.commit()
        return {
            "status": "ok" if stored else "warn",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbols": sorted(requested_assets),
            "observed_at": observed_at.isoformat(),
            "checked_markets": len(markets),
            "stored_real_quote_snapshots": stored,
            "stored_order_book_snapshots": order_books_stored,
            "order_book_errors": order_book_errors[:20],
            "skipped_count": len(skipped),
            "skipped": skipped[:20],
            "asset_counts": dict(sorted(asset_counts.items())),
            "recent_quote_evidence": _crypto_quote_evidence_summary(recent_snapshots, [], settings=self.settings),
        }

    async def _maybe_capture_order_book(
        self,
        repo: PlatformRepository,
        market: CryptoMarket,
        *,
        frequency: str,
        observed_at: datetime,
    ) -> str:
        """Capture a REST order-book snapshot for an open market, throttled per market."""
        if not self.settings.crypto_orderbook_collect_enabled:
            return "disabled"
        interval = max(0, int(self.settings.crypto_orderbook_collect_interval_seconds))
        now_monotonic = time.monotonic()
        last_fetch = self._order_book_last_fetch_monotonic.get(market.market_ticker)
        if last_fetch is not None and now_monotonic - last_fetch < interval:
            return "throttled"
        self._order_book_last_fetch_monotonic[market.market_ticker] = now_monotonic
        if len(self._order_book_last_fetch_monotonic) > 1024:
            cutoff = now_monotonic - 3600.0
            self._order_book_last_fetch_monotonic = {
                ticker: fetched_at
                for ticker, fetched_at in self._order_book_last_fetch_monotonic.items()
                if fetched_at >= cutoff
            }
        try:
            response = await self.kalshi.get_market_orderbook(market.market_ticker)
        except Exception as exc:
            logger.warning(
                "crypto order book fetch failed",
                extra={"market_ticker": market.market_ticker, "error": str(exc)},
            )
            return f"error: {exc}"
        orderbook = response.get("orderbook") if isinstance(response, dict) else None
        metrics = _crypto_order_book_depth_metrics(orderbook, top_levels=CRYPTO_ORDER_BOOK_TOP_LEVELS)
        await repo.upsert_crypto_order_book_snapshot(
            kalshi_env=self.settings.kalshi_env,
            provider="kalshi",
            asset_symbol=market.asset_symbol,
            frequency=frequency,
            market_ticker=market.market_ticker,
            source_kind="rest_orderbook",
            source_id=market.market_ticker,
            observed_at=observed_at,
            best_bid_dollars=metrics["best_bid_dollars"],
            best_ask_dollars=metrics["best_ask_dollars"],
            mid_dollars=metrics["mid_dollars"],
            spread_bps=metrics["spread_bps"],
            bid_depth=metrics["bid_depth"],
            ask_depth=metrics["ask_depth"],
            depth_imbalance=metrics["depth_imbalance"],
            payload={
                "orderbook": orderbook if isinstance(orderbook, dict) else {},
                "top_levels": CRYPTO_ORDER_BOOK_TOP_LEVELS,
            },
        )
        return "stored"

    async def status(
        self,
        *,
        frequency: str = "15m",
        days: int | float | None = None,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=cutoff,
                limit=200_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=cutoff,
                limit=500_000,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=cutoff,
                limit=1_000_000,
            )
            await session.commit()
        expected_assets = requested_assets or _crypto_expected_spot_assets(
            self.settings,
            observed_assets={row.asset_symbol for row in snapshots} | {row.asset_symbol for row in candles},
        )
        quote_rows = _crypto_decision_rows(snapshots, candles, spot_rows, settings=self.settings)
        return {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "data_quality": _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            ),
            "spot_quality": _crypto_spot_quality(
                spot_rows,
                expected_assets=expected_assets,
                min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
                settings=self.settings,
            ),
            "quote_evidence": _crypto_quote_evidence_summary(snapshots, quote_rows, settings=self.settings),
        }

    async def _list_historical_markets(
        self,
        series_ticker: str,
        *,
        cutoff: datetime | None = None,
        frequency: str = "15m",
        series: CryptoSeries | None = None,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages_fetched = 0
        for _ in range(100):
            params: dict[str, Any] = {"series_ticker": series_ticker, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            try:
                response = await self.kalshi.list_historical_markets(**params)
            except httpx.HTTPError as exc:
                errors.append(str(exc))
                break
            page_rows = _rows_from_response(response, "markets")
            rows.extend(page_rows)
            pages_fetched += 1
            if cutoff is not None and series is not None and self.settings.crypto_historical_pagination_stop_at_cutoff:
                parsed_page = [
                    parsed
                    for row in page_rows
                    if (parsed := parse_crypto_market(row, series=series, frequency=frequency)) is not None
                ]
                if parsed_page and all(
                    (market.close_time or market.expected_expiration_time) is not None
                    and (market.close_time or market.expected_expiration_time) < cutoff
                    for market in parsed_page
                ):
                    break
            cursor = response.get("cursor") or response.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        return {
            "rows": rows,
            "rows_seen": len(rows),
            "pages_fetched": pages_fetched,
            "errors": errors,
        }

    async def _list_settled_markets(
        self,
        series: CryptoSeries,
        *,
        cutoff: datetime,
        frequency: str,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        markets: list[CryptoMarket] = []
        errors: list[str] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages_fetched = 0
        for _ in range(100):
            params: dict[str, Any] = {
                "series_ticker": series.series_ticker,
                "status": "settled",
                "limit": 1000,
                "min_close_ts": int(cutoff.timestamp()),
            }
            if cursor:
                params["cursor"] = cursor
            try:
                response = await self.kalshi.list_markets(**params)
            except httpx.HTTPError as exc:
                errors.append(str(exc))
                break
            page_rows = _rows_from_response(response, "markets")
            rows.extend(page_rows)
            pages_fetched += 1
            parsed_page: list[CryptoMarket] = []
            for row in page_rows:
                parsed = parse_crypto_market(row, series=series, frequency=frequency)
                if parsed is None:
                    continue
                parsed_page.append(parsed)
                close_time = parsed.close_time or parsed.expected_expiration_time
                if close_time is None or close_time >= cutoff:
                    markets.append(parsed)
            if parsed_page and all(
                (market.close_time or market.expected_expiration_time) is not None
                and (market.close_time or market.expected_expiration_time) < cutoff
                for market in parsed_page
            ):
                if self.settings.crypto_settled_pagination_stop_at_cutoff:
                    break
                logger.debug(
                    "crypto settled page for %s is older than cutoff; continuing pagination because ordering is not guaranteed",
                    series.series_ticker,
                )
            cursor = response.get("cursor") or response.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        return {
            "rows": rows,
            "markets": markets,
            "rows_seen": len(rows),
            "pages_fetched": pages_fetched,
            "errors": errors,
        }

    async def _fetch_candle_rows(self, market: CryptoMarket, *, cutoff: datetime) -> dict[str, Any]:
        now = datetime.now(UTC)
        end_time = min(now, market.close_time or market.expected_expiration_time or now)
        if market.close_time is not None and market.close_time < now:
            end_time = min(now, market.close_time + timedelta(minutes=1))
        start_time = market.open_time or (end_time - timedelta(minutes=20))
        start_time = max(cutoff, start_time)
        if start_time >= end_time:
            start_time = end_time - timedelta(minutes=20)
        params = {
            "period_interval": 1,
            "start_ts": int(start_time.timestamp()),
            "end_ts": int(end_time.timestamp()),
        }
        closed = market.close_time is not None and market.close_time < now
        sources = ("live", "historical") if closed else ("live",)
        response: dict[str, Any] | None = None
        selected_source = "unknown"
        errors: list[dict[str, str]] = []
        for source in sources:
            try:
                if source == "live":
                    candidate_response = await self.kalshi.get_market_candlesticks(
                        market.series_ticker,
                        market.market_ticker,
                        **params,
                    )
                else:
                    candidate_response = await self.kalshi.get_historical_market_candlesticks(
                        market.series_ticker,
                        market.market_ticker,
                        **params,
                    )
            except httpx.HTTPError as exc:
                errors.append({"source": source, "error": str(exc)})
                continue
            candidate_rows = _rows_from_response(candidate_response, "candlesticks") or _rows_from_response(
                candidate_response,
                "candles",
            )
            response = candidate_response
            selected_source = source
            if candidate_rows or source == sources[-1]:
                break
            errors.append({"source": source, "error": "empty_candles"})
        if response is None:
            logger.info("crypto candlestick capture skipped for %s", market.market_ticker, extra={"errors": errors})
            return {
                "status": "error",
                "stored": 0,
                "candles": [],
                "source": selected_source,
                "attempted_sources": list(sources),
                "error": "; ".join(f"{item['source']}: {item['error']}" for item in errors),
            }
        candles: list[dict[str, Any]] = []
        for row in _rows_from_response(response, "candlesticks") or _rows_from_response(response, "candles"):
            candle = normalize_candlestick(row)
            if candle is None:
                continue
            candles.append(candle)
        return {
            "status": "ok",
            "stored": 0,
            "candles": candles,
            "source": selected_source,
            "attempted_sources": list(sources),
            "errors": errors,
        }

    async def _store_candle_rows(
        self,
        repo: PlatformRepository,
        market: CryptoMarket,
        capture: dict[str, Any],
    ) -> dict[str, Any]:
        count = 0
        for candle in capture.get("candles") or []:
            await repo.upsert_crypto_market_candlestick(
                kalshi_env=self.settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                period_interval=candle["period_interval"],
                end_period_ts=candle["end_period_ts"],
                open_dollars=candle["open_dollars"],
                high_dollars=candle["high_dollars"],
                low_dollars=candle["low_dollars"],
                close_dollars=candle["close_dollars"],
                volume=candle["volume"],
                payload=candle["payload"],
            )
            count += 1
        result = {key: value for key, value in capture.items() if key != "candles"}
        result["stored"] = count
        return result

    async def _capture_candles(self, repo: PlatformRepository, market: CryptoMarket, *, cutoff: datetime) -> dict[str, Any]:
        capture = await self._fetch_candle_rows(market, cutoff=cutoff)
        return await self._store_candle_rows(repo, market, capture)

    async def _capture_candles_for_markets(
        self,
        session: AsyncSession,
        repo: PlatformRepository,
        markets: list[CryptoMarket],
        *,
        cutoff: datetime,
        commit_batch_size: int,
        frequency: str | None = None,
    ) -> list[tuple[CryptoMarket, dict[str, Any]]]:
        concurrency = max(1, int(self.settings.crypto_history_candle_concurrency))
        captures: list[tuple[CryptoMarket, dict[str, Any]]] = []
        markets = self._partition_covered_markets(
            markets,
            captures,
            coverage=await self._candlestick_coverage_for_skip(repo, cutoff=cutoff, frequency=frequency, market_count=len(markets)),
        )
        stored_since_commit = 0
        for offset in range(0, len(markets), concurrency):
            batch = markets[offset : offset + concurrency]
            if concurrency <= 1:
                fetched = [(market, await self._capture_candles(repo, market, cutoff=cutoff)) for market in batch]
                captures.extend(fetched)
                stored_since_commit += len(fetched)
            else:
                fetched = await asyncio.gather(
                    *(self._fetch_candle_rows(market, cutoff=cutoff) for market in batch)
                )
                for market, capture in zip(batch, fetched, strict=True):
                    stored_capture = await self._store_candle_rows(repo, market, capture)
                    captures.append((market, stored_capture))
                    stored_since_commit += 1
            if stored_since_commit >= commit_batch_size:
                await session.commit()
                stored_since_commit = 0
        if stored_since_commit:
            await session.commit()
        return captures

    async def _candlestick_coverage_for_skip(
        self,
        repo: PlatformRepository,
        *,
        cutoff: datetime,
        frequency: str | None,
        market_count: int,
    ) -> dict[str, datetime]:
        if not self.settings.crypto_history_skip_existing_candles or market_count == 0:
            return {}
        return await repo.map_crypto_candlestick_coverage(
            kalshi_env=self.settings.kalshi_env,
            frequency=normalize_frequency(frequency) if frequency else None,
            since=cutoff,
        )

    def _partition_covered_markets(
        self,
        markets: list[CryptoMarket],
        captures: list[tuple[CryptoMarket, dict[str, Any]]],
        *,
        coverage: dict[str, datetime],
    ) -> list[CryptoMarket]:
        """Append skipped_existing captures for settled markets whose stored candles
        already reach their close time; return the markets still needing a fetch."""
        if not coverage:
            return markets
        now = datetime.now(UTC)
        pending: list[CryptoMarket] = []
        skipped = 0
        for market in markets:
            close_time = market.close_time
            covered_until = coverage.get(market.market_ticker)
            if covered_until is not None and covered_until.tzinfo is None:
                covered_until = covered_until.replace(tzinfo=UTC)
            if (
                close_time is not None
                and close_time < now
                and covered_until is not None
                and covered_until >= close_time - timedelta(minutes=1)
            ):
                skipped += 1
                captures.append(
                    (
                        market,
                        {
                            "status": "skipped_existing",
                            "stored": 0,
                            "source": "db_cache",
                            "attempted_sources": [],
                        },
                    )
                )
                continue
            pending.append(market)
        if skipped:
            logger.info(
                "crypto candlestick crawl skipping %d/%d markets with complete stored candles",
                skipped,
                len(markets),
            )
        return pending


class CryptoSpotService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    def _coinbase_client(self) -> CoinbaseSpotClient:
        credentials = None
        if self.settings.coinbase_advanced_trade_authenticated_enabled:
            credentials = load_coinbase_cdp_credentials(
                key_file=self.settings.coinbase_cdp_api_key_file,
                key_name=self.settings.coinbase_cdp_key_name,
                private_key=self.settings.coinbase_cdp_private_key,
            )
        return CoinbaseSpotClient(
            timeout_seconds=self.settings.crypto_spot_request_timeout_seconds,
            credentials=credentials,
        )

    def _kraken_client(self) -> KrakenSpotClient | None:
        if not self.settings.crypto_spot_kraken_enabled:
            return None
        return KrakenSpotClient(timeout_seconds=self.settings.crypto_spot_request_timeout_seconds)

    async def _collect_kraken_rows(
        self,
        repo: PlatformRepository,
        kraken: KrakenSpotClient | None,
        asset: str,
        *,
        frequency: str,
        interval_seconds: int,
        start: datetime,
        end: datetime,
        provider_stats: dict[str, dict[str, Any]],
    ) -> int:
        """Additive secondary venue; Kraken failures never affect the Coinbase path."""
        if kraken is None or not KRAKEN_PAIRS.get(asset):
            return 0
        try:
            rows = await kraken.fetch_ohlc(asset, start=start, end=end, interval_seconds=interval_seconds)
            if not rows:
                return 0
            stored = await self._store_rows(repo, rows, frequency=frequency, interval_seconds=interval_seconds)
        except Exception as exc:
            logger.debug("kraken spot collection failed", extra={"asset_symbol": asset, "error": str(exc)})
            provider_stats["kraken"]["errors"].append({"asset_symbol": asset, "error": str(exc)})
            return 0
        provider_stats["kraken"]["stored"] += stored
        provider_stats["kraken"]["assets"].append(asset)
        return stored

    async def coinbase_products(
        self,
        *,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        assets = sorted({normalize_asset_symbol(symbol) for symbol in asset_symbols}) if asset_symbols else sorted(COINBASE_PRODUCT_IDS)
        coinbase = self._coinbase_client()
        products: dict[str, Any] = {}
        try:
            for asset in assets:
                product_id = COINBASE_PRODUCT_IDS.get(asset) or f"{asset}-USD"
                product = await coinbase.fetch_product(product_id)
                products[asset] = {
                    "asset_symbol": asset,
                    "product_id": product_id,
                    "configured_for_spot_collection": asset in COINBASE_PRODUCT_IDS,
                    "coinbase_supported": product is not None,
                    "base_currency_id": (product or {}).get("base_currency_id") or (product or {}).get("base_currency"),
                    "quote_currency_id": (product or {}).get("quote_currency_id") or (product or {}).get("quote_currency"),
                    "product_type": (product or {}).get("product_type"),
                    "trading_disabled": bool((product or {}).get("trading_disabled")),
                    "status": (product or {}).get("status") or ("available" if product is not None else "missing"),
                }
        finally:
            await coinbase.aclose()
        return {
            "status": "ok",
            "authenticated": bool(coinbase.credentials),
            "assets": products,
            "coinbase_live_quality_assets": sorted(asset for asset, payload in products.items() if payload["configured_for_spot_collection"] and payload["coinbase_supported"]),
            "proxy_only_assets": sorted(asset for asset, payload in products.items() if not payload["configured_for_spot_collection"]),
        }

    async def collect_current(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        assets = await self._asset_symbols(asset_symbols=asset_symbols, frequency=freq)
        provider_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"stored": 0, "assets": [], "errors": []})
        stored_total = 0
        coinbase = self._coinbase_client()
        proxy_fallback_enabled = bool(self.settings.crypto_spot_proxy_fallback_enabled)
        coingecko = CoinGeckoSpotClient(timeout_seconds=self.settings.crypto_spot_request_timeout_seconds) if proxy_fallback_enabled else None
        kraken = self._kraken_client()
        kraken_interval_seconds = interval_seconds_for_frequency(freq)
        kraken_window_end = datetime.now(UTC)
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                for asset in assets:
                    row: SpotOHLC | None = None
                    attempted: list[str] = []
                    if asset in COINBASE_PRODUCT_IDS:
                        attempted.append("coinbase")
                        try:
                            row = await coinbase.fetch_current(asset)
                        except Exception as exc:
                            provider_stats["coinbase"]["errors"].append({"asset_symbol": asset, "error": str(exc)})
                    if row is None and proxy_fallback_enabled and asset in COINGECKO_IDS and coingecko is not None:
                        attempted.append("coingecko")
                        try:
                            row = await coingecko.fetch_current(asset)
                        except Exception as exc:
                            provider_stats["coingecko"]["errors"].append({"asset_symbol": asset, "error": str(exc)})
                    stored_total += await self._collect_kraken_rows(
                        repo,
                        kraken,
                        asset,
                        frequency=freq,
                        interval_seconds=kraken_interval_seconds,
                        start=kraken_window_end - timedelta(seconds=kraken_interval_seconds * 3),
                        end=kraken_window_end,
                        provider_stats=provider_stats,
                    )
                    if row is None:
                        provider_stats["none"]["errors"].append(
                            {
                                "asset_symbol": asset,
                                "error": "no_current_spot_returned",
                                "attempted": attempted,
                            }
                        )
                        continue
                    stored = await self._store_rows(repo, [row], frequency=freq, interval_seconds=0)
                    provider_stats[row.provider]["stored"] += stored
                    provider_stats[row.provider]["assets"].append(asset)
                    stored_total += stored
                await session.commit()
                since = datetime.now(UTC) - timedelta(days=1)
                spot_rows = await repo.list_crypto_spot_ohlc(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=assets,
                    since=since,
                    limit=100_000,
                )
                await session.commit()
        finally:
            await coinbase.aclose()
            if coingecko is not None:
                await coingecko.aclose()
            if kraken is not None:
                await kraken.aclose()
        return {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbols": assets,
            "stored": stored_total,
            "proxy_fallback_enabled": proxy_fallback_enabled,
            "kraken_enabled": kraken is not None,
            "providers": {key: {**value, "error_count": len(value["errors"])} for key, value in provider_stats.items()},
            "spot_quality": _crypto_spot_quality(
                spot_rows,
                expected_assets=assets,
                min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
                settings=self.settings,
            ),
        }

    async def backfill(
        self,
        *,
        days: int | None = None,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        interval_seconds: int | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        # interval_seconds override lets a one-time historical densification fetch
        # finer candles (e.g. 60s) than the frequency default (900s for 15m).
        # Finer rows coexist with the coarse ones — interval_seconds is part of
        # the spot-OHLC unique key — and the training lookup reads by frequency,
        # so both densify the same series.
        interval_seconds = int(interval_seconds) if interval_seconds else interval_seconds_for_frequency(freq)
        lookback_days = days or self.settings.crypto_history_lookback_days
        end = datetime.now(UTC)
        start = end - timedelta(days=lookback_days)
        assets = await self._asset_symbols(asset_symbols=asset_symbols, frequency=freq)
        provider_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"stored": 0, "assets": [], "errors": []})
        stored_total = 0
        coinbase = self._coinbase_client()
        proxy_fallback_enabled = bool(self.settings.crypto_spot_proxy_fallback_enabled)
        coingecko = CoinGeckoSpotClient(timeout_seconds=self.settings.crypto_spot_request_timeout_seconds) if proxy_fallback_enabled else None
        kraken = self._kraken_client()
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                for index, asset in enumerate(assets, start=1):
                    rows: list[SpotOHLC] = []
                    attempted: list[str] = []
                    if asset in COINBASE_PRODUCT_IDS:
                        attempted.append("coinbase")
                        try:
                            rows = await coinbase.fetch_ohlc(
                                asset,
                                start=start,
                                end=end,
                                interval_seconds=interval_seconds,
                            )
                        except Exception as exc:
                            provider_stats["coinbase"]["errors"].append({"asset_symbol": asset, "error": str(exc)})
                    if not rows and proxy_fallback_enabled and asset in COINGECKO_IDS and coingecko is not None:
                        attempted.append("coingecko")
                        try:
                            rows = await coingecko.fetch_ohlc(
                                asset,
                                start=start,
                                end=end,
                                interval_seconds=interval_seconds,
                            )
                        except Exception as exc:
                            provider_stats["coingecko"]["errors"].append({"asset_symbol": asset, "error": str(exc)})
                    stored_total += await self._collect_kraken_rows(
                        repo,
                        kraken,
                        asset,
                        frequency=freq,
                        interval_seconds=interval_seconds,
                        start=start,
                        end=end,
                        provider_stats=provider_stats,
                    )
                    if not rows:
                        provider_stats["none"]["errors"].append(
                            {
                                "asset_symbol": asset,
                                "error": "no_spot_rows_returned",
                                "attempted": attempted,
                            }
                        )
                        continue
                    stored = await self._store_rows(repo, rows, frequency=freq, interval_seconds=interval_seconds)
                    provider = rows[0].provider
                    provider_stats[provider]["stored"] += stored
                    provider_stats[provider]["assets"].append(asset)
                    stored_total += stored
                    if index % 3 == 0:
                        await session.commit()
                await session.commit()
                spot_rows = await repo.list_crypto_spot_ohlc(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    since=start,
                    limit=1_000_000,
                )
                await session.commit()
        finally:
            await coinbase.aclose()
            if coingecko is not None:
                await coingecko.aclose()
            if kraken is not None:
                await kraken.aclose()
        return {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "lookback_days": lookback_days,
            "asset_symbols": assets,
            "stored": stored_total,
            "proxy_fallback_enabled": proxy_fallback_enabled,
            "kraken_enabled": kraken is not None,
            "providers": {key: {**value, "error_count": len(value["errors"])} for key, value in provider_stats.items()},
            "spot_quality": _crypto_spot_quality(
                spot_rows,
                expected_assets=assets,
                min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
                settings=self.settings,
            ),
        }

    async def status(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        assets = await self._asset_symbols(asset_symbols=asset_symbols, frequency=freq)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=1_000_000,
            )
            await session.commit()
        return {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "spot_quality": _crypto_spot_quality(
                rows,
                expected_assets=assets,
                min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
                settings=self.settings,
            ),
        }

    async def _asset_symbols(self, *, asset_symbols: list[str] | None, frequency: str) -> list[str]:
        if asset_symbols:
            return sorted({normalize_asset_symbol(symbol) for symbol in asset_symbols})
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=frequency,
                kalshi_env=self.settings.kalshi_env,
                limit=100_000,
            )
            await session.commit()
        discovered = {row.asset_symbol for row in snapshots}
        discovered.update(COINBASE_PRODUCT_IDS)
        if self.settings.crypto_spot_proxy_fallback_enabled:
            discovered.update(COINGECKO_IDS)
        return sorted(discovered)

    async def _store_rows(
        self,
        repo: PlatformRepository,
        rows: list[SpotOHLC],
        *,
        frequency: str,
        interval_seconds: int,
    ) -> int:
        stored = 0
        observed_at = datetime.now(UTC)
        for row in rows:
            if row.close_dollars is None or row.end_ts > observed_at:
                continue
            await repo.upsert_crypto_spot_ohlc(
                kalshi_env=self.settings.kalshi_env,
                provider=row.provider,
                asset_symbol=row.asset_symbol,
                quote_currency="USD",
                frequency=frequency,
                interval_seconds=interval_seconds,
                start_ts=row.start_ts,
                end_ts=row.end_ts,
                open_dollars=row.open_dollars,
                high_dollars=row.high_dollars,
                low_dollars=row.low_dollars,
                close_dollars=row.close_dollars,
                volume=row.volume,
                observed_at=observed_at,
                source_kind=row.source_kind,
                source_id=row.source_id,
                payload={
                    **row.payload,
                    "provenance": {
                        "source": row.provider,
                        "source_kind": row.source_kind,
                        "source_id": row.source_id,
                        "leakage_risk": "point_in_time",
                        "observed_at": observed_at.isoformat(),
                    },
                },
            )
            stored += 1
        return stored

    async def collect_funding_rates(
        self,
        *,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        """Fetch the latest settled funding rates from OKX and persist them."""
        assets = list(asset_symbols or OKX_ASSET_INST_IDS.keys())
        stored_total = 0
        errors: list[dict[str, Any]] = []
        client = OkxFundingRateClient(timeout_seconds=self.settings.crypto_spot_request_timeout_seconds)
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                for asset in sorted(assets):
                    try:
                        records = await client.fetch_history(asset, limit=10)
                        for rec in records:
                            await repo.upsert_crypto_funding_rate(
                                provider=rec.provider,
                                asset_symbol=rec.asset_symbol,
                                quote_currency=rec.quote_currency,
                                settlement_ts=rec.settlement_ts,
                                funding_rate=rec.funding_rate,
                                realized_rate=rec.realized_rate,
                                payload=rec.payload,
                            )
                            stored_total += 1
                    except Exception as exc:
                        errors.append({"asset_symbol": asset, "error": str(exc)})
                await session.commit()
        finally:
            await client.aclose()
        return {"stored": stored_total, "assets": assets, "errors": errors}

    async def backfill_funding_rates(
        self,
        *,
        asset_symbols: list[str] | None = None,
        max_pages: int = 30,
    ) -> dict[str, Any]:
        """Paginate OKX funding rate history back as far as available and persist all records.

        OKX returns at most 100 records per page; max_pages=30 covers ~90 days.
        """
        assets = list(asset_symbols or OKX_ASSET_INST_IDS.keys())
        per_asset: dict[str, Any] = {}
        client = OkxFundingRateClient(timeout_seconds=self.settings.crypto_spot_request_timeout_seconds)
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                for asset in sorted(assets):
                    stored = 0
                    oldest_ts: datetime | None = None
                    after_ts: datetime | None = None
                    error: str | None = None
                    try:
                        for _ in range(max_pages):
                            records = await client.fetch_history(asset, limit=100, after_ts=after_ts)
                            if not records:
                                break
                            for rec in records:
                                await repo.upsert_crypto_funding_rate(
                                    provider=rec.provider,
                                    asset_symbol=rec.asset_symbol,
                                    quote_currency=rec.quote_currency,
                                    settlement_ts=rec.settlement_ts,
                                    funding_rate=rec.funding_rate,
                                    realized_rate=rec.realized_rate,
                                    payload=rec.payload,
                                )
                                stored += 1
                                if oldest_ts is None or rec.settlement_ts < oldest_ts:
                                    oldest_ts = rec.settlement_ts
                            # oldest record in this page → next page goes further back
                            after_ts = min(rec.settlement_ts for rec in records)
                    except Exception as exc:
                        error = str(exc)
                    per_asset[asset] = {
                        "stored": stored,
                        "oldest_ts": oldest_ts.isoformat() if oldest_ts else None,
                        "error": error,
                    }
                await session.commit()
        finally:
            await client.aclose()
        total = sum(v["stored"] for v in per_asset.values())
        return {"total_stored": total, "per_asset": per_asset}


class CryptoTrainingBackfillService:
    """Materialize point-in-time crypto training data before model fitting."""

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        history_service: CryptoHistoryService,
        spot_service: CryptoSpotService | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.history_service = history_service
        self.spot_service = spot_service

    async def prepare(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        settled_days: int | None = None,
        history_days: int | None = None,
        spot_days: int | None = None,
        run_source_backfill: bool = True,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        assets = normalize_asset_symbols(asset_symbols)
        errors: list[dict[str, Any]] = []
        steps: dict[str, Any] = {}
        if run_source_backfill:
            steps["collect_open"] = await self._capture_step(
                errors,
                "collect_open",
                lambda: self.history_service.collect_open(frequency=freq, asset_symbols=assets or None),
            )
            steps["collect_settled"] = await self._capture_step(
                errors,
                "collect_settled",
                lambda: self.history_service.collect_settled(
                    frequency=freq,
                    days=settled_days or self.settings.crypto_training_preflight_settled_days,
                    asset_symbols=assets or None,
                ),
            )
            steps["history_bootstrap"] = await self._capture_step(
                errors,
                "history_bootstrap",
                lambda: self.history_service.bootstrap(
                    frequency=freq,
                    days=history_days or self.settings.crypto_training_preflight_history_days,
                    asset_symbols=assets or None,
                ),
            )
            if self.spot_service is not None:
                steps["spot_backfill"] = await self._capture_step(
                    errors,
                    "spot_backfill",
                    lambda: self.spot_service.backfill(
                        frequency=freq,
                        days=spot_days or self.settings.crypto_training_preflight_spot_days,
                        asset_symbols=assets or None,
                    ),
                )
                steps["spot_current"] = await self._capture_step(
                    errors,
                    "spot_current",
                    lambda: self.spot_service.collect_current(frequency=freq, asset_symbols=assets or None),
                )

        materialized = await self.materialize(
            frequency=freq,
            asset_symbols=assets or None,
            materialize_microstructure=run_source_backfill,
            materialize_settlement_windows=run_source_backfill,
        )
        blockers = list(materialized.get("blockers") or [])
        blockers.extend(f"backfill_{error['step']}_failed" for error in errors)
        status = "ok" if not blockers else "blocked"
        return {
            "status": status,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "asset_symbols": assets,
            "run_source_backfill": run_source_backfill,
            "steps": steps,
            "materialized": materialized,
            "blockers": blockers,
            "errors": errors,
        }

    async def _capture_step(
        self,
        errors: list[dict[str, Any]],
        step: str,
        awaitable_factory: Callable[[], Awaitable[Any]],
    ) -> dict[str, Any]:
        for attempt, delay in enumerate((0.0, *CRYPTO_TRAINING_STEP_RETRY_DELAYS_SECONDS), start=1):
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                result = await awaitable_factory()
                return _crypto_training_step_summary(result)
            except Exception as exc:
                has_retry = attempt <= len(CRYPTO_TRAINING_STEP_RETRY_DELAYS_SECONDS)
                if has_retry and _is_crypto_transient_network_error(exc):
                    logger.warning(
                        "crypto_training_preflight_step_retry step=%s attempt=%s next_delay_seconds=%s reason=%s",
                        step,
                        attempt,
                        CRYPTO_TRAINING_STEP_RETRY_DELAYS_SECONDS[attempt - 1],
                        exc,
                    )
                    continue
                logger.warning("crypto_training_preflight step failed step=%s", step, exc_info=True)
                errors.append({"step": step, "error": str(exc)})
                return {"status": "error", "error": str(exc)}
        raise RuntimeError("crypto training preflight retry loop exited unexpectedly")

    async def materialize(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        materialize_microstructure: bool = True,
        materialize_settlement_windows: bool = True,
    ) -> dict[str, Any]:
        for attempt, delay in enumerate((0.0, *CRYPTO_TRAINING_DB_RETRY_DELAYS_SECONDS), start=1):
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                return await self._materialize_once(
                    frequency=frequency,
                    asset_symbols=asset_symbols,
                    materialize_microstructure=materialize_microstructure,
                    materialize_settlement_windows=materialize_settlement_windows,
                )
            except Exception as exc:
                if attempt > len(CRYPTO_TRAINING_DB_RETRY_DELAYS_SECONDS) or not _is_crypto_db_disconnect(exc):
                    raise
                logger.warning(
                    "crypto_training_materialize_db_retry frequency=%s attempt=%s next_delay_seconds=%s reason=%s",
                    frequency,
                    attempt,
                    CRYPTO_TRAINING_DB_RETRY_DELAYS_SECONDS[attempt - 1],
                    exc,
                )
        raise RuntimeError("crypto training materialize retry loop exited unexpectedly")

    async def _materialize_once(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        materialize_microstructure: bool = True,
        materialize_settlement_windows: bool = True,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        lookback_days = max(1, int(self.settings.crypto_train_lookback_days))
        now_utc = datetime.now(UTC)
        full_since = now_utc - timedelta(days=lookback_days)
        since = full_since  # may be narrowed to the incremental tail below
        build_id = _crypto_training_build_id(
            {
                "kalshi_env": self.settings.kalshi_env,
                "frequency": freq,
                "asset_symbols": requested_assets,
                "since": since.isoformat(),
                "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            }
        )
        # READ PHASE — close the session immediately after fetching so the
        # connection is not held idle during the multi-hour compute phase.
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            watermark = None
            if self.settings.crypto_train_incremental_materialize_enabled:
                watermark = await repo.get_crypto_training_feature_watermark(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    feature_schema_version=CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                )
            since, upsert_floor, since_reason = _resolve_incremental_materialize_since(
                full_since=full_since,
                now=now_utc,
                watermark=watermark,
                enabled=self.settings.crypto_train_incremental_materialize_enabled,
                warmup_hours=self.settings.crypto_train_incremental_warmup_hours,
                max_gap_hours=self.settings.crypto_train_incremental_max_gap_hours,
                label_refresh_hours=self.settings.crypto_train_incremental_label_refresh_hours,
            )
            logger.info(
                "crypto_materialize window freq=%s mode=%s since=%s watermark=%s upsert_floor=%s",
                freq,
                since_reason,
                since.isoformat(),
                watermark.isoformat() if watermark else None,
                upsert_floor.isoformat() if upsert_floor else None,
            )
            if since_reason == "full_insufficient_recency_margin":
                logger.warning(
                    "crypto_materialize falling back to full rebuild: warmup_hours=%s is too "
                    "close to label_refresh_hours=%s (need >= %sh of recency context between "
                    "the read window and the upsert floor). Increase crypto_train_incremental_"
                    "warmup_hours or lower crypto_train_incremental_label_refresh_hours to "
                    "re-enable partial incremental materialize.",
                    self.settings.crypto_train_incremental_warmup_hours,
                    self.settings.crypto_train_incremental_label_refresh_hours,
                    _INCREMENTAL_MIN_RECENCY_CONTEXT_HOURS,
                )
            snapshots = await repo.list_crypto_settled_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=since,
                limit=self.settings.crypto_train_max_snapshots,
                defer_payload=True,
            )
            live_quote_snapshots = await repo.list_crypto_live_quote_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=since,
                limit=self.settings.crypto_train_max_snapshots,
                defer_payload=True,
            )
            if live_quote_snapshots:
                snapshots = list(snapshots) + live_quote_snapshots
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets or None,
                since=since,
                limit=self.settings.crypto_train_max_candlesticks,
                defer_payload=True,
            )
            spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                repo,
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                requested_assets=requested_assets,
                since=since,
                limit=self.settings.crypto_train_max_spot_rows,
            )
        snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
        candles = _filter_crypto_snapshot_rows(candles, requested_assets)
        spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))

        # COMPUTE PHASE — pure in-memory; no DB connection held. Run in a thread
        # so the asyncio event loop is not blocked during the multi-hour CPU pass.
        loop = asyncio.get_event_loop()
        _build_workers = int(self.settings.crypto_train_build_workers)
        if _build_workers > 1:
            decision_rows = await loop.run_in_executor(
                None,
                lambda: _crypto_decision_rows_parallel(
                    snapshots,
                    candles,
                    spot_rows,
                    settings=self.settings,
                    workers=_build_workers,
                ),
            )
        else:
            decision_rows = await loop.run_in_executor(
                None,
                lambda: _crypto_decision_rows(snapshots, candles, spot_rows, settings=self.settings),
            )

        # WRITE PHASE — fresh session after compute so there is no stale connection.
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            if materialize_microstructure:
                await self._materialize_spot_microstructure(repo, spot_rows, frequency=freq)
            benchmark_count = 0
            if materialize_settlement_windows:
                benchmark_count = await self._materialize_settlement_windows(
                    repo,
                    snapshots=snapshots,
                    spot_rows=spot_rows,
                    frequency=freq,
                )
            feature_row_values: list[dict[str, Any]] = []
            for row in decision_rows:
                if upsert_floor is not None and _as_utc_datetime(row.get("decision_ts")) <= upsert_floor:
                    continue
                payload = _crypto_training_json_ready(row)
                feature_hash = _crypto_training_build_id(payload)
                feature_row_values.append(
                    dict(
                        kalshi_env=self.settings.kalshi_env,
                        frequency=freq,
                        market_ticker=str(row.get("market_ticker") or ""),
                        asset_symbol=normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
                        row_id=str(row.get("row_id") or feature_hash),
                        decision_time=_as_utc_datetime(row.get("decision_ts")),
                        settlement_time=_as_utc_datetime(row.get("settlement_ts")) if row.get("settlement_ts") else None,
                        label_yes=int(row["label_yes"]) if row.get("label_yes") in {0, 1} else None,
                        strict_trade_eligible=bool(row.get("strict_trade_eligible")),
                        feature_schema_version=CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                        feature_hash=feature_hash,
                        source_build_id=build_id,
                        quality_score=_crypto_training_row_quality_score(row),
                        payload={
                            "schema_version": "crypto-training-feature-row-v1",
                            "decision_row": payload,
                        },
                    )
                )
            await repo.bulk_upsert_crypto_training_feature_rows(feature_row_values)
            rows_upserted = len(feature_row_values)
            outcome_count = await self._materialize_decision_outcomes(
                session,
                repo,
                frequency=freq,
                asset_symbols=requested_assets,
                since=since,
                snapshots=snapshots,
            )
            strict_rows = sum(1 for row in decision_rows if row.get("strict_trade_eligible"))
            spot_coverage = _spot_feature_coverage(decision_rows)
            window_start = min((row.get("decision_ts") for row in decision_rows if row.get("decision_ts")), default=None)
            window_end = max((row.get("decision_ts") for row in decision_rows if row.get("decision_ts")), default=None)
            blockers = _crypto_training_quality_blockers(
                decision_rows,
                spot_coverage=spot_coverage,
                settings=self.settings,
                asset_symbols=requested_assets,
            )
            quality_status = "ok" if not blockers else "blocked"
            # NOTE: on incremental runs rows_materialized = rows actually upserted (the tail),
            # while the quality/coverage metrics below intentionally evaluate the full read window's health.
            quality = await repo.record_crypto_data_quality_run(
                kalshi_env=self.settings.kalshi_env,
                frequency=freq,
                asset_symbol=requested_assets[0] if len(requested_assets) == 1 else ("MULTI" if requested_assets else "ALL"),
                run_kind="pre_training",
                status=quality_status,
                source_build_id=build_id,
                window_start_ts=window_start,
                window_end_ts=window_end,
                rows_materialized=rows_upserted,
                strict_trade_eligible_rows=strict_rows,
                spot_coverage_pct=spot_coverage,
                decision_outcome_count=outcome_count,
                metrics={
                    "sample_count": len(decision_rows),
                    "strict_trade_eligible_rows": strict_rows,
                    "spot_coverage_pct": spot_coverage,
                    "benchmark_window_count": benchmark_count,
                    "microstructure_materialized": materialize_microstructure,
                    "settlement_windows_materialized": materialize_settlement_windows,
                    "blockers": blockers,
                },
                payload={
                    "snapshot_count": len(snapshots),
                    "live_quote_snapshot_count": len(live_quote_snapshots),
                    "candlestick_count": len(candles),
                    "spot_row_count": len(spot_rows),
                    "asset_symbols": requested_assets,
                    "microstructure_materialized": materialize_microstructure,
                    "settlement_windows_materialized": materialize_settlement_windows,
                },
            )
            await session.commit()
        return {
            "status": quality_status,
            "source_build_id": build_id,
            "quality_run_id": quality.id,
            "rows_materialized": rows_upserted,
            "strict_trade_eligible_rows": strict_rows,
            "spot_coverage_pct": spot_coverage,
            "decision_outcome_count": outcome_count,
            "benchmark_window_count": benchmark_count,
            "microstructure_materialized": materialize_microstructure,
            "settlement_windows_materialized": materialize_settlement_windows,
            "blockers": blockers,
        }

    async def _materialize_spot_microstructure(
        self,
        repo: PlatformRepository,
        spot_rows: list[CryptoSpotOHLCRecord],
        *,
        frequency: str,
    ) -> None:
        ops_since_commit = 0

        async def run_upsert(label: str, operation: Callable[[], Awaitable[None]]) -> None:
            nonlocal ops_since_commit
            await self._run_microstructure_upsert(repo, label, operation)
            ops_since_commit += 1
            if ops_since_commit >= CRYPTO_MICROSTRUCTURE_UPSERT_COMMIT_INTERVAL:
                await repo.session.commit()
                ops_since_commit = 0

        for row in spot_rows:
            payload = row.payload if isinstance(row.payload, dict) else {}
            microstructure = payload.get("market_microstructure") if isinstance(payload.get("market_microstructure"), dict) else {}
            best_bid_ask = microstructure.get("best_bid_ask") if isinstance(microstructure.get("best_bid_ask"), dict) else {}
            if best_bid_ask:
                bid_depth = _optional_decimal(best_bid_ask.get("best_bid_size"))
                ask_depth = _optional_decimal(best_bid_ask.get("best_ask_size"))
                depth_imbalance = None
                if bid_depth is not None and ask_depth is not None and bid_depth + ask_depth > 0:
                    depth_imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)

                async def upsert_order_book_snapshot(
                    *,
                    row: CryptoSpotOHLCRecord = row,
                    best_bid_ask: dict[str, Any] = best_bid_ask,
                    bid_depth: Decimal | None = bid_depth,
                    ask_depth: Decimal | None = ask_depth,
                    depth_imbalance: Decimal | None = depth_imbalance,
                ) -> None:
                    await repo.upsert_crypto_order_book_snapshot(
                        kalshi_env=self.settings.kalshi_env,
                        provider=row.provider,
                        asset_symbol=row.asset_symbol,
                        frequency=frequency,
                        market_ticker="",
                        source_kind=row.source_kind,
                        source_id=row.source_id,
                        observed_at=_as_utc_datetime(row.end_ts),
                        best_bid_dollars=_optional_decimal(best_bid_ask.get("best_bid_dollars")),
                        best_ask_dollars=_optional_decimal(best_bid_ask.get("best_ask_dollars")),
                        mid_dollars=_optional_decimal(best_bid_ask.get("mid_dollars")),
                        spread_bps=int(best_bid_ask["spread_bps"]) if best_bid_ask.get("spread_bps") not in (None, "") else None,
                        bid_depth=bid_depth,
                        ask_depth=ask_depth,
                        depth_imbalance=depth_imbalance,
                        payload={"spot_ohlc_id": row.id, "raw": best_bid_ask},
                    )

                await run_upsert("order_book_snapshot", upsert_order_book_snapshot)
            trades = microstructure.get("recent_trades") if isinstance(microstructure.get("recent_trades"), list) else []
            for idx, trade in enumerate(trades):
                if not isinstance(trade, dict):
                    continue
                observed = parse_datetime(trade.get("time")) or row.end_ts
                trade_id = str(trade.get("trade_id") or f"{row.id}:{idx}")

                async def upsert_trade_tick(
                    *,
                    row: CryptoSpotOHLCRecord = row,
                    trade: dict[str, Any] = trade,
                    observed: datetime = observed,
                    trade_id: str = trade_id,
                ) -> None:
                    await repo.upsert_crypto_trade_tick(
                        kalshi_env=self.settings.kalshi_env,
                        provider=row.provider,
                        asset_symbol=row.asset_symbol,
                        frequency=frequency,
                        market_ticker="",
                        source_kind=row.source_kind,
                        source_id=row.source_id or "",
                        trade_id=trade_id,
                        observed_at=_as_utc_datetime(observed),
                        side=str(trade.get("side") or "") or None,
                        price_dollars=_optional_decimal(trade.get("price_dollars") or trade.get("price")),
                        size=_optional_decimal(trade.get("size")),
                        payload={"spot_ohlc_id": row.id, "raw": trade},
                    )

                await run_upsert("trade_tick", upsert_trade_tick)
        if ops_since_commit:
            await repo.session.commit()

    async def _run_microstructure_upsert(
        self,
        repo: PlatformRepository,
        label: str,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        try:
            await operation()
        except Exception as exc:
            if not _is_crypto_db_disconnect(exc):
                raise
            logger.warning(
                "crypto_training_microstructure_upsert_retry label=%s reason=%s",
                label,
                exc,
            )
            await repo.session.rollback()
            await operation()

    async def _materialize_settlement_windows(
        self,
        repo: PlatformRepository,
        *,
        snapshots: list[CryptoMarketSnapshotRecord],
        spot_rows: list[CryptoSpotOHLCRecord],
        frequency: str,
    ) -> int:
        spot_by_asset: dict[str, list[CryptoSpotOHLCRecord]] = defaultdict(list)
        for row in spot_rows:
            if row.close_dollars is not None:
                spot_by_asset[row.asset_symbol].append(row)
        for rows in spot_by_asset.values():
            rows.sort(key=lambda item: item.end_ts)
        settled = _crypto_settlement_snapshots_by_market(snapshots)
        count = 0
        for market_ticker, snapshot in settled.items():
            close_time = snapshot.close_time or snapshot.expected_expiration_time
            if close_time is None:
                continue
            context = _settlement_benchmark_context(
                spot_by_asset.get(snapshot.asset_symbol, []),
                close_time=close_time,
                target_price=snapshot.target_price_dollars,
                frequency=frequency,
            )
            await repo.upsert_crypto_settlement_benchmark_window(
                kalshi_env=self.settings.kalshi_env,
                market_ticker=market_ticker,
                asset_symbol=snapshot.asset_symbol,
                frequency=frequency,
                target_price_dollars=snapshot.target_price_dollars,
                window_start_ts=context.get("settlement_window_start_ts"),
                window_end_ts=context.get("settlement_window_end_ts"),
                sample_count=int(context.get("settlement_window_sample_count") or 0),
                open_dollars=context.get("settlement_window_open_dollars"),
                high_dollars=context.get("settlement_window_high_dollars"),
                low_dollars=context.get("settlement_window_low_dollars"),
                close_dollars=context.get("settlement_window_close_dollars"),
                twap_dollars=context.get("settlement_window_twap_dollars"),
                vwap_dollars=context.get("settlement_window_vwap_dollars"),
                payload={"status": context.get("settlement_window_status")},
            )
            count += 1
        return count

    async def _materialize_decision_outcomes(
        self,
        session: AsyncSession,
        repo: PlatformRepository,
        *,
        frequency: str,
        asset_symbols: list[str],
        since: datetime,
        snapshots: list[CryptoMarketSnapshotRecord],
    ) -> int:
        snapshot_by_market = {row.market_ticker: row for row in snapshots}
        stmt = select(DecisionTraceRecord).where(
            DecisionTraceRecord.kalshi_env == self.settings.kalshi_env,
            DecisionTraceRecord.decision_time >= since,
        )
        traces = list((await session.execute(stmt.limit(50_000))).scalars())
        if asset_symbols:
            traces = [
                trace
                for trace in traces
                if normalize_asset_symbol(
                    str(_trace_value(trace.trace, "asset_symbol", "asset") or getattr(snapshot_by_market.get(trace.market_ticker), "asset_symbol", ""))
                )
                in set(asset_symbols)
            ]
        count = 0
        for trace in traces:
            snapshot = snapshot_by_market.get(trace.market_ticker)
            asset = normalize_asset_symbol(
                str(_trace_value(trace.trace, "asset_symbol", "asset") or getattr(snapshot, "asset_symbol", "UNKNOWN"))
            )
            trace_frequency = normalize_frequency(
                str(_trace_value(trace.trace, "frequency") or getattr(snapshot, "frequency", frequency))
            ) or frequency
            if trace_frequency != frequency:
                continue
            fills = list(
                (
                    await session.execute(
                        select(FillRecord).where(
                            FillRecord.kalshi_env == self.settings.kalshi_env,
                            FillRecord.market_ticker == trace.market_ticker,
                        )
                    )
                ).scalars()
            )
            realized = _crypto_realized_fill_pnl(fills)
            await repo.upsert_crypto_decision_outcome(
                kalshi_env=self.settings.kalshi_env,
                frequency=frequency,
                market_ticker=trace.market_ticker,
                asset_symbol=asset,
                decision_time=trace.decision_time,
                decision_kind=trace.decision_kind,
                input_hash=trace.input_hash or trace.trace_hash or trace.id,
                trace_hash=trace.trace_hash,
                model_version=str(_trace_value(trace.trace, "model_version", "version") or "") or None,
                prediction_yes=_optional_decimal(_trace_value(trace.trace, "prediction_yes", "fair_yes_dollars", "probability", "p_yes")),
                selected_side=str(_trace_value(trace.trace, "selected_side", "side") or "") or None,
                selected_price_dollars=_optional_decimal(_trace_value(trace.trace, "selected_price_dollars", "yes_price_dollars", "price")),
                selected_count_fp=_optional_decimal(_trace_value(trace.trace, "selected_count_fp", "count_fp")),
                gate_status=str(_trace_value(trace.trace, "gate_status", "status") or "") or None,
                settlement_result=getattr(snapshot, "settlement_result", None),
                simulated_pnl_dollars=_optional_decimal(_trace_value(trace.trace, "simulated_pnl_dollars", "net_pnl")),
                realized_pnl_dollars=realized,
                fill_count=len(fills),
                source_snapshot_ids=trace.source_snapshot_ids or {},
                payload={"trace_id": trace.id, "trace": _crypto_training_json_ready(trace.trace or {})},
            )
            count += 1
        return count

class CryptoForecastService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        agent_pack_service: AgentPackService | None = None,
        spot_service: CryptoSpotService | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.agent_pack_service = agent_pack_service or AgentPackService(settings)
        self.spot_service = spot_service

    async def train(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        use_feature_store: bool = False,
        feature_store_only: bool = False,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        lookback_days = max(1, int(self.settings.crypto_train_lookback_days))
        since = datetime.now(UTC) - timedelta(days=lookback_days)

        # Read phase — close session before heavy compute so the connection
        # is not held idle in transaction during multi-hour fitting.
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            feature_records = []
            if use_feature_store or feature_store_only:
                feature_records = await repo.list_crypto_training_feature_rows(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=since,
                    limit=self.settings.crypto_train_max_snapshots,
                )
            funding_rate_rows = await repo.list_crypto_funding_rates_bulk(
                asset_symbols=requested_assets or None,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            trained_from = "crypto_training_feature_rows"
            if feature_records:
                decision_rows = [_crypto_training_row_payload(record) for record in reversed(feature_records)]
            elif feature_store_only:
                decision_rows = []
            else:
                rows = await repo.list_crypto_settled_market_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=since,
                    limit=self.settings.crypto_train_max_snapshots,
                )
                live_quote_rows = await repo.list_crypto_live_quote_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=since,
                    limit=self.settings.crypto_train_max_snapshots,
                )
                if live_quote_rows:
                    rows = _dedupe_crypto_snapshot_rows([*rows, *live_quote_rows])
                candles = await repo.list_crypto_market_candlesticks(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=since,
                    limit=self.settings.crypto_train_max_candlesticks,
                )
                spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    requested_assets=requested_assets,
                    since=since,
                    limit=self.settings.crypto_train_max_spot_rows,
                )
                rows = _filter_crypto_snapshot_rows(rows, requested_assets)
                candles = _filter_crypto_snapshot_rows(candles, requested_assets)
                spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
                rows = _filter_snapshots_by_per_asset_funding_cutoff(funding_rate_rows, rows)
                decision_rows = _crypto_decision_rows(
                    rows,
                    candles,
                    spot_rows,
                    funding_rate_rows=funding_rate_rows,
                    settings=self.settings,
                )
                trained_from = "point_in_time_crypto_snapshots_and_candles"

        # Compute phase — no DB connection held open.
        sample_count = len(decision_rows)
        payload = _fit_crypto_calibration(decision_rows, settings=self.settings, crypto_policy=crypto_policy)
        metrics = _crypto_model_metrics(decision_rows, payload, settings=self.settings, crypto_policy=crypto_policy)
        training_quality_blockers = _crypto_training_quality_blockers(
            decision_rows,
            spot_coverage=_spot_feature_coverage(decision_rows),
            settings=self.settings,
            asset_symbols=requested_assets,
        )
        live_readiness_status = "ready" if not training_quality_blockers else "blocked"
        metrics = {
            **metrics,
            "training_quality_blockers": training_quality_blockers,
            "live_readiness_status": live_readiness_status,
        }
        status = "trained" if sample_count >= self.settings.crypto_min_training_samples else "insufficient_data"
        artifact_payload = {
            **payload,
            "frequency": freq,
            "asset_symbols": requested_assets,
            "trained_from": trained_from,
            "live_readiness_status": live_readiness_status,
            "training_quality_blockers": training_quality_blockers,
            "feature_store_only": feature_store_only,
            "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            "feature_set": [
                "market_mid_logit",
                "asset",
                "time_to_close",
                "time_to_close_bucket",
                "market_age",
                "target_price",
                "execution_price",
                "spread",
                "quote_source",
                "proxy_quote_flag",
                "mid",
                "volume",
                "open_interest",
                "candlestick_momentum",
                "spot_moneyness",
                "spot_momentum",
                "spot_return_windows",
                "spot_realized_volatility",
                "spot_target_distance_volatility",
                "kalshi_mid_spot_gap",
                "recent_same_asset_behavior",
                "quote_velocity",
                "settlement_benchmark_window",
                "expanded_cross_asset_regime",
                "funding_rate",
            ],
            "metrics_scope": metrics.get("validation_scope") or "walk_forward_time_ordered",
            "candidate_registry_version": CRYPTO_CANDIDATE_REGISTRY_VERSION,
            "dependency_versions": _crypto_dependency_versions(),
        }

        # Write phase — fresh short-lived session for the artifact insert.
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("model", requested_assets),
                version=_version(f"crypto-{freq}-model", {"metrics": metrics, "payload": artifact_payload}),
                status=status,
                sample_count=sample_count,
                metrics=metrics,
                payload=artifact_payload,
                kalshi_env=self.settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            await session.commit()
        # Observability-only: one INFO line summarizing the per-asset model
        # selection table so a market_mid_baseline fallback is self-explaining
        # in logs. Pure projection of already-computed metrics; no recompute.
        logger.info(
            "crypto model selection freq=%s assets=%s champion=%s reason=%s oos_net=%s oos_adv=%s candidates=%s",
            freq,
            requested_assets,
            metrics.get("champion_model"),
            metrics.get("champion_selection_reason"),
            metrics.get("champion_oos_net_pnl"),
            metrics.get("champion_oos_pnl_advantage_vs_market_mid"),
            [
                (
                    row.get("name"),
                    row.get("policy_net"),
                    row.get("policy_advantage"),
                    row.get("selected_count"),
                    row.get("profit_deployable"),
                )
                for row in metrics.get("candidate_selection_table", [])
            ],
        )
        return {
            "status": status,
            "kalshi_env": self.settings.kalshi_env,
            "asset_symbols": requested_assets,
            "version": artifact.version,
            "sample_count": sample_count,
            "metrics": metrics,
            "payload": artifact_payload,
        }

    async def refresh_edge_shrinkage(self, *, frequency: str = "15m") -> dict[str, Any]:
        """Refit predicted-edge -> realized-P&L shrinkage from recent live fills.

        Stores the fit in deployment-control notes under
        ``crypto_edge_shrinkage:{frequency}`` so decision-time candidate
        generation can discount predicted edges (runtime data, not config).
        """
        freq = normalize_frequency(frequency) or "15m"
        note_key = crypto_edge_shrinkage_note_key(freq)
        if not self.settings.crypto_edge_shrinkage_enabled:
            return {"status": "disabled", "frequency": freq, "note_key": note_key}
        lookback_days = max(1, int(self.settings.crypto_edge_shrinkage_lookback_days))
        since = datetime.now(UTC) - timedelta(days=lookback_days)
        strategy_code = crypto_strategy_code_for_frequency(freq)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            observations = await repo.list_crypto_edge_shrinkage_fill_observations(
                strategy_codes=[strategy_code],
                since=since,
                kalshi_env=self.settings.kalshi_env,
            )
            fit = fit_edge_shrinkage(
                observations,
                beta_floor=float(self.settings.crypto_edge_shrinkage_beta_floor),
            )
            note_value = {
                **fit,
                "frequency": freq,
                "strategy_code": strategy_code,
                "lookback_days": lookback_days,
                "kalshi_env": self.settings.kalshi_env,
                "updated_at": datetime.now(UTC).isoformat(),
            }
            await repo.update_deployment_note_key(
                note_key,
                lambda _previous: note_value,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        logger.info(
            "crypto edge shrinkage refreshed: frequency=%s beta=%s raw_beta=%s sample_count=%s status=%s",
            freq,
            note_value["beta"],
            note_value["raw_beta"],
            note_value["sample_count"],
            note_value["status"],
        )
        return {**note_value, "note_key": note_key}

    async def candidates(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            feature_decision_rows = (
                await _crypto_training_feature_decision_rows(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=100_000,
                )
                if self.settings.crypto_training_feature_store_enabled
                else []
            )
            rows: list[CryptoMarketSnapshotRecord] = []
            candles: list[CryptoMarketCandlestickRecord] = []
            spot_rows: list[CryptoSpotOHLCRecord] = []
            if not feature_decision_rows:
                rows = await repo.list_crypto_market_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    settled_only=True,
                    limit=100_000,
                )
                live_quote_rows = await repo.list_crypto_live_quote_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=100_000,
                )
                if live_quote_rows:
                    rows = _dedupe_crypto_snapshot_rows([*rows, *live_quote_rows])
                candles = await repo.list_crypto_market_candlesticks(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=200_000,
                )
                spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    requested_assets=requested_assets,
                    since=cutoff,
                    limit=500_000,
                )
            artifact = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("model", requested_assets),
                kalshi_env=self.settings.kalshi_env,
            )
            funding_rate_rows = await repo.list_crypto_funding_rates_bulk(
                asset_symbols=requested_assets or None,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            await session.commit()
        dataset_source = "crypto_training_feature_rows" if feature_decision_rows else "settled_snapshots_rebuilt"
        if feature_decision_rows:
            decision_rows = _filter_crypto_dict_rows(feature_decision_rows, requested_assets)
        else:
            rows = _filter_crypto_snapshot_rows(rows, requested_assets)
            candles = _filter_crypto_snapshot_rows(candles, requested_assets)
            spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
            rows = _filter_snapshots_by_per_asset_funding_cutoff(funding_rate_rows, rows)
            decision_rows = _crypto_decision_rows(rows, candles, spot_rows, funding_rate_rows=funding_rate_rows, settings=self.settings)
        model_payload = artifact.payload if artifact is not None else None
        candidate_report = _crypto_model_candidate_report(
            decision_rows,
            settings=self.settings,
            crypto_policy=crypto_policy,
        )
        quality_report = _crypto_candidate_quality_report(decision_rows, model_payload, settings=self.settings)
        quality_dataset = quality_report.get("dataset") if isinstance(quality_report.get("dataset"), dict) else {}
        quality_report["dataset"] = {
            **quality_dataset,
            "source": dataset_source,
            "feature_row_count": len(feature_decision_rows),
            "rebuilt_snapshot_count": len(rows),
            "rebuilt_candlestick_count": len(candles),
            "rebuilt_spot_row_count": len(spot_rows),
        }
        return {
            "schema_version": "crypto-model-candidates-v2",
            "status": "ok" if model_payload else "missing_model",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "asset_symbols": requested_assets,
            "model": _artifact_summary(artifact),
            "primary_metric": "oos_candidate_net_pnl",
            "candidate_report": candidate_report,
            "ranked_candidates": candidate_report.get("candidates") or [],
            **quality_report,
        }

    async def forecast(self, market: CryptoMarket) -> StrategySignal:
        features = self.features(market)
        if not self.settings.crypto_enabled or not crypto_frequency_enabled(self.settings, market.frequency):
            return self._stand_down(market, StandDownReason.CRYPTO_DISABLED, "Crypto trading workflow is disabled.", features)
        if self.spot_service is not None:
            try:
                await self.spot_service.collect_current(
                    frequency=market.frequency,
                    asset_symbols=[market.asset_symbol],
                )
            except Exception:
                logger.warning(
                    "crypto current spot refresh failed asset=%s frequency=%s",
                    market.asset_symbol,
                    market.frequency,
                    exc_info=True,
                )
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            artifact = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
                prefer_generic=self.settings.crypto_model_nightly_pooled_only,
            )
            _now = datetime.now(UTC)
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=market.frequency,
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
                until=_now,
                limit=30,
            )
            cross_asset_spot: dict[str, list[CryptoSpotOHLCRecord]] = {}
            for _ca in [a for a in CRYPTO_CROSS_ASSET_FEATURE_ASSETS if a != market.asset_symbol]:
                cross_asset_spot[_ca] = await repo.list_crypto_spot_ohlc(
                    frequency=market.frequency,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbol=_ca,
                    until=_now,
                    limit=10,
                )
            funding_rate_rows = await repo.list_crypto_funding_rates(
                market.asset_symbol,
                limit=5,
            )
            backtest = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="backtest",
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
            )
            gate = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
            )
            touch_gate = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="replay_gate_touch20",
                kalshi_env=self.settings.kalshi_env,
                asset_symbol=market.asset_symbol,
                allow_generic_fallback=False,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            note_modes = CryptoAssetControlService(
                settings=self.settings,
                session_factory=self.session_factory,
            ).modes_from_notes(getattr(control, "notes", None))
            edge_shrinkage = _crypto_edge_shrinkage_from_notes(
                getattr(control, "notes", None),
                frequency=market.frequency,
                settings=self.settings,
            )
            prior_quote_rows = await repo.list_crypto_market_snapshots(
                market_ticker=market.market_ticker,
                kalshi_env=self.settings.kalshi_env,
                limit=1,
            )
            prior_quote = prior_quote_rows[0] if prior_quote_rows else None
            await session.commit()
        crypto_policy = _runtime_crypto_policy_with_asset_modes(
            crypto_policy,
            _resolved_crypto_asset_modes(
                asset_symbols=[market.asset_symbol],
                note_modes=note_modes,
                crypto_policy=crypto_policy,
            ),
        )
        mid = market.mid_yes_dollars or market.last_price_dollars or Decimal("0.5000")
        market_row = _crypto_live_market_row(
            market,
            spot_rows=spot_rows,
            cross_asset_spot=cross_asset_spot,
            funding_rate_rows=funding_rate_rows,
            prior_quote=prior_quote,
            settings=self.settings,
        )
        features = {**features, "spot_features": _json_ready_spot_features(market_row)}
        empirical_bucket_matrix = _crypto_empirical_bucket_matrix_from_artifacts(gate, backtest)
        last_minute_passive_price_matrix = _crypto_last_minute_passive_price_matrix_from_artifacts(gate, backtest)
        btc_1h_touch_configured = _crypto_btc_1h_touch_policy_configured_for_row(market_row, settings=self.settings)
        btc_1h_touch_allowed = btc_1h_touch_configured and _crypto_touch_replay_gate_passed(touch_gate)
        if btc_1h_touch_configured and not btc_1h_touch_allowed:
            features = {
                **features,
                "touch_strategy_ignored": {
                    "enabled": True,
                    "policy": "btc_1h_touch20",
                    "reason": "touch20_replay_gate_missing_or_blocked",
                    "gate_status": getattr(touch_gate, "status", None) if touch_gate is not None else "missing",
                    "gate_version": getattr(touch_gate, "version", None) if touch_gate is not None else None,
                },
            }
            return self._stand_down(
                market,
                StandDownReason.CRYPTO_MODEL_UNAVAILABLE,
                "BTC 1h touch strategy is enabled, but the touch +20% replay gate is missing or blocked; stand down.",
                features,
                fair=mid,
            )
        touch_strategy_ignored = (
            bool(self.settings.crypto_touch_strategy_enabled)
            and bool(self.settings.crypto_model_trained_replay_only)
            and not btc_1h_touch_allowed
        )
        if touch_strategy_ignored:
            features = {
                **features,
                "touch_strategy_ignored": {
                    "enabled": True,
                    "reason": "model_trained_replay_only",
                },
            }
        touch_strategy_allowed = (
            (bool(self.settings.crypto_touch_strategy_enabled) and not bool(self.settings.crypto_model_trained_replay_only))
            or btc_1h_touch_allowed
        )
        if touch_strategy_allowed:
            payload = {
                "model_type": "deterministic_touch_strategy",
                "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            }
            fair = _clamp_price(mid)
        elif artifact is None or artifact.status != "trained":
            return self._stand_down(
                market,
                StandDownReason.CRYPTO_MODEL_UNAVAILABLE,
                "Crypto model artifact is missing or not trained; stand down.",
                features,
                fair=mid,
            )
        else:
            payload = artifact.payload or {}
            fair = _predict_crypto_probability(market_row, payload)
            fair, _direction_mismatch = _crypto_model_spot_direction_check(
                market_row, fair, settings=self.settings
            )
            if _direction_mismatch is not None:
                logger.warning(
                    "crypto model-spot direction conflict: %s asset=%s fair_raw=%s corrected=%s moneyness=%s",
                    _direction_mismatch["reason"],
                    market.asset_symbol,
                    _direction_mismatch["raw_model_fair_yes"],
                    _direction_mismatch["corrected_fair_yes"],
                    _direction_mismatch["spot_moneyness_pct"],
                )
            features = {
                **features,
                "model_spot_direction_check": _direction_mismatch,
            }
        action, side, target_yes, edge_bps, trace = _crypto_recommendation(
            market=market,
            fair_yes=fair,
            settings=self.settings,
            crypto_policy=crypto_policy,
            row=market_row,
            empirical_bucket_matrix=empirical_bucket_matrix,
            last_minute_passive_price_matrix=last_minute_passive_price_matrix,
            enforce_empirical_bucket_gate=True,
            touch_replay_gate=touch_gate,
            edge_shrinkage=edge_shrinkage,
        )
        entry_policy = crypto_policy.entry_for_asset(market.asset_symbol, frequency=market.frequency)
        runtime_trading_enabled = self.settings.crypto_trading_enabled or crypto_policy.trading_enabled
        trade_fair = _decimal(trace.get("fair_yes_dollars") or fair)
        confidence = min(0.95, max(float(entry_policy["min_confidence"]), 0.80 + abs(edge_bps) / 20000))
        eligibility = None
        stand_down_reason = None
        outcome = trace["outcome"]
        display_side = side.value if side is not None else trace.get("selected_side")
        display_decision = (
            f"recommend {display_side.upper()}"
            if side is not None and display_side
            else f"predict {str(display_side).upper()}" if display_side else "no trade"
        )
        frequency_label = crypto_frequency_label(market.frequency)
        strategy_code = crypto_strategy_code_for_frequency(market.frequency)
        summary = (
            f"{market.asset_symbol} {frequency_label} market-anchored fair yes {trade_fair} "
            f"(model {fair}); "
            f"{display_decision} edge {edge_bps}bps."
        )
        if side is None:
            stand_down_reason = StandDownReason.NO_ACTIONABLE_EDGE
            eligibility = TradeEligibilityVerdict(
                eligible=False,
                reasons=["Predicted crypto winner does not clear live trading gates."],
                stand_down_reason=stand_down_reason,
                evaluation_outcome=outcome,
                candidate_trace=trace,
            )
        else:
            eligibility = TradeEligibilityVerdict(
                eligible=True,
                reasons=[],
                evaluation_outcome=outcome,
                candidate_trace=trace,
            )
        return StrategySignal(
            fair_yes_dollars=trade_fair,
            confidence=confidence,
            edge_bps=edge_bps,
            recommended_action=action,
            recommended_side=side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            resolution_state=WeatherResolutionState.UNRESOLVED,
            eligibility=eligibility,
            stand_down_reason=stand_down_reason,
            evaluation_outcome=outcome,
            candidate_trace={
                **trace,
                "market_domain": "crypto",
                "frequency": market.frequency,
                "strategy_code": strategy_code,
                "features": features,
                "model_version": artifact.version if artifact is not None else "deterministic-touch-strategy",
                "model_metrics": artifact.metrics if artifact is not None else {},
                "empirical_bucket_matrix_count": len(empirical_bucket_matrix),
                "last_minute_passive_price_matrix_count": len(last_minute_passive_price_matrix),
                "prediction_model": {
                    "baseline_probability": _money_text(mid),
                    "calibrated_probability": _money_text(fair),
                    "market_anchored_probability": _money_text(trade_fair),
                    "market_price_anchor": trace.get("market_price_anchor"),
                    "calibration_version": artifact.version if artifact is not None else None,
                    "model_version": artifact.version if artifact is not None else "deterministic-touch-strategy",
                    "feature_schema_version": payload.get("feature_schema_version"),
                    "model_type": payload.get("model_type"),
                    "candidate_registry_version": payload.get("candidate_registry_version"),
                    "candidate_champion": (payload.get("candidate_report") or {}).get("champion_name") if isinstance(payload.get("candidate_report"), dict) else None,
                    "ensemble_weights": payload.get("ensemble_weights"),
                    "status": artifact.status if artifact is not None else "deterministic",
                    "metric_deltas": _crypto_metric_deltas(artifact.metrics or {}) if artifact is not None else {},
                    "reason": None,
                },
                "trade_selection_model": {
                    "expected_net_pnl": _money_text(
                        _expected_crypto_net_pnl(
                            market,
                            side,
                            trade_fair,
                            fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
                        )
                        if side is not None
                        else None
                    ),
                    "candidate_status": trace.get("candidate_status"),
                    "expected_net_edge": trace.get("expected_net_edge"),
                    "edge_shrinkage": trace.get("edge_shrinkage"),
                    "shrunk_edge_bps": trace.get("shrunk_edge_bps"),
                    "rank": trace.get("rank"),
                    "bucket_key": trace.get("bucket_key"),
                    "empirical_bucket_gate": trace.get("empirical_bucket_gate"),
                    "empirical_bucket_status": trace.get("empirical_bucket_status"),
                    "empirical_bucket_late_override": trace.get("empirical_bucket_late_override"),
                    "empirical_bucket_gap_sample": trace.get("empirical_bucket_gap_sample"),
                    "last_minute_passive_market_confidence": trace.get("last_minute_passive_market_confidence") is True,
                    "last_minute_passive": trace.get("last_minute_passive"),
                    "last_minute_passive_bid_threshold_dollars": trace.get("last_minute_passive_bid_threshold_dollars"),
                    "last_minute_price_source": trace.get("last_minute_price_source"),
                    "last_minute_chosen_bid_dollars": trace.get("last_minute_chosen_bid_dollars"),
                    "last_minute_fixed_fallback_bid_dollars": trace.get("last_minute_fixed_fallback_bid_dollars"),
                    "last_minute_price_matrix_key": trace.get("last_minute_price_matrix_key"),
                    "last_minute_price_matrix_base_key": trace.get("last_minute_price_matrix_base_key"),
                    "last_minute_price_matrix_sample_count": trace.get("last_minute_price_matrix_sample_count"),
                    "last_minute_price_matrix_fill_count": trace.get("last_minute_price_matrix_fill_count"),
                    "last_minute_price_matrix_fill_rate": trace.get("last_minute_price_matrix_fill_rate"),
                    "last_minute_price_matrix_net_pnl": trace.get("last_minute_price_matrix_net_pnl"),
                    "last_minute_price_matrix_net_pnl_per_signal": trace.get("last_minute_price_matrix_net_pnl_per_signal"),
                    "last_minute_passive_no_cross": trace.get("last_minute_passive_no_cross"),
                    "decision": "selected" if side is not None else "stand_down",
                    "status": "shadow_only" if trace.get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW else trace.get("candidate_status"),
                    "reason": trace.get("selection_reason") or ("crypto_live_trading_disabled" if not runtime_trading_enabled else None),
                    "backtest_version": backtest.version if backtest is not None else None,
                    "replay_gate_status": gate.status if gate is not None else "missing",
                    "touch_replay_gate_status": touch_gate.status if touch_gate is not None else "missing",
                    "runtime_crypto_policy": _runtime_crypto_policy_payload(
                        crypto_policy,
                        asset_symbol=market.asset_symbol,
                        frequency=market.frequency,
                    ),
                },
            },
            capital_bucket="safe",
            confidence_band="high" if confidence >= 0.85 else "medium",
        )

    def features(self, market: CryptoMarket) -> dict[str, Any]:
        now = datetime.now(UTC)
        time_to_close_seconds = None
        if market.close_time is not None:
            time_to_close_seconds = max(0, int((market.close_time - now).total_seconds()))
        return {
            "asset": market.asset_symbol,
            "time_to_close_seconds": time_to_close_seconds,
            "target_price_dollars": _money_text(market.target_price_dollars),
            "yes_bid_dollars": _money_text(market.yes_bid_dollars),
            "yes_ask_dollars": _money_text(market.yes_ask_dollars),
            "no_bid_dollars": _money_text(market.no_bid_dollars),
            "no_ask_dollars": _money_text(market.no_ask_dollars),
            "spread_bps": market.spread_bps,
            "mid_yes_dollars": _money_text(market.mid_yes_dollars),
            "last_price_dollars": _money_text(market.last_price_dollars),
            "volume": market.volume,
            "open_interest": market.open_interest,
        }

    def _stand_down(
        self,
        market: CryptoMarket,
        reason: StandDownReason,
        summary: str,
        features: dict[str, Any],
        *,
        fair: Decimal | None = None,
    ) -> StrategySignal:
        fair_yes = _clamp_price(fair or market.mid_yes_dollars or market.last_price_dollars or Decimal("0.5000"))
        return StrategySignal(
            fair_yes_dollars=fair_yes,
            confidence=0.0,
            edge_bps=0,
            recommended_action=None,
            recommended_side=None,
            target_yes_price_dollars=None,
            summary=summary,
            resolution_state=WeatherResolutionState.UNRESOLVED,
            eligibility=TradeEligibilityVerdict(
                eligible=False,
                reasons=[summary],
                stand_down_reason=reason,
                evaluation_outcome="stand_down",
                candidate_trace={"market_domain": "crypto", "features": features},
            ),
            stand_down_reason=reason,
            evaluation_outcome="stand_down",
            candidate_trace={
                "market_domain": "crypto",
                "frequency": market.frequency,
                "strategy_code": crypto_strategy_code_for_frequency(market.frequency),
                "stand_down_reason": reason.value,
                "features": features,
            },
        )


class CryptoReplayService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        agent_pack_service: AgentPackService | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.agent_pack_service = agent_pack_service or AgentPackService(settings)

    async def run(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        limit: int | None = None,
        persist: bool = True,
        asset_symbols: list[str] | None = None,
        objective: str = "settlement",
    ) -> dict[str, Any]:
        if _crypto_objective_is_touch20(objective):
            report = await self._build_touch20_report(
                frequency=frequency,
                days=days,
                limit=limit,
                command="run",
                asset_symbols=asset_symbols,
            )
            if persist:
                async with self.session_factory() as session:
                    repo = PlatformRepository(session)
                    artifact = await repo.record_crypto_model_artifact(
                        frequency=report["frequency"],
                        artifact_type=_crypto_artifact_type("backtest_touch20", report.get("asset_symbols") or []),
                        version=_version(f"crypto-{report['frequency']}-touch20-backtest", report),
                        status=report["status"],
                        sample_count=int((report.get("metrics") or {}).get("trade_candidate_count") or 0),
                        metrics=report.get("metrics") or {},
                        payload=report,
                        kalshi_env=self.settings.kalshi_env,
                        trained_at=datetime.now(UTC),
                    )
                    await session.commit()
                report["version"] = artifact.version
            return report

        report = await self._build_report(
            frequency=frequency,
            days=days,
            limit=limit,
            command="run",
            asset_symbols=asset_symbols,
        )
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                artifact = await repo.record_crypto_model_artifact(
                    frequency=report["frequency"],
                    artifact_type=_crypto_artifact_type("backtest", report.get("asset_symbols") or []),
                    version=_version(f"crypto-{report['frequency']}-backtest", report),
                    status=report["status"],
                    sample_count=int((report.get("dataset") or {}).get("row_count") or 0),
                    metrics=report.get("metrics") or {},
                    payload=report,
                    kalshi_env=self.settings.kalshi_env,
                    trained_at=datetime.now(UTC),
                )
                per_asset_metrics = (report.get("metrics") or {}).get("per_asset_metrics") or {}
                for asset, asset_metrics in per_asset_metrics.items():
                    await repo.record_crypto_model_artifact(
                        frequency=report["frequency"],
                        artifact_type=_crypto_artifact_type("backtest", [asset]),
                        version=_version(f"crypto-{report['frequency']}-backtest", report),
                        status=report["status"],
                        sample_count=int(asset_metrics.get("oos_trade_candidate_count") or 0),
                        metrics={**(report.get("metrics") or {}), **asset_metrics, "metrics_scope": "per_asset"},
                        payload={"asset_symbol": asset, "pooled_version": artifact.version},
                        kalshi_env=self.settings.kalshi_env,
                        trained_at=datetime.now(UTC),
                    )
                await session.commit()
            report["version"] = artifact.version
        return report

    async def validate(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        limit: int | None = None,
        asset_symbols: list[str] | None = None,
        objective: str = "settlement",
    ) -> dict[str, Any]:
        if _crypto_objective_is_touch20(objective):
            return await self._build_touch20_report(
                frequency=frequency,
                days=days,
                limit=limit,
                command="validate",
                asset_symbols=asset_symbols,
            )
        return await self._build_report(
            frequency=frequency,
            days=days,
            limit=limit,
            command="validate",
            asset_symbols=asset_symbols,
        )

    async def optimize_entry_policy(
        self,
        *,
        frequency: str = "15m",
        days: int | None = 30,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            feature_decision_rows = (
                await _crypto_training_feature_decision_rows(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=200_000,
                )
                if self.settings.crypto_training_feature_store_enabled
                else []
            )
            snapshots: list[CryptoMarketSnapshotRecord] = []
            candles: list[CryptoMarketCandlestickRecord] = []
            spot_rows: list[CryptoSpotOHLCRecord] = []
            if not feature_decision_rows:
                snapshots = await repo.list_crypto_market_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    settled_only=True,
                    limit=200_000,
                )
                live_quote_snapshots = await repo.list_crypto_live_quote_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=200_000,
                )
                if live_quote_snapshots:
                    snapshots = _dedupe_crypto_snapshot_rows([*snapshots, *live_quote_snapshots])
                candles = await repo.list_crypto_market_candlesticks(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=500_000,
                )
                spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    requested_assets=requested_assets,
                    since=cutoff,
                    limit=1_000_000,
                )
            funding_rate_rows = await repo.list_crypto_funding_rates_bulk(
                asset_symbols=requested_assets or None,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            await session.commit()
        if feature_decision_rows:
            rows = _filter_crypto_dict_rows(feature_decision_rows, requested_assets)
        else:
            snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
            candles = _filter_crypto_snapshot_rows(candles, requested_assets)
            spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
            snapshots = _filter_snapshots_by_per_asset_funding_cutoff(funding_rate_rows, snapshots)
            rows = _crypto_decision_rows(snapshots, candles, spot_rows, funding_rate_rows=funding_rate_rows, settings=self.settings)
        rows.sort(key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
        assets = requested_assets or sorted({normalize_asset_symbol(str(row.get("asset_symbol") or "")) for row in rows if row.get("asset_symbol")})
        asset_reports: list[dict[str, Any]] = []
        staged_overrides: dict[str, dict[str, Any]] = {}
        for asset in assets:
            asset_rows = [row for row in rows if normalize_asset_symbol(str(row.get("asset_symbol") or "")) == asset]
            report = _crypto_optimize_asset_entry_policy(
                asset,
                asset_rows,
                settings=self.settings,
                crypto_policy=crypto_policy,
                frequency=freq,
            )
            asset_reports.append(report)
            winner = report.get("winner")
            if report.get("status") == "stageable" and isinstance(winner, dict):
                entry = winner.get("entry_policy")
                if isinstance(entry, dict):
                    staged_overrides[str(report.get("override_key") or crypto_entry_override_key(asset, freq))] = entry
        return {
            "schema_version": "crypto-entry-policy-optimizer-v1",
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "assets": assets,
            "grid": CRYPTO_ENTRY_OPTIMIZER_GRID,
            "requirements": {
                "min_oos_trade_candidates": crypto_policy.replay_min_trade_candidates,
                "min_net_pl_dollars": crypto_policy.replay_min_net_pl_dollars,
                "min_pnl_advantage_dollars": crypto_policy.replay_min_pnl_advantage_dollars,
                "max_hard_cap_breaches": crypto_policy.replay_max_hard_cap_breaches,
                "min_spot_coverage_pct": crypto_policy.replay_min_spot_coverage_pct,
            },
            "asset_reports": asset_reports,
            "stageable_assets": sorted(staged_overrides),
            "staged_override_payload": (
                {"crypto_policy": {"asset_entry_overrides": staged_overrides}}
                if staged_overrides
                else None
            ),
        }

    async def gate(
        self,
        *,
        frequency: str = "15m",
        asset_symbols: list[str] | None = None,
        objective: str = "settlement",
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        if _crypto_objective_is_touch20(objective):
            touch_assets = requested_assets or ["BTC"]
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                backtest = await repo.get_latest_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_crypto_artifact_type("backtest_touch20", touch_assets),
                    kalshi_env=self.settings.kalshi_env,
                )
                metrics = dict((backtest.metrics if backtest is not None else None) or {})
                if backtest is None:
                    metrics["backtest_missing"] = True
                reasons = _crypto_touch_replay_gate_reasons(metrics, settings=self.settings)
                gate = {
                    "passed": not reasons,
                    "reasons": reasons,
                    "requirements": {
                        "min_trade_candidates": self.settings.crypto_1h_touch_replay_min_candidates,
                        "min_net_pl_dollars": self.settings.crypto_1h_touch_replay_min_net_pnl_dollars,
                        "min_pnl_per_candidate_dollars": self.settings.crypto_1h_touch_replay_min_pnl_per_candidate_dollars,
                        "max_hard_cap_breaches": self.settings.crypto_1h_touch_replay_max_hard_cap_breaches,
                        "min_touch_rate": self.settings.crypto_1h_touch_replay_min_touch_rate,
                        "requires_allowed_bucket_support": True,
                    },
                    "objective": "touch_20pct_before_close",
                }
                artifact = await repo.record_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_crypto_artifact_type("replay_gate_touch20", touch_assets),
                    version=_version(f"crypto-{freq}-touch20-gate", gate),
                    status="passed" if gate["passed"] else "blocked",
                    sample_count=int(metrics.get("trade_candidate_count") or 0),
                    metrics=metrics,
                    payload=gate,
                    kalshi_env=self.settings.kalshi_env,
                    trained_at=datetime.now(UTC),
                )
                control = await repo.ensure_deployment_control(self.settings.app_color)
                notes = dict(control.notes or {})
                notes.update(
                    _crypto_touch_replay_gate_note_updates(
                        frequency=freq,
                        asset_symbols=touch_assets,
                        status=artifact.status,
                        version=artifact.version,
                        updated_at=datetime.now(UTC),
                        reasons=list(gate["reasons"]),
                    )
                )
                control.notes = notes
                await session.commit()
            return {
                "status": artifact.status,
                "kalshi_env": self.settings.kalshi_env,
                "asset_symbols": touch_assets,
                "version": artifact.version,
                **gate,
            }
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("model", requested_assets),
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("backtest", requested_assets),
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict(
                (backtest.metrics if backtest is not None else None)
                or (model.metrics if model is not None else {})
                or {}
            )
            if model is None:
                metrics["model_missing"] = True
            if backtest is None:
                metrics["backtest_missing"] = True
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            if len(requested_assets) > 1 and (model is None or backtest is None):
                metrics, gate = await self._gate_from_per_asset_artifacts(
                    repo,
                    frequency=freq,
                    asset_symbols=requested_assets,
                    crypto_policy=crypto_policy,
                )
            else:
                gate = self.evaluate_gate(metrics, crypto_policy=crypto_policy)
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("replay_gate", requested_assets),
                version=_version(f"crypto-{freq}-gate", gate),
                status="passed" if gate["passed"] else "blocked",
                sample_count=int(metrics.get("resolved_sample_count") or 0),
                metrics=metrics,
                payload=gate,
                kalshi_env=self.settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            control = await repo.ensure_deployment_control(self.settings.app_color)
            notes = dict(control.notes or {})
            notes.update(
                _crypto_replay_gate_note_updates(
                    frequency=freq,
                    asset_symbols=requested_assets,
                    status=artifact.status,
                    version=artifact.version,
                    updated_at=datetime.now(UTC),
                    reasons=list(gate["reasons"]),
                )
            )
            control.notes = notes
            await session.commit()
        return {
            "status": artifact.status,
            "kalshi_env": self.settings.kalshi_env,
            "asset_symbols": requested_assets,
            "version": artifact.version,
            **gate,
        }

    async def _gate_from_per_asset_artifacts(
        self,
        repo: PlatformRepository,
        *,
        frequency: str,
        asset_symbols: list[str],
        crypto_policy: RuntimeCryptoPolicy,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        asset_results: list[dict[str, Any]] = []
        aggregate_metrics: dict[str, Any] = {
            "asset_count": len(asset_symbols),
            "aggregate_source": "per_asset_artifacts",
            "asset_symbols": asset_symbols,
        }
        count_keys = (
            "sample_count",
            "resolved_sample_count",
            "prediction_eligible_count",
            "strict_trade_eligible_count",
            "proxy_quote_row_count",
            "real_quote_row_count",
            "trade_candidate_count",
            "current_model_live_quality_candidate_count",
            "live_quality_candidate_count",
            "exploratory_shadow_candidate_count",
            "oos_trade_candidate_count",
            "oos_fold_count",
            "hard_cap_breaches",
            "candle_count",
            "leakage_row_count",
            "last_minute_passive_price_matrix_count",
        )
        money_keys = (
            "net_simulated_pl_dollars",
            "market_mid_net_simulated_pl_dollars",
            "pnl_advantage_vs_market_mid_dollars",
            "oos_net_simulated_pl_dollars",
            "oos_market_mid_net_simulated_pl_dollars",
            "oos_pnl_advantage_vs_market_mid_dollars",
            "fees_dollars",
        )
        counter_keys = (
            "candidate_status_counts",
            "candidate_reason_counts",
            "top_candidate_status_counts",
            "top_candidate_reason_counts",
            "candidate_rejection_reason_counts",
        )
        weighted_keys = (
            "spot_feature_coverage_pct",
            "calibration_brier",
            "market_mid_brier",
            "calibration_log_loss",
            "market_mid_log_loss",
            "calibration_ece",
            "market_mid_ece",
        )
        # Market-grouped keys: only aggregated when every asset artifact carries
        # them, so mixed old/new artifacts keep row-count gate behavior.
        market_count_keys = (
            "resolved_market_count",
            "strict_trade_eligible_market_count",
            "trade_candidate_market_count",
            "current_model_live_quality_market_count",
            "oos_trade_candidate_market_count",
        )
        market_weighted_keys = (
            "calibration_brier_market_weighted",
            "market_mid_brier_market_weighted",
            "calibration_log_loss_market_weighted",
            "market_mid_log_loss_market_weighted",
            "calibration_ece_market_weighted",
            "market_mid_ece_market_weighted",
        )
        market_count_totals: dict[str, int] = dict.fromkeys(market_count_keys, 0)
        market_count_missing: set[str] = set()
        market_weighted_totals: dict[str, float] = dict.fromkeys(market_weighted_keys, 0.0)
        market_weighted_counts: dict[str, int] = dict.fromkeys(market_weighted_keys, 0)
        weighted_totals: dict[str, float] = {key: 0.0 for key in weighted_keys}
        weighted_counts: dict[str, int] = {key: 0 for key in weighted_keys}
        aggregate_reasons: list[str] = []
        oos_statuses: set[str] = set()
        missing_model_assets: list[str] = []
        missing_backtest_assets: list[str] = []
        aggregate_bucket_matrix: list[dict[str, Any]] = []
        aggregate_last_minute_price_matrix: list[dict[str, Any]] = []

        for asset in asset_symbols:
            model = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type=_crypto_artifact_type("model", [asset]),
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type=_crypto_artifact_type("backtest", [asset]),
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict(
                (backtest.metrics if backtest is not None else None)
                or (model.metrics if model is not None else {})
                or {}
            )
            if model is None:
                metrics["model_missing"] = True
                missing_model_assets.append(asset)
            if backtest is None:
                metrics["backtest_missing"] = True
                missing_backtest_assets.append(asset)
            gate = self.evaluate_gate(metrics, crypto_policy=crypto_policy)
            if not gate["passed"]:
                aggregate_reasons.extend(f"{asset}: {reason}" for reason in gate["reasons"])
            aggregate_bucket_matrix.extend(
                bucket for bucket in metrics.get("bucket_matrix") or [] if isinstance(bucket, dict)
            )
            aggregate_last_minute_price_matrix.extend(
                row for row in metrics.get("last_minute_passive_price_matrix") or [] if isinstance(row, dict)
            )
            asset_results.append(
                {
                    "asset": asset,
                    "status": "passed" if gate["passed"] else "blocked",
                    "reasons": gate["reasons"],
                    "model": _artifact_summary(model),
                    "backtest": _artifact_summary(backtest),
                    "metrics": {
                        key: metrics.get(key)
                        for key in (
                            "resolved_sample_count",
                            "strict_trade_eligible_count",
                            "current_model_live_quality_candidate_count",
                            "oos_trade_candidate_count",
                            "oos_net_simulated_pl_dollars",
                            "oos_pnl_advantage_vs_market_mid_dollars",
                            "spot_feature_coverage_pct",
                        )
                    },
                }
            )
            for key in count_keys:
                aggregate_metrics[key] = int(aggregate_metrics.get(key) or 0) + int(metrics.get(key) or 0)
            for key in money_keys:
                aggregate_metrics[key] = float(aggregate_metrics.get(key) or 0.0) + float(metrics.get(key) or 0.0)
            for key in counter_keys:
                counter = Counter(aggregate_metrics.get(key) or {})
                counter.update(metrics.get(key) or {})
                aggregate_metrics[key] = dict(counter)
            weight = int(metrics.get("resolved_sample_count") or metrics.get("sample_count") or 0)
            for key in weighted_keys:
                value = metrics.get(key)
                if value is None or weight <= 0:
                    continue
                weighted_totals[key] += float(value) * weight
                weighted_counts[key] += weight
            for key in market_count_keys:
                if metrics.get(key) is None:
                    market_count_missing.add(key)
                else:
                    market_count_totals[key] += int(metrics.get(key) or 0)
            market_weight = int(metrics.get("resolved_market_count") or 0)
            for key in market_weighted_keys:
                value = metrics.get(key)
                if value is None or market_weight <= 0:
                    continue
                market_weighted_totals[key] += float(value) * market_weight
                market_weighted_counts[key] += market_weight
            oos_statuses.add(str(metrics.get("oos_evaluation_status") or "").strip().lower())

        for key in weighted_keys:
            if weighted_counts[key] > 0:
                aggregate_metrics[key] = weighted_totals[key] / weighted_counts[key]
        for key in market_count_keys:
            if key not in market_count_missing:
                aggregate_metrics[key] = market_count_totals[key]
        for key in market_weighted_keys:
            if market_weighted_counts[key] > 0:
                aggregate_metrics[key] = market_weighted_totals[key] / market_weighted_counts[key]
        aggregate_metrics["missing_model_assets"] = missing_model_assets
        aggregate_metrics["missing_backtest_assets"] = missing_backtest_assets
        aggregate_metrics["model_missing"] = bool(missing_model_assets)
        aggregate_metrics["backtest_missing"] = bool(missing_backtest_assets)
        aggregate_metrics["oos_evaluation_status"] = (
            "ok"
            if oos_statuses <= {"", "ok"} and int(aggregate_metrics.get("oos_fold_count") or 0) > 0
            else "partial"
        )
        aggregate_metrics["asset_gate_results"] = asset_results
        aggregate_metrics["last_minute_passive_price_matrix"] = aggregate_last_minute_price_matrix
        aggregate_metrics["last_minute_passive_price_matrix_count"] = len(aggregate_last_minute_price_matrix)
        aggregate_metrics = _crypto_metrics_with_empirical_buckets(
            aggregate_metrics,
            bucket_matrix=aggregate_bucket_matrix,
            settings=self.settings,
            crypto_policy=crypto_policy,
            requested_asset_symbols=asset_symbols,
            force_requested_assets=bool(asset_symbols),
        )
        aggregate_gate = self.evaluate_gate(aggregate_metrics, crypto_policy=crypto_policy)
        gate = {
            **aggregate_gate,
            "passed": not aggregate_reasons and aggregate_gate["passed"],
            "reasons": aggregate_reasons or aggregate_gate["reasons"],
            "aggregate_source": "per_asset_artifacts",
            "asset_gate_results": asset_results,
        }
        return aggregate_metrics, gate

    def evaluate_gate(
        self,
        metrics: dict[str, Any],
        *,
        crypto_policy: RuntimeCryptoPolicy | None = None,
    ) -> dict[str, Any]:
        runtime_policy = crypto_policy or self.agent_pack_service.runtime_crypto_policy()
        reasons = _crypto_replay_gate_reasons(metrics, crypto_policy=runtime_policy)
        return {
            "passed": not reasons,
            "reasons": reasons,
            "requirements": {
                "min_resolved_markets": runtime_policy.replay_min_resolved_markets,
                "min_trade_candidates": runtime_policy.replay_min_trade_candidates,
                "min_net_pl_dollars": runtime_policy.replay_min_net_pl_dollars,
                "max_hard_cap_breaches": runtime_policy.replay_max_hard_cap_breaches,
                "pnl_beats_market_mid": runtime_policy.replay_require_pnl_beats_market_mid,
                "min_pnl_advantage_dollars": runtime_policy.replay_min_pnl_advantage_dollars,
                "calibration_better_than_mid": runtime_policy.replay_require_calibration_better_than_mid,
                "calibration_metrics_diagnostic": ["brier", "log_loss", "ece"],
                "requires_out_of_sample_replay": True,
                "requires_candles": True,
                "requires_point_in_time_rows": True,
                "min_spot_coverage_pct": runtime_policy.replay_min_spot_coverage_pct,
                "requires_real_quotes_for_strict_trade_quality": True,
            },
        }

    async def _build_touch20_report(
        self,
        *,
        frequency: str,
        days: int | None,
        limit: int | None,
        command: str,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "1h"
        requested_assets = normalize_asset_symbols(asset_symbols) or ["BTC"]
        requested_assets = [asset for asset in requested_assets if asset == "BTC"] or ["BTC"]
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            snapshots = await repo.list_crypto_settled_live_quote_path_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets,
                since=cutoff,
                limit=limit or 200_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                asset_symbols=requested_assets,
                since=cutoff,
                limit=500_000,
            )
            spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                repo,
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                requested_assets=requested_assets,
                since=cutoff,
                limit=1_000_000,
            )
            funding_rate_rows = await repo.list_crypto_funding_rates_bulk(
                asset_symbols=requested_assets,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            await session.commit()
        dataset_source = "settled_live_quote_paths"
        snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
        candles = _filter_crypto_snapshot_rows(candles, requested_assets)
        spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
        snapshots = _filter_snapshots_by_per_asset_funding_cutoff(funding_rate_rows, snapshots)
        rebuilt_rows = _crypto_decision_rows(snapshots, candles, spot_rows, funding_rate_rows=funding_rate_rows, settings=self.settings)
        proxy_quote_row_count = sum(1 for row in rebuilt_rows if row.get("quote_source") != "snapshot_quotes")
        rows = [
            row
            for row in rebuilt_rows
            if row.get("quote_source") == "snapshot_quotes"
            and row.get("strict_trade_eligible") is True
            and row.get("yes_bid_dollars") is not None
            and row.get("yes_ask_dollars") is not None
            and row.get("no_bid_dollars") is not None
            and row.get("no_ask_dollars") is not None
        ]
        rows.sort(key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
        if limit and limit > 0:
            rows = rows[-limit:]
        replay = _evaluate_crypto_touch20_replay(
            rows,
            settings=self.settings,
            crypto_policy=crypto_policy,
        )
        metrics = replay.get("metrics") or {}
        metrics["dataset_source"] = dataset_source
        metrics["real_quote_path_snapshot_count"] = len(snapshots)
        metrics["real_quote_path_row_count"] = len(rows)
        metrics["proxy_quote_row_count"] = proxy_quote_row_count
        if len(rows) <= 0:
            metrics["touch_replay_data_missing_reason"] = "missing_real_quote_path_evidence"
        reasons = _crypto_touch_replay_gate_reasons(metrics, settings=self.settings)
        gate = {
            "passed": not reasons,
            "reasons": reasons,
            "objective": "touch_20pct_before_close",
            "requirements": {
                "min_trade_candidates": self.settings.crypto_1h_touch_replay_min_candidates,
                "min_net_pl_dollars": self.settings.crypto_1h_touch_replay_min_net_pnl_dollars,
                "min_pnl_per_candidate_dollars": self.settings.crypto_1h_touch_replay_min_pnl_per_candidate_dollars,
                "max_hard_cap_breaches": self.settings.crypto_1h_touch_replay_max_hard_cap_breaches,
                "min_touch_rate": self.settings.crypto_1h_touch_replay_min_touch_rate,
                "requires_allowed_bucket_support": True,
            },
        }
        issues: list[dict[str, Any]] = []
        for reason in reasons:
            issues.append({
                "severity": "fail" if command == "validate" else "warn",
                "code": _issue_code(reason),
                "message": reason,
            })
        status = "pass"
        if any(issue["severity"] == "fail" for issue in issues):
            status = "fail"
        elif any(issue["severity"] == "warn" for issue in issues):
            status = "warn"
        return {
            "schema_version": "crypto-touch20-backtest-report-v1",
            "status": status,
            "command": command,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "asset_symbols": requested_assets,
            "objective": "touch_20pct_before_close",
            "dataset": {
                "source": dataset_source,
                "row_count": len(rows),
                "rebuilt_row_count": len(rebuilt_rows),
                "real_quote_path_row_count": len(rows),
                "proxy_quote_row_count": proxy_quote_row_count,
                "snapshot_count": len(snapshots),
                "settled_snapshot_count": sum(1 for row in snapshots if row.settlement_result in {"yes", "no"}),
                "candlestick_count": len(candles),
                "spot_row_count": len(spot_rows),
                "assets": sorted({str(row.get("asset_symbol")) for row in rows}),
            },
            "touch_replay": replay,
            "metrics": metrics,
            "promotion_gate": gate,
            "issues": issues,
        }

    async def _build_report(
        self,
        *,
        frequency: str,
        days: int | None,
        limit: int | None,
        command: str,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = normalize_asset_symbols(asset_symbols)
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        replay_source_limit = max(int(limit or 0), 200_000)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            feature_decision_rows = (
                await _crypto_training_feature_decision_rows(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=replay_source_limit,
                )
                if self.settings.crypto_training_feature_store_enabled
                else []
            )
            snapshots: list[CryptoMarketSnapshotRecord] = []
            candles: list[CryptoMarketCandlestickRecord] = []
            spot_rows: list[CryptoSpotOHLCRecord] = []
            if not feature_decision_rows:
                snapshots = await repo.list_crypto_market_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    settled_only=True,
                    limit=200_000,
                )
                live_quote_snapshots = await repo.list_crypto_live_quote_snapshots(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=200_000,
                )
                if live_quote_snapshots:
                    snapshots = _dedupe_crypto_snapshot_rows([*snapshots, *live_quote_snapshots])
                candles = await repo.list_crypto_market_candlesticks(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    asset_symbols=requested_assets or None,
                    since=cutoff,
                    limit=500_000,
                )
                spot_rows = await _list_crypto_spot_rows_with_cross_assets(
                    repo,
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    requested_assets=requested_assets,
                    since=cutoff,
                    limit=1_000_000,
                )
            funding_rate_rows = await repo.list_crypto_funding_rates_bulk(
                asset_symbols=requested_assets or None,
            )
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type=_crypto_artifact_type("model", requested_assets),
                kalshi_env=self.settings.kalshi_env,
            )
            active_pack = await self.agent_pack_service.get_active_pack(repo)
            crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            note_modes = CryptoAssetControlService(
                settings=self.settings,
                session_factory=self.session_factory,
            ).modes_from_notes(getattr(control, "notes", None))
            await session.commit()
        dataset_source = "crypto_training_feature_rows" if feature_decision_rows else "settled_snapshots_rebuilt"
        if feature_decision_rows:
            rows = _filter_crypto_dict_rows(feature_decision_rows, requested_assets)
        else:
            snapshots = _filter_crypto_snapshot_rows(snapshots, requested_assets)
            candles = _filter_crypto_snapshot_rows(candles, requested_assets)
            spot_rows = _filter_crypto_snapshot_rows(spot_rows, _crypto_spot_feature_asset_scope(requested_assets))
            snapshots = _filter_snapshots_by_per_asset_funding_cutoff(funding_rate_rows, snapshots)
            rows = _crypto_decision_rows(snapshots, candles, spot_rows, funding_rate_rows=funding_rate_rows, settings=self.settings)
        rows.sort(key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
        if limit and limit > 0:
            rows = _crypto_limit_replay_rows_for_oos(rows, limit=limit)
        mode_assets = requested_assets or sorted(
            {normalize_asset_symbol(str(row.get("asset_symbol") or "")) for row in rows if row.get("asset_symbol")}
        )
        crypto_policy = _runtime_crypto_policy_with_asset_modes(
            crypto_policy,
            _resolved_crypto_asset_modes(
                asset_symbols=mode_assets,
                note_modes=note_modes,
                crypto_policy=crypto_policy,
            ),
        )
        model_payload = model.payload if model is not None and isinstance(model.payload, dict) else None
        backtest = _evaluate_crypto_walk_forward(
            rows,
            settings=self.settings,
            crypto_policy=crypto_policy,
            diagnostic_model=model_payload,
            empirical_bucket_requested_assets=requested_assets,
            force_empirical_bucket_for_requested_assets=bool(requested_assets),
        )
        data_quality = (
            _crypto_feature_row_data_quality(rows, min_training_samples=self.settings.crypto_min_training_samples)
            if feature_decision_rows
            else _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            )
        )
        spot_quality = (
            _crypto_spot_quality_from_feature_rows(
                rows,
                expected_assets=mode_assets,
                min_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
            )
            if feature_decision_rows
            else _crypto_spot_quality(
                spot_rows,
                expected_assets=(
                    requested_assets
                    if requested_assets
                    else _crypto_expected_spot_assets(self.settings, observed_assets={row.asset_symbol for row in snapshots})
                ),
                min_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
                settings=self.settings,
            )
        )
        metrics = {
            **(backtest.get("metrics") or {}),
            "sample_count": len(rows),
            "resolved_sample_count": len(rows),
            "candle_count": data_quality["candle_count"],
            "leakage_row_count": 0,
            "spot_row_count": len(spot_rows),
            "spot_feature_coverage_pct": _spot_feature_coverage(rows),
            "strict_trade_eligible_count": sum(1 for row in rows if row.get("strict_trade_eligible")),
            "proxy_quote_row_count": sum(1 for row in rows if row.get("quote_source") != "snapshot_quotes"),
            "real_quote_row_count": sum(1 for row in rows if row.get("quote_source") == "snapshot_quotes"),
            "metrics_scope": "walk_forward",
        }
        metrics = _crypto_metrics_with_empirical_buckets(
            metrics,
            bucket_matrix=(backtest.get("bucket_matrix") or metrics.get("bucket_matrix") or []),
            settings=self.settings,
            crypto_policy=crypto_policy,
            requested_asset_symbols=requested_assets,
            force_requested_assets=bool(requested_assets),
        )
        gate = self.evaluate_gate(metrics, crypto_policy=crypto_policy)
        issues: list[dict[str, Any]] = []
        if not (self.settings.crypto_trading_enabled or crypto_policy.trading_enabled):
            issues.append({"severity": "info", "code": "crypto_trading_disabled", "message": "Global crypto trading is disabled."})
        if not (self.settings.crypto_autonomy_enabled or crypto_policy.production_autonomy_enabled):
            issues.append({"severity": "info", "code": "crypto_autonomy_disabled", "message": "Crypto autonomy is disabled."})
        for reason in gate["reasons"]:
            severity = "fail" if command == "validate" else "warn"
            issues.append({"severity": severity, "code": _issue_code(reason), "message": reason})
        status = "pass"
        if any(issue["severity"] == "fail" for issue in issues):
            status = "fail"
        elif any(issue["severity"] == "warn" for issue in issues):
            status = "warn"
        return {
            "schema_version": "crypto-backtest-report-v1",
            "status": status,
            "command": command,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "asset_symbols": requested_assets,
            "dataset": {
                "source": dataset_source,
                "row_count": len(rows),
                "feature_row_count": len(feature_decision_rows),
                "snapshot_count": len(snapshots),
                "settled_snapshot_count": sum(1 for row in snapshots if row.settlement_result in {"yes", "no"}),
                "candlestick_count": len(candles),
                "spot_row_count": len(spot_rows),
                "assets": sorted({str(row.get("asset_symbol")) for row in rows}),
            },
            "data_quality": data_quality,
            "spot_quality": spot_quality,
            "model": _artifact_summary(model),
            "runtime_crypto_policy": _runtime_crypto_policy_payload(crypto_policy),
            "walk_forward": backtest,
            "metrics": metrics,
            "promotion_gate": gate,
            "issues": issues,
        }


class CryptoExecutionService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        base_execution_service: ExecutionService,
        asset_control_service: CryptoAssetControlService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.base_execution_service = base_execution_service
        self.asset_control_service = asset_control_service

    @staticmethod
    def passive_yes_price(market: CryptoMarket, side: ContractSide) -> Decimal | None:
        tick = CRYPTO_PASSIVE_PRICE_TICK
        yes_bid = market.yes_bid_dollars
        yes_ask = market.yes_ask_dollars
        if yes_bid is None and yes_ask is None:
            return None
        if side == ContractSide.YES:
            if yes_bid is not None:
                return _clamp_cent_price(yes_bid)
            price = yes_ask - tick if yes_ask is not None else None
            if price is None or price < Decimal("0.01"):
                return None
            return _clamp_cent_price(price)
        if yes_ask is not None:
            return _clamp_cent_price(yes_ask)
        price = yes_bid + tick if yes_bid is not None else None
        if price is None or price > Decimal("0.99"):
            return None
        return _clamp_cent_price(price)

    async def execute(
        self,
        *,
        room: Room,
        control: Any,
        ticket: TradeTicket,
        client_order_id: str,
        fair_yes_dollars: Decimal,
        market: CryptoMarket,
        signal: StrategySignal,
        crypto_policy: RuntimeCryptoPolicy | None = None,
        decision_lineage: dict[str, Any] | None = None,
    ) -> ExecReceiptPayload:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=room.kalshi_env)
            fresh_control = await repo.get_deployment_control(kalshi_env=room.kalshi_env)
            explicit_asset_mode = self.asset_control_service.explicit_mode_for_control(
                fresh_control,
                market.asset_symbol,
                frequency=market.frequency,
            )
            asset_mode = self.asset_control_service.mode_for_control(
                fresh_control,
                market.asset_symbol,
                crypto_policy=crypto_policy,
                frequency=market.frequency,
            )
            gate = await _latest_crypto_artifact_for_asset(
                repo,
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=room.kalshi_env,
                asset_symbol=market.asset_symbol,
            )
            if _crypto_signal_uses_btc_1h_touch20_gate(signal, market, settings=self.settings):
                gate = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=market.frequency,
                    artifact_type="replay_gate_touch20",
                    kalshi_env=room.kalshi_env,
                    asset_symbol=market.asset_symbol,
                    allow_generic_fallback=False,
                )
            await session.commit()
        if asset_mode != CRYPTO_ASSET_MODE_LIVE:
            if self.settings.app_shadow_mode or room.shadow_mode:
                return ExecReceiptPayload(
                    status="shadow_skipped",
                    client_order_id=client_order_id,
                    details={
                        "reason": "crypto asset is shadowed",
                        "asset_symbol": market.asset_symbol,
                        "asset_mode": asset_mode,
                    },
                )
            return ExecReceiptPayload(
                status="crypto_asset_live_disabled",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto asset mode is not live",
                    "asset_symbol": market.asset_symbol,
                    "asset_mode": asset_mode,
                },
            )
        if str(room.kalshi_env or "").strip().lower() != "demo" and explicit_asset_mode != CRYPTO_ASSET_MODE_LIVE:
            return ExecReceiptPayload(
                status="crypto_asset_live_disabled",
                client_order_id=client_order_id,
                details={
                    "reason": "production crypto asset is not explicitly live in deployment control",
                    "asset_symbol": market.asset_symbol,
                    "asset_mode": asset_mode,
                    "control_asset_mode": explicit_asset_mode,
                },
            )
        selection = ((signal.candidate_trace or {}).get("trade_selection_model") or {}) if signal.candidate_trace else {}
        candidate_status = selection.get("candidate_status") or (signal.candidate_trace or {}).get("candidate_status")
        if candidate_status and candidate_status != CRYPTO_LIVE_QUALITY:
            return ExecReceiptPayload(
                status="crypto_candidate_not_live_eligible",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto candidate is shadow exploratory or otherwise not live eligible",
                    "candidate_status": candidate_status,
                },
            )
        trading_enabled = self.settings.crypto_trading_enabled or bool(
            crypto_policy.trading_enabled if crypto_policy is not None else False
        )
        if not trading_enabled:
            return ExecReceiptPayload(
                status="crypto_trading_disabled",
                client_order_id=client_order_id,
                details={"reason": "crypto_trading_enabled is false"},
            )
        replay_gate_blockers = _runtime_replay_gate_blockers(gate, crypto_policy)
        if replay_gate_blockers:
            return ExecReceiptPayload(
                status="crypto_replay_gate_blocked",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto replay gate has not passed",
                    "gate_status": gate.status if gate is not None else "missing",
                    "gate_version": gate.version if gate is not None else None,
                    "gate_artifact_type": gate.artifact_type if gate is not None else None,
                    "gate_runtime_blockers": replay_gate_blockers,
                    "runtime_crypto_policy": _runtime_crypto_policy_payload(
                        crypto_policy,
                        asset_symbol=market.asset_symbol,
                        frequency=market.frequency,
                    )
                    if crypto_policy is not None
                    else None,
                },
            )
        if _crypto_market_closed_for_execution(market):
            return ExecReceiptPayload(
                status="crypto_market_closed",
                client_order_id=client_order_id,
                details={
                    "reason": "market close time has passed",
                    "market_ticker": market.market_ticker,
                    "close_time": _datetime_text(market.close_time or market.expected_expiration_time),
                    "no_order_submitted": True,
                },
            )
        if bool(self.settings.crypto_model_trained_replay_only) and crypto_last_minute_passive_trace(signal.candidate_trace):
            return ExecReceiptPayload(
                status="crypto_candidate_not_live_eligible",
                client_order_id=client_order_id,
                details={
                    "reason": "model_trained_replay_only_blocks_last_minute_passive",
                    "candidate_status": candidate_status,
                    "no_order_submitted": True,
                },
            )
        if crypto_last_minute_passive_trace(signal.candidate_trace):
            fixed_ticket = ticket.model_copy(update={"time_in_force": KALSHI_GTC_TIME_IN_FORCE})
            live_pnl_gate = await self._live_pnl_gate(
                room=room,
                ticket=fixed_ticket,
                market=market,
                liquidity="maker",
                yes_price_dollars=fixed_ticket.yes_price_dollars,
            )
            if _crypto_live_pnl_gate_blocks(live_pnl_gate, settings=self.settings):
                return ExecReceiptPayload(
                    status="crypto_live_pnl_gate_blocked",
                    client_order_id=client_order_id,
                    details={
                        "reason": "crypto_live_pnl_gate_blocked",
                        "crypto_live_pnl_gate": live_pnl_gate,
                        "decision_lineage": decision_lineage or {},
                        "no_order_submitted": True,
                    },
                )
            receipt = await self.base_execution_service.execute_fixed_limit_until_close(
                ticket=fixed_ticket,
                client_order_id=f"{client_order_id}:maker",
                close_time=market.close_time or market.expected_expiration_time,
            )
            receipt.details = {
                **(receipt.details if isinstance(receipt.details, dict) else {}),
                "crypto_order_mode": "last_minute_passive",
                "fixed_limit_until_close": True,
                "crypto_live_pnl_gate": live_pnl_gate,
                "decision_lineage": _crypto_order_decision_lineage(
                    decision_lineage,
                    liquidity="maker",
                    order_mode="last_minute_passive",
                ),
                "last_minute_passive": (
                    ((signal.candidate_trace or {}).get("last_minute_passive") or {})
                    if isinstance(signal.candidate_trace, dict)
                    else {}
                ),
                "no_taker_fallback": True,
            }
            return receipt
        order_mode = str(self.settings.crypto_order_mode or CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER).strip().lower()
        passive_price = self.passive_yes_price(market, ticket.side)
        passive_live_pnl_gate: dict[str, Any] | None = None
        taker_fallback_checked = False
        if order_mode in {CRYPTO_ORDER_MODE_PASSIVE_ONLY, CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER}:
            if passive_price is None:
                if order_mode == CRYPTO_ORDER_MODE_PASSIVE_ONLY:
                    return ExecReceiptPayload(
                        status="passive_unfilled_no_taker",
                        client_order_id=client_order_id,
                        details={
                            "reason": "passive_price_unavailable",
                            "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_ONLY,
                            "no_order_submitted": True,
                        },
                    )
            else:
                passive_live_pnl_gate = await self._live_pnl_gate(
                    room=room,
                    ticket=ticket,
                    market=market,
                    liquidity="maker",
                    yes_price_dollars=passive_price,
                )
                if _crypto_live_pnl_gate_blocks(passive_live_pnl_gate, settings=self.settings):
                    if order_mode == CRYPTO_ORDER_MODE_PASSIVE_ONLY:
                        return ExecReceiptPayload(
                            status="crypto_live_pnl_gate_blocked",
                            client_order_id=client_order_id,
                            details={
                                "reason": "crypto_live_pnl_gate_blocked",
                                "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_ONLY,
                                "crypto_live_pnl_gate": passive_live_pnl_gate,
                                "decision_lineage": _crypto_order_decision_lineage(
                                    decision_lineage,
                                    liquidity="maker",
                                    order_mode=order_mode,
                                ),
                                "no_order_submitted": True,
                            },
                        )
                else:
                    passive_ticket = ticket.model_copy(
                        update={"yes_price_dollars": passive_price, "time_in_force": KALSHI_GTC_TIME_IN_FORCE}
                    )
                    passive_receipt = await self.base_execution_service.execute(
                        room=room,
                        control=fresh_control,
                        ticket=passive_ticket,
                        client_order_id=f"{client_order_id}:maker",
                        fair_yes_dollars=fair_yes_dollars,
                        min_edge_bps=(
                            int(
                                crypto_policy.entry_for_asset(
                                    market.asset_symbol,
                                    frequency=market.frequency,
                                )["min_fee_adjusted_edge_bps"]
                            )
                            if crypto_policy is not None
                            else None
                        ),
                    )
                    passive_receipt.details = {
                        **(passive_receipt.details if isinstance(passive_receipt.details, dict) else {}),
                        "crypto_live_pnl_gate": passive_live_pnl_gate,
                        "decision_lineage": _crypto_order_decision_lineage(
                            decision_lineage,
                            liquidity="maker",
                            order_mode=order_mode,
                        ),
                    }
                    if passive_receipt.status not in {"unfilled_cancelled", "requote_edge_lost"}:
                        passive_receipt.details = {**passive_receipt.details, "crypto_order_mode": order_mode}
                        return passive_receipt
                    if order_mode == CRYPTO_ORDER_MODE_PASSIVE_ONLY:
                        return ExecReceiptPayload(
                            status="passive_unfilled_no_taker",
                            client_order_id=client_order_id,
                            details={
                                "reason": "passive_order_unfilled_or_edge_lost",
                                "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_ONLY,
                                "passive_receipt": passive_receipt.model_dump(mode="json"),
                                "no_taker_fallback": True,
                            },
                        )
                    taker_fallback_checked = True
                    if not self._allow_taker_fallback(market, signal, crypto_policy=crypto_policy):
                        return ExecReceiptPayload(
                            status="passive_unfilled_taker_blocked",
                            client_order_id=client_order_id,
                            details={
                                "reason": "taker_fallback_not_allowed",
                                "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER,
                                "passive_receipt": passive_receipt.model_dump(mode="json"),
                            },
                        )
        if order_mode == CRYPTO_ORDER_MODE_PASSIVE_ONLY:
            return ExecReceiptPayload(
                status="passive_unfilled_no_taker",
                client_order_id=client_order_id,
                details={
                    "reason": "passive_only_no_taker_fallback",
                    "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_ONLY,
                    "no_order_submitted": True,
                },
            )
        if order_mode == CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER and not taker_fallback_checked:
            if not self._allow_taker_fallback(market, signal, crypto_policy=crypto_policy):
                return ExecReceiptPayload(
                    status="passive_unfilled_taker_blocked",
                    client_order_id=client_order_id,
                    details={
                        "reason": "taker_fallback_not_allowed",
                        "crypto_order_mode": CRYPTO_ORDER_MODE_PASSIVE_THEN_TAKER,
                        "passive_price_unavailable": passive_price is None,
                        "no_order_submitted": passive_price is None,
                    },
                )
        taker_live_pnl_gate = await self._live_pnl_gate(
            room=room,
            ticket=ticket,
            market=market,
            liquidity="taker",
            yes_price_dollars=ticket.yes_price_dollars,
        )
        if _crypto_live_pnl_gate_blocks(taker_live_pnl_gate, settings=self.settings):
            return ExecReceiptPayload(
                status="crypto_live_pnl_gate_blocked",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto_live_pnl_gate_blocked",
                    "crypto_order_mode": order_mode,
                    "crypto_live_pnl_gate": taker_live_pnl_gate,
                    "passive_live_pnl_gate": passive_live_pnl_gate,
                    "decision_lineage": _crypto_order_decision_lineage(
                        decision_lineage,
                        liquidity="taker",
                        order_mode=order_mode,
                    ),
                    "no_order_submitted": True,
                },
            )
        receipt = await self.base_execution_service.execute(
            room=room,
            control=fresh_control,
            ticket=ticket,
            client_order_id=f"{client_order_id}:taker",
            fair_yes_dollars=fair_yes_dollars,
            min_edge_bps=(
                int(
                    crypto_policy.entry_for_asset(
                        market.asset_symbol,
                        frequency=market.frequency,
                    )["min_fee_adjusted_edge_bps"]
                )
                if crypto_policy is not None
                else None
            ),
        )
        receipt.details = {
            **(receipt.details if isinstance(receipt.details, dict) else {}),
            "crypto_order_mode": order_mode,
            "crypto_live_pnl_gate": taker_live_pnl_gate,
            "passive_live_pnl_gate": passive_live_pnl_gate,
            "decision_lineage": _crypto_order_decision_lineage(
                decision_lineage,
                liquidity="taker",
                order_mode=order_mode,
            ),
        }
        return receipt

    async def _live_pnl_gate(
        self,
        *,
        room: Room,
        ticket: TradeTicket,
        market: CryptoMarket,
        liquidity: str,
        yes_price_dollars: Decimal,
    ) -> dict[str, Any]:
        contract_price = yes_price_dollars if ticket.side == ContractSide.YES else Decimal("1.0000") - yes_price_dollars
        strategy_code = crypto_strategy_code_for_frequency(market.frequency)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=room.kalshi_env)
            stats = await repo.get_crypto_live_pnl_cell_stats(
                kalshi_env=room.kalshi_env,
                strategy_code=strategy_code,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                side=ticket.side.value,
                contract_price_dollars=contract_price,
                liquidity=liquidity,
                lookback_days=self.settings.crypto_live_pnl_gate_lookback_days,
            )
            await session.commit()
        return _crypto_live_pnl_gate_payload(
            stats,
            settings=self.settings,
            contract_price_dollars=contract_price,
        )

    def _allow_taker_fallback(
        self,
        market: CryptoMarket,
        signal: StrategySignal,
        *,
        crypto_policy: RuntimeCryptoPolicy | None = None,
    ) -> bool:
        if market.close_time is None:
            return False
        seconds_to_close = (market.close_time - datetime.now(UTC)).total_seconds()
        min_edge_bps = (
            int(
                crypto_policy.entry_for_asset(
                    market.asset_symbol,
                    frequency=market.frequency,
                )["min_fee_adjusted_edge_bps"]
            )
            if crypto_policy is not None
            else self.settings.risk_min_edge_bps
        )
        expected_net_edge = _crypto_signal_expected_net_edge_bps(signal)
        if crypto_late_sure_thing_trace(signal.candidate_trace):
            selection = ((signal.candidate_trace or {}).get("trade_selection_model") or {}) if signal.candidate_trace else {}
            candidate_status = selection.get("candidate_status") or (signal.candidate_trace or {}).get("candidate_status")
            return (
                candidate_status == CRYPTO_LIVE_QUALITY
                and seconds_to_close <= self.settings.crypto_late_sure_thing_max_seconds_to_close
            )
        return (
            seconds_to_close <= self.settings.crypto_taker_fallback_close_seconds
            and expected_net_edge is not None
            and expected_net_edge >= min_edge_bps
        )


class CryptoWorkflowService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        market_service: CryptoMarketService,
        forecast_service: CryptoForecastService,
        risk_engine: DeterministicRiskEngine,
        execution_service: CryptoExecutionService,
        asset_control_service: CryptoAssetControlService,
        agent_pack_service: AgentPackService | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.forecast_service = forecast_service
        self.risk_engine = risk_engine
        self.execution_service = execution_service
        self.asset_control_service = asset_control_service
        self.agent_pack_service = agent_pack_service or AgentPackService(settings)

    async def run_room(self, room_id: str, *, reason: str = "manual") -> None:
        market: CryptoMarket | None = None
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is None:
                    raise KeyError(room_id)
                await repo.update_room_stage(room.id, RoomStage.RESEARCHING)
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.SYSTEM,
                        kind=MessageKind.OBSERVATION,
                        stage=RoomStage.RESEARCHING,
                        content=f"Crypto workflow started ({reason}).",
                        payload={"market_domain": "crypto", "reason": reason},
                    ),
                )
                await session.commit()

            market = await self.market_service.get_market(room.market_ticker, persist=True)
            market_observed_at = datetime.now(UTC)
            frequency_label = crypto_frequency_label(market.frequency)
            strategy_code = crypto_strategy_code_for_frequency(market.frequency)
            signal = await self.forecast_service.forecast(market)
            signal_observed_at = datetime.now(UTC)

            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is None:
                    raise KeyError(room_id)
                control = await repo.ensure_deployment_control(self.settings.app_color)
                active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
                crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
                gate = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=market.frequency,
                    artifact_type="replay_gate",
                    kalshi_env=room.kalshi_env,
                    asset_symbol=market.asset_symbol,
                )
                backtest = await _latest_crypto_artifact_for_asset(
                    repo,
                    frequency=market.frequency,
                    artifact_type="backtest",
                    kalshi_env=room.kalshi_env,
                    asset_symbol=market.asset_symbol,
                )
                if _crypto_signal_uses_btc_1h_touch20_gate(signal, market, settings=self.settings):
                    gate = await _latest_crypto_artifact_for_asset(
                        repo,
                        frequency=market.frequency,
                        artifact_type="replay_gate_touch20",
                        kalshi_env=room.kalshi_env,
                        asset_symbol=market.asset_symbol,
                        allow_generic_fallback=False,
                    )
                    backtest = await _latest_crypto_artifact_for_asset(
                        repo,
                        frequency=market.frequency,
                        artifact_type="backtest_touch20",
                        kalshi_env=room.kalshi_env,
                        asset_symbol=market.asset_symbol,
                        allow_generic_fallback=False,
                    )
                live_status = self.asset_control_service.market_live_status(
                    control=control,
                    replay_gate=gate,
                    market=market,
                    has_write_credentials=self.market_service.kalshi.write_credentials is not None,
                    crypto_policy=crypto_policy,
                )
                market_artifact = await repo.save_artifact(
                    room_id=room.id,
                    artifact_type="market_snapshot",
                    source="crypto_workflow",
                    title=f"{market.asset_symbol} {frequency_label} crypto snapshot",
                    payload={
                        "market_domain": "crypto",
                        "frequency": market.frequency,
                        "strategy_code": strategy_code,
                        "asset_mode": live_status["asset_mode"],
                        "control_asset_mode": live_status["control_asset_mode"],
                        "live_eligible": live_status["live_eligible"],
                        "live_blockers": live_status["live_blockers"],
                        "global_live_blockers": live_status["global_live_blockers"],
                        "market": market.to_payload(),
                    },
                )
                await repo.update_room_stage(room.id, RoomStage.PROPOSING)
                signal_record = await repo.save_signal(
                    room_id=room.id,
                    market_ticker=market.market_ticker,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    edge_bps=signal.edge_bps,
                    confidence=signal.confidence,
                    summary=signal.summary,
                    payload={
                        "market_domain": "crypto",
                        "frequency": market.frequency,
                        "strategy_code": strategy_code,
                        "recommended_action": signal.recommended_action.value if signal.recommended_action else None,
                        "recommended_side": signal.recommended_side.value if signal.recommended_side else None,
                        "target_yes_price_dollars": _money_text(signal.target_yes_price_dollars),
                        "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason else None,
                        "evaluation_outcome": signal.evaluation_outcome,
                        "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility else None,
                        "candidate_trace": signal.candidate_trace,
                        "market_artifact_id": market_artifact.id,
                        "crypto_modeling": {
                            "model_version": (signal.candidate_trace or {}).get("model_version"),
                            "backtest_version": backtest.version if backtest is not None else None,
                            "replay_gate_status": gate.status if gate is not None else "missing",
                            "replay_gate_artifact_type": gate.artifact_type if gate is not None else None,
                            "data_quality_status": (
                                ((backtest.payload or {}).get("data_quality") or {}).get("status")
                                if backtest is not None
                                else None
                            ),
                            "prediction_model": (signal.candidate_trace or {}).get("prediction_model"),
                            "trade_selection_model": (signal.candidate_trace or {}).get("trade_selection_model"),
                            "runtime_crypto_policy": _runtime_crypto_policy_payload(
                                crypto_policy,
                                asset_symbol=market.asset_symbol,
                                frequency=market.frequency,
                            ),
                        },
                    },
                )
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.TRADER,
                        kind=MessageKind.TRADE_IDEA,
                        stage=RoomStage.PROPOSING,
                        content=signal.summary,
                        payload={"signal_id": signal_record.id, **(signal_record.payload or {})},
                    ),
                )
                await _upsert_crypto_decision_outcome_for_signal(
                    repo,
                    room=room,
                    market=market,
                    signal=signal,
                    signal_record=signal_record,
                    decision_kind="selected" if _signal_is_tradeable(signal) else "stand_down",
                    gate_status=(
                        "eligible"
                        if _signal_is_tradeable(signal)
                        else str(
                            ((signal.candidate_trace or {}).get("trade_selection_model") or {}).get("candidate_status")
                            or (signal.candidate_trace or {}).get("candidate_status")
                            or "blocked"
                        )
                    ),
                    selected_count_fp=None,
                )
                if _crypto_market_closed_for_execution(market):
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await repo.append_message(
                        room.id,
                        RoomMessageCreate(
                            role=AgentRole.SYSTEM,
                            kind=MessageKind.OPS_ALERT,
                            stage=RoomStage.COMPLETE,
                            content="Crypto market is already closed; no trade ticket created.",
                            payload={
                                "market_domain": "crypto",
                                "market_ticker": market.market_ticker,
                                "reason": "crypto_market_closed",
                                "close_time": _datetime_text(market.close_time or market.expected_expiration_time),
                                "no_order_submitted": True,
                            },
                        ),
                    )
                    await session.commit()
                    return
                if not _signal_is_tradeable(signal):
                    stand_down_payload = _crypto_signal_stand_down_payload(signal)
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await repo.append_message(
                        room.id,
                        RoomMessageCreate(
                            role=AgentRole.SYSTEM,
                            kind=MessageKind.OBSERVATION,
                            stage=RoomStage.COMPLETE,
                            content=_crypto_signal_stand_down_content(stand_down_payload),
                            payload={
                                "market_domain": "crypto",
                                "frequency": market.frequency,
                                "strategy_code": strategy_code,
                                "market_ticker": market.market_ticker,
                                "no_order_submitted": True,
                                **stand_down_payload,
                            },
                        ),
                    )
                    await session.commit()
                    return

                default_count_fp = quantize_count(Decimal(str(self.settings.crypto_default_order_count_fp)))
                time_in_force = (
                    KALSHI_GTC_TIME_IN_FORCE
                    if crypto_last_minute_passive_trace(signal.candidate_trace)
                    else "immediate_or_cancel"
                )
                base_ticket = TradeTicket(
                    market_ticker=market.market_ticker,
                    action=TradeAction.BUY,
                    side=signal.recommended_side,
                    yes_price_dollars=signal.target_yes_price_dollars,
                    count_fp=default_count_fp,
                    capital_bucket=signal.capital_bucket,
                    time_in_force=time_in_force,
                    note=(
                        f"{strategy_code} last-minute passive rest-to-close candidate"
                        if time_in_force == KALSHI_GTC_TIME_IN_FORCE
                        else f"{strategy_code} passive-first candidate"
                    ),
                )
                risk_context = await self._risk_context(
                    repo,
                    room,
                    base_ticket,
                    market,
                    market_observed_at=market_observed_at,
                    research_observed_at=signal_observed_at,
                )
                count_fp, sizing_diagnostics = _crypto_dynamic_order_count_fp(
                    settings=self.settings,
                    ticket=base_ticket,
                    signal=signal,
                    context=risk_context,
                )
                fee_to_edge_review = (
                    sizing_diagnostics.get("fee_to_edge") if isinstance(sizing_diagnostics, dict) else None
                )
                if isinstance(fee_to_edge_review, dict) and fee_to_edge_review.get("status") == "blocked_fee_ratio":
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await repo.append_message(
                        room.id,
                        RoomMessageCreate(
                            role=AgentRole.SYSTEM,
                            kind=MessageKind.OBSERVATION,
                            stage=RoomStage.COMPLETE,
                            content=(
                                "Rounded-up Kalshi fee consumes too much of the expected edge at every "
                                "permitted order size; no trade ticket created."
                            ),
                            payload={
                                "market_domain": "crypto",
                                "frequency": market.frequency,
                                "strategy_code": strategy_code,
                                "market_ticker": market.market_ticker,
                                "reason": "blocked_fee_ratio",
                                "skip_reason": "blocked_fee_ratio",
                                "fee_to_edge": fee_to_edge_review,
                                "crypto_dynamic_sizing": sizing_diagnostics,
                                "no_order_submitted": True,
                            },
                        ),
                    )
                    await session.commit()
                    return
                ticket = base_ticket.model_copy(update={"count_fp": count_fp})
                client_order_id = make_client_order_id(room.id, market.market_ticker, ticket.nonce)
                decision_lineage = _crypto_decision_lineage_payload(
                    room=room,
                    market=market,
                    signal=signal,
                    signal_record=signal_record,
                    strategy_code=strategy_code,
                    gate=gate,
                    backtest=backtest,
                    live_status=live_status,
                    proposed_count_fp=count_fp,
                    sizing_diagnostics=sizing_diagnostics,
                )
                ticket_record = await repo.save_trade_ticket(
                    room.id,
                    ticket,
                    client_order_id,
                    strategy_code=strategy_code,
                )
                ticket_record.payload = {
                    **(ticket_record.payload or {}),
                    "market_domain": "crypto",
                    "frequency": market.frequency,
                    "asset_symbol": market.asset_symbol,
                    "asset_mode": live_status["asset_mode"],
                    "control_asset_mode": live_status["control_asset_mode"],
                    "live_eligible": live_status["live_eligible"],
                    "crypto_modeling": (signal_record.payload or {}).get("crypto_modeling"),
                    "prediction_model": ((signal_record.payload or {}).get("crypto_modeling") or {}).get("prediction_model"),
                    "trade_selection_model": ((signal_record.payload or {}).get("crypto_modeling") or {}).get("trade_selection_model"),
                    "crypto_dynamic_sizing": sizing_diagnostics,
                    "decision_lineage": {
                        **decision_lineage,
                        "trade_ticket_id": ticket_record.id,
                        "client_order_id": client_order_id,
                    },
                }
                decision_lineage = dict(ticket_record.payload["decision_lineage"])
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.TRADER,
                        kind=MessageKind.TRADE_TICKET,
                        stage=RoomStage.PROPOSING,
                        content=f"Proposed crypto {ticket.side.value.upper()} ticket for {ticket.count_fp} contracts.",
                        payload={**ticket_record.payload, "strategy_code": strategy_code},
                    ),
                )
                await repo.update_room_stage(room.id, RoomStage.RISK)
                runtime_thresholds = self.agent_pack_service.runtime_crypto_thresholds(
                    crypto_policy,
                    asset_symbol=market.asset_symbol,
                    frequency=market.frequency,
                )
                if str((signal.candidate_trace or {}).get("objective") or "").startswith("touch_"):
                    runtime_thresholds.risk_min_contract_price_dollars = min(
                        float(runtime_thresholds.risk_min_contract_price_dollars),
                        float(self.settings.crypto_touch_strategy_min_contract_price_dollars),
                    )
                verdict = self.risk_engine.evaluate(
                    room=room,
                    control=control,
                    ticket=ticket,
                    signal=signal,
                    context=risk_context,
                    thresholds=runtime_thresholds,
                )
                await repo.save_risk_verdict(
                    room_id=room.id,
                    ticket_id=ticket_record.id,
                    status=verdict.status,
                    reasons=verdict.reasons,
                    approved_notional_dollars=verdict.approved_notional_dollars,
                    approved_count_fp=verdict.approved_count_fp,
                    payload=verdict.model_dump(mode="json"),
                )
                decision_lineage = {
                    **decision_lineage,
                    "risk_status": verdict.status.value,
                    "risk_reason_codes": verdict.reason_codes,
                    "risk_reasons": verdict.reasons,
                    "approved_count_fp": _count_text(verdict.approved_count_fp),
                    "approved_notional_dollars": _money_text(verdict.approved_notional_dollars),
                    "net_edge_bps": verdict.net_edge_bps,
                    "fee_edge_bps": verdict.fee_edge_bps,
                }
                ticket_record.payload = {
                    **(ticket_record.payload or {}),
                    "decision_lineage": decision_lineage,
                }
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.RISK_OFFICER,
                        kind=MessageKind.RISK_VERDICT,
                        stage=RoomStage.RISK,
                        content=f"Crypto risk verdict: {verdict.status.value}.",
                        payload=verdict.model_dump(mode="json"),
                    ),
                )
                if verdict.status != RiskStatus.APPROVED:
                    await repo.update_trade_ticket_status(ticket_record.id, "blocked")
                    if self.settings.app_shadow_mode or room.shadow_mode or live_status["asset_mode"] == CRYPTO_ASSET_MODE_SHADOW:
                        receipt = ExecReceiptPayload(
                            status="shadow_skipped",
                            client_order_id=client_order_id,
                            details={
                                "reason": "risk_blocked_before_execution",
                                "asset_symbol": market.asset_symbol,
                                "asset_mode": live_status["asset_mode"],
                                "live_eligible": live_status["live_eligible"],
                                "risk_status": verdict.status.value,
                                "risk_reasons": verdict.reasons,
                                "no_order_submitted": True,
                            },
                        )
                        await repo.append_message(
                            room.id,
                            RoomMessageCreate(
                                role=AgentRole.EXECUTION_CLERK,
                                kind=MessageKind.EXEC_RECEIPT,
                                stage=RoomStage.EXECUTING,
                                content=f"Crypto execution status: {receipt.status}.",
                                payload=receipt.model_dump(mode="json"),
                            ),
                        )
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await session.commit()
                    return

                approved_ticket = approved_ticket_for_verdict(ticket, verdict)
                await repo.update_trade_ticket_status(ticket_record.id, "approved")
                await repo.update_room_stage(room.id, RoomStage.EXECUTING)
                await session.commit()
                receipt = await self.execution_service.execute(
                    room=room,
                    control=control,
                    ticket=approved_ticket,
                    client_order_id=client_order_id,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    market=market,
                    signal=signal,
                    crypto_policy=crypto_policy,
                    decision_lineage=decision_lineage,
                )
                no_order_statuses = {
                    "shadow_skipped",
                    "inactive_color_skipped",
                    "crypto_asset_live_disabled",
                    "crypto_trading_disabled",
                    "crypto_replay_gate_blocked",
                    "crypto_live_pnl_gate_blocked",
                    "crypto_candidate_not_live_eligible",
                    "crypto_market_closed",
                }
                if receipt.external_order_id or receipt.status not in no_order_statuses:
                    execution_client_order_id = _receipt_kalshi_client_order_id(
                        receipt.details if isinstance(receipt.details, dict) else {},
                        client_order_id,
                    )
                    await repo.save_order(
                        ticket_id=ticket_record.id,
                        client_order_id=execution_client_order_id,
                        market_ticker=approved_ticket.market_ticker,
                        status=receipt.status,
                        side=approved_ticket.side.value,
                        action=approved_ticket.action.value,
                        yes_price_dollars=approved_ticket.yes_price_dollars,
                        count_fp=approved_ticket.count_fp,
                        raw=_crypto_raw_with_decision_lineage(
                            receipt.details if isinstance(receipt.details, dict) else {},
                            decision_lineage,
                        ),
                        kalshi_order_id=receipt.external_order_id,
                        kalshi_env=room.kalshi_env,
                        strategy_code=strategy_code,
                    )
                await repo.update_trade_ticket_status(ticket_record.id, receipt.status)
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.EXECUTION_CLERK,
                        kind=MessageKind.EXEC_RECEIPT,
                        stage=RoomStage.EXECUTING,
                        content=f"Crypto execution status: {receipt.status}.",
                        payload=receipt.model_dump(mode="json"),
                    ),
                )
                await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                await session.commit()
        except Exception:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is not None:
                    await repo.update_room_stage(room.id, RoomStage.FAILED)
                    await repo.append_message(
                        room.id,
                        RoomMessageCreate(
                            role=AgentRole.SYSTEM,
                            kind=MessageKind.OPS_ALERT,
                            stage=RoomStage.FAILED,
                            content="Crypto workflow failed; see logs.",
                            payload={"market_domain": "crypto", "market_ticker": market.market_ticker if market else None},
                        ),
                    )
                    await session.commit()
            raise

    async def _risk_context(
        self,
        repo: PlatformRepository,
        room: Room,
        ticket: TradeTicket,
        market: CryptoMarket,
        *,
        market_observed_at: datetime | None = None,
        research_observed_at: datetime | None = None,
    ) -> RiskContext:
        strategy_code = crypto_strategy_code_for_frequency(market.frequency)
        positions = await repo.list_positions_for_ticker(
            room.market_ticker,
            kalshi_env=room.kalshi_env,
            subaccount=self.settings.kalshi_subaccount,
        )
        open_position = max(positions, key=lambda p: p.count_fp) if positions else None
        all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
        pending_order_count_fp = await repo.get_pending_buy_count_fp(
            room.market_ticker,
            ticket.side.value,
            kalshi_env=room.kalshi_env,
            subaccount=self.settings.kalshi_subaccount,
        )
        pending_order_notional = estimate_notional_dollars(
            ticket.side,
            ticket.yes_price_dollars,
            pending_order_count_fp,
        )
        total_capital = await repo.get_total_capital_dollars(kalshi_env=room.kalshi_env)
        portfolio_position_notional = await repo.get_crypto_portfolio_position_notional_dollars(
            kalshi_env=room.kalshi_env,
            subaccount=self.settings.kalshi_subaccount,
        )
        portfolio_pending_notional = await repo.get_crypto_portfolio_pending_buy_notional_dollars(
            kalshi_env=room.kalshi_env,
            subaccount=self.settings.kalshi_subaccount,
        )
        portfolio_bucket_snapshot = None
        if total_capital is not None and total_capital > Decimal("0"):
            portfolio_bucket_snapshot = await repo.portfolio_bucket_snapshot(
                kalshi_env=room.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
                total_capital_dollars=total_capital,
                safe_capital_reserve_ratio=self.settings.risk_safe_capital_reserve_ratio,
                risky_capital_max_ratio=self.settings.risk_risky_capital_max_ratio,
            )
        strategy_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code=strategy_code,
            kalshi_env=room.kalshi_env,
        )
        strategy_asset_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy_asset(
            strategy_code=strategy_code,
            asset_symbol=market.asset_symbol,
            kalshi_env=room.kalshi_env,
        )
        current_position_notional = (
            abs(Decimal(str(open_position.count_fp))) * Decimal(str(open_position.average_price_dollars))
            if open_position is not None
            else Decimal("0")
        )
        return RiskContext(
            market_observed_at=market_observed_at,
            research_observed_at=research_observed_at,
            total_capital_dollars=total_capital,
            current_position_notional_dollars=current_position_notional,
            current_position_count_fp=open_position.count_fp if open_position is not None else Decimal("0"),
            current_position_side=open_position.side if open_position is not None else None,
            pending_order_count_fp=pending_order_count_fp,
            pending_order_notional_dollars=pending_order_notional,
            portfolio_position_notional_dollars=portfolio_position_notional,
            portfolio_pending_order_notional_dollars=portfolio_pending_notional,
            portfolio_bucket_snapshot=portfolio_bucket_snapshot,
            open_ticker_count=len({position.market_ticker for position in all_positions}),
            strategy_code=strategy_code,
            strategy_daily_realized_pnl_dollars=strategy_daily_pnl,
            strategy_asset_daily_realized_pnl_dollars=strategy_asset_daily_pnl,
            signal_asset_symbol=market.asset_symbol,
        )


class CryptoAutonomyService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        market_service: CryptoMarketService,
        asset_control_service: CryptoAssetControlService,
        workflow_service: CryptoWorkflowService,
        agent_pack_service: AgentPackService | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.asset_control_service = asset_control_service
        self.workflow_service = workflow_service
        self.agent_pack_service = agent_pack_service or AgentPackService(settings)

    async def run_once(
        self,
        *,
        frequency: str = "15m",
        force: bool = False,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        requested_assets = set(normalize_asset_symbols(asset_symbols))
        production_mode = str(self.settings.kalshi_env or "").strip().lower() != "demo"
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
                active_pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
                crypto_policy = self.agent_pack_service.runtime_crypto_policy(active_pack)
                gate = await repo.get_latest_crypto_model_artifact(
                    frequency=freq,
                    artifact_type="replay_gate",
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
        except Exception:
            if production_mode and not self.settings.crypto_production_autonomy_enabled:
                return {
                    "status": "production_blocked",
                    "kalshi_env": self.settings.kalshi_env,
                    "frequency": freq,
                    "reason": "crypto production autonomy requires CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true or promoted runtime policy",
                }
            raise
        runtime_autonomy_enabled = bool(crypto_policy.production_autonomy_enabled)
        production_autonomy_enabled = self.settings.crypto_production_autonomy_enabled or crypto_policy.production_autonomy_enabled
        shadow_evidence_mode = _resolve_crypto_shadow_evidence_mode(
            production_mode=production_mode,
            quote_evidence_enabled=self.settings.crypto_quote_evidence_enabled,
            production_autonomy_enabled=production_autonomy_enabled,
            shadow_evidence_always=self.settings.crypto_shadow_evidence_always_enabled,
        )
        if not self.settings.crypto_autonomy_enabled and not runtime_autonomy_enabled and not force and not shadow_evidence_mode:
            return {
                "status": "disabled",
                "kalshi_env": self.settings.kalshi_env,
                "frequency": freq,
                "reason": "crypto_autonomy_enabled is false and active runtime crypto policy has production_autonomy_enabled=false",
            }
        if production_mode and not production_autonomy_enabled and not shadow_evidence_mode:
            return {
                "status": "production_blocked",
                "kalshi_env": self.settings.kalshi_env,
                "frequency": freq,
                "reason": "crypto production autonomy requires CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true or promoted runtime policy",
            }
        if control.active_color != self.settings.app_color:
            return {
                "status": "inactive_color",
                "kalshi_env": self.settings.kalshi_env,
                "frequency": freq,
                "active_color": control.active_color,
                "app_color": self.settings.app_color,
            }

        discovered = await self.market_service.discover_markets(
            frequency=freq,
            status="open",
            persist=True,
            asset_symbols=sorted(requested_assets) or None,
        )
        if requested_assets:
            discovered = [market for market in discovered if normalize_asset_symbol(market.asset_symbol) in requested_assets]
        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        min_seconds = crypto_autonomy_min_seconds_to_close_for_frequency(self.settings, freq)
        markets, ineligible = _eligible_market_per_asset(
            discovered,
            min_seconds_to_close=min_seconds,
            min_market_age_seconds=max(0, int(self.settings.crypto_live_min_market_age_seconds)),
        )
        max_rooms = max(0, int(self.settings.crypto_autonomy_max_rooms_per_run))
        max_per_asset = max(1, int(self.settings.crypto_autonomy_max_per_asset_per_run))
        markets, cap_skips = _cap_crypto_autonomy_markets(markets, max_rooms=max_rooms, max_per_asset=max_per_asset)
        reevaluated: list[dict[str, Any]] = []
        skipped.extend(ineligible)
        skipped.extend(cap_skips)

        replay_gates_by_asset: dict[str, Any] = {}
        touch_replay_gates_by_asset: dict[str, Any] = {}
        if markets:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                for asset_symbol in sorted({market.asset_symbol for market in markets}):
                    asset_key = normalize_asset_symbol(asset_symbol)
                    replay_gates_by_asset[asset_key] = await _latest_crypto_artifact_for_asset(
                        repo,
                        frequency=freq,
                        artifact_type="replay_gate",
                        kalshi_env=self.settings.kalshi_env,
                        asset_symbol=asset_symbol,
                    )
                    if freq == "1h" and asset_key in (_normalize_asset_csv(self.settings.crypto_1h_touch_assets) or {"BTC"}):
                        touch_replay_gates_by_asset[asset_key] = await _latest_crypto_artifact_for_asset(
                            repo,
                            frequency=freq,
                            artifact_type="replay_gate_touch20",
                            kalshi_env=self.settings.kalshi_env,
                            asset_symbol=asset_symbol,
                            allow_generic_fallback=False,
                        )
                await session.commit()

        for market in markets:
            try:
                seconds_to_close = int((market.close_time - datetime.now(UTC)).total_seconds())
                asset_key = normalize_asset_symbol(market.asset_symbol)
                touch_gate = touch_replay_gates_by_asset.get(asset_key)
                market_gate = replay_gates_by_asset.get(asset_key, gate)
                if (
                    _crypto_btc_1h_touch_policy_configured_for_market(market, settings=self.settings)
                    and _crypto_touch_replay_gate_passed(touch_gate)
                ):
                    market_gate = touch_gate

                live_status = self.asset_control_service.market_live_status(
                    control=control,
                    replay_gate=market_gate,
                    market=market,
                    has_write_credentials=self.market_service.kalshi.write_credentials is not None,
                    crypto_policy=crypto_policy,
                )
                if live_status["asset_mode"] == CRYPTO_ASSET_MODE_OFF:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "asset_mode_off",
                        }
                    )
                    continue
                shadow_evidence_allowed = (
                    shadow_evidence_mode
                    and live_status["asset_mode"] == CRYPTO_ASSET_MODE_SHADOW
                )
                if production_mode and not live_status["live_eligible"] and not shadow_evidence_allowed:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "not_live_eligible",
                            "asset_mode": live_status["asset_mode"],
                            "live_blockers": live_status["live_blockers"],
                        }
                    )
                    continue

                if self.settings.crypto_book_noise_gate_enabled:
                    async with self.session_factory() as session:
                        repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                        lookback = datetime.now(UTC) - timedelta(seconds=self.settings.crypto_book_noise_lookback_seconds)
                        recent_snaps = await repo.list_crypto_market_snapshots(
                            market_ticker=market.market_ticker,
                            since=lookback,
                            kalshi_env=self.settings.kalshi_env,
                            limit=10,
                        )
                        await session.commit()
                    noise_reason = _crypto_book_noise_reason(recent_snaps, settings=self.settings)
                    if noise_reason:
                        skipped.append(
                            {
                                "market_ticker": market.market_ticker,
                                "asset_symbol": market.asset_symbol,
                                "reason": noise_reason,
                            }
                        )
                        continue

                async with self.session_factory() as session:
                    repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                    existing = await repo.get_latest_room_for_market(
                        market.market_ticker,
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()
                if existing is not None:
                    if live_status["asset_mode"] == CRYPTO_ASSET_MODE_LIVE and live_status["live_eligible"]:
                        await self.workflow_service.run_room(existing.id, reason="crypto_autonomy_reevaluate")
                        reevaluated.append(
                            {
                                "room_id": existing.id,
                                "market_ticker": market.market_ticker,
                                "asset_symbol": market.asset_symbol,
                                "seconds_to_close": seconds_to_close,
                                "requested_asset_mode": live_status["asset_mode"],
                            }
                        )
                        continue
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "room_already_exists",
                            "room_id": existing.id,
                        }
                    )
                    continue

                result = await self.market_service.create_room_for_market(
                    market.market_ticker,
                    reason="crypto_autonomy",
                )
                await self.workflow_service.run_room(result["room_id"], reason="crypto_autonomy")
                created.append(
                    {
                        **result,
                        "seconds_to_close": seconds_to_close,
                        "requested_asset_mode": live_status["asset_mode"],
                    }
                )
            except Exception as exc:
                logger.warning("crypto autonomy failed for %s", market.market_ticker, exc_info=True)
                errors.append(
                    {
                        "market_ticker": market.market_ticker,
                        "asset_symbol": market.asset_symbol,
                        "error": str(exc),
                    }
                )

        result = {
            "status": "ok",
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "forced": force,
            "asset_symbols": sorted(requested_assets),
            "shadow_evidence_mode": shadow_evidence_mode,
            "checked_markets": len(discovered),
            "eligible_markets": len(markets),
            "reevaluated": reevaluated,
            "caps": {
                "max_rooms_per_run": max_rooms,
                "max_per_asset_per_run": max_per_asset,
            },
            "created": created,
            "skipped": skipped,
            "errors": errors,
        }
        await self._log_cycle(
            result,
            discovered=discovered,
            selected_markets=markets,
            min_seconds_to_close=min_seconds,
            min_market_age_seconds=max(0, int(self.settings.crypto_live_min_market_age_seconds)),
        )
        return result

    async def _log_cycle(
        self,
        result: dict[str, Any],
        *,
        discovered: list[CryptoMarket],
        selected_markets: list[CryptoMarket],
        min_seconds_to_close: int,
        min_market_age_seconds: int,
    ) -> None:
        payload = _crypto_autonomy_cycle_ops_payload(
            result,
            discovered=discovered,
            selected_markets=selected_markets,
            min_seconds_to_close=min_seconds_to_close,
            min_market_age_seconds=min_market_age_seconds,
        )
        severity = "warning" if payload["error_count"] else "info"
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.log_ops_event(
                    severity=severity,
                    source="crypto_autonomy",
                    summary=_crypto_autonomy_cycle_ops_summary(payload),
                    payload=payload,
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
        except Exception:
            logger.warning("failed to log crypto autonomy cycle telemetry", exc_info=True)


def _crypto_autonomy_cycle_ops_payload(
    result: dict[str, Any],
    *,
    discovered: list[CryptoMarket],
    selected_markets: list[CryptoMarket],
    min_seconds_to_close: int,
    min_market_age_seconds: int,
) -> dict[str, Any]:
    assets: dict[str, dict[str, Any]] = {}

    def asset_entry(asset_symbol: str | None) -> dict[str, Any]:
        symbol = normalize_asset_symbol(asset_symbol or "UNKNOWN")
        if symbol not in assets:
            assets[symbol] = {
                "discovered_market_count": 0,
                "selected_market_tickers": [],
                "created": [],
                "reevaluated": [],
                "skipped": [],
                "errors": [],
                "skip_reason_counts": {},
                "error_count": 0,
            }
        return assets[symbol]

    for symbol in result.get("asset_symbols") or []:
        asset_entry(str(symbol))
    for market in discovered:
        entry = asset_entry(market.asset_symbol)
        entry["discovered_market_count"] += 1
    for market in selected_markets:
        entry = asset_entry(market.asset_symbol)
        _append_limited(entry["selected_market_tickers"], market.market_ticker)

    for item in result.get("created") or []:
        if not isinstance(item, dict):
            continue
        entry = asset_entry(item.get("asset_symbol"))
        _append_limited(entry["created"], _crypto_autonomy_compact_cycle_item(item))

    for item in result.get("reevaluated") or []:
        if not isinstance(item, dict):
            continue
        entry = asset_entry(item.get("asset_symbol"))
        _append_limited(entry["reevaluated"], _crypto_autonomy_compact_cycle_item(item))

    reason_counts: Counter[str] = Counter()
    live_blocker_counts: Counter[str] = Counter()
    for item in result.get("skipped") or []:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason") or "unknown")
        reason_counts[reason] += 1
        entry = asset_entry(item.get("asset_symbol"))
        entry["skip_reason_counts"][reason] = int(entry["skip_reason_counts"].get(reason, 0)) + 1
        _append_limited(entry["skipped"], _crypto_autonomy_compact_cycle_item(item))
        for blocker in item.get("live_blockers") or []:
            live_blocker_counts[str(blocker)] += 1

    for item in result.get("errors") or []:
        if not isinstance(item, dict):
            continue
        entry = asset_entry(item.get("asset_symbol"))
        entry["error_count"] += 1
        _append_limited(entry["errors"], _crypto_autonomy_compact_cycle_item(item))

    return {
        "schema_version": CRYPTO_AUTONOMY_CYCLE_OPS_SCHEMA_VERSION,
        "kalshi_env": result.get("kalshi_env"),
        "frequency": result.get("frequency"),
        "forced": bool(result.get("forced")),
        "shadow_evidence_mode": bool(result.get("shadow_evidence_mode")),
        "requested_assets": list(result.get("asset_symbols") or []),
        "checked_markets": int(result.get("checked_markets") or 0),
        "eligible_markets": int(result.get("eligible_markets") or 0),
        "created_count": len(result.get("created") or []),
        "reevaluated_count": len(result.get("reevaluated") or []),
        "skipped_count": len(result.get("skipped") or []),
        "error_count": len(result.get("errors") or []),
        "skip_reason_counts": dict(sorted(reason_counts.items())),
        "live_blocker_counts": dict(sorted(live_blocker_counts.items())),
        "caps": _crypto_json_safe(result.get("caps") or {}),
        "min_seconds_to_close": int(min_seconds_to_close),
        "min_market_age_seconds": int(min_market_age_seconds),
        "assets": dict(sorted(assets.items())),
    }


def _crypto_autonomy_cycle_ops_summary(payload: dict[str, Any]) -> str:
    reason_counts = payload.get("skip_reason_counts") or {}
    top_reason = ""
    if reason_counts:
        reason, count = max(reason_counts.items(), key=lambda item: int(item[1]))
        top_reason = f" top_skip={reason}:{count}"
    return (
        f"Crypto autonomy cycle {payload.get('frequency')}: "
        f"created={payload.get('created_count', 0)} "
        f"reevaluated={payload.get('reevaluated_count', 0)} "
        f"skipped={payload.get('skipped_count', 0)} "
        f"errors={payload.get('error_count', 0)}"
        f"{top_reason}"
    )


def _crypto_autonomy_compact_cycle_item(item: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "market_ticker",
        "asset_symbol",
        "reason",
        "room_id",
        "seconds_to_close",
        "market_age_seconds",
        "min_market_age_seconds",
        "asset_mode",
        "requested_asset_mode",
        "live_blockers",
        "error",
    )
    return {
        key: _crypto_json_safe(item[key])
        for key in keys
        if key in item and item[key] is not None
    }


def _append_limited(items: list[Any], item: Any, *, limit: int = 10) -> None:
    if len(items) < limit:
        items.append(item)


def _crypto_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _crypto_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_crypto_json_safe(item) for item in value]
    return str(value)


def _market_from_snapshot(row: CryptoMarketSnapshotRecord) -> CryptoMarket:
    payload = row.payload or {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else payload
    return CryptoMarket(
        market_ticker=row.market_ticker,
        series_ticker=row.series_ticker,
        event_ticker=row.event_ticker,
        asset_symbol=row.asset_symbol,
        frequency=row.frequency,
        title=row.title,
        status=row.status,
        open_time=row.open_time,
        close_time=row.close_time,
        expected_expiration_time=row.expected_expiration_time,
        target_price_dollars=row.target_price_dollars,
        yes_bid_dollars=row.yes_bid_dollars,
        yes_ask_dollars=row.yes_ask_dollars,
        no_bid_dollars=row.no_bid_dollars,
        no_ask_dollars=row.no_ask_dollars,
        last_price_dollars=row.last_price_dollars,
        volume=row.volume,
        open_interest=row.open_interest,
        settlement_result=row.settlement_result,
        raw=raw,
    )


def _artifact_summary(artifact: Any | None) -> dict[str, Any]:
    if artifact is None:
        return {"status": "missing", "version": None, "metrics": {}, "payload": {}}
    return {
        "status": artifact.status,
        "version": artifact.version,
        "sample_count": artifact.sample_count,
        "metrics": artifact.metrics,
        "payload": artifact.payload,
        "updated_at": artifact.updated_at.isoformat() if artifact.updated_at else None,
    }


def _crypto_signal_fair_yes_from_payload(signal_payload: dict[str, Any] | None) -> Decimal | None:
    if not isinstance(signal_payload, dict):
        return None
    trace = signal_payload.get("candidate_trace") if isinstance(signal_payload.get("candidate_trace"), dict) else {}
    crypto_modeling = signal_payload.get("crypto_modeling") if isinstance(signal_payload.get("crypto_modeling"), dict) else {}
    prediction_model = trace.get("prediction_model") if isinstance(trace.get("prediction_model"), dict) else {}
    if not prediction_model and isinstance(crypto_modeling, dict):
        prediction_model = (
            crypto_modeling.get("prediction_model")
            if isinstance(crypto_modeling.get("prediction_model"), dict)
            else {}
        )
    anchor = trace.get("market_price_anchor") if isinstance(trace.get("market_price_anchor"), dict) else {}
    for value in (
        signal_payload.get("raw_fair_yes_dollars"),
        trace.get("raw_fair_yes_dollars"),
        anchor.get("raw_fair_yes_dollars") if isinstance(anchor, dict) else None,
        anchor.get("input_fair_yes_dollars") if isinstance(anchor, dict) else None,
        prediction_model.get("calibrated_probability") if isinstance(prediction_model, dict) else None,
        signal_payload.get("fair_yes_dollars"),
        trace.get("fair_yes_dollars"),
    ):
        if value in (None, ""):
            continue
        try:
            return _clamp_price(_decimal(value))
        except Exception:
            continue
    return None


def _crypto_signal_payload_with_current_quote_metrics(
    signal_payload: dict[str, Any] | None,
    *,
    market: CryptoMarket,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any] | None:
    if not isinstance(signal_payload, dict):
        return signal_payload
    fair_yes = _crypto_signal_fair_yes_from_payload(signal_payload)
    if fair_yes is None:
        return signal_payload
    try:
        row = _crypto_live_market_row(market, settings=settings)
        action, side, target_yes, edge_bps, trace = _crypto_recommendation(
            market=market,
            fair_yes=fair_yes,
            settings=settings,
            crypto_policy=crypto_policy,
            row=row,
            require_spot_features=False,
        )
    except Exception:
        logger.debug("failed to refresh crypto signal quote metrics", exc_info=True)
        return signal_payload

    refreshed = dict(signal_payload)
    existing_trace = (
        dict(refreshed.get("candidate_trace"))
        if isinstance(refreshed.get("candidate_trace"), dict)
        else {}
    )
    if (
        isinstance(existing_trace.get("empirical_bucket_gate"), dict)
        and isinstance(trace.get("empirical_bucket_gate"), dict)
        and trace["empirical_bucket_gate"].get("enforced") is not True
    ):
        trace["empirical_bucket_gate"] = existing_trace["empirical_bucket_gate"]
        trace["empirical_bucket_status"] = existing_trace.get("empirical_bucket_status")
    existing_candidates = {
        (str(candidate.get("side") or ""), str(candidate.get("bucket_key") or "")): candidate
        for candidate in (existing_trace.get("candidates") or [])
        if isinstance(candidate, dict)
    }
    for candidate in trace.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        gate = candidate.get("empirical_bucket_gate")
        existing_candidate = existing_candidates.get((str(candidate.get("side") or ""), str(candidate.get("bucket_key") or "")))
        if (
            isinstance(existing_candidate, dict)
            and isinstance(existing_candidate.get("empirical_bucket_gate"), dict)
            and isinstance(gate, dict)
            and gate.get("enforced") is not True
        ):
            candidate["empirical_bucket_gate"] = existing_candidate["empirical_bucket_gate"]
            candidate["empirical_bucket_status"] = existing_candidate.get("empirical_bucket_status")
    refreshed_trace = {
        **existing_trace,
        **trace,
        "quote_metrics_refreshed_at": datetime.now(UTC).isoformat(),
        "quote_metrics_source": "current_market_quote_cached_prediction",
    }
    refreshed["edge_bps"] = edge_bps
    refreshed["fair_yes_dollars"] = trace.get("fair_yes_dollars")
    refreshed["raw_fair_yes_dollars"] = trace.get("raw_fair_yes_dollars")
    refreshed["recommended_action"] = action.value if action is not None else None
    refreshed["recommended_side"] = side.value if side is not None else None
    refreshed["target_yes_price_dollars"] = (
        _money_text(target_yes)
        if target_yes is not None
        else None
    )
    refreshed["candidate_trace"] = refreshed_trace

    crypto_modeling = refreshed.get("crypto_modeling")
    if isinstance(crypto_modeling, dict):
        trade_selection_model = (
            dict(crypto_modeling.get("trade_selection_model"))
            if isinstance(crypto_modeling.get("trade_selection_model"), dict)
            else {}
        )
        trade_selection_model.update(
            {
                "candidate_status": trace.get("candidate_status"),
                "expected_net_edge": trace.get("expected_net_edge"),
                "rank": trace.get("rank"),
                "bucket_key": trace.get("bucket_key"),
                "empirical_bucket_gate": trace.get("empirical_bucket_gate"),
                "empirical_bucket_status": trace.get("empirical_bucket_status"),
                "last_minute_passive_market_confidence": trace.get("last_minute_passive_market_confidence") is True,
                "last_minute_passive": trace.get("last_minute_passive"),
                "last_minute_passive_bid_threshold_dollars": trace.get("last_minute_passive_bid_threshold_dollars"),
                "last_minute_price_source": trace.get("last_minute_price_source"),
                "last_minute_chosen_bid_dollars": trace.get("last_minute_chosen_bid_dollars"),
                "last_minute_fixed_fallback_bid_dollars": trace.get("last_minute_fixed_fallback_bid_dollars"),
                "last_minute_price_matrix_key": trace.get("last_minute_price_matrix_key"),
                "last_minute_price_matrix_base_key": trace.get("last_minute_price_matrix_base_key"),
                "last_minute_price_matrix_sample_count": trace.get("last_minute_price_matrix_sample_count"),
                "last_minute_price_matrix_fill_count": trace.get("last_minute_price_matrix_fill_count"),
                "last_minute_price_matrix_fill_rate": trace.get("last_minute_price_matrix_fill_rate"),
                "last_minute_price_matrix_net_pnl": trace.get("last_minute_price_matrix_net_pnl"),
                "last_minute_price_matrix_net_pnl_per_signal": trace.get("last_minute_price_matrix_net_pnl_per_signal"),
                "last_minute_passive_no_cross": trace.get("last_minute_passive_no_cross"),
                "decision": "selected" if action is not None else "stand_down",
                "status": (
                    "shadow_only"
                    if trace.get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW
                    else trace.get("candidate_status")
                ),
                "reason": trace.get("selection_reason"),
            }
        )
        refreshed["crypto_modeling"] = {
            **crypto_modeling,
            "trade_selection_model": trade_selection_model,
        }
    return refreshed


def _crypto_replay_gate_dashboard_summary(
    *,
    gates_by_asset: dict[str, Any | None],
    generic_gate: Any | None,
    live_asset_symbols: list[str],
    displayed_asset_symbols: list[str],
) -> dict[str, Any]:
    generic_summary = _artifact_summary(generic_gate)
    asset_statuses = {
        normalize_asset_symbol(asset_symbol): _artifact_summary(
            gates_by_asset.get(normalize_asset_symbol(asset_symbol), generic_gate)
        )
        for asset_symbol in sorted(set(live_asset_symbols + displayed_asset_symbols))
    }
    live_assets = [normalize_asset_symbol(asset_symbol) for asset_symbol in live_asset_symbols]
    displayed_assets = [normalize_asset_symbol(asset_symbol) for asset_symbol in displayed_asset_symbols]
    scoped_assets = live_assets or displayed_assets
    scoped_statuses = [
        asset_statuses.get(asset_symbol, generic_summary).get("status") or "missing"
        for asset_symbol in scoped_assets
    ]
    if not scoped_statuses:
        status = generic_summary["status"]
        scope = "generic"
    elif all(status == "passed" for status in scoped_statuses):
        status = "passed"
        scope = "live_assets" if live_assets else "displayed_assets"
    elif len(set(scoped_statuses)) > 1:
        status = "mixed"
        scope = "live_assets" if live_assets else "displayed_assets"
    else:
        status = scoped_statuses[0]
        scope = "live_assets" if live_assets else "displayed_assets"
    base_summary = (
        asset_statuses.get(scoped_assets[0], generic_summary)
        if len(scoped_assets) == 1
        else generic_summary
    )
    return {
        **base_summary,
        "status": status,
        "scope": scope,
        "asset_statuses": asset_statuses,
        "generic_status": generic_summary["status"],
        "generic": generic_summary,
    }


def _crypto_dashboard_signal_summary(market_payloads: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for market in market_payloads:
        signal = market.get("signal")
        if not isinstance(signal, dict):
            continue
        trace = signal.get("candidate_trace")
        if not isinstance(trace, dict):
            continue
        reason = str(trace.get("selection_reason") or "")
        candidate_status = str(trace.get("candidate_status") or "")
        if candidate_status == CRYPTO_LIVE_QUALITY and reason == "positive_fee_adjusted_live_quality_edge":
            counts["normal_edge_trade_count"] += 1
        if trace.get("late_high_confidence_directional_entry") is True or reason == "late_high_confidence_directional_entry":
            counts["late_high_confidence_trade_count"] += 1
        if trace.get("last_minute_passive_market_confidence") is True or reason == CRYPTO_LAST_MINUTE_PASSIVE_REASON:
            counts["last_minute_passive_trade_count"] += 1
            source = str(trace.get("last_minute_price_source") or "")
            if source == "learned_price_matrix":
                counts["last_minute_passive_learned_price_count"] += 1
            elif source == "fixed_bid":
                counts["last_minute_passive_fixed_fallback_count"] += 1
        gate = trace.get("empirical_bucket_gate")
        if isinstance(gate, dict):
            status = str(gate.get("status") or "unknown")
            if status == "allowed":
                counts["empirical_bucket_allowed_count"] += 1
            elif status == "override_allowed":
                counts["empirical_bucket_override_count"] += 1
                original_reason = str(gate.get("original_reason") or gate.get("reason") or "unknown")
                counts[f"empirical_bucket_override_{original_reason}_count"] += 1
            elif status == "blocked":
                counts["empirical_bucket_blocked_count"] += 1
            elif status == "unknown":
                counts["empirical_bucket_unknown_count"] += 1
    return {
        "normal_edge_trade_count": counts["normal_edge_trade_count"],
        "late_high_confidence_trade_count": counts["late_high_confidence_trade_count"],
        "last_minute_passive_trade_count": counts["last_minute_passive_trade_count"],
        "last_minute_passive_learned_price_count": counts["last_minute_passive_learned_price_count"],
        "last_minute_passive_fixed_fallback_count": counts["last_minute_passive_fixed_fallback_count"],
        "empirical_bucket_allowed_count": counts["empirical_bucket_allowed_count"],
        "empirical_bucket_blocked_count": counts["empirical_bucket_blocked_count"],
        "empirical_bucket_unknown_count": counts["empirical_bucket_unknown_count"],
        "empirical_bucket_override_count": counts["empirical_bucket_override_count"],
        "empirical_bucket_override_missing_count": counts["empirical_bucket_override_empirical_bucket_missing_count"],
        "empirical_bucket_override_low_win_rate_count": counts["empirical_bucket_override_empirical_bucket_low_win_rate_count"],
        "empirical_bucket_override_negative_pnl_count": counts["empirical_bucket_override_empirical_bucket_negative_pnl_count"],
    }


def _receipt_kalshi_client_order_id(details: dict[str, Any] | None, fallback: str) -> str:
    sources: list[Any] = [details or {}]
    if isinstance(details, dict):
        for key in ("order", "raw"):
            value = details.get(key)
            if isinstance(value, dict):
                sources.append(value)
                nested_order = value.get("order")
                if isinstance(nested_order, dict):
                    sources.append(nested_order)
    for source in sources:
        if not isinstance(source, dict):
            continue
        raw = source.get("client_order_id") or source.get("client_id")
        if raw not in (None, ""):
            return str(raw)
    return fallback


def _runtime_crypto_policy_payload(
    crypto_policy: RuntimeCryptoPolicy,
    *,
    asset_symbol: str | None = None,
    frequency: str | None = None,
) -> dict[str, Any]:
    return {
        "entry": crypto_policy.entry_for_asset(asset_symbol, frequency=frequency),
        "replay": {
            "min_resolved_markets": crypto_policy.replay_min_resolved_markets,
            "min_trade_candidates": crypto_policy.replay_min_trade_candidates,
            "min_net_pl_dollars": crypto_policy.replay_min_net_pl_dollars,
            "max_hard_cap_breaches": crypto_policy.replay_max_hard_cap_breaches,
            "min_spot_coverage_pct": crypto_policy.replay_min_spot_coverage_pct,
            "require_calibration_better_than_mid": crypto_policy.replay_require_calibration_better_than_mid,
            "require_pnl_beats_market_mid": crypto_policy.replay_require_pnl_beats_market_mid,
            "min_pnl_advantage_dollars": crypto_policy.replay_min_pnl_advantage_dollars,
        },
        "live": {
            "trading_enabled": crypto_policy.trading_enabled,
            "production_autonomy_enabled": crypto_policy.production_autonomy_enabled,
            "asset_mode": crypto_policy.asset_modes.get(normalize_asset_symbol(asset_symbol or "UNKNOWN")),
        },
    }


def _resolved_crypto_asset_modes(
    *,
    asset_symbols: list[str],
    note_modes: dict[str, str],
    crypto_policy: RuntimeCryptoPolicy,
) -> dict[str, str]:
    symbols = {normalize_asset_symbol(symbol) for symbol in asset_symbols}
    symbols.update(note_modes)
    symbols.update(crypto_policy.asset_modes)
    resolved: dict[str, str] = {}
    for symbol in sorted(symbols):
        note_mode = note_modes.get(symbol)
        if note_mode == CRYPTO_ASSET_MODE_OFF:
            resolved[symbol] = CRYPTO_ASSET_MODE_OFF
        elif symbol in crypto_policy.asset_modes:
            resolved[symbol] = crypto_policy.asset_modes[symbol]
        else:
            resolved[symbol] = note_mode or CRYPTO_ASSET_MODE_SHADOW
    return resolved


def _runtime_replay_gate_blockers(replay_gate: Any | None, crypto_policy: RuntimeCryptoPolicy | None) -> list[str]:
    if replay_gate is None:
        return ["Crypto replay gate is missing."]
    if str(getattr(replay_gate, "artifact_type", "") or "").startswith("replay_gate_touch20"):
        gate_status = getattr(replay_gate, "status", None)
        return [] if gate_status == "passed" else [f"BTC 1h touch replay gate is {gate_status or 'missing'}."]
    if crypto_policy is None:
        gate_status = getattr(replay_gate, "status", None)
        return [] if gate_status == "passed" else [f"Crypto replay gate is {gate_status or 'missing'}."]
    metrics = dict(getattr(replay_gate, "metrics", None) or {})
    if not metrics:
        gate_status = getattr(replay_gate, "status", None)
        return [] if gate_status == "passed" else [f"Crypto replay gate is {gate_status or 'missing'}."]
    reasons = _crypto_replay_gate_reasons(metrics, crypto_policy=crypto_policy)
    return [f"Crypto replay gate runtime policy blocked: {reason}" for reason in reasons]


def _runtime_replay_gate_passed(replay_gate: Any | None, crypto_policy: RuntimeCryptoPolicy | None) -> bool:
    return not _runtime_replay_gate_blockers(replay_gate, crypto_policy)


def _crypto_live_pnl_gate_payload(
    stats: dict[str, Any],
    *,
    settings: Settings,
    contract_price_dollars: Decimal,
) -> dict[str, Any]:
    enabled = bool(settings.crypto_live_pnl_gate_enabled)
    min_fills = int(settings.crypto_live_pnl_gate_min_fills)
    min_contracts = Decimal(str(settings.crypto_live_pnl_gate_min_contracts))
    min_net_pnl = Decimal(str(settings.crypto_live_pnl_gate_min_net_pnl_dollars))
    min_pnl_per_contract = Decimal(str(settings.crypto_live_pnl_gate_min_pnl_per_contract_dollars))
    fill_count = int(stats.get("fill_count") or 0)
    contracts = Decimal(str(stats.get("contracts") or "0"))
    net_pnl = Decimal(str(stats.get("net_pnl_dollars") or "0"))
    pnl_per_contract = Decimal(str(stats.get("pnl_per_contract_dollars") or "0"))
    evidence_ready = fill_count >= min_fills or contracts >= min_contracts
    blockers: list[str] = []
    if enabled and evidence_ready:
        if net_pnl < min_net_pnl:
            blockers.append(
                f"Live P&L cell net P/L ${net_pnl:.2f} below minimum ${min_net_pnl:.2f} "
                f"over {fill_count} fills / {contracts:.2f} contracts."
            )
        if pnl_per_contract < min_pnl_per_contract:
            blockers.append(
                f"Live P&L cell P/L per contract ${pnl_per_contract:.4f} below minimum "
                f"${min_pnl_per_contract:.4f}."
            )
    return {
        "enabled": enabled,
        "mode": str(settings.crypto_live_pnl_gate_mode or "block").strip().lower(),
        "status": "blocked" if blockers else ("insufficient_evidence" if enabled and not evidence_ready else "passed"),
        "blockers": blockers,
        "contract_price_dollars": str(contract_price_dollars.quantize(Decimal("0.0001"))),
        "thresholds": {
            "evidence_rule": "min_fills_or_min_contracts",
            "min_fills": min_fills,
            "min_contracts": str(min_contracts.quantize(Decimal("0.01"))),
            "min_net_pnl_dollars": str(min_net_pnl.quantize(Decimal("0.0001"))),
            "min_pnl_per_contract_dollars": str(min_pnl_per_contract.quantize(Decimal("0.0001"))),
        },
        "stats": stats,
    }


def _crypto_live_pnl_gate_blocks(payload: dict[str, Any], *, settings: Settings) -> bool:
    if not bool(settings.crypto_live_pnl_gate_enabled):
        return False
    if str(settings.crypto_live_pnl_gate_mode or "block").strip().lower() not in {"block", "enforce", "live"}:
        return False
    return bool(payload.get("blockers"))


def _crypto_order_decision_lineage(
    lineage: dict[str, Any] | None,
    *,
    liquidity: str,
    order_mode: str,
) -> dict[str, Any]:
    return {
        **(lineage or {}),
        "liquidity_policy": liquidity,
        "crypto_order_mode": order_mode,
    }


def _crypto_raw_with_decision_lineage(raw: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
    receipt_lineage = raw.get("decision_lineage") if isinstance(raw, dict) else None
    merged_lineage = {
        **(lineage or {}),
        **(receipt_lineage if isinstance(receipt_lineage, dict) else {}),
    }
    return {**(raw if isinstance(raw, dict) else {}), "decision_lineage": merged_lineage}


def _crypto_decision_lineage_payload(
    *,
    room: Room,
    market: CryptoMarket,
    signal: StrategySignal,
    signal_record: Signal,
    strategy_code: str,
    gate: Any | None,
    backtest: Any | None,
    live_status: dict[str, Any],
    proposed_count_fp: Decimal,
    sizing_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    prediction_model = trace.get("prediction_model") if isinstance(trace.get("prediction_model"), dict) else {}
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    return {
        "schema_version": "crypto-decision-lineage-v1",
        "room_id": room.id,
        "signal_id": signal_record.id,
        "signal_created_at": signal_record.created_at.isoformat() if signal_record.created_at else None,
        "market_ticker": market.market_ticker,
        "asset_symbol": market.asset_symbol,
        "frequency": market.frequency,
        "strategy_code": strategy_code,
        "fair_yes_dollars": _money_text(signal.fair_yes_dollars),
        "target_yes_price_dollars": _money_text(signal.target_yes_price_dollars),
        "selected_price_dollars": _money_text(signal.target_yes_price_dollars),
        "selected_side": signal.recommended_side.value if signal.recommended_side else trace.get("selected_side"),
        "edge_bps": signal.edge_bps,
        "expected_net_edge_bps": _crypto_signal_expected_net_edge_bps(signal),
        "confidence": signal.confidence,
        "bucket_key": trace.get("bucket_key"),
        "price_bucket": _price_band(
            signal.target_yes_price_dollars
            if signal.recommended_side == ContractSide.YES
            else Decimal("1.0000") - signal.target_yes_price_dollars
        )
        if signal.target_yes_price_dollars is not None and signal.recommended_side is not None
        else None,
        "candidate_status": selection.get("candidate_status") or trace.get("candidate_status"),
        "selection_reason": selection.get("selection_reason") or trace.get("selection_reason"),
        "model_version": trace.get("model_version"),
        "prediction_model": prediction_model,
        "backtest_version": backtest.version if backtest is not None else None,
        "replay_gate_status": gate.status if gate is not None else "missing",
        "replay_gate_version": gate.version if gate is not None else None,
        "replay_gate_artifact_type": gate.artifact_type if gate is not None else None,
        "asset_mode": live_status.get("asset_mode"),
        "control_asset_mode": live_status.get("control_asset_mode"),
        "live_eligible": live_status.get("live_eligible"),
        "proposed_count_fp": _count_text(proposed_count_fp),
        "sizing": sizing_diagnostics,
    }


async def _upsert_crypto_decision_outcome_for_signal(
    repo: PlatformRepository,
    *,
    room: Room,
    market: CryptoMarket,
    signal: StrategySignal,
    signal_record: Signal,
    decision_kind: str,
    gate_status: str | None,
    selected_count_fp: Decimal | None,
) -> None:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    prediction_model = trace.get("prediction_model") if isinstance(trace.get("prediction_model"), dict) else {}
    decision_time = signal_record.created_at or datetime.now(UTC)
    input_payload = {
        "signal_id": signal_record.id,
        "room_id": room.id,
        "market_ticker": market.market_ticker,
        "decision_time": decision_time.isoformat(),
        "decision_kind": decision_kind,
    }
    input_hash = hashlib.sha256(json.dumps(input_payload, sort_keys=True).encode("utf-8")).hexdigest()
    await repo.upsert_crypto_decision_outcome(
        kalshi_env=room.kalshi_env,
        frequency=market.frequency,
        market_ticker=market.market_ticker,
        asset_symbol=market.asset_symbol,
        decision_time=decision_time,
        decision_kind=decision_kind,
        input_hash=input_hash,
        trace_hash=None,
        model_version=str(trace.get("model_version") or "") or None,
        prediction_yes=_optional_decimal(
            prediction_model.get("calibrated_probability")
            or prediction_model.get("raw_probability")
            or signal.fair_yes_dollars
        ),
        selected_side=signal.recommended_side.value if signal.recommended_side else trace.get("selected_side"),
        selected_price_dollars=signal.target_yes_price_dollars,
        selected_count_fp=selected_count_fp,
        gate_status=gate_status,
        settlement_result=None,
        simulated_pnl_dollars=_optional_decimal(
            trace.get("expected_net_pnl")
            or trace.get("last_minute_price_matrix_net_pnl")
            or trace.get("simulated_pnl_dollars")
        ),
        realized_pnl_dollars=None,
        fill_count=0,
        source_snapshot_ids=trace.get("source_snapshot_ids") or {},
        payload={
            "signal_id": signal_record.id,
            "room_id": room.id,
            "candidate_trace": _crypto_training_json_ready(trace),
        },
    )


def _crypto_signal_stand_down_payload(signal: StrategySignal) -> dict[str, Any]:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    eligibility = signal.eligibility
    eligibility_trace = (
        eligibility.candidate_trace
        if eligibility is not None and isinstance(eligibility.candidate_trace, dict)
        else {}
    )
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    if not selection and isinstance(eligibility_trace.get("trade_selection_model"), dict):
        selection = eligibility_trace["trade_selection_model"]
    stand_down_reason = signal.stand_down_reason
    if stand_down_reason is None and eligibility is not None:
        stand_down_reason = eligibility.stand_down_reason
    return _crypto_json_safe(
        {
            "reason": "signal_not_tradeable",
            "stand_down_reason": getattr(stand_down_reason, "value", stand_down_reason),
            "evaluation_outcome": signal.evaluation_outcome
            or (eligibility.evaluation_outcome if eligibility is not None else None)
            or trace.get("outcome")
            or eligibility_trace.get("outcome"),
            "candidate_status": selection.get("candidate_status")
            or trace.get("candidate_status")
            or eligibility_trace.get("candidate_status"),
            "selection_reason": selection.get("reason")
            or trace.get("selection_reason")
            or eligibility_trace.get("selection_reason"),
            "pre_empirical_selection_reason": selection.get("pre_empirical_reason")
            or trace.get("pre_empirical_selection_reason")
            or eligibility_trace.get("pre_empirical_selection_reason"),
            "expected_net_edge": selection.get("expected_net_edge") or trace.get("expected_net_edge"),
            "selected_side": trace.get("selected_side") or eligibility_trace.get("selected_side"),
            "selected_edge_bps": trace.get("selected_edge_bps") or signal.edge_bps,
            "target_yes_price_dollars": trace.get("target_yes_price_dollars"),
            "empirical_bucket_status": selection.get("empirical_bucket_status")
            or trace.get("empirical_bucket_status"),
            "empirical_bucket_reason": (
                selection.get("empirical_bucket_gate") or {}
            ).get("reason")
            if isinstance(selection.get("empirical_bucket_gate"), dict)
            else None,
            "last_minute_passive_reason": (
                selection.get("last_minute_passive") or {}
            ).get("reason")
            if isinstance(selection.get("last_minute_passive"), dict)
            else None,
            "gate_cascade": trace.get("gate_cascade") or eligibility_trace.get("gate_cascade"),
        }
    )


def _crypto_signal_stand_down_content(payload: dict[str, Any]) -> str:
    outcome = payload.get("evaluation_outcome") or "not_tradeable"
    detail = payload.get("selection_reason") or payload.get("candidate_status") or payload.get("stand_down_reason")
    if detail:
        return f"No crypto trade ticket created: {outcome} ({detail})."
    return f"No crypto trade ticket created: {outcome}."


def _crypto_price_bucket_gate_reasons(
    bucket_matrix: list[dict[str, Any]] | None,
    *,
    crypto_policy: RuntimeCryptoPolicy,
) -> list[str]:
    """Hard-fail any entry-price bucket that bleeds, even when pooled metrics pass.

    Groups rows by ``entry_price_band``, summing ``sample_count`` and ``net_pnl`` and
    reconstructing an aggregate win rate from per-row ``win_rate * sample_count``. Buckets
    whose summed sample count is below the per-bucket floor are SKIPPED (safety valve so
    small buckets cannot block promotion).
    """
    reasons: list[str] = []
    grouped: dict[str, dict[str, Any]] = {}
    for bucket in bucket_matrix or []:
        if not isinstance(bucket, dict):
            continue
        band = bucket.get("entry_price_band")
        if not band:
            continue
        try:
            samples = int(bucket.get("sample_count") or 0)
        except (TypeError, ValueError):
            continue
        if samples <= 0:
            continue
        win_rate = bucket.get("outcome_win_rate")
        if win_rate is None:
            win_rate = bucket.get("win_rate")
        if win_rate is None:
            continue
        try:
            net_pnl = float(Decimal(str(bucket.get("net_pnl") or "0")))
        except Exception:
            net_pnl = 0.0
        agg = grouped.setdefault(str(band), {"samples": 0, "wins": 0.0, "net_pnl": 0.0})
        agg["samples"] += samples
        agg["wins"] += round(float(win_rate) * samples)
        agg["net_pnl"] += net_pnl

    min_samples = int(crypto_policy.replay_per_price_bucket_min_samples)
    min_win_rate = float(crypto_policy.replay_per_price_bucket_min_win_rate)
    min_net_pnl = float(crypto_policy.replay_per_price_bucket_min_net_pnl_dollars)
    for band, agg in sorted(grouped.items()):
        samples = int(agg["samples"])
        if samples < min_samples:
            continue
        agg_win_rate = agg["wins"] / samples if samples > 0 else 0.0
        if agg_win_rate < min_win_rate:
            reasons.append(
                f"Entry-price bucket {band} win rate {agg_win_rate:.1%} below minimum "
                f"{min_win_rate:.1%} over {samples} samples."
            )
        if agg["net_pnl"] < min_net_pnl:
            reasons.append(
                f"Entry-price bucket {band} net P/L ${agg['net_pnl']:.2f} below minimum "
                f"${min_net_pnl:.2f} over {samples} samples."
            )
    return reasons


def _crypto_replay_gate_reasons(metrics: dict[str, Any], *, crypto_policy: RuntimeCryptoPolicy) -> list[str]:
    reasons: list[str] = []
    bucket_gate_applied = bool(metrics.get("empirical_bucket_gate_applied_to_metrics")) or str(
        metrics.get("metrics_source") or ""
    ) == "empirical_bucket_gated"
    # Market-count keys (new artifacts) replace row counts for candidate-count
    # checks; absent keys (old artifacts) keep row-count behavior unchanged.
    resolved_market_count = metrics.get("resolved_market_count")
    if resolved_market_count is not None:
        resolved = int(resolved_market_count or 0)
    else:
        resolved = int(metrics.get("resolved_sample_count") or metrics.get("sample_count") or 0)
    current_model_market_count = metrics.get("current_model_live_quality_market_count")
    if current_model_market_count is not None:
        current_model_candidates = int(current_model_market_count or 0)
    else:
        current_model_candidates = int(
            metrics.get("current_model_live_quality_candidate_count", metrics.get("trade_candidate_count")) or 0
        )
    oos_market_candidates = metrics.get("oos_trade_candidate_market_count")
    if oos_market_candidates is not None:
        oos_candidates = int(oos_market_candidates or 0)
    else:
        raw_oos_candidates = metrics.get("oos_trade_candidate_count")
        oos_candidates = int(raw_oos_candidates or 0)
    oos_fold_count = metrics.get("oos_fold_count")
    oos_evaluation_status = str(metrics.get("oos_evaluation_status") or "").strip().lower()
    has_oos_markers = oos_fold_count is not None or bool(oos_evaluation_status)
    has_usable_oos = has_oos_markers and int(oos_fold_count or 0) > 0 and oos_evaluation_status in {"", "ok"}
    net_pl = float(metrics.get("oos_net_simulated_pl_dollars", metrics.get("net_simulated_pl_dollars") or 0.0) or 0.0)
    market_mid_net_pl = float(
        metrics.get("oos_market_mid_net_simulated_pl_dollars", metrics.get("market_mid_net_simulated_pl_dollars") or 0.0)
        or 0.0
    )
    pnl_advantage = float(
        metrics.get(
            "oos_pnl_advantage_vs_market_mid_dollars",
            metrics.get("pnl_advantage_vs_market_mid_dollars") or (net_pl - market_mid_net_pl),
        )
        or 0.0
    )
    hard_cap_breaches = int(metrics.get("hard_cap_breaches") or 0)
    calibration = metrics.get("calibration_brier")
    market_mid = metrics.get("market_mid_brier")
    calibration_log_loss = metrics.get("calibration_log_loss")
    market_mid_log_loss = metrics.get("market_mid_log_loss")
    calibration_ece = metrics.get("calibration_ece")
    market_mid_ece = metrics.get("market_mid_ece")
    candle_count = int(metrics.get("candle_count") or 0)
    leakage_rows = int(metrics.get("leakage_row_count") or 0)
    spot_coverage = float(metrics.get("spot_feature_coverage_pct") or 0.0)
    strict_market_count = metrics.get("strict_trade_eligible_market_count")
    if strict_market_count is not None:
        strict_trade_rows = int(strict_market_count or 0)
    else:
        strict_trade_rows = int(metrics.get("strict_trade_eligible_count") or 0)
    if metrics.get("model_missing"):
        reasons.append("Crypto model artifact is missing.")
    if metrics.get("backtest_missing"):
        reasons.append("Crypto backtest artifact is missing.")
    if candle_count <= 0:
        reasons.append("Crypto candlestick coverage is missing.")
    if has_oos_markers and (int(oos_fold_count or 0) <= 0 or oos_evaluation_status not in {"", "ok"}):
        reasons.append(
            "Out-of-sample replay is unavailable "
            f"(status={oos_evaluation_status or 'unknown'}, folds={int(oos_fold_count or 0)})."
        )
    if leakage_rows > 0:
        reasons.append(f"Replay includes {leakage_rows} non-point-in-time rows.")
    if spot_coverage < crypto_policy.replay_min_spot_coverage_pct:
        reasons.append(
            f"Spot feature coverage {spot_coverage:.1%} below minimum "
            f"{crypto_policy.replay_min_spot_coverage_pct:.1%}."
        )
    if strict_trade_rows < crypto_policy.replay_min_trade_candidates:
        reasons.append(
            f"Strict real-quote row coverage {strict_trade_rows} below minimum "
            f"{crypto_policy.replay_min_trade_candidates}."
        )
    if resolved < crypto_policy.replay_min_resolved_markets:
        reasons.append(
            f"Resolved sample coverage {resolved} below minimum {crypto_policy.replay_min_resolved_markets}."
        )
    if has_usable_oos and oos_candidates < crypto_policy.replay_min_trade_candidates:
        reasons.append(
            f"Out-of-sample trade candidate count {oos_candidates} below minimum "
            f"{crypto_policy.replay_min_trade_candidates}."
        )
    if current_model_candidates < crypto_policy.replay_min_trade_candidates:
        reasons.append(
            f"Current model live-quality candidate count {current_model_candidates} below minimum "
            f"{crypto_policy.replay_min_trade_candidates}."
        )
    if net_pl <= crypto_policy.replay_min_net_pl_dollars:
        reasons.append(f"Net simulated P/L ${net_pl:.2f} does not clear required positive threshold.")
    bucket_gated_market_mid_tie = (
        str(metrics.get("metrics_source") or "") == "empirical_bucket_gated"
        and float(crypto_policy.replay_min_pnl_advantage_dollars) == 0.0
        and abs(pnl_advantage) <= 1e-9
        and net_pl > crypto_policy.replay_min_net_pl_dollars
    )
    if (
        crypto_policy.replay_require_pnl_beats_market_mid
        and pnl_advantage <= crypto_policy.replay_min_pnl_advantage_dollars
        and not bucket_gated_market_mid_tie
    ):
        reasons.append(
            "Model fee-adjusted P/L does not beat the market-mid baseline "
            f"(${net_pl:.2f} vs ${market_mid_net_pl:.2f}; advantage ${pnl_advantage:.2f})."
        )
    if hard_cap_breaches > crypto_policy.replay_max_hard_cap_breaches:
        reasons.append(f"Replay hard-cap breaches {hard_cap_breaches} exceed limit.")
    if crypto_policy.replay_require_calibration_better_than_mid:
        if calibration is None or market_mid is None or float(calibration) > float(market_mid):
            reasons.append("Calibration Brier does not beat the market-mid baseline.")
        if (
            calibration_log_loss is None
            or market_mid_log_loss is None
            or float(calibration_log_loss) > float(market_mid_log_loss)
        ):
            reasons.append("Calibration log-loss does not beat the market-mid baseline.")
        if calibration_ece is None or market_mid_ece is None or float(calibration_ece) > float(market_mid_ece):
            reasons.append("Calibration ECE does not beat the market-mid baseline.")
        market_weighted_pairs = (
            ("calibration_brier_market_weighted", "market_mid_brier_market_weighted", "Brier"),
            ("calibration_log_loss_market_weighted", "market_mid_log_loss_market_weighted", "log-loss"),
            ("calibration_ece_market_weighted", "market_mid_ece_market_weighted", "ECE"),
        )
        for model_key, mid_key, label in market_weighted_pairs:
            if model_key not in metrics and mid_key not in metrics:
                continue
            model_value = metrics.get(model_key)
            mid_value = metrics.get(mid_key)
            if model_value is None or mid_value is None or float(model_value) > float(mid_value):
                reasons.append(f"Market-weighted calibration {label} does not beat the market-mid baseline.")
    if current_model_candidates >= crypto_policy.replay_min_trade_candidates:
        pnl_per_candidate = net_pl / current_model_candidates if current_model_candidates > 0 else 0.0
        if pnl_per_candidate < crypto_policy.replay_min_pnl_per_candidate_dollars:
            reasons.append(
                f"Net simulated P/L per candidate ${pnl_per_candidate:.4f} below minimum "
                f"${crypto_policy.replay_min_pnl_per_candidate_dollars:.4f}."
            )
    if crypto_policy.replay_per_price_bucket_gate_enabled:
        bucket_matrix = metrics.get("bucket_matrix")
        if bucket_gate_applied:
            bucket_matrix = (
                ((metrics.get("bucket_gated_metrics") or {}).get("selection_policy") or {}).get("worst_buckets")
                or bucket_matrix
            )
        reasons.extend(
            _crypto_price_bucket_gate_reasons(
                bucket_matrix,
                crypto_policy=crypto_policy,
            )
        )
    return reasons


def _nearest_market_per_asset(markets: list[CryptoMarket]) -> list[CryptoMarket]:
    now = datetime.now(UTC)
    by_asset: dict[str, CryptoMarket] = {}
    for market in markets:
        existing = by_asset.get(market.asset_symbol)
        if existing is None or _market_sort_key(market, now) < _market_sort_key(existing, now):
            by_asset[market.asset_symbol] = market
    return sorted(by_asset.values(), key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol))


def _market_sort_key(market: CryptoMarket, now: datetime) -> tuple[int, float, int, float, int, str]:
    mid = market.mid_yes_dollars or market.last_price_dollars
    quote_missing = 0 if market.yes_bid_dollars is not None and market.yes_ask_dollars is not None else 1
    mid_distance = abs(float(mid - Decimal("0.5000"))) if mid is not None else float("inf")
    spread_bps = market.spread_bps if market.spread_bps is not None else 1_000_000
    if market.close_time is None:
        return (2, float("inf"), quote_missing, mid_distance, spread_bps, market.market_ticker)
    seconds = (market.close_time - now).total_seconds()
    if seconds >= 0:
        return (0, seconds, quote_missing, mid_distance, spread_bps, market.market_ticker)
    return (1, abs(seconds), quote_missing, mid_distance, spread_bps, market.market_ticker)


def _eligible_market_per_asset(
    markets: list[CryptoMarket],
    *,
    min_seconds_to_close: int,
    min_market_age_seconds: int = 0,
) -> tuple[list[CryptoMarket], list[dict[str, Any]]]:
    now = datetime.now(UTC)
    grouped: dict[str, list[CryptoMarket]] = {}
    for market in markets:
        grouped.setdefault(market.asset_symbol, []).append(market)

    selected: list[CryptoMarket] = []
    skipped: list[dict[str, Any]] = []
    for asset_symbol, asset_markets in sorted(grouped.items()):
        ordered = sorted(asset_markets, key=lambda market: _market_sort_key(market, now))
        chosen: CryptoMarket | None = None
        latest_skip: dict[str, Any] | None = None
        for market in ordered:
            if market.close_time is None:
                latest_skip = {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "missing_close_time",
                }
                continue
            seconds_to_close = int((market.close_time - now).total_seconds())
            if seconds_to_close < min_seconds_to_close:
                latest_skip = {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "too_close_to_close",
                    "seconds_to_close": seconds_to_close,
                }
                continue
            market_age_seconds = _crypto_live_market_age_seconds(now, market)
            if min_market_age_seconds > 0 and (
                market_age_seconds is None or market_age_seconds < min_market_age_seconds
            ):
                latest_skip = {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "crypto_market_too_early_for_live_entry",
                    "market_age_seconds": market_age_seconds,
                    "min_market_age_seconds": min_market_age_seconds,
                }
                continue
            chosen = market
            break
        if chosen is not None:
            selected.append(chosen)
        elif latest_skip is not None:
            skipped.append(latest_skip)
        else:
            skipped.append({"asset_symbol": asset_symbol, "reason": "no_markets"})

    return (
        sorted(selected, key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol)),
        skipped,
    )


def _crypto_live_market_age_seconds(now: datetime, market: CryptoMarket) -> int | None:
    market_age = _crypto_market_age_seconds(now, market.open_time)
    if market_age is not None:
        return market_age
    close_time = market.close_time or market.expected_expiration_time
    if close_time is None:
        return None
    frequency = normalize_frequency(market.frequency) or "15m"
    try:
        interval_seconds = interval_seconds_for_frequency(frequency)
    except ValueError:
        return None
    seconds_to_close = int((_as_utc_datetime(close_time) - _as_utc_datetime(now)).total_seconds())
    return max(0, interval_seconds - seconds_to_close)


def _cap_crypto_autonomy_markets(
    markets: list[CryptoMarket],
    *,
    max_rooms: int,
    max_per_asset: int,
) -> tuple[list[CryptoMarket], list[dict[str, Any]]]:
    if max_rooms <= 0:
        return [], [
            {
                "market_ticker": market.market_ticker,
                "asset_symbol": market.asset_symbol,
                "reason": "autonomy_room_cap_zero",
            }
            for market in markets
        ]
    counts: Counter[str] = Counter()
    selected: list[CryptoMarket] = []
    skipped: list[dict[str, Any]] = []
    for market in markets:
        if counts[market.asset_symbol] >= max_per_asset:
            skipped.append(
                {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "autonomy_asset_cap",
                }
            )
            continue
        if len(selected) >= max_rooms:
            skipped.append(
                {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "autonomy_total_room_cap",
                }
            )
            continue
        selected.append(market)
        counts[market.asset_symbol] += 1
    return selected, skipped


def _row_mid(row: CryptoMarketSnapshotRecord) -> Decimal | None:
    yes_bid = _snapshot_price(row, attr="yes_bid_dollars", dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",))
    yes_ask = _snapshot_price(row, attr="yes_ask_dollars", dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / Decimal("2")
    return _snapshot_price(
        row,
        attr="last_price_dollars",
        dollar_keys=("last_price_dollars", "last_trade_price_dollars"),
        cent_keys=("last_price", "last_trade_price"),
    )


def _snapshot_payload_sources(row: CryptoMarketSnapshotRecord) -> list[dict[str, Any]]:
    payload = row.payload if isinstance(getattr(row, "payload", None), dict) else {}
    sources: list[dict[str, Any]] = []
    for source in (
        payload,
        payload.get("market"),
        payload.get("raw"),
        (payload.get("raw") or {}).get("market") if isinstance(payload.get("raw"), dict) else None,
    ):
        if isinstance(source, dict):
            sources.append(source)
    return sources


def _snapshot_price(
    row: CryptoMarketSnapshotRecord,
    *,
    attr: str,
    dollar_keys: tuple[str, ...],
    cent_keys: tuple[str, ...],
) -> Decimal | None:
    value = getattr(row, attr, None)
    if value is not None:
        return value
    for source in _snapshot_payload_sources(row):
        parsed = parse_price(source, dollar_keys=dollar_keys, cent_keys=cent_keys)
        if parsed is not None:
            return parsed
    return None


def _crypto_live_market_row(
    market: CryptoMarket,
    *,
    spot_rows: list[CryptoSpotOHLCRecord] | None = None,
    cross_asset_spot: dict[str, list[CryptoSpotOHLCRecord]] | None = None,
    funding_rate_rows: list[CryptoFundingRateRecord] | None = None,
    prior_quote: CryptoMarketSnapshotRecord | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    mid = market.mid_yes_dollars or market.last_price_dollars or Decimal("0.5000")
    close_time = market.close_time or market.expected_expiration_time
    no_ask = market.no_ask_dollars
    if no_ask is None and market.yes_bid_dollars is not None:
        no_ask = Decimal("1.0000") - market.yes_bid_dollars
    strict_trade_eligible = market.yes_bid_dollars is not None and market.yes_ask_dollars is not None
    market_age_seconds = _crypto_market_age_seconds(now, market.open_time)
    row = {
        "row_id": f"live:{market.market_ticker}:{now.isoformat()}",
        "market_ticker": market.market_ticker,
        "series_ticker": market.series_ticker,
        "asset_symbol": market.asset_symbol,
        "frequency": market.frequency,
        "source_kind": "live_market_snapshot",
        "quote_source": "live_market_snapshot",
        "leakage_status": "point_in_time",
        "prediction_eligible": True,
        "strict_trade_eligible": strict_trade_eligible,
        "execution_model_status": "real_quote_taker" if strict_trade_eligible else "missing_real_quote",
        "decision_ts": now,
        "settlement_ts": close_time,
        "market_day": now.date().isoformat(),
        "target_price_dollars": market.target_price_dollars,
        "mid_yes_dollars": _clamp_price(mid),
        "yes_bid_dollars": _clamp_price(market.yes_bid_dollars) if market.yes_bid_dollars is not None else None,
        "yes_ask_dollars": _clamp_price(market.yes_ask_dollars) if market.yes_ask_dollars is not None else None,
        "no_bid_dollars": _clamp_price(market.no_bid_dollars) if market.no_bid_dollars is not None else None,
        "no_ask_dollars": _clamp_price(no_ask) if no_ask is not None else None,
        "spread_bps": market.spread_bps,
        "volume": market.volume,
        "open_interest": market.open_interest,
        "time_to_close_seconds": int((close_time - now).total_seconds()) if close_time is not None else None,
        "market_age_seconds": market_age_seconds,
        "candle_momentum_dollars": Decimal("0"),
        "spot_feature_status": "missing",
        "asset_recent_yes_rate": None,
        "asset_recent_mid_error": None,
    }
    sorted_spot_rows = sorted(spot_rows or [], key=lambda item: item.end_ts)
    row.update(
        _spot_context_for_decision(
            sorted_spot_rows,
            decision_ts=now,
            target_price=market.target_price_dollars,
            mid_yes=_clamp_price(mid),
            settings=settings,
            mode=CRYPTO_SPOT_CONTEXT_LIVE,
        )
    )
    row.update(
        _settlement_window_feature_context(
            sorted_spot_rows,
            close_time=close_time,
            target_price=market.target_price_dollars,
            frequency=market.frequency,
            as_of=now,
        )
    )
    row.update(
        _cross_asset_context(
            cross_asset_spot or {},
            decision_ts=now,
            exclude_asset=market.asset_symbol,
        )
    )
    if prior_quote is not None:
        row.update(
            _crypto_quote_sequence_context(
                _clamp_price(mid),
                float(market.spread_bps) if market.spread_bps is not None else None,
                now,
                _row_mid(prior_quote),
                _crypto_snapshot_spread_bps(prior_quote, None, None),
                prior_quote.observed_at,
            )
        )
    funding_by_asset: dict[str, list[CryptoFundingRateRecord]] = defaultdict(list)
    for rate in funding_rate_rows or []:
        funding_by_asset[rate.asset_symbol].append(rate)
    for asset_rates in funding_by_asset.values():
        asset_rates.sort(key=lambda r: r.settlement_ts)
    row.update(_funding_rate_context_for_decision(funding_by_asset, market.asset_symbol, decision_ts=now))
    return row


def _recent_momentum_adjustment(features: dict[str, Any]) -> Decimal:
    last_raw = features.get("last_price_dollars")
    mid_raw = features.get("mid_yes_dollars")
    if last_raw in (None, "") or mid_raw in (None, ""):
        return Decimal("0")
    return ((Decimal(str(last_raw)) - Decimal(str(mid_raw))) / Decimal("4")).quantize(Decimal("0.0001"))


def _crypto_recommendation(
    *,
    market: CryptoMarket,
    fair_yes: Decimal,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    row: dict[str, Any] | None = None,
    require_spot_features: bool = True,
    empirical_bucket_matrix: list[dict[str, Any]] | None = None,
    last_minute_passive_price_matrix: list[dict[str, Any]] | None = None,
    enforce_empirical_bucket_gate: bool = False,
    touch_replay_gate: Any | None = None,
    edge_shrinkage: dict[str, Any] | None = None,
) -> tuple[TradeAction | None, ContractSide | None, Decimal | None, int, dict[str, Any]]:
    row = row or _crypto_live_market_row(market, settings=settings)
    btc_1h_touch_configured = _crypto_btc_1h_touch_policy_configured_for_row(row, settings=settings)
    btc_1h_touch_gate_passed = _crypto_touch_replay_gate_passed(touch_replay_gate)
    btc_1h_touch_allowed = btc_1h_touch_configured and btc_1h_touch_gate_passed
    legacy_touch_allowed = bool(settings.crypto_touch_strategy_enabled) and not bool(settings.crypto_model_trained_replay_only)
    if btc_1h_touch_configured and not btc_1h_touch_gate_passed:
        target_pct = Decimal(str(settings.crypto_1h_touch_take_profit_pct))
        objective = _crypto_touch_objective(target_pct)
        return (
            None,
            None,
            None,
            0,
            {
                "outcome": "touch_strategy_blocked",
                "fair_yes_dollars": _money_text(_clamp_price(fair_yes)),
                "raw_fair_yes_dollars": _money_text(_clamp_price(fair_yes)),
                "market_anchored_fair_yes_dollars": _money_text(_clamp_price(fair_yes)),
                "selected_side": None,
                "selected_edge_bps": 0,
                "candidate_status": "blocked_touch_strategy",
                "selection_reason": "touch20_replay_gate_missing_or_blocked",
                "expected_net_edge": None,
                "rank": None,
                "bucket_key": None,
                "target_yes_price_dollars": None,
                "min_edge_bps": _crypto_entry_policy_for_row(row, settings=settings, crypto_policy=crypto_policy)["min_fee_adjusted_edge_bps"],
                "spread_bps": market.spread_bps,
                "candidates": [],
                "gate_cascade": [],
                "touch_strategy": {
                    "enabled": True,
                    "policy": "btc_1h_touch20",
                    "objective": _crypto_touch_exit_objective(target_pct),
                    "take_profit_pct": float(target_pct),
                    "gate_required": True,
                    "gate_status": getattr(touch_replay_gate, "status", None) if touch_replay_gate is not None else "missing",
                },
                "touch_replay_gate": {
                    "status": getattr(touch_replay_gate, "status", None) if touch_replay_gate is not None else "missing",
                    "version": getattr(touch_replay_gate, "version", None) if touch_replay_gate is not None else None,
                    "artifact_type": getattr(touch_replay_gate, "artifact_type", None) if touch_replay_gate is not None else "replay_gate_touch20:BTC",
                },
                "objective": objective,
                **_crypto_settlement_diagnostics(row),
            },
        )
    if legacy_touch_allowed or btc_1h_touch_allowed:
        raw_fair_yes = _clamp_price(fair_yes)
        entry_policy = _crypto_entry_policy_for_row(row, settings=settings, crypto_policy=crypto_policy)
        candidates = _crypto_touch_strategy_candidates(
            row,
            settings=settings,
            crypto_policy=crypto_policy,
            empirical_bucket_matrix=empirical_bucket_matrix,
            enforce_empirical_bucket_gate=enforce_empirical_bucket_gate,
            btc_1h_touch_policy=btc_1h_touch_allowed,
        )
        selected = next((candidate for candidate in candidates if candidate.get("candidate_status") == CRYPTO_LIVE_QUALITY), None)
        best = selected or (candidates[0] if candidates else {})
        objective = str(best.get("objective") or _crypto_touch_policy_context(
            row,
            settings=settings,
            btc_1h_touch_policy=btc_1h_touch_allowed,
        )["objective"])
        selected_status = str(best.get("candidate_status") or "")
        selected_side_raw = str(best.get("side") or "yes")
        selected_side = ContractSide(selected_side_raw) if selected_side_raw in {"yes", "no"} else None
        touch_probability = _decimal(best.get("model_probability") or Decimal("0.5000"))
        trade_fair_yes = (
            touch_probability
            if selected_side == ContractSide.YES
            else Decimal("1.0000") - touch_probability
            if selected_side == ContractSide.NO
            else raw_fair_yes
        )
        target_raw = best.get("target_yes_price_dollars")
        target_yes = quantize_price(target_raw) if target_raw is not None else None
        edge_bps = int(best.get("edge_bps") or 0)
        live_quality = selected is not None and selected_side is not None and target_yes is not None
        return (
            TradeAction.BUY if live_quality else None,
            selected_side if live_quality else None,
            target_yes if live_quality else None,
            edge_bps,
            {
                "outcome": "candidate_selected" if live_quality else "touch_strategy_blocked",
                "fair_yes_dollars": _money_text(trade_fair_yes),
                "raw_fair_yes_dollars": _money_text(raw_fair_yes),
                "market_anchored_fair_yes_dollars": _money_text(trade_fair_yes),
                "market_price_anchor": {"enabled": False, "reason": "touch_strategy_uses_bracket_probability"},
                "raw_predicted_winner_side": selected_side_raw,
                "predicted_winner_side": selected_side_raw,
                "selected_side": selected_side.value if selected_side is not None else selected_side_raw,
                "selected_edge_bps": edge_bps,
                "candidate_status": selected_status,
                "selection_reason": best.get("reason"),
                "expected_net_edge": best.get("expected_net_edge"),
                "rank": best.get("rank"),
                "bucket_key": best.get("bucket_key"),
                "empirical_bucket_gate": best.get("empirical_bucket_gate"),
                "empirical_bucket_status": best.get("empirical_bucket_status"),
                "target_yes_price_dollars": _money_text(target_yes) if target_yes is not None else None,
                "min_edge_bps": entry_policy["min_fee_adjusted_edge_bps"],
                "max_spread_bps": entry_policy["max_spread_bps"],
                "spread_bps": market.spread_bps,
                "candidates": candidates,
                "gate_cascade": _crypto_candidate_gate_cascade(candidates, selected=selected if live_quality else None),
                "touch_strategy": best.get("touch_strategy"),
                "touch_replay_gate": (
                    {
                        "status": getattr(touch_replay_gate, "status", None),
                        "version": getattr(touch_replay_gate, "version", None),
                        "artifact_type": getattr(touch_replay_gate, "artifact_type", None),
                    }
                    if btc_1h_touch_allowed
                    else None
                ),
                "objective": objective,
                **_crypto_settlement_diagnostics(row),
            },
        )
    raw_fair_yes = _clamp_price(fair_yes)
    trade_fair_yes = _crypto_market_anchored_probability(row, raw_fair_yes, settings=settings)
    market_price_anchor = _crypto_market_price_anchor_trace(
        row,
        raw_fair_yes,
        trade_fair_yes,
        settings=settings,
    )
    candidates = _crypto_trade_candidates(
        row,
        raw_fair_yes,
        settings=settings,
        crypto_policy=crypto_policy,
        require_spot_features=require_spot_features,
        empirical_bucket_matrix=empirical_bucket_matrix,
        last_minute_passive_price_matrix=last_minute_passive_price_matrix,
        enforce_empirical_bucket_gate=enforce_empirical_bucket_gate,
        edge_shrinkage=edge_shrinkage,
    )
    entry_policy = _crypto_entry_policy_for_row(row, settings=settings, crypto_policy=crypto_policy)
    settlement_diagnostics = _crypto_settlement_diagnostics(row)
    raw_prediction_side = "yes" if raw_fair_yes >= Decimal("0.5000") else "no"
    prediction_side = "yes" if trade_fair_yes >= Decimal("0.5000") else "no"
    selected = next(
        (
            candidate
            for candidate in candidates
            if candidate.get("last_minute_passive_market_confidence") is True
            and candidate.get("candidate_status") == CRYPTO_LIVE_QUALITY
        ),
        None,
    )
    if selected is None:
        selected = _crypto_preferred_candidate_for_statuses(candidates, {CRYPTO_LIVE_QUALITY})
    if selected is None:
        selected = next((candidate for candidate in candidates if candidate.get("side") == prediction_side), None)
    if selected is None:
        edge_bps = max([int(candidate["edge_bps"]) for candidate in candidates if candidate["edge_bps"] is not None] or [0])
        return None, None, None, edge_bps, {
            "outcome": "no_candidate",
            "fair_yes_dollars": _money_text(trade_fair_yes),
            "raw_fair_yes_dollars": _money_text(raw_fair_yes),
            "market_anchored_fair_yes_dollars": _money_text(trade_fair_yes),
            "market_price_anchor": market_price_anchor,
            "raw_predicted_winner_side": raw_prediction_side,
            "predicted_winner_side": prediction_side,
            "min_edge_bps": entry_policy["min_fee_adjusted_edge_bps"],
            "max_spread_bps": entry_policy["max_spread_bps"],
            "spread_bps": market.spread_bps,
            "candidates": candidates,
            "gate_cascade": _crypto_candidate_gate_cascade(candidates),
            **settlement_diagnostics,
        }
    selected_status = str(selected.get("candidate_status") or "")
    side = ContractSide(str(selected.get("side") or prediction_side))
    target_raw = selected.get("target_yes_price_dollars")
    target_yes = quantize_price(target_raw) if target_raw is not None else None
    edge_bps = int(selected["edge_bps"] or 0)
    live_quality = selected_status == CRYPTO_LIVE_QUALITY and target_yes is not None
    return (TradeAction.BUY if live_quality else None), (side if live_quality else None), (target_yes if live_quality else None), edge_bps, {
        "outcome": "candidate_selected" if live_quality else "predicted_winner_blocked",
        "fair_yes_dollars": _money_text(trade_fair_yes),
        "raw_fair_yes_dollars": _money_text(raw_fair_yes),
        "market_anchored_fair_yes_dollars": _money_text(trade_fair_yes),
        "market_price_anchor": market_price_anchor,
        "raw_predicted_winner_side": raw_prediction_side,
        "predicted_winner_side": prediction_side,
        "selected_side": side.value,
        "selected_edge_bps": edge_bps,
        "candidate_status": selected_status,
        "selection_reason": selected.get("reason"),
        "pre_empirical_selection_reason": selected.get("pre_empirical_reason"),
        "expected_net_edge": selected.get("expected_net_edge"),
        "edge_shrinkage": selected.get("edge_shrinkage"),
        "shrunk_edge_bps": selected.get("shrunk_edge_bps"),
        "late_high_confidence_directional_entry": selected.get("late_high_confidence_directional_entry") is True,
        "last_minute_passive_market_confidence": selected.get("last_minute_passive_market_confidence") is True,
        "last_minute_passive": selected.get("last_minute_passive"),
        "last_minute_passive_bid_threshold_dollars": selected.get("last_minute_passive_bid_threshold_dollars"),
        "last_minute_price_source": selected.get("last_minute_price_source"),
        "last_minute_chosen_bid_dollars": selected.get("last_minute_chosen_bid_dollars"),
        "last_minute_fixed_fallback_bid_dollars": selected.get("last_minute_fixed_fallback_bid_dollars"),
        "last_minute_price_matrix_key": selected.get("last_minute_price_matrix_key"),
        "last_minute_price_matrix_base_key": selected.get("last_minute_price_matrix_base_key"),
        "last_minute_price_matrix_sample_count": selected.get("last_minute_price_matrix_sample_count"),
        "last_minute_price_matrix_fill_count": selected.get("last_minute_price_matrix_fill_count"),
        "last_minute_price_matrix_fill_rate": selected.get("last_minute_price_matrix_fill_rate"),
        "last_minute_price_matrix_net_pnl": selected.get("last_minute_price_matrix_net_pnl"),
        "last_minute_price_matrix_net_pnl_per_signal": selected.get("last_minute_price_matrix_net_pnl_per_signal"),
        "last_minute_passive_no_cross": selected.get("last_minute_passive_no_cross"),
        "rank": selected.get("rank"),
        "bucket_key": selected.get("bucket_key"),
        "empirical_bucket_gate": selected.get("empirical_bucket_gate"),
        "empirical_bucket_status": selected.get("empirical_bucket_status"),
        "empirical_bucket_late_override": selected.get("empirical_bucket_late_override"),
        "empirical_bucket_gap_sample": selected.get("empirical_bucket_gap_sample"),
        "target_yes_price_dollars": _money_text(target_yes) if target_yes is not None else None,
        "min_edge_bps": entry_policy["min_fee_adjusted_edge_bps"],
        "max_spread_bps": entry_policy["max_spread_bps"],
        "spread_bps": market.spread_bps,
        "candidates": candidates,
        "gate_cascade": _crypto_candidate_gate_cascade(candidates, selected=selected if live_quality else None),
        **settlement_diagnostics,
    }


def _crypto_candidate_gate_cascade(
    candidates: list[dict[str, Any]],
    *,
    selected: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cascade: list[dict[str, Any]] = []
    for candidate in candidates:
        reason = str(candidate.get("reason") or "")
        status = str(candidate.get("candidate_status") or "")
        outcome = "allow" if candidate is selected or status == CRYPTO_LIVE_QUALITY else "block"
        cascade.append(
            {
                "gate_name": "crypto_candidate_selector",
                "outcome": outcome,
                "gate_detail": {
                    "side": candidate.get("side"),
                    "candidate_status": status,
                    "reason": reason,
                    "spread_bps": candidate.get("spread_bps"),
                    "expected_net_edge": candidate.get("expected_net_edge"),
                    "live_eligible": candidate.get("live_eligible"),
                    "bucket_key": candidate.get("bucket_key"),
                    "empirical_bucket_status": candidate.get("empirical_bucket_status"),
                    "empirical_bucket_reason": (
                        candidate.get("empirical_bucket_gate") or {}
                    ).get("reason") if isinstance(candidate.get("empirical_bucket_gate"), dict) else None,
                    "empirical_bucket_override_reason": (
                        candidate.get("empirical_bucket_gate") or {}
                    ).get("override_reason") if isinstance(candidate.get("empirical_bucket_gate"), dict) else None,
                    "last_minute_passive_market_confidence": candidate.get("last_minute_passive_market_confidence") is True,
                    "last_minute_passive_reason": (
                        candidate.get("last_minute_passive") or {}
                    ).get("reason") if isinstance(candidate.get("last_minute_passive"), dict) else None,
                    "last_minute_price_source": candidate.get("last_minute_price_source"),
                    "last_minute_chosen_bid_dollars": candidate.get("last_minute_chosen_bid_dollars"),
                    "last_minute_price_matrix_key": candidate.get("last_minute_price_matrix_key"),
                },
            }
        )
    return cascade


def _signal_is_tradeable(signal: StrategySignal) -> bool:
    return (
        signal.recommended_action is not None
        and signal.recommended_side is not None
        and signal.target_yes_price_dollars is not None
        and signal.eligibility is not None
        and signal.eligibility.eligible
    )


def _crypto_signal_candidate_status(signal: StrategySignal) -> str | None:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    raw_status = selection.get("candidate_status") if isinstance(selection, dict) else None
    if raw_status in (None, ""):
        raw_status = trace.get("candidate_status")
    if raw_status in (None, ""):
        return None
    return str(raw_status)


def _crypto_signal_empirical_late_override_gate(signal: StrategySignal) -> dict[str, Any] | None:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    gate = trace.get("empirical_bucket_gate")
    if not isinstance(gate, dict):
        selection = trace.get("trade_selection_model")
        if isinstance(selection, dict):
            gate = selection.get("empirical_bucket_gate")
    if not isinstance(gate, dict):
        return None
    if gate.get("override_allowed") is True or gate.get("status") == "override_allowed":
        return gate
    late_override = gate.get("late_override")
    if isinstance(late_override, dict) and late_override.get("allowed") is True:
        return gate
    return None


def _crypto_ticket_unit_cost(ticket: TradeTicket) -> Decimal:
    if ticket.side == ContractSide.YES:
        return ticket.yes_price_dollars
    return Decimal("1.0000") - ticket.yes_price_dollars


def _crypto_signal_edge_per_contract_dollars(
    signal: StrategySignal,
    *,
    unit_cost: Decimal,
    settings: Settings,
) -> tuple[Decimal | None, str]:
    """Per-contract expected gross edge dollars for the selected candidate.

    Prefers the shrunk edge when an edge-shrinkage review is recorded on the
    candidate trace; otherwise reconstructs the raw edge as
    ``expected_net_edge + single-contract taker fee``.
    """
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    shrinkage = selection.get("edge_shrinkage")
    if not isinstance(shrinkage, dict):
        shrinkage = trace.get("edge_shrinkage")
    if isinstance(shrinkage, dict) and shrinkage.get("enforced") is True and shrinkage.get("shrunk_edge_dollars") not in (None, ""):
        try:
            return Decimal(str(shrinkage["shrunk_edge_dollars"])), "shrunk_edge"
        except (ArithmeticError, TypeError, ValueError):
            pass
    raw_net = selection.get("expected_net_edge")
    if raw_net in (None, ""):
        raw_net = trace.get("expected_net_edge")
    if raw_net in (None, ""):
        return None, "missing"
    try:
        net_edge = Decimal(str(raw_net))
    except (ArithmeticError, TypeError, ValueError):
        return None, "invalid"
    if unit_cost <= Decimal("0") or unit_cost >= Decimal("1"):
        return None, "invalid_unit_cost"
    single_contract_fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=unit_cost,
        count=Decimal("1.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    return net_edge + single_contract_fee, "raw_edge"


def _crypto_fee_to_edge_review(
    *,
    settings: Settings,
    unit_cost: Decimal,
    edge_per_contract_dollars: Decimal | None,
    edge_source: str,
    count_fp: Decimal,
    max_count_fp: Decimal,
) -> tuple[Decimal, dict[str, Any]]:
    """Enforce a maximum (rounded-up taker fee) / (expected gross edge) ratio.

    The cent ceiling on Kalshi fees is brutal at 1-2 contract sizes, so the
    floor first tries to amortize the rounded-up fee by growing the count
    toward ``max_count_fp``; if the caps prevent reaching the ratio the
    candidate is blocked with ``blocked_fee_ratio``.
    """
    max_ratio = Decimal(str(settings.crypto_max_fee_to_edge_ratio))
    review: dict[str, Any] = {
        "status": "ok",
        "max_ratio": float(max_ratio),
        "edge_source": edge_source,
        "edge_per_contract_dollars": _money_text(edge_per_contract_dollars),
        "initial_count_fp": _count_text(count_fp),
        "final_count_fp": _count_text(count_fp),
        "max_count_fp": _count_text(max_count_fp),
        "fee_dollars": None,
        "edge_dollars": None,
        "ratio": None,
    }
    if max_ratio <= Decimal("0"):
        review["status"] = "skipped_disabled"
        return count_fp, review
    if edge_per_contract_dollars is None or edge_per_contract_dollars <= Decimal("0"):
        review["status"] = "skipped_no_positive_edge"
        return count_fp, review
    if unit_cost <= Decimal("0") or unit_cost >= Decimal("1") or count_fp <= Decimal("0"):
        review["status"] = "skipped_unpriceable"
        return count_fp, review
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))

    def fee_at(count_value: Decimal) -> Decimal:
        return estimate_kalshi_taker_fee_dollars(
            price_dollars=unit_cost,
            count=count_value,
            fee_rate=fee_rate,
        )

    def ratio_ok(count_value: Decimal) -> bool:
        return fee_at(count_value) <= max_ratio * edge_per_contract_dollars * count_value

    def record(count_value: Decimal, status: str) -> tuple[Decimal, dict[str, Any]]:
        fee = fee_at(count_value)
        edge_dollars = (edge_per_contract_dollars * count_value).quantize(Decimal("0.0001"))
        review.update(
            {
                "status": status,
                "final_count_fp": _count_text(count_value),
                "fee_dollars": _money_text(fee),
                "edge_dollars": _money_text(edge_dollars),
                "ratio": float(fee / edge_dollars) if edge_dollars > 0 else None,
            }
        )
        return count_value, review

    if ratio_ok(count_fp):
        return record(count_fp, "ok")
    ceiling = max(count_fp, _floor_count_fp(max_count_fp))
    candidate = count_fp
    for _ in range(10_000):
        next_candidate = min(ceiling, _floor_count_fp(candidate) + Decimal("1.00"))
        if next_candidate <= candidate:
            break
        candidate = next_candidate
        if ratio_ok(candidate):
            return record(candidate, "adjusted")
    if candidate > count_fp and ratio_ok(candidate):
        return record(candidate, "adjusted")
    blocked_count, blocked_review = record(count_fp, "blocked_fee_ratio")
    return blocked_count, blocked_review


def _floor_count_fp(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def crypto_pnl_sizing_target_pct(metrics: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    """Convert nightly replay P&L into a per-market target position percent."""
    hard_max_pct = Decimal("0.20")
    configured_max_pct = Decimal(str(settings.crypto_dynamic_order_max_position_pct))
    max_pct = min(hard_max_pct, max(Decimal("0"), configured_max_pct))
    min_pct = min(
        max(Decimal("0"), Decimal(str(settings.crypto_dynamic_order_min_position_pct))),
        max_pct,
    )
    scale = max(
        Decimal("0.000001"),
        Decimal(str(settings.crypto_dynamic_order_pnl_scale_per_candidate_dollars)),
    )
    net_pl = Decimal(
        str(metrics.get("oos_net_simulated_pl_dollars", metrics.get("net_simulated_pl_dollars") or 0) or 0)
    )
    candidate_count = int(
        metrics.get(
            "oos_trade_candidate_count",
            metrics.get("current_model_live_quality_candidate_count", metrics.get("trade_candidate_count") or 0),
        )
        or 0
    )
    pnl_per_candidate = (net_pl / Decimal(candidate_count)) if candidate_count > 0 else Decimal("0")
    ratio = Decimal("0")
    if net_pl > Decimal("0") and candidate_count > 0:
        ratio = min(Decimal("1"), max(Decimal("0"), pnl_per_candidate / scale))
    target_pct = (min_pct + ((max_pct - min_pct) * ratio)).quantize(Decimal("0.000001"))
    spread_bps = int(
        (
            Decimal(CRYPTO_MIN_SPREAD_BPS)
            + ((Decimal(CRYPTO_MAX_SPREAD_BPS) - Decimal(CRYPTO_MIN_SPREAD_BPS)) * ratio)
        ).to_integral_value()
    )
    return {
        "target_position_pct": float(target_pct),
        "max_spread_bps": spread_bps,
        "diagnostics": {
            "net_simulated_pl_dollars": float(net_pl),
            "candidate_count": candidate_count,
            "pnl_per_candidate_dollars": float(pnl_per_candidate),
            "scale_per_candidate_dollars": float(scale),
            "ratio": float(ratio),
            "min_pct": float(min_pct),
            "max_pct": float(max_pct),
        },
    }


def _crypto_dynamic_order_count_fp(
    *,
    settings: Settings,
    ticket: TradeTicket,
    signal: StrategySignal,
    context: RiskContext,
) -> tuple[Decimal, dict[str, Any]]:
    configured_default_count = quantize_count(Decimal(str(settings.crypto_default_order_count_fp)))
    candidate_status = _crypto_signal_candidate_status(signal)
    unit_cost = _crypto_ticket_unit_cost(ticket)
    late_override_gate = _crypto_signal_empirical_late_override_gate(signal)
    late_override_cap = (
        quantize_count(Decimal(str(settings.crypto_empirical_late_override_max_count_fp)))
        if late_override_gate is not None
        else None
    )
    default_cap_candidates = [configured_default_count]
    if late_override_cap is not None and late_override_cap > Decimal("0"):
        default_cap_candidates.append(late_override_cap)
    default_count = min(default_cap_candidates)
    requested_count = default_count
    target_pct = Decimal(str(settings.crypto_dynamic_order_target_position_pct))
    risk_pct = Decimal(str(settings.risk_position_pct))
    effective_target_pct = min(target_pct, risk_pct)
    max_order_count = Decimal(str(settings.risk_max_order_count_fp))
    max_position_count = Decimal(str(settings.risk_max_position_count_fp_per_ticker))
    current_position_count = Decimal(str(context.current_position_count_fp or Decimal("0")))
    pending_order_count = Decimal(str(context.pending_order_count_fp or Decimal("0")))
    remaining_position_count = max_position_count - current_position_count - pending_order_count
    current_notional = Decimal(str(context.current_position_notional_dollars or Decimal("0")))
    pending_notional = Decimal(str(context.pending_order_notional_dollars or Decimal("0")))
    total_capital = context.total_capital_dollars

    diagnostics: dict[str, Any] = {
        "enabled": bool(settings.crypto_dynamic_order_sizing_enabled),
        "scope": str(settings.crypto_dynamic_order_sizing_scope or ""),
        "mode": "default",
        "reason": None,
        "candidate_status": candidate_status,
        "configured_default_count_fp": _count_text(configured_default_count),
        "default_count_fp": _count_text(default_count),
        "requested_count_fp": _count_text(requested_count),
        "empirical_bucket_late_override_cap_active": late_override_gate is not None,
        "empirical_bucket_late_override_max_count_fp": _count_text(late_override_cap),
        "empirical_bucket_late_override_reason": late_override_gate.get("override_reason") if late_override_gate else None,
        "unit_cost_dollars": _money_text(unit_cost),
        "target_position_pct": float(target_pct),
        "risk_position_pct": float(risk_pct),
        "effective_target_position_pct": float(effective_target_pct),
        "total_capital_dollars": _money_text(total_capital),
        "target_notional_dollars": None,
        "available_notional_dollars": None,
        "current_position_notional_dollars": _money_text(current_notional),
        "pending_order_notional_dollars": _money_text(pending_notional),
        "current_position_count_fp": _count_text(current_position_count),
        "pending_order_count_fp": _count_text(pending_order_count),
        "risk_max_order_count_fp": _count_text(max_order_count),
        "risk_max_position_count_fp_per_ticker": _count_text(max_position_count),
        "remaining_position_count_fp": _count_text(remaining_position_count),
        "raw_count_fp": None,
        "capped_count_fp": None,
        "fee_to_edge": None,
    }

    edge_per_contract, edge_source = _crypto_signal_edge_per_contract_dollars(
        signal,
        unit_cost=unit_cost,
        settings=settings,
    )

    def apply_fee_to_edge_floor(count_value: Decimal, *, budget_cap_fp: Decimal | None = None) -> Decimal:
        if candidate_status != CRYPTO_LIVE_QUALITY:
            diagnostics["fee_to_edge"] = {
                "status": "skipped_not_live_quality",
                "candidate_status": candidate_status,
            }
            return count_value
        cap_values = [_floor_count_fp(max_order_count), _floor_count_fp(max(Decimal("0"), remaining_position_count))]
        if late_override_cap is not None and late_override_cap > Decimal("0"):
            cap_values.append(_floor_count_fp(late_override_cap))
        if budget_cap_fp is not None:
            cap_values.append(_floor_count_fp(budget_cap_fp))
        final_count, review = _crypto_fee_to_edge_review(
            settings=settings,
            unit_cost=unit_cost,
            edge_per_contract_dollars=edge_per_contract,
            edge_source=edge_source,
            count_fp=count_value,
            max_count_fp=min(cap_values),
        )
        diagnostics["fee_to_edge"] = review
        if review["status"] == "adjusted":
            diagnostics["requested_count_fp"] = _count_text(final_count)
        return final_count

    def use_default(reason: str) -> tuple[Decimal, dict[str, Any]]:
        diagnostics["reason"] = reason
        diagnostics["requested_count_fp"] = _count_text(default_count)
        return apply_fee_to_edge_floor(default_count), diagnostics

    if not settings.crypto_dynamic_order_sizing_enabled:
        return use_default("dynamic_sizing_disabled")

    scope = str(settings.crypto_dynamic_order_sizing_scope or "").strip().lower()
    if scope != "live_quality":
        return use_default("unsupported_dynamic_sizing_scope")
    if candidate_status != CRYPTO_LIVE_QUALITY:
        return use_default("candidate_not_live_quality")
    if total_capital is None:
        return use_default("missing_total_capital")
    total_capital_dec = Decimal(str(total_capital))
    if total_capital_dec <= Decimal("0"):
        return use_default("non_positive_total_capital")
    if effective_target_pct <= Decimal("0"):
        return use_default("non_positive_target_position_pct")
    if unit_cost <= Decimal("0"):
        return use_default("non_positive_unit_cost")

    target_notional = (total_capital_dec * effective_target_pct).quantize(Decimal("0.0001"))
    available_notional = (target_notional - current_notional - pending_notional).quantize(Decimal("0.0001"))
    raw_count = _floor_count_fp(available_notional / unit_cost) if available_notional > Decimal("0") else Decimal("0.00")
    cap_candidates = [raw_count, _floor_count_fp(max_order_count), _floor_count_fp(remaining_position_count)]
    if late_override_cap is not None:
        cap_candidates.append(_floor_count_fp(late_override_cap))
    capped_count = min(cap_candidates)
    capped_count = max(Decimal("0.00"), _floor_count_fp(capped_count))
    diagnostics.update(
        {
            "target_notional_dollars": _money_text(target_notional),
            "available_notional_dollars": _money_text(available_notional),
            "raw_count_fp": _count_text(raw_count),
            "capped_count_fp": _count_text(capped_count),
        }
    )
    if capped_count < default_count:
        return use_default("dynamic_count_below_default")

    requested_count = quantize_count(capped_count)
    diagnostics["mode"] = "dynamic"
    diagnostics["reason"] = "target_position_budget"
    diagnostics["requested_count_fp"] = _count_text(requested_count)
    requested_count = quantize_count(apply_fee_to_edge_floor(requested_count, budget_cap_fp=raw_count))
    return requested_count, diagnostics


def _crypto_data_quality(
    snapshots: list[CryptoMarketSnapshotRecord],
    candles: list[CryptoMarketCandlestickRecord],
    *,
    min_training_samples: int,
) -> dict[str, Any]:
    assets = sorted({row.asset_symbol for row in snapshots} | {row.asset_symbol for row in candles})
    candle_markets = {row.market_ticker for row in candles}
    by_asset: dict[str, dict[str, Any]] = {}
    for asset in assets:
        asset_snapshots = [row for row in snapshots if row.asset_symbol == asset]
        asset_candles = [row for row in candles if row.asset_symbol == asset]
        settled = [row for row in asset_snapshots if row.settlement_result in {"yes", "no"}]
        snapshot_markets = {row.market_ticker for row in asset_snapshots}
        latest_observed = max((row.observed_at for row in asset_snapshots), default=None)
        latest_candle = max((row.end_period_ts for row in asset_candles), default=None)
        by_asset[asset] = {
            "snapshot_count": len(asset_snapshots),
            "settled_snapshot_count": len(settled),
            "candle_count": len(asset_candles),
            "market_count": len(snapshot_markets),
            "markets_missing_candles": len(snapshot_markets - candle_markets),
            "latest_observed_at": latest_observed.isoformat() if latest_observed else None,
            "latest_candle_at": latest_candle.isoformat() if latest_candle else None,
        }
    settled_snapshot_count = sum(1 for row in snapshots if row.settlement_result in {"yes", "no"})
    status = "ready" if settled_snapshot_count >= min_training_samples and candles else "needs_data"
    return {
        "status": status,
        "snapshot_count": len(snapshots),
        "settled_snapshot_count": settled_snapshot_count,
        "unresolved_snapshot_count": len(snapshots) - settled_snapshot_count,
        "candle_count": len(candles),
        "asset_count": len(assets),
        "assets": by_asset,
        "source_kind_counts": dict(Counter(row.source_kind for row in snapshots)),
    }


def _crypto_lightweight_settled_data_quality(
    *,
    expected_assets: list[str],
    asset_counts: Counter[str],
    settled_snapshot_count: int,
    candle_count: int,
) -> dict[str, Any]:
    return {
        "status": "skipped",
        "reason": "quality_summary_disabled",
        "settled_snapshot_count": settled_snapshot_count,
        "candle_count": candle_count,
        "asset_count": len(expected_assets),
        "assets": {
            asset: {"settled_snapshot_count": int(asset_counts.get(asset) or 0)}
            for asset in expected_assets
        },
    }


def _crypto_feature_row_data_quality(
    rows: list[dict[str, Any]],
    *,
    min_training_samples: int,
) -> dict[str, Any]:
    assets = sorted({normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")) for row in rows})
    by_asset: dict[str, dict[str, Any]] = {}
    for asset in assets:
        asset_rows = [row for row in rows if normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")) == asset]
        latest_decision = max((_as_utc_datetime(row["decision_ts"]) for row in asset_rows if row.get("decision_ts")), default=None)
        latest_settlement = max((_as_utc_datetime(row["settlement_ts"]) for row in asset_rows if row.get("settlement_ts")), default=None)
        by_asset[asset] = {
            "feature_row_count": len(asset_rows),
            "strict_trade_eligible_rows": sum(1 for row in asset_rows if row.get("strict_trade_eligible")),
            "resolved_label_count": sum(1 for row in asset_rows if row.get("label_yes") in {0, 1}),
            "latest_decision_ts": latest_decision.isoformat() if latest_decision else None,
            "latest_settlement_ts": latest_settlement.isoformat() if latest_settlement else None,
        }
    candle_feature_count = sum(
        1
        for row in rows
        if "candlestick" in str(row.get("source_kind") or "")
        or str(row.get("spot_source_kind") or "") == "spot_ohlc"
        or int(row.get("candle_count") or 0) > 0
        or row.get("candle_momentum_dollars") is not None
        or row.get("candle_momentum") is not None
        or row.get("market_mid_change_1") is not None
    )
    return {
        "status": "ready" if len(rows) >= min_training_samples else "needs_data",
        "source": "crypto_training_feature_rows",
        "feature_row_count": len(rows),
        "strict_trade_eligible_rows": sum(1 for row in rows if row.get("strict_trade_eligible")),
        "resolved_label_count": sum(1 for row in rows if row.get("label_yes") in {0, 1}),
        "snapshot_count": 0,
        "settled_snapshot_count": len(rows),
        "unresolved_snapshot_count": 0,
        "candle_count": candle_feature_count,
        "asset_count": len(assets),
        "assets": by_asset,
        "source_kind_counts": dict(Counter(str(row.get("source_kind") or "") for row in rows)),
    }


def _crypto_spot_quality(
    rows: list[CryptoSpotOHLCRecord],
    *,
    expected_assets: list[str],
    min_coverage_pct: float,
    settings: Settings | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    expected = sorted({normalize_asset_symbol(asset) for asset in expected_assets})
    assets_with_rows = sorted({row.asset_symbol for row in rows})
    by_asset: dict[str, dict[str, Any]] = {}
    for asset in sorted(set(expected) | set(assets_with_rows)):
        asset_rows = [row for row in rows if row.asset_symbol == asset]
        latest = max((_as_utc_datetime(row.end_ts) for row in asset_rows), default=None)
        providers = dict(Counter(row.provider for row in asset_rows))
        source_kinds = dict(Counter(row.source_kind for row in asset_rows))
        latest_row = max(asset_rows, key=lambda row: _as_utc_datetime(row.end_ts), default=None)
        freshness_limit = (
            _crypto_spot_max_stale_seconds(latest_row.provider, latest_row.source_kind, settings=settings)
            if latest_row is not None
            else None
        )
        proxy_only = bool(latest_row is not None and _crypto_spot_is_proxy(latest_row.provider, latest_row.source_kind))
        by_asset[asset] = {
            "row_count": len(asset_rows),
            "provider_counts": providers,
            "source_kind_counts": source_kinds,
            "latest_end_ts": latest.isoformat() if latest else None,
            "stale_seconds": int((now - latest).total_seconds()) if latest else None,
            "freshness_limit_seconds": freshness_limit,
            "proxy_only": proxy_only,
        }
    coverage = (len([asset for asset in expected if asset in assets_with_rows]) / len(expected)) if expected else 0.0
    stale_assets = [
        asset
        for asset, summary in by_asset.items()
        if summary["latest_end_ts"] is None
        or int(summary["stale_seconds"] or 0) > int(summary["freshness_limit_seconds"] or 0)
    ]
    status = "ready" if coverage >= min_coverage_pct and not [asset for asset in expected if asset in stale_assets] else "needs_data"
    return {
        "status": status,
        "row_count": len(rows),
        "asset_count": len(assets_with_rows),
        "expected_assets": expected,
        "covered_assets": assets_with_rows,
        "missing_assets": sorted(set(expected) - set(assets_with_rows)),
        "coverage_pct": _ratio(coverage),
        "min_coverage_pct": _ratio(min_coverage_pct),
        "stale_assets": stale_assets,
        "provider_counts": dict(Counter(row.provider for row in rows)),
        "source_kind_counts": dict(Counter(row.source_kind for row in rows)),
        "assets": by_asset,
    }


def _crypto_spot_quality_from_feature_rows(
    rows: list[dict[str, Any]],
    *,
    expected_assets: list[str],
    min_coverage_pct: float,
) -> dict[str, Any]:
    expected = sorted({normalize_asset_symbol(asset) for asset in expected_assets})
    observed = sorted({normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")) for row in rows})
    by_asset: dict[str, dict[str, Any]] = {}
    for asset in sorted(set(expected) | set(observed)):
        asset_rows = [row for row in rows if normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")) == asset]
        available = [row for row in asset_rows if row.get("spot_feature_status") == "available"]
        latest = max((_as_utc_datetime(row["spot_observed_end_ts"]) for row in asset_rows if row.get("spot_observed_end_ts")), default=None)
        by_asset[asset] = {
            "row_count": len(asset_rows),
            "feature_coverage_pct": _ratio(len(available) / len(asset_rows)) if asset_rows else 0.0,
            "latest_end_ts": latest.isoformat() if latest else None,
            "provider_counts": dict(Counter(str(row.get("spot_provider") or "unknown") for row in asset_rows)),
            "source_kind_counts": dict(Counter(str(row.get("spot_source_kind") or "unknown") for row in asset_rows)),
            "proxy_only_rows": sum(1 for row in asset_rows if row.get("spot_proxy_only")),
        }
    coverage = _spot_feature_coverage(rows)
    covered_assets = [
        asset
        for asset, summary in by_asset.items()
        if float(summary.get("feature_coverage_pct") or 0.0) > 0.0
    ]
    return {
        "status": "ready" if coverage >= min_coverage_pct else "needs_data",
        "source": "crypto_training_feature_rows",
        "row_count": len(rows),
        "asset_count": len(observed),
        "expected_assets": expected,
        "covered_assets": sorted(covered_assets),
        "missing_assets": sorted(set(expected) - set(covered_assets)),
        "coverage_pct": _ratio(coverage),
        "min_coverage_pct": _ratio(min_coverage_pct),
        "stale_assets": [],
        "provider_counts": dict(Counter(str(row.get("spot_provider") or "unknown") for row in rows)),
        "source_kind_counts": dict(Counter(str(row.get("spot_source_kind") or "unknown") for row in rows)),
        "assets": by_asset,
    }


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _datetime_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc_datetime(value).isoformat()


def _crypto_market_closed_for_execution(market: CryptoMarket, *, now: datetime | None = None) -> bool:
    close_time = market.close_time or market.expected_expiration_time
    if close_time is None:
        return False
    current = _as_utc_datetime(now or datetime.now(UTC))
    return _as_utc_datetime(close_time) <= current


async def _crypto_shadow_evidence_counts(
    session: AsyncSession,
    *,
    kalshi_env: str,
    market_tickers: set[str],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=7)

    async def count(stmt: Any) -> int:
        return int((await session.execute(stmt)).scalar_one() or 0)

    ticker_filter = list(market_tickers)
    room_stmt = select(func.count(Room.id)).where(Room.kalshi_env == kalshi_env, Room.created_at >= recent_cutoff)
    signal_stmt = select(func.count(Signal.id)).where(Signal.created_at >= recent_cutoff)
    if ticker_filter:
        room_stmt = room_stmt.where(Room.market_ticker.in_(ticker_filter))
        signal_stmt = signal_stmt.where(Signal.market_ticker.in_(ticker_filter))
    crypto_strategy_codes = list(CRYPTO_STRATEGY_CODES.values())
    ticket_stmt = select(func.count(TradeTicketRecord.id)).where(
        TradeTicketRecord.strategy_code.in_(crypto_strategy_codes),
        TradeTicketRecord.created_at >= recent_cutoff,
    )
    risk_stmt = (
        select(func.count(RiskVerdictRecord.id))
        .join(TradeTicketRecord, RiskVerdictRecord.ticket_id == TradeTicketRecord.id)
        .where(
            TradeTicketRecord.strategy_code.in_(crypto_strategy_codes),
            RiskVerdictRecord.created_at >= recent_cutoff,
        )
    )
    shadow_receipt_stmt = select(func.count(RoomMessage.id)).where(
        RoomMessage.kind == MessageKind.EXEC_RECEIPT.value,
        RoomMessage.created_at >= recent_cutoff,
        RoomMessage.content.ilike("%shadow_skipped%"),
    )
    live_order_stmt = select(func.count(OrderRecord.id)).where(
        OrderRecord.strategy_code.in_(crypto_strategy_codes),
        OrderRecord.kalshi_env == kalshi_env,
    )
    if ticker_filter:
        ticket_stmt = ticket_stmt.where(TradeTicketRecord.market_ticker.in_(ticker_filter))
        risk_stmt = risk_stmt.where(TradeTicketRecord.market_ticker.in_(ticker_filter))
        live_order_stmt = live_order_stmt.where(OrderRecord.market_ticker.in_(ticker_filter))
    recent_live_order_stmt = live_order_stmt.where(OrderRecord.created_at >= recent_cutoff)
    return {
        "window_days": 7,
        "recent_rooms": await count(room_stmt),
        "recent_signals": await count(signal_stmt),
        "recent_trade_tickets": await count(ticket_stmt),
        "recent_risk_verdicts": await count(risk_stmt),
        "recent_shadow_skipped_receipts": await count(shadow_receipt_stmt),
        "live_order_count": await count(live_order_stmt),
        "recent_live_order_count": await count(recent_live_order_stmt),
    }


def _crypto_quote_evidence_summary(
    snapshots: list[CryptoMarketSnapshotRecord],
    decision_rows: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    def _day_key(value: Any) -> str | None:
        if isinstance(value, datetime):
            return _as_utc_datetime(value).date().isoformat()
        if isinstance(value, str):
            parsed = parse_datetime(value)
            if parsed is not None:
                return _as_utc_datetime(parsed).date().isoformat()
            raw = value.strip()
            return raw[:10] if raw else None
        return None

    real_snapshots = [
        row
        for row in snapshots
        if _snapshot_price(row, attr="yes_bid_dollars", dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",)) is not None
        and _snapshot_price(row, attr="yes_ask_dollars", dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",)) is not None
    ]
    labeled_real_rows = [
        row
        for row in decision_rows
        if row.get("strict_trade_eligible") and row.get("label_yes") in {0, 1}
    ]
    real_quote_rows = [row for row in decision_rows if row.get("quote_source") == "snapshot_quotes"]
    strict_trade_rows = [row for row in decision_rows if row.get("strict_trade_eligible")]
    proxy_rows = [row for row in decision_rows if row.get("quote_source") != "snapshot_quotes"]
    candidates_by_asset: dict[str, dict[str, Any]] = {}
    strict_days_by_asset: dict[str, dict[str, set[str]]] = {}
    for row in decision_rows:
        asset = normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
        summary = candidates_by_asset.setdefault(
            asset,
            {
                "real_quote_rows": 0,
                "labeled_real_quote_rows": 0,
                "proxy_rows": 0,
                "strict_trade_eligible_rows": 0,
                "prediction_only_rows": 0,
            },
        )
        days = strict_days_by_asset.setdefault(
            asset,
            {
                "market_days": set(),
                "settlement_days": set(),
            },
        )
        if row.get("quote_source") == "snapshot_quotes":
            summary["real_quote_rows"] += 1
        else:
            summary["proxy_rows"] += 1
            summary["prediction_only_rows"] += 1
        if row.get("strict_trade_eligible"):
            summary["strict_trade_eligible_rows"] += 1
            market_day = _day_key(row.get("market_day")) or _day_key(row.get("decision_ts"))
            settlement_day = _day_key(row.get("settlement_ts"))
            if market_day:
                days["market_days"].add(market_day)
            if settlement_day:
                days["settlement_days"].add(settlement_day)
            if row.get("label_yes") in {0, 1}:
                summary["labeled_real_quote_rows"] += 1
    for asset, days in strict_days_by_asset.items():
        summary = candidates_by_asset.setdefault(
            asset,
            {
                "real_quote_rows": 0,
                "labeled_real_quote_rows": 0,
                "proxy_rows": 0,
                "strict_trade_eligible_rows": 0,
                "prediction_only_rows": 0,
            },
        )
        market_days = sorted(days["market_days"])
        settlement_days = sorted(days["settlement_days"])
        summary["strict_market_day_count"] = len(market_days)
        summary["strict_market_days"] = market_days
        summary["strict_settlement_day_count"] = len(settlement_days)
        summary["strict_settlement_days"] = settlement_days
    strict_quote_ingestion_audit: dict[str, dict[str, Any]] = {}
    for asset in sorted({row.asset_symbol for row in snapshots} | {str(row.get("asset_symbol") or "UNKNOWN") for row in decision_rows}):
        asset_snapshots = [row for row in snapshots if normalize_asset_symbol(row.asset_symbol) == normalize_asset_symbol(asset)]
        asset_decisions = [row for row in decision_rows if normalize_asset_symbol(str(row.get("asset_symbol") or "")) == normalize_asset_symbol(asset)]
        strict_asset_decisions = [row for row in asset_decisions if row.get("strict_trade_eligible")]
        strict_market_days = sorted(
            {
                day
                for row in strict_asset_decisions
                if (day := (_day_key(row.get("market_day")) or _day_key(row.get("decision_ts"))))
            }
        )
        strict_settlement_days = sorted(
            {
                day
                for row in strict_asset_decisions
                if (day := _day_key(row.get("settlement_ts")))
            }
        )
        candidate_generated = 0
        eligible_candidate_generated = 0
        if settings is not None:
            for row in asset_decisions:
                if row.get("label_yes") not in {0, 1}:
                    continue
                candidates = _crypto_trade_candidates(row, _decimal(row.get("mid_yes_dollars") or Decimal("0.5000")), settings=settings)
                if candidates:
                    candidate_generated += 1
                if any(candidate.get("status") == "eligible" for candidate in candidates):
                    eligible_candidate_generated += 1
        counts = {
            "snapshot_present": len(asset_snapshots),
            "real_bid_ask_present": sum(
                1
                for row in asset_snapshots
                if _snapshot_price(row, attr="yes_bid_dollars", dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",)) is not None
                and _snapshot_price(row, attr="yes_ask_dollars", dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",)) is not None
            ),
            "settled_label_joined": sum(1 for row in asset_decisions if row.get("label_yes") in {0, 1}),
            "point_in_time_rows": sum(1 for row in asset_decisions if row.get("leakage_status") == "point_in_time"),
            "spot_joined": sum(1 for row in asset_decisions if row.get("spot_feature_status") == "available"),
            "spot_stale_blocked": sum(1 for row in asset_decisions if row.get("spot_feature_status") == "stale"),
            "spot_proxy_only": sum(
                1
                for row in asset_decisions
                if bool(row.get("spot_proxy_only"))
                or _crypto_spot_is_proxy(row.get("spot_provider"), row.get("spot_source_kind"))
            ),
            "strict_trade_eligible": sum(1 for row in asset_decisions if row.get("strict_trade_eligible")),
            "strict_market_day_count": len(strict_market_days),
            "strict_market_days": strict_market_days,
            "strict_settlement_day_count": len(strict_settlement_days),
            "strict_settlement_days": strict_settlement_days,
            "candidate_generated": candidate_generated,
            "eligible_candidate_generated": eligible_candidate_generated,
        }
        strict_quote_ingestion_audit[normalize_asset_symbol(asset)] = {
            **counts,
            "blocker_stage": _crypto_strict_quote_blocker_stage(counts),
        }
    return {
        "real_quote_snapshot_count": len(real_snapshots),
        "real_quote_decision_rows": len(real_quote_rows),
        "labeled_real_quote_rows": len(labeled_real_rows),
        "strict_trade_eligible_count": len(strict_trade_rows),
        "proxy_row_count": len(proxy_rows),
        "prediction_only_proxy_row_count": len(proxy_rows),
        "trade_candidate_support_by_asset": dict(sorted(candidates_by_asset.items())),
        "strict_quote_ingestion_audit_by_asset": strict_quote_ingestion_audit,
        "assets_missing_settled_markets": _crypto_assets_missing_settled_markets(snapshots),
        "source_kind_counts": dict(Counter(row.source_kind for row in snapshots)),
        "assets_with_real_quotes": sorted({row.asset_symbol for row in real_snapshots}),
    }


def _crypto_assets_missing_settled_markets(
    snapshots: list[CryptoMarketSnapshotRecord],
    *,
    expected_assets: list[str] | None = None,
) -> list[str]:
    assets = sorted({normalize_asset_symbol(asset) for asset in (expected_assets or [])} | {row.asset_symbol for row in snapshots})
    missing: list[str] = []
    for asset in assets:
        asset_snapshots = [row for row in snapshots if normalize_asset_symbol(row.asset_symbol) == asset]
        raw_snapshots = [row for row in asset_snapshots if row.source_kind != "settled_backfill"]
        settled_snapshots = [row for row in asset_snapshots if row.settlement_result in {"yes", "no"}]
        if raw_snapshots and not settled_snapshots:
            missing.append(asset)
    return missing


def _crypto_strict_quote_blocker_stage(counts: dict[str, Any]) -> str:
    if int(counts.get("snapshot_present") or 0) <= 0:
        return "missing_snapshot"
    if int(counts.get("real_bid_ask_present") or 0) <= 0:
        return "missing_real_bid_ask"
    if int(counts.get("settled_label_joined") or 0) <= 0:
        return "missing_settled_label"
    if int(counts.get("point_in_time_rows") or 0) <= 0:
        return "missing_point_in_time_row"
    if int(counts.get("spot_joined") or 0) <= 0:
        return "missing_spot_join"
    if int(counts.get("strict_trade_eligible") or 0) <= 0:
        return "missing_strict_trade_eligible"
    if int(counts.get("eligible_candidate_generated") or 0) <= 0:
        if int(counts.get("spot_proxy_only") or 0) >= int(counts.get("spot_joined") or 0):
            return "spot_source_proxy_only"
        return "candidate_generation_blocked"
    return "candidate_generated"


def _crypto_readiness_score(
    *,
    settings: Settings,
    data_quality: dict[str, Any],
    spot_quality: dict[str, Any],
    shadow_evidence: dict[str, Any],
    model: dict[str, Any],
    backtest: dict[str, Any],
    gate: dict[str, Any],
    global_live_blockers: list[str],
    active_color: str,
) -> dict[str, Any]:
    del global_live_blockers
    is_active_color = settings.app_color == active_color
    windowed_live_orders = int(shadow_evidence.get("recent_live_order_count") or 0)
    applicable_live_orders = windowed_live_orders if is_active_color else 0
    effectively_trading = settings.crypto_trading_enabled and is_active_color
    safety = 10 if not effectively_trading and applicable_live_orders == 0 else 0
    data_freshness = 8 if data_quality.get("status") == "ready" else 4
    spot_coverage = float(spot_quality.get("coverage_pct") or 0.0)
    feature_coverage = int(round(min(1.0, max(0.0, spot_coverage)) * 10))
    if spot_quality.get("status") == "needs_data":
        feature_coverage = min(feature_coverage, 7)
    shadow_complete = all(
        int(shadow_evidence.get(key) or 0) > 0
        for key in ("recent_rooms", "recent_trade_tickets", "recent_risk_verdicts", "recent_shadow_skipped_receipts")
    )
    shadow_score = 10 if shadow_complete else (5 if int(shadow_evidence.get("recent_rooms") or 0) > 0 else 0)
    backtest_metrics = backtest.get("metrics") or {}
    metric_pairs = [
        ("calibration_brier", "market_mid_brier"),
        ("calibration_log_loss", "market_mid_log_loss"),
        ("calibration_ece", "market_mid_ece"),
    ]
    improved = 0
    comparable = 0
    for current_key, baseline_key in metric_pairs:
        current = backtest_metrics.get(current_key)
        baseline = backtest_metrics.get(baseline_key)
        if current is None or baseline is None:
            continue
        comparable += 1
        if float(current) < float(baseline):
            improved += 1
    model_oos = int(round((improved / comparable) * 10)) if comparable else (5 if model.get("status") == "trained" else 2)
    net_pnl = float(backtest_metrics.get("net_simulated_pl_dollars") or 0.0)
    strict_candidates = int(
        backtest_metrics.get("current_model_live_quality_candidate_count", backtest_metrics.get("trade_candidate_count")) or 0
    )
    replay_pnl = 10 if net_pnl > 0 and strict_candidates >= settings.crypto_replay_min_trade_candidates else (6 if net_pnl > 0 else 3)
    gates = 10 if gate.get("status") == "passed" else (5 if gate.get("status") == "blocked" else 2)
    components = {
        "safety": safety,
        "data_freshness": data_freshness,
        "feature_coverage": feature_coverage,
        "shadow_evidence": shadow_score,
        "model_oos_metrics": model_oos,
        "replay_pnl": replay_pnl,
        "promotion_gates": gates,
    }
    score = round(sum(components.values()) / len(components), 1)
    blockers: list[str] = []
    if applicable_live_orders:
        blockers.append("crypto_live_orders_detected")
    if spot_quality.get("status") != "ready":
        blockers.append("spot_feature_coverage_or_freshness_needs_work")
    if not shadow_complete:
        blockers.append("shadow_ticket_risk_receipt_evidence_missing")
    if model_oos < 10:
        blockers.append("model_oos_metrics_do_not_all_beat_market_mid")
    if gate.get("status") != "passed":
        blockers.append("strict_replay_gate_blocked")
    return {
        "schema_version": "crypto-readiness-score-v1",
        "target": "shadow_ready_8_of_10",
        "score": score,
        "components": components,
        "shadow_ready": score >= 8.0 and safety == 10 and shadow_complete,
        "live_ready": False,
        "blockers": blockers,
    }


def _crypto_quote_sequence_context(
    current_mid: Decimal | None,
    current_spread_bps: int | float | None,
    current_observed_at: datetime | None,
    prior_mid: Decimal | None,
    prior_spread_bps: int | float | None,
    prior_observed_at: datetime | None,
) -> dict[str, Any]:
    """Quote-to-quote dynamics for the same market: mid change, velocity,
    spread change, and observation gap. Empty when there is no prior quote."""
    if current_mid is None or prior_mid is None or current_observed_at is None or prior_observed_at is None:
        return {}
    gap_seconds = (_as_utc_datetime(current_observed_at) - _as_utc_datetime(prior_observed_at)).total_seconds()
    if gap_seconds <= 0:
        return {}
    change = current_mid - prior_mid
    context: dict[str, Any] = {
        "market_mid_change_1": change,
        "market_mid_velocity_per_min": change / Decimal(str(gap_seconds / 60.0)),
        "quote_observation_gap_seconds": gap_seconds,
    }
    if current_spread_bps is not None and prior_spread_bps is not None:
        context["spread_change_bps_1"] = float(current_spread_bps) - float(prior_spread_bps)
    return context


def _crypto_snapshot_spread_bps(snapshot: Any, yes_bid: Decimal | None, yes_ask: Decimal | None) -> float | None:
    recorded = getattr(snapshot, "spread_bps", None)
    if recorded is not None:
        return float(recorded)
    # Market snapshots have no spread_bps column (only order-book snapshots do):
    # derive from the snapshot's own quotes when the caller didn't extract them,
    # otherwise prior-quote spread is always None and spread_change_bps_1 is dead.
    if yes_bid is None and snapshot is not None:
        yes_bid = _snapshot_price(snapshot, attr="yes_bid_dollars", dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",))
    if yes_ask is None and snapshot is not None:
        yes_ask = _snapshot_price(snapshot, attr="yes_ask_dollars", dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",))
    if yes_bid is None or yes_ask is None:
        return None
    return float((yes_ask - yes_bid) * Decimal("10000"))


def _crypto_decision_rows(
    snapshots: list[CryptoMarketSnapshotRecord],
    candles: list[CryptoMarketCandlestickRecord],
    spot_rows: list[CryptoSpotOHLCRecord] | None = None,
    *,
    funding_rate_rows: list[CryptoFundingRateRecord] | None = None,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    candles_by_market: dict[str, list[CryptoMarketCandlestickRecord]] = defaultdict(list)
    for candle in candles:
        candles_by_market[candle.market_ticker].append(candle)
    for market_candles in candles_by_market.values():
        market_candles.sort(key=lambda row: row.end_period_ts)
    spot_by_asset: dict[str, list[CryptoSpotOHLCRecord]] = defaultdict(list)
    for row in spot_rows or []:
        if row.close_dollars is None:
            continue
        spot_by_asset[row.asset_symbol].append(row)
    for asset_rows in spot_by_asset.values():
        asset_rows.sort(key=lambda row: row.end_ts)
    funding_by_asset: dict[str, list[CryptoFundingRateRecord]] = defaultdict(list)
    for rate in funding_rate_rows or []:
        funding_by_asset[rate.asset_symbol].append(rate)
    # already oldest-first from list_crypto_funding_rates_bulk; sort to be safe
    for asset_rates in funding_by_asset.values():
        asset_rates.sort(key=lambda r: r.settlement_ts)
    spot_end_times_by_asset = {
        asset: [_as_utc_datetime(row.end_ts) for row in asset_rows]
        for asset, asset_rows in spot_by_asset.items()
    }
    prepared_spot_by_asset = {
        asset: _prepare_spot_context_series(asset_rows)
        for asset, asset_rows in spot_by_asset.items()
    }

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, datetime]] = set()
    settled_snapshots_by_market = _crypto_settlement_snapshots_by_market(snapshots)
    prior_snapshot_by_key: dict[tuple[str, datetime], CryptoMarketSnapshotRecord] = {}
    snapshots_by_market: dict[str, list[CryptoMarketSnapshotRecord]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_market[snapshot.market_ticker].append(snapshot)
    for market_snapshots in snapshots_by_market.values():
        market_snapshots.sort(key=lambda r: _as_utc_datetime(r.observed_at))
        prev: CryptoMarketSnapshotRecord | None = None
        for snap in market_snapshots:
            if prev is not None:
                prior_snapshot_by_key[(snap.market_ticker, snap.observed_at)] = prev
            prev = snap
    for snapshot in snapshots:
        settlement_snapshot = settled_snapshots_by_market.get(snapshot.market_ticker)
        settlement_result = getattr(snapshot, "settlement_result", None)
        if settlement_result not in {"yes", "no"}:
            if settlement_snapshot is None:
                continue
            settlement_result = settlement_snapshot.settlement_result
        if settlement_result not in {"yes", "no"}:
            continue
        decision_ts = snapshot.observed_at
        close_time = (
            getattr(settlement_snapshot, "close_time", None)
            or getattr(settlement_snapshot, "expected_expiration_time", None)
            if settlement_snapshot is not None
            else None
        ) or snapshot.close_time or snapshot.expected_expiration_time
        if close_time is not None and decision_ts >= close_time:
            continue
        key = (snapshot.market_ticker, decision_ts)
        if key in seen:
            continue
        seen.add(key)
        candle = _nearest_candle(candles_by_market.get(snapshot.market_ticker, []), decision_ts)
        mid = _row_mid(snapshot) or (candle.close_dollars if candle is not None else None)
        if mid is None:
            continue
        yes_bid = _snapshot_price(snapshot, attr="yes_bid_dollars", dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",))
        yes_ask = _snapshot_price(snapshot, attr="yes_ask_dollars", dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",))
        no_bid = _snapshot_price(snapshot, attr="no_bid_dollars", dollar_keys=("no_bid_dollars",), cent_keys=("no_bid",))
        no_ask = _snapshot_price(snapshot, attr="no_ask_dollars", dollar_keys=("no_ask_dollars",), cent_keys=("no_ask",))
        quote_source = "snapshot_quotes"
        if yes_bid is None or yes_ask is None:
            quote_source = "candlestick_close_proxy"
            yes_bid = mid
            yes_ask = mid
            no_bid = Decimal("1") - mid
            no_ask = Decimal("1") - mid
        else:
            if no_bid is None:
                no_bid = Decimal("1") - yes_ask
            if no_ask is None:
                no_ask = Decimal("1") - yes_bid
        prior_candle = _prior_candle(candles_by_market.get(snapshot.market_ticker, []), decision_ts)
        candle_momentum = None
        if candle is not None and prior_candle is not None and candle.close_dollars is not None and prior_candle.close_dollars is not None:
            candle_momentum = candle.close_dollars - prior_candle.close_dollars
        prior_snap = prior_snapshot_by_key.get((snapshot.market_ticker, decision_ts))
        quote_sequence_context = _crypto_quote_sequence_context(
            mid,
            _crypto_snapshot_spread_bps(snapshot, yes_bid, yes_ask),
            decision_ts,
            _row_mid(prior_snap) if prior_snap is not None else None,
            _crypto_snapshot_spread_bps(prior_snap, None, None) if prior_snap is not None else None,
            prior_snap.observed_at if prior_snap is not None else None,
        )
        spot_context = _spot_context_for_decision(
            spot_by_asset.get(snapshot.asset_symbol, []),
            spot_end_times=spot_end_times_by_asset.get(snapshot.asset_symbol),
            prepared=prepared_spot_by_asset.get(snapshot.asset_symbol),
            decision_ts=decision_ts,
            target_price=snapshot.target_price_dollars or (settlement_snapshot.target_price_dollars if settlement_snapshot is not None else None),
            mid_yes=_clamp_price(mid),
            settings=settings,
            mode=CRYPTO_SPOT_CONTEXT_HISTORICAL,
        )
        strict_trade_eligible = quote_source == "snapshot_quotes"
        market_age_seconds = _crypto_market_age_seconds(decision_ts, getattr(snapshot, "open_time", None))
        target_price = snapshot.target_price_dollars or (settlement_snapshot.target_price_dollars if settlement_snapshot is not None else None)
        settlement_joined = settlement_snapshot is not None and settlement_snapshot is not snapshot
        settlement_window_context = _settlement_window_feature_context(
            spot_by_asset.get(snapshot.asset_symbol, []),
            spot_end_times=spot_end_times_by_asset.get(snapshot.asset_symbol),
            close_time=close_time,
            target_price=target_price,
            frequency=snapshot.frequency,
            as_of=decision_ts,
        )
        rows.append(
            {
                "row_id": f"{snapshot.market_ticker}:{decision_ts.isoformat()}",
                "market_ticker": snapshot.market_ticker,
                "series_ticker": snapshot.series_ticker,
                "asset_symbol": snapshot.asset_symbol,
                "frequency": snapshot.frequency,
                "source_kind": snapshot.source_kind,
                "quote_source": quote_source,
                "leakage_status": "point_in_time",
                "prediction_eligible": True,
                "strict_trade_eligible": strict_trade_eligible,
                "execution_model_status": "real_quote_taker" if strict_trade_eligible else "proxy_quote_prediction_only",
                "decision_ts": decision_ts,
                "settlement_ts": close_time,
                "market_day": decision_ts.date().isoformat(),
                "target_price_dollars": target_price,
                "mid_yes_dollars": _clamp_price(mid),
                "yes_bid_dollars": _clamp_price(yes_bid),
                "yes_ask_dollars": _clamp_price(yes_ask),
                "no_bid_dollars": _clamp_price(no_bid) if no_bid is not None else None,
                "no_ask_dollars": _clamp_price(no_ask) if no_ask is not None else None,
                "spread_bps": int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value()) if yes_bid is not None and yes_ask is not None else None,
                "volume": snapshot.volume,
                "open_interest": snapshot.open_interest,
                "time_to_close_seconds": int((close_time - decision_ts).total_seconds()) if close_time is not None else None,
                "market_age_seconds": market_age_seconds,
                "settlement_result": settlement_result,
                "settlement_label_source": "joined_settled_snapshot" if settlement_joined else "snapshot",
                "label_yes": 1 if settlement_result == "yes" else 0,
                "candle_count": len(candles_by_market.get(snapshot.market_ticker, [])),
                "candle_momentum_dollars": candle_momentum,
                **spot_context,
                **settlement_window_context,
                **_cross_asset_context(spot_by_asset, decision_ts=decision_ts, exclude_asset=snapshot.asset_symbol),
                **_funding_rate_context_for_decision(funding_by_asset, snapshot.asset_symbol, decision_ts=decision_ts),
                **quote_sequence_context,
            }
        )
    for market_ticker, snapshot in settled_snapshots_by_market.items():
        close_time = snapshot.close_time or snapshot.expected_expiration_time
        if close_time is None:
            continue
        replay_candles = [
            candle
            for candle in candles_by_market.get(market_ticker, [])
            if candle.end_period_ts < close_time and candle.close_dollars is not None
        ][-4:]
        for candle in replay_candles:
            decision_ts = candle.end_period_ts
            key = (snapshot.market_ticker, decision_ts)
            if key in seen:
                continue
            seen.add(key)
            mid = _clamp_price(candle.close_dollars)
            prior_candle = _prior_candle(candles_by_market.get(snapshot.market_ticker, []), decision_ts)
            candle_momentum = None
            if prior_candle is not None and prior_candle.close_dollars is not None:
                candle_momentum = candle.close_dollars - prior_candle.close_dollars
            spot_context = _spot_context_for_decision(
                spot_by_asset.get(snapshot.asset_symbol, []),
                spot_end_times=spot_end_times_by_asset.get(snapshot.asset_symbol),
                prepared=prepared_spot_by_asset.get(snapshot.asset_symbol),
                decision_ts=decision_ts,
                target_price=snapshot.target_price_dollars,
                mid_yes=mid,
                settings=settings,
                mode=CRYPTO_SPOT_CONTEXT_HISTORICAL,
            )
            settlement_window_context = _settlement_window_feature_context(
                spot_by_asset.get(snapshot.asset_symbol, []),
                spot_end_times=spot_end_times_by_asset.get(snapshot.asset_symbol),
                close_time=close_time,
                target_price=snapshot.target_price_dollars,
                frequency=snapshot.frequency,
                as_of=decision_ts,
            )
            rows.append(
                {
                    "row_id": f"candle_proxy:{snapshot.market_ticker}:{decision_ts.isoformat()}",
                    "market_ticker": snapshot.market_ticker,
                    "series_ticker": snapshot.series_ticker,
                    "asset_symbol": snapshot.asset_symbol,
                    "frequency": snapshot.frequency,
                    "source_kind": "kalshi_candlestick_replay_proxy",
                    "quote_source": "candlestick_close_proxy",
                    "leakage_status": "point_in_time",
                    "prediction_eligible": True,
                    "strict_trade_eligible": False,
                    "execution_model_status": "proxy_quote_prediction_only",
                    "decision_ts": decision_ts,
                    "settlement_ts": close_time,
                    "market_day": decision_ts.date().isoformat(),
                    "target_price_dollars": snapshot.target_price_dollars,
                    "mid_yes_dollars": mid,
                    "yes_bid_dollars": mid,
                    "yes_ask_dollars": mid,
                    "no_bid_dollars": _clamp_price(Decimal("1") - mid),
                    "no_ask_dollars": _clamp_price(Decimal("1") - mid),
                    "spread_bps": 0,
                    "volume": candle.volume if candle.volume is not None else snapshot.volume,
                    "open_interest": snapshot.open_interest,
                    "time_to_close_seconds": int((close_time - decision_ts).total_seconds()),
                    "market_age_seconds": _crypto_market_age_seconds(decision_ts, getattr(snapshot, "open_time", None)),
                    "settlement_result": snapshot.settlement_result,
                    "label_yes": 1 if snapshot.settlement_result == "yes" else 0,
                    "candle_count": len(candles_by_market.get(snapshot.market_ticker, [])),
                    "candle_momentum_dollars": candle_momentum,
                    **spot_context,
                    **settlement_window_context,
                    **_cross_asset_context(spot_by_asset, decision_ts=decision_ts, exclude_asset=snapshot.asset_symbol),
                    **_funding_rate_context_for_decision(funding_by_asset, snapshot.asset_symbol, decision_ts=decision_ts),
                }
            )
    return _crypto_add_recent_asset_features(rows)


def _crypto_decision_rows_parallel(
    snapshots: list[CryptoMarketSnapshotRecord],
    candles: list[CryptoMarketCandlestickRecord],
    spot_rows: list[CryptoSpotOHLCRecord] | None = None,
    *,
    settings: Settings | None = None,
    workers: int,
    funding_rate_rows: list[CryptoFundingRateRecord] | None = None,
) -> list[dict[str, Any]]:
    """Asset-partitioned parallel build.

    workers<=1 (or a single asset in the dataset) delegates to the serial
    `_crypto_decision_rows` unchanged. Otherwise: split snapshots and candles by
    asset_symbol, broadcast ALL spot_rows + funding_rate_rows to each worker
    (cross-asset spot features require the full set), run each asset's build in a
    separate process, then concatenate. The output row set is identical to the
    serial path (asserted by the parity test).

    NOTE on memory: ProcessPoolExecutor pickles every argument per submitted task,
    so the full spot_rows + funding_rate_rows are copied into each worker (~N
    copies for N concurrent assets). On the dense 60s spot dataset this broadcast
    is the dominant memory cost; the operator bounds it via
    `crypto_train_build_workers`. The win holds because per-asset COMPUTE
    dominates the pickle/transfer overhead.
    """
    assets = sorted({str(s.asset_symbol) for s in snapshots})
    if workers <= 1 or len(assets) <= 1:
        return _crypto_decision_rows(
            snapshots,
            candles,
            spot_rows,
            settings=settings,
            funding_rate_rows=funding_rate_rows,
        )

    max_workers = min(workers, len(assets))
    logger.info(
        "crypto.parallel_build: assets=%d workers=%d spot_rows=%d funding_rows=%d",
        len(assets),
        max_workers,
        len(spot_rows or []),
        len(funding_rate_rows or []),
    )

    snapshots_by_asset: dict[str, list[CryptoMarketSnapshotRecord]] = defaultdict(list)
    for snap in snapshots:
        snapshots_by_asset[str(snap.asset_symbol)].append(snap)
    candles_by_asset: dict[str, list[CryptoMarketCandlestickRecord]] = defaultdict(list)
    for candle in candles:
        candles_by_asset[str(candle.asset_symbol)].append(candle)

    rows: list[dict[str, Any]] = []
    _ctx = multiprocessing.get_context("spawn")
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers, mp_context=_ctx) as pool:
        futures = {
            pool.submit(
                _crypto_decision_rows,
                snapshots_by_asset.get(asset, []),
                candles_by_asset.get(asset, []),
                spot_rows,
                settings=settings,
                funding_rate_rows=funding_rate_rows,
            ): asset
            for asset in assets
        }
        for future in concurrent.futures.as_completed(futures):
            rows.extend(future.result())
    return rows


def _crypto_settlement_snapshots_by_market(
    snapshots: list[CryptoMarketSnapshotRecord],
) -> dict[str, CryptoMarketSnapshotRecord]:
    settled = [
        snapshot
        for snapshot in snapshots
        if getattr(snapshot, "settlement_result", None) in {"yes", "no"}
    ]
    settled.sort(
        key=lambda snapshot: (
            str(getattr(snapshot, "market_ticker", "")),
            _crypto_sort_datetime(
                getattr(snapshot, "close_time", None)
                or getattr(snapshot, "expected_expiration_time", None)
                or getattr(snapshot, "observed_at", None)
            ),
            _crypto_sort_datetime(getattr(snapshot, "observed_at", None)),
        )
    )
    return {snapshot.market_ticker: snapshot for snapshot in settled}


def _crypto_sort_datetime(value: datetime | None) -> datetime:
    return _as_utc_datetime(value) if value is not None else datetime.min.replace(tzinfo=UTC)


def _crypto_settlement_observed_at(market: CryptoMarket) -> datetime:
    raw = market.raw or {}
    for key in ("settlement_ts", "settlement_time", "settled_time", "finalized_time"):
        parsed = parse_datetime(raw.get(key))
        if parsed is not None:
            return parsed
    return market.expected_expiration_time or market.close_time or datetime.now(UTC)


def _crypto_spot_max_stale_seconds(
    provider: str | None,
    source_kind: str | None,
    *,
    settings: Settings | None = None,
) -> int:
    provider_key = str(provider or "").strip().lower()
    if provider_key == "coinbase" and settings is not None:
        return int(settings.crypto_spot_coinbase_max_stale_seconds)
    if provider_key == "coingecko" and settings is not None:
        return int(settings.crypto_spot_coingecko_max_stale_seconds)
    if provider_key in CRYPTO_SPOT_MAX_STALE_SECONDS_BY_PROVIDER:
        return CRYPTO_SPOT_MAX_STALE_SECONDS_BY_PROVIDER[provider_key]
    if str(source_kind or "").strip().lower() == "spot_price_proxy":
        if settings is not None:
            return int(settings.crypto_spot_coingecko_max_stale_seconds)
        return CRYPTO_SPOT_MAX_STALE_SECONDS_BY_PROVIDER["coingecko"]
    return CRYPTO_SPOT_MAX_STALE_SECONDS_BY_PROVIDER["coinbase"]


def _crypto_spot_max_context_gap_seconds(
    provider: str | None,
    source_kind: str | None,
    *,
    mode: str,
    interval_seconds: int | None,
    settings: Settings | None = None,
) -> int:
    live_limit = _crypto_spot_max_stale_seconds(provider, source_kind, settings=settings)
    if str(mode or "").strip().lower() == CRYPTO_SPOT_CONTEXT_HISTORICAL:
        interval = 900 if interval_seconds is None else int(interval_seconds)
        return max(0, interval) + live_limit
    return live_limit


def _crypto_spot_is_proxy(provider: str | None, source_kind: str | None) -> bool:
    provider_key = str(provider or "").strip().lower()
    source_key = str(source_kind or "").strip().lower()
    if not provider_key and not source_key:
        return False
    if provider_key in {"coinbase", "kraken"} and source_key in {"spot_ohlc", "spot_tick"}:
        return False
    return source_key not in {"spot_ohlc", "spot_tick"} or provider_key == "coingecko"


def _crypto_expected_spot_assets(settings: Settings, *, observed_assets: set[str] | None = None) -> list[str]:
    assets = {normalize_asset_symbol(asset) for asset in (observed_assets or set()) if str(asset or "").strip()}
    assets.update(COINBASE_PRODUCT_IDS)
    if settings.crypto_spot_proxy_fallback_enabled:
        assets.update(COINGECKO_IDS)
    return sorted(assets)


def _dedup_spot_rows_by_provider_preference(rows: list[CryptoSpotOHLCRecord]) -> list[CryptoSpotOHLCRecord]:
    """Collapse same-period rows from multiple venues to a single preferred row.

    Momentum/return features treat consecutive rows as distinct periods, so
    interleaved duplicate timestamps from a second provider would corrupt them.
    Rows sharing (end_ts rounded to the second, interval_seconds) keep the
    preferred provider: coinbase > kraken > coingecko. Input ordering is
    preserved (first occurrence position wins).
    """
    if len(rows) < 2:
        return list(rows)
    default_rank = len(CRYPTO_SPOT_PROVIDER_PREFERENCE)
    best_by_key: dict[tuple[int, Any], tuple[int, CryptoSpotOHLCRecord]] = {}
    keys_in_order: list[tuple[int, Any]] = []
    for row in rows:
        key = (
            int(_as_utc_datetime(row.end_ts).timestamp()),
            getattr(row, "interval_seconds", None),
        )
        rank = CRYPTO_SPOT_PROVIDER_PREFERENCE.get(str(row.provider or "").strip().lower(), default_rank)
        existing = best_by_key.get(key)
        if existing is None:
            keys_in_order.append(key)
            best_by_key[key] = (rank, row)
        elif rank < existing[0]:
            best_by_key[key] = (rank, row)
    if len(keys_in_order) == len(rows):
        return list(rows)
    return [best_by_key[key][1] for key in keys_in_order]


def _prepare_spot_context_series(spot_rows: list[CryptoSpotOHLCRecord]) -> dict[str, Any]:
    """Precompute the decision-time-independent parts of _spot_context_for_decision.

    Dedup, the close-not-None filter, and the tick/historical split do not
    depend on the decision timestamp, so doing them per training row made
    feature building O(rows x spot) — hours over a 180d lookback. Prepared
    once per asset, each row needs only a bisect plus a <=40-row tail.
    """
    clean = _dedup_spot_rows_by_provider_preference(
        [row for row in spot_rows if row.close_dollars is not None]
    )
    hist = [row for row in clean if str(row.source_kind or "").strip().lower() != "spot_tick"]
    micro_prefix: list[int] = []
    last = -1
    for i, row in enumerate(clean):
        payload = getattr(row, "payload", None)
        if isinstance(payload, dict) and isinstance(payload.get("market_microstructure"), dict):
            last = i
        micro_prefix.append(last)
    return {
        "rows": clean,
        "end_times": [_as_utc_datetime(row.end_ts) for row in clean],
        "hist_rows": hist,
        "hist_end_times": [_as_utc_datetime(row.end_ts) for row in hist],
        "micro_prefix": micro_prefix,
    }


# Largest tail any consumer needs: returns up to periods=24 (25 rows) and the
# 33-row volatility window, with headroom.
_SPOT_CONTEXT_TAIL_ROWS = 40


def _spot_context_for_decision(
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    spot_end_times: list[datetime] | None = None,
    decision_ts: datetime,
    target_price: Decimal | None,
    mid_yes: Decimal,
    settings: Settings | None = None,
    mode: str = CRYPTO_SPOT_CONTEXT_HISTORICAL,
    prepared: dict[str, Any] | None = None,
) -> dict[str, Any]:
    decision_utc = _as_utc_datetime(decision_ts)
    prepared_microstructure_row: CryptoSpotOHLCRecord | None = None
    if prepared is not None:
        idx = bisect_right(prepared["end_times"], decision_utc)
        eligible = prepared["rows"][max(0, idx - _SPOT_CONTEXT_TAIL_ROWS) : idx]
        micro_i = prepared["micro_prefix"][idx - 1] if idx > 0 else -1
        prepared_microstructure_row = prepared["rows"][micro_i] if micro_i >= 0 else None
        if str(mode or "").strip().lower() == CRYPTO_SPOT_CONTEXT_HISTORICAL:
            hidx = bisect_right(prepared["hist_end_times"], decision_utc)
            if hidx > 0:
                eligible = prepared["hist_rows"][max(0, hidx - _SPOT_CONTEXT_TAIL_ROWS) : hidx]
    elif spot_end_times is not None:
        eligible = [row for row in spot_rows[:bisect_right(spot_end_times, decision_utc)] if row.close_dollars is not None]
        eligible = _dedup_spot_rows_by_provider_preference(eligible)
    else:
        eligible = [
            row
            for row in spot_rows
            if _as_utc_datetime(row.end_ts) <= decision_utc and row.close_dollars is not None
        ]
        eligible = _dedup_spot_rows_by_provider_preference(eligible)
    if not eligible:
        return {
            "spot_feature_status": "missing",
            "spot_provider": None,
            "spot_source_kind": None,
            "spot_proxy_only": None,
            "spot_context_mode": mode,
            "spot_observed_end_ts": None,
            "spot_stale_seconds": None,
            "spot_max_stale_seconds": None,
            "spot_close_dollars": None,
            "spot_moneyness_dollars": None,
            "spot_moneyness_pct": None,
            "spot_momentum_pct": None,
            "spot_return_1_pct": None,
            "spot_return_3_pct": None,
            "spot_return_6_pct": None,
            "spot_realized_volatility": None,
            "spot_target_distance_volatility": None,
            "kalshi_mid_spot_gap": None,
            "spot_exchange_bid_dollars": None,
            "spot_exchange_ask_dollars": None,
            "spot_exchange_mid_dollars": None,
            "spot_exchange_spread_bps": None,
            "spot_exchange_latest_trade_size": None,
            "spot_exchange_recent_trade_count": None,
            "spot_return_12_pct": None,
            "spot_return_24_pct": None,
            "spot_realized_volatility_32": None,
        }
    # Microstructure (exchange best bid/ask, recent trade count) only rides on
    # spot_tick payloads; capture the latest one BEFORE the historical mode
    # drops tick rows, so training sees the same features as live.
    if prepared is not None:
        microstructure_row = prepared_microstructure_row
    else:
        microstructure_row = next(
            (
                row
                for row in reversed(eligible)
                if isinstance(getattr(row, "payload", None), dict)
                and isinstance(row.payload.get("market_microstructure"), dict)
            ),
            None,
        )
        if str(mode or "").strip().lower() == CRYPTO_SPOT_CONTEXT_HISTORICAL:
            historical_eligible = [
                row
                for row in eligible
                if str(row.source_kind or "").strip().lower() != "spot_tick"
            ]
            if historical_eligible:
                eligible = historical_eligible
    current = eligible[-1]
    close = _decimal(current.close_dollars)
    stale_seconds = int((decision_utc - _as_utc_datetime(current.end_ts)).total_seconds())
    max_stale_seconds = _crypto_spot_max_context_gap_seconds(
        current.provider,
        current.source_kind,
        mode=mode,
        interval_seconds=getattr(current, "interval_seconds", None),
        settings=settings,
    )
    proxy_source = _crypto_spot_is_proxy(current.provider, current.source_kind)
    stale = stale_seconds > max_stale_seconds
    prior = eligible[-2] if len(eligible) >= 2 else None
    prior_close = _decimal(prior.close_dollars) if prior is not None else None
    momentum_pct = None
    if prior_close is not None and prior_close > 0:
        momentum_pct = (close - prior_close) / prior_close
    spot_return_1_pct = momentum_pct
    spot_return_3_pct = _spot_return_pct(eligible, periods=3)
    spot_return_6_pct = _spot_return_pct(eligible, periods=6)
    spot_return_12_pct = _spot_return_pct(eligible, periods=12)
    spot_return_24_pct = _spot_return_pct(eligible, periods=24)
    returns: list[Decimal] = []
    window = eligible[-9:]
    for before, after in zip(window, window[1:], strict=False):
        before_close = _decimal(before.close_dollars)
        after_close = _decimal(after.close_dollars)
        if before_close > 0:
            returns.append((after_close - before_close) / before_close)
    volatility = None
    if returns:
        mean = sum(returns, Decimal("0")) / Decimal(len(returns))
        variance = sum((value - mean) * (value - mean) for value in returns) / Decimal(len(returns))
        volatility = Decimal(str(math.sqrt(float(variance))))
    returns_32: list[Decimal] = []
    window_32 = eligible[-33:]
    for before, after in zip(window_32, window_32[1:], strict=False):
        before_close = _decimal(before.close_dollars)
        after_close = _decimal(after.close_dollars)
        if before_close > 0:
            returns_32.append((after_close - before_close) / before_close)
    volatility_32 = None
    if returns_32:
        mean_32 = sum(returns_32, Decimal("0")) / Decimal(len(returns_32))
        variance_32 = sum((v - mean_32) * (v - mean_32) for v in returns_32) / Decimal(len(returns_32))
        volatility_32 = Decimal(str(math.sqrt(float(variance_32))))
    moneyness = None
    moneyness_pct = None
    spot_probability_proxy = None
    if target_price is not None and target_price > 0:
        moneyness = close - target_price
        moneyness_pct = moneyness / target_price
        spot_probability_proxy = Decimal("0.5000") + max(Decimal("-0.5000"), min(Decimal("0.5000"), moneyness_pct * Decimal("20")))
    target_distance_volatility = None
    if moneyness_pct is not None and volatility is not None and volatility > 0:
        target_distance_volatility = moneyness_pct / volatility
    kalshi_gap = mid_yes - spot_probability_proxy if spot_probability_proxy is not None else None
    payload = current.payload if isinstance(current.payload, dict) else {}
    microstructure = payload.get("market_microstructure") if isinstance(payload.get("market_microstructure"), dict) else {}
    if not microstructure and microstructure_row is not None:
        microstructure = microstructure_row.payload.get("market_microstructure") or {}
    best_bid_ask = microstructure.get("best_bid_ask") if isinstance(microstructure.get("best_bid_ask"), dict) else {}
    latest_trade = microstructure.get("latest_trade") if isinstance(microstructure.get("latest_trade"), dict) else {}
    spot_exchange_bid = _optional_decimal(best_bid_ask.get("best_bid_dollars"))
    spot_exchange_ask = _optional_decimal(best_bid_ask.get("best_ask_dollars"))
    spot_exchange_mid = _optional_decimal(best_bid_ask.get("mid_dollars"))
    spot_exchange_spread = best_bid_ask.get("spread_bps")
    spot_exchange_latest_trade_size = _optional_decimal(latest_trade.get("size"))
    spot_exchange_recent_trade_count = microstructure.get("recent_trade_count")
    return {
        "spot_feature_status": "available" if not stale else "stale",
        "spot_provider": current.provider,
        "spot_source_kind": current.source_kind,
        "spot_proxy_only": proxy_source,
        "spot_context_mode": mode,
        "spot_observed_end_ts": _as_utc_datetime(current.end_ts),
        "spot_stale_seconds": stale_seconds,
        "spot_max_stale_seconds": max_stale_seconds,
        "spot_close_dollars": close,
        "spot_moneyness_dollars": moneyness,
        "spot_moneyness_pct": moneyness_pct,
        "spot_momentum_pct": momentum_pct,
        "spot_return_1_pct": spot_return_1_pct,
        "spot_return_3_pct": spot_return_3_pct,
        "spot_return_6_pct": spot_return_6_pct,
        "spot_realized_volatility": volatility,
        "spot_target_distance_volatility": target_distance_volatility,
        "kalshi_mid_spot_gap": kalshi_gap,
        "spot_exchange_bid_dollars": spot_exchange_bid,
        "spot_exchange_ask_dollars": spot_exchange_ask,
        "spot_exchange_mid_dollars": spot_exchange_mid,
        "spot_exchange_spread_bps": int(spot_exchange_spread) if spot_exchange_spread not in (None, "") else None,
        "spot_exchange_latest_trade_size": spot_exchange_latest_trade_size,
        "spot_exchange_recent_trade_count": int(spot_exchange_recent_trade_count) if spot_exchange_recent_trade_count not in (None, "") else None,
        "spot_return_12_pct": spot_return_12_pct,
        "spot_return_24_pct": spot_return_24_pct,
        "spot_realized_volatility_32": volatility_32,
    }


def _spot_return_pct(spot_rows: list[CryptoSpotOHLCRecord], *, periods: int) -> Decimal | None:
    if len(spot_rows) <= periods:
        return None
    current_close = _decimal(spot_rows[-1].close_dollars)
    prior_close = _decimal(spot_rows[-1 - periods].close_dollars)
    if prior_close <= 0:
        return None
    return (current_close - prior_close) / prior_close


def _cross_asset_context(
    spot_by_asset: dict[str, list[CryptoSpotOHLCRecord]],
    *,
    decision_ts: datetime,
    exclude_asset: str | None = None,
) -> dict[str, Any]:
    """Per-asset spot returns for every cross-asset feature the schema consumes.

    The feature vector reads `{asset}_return_{1,3}_pct` for ALL of
    CRYPTO_CROSS_ASSET_FEATURE_ASSETS; only emitting BTC/ETH left the other
    five permanently zero (dead features in every trained model). The market's
    own asset is excluded so training matches the live path, which never
    feeds self-asset rows here (own momentum is already in spot_return_*).
    """
    decision_utc = _as_utc_datetime(decision_ts)

    def _cross_return(asset: str, periods: int) -> Decimal | None:
        # Called once per training row; the per-call sorted() over a year of
        # backfilled spot rows turned training into an O(rows x spot log spot)
        # crawl (10h+ CPU). Rows arrive ascending from the training builders;
        # the live path fetches small most-recent-first lists, so normalize
        # cheaply and bisect to the as-of cutoff instead.
        rows = spot_by_asset.get(asset) or []
        if not rows:
            return None
        if len(rows) > 1 and _as_utc_datetime(rows[0].end_ts) > _as_utc_datetime(rows[-1].end_ts):
            rows = sorted(rows, key=lambda r: r.end_ts)
        idx = bisect_right(rows, decision_utc, key=lambda r: _as_utc_datetime(r.end_ts))
        if idx <= periods:
            return None
        window = [r for r in rows[max(0, idx - (periods + 8)) : idx] if r.close_dollars is not None]
        return _spot_return_pct(window, periods=periods)

    excluded = (exclude_asset or "").strip().upper()
    context: dict[str, Any] = {}
    for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS:
        key = asset.lower()
        if asset == excluded:
            context[f"{key}_return_1_pct"] = None
            context[f"{key}_return_3_pct"] = None
            continue
        context[f"{key}_return_1_pct"] = _cross_return(asset, 1)
        context[f"{key}_return_3_pct"] = _cross_return(asset, 3)
    return context


def _funding_rate_context_for_decision(
    funding_by_asset: dict[str, list[CryptoFundingRateRecord]],
    asset_symbol: str,
    *,
    decision_ts: datetime,
) -> dict[str, Any]:
    decision_utc = _as_utc_datetime(decision_ts)
    rows = funding_by_asset.get(asset_symbol) or []
    # rows are pre-sorted oldest-first; find those settled at or before decision_ts
    eligible = [r for r in rows if _as_utc_datetime(r.settlement_ts) <= decision_utc]
    if not eligible:
        return {"funding_rate_current": Decimal("0"), "funding_rate_delta": Decimal("0")}
    current = eligible[-1].realized_rate
    prior = eligible[-2].realized_rate if len(eligible) >= 2 else current
    return {
        "funding_rate_current": current,
        "funding_rate_delta": current - prior,
    }


def _filter_snapshots_by_per_asset_funding_cutoff(
    funding_rate_rows: list[CryptoFundingRateRecord],
    snapshots: list,
) -> list:
    """Keep only snapshots at or after the earliest funding rate settlement for each asset.

    Assets with no funding rate rows are excluded entirely, enforcing train/eval parity:
    a row is included iff its asset has coverage AND observed_at >= that asset's first settlement.
    """
    cutoff_by_asset: dict[str, datetime] = {}
    for _fr in funding_rate_rows:
        _ts = _as_utc_datetime(_fr.settlement_ts)
        if _fr.asset_symbol not in cutoff_by_asset or _ts < cutoff_by_asset[_fr.asset_symbol]:
            cutoff_by_asset[_fr.asset_symbol] = _ts
    if not cutoff_by_asset:
        return snapshots
    return [
        s for s in snapshots
        if cutoff_by_asset.get(s.asset_symbol) is not None
        and _as_utc_datetime(s.observed_at) >= cutoff_by_asset[s.asset_symbol]
    ]


def _crypto_add_recent_asset_features(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    enriched: list[dict[str, Any]] = []
    for row in ordered:
        asset = str(row.get("asset_symbol") or "unknown")
        decision_ts = row.get("decision_ts")
        prior = [
            item
            for item in history[asset][-20:]
            if decision_ts is None or item.get("settlement_ts") is None or item["settlement_ts"] <= decision_ts
        ]
        updated = dict(row)
        if prior:
            yes_rate = sum(int(item["label_yes"]) for item in prior) / len(prior)
            mid_error = sum(int(item["label_yes"]) - float(_decimal(item["mid_yes_dollars"])) for item in prior) / len(prior)
            updated["asset_recent_yes_rate"] = Decimal(str(round(yes_rate, 6)))
            updated["asset_recent_mid_error"] = Decimal(str(round(mid_error, 6)))
            updated["asset_recent_sample_count"] = len(prior)
        else:
            updated["asset_recent_yes_rate"] = None
            updated["asset_recent_mid_error"] = None
            updated["asset_recent_sample_count"] = 0
        enriched.append(updated)
        history[asset].append(updated)
    return enriched


def _spot_feature_coverage(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return _ratio(sum(1 for row in rows if row.get("spot_feature_status") == "available") / len(rows)) or 0.0


def _json_ready_spot_features(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "spot_feature_status",
        "spot_provider",
        "spot_source_kind",
        "spot_proxy_only",
        "spot_observed_end_ts",
        "spot_stale_seconds",
        "spot_max_stale_seconds",
        "spot_close_dollars",
        "spot_moneyness_dollars",
        "spot_moneyness_pct",
        "spot_momentum_pct",
        "spot_return_1_pct",
        "spot_return_3_pct",
        "spot_return_6_pct",
        "spot_realized_volatility",
        "spot_target_distance_volatility",
        "kalshi_mid_spot_gap",
    ]
    result: dict[str, Any] = {}
    for key in keys:
        value = row.get(key)
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, Decimal):
            result[key] = str(value.quantize(Decimal("0.000001")))
        else:
            result[key] = value
    return result


def _crypto_spot_distance_signal_value(row: dict[str, Any]) -> Decimal | None:
    value = row.get("spot_target_distance_volatility")
    if value is None:
        pct = row.get("spot_moneyness_pct")
        if pct is None:
            return None
        value = Decimal(str(pct)) * Decimal("20")
    try:
        return _decimal(value)
    except Exception:
        return None


def _crypto_training_step_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if key
        in {
            "status",
            "kalshi_env",
            "frequency",
            "asset_symbols",
            "lookback_days",
            "stored",
            "markets_stored",
            "settled_markets_stored",
            "stored_real_quote_snapshots",
            "candles_stored",
            "errors",
            "data_quality",
            "spot_quality",
            "recent_quote_evidence",
        }
    }


def _crypto_training_json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _crypto_training_json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_crypto_training_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_crypto_training_json_ready(item) for item in value]
    return value


def _crypto_training_build_id(payload: Any) -> str:
    normalized = _crypto_training_json_ready(payload)
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


_CRYPTO_TRAINING_DATETIME_KEYS = {
    "decision_ts",
    "settlement_ts",
    "spot_observed_end_ts",
    "settlement_window_start_ts",
    "settlement_window_end_ts",
}


def _crypto_training_row_payload(record: Any) -> dict[str, Any]:
    payload = record.payload if isinstance(record.payload, dict) else {}
    row = payload.get("decision_row") if isinstance(payload.get("decision_row"), dict) else dict(payload)
    hydrated = dict(row)
    for key in _CRYPTO_TRAINING_DATETIME_KEYS:
        if isinstance(hydrated.get(key), str):
            parsed = parse_datetime(hydrated.get(key))
            if parsed is not None:
                hydrated[key] = parsed
    return hydrated


async def _crypto_training_feature_decision_rows(
    repo: PlatformRepository,
    *,
    frequency: str,
    kalshi_env: str,
    asset_symbols: list[str] | None,
    since: datetime | None,
    until: datetime | None = None,
    limit: int = 100_000,
) -> list[dict[str, Any]]:
    records = await repo.list_crypto_training_feature_rows(
        frequency=frequency,
        kalshi_env=kalshi_env,
        asset_symbols=asset_symbols or None,
        since=since,
        until=until,
        limit=limit,
    )
    return [_crypto_training_row_payload(record) for record in reversed(records)]


def _crypto_training_row_quality_score(row: dict[str, Any]) -> float:
    score = 1.0
    if not row.get("strict_trade_eligible"):
        score -= 0.25
    if row.get("spot_feature_status") != "available":
        score -= 0.25
    if row.get("quote_source") != "snapshot_quotes":
        score -= 0.15
    if bool(row.get("spot_proxy_only")):
        score -= 0.10
    # Time-proximity weight: rows made 30s–12min before settlement are most
    # informative for our decision window; very-close (<30s) and very-early
    # (>12min) rows are down-weighted by up to 30%.
    ttc = max(0.0, float(row.get("time_to_close_seconds") or 0))
    if ttc < 30:
        score *= 0.70
    elif ttc > 720:
        score *= 0.80
    return max(0.0, min(1.0, score))


def _crypto_row_market_ticker(row: dict[str, Any]) -> str:
    return str(row.get("market_ticker") or "unknown")


def _crypto_distinct_market_count(rows: list[dict[str, Any]]) -> int:
    return len({_crypto_row_market_ticker(row) for row in rows})


def _crypto_training_sample_weights(
    rows: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
) -> list[float]:
    quality = [float(_crypto_training_row_quality_score(row)) for row in rows]
    if settings is not None and not settings.crypto_train_market_balanced_weights:
        return quality
    # Down-weight markets with many snapshot rows (15s cadence yields ~60
    # near-identical rows per 15m market) so each market contributes equally.
    market_counts = Counter(_crypto_row_market_ticker(row) for row in rows)
    weights = [
        score / max(1, market_counts[_crypto_row_market_ticker(row)])
        for score, row in zip(quality, rows, strict=True)
    ]
    total = sum(weights)
    if total <= 0.0:
        return weights
    scale = len(weights) / total
    return [weight * scale for weight in weights]


def _probability_metrics_market_weighted(
    predictions: list[tuple[Decimal, int, str]],
) -> dict[str, Any]:
    grouped: dict[str, list[tuple[Decimal, int]]] = defaultdict(list)
    for probability, label, market_ticker in predictions:
        grouped[market_ticker].append((probability, label))
    if not grouped:
        return {"market_count": 0, "brier": None, "log_loss": None, "ece": None}
    per_market = [_probability_metrics_decimal(items) for items in grouped.values()]

    def _mean(key: str) -> float | None:
        values = [metrics[key] for metrics in per_market if metrics.get(key) is not None]
        return sum(values) / len(values) if values else None

    return {
        "market_count": len(grouped),
        "brier": _mean("brier"),
        "log_loss": _mean("log_loss"),
        "ece": _mean("ece"),
    }


def _crypto_dead_feature_report(
    rows: list[dict[str, Any]],
    *,
    schema: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not rows:
        return {"dead_feature_names": [], "dead_feature_count": 0}
    schema = schema or _crypto_feature_schema(rows)
    defaults = defaults or _crypto_feature_defaults(rows)
    feature_names = list(schema.get("feature_names") or [])
    # Streamed zero-variance check: avoids materializing a second copy of the
    # full training matrix.
    first_vector: list[float] | None = None
    varying: set[int] = set()
    for row in rows:
        vector = _crypto_raw_feature_vector(row, schema, defaults=defaults)
        if first_vector is None:
            first_vector = vector
            continue
        for index, value in enumerate(vector):
            if index not in varying and value != first_vector[index]:
                varying.add(index)
    dead = [name for index, name in enumerate(feature_names) if index not in varying]
    # Microstructure features (exchange spread, recent-trade-count) only exist in
    # the dense live spot snapshots; historical OHLC backfill has none, so they
    # read constant on any history-dominated corpus. That is a data-density
    # symptom — the same one behind the spot-coverage blocker — not a pipeline
    # bug, so it is reported at INFO. Any OTHER dead feature is a real regression
    # and stays at WARNING.
    data_density_dead = [name for name in dead if name in _CRYPTO_DATA_DENSITY_FEATURES]
    unexpected_dead = [name for name in dead if name not in _CRYPTO_DATA_DENSITY_FEATURES]
    if unexpected_dead:
        logger.warning(
            "crypto_training_dead_features count=%d names=%s",
            len(unexpected_dead),
            ",".join(unexpected_dead),
        )
    if data_density_dead:
        logger.info(
            "crypto_training_data_density_constant_features count=%d names=%s",
            len(data_density_dead),
            ",".join(data_density_dead),
        )
    return {
        "dead_feature_names": dead,
        "dead_feature_count": len(dead),
        "unexpected_dead_feature_names": unexpected_dead,
        "data_density_dead_feature_names": data_density_dead,
    }


# Features that are populated only by dense live spot snapshots (market
# microstructure + availability flags). They read constant on history-dominated
# corpora until enough live data accumulates; not pipeline regressions.
_CRYPTO_DATA_DENSITY_FEATURES = frozenset(
    {
        "spot_available",
        "settlement_window_available",
        "spot_exchange_spread_bps",
        "spot_exchange_recent_trade_count",
    }
)


def _crypto_training_quality_blockers(
    rows: list[dict[str, Any]],
    *,
    spot_coverage: float,
    settings: Settings,
    asset_symbols: list[str] | None = None,
) -> list[str]:
    blockers: list[str] = []
    if len(rows) < settings.crypto_min_training_samples:
        blockers.append(
            f"feature_rows_below_min:{len(rows)}<{settings.crypto_min_training_samples}"
        )
    strict_rows = sum(1 for row in rows if row.get("strict_trade_eligible"))
    if strict_rows <= 0:
        blockers.append("missing_strict_trade_eligible_rows")
    min_strict = max(0, int(settings.crypto_training_preflight_min_strict_rows))
    if strict_rows < min_strict:
        blockers.append(f"strict_trade_eligible_rows_below_min:{strict_rows}<{min_strict}")
    min_strict_ratio = max(0.0, float(settings.crypto_training_preflight_min_strict_row_ratio))
    strict_ratio = strict_rows / len(rows) if rows else 0.0
    if rows and min_strict_ratio > 0.0 and strict_ratio < min_strict_ratio:
        blockers.append(f"strict_trade_eligible_ratio_below_min:{strict_ratio:.3f}<{min_strict_ratio:.3f}")
    min_strict_per_asset = max(0, int(settings.crypto_training_preflight_min_strict_rows_per_asset))
    if min_strict_per_asset > 0:
        expected_assets = normalize_asset_symbols(asset_symbols) or sorted(
            {
                normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
                for row in rows
                if row.get("asset_symbol")
            }
        )
        strict_by_asset: Counter[str] = Counter(
            normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
            for row in rows
            if row.get("strict_trade_eligible")
        )
        for asset in expected_assets:
            asset_strict = strict_by_asset.get(asset, 0)
            if asset_strict < min_strict_per_asset:
                blockers.append(
                    f"strict_trade_eligible_rows_below_min_by_asset:{asset}:{asset_strict}<{min_strict_per_asset}"
                )
    min_spot = max(0.0, float(settings.crypto_training_preflight_min_spot_coverage_pct))
    if rows and spot_coverage < min_spot:
        blockers.append(f"spot_coverage_below_min:{spot_coverage:.3f}<{min_spot:.3f}")
    return blockers


def _settlement_benchmark_context(
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    spot_end_times: list[datetime] | None = None,
    close_time: datetime | None,
    target_price: Decimal | None,
    frequency: str,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if close_time is None:
        return {
            "settlement_window_status": "missing_close_time",
            "settlement_window_sample_count": 0,
            "settlement_window_start_ts": None,
            "settlement_window_end_ts": None,
            "settlement_window_open_dollars": None,
            "settlement_window_high_dollars": None,
            "settlement_window_low_dollars": None,
            "settlement_window_close_dollars": None,
            "settlement_window_twap_dollars": None,
            "settlement_window_vwap_dollars": None,
            "settlement_twap_target_distance_pct": None,
            "settlement_window_return_pct": None,
            "settlement_window_volatility": None,
        }
    try:
        interval = interval_seconds_for_frequency(frequency)
    except ValueError:
        interval = 900
    close_utc = _as_utc_datetime(close_time)
    start = close_utc - timedelta(seconds=interval)
    end_cap = close_utc if as_of is None else min(close_utc, _as_utc_datetime(as_of))
    if spot_end_times is not None:
        start_index = bisect_left(spot_end_times, start)
        end_index = bisect_right(spot_end_times, end_cap)
        window_rows = spot_rows[start_index:end_index]
        eligible = [row for row in window_rows if row.close_dollars is not None]
    else:
        eligible = [
            row
            for row in spot_rows
            if row.close_dollars is not None and start <= _as_utc_datetime(row.end_ts) <= end_cap
        ]
    eligible.sort(key=lambda row: row.end_ts)
    if not eligible:
        return {
            "settlement_window_status": "missing_spot_window",
            "settlement_window_sample_count": 0,
            "settlement_window_start_ts": start,
            "settlement_window_end_ts": end_cap,
            "settlement_window_open_dollars": None,
            "settlement_window_high_dollars": None,
            "settlement_window_low_dollars": None,
            "settlement_window_close_dollars": None,
            "settlement_window_twap_dollars": None,
            "settlement_window_vwap_dollars": None,
            "settlement_twap_target_distance_pct": None,
            "settlement_window_return_pct": None,
            "settlement_window_volatility": None,
        }
    closes = [_decimal(row.close_dollars) for row in eligible]
    highs = [_decimal(row.high_dollars if row.high_dollars is not None else row.close_dollars) for row in eligible]
    lows = [_decimal(row.low_dollars if row.low_dollars is not None else row.close_dollars) for row in eligible]
    volumes = [_optional_decimal(row.volume) or Decimal("0") for row in eligible]
    twap = sum(closes, Decimal("0")) / Decimal(len(closes))
    volume_sum = sum(volumes, Decimal("0"))
    vwap = (
        sum((close * volume for close, volume in zip(closes, volumes, strict=False)), Decimal("0")) / volume_sum
        if volume_sum > 0
        else None
    )
    target_distance = None
    if target_price is not None and target_price > 0:
        target_distance = (twap - target_price) / target_price
    window_return = None
    if closes[0] > 0:
        window_return = (closes[-1] - closes[0]) / closes[0]
    returns = [
        (after - before) / before
        for before, after in zip(closes, closes[1:], strict=False)
        if before > 0
    ]
    volatility = None
    if returns:
        mean = sum(returns, Decimal("0")) / Decimal(len(returns))
        variance = sum((item - mean) * (item - mean) for item in returns) / Decimal(len(returns))
        volatility = Decimal(str(math.sqrt(float(variance))))
    return {
        "settlement_window_status": "available",
        "settlement_window_sample_count": len(eligible),
        "settlement_window_start_ts": _as_utc_datetime(eligible[0].end_ts),
        "settlement_window_end_ts": _as_utc_datetime(eligible[-1].end_ts),
        "settlement_window_open_dollars": _optional_decimal(eligible[0].open_dollars) or closes[0],
        "settlement_window_high_dollars": max(highs),
        "settlement_window_low_dollars": min(lows),
        "settlement_window_close_dollars": closes[-1],
        "settlement_window_twap_dollars": twap,
        "settlement_window_vwap_dollars": vwap,
        "settlement_twap_target_distance_pct": target_distance,
        "settlement_window_return_pct": window_return,
        "settlement_window_volatility": volatility,
    }


_CRYPTO_SETTLEMENT_WINDOW_FEATURE_KEYS = (
    "settlement_window_status",
    "settlement_window_sample_count",
    "settlement_twap_target_distance_pct",
    "settlement_window_return_pct",
    "settlement_window_volatility",
)


def _settlement_window_feature_context(
    spot_rows: list[CryptoSpotOHLCRecord],
    *,
    spot_end_times: list[datetime] | None = None,
    close_time: datetime | None,
    target_price: Decimal | None,
    frequency: str,
    as_of: datetime,
) -> dict[str, Any]:
    context = _settlement_benchmark_context(
        spot_rows,
        spot_end_times=spot_end_times,
        close_time=close_time,
        target_price=target_price,
        frequency=frequency,
        as_of=as_of,
    )
    return {key: context.get(key) for key in _CRYPTO_SETTLEMENT_WINDOW_FEATURE_KEYS}


def _trace_value(payload: Any, *keys: str) -> Any:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if payload.get(key) not in (None, ""):
            return payload.get(key)
    for value in payload.values():
        if isinstance(value, dict):
            found = _trace_value(value, *keys)
            if found not in (None, ""):
                return found
    return None


def _crypto_realized_fill_pnl(fills: list[FillRecord]) -> Decimal | None:
    pnls = [
        PlatformRepository._fill_pnl_metrics([fill]).get("total_pnl_dollars")
        for fill in fills
    ]
    values = [_optional_decimal(value) for value in pnls if value not in (None, "")]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return sum(values, Decimal("0"))


def _nearest_candle(
    candles: list[CryptoMarketCandlestickRecord],
    decision_ts: datetime,
    *,
    candle_end_times: list[datetime] | None = None,
) -> CryptoMarketCandlestickRecord | None:
    if candle_end_times is not None:
        index = bisect_right(candle_end_times, _as_utc_datetime(decision_ts)) - 1
        return candles[index] if index >= 0 else None
    eligible = [row for row in candles if row.end_period_ts <= decision_ts]
    return eligible[-1] if eligible else None


def _prior_candle(
    candles: list[CryptoMarketCandlestickRecord],
    decision_ts: datetime,
    *,
    candle_end_times: list[datetime] | None = None,
) -> CryptoMarketCandlestickRecord | None:
    if candle_end_times is not None:
        eligible_count = bisect_left(candle_end_times, _as_utc_datetime(decision_ts))
        return candles[eligible_count - 2] if eligible_count >= 2 else None
    eligible = [row for row in candles if row.end_period_ts < decision_ts]
    return eligible[-2] if len(eligible) >= 2 else None


def _recent_candles_before(
    candles: list[CryptoMarketCandlestickRecord],
    close_time: datetime,
    *,
    candle_end_times: list[datetime] | None = None,
    limit: int,
) -> list[CryptoMarketCandlestickRecord]:
    if limit <= 0:
        return []
    if candle_end_times is None:
        return [
            candle
            for candle in candles
            if candle.end_period_ts < close_time and candle.close_dollars is not None
        ][-limit:]
    end_index = bisect_left(candle_end_times, _as_utc_datetime(close_time))
    replay_candles: list[CryptoMarketCandlestickRecord] = []
    for index in range(end_index - 1, -1, -1):
        candle = candles[index]
        if candle.close_dollars is None:
            continue
        replay_candles.append(candle)
        if len(replay_candles) >= limit:
            break
    replay_candles.reverse()
    return replay_candles


def _crypto_feature_schema(rows: list[dict[str, Any]]) -> dict[str, Any]:
    assets = sorted({str(row.get("asset_symbol") or "unknown") for row in rows})
    numeric = [
        "market_mid_logit",
        "mid_yes",
        "time_to_close_ratio",
        "execution_spread",
        "volume_log",
        "open_interest_log",
        "candle_momentum",
        "target_price_log",
        "spot_available",
        "spot_moneyness_pct",
        "spot_momentum_pct",
        "spot_return_1_pct",
        "spot_return_3_pct",
        "spot_return_6_pct",
        "spot_realized_volatility",
        "spot_target_distance_volatility",
        "kalshi_mid_spot_gap",
        "spot_stale_ratio",
        "asset_recent_yes_rate_delta",
        "asset_recent_mid_error",
        "quote_source_candlestick_proxy",
        "quote_source_snapshot_quotes",
        "strict_trade_eligible",
        # time_to_close_bucket_15m_plus is intentionally absent: 15m markets never
        # reach it and the 1h bucketing emits different bucket names, so the column
        # was constant-zero in every corpus — and one-hots drop one level anyway.
        "time_to_close_bucket_0_5m",
        "time_to_close_bucket_5_10m",
        "time_to_close_bucket_10_15m",
        "market_age_ratio",
        "spot_exchange_spread_bps",
        "spot_exchange_recent_trade_count",
        "spot_return_12_pct",
        "spot_return_24_pct",
        "spot_realized_volatility_32",
        "market_mid_change_1",
        "market_mid_velocity_per_min",
        "spread_change_bps_1",
        "quote_observation_gap_ratio",
        "settlement_window_available",
        "settlement_window_sample_count",
        "settlement_twap_target_distance_pct",
        "settlement_window_return_pct",
        "settlement_window_volatility",
        "close_hour_sin",
        "close_hour_cos",
        "close_dow_sin",
        "close_dow_cos",
        *[
            f"{asset.lower()}_return_{period}_pct"
            for asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS
            for period in (1, 3)
        ],
        "funding_rate_current",
        "funding_rate_delta",
        "bid_pressure",
        "spread_vs_vol_ratio",
        "is_us_session",
        "is_asia_session",
    ]
    if len(assets) == 1:
        # Single-asset corpus: the self cross-asset return is excluded at feature
        # build time (train/live skew guard) and a lone one-hot is constant — both
        # carry zero information, so drop the columns instead of training on them.
        self_keys = {f"{assets[0].lower()}_return_1_pct", f"{assets[0].lower()}_return_3_pct"}
        numeric = [name for name in numeric if name not in self_keys]
        onehot_assets: list[str] = []
    else:
        onehot_assets = assets
    feature_names = [*numeric, *[f"asset={asset}" for asset in onehot_assets]]
    # NOTE: do not auto-prune corpus-constant columns here. This builder is called
    # per walk-forward fold, so per-fold pruning would yield inconsistent schemas
    # across folds and flood logs with false-alarm warnings for features that are
    # merely constant in an early/sparse fold (volume_log, funding_rate, ...).
    # Structural constants (lone one-hot, self cross-returns) are pruned above
    # because they are constant by construction in every fold. Data-driven
    # constants (microstructure features with no historical coverage) are surfaced
    # once on the full corpus by _crypto_dead_feature_report instead — that keeps
    # the dead-feature signal meaningful and the fold schemas stable.
    return {
        "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
        "feature_names": feature_names,
        "numeric_feature_names": numeric,
        "asset_categories": assets,
    }


def _crypto_feature_defaults(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_asset[str(row.get("asset_symbol") or "unknown")].append(row)
    asset_defaults: dict[str, dict[str, float]] = {}
    for asset, asset_rows in by_asset.items():
        recent_yes = [
            float(_decimal(row["asset_recent_yes_rate"]))
            for row in asset_rows
            if row.get("asset_recent_yes_rate") is not None
        ]
        recent_error = [
            float(_decimal(row["asset_recent_mid_error"]))
            for row in asset_rows
            if row.get("asset_recent_mid_error") is not None
        ]
        asset_defaults[asset] = {
            "asset_recent_yes_rate": sum(recent_yes) / len(recent_yes) if recent_yes else 0.5,
            "asset_recent_mid_error": sum(recent_error) / len(recent_error) if recent_error else 0.0,
        }
    return {
        "global": {
            "asset_recent_yes_rate": 0.5,
            "asset_recent_mid_error": 0.0,
        },
        "by_asset": asset_defaults,
    }


def _crypto_raw_feature_vector(
    row: dict[str, Any],
    schema: dict[str, Any],
    *,
    defaults: dict[str, Any] | None = None,
) -> list[float]:
    mid = float(_clamp_price(_decimal(row.get("mid_yes_dollars") or Decimal("0.5000"))))
    asset = str(row.get("asset_symbol") or "unknown")
    spread_bps = float(row.get("spread_bps") or 0)
    time_to_close = max(0.0, float(row.get("time_to_close_seconds") or 0))
    volume = max(0.0, float(row.get("volume") or 0))
    open_interest = max(0.0, float(row.get("open_interest") or 0))
    target_price = max(0.0, float(_decimal(row.get("target_price_dollars") or Decimal("0"))))
    candle_momentum = float(_decimal(row.get("candle_momentum_dollars") or Decimal("0")))
    spot_moneyness = float(_decimal(row.get("spot_moneyness_pct") or Decimal("0")))
    spot_momentum = float(_decimal(row.get("spot_momentum_pct") or Decimal("0")))
    spot_return_1 = float(_decimal(row.get("spot_return_1_pct") or Decimal("0")))
    spot_return_3 = float(_decimal(row.get("spot_return_3_pct") or Decimal("0")))
    spot_return_6 = float(_decimal(row.get("spot_return_6_pct") or Decimal("0")))
    spot_return_12 = float(_decimal(row.get("spot_return_12_pct") or Decimal("0")))
    spot_return_24 = float(_decimal(row.get("spot_return_24_pct") or Decimal("0")))
    spot_volatility = float(_decimal(row.get("spot_realized_volatility") or Decimal("0")))
    spot_volatility_32 = float(_decimal(row.get("spot_realized_volatility_32") or Decimal("0")))
    spot_target_distance_volatility = float(_decimal(row.get("spot_target_distance_volatility") or Decimal("0")))
    kalshi_mid_spot_gap = float(_decimal(row.get("kalshi_mid_spot_gap") or Decimal("0")))
    spot_stale_seconds = max(0.0, float(row.get("spot_stale_seconds") or 0))
    spot_exchange_spread = max(0.0, float(row.get("spot_exchange_spread_bps") or 0))
    spot_exchange_trade_count = max(0.0, float(row.get("spot_exchange_recent_trade_count") or 0))
    market_mid_change = float(_decimal(row.get("market_mid_change_1") or Decimal("0")))
    market_mid_velocity = float(_decimal(row.get("market_mid_velocity_per_min") or Decimal("0")))
    spread_change = float(row.get("spread_change_bps_1") or 0)
    quote_observation_gap = max(0.0, float(row.get("quote_observation_gap_seconds") or 0))
    settlement_sample_count = max(0.0, float(row.get("settlement_window_sample_count") or 0))
    settlement_target_distance = float(_decimal(row.get("settlement_twap_target_distance_pct") or Decimal("0")))
    settlement_window_return = float(_decimal(row.get("settlement_window_return_pct") or Decimal("0")))
    settlement_window_volatility = float(_decimal(row.get("settlement_window_volatility") or Decimal("0")))
    market_age_seconds = max(0.0, float(row.get("market_age_seconds") or 0))
    default_values = _crypto_default_values_for_asset(asset, defaults or {})
    recent_yes = row.get("asset_recent_yes_rate")
    recent_error = row.get("asset_recent_mid_error")
    time_to_close_bucket = _crypto_time_to_close_bucket(time_to_close, row.get("frequency"))
    settlement_ts = row.get("settlement_ts")
    if isinstance(settlement_ts, datetime):
        _close_hour = _as_utc_datetime(settlement_ts).hour
        _close_dow = _as_utc_datetime(settlement_ts).weekday()
        close_hour_sin = math.sin(2 * math.pi * _close_hour / 24)
        close_hour_cos = math.cos(2 * math.pi * _close_hour / 24)
        close_dow_sin = math.sin(2 * math.pi * _close_dow / 7)
        close_dow_cos = math.cos(2 * math.pi * _close_dow / 7)
    else:
        close_hour_sin = close_hour_cos = close_dow_sin = close_dow_cos = 0.0
    numeric_values = {
        "market_mid_logit": math.log(max(1e-6, mid) / max(1e-6, 1.0 - mid)),
        "mid_yes": mid,
        "time_to_close_ratio": min(time_to_close / 900.0, 4.0),
        "execution_spread": min(spread_bps / 10000.0, 1.0),
        "volume_log": math.log1p(volume) / 12.0,
        "open_interest_log": math.log1p(open_interest) / 12.0,
        "candle_momentum": max(-0.25, min(0.25, candle_momentum)) * 4.0,
        "target_price_log": math.log1p(target_price) / 12.0 if target_price > 0 else 0.0,
        "spot_available": 1.0 if row.get("spot_feature_status") == "available" else 0.0,
        "spot_moneyness_pct": max(-0.25, min(0.25, spot_moneyness)) * 4.0,
        "spot_momentum_pct": max(-0.05, min(0.05, spot_momentum)) * 20.0,
        "spot_return_1_pct": max(-0.05, min(0.05, spot_return_1)) * 20.0,
        "spot_return_3_pct": max(-0.10, min(0.10, spot_return_3)) * 10.0,
        "spot_return_6_pct": max(-0.15, min(0.15, spot_return_6)) * (20.0 / 3.0),
        "spot_realized_volatility": max(0.0, min(0.10, spot_volatility)) * 10.0,
        "spot_target_distance_volatility": max(-8.0, min(8.0, spot_target_distance_volatility)) / 8.0,
        "kalshi_mid_spot_gap": max(-0.50, min(0.50, kalshi_mid_spot_gap)) * 2.0,
        "spot_stale_ratio": min(spot_stale_seconds / 3600.0, 6.0) / 6.0,
        "asset_recent_yes_rate_delta": float(_decimal(recent_yes)) - 0.5 if recent_yes is not None else default_values["asset_recent_yes_rate"] - 0.5,
        "asset_recent_mid_error": float(_decimal(recent_error)) if recent_error is not None else default_values["asset_recent_mid_error"],
        "quote_source_candlestick_proxy": 1.0 if row.get("quote_source") == "candlestick_close_proxy" else 0.0,
        "quote_source_snapshot_quotes": 1.0 if row.get("quote_source") in {"snapshot_quotes", "live_market_snapshot"} else 0.0,
        "strict_trade_eligible": 1.0 if row.get("strict_trade_eligible") else 0.0,
        "time_to_close_bucket_0_5m": 1.0 if time_to_close_bucket == "0_5m" else 0.0,
        "time_to_close_bucket_5_10m": 1.0 if time_to_close_bucket == "5_10m" else 0.0,
        "time_to_close_bucket_10_15m": 1.0 if time_to_close_bucket == "10_15m" else 0.0,
        "time_to_close_bucket_15m_plus": 1.0 if time_to_close_bucket == "15m_plus" else 0.0,
        "market_age_ratio": min(market_age_seconds / 900.0, 8.0) / 8.0,
        "spot_exchange_spread_bps": min(spot_exchange_spread / 200.0, 1.0),
        # Log scaling: the old min(count, 50)/50 saturated permanently for liquid
        # assets (BTC always has 50+ recent trades), flattening the feature to a
        # constant. log1p keeps low-end resolution and ceilings at ~500 trades.
        "spot_exchange_recent_trade_count": min(math.log1p(spot_exchange_trade_count) / math.log1p(500.0), 1.0),
        "spot_return_12_pct": max(-0.20, min(0.20, spot_return_12)) * 5.0,
        "spot_return_24_pct": max(-0.30, min(0.30, spot_return_24)) * (10.0 / 3.0),
        "spot_realized_volatility_32": max(0.0, min(0.20, spot_volatility_32)) * 5.0,
        "market_mid_change_1": max(-0.25, min(0.25, market_mid_change)) * 4.0,
        "market_mid_velocity_per_min": max(-0.25, min(0.25, market_mid_velocity)) * 4.0,
        "spread_change_bps_1": max(-500.0, min(500.0, spread_change)) / 500.0,
        "quote_observation_gap_ratio": min(quote_observation_gap / 900.0, 4.0) / 4.0,
        "settlement_window_available": 1.0 if row.get("settlement_window_status") == "available" else 0.0,
        "settlement_window_sample_count": min(math.log1p(settlement_sample_count) / math.log1p(60), 1.0),
        "settlement_twap_target_distance_pct": max(-0.25, min(0.25, settlement_target_distance)) * 4.0,
        "settlement_window_return_pct": max(-0.10, min(0.10, settlement_window_return)) * 10.0,
        "settlement_window_volatility": max(0.0, min(0.10, settlement_window_volatility)) * 10.0,
        "close_hour_sin": close_hour_sin,
        "close_hour_cos": close_hour_cos,
        "close_dow_sin": close_dow_sin,
        "close_dow_cos": close_dow_cos,
    }
    for cross_asset in CRYPTO_CROSS_ASSET_FEATURE_ASSETS:
        key = cross_asset.lower()
        return_1 = float(_decimal(row.get(f"{key}_return_1_pct") or Decimal("0")))
        return_3 = float(_decimal(row.get(f"{key}_return_3_pct") or Decimal("0")))
        numeric_values[f"{key}_return_1_pct"] = max(-0.05, min(0.05, return_1)) * 20.0
        numeric_values[f"{key}_return_3_pct"] = max(-0.10, min(0.10, return_3)) * 10.0
    # Funding rate features: already computed and stored in every training/inference row
    # but were missing from v5 schema. OKX 8h rates are ~0.0001–0.0003; scale to [-1, 1].
    funding_current = float(_decimal(row.get("funding_rate_current") or Decimal("0")))
    funding_delta = float(_decimal(row.get("funding_rate_delta") or Decimal("0")))
    numeric_values["funding_rate_current"] = max(-1.0, min(1.0, funding_current / 0.003))
    numeric_values["funding_rate_delta"] = max(-1.0, min(1.0, funding_delta / 0.002))
    # v6 features: bid pressure, spread vs vol, session indicators
    yes_bid = float(_decimal(row.get("yes_bid_dollars") or Decimal("0")))
    bid_pressure = (yes_bid / mid) if mid > 1e-6 else 0.5
    numeric_values["bid_pressure"] = max(0.0, min(2.0, bid_pressure)) - 1.0
    spread_vs_vol = (spread_bps / 10000.0) / max(spot_volatility, 1e-6) if spot_volatility > 0 else 0.0
    numeric_values["spread_vs_vol_ratio"] = max(0.0, min(10.0, spread_vs_vol)) / 10.0
    if isinstance(settlement_ts, datetime):
        _close_hour_utc = _as_utc_datetime(settlement_ts).hour + _as_utc_datetime(settlement_ts).minute / 60.0
        numeric_values["is_us_session"] = 1.0 if 13.5 <= _close_hour_utc < 20.0 else 0.0
        numeric_values["is_asia_session"] = 1.0 if _close_hour_utc < 8.0 or _close_hour_utc >= 23.0 else 0.0
    else:
        numeric_values["is_us_session"] = 0.0
        numeric_values["is_asia_session"] = 0.0
    values: list[float] = [numeric_values[name] for name in schema.get("numeric_feature_names") or []]
    # Emit one-hots for exactly the asset columns the schema's feature_names carry:
    # new single-asset schemas prune the constant lone one-hot, while artifacts
    # trained before the pruning still list (and expect) it.
    feature_name_set = set(schema.get("feature_names") or [])
    values.extend(
        1.0 if asset == category else 0.0
        for category in schema.get("asset_categories") or []
        if f"asset={category}" in feature_name_set
    )
    return values


def _crypto_time_to_close_bucket(seconds: float, frequency: str | None = None) -> str:
    if normalize_frequency(frequency) == "1h":
        if seconds <= 300:
            return "0_5m"
        if seconds <= 900:
            return "5_15m"
        if seconds <= 1800:
            return "15_30m"
        if seconds <= 2700:
            return "30_45m"
        if seconds <= 3600:
            return "45_60m"
        return "60m_plus"
    if seconds <= 300:
        return "0_5m"
    if seconds <= 600:
        return "5_10m"
    if seconds <= 900:
        return "10_15m"
    return "15m_plus"


def _crypto_spot_distance_band(row: dict[str, Any]) -> str:
    value = _crypto_spot_distance_signal_value(row)
    if value is None:
        return "missing"
    score = float(value)
    if score <= -2.0:
        return "far_below"
    if score < -0.5:
        return "below"
    if score <= 0.5:
        return "near"
    if score < 2.0:
        return "above"
    return "far_above"


def _crypto_market_age_seconds(decision_ts: datetime, open_time: datetime | None) -> int | None:
    if open_time is None:
        return None
    return max(0, int((_as_utc_datetime(decision_ts) - _as_utc_datetime(open_time)).total_seconds()))


def _crypto_default_values_for_asset(asset: str, defaults: dict[str, Any]) -> dict[str, float]:
    global_defaults = defaults.get("global") if isinstance(defaults.get("global"), dict) else {}
    by_asset = defaults.get("by_asset") if isinstance(defaults.get("by_asset"), dict) else {}
    asset_defaults = by_asset.get(asset) if isinstance(by_asset.get(asset), dict) else {}
    return {
        "asset_recent_yes_rate": float(asset_defaults.get("asset_recent_yes_rate", global_defaults.get("asset_recent_yes_rate", 0.5))),
        "asset_recent_mid_error": float(asset_defaults.get("asset_recent_mid_error", global_defaults.get("asset_recent_mid_error", 0.0))),
    }


def _crypto_training_cutoff(rows: list[dict[str, Any]]) -> dict[str, Any]:
    min_decision = min((row.get("decision_ts") for row in rows if row.get("decision_ts")), default=None)
    max_decision = max((row.get("decision_ts") for row in rows if row.get("decision_ts")), default=None)
    return {
        "min_decision_ts": min_decision.isoformat() if isinstance(min_decision, datetime) else None,
        "max_decision_ts": max_decision.isoformat() if isinstance(max_decision, datetime) else None,
        "row_count": len(rows),
    }


def _fit_crypto_heuristic_calibration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "model_type": "heuristic_adjustment",
            "global_adjustment_bps": 0,
            "asset_adjustments_bps": {},
            "feature_weights": {},
        }
    outcome_avg = sum((Decimal(row["label_yes"]) for row in rows), Decimal("0")) / len(rows)
    mid_avg = sum((_decimal(row["mid_yes_dollars"]) for row in rows), Decimal("0")) / len(rows)
    global_adjustment = outcome_avg - mid_avg
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_asset[str(row["asset_symbol"])].append(row)
    asset_adjustments: dict[str, int] = {}
    for asset, asset_rows in by_asset.items():
        asset_outcome = sum((Decimal(row["label_yes"]) for row in asset_rows), Decimal("0")) / len(asset_rows)
        asset_mid = sum((_decimal(row["mid_yes_dollars"]) for row in asset_rows), Decimal("0")) / len(asset_rows)
        asset_adjustments[asset] = int(((asset_outcome - asset_mid - global_adjustment) * Decimal("10000")).to_integral_value())
    return {
        "model_type": "heuristic_adjustment",
        "global_adjustment_bps": int((global_adjustment * Decimal("10000")).to_integral_value()),
        "asset_adjustments_bps": asset_adjustments,
        "feature_weights": {
            "candlestick_momentum": 0.25,
            "spread_penalty_bps_per_100bps": -8,
            "time_to_close_decay": 0.10,
        },
    }


def _package_version(package: str) -> str | None:
    try:
        return importlib_metadata.version(package)
    except importlib_metadata.PackageNotFoundError:
        return None
    except Exception:
        return None


def _fit_crypto_calibration(
    rows: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    include_candidate_report: bool = True,
) -> dict[str, Any]:
    fallback = _fit_crypto_heuristic_calibration(rows)
    if not rows:
        return fallback
    labels = [int(row["label_yes"]) for row in rows]
    if len(set(labels)) < 2:
        return {**fallback, "fallback_reason": "single_class_training_rows"}

    schema = _crypto_feature_schema(rows)
    defaults = _crypto_feature_defaults(rows)
    candidates = _fit_crypto_model_candidates(rows, schema=schema, defaults=defaults, fallback=fallback, settings=settings)
    candidate_report = (
        _crypto_model_candidate_report(
            rows,
            settings=settings,
            crypto_policy=crypto_policy,
            full_candidate_status=candidates,
        )
        if include_candidate_report
        else _crypto_in_sample_candidate_report(rows, candidates, settings=settings, crypto_policy=crypto_policy)
    )
    champion_name = str(candidate_report.get("champion_name") or "sklearn_logistic")
    if champion_name == "calibrated_weighted_ensemble":
        member_models = {
            name: dict(candidates[name]["model"])
            for name in (candidate_report.get("ensemble_weights") or {})
            if candidates.get(name, {}).get("status") == "available" and candidates[name].get("model") is not None
        }
        if member_models:
            return {
                "model_type": "calibrated_weighted_ensemble",
                "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                "feature_names": schema["feature_names"],
                "numeric_feature_names": schema["numeric_feature_names"],
                "asset_categories": schema["asset_categories"],
                "positive_label": "yes",
                "ensemble_weights": dict(candidate_report.get("ensemble_weights") or {}),
                "member_models": member_models,
                "fallback_model": fallback,
                "feature_defaults": defaults,
                "candidate_report": candidate_report,
                "training_cutoff": _crypto_training_cutoff(rows),
            }
    if candidates.get(champion_name, {}).get("status") == "available" and candidates[champion_name].get("model") is not None:
        model = dict(candidates[champion_name]["model"])
        model["candidate_report"] = candidate_report
        return model
    for fallback_name in ("sklearn_logistic", "market_mid_baseline"):
        if candidates.get(fallback_name, {}).get("status") == "available" and candidates[fallback_name].get("model") is not None:
            model = dict(candidates[fallback_name]["model"])
            model["candidate_report"] = {
                **candidate_report,
                "champion_fallback_reason": f"selected_champion_unavailable:{champion_name}",
            }
            return model
    return {**fallback, "fallback_reason": "no_candidate_model_available", "candidate_report": candidate_report}


def _fit_crypto_model_candidates(
    rows: list[dict[str, Any]],
    *,
    schema: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
    fallback: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, dict[str, Any]]:
    schema = schema or _crypto_feature_schema(rows)
    defaults = defaults or _crypto_feature_defaults(rows)
    fallback = fallback or _fit_crypto_heuristic_calibration(rows)
    labels = [int(row["label_yes"]) for row in rows]
    raw_matrix = [_crypto_raw_feature_vector(row, schema, defaults=defaults) for row in rows]
    result: dict[str, dict[str, Any]] = {
        "market_mid_baseline": {
            "name": "market_mid_baseline",
            "status": "available",
            "model": _market_mid_crypto_model(schema=schema, defaults=defaults, fallback=fallback, rows=rows),
            "dependency_version": None,
        },
        "current_heuristic": {
            "name": "current_heuristic",
            "status": "available",
            "model": {**fallback, "model_type": "current_heuristic", "training_cutoff": _crypto_training_cutoff(rows)},
            "dependency_version": None,
        },
        "spot_distance_residual": {
            "name": "spot_distance_residual",
            "status": "available",
            "model": _fit_crypto_spot_distance_residual_model(rows, fallback=fallback),
            "dependency_version": None,
        },
        "spot_distance_contrarian": {
            "name": "spot_distance_contrarian",
            "status": "available",
            "model": _fit_crypto_spot_distance_contrarian_model(rows, fallback=fallback),
            "dependency_version": None,
        },
        "spot_distance_contrarian_gated": {
            "name": "spot_distance_contrarian_gated",
            "status": "available",
            "model": _fit_crypto_spot_distance_contrarian_gated_model(rows, fallback=fallback),
            "dependency_version": None,
        },
        "asset_time_calibration": {
            "name": "asset_time_calibration",
            "status": "available",
            "model": _fit_crypto_asset_time_calibration_model(rows, fallback=fallback),
            "dependency_version": None,
        },
    }
    if not rows or len(set(labels)) < 2:
        reason = "need_two_outcome_classes"
        for name in ("sklearn_logistic", "xgboost_classifier", "lightgbm_classifier"):
            result[name] = {"name": name, "status": "unavailable", "reason": reason, "dependency_version": None}
        return result
    def _fit_logistic() -> dict[str, Any]:
        return _fit_crypto_logistic_model(rows, raw_matrix, labels, schema=schema, defaults=defaults, fallback=fallback)

    def _fit_xgboost() -> dict[str, Any]:
        return _fit_crypto_xgboost_model(rows, raw_matrix, labels, schema=schema, defaults=defaults, fallback=fallback, settings=settings)

    def _fit_lightgbm() -> dict[str, Any]:
        return _fit_crypto_lightgbm_model(rows, raw_matrix, labels, schema=schema, defaults=defaults, fallback=fallback, settings=settings)

    if _crypto_resolved_xgb_device(len(labels)) in _GPU_TREE_DEVICES:
        # XGBoost trains on the GPU while logistic + LightGBM occupy the CPU
        # cores; all three fits release the GIL, so overlapping wastes neither
        # device. Kept sequential on CPU-only hosts to avoid oversubscribing
        # cores between two OpenMP fits.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="crypto-xgb-fit") as pool:
            xgboost_future = pool.submit(_fit_xgboost)
            logistic_result = _fit_logistic()
            lightgbm_result = _fit_lightgbm()
            xgboost_result = xgboost_future.result()
    else:
        logistic_result = _fit_logistic()
        xgboost_result = _fit_xgboost()
        lightgbm_result = _fit_lightgbm()
    result["sklearn_logistic"] = logistic_result
    result["xgboost_classifier"] = xgboost_result
    result["lightgbm_classifier"] = lightgbm_result
    return result


def _market_mid_crypto_model(
    *,
    schema: dict[str, Any],
    defaults: dict[str, Any],
    fallback: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "model_type": "market_mid_baseline",
        "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
        "feature_names": schema["feature_names"],
        "numeric_feature_names": schema["numeric_feature_names"],
        "asset_categories": schema["asset_categories"],
        "positive_label": "yes",
        "feature_defaults": defaults,
        "fallback_model": fallback,
        "training_cutoff": _crypto_training_cutoff(rows),
    }


def _crypto_calibration_training_and_holdout_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    if len(rows) >= 2000:
        split_idx = max(1, min(len(rows) - 1, int(len(rows) * 0.85)))
        return rows[:split_idx], rows[split_idx:], "chronological_holdout"
    return rows, rows, "in_sample_small_sample"


def _fit_crypto_spot_distance_residual_model(rows: list[dict[str, Any]], *, fallback: dict[str, Any]) -> dict[str, Any]:
    fit_rows, calibration_rows, calibration_scope = _crypto_calibration_training_and_holdout_rows(rows)
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for row in fit_rows:
        key = "|".join([str(row.get("asset_symbol") or "unknown"), _crypto_spot_distance_band(row)])
        grouped[key].append(Decimal(int(row["label_yes"])) - _decimal(row.get("mid_yes_dollars")))
    adjustments = {
        key: int(((sum(values, Decimal("0")) / Decimal(len(values))) * Decimal("10000")).to_integral_value())
        for key, values in grouped.items()
        if values
    }
    model = {
        "model_type": "spot_distance_residual",
        "bucket_adjustments_bps": adjustments,
        "fallback_model": fallback,
        "calibration_scope": calibration_scope,
        "calibration_fit_rows": len(fit_rows),
        "calibration_holdout_rows": len(calibration_rows) if calibration_scope == "chronological_holdout" else 0,
        "training_cutoff": _crypto_training_cutoff(rows),
    }
    model["probability_calibration"] = _fit_probability_calibration(
        [_predict_crypto_probability(row, model, apply_calibration=False) for row in calibration_rows],
        [int(row["label_yes"]) for row in calibration_rows],
    )
    return model


def _fit_crypto_spot_distance_contrarian_model(rows: list[dict[str, Any]], *, fallback: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_type": "spot_distance_contrarian",
        "fallback_model": fallback,
        "training_cutoff": _crypto_training_cutoff(rows),
        "positive_distance_yes_probability": "0.0100",
        "negative_distance_yes_probability": "0.9900",
    }


def _fit_crypto_spot_distance_contrarian_gated_model(rows: list[dict[str, Any]], *, fallback: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_type": "spot_distance_contrarian_gated",
        "fallback_model": fallback,
        "training_cutoff": _crypto_training_cutoff(rows),
        "positive_distance_yes_probability": "0.1000",
        "negative_distance_yes_probability": "0.9000",
        "activation_max_spread_bps": 1500,
        "activation_min_contract_price_dollars": "0.2500",
        "activation_max_entry_price_dollars": "0.6500",
        "activation_min_remaining_payout_bps": 300,
        "activation_requires_strict_trade_eligible": True,
        "activation_requires_snapshot_quotes": True,
        "activation_requires_spot_features": True,
    }


def _fit_crypto_asset_time_calibration_model(rows: list[dict[str, Any]], *, fallback: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for row in rows:
        bucket = _crypto_time_to_close_bucket(float(row.get("time_to_close_seconds") or 0))
        key = "|".join([str(row.get("asset_symbol") or "unknown"), bucket])
        grouped[key].append(Decimal(int(row["label_yes"])) - _decimal(row.get("mid_yes_dollars")))
    adjustments = {
        key: int(((sum(values, Decimal("0")) / Decimal(len(values))) * Decimal("10000")).to_integral_value())
        for key, values in grouped.items()
        if values
    }
    return {
        "model_type": "asset_time_calibration",
        "bucket_adjustments_bps": adjustments,
        "fallback_model": fallback,
        "training_cutoff": _crypto_training_cutoff(rows),
    }


def _fit_crypto_logistic_model(
    rows: list[dict[str, Any]],
    raw_matrix: list[list[float]],
    labels: list[int],
    *,
    schema: dict[str, Any],
    defaults: dict[str, Any],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    try:
        import sklearn
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:  # pragma: no cover - dependency failures are surfaced in artifact payload.
        return {"name": "sklearn_logistic", "status": "unavailable", "reason": f"sklearn_unavailable:{exc}", "dependency_version": None}
    try:
        scaler = StandardScaler()
        fit_rows, calibration_rows, calibration_scope = _crypto_calibration_training_and_holdout_rows(rows)
        fit_count = len(fit_rows)
        fit_matrix = raw_matrix[:fit_count]
        fit_labels = labels[:fit_count]
        if len(set(fit_labels)) < 2:
            fit_rows = rows
            calibration_rows = rows
            calibration_scope = "in_sample_holdout_single_class_fallback"
            fit_matrix = raw_matrix
            fit_labels = labels
        scaled = scaler.fit_transform(fit_matrix)
        classifier = LogisticRegression(
            C=0.75,
            class_weight="balanced",
            max_iter=1000,
            random_state=17,
            solver="lbfgs",
        )
        classifier.fit(scaled, fit_labels)
        model = {
            "model_type": "sklearn_logistic",
            "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            "feature_names": schema["feature_names"],
            "numeric_feature_names": schema["numeric_feature_names"],
            "asset_categories": schema["asset_categories"],
            "scaler": {
                "mean": [float(value) for value in scaler.mean_],
                "scale": [float(value) if float(value) != 0.0 else 1.0 for value in scaler.scale_],
            },
            "coefficients": [float(value) for value in classifier.coef_[0]],
            "intercept": float(classifier.intercept_[0]),
            "positive_label": "yes",
            "sklearn": {
                "version": sklearn.__version__,
                "estimator": "LogisticRegression",
                "solver": "lbfgs",
                "class_weight": "balanced",
                "random_state": 17,
            },
            "feature_defaults": defaults,
            "fallback_model": fallback,
            "calibration_scope": calibration_scope,
            "calibration_fit_rows": len(fit_rows),
            "calibration_holdout_rows": len(calibration_rows) if calibration_scope == "chronological_holdout" else 0,
            "training_cutoff": _crypto_training_cutoff(rows),
        }
        model["probability_calibration"] = _fit_probability_calibration(
            [_predict_crypto_probability(row, model, apply_calibration=False) for row in calibration_rows],
            [int(row["label_yes"]) for row in calibration_rows],
        )
        return {"name": "sklearn_logistic", "status": "available", "model": model, "dependency_version": sklearn.__version__}
    except Exception as exc:
        return {"name": "sklearn_logistic", "status": "unavailable", "reason": f"sklearn_fit_failed:{exc}", "dependency_version": _package_version("scikit-learn")}


_GPU_TREE_DEVICES: frozenset[str] = frozenset({"cuda", "gpu"})


def _crypto_bootstrap_observed_at(market: "CryptoMarket", *, now: datetime | None = None) -> datetime:
    current = now or datetime.now(UTC)
    close_time = market.close_time
    if close_time is None:
        return current
    return min(close_time, current)


def _resolve_tree_device(
    requested: str, n_rows: int, *, gpu_min_rows: int
) -> tuple[str, str | None]:
    requested = (requested or "").strip().lower() or "cpu"
    if requested in _GPU_TREE_DEVICES and gpu_min_rows > 0 and n_rows < gpu_min_rows:
        return "cpu", f"rows={n_rows} below gpu_min_rows={gpu_min_rows}"
    return requested, None


def _crypto_resolved_xgb_device(n_rows: int) -> str:
    requested = os.environ.get("CRYPTO_XGBOOST_DEVICE", "cpu").lower()
    gpu_min_rows = int(os.environ.get("CRYPTO_GPU_MIN_ROWS", "20000") or 20000)
    device, _ = _resolve_tree_device(requested, n_rows, gpu_min_rows=gpu_min_rows)
    return device


def _fit_tree_with_device_fallback(
    build_and_fit: "Callable[[str], Any]", device: str, *, model_label: str
) -> tuple["Any", str]:
    """Run build_and_fit(device); on a GPU failure, retry once on CPU."""
    try:
        return build_and_fit(device), device
    except Exception as exc:
        if device not in _GPU_TREE_DEVICES:
            raise
        logger.warning(
            "crypto_%s_gpu_fit_failed device=%s falling back to cpu: %s",
            model_label,
            device,
            exc,
        )
        return build_and_fit("cpu"), "cpu"


def _fit_crypto_xgboost_model(
    rows: list[dict[str, Any]],
    raw_matrix: list[list[float]],
    labels: list[int],
    *,
    schema: dict[str, Any],
    defaults: dict[str, Any],
    fallback: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    try:
        import xgboost as xgb
    except Exception as exc:
        return {"name": "xgboost_classifier", "status": "unavailable", "reason": f"xgboost_unavailable:{exc}", "dependency_version": None}
    try:
        _xgb_requested = os.environ.get("CRYPTO_XGBOOST_DEVICE", "cpu").lower()
        _xgb_gpu_min_rows = int(os.environ.get("CRYPTO_GPU_MIN_ROWS", "20000") or 20000)
        _xgb_device, _xgb_downgrade = _resolve_tree_device(_xgb_requested, len(labels), gpu_min_rows=_xgb_gpu_min_rows)
        if _xgb_downgrade:
            logger.debug("crypto_xgboost_device_downgraded: %s", _xgb_downgrade)
        _xgb_min_child = 20 if len(raw_matrix) > 20000 else 10 if len(raw_matrix) > 5000 else 5
        # Fit on full dataset; hold out most-recent 15% for isotonic calibration when
        # there's enough data — prevents in-sample calibration overfitting.
        _cal_split_idx = int(len(raw_matrix) * 0.85) if len(raw_matrix) >= 2000 else len(raw_matrix)
        _sample_weights = _crypto_training_sample_weights(rows[:_cal_split_idx], settings=settings)

        def _build_and_fit_xgb(device: str) -> Any:
            kwargs: dict[str, Any] = {"tree_method": "hist"}
            if device not in ("", "cpu"):
                kwargs["device"] = device
            fitted = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_weight=_xgb_min_child,
                reg_lambda=1.5,
                eval_metric="logloss",
                random_state=17,
                n_jobs=-1,
                **kwargs,
            )
            fitted.fit(raw_matrix[:_cal_split_idx], labels[:_cal_split_idx], sample_weight=_sample_weights)
            return fitted

        classifier, _xgb_device = _fit_tree_with_device_fallback(_build_and_fit_xgb, _xgb_device, model_label="xgboost")
        booster = classifier.get_booster()
        try:
            raw_booster = booster.save_raw(raw_format="json")
        except TypeError:
            raw_booster = booster.save_raw()
        raw_bytes = raw_booster if isinstance(raw_booster, bytes) else str(raw_booster).encode("utf-8")
        model = {
            "model_type": "xgboost_classifier",
            "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            "feature_names": schema["feature_names"],
            "numeric_feature_names": schema["numeric_feature_names"],
            "asset_categories": schema["asset_categories"],
            "booster_raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "positive_label": "yes",
            "xgboost": {
                "version": getattr(xgb, "__version__", None),
                "estimator": "XGBClassifier",
                "random_state": 17,
                "tree_method": "hist",
                "device": _xgb_device,
            },
            "feature_defaults": defaults,
            "fallback_model": fallback,
            "training_cutoff": _crypto_training_cutoff(rows),
        }
        _cal_rows = rows[_cal_split_idx:] if _cal_split_idx < len(rows) else rows
        _cal_labels = labels[_cal_split_idx:] if _cal_split_idx < len(labels) else labels
        model["probability_calibration"] = _fit_probability_calibration(
            _xgboost_predict_batch(_cal_rows, model, apply_calibration=False),
            _cal_labels,
        )
        return {"name": "xgboost_classifier", "status": "available", "model": model, "dependency_version": getattr(xgb, "__version__", None)}
    except Exception as exc:
        return {"name": "xgboost_classifier", "status": "unavailable", "reason": f"xgboost_fit_failed:{exc}", "dependency_version": _package_version("xgboost") or _package_version("xgboost-cpu")}


def _fit_crypto_lightgbm_model(
    rows: list[dict[str, Any]],
    raw_matrix: list[list[float]],
    labels: list[int],
    *,
    schema: dict[str, Any],
    defaults: dict[str, Any],
    fallback: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    try:
        import lightgbm as lgb
    except Exception as exc:
        return {"name": "lightgbm_classifier", "status": "unavailable", "reason": f"lightgbm_unavailable:{exc}", "dependency_version": None}
    try:
        _lgb_n_jobs = int(os.environ.get("CRYPTO_LIGHTGBM_N_JOBS", "-1"))
        _lgb_requested = os.environ.get("CRYPTO_LIGHTGBM_DEVICE", "cpu")
        # gpu_min_rows=0: no size gate for LightGBM — honor operator request as-is.
        _lgb_device, _ = _resolve_tree_device(_lgb_requested, len(labels), gpu_min_rows=0)
        _lgb_min_child = 20 if len(raw_matrix) > 20000 else 10 if len(raw_matrix) > 5000 else 5
        _cal_split_idx = int(len(raw_matrix) * 0.85) if len(raw_matrix) >= 2000 else len(raw_matrix)
        _sample_weights = _crypto_training_sample_weights(rows[:_cal_split_idx], settings=settings)

        def _build_and_fit_lgb(device: str) -> Any:
            fitted = lgb.LGBMClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.05,
                num_leaves=15,
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_samples=_lgb_min_child,
                reg_lambda=1.5,
                random_state=17,
                n_jobs=_lgb_n_jobs,
                device=device,
                verbosity=-1,
            )
            fitted.fit(raw_matrix[:_cal_split_idx], labels[:_cal_split_idx], sample_weight=_sample_weights)
            return fitted

        classifier, _lgb_device = _fit_tree_with_device_fallback(_build_and_fit_lgb, _lgb_device, model_label="lightgbm")
        booster = classifier.booster_
        model = {
            "model_type": "lightgbm_classifier",
            "feature_schema_version": CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
            "feature_names": schema["feature_names"],
            "numeric_feature_names": schema["numeric_feature_names"],
            "asset_categories": schema["asset_categories"],
            "booster_model_string": booster.model_to_string(),
            "positive_label": "yes",
            "lightgbm": {
                "version": getattr(lgb, "__version__", None),
                "estimator": "LGBMClassifier",
                "random_state": 17,
                "device": _lgb_device,
            },
            "feature_defaults": defaults,
            "fallback_model": fallback,
            "training_cutoff": _crypto_training_cutoff(rows),
        }
        _cal_rows = rows[_cal_split_idx:] if _cal_split_idx < len(rows) else rows
        _cal_labels = labels[_cal_split_idx:] if _cal_split_idx < len(labels) else labels
        model["probability_calibration"] = _fit_probability_calibration(
            _lightgbm_predict_batch(_cal_rows, model, apply_calibration=False),
            _cal_labels,
        )
        return {"name": "lightgbm_classifier", "status": "available", "model": model, "dependency_version": getattr(lgb, "__version__", None)}
    except Exception as exc:
        return {"name": "lightgbm_classifier", "status": "unavailable", "reason": f"lightgbm_fit_failed:{exc}", "dependency_version": _package_version("lightgbm")}


def _fit_probability_calibration(predictions: list[Decimal], labels: list[int]) -> dict[str, Any] | None:
    if len(predictions) < 12 or len(set(labels)) < 2 or len({str(value) for value in predictions}) < 3:
        return None
    try:
        from sklearn.isotonic import IsotonicRegression
    except Exception:
        return None
    try:
        x_values = [float(value) for value in predictions]
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(x_values, labels)
        return {
            "method": "isotonic",
            "sample_count": len(predictions),
            "thresholds_x": [float(value) for value in calibrator.X_thresholds_],
            "thresholds_y": [float(value) for value in calibrator.y_thresholds_],
        }
    except Exception:
        return None


def _apply_probability_calibration(probability: Decimal, calibration: dict[str, Any] | None) -> Decimal:
    if not calibration or calibration.get("method") != "isotonic":
        return _clamp_price(probability)
    xs = [float(value) for value in calibration.get("thresholds_x") or []]
    ys = [float(value) for value in calibration.get("thresholds_y") or []]
    if len(xs) != len(ys) or not xs:
        return _clamp_price(probability)
    value = float(probability)
    if value <= xs[0]:
        return _clamp_price(Decimal(str(ys[0])))
    if value >= xs[-1]:
        return _clamp_price(Decimal(str(ys[-1])))
    for idx in range(1, len(xs)):
        if value <= xs[idx]:
            left_x = xs[idx - 1]
            right_x = xs[idx]
            left_y = ys[idx - 1]
            right_y = ys[idx]
            if right_x == left_x:
                return _clamp_price(Decimal(str(right_y)))
            ratio = (value - left_x) / (right_x - left_x)
            return _clamp_price(Decimal(str(left_y + (right_y - left_y) * ratio)))
    return _clamp_price(probability)


def _crypto_spot_distance_contrarian_gate_active(
    row: dict[str, Any],
    model: dict[str, Any],
    *,
    distance: Decimal,
) -> bool:
    if bool(model.get("activation_requires_strict_trade_eligible", True)) and row.get("strict_trade_eligible") is False:
        return False
    if bool(model.get("activation_requires_snapshot_quotes", True)):
        quote_source = str(row.get("quote_source") or "").strip().lower()
        if quote_source and quote_source != "snapshot_quotes":
            return False
    if bool(model.get("activation_requires_spot_features", True)):
        spot_status = str(row.get("spot_feature_status") or "").strip().lower()
        if spot_status != "available":
            return False
        if bool(row.get("spot_proxy_only")) or _crypto_spot_is_proxy(row.get("spot_provider"), row.get("spot_source_kind")):
            return False
    spread_bps = _optional_int(row.get("spread_bps"))
    max_spread_bps = _optional_int(model.get("activation_max_spread_bps")) or 0
    if spread_bps is None or (max_spread_bps > 0 and spread_bps > max_spread_bps):
        return False
    side = "no" if distance >= Decimal("0") else "yes"
    cost = _crypto_side_ask(row, side)
    if cost is None:
        return False
    min_contract_price = _decimal(model.get("activation_min_contract_price_dollars") or Decimal("0"))
    max_entry_price = _decimal(model.get("activation_max_entry_price_dollars") or Decimal("0"))
    min_remaining_payout = Decimal(_optional_int(model.get("activation_min_remaining_payout_bps")) or 0) / Decimal("10000")
    if min_contract_price > 0 and cost < min_contract_price:
        return False
    if max_entry_price > 0 and cost > max_entry_price:
        return False
    if min_remaining_payout > 0 and Decimal("1.0000") - cost < min_remaining_payout:
        return False
    return True


def _predict_crypto_probability(
    row: dict[str, Any],
    model: dict[str, Any] | None,
    *,
    apply_calibration: bool = True,
) -> Decimal:
    mid = _decimal(row.get("mid_yes_dollars"))
    if not model:
        return _clamp_price(mid)
    model_type = model.get("model_type")
    if model_type == "market_mid_baseline":
        return _clamp_price(mid)
    if model_type == "spot_distance_residual":
        key = "|".join([str(row.get("asset_symbol") or "unknown"), _crypto_spot_distance_band(row)])
        adjustment = Decimal(int((model.get("bucket_adjustments_bps") or {}).get(key, 0))) / Decimal("10000")
        probability = _clamp_price(_predict_crypto_probability(row, model.get("fallback_model")) + adjustment)
        return _apply_probability_calibration(probability, model.get("probability_calibration")) if apply_calibration else probability
    if model_type == "spot_distance_contrarian":
        distance = _crypto_spot_distance_signal_value(row)
        if distance is None:
            return _predict_crypto_probability(row, model.get("fallback_model"))
        if distance >= Decimal("0"):
            return _clamp_price(_decimal(model.get("positive_distance_yes_probability") or Decimal("0.0100")))
        return _clamp_price(_decimal(model.get("negative_distance_yes_probability") or Decimal("0.9900")))
    if model_type == "spot_distance_contrarian_gated":
        distance = _crypto_spot_distance_signal_value(row)
        if distance is None or not _crypto_spot_distance_contrarian_gate_active(row, model, distance=distance):
            return _predict_crypto_probability(row, model.get("fallback_model"))
        if distance >= Decimal("0"):
            return _clamp_price(_decimal(model.get("positive_distance_yes_probability") or Decimal("0.1000")))
        return _clamp_price(_decimal(model.get("negative_distance_yes_probability") or Decimal("0.9000")))
    if model_type == "asset_time_calibration":
        bucket = _crypto_time_to_close_bucket(float(row.get("time_to_close_seconds") or 0))
        key = "|".join([str(row.get("asset_symbol") or "unknown"), bucket])
        adjustment = Decimal(int((model.get("bucket_adjustments_bps") or {}).get(key, 0))) / Decimal("10000")
        return _clamp_price(_predict_crypto_probability(row, model.get("fallback_model")) + adjustment)
    if model_type == "calibrated_weighted_ensemble":
        try:
            weights = {str(name): float(weight) for name, weight in (model.get("ensemble_weights") or {}).items()}
            members = model.get("member_models") or {}
            total_weight = sum(weight for name, weight in weights.items() if name in members)
            if total_weight <= 0:
                return _predict_crypto_probability(row, model.get("fallback_model"))
            probability = Decimal("0")
            for name, weight in weights.items():
                if name not in members:
                    continue
                probability += _predict_crypto_probability(row, members[name]) * Decimal(str(weight / total_weight))
            return _clamp_price(probability)
        except Exception:
            return _predict_crypto_probability(row, model.get("fallback_model"))
    if model_type == "sklearn_logistic":
        try:
            schema = {
                "feature_names": list(model.get("feature_names") or []),
                "numeric_feature_names": list(model.get("numeric_feature_names") or []),
                "asset_categories": list(model.get("asset_categories") or []),
            }
            raw = _crypto_raw_feature_vector(row, schema, defaults=model.get("feature_defaults") or {})
            scaler = model.get("scaler") or {}
            means = [float(value) for value in scaler.get("mean") or []]
            scales = [float(value) or 1.0 for value in scaler.get("scale") or []]
            coefficients = [float(value) for value in model.get("coefficients") or []]
            if not raw or len(raw) != len(coefficients) or len(means) != len(raw) or len(scales) != len(raw):
                return _predict_crypto_probability(row, model.get("fallback_model"))
            logit = float(model.get("intercept") or 0.0)
            for value, mean, scale, coefficient in zip(raw, means, scales, coefficients, strict=True):
                logit += ((value - mean) / scale) * coefficient
            probability = _clamp_price(Decimal(str(1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, logit)))))))
            return _apply_probability_calibration(probability, model.get("probability_calibration")) if apply_calibration else probability
        except Exception:
            return _predict_crypto_probability(row, model.get("fallback_model"))
    if model_type == "xgboost_classifier":
        try:
            import xgboost as xgb

            schema = {
                "feature_names": list(model.get("feature_names") or []),
                "numeric_feature_names": list(model.get("numeric_feature_names") or []),
                "asset_categories": list(model.get("asset_categories") or []),
            }
            raw = _crypto_raw_feature_vector(row, schema, defaults=model.get("feature_defaults") or {})
            booster = xgb.Booster()
            booster.load_model(bytearray(base64.b64decode(str(model.get("booster_raw_base64") or ""))))
            probability = _clamp_price(Decimal(str(float(booster.predict(xgb.DMatrix([raw]))[0]))))
            return _apply_probability_calibration(probability, model.get("probability_calibration")) if apply_calibration else probability
        except Exception:
            return _predict_crypto_probability(row, model.get("fallback_model"))
    if model_type == "lightgbm_classifier":
        try:
            import lightgbm as lgb

            schema = {
                "feature_names": list(model.get("feature_names") or []),
                "numeric_feature_names": list(model.get("numeric_feature_names") or []),
                "asset_categories": list(model.get("asset_categories") or []),
            }
            raw = _crypto_raw_feature_vector(row, schema, defaults=model.get("feature_defaults") or {})
            booster = lgb.Booster(model_str=str(model.get("booster_model_string") or ""))
            probability = _clamp_price(Decimal(str(float(booster.predict([raw])[0]))))
            return _apply_probability_calibration(probability, model.get("probability_calibration")) if apply_calibration else probability
        except Exception:
            return _predict_crypto_probability(row, model.get("fallback_model"))
    adjustment = Decimal(int(model.get("global_adjustment_bps") or 0)) / Decimal("10000")
    adjustment += Decimal(int((model.get("asset_adjustments_bps") or {}).get(str(row.get("asset_symbol")), 0))) / Decimal("20000")
    momentum = _decimal(row.get("candle_momentum_dollars") or Decimal("0")) * Decimal("0.25")
    spread_bps = int(row.get("spread_bps") or 0)
    spread_penalty = Decimal(max(0, spread_bps - 100)) / Decimal("10000") / Decimal("8")
    return _clamp_price(mid + adjustment + momentum - spread_penalty)


def _xgboost_predict_batch(
    rows: list[dict[str, Any]],
    model: dict[str, Any],
    *,
    apply_calibration: bool = True,
) -> list[Decimal]:
    try:
        import xgboost as xgb

        schema = {
            "feature_names": list(model.get("feature_names") or []),
            "numeric_feature_names": list(model.get("numeric_feature_names") or []),
            "asset_categories": list(model.get("asset_categories") or []),
        }
        defaults = model.get("feature_defaults") or {}
        matrix = [_crypto_raw_feature_vector(row, schema, defaults=defaults) for row in rows]
        booster = xgb.Booster()
        booster.load_model(bytearray(base64.b64decode(str(model.get("booster_raw_base64") or ""))))
        raw_probs = booster.predict(xgb.DMatrix(matrix))
        cal = model.get("probability_calibration") if apply_calibration else None
        results = []
        for p in raw_probs:
            prob = _clamp_price(Decimal(str(float(p))))
            results.append(_apply_probability_calibration(prob, cal) if apply_calibration else prob)
        return results
    except Exception:
        return [_predict_crypto_probability(row, model, apply_calibration=apply_calibration) for row in rows]


def _lightgbm_predict_batch(
    rows: list[dict[str, Any]],
    model: dict[str, Any],
    *,
    apply_calibration: bool = True,
) -> list[Decimal]:
    try:
        import lightgbm as lgb

        schema = {
            "feature_names": list(model.get("feature_names") or []),
            "numeric_feature_names": list(model.get("numeric_feature_names") or []),
            "asset_categories": list(model.get("asset_categories") or []),
        }
        defaults = model.get("feature_defaults") or {}
        matrix = [_crypto_raw_feature_vector(row, schema, defaults=defaults) for row in rows]
        booster = lgb.Booster(model_str=str(model.get("booster_model_string") or ""))
        raw_probs = booster.predict(matrix)
        cal = model.get("probability_calibration") if apply_calibration else None
        results = []
        for p in raw_probs:
            prob = _clamp_price(Decimal(str(float(p))))
            results.append(_apply_probability_calibration(prob, cal) if apply_calibration else prob)
        return results
    except Exception:
        return [_predict_crypto_probability(row, model, apply_calibration=apply_calibration) for row in rows]


def _crypto_batch_predict_ensemble(
    rows: list[dict[str, Any]],
    models: dict[str, dict[str, Any]],
    weights: dict[str, float],
) -> list[Decimal]:
    total_weight = sum(w for name, w in weights.items() if name in models)
    if not rows or total_weight <= 0:
        return [_clamp_price(_decimal(row.get("mid_yes_dollars"))) for row in rows]
    member_probs: dict[str, list[Decimal]] = {}
    for name, weight in weights.items():
        model = models.get(name)
        if model is None:
            continue
        model_type = model.get("model_type")
        if model_type == "xgboost_classifier":
            member_probs[name] = _xgboost_predict_batch(rows, model)
        elif model_type == "lightgbm_classifier":
            member_probs[name] = _lightgbm_predict_batch(rows, model)
        else:
            member_probs[name] = [_predict_crypto_probability(row, model) for row in rows]
    results = []
    for i in range(len(rows)):
        prob = Decimal("0")
        for name, weight in weights.items():
            if name in member_probs:
                prob += member_probs[name][i] * Decimal(str(weight / total_weight))
        results.append(_clamp_price(prob))
    return results


def _crypto_predictions_for_model(rows: list[dict[str, Any]], model: dict[str, Any] | None) -> list[tuple[Decimal, int]]:
    if not rows:
        return []
    model_type = (model or {}).get("model_type")
    if model_type == "xgboost_classifier" and model is not None:
        probs = _xgboost_predict_batch(rows, model)
        return list(zip(probs, (int(row["label_yes"]) for row in rows)))
    if model_type == "lightgbm_classifier" and model is not None:
        probs = _lightgbm_predict_batch(rows, model)
        return list(zip(probs, (int(row["label_yes"]) for row in rows)))
    return [(_predict_crypto_probability(row, model), int(row["label_yes"])) for row in rows]


def _crypto_model_spot_direction_check(
    market_row: dict[str, Any],
    fair: Decimal,
    *,
    settings: Settings,
) -> tuple[Decimal, dict[str, Any] | None]:
    """Detect model-spot direction inversions and clip the output to the market mid.

    Returns (corrected_fair, mismatch_trace_or_None).

    A mismatch is declared when ALL three conditions hold:
      1. spot_feature_status == "available"   — we have a fresh, non-stale spot price
      2. spot_moneyness_pct > min_moneyness   — spot is clearly above the target
                                                (YES-contract is in-the-money by a
                                                meaningful margin)
      3. fair < max_fair_yes_threshold        — the model predicts a low YES probability
                                                (i.e., it thinks NO wins), contradicting
                                                the spot evidence

    In this case the raw model output is almost certainly an inversion artefact from
    cap_strike-contaminated training rows where positive moneyness was paired with a
    YES=0 label.  We replace `fair` with `mid` (the market price) — the safest neutral
    fallback — so the downstream anchor and edge logic see a price consistent with the
    available physical signal rather than an inverted model output.

    The symmetric case (spot well below target, model predicts very high YES) is NOT
    corrected here; that scenario is less likely to cause a live-loss trade because the
    anchor system already constrains extreme high fair_yes values through the edge gate.
    """
    if not settings.crypto_model_spot_direction_check_enabled:
        return fair, None
    spot_status = str(market_row.get("spot_feature_status") or "")
    if spot_status != "available":
        return fair, None
    moneyness_raw = market_row.get("spot_moneyness_pct")
    if moneyness_raw is None:
        return fair, None
    try:
        moneyness = float(_decimal(moneyness_raw))
    except Exception:
        return fair, None
    min_moneyness = float(settings.crypto_model_spot_direction_min_moneyness_pct)
    max_fair_yes = Decimal(str(settings.crypto_model_spot_direction_max_fair_yes))
    if moneyness < min_moneyness:
        return fair, None
    if fair >= max_fair_yes:
        return fair, None
    # Conflict detected: spot is above target (YES should win) but model says low YES.
    # Fallback to market mid so we neither buy the wrong side nor claim false edge.
    mid_raw = market_row.get("mid_yes_dollars")
    fallback = _clamp_price(_decimal(mid_raw)) if mid_raw is not None else Decimal("0.5000")
    mismatch = {
        "reason": "model_spot_direction_conflict",
        "raw_model_fair_yes": _money_text(fair),
        "corrected_fair_yes": _money_text(fallback),
        "spot_moneyness_pct": str(moneyness),
        "min_moneyness_threshold": str(min_moneyness),
        "max_fair_yes_threshold": str(max_fair_yes),
        "spot_feature_status": spot_status,
    }
    return fallback, mismatch


def _crypto_candidate_metric_entry(
    *,
    name: str,
    status: str,
    metrics: dict[str, Any] | None = None,
    policy_metrics: dict[str, Any] | None = None,
    reason: str | None = None,
    dependency_version: str | None = None,
    fold_count: int | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "metrics": metrics,
        "policy_metrics": policy_metrics,
        "reason": reason,
        "dependency_version": dependency_version,
        "fold_count": fold_count,
    }


def _metric_regression_limit(value: float | None) -> float | None:
    if value is None:
        return None
    return value + max(0.001, abs(value) * CRYPTO_PROBABILITY_GUARDRAIL_TOLERANCE)


def _crypto_candidate_guardrail_failures(
    metrics: dict[str, Any],
    *,
    market_mid_metrics: dict[str, Any] | None,
    logistic_metrics: dict[str, Any] | None,
    candidate_name: str,
) -> list[str]:
    if candidate_name == "market_mid_baseline":
        return []
    failures: list[str] = []
    references = [("market_mid", market_mid_metrics)]
    if candidate_name != "sklearn_logistic":
        references.append(("sklearn_logistic", logistic_metrics))
    for reference_name, reference in references:
        if not reference:
            continue
        for key in ("log_loss", "ece"):
            candidate_value = metrics.get(key)
            reference_value = reference.get(key)
            if candidate_value is None or reference_value is None:
                continue
            limit = _metric_regression_limit(float(reference_value))
            if limit is not None and float(candidate_value) > limit:
                failures.append(f"{key}_regressed_vs_{reference_name}")
    return failures


def _candidate_policy_net(policy: dict[str, Any] | None) -> Decimal:
    return _decimal((policy or {}).get("net_pnl") or Decimal("0"))


def _candidate_policy_selected_count(policy: dict[str, Any] | None) -> int:
    return int((policy or {}).get("selected_count") or 0)


def _candidate_policy_advantage(policy: dict[str, Any] | None) -> Decimal:
    return _decimal((policy or {}).get("pnl_advantage_vs_market_mid_dollars") or Decimal("0"))


def _crypto_candidate_policy_metrics(
    name: str,
    trade_rows: list[dict[str, Any]],
    *,
    settings: Settings,
    market_mid_net_pnl: Decimal,
) -> dict[str, Any]:
    metrics = _crypto_policy_metrics(name, trade_rows, settings=settings)
    net = _candidate_policy_net(metrics)
    advantage = net - market_mid_net_pnl
    return {
        **metrics,
        "market_mid_net_pnl": str(market_mid_net_pnl.quantize(Decimal("0.0001"))),
        "pnl_advantage_vs_market_mid_dollars": str(advantage.quantize(Decimal("0.0001"))),
        "positive_net_pnl": net > Decimal("0"),
        "positive_market_mid_advantage": advantage > Decimal("0"),
    }


def _crypto_attach_candidate_policy_metrics(
    entries: list[dict[str, Any]],
    trade_rows_by_name: dict[str, list[dict[str, Any]]],
    *,
    settings: Settings | None,
) -> list[dict[str, Any]]:
    if settings is None:
        return entries
    market_mid_policy = _crypto_policy_metrics(
        "market_mid_baseline",
        trade_rows_by_name.get("market_mid_baseline", []),
        settings=settings,
    )
    market_mid_net = _candidate_policy_net(market_mid_policy)
    attached: list[dict[str, Any]] = []
    for entry in entries:
        name = str(entry.get("name") or "")
        policy_metrics = _crypto_candidate_policy_metrics(
            name,
            trade_rows_by_name.get(name, []),
            settings=settings,
            market_mid_net_pnl=market_mid_net,
        )
        attached.append({**entry, "policy_metrics": policy_metrics})
    return attached


def _crypto_candidate_has_profit_metrics(entry: dict[str, Any]) -> bool:
    return isinstance(entry.get("policy_metrics"), dict)


def _crypto_model_min_policy_selected_count(
    settings: Settings | None,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> int:
    if crypto_policy is not None:
        return max(1, int(crypto_policy.replay_min_trade_candidates))
    if settings is not None:
        return max(1, int(settings.crypto_replay_min_trade_candidates))
    return 1


def _crypto_candidate_has_min_policy_support(entry: dict[str, Any], *, min_selected_count: int) -> bool:
    policy = entry.get("policy_metrics") if isinstance(entry.get("policy_metrics"), dict) else None
    if policy is None:
        return True
    return _candidate_policy_selected_count(policy) >= max(1, int(min_selected_count))


def _crypto_candidate_selection_status_ok(entry: dict[str, Any], *, allow_guardrail_failed: bool = False) -> bool:
    status = entry.get("status")
    return status == "available" or (allow_guardrail_failed and status == "guardrail_failed")


def _crypto_guardrail_warnings_are_diagnostic(
    settings: Settings | None,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> bool:
    if crypto_policy is not None:
        return not bool(crypto_policy.replay_require_calibration_better_than_mid)
    if settings is not None:
        return not bool(settings.crypto_replay_require_calibration_better_than_mid)
    return False


def _crypto_candidate_is_profit_deployable(
    entry: dict[str, Any],
    *,
    min_selected_count: int = 1,
    allow_guardrail_failed: bool = False,
) -> bool:
    policy = entry.get("policy_metrics") if isinstance(entry.get("policy_metrics"), dict) else None
    return (
        policy is not None
        and _crypto_candidate_selection_status_ok(entry, allow_guardrail_failed=allow_guardrail_failed)
        and entry.get("name") not in CRYPTO_MODEL_BASELINE_CANDIDATES
        and _candidate_policy_selected_count(policy) >= max(1, int(min_selected_count))
        and _candidate_policy_net(policy) > Decimal("0")
        and _candidate_policy_advantage(policy) > Decimal("0")
    )


def _crypto_candidate_profit_sort_key(entry: dict[str, Any]) -> tuple[Decimal, Decimal, int, float, str]:
    policy = entry.get("policy_metrics") if isinstance(entry.get("policy_metrics"), dict) else {}
    metrics = entry.get("metrics") if isinstance(entry.get("metrics"), dict) else {}
    brier = metrics.get("brier") if isinstance(metrics, dict) else None
    return (
        _candidate_policy_net(policy),
        _candidate_policy_advantage(policy),
        _candidate_policy_selected_count(policy),
        -(float(brier) if brier is not None else 999.0),
        str(entry.get("name")),
    )


def _crypto_select_champion(
    candidates: list[dict[str, Any]],
    *,
    min_selected_count: int = 1,
    allow_guardrail_failed_profit_candidates: bool = False,
) -> str:
    profit_candidates = [
        candidate
        for candidate in candidates
        if _crypto_model_selection_usable(
            candidate,
            allow_guardrail_failed=allow_guardrail_failed_profit_candidates,
        )
        and _crypto_candidate_has_profit_metrics(candidate)
        and candidate.get("name") not in CRYPTO_MODEL_BASELINE_CANDIDATES
    ]
    supported_profit_candidates = [
        candidate
        for candidate in profit_candidates
        if _crypto_candidate_has_min_policy_support(candidate, min_selected_count=min_selected_count)
    ]
    deployable = [
        candidate
        for candidate in supported_profit_candidates
        if _crypto_candidate_is_profit_deployable(
            candidate,
            min_selected_count=min_selected_count,
            allow_guardrail_failed=allow_guardrail_failed_profit_candidates,
        )
    ]
    if deployable:
        deployable.sort(key=_crypto_candidate_profit_sort_key, reverse=True)
        return str(deployable[0]["name"])
    deployable_by_probability = [
        candidate
        for candidate in candidates
        if _crypto_model_selection_usable(candidate)
        and _crypto_candidate_has_min_policy_support(candidate, min_selected_count=min_selected_count)
        and (
            not _crypto_candidate_has_profit_metrics(candidate)
            or _crypto_candidate_is_profit_deployable(
                candidate,
                min_selected_count=min_selected_count,
                allow_guardrail_failed=allow_guardrail_failed_profit_candidates,
            )
        )
    ]
    if deployable_by_probability:
        deployable_by_probability.sort(key=lambda item: (float((item.get("metrics") or {})["brier"]), str(item.get("name"))))
        return str(deployable_by_probability[0]["name"])
    baseline = [
        candidate
        for candidate in candidates
        if candidate.get("name") in CRYPTO_MODEL_BASELINE_CANDIDATES
        and candidate.get("status") == "available"
        and isinstance(candidate.get("metrics"), dict)
        and (candidate.get("metrics") or {}).get("brier") is not None
    ]
    if baseline:
        baseline.sort(key=lambda item: (float((item.get("metrics") or {})["brier"]), str(item.get("name"))))
        return str(baseline[0]["name"])
    return "sklearn_logistic"


def _crypto_ensemble_weights_from_metrics(candidates: list[dict[str, Any]]) -> dict[str, float]:
    eligible = [
        candidate
        for candidate in candidates
        if candidate.get("name")
        not in {"market_mid_baseline", "calibrated_weighted_ensemble", "spot_distance_contrarian_gated"}
        and candidate.get("status") == "available"
        and isinstance(candidate.get("metrics"), dict)
        and (candidate.get("metrics") or {}).get("brier") is not None
    ]
    if len(eligible) < 2:
        return {}
    best_brier = min(float((candidate.get("metrics") or {})["brier"]) for candidate in eligible)
    selected = [
        candidate
        for candidate in eligible
        if float((candidate.get("metrics") or {})["brier"]) <= best_brier * 1.05 + 1e-12
    ]
    if len(selected) < 2:
        return {}
    inverse = {
        str(candidate["name"]): 1.0 / max(1e-9, float((candidate.get("metrics") or {})["brier"]))
        for candidate in selected
    }
    total = sum(inverse.values())
    return {name: round(weight / total, 6) for name, weight in sorted(inverse.items())}


def _crypto_predict_ensemble_from_models(
    row: dict[str, Any],
    models: dict[str, dict[str, Any]],
    weights: dict[str, float],
) -> Decimal:
    total_weight = sum(weight for name, weight in weights.items() if name in models)
    if total_weight <= 0:
        return _clamp_price(_decimal(row.get("mid_yes_dollars")))
    probability = Decimal("0")
    for name, weight in weights.items():
        if name not in models:
            continue
        probability += _predict_crypto_probability(row, models[name]) * Decimal(str(weight / total_weight))
    return _clamp_price(probability)


def _crypto_in_sample_candidate_report(
    rows: list[dict[str, Any]],
    candidate_status: dict[str, dict[str, Any]],
    *,
    settings: Settings | None = None,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    market_metrics: dict[str, Any] | None = None
    logistic_metrics: dict[str, Any] | None = None
    trade_rows_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for name in CRYPTO_MODEL_CANDIDATE_NAMES:
        status = candidate_status.get(name) or {"name": name, "status": "unavailable", "reason": "not_registered"}
        if status.get("status") == "available" and status.get("model") is not None:
            predictions = _crypto_predictions_for_model(rows, status["model"])
            if settings is not None:
                for row, (prediction, _label) in zip(rows, predictions, strict=True):
                    trade = _simulate_crypto_trade(row, prediction, settings=settings, crypto_policy=crypto_policy)
                    if trade["status"] == "fillable":
                        trade_rows_by_name[name].append({**row, "simulation": trade})
            metrics = _probability_metrics_decimal(predictions)
            if name == "market_mid_baseline":
                market_metrics = metrics
            if name == "sklearn_logistic":
                logistic_metrics = metrics
            entries.append(
                _crypto_candidate_metric_entry(
                    name=name,
                    status="available",
                    metrics=metrics,
                    dependency_version=status.get("dependency_version"),
                )
            )
        else:
            entries.append(
                _crypto_candidate_metric_entry(
                    name=name,
                    status="unavailable",
                    reason=status.get("reason"),
                    dependency_version=status.get("dependency_version"),
                )
            )
    guarded_entries = _crypto_apply_candidate_guardrails(entries, market_metrics=market_metrics, logistic_metrics=logistic_metrics)
    model_map = {
        name: status["model"]
        for name, status in candidate_status.items()
        if status.get("status") == "available" and status.get("model") is not None
    }
    guarded_entries, ensemble_weights = _crypto_add_ensemble_candidate(rows, guarded_entries, model_map)
    if settings is not None and ensemble_weights:
        ensemble_preds = _crypto_batch_predict_ensemble(rows, model_map, ensemble_weights)
        for row, prediction in zip(rows, ensemble_preds):
            trade = _simulate_crypto_trade(
                row,
                prediction,
                settings=settings,
                crypto_policy=crypto_policy,
            )
            if trade["status"] == "fillable":
                trade_rows_by_name["calibrated_weighted_ensemble"].append({**row, "simulation": trade})
    guarded_entries = _crypto_attach_candidate_policy_metrics(guarded_entries, trade_rows_by_name, settings=settings)
    min_policy_selected_count = _crypto_model_min_policy_selected_count(settings, crypto_policy)
    allow_guardrail_failed_profit_candidates = _crypto_guardrail_warnings_are_diagnostic(settings, crypto_policy)
    champion = _crypto_select_champion(
        guarded_entries,
        min_selected_count=min_policy_selected_count,
        allow_guardrail_failed_profit_candidates=allow_guardrail_failed_profit_candidates,
    )
    champion_entry = _crypto_candidate_entry_by_name(guarded_entries, champion)
    return {
        "schema_version": CRYPTO_CANDIDATE_REGISTRY_VERSION,
        "status": "ok",
        "selection_scope": "in_sample_training_fallback",
        "primary_metric": "oos_candidate_net_pnl",
        "selection_policy": "prefer_positive_oos_pnl_non_market_candidate_then_pnl_advantage",
        "min_policy_selected_count": min_policy_selected_count,
        "selection_baselines": sorted(CRYPTO_MODEL_BASELINE_CANDIDATES),
        "guardrails": {
            "log_loss_ece_max_regression_pct": CRYPTO_PROBABILITY_GUARDRAIL_TOLERANCE,
            "references": ["market_mid_baseline", "sklearn_logistic"],
            "mode": "diagnostic_for_non_market_selection",
            "allow_guardrail_failed_profit_candidates": allow_guardrail_failed_profit_candidates,
        },
        "fold_count": 0,
        "candidates": sorted(guarded_entries, key=_crypto_candidate_sort_key),
        "champion_name": champion,
        "champion_status": champion_entry.get("status") if champion_entry else None,
        "champion_selection_reason": _crypto_champion_selection_reason(
            champion_entry,
            min_selected_count=min_policy_selected_count,
            allow_guardrail_failed_profit_candidates=allow_guardrail_failed_profit_candidates,
        ),
        "champion_validation_metrics": _metrics_for_candidate(guarded_entries, champion),
        "champion_policy_metrics": champion_entry.get("policy_metrics") if champion_entry else None,
        "ensemble_weights": ensemble_weights,
        "dependency_versions": _crypto_dependency_versions(),
    }


def _crypto_model_candidate_report(
    rows: list[dict[str, Any]],
    *,
    settings: Settings | None,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    full_candidate_status: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    min_train_rows = max(2, min(settings.crypto_min_training_samples, 20)) if settings is not None else max(2, min(len(rows) // 2, 20))
    max_folds = max(0, int(settings.crypto_model_candidate_max_walk_forward_folds)) if settings is not None else 0
    folds = _crypto_walk_forward_folds(rows, min_train_rows=min_train_rows, max_folds=max_folds or None)
    if not folds:
        report = _crypto_in_sample_candidate_report(
            rows,
            full_candidate_status or _fit_crypto_model_candidates(rows, settings=settings),
            settings=settings,
            crypto_policy=crypto_policy,
        )
        report["status"] = "insufficient_walk_forward_data"
        report["reason"] = "need_settled_point_in_time_crypto_rows_across_market_days"
        return report

    predictions_by_candidate: dict[str, list[tuple[Decimal, int]]] = defaultdict(list)
    trade_rows_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unavailable_reasons: dict[str, str] = {}
    dependency_versions: dict[str, str | None] = {}
    fold_summaries: list[dict[str, Any]] = []
    for fold in folds:
        train_rows = fold["train_rows"]
        test_rows = fold["test_rows"]
        schema = _crypto_feature_schema(train_rows)
        defaults = _crypto_feature_defaults(train_rows)
        fallback = _fit_crypto_heuristic_calibration(train_rows)
        candidate_status = _fit_crypto_model_candidates(train_rows, schema=schema, defaults=defaults, fallback=fallback, settings=settings)
        available_models = {
            name: status["model"]
            for name, status in candidate_status.items()
            if status.get("status") == "available" and status.get("model") is not None
        }
        train_report = _crypto_in_sample_candidate_report(
            train_rows,
            candidate_status,
            settings=settings,
            crypto_policy=crypto_policy,
        )
        weights = dict(train_report.get("ensemble_weights") or {})
        if len(weights) >= 2:
            ensemble_test_preds = _crypto_batch_predict_ensemble(test_rows, available_models, weights)
            for row, prediction in zip(test_rows, ensemble_test_preds):
                predictions_by_candidate["calibrated_weighted_ensemble"].append((prediction, int(row["label_yes"])))
                if settings is not None:
                    trade = _simulate_crypto_trade(row, prediction, settings=settings, crypto_policy=crypto_policy)
                    if trade["status"] == "fillable":
                        trade_rows_by_candidate["calibrated_weighted_ensemble"].append({**row, "simulation": trade})
        else:
            unavailable_reasons.setdefault("calibrated_weighted_ensemble", "need_at_least_two_guardrail_clean_members")
        for name in CRYPTO_MODEL_CANDIDATE_NAMES:
            status = candidate_status.get(name) or {"status": "unavailable", "reason": "not_registered"}
            dependency_versions[name] = status.get("dependency_version")
            if status.get("status") != "available" or status.get("model") is None:
                unavailable_reasons.setdefault(name, str(status.get("reason") or "unavailable"))
                continue
            batch_preds = _crypto_predictions_for_model(test_rows, status["model"])
            for row, (prediction, label) in zip(test_rows, batch_preds, strict=True):
                predictions_by_candidate[name].append((prediction, label))
                if settings is not None:
                    trade = _simulate_crypto_trade(row, prediction, settings=settings, crypto_policy=crypto_policy)
                    if trade["status"] == "fillable":
                        trade_rows_by_candidate[name].append({**row, "simulation": trade})
        fold_summaries.append(
            {
                "fold_id": fold["fold_id"],
                "train_rows": len(train_rows),
                "test_rows": len(test_rows),
                "train_cutoff_market_day": fold["train_cutoff_market_day"],
                "ensemble_weights": weights,
                "available_candidates": sorted(available_models),
            }
        )

    market_metrics = _probability_metrics_decimal(predictions_by_candidate.get("market_mid_baseline", []))
    logistic_metrics = _probability_metrics_decimal(predictions_by_candidate.get("sklearn_logistic", []))
    entries: list[dict[str, Any]] = []
    for name in (*CRYPTO_MODEL_CANDIDATE_NAMES, "calibrated_weighted_ensemble"):
        predictions = predictions_by_candidate.get(name, [])
        if predictions:
            entries.append(
                _crypto_candidate_metric_entry(
                    name=name,
                    status="available",
                    metrics=_probability_metrics_decimal(predictions),
                    reason=None,
                    dependency_version=dependency_versions.get(name),
                    fold_count=len(folds),
                )
            )
        else:
            entries.append(
                _crypto_candidate_metric_entry(
                    name=name,
                    status="unavailable",
                    metrics=None,
                    reason=unavailable_reasons.get(name) or "no_walk_forward_predictions",
                    dependency_version=dependency_versions.get(name),
                    fold_count=len(folds),
                )
            )
    entries = _crypto_apply_candidate_guardrails(entries, market_metrics=market_metrics, logistic_metrics=logistic_metrics)
    entries = _crypto_attach_candidate_policy_metrics(entries, trade_rows_by_candidate, settings=settings)
    ensemble_entry = next((entry for entry in entries if entry["name"] == "calibrated_weighted_ensemble"), None)
    min_policy_selected_count = _crypto_model_min_policy_selected_count(settings, crypto_policy)
    allow_guardrail_failed_profit_candidates = _crypto_guardrail_warnings_are_diagnostic(settings, crypto_policy)
    champion = _crypto_select_champion(
        entries,
        min_selected_count=min_policy_selected_count,
        allow_guardrail_failed_profit_candidates=allow_guardrail_failed_profit_candidates,
    )
    champion_entry = _crypto_candidate_entry_by_name(entries, champion)
    return {
        "schema_version": CRYPTO_CANDIDATE_REGISTRY_VERSION,
        "status": "ok",
        "selection_scope": "walk_forward_time_ordered",
        "primary_metric": "oos_candidate_net_pnl",
        "selection_policy": "prefer_positive_oos_pnl_non_market_candidate_then_pnl_advantage",
        "min_policy_selected_count": min_policy_selected_count,
        "selection_baselines": sorted(CRYPTO_MODEL_BASELINE_CANDIDATES),
        "guardrails": {
            "log_loss_ece_max_regression_pct": CRYPTO_PROBABILITY_GUARDRAIL_TOLERANCE,
            "references": ["market_mid_baseline", "sklearn_logistic"],
            "mode": "diagnostic_for_non_market_selection",
            "allow_guardrail_failed_profit_candidates": allow_guardrail_failed_profit_candidates,
        },
        "fold_count": len(folds),
        "folds": fold_summaries,
        "candidates": sorted(entries, key=_crypto_candidate_sort_key),
        "champion_name": champion,
        "champion_status": champion_entry.get("status") if champion_entry else None,
        "champion_selection_reason": _crypto_champion_selection_reason(
            champion_entry,
            min_selected_count=min_policy_selected_count,
            allow_guardrail_failed_profit_candidates=allow_guardrail_failed_profit_candidates,
        ),
        "champion_validation_metrics": _metrics_for_candidate(entries, champion),
        "champion_policy_metrics": champion_entry.get("policy_metrics") if champion_entry else None,
        "ensemble_weights": _crypto_ensemble_weights_from_metrics(entries) if ensemble_entry and ensemble_entry.get("status") == "available" else {},
        "dependency_versions": _crypto_dependency_versions(),
    }


def _crypto_add_ensemble_candidate(
    rows: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    model_map: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    weights = _crypto_ensemble_weights_from_metrics(entries)
    if len(weights) < 2:
        entries.append(
            _crypto_candidate_metric_entry(
                name="calibrated_weighted_ensemble",
                status="unavailable",
                reason="need_at_least_two_guardrail_clean_members",
            )
        )
        return entries, {}
    predictions = [(_crypto_predict_ensemble_from_models(row, model_map, weights), int(row["label_yes"])) for row in rows]
    ensemble_metrics = _probability_metrics_decimal(predictions)
    market_metrics = _metrics_for_candidate(entries, "market_mid_baseline")
    logistic_metrics = _metrics_for_candidate(entries, "sklearn_logistic")
    failures = _crypto_candidate_guardrail_failures(
        ensemble_metrics,
        market_mid_metrics=market_metrics,
        logistic_metrics=logistic_metrics,
        candidate_name="calibrated_weighted_ensemble",
    )
    entries.append(
        _crypto_candidate_metric_entry(
            name="calibrated_weighted_ensemble",
            status="guardrail_failed" if failures else "available",
            metrics=ensemble_metrics,
            reason=",".join(failures) if failures else None,
        )
    )
    return entries, weights


def _crypto_apply_candidate_guardrails(
    entries: list[dict[str, Any]],
    *,
    market_metrics: dict[str, Any] | None,
    logistic_metrics: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    guarded: list[dict[str, Any]] = []
    for entry in entries:
        if entry.get("status") != "available" or not isinstance(entry.get("metrics"), dict):
            guarded.append(entry)
            continue
        failures = _crypto_candidate_guardrail_failures(
            entry["metrics"],
            market_mid_metrics=market_metrics,
            logistic_metrics=logistic_metrics,
            candidate_name=str(entry["name"]),
        )
        if failures:
            guarded.append({**entry, "status": "guardrail_failed", "reason": ",".join(failures)})
        else:
            guarded.append(entry)
    return guarded


def _crypto_candidate_entry_by_name(entries: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for entry in entries:
        if entry.get("name") == name:
            return entry
    return None


def _crypto_candidate_sort_key(entry: dict[str, Any]) -> tuple[int, float, str]:
    metrics = entry.get("metrics") if isinstance(entry.get("metrics"), dict) else {}
    brier = metrics.get("brier") if isinstance(metrics, dict) else None
    status_rank = 0 if entry.get("status") == "available" else 1
    return (status_rank, float(brier) if brier is not None else 999.0, str(entry.get("name")))


def _metrics_for_candidate(entries: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for entry in entries:
        if entry.get("name") == name and isinstance(entry.get("metrics"), dict):
            return entry["metrics"]
    return None


def _crypto_model_selection_usable(entry: dict[str, Any], *, allow_guardrail_failed: bool = False) -> bool:
    if entry.get("name") in CRYPTO_MODEL_BASELINE_CANDIDATES:
        return False
    if not _crypto_candidate_selection_status_ok(entry, allow_guardrail_failed=allow_guardrail_failed):
        return False
    metrics = entry.get("metrics")
    return isinstance(metrics, dict) and metrics.get("brier") is not None


def _crypto_champion_selection_reason(
    entry: dict[str, Any] | None,
    *,
    min_selected_count: int = 1,
    allow_guardrail_failed_profit_candidates: bool = False,
) -> str:
    if not entry:
        return "no_candidate_entry"
    if entry.get("name") in CRYPTO_MODEL_BASELINE_CANDIDATES:
        return "fallback_market_mid_no_non_market_candidate"
    if _crypto_candidate_has_profit_metrics(entry):
        if _crypto_candidate_is_profit_deployable(
            entry,
            min_selected_count=min_selected_count,
            allow_guardrail_failed=allow_guardrail_failed_profit_candidates,
        ):
            if entry.get("status") == "guardrail_failed":
                return "selected_non_market_candidate_with_diagnostic_guardrail_warnings"
            return "selected_positive_oos_pnl_non_market_candidate"
        return "diagnostic_only_best_non_market_oos_pnl"
    if entry.get("status") == "guardrail_failed":
        return "diagnostic_only_guardrail_failed_non_market_candidate"
    return "selected_non_market_candidate"


def _crypto_dependency_versions() -> dict[str, str | None]:
    return {
        "scikit_learn": _package_version("scikit-learn"),
        "xgboost": _package_version("xgboost") or _package_version("xgboost-cpu"),
        "lightgbm": _package_version("lightgbm"),
    }


def _crypto_model_metrics(
    rows: list[dict[str, Any]],
    model: dict[str, Any],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any]:
    # Batch-predict once and reuse for both calibrated metrics and exploratory simulation.
    model_type = (model or {}).get("model_type")
    if model_type == "xgboost_classifier":
        all_predicted = _xgboost_predict_batch(rows, model)
    elif model_type == "lightgbm_classifier":
        all_predicted = _lightgbm_predict_batch(rows, model)
    else:
        all_predicted = [_predict_crypto_probability(row, model) for row in rows]

    baseline_predictions: list[tuple[Decimal, int]] = []
    calibrated_predictions: list[tuple[Decimal, int]] = []
    baseline_market_predictions: list[tuple[Decimal, int, str]] = []
    calibrated_market_predictions: list[tuple[Decimal, int, str]] = []
    baseline_simulated = []
    simulated = []
    for row, predicted in zip(rows, all_predicted):
        label = int(row["label_yes"])
        baseline = _decimal(row["mid_yes_dollars"])
        market_ticker = _crypto_row_market_ticker(row)
        baseline_predictions.append((baseline, label))
        calibrated_predictions.append((predicted, label))
        baseline_market_predictions.append((baseline, label, market_ticker))
        calibrated_market_predictions.append((predicted, label, market_ticker))
        baseline_simulated.append(_simulate_crypto_trade(row, baseline, settings=settings, crypto_policy=crypto_policy))
        simulated.append(_simulate_crypto_trade(row, predicted, settings=settings, crypto_policy=crypto_policy))
    exploratory = [
        _simulate_crypto_trade(
            row,
            predicted,
            settings=settings,
            crypto_policy=crypto_policy,
            policy=CRYPTO_EXPLORATORY_SHADOW,
        )
        for row, predicted in zip(rows, all_predicted)
    ]
    fillable = [item for item in simulated if item["status"] == "fillable"]
    baseline_fillable = [item for item in baseline_simulated if item["status"] == "fillable"]
    exploratory_fillable = [item for item in exploratory if item["status"] == "fillable"]
    net = sum((_decimal(item["net_pnl"]) for item in fillable), Decimal("0"))
    baseline_net = sum((_decimal(item["net_pnl"]) for item in baseline_fillable), Decimal("0"))
    fees = sum((_decimal(item["fees"]) for item in fillable), Decimal("0"))
    hard_cap_breaches = sum(1 for item in fillable if _decimal(item["net_pnl"]) < Decimal("-1.0000"))
    baseline_metrics = _probability_metrics_decimal(baseline_predictions)
    calibrated_metrics = _probability_metrics_decimal(calibrated_predictions)
    baseline_market_metrics = _probability_metrics_market_weighted(baseline_market_predictions)
    calibrated_market_metrics = _probability_metrics_market_weighted(calibrated_market_predictions)
    independent_market_count = _crypto_distinct_market_count(rows)
    fillable_market_count = len(
        {
            _crypto_row_market_ticker(row)
            for row, item in zip(rows, simulated)
            if item["status"] == "fillable"
        }
    )
    dead_features = _crypto_dead_feature_report(rows)
    metrics = {
        "sample_count": len(rows),
        "resolved_sample_count": len(rows),
        "prediction_eligible_count": sum(1 for row in rows if row.get("prediction_eligible", True)),
        "strict_trade_eligible_count": sum(1 for row in rows if row.get("strict_trade_eligible")),
        "proxy_quote_row_count": sum(1 for row in rows if row.get("quote_source") != "snapshot_quotes"),
        "real_quote_row_count": sum(1 for row in rows if row.get("quote_source") == "snapshot_quotes"),
        "spot_feature_coverage_pct": _spot_feature_coverage(rows),
        "trade_candidate_count": len(fillable),
        "current_model_live_quality_candidate_count": len(fillable),
        "live_quality_candidate_count": len(fillable),
        "exploratory_shadow_candidate_count": sum(1 for item in exploratory_fillable if item.get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW),
        "net_simulated_pl_dollars": float(net),
        "market_mid_net_simulated_pl_dollars": float(baseline_net),
        "pnl_advantage_vs_market_mid_dollars": float(net - baseline_net),
        "fees_dollars": float(fees),
        "hard_cap_breaches": hard_cap_breaches,
        "calibration_brier": calibrated_metrics["brier"],
        "market_mid_brier": baseline_metrics["brier"],
        "calibration_log_loss": calibrated_metrics["log_loss"],
        "market_mid_log_loss": baseline_metrics["log_loss"],
        "calibration_ece": calibrated_metrics["ece"],
        "market_mid_ece": baseline_metrics["ece"],
        "independent_market_count": independent_market_count,
        "rows_per_market_mean": (len(rows) / independent_market_count) if independent_market_count else None,
        "resolved_market_count": independent_market_count,
        "strict_trade_eligible_market_count": _crypto_distinct_market_count(
            [row for row in rows if row.get("strict_trade_eligible")]
        ),
        "trade_candidate_market_count": fillable_market_count,
        "current_model_live_quality_market_count": fillable_market_count,
        "calibration_brier_market_weighted": calibrated_market_metrics["brier"],
        "market_mid_brier_market_weighted": baseline_market_metrics["brier"],
        "calibration_log_loss_market_weighted": calibrated_market_metrics["log_loss"],
        "market_mid_log_loss_market_weighted": baseline_market_metrics["log_loss"],
        "calibration_ece_market_weighted": calibrated_market_metrics["ece"],
        "market_mid_ece_market_weighted": baseline_market_metrics["ece"],
        "dead_feature_names": dead_features["dead_feature_names"],
        "dead_feature_count": dead_features["dead_feature_count"],
        "fee_model_version": current_fee_model_version(),
        "metrics_scope": "in_sample",
    }
    candidate_report = model.get("candidate_report") if isinstance(model, dict) else None
    if isinstance(candidate_report, dict):
        champion_metrics = candidate_report.get("champion_validation_metrics") or {}
        champion_policy_metrics = candidate_report.get("champion_policy_metrics") or {}
        metrics.update(
            {
                "champion_model": candidate_report.get("champion_name") or model.get("model_type"),
                "champion_status": candidate_report.get("champion_status"),
                "champion_selection_reason": candidate_report.get("champion_selection_reason"),
                "champion_selection_policy": candidate_report.get("selection_policy"),
                "champion_oos_selected_count": champion_policy_metrics.get("selected_count"),
                "champion_oos_net_pnl": champion_policy_metrics.get("net_pnl"),
                "champion_oos_pnl_advantage_vs_market_mid": champion_policy_metrics.get(
                    "pnl_advantage_vs_market_mid_dollars"
                ),
                "validation_brier": champion_metrics.get("brier"),
                "validation_log_loss": champion_metrics.get("log_loss"),
                "validation_ece": champion_metrics.get("ece"),
                "validation_fold_count": candidate_report.get("fold_count"),
                "validation_scope": candidate_report.get("selection_scope"),
            }
        )
        # Additive, observability-only: project the per-candidate OOS selection
        # table into flat metrics so a market_mid_baseline fallback is
        # self-explaining (which losing candidates existed and why none won).
        # No selection logic runs here; this is a read-side projection of data
        # already computed by the candidate report. Guard every accessor.
        guardrails = candidate_report.get("guardrails") if isinstance(candidate_report.get("guardrails"), dict) else {}
        sel_min_selected_count = candidate_report.get("min_policy_selected_count")
        try:
            sel_min_selected_count = max(1, int(sel_min_selected_count))
        except (TypeError, ValueError):
            sel_min_selected_count = 1
        sel_allow_guardrail_failed = bool(guardrails.get("allow_guardrail_failed_profit_candidates"))
        selection_table: list[dict[str, Any]] = []
        for entry in candidate_report.get("candidates") or []:
            if not isinstance(entry, dict):
                continue
            entry_metrics = entry.get("metrics") if isinstance(entry.get("metrics"), dict) else {}
            entry_policy = entry.get("policy_metrics") if isinstance(entry.get("policy_metrics"), dict) else None
            brier_value = entry_metrics.get("brier") if isinstance(entry_metrics, dict) else None
            profit_deployable = _crypto_candidate_is_profit_deployable(
                entry,
                min_selected_count=sel_min_selected_count,
                allow_guardrail_failed=sel_allow_guardrail_failed,
            )
            probability_deployable = _crypto_model_selection_usable(
                entry,
                allow_guardrail_failed=sel_allow_guardrail_failed,
            ) and _crypto_candidate_has_min_policy_support(
                entry,
                min_selected_count=sel_min_selected_count,
            )
            selection_table.append(
                {
                    "name": entry.get("name"),
                    "brier": float(brier_value) if brier_value is not None else None,
                    "policy_net": float(_candidate_policy_net(entry_policy)) if entry_policy is not None else None,
                    "policy_advantage": float(_candidate_policy_advantage(entry_policy)) if entry_policy is not None else None,
                    "selected_count": _candidate_policy_selected_count(entry_policy) if entry_policy is not None else None,
                    "profit_deployable": bool(profit_deployable),
                    "probability_deployable": bool(probability_deployable),
                    "status": entry.get("status"),
                    "reason": entry.get("reason"),
                }
            )
        metrics["champion_model_type"] = model.get("model_type")
        metrics["candidate_selection_table"] = selection_table
    return metrics


def _crypto_return_feature(row: dict[str, Any]) -> float:
    for key in ("spot_return_6_pct", "spot_return_3_pct", "spot_return_1_pct", "spot_momentum_pct"):
        value = row.get(key)
        if value not in (None, ""):
            return float(_decimal(value))
    return float(_decimal(row.get("candle_momentum_dollars") or Decimal("0")))


def _fit_crypto_linear_return_baseline(rows: list[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    grouped: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        if row.get("label_yes") not in {0, 1}:
            continue
        grouped[str(row.get("asset_symbol") or "GLOBAL")].append((_crypto_return_feature(row), float(int(row["label_yes"]))))
        grouped["GLOBAL"].append((_crypto_return_feature(row), float(int(row["label_yes"]))))
    models: dict[str, tuple[float, float]] = {}
    for asset, values in grouped.items():
        if not values:
            continue
        xs = [item[0] for item in values]
        ys = [item[1] for item in values]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        variance = sum((x - mean_x) * (x - mean_x) for x in xs)
        if variance <= 1e-12:
            models[asset] = (mean_y, 0.0)
            continue
        covariance = sum((x - mean_x) * (y - mean_y) for x, y in values)
        slope = covariance / variance
        intercept = mean_y - slope * mean_x
        models[asset] = (intercept, slope)
    return models


def _predict_crypto_linear_return_baseline(row: dict[str, Any], model: dict[str, tuple[float, float]]) -> Decimal:
    intercept, slope = model.get(str(row.get("asset_symbol") or "")) or model.get("GLOBAL") or (0.5, 0.0)
    return _clamp_price(Decimal(str(intercept + slope * _crypto_return_feature(row))))


def _crypto_baseline_probability(
    row: dict[str, Any],
    name: str,
    *,
    linear_model: dict[str, tuple[float, float]] | None = None,
) -> Decimal:
    if name == "always_0_5":
        return Decimal("0.5000")
    if name == "last_direction":
        momentum = _decimal(row.get("candle_momentum_dollars") or Decimal("0"))
        if momentum > 0:
            return Decimal("0.5500")
        if momentum < 0:
            return Decimal("0.4500")
        return Decimal("0.5000")
    if name == "naive_momentum":
        return Decimal("0.5500") if _crypto_return_feature(row) > 0 else Decimal("0.4500")
    if name == "linear_on_returns":
        return _predict_crypto_linear_return_baseline(row, linear_model or {})
    if name == "market_mid_baseline":
        return _decimal(row["mid_yes_dollars"])
    raise ValueError(f"unknown crypto baseline {name}")


def _runtime_crypto_policy_with_asset_entry(
    crypto_policy: RuntimeCryptoPolicy,
    asset_symbol: str,
    entry_policy: dict[str, Any],
    *,
    frequency: str | None = None,
) -> RuntimeCryptoPolicy:
    overrides = {
        symbol: dict(values)
        for symbol, values in (crypto_policy.asset_entry_overrides or {}).items()
    }
    override_key = crypto_entry_override_key(asset_symbol, frequency)
    override_entry = dict(entry_policy)
    override_entry["min_remaining_payout_bps"] = int(
        override_entry.get("min_remaining_payout_bps")
        if override_entry.get("min_remaining_payout_bps") is not None
        else crypto_policy.min_remaining_payout_bps
    )
    overrides[override_key] = override_entry
    return RuntimeCryptoPolicy(
        min_fee_adjusted_edge_bps=crypto_policy.min_fee_adjusted_edge_bps,
        max_spread_bps=crypto_policy.max_spread_bps,
        min_confidence=crypto_policy.min_confidence,
        min_contract_price_dollars=crypto_policy.min_contract_price_dollars,
        min_remaining_payout_bps=crypto_policy.min_remaining_payout_bps,
        max_credible_edge_bps=crypto_policy.max_credible_edge_bps,
        target_position_pct=crypto_policy.target_position_pct,
        replay_min_resolved_markets=crypto_policy.replay_min_resolved_markets,
        replay_min_trade_candidates=crypto_policy.replay_min_trade_candidates,
        replay_min_net_pl_dollars=crypto_policy.replay_min_net_pl_dollars,
        replay_min_pnl_per_candidate_dollars=crypto_policy.replay_min_pnl_per_candidate_dollars,
        replay_max_hard_cap_breaches=crypto_policy.replay_max_hard_cap_breaches,
        replay_min_spot_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
        replay_require_calibration_better_than_mid=crypto_policy.replay_require_calibration_better_than_mid,
        replay_require_pnl_beats_market_mid=crypto_policy.replay_require_pnl_beats_market_mid,
        replay_min_pnl_advantage_dollars=crypto_policy.replay_min_pnl_advantage_dollars,
        replay_per_price_bucket_gate_enabled=crypto_policy.replay_per_price_bucket_gate_enabled,
        replay_per_price_bucket_min_samples=crypto_policy.replay_per_price_bucket_min_samples,
        replay_per_price_bucket_min_win_rate=crypto_policy.replay_per_price_bucket_min_win_rate,
        replay_per_price_bucket_min_net_pnl_dollars=crypto_policy.replay_per_price_bucket_min_net_pnl_dollars,
        trading_enabled=crypto_policy.trading_enabled,
        production_autonomy_enabled=crypto_policy.production_autonomy_enabled,
        asset_modes=dict(crypto_policy.asset_modes or {}),
        asset_entry_overrides=overrides,
    )


def _runtime_crypto_policy_with_asset_modes(
    crypto_policy: RuntimeCryptoPolicy,
    asset_modes: dict[str, str],
) -> RuntimeCryptoPolicy:
    modes = {
        normalize_asset_symbol(symbol): normalize_asset_mode(mode)
        for symbol, mode in (asset_modes or {}).items()
    }
    return RuntimeCryptoPolicy(
        min_fee_adjusted_edge_bps=crypto_policy.min_fee_adjusted_edge_bps,
        max_spread_bps=crypto_policy.max_spread_bps,
        min_confidence=crypto_policy.min_confidence,
        min_contract_price_dollars=crypto_policy.min_contract_price_dollars,
        min_remaining_payout_bps=crypto_policy.min_remaining_payout_bps,
        max_credible_edge_bps=crypto_policy.max_credible_edge_bps,
        target_position_pct=crypto_policy.target_position_pct,
        replay_min_resolved_markets=crypto_policy.replay_min_resolved_markets,
        replay_min_trade_candidates=crypto_policy.replay_min_trade_candidates,
        replay_min_net_pl_dollars=crypto_policy.replay_min_net_pl_dollars,
        replay_min_pnl_per_candidate_dollars=crypto_policy.replay_min_pnl_per_candidate_dollars,
        replay_max_hard_cap_breaches=crypto_policy.replay_max_hard_cap_breaches,
        replay_min_spot_coverage_pct=crypto_policy.replay_min_spot_coverage_pct,
        replay_require_calibration_better_than_mid=crypto_policy.replay_require_calibration_better_than_mid,
        replay_require_pnl_beats_market_mid=crypto_policy.replay_require_pnl_beats_market_mid,
        replay_min_pnl_advantage_dollars=crypto_policy.replay_min_pnl_advantage_dollars,
        replay_per_price_bucket_gate_enabled=crypto_policy.replay_per_price_bucket_gate_enabled,
        replay_per_price_bucket_min_samples=crypto_policy.replay_per_price_bucket_min_samples,
        replay_per_price_bucket_min_win_rate=crypto_policy.replay_per_price_bucket_min_win_rate,
        replay_per_price_bucket_min_net_pnl_dollars=crypto_policy.replay_per_price_bucket_min_net_pnl_dollars,
        trading_enabled=crypto_policy.trading_enabled,
        production_autonomy_enabled=crypto_policy.production_autonomy_enabled,
        asset_modes=modes,
        asset_entry_overrides=dict(crypto_policy.asset_entry_overrides or {}),
    )


def _crypto_entry_policy_grid(base_entry: dict[str, Any]) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    for min_edge in CRYPTO_ENTRY_OPTIMIZER_GRID["min_fee_adjusted_edge_bps"]:
        for max_spread in CRYPTO_ENTRY_OPTIMIZER_GRID["max_spread_bps"]:
            for min_price in CRYPTO_ENTRY_OPTIMIZER_GRID["min_contract_price_dollars"]:
                for min_remaining in CRYPTO_ENTRY_OPTIMIZER_GRID["min_remaining_payout_bps"]:
                    policies.append(
                        {
                            **base_entry,
                            "min_fee_adjusted_edge_bps": min_edge,
                            "max_spread_bps": max_spread,
                            "min_contract_price_dollars": min_price,
                            "min_remaining_payout_bps": min_remaining,
                        }
                    )
    return policies


def _crypto_oos_prediction_rows(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    max_folds = max(0, int(settings.crypto_model_candidate_max_walk_forward_folds))
    folds = _crypto_walk_forward_folds(
        rows,
        min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)),
        max_folds=max_folds or None,
    )
    predicted_rows: list[dict[str, Any]] = []
    fold_summaries: list[dict[str, Any]] = []
    for fold in folds:
        model = _fit_crypto_calibration(
            fold["train_rows"],
            settings=settings,
            crypto_policy=crypto_policy,
            include_candidate_report=False,
        )
        for row in fold["test_rows"]:
            predicted_rows.append(
                {
                    **row,
                    "oos_predicted_yes": _predict_crypto_probability(row, model),
                    "oos_market_mid_yes": _decimal(row["mid_yes_dollars"]),
                }
            )
        fold_summaries.append(
            {
                "fold_id": fold["fold_id"],
                "train_rows": len(fold["train_rows"]),
                "test_rows": len(fold["test_rows"]),
                "train_cutoff_market_day": fold["train_cutoff_market_day"],
            }
        )
    return predicted_rows, fold_summaries


def _crypto_evaluate_oos_predictions_for_entry(
    predicted_rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy,
) -> tuple[dict[str, Any], dict[str, Any]]:
    model_trades: list[dict[str, Any]] = []
    market_mid_trades: list[dict[str, Any]] = []
    for row in predicted_rows:
        model_trade = _simulate_crypto_trade(
            row,
            _decimal(row["oos_predicted_yes"]),
            settings=settings,
            crypto_policy=crypto_policy,
        )
        if model_trade["status"] == "fillable":
            model_trades.append({**row, "simulation": model_trade})
        market_trade = _simulate_crypto_trade(
            row,
            _decimal(row["oos_market_mid_yes"]),
            settings=settings,
            crypto_policy=crypto_policy,
        )
        if market_trade["status"] == "fillable":
            market_mid_trades.append({**row, "simulation": market_trade})
    market_mid_metrics = _crypto_policy_metrics("market_mid_baseline", market_mid_trades, settings=settings)
    model_metrics = _crypto_candidate_policy_metrics(
        "candidate_quality_policy",
        model_trades,
        settings=settings,
        market_mid_net_pnl=_candidate_policy_net(market_mid_metrics),
    )
    return model_metrics, market_mid_metrics


def _crypto_optimizer_blockers(
    metrics: dict[str, Any],
    *,
    spot_coverage: float,
    crypto_policy: RuntimeCryptoPolicy,
) -> list[str]:
    blockers: list[str] = []
    selected_count = _candidate_policy_selected_count(metrics)
    if selected_count < crypto_policy.replay_min_trade_candidates:
        blockers.append(f"oos_trade_candidate_count {selected_count} < {crypto_policy.replay_min_trade_candidates}")
    if _candidate_policy_net(metrics) <= Decimal(str(crypto_policy.replay_min_net_pl_dollars)):
        blockers.append("net simulated P/L is not positive")
    if _candidate_policy_advantage(metrics) <= Decimal(str(crypto_policy.replay_min_pnl_advantage_dollars)):
        blockers.append("model simulated P/L does not beat market-mid baseline")
    if int(metrics.get("hard_cap_breaches") or 0) > crypto_policy.replay_max_hard_cap_breaches:
        blockers.append(
            f"hard_cap_breaches {int(metrics.get('hard_cap_breaches') or 0)} > {crypto_policy.replay_max_hard_cap_breaches}"
        )
    if spot_coverage < crypto_policy.replay_min_spot_coverage_pct:
        blockers.append(f"spot coverage {spot_coverage:.2%} < {crypto_policy.replay_min_spot_coverage_pct:.2%}")
    return blockers


def _crypto_optimization_sort_key(result: dict[str, Any]) -> tuple[Decimal, Decimal, int, int]:
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    return (
        _candidate_policy_net(metrics),
        _candidate_policy_advantage(metrics),
        _candidate_policy_selected_count(metrics),
        -len(result.get("blockers") or []),
    )


def _crypto_optimize_asset_entry_policy(
    asset_symbol: str,
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy,
    frequency: str | None = None,
) -> dict[str, Any]:
    asset = normalize_asset_symbol(asset_symbol)
    override_key = crypto_entry_override_key(asset, frequency)
    base_entry = crypto_policy.entry_for_asset(asset, frequency=frequency)
    predicted_rows, folds = _crypto_oos_prediction_rows(rows, settings=settings, crypto_policy=crypto_policy)
    strict_rows = sum(1 for row in rows if row.get("strict_trade_eligible"))
    spot_coverage = _spot_feature_coverage(rows)
    if not predicted_rows:
        return {
            "asset": asset,
            "override_key": override_key,
            "status": "blocked",
            "current_entry_policy": base_entry,
            "evaluated_policy_count": 0,
            "oos_evaluation_status": "insufficient_data",
            "oos_fold_count": 0,
            "strict_trade_eligible_count": strict_rows,
            "spot_feature_coverage_pct": spot_coverage,
            "winner": None,
            "best_policy": None,
            "blockers": ["oos_replay_unavailable"],
            "staged_override_payload": None,
        }
    evaluations: list[dict[str, Any]] = []
    for entry_policy in _crypto_entry_policy_grid(base_entry):
        candidate_policy = _runtime_crypto_policy_with_asset_entry(
            crypto_policy,
            asset,
            entry_policy,
            frequency=frequency,
        )
        metrics, market_mid_metrics = _crypto_evaluate_oos_predictions_for_entry(
            predicted_rows,
            settings=settings,
            crypto_policy=candidate_policy,
        )
        blockers = _crypto_optimizer_blockers(metrics, spot_coverage=spot_coverage, crypto_policy=crypto_policy)
        evaluations.append(
            {
                "entry_policy": entry_policy,
                "passed": not blockers,
                "blockers": blockers,
                "metrics": metrics,
                "market_mid_metrics": market_mid_metrics,
            }
        )
    passing = [item for item in evaluations if item["passed"]]
    passing.sort(key=_crypto_optimization_sort_key, reverse=True)
    evaluations.sort(key=_crypto_optimization_sort_key, reverse=True)
    winner = passing[0] if passing else None
    best = evaluations[0] if evaluations else None
    return {
        "asset": asset,
        "override_key": override_key,
        "status": "stageable" if winner else "blocked",
        "current_entry_policy": base_entry,
        "evaluated_policy_count": len(evaluations),
        "oos_evaluation_status": "ok",
        "oos_fold_count": len(folds),
        "strict_trade_eligible_count": strict_rows,
        "spot_feature_coverage_pct": spot_coverage,
        "winner": winner,
        "best_policy": best,
        "blockers": [] if winner else list(best.get("blockers") or ["no_policy_passed"]) if best else ["no_policy_evaluated"],
        "top_policies": evaluations[:10],
        "staged_override_payload": (
            {"crypto_policy": {"asset_entry_overrides": {override_key: winner["entry_policy"]}}}
            if winner
            else None
        ),
    }


def _evaluate_crypto_walk_forward(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    diagnostic_model: dict[str, Any] | None = None,
    empirical_bucket_requested_assets: list[str] | None = None,
    force_empirical_bucket_for_requested_assets: bool = False,
) -> dict[str, Any]:
    baseline_names = ("market_mid_baseline", "always_0_5", "last_direction", "naive_momentum", "linear_on_returns")
    support_model = diagnostic_model if isinstance(diagnostic_model, dict) and diagnostic_model else None
    if support_model is None and rows:
        support_model = _fit_crypto_calibration(
            rows,
            settings=settings,
            crypto_policy=crypto_policy,
            include_candidate_report=False,
        )
    diagnostic_quality = _crypto_candidate_quality_report(
        rows,
        support_model,
        settings=settings,
        crypto_policy=crypto_policy,
    )
    diagnostic_live_policy = diagnostic_quality["live_quality_policy"]
    diagnostic_shadow_policy = diagnostic_quality["shadow_exploration_policy"]
    last_minute_passive_price_matrix = _crypto_last_minute_passive_price_matrix(rows, settings=settings)
    max_folds = max(0, int(settings.crypto_model_candidate_max_walk_forward_folds))
    folds = _crypto_walk_forward_folds(
        rows,
        min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)),
        max_folds=max_folds or None,
    )
    if not folds:
        empty_metrics = _crypto_model_metrics([], {}, settings=settings, crypto_policy=crypto_policy)
        no_fold_market_count = _crypto_distinct_market_count(rows)
        empty_metrics.update(
            {
                "oos_evaluation_status": "insufficient_data",
                "oos_fold_count": 0,
                "oos_trade_candidate_count": 0,
                "oos_trade_candidate_market_count": 0,
                "independent_market_count": no_fold_market_count,
                "rows_per_market_mean": (len(rows) / no_fold_market_count) if no_fold_market_count else None,
                "resolved_market_count": no_fold_market_count,
                "strict_trade_eligible_market_count": _crypto_distinct_market_count(
                    [row for row in rows if row.get("strict_trade_eligible")]
                ),
                "trade_candidate_market_count": int(diagnostic_live_policy.get("selected_market_count") or 0),
                "current_model_live_quality_market_count": int(diagnostic_live_policy.get("selected_market_count") or 0),
                "oos_net_simulated_pl_dollars": 0.0,
                "oos_market_mid_net_simulated_pl_dollars": 0.0,
                "oos_pnl_advantage_vs_market_mid_dollars": 0.0,
                "sample_count": len(rows),
                "resolved_sample_count": len(rows),
                "prediction_eligible_count": sum(1 for row in rows if row.get("prediction_eligible", True)),
                "strict_trade_eligible_count": sum(1 for row in rows if row.get("strict_trade_eligible")),
                "proxy_quote_row_count": sum(1 for row in rows if row.get("quote_source") != "snapshot_quotes"),
                "real_quote_row_count": sum(1 for row in rows if row.get("quote_source") == "snapshot_quotes"),
                "spot_feature_coverage_pct": _spot_feature_coverage(rows),
                "trade_candidate_count": diagnostic_live_policy["selected_count"],
                "current_model_live_quality_candidate_count": diagnostic_live_policy["selected_count"],
                "live_quality_candidate_count": diagnostic_live_policy["selected_count"],
                "exploratory_shadow_candidate_count": diagnostic_quality["exploratory_shadow_count"],
                "diagnostic_net_simulated_pl_dollars": float(_decimal(diagnostic_live_policy["net_pnl"])),
                "diagnostic_shadow_net_simulated_pl_dollars": float(_decimal(diagnostic_shadow_policy["net_pnl"])),
                "candidate_status_counts": diagnostic_quality["candidate_status_counts"],
                "candidate_reason_counts": diagnostic_quality["candidate_reason_counts"],
                "top_candidate_status_counts": diagnostic_quality["top_candidate_status_counts"],
                "top_candidate_reason_counts": diagnostic_quality["top_candidate_reason_counts"],
                "candidate_rejection_reason_counts": diagnostic_quality["candidate_rejection_reason_counts"],
                "candidate_counts_by_asset": diagnostic_quality["by_asset"],
                "last_minute_passive_price_matrix": last_minute_passive_price_matrix,
                "last_minute_passive_price_matrix_count": len(last_minute_passive_price_matrix),
                **_crypto_passive_replay_metrics([], rows, settings=settings),
            }
        )
        empty_metrics = _crypto_apply_empirical_bucket_gate_to_replay_metrics(
            empty_metrics,
            selection_trades=[],
            market_mid_trades=[],
            bucket_matrix=[],
            settings=settings,
            crypto_policy=crypto_policy,
            requested_asset_symbols=empirical_bucket_requested_assets,
            force_requested_assets=force_empirical_bucket_for_requested_assets,
        )
        baseline_policies = [
            _crypto_policy_metrics(name, [], settings=settings)
            for name in baseline_names
        ]
        return {
            "status": "insufficient_data",
            "reason": "need_settled_point_in_time_crypto_rows_across_market_days",
            "fold_count": 0,
            "folds": [],
            "baseline_policy": _crypto_policy_metrics("market_mid_baseline", [], settings=settings),
            "baseline_policies": baseline_policies,
            "candidate_policies": [
                _crypto_policy_metrics("current_heuristic", [], settings=settings),
                _crypto_policy_metrics("calibrated_prediction", [], settings=settings),
                diagnostic_live_policy | {"policy_name": "candidate_quality_policy", "policy_family": "strict_candidate_quality"},
                diagnostic_live_policy | {"policy_name": "live_review_candidate", "policy_family": "live_review_candidate"},
                diagnostic_shadow_policy | {"policy_name": "shadow_exploration_policy", "policy_family": "shadow_exploration"},
            ],
            "bucket_matrix": [],
            "last_minute_passive_price_matrix": last_minute_passive_price_matrix,
            "bucket_diagnostics": _crypto_bucket_diagnostics([]),
            "candidate_quality": diagnostic_quality,
            "metrics": empty_metrics,
        }
    baseline_trades_by_name: dict[str, list[dict[str, Any]]] = {name: [] for name in baseline_names}
    baseline_predictions_by_name: dict[str, list[tuple[Decimal, int]]] = {name: [] for name in baseline_names}
    model_candidate_trades_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    model_candidate_predictions_by_name: dict[str, list[tuple[Decimal, int]]] = defaultdict(list)
    heuristic_trades: list[dict[str, Any]] = []
    calibrated_trades: list[dict[str, Any]] = []
    selection_trades: list[dict[str, Any]] = []
    exploratory_trades: list[dict[str, Any]] = []
    heuristic_predictions: list[tuple[Decimal, int]] = []
    calibrated_predictions: list[tuple[Decimal, int]] = []
    prediction_market_tickers: list[str] = []
    fold_summaries: list[dict[str, Any]] = []
    for fold in folds:
        model = _fit_crypto_calibration(
            fold["train_rows"],
            settings=settings,
            crypto_policy=crypto_policy,
            include_candidate_report=False,
        )
        heuristic_model = model.get("fallback_model") if isinstance(model.get("fallback_model"), dict) else _fit_crypto_heuristic_calibration(fold["train_rows"])
        linear_return_model = _fit_crypto_linear_return_baseline(fold["train_rows"])
        schema = _crypto_feature_schema(fold["train_rows"])
        defaults = _crypto_feature_defaults(fold["train_rows"])
        fallback = _fit_crypto_heuristic_calibration(fold["train_rows"])
        candidate_status = _fit_crypto_model_candidates(fold["train_rows"], schema=schema, defaults=defaults, fallback=fallback, settings=settings)
        available_candidate_models = {
            name: status["model"]
            for name, status in candidate_status.items()
            if status.get("status") == "available" and status.get("model") is not None
        }
        fold_baselines: dict[str, list[dict[str, Any]]] = {name: [] for name in baseline_names}
        fold_heuristic: list[dict[str, Any]] = []
        fold_calibrated: list[dict[str, Any]] = []
        fold_selection: list[dict[str, Any]] = []
        fold_exploratory: list[dict[str, Any]] = []
        for row in fold["test_rows"]:
            baseline_predictions: dict[str, Decimal] = {
                name: _crypto_baseline_probability(row, name, linear_model=linear_return_model)
                for name in baseline_names
            }
            heuristic = _predict_crypto_probability(row, heuristic_model)
            calibrated = _predict_crypto_probability(row, model)
            for name, candidate_model in available_candidate_models.items():
                candidate_prediction = _predict_crypto_probability(row, candidate_model)
                model_candidate_predictions_by_name[name].append((candidate_prediction, int(row["label_yes"])))
                candidate_trade = _simulate_crypto_trade(row, candidate_prediction, settings=settings, crypto_policy=crypto_policy)
                if candidate_trade["status"] == "fillable":
                    model_candidate_trades_by_name[name].append({**row, "simulation": candidate_trade})
            for name, prediction in baseline_predictions.items():
                baseline_predictions_by_name[name].append((prediction, int(row["label_yes"])))
            heuristic_predictions.append((heuristic, int(row["label_yes"])))
            calibrated_predictions.append((calibrated, int(row["label_yes"])))
            prediction_market_tickers.append(_crypto_row_market_ticker(row))
            baseline_trade_by_name = {
                name: _simulate_crypto_trade(row, prediction, settings=settings, crypto_policy=crypto_policy)
                for name, prediction in baseline_predictions.items()
            }
            heuristic_trade = _simulate_crypto_trade(row, heuristic, settings=settings, crypto_policy=crypto_policy)
            calibrated_trade = _simulate_crypto_trade(row, calibrated, settings=settings, crypto_policy=crypto_policy)
            exploratory_trade = _simulate_crypto_trade(
                row,
                calibrated,
                settings=settings,
                crypto_policy=crypto_policy,
                policy=CRYPTO_EXPLORATORY_SHADOW,
            )
            for name, trade in baseline_trade_by_name.items():
                if trade["status"] == "fillable":
                    fold_baselines[name].append({**row, "simulation": trade})
            if heuristic_trade["status"] == "fillable":
                fold_heuristic.append({**row, "simulation": heuristic_trade})
            if calibrated_trade["status"] == "fillable":
                fold_calibrated.append({**row, "simulation": calibrated_trade})
                fold_selection.append({**row, "simulation": calibrated_trade})
            if exploratory_trade["status"] == "fillable":
                fold_exploratory.append({**row, "simulation": exploratory_trade})
        for name, trades in fold_baselines.items():
            baseline_trades_by_name[name].extend(trades)
        heuristic_trades.extend(fold_heuristic)
        calibrated_trades.extend(fold_calibrated)
        selection_trades.extend(fold_selection)
        fold_exploratory_only = [
            row for row in fold_exploratory if (row.get("simulation") or {}).get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW
        ]
        exploratory_trades.extend(_cap_crypto_exploratory_rows(fold_exploratory_only, settings=settings))
        fold_summaries.append(
            {
                "fold_id": fold["fold_id"],
                "train_rows": len(fold["train_rows"]),
                "test_rows": len(fold["test_rows"]),
                "baseline_selected_count": len(fold_baselines["market_mid_baseline"]),
                "baseline_selected_counts": {name: len(trades) for name, trades in fold_baselines.items()},
                "current_heuristic_selected_count": len(fold_heuristic),
                "calibrated_selected_count": len(fold_calibrated),
                "trade_selection_selected_count": len(fold_selection),
                "shadow_exploration_selected_count": len(fold_exploratory_only),
                "train_cutoff_market_day": fold["train_cutoff_market_day"],
            }
        )
    baseline_policies = [
        _crypto_policy_metrics(name, baseline_trades_by_name[name], settings=settings)
        for name in baseline_names
    ]
    baseline_policy = baseline_policies[0]
    market_mid_net = _candidate_policy_net(baseline_policy)
    model_candidate_policies = [
        _crypto_candidate_policy_metrics(
            name,
            model_candidate_trades_by_name.get(name, []),
            settings=settings,
            market_mid_net_pnl=market_mid_net,
        )
        for name in CRYPTO_MODEL_CANDIDATE_NAMES
        if name in model_candidate_predictions_by_name or name in model_candidate_trades_by_name
    ]
    selected_model_policy = _crypto_select_model_policy_by_profit(model_candidate_policies) or _crypto_candidate_policy_metrics(
        "calibrated_prediction",
        calibrated_trades,
        settings=settings,
        market_mid_net_pnl=market_mid_net,
    )
    heuristic_policy = _crypto_policy_metrics("current_heuristic", heuristic_trades, settings=settings)
    calibrated_policy = _crypto_policy_metrics("calibrated_prediction", calibrated_trades, settings=settings)
    selection_policy = {
        **selected_model_policy,
        "policy_name": "candidate_quality_policy",
        "source_model_policy_name": selected_model_policy.get("policy_name"),
        "policy_family": "strict_candidate_quality",
    }
    live_review_policy = _crypto_policy_metrics("live_review_candidate", selection_trades, settings=settings)
    exploratory_policy = _crypto_policy_metrics("shadow_exploration_policy", exploratory_trades, settings=settings)
    probability = {
        "baseline": _probability_metrics_decimal(baseline_predictions_by_name["market_mid_baseline"]),
        "baselines": {
            name: _probability_metrics_decimal(predictions)
            for name, predictions in baseline_predictions_by_name.items()
        },
        "current_heuristic": _probability_metrics_decimal(heuristic_predictions),
        "calibrated": _probability_metrics_decimal(calibrated_predictions),
        "baseline_market_weighted": _probability_metrics_market_weighted(
            [
                (prediction, label, market_ticker)
                for (prediction, label), market_ticker in zip(
                    baseline_predictions_by_name["market_mid_baseline"], prediction_market_tickers, strict=True
                )
            ]
        ),
        "calibrated_market_weighted": _probability_metrics_market_weighted(
            [
                (prediction, label, market_ticker)
                for (prediction, label), market_ticker in zip(
                    calibrated_predictions, prediction_market_tickers, strict=True
                )
            ]
        ),
    }
    _winning_policy_name = selected_model_policy.get("policy_name")
    _bucket_trades = model_candidate_trades_by_name.get(_winning_policy_name or "") or calibrated_trades
    bucket_matrix = _crypto_bucket_matrix(_bucket_trades, settings=settings)
    bucket_diagnostics = _crypto_bucket_diagnostics(selection_trades)
    passive_replay_metrics = _crypto_passive_replay_metrics(selection_trades, rows, settings=settings)
    oos_by_asset: dict[str, int] = {}
    for _trade in selection_trades:
        _asset = str(_trade.get("asset_symbol") or "unknown")
        oos_by_asset[_asset] = oos_by_asset.get(_asset, 0) + 1
    return {
        "status": "ok",
        "fold_count": len(folds),
        "folds": fold_summaries,
        "prediction_metrics": probability,
        "baseline_policy": baseline_policy,
        "baseline_policies": baseline_policies,
        "model_candidate_policies": model_candidate_policies,
        "candidate_policies": [
            heuristic_policy,
            calibrated_policy,
            *model_candidate_policies,
            selection_policy,
            live_review_policy,
            exploratory_policy,
        ],
        "bucket_matrix": bucket_matrix,
        "last_minute_passive_price_matrix": last_minute_passive_price_matrix,
        "bucket_diagnostics": bucket_diagnostics,
        "candidate_quality": diagnostic_quality,
        "metrics": _crypto_apply_empirical_bucket_gate_to_replay_metrics(
            {
                "sample_count": len(rows),
                "resolved_sample_count": len(rows),
                "resolved_market_count": _crypto_distinct_market_count(rows),
                "independent_market_count": _crypto_distinct_market_count(rows),
                "rows_per_market_mean": (
                    len(rows) / _crypto_distinct_market_count(rows) if rows else None
                ),
                "prediction_eligible_count": sum(1 for row in rows if row.get("prediction_eligible", True)),
                "strict_trade_eligible_count": sum(1 for row in rows if row.get("strict_trade_eligible")),
                "strict_trade_eligible_market_count": _crypto_distinct_market_count(
                    [row for row in rows if row.get("strict_trade_eligible")]
                ),
                "proxy_quote_row_count": sum(1 for row in rows if row.get("quote_source") != "snapshot_quotes"),
                "real_quote_row_count": sum(1 for row in rows if row.get("quote_source") == "snapshot_quotes"),
                "spot_feature_coverage_pct": _spot_feature_coverage(rows),
                "trade_candidate_count": diagnostic_live_policy["selected_count"],
                "trade_candidate_market_count": int(diagnostic_live_policy.get("selected_market_count") or 0),
                "current_model_live_quality_candidate_count": diagnostic_live_policy["selected_count"],
                "current_model_live_quality_market_count": int(diagnostic_live_policy.get("selected_market_count") or 0),
                "live_quality_candidate_count": diagnostic_live_policy["selected_count"],
                "exploratory_shadow_candidate_count": diagnostic_quality["exploratory_shadow_count"],
                "oos_evaluation_status": "ok",
                "oos_fold_count": len(folds),
                "oos_trade_candidate_count": selection_policy["selected_count"],
                "oos_trade_candidate_market_count": int(selection_policy.get("selected_market_count") or 0),
                "oos_net_simulated_pl_dollars": float(_decimal(selection_policy["net_pnl"])),
                "oos_market_mid_net_simulated_pl_dollars": float(_decimal(baseline_policy["net_pnl"])),
                "oos_pnl_advantage_vs_market_mid_dollars": float(
                    _decimal(selection_policy["net_pnl"]) - _decimal(baseline_policy["net_pnl"])
                ),
                "diagnostic_net_simulated_pl_dollars": float(_decimal(diagnostic_live_policy["net_pnl"])),
                "diagnostic_shadow_net_simulated_pl_dollars": float(_decimal(diagnostic_shadow_policy["net_pnl"])),
                "net_simulated_pl_dollars": float(_decimal(selection_policy["net_pnl"])),
                "market_mid_net_simulated_pl_dollars": float(_decimal(baseline_policy["net_pnl"])),
                "pnl_advantage_vs_market_mid_dollars": float(
                    _decimal(selection_policy["net_pnl"]) - _decimal(baseline_policy["net_pnl"])
                ),
                "fees_dollars": float(_decimal(selection_policy["fees"])),
                "hard_cap_breaches": selection_policy["hard_cap_breaches"],
                "calibration_brier": probability["calibrated"]["brier"],
                "market_mid_brier": probability["baseline"]["brier"],
                "calibration_log_loss": probability["calibrated"]["log_loss"],
                "market_mid_log_loss": probability["baseline"]["log_loss"],
                "calibration_ece": probability["calibrated"]["ece"],
                "market_mid_ece": probability["baseline"]["ece"],
                "calibration_brier_market_weighted": probability["calibrated_market_weighted"]["brier"],
                "market_mid_brier_market_weighted": probability["baseline_market_weighted"]["brier"],
                "calibration_log_loss_market_weighted": probability["calibrated_market_weighted"]["log_loss"],
                "market_mid_log_loss_market_weighted": probability["baseline_market_weighted"]["log_loss"],
                "calibration_ece_market_weighted": probability["calibrated_market_weighted"]["ece"],
                "market_mid_ece_market_weighted": probability["baseline_market_weighted"]["ece"],
                "fee_model_version": current_fee_model_version(),
                "candidate_status_counts": diagnostic_quality["candidate_status_counts"],
                "candidate_reason_counts": diagnostic_quality["candidate_reason_counts"],
                "top_candidate_status_counts": diagnostic_quality["top_candidate_status_counts"],
                "top_candidate_reason_counts": diagnostic_quality["top_candidate_reason_counts"],
                "candidate_rejection_reason_counts": diagnostic_quality["candidate_rejection_reason_counts"],
                "candidate_counts_by_asset": diagnostic_quality["by_asset"],
                "last_minute_passive_price_matrix": last_minute_passive_price_matrix,
                "last_minute_passive_price_matrix_count": len(last_minute_passive_price_matrix),
                **passive_replay_metrics,
                "oos_trade_candidate_count_by_asset": oos_by_asset,
                "per_asset_metrics": {
                    asset: {"oos_trade_candidate_count": count}
                    for asset, count in oos_by_asset.items()
                },
            },
            selection_trades=selection_trades,
            market_mid_trades=baseline_trades_by_name["market_mid_baseline"],
            bucket_matrix=bucket_matrix,
            settings=settings,
            crypto_policy=crypto_policy,
            requested_asset_symbols=empirical_bucket_requested_assets,
            force_requested_assets=force_empirical_bucket_for_requested_assets,
        ),
    }


def _crypto_walk_forward_folds(
    rows: list[dict[str, Any]],
    *,
    min_train_rows: int,
    max_folds: int | None = None,
) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (str(row.get("market_day")), row.get("decision_ts") or datetime.max.replace(tzinfo=UTC)))
    days = sorted({str(row["market_day"]) for row in ordered if row.get("market_day")})
    fold_days = days[-max_folds:] if max_folds is not None and max_folds > 0 else days
    folds: list[dict[str, Any]] = []
    for day in fold_days:
        train = [row for row in ordered if str(row.get("market_day")) < day]
        test = [row for row in ordered if str(row.get("market_day")) == day]
        if len(train) < min_train_rows or not test:
            continue
        folds.append(
            {
                "fold_id": f"crypto-wf-{len(folds) + 1}",
                "train_cutoff_market_day": day,
                "train_rows": train,
                "test_rows": test,
            }
        )
    return folds


def _crypto_limit_replay_rows_for_oos(rows: list[dict[str, Any]], *, limit: int | None) -> list[dict[str, Any]]:
    if limit is None or limit <= 0 or len(rows) <= limit:
        return list(rows)
    ordered = sorted(rows, key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    no_day_rows: list[dict[str, Any]] = []
    for row in ordered:
        day = str(row.get("market_day") or "").strip()
        if day:
            by_day[day].append(row)
        else:
            no_day_rows.append(row)
    days = sorted(by_day)
    if len(days) < 2:
        return ordered[-limit:]

    latest_day = days[-1]
    prior_rows = [row for day in days[:-1] for row in by_day[day]]
    latest_rows = by_day[latest_day]
    if not prior_rows:
        return ordered[-limit:]

    prior_quota = min(len(prior_rows), max(1, limit // 2))
    latest_quota = min(len(latest_rows), max(0, limit - prior_quota))
    remaining = limit - prior_quota - latest_quota
    if remaining > 0:
        extra_prior = min(len(prior_rows) - prior_quota, remaining)
        prior_quota += extra_prior
        remaining -= extra_prior
    if remaining > 0:
        latest_quota += min(len(latest_rows) - latest_quota, remaining)
    selected = [*prior_rows[-prior_quota:], *latest_rows[-latest_quota:]]
    if len(selected) < limit and no_day_rows:
        selected = [*no_day_rows[-(limit - len(selected)):], *selected]
    return sorted(selected[-limit:], key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))


def _crypto_entry_policy_for_row(
    row: dict[str, Any],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any]:
    if crypto_policy is not None:
        entry = dict(
            crypto_policy.entry_for_asset(
                str(row.get("asset_symbol") or ""),
                frequency=_crypto_frequency_for_row(row),
            )
        )
        entry["min_fee_adjusted_edge_bps"] = max(
            int(entry["min_fee_adjusted_edge_bps"]),
            int(settings.risk_min_edge_bps),
        )
        entry["min_contract_price_dollars"] = max(
            float(entry["min_contract_price_dollars"]),
            float(settings.risk_min_contract_price_dollars),
        )
        entry["max_entry_price_dollars"] = max(0.0, float(settings.crypto_max_entry_price_dollars))
        entry["min_remaining_payout_bps"] = max(
            0,
            min(
                10000,
                int(
                    entry.get("min_remaining_payout_bps")
                    if entry.get("min_remaining_payout_bps") is not None
                    else CRYPTO_MIN_REMAINING_PAYOUT_BPS
                ),
            ),
        )
        return entry
    return {
        "min_fee_adjusted_edge_bps": int(settings.risk_min_edge_bps),
        "max_spread_bps": int(settings.crypto_live_max_spread_bps),
        "min_confidence": float(settings.risk_min_confidence),
        "min_contract_price_dollars": float(settings.risk_min_contract_price_dollars),
        "max_entry_price_dollars": max(0.0, float(settings.crypto_max_entry_price_dollars)),
        "min_remaining_payout_bps": CRYPTO_MIN_REMAINING_PAYOUT_BPS,
        "max_credible_edge_bps": int(settings.risk_max_credible_edge_bps),
    }


def crypto_edge_shrinkage_note_key(frequency: object) -> str:
    normalized = normalize_frequency(frequency) or "15m"
    return f"{CRYPTO_EDGE_SHRINKAGE_NOTE_PREFIX}:{normalized}"


def _crypto_edge_shrinkage_from_notes(
    notes: dict[str, Any] | None,
    *,
    frequency: object,
    settings: Settings,
) -> dict[str, Any] | None:
    if not settings.crypto_edge_shrinkage_enabled:
        return None
    if not isinstance(notes, dict):
        return None
    value = notes.get(crypto_edge_shrinkage_note_key(frequency))
    return value if isinstance(value, dict) and value else None


def _crypto_edge_shrinkage_review(
    *,
    raw_edge: Decimal,
    fee: Decimal,
    edge_shrinkage: dict[str, Any] | None,
    settings: Settings,
) -> dict[str, Any]:
    """Discount a candidate's raw edge by the fitted shrinkage beta.

    Diagnostics are always recorded when a shrinkage fit is available; the
    block flag only fires when enforcement is enabled, the fit status is ok,
    and the fit has enough live fills behind it.
    """
    review: dict[str, Any] = {
        "available": False,
        "enforced": False,
        "blocked": False,
        "beta": None,
        "fit_status": None,
        "sample_count": 0,
        "shrunk_edge_dollars": None,
        "shrunk_edge_bps": None,
        "shrunk_net_edge_dollars": None,
    }
    if not isinstance(edge_shrinkage, dict) or not edge_shrinkage:
        return review
    try:
        beta = Decimal(str(edge_shrinkage.get("beta")))
    except (ArithmeticError, TypeError, ValueError):
        return review
    fit_status = str(edge_shrinkage.get("status") or "")
    try:
        sample_count = int(edge_shrinkage.get("sample_count") or 0)
    except (TypeError, ValueError):
        sample_count = 0
    shrunk_edge = (raw_edge * beta).quantize(Decimal("0.0001"))
    shrunk_net_edge = shrunk_edge - fee
    review.update(
        {
            "available": True,
            "beta": float(beta),
            "fit_status": fit_status,
            "sample_count": sample_count,
            "shrunk_edge_dollars": str(shrunk_edge),
            "shrunk_edge_bps": int((shrunk_edge * Decimal("10000")).to_integral_value()),
            "shrunk_net_edge_dollars": str(shrunk_net_edge.quantize(Decimal("0.0001"))),
        }
    )
    enforced = (
        bool(settings.crypto_edge_shrinkage_enforce)
        and fit_status == EDGE_SHRINKAGE_STATUS_OK
        and sample_count >= int(settings.crypto_edge_shrinkage_min_fills)
    )
    review["enforced"] = enforced
    if enforced and shrunk_net_edge <= Decimal("0"):
        review["blocked"] = True
    return review


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _is_crypto_db_disconnect(exc: BaseException) -> bool:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__

    if any(bool(getattr(item, "connection_invalidated", False)) for item in chain):
        return True
    if any(isinstance(item, (BrokenPipeError, ConnectionRefusedError, ConnectionResetError, TimeoutError)) for item in chain):
        return True

    text = " ".join(
        f"{type(item).__name__}:{getattr(item, 'orig', '')} {item}" for item in chain
    ).lower()
    disconnect_markers = (
        "brokenpipeerror",
        "connectiondoesnotexisterror",
        "connection refused",
        "connection was closed",
        "connect call failed",
        "could not connect to server",
        "database system is shutting down",
        "database system is starting up",
        "not yet accepting connections",
        "server closed the connection unexpectedly",
        "terminating connection",
    )
    return any(marker in text for marker in disconnect_markers)


def _is_crypto_transient_network_error(exc: BaseException) -> bool:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__

    retryable_types = (
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.NetworkError,
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
        httpx.TimeoutException,
        ConnectionError,
        ConnectionResetError,
        TimeoutError,
    )
    if any(isinstance(item, retryable_types) for item in chain):
        return True
    text = " ".join(f"{type(item).__name__}: {item}" for item in chain).lower()
    transient_markers = (
        "name resolution",
        "temporary failure",
        "connection reset",
        "connection refused",
        "network is unreachable",
        "server disconnected",
        "timed out",
    )
    return any(marker in text for marker in transient_markers)


def _crypto_frequency_for_row(row: dict[str, Any]) -> str:
    frequency = normalize_frequency(
        row.get("frequency")
        or row.get("market_frequency")
        or row.get("settlement_frequency")
        or row.get("prediction_frequency")
    )
    if frequency:
        return frequency
    series_ticker = str(row.get("series_ticker") or "").upper()
    market_ticker = str(row.get("market_ticker") or "").upper()
    if "15M" in series_ticker or "15M" in market_ticker:
        return "15m"
    if "1H" in series_ticker or "1H" in market_ticker:
        return "1h"
    if series_ticker.endswith("D") or "-T" in market_ticker:
        return "1h"
    return "15m"


def _crypto_book_noise_reason(
    snapshots: list,
    *,
    settings: Settings,
) -> str | None:
    if not settings.crypto_book_noise_gate_enabled or len(snapshots) < 2:
        return None
    mids = [
        (s.yes_bid_dollars + s.yes_ask_dollars) / Decimal("2")
        for s in snapshots
        if s.yes_bid_dollars is not None and s.yes_ask_dollars is not None
    ]
    if len(mids) < 2:
        return None
    if (max(mids) - min(mids)) >= Decimal(str(settings.crypto_book_noise_yes_range_threshold)):
        return "crypto_market_book_noise"
    return None


def _crypto_live_entry_window_reason(row: dict[str, Any], *, settings: Settings) -> str | None:
    frequency = _crypto_frequency_for_row(row)
    try:
        interval_seconds = interval_seconds_for_frequency(frequency)
    except ValueError:
        interval_seconds = 900
    market_age = _optional_int(row.get("market_age_seconds"))
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    if market_age is None and time_to_close is not None and time_to_close <= interval_seconds:
        market_age = max(0, interval_seconds - time_to_close)
    if time_to_close is None and market_age is not None and market_age <= interval_seconds:
        time_to_close = max(0, interval_seconds - market_age)
    if market_age is None or time_to_close is None:
        return "crypto_entry_window_unknown"
    if market_age < max(0, int(settings.crypto_live_min_market_age_seconds)):
        return "crypto_market_too_early_for_live_entry"
    if time_to_close < crypto_autonomy_min_seconds_to_close_for_frequency(settings, frequency):
        return "crypto_market_too_late_for_live_entry"
    return None


def _crypto_late_sure_thing_candidate(
    *,
    side: str,
    model_winner: bool,
    probability: Decimal,
    live_entry_window_reason: str | None,
    row: dict[str, Any],
    settings: Settings,
) -> bool:
    if not bool(settings.crypto_late_sure_thing_enabled):
        return False
    if live_entry_window_reason == "crypto_market_too_early_for_live_entry":
        return False
    if not model_winner:
        return False
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    if time_to_close is None:
        return False
    if time_to_close <= 0:
        return False
    if time_to_close > max(0, int(settings.crypto_late_sure_thing_max_seconds_to_close)):
        return False
    min_probability = _crypto_late_sure_thing_min_probability(
        time_to_close_seconds=time_to_close,
        settings=settings,
    )
    if probability < min_probability:
        return False
    market_probability = _crypto_market_side_probability(row, side)
    if market_probability is None:
        return False
    min_market_probability = Decimal(str(settings.crypto_late_sure_thing_min_market_probability))
    return market_probability >= min_market_probability


def _crypto_late_sure_thing_min_probability(
    *,
    time_to_close_seconds: int,
    settings: Settings,
) -> Decimal:
    base_probability = Decimal(str(settings.crypto_late_sure_thing_min_probability))
    standard_max_seconds = max(0, int(settings.crypto_late_sure_thing_standard_max_seconds_to_close))
    if time_to_close_seconds <= standard_max_seconds:
        return base_probability
    extended_probability = Decimal(str(settings.crypto_late_sure_thing_extended_min_probability))
    return max(base_probability, extended_probability)


def _crypto_late_sure_thing_near_strike_momentum_guard(
    *,
    side: str,
    probability: Decimal,
    row: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    enabled = bool(settings.crypto_late_sure_thing_near_strike_momentum_guard_enabled)
    standard_max_seconds = max(0, int(settings.crypto_late_sure_thing_standard_max_seconds_to_close))
    max_moneyness = Decimal(str(settings.crypto_late_sure_thing_near_strike_max_moneyness_pct))
    min_adverse_return = Decimal(str(settings.crypto_late_sure_thing_near_strike_min_adverse_return_pct))
    min_adverse_returns = max(1, int(settings.crypto_late_sure_thing_near_strike_min_adverse_returns))
    min_probability = Decimal(str(settings.crypto_late_sure_thing_near_strike_min_probability))
    review: dict[str, Any] = {
        "enabled": enabled,
        "blocked": False,
        "applied": False,
        "reason": "disabled" if not enabled else "not_evaluated",
        "standard_max_seconds_to_close": standard_max_seconds,
        "max_moneyness_pct": str(max_moneyness),
        "min_adverse_return_pct": str(min_adverse_return),
        "min_adverse_returns": min_adverse_returns,
        "min_probability": str(min_probability),
    }
    if not enabled:
        return review
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    review["time_to_close_seconds"] = time_to_close
    if time_to_close is None:
        review["reason"] = "time_to_close_unknown"
        return review
    if time_to_close > standard_max_seconds:
        review["reason"] = "outside_standard_late_window"
        return review
    moneyness = _optional_decimal(row.get("spot_moneyness_pct"))
    review["spot_moneyness_pct"] = str(moneyness) if moneyness is not None else None
    if moneyness is None:
        review["reason"] = "spot_moneyness_unknown"
        return review
    if abs(moneyness) > max_moneyness:
        review["reason"] = "not_near_strike"
        return review
    returns: list[dict[str, Any]] = []
    adverse_returns = 0
    normalized_side = str(side or "").lower()
    for key in ("spot_return_1_pct", "spot_return_3_pct", "spot_return_6_pct", "spot_momentum_pct"):
        value = _optional_decimal(row.get(key))
        if value is None:
            continue
        adverse = (
            (normalized_side == "no" and value >= min_adverse_return)
            or (normalized_side == "yes" and value <= -min_adverse_return)
        )
        if adverse:
            adverse_returns += 1
        returns.append({"key": key, "value": str(value), "adverse": adverse})
    review["returns"] = returns
    review["adverse_return_count"] = adverse_returns
    if not returns:
        review["reason"] = "momentum_unknown"
        return review
    if adverse_returns < min_adverse_returns:
        review["reason"] = "momentum_not_adverse"
        return review
    review["applied"] = True
    if probability < min_probability:
        review["blocked"] = True
        review["reason"] = "near_strike_adverse_momentum_probability_below_min"
        return review
    review["reason"] = "near_strike_adverse_momentum_probability_allowed"
    return review


def _crypto_late_sure_thing_reversal_risk_review(
    *,
    side: str,
    row: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    reversal_enabled = bool(settings.crypto_late_sure_thing_reversal_guard_enabled)
    target_distance_enabled = bool(settings.crypto_late_sure_thing_target_distance_guard_enabled)
    min_seconds_to_close = max(0, int(settings.crypto_late_sure_thing_reversal_guard_min_seconds_to_close))
    min_adverse_return = Decimal(str(settings.crypto_late_sure_thing_reversal_guard_min_adverse_return_pct))
    min_target_distance = abs(Decimal(str(settings.crypto_late_sure_thing_min_target_distance_volatility)))
    review: dict[str, Any] = {
        "reversal_guard_enabled": reversal_enabled,
        "target_distance_guard_enabled": target_distance_enabled,
        "blocked": False,
        "applied": False,
        "reason": "not_evaluated",
        "min_seconds_to_close": min_seconds_to_close,
        "min_adverse_return_pct": str(min_adverse_return),
        "min_target_distance_volatility": str(min_target_distance),
    }
    normalized_side = str(side or "").lower()
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    review["time_to_close_seconds"] = time_to_close
    returns: list[dict[str, Any]] = []
    adverse_returns = 0
    for key in ("spot_return_1_pct", "spot_return_3_pct", "spot_return_6_pct", "spot_momentum_pct"):
        value = _optional_decimal(row.get(key))
        if value is None:
            continue
        adverse = (
            (normalized_side == "no" and value >= min_adverse_return)
            or (normalized_side == "yes" and value <= -min_adverse_return)
        )
        if adverse:
            adverse_returns += 1
        returns.append({"key": key, "value": str(value), "adverse": adverse})
    review["returns"] = returns
    review["adverse_return_count"] = adverse_returns
    if reversal_enabled:
        if time_to_close is None:
            review["reason"] = "time_to_close_unknown"
        elif time_to_close >= min_seconds_to_close and adverse_returns > 0:
            review["applied"] = True
            review["blocked"] = True
            review["reason"] = "extended_window_adverse_momentum"
            return review
    target_distance = _optional_decimal(row.get("spot_target_distance_volatility"))
    review["target_distance_volatility"] = str(target_distance) if target_distance is not None else None
    if target_distance_enabled:
        if target_distance is None:
            review["reason"] = review["reason"] if review["reason"] != "not_evaluated" else "target_distance_unknown"
            return review
        target_distance_ok = (
            (normalized_side == "no" and target_distance <= -min_target_distance)
            or (normalized_side == "yes" and target_distance >= min_target_distance)
        )
        review["target_distance_ok"] = target_distance_ok
        if not target_distance_ok:
            review["applied"] = True
            review["blocked"] = True
            review["reason"] = "target_distance_volatility_below_min"
            return review
    review["reason"] = "allowed"
    return review


def crypto_late_sure_thing_trace(candidate_trace: dict[str, Any] | None) -> bool:
    return bool(
        isinstance(candidate_trace, dict)
        and candidate_trace.get("late_high_confidence_directional_entry") is True
    )


def crypto_last_minute_passive_trace(candidate_trace: dict[str, Any] | None) -> bool:
    return bool(
        isinstance(candidate_trace, dict)
        and candidate_trace.get("last_minute_passive_market_confidence") is True
    )


def _crypto_last_minute_passive_enabled_for_asset(
    row: dict[str, Any],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> bool:
    if not bool(settings.crypto_last_minute_passive_enabled):
        return False
    asset = normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
    configured = _normalize_asset_csv(settings.crypto_last_minute_passive_assets)
    if not configured:
        return True
    if asset in configured:
        return True
    if "LIVE" not in configured:
        return False
    return (
        crypto_policy is not None
        and (crypto_policy.asset_modes or {}).get(asset) == CRYPTO_ASSET_MODE_LIVE
    )


def _crypto_last_minute_passive_review(
    *,
    side: str,
    row: dict[str, Any],
    cost: Decimal,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    price_matrix: list[dict[str, Any]] | None = None,
    min_contract_price: Decimal | None = None,
) -> dict[str, Any]:
    asset = normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
    bids = _crypto_last_minute_passive_bid_by_asset(settings)
    fixed_threshold = bids.get(asset)
    threshold = fixed_threshold
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    market_side_probability = _crypto_market_side_probability(row, side)
    max_seconds = max(0, int(settings.crypto_last_minute_passive_max_seconds_to_close))
    review: dict[str, Any] = {
        "enabled": bool(settings.crypto_last_minute_passive_enabled),
        "allowed": False,
        "reason": "last_minute_passive_not_allowed",
        "asset_symbol": asset,
        "side": side,
        "time_to_close_seconds": time_to_close,
        "max_seconds_to_close": max_seconds,
        "bid_threshold_dollars": _money_text(threshold),
        "fixed_fallback_bid_dollars": _money_text(fixed_threshold),
        "last_minute_price_source": "fixed_bid",
        "chosen_bid_dollars": _money_text(threshold),
        "price_matrix": None,
        "market_side_probability": (
            str(market_side_probability.quantize(Decimal("0.0001")))
            if market_side_probability is not None
            else None
        ),
        "current_side_ask_dollars": _money_text(cost),
        "require_no_cross": bool(settings.crypto_last_minute_passive_require_no_cross),
        "risk_mode": str(settings.crypto_last_minute_passive_risk_mode or ""),
    }
    if bool(settings.crypto_model_trained_replay_only):
        return {**review, "reason": "model_trained_replay_only"}
    if not review["enabled"]:
        return {**review, "reason": "last_minute_passive_disabled"}
    if not _crypto_last_minute_passive_enabled_for_asset(
        row,
        settings=settings,
        crypto_policy=crypto_policy,
    ):
        return {**review, "reason": "asset_not_configured_for_last_minute_passive"}
    if time_to_close is None:
        return {**review, "reason": "time_to_close_unknown"}
    if time_to_close <= 0:
        return {**review, "reason": "market_closed"}
    if time_to_close > max_seconds:
        return {**review, "reason": "outside_last_minute_passive_window"}
    if market_side_probability is None:
        return {**review, "reason": "market_side_probability_unknown"}
    matrix_review = _crypto_last_minute_passive_price_matrix_choice(
        side=side,
        row=row,
        current_cost=cost,
        market_side_probability=market_side_probability,
        min_contract_price=min_contract_price or Decimal("0.01"),
        settings=settings,
        price_matrix=price_matrix,
    )
    review["price_matrix"] = matrix_review
    if matrix_review.get("allowed") is True:
        threshold = _clamp_cent_price(_decimal(matrix_review["bid_price_dollars"]))
        review["bid_threshold_dollars"] = _money_text(threshold)
        review["chosen_bid_dollars"] = _money_text(threshold)
        review["last_minute_price_source"] = "learned_price_matrix"
        review["matrix_key"] = matrix_review.get("matrix_key")
        review["matrix_base_key"] = matrix_review.get("matrix_base_key")
        review["matrix_sample_count"] = matrix_review.get("sample_count")
        review["matrix_fill_count"] = matrix_review.get("fill_count")
        review["matrix_fill_rate"] = matrix_review.get("fill_rate")
        review["matrix_win_rate"] = matrix_review.get("win_rate")
        review["matrix_gross_pnl"] = matrix_review.get("gross_pnl")
        review["matrix_net_pnl"] = matrix_review.get("net_pnl")
        review["matrix_net_pnl_per_signal"] = matrix_review.get("net_pnl_per_signal")
        review["matrix_net_pnl_per_fill"] = matrix_review.get("net_pnl_per_fill")
    elif threshold is None:
        return {**review, "reason": "asset_missing_last_minute_passive_bid"}
    elif str(settings.crypto_last_minute_passive_price_matrix_fallback or "").strip().lower() != "fixed_bid":
        return {**review, "reason": matrix_review.get("reason") or "price_matrix_not_allowed"}
    if market_side_probability <= threshold:
        return {**review, "reason": "market_confidence_below_last_minute_bid"}
    if bool(settings.crypto_last_minute_passive_require_no_cross) and cost <= threshold:
        return {**review, "reason": "last_minute_passive_would_cross_touch"}
    expected_edge = market_side_probability - threshold
    return {
        **review,
        "allowed": True,
        "reason": CRYPTO_LAST_MINUTE_PASSIVE_REASON,
        "bid_threshold_dollars": _money_text(threshold),
        "chosen_bid_dollars": _money_text(threshold),
        "expected_edge_dollars": str(expected_edge.quantize(Decimal("0.0001"))),
    }


def _crypto_configured_market_price_anchor_weight(settings: Settings) -> Decimal:
    if not bool(settings.crypto_market_price_anchor_enabled):
        return Decimal("0")
    raw_weight = Decimal(str(settings.crypto_market_price_anchor_weight))
    return max(Decimal("0"), min(Decimal("1"), raw_weight))


def _crypto_market_price_anchor_weight(row: dict[str, Any], *, settings: Settings) -> Decimal:
    configured_weight = _crypto_configured_market_price_anchor_weight(settings)
    if configured_weight <= Decimal("0"):
        return Decimal("0")
    market_mid = row.get("mid_yes_dollars")
    if market_mid in (None, ""):
        return Decimal("0")
    try:
        market_probability = _clamp_price(_decimal(market_mid))
    except Exception:
        return Decimal("0")
    # Scale anchor weight by how far the market is from 50/50: extremes get full
    # weight, near-50 gets a floor (never zero) so the market still constrains
    # model predictions that deviate wildly from a balanced market price.
    # Floor of 0.30 ensures minimum 22.5% market anchoring at any price level,
    # preventing the model from predicting 12% YES when market prices 50%.
    raw_extremity = min(Decimal("1"), abs(market_probability - Decimal("0.5000")) / Decimal("0.2500"))
    extremity = Decimal("0.30") + Decimal("0.70") * raw_extremity
    return configured_weight * extremity


def _crypto_market_anchored_probability(
    row: dict[str, Any],
    predicted_yes: Decimal,
    *,
    settings: Settings,
) -> Decimal:
    weight = _crypto_market_price_anchor_weight(row, settings=settings)
    model_probability = _clamp_price(predicted_yes)
    if weight <= Decimal("0"):
        return model_probability
    market_mid = row.get("mid_yes_dollars")
    if market_mid in (None, ""):
        return model_probability
    try:
        market_probability = _clamp_price(_decimal(market_mid))
    except Exception:
        return model_probability
    return _clamp_price((market_probability * weight) + (model_probability * (Decimal("1") - weight)))


def _crypto_market_price_anchor_trace(row: dict[str, Any], raw_fair_yes: Decimal, anchored_fair_yes: Decimal, *, settings: Settings) -> dict[str, Any]:
    configured_weight = _crypto_configured_market_price_anchor_weight(settings)
    effective_weight = _crypto_market_price_anchor_weight(row, settings=settings)
    return {
        "enabled": bool(settings.crypto_market_price_anchor_enabled),
        "configured_weight": float(configured_weight),
        "effective_weight": float(effective_weight),
        "market_mid_yes_dollars": _crypto_market_mid_probability_text(row),
        "raw_fair_yes_dollars": _money_text(_clamp_price(raw_fair_yes)),
        "anchored_fair_yes_dollars": _money_text(_clamp_price(anchored_fair_yes)),
    }


def _crypto_market_mid_probability_text(row: dict[str, Any]) -> str | None:
    mid = row.get("mid_yes_dollars")
    if mid in (None, ""):
        return None
    try:
        return _money_text(_clamp_price(_decimal(mid)))
    except Exception:
        return None


def _crypto_market_side_probability(row: dict[str, Any], side: str) -> Decimal | None:
    mid = row.get("mid_yes_dollars")
    if mid in (None, ""):
        return None
    try:
        mid_yes = _clamp_price(_decimal(mid))
    except Exception:
        return None
    if str(side).lower() == "yes":
        return mid_yes
    if str(side).lower() == "no":
        return _clamp_price(Decimal("1.0000") - mid_yes)
    return None


def _crypto_side_ask(row: dict[str, Any], side: str) -> Decimal | None:
    key = "yes_ask_dollars" if str(side).lower() == "yes" else "no_ask_dollars"
    raw = row.get(key)
    if raw in (None, ""):
        return None
    try:
        return _clamp_price(_decimal(raw))
    except Exception:
        return None


def _crypto_side_bid(row: dict[str, Any], side: str) -> Decimal | None:
    key = "yes_bid_dollars" if str(side).lower() == "yes" else "no_bid_dollars"
    raw = row.get(key)
    if raw in (None, ""):
        return None
    try:
        return _clamp_price(_decimal(raw))
    except Exception:
        return None


def _crypto_last_minute_price_matrix_base_key(
    row: dict[str, Any],
    *,
    side: str,
    market_side_probability: Decimal | None = None,
) -> str | None:
    probability = market_side_probability
    if probability is None:
        probability = _crypto_market_side_probability(row, side)
    if probability is None:
        return None
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    if time_to_close is None:
        return None
    return "|".join(
        [
            normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
            str(side).lower(),
            _crypto_time_to_close_bucket(float(time_to_close)),
            _price_band(_clamp_price(probability)),
            _spread_band(row.get("spread_bps")),
        ]
    )


def _crypto_last_minute_price_matrix_key(
    row: dict[str, Any],
    *,
    side: str,
    bid: Decimal,
    market_side_probability: Decimal | None = None,
) -> str | None:
    base = _crypto_last_minute_price_matrix_base_key(
        row,
        side=side,
        market_side_probability=market_side_probability,
    )
    if base is None:
        return None
    return f"{base}|{_money_text(_clamp_cent_price(bid))}"


def _crypto_last_minute_passive_price_matrix_by_base(
    price_matrix: list[dict[str, Any]] | None,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in price_matrix or []:
        if not isinstance(row, dict):
            continue
        base_key = str(row.get("matrix_base_key") or "").strip()
        if not base_key:
            matrix_key = str(row.get("matrix_key") or "").strip()
            parts = matrix_key.split("|")
            if len(parts) >= 6:
                base_key = "|".join(parts[:5])
        if base_key:
            grouped[base_key].append(row)
    return grouped


def _crypto_last_minute_passive_price_matrix_choice(
    *,
    side: str,
    row: dict[str, Any],
    current_cost: Decimal,
    market_side_probability: Decimal,
    min_contract_price: Decimal,
    settings: Settings,
    price_matrix: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    review: dict[str, Any] = {
        "enabled": bool(settings.crypto_last_minute_passive_price_matrix_enabled),
        "status": "disabled",
        "allowed": False,
        "reason": "price_matrix_disabled",
        "matrix_base_key": None,
        "matrix_key": None,
        "candidate_count": 0,
        "qualified_count": 0,
        "min_samples": int(settings.crypto_last_minute_passive_price_matrix_min_samples),
        "min_fills": int(settings.crypto_last_minute_passive_price_matrix_min_fills),
        "min_fill_rate": float(settings.crypto_last_minute_passive_price_matrix_min_fill_rate),
        "min_net_pnl_dollars": str(
            Decimal(str(settings.crypto_last_minute_passive_price_matrix_min_net_pnl_dollars)).quantize(Decimal("0.0001"))
        ),
    }
    if not review["enabled"]:
        return review
    base_key = _crypto_last_minute_price_matrix_base_key(
        row,
        side=side,
        market_side_probability=market_side_probability,
    )
    review["matrix_base_key"] = base_key
    if base_key is None:
        return {**review, "status": "missing", "reason": "price_matrix_base_key_unavailable"}
    candidates = _crypto_last_minute_passive_price_matrix_by_base(price_matrix).get(base_key, [])
    review["candidate_count"] = len(candidates)
    if not candidates:
        return {**review, "status": "missing", "reason": "price_matrix_missing"}
    min_samples = int(settings.crypto_last_minute_passive_price_matrix_min_samples)
    min_fills = int(settings.crypto_last_minute_passive_price_matrix_min_fills)
    min_fill_rate = float(settings.crypto_last_minute_passive_price_matrix_min_fill_rate)
    min_net_pnl = Decimal(str(settings.crypto_last_minute_passive_price_matrix_min_net_pnl_dollars))
    qualified: list[dict[str, Any]] = []
    for candidate in candidates:
        bid_raw = candidate.get("bid_price_dollars")
        if bid_raw in (None, ""):
            continue
        try:
            bid = _clamp_cent_price(_decimal(bid_raw))
        except Exception:
            continue
        sample_count = int(candidate.get("sample_count") or 0)
        fill_count = int(candidate.get("fill_count") or 0)
        fill_rate = float(candidate.get("fill_rate") or 0.0)
        net_pnl = _decimal(candidate.get("net_pnl") or Decimal("0"))
        if bid >= current_cost:
            continue
        if bid < min_contract_price:
            continue
        if market_side_probability <= bid:
            continue
        if sample_count < min_samples:
            continue
        if fill_count < min_fills:
            continue
        if fill_rate < min_fill_rate:
            continue
        if net_pnl < min_net_pnl:
            continue
        qualified.append({**candidate, "_bid_decimal": bid})
    review["qualified_count"] = len(qualified)
    if not qualified:
        return {**review, "status": "blocked", "reason": "price_matrix_no_mature_profitable_bid"}
    qualified.sort(
        key=lambda candidate: (
            _decimal(candidate.get("net_pnl_per_signal") or Decimal("-999")),
            float(candidate.get("fill_rate") or 0.0),
            -float(candidate["_bid_decimal"]),
        ),
        reverse=True,
    )
    selected = qualified[0]
    bid = selected["_bid_decimal"]
    public_selected = {key: value for key, value in selected.items() if key != "_bid_decimal"}
    return {
        **review,
        "status": "allowed",
        "allowed": True,
        "reason": "price_matrix_bid_selected",
        "matrix_key": selected.get("matrix_key"),
        "bid_price_dollars": _money_text(bid),
        "selected": public_selected,
        "sample_count": int(selected.get("sample_count") or 0),
        "fill_count": int(selected.get("fill_count") or 0),
        "fill_rate": float(selected.get("fill_rate") or 0.0),
        "win_rate": selected.get("win_rate"),
        "gross_pnl": selected.get("gross_pnl"),
        "net_pnl": selected.get("net_pnl"),
        "net_pnl_per_signal": selected.get("net_pnl_per_signal"),
        "net_pnl_per_fill": selected.get("net_pnl_per_fill"),
    }


def _crypto_settlement_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    provider = row.get("spot_provider")
    source_kind = row.get("spot_source_kind")
    provider_text = str(provider or "").strip().lower()
    settlement_proxy = bool(provider_text == "coinbase" and not _crypto_spot_is_proxy(provider, source_kind))
    return {
        "settlement_benchmark_source": CRYPTO_SETTLEMENT_BENCHMARK_SOURCE,
        "model_input_spot_provider": provider,
        "model_input_spot_source_kind": source_kind,
        "settlement_proxy_for_cfb_rti": settlement_proxy,
        "settlement_diagnostic_codes": [CRYPTO_SETTLEMENT_PROXY_REASON_CODE] if settlement_proxy else [],
    }


def _touch_strategy_fee(price: Decimal, *, fee_rate: Decimal) -> Decimal:
    return estimate_kalshi_taker_fee_dollars(
        price_dollars=_clamp_price(price),
        count=Decimal("1.00"),
        fee_rate=fee_rate,
    )


def _touch_strategy_exit_price_for_net_profit(
    entry_cost: Decimal,
    *,
    target_pct: Decimal,
    fee_rate: Decimal,
) -> Decimal | None:
    entry_cost = _clamp_price(entry_cost)
    entry_fee = _touch_strategy_fee(entry_cost, fee_rate=fee_rate)
    denominator = entry_cost + entry_fee
    if denominator <= Decimal("0"):
        return None
    required_net_profit = denominator * target_pct
    low = entry_cost
    high = Decimal("0.9999")
    high_net = high - entry_cost - entry_fee - _touch_strategy_fee(high, fee_rate=fee_rate)
    if high_net < required_net_profit:
        return None
    for _ in range(32):
        mid = (low + high) / Decimal("2")
        net_profit = mid - entry_cost - entry_fee - _touch_strategy_fee(mid, fee_rate=fee_rate)
        if net_profit >= required_net_profit:
            high = mid
        else:
            low = mid
    return quantize_price(high)


def _touch_strategy_exit_price_for_net_loss(
    entry_cost: Decimal,
    *,
    stop_loss_pct: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    entry_cost = _clamp_price(entry_cost)
    entry_fee = _touch_strategy_fee(entry_cost, fee_rate=fee_rate)
    denominator = max(Decimal("0.0001"), entry_cost + entry_fee)
    allowed_net_loss = -(denominator * stop_loss_pct)
    low = Decimal("0.0001")
    high = entry_cost
    for _ in range(32):
        mid = (low + high) / Decimal("2")
        net_return = mid - entry_cost - entry_fee - _touch_strategy_fee(mid, fee_rate=fee_rate)
        if net_return <= allowed_net_loss:
            low = mid
        else:
            high = mid
    return quantize_price(low)


def _touch_strategy_entry_cost(row: dict[str, Any], side: str) -> Decimal | None:
    if side == "yes":
        value = row.get("yes_ask_dollars")
    else:
        value = row.get("no_ask_dollars")
    if value in (None, ""):
        return None
    try:
        cost = _clamp_price(_decimal(value))
    except Exception:
        return None
    if cost <= Decimal("0") or cost >= Decimal("1"):
        return None
    return cost


def _touch_strategy_target_yes_price(side: str, side_cost: Decimal) -> Decimal:
    return side_cost if side == "yes" else Decimal("1.0000") - side_cost


def _touch_strategy_allowed_spread(row: dict[str, Any], cost: Decimal, *, settings: Settings) -> tuple[bool, dict[str, Any]]:
    spread_bps = _optional_int(row.get("spread_bps"))
    max_spread_cents = Decimal(
        str(
            settings.crypto_touch_strategy_max_spread_cents_under_20c
            if cost < Decimal("0.2000")
            else settings.crypto_touch_strategy_max_spread_cents
        )
    )
    max_spread_bps = int((max_spread_cents * Decimal("10000")).to_integral_value())
    return (
        spread_bps is not None and spread_bps <= max_spread_bps,
        {
            "spread_bps": spread_bps,
            "max_spread_bps": max_spread_bps,
            "max_spread_cents": str(max_spread_cents.quantize(Decimal("0.0001"))),
            "tier": "under_20c" if cost < Decimal("0.2000") else "standard",
        },
    )


def _bounded_decimal(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _touch_strategy_probability(
    row: dict[str, Any],
    *,
    side: str,
    current_side_mid: Decimal,
    target_exit_price: Decimal,
    stop_exit_price: Decimal,
) -> Decimal:
    target_gap = max(Decimal("0.0001"), target_exit_price - current_side_mid)
    stop_gap = max(Decimal("0.0001"), current_side_mid - stop_exit_price)
    bracket_probability = stop_gap / (target_gap + stop_gap)
    time_to_close = max(0, _optional_int(row.get("time_to_close_seconds")) or 0)
    frequency = normalize_frequency(str(row.get("frequency") or "15m")) or "15m"
    interval_seconds = Decimal("3600") if frequency == "1h" else Decimal("900")
    time_scale = Decimal(str(math.sqrt(max(Decimal("0"), Decimal(time_to_close) / interval_seconds))))
    spot_vol = _decimal(row.get("spot_realized_volatility_32") or row.get("spot_realized_volatility") or Decimal("0"))
    vol_bonus = _bounded_decimal(spot_vol * time_scale * Decimal("50"), Decimal("0"), Decimal("0.1800"))
    side_multiplier = Decimal("1") if side == "yes" else Decimal("-1")
    return_1 = _decimal(row.get("spot_return_1_pct") or Decimal("0"))
    return_3 = _decimal(row.get("spot_return_3_pct") or Decimal("0"))
    momentum_bonus = _bounded_decimal(
        side_multiplier * ((return_1 * Decimal("30")) + (return_3 * Decimal("15"))),
        Decimal("-0.1200"),
        Decimal("0.1200"),
    )
    distance = _decimal(row.get("spot_target_distance_volatility") or Decimal("0"))
    moneyness_bonus = _bounded_decimal(side_multiplier * distance / Decimal("40"), Decimal("-0.0800"), Decimal("0.0800"))
    spread_penalty = Decimal(max(0, _optional_int(row.get("spread_bps")) or 0)) / Decimal("10000") / Decimal("4")
    probability = bracket_probability + vol_bonus + momentum_bonus + moneyness_bonus - spread_penalty
    return _bounded_decimal(probability, Decimal("0.0100"), Decimal("0.9900")).quantize(Decimal("0.0001"))


def _crypto_touch_strategy_candidates(
    row: dict[str, Any],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    empirical_bucket_matrix: list[dict[str, Any]] | None = None,
    enforce_empirical_bucket_gate: bool = False,
    btc_1h_touch_policy: bool = False,
) -> list[dict[str, Any]]:
    settlement_diagnostics = _crypto_settlement_diagnostics(row)
    policy_context = _crypto_touch_policy_context(
        row,
        settings=settings,
        btc_1h_touch_policy=btc_1h_touch_policy,
    )
    target_pct = Decimal(str(policy_context["target_pct"]))
    stop_pct = Decimal(str(policy_context["stop_pct"]))
    objective = str(policy_context["objective"])
    exit_objective = str(policy_context["exit_objective"])
    min_price = Decimal(str(settings.crypto_touch_strategy_min_contract_price_dollars))
    min_probability = Decimal(str(settings.crypto_touch_strategy_min_touch_probability))
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_policy = _crypto_entry_policy_for_row(row, settings=settings, crypto_policy=crypto_policy)
    min_live_edge = Decimal(int(entry_policy["min_fee_adjusted_edge_bps"])) / Decimal("10000")
    market_mid_probability = _crypto_market_mid_probability_text(row)
    anchor_weight = _crypto_market_price_anchor_weight(row, settings=settings)
    live_entry_window_reason = _crypto_touch_entry_window_reason(
        row,
        settings=settings,
        policy_context=policy_context,
    )
    require_empirical = bool(settings.crypto_touch_strategy_require_empirical_bucket)
    if row.get("strict_trade_eligible") is False:
        return [
            {
                "side": side,
                "status": "blocked",
                "candidate_status": "prediction_only_proxy_quote",
                "reason": row.get("execution_model_status") or "row_has_no_real_bid_ask_quotes",
                "edge_bps": None,
                "expected_net_edge": None,
                "target_yes_price_dollars": None,
                "rank": rank,
                "live_eligible": False,
                "objective": objective,
                **settlement_diagnostics,
            }
            for rank, side in enumerate(("yes", "no"), start=1)
        ]
    spot_status = str(row.get("spot_feature_status") or "").strip().lower()
    spot_proxy_only = bool(row.get("spot_proxy_only")) or _crypto_spot_is_proxy(
        row.get("spot_provider"),
        row.get("spot_source_kind"),
    )
    if spot_status != "available" or spot_proxy_only:
        reason = "spot_source_proxy_only" if spot_proxy_only else ("spot_data_stale" if spot_status == "stale" else "spot_data_missing_or_stale")
        return [
            {
                "side": side,
                "status": "blocked",
                "candidate_status": "prediction_only_proxy_quote",
                "reason": reason,
                "edge_bps": None,
                "expected_net_edge": None,
                "target_yes_price_dollars": None,
                "rank": rank,
                "live_eligible": False,
                "objective": objective,
                **settlement_diagnostics,
            }
            for rank, side in enumerate(("yes", "no"), start=1)
        ]

    candidates: list[dict[str, Any]] = []
    for side in ("yes", "no"):
        cost = _touch_strategy_entry_cost(row, side)
        current_side_mid = _crypto_market_side_probability(row, side)
        status = "blocked"
        candidate_status = "blocked_touch_strategy"
        reason = "missing_quote"
        edge_bps: int | None = None
        expected_net_edge: Decimal | None = None
        target_exit_price: Decimal | None = None
        stop_exit_price: Decimal | None = None
        touch_probability: Decimal | None = None
        spread_allowed = False
        spread_review: dict[str, Any] = {}
        empirical_bucket_gate: dict[str, Any] = {
            "status": "not_evaluated",
            "allowed": True,
            "enforced": False,
            "reason": "touch_strategy_empirical_bucket_not_required",
        }
        bucket_key = ""
        if cost is not None and current_side_mid is not None:
            spread_allowed, spread_review = _touch_strategy_allowed_spread(row, cost, settings=settings)
            target_exit_price = _touch_strategy_exit_price_for_net_profit(
                cost,
                target_pct=target_pct,
                fee_rate=fee_rate,
            )
            stop_exit_price = _touch_strategy_exit_price_for_net_loss(
                cost,
                stop_loss_pct=stop_pct,
                fee_rate=fee_rate,
            )
            bucket_key = _crypto_bucket_key(row, {"side": side, "execution_price_dollars": _money_text(cost)})
            if require_empirical:
                empirical_bucket_gate = _crypto_empirical_bucket_gate_for_candidate(
                    row,
                    bucket_key=bucket_key,
                    settings=settings,
                    crypto_policy=crypto_policy,
                    bucket_matrix=empirical_bucket_matrix,
                    enforce=enforce_empirical_bucket_gate,
                )
            if cost < min_price:
                reason = "touch_contract_price_below_min"
            elif target_exit_price is None:
                reason = "touch_target_profit_impossible_after_fees"
            elif target_exit_price >= Decimal("1.0000"):
                reason = "touch_target_above_max_payout"
            elif not spread_allowed:
                reason = "touch_spread_above_tier_max"
            elif live_entry_window_reason is not None:
                reason = live_entry_window_reason
            elif require_empirical and empirical_bucket_gate.get("status") not in {"allowed", "override_allowed"}:
                reason = "touch_empirical_bucket_not_allowed"
                candidate_status = "blocked_empirical_bucket"
            else:
                touch_probability = _touch_strategy_probability(
                    row,
                    side=side,
                    current_side_mid=current_side_mid,
                    target_exit_price=target_exit_price,
                    stop_exit_price=stop_exit_price,
                )
                downside_return = stop_pct
                if bool(policy_context.get("no_initial_hard_stop")):
                    downside_return = Decimal("0")
                expected_return = (touch_probability * target_pct) - ((Decimal("1") - touch_probability) * downside_return)
                expected_net_edge = expected_return * cost
                edge_bps = int((expected_return * Decimal("10000")).to_integral_value())
                if touch_probability < min_probability:
                    reason = "touch_probability_below_min"
                elif expected_return < min_live_edge:
                    reason = "touch_expected_edge_below_live_min"
                else:
                    status = "eligible"
                    candidate_status = CRYPTO_LIVE_QUALITY
                    reason = f"{objective}_target"
        candidates.append(
            {
                "side": side,
                "status": status,
                "candidate_status": candidate_status,
                "reason": reason,
                "target_yes_price_dollars": _money_text(_touch_strategy_target_yes_price(side, cost)) if cost is not None else None,
                "execution_price_dollars": _money_text(cost) if cost is not None else None,
                "edge_bps": edge_bps,
                "expected_net_edge": str(expected_net_edge.quantize(Decimal("0.0001"))) if expected_net_edge is not None else None,
                "model_probability": str(touch_probability) if touch_probability is not None else None,
                "raw_model_probability": str(touch_probability) if touch_probability is not None else None,
                "market_anchored_probability": str(touch_probability) if touch_probability is not None else None,
                "market_mid_probability": market_mid_probability,
                "market_side_probability": str(current_side_mid.quantize(Decimal("0.0001"))) if current_side_mid is not None else None,
                "market_price_anchor_weight": float(anchor_weight),
                "model_winner": touch_probability is not None and touch_probability >= Decimal("0.5000"),
                "raw_model_winner": touch_probability is not None and touch_probability >= Decimal("0.5000"),
                "expected_fee": None,
                "remaining_payout_dollars": str((Decimal("1") - cost).quantize(Decimal("0.0001"))) if cost is not None else None,
                "bucket_key": bucket_key or None,
                "empirical_bucket_gate": empirical_bucket_gate,
                "empirical_bucket_status": empirical_bucket_gate.get("status"),
                "spread_bps": row.get("spread_bps"),
                "touch_strategy": {
                    "enabled": True,
                    "policy": policy_context.get("policy_name"),
                    "objective": exit_objective,
                    "take_profit_pct": float(target_pct),
                    "stop_loss_pct": None if policy_context.get("no_initial_hard_stop") else float(stop_pct),
                    "no_initial_hard_stop": bool(policy_context.get("no_initial_hard_stop")),
                    "min_contract_price_dollars": str(min_price.quantize(Decimal("0.0001"))),
                    "entry_cost_dollars": _money_text(cost) if cost is not None else None,
                    "current_side_mid_dollars": _money_text(current_side_mid) if current_side_mid is not None else None,
                    "target_exit_side_price_dollars": _money_text(target_exit_price) if target_exit_price is not None else None,
                    "stop_exit_side_price_dollars": (
                        None
                        if policy_context.get("no_initial_hard_stop")
                        else _money_text(stop_exit_price) if stop_exit_price is not None else None
                    ),
                    "target_exit_yes_price_dollars": (
                        _money_text(_touch_strategy_target_yes_price(side, target_exit_price))
                        if target_exit_price is not None
                        else None
                    ),
                    "stop_exit_yes_price_dollars": (
                        None
                        if policy_context.get("no_initial_hard_stop")
                        else (
                            _money_text(_touch_strategy_target_yes_price(side, stop_exit_price))
                            if stop_exit_price is not None
                            else None
                        )
                    ),
                    "touch_probability": str(touch_probability) if touch_probability is not None else None,
                    "min_touch_probability": str(min_probability.quantize(Decimal("0.0001"))),
                    "spread_guard": spread_review,
                    "empirical_bucket_required": require_empirical,
                    "min_market_age_seconds": policy_context.get("min_market_age_seconds"),
                    "min_seconds_to_close": policy_context.get("min_seconds_to_close"),
                },
                "runtime_thresholds": dict(entry_policy),
                "time_to_close_seconds": row.get("time_to_close_seconds"),
                "live_entry_window_reason": live_entry_window_reason,
                "rank": None,
                "live_eligible": candidate_status == CRYPTO_LIVE_QUALITY,
                "objective": objective,
                **settlement_diagnostics,
            }
        )
    candidates.sort(
        key=lambda item: (
            item.get("candidate_status") == CRYPTO_LIVE_QUALITY,
            _decimal(item.get("model_probability") or Decimal("-1")),
            _decimal(item.get("expected_net_edge") or Decimal("-999")),
            item["side"],
        ),
        reverse=True,
    )
    for idx, candidate in enumerate(candidates, start=1):
        candidate["rank"] = idx
    return candidates


def _crypto_trade_candidates(
    row: dict[str, Any],
    predicted_yes: Decimal,
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    require_spot_features: bool = True,
    empirical_bucket_matrix: list[dict[str, Any]] | None = None,
    last_minute_passive_price_matrix: list[dict[str, Any]] | None = None,
    enforce_empirical_bucket_gate: bool = False,
    empirical_bucket_requested_assets: list[str] | None = None,
    force_empirical_bucket_for_requested_assets: bool = False,
    edge_shrinkage: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    settlement_diagnostics = _crypto_settlement_diagnostics(row)
    raw_predicted_yes = _clamp_price(predicted_yes)
    anchored_predicted_yes = _crypto_market_anchored_probability(row, raw_predicted_yes, settings=settings)
    predicted_winner_side = "yes" if anchored_predicted_yes >= Decimal("0.5000") else "no"
    raw_predicted_winner_side = "yes" if raw_predicted_yes >= Decimal("0.5000") else "no"
    anchor_weight = _crypto_market_price_anchor_weight(row, settings=settings)
    market_mid_probability = _crypto_market_mid_probability_text(row)
    if row.get("strict_trade_eligible") is False:
        return [
            {
                "side": side,
                "status": "blocked",
                "candidate_status": "prediction_only_proxy_quote",
                "reason": row.get("execution_model_status") or "row_has_no_real_bid_ask_quotes",
                "edge_bps": None,
                "expected_net_edge": None,
                "model_probability": None,
                "raw_model_probability": None,
                "market_anchored_probability": None,
                "market_mid_probability": market_mid_probability,
                "market_price_anchor_weight": float(anchor_weight),
                "raw_model_winner": side == raw_predicted_winner_side,
                "model_winner": side == predicted_winner_side,
                "target_yes_price_dollars": None,
                "spread_bps": row.get("spread_bps"),
                "rank": rank,
                "live_eligible": False,
                **settlement_diagnostics,
            }
            for rank, side in enumerate(("yes", "no"), start=1)
        ]
    spot_status = str(row.get("spot_feature_status") or "").strip().lower()
    spot_proxy_only = bool(row.get("spot_proxy_only")) or _crypto_spot_is_proxy(
        row.get("spot_provider"),
        row.get("spot_source_kind"),
    )
    if require_spot_features and (spot_status != "available" or spot_proxy_only):
        reason = "spot_data_missing_or_stale"
        if spot_proxy_only:
            reason = "spot_source_proxy_only"
        elif spot_status == "stale":
            reason = "spot_data_stale"
        return [
            {
                "side": side,
                "status": "blocked",
                "candidate_status": "prediction_only_proxy_quote",
                "reason": reason,
                "edge_bps": None,
                "expected_net_edge": None,
                "model_probability": None,
                "raw_model_probability": None,
                "market_anchored_probability": None,
                "market_mid_probability": market_mid_probability,
                "market_price_anchor_weight": float(anchor_weight),
                "raw_model_winner": side == raw_predicted_winner_side,
                "model_winner": side == predicted_winner_side,
                "target_yes_price_dollars": None,
                "spread_bps": row.get("spread_bps"),
                "spot_feature_status": row.get("spot_feature_status"),
                "spot_provider": row.get("spot_provider"),
                "spot_source_kind": row.get("spot_source_kind"),
                "spot_stale_seconds": row.get("spot_stale_seconds"),
                "spot_exchange_spread_bps": row.get("spot_exchange_spread_bps"),
                "spot_exchange_recent_trade_count": row.get("spot_exchange_recent_trade_count"),
                "rank": rank,
                "live_eligible": False,
                **settlement_diagnostics,
            }
            for rank, side in enumerate(("yes", "no"), start=1)
        ]
    quote_inputs = [
        ("yes", _decimal(row.get("yes_ask_dollars")) if row.get("yes_ask_dollars") is not None else None),
        ("no", _decimal(row.get("no_ask_dollars")) if row.get("no_ask_dollars") is not None else None),
    ]
    entry_policy = _crypto_entry_policy_for_row(row, settings=settings, crypto_policy=crypto_policy)
    min_live_edge = Decimal(int(entry_policy["min_fee_adjusted_edge_bps"])) / Decimal("10000")
    max_live_spread = int(entry_policy["max_spread_bps"])
    min_contract_price = Decimal(str(entry_policy["min_contract_price_dollars"]))
    max_entry_price = Decimal(str(entry_policy.get("max_entry_price_dollars") or "0"))
    min_remaining_payout = Decimal(int(entry_policy["min_remaining_payout_bps"])) / Decimal("10000")
    max_credible_edge_bps = int(entry_policy["max_credible_edge_bps"])
    min_shadow_edge = Decimal(str(settings.crypto_shadow_exploration_min_expected_net_edge_dollars))
    max_shadow_spread = int(settings.crypto_shadow_exploration_max_spread_bps)
    spread_bps = int(row.get("spread_bps") or 0)
    live_entry_window_reason = _crypto_live_entry_window_reason(row, settings=settings)
    for side, cost in quote_inputs:
        if cost is None:
            candidates.append(
                {
                    "side": side,
                    "status": "blocked",
                    "candidate_status": "unfillable",
                    "reason": "missing_quote",
                    "edge_bps": None,
                    "expected_net_edge": None,
                    "model_probability": None,
                    "raw_model_probability": None,
                    "market_anchored_probability": None,
                    "market_mid_probability": market_mid_probability,
                    "market_price_anchor_weight": float(anchor_weight),
                    "model_winner": side == predicted_winner_side,
                    "raw_model_winner": side == raw_predicted_winner_side,
                    "target_yes_price_dollars": None,
                    **settlement_diagnostics,
                }
            )
            continue
        probability = anchored_predicted_yes if side == "yes" else Decimal("1.0000") - anchored_predicted_yes
        raw_probability = raw_predicted_yes if side == "yes" else Decimal("1.0000") - raw_predicted_yes
        raw_edge = probability - cost
        fee = estimate_kalshi_taker_fee_dollars(
            price_dollars=cost,
            count=Decimal("1.00"),
            fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
        )
        expected_net_edge = raw_edge - fee
        target_yes = cost if side == "yes" else Decimal("1.0000") - cost
        remaining_payout = Decimal("1.0000") - cost
        raw_edge_bps = int((raw_edge * Decimal("10000")).to_integral_value())
        model_winner = side == predicted_winner_side
        raw_model_winner = side == raw_predicted_winner_side
        market_side_probability = _crypto_market_side_probability(row, side)
        last_minute_passive_review = _crypto_last_minute_passive_review(
            side=side,
            row=row,
            cost=cost,
            settings=settings,
            crypto_policy=crypto_policy,
            price_matrix=last_minute_passive_price_matrix,
            min_contract_price=min_contract_price,
        )
        late_sure_thing = _crypto_late_sure_thing_candidate(
            side=side,
            model_winner=model_winner,
            probability=probability,
            live_entry_window_reason=live_entry_window_reason,
            row=row,
            settings=settings,
        )
        late_near_strike_momentum_guard = _crypto_late_sure_thing_near_strike_momentum_guard(
            side=side,
            probability=probability,
            row=row,
            settings=settings,
        )
        late_reversal_risk_review = _crypto_late_sure_thing_reversal_risk_review(
            side=side,
            row=row,
            settings=settings,
        )
        late_sure_thing_base = late_sure_thing
        if late_reversal_risk_review.get("blocked") is True:
            late_sure_thing = False
        if late_near_strike_momentum_guard.get("blocked") is True:
            late_sure_thing = False
        candidate_status = "blocked_fee_edge"
        status = "blocked"
        reason = "fee_adjusted_edge_below_live_min"
        if spread_bps > max_live_spread:
            reason = "spread_above_live_max"
        elif cost < min_contract_price:
            reason = "contract_price_below_crypto_min"
        elif min_remaining_payout > 0 and remaining_payout < min_remaining_payout:
            reason = "remaining_payout_below_crypto_min"
        elif raw_edge_bps > max_credible_edge_bps:
            reason = "edge_above_crypto_credible_max"
        elif late_sure_thing and expected_net_edge >= min_live_edge:
            status = "eligible"
            candidate_status = CRYPTO_LIVE_QUALITY
            reason = "late_high_confidence_directional_entry"
        elif late_sure_thing_base and late_reversal_risk_review.get("blocked") is True:
            candidate_status = "blocked_late_reversal_risk"
            reason = "late_high_confidence_reversal_risk_guard"
        elif late_sure_thing_base and late_near_strike_momentum_guard.get("blocked") is True:
            candidate_status = "blocked_near_strike_momentum"
            reason = "late_high_confidence_near_strike_momentum_guard"
        elif expected_net_edge >= min_live_edge and live_entry_window_reason is None:
            status = "eligible"
            candidate_status = CRYPTO_LIVE_QUALITY
            reason = "positive_fee_adjusted_live_quality_edge"
        elif not late_sure_thing and expected_net_edge >= min_shadow_edge and spread_bps <= max_shadow_spread:
            status = "eligible"
            candidate_status = CRYPTO_EXPLORATORY_SHADOW
            reason = live_entry_window_reason or "broad_shadow_exploration"
        elif spread_bps > max_shadow_spread:
            reason = "spread_above_shadow_exploration_max"
        execution_cost = cost
        last_minute_passive = last_minute_passive_review.get("allowed") is True
        if last_minute_passive:
            execution_cost = _decimal(last_minute_passive_review["bid_threshold_dollars"])
            target_yes = execution_cost if side == "yes" else Decimal("1.0000") - execution_cost
            remaining_payout = Decimal("1.0000") - execution_cost
            market_edge = (market_side_probability or Decimal("0")) - execution_cost
            fee = estimate_kalshi_taker_fee_dollars(
                price_dollars=execution_cost,
                count=Decimal("1.00"),
                fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
            )
            expected_net_edge = market_edge - fee
            raw_edge_bps = int((market_edge * Decimal("10000")).to_integral_value())
            status = "eligible"
            candidate_status = CRYPTO_LIVE_QUALITY
            reason = CRYPTO_LAST_MINUTE_PASSIVE_REASON
        elif (
            last_minute_passive_review.get("reason") == "last_minute_passive_would_cross_touch"
            and candidate_status != CRYPTO_LIVE_QUALITY
        ):
            candidate_status = "blocked_last_minute_passive"
            reason = "last_minute_passive_would_cross_touch"
        if (
            max_entry_price > 0
            and execution_cost > max_entry_price
            and candidate_status in {CRYPTO_LIVE_QUALITY, CRYPTO_EXPLORATORY_SHADOW}
        ):
            status = "blocked"
            candidate_status = "blocked_max_entry_price"
            reason = "contract_price_above_crypto_max_entry"
        edge_shrinkage_review = _crypto_edge_shrinkage_review(
            raw_edge=expected_net_edge + fee,
            fee=fee,
            edge_shrinkage=edge_shrinkage,
            settings=settings,
        )
        if (
            edge_shrinkage_review.get("blocked") is True
            and candidate_status in {CRYPTO_LIVE_QUALITY, CRYPTO_EXPLORATORY_SHADOW}
        ):
            status = "blocked"
            candidate_status = "blocked_shrunk_edge"
            reason = "shrunk_fee_adjusted_edge_not_positive"
        bucket_key = _crypto_bucket_key(row, {"side": side, "execution_price_dollars": _money_text(execution_cost)})
        pre_empirical_status = candidate_status
        pre_empirical_reason = reason
        empirical_bucket_gate = _crypto_empirical_bucket_gate_for_candidate(
            row,
            bucket_key=bucket_key,
            settings=settings,
            crypto_policy=crypto_policy,
            bucket_matrix=empirical_bucket_matrix,
            enforce=enforce_empirical_bucket_gate,
            requested_asset_symbols=empirical_bucket_requested_assets,
            force_requested_assets=force_empirical_bucket_for_requested_assets,
        )
        empirical_bucket_late_override = _crypto_empirical_late_override_review(
            row,
            empirical_bucket_gate,
            pre_empirical_reason=pre_empirical_reason,
            late_sure_thing=late_sure_thing,
            settings=settings,
        )
        if (
            candidate_status == CRYPTO_LIVE_QUALITY
            and not last_minute_passive
            and empirical_bucket_gate.get("enforced") is True
            and empirical_bucket_gate.get("status") != "allowed"
        ):
            empirical_bucket_gate = _crypto_empirical_gate_with_late_override(
                empirical_bucket_gate,
                empirical_bucket_late_override,
            )
            if empirical_bucket_gate.get("status") != "override_allowed":
                status = "blocked"
                candidate_status = "blocked_empirical_bucket"
                reason = "empirical_bucket_not_allowed"
        candidates.append(
            {
                "side": side,
                "status": status,
                "candidate_status": candidate_status,
                "reason": reason,
                "pre_empirical_candidate_status": pre_empirical_status,
                "pre_empirical_reason": pre_empirical_reason,
                "target_yes_price_dollars": _money_text(_clamp_price(target_yes)),
                "execution_price_dollars": _money_text(_clamp_price(execution_cost)),
                "edge_bps": raw_edge_bps,
                "expected_net_edge": str(expected_net_edge.quantize(Decimal("0.0001"))),
                "model_probability": str(probability.quantize(Decimal("0.0001"))),
                "raw_model_probability": str(raw_probability.quantize(Decimal("0.0001"))),
                "market_anchored_probability": str(probability.quantize(Decimal("0.0001"))),
                "market_mid_probability": market_mid_probability,
                "market_side_probability": str(market_side_probability.quantize(Decimal("0.0001")))
                if market_side_probability is not None
                else None,
                "market_price_anchor_weight": float(anchor_weight),
                "model_winner": model_winner,
                "raw_model_winner": raw_model_winner,
                "expected_fee": str(fee.quantize(Decimal("0.0001"))),
                "edge_shrinkage": edge_shrinkage_review,
                "shrunk_edge_bps": edge_shrinkage_review.get("shrunk_edge_bps"),
                "remaining_payout_dollars": str(remaining_payout.quantize(Decimal("0.0001"))),
                "bucket_key": bucket_key,
                "empirical_bucket_gate": empirical_bucket_gate,
                "empirical_bucket_status": empirical_bucket_gate.get("status"),
                "empirical_bucket_late_override": empirical_bucket_late_override,
                "empirical_bucket_gap_sample": _crypto_empirical_bucket_gap_sample(
                    row,
                    side=side,
                    cost=execution_cost,
                    target_yes=target_yes,
                    pre_empirical_status=pre_empirical_status,
                    pre_empirical_reason=pre_empirical_reason,
                    candidate_status=candidate_status,
                    reason=reason,
                    edge_bps=raw_edge_bps,
                    expected_net_edge=expected_net_edge,
                    model_probability=probability,
                    raw_model_probability=raw_probability,
                    market_side_probability=market_side_probability,
                    fee=fee,
                    remaining_payout=remaining_payout,
                    bucket_key=bucket_key,
                    empirical_bucket_gate=empirical_bucket_gate,
                    empirical_bucket_late_override=empirical_bucket_late_override,
                    late_sure_thing=late_sure_thing,
                ),
                "spread_bps": spread_bps,
                "spot_exchange_spread_bps": row.get("spot_exchange_spread_bps"),
                "spot_exchange_recent_trade_count": row.get("spot_exchange_recent_trade_count"),
                "runtime_thresholds": dict(entry_policy),
                "market_age_seconds": row.get("market_age_seconds"),
                "time_to_close_seconds": row.get("time_to_close_seconds"),
                "live_entry_window_reason": live_entry_window_reason,
                "late_high_confidence_directional_entry": late_sure_thing,
                "late_high_confidence_near_strike_momentum_guard": late_near_strike_momentum_guard,
                "late_high_confidence_reversal_risk": late_reversal_risk_review,
                "last_minute_passive_market_confidence": last_minute_passive,
                "last_minute_passive": last_minute_passive_review,
                "last_minute_passive_bid_threshold_dollars": last_minute_passive_review.get("bid_threshold_dollars"),
                "last_minute_price_source": last_minute_passive_review.get("last_minute_price_source"),
                "last_minute_chosen_bid_dollars": last_minute_passive_review.get("chosen_bid_dollars"),
                "last_minute_fixed_fallback_bid_dollars": last_minute_passive_review.get("fixed_fallback_bid_dollars"),
                "last_minute_price_matrix_key": last_minute_passive_review.get("matrix_key"),
                "last_minute_price_matrix_base_key": last_minute_passive_review.get("matrix_base_key"),
                "last_minute_price_matrix_sample_count": last_minute_passive_review.get("matrix_sample_count"),
                "last_minute_price_matrix_fill_count": last_minute_passive_review.get("matrix_fill_count"),
                "last_minute_price_matrix_fill_rate": last_minute_passive_review.get("matrix_fill_rate"),
                "last_minute_price_matrix_net_pnl": last_minute_passive_review.get("matrix_net_pnl"),
                "last_minute_price_matrix_net_pnl_per_signal": last_minute_passive_review.get("matrix_net_pnl_per_signal"),
                "last_minute_passive_no_cross": (
                    last_minute_passive_review.get("reason") != "last_minute_passive_would_cross_touch"
                ),
                "low_price_shadow_diagnostic": execution_cost < Decimal("0.5000"),
                "rank": None,
                "live_eligible": candidate_status == CRYPTO_LIVE_QUALITY,
                **settlement_diagnostics,
            }
        )
    candidates.sort(
        key=lambda item: (
            item.get("last_minute_passive_market_confidence") is True,
            item.get("model_winner") is True,
            _decimal(item.get("model_probability") or Decimal("-1")),
            _decimal(item.get("expected_net_edge") or Decimal("-999")),
            item["side"],
        ),
        reverse=True,
    )
    for idx, candidate in enumerate(candidates, start=1):
        candidate["rank"] = idx
    return candidates


def _crypto_shadow_ranked_fallback(candidates: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any] | None:
    max_shadow_spread = int(settings.crypto_shadow_exploration_max_spread_bps)
    for candidate in candidates:
        if candidate.get("execution_price_dollars") is None:
            continue
        if int(candidate.get("spread_bps") or 0) > max_shadow_spread:
            continue
        selected = dict(candidate)
        selected["status"] = "eligible"
        selected["candidate_status"] = CRYPTO_EXPLORATORY_SHADOW
        selected["reason"] = "ranked_shadow_exploration_below_min_edge"
        selected["live_eligible"] = False
        selected["shadow_floor_bypassed"] = True
        return selected
    return None


def _crypto_preferred_candidate_for_statuses(
    candidates: list[dict[str, Any]],
    allowed_statuses: set[str],
) -> dict[str, Any] | None:
    if not candidates or not allowed_statuses:
        return None
    for candidate in candidates:
        if (
            candidate.get("last_minute_passive_market_confidence") is True
            and candidate.get("candidate_status") in allowed_statuses
        ):
            return candidate
    if CRYPTO_LIVE_QUALITY in allowed_statuses:
        for candidate in candidates:
            if candidate.get("candidate_status") == CRYPTO_LIVE_QUALITY:
                return candidate
    for candidate in candidates:
        if candidate.get("candidate_status") in allowed_statuses:
            return candidate
    return None


def _simulate_crypto_trade(
    row: dict[str, Any],
    predicted_yes: Decimal,
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    policy: str = "live_quality",
    empirical_bucket_matrix: list[dict[str, Any]] | None = None,
    last_minute_passive_price_matrix: list[dict[str, Any]] | None = None,
    enforce_empirical_bucket_gate: bool = False,
    empirical_bucket_requested_assets: list[str] | None = None,
    force_empirical_bucket_for_requested_assets: bool = False,
) -> dict[str, Any]:
    label_yes = int(row["label_yes"])
    candidates = _crypto_trade_candidates(
        row,
        predicted_yes,
        settings=settings,
        crypto_policy=crypto_policy,
        empirical_bucket_matrix=empirical_bucket_matrix,
        last_minute_passive_price_matrix=last_minute_passive_price_matrix,
        enforce_empirical_bucket_gate=enforce_empirical_bucket_gate,
        empirical_bucket_requested_assets=empirical_bucket_requested_assets,
        force_empirical_bucket_for_requested_assets=force_empirical_bucket_for_requested_assets,
    )
    allowed_statuses = {CRYPTO_LIVE_QUALITY}
    if policy == CRYPTO_EXPLORATORY_SHADOW:
        allowed_statuses = {CRYPTO_LIVE_QUALITY, CRYPTO_EXPLORATORY_SHADOW}
    selected = _crypto_preferred_candidate_for_statuses(candidates, allowed_statuses) or (candidates[0] if candidates else {})
    if selected and selected.get("candidate_status") not in allowed_statuses and policy == CRYPTO_EXPLORATORY_SHADOW:
        fallback = _crypto_shadow_ranked_fallback(candidates[:1], settings=settings)
        if fallback is not None:
            selected = fallback
    if not selected or selected.get("candidate_status") not in allowed_statuses:
        best = selected or {}
        return {
            "status": "not_selected",
            "side": best.get("side"),
            "reason": best.get("reason") or "no_candidate",
            "candidate_status": best.get("candidate_status"),
            "expected_net_edge": best.get("expected_net_edge"),
            "bucket_key": best.get("bucket_key"),
            "empirical_bucket_gate": best.get("empirical_bucket_gate"),
            "candidates": candidates,
        }
    side = str(selected["side"])
    cost = _decimal(selected["execution_price_dollars"])
    fee = _decimal(selected["expected_fee"])
    payoff = Decimal(label_yes) if side == "yes" else Decimal(1 - label_yes)
    gross = payoff - cost
    net = gross - fee
    return {
        "status": "fillable",
        "side": side,
        "candidate_status": selected["candidate_status"],
        "live_eligible": selected["candidate_status"] == CRYPTO_LIVE_QUALITY,
        "reason": selected["reason"],
        "execution_price_dollars": str(cost.quantize(Decimal("0.0001"))),
        "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
        "fees": str(fee.quantize(Decimal("0.0001"))),
        "net_pnl": str(net.quantize(Decimal("0.0001"))),
        "expected_net_edge": selected["expected_net_edge"],
        "bucket_key": selected["bucket_key"],
        "empirical_bucket_gate": selected.get("empirical_bucket_gate"),
        "candidates": candidates,
    }


def _crypto_touch_replay_first_touch(
    future_rows: list[dict[str, Any]],
    *,
    side: str,
    target_exit_side_price: Decimal,
) -> tuple[dict[str, Any], Decimal] | None:
    for future in future_rows:
        bid = _crypto_side_bid(future, side)
        if bid is None:
            continue
        if bid >= target_exit_side_price:
            return future, bid
    return None


def _crypto_passive_side_ask(row: dict[str, Any], side: str) -> Decimal | None:
    """Side ask with a derived fallback: ask = 1 - opposite-side bid."""
    ask = _crypto_side_ask(row, side)
    if ask is not None:
        return ask
    opposite = "no" if str(side).lower() == "yes" else "yes"
    opposite_bid = _crypto_side_bid(row, opposite)
    if opposite_bid is None:
        return None
    return _clamp_price(Decimal("1.0000") - opposite_bid)


def _crypto_passive_entry_limit_price(row: dict[str, Any], side: str) -> Decimal | None:
    """Passive entry limit: the side's bid, else side mid minus one tick."""
    bid = _crypto_side_bid(row, side)
    if bid is not None:
        return bid
    side_mid = _crypto_market_side_probability(row, side)
    if side_mid is None:
        return None
    return _clamp_price(side_mid - CRYPTO_PASSIVE_PRICE_TICK)


def _crypto_passive_replay_first_fill(
    future_rows: list[dict[str, Any]],
    *,
    side: str,
    limit_price: Decimal,
    decision_ts: datetime | None = None,
    close_ts: datetime | None = None,
) -> tuple[dict[str, Any], Decimal] | None:
    """First later row where the market trades through the passive limit.

    A resting buy at ``limit_price`` (in side terms) fills once a later
    snapshot shows the side's ask at or below the limit. Rows outside the
    ``decision_ts < t <= close_ts`` window are ignored.
    """
    for future in future_rows:
        future_ts = future.get("decision_ts")
        if decision_ts is not None or close_ts is not None:
            if not isinstance(future_ts, datetime):
                continue
            if decision_ts is not None and future_ts <= decision_ts:
                continue
            if close_ts is not None and future_ts > close_ts:
                continue
        ask = _crypto_passive_side_ask(future, side)
        if ask is None:
            continue
        if ask <= limit_price:
            return future, ask
    return None


def _simulate_crypto_passive_trade(
    row: dict[str, Any],
    future_rows: list[dict[str, Any]],
    selected: dict[str, Any],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Passive-bid mirror of the taker settlement simulation.

    Passive fills are modeled with maker fees where configured. Adverse
    selection remains visible through the filled-vs-unfilled settlement split.
    """
    side = str(selected.get("side") or "yes")
    limit_price = _crypto_passive_entry_limit_price(row, side)
    if limit_price is None:
        return {
            "status": "no_passive_price",
            "side": side,
            "filled": False,
            "limit_price_dollars": None,
            "fill_price_dollars": None,
            "fill_decision_ts": None,
            "gross_pnl": None,
            "fees": None,
            "net_pnl": None,
        }
    decision_ts = row.get("decision_ts") if isinstance(row.get("decision_ts"), datetime) else None
    close_ts = row.get("settlement_ts") if isinstance(row.get("settlement_ts"), datetime) else None
    filled = _crypto_passive_replay_first_fill(
        future_rows,
        side=side,
        limit_price=limit_price,
        decision_ts=decision_ts,
        close_ts=close_ts,
    )
    result: dict[str, Any] = {
        "status": "simulated",
        "side": side,
        "filled": filled is not None,
        "limit_price_dollars": str(limit_price.quantize(Decimal("0.0001"))),
        "fill_price_dollars": None,
        "fill_decision_ts": None,
        "gross_pnl": None,
        "fees": None,
        "net_pnl": None,
    }
    if filled is None:
        return result
    fill_row, fill_ask = filled
    label_yes = int(row["label_yes"])
    payoff = Decimal(label_yes) if side == "yes" else Decimal(1 - label_yes)
    gross = payoff - limit_price
    maker_fee_rate = Decimal(str(settings.kalshi_maker_fee_rate)) if settings is not None else Decimal("0.0175")
    maker_fee_applies = bool(settings.kalshi_maker_fee_enabled) if settings is not None else True
    fee = estimate_kalshi_maker_fee_dollars(
        price_dollars=limit_price,
        count=Decimal("1.00"),
        fee_rate=maker_fee_rate,
        maker_fee_applies=maker_fee_applies,
    )
    net = gross - fee
    result.update(
        {
            "fill_price_dollars": str(fill_ask.quantize(Decimal("0.0001"))),
            "fill_decision_ts": fill_row.get("decision_ts"),
            "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
            "fees": str(fee.quantize(Decimal("0.0001"))),
            "fee_source": "estimated_maker" if maker_fee_applies else "maker_fee_disabled",
            "fee_model_version": current_fee_model_version(),
            "net_pnl": str(net.quantize(Decimal("0.0001"))),
        }
    )
    return result


def _crypto_passive_replay_metrics(
    selection_trades: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Adverse-selection-aware passive simulation across replay candidates.

    For every taker-filled replay candidate, simulates resting a passive bid
    at the candidate's bid (or mid minus one tick) and filling only when a
    later snapshot of the same market trades through the limit before close.
    Settlement P&L at the taker price is kept per candidate so filled vs
    unfilled cohorts expose adverse selection.
    """
    rows_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_market[str(row.get("market_ticker") or "")].append(row)
    for market_rows in rows_by_market.values():
        market_rows.sort(key=lambda row: row.get("decision_ts") or datetime.max.replace(tzinfo=UTC))

    eligible_count = 0
    filled_count = 0
    passive_net = Decimal("0")
    passive_gross = Decimal("0")
    passive_fees = Decimal("0")
    taker_net = Decimal("0")
    filled_taker_pnls: list[Decimal] = []
    unfilled_taker_pnls: list[Decimal] = []
    filled_passive_pnls: list[Decimal] = []
    for trade in selection_trades:
        simulation = trade.get("simulation") if isinstance(trade.get("simulation"), dict) else {}
        taker_pnl = _decimal(simulation.get("net_pnl") or Decimal("0"))
        taker_net += taker_pnl
        market_rows = rows_by_market.get(str(trade.get("market_ticker") or ""), [])
        decision_ts = trade.get("decision_ts")
        future_rows = [
            future
            for future in market_rows
            if isinstance(future.get("decision_ts"), datetime)
            and isinstance(decision_ts, datetime)
            and future["decision_ts"] > decision_ts
        ]
        passive = _simulate_crypto_passive_trade(trade, future_rows, simulation, settings=settings)
        if passive["status"] != "simulated":
            continue
        eligible_count += 1
        if passive["filled"]:
            filled_count += 1
            passive_pnl = _decimal(passive["net_pnl"])
            passive_gross += _decimal(passive.get("gross_pnl") or Decimal("0"))
            passive_fees += _decimal(passive.get("fees") or Decimal("0"))
            passive_net += passive_pnl
            filled_passive_pnls.append(passive_pnl)
            filled_taker_pnls.append(taker_pnl)
        else:
            unfilled_taker_pnls.append(taker_pnl)

    def _mean(values: list[Decimal]) -> float | None:
        return float(sum(values, Decimal("0")) / Decimal(len(values))) if values else None

    return {
        "passive_eligible_candidate_count": eligible_count,
        "passive_filled_candidate_count": filled_count,
        "passive_fill_rate": _ratio(filled_count / eligible_count) if eligible_count else None,
        "passive_gross_simulated_pl_dollars": float(passive_gross),
        "passive_fees_dollars": float(passive_fees),
        "passive_fee_model_version": current_fee_model_version(),
        "passive_net_simulated_pl_dollars": float(passive_net),
        "passive_avg_pnl_filled": _mean(filled_passive_pnls),
        "taker_vs_passive_pl_delta_dollars": float(taker_net - passive_net),
        "passive_filled_avg_settlement_pnl": _mean(filled_taker_pnls),
        "passive_unfilled_avg_settlement_pnl": _mean(unfilled_taker_pnls),
    }


def _simulate_crypto_touch_trade(
    row: dict[str, Any],
    future_rows: list[dict[str, Any]],
    selected: dict[str, Any],
    *,
    settings: Settings,
) -> dict[str, Any]:
    side = str(selected["side"])
    cost = _decimal(selected["execution_price_dollars"])
    label_yes = int(row["label_yes"])
    fee_rate = Decimal(str(settings.kalshi_taker_fee_rate))
    entry_fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=cost,
        count=Decimal("1.00"),
        fee_rate=fee_rate,
    )
    touch = selected.get("touch_strategy") if isinstance(selected.get("touch_strategy"), dict) else {}
    target_exit = _decimal(touch.get("target_exit_side_price_dollars"))
    touched = _crypto_touch_replay_first_touch(
        future_rows,
        side=side,
        target_exit_side_price=target_exit,
    )
    if touched is not None:
        touch_row, exit_value = touched
        exit_fee = estimate_kalshi_taker_fee_dollars(
            price_dollars=exit_value,
            count=Decimal("1.00"),
            fee_rate=fee_rate,
        )
        gross = exit_value - cost
        net = gross - entry_fee - exit_fee
        return {
            "status": "fillable",
            "exit_mode": "take_profit_touch",
            "touched": True,
            "side": side,
            "candidate_status": selected.get("candidate_status"),
            "reason": selected.get("reason"),
            "execution_price_dollars": str(cost.quantize(Decimal("0.0001"))),
            "target_exit_side_price_dollars": str(target_exit.quantize(Decimal("0.0001"))),
            "exit_side_price_dollars": str(exit_value.quantize(Decimal("0.0001"))),
            "exit_decision_ts": touch_row.get("decision_ts"),
            "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
            "fees": str((entry_fee + exit_fee).quantize(Decimal("0.0001"))),
            "net_pnl": str(net.quantize(Decimal("0.0001"))),
            "expected_net_edge": selected.get("expected_net_edge"),
            "bucket_key": selected.get("bucket_key"),
            "touch_strategy": touch,
        }
    payoff = Decimal(label_yes) if side == "yes" else Decimal(1 - label_yes)
    gross = payoff - cost
    net = gross - entry_fee
    return {
        "status": "fillable",
        "exit_mode": "settlement_hold",
        "touched": False,
        "side": side,
        "candidate_status": selected.get("candidate_status"),
        "reason": selected.get("reason"),
        "execution_price_dollars": str(cost.quantize(Decimal("0.0001"))),
        "target_exit_side_price_dollars": str(target_exit.quantize(Decimal("0.0001"))),
        "exit_side_price_dollars": None,
        "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
        "fees": str(entry_fee.quantize(Decimal("0.0001"))),
        "net_pnl": str(net.quantize(Decimal("0.0001"))),
        "expected_net_edge": selected.get("expected_net_edge"),
        "bucket_key": selected.get("bucket_key"),
        "touch_strategy": touch,
    }


def _crypto_touch_bucket_matrix(trade_rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    del settings
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        key = _crypto_bucket_key(row, row.get("simulation") or {})
        grouped[key].append(row)
    matrix: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in rows]
        gross = [_decimal((row.get("simulation") or {}).get("gross_pnl")) for row in rows]
        touch_count = sum(1 for row in rows if (row.get("simulation") or {}).get("touched") is True)
        net_positive = sum(1 for value in values if value > 0)
        first = rows[0]
        net = sum(values, Decimal("0"))
        touch_rate = _ratio(touch_count / len(values)) if values else None
        matrix.append(
            {
                "bucket_key": key,
                "asset_symbol": first.get("asset_symbol"),
                "side": (first.get("simulation") or {}).get("side"),
                "entry_price_band": _price_band(_decimal((first.get("simulation") or {}).get("execution_price_dollars") or first.get("mid_yes_dollars"))),
                "spread_band": _spread_band(first.get("spread_bps")),
                "time_to_close_bucket": _crypto_time_to_close_bucket(float(first.get("time_to_close_seconds") or 0)),
                "sample_count": len(values),
                "touch_count": touch_count,
                "touch_rate": touch_rate,
                "win_rate": touch_rate,
                "outcome_win_rate": touch_rate,
                "net_positive_rate": _ratio(net_positive / len(values)) if values else None,
                "win_rate_basis": "touch_20pct_before_close",
                "gross_pnl": str(sum(gross, Decimal("0")).quantize(Decimal("0.0001"))),
                "fees": str(sum(fees, Decimal("0")).quantize(Decimal("0.0001"))),
                "net_pnl": str(net.quantize(Decimal("0.0001"))),
            }
        )
    matrix.sort(key=lambda item: (_decimal(item["net_pnl"]), item["bucket_key"]), reverse=True)
    return matrix


def _evaluate_crypto_touch20_replay(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any]:
    asset = "BTC"
    scoped_rows = [
        row
        for row in rows
        if _safe_normalize_asset_symbol(row.get("asset_symbol")) == asset
        and _crypto_frequency_for_row(row) == "1h"
        and row.get("label_yes") in {0, 1}
        and isinstance(row.get("decision_ts"), datetime)
        and isinstance(row.get("settlement_ts"), datetime)
    ]
    scoped_rows.sort(key=lambda row: (row.get("market_ticker") or "", row.get("decision_ts") or datetime.max.replace(tzinfo=UTC)))
    rows_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scoped_rows:
        rows_by_market[str(row.get("market_ticker") or "")].append(row)
    trades: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    for market_rows in rows_by_market.values():
        market_rows.sort(key=lambda row: row.get("decision_ts") or datetime.max.replace(tzinfo=UTC))
        for idx, row in enumerate(market_rows):
            candidates = _crypto_touch_strategy_candidates(
                row,
                settings=settings,
                crypto_policy=crypto_policy,
                btc_1h_touch_policy=True,
            )
            best = candidates[0] if candidates else {}
            status_counts[str(best.get("candidate_status") or "unknown")] += 1
            reason_counts[str(best.get("reason") or "unknown")] += 1
            selected = next((candidate for candidate in candidates if candidate.get("candidate_status") == CRYPTO_LIVE_QUALITY), None)
            if selected is None:
                continue
            decision_ts = row["decision_ts"]
            settlement_ts = row["settlement_ts"]
            future_rows = [
                future
                for future in market_rows[idx + 1 :]
                if isinstance(future.get("decision_ts"), datetime)
                and future["decision_ts"] > decision_ts
                and future["decision_ts"] < settlement_ts
            ]
            simulation = _simulate_crypto_touch_trade(row, future_rows, selected, settings=settings)
            trades.append({**row, "simulation": simulation, "predicted_yes_dollars": row.get("mid_yes_dollars")})
    values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in trades]
    fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in trades]
    touch_count = sum(1 for row in trades if (row.get("simulation") or {}).get("touched") is True)
    settlement_hold_count = len(trades) - touch_count
    bucket_matrix = _crypto_touch_bucket_matrix(trades, settings=settings)
    empirical_summary = _crypto_empirical_bucket_summary(
        bucket_matrix,
        settings=settings,
        crypto_policy=crypto_policy,
        requested_asset_symbols=[asset],
        force_requested_assets=True,
    )
    net = sum(values, Decimal("0"))
    metrics = {
        "objective": "touch_20pct_before_close",
        "asset_symbols": [asset],
        "sample_count": len(scoped_rows),
        "resolved_sample_count": len(scoped_rows),
        "strict_trade_eligible_count": sum(1 for row in scoped_rows if row.get("strict_trade_eligible")),
        "trade_candidate_count": len(trades),
        "current_model_live_quality_candidate_count": len(trades),
        "live_quality_candidate_count": len(trades),
        "touch_count": touch_count,
        "touch_rate": _ratio(touch_count / len(trades)) if trades else 0.0,
        "settlement_hold_count": settlement_hold_count,
        "net_simulated_pl_dollars": float(net),
        "fees_dollars": float(sum(fees, Decimal("0"))),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
        "pnl_per_candidate_dollars": float(net / Decimal(len(trades))) if trades else 0.0,
        "candidate_status_counts": dict(status_counts),
        "candidate_reason_counts": dict(reason_counts),
        "bucket_matrix": bucket_matrix,
        "allowed_bucket_keys": empirical_summary["allowed_bucket_keys"],
        "blocked_bucket_keys": empirical_summary["blocked_bucket_keys"],
        "empirical_bucket_gate": empirical_summary,
        "fee_model_version": current_fee_model_version(),
    }
    return {
        "status": "ok" if trades else "warn",
        "objective": "touch_20pct_before_close",
        "asset_symbols": [asset],
        "metrics": metrics,
        "bucket_matrix": bucket_matrix,
        "trades": trades[:100],
    }


def _crypto_touch_replay_gate_reasons(metrics: dict[str, Any], *, settings: Settings) -> list[str]:
    reasons: list[str] = []
    if not metrics:
        return ["BTC 1h touch replay artifact is missing."]
    if metrics.get("backtest_missing"):
        reasons.append("BTC 1h touch replay artifact is missing.")
    real_quote_path_rows = int(metrics.get("real_quote_path_row_count", metrics.get("strict_trade_eligible_count")) or 0)
    if not metrics.get("backtest_missing") and (
        metrics.get("touch_replay_data_missing_reason") == "missing_real_quote_path_evidence" or real_quote_path_rows <= 0
    ):
        reasons.append("BTC 1h touch replay has no settled real quote-path evidence.")
    candidates = int(metrics.get("current_model_live_quality_candidate_count", metrics.get("trade_candidate_count")) or 0)
    min_candidates = max(1, int(settings.crypto_1h_touch_replay_min_candidates))
    net_pl = float(metrics.get("net_simulated_pl_dollars") or 0.0)
    min_net = float(settings.crypto_1h_touch_replay_min_net_pnl_dollars)
    hard_cap_breaches = int(metrics.get("hard_cap_breaches") or 0)
    max_hard_cap = int(settings.crypto_1h_touch_replay_max_hard_cap_breaches)
    touch_rate = float(metrics.get("touch_rate") or 0.0)
    min_touch_rate = float(settings.crypto_1h_touch_replay_min_touch_rate)
    if candidates < min_candidates:
        reasons.append(f"BTC 1h touch replay candidate count {candidates} below minimum {min_candidates}.")
    if net_pl <= min_net:
        reasons.append(f"BTC 1h touch replay net P/L ${net_pl:.2f} does not clear required positive threshold.")
    if candidates >= min_candidates:
        pnl_per_candidate = net_pl / candidates if candidates > 0 else 0.0
        if pnl_per_candidate < float(settings.crypto_1h_touch_replay_min_pnl_per_candidate_dollars):
            reasons.append(
                f"BTC 1h touch replay P/L per candidate ${pnl_per_candidate:.4f} below minimum "
                f"${float(settings.crypto_1h_touch_replay_min_pnl_per_candidate_dollars):.4f}."
            )
    if hard_cap_breaches > max_hard_cap:
        reasons.append(f"BTC 1h touch replay hard-cap breaches {hard_cap_breaches} exceed limit {max_hard_cap}.")
    if touch_rate < min_touch_rate:
        reasons.append(f"BTC 1h touch replay touch rate {touch_rate:.1%} below minimum {min_touch_rate:.1%}.")
    if not (metrics.get("allowed_bucket_keys") or []):
        reasons.append("BTC 1h touch replay has no allowed bucket support.")
    return reasons


def _crypto_policy_metrics(policy_name: str, trade_rows: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any]:
    values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in trade_rows]
    gross = [_decimal((row.get("simulation") or {}).get("gross_pnl")) for row in trade_rows]
    fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in trade_rows]
    wins = sum(1 for value in values if value > 0)
    return {
        "policy_name": policy_name,
        "policy_family": {
            "baseline_market_mid": "prediction_only",
            "market_mid_baseline": "prediction_only",
            "always_0_5": "trivial_baseline",
            "last_direction": "trivial_baseline",
            "naive_momentum": "trivial_baseline",
            "linear_on_returns": "trivial_baseline",
            "current_heuristic": "prediction_only",
            "calibrated_prediction": "prediction_only",
            "candidate_quality_policy": "strict_candidate_quality",
            "shadow_exploration_policy": "shadow_exploration",
            "live_review_candidate": "live_review_candidate",
        }.get(policy_name, policy_name),
        "selected_count": len(trade_rows),
        "selected_market_count": _crypto_distinct_market_count(trade_rows),
        "fillable_count": len(trade_rows),
        "coverage": None,
        "gross_pnl": str(sum(gross, Decimal("0")).quantize(Decimal("0.0001"))),
        "fees": str(sum(fees, Decimal("0")).quantize(Decimal("0.0001"))),
        "net_pnl": str(sum(values, Decimal("0")).quantize(Decimal("0.0001"))),
        "max_drawdown": str(_crypto_max_drawdown(values).quantize(Decimal("0.0001"))),
        "sortino": _ratio(_crypto_sortino(values)),
        "sharpe": _ratio(_crypto_sharpe(values)),
        "win_rate": _ratio(wins / len(values)) if values else None,
        "win_rate_display_only": True,
        "cluster_count": len({(row.get("asset_symbol"), row.get("market_day")) for row in trade_rows}),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
        "worst_buckets": _crypto_bucket_matrix(trade_rows, settings=settings)[:10],
    }


_CRYPTO_REPLAY_GATE_METRIC_KEYS = (
    "trade_candidate_count",
    "trade_candidate_market_count",
    "current_model_live_quality_candidate_count",
    "current_model_live_quality_market_count",
    "live_quality_candidate_count",
    "oos_trade_candidate_count",
    "oos_trade_candidate_market_count",
    "oos_net_simulated_pl_dollars",
    "oos_market_mid_net_simulated_pl_dollars",
    "oos_pnl_advantage_vs_market_mid_dollars",
    "net_simulated_pl_dollars",
    "market_mid_net_simulated_pl_dollars",
    "pnl_advantage_vs_market_mid_dollars",
    "fees_dollars",
    "hard_cap_breaches",
)


def _crypto_replay_gate_metric_snapshot(metrics: dict[str, Any]) -> dict[str, Any]:
    return {key: metrics.get(key) for key in _CRYPTO_REPLAY_GATE_METRIC_KEYS if key in metrics}


def _crypto_apply_empirical_bucket_gate_to_replay_metrics(
    metrics: dict[str, Any],
    *,
    selection_trades: list[dict[str, Any]],
    market_mid_trades: list[dict[str, Any]],
    bucket_matrix: list[dict[str, Any]],
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    requested_asset_symbols: list[str] | None = None,
    force_requested_assets: bool = False,
) -> dict[str, Any]:
    metrics_with_buckets = _crypto_metrics_with_empirical_buckets(
        metrics,
        bucket_matrix=bucket_matrix,
        settings=settings,
        crypto_policy=crypto_policy,
        requested_asset_symbols=requested_asset_symbols,
        force_requested_assets=force_requested_assets,
    )
    summary = metrics_with_buckets["empirical_bucket_gate"]
    pre_bucket_metrics = _crypto_replay_gate_metric_snapshot(metrics_with_buckets)
    gate_applies = bool(summary.get("enabled")) and bool(summary.get("enforced_assets"))
    if not gate_applies:
        return {
            **metrics_with_buckets,
            "pre_bucket_gate_metrics": pre_bucket_metrics,
            "bucket_gated_metrics": None,
            "empirical_bucket_gate_applied_to_metrics": False,
            "metrics_source": metrics_with_buckets.get("metrics_scope") or metrics.get("metrics_scope"),
        }

    allowed = set(summary.get("allowed_bucket_keys") or [])
    gated_selection_trades = [
        row
        for row in selection_trades
        if str((row.get("simulation") or {}).get("bucket_key") or "") in allowed
    ]
    gated_market_mid_trades = [
        row
        for row in market_mid_trades
        if str((row.get("simulation") or {}).get("bucket_key") or "") in allowed
    ]
    gated_market_mid_policy = _crypto_policy_metrics(
        "market_mid_baseline_bucket_gated",
        gated_market_mid_trades,
        settings=settings,
    )
    gated_selection_policy = _crypto_candidate_policy_metrics(
        "candidate_quality_policy_bucket_gated",
        gated_selection_trades,
        settings=settings,
        market_mid_net_pnl=_candidate_policy_net(gated_market_mid_policy),
    )
    selected_count = _candidate_policy_selected_count(gated_selection_policy)
    net_pnl = _candidate_policy_net(gated_selection_policy)
    market_mid_net = _candidate_policy_net(gated_market_mid_policy)
    advantage = net_pnl - market_mid_net
    bucket_metrics = {
        "trade_candidate_count": selected_count,
        "current_model_live_quality_candidate_count": selected_count,
        "live_quality_candidate_count": selected_count,
        "oos_trade_candidate_count": selected_count,
        "oos_net_simulated_pl_dollars": float(net_pnl),
        "oos_market_mid_net_simulated_pl_dollars": float(market_mid_net),
        "oos_pnl_advantage_vs_market_mid_dollars": float(advantage),
        "net_simulated_pl_dollars": float(net_pnl),
        "market_mid_net_simulated_pl_dollars": float(market_mid_net),
        "pnl_advantage_vs_market_mid_dollars": float(advantage),
        "fees_dollars": float(_decimal(gated_selection_policy.get("fees") or Decimal("0"))),
        "hard_cap_breaches": int(gated_selection_policy.get("hard_cap_breaches") or 0),
    }
    # Only override market-grouped counts when the source metrics carry them, so
    # old-style metrics keep row-count gate behavior untouched.
    if any(
        key in metrics_with_buckets
        for key in ("trade_candidate_market_count", "current_model_live_quality_market_count", "oos_trade_candidate_market_count")
    ):
        selected_market_count = _crypto_distinct_market_count(gated_selection_trades)
        bucket_metrics.update(
            {
                "trade_candidate_market_count": selected_market_count,
                "current_model_live_quality_market_count": selected_market_count,
                "oos_trade_candidate_market_count": selected_market_count,
            }
        )
    return {
        **metrics_with_buckets,
        **bucket_metrics,
        "pre_bucket_gate_metrics": pre_bucket_metrics,
        "bucket_gated_metrics": {
            **bucket_metrics,
            "selection_policy": gated_selection_policy,
            "market_mid_policy": gated_market_mid_policy,
            "allowed_bucket_keys": sorted(allowed),
        },
        "empirical_bucket_gate_applied_to_metrics": True,
        "metrics_source": "empirical_bucket_gated",
    }


def _crypto_select_model_policy_by_profit(policies: list[dict[str, Any]]) -> dict[str, Any] | None:
    non_market = [policy for policy in policies if policy.get("policy_name") not in CRYPTO_MODEL_BASELINE_CANDIDATES]
    profitable = [
        policy
        for policy in non_market
        if _candidate_policy_selected_count(policy) > 0
        and _candidate_policy_net(policy) > Decimal("0")
        and _candidate_policy_advantage(policy) > Decimal("0")
    ]
    if profitable:
        profitable.sort(
            key=lambda policy: (
                _candidate_policy_net(policy),
                _candidate_policy_advantage(policy),
                _candidate_policy_selected_count(policy),
                str(policy.get("policy_name")),
            ),
            reverse=True,
        )
        return {**profitable[0], "selection_status": "deployable_candidate"}
    if non_market:
        non_market.sort(
            key=lambda policy: (
                _candidate_policy_advantage(policy),
                _candidate_policy_net(policy),
                _candidate_policy_selected_count(policy),
                str(policy.get("policy_name")),
            ),
            reverse=True,
        )
        return {**non_market[0], "selection_status": "diagnostic_only"}
    return None


def _cap_crypto_exploratory_rows(rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            (row.get("simulation") or {}).get("candidate_status") == CRYPTO_LIVE_QUALITY,
            _decimal((row.get("simulation") or {}).get("expected_net_edge") or Decimal("-999")),
        ),
        reverse=True,
    )
    max_total = max(0, int(settings.crypto_shadow_exploration_max_candidates_per_run))
    max_per_asset = max(1, int(settings.crypto_shadow_exploration_max_per_asset_per_run))
    counts: Counter[str] = Counter()
    capped: list[dict[str, Any]] = []
    for row in ordered:
        asset = str(row.get("asset_symbol") or "unknown")
        if counts[asset] >= max_per_asset:
            continue
        capped.append(row)
        counts[asset] += 1
        if len(capped) >= max_total:
            break
    return capped


def _crypto_candidate_quality_report(
    rows: list[dict[str, Any]],
    model: dict[str, Any] | None,
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
) -> dict[str, Any]:
    diagnostic_rows = [row for row in rows if _crypto_candidate_diagnostic_row(row)]
    diagnostic_scope = "strict_point_in_time_rows" if diagnostic_rows else "all_rows_no_strict_point_in_time"
    if not diagnostic_rows:
        diagnostic_rows = rows
    scored: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    top_status_counts: Counter[str] = Counter()
    top_reason_counts: Counter[str] = Counter()
    rejection_reason_counts: Counter[str] = Counter()
    low_price_diagnostic_counts: Counter[str] = Counter()
    side_counts: Counter[str] = Counter()
    by_asset: dict[str, dict[str, Any]] = {}
    for row in diagnostic_rows:
        predicted = _predict_crypto_probability(row, model)
        simulation = _simulate_crypto_trade(
            row,
            predicted,
            settings=settings,
            crypto_policy=crypto_policy,
            policy=CRYPTO_EXPLORATORY_SHADOW,
        )
        candidates = simulation.get("candidates") or []
        asset = normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
        asset_summary = by_asset.setdefault(
            asset,
            {
                "row_count": 0,
                "strict_trade_eligible_count": 0,
                "candidate_status_counts": Counter(),
                "candidate_reason_counts": Counter(),
                "top_candidate_status_counts": Counter(),
                "top_candidate_reason_counts": Counter(),
                "candidate_rejection_reason_counts": Counter(),
                "low_price_shadow_diagnostic_count": 0,
                "live_quality_candidate_count": 0,
                "exploratory_shadow_candidate_count": 0,
            },
        )
        asset_summary["row_count"] += 1
        if row.get("strict_trade_eligible"):
            asset_summary["strict_trade_eligible_count"] += 1
        for candidate in candidates:
            candidate_status = str(candidate.get("candidate_status") or "unknown")
            candidate_reason = str(candidate.get("reason") or "unknown")
            status_counts[candidate_status] += 1
            reason_counts[candidate_reason] += 1
            asset_summary["candidate_status_counts"][candidate_status] += 1
            asset_summary["candidate_reason_counts"][candidate_reason] += 1
            if candidate.get("low_price_shadow_diagnostic"):
                low_price_diagnostic_counts[str(candidate.get("side") or "unknown")] += 1
                asset_summary["low_price_shadow_diagnostic_count"] += 1
        if candidates:
            top_candidate = candidates[0]
            top_status = str(top_candidate.get("candidate_status") or "unknown")
            top_reason = str(top_candidate.get("reason") or "unknown")
            top_status_counts[top_status] += 1
            top_reason_counts[top_reason] += 1
            asset_summary["top_candidate_status_counts"][top_status] += 1
            asset_summary["top_candidate_reason_counts"][top_reason] += 1
            rejection_reason = _crypto_live_rejection_reason(top_candidate)
            if rejection_reason:
                rejection_reason_counts[rejection_reason] += 1
                asset_summary["candidate_rejection_reason_counts"][rejection_reason] += 1
        if simulation["status"] != "fillable":
            continue
        side_counts[str(simulation.get("side") or "unknown")] += 1
        if simulation.get("candidate_status") == CRYPTO_LIVE_QUALITY:
            asset_summary["live_quality_candidate_count"] += 1
        if simulation.get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW:
            asset_summary["exploratory_shadow_candidate_count"] += 1
        scored.append({**row, "simulation": simulation, "predicted_yes_dollars": predicted})
    live_quality = [row for row in scored if (row.get("simulation") or {}).get("candidate_status") == CRYPTO_LIVE_QUALITY]
    exploratory_scored = [row for row in scored if (row.get("simulation") or {}).get("candidate_status") == CRYPTO_EXPLORATORY_SHADOW]
    capped = _cap_crypto_exploratory_rows(scored, settings=settings)
    top_candidates = sorted(
        capped,
        key=lambda row: _decimal((row.get("simulation") or {}).get("expected_net_edge") or Decimal("-999")),
        reverse=True,
    )
    return {
        "dataset": {
            "row_count": len(rows),
            "candidate_diagnostic_row_count": len(diagnostic_rows),
            "candidate_diagnostic_scope": diagnostic_scope,
            "asset_count": len({row.get("asset_symbol") for row in rows}),
            "assets": sorted({str(row.get("asset_symbol")) for row in rows}),
        },
        "candidate_status_counts": dict(status_counts),
        "candidate_reason_counts": dict(reason_counts),
        "top_candidate_status_counts": dict(top_status_counts),
        "top_candidate_reason_counts": dict(top_reason_counts),
        "candidate_rejection_reason_counts": dict(rejection_reason_counts),
        "low_price_shadow_diagnostic_counts": dict(low_price_diagnostic_counts),
        "by_asset": {
            asset: {
                **{
                    key: value
                    for key, value in summary.items()
                    if key
                    not in {
                        "candidate_status_counts",
                        "candidate_reason_counts",
                        "top_candidate_status_counts",
                        "top_candidate_reason_counts",
                        "candidate_rejection_reason_counts",
                    }
                },
                "candidate_status_counts": dict(summary["candidate_status_counts"]),
                "candidate_reason_counts": dict(summary["candidate_reason_counts"]),
                "top_candidate_status_counts": dict(summary["top_candidate_status_counts"]),
                "top_candidate_reason_counts": dict(summary["top_candidate_reason_counts"]),
                "candidate_rejection_reason_counts": dict(summary["candidate_rejection_reason_counts"]),
            }
            for asset, summary in sorted(by_asset.items())
        },
        "selected_side_counts": dict(side_counts),
        "candidate_caps": {
            "max_candidates_per_run": settings.crypto_shadow_exploration_max_candidates_per_run,
            "max_per_asset_per_run": settings.crypto_shadow_exploration_max_per_asset_per_run,
            "min_expected_net_edge_dollars": settings.crypto_shadow_exploration_min_expected_net_edge_dollars,
            "max_spread_bps": settings.crypto_shadow_exploration_max_spread_bps,
        },
        "live_quality_policy": _crypto_policy_metrics("live_quality", live_quality, settings=settings),
        "shadow_exploration_policy": _crypto_policy_metrics("shadow_exploration_capped", capped, settings=settings),
        "exploratory_shadow_count": len(exploratory_scored),
        "top_candidates": [
            {
                "row_id": row.get("row_id"),
                "asset_symbol": row.get("asset_symbol"),
                "market_ticker": row.get("market_ticker"),
                "side": (row.get("simulation") or {}).get("side"),
                "candidate_status": (row.get("simulation") or {}).get("candidate_status"),
                "expected_net_edge": (row.get("simulation") or {}).get("expected_net_edge"),
                "net_pnl": (row.get("simulation") or {}).get("net_pnl"),
                "bucket_key": (row.get("simulation") or {}).get("bucket_key"),
                "spread_bps": row.get("spread_bps"),
            }
            for row in top_candidates[:50]
        ],
        "bucket_matrix": _crypto_bucket_matrix(scored, settings=settings),
        "bucket_diagnostics": _crypto_bucket_diagnostics(scored),
    }


def _crypto_candidate_diagnostic_row(row: dict[str, Any]) -> bool:
    if row.get("strict_trade_eligible") is not True:
        return False
    if str(row.get("quote_source") or "") != "snapshot_quotes":
        return False
    if str(row.get("leakage_status") or "") != "point_in_time":
        return False
    if row.get("prediction_eligible") is False:
        return False
    return True


def _crypto_live_rejection_reason(candidate: dict[str, Any]) -> str | None:
    status = str(candidate.get("candidate_status") or "")
    reason = str(candidate.get("reason") or "unknown")
    if status == CRYPTO_LIVE_QUALITY:
        return None
    if status == CRYPTO_EXPLORATORY_SHADOW:
        if reason in {
            "crypto_market_too_early_for_live_entry",
            "crypto_market_too_late_for_live_entry",
            "crypto_entry_window_unknown",
        }:
            return reason
        return "fee_adjusted_edge_below_live_min"
    return reason


def _eligible_crypto_buckets(rows: list[dict[str, Any]], *, settings: Settings) -> set[str]:
    simulations = [{**row, "simulation": _simulate_crypto_trade(row, _decimal(row["mid_yes_dollars"]), settings=settings)} for row in rows]
    matrix = _crypto_bucket_matrix([row for row in simulations if row["simulation"]["status"] == "fillable"], settings=settings)
    return set(_crypto_empirical_bucket_summary(matrix, settings=settings)["allowed_bucket_keys"])


def _crypto_bucket_matrix(trade_rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    del settings
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        key = _crypto_bucket_key(row, row.get("simulation") or {})
        grouped[key].append(row)
    matrix: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in rows]
        gross = [_decimal((row.get("simulation") or {}).get("gross_pnl")) for row in rows]
        net_positive = sum(1 for value in values if value > 0)
        outcome_wins = sum(1 for row in rows if _crypto_trade_outcome_won(row))
        first = rows[0]
        net = sum(values, Decimal("0"))
        outcome_win_rate = _ratio(outcome_wins / len(values)) if values else None
        matrix.append(
            {
                "bucket_key": key,
                "asset_symbol": first.get("asset_symbol"),
                "side": (first.get("simulation") or {}).get("side"),
                "entry_price_band": _price_band(_decimal((first.get("simulation") or {}).get("execution_price_dollars") or first.get("mid_yes_dollars"))),
                "spread_band": _spread_band(first.get("spread_bps")),
                "time_to_close_bucket": _crypto_time_to_close_bucket(float(first.get("time_to_close_seconds") or 0)),
                "sample_count": len(values),
                "win_rate": outcome_win_rate,
                "outcome_win_rate": outcome_win_rate,
                "net_positive_rate": _ratio(net_positive / len(values)) if values else None,
                "win_rate_basis": "settlement_outcome",
                "gross_pnl": str(sum(gross, Decimal("0")).quantize(Decimal("0.0001"))),
                "fees": str(sum(fees, Decimal("0")).quantize(Decimal("0.0001"))),
                "net_pnl": str(net.quantize(Decimal("0.0001"))),
            }
        )
    matrix.sort(key=lambda item: (_decimal(item["net_pnl"]), item["bucket_key"]))
    return matrix


def _crypto_last_minute_passive_price_matrix(rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    max_seconds = max(0, int(settings.crypto_last_minute_passive_max_seconds_to_close))
    ladder = _crypto_last_minute_passive_price_ladder(settings)
    rows_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("strict_trade_eligible") is not True:
            continue
        if str(row.get("quote_source") or "") != "snapshot_quotes":
            continue
        if row.get("label_yes") is None:
            continue
        decision_ts = row.get("decision_ts")
        settlement_ts = row.get("settlement_ts")
        if not isinstance(decision_ts, datetime) or not isinstance(settlement_ts, datetime):
            continue
        time_to_close = _optional_int(row.get("time_to_close_seconds"))
        if time_to_close is None or time_to_close <= 0 or time_to_close > max_seconds:
            continue
        rows_by_market[str(row.get("market_ticker") or "")].append(row)
    for market_rows in rows_by_market.values():
        market_rows.sort(key=lambda item: item.get("decision_ts") or datetime.max.replace(tzinfo=UTC))

    grouped: dict[str, dict[str, Any]] = {}
    for market_rows in rows_by_market.values():
        for idx, row in enumerate(market_rows):
            decision_ts = row.get("decision_ts")
            settlement_ts = row.get("settlement_ts")
            future_rows = [
                future
                for future in market_rows[idx + 1 :]
                if isinstance(future.get("decision_ts"), datetime)
                and future["decision_ts"] > decision_ts
                and future["decision_ts"] < settlement_ts
            ]
            if not future_rows:
                continue
            label_yes = int(row.get("label_yes") or 0)
            for side in ("yes", "no"):
                cost = _crypto_side_ask(row, side)
                market_side_probability = _crypto_market_side_probability(row, side)
                if cost is None or market_side_probability is None:
                    continue
                base_key = _crypto_last_minute_price_matrix_base_key(
                    row,
                    side=side,
                    market_side_probability=market_side_probability,
                )
                if base_key is None:
                    continue
                future_costs = [
                    value
                    for future in future_rows
                    if (value := _crypto_side_ask(future, side)) is not None
                ]
                if not future_costs:
                    continue
                for bid in ladder:
                    if bid >= cost:
                        continue
                    if market_side_probability <= bid:
                        continue
                    matrix_key = _crypto_last_minute_price_matrix_key(
                        row,
                        side=side,
                        bid=bid,
                        market_side_probability=market_side_probability,
                    )
                    if matrix_key is None:
                        continue
                    bucket = grouped.setdefault(
                        matrix_key,
                        {
                            "matrix_key": matrix_key,
                            "matrix_base_key": base_key,
                            "asset_symbol": normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
                            "side": side,
                            "time_to_close_bucket": _crypto_time_to_close_bucket(float(row.get("time_to_close_seconds") or 0)),
                            "market_probability_band": _price_band(_clamp_price(market_side_probability)),
                            "spread_band": _spread_band(row.get("spread_bps")),
                            "bid_price_dollars": _money_text(bid),
                            "sample_count": 0,
                            "fill_count": 0,
                            "win_count": 0,
                            "gross_pnl": Decimal("0"),
                            "fees": Decimal("0"),
                            "net_pnl": Decimal("0"),
                        },
                    )
                    bucket["sample_count"] += 1
                    filled = any(future_cost <= bid for future_cost in future_costs)
                    if not filled:
                        continue
                    bucket["fill_count"] += 1
                    side_won = (side == "yes" and label_yes == 1) or (side == "no" and label_yes == 0)
                    if side_won:
                        bucket["win_count"] += 1
                    gross = (Decimal("1") if side_won else Decimal("0")) - bid
                    fee = estimate_kalshi_taker_fee_dollars(
                        price_dollars=bid,
                        count=Decimal("1.00"),
                        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
                    )
                    bucket["gross_pnl"] += gross
                    bucket["fees"] += fee
                    bucket["net_pnl"] += gross - fee
    matrix: list[dict[str, Any]] = []
    for bucket in grouped.values():
        sample_count = int(bucket["sample_count"])
        fill_count = int(bucket["fill_count"])
        gross = _decimal(bucket["gross_pnl"])
        fees = _decimal(bucket["fees"])
        net = _decimal(bucket["net_pnl"])
        item = {
            **bucket,
            "sample_count": sample_count,
            "fill_count": fill_count,
            "win_count": int(bucket["win_count"]),
            "fill_rate": _ratio(fill_count / sample_count) if sample_count else None,
            "win_rate": _ratio(int(bucket["win_count"]) / fill_count) if fill_count else None,
            "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
            "fees": str(fees.quantize(Decimal("0.0001"))),
            "net_pnl": str(net.quantize(Decimal("0.0001"))),
            "net_pnl_per_signal": str((net / Decimal(sample_count)).quantize(Decimal("0.0001"))) if sample_count else None,
            "net_pnl_per_fill": str((net / Decimal(fill_count)).quantize(Decimal("0.0001"))) if fill_count else None,
            "fee_model_version": current_fee_model_version(),
        }
        matrix.append(item)
    matrix.sort(
        key=lambda item: (
            _decimal(item.get("net_pnl_per_signal") or Decimal("-999")),
            float(item.get("fill_rate") or 0.0),
            -float(_decimal(item.get("bid_price_dollars") or Decimal("0"))),
            str(item.get("matrix_key") or ""),
        ),
        reverse=True,
    )
    return matrix


def _crypto_bucket_diagnostics(trade_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "by_price_bucket": _crypto_dimension_bucket_diagnostics(
            trade_rows,
            lambda row: _price_band(
                _decimal((row.get("simulation") or {}).get("execution_price_dollars") or row.get("mid_yes_dollars"))
            ),
        ),
        "by_time_to_close_bucket": _crypto_dimension_bucket_diagnostics(
            trade_rows,
            lambda row: _crypto_time_to_close_bucket(float(row.get("time_to_close_seconds") or 0)),
        ),
        "by_spread_bucket": _crypto_dimension_bucket_diagnostics(
            trade_rows,
            lambda row: _spread_band(row.get("spread_bps")),
        ),
    }


def _crypto_trade_outcome_won(row: dict[str, Any]) -> bool:
    label = row.get("label_yes")
    if label is None:
        return False
    side = str((row.get("simulation") or {}).get("side") or "").lower()
    try:
        label_yes = int(label)
    except (TypeError, ValueError):
        return False
    if side == "yes":
        return label_yes == 1
    if side == "no":
        return label_yes == 0
    return False


def _crypto_dimension_bucket_diagnostics(
    trade_rows: list[dict[str, Any]],
    bucket_fn: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        grouped[bucket_fn(row)].append(row)
    diagnostics: list[dict[str, Any]] = []
    for bucket, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        edges = [
            _decimal((row.get("simulation") or {}).get("expected_net_edge"))
            for row in rows
            if (row.get("simulation") or {}).get("expected_net_edge") not in (None, "")
        ]
        wins = sum(1 for value in values if value > 0)
        prediction_pairs = [
            (_decimal(row.get("predicted_yes_dollars")), int(row.get("label_yes")))
            for row in rows
            if row.get("predicted_yes_dollars") not in (None, "") and row.get("label_yes") is not None
        ]
        calibration = _probability_metrics_decimal(prediction_pairs)
        diagnostics.append(
            {
                "bucket": bucket,
                "selected_count": len(rows),
                "win_rate": _ratio(wins / len(values)) if values else None,
                "net_pnl": str(sum(values, Decimal("0")).quantize(Decimal("0.0001"))),
                "mean_expected_net_edge": (
                    str((sum(edges, Decimal("0")) / Decimal(len(edges))).quantize(Decimal("0.0001")))
                    if edges
                    else None
                ),
                "calibration_ece": calibration.get("ece"),
            }
        )
    diagnostics.sort(key=lambda item: str(item["bucket"]))
    return diagnostics


def _crypto_bucket_key(row: dict[str, Any], simulation: dict[str, Any]) -> str:
    side = simulation.get("side") or "unknown"
    price = _decimal(simulation.get("execution_price_dollars") or row.get("mid_yes_dollars"))
    return "|".join(
        [
            str(row.get("asset_symbol") or "unknown"),
            str(side),
            _price_band(price),
            _spread_band(row.get("spread_bps")),
            _crypto_time_to_close_bucket(float(row.get("time_to_close_seconds") or 0)),
        ]
    )


def _crypto_empirical_bucket_gate_enabled_for_asset(
    row: dict[str, Any],
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    requested_asset_symbols: list[str] | None = None,
    force_requested_assets: bool = False,
) -> bool:
    if not bool(settings.crypto_empirical_bucket_gate_enabled):
        return False
    asset = normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN"))
    configured = _normalize_asset_csv(settings.crypto_empirical_bucket_gate_assets)
    requested_assets = set(normalize_asset_symbols(requested_asset_symbols))
    if force_requested_assets and requested_assets and asset in requested_assets:
        return True
    if not configured:
        return True
    if asset in configured:
        return True
    if "LIVE" not in configured:
        return False
    return (
        crypto_policy is not None
        and (crypto_policy.asset_modes or {}).get(asset) == CRYPTO_ASSET_MODE_LIVE
    )


def _crypto_bucket_matrix_by_key(bucket_matrix: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    keyed: dict[str, dict[str, Any]] = {}
    for bucket in bucket_matrix or []:
        if not isinstance(bucket, dict):
            continue
        key = str(bucket.get("bucket_key") or "").strip()
        if key:
            keyed[key] = bucket
    return keyed


def _normalize_reason_csv(value: str | None) -> set[str]:
    return {raw.strip().lower() for raw in str(value or "").replace(";", ",").split(",") if raw.strip()}


def _crypto_empirical_bucket_review(bucket: dict[str, Any] | None, *, settings: Settings) -> dict[str, Any]:
    min_samples = int(settings.crypto_empirical_bucket_min_samples)
    min_net_pnl = Decimal(str(settings.crypto_empirical_bucket_min_net_pnl_dollars))
    min_win_rate = float(settings.crypto_empirical_bucket_min_win_rate)
    if bucket is None:
        return {
            "status": "unknown",
            "allowed": False,
            "reason": "empirical_bucket_missing",
            "sample_count": 0,
            "min_samples": min_samples,
            "min_net_pnl_dollars": str(min_net_pnl.quantize(Decimal("0.0001"))),
            "min_win_rate": min_win_rate,
        }
    sample_count = int(bucket.get("sample_count") or 0)
    net_pnl = _decimal(bucket.get("net_pnl") or Decimal("0"))
    win_rate_raw = bucket.get("outcome_win_rate", bucket.get("win_rate"))
    win_rate = float(win_rate_raw) if win_rate_raw not in (None, "") else None
    net_positive_rate_raw = bucket.get("net_positive_rate")
    net_positive_rate = (
        float(net_positive_rate_raw)
        if net_positive_rate_raw not in (None, "")
        else None
    )
    if sample_count < min_samples:
        reason = "empirical_bucket_under_sampled"
    elif net_pnl < min_net_pnl:
        reason = "empirical_bucket_negative_pnl"
    elif win_rate is None or win_rate < min_win_rate:
        reason = "empirical_bucket_low_win_rate"
    else:
        reason = "empirical_bucket_allowed"
    allowed = reason == "empirical_bucket_allowed"
    return {
        "status": "allowed" if allowed else "blocked",
        "allowed": allowed,
        "reason": reason,
        "sample_count": sample_count,
        "net_pnl": str(net_pnl.quantize(Decimal("0.0001"))),
        "win_rate": win_rate,
        "outcome_win_rate": win_rate,
        "net_positive_rate": net_positive_rate,
        "win_rate_basis": str(bucket.get("win_rate_basis") or "settlement_outcome"),
        "min_samples": min_samples,
        "min_net_pnl_dollars": str(min_net_pnl.quantize(Decimal("0.0001"))),
        "min_win_rate": min_win_rate,
    }


def _crypto_empirical_late_override_review(
    row: dict[str, Any],
    empirical_bucket_gate: dict[str, Any],
    *,
    pre_empirical_reason: str,
    late_sure_thing: bool,
    settings: Settings,
) -> dict[str, Any]:
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    time_bucket = (
        _crypto_time_to_close_bucket(float(time_to_close))
        if time_to_close is not None
        else "unknown"
    )
    original_reason = str(empirical_bucket_gate.get("reason") or "").strip()
    allowed_reasons = _normalize_reason_csv(settings.crypto_empirical_late_override_reasons)
    max_seconds = max(0, int(settings.crypto_empirical_late_override_max_seconds_to_close))
    max_count = quantize_count(Decimal(str(settings.crypto_empirical_late_override_max_count_fp)))
    review: dict[str, Any] = {
        "enabled": bool(settings.crypto_empirical_late_override_enabled),
        "allowed": False,
        "reason": "late_empirical_override_not_allowed",
        "original_bucket_status": empirical_bucket_gate.get("status"),
        "original_bucket_reason": original_reason,
        "pre_empirical_reason": pre_empirical_reason,
        "late_high_confidence_directional_entry": late_sure_thing,
        "time_to_close_seconds": time_to_close,
        "time_to_close_bucket": time_bucket,
        "max_seconds_to_close": max_seconds,
        "allowed_reasons": sorted(allowed_reasons),
        "max_count_fp": _count_text(max_count),
    }
    if not review["enabled"]:
        return {**review, "reason": "late_empirical_override_disabled"}
    if not late_sure_thing or pre_empirical_reason != "late_high_confidence_directional_entry":
        return {**review, "reason": "not_late_high_confidence_entry"}
    if time_to_close is None:
        return {**review, "reason": "time_to_close_unknown"}
    if time_to_close > max_seconds:
        return {**review, "reason": "outside_late_override_window"}
    if time_bucket != "0_5m":
        return {**review, "reason": "outside_0_5m_bucket"}
    if max_count <= Decimal("0"):
        return {**review, "reason": "non_positive_late_override_count_cap"}
    if original_reason == "empirical_bucket_negative_pnl" and not bool(
        settings.crypto_empirical_late_override_negative_pnl_enabled
    ):
        return {**review, "reason": "negative_pnl_override_disabled"}
    if original_reason not in allowed_reasons:
        return {**review, "reason": "bucket_reason_not_late_override_allowed"}
    if original_reason == "empirical_bucket_low_win_rate":
        net_pnl = _decimal(empirical_bucket_gate.get("net_pnl") or Decimal("0"))
        if net_pnl < Decimal("0"):
            return {**review, "reason": "low_win_rate_bucket_negative_pnl"}
    return {
        **review,
        "allowed": True,
        "reason": f"late_high_confidence_{original_reason}_override",
    }


def _crypto_empirical_gate_with_late_override(
    empirical_bucket_gate: dict[str, Any],
    override_review: dict[str, Any],
) -> dict[str, Any]:
    if not override_review.get("allowed"):
        return empirical_bucket_gate
    return {
        **empirical_bucket_gate,
        "status": "override_allowed",
        "allowed": True,
        "override_allowed": True,
        "original_status": empirical_bucket_gate.get("status"),
        "original_reason": empirical_bucket_gate.get("reason"),
        "override_reason": override_review.get("reason"),
        "late_override": override_review,
    }


def _crypto_empirical_bucket_gap_sample(
    row: dict[str, Any],
    *,
    side: str,
    cost: Decimal,
    target_yes: Decimal,
    pre_empirical_status: str,
    pre_empirical_reason: str,
    candidate_status: str,
    reason: str,
    edge_bps: int,
    expected_net_edge: Decimal,
    model_probability: Decimal,
    raw_model_probability: Decimal,
    market_side_probability: Decimal | None,
    fee: Decimal,
    remaining_payout: Decimal,
    bucket_key: str,
    empirical_bucket_gate: dict[str, Any],
    empirical_bucket_late_override: dict[str, Any],
    late_sure_thing: bool,
) -> dict[str, Any]:
    gate_reason = str(
        empirical_bucket_gate.get("original_reason")
        or empirical_bucket_gate.get("reason")
        or "unknown"
    )
    time_to_close = _optional_int(row.get("time_to_close_seconds"))
    spread_bps = _optional_int(row.get("spread_bps"))
    market_ticker = str(row.get("market_ticker") or "unknown")
    return {
        "schema_version": "crypto-empirical-gap-sample-v1",
        "market_ticker": market_ticker,
        "asset_symbol": normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
        "side": side,
        "dedupe_key": "|".join([market_ticker, side, bucket_key, pre_empirical_reason]),
        "bucket_key": bucket_key,
        "bucket_reason": gate_reason,
        "bucket_status": empirical_bucket_gate.get("status"),
        "bucket_allowed": empirical_bucket_gate.get("allowed") is True,
        "bucket_enforced": empirical_bucket_gate.get("enforced") is True,
        "bucket_sample_count": empirical_bucket_gate.get("sample_count"),
        "bucket_net_pnl": empirical_bucket_gate.get("net_pnl"),
        "bucket_win_rate": empirical_bucket_gate.get("win_rate"),
        "bucket_net_positive_rate": empirical_bucket_gate.get("net_positive_rate"),
        "pre_empirical_candidate_status": pre_empirical_status,
        "pre_empirical_reason": pre_empirical_reason,
        "candidate_status": candidate_status,
        "reason": reason,
        "late_high_confidence_directional_entry": late_sure_thing,
        "late_override_allowed": empirical_bucket_gate.get("override_allowed") is True,
        "late_override_reason": empirical_bucket_gate.get("override_reason")
        or empirical_bucket_late_override.get("reason"),
        "target_yes_price_dollars": _money_text(_clamp_price(target_yes)),
        "execution_price_dollars": _money_text(_clamp_price(cost)),
        "entry_price_band": _price_band(cost),
        "remaining_payout_dollars": str(remaining_payout.quantize(Decimal("0.0001"))),
        "edge_bps": edge_bps,
        "expected_net_edge": str(expected_net_edge.quantize(Decimal("0.0001"))),
        "expected_fee": str(fee.quantize(Decimal("0.0001"))),
        "model_probability": str(model_probability.quantize(Decimal("0.0001"))),
        "raw_model_probability": str(raw_model_probability.quantize(Decimal("0.0001"))),
        "market_side_probability": (
            str(market_side_probability.quantize(Decimal("0.0001")))
            if market_side_probability is not None
            else None
        ),
        "time_to_close_seconds": time_to_close,
        "time_to_close_bucket": _crypto_time_to_close_bucket(float(time_to_close or 0)),
        "spread_bps": spread_bps,
        "spread_band": _spread_band(spread_bps),
        "yes_bid_dollars": _money_text(row.get("yes_bid_dollars")),
        "yes_ask_dollars": _money_text(row.get("yes_ask_dollars")),
        "no_bid_dollars": _money_text(row.get("no_bid_dollars")),
        "no_ask_dollars": _money_text(row.get("no_ask_dollars")),
        "volume": row.get("volume"),
        "open_interest": row.get("open_interest"),
        "market_age_seconds": row.get("market_age_seconds"),
        "spot_feature_status": row.get("spot_feature_status"),
        "spot_provider": row.get("spot_provider"),
        "spot_source_kind": row.get("spot_source_kind"),
        "spot_exchange_spread_bps": row.get("spot_exchange_spread_bps"),
        "spot_exchange_recent_trade_count": row.get("spot_exchange_recent_trade_count"),
        "gap_analysis_candidate": (
            pre_empirical_status == CRYPTO_LIVE_QUALITY
            and empirical_bucket_gate.get("enforced") is True
            and gate_reason != "empirical_bucket_allowed"
        ),
    }


def _crypto_empirical_bucket_gate_for_candidate(
    row: dict[str, Any],
    *,
    bucket_key: str,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    bucket_matrix: list[dict[str, Any]] | None,
    enforce: bool,
    requested_asset_symbols: list[str] | None = None,
    force_requested_assets: bool = False,
) -> dict[str, Any]:
    enabled_for_asset = _crypto_empirical_bucket_gate_enabled_for_asset(
        row,
        settings=settings,
        crypto_policy=crypto_policy,
        requested_asset_symbols=requested_asset_symbols,
        force_requested_assets=force_requested_assets,
    )
    if not enforce or not enabled_for_asset:
        return {
            "status": "not_evaluated" if enabled_for_asset else "not_applicable",
            "allowed": True,
            "enforced": False,
            "bucket_key": bucket_key,
            "reason": "empirical_bucket_gate_not_enforced" if enabled_for_asset else "asset_not_configured_for_empirical_bucket_gate",
        }
    bucket = _crypto_bucket_matrix_by_key(bucket_matrix).get(bucket_key)
    review = _crypto_empirical_bucket_review(bucket, settings=settings)
    return {
        **review,
        "enforced": True,
        "bucket_key": bucket_key,
    }


def _crypto_empirical_bucket_summary(
    bucket_matrix: list[dict[str, Any]] | None,
    *,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    requested_asset_symbols: list[str] | None = None,
    force_requested_assets: bool = False,
) -> dict[str, Any]:
    allowed: list[str] = []
    blocked: list[str] = []
    reviews: dict[str, dict[str, Any]] = {}
    enforced_assets: set[str] = set()
    for key, bucket in sorted(_crypto_bucket_matrix_by_key(bucket_matrix).items()):
        if not _crypto_empirical_bucket_gate_enabled_for_asset(
            bucket,
            settings=settings,
            crypto_policy=crypto_policy,
            requested_asset_symbols=requested_asset_symbols,
            force_requested_assets=force_requested_assets,
        ):
            continue
        enforced_assets.add(normalize_asset_symbol(str(bucket.get("asset_symbol") or "UNKNOWN")))
        review = _crypto_empirical_bucket_review(bucket, settings=settings)
        reviews[key] = review
        if review["allowed"]:
            allowed.append(key)
        else:
            blocked.append(key)
    return {
        "enabled": bool(settings.crypto_empirical_bucket_gate_enabled),
        "assets": sorted(_normalize_asset_csv(settings.crypto_empirical_bucket_gate_assets)),
        "enforced_assets": sorted(enforced_assets),
        "min_samples": int(settings.crypto_empirical_bucket_min_samples),
        "min_net_pnl_dollars": float(settings.crypto_empirical_bucket_min_net_pnl_dollars),
        "min_win_rate": float(settings.crypto_empirical_bucket_min_win_rate),
        "allowed_bucket_keys": allowed,
        "blocked_bucket_keys": blocked,
        "bucket_reviews": reviews,
    }


def _crypto_metrics_with_empirical_buckets(
    metrics: dict[str, Any],
    *,
    bucket_matrix: list[dict[str, Any]] | None,
    settings: Settings,
    crypto_policy: RuntimeCryptoPolicy | None = None,
    requested_asset_symbols: list[str] | None = None,
    force_requested_assets: bool = False,
) -> dict[str, Any]:
    summary = _crypto_empirical_bucket_summary(
        bucket_matrix,
        settings=settings,
        crypto_policy=crypto_policy,
        requested_asset_symbols=requested_asset_symbols,
        force_requested_assets=force_requested_assets,
    )
    return {
        **metrics,
        "bucket_matrix": list(bucket_matrix or []),
        "allowed_bucket_keys": summary["allowed_bucket_keys"],
        "blocked_bucket_keys": summary["blocked_bucket_keys"],
        "empirical_bucket_gate": summary,
    }


def _crypto_empirical_bucket_matrix_from_artifacts(*artifacts: Any) -> list[dict[str, Any]]:
    for artifact in artifacts:
        if artifact is None:
            continue
        metrics = artifact.metrics if isinstance(getattr(artifact, "metrics", None), dict) else {}
        payload = artifact.payload if isinstance(getattr(artifact, "payload", None), dict) else {}
        matrix = metrics.get("bucket_matrix")
        if isinstance(matrix, list):
            return [bucket for bucket in matrix if isinstance(bucket, dict)]
        matrix = payload.get("bucket_matrix")
        if isinstance(matrix, list):
            return [bucket for bucket in matrix if isinstance(bucket, dict)]
        walk_forward = payload.get("walk_forward") if isinstance(payload.get("walk_forward"), dict) else {}
        matrix = walk_forward.get("bucket_matrix") if isinstance(walk_forward, dict) else None
        if isinstance(matrix, list):
            return [bucket for bucket in matrix if isinstance(bucket, dict)]
    return []


def _crypto_last_minute_passive_price_matrix_from_artifacts(*artifacts: Any) -> list[dict[str, Any]]:
    for artifact in artifacts:
        if artifact is None:
            continue
        metrics = artifact.metrics if isinstance(getattr(artifact, "metrics", None), dict) else {}
        payload = artifact.payload if isinstance(getattr(artifact, "payload", None), dict) else {}
        matrix = metrics.get("last_minute_passive_price_matrix")
        if isinstance(matrix, list):
            return [row for row in matrix if isinstance(row, dict)]
        matrix = payload.get("last_minute_passive_price_matrix")
        if isinstance(matrix, list):
            return [row for row in matrix if isinstance(row, dict)]
        walk_forward = payload.get("walk_forward") if isinstance(payload.get("walk_forward"), dict) else {}
        matrix = walk_forward.get("last_minute_passive_price_matrix") if isinstance(walk_forward, dict) else None
        if isinstance(matrix, list):
            return [row for row in matrix if isinstance(row, dict)]
    return []


def _expected_crypto_net_pnl(
    market: CryptoMarket,
    side: ContractSide,
    fair_yes: Decimal,
    *,
    fee_rate: Decimal,
) -> Decimal | None:
    if side == ContractSide.YES:
        cost = market.yes_ask_dollars
        probability = fair_yes
    else:
        cost = market.no_ask_dollars if market.no_ask_dollars is not None else (Decimal("1") - market.yes_bid_dollars if market.yes_bid_dollars is not None else None)
        probability = Decimal("1") - fair_yes
    if cost is None:
        return None
    fee = estimate_kalshi_taker_fee_dollars(price_dollars=cost, count=Decimal("1.00"), fee_rate=fee_rate)
    return (probability - cost - fee).quantize(Decimal("0.0001"))


def _crypto_signal_expected_net_edge_bps(signal: StrategySignal) -> int | None:
    trace = signal.candidate_trace if isinstance(signal.candidate_trace, dict) else {}
    selection = trace.get("trade_selection_model") if isinstance(trace.get("trade_selection_model"), dict) else {}
    raw_value = selection.get("expected_net_edge") if isinstance(selection, dict) else None
    if raw_value in (None, ""):
        raw_value = trace.get("expected_net_edge")
    if raw_value in (None, ""):
        return None
    try:
        return int((_decimal(raw_value) * Decimal("10000")).to_integral_value())
    except Exception:
        return None


def _crypto_metric_deltas(metrics: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in ("brier", "log_loss", "ece"):
        calibrated = metrics.get(f"calibration_{name}")
        baseline = metrics.get(f"market_mid_{name}")
        result[name] = None
        if calibrated is not None and baseline is not None:
            result[name] = _ratio(float(baseline) - float(calibrated))
    return result


def _probability_metrics_decimal(predictions: list[tuple[Decimal, int]]) -> dict[str, Any]:
    if not predictions:
        return {"sample_count": 0, "brier": None, "log_loss": None, "ece": None}
    brier = sum((float(probability) - label) ** 2 for probability, label in predictions) / len(predictions)
    log_loss = -sum(
        label * math.log(max(1e-9, float(probability)))
        + (1 - label) * math.log(max(1e-9, 1 - float(probability)))
        for probability, label in predictions
    ) / len(predictions)
    buckets: dict[int, list[tuple[Decimal, int]]] = defaultdict(list)
    for probability, label in predictions:
        buckets[min(9, int(float(probability) * 10))].append((probability, label))
    ece = 0.0
    reliability = []
    for bucket, values in sorted(buckets.items()):
        predicted = sum(float(probability) for probability, _ in values) / len(values)
        observed = sum(label for _, label in values) / len(values)
        ece += (len(values) / len(predictions)) * abs(predicted - observed)
        reliability.append(
            {
                "bucket": f"{bucket / 10:.1f}-{(bucket + 1) / 10:.1f}",
                "sample_count": len(values),
                "avg_prediction": _ratio(predicted),
                "observed_rate": _ratio(observed),
            }
        )
    return {
        "sample_count": len(predictions),
        "brier": _ratio(brier),
        "log_loss": _ratio(log_loss),
        "ece": _ratio(ece),
        "reliability_buckets": reliability,
    }


def _crypto_max_drawdown(values: list[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _crypto_sharpe(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    variance = sum((value - mean) ** 2 for value in floats) / len(floats)
    return mean / max(math.sqrt(variance), 0.01)


def _crypto_sortino(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    downside = [value for value in floats if value < 0]
    downside_dev = math.sqrt(sum(value * value for value in downside) / len(downside)) if downside else 0.0
    return mean / max(downside_dev, 0.01)


def _price_band(price: Decimal) -> str:
    if price < Decimal("0.25"):
        return "0.00-0.25"
    if price < Decimal("0.50"):
        return "0.25-0.50"
    if price < Decimal("0.75"):
        return "0.50-0.75"
    return "0.75-1.00"


def _spread_band(spread_bps: Any) -> str:
    try:
        value = int(spread_bps or 0)
    except (TypeError, ValueError):
        return "unknown"
    if value <= 100:
        return "tight"
    if value <= 300:
        return "normal"
    return "wide"


def _ratio(value: float | None) -> float | None:
    return round(value, 6) if value is not None and math.isfinite(value) else None


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return _decimal(value)
    except Exception:
        return None


def _issue_code(reason: str) -> str:
    lowered = "".join(ch if ch.isalnum() else "_" for ch in reason.lower()).strip("_")
    return lowered[:80] or "crypto_replay_issue"
