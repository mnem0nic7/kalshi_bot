from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from kalshi_bot.db.models import HistoricalMarketSnapshotRecord
from kalshi_bot.db.repositories import PlatformRepository


DECISION_SIGNAL_MARKET_SOURCE_KIND = "decision_signal_market_snapshot"
DAEMON_MARKET_PRICE_SOURCE_KIND = "daemon_market_price_snapshot"
TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND = "trade_analysis_backfill_candlestick"


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _as_utc(value: Any | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.0001"))
    except Exception:
        return None


def _market_from_response(market_response: dict[str, Any]) -> dict[str, Any]:
    market = market_response.get("market") if isinstance(market_response.get("market"), dict) else market_response
    return dict(market) if isinstance(market, dict) else {}


def _series_from_ticker(ticker: str) -> str | None:
    series = str(ticker or "").split("-", 1)[0]
    return series or None


def _market_day_from_ticker(ticker: str) -> str:
    parts = str(ticker or "").split("-")
    return parts[1] if len(parts) >= 2 else "unknown"


def _timestamp_from_market(market: dict[str, Any], *keys: str) -> datetime | None:
    for key in keys:
        if value := _as_utc(market.get(key)):
            return value
    return None


async def archive_point_in_time_market_snapshot(
    repo: PlatformRepository,
    *,
    market_response: dict[str, Any],
    observed_at: datetime | None,
    kalshi_env: str,
    market_ticker: str,
    source_kind: str,
    source_id: str,
    recovered: bool = False,
    leakage_risk: str = "none",
    decision_ts: datetime | None = None,
    extra_payload: dict[str, Any] | None = None,
) -> HistoricalMarketSnapshotRecord | None:
    """Persist a quote snapshot into durable historical storage.

    The caller is responsible for passing an observed-at time that was known at
    decision time. Rows with no bid/ask are ignored because they cannot support
    trade-analysis or backtesting execution simulation.
    """
    observed_at = _as_utc(observed_at)
    if observed_at is None:
        return None
    market = _market_from_response(market_response)
    ticker = str(market.get("ticker") or market.get("market_ticker") or market_ticker)
    yes_bid = _decimal_or_none(market.get("yes_bid_dollars") or market.get("yes_bid"))
    yes_ask = _decimal_or_none(market.get("yes_ask_dollars") or market.get("yes_ask"))
    no_ask = _decimal_or_none(market.get("no_ask_dollars") or market.get("no_ask"))
    if no_ask is None and yes_bid is not None:
        no_ask = (Decimal("1.0000") - yes_bid).quantize(Decimal("0.0001"))
    if yes_bid is None and yes_ask is None:
        return None

    last_price = _decimal_or_none(
        market.get("last_price_dollars")
        or market.get("last_trade_dollars")
        or market.get("last_price")
    )
    compact_market = {
        "ticker": ticker,
        "yes_bid_dollars": str(yes_bid) if yes_bid is not None else None,
        "yes_ask_dollars": str(yes_ask) if yes_ask is not None else None,
        "no_ask_dollars": str(no_ask) if no_ask is not None else None,
        "last_price_dollars": str(last_price) if last_price is not None else None,
        "volume": market.get("volume"),
        "close_ts": market.get("close_ts") or market.get("close_time"),
    }
    provenance = {
        "recovered": recovered,
        "source": source_kind,
        "source_kind": source_kind,
        "source_id": source_id,
        "kalshi_env": kalshi_env,
        "market_ticker": ticker,
        "observed_at": observed_at.isoformat(),
        "decision_ts": _as_utc(decision_ts).isoformat() if _as_utc(decision_ts) is not None else None,
        "leakage_risk": leakage_risk,
    }
    payload = {
        "market": compact_market,
        "source_snapshot": market_response,
        "snapshot_provenance": provenance,
        **(extra_payload or {}),
    }
    return await repo.upsert_historical_market_snapshot(
        market_ticker=ticker,
        series_ticker=_series_from_ticker(ticker),
        station_id=None,
        local_market_day=_market_day_from_ticker(ticker),
        asof_ts=observed_at,
        source_kind=source_kind,
        source_id=source_id,
        source_hash=_hash_payload(payload),
        close_ts=_timestamp_from_market(market, "close_time", "close_ts"),
        settlement_ts=_timestamp_from_market(market, "settlement_ts", "settlement_time"),
        yes_bid_dollars=yes_bid,
        yes_ask_dollars=yes_ask,
        no_ask_dollars=no_ask,
        last_price_dollars=last_price,
        payload=payload,
    )
