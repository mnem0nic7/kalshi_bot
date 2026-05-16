from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.crypto.models import CryptoMarket, CryptoSeries

FIFTEEN_MIN_ALIASES = {"15m", "15min", "fifteen_min", "fifteen-minute", "fifteen_minute"}
ASSET_SYMBOL_ALIASES = {
    "RIPPLE": "XRP",
    "SOLE": "SOL",
}


def normalize_frequency(value: object) -> str | None:
    text = str(value or "").strip().lower().replace(" ", "_")
    if text in FIFTEEN_MIN_ALIASES:
        return "15m"
    if text in {"hourly", "1h", "one_hour"}:
        return "1h"
    if text in {"daily", "1d", "day"}:
        return "1d"
    return text or None


def asset_symbol_from_series(raw: dict[str, Any] | str) -> str:
    if isinstance(raw, str):
        ticker = raw
        title = ""
    else:
        ticker = str(raw.get("series_ticker") or raw.get("ticker") or "")
        title = str(raw.get("title") or raw.get("name") or "")
        for key in ("asset_symbol", "asset", "underlying"):
            value = raw.get(key)
            if isinstance(value, str) and 2 <= len(value.strip()) <= 8:
                return _clean_symbol(value)
        tags = raw.get("tags")
        if isinstance(tags, list):
            for tag in tags:
                value = str(tag).strip()
                if value.isupper() and 2 <= len(value) <= 8 and value not in {"CRYPTO"}:
                    return _clean_symbol(value)

    symbol = ticker.upper()
    if symbol.startswith("KX"):
        symbol = symbol[2:]
    for suffix in ("15M", "1H", "DAILY", "D"):
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
            break
    if symbol.startswith("X") and len(symbol) > 3:
        symbol = symbol[1:]
    if symbol:
        return _clean_symbol(symbol)

    match = re.search(r"\b([A-Z]{2,8})\b", title.upper())
    return _clean_symbol(match.group(1) if match else "CRYPTO")


def parse_crypto_series(raw: dict[str, Any], *, frequency: str = "15m") -> CryptoSeries | None:
    normalized = normalize_frequency(raw.get("frequency") or raw.get("market_frequency") or raw.get("settlement_frequency"))
    if normalized != normalize_frequency(frequency):
        return None
    category = str(raw.get("category") or raw.get("category_name") or "").strip()
    if category.lower() != "crypto":
        return None
    ticker = str(raw.get("series_ticker") or raw.get("ticker") or "").strip()
    if not ticker:
        return None
    return CryptoSeries(
        series_ticker=ticker,
        title=raw.get("title") or raw.get("name"),
        category=category,
        frequency=normalized or "15m",
        asset_symbol=asset_symbol_from_series(raw),
        raw=raw,
    )


def parse_crypto_market(
    raw: dict[str, Any],
    *,
    series: CryptoSeries | None = None,
    frequency: str = "15m",
) -> CryptoMarket | None:
    market = raw.get("market") if isinstance(raw.get("market"), dict) else raw
    ticker = str(market.get("ticker") or market.get("market_ticker") or "").strip()
    if not ticker:
        return None
    series_ticker = str(
        market.get("series_ticker")
        or market.get("series")
        or (series.series_ticker if series is not None else "")
    ).strip()
    if not series_ticker:
        series_ticker = _series_from_market_ticker(ticker)
    asset = series.asset_symbol if series is not None else asset_symbol_from_series(series_ticker)
    open_time = parse_datetime(market.get("open_time") or market.get("open_ts"))
    close_time = parse_datetime(market.get("close_time") or market.get("close_ts"))
    expected_expiration_time = parse_datetime(
        market.get("expected_expiration_time")
        or market.get("expected_expiration_ts")
        or market.get("expiration_time")
    )
    normalized = (
        normalize_frequency(series.frequency if series is not None else market.get("frequency"))
        or _frequency_from_ticker(series_ticker)
        or _frequency_from_duration(open_time, close_time)
        or normalize_frequency(frequency)
        or "15m"
    )
    return CryptoMarket(
        market_ticker=ticker,
        series_ticker=series_ticker,
        event_ticker=market.get("event_ticker"),
        asset_symbol=asset,
        frequency=normalized,
        title=market.get("title") or market.get("market_title") or market.get("yes_sub_title"),
        subtitle=market.get("subtitle") or market.get("sub_title"),
        status=market.get("status"),
        open_time=open_time,
        close_time=close_time,
        expected_expiration_time=expected_expiration_time,
        target_price_dollars=parse_target_price(market),
        yes_bid_dollars=parse_price(market, dollar_keys=("yes_bid_dollars",), cent_keys=("yes_bid",)),
        yes_ask_dollars=parse_price(market, dollar_keys=("yes_ask_dollars",), cent_keys=("yes_ask",)),
        no_bid_dollars=parse_price(market, dollar_keys=("no_bid_dollars",), cent_keys=("no_bid",)),
        no_ask_dollars=parse_price(market, dollar_keys=("no_ask_dollars",), cent_keys=("no_ask",)),
        last_price_dollars=parse_price(
            market,
            dollar_keys=("last_price_dollars", "last_trade_price_dollars"),
            cent_keys=("last_price", "last_trade_price"),
        ),
        volume=parse_int(market.get("volume") or market.get("volume_24h")),
        open_interest=parse_int(market.get("open_interest")),
        settlement_result=parse_settlement_result(market),
        raw=market,
    )


def parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        timestamp = float(value) / (1000 if float(value) > 10_000_000_000 else 1)
        return datetime.fromtimestamp(timestamp, tz=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.isdigit():
            return parse_datetime(int(text))
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def parse_price(
    raw: dict[str, Any],
    *,
    dollar_keys: tuple[str, ...],
    cent_keys: tuple[str, ...],
) -> Decimal | None:
    for key in dollar_keys:
        value = raw.get(key)
        if value not in (None, ""):
            return _quantized_price(value)
    for key in cent_keys:
        value = raw.get(key)
        if value in (None, ""):
            continue
        decimal = _decimal(value)
        if decimal is None:
            continue
        if decimal > Decimal("1"):
            decimal = decimal / Decimal("100")
        return _quantized_price(decimal)
    return None


def parse_target_price(raw: dict[str, Any]) -> Decimal | None:
    for key in (
        "floor_strike",
        "floor_strike_dollars",
        "strike_price",
        "strike_price_dollars",
        "cap_strike",
        "cap_strike_dollars",
    ):
        value = raw.get(key)
        if value in (None, ""):
            continue
        decimal = _decimal(value)
        if decimal is not None:
            return decimal.quantize(Decimal("0.00000001"))
        match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", str(value))
        if match:
            return Decimal(match.group(0).replace(",", "")).quantize(Decimal("0.00000001"))
    for key in ("yes_sub_title", "no_sub_title", "subtitle", "sub_title", "rules_primary", "title", "market_title"):
        value = str(raw.get(key) or "")
        match = re.search(r"\$\s*([-+]?\d[\d,]*(?:\.\d+)?)", value)
        if match:
            return Decimal(match.group(1).replace(",", "")).quantize(Decimal("0.00000001"))
    return None


def parse_settlement_result(raw: dict[str, Any]) -> str | None:
    for key in ("settlement_result", "result", "market_result", "settled_result"):
        value = raw.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip().lower()
        if text in {"yes", "y", "above", "1", "true"}:
            return "yes"
        if text in {"no", "n", "below", "0", "false"}:
            return "no"
        return text
    return None


def parse_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def normalize_candlestick(raw: dict[str, Any]) -> dict[str, Any] | None:
    end_period = parse_datetime(
        raw.get("end_period_ts")
        or raw.get("end_period_time")
        or raw.get("period_end")
        or raw.get("time")
        or raw.get("timestamp")
    )
    if end_period is None:
        return None
    price_source = raw
    for key in ("yes_ask", "yes_bid", "price", "prices"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            price_source = nested
            break
    return {
        "end_period_ts": end_period,
        "period_interval": parse_int(raw.get("period_interval") or raw.get("interval") or 1) or 1,
        "open_dollars": _ohlc_price(price_source, "open"),
        "high_dollars": _ohlc_price(price_source, "high"),
        "low_dollars": _ohlc_price(price_source, "low"),
        "close_dollars": _ohlc_price(price_source, "close"),
        "volume": parse_int(raw.get("volume") or raw.get("volume_fp")),
        "payload": raw,
    }


def _ohlc_price(raw: dict[str, Any], key: str) -> Decimal | None:
    for candidate in (f"{key}_dollars", key, f"{key}_price"):
        if candidate in raw and raw[candidate] not in (None, ""):
            value = _decimal(raw[candidate])
            if value is None:
                return None
            if value > Decimal("1"):
                value = value / Decimal("100")
            return _quantized_price(value)
    return None


def _series_from_market_ticker(ticker: str) -> str:
    upper = ticker.upper()
    if "-" in upper:
        return upper.split("-", 1)[0]
    match = re.match(r"([A-Z0-9]+15M)", upper)
    return match.group(1) if match else upper


def _frequency_from_ticker(ticker: str) -> str | None:
    upper = str(ticker or "").upper()
    if "15M" in upper:
        return "15m"
    if upper.endswith("1H"):
        return "1h"
    return None


def _frequency_from_duration(open_time: datetime | None, close_time: datetime | None) -> str | None:
    if open_time is None or close_time is None:
        return None
    seconds = int((close_time - open_time).total_seconds())
    if 600 <= seconds <= 1_200:
        return "15m"
    if 3_000 <= seconds <= 4_200:
        return "1h"
    if 80_000 <= seconds <= 90_000:
        return "1d"
    return None


def _clean_symbol(value: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]", "", value.upper())
    cleaned = ASSET_SYMBOL_ALIASES.get(cleaned, cleaned)
    return cleaned or "CRYPTO"


def _decimal(value: object) -> Decimal | None:
    try:
        return Decimal(str(value).replace(",", "").replace("$", "").strip())
    except (InvalidOperation, ValueError):
        return None


def _quantized_price(value: object) -> Decimal | None:
    decimal = _decimal(value)
    if decimal is None:
        return None
    try:
        return quantize_price(decimal)
    except ValueError:
        return None
