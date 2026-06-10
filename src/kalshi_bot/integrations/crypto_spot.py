from __future__ import annotations

import base64
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import secrets
import time
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, utils
import httpx


COINBASE_PRODUCT_IDS = {
    "ADA": "ADA-USD",
    "BCH": "BCH-USD",
    "BNB": "BNB-USD",
    "BTC": "BTC-USD",
    "DOGE": "DOGE-USD",
    "ETH": "ETH-USD",
    "HYPE": "HYPE-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}

COINGECKO_IDS = {
    "ADA": "cardano",
    "BCH": "bitcoin-cash",
    "BNB": "binancecoin",
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "ETH": "ethereum",
    "HYPE": "hyperliquid",
    "SOL": "solana",
    "XRP": "ripple",
}

# Requested Kraken pair per asset. None means the asset is not listed on Kraken
# and must be skipped (never proxied). Kraken responds keyed by its canonical
# pair name (e.g. "XBTUSD" comes back as "XXBTZUSD"), so parsing must not rely
# on the requested pair appearing in the result.
KRAKEN_PAIRS: dict[str, str | None] = {
    "ADA": "ADAUSD",
    "BCH": "BCHUSD",
    "BNB": None,
    "BTC": "XBTUSD",
    "DOGE": "DOGEUSD",
    "ETH": "ETHUSD",
    "HYPE": None,
    "SOL": "SOLUSD",
    "XRP": "XRPUSD",
}


@dataclass(slots=True)
class SpotOHLC:
    provider: str
    asset_symbol: str
    start_ts: datetime | None
    end_ts: datetime
    open_dollars: Decimal | None
    high_dollars: Decimal | None
    low_dollars: Decimal | None
    close_dollars: Decimal | None
    volume: Decimal | None = None
    source_kind: str = "spot_ohlc"
    source_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CoinbaseCdpCredentials:
    name: str
    private_key: str


def load_coinbase_cdp_credentials(
    *,
    key_file: str | None = None,
    key_name: str | None = None,
    private_key: str | None = None,
) -> CoinbaseCdpCredentials | None:
    name = str(key_name or "").strip()
    secret = str(private_key or "").strip()
    if name and secret:
        return CoinbaseCdpCredentials(name=name, private_key=_normalize_pem(secret))
    if not key_file:
        return None
    path = Path(key_file).expanduser()
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    file_name = str(data.get("name") or "").strip()
    file_key = str(data.get("privateKey") or "").strip()
    if not file_name or not file_key:
        return None
    return CoinbaseCdpCredentials(name=file_name, private_key=_normalize_pem(file_key))


def normalize_spot_asset_symbol(asset_symbol: str) -> str:
    normalized = "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())
    if not normalized:
        raise ValueError("asset_symbol is required")
    return normalized


def interval_seconds_for_frequency(frequency: str) -> int:
    normalized = str(frequency or "").strip().lower()
    if normalized in {"15m", "15min", "fifteen_min", "fifteen-minute", "fifteen_minute"}:
        return 900
    if normalized in {"1h", "hourly", "one_hour"}:
        return 3600
    if normalized in {"1d", "daily", "day"}:
        return 86_400
    raise ValueError(f"unsupported crypto spot frequency: {frequency}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _floor_time(value: datetime, interval_seconds: int) -> datetime:
    ts = int(_utc(value).timestamp())
    return datetime.fromtimestamp(ts - (ts % interval_seconds), UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _normalize_pem(value: str) -> str:
    return value.replace("\\n", "\n")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _json_b64url(data: dict[str, Any]) -> str:
    return _b64url(json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _coinbase_jwt_uri(method: str, request_host: str, request_path: str) -> str:
    return f"{method.upper()} {request_host}{request_path}"


def _build_coinbase_rest_jwt(
    *,
    credentials: CoinbaseCdpCredentials,
    method: str,
    request_host: str,
    request_path: str,
) -> str:
    key = serialization.load_pem_private_key(credentials.private_key.encode("utf-8"), password=None)
    now = int(time.time())
    header: dict[str, Any] = {
        "typ": "JWT",
        "kid": credentials.name,
        "nonce": secrets.token_hex(16),
    }
    claims = {
        "sub": credentials.name,
        "iss": "cdp",
        "aud": ["cdp_service"],
        "nbf": now,
        "exp": now + 120,
        "uri": _coinbase_jwt_uri(method, request_host, request_path),
    }
    if isinstance(key, ec.EllipticCurvePrivateKey):
        header["alg"] = "ES256"
        signing_input = f"{_json_b64url(header)}.{_json_b64url(claims)}".encode("ascii")
        signature_der = key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
        r, s = utils.decode_dss_signature(signature_der)
        signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    elif isinstance(key, ed25519.Ed25519PrivateKey):
        header["alg"] = "EdDSA"
        signing_input = f"{_json_b64url(header)}.{_json_b64url(claims)}".encode("ascii")
        signature = key.sign(signing_input)
    else:
        raise TypeError("unsupported Coinbase CDP private key type")
    return f"{signing_input.decode('ascii')}.{_b64url(signature)}"


def _parse_coinbase_candles_payload(
    payload: Any,
    *,
    symbol: str,
    product_id: str,
    interval_seconds: int,
) -> list[SpotOHLC]:
    raw_rows = payload.get("candles") if isinstance(payload, dict) else payload
    if not isinstance(raw_rows, list):
        return []
    rows: list[SpotOHLC] = []
    for raw in raw_rows:
        if isinstance(raw, dict):
            start_raw = raw.get("start") or raw.get("start_ts")
            start_ts = datetime.fromtimestamp(int(start_raw), UTC) if str(start_raw or "").isdigit() else _parse_datetime(start_raw)
            if start_ts is None:
                continue
            rows.append(
                SpotOHLC(
                    provider="coinbase",
                    asset_symbol=symbol,
                    start_ts=start_ts,
                    end_ts=start_ts + timedelta(seconds=interval_seconds),
                    open_dollars=_decimal(raw.get("open")),
                    high_dollars=_decimal(raw.get("high")),
                    low_dollars=_decimal(raw.get("low")),
                    close_dollars=_decimal(raw.get("close")),
                    volume=_decimal(raw.get("volume")),
                    source_kind="spot_ohlc",
                    source_id=product_id,
                    payload={"raw": raw, "product_id": product_id},
                )
            )
            continue
        if not isinstance(raw, list) or len(raw) < 5:
            continue
        start_ts = datetime.fromtimestamp(int(raw[0]), UTC)
        rows.append(
            SpotOHLC(
                provider="coinbase",
                asset_symbol=symbol,
                start_ts=start_ts,
                end_ts=start_ts + timedelta(seconds=interval_seconds),
                open_dollars=_decimal(raw[3]),
                high_dollars=_decimal(raw[2]),
                low_dollars=_decimal(raw[1]),
                close_dollars=_decimal(raw[4]),
                volume=_decimal(raw[5]) if len(raw) >= 6 else None,
                source_kind="spot_ohlc",
                source_id=product_id,
                payload={"raw": raw, "product_id": product_id},
            )
        )
    return rows


def _normalize_coinbase_ticker_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    trades = payload.get("trades")
    if isinstance(trades, list) and trades:
        latest = trades[0]
        if isinstance(latest, dict):
            return {
                **latest,
                "price": latest.get("price") or latest.get("trade_price"),
                "time": latest.get("time") or latest.get("trade_time"),
                "volume": latest.get("size") or latest.get("volume"),
            }
    return payload


def _normalize_coinbase_trade(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    price = raw.get("price") or raw.get("trade_price")
    observed = raw.get("time") or raw.get("trade_time")
    if price in (None, ""):
        return None
    return {
        "trade_id": raw.get("trade_id"),
        "product_id": raw.get("product_id"),
        "price_dollars": str(price),
        "size": str(raw.get("size") or raw.get("volume") or ""),
        "side": raw.get("side"),
        "time": str(observed or ""),
        "bid_dollars": str(raw.get("bid") or ""),
        "ask_dollars": str(raw.get("ask") or ""),
    }


def _normalize_coinbase_trades_payload(payload: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        raw_trades = payload.get("trades")
    else:
        raw_trades = payload
    if not isinstance(raw_trades, list):
        return []
    trades: list[dict[str, Any]] = []
    for raw in raw_trades[: max(0, limit)]:
        trade = _normalize_coinbase_trade(raw)
        if trade is not None:
            trades.append(trade)
    return trades


def _normalize_coinbase_best_bid_ask_payload(payload: Any, *, product_id: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    raw_book: dict[str, Any] | None = None
    books = payload.get("pricebooks")
    if isinstance(books, list):
        for candidate in books:
            if isinstance(candidate, dict) and str(candidate.get("product_id") or "") == product_id:
                raw_book = candidate
                break
        if raw_book is None and books and isinstance(books[0], dict):
            raw_book = books[0]
    else:
        raw_book = payload
    if not isinstance(raw_book, dict):
        return None
    bid_price = raw_book.get("best_bid") or raw_book.get("bid")
    ask_price = raw_book.get("best_ask") or raw_book.get("ask")
    bid_size = raw_book.get("best_bid_size") or raw_book.get("bid_size")
    ask_size = raw_book.get("best_ask_size") or raw_book.get("ask_size")
    bids = raw_book.get("bids")
    asks = raw_book.get("asks")
    if isinstance(bids, list) and bids and isinstance(bids[0], dict):
        bid_price = bid_price or bids[0].get("price")
        bid_size = bid_size or bids[0].get("size")
    if isinstance(asks, list) and asks and isinstance(asks[0], dict):
        ask_price = ask_price or asks[0].get("price")
        ask_size = ask_size or asks[0].get("size")
    bid = _decimal(bid_price)
    ask = _decimal(ask_price)
    spread_bps = None
    mid = None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / Decimal("2")
        if mid > 0:
            spread_bps = int(((ask - bid) / mid * Decimal("10000")).to_integral_value())
    return {
        "product_id": raw_book.get("product_id") or product_id,
        "best_bid_dollars": str(bid) if bid is not None else "",
        "best_ask_dollars": str(ask) if ask is not None else "",
        "best_bid_size": str(bid_size or ""),
        "best_ask_size": str(ask_size or ""),
        "mid_dollars": str(mid) if mid is not None else "",
        "spread_bps": spread_bps,
        "time": str(raw_book.get("time") or ""),
    }


def _coinbase_market_microstructure(
    *,
    product_id: str,
    ticker_payload: Any,
    best_bid_ask: dict[str, Any] | None,
    authenticated: bool,
) -> dict[str, Any]:
    ticker_book = _normalize_coinbase_best_bid_ask_payload(ticker_payload, product_id=product_id)
    book = best_bid_ask or ticker_book or {}
    trades = _normalize_coinbase_trades_payload(ticker_payload, limit=20)
    latest_trade = trades[0] if trades else None
    return {
        "product_id": product_id,
        "authenticated": authenticated,
        "best_bid_ask": book,
        "recent_trades": trades,
        "recent_trade_count": len(trades),
        "latest_trade": latest_trade,
    }


class CoinbaseSpotClient:
    base_url = "https://api.exchange.coinbase.com"
    authenticated_base_url = "https://api.coinbase.com"
    authenticated_request_host = "api.coinbase.com"

    def __init__(
        self,
        *,
        timeout_seconds: float = 30.0,
        credentials: CoinbaseCdpCredentials | None = None,
    ) -> None:
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "kalshi-bot/crypto-spot"},
        )
        self.authenticated_client = httpx.AsyncClient(
            base_url=self.authenticated_base_url,
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "kalshi-bot/crypto-spot"},
        )
        self.credentials = credentials

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.authenticated_client.aclose()

    async def _authenticated_get(self, request_path: str, *, params: dict[str, Any] | None = None) -> httpx.Response | None:
        if self.credentials is None:
            return None
        jwt = _build_coinbase_rest_jwt(
            credentials=self.credentials,
            method="GET",
            request_host=self.authenticated_request_host,
            request_path=request_path,
        )
        response = await self.authenticated_client.get(
            request_path,
            params=params,
            headers={
                "Authorization": f"Bearer {jwt}",
                "Cache-Control": "no-cache",
            },
        )
        response.raise_for_status()
        return response

    async def fetch_ohlc(
        self,
        asset_symbol: str,
        *,
        start: datetime,
        end: datetime,
        interval_seconds: int,
    ) -> list[SpotOHLC]:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        product_id = COINBASE_PRODUCT_IDS.get(symbol)
        if product_id is None:
            raise KeyError(f"coinbase unsupported asset: {symbol}")
        start_utc = _utc(start)
        end_utc = _utc(end)
        if start_utc >= end_utc:
            return []

        rows: list[SpotOHLC] = []
        # Coinbase rejects candle requests above 300 points, so fetch bounded windows.
        chunk_seconds = max(interval_seconds, interval_seconds * 300)
        cursor = start_utc
        while cursor < end_utc:
            chunk_end = min(end_utc, cursor + timedelta(seconds=chunk_seconds))
            request_path = f"/api/v3/brokerage/products/{product_id}/candles"
            params = {
                "start": cursor.isoformat().replace("+00:00", "Z"),
                "end": chunk_end.isoformat().replace("+00:00", "Z"),
                "granularity": interval_seconds,
            }
            try:
                response = await self._authenticated_get(request_path, params=params)
            except Exception:
                response = None
            if response is None:
                response = await self.client.get(f"/products/{product_id}/candles", params=params)
            response.raise_for_status()
            payload = response.json()
            rows.extend(_parse_coinbase_candles_payload(payload, symbol=symbol, product_id=product_id, interval_seconds=interval_seconds))
            cursor = chunk_end
        deduped = {(row.provider, row.asset_symbol, row.end_ts): row for row in rows}
        return sorted(deduped.values(), key=lambda row: row.end_ts)

    async def fetch_best_bid_ask(self, asset_symbol: str) -> dict[str, Any] | None:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        product_id = COINBASE_PRODUCT_IDS.get(symbol)
        if product_id is None:
            raise KeyError(f"coinbase unsupported asset: {symbol}")
        request_path = "/api/v3/brokerage/best_bid_ask"
        try:
            response = await self._authenticated_get(request_path, params={"product_ids": product_id})
            if response is not None:
                return _normalize_coinbase_best_bid_ask_payload(response.json(), product_id=product_id)
        except Exception:
            pass
        try:
            response = await self.client.get(f"/products/{product_id}/book", params={"level": 1}, headers={"Cache-Control": "no-cache"})
            response.raise_for_status()
            return _normalize_coinbase_best_bid_ask_payload(response.json(), product_id=product_id)
        except Exception:
            return None

    async def fetch_product(self, product_id: str) -> dict[str, Any] | None:
        normalized = str(product_id or "").strip().upper()
        if not normalized:
            raise ValueError("product_id is required")
        request_path = f"/api/v3/brokerage/products/{normalized}"
        try:
            response = await self._authenticated_get(request_path)
            if response is not None:
                payload = response.json()
                if isinstance(payload, dict):
                    product = payload.get("product") if isinstance(payload.get("product"), dict) else payload
                    return dict(product)
        except Exception:
            pass
        try:
            response = await self.client.get(f"/products/{normalized}")
            response.raise_for_status()
            payload = response.json()
            return dict(payload) if isinstance(payload, dict) else None
        except Exception:
            return None

    async def fetch_current(self, asset_symbol: str) -> SpotOHLC | None:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        product_id = COINBASE_PRODUCT_IDS.get(symbol)
        if product_id is None:
            raise KeyError(f"coinbase unsupported asset: {symbol}")
        request_path = f"/api/v3/brokerage/products/{product_id}/ticker"
        authenticated = False
        try:
            response = await self._authenticated_get(request_path, params={"limit": "20"})
            authenticated = response is not None
        except Exception:
            response = None
        if response is None:
            response = await self.client.get(f"/products/{product_id}/ticker", headers={"Cache-Control": "no-cache"})
        response.raise_for_status()
        payload = response.json()
        try:
            best_bid_ask = await self.fetch_best_bid_ask(symbol)
        except Exception:
            best_bid_ask = None
        microstructure = _coinbase_market_microstructure(
            product_id=product_id,
            ticker_payload=payload,
            best_bid_ask=best_bid_ask,
            authenticated=authenticated,
        )
        raw = _normalize_coinbase_ticker_payload(payload)
        book_price = _decimal((microstructure.get("best_bid_ask") or {}).get("mid_dollars") if isinstance(microstructure.get("best_bid_ask"), dict) else None)
        price = book_price or _decimal(raw.get("price"))
        if price is None:
            return None
        now = datetime.now(UTC)
        observed_candidates = [
            value
            for value in (
                _parse_datetime(((microstructure.get("best_bid_ask") or {}).get("time")) if isinstance(microstructure.get("best_bid_ask"), dict) else None),
                _parse_datetime(raw.get("time") or raw.get("trade_time")),
            )
            if value is not None
        ]
        if not observed_candidates and best_bid_ask is not None:
            observed_candidates.append(now)
        observed = min(max(observed_candidates) if observed_candidates else now, now)
        return SpotOHLC(
            provider="coinbase",
            asset_symbol=symbol,
            start_ts=None,
            end_ts=observed,
            open_dollars=None,
            high_dollars=None,
            low_dollars=None,
            close_dollars=price,
            volume=_decimal(raw.get("volume") or raw.get("volume_24h")),
            source_kind="spot_tick",
            source_id=product_id,
            payload={
                "raw": payload,
                "product_id": product_id,
                "authenticated": authenticated,
                "market_microstructure": microstructure,
            },
        )


class CoinGeckoSpotClient:
    base_url = "https://api.coingecko.com/api/v3"

    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "kalshi-bot/crypto-spot"},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def fetch_ohlc(
        self,
        asset_symbol: str,
        *,
        start: datetime,
        end: datetime,
        interval_seconds: int,
    ) -> list[SpotOHLC]:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        coin_id = COINGECKO_IDS.get(symbol)
        if coin_id is None:
            raise KeyError(f"coingecko unsupported asset: {symbol}")
        start_utc = _utc(start)
        end_utc = _utc(end)
        if start_utc >= end_utc:
            return []
        response = await self.client.get(
            f"/coins/{coin_id}/market_chart/range",
            params={
                "vs_currency": "usd",
                "from": int(start_utc.timestamp()),
                "to": int(end_utc.timestamp()),
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        prices = payload.get("prices") if isinstance(payload.get("prices"), list) else []
        volumes = payload.get("total_volumes") if isinstance(payload.get("total_volumes"), list) else []
        volume_by_ms = {
            int(item[0]): _decimal(item[1])
            for item in volumes
            if isinstance(item, list) and len(item) >= 2 and _decimal(item[1]) is not None
        }
        grouped: dict[datetime, list[tuple[datetime, Decimal, Decimal | None]]] = defaultdict(list)
        for item in prices:
            if not isinstance(item, list) or len(item) < 2:
                continue
            price = _decimal(item[1])
            if price is None:
                continue
            observed = datetime.fromtimestamp(int(item[0]) / 1000, UTC)
            bucket_end = _floor_time(observed, interval_seconds) + timedelta(seconds=interval_seconds)
            if bucket_end > end_utc + timedelta(seconds=interval_seconds):
                continue
            grouped[bucket_end].append((observed, price, volume_by_ms.get(int(item[0]))))

        rows: list[SpotOHLC] = []
        for bucket_end, values in sorted(grouped.items()):
            values.sort(key=lambda item: item[0])
            prices_in_bucket = [value[1] for value in values]
            volume = sum((value[2] for value in values if value[2] is not None), Decimal("0"))
            rows.append(
                SpotOHLC(
                    provider="coingecko",
                    asset_symbol=symbol,
                    start_ts=bucket_end - timedelta(seconds=interval_seconds),
                    end_ts=bucket_end,
                    open_dollars=prices_in_bucket[0],
                    high_dollars=max(prices_in_bucket),
                    low_dollars=min(prices_in_bucket),
                    close_dollars=prices_in_bucket[-1],
                    volume=volume if volume else None,
                    source_kind="spot_price_proxy",
                    source_id=coin_id,
                    payload={
                        "coin_id": coin_id,
                        "proxy_ohlc": True,
                        "point_count": len(values),
                        "first_observed_at": values[0][0].isoformat(),
                        "last_observed_at": values[-1][0].isoformat(),
                    },
                )
            )
        return rows

    async def fetch_current(self, asset_symbol: str) -> SpotOHLC | None:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        coin_id = COINGECKO_IDS.get(symbol)
        if coin_id is None:
            raise KeyError(f"coingecko unsupported asset: {symbol}")
        response = await self.client.get(
            "/simple/price",
            params={
                "ids": coin_id,
                "vs_currencies": "usd",
                "include_last_updated_at": "true",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        raw = payload.get(coin_id)
        if not isinstance(raw, dict):
            return None
        price = _decimal(raw.get("usd"))
        if price is None:
            return None
        updated_at = raw.get("last_updated_at")
        now = datetime.now(UTC)
        observed = min(datetime.fromtimestamp(int(updated_at), UTC), now) if updated_at is not None else now
        return SpotOHLC(
            provider="coingecko",
            asset_symbol=symbol,
            start_ts=None,
            end_ts=observed,
            open_dollars=None,
            high_dollars=None,
            low_dollars=None,
            close_dollars=price,
            volume=None,
            source_kind="spot_price_proxy",
            source_id=coin_id,
            payload={"raw": raw, "coin_id": coin_id, "proxy_ohlc": True, "current_price": True},
        )


def _parse_kraken_ohlc_payload(
    payload: Any,
    *,
    symbol: str,
    requested_pair: str,
    interval_seconds: int,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[SpotOHLC]:
    if not isinstance(payload, dict):
        return []
    errors = payload.get("error")
    if isinstance(errors, list) and errors:
        raise ValueError(f"kraken OHLC error for {requested_pair}: {errors}")
    result = payload.get("result")
    if not isinstance(result, dict):
        return []
    # Kraken keys the candle list by its canonical pair name (e.g. XXBTZUSD for
    # a requested XBTUSD), so take the first non-"last" key.
    pair_key = next((key for key in result if key != "last"), None)
    candles = result.get(pair_key) if pair_key is not None else None
    if not isinstance(candles, list):
        return []
    rows: list[SpotOHLC] = []
    for item in candles:
        if not isinstance(item, (list, tuple)) or len(item) < 7:
            continue
        try:
            start_ts = datetime.fromtimestamp(int(float(item[0])), UTC)
        except Exception:
            continue
        end_ts = start_ts + timedelta(seconds=interval_seconds)
        if start is not None and end_ts <= _utc(start):
            continue
        if end is not None and end_ts > _utc(end) + timedelta(seconds=interval_seconds):
            continue
        close = _decimal(item[4])
        if close is None:
            continue
        rows.append(
            SpotOHLC(
                provider="kraken",
                asset_symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                open_dollars=_decimal(item[1]),
                high_dollars=_decimal(item[2]),
                low_dollars=_decimal(item[3]),
                close_dollars=close,
                volume=_decimal(item[6]),
                source_kind="spot_ohlc",
                source_id=pair_key,
                payload={
                    "pair": pair_key,
                    "requested_pair": requested_pair,
                    "vwap": str(item[5]) if len(item) > 5 and item[5] not in (None, "") else None,
                    "trade_count": int(item[7]) if len(item) > 7 and isinstance(item[7], (int, float)) else None,
                },
            )
        )
    rows.sort(key=lambda row: row.end_ts)
    return rows


class KrakenSpotClient:
    base_url = "https://api.kraken.com"

    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": "kalshi-bot/crypto-spot"},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def fetch_ohlc(
        self,
        asset_symbol: str,
        *,
        start: datetime,
        end: datetime,
        interval_seconds: int,
    ) -> list[SpotOHLC]:
        symbol = normalize_spot_asset_symbol(asset_symbol)
        pair = KRAKEN_PAIRS.get(symbol)
        if pair is None:
            raise KeyError(f"kraken unsupported asset: {symbol}")
        start_utc = _utc(start)
        end_utc = _utc(end)
        if start_utc >= end_utc:
            return []
        # Kraken's OHLC endpoint returns at most the latest 720 candles; `since`
        # only trims the front of that window, so no pagination is possible.
        response = await self.client.get(
            "/0/public/OHLC",
            params={
                "pair": pair,
                "interval": max(1, interval_seconds // 60),
                "since": int(start_utc.timestamp()) - 1,
            },
        )
        response.raise_for_status()
        return _parse_kraken_ohlc_payload(
            response.json(),
            symbol=symbol,
            requested_pair=pair,
            interval_seconds=interval_seconds,
            start=start_utc,
            end=end_utc,
        )
