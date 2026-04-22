from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import json
import os
from pathlib import Path
from time import time
from typing import Any
from urllib.parse import urlsplit

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import logging

from kalshi_bot.config import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class KalshiCredentials:
    key_id: str
    private_key_path: Path


class KalshiSigner:
    def __init__(self, private_key_path: Path) -> None:
        mode = os.stat(private_key_path).st_mode & 0o777
        if mode & 0o022:
            raise PermissionError(
                f"RSA private key {private_key_path} has unsafe permissions ({oct(mode)}). "
                "Run: chmod 600 <key_path>"
            )
        key_bytes = private_key_path.read_bytes()
        self.private_key = serialization.load_pem_private_key(key_bytes, password=None)

    def sign(self, message: str) -> str:
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")


class KalshiClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.AsyncClient(timeout=30.0, headers={"Content-Type": "application/json"})
        self.base_path = urlsplit(settings.kalshi_rest_base_url).path.rstrip("/")
        self.read_credentials = self._load_credentials(write=False)
        self.write_credentials = self._load_credentials(write=True)
        self._signers: dict[tuple[str, bool], KalshiSigner] = {}

    def _load_credentials(self, *, write: bool) -> KalshiCredentials | None:
        key_id = self.settings.api_key_id(write=write)
        key_path = self.settings.key_path(write=write)
        if not key_id or key_path is None:
            return None
        return KalshiCredentials(key_id=key_id, private_key_path=key_path)

    def _get_signer(self, credentials: KalshiCredentials, *, write: bool) -> KalshiSigner:
        cache_key = (credentials.key_id, write)
        signer = self._signers.get(cache_key)
        if signer is None:
            signer = KalshiSigner(credentials.private_key_path)
            self._signers[cache_key] = signer
        return signer

    def _auth_headers(self, method: str, path: str, *, write: bool) -> dict[str, str]:
        credentials = self.write_credentials if write else self.read_credentials
        if credentials is None:
            raise RuntimeError(f"Missing {'write' if write else 'read'} Kalshi credentials")
        timestamp = str(int(time() * 1000))
        signing_path = f"{self.base_path}{path.split('?')[0]}"
        signature = self._get_signer(credentials, write=write).sign(f"{timestamp}{method.upper()}{signing_path}")
        return {
            "KALSHI-ACCESS-KEY": credentials.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

    def websocket_auth_headers(self) -> dict[str, str]:
        credentials = self.read_credentials or self.write_credentials
        if credentials is None:
            raise RuntimeError("Missing Kalshi credentials for websocket connection")
        timestamp = str(int(time() * 1000))
        signature = self._get_signer(credentials, write=False).sign(f"{timestamp}GET/trade-api/ws/v2")
        return {
            "KALSHI-ACCESS-KEY": credentials.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }

    async def close(self) -> None:
        await self.client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        write: bool = False,
    ) -> dict[str, Any]:
        filtered_params = (
            {
                key: value
                for key, value in params.items()
                if value is not None and value != ""
            }
            if params is not None
            else None
        )
        max_retries = 3
        backoff = 5.0
        for attempt in range(max_retries + 1):
            headers = self._auth_headers(method, path, write=write)
            response = await self.client.request(
                method=method,
                url=f"{self.settings.kalshi_rest_base_url}{path}",
                params=filtered_params,
                json=json,
                headers=headers,
            )
            if response.status_code != 429 or attempt == max_retries:
                if response.is_error:
                    try:
                        body = response.json()
                    except Exception:
                        body = response.text
                    logger.warning("Kalshi %s %s → %d: %s", method, path, response.status_code, body)
                response.raise_for_status()
                return response.json()
            retry_after = response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else backoff
            backoff = min(backoff * 2, 60.0)
            await asyncio.sleep(wait)
        # unreachable — loop always returns or raises
        response.raise_for_status()
        return response.json()

    async def get_market(self, ticker: str) -> dict[str, Any]:
        return await self._request("GET", f"/markets/{ticker}")

    async def list_markets(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/markets", params=params)

    async def list_historical_markets(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/historical/markets", params=params)

    async def get_market_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        **params: Any,
    ) -> dict[str, Any]:
        return await self._request("GET", f"/series/{series_ticker}/markets/{market_ticker}/candlesticks", params=params)

    async def get_balance(self) -> dict[str, Any]:
        return await self._request("GET", "/portfolio/balance")

    async def get_positions(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/portfolio/positions", params=params)

    async def get_orders(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/portfolio/orders", params=params)

    async def get_fills(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/portfolio/fills", params=params)

    async def get_settlements(self, **params: Any) -> dict[str, Any]:
        return await self._request("GET", "/portfolio/settlements", params=params)

    async def get_historical_cutoff(self) -> dict[str, Any]:
        return await self._request("GET", "/historical/cutoff")

    async def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/portfolio/orders", json=payload, write=True)

    async def get_order(self, order_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/portfolio/orders/{order_id}")

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/portfolio/orders/{order_id}", write=True)


class KalshiWebSocketClient:
    def __init__(self, settings: Settings, kalshi: KalshiClient) -> None:
        self.settings = settings
        self.kalshi = kalshi
        self.websocket = None
        self.message_id = 1

    async def connect(self) -> None:
        self.websocket = await websockets.connect(
            self.settings.kalshi_websocket_url,
            additional_headers=self.kalshi.websocket_auth_headers(),
            ping_interval=20,
            ping_timeout=60,
            close_timeout=10,
            max_queue=1024,
        )

    async def close(self) -> None:
        if self.websocket is not None:
            await self.websocket.close()
            self.websocket = None

    async def subscribe(self, channels: list[str], *, market_tickers: list[str] | None = None) -> None:
        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected")
        params: dict[str, Any] = {"channels": channels}
        if market_tickers:
            if len(market_tickers) == 1:
                params["market_ticker"] = market_tickers[0]
            else:
                params["market_tickers"] = market_tickers
        payload = {"id": self.message_id, "cmd": "subscribe", "params": params}
        self.message_id += 1
        await self.websocket.send(json.dumps(payload))

    async def iter_messages(self):
        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected")
        async for raw in self.websocket:
            yield json.loads(raw)
