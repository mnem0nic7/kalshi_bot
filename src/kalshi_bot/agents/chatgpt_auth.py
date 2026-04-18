from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_AUTH_URL = "https://auth.openai.com/oauth/token"
_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
_REFRESH_BUFFER_SECONDS = 86400  # refresh when < 1 day remaining on access_token


def _jwt_exp(token: str) -> float:
    """Return the exp Unix timestamp from a JWT without verifying the signature."""
    try:
        payload_b64 = token.split(".")[1]
        pad = "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + pad))
        return float(payload["exp"])
    except Exception as exc:
        raise ValueError(f"Cannot decode JWT exp claim: {exc}") from exc


class ChatGPTAuthManager:
    """Reads and refreshes ChatGPT OAuth tokens stored in ~/.codex/auth.json."""

    def __init__(self, auth_json_path: Path) -> None:
        self._path = auth_json_path
        self._lock = asyncio.Lock()

    def _load(self) -> dict[str, Any]:
        return json.loads(self._path.read_text(encoding="utf-8"))

    def _save(self, data: dict[str, Any]) -> None:
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        self._path.chmod(0o600)

    async def get_account_id(self) -> str:
        return self._load()["tokens"]["account_id"]

    async def get_access_token(self) -> str:
        async with self._lock:
            data = self._load()
            token = data["tokens"]["access_token"]
            try:
                exp = _jwt_exp(token)
                if time.time() + _REFRESH_BUFFER_SECONDS < exp:
                    return token
            except ValueError:
                pass
            logger.info("ChatGPT access_token near expiry — refreshing")
            return await self._refresh(data)

    async def _refresh(self, data: dict[str, Any]) -> str:
        refresh_token = data["tokens"]["refresh_token"]
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                _AUTH_URL,
                json={
                    "client_id": _CLIENT_ID,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                headers={"Content-Type": "application/json"},
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ChatGPT token refresh failed ({resp.status_code}). "
                "Run `codex login` to re-authenticate."
            )
        body = resp.json()
        new_access = body.get("access_token")
        if not new_access:
            raise RuntimeError("Token refresh response missing access_token. Run `codex login`.")
        data["tokens"]["access_token"] = new_access
        if body.get("id_token"):
            data["tokens"]["id_token"] = body["id_token"]
        if body.get("refresh_token"):
            data["tokens"]["refresh_token"] = body["refresh_token"]
        from datetime import datetime, timezone
        data["last_refresh"] = datetime.now(timezone.utc).isoformat()
        self._save(data)
        logger.info("ChatGPT access_token refreshed successfully")
        return new_access
