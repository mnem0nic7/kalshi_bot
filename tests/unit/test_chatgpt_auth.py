"""Unit tests for ChatGPTAuthManager (agents/chatgpt_auth.py)."""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.agents.chatgpt_auth import ChatGPTAuthManager, _jwt_exp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_jwt(exp: float) -> str:
    """Produce a minimal unsigned JWT with the given exp claim."""
    import base64
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload_bytes = json.dumps({"exp": exp}).encode()
    payload = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()
    return f"{header}.{payload}.signature"


def _write_auth_json(path: Path, *, access_token: str, refresh_token: str = "rt_test", account_id: str = "acct-123") -> None:
    data = {
        "auth_mode": "chatgpt",
        "tokens": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "account_id": account_id,
        },
        "last_refresh": "2026-04-23T00:00:00+00:00",
    }
    path.write_text(json.dumps(data), encoding="utf-8")


# ---------------------------------------------------------------------------
# _jwt_exp
# ---------------------------------------------------------------------------

def test_jwt_exp_extracts_timestamp() -> None:
    future = time.time() + 3600
    token = _make_jwt(future)
    assert abs(_jwt_exp(token) - future) < 1.0


def test_jwt_exp_raises_on_malformed() -> None:
    with pytest.raises(ValueError):
        _jwt_exp("not.a.jwt")


# ---------------------------------------------------------------------------
# get_account_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_account_id_returns_value(tmp_path: Path) -> None:
    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=_make_jwt(time.time() + 86401), account_id="acct-xyz")
    mgr = ChatGPTAuthManager(auth_path)
    assert await mgr.get_account_id() == "acct-xyz"


# ---------------------------------------------------------------------------
# get_access_token — valid (no refresh needed)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_access_token_returns_unexpired_token(tmp_path: Path) -> None:
    """Valid token (exp > now + buffer) → returned without calling _refresh."""
    future_exp = time.time() + 86401  # more than 1-day buffer
    token = _make_jwt(future_exp)
    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=token)

    mgr = ChatGPTAuthManager(auth_path)
    with patch.object(mgr, "_refresh", new_callable=AsyncMock) as mock_refresh:
        result = await mgr.get_access_token()

    assert result == token
    mock_refresh.assert_not_called()


# ---------------------------------------------------------------------------
# get_access_token — near-expiry triggers refresh
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_access_token_refreshes_near_expiry(tmp_path: Path) -> None:
    """Token expiring within the 1-day buffer → _refresh called, new token returned."""
    near_exp = time.time() + 3600  # only 1 hour left; below 86400 buffer
    stale_token = _make_jwt(near_exp)
    fresh_token = _make_jwt(time.time() + 864000)  # 10 days

    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=stale_token)

    mgr = ChatGPTAuthManager(auth_path)
    with patch.object(mgr, "_refresh", new_callable=AsyncMock, return_value=fresh_token) as mock_refresh:
        result = await mgr.get_access_token()

    assert result == fresh_token
    mock_refresh.assert_called_once()


# ---------------------------------------------------------------------------
# Concurrent callers — lock prevents double-refresh
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_callers_refresh_exactly_once(tmp_path: Path) -> None:
    """Multiple concurrent get_access_token() calls must not double-refresh."""
    near_exp = time.time() + 3600  # will trigger refresh
    stale_token = _make_jwt(near_exp)
    fresh_token = _make_jwt(time.time() + 864000)

    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=stale_token)

    refresh_call_count = 0

    async def _fake_refresh(data):
        nonlocal refresh_call_count
        refresh_call_count += 1
        await asyncio.sleep(0.01)  # simulate network latency
        # Update the file so subsequent loads see a fresh token
        data["tokens"]["access_token"] = fresh_token
        auth_path.write_text(json.dumps(data), encoding="utf-8")
        return fresh_token

    mgr = ChatGPTAuthManager(auth_path)
    with patch.object(mgr, "_refresh", side_effect=_fake_refresh):
        results = await asyncio.gather(*[mgr.get_access_token() for _ in range(5)])

    # The lock guarantees _refresh fires exactly once
    assert refresh_call_count == 1
    # All callers that go through the refresh path get the fresh token
    assert all(r == fresh_token for r in results)


# ---------------------------------------------------------------------------
# Refresh response with rotated refresh_token
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_refresh_stores_rotated_refresh_token(tmp_path: Path) -> None:
    """If the refresh response includes a new refresh_token, it is saved to auth.json."""
    near_exp = time.time() + 3600
    stale_token = _make_jwt(near_exp)
    fresh_token = _make_jwt(time.time() + 864000)
    new_rt = "rt_rotated_xyz"

    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=stale_token, refresh_token="rt_original")

    mgr = ChatGPTAuthManager(auth_path)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": fresh_token,
        "refresh_token": new_rt,
    }

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
        # Directly call _refresh to test the storage logic
        data = json.loads(auth_path.read_text())
        await mgr._refresh(data)

    saved = json.loads(auth_path.read_text())
    assert saved["tokens"]["refresh_token"] == new_rt
    assert saved["tokens"]["access_token"] == fresh_token


# ---------------------------------------------------------------------------
# Refresh HTTP 400 → RuntimeError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_refresh_400_raises_runtime_error(tmp_path: Path) -> None:
    """Refresh endpoint returning non-200 → RuntimeError with helpful message."""
    near_exp = time.time() + 3600
    stale_token = _make_jwt(near_exp)

    auth_path = tmp_path / "auth.json"
    _write_auth_json(auth_path, access_token=stale_token)

    mgr = ChatGPTAuthManager(auth_path)

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {"error": "invalid_grant"}

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
        data = json.loads(auth_path.read_text())
        with pytest.raises(RuntimeError, match="codex login"):
            await mgr._refresh(data)
