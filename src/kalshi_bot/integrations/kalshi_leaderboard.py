from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import html
import io
import json
import re
from typing import Any
from urllib.parse import urljoin

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.integrations.kalshi import KalshiClient


LEADERBOARD_NAMES = ("volume", "projected_pnl", "num_markets_traded")
LEADERBOARD_TIME_WINDOWS = ("daily", "weekly", "monthly", "yearly", "all_time")
LEADERBOARD_SOURCES = ("web", "direct", "auto")
LEADERBOARD_CATEGORIES = (
    "",
    "Politics",
    "Sports",
    "Entertainment",
    "Crypto",
    "Climate and Weather",
    "Economics",
    "Mentions",
    "Companies",
    "Financials",
    "Science and Technology",
    "Elections",
)

_CATEGORY_LOOKUP = {item.casefold(): item for item in LEADERBOARD_CATEGORIES}
_METRIC_KEYS = {
    "volume": ("volume", "total_volume", "totalVolume", "value", "score"),
    "projected_pnl": ("projected_pnl", "projectedPnl", "pnl", "profit", "value", "score"),
    "num_markets_traded": (
        "num_markets_traded",
        "numMarketsTraded",
        "markets_traded",
        "marketsTraded",
        "value",
        "score",
    ),
}


@dataclass(slots=True)
class KalshiLeaderboardEntry:
    rank: int | None
    username: str | None
    display_name: str | None
    member_id: str | None
    value: Any
    profile_image_url: str | None
    is_anonymous: bool | None
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "username": self.username,
            "display_name": self.display_name,
            "member_id": self.member_id,
            "value": self.value,
            "profile_image_url": self.profile_image_url,
            "is_anonymous": self.is_anonymous,
            "raw": self.raw,
        }


@dataclass(slots=True)
class KalshiLeaderboardSnapshot:
    source: str
    name: str
    time_window: str
    category: str
    source_url: str
    fetched_at: datetime
    entries: list[KalshiLeaderboardEntry]
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "name": self.name,
            "time": self.time_window,
            "category": self.category,
            "source_url": self.source_url,
            "fetched_at": self.fetched_at.isoformat(),
            "entries": [entry.to_dict() for entry in self.entries],
            "raw": self.raw,
        }


def normalize_leaderboard_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in LEADERBOARD_NAMES:
        raise ValueError(f"leaderboard name must be one of: {', '.join(LEADERBOARD_NAMES)}")
    return normalized


def normalize_leaderboard_time_window(value: str) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if normalized not in LEADERBOARD_TIME_WINDOWS:
        raise ValueError(f"leaderboard time must be one of: {', '.join(LEADERBOARD_TIME_WINDOWS)}")
    return normalized


def normalize_leaderboard_category(value: str | None) -> str:
    if value is None:
        return ""
    normalized = " ".join(str(value).replace("+", " ").split())
    if normalized == "":
        return ""
    canonical = _CATEGORY_LOOKUP.get(normalized.casefold())
    if canonical is None:
        choices = ", ".join(item or "(all)" for item in LEADERBOARD_CATEGORIES)
        raise ValueError(f"leaderboard category must be one of: {choices}")
    return canonical


class KalshiLeaderboardScraper:
    """Read-only first-party scraper for Kalshi's social leaderboard."""

    def __init__(
        self,
        settings: Settings,
        kalshi: KalshiClient,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self.kalshi = kalshi
        self.http_client = http_client or httpx.AsyncClient(
            timeout=settings.kalshi_leaderboard_timeout_seconds
        )
        self._owns_http_client = http_client is None

    async def close(self) -> None:
        if self._owns_http_client:
            await self.http_client.aclose()

    async def fetch(
        self,
        *,
        name: str = "projected_pnl",
        time_window: str = "daily",
        category: str | None = "",
        limit: int | None = None,
        source: str = "direct",
        require_auth: bool = True,
    ) -> KalshiLeaderboardSnapshot:
        name = normalize_leaderboard_name(name)
        time_window = normalize_leaderboard_time_window(time_window)
        category = normalize_leaderboard_category(category)
        source = _normalize_source(source)
        if source == "web":
            try:
                return await self._fetch_web(
                    name=name,
                    time_window=time_window,
                    category=category,
                    limit=limit,
                )
            except RuntimeError:
                raise
        if source == "auto":
            try:
                return await self._fetch_direct(
                    name=name,
                    time_window=time_window,
                    category=category,
                    limit=limit,
                    require_auth=require_auth,
                )
            except RuntimeError:
                return await self._fetch_web(
                    name=name,
                    time_window=time_window,
                    category=category,
                    limit=limit,
                )
        return await self._fetch_direct(
            name=name,
            time_window=time_window,
            category=category,
            limit=limit,
            require_auth=require_auth,
        )

    async def _fetch_web(
        self,
        *,
        name: str,
        time_window: str,
        category: str,
        limit: int | None,
    ) -> KalshiLeaderboardSnapshot:
        try:
            return await self._fetch_direct(
                name=name,
                time_window=time_window,
                category=category,
                limit=limit,
                require_auth=True,
                source="web",
            )
        except RuntimeError:
            pass

        params: dict[str, Any] = {"name": name, "time": time_window}
        if category:
            params["category"] = category
        response = await self.http_client.get(
            self.settings.kalshi_leaderboard_web_url,
            params=params,
            headers=self._web_headers(),
        )
        if response.status_code in {401, 403}:
            raise RuntimeError(
                "Kalshi leaderboard page rejected the request; set a logged-in "
                "KALSHI_LEADERBOARD_COOKIE or KALSHI_LEADERBOARD_AUTHORIZATION value."
            )
        if response.is_error:
            raise RuntimeError(
                f"Kalshi leaderboard page request failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        rows = _extract_rows_from_html(response.text)
        if not rows:
            credential_hint = (
                ""
                if self._has_web_credentials()
                else "; set KALSHI_LEADERBOARD_COOKIE if the page requires a logged-in session"
            )
            raise RuntimeError(
                f"Kalshi leaderboard page did not contain parseable leaderboard rows{credential_hint}"
            )
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return KalshiLeaderboardSnapshot(
            source="web",
            name=name,
            time_window=time_window,
            category=category,
            source_url=str(response.url),
            fetched_at=datetime.now(UTC),
            entries=[_entry_from_row(row, index=index, metric=name) for index, row in enumerate(rows, start=1)],
            raw={"rows": rows},
        )

    async def _fetch_direct(
        self,
        *,
        name: str,
        time_window: str,
        category: str,
        limit: int | None,
        require_auth: bool,
        source: str = "direct",
    ) -> KalshiLeaderboardSnapshot:
        path = self.settings.kalshi_leaderboard_path
        params: dict[str, Any] = {"metric_name": name, "time_period": time_window}
        if category:
            params["category"] = category
        if limit is not None and limit > 0:
            params["limit"] = limit
        if require_auth and not self._has_web_credentials():
            raise RuntimeError(
                "Missing Kalshi leaderboard web credentials; set KALSHI_LEADERBOARD_COOKIE "
                "and KALSHI_LEADERBOARD_CSRF_TOKEN"
            )

        response = await self.http_client.get(
            _join_url(self.settings.kalshi_leaderboard_base_url, path),
            params=params,
            headers=self._api_headers(),
        )
        if response.status_code in {401, 403}:
            raise RuntimeError(
                "Kalshi leaderboard request was unauthorized; check KALSHI_LEADERBOARD_COOKIE "
                "and KALSHI_LEADERBOARD_CSRF_TOKEN"
            )
        if response.is_error:
            raise RuntimeError(
                f"Kalshi leaderboard request failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Kalshi leaderboard request did not return JSON: {response.text[:500]}"
            ) from exc
        raw = payload if isinstance(payload, dict) else {"data": payload}
        rows = _extract_rows(raw)
        if limit is not None and limit > 0:
            rows = rows[:limit]
        return KalshiLeaderboardSnapshot(
            source=source,
            name=name,
            time_window=time_window,
            category=category,
            source_url=str(response.url),
            fetched_at=datetime.now(UTC),
            entries=[_entry_from_row(row, index=index, metric=name) for index, row in enumerate(rows, start=1)],
            raw=raw,
        )

    def _api_headers(self) -> dict[str, str]:
        headers = self._credential_headers()
        headers.update(
            {
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://kalshi.com",
                "Referer": self.settings.kalshi_leaderboard_web_url,
                "User-Agent": self.settings.kalshi_leaderboard_user_agent,
            }
        )
        return headers

    def _web_headers(self) -> dict[str, str]:
        headers = self._credential_headers()
        headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": self.settings.kalshi_leaderboard_user_agent,
            }
        )
        return headers

    def _credential_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.settings.kalshi_leaderboard_cookie:
            headers["Cookie"] = self.settings.kalshi_leaderboard_cookie
        if self.settings.kalshi_leaderboard_authorization:
            headers["Authorization"] = self.settings.kalshi_leaderboard_authorization
        if self.settings.kalshi_leaderboard_csrf_token:
            headers["X-CSRF-Token"] = self.settings.kalshi_leaderboard_csrf_token
        return headers

    def _has_web_credentials(self) -> bool:
        return bool(
            self.settings.kalshi_leaderboard_cookie
            or self.settings.kalshi_leaderboard_authorization
            or self.settings.kalshi_leaderboard_csrf_token
        )


def leaderboard_snapshot_to_csv(snapshot: KalshiLeaderboardSnapshot) -> str:
    output = io.StringIO()
    columns = [
        "rank",
        "username",
        "display_name",
        "member_id",
        "value",
        "profile_image_url",
        "is_anonymous",
        "name",
        "time",
        "category",
        "raw_json",
    ]
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for entry in snapshot.entries:
        writer.writerow(
            {
                "rank": entry.rank,
                "username": entry.username,
                "display_name": entry.display_name,
                "member_id": entry.member_id,
                "value": entry.value,
                "profile_image_url": entry.profile_image_url,
                "is_anonymous": entry.is_anonymous,
                "name": snapshot.name,
                "time": snapshot.time_window,
                "category": snapshot.category,
                "raw_json": json.dumps(entry.raw, sort_keys=True, ensure_ascii=True),
            }
        )
    return output.getvalue()


def _join_url(base_url: str, path: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _extract_rows_from_html(document: str) -> list[dict[str, Any]]:
    for payload in _json_payloads_from_html(document):
        rows = _extract_rows(payload)
        if rows:
            return rows
    return _extract_rows_from_text(document)


def _json_payloads_from_html(document: str) -> list[Any]:
    payloads: list[Any] = []
    script_pattern = re.compile(r"<script\b[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
    for match in script_pattern.finditer(document):
        script = html.unescape(match.group(1)).strip()
        if not script:
            continue
        for candidate in _json_candidates(script):
            try:
                payloads.append(json.loads(candidate))
            except json.JSONDecodeError:
                continue
    return payloads


def _json_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    stripped = text.strip()
    if stripped.startswith(("{", "[")):
        candidates.append(stripped)
    next_data_match = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, re.DOTALL)
    if next_data_match:
        candidates.append(html.unescape(next_data_match.group(1)).strip())
    for pattern in (
        r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
        r"window\.__APOLLO_STATE__\s*=\s*({.*?});",
        r"self\.__next_f\.push\(\s*(\[.*?\])\s*\)",
    ):
        for match in re.finditer(pattern, text, re.DOTALL):
            candidates.append(match.group(1).strip())
    return candidates


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, str):
        text = html.unescape(payload).strip()
        if "leaderboard" not in text.lower():
            return []
        for candidate in _json_candidates(text):
            try:
                rows = _extract_rows(json.loads(candidate))
            except json.JSONDecodeError:
                continue
            if rows:
                return rows
        return []
    if isinstance(payload, list):
        rows = [item for item in payload if isinstance(item, dict) and _looks_like_leaderboard_row(item)]
        if rows:
            return rows
        for item in payload:
            rows = _extract_rows(item)
            if rows:
                return rows
        return []
    if not isinstance(payload, dict):
        return []
    for key in ("rank_list", "leaderboard", "rankings", "entries", "results", "items", "rows", "users"):
        rows = _extract_rows(payload.get(key))
        if rows:
            return rows
    for value in payload.values():
        rows = _extract_rows(value)
        if rows:
            return rows
    return []


def _extract_rows_from_text(document: str) -> list[dict[str, Any]]:
    decoded = html.unescape(document)
    if "leaderboard" not in decoded.lower():
        return []
    rows: list[dict[str, Any]] = []
    row_pattern = re.compile(
        r"(?P<rank>\d{1,4})\s+(?P<name>[A-Za-z0-9_.@-]{2,64})\s+(?P<value>\$?-?[0-9][0-9,]*(?:\.[0-9]+)?[KMB]?)"
    )
    for match in row_pattern.finditer(decoded):
        rows.append(
            {
                "rank": int(match.group("rank")),
                "username": match.group("name"),
                "value": match.group("value"),
            }
        )
    return rows


def _looks_like_leaderboard_row(row: dict[str, Any]) -> bool:
    return any(key in row for key in ("rank", "position", "place", "ranking")) and any(
        key in row
        for key in (
            "username",
            "user_name",
            "userName",
            "handle",
            "nickname",
            "display_name",
            "displayName",
            "name",
        )
    )


def _entry_from_row(row: dict[str, Any], *, index: int, metric: str) -> KalshiLeaderboardEntry:
    return KalshiLeaderboardEntry(
        rank=_int_or_none(_first_present(row, "rank", "position", "place", "ranking")) or index,
        username=_str_or_none(_first_present(row, "username", "user_name", "userName", "handle", "nickname")),
        display_name=_str_or_none(_first_present(row, "display_name", "displayName", "name", "nickname")),
        member_id=_str_or_none(_first_present(row, "member_id", "memberId", "social_id", "socialId", "user_id", "userId", "id")),
        value=_first_present(row, *_METRIC_KEYS.get(metric, ("value", "score"))),
        profile_image_url=_str_or_none(
            _first_present(row, "profile_image_url", "profileImageUrl", "profile_image_path", "profileImagePath")
        ),
        is_anonymous=_bool_or_none(_first_present(row, "is_anonymous", "isAnonymous", "anonymous")),
        raw=dict(row),
    )


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _str_or_none(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _normalize_source(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in LEADERBOARD_SOURCES:
        raise ValueError("leaderboard source must be one of: web, direct, auto")
    return normalized
