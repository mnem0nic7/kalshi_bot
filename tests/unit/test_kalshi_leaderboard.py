from __future__ import annotations

import httpx
import pytest

from kalshi_bot.config import Settings
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.kalshi_leaderboard import (
    KalshiLeaderboardScraper,
    _extract_rows_from_html,
    leaderboard_snapshot_to_csv,
    normalize_leaderboard_category,
)


def _settings() -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        kalshi_env="production",
        kalshi_read_api_key_id=None,
        kalshi_read_private_key_path=None,
        kalshi_write_api_key_id=None,
        kalshi_write_private_key_path=None,
        demo_kalshi_api_key=None,
        live_kalshi_api_key=None,
    )


@pytest.mark.asyncio
async def test_leaderboard_scraper_fetches_authenticated_json() -> None:
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        seen["cookie"] = request.headers.get("Cookie")
        seen["csrf"] = request.headers.get("X-CSRF-Token")
        seen["user_agent"] = request.headers.get("User-Agent")
        return httpx.Response(
            200,
            json={
                "rank_list": [
                    {
                        "rank": 1,
                        "nickname": "top_trader",
                        "social_id": "user-1",
                        "value": "$123.45",
                        "is_anonymous": False,
                    }
                ]
            },
        )

    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        kalshi_env="production",
        kalshi_read_api_key_id=None,
        kalshi_read_private_key_path=None,
        kalshi_write_api_key_id=None,
        kalshi_write_private_key_path=None,
        demo_kalshi_api_key=None,
        live_kalshi_api_key=None,
        kalshi_leaderboard_cookie="sessionid=secret",
        kalshi_leaderboard_csrf_token="csrf-token",
    )
    kalshi = KalshiClient(settings)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    scraper = KalshiLeaderboardScraper(settings, kalshi, http_client=client)

    snapshot = await scraper.fetch(
        name="projected_pnl",
        time_window="daily",
        category="Climate+and+Weather",
        limit=10,
        source="direct",
    )

    assert seen["path"] == "/v1/social/leaderboard"
    assert seen["params"] == {
        "metric_name": "projected_pnl",
        "time_period": "daily",
        "category": "Climate and Weather",
        "limit": "10",
    }
    assert seen["cookie"] == "sessionid=secret"
    assert seen["csrf"] == "csrf-token"
    assert seen["user_agent"] == settings.kalshi_leaderboard_user_agent
    assert snapshot.category == "Climate and Weather"
    assert len(snapshot.entries) == 1
    assert snapshot.entries[0].rank == 1
    assert snapshot.entries[0].username == "top_trader"
    assert snapshot.entries[0].display_name == "top_trader"
    assert snapshot.entries[0].member_id == "user-1"
    assert snapshot.entries[0].value == "$123.45"
    assert snapshot.entries[0].is_anonymous is False

    await client.aclose()
    await kalshi.close()


@pytest.mark.asyncio
async def test_leaderboard_scraper_requires_web_credentials_by_default() -> None:
    settings = _settings()
    kalshi = KalshiClient(settings)
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    scraper = KalshiLeaderboardScraper(settings, kalshi, http_client=client)

    with pytest.raises(RuntimeError, match="Missing Kalshi leaderboard web credentials"):
        await scraper.fetch(source="direct")

    await client.aclose()
    await kalshi.close()


def test_leaderboard_category_normalizes_plus_encoded_values() -> None:
    assert normalize_leaderboard_category("Climate+and+Weather") == "Climate and Weather"
    assert normalize_leaderboard_category(" science   and technology ") == "Science and Technology"


@pytest.mark.asyncio
async def test_leaderboard_scraper_reads_first_party_web_page_state() -> None:
    seen: dict[str, object] = {}
    html_payload = """
    <html>
      <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"leaderboard":[
          {"rank":1,"nickname":"alpha","member_id":"m1","projectedPnl":"$99.00","isAnonymous":false}
        ]}}}
      </script>
    </html>
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/social/leaderboard":
            return httpx.Response(404, json={"error": "not mocked"})
        seen["url"] = str(request.url)
        seen["accept"] = request.headers.get("Accept")
        seen["cookie"] = request.headers.get("Cookie")
        seen["authorization"] = request.headers.get("Authorization")
        seen["csrf"] = request.headers.get("X-CSRF-Token")
        return httpx.Response(200, text=html_payload)

    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        kalshi_env="production",
        kalshi_read_api_key_id=None,
        kalshi_read_private_key_path=None,
        kalshi_write_api_key_id=None,
        kalshi_write_private_key_path=None,
        demo_kalshi_api_key=None,
        live_kalshi_api_key=None,
        kalshi_leaderboard_cookie="sessionid=secret",
        kalshi_leaderboard_authorization="Bearer web-token",
        kalshi_leaderboard_csrf_token="csrf-token",
    )
    kalshi = KalshiClient(settings)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    scraper = KalshiLeaderboardScraper(settings, kalshi, http_client=client)

    snapshot = await scraper.fetch(name="projected_pnl", time_window="daily", limit=5, source="web")

    assert settings.kalshi_leaderboard_web_url in seen["url"]
    assert "text/html" in str(seen["accept"])
    assert seen["cookie"] == "sessionid=secret"
    assert seen["authorization"] == "Bearer web-token"
    assert seen["csrf"] == "csrf-token"
    assert snapshot.source == "web"
    assert snapshot.entries[0].username == "alpha"
    assert snapshot.entries[0].value == "$99.00"
    assert snapshot.entries[0].is_anonymous is False

    await client.aclose()
    await kalshi.close()


@pytest.mark.asyncio
async def test_leaderboard_web_auth_error_mentions_session_cookie() -> None:
    settings = _settings()
    kalshi = KalshiClient(settings)
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(403, text="blocked")))
    scraper = KalshiLeaderboardScraper(settings, kalshi, http_client=client)

    with pytest.raises(RuntimeError, match="KALSHI_LEADERBOARD_COOKIE"):
        await scraper.fetch(source="web")

    await client.aclose()
    await kalshi.close()


def test_leaderboard_html_parser_reads_embedded_json_rows() -> None:
    rows = _extract_rows_from_html(
        """
        <script type="application/json">
          {"leaderboard":[{"rank":2,"username":"beta","volume":"1200"}]}
        </script>
        """
    )

    assert rows == [{"rank": 2, "username": "beta", "volume": "1200"}]


def test_leaderboard_snapshot_csv_contains_normalized_rows() -> None:
    from datetime import UTC, datetime

    from kalshi_bot.integrations.kalshi_leaderboard import (
        KalshiLeaderboardEntry,
        KalshiLeaderboardSnapshot,
    )

    snapshot = KalshiLeaderboardSnapshot(
        source="web",
        name="volume",
        time_window="weekly",
        category="Crypto",
        source_url="https://example.test/leaderboard",
        fetched_at=datetime(2026, 5, 11, tzinfo=UTC),
        entries=[
            KalshiLeaderboardEntry(
                rank=2,
                username="vol_trader",
                display_name=None,
                member_id="user-2",
                value="1000",
                profile_image_url=None,
                is_anonymous=None,
                raw={"rank": 2, "username": "vol_trader", "volume": "1000"},
            )
        ],
        raw={"leaderboard": []},
    )

    csv_payload = leaderboard_snapshot_to_csv(snapshot)

    assert "rank,username,display_name,member_id,value,profile_image_url,is_anonymous,name,time,category,raw_json" in csv_payload
    assert "vol_trader" in csv_payload
    assert "Crypto" in csv_payload
