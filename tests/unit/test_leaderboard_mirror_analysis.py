from __future__ import annotations

from datetime import UTC, datetime

from kalshi_bot.integrations.kalshi_leaderboard import (
    KalshiLeaderboardEntry,
    KalshiLeaderboardSnapshot,
)
from kalshi_bot.services.leaderboard_mirror_analysis import (
    ActiveMarketExposure,
    build_leaderboard_mirror_report,
    infer_leaderboard_category_for_market,
    leaderboard_mirror_report_to_csv,
)


def _snapshot(
    *,
    metric: str,
    time_window: str,
    category: str,
    entries: list[KalshiLeaderboardEntry],
) -> KalshiLeaderboardSnapshot:
    return KalshiLeaderboardSnapshot(
        source="direct",
        name=metric,
        time_window=time_window,
        category=category,
        source_url=f"https://example.test/{metric}/{time_window}/{category}",
        fetched_at=datetime(2026, 5, 11, tzinfo=UTC),
        entries=entries,
        raw={"rank_list": [entry.raw for entry in entries]},
    )


def _entry(username: str, *, rank: int, value: object, member_id: str | None = None) -> KalshiLeaderboardEntry:
    return KalshiLeaderboardEntry(
        rank=rank,
        username=username,
        display_name=username,
        member_id=member_id,
        value=value,
        profile_image_url=None,
        is_anonymous=False,
        raw={"rank": rank, "nickname": username, "value": value},
    )


def test_infer_leaderboard_category_for_market_tickers() -> None:
    assert infer_leaderboard_category_for_market("KXBTC15M-TEST") == "Crypto"
    assert infer_leaderboard_category_for_market("KXHIGHNY-26APR24-T67") == "Climate and Weather"
    assert infer_leaderboard_category_for_market("KXELECTION-TEST") is None


def test_leaderboard_report_scores_consistent_category_leaders() -> None:
    active_markets = [
        ActiveMarketExposure(
            market_ticker="KXHIGHNY-26APR24-T67",
            category="Climate and Weather",
            sources=["position"],
        )
    ]
    snapshots = [
        _snapshot(
            metric="projected_pnl",
            time_window="daily",
            category="Climate and Weather",
            entries=[
                _entry("consistent", rank=1, value="$125.50", member_id="social-1"),
                _entry("daily_only", rank=2, value="$90.00", member_id="social-2"),
                KalshiLeaderboardEntry(
                    rank=3,
                    username="hidden",
                    display_name="hidden",
                    member_id=None,
                    value="$50.00",
                    profile_image_url=None,
                    is_anonymous=True,
                    raw={"rank": 3, "value": "$50.00"},
                ),
            ],
        ),
        _snapshot(
            metric="projected_pnl",
            time_window="monthly",
            category="Climate and Weather",
            entries=[_entry("consistent", rank=4, value="$825.00", member_id="social-1")],
        ),
        _snapshot(
            metric="num_markets_traded",
            time_window="weekly",
            category="Climate and Weather",
            entries=[_entry("consistent", rank=3, value=18, member_id="social-1")],
        ),
        _snapshot(
            metric="volume",
            time_window="weekly",
            category="Climate and Weather",
            entries=[_entry("consistent", rank=5, value="$5000", member_id="social-1")],
        ),
    ]

    report = build_leaderboard_mirror_report(
        kalshi_env="production",
        active_markets=active_markets,
        snapshots=snapshots,
        top=10,
    )

    assert report.skipped_anonymous_entries == 1
    assert report.candidates[0].username == "consistent"
    assert report.candidates[0].recommendation == "shadow_mirror_candidate"
    assert report.candidates[0].score > report.candidates[1].score
    assert "Climate and Weather" in report.candidates[0].matched_categories
    assert any("aggregate by category" in caution for caution in report.candidates[0].cautions)


def test_leaderboard_report_csv_contains_candidate_summary() -> None:
    report = build_leaderboard_mirror_report(
        kalshi_env="demo",
        active_markets=[
            ActiveMarketExposure(market_ticker="KXBTC15M-TEST", category="Crypto", sources=["cli"])
        ],
        snapshots=[
            _snapshot(
                metric="projected_pnl",
                time_window="all_time",
                category="Crypto",
                entries=[_entry("crypto_winner", rank=1, value="$1000", member_id="social-3")],
            )
        ],
        top=5,
    )

    csv_payload = leaderboard_mirror_report_to_csv(report)

    assert "recommendation,score,username" in csv_payload
    assert "crypto_winner" in csv_payload
    assert "Crypto" in csv_payload
