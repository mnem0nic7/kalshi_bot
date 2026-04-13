from datetime import UTC, datetime
from types import SimpleNamespace

from kalshi_bot.web.control_room import _recent_room_outcomes, _series_filter_options


def test_recent_room_outcomes_excludes_running_rooms_from_resolved_total() -> None:
    now = datetime(2026, 4, 13, 16, 0, tzinfo=UTC)
    room_views = [
        {"status": "running", "updated_at": "2026-04-13T15:55:00+00:00"},
        {"status": "running", "updated_at": "2026-04-13T15:54:00+00:00"},
        {"status": "blocked", "updated_at": "2026-04-13T15:53:00+00:00"},
        {"status": "stand_down", "updated_at": "2026-04-13T15:52:00+00:00"},
        {"status": "failed", "updated_at": "2026-04-13T15:51:00+00:00"},
        {"status": "succeeded", "updated_at": "2026-04-13T15:50:00+00:00"},
    ]

    outcomes = _recent_room_outcomes(room_views, now=now)

    assert outcomes["total"] == 6
    assert outcomes["running"] == 2
    assert outcomes["resolved_total"] == 4
    assert outcomes["success_rate"] == 0.25


def test_series_filter_options_follow_configured_templates() -> None:
    templates = [
        SimpleNamespace(series_ticker="KXHIGHAUS", location_name="Austin", display_name="Austin Daily High Temperature"),
        SimpleNamespace(series_ticker="KXHIGHNY", location_name="New York City", display_name="NYC Daily High Temperature"),
    ]

    options = _series_filter_options(
        [
            {"series_ticker": "KXHIGHNY", "label": "Will the high temp in NYC be >68 on Apr 11, 2026?"},
            {"series_ticker": "KXHIGHAUS", "label": "Will the high temp in Austin be >88 on Apr 11, 2026?"},
        ],
        templates=templates,
    )

    assert options == [
        {"id": "all", "label": "All Series"},
        {"id": "KXHIGHAUS", "label": "Austin"},
        {"id": "KXHIGHNY", "label": "New York City"},
    ]
