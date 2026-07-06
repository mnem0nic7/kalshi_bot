"""Parser tests for the Cleveland Fed nowcast frame archive.

Fixture (`tests/fixtures/macro/nowcast_month_fixture.json`) is a TRIMMED excerpt (3 of ~157
frames) captured from the real `nowcast_month.json` endpoint on 2026-07-06: the frame for
target month 2024-06 (has both a nowcast run-up and a released "Actual" value, plus the marker
labels `CPI May` / `PCE May` / `CPI Jun`), the frame for 2013-12 (crosses the Dec->Jan calendar
boundary), and the frame for 2026-07 (current month at capture time — no actual yet, the
data-gap case).
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from kalshi_bot.macro.nowcast_source import NowcastReading, latest_as_of, parse_frames

FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "macro" / "nowcast_month_fixture.json"


@pytest.fixture()
def fixture_frames() -> list[dict]:
    return json.loads(FIXTURE_PATH.read_text())


def test_parses_known_nonempty_values(fixture_frames: list[dict]) -> None:
    readings = parse_frames(fixture_frames, "mom")
    assert readings, "expected at least one reading from the fixture"
    # every reading is tagged with the index_kind the caller passed in
    assert all(r.index_kind == "mom" for r in readings)


def test_tags_index_kind_from_caller_not_content(fixture_frames: list[dict]) -> None:
    # the frame shape is identical for MoM/YoY; the caller's index_kind is what distinguishes them
    mom_readings = parse_frames(fixture_frames, "mom")
    yoy_readings = parse_frames(fixture_frames, "yoy")
    assert all(r.index_kind == "yoy" for r in yoy_readings)
    assert len(mom_readings) == len(yoy_readings)


def test_excludes_marker_labels_and_aligns_dates(fixture_frames: list[dict]) -> None:
    readings = parse_frames(fixture_frames, "mom")
    cpi_2024_06 = [r for r in readings if r.target_month == "2024-06" and r.series == "cpi" and not r.is_actual]
    assert cpi_2024_06, "expected non-actual CPI readings for 2024-06"
    # the marker labels ("CPI May", "PCE May", "CPI Jun") must never become as_of dates
    for r in cpi_2024_06:
        assert r.as_of_date.year == 2024
        assert r.as_of_date.month in (6, 7)
    # first known reading in the fixture's 2024-06 window is 06/03
    assert min(r.as_of_date for r in cpi_2024_06) == date(2024, 6, 3)


def test_known_point_value_2024_06(fixture_frames: list[dict]) -> None:
    """Spot-check one real value straight from the live-captured JSON: 06/03/2024 CPI nowcast."""
    readings = parse_frames(fixture_frames, "mom")
    match = [
        r
        for r in readings
        if r.target_month == "2024-06" and r.series == "cpi" and not r.is_actual and r.as_of_date == date(2024, 6, 3)
    ]
    assert len(match) == 1
    assert match[0].value_pct == pytest.approx(0.108742818232886)


def test_actual_reading_recorded_when_present(fixture_frames: list[dict]) -> None:
    readings = parse_frames(fixture_frames, "mom")
    actuals = [r for r in readings if r.target_month == "2024-06" and r.series == "cpi" and r.is_actual]
    assert len(actuals) == 1
    assert actuals[0].value_pct == pytest.approx(-0.0561896400351314)


def test_year_boundary_wraps_correctly(fixture_frames: list[dict]) -> None:
    """The 2013-12 frame's window runs from December into January — must not stay in 2013."""
    readings = parse_frames(fixture_frames, "mom")
    dec_2013 = [r for r in readings if r.target_month == "2013-12" and r.series == "cpi" and not r.is_actual]
    assert dec_2013
    years = {r.as_of_date.year for r in dec_2013}
    assert years == {2013, 2014}, f"expected the window to wrap into 2014, got years {years}"
    # a late-window (January) reading really is dated in January, not December
    january_readings = [r for r in dec_2013 if r.as_of_date.year == 2014]
    assert all(r.as_of_date.month == 1 for r in january_readings)


def test_data_gap_month_has_no_actual_and_does_not_raise(fixture_frames: list[dict]) -> None:
    """2026-07 (current month at capture time) has a nowcast path but no actual yet."""
    readings = parse_frames(fixture_frames, "mom")
    month_readings = [r for r in readings if r.target_month == "2026-07" and r.series == "cpi"]
    assert month_readings, "expected some nowcast path readings for the in-progress month"
    assert all(not r.is_actual for r in month_readings)


def test_point_in_time_lookahead_guard() -> None:
    readings = [
        NowcastReading(date(2024, 6, 3), "2024-06", "cpi", "mom", 0.10, is_actual=False),
        NowcastReading(date(2024, 6, 10), "2024-06", "cpi", "mom", 0.09, is_actual=False),
        NowcastReading(date(2024, 6, 20), "2024-06", "cpi", "mom", 0.07, is_actual=False),
    ]
    # querying at a date between two readings returns the earlier one, never the later
    hit = latest_as_of(
        readings, target_month="2024-06", series="cpi", index_kind="mom", as_of_date=date(2024, 6, 15)
    )
    assert hit is not None
    assert hit.as_of_date == date(2024, 6, 10)
    assert hit.value_pct == pytest.approx(0.09)

    # querying before any reading exists returns None, not the earliest reading (no lookahead)
    miss = latest_as_of(
        readings, target_month="2024-06", series="cpi", index_kind="mom", as_of_date=date(2024, 6, 1)
    )
    assert miss is None


def test_malformed_subcaption_is_skipped_not_raised() -> None:
    raw = [{"chart": {"subcaption": "not-a-month"}, "categories": [{"category": []}], "dataset": []}]
    assert parse_frames(raw, "mom") == []


def test_length_mismatch_series_is_skipped_not_raised() -> None:
    raw = [
        {
            "chart": {"subcaption": "2024-6"},
            "categories": [{"category": [{"label": "06/03"}, {"label": "06/04"}]}],
            "dataset": [{"seriesname": "CPI Inflation", "data": [{"value": "0.1"}]}],  # length 1 vs 2 dates
        }
    ]
    assert parse_frames(raw, "mom") == []
