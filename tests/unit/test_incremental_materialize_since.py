"""Pure unit tests for the incremental-materialize window resolver.

These tests exercise the module-level pure function
``_resolve_incremental_materialize_since`` in ``kalshi_bot.crypto.services``
with no DB or service wiring. The function decides what ``since`` the
``_materialize_once`` READ phase should use (full lookback vs. narrow
incremental tail) AND the ``upsert_floor`` below which already-correct rows are
preserved instead of being re-upserted with truncated recency features.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.crypto.services import _resolve_incremental_materialize_since


NOW = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)
FULL_SINCE = NOW - timedelta(days=60)
LABEL_REFRESH_HOURS = 24


def test_disabled_returns_full_window() -> None:
    """enabled=False always returns the full lookback since, regardless of watermark."""
    watermark = NOW - timedelta(hours=2)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=False,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert since == FULL_SINCE
    assert upsert_floor is None
    assert reason == "full_disabled"


def test_cold_cache_none_watermark_returns_full_window() -> None:
    """No persisted rows (watermark is None) => cold cache => full rebuild."""
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=None,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert since == FULL_SINCE
    assert upsert_floor is None
    assert reason == "full_cold_cache"


def test_gap_exceeds_max_returns_full_window() -> None:
    """A watermark older than max_gap_hours forces a safe full rebuild."""
    watermark = NOW - timedelta(hours=200)  # > 168h max gap
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert since == FULL_SINCE
    assert upsert_floor is None
    assert reason == "full_gap_exceeds_max"


def test_normal_incremental_returns_watermark_minus_warmup() -> None:
    """A recent watermark yields the narrow tail = watermark - warmup_hours."""
    watermark = NOW - timedelta(hours=2)
    warmup_hours = 72
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=warmup_hours,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert since == watermark - timedelta(hours=warmup_hours)
    # The narrowed window must be strictly newer than the full lookback window.
    assert since > FULL_SINCE
    # The upsert floor is the label-refresh tail.
    assert upsert_floor == watermark - timedelta(hours=LABEL_REFRESH_HOURS)


def test_incremental_read_window_strictly_wider_than_upsert_zone() -> None:
    """The read window must start strictly before the upsert floor.

    warmup_hours (72) >> label_refresh_hours (24), so every re-upserted row
    (decision_ts > upsert_floor) has its full recency look-back contained in the
    read window and recomputes identically to a full rebuild.
    """
    watermark = NOW - timedelta(hours=2)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert upsert_floor is not None
    assert since < upsert_floor


def test_incremental_clamped_to_full_since() -> None:
    """When watermark - warmup is older than full_since, clamp to full_since.

    This guards against ever reading MORE than the lookback window. Here the
    watermark sits right at the edge of the lookback window, so subtracting the
    warmup would reach before full_since.
    """
    watermark = FULL_SINCE + timedelta(hours=1)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=24 * 365,  # large so the gap rule does not trip
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert since == FULL_SINCE
    assert upsert_floor == watermark - timedelta(hours=LABEL_REFRESH_HOURS)


def test_naive_watermark_is_normalized_and_does_not_crash() -> None:
    """A naive (tz-less) watermark is treated as UTC and resolves cleanly."""
    naive_watermark = (NOW - timedelta(hours=2)).replace(tzinfo=None)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=naive_watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert since.tzinfo is not None
    # Equivalent to the tz-aware watermark path.
    expected = (NOW - timedelta(hours=2)) - timedelta(hours=72)
    assert since == expected
    assert upsert_floor is not None
    assert upsert_floor.tzinfo is not None
    assert upsert_floor == (NOW - timedelta(hours=2)) - timedelta(hours=LABEL_REFRESH_HOURS)


def test_warmup_hours_floored_at_one() -> None:
    """warmup_hours <= 0 is floored to 1 hour, never zero or negative."""
    watermark = NOW - timedelta(hours=2)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=0,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert since == watermark - timedelta(hours=1)
    assert upsert_floor == watermark - timedelta(hours=LABEL_REFRESH_HOURS)


def test_label_refresh_hours_floored_at_one() -> None:
    """label_refresh_hours <= 0 is floored to 1 hour, never zero or negative."""
    watermark = NOW - timedelta(hours=2)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=0,
    )
    assert reason == "incremental"
    assert upsert_floor == watermark - timedelta(hours=1)


def test_gap_exactly_at_max_is_incremental() -> None:
    """A gap exactly equal to max_gap_hours is NOT over the limit (boundary)."""
    watermark = NOW - timedelta(hours=168)
    since, upsert_floor, reason = _resolve_incremental_materialize_since(
        full_since=FULL_SINCE,
        now=NOW,
        watermark=watermark,
        enabled=True,
        warmup_hours=72,
        max_gap_hours=168,
        label_refresh_hours=LABEL_REFRESH_HOURS,
    )
    assert reason == "incremental"
    assert since == watermark - timedelta(hours=72)
    assert upsert_floor == watermark - timedelta(hours=LABEL_REFRESH_HOURS)
