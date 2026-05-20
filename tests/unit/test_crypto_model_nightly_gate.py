"""Unit tests for the crypto nightly-regen refresh gate.

These exercise the pure decision helper directly, so they don't spin up the
daemon (the integration variants in test_daemon_service.py cover the wiring).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kalshi_bot.services.daemon import DaemonService

NOW = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)
MAX_AGE = timedelta(hours=24)
MIN_NEW = 60


def reason(**kw):
    base = dict(
        artifact_status="trained",
        trained_at=NOW - timedelta(hours=1),
        now=NOW,
        max_age=MAX_AGE,
        new_rows=0,
        min_new_rows=MIN_NEW,
    )
    base.update(kw)
    return DaemonService._crypto_model_refresh_reason(**base)


def test_missing_artifact_refreshes():
    assert reason(artifact_status=None, trained_at=None) == "missing"


def test_trained_and_fresh_is_skipped():
    assert reason(artifact_status="trained", trained_at=NOW - timedelta(hours=2), new_rows=5) is None


def test_trained_but_aged_out_refreshes():
    assert reason(artifact_status="trained", trained_at=NOW - timedelta(hours=48)) == "aged_out"


def test_trained_with_new_data_refreshes():
    assert reason(artifact_status="trained", trained_at=NOW - timedelta(hours=2), new_rows=MIN_NEW) == "new_data"


def test_naive_trained_at_is_treated_as_utc():
    # tz-naive trained_at must not crash and should still detect age
    assert reason(artifact_status="trained", trained_at=datetime(2026, 5, 18, 12, 0)) == "aged_out"


def test_data_starved_stays_dormant_even_when_aged():
    # The BNB-1h case: insufficient_data + no new strict rows -> do NOT retry,
    # even though the artifact is well past max_age.
    assert reason(artifact_status="insufficient_data", trained_at=NOW - timedelta(hours=72), new_rows=0) is None


def test_data_starved_reactivates_on_new_data():
    assert (
        reason(artifact_status="insufficient_data", trained_at=NOW - timedelta(hours=72), new_rows=MIN_NEW)
        == "new_data"
    )
