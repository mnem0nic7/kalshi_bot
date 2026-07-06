"""Planned clean restart between materialize chunks (2026-07-06).

The 1h catch-up OOM'd at every window size (48h/24h/12h) because memory
accumulates across chunks within one process — while a FRESH process passed
every one of those same windows. So instead of letting the memcg kill provide
the fresh process (mid-chunk, with a 32g thrash spike and a lost chunk), the
stepped loop now exits cleanly BETWEEN chunks once RSS crosses a threshold;
docker `unless-stopped` restarts it and the committed watermark resumes the
catch-up exactly where it left off.
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from kalshi_bot.crypto import train_loop
from kalshi_bot.crypto.services import CryptoTrainingBackfillService
from kalshi_bot.crypto.train_loop import process_rss_bytes, should_restart_for_rss


def test_process_rss_bytes_reads_own_process():
    rss = process_rss_bytes()
    assert rss > 10_000_000  # a python test process is >10MB


def test_should_restart_threshold_semantics():
    gb = 1024**3
    assert should_restart_for_rss(19 * gb, 18.0) is True
    assert should_restart_for_rss(17 * gb, 18.0) is False
    assert should_restart_for_rss(19 * gb, 0) is False      # 0 disables
    assert should_restart_for_rss(19 * gb, None) is False   # unset disables
    assert should_restart_for_rss(0, 18.0) is False         # unreadable rss -> never


async def test_stepped_loop_exits_cleanly_between_chunks(monkeypatch):
    svc = CryptoTrainingBackfillService.__new__(CryptoTrainingBackfillService)
    svc.settings = SimpleNamespace(
        crypto_train_materialize_max_step_hours=1.0,
        crypto_train_materialize_restart_rss_gb=18.0,
        crypto_train_lookback_days=1,           # 24 one-hour chunks
        crypto_train_incremental_materialize_enabled=False,
        kalshi_env="production",
    )

    ran = []

    async def _fake_once(**kwargs):
        ran.append(kwargs.get("window_end"))
        return {"status": "ok"}

    svc._materialize_once = _fake_once

    # RSS low for the first two chunks, high from the third on.
    monkeypatch.setattr(
        train_loop, "process_rss_bytes",
        lambda: 30 * 1024**3 if len(ran) >= 3 else 1 * 1024**3,
    )
    killed = []
    monkeypatch.setattr(
        "kalshi_bot.crypto.services.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    result = await svc._materialize_stepped(frequency="1h")

    assert len(ran) == 3                      # stopped after the high-RSS chunk
    assert result["status"] == "restart_pending"
    assert result["chunks_completed"] == 3
    assert len(killed) == 1                   # one clean self-SIGTERM


async def test_stepped_loop_never_restarts_after_final_chunk(monkeypatch):
    """The last chunk may leave RSS high — exiting then would be pointless churn:
    the pass is done and the caller proceeds to the fit."""
    svc = CryptoTrainingBackfillService.__new__(CryptoTrainingBackfillService)
    svc.settings = SimpleNamespace(
        crypto_train_materialize_max_step_hours=13.0,   # 1-day window -> 2 chunks
        crypto_train_materialize_restart_rss_gb=18.0,
        crypto_train_lookback_days=1,
        crypto_train_incremental_materialize_enabled=False,
        kalshi_env="production",
    )

    ran = []

    async def _fake_once(**kwargs):
        ran.append(kwargs.get("window_end"))
        return {"status": "ok"}

    svc._materialize_once = _fake_once
    monkeypatch.setattr(train_loop, "process_rss_bytes", lambda: 30 * 1024**3)
    killed = []
    monkeypatch.setattr(
        "kalshi_bot.crypto.services.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    result = await svc._materialize_stepped(frequency="1h")

    # restart fired after chunk 1 (not after the final chunk) — the loop always
    # exits between chunks, never after the last one.
    assert len(ran) == 1
    assert result["status"] == "restart_pending"
    assert len(killed) == 1
