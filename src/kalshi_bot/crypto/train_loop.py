"""Pure helpers for the continuous-train work order + resume cursor.

Kept free of IO so the ordering/resume logic is unit-testable. The daemon's
``_run_crypto_continuous_train_loop`` builds the work order once, persists the
last-completed (asset, frequency) pair to a deployment-control note after each
item, and resumes at ``resume_index`` on (re)start — so a restart picks up where
it left off instead of re-training the 15m pass from scratch and starving 1h.
"""
from __future__ import annotations

from datetime import datetime, timedelta


def materialize_window_bounds(
    frontier: datetime, now: datetime, step_hours: float
) -> list[datetime]:
    """Forward-stepping window-end boundaries for a CHUNKED materialize.

    A single materialize pass reads ``[watermark-warmup, now]`` and commits its
    rows in ONE upsert at the very end, so a kill mid-pass loses the whole (e.g.
    11-day) window and the restart redoes it. Chunking instead runs a bounded pass
    per sub-window: each commits, advancing the implicit watermark (max committed
    ``decision_time``), so a kill only loses the current chunk and the next pass
    resumes from the committed frontier.

    Returns the successive window-end timestamps stepping from ``frontier`` to
    ``now`` in ``step_hours`` increments, the last bound being exactly ``now`` (so
    the final chunk equals the tail of an unbounded pass). Returns ``[]`` when
    chunking is disabled (``step_hours <= 0`` — caller does a single unbounded
    pass, the legacy behavior) or already caught up (``frontier >= now``).
    """
    if step_hours <= 0 or frontier >= now:
        return []
    step = timedelta(hours=step_hours)
    bounds: list[datetime] = []
    edge = frontier + step
    while edge < now:
        bounds.append(edge)
        edge += step
    bounds.append(now)  # final chunk always lands exactly on now
    return bounds


def build_train_work_order(
    assets: list[str], frequencies: list[str]
) -> list[tuple[str, str]]:
    """Asset-major interleave: every frequency for an asset before the next asset.

    This puts the first 1h item right after the first asset's 15m, so even a
    short uptime window trains some 1h (the old frequency-major order ran the
    entire 15m pass first, so 1h never got a turn before a restart).
    """
    return [(asset, freq) for asset in assets for freq in frequencies]


def resume_index(
    work_order: list[tuple[str, str]], last_completed: tuple[str, str] | None
) -> int:
    """Index of the item to run next, given the last fully completed pair.

    Returns 0 (restart the sweep) when there is no cursor, the work order is
    empty, or the cursor's pair is no longer in the order (e.g. the configured
    assets/frequencies changed) — a safe, idempotent fallback.
    """
    if not work_order or not last_completed:
        return 0
    try:
        i = work_order.index(tuple(last_completed))
    except ValueError:
        return 0
    return (i + 1) % len(work_order)


def frequency_last_indices(work_order: list[tuple[str, str]]) -> dict[int, str]:
    """Map {index: frequency} for the LAST item of each frequency in the order.

    The loop refreshes a frequency's pooled gate + sizing policy once its final
    asset for that frequency completes, preserving the per-frequency refresh
    semantics of the original loop under the interleaved order.
    """
    last: dict[str, int] = {}
    for i, (_asset, freq) in enumerate(work_order):
        last[freq] = i
    return {i: freq for freq, i in last.items()}
