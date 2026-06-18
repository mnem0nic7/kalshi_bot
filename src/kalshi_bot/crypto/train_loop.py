"""Pure helpers for the continuous-train work order + resume cursor.

Kept free of IO so the ordering/resume logic is unit-testable. The daemon's
``_run_crypto_continuous_train_loop`` builds the work order once, persists the
last-completed (asset, frequency) pair to a deployment-control note after each
item, and resumes at ``resume_index`` on (re)start — so a restart picks up where
it left off instead of re-training the 15m pass from scratch and starving 1h.
"""
from __future__ import annotations


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
