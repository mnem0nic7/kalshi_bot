"""Detection-constant lockstep guard between the two stale-quote scanners.

scripts/stale_quote_pilot.py (15m, live-capable) and scripts/stale_quote_shadow_1h.py
(1h, shadow-only) each carry their OWN copy of the stale-quote detection logic
(comment: "keep in lockstep with scripts/stale_quote_shadow.py" /
"lockstep with scripts/stale_quote_pilot.py"). If someone tunes one script's
VOL_MODEL / thresholds without the other, the 1h shadow scanner silently stops
measuring the same edge the 15m pilot validated — this test catches that drift.

Parses the scripts as source text via ast rather than importing them: they pull
in app modules (ExecutionService, KalshiClient, DB session factories) purely as
side effects of being scripts, and this test only cares about the module-level
constants, not runtime behavior.

POLL_S and MAX_TTC_S are DELIBERATE per-timeframe differences (15m needs a
faster loop + a much tighter time-to-close ceiling than an hourly market) and
must NOT be asserted equal — see test_deliberate_diffs_are_not_locked_together.
"""
from __future__ import annotations

import ast
from pathlib import Path

from kalshi_bot.crypto.stale_pilot_readout import SERIES_15M

REPO_ROOT = Path(__file__).resolve().parents[2]
PILOT_PATH = REPO_ROOT / "scripts" / "stale_quote_pilot.py"
SHADOW_1H_PATH = REPO_ROOT / "scripts" / "stale_quote_shadow_1h.py"

LOCKSTEP_NAMES = {"VOL_MODEL", "LOOKBACK_S", "DFAIR_TH", "QUOTE_EPS", "MAX_SPREAD", "MIN_TTC_S"}


def _module_level_constants(path: Path, names: set[str]) -> dict:
    tree = ast.parse(path.read_text(), filename=str(path))
    found: dict = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in names:
                found[name] = ast.literal_eval(node.value)
    return found


def test_pilot_and_shadow_1h_share_detection_constants():
    pilot = _module_level_constants(PILOT_PATH, LOCKSTEP_NAMES)
    shadow_1h = _module_level_constants(SHADOW_1H_PATH, LOCKSTEP_NAMES)

    for name in sorted(LOCKSTEP_NAMES):
        assert name in pilot, f"{name} missing from {PILOT_PATH.name}"
        assert name in shadow_1h, f"{name} missing from {SHADOW_1H_PATH.name}"
        assert pilot[name] == shadow_1h[name], (
            f"{name} diverged between scanners: "
            f"{PILOT_PATH.name}={pilot[name]!r} vs {SHADOW_1H_PATH.name}={shadow_1h[name]!r}"
        )


def test_pilot_series_matches_readout_series_15m():
    pilot = _module_level_constants(PILOT_PATH, {"SERIES"})
    assert pilot["SERIES"] == SERIES_15M


def test_deliberate_diffs_are_not_locked_together():
    """POLL_S and MAX_TTC_S are intentional per-timeframe divergences; this
    documents the exemption so nobody "fixes" the lockstep test by force-
    equalizing them."""
    pilot = _module_level_constants(PILOT_PATH, {"POLL_S", "MAX_TTC_S"})
    shadow_1h = _module_level_constants(SHADOW_1H_PATH, {"POLL_S", "MAX_TTC_S"})
    assert pilot["POLL_S"] != shadow_1h["POLL_S"]
    assert pilot["MAX_TTC_S"] != shadow_1h["MAX_TTC_S"]
