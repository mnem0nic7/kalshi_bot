"""The dedicated training node bypasses the active-color gate for the nightly,
while live daemons (training_node=False) still require active color. This keeps
exactly one trainer (the dedicated node) running training, off the trading
process entirely."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from kalshi_bot.services.daemon import DaemonService


class _Stub:
    """Minimal stand-in for `self` exposing only what the gate touches."""

    def __init__(self, *, nightly_auto: bool, training_node: bool, active: bool):
        self.settings = SimpleNamespace(
            crypto_model_nightly_auto_enabled=nightly_auto,
            crypto_training_node=training_node,
        )
        self._active = active
        self.ran = False

    async def _is_active_color(self) -> bool:
        return self._active

    async def _maybe_run_crypto_model_nightly(self):
        self.ran = True
        return {"pooled": "refreshed"}


async def _gate(stub: _Stub):
    return await DaemonService._maybe_run_crypto_model_nightly_if_active(stub)


async def test_training_node_runs_even_when_not_active() -> None:
    stub = _Stub(nightly_auto=True, training_node=True, active=False)
    await _gate(stub)
    assert stub.ran is True


async def test_live_daemon_skips_when_not_active() -> None:
    stub = _Stub(nightly_auto=True, training_node=False, active=False)
    await _gate(stub)
    assert stub.ran is False


async def test_live_daemon_runs_when_active() -> None:
    stub = _Stub(nightly_auto=True, training_node=False, active=True)
    await _gate(stub)
    assert stub.ran is True


async def test_disabled_nightly_never_runs_even_on_training_node() -> None:
    stub = _Stub(nightly_auto=False, training_node=True, active=True)
    await _gate(stub)
    assert stub.ran is False
