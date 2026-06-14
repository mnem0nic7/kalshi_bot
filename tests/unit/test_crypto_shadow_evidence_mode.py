"""Unit tests for _resolve_crypto_shadow_evidence_mode (P3).

The flag crypto_shadow_evidence_always_enabled must let the shadow-evidence
decision path run ALONGSIDE production autonomy, while preserving the legacy
behavior (shadow-evidence only when production autonomy is off) by default. It
gates decision LOGGING only; live execution is enforced elsewhere by
asset_mode==LIVE checks.
"""

from __future__ import annotations

from kalshi_bot.crypto.services import _resolve_crypto_shadow_evidence_mode


def test_legacy_off_under_production_autonomy_by_default() -> None:
    # Default (flag off) + production autonomy on => suppressed (the pre-fix state
    # that silenced HYPE/ETH/SOL/XRP).
    assert (
        _resolve_crypto_shadow_evidence_mode(
            production_mode=True,
            quote_evidence_enabled=True,
            production_autonomy_enabled=True,
            shadow_evidence_always=False,
        )
        is False
    )


def test_flag_restores_shadow_evidence_alongside_production_autonomy() -> None:
    # The P3 fix: flag on => shadow-evidence runs even with production autonomy on.
    assert (
        _resolve_crypto_shadow_evidence_mode(
            production_mode=True,
            quote_evidence_enabled=True,
            production_autonomy_enabled=True,
            shadow_evidence_always=True,
        )
        is True
    )


def test_legacy_path_still_on_when_autonomy_off() -> None:
    # Legacy behavior preserved: shadow-evidence on when production autonomy off,
    # regardless of the new flag.
    for flag in (False, True):
        assert (
            _resolve_crypto_shadow_evidence_mode(
                production_mode=True,
                quote_evidence_enabled=True,
                production_autonomy_enabled=False,
                shadow_evidence_always=flag,
            )
            is True
        )


def test_requires_production_mode_and_quote_evidence() -> None:
    # Never engages in demo mode or when quote-evidence collection is disabled,
    # even with the flag set.
    assert (
        _resolve_crypto_shadow_evidence_mode(
            production_mode=False,
            quote_evidence_enabled=True,
            production_autonomy_enabled=True,
            shadow_evidence_always=True,
        )
        is False
    )
    assert (
        _resolve_crypto_shadow_evidence_mode(
            production_mode=True,
            quote_evidence_enabled=False,
            production_autonomy_enabled=True,
            shadow_evidence_always=True,
        )
        is False
    )
