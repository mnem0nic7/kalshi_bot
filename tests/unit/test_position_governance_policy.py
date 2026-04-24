from __future__ import annotations

from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.services.position_governance import classify_position_health


def _settings() -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        risk_min_edge_bps=500,
        risk_min_contract_price_dollars=0.25,
        risk_min_probability_extremity_pct=25.0,
        risk_probability_midband_max_extra_edge_bps=500,
    )


def _classify(
    *,
    fair_yes: str,
    side: str,
    current_price: str,
) -> dict:
    return classify_position_health(
        settings=_settings(),
        position_side=side,
        average_price_dollars=Decimal(current_price),
        current_price_dollars=Decimal(current_price),
        signal_payload={
            "fair_yes_dollars": fair_yes,
            "recommended_side": side,
            "trade_regime": "standard",
        },
        stop_loss_outcome_status=None,
        stop_loss_stopped_at=None,
    )


def test_position_governance_probability_boundaries_do_not_block_fresh_entry() -> None:
    low_boundary = _classify(fair_yes="0.2500", side="no", current_price="0.7000")
    high_boundary = _classify(fair_yes="0.7500", side="yes", current_price="0.7000")

    assert low_boundary["fresh_entry_allowed"] is True
    assert low_boundary["fresh_entry_reasons"] == []
    assert high_boundary["fresh_entry_allowed"] is True
    assert high_boundary["fresh_entry_reasons"] == []


def test_position_governance_midband_probability_blocks_weak_edge() -> None:
    result = _classify(fair_yes="0.6400", side="yes", current_price="0.6000")

    assert result["fresh_entry_allowed"] is False
    assert any("requires 720bps edge" in reason for reason in result["fresh_entry_reasons"])


def test_position_governance_midband_probability_allows_strong_edge() -> None:
    result = _classify(fair_yes="0.6400", side="yes", current_price="0.5600")

    assert result["fresh_entry_allowed"] is True
    assert result["fresh_entry_reasons"] == []
