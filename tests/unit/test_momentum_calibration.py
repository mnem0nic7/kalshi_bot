"""
Unit-test clusters for pure helpers in services/momentum_calibration.py.

CF  – bootstrap CI: determinism, CI brackets estimate, width > 0.
CG  – P&L sign direction: adverse slope → negative ratio; favorable → positive.
CJ  – series-ticker regex: valid parse, malformed → no match, non-weather → no match.
CK  – P&L formula: YES/NO sides, slope_against sign, None settlement → None pnl/ratio.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.services.momentum_calibration import (
    _SERIES_TICKER_RE,
    _bootstrap_ci_mean,
    _build_analysis_records,
)


# ── CF: bootstrap CI determinism ─────────────────────────────────────────────


def test_bootstrap_ci_same_seed_same_output() -> None:
    values = [float(x) for x in range(1, 21)]
    lo1, hi1 = _bootstrap_ci_mean(values)
    lo2, hi2 = _bootstrap_ci_mean(values)
    assert lo1 == lo2 and hi1 == hi2


def test_bootstrap_ci_brackets_point_estimate() -> None:
    values = [2.0] * 50
    lo, hi = _bootstrap_ci_mean(values)
    assert lo <= 2.0 <= hi


def test_bootstrap_ci_width_is_positive_for_varied_input() -> None:
    values = [float(x) for x in range(1, 21)]
    lo, hi = _bootstrap_ci_mean(values)
    assert hi > lo


# ── CG: P&L sign direction ───────────────────────────────────────────────────


def _row(
    *,
    slope_cpmin: float | None,
    recommended_side: str = "yes",
    settlement_value_dollars: float | None = None,
    fair_yes_dollars: float = 0.60,
    edge_bps: int = 1000,
) -> dict:
    return {
        "room_id": "r-cg",
        "market_ticker": "KXHIGHBOS-26APR10-T58",
        "local_market_day": "2026-04-10",
        "checkpoint_ts": None,
        "edge_bps": edge_bps,
        "fair_yes_dollars": Decimal(str(fair_yes_dollars)),
        "signal_payload": {
            "momentum_slope_cents_per_min": slope_cpmin,
            "recommended_side": recommended_side,
        },
        "settlement_value_dollars": (
            Decimal(str(settlement_value_dollars))
            if settlement_value_dollars is not None
            else None
        ),
        "kalshi_result": None,
    }


def test_adverse_slope_with_loss_gives_negative_ratio() -> None:
    # YES recommendation, slope > 0 (adverse = price moving up against us?),
    # market settles NO → loss.
    records = _build_analysis_records([
        _row(slope_cpmin=1.0, recommended_side="yes", settlement_value_dollars=0.0)
    ])
    assert records[0]["slope_against"] > 0
    assert records[0]["settlement_pnl"] < 0
    assert records[0]["ratio"] < 0


def test_favorable_slope_with_win_gives_positive_ratio() -> None:
    # YES recommendation, slope < 0 (favorable), market settles YES → win.
    records = _build_analysis_records([
        _row(slope_cpmin=-1.0, recommended_side="yes", settlement_value_dollars=1.0)
    ])
    assert records[0]["slope_against"] < 0
    assert records[0]["settlement_pnl"] > 0
    assert records[0]["ratio"] > 0


# ── CJ: series-ticker regex ───────────────────────────────────────────────────


def test_series_ticker_regex_valid_bos() -> None:
    m = _SERIES_TICKER_RE.match("KXHIGHBOS-26APR10-T58")
    assert m is not None
    assert m.group(1) == "KXHIGHBOS"


def test_series_ticker_regex_valid_nyc() -> None:
    m = _SERIES_TICKER_RE.match("KXHIGHNYC-26APR10-T80")
    assert m is not None
    assert m.group(1) == "KXHIGHNYC"


def test_series_ticker_regex_missing_threshold_suffix() -> None:
    assert _SERIES_TICKER_RE.match("KXHIGHBOS-26APR10") is None


def test_series_ticker_regex_malformed_threshold() -> None:
    assert _SERIES_TICKER_RE.match("KXHIGHBOS-26APR10-badT") is None


def test_series_ticker_regex_non_kxhigh_prefix() -> None:
    assert _SERIES_TICKER_RE.match("KXLOWBOS-26APR10-T58") is None
    assert _SERIES_TICKER_RE.match("HIGHBOS-26APR10-T58") is None


def test_series_ticker_regex_empty_string() -> None:
    assert _SERIES_TICKER_RE.match("") is None


# ── CK: P&L formula correctness ──────────────────────────────────────────────


def test_yes_settled_yes_positive_pnl() -> None:
    records = _build_analysis_records([
        _row(
            slope_cpmin=1.0,
            recommended_side="yes",
            fair_yes_dollars=0.60,
            settlement_value_dollars=1.0,
            edge_bps=1000,
        )
    ])
    r = records[0]
    # settlement_pnl = sv - fyd = 1.0 - 0.60 = 0.40
    assert r["settlement_pnl"] == pytest.approx(0.40)
    # slope_against = slope_cpmin for YES side
    assert r["slope_against"] == pytest.approx(1.0)
    # ratio = pnl / edge_dollars = 0.40 / (1000/10000) = 4.0
    assert r["ratio"] == pytest.approx(0.40 / 0.10)


def test_yes_settled_no_negative_pnl() -> None:
    records = _build_analysis_records([
        _row(
            slope_cpmin=1.0,
            recommended_side="yes",
            fair_yes_dollars=0.60,
            settlement_value_dollars=0.0,
            edge_bps=1000,
        )
    ])
    r = records[0]
    # settlement_pnl = sv - fyd = 0.0 - 0.60 = -0.60
    assert r["settlement_pnl"] == pytest.approx(-0.60)
    assert r["ratio"] == pytest.approx(-0.60 / 0.10)


def test_no_settled_no_positive_pnl() -> None:
    records = _build_analysis_records([
        _row(
            slope_cpmin=1.0,
            recommended_side="no",
            fair_yes_dollars=0.40,
            settlement_value_dollars=0.0,
            edge_bps=1000,
        )
    ])
    r = records[0]
    # settlement_pnl = fyd - sv = 0.40 - 0.0 = 0.40
    assert r["settlement_pnl"] == pytest.approx(0.40)
    # slope_against = -slope_cpmin for NO side
    assert r["slope_against"] == pytest.approx(-1.0)


def test_no_settled_yes_negative_pnl() -> None:
    records = _build_analysis_records([
        _row(
            slope_cpmin=1.0,
            recommended_side="no",
            fair_yes_dollars=0.40,
            settlement_value_dollars=1.0,
            edge_bps=1000,
        )
    ])
    r = records[0]
    # settlement_pnl = fyd - sv = 0.40 - 1.0 = -0.60
    assert r["settlement_pnl"] == pytest.approx(-0.60)
    assert r["slope_against"] == pytest.approx(-1.0)


def test_no_settlement_gives_none_pnl_and_ratio() -> None:
    records = _build_analysis_records([
        _row(slope_cpmin=1.0, settlement_value_dollars=None)
    ])
    r = records[0]
    assert r["settlement_pnl"] is None
    assert r["ratio"] is None


def test_no_slope_in_payload_gives_none_slope_against() -> None:
    records = _build_analysis_records([
        _row(slope_cpmin=None, settlement_value_dollars=1.0)
    ])
    r = records[0]
    assert r["slope_against"] is None


def test_fair_yes_dollars_is_used_not_fill_price() -> None:
    # Two rows: same settlement but different fair_yes — pnl should differ.
    rows = [
        _row(slope_cpmin=0.5, fair_yes_dollars=0.50, settlement_value_dollars=1.0, edge_bps=1000),
        _row(slope_cpmin=0.5, fair_yes_dollars=0.70, settlement_value_dollars=1.0, edge_bps=1000),
    ]
    records = _build_analysis_records(rows)
    assert records[0]["settlement_pnl"] == pytest.approx(0.50)
    assert records[1]["settlement_pnl"] == pytest.approx(0.30)
