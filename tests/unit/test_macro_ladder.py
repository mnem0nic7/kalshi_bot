"""Tests for Kalshi CPI ladder parsing.

Sample raw market rows are shaped like the real 2026-07-06 live `/markets?event_ticker=...`
payload (field names verified against the actual API response for `KXCPI-26MAY-T0.2`): dollar
quote fields carry `_dollars` suffixes, size fields carry `_fp` suffixes, and the ladder key is
`floor_strike` + `strike_type="greater"`.
"""
from __future__ import annotations

from kalshi_bot.macro.ladder import near_money_rungs, parse_ladder, parse_market


def _raw_market(ticker: str, floor_strike: float, yes_bid: str, yes_ask: str, *, result: str | None = None,
                 volume_fp: str = "0.00", open_interest_fp: str = "0.00") -> dict:
    return {
        "ticker": ticker,
        "floor_strike": floor_strike,
        "strike_type": "greater",
        "yes_bid_dollars": yes_bid,
        "yes_ask_dollars": yes_ask,
        "volume_fp": volume_fp,
        "open_interest_fp": open_interest_fp,
        "result": result,
        "status": "finalized" if result else "active",
    }


def test_parse_market_uses_dollar_and_fp_field_names() -> None:
    raw = _raw_market(
        "KXCPI-26MAY-T0.2", 0.2, "0.9600", "0.9700", result="yes", volume_fp="14767.45", open_interest_fp="11122.04"
    )
    rung = parse_market(raw)
    assert rung.ticker == "KXCPI-26MAY-T0.2"
    assert rung.floor_strike == 0.2
    assert rung.strike_type == "greater"
    assert rung.yes_bid == 0.96
    assert rung.yes_ask == 0.97
    assert rung.mid == 0.965
    assert rung.volume == 14767.45
    assert rung.open_interest == 11122.04
    assert rung.result == "yes"


def test_parse_market_missing_floor_strike_raises() -> None:
    raw = _raw_market("KXCPI-26MAY-T0.2", 0.2, "0.10", "0.20")
    raw["floor_strike"] = None
    try:
        parse_market(raw)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_parse_ladder_sorts_by_strike_ascending() -> None:
    raws = [
        _raw_market("KXCPI-26MAY-T0.5", 0.5, "0.10", "0.20"),
        _raw_market("KXCPI-26MAY-T-0.3", -0.3, "0.90", "0.95"),
        _raw_market("KXCPI-26MAY-T0.1", 0.1, "0.40", "0.45"),
    ]
    rungs = parse_ladder(raws)
    assert [r.floor_strike for r in rungs] == [-0.3, 0.1, 0.5]


def test_parse_ladder_skips_malformed_rows_and_logs_count(caplog) -> None:
    good = _raw_market("KXCPI-26MAY-T0.1", 0.1, "0.40", "0.45")
    bad_missing_bid = {"ticker": "broken", "floor_strike": 0.2, "strike_type": "greater"}
    with caplog.at_level("WARNING"):
        rungs = parse_ladder([good, bad_missing_bid])
    assert len(rungs) == 1
    assert rungs[0].ticker == "KXCPI-26MAY-T0.1"
    assert "skipped 1 malformed" in caplog.text


def test_near_money_rungs_filters_by_mid_band() -> None:
    raws = [
        _raw_market("far-no", -0.3, "0.00", "0.02"),  # mid 0.01 -> excluded
        _raw_market("near", 0.1, "0.45", "0.55"),  # mid 0.50 -> included
        _raw_market("far-yes", 1.0, "0.98", "1.00"),  # mid 0.99 -> excluded
    ]
    rungs = parse_ladder(raws)
    near = near_money_rungs(rungs, low=0.05, high=0.95)
    assert [r.ticker for r in near] == ["near"]
