"""Tests for `macro.backtest`: synthetic-month signal quality, market edge, fillability, verdict.

Per the spec's testing section: "on a synthetic month where the nowcast is deliberately
better/worse than a baseline, the verdict flips correctly; fee model matches `rate*p*(1-p)`."
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from kalshi_bot.macro.backtest import (
    FillabilitySummary,
    Gate0Verdict,
    MarketEdgeResult,
    MonthlySeries,
    SignalQualityResult,
    _fee_dollars,
    build_monthly_series,
    evaluate_market_edge_for_event,
    evaluate_signal_quality,
    fit_baseline_sigma,
    fit_sigma_curve_from_history,
    horizon_days,
    naive_carry_forward_baseline,
)
from kalshi_bot.macro.distribution import SigmaCurve, SigmaCurvePoint
from kalshi_bot.macro.ladder import parse_market
from kalshi_bot.macro.nowcast_source import NowcastReading
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars


def _build_synthetic_readings(*, nowcast_close_to_actual: bool) -> list[NowcastReading]:
    """Six months of a toy series: alternating actual values, plus a naive-hostile pattern.

    The naive carry-forward baseline (previous month's actual) is always off by 0.5 here. The
    nowcast is either close (0.02 error, in the `nowcast_close_to_actual=True` branch) or exactly
    as bad as the baseline flipped in sign (`False` branch) so the margin should flip sign too.
    """
    months = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06"]
    actuals = [0.10, 0.60, 0.10, 0.60, 0.10, 0.60]  # alternates hard -> baseline (prev month) always wrong by 0.5
    readings: list[NowcastReading] = []
    for i, (month, actual) in enumerate(zip(months, actuals)):
        release_date = date(2023, i + 1, 15)
        readings.append(NowcastReading(release_date, month, "cpi", "mom", actual, is_actual=True))
        if nowcast_close_to_actual:
            nowcast_point = actual + 0.02  # small, consistent error
        else:
            # as wrong as the naive baseline: whatever the *previous* actual was
            prev_actual = actuals[i - 1] if i > 0 else actual
            nowcast_point = prev_actual
        for day_offset, as_of_day in enumerate((1, 5, 10)):
            readings.append(
                NowcastReading(
                    date(2023, i + 1, as_of_day), month, "cpi", "mom", nowcast_point, is_actual=False
                )
            )
    return readings


def test_signal_quality_margin_flips_sign_between_good_and_bad_nowcast() -> None:
    good_readings = _build_synthetic_readings(nowcast_close_to_actual=True)
    bad_readings = _build_synthetic_readings(nowcast_close_to_actual=False)

    strikes = [round(-0.2 + 0.05 * i, 2) for i in range(20)]  # -0.2 .. 0.75

    def _margin(readings: list[NowcastReading]) -> float:
        monthly = build_monthly_series(readings, series="cpi", index_kind="mom")
        sigma_curve = fit_sigma_curve_from_history(monthly, bucket_days=30, min_observations=1)
        baseline = naive_carry_forward_baseline(monthly)
        baseline_sigma_curve = fit_baseline_sigma(monthly, baseline)
        result = evaluate_signal_quality(
            monthly,
            strikes=strikes,
            sigma_curve=sigma_curve,
            baseline=baseline,
            baseline_sigma_curve=baseline_sigma_curve,
        )
        return result.margin

    good_margin = _margin(good_readings)
    bad_margin = _margin(bad_readings)
    assert good_margin > 0, "a nowcast close to the actual should beat the naive baseline"
    assert bad_margin < 0, "a nowcast as wrong as carry-forward (opposite direction) should lose to baseline"


def test_horizon_days_never_negative() -> None:
    assert horizon_days(date(2024, 1, 10), date(2024, 1, 15)) == 0
    assert horizon_days(date(2024, 1, 20), date(2024, 1, 10)) == 10


def test_naive_carry_forward_uses_most_recent_prior_actual() -> None:
    readings = [
        NowcastReading(date(2023, 1, 15), "2023-01", "cpi", "mom", 0.30, is_actual=True),
        NowcastReading(date(2023, 3, 15), "2023-03", "cpi", "mom", 0.40, is_actual=True),
    ]
    monthly = build_monthly_series(readings, series="cpi", index_kind="mom")
    baseline = naive_carry_forward_baseline(monthly)
    # 2023-01 has no prior month -> no baseline entry
    assert "2023-01" not in baseline
    # 2023-03's baseline carries forward 2023-01's actual (the only known prior)
    assert baseline["2023-03"] == pytest.approx(0.30)


def test_fee_matches_rate_p_one_minus_p() -> None:
    for price in (0.10, 0.30, 0.5, 0.7, 0.95):
        expected = float(
            estimate_kalshi_taker_fee_dollars(price_dollars=Decimal(str(price)), fee_rate=Decimal("0.07"))
        )
        assert _fee_dollars(price, fee_rate=0.07) == pytest.approx(expected)


def test_fee_is_symmetric_around_50_cents() -> None:
    assert _fee_dollars(0.3, fee_rate=0.07) == pytest.approx(_fee_dollars(0.7, fee_rate=0.07))


def _rung(ticker: str, strike: float, yes_bid: str, yes_ask: str, result: str) -> "object":
    return parse_market(
        {
            "ticker": ticker,
            "floor_strike": strike,
            "strike_type": "greater",
            "yes_bid_dollars": yes_bid,
            "yes_ask_dollars": yes_ask,
            "volume_fp": "100.0",
            "open_interest_fp": "50.0",
            "result": result,
        }
    )


def test_market_edge_positive_when_nowcast_correctly_disagrees_with_mispriced_mid() -> None:
    # Kalshi mid says 20% chance of "yes" (mispriced low); the nowcast (and the eventual outcome)
    # say it's actually very likely -> nowcast should show a Brier + PnL edge over the mid.
    strike = 0.30
    rung = _rung("EVT-T0.3", strike, "0.15", "0.25", "yes")  # mid = 0.20

    monthly_series = MonthlySeries(
        target_month="2024-01",
        release_date=date(2024, 1, 20),
        actual=0.70,  # actual well above strike -> "yes" is correct and heavily favored
        path=((date(2024, 1, 1), 0.70),),
    )
    sigma_curve = SigmaCurve([SigmaCurvePoint(0, 0.05)])  # tight sigma -> confident correct call

    result = evaluate_market_edge_for_event(
        "EVT",
        [(rung, [(date(2024, 1, 5), 0.15, 0.25)])],
        monthly_series,
        sigma_curve,
    )
    assert result.n_observations == 1
    assert result.nowcast_brier < result.kalshi_mid_brier
    assert result.net_pnl_dollars > 0
    assert result.beats_mid is True


def test_market_edge_excludes_observations_on_or_after_release() -> None:
    strike = 0.30
    rung = _rung("EVT-T0.3", strike, "0.15", "0.25", "yes")
    monthly_series = MonthlySeries(
        target_month="2024-01",
        release_date=date(2024, 1, 20),
        actual=0.70,
        path=((date(2024, 1, 1), 0.70),),
    )
    sigma_curve = SigmaCurve([SigmaCurvePoint(0, 0.05)])
    result = evaluate_market_edge_for_event(
        "EVT",
        [(rung, [(date(2024, 1, 20), 0.15, 0.25), (date(2024, 1, 25), 0.10, 0.20)])],  # both on/after release
        monthly_series,
        sigma_curve,
    )
    assert result.n_observations == 0
    assert result.nowcast_brier is None
    assert result.net_pnl_dollars == 0.0


def test_fillability_summary_reports_near_money_volume_oi() -> None:
    from kalshi_bot.macro.backtest import summarize_fillability

    rungs = [
        _rung("far", -1.0, "0.00", "0.02", "yes"),  # mid 0.01 -> excluded
        _rung("near1", 0.1, "0.40", "0.50", "yes"),  # mid 0.45
        _rung("near2", 0.2, "0.55", "0.65", "no"),  # mid 0.60
    ]
    summary = summarize_fillability("EVT", rungs)
    assert summary.n_near_money_rungs == 2
    assert summary.max_volume == 100.0
    assert summary.max_open_interest == 50.0
    assert summary.min_near_money_volume == 100.0  # both fixture rungs share the same fixed volume


def test_gate0_verdict_part2_is_decisive() -> None:
    signal = SignalQualityResult(
        n_months=100,
        n_score_points=1000,
        nowcast_brier=0.05,
        baseline_brier=0.10,
        final_vintage_nowcast_brier=0.05,
        final_vintage_baseline_brier=0.10,
    )
    fillability = [
        FillabilitySummary(
            event_ticker="EVT",
            n_near_money_rungs=3,
            max_volume=1000.0,
            max_open_interest=500.0,
            min_near_money_volume=100.0,
            min_near_money_open_interest=50.0,
        )
    ]
    failing_market_edge = [
        MarketEdgeResult(
            event_ticker="EVT",
            n_observations=10,
            nowcast_brier=0.20,
            kalshi_mid_brier=0.10,  # mid beats nowcast
            net_pnl_dollars=-5.0,
            fees_paid_dollars=1.0,
        )
    ]
    verdict = Gate0Verdict(
        signal_quality=signal,
        market_edge_results=failing_market_edge,
        fillability_results=fillability,
        min_brier_margin=0.01,
    )
    # part 1 and part 3 both pass in isolation, but part 2 is decisive
    assert verdict.part1_signal_passes is True
    assert verdict.part3_fillability_passes is True
    assert verdict.part2_market_edge_passes is False
    assert verdict.overall_go is False


def test_gate0_verdict_passes_only_when_all_three_parts_pass() -> None:
    signal = SignalQualityResult(
        n_months=100,
        n_score_points=1000,
        nowcast_brier=0.05,
        baseline_brier=0.10,
        final_vintage_nowcast_brier=0.05,
        final_vintage_baseline_brier=0.10,
    )
    fillability = [
        FillabilitySummary(
            event_ticker="EVT",
            n_near_money_rungs=3,
            max_volume=1000.0,
            max_open_interest=500.0,
            min_near_money_volume=100.0,
            min_near_money_open_interest=50.0,
        )
    ]
    passing_market_edge = [
        MarketEdgeResult(
            event_ticker="EVT",
            n_observations=10,
            nowcast_brier=0.05,
            kalshi_mid_brier=0.10,
            net_pnl_dollars=5.0,
            fees_paid_dollars=1.0,
        )
    ]
    verdict = Gate0Verdict(
        signal_quality=signal,
        market_edge_results=passing_market_edge,
        fillability_results=fillability,
        min_brier_margin=0.01,
    )
    assert verdict.overall_go is True
