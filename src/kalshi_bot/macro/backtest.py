"""Gate-0 backtest: signal quality, market edge, and fillability (script + importable fns).

Three independent checks, per the spec's go/no-go:

1. **Signal** (`evaluate_signal_quality`) — is the nowcast-implied ladder better calibrated
   (lower Brier) than a naive carry-forward baseline against the eventual actual CPI print? This
   uses the Cleveland Fed's own ~13y vintage archive end-to-end offline; no Kalshi data needed.
2. **Market edge** (`evaluate_market_edge_for_event`) — **decisive**. Does the nowcast-implied
   ladder beat Kalshi's own mid price on the tradeable (near-money) subset, net of
   `rate * p * (1-p)` fees? This can only be measured where both a nowcast vintage path and
   Kalshi price history exist, which as of 2026-07-06 is a *tiny* number of events (the public
   API's price-history retention is ~2 months) — reported honestly as such, never dressed up as
   a large-sample result.
3. **Fillability** (`summarize_fillability`) — do the near-money strikes that would generate the
   edge actually have real volume/OI?

`Gate0Verdict` combines the three into a single verdict (`overall_go`), with part 2 (market edge)
as the decisive test: per the spec, if it fails, gate-0 fails regardless of parts 1 and 3.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from kalshi_bot.macro.distribution import NowcastDistribution, SigmaCurve
from kalshi_bot.macro.ladder import LadderRung
from kalshi_bot.macro.nowcast_source import NowcastReading
from kalshi_bot.services.fee_model import KALSHI_DEFAULT_TAKER_FEE_RATE, estimate_kalshi_taker_fee_dollars

DEFAULT_HORIZON_BUCKET_DAYS = 5
DEFAULT_MIN_BUCKET_OBSERVATIONS = 3


@dataclass(frozen=True, slots=True)
class MonthlySeries:
    """One target month's nowcast run-up + eventual actual, for a single (series, index_kind)."""

    target_month: str
    release_date: date | None
    actual: float | None
    path: tuple[tuple[date, float], ...]  # (as_of_date, nowcast_value), sorted ascending

    def path_before(self, cutoff: date) -> list[tuple[date, float]]:
        """Readings strictly before `cutoff` — used to enforce point-in-time discipline."""
        return [(d, v) for d, v in self.path if d < cutoff]


def build_monthly_series(
    readings: list[NowcastReading], *, series: str = "cpi", index_kind: str = "mom"
) -> dict[str, MonthlySeries]:
    """Group readings into one `MonthlySeries` per target month for the given (series, index_kind)."""
    by_month: dict[str, dict[str, object]] = defaultdict(lambda: {"actual": None, "release_date": None, "path": []})
    for r in readings:
        if r.series != series or r.index_kind != index_kind:
            continue
        bucket = by_month[r.target_month]
        if r.is_actual:
            bucket["actual"] = r.value_pct
            bucket["release_date"] = r.as_of_date
        else:
            bucket["path"].append((r.as_of_date, r.value_pct))  # type: ignore[union-attr]

    result: dict[str, MonthlySeries] = {}
    for target_month, bucket in by_month.items():
        path = sorted(bucket["path"])  # type: ignore[arg-type]
        result[target_month] = MonthlySeries(
            target_month=target_month,
            release_date=bucket["release_date"],  # type: ignore[arg-type]
            actual=bucket["actual"],  # type: ignore[arg-type]
            path=tuple(path),
        )
    return result


def horizon_days(release_date: date, as_of_date: date) -> int:
    return max(0, (release_date - as_of_date).days)


def fit_sigma_curve_from_history(
    monthly: dict[str, MonthlySeries],
    *,
    bucket_days: int = DEFAULT_HORIZON_BUCKET_DAYS,
    min_observations: int = DEFAULT_MIN_BUCKET_OBSERVATIONS,
) -> SigmaCurve:
    """Fit sigma(horizon) from |actual - nowcast_at_horizon| across all months with a known actual."""
    errors_by_bucket: dict[int, list[float]] = defaultdict(list)
    for series_data in monthly.values():
        if series_data.actual is None or series_data.release_date is None:
            continue
        for as_of, value in series_data.path_before(series_data.release_date):
            h = horizon_days(series_data.release_date, as_of)
            bucket = (h // bucket_days) * bucket_days
            errors_by_bucket[bucket].append(series_data.actual - value)
    return SigmaCurve.fit(errors_by_bucket, min_observations=min_observations)


def naive_carry_forward_baseline(monthly: dict[str, MonthlySeries]) -> dict[str, float]:
    """Baseline point estimate: the most recently *known* prior month's actual value.

    Chronological carry-forward — for target month `t`, the baseline is the actual of the most
    recent month before `t` that has a known actual, so it never uses information unavailable
    before `t`'s own release.
    """
    ordered_months = sorted(monthly.keys())
    baseline: dict[str, float] = {}
    prev_actual: float | None = None
    for target_month in ordered_months:
        if prev_actual is not None:
            baseline[target_month] = prev_actual
        if monthly[target_month].actual is not None:
            prev_actual = monthly[target_month].actual
    return baseline


def fit_baseline_sigma(monthly: dict[str, MonthlySeries], baseline: dict[str, float]) -> SigmaCurve:
    """Single-point sigma curve for the naive baseline (horizon-invariant: no intra-month info)."""
    errors = [
        monthly[tm].actual - baseline[tm]
        for tm in baseline
        if tm in monthly and monthly[tm].actual is not None
    ]
    return SigmaCurve.fit({0: errors}, min_observations=1)


def brier_score(prob_yes: float, outcome: bool) -> float:
    return (prob_yes - (1.0 if outcome else 0.0)) ** 2


@dataclass(frozen=True, slots=True)
class SignalQualityResult:
    n_months: int
    n_score_points: int
    nowcast_brier: float
    baseline_brier: float
    final_vintage_nowcast_brier: float
    final_vintage_baseline_brier: float

    @property
    def margin(self) -> float:
        """Positive means the nowcast beats the baseline (lower Brier is better)."""
        return self.baseline_brier - self.nowcast_brier

    @property
    def final_vintage_margin(self) -> float:
        return self.final_vintage_baseline_brier - self.final_vintage_nowcast_brier


def evaluate_signal_quality(
    monthly: dict[str, MonthlySeries],
    *,
    strikes: list[float],
    sigma_curve: SigmaCurve,
    baseline: dict[str, float],
    baseline_sigma_curve: SigmaCurve,
    dist_kind: str = "gaussian",
) -> SignalQualityResult:
    """Pooled Brier comparison: nowcast-implied ladder vs naive-baseline-implied ladder.

    Pooled across every (month, as-of date, strike) triple with a known eventual actual — this
    over-counts within a month (the as-of path is autocorrelated), so `final_vintage_*` fields
    also report the more conservative one-observation-per-month statistic (using only the last
    pre-release reading each month) as a robustness check.
    """
    pooled_now: list[float] = []
    pooled_base: list[float] = []
    final_now: list[float] = []
    final_base: list[float] = []
    n_months = 0
    baseline_sigma = baseline_sigma_curve.sigma_at(0)

    for target_month, series_data in monthly.items():
        if series_data.actual is None or series_data.release_date is None or target_month not in baseline:
            continue
        path = series_data.path_before(series_data.release_date)
        if not path:
            continue
        n_months += 1
        baseline_point = baseline[target_month]
        actual = series_data.actual

        last_as_of, last_value = max(path, key=lambda pair: pair[0])
        last_sigma = sigma_curve.sigma_at(horizon_days(series_data.release_date, last_as_of))
        now_dist_final = NowcastDistribution(point_estimate=last_value, sigma=last_sigma, kind=dist_kind)
        base_dist = NowcastDistribution(point_estimate=baseline_point, sigma=baseline_sigma, kind=dist_kind)
        for k in strikes:
            outcome = actual > k
            final_now.append(brier_score(now_dist_final.prob_above(k), outcome))
            final_base.append(brier_score(base_dist.prob_above(k), outcome))

        for as_of, value in path:
            sigma = sigma_curve.sigma_at(horizon_days(series_data.release_date, as_of))
            now_dist = NowcastDistribution(point_estimate=value, sigma=sigma, kind=dist_kind)
            for k in strikes:
                outcome = actual > k
                pooled_now.append(brier_score(now_dist.prob_above(k), outcome))
                pooled_base.append(brier_score(base_dist.prob_above(k), outcome))

    if not pooled_now:
        raise ValueError("no scoreable (month, strike) observations — check inputs")

    return SignalQualityResult(
        n_months=n_months,
        n_score_points=len(pooled_now),
        nowcast_brier=sum(pooled_now) / len(pooled_now),
        baseline_brier=sum(pooled_base) / len(pooled_base),
        final_vintage_nowcast_brier=sum(final_now) / len(final_now),
        final_vintage_baseline_brier=sum(final_base) / len(final_base),
    )


@dataclass(frozen=True, slots=True)
class MarketEdgeObservation:
    as_of: date
    strike: float
    kalshi_mid: float
    nowcast_prob: float
    outcome: bool
    net_pnl_dollars: float


@dataclass(frozen=True, slots=True)
class MarketEdgeResult:
    event_ticker: str
    n_observations: int
    nowcast_brier: float | None
    kalshi_mid_brier: float | None
    net_pnl_dollars: float
    fees_paid_dollars: float

    @property
    def brier_margin(self) -> float | None:
        """Positive means the nowcast beats the Kalshi mid (lower Brier is better)."""
        if self.nowcast_brier is None or self.kalshi_mid_brier is None:
            return None
        return self.kalshi_mid_brier - self.nowcast_brier

    @property
    def beats_mid(self) -> bool:
        return self.n_observations > 0 and self.net_pnl_dollars > 0 and (self.brier_margin or 0.0) > 0


def evaluate_market_edge_for_event(
    event_ticker: str,
    rungs_with_history: list[tuple[LadderRung, list[tuple[date, float, float]]]],
    monthly_series: MonthlySeries,
    sigma_curve: SigmaCurve,
    *,
    dist_kind: str = "gaussian",
    near_money_low: float = 0.05,
    near_money_high: float = 0.95,
    fee_rate: float = float(KALSHI_DEFAULT_TAKER_FEE_RATE),
) -> MarketEdgeResult:
    """Simulate net-of-fees P&L + Brier comparison for one event's near-money ladder.

    `rungs_with_history` is `(rung, [(as_of_date, yes_bid, yes_ask), ...])` per strike — the
    daily price path for that strike, restricted by the caller to observations strictly before
    the release date (point-in-time; enforced again here defensively).

    For every point-in-time observation where the mid sits in the tradeable band, the "trade"
    always follows the nowcast's implied direction (buy YES at the ask if the nowcast prob is
    above the mid, else buy NO at `1 - yes_bid`) — this reports the P&L of *acting on the signal
    every time it disagrees with the market*, not a cherry-picked subset of winning trades.
    """
    if monthly_series.actual is None or monthly_series.release_date is None:
        raise ValueError(f"{event_ticker}: monthly series has no known actual/release date")
    release_date = monthly_series.release_date
    actual = monthly_series.actual
    path = monthly_series.path_before(release_date)

    observations: list[MarketEdgeObservation] = []
    net_pnl = 0.0
    fees_paid = 0.0

    for rung, history in rungs_with_history:
        if rung.result is None:
            continue
        outcome = rung.result == "yes"
        for as_of, yes_bid, yes_ask in history:
            if as_of >= release_date or yes_ask <= yes_bid:
                continue
            mid = (yes_bid + yes_ask) / 2.0
            if mid < near_money_low or mid > near_money_high:
                continue
            nowcast_reading = _nowcast_at_or_before(path, as_of)
            if nowcast_reading is None:
                continue
            _, nowcast_value = nowcast_reading
            sigma = sigma_curve.sigma_at(horizon_days(release_date, as_of))
            nowcast_prob = NowcastDistribution(
                point_estimate=nowcast_value, sigma=sigma, kind=dist_kind
            ).prob_above(rung.floor_strike)

            if nowcast_prob > mid:
                price = yes_ask
                fee = _fee_dollars(price, fee_rate=fee_rate)
                pnl = (1.0 if outcome else 0.0) - price - fee
            else:
                no_price = 1.0 - yes_bid
                fee = _fee_dollars(no_price, fee_rate=fee_rate)
                pnl = (0.0 if outcome else 1.0) - no_price - fee

            net_pnl += pnl
            fees_paid += fee
            observations.append(
                MarketEdgeObservation(
                    as_of=as_of,
                    strike=rung.floor_strike,
                    kalshi_mid=mid,
                    nowcast_prob=nowcast_prob,
                    outcome=outcome,
                    net_pnl_dollars=pnl,
                )
            )

    nowcast_brier = None
    mid_brier = None
    if observations:
        nowcast_brier = sum(brier_score(o.nowcast_prob, o.outcome) for o in observations) / len(observations)
        mid_brier = sum(brier_score(o.kalshi_mid, o.outcome) for o in observations) / len(observations)

    return MarketEdgeResult(
        event_ticker=event_ticker,
        n_observations=len(observations),
        nowcast_brier=nowcast_brier,
        kalshi_mid_brier=mid_brier,
        net_pnl_dollars=net_pnl,
        fees_paid_dollars=fees_paid,
    )


def _fee_dollars(price_dollars: float, *, fee_rate: float) -> float:
    """`rate * p * (1-p)` taker fee in dollars, reusing the exact production fee model.

    Prices at exactly 0 or 1 are clamped into the model's valid `[0, 1]` domain (a settled/
    resolved-far strike can show a 0.00 or 1.00 quote); the fee at those bounds is 0 either way.
    """
    clamped = min(1.0, max(0.0, price_dollars))
    return float(
        estimate_kalshi_taker_fee_dollars(price_dollars=Decimal(str(clamped)), fee_rate=Decimal(str(fee_rate)))
    )


def _nowcast_at_or_before(path: list[tuple[date, float]], as_of: date) -> tuple[date, float] | None:
    best: tuple[date, float] | None = None
    for reading_date, value in path:
        if reading_date <= as_of:
            best = (reading_date, value)
        else:
            break
    return best


@dataclass(frozen=True, slots=True)
class FillabilitySummary:
    event_ticker: str
    n_near_money_rungs: int
    max_volume: float
    max_open_interest: float
    min_near_money_volume: float
    min_near_money_open_interest: float


def summarize_fillability(
    event_ticker: str, rungs: list[LadderRung], *, near_money_low: float = 0.05, near_money_high: float = 0.95
) -> FillabilitySummary:
    near_money = [r for r in rungs if near_money_low <= r.mid <= near_money_high]
    if not near_money:
        return FillabilitySummary(
            event_ticker=event_ticker,
            n_near_money_rungs=0,
            max_volume=0.0,
            max_open_interest=0.0,
            min_near_money_volume=0.0,
            min_near_money_open_interest=0.0,
        )
    return FillabilitySummary(
        event_ticker=event_ticker,
        n_near_money_rungs=len(near_money),
        max_volume=max(r.volume for r in near_money),
        max_open_interest=max(r.open_interest for r in near_money),
        min_near_money_volume=min(r.volume for r in near_money),
        min_near_money_open_interest=min(r.open_interest for r in near_money),
    )


@dataclass(frozen=True, slots=True)
class Gate0Verdict:
    signal_quality: SignalQualityResult
    market_edge_results: list[MarketEdgeResult]
    fillability_results: list[FillabilitySummary]
    min_brier_margin: float

    @property
    def part1_signal_passes(self) -> bool:
        return self.signal_quality.margin > self.min_brier_margin

    @property
    def part2_market_edge_passes(self) -> bool:
        """Decisive. Requires every event with observations to beat the mid on both Brier and P&L."""
        scored = [r for r in self.market_edge_results if r.n_observations > 0]
        if not scored:
            return False
        return all(r.beats_mid for r in scored)

    @property
    def part3_fillability_passes(self) -> bool:
        scored = [r for r in self.fillability_results if r.n_near_money_rungs > 0]
        if not scored:
            return False
        return all(r.min_near_money_volume > 0 and r.min_near_money_open_interest > 0 for r in scored)

    @property
    def overall_go(self) -> bool:
        """Part 2 is decisive: if it fails, gate-0 fails regardless of parts 1 and 3."""
        if not self.part2_market_edge_passes:
            return False
        return self.part1_signal_passes and self.part3_fillability_passes
