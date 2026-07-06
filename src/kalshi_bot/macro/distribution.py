"""Nowcast point-estimate -> ladder probability distribution (pure math, no I/O).

Gate-0 compares three tail-probability models fit from the nowcast-vs-actual vintage history —
Gaussian, Student-t, and an empirical error CDF — and keeps whichever calibrates best on held-out
months (fat tails plausibly matter here, cf. `project_settlement_basis_finding`). All three reduce
to the same public shape: given a nowcast point estimate + a horizon-dependent spread, compute
``P(value > k)`` for a ladder of strikes ``k``. This is monotonic by construction from a single
CDF for the Gaussian/Student-t branches; the empirical branch can violate monotonicity on a small
sample, so `NowcastDistribution.ladder_probabilities` asserts it explicitly and fails loud rather
than silently returning an inconsistent ladder.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from scipy import stats

DistributionKind = Literal["gaussian", "student_t", "empirical"]

DEFAULT_STUDENT_T_DF = 5.0


@dataclass(frozen=True, slots=True)
class SigmaCurvePoint:
    horizon_days: int
    sigma: float


class SigmaCurve:
    """Piecewise-linear sigma(horizon_days), fit from |actual - nowcast_at_horizon| history."""

    def __init__(self, points: Sequence[SigmaCurvePoint]) -> None:
        if not points:
            raise ValueError("SigmaCurve needs at least one point")
        for p in points:
            if p.sigma <= 0:
                raise ValueError(f"sigma must be > 0, got {p.sigma} at horizon {p.horizon_days}")
        self._points = tuple(sorted(points, key=lambda p: p.horizon_days))

    @property
    def points(self) -> tuple[SigmaCurvePoint, ...]:
        return self._points

    def sigma_at(self, horizon_days: int) -> float:
        pts = self._points
        if horizon_days <= pts[0].horizon_days:
            return pts[0].sigma
        if horizon_days >= pts[-1].horizon_days:
            return pts[-1].sigma
        for lo, hi in zip(pts, pts[1:]):
            if lo.horizon_days <= horizon_days <= hi.horizon_days:
                if hi.horizon_days == lo.horizon_days:
                    return lo.sigma
                frac = (horizon_days - lo.horizon_days) / (hi.horizon_days - lo.horizon_days)
                return lo.sigma + frac * (hi.sigma - lo.sigma)
        return pts[-1].sigma  # pragma: no cover - unreachable given the sorted scan above

    @classmethod
    def fit(
        cls,
        errors_by_horizon_bucket: dict[int, Sequence[float]],
        *,
        min_sigma: float = 1e-6,
        min_observations: int = 1,
    ) -> "SigmaCurve":
        """Fit one sigma point per horizon bucket from historical (actual - nowcast) errors.

        Buckets with fewer than `min_observations` residuals are dropped (too noisy to trust);
        callers doing point-in-time backtesting should only pass buckets built from data available
        *before* the date being evaluated.
        """
        points: list[SigmaCurvePoint] = []
        for horizon, errors in errors_by_horizon_bucket.items():
            errors = list(errors)
            if len(errors) < min_observations:
                continue
            n = len(errors)
            mean = sum(errors) / n
            variance = sum((e - mean) ** 2 for e in errors) / n if n > 1 else mean**2
            sigma = max(math.sqrt(variance), min_sigma)
            points.append(SigmaCurvePoint(horizon_days=horizon, sigma=sigma))
        if not points:
            raise ValueError("no horizon buckets with enough observations to fit a SigmaCurve")
        return cls(points)


@dataclass(frozen=True, slots=True)
class NowcastDistribution:
    """``P(value > k)`` for a ladder of strikes, given a nowcast point estimate + spread."""

    point_estimate: float
    sigma: float
    kind: DistributionKind = "gaussian"
    student_t_df: float = DEFAULT_STUDENT_T_DF
    empirical_errors: tuple[float, ...] = field(default=())

    def __post_init__(self) -> None:
        if self.sigma <= 0:
            raise ValueError(f"sigma must be > 0, got {self.sigma}")
        if self.kind == "empirical" and not self.empirical_errors:
            raise ValueError("kind='empirical' requires non-empty empirical_errors")

    def prob_above(self, strike: float) -> float:
        """``P(actual > strike)``, clamped to ``[0, 1]``."""
        if self.kind == "gaussian":
            z = (strike - self.point_estimate) / self.sigma
            p = 1.0 - _gaussian_cdf(z)
        elif self.kind == "student_t":
            z = (strike - self.point_estimate) / self.sigma
            p = 1.0 - float(stats.t.cdf(z, df=self.student_t_df))
        elif self.kind == "empirical":
            p = _empirical_survival(strike, self.point_estimate, self.empirical_errors)
        else:  # pragma: no cover - Literal keeps this unreachable in typed callers
            raise ValueError(f"unknown distribution kind: {self.kind}")
        return min(1.0, max(0.0, p))

    def ladder_probabilities(self, strikes: Sequence[float]) -> list[float]:
        """``P(actual > k)`` for every strike, asserted non-increasing in ``k``."""
        probs = [self.prob_above(k) for k in strikes]
        _assert_monotonic_non_increasing(strikes, probs)
        return probs


def _gaussian_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _empirical_survival(strike: float, point_estimate: float, errors: tuple[float, ...]) -> float:
    """Resample the raw (actual - nowcast) residual distribution rather than a scaled CDF.

    ``P(actual > strike) = P(actual - point_estimate > strike - point_estimate)
                         = P(error > strike - point_estimate)``
    estimated as the empirical exceedance fraction over historical residuals.
    """
    threshold = strike - point_estimate
    n = len(errors)
    exceed = sum(1 for e in errors if e > threshold)
    return exceed / n


def _assert_monotonic_non_increasing(strikes: Sequence[float], probs: Sequence[float]) -> None:
    order = sorted(range(len(strikes)), key=lambda i: strikes[i])
    ordered = [probs[i] for i in order]
    for lower, higher in zip(ordered, ordered[1:]):
        if higher > lower + 1e-9:
            raise AssertionError(f"ladder probabilities not monotonic non-increasing: {ordered}")
