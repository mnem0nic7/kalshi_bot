"""Weather-signal calibration metrics: Brier score, log-loss, reliability curve.

Measures the quality of ``score_weather_market`` fair-value predictions against
settled Kalshi outcomes. Consumes the signals persisted per replay room and
joins them to ``HistoricalSettlementLabelRecord``.

This module is a **measurement addition only** — it does not change the signal
or risk engine. Use the results to decide whether the win-rate auto-adjust loop
(``StrategyEvaluationService``) is being steered by a well-calibrated signal or
just lucky fills.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Iterable, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.db.models import (
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    Room,
    Signal,
)

_LOG_LOSS_EPS = 1e-15
_DEFAULT_BUCKETS = 10


@dataclass(slots=True, frozen=True)
class ReliabilityBucket:
    """One (p, y) bucket on the reliability diagram.

    ``predicted_mean`` and ``observed_frequency`` should roughly coincide on the
    diagonal for a well-calibrated signal.
    """

    lower: float
    upper: float
    count: int
    predicted_mean: float
    observed_frequency: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "lower": self.lower,
            "upper": self.upper,
            "count": self.count,
            "predicted_mean": self.predicted_mean,
            "observed_frequency": self.observed_frequency,
        }


@dataclass(slots=True, frozen=True)
class CalibrationSummary:
    n: int
    brier_score: float | None
    log_loss: float | None
    reliability: list[ReliabilityBucket] = field(default_factory=list)
    bucket_label: str | None = None  # e.g. "KXHIGHNY" or "2026-04"

    def to_dict(self) -> dict[str, object]:
        return {
            "n": self.n,
            "brier_score": self.brier_score,
            "log_loss": self.log_loss,
            "reliability": [b.to_dict() for b in self.reliability],
            "bucket_label": self.bucket_label,
        }


def brier_score(predictions: Sequence[float], outcomes: Sequence[int]) -> float | None:
    """Mean squared error between predicted probability and realized outcome.

    0 = perfect. 0.25 = uninformative (constant 0.5). Higher = worse.
    """
    if not predictions:
        return None
    if len(predictions) != len(outcomes):
        raise ValueError("predictions and outcomes must have equal length")
    total = 0.0
    for p, y in zip(predictions, outcomes):
        total += (p - y) ** 2
    return total / len(predictions)


def log_loss(predictions: Sequence[float], outcomes: Sequence[int]) -> float | None:
    """Negative log-likelihood. Probabilities are clipped away from {0,1}."""
    if not predictions:
        return None
    if len(predictions) != len(outcomes):
        raise ValueError("predictions and outcomes must have equal length")
    total = 0.0
    for p, y in zip(predictions, outcomes):
        clipped = min(max(p, _LOG_LOSS_EPS), 1.0 - _LOG_LOSS_EPS)
        total += -(y * math.log(clipped) + (1 - y) * math.log(1.0 - clipped))
    return total / len(predictions)


def reliability_curve(
    predictions: Sequence[float],
    outcomes: Sequence[int],
    *,
    n_buckets: int = _DEFAULT_BUCKETS,
) -> list[ReliabilityBucket]:
    """Equal-width reliability diagram. Empty buckets are omitted from output."""
    if n_buckets <= 0:
        raise ValueError("n_buckets must be positive")
    if len(predictions) != len(outcomes):
        raise ValueError("predictions and outcomes must have equal length")
    width = 1.0 / n_buckets
    sums_p: list[float] = [0.0] * n_buckets
    sums_y: list[float] = [0.0] * n_buckets
    counts: list[int] = [0] * n_buckets
    for p, y in zip(predictions, outcomes):
        # Clamp to [0,1] in case a predicted probability leaked slightly out.
        clamped = min(max(p, 0.0), 1.0)
        # Last bucket is inclusive on the right so p=1.0 lands in n_buckets-1.
        idx = min(int(clamped / width), n_buckets - 1)
        sums_p[idx] += clamped
        sums_y[idx] += y
        counts[idx] += 1
    buckets: list[ReliabilityBucket] = []
    for i in range(n_buckets):
        if counts[i] == 0:
            continue
        buckets.append(
            ReliabilityBucket(
                lower=i * width,
                upper=(i + 1) * width,
                count=counts[i],
                predicted_mean=sums_p[i] / counts[i],
                observed_frequency=sums_y[i] / counts[i],
            )
        )
    return buckets


def calibration_from_pairs(
    predictions: Sequence[float],
    outcomes: Sequence[int],
    *,
    n_buckets: int = _DEFAULT_BUCKETS,
    bucket_label: str | None = None,
) -> CalibrationSummary:
    """One-shot: compute Brier + log-loss + reliability curve on a paired series."""
    return CalibrationSummary(
        n=len(predictions),
        brier_score=brier_score(predictions, outcomes),
        log_loss=log_loss(predictions, outcomes),
        reliability=reliability_curve(predictions, outcomes, n_buckets=n_buckets),
        bucket_label=bucket_label,
    )


def _outcome_from_kalshi_result(result: str | None) -> int | None:
    if result == "yes":
        return 1
    if result == "no":
        return 0
    return None


def _month_key(local_market_day: str | None) -> str | None:
    """Extract "YYYY-MM" from an ISO-ish "YYYY-MM-DD" day string."""
    if not local_market_day or len(local_market_day) < 7:
        return None
    return local_market_day[:7]


class SignalCalibrationService:
    """Pulls signal/settlement pairs from Postgres and aggregates them.

    Only signals with a settled outcome contribute — an unresolved market tells
    us nothing about the signal's calibration.
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def _load_pairs(
        self,
        *,
        date_from: date | None,
        date_to: date | None,
        series_ticker: str | None,
    ) -> list[dict[str, object]]:
        """Return rows with fair_yes, outcome (0/1), series_ticker, local_market_day."""
        stmt = (
            select(
                Signal.fair_yes_dollars,
                HistoricalReplayRunRecord.series_ticker,
                HistoricalReplayRunRecord.local_market_day,
                HistoricalSettlementLabelRecord.kalshi_result,
            )
            .select_from(Signal)
            .join(Room, Room.id == Signal.room_id)
            .join(HistoricalReplayRunRecord, HistoricalReplayRunRecord.room_id == Room.id)
            .join(
                HistoricalSettlementLabelRecord,
                HistoricalSettlementLabelRecord.market_ticker == Signal.market_ticker,
            )
            .where(HistoricalSettlementLabelRecord.kalshi_result.in_(("yes", "no")))
        )
        if series_ticker is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.series_ticker == series_ticker)
        if date_from is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.local_market_day >= date_from.isoformat())
        if date_to is not None:
            stmt = stmt.where(HistoricalReplayRunRecord.local_market_day <= date_to.isoformat())

        rows: list[dict[str, object]] = []
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            for fair_yes, series, day, kalshi_result in result.all():
                outcome = _outcome_from_kalshi_result(kalshi_result)
                if outcome is None or fair_yes is None:
                    continue
                rows.append(
                    {
                        "prediction": float(fair_yes),
                        "outcome": outcome,
                        "series_ticker": series,
                        "local_market_day": day,
                    }
                )
        return rows

    async def compute_overall(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        series_ticker: str | None = None,
        n_buckets: int = _DEFAULT_BUCKETS,
    ) -> CalibrationSummary:
        pairs = await self._load_pairs(
            date_from=date_from, date_to=date_to, series_ticker=series_ticker
        )
        return calibration_from_pairs(
            [p["prediction"] for p in pairs],  # type: ignore[arg-type]
            [p["outcome"] for p in pairs],  # type: ignore[arg-type]
            n_buckets=n_buckets,
            bucket_label=series_ticker,
        )

    async def compute_per_series(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        n_buckets: int = _DEFAULT_BUCKETS,
    ) -> list[CalibrationSummary]:
        pairs = await self._load_pairs(
            date_from=date_from, date_to=date_to, series_ticker=None
        )
        return _bucketed(pairs, key="series_ticker", n_buckets=n_buckets)

    async def compute_per_month(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        series_ticker: str | None = None,
        n_buckets: int = _DEFAULT_BUCKETS,
    ) -> list[CalibrationSummary]:
        pairs = await self._load_pairs(
            date_from=date_from, date_to=date_to, series_ticker=series_ticker
        )
        # Rewrite the bucket key to the month derived from local_market_day.
        enriched = []
        for p in pairs:
            month = _month_key(p.get("local_market_day"))  # type: ignore[arg-type]
            if month is None:
                continue
            enriched.append({**p, "_month": month})
        return _bucketed(enriched, key="_month", n_buckets=n_buckets)


def _bucketed(
    pairs: Iterable[dict[str, object]],
    *,
    key: str,
    n_buckets: int,
) -> list[CalibrationSummary]:
    """Group pairs by the named key and compute one summary per group."""
    groups: dict[str, tuple[list[float], list[int]]] = {}
    for p in pairs:
        k = p.get(key)
        if k is None:
            continue
        label = str(k)
        preds, ys = groups.setdefault(label, ([], []))
        preds.append(float(p["prediction"]))  # type: ignore[arg-type]
        ys.append(int(p["outcome"]))  # type: ignore[arg-type]
    summaries: list[CalibrationSummary] = []
    for label in sorted(groups):
        preds, ys = groups[label]
        summaries.append(
            calibration_from_pairs(preds, ys, n_buckets=n_buckets, bucket_label=label)
        )
    return summaries
