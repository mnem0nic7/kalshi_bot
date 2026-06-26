from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sqlalchemy import bindparam, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import HistoricalSettlementLabelRecord, HistoricalWeatherSnapshotRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.weather.scoring import (
    extract_current_temp_f,
    extract_forecast_high_f,
    extract_gridpoint_max_temp_f,
    gaussian_probability,
)
from kalshi_bot.forecast.calibration_metrics import brier_score_decomposition
from kalshi_bot.weather.intraday import (
    IntradayFeatureInput,
    NUMERIC_INTRADAY_FEATURES,
    calibrate_intraday_probability,
    fit_intraday_calibrator,
    intraday_artifact_fallback_reason,
    intraday_feature_values,
    normalize_weather_operator,
    oriented_weather_delta,
)
from kalshi_bot.weather.sigma_calibration import season_for_month
from kalshi_bot.weather.sigma_calibration import (
    _crps_normal,
    fit_lead_factors,
    fit_sigma_base,
    lead_bucket_for_hours,
    persist_lead_factors,
    persist_sigma_params,
)


PREFERRED_WEATHER_SOURCE_KINDS = (
    "checkpoint_archived_weather_bundle",
    "coverage_repair_checkpoint_promotion",
    "external_forecast_archive_weather_bundle",
    "archived_weather_bundle",
    "captured_weather_bundle",
    "daemon_checkpoint_capture",
)

RESIDUAL_CHECKPOINT_PREFIX = "weather_residual_model"
INTRADAY_CHECKPOINT_PREFIX = "weather_intraday_model"


@dataclass(slots=True)
class PredictionRow:
    series_ticker: str
    station_id: str
    local_market_day: str
    asof_ts: datetime
    settlement_ts: datetime | None
    source_kind: str
    forecast_high_f: float
    actual_high_f: float
    current_temp_f: float | None
    payload: dict[str, Any]

    @property
    def residual_f(self) -> float:
        return self.actual_high_f - self.forecast_high_f


@dataclass(slots=True)
class IntradayTrainingRow:
    market_ticker: str
    series_ticker: str
    station_id: str
    local_market_day: str
    asof_ts: datetime
    settlement_ts: datetime | None
    forecast_updated_ts: datetime | None
    source_kind: str
    threshold_f: float
    operator: str
    outcome_yes: int
    forecast_high_f: float
    current_temp_f: float | None
    observed_high_so_far_f: float | None
    source_disagreement_f: float | None
    timezone_name: str | None
    payload: dict[str, Any]


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _parse_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _metrics(errors: list[float]) -> dict[str, Any]:
    if not errors:
        return {"count": 0}
    arr = np.asarray(errors, dtype=float)
    abs_arr = np.abs(arr)
    return {
        "count": int(arr.size),
        "mae_f": float(np.mean(abs_arr)),
        "rmse_f": float(np.sqrt(np.mean(arr * arr))),
        "bias_actual_minus_forecast_f": float(np.mean(arr)),
        "within_1f_pct": float(np.mean(abs_arr <= 1.0) * 100.0),
        "within_2f_pct": float(np.mean(abs_arr <= 2.0) * 100.0),
        "within_3f_pct": float(np.mean(abs_arr <= 3.0) * 100.0),
        "within_5f_pct": float(np.mean(abs_arr <= 5.0) * 100.0),
    }


def _threshold_from_ticker(market_ticker: str) -> float | None:
    match = re.search(r"-T(\d+)$", market_ticker or "")
    return float(match.group(1)) if match else None


def _result_to_binary(label: HistoricalSettlementLabelRecord) -> int | None:
    result = str(label.crosscheck_result or label.kalshi_result or "").strip().lower()
    if result in {"yes", "win", "1", "true"}:
        return 1
    if result in {"no", "loss", "0", "false"}:
        return 0
    return None


def _weather_features(row: PredictionRow) -> dict[str, float]:
    payload = row.payload or {}
    forecast_payload = payload.get("forecast") or {}
    grid_payload = payload.get("forecast_grid") or {}
    grid_high = extract_gridpoint_max_temp_f(grid_payload, target_date=date.fromisoformat(row.local_market_day)) if grid_payload else None
    daily_high = extract_forecast_high_f(forecast_payload, target_date=date.fromisoformat(row.local_market_day)) if forecast_payload else None
    source_values = [value for value in (grid_high, daily_high) if value is not None]
    disagreement = max(source_values) - min(source_values) if len(source_values) > 1 else 0.0
    observed_high_so_far = row.current_temp_f
    observation_payload = payload.get("observation") or {}
    current_temp = row.current_temp_f
    if current_temp is None:
        current_temp = extract_current_temp_f(observation_payload)
    lead_hours = 0.0
    if row.settlement_ts is not None:
        lead_hours = max(0.0, (_as_utc(row.settlement_ts) - _as_utc(row.asof_ts)).total_seconds() / 3600.0)  # type: ignore[operator]
    forecast_revision_age_hours = 0.0
    return {
        "lead_hours": lead_hours,
        "source_disagreement_f": float(disagreement),
        "forecast_revision_age_hours": forecast_revision_age_hours,
        "current_minus_forecast_f": (
            float(current_temp - row.forecast_high_f) if current_temp is not None else 0.0
        ),
        "observed_high_minus_forecast_f": (
            float(observed_high_so_far - row.forecast_high_f) if observed_high_so_far is not None else 0.0
        ),
    }


def _source_disagreement_from_payload(payload: dict[str, Any], local_market_day: str) -> float:
    try:
        target_date = date.fromisoformat(local_market_day)
    except ValueError:
        target_date = None
    forecast_payload = payload.get("forecast") or {}
    grid_payload = payload.get("forecast_grid") or {}
    grid_high = extract_gridpoint_max_temp_f(grid_payload, target_date=target_date) if grid_payload else None
    daily_high = extract_forecast_high_f(forecast_payload, target_date=target_date) if forecast_payload else None
    source_values = [value for value in (grid_high, daily_high) if value is not None]
    return float(max(source_values) - min(source_values)) if len(source_values) > 1 else 0.0


def _timezone_from_payload(payload: dict[str, Any]) -> str | None:
    mapping = payload.get("mapping") if isinstance(payload.get("mapping"), dict) else {}
    value = mapping.get("timezone_name")
    return str(value) if value else None


def _intraday_feature_input(row: IntradayTrainingRow) -> IntradayFeatureInput:
    return IntradayFeatureInput(
        market_ticker=row.market_ticker,
        series_ticker=row.series_ticker,
        station_id=row.station_id,
        timezone_name=row.timezone_name,
        local_market_day=row.local_market_day,
        threshold_f=row.threshold_f,
        operator=row.operator,
        asof_ts=row.asof_ts,
        forecast_updated_ts=row.forecast_updated_ts,
        settlement_ts=row.settlement_ts,
        forecast_high_f=row.forecast_high_f,
        current_temp_f=row.current_temp_f,
        observed_high_so_far_f=row.observed_high_so_far_f,
        source_disagreement_f=row.source_disagreement_f,
    )


def _baseline_probability_for_intraday_row(row: IntradayTrainingRow) -> float:
    delta = oriented_weather_delta(row.forecast_high_f, row.threshold_f, row.operator)
    return gaussian_probability(delta, sigma_f=3.5)


def _brier_score(probabilities: list[float], outcomes: list[int]) -> float | None:
    if not probabilities:
        return None
    arr = np.asarray(probabilities, dtype=float)
    y = np.asarray(outcomes, dtype=float)
    return float(np.mean((arr - y) ** 2))


def _calibration_buckets(probabilities: list[float], outcomes: list[int]) -> list[dict[str, Any]]:
    buckets: list[dict[str, Any]] = []
    for index in range(5):
        lo = index / 5
        hi = (index + 1) / 5
        selected = [
            (probability, outcome)
            for probability, outcome in zip(probabilities, outcomes, strict=True)
            if lo <= probability < hi or (index == 4 and probability == 1.0)
        ]
        if not selected:
            buckets.append({"bucket": f"{lo:.1f}-{hi:.1f}", "count": 0})
            continue
        avg_probability = float(np.mean([item[0] for item in selected]))
        observed_rate = float(np.mean([item[1] for item in selected]))
        buckets.append(
            {
                "bucket": f"{lo:.1f}-{hi:.1f}",
                "count": len(selected),
                "avg_probability": avg_probability,
                "observed_rate": observed_rate,
                "abs_error": abs(avg_probability - observed_rate),
            }
        )
    return buckets


class WeatherPredictionService:
    def __init__(self, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    @staticmethod
    def residual_checkpoint_name(kalshi_env: str) -> str:
        return f"{RESIDUAL_CHECKPOINT_PREFIX}:{kalshi_env}:active"

    @staticmethod
    def intraday_checkpoint_name(kalshi_env: str) -> str:
        return f"{INTRADAY_CHECKPOINT_PREFIX}:{kalshi_env}:active"

    async def load_active_residual_model(self, *, kalshi_env: str | None = None) -> dict[str, Any] | None:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            checkpoint = await repo.get_checkpoint(self.residual_checkpoint_name(env))
            if checkpoint is None:
                return None
            payload = dict(checkpoint.payload or {})
            if not payload.get("active"):
                return None
            created_at = _parse_utc_datetime(payload.get("created_at"))
            max_age = timedelta(hours=max(1, int(self.settings.weather_residual_model_max_age_hours)))
            if created_at is not None and datetime.now(UTC) - created_at > max_age:
                return None
            return payload

    async def load_active_intraday_model(self, *, kalshi_env: str | None = None) -> dict[str, Any] | None:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            checkpoint = await repo.get_checkpoint(self.intraday_checkpoint_name(env))
            if checkpoint is None:
                return None
            payload = dict(checkpoint.payload or {})
            reason = intraday_artifact_fallback_reason(
                payload,
                max_age_hours=self.settings.weather_intraday_model_max_age_hours,
            )
            return None if reason is not None else payload

    async def status(self, *, kalshi_env: str | None = None) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            residual_checkpoint = await repo.get_checkpoint(self.residual_checkpoint_name(env))
            intraday_checkpoint = await repo.get_checkpoint(self.intraday_checkpoint_name(env))
            intraday_payload = dict(intraday_checkpoint.payload or {}) if intraday_checkpoint is not None else None
            return {
                "kalshi_env": env,
                "residual_model": dict(residual_checkpoint.payload or {}) if residual_checkpoint is not None else None,
                "intraday_model": intraday_payload,
                "intraday_model_usable": (
                    intraday_artifact_fallback_reason(
                        intraday_payload,
                        max_age_hours=self.settings.weather_intraday_model_max_age_hours,
                    )
                    is None
                    if intraday_payload is not None
                    else False
                ),
            }

    async def evaluate(self, *, series: list[str] | None = None) -> dict[str, Any]:
        rows, labels = await self._load_rows(series=series)
        raw_errors = [row.residual_f for row in rows]
        by_station: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            by_station[row.series_ticker].append(row.residual_f)
        label_days = {
            (str(label.series_ticker), str(label.local_market_day))
            for label in labels
            if label.series_ticker is not None and label.crosscheck_high_f is not None
        }
        matched_days = {(row.series_ticker, row.local_market_day) for row in rows}
        result = {
            "row_count": len(rows),
            "series_count": len({row.series_ticker for row in rows}),
            "date_from": min((row.local_market_day for row in rows), default=None),
            "date_to": max((row.local_market_day for row in rows), default=None),
            "overall": _metrics(raw_errors),
            "by_station": {
                station: _metrics(errors)
                for station, errors in sorted(by_station.items())
            },
            "label_count": len(labels),
            "label_series_day_count": len(label_days),
            "matched_series_day_pct": (
                (len(matched_days) / len(label_days)) * 100.0
                if label_days
                else None
            ),
        }
        missing_count = max(0, len(label_days) - len(matched_days))
        if missing_count and len(rows) == 0:
            await self._log_ops_event(
                severity="warning",
                summary="Weather prediction has settlement labels but no matched weather source rows.",
                event_type="missing_source_coverage",
                payload={
                    "label_series_day_count": len(label_days),
                    "matched_series_day_count": len(matched_days),
                    "series": series or [],
                },
            )
        return result

    async def evaluate_intraday_model(self, *, series: list[str] | None = None) -> dict[str, Any]:
        return await self.train_intraday_model(kalshi_env=self.settings.kalshi_env, dry_run=True, series=series)

    async def train_intraday_model(
        self,
        *,
        kalshi_env: str | None = None,
        dry_run: bool = False,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        rows = await self._load_intraday_rows(series=series)
        version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        artifact = self._fit_intraday_artifact(rows, version=version)
        artifact["kalshi_env"] = env
        artifact["dry_run"] = dry_run
        if artifact.get("active") and not dry_run:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=env)
                await repo.set_checkpoint(self.intraday_checkpoint_name(env), artifact.get("version"), artifact)
                await session.commit()
        return artifact

    def _fit_intraday_artifact(self, rows: list[IntradayTrainingRow], *, version: str) -> dict[str, Any]:
        created_at = datetime.now(UTC).isoformat()
        row_count = len(rows)
        unique_markets = len({row.market_ticker for row in rows})
        unique_series_days = len({(row.series_ticker, row.local_market_day) for row in rows})
        base_artifact: dict[str, Any] = {
            "active": False,
            "model_type": "weather_intraday_logistic_v1",
            "version": version,
            "created_at": created_at,
            "row_count": row_count,
            "unique_market_count": unique_markets,
            "unique_series_day_count": unique_series_days,
            "feature_names": [],
            "feature_means": {},
            "feature_scales": {},
            "coefficients": [],
            "intercept": 0.0,
            "metrics": {},
            "gates": {},
        }
        days = sorted({row.local_market_day for row in rows})
        if row_count < self.settings.weather_intraday_min_train_rows or len(days) < 5:
            return {
                **base_artifact,
                "reason": "insufficient_rows",
                "gates": {
                    "min_train_rows": self.settings.weather_intraday_min_train_rows,
                    "observed_rows": row_count,
                    "observed_days": len(days),
                },
            }
        holdout_start = days[max(1, int(len(days) * 0.8))]
        train_rows = [row for row in rows if row.local_market_day < holdout_start]
        holdout_rows = [row for row in rows if row.local_market_day >= holdout_start]
        if len(holdout_rows) < self.settings.weather_intraday_min_holdout_rows:
            return {
                **base_artifact,
                "reason": "insufficient_holdout_rows",
                "gates": {
                    "min_holdout_rows": self.settings.weather_intraday_min_holdout_rows,
                    "observed_holdout_rows": len(holdout_rows),
                    "holdout_start": holdout_start,
                },
            }
        train_days = sorted({row.local_market_day for row in train_rows})
        calibration_start = train_days[max(1, int(len(train_days) * 0.8))] if len(train_days) >= 5 else holdout_start
        fit_rows = [row for row in train_rows if row.local_market_day < calibration_start]
        calibration_rows = [row for row in train_rows if row.local_market_day >= calibration_start]
        if len(fit_rows) < self.settings.weather_intraday_min_holdout_rows or len(set(row.outcome_yes for row in fit_rows)) < 2:
            fit_rows = train_rows
            calibration_rows = train_rows

        y_train = np.asarray([row.outcome_yes for row in fit_rows], dtype=int)
        if len(set(y_train.tolist())) < 2:
            return {**base_artifact, "reason": "single_class_train_rows"}

        feature_names = self._intraday_feature_names(rows)
        x_train = self._intraday_matrix(fit_rows, feature_names)
        x_calibration = self._intraday_matrix(calibration_rows, feature_names)
        x_holdout = self._intraday_matrix(holdout_rows, feature_names)
        means = np.mean(x_train, axis=0)
        scales = np.std(x_train, axis=0)
        scales = np.where(scales == 0.0, 1.0, scales)
        x_train_scaled = (x_train - means) / scales
        x_calibration_scaled = (x_calibration - means) / scales if len(calibration_rows) else x_train_scaled
        x_holdout_scaled = (x_holdout - means) / scales

        model = LogisticRegression(max_iter=500, solver="lbfgs")
        model.fit(x_train_scaled, y_train)
        raw_calibration_probabilities = [float(value) for value in model.predict_proba(x_calibration_scaled)[:, 1]]
        calibration_outcomes = [row.outcome_yes for row in calibration_rows] or [row.outcome_yes for row in fit_rows]
        configured_method = str(self.settings.weather_intraday_calibration_method or "auto").strip().lower()
        calibration_artifact = fit_intraday_calibrator(
            raw_calibration_probabilities,
            calibration_outcomes,
            isotonic_min_rows=self.settings.weather_intraday_isotonic_min_rows,
            method=None if configured_method in {"", "auto"} else configured_method,
        )
        raw_holdout_probabilities = [float(value) for value in model.predict_proba(x_holdout_scaled)[:, 1]]
        model_probabilities = [
            calibrate_intraday_probability(value, calibration_artifact) for value in raw_holdout_probabilities
        ]
        baseline_probabilities = [_baseline_probability_for_intraday_row(row) for row in holdout_rows]
        outcomes = [row.outcome_yes for row in holdout_rows]
        initial_series_regressions = self._intraday_series_regressions(
            holdout_rows,
            model_probabilities,
            baseline_probabilities,
        )
        series_fallbacks = sorted(
            {
                str(row.get("series_ticker"))
                for row in initial_series_regressions
                if float(row.get("brier_delta") or 0.0) > self.settings.weather_intraday_max_series_brier_regression
            }
        )
        if series_fallbacks:
            fallback_set = set(series_fallbacks)
            model_probabilities = [
                baseline_probability if row.series_ticker in fallback_set else model_probability
                for row, model_probability, baseline_probability in zip(
                    holdout_rows,
                    model_probabilities,
                    baseline_probabilities,
                    strict=True,
                )
            ]
        model_brier = _brier_score(model_probabilities, outcomes)
        baseline_brier = _brier_score(baseline_probabilities, outcomes)
        improvement_pct = (
            (baseline_brier - model_brier) / baseline_brier
            if baseline_brier and model_brier is not None
            else 0.0
        )
        calibration = _calibration_buckets(model_probabilities, outcomes)
        checked_buckets = [
            bucket
            for bucket in calibration
            if int(bucket.get("count") or 0) >= self.settings.weather_intraday_min_calibration_bucket_rows
        ]
        max_bucket_error = max((float(bucket.get("abs_error") or 0.0) for bucket in checked_buckets), default=0.0)
        series_regressions = self._intraday_series_regressions(
            holdout_rows,
            model_probabilities,
            baseline_probabilities,
        )
        severe_series_regressions = [
            row
            for row in series_regressions
            if float(row.get("brier_delta") or 0.0) > self.settings.weather_intraday_max_series_brier_regression
        ]
        gates = {
            "min_train_rows": {
                "passed": row_count >= self.settings.weather_intraday_min_train_rows,
                "observed": row_count,
                "required": self.settings.weather_intraday_min_train_rows,
            },
            "min_holdout_rows": {
                "passed": len(holdout_rows) >= self.settings.weather_intraday_min_holdout_rows,
                "observed": len(holdout_rows),
                "required": self.settings.weather_intraday_min_holdout_rows,
            },
            "brier_improvement": {
                "passed": improvement_pct >= self.settings.weather_intraday_min_brier_improvement_pct,
                "observed": improvement_pct,
                "required": self.settings.weather_intraday_min_brier_improvement_pct,
            },
            "calibration": {
                "passed": max_bucket_error <= self.settings.weather_intraday_max_calibration_error,
                "observed_max_bucket_error": max_bucket_error,
                "required_max_bucket_error": self.settings.weather_intraday_max_calibration_error,
                "checked_bucket_count": len(checked_buckets),
            },
            "series_regression": {
                "passed": len(severe_series_regressions) == 0,
                "severe_regression_count": len(severe_series_regressions),
                "max_allowed_brier_delta": self.settings.weather_intraday_max_series_brier_regression,
            },
        }
        active = all(bool(gate.get("passed")) for gate in gates.values())
        return {
            **base_artifact,
            "active": active,
            "reason": "gates_passed" if active else "gates_not_passed",
            "feature_names": feature_names,
            "feature_means": {name: float(value) for name, value in zip(feature_names, means, strict=True)},
            "feature_scales": {name: float(value) for name, value in zip(feature_names, scales, strict=True)},
            "coefficients": [float(value) for value in model.coef_[0]],
            "intercept": float(model.intercept_[0]),
            "calibration": calibration_artifact,
            "series_fallbacks": series_fallbacks,
            "metrics": {
                "train_rows": len(train_rows),
                "fit_rows": len(fit_rows),
                "calibration_rows": len(calibration_rows),
                "holdout_rows": len(holdout_rows),
                "holdout_start": holdout_start,
                "calibration_start": calibration_start,
                "baseline_brier": baseline_brier,
                "model_brier": model_brier,
                "brier_improvement_pct": improvement_pct,
                "score_decomposition": brier_score_decomposition(model_probabilities, outcomes),
                "calibration_buckets": calibration,
                "initial_series_regressions": initial_series_regressions[:20],
                "series_regressions": series_regressions[:20],
            },
            "gates": gates,
        }

    def _intraday_feature_names(self, rows: list[IntradayTrainingRow]) -> list[str]:
        names: set[str] = set(NUMERIC_INTRADAY_FEATURES)
        for row in rows:
            names.update(intraday_feature_values(_intraday_feature_input(row)).keys())
        categorical = sorted(name for name in names if name not in NUMERIC_INTRADAY_FEATURES)
        return [name for name in NUMERIC_INTRADAY_FEATURES if name in names] + categorical

    def _intraday_matrix(self, rows: list[IntradayTrainingRow], feature_names: list[str]) -> np.ndarray:
        matrix = np.zeros((len(rows), len(feature_names)), dtype=float)
        for row_idx, row in enumerate(rows):
            values = intraday_feature_values(_intraday_feature_input(row))
            for col_idx, name in enumerate(feature_names):
                matrix[row_idx, col_idx] = float(values.get(name, 0.0))
        return matrix

    def _intraday_series_regressions(
        self,
        rows: list[IntradayTrainingRow],
        model_probabilities: list[float],
        baseline_probabilities: list[float],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, list[Any]]] = defaultdict(lambda: {"model": [], "baseline": [], "outcomes": []})
        for row, model_probability, baseline_probability in zip(
            rows,
            model_probabilities,
            baseline_probabilities,
            strict=True,
        ):
            bucket = grouped[row.series_ticker]
            bucket["model"].append(model_probability)
            bucket["baseline"].append(baseline_probability)
            bucket["outcomes"].append(row.outcome_yes)
        regressions: list[dict[str, Any]] = []
        for series_ticker, values in grouped.items():
            count = len(values["outcomes"])
            if count < self.settings.weather_intraday_min_series_holdout_rows:
                continue
            model_brier = _brier_score(values["model"], values["outcomes"])
            baseline_brier = _brier_score(values["baseline"], values["outcomes"])
            if model_brier is None or baseline_brier is None:
                continue
            regressions.append(
                {
                    "series_ticker": series_ticker,
                    "count": count,
                    "baseline_brier": baseline_brier,
                    "model_brier": model_brier,
                    "brier_delta": model_brier - baseline_brier,
                }
            )
        regressions.sort(key=lambda row: float(row.get("brier_delta") or 0.0), reverse=True)
        return regressions

    async def refit_sigma(self, *, version: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        rows, _labels = await self._load_rows(series=None)
        version = version or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        by_station_season: dict[tuple[str, str], list[float]] = defaultdict(list)
        by_lead: dict[str, list[float]] = defaultdict(list)
        lead_counts: dict[str, int] = defaultdict(int)
        for row in rows:
            season = season_for_month(int(row.local_market_day[5:7]))
            by_station_season[(row.station_id, season)].append(row.residual_f)
            if row.settlement_ts is not None:
                lead_hours = max(0.0, (_as_utc(row.settlement_ts) - _as_utc(row.asof_ts)).total_seconds() / 3600.0)  # type: ignore[operator]
                bucket = lead_bucket_for_hours(lead_hours)
                by_lead[bucket].append(row.residual_f)
                lead_counts[bucket] += 1

        station_params: dict[tuple[str, str], dict[str, Any]] = {}
        for key, residuals in sorted(by_station_season.items()):
            params = fit_sigma_base(residuals, global_sigma=3.5, global_bias=0.0)
            if params:
                station_params[key] = params
        lead_factors = fit_lead_factors(by_lead)
        result = {
            "version": version,
            "dry_run": dry_run,
            "matched_rows": len(rows),
            "station_sigma_row_count": len(station_params),
            "lead_factor_count": len(lead_factors),
            "qualifying_station_sigma_row_count": sum(
                1
                for params in station_params.values()
                if params.get("sample_count", 0) >= self.settings.sigma_min_samples_beats_global
                and (params.get("crps_improvement_vs_global") or 0.0) > self.settings.sigma_min_crps_improvement
            ),
            "station_sigma": {
                f"{station}:{season}": params
                for (station, season), params in station_params.items()
            },
            "lead_factors": lead_factors,
            "lead_counts": dict(lead_counts),
        }
        if dry_run:
            return result
        async with self.session_factory() as session:
            for (station, season), params in station_params.items():
                await persist_sigma_params(session, station, season, params, version)
            await persist_lead_factors(session, lead_factors, dict(lead_counts), version)
            if result["qualifying_station_sigma_row_count"] == 0:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.log_ops_event(
                    severity="warning",
                    summary="Weather sigma refit produced no qualifying station calibration rows.",
                    source="weather_prediction",
                    payload={
                        "event_type": "stale_or_weak_sigma_fit",
                        "version": version,
                        "matched_rows": len(rows),
                        "station_sigma_row_count": len(station_params),
                        "min_samples_beats_global": self.settings.sigma_min_samples_beats_global,
                    },
                )
            await session.commit()
        return result

    async def station_diagnostics(self, *, min_days: int = 5) -> dict[str, Any]:
        rows, _labels = await self._load_rows(series=None)
        by_station: dict[str, list[PredictionRow]] = defaultdict(list)
        for row in rows:
            by_station[row.series_ticker].append(row)
        diagnostics: list[dict[str, Any]] = []
        for station, station_rows in sorted(by_station.items()):
            if len(station_rows) < min_days:
                continue
            metrics = _metrics([row.residual_f for row in station_rows])
            diagnostics.append(
                {
                    "series_ticker": station,
                    "station_id": station_rows[0].station_id,
                    "day_count": len(station_rows),
                    **metrics,
                }
            )
        diagnostics.sort(key=lambda row: row.get("mae_f") or 0.0, reverse=True)
        weak = [
            row
            for row in diagnostics
            if float(row.get("mae_f") or 0.0) >= 3.0
            or float(row.get("within_3f_pct") or 100.0) < 75.0
        ]
        if weak:
            await self._log_ops_event(
                severity="warning",
                summary=f"Weather station diagnostics flagged {len(weak)} weak station fits.",
                event_type="weak_station_diagnostics",
                payload={"weak_station_count": len(weak), "stations": weak[:10], "min_days": min_days},
            )
        return {"stations": diagnostics, "weak_station_count": len(weak), "min_days": min_days}

    async def train_residual_model(self, *, kalshi_env: str | None = None, dry_run: bool = False) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        rows, labels = await self._load_rows(series=None)
        if len(rows) < 20:
            artifact = {
                "active": False,
                "reason": "insufficient_rows",
                "row_count": len(rows),
                "created_at": datetime.now(UTC).isoformat(),
            }
            if not dry_run:
                await self._persist_residual_artifact(env, artifact)
            return artifact

        rows = sorted(rows, key=lambda row: (row.local_market_day, row.series_ticker, row.asof_ts))
        split = max(1, int(len(rows) * 0.8))
        train_rows = rows[:split]
        holdout_rows = rows[split:]
        feature_names = self._feature_names(rows)
        x_train = self._matrix(train_rows, feature_names)
        y_train = np.asarray([row.residual_f for row in train_rows], dtype=float)
        alpha = 1.0
        xtx = x_train.T @ x_train
        coef = np.linalg.solve(xtx + alpha * np.eye(xtx.shape[0]), x_train.T @ y_train)

        raw_holdout = [row.residual_f for row in holdout_rows]
        adjusted_holdout = [
            row.actual_high_f - (row.forecast_high_f + float((self._matrix([row], feature_names) @ coef)[0]))
            for row in holdout_rows
        ]
        raw_metrics = _metrics(raw_holdout)
        adjusted_metrics = _metrics(adjusted_holdout)
        raw_crps = _crps_normal(3.5, 0.0, raw_holdout)
        adjusted_crps = _crps_normal(3.5, 0.0, adjusted_holdout)
        brier = self._brier_metrics(labels, rows, feature_names, coef, holdout_only=True)
        raw_mae = float(raw_metrics.get("mae_f") or math.inf)
        adjusted_mae = float(adjusted_metrics.get("mae_f") or math.inf)
        mae_improvement_pct = (raw_mae - adjusted_mae) / raw_mae if raw_mae and math.isfinite(raw_mae) else 0.0
        crps_improvement_pct = (
            (raw_crps - adjusted_crps) / raw_crps
            if raw_crps and math.isfinite(raw_crps) and math.isfinite(adjusted_crps)
            else 0.0
        )
        raw_brier = brier.get("raw_brier")
        adjusted_brier = brier.get("adjusted_brier")
        brier_improvement_pct = (
            ((raw_brier - adjusted_brier) / raw_brier)
            if raw_brier and adjusted_brier is not None
            else 0.0
        )
        active = (
            mae_improvement_pct >= self.settings.weather_residual_min_mae_improvement_pct
            and crps_improvement_pct >= self.settings.weather_residual_min_crps_improvement_pct
            and brier_improvement_pct >= self.settings.weather_residual_min_brier_improvement_pct
        )
        artifact = {
            "active": active,
            "version": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
            "created_at": datetime.now(UTC).isoformat(),
            "feature_names": feature_names,
            "coefficients": [float(value) for value in coef],
            "intercept": 0.0,
            "max_abs_adjustment_f": 5.0,
            "metrics": {
                "train_rows": len(train_rows),
                "holdout_rows": len(holdout_rows),
                "raw": raw_metrics,
                "adjusted": adjusted_metrics,
                "mae_improvement_pct": mae_improvement_pct,
                "crps": {
                    "raw_crps": raw_crps,
                    "adjusted_crps": adjusted_crps,
                },
                "crps_improvement_pct": crps_improvement_pct,
                "brier": brier,
                "brier_improvement_pct": brier_improvement_pct,
            },
            "reason": "gates_passed" if active else "gates_not_passed",
        }
        if not dry_run:
            await self._persist_residual_artifact(env, artifact)
        return artifact

    async def _persist_residual_artifact(self, env: str, artifact: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            await repo.set_checkpoint(self.residual_checkpoint_name(env), artifact.get("version"), artifact)
            if not artifact.get("active"):
                await repo.log_ops_event(
                    severity="warning",
                    summary="Weather residual model failed quality gates; runtime will use ensemble and sigma fallback.",
                    source="weather_prediction",
                    payload={
                        "event_type": "residual_model_fallback",
                        "kalshi_env": env,
                        "reason": artifact.get("reason"),
                        "metrics": artifact.get("metrics") or {},
                    },
                )
            await session.commit()

    async def _log_ops_event(
        self,
        *,
        severity: str,
        summary: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            await repo.log_ops_event(
                severity=severity,
                summary=summary,
                source="weather_prediction",
                payload={"event_type": event_type, **payload},
            )
            await session.commit()

    def _feature_names(self, rows: list[PredictionRow]) -> list[str]:
        stations = sorted({f"station:{row.station_id}" for row in rows if row.station_id})
        seasons = sorted({f"season:{season_for_month(int(row.local_market_day[5:7]))}" for row in rows})
        return [
            "lead_hours",
            "source_disagreement_f",
            "forecast_revision_age_hours",
            "current_minus_forecast_f",
            "observed_high_minus_forecast_f",
            *stations,
            *seasons,
        ]

    def _matrix(self, rows: list[PredictionRow], feature_names: list[str]) -> np.ndarray:
        matrix = np.zeros((len(rows), len(feature_names)), dtype=float)
        for row_idx, row in enumerate(rows):
            base = _weather_features(row)
            base[f"station:{row.station_id}"] = 1.0
            base[f"season:{season_for_month(int(row.local_market_day[5:7]))}"] = 1.0
            for col_idx, name in enumerate(feature_names):
                matrix[row_idx, col_idx] = float(base.get(name, 0.0))
        return matrix

    def _brier_metrics(
        self,
        labels: list[HistoricalSettlementLabelRecord],
        rows: list[PredictionRow],
        feature_names: list[str],
        coef: np.ndarray,
        *,
        holdout_only: bool,
    ) -> dict[str, Any]:
        by_key = {(row.series_ticker, row.local_market_day): row for row in rows}
        holdout_start = rows[int(len(rows) * 0.8)].local_market_day if holdout_only and len(rows) >= 5 else None
        raw_losses: list[float] = []
        adjusted_losses: list[float] = []
        for label in labels:
            if label.series_ticker is None or holdout_start is not None and label.local_market_day < holdout_start:
                continue
            row = by_key.get((label.series_ticker, label.local_market_day))
            if row is None:
                continue
            threshold = _threshold_from_ticker(label.market_ticker)
            outcome = _result_to_binary(label)
            if threshold is None or outcome is None:
                continue
            adjustment = float((self._matrix([row], feature_names) @ coef)[0])
            raw_prob = gaussian_probability(row.forecast_high_f - threshold, sigma_f=3.5)
            adjusted_prob = gaussian_probability(row.forecast_high_f + adjustment - threshold, sigma_f=3.5)
            raw_losses.append((raw_prob - outcome) ** 2)
            adjusted_losses.append((adjusted_prob - outcome) ** 2)
        if not raw_losses:
            return {"count": 0, "raw_brier": None, "adjusted_brier": None}
        return {
            "count": len(raw_losses),
            "raw_brier": float(np.mean(raw_losses)),
            "adjusted_brier": float(np.mean(adjusted_losses)),
        }

    async def _load_rows(
        self,
        *,
        series: list[str] | None,
    ) -> tuple[list[PredictionRow], list[HistoricalSettlementLabelRecord]]:
        async with self.session_factory() as session:
            label_stmt = select(HistoricalSettlementLabelRecord).where(
                HistoricalSettlementLabelRecord.crosscheck_high_f.is_not(None),
                HistoricalSettlementLabelRecord.series_ticker.is_not(None),
            )
            if series:
                label_stmt = label_stmt.where(HistoricalSettlementLabelRecord.series_ticker.in_(series))
            labels = list((await session.execute(label_stmt)).scalars())

            if session.get_bind().dialect.name == "postgresql":
                rows = await self._load_rows_postgres(session, series=series)
                return rows, labels

            label_days: dict[tuple[str, str], dict[str, Any]] = {}
            highs_by_day: dict[tuple[str, str], set[float]] = defaultdict(set)
            for label in labels:
                key = (str(label.series_ticker), str(label.local_market_day))
                highs_by_day[key].add(float(label.crosscheck_high_f))
                existing = label_days.setdefault(
                    key,
                    {"settlement_ts": label.settlement_ts, "actual_high_f": float(label.crosscheck_high_f)},
                )
                if label.settlement_ts is not None and (
                    existing.get("settlement_ts") is None or label.settlement_ts > existing["settlement_ts"]
                ):
                    existing["settlement_ts"] = label.settlement_ts
            label_days = {key: value for key, value in label_days.items() if len(highs_by_day[key]) == 1}

            weather_stmt = select(HistoricalWeatherSnapshotRecord).where(
                HistoricalWeatherSnapshotRecord.forecast_high_f.is_not(None),
                HistoricalWeatherSnapshotRecord.series_ticker.is_not(None),
                HistoricalWeatherSnapshotRecord.source_kind.in_(PREFERRED_WEATHER_SOURCE_KINDS),
            )
            if series:
                weather_stmt = weather_stmt.where(HistoricalWeatherSnapshotRecord.series_ticker.in_(series))
            if label_days:
                weather_stmt = weather_stmt.where(
                    tuple_(
                        HistoricalWeatherSnapshotRecord.series_ticker,
                        HistoricalWeatherSnapshotRecord.local_market_day,
                    ).in_(list(label_days.keys()))
                )
            snapshots = list((await session.execute(weather_stmt)).scalars())

        grouped: dict[tuple[str, str], list[HistoricalWeatherSnapshotRecord]] = defaultdict(list)
        for snapshot in snapshots:
            key = (str(snapshot.series_ticker), str(snapshot.local_market_day))
            if key in label_days:
                grouped[key].append(snapshot)

        source_rank = {source: index for index, source in enumerate(PREFERRED_WEATHER_SOURCE_KINDS)}
        rows: list[PredictionRow] = []
        for key, day_snapshots in grouped.items():
            day = label_days[key]
            settlement_ts = _as_utc(day.get("settlement_ts"))
            eligible = [
                snapshot
                for snapshot in day_snapshots
                if settlement_ts is None or _as_utc(snapshot.asof_ts) <= settlement_ts
            ]
            if not eligible:
                continue
            best = sorted(
                eligible,
                key=lambda snapshot: (
                    source_rank.get(snapshot.source_kind, 999),
                    -(_as_utc(snapshot.asof_ts) or datetime.min.replace(tzinfo=UTC)).timestamp(),
                    -int(getattr(snapshot, "id", 0) or 0),
                ),
            )[0]
            rows.append(
                PredictionRow(
                    series_ticker=key[0],
                    station_id=best.station_id,
                    local_market_day=key[1],
                    asof_ts=_as_utc(best.asof_ts) or best.asof_ts,
                    settlement_ts=settlement_ts,
                    source_kind=best.source_kind,
                    forecast_high_f=float(best.forecast_high_f),
                    actual_high_f=float(day["actual_high_f"]),
                    current_temp_f=float(best.current_temp_f) if best.current_temp_f is not None else None,
                    payload=dict(best.payload or {}),
                )
            )
        return rows, labels

    async def _load_intraday_rows(self, *, series: list[str] | None) -> list[IntradayTrainingRow]:
        async with self.session_factory() as session:
            if session.get_bind().dialect.name == "postgresql":
                return await self._load_intraday_rows_postgres(session, series=series)

            label_stmt = select(HistoricalSettlementLabelRecord).where(
                HistoricalSettlementLabelRecord.crosscheck_high_f.is_not(None),
                HistoricalSettlementLabelRecord.series_ticker.is_not(None),
            )
            if series:
                label_stmt = label_stmt.where(HistoricalSettlementLabelRecord.series_ticker.in_(series))
            labels = list((await session.execute(label_stmt)).scalars())
            weather_stmt = select(HistoricalWeatherSnapshotRecord).where(
                HistoricalWeatherSnapshotRecord.forecast_high_f.is_not(None),
                HistoricalWeatherSnapshotRecord.series_ticker.is_not(None),
                HistoricalWeatherSnapshotRecord.source_kind.in_(PREFERRED_WEATHER_SOURCE_KINDS),
            )
            if series:
                weather_stmt = weather_stmt.where(HistoricalWeatherSnapshotRecord.series_ticker.in_(series))
            snapshots = list((await session.execute(weather_stmt)).scalars())

        snapshots_by_key: dict[tuple[str, str], list[HistoricalWeatherSnapshotRecord]] = defaultdict(list)
        for snapshot in snapshots:
            snapshots_by_key[(str(snapshot.series_ticker), str(snapshot.local_market_day))].append(snapshot)
        rows: list[IntradayTrainingRow] = []
        for key, items in snapshots_by_key.items():
            running_high: float | None = None
            ranked_by_hour: dict[datetime, tuple[HistoricalWeatherSnapshotRecord, float | None]] = {}
            for snapshot in sorted(items, key=lambda item: (_as_utc(item.asof_ts) or item.asof_ts, int(item.id or 0))):
                current = float(snapshot.current_temp_f) if snapshot.current_temp_f is not None else None
                if current is not None:
                    running_high = current if running_high is None else max(running_high, current)
                hour_key = (_as_utc(snapshot.asof_ts) or snapshot.asof_ts).replace(minute=0, second=0, microsecond=0)
                ranked_by_hour[hour_key] = (snapshot, running_high)
            labels_for_key = [
                label
                for label in labels
                if (str(label.series_ticker), str(label.local_market_day)) == key
            ]
            for label in labels_for_key:
                rows.extend(
                    self._intraday_rows_from_label_and_snapshots(
                        label=label,
                        snapshots=list(ranked_by_hour.values()),
                    )
                )
        rows.sort(key=lambda row: (row.local_market_day, row.market_ticker, row.asof_ts))
        return rows

    async def _load_intraday_rows_postgres(
        self,
        session: AsyncSession,
        *,
        series: list[str] | None,
    ) -> list[IntradayTrainingRow]:
        label_filter = "AND series_ticker IN :series" if series else ""
        ranked_filter = "AND l.series_ticker IN :series" if series else ""
        source_case = "\n".join(
            f"                    WHEN '{source}' THEN {index}"
            for index, source in enumerate(PREFERRED_WEATHER_SOURCE_KINDS)
        )
        stmt = text(
            f"""
            WITH labels AS (
                SELECT
                    market_ticker,
                    series_ticker,
                    local_market_day,
                    settlement_ts,
                    crosscheck_result,
                    kalshi_result,
                    payload
                FROM historical_settlement_labels
                WHERE crosscheck_high_f IS NOT NULL
                  AND series_ticker IS NOT NULL
                  {label_filter}
            ),
            snapshots AS (
                SELECT
                    h.*,
                    max(h.current_temp_f::float) OVER (
                        PARTITION BY h.station_id, h.local_market_day
                        ORDER BY h.asof_ts ASC, h.id ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS observed_high_so_far_f
                FROM historical_weather_snapshots h
                WHERE h.forecast_high_f IS NOT NULL
                  AND h.series_ticker IS NOT NULL
                  AND h.source_kind IN :sources
            ),
            ranked AS (
                SELECT
                    l.market_ticker,
                    l.crosscheck_result,
                    l.kalshi_result,
                    l.payload AS label_payload,
                    h.series_ticker,
                    h.station_id,
                    h.local_market_day,
                    h.asof_ts,
                    l.settlement_ts,
                    h.forecast_updated_ts,
                    h.source_kind,
                    h.forecast_high_f,
                    h.current_temp_f,
                    h.observed_high_so_far_f,
                    h.payload AS weather_payload,
                    row_number() OVER (
                        PARTITION BY l.market_ticker, date_trunc('hour', h.asof_ts)
                        ORDER BY
                            CASE h.source_kind
{source_case}
                                ELSE 999
                            END ASC,
                            h.asof_ts DESC,
                            h.id DESC
                    ) AS rank
                FROM labels l
                JOIN snapshots h
                  ON l.series_ticker = h.series_ticker
                 AND l.local_market_day = h.local_market_day
                WHERE (l.settlement_ts IS NULL OR h.asof_ts <= l.settlement_ts)
                  {ranked_filter}
            )
            SELECT *
            FROM ranked
            WHERE rank = 1
            ORDER BY local_market_day ASC, market_ticker ASC, asof_ts ASC
            """
        ).bindparams(bindparam("sources", expanding=True))
        params: dict[str, Any] = {"sources": list(PREFERRED_WEATHER_SOURCE_KINDS)}
        if series:
            stmt = stmt.bindparams(bindparam("series", expanding=True))
            params["series"] = series
        result = await session.execute(stmt, params)
        rows: list[IntradayTrainingRow] = []
        for mapping in result.mappings():
            row = self._intraday_row_from_mapping(mapping)
            if row is not None:
                rows.append(row)
        return rows

    def _intraday_row_from_mapping(self, mapping: Any) -> IntradayTrainingRow | None:
        threshold = _threshold_from_ticker(str(mapping["market_ticker"]))
        outcome = _result_to_binary(
            HistoricalSettlementLabelRecord(
                market_ticker=str(mapping["market_ticker"]),
                local_market_day=str(mapping["local_market_day"]),
                source_kind="training_row",
                crosscheck_result=mapping.get("crosscheck_result"),
                kalshi_result=mapping.get("kalshi_result"),
            )
        )
        if threshold is None or outcome is None:
            return None
        label_payload = dict(mapping.get("label_payload") or {})
        weather_payload = dict(mapping.get("weather_payload") or {})
        operator = normalize_weather_operator(None, market_payload=label_payload)
        return IntradayTrainingRow(
            market_ticker=str(mapping["market_ticker"]),
            series_ticker=str(mapping["series_ticker"]),
            station_id=str(mapping["station_id"]),
            local_market_day=str(mapping["local_market_day"]),
            asof_ts=_as_utc(mapping["asof_ts"]) or mapping["asof_ts"],
            settlement_ts=_as_utc(mapping["settlement_ts"]),
            forecast_updated_ts=_as_utc(mapping["forecast_updated_ts"]),
            source_kind=str(mapping["source_kind"]),
            threshold_f=float(threshold),
            operator=operator,
            outcome_yes=int(outcome),
            forecast_high_f=float(mapping["forecast_high_f"]),
            current_temp_f=float(mapping["current_temp_f"]) if mapping["current_temp_f"] is not None else None,
            observed_high_so_far_f=(
                float(mapping["observed_high_so_far_f"]) if mapping["observed_high_so_far_f"] is not None else None
            ),
            source_disagreement_f=_source_disagreement_from_payload(weather_payload, str(mapping["local_market_day"])),
            timezone_name=_timezone_from_payload(weather_payload),
            payload=weather_payload,
        )

    def _intraday_rows_from_label_and_snapshots(
        self,
        *,
        label: HistoricalSettlementLabelRecord,
        snapshots: list[tuple[HistoricalWeatherSnapshotRecord, float | None]],
    ) -> list[IntradayTrainingRow]:
        threshold = _threshold_from_ticker(label.market_ticker)
        outcome = _result_to_binary(label)
        if threshold is None or outcome is None:
            return []
        operator = normalize_weather_operator(None, market_payload=dict(label.payload or {}))
        rows: list[IntradayTrainingRow] = []
        for snapshot, high_so_far in snapshots:
            if label.settlement_ts is not None and _as_utc(snapshot.asof_ts) and _as_utc(snapshot.asof_ts) > _as_utc(label.settlement_ts):  # type: ignore[operator]
                continue
            payload = dict(snapshot.payload or {})
            rows.append(
                IntradayTrainingRow(
                    market_ticker=label.market_ticker,
                    series_ticker=str(label.series_ticker),
                    station_id=snapshot.station_id,
                    local_market_day=label.local_market_day,
                    asof_ts=_as_utc(snapshot.asof_ts) or snapshot.asof_ts,
                    settlement_ts=_as_utc(label.settlement_ts),
                    forecast_updated_ts=_as_utc(snapshot.forecast_updated_ts),
                    source_kind=snapshot.source_kind,
                    threshold_f=float(threshold),
                    operator=operator,
                    outcome_yes=int(outcome),
                    forecast_high_f=float(snapshot.forecast_high_f),
                    current_temp_f=float(snapshot.current_temp_f) if snapshot.current_temp_f is not None else None,
                    observed_high_so_far_f=high_so_far,
                    source_disagreement_f=_source_disagreement_from_payload(payload, label.local_market_day),
                    timezone_name=_timezone_from_payload(payload),
                    payload=payload,
                )
            )
        return rows

    async def _load_rows_postgres(
        self,
        session: AsyncSession,
        *,
        series: list[str] | None,
    ) -> list[PredictionRow]:
        label_filter = "AND series_ticker IN :series" if series else ""
        ranked_filter = "AND l.series_ticker IN :series" if series else ""
        source_case = "\n".join(
            f"                    WHEN '{source}' THEN {index}"
            for index, source in enumerate(PREFERRED_WEATHER_SOURCE_KINDS)
        )
        stmt = text(
            f"""
            WITH labels AS (
                SELECT
                    series_ticker,
                    local_market_day,
                    min(crosscheck_high_f::float) AS actual_high_f,
                    max(settlement_ts) AS settlement_ts,
                    count(DISTINCT crosscheck_high_f) AS high_count
                FROM historical_settlement_labels
                WHERE crosscheck_high_f IS NOT NULL
                  AND series_ticker IS NOT NULL
                  {label_filter}
                GROUP BY series_ticker, local_market_day
                HAVING count(DISTINCT crosscheck_high_f) = 1
            ),
            ranked AS (
                SELECT
                    h.series_ticker,
                    h.station_id,
                    h.local_market_day,
                    h.asof_ts,
                    l.settlement_ts,
                    h.source_kind,
                    h.forecast_high_f,
                    l.actual_high_f,
                    h.current_temp_f,
                    h.payload,
                    row_number() OVER (
                        PARTITION BY h.series_ticker, h.local_market_day
                        ORDER BY
                            CASE h.source_kind
{source_case}
                                ELSE 999
                            END ASC,
                            h.asof_ts DESC,
                            h.id DESC
                    ) AS rank
                FROM historical_weather_snapshots h
                JOIN labels l
                  ON l.series_ticker = h.series_ticker
                 AND l.local_market_day = h.local_market_day
                WHERE h.forecast_high_f IS NOT NULL
                  AND h.series_ticker IS NOT NULL
                  AND h.source_kind IN :sources
                  {ranked_filter}
                  AND (l.settlement_ts IS NULL OR h.asof_ts <= l.settlement_ts)
            )
            SELECT
                series_ticker,
                station_id,
                local_market_day,
                asof_ts,
                settlement_ts,
                source_kind,
                forecast_high_f,
                actual_high_f,
                current_temp_f,
                payload
            FROM ranked
            WHERE rank = 1
            ORDER BY local_market_day ASC, series_ticker ASC
            """
        ).bindparams(bindparam("sources", expanding=True))
        params: dict[str, Any] = {"sources": list(PREFERRED_WEATHER_SOURCE_KINDS)}
        if series:
            stmt = stmt.bindparams(bindparam("series", expanding=True))
            params["series"] = series
        result = await session.execute(stmt, params)
        rows: list[PredictionRow] = []
        for mapping in result.mappings():
            rows.append(
                PredictionRow(
                    series_ticker=str(mapping["series_ticker"]),
                    station_id=str(mapping["station_id"]),
                    local_market_day=str(mapping["local_market_day"]),
                    asof_ts=_as_utc(mapping["asof_ts"]) or mapping["asof_ts"],
                    settlement_ts=_as_utc(mapping["settlement_ts"]),
                    source_kind=str(mapping["source_kind"]),
                    forecast_high_f=float(mapping["forecast_high_f"]),
                    actual_high_f=float(mapping["actual_high_f"]),
                    current_temp_f=(
                        float(mapping["current_temp_f"]) if mapping["current_temp_f"] is not None else None
                    ),
                    payload=dict(mapping["payload"] or {}),
                )
            )
        return rows
