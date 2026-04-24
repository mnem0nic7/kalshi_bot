from __future__ import annotations

"""Decision-corpus calibration report generator.

This module is a read-only PR 1.5 consumer of the PR 1 decision corpus.
It evaluates settlement-model calibration: whether ``fair_yes_dollars``
matches binary settlement outcomes. It does not evaluate policy profitability,
ranking quality, or execution quality.

Calibration-cell invariant:
    One valid primary row contributes to exactly one calibration cell, assigned
    by its recorded ``support_level``. Cells are disjoint. This report does not
    recompute PR 1 backoff logic and does not aggregate rows into parent cells.

Terminology:
    A support bucket is PR 1's build-time backoff bucket. It may contain rows
    that eventually received more-specific support levels. A calibration cell
    is PR 1.5's aggregation unit: rows whose recorded ``support_level`` matches
    the cell level, grouped by the dimensions for that level.

JSON schema v1:
    {
      "report_metadata": {...},
      "coverage": {
        "row_counts": {...},
        "source_provenance": [...],
        "support_status": [...],
        "support_level": [...],
        "skips": [...]
      },
      "aggregates": {
        "primary": {...},
        "descriptive": {...},
        "degraded_provenance": {...}
      },
      "reliability_curve": [
        {
          "bucket_lower": float,
          "bucket_upper": float,
          "bucket_label": str,
          "predicted_mean": float | None,
          "observed_rate": float | None,
          "n": int,
          "support_band": "high" | "medium" | "low"
        }
      ],
      "cells": [
        {
          "cell_key": dict,
          "support_level": str,
          "cell_n": int,
          "build_support_n": int,
          "cell_support_status": str,
          "build_support_status": str,
          "brier": float,
          "log_loss": float,
          ...
        }
      ],
      "markdown": {"json_filename": str, "markdown_filename": str}
    }

Fields documented in schema v1 are stable. Future fields may be added; existing
fields are not renamed or removed without bumping ``report_schema_version``.
"""

import json
import math
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import DecisionCorpusBuildRecord, DecisionCorpusRowRecord
from kalshi_bot.db.repositories import PlatformRepository


REPORT_SCHEMA_VERSION = "v1"
REPORT_TOP_N = 20
LOG_LOSS_EPSILON = 1e-6

SUPPORTED_N = 100
SUPPORTED_MARKET_DAYS = 20
EXPLORATORY_N = 30
EXPLORATORY_MARKET_DAYS = 10
ABSOLUTE_MIN_PRIMARY_N = 10

SUPPORT_STATUS_ORDER = {"insufficient": 0, "exploratory": 1, "supported": 2}
SUPPORT_LEVEL_DIMENSIONS: dict[str, tuple[str, ...]] = {
    "L1_station_season_lead_regime": ("station_id", "season_bucket", "lead_bucket", "trade_regime"),
    "L2_station_season_lead": ("station_id", "season_bucket", "lead_bucket"),
    "L3_station_season": ("station_id", "season_bucket"),
    "L4_season_lead": ("season_bucket", "lead_bucket"),
    "L5_global": (),
}

CLEAN_PRIMARY_PROVENANCE = {
    "historical_replay_full_checkpoint",
    "historical_replay_partial_checkpoint",
    "historical_replay_late_only",
}
DEGRADED_PROVENANCE = {
    "historical_replay_external_forecast_repair",
    "historical_replay_unknown",
}
FINAL_CHECKPOINT_LABELS = {
    "3",
    "1700",
    "late",
    "final",
    "close",
    "checkpoint_3",
    "late_1700",
    "late_1700_local",
}


@dataclass(frozen=True, slots=True)
class _CalibrationRow:
    row: DecisionCorpusRowRecord
    prediction: float
    outcome: int
    brier: float
    log_loss: float
    provenance_scope: str


class DecisionCorpusCalibrationReportService:
    def __init__(self, settings: Settings, session_factory: async_sessionmaker) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def calibration_report(
        self,
        *,
        output: Path,
        build_id: str | None = None,
        kalshi_env: str | None = None,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        if bool(build_id) == bool(kalshi_env):
            raise ValueError("Specify exactly one of --build-id or --env")
        generated_at = _utc(generated_at or datetime.now(UTC))

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            build, selection = await self._resolve_build(repo, build_id=build_id, kalshi_env=kalshi_env)
            rows = await repo.list_decision_corpus_rows(build_id=build.id)
            promotions = await self._promotion_metadata(repo, build_id=build.id)

        json_path, markdown_path = _output_paths(output, build.id, generated_at)
        report = self._compute_report(
            build=build,
            rows=rows,
            selection=selection,
            promotions=promotions,
            generated_at=generated_at,
            json_filename=json_path.name,
            markdown_filename=markdown_path.name,
        )
        markdown = self._render_markdown(report)

        json_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        markdown_path.write_text(markdown, encoding="utf-8")

        self._print_stdout_summary(report, json_path=json_path, markdown_path=markdown_path)
        return {
            "status": "ok",
            "exit_code": report["report_metadata"]["exit_code"],
            "build_id": build.id,
            "selection_mode": selection["mode"],
            "json_path": str(json_path),
            "markdown_path": str(markdown_path),
            "warnings": report["report_metadata"]["warnings"],
        }

    async def _resolve_build(
        self,
        repo: PlatformRepository,
        *,
        build_id: str | None,
        kalshi_env: str | None,
    ) -> tuple[DecisionCorpusBuildRecord, dict[str, Any]]:
        if build_id is not None:
            build = await repo.get_decision_corpus_build(build_id)
            if build is None:
                raise KeyError(f"Decision corpus build {build_id} not found")
            if build.status != "successful":
                raise ValueError(f"Decision corpus build {build_id} has status={build.status}; expected successful")
            return build, {"mode": "explicit", "build_id": build.id, "kalshi_env": None}

        env = (kalshi_env or self.settings.kalshi_env or "demo").strip() or "demo"
        checkpoint = await repo.get_checkpoint(repo.decision_corpus_current_checkpoint_name(kalshi_env=env))
        if checkpoint is None or not (checkpoint.payload or {}).get("build_id"):
            raise ValueError(
                f"No current corpus build is promoted for env={env}. "
                "Run 'decision-corpus list-builds' to see builds, promote one, or use --build-id."
            )
        pointed_build_id = str((checkpoint.payload or {}).get("build_id"))
        build = await repo.get_decision_corpus_build(pointed_build_id)
        if build is None:
            raise ValueError(f"Current corpus build pointer for env={env} references missing build {pointed_build_id}")
        if build.status != "successful":
            raise ValueError(
                f"Current build {pointed_build_id} for env={env} has status={build.status}. "
                f"Investigate with 'decision-corpus inspect-build {pointed_build_id}'."
            )
        return build, {
            "mode": "current",
            "build_id": build.id,
            "kalshi_env": env,
            "promoted_at": (checkpoint.payload or {}).get("promoted_at"),
            "checkpoint_stream": checkpoint.stream_name,
        }

    async def _promotion_metadata(self, repo: PlatformRepository, *, build_id: str) -> list[dict[str, Any]]:
        promotions: list[dict[str, Any]] = []
        for checkpoint in await repo.list_checkpoints(prefix="current_decision_corpus_build:"):
            payload = checkpoint.payload or {}
            if str(payload.get("build_id") or "") != build_id:
                continue
            promotions.append(
                {
                    "stream_name": checkpoint.stream_name,
                    "kalshi_env": payload.get("kalshi_env") or checkpoint.stream_name.split(":")[-1],
                    "promoted_at": payload.get("promoted_at"),
                    "actor": payload.get("actor"),
                    "still_current": True,
                }
            )
        return promotions

    def _compute_report(
        self,
        *,
        build: DecisionCorpusBuildRecord,
        rows: list[DecisionCorpusRowRecord],
        selection: dict[str, Any],
        promotions: list[dict[str, Any]],
        generated_at: datetime,
        json_filename: str,
        markdown_filename: str,
    ) -> dict[str, Any]:
        total_rows = len(rows)
        valid_rows: list[_CalibrationRow] = []
        skip_counts: Counter[str] = Counter()
        contaminated_clean_rows = 0
        for row in rows:
            prediction = _float_or_none(row.fair_yes_dollars)
            if prediction is None:
                skip_counts["null_fair_yes_dollars"] += 1
                continue
            if prediction < 0 or prediction > 1:
                skip_counts["invalid_fair_yes_dollars"] += 1
                continue
            outcome = _outcome(row.settlement_result)
            if outcome is None:
                skip_counts[f"non_binary_settlement:{row.settlement_result or 'null'}"] += 1
                continue
            if self._has_clean_provenance_violation(row):
                contaminated_clean_rows += 1
                skip_counts["clean_provenance_asof_violation"] += 1
                continue
            brier = (prediction - outcome) ** 2
            log_loss = _log_loss(prediction, outcome)
            valid_rows.append(
                _CalibrationRow(
                    row=row,
                    prediction=prediction,
                    outcome=outcome,
                    brier=brier,
                    log_loss=log_loss,
                    provenance_scope=self._provenance_scope(row.source_provenance),
                )
            )

        clean_rows = [item for item in valid_rows if item.provenance_scope == "primary_clean"]
        primary_rows = [item for item in clean_rows if item.row.support_status in {"supported", "exploratory"}]
        degraded_rows = [item for item in valid_rows if item.provenance_scope == "degraded"]
        clean_market_days = len({item.row.local_market_day for item in primary_rows})
        primary_coverage_status = _coverage_status(len(primary_rows), clean_market_days)
        exit_code, warnings = self._coverage_warnings(
            clean_rows=primary_rows,
            clean_market_days=clean_market_days,
            total_rows=total_rows,
            primary_coverage_status=primary_coverage_status,
        )
        include_primary_metrics = len(primary_rows) >= ABSOLUTE_MIN_PRIMARY_N

        cells = self._calibration_cells(primary_rows) if include_primary_metrics else []
        reliability_curve = self._reliability_curve(valid_rows)
        coverage = self._coverage(rows, valid_rows, skip_counts, contaminated_clean_rows=contaminated_clean_rows)
        aggregates = {
            "primary": self._aggregate(primary_rows if include_primary_metrics else []),
            "descriptive": self._aggregate(valid_rows),
            "degraded_provenance": self._aggregate(degraded_rows),
        }
        aggregates["primary"]["coverage_status"] = primary_coverage_status
        aggregates["primary"]["metrics_omitted"] = not include_primary_metrics
        aggregates["primary"]["provenance_included"] = sorted(CLEAN_PRIMARY_PROVENANCE)
        aggregates["degraded_provenance"]["provenance_included"] = sorted(DEGRADED_PROVENANCE)

        report = {
            "report_metadata": {
                "report_schema_version": REPORT_SCHEMA_VERSION,
                "report_kind": "decision_corpus_calibration",
                "scope": "settlement_model_probability_calibration",
                "prediction": "fair_yes_dollars",
                "outcome": "settlement_result yes=1 no=0",
                "metrics": ["brier", "log_loss", "reliability_curve"],
                "included_rows": "rows with fair_yes_dollars and binary settlement_result; stand-down rows included",
                "build_id": build.id,
                "build_version": build.version,
                "build_status": build.status,
                "build_created_at": _iso(build.created_at),
                "build_finished_at": _iso(build.finished_at),
                "build_row_count": build.row_count,
                "build_date_from": build.date_from.isoformat(),
                "build_date_to": build.date_to.isoformat(),
                "build_git_sha": build.git_sha,
                "selection": selection,
                "promotions": promotions,
                "generated_at": _iso(generated_at),
                "calibration_report_code_version": _git_sha(),
                "support_floors": {
                    "supported": {"rows": SUPPORTED_N, "market_days": SUPPORTED_MARKET_DAYS},
                    "exploratory": {"rows": EXPLORATORY_N, "market_days": EXPLORATORY_MARKET_DAYS},
                    "absolute_min_primary_rows": ABSOLUTE_MIN_PRIMARY_N,
                },
                "reliability_buckets": 10,
                "log_loss_epsilon": LOG_LOSS_EPSILON,
                "primary_coverage_status": primary_coverage_status,
                "warnings": warnings,
                "exit_code": exit_code,
            },
            "coverage": coverage,
            "aggregates": aggregates,
            "reliability_curve": reliability_curve,
            "cells": cells,
            "markdown": {
                "json_filename": json_filename,
                "markdown_filename": markdown_filename,
                "per_cell_json_key": "cells[]",
                "per_cell_count": len(cells),
            },
        }
        return report

    def _has_clean_provenance_violation(self, row: DecisionCorpusRowRecord) -> bool:
        if row.source_provenance not in CLEAN_PRIMARY_PROVENANCE:
            return False
        source_asof = _utc(row.source_asof_ts)
        checkpoint_ts = _utc(row.checkpoint_ts)
        if source_asof is not None and checkpoint_ts is not None and source_asof > checkpoint_ts:
            return True
        if row.source_provenance == "historical_replay_late_only":
            details = row.source_details or {}
            diagnostics = row.diagnostics or {}
            checkpoint_label = str(details.get("checkpoint_label") or diagnostics.get("checkpoint_label") or "").strip().lower()
            if checkpoint_label and checkpoint_label not in FINAL_CHECKPOINT_LABELS:
                return True
        return False

    def _provenance_scope(self, source_provenance: str) -> str:
        if source_provenance in CLEAN_PRIMARY_PROVENANCE:
            return "primary_clean"
        if source_provenance in DEGRADED_PROVENANCE:
            return "degraded"
        return "unknown"

    def _coverage_warnings(
        self,
        *,
        clean_rows: list[_CalibrationRow],
        clean_market_days: int,
        total_rows: int,
        primary_coverage_status: str,
    ) -> tuple[int, list[dict[str, Any]]]:
        clean_n = len(clean_rows)
        if primary_coverage_status in {"supported", "exploratory"}:
            warning: list[dict[str, Any]] = []
            if primary_coverage_status == "exploratory":
                warning.append(
                    {
                        "type": "exploratory_primary_clean_coverage",
                        "severity": "warning",
                        "message": "Clean-provenance primary coverage is exploratory, not supported",
                        "clean_rows": clean_n,
                        "clean_market_days": clean_market_days,
                        "floor_supported": {"rows": SUPPORTED_N, "market_days": SUPPORTED_MARKET_DAYS},
                        "recommended_action": "expand_clean_corpus",
                    }
                )
            return 0, warning

        severity = "critical"
        warning = {
            "type": "insufficient_primary_clean_coverage",
            "severity": severity,
            "message": "Clean-provenance row count is below exploratory support floor",
            "clean_rows": clean_n,
            "total_rows": total_rows,
            "clean_fraction": _safe_div(clean_n, total_rows),
            "clean_market_days": clean_market_days,
            "floor_supported": {"rows": SUPPORTED_N, "market_days": SUPPORTED_MARKET_DAYS},
            "floor_exploratory": {"rows": EXPLORATORY_N, "market_days": EXPLORATORY_MARKET_DAYS},
            "absolute_min_primary_rows": ABSOLUTE_MIN_PRIMARY_N,
            "recommended_action": "expand_clean_corpus",
        }
        if clean_n < ABSOLUTE_MIN_PRIMARY_N:
            warning["type"] = "primary_clean_coverage_below_absolute_minimum"
            warning["message"] = "Clean-provenance primary coverage is below absolute minimum; primary metrics omitted"
            return 2, [warning]
        return 1, [warning]

    def _coverage(
        self,
        rows: list[DecisionCorpusRowRecord],
        valid_rows: list[_CalibrationRow],
        skip_counts: Counter[str],
        *,
        contaminated_clean_rows: int,
    ) -> dict[str, Any]:
        valid_row_ids = {item.row.id for item in valid_rows}
        clean_valid = [item for item in valid_rows if item.provenance_scope == "primary_clean"]
        degraded_valid = [item for item in valid_rows if item.provenance_scope == "degraded"]
        return {
            "row_counts": {
                "total_rows": len(rows),
                "valid_prediction_and_binary_outcome": len(valid_rows),
                "primary_clean_valid_rows": len(clean_valid),
                "degraded_valid_rows": len(degraded_valid),
                "stand_down_valid_rows": sum(1 for item in valid_rows if item.row.recommended_side is None),
                "skipped_rows": len(rows) - len(valid_row_ids),
                "contaminated_clean_rows": contaminated_clean_rows,
            },
            "skips": [{"reason": reason, "rows": count} for reason, count in sorted(skip_counts.items())],
            "source_provenance": _counter_table(row.source_provenance for row in rows),
            "source_provenance_valid": _counter_table(item.row.source_provenance for item in valid_rows),
            "support_status": _counter_table(row.support_status for row in rows),
            "support_level": _counter_table(row.support_level for row in rows),
            "local_market_days": len({row.local_market_day for row in rows}),
        }

    def _aggregate(self, rows: list[_CalibrationRow]) -> dict[str, Any]:
        if not rows:
            return {"n": 0, "brier": None, "log_loss": None, "observed_rate": None, "prediction_mean": None}
        return {
            "n": len(rows),
            "market_days": len({item.row.local_market_day for item in rows}),
            "brier": _mean(item.brier for item in rows),
            "log_loss": _mean(item.log_loss for item in rows),
            "observed_rate": _mean(float(item.outcome) for item in rows),
            "prediction_mean": _mean(item.prediction for item in rows),
        }

    def _reliability_curve(self, rows: list[_CalibrationRow]) -> list[dict[str, Any]]:
        buckets: list[list[_CalibrationRow]] = [[] for _ in range(10)]
        for item in rows:
            index = min(9, max(0, int(item.prediction * 10)))
            buckets[index].append(item)
        result: list[dict[str, Any]] = []
        for index, bucket_rows in enumerate(buckets):
            lower = index / 10
            upper = (index + 1) / 10
            n = len(bucket_rows)
            result.append(
                {
                    "bucket_lower": lower,
                    "bucket_upper": upper,
                    "bucket_label": f"[{lower:.1f}, {upper:.1f}{']' if index == 9 else ')'}",
                    "predicted_mean": _mean((item.prediction for item in bucket_rows)) if bucket_rows else None,
                    "observed_rate": _mean((float(item.outcome) for item in bucket_rows)) if bucket_rows else None,
                    "n": n,
                    "support_band": "high" if n >= 1000 else "medium" if n >= 300 else "low",
                }
            )
        return result

    def _calibration_cells(self, rows: list[_CalibrationRow]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, tuple[tuple[str, str], ...]], list[_CalibrationRow]] = defaultdict(list)
        for item in rows:
            cell_key = self._cell_key(item.row)
            grouped[(item.row.support_level, tuple(sorted(cell_key.items())))].append(item)

        cells: list[dict[str, Any]] = []
        for (support_level, key_tuple), cell_rows in grouped.items():
            cell_key = dict(key_tuple)
            build_status = _minimum_support_status(item.row.support_status for item in cell_rows)
            cell_n = len(cell_rows)
            build_support_n = max((item.row.support_n for item in cell_rows), default=0)
            cell_status = _cell_support_status(cell_n)
            cells.append(
                {
                    "cell_key": cell_key,
                    "cell_key_label": _cell_key_label(cell_key),
                    "support_level": support_level,
                    "cell_n": cell_n,
                    "build_support_n": build_support_n,
                    "build_support_market_days": max((item.row.support_market_days for item in cell_rows), default=0),
                    "build_support_recency_days": _min_optional(item.row.support_recency_days for item in cell_rows),
                    "cell_support_status": cell_status,
                    "build_support_status": build_status,
                    "support_status_divergence": cell_status != build_status,
                    "support_n_delta": build_support_n - cell_n,
                    "brier": _mean(item.brier for item in cell_rows),
                    "log_loss": _mean(item.log_loss for item in cell_rows),
                    "observed_rate": _mean(float(item.outcome) for item in cell_rows),
                    "prediction_mean": _mean(item.prediction for item in cell_rows),
                    "backoff_path": cell_rows[0].row.backoff_path or [],
                }
            )
        return sorted(cells, key=lambda item: (item["support_level"], item["cell_key_label"]))

    def _cell_key(self, row: DecisionCorpusRowRecord) -> dict[str, str]:
        dimensions = SUPPORT_LEVEL_DIMENSIONS.get(row.support_level, ())
        if not dimensions:
            return {"global": "global"}
        diagnostics = row.diagnostics or {}
        values: dict[str, str] = {}
        for dimension in dimensions:
            if dimension == "season_bucket":
                value = diagnostics.get("season_bucket")
            elif dimension == "lead_bucket":
                value = diagnostics.get("lead_bucket")
            else:
                value = getattr(row, dimension, None)
            values[dimension] = str(value or "unknown")
        return values

    def _render_markdown(self, report: dict[str, Any]) -> str:
        metadata = report["report_metadata"]
        coverage = report["coverage"]
        aggregates = report["aggregates"]
        warnings = metadata["warnings"]
        lines: list[str] = []
        lines.append("# Calibration Report")
        lines.append("")
        if warnings:
            lines.extend(self._warning_markdown(warnings[0], metadata["primary_coverage_status"]))
            lines.append("")
        lines.extend(
            [
                "**Scope:** This report evaluates the settlement model's probability calibration: whether "
                "`fair_yes_dollars` predictions match actual settlement outcomes. It does not evaluate policy "
                "performance, trade profitability, or execution quality.",
                "",
                f"**Build:** `{metadata['build_id']}`",
                f"**Build version:** `{metadata['build_version']}`",
                f"**Build created:** {metadata['build_created_at']}",
                f"**Build row count:** {metadata['build_row_count']}",
                f"**Selection mode:** {metadata['selection']['mode']}",
                f"**Report generated at:** {metadata['generated_at']}",
                f"**Calibration-report code version:** `{metadata['calibration_report_code_version']}`",
                f"**Report schema version:** `{metadata['report_schema_version']}`",
                f"**Config:** supported={SUPPORTED_N}/{SUPPORTED_MARKET_DAYS} market-days, "
                f"exploratory={EXPLORATORY_N}/{EXPLORATORY_MARKET_DAYS} market-days, "
                f"log_loss_epsilon={LOG_LOSS_EPSILON}",
                "",
                "**Prediction:** `fair_yes_dollars` (P(YES settles)).  ",
                "**Outcome:** `settlement_result == 'yes'` => 1, `'no'` => 0.  ",
                "**Metric:** Brier = `(prediction - outcome)^2`; log-loss uses clipped probabilities.",
                "",
            ]
        )
        lines.extend(self._aggregate_markdown(aggregates))
        lines.extend(self._reliability_markdown(report["reliability_curve"]))
        lines.extend(self._coverage_markdown(coverage))
        lines.extend(self._cell_markdown(report))
        if metadata["primary_coverage_status"] != "supported":
            lines.extend(self._recommended_actions_markdown(coverage))
        lines.extend(
            [
                "## Full Data",
                "",
                f"Full per-cell detail is in `{report['markdown']['json_filename']}` at JSON key "
                f"`cells[]` ({report['markdown']['per_cell_count']} entries).",
                "",
            ]
        )
        if warnings and metadata["exit_code"] in {1, 2}:
            lines.extend(
                [
                    "## Warning Reminder",
                    "",
                    "Primary metrics above are below the exploratory clean-provenance coverage floor. "
                    "Treat them as indicative only.",
                    "",
                ]
            )
        return "\n".join(lines)

    def _warning_markdown(self, warning: dict[str, Any], status: str) -> list[str]:
        clean_rows = warning.get("clean_rows", 0)
        total_rows = warning.get("total_rows")
        fraction = warning.get("clean_fraction")
        if warning["type"] == "primary_clean_coverage_below_absolute_minimum":
            headline = "## WARNING: Primary Clean Coverage Below Absolute Minimum"
            body = "Primary calibration metrics are omitted because clean-provenance coverage is too small."
        elif warning["type"] == "insufficient_primary_clean_coverage":
            headline = "## WARNING: Insufficient Primary Clean Coverage"
            body = "Primary calibration metrics are indicative only."
        else:
            headline = "## Exploratory Primary Clean Coverage"
            body = "Primary calibration metrics are available but below the supported floor."
        total_text = f" of {total_rows}" if total_rows is not None else ""
        fraction_text = f" ({_fmt_pct(fraction)})" if fraction is not None else ""
        return [
            headline,
            "",
            f"Clean-provenance rows total **{clean_rows}{total_text}**{fraction_text}, spanning "
            f"**{warning.get('clean_market_days', 0)} market-days**.",
            f"Coverage status: **{status.upper()}**. {body}",
            "",
            f"- Supported floor: {SUPPORTED_N} rows / {SUPPORTED_MARKET_DAYS} market-days",
            f"- Exploratory floor: {EXPLORATORY_N} rows / {EXPLORATORY_MARKET_DAYS} market-days",
            "",
        ]

    def _aggregate_markdown(self, aggregates: dict[str, Any]) -> list[str]:
        rows = []
        for label, key in (
            ("Primary clean", "primary"),
            ("Descriptive all valid", "descriptive"),
            ("Diagnostic degraded provenance", "degraded_provenance"),
        ):
            aggregate = aggregates[key]
            rows.append(
                [
                    label,
                    str(aggregate.get("n", 0)),
                    str(aggregate.get("market_days", "—")),
                    aggregate.get("coverage_status", "—"),
                    _fmt_metric(aggregate.get("brier")),
                    _fmt_metric(aggregate.get("log_loss")),
                    _fmt_metric(aggregate.get("prediction_mean")),
                    _fmt_metric(aggregate.get("observed_rate")),
                ]
            )
        return [
            "## Aggregate Calibration",
            "",
            "Primary is the model-quality metric: clean provenance and supported/exploratory rows only. "
            "Descriptive includes all valid rows. Degraded is diagnostic, not a substitute for primary.",
            "",
            _md_table(
                ["Scope", "N", "Market days", "Coverage", "Brier", "Log-loss", "Pred mean", "Observed"],
                rows,
            ),
            "",
        ]

    def _reliability_markdown(self, curve: list[dict[str, Any]]) -> list[str]:
        rows = [
            [
                bucket["bucket_label"],
                _fmt_metric(bucket["predicted_mean"]),
                _fmt_metric(bucket["observed_rate"]),
                str(bucket["n"]),
                bucket["support_band"],
            ]
            for bucket in curve
        ]
        return [
            "## Reliability Curve",
            "",
            "Reliability buckets include all valid, non-contaminated rows. Low-count buckets are noisier.",
            "",
            _md_table(["Bucket", "Predicted", "Observed", "N", "Support"], rows),
            "",
        ]

    def _coverage_markdown(self, coverage: dict[str, Any]) -> list[str]:
        counts = coverage["row_counts"]
        lines = [
            "## Coverage",
            "",
            f"Rows with valid prediction and binary outcome: **{counts['valid_prediction_and_binary_outcome']}** "
            f"of **{counts['total_rows']}**.",
            f"Stand-down rows included in calibration: **{counts['stand_down_valid_rows']}**.",
            "",
            "### Source Provenance",
            "",
            _md_table(["Source provenance", "Rows"], [[item["value"], str(item["rows"])] for item in coverage["source_provenance"]]),
            "",
            "### Support Status",
            "",
            _md_table(["Support status", "Rows"], [[item["value"], str(item["rows"])] for item in coverage["support_status"]]),
            "",
        ]
        if coverage["skips"]:
            lines.extend(
                [
                    "### Skipped Rows",
                    "",
                    _md_table(["Reason", "Rows"], [[item["reason"], str(item["rows"])] for item in coverage["skips"]]),
                    "",
                ]
            )
        return lines

    def _cell_markdown(self, report: dict[str, Any]) -> list[str]:
        cells = report["cells"]
        lines = [
            "## Calibration Cells",
            "",
            "A calibration cell groups rows by their recorded `support_level`. `cell_n` is the rows in this "
            "report cell; `build_support_n` is the PR 1 support bucket size used during corpus build.",
            "",
        ]
        eligible_worst = [cell for cell in cells if cell["cell_n"] >= EXPLORATORY_N]
        if eligible_worst:
            selected = sorted(
                eligible_worst,
                key=lambda cell: (-cell["brier"], cell["cell_n"], cell["cell_key_label"]),
            )[:REPORT_TOP_N]
            lines.extend(
                [
                    "### Worst Calibration Cells By Brier",
                    "",
                    "Cells where the model is least calibrated at exploratory-or-better cell sample sizes. "
                    "High Brier here is a model-quality signal.",
                    "",
                    self._cell_table(selected),
                    "",
                ]
            )
            if len(selected) == len(eligible_worst):
                lines.extend([f"Showing all {len(selected)} eligible cells; curation threshold not reached.", ""])
        else:
            lines.extend(
                [
                    "### Worst Calibration Cells By Brier",
                    "",
                    f"No calibration cells meet the exploratory row-count floor ({EXPLORATORY_N}); omitted to avoid noise-ranking.",
                    "",
                ]
            )

        largest = [cell for cell in cells if cell["cell_n"] >= EXPLORATORY_N]
        if largest:
            selected = sorted(
                largest,
                key=lambda cell: (-cell["cell_n"], cell["brier"], cell["cell_key_label"]),
            )[:REPORT_TOP_N]
            lines.extend(
                [
                    "### Largest Calibration Cells",
                    "",
                    "Cells with the most evidence. Calibration estimates here are the easiest to interpret.",
                    "",
                    self._cell_table(selected),
                    "",
                ]
            )
            if len(selected) == len(largest):
                lines.extend([f"Showing all {len(selected)} eligible cells; curation threshold not reached.", ""])
        else:
            lines.extend(
                [
                    "### Largest Calibration Cells",
                    "",
                    f"No calibration cells meet the exploratory row-count floor ({EXPLORATORY_N}); largest-cell ranking omitted.",
                    "",
                ]
            )

        divergent = [cell for cell in cells if cell["support_status_divergence"]]
        if divergent:
            selected = sorted(
                divergent,
                key=lambda cell: (-cell["support_n_delta"], -cell["cell_n"], cell["cell_key_label"]),
            )[:REPORT_TOP_N]
            lines.extend(
                [
                    "### Support-Level Divergence",
                    "",
                    f"{len(divergent)} calibration cells have `cell_support_status != build_support_status`. "
                    "This happens when the backoff bucket met one floor but this disjoint calibration cell has another.",
                    "",
                    self._cell_table(selected),
                    "",
                ]
            )
        else:
            lines.extend(
                [
                    "### Support-Level Divergence",
                    "",
                    "All calibration cells' `cell_support_status` matches their `build_support_status`. "
                    "No divergence detected in this build.",
                    "",
                ]
            )
        return lines

    def _cell_table(self, cells: list[dict[str, Any]]) -> str:
        rows = []
        for cell in cells:
            key = cell["cell_key"]
            rows.append(
                [
                    key.get("station_id", "—"),
                    key.get("season_bucket", "—"),
                    key.get("lead_bucket", "—"),
                    key.get("trade_regime", "—"),
                    cell["support_level"],
                    str(cell["cell_n"]),
                    str(cell["build_support_n"]),
                    cell["cell_support_status"],
                    cell["build_support_status"],
                    _fmt_metric(cell["brier"]),
                    _fmt_metric(cell["log_loss"]),
                ]
            )
        return _md_table(
            [
                "Station",
                "Season",
                "Lead",
                "Regime",
                "Level",
                "cell_n",
                "build_support_n",
                "cell_status",
                "build_status",
                "Brier",
                "Log-loss",
            ],
            rows,
        )

    def _recommended_actions_markdown(self, coverage: dict[str, Any]) -> list[str]:
        counts = coverage["row_counts"]
        return [
            "## Recommended Actions",
            "",
            f"Current clean-provenance coverage is {counts['primary_clean_valid_rows']} rows against "
            f"{counts['degraded_valid_rows']} degraded-provenance rows.",
            "",
            "1. Continue live-shadow operation once live-shadow corpus capture is added.",
            "2. Extend historical replay to date ranges with complete checkpoint archives.",
            "3. Investigate checkpoint archive completeness if forecast-repair rows dominate the corpus.",
            "",
            "Not recommended: relaxing primary-metric provenance scope to include forecast-repair rows. "
            "That would undermine cross-build comparability.",
            "",
        ]

    def _print_stdout_summary(self, report: dict[str, Any], *, json_path: Path, markdown_path: Path) -> None:
        metadata = report["report_metadata"]
        coverage = report["coverage"]["row_counts"]
        warnings = metadata["warnings"]
        if warnings and metadata["exit_code"] in {1, 2}:
            warning = warnings[0]
            print("WARNING: Insufficient clean-provenance coverage")
            print(
                f"  Clean rows: {warning.get('clean_rows', 0)} / {warning.get('total_rows', coverage['total_rows'])}; "
                f"clean market-days: {warning.get('clean_market_days', 0)}"
            )
            print("  Primary metrics are indicative only." if metadata["exit_code"] == 1 else "  Primary metrics omitted.")
        selection = metadata["selection"]
        if selection["mode"] == "current":
            selection_text = f"current on env={selection.get('kalshi_env')}, promoted {selection.get('promoted_at') or 'unknown'}"
        else:
            selection_text = "explicit --build-id"
        support_counts = {item["value"]: item["rows"] for item in report["coverage"]["support_status"]}
        print(f"Running calibration report for build {metadata['build_id']} ({selection_text}).")
        print(
            f"Build row count: {metadata['build_row_count']}. "
            f"Support distribution: supported={support_counts.get('supported', 0)}, "
            f"exploratory={support_counts.get('exploratory', 0)}, "
            f"insufficient={support_counts.get('insufficient', 0)}."
        )
        print(f"Report written to {json_path} and {markdown_path}.")
        if warnings and metadata["exit_code"] in {1, 2}:
            print("Reminder: primary metrics are below the clean-provenance support floor.")


def _outcome(settlement_result: str | None) -> int | None:
    if settlement_result == "yes":
        return 1
    if settlement_result == "no":
        return 0
    return None


def _log_loss(prediction: float, outcome: int) -> float:
    p = min(1.0 - LOG_LOSS_EPSILON, max(LOG_LOSS_EPSILON, prediction))
    if outcome == 1:
        return -math.log(p)
    return -math.log(1.0 - p)


def _coverage_status(n: int, market_days: int) -> str:
    if n >= SUPPORTED_N and market_days >= SUPPORTED_MARKET_DAYS:
        return "supported"
    if n >= EXPLORATORY_N and market_days >= EXPLORATORY_MARKET_DAYS:
        return "exploratory"
    return "insufficient"


def _cell_support_status(n: int) -> str:
    if n >= SUPPORTED_N:
        return "supported"
    if n >= EXPLORATORY_N:
        return "exploratory"
    return "insufficient"


def _minimum_support_status(statuses: Any) -> str:
    values = list(statuses)
    if not values:
        return "insufficient"
    return min(values, key=lambda value: SUPPORT_STATUS_ORDER.get(str(value), -1))


def _counter_table(values: Any) -> list[dict[str, Any]]:
    counts = Counter(str(value or "unknown") for value in values)
    return [{"value": value, "rows": counts[value]} for value in sorted(counts)]


def _mean(values: Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return sum(float(item) for item in items) / len(items)


def _safe_div(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    normalized = _utc(value)
    return normalized.isoformat() if normalized is not None else None


def _min_optional(values: Any) -> int | None:
    items = [item for item in values if item is not None]
    return min(items) if items else None


def _output_paths(output: Path, build_id: str, generated_at: datetime) -> tuple[Path, Path]:
    stamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    if output.suffix in {".json", ".md"}:
        base = output.with_suffix("")
        return base.with_suffix(".json"), base.with_suffix(".md")
    return (
        output / f"calibration_{build_id}_{stamp}.json",
        output / f"calibration_{build_id}_{stamp}.md",
    )


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _cell_key_label(cell_key: dict[str, str]) -> str:
    return "|".join(f"{key}={value}" for key, value in sorted(cell_key.items()))


def _fmt_metric(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.4f}"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value) * 100:.1f}%"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    def cell(value: Any) -> str:
        return str(value).replace("\n", " ").replace("|", "\\|")

    header = "| " + " | ".join(cell(item) for item in headers) + " |"
    divider = "| " + " | ".join("---" for _ in headers) + " |"
    body = ["| " + " | ".join(cell(item) for item in row) + " |" for row in rows]
    return "\n".join([header, divider, *body])
