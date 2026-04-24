"""Decision-corpus-backed strategy regression ranking report.

This module is PR 2's read-only ranking consumer of the decision corpus. It does
not write strategy_results, checkpoints, assignments, or live-path state. The
legacy room-based StrategyRegressionService remains untouched for daemon and UI
compatibility until a separate migration PR updates those consumers.

Ranking invariant:
    The primary ranking metric is clustered Sortino over
    ``pnl_counterfactual_target_with_fees`` aggregated by
    ``kalshi_env + series_ticker + local_market_day``. ``win_rate`` is computed
    only as display metadata and is never used in sort, filter, tiebreak, or
    promotion logic.

JSON schema v1:
    {
      "report_metadata": {...},
      "diagnostics": {...},
      "leaderboard": [{...}],
      "result_rows": [{...}],
      "recommended_for_promotion": [{...}],
      "indicative_only": [{...}],
      "markdown": {"json_filename": str, "markdown_filename": str}
    }

Fields documented in schema v1 are stable. Future fields may be added; existing
fields are not renamed or removed without bumping ``report_schema_version``.
"""

from __future__ import annotations

import json
import math
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import DecisionCorpusBuildRecord, DecisionCorpusRowRecord, StrategyRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.strategy_regression import RegressionStrategySpec, _thresholds_from_dict, _would_have_traded


REPORT_SCHEMA_VERSION = "v1"
RANKING_VERSION = "clustered_sortino_v1"
REPORT_TOP_N = 20


@dataclass(frozen=True, slots=True)
class _ContributingRow:
    row: DecisionCorpusRowRecord
    pnl: Decimal


class StrategyRegressionRankingReportService:
    def __init__(self, settings: Settings, session_factory: async_sessionmaker) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def rank_report(
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
            strategy_records = await repo.list_strategies(active_only=True)

        json_path, markdown_path = _output_paths(output, build.id, generated_at)
        report = self._compute_report(
            build=build,
            rows=rows,
            strategies=[_strategy_spec(record) for record in strategy_records],
            selection=selection,
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

    def _compute_report(
        self,
        *,
        build: DecisionCorpusBuildRecord,
        rows: list[DecisionCorpusRowRecord],
        strategies: list[RegressionStrategySpec],
        selection: dict[str, Any],
        generated_at: datetime,
        json_filename: str,
        markdown_filename: str,
    ) -> dict[str, Any]:
        settings_payload = {
            "promote_floor_clusters": int(self.settings.strategy_regression_promote_floor_clusters),
            "min_clusters_for_ranking": int(self.settings.strategy_regression_min_clusters_for_ranking),
            "min_sortino_for_promotion": float(self.settings.strategy_regression_min_sortino_for_promotion),
            "sortino_downside_epsilon_dollars": float(
                self.settings.strategy_regression_sortino_downside_epsilon_dollars
            ),
        }
        warnings: list[dict[str, Any]] = []
        diagnostics: dict[str, Any] = {
            "total_corpus_rows": len(rows),
            "strategy_count": len(strategies),
            "ranking_input": "pnl_counterfactual_target_with_fees",
            "cluster_key": ["kalshi_env", "series_ticker_or_station_id", "local_market_day"],
            "display_only_fields": ["win_rate"],
        }

        if not rows:
            warnings.append({
                "type": "empty_corpus",
                "severity": "critical",
                "message": "Selected decision corpus build has zero rows; ranking cannot be computed.",
            })
        if not strategies:
            warnings.append({
                "type": "no_active_strategies",
                "severity": "critical",
                "message": "No active strategy presets are available; ranking cannot be computed.",
            })

        result_rows: list[dict[str, Any]] = []
        city_results: dict[str, list[dict[str, Any]]] = defaultdict(list)
        leaderboard: list[dict[str, Any]] = []
        if rows and strategies:
            for strategy in strategies:
                thresholds = _thresholds_from_dict(strategy.thresholds)
                rows_by_series: dict[str, list[DecisionCorpusRowRecord]] = defaultdict(list)
                for row in rows:
                    series_key = _series_key(row)
                    rows_by_series[series_key].append(row)

                strategy_contributors: list[_ContributingRow] = []
                strategy_candidate_count = 0
                strategy_null_pnl_count = 0
                strategy_insufficient_count = 0
                for series_key in sorted(rows_by_series):
                    series_rows = rows_by_series[series_key]
                    series_metrics = self._evaluate_strategy_rows(
                        strategy=strategy,
                        series_ticker=series_key,
                        rows=series_rows,
                        thresholds=thresholds,
                    )
                    result_rows.append(series_metrics)
                    city_results[series_key].append(series_metrics)
                    strategy_contributors.extend(series_metrics["_contributors"])
                    strategy_candidate_count += int(series_metrics["candidate_decision_count"])
                    strategy_null_pnl_count += int(series_metrics["null_pnl_rows_excluded"])
                    strategy_insufficient_count += int(series_metrics["insufficient_support_rows_excluded"])

                overall_metrics = self._metric_payload(
                    strategy=strategy,
                    series_ticker=None,
                    contributors=strategy_contributors,
                    candidate_decision_count=strategy_candidate_count,
                    null_pnl_rows_excluded=strategy_null_pnl_count,
                    insufficient_support_rows_excluded=strategy_insufficient_count,
                    total_rows_evaluated=len(rows),
                )
                leaderboard.append(overall_metrics)

            for row in result_rows:
                row.pop("_contributors", None)
            for row in leaderboard:
                row.pop("_contributors", None)
            for series_key in city_results:
                city_results[series_key] = _rank_rows(city_results[series_key])
                for row in city_results[series_key]:
                    row.pop("_contributors", None)
            result_rows = _rank_rows(result_rows)
            leaderboard = _rank_rows(leaderboard)

        recommended_for_promotion = [row for row in result_rows if row.get("promotion_candidate")]
        indicative_only = [
            row
            for row in result_rows
            if row.get("below_support_floor") or row.get("insufficient_for_ranking")
        ]
        rows_clearing_promote_floor = [
            row for row in result_rows if not row.get("below_support_floor") and not row.get("insufficient_for_ranking")
        ]

        if result_rows and not rows_clearing_promote_floor:
            warnings.append({
                "type": "insufficient_cluster_coverage",
                "severity": "critical",
                "message": "No strategy/series row clears the cluster promotion floor; rankings are indicative only.",
                "promote_floor_clusters": settings_payload["promote_floor_clusters"],
                "max_cluster_count": max(int(row.get("cluster_count") or 0) for row in result_rows),
            })
        elif result_rows and len(rows_clearing_promote_floor) < len(result_rows):
            warnings.append({
                "type": "partial_cluster_coverage",
                "severity": "warning",
                "message": "Some strategy/series rows are below the cluster promotion floor.",
                "promote_floor_clusters": settings_payload["promote_floor_clusters"],
                "below_floor_rows": len(result_rows) - len(rows_clearing_promote_floor),
            })

        if not rows or not strategies:
            exit_code = 2
        elif not rows_clearing_promote_floor:
            exit_code = 1
        else:
            exit_code = 0

        diagnostics.update({
            "result_row_count": len(result_rows),
            "leaderboard_count": len(leaderboard),
            "recommended_for_promotion_count": len(recommended_for_promotion),
            "indicative_only_count": len(indicative_only),
            "rows_clearing_promote_floor": len(rows_clearing_promote_floor),
        })

        return {
            "report_metadata": {
                "report_kind": "strategy_regression_ranking",
                "report_schema_version": REPORT_SCHEMA_VERSION,
                "ranking_version": RANKING_VERSION,
                "build_id": build.id,
                "build_version": build.version,
                "build_created_at": _iso(build.created_at),
                "build_finished_at": _iso(build.finished_at),
                "build_row_count": build.row_count,
                "selection": selection,
                "generated_at": generated_at.isoformat(),
                "code_version": _git_sha(),
                "settings": settings_payload,
                "warnings": warnings,
                "exit_code": exit_code,
            },
            "diagnostics": diagnostics,
            "leaderboard": leaderboard,
            "result_rows": result_rows,
            "city_results": dict(city_results),
            "recommended_for_promotion": recommended_for_promotion[:REPORT_TOP_N],
            "indicative_only": indicative_only[:REPORT_TOP_N],
            "markdown": {
                "json_filename": json_filename,
                "markdown_filename": markdown_filename,
                "result_rows_json_path": "result_rows[]",
            },
        }

    def _evaluate_strategy_rows(
        self,
        *,
        strategy: RegressionStrategySpec,
        series_ticker: str,
        rows: list[DecisionCorpusRowRecord],
        thresholds: Any,
    ) -> dict[str, Any]:
        contributors: list[_ContributingRow] = []
        candidate_decision_count = 0
        null_pnl_rows_excluded = 0
        insufficient_support_rows_excluded = 0
        for row in rows:
            if not _row_would_have_traded(row, thresholds):
                continue
            candidate_decision_count += 1
            if row.support_status == "insufficient":
                insufficient_support_rows_excluded += 1
                continue
            pnl = _decimal_or_none(row.pnl_counterfactual_target_with_fees)
            if pnl is None:
                null_pnl_rows_excluded += 1
                continue
            contributors.append(_ContributingRow(row=row, pnl=pnl))

        return self._metric_payload(
            strategy=strategy,
            series_ticker=series_ticker,
            contributors=contributors,
            candidate_decision_count=candidate_decision_count,
            null_pnl_rows_excluded=null_pnl_rows_excluded,
            insufficient_support_rows_excluded=insufficient_support_rows_excluded,
            total_rows_evaluated=len(rows),
        )

    def _metric_payload(
        self,
        *,
        strategy: RegressionStrategySpec,
        series_ticker: str | None,
        contributors: list[_ContributingRow],
        candidate_decision_count: int,
        null_pnl_rows_excluded: int,
        insufficient_support_rows_excluded: int,
        total_rows_evaluated: int,
    ) -> dict[str, Any]:
        clusters: dict[tuple[str, str, str], Decimal] = defaultdict(lambda: Decimal("0"))
        supported_clusters: set[tuple[str, str, str]] = set()
        for item in contributors:
            key = _cluster_key(item.row)
            clusters[key] += item.pnl
            if item.row.support_status == "supported":
                supported_clusters.add(key)

        cluster_values = [float(value) for _, value in sorted(clusters.items(), key=lambda item: item[0])]
        cluster_count = len(cluster_values)
        min_clusters = int(self.settings.strategy_regression_min_clusters_for_ranking)
        promote_floor = int(self.settings.strategy_regression_promote_floor_clusters)
        min_sortino = float(self.settings.strategy_regression_min_sortino_for_promotion)
        epsilon = float(self.settings.strategy_regression_sortino_downside_epsilon_dollars)
        mean_cluster_pnl = (sum(cluster_values) / cluster_count) if cluster_count else None
        median_cluster_pnl = median(cluster_values) if cluster_values else None
        total_net_pnl = sum(cluster_values) if cluster_values else None
        downside_values = [value for value in cluster_values if value < 0]
        downside_stdev = (
            math.sqrt(sum(value * value for value in downside_values) / len(downside_values))
            if downside_values
            else 0.0
        )
        effective_downside_stdev = max(downside_stdev, epsilon)
        total_stdev = _population_stdev(cluster_values)
        effective_total_stdev = max(total_stdev, epsilon) if total_stdev is not None else None
        insufficient_for_ranking = cluster_count < min_clusters
        sortino = None
        sharpe = None
        if mean_cluster_pnl is not None and not insufficient_for_ranking:
            sortino = mean_cluster_pnl / effective_downside_stdev
            if effective_total_stdev is not None:
                sharpe = mean_cluster_pnl / effective_total_stdev
        positive_rows = sum(1 for item in contributors if item.pnl > 0)
        positive_clusters = sum(1 for value in cluster_values if value > 0)
        win_rate = (positive_rows / len(contributors)) if contributors else None
        percent_clusters_positive = (positive_clusters / cluster_count) if cluster_count else None
        below_support_floor = cluster_count < promote_floor
        promotion_candidate = bool(
            not insufficient_for_ranking
            and not below_support_floor
            and sortino is not None
            and sortino >= min_sortino
            and total_net_pnl is not None
            and total_net_pnl > 0
        )

        return {
            "ranking_version": RANKING_VERSION,
            "strategy_id": strategy.id,
            "strategy_name": strategy.name,
            "strategy_description": strategy.description,
            "series_ticker": series_ticker,
            "total_rows_evaluated": total_rows_evaluated,
            "candidate_decision_count": candidate_decision_count,
            "total_rows_contributing": len(contributors),
            "null_pnl_rows_excluded": null_pnl_rows_excluded,
            "insufficient_support_rows_excluded": insufficient_support_rows_excluded,
            "cluster_count": cluster_count,
            "supported_clusters": len(supported_clusters),
            "total_net_pnl_dollars": _round(total_net_pnl),
            "mean_cluster_pnl": _round(mean_cluster_pnl),
            "median_cluster_pnl": _round(median_cluster_pnl),
            "sortino": _round(sortino),
            "sharpe": _round(sharpe),
            "downside_stdev": _round(downside_stdev if cluster_count else None),
            "effective_downside_stdev": _round(effective_downside_stdev if cluster_count else None),
            "percent_clusters_positive": _round(percent_clusters_positive),
            "win_rate": _round(win_rate),
            "win_rate_display_only": True,
            "below_support_floor": below_support_floor,
            "insufficient_for_ranking": insufficient_for_ranking,
            "promotion_candidate": promotion_candidate,
            "_contributors": contributors,
        }

    def _render_markdown(self, report: dict[str, Any]) -> str:
        metadata = report["report_metadata"]
        settings = metadata["settings"]
        lines: list[str] = [
            "# Strategy Regression Ranking",
            "",
            "**Scope:** This report ranks strategy presets with clustered Sortino over decision-corpus policy PnL. "
            "It is read-only and does not modify live strategy assignments.",
            "",
            f"**Build:** {metadata['build_id']}",
            f"**Ranking version:** `{metadata['ranking_version']}`",
            f"**Selection mode:** {metadata['selection'].get('mode')}",
            f"**Generated at:** {metadata['generated_at']}",
            f"**Cluster floor:** promote >= {settings['promote_floor_clusters']} clusters; "
            f"rank >= {settings['min_clusters_for_ranking']} clusters",
            "",
        ]
        if metadata["warnings"]:
            lines.extend(["## Warnings", ""])
            for warning in metadata["warnings"]:
                lines.append(f"- **{warning['severity'].upper()} {warning['type']}**: {warning['message']}")
            lines.append("")

        lines.extend([
            "## Recommended For Promotion",
            "",
            "Rows here clear the cluster floor, Sortino floor, and positive-PnL gate.",
            "",
        ])
        recommended = report["recommended_for_promotion"]
        if not recommended:
            lines.extend(["No strategy/series rows currently clear the promotion gates.", ""])
        else:
            lines.extend(_rows_table(recommended))

        lines.extend([
            "## Leaderboard",
            "",
            "Overall strategy rows across all series, ranked by Sortino. Win rate is display-only.",
            "",
        ])
        lines.extend(_rows_table(report["leaderboard"][:REPORT_TOP_N]))

        lines.extend([
            "## Indicative Only",
            "",
            "Rows below cluster floors are shown for visibility but should not drive promotion decisions.",
            "",
        ])
        indicative = report["indicative_only"]
        if not indicative:
            lines.extend(["No indicative-only rows.", ""])
        else:
            lines.extend(_rows_table(indicative))

        lines.extend([
            "## Machine Data",
            "",
            f"Full per-row detail is in `{report['markdown']['json_filename']}` at JSON key `result_rows[]`.",
            "",
        ])
        return "\n".join(lines)

    def _print_stdout_summary(self, report: dict[str, Any], *, json_path: Path, markdown_path: Path) -> None:
        metadata = report["report_metadata"]
        print(
            f"Running strategy regression ranking for build {metadata['build_id']} "
            f"({metadata['selection'].get('mode')})."
        )
        print(
            f"Ranking version: {metadata['ranking_version']}. "
            f"Result rows: {report['diagnostics']['result_row_count']}. "
            f"Recommended: {report['diagnostics']['recommended_for_promotion_count']}."
        )
        for warning in metadata["warnings"]:
            print(f"WARNING {warning['type']}: {warning['message']}")
        print(f"Report written to {json_path} and {markdown_path}.")


def _row_would_have_traded(row: DecisionCorpusRowRecord, thresholds: Any) -> bool:
    if row.edge_bps is None:
        return False
    return _would_have_traded(
        {
            "edge_bps": int(row.edge_bps),
            "signal_payload": dict(row.signal_payload or {}),
        },
        thresholds,
    )


def _strategy_spec(record: StrategyRecord) -> RegressionStrategySpec:
    return RegressionStrategySpec(
        id=record.id,
        name=record.name,
        description=record.description,
        thresholds=dict(record.thresholds or {}),
    )


def _rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=_rank_key)


def _rank_key(row: dict[str, Any]) -> tuple[int, float, int, float, str, str]:
    metric = row.get("sortino")
    total_pnl = row.get("total_net_pnl_dollars")
    return (
        0 if metric is not None else 1,
        -(float(metric) if metric is not None else float("-inf")),
        -int(row.get("cluster_count") or 0),
        -(float(total_pnl) if total_pnl is not None else float("-inf")),
        str(row.get("series_ticker") or ""),
        str(row.get("strategy_name") or ""),
    )


def _cluster_key(row: DecisionCorpusRowRecord) -> tuple[str, str, str]:
    return (
        str(row.kalshi_env or "unknown_env"),
        _series_key(row),
        str(row.local_market_day or "unknown_day"),
    )


def _series_key(row: DecisionCorpusRowRecord) -> str:
    return str(row.series_ticker or row.station_id or "unknown_series")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _population_stdev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _round(value: float | Decimal | None, places: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), places)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return _utc(value).isoformat() if value is not None else None


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def _output_paths(output: Path, build_id: str, generated_at: datetime) -> tuple[Path, Path]:
    if output.suffix:
        json_path = output
        markdown_path = output.with_suffix(".md")
        return json_path, markdown_path
    stamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    stem = f"strategy_ranking_{build_id}_{stamp}"
    return output / f"{stem}.json", output / f"{stem}.md"


def _rows_table(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["No rows.", ""]
    lines = [
        "| Strategy | Series | Sortino | Clusters | Mean Cluster PnL | Total PnL | Win Rate (Display Only) | Status |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:REPORT_TOP_N]:
        if row.get("insufficient_for_ranking"):
            status = "insufficient"
        elif row.get("below_support_floor"):
            status = "below floor"
        elif row.get("promotion_candidate"):
            status = "candidate"
        else:
            status = "ranked"
        lines.append(
            "| "
            f"{row.get('strategy_name')} | "
            f"{row.get('series_ticker') or 'all'} | "
            f"{_display_float(row.get('sortino'))} | "
            f"{row.get('cluster_count')} | "
            f"{_display_money(row.get('mean_cluster_pnl'))} | "
            f"{_display_money(row.get('total_net_pnl_dollars'))} | "
            f"{_display_pct(row.get('win_rate'))} | "
            f"{status} |"
        )
    lines.append("")
    return lines


def _display_float(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def _display_money(value: Any) -> str:
    return "n/a" if value is None else f"${float(value):+.4f}"


def _display_pct(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.1%}"
