from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import numpy as np
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import HistoricalReplayRunRecord, HistoricalSettlementLabelRecord, Signal
from kalshi_bot.integrations.kalshi import KalshiClient

logger = logging.getLogger(__name__)

_SERIES_TICKER_RE = re.compile(r"^(KXHIGH[A-Z]+)-[A-Z0-9]+-T\d+")
_SLOPE_WINDOW_MINUTES = 60
_MIN_CANDLE_POINTS = 5
_MOMENTUM_WEIGHT_FLOOR = 0.3  # Step 3 config; hardcoded here pending calibration


class MomentumCalibrationService:
    """
    Step-2 calibration tooling for the momentum entry-weight feature.

    Run in order:
      1. backfill_slopes  — fetch Kalshi 1-min candlesticks and write
                            momentum_slope_cents_per_min to Signal.payload (write-if-absent).
      2. first_look_report — read stored slopes; bucket table + bootstrap CIs.
      3. deploy_calibration — one-parameter scale fit + veto candidates; saves log.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        kalshi_client: KalshiClient,
        settings: Settings,
    ) -> None:
        self.session_factory = session_factory
        self.kalshi = kalshi_client
        self.settings = settings

    # ── public API ──────────────────────────────────────────────────────────

    async def backfill_slopes(self, date_from: str, date_to: str) -> dict[str, Any]:
        """Fetch 60-min candlestick slopes and store to Signal.payload (write-if-absent)."""
        rows = await self._load_corpus(date_from, date_to)
        n_total = len(rows)
        n_already = 0
        n_written = 0
        n_null = 0

        async with self.session_factory() as session:
            for row in rows:
                payload: dict = row["signal_payload"] or {}
                if "momentum_slope_cents_per_min" in payload:
                    n_already += 1
                    continue

                slope = await self._fetch_slope(
                    series_ticker=row["series_ticker"],
                    market_ticker=row["market_ticker"],
                    before_ts=row["checkpoint_ts"],
                )
                if slope is None:
                    n_null += 1

                new_payload = {
                    **payload,
                    "momentum_slope_cents_per_min": slope,
                    "momentum_slope_source": "calibration_backfill",
                }
                await session.execute(
                    update(Signal)
                    .where(Signal.id == row["signal_id"])
                    .values(payload=new_payload)
                )
                n_written += 1

            await session.commit()

        return {
            "n_total": n_total,
            "n_already_had_slope": n_already,
            "n_written": n_written,
            "n_null_slope": n_null,
        }

    async def first_look_report(
        self,
        date_from: str,
        date_to: str,
        output_path: Path | None = None,
    ) -> dict[str, Any]:
        """Load stored slopes + settlement outcomes; return bucket table and bootstrap CIs."""
        rows = await self._load_corpus(date_from, date_to)
        records = _build_analysis_records(rows)
        report = _first_look_analysis(records)
        if output_path is not None:
            _write_jsonl(output_path, records)
        _print_first_look(report)
        return report

    async def deploy_calibration(
        self,
        date_from: str,
        date_to: str,
        min_observations: int,
        output_path: Path | None = None,
    ) -> dict[str, Any]:
        """Fit scale + veto candidates; save timestamped log if output_path given."""
        rows = await self._load_corpus(date_from, date_to)
        records = _build_analysis_records(rows)
        n_usable = sum(
            1
            for r in records
            if r["slope_against"] is not None and r["settlement_pnl"] is not None
        )
        if n_usable < min_observations:
            return {
                "error": f"insufficient observations: {n_usable} < {min_observations}",
                "n_usable": n_usable,
            }

        result = _deploy_analysis(records)
        if output_path is not None:
            log_entry = {
                "run_at": datetime.now(UTC).isoformat(),
                "date_from": date_from,
                "date_to": date_to,
                "min_observations": min_observations,
                "result": result,
            }
            _write_jsonl(output_path, [log_entry])
        _print_deploy(result)
        return result

    # ── internals ───────────────────────────────────────────────────────────

    async def _load_corpus(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            # Latest signal per room (same semantics as get_strategy_regression_rooms).
            latest_signal = (
                select(
                    Signal.id.label("signal_id"),
                    Signal.room_id.label("room_id"),
                    Signal.edge_bps.label("edge_bps"),
                    Signal.fair_yes_dollars.label("fair_yes_dollars"),
                    Signal.payload.label("signal_payload"),
                    func.row_number()
                    .over(
                        partition_by=Signal.room_id,
                        order_by=(Signal.created_at.desc(), Signal.id.desc()),
                    )
                    .label("rn"),
                )
                .subquery()
            )

            stmt = (
                select(
                    HistoricalReplayRunRecord.room_id.label("room_id"),
                    HistoricalReplayRunRecord.market_ticker.label("market_ticker"),
                    HistoricalReplayRunRecord.series_ticker.label("run_series_ticker"),
                    HistoricalReplayRunRecord.checkpoint_ts.label("checkpoint_ts"),
                    HistoricalReplayRunRecord.local_market_day.label("local_market_day"),
                    latest_signal.c.signal_id,
                    latest_signal.c.edge_bps,
                    latest_signal.c.fair_yes_dollars,
                    latest_signal.c.signal_payload,
                    HistoricalSettlementLabelRecord.series_ticker.label("label_series_ticker"),
                    HistoricalSettlementLabelRecord.kalshi_result,
                    HistoricalSettlementLabelRecord.settlement_value_dollars,
                )
                .join(
                    latest_signal,
                    (latest_signal.c.room_id == HistoricalReplayRunRecord.room_id)
                    & (latest_signal.c.rn == 1),
                )
                .outerjoin(
                    HistoricalSettlementLabelRecord,
                    HistoricalSettlementLabelRecord.market_ticker
                    == HistoricalReplayRunRecord.market_ticker,
                )
                .where(
                    HistoricalReplayRunRecord.status == "completed",
                    HistoricalReplayRunRecord.room_id.is_not(None),
                    HistoricalReplayRunRecord.market_ticker.like("KXHIGH%"),
                    HistoricalReplayRunRecord.local_market_day >= date_from,
                    HistoricalReplayRunRecord.local_market_day <= date_to,
                    latest_signal.c.edge_bps >= self.settings.risk_min_edge_bps,
                )
                .order_by(
                    HistoricalReplayRunRecord.checkpoint_ts.asc(),
                    HistoricalReplayRunRecord.market_ticker.asc(),
                )
            )
            result = await session.execute(stmt)
            rows: list[dict[str, Any]] = []
            for r in result.mappings():
                row = dict(r)
                # Series ticker: replay run record > settlement label > regex fallback.
                series_ticker = row.get("run_series_ticker") or row.get("label_series_ticker")
                if series_ticker is None:
                    m = _SERIES_TICKER_RE.match(row["market_ticker"])
                    if m:
                        series_ticker = m.group(1)
                row["series_ticker"] = series_ticker
                rows.append(row)
            return rows

    async def _fetch_slope(
        self,
        series_ticker: str | None,
        market_ticker: str,
        before_ts: datetime,
    ) -> float | None:
        if series_ticker is None:
            return None
        window_start = before_ts - timedelta(minutes=_SLOPE_WINDOW_MINUTES)
        try:
            response = await self.kalshi.get_market_candlesticks(
                series_ticker,
                market_ticker,
                period_interval=1,
                start_ts=int(window_start.timestamp()),
                end_ts=int(before_ts.timestamp()),
            )
        except httpx.HTTPStatusError:
            return None

        cutoff_ts = int(before_ts.timestamp())
        points: list[tuple[float, float]] = []
        for candle in response.get("candlesticks") or []:
            ts_raw = candle.get("end_period_ts")
            if ts_raw is None:
                continue
            try:
                ts = int(ts_raw)
            except (ValueError, TypeError):
                continue
            if ts > cutoff_ts:
                continue
            yes_bid = (candle.get("yes_bid") or {}).get("close_dollars")
            yes_ask = (candle.get("yes_ask") or {}).get("close_dollars")
            if yes_bid is None or yes_ask is None:
                continue
            try:
                mid = (float(yes_bid) + float(yes_ask)) / 2.0
            except (ValueError, TypeError):
                continue
            points.append((float(ts), mid))

        if len(points) < _MIN_CANDLE_POINTS:
            return None

        xs = np.array([p[0] for p in points])
        ys = np.array([p[1] for p in points])
        xs = xs - xs[0]
        slope_per_s = float(np.polyfit(xs, ys, 1)[0])
        return slope_per_s * 100.0 * 60.0  # $/s → ¢/min


# ── pure analysis helpers (no I/O, no DB) ────────────────────────────────────


def _build_analysis_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        payload: dict = row["signal_payload"] or {}
        slope_cpmin = payload.get("momentum_slope_cents_per_min")
        recommended_side: str = payload.get("recommended_side") or "yes"

        slope_against: float | None = None
        if slope_cpmin is not None:
            sa = float(slope_cpmin)
            slope_against = sa if recommended_side == "yes" else -sa

        settlement_pnl: float | None = None
        if row.get("settlement_value_dollars") is not None:
            sv = float(row["settlement_value_dollars"])
            fyd = float(row["fair_yes_dollars"])
            settlement_pnl = (sv - fyd) if recommended_side == "yes" else (fyd - sv)

        edge_bps = int(row["edge_bps"])
        edge_dollars = edge_bps / 10000.0
        ratio: float | None = None
        if settlement_pnl is not None and edge_dollars > 0:
            ratio = settlement_pnl / edge_dollars

        records.append(
            {
                "room_id": row["room_id"],
                "market_ticker": row["market_ticker"],
                "local_market_day": row["local_market_day"],
                "checkpoint_ts": row["checkpoint_ts"].isoformat()
                if row["checkpoint_ts"]
                else None,
                "edge_bps": edge_bps,
                "fair_yes_dollars": float(row["fair_yes_dollars"]),
                "recommended_side": recommended_side,
                "slope_cpmin": slope_cpmin,
                "slope_against": slope_against,
                "settlement_pnl": settlement_pnl,
                "ratio": ratio,
                "kalshi_result": row.get("kalshi_result"),
            }
        )
    return records


def _first_look_analysis(records: list[dict[str, Any]]) -> dict[str, Any]:
    n_total = len(records)
    n_with_slope = sum(1 for r in records if r["slope_cpmin"] is not None)
    n_with_settlement = sum(1 for r in records if r["settlement_pnl"] is not None)
    n_usable = sum(
        1
        for r in records
        if r["slope_against"] is not None and r["ratio"] is not None
    )

    slopes = [r["slope_cpmin"] for r in records if r["slope_cpmin"] is not None]
    slope_dist: dict[str, Any] = {}
    if slopes:
        arr = np.array(slopes, dtype=float)
        slope_dist = {
            "mean": float(np.mean(arr)),
            "std": float(np.std(arr)),
            "p10": float(np.percentile(arr, 10)),
            "p50": float(np.percentile(arr, 50)),
            "p90": float(np.percentile(arr, 90)),
        }

    bucket_edges = [float("-inf"), -0.4, -0.2, 0.0, 0.2, float("inf")]
    bucket_labels = ["< -0.4", "-0.4 to -0.2", "-0.2 to 0", "0 to 0.2", "> 0.2"]
    usable = [
        (r["slope_against"], r["ratio"])
        for r in records
        if r["slope_against"] is not None and r["ratio"] is not None
    ]
    buckets: list[dict[str, Any]] = []
    for i, label in enumerate(bucket_labels):
        lo, hi = bucket_edges[i], bucket_edges[i + 1]
        cohort = [rt for sa, rt in usable if lo <= sa < hi]
        b: dict[str, Any] = {"label": label, "count": len(cohort)}
        if cohort:
            b["mean_ratio"] = float(np.mean(cohort))
            b["ci_95_lo"], b["ci_95_hi"] = _bootstrap_ci_mean(cohort)
        buckets.append(b)

    favorable = [rt for sa, rt in usable if sa <= 0.0]
    adverse = [rt for sa, rt in usable if sa > 0.0]
    cohort_cmp: dict[str, Any] = {}
    if favorable and adverse:
        fmean = float(np.mean(favorable))
        amean = float(np.mean(adverse))
        flo, fhi = _bootstrap_ci_mean(favorable)
        alo, ahi = _bootstrap_ci_mean(adverse)
        cohort_cmp = {
            "favorable": {"count": len(favorable), "mean_ratio": fmean, "ci_95_lo": flo, "ci_95_hi": fhi},
            "adverse": {"count": len(adverse), "mean_ratio": amean, "ci_95_lo": alo, "ci_95_hi": ahi},
            "mean_diff_favorable_minus_adverse": fmean - amean,
        }

    return {
        "corpus": {
            "n_total": n_total,
            "n_with_slope": n_with_slope,
            "n_with_settlement": n_with_settlement,
            "n_usable": n_usable,
            "null_slope_rate": round(1.0 - n_with_slope / n_total, 4) if n_total else None,
        },
        "slope_distribution": slope_dist,
        "buckets": buckets,
        "cohort_comparison": cohort_cmp,
    }


def _deploy_analysis(records: list[dict[str, Any]]) -> dict[str, Any]:
    base = _first_look_analysis(records)

    adverse_pairs = [
        (r["slope_against"], r["ratio"])
        for r in records
        if r["slope_against"] is not None
        and r["slope_against"] > 0
        and r["ratio"] is not None
    ]

    fit: dict[str, Any] = {}
    veto_candidates: list[dict[str, Any]] = []

    if len(adverse_pairs) >= 10:
        xs = np.array([sa for sa, _ in adverse_pairs], dtype=float)
        ys = np.array([rt for _, rt in adverse_pairs], dtype=float)

        # OLS through origin: ratio ≈ 1 - slope_against/scale
        # → 1 - ratio ≈ slope_against/scale  → scale = Σx² / Σx(1-y)
        denom = float(xs @ (1.0 - ys))
        if denom > 0:
            scale_fit = float(xs @ xs) / denom
            residuals = (1.0 - ys) - xs / scale_fit
            rmse = float(np.sqrt(np.mean(residuals**2)))

            rng = np.random.default_rng(42)
            boot_scales: list[float] = []
            for _ in range(1000):
                idx = rng.integers(0, len(xs), size=len(xs))
                bx, by = xs[idx], ys[idx]
                bd = float(bx @ (1.0 - by))
                if bd > 0:
                    boot_scales.append(float(bx @ bx) / bd)

            ci_lo = ci_hi = ci_frac = None
            if boot_scales:
                ci_lo = float(np.percentile(boot_scales, 2.5))
                ci_hi = float(np.percentile(boot_scales, 97.5))
                if scale_fit != 0:
                    ci_frac = round((ci_hi - ci_lo) / abs(scale_fit), 4)

            fit = {
                "scale_fit": round(scale_fit, 4),
                "rmse": round(rmse, 4),
                "n_adverse": len(adverse_pairs),
                "ci_95_lo": round(ci_lo, 4) if ci_lo is not None else None,
                "ci_95_hi": round(ci_hi, 4) if ci_hi is not None else None,
                "ci_width_fraction": ci_frac,
            }

            # Three veto candidates where the weight function crosses thresholds.
            # ratio_at_threshold  = max(floor, 1 - slope/scale)
            # slope_at_threshold  = scale * (1 - threshold)
            veto_candidates = [
                {
                    "label": "ratio_at_floor",
                    "threshold_ratio": _MOMENTUM_WEIGHT_FLOOR,
                    "slope_against_cents_per_min": round(scale_fit * (1.0 - _MOMENTUM_WEIGHT_FLOOR), 4),
                },
                {
                    "label": "ratio_at_0.5",
                    "threshold_ratio": 0.5,
                    "slope_against_cents_per_min": round(scale_fit * 0.5, 4),
                },
                {
                    "label": "ratio_at_0.7",
                    "threshold_ratio": 0.7,
                    "slope_against_cents_per_min": round(scale_fit * 0.3, 4),
                },
            ]

    return {
        **base,
        "scale_fit": fit,
        "veto_candidates": veto_candidates,
        "run_at": datetime.now(UTC).isoformat(),
    }


def _bootstrap_ci_mean(values: list[float], n_boot: int = 1000) -> tuple[float, float]:
    rng = np.random.default_rng(42)
    arr = np.array(values, dtype=float)
    boot_means = np.fromiter(
        (rng.choice(arr, size=len(arr), replace=True).mean() for _ in range(n_boot)),
        dtype=float,
        count=n_boot,
    )
    return float(np.percentile(boot_means, 2.5)), float(np.percentile(boot_means, 97.5))


def _print_first_look(report: dict[str, Any]) -> None:
    corpus = report["corpus"]
    print(
        f"\n── Corpus ──────────────────────────────────────────────────────────\n"
        f"  Total rooms : {corpus['n_total']}\n"
        f"  With slope  : {corpus['n_with_slope']}  (null rate: {corpus.get('null_slope_rate', '?'):.1%})\n"
        f"  With P&L    : {corpus['n_with_settlement']}\n"
        f"  Usable      : {corpus['n_usable']}\n"
    )

    sd = report.get("slope_distribution", {})
    if sd:
        print(
            f"── Slope distribution (¢/min, signed per recommended side) ─────────\n"
            f"  mean={sd['mean']:.3f}  std={sd['std']:.3f}  "
            f"p10={sd['p10']:.3f}  p50={sd['p50']:.3f}  p90={sd['p90']:.3f}\n"
        )

    print("── Bucket table (slope_against ¢/min) ──────────────────────────────")
    print(f"  {'Bucket':<18}  {'N':>5}  {'mean ratio':>10}  {'95% CI':>22}")
    print(f"  {'-'*18}  {'-'*5}  {'-'*10}  {'-'*22}")
    for b in report["buckets"]:
        ci = (
            f"[{b['ci_95_lo']:+.3f}, {b['ci_95_hi']:+.3f}]"
            if "ci_95_lo" in b
            else "—"
        )
        mr = f"{b['mean_ratio']:+.3f}" if "mean_ratio" in b else "—"
        print(f"  {b['label']:<18}  {b['count']:>5}  {mr:>10}  {ci:>22}")

    cc = report.get("cohort_comparison", {})
    if cc:
        print(
            f"\n── Cohort comparison ────────────────────────────────────────────────\n"
            f"  Favorable (slope_against ≤ 0): n={cc['favorable']['count']}  "
            f"mean={cc['favorable']['mean_ratio']:+.3f}  "
            f"95%CI=[{cc['favorable']['ci_95_lo']:+.3f},{cc['favorable']['ci_95_hi']:+.3f}]\n"
            f"  Adverse   (slope_against > 0): n={cc['adverse']['count']}  "
            f"mean={cc['adverse']['mean_ratio']:+.3f}  "
            f"95%CI=[{cc['adverse']['ci_95_lo']:+.3f},{cc['adverse']['ci_95_hi']:+.3f}]\n"
            f"  Mean diff (fav − adv): {cc['mean_diff_favorable_minus_adverse']:+.3f}\n"
        )


def _print_deploy(result: dict[str, Any]) -> None:
    _print_first_look(result)

    fit = result.get("scale_fit", {})
    if fit:
        ci_str = (
            f"[{fit['ci_95_lo']:.4f}, {fit['ci_95_hi']:.4f}]"
            if fit.get("ci_95_lo") is not None
            else "—"
        )
        print(
            f"── One-parameter fit ────────────────────────────────────────────────\n"
            f"  scale_fit          = {fit['scale_fit']:.4f} ¢/min\n"
            f"  95% bootstrap CI   = {ci_str}\n"
            f"  CI width fraction  = {fit.get('ci_width_fraction', '?')}\n"
            f"  RMSE               = {fit['rmse']:.4f}\n"
            f"  n_adverse          = {fit['n_adverse']}\n"
        )

    vetos = result.get("veto_candidates", [])
    if vetos:
        print("── Veto candidates (momentum_slope_veto_cents_per_min) ─────────────")
        for v in vetos:
            slope = v.get("slope_against_cents_per_min")
            slope_str = f"{slope:.4f}" if slope is not None else "—"
            print(f"  {v['label']:<22}  slope_against = {slope_str} ¢/min  (ratio threshold = {v['threshold_ratio']:.2f})")
        print()


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, default=str))
            fh.write("\n")
