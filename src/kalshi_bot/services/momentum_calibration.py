"""
Step-2 calibration tooling for the momentum entry-weight feature.

Public commands (run in order):
  backfill_slopes  — fetch Kalshi 1-min candlesticks and write
                     momentum_slope_cents_per_min to Signal.payload (write-if-absent).
  preview          — full analysis (fit + buckets + bootstrap CIs), read-only, never writes.
  stage            — full analysis + sanity bounds + write pending_momentum_calibration:{env}.
  promote          — atomic rename pending → active + ops_event.
  reject           — clear pending + ops_event (idempotent).
  status           — print current active + pending calibration state.

Consumer interface (used by Step 3's post-processor and veto gate, inert in Phase 1):
  MomentumCalibrationParams — frozen dataclass of the four runtime parameters.
  get_active_momentum_calibration(repo, settings) — live-read helper, per-field fallback to Settings.

`get_active_momentum_calibration()` is consumed by Step 3's post-processor and veto gate.
Phase 1 defines the contract and enforces its fallback behavior via tests;
Phase 1 does not call this helper at runtime.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import numpy as np
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import HistoricalReplayRunRecord, HistoricalSettlementLabelRecord, Signal
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient

logger = logging.getLogger(__name__)

_SERIES_TICKER_RE = re.compile(r"^(KXHIGH[A-Z]+)-[A-Z0-9]+-T\d+")
_SLOPE_WINDOW_MINUTES = 60
_MIN_CANDLE_POINTS = 5
_PENDING_STALE_HOURS = 24
_CALIBRATION_SCRIPT_VERSION = "1.0"

# Sanity bounds enforced by `stage` before writing a pending checkpoint.
_SCALE_MIN = 0.1
_SCALE_MAX = 10.0
_CI_WIDTH_FRACTION_MAX = 0.5


# ── Consumer interface (Phase 1 contract; wired into supervisor in Step 3) ───


@dataclass(frozen=True)
class MomentumCalibrationParams:
    """Runtime parameters consumed by Step 3's post-processor and veto gate."""

    momentum_weight_scale_cents_per_min: float
    momentum_slope_veto_cents_per_min: float | None
    momentum_weight_floor: float
    momentum_veto_staleness_gate: float


def get_active_momentum_calibration(
    repo: PlatformRepository,
    settings: Settings,
) -> MomentumCalibrationParams:
    """
    Return calibration params from the active checkpoint, with per-field fallback to Settings.

    This function is SYNC — callers must have already awaited the checkpoint read.
    For the async version (to be wired in Step 3), use get_active_momentum_calibration_async().
    """
    raise NotImplementedError(
        "Use get_active_momentum_calibration_async() for async contexts; "
        "this sync wrapper is a placeholder for the Step 3 wiring."
    )


async def get_active_momentum_calibration_async(
    repo: PlatformRepository,
    settings: Settings,
) -> MomentumCalibrationParams:
    """
    Read the active momentum_calibration:{env} checkpoint and return typed params.
    Falls back per-field to Settings defaults when the checkpoint is absent or a field is missing.
    Partial checkpoints (missing individual fields) are supported for forward-compat.
    """
    cp = await repo.get_checkpoint(f"momentum_calibration:{settings.kalshi_env}")
    payload: dict[str, Any] = (cp.payload if cp is not None else {}) or {}

    def _get(key: str, default: Any) -> Any:
        v = payload.get(key)
        return v if v is not None else default

    return MomentumCalibrationParams(
        momentum_weight_scale_cents_per_min=float(
            _get("momentum_weight_scale_cents_per_min", settings.momentum_weight_scale_cents_per_min)
        ),
        momentum_slope_veto_cents_per_min=(
            float(payload["momentum_slope_veto_cents_per_min"])
            if payload.get("momentum_slope_veto_cents_per_min") is not None
            else settings.momentum_slope_veto_cents_per_min
        ),
        momentum_weight_floor=float(
            _get("momentum_weight_floor", settings.momentum_weight_floor)
        ),
        momentum_veto_staleness_gate=float(
            _get("momentum_veto_staleness_gate", settings.momentum_veto_staleness_gate)
        ),
    )


async def get_momentum_calibration_state(
    repo: PlatformRepository,
    kalshi_env: str,
) -> dict[str, Any]:
    """
    Return the full calibration state (active + pending) for status display and control-room.
    Different projection from get_active_momentum_calibration_async — includes audit metadata.
    """
    active_cp = await repo.get_checkpoint(f"momentum_calibration:{kalshi_env}")
    pending_cp = await repo.get_checkpoint(f"pending_momentum_calibration:{kalshi_env}")

    def _cp_to_dict(cp: Any) -> dict[str, Any] | None:
        if cp is None:
            return None
        return dict(cp.payload or {})

    state = {
        "active": _cp_to_dict(active_cp),
        "pending": _cp_to_dict(pending_cp),
    }
    if pending_cp is not None and state["pending"]:
        staged_at_raw = state["pending"].get("staged_at")
        if staged_at_raw:
            try:
                staged_at = datetime.fromisoformat(staged_at_raw)
                age_hours = (datetime.now(UTC) - staged_at).total_seconds() / 3600
                state["pending_age_hours"] = round(age_hours, 2)
                state["pending_is_stale"] = age_hours >= _PENDING_STALE_HOURS
            except (ValueError, TypeError):
                pass
    return state


# ── Service class ─────────────────────────────────────────────────────────────


class MomentumCalibrationService:
    """CLI-facing service for the Step-2 calibration workflow."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        kalshi_client: KalshiClient,
        settings: Settings,
    ) -> None:
        self.session_factory = session_factory
        self.kalshi = kalshi_client
        self.settings = settings

    # ── public commands ──────────────────────────────────────────────────────

    async def backfill_slopes(self, date_from: str, date_to: str) -> dict[str, Any]:
        """Fetch 60-min candlestick slopes and write to Signal.payload (write-if-absent)."""
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

    async def preview(
        self,
        date_from: str,
        date_to: str,
        output_path: Path | None = None,
    ) -> dict[str, Any]:
        """Full analysis (fit + buckets + CIs). Read-only — never writes DB state."""
        rows = await self._load_corpus(date_from, date_to)
        records = _build_analysis_records(rows)
        result = _deploy_analysis(records)
        if output_path is not None:
            _write_jsonl(output_path, records)
        _print_deploy(result)
        return result

    async def stage(
        self,
        date_from: str,
        date_to: str,
        min_observations: int = 1000,
        staged_by: str | None = None,
        force: bool = False,
        output_path: Path | None = None,
    ) -> dict[str, Any]:
        """
        Run full analysis, enforce sanity bounds, write pending_momentum_calibration:{env}.
        Exits (returns error dict) without writing if bounds or corpus size fail.
        """
        rows = await self._load_corpus(date_from, date_to)
        records = _build_analysis_records(rows)
        n_usable = sum(
            1
            for r in records
            if r["slope_against"] is not None and r["ratio"] is not None
        )
        if n_usable < min_observations:
            return {
                "ok": False,
                "error": f"insufficient observations: {n_usable} < {min_observations}",
                "n_usable": n_usable,
            }

        result = _deploy_analysis(records)
        fit = result.get("scale_fit") or {}
        scale = fit.get("scale_fit")
        ci_frac = fit.get("ci_width_fraction")

        # Sanity bounds.
        if scale is None:
            return {"ok": False, "error": "fit did not converge (insufficient adverse cohort)"}
        if not (_SCALE_MIN <= scale <= _SCALE_MAX):
            return {
                "ok": False,
                "error": f"scale {scale:.4f} outside bounds [{_SCALE_MIN}, {_SCALE_MAX}]",
                "scale": scale,
            }
        veto_candidates = result.get("veto_candidates") or []
        proposed_veto = veto_candidates[0].get("slope_against_cents_per_min") if veto_candidates else None
        if proposed_veto is not None and proposed_veto < 0:
            return {
                "ok": False,
                "error": f"veto candidate {proposed_veto:.4f} < 0 (must be non-negative)",
                "proposed_veto": proposed_veto,
            }
        if ci_frac is not None and ci_frac > _CI_WIDTH_FRACTION_MAX:
            return {
                "ok": False,
                "error": f"CI width fraction {ci_frac:.4f} > {_CI_WIDTH_FRACTION_MAX} (fit too uncertain)",
                "ci_width_fraction": ci_frac,
            }

        pending_key = f"pending_momentum_calibration:{self.settings.kalshi_env}"
        active_key = f"momentum_calibration:{self.settings.kalshi_env}"

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            existing_pending = await repo.get_checkpoint(pending_key)
            if existing_pending is not None:
                staged_at_raw = (existing_pending.payload or {}).get("staged_at")
                if staged_at_raw:
                    try:
                        staged_at = datetime.fromisoformat(staged_at_raw)
                        age_h = (datetime.now(UTC) - staged_at).total_seconds() / 3600
                        if age_h >= _PENDING_STALE_HOURS and not force:
                            return {
                                "ok": False,
                                "error": (
                                    f"stale pending exists (staged {age_h:.1f}h ago). "
                                    f"Run with --force to overwrite, or `reject` to discard."
                                ),
                                "staged_at": staged_at_raw,
                            }
                        logger.info(
                            "Replacing pending staged at %s with new fit (age %.1fh)",
                            staged_at_raw,
                            age_h,
                        )
                    except (ValueError, TypeError):
                        pass

            active_cp = await repo.get_checkpoint(active_key)
            active_payload = dict((active_cp.payload or {}) if active_cp else {})
            previous_scale = active_payload.get("momentum_weight_scale_cents_per_min")
            previous_veto = active_payload.get("momentum_slope_veto_cents_per_min")

            now_iso = datetime.now(UTC).isoformat()
            who = staged_by or os.getenv("USER") or "cli"
            checkpoint_payload: dict[str, Any] = {
                "momentum_weight_scale_cents_per_min": scale,
                "momentum_slope_veto_cents_per_min": proposed_veto,
                "momentum_weight_floor": self.settings.momentum_weight_floor,
                "momentum_veto_staleness_gate": self.settings.momentum_veto_staleness_gate,
                "corpus_n_usable": n_usable,
                "corpus_date_from": date_from,
                "corpus_date_to": date_to,
                "ci_95_lo": fit.get("ci_95_lo"),
                "ci_95_hi": fit.get("ci_95_hi"),
                "ci_width_fraction": ci_frac,
                "previous_scale": previous_scale,
                "previous_veto": previous_veto,
                "staged_at": now_iso,
                "staged_by": who,
                "provenance": "manual",
                "calibration_script_version": _CALIBRATION_SCRIPT_VERSION,
            }

            await repo.set_checkpoint(pending_key, cursor=None, payload=checkpoint_payload)
            await repo.log_ops_event(
                severity="info",
                summary=f"Momentum calibration staged: scale={scale:.4f}, veto={proposed_veto}",
                source="momentum_calibration",
                payload=checkpoint_payload,
            )
            await session.commit()

        if output_path is not None:
            _write_jsonl(output_path, records)
        _print_deploy(result)
        _print_stage_summary(checkpoint_payload)
        return {"ok": True, "checkpoint": checkpoint_payload, **result}

    async def promote(self, activated_by: str | None = None) -> dict[str, Any]:
        """Atomically rename pending → active. Emits ops_event. Non-zero return if no pending."""
        pending_key = f"pending_momentum_calibration:{self.settings.kalshi_env}"
        active_key = f"momentum_calibration:{self.settings.kalshi_env}"

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            pending_cp = await repo.get_checkpoint(pending_key)
            if pending_cp is None:
                return {"ok": False, "error": "no pending calibration to promote"}

            who = activated_by or os.getenv("USER") or "cli"
            now_iso = datetime.now(UTC).isoformat()
            active_payload = {
                **(pending_cp.payload or {}),
                "activated_at": now_iso,
                "activated_by": who,
            }
            await repo.set_checkpoint(active_key, cursor=None, payload=active_payload)

            # Delete the pending checkpoint by overwriting with an empty sentinel, then
            # delete via raw update (set_checkpoint upserts, so we delete the row directly).
            from sqlalchemy import delete as sa_delete
            from kalshi_bot.db.models import Checkpoint
            await session.execute(
                sa_delete(Checkpoint).where(Checkpoint.stream_name == pending_key)
            )

            await repo.log_ops_event(
                severity="info",
                summary=(
                    f"Momentum calibration activated: "
                    f"scale={active_payload.get('momentum_weight_scale_cents_per_min')}"
                ),
                source="momentum_calibration",
                payload=active_payload,
            )
            await session.commit()

        print(f"Promoted. Active scale={active_payload.get('momentum_weight_scale_cents_per_min'):.4f}  "
              f"veto={active_payload.get('momentum_slope_veto_cents_per_min')}")
        return {"ok": True, "active": active_payload}

    async def reject(self) -> dict[str, Any]:
        """Clear pending calibration (idempotent — exit 0 if nothing to reject)."""
        pending_key = f"pending_momentum_calibration:{self.settings.kalshi_env}"

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            from sqlalchemy import delete as sa_delete
            from kalshi_bot.db.models import Checkpoint

            pending_cp = await repo.get_checkpoint(pending_key)
            if pending_cp is None:
                print("No pending calibration to reject.")
                return {"ok": True, "action": "noop"}

            payload = dict(pending_cp.payload or {})
            await session.execute(
                sa_delete(Checkpoint).where(Checkpoint.stream_name == pending_key)
            )
            await repo.log_ops_event(
                severity="info",
                summary="Momentum calibration pending rejected",
                source="momentum_calibration",
                payload=payload,
            )
            await session.commit()

        print("Pending calibration rejected.")
        return {"ok": True, "action": "rejected", "rejected_payload": payload}

    async def status(self) -> dict[str, Any]:
        """Print and return current active + pending calibration state."""
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            state = await get_momentum_calibration_state(repo, self.settings.kalshi_env)
            await session.commit()
        _print_status(state)
        return state

    # ── internals ────────────────────────────────────────────────────────────

    async def _load_corpus(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
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
                series_ticker = row.get("run_series_ticker") or row.get("label_series_ticker")
                if series_ticker is None:
                    m = _SERIES_TICKER_RE.match(row["market_ticker"])
                    if m:
                        series_ticker = m.group(1)
                    else:
                        logger.warning("Could not derive series_ticker for %s", row["market_ticker"])
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
        return float(np.polyfit(xs, ys, 1)[0]) * 100.0 * 60.0  # $/s → ¢/min


# ── pure analysis helpers ─────────────────────────────────────────────────────


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
            # Counterfactual P&L: frictionless, uses fair_yes_dollars (not fill price).
            # YES side: (settled_yes - fair_yes). NO side: (fair_yes - settled_yes).
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
                "checkpoint_ts": (
                    row["checkpoint_ts"].isoformat() if row["checkpoint_ts"] else None
                ),
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
            "favorable": {
                "count": len(favorable),
                "mean_ratio": fmean,
                "ci_95_lo": flo,
                "ci_95_hi": fhi,
            },
            "adverse": {
                "count": len(adverse),
                "mean_ratio": amean,
                "ci_95_lo": alo,
                "ci_95_hi": ahi,
            },
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
        # OLS through origin: 1 - ratio ≈ slope_against / scale → scale = Σx² / Σx(1-y)
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

            floor = 0.3  # Step 3 config key default; hardcoded here (pre-Step-3)
            veto_candidates = [
                {
                    "label": "ratio_at_floor",
                    "threshold_ratio": floor,
                    "slope_against_cents_per_min": round(scale_fit * (1.0 - floor), 4),
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


# ── print helpers ─────────────────────────────────────────────────────────────


def _print_deploy(result: dict[str, Any]) -> None:
    corpus = result["corpus"]
    print(
        f"\n── Corpus ──────────────────────────────────────────────────────────\n"
        f"  Total rooms : {corpus['n_total']}\n"
        f"  With slope  : {corpus['n_with_slope']}  "
        f"(null rate: {corpus.get('null_slope_rate', 0):.1%})\n"
        f"  With P&L    : {corpus['n_with_settlement']}\n"
        f"  Usable      : {corpus['n_usable']}\n"
    )

    sd = result.get("slope_distribution", {})
    if sd:
        print(
            f"── Slope distribution (¢/min) ───────────────────────────────────────\n"
            f"  mean={sd['mean']:.3f}  std={sd['std']:.3f}  "
            f"p10={sd['p10']:.3f}  p50={sd['p50']:.3f}  p90={sd['p90']:.3f}\n"
        )

    print("── Bucket table (slope_against ¢/min) ──────────────────────────────")
    print(f"  {'Bucket':<18}  {'N':>5}  {'mean ratio':>10}  {'95% CI':>22}")
    print(f"  {'-'*18}  {'-'*5}  {'-'*10}  {'-'*22}")
    for b in result["buckets"]:
        ci = (
            f"[{b['ci_95_lo']:+.3f}, {b['ci_95_hi']:+.3f}]"
            if "ci_95_lo" in b
            else "—"
        )
        mr = f"{b['mean_ratio']:+.3f}" if "mean_ratio" in b else "—"
        print(f"  {b['label']:<18}  {b['count']:>5}  {mr:>10}  {ci:>22}")

    cc = result.get("cohort_comparison", {})
    if cc:
        fav = cc["favorable"]
        adv = cc["adverse"]
        print(
            f"\n── Cohort comparison ────────────────────────────────────────────────\n"
            f"  Favorable: n={fav['count']}  mean={fav['mean_ratio']:+.3f}  "
            f"CI=[{fav['ci_95_lo']:+.3f},{fav['ci_95_hi']:+.3f}]\n"
            f"  Adverse:   n={adv['count']}  mean={adv['mean_ratio']:+.3f}  "
            f"CI=[{adv['ci_95_lo']:+.3f},{adv['ci_95_hi']:+.3f}]\n"
            f"  Mean diff (fav − adv): {cc['mean_diff_favorable_minus_adverse']:+.3f}\n"
        )

    fit = result.get("scale_fit", {})
    if fit:
        ci_str = (
            f"[{fit['ci_95_lo']:.4f}, {fit['ci_95_hi']:.4f}]"
            if fit.get("ci_95_lo") is not None
            else "—"
        )
        print(
            f"── One-parameter fit ────────────────────────────────────────────────\n"
            f"  scale_fit        = {fit['scale_fit']:.4f} ¢/min\n"
            f"  95% bootstrap CI = {ci_str}\n"
            f"  CI width frac    = {fit.get('ci_width_fraction', '?')}\n"
            f"  RMSE             = {fit['rmse']:.4f}\n"
            f"  n_adverse        = {fit['n_adverse']}\n"
        )

    vetos = result.get("veto_candidates", [])
    if vetos:
        print("── Veto candidates (momentum_slope_veto_cents_per_min) ─────────────")
        for v in vetos:
            slope = v.get("slope_against_cents_per_min")
            slope_str = f"{slope:.4f}" if slope is not None else "—"
            print(
                f"  {v['label']:<22}  slope_against = {slope_str} ¢/min  "
                f"(ratio threshold = {v['threshold_ratio']:.2f})"
            )
        print()


def _print_stage_summary(payload: dict[str, Any]) -> None:
    prev_scale = payload.get("previous_scale")
    new_scale = payload.get("momentum_weight_scale_cents_per_min")
    delta_str = ""
    if prev_scale is not None and new_scale is not None and prev_scale != 0:
        pct = (new_scale - prev_scale) / abs(prev_scale) * 100
        delta_str = f" ({pct:+.1f}% from {prev_scale})"
    print(
        f"\n── Staged ───────────────────────────────────────────────────────────\n"
        f"  scale  = {new_scale}{delta_str}\n"
        f"  veto   = {payload.get('momentum_slope_veto_cents_per_min')}\n"
        f"  staged_by = {payload.get('staged_by')}\n"
        f"  Run `calibrate-momentum promote` to activate.\n"
    )


def _print_status(state: dict[str, Any]) -> None:
    active = state.get("active")
    pending = state.get("pending")
    print("── Active calibration ───────────────────────────────────────────────")
    if active:
        print(f"  scale  = {active.get('momentum_weight_scale_cents_per_min')}")
        print(f"  veto   = {active.get('momentum_slope_veto_cents_per_min')}")
        print(f"  activated_at = {active.get('activated_at')}")
        print(f"  activated_by = {active.get('activated_by')}")
    else:
        print("  (none — Settings defaults apply)")
    print("\n── Pending calibration ──────────────────────────────────────────────")
    if pending:
        age_h = state.get("pending_age_hours", "?")
        stale = state.get("pending_is_stale", False)
        stale_tag = " ⚠ STALE" if stale else ""
        print(f"  scale  = {pending.get('momentum_weight_scale_cents_per_min')}")
        print(f"  veto   = {pending.get('momentum_slope_veto_cents_per_min')}")
        print(f"  staged_at = {pending.get('staged_at')} ({age_h}h ago{stale_tag})")
        print(f"  staged_by = {pending.get('staged_by')}")
        print("  Run `calibrate-momentum promote` to activate.")
    else:
        print("  (none)")
    print()


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, default=str))
            fh.write("\n")
