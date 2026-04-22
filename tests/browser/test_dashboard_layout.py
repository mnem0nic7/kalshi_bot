from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request

import pytest
import uvicorn

from kalshi_bot.config import get_settings
import kalshi_bot.web.app as web_app_module
from kalshi_bot.web.app import create_app

playwright_sync_api = pytest.importorskip("playwright.sync_api")
Page = playwright_sync_api.Page
sync_playwright = playwright_sync_api.sync_playwright

VIEWPORTS = [
    pytest.param("desktop", {"width": 1280, "height": 1400}, id="desktop"),
    pytest.param("narrow", {"width": 760, "height": 1400}, id="narrow"),
]


def _build_positions(count: int) -> list[dict[str, object]]:
    positions: list[dict[str, object]] = []
    for idx in range(count):
        positions.append(
            {
                "market_ticker": f"KXTEST-{idx:02d}",
                "model_quality_status": "ok",
                "trade_regime": "normal",
                "warn_only_blocked": False,
                "recommended_size_cap_fp": None,
                "count_fp": "10",
                "side": "yes",
                "average_price_display": "$0.45",
                "current_price_display": "$0.48",
                "notional_display": "$4.80",
                "unrealized_pnl_tone": "good",
                "unrealized_pnl_display": "+$0.30",
                "model_quality_reasons": [],
            }
        )
    return positions


def _build_recent_trade_proposals() -> list[dict[str, object]]:
    return [
        {
            "market_ticker": "KXHIGHTPHX-26APR23-T92",
            "side": "yes",
            "side_tone": "good",
            "yes_price_dollars": "0.0400",
            "count_fp": "93.75",
            "status": "proposed",
            "status_tone": "neutral",
            "risk_status": "blocked",
            "risk_status_tone": "bad",
            "approved_notional_dollars": None,
        },
        {
            "market_ticker": "KXHIGHNY-26APR23-T75",
            "side": "yes",
            "side_tone": "good",
            "yes_price_dollars": "0.4000",
            "count_fp": "12.50",
            "status": "proposed",
            "status_tone": "neutral",
            "risk_status": "approved",
            "risk_status_tone": "good",
            "approved_notional_dollars": "5.0000",
        },
    ]


def _build_env_payload(
    cash_display: str,
    positions_count: int,
    *,
    daily_pnl_display: str = "+$42.00",
    daily_pnl_tone: str = "good",
    daily_pnl_line_display: str = "+$42.00 (0.42%) today (PT)",
    total_value_dollars: str | None = "$115.20",
    total_value_display: str = "$115.20",
    total_value_label: str = "Current",
    total_value_is_marked: bool = True,
    total_notional_display: str = "$108.00",
    total_unrealized_pnl_display: str = "+$7.20",
    total_unrealized_pnl_tone: str = "good",
    has_pnl_summary: bool = True,
    recent_trade_proposals: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "portfolio": {
            "cash_display": cash_display,
            "portfolio_display": "$9,999.00",
            "positions_value_display": "$1,250.00",
            "gain_loss_display": "+$42.00",
            "gain_loss_tone": "good",
        },
        "daily_pnl_dollars": None,
        "daily_pnl_display": daily_pnl_display,
        "daily_pnl_tone": daily_pnl_tone,
        "daily_pnl_line_display": daily_pnl_line_display,
        "alerts": [],
        "active_rooms": [],
        "positions": _build_positions(positions_count),
        "recent_trade_proposals": recent_trade_proposals or [],
        "positions_summary": {
            "capital_buckets": None,
            "total_current_value_dollars": total_value_dollars if total_value_is_marked else None,
            "total_current_value_display": total_value_display if total_value_is_marked else "$0.00",
            "total_value_dollars": total_value_dollars,
            "total_value_display": total_value_display,
            "total_value_label": total_value_label,
            "total_value_is_marked": total_value_is_marked,
            "total_notional_display": total_notional_display,
            "total_unrealized_pnl_display": total_unrealized_pnl_display,
            "total_unrealized_pnl_tone": total_unrealized_pnl_tone,
            "has_pnl_summary": has_pnl_summary,
        },
    }


def _build_strategies_payload(
    window_days: int = 180,
    *,
    selected_series_ticker: str | None = None,
    selected_strategy_name: str | None = None,
) -> dict[str, object]:
    review_available = window_days == 180
    inactive_review = {
        "status": None,
        "label": "180d only",
        "reason": "Assignment review is based only on the latest stored 180d snapshot.",
        "needs_review": False,
        "basis_window_days": 180,
    }
    leaderboard = [
        {
            "name": "moderate",
            "description": "Balanced filters matching current live settings.",
            "selected": selected_series_ticker is None and selected_strategy_name != "aggressive",
            "threshold_groups": [
                {"label": "Risk", "items": [{"label": "Risk Min Edge Bps", "value": "40"}]},
                {"label": "Strategy", "items": [{"label": "Strategy Quality Edge Buffer Bps", "value": "20"}]},
            ],
            "overall_win_rate": 0.73,
            "overall_win_rate_display": "73%",
            "overall_trade_rate": 0.58,
            "overall_trade_rate_display": "58%",
            "total_pnl_dollars": 18.4,
            "total_pnl_display": "+$18.40",
            "avg_edge_bps": 62.0,
            "avg_edge_bps_display": "62bps",
            "total_rooms_evaluated": 44,
            "total_rooms_evaluated_display": "44",
            "total_trade_count": 26,
            "total_trade_count_display": "26",
            "total_resolved_trade_count": 26,
            "total_resolved_trade_count_display": "26",
            "total_unscored_trade_count": 0,
            "total_unscored_trade_count_display": "0",
            "outcome_coverage_rate": 1.0,
            "outcome_coverage_rate_display": "100%",
            "outcome_coverage_display": "26/26 scored",
            "cities_led": 1,
            "assigned_city_count": 1,
        },
        {
            "name": "aggressive",
            "description": "Loose filters, higher trade frequency.",
            "selected": selected_strategy_name == "aggressive",
            "threshold_groups": [
                {"label": "Risk", "items": [{"label": "Risk Min Edge Bps", "value": "20"}]},
                {"label": "Strategy", "items": [{"label": "Strategy Quality Edge Buffer Bps", "value": "0"}]},
            ],
            "overall_win_rate": 0.60,
            "overall_win_rate_display": "60%",
            "overall_trade_rate": 0.50,
            "overall_trade_rate_display": "50%",
            "total_pnl_dollars": 5.0,
            "total_pnl_display": "+$5.00",
            "avg_edge_bps": 75.0,
            "avg_edge_bps_display": "75bps",
            "total_rooms_evaluated": 44,
            "total_rooms_evaluated_display": "44",
            "total_trade_count": 20,
            "total_trade_count_display": "20",
            "total_resolved_trade_count": 20,
            "total_resolved_trade_count_display": "20",
            "total_unscored_trade_count": 0,
            "total_unscored_trade_count_display": "0",
            "outcome_coverage_rate": 1.0,
            "outcome_coverage_rate_display": "100%",
            "outcome_coverage_display": "20/20 scored",
            "cities_led": 0,
            "assigned_city_count": 1,
        },
    ]
    ny_recommendation_status = "strong_recommendation"
    ny_recommendation_label = "Strong recommendation"
    ny_review = {
        "status": "drifted_assignment",
        "label": "Drifted assignment",
        "reason": "The current canonical assignment (aggressive) no longer matches the latest 180d recommendation (moderate).",
        "needs_review": True,
        "basis_window_days": 180,
    } if review_available else inactive_review
    chi_review = {
        "status": "aligned",
        "label": "Aligned",
        "reason": "The current canonical assignment still matches the latest eligible 180d recommendation.",
        "needs_review": False,
        "basis_window_days": 180,
    } if review_available else inactive_review
    ny_approval_eligible = review_available
    city_matrix = [
        {
            "series_ticker": "KXHIGHNY",
            "city_label": "New York City",
            "location_name": "New York City",
            "selected": selected_series_ticker == "KXHIGHNY",
            "assignment": {"strategy_name": "aggressive", "assigned_at": "2026-04-21T18:00:00+00:00", "assigned_by": "auto_regression"},
            "assignment_context_status": "differs_from_recommendation",
            "best_strategy": "moderate",
            "best_strategy_win_rate": 0.75,
            "best_strategy_win_rate_display": "75%",
            "best_resolved_trade_count": 12,
            "best_resolved_trade_count_display": "12",
            "best_outcome_coverage_display": "12/12 scored",
            "runner_up_strategy": "aggressive",
            "runner_up_win_rate_display": "60%",
            "gap_to_runner_up": 0.15,
            "gap_to_runner_up_display": "15%",
            "gap_to_assignment": 0.15,
            "gap_to_assignment_display": "15%",
            "evidence_status": "strong",
            "evidence_label": "Strong recommendation",
            "trade_count_sufficient": True,
            "resolved_trade_count_sufficient": True,
            "outcome_coverage_sufficient": True,
            "gap_threshold_sufficient": True,
            "lean_gap_sufficient": True,
            "assignment_gap_sufficient": True,
            "assignment_status": ny_recommendation_status,
            "assignment_status_label": ny_recommendation_label,
            "recommendation": {
                "strategy_name": "moderate",
                "status": ny_recommendation_status,
                "label": ny_recommendation_label,
                "resolved_trade_count": 12,
                "resolved_trade_count_display": "12",
                "outcome_coverage_rate": 1.0,
                "outcome_coverage_rate_display": "100%",
                "gap_to_runner_up": 0.15,
                "gap_to_runner_up_display": "15%",
                "writes_assignment": False,
            },
            "review": ny_review,
            "approval_eligible": ny_approval_eligible,
            "approval_label": "Ready to approve" if ny_approval_eligible else "180d only",
            "approval_window_days": 180,
            "approval_requires_note": True,
            "approval_reason": "Manual approval validates against the latest stored 180d snapshot.",
            "can_promote": False,
            "sort_priority": 0 if review_available else 1,
            "metrics": [
                {"strategy_name": "moderate", "selected": selected_strategy_name == "moderate", "is_assigned": False, "is_best": True, "is_runner_up": False, "rooms_evaluated": 20, "trade_count": 12, "resolved_trade_count": 12, "resolved_trade_count_display": "12", "unscored_trade_count": 0, "unscored_trade_count_display": "0", "outcome_coverage_rate": 1.0, "outcome_coverage_rate_display": "100%", "outcome_coverage_display": "12/12 scored", "trade_rate": 0.60, "trade_rate_display": "60%", "win_rate": 0.75, "win_rate_display": "75%", "win_rate_interval_lower": 0.55, "win_rate_interval_upper": 0.88, "win_rate_interval_display": "55%-88%", "total_pnl_dollars": 8.4, "total_pnl_display": "+$8.40", "avg_edge_bps": 68.0, "avg_edge_bps_display": "68bps", "has_data": True},
                {"strategy_name": "aggressive", "selected": selected_strategy_name == "aggressive", "is_assigned": True, "is_best": False, "is_runner_up": True, "rooms_evaluated": 20, "trade_count": 10, "resolved_trade_count": 10, "resolved_trade_count_display": "10", "unscored_trade_count": 0, "unscored_trade_count_display": "0", "outcome_coverage_rate": 1.0, "outcome_coverage_rate_display": "100%", "outcome_coverage_display": "10/10 scored", "trade_rate": 0.50, "trade_rate_display": "50%", "win_rate": 0.60, "win_rate_display": "60%", "win_rate_interval_lower": 0.39, "win_rate_interval_upper": 0.78, "win_rate_interval_display": "39%-78%", "total_pnl_dollars": 5.0, "total_pnl_display": "+$5.00", "avg_edge_bps": 75.0, "avg_edge_bps_display": "75bps", "has_data": True},
            ],
        },
        {
            "series_ticker": "KXHIGHCHI",
            "city_label": "Chicago",
            "location_name": "Chicago",
            "selected": selected_series_ticker == "KXHIGHCHI",
            "assignment": {"strategy_name": "moderate", "assigned_at": "2026-04-21T18:00:00+00:00", "assigned_by": "auto_regression"},
            "assignment_context_status": "matches_recommendation",
            "best_strategy": "moderate",
            "best_strategy_win_rate": 0.58,
            "best_strategy_win_rate_display": "58%",
            "best_resolved_trade_count": 14,
            "best_resolved_trade_count_display": "14",
            "best_outcome_coverage_display": "14/14 scored",
            "runner_up_strategy": "aggressive",
            "runner_up_win_rate_display": "40%",
            "gap_to_runner_up": 0.18,
            "gap_to_runner_up_display": "18%",
            "gap_to_assignment": None,
            "gap_to_assignment_display": "—",
            "evidence_status": "strong",
            "evidence_label": "Strong recommendation",
            "trade_count_sufficient": True,
            "resolved_trade_count_sufficient": True,
            "outcome_coverage_sufficient": True,
            "gap_threshold_sufficient": True,
            "lean_gap_sufficient": True,
            "assignment_gap_sufficient": True,
            "assignment_status": "strong_recommendation",
            "assignment_status_label": "Strong recommendation",
            "recommendation": {
                "strategy_name": "moderate",
                "status": "strong_recommendation",
                "label": "Strong recommendation",
                "resolved_trade_count": 14,
                "resolved_trade_count_display": "14",
                "outcome_coverage_rate": 1.0,
                "outcome_coverage_rate_display": "100%",
                "gap_to_runner_up": 0.18,
                "gap_to_runner_up_display": "18%",
                "writes_assignment": False,
            },
            "review": chi_review,
            "approval_eligible": False,
            "approval_label": "Already assigned",
            "approval_window_days": 180,
            "approval_requires_note": True,
            "approval_reason": "Canonical assignment already matches the current recommendation.",
            "can_promote": False,
            "sort_priority": 3 if review_available else 1,
            "metrics": [
                {"strategy_name": "moderate", "selected": selected_strategy_name == "moderate", "is_assigned": True, "is_best": True, "is_runner_up": False, "rooms_evaluated": 24, "trade_count": 14, "resolved_trade_count": 14, "resolved_trade_count_display": "14", "unscored_trade_count": 0, "unscored_trade_count_display": "0", "outcome_coverage_rate": 1.0, "outcome_coverage_rate_display": "100%", "outcome_coverage_display": "14/14 scored", "trade_rate": 0.58, "trade_rate_display": "58%", "win_rate": 0.58, "win_rate_display": "58%", "win_rate_interval_lower": 0.36, "win_rate_interval_upper": 0.77, "win_rate_interval_display": "36%-77%", "total_pnl_dollars": 2.8, "total_pnl_display": "+$2.80", "avg_edge_bps": 52.0, "avg_edge_bps_display": "52bps", "has_data": True},
                {"strategy_name": "aggressive", "selected": selected_strategy_name == "aggressive", "is_assigned": False, "is_best": False, "is_runner_up": True, "rooms_evaluated": 24, "trade_count": 10, "resolved_trade_count": 10, "resolved_trade_count_display": "10", "unscored_trade_count": 0, "unscored_trade_count_display": "0", "outcome_coverage_rate": 1.0, "outcome_coverage_rate_display": "100%", "outcome_coverage_display": "10/10 scored", "trade_rate": 0.42, "trade_rate_display": "42%", "win_rate": 0.40, "win_rate_display": "40%", "win_rate_interval_lower": 0.19, "win_rate_interval_upper": 0.64, "win_rate_interval_display": "19%-64%", "total_pnl_dollars": -1.2, "total_pnl_display": "-$1.20", "avg_edge_bps": 58.0, "avg_edge_bps_display": "58bps", "has_data": True},
            ],
        },
    ]
    if selected_series_ticker == "KXHIGHNY":
        rationale = {"best_strategy": "moderate", "best_trade_count_display": "12", "best_resolved_trade_count_display": "12", "best_unscored_trade_count_display": "0", "best_outcome_coverage_display": "12/12 scored", "gap_to_runner_up_display": "15%", "gap_to_current_assignment_display": "15%", "winner_wilson_display": "55%-88%", "runner_up_wilson_display": "39%-78%", "recommendation_status": ny_recommendation_status, "recommendation_label": ny_recommendation_label, "meets_trade_threshold": True, "meets_coverage_threshold": True, "meets_gap_threshold": True, "meets_lean_gap_threshold": True, "clears_promotion_rule": False, "writes_assignment": False}
        detail_context = {
            "type": "city",
            "selected_series_ticker": "KXHIGHNY",
            "selected_strategy_name": None,
            "city": city_matrix[0],
            "ranking": city_matrix[0]["metrics"],
            "promotion_rationale": rationale,
            "recommendation_rationale": rationale,
            "review": {
                "available": review_available,
                "status": ny_review["status"],
                "label": ny_review["label"],
                "reason": ny_review["reason"],
                "needs_review": ny_review["needs_review"],
                "basis_window_days": 180,
                "current_assignment": {"strategy_name": "aggressive", "assigned_at": "2026-04-21T18:00:00+00:00", "assigned_by": "auto_regression"},
                "latest_recommendation": {
                    "strategy_name": "moderate",
                    "status": ny_recommendation_status,
                    "label": ny_recommendation_label,
                    "gap_to_runner_up_display": "15%",
                    "resolved_trade_count_display": "12",
                    "outcome_coverage_display": "12/12 scored",
                },
                "last_approval_event": {
                    "created_at": "2026-04-21T18:30:00+00:00",
                    "note": "Operator approved the current 180d winner.",
                    "previous_strategy": "aggressive",
                    "new_strategy": "moderate",
                } if review_available else None,
                "next_action_label": "Replace current assignment",
                "next_action_copy": "Approve the latest 180d winner to replace the current canonical assignment with the new recommendation.",
            },
            "approval": {
                "eligible": ny_approval_eligible,
                "label": "Ready to approve" if ny_approval_eligible else "180d only",
                "window_days": 180,
                "requires_note": True,
                "reason": "Manual approval validates against the latest stored 180d snapshot.",
                "strategy_name": "moderate",
                "recommendation_status": ny_recommendation_status,
                "recommendation_label": ny_recommendation_label,
                "assignment_context_status": "differs_from_recommendation",
            },
            "threshold_comparison": [
                {"strategy_name": "moderate", "role": "best", "threshold_groups": leaderboard[0]["threshold_groups"]},
                {"strategy_name": "aggressive", "role": "runner_up", "threshold_groups": leaderboard[1]["threshold_groups"]},
            ],
            "trend": {
                "title": "Stored regression history",
                "window_days": 180,
                "note": "Trend history uses stored 180d regression snapshots.",
                "series": [
                    {"strategy_name": "moderate", "points": [{"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.67}, {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.75}]},
                    {"strategy_name": "aggressive", "points": [{"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.52}, {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.60}]},
                ],
            },
            "recent_events": [
                {"kind": "promotion", "summary": "Strategy auto-promoted for KXHIGHNY: aggressive -> moderate", "source": "strategy_regression", "created_at": "2026-04-21T18:00:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12},
                {"kind": "assignment_approval", "summary": "Approved strategy assignment for KXHIGHNY: aggressive -> moderate", "source": "strategy_review", "created_at": "2026-04-21T18:30:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12, "note": "Operator approved the current 180d winner."},
            ],
        }
    else:
        selected = leaderboard[0] if selected_strategy_name != "aggressive" else leaderboard[1]
        detail_context = {
            "type": "strategy",
            "selected_series_ticker": None,
            "selected_strategy_name": selected["name"],
            "strategy": {**selected, "threshold_groups": selected["threshold_groups"]},
            "strongest_cities": [{"series_ticker": "KXHIGHNY", "city_label": "New York City", "win_rate_display": "75%", "trade_count_display": "12", "total_pnl_display": "+$8.40"}],
            "weakest_cities": [{"series_ticker": "KXHIGHCHI", "city_label": "Chicago", "win_rate_display": "58%", "trade_count_display": "14", "total_pnl_display": "+$2.80"}],
            "city_distribution": [
                {"series_ticker": "KXHIGHNY", "city_label": "New York City", "win_rate_display": "75%", "trade_rate_display": "60%", "trade_count_display": "12", "trade_count": 12, "total_pnl_display": "+$8.40", "total_pnl_dollars": 8.4, "is_best": True, "is_assigned": False},
                {"series_ticker": "KXHIGHCHI", "city_label": "Chicago", "win_rate_display": "58%", "trade_rate_display": "58%", "trade_count_display": "14", "trade_count": 14, "total_pnl_display": "+$2.80", "total_pnl_dollars": 2.8, "is_best": True, "is_assigned": True},
            ],
            "trend": {
                "title": "Stored regression history",
                "note": "Current leaderboard metrics reflect the selected window.",
                "points": [
                    {"run_at": "2026-04-20T18:00:00+00:00", "win_rate": 0.66, "trade_rate": 0.52, "total_pnl_dollars": 12.4},
                    {"run_at": "2026-04-21T18:00:00+00:00", "win_rate": 0.73, "trade_rate": 0.58, "total_pnl_dollars": 18.4},
                ],
            },
            "recent_events": [{"kind": "threshold_adjustment", "summary": "Auto-adjusted risk_min_edge_bps 50->40", "source": "strategy_eval", "created_at": "2026-04-21T18:00:00+00:00", "change_display": "50bps -> 40bps", "win_rate_display": "63%", "trade_count": 82}],
        }

    return {
        "summary": {
            "window_days": window_days,
            "window_display": f"{window_days}d",
            "window_options": [30, 90, 180],
            "source_mode": "stored_snapshot" if window_days == 180 else "live_eval",
            "recommendation_mode": "recommendation_only",
            "manual_approval_enabled": True,
            "approval_window_days": 180,
            "review_available": review_available,
            "review_window_days": 180 if review_available else None,
            "last_regression_run": "2026-04-21T18:00:00+00:00",
            "rooms_scanned": 84,
            "rooms_scanned_display": "84",
            "cities_evaluated": 2,
            "cities_evaluated_display": "2",
            "best_strategy_name": "moderate",
            "best_strategy_win_rate": 0.73,
            "best_strategy_win_rate_display": "73%",
            "strong_recommendations_count": 2 if window_days == 180 else 0,
            "lean_recommendations_count": 0,
            "ready_for_approval_count": 0 if review_available else None,
            "needs_review_count": 1 if review_available else None,
            "drifted_assignments_count": 1 if review_available else None,
            "evidence_weakened_count": 0 if review_available else None,
            "aligned_assignments_count": 1 if review_available else None,
            "recent_promotions_count": 1,
            "recent_approvals_count": 1,
            "assignments_covered": 2,
            "assignments_total": 2,
            "assignments_covered_display": "2 / 2",
            "methodology_note": "Canonical outcomes, manual approval",
        },
        "leaderboard": leaderboard,
        "city_matrix": city_matrix,
        "detail_context": detail_context,
        "codex_lab": {
            "available": True,
            "provider": "gemini",
            "provider_label": "Gemini",
            "model": "gemini-2.5-pro",
            "provider_options": [
                {
                    "id": "gemini",
                    "label": "Gemini",
                    "default_model": "gemini-2.5-pro",
                    "suggested_models": ["gemini-2.5-pro", "gemini-2.5-flash"],
                },
                {
                    "id": "codex",
                    "label": "Codex",
                    "default_model": "gpt-4o",
                    "suggested_models": ["gpt-4o"],
                },
            ],
            "creation_window_days": 180,
            "recent_runs": [
                {
                    "id": "run-suggest-1",
                    "mode": "suggest",
                    "status": "completed",
                    "trigger_source": "nightly",
                    "provider": "gemini",
                    "model": "gemini-2.5-pro",
                    "window_days": window_days,
                    "series_ticker": "KXHIGHNY",
                    "strategy_name": None,
                    "created_at": "2026-04-21T18:40:00+00:00",
                    "updated_at": "2026-04-21T18:42:00+00:00",
                    "summary": "Saved as inactive preset balanced-plus.",
                    "saved_strategy_name": "balanced-plus",
                },
                {
                    "id": "run-evaluate-1",
                    "mode": "evaluate",
                    "status": "completed",
                    "trigger_source": "manual",
                    "provider": "codex",
                    "model": "gpt-4o",
                    "window_days": window_days,
                    "series_ticker": None,
                    "strategy_name": "moderate",
                    "created_at": "2026-04-21T18:35:00+00:00",
                    "updated_at": "2026-04-21T18:36:00+00:00",
                    "summary": "Moderate still leads the active presets, but New York remains the biggest assignment mismatch.",
                    "saved_strategy_name": None,
                },
            ],
            "inactive_codex_strategies": [
                {
                    "name": "balanced-plus",
                    "description": "A Codex-suggested preset that splits the difference between moderate and aggressive.",
                    "created_at": "2026-04-21T18:42:00+00:00",
                    "labels": ["city-specific", "gap-sensitive"],
                    "rationale": "Designed to preserve moderate's coverage while adding slightly looser spread tolerance.",
                    "source_run_id": "run-suggest-1",
                }
            ],
        },
        "recent_promotions": [
            {"kind": "promotion", "summary": "Strategy auto-promoted for KXHIGHNY: aggressive -> moderate", "source": "strategy_regression", "created_at": "2026-04-21T18:00:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12},
            {"kind": "threshold_adjustment", "summary": "Auto-adjusted risk_min_edge_bps 50->40", "source": "strategy_eval", "created_at": "2026-04-21T18:00:00+00:00", "change_display": "50bps -> 40bps", "win_rate_display": "63%", "trade_count": 82},
            {"kind": "assignment_approval", "summary": "Approved strategy assignment for KXHIGHNY: aggressive -> moderate", "source": "strategy_review", "created_at": "2026-04-21T18:30:00+00:00", "series_ticker": "KXHIGHNY", "win_rate_display": "75%", "trade_count": 12, "note": "Operator approved the current 180d winner."},
        ],
        "methodology": {
            "title": "How to read this tab",
            "points": [
                "Data comes from historical replay rooms, not live forward testing.",
                "Default view uses a rolling 180d regression snapshot.",
            ],
            "recommendation_trade_threshold": 20,
            "recommendation_outcome_coverage_threshold": 0.95,
            "recommendation_lean_gap_threshold": 0.01,
            "recommendation_strong_gap_threshold": 0.02,
            "promotion_trade_threshold": 20,
            "promotion_gap_threshold": 0.05,
        },
    }


def _artifact_root() -> Path:
    root = os.getenv("PLAYWRIGHT_ARTIFACTS_DIR")
    if root:
        return Path(root)
    return Path(tempfile.gettempdir()) / "kalshi_bot_playwright"


@contextmanager
def _serve_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payloads: dict[str, object] | None = None,
    strategies_payload_factory=None,
    *,
    site_kind: str = "combined",
):
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "browser.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    monkeypatch.setenv("WEB_SITE_KIND", site_kind)
    get_settings.cache_clear()

    if payloads is None:
        payloads = {
            "demo": _build_env_payload("$1,250.00", positions_count=24),
            "production": _build_env_payload("$2,500.00", positions_count=18),
        }

    async def fake_build_env_dashboard(_container, kalshi_env: str) -> dict[str, object]:
        return payloads[kalshi_env]

    async def fake_build_strategies_dashboard(
        _container,
        *,
        window_days: int = 180,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
    ) -> dict[str, object]:
        factory = strategies_payload_factory or _build_strategies_payload
        return factory(window_days, selected_series_ticker=series_ticker, selected_strategy_name=strategy_name)

    monkeypatch.setattr(web_app_module, "build_env_dashboard", fake_build_env_dashboard)
    monkeypatch.setattr(web_app_module, "build_strategies_dashboard", fake_build_strategies_dashboard)
    app = create_app()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}/"
    deadline = time.time() + 10
    try:
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(base_url, timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.HTTPError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            raise RuntimeError("Timed out waiting for the dashboard test server to start")
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        get_settings.cache_clear()


def _collect_layout_metrics(page: Page, env_key: str) -> dict[str, object]:
    if env_key == "production":
        page.locator('.dash-tab[data-env="production"]').click(timeout=15_000)
        page.wait_for_function("() => !document.querySelector('#panel-production').hidden", timeout=15_000)

    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const grid = panel?.querySelector('.dash-grid');
          const card = panel?.querySelector('.dash-card-alerts');
          const header = card?.querySelector('.dash-card-header');
          const rooms = card?.querySelector('.active-rooms-section');
          const positions = panel?.querySelector('.dash-card-positions');
          if (!panel || !grid || !card || !header || !rooms || !positions) {
            return null;
          }
          const rect = (node) => {
            const box = node.getBoundingClientRect();
            return { top: box.top, bottom: box.bottom, height: box.height };
          };
          const cardRect = rect(card);
          const headerRect = rect(header);
          const roomsRect = rect(rooms);
          const positionsRect = rect(positions);
          return {
            activeTabEnv: document.querySelector('.dash-tab.is-active')?.dataset.env ?? null,
            panelHidden: panel.hidden,
            gridAlignItems: getComputedStyle(grid).alignItems,
            cardAlignContent: getComputedStyle(card).alignContent,
            cardHeight: Math.round(cardRect.height),
            positionsHeight: Math.round(positionsRect.height),
            cardToHeaderGap: Math.round(headerRect.top - cardRect.top),
            headerToRoomsGap: Math.round(roomsRect.top - headerRect.bottom),
          };
        }
        """,
        env_key,
    )


def _assert_layout(page: Page, env_key: str, viewport_name: str) -> None:
    artifact_path = _artifact_root() / f"dashboard-layout-{viewport_name}-{env_key}.png"
    metrics = _collect_layout_metrics(page, env_key)
    try:
        assert metrics is not None, f"Missing layout metrics for {env_key}"
        assert metrics["activeTabEnv"] == env_key, metrics
        assert metrics["panelHidden"] is False, metrics
        assert metrics["gridAlignItems"] == "start", metrics
        assert metrics["cardAlignContent"] == "start", metrics
        assert metrics["cardHeight"] < metrics["positionsHeight"], metrics
        assert metrics["cardToHeaderGap"] <= 24, metrics
        assert metrics["headerToRoomsGap"] <= 24, metrics
    except AssertionError as exc:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(artifact_path), full_page=True)
        raise AssertionError(f"{exc}\nScreenshot: {artifact_path}") from exc


@pytest.mark.parametrize(("viewport_name", "viewport"), VIEWPORTS)
def test_dashboard_alerts_card_stays_top_aligned(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, viewport_name: str, viewport: dict[str, int]
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as playwright_context:
            browser = playwright_context.chromium.launch(headless=True)
            page = browser.new_page(viewport=viewport, device_scale_factor=1)
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .dash-card-alerts", timeout=15_000)
                _assert_layout(page, "demo", viewport_name)
                _assert_layout(page, "production", viewport_name)
            finally:
                browser.close()


def _positions_total_row(page: Page, env_key: str = "demo") -> dict[str, object] | None:
    """Return label and value text from the positions tfoot row, or None if absent."""
    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const tfoot = panel?.querySelector('.positions-totals');
          if (!tfoot) return null;
          const cells = tfoot.querySelectorAll('td');
          return {
            label: cells[0]?.textContent?.trim() ?? null,
            value: cells[1]?.textContent?.trim() ?? null,
            pnl:   cells[2]?.textContent?.trim() ?? null,
          };
        }
        """,
        env_key,
    )


def _portfolio_recent_line(page: Page, env_key: str = "demo") -> str | None:
    return page.evaluate(
        """
        (envKey) => {
          const panel = document.querySelector(`#panel-${envKey}`);
          const line = panel?.querySelector('.dash-stat-portfolio .dash-stat-detail');
          return line?.textContent?.trim() ?? null;
        }
        """,
        env_key,
    )


def test_positions_total_row_shows_notional_and_pnl_when_marked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            total_notional_display="$108.00",
            total_unrealized_pnl_display="+$7.20",
            total_unrealized_pnl_tone="good",
            has_pnl_summary=True,
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .positions-table", timeout=15_000)
                row = _positions_total_row(page)
                assert row is not None, "positions tfoot not rendered"
                assert row["label"] == "Totals", row
                assert row["value"] == "$108.00", row
                assert row["pnl"] == "+$7.20", row
            finally:
                browser.close()


def test_portfolio_card_shows_recent_pnl_line(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            daily_pnl_display="+$65.30",
            daily_pnl_tone="good",
            daily_pnl_line_display="+$65.30 (9.96%) today (PT)",
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .dash-stat-portfolio .dash-stat-detail", timeout=15_000)
                assert _portfolio_recent_line(page) == "+$65.30 (9.96%) today (PT)"
            finally:
                browser.close()


def test_recent_trade_proposals_render_at_bottom_of_demo_and_production(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            recent_trade_proposals=_build_recent_trade_proposals(),
        ),
        "production": _build_env_payload(
            "$2,500.00",
            positions_count=1,
            recent_trade_proposals=_build_recent_trade_proposals(),
        ),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector('#panel-demo [data-testid="recent-trade-proposals"]', timeout=15_000)
                demo_text = page.locator('#panel-demo [data-testid="recent-trade-proposals"]').text_content(timeout=15_000) or ""
                assert "Recent Trade Proposals" in demo_text
                assert "KXHIGHTPHX-26APR23-T92" in demo_text
                assert "0.0400" in demo_text
                assert "blocked" in demo_text

                page.locator('.dash-tab[data-env="production"]').click(timeout=15_000)
                page.wait_for_selector('#panel-production [data-testid="recent-trade-proposals"]', timeout=15_000)
                production_text = page.locator('#panel-production [data-testid="recent-trade-proposals"]').text_content(timeout=15_000) or ""
                assert "KXHIGHNY-26APR23-T75" in production_text
                assert "5.0000" in production_text
            finally:
                browser.close()


def test_positions_total_row_shows_dash_pnl_when_unmarked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "demo": _build_env_payload(
            "$1,250.00",
            positions_count=3,
            total_notional_display="$108.00",
            total_unrealized_pnl_display="—",
            total_unrealized_pnl_tone="neutral",
            has_pnl_summary=False,
        ),
        "production": _build_env_payload("$2,500.00", positions_count=1),
    }
    with _serve_dashboard(monkeypatch, tmp_path, payloads=payloads) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector("#panel-demo .positions-table", timeout=15_000)
                row = _positions_total_row(page)
                assert row is not None, "positions tfoot not rendered"
                assert row["label"] == "Totals", row
                assert row["value"] == "$108.00", row
                assert row["pnl"] == "—", row
            finally:
                browser.close()


@pytest.mark.parametrize(("viewport_name", "viewport"), VIEWPORTS)
def test_strategies_tab_renders_filters_and_drilldowns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, viewport_name: str, viewport: dict[str, int]
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport=viewport, device_scale_factor=1)
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.locator('.dash-tab[data-env="strategies"]').click(timeout=15_000)
                page.wait_for_selector("#strategies-summary", timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-cities')?.hidden === false",
                    timeout=15_000,
                )
                assert "180d" in (page.locator("#strategies-summary").text_content(timeout=15_000) or "")
                focus_modes = page.locator("#strategies-focus-switch button").evaluate_all(
                    "(nodes) => nodes.map((node) => node.dataset.focusMode)"
                )
                assert focus_modes == ["cities", "strategies", "review"]
                page.wait_for_function(
                    "() => document.querySelector('#strategies-cities-detail h3')?.textContent?.includes('KXHIGHNY')",
                    timeout=15_000,
                )
                summary_text = page.locator("#strategies-summary").text_content(timeout=15_000) or ""
                assert "Cities Evaluated" in summary_text
                assert "Actionable Cities" in summary_text
                assert "Assignment Mismatches" in summary_text
                assert "Low-Confidence Cities" in summary_text
                detail_text = page.locator("#strategies-cities-detail").text_content(timeout=15_000) or ""
                assert "City Research Brief" in detail_text
                assert "Evidence interpretation" in detail_text
                assert "Approval" in detail_text
                assert page.locator("#strategies-cities-detail textarea").is_visible()
                comparison_card_positions = page.locator("#strategies-cities-detail .strategy-comparison-card").evaluate_all(
                    "(nodes) => nodes.slice(0, 2).map((node) => ({ left: Math.round(node.getBoundingClientRect().left), clientWidth: node.clientWidth, scrollWidth: node.scrollWidth }))"
                )
                assert len(comparison_card_positions) == 2
                if viewport_name == "narrow":
                    assert comparison_card_positions[0]["left"] == comparison_card_positions[1]["left"]
                assert all(item["scrollWidth"] <= item["clientWidth"] + 2 for item in comparison_card_positions)
                page.locator('#strategies-cities-detail [data-testid="strategy-open-evaluation-lab"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )
                codex_text = page.locator("#strategies-codex-lab").text_content(timeout=15_000) or ""
                assert "Gemini ready" in codex_text
                assert "gemini-2.5-pro" in codex_text
                assert "Nightly" in codex_text
                assert "Saved Inactive Presets" in codex_text
                assert "balanced-plus" in codex_text
                assert page.locator("#strategies-codex-provider").input_value(timeout=15_000) == "gemini"
                assert page.locator("#strategies-codex-model").input_value(timeout=15_000) == "gemini-2.5-pro"
                assert page.locator("#strategies-codex-model").get_attribute("list", timeout=15_000) == "strategies-codex-model-options"
                model_options = page.locator("#strategies-codex-model-options option").evaluate_all(
                    "(nodes) => nodes.map((node) => node.value)"
                )
                assert model_options == ["gemini-2.5-pro", "gemini-2.5-flash"]
                provider_row_box = page.locator("#strategies-codex-lab .strategy-codex-provider-row").bounding_box()
                provider_box = page.locator("#strategies-codex-provider").bounding_box()
                model_box = page.locator("#strategies-codex-model").bounding_box()
                assert provider_row_box is not None
                assert provider_box is not None
                assert model_box is not None
                provider_row_right = provider_row_box["x"] + provider_row_box["width"] + 2
                assert provider_box["x"] + provider_box["width"] <= provider_row_right
                assert model_box["x"] + model_box["width"] <= provider_row_right
                page.locator('#strategies-focus-switch button[data-focus-mode="cities"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-cities')?.hidden === false",
                    timeout=15_000,
                )

                page.wait_for_selector("#strategies-city-matrix table", timeout=15_000)
                header_text = page.locator("#strategies-city-matrix thead").text_content(timeout=15_000) or ""
                assert "Best Strategy" in header_text
                assert "Resolved" in header_text
                assert "moderate" not in header_text
                assert "aggressive" not in header_text

                assert "Actionable" in (page.locator('#strategies-city-filters button[data-city-filter="actionable"]').text_content(timeout=15_000) or "")
                assert "2" in (page.locator('#strategies-city-filters button[data-city-filter="actionable"]').text_content(timeout=15_000) or "")
                assert "1" in (page.locator('#strategies-city-filters button[data-city-filter="needs_review"]').text_content(timeout=15_000) or "")
                assert "1" in (page.locator('#strategies-city-filters button[data-city-filter="mismatch"]').text_content(timeout=15_000) or "")

                page.locator('#strategies-city-filters button[data-city-filter="no_outcomes"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => (document.querySelector('#strategies-city-matrix')?.textContent || '').includes('No cities match the current search or filter.')",
                    timeout=15_000,
                )
                assert "No city research brief is available" in (page.locator("#strategies-cities-detail").text_content(timeout=15_000) or "")
                page.locator('#strategies-city-filters button[data-city-filter="all"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-cities-detail h3')?.textContent?.includes('KXHIGHNY')",
                    timeout=15_000,
                )

                page.locator("#strategies-city-search").fill("aggressive", timeout=15_000)
                page.wait_for_function(
                    "() => (document.querySelector('#strategies-city-matrix tbody')?.textContent || '').includes('New York City')",
                    timeout=15_000,
                )
                matrix_text = page.locator("#strategies-city-matrix tbody").text_content(timeout=15_000) or ""
                assert "New York City" in matrix_text
                assert "Chicago" not in matrix_text
                page.locator("#strategies-city-search").fill("", timeout=15_000)

                page.locator('#strategies-focus-switch button[data-focus-mode="strategies"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )
                assert page.locator("#strategies-codex-lab").is_visible()
                page.wait_for_selector("#strategies-leaderboard .strategy-card", timeout=15_000)
                page.locator('#strategies-leaderboard button[data-strategy-name="aggressive"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-detail h3')?.textContent?.includes('aggressive')",
                    timeout=15_000,
                )
                assert "Stored regression history" in (page.locator("#strategies-detail").text_content(timeout=15_000) or "")
                assert page.locator("#strategies-recent").is_visible()
                assert page.locator("#strategies-methodology").is_visible()

                page.locator('#strategies-focus-switch button[data-focus-mode="cities"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-cities')?.hidden === false",
                    timeout=15_000,
                )
                page.wait_for_function(
                    "() => document.querySelector('#strategies-cities-detail h3')?.textContent?.includes('KXHIGHNY')",
                    timeout=15_000,
                )

                page.locator('#strategies-window-filter button[data-window-days="90"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-summary')?.textContent?.includes('90d')",
                    timeout=15_000,
                )
                page.wait_for_function(
                    "() => document.querySelector('#strategies-cities-detail h3')?.textContent?.includes('KXHIGHNY')",
                    timeout=15_000,
                )
                assert "Review Queue" not in (page.locator("#strategies-focus-switch").text_content(timeout=15_000) or "")
                assert page.locator('#strategies-city-filters button[data-city-filter="needs_review"]').count() == 0
                detail_text_90d = page.locator("#strategies-cities-detail").text_content(timeout=15_000) or ""
                assert "City Research Brief" in detail_text_90d
                assert "Evidence interpretation" in detail_text_90d
                assert "Latest approval note" not in detail_text_90d
                assert page.locator("#strategies-cities-detail textarea").count() == 0
                assert "Trend History" in detail_text_90d

                page.locator('#strategies-focus-switch button[data-focus-mode="strategies"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )
                page.wait_for_selector("#strategies-leaderboard .strategy-card", timeout=15_000)
                assert "moderate" in (page.locator("#strategies-leaderboard").text_content(timeout=15_000) or "")
                assert "Stored regression history" in (page.locator("#strategies-detail").text_content(timeout=15_000) or "")
                assert page.locator("#strategies-recent").is_visible()
                assert page.locator("#strategies-methodology").is_visible()
            finally:
                browser.close()


def test_strategy_lab_redirects_to_login_when_session_expires(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.locator('.dash-tab[data-env="strategies"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-cities')?.hidden === false",
                    timeout=15_000,
                )
                page.locator('#strategies-cities-detail [data-testid="strategy-open-evaluation-lab"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )
                page.evaluate(
                    """
                    (loginUrl) => {
                      const originalFetch = window.fetch.bind(window);
                      window.fetch = async (input, init) => {
                        const url = typeof input === "string" ? input : input.url;
                        const method = String((init && init.method) || "GET").toUpperCase();
                        if (method === "POST" && url.includes("/api/strategies/codex/runs")) {
                          return new Response(
                            JSON.stringify({ error: "auth_required", login_url: loginUrl }),
                            {
                              status: 401,
                              headers: { "Content-Type": "application/json" },
                            },
                          );
                        }
                        return originalFetch(input, init);
                      };
                    }
                    """,
                    "/login?next=/api/strategies/codex/runs",
                )

                page.locator('#strategies-codex-lab [data-testid="strategy-codex-run"]').click(timeout=15_000)
                page.wait_for_url("**/login?next=%2F", timeout=15_000)
            finally:
                browser.close()


def test_strategy_lab_redirects_to_login_on_generic_401(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _serve_dashboard(monkeypatch, tmp_path) as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.locator('.dash-tab[data-env="strategies"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-cities')?.hidden === false",
                    timeout=15_000,
                )
                page.locator('#strategies-cities-detail [data-testid="strategy-open-evaluation-lab"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )
                page.evaluate(
                    """
                    () => {
                      const originalFetch = window.fetch.bind(window);
                      window.fetch = async (input, init) => {
                        const url = typeof input === "string" ? input : input.url;
                        const method = String((init && init.method) || "GET").toUpperCase();
                        if (method === "POST" && url.includes("/api/strategies/codex/runs")) {
                          return new Response("expired session", { status: 401 });
                        }
                        return originalFetch(input, init);
                      };
                    }
                    """,
                )

                page.locator('#strategies-codex-lab [data-testid="strategy-codex-run"]').click(timeout=15_000)
                page.wait_for_url("**/login?next=%2F", timeout=15_000)
            finally:
                browser.close()


def test_single_site_shell_renders_static_site_label(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with _serve_dashboard(monkeypatch, tmp_path, site_kind="demo") as base_url:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            try:
                page.goto(base_url, wait_until="load", timeout=15_000)
                page.wait_for_selector(".dash-shell-site-label", timeout=15_000)
                assert page.locator(".dash-shell-site-label").text_content(timeout=15_000) == "Demo"
                assert page.locator(".dash-tab").count() == 0
                assert page.title() == "Kalshi Bot Demo"
                assert page.locator("#panel-demo").is_visible()
            finally:
                browser.close()
