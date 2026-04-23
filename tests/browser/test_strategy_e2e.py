from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import pytest
import uvicorn

import kalshi_bot.services.container as container_module
import kalshi_bot.web.app as web_app_module
from kalshi_bot.config import get_settings
from kalshi_bot.core.enums import ContractSide, RoomOrigin, RoomStage, TradeAction
from kalshi_bot.core.schemas import RoomCreate, StrategyCodexEvaluationPayload, StrategyCodexSuggestionPayload, StrategyThresholdPreset, TradeTicket
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.strategy_codex import STRATEGY_LAB_SOURCE, StrategyCodexService
from kalshi_bot.services.strategy_regression import (
    STRATEGY_PRESETS,
    RegressionStrategySpec,
    StrategyRegressionService,
    _recommendation_decision,
)
from kalshi_bot.web.app import create_app
from kalshi_bot.web.control_room import STRATEGY_APPROVAL_ASSIGNED_BY, STRATEGY_APPROVAL_EVENT_KIND, STRATEGY_APPROVAL_SOURCE

playwright_sync_api = pytest.importorskip("playwright.sync_api")
Page = playwright_sync_api.Page
sync_playwright = playwright_sync_api.sync_playwright

FIXTURE_NOW = datetime(2026, 4, 22, 15, 0, tzinfo=UTC)
DESKTOP_VIEWPORT = {"width": 1440, "height": 1100}
APPROVAL_NOTE = "Approving moderate after the latest 180d snapshot held the lead across scored replay rooms."
SUGGESTED_PRESET_NAME = "balanced-plus"


@dataclass(frozen=True)
class ReplaySeedRow:
    series_ticker: str
    market_ticker: str
    local_market_day: str
    checkpoint_ts: datetime
    edge_bps: int
    settlement_value_dollars: Decimal
    ticket_yes_price_dollars: Decimal = Decimal("0.4500")
    fair_yes_dollars: Decimal = Decimal("0.6200")
    crosscheck_high_f: Decimal = Decimal("78.00")


@dataclass(frozen=True)
class StrategyFixtureState:
    approval_note: str = APPROVAL_NOTE
    approved_series_ticker: str = "KXHIGHNY"
    approved_strategy_name: str = "moderate"
    previous_strategy_name: str = "aggressive"
    aligned_series_ticker: str = "KXHIGHCHI"
    accepted_strategy_name: str = SUGGESTED_PRESET_NAME


@dataclass
class StrategyE2EServer:
    base_url: str
    container: container_module.AppContainer
    fixture: StrategyFixtureState


class FakeStrategyProvider:
    def __init__(self, provider_id: str) -> None:
        self.provider_id = provider_id

    async def close(self) -> None:
        return None

    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        schema_model=None,
    ) -> dict[str, Any]:
        del system_prompt, user_prompt, model, temperature
        if schema_model is StrategyCodexEvaluationPayload:
            payload = {
                "summary": "Gemini reviewed the latest 180d snapshot and found the strongest edge concentrated in the moderate preset.",
                "strengths": [
                    "Moderate leads the stored 180d snapshot in both configured cities.",
                    "The recommendation queue has clear scored evidence behind the leading preset.",
                ],
                "risks": [
                    "New York is still assigned to an older preset until the operator approves the drifted recommendation.",
                ],
                "opportunities": [
                    "Use the evaluation lab to validate whether a narrower threshold profile can complement moderate.",
                ],
                "recommended_actions": [
                    "Approve the drifted New York assignment on the 180d view.",
                    "Run a deterministic suggestion backtest before saving a new preset.",
                ],
            }
        else:
            payload = {
                "name": SUGGESTED_PRESET_NAME,
                "description": "A tighter balanced preset that still catches the high-confidence city setups.",
                "labels": ["strategy-lab", "deterministic", "180d"],
                "rationale": "This candidate trims the low-edge tail while preserving enough volume to backtest cleanly on the 180d replay window.",
                "thresholds": StrategyThresholdPreset(
                    risk_min_edge_bps=45,
                    risk_max_order_notional_dollars=12.0,
                    risk_max_position_notional_dollars=30.0,
                    trigger_max_spread_bps=450,
                    trigger_cooldown_seconds=240,
                    strategy_quality_edge_buffer_bps=10,
                    strategy_min_remaining_payout_bps=400,
                    risk_safe_capital_reserve_ratio=0.65,
                    risk_risky_capital_max_ratio=0.35,
                ).model_dump(mode="json"),
            }
        return schema_model.model_validate(payload).model_dump(mode="json") if schema_model is not None else payload


class FakeProviderRouter:
    def __init__(self) -> None:
        self.gemini = FakeStrategyProvider("gemini")
        self.hosted = FakeStrategyProvider("openai")

    async def close(self) -> None:
        await self.gemini.close()
        await self.hosted.close()


def _artifact_root() -> Path:
    root = os.getenv("PLAYWRIGHT_ARTIFACTS_DIR")
    if root:
        return Path(root)
    return Path(tempfile.gettempdir()) / "kalshi_bot_playwright"


def _fixture_market_map() -> str:
    return (
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
  - series_ticker: KXHIGHCHI
    display_name: Chicago Daily High Temperature
    station_id: KORD
    daily_summary_station_id: USW00094846
    location_name: Chicago
    timezone_name: America/Chicago
    latitude: 41.9786
    longitude: -87.9048
""".strip()
        + "\n"
    )


def _active_presets() -> list[dict[str, Any]]:
    active_names = {"aggressive", "moderate"}
    return [preset for preset in STRATEGY_PRESETS if preset["name"] in active_names]


def _strategy_specs(strategies: dict[str, Any]) -> list[RegressionStrategySpec]:
    return [
        RegressionStrategySpec(
            id=strategy.id,
            name=strategy.name,
            description=strategy.description,
            thresholds=strategy.thresholds,
        )
        for strategy in strategies.values()
    ]


def _replay_seed_rows() -> list[ReplaySeedRow]:
    rows: list[ReplaySeedRow] = []
    base_day = date(2026, 2, 1)

    def add_rows(
        *,
        series_ticker: str,
        market_prefix: str,
        high_edge_wins: int,
        high_edge_losses: int,
        candidate_only_wins: int,
        candidate_only_losses: int,
        aggressive_only_wins: int,
        aggressive_only_losses: int,
    ) -> None:
        total_high = high_edge_wins + high_edge_losses
        total_candidate_only = candidate_only_wins + candidate_only_losses
        total_aggressive_only = aggressive_only_wins + aggressive_only_losses
        outcomes = (
            [(60, True)] * high_edge_wins
            + [(60, False)] * high_edge_losses
            + [(47, True)] * candidate_only_wins
            + [(47, False)] * candidate_only_losses
            + [(35, True)] * aggressive_only_wins
            + [(35, False)] * aggressive_only_losses
        )
        assert total_high == 20
        assert total_candidate_only == 2
        assert total_aggressive_only == 2
        assert len(outcomes) == 24
        for index, (edge_bps, win) in enumerate(outcomes, start=1):
            local_day = base_day + timedelta(days=len(rows))
            rows.append(
                ReplaySeedRow(
                    series_ticker=series_ticker,
                    market_ticker=f"{market_prefix}-26APR{index:02d}-T70",
                    local_market_day=local_day.isoformat(),
                    checkpoint_ts=datetime.combine(local_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=18),
                    edge_bps=edge_bps,
                    settlement_value_dollars=Decimal("1.0000") if win else Decimal("0.0000"),
                    crosscheck_high_f=Decimal("81.00") if series_ticker == "KXHIGHNY" else Decimal("76.00"),
                )
            )

    add_rows(
        series_ticker="KXHIGHNY",
        market_prefix="KXHIGHNY",
        high_edge_wins=15,
        high_edge_losses=5,
        candidate_only_wins=1,
        candidate_only_losses=1,
        aggressive_only_wins=0,
        aggressive_only_losses=2,
    )
    add_rows(
        series_ticker="KXHIGHCHI",
        market_prefix="KXHIGHCHI",
        high_edge_wins=13,
        high_edge_losses=7,
        candidate_only_wins=0,
        candidate_only_losses=2,
        aggressive_only_wins=1,
        aggressive_only_losses=1,
    )
    return rows


def _older_history_rows(latest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    older_run_at = FIXTURE_NOW - timedelta(days=35)
    older_date_from = (older_run_at - timedelta(days=180)).date().isoformat()
    older_date_to = older_run_at.date().isoformat()
    history_rows: list[dict[str, Any]] = []
    for row in latest_rows:
        trade_count = max(int(row["trade_count"]) - 2, 12)
        resolved_trade_count = max(int(row["resolved_trade_count"]) - 2, 12)
        win_count = max(min(int(row["win_count"]) - 1, resolved_trade_count), 0)
        total_pnl = row["total_pnl_dollars"]
        scaled_pnl = (Decimal(str(total_pnl)) * Decimal("0.82")).quantize(Decimal("0.0001")) if total_pnl is not None else None
        history_rows.append(
            {
                "strategy_id": row["strategy_id"],
                "run_at": older_run_at,
                "date_from": older_date_from,
                "date_to": older_date_to,
                "series_ticker": row["series_ticker"],
                "rooms_evaluated": int(row["rooms_evaluated"]),
                "trade_count": trade_count,
                "resolved_trade_count": resolved_trade_count,
                "unscored_trade_count": max(trade_count - resolved_trade_count, 0),
                "win_count": win_count,
                "total_pnl_dollars": scaled_pnl,
                "trade_rate": round(trade_count / int(row["rooms_evaluated"]), 4),
                "win_rate": round(win_count / resolved_trade_count, 4) if resolved_trade_count > 0 else None,
                "avg_edge_bps": row["avg_edge_bps"],
            }
        )
    return history_rows


async def _seed_strategy_fixture(container: container_module.AppContainer) -> StrategyFixtureState:
    fixture = StrategyFixtureState()
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control(
            container.settings.app_color,
            kalshi_env=container.settings.kalshi_env,
            initial_active_color=container.settings.app_color,
            initial_kill_switch_enabled=container.settings.app_enable_kill_switch,
        )
        await container.agent_pack_service.ensure_initialized(repo)
        await container.historical_heuristic_service.ensure_initialized(repo)
        strategies: dict[str, Any] = {}
        for preset in _active_presets():
            strategies[preset["name"]] = await repo.create_strategy(
                name=preset["name"],
                description=preset["description"],
                thresholds=preset["thresholds"],
                is_active=True,
                source="builtin",
            )

        for index, row in enumerate(_replay_seed_rows(), start=1):
            room = await repo.create_room(
                RoomCreate(
                    name=f"Historical replay {row.market_ticker}",
                    market_ticker=row.market_ticker,
                ),
                active_color=control.active_color,
                shadow_mode=False,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=container.settings.kalshi_env,
                room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                agent_pack_version="builtin-gemini-v1",
            )
            await repo.update_room_stage(room.id, RoomStage.COMPLETE)
            await repo.save_signal(
                room_id=room.id,
                market_ticker=row.market_ticker,
                fair_yes_dollars=row.fair_yes_dollars,
                edge_bps=row.edge_bps,
                confidence=0.88,
                summary=f"Deterministic replay signal for {row.market_ticker}",
                payload={
                    "series_ticker": row.series_ticker,
                    "eligibility": {
                        "market_spread_bps": 10,
                        "remaining_payout_dollars": "0.90",
                    },
                },
            )
            await repo.save_trade_ticket(
                room.id,
                TradeTicket(
                    market_ticker=row.market_ticker,
                    action=TradeAction.BUY,
                    side=ContractSide.YES,
                    yes_price_dollars=row.ticket_yes_price_dollars,
                    count_fp=Decimal("1.00"),
                    note=f"Replay ticket for {row.market_ticker}",
                ),
                client_order_id=f"strategy-e2e-{index}",
            )
            await repo.upsert_historical_settlement_label(
                market_ticker=row.market_ticker,
                series_ticker=row.series_ticker,
                local_market_day=row.local_market_day,
                source_kind="kalshi_primary",
                kalshi_result="yes" if row.settlement_value_dollars == Decimal("1.0000") else "no",
                settlement_value_dollars=row.settlement_value_dollars,
                settlement_ts=row.checkpoint_ts + timedelta(hours=10),
                crosscheck_status="match",
                crosscheck_high_f=row.crosscheck_high_f,
                crosscheck_result="yes" if row.settlement_value_dollars == Decimal("1.0000") else "no",
                payload={"market": {"ticker": row.market_ticker}},
            )
            await repo.create_historical_replay_run(
                room_id=room.id,
                market_ticker=row.market_ticker,
                series_ticker=row.series_ticker,
                local_market_day=row.local_market_day,
                checkpoint_label="late_1400",
                checkpoint_ts=row.checkpoint_ts,
                status="completed",
                agent_pack_version="builtin-gemini-v1",
                payload={
                    "historical_provenance": {
                        "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                        "local_market_day": row.local_market_day,
                        "checkpoint_label": "late_1400",
                        "checkpoint_ts": row.checkpoint_ts.isoformat(),
                        "timezone_name": "America/New_York" if row.series_ticker == "KXHIGHNY" else "America/Chicago",
                        "coverage_class": "full_checkpoint_coverage",
                        "source_coverage": {
                            "market_snapshot": True,
                            "weather_snapshot": True,
                            "settlement_label": True,
                        },
                    }
                },
            )

        await session.commit()

    regression_service = StrategyRegressionService(
        container.settings,
        container.session_factory,
        container.weather_directory,
        container.agent_pack_service,
    )
    evaluation = await regression_service.evaluate_strategy_specs(
        strategies=_strategy_specs(strategies),
        window_days=180,
        run_at=FIXTURE_NOW,
    )
    assert evaluation["status"] == "ok"

    recommendation_counts: Counter[str] = Counter()
    top_candidates: list[dict[str, Any]] = []
    for series_ticker, results in dict(evaluation["city_results"]).items():
        decision = _recommendation_decision(
            results_by_strategy=results,
            current_name=fixture.previous_strategy_name if series_ticker == fixture.approved_series_ticker else fixture.approved_strategy_name,
        )
        recommendation = decision["recommendation"]
        recommendation_counts[recommendation["status"]] += 1
        if recommendation["strategy_name"] is not None:
            top_candidates.append(
                {
                    "series_ticker": series_ticker,
                    "strategy_name": recommendation["strategy_name"],
                    "status": recommendation["status"],
                    "resolved_trade_count": recommendation["resolved_trade_count"],
                    "outcome_coverage_rate": recommendation["outcome_coverage_rate"],
                    "gap_to_runner_up": recommendation["gap_to_runner_up"],
                    "win_rate": decision["best_row"]["win_rate"] if decision["best_row"] is not None else None,
                }
            )

    ny_decision = _recommendation_decision(
        results_by_strategy=evaluation["city_results"][fixture.approved_series_ticker],
        current_name=fixture.previous_strategy_name,
    )
    chi_decision = _recommendation_decision(
        results_by_strategy=evaluation["city_results"][fixture.aligned_series_ticker],
        current_name=fixture.approved_strategy_name,
    )
    assert ny_decision["recommendation"]["strategy_name"] == fixture.approved_strategy_name
    assert ny_decision["recommendation"]["status"] == "strong_recommendation"
    assert chi_decision["recommendation"]["strategy_name"] == fixture.approved_strategy_name

    latest_rows = list(evaluation["result_rows"])
    history_rows = _older_history_rows(latest_rows)

    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        await repo.save_strategy_results(history_rows)
        await repo.save_strategy_results(latest_rows)
        await repo.set_city_strategy_assignment(
            fixture.approved_series_ticker,
            fixture.previous_strategy_name,
            assigned_by="auto_regression",
        )
        await repo.set_city_strategy_assignment(
            fixture.aligned_series_ticker,
            fixture.approved_strategy_name,
            assigned_by="auto_regression",
        )
        await repo.set_checkpoint(
            "strategy_regression",
            None,
            {
                "ran_at": FIXTURE_NOW.isoformat(),
                "rooms_scanned": evaluation["diagnostics"]["rooms_scanned"],
                "rooms_included": evaluation["diagnostics"]["rooms_included"],
                "rooms_skipped_stand_down": evaluation["diagnostics"]["rooms_skipped_stand_down"],
                "rooms_skipped_unmapped": evaluation["diagnostics"]["rooms_skipped_unmapped"],
                "series_evaluated": evaluation["diagnostics"]["series_evaluated"],
                "series_prefixes_seen": evaluation["diagnostics"]["series_prefixes_seen"],
                "strategies_evaluated": len(strategies),
                "recommendation_mode": "recommendation_only",
                "cleared_auto_assignments": 0,
                "promotions": [],
                "recommendation_counts": dict(recommendation_counts),
                "top_candidates": top_candidates[:5],
                "window_days": 180,
            },
        )
        await repo.log_ops_event(
            severity="info",
            summary=f"Strategy auto-promoted for {fixture.approved_series_ticker}: {fixture.previous_strategy_name} -> {fixture.approved_strategy_name}",
            source="strategy_regression",
            payload={
                "series_ticker": fixture.approved_series_ticker,
                "previous_strategy": fixture.previous_strategy_name,
                "new_strategy": fixture.approved_strategy_name,
                "new_win_rate": ny_decision["best_row"]["win_rate"],
                "trade_count": ny_decision["best_row"]["trade_count"],
                "resolved_trade_count": ny_decision["best_row"]["resolved_trade_count"],
                "unscored_trade_count": ny_decision["best_row"]["unscored_trade_count"],
                "gap_to_runner_up": ny_decision["recommendation"]["gap_to_runner_up"],
                "clears_promotion_rule": True,
            },
        )
        await repo.log_ops_event(
            severity="info",
            summary="Auto-adjusted risk_min_edge_bps 50->45",
            source="strategy_eval",
            payload={
                "direction": "loosened",
                "old_bps": 50,
                "new_bps": 45,
                "win_rate": 0.68,
                "total_contracts": 44,
            },
        )
        await session.commit()
    return fixture


def _install_fake_strategy_providers(container: container_module.AppContainer) -> None:
    fake_router = FakeProviderRouter()
    regression_service = StrategyRegressionService(
        container.settings,
        container.session_factory,
        container.weather_directory,
        container.agent_pack_service,
    )
    container.strategy_codex_service = StrategyCodexService(
        container.settings,
        container.session_factory,
        regression_service,
        fake_router,
    )
    container.strategy_dashboard_service.strategy_codex_service = container.strategy_codex_service


def _screenshot_on_failure(page: Page | None, *, site_kind: str) -> None:
    if page is None:
        return
    artifact_path = _artifact_root() / f"strategy-e2e-{site_kind}.png"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(artifact_path), full_page=True)


def _wait_for_strategy_ui(page: Page, *, base_url: str, site_kind: str) -> None:
    page.goto(f"{base_url}/", wait_until="load", timeout=20_000)
    if site_kind == "combined":
        page.locator('.dash-tab[data-env="strategies"]').click(timeout=15_000)
    page.wait_for_selector("#strategies-summary", timeout=20_000)
    page.wait_for_function(
        "() => document.querySelector('#strategies-summary')?.textContent?.includes('180d')",
        timeout=20_000,
    )
    page.wait_for_function(
        "() => document.querySelector('#strategies-cities-detail h3')?.textContent?.includes('KXHIGHNY')",
        timeout=20_000,
    )


def _assert_post_browser_db_state(server: StrategyE2EServer) -> None:
    async def _assertions() -> None:
        async with server.container.session_factory() as session:
            repo = PlatformRepository(session)
            assignment = await repo.get_city_strategy_assignment(server.fixture.approved_series_ticker)
            approval_events = await repo.list_ops_events(limit=20, sources=[STRATEGY_APPROVAL_SOURCE])
            codex_runs = await repo.list_strategy_codex_runs(limit=10)
            saved_strategy = await repo.get_strategy_by_name(server.fixture.accepted_strategy_name)
            await session.commit()

        assert assignment is not None
        assert assignment.strategy_name == server.fixture.approved_strategy_name
        assert assignment.assigned_by == STRATEGY_APPROVAL_ASSIGNED_BY
        assert any(
            event.payload.get("event_kind") == STRATEGY_APPROVAL_EVENT_KIND
            and event.payload.get("series_ticker") == server.fixture.approved_series_ticker
            and event.payload.get("note") == server.fixture.approval_note
            for event in approval_events
        )
        suggestion_run = next(run for run in codex_runs if run.mode == "suggest")
        assert suggestion_run.status == "completed"
        assert (suggestion_run.payload or {}).get("saved_strategy_name") == server.fixture.accepted_strategy_name
        assert saved_strategy is not None
        assert saved_strategy.source == STRATEGY_LAB_SOURCE
        assert saved_strategy.is_active is True

    error: BaseException | None = None

    def _runner() -> None:
        nonlocal error
        try:
            asyncio.run(_assertions())
        except BaseException as exc:  # pragma: no cover - surfaced by the caller
            error = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join(timeout=10)
    if thread.is_alive():
        raise TimeoutError("Timed out while verifying post-browser strategy DB state")
    if error is not None:
        raise error


@contextmanager
def _serve_strategy_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    site_kind: str,
):
    map_path = tmp_path / "strategy-markets.yaml"
    map_path.write_text(_fixture_market_map(), encoding="utf-8")
    db_path = tmp_path / f"strategy-{site_kind}.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    monkeypatch.setenv("WEB_SITE_KIND", site_kind)
    get_settings.cache_clear()

    original_build = container_module.AppContainer.build
    seeded_container: container_module.AppContainer | None = None
    fixture_state: StrategyFixtureState | None = None

    async def build_seeded_container(cls, *, bootstrap_db: bool = True):
        nonlocal seeded_container, fixture_state
        del cls, bootstrap_db
        if seeded_container is None:
            seeded_container = await original_build(bootstrap_db=False)
            fixture_state = await _seed_strategy_fixture(seeded_container)
            _install_fake_strategy_providers(seeded_container)
        return seeded_container

    monkeypatch.setattr(web_app_module.AppContainer, "build", classmethod(build_seeded_container))
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

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    try:
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base_url}/", timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.HTTPError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            raise RuntimeError("Timed out waiting for the strategy dashboard test server to start")

        assert seeded_container is not None
        assert fixture_state is not None
        yield StrategyE2EServer(base_url=base_url, container=seeded_container, fixture=fixture_state)
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        get_settings.cache_clear()


@pytest.mark.parametrize("site_kind", ["strategies", "combined"])
def test_strategy_operator_flow_e2e(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, site_kind: str) -> None:
    with _serve_strategy_dashboard(monkeypatch, tmp_path, site_kind=site_kind) as server:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page: Page | None = None
            try:
                page = browser.new_page(viewport=DESKTOP_VIEWPORT, device_scale_factor=1)
                _wait_for_strategy_ui(page, base_url=server.base_url, site_kind=site_kind)

                summary_text = page.locator("#strategies-summary").text_content(timeout=15_000) or ""
                assert "180d" in summary_text
                assert "Cities Evaluated" in summary_text

                city_detail = page.locator("#strategies-cities-detail")
                approval_note = city_detail.locator('[data-testid="strategy-approval-note"]').first
                approval_submit = city_detail.locator('[data-testid="strategy-approval-submit"]').first
                approval_message = city_detail.locator('[data-testid="strategy-approval-message"]').first

                detail_text = city_detail.text_content(timeout=15_000) or ""
                assert "KXHIGHNY" in detail_text
                assert "Approval" in detail_text
                approval_note.wait_for(timeout=15_000)
                assert approval_note.is_visible()

                approval_note.fill(server.fixture.approval_note, timeout=15_000)
                approval_submit.click(timeout=15_000)

                approval_message.wait_for(timeout=20_000)
                page.wait_for_function(
                    "(note) => (document.querySelector('#strategies-cities-detail')?.textContent || '').includes(note)",
                    arg=server.fixture.approval_note,
                    timeout=20_000,
                )
                refreshed_detail = city_detail.text_content(timeout=15_000) or ""
                assert "Latest approval note" in refreshed_detail
                assert server.fixture.approval_note in refreshed_detail
                assert city_detail.locator('[data-testid="strategy-approval-note"]').count() == 0

                page.locator('#strategies-city-matrix button[data-series-ticker="KXHIGHNY"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => (document.querySelector('#strategies-cities-detail')?.textContent || '').includes('Current assignment')",
                    timeout=15_000,
                )

                city_detail.locator('[data-testid="strategy-open-evaluation-lab"]').click(timeout=15_000)
                page.wait_for_function(
                    "() => document.querySelector('#strategies-focus-strategies')?.hidden === false",
                    timeout=15_000,
                )

                codex_lab = page.locator("#strategies-codex-lab")
                provider_input = codex_lab.locator('[data-testid="strategy-codex-provider"]').first
                model_input = codex_lab.locator('[data-testid="strategy-codex-model"]').first
                run_button = codex_lab.locator('[data-testid="strategy-codex-run"]').first
                run_item = codex_lab.locator('[data-testid="strategy-codex-run-item"]').first
                run_detail = codex_lab.locator('[data-testid="strategy-codex-run-detail"]').first
                accept_button = codex_lab.locator('[data-testid="strategy-codex-accept-suggestion"]').first
                message_box = codex_lab.locator('[data-testid="strategy-codex-message"]').first

                assert provider_input.input_value(timeout=15_000) == "gemini"
                provider_options = codex_lab.locator("#strategies-codex-provider option").evaluate_all(
                    "(nodes) => nodes.map((node) => node.textContent)"
                )
                assert provider_options == ["Gemini", "OpenAI"]
                assert "Codex" not in provider_options
                assert model_input.input_value(timeout=15_000) == "gemini-2.5-pro"
                model_options = codex_lab.locator("#strategies-codex-model option").evaluate_all(
                    "(nodes) => nodes.map((node) => node.value)"
                )
                assert model_options == ["gemini-2.5-pro", "gemini-2.5-flash"]
                model_input.select_option("gemini-2.5-flash")
                assert model_input.input_value(timeout=15_000) == "gemini-2.5-flash"

                page.locator('#strategies-codex-lab .strategy-codex-mode-switch button:has-text("Suggest")').click(timeout=15_000)
                run_button.click(timeout=15_000)

                run_item.wait_for(timeout=20_000)
                run_detail.wait_for(timeout=20_000)
                accept_button.wait_for(timeout=20_000)

                codex_detail = run_detail.text_content(timeout=15_000) or ""
                assert server.fixture.accepted_strategy_name in codex_detail
                assert "Deterministic Backtest" in codex_detail
                assert "Strongest Cities" in codex_detail

                accept_button.click(timeout=15_000)
                page.wait_for_function(
                    "(name) => !!document.querySelector(`[data-testid=\"strategy-codex-inactive-preset\"][data-strategy-name=\"${name}\"]`)",
                    arg=server.fixture.accepted_strategy_name,
                    timeout=20_000,
                )
                page.wait_for_function(
                    "(name) => Array.from(document.querySelectorAll('[data-testid=\"strategy-codex-message\"]')).some((node) => (node.textContent || '').includes(`Saved ${name} as an inactive preset.`))",
                    arg=server.fixture.accepted_strategy_name,
                    timeout=20_000,
                )
                assert f"Saved {server.fixture.accepted_strategy_name} as an inactive preset." in (message_box.text_content(timeout=15_000) or "")

                preset = page.locator(
                    f'[data-testid="strategy-codex-inactive-preset"][data-strategy-name="{server.fixture.accepted_strategy_name}"]'
                )
                preset.locator('[data-testid="strategy-codex-activate-preset"]').click(timeout=15_000)
                page.wait_for_function(
                    "(name) => Array.from(document.querySelectorAll('[data-testid=\"strategy-codex-message\"]')).some((node) => (node.textContent || '').includes(`${name} is now active.`))",
                    arg=server.fixture.accepted_strategy_name,
                    timeout=20_000,
                )
                page.wait_for_function(
                    "(name) => !document.querySelector(`[data-testid=\"strategy-codex-inactive-preset\"][data-strategy-name=\"${name}\"]`)",
                    arg=server.fixture.accepted_strategy_name,
                    timeout=20_000,
                )
                page.wait_for_function(
                    "(name) => Array.from(document.querySelectorAll('[data-testid=\"strategy-codex-run-detail\"]')).some((node) => (node.textContent || '').includes(`${name} is active.`))",
                    arg=server.fixture.accepted_strategy_name,
                    timeout=20_000,
                )

                _assert_post_browser_db_state(server)
            except Exception:
                _screenshot_on_failure(page, site_kind=site_kind)
                raise
            finally:
                browser.close()
