from pathlib import Path
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import kalshi_bot.web.control_room as control_room_module
from kalshi_bot.web.control_room import _classify_room, _recent_room_outcomes, _series_filter_options
from kalshi_bot.weather.mapping import WeatherMarketDirectory


def test_recent_room_outcomes_excludes_running_rooms_from_resolved_total() -> None:
    now = datetime(2026, 4, 13, 16, 0, tzinfo=UTC)
    room_views = [
        {"status": "running", "updated_at": "2026-04-13T15:55:00+00:00"},
        {"status": "running", "updated_at": "2026-04-13T15:54:00+00:00"},
        {"status": "blocked", "updated_at": "2026-04-13T15:53:00+00:00"},
        {"status": "stand_down", "updated_at": "2026-04-13T15:52:00+00:00"},
        {"status": "failed", "updated_at": "2026-04-13T15:51:00+00:00"},
        {"status": "succeeded", "updated_at": "2026-04-13T15:50:00+00:00"},
    ]

    outcomes = _recent_room_outcomes(room_views, now=now)

    assert outcomes["total"] == 6
    assert outcomes["running"] == 2
    assert outcomes["resolved_total"] == 4
    assert outcomes["success_rate"] == 0.25


def test_series_filter_options_follow_configured_templates() -> None:
    templates = [
        SimpleNamespace(series_ticker="KXHIGHAUS", location_name="Austin", display_name="Austin Daily High Temperature"),
        SimpleNamespace(series_ticker="KXHIGHNY", location_name="New York City", display_name="NYC Daily High Temperature"),
    ]

    options = _series_filter_options(
        [
            {"series_ticker": "KXHIGHNY", "label": "Will the high temp in NYC be >68 on Apr 11, 2026?"},
            {"series_ticker": "KXHIGHAUS", "label": "Will the high temp in Austin be >88 on Apr 11, 2026?"},
        ],
        templates=templates,
    )

    assert options == [
        {"id": "all", "label": "All Series"},
        {"id": "KXHIGHAUS", "label": "Austin"},
        {"id": "KXHIGHNY", "label": "New York City"},
    ]


def test_classify_room_treats_failed_stage_as_failed() -> None:
    bundle = SimpleNamespace(
        outcome=SimpleNamespace(
            fills_observed=0,
            orders_submitted=0,
            ticket_generated=False,
            risk_status=None,
            blocked_by=None,
            final_status="failed",
            stand_down_reason=None,
            room_stage="failed",
        )
    )

    classification = _classify_room(bundle)

    assert classification == {"status": "failed", "label": "Failed", "tone": "bad"}


def test_position_view_ignores_one_sided_book_for_mark_to_market() -> None:
    now = datetime(2026, 4, 17, 18, 5, tzinfo=UTC)
    position = SimpleNamespace(
        market_ticker="KXHIGHCHI-26APR17-T79",
        side="no",
        count_fp=Decimal("24.00"),
        average_price_dollars=Decimal("0.5600"),
        updated_at=now,
    )
    market_state = SimpleNamespace(
        market_ticker="KXHIGHCHI-26APR17-T79",
        yes_bid_dollars=Decimal("0.0400"),
        yes_ask_dollars=None,
        last_trade_dollars=None,
        observed_at=now,
    )

    view = control_room_module._position_view(position, market_state)

    assert view["current_price_display"] == "—"
    assert view["unrealized_pnl_display"] == "—"


class _FakeSession:
    async def commit(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> "_FakeSessionFactory":
        return self

    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def all(self) -> list[tuple]:
        return self._rows


class _OutcomeSession(_FakeSession):
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    async def execute(self, _stmt) -> _FakeResult:
        return _FakeResult(self._rows)


class _OutcomeSessionFactory(_FakeSessionFactory):
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    async def __aenter__(self) -> _OutcomeSession:
        return _OutcomeSession(self._rows)


@pytest.mark.asyncio
async def test_research_confidence_summary_uses_cached_dossiers(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        SimpleNamespace(market_ticker="KXHIGHNY", confidence=0.91),
        SimpleNamespace(market_ticker="KXHIGHAUS", confidence=0.82),
        SimpleNamespace(market_ticker="IGNORED", confidence=0.2),
    ]

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_research_dossiers(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit >= 200
            return records

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        weather_directory=SimpleNamespace(
            all=lambda: [
                SimpleNamespace(market_ticker="KXHIGHNY"),
                SimpleNamespace(market_ticker="KXHIGHAUS"),
            ]
        ),
    )

    summary = await control_room_module._research_confidence_summary(container)

    assert summary == {"average": 0.86, "count": 2, "sparkline": [0.82, 0.91]}


@pytest.mark.asyncio
async def test_recent_room_outcome_views_builds_lightweight_statuses() -> None:
    now = datetime(2026, 4, 14, 18, 0, tzinfo=UTC)
    rows = [
        (
            SimpleNamespace(
                id="room-succeeded",
                name="Succeeded",
                market_ticker="KXHIGHNY-26APR14-T70",
                stage="complete",
                updated_at=now,
                created_at=now,
                agent_pack_version="pack-v1",
                shadow_mode=True,
                room_origin="shadow",
            ),
            1,
            1,
            0,
            "approved",
            True,
            True,
            None,
        ),
        (
            SimpleNamespace(
                id="room-blocked",
                name="Blocked",
                market_ticker="KXHIGHNY-26APR14-T71",
                stage="complete",
                updated_at=now,
                created_at=now,
                agent_pack_version="pack-v1",
                shadow_mode=True,
                room_origin="shadow",
            ),
            1,
            0,
            0,
            "blocked",
            True,
            True,
            None,
        ),
        (
            SimpleNamespace(
                id="room-stand-down",
                name="Stand Down",
                market_ticker="KXHIGHNY-26APR14-T72",
                stage="complete",
                updated_at=now,
                created_at=now,
                agent_pack_version="pack-v1",
                shadow_mode=True,
                room_origin="shadow",
            ),
            0,
            0,
            0,
            None,
            True,
            False,
            "spread_too_wide",
        ),
    ]

    container = SimpleNamespace(session_factory=_OutcomeSessionFactory(rows))

    views = await control_room_module._recent_room_outcome_views(container, now=now)

    assert [item["status"] for item in views] == ["succeeded", "blocked", "stand_down"]


@pytest.mark.asyncio
async def test_build_control_room_summary_skips_live_market_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def get_deployment_control(self) -> SimpleNamespace:
            return SimpleNamespace(active_color="green", kill_switch_enabled=False, execution_lock_holder=None)

        async def list_positions(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit > 0
            return []

        async def list_ops_events(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit > 0
            return []

        async def get_daily_pnl_dollars(self) -> Decimal | None:
            return None

    async def fail_configured_markets(_container) -> list[dict]:
        raise AssertionError("summary should not call live market discovery")

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)
    monkeypatch.setattr(control_room_module, "_configured_markets", fail_configured_markets)
    monkeypatch.setattr(
        control_room_module,
        "_research_confidence_summary",
        AsyncMock(return_value={"average": 0.88, "count": 4, "sparkline": [0.8, 0.9]}),
    )
    monkeypatch.setattr(control_room_module, "_recent_room_bundles", AsyncMock(return_value=[]))
    monkeypatch.setattr(control_room_module, "_recent_room_outcome_views", AsyncMock(return_value=[]))
    monkeypatch.setattr(control_room_module, "_current_intel_board", AsyncMock(return_value=[]))

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        watchdog_service=SimpleNamespace(
            get_status=AsyncMock(
                return_value={"updated_at": "2026-04-14T18:00:00+00:00", "colors": {"green": {"combined_healthy": True}}}
            )
        ),
        training_corpus_service=SimpleNamespace(
            get_dashboard_status=AsyncMock(return_value={"quality_debt_summary": {}, "top_blockers": [], "next_actions": []})
        ),
    )

    summary = await control_room_module.build_control_room_summary(container)

    assert summary["research_confidence"]["average"] == 0.88
    assert summary["research_confidence"]["count"] == 4


@pytest.mark.asyncio
async def test_build_control_room_bootstrap_skips_live_market_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def get_deployment_control(self) -> SimpleNamespace:
            return SimpleNamespace(active_color="green", kill_switch_enabled=False, execution_lock_holder=None)

        async def list_positions(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit > 0
            return []

        async def list_ops_events(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit > 0
            return []

    async def fail_configured_markets(_container) -> list[dict]:
        raise AssertionError("bootstrap should not call live market discovery")

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)
    monkeypatch.setattr(control_room_module, "_configured_markets", fail_configured_markets)
    monkeypatch.setattr(
        control_room_module,
        "_research_confidence_summary",
        AsyncMock(return_value={"average": 0.77, "count": 3, "sparkline": [0.7, 0.8, 0.81]}),
    )
    monkeypatch.setattr(control_room_module, "_recent_room_bundles", AsyncMock(return_value=[]))
    monkeypatch.setattr(control_room_module, "_recent_room_outcome_views", AsyncMock(return_value=[]))
    monkeypatch.setattr(control_room_module, "_current_intel_board", AsyncMock(return_value=[]))

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        watchdog_service=SimpleNamespace(
            get_status=AsyncMock(
                return_value={"updated_at": "2026-04-14T18:00:00+00:00", "colors": {"green": {"combined_healthy": True}}}
            )
        ),
        training_corpus_service=SimpleNamespace(
            get_dashboard_status=AsyncMock(return_value={"quality_debt_summary": {}, "top_blockers": [], "next_actions": []})
        ),
        self_improve_service=SimpleNamespace(get_dashboard_status=AsyncMock(return_value={})),
        historical_intelligence_service=SimpleNamespace(get_dashboard_status=AsyncMock(return_value={})),
    )

    bootstrap = await control_room_module.build_control_room_bootstrap(container)

    assert bootstrap["summary"]["research_confidence"]["average"] == 0.77
    assert bootstrap["initial_tab"] == "overview"


@pytest.mark.asyncio
async def test_build_env_dashboard_includes_balance_and_position_pnl(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 17, 17, 50, tzinfo=UTC)
    positions = [
        SimpleNamespace(
            market_ticker="KXHIGHCHI-26APR17-T79",
            side="no",
            count_fp=Decimal("24.00"),
            average_price_dollars=Decimal("0.5600"),
            updated_at=now,
        ),
        SimpleNamespace(
            market_ticker="KXHIGHTSFO-26APR17-T71",
            side="yes",
            count_fp=Decimal("25.00"),
            average_price_dollars=Decimal("0.1600"),
            updated_at=now,
        ),
    ]
    market_states = [
        SimpleNamespace(
            market_ticker="KXHIGHCHI-26APR17-T79",
            yes_bid_dollars=Decimal("0.0400"),
            yes_ask_dollars=None,
            last_trade_dollars=None,
            observed_at=now,
        ),
        SimpleNamespace(
            market_ticker="KXHIGHTSFO-26APR17-T71",
            yes_bid_dollars=None,
            yes_ask_dollars=None,
            last_trade_dollars=None,
            observed_at=now,
        ),
    ]

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_positions(
            self,
            *,
            limit: int,
            kalshi_env: str | None = None,
            subaccount: int | None = None,
        ) -> list[SimpleNamespace]:
            assert limit == 100
            assert kalshi_env == "demo"
            return positions

        async def list_ops_events(self, *, limit: int) -> list[SimpleNamespace]:
            assert limit == 50
            return []

        async def list_market_states(self, market_tickers: list[str]) -> list[SimpleNamespace]:
            assert market_tickers == [position.market_ticker for position in positions]
            return market_states

        async def get_checkpoint(self, stream_name: str) -> SimpleNamespace:
            assert stream_name == "reconcile"
            return SimpleNamespace(
                payload={"balance": {"balance": 60582, "portfolio_value": 1600}},
                updated_at=now,
            )

        async def get_research_dossier(self, market_ticker: str) -> SimpleNamespace | None:
            payloads = {
                "KXHIGHCHI-26APR17-T79": {
                    "trade_regime": "standard",
                    "model_quality_status": "warn",
                    "model_quality_reasons": ["Strict quality review would block this setup because the order book is effectively broken."],
                    "recommended_size_cap_fp": None,
                    "warn_only_blocked": True,
                },
                "KXHIGHTSFO-26APR17-T71": {
                    "trade_regime": "near_threshold",
                    "model_quality_status": "warn",
                    "model_quality_reasons": ["Near-threshold setup carries low confidence and should be sized conservatively."],
                    "recommended_size_cap_fp": "10.00",
                    "warn_only_blocked": False,
                },
            }
            payload = payloads.get(market_ticker)
            return SimpleNamespace(payload=payload) if payload is not None else None

        async def latest_signal_payloads_for_markets(
            self,
            *,
            market_tickers: list[str],
            kalshi_env: str,
        ) -> dict[str, dict[str, str]]:
            assert market_tickers == [position.market_ticker for position in positions]
            assert kalshi_env == "demo"
            return {
                "KXHIGHCHI-26APR17-T79": {"capital_bucket": "safe", "trade_regime": "standard"},
                "KXHIGHTSFO-26APR17-T71": {"capital_bucket": "risky", "trade_regime": "near_threshold"},
            }

        async def list_active_rooms(
            self,
            *,
            kalshi_env: str | None = None,
            updated_within_seconds: int | None = None,
            limit: int = 20,
        ) -> list[SimpleNamespace]:
            return []

        async def get_total_capital_dollars(self) -> Decimal | None:
            return Decimal("534.17")

        async def get_daily_portfolio_baseline_dollars(self) -> Decimal | None:
            return Decimal("618.61")

        async def get_daily_pnl_dollars(self) -> Decimal | None:
            return Decimal("3.21")

        async def get_fill_win_rate_30d(self) -> dict:
            return {"won_contracts": 47.0, "total_contracts": 68.0}

        async def portfolio_bucket_snapshot(
            self,
            *,
            kalshi_env: str,
            subaccount: int,
            total_capital_dollars: Decimal,
            safe_capital_reserve_ratio: float,
            risky_capital_max_ratio: float,
        ) -> SimpleNamespace:
            assert kalshi_env == "demo"
            assert subaccount == 0
            assert total_capital_dollars == Decimal("534.17")
            assert safe_capital_reserve_ratio == 0.70
            assert risky_capital_max_ratio == 0.30
            return SimpleNamespace(
                safe_used_dollars=Decimal("13.4400"),
                safe_remaining_dollars=Decimal("232.5600"),
                safe_reserve_target_dollars=Decimal("175.0000"),
                risky_used_dollars=Decimal("4.0000"),
                risky_limit_dollars=Decimal("75.0000"),
                risky_remaining_dollars=Decimal("71.0000"),
                overall_used_dollars=Decimal("17.4400"),
                overall_remaining_dollars=Decimal("232.5600"),
            )

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)

    async def fake_get_market(ticker: str) -> dict[str, str]:
        markets = {
            "KXHIGHCHI-26APR17-T79": {
                "ticker": "KXHIGHCHI-26APR17-T79",
                "last_price_dollars": "0.5000",
            },
            "KXHIGHTSFO-26APR17-T71": {
                "ticker": "KXHIGHTSFO-26APR17-T71",
                "last_price_dollars": "0.1600",
            },
        }
        return markets[ticker]

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        watchdog_service=SimpleNamespace(get_status=AsyncMock(return_value={"updated_at": now.isoformat(), "colors": {}})),
        kalshi=SimpleNamespace(get_market=AsyncMock(side_effect=fake_get_market)),
        settings=SimpleNamespace(app_color="blue", kalshi_subaccount=0, trigger_active_room_stale_seconds=1800),
        agent_pack_service=SimpleNamespace(
            get_pack_for_color=AsyncMock(return_value=SimpleNamespace()),
            runtime_thresholds=lambda _pack: SimpleNamespace(
                risk_max_position_notional_dollars=250.0,
                risk_safe_capital_reserve_ratio=0.70,
                risk_risky_capital_max_ratio=0.30,
            ),
        ),
    )

    payload = await control_room_module.build_env_dashboard(container, "demo")

    assert payload["portfolio"]["cash_display"] == "$605.82"
    assert payload["portfolio"]["portfolio_display"] == "$621.82"
    assert payload["portfolio"]["positions_value_display"] == "$16.00"
    assert payload["portfolio"]["gain_loss_display"] == "-$1.44"
    assert payload["daily_pnl_display"] == "+$3.21"
    assert payload["daily_pnl_percent_display"] == "0.52%"
    assert payload["daily_pnl_line_display"] == "+$3.21 (0.52%) today (PT)"
    assert payload["positions_summary"]["has_pnl_summary"] is True
    assert payload["positions"][0]["current_price_display"] == "$0.5000"
    assert payload["positions"][0]["unrealized_pnl_display"] == "-$1.44"
    assert payload["positions"][0]["model_quality_status"] == "warn"
    assert payload["positions"][0]["warn_only_blocked"] is True
    assert payload["positions"][0]["capital_bucket"] == "safe"
    assert payload["positions"][1]["current_price_display"] == "$0.1600"
    assert payload["positions"][1]["unrealized_pnl_display"] == "$0.00"
    assert payload["positions"][1]["trade_regime"] == "near_threshold"
    assert payload["positions"][1]["recommended_size_cap_fp"] == "10.00"
    assert payload["positions_summary"]["capital_buckets"]["risky_limit_display"] == "$75.00"
    assert container.kalshi.get_market.await_count == 2


def _strategy_thresholds(*, min_edge_bps: int, quality_buffer_bps: int = 20, min_remaining_payout_bps: int = 500) -> dict[str, object]:
    return {
        "risk_min_edge_bps": min_edge_bps,
        "risk_max_order_notional_dollars": 10.0,
        "risk_max_position_notional_dollars": 25.0,
        "trigger_max_spread_bps": 500,
        "trigger_cooldown_seconds": 300,
        "strategy_quality_edge_buffer_bps": quality_buffer_bps,
        "strategy_min_remaining_payout_bps": min_remaining_payout_bps,
        "risk_safe_capital_reserve_ratio": 0.70,
        "risk_risky_capital_max_ratio": 0.30,
    }


class _FakeStrategyWeatherDirectory:
    def __init__(self) -> None:
        self._templates = [
            SimpleNamespace(series_ticker="KXHIGHNY", label="New York City", location_name="New York City"),
            SimpleNamespace(series_ticker="KXHIGHCHI", label="Chicago", location_name="Chicago"),
        ]
        self._resolved = {
            "KXHIGHNY-ROOM-1": SimpleNamespace(series_ticker="KXHIGHNY"),
            "KXHIGHNY-ROOM-2": SimpleNamespace(series_ticker="KXHIGHNY"),
            "KXHIGHCHI-ROOM-1": SimpleNamespace(series_ticker="KXHIGHCHI"),
        }

    def templates(self) -> list[SimpleNamespace]:
        return list(self._templates)

    def all(self) -> list[SimpleNamespace]:
        return []

    def resolve_market(self, market_ticker: str) -> SimpleNamespace | None:
        return self._resolved.get(market_ticker)


def _strategy_result_row(
    *,
    strategy_id: int,
    run_at: datetime,
    series_ticker: str,
    rooms_evaluated: int,
    trade_count: int,
    resolved_trade_count: int,
    win_count: int,
    total_pnl_dollars: Decimal | None,
    trade_rate: Decimal | None,
    win_rate: Decimal | None,
    avg_edge_bps: Decimal | None,
    unscored_trade_count: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id=strategy_id,
        run_at=run_at,
        series_ticker=series_ticker,
        rooms_evaluated=rooms_evaluated,
        trade_count=trade_count,
        resolved_trade_count=resolved_trade_count,
        unscored_trade_count=unscored_trade_count if unscored_trade_count is not None else max(0, trade_count - resolved_trade_count),
        win_count=win_count,
        total_pnl_dollars=total_pnl_dollars,
        trade_rate=trade_rate,
        win_rate=win_rate,
        avg_edge_bps=avg_edge_bps,
    )


def _replay_room(
    *,
    market_ticker: str,
    edge_bps: int,
    settlement_value_dollars: str | None,
    ticket_yes_price_dollars: str | None,
    series_ticker: str | None = None,
) -> dict[str, object]:
    return {
        "market_ticker": market_ticker,
        "series_ticker": series_ticker,
        "edge_bps": edge_bps,
        "fair_yes_dollars": Decimal("0.6200"),
        "signal_payload": {"eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"}},
        "ticket_side": "yes" if ticket_yes_price_dollars is not None else None,
        "ticket_yes_price_dollars": ticket_yes_price_dollars,
        "ticket_count_fp": "1.00" if ticket_yes_price_dollars is not None else None,
        "settlement_value_dollars": settlement_value_dollars,
        "kalshi_result": "yes" if settlement_value_dollars == "1.0000" else "no" if settlement_value_dollars == "0.0000" else None,
    }


@pytest.mark.asyncio
async def test_build_strategies_dashboard_builds_research_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 21, 18, 0, tzinfo=UTC)

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            assert active_only is True
            return [
                SimpleNamespace(id=1, name="aggressive", description="Loose filters", thresholds=_strategy_thresholds(min_edge_bps=20)),
                SimpleNamespace(id=2, name="moderate", description="Balanced filters", thresholds=_strategy_thresholds(min_edge_bps=40)),
            ]

        async def list_city_strategy_assignments(self) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(series_ticker="KXHIGHNY", strategy_name="aggressive", assigned_at=now, assigned_by="auto_regression"),
                SimpleNamespace(series_ticker="KXHIGHCHI", strategy_name="moderate", assigned_at=now, assigned_by="auto_regression"),
            ]

        async def get_checkpoint(self, stream_name: str) -> SimpleNamespace:
            assert stream_name == "strategy_regression"
            return SimpleNamespace(
                payload={"ran_at": now.isoformat(), "rooms_scanned": 84, "series_evaluated": 2, "promotions": []},
                updated_at=now,
            )

        async def list_ops_events(self, *, limit: int, sources: list[str] | None = None, created_after=None) -> list[SimpleNamespace]:
            assert "strategy_regression" in (sources or [])
            assert "strategy_eval" in (sources or [])
            assert "strategy_review" in (sources or [])
            return [
                SimpleNamespace(
                    source="strategy_regression",
                    summary="Strategy auto-promoted for KXHIGHNY: aggressive -> moderate",
                    payload={
                        "series_ticker": "KXHIGHNY",
                        "previous_strategy": "aggressive",
                        "new_strategy": "moderate",
                        "new_win_rate": 0.75,
                        "trade_count": 24,
                        "resolved_trade_count": 24,
                        "unscored_trade_count": 0,
                        "gap_to_runner_up": 0.15,
                        "clears_promotion_rule": True,
                    },
                    updated_at=now,
                ),
                SimpleNamespace(
                    source="strategy_eval",
                    summary="Auto-adjusted risk_min_edge_bps 50->40",
                    payload={"direction": "loosened", "old_bps": 50, "new_bps": 40, "win_rate": 0.63, "total_contracts": 82},
                    updated_at=now,
                ),
                SimpleNamespace(
                    source="strategy_review",
                    summary="Approved strategy assignment for KXHIGHNY: aggressive -> moderate",
                    payload={
                        "event_kind": "assignment_approval",
                        "series_ticker": "KXHIGHNY",
                        "previous_strategy": "aggressive",
                        "new_strategy": "moderate",
                        "recommendation_status": "strong_recommendation",
                        "recommendation_label": "Strong recommendation",
                        "trade_count": 24,
                        "resolved_trade_count": 24,
                        "unscored_trade_count": 0,
                        "gap_to_runner_up": 0.15,
                        "outcome_coverage_rate": 1.0,
                        "new_win_rate": 0.75,
                        "note": "Observed enough evidence in the latest 180d snapshot.",
                    },
                    updated_at=now,
                ),
            ]

        async def get_latest_strategy_results(self) -> list[SimpleNamespace]:
            return [
                _strategy_result_row(strategy_id=1, run_at=now, series_ticker="KXHIGHNY", rooms_evaluated=40, trade_count=20, resolved_trade_count=20, win_count=12, total_pnl_dollars=Decimal("5.00"), trade_rate=Decimal("0.5000"), win_rate=Decimal("0.6000"), avg_edge_bps=Decimal("75.0")),
                _strategy_result_row(strategy_id=2, run_at=now, series_ticker="KXHIGHNY", rooms_evaluated=40, trade_count=24, resolved_trade_count=24, win_count=18, total_pnl_dollars=Decimal("8.40"), trade_rate=Decimal("0.6000"), win_rate=Decimal("0.7500"), avg_edge_bps=Decimal("68.0")),
                _strategy_result_row(strategy_id=1, run_at=now, series_ticker="KXHIGHCHI", rooms_evaluated=44, trade_count=10, resolved_trade_count=10, win_count=4, total_pnl_dollars=Decimal("-1.20"), trade_rate=Decimal("0.2273"), win_rate=Decimal("0.4000"), avg_edge_bps=Decimal("58.0")),
                _strategy_result_row(strategy_id=2, run_at=now, series_ticker="KXHIGHCHI", rooms_evaluated=44, trade_count=26, resolved_trade_count=26, win_count=15, total_pnl_dollars=Decimal("2.80"), trade_rate=Decimal("0.5909"), win_rate=Decimal("0.5769"), avg_edge_bps=Decimal("52.0")),
            ]

        async def list_strategy_results_history(
            self,
            *,
            strategy_ids: list[int] | None = None,
            series_ticker: str | None = None,
            run_after=None,
            limit: int = 500,
        ) -> list[SimpleNamespace]:
            assert limit > 0
            if strategy_ids == [2]:
                earlier = now.replace(day=20)
                return [
                    _strategy_result_row(strategy_id=2, run_at=earlier, series_ticker="KXHIGHNY", rooms_evaluated=32, trade_count=18, resolved_trade_count=18, win_count=12, total_pnl_dollars=Decimal("5.20"), trade_rate=Decimal("0.5625"), win_rate=Decimal("0.6667"), avg_edge_bps=Decimal("60.0")),
                    _strategy_result_row(strategy_id=2, run_at=earlier, series_ticker="KXHIGHCHI", rooms_evaluated=30, trade_count=20, resolved_trade_count=20, win_count=11, total_pnl_dollars=Decimal("1.10"), trade_rate=Decimal("0.6667"), win_rate=Decimal("0.5500"), avg_edge_bps=Decimal("49.0")),
                    _strategy_result_row(strategy_id=2, run_at=now, series_ticker="KXHIGHNY", rooms_evaluated=40, trade_count=24, resolved_trade_count=24, win_count=18, total_pnl_dollars=Decimal("8.40"), trade_rate=Decimal("0.6000"), win_rate=Decimal("0.7500"), avg_edge_bps=Decimal("68.0")),
                    _strategy_result_row(strategy_id=2, run_at=now, series_ticker="KXHIGHCHI", rooms_evaluated=44, trade_count=26, resolved_trade_count=26, win_count=15, total_pnl_dollars=Decimal("2.80"), trade_rate=Decimal("0.5909"), win_rate=Decimal("0.5769"), avg_edge_bps=Decimal("52.0")),
                ]
            return []

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        weather_directory=_FakeStrategyWeatherDirectory(),
    )

    payload = await control_room_module.build_strategies_dashboard(container)

    assert set(payload) == {"summary", "leaderboard", "city_matrix", "detail_context", "recent_promotions", "methodology"}
    assert payload["summary"]["window_days"] == 180
    assert payload["summary"]["recommendation_mode"] == "recommendation_only"
    assert payload["summary"]["best_strategy_name"] == "moderate"
    assert payload["summary"]["strong_recommendations_count"] == 2
    assert payload["summary"]["lean_recommendations_count"] == 0
    assert payload["detail_context"]["type"] == "strategy"
    city_row = next(row for row in payload["city_matrix"] if row["series_ticker"] == "KXHIGHNY")
    assert city_row["best_strategy"] == "moderate"
    assert city_row["assignment"]["strategy_name"] == "aggressive"
    assert city_row["recommendation"]["strategy_name"] == "moderate"
    assert city_row["recommendation"]["status"] == "strong_recommendation"
    assert city_row["assignment_context_status"] == "differs_from_recommendation"
    assert city_row["approval_eligible"] is True
    assert city_row["approval_label"] == "Ready to approve"
    assert city_row["approval_window_days"] == 180
    assert city_row["can_promote"] is False
    assert city_row["best_outcome_coverage_display"] == "24/24 scored"
    assert payload["recent_promotions"][0]["kind"] == "promotion"
    assert payload["recent_promotions"][0]["resolved_trade_count"] == 24
    assert any(event["kind"] == "assignment_approval" for event in payload["recent_promotions"])
    assert payload["summary"]["recent_approvals_count"] == 1
    assert payload["leaderboard"][0]["name"] == "moderate"
    assert payload["leaderboard"][0]["outcome_coverage_display"] == "50/50 scored"


@pytest.mark.asyncio
async def test_build_strategies_dashboard_live_window_builds_city_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 21, 18, 0, tzinfo=UTC)

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(id=1, name="aggressive", description="Loose filters", thresholds=_strategy_thresholds(min_edge_bps=20)),
                SimpleNamespace(id=2, name="moderate", description="Balanced filters", thresholds=_strategy_thresholds(min_edge_bps=40)),
            ]

        async def list_city_strategy_assignments(self) -> list[SimpleNamespace]:
            return [SimpleNamespace(series_ticker="KXHIGHNY", strategy_name="aggressive", assigned_at=now, assigned_by="auto_regression")]

        async def get_checkpoint(self, stream_name: str) -> SimpleNamespace:
            return SimpleNamespace(payload={"ran_at": now.isoformat(), "rooms_scanned": 10, "series_evaluated": 1, "promotions": []}, updated_at=now)

        async def list_ops_events(self, *, limit: int, sources: list[str] | None = None, created_after=None) -> list[SimpleNamespace]:
            return []

        async def get_strategy_regression_rooms(self, date_from: datetime, date_to: datetime) -> list[dict[str, object]]:
            assert (date_to - date_from).days >= 29
            return [
                _replay_room(market_ticker="KXHIGHNY-26APR18-T68", edge_bps=55, ticket_yes_price_dollars="0.40", settlement_value_dollars="1.0000", series_ticker="KXHIGHNY"),
                _replay_room(market_ticker="KXHIGHNY-26APR18-T61", edge_bps=35, ticket_yes_price_dollars="0.45", settlement_value_dollars="0.0000", series_ticker="KXHIGHNY"),
                _replay_room(market_ticker="KXHIGHCHI-26APR18-T65", edge_bps=25, ticket_yes_price_dollars="0.41", settlement_value_dollars="1.0000", series_ticker="KXHIGHCHI"),
            ]

        async def list_strategy_results_history(
            self,
            *,
            strategy_ids: list[int] | None = None,
            series_ticker: str | None = None,
            run_after=None,
            limit: int = 500,
        ) -> list[SimpleNamespace]:
            if series_ticker == "KXHIGHNY":
                earlier = now.replace(day=20)
                return [
                    _strategy_result_row(strategy_id=1, run_at=earlier, series_ticker="KXHIGHNY", rooms_evaluated=12, trade_count=8, resolved_trade_count=8, win_count=4, total_pnl_dollars=Decimal("1.00"), trade_rate=Decimal("0.6667"), win_rate=Decimal("0.5000"), avg_edge_bps=Decimal("40.0")),
                    _strategy_result_row(strategy_id=2, run_at=earlier, series_ticker="KXHIGHNY", rooms_evaluated=12, trade_count=6, resolved_trade_count=6, win_count=4, total_pnl_dollars=Decimal("1.40"), trade_rate=Decimal("0.5000"), win_rate=Decimal("0.6667"), avg_edge_bps=Decimal("48.0")),
                    _strategy_result_row(strategy_id=1, run_at=now, series_ticker="KXHIGHNY", rooms_evaluated=14, trade_count=10, resolved_trade_count=10, win_count=6, total_pnl_dollars=Decimal("1.80"), trade_rate=Decimal("0.7143"), win_rate=Decimal("0.6000"), avg_edge_bps=Decimal("44.0")),
                    _strategy_result_row(strategy_id=2, run_at=now, series_ticker="KXHIGHNY", rooms_evaluated=14, trade_count=8, resolved_trade_count=8, win_count=6, total_pnl_dollars=Decimal("2.20"), trade_rate=Decimal("0.5714"), win_rate=Decimal("0.7500"), avg_edge_bps=Decimal("51.0")),
                ]
            return []

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)
    monkeypatch.setattr(control_room_module, "datetime", SimpleNamespace(now=lambda _tz=UTC: now))

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        weather_directory=WeatherMarketDirectory.from_file(Path("docs/examples/weather_markets.example.yaml")),
    )

    payload = await control_room_module.build_strategies_dashboard(container, window_days=30, series_ticker="KXHIGHNY")

    assert payload["summary"]["window_days"] == 30
    assert payload["summary"]["source_mode"] == "live_eval"
    assert payload["summary"]["cities_evaluated"] == 2
    assert payload["detail_context"]["type"] == "city"
    assert payload["detail_context"]["selected_series_ticker"] == "KXHIGHNY"
    assert payload["detail_context"]["city"]["approval_eligible"] is False
    assert payload["detail_context"]["approval"]["eligible"] is False
    assert payload["detail_context"]["recommendation_rationale"]["best_strategy"] in {"aggressive", "moderate"}
    assert payload["detail_context"]["recommendation_rationale"]["best_outcome_coverage_display"].endswith("scored")
    assert payload["detail_context"]["recommendation_rationale"]["recommendation_status"] == "low_sample"
    assert payload["detail_context"]["recommendation_rationale"]["writes_assignment"] is False
    assert payload["detail_context"]["trend"]["window_days"] == 180


@pytest.mark.asyncio
async def test_build_strategies_dashboard_uses_neutral_summary_when_no_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 21, 18, 0, tzinfo=UTC)

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            assert active_only is True
            return [
                SimpleNamespace(id=1, name="aggressive", description="Loose filters", thresholds=_strategy_thresholds(min_edge_bps=20)),
                SimpleNamespace(id=2, name="moderate", description="Balanced filters", thresholds=_strategy_thresholds(min_edge_bps=40)),
            ]

        async def list_city_strategy_assignments(self) -> list[SimpleNamespace]:
            return []

        async def get_checkpoint(self, stream_name: str) -> SimpleNamespace:
            assert stream_name == "strategy_regression"
            return SimpleNamespace(payload={"ran_at": now.isoformat(), "rooms_scanned": 12, "series_evaluated": 0, "promotions": []}, updated_at=now)

        async def list_ops_events(self, *, limit: int, sources: list[str] | None = None, created_after=None) -> list[SimpleNamespace]:
            return []

        async def get_latest_strategy_results(self) -> list[SimpleNamespace]:
            return [
                _strategy_result_row(
                    strategy_id=1,
                    run_at=now,
                    series_ticker="KXHIGHNY",
                    rooms_evaluated=12,
                    trade_count=6,
                    resolved_trade_count=0,
                    unscored_trade_count=6,
                    win_count=0,
                    total_pnl_dollars=None,
                    trade_rate=Decimal("0.5000"),
                    win_rate=None,
                    avg_edge_bps=Decimal("41.0"),
                ),
                _strategy_result_row(
                    strategy_id=2,
                    run_at=now,
                    series_ticker="KXHIGHNY",
                    rooms_evaluated=12,
                    trade_count=4,
                    resolved_trade_count=0,
                    unscored_trade_count=4,
                    win_count=0,
                    total_pnl_dollars=None,
                    trade_rate=Decimal("0.3333"),
                    win_rate=None,
                    avg_edge_bps=Decimal("53.0"),
                ),
            ]

        async def list_strategy_results_history(
            self,
            *,
            strategy_ids: list[int] | None = None,
            series_ticker: str | None = None,
            run_after=None,
            limit: int = 500,
        ) -> list[SimpleNamespace]:
            return []

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        weather_directory=_FakeStrategyWeatherDirectory(),
    )

    payload = await control_room_module.build_strategies_dashboard(container)

    assert payload["summary"]["best_strategy_name"] == "—"
    assert payload["summary"]["best_strategy_win_rate"] is None
    assert payload["summary"]["cities_evaluated"] == 1
    assert payload["summary"]["strong_recommendations_count"] == 0
    assert payload["summary"]["lean_recommendations_count"] == 0
    assert payload["leaderboard"][0]["total_pnl_display"] == "—"
    assert payload["city_matrix"][0]["evidence_status"] == "no_outcomes"
    assert payload["city_matrix"][0]["recommendation"]["status"] == "no_outcomes"
    assert payload["detail_context"]["type"] == "empty"


@pytest.mark.asyncio
async def test_build_strategies_dashboard_excludes_legacy_unscored_promotions(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 21, 18, 0, tzinfo=UTC)

    class FakeRepo:
        def __init__(self, _session) -> None:
            pass

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            assert active_only is True
            return [
                SimpleNamespace(id=1, name="aggressive", description="Loose filters", thresholds=_strategy_thresholds(min_edge_bps=20)),
                SimpleNamespace(id=2, name="moderate", description="Balanced filters", thresholds=_strategy_thresholds(min_edge_bps=40)),
            ]

        async def list_city_strategy_assignments(self) -> list[SimpleNamespace]:
            return []

        async def get_checkpoint(self, stream_name: str) -> SimpleNamespace:
            assert stream_name == "strategy_regression"
            return SimpleNamespace(payload={"ran_at": now.isoformat(), "rooms_scanned": 10, "series_evaluated": 1, "promotions": []}, updated_at=now)

        async def list_ops_events(self, *, limit: int, sources: list[str] | None = None, created_after=None) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    source="strategy_regression",
                    summary="Strategy auto-promoted for KXHIGHNY: none -> aggressive",
                    payload={"event_kind": "promotion", "series_ticker": "KXHIGHNY", "new_strategy": "aggressive", "new_win_rate": 0.0, "trade_count": 24},
                    updated_at=now,
                )
            ]

        async def get_latest_strategy_results(self) -> list[SimpleNamespace]:
            return []

        async def list_strategy_results_history(
            self,
            *,
            strategy_ids: list[int] | None = None,
            series_ticker: str | None = None,
            run_after=None,
            limit: int = 500,
        ) -> list[SimpleNamespace]:
            return []

    monkeypatch.setattr(control_room_module, "PlatformRepository", FakeRepo)

    container = SimpleNamespace(
        session_factory=_FakeSessionFactory(),
        weather_directory=_FakeStrategyWeatherDirectory(),
    )

    payload = await control_room_module.build_strategies_dashboard(container)

    assert payload["summary"]["recent_promotions_count"] == 0
    assert payload["recent_promotions"] == []
