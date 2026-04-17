from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import kalshi_bot.web.control_room as control_room_module
from kalshi_bot.web.control_room import _classify_room, _recent_room_outcomes, _series_filter_options


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
            assert total_capital_dollars == Decimal("250.0")
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
