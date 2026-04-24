from __future__ import annotations

import inspect
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import kalshi_bot.services.strategy_regression_ranking as ranking_module
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_regression_ranking import (
    RANKING_VERSION,
    StrategyRegressionRankingReportService,
    _rank_rows,
)


def _thresholds(*, min_edge_bps: int = 0) -> dict:
    return {
        "risk_min_edge_bps": min_edge_bps,
        "risk_max_order_notional_dollars": 15.0,
        "risk_max_position_notional_dollars": 40.0,
        "trigger_max_spread_bps": 800,
        "trigger_cooldown_seconds": 180,
        "strategy_quality_edge_buffer_bps": 0,
        "strategy_min_remaining_payout_bps": 0,
        "risk_safe_capital_reserve_ratio": 0.60,
        "risk_risky_capital_max_ratio": 0.40,
    }


async def _setup(tmp_path: Path, **setting_overrides) -> SimpleNamespace:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'strategy_regression_ranking.db'}",
        strategy_regression_promote_floor_clusters=setting_overrides.get("promote_floor", 30),
        strategy_regression_min_clusters_for_ranking=setting_overrides.get("min_clusters", 3),
        strategy_regression_min_sortino_for_promotion=setting_overrides.get("min_sortino", 0.5),
        strategy_regression_sortino_downside_epsilon_dollars=setting_overrides.get("epsilon", 1.0),
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    return SimpleNamespace(
        settings=settings,
        session_factory=session_factory,
        service=StrategyRegressionRankingReportService(settings, session_factory),
    )


async def _seed_strategies(session_factory, specs: list[tuple[str, int]]) -> None:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for name, min_edge_bps in specs:
            await repo.create_strategy(
                name=name,
                description=f"{name} fixture",
                thresholds=_thresholds(min_edge_bps=min_edge_bps),
                source="fixture",
            )
        await session.commit()


async def _create_build(
    session_factory,
    rows: list[dict],
    *,
    promote_env: str | None = None,
) -> str:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        build = await repo.create_decision_corpus_build(
            version="strategy-ranking-fixture",
            date_from=date(2026, 4, 20),
            date_to=date(2026, 4, 24),
            source={"type": "fixture"},
            filters={"fixture": True},
            git_sha="fixture-sha",
        )
        for index, spec in enumerate(rows):
            local_day = date.fromisoformat(spec.get("local_market_day", "2026-04-20"))
            checkpoint_ts = spec.get("checkpoint_ts") or datetime(
                local_day.year,
                local_day.month,
                local_day.day,
                17,
                index % 60,
                tzinfo=UTC,
            )
            room = await repo.create_room(
                RoomCreate(name=f"Ranking fixture {index}", market_ticker=f"KXRANK-{index}"),
                active_color=DeploymentColor.BLUE.value,
                shadow_mode=True,
                kill_switch_enabled=False,
                kalshi_env=spec.get("kalshi_env", "demo"),
                room_origin="historical_replay",
                agent_pack_version="model-v1",
            )
            await repo.add_decision_corpus_row(
                corpus_build_id=build.id,
                room_id=room.id,
                market_ticker=spec.get("market_ticker", f"KXRANK-{index}"),
                series_ticker=spec.get("series_ticker", "KXHIGHNY"),
                station_id=spec.get("station_id", "KNYC"),
                local_market_day=local_day.isoformat(),
                checkpoint_ts=checkpoint_ts,
                kalshi_env=spec.get("kalshi_env", "demo"),
                deployment_color="blue",
                model_version=spec.get("model_version", "model-v1"),
                policy_version=spec.get("policy_version", "policy-v1"),
                source_asof_ts=checkpoint_ts,
                quote_observed_at=checkpoint_ts,
                quote_captured_at=checkpoint_ts,
                time_to_settlement_at_checkpoint_minutes=360,
                fair_yes_dollars=Decimal("0.6000"),
                edge_bps=spec.get("edge_bps", 100),
                recommended_side=spec.get("recommended_side", "yes"),
                target_yes_price_dollars=Decimal("0.5000"),
                eligibility_status=spec.get("eligibility_status", "eligible"),
                stand_down_reason=spec.get("stand_down_reason"),
                trade_regime="standard",
                liquidity_regime="normal",
                support_status=spec.get("support_status", "supported"),
                support_level="L1_station_season_lead_regime",
                support_n=150,
                support_market_days=22,
                support_recency_days=10,
                backoff_path=[
                    {
                        "level": "L1_station_season_lead_regime",
                        "n": 150,
                        "market_days": 22,
                        "status": spec.get("support_status", "supported"),
                        "failed_on": [],
                    }
                ],
                settlement_result="yes",
                settlement_value_dollars=Decimal("1.0000"),
                pnl_counterfactual_target_frictionless=spec.get("pnl_target_frictionless"),
                pnl_counterfactual_target_with_fees=spec.get("pnl_target_with_fees"),
                pnl_model_fair_frictionless=spec.get("pnl_model_fair", Decimal("-99.000000")),
                pnl_executed_realized=None,
                fee_counterfactual_dollars=Decimal("0.000000"),
                counterfactual_count=Decimal("1.00"),
                executed_count=None,
                fee_model_version="fixture-fee-v1",
                source_provenance="historical_replay_full_checkpoint",
                source_details={"checkpoint_label": "checkpoint_1"},
                signal_payload={
                    "eligibility": {
                        "market_spread_bps": spec.get("spread_bps", 10),
                        "remaining_payout_dollars": spec.get("remaining_payout_dollars", "0.90"),
                    }
                },
                quote_snapshot={},
                settlement_payload={},
                diagnostics={"season_bucket": "spring", "lead_bucket": "near"},
            )
        await repo.mark_decision_corpus_build_successful(build.id, row_count=len(rows))
        if promote_env is not None:
            await repo.promote_decision_corpus_build(build.id, kalshi_env=promote_env, actor="fixture")
        await session.commit()
        return build.id


def _rows_for_pnls(values: list[Decimal | None]) -> list[dict]:
    rows = []
    for index, value in enumerate(values):
        local_day = date(2026, 4, 20) + timedelta(days=index)
        rows.append(
            {
                "local_market_day": local_day.isoformat(),
                "market_ticker": f"KXRANK-{index}",
                "pnl_target_frictionless": value,
                "pnl_target_with_fees": value,
                "pnl_model_fair": Decimal("-99.000000"),
            }
        )
    return rows


@pytest.mark.asyncio
async def test_rank_report_uses_target_with_fees_excludes_null_pnl_and_tags_version(tmp_path: Path) -> None:
    harness = await _setup(tmp_path, promote_floor=3, min_clusters=3)
    await _seed_strategies(harness.session_factory, [("loose", 0), ("strict", 999)])
    rows = _rows_for_pnls([Decimal("3.000000"), Decimal("-1.000000"), Decimal("2.000000"), None])
    rows[-1]["recommended_side"] = None
    rows[-1]["stand_down_reason"] = "below_min_edge"
    build_id = await _create_build(harness.session_factory, rows)

    result = await harness.service.rank_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 11, 0, tzinfo=UTC),
    )

    assert result["exit_code"] == 0
    report = json.loads(Path(result["json_path"]).read_text())
    assert report["report_metadata"]["ranking_version"] == RANKING_VERSION
    assert report["diagnostics"]["ranking_input"] == "pnl_counterfactual_target_with_fees"
    loose_row = next(row for row in report["result_rows"] if row["strategy_name"] == "loose")
    assert loose_row["ranking_version"] == RANKING_VERSION
    assert loose_row["cluster_count"] == 3
    assert loose_row["total_rows_contributing"] == 3
    assert loose_row["null_pnl_rows_excluded"] == 1
    assert loose_row["total_net_pnl_dollars"] == pytest.approx(4.0)
    assert loose_row["sortino"] == pytest.approx(1.333333)
    assert loose_row["win_rate_display_only"] is True
    assert report["recommended_for_promotion"][0]["strategy_name"] == "loose"


@pytest.mark.asyncio
async def test_rank_report_flags_sparse_clusters_without_computing_sortino(tmp_path: Path) -> None:
    harness = await _setup(tmp_path, promote_floor=30, min_clusters=3)
    await _seed_strategies(harness.session_factory, [("loose", 0)])
    build_id = await _create_build(
        harness.session_factory,
        _rows_for_pnls([Decimal("1.000000"), Decimal("-0.500000")]),
    )

    result = await harness.service.rank_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 11, 1, tzinfo=UTC),
    )

    assert result["exit_code"] == 1
    report = json.loads(Path(result["json_path"]).read_text())
    row = report["result_rows"][0]
    assert row["cluster_count"] == 2
    assert row["sortino"] is None
    assert row["insufficient_for_ranking"] is True
    assert row["below_support_floor"] is True
    assert report["recommended_for_promotion"] == []
    assert report["report_metadata"]["warnings"][0]["type"] == "insufficient_cluster_coverage"


@pytest.mark.asyncio
async def test_env_selection_uses_current_build_and_writes_timestamped_outputs(tmp_path: Path) -> None:
    harness = await _setup(tmp_path, promote_floor=3, min_clusters=3)
    await _seed_strategies(harness.session_factory, [("loose", 0)])
    build_id = await _create_build(
        harness.session_factory,
        _rows_for_pnls([Decimal("1.000000"), Decimal("-0.500000"), Decimal("1.500000")]),
        promote_env="demo",
    )

    result = await harness.service.rank_report(
        kalshi_env="demo",
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 11, 2, tzinfo=UTC),
    )

    assert result["exit_code"] == 0
    assert result["build_id"] == build_id
    assert result["selection_mode"] == "current"
    assert Path(result["json_path"]).name == f"strategy_ranking_{build_id}_20260424T110200Z.json"


def test_rank_rows_orders_by_sortino_not_display_win_rate() -> None:
    ranked = _rank_rows(
        [
            {
                "strategy_name": "high_win_low_sortino",
                "series_ticker": "KX",
                "sortino": 0.2,
                "cluster_count": 40,
                "total_net_pnl_dollars": 1.0,
                "win_rate": 0.95,
            },
            {
                "strategy_name": "low_win_high_sortino",
                "series_ticker": "KX",
                "sortino": 1.5,
                "cluster_count": 40,
                "total_net_pnl_dollars": 4.0,
                "win_rate": 0.35,
            },
        ]
    )

    assert ranked[0]["strategy_name"] == "low_win_high_sortino"


def test_win_rate_is_display_only_not_rank_key_logic() -> None:
    rank_key_source = inspect.getsource(ranking_module._rank_key)

    assert "win_rate" not in rank_key_source
