from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.decision_corpus_calibration import DecisionCorpusCalibrationReportService


async def _setup(tmp_path: Path) -> SimpleNamespace:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'decision_corpus_calibration.db'}",
        kalshi_taker_fee_rate=0.07,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    return SimpleNamespace(
        settings=settings,
        session_factory=session_factory,
        service=DecisionCorpusCalibrationReportService(settings, session_factory),
    )


async def _create_build(
    session_factory,
    specs: list[dict],
    *,
    version: str = "calibration-fixture",
    promote_env: str | None = None,
) -> str:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        build = await repo.create_decision_corpus_build(
            version=version,
            date_from=date(2026, 4, 20),
            date_to=date(2026, 4, 24),
            source={"type": "fixture"},
            filters={"fixture": True},
            git_sha="fixture-sha",
        )
        for index, spec in enumerate(specs):
            room = await repo.create_room(
                RoomCreate(name=f"Calibration fixture {index}", market_ticker=f"KXFIXTURE-{index}"),
                active_color=DeploymentColor.BLUE.value,
                shadow_mode=True,
                kill_switch_enabled=False,
                kalshi_env=spec.get("kalshi_env", "demo"),
                room_origin="historical_replay",
                agent_pack_version="model-v1",
            )
            checkpoint_ts = spec.get("checkpoint_ts") or datetime(2026, 4, 20, 17, 0, tzinfo=UTC) + timedelta(minutes=index)
            local_market_day = spec.get("local_market_day") or checkpoint_ts.date().isoformat()
            source_provenance = spec.get("source_provenance", "historical_replay_full_checkpoint")
            await repo.add_decision_corpus_row(
                corpus_build_id=build.id,
                room_id=room.id,
                market_ticker=spec.get("market_ticker", f"KXFIXTURE-{index}"),
                series_ticker=spec.get("series_ticker", "KXFIXTURE"),
                station_id=spec.get("station_id", "KNYC"),
                local_market_day=local_market_day,
                checkpoint_ts=checkpoint_ts,
                kalshi_env=spec.get("kalshi_env", "demo"),
                deployment_color="blue",
                model_version=spec.get("model_version", "model-v1"),
                policy_version=spec.get("policy_version", "policy-v1"),
                source_asof_ts=spec.get("source_asof_ts", checkpoint_ts),
                quote_observed_at=checkpoint_ts,
                quote_captured_at=checkpoint_ts,
                time_to_settlement_at_checkpoint_minutes=spec.get("lead_minutes", 360),
                fair_yes_dollars=spec.get("fair_yes_dollars", Decimal("0.6000")),
                recommended_side=spec.get("recommended_side", "yes"),
                target_yes_price_dollars=spec.get("target_yes_price_dollars", Decimal("0.5500")),
                eligibility_status=spec.get("eligibility_status", "eligible"),
                stand_down_reason=spec.get("stand_down_reason"),
                trade_regime=spec.get("trade_regime", "standard"),
                liquidity_regime=spec.get("liquidity_regime", "normal"),
                support_status=spec.get("support_status", "supported"),
                support_level=spec.get("support_level", "L1_station_season_lead_regime"),
                support_n=spec.get("support_n", 150),
                support_market_days=spec.get("support_market_days", 22),
                support_recency_days=spec.get("support_recency_days", 10),
                backoff_path=spec.get(
                    "backoff_path",
                    [
                        {
                            "level": spec.get("support_level", "L1_station_season_lead_regime"),
                            "n": spec.get("support_n", 150),
                            "market_days": spec.get("support_market_days", 22),
                            "status": spec.get("support_status", "supported"),
                            "failed_on": [],
                        }
                    ],
                ),
                settlement_result=spec.get("settlement_result", "yes"),
                settlement_value_dollars=spec.get("settlement_value_dollars", Decimal("1.0000")),
                pnl_model_fair_frictionless=Decimal("0.400000"),
                source_provenance=source_provenance,
                source_details=spec.get("source_details", {"checkpoint_label": "checkpoint_1"}),
                signal_payload={},
                quote_snapshot={},
                settlement_payload={},
                diagnostics=spec.get(
                    "diagnostics",
                    {
                        "season_bucket": "spring",
                        "lead_bucket": "near",
                        "checkpoint_label": spec.get("source_details", {}).get("checkpoint_label", "checkpoint_1"),
                    },
                ),
            )
        await repo.mark_decision_corpus_build_successful(build.id, row_count=len(specs))
        if promote_env is not None:
            await repo.promote_decision_corpus_build(build.id, kalshi_env=promote_env, actor="fixture")
        await session.commit()
        return build.id


def _clean_specs(count: int, *, days: int = 3, **overrides) -> list[dict]:
    specs = []
    for index in range(count):
        local_day = date(2026, 4, 20) + timedelta(days=index % days)
        specs.append(
            {
                "market_ticker": f"KXCLEAN-{index}",
                "local_market_day": local_day.isoformat(),
                "checkpoint_ts": datetime(local_day.year, local_day.month, local_day.day, 17, 0, tzinfo=UTC),
                "fair_yes_dollars": Decimal("0.6000"),
                "settlement_result": "yes" if index % 2 == 0 else "no",
                **overrides,
            }
        )
    return specs


@pytest.mark.asyncio
async def test_calibration_report_warns_on_sparse_clean_primary_and_counts_degraded_and_skips(tmp_path) -> None:
    harness = await _setup(tmp_path)
    specs = _clean_specs(16, days=3)
    specs[0]["recommended_side"] = None
    specs[0]["stand_down_reason"] = "below_min_edge"
    specs += _clean_specs(
        3,
        days=1,
        market_ticker="placeholder",
        source_provenance="historical_replay_external_forecast_repair",
        source_details={"checkpoint_label": "checkpoint_1", "repair_sources": ["open_meteo_forecast_archive"]},
    )
    specs.append({"market_ticker": "KXSKIP-NULL", "fair_yes_dollars": None, "settlement_result": "yes"})
    specs.append({"market_ticker": "KXSKIP-VOID", "fair_yes_dollars": Decimal("0.5000"), "settlement_result": "void"})
    build_id = await _create_build(harness.session_factory, specs)

    result = await harness.service.calibration_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 30, tzinfo=UTC),
    )

    assert result["exit_code"] == 1
    report = json.loads(Path(result["json_path"]).read_text())
    assert report["report_metadata"]["warnings"][0]["type"] == "insufficient_primary_clean_coverage"
    assert report["aggregates"]["primary"]["n"] == 16
    assert report["aggregates"]["degraded_provenance"]["n"] == 3
    assert report["aggregates"]["descriptive"]["n"] == 19
    assert report["coverage"]["row_counts"]["stand_down_valid_rows"] == 1
    assert {"reason": "null_fair_yes_dollars", "rows": 1} in report["coverage"]["skips"]
    assert {"reason": "non_binary_settlement:void", "rows": 1} in report["coverage"]["skips"]
    markdown = Path(result["markdown_path"]).read_text()
    assert "WARNING: Insufficient Primary Clean Coverage" in markdown
    assert "Full per-cell detail is in" in markdown
    assert "`cells[]`" in markdown


@pytest.mark.asyncio
async def test_calibration_rows_are_partitioned_not_overlapping(tmp_path) -> None:
    harness = await _setup(tmp_path)
    specs = []
    specs += _clean_specs(12, support_level="L1_station_season_lead_regime", support_status="supported", support_n=150)
    specs += _clean_specs(
        11,
        support_level="L2_station_season_lead",
        support_status="supported",
        support_n=140,
        diagnostics={"season_bucket": "spring", "lead_bucket": "near", "checkpoint_label": "checkpoint_1"},
    )
    specs += _clean_specs(
        10,
        support_level="L5_global",
        support_status="exploratory",
        support_n=35,
        support_market_days=11,
    )
    build_id = await _create_build(harness.session_factory, specs)

    result = await harness.service.calibration_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 31, tzinfo=UTC),
    )

    report = json.loads(Path(result["json_path"]).read_text())
    assert sum(cell["cell_n"] for cell in report["cells"]) == report["aggregates"]["primary"]["n"]
    assert report["aggregates"]["primary"]["n"] == 33


@pytest.mark.asyncio
async def test_env_selection_uses_current_build_and_writes_timestamped_outputs(tmp_path) -> None:
    harness = await _setup(tmp_path)
    build_id = await _create_build(
        harness.session_factory,
        _clean_specs(30, days=10, support_status="exploratory", support_n=30, support_market_days=10),
        promote_env="demo",
    )

    result = await harness.service.calibration_report(
        kalshi_env="demo",
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 32, tzinfo=UTC),
    )

    assert result["exit_code"] == 0
    assert result["build_id"] == build_id
    assert result["selection_mode"] == "current"
    assert Path(result["json_path"]).name == f"calibration_{build_id}_20260424T103200Z.json"
    report = json.loads(Path(result["json_path"]).read_text())
    assert report["report_metadata"]["selection"]["kalshi_env"] == "demo"
    assert report["report_metadata"]["primary_coverage_status"] == "exploratory"


@pytest.mark.asyncio
async def test_primary_metrics_are_omitted_below_absolute_minimum(tmp_path) -> None:
    harness = await _setup(tmp_path)
    build_id = await _create_build(harness.session_factory, _clean_specs(5, days=1))

    result = await harness.service.calibration_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 33, tzinfo=UTC),
    )

    assert result["exit_code"] == 2
    report = json.loads(Path(result["json_path"]).read_text())
    assert report["aggregates"]["primary"]["metrics_omitted"] is True
    assert report["aggregates"]["primary"]["brier"] is None
    assert report["report_metadata"]["warnings"][0]["type"] == "primary_clean_coverage_below_absolute_minimum"


@pytest.mark.asyncio
async def test_reliability_curve_uses_all_valid_rows_while_cells_use_primary_clean_rows(tmp_path) -> None:
    harness = await _setup(tmp_path)
    specs = _clean_specs(30, days=10, support_status="exploratory", support_n=30, support_market_days=10)
    specs += _clean_specs(
        5,
        days=2,
        source_provenance="historical_replay_external_forecast_repair",
        source_details={"checkpoint_label": "checkpoint_1", "repair_sources": ["open_meteo_forecast_archive"]},
    )
    build_id = await _create_build(harness.session_factory, specs)

    result = await harness.service.calibration_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 34, tzinfo=UTC),
    )

    report = json.loads(Path(result["json_path"]).read_text())
    assert sum(bucket["n"] for bucket in report["reliability_curve"]) == 35
    assert sum(cell["cell_n"] for cell in report["cells"]) == 30


@pytest.mark.asyncio
async def test_late_only_asof_violation_is_excluded_from_calibration(tmp_path) -> None:
    harness = await _setup(tmp_path)
    checkpoint_ts = datetime(2026, 4, 20, 22, 0, tzinfo=UTC)
    specs = [
        {
            "market_ticker": "KXLATE-CLEAN",
            "checkpoint_ts": checkpoint_ts,
            "local_market_day": "2026-04-20",
            "source_provenance": "historical_replay_late_only",
            "source_details": {"checkpoint_label": "checkpoint_3"},
            "diagnostics": {"season_bucket": "spring", "lead_bucket": "imminent", "checkpoint_label": "checkpoint_3"},
        },
        {
            "market_ticker": "KXLATE-BAD",
            "checkpoint_ts": checkpoint_ts,
            "local_market_day": "2026-04-20",
            "source_asof_ts": checkpoint_ts + timedelta(hours=1),
            "source_provenance": "historical_replay_late_only",
            "source_details": {"checkpoint_label": "checkpoint_3"},
            "diagnostics": {"season_bucket": "spring", "lead_bucket": "imminent", "checkpoint_label": "checkpoint_3"},
        },
    ]
    build_id = await _create_build(harness.session_factory, specs)

    result = await harness.service.calibration_report(
        build_id=build_id,
        output=tmp_path / "reports",
        generated_at=datetime(2026, 4, 24, 10, 35, tzinfo=UTC),
    )

    report = json.loads(Path(result["json_path"]).read_text())
    assert report["coverage"]["row_counts"]["contaminated_clean_rows"] == 1
    assert report["coverage"]["row_counts"]["valid_prediction_and_binary_outcome"] == 1
    assert {"reason": "clean_provenance_asof_violation", "rows": 1} in report["coverage"]["skips"]


def test_calibration_report_does_not_import_pr1_backoff_logic() -> None:
    source = Path("src/kalshi_bot/services/decision_corpus_calibration.py").read_text()

    assert "from kalshi_bot.services.decision_corpus import" not in source
    assert "import kalshi_bot.services.decision_corpus" not in source
