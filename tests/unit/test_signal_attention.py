from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.decision_policy_variants import DecisionPolicyVariantService
from kalshi_bot.services.signal_attention import (
    SignalAttentionService,
    attention_rows_to_csv,
    extract_decision_fields,
)


NOW = datetime(2026, 5, 10, 17, 0, tzinfo=UTC)


def _row(
    market: str,
    idx: int,
    *,
    edge: int,
    primary: str | None = None,
    qa_edge: int | None = None,
    fair: str = "0.5000",
    bucket: str | None = None,
) -> dict:
    return {
        "room_id": f"{market}-room-{idx}",
        "market_ticker": market,
        "updated_at": (NOW + timedelta(minutes=idx * 10)).isoformat(),
        "edge_bps": edge,
        "quality_adjusted_edge_bps": qa_edge,
        "fair_yes_adjusted": fair,
        "fair_yes_initial": "0.5000",
        "primary_block_reason": primary,
        "bucket_id": bucket,
        "bucket_sample_count": 2,
        "bucket_min_required": 20,
        "forecast_threshold_delta_f": 2.0,
        "high_so_far_f": 71.0,
    }


def test_extract_decision_fields_surfaces_structured_gate_quote_bucket_and_weather_values() -> None:
    payload = {
        "candidate_trace": {
            "selected_side": "no",
            "min_contract_price_dollars": "0.2500",
            "quality_buffer_bps": 25,
            "policy_variant_applied": "low_price_high_edge",
            "baseline_block_reason": "below_min_contract_price",
            "policy_variants": {
                "low_price_high_edge": {
                    "matched": True,
                    "applied": True,
                    "would_have_entered": True,
                }
            },
            "candidates": [
                {
                    "side": "yes",
                    "status": "skipped",
                    "reason": "below_min_contract_price",
                    "traded_price_dollars": "0.0500",
                    "edge_bps": 1800,
                    "quality_adjusted_edge_bps": 1775,
                },
                {
                    "side": "no",
                    "status": "selected",
                    "reason": "selected_best_quality_adjusted_edge",
                    "traded_price_dollars": "0.6200",
                    "target_yes_price_dollars": "0.3800",
                    "edge_bps": 3373,
                    "quality_adjusted_edge_bps": 3348,
                },
            ],
        },
        "eligibility": {
            "stand_down_reason": "empirical_gate_block",
            "edge_after_quality_buffer_bps": 3348,
        },
        "empirical_gate": {
            "reason": "empirical_gate_under_sampled",
            "bucket_key": "KXHIGHTOKC|TOKC|no|A|60-69c|delta:<=-5f|conf:high|spread:500bps+",
            "actual_sample_count": 7,
        },
        "trader_context": {
            "numeric_facts": {
                "forecast_high_f": 84.0,
                "threshold_f": 74.0,
                "forecast_delta_f": -10.0,
                "observed_high_so_far_f": 70.0,
                "operator": "<",
            }
        },
        "market_snapshot": {
            "market": {
                "yes_bid_dollars": "0.3700",
                "yes_ask_dollars": "0.4000",
                "close_time": "2026-05-10T23:00:00+00:00",
            }
        },
        "prediction_provenance": {
            "baseline_fair_yes": "0.2500",
            "intraday_model": {"status": "used", "baseline_fair_yes": "0.2500", "intraday_fair_yes": "0.0951"},
        },
    }

    fields = extract_decision_fields(
        payload,
        settings=Settings(database_url="sqlite+aiosqlite:///./test.db"),
        room_id="room-okc",
        market_ticker="KXHIGHTOKC-26MAY11-T74",
        updated_at=NOW,
        row_fair_yes="0.0951",
        row_edge_bps=3373,
    )

    assert fields["primary_block_reason"] == "empirical_gate_under_sampled"
    assert "empirical_gate_under_sampled" in fields["gates_failed"]
    assert fields["policy_variant_applied"] == "low_price_high_edge"
    assert fields["baseline_block_reason"] == "below_min_contract_price"
    assert fields["policy_variants"]["low_price_high_edge"]["applied"] is True
    assert fields["forecast_high_f"] == 84.0
    assert fields["contract_direction"] == "below"
    assert fields["yes_bid"] == "0.3700"
    assert fields["no_ask"] == "0.6300"
    assert fields["entry_price"] == "0.6200"
    assert fields["fair_yes_initial"] == "0.2500"
    assert fields["fair_yes_adjusted"] == "0.0951"
    assert fields["bucket_sample_count"] == 7
    assert fields["bucket_min_required"] == 20


def test_signal_attention_detects_all_configured_patterns() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = SignalAttentionService(settings)
    rows = [
        _row("KXHIGHTOKC-26MAY11-T67", 1, edge=1673, primary="empirical_gate_under_sampled", bucket="okc-bucket"),
        _row("KXHIGHTOKC-26MAY11-T67", 2, edge=2300, primary="empirical_gate_under_sampled", bucket="okc-bucket"),
        _row("KXHIGHTOKC-26MAY11-T67", 3, edge=3373, primary="empirical_gate_under_sampled", bucket="okc-bucket"),
        _row("KXHIGHNOLA-26MAY11-T79", 1, edge=902, fair="0.4200"),
        _row("KXHIGHNOLA-26MAY11-T79", 2, edge=902, fair="0.4300"),
        _row("KXHIGHNOLA-26MAY11-T79", 3, edge=902, fair="0.4400"),
        _row("KXHIGHLAX-26MAY10-T72", 1, edge=5549, primary="forecast_too_close_to_threshold", fair="0.0951"),
        *[_row("KXHIGHBOS-26MAY11-T61", idx, edge=700 + idx, fair="0.1234") for idx in range(1, 6)],
        _row("KXHIGHSEA-26MAY11-T67", 1, edge=520, primary="min_edge", qa_edge=430),
        _row("KXHIGHSEA-26MAY11-T67", 2, edge=540, primary="min_edge", qa_edge=450),
        _row("KXHIGHSEA-26MAY11-T67", 3, edge=590, primary="min_edge", qa_edge=499),
    ]

    patterns = service.detect_patterns(rows)
    found = {(item["pattern"], item["market"]) for item in patterns}

    assert ("cold_start_trapped", "KXHIGHTOKC-26MAY11-T67") in found
    assert ("edge_growing", "KXHIGHTOKC-26MAY11-T67") in found
    assert ("static_edge", "KXHIGHNOLA-26MAY11-T79") in found
    assert ("intraday_resolved_separation_block", "KXHIGHLAX-26MAY10-T72") in found
    assert ("static_fair_value", "KXHIGHBOS-26MAY11-T61") in found
    assert ("recurring_near_miss_edge_floor", "KXHIGHSEA-26MAY11-T67") in found

    csv_text = attention_rows_to_csv(patterns)
    assert "pattern,market,n_evaluations" in csv_text
    assert "KXHIGHTOKC-26MAY11-T67-room-1" in csv_text


def test_empirical_bootstrap_override_requires_cold_start_pattern() -> None:
    service = SignalAttentionService(Settings(database_url="sqlite+aiosqlite:///./test.db"))

    rows = [
        _row("KXHIGHTDAL-26MAY11-T75", 1, edge=1600, primary="empirical_gate_under_sampled"),
        _row("KXHIGHTDAL-26MAY11-T75", 2, edge=2100, primary="empirical_gate_under_sampled"),
        _row("KXHIGHTDAL-26MAY11-T75", 3, edge=2600, primary="empirical_gate_under_sampled"),
    ]

    override = service.empirical_bootstrap_override(rows)

    assert override is not None
    assert override["pattern"] == "cold_start_trapped"
    assert override["last_edge_bps"] == 2600


def test_empirical_bootstrap_policy_respects_live_flag() -> None:
    service = SignalAttentionService(Settings(database_url="sqlite+aiosqlite:///./test.db"))
    override = service.empirical_bootstrap_override(
        [
            _row("KXHIGHTDAL-26MAY11-T75", 1, edge=1600, primary="empirical_gate_under_sampled"),
            _row("KXHIGHTDAL-26MAY11-T75", 2, edge=2100, primary="empirical_gate_under_sampled"),
            _row("KXHIGHTDAL-26MAY11-T75", 3, edge=2600, primary="empirical_gate_under_sampled"),
        ]
    )
    policy = DecisionPolicyVariantService(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            empirical_bootstrap_live_enabled=False,
            decision_policy_variants_shadow_enabled=True,
        )
    ).empirical_bootstrap_decision(override)

    assert policy is not None
    trace = policy.to_trace()
    assert trace["matched"] is True
    assert trace["live_enabled"] is False
    assert trace["would_have_entered"] is True


def test_static_signal_guard_requires_latest_consecutive_static_run() -> None:
    service = SignalAttentionService(Settings(database_url="sqlite+aiosqlite:///./test.db"))
    rows = [
        _row("KXHIGHNOLA-26MAY11-T79", 1, edge=902, fair="0.4200"),
        _row("KXHIGHNOLA-26MAY11-T79", 2, edge=902, fair="0.4200"),
        _row("KXHIGHNOLA-26MAY11-T79", 3, edge=902, fair="0.4200"),
        _row("KXHIGHNOLA-26MAY11-T79", 4, edge=1200, fair="0.5100"),
    ]

    assert service.static_signal_guard(rows) is None

    rows.append(_row("KXHIGHNOLA-26MAY11-T79", 5, edge=1200, fair="0.5100"))
    rows.append(_row("KXHIGHNOLA-26MAY11-T79", 6, edge=1200, fair="0.5100"))
    assert service.static_signal_guard(rows) is None

    rows[-1]["market_stale"] = True

    guard = service.static_signal_guard(rows)

    assert guard is not None
    assert guard["pattern"] == "static_edge"
    assert guard["room_ids"] == [
        "KXHIGHNOLA-26MAY11-T79-room-4",
        "KXHIGHNOLA-26MAY11-T79-room-5",
        "KXHIGHNOLA-26MAY11-T79-room-6",
    ]


def _score_row(market: str, idx: int, *, side: str = "yes", confidence: float = 0.94, edge: int = 5200) -> dict:
    return {
        "room_id": f"room-{idx}",
        "market_ticker": market,
        "updated_at": (NOW + timedelta(minutes=idx)).isoformat(),
        "recommended_side": side.upper(),
        "confidence": confidence,
        "quality_adjusted_edge_bps": edge,
        "edge_bps": edge + 250,
        "forecast_threshold_delta_f": 4.0 if confidence >= 0.75 else 2.0,
        "primary_block_reason": "forecast_too_close_to_threshold",
        "gates_failed": ["forecast_too_close_to_threshold"],
        "yes_bid": "0.3000",
        "yes_ask": "0.3500",
        "yes_mid": "0.3250",
        "no_bid": "0.6500",
        "no_ask": "0.7000",
        "no_mid": "0.6750",
        "bucket_id": "score-bucket",
    }


@pytest.mark.asyncio
async def test_rejected_weather_scorer_dedupes_scores_ask_and_persists_evidence(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/score.db",
        weather_rejected_opportunity_min_settled=5,
        weather_rejected_opportunity_min_accuracy=0.60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    markets = [f"KXHIGHNY-26MAY1{idx}-T66" for idx in range(5)]

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        for market in markets:
            await repo.upsert_historical_settlement_label(
                market_ticker=market,
                series_ticker="KXHIGHNY",
                local_market_day="26MAY10",
                source_kind="test",
                kalshi_result="yes",
                settlement_value_dollars=Decimal("1.0000"),
                settlement_ts=NOW,
                crosscheck_status="match",
                crosscheck_high_f=Decimal("70"),
                crosscheck_result="yes",
                payload={},
            )
        await session.commit()

    rows = [_score_row(market, idx) for idx, market in enumerate(markets)]
    rows.append(_score_row(markets[0], 99, edge=9000))

    async def fake_load_rows(*args, **kwargs):
        return rows

    monkeypatch.setattr(SignalAttentionService, "load_rows", fake_load_rows)

    async with session_factory() as session:
        service = SignalAttentionService(settings)
        report = await service.score_rejected_weather_opportunities(
            session,
            kalshi_env="production",
            lookback_hours=168,
            persist_bootstrap_evidence=True,
            dry_run=False,
        )
        await session.commit()

    assert report["candidate_count"] == 5
    assert report["settled_count"] == 5
    assert report["unlock"]["passed"] is True
    assert Decimal(report["ask_net_pnl_dollars"]) > Decimal("0")
    assert report["persistence"]["created"] == 5

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        evidence = await repo.list_weather_bootstrap_historical_evidence(kalshi_env="production", limit=10)
        await session.commit()

    assert len(evidence) == 5
    assert all(row.payload["source"] == "rejected_weather_opportunity_score" for row in evidence)

    async with session_factory() as session:
        service = SignalAttentionService(settings)
        second = await service.score_rejected_weather_opportunities(
            session,
            kalshi_env="production",
            lookback_hours=168,
            persist_bootstrap_evidence=True,
            dry_run=False,
        )
        await session.commit()

    assert second["persistence"]["existing"] == 5
    assert second["persistence"]["created"] == 0
    await engine.dispose()


def test_room_creation_backoff_blocks_static_and_releases_on_price_move() -> None:
    service = SignalAttentionService(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            weather_static_signal_backoff_cooldown_seconds=1800,
        )
    )
    rows = [
        {
            **_row("KXHIGHBOS-26MAY11-T61", idx, edge=1200, fair="0.5100"),
            "yes_mid": "0.5000",
            "market_close_time": (NOW + timedelta(hours=6)).isoformat(),
            "time_to_close_minutes": 360,
        }
        for idx in range(1, 4)
    ]

    backoff = service.room_creation_backoff(rows, current_mid_dollars=Decimal("0.5000"), now=NOW + timedelta(minutes=31))

    assert backoff is not None
    assert backoff["reason"] == "static_signal_backoff"

    released = service.room_creation_backoff(rows, current_mid_dollars=Decimal("0.5600"), now=NOW + timedelta(minutes=31))

    assert released is None

    edge_released = service.room_creation_backoff(
        rows,
        current_mid_dollars=Decimal("0.5000"),
        current_edge_bps=1800,
        now=NOW + timedelta(minutes=31),
    )

    assert edge_released is None


@pytest.mark.asyncio
async def test_rejected_weather_scorer_persists_evidence_even_when_unlock_fails(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/score_failed_unlock.db",
        weather_rejected_opportunity_min_settled=6,
        weather_rejected_opportunity_min_accuracy=0.60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    markets = [f"KXHIGHNY-26MAY2{idx}-T66" for idx in range(5)]

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        for market in markets:
            await repo.upsert_historical_settlement_label(
                market_ticker=market,
                series_ticker="KXHIGHNY",
                local_market_day="26MAY20",
                source_kind="test",
                kalshi_result="yes",
                settlement_value_dollars=Decimal("1.0000"),
                settlement_ts=NOW,
                crosscheck_status="match",
                crosscheck_high_f=Decimal("70"),
                crosscheck_result="yes",
                payload={},
            )
        await session.commit()

    async def fake_load_rows(*args, **kwargs):
        return [_score_row(market, idx) for idx, market in enumerate(markets)]

    monkeypatch.setattr(SignalAttentionService, "load_rows", fake_load_rows)

    async with session_factory() as session:
        report = await SignalAttentionService(settings).score_rejected_weather_opportunities(
            session,
            kalshi_env="production",
            lookback_hours=168,
            persist_bootstrap_evidence=True,
            dry_run=False,
        )
        await session.commit()

    assert report["unlock"]["passed"] is False
    assert report["persistence"]["created"] == 5

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        evidence = await repo.list_weather_bootstrap_historical_evidence(kalshi_env="production", limit=10)
        await session.commit()

    assert len(evidence) == 5
    await engine.dispose()
