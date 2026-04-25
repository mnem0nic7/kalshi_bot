from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    FillRecord,
    HistoricalMarketSnapshotRecord,
    HistoricalSettlementLabelRecord,
    HistoricalWeatherSnapshotRecord,
    MarketPriceHistory,
    OrderRecord,
    RiskVerdictRecord,
    Room,
    Signal,
    TradeTicketRecord,
)
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.trade_analysis import TradeAnalysisService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping


NOW = datetime(2026, 4, 24, 15, 0, tzinfo=UTC)
TICKER = "KXHIGHNY-26APR24-T67"


@pytest.fixture
async def analysis_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/trade-analysis.db",
        risk_stale_market_seconds=120,
        risk_stale_weather_seconds=600,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    directory = WeatherMarketDirectory(
        {
            TICKER: WeatherMarketMapping(
                market_ticker=TICKER,
                station_id="KNYC",
                location_name="New York",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=67.0,
                series_ticker="KXHIGHNY",
            )
        }
    )
    yield settings, session_factory, directory
    await engine.dispose()


def _room(room_id: str, *, ticker: str = TICKER, created_at: datetime | None = None) -> Room:
    ts = created_at or NOW - timedelta(minutes=30)
    return Room(
        id=room_id,
        name=room_id,
        market_ticker=ticker,
        kalshi_env="production",
        shadow_mode=False,
        agent_pack_version="pack-v1",
        created_at=ts,
        updated_at=ts,
    )


def _signal(room_id: str, *, created_at: datetime | None = None, edge_bps: int = 900) -> Signal:
    ts = created_at or NOW - timedelta(minutes=25)
    return Signal(
        id=f"sig-{room_id}",
        room_id=room_id,
        market_ticker=TICKER,
        fair_yes_dollars=Decimal("0.7000"),
        edge_bps=edge_bps,
        confidence=0.82,
        summary="edge",
        payload={
            "trade_regime": "standard",
            "eligibility": {"market_spread_bps": 200, "remaining_payout_dollars": "0.6000"},
            "trade_selection": {"evaluation_outcome": "candidate_selected"},
        },
        created_at=ts,
        updated_at=ts,
    )


def _ticket(room_id: str, *, status: str = "approved", strategy_code: str | None = "A") -> TradeTicketRecord:
    return TradeTicketRecord(
        id=f"ticket-{room_id}",
        room_id=room_id,
        market_ticker=TICKER,
        action="buy",
        side="yes",
        yes_price_dollars=Decimal("0.5000"),
        count_fp=Decimal("10.00"),
        time_in_force="immediate_or_cancel",
        client_order_id=f"coid-{room_id}",
        status=status,
        strategy_code=strategy_code,
        created_at=NOW - timedelta(minutes=20),
        updated_at=NOW - timedelta(minutes=20),
    )


def _risk(ticket_id: str, room_id: str, *, status: str = "approved") -> RiskVerdictRecord:
    return RiskVerdictRecord(
        id=f"risk-{room_id}",
        room_id=room_id,
        ticket_id=ticket_id,
        status=status,
        reasons=["ok" if status == "approved" else "blocked"],
        approved_notional_dollars=Decimal("5.0000") if status == "approved" else None,
        approved_count_fp=Decimal("10.00") if status == "approved" else None,
        payload={},
        created_at=NOW - timedelta(minutes=19),
        updated_at=NOW - timedelta(minutes=19),
    )


def _snapshots() -> list[object]:
    return [
        MarketPriceHistory(
            kalshi_env="production",
            market_ticker=TICKER,
            yes_bid_dollars=Decimal("0.4800"),
            yes_ask_dollars=Decimal("0.5200"),
            mid_dollars=Decimal("0.5000"),
            last_trade_dollars=Decimal("0.5100"),
            volume=100,
            observed_at=NOW - timedelta(minutes=22),
        ),
        # Future snapshot must never be selected for the decision.
        MarketPriceHistory(
            kalshi_env="production",
            market_ticker=TICKER,
            yes_bid_dollars=Decimal("0.9000"),
            yes_ask_dollars=Decimal("0.9500"),
            mid_dollars=Decimal("0.9250"),
            last_trade_dollars=Decimal("0.9400"),
            volume=999,
            observed_at=NOW - timedelta(minutes=5),
        ),
        HistoricalWeatherSnapshotRecord(
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            local_market_day="26APR24",
            asof_ts=NOW - timedelta(minutes=23),
            source_kind="test",
            source_id="weather-before",
            forecast_updated_ts=NOW - timedelta(minutes=23),
            forecast_high_f=Decimal("70.00"),
            current_temp_f=Decimal("65.00"),
            payload={},
        ),
        HistoricalWeatherSnapshotRecord(
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            local_market_day="26APR24",
            asof_ts=NOW - timedelta(minutes=5),
            source_kind="test",
            source_id="weather-after",
            forecast_updated_ts=NOW - timedelta(minutes=5),
            forecast_high_f=Decimal("90.00"),
            current_temp_f=Decimal("85.00"),
            payload={},
        ),
        HistoricalSettlementLabelRecord(
            market_ticker=TICKER,
            series_ticker="KXHIGHNY",
            local_market_day="26APR24",
            kalshi_result="yes",
            settlement_value_dollars=Decimal("1.0000"),
            settlement_ts=NOW,
            crosscheck_status="matched",
            payload={},
        ),
    ]


@pytest.mark.asyncio
async def test_trade_analysis_builds_asof_decision_rows_without_leakage(analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    async with session_factory() as session:
        room = _room("room-filled")
        ticket = _ticket(room.id)
        order = OrderRecord(
            id="order-filled",
            trade_ticket_id=ticket.id,
            kalshi_env="production",
            kalshi_order_id="kord-filled",
            client_order_id=ticket.client_order_id,
            market_ticker=TICKER,
            status="filled",
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("10.00"),
            strategy_code="A",
            raw={},
            created_at=NOW - timedelta(minutes=18),
            updated_at=NOW - timedelta(minutes=18),
        )
        fill = FillRecord(
            order_id=order.id,
            kalshi_env="production",
            trade_id="trade-filled",
            market_ticker=TICKER,
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.5100"),
            count_fp=Decimal("10.00"),
            strategy_code="A",
            raw={"fee_cost": "0.0200"},
            created_at=NOW - timedelta(minutes=17),
            updated_at=NOW - timedelta(minutes=17),
        )
        session.add_all([room, _signal(room.id), ticket, _risk(ticket.id, room.id), order, fill, *_snapshots()])
        await session.commit()

    dataset = await TradeAnalysisService(settings, session_factory, directory).build_dataset(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    assert dataset.summary["row_count"] == 1
    row = dataset.rows[0]
    assert row["decision_status"] == "filled"
    assert row["training_eligible"] is True
    assert row["label_win"] is True
    assert row["yes_bid_dollars"] == "0.4800"
    assert row["forecast_high_f"] == "70.00"
    assert row["gross_pnl_dollars"] == "4.9000"
    assert row["market_snapshot_source"] == "market_price_history"
    assert row["market_stale_seconds"] == 120.0
    assert row["market_stale_threshold_seconds"] == 120.0
    assert row["market_stale_overage_seconds"] is None
    assert row["market_snapshot_age_bucket"] == "61-300s"


@pytest.mark.asyncio
async def test_trade_analysis_keeps_excluded_rows_with_reasons(analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    async with session_factory() as session:
        room = _room("room-rejected")
        ticket = _ticket(room.id, status="proposed", strategy_code=None)
        session.add_all([
            room,
            _signal(room.id),
            ticket,
            _risk(ticket.id, room.id, status="blocked"),
        ])
        await session.commit()

    dataset = await TradeAnalysisService(settings, session_factory, directory).build_dataset(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    assert dataset.summary["row_count"] == 1
    row = dataset.rows[0]
    assert row["decision_status"] == "risk_blocked"
    assert row["training_eligible"] is False
    assert "missing_market_snapshot" in row["exclusion_reasons"]
    assert "missing_weather_snapshot" in row["exclusion_reasons"]
    assert "missing_settlement_label" in row["exclusion_reasons"]
    assert "missing_strategy_attribution" in row["exclusion_reasons"]

    report = await TradeAnalysisService(settings, session_factory, directory).build_report(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    assert report["top_exclusion_reasons_by_series"][0] == {
        "series_ticker": "KXHIGHNY",
        "reason": "missing_market_snapshot",
        "rows": 1,
    }


@pytest.mark.asyncio
async def test_trade_analysis_historical_market_fallback_requires_fresh_point_in_time_snapshot(analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    async with session_factory() as session:
        room = _room("room-historical-fresh")
        ticket = _ticket(room.id)
        session.add_all(
            [
                room,
                _signal(room.id),
                ticket,
                _risk(ticket.id, room.id),
                HistoricalMarketSnapshotRecord(
                    market_ticker=TICKER,
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="26APR24",
                    asof_ts=NOW - timedelta(minutes=200),
                    source_kind="captured_market_snapshot",
                    source_id="stale-captured",
                    yes_bid_dollars=Decimal("0.1000"),
                    yes_ask_dollars=Decimal("0.2000"),
                    payload={},
                ),
                HistoricalMarketSnapshotRecord(
                    market_ticker=TICKER,
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="26APR24",
                    asof_ts=NOW - timedelta(minutes=19),
                    source_kind="kalshi_final_market",
                    source_id="final-market",
                    yes_bid_dollars=Decimal("0.9000"),
                    yes_ask_dollars=Decimal("0.9500"),
                    payload={},
                ),
                HistoricalMarketSnapshotRecord(
                    market_ticker=TICKER,
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="26APR24",
                    asof_ts=NOW - timedelta(minutes=21),
                    source_kind="captured_market_snapshot",
                    source_id="fresh-captured",
                    yes_bid_dollars=Decimal("0.4400"),
                    yes_ask_dollars=Decimal("0.4600"),
                    payload={},
                ),
                *[item for item in _snapshots() if not isinstance(item, MarketPriceHistory)],
            ]
        )
        await session.commit()

    dataset = await TradeAnalysisService(settings, session_factory, directory).build_dataset(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    row = dataset.rows[0]
    assert row["training_eligible"] is True
    assert row["market_snapshot_source"] == "historical_market_snapshots"
    assert row["market_snapshot_source_kind"] == "captured_market_snapshot"
    assert row["market_snapshot_source_id"] == "fresh-captured"
    assert row["yes_bid_dollars"] == "0.4400"


@pytest.mark.asyncio
async def test_trade_analysis_historical_market_fallback_ignores_stale_and_final_snapshots(analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    async with session_factory() as session:
        room = _room("room-historical-stale")
        ticket = _ticket(room.id)
        session.add_all(
            [
                room,
                _signal(room.id),
                ticket,
                _risk(ticket.id, room.id),
                HistoricalMarketSnapshotRecord(
                    market_ticker=TICKER,
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="26APR24",
                    asof_ts=NOW - timedelta(minutes=200),
                    source_kind="captured_market_snapshot",
                    source_id="stale-captured",
                    yes_bid_dollars=Decimal("0.1000"),
                    yes_ask_dollars=Decimal("0.2000"),
                    payload={},
                ),
                HistoricalMarketSnapshotRecord(
                    market_ticker=TICKER,
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="26APR24",
                    asof_ts=NOW - timedelta(minutes=19),
                    source_kind="kalshi_final_market",
                    source_id="final-market",
                    yes_bid_dollars=Decimal("0.9000"),
                    yes_ask_dollars=Decimal("0.9500"),
                    payload={},
                ),
                *[item for item in _snapshots() if not isinstance(item, MarketPriceHistory)],
            ]
        )
        await session.commit()

    dataset = await TradeAnalysisService(settings, session_factory, directory).build_dataset(
        kalshi_env="production",
        days=7,
        now=NOW,
    )

    row = dataset.rows[0]
    assert row["training_eligible"] is False
    assert row["market_snapshot_source"] is None
    assert "missing_market_snapshot" in row["exclusion_reasons"]


@pytest.mark.asyncio
async def test_trade_analysis_is_non_mutating(analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    async with session_factory() as session:
        room = _room("room-safe")
        session.add_all([room, _signal(room.id)])
        await session.commit()

    async def counts() -> tuple[int, int]:
        async with session_factory() as session:
            room_count = len(list((await session.execute(select(Room))).scalars()))
            fill_count = len(list((await session.execute(select(FillRecord))).scalars()))
            return room_count, fill_count

    before = await counts()
    await TradeAnalysisService(settings, session_factory, directory).build_report(kalshi_env="production", days=7, now=NOW)
    after = await counts()

    assert after == before


@pytest.mark.asyncio
async def test_trade_analysis_dataset_write_and_model_eval(tmp_path, analysis_harness) -> None:
    settings, session_factory, directory = analysis_harness
    service = TradeAnalysisService(settings, session_factory, directory)
    path = tmp_path / "dataset.jsonl"
    rows = []
    base = NOW - timedelta(days=30)
    for idx in range(24):
        label = idx % 3 != 0
        rows.append({
            "schema_version": "trade-analysis-v1",
            "kalshi_env": "production",
            "room_id": f"r{idx}",
            "market_ticker": TICKER,
            "decision_ts": (base + timedelta(days=idx)).isoformat(),
            "edge_bps": 1000 + idx,
            "confidence": 0.60 + (0.01 * idx),
            "ticket_yes_price_dollars": "0.5000",
            "spread_dollars": "0.0400",
            "market_stale_seconds": 30,
            "weather_stale_seconds": 300,
            "forecast_residual_f": None,
            "label_win": label,
            "training_eligible": True,
            "gross_pnl_dollars": "1.0000" if label else "-1.0000",
            "exclusion_reasons": [],
        })
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    result = await service.model_eval(dataset_path=path, now=NOW)

    assert result["read_only"] is True
    assert result["dataset"]["rows"] == 24
    assert result["dataset"]["eligible_rows"] == 24
    assert result["feature_diagnostics"]["model_eligible_rows"]["forecast_residual_f"]["missing_count"] == 24
    assert result["feature_diagnostics"]["train_rows"]["forecast_residual_f"]["missing_count"] == 16
    assert result["feature_diagnostics"]["test_rows"]["forecast_residual_f"]["missing_count"] == 8
    assert result["feature_diagnostics"]["test_rows"]["forecast_residual_f"]["imputation_value"] == 0.0
    assert result["metrics"]["status"] == "ok"
    assert result["metrics"]["train_window"]["end"] < result["metrics"]["test_window"]["start"]
    assert "picked_trade_diagnostics" in result["metrics"]
    assert "worst_picked_rows" in result["metrics"]["picked_trade_diagnostics"]

    output = tmp_path / "written.jsonl"
    write_result = await service.write_dataset(output=output, kalshi_env="production", days=7, now=NOW)
    assert Path(write_result["output"]).exists()
    assert write_result["schema_version"] == "trade-analysis-v1"
