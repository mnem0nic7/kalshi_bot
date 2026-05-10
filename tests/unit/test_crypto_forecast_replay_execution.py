from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, RiskStatus, RoomOrigin, StandDownReason, TradeAction
from kalshi_bot.core.schemas import (
    AgentPackCryptoEntryPolicy,
    AgentPackCryptoPolicy,
    AgentPackCryptoReplayPolicy,
    ExecReceiptPayload,
    RiskVerdictPayload,
    RoomCreate,
    TradeEligibilityVerdict,
    TradeTicket,
)
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import (
    CRYPTO_EXPLORATORY_SHADOW,
    CRYPTO_LIVE_QUALITY,
    CryptoAssetControlService,
    CryptoAutonomyService,
    CryptoHistoryService,
    CryptoWorkflowService,
    CryptoExecutionService,
    CryptoForecastService,
    CryptoReplayService,
    _crypto_data_quality,
    _crypto_decision_rows,
    _crypto_feature_schema,
    _crypto_model_candidate_report,
    _crypto_raw_feature_vector,
    _crypto_trade_candidates,
    _fit_crypto_calibration,
    _predict_crypto_probability,
)
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.db.models import OrderRecord, RiskVerdictRecord, RoomMessage
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.signal import StrategySignal


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto.db",
        "web_auth_enabled": False,
        "risk_min_edge_bps": 50,
        "risk_min_confidence": 0.50,
        "risk_min_probability_extremity_pct": 0.0,
        "crypto_min_training_samples": 2,
        "crypto_replay_min_resolved_markets": 2,
        "crypto_replay_min_trade_candidates": 1,
    }
    values.update(overrides)
    return Settings(**values)


def _market(**overrides) -> CryptoMarket:
    values = {
        "market_ticker": "KXBTC15M-26APR30-B76468",
        "series_ticker": "KXBTC15M",
        "asset_symbol": "BTC",
        "frequency": "15m",
        "target_price_dollars": Decimal("76468.89000000"),
        "yes_bid_dollars": Decimal("0.4700"),
        "yes_ask_dollars": Decimal("0.4900"),
        "no_ask_dollars": Decimal("0.5300"),
        "last_price_dollars": Decimal("0.4800"),
        "volume": 105_941,
        "close_time": datetime.now(UTC) + timedelta(minutes=5),
        "status": "open",
    }
    values.update(overrides)
    return CryptoMarket(**values)


def _signal() -> StrategySignal:
    return StrategySignal(
        fair_yes_dollars=Decimal("0.6500"),
        confidence=0.90,
        edge_bps=1600,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.4900"),
        summary="BTC crypto test signal.",
        eligibility=TradeEligibilityVerdict(eligible=True),
    )


async def _seed_crypto_training_rows(session_factory, settings: Settings, *, days: int = 4) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        for idx in range(days):
            for asset, series in (("BTC", "KXBTC15M"), ("ETH", "KXETH15M")):
                close_time = base + timedelta(days=idx, minutes=15)
                ticker = f"{series}-TEST-{idx}"
                yes_bid = Decimal("0.4200") if idx % 2 == 0 else Decimal("0.5600")
                yes_ask = yes_bid + Decimal("0.0200")
                settlement = "yes" if idx % 2 == 0 else "no"
                await repo.record_crypto_market_snapshot(
                    kalshi_env=settings.kalshi_env,
                    series_ticker=series,
                    market_ticker=ticker,
                    asset_symbol=asset,
                    frequency="15m",
                    status="settled",
                    close_time=close_time,
                    yes_bid_dollars=yes_bid,
                    yes_ask_dollars=yes_ask,
                    no_ask_dollars=Decimal("1.0000") - yes_bid,
                    last_price_dollars=yes_bid,
                    volume=100 + idx,
                    open_interest=50 + idx,
                    settlement_result=settlement,
                    observed_at=close_time - timedelta(minutes=1),
                    source_kind="historical",
                    payload={"unit": True},
                )
                await repo.upsert_crypto_market_candlestick(
                    kalshi_env=settings.kalshi_env,
                    series_ticker=series,
                    market_ticker=ticker,
                    asset_symbol=asset,
                    frequency="15m",
                    period_interval=1,
                    end_period_ts=close_time - timedelta(minutes=1),
                    open_dollars=yes_bid,
                    high_dollars=yes_ask,
                    low_dollars=yes_bid - Decimal("0.0100"),
                    close_dollars=yes_bid + Decimal("0.0100"),
                    volume=20 + idx,
                    payload={"unit": True},
                )
        await session.commit()


@pytest.mark.asyncio
async def test_crypto_history_paginates_historical_markets(tmp_path) -> None:
    settings = _settings(tmp_path)

    class _FakeKalshi:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def list_historical_markets(self, **params):
            self.calls.append(params)
            if "cursor" not in params:
                return {"markets": [{"ticker": "KXBTC15M-FIRST", "series_ticker": "KXBTC15M"}], "cursor": "next"}
            return {"markets": [{"ticker": "KXBTC15M-SECOND", "series_ticker": "KXBTC15M"}]}

    fake = _FakeKalshi()
    result = await CryptoHistoryService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        kalshi=fake,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )._list_historical_markets("KXBTC15M")

    assert result["pages_fetched"] == 2
    assert result["rows_seen"] == 2
    assert fake.calls[0]["series_ticker"] == "KXBTC15M"
    assert fake.calls[1]["cursor"] == "next"


@pytest.mark.asyncio
async def test_crypto_history_captures_candles_with_market_local_window(tmp_path) -> None:
    settings = _settings(tmp_path)
    close_time = datetime.now(UTC) - timedelta(days=30)
    market = _market(
        open_time=close_time - timedelta(minutes=15),
        close_time=close_time,
        status="settled",
        settlement_result="yes",
    )

    class _FakeKalshi:
        def __init__(self) -> None:
            self.historical_calls: list[dict[str, object]] = []
            self.live_calls: list[dict[str, object]] = []

        async def get_historical_market_candlesticks(self, series_ticker: str, market_ticker: str, **params):
            self.historical_calls.append({"series_ticker": series_ticker, "market_ticker": market_ticker, **params})
            return {
                "candlesticks": [
                    {
                        "end_period_ts": (close_time - timedelta(minutes=1)).isoformat(),
                        "open": "0.47",
                        "high": "0.49",
                        "low": "0.46",
                        "close": "0.48",
                        "volume": 12,
                    }
                ]
            }

        async def get_market_candlesticks(self, series_ticker: str, market_ticker: str, **params):
            self.live_calls.append({"series_ticker": series_ticker, "market_ticker": market_ticker, **params})
            return {"candlesticks": []}

    class _FakeRepo:
        def __init__(self) -> None:
            self.candles: list[dict[str, object]] = []

        async def list_crypto_market_candlesticks(self, **kwargs):
            del kwargs
            return []

        async def upsert_crypto_market_candlestick(self, **kwargs) -> None:
            self.candles.append(kwargs)

    fake_kalshi = _FakeKalshi()
    fake_repo = _FakeRepo()

    stored = await CryptoHistoryService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        kalshi=fake_kalshi,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )._capture_candles(fake_repo, market, cutoff=datetime.now(UTC) - timedelta(days=180))  # type: ignore[arg-type]

    assert stored["status"] == "ok"
    assert stored["stored"] == 1
    assert fake_kalshi.live_calls == []
    assert len(fake_kalshi.historical_calls) == 1
    params = fake_kalshi.historical_calls[0]
    assert int(params["end_ts"]) - int(params["start_ts"]) <= 21 * 60
    assert int(params["start_ts"]) >= int((close_time - timedelta(minutes=16)).timestamp())
    assert fake_repo.candles[0]["market_ticker"] == market.market_ticker


@pytest.mark.asyncio
async def test_crypto_history_refetches_existing_historical_candles_to_fill_gaps(tmp_path) -> None:
    settings = _settings(tmp_path)
    close_time = datetime.now(UTC) - timedelta(days=30)
    market = _market(open_time=close_time - timedelta(minutes=15), close_time=close_time, status="settled")

    class _FakeKalshi:
        def __init__(self) -> None:
            self.calls = 0

        async def get_historical_market_candlesticks(self, *args, **kwargs):
            del args, kwargs
            self.calls += 1
            return {"candlesticks": []}

    class _FakeRepo:
        async def list_crypto_market_candlesticks(self, **kwargs):
            del kwargs
            return [object()]

    fake_kalshi = _FakeKalshi()
    result = await CryptoHistoryService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        kalshi=fake_kalshi,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )._capture_candles(_FakeRepo(), market, cutoff=datetime.now(UTC) - timedelta(days=180))  # type: ignore[arg-type]

    assert result == {"status": "ok", "stored": 0}
    assert fake_kalshi.calls == 1


class _FakeBaseExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        return ExecReceiptPayload(
            status="submitted",
            client_order_id=kwargs["client_order_id"],
            details={"called": True},
        )


class _FakeWorkflowMarketService:
    def __init__(self, market: CryptoMarket) -> None:
        self.market = market
        self.kalshi = type("_FakeKalshi", (), {"write_credentials": None})()

    async def get_market(self, market_ticker: str, *, persist: bool = True) -> CryptoMarket:
        assert market_ticker == self.market.market_ticker
        assert persist is True
        return self.market


class _FakeForecastService:
    async def forecast(self, market: CryptoMarket) -> StrategySignal:
        del market
        return _signal()


class _ApproveRiskEngine:
    def evaluate(self, *, ticket: TradeTicket, **kwargs) -> RiskVerdictPayload:
        del kwargs
        return RiskVerdictPayload(
            status=RiskStatus.APPROVED,
            approved_count_fp=ticket.count_fp,
            approved_notional_dollars=ticket.yes_price_dollars * ticket.count_fp,
        )


class _BlockRiskEngine:
    def evaluate(self, **kwargs) -> RiskVerdictPayload:
        del kwargs
        return RiskVerdictPayload(
            status=RiskStatus.BLOCKED,
            reasons=["unit risk block"],
        )


@pytest.mark.asyncio
async def test_crypto_forecast_stands_down_without_trained_artifact(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    signal = await CryptoForecastService(settings=settings, session_factory=session_factory).forecast(_market())

    assert signal.recommended_side is None
    assert signal.stand_down_reason == StandDownReason.CRYPTO_MODEL_UNAVAILABLE
    assert signal.candidate_trace["market_domain"] == "crypto"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_forecast_uses_deterministic_model_artifact(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="model",
            version="test-model",
            status="trained",
            sample_count=2,
            metrics={"sample_count": 2},
            payload={"global_adjustment_bps": 800, "asset_adjustments_bps": {"BTC": 200}},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    signal = await CryptoForecastService(settings=settings, session_factory=session_factory).forecast(_market())

    assert signal.recommended_side == ContractSide.YES
    assert signal.edge_bps > 50
    assert signal.candidate_trace["model_version"] == "test-model"
    await engine.dispose()


def test_crypto_replay_gate_requires_positive_coverage_and_calibration(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    blocked = service.evaluate_gate(
        {
            "resolved_sample_count": 1,
            "trade_candidate_count": 0,
            "net_simulated_pl_dollars": -1.0,
            "hard_cap_breaches": 0,
            "calibration_brier": 0.30,
            "market_mid_brier": 0.25,
        }
    )
    passed = service.evaluate_gate(
        {
            "resolved_sample_count": 2,
            "trade_candidate_count": 1,
            "net_simulated_pl_dollars": 1.0,
            "hard_cap_breaches": 0,
            "calibration_brier": 0.20,
            "market_mid_brier": 0.25,
            "calibration_log_loss": 0.55,
            "market_mid_log_loss": 0.60,
            "calibration_ece": 0.05,
            "market_mid_ece": 0.08,
            "candle_count": 1,
            "spot_feature_coverage_pct": 1.0,
            "strict_trade_eligible_count": 1,
        }
    )

    assert blocked["passed"] is False
    assert passed["passed"] is True


def test_crypto_replay_gate_uses_runtime_crypto_thresholds(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_replay_min_resolved_markets=500)
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]
    policy = AgentPackService(settings).runtime_crypto_policy(
        AgentPackService(settings).default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    replay=AgentPackCryptoReplayPolicy(
                        min_resolved_markets=2,
                        min_trade_candidates=1,
                        min_net_pl_dollars=0.0,
                        max_hard_cap_breaches=0,
                        min_spot_coverage_pct=0.80,
                        require_calibration_better_than_mid=False,
                    )
                )
            }
        )
    )

    passed = service.evaluate_gate(
        {
            "resolved_sample_count": 2,
            "trade_candidate_count": 1,
            "strict_trade_eligible_count": 1,
            "net_simulated_pl_dollars": 1.0,
            "hard_cap_breaches": 0,
            "candle_count": 1,
            "spot_feature_coverage_pct": 1.0,
        },
        crypto_policy=policy,
    )

    assert passed["passed"] is True
    assert passed["requirements"]["min_resolved_markets"] == 2


def test_crypto_decision_rows_use_candle_proxy_when_snapshot_quotes_missing(tmp_path) -> None:
    del tmp_path
    close = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-TEST",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": close - timedelta(minutes=1),
            "close_time": close,
            "expected_expiration_time": close,
            "target_price_dollars": Decimal("100000.00000000"),
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "no_ask_dollars": None,
            "last_price_dollars": None,
            "volume": 10,
            "open_interest": 5,
        },
    )()
    candle = type(
        "_Candle",
        (),
        {
            "market_ticker": "KXBTC15M-TEST",
            "asset_symbol": "BTC",
            "end_period_ts": close - timedelta(minutes=1),
            "close_dollars": Decimal("0.6100"),
        },
    )()

    rows = _crypto_decision_rows([snapshot], [candle])  # type: ignore[list-item]

    assert len(rows) == 1
    assert rows[0]["quote_source"] == "candlestick_close_proxy"
    assert rows[0]["strict_trade_eligible"] is False
    assert rows[0]["execution_model_status"] == "proxy_quote_prediction_only"
    assert rows[0]["yes_ask_dollars"] == Decimal("0.6100")


def test_crypto_decision_rows_recover_real_quotes_from_snapshot_payload(tmp_path) -> None:
    del tmp_path
    close = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-QUOTED",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": close - timedelta(minutes=5),
            "close_time": close,
            "expected_expiration_time": close,
            "target_price_dollars": Decimal("100000.00000000"),
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "no_ask_dollars": None,
            "last_price_dollars": None,
            "volume": 10,
            "open_interest": 5,
            "payload": {
                "raw": {
                    "yes_bid": 47,
                    "yes_ask": 49,
                    "no_ask": 53,
                }
            },
        },
    )()
    candle = type(
        "_Candle",
        (),
        {
            "market_ticker": "KXBTC15M-QUOTED",
            "asset_symbol": "BTC",
            "end_period_ts": close - timedelta(minutes=5),
            "close_dollars": Decimal("0.6100"),
        },
    )()

    rows = _crypto_decision_rows([snapshot], [candle])  # type: ignore[list-item]

    assert len(rows) == 1
    assert rows[0]["quote_source"] == "snapshot_quotes"
    assert rows[0]["strict_trade_eligible"] is True
    assert rows[0]["execution_model_status"] == "real_quote_taker"
    assert rows[0]["yes_bid_dollars"] == Decimal("0.4700")
    assert rows[0]["yes_ask_dollars"] == Decimal("0.4900")
    assert rows[0]["no_ask_dollars"] == Decimal("0.5300")


def test_crypto_decision_rows_join_settlement_labels_to_live_quote_snapshots(tmp_path) -> None:
    del tmp_path
    close = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    quote_snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-JOINED",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "live",
            "settlement_result": None,
            "observed_at": close - timedelta(minutes=3),
            "open_time": close - timedelta(minutes=15),
            "close_time": close,
            "expected_expiration_time": close,
            "target_price_dollars": Decimal("100000.00000000"),
            "yes_bid_dollars": Decimal("0.4700"),
            "yes_ask_dollars": Decimal("0.4900"),
            "no_ask_dollars": Decimal("0.5300"),
            "last_price_dollars": Decimal("0.4800"),
            "volume": 10,
            "open_interest": 5,
        },
    )()
    settled_snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-JOINED",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": close,
            "open_time": close - timedelta(minutes=15),
            "close_time": close,
            "expected_expiration_time": close,
            "target_price_dollars": Decimal("100000.00000000"),
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "no_ask_dollars": None,
            "last_price_dollars": None,
            "volume": 10,
            "open_interest": 5,
        },
    )()

    rows = _crypto_decision_rows([quote_snapshot, settled_snapshot], [])  # type: ignore[list-item]

    assert len(rows) == 1
    assert rows[0]["quote_source"] == "snapshot_quotes"
    assert rows[0]["strict_trade_eligible"] is True
    assert rows[0]["settlement_result"] == "yes"
    assert rows[0]["settlement_label_source"] == "joined_settled_snapshot"
    assert rows[0]["yes_bid_dollars"] == Decimal("0.4700")
    assert rows[0]["yes_ask_dollars"] == Decimal("0.4900")


def test_crypto_decision_rows_generate_prediction_only_preclose_candle_proxy(tmp_path) -> None:
    del tmp_path
    close = datetime(2026, 5, 1, 12, 15, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-PRECLOSE",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "final_market",
            "settlement_result": "no",
            "observed_at": close,
            "close_time": close,
            "expected_expiration_time": close,
            "target_price_dollars": Decimal("100000.00000000"),
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "no_ask_dollars": None,
            "last_price_dollars": None,
            "volume": 10,
            "open_interest": 5,
        },
    )()
    candle = type(
        "_Candle",
        (),
        {
            "market_ticker": "KXBTC15M-PRECLOSE",
            "asset_symbol": "BTC",
            "end_period_ts": close - timedelta(minutes=1),
            "close_dollars": Decimal("0.3900"),
            "volume": 7,
        },
    )()

    rows = _crypto_decision_rows([snapshot], [candle])  # type: ignore[list-item]
    proxy_rows = [row for row in rows if row["row_id"].startswith("candle_proxy:")]

    assert len(proxy_rows) == 1
    assert proxy_rows[0]["source_kind"] == "kalshi_candlestick_replay_proxy"
    assert proxy_rows[0]["prediction_eligible"] is True
    assert proxy_rows[0]["strict_trade_eligible"] is False
    assert proxy_rows[0]["execution_model_status"] == "proxy_quote_prediction_only"


def test_crypto_feature_vector_is_deterministic_and_point_in_time(tmp_path) -> None:
    del tmp_path
    rows = [
        {
            "asset_symbol": "BTC",
            "mid_yes_dollars": Decimal("0.4500"),
            "time_to_close_seconds": 300,
            "spread_bps": 120,
            "volume": 100,
            "open_interest": 25,
            "candle_momentum_dollars": Decimal("0.0100"),
            "target_price_dollars": Decimal("70000"),
            "asset_recent_yes_rate": Decimal("0.6000"),
            "asset_recent_mid_error": Decimal("0.0500"),
            "quote_source": "snapshot_quotes",
        },
        {
            "asset_symbol": "ETH",
            "mid_yes_dollars": Decimal("0.5500"),
            "time_to_close_seconds": 600,
            "spread_bps": 90,
            "volume": 200,
            "open_interest": 50,
            "candle_momentum_dollars": Decimal("-0.0100"),
            "target_price_dollars": Decimal("3500"),
            "asset_recent_yes_rate": None,
            "asset_recent_mid_error": None,
            "quote_source": "candlestick_close_proxy",
        },
    ]

    schema = _crypto_feature_schema(rows)
    first = _crypto_raw_feature_vector(rows[0], schema)
    second = _crypto_raw_feature_vector(rows[0], schema)

    assert schema["feature_schema_version"] == "crypto-rich-v3"
    assert schema["asset_categories"] == ["BTC", "ETH"]
    assert "spot_return_3_pct" in schema["feature_names"]
    assert "time_to_close_bucket_0_5m" in schema["feature_names"]
    assert "quote_source_snapshot_quotes" in schema["feature_names"]
    assert first == second
    assert len(first) == len(schema["feature_names"])
    proxy = _crypto_raw_feature_vector(rows[1], schema)
    quote_real_idx = schema["feature_names"].index("quote_source_snapshot_quotes")
    quote_proxy_idx = schema["feature_names"].index("quote_source_candlestick_proxy")
    assert first[quote_real_idx] == 1.0
    assert proxy[quote_real_idx] == 0.0
    assert proxy[quote_proxy_idx] == 1.0


def test_crypto_serialized_logistic_prediction_is_stable(tmp_path) -> None:
    del tmp_path
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    rows = []
    for idx in range(8):
        rows.append(
            {
                "row_id": f"row-{idx}",
                "market_ticker": f"KXBTC15M-{idx}",
                "asset_symbol": "BTC" if idx < 4 else "ETH",
                "mid_yes_dollars": Decimal("0.3000") if idx % 2 == 0 else Decimal("0.7000"),
                "yes_bid_dollars": Decimal("0.2900") if idx % 2 == 0 else Decimal("0.6900"),
                "yes_ask_dollars": Decimal("0.3100") if idx % 2 == 0 else Decimal("0.7100"),
                "no_ask_dollars": Decimal("0.7100") if idx % 2 == 0 else Decimal("0.3100"),
                "time_to_close_seconds": 300,
                "spread_bps": 200,
                "volume": 100 + idx,
                "open_interest": 50 + idx,
                "candle_momentum_dollars": Decimal("0.0100") if idx % 2 == 0 else Decimal("-0.0100"),
                "target_price_dollars": Decimal("70000"),
                "asset_recent_yes_rate": Decimal("0.5000"),
                "asset_recent_mid_error": Decimal("0.0000"),
                "quote_source": "snapshot_quotes",
                "label_yes": 1 if idx % 2 == 0 else 0,
                "decision_ts": base + timedelta(minutes=idx),
                "settlement_ts": base + timedelta(minutes=idx + 1),
                "market_day": "2026-05-01",
            }
        )

    model = _fit_crypto_calibration(rows)
    first = _predict_crypto_probability(rows[0], model)
    second = _predict_crypto_probability(rows[0], model)

    assert model["model_type"] in {
        "sklearn_logistic",
        "xgboost_classifier",
        "lightgbm_classifier",
        "calibrated_weighted_ensemble",
        "market_mid_baseline",
    }
    assert model["feature_schema_version"] == "crypto-rich-v3"
    assert model["candidate_report"]["primary_metric"] == "brier"
    assert first == second
    assert Decimal("0.0100") <= first <= Decimal("0.9900")


def test_crypto_candidate_registry_reports_optional_rich_models(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_min_training_samples=4)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    rows = []
    for day in range(4):
        for idx, asset in enumerate(("BTC", "ETH")):
            label = 1 if (day + idx) % 2 == 0 else 0
            mid = Decimal("0.3500") if label else Decimal("0.6500")
            rows.append(
                {
                    "row_id": f"row-{day}-{asset}",
                    "market_ticker": f"KX{asset}15M-{day}",
                    "asset_symbol": asset,
                    "mid_yes_dollars": mid,
                    "yes_bid_dollars": mid - Decimal("0.0100"),
                    "yes_ask_dollars": mid + Decimal("0.0100"),
                    "no_ask_dollars": Decimal("1.0000") - mid,
                    "time_to_close_seconds": 300 + (idx * 300),
                    "market_age_seconds": 600,
                    "spread_bps": 200,
                    "volume": 100 + day,
                    "open_interest": 50 + day,
                    "candle_momentum_dollars": Decimal("0.0100") if label else Decimal("-0.0100"),
                    "target_price_dollars": Decimal("70000"),
                    "asset_recent_yes_rate": Decimal("0.5000"),
                    "asset_recent_mid_error": Decimal("0.0000"),
                    "quote_source": "snapshot_quotes",
                    "strict_trade_eligible": True,
                    "label_yes": label,
                    "decision_ts": base + timedelta(days=day, minutes=idx),
                    "settlement_ts": base + timedelta(days=day, minutes=idx + 15),
                    "market_day": (base + timedelta(days=day)).date().isoformat(),
                }
            )

    report = _crypto_model_candidate_report(rows, settings=settings)
    names = {candidate["name"]: candidate for candidate in report["candidates"]}

    assert report["primary_metric"] == "brier"
    assert report["selection_scope"] == "walk_forward_time_ordered"
    assert report["champion_name"] in names
    assert names["market_mid_baseline"]["metrics"]["brier"] is not None
    assert names["sklearn_logistic"]["status"] in {"available", "guardrail_failed"}
    assert names["xgboost_classifier"]["status"] in {"available", "unavailable", "guardrail_failed"}
    assert names["lightgbm_classifier"]["status"] in {"available", "unavailable", "guardrail_failed"}


def test_crypto_ensemble_prediction_is_deterministic(tmp_path) -> None:
    del tmp_path
    row = {
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.4500"),
        "time_to_close_seconds": 300,
        "spread_bps": 120,
        "volume": 100,
        "open_interest": 25,
        "candle_momentum_dollars": Decimal("0.0100"),
        "target_price_dollars": Decimal("70000"),
        "asset_recent_yes_rate": Decimal("0.6000"),
        "asset_recent_mid_error": Decimal("0.0500"),
        "quote_source": "snapshot_quotes",
        "strict_trade_eligible": True,
    }
    schema = _crypto_feature_schema([row])
    market_model = {
        "model_type": "market_mid_baseline",
        "feature_names": schema["feature_names"],
        "numeric_feature_names": schema["numeric_feature_names"],
        "asset_categories": schema["asset_categories"],
    }
    heuristic_model = {
        "model_type": "heuristic_adjustment",
        "global_adjustment_bps": 100,
        "asset_adjustments_bps": {"BTC": 50},
    }
    ensemble = {
        "model_type": "calibrated_weighted_ensemble",
        "ensemble_weights": {"market_mid_baseline": 0.25, "heuristic_adjustment": 0.75},
        "member_models": {
            "market_mid_baseline": market_model,
            "heuristic_adjustment": heuristic_model,
        },
    }

    first = _predict_crypto_probability(row, ensemble)
    second = _predict_crypto_probability(row, ensemble)

    assert first == second
    assert Decimal("0.0100") <= first <= Decimal("0.9900")


def test_crypto_candidate_quality_classifies_live_and_exploratory(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    row = {
        "market_ticker": "KXBTC15M-CAND",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4700"),
        "yes_ask_dollars": Decimal("0.4900"),
        "no_ask_dollars": Decimal("0.5300"),
        "spread_bps": 200,
    }

    live = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings)
    exploratory = _crypto_trade_candidates(row, Decimal("0.5000"), settings=settings)

    assert live[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert exploratory[0]["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert exploratory[0]["live_eligible"] is False


def test_crypto_candidate_quality_uses_runtime_crypto_policy(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=50, trigger_max_spread_bps=1000)
    service = AgentPackService(settings)
    pack = service.default_pack().model_copy(
        update={
            "crypto_policy": AgentPackCryptoPolicy(
                entry=AgentPackCryptoEntryPolicy(
                    min_fee_adjusted_edge_bps=500,
                    max_spread_bps=100,
                    min_confidence=0.50,
                    min_contract_price_dollars=0.03,
                    min_remaining_payout_bps=500,
                    max_credible_edge_bps=5000,
                ),
                asset_entry_overrides={
                    "BTC": AgentPackCryptoEntryPolicy(max_spread_bps=250),
                },
            )
        }
    )
    policy = service.runtime_crypto_policy(pack)
    row = {
        "market_ticker": "KXBTC15M-RUNTIME",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4700"),
        "yes_ask_dollars": Decimal("0.4900"),
        "no_ask_dollars": Decimal("0.5300"),
        "spread_bps": 200,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings, crypto_policy=policy)

    assert candidates[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert candidates[0]["runtime_thresholds"]["max_spread_bps"] == 250


def test_crypto_candidate_quality_blocks_runtime_spread_over_asset_limit(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=50, trigger_max_spread_bps=1000)
    service = AgentPackService(settings)
    pack = service.default_pack().model_copy(
        update={
            "crypto_policy": AgentPackCryptoPolicy(
                entry=AgentPackCryptoEntryPolicy(
                    min_fee_adjusted_edge_bps=500,
                    max_spread_bps=100,
                    min_confidence=0.50,
                    min_contract_price_dollars=0.03,
                    min_remaining_payout_bps=500,
                    max_credible_edge_bps=5000,
                )
            )
        }
    )
    policy = service.runtime_crypto_policy(pack)
    row = {
        "market_ticker": "KXETH15M-RUNTIME",
        "asset_symbol": "ETH",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4700"),
        "yes_ask_dollars": Decimal("0.4900"),
        "no_ask_dollars": Decimal("0.5300"),
        "spread_bps": 200,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings, crypto_policy=policy)

    assert candidates[0]["candidate_status"] != CRYPTO_LIVE_QUALITY
    assert candidates[0]["reason"] == "spread_above_live_max"


def test_crypto_proxy_quote_rows_are_prediction_only(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=50)
    row = {
        "market_ticker": "KXBTC15M-PROXY",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 0,
        "strict_trade_eligible": False,
        "execution_model_status": "proxy_quote_prediction_only",
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert {candidate["candidate_status"] for candidate in candidates} == {"prediction_only_proxy_quote"}
    assert all(candidate["live_eligible"] is False for candidate in candidates)


def test_crypto_data_quality_reports_per_asset_gaps(tmp_path) -> None:
    del tmp_path
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "asset_symbol": "BTC",
            "market_ticker": "KXBTC15M-TEST",
            "settlement_result": "yes",
            "observed_at": now,
            "source_kind": "historical",
        },
    )()
    quality = _crypto_data_quality([snapshot], [], min_training_samples=1)  # type: ignore[list-item]

    assert quality["status"] == "needs_data"
    assert quality["assets"]["BTC"]["settled_snapshot_count"] == 1
    assert quality["assets"]["BTC"]["markets_missing_candles"] == 1


@pytest.mark.asyncio
async def test_crypto_train_stores_model_with_fee_aware_metrics(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_min_training_samples=4, crypto_replay_min_resolved_markets=4)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    await _seed_crypto_training_rows(session_factory, settings, days=4)

    service = CryptoForecastService(settings=settings, session_factory=session_factory)
    result = await service.train(frequency="15m")
    candidates = await service.candidates(frequency="15m", days=30)

    assert result["status"] == "trained"
    assert result["metrics"]["resolved_sample_count"] == 8
    assert result["metrics"]["fees_dollars"] >= 0
    assert result["metrics"]["champion_model"] in {
        "market_mid_baseline",
        "sklearn_logistic",
        "xgboost_classifier",
        "lightgbm_classifier",
        "calibrated_weighted_ensemble",
    }
    assert result["payload"]["candidate_report"]["primary_metric"] == "brier"
    assert result["payload"]["candidate_registry_version"] == "crypto-candidate-registry-v1"
    assert "candlestick_momentum" in result["payload"]["feature_set"]
    assert candidates["schema_version"] == "crypto-model-candidates-v2"
    assert candidates["primary_metric"] == "brier"
    assert candidates["ranked_candidates"]
    assert candidates["candidate_report"]["champion_name"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_replay_run_validate_and_gate_use_backtest_metrics(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_min_training_samples=4,
        crypto_replay_min_resolved_markets=4,
        crypto_replay_min_trade_candidates=0,
        crypto_replay_require_calibration_better_than_mid=False,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    await _seed_crypto_training_rows(session_factory, settings, days=5)
    await CryptoForecastService(settings=settings, session_factory=session_factory).train(frequency="15m")
    replay = CryptoReplayService(settings=settings, session_factory=session_factory)

    run_report = await replay.run(frequency="15m", days=30, persist=True)
    validate_report = await replay.validate(frequency="15m", days=30)
    gate = await replay.gate(frequency="15m")

    assert run_report["schema_version"] == "crypto-backtest-report-v1"
    assert run_report["data_quality"]["candle_count"] == 10
    assert "baseline_policy" in run_report["walk_forward"]
    assert validate_report["status"] in {"pass", "warn", "fail"}
    assert gate["requirements"]["requires_candles"] is True
    await engine.dispose()


def test_passive_prices_do_not_cross_crypto_touch(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = CryptoExecutionService(  # noqa: F841 - documents constructor compatibility.
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        base_execution_service=None,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(settings=settings, session_factory=None),  # type: ignore[arg-type]
    )
    market = _market(yes_bid_dollars=Decimal("0.4700"), yes_ask_dollars=Decimal("0.4900"))

    yes_price = service.passive_yes_price(market, ContractSide.YES)
    no_price = service.passive_yes_price(market, ContractSide.NO)

    assert yes_price == Decimal("0.4701")
    assert no_price == Decimal("0.4899")


@pytest.mark.asyncio
async def test_crypto_asset_modes_default_shadow_and_persist(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = CryptoAssetControlService(settings=settings, session_factory=session_factory)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.update_deployment_notes(
            {
                "kill_switch": {"mode": "manual_or_external"},
                "agent_packs": {"active_version": "green-pack"},
                "crypto_replay_gate": {"status": "passed"},
            }
        )
        await session.commit()

    default_modes = await service.list_asset_modes(asset_symbols=["btc", "ETH"])
    updated = await service.set_asset_mode("btc", "live", actor="test")
    listed = await service.list_asset_modes(asset_symbols=["BTC", "ETH"])
    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        preserved_notes = control.notes
        await session.commit()

    assert default_modes["modes"] == {"BTC": "shadow", "ETH": "shadow"}
    assert updated["previous_mode"] == "shadow"
    assert listed["modes"]["BTC"] == "live"
    assert listed["modes"]["ETH"] == "shadow"
    assert listed["counts"]["live"] == 1
    assert preserved_notes["kill_switch"] == {"mode": "manual_or_external"}
    assert preserved_notes["agent_packs"] == {"active_version": "green-pack"}
    assert preserved_notes["crypto_replay_gate"] == {"status": "passed"}
    await engine.dispose()


def test_crypto_runtime_policy_can_make_asset_live_without_env_trading_flag(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=False)
    service = AgentPackService(settings)
    policy = service.runtime_crypto_policy(
        service.default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    live={
                        "trading_enabled": True,
                        "production_autonomy_enabled": True,
                        "asset_modes": {"BTC": "live"},
                    }
                )
            }
        )
    )
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]
    control = type("_Control", (), {"notes": {}, "kill_switch_enabled": False, "active_color": settings.app_color})()
    gate = type("_Gate", (), {"status": "blocked", "metrics": {
        "resolved_sample_count": settings.crypto_replay_min_resolved_markets,
        "trade_candidate_count": settings.crypto_replay_min_trade_candidates,
        "strict_trade_eligible_count": settings.crypto_replay_min_trade_candidates,
        "net_simulated_pl_dollars": 10.0,
        "hard_cap_breaches": 0,
        "candle_count": 1,
        "spot_feature_coverage_pct": 1.0,
        "calibration_brier": 0.20,
        "market_mid_brier": 0.25,
        "calibration_log_loss": 0.55,
        "market_mid_log_loss": 0.60,
        "calibration_ece": 0.05,
        "market_mid_ece": 0.08,
    }})()

    status = asset_control.market_live_status(
        control=control,
        replay_gate=gate,
        market=_market(asset_symbol="BTC"),
        has_write_credentials=True,
        crypto_policy=policy,
    )

    assert status["asset_mode"] == "live"
    assert status["live_eligible"] is True
    assert status["global_live_blockers"] == []


def test_crypto_manual_off_note_overrides_runtime_live_policy(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=False)
    service = AgentPackService(settings)
    policy = service.runtime_crypto_policy(
        service.default_pack().model_copy(
            update={"crypto_policy": AgentPackCryptoPolicy(live={"trading_enabled": True, "asset_modes": {"BTC": "live"}})}
        )
    )
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]
    control = type(
        "_Control",
        (),
        {"notes": {"crypto_asset_modes": {"BTC": "off"}}, "kill_switch_enabled": False, "active_color": settings.app_color},
    )()

    assert asset_control.mode_for_control(control, "BTC", crypto_policy=policy) == "off"


@pytest.mark.asyncio
async def test_crypto_execution_blocks_non_live_asset_before_base_execution(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate",
            version="passed-gate",
            status="passed",
            sample_count=1000,
            metrics={},
            payload={"passed": True},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "crypto_asset_live_disabled"
    assert receipt.details["asset_mode"] == "shadow"
    assert fake_base.calls == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_preserves_shadow_status_for_shadow_room(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC shadow crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-shadow-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "shadow_skipped"
    assert receipt.details["asset_mode"] == "shadow"
    assert fake_base.calls == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_live_asset_reaches_base_execution(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate",
            version="passed-gate",
            status="passed",
            sample_count=1000,
            metrics={},
            payload={"passed": True},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "submitted"
    assert fake_base.calls[0]["client_order_id"] == "crypto-test:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_non_live_asset_receipt_does_not_save_order(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=_FakeBaseExecution(),  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(),  # type: ignore[arg-type]
        risk_engine=_ApproveRiskEngine(),  # type: ignore[arg-type]
        execution_service=execution,
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC non-live workflow", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await session.commit()

    await workflow.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    assert ticket is not None
    assert ticket.status == "crypto_asset_live_disabled"
    assert orders == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_shadow_room_creates_shadow_ticket_without_order(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=False)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=_FakeBaseExecution(),  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(),  # type: ignore[arg-type]
        risk_engine=_ApproveRiskEngine(),  # type: ignore[arg-type]
        execution_service=execution,
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC shadow workflow", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    await workflow.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        verdicts = list((await session.execute(select(RiskVerdictRecord))).scalars())
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    assert ticket is not None
    assert ticket.status == "shadow_skipped"
    assert ticket.payload["asset_mode"] == "shadow"
    assert ticket.payload["crypto_modeling"] is not None
    assert len(verdicts) == 1
    assert orders == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_shadow_risk_block_records_shadow_skipped_receipt(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=False)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10), yes_bid_dollars=Decimal("0.0100"))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=_FakeBaseExecution(),  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(),  # type: ignore[arg-type]
        risk_engine=_BlockRiskEngine(),  # type: ignore[arg-type]
        execution_service=execution,
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC shadow risk block", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    await workflow.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        messages = list((await session.execute(select(RoomMessage))).scalars())
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    receipts = [message for message in messages if message.kind == "ExecReceipt"]
    assert ticket is not None
    assert ticket.status == "blocked"
    assert verdict is not None
    assert verdict.status == RiskStatus.BLOCKED.value
    assert len(receipts) == 1
    assert "shadow_skipped" in receipts[0].content
    assert receipts[0].payload["details"]["reason"] == "risk_blocked_before_execution"
    assert receipts[0].payload["details"]["no_order_submitted"] is True
    assert orders == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_skips_off_assets_and_existing_rooms(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=True,
        crypto_autonomy_enabled=True,
        crypto_autonomy_min_seconds_to_close=120,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "off", actor="test")
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await repo.create_room(
            RoomCreate(name="ETH duplicate", market_ticker="KXETH15M-TEST"),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        def __init__(self) -> None:
            self.kalshi = _FakeKalshi()
            self.created: list[str] = []
            base = datetime.now(UTC)
            self.markets = [
                _market(market_ticker="KXBTC15M-TEST", asset_symbol="BTC", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXETH15M-TEST", asset_symbol="ETH", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXSOL15M-TEST", asset_symbol="SOL", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXADA15M-SOON", asset_symbol="ADA", close_time=base + timedelta(seconds=45)),
                _market(market_ticker="KXADA15M-LATER", asset_symbol="ADA", close_time=base + timedelta(minutes=5)),
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            market = next(item for item in self.markets if item.market_ticker == market_ticker)
            return {
                "room_id": f"room-{market_ticker}",
                "market_ticker": market_ticker,
                "asset_symbol": market.asset_symbol,
                "asset_mode": "shadow",
                "live_eligible": False,
                "live_blockers": ["shadow"],
            }

    class _FakeWorkflowService:
        def __init__(self) -> None:
            self.ran: list[str] = []

        async def run_room(self, room_id: str, *, reason: str) -> None:
            self.ran.append(room_id)

    market_service = _FakeMarketService()
    workflow_service = _FakeWorkflowService()
    result = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=market_service,  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=workflow_service,  # type: ignore[arg-type]
    ).run_once()

    assert result["status"] == "ok"
    assert set(market_service.created) == {"KXADA15M-LATER", "KXSOL15M-TEST"}
    assert set(workflow_service.ran) == {"room-KXADA15M-LATER", "room-KXSOL15M-TEST"}
    assert {item["reason"] for item in result["skipped"]} == {"asset_mode_off", "room_already_exists"}
    assert "KXADA15M-SOON" not in market_service.created
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_run_once_can_be_forced_for_operator_shadow_pass(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=True,
        crypto_autonomy_enabled=False,
        crypto_autonomy_min_seconds_to_close=120,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        kalshi = _FakeKalshi()

        def __init__(self) -> None:
            self.created: list[str] = []
            self.markets = [
                _market(
                    market_ticker="KXBTC15M-FORCE",
                    asset_symbol="BTC",
                    close_time=datetime.now(UTC) + timedelta(minutes=10),
                )
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            return {
                "room_id": "room-force",
                "market_ticker": market_ticker,
                "asset_symbol": "BTC",
                "asset_mode": "shadow",
                "live_eligible": False,
                "live_blockers": ["shadow"],
            }

    class _FakeWorkflowService:
        async def run_room(self, room_id: str, *, reason: str) -> None:
            self.room_id = room_id
            self.reason = reason

    market_service = _FakeMarketService()
    workflow_service = _FakeWorkflowService()
    disabled = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=market_service,  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=workflow_service,  # type: ignore[arg-type]
    ).run_once()
    forced = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=market_service,  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=workflow_service,  # type: ignore[arg-type]
    ).run_once(force=True)

    assert disabled["status"] == "disabled"
    assert forced["status"] == "ok"
    assert forced["forced"] is True
    assert market_service.created == ["KXBTC15M-FORCE"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_requires_explicit_production_fuse(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        kalshi_env="production",
        app_shadow_mode=False,
        crypto_autonomy_enabled=True,
        crypto_production_autonomy_enabled=False,
    )
    engine = create_engine(settings)
    result = await CryptoAutonomyService(
        settings=settings,
        session_factory=create_session_factory(engine),
        market_service=object(),  # type: ignore[arg-type]
        asset_control_service=object(),  # type: ignore[arg-type]
        workflow_service=object(),  # type: ignore[arg-type]
    ).run_once()

    assert result["status"] == "production_blocked"
    assert "CRYPTO_PRODUCTION_AUTONOMY_ENABLED" in result["reason"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_production_fuse_skips_non_live_eligible_markets(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        kalshi_env="production",
        app_shadow_mode=False,
        crypto_autonomy_enabled=True,
        crypto_production_autonomy_enabled=True,
        crypto_trading_enabled=True,
        crypto_autonomy_min_seconds_to_close=120,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            kalshi_env=settings.kalshi_env,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        kalshi = _FakeKalshi()

        def __init__(self) -> None:
            self.created: list[str] = []
            self.markets = [
                _market(
                    market_ticker="KXBTC15M-PROD",
                    asset_symbol="BTC",
                    close_time=datetime.now(UTC) + timedelta(minutes=10),
                )
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            return {"room_id": "should-not-create"}

    class _FakeWorkflowService:
        async def run_room(self, room_id: str, *, reason: str) -> None:
            self.room_id = room_id

    market_service = _FakeMarketService()
    result = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=market_service,  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=_FakeWorkflowService(),  # type: ignore[arg-type]
    ).run_once()

    assert result["status"] == "ok"
    assert market_service.created == []
    assert result["skipped"][0]["reason"] == "not_live_eligible"
    assert "Asset BTC mode is shadow; set it to live to allow live orders." in result["skipped"][0]["live_blockers"]
    await engine.dispose()
