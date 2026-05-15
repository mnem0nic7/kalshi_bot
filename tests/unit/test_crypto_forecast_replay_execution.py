from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, RiskStatus, RoomOrigin, StandDownReason, TradeAction
from kalshi_bot.core.schemas import (
    AgentPackCryptoEntryPolicy,
    AgentPackCryptoLivePolicy,
    AgentPackCryptoPolicy,
    AgentPackCryptoReplayPolicy,
    ExecReceiptPayload,
    RiskVerdictPayload,
    RoomCreate,
    TradeEligibilityVerdict,
    TradeTicket,
)
from kalshi_bot.crypto.models import CryptoMarket, CryptoSeries
from kalshi_bot.crypto.services import (
    CRYPTO_EXPLORATORY_SHADOW,
    CRYPTO_LAST_MINUTE_PASSIVE_REASON,
    CRYPTO_LIVE_QUALITY,
    CryptoAssetControlService,
    CryptoAutonomyService,
    CryptoHistoryService,
    CryptoWorkflowService,
    CryptoExecutionService,
    CryptoForecastService,
    CryptoReplayService,
    _crypto_apply_empirical_bucket_gate_to_replay_metrics,
    _crypto_data_quality,
    _crypto_decision_rows,
    _crypto_bucket_key,
    _crypto_bucket_matrix,
    _evaluate_crypto_walk_forward,
    _crypto_feature_schema,
    _crypto_dynamic_order_count_fp,
    _crypto_last_minute_passive_bid_by_asset,
    _crypto_model_candidate_report,
    _crypto_optimize_asset_entry_policy,
    _crypto_recommendation,
    _crypto_replay_gate_dashboard_summary,
    _crypto_raw_feature_vector,
    _crypto_select_champion,
    _crypto_signal_payload_with_current_quote_metrics,
    _crypto_trade_candidates,
    _fit_crypto_calibration,
    _predict_crypto_probability,
)
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.db.models import OrderRecord, RiskVerdictRecord, RoomMessage
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
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
        "crypto_autonomy_enabled": False,
        "crypto_production_autonomy_enabled": False,
        "crypto_trading_enabled": False,
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


def _replay_row(**overrides) -> dict[str, object]:
    day = str(overrides.pop("market_day", "2026-05-01"))
    values: dict[str, object] = {
        "market_ticker": f"KXBTC15M-{day}",
        "asset_symbol": "BTC",
        "market_day": day,
        "decision_ts": datetime.fromisoformat(f"{day}T12:00:00+00:00"),
        "settlement_ts": datetime.fromisoformat(f"{day}T12:15:00+00:00"),
        "label_yes": 1,
        "mid_yes_dollars": Decimal("0.7000"),
        "yes_bid_dollars": Decimal("0.4800"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "quote_source": "snapshot_quotes",
        "strict_trade_eligible": True,
        "prediction_eligible": True,
        "leakage_status": "point_in_time",
        "time_to_close_seconds": 300,
        "market_age_seconds": 600,
        "volume": 100,
        "open_interest": 50,
        "target_price_dollars": Decimal("100.00000000"),
        "candle_momentum_dollars": Decimal("0.0100"),
        "spot_return_1_pct": Decimal("0.0100"),
        "spot_return_3_pct": Decimal("0.0100"),
        "spot_return_6_pct": Decimal("0.0100"),
    }
    values.update(overrides)
    return values


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


def _live_quality_signal(**overrides) -> StrategySignal:
    signal = _signal()
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "0.0600",
        },
    }
    if overrides:
        for key, value in overrides.items():
            setattr(signal, key, value)
    return signal


def _risk_context_for_sizing(**overrides) -> RiskContext:
    values = {
        "market_observed_at": datetime.now(UTC),
        "research_observed_at": datetime.now(UTC),
        "total_capital_dollars": Decimal("100.00"),
        "current_position_notional_dollars": Decimal("0"),
        "current_position_count_fp": Decimal("0"),
        "pending_order_count_fp": Decimal("0"),
        "pending_order_notional_dollars": Decimal("0"),
        "open_ticker_count": 0,
        "strategy_code": "CRYPTO_15M",
    }
    values.update(overrides)
    return RiskContext(**values)


def _sizing_ticket(**overrides) -> TradeTicket:
    values = {
        "market_ticker": "KXBTC15M-SIZING",
        "action": TradeAction.BUY,
        "side": ContractSide.YES,
        "yes_price_dollars": Decimal("0.5000"),
        "count_fp": Decimal("1.00"),
    }
    values.update(overrides)
    return TradeTicket(**values)


def test_crypto_dynamic_sizing_live_quality_yes_uses_position_budget(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.5000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    assert count == Decimal("20.00")
    assert diagnostics["mode"] == "dynamic"
    assert diagnostics["unit_cost_dollars"] == "0.5000"
    assert diagnostics["target_notional_dollars"] == "10.0000"


def test_crypto_dynamic_sizing_live_quality_no_uses_no_unit_cost(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.NO, yes_price_dollars=Decimal("0.2000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    assert count == Decimal("12.50")
    assert diagnostics["unit_cost_dollars"] == "0.8000"


def test_crypto_dynamic_sizing_floors_to_count_cent(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.3333")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    assert count == Decimal("30.00")
    assert diagnostics["raw_count_fp"] == "30.00"


def test_crypto_dynamic_sizing_caps_order_and_remaining_position_count(tmp_path) -> None:
    order_capped_settings = _settings(
        tmp_path,
        risk_position_pct=0.10,
        crypto_dynamic_order_target_position_pct=0.10,
        risk_max_order_count_fp=5.0,
    )

    order_capped, order_diag = _crypto_dynamic_order_count_fp(
        settings=order_capped_settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.1000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    position_capped_settings = _settings(
        tmp_path,
        risk_position_pct=0.10,
        crypto_dynamic_order_target_position_pct=0.10,
        risk_max_position_count_fp_per_ticker=10.0,
    )
    position_capped, position_diag = _crypto_dynamic_order_count_fp(
        settings=position_capped_settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.1000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(
            total_capital_dollars=Decimal("100.00"),
            current_position_count_fp=Decimal("3.00"),
            pending_order_count_fp=Decimal("2.00"),
        ),
    )

    assert order_capped == Decimal("5.00")
    assert order_diag["capped_count_fp"] == "5.00"
    assert position_capped == Decimal("5.00")
    assert position_diag["remaining_position_count_fp"] == "5.00"
    assert position_diag["capped_count_fp"] == "5.00"


def test_crypto_dynamic_sizing_caps_late_empirical_override_to_one_contract(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_position_pct=0.10,
        crypto_dynamic_order_target_position_pct=0.10,
        risk_max_order_count_fp=500.0,
        risk_max_position_count_fp_per_ticker=500.0,
    )
    signal = _live_quality_signal()
    signal.candidate_trace = {
        **(signal.candidate_trace or {}),
        "empirical_bucket_gate": {
            "status": "override_allowed",
            "allowed": True,
            "override_allowed": True,
            "override_reason": "late_high_confidence_empirical_bucket_missing_override",
            "original_reason": "empirical_bucket_missing",
        },
    }

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.5000")),
        signal=signal,
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    assert count == Decimal("1.00")
    assert diagnostics["raw_count_fp"] == "20.00"
    assert diagnostics["capped_count_fp"] == "1.00"
    assert diagnostics["empirical_bucket_late_override_cap_active"] is True


def test_crypto_dynamic_sizing_late_high_confidence_uses_position_budget(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_position_pct=0.10,
        crypto_dynamic_order_target_position_pct=0.10,
        risk_max_order_count_fp=500.0,
        risk_max_position_count_fp_per_ticker=500.0,
    )
    signal = _live_quality_signal()
    signal.candidate_trace = {
        **(signal.candidate_trace or {}),
        "late_high_confidence_directional_entry": True,
    }

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.NO, yes_price_dollars=Decimal("0.1300")),
        signal=signal,
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )

    assert count == Decimal("11.49")
    assert diagnostics["raw_count_fp"] == "11.49"
    assert diagnostics["capped_count_fp"] == "11.49"
    assert "late_high_confidence_max_loss_cap_active" not in diagnostics
    assert "late_high_confidence_max_loss_count_fp" not in diagnostics


def test_crypto_dynamic_sizing_subtracts_current_and_pending_notional(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.5000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(
            total_capital_dollars=Decimal("100.00"),
            current_position_notional_dollars=Decimal("4.0000"),
            pending_order_notional_dollars=Decimal("2.0000"),
        ),
    )

    assert count == Decimal("8.00")
    assert diagnostics["available_notional_dollars"] == "4.0000"


def test_crypto_dynamic_sizing_non_live_quality_and_missing_capital_keep_default(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    non_live_count, non_live_diag = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.5000")),
        signal=_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=Decimal("100.00")),
    )
    missing_capital_count, missing_capital_diag = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(side=ContractSide.YES, yes_price_dollars=Decimal("0.5000")),
        signal=_live_quality_signal(),
        context=_risk_context_for_sizing(total_capital_dollars=None),
    )

    assert non_live_count == Decimal("1.00")
    assert non_live_diag["reason"] == "candidate_not_live_quality"
    assert missing_capital_count == Decimal("1.00")
    assert missing_capital_diag["reason"] == "missing_total_capital"


def test_crypto_replay_gate_passes_profitable_model_even_when_calibration_lags_market_mid(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_replay_require_calibration_better_than_mid=False,
        crypto_replay_require_pnl_beats_market_mid=True,
    )
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    gate = service.evaluate_gate(
        {
            "resolved_sample_count": 20,
            "trade_candidate_count": 3,
            "strict_trade_eligible_count": 20,
            "net_simulated_pl_dollars": 4.0,
            "market_mid_net_simulated_pl_dollars": 1.0,
            "pnl_advantage_vs_market_mid_dollars": 3.0,
            "hard_cap_breaches": 0,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
            "calibration_brier": 0.30,
            "market_mid_brier": 0.20,
            "calibration_log_loss": 0.90,
            "market_mid_log_loss": 0.70,
            "calibration_ece": 0.15,
            "market_mid_ece": 0.05,
        }
    )

    assert gate["passed"] is True
    assert gate["requirements"]["pnl_beats_market_mid"] is True
    assert gate["requirements"]["calibration_better_than_mid"] is False


def test_crypto_replay_gate_blocks_losing_model_even_with_good_calibration(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_replay_require_calibration_better_than_mid=False,
        crypto_replay_require_pnl_beats_market_mid=True,
    )
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    gate = service.evaluate_gate(
        {
            "resolved_sample_count": 20,
            "trade_candidate_count": 3,
            "strict_trade_eligible_count": 20,
            "net_simulated_pl_dollars": -1.0,
            "market_mid_net_simulated_pl_dollars": -2.0,
            "pnl_advantage_vs_market_mid_dollars": 1.0,
            "hard_cap_breaches": 0,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
            "calibration_brier": 0.10,
            "market_mid_brier": 0.20,
            "calibration_log_loss": 0.50,
            "market_mid_log_loss": 0.70,
            "calibration_ece": 0.01,
            "market_mid_ece": 0.05,
        }
    )

    assert gate["passed"] is False
    assert any("Net simulated P/L" in reason for reason in gate["reasons"])


def test_crypto_replay_gate_allows_bucket_gated_market_mid_tie_when_profitable(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_replay_require_calibration_better_than_mid=False,
        crypto_replay_require_pnl_beats_market_mid=True,
    )
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    gate = service.evaluate_gate(
        {
            "metrics_source": "empirical_bucket_gated",
            "resolved_sample_count": 20,
            "trade_candidate_count": 3,
            "current_model_live_quality_candidate_count": 3,
            "strict_trade_eligible_count": 20,
            "net_simulated_pl_dollars": 0.21,
            "market_mid_net_simulated_pl_dollars": 0.21,
            "pnl_advantage_vs_market_mid_dollars": 0.0,
            "hard_cap_breaches": 0,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
        }
    )

    assert gate["passed"] is True


def test_crypto_replay_gate_blocks_bucket_gated_market_mid_underperformance(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_replay_require_calibration_better_than_mid=False,
        crypto_replay_require_pnl_beats_market_mid=True,
    )
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    gate = service.evaluate_gate(
        {
            "metrics_source": "empirical_bucket_gated",
            "resolved_sample_count": 20,
            "trade_candidate_count": 3,
            "current_model_live_quality_candidate_count": 3,
            "strict_trade_eligible_count": 20,
            "net_simulated_pl_dollars": 0.20,
            "market_mid_net_simulated_pl_dollars": 0.21,
            "pnl_advantage_vs_market_mid_dollars": -0.01,
            "hard_cap_breaches": 0,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
        }
    )

    assert gate["passed"] is False
    assert any("market-mid baseline" in reason for reason in gate["reasons"])


@pytest.mark.asyncio
async def test_crypto_replay_gate_falls_back_to_per_asset_artifacts_for_multi_asset_request(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=5,
        crypto_replay_min_trade_candidates=2,
        crypto_replay_require_calibration_better_than_mid=False,
        crypto_replay_require_pnl_beats_market_mid=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    def metrics(candidate_count: int, net_pnl: float) -> dict[str, object]:
        return {
            "sample_count": 10,
            "resolved_sample_count": 10,
            "strict_trade_eligible_count": 10,
            "trade_candidate_count": candidate_count,
            "current_model_live_quality_candidate_count": candidate_count,
            "oos_trade_candidate_count": candidate_count,
            "oos_fold_count": 1,
            "oos_evaluation_status": "ok",
            "net_simulated_pl_dollars": net_pnl,
            "market_mid_net_simulated_pl_dollars": 0.0,
            "pnl_advantage_vs_market_mid_dollars": net_pnl,
            "oos_net_simulated_pl_dollars": net_pnl,
            "oos_market_mid_net_simulated_pl_dollars": 0.0,
            "oos_pnl_advantage_vs_market_mid_dollars": net_pnl,
            "hard_cap_breaches": 0,
            "candle_count": 10,
            "spot_feature_coverage_pct": 1.0,
            "calibration_brier": 0.10,
            "market_mid_brier": 0.20,
            "calibration_log_loss": 0.40,
            "market_mid_log_loss": 0.50,
            "calibration_ece": 0.01,
            "market_mid_ece": 0.02,
        }

    async with session_factory() as session:
        repo = PlatformRepository(session)
        for asset, candidate_count, net_pnl in (("BTC", 2, 3.0), ("ETH", 0, 0.0)):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=f"model:{asset}",
                version=f"model-{asset}",
                status="trained",
                sample_count=10,
                metrics=metrics(candidate_count, net_pnl),
                payload={"asset_symbols": [asset]},
                kalshi_env=settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=f"backtest:{asset}",
                version=f"backtest-{asset}",
                status="pass",
                sample_count=10,
                metrics=metrics(candidate_count, net_pnl),
                payload={"asset_symbols": [asset]},
                kalshi_env=settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
        await session.commit()

    service = CryptoReplayService(settings=settings, session_factory=session_factory)
    result = await service.gate(frequency="15m", asset_symbols=["BTC", "ETH"])

    assert result["status"] == "blocked"
    assert result["aggregate_source"] == "per_asset_artifacts"
    assert not any(reason == "Crypto model artifact is missing." for reason in result["reasons"])
    assert any(
        reason.startswith("ETH: Out-of-sample trade candidate count 0 below minimum 2.")
        for reason in result["reasons"]
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        aggregate_gate = await repo.get_latest_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate",
            kalshi_env=settings.kalshi_env,
        )
        await session.commit()

    assert aggregate_gate is not None
    assert aggregate_gate.status == "blocked"
    assert aggregate_gate.metrics["aggregate_source"] == "per_asset_artifacts"
    await engine.dispose()


def test_crypto_candidate_demotes_stale_coinbase_spot(tmp_path) -> None:
    settings = _settings(tmp_path)
    row = {
        "strict_trade_eligible": True,
        "spot_feature_status": "stale",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "spot_stale_seconds": 30,
        "yes_ask_dollars": Decimal("0.4000"),
        "no_ask_dollars": Decimal("0.6000"),
        "spread_bps": 100,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.7000"), settings=settings)

    assert {candidate["candidate_status"] for candidate in candidates} == {"prediction_only_proxy_quote"}
    assert {candidate["reason"] for candidate in candidates} == {"spot_data_stale"}


def test_crypto_candidate_demotes_coingecko_proxy_spot(tmp_path) -> None:
    settings = _settings(tmp_path)
    row = {
        "strict_trade_eligible": True,
        "spot_feature_status": "available",
        "spot_provider": "coingecko",
        "spot_source_kind": "spot_price_proxy",
        "spot_stale_seconds": 30,
        "yes_ask_dollars": Decimal("0.4000"),
        "no_ask_dollars": Decimal("0.6000"),
        "spread_bps": 100,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.7000"), settings=settings)

    assert {candidate["candidate_status"] for candidate in candidates} == {"prediction_only_proxy_quote"}
    assert {candidate["reason"] for candidate in candidates} == {"spot_source_proxy_only"}


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
                await repo.upsert_crypto_spot_ohlc(
                    kalshi_env=settings.kalshi_env,
                    provider="coinbase",
                    asset_symbol=asset,
                    quote_currency="USD",
                    frequency="15m",
                    interval_seconds=900,
                    start_ts=close_time - timedelta(minutes=16),
                    end_ts=close_time - timedelta(minutes=1),
                    close_dollars=Decimal("100.00000000") + Decimal(idx),
                    observed_at=close_time - timedelta(minutes=1),
                    source_kind="spot_ohlc",
                    source_id=f"{asset}-USD",
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
async def test_crypto_history_paginates_recent_settled_markets_until_lookback(tmp_path) -> None:
    settings = _settings(tmp_path)
    now = datetime.now(UTC)
    series = CryptoSeries(
        series_ticker="KXBTC15M",
        title="BTC 15m",
        category="Crypto",
        frequency="15m",
        asset_symbol="BTC",
    )

    class _FakeKalshi:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def list_markets(self, **params):
            self.calls.append(params)
            if "cursor" not in params:
                return {
                    "markets": [
                        {
                            "ticker": "KXBTC15M-RECENT",
                            "series_ticker": "KXBTC15M",
                            "close_time": (now - timedelta(hours=1)).isoformat(),
                            "result": "yes",
                            "status": "finalized",
                        }
                    ],
                    "cursor": "older",
                }
            return {
                "markets": [
                    {
                        "ticker": "KXBTC15M-OLD",
                        "series_ticker": "KXBTC15M",
                        "close_time": (now - timedelta(days=5)).isoformat(),
                        "result": "no",
                        "status": "finalized",
                    }
                ]
            }

    fake = _FakeKalshi()
    result = await CryptoHistoryService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        kalshi=fake,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )._list_settled_markets(series, cutoff=now - timedelta(days=2), frequency="15m")

    assert result["pages_fetched"] == 2
    assert result["rows_seen"] == 2
    assert [market.market_ticker for market in result["markets"]] == ["KXBTC15M-RECENT"]
    assert fake.calls[0]["status"] == "settled"
    assert fake.calls[0]["series_ticker"] == "KXBTC15M"
    assert fake.calls[1]["cursor"] == "older"


@pytest.mark.asyncio
async def test_crypto_history_settled_pagination_does_not_assume_ordering(tmp_path) -> None:
    settings = _settings(tmp_path)
    now = datetime.now(UTC)
    series = CryptoSeries(
        series_ticker="KXETH15M",
        title="ETH 15m",
        category="Crypto",
        frequency="15m",
        asset_symbol="ETH",
    )

    class _FakeKalshi:
        async def list_markets(self, **params):
            if "cursor" not in params:
                return {
                    "markets": [
                        {
                            "ticker": "KXETH15M-OLD",
                            "series_ticker": "KXETH15M",
                            "close_time": (now - timedelta(days=5)).isoformat(),
                            "result": "no",
                            "status": "finalized",
                        }
                    ],
                    "cursor": "recent",
                }
            return {
                "markets": [
                    {
                        "ticker": "KXETH15M-RECENT",
                        "series_ticker": "KXETH15M",
                        "close_time": (now - timedelta(hours=1)).isoformat(),
                        "result": "yes",
                        "status": "finalized",
                    }
                ]
            }

    result = await CryptoHistoryService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        kalshi=_FakeKalshi(),  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )._list_settled_markets(series, cutoff=now - timedelta(days=2), frequency="15m")

    assert result["pages_fetched"] == 2
    assert [market.market_ticker for market in result["markets"]] == ["KXETH15M-RECENT"]


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
    assert len(fake_kalshi.live_calls) == 1
    assert len(fake_kalshi.historical_calls) == 1
    assert stored["source"] == "historical"
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
            self.historical_calls = 0
            self.live_calls = 0

        async def get_market_candlesticks(self, *args, **kwargs):
            del args, kwargs
            self.live_calls += 1
            return {"candlesticks": []}

        async def get_historical_market_candlesticks(self, *args, **kwargs):
            del args, kwargs
            self.historical_calls += 1
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

    assert result["status"] == "ok"
    assert result["stored"] == 0
    assert result["source"] == "historical"
    assert fake_kalshi.live_calls == 1
    assert fake_kalshi.historical_calls == 1


@pytest.mark.asyncio
async def test_crypto_history_collect_settled_appends_terminal_label_snapshot(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    close_time = datetime.now(UTC) - timedelta(minutes=15)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.record_crypto_market_snapshot(
            kalshi_env=settings.kalshi_env,
            series_ticker="KXBTC15M",
            market_ticker="KXBTC15M-JOIN",
            asset_symbol="BTC",
            frequency="15m",
            status="active",
            close_time=close_time,
            target_price_dollars=Decimal("100000.00000000"),
            yes_bid_dollars=Decimal("0.4200"),
            yes_ask_dollars=Decimal("0.4400"),
            no_ask_dollars=Decimal("0.5800"),
            observed_at=close_time - timedelta(minutes=3),
            source_kind="live_quote_evidence",
            payload={"unit": True},
        )
        await session.commit()

    class _FakeKalshi:
        async def list_markets(self, **params):
            assert params["status"] == "settled"
            return {
                "markets": [
                    {
                        "ticker": "KXBTC15M-JOIN",
                        "series_ticker": "KXBTC15M",
                        "close_time": close_time.isoformat(),
                        "expected_expiration_time": (close_time + timedelta(minutes=5)).isoformat(),
                        "settlement_ts": (close_time + timedelta(seconds=2)).isoformat(),
                        "result": "yes",
                        "status": "finalized",
                        "yes_sub_title": "Target Price: $100000",
                    }
                ]
            }

    class _FakeMarketService:
        async def discover_series(self, **kwargs) -> list[CryptoSeries]:
            assert kwargs["frequency"] == "15m"
            return [
                CryptoSeries(
                    series_ticker="KXBTC15M",
                    title="BTC 15m",
                    category="Crypto",
                    frequency="15m",
                    asset_symbol="BTC",
                )
            ]

        async def record_market_snapshot(self, repo, market: CryptoMarket, *, source_kind: str, observed_at: datetime):
            return await repo.record_crypto_market_snapshot(
                kalshi_env=settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                status=market.status,
                close_time=market.close_time,
                expected_expiration_time=market.expected_expiration_time,
                target_price_dollars=market.target_price_dollars,
                yes_bid_dollars=market.yes_bid_dollars,
                yes_ask_dollars=market.yes_ask_dollars,
                no_ask_dollars=market.no_ask_dollars,
                last_price_dollars=market.last_price_dollars,
                settlement_result=market.settlement_result,
                source_kind=source_kind,
                observed_at=observed_at,
                payload=market.to_payload(),
            )

    service = CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=_FakeKalshi(),  # type: ignore[arg-type]
        market_service=_FakeMarketService(),  # type: ignore[arg-type]
    )

    async def fake_capture_candles(repo, market: CryptoMarket, *, cutoff: datetime) -> dict[str, object]:
        del repo, market, cutoff
        return {"status": "error", "stored": 0, "source": "live", "error": "temporary candle gap"}

    service._capture_candles = fake_capture_candles  # type: ignore[method-assign]
    result = await service.collect_settled(days=2, frequency="15m", asset_symbols=["BTC"])

    async with session_factory() as session:
        repo = PlatformRepository(session)
        snapshots = await repo.list_crypto_market_snapshots(kalshi_env=settings.kalshi_env, limit=10)
        rows = _crypto_decision_rows(snapshots, [], settings=settings)
        await session.commit()

    assert result["status"] == "ok"
    assert result["settled_markets_stored"] == 1
    assert result["candle_capture"]["error_count"] == 1
    assert {snapshot.source_kind for snapshot in snapshots} == {"live_quote_evidence", "settled_backfill"}
    settled_snapshot = next(snapshot for snapshot in snapshots if snapshot.source_kind == "settled_backfill")
    assert settled_snapshot.settlement_result == "yes"
    observed_at = settled_snapshot.observed_at
    comparable_close_time = close_time if observed_at.tzinfo is not None else close_time.replace(tzinfo=None)
    assert observed_at > comparable_close_time
    assert len(rows) == 1
    assert rows[0]["strict_trade_eligible"] is True
    assert rows[0]["settlement_label_source"] == "joined_settled_snapshot"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_history_collect_settled_reports_zero_counts_for_requested_assets(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    close_time = datetime.now(UTC) - timedelta(minutes=15)
    assets = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"]

    class _FakeKalshi:
        async def list_markets(self, **params):
            if params["series_ticker"] != "KXBTC15M":
                return {"markets": []}
            return {
                "markets": [
                    {
                        "ticker": "KXBTC15M-JOIN",
                        "series_ticker": "KXBTC15M",
                        "close_time": close_time.isoformat(),
                        "expected_expiration_time": (close_time + timedelta(minutes=5)).isoformat(),
                        "result": "yes",
                        "status": "finalized",
                        "yes_sub_title": "Target Price: $100000",
                    }
                ]
            }

    class _FakeMarketService:
        async def discover_series(self, **kwargs) -> list[CryptoSeries]:
            assert kwargs["frequency"] == "15m"
            return [
                CryptoSeries(
                    series_ticker=f"KX{asset}15M" if asset != "XRP" else "KXXRP15M",
                    title=f"{asset} 15m",
                    category="Crypto",
                    frequency="15m",
                    asset_symbol=asset,
                )
                for asset in assets
            ]

        async def record_market_snapshot(self, repo, market: CryptoMarket, *, source_kind: str, observed_at: datetime):
            return await repo.record_crypto_market_snapshot(
                kalshi_env=settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                status=market.status,
                close_time=market.close_time,
                expected_expiration_time=market.expected_expiration_time,
                target_price_dollars=market.target_price_dollars,
                settlement_result=market.settlement_result,
                source_kind=source_kind,
                observed_at=observed_at,
                payload=market.to_payload(),
            )

    service = CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=_FakeKalshi(),  # type: ignore[arg-type]
        market_service=_FakeMarketService(),  # type: ignore[arg-type]
    )

    async def fake_capture_candles(repo, market: CryptoMarket, *, cutoff: datetime) -> dict[str, object]:
        del repo, market, cutoff
        return {"status": "ok", "stored": 0, "source": "live"}

    service._capture_candles = fake_capture_candles  # type: ignore[method-assign]
    result = await service.collect_settled(days=2, frequency="15m", asset_symbols=assets)

    assert result["status"] == "ok"
    assert result["settled_markets_stored"] == 1
    assert result["asset_symbols"] == ["BNB", "BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"]
    assert result["asset_counts"] == {
        "BNB": 0,
        "BTC": 1,
        "DOGE": 0,
        "ETH": 0,
        "HYPE": 0,
        "SOL": 0,
        "XRP": 0,
    }
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_history_status_reports_assets_missing_settled_labels(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    observed_at = datetime.now(UTC) - timedelta(minutes=10)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.record_crypto_market_snapshot(
            kalshi_env=settings.kalshi_env,
            series_ticker="KXETH15M",
            market_ticker="KXETH15M-MISSING-LABEL",
            asset_symbol="ETH",
            frequency="15m",
            status="active",
            close_time=observed_at + timedelta(minutes=5),
            target_price_dollars=Decimal("3000.00000000"),
            yes_bid_dollars=Decimal("0.4200"),
            yes_ask_dollars=Decimal("0.4500"),
            no_ask_dollars=Decimal("0.5800"),
            observed_at=observed_at,
            source_kind="live_quote_evidence",
            payload={"unit": True},
        )
        await session.commit()

    history_status = await CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=None,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    ).status(frequency="15m", days=1)

    quote_evidence = history_status["quote_evidence"]
    assert "ETH" in quote_evidence["assets_missing_settled_markets"]
    eth_audit = quote_evidence["strict_quote_ingestion_audit_by_asset"]["ETH"]
    assert eth_audit["snapshot_present"] == 1
    assert eth_audit["real_bid_ask_present"] == 1
    assert eth_audit["settled_label_joined"] == 0
    assert eth_audit["blocker_stage"] == "missing_settled_label"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_history_bootstrap_scopes_requested_assets(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    class _FakeMarketService:
        def __init__(self) -> None:
            self.markets = [
                _market(market_ticker="KXBTC15M-OPEN", series_ticker="KXBTC15M", asset_symbol="BTC"),
                _market(market_ticker="KXXRP15M-OPEN", series_ticker="KXXRP15M", asset_symbol="XRP"),
            ]
            self.series = [
                CryptoSeries(
                    series_ticker="KXBTC15M",
                    title="BTC 15m",
                    category="Crypto",
                    frequency="15m",
                    asset_symbol="BTC",
                ),
                CryptoSeries(
                    series_ticker="KXXRP15M",
                    title="XRP 15m",
                    category="Crypto",
                    frequency="15m",
                    asset_symbol="XRP",
                ),
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            assert kwargs["status"] == "open"
            assert kwargs["persist"] is True
            return self.markets

        async def discover_series(self, **kwargs) -> list[CryptoSeries]:
            assert kwargs["frequency"] == "15m"
            return self.series

        async def record_market_snapshot(self, repo, market: CryptoMarket, *, source_kind: str, observed_at: datetime):
            return await repo.record_crypto_market_snapshot(
                kalshi_env=settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                status=market.status,
                close_time=market.close_time,
                target_price_dollars=market.target_price_dollars,
                yes_bid_dollars=market.yes_bid_dollars,
                yes_ask_dollars=market.yes_ask_dollars,
                no_ask_dollars=market.no_ask_dollars,
                last_price_dollars=market.last_price_dollars,
                source_kind=source_kind,
                observed_at=observed_at,
                payload=market.to_payload(),
            )

    service = CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=object(),  # type: ignore[arg-type]
        market_service=_FakeMarketService(),  # type: ignore[arg-type]
    )
    historical_calls: list[str] = []
    captured_assets: list[str] = []

    async def fake_list_historical_markets(series_ticker: str) -> dict[str, object]:
        historical_calls.append(series_ticker)
        return {"rows": [], "errors": [], "pages_fetched": 0, "rows_seen": 0}

    async def fake_capture_candles(repo, market: CryptoMarket, *, cutoff: datetime) -> dict[str, object]:
        del repo, cutoff
        captured_assets.append(market.asset_symbol)
        return {"status": "ok", "stored": 0}

    service._list_historical_markets = fake_list_historical_markets  # type: ignore[method-assign]
    service._capture_candles = fake_capture_candles  # type: ignore[method-assign]

    result = await service.bootstrap(days=10, frequency="15m", asset_symbols=["xrp"])

    assert result["asset_symbols"] == ["XRP"]
    assert result["markets_stored"] == 1
    assert result["series"] == [
        {
            "series_ticker": "KXXRP15M",
            "asset_symbol": "XRP",
            "pages_fetched": 0,
            "rows_seen": 0,
            "markets_in_window": 0,
            "errors": [],
        }
    ]
    assert historical_calls == ["KXXRP15M"]
    assert captured_assets == ["XRP"]
    assert set(result["data_quality"]["assets"]) == {"XRP"}
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_history_collect_open_stores_real_quote_snapshots_only(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    class _FakeMarketService:
        def __init__(self) -> None:
            self.markets = [
                _market(market_ticker="KXBTC15M-REAL", asset_symbol="BTC"),
                _market(market_ticker="KXETH15M-MISSING", asset_symbol="ETH", yes_bid_dollars=None),
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            assert kwargs["status"] == "open"
            assert kwargs["persist"] is False
            return self.markets

        async def record_market_snapshot(self, repo, market: CryptoMarket, *, source_kind: str, observed_at: datetime):
            return await repo.record_crypto_market_snapshot(
                kalshi_env=settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                status=market.status,
                close_time=market.close_time,
                target_price_dollars=market.target_price_dollars,
                yes_bid_dollars=market.yes_bid_dollars,
                yes_ask_dollars=market.yes_ask_dollars,
                no_ask_dollars=market.no_ask_dollars,
                last_price_dollars=market.last_price_dollars,
                source_kind=source_kind,
                observed_at=observed_at,
                payload=market.to_payload(),
            )

    result = await CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=object(),  # type: ignore[arg-type]
        market_service=_FakeMarketService(),  # type: ignore[arg-type]
    ).collect_open(frequency="15m")

    async with session_factory() as session:
        repo = PlatformRepository(session)
        snapshots = await repo.list_crypto_market_snapshots(kalshi_env=settings.kalshi_env, limit=10)
        await session.commit()

    assert result["status"] == "ok"
    assert result["stored_real_quote_snapshots"] == 1
    assert result["skipped"][0]["reason"] == "missing_real_bid_ask"
    assert len(snapshots) == 1
    assert snapshots[0].source_kind == "live_quote_evidence"
    assert snapshots[0].market_ticker == "KXBTC15M-REAL"
    await engine.dispose()


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


class _FakeBaseExecutionStatus:
    def __init__(self, statuses: list[str]) -> None:
        self.statuses = list(statuses)
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        status = self.statuses.pop(0)
        return ExecReceiptPayload(
            status=status,
            client_order_id=kwargs["client_order_id"],
            details={"called": True},
        )


class _FakeFixedLimitExecution(_FakeBaseExecution):
    def __init__(self) -> None:
        super().__init__()
        self.fixed_calls: list[dict[str, object]] = []

    async def execute_fixed_limit_until_close(self, **kwargs) -> ExecReceiptPayload:
        self.fixed_calls.append(kwargs)
        return ExecReceiptPayload(
            status="submitted",
            external_order_id="order-last-minute",
            client_order_id=kwargs["client_order_id"],
            details={
                "order": {
                    "order_id": "order-last-minute",
                    "client_order_id": kwargs["client_order_id"],
                }
            },
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
    def __init__(self, signal: StrategySignal | None = None) -> None:
        self.signal = signal

    async def forecast(self, market: CryptoMarket) -> StrategySignal:
        del market
        return self.signal or _signal()


class _RecordingCryptoExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        return ExecReceiptPayload(
            status="submitted",
            external_order_id="crypto-dynamic-order-1",
            client_order_id=kwargs["client_order_id"],
            details={"called": True},
        )


class _FailingCryptoExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        raise RuntimeError("exchange side effect failed after approval")


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
    current_spot_calls: list[dict[str, object]] = []

    class _SpotService:
        async def collect_current(self, **kwargs: object) -> dict[str, object]:
            current_spot_calls.append(kwargs)
            return {"status": "ok", "stored": 1}

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
        now = datetime.now(UTC)
        bucket_matrix = [
            _empirical_bucket(
                "BTC|yes|0.50-0.75|tight|5_10m",
                sample_count=25,
                net_pnl="3.0000",
                win_rate=0.64,
            )
        ]
        for artifact_type, version, status in (
            ("backtest:BTC", "test-backtest", "pass"),
            ("replay_gate:BTC", "test-gate", "passed"),
        ):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=artifact_type,
                version=version,
                status=status,
                sample_count=25,
                metrics={"bucket_matrix": bucket_matrix, "allowed_bucket_keys": ["BTC|yes|0.50-0.75|tight|5_10m"]},
                payload={"bucket_matrix": bucket_matrix},
                kalshi_env=settings.kalshi_env,
                trained_at=now,
            )
        await repo.upsert_crypto_spot_ohlc(
            kalshi_env=settings.kalshi_env,
            provider="coinbase",
            asset_symbol="BTC",
            quote_currency="USD",
            frequency="15m",
            interval_seconds=900,
            start_ts=now - timedelta(minutes=15),
            end_ts=now,
            close_dollars=Decimal("76460.00000000"),
            observed_at=now,
            source_kind="spot_ohlc",
            source_id="BTC-USD",
            payload={"unit": True},
        )
        await session.commit()

    signal = await CryptoForecastService(
        settings=settings,
        session_factory=session_factory,
        spot_service=_SpotService(),  # type: ignore[arg-type]
    ).forecast(
        _market(
            open_time=now - timedelta(minutes=5),
            close_time=now + timedelta(minutes=10),
            yes_bid_dollars=Decimal("0.5000"),
            yes_ask_dollars=Decimal("0.5000"),
            no_ask_dollars=Decimal("0.5000"),
            last_price_dollars=Decimal("0.5000"),
        )
    )

    assert signal.recommended_side == ContractSide.YES
    assert signal.edge_bps > 50
    assert signal.candidate_trace["model_version"] == "test-model"
    assert current_spot_calls == [{"frequency": "15m", "asset_symbols": ["BTC"]}]
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
    assert model["candidate_report"]["primary_metric"] == "oos_candidate_net_pnl"
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

    assert report["primary_metric"] == "oos_candidate_net_pnl"
    assert report["selection_scope"] == "walk_forward_time_ordered"
    assert report["champion_name"] in names
    assert names["market_mid_baseline"]["metrics"]["brier"] is not None
    assert names["sklearn_logistic"]["status"] in {"available", "guardrail_failed"}
    assert names["xgboost_classifier"]["status"] in {"available", "unavailable", "guardrail_failed"}
    assert names["lightgbm_classifier"]["status"] in {"available", "unavailable", "guardrail_failed"}


def test_crypto_model_selection_prefers_non_market_candidate_over_market_mid_baseline() -> None:
    champion = _crypto_select_champion(
        [
            {
                "name": "market_mid_baseline",
                "status": "available",
                "metrics": {"brier": 0.0500},
            },
            {
                "name": "spot_distance_residual",
                "status": "guardrail_failed",
                "metrics": {"brier": 0.0600},
                "reason": "log_loss_regressed_vs_market_mid",
            },
            {
                "name": "sklearn_logistic",
                "status": "unavailable",
                "metrics": None,
            },
        ]
    )

    assert champion == "spot_distance_residual"


def test_crypto_model_selection_falls_back_to_market_mid_when_no_trade_model_is_usable() -> None:
    champion = _crypto_select_champion(
        [
            {
                "name": "market_mid_baseline",
                "status": "available",
                "metrics": {"brier": 0.0500},
            },
            {
                "name": "sklearn_logistic",
                "status": "unavailable",
                "metrics": None,
            },
        ]
    )

    assert champion == "market_mid_baseline"


def test_crypto_model_selection_prefers_positive_oos_pnl_over_brier() -> None:
    champion = _crypto_select_champion(
        [
            {
                "name": "market_mid_baseline",
                "status": "available",
                "metrics": {"brier": 0.0300},
                "policy_metrics": {
                    "selected_count": 8,
                    "net_pnl": "0.0000",
                    "pnl_advantage_vs_market_mid_dollars": "0.0000",
                },
            },
            {
                "name": "spot_distance_residual",
                "status": "available",
                "metrics": {"brier": 0.0900},
                "policy_metrics": {
                    "selected_count": 3,
                    "net_pnl": "2.0000",
                    "pnl_advantage_vs_market_mid_dollars": "2.0000",
                },
            },
            {
                "name": "sklearn_logistic",
                "status": "available",
                "metrics": {"brier": 0.0400},
                "policy_metrics": {
                    "selected_count": 3,
                    "net_pnl": "-1.0000",
                    "pnl_advantage_vs_market_mid_dollars": "-1.0000",
                },
            },
        ]
    )

    assert champion == "spot_distance_residual"


def test_crypto_entry_optimizer_stages_only_passing_policy(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_min_training_samples=2,
        crypto_replay_min_trade_candidates=1,
        crypto_replay_require_pnl_beats_market_mid=False,
        risk_min_edge_bps=750,
    )
    policy = AgentPackService(settings).runtime_crypto_policy()
    rows = [
        _replay_row(
            market_day=f"2026-05-0{day}",
            market_ticker=f"KXBTC15M-OPT-{day}",
            mid_yes_dollars=Decimal("0.5000"),
            yes_bid_dollars=Decimal("0.4900"),
            yes_ask_dollars=Decimal("0.5000"),
            no_ask_dollars=Decimal("0.5100"),
            spread_bps=100,
            label_yes=1,
        )
        for day in range(1, 5)
    ]

    result = _crypto_optimize_asset_entry_policy("BTC", rows, settings=settings, crypto_policy=policy)

    assert result["status"] == "stageable"
    assert result["winner"]["entry_policy"]["min_fee_adjusted_edge_bps"] in {750, 1000, 1500, 2500, 5000}
    assert result["staged_override_payload"]["crypto_policy"]["asset_entry_overrides"]["BTC"]


def test_crypto_entry_optimizer_leaves_policy_unchanged_when_no_policy_passes(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_min_training_samples=2,
        crypto_replay_min_trade_candidates=50,
    )
    policy = AgentPackService(settings).runtime_crypto_policy()
    rows = [
        _replay_row(
                market_day=f"2026-05-0{day}",
                market_ticker=f"KXBTC15M-OPT-BLOCK-{day}",
                mid_yes_dollars=Decimal("0.7000"),
                yes_bid_dollars=Decimal("0.4900"),
                yes_ask_dollars=Decimal("0.5000"),
                no_ask_dollars=Decimal("0.5100"),
            spread_bps=100,
            label_yes=1,
        )
        for day in range(1, 5)
    ]

    result = _crypto_optimize_asset_entry_policy("BTC", rows, settings=settings, crypto_policy=policy)

    assert result["status"] == "blocked"
    assert result["staged_override_payload"] is None
    assert "oos_trade_candidate_count" in result["blockers"][0]


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
    settings = _settings(tmp_path, risk_min_edge_bps=500, crypto_empirical_bucket_gate_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-CAND",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4800"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    live = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings)
    exploratory = _crypto_trade_candidates(row, Decimal("0.5000"), settings=settings)

    assert live[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert exploratory[0]["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert exploratory[0]["live_eligible"] is False


def test_crypto_candidate_quality_blocks_live_before_entry_window(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=750, crypto_live_min_market_age_seconds=180)
    row = {
        "market_ticker": "KXBTC15M-EARLY",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 0,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 780,
        "market_age_seconds": 120,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert candidates[0]["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert candidates[0]["reason"] == "crypto_market_too_early_for_live_entry"
    assert candidates[0]["live_eligible"] is False


def test_crypto_candidate_quality_blocks_live_inside_final_window(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=750, crypto_autonomy_min_seconds_to_close=120)
    row = {
        "market_ticker": "KXBTC15M-LATE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 0,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.8000"), settings=settings)

    assert candidates[0]["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert candidates[0]["reason"] == "crypto_market_too_late_for_live_entry"
    assert candidates[0]["live_eligible"] is False


def test_crypto_candidate_quality_allows_late_high_confidence_directional_entry(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_max_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-SURE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.0500"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert no_candidate["reason"] == "late_high_confidence_directional_entry"
    assert no_candidate["edge_bps"] < 0
    assert no_candidate["expected_net_edge"].startswith("-")
    assert no_candidate["live_entry_window_reason"] == "crypto_market_too_late_for_live_entry"
    assert no_candidate["late_high_confidence_directional_entry"] is True


def test_crypto_late_high_confidence_default_probability_floor_is_85(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-DEFAULT-PROBABILITY",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.8500"),
        "yes_bid_dollars": Decimal("0.8400"),
        "yes_ask_dollars": Decimal("0.8600"),
        "no_ask_dollars": Decimal("0.1600"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 120,
        "market_age_seconds": 780,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.8500"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert settings.crypto_late_sure_thing_min_probability == 0.85
    assert yes_candidate["market_anchored_probability"] == "0.8500"
    assert yes_candidate["reason"] == "late_high_confidence_directional_entry"
    assert yes_candidate["late_high_confidence_directional_entry"] is True


def test_crypto_late_high_confidence_requires_90_probability_from_180_to_300_seconds(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_standard_max_seconds_to_close=180,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_extended_min_probability=0.90,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-EXTENDED-TOO-LOW",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.8900"),
        "yes_bid_dollars": Decimal("0.8800"),
        "yes_ask_dollars": Decimal("0.9000"),
        "no_ask_dollars": Decimal("0.1200"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.8900"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["market_anchored_probability"] == "0.8900"
    assert yes_candidate["reason"] != "late_high_confidence_directional_entry"
    assert yes_candidate["candidate_status"] != CRYPTO_LIVE_QUALITY
    assert yes_candidate["late_high_confidence_directional_entry"] is False


def test_crypto_late_high_confidence_allows_90_probability_from_180_to_300_seconds(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_standard_max_seconds_to_close=180,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_extended_min_probability=0.90,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-EXTENDED-STRICT",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.9000"),
        "yes_bid_dollars": Decimal("0.8900"),
        "yes_ask_dollars": Decimal("0.9100"),
        "no_ask_dollars": Decimal("0.1100"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["market_anchored_probability"] == "0.9000"
    assert yes_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert yes_candidate["reason"] == "late_high_confidence_directional_entry"
    assert yes_candidate["late_high_confidence_directional_entry"] is True


def test_crypto_late_high_confidence_blocks_extended_window_adverse_momentum(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_standard_max_seconds_to_close=180,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_extended_min_probability=0.90,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXDOGE15M-LATE-REVERSAL",
        "asset_symbol": "DOGE",
        "mid_yes_dollars": Decimal("0.1000"),
        "yes_bid_dollars": Decimal("0.0900"),
        "yes_ask_dollars": Decimal("0.1100"),
        "no_ask_dollars": Decimal("0.9100"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
        "spot_return_1_pct": Decimal("0.0002"),
        "spot_return_3_pct": Decimal("-0.0001"),
        "spot_target_distance_volatility": Decimal("-4.0000"),
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.1000"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["candidate_status"] == "blocked_late_reversal_risk"
    assert no_candidate["reason"] == "late_high_confidence_reversal_risk_guard"
    assert no_candidate["late_high_confidence_directional_entry"] is False
    review = no_candidate["late_high_confidence_reversal_risk"]
    assert review["blocked"] is True
    assert review["reason"] == "extended_window_adverse_momentum"
    assert review["adverse_return_count"] == 1


def test_crypto_late_high_confidence_blocks_target_distance_below_margin(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=0,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_min_market_probability=0.75,
        crypto_late_sure_thing_min_target_distance_volatility=3.0,
    )
    row = {
        "market_ticker": "KXETH15M-LATE-TARGET-MARGIN",
        "asset_symbol": "ETH",
        "mid_yes_dollars": Decimal("0.1000"),
        "yes_bid_dollars": Decimal("0.0900"),
        "yes_ask_dollars": Decimal("0.1100"),
        "no_ask_dollars": Decimal("0.9100"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 120,
        "market_age_seconds": 780,
        "spot_return_1_pct": Decimal("-0.0002"),
        "spot_return_3_pct": Decimal("-0.0001"),
        "spot_target_distance_volatility": Decimal("-2.5000"),
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.1000"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["candidate_status"] == "blocked_late_reversal_risk"
    assert no_candidate["reason"] == "late_high_confidence_reversal_risk_guard"
    review = no_candidate["late_high_confidence_reversal_risk"]
    assert review["blocked"] is True
    assert review["reason"] == "target_distance_volatility_below_min"
    assert review["target_distance_ok"] is False


def test_crypto_late_high_confidence_blocks_near_strike_adverse_momentum_below_90(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=0,
        crypto_late_sure_thing_standard_max_seconds_to_close=180,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_near_strike_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXXRP15M-NEAR-STRIKE-ADVERSE",
        "asset_symbol": "XRP",
        "mid_yes_dollars": Decimal("0.1055"),
        "yes_bid_dollars": Decimal("0.0910"),
        "yes_ask_dollars": Decimal("0.1200"),
        "no_ask_dollars": Decimal("0.9090"),
        "spread_bps": 290,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 79,
        "market_age_seconds": 821,
        "spot_moneyness_pct": Decimal("-0.000035"),
        "spot_return_1_pct": Decimal("0.000698"),
        "spot_return_3_pct": Decimal("0.000558"),
        "spot_return_6_pct": Decimal("0.000558"),
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.1057"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["model_probability"] == "0.8944"
    assert no_candidate["candidate_status"] == "blocked_near_strike_momentum"
    assert no_candidate["reason"] == "late_high_confidence_near_strike_momentum_guard"
    assert no_candidate["late_high_confidence_directional_entry"] is False
    guard = no_candidate["late_high_confidence_near_strike_momentum_guard"]
    assert guard["blocked"] is True
    assert guard["reason"] == "near_strike_adverse_momentum_probability_below_min"
    assert guard["adverse_return_count"] >= 2


def test_crypto_late_high_confidence_allows_near_strike_adverse_momentum_at_90(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=0,
        crypto_late_sure_thing_standard_max_seconds_to_close=180,
        crypto_late_sure_thing_min_probability=0.85,
        crypto_late_sure_thing_near_strike_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXXRP15M-NEAR-STRIKE-ADVERSE-OK",
        "asset_symbol": "XRP",
        "mid_yes_dollars": Decimal("0.1000"),
        "yes_bid_dollars": Decimal("0.0910"),
        "yes_ask_dollars": Decimal("0.1200"),
        "no_ask_dollars": Decimal("0.9090"),
        "spread_bps": 290,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 79,
        "market_age_seconds": 821,
        "spot_moneyness_pct": Decimal("-0.000035"),
        "spot_return_1_pct": Decimal("0.000698"),
        "spot_return_3_pct": Decimal("0.000558"),
        "spot_return_6_pct": Decimal("0.000558"),
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.1000"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["model_probability"] == "0.9000"
    assert no_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert no_candidate["reason"] == "late_high_confidence_directional_entry"
    assert no_candidate["late_high_confidence_directional_entry"] is True
    assert no_candidate["late_high_confidence_near_strike_momentum_guard"]["reason"] == (
        "near_strike_adverse_momentum_probability_allowed"
    )


def test_crypto_candidate_quality_allows_late_market_confirmed_edge_bypass_before_no_entry_cutoff(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_min_probability=0.90,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-MARKET-CONFIRMED",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.9650"),
        "yes_bid_dollars": Decimal("0.9600"),
        "yes_ask_dollars": Decimal("0.9700"),
        "no_ask_dollars": Decimal("0.0400"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9500"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert yes_candidate["reason"] == "late_high_confidence_directional_entry"
    assert yes_candidate["edge_bps"] < 0
    assert yes_candidate["expected_net_edge"].startswith("-")
    assert yes_candidate["market_side_probability"] == "0.9650"
    assert yes_candidate["live_entry_window_reason"] is None
    assert yes_candidate["late_high_confidence_directional_entry"] is True


def test_crypto_late_high_confidence_requires_market_price_confirmation(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=5000,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_max_seconds_to_close=300,
        crypto_late_sure_thing_min_probability=0.90,
        crypto_late_sure_thing_min_market_probability=0.75,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-MODEL-ONLY",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9500"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] != CRYPTO_LIVE_QUALITY
    assert yes_candidate["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert yes_candidate["reason"] == "broad_shadow_exploration"
    assert yes_candidate["market_side_probability"] == "0.5000"
    assert yes_candidate["late_high_confidence_directional_entry"] is False


def _empirical_bucket(
    bucket_key: str,
    *,
    sample_count: int = 20,
    net_pnl: str = "1.0000",
    win_rate: float = 0.60,
    outcome_win_rate: float | None = None,
    net_positive_rate: float | None = None,
) -> dict[str, object]:
    bucket: dict[str, object] = {
        "bucket_key": bucket_key,
        "asset_symbol": bucket_key.split("|", 1)[0],
        "sample_count": sample_count,
        "net_pnl": net_pnl,
        "win_rate": win_rate,
    }
    if outcome_win_rate is not None:
        bucket["outcome_win_rate"] = outcome_win_rate
        bucket["win_rate_basis"] = "settlement_outcome"
    if net_positive_rate is not None:
        bucket["net_positive_rate"] = net_positive_rate
    return bucket


def test_crypto_bucket_matrix_win_rate_tracks_outcome_not_net_positive(tmp_path) -> None:
    settings = _settings(tmp_path)
    rows = [
        {
            **_replay_row(
                market_day=f"2026-05-0{idx + 1}",
                label_yes=1 if idx < 2 else 0,
                yes_ask_dollars=Decimal("0.9900"),
                spread_bps=0,
                time_to_close_seconds=60,
            ),
            "simulation": {
                "side": "yes",
                "execution_price_dollars": "0.9900",
                "gross_pnl": "0.0100" if idx < 2 else "-0.9900",
                "fees": "0.0100",
                "net_pnl": "0.0000" if idx < 2 else "-1.0000",
            },
        }
        for idx in range(3)
    ]

    bucket = _crypto_bucket_matrix(rows, settings=settings)[0]

    assert bucket["bucket_key"] == "BTC|yes|0.75-1.00|tight|0_5m"
    assert bucket["win_rate"] == pytest.approx(2 / 3)
    assert bucket["outcome_win_rate"] == pytest.approx(2 / 3)
    assert bucket["net_positive_rate"] == pytest.approx(0.0)
    assert bucket["win_rate_basis"] == "settlement_outcome"


def test_crypto_bucket_key_includes_time_to_close_bucket(tmp_path) -> None:
    settings = _settings(tmp_path)
    del settings
    row = _replay_row(time_to_close_seconds=601, spread_bps=200)

    key = _crypto_bucket_key(row, {"side": "yes", "execution_price_dollars": "0.5000"})

    assert key == "BTC|yes|0.50-0.75|normal|10_15m"


def test_crypto_empirical_bucket_gate_blocks_missing_or_bad_buckets(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500, crypto_empirical_bucket_gate_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-BUCKET-GATE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }
    bucket_key = _crypto_bucket_key(row, {"side": "yes", "execution_price_dollars": "0.5000"})

    for matrix, reason in (
        ([], "empirical_bucket_missing"),
        ([_empirical_bucket(bucket_key, sample_count=19)], "empirical_bucket_under_sampled"),
        ([_empirical_bucket(bucket_key, net_pnl="-0.0100")], "empirical_bucket_negative_pnl"),
        ([_empirical_bucket(bucket_key, win_rate=0.50)], "empirical_bucket_low_win_rate"),
    ):
        candidates = _crypto_trade_candidates(
            row,
            Decimal("0.9000"),
            settings=settings,
            empirical_bucket_matrix=matrix,  # type: ignore[arg-type]
            enforce_empirical_bucket_gate=True,
        )
        yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")
        assert yes_candidate["candidate_status"] == "blocked_empirical_bucket"
        assert yes_candidate["reason"] == "empirical_bucket_not_allowed"
        assert yes_candidate["empirical_bucket_gate"]["reason"] == reason
        assert yes_candidate["live_eligible"] is False


def test_crypto_empirical_bucket_gate_uses_outcome_win_rate_when_present(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500, crypto_empirical_bucket_gate_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-BUCKET-GATE-OUTCOME",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.9900"),
        "yes_bid_dollars": Decimal("0.9900"),
        "yes_ask_dollars": Decimal("0.9900"),
        "no_ask_dollars": Decimal("0.0100"),
        "spread_bps": 0,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }
    bucket_key = _crypto_bucket_key(row, {"side": "yes", "execution_price_dollars": "0.9900"})

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.9990"),
        settings=settings,
        empirical_bucket_matrix=[
            _empirical_bucket(
                bucket_key,
                net_pnl="0.0100",
                win_rate=0.0,
                outcome_win_rate=0.95,
                net_positive_rate=0.0,
            )
        ],
        enforce_empirical_bucket_gate=True,
    )
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert yes_candidate["empirical_bucket_gate"]["reason"] == "empirical_bucket_allowed"
    assert yes_candidate["empirical_bucket_gate"]["win_rate"] == pytest.approx(0.95)
    assert yes_candidate["empirical_bucket_gate"]["net_positive_rate"] == pytest.approx(0.0)


def test_crypto_empirical_bucket_gate_defaults_to_dynamic_live_assets(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    policy = AgentPackService(settings).runtime_crypto_policy(
        AgentPackService(settings).default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    live=AgentPackCryptoLivePolicy(asset_modes={"HYPE": "live"})
                )
            }
        )
    )
    row = {
        "market_ticker": "KXHYPE15M-BUCKET-GATE",
        "asset_symbol": "HYPE",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.9000"),
        settings=settings,
        crypto_policy=policy,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert settings.crypto_empirical_bucket_gate_assets == "live"
    assert yes_candidate["candidate_status"] == "blocked_empirical_bucket"
    assert yes_candidate["reason"] == "empirical_bucket_not_allowed"
    assert yes_candidate["empirical_bucket_gate"]["reason"] == "empirical_bucket_missing"


def test_crypto_empirical_bucket_gate_applies_to_requested_shadow_replay_asset(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    policy = AgentPackService(settings).runtime_crypto_policy(
        AgentPackService(settings).default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    live=AgentPackCryptoLivePolicy(asset_modes={"SOL": "shadow"})
                )
            }
        )
    )
    row = {
        "market_ticker": "KXSOL15M-BUCKET-GATE",
        "asset_symbol": "SOL",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.9000"),
        settings=settings,
        crypto_policy=policy,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
        empirical_bucket_requested_assets=["SOL"],
        force_empirical_bucket_for_requested_assets=True,
    )
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] == "blocked_empirical_bucket"
    assert yes_candidate["empirical_bucket_gate"]["reason"] == "empirical_bucket_missing"


def test_crypto_empirical_bucket_gate_preserves_explicit_asset_csv_scope(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500, crypto_empirical_bucket_gate_assets="BTC")
    row = {
        "market_ticker": "KXHYPE15M-BUCKET-GATE",
        "asset_symbol": "HYPE",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4900"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.9000"),
        settings=settings,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert yes_candidate["empirical_bucket_gate"]["reason"] == "asset_not_configured_for_empirical_bucket_gate"


def test_crypto_last_minute_passive_bid_map_parses_v1_thresholds(tmp_path) -> None:
    settings = _settings(tmp_path)

    bids = _crypto_last_minute_passive_bid_by_asset(settings)

    assert bids == {
        "BTC": Decimal("0.55"),
        "ETH": Decimal("0.54"),
        "XRP": Decimal("0.54"),
        "SOL": Decimal("0.63"),
        "DOGE": Decimal("0.65"),
        "BNB": Decimal("0.77"),
        "HYPE": Decimal("0.84"),
    }


def test_crypto_last_minute_passive_uses_market_confidence_over_model_winner(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_last_minute_passive_assets="live",
        crypto_empirical_bucket_gate_assets="BTC",
    )
    policy = AgentPackService(settings).runtime_crypto_policy(
        AgentPackService(settings).default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    live=AgentPackCryptoLivePolicy(asset_modes={"BTC": "live"})
                )
            }
        )
    )
    row = {
        "market_ticker": "KXBTC15M-LAST-MINUTE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.7000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.6000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 1000,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 45,
        "market_age_seconds": 855,
    }

    action, side, target_yes, edge_bps, trace = _crypto_recommendation(
        market=_market(),
        fair_yes=Decimal("0.2000"),
        settings=settings,
        crypto_policy=policy,
        row=row,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )

    assert action == TradeAction.BUY
    assert side == ContractSide.YES
    assert target_yes == Decimal("0.5500")
    assert edge_bps == 1500
    assert trace["selection_reason"] == CRYPTO_LAST_MINUTE_PASSIVE_REASON
    assert trace["last_minute_passive_market_confidence"] is True
    assert trace["empirical_bucket_gate"]["reason"] == "empirical_bucket_missing"
    assert trace["candidate_status"] == CRYPTO_LIVE_QUALITY


def test_crypto_last_minute_passive_no_side_converts_bid_to_yes_price(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_last_minute_passive_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-LAST-MINUTE-NO",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.3000"),
        "yes_bid_dollars": Decimal("0.4000"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.6000"),
        "spread_bps": 1000,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 30,
        "market_age_seconds": 870,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert no_candidate["reason"] == CRYPTO_LAST_MINUTE_PASSIVE_REASON
    assert no_candidate["execution_price_dollars"] == "0.5500"
    assert no_candidate["target_yes_price_dollars"] == "0.4500"


def test_crypto_last_minute_passive_blocks_when_bid_would_cross_touch(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_last_minute_passive_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-LAST-MINUTE-CROSS",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.7000"),
        "yes_bid_dollars": Decimal("0.5300"),
        "yes_ask_dollars": Decimal("0.5400"),
        "no_ask_dollars": Decimal("0.4700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 30,
        "market_age_seconds": 870,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.2000"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] == "blocked_last_minute_passive"
    assert yes_candidate["reason"] == "last_minute_passive_would_cross_touch"
    assert yes_candidate["last_minute_passive_market_confidence"] is False


def test_crypto_last_minute_passive_stays_inside_final_60_seconds(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_last_minute_passive_assets="BTC")
    row = {
        "market_ticker": "KXBTC15M-LAST-MINUTE-OUTSIDE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.7000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.6000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 1000,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 61,
        "market_age_seconds": 839,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.2000"), settings=settings)
    yes_candidate = next(candidate for candidate in candidates if candidate["side"] == "yes")

    assert yes_candidate["candidate_status"] != CRYPTO_LIVE_QUALITY
    assert yes_candidate["last_minute_passive"]["reason"] == "outside_last_minute_passive_window"


def test_late_high_confidence_candidate_still_requires_empirical_bucket(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_empirical_bucket_gate_assets="BTC",
        crypto_empirical_late_override_enabled=False,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-BUCKET",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.0500"),
        settings=settings,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["late_high_confidence_directional_entry"] is True
    assert no_candidate["pre_empirical_reason"] == "late_high_confidence_directional_entry"
    assert no_candidate["candidate_status"] == "blocked_empirical_bucket"
    assert no_candidate["reason"] == "empirical_bucket_not_allowed"


def test_late_high_confidence_overrides_missing_empirical_bucket_when_near_close(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_empirical_bucket_gate_assets="BTC",
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-BUCKET-MISSING",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.0500"),
        settings=settings,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["late_high_confidence_directional_entry"] is True
    assert no_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert no_candidate["reason"] == "late_high_confidence_directional_entry"
    assert no_candidate["empirical_bucket_status"] == "override_allowed"
    assert no_candidate["empirical_bucket_gate"]["original_reason"] == "empirical_bucket_missing"
    assert no_candidate["empirical_bucket_gate"]["late_override"]["max_count_fp"] == "1.00"
    assert no_candidate["empirical_bucket_gap_sample"]["schema_version"] == "crypto-empirical-gap-sample-v1"
    assert no_candidate["empirical_bucket_gap_sample"]["gap_analysis_candidate"] is True
    assert no_candidate["empirical_bucket_gap_sample"]["late_override_allowed"] is True
    assert no_candidate["empirical_bucket_gap_sample"]["dedupe_key"] == (
        "KXBTC15M-LATE-BUCKET-MISSING|no|BTC|no|0.75-1.00|tight|0_5m|"
        "late_high_confidence_directional_entry"
    )
    assert no_candidate["live_eligible"] is True


def test_late_high_confidence_overrides_low_win_rate_bucket_with_positive_pnl(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_empirical_bucket_gate_assets="BTC",
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-BUCKET-LOW-WIN",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }
    bucket_key = _crypto_bucket_key(row, {"side": "no", "execution_price_dollars": "0.9700"})

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.0500"),
        settings=settings,
        empirical_bucket_matrix=[_empirical_bucket(bucket_key, net_pnl="0.0100", win_rate=0.50)],
        enforce_empirical_bucket_gate=True,
    )
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert no_candidate["empirical_bucket_status"] == "override_allowed"
    assert no_candidate["empirical_bucket_gate"]["original_reason"] == "empirical_bucket_low_win_rate"
    assert no_candidate["empirical_bucket_gate"]["late_override"]["reason"] == (
        "late_high_confidence_empirical_bucket_low_win_rate_override"
    )


def test_late_high_confidence_does_not_override_negative_pnl_bucket_by_default(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_empirical_bucket_gate_assets="BTC",
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-BUCKET-NEG-PNL",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 60,
        "market_age_seconds": 840,
    }
    bucket_key = _crypto_bucket_key(row, {"side": "no", "execution_price_dollars": "0.9700"})

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.0500"),
        settings=settings,
        empirical_bucket_matrix=[_empirical_bucket(bucket_key, net_pnl="-0.0100", win_rate=0.95)],
        enforce_empirical_bucket_gate=True,
    )
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["late_high_confidence_directional_entry"] is True
    assert no_candidate["candidate_status"] == "blocked_empirical_bucket"
    assert no_candidate["reason"] == "empirical_bucket_not_allowed"
    assert no_candidate["empirical_bucket_gate"]["reason"] == "empirical_bucket_negative_pnl"
    assert no_candidate["empirical_bucket_late_override"]["reason"] == "negative_pnl_override_disabled"


def test_late_high_confidence_bucket_override_stays_inside_180_seconds(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        risk_min_edge_bps=500,
        crypto_empirical_bucket_gate_assets="BTC",
        crypto_autonomy_min_seconds_to_close=120,
        crypto_late_sure_thing_min_probability=0.90,
        crypto_late_sure_thing_max_seconds_to_close=300,
    )
    row = {
        "market_ticker": "KXBTC15M-LATE-BUCKET-OUTSIDE-OVERRIDE",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.0350"),
        "yes_bid_dollars": Decimal("0.0300"),
        "yes_ask_dollars": Decimal("0.0400"),
        "no_ask_dollars": Decimal("0.9700"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 240,
        "market_age_seconds": 660,
    }

    candidates = _crypto_trade_candidates(
        row,
        Decimal("0.0500"),
        settings=settings,
        empirical_bucket_matrix=[],
        enforce_empirical_bucket_gate=True,
    )
    no_candidate = next(candidate for candidate in candidates if candidate["side"] == "no")

    assert no_candidate["late_high_confidence_directional_entry"] is True
    assert no_candidate["candidate_status"] == "blocked_empirical_bucket"
    assert no_candidate["empirical_bucket_late_override"]["reason"] == "outside_late_override_window"


def test_crypto_candidate_quality_allows_mid_window_with_750bps_edge(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=750)
    row = {
        "market_ticker": "KXBTC15M-MID",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.5000"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 0,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert candidates[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert candidates[0]["runtime_thresholds"]["min_fee_adjusted_edge_bps"] == 750
    assert candidates[0]["settlement_benchmark_source"] == "cfb_rti_60s_average"
    assert candidates[0]["settlement_proxy_for_cfb_rti"] is True
    assert candidates[0]["settlement_diagnostic_codes"] == ["crypto_settlement_proxy_for_cfb_rti"]


def test_crypto_candidate_quality_allows_high_cost_down_when_net_edge_positive(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=100)
    row = {
        "market_ticker": "KXBTC15M-HIGH-COST-DOWN",
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.1500"),
        "yes_bid_dollars": Decimal("0.1500"),
        "yes_ask_dollars": Decimal("0.1500"),
        "no_ask_dollars": Decimal("0.8500"),
        "spread_bps": 0,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.0000"), settings=settings)

    assert candidates[0]["side"] == "no"
    assert candidates[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert candidates[0]["reason"] == "positive_fee_adjusted_live_quality_edge"
    assert candidates[0]["remaining_payout_dollars"] == "0.1500"
    assert candidates[0]["runtime_thresholds"]["min_remaining_payout_bps"] == 0


def test_crypto_dashboard_signal_metrics_follow_current_quote(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    market = CryptoMarket(
        market_ticker="KXBTC15M-QUOTE-MOVE",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        status="open",
        close_time=datetime.now(UTC) + timedelta(minutes=10),
        target_price_dollars=Decimal("75000.0000"),
        yes_bid_dollars=Decimal("0.1900"),
        yes_ask_dollars=Decimal("0.2000"),
        no_ask_dollars=Decimal("0.8000"),
    )
    signal_payload = {
        "fair_yes_dollars": "0.0500",
        "edge_bps": 1000,
        "recommended_side": "no",
        "candidate_trace": {
            "selected_side": "no",
            "selected_edge_bps": 1000,
            "expected_net_edge": "0.1000",
            "candidates": [
                {
                    "side": "no",
                    "edge_bps": 1000,
                    "expected_net_edge": "0.1000",
                    "execution_price_dollars": "0.8500",
                }
            ],
        },
    }

    refreshed = _crypto_signal_payload_with_current_quote_metrics(
        signal_payload,
        market=market,
        settings=settings,
    )

    assert refreshed is not None
    trace = refreshed["candidate_trace"]
    no_candidate = next(candidate for candidate in trace["candidates"] if candidate["side"] == "no")
    assert refreshed["edge_bps"] == 412
    assert trace["selected_edge_bps"] == 412
    assert refreshed["raw_fair_yes_dollars"] == "0.0500"
    assert refreshed["fair_yes_dollars"] == "0.1588"
    assert trace["raw_fair_yes_dollars"] == "0.0500"
    assert trace["market_anchored_fair_yes_dollars"] == "0.1588"
    assert no_candidate["execution_price_dollars"] == "0.8000"
    assert no_candidate["edge_bps"] == 412
    assert no_candidate["expected_net_edge"] != "0.1000"
    assert trace["quote_metrics_source"] == "current_market_quote_cached_prediction"


def test_crypto_dashboard_quote_refresh_anchors_contrarian_low_price_prediction_to_market(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    market = _market(
        close_time=datetime.now(UTC) + timedelta(minutes=10),
        yes_bid_dollars=Decimal("0.9700"),
        yes_ask_dollars=Decimal("0.9800"),
        no_ask_dollars=Decimal("0.0250"),
        last_price_dollars=Decimal("0.9800"),
    )
    signal_payload = {
        "fair_yes_dollars": "0.3664",
        "edge_bps": 6086,
        "recommended_side": "no",
        "target_yes_price_dollars": "0.9750",
        "candidate_trace": {
            "selected_side": "no",
            "selected_edge_bps": 6086,
            "selection_reason": "contract_price_below_crypto_min",
        },
    }

    refreshed = _crypto_signal_payload_with_current_quote_metrics(
        signal_payload,
        market=market,
        settings=settings,
    )

    assert refreshed is not None
    assert refreshed["recommended_action"] is None
    assert refreshed["recommended_side"] is None
    assert refreshed["target_yes_price_dollars"] is None
    trace = refreshed["candidate_trace"]
    assert trace["outcome"] == "predicted_winner_blocked"
    assert trace["raw_predicted_winner_side"] == "no"
    assert trace["predicted_winner_side"] == "yes"
    assert trace["selected_side"] == "yes"
    assert trace["selection_reason"] == "fee_adjusted_edge_below_live_min"
    assert trace["raw_fair_yes_dollars"] == "0.3664"
    assert trace["market_anchored_fair_yes_dollars"] == "0.8229"
    no_candidate = next(candidate for candidate in trace["candidates"] if candidate["side"] == "no")
    yes_candidate = next(candidate for candidate in trace["candidates"] if candidate["side"] == "yes")
    assert no_candidate["candidate_status"] == "blocked_fee_edge"
    assert no_candidate["reason"] == "contract_price_below_crypto_min"
    assert yes_candidate["model_winner"] is True
    assert yes_candidate["raw_model_winner"] is False


def test_crypto_recommendation_selects_market_anchored_winner_not_low_price_value_side(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=500)
    market = _market(
        yes_bid_dollars=Decimal("0.8900"),
        yes_ask_dollars=Decimal("0.9000"),
        no_ask_dollars=Decimal("0.1100"),
        last_price_dollars=Decimal("0.8900"),
    )
    row = {
        "market_ticker": market.market_ticker,
        "asset_symbol": "BTC",
        "mid_yes_dollars": Decimal("0.8950"),
        "yes_bid_dollars": Decimal("0.8900"),
        "yes_ask_dollars": Decimal("0.9000"),
        "no_ask_dollars": Decimal("0.1100"),
        "spread_bps": 100,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_tick",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    action, side, target_yes, edge_bps, trace = _crypto_recommendation(
        market=market,
        fair_yes=Decimal("0.8667"),
        settings=settings,
        row=row,
    )

    assert action is None
    assert side is None
    assert target_yes is None
    assert edge_bps == -121
    assert trace["outcome"] == "predicted_winner_blocked"
    assert trace["predicted_winner_side"] == "yes"
    assert trace["selected_side"] == "yes"
    assert trace["selection_reason"] == "broad_shadow_exploration"
    assert trace["raw_fair_yes_dollars"] == "0.8667"
    assert trace["market_anchored_fair_yes_dollars"] == "0.8879"
    assert trace["candidates"][0]["side"] == "yes"
    assert trace["candidates"][0]["model_winner"] is True
    assert trace["candidates"][1]["side"] == "no"


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
        "yes_bid_dollars": Decimal("0.4800"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings, crypto_policy=policy)

    assert candidates[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert candidates[0]["runtime_thresholds"]["max_spread_bps"] == 250
    assert candidates[0]["runtime_thresholds"]["min_contract_price_dollars"] == 0.5


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
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }

    candidates = _crypto_trade_candidates(row, Decimal("0.6000"), settings=settings, crypto_policy=policy)

    assert candidates[0]["candidate_status"] != CRYPTO_LIVE_QUALITY
    assert candidates[0]["reason"] == "spread_above_live_max"


def test_crypto_replay_sparse_rows_keep_candidate_diagnostics_but_block_oos(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_min_training_samples=20, crypto_replay_min_resolved_markets=2)
    rows = [
        _replay_row(market_day="2026-05-01", market_ticker="KXBTC15M-SPARSE-1"),
        _replay_row(market_day="2026-05-01", market_ticker="KXBTC15M-SPARSE-2"),
    ]

    report = _evaluate_crypto_walk_forward(
        rows,  # type: ignore[arg-type]
        settings=settings,
        diagnostic_model={"model_type": "market_mid_baseline"},
    )
    gate = CryptoReplayService(settings=settings, session_factory=None).evaluate_gate(report["metrics"])  # type: ignore[arg-type]

    assert report["status"] == "insufficient_data"
    assert report["metrics"]["trade_candidate_count"] == 2
    assert report["metrics"]["oos_fold_count"] == 0
    assert report["metrics"]["oos_evaluation_status"] == "insufficient_data"
    assert report["candidate_quality"]["live_quality_policy"]["selected_count"] == 2
    assert gate["passed"] is False
    assert any("Out-of-sample replay is unavailable" in reason for reason in gate["reasons"])


def test_crypto_replay_candidate_selection_does_not_require_empirical_bucket_maturity(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_min_training_samples=2,
        crypto_replay_min_resolved_markets=3,
        crypto_replay_min_trade_candidates=1,
    )
    rows = [
        _replay_row(market_day="2026-05-01", market_ticker="KXBTC15M-BUCKET-1"),
        _replay_row(market_day="2026-05-02", market_ticker="KXBTC15M-BUCKET-2"),
        _replay_row(market_day="2026-05-03", market_ticker="KXBTC15M-BUCKET-3"),
    ]

    report = _evaluate_crypto_walk_forward(
        rows,  # type: ignore[arg-type]
        settings=settings,
        diagnostic_model={"model_type": "market_mid_baseline"},
    )

    assert report["status"] == "ok"
    assert report["metrics"]["oos_fold_count"] == 1
    assert report["metrics"]["trade_candidate_count"] == 3
    assert report["metrics"]["oos_trade_candidate_count"] == 1
    assert any(policy["policy_name"] == "candidate_quality_policy" for policy in report["candidate_policies"])
    assert report["candidate_policies"][2]["selected_count"] == 1
    assert report["bucket_matrix"]
    assert all(bucket["sample_count"] < settings.trade_behavior_empirical_gate_min_settled_fills for bucket in report["bucket_matrix"])


def test_crypto_replay_uses_runtime_asset_entry_overrides_for_candidate_support(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=50)
    rows = [_replay_row(market_day="2026-05-01", mid_yes_dollars=Decimal("0.6000"))]
    service = AgentPackService(settings)
    policy = service.runtime_crypto_policy(
        service.default_pack().model_copy(
            update={
                "crypto_policy": AgentPackCryptoPolicy(
                    asset_entry_overrides={
                        "BTC": AgentPackCryptoEntryPolicy(min_fee_adjusted_edge_bps=3000),
                    }
                )
            }
        )
    )

    default_report = _evaluate_crypto_walk_forward(
        rows,  # type: ignore[arg-type]
        settings=settings,
        diagnostic_model={"model_type": "market_mid_baseline"},
    )
    override_report = _evaluate_crypto_walk_forward(
        rows,  # type: ignore[arg-type]
        settings=settings,
        crypto_policy=policy,
        diagnostic_model={"model_type": "market_mid_baseline"},
    )

    assert default_report["metrics"]["trade_candidate_count"] == 1
    assert override_report["metrics"]["trade_candidate_count"] == 0
    assert override_report["metrics"]["candidate_rejection_reason_counts"] == {"fee_adjusted_edge_below_live_min": 1}


def test_crypto_replay_candidate_blockers_prefer_strict_rows_over_proxy_rows(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_min_training_samples=20, risk_min_edge_bps=5000)
    strict = _replay_row(market_ticker="KXBTC15M-STRICT", mid_yes_dollars=Decimal("0.5000"))
    proxy_rows = [
        _replay_row(
            market_ticker=f"KXBTC15M-PROXY-{idx}",
            row_id=f"proxy-{idx}",
            quote_source="candlestick_close_proxy",
            strict_trade_eligible=False,
            execution_model_status="proxy_quote_prediction_only",
        )
        for idx in range(3)
    ]

    report = _evaluate_crypto_walk_forward(
        [strict, *proxy_rows],  # type: ignore[list-item]
        settings=settings,
        diagnostic_model={"model_type": "market_mid_baseline"},
    )

    assert report["candidate_quality"]["dataset"]["row_count"] == 4
    assert report["candidate_quality"]["dataset"]["candidate_diagnostic_row_count"] == 1
    assert report["candidate_quality"]["dataset"]["candidate_diagnostic_scope"] == "strict_point_in_time_rows"
    assert report["metrics"]["trade_candidate_count"] == 0
    assert report["metrics"]["candidate_rejection_reason_counts"] == {"fee_adjusted_edge_below_live_min": 1}


def test_crypto_replay_gate_blocks_zero_oos_folds_even_with_strong_metrics(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_replay_min_resolved_markets=2, crypto_replay_min_trade_candidates=1)
    metrics = {
        "resolved_sample_count": 10,
        "trade_candidate_count": 10,
        "strict_trade_eligible_count": 10,
        "net_simulated_pl_dollars": 10.0,
        "market_mid_net_simulated_pl_dollars": 0.0,
        "pnl_advantage_vs_market_mid_dollars": 10.0,
        "oos_evaluation_status": "insufficient_data",
        "oos_fold_count": 0,
        "oos_net_simulated_pl_dollars": 10.0,
        "oos_market_mid_net_simulated_pl_dollars": 0.0,
        "oos_pnl_advantage_vs_market_mid_dollars": 10.0,
        "hard_cap_breaches": 0,
        "candle_count": 10,
        "spot_feature_coverage_pct": 1.0,
    }

    gate = CryptoReplayService(settings=settings, session_factory=None).evaluate_gate(metrics)  # type: ignore[arg-type]

    assert gate["passed"] is False
    assert any("Out-of-sample replay is unavailable" in reason for reason in gate["reasons"])


def test_crypto_replay_gate_separates_oos_candidates_from_current_model_candidates(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_replay_min_resolved_markets=500, crypto_replay_min_trade_candidates=50)
    metrics = {
        "resolved_sample_count": 500,
        "trade_candidate_count": 0,
        "oos_trade_candidate_count": 106,
        "strict_trade_eligible_count": 365,
        "net_simulated_pl_dollars": 9.68,
        "market_mid_net_simulated_pl_dollars": 0.0,
        "pnl_advantage_vs_market_mid_dollars": 9.68,
        "oos_evaluation_status": "ok",
        "oos_fold_count": 1,
        "oos_net_simulated_pl_dollars": 9.68,
        "oos_market_mid_net_simulated_pl_dollars": 0.0,
        "oos_pnl_advantage_vs_market_mid_dollars": 9.68,
        "hard_cap_breaches": 0,
        "candle_count": 10,
        "spot_feature_coverage_pct": 1.0,
    }

    gate = CryptoReplayService(settings=settings, session_factory=None).evaluate_gate(metrics)  # type: ignore[arg-type]

    assert gate["passed"] is False
    assert "Current model live-quality candidate count 0 below minimum 50." in gate["reasons"]
    assert not any("Out-of-sample trade candidate count" in reason for reason in gate["reasons"])


def test_crypto_replay_gate_uses_positive_bucket_gated_metrics(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_empirical_bucket_gate_assets="SOL",
        crypto_empirical_bucket_min_samples=2,
        crypto_empirical_bucket_min_win_rate=0.50,
        crypto_replay_require_pnl_beats_market_mid=True,
        crypto_replay_require_calibration_better_than_mid=False,
    )
    allowed_key = "SOL|yes|0.75-1.00|normal|5_10m"
    blocked_key = "SOL|no|0.75-1.00|normal|5_10m"
    selection_trades = [
        {
            "asset_symbol": "SOL",
            "market_day": "2026-05-14",
            "mid_yes_dollars": Decimal("0.8000"),
            "spread_bps": 100,
            "time_to_close_seconds": 600,
            "simulation": {
                "bucket_key": allowed_key,
                "side": "yes",
                "execution_price_dollars": "0.8000",
                "net_pnl": "1.0000",
                "gross_pnl": "1.0000",
                "fees": "0.0000",
            },
        },
        {
            "asset_symbol": "SOL",
            "market_day": "2026-05-14",
            "mid_yes_dollars": Decimal("0.8000"),
            "spread_bps": 100,
            "time_to_close_seconds": 600,
            "simulation": {
                "bucket_key": allowed_key,
                "side": "yes",
                "execution_price_dollars": "0.8000",
                "net_pnl": "0.8000",
                "gross_pnl": "0.8000",
                "fees": "0.0000",
            },
        },
        {
            "asset_symbol": "SOL",
            "market_day": "2026-05-14",
            "mid_yes_dollars": Decimal("0.8000"),
            "spread_bps": 100,
            "time_to_close_seconds": 600,
            "simulation": {
                "bucket_key": blocked_key,
                "side": "no",
                "execution_price_dollars": "0.8000",
                "net_pnl": "-4.0000",
                "gross_pnl": "-4.0000",
                "fees": "0.0000",
            },
        },
    ]
    metrics = _crypto_apply_empirical_bucket_gate_to_replay_metrics(
        {
            "resolved_sample_count": 20,
            "strict_trade_eligible_count": 20,
            "trade_candidate_count": 3,
            "current_model_live_quality_candidate_count": 3,
            "oos_trade_candidate_count": 3,
            "net_simulated_pl_dollars": -2.2,
            "market_mid_net_simulated_pl_dollars": 0.0,
            "pnl_advantage_vs_market_mid_dollars": -2.2,
            "oos_evaluation_status": "ok",
            "oos_fold_count": 1,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
            "hard_cap_breaches": 0,
        },
        selection_trades=selection_trades,
        market_mid_trades=[],
        bucket_matrix=[
            _empirical_bucket(allowed_key, sample_count=2, win_rate=1.0, net_pnl="1.8000"),
            _empirical_bucket(blocked_key, sample_count=2, win_rate=0.0, net_pnl="-4.0000"),
        ],
        settings=settings,
    )
    gate = CryptoReplayService(settings=settings, session_factory=None).evaluate_gate(metrics)  # type: ignore[arg-type]

    assert metrics["empirical_bucket_gate_applied_to_metrics"] is True
    assert metrics["pre_bucket_gate_metrics"]["net_simulated_pl_dollars"] == -2.2
    assert metrics["net_simulated_pl_dollars"] == 1.8
    assert metrics["trade_candidate_count"] == 2
    assert gate["passed"] is True


def test_crypto_replay_gate_blocks_when_asset_has_no_allowed_buckets(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        crypto_replay_min_resolved_markets=10,
        crypto_replay_min_trade_candidates=2,
        crypto_empirical_bucket_gate_assets="BNB",
        crypto_empirical_bucket_min_samples=20,
        crypto_replay_require_calibration_better_than_mid=False,
    )
    bucket_key = "BNB|yes|0.75-1.00|normal|5_10m"
    metrics = _crypto_apply_empirical_bucket_gate_to_replay_metrics(
        {
            "resolved_sample_count": 20,
            "strict_trade_eligible_count": 20,
            "trade_candidate_count": 2,
            "current_model_live_quality_candidate_count": 2,
            "oos_trade_candidate_count": 2,
            "net_simulated_pl_dollars": 3.0,
            "market_mid_net_simulated_pl_dollars": 0.0,
            "pnl_advantage_vs_market_mid_dollars": 3.0,
            "oos_evaluation_status": "ok",
            "oos_fold_count": 1,
            "candle_count": 20,
            "spot_feature_coverage_pct": 1.0,
            "hard_cap_breaches": 0,
        },
        selection_trades=[],
        market_mid_trades=[],
        bucket_matrix=[
            _empirical_bucket(bucket_key, sample_count=3, win_rate=1.0, net_pnl="3.0000"),
        ],
        settings=settings,
    )
    gate = CryptoReplayService(settings=settings, session_factory=None).evaluate_gate(metrics)  # type: ignore[arg-type]

    assert metrics["empirical_bucket_gate_applied_to_metrics"] is True
    assert metrics["empirical_bucket_gate"]["allowed_bucket_keys"] == []
    assert metrics["trade_candidate_count"] == 0
    assert gate["passed"] is False


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
        "asset_time_calibration",
        "current_heuristic",
        "market_mid_baseline",
        "sklearn_logistic",
        "spot_distance_residual",
        "xgboost_classifier",
        "lightgbm_classifier",
        "calibrated_weighted_ensemble",
    }
    assert result["payload"]["candidate_report"]["primary_metric"] == "oos_candidate_net_pnl"
    assert result["payload"]["candidate_report"]["selection_policy"] == "prefer_positive_oos_pnl_non_market_candidate_then_pnl_advantage"
    assert result["metrics"]["champion_selection_reason"] in {
        "fallback_market_mid_no_non_market_candidate",
        "diagnostic_only_best_non_market_oos_pnl",
        "selected_positive_oos_pnl_non_market_candidate",
        "selected_non_market_candidate",
        "selected_non_market_candidate_with_diagnostic_guardrail_warnings",
    }
    assert result["payload"]["candidate_registry_version"] == "crypto-candidate-registry-v1"
    assert "candlestick_momentum" in result["payload"]["feature_set"]
    assert candidates["schema_version"] == "crypto-model-candidates-v2"
    assert candidates["primary_metric"] == "oos_candidate_net_pnl"
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
    history_status = await CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=None,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    ).status(frequency="15m", days=30)
    await CryptoForecastService(settings=settings, session_factory=session_factory).train(frequency="15m")
    replay = CryptoReplayService(settings=settings, session_factory=session_factory)

    run_report = await replay.run(frequency="15m", days=30, persist=True)
    validate_report = await replay.validate(frequency="15m", days=30)
    gate = await replay.gate(frequency="15m")

    assert run_report["schema_version"] == "crypto-backtest-report-v1"
    btc_audit = history_status["quote_evidence"]["strict_quote_ingestion_audit_by_asset"]["BTC"]
    assert btc_audit["snapshot_present"] == 5
    assert btc_audit["real_bid_ask_present"] == 5
    assert btc_audit["settled_label_joined"] == 5
    assert btc_audit["point_in_time_rows"] == 5
    assert btc_audit["spot_joined"] == 5
    assert btc_audit["strict_trade_eligible"] == 5
    assert btc_audit["candidate_generated"] == 5
    assert run_report["data_quality"]["candle_count"] == 10
    assert "baseline_policy" in run_report["walk_forward"]
    assert {item["policy_name"] for item in run_report["walk_forward"]["baseline_policies"]} >= {
        "market_mid_baseline",
        "always_0_5",
        "last_direction",
        "naive_momentum",
        "linear_on_returns",
    }
    assert "pnl_advantage_vs_market_mid_dollars" in run_report["metrics"]
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

    assert yes_price == Decimal("0.47")
    assert no_price == Decimal("0.49")


def test_passive_prices_use_valid_cent_ticks_for_one_cent_spread(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = CryptoExecutionService(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        base_execution_service=None,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(settings=settings, session_factory=None),  # type: ignore[arg-type]
    )
    market = _market(yes_bid_dollars=Decimal("0.6500"), yes_ask_dollars=Decimal("0.6600"))

    yes_price = service.passive_yes_price(market, ContractSide.YES)
    no_price = service.passive_yes_price(market, ContractSide.NO)

    assert yes_price == Decimal("0.65")
    assert no_price == Decimal("0.66")


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


@pytest.mark.asyncio
async def test_fill_resolution_prefers_attributed_order_duplicate(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        room = await repo.create_room(
            RoomCreate(name="BTC fill attribution", market_ticker="KXBTC15M-FILL"),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        ticket = await repo.save_trade_ticket(
            room.id,
            TradeTicket(
                market_ticker=room.market_ticker,
                action=TradeAction.BUY,
                side=ContractSide.NO,
                yes_price_dollars=Decimal("0.1500"),
                count_fp=Decimal("1.00"),
            ),
            client_order_id="room:btc-fill-ticket",
            strategy_code="CRYPTO_15M",
        )
        attributed = await repo.save_order(
            ticket_id=ticket.id,
            client_order_id="room:btc-fill-ticket:maker",
            market_ticker=room.market_ticker,
            status="submitted",
            side="no",
            action="buy",
            yes_price_dollars=Decimal("0.1500"),
            count_fp=Decimal("1.00"),
            raw={"order": {"client_order_id": "room:btc-fill-ticket:maker"}},
            kalshi_order_id="kalshi-order-1",
            kalshi_env=settings.kalshi_env,
            strategy_code="CRYPTO_15M",
        )
        await repo.save_order(
            ticket_id=None,
            client_order_id="websocket-duplicate",
            market_ticker=room.market_ticker,
            status="filled",
            side="no",
            action="buy",
            yes_price_dollars=Decimal("0.1500"),
            count_fp=Decimal("1.00"),
            raw={"source": "websocket"},
            kalshi_order_id="kalshi-order-1",
            kalshi_env=settings.kalshi_env,
            strategy_code=None,
        )
        fill = await repo.upsert_fill(
            market_ticker=room.market_ticker,
            side="yes",
            action="buy",
            yes_price_dollars=Decimal("0.1500"),
            count_fp=Decimal("1.00"),
            raw={"order_id": "kalshi-order-1"},
            trade_id="trade-1",
            kalshi_env=settings.kalshi_env,
        )
        fill_order_id = fill.order_id
        fill_strategy_code = fill.strategy_code
        fill_side = fill.side
        attributed_id = attributed.id
        await session.commit()

    assert fill_order_id == attributed_id
    assert fill_strategy_code == "CRYPTO_15M"
    assert fill_side == "no"
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


def test_crypto_dashboard_replay_gate_summary_prefers_live_asset_gates() -> None:
    blocked_generic = SimpleNamespace(
        status="blocked",
        version="generic-blocked",
        sample_count=100,
        metrics={"reason": "aggregate includes shadow assets"},
        payload={},
        updated_at=None,
    )
    passed_btc = SimpleNamespace(
        status="passed",
        version="btc-passed",
        sample_count=100,
        metrics={"asset": "BTC"},
        payload={},
        updated_at=None,
    )
    blocked_eth = SimpleNamespace(
        status="blocked",
        version="eth-blocked",
        sample_count=100,
        metrics={"asset": "ETH"},
        payload={},
        updated_at=None,
    )

    summary = _crypto_replay_gate_dashboard_summary(
        gates_by_asset={"BTC": passed_btc, "ETH": blocked_eth},
        generic_gate=blocked_generic,
        live_asset_symbols=["BTC"],
        displayed_asset_symbols=["BTC", "ETH"],
    )

    assert summary["status"] == "passed"
    assert summary["version"] == "btc-passed"
    assert summary["scope"] == "live_assets"
    assert summary["generic_status"] == "blocked"
    assert summary["asset_statuses"]["BTC"]["status"] == "passed"
    assert summary["asset_statuses"]["ETH"]["status"] == "blocked"


def test_crypto_production_runtime_live_policy_requires_control_live_note(tmp_path) -> None:
    settings = _settings(tmp_path, kalshi_env="production", app_shadow_mode=False, crypto_trading_enabled=False)
    service = AgentPackService(settings)
    policy = service.runtime_crypto_policy(
        service.default_pack().model_copy(
            update={"crypto_policy": AgentPackCryptoPolicy(live={"trading_enabled": True, "asset_modes": {"BTC": "live"}})}
        )
    )
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]
    control = type("_Control", (), {"notes": {}, "kill_switch_enabled": False, "active_color": settings.app_color})()
    gate = type("_Gate", (), {"status": "passed", "metrics": {
        "resolved_sample_count": settings.crypto_replay_min_resolved_markets,
        "trade_candidate_count": settings.crypto_replay_min_trade_candidates,
        "strict_trade_eligible_count": settings.crypto_replay_min_trade_candidates,
        "net_simulated_pl_dollars": 10.0,
        "hard_cap_breaches": 0,
        "candle_count": 1,
        "spot_feature_coverage_pct": 1.0,
    }})()

    status = asset_control.market_live_status(
        control=control,
        replay_gate=gate,
        market=_market(asset_symbol="BTC"),
        has_write_credentials=True,
        crypto_policy=policy,
    )

    assert status["asset_mode"] == "live"
    assert status["control_asset_mode"] == "shadow"
    assert status["live_eligible"] is False
    assert "not explicitly live in deployment control" in status["live_blockers"][0]


def test_crypto_production_explicit_shadow_note_overrides_runtime_live_policy(tmp_path) -> None:
    settings = _settings(tmp_path, kalshi_env="production", app_shadow_mode=False, crypto_trading_enabled=False)
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
        {"notes": {"crypto_asset_modes": {"BTC": "shadow"}}, "kill_switch_enabled": False, "active_color": settings.app_color},
    )()

    assert asset_control.mode_for_control(control, "BTC", crypto_policy=policy) == "shadow"


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
async def test_crypto_execution_production_requires_explicit_control_live_mode(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        kalshi_env="production",
        app_shadow_mode=False,
        crypto_trading_enabled=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    policy = AgentPackService(settings).runtime_crypto_policy(
        AgentPackService(settings).default_pack().model_copy(
            update={"crypto_policy": AgentPackCryptoPolicy(live={"trading_enabled": True, "asset_modes": {"XRP": "live"}})}
        )
    )
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(
        market_ticker="KXXRP15M-26MAY131630-30",
        series_ticker="KXXRP15M",
        asset_symbol="XRP",
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            kalshi_env=settings.kalshi_env,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="XRP crypto", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate:XRP",
            version="passed-xrp-gate",
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
            side=ContractSide.NO,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-xrp-test",
        fair_yes_dollars=Decimal("0.3500"),
        market=market,
        signal=_signal(),
        crypto_policy=policy,
    )

    assert receipt.status == "crypto_asset_live_disabled"
    assert receipt.details["control_asset_mode"] == "shadow"
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
    assert fake_base.calls[0]["ticket"].yes_price_dollars == Decimal("0.47")
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_last_minute_passive_execution_uses_fixed_rest_to_close(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeFixedLimitExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(close_time=datetime.now(UTC) + timedelta(seconds=45))
    signal = _signal()
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "last_minute_passive_market_confidence": True,
        "last_minute_passive": {
            "bid_threshold_dollars": "0.55",
            "market_side_probability": "0.7000",
        },
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "last_minute_passive_market_confidence": True,
        },
    }
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC last-minute passive", market_ticker=market.market_ticker),
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
            yes_price_dollars=Decimal("0.5500"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-last-minute",
        fair_yes_dollars=Decimal("0.2000"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "submitted"
    assert receipt.details["crypto_order_mode"] == "last_minute_passive"
    assert receipt.details["no_taker_fallback"] is True
    assert fake_base.calls == []
    assert len(fake_base.fixed_calls) == 1
    fixed_call = fake_base.fixed_calls[0]
    assert fixed_call["client_order_id"] == "crypto-last-minute:maker"
    assert fixed_call["ticket"].yes_price_dollars == Decimal("0.5500")
    assert fixed_call["ticket"].time_in_force == "good_till_canceled"
    assert fixed_call["close_time"] == market.close_time
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_passive_only_never_falls_back_to_taker(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_only",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus(["unfilled_cancelled"])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market()
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC passive only", market_ticker=market.market_ticker),
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
        client_order_id="crypto-passive-only",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=_signal(),
    )

    assert receipt.status == "passive_unfilled_no_taker"
    assert receipt.details["no_taker_fallback"] is True
    assert len(fake_base.calls) == 1
    assert fake_base.calls[0]["client_order_id"] == "crypto-passive-only:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_taker_fallback_requires_fee_adjusted_net_edge(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_then_taker",
        crypto_taker_fallback_close_seconds=90,
        risk_min_edge_bps=750,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus(["unfilled_cancelled"])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(close_time=datetime.now(UTC) + timedelta(seconds=60))
    signal = _signal()
    signal.candidate_trace = {
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "0.0500",
        }
    }
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC net edge taker", market_ticker=market.market_ticker),
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
        client_order_id="crypto-net-edge-taker",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "passive_unfilled_taker_blocked"
    assert len(fake_base.calls) == 1
    assert fake_base.calls[0]["client_order_id"] == "crypto-net-edge-taker:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_normal_taker_fallback_disabled_when_close_window_zero(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_then_taker",
        crypto_taker_fallback_close_seconds=0,
        risk_min_edge_bps=50,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus(["unfilled_cancelled"])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(close_time=datetime.now(UTC) + timedelta(seconds=60))
    signal = _signal()
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "0.1600",
        },
    }
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC normal fallback disabled", market_ticker=market.market_ticker),
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
        client_order_id="crypto-normal-fallback-disabled",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "passive_unfilled_taker_blocked"
    assert receipt.details["reason"] == "taker_fallback_not_allowed"
    assert len(fake_base.calls) == 1
    assert fake_base.calls[0]["client_order_id"] == "crypto-normal-fallback-disabled:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_late_high_confidence_can_fall_back_to_taker_when_normal_window_zero(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_then_taker",
        crypto_taker_fallback_close_seconds=0,
        crypto_late_sure_thing_max_seconds_to_close=300,
        risk_min_edge_bps=750,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus(["unfilled_cancelled", "submitted"])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(close_time=datetime.now(UTC) + timedelta(seconds=60))
    signal = _signal()
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "late_high_confidence_directional_entry": True,
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "-0.0200",
            "late_high_confidence_directional_entry": True,
        },
    }
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC late fallback", market_ticker=market.market_ticker),
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
        client_order_id="crypto-late-fallback",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "submitted"
    assert len(fake_base.calls) == 2
    assert fake_base.calls[0]["client_order_id"] == "crypto-late-fallback:maker"
    assert fake_base.calls[1]["client_order_id"] == "crypto-late-fallback:taker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_late_high_confidence_taker_fallback_requires_live_quality(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_then_taker",
        crypto_taker_fallback_close_seconds=0,
        crypto_late_sure_thing_max_seconds_to_close=300,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus(["unfilled_cancelled"])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(close_time=datetime.now(UTC) + timedelta(seconds=60))
    signal = _signal()
    signal.candidate_trace = {"late_high_confidence_directional_entry": True}
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC late fallback no live quality", market_ticker=market.market_ticker),
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
        client_order_id="crypto-late-no-live-quality",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "passive_unfilled_taker_blocked"
    assert len(fake_base.calls) == 1
    assert fake_base.calls[0]["client_order_id"] == "crypto-late-no-live-quality:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_passive_then_taker_blocks_when_passive_quote_missing_and_fallback_not_allowed(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_order_mode="passive_then_taker",
        crypto_taker_fallback_close_seconds=0,
        risk_min_edge_bps=50,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecutionStatus([])
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    market = _market(
        close_time=datetime.now(UTC) + timedelta(seconds=60),
        yes_bid_dollars=None,
        yes_ask_dollars=None,
    )
    signal = _signal()
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "0.1600",
        },
    }
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC missing passive quote", market_ticker=market.market_ticker),
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
        client_order_id="crypto-missing-passive",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=signal,
    )

    assert receipt.status == "passive_unfilled_taker_blocked"
    assert receipt.details["passive_price_unavailable"] is True
    assert receipt.details["no_order_submitted"] is True
    assert fake_base.calls == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_dynamic_sizing_saves_ticket_and_executes_approved_count(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
        risk_max_order_count_fp=500.0,
        risk_max_position_count_fp_per_ticker=500.0,
        risk_position_pct=0.10,
        crypto_dynamic_order_target_position_pct=0.10,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10), yes_bid_dollars=Decimal("0.5000"))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = _RecordingCryptoExecution()
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(
            _live_quality_signal(
                target_yes_price_dollars=Decimal("0.5000"),
                fair_yes_dollars=Decimal("0.6500"),
            )
        ),  # type: ignore[arg-type]
        risk_engine=DeterministicRiskEngine(settings),
        execution_service=execution,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await repo.set_checkpoint(
            f"reconcile:{settings.kalshi_env}",
            None,
            {
                "balance": {"balance": 10000, "portfolio_value": 0},
                "reconciled_at": datetime.now(UTC).isoformat(),
            },
        )
        room = await repo.create_room(
            RoomCreate(name="BTC dynamic sizing workflow", market_ticker=market.market_ticker),
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
        verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        await session.commit()

    assert ticket is not None
    assert ticket.count_fp == Decimal("20.00")
    assert ticket.payload["crypto_dynamic_sizing"]["mode"] == "dynamic"
    assert ticket.payload["crypto_dynamic_sizing"]["requested_count_fp"] == "20.00"
    assert verdict is not None
    assert verdict.approved_count_fp == Decimal("20.00")
    assert execution.calls[0]["ticket"].count_fp == Decimal("20.00")
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_commits_approved_ticket_before_external_execution_failure(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        risk_min_edge_bps=50,
        risk_min_probability_extremity_pct=0.0,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10), yes_bid_dollars=Decimal("0.5000"))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = _FailingCryptoExecution()
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(
            _live_quality_signal(
                target_yes_price_dollars=Decimal("0.5000"),
                fair_yes_dollars=Decimal("0.6500"),
            )
        ),  # type: ignore[arg-type]
        risk_engine=_ApproveRiskEngine(),  # type: ignore[arg-type]
        execution_service=execution,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await repo.set_checkpoint(
            f"reconcile:{settings.kalshi_env}",
            None,
            {
                "balance": {"balance": 10000, "portfolio_value": 0},
                "reconciled_at": datetime.now(UTC).isoformat(),
            },
        )
        room = await repo.create_room(
            RoomCreate(name="BTC execution failure workflow", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await session.commit()

    with pytest.raises(RuntimeError, match="exchange side effect failed"):
        await workflow.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        verdict = await repo.get_latest_risk_verdict_for_room(room.id)
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    assert execution.calls
    assert ticket is not None
    assert ticket.status == "approved"
    assert verdict is not None
    assert verdict.status == RiskStatus.APPROVED.value
    assert orders == []
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
async def test_crypto_autonomy_reevaluates_existing_live_room(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=False,
        crypto_autonomy_enabled=True,
        crypto_trading_enabled=True,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_live_min_market_age_seconds=180,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    now = datetime.now(UTC)
    market = _market(
        market_ticker="KXBTC15M-LIVE-REEVAL",
        asset_symbol="BTC",
        open_time=now - timedelta(minutes=5),
        close_time=now + timedelta(minutes=10),
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await AgentPackService(settings).ensure_initialized(repo)
        room = await repo.create_room(
            RoomCreate(name="BTC live duplicate", market_ticker=market.market_ticker),
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
            trained_at=now,
        )
        await session.commit()
    await asset_control.set_asset_mode("BTC", "live", actor="test")

    class _FakeKalshi:
        write_credentials = object()

    class _FakeMarketService:
        kalshi = _FakeKalshi()

        def __init__(self) -> None:
            self.created: list[str] = []

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return [market]

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            raise AssertionError("existing live room should be re-evaluated, not recreated")

    class _FakeWorkflowService:
        def __init__(self) -> None:
            self.runs: list[tuple[str, str]] = []

        async def run_room(self, room_id: str, *, reason: str) -> None:
            self.runs.append((room_id, reason))

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
    assert market_service.created == []
    assert workflow_service.runs == [(room.id, "crypto_autonomy_reevaluate")]
    assert result["reevaluated"][0]["room_id"] == room.id
    assert result["reevaluated"][0]["market_ticker"] == market.market_ticker
    assert not any(item.get("reason") == "room_already_exists" for item in result["skipped"])
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_skips_markets_before_live_entry_window(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=True,
        crypto_autonomy_enabled=True,
        crypto_autonomy_min_seconds_to_close=120,
        crypto_live_min_market_age_seconds=180,
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
            now = datetime.now(UTC)
            self.markets = [
                _market(
                    market_ticker="KXBTC15M-EARLY",
                    asset_symbol="BTC",
                    open_time=now - timedelta(seconds=90),
                    close_time=now + timedelta(seconds=810),
                )
            ]
            self.created: list[str] = []

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            raise AssertionError("early market should not create a room")

    class _FakeWorkflowService:
        async def run_room(self, room_id: str, *, reason: str) -> None:
            raise AssertionError("early market should not run a room")

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
    assert result["eligible_markets"] == 0
    assert result["skipped"][0]["reason"] == "crypto_market_too_early_for_live_entry"
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
async def test_crypto_autonomy_production_shadow_evidence_runs_without_live_fuse(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        kalshi_env="production",
        app_shadow_mode=True,
        crypto_autonomy_enabled=False,
        crypto_production_autonomy_enabled=False,
        crypto_trading_enabled=False,
        crypto_quote_evidence_enabled=True,
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
        await AgentPackService(settings).ensure_initialized(repo)
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        kalshi = _FakeKalshi()

        def __init__(self) -> None:
            self.created: list[str] = []
            self.markets = [
                _market(
                    market_ticker="KXBTC15M-SHADOW-EVIDENCE",
                    asset_symbol="BTC",
                    close_time=datetime.now(UTC) + timedelta(minutes=10),
                )
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            return {
                "room_id": "room-shadow-evidence",
                "market_ticker": market_ticker,
                "asset_symbol": "BTC",
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
    assert result["shadow_evidence_mode"] is True
    assert market_service.created == ["KXBTC15M-SHADOW-EVIDENCE"]
    assert workflow_service.ran == ["room-shadow-evidence"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_runtime_policy_satisfies_production_fuse(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        kalshi_env="production",
        app_shadow_mode=False,
        crypto_autonomy_enabled=False,
        crypto_production_autonomy_enabled=False,
        crypto_trading_enabled=False,
        crypto_autonomy_min_seconds_to_close=120,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    agent_pack_service = AgentPackService(settings)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    runtime_pack = agent_pack_service.default_pack().model_copy(
        update={
            "version": "crypto-runtime-live-test",
            "crypto_policy": AgentPackCryptoPolicy(
                live={
                    "trading_enabled": True,
                    "production_autonomy_enabled": True,
                    "asset_modes": {"BTC": "live"},
                }
            ),
        }
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            kalshi_env=settings.kalshi_env,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await agent_pack_service.ensure_initialized(repo)
        await repo.update_agent_pack(runtime_pack)
        await agent_pack_service.assign_pack_to_color(repo, color=settings.app_color, version=runtime_pack.version)
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        kalshi = _FakeKalshi()

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return [
                _market(
                    market_ticker="KXBTC15M-RUNTIME",
                    asset_symbol="BTC",
                    close_time=datetime.now(UTC) + timedelta(minutes=10),
                )
            ]

    class _FakeWorkflowService:
        async def run_room(self, room_id: str, *, reason: str) -> None:
            raise AssertionError("runtime policy test should not create a live room while write credentials are missing")

    result = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeMarketService(),  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=_FakeWorkflowService(),  # type: ignore[arg-type]
        agent_pack_service=agent_pack_service,
    ).run_once()

    assert result["status"] == "ok"
    assert result["kalshi_env"] == "production"
    assert result["skipped"][0]["reason"] == "not_live_eligible"
    assert "Kalshi write credentials are missing." in result["skipped"][0]["live_blockers"]
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
