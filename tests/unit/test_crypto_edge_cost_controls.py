from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import TradeTicket
from kalshi_bot.crypto.edge_shrinkage import fit_edge_shrinkage
from kalshi_bot.crypto.services import (
    CRYPTO_EXPLORATORY_SHADOW,
    CRYPTO_LIVE_QUALITY,
    CryptoForecastService,
    _crypto_dynamic_order_count_fp,
    _crypto_edge_shrinkage_from_notes,
    _crypto_fee_to_edge_review,
    _crypto_passive_entry_limit_price,
    _crypto_passive_replay_first_fill,
    _crypto_passive_replay_metrics,
    _crypto_trade_candidates,
    _simulate_crypto_passive_trade,
    crypto_edge_shrinkage_note_key,
)
from kalshi_bot.db.models import FillRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.risk import RiskContext
from kalshi_bot.services.signal import StrategySignal


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto.db",
        "web_auth_enabled": False,
        "risk_min_edge_bps": 500,
        "risk_min_confidence": 0.50,
        "crypto_autonomy_enabled": False,
        "crypto_trading_enabled": False,
        "crypto_model_trained_replay_only": False,
    }
    values.update(overrides)
    return Settings(**values)


def _candidate_row(**overrides) -> dict[str, object]:
    values: dict[str, object] = {
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
    values.update(overrides)
    return values


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
        "no_bid_dollars": Decimal("0.4800"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 200,
    }
    values.update(overrides)
    return values


# --- Task 1: edge shrinkage fit -------------------------------------------------


def test_fit_edge_shrinkage_empty_input_is_insufficient_data() -> None:
    fit = fit_edge_shrinkage([])

    assert fit["status"] == "insufficient_data"
    assert fit["beta"] == 1.0
    assert fit["raw_beta"] is None
    assert fit["sample_count"] == 0


def test_fit_edge_shrinkage_degenerate_zero_predictions_is_insufficient_data() -> None:
    fit = fit_edge_shrinkage([(0.0, 0.10), (0.0, -0.20)])

    assert fit["status"] == "insufficient_data"
    assert fit["beta"] == 1.0


def test_fit_edge_shrinkage_recovers_origin_slope() -> None:
    observations = [(0.10, 0.05), (0.20, 0.10), (0.05, 0.025)]

    fit = fit_edge_shrinkage(observations, beta_floor=0.2)

    assert fit["status"] == "ok"
    assert fit["sample_count"] == 3
    assert abs(fit["raw_beta"] - 0.5) < 1e-9
    assert abs(fit["beta"] - 0.5) < 1e-9


def test_fit_edge_shrinkage_clamps_to_floor_and_one() -> None:
    floored = fit_edge_shrinkage([(0.10, 0.005), (0.20, 0.010)], beta_floor=0.2)
    capped = fit_edge_shrinkage([(0.10, 0.30), (0.20, 0.60)], beta_floor=0.2)

    assert abs(floored["raw_beta"] - 0.05) < 1e-9
    assert floored["beta"] == 0.2
    assert abs(capped["raw_beta"] - 3.0) < 1e-9
    assert capped["beta"] == 1.0


def test_fit_edge_shrinkage_skips_non_finite_pairs() -> None:
    fit = fit_edge_shrinkage([(0.10, 0.05), (float("nan"), 0.10), (0.20, float("inf"))])

    assert fit["sample_count"] == 1
    assert abs(fit["raw_beta"] - 0.5) < 1e-9


# --- Task 1: decision-time shrinkage veto ---------------------------------------


def test_crypto_candidate_blocked_when_shrunk_edge_cannot_cover_fee(tmp_path) -> None:
    settings = _settings(tmp_path)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 100}

    candidates = _crypto_trade_candidates(
        _candidate_row(),
        Decimal("0.6000"),
        settings=settings,
        edge_shrinkage=shrinkage,
    )

    top = candidates[0]
    assert top["candidate_status"] == "blocked_shrunk_edge"
    assert top["reason"] == "shrunk_fee_adjusted_edge_not_positive"
    assert top["status"] == "blocked"
    assert top["edge_shrinkage"]["enforced"] is True
    assert top["edge_shrinkage"]["blocked"] is True
    assert top["edge_shrinkage"]["beta"] == 0.05
    assert top["shrunk_edge_bps"] is not None


def test_crypto_candidate_shrinkage_not_enforced_below_min_fills(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_edge_shrinkage_min_fills=40)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 10}

    candidates = _crypto_trade_candidates(
        _candidate_row(),
        Decimal("0.6000"),
        settings=settings,
        edge_shrinkage=shrinkage,
    )

    top = candidates[0]
    assert top["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert top["edge_shrinkage"]["available"] is True
    assert top["edge_shrinkage"]["enforced"] is False
    assert top["edge_shrinkage"]["blocked"] is False


def test_crypto_candidate_shrinkage_records_diagnostics_when_enforce_disabled(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_edge_shrinkage_enforce=False)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 100}

    candidates = _crypto_trade_candidates(
        _candidate_row(),
        Decimal("0.6000"),
        settings=settings,
        edge_shrinkage=shrinkage,
    )

    top = candidates[0]
    assert top["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert top["edge_shrinkage"]["available"] is True
    assert top["edge_shrinkage"]["enforced"] is False


def test_crypto_candidate_min_edge_gate_unchanged_without_shrinkage(tmp_path) -> None:
    settings = _settings(tmp_path)

    live = _crypto_trade_candidates(_candidate_row(), Decimal("0.6000"), settings=settings)
    shadow = _crypto_trade_candidates(_candidate_row(), Decimal("0.5000"), settings=settings)

    assert live[0]["candidate_status"] == CRYPTO_LIVE_QUALITY
    assert shadow[0]["candidate_status"] == CRYPTO_EXPLORATORY_SHADOW
    assert live[0]["edge_shrinkage"]["available"] is False


def test_crypto_edge_shrinkage_from_notes_respects_enabled_flag(tmp_path) -> None:
    notes = {crypto_edge_shrinkage_note_key("15m"): {"beta": 0.4, "status": "ok", "sample_count": 50}}

    enabled = _crypto_edge_shrinkage_from_notes(notes, frequency="15m", settings=_settings(tmp_path))
    disabled = _crypto_edge_shrinkage_from_notes(
        notes,
        frequency="15m",
        settings=_settings(tmp_path, crypto_edge_shrinkage_enabled=False),
    )
    missing = _crypto_edge_shrinkage_from_notes(notes, frequency="1h", settings=_settings(tmp_path))

    assert enabled == {"beta": 0.4, "status": "ok", "sample_count": 50}
    assert disabled is None
    assert missing is None


# --- Task 2: fee-to-edge sizing floor -------------------------------------------


def test_fee_to_edge_review_ok_at_initial_count(tmp_path) -> None:
    settings = _settings(tmp_path)

    count, review = _crypto_fee_to_edge_review(
        settings=settings,
        unit_cost=Decimal("0.5000"),
        edge_per_contract_dollars=Decimal("0.1000"),
        edge_source="raw_edge",
        count_fp=Decimal("1.00"),
        max_count_fp=Decimal("20.00"),
    )

    assert count == Decimal("1.00")
    assert review["status"] == "ok"
    assert review["fee_dollars"] == "0.02"
    assert review["ratio"] <= 0.5


def test_fee_to_edge_review_amortizes_round_up_fee_by_growing_count(tmp_path) -> None:
    settings = _settings(tmp_path)

    # Raw fee per contract is 0.0175; the cent ceiling makes a 1-lot pay 0.02,
    # breaching ratio 0.5 * 0.038. At 4 contracts the ceiling amortizes:
    # fee = ceil(0.07) = 0.07 <= 0.5 * 0.038 * 4 = 0.076.
    count, review = _crypto_fee_to_edge_review(
        settings=settings,
        unit_cost=Decimal("0.5000"),
        edge_per_contract_dollars=Decimal("0.0380"),
        edge_source="raw_edge",
        count_fp=Decimal("1.00"),
        max_count_fp=Decimal("20.00"),
    )

    assert review["status"] == "adjusted"
    assert count == Decimal("4.00")
    assert review["fee_dollars"] == "0.07"


def test_fee_to_edge_review_blocks_when_caps_prevent_amortization(tmp_path) -> None:
    settings = _settings(tmp_path)

    count, review = _crypto_fee_to_edge_review(
        settings=settings,
        unit_cost=Decimal("0.5000"),
        edge_per_contract_dollars=Decimal("0.0380"),
        edge_source="raw_edge",
        count_fp=Decimal("1.00"),
        max_count_fp=Decimal("2.00"),
    )

    assert review["status"] == "blocked_fee_ratio"
    assert count == Decimal("1.00")
    assert review["fee_dollars"] is not None
    assert review["edge_dollars"] is not None
    assert review["ratio"] > 0.5


def test_fee_to_edge_review_blocks_when_edge_below_unrounded_fee(tmp_path) -> None:
    settings = _settings(tmp_path)

    # Allowed fee per contract (0.5 * 0.021) is below even the unrounded
    # 0.0175 fee, so no count can ever satisfy the ratio.
    count, review = _crypto_fee_to_edge_review(
        settings=settings,
        unit_cost=Decimal("0.5000"),
        edge_per_contract_dollars=Decimal("0.0210"),
        edge_source="raw_edge",
        count_fp=Decimal("1.00"),
        max_count_fp=Decimal("10000.00"),
    )

    assert review["status"] == "blocked_fee_ratio"
    assert count == Decimal("1.00")


def test_fee_to_edge_review_disabled_with_non_positive_ratio(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_max_fee_to_edge_ratio=0.0)

    count, review = _crypto_fee_to_edge_review(
        settings=settings,
        unit_cost=Decimal("0.5000"),
        edge_per_contract_dollars=Decimal("0.0010"),
        edge_source="raw_edge",
        count_fp=Decimal("1.00"),
        max_count_fp=Decimal("1.00"),
    )

    assert count == Decimal("1.00")
    assert review["status"] == "skipped_disabled"


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


def _sizing_signal(*, candidate_status: str = CRYPTO_LIVE_QUALITY, expected_net_edge: str = "0.0600") -> StrategySignal:
    return StrategySignal(
        fair_yes_dollars=Decimal("0.6000"),
        confidence=0.9,
        edge_bps=600,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5000"),
        summary="sizing-test",
        candidate_trace={
            "candidate_status": candidate_status,
            "trade_selection_model": {
                "candidate_status": candidate_status,
                "expected_net_edge": expected_net_edge,
            },
        },
    )


def _sizing_context(**overrides) -> RiskContext:
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


def test_dynamic_sizing_passes_fee_floor_with_healthy_edge(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(),
        signal=_sizing_signal(expected_net_edge="0.0600"),
        context=_sizing_context(),
    )

    assert count == Decimal("20.00")
    assert diagnostics["fee_to_edge"]["status"] == "ok"


def test_dynamic_sizing_blocks_fee_ratio_when_edge_too_thin(tmp_path) -> None:
    settings = _settings(tmp_path, risk_position_pct=0.10, crypto_dynamic_order_target_position_pct=0.10)

    # expected_net_edge 0.0010 -> raw edge/contract 0.021; allowed fee/contract
    # 0.0105 is below the unrounded 0.0175 fee, so no count can comply.
    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(),
        signal=_sizing_signal(expected_net_edge="0.0010"),
        context=_sizing_context(),
    )

    assert diagnostics["fee_to_edge"]["status"] == "blocked_fee_ratio"
    assert diagnostics["fee_to_edge"]["fee_dollars"] is not None
    assert diagnostics["fee_to_edge"]["edge_dollars"] is not None
    assert count == Decimal("20.00")


def test_dynamic_sizing_skips_fee_floor_for_non_live_quality(tmp_path) -> None:
    settings = _settings(tmp_path)

    count, diagnostics = _crypto_dynamic_order_count_fp(
        settings=settings,
        ticket=_sizing_ticket(),
        signal=_sizing_signal(candidate_status=CRYPTO_EXPLORATORY_SHADOW, expected_net_edge="0.0010"),
        context=_sizing_context(),
    )

    assert count == Decimal("1.00")
    assert diagnostics["fee_to_edge"]["status"] == "skipped_not_live_quality"


# --- Task 3: passive-fill replay -------------------------------------------------


def test_passive_entry_limit_price_uses_bid_then_mid_minus_tick() -> None:
    with_bid = _replay_row()
    without_bid = _replay_row(yes_bid_dollars=None, mid_yes_dollars=Decimal("0.7000"))

    assert _crypto_passive_entry_limit_price(with_bid, "yes") == Decimal("0.4800")
    assert _crypto_passive_entry_limit_price(without_bid, "yes") == Decimal("0.6900")
    # NO side derives from the yes mid: 1 - 0.70 - 0.01.
    assert _crypto_passive_entry_limit_price(_replay_row(no_bid_dollars=None), "no") == Decimal("0.2900")


def test_passive_fill_requires_market_to_trade_through_limit() -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    future_rows = [
        _replay_row(decision_ts=base + timedelta(minutes=1), yes_ask_dollars=Decimal("0.5500")),
        _replay_row(decision_ts=base + timedelta(minutes=2), yes_ask_dollars=Decimal("0.4700")),
    ]

    no_fill = _crypto_passive_replay_first_fill(
        future_rows[:1],
        side="yes",
        limit_price=Decimal("0.4800"),
    )
    fill = _crypto_passive_replay_first_fill(
        future_rows,
        side="yes",
        limit_price=Decimal("0.4800"),
    )

    assert no_fill is None
    assert fill is not None
    assert fill[1] == Decimal("0.4700")
    assert fill[0]["decision_ts"] == base + timedelta(minutes=2)


def test_passive_fill_respects_close_time_bound() -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    close_ts = base + timedelta(minutes=15)
    future_rows = [
        _replay_row(decision_ts=base + timedelta(minutes=20), yes_ask_dollars=Decimal("0.4000")),
    ]

    fill = _crypto_passive_replay_first_fill(
        future_rows,
        side="yes",
        limit_price=Decimal("0.4800"),
        decision_ts=base,
        close_ts=close_ts,
    )

    assert fill is None


def test_passive_fill_derives_no_ask_from_yes_bid_when_absent() -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    future_rows = [
        _replay_row(
            decision_ts=base + timedelta(minutes=1),
            no_ask_dollars=None,
            yes_bid_dollars=Decimal("0.6000"),
        ),
    ]

    fill = _crypto_passive_replay_first_fill(
        future_rows,
        side="no",
        limit_price=Decimal("0.4500"),
    )

    assert fill is not None
    assert fill[1] == Decimal("0.4000")


def test_simulate_passive_trade_charges_estimated_maker_fee() -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    row = _replay_row(decision_ts=base, label_yes=1)
    future_rows = [
        _replay_row(decision_ts=base + timedelta(minutes=2), yes_ask_dollars=Decimal("0.4700")),
    ]

    filled = _simulate_crypto_passive_trade(row, future_rows, {"side": "yes"})
    unfilled = _simulate_crypto_passive_trade(row, [], {"side": "yes"})

    assert filled["filled"] is True
    assert filled["limit_price_dollars"] == "0.4800"
    assert filled["fees"] == "0.0100"
    assert filled["fee_source"] == "estimated_maker"
    assert filled["net_pnl"] == "0.5100"
    assert unfilled["filled"] is False
    assert unfilled["net_pnl"] is None


def test_passive_replay_metrics_expose_adverse_selection_split() -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    filled_market = _replay_row(
        market_ticker="KXBTC15M-FILLED",
        decision_ts=base,
        label_yes=0,
    )
    unfilled_market = _replay_row(
        market_ticker="KXBTC15M-UNFILLED",
        decision_ts=base,
        label_yes=1,
    )
    rows = [
        filled_market,
        # Trades through the 0.48 passive limit before close -> passive fill.
        _replay_row(
            market_ticker="KXBTC15M-FILLED",
            decision_ts=base + timedelta(minutes=2),
            yes_ask_dollars=Decimal("0.4700"),
        ),
        unfilled_market,
        # Never trades through -> passive order rests unfilled.
        _replay_row(
            market_ticker="KXBTC15M-UNFILLED",
            decision_ts=base + timedelta(minutes=2),
            yes_ask_dollars=Decimal("0.6000"),
        ),
    ]
    selection_trades = [
        {
            **filled_market,
            "simulation": {"side": "yes", "net_pnl": "-0.5200", "execution_price_dollars": "0.5000"},
        },
        {
            **unfilled_market,
            "simulation": {"side": "yes", "net_pnl": "0.4800", "execution_price_dollars": "0.5000"},
        },
    ]

    metrics = _crypto_passive_replay_metrics(selection_trades, rows)

    assert metrics["passive_eligible_candidate_count"] == 2
    assert metrics["passive_filled_candidate_count"] == 1
    assert metrics["passive_fill_rate"] == 0.5
    # Filled passive entry at 0.48, market settles NO: gross -0.48 minus 0.01 maker fee.
    assert abs(metrics["passive_gross_simulated_pl_dollars"] - (-0.48)) < 1e-9
    assert abs(metrics["passive_fees_dollars"] - 0.01) < 1e-9
    assert abs(metrics["passive_net_simulated_pl_dollars"] - (-0.49)) < 1e-9
    assert abs(metrics["passive_avg_pnl_filled"] - (-0.49)) < 1e-9
    # Taker net (-0.52 + 0.48 = -0.04) minus passive net (-0.49) = +0.45.
    assert abs(metrics["taker_vs_passive_pl_delta_dollars"] - 0.45) < 1e-9
    # The candidate that filled passively is the losing one (adverse selection).
    assert abs(metrics["passive_filled_avg_settlement_pnl"] - (-0.52)) < 1e-9
    assert abs(metrics["passive_unfilled_avg_settlement_pnl"] - 0.48) < 1e-9


def test_passive_replay_metrics_empty_input() -> None:
    metrics = _crypto_passive_replay_metrics([], [])

    assert metrics["passive_eligible_candidate_count"] == 0
    assert metrics["passive_filled_candidate_count"] == 0
    assert metrics["passive_fill_rate"] is None
    assert metrics["passive_gross_simulated_pl_dollars"] == 0.0
    assert metrics["passive_fees_dollars"] == 0.0
    assert metrics["passive_net_simulated_pl_dollars"] == 0.0
    assert metrics["passive_avg_pnl_filled"] is None
    assert metrics["passive_filled_avg_settlement_pnl"] is None
    assert metrics["passive_unfilled_avg_settlement_pnl"] is None


# --- Task 1: fill observations + note refresh ------------------------------------


def _shrinkage_fill(idx: int, **overrides) -> FillRecord:
    created_at = datetime.now(UTC) - timedelta(days=1, minutes=idx)
    values = {
        "id": f"fill-{idx}",
        "kalshi_env": "demo",
        "trade_id": f"trade-{idx}",
        "market_ticker": "KXBTC15M-OBS",
        "side": "yes",
        "action": "buy",
        "yes_price_dollars": Decimal("0.5000"),
        "count_fp": Decimal("2.00"),
        "settlement_result": None,
        "strategy_code": "CRYPTO_15M",
        "decision_edge_bps": None,
        "raw": {},
        "created_at": created_at,
        "updated_at": created_at,
    }
    values.update(overrides)
    return FillRecord(**values)


async def test_edge_shrinkage_observations_and_refresh_note(tmp_path) -> None:
    settings = _settings(tmp_path, kalshi_env="demo", crypto_edge_shrinkage_lookback_days=30)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    try:
        async with session_factory() as session:
            session.add_all(
                [
                    # Settled win on YES @ 0.50 with 10% predicted edge -> (0.10, 0.50).
                    _shrinkage_fill(1, decision_edge_bps=1000, settlement_result="win"),
                    # Settled loss on NO (yes px 0.60 -> cost 0.40) -> (0.05, -0.40).
                    _shrinkage_fill(
                        2,
                        market_ticker="KXETH15M-OBS",
                        side="no",
                        yes_price_dollars=Decimal("0.6000"),
                        decision_edge_bps=500,
                        settlement_result="loss",
                    ),
                    # Exited via sell @ 0.70 against buy @ 0.40 -> (0.08, 0.30).
                    _shrinkage_fill(
                        3,
                        market_ticker="KXSOL15M-OBS",
                        yes_price_dollars=Decimal("0.4000"),
                        decision_edge_bps=800,
                    ),
                    _shrinkage_fill(
                        4,
                        market_ticker="KXSOL15M-OBS",
                        action="sell",
                        yes_price_dollars=Decimal("0.7000"),
                        count_fp=Decimal("2.00"),
                    ),
                    # No decision edge recorded -> excluded.
                    _shrinkage_fill(5, market_ticker="KXXRP15M-OBS", settlement_result="win"),
                    # Unresolved (no sell, no settlement) -> excluded.
                    _shrinkage_fill(6, market_ticker="KXDOGE15M-OBS", decision_edge_bps=900),
                ]
            )
            await session.commit()

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            observations = await repo.list_crypto_edge_shrinkage_fill_observations(
                strategy_codes=["CRYPTO_15M"],
                since=datetime.now(UTC) - timedelta(days=30),
                kalshi_env="demo",
            )

        expected = [(0.05, -0.40), (0.08, 0.30), (0.10, 0.50)]
        assert len(observations) == 3
        for (predicted, realized), (expected_predicted, expected_realized) in zip(sorted(observations), expected):
            assert abs(predicted - expected_predicted) < 1e-9
            assert abs(realized - expected_realized) < 1e-9

        forecast = CryptoForecastService(settings=settings, session_factory=session_factory)
        result = await forecast.refresh_edge_shrinkage(frequency="15m")

        assert result["status"] == "ok"
        assert result["sample_count"] == 3
        assert result["note_key"] == "crypto_edge_shrinkage:15m"

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            control = await repo.get_deployment_control(kalshi_env="demo")
            note = (control.notes or {}).get("crypto_edge_shrinkage:15m")

        assert isinstance(note, dict)
        assert note["sample_count"] == 3
        assert note["frequency"] == "15m"
        assert 0.2 <= note["beta"] <= 1.0
        assert note["updated_at"] is not None
    finally:
        await engine.dispose()
