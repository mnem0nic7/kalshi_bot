from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.decision_trace import (
    build_deterministic_decision_trace,
    replay_decision_trace,
    stable_hash,
)


def test_stable_hash_ignores_mapping_order_and_normalizes_scalars() -> None:
    left = {"b": Decimal("0.2500"), "a": datetime(2026, 4, 27, tzinfo=UTC)}
    right = {"a": "2026-04-27T00:00:00+00:00", "b": "0.2500"}

    assert stable_hash(left) == stable_hash(right)


def test_build_and_replay_deterministic_decision_trace_round_trips() -> None:
    room = SimpleNamespace(
        id="room-1",
        market_ticker="KXHIGHNY-26APR27-T69",
        kalshi_env="demo",
        agent_pack_version="builtin-gemini-v1",
    )
    signal = SimpleNamespace(
        fair_yes_dollars=Decimal("0.8100"),
        edge_bps=6100,
        confidence=0.88,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.NO,
        target_yes_price_dollars=Decimal("0.2000"),
        capital_bucket="safe",
        trade_regime="standard",
        strategy_mode=None,
        resolution_state=None,
        stand_down_reason=None,
        evaluation_outcome="approved",
        size_factor=Decimal("1.00"),
        recommended_size_cap_fp=None,
        summary="Selected NO; YES below min edge",
        eligibility=SimpleNamespace(
            eligible=True,
            evaluation_outcome="candidate_selected",
            stand_down_reason=None,
        ),
        weather=None,
    )
    thresholds = RuntimeThresholds(
        risk_min_edge_bps=10,
        risk_max_order_notional_dollars=50.0,
        risk_max_position_notional_dollars=100.0,
        trigger_max_spread_bps=1200,
        trigger_cooldown_seconds=0,
        strategy_quality_edge_buffer_bps=0,
        strategy_min_remaining_payout_bps=100,
    )
    ticket_record = SimpleNamespace(
        id="ticket-1",
        market_ticker=room.market_ticker,
        action="buy",
        side="no",
        yes_price_dollars=Decimal("0.2000"),
        count_fp=Decimal("2.00"),
        time_in_force="immediate_or_cancel",
        client_order_id="client-1",
        status="proposed",
        strategy_code="A",
        payload={"nonce": "n-1"},
    )
    risk_record = SimpleNamespace(
        id="risk-1",
        status="approved",
        reasons=[],
        approved_notional_dollars=Decimal("1.60"),
        approved_count_fp=Decimal("2.00"),
        payload={"status": "approved"},
    )
    receipt = SimpleNamespace(status="shadow_skipped", details={"shadow": True})

    input_hash, trace_hash, trace = build_deterministic_decision_trace(
        room=room,
        signal=signal,
        thresholds=thresholds,
        candidate_trace={"outcome": "candidate_selected", "selected_side": "no"},
        final_status="shadow_skipped",
        evaluation_outcome="approved",
        ticket_record=ticket_record,
        risk_verdict_record=risk_record,
        receipt=receipt,
        market_observed_at=datetime(2026, 4, 27, 18, 0, tzinfo=UTC),
        source_snapshot_ids={"market_state": {"market_ticker": room.market_ticker}},
        sizing={"suggested_count_fp": Decimal("2.00")},
    )
    replay = replay_decision_trace(trace, expected_trace_hash=trace_hash)

    assert trace["decision_kind"] == "entry"
    assert trace["input_hash"] == input_hash
    assert trace["trace_hash"] == trace_hash
    assert replay.ok is True
    assert replay.normalized_intent["ticket_side"] == "no"
    assert replay.normalized_intent["risk_status"] == "approved"


def test_replay_detects_trace_tampering() -> None:
    trace = {
        "schema_version": "decision_trace.v1",
        "path_version": "deterministic-fast-path.v1",
        "decision_kind": "stand_down",
        "market_ticker": "KXHIGHNY-26APR27-T69",
        "kalshi_env": "demo",
        "final_status": "stand_down",
        "evaluation_outcome": "pre_risk_filtered",
        "inputs": {"market_ticker": "KXHIGHNY-26APR27-T69"},
        "signal": {},
        "candidate_trace": {},
    }
    trace["input_hash"] = stable_hash(trace["inputs"])
    trace["trace_hash"] = stable_hash({"wrong": True})

    replay = replay_decision_trace(trace)

    assert replay.ok is False
    assert "trace_hash" in replay.mismatches
