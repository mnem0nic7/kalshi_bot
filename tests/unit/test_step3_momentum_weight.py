"""
Step 3 tests: StrategySignal schema, apply_momentum_weight_to_signal, supervisor wiring, eligibility.

Cluster A: weight function boundary conditions
Cluster B: edge_for_eligibility() null-fallback, float promotion
Cluster C: post-processor exception narrowing (DBAPIError/TimeoutError caught; TypeError propagates)
Cluster D: evaluate_trade_eligibility uses edge_for_eligibility() not raw edge_bps
Cluster F: slope extraction (¢/min conversion, < 5 points, signed)
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.core.enums import ContractSide, StandDownReason, TradeAction
from kalshi_bot.core.schemas import ResearchFreshness
from kalshi_bot.services.momentum_calibration import MomentumCalibrationParams
from kalshi_bot.services.signal import StrategySignal, apply_momentum_weight_to_signal, evaluate_trade_eligibility

_T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


# ── helpers ──────────────────────────────────────────────────────────────────


def _signal(
    side: ContractSide | None = ContractSide.YES,
    edge_bps: int = 100,
) -> StrategySignal:
    return StrategySignal(
        fair_yes_dollars=Decimal("0.60"),
        confidence=0.85,
        edge_bps=edge_bps,
        recommended_action=TradeAction.BUY,
        recommended_side=side,
        target_yes_price_dollars=Decimal("0.60"),
        summary="test",
    )


def _params(
    scale: float = 1.0,
    floor: float = 0.3,
    veto: float | None = None,
    staleness_gate: float = 0.5,
) -> MomentumCalibrationParams:
    return MomentumCalibrationParams(
        momentum_weight_scale_cents_per_min=scale,
        momentum_slope_veto_cents_per_min=veto,
        momentum_weight_floor=floor,
        momentum_veto_staleness_gate=staleness_gate,
    )


def _history(slope_cpmin: float, n: int = 10) -> list:
    """Price history rows with given slope in ¢/min (linear, 1-min intervals)."""
    slope_dpm = slope_cpmin / 100.0  # ¢/min → $/min
    return [
        SimpleNamespace(
            observed_at=_T0 + timedelta(minutes=i),
            mid_dollars=Decimal("0.50") + Decimal(str(round(slope_dpm * i, 8))),
        )
        for i in range(n)
    ]


def _run(
    signal: StrategySignal,
    *,
    slope_cpmin: float = 0.0,
    n: int = 10,
    scale: float = 1.0,
    floor: float = 0.3,
    staleness: float = 0.0,
    shadow_mode: bool = True,
    research_stale_seconds: int = 900,
) -> StrategySignal:
    """Convenience wrapper: sets staleness via reference_time offset from _T0."""
    h = _history(slope_cpmin=slope_cpmin, n=n)
    ref_time = _T0 + timedelta(seconds=staleness * research_stale_seconds)
    return apply_momentum_weight_to_signal(
        signal,
        params=_params(scale=scale, floor=floor),
        price_history=h,
        research_stale_seconds=research_stale_seconds,
        bundle_age_reference=_T0,
        shadow_mode=shadow_mode,
        reference_time=ref_time,
    )


def _fresh_eligibility(
    signal: StrategySignal,
    *,
    min_edge_bps: int = 50,
    quality_buffer_bps: int = 10,
    max_spread_bps: int = 3000,
) -> "TradeEligibilityVerdict":
    from kalshi_bot.config import Settings

    settings = Settings(
        database_url="sqlite+aiosqlite:///tmp_test.db",
        risk_min_edge_bps=min_edge_bps,
        strategy_quality_edge_buffer_bps=quality_buffer_bps,
        trigger_max_spread_bps=max_spread_bps,
    )
    freshness = ResearchFreshness(
        refreshed_at=_T0,
        expires_at=_T0 + timedelta(seconds=900),
        stale=False,
    )
    market_snapshot = {
        "yes_bid_dollars": "0.54",
        "yes_ask_dollars": "0.56",
        "no_ask_dollars": "0.45",
    }
    thresholds = SimpleNamespace(
        risk_min_edge_bps=min_edge_bps,
        trigger_max_spread_bps=max_spread_bps,
        strategy_quality_edge_buffer_bps=quality_buffer_bps,
        strategy_min_remaining_payout_bps=500,
    )
    return evaluate_trade_eligibility(
        settings=settings,
        signal=signal,
        market_snapshot=market_snapshot,
        market_observed_at=_T0,
        research_freshness=freshness,
        thresholds=thresholds,
        decision_time=_T0,
    )


# ── Cluster A: weight function ────────────────────────────────────────────────


class TestCA_WeightFunction:
    def test_A1_zero_adverse_slope_gives_weight_one(self):
        # slope_cpmin=0 → slope_against=0 → base_w=1 → effective_w=1 at any staleness
        result = _run(_signal(ContractSide.YES), slope_cpmin=0.0, staleness=1.0)
        assert result.momentum_weight == pytest.approx(1.0)

    def test_A2_slope_at_scale_clamps_to_floor_when_stale(self):
        # YES, slope_cpmin=-1.0 → slope_against=1.0 = scale=1.0
        # base_w = max(0.3, 1 - 1.0/1.0) = max(0.3, 0.0) = 0.3
        # staleness=1 → effective_w = base_w = 0.3
        result = _run(_signal(ContractSide.YES), slope_cpmin=-1.0, scale=1.0, floor=0.3, staleness=1.0)
        assert result.momentum_weight == pytest.approx(0.3)

    def test_A3_slope_exceeds_scale_still_clamps_to_floor(self):
        # YES, slope_cpmin=-2.0 → slope_against=2.0 > scale=1.0
        # base_w = max(0.3, 1 - 2.0/1.0) = max(0.3, -1.0) = 0.3
        result = _run(_signal(ContractSide.YES), slope_cpmin=-2.0, scale=1.0, floor=0.3, staleness=1.0)
        assert result.momentum_weight == pytest.approx(0.3)

    def test_A4_favorable_slope_gives_weight_one_even_when_stale(self):
        # YES, slope_cpmin=+1.0 → slope_against=-1.0 → max(0, -1.0)=0 → base_w=1
        result = _run(_signal(ContractSide.YES), slope_cpmin=1.0, scale=1.0, floor=0.3, staleness=1.0)
        assert result.momentum_weight == pytest.approx(1.0)

    def test_A5_fresh_model_gives_weight_one_regardless_of_adverse_slope(self):
        # Very adverse slope, but staleness=0 → effective_w = 1 (no discount for fresh model)
        result = _run(_signal(ContractSide.YES), slope_cpmin=-5.0, scale=1.0, floor=0.3, staleness=0.0)
        assert result.momentum_weight == pytest.approx(1.0)

    def test_A6_fully_stale_model_gives_base_w(self):
        # YES, slope_cpmin=-0.5 → slope_against=0.5 → base_w = max(0.3, 1-0.5/1.0) = 0.5
        # staleness=1 → effective_w = base_w = 0.5
        result = _run(_signal(ContractSide.YES), slope_cpmin=-0.5, scale=1.0, floor=0.3, staleness=1.0)
        assert result.momentum_weight == pytest.approx(0.5)

    def test_A7_no_side_stamps_slope_only(self):
        # No recommended_side → slope stamped but weight/edge not computed
        signal = _signal(side=None)
        result = _run(signal, slope_cpmin=-1.0)
        assert result.momentum_slope_cents_per_min is not None
        assert result.momentum_weight is None

    def test_A8_shadow_mode_true_does_not_stamp_edge_effective(self):
        result = _run(_signal(ContractSide.YES), slope_cpmin=-1.0, staleness=1.0, shadow_mode=True)
        assert result.edge_effective_bps is None
        assert result.momentum_weight is not None  # weight still stamped

    def test_A9_shadow_mode_false_stamps_edge_effective(self):
        # base_w=0.3, staleness=1 → effective_w=0.3; edge_bps=100 → edge_effective=30
        result = _run(
            _signal(ContractSide.YES, edge_bps=100),
            slope_cpmin=-1.0,
            scale=1.0,
            floor=0.3,
            staleness=1.0,
            shadow_mode=False,
        )
        assert result.edge_effective_bps == pytest.approx(30.0)

    def test_A10_no_side_adverse_slope_for_no_trade(self):
        # NO trade, price going up (slope_cpmin=+1.0) = adverse for NO
        # slope_against = slope_cpmin = 1.0 = scale → base_w = floor
        result = _run(_signal(ContractSide.NO), slope_cpmin=1.0, scale=1.0, floor=0.3, staleness=1.0)
        assert result.momentum_weight == pytest.approx(0.3)

    def test_A11_partial_staleness_interpolates(self):
        # base_w=0.5 (from A6 setup), staleness=0.5
        # effective_w = 1 - 0.5*(1-0.5) = 1 - 0.25 = 0.75
        result = _run(_signal(ContractSide.YES), slope_cpmin=-0.5, scale=1.0, floor=0.3, staleness=0.5)
        assert result.momentum_weight == pytest.approx(0.75)


# ── Cluster B: edge_for_eligibility() ────────────────────────────────────────


class TestCB_EdgeForEligibility:
    def test_B1_none_effective_falls_back_to_edge_bps(self):
        s = _signal(edge_bps=150)
        assert s.edge_for_eligibility() == 150.0

    def test_B2_set_effective_bps_returned(self):
        s = _signal(edge_bps=150)
        s.edge_effective_bps = 90.5
        assert s.edge_for_eligibility() == pytest.approx(90.5)

    def test_B3_return_type_is_float(self):
        s = _signal(edge_bps=100)
        result = s.edge_for_eligibility()
        assert isinstance(result, float)

    def test_B4_zero_effective_not_treated_as_none(self):
        s = _signal(edge_bps=100)
        s.edge_effective_bps = 0.0
        assert s.edge_for_eligibility() == 0.0  # 0.0 != None, no fallback

    def test_B5_int_edge_bps_promoted_to_float(self):
        s = _signal(edge_bps=200)
        assert s.edge_for_eligibility() == 200.0
        assert isinstance(s.edge_for_eligibility(), float)

    def test_B6_negative_effective_returned_as_is(self):
        s = _signal(edge_bps=50)
        s.edge_effective_bps = -10.0
        assert s.edge_for_eligibility() == pytest.approx(-10.0)


# ── Cluster C: post-processor exception narrowing ─────────────────────────────


def _make_supervisor(tmp_path):
    """Build a WorkflowSupervisor with all dependencies mocked except settings."""
    from kalshi_bot.config import Settings
    from kalshi_bot.orchestration.supervisor import WorkflowSupervisor

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/test.db")

    sup = WorkflowSupervisor.__new__(WorkflowSupervisor)
    sup.settings = settings
    sup._momentum_post_processor_rate_limit = {}
    return sup


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_checkpoint = AsyncMock(return_value=None)
    repo.fetch_recent_prices = AsyncMock(return_value=[])
    repo.log_ops_event = AsyncMock()
    return repo


class TestCC_PostProcessorExceptionNarrowing:
    @pytest.fixture
    def supervisor(self, tmp_path):
        return _make_supervisor(tmp_path)

    @pytest.fixture
    def repo(self):
        return _make_repo()

    async def test_C1_dbapi_error_leaves_fields_none(self, supervisor, repo):
        from sqlalchemy.exc import DBAPIError

        repo.fetch_recent_prices = AsyncMock(
            side_effect=DBAPIError("stmt", {}, Exception("orig"))
        )
        signal = _signal(ContractSide.YES)
        result = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert result.momentum_slope_cents_per_min is None
        assert result.momentum_weight is None
        assert result.edge_effective_bps is None

    async def test_C2_dbapi_error_emits_warning_ops_event(self, supervisor, repo):
        from sqlalchemy.exc import DBAPIError

        repo.fetch_recent_prices = AsyncMock(
            side_effect=DBAPIError("stmt", {}, Exception("orig"))
        )
        signal = _signal(ContractSide.YES)
        await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        repo.log_ops_event.assert_awaited_once()
        kwargs = repo.log_ops_event.call_args.kwargs
        assert kwargs["severity"] == "warning"
        assert kwargs["source"] == "momentum_post_processor"

    async def test_C3_rate_limit_suppresses_second_ops_event_within_window(self, supervisor, repo):
        from sqlalchemy.exc import DBAPIError

        repo.fetch_recent_prices = AsyncMock(
            side_effect=DBAPIError("stmt", {}, Exception("orig"))
        )
        signal = _signal(ContractSide.YES)
        # Two calls in quick succession — same (env, exc_class) key
        await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert repo.log_ops_event.await_count == 1

    async def test_C4_timeout_error_leaves_fields_none(self, supervisor, repo):
        repo.fetch_recent_prices = AsyncMock(side_effect=asyncio.TimeoutError())
        signal = _signal(ContractSide.YES)
        result = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert result.momentum_slope_cents_per_min is None

    async def test_C5_type_error_propagates(self, supervisor, repo):
        signal = _signal(ContractSide.YES)
        with patch(
            "kalshi_bot.orchestration.supervisor.apply_momentum_weight_to_signal",
            side_effect=TypeError("programming error"),
        ):
            with pytest.raises(TypeError, match="programming error"):
                await supervisor._try_apply_momentum_post_processor(
                    signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
                )


# ── Cluster D: eligibility uses effective edge ────────────────────────────────


class TestCD_EligibilityUsesEffectiveEdge:
    def test_D1_none_effective_falls_back_to_raw_edge_for_eligibility(self):
        # edge_bps=100, effective=None → edge_for_eligibility()=100 → 100-10=90 >= 50 → eligible
        s = _signal(edge_bps=100)
        assert s.edge_effective_bps is None
        verdict = _fresh_eligibility(s, min_edge_bps=50, quality_buffer_bps=10)
        assert verdict.eligible

    def test_D2_suppressed_effective_edge_causes_stand_down(self):
        # edge_bps=100 but effective=20 (momentum discounted) → 20-10=10 < 50 → stand down
        s = _signal(edge_bps=100)
        s.edge_effective_bps = 20.0
        verdict = _fresh_eligibility(s, min_edge_bps=50, quality_buffer_bps=10)
        assert not verdict.eligible
        assert verdict.stand_down_reason == StandDownReason.NO_ACTIONABLE_EDGE

    def test_D3_sufficient_effective_edge_passes(self):
        # edge_bps=100, effective=65 → 65-10=55 >= 50 → eligible
        s = _signal(edge_bps=100)
        s.edge_effective_bps = 65.0
        verdict = _fresh_eligibility(s, min_edge_bps=50, quality_buffer_bps=10)
        assert verdict.eligible

    def test_D4_effective_edge_exactly_at_threshold_passes(self):
        # 60 - 10 = 50 exactly = min_edge_bps → eligible (not strictly less than)
        s = _signal(edge_bps=100)
        s.edge_effective_bps = 60.0
        verdict = _fresh_eligibility(s, min_edge_bps=50, quality_buffer_bps=10)
        assert verdict.eligible

    def test_D5_shadow_mode_signal_uses_raw_edge(self):
        # shadow_mode=True: apply_momentum_weight leaves edge_effective_bps=None
        # Simulate: run post-processor in shadow mode, then check eligibility uses raw
        s = _signal(ContractSide.YES, edge_bps=100)
        _run(s, slope_cpmin=-1.0, staleness=1.0, shadow_mode=True)
        # After shadow mode: effective=None, weight is stamped
        assert s.edge_effective_bps is None
        assert s.momentum_weight is not None
        # Eligibility should use raw edge_bps=100 → 100-10=90 >= 50 → eligible
        verdict = _fresh_eligibility(s, min_edge_bps=50, quality_buffer_bps=10)
        assert verdict.eligible


# ── Cluster F: slope extraction ───────────────────────────────────────────────


class TestCF_SlopeExtraction:
    def test_F1_dollars_per_second_to_cents_per_minute(self):
        # _history generates exactly linear data; polyfit should recover slope
        h = _history(slope_cpmin=2.0)
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min == pytest.approx(2.0, abs=1e-3)

    def test_F2_fewer_than_five_points_leaves_fields_none(self):
        h = _history(slope_cpmin=2.0, n=4)
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min is None
        assert signal.momentum_weight is None
        assert signal.edge_effective_bps is None

    def test_F3_exactly_five_points_computes_correctly(self):
        h = _history(slope_cpmin=1.5, n=5)
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min == pytest.approx(1.5, abs=1e-3)

    def test_F4_negative_slope_stamped_signed_not_clamped(self):
        h = _history(slope_cpmin=-3.0)
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min is not None
        assert signal.momentum_slope_cents_per_min < 0
        assert signal.momentum_slope_cents_per_min == pytest.approx(-3.0, abs=1e-3)

    def test_F5_rows_with_none_mid_excluded_from_fit(self):
        h = _history(slope_cpmin=1.0, n=8)
        # Inject Nones into two rows — should still have 6 valid points
        h[2] = SimpleNamespace(observed_at=h[2].observed_at, mid_dollars=None)
        h[5] = SimpleNamespace(observed_at=h[5].observed_at, mid_dollars=None)
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min is not None

    def test_F6_zero_valid_points_leaves_fields_none(self):
        h = [SimpleNamespace(observed_at=_T0, mid_dollars=None) for _ in range(10)]
        signal = _signal(ContractSide.YES)
        apply_momentum_weight_to_signal(
            signal,
            params=_params(),
            price_history=h,
            research_stale_seconds=900,
        )
        assert signal.momentum_slope_cents_per_min is None
