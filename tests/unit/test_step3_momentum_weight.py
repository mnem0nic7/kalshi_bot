"""
Step 3 tests: StrategySignal schema, apply_momentum_weight_to_signal, supervisor wiring, eligibility.

Cluster A: weight function boundary conditions
Cluster B: edge_for_eligibility() null-fallback, float promotion
Cluster C: post-processor exception narrowing (DBAPIError/TimeoutError caught; TypeError propagates)
Cluster D: evaluate_trade_eligibility uses edge_for_eligibility() not raw edge_bps
Cluster F: slope extraction (¢/min conversion, < 5 points, signed)
Cluster G: get_momentum_shadow_metrics repo aggregation
Cluster H: _momentum_calibration_summary shadow_metrics sub-key shape
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
        result, outcome = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert result.momentum_slope_cents_per_min is None
        assert result.momentum_weight is None
        assert result.edge_effective_bps is None
        assert outcome == "price_history_error"

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
        result, outcome = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert result.momentum_slope_cents_per_min is None
        assert outcome == "price_history_error"

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

    async def test_C6_no_checkpoint_yields_calibration_missing_outcome(self, supervisor, repo):
        # get_checkpoint returns None → checkpoint_exists=False → "calibration_missing"
        # fetch_recent_prices returns enough points for a slope
        repo.get_checkpoint = AsyncMock(return_value=None)
        repo.fetch_recent_prices = AsyncMock(return_value=_history(slope_cpmin=0.5, n=10))
        signal = _signal(ContractSide.YES)
        result, outcome = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert outcome == "calibration_missing"
        # slope is still stamped even when calibration is missing
        assert result.momentum_slope_cents_per_min is not None

    async def test_C7_few_price_points_yields_insufficient_points_outcome(self, supervisor, repo):
        # checkpoint exists but < 5 price points → slope is None → "insufficient_points"
        from unittest.mock import MagicMock
        cp = MagicMock()
        cp.payload = {}
        repo.get_checkpoint = AsyncMock(return_value=cp)
        repo.fetch_recent_prices = AsyncMock(return_value=_history(slope_cpmin=0.5, n=3))
        signal = _signal(ContractSide.YES)
        result, outcome = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert outcome == "insufficient_points"
        assert result.momentum_slope_cents_per_min is None

    async def test_C8_success_outcome_when_checkpoint_and_enough_points(self, supervisor, repo):
        from unittest.mock import MagicMock
        cp = MagicMock()
        cp.payload = {}
        repo.get_checkpoint = AsyncMock(return_value=cp)
        repo.fetch_recent_prices = AsyncMock(return_value=_history(slope_cpmin=0.5, n=10))
        signal = _signal(ContractSide.YES)
        result, outcome = await supervisor._try_apply_momentum_post_processor(
            signal, repo=repo, market_ticker="WX-TEST", bundle_age_reference=None
        )
        assert outcome == "success"
        assert result.momentum_slope_cents_per_min is not None


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


# ── Cluster G: get_momentum_shadow_metrics ────────────────────────────────────


def _make_payloads(rows: list[dict]) -> list[dict | None]:
    """Build a list of signal payload dicts for mock repo responses."""
    return rows


class TestCG_ShadowMetricsRepo:
    """Unit tests for get_momentum_shadow_metrics using a mock session."""

    def _make_repo_with_payloads(self, payloads: list) -> MagicMock:
        from unittest.mock import MagicMock, AsyncMock

        mock_result = MagicMock()
        mock_result.scalars.return_value = payloads
        session = MagicMock()
        session.execute = AsyncMock(return_value=mock_result)

        from kalshi_bot.db.repositories import PlatformRepository
        repo = PlatformRepository.__new__(PlatformRepository)
        repo.session = session
        repo._kalshi_env = "demo"
        return repo

    async def test_G1_empty_window_returns_zero_counts(self):
        from kalshi_bot.db.repositories import PlatformRepository
        repo = self._make_repo_with_payloads([])
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["total"] == 0
        assert result["by_outcome"]["success"] == 0
        assert result["avg_slope_cents_per_min"] is None
        assert result["avg_weight"] is None
        assert result["veto_fraction"] is None

    async def test_G2_success_rows_counted_and_avg_weight_computed(self):
        from kalshi_bot.db.repositories import PlatformRepository
        payloads = [
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": 1.0, "momentum_weight": 0.8},
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": 2.0, "momentum_weight": 0.6},
        ]
        repo = self._make_repo_with_payloads(payloads)
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["by_outcome"]["success"] == 2
        assert result["avg_slope_cents_per_min"] == pytest.approx(1.5)
        assert result["avg_weight"] == pytest.approx(0.7)

    async def test_G3_calibration_missing_rows_counted_slope_included_in_avg(self):
        from kalshi_bot.db.repositories import PlatformRepository
        payloads = [
            {"momentum_post_processor_outcome": "calibration_missing", "momentum_slope_cents_per_min": 3.0, "momentum_weight": 0.5},
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": 1.0, "momentum_weight": 0.9},
        ]
        repo = self._make_repo_with_payloads(payloads)
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["by_outcome"]["calibration_missing"] == 1
        assert result["by_outcome"]["success"] == 1
        # slope avg includes both rows (calibration_missing still has slope stamped)
        assert result["avg_slope_cents_per_min"] == pytest.approx(2.0)
        # weight avg is success-only
        assert result["avg_weight"] == pytest.approx(0.9)

    async def test_G4_price_history_error_rows_have_no_slope_contribution(self):
        from kalshi_bot.db.repositories import PlatformRepository
        payloads = [
            {"momentum_post_processor_outcome": "price_history_error"},
            {"momentum_post_processor_outcome": "price_history_error"},
        ]
        repo = self._make_repo_with_payloads(payloads)
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["by_outcome"]["price_history_error"] == 2
        assert result["avg_slope_cents_per_min"] is None
        assert result["avg_weight"] is None

    async def test_G5_none_payload_counted_as_unknown(self):
        from kalshi_bot.db.repositories import PlatformRepository
        repo = self._make_repo_with_payloads([None, None])
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["by_outcome"]["unknown"] == 2
        assert result["total"] == 2

    async def test_G6_veto_fraction_computed_for_success_rows_exceeding_threshold(self):
        from kalshi_bot.db.repositories import PlatformRepository
        payloads = [
            # slope_against = |slope| for a YES signal with negative slope = adverse
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": -2.0, "momentum_weight": 0.5},
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": -0.2, "momentum_weight": 0.9},
            {"momentum_post_processor_outcome": "success", "momentum_slope_cents_per_min": 0.5, "momentum_weight": 0.8},
        ]
        repo = self._make_repo_with_payloads(payloads)
        # threshold=1.0 → only |−2.0| > 1.0 qualifies
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo", veto_threshold_cents_per_min=1.0)
        assert result["veto_fraction"] == pytest.approx(1 / 3)

    async def test_G7_unknown_outcome_string_bucketed_as_unknown(self):
        from kalshi_bot.db.repositories import PlatformRepository
        payloads = [
            {"momentum_post_processor_outcome": "unexpected_future_value"},
        ]
        repo = self._make_repo_with_payloads(payloads)
        result = await repo.get_momentum_shadow_metrics(kalshi_env="demo")
        assert result["by_outcome"]["unknown"] == 1


# ── Cluster H: control room shadow_metrics card shape ─────────────────────────


class TestCH_ControlRoomShadowMetrics:
    """Unit tests for _momentum_calibration_summary shadow_metrics sub-key."""

    async def test_H1_summary_includes_shadow_metrics_key(self):
        """_momentum_calibration_summary must include a 'shadow_metrics' key."""
        from kalshi_bot.web.control_room import _momentum_calibration_summary

        shadow_stub = {
            "window_hours": 24,
            "total": 5,
            "by_outcome": {"success": 3, "calibration_missing": 1, "insufficient_points": 1,
                           "price_history_error": 0, "unknown": 0},
            "avg_slope_cents_per_min": 1.2,
            "avg_weight": 0.75,
            "veto_fraction": 0.0,
        }

        mock_repo = AsyncMock()
        mock_repo.get_checkpoint = AsyncMock(return_value=None)
        mock_repo.get_momentum_shadow_metrics = AsyncMock(return_value=shadow_stub)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.commit = AsyncMock()

        with patch("kalshi_bot.web.control_room.PlatformRepository", return_value=mock_repo):
            with patch("kalshi_bot.services.momentum_calibration.get_momentum_calibration_state",
                       new_callable=AsyncMock, return_value={"active": None, "pending": None}):
                container = MagicMock()
                container.session_factory.return_value = mock_session
                container.settings.kalshi_env = "demo"
                container.settings.app_color = "blue"
                container.settings.momentum_slope_veto_cents_per_min = 0.5

                result = await _momentum_calibration_summary(container)

        assert "shadow_metrics" in result
        sm = result["shadow_metrics"]
        assert sm["window_hours"] == 24
        assert sm["total"] == 5
        assert "by_outcome" in sm
        assert "avg_slope_cents_per_min" in sm
        assert "avg_weight" in sm
        assert "veto_fraction" in sm

    async def test_H2_shadow_metrics_by_outcome_has_all_four_outcome_keys(self):
        """by_outcome must contain success/calibration_missing/insufficient_points/price_history_error."""
        from kalshi_bot.web.control_room import _momentum_calibration_summary

        shadow_stub = {
            "window_hours": 24,
            "total": 0,
            "by_outcome": {"success": 0, "calibration_missing": 0, "insufficient_points": 0,
                           "price_history_error": 0, "unknown": 0},
            "avg_slope_cents_per_min": None,
            "avg_weight": None,
            "veto_fraction": None,
        }

        mock_repo = AsyncMock()
        mock_repo.get_checkpoint = AsyncMock(return_value=None)
        mock_repo.get_momentum_shadow_metrics = AsyncMock(return_value=shadow_stub)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.commit = AsyncMock()

        with patch("kalshi_bot.web.control_room.PlatformRepository", return_value=mock_repo):
            with patch("kalshi_bot.services.momentum_calibration.get_momentum_calibration_state",
                       new_callable=AsyncMock, return_value={"active": None, "pending": None}):
                container = MagicMock()
                container.session_factory.return_value = mock_session
                container.settings.kalshi_env = "demo"
                container.settings.app_color = "blue"
                container.settings.momentum_slope_veto_cents_per_min = 0.5

                result = await _momentum_calibration_summary(container)

        by_outcome = result["shadow_metrics"]["by_outcome"]
        for key in ("success", "calibration_missing", "insufficient_points", "price_history_error"):
            assert key in by_outcome
