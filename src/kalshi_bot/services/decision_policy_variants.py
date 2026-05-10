from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from kalshi_bot.config import Settings

LOW_PRICE_HIGH_EDGE = "low_price_high_edge"
REMAINING_PAYOUT_RELAXATION = "remaining_payout_relaxation"
INTRADAY_SEPARATION_OVERRIDE = "intraday_separation_override"
EMPIRICAL_BOOTSTRAP = "empirical_bootstrap"

POLICY_VARIANT_KEYS = (
    LOW_PRICE_HIGH_EDGE,
    REMAINING_PAYOUT_RELAXATION,
    INTRADAY_SEPARATION_OVERRIDE,
    EMPIRICAL_BOOTSTRAP,
)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _decimal_text(value: Any) -> str | None:
    decimal = _decimal_or_none(value)
    return f"{decimal:.4f}" if decimal is not None else None


@dataclass(slots=True)
class PolicyVariantDecision:
    key: str
    matched: bool
    live_enabled: bool
    shadow_enabled: bool
    applied: bool
    would_have_entered: bool
    reason: str
    thresholds: dict[str, Any]
    diagnostics: dict[str, Any]

    def to_trace(self) -> dict[str, Any]:
        return {
            "matched": self.matched,
            "shadow_enabled": self.shadow_enabled,
            "live_enabled": self.live_enabled,
            "would_have_entered": self.would_have_entered,
            "applied": self.applied,
            "reason": self.reason,
            "thresholds": self.thresholds,
            "diagnostics": self.diagnostics,
        }


@dataclass(slots=True)
class CandidatePolicyResult:
    policy_variants: dict[str, dict[str, Any]]
    applied_variant: str | None
    selected_candidate: dict[str, Any] | None


@dataclass(slots=True)
class DecisionPolicyVariantService:
    settings: Settings

    @property
    def shadow_enabled(self) -> bool:
        return bool(self.settings.decision_policy_variants_shadow_enabled)

    def live_enabled(self, key: str) -> bool:
        if key == LOW_PRICE_HIGH_EDGE:
            return bool(self.settings.low_price_high_edge_live_enabled)
        if key == REMAINING_PAYOUT_RELAXATION:
            return bool(self.settings.remaining_payout_relaxation_live_enabled)
        if key == INTRADAY_SEPARATION_OVERRIDE:
            return bool(self.settings.intraday_separation_override_live_enabled)
        if key == EMPIRICAL_BOOTSTRAP:
            return bool(self.settings.empirical_bootstrap_live_enabled)
        return False

    def empty_trace_fields(self) -> dict[str, Any]:
        return {
            "policy_variants": {},
            "policy_variant_applied": None,
            "baseline_block_reason": None,
        }

    def merge_trace(
        self,
        candidate_trace: dict[str, Any] | None,
        *,
        decisions: dict[str, dict[str, Any]] | None = None,
        applied_variant: str | None = None,
        baseline_block_reason: str | None = None,
    ) -> dict[str, Any]:
        trace = dict(candidate_trace or {})
        existing = trace.get("policy_variants") if isinstance(trace.get("policy_variants"), dict) else {}
        trace["policy_variants"] = {**existing, **dict(decisions or {})}
        if applied_variant is not None:
            trace["policy_variant_applied"] = applied_variant
        else:
            trace.setdefault("policy_variant_applied", None)
        if baseline_block_reason is not None:
            trace["baseline_block_reason"] = baseline_block_reason
        else:
            trace.setdefault("baseline_block_reason", None)
        return trace

    def evaluate_candidate_variants(self, candidates: list[dict[str, Any]]) -> CandidatePolicyResult:
        decisions: dict[str, dict[str, Any]] = {}
        live_candidates: list[tuple[dict[str, Any], str]] = []
        for candidate in candidates:
            decision = self._candidate_decision(candidate)
            if decision is None:
                continue
            decisions[decision.key] = decision.to_trace()
            if decision.live_enabled:
                live_candidates.append((candidate, decision.key))
        if not live_candidates:
            return CandidatePolicyResult(policy_variants=decisions, applied_variant=None, selected_candidate=None)
        selected, key = max(
            live_candidates,
            key=lambda item: (
                _int_or_none(item[0].get("quality_adjusted_edge_bps")) or -1_000_000,
                _int_or_none(item[0].get("edge_bps")) or -1_000_000,
                _decimal_or_none(item[0].get("traded_price_dollars")) or Decimal("0"),
            ),
        )
        decisions[key]["applied"] = True
        return CandidatePolicyResult(
            policy_variants=decisions,
            applied_variant=key,
            selected_candidate=dict(selected),
        )

    def _candidate_decision(self, candidate: dict[str, Any]) -> PolicyVariantDecision | None:
        reason = str(candidate.get("reason") or "")
        qa_edge = _int_or_none(candidate.get("quality_adjusted_edge_bps"))
        if qa_edge is None or qa_edge < self.settings.policy_variant_min_quality_adjusted_edge_bps:
            return None
        if candidate.get("spread_status") == "too_wide":
            return None
        if reason == "below_min_contract_price":
            return self._low_price_decision(candidate, qa_edge)
        if reason == "insufficient_remaining_payout":
            return self._remaining_payout_decision(candidate, qa_edge)
        return None

    def _low_price_decision(self, candidate: dict[str, Any], qa_edge: int) -> PolicyVariantDecision | None:
        entry_price = _decimal_or_none(candidate.get("traded_price_dollars"))
        policy_floor = Decimal(str(self.settings.low_price_variant_min_entry_price_dollars))
        baseline_floor = Decimal(str(self.settings.risk_min_contract_price_dollars))
        if entry_price is None or entry_price < policy_floor:
            return None
        live_enabled = self.live_enabled(LOW_PRICE_HIGH_EDGE)
        return PolicyVariantDecision(
            key=LOW_PRICE_HIGH_EDGE,
            matched=True,
            live_enabled=live_enabled,
            shadow_enabled=self.shadow_enabled,
            applied=False,
            would_have_entered=self.shadow_enabled or live_enabled,
            reason="baseline entry price floor blocked a high-edge candidate above the policy floor",
            thresholds={
                "baseline_min_entry_price_dollars": _decimal_text(baseline_floor),
                "policy_min_entry_price_dollars": _decimal_text(policy_floor),
                "min_quality_adjusted_edge_bps": self.settings.policy_variant_min_quality_adjusted_edge_bps,
            },
            diagnostics={
                "side": candidate.get("side"),
                "entry_price_dollars": _decimal_text(entry_price),
                "quality_adjusted_edge_bps": qa_edge,
                "baseline_reason": candidate.get("reason"),
            },
        )

    def _remaining_payout_decision(self, candidate: dict[str, Any], qa_edge: int) -> PolicyVariantDecision | None:
        remaining = _decimal_or_none(candidate.get("remaining_payout_dollars"))
        policy_floor = Decimal(self.settings.remaining_payout_variant_min_payout_bps) / Decimal("10000")
        if remaining is None or remaining <= policy_floor:
            return None
        live_enabled = self.live_enabled(REMAINING_PAYOUT_RELAXATION)
        return PolicyVariantDecision(
            key=REMAINING_PAYOUT_RELAXATION,
            matched=True,
            live_enabled=live_enabled,
            shadow_enabled=self.shadow_enabled,
            applied=False,
            would_have_entered=self.shadow_enabled or live_enabled,
            reason="baseline remaining-payout floor blocked a high-edge candidate above the policy floor",
            thresholds={
                "baseline_min_remaining_payout_bps": self.settings.strategy_min_remaining_payout_bps,
                "policy_min_remaining_payout_bps": self.settings.remaining_payout_variant_min_payout_bps,
                "min_quality_adjusted_edge_bps": self.settings.policy_variant_min_quality_adjusted_edge_bps,
            },
            diagnostics={
                "side": candidate.get("side"),
                "remaining_payout_dollars": _decimal_text(remaining),
                "quality_adjusted_edge_bps": qa_edge,
                "baseline_reason": candidate.get("reason"),
            },
        )

    def intraday_separation_decision(
        self,
        *,
        fair_yes_dollars: Any,
        forecast_delta_f: float | None,
        prediction_provenance: dict[str, Any] | None,
        edge_after_quality_buffer_bps: float | int | None,
    ) -> PolicyVariantDecision | None:
        qa_edge = _int_or_none(edge_after_quality_buffer_bps)
        if qa_edge is None or qa_edge < self.settings.policy_variant_min_quality_adjusted_edge_bps:
            return None
        provenance = dict(prediction_provenance or {})
        intraday = provenance.get("intraday_model")
        if not isinstance(intraday, dict) or intraday.get("status") != "used":
            return None
        fair_yes = _decimal_or_none(fair_yes_dollars)
        low = Decimal(str(self.settings.intraday_resolved_low_fair_yes))
        high = Decimal(str(self.settings.intraday_resolved_high_fair_yes))
        if fair_yes is None or low < fair_yes < high:
            return None
        live_enabled = self.live_enabled(INTRADAY_SEPARATION_OVERRIDE)
        return PolicyVariantDecision(
            key=INTRADAY_SEPARATION_OVERRIDE,
            matched=True,
            live_enabled=live_enabled,
            shadow_enabled=self.shadow_enabled,
            applied=False,
            would_have_entered=self.shadow_enabled or live_enabled,
            reason="intraday-adjusted fair value is near-resolved despite a close raw forecast delta",
            thresholds={
                "low_fair_yes": _decimal_text(low),
                "high_fair_yes": _decimal_text(high),
                "min_quality_adjusted_edge_bps": self.settings.policy_variant_min_quality_adjusted_edge_bps,
            },
            diagnostics={
                "fair_yes_adjusted": _decimal_text(fair_yes),
                "fair_yes_initial": intraday.get("baseline_fair_yes") or provenance.get("baseline_fair_yes"),
                "forecast_delta_f": forecast_delta_f,
                "quality_adjusted_edge_bps": qa_edge,
            },
        )

    def empirical_bootstrap_decision(self, override: dict[str, Any] | None) -> PolicyVariantDecision | None:
        if override is None:
            return None
        live_enabled = self.live_enabled(EMPIRICAL_BOOTSTRAP)
        return PolicyVariantDecision(
            key=EMPIRICAL_BOOTSTRAP,
            matched=True,
            live_enabled=live_enabled,
            shadow_enabled=self.shadow_enabled,
            applied=False,
            would_have_entered=self.shadow_enabled or live_enabled,
            reason="empirical under-sampling repeated on a high-edge signal",
            thresholds={
                "min_evaluations": self.settings.empirical_bootstrap_min_evaluations,
                "min_edge_bps": self.settings.empirical_bootstrap_min_edge_bps,
                "last_edge_bps": self.settings.empirical_bootstrap_last_edge_bps,
            },
            diagnostics={"attention_pattern": override},
        )


def policy_variant_applied(candidate_trace: dict[str, Any] | None, key: str) -> bool:
    if not isinstance(candidate_trace, dict):
        return False
    if candidate_trace.get("policy_variant_applied") == key:
        return True
    variants = candidate_trace.get("policy_variants")
    return isinstance(variants, dict) and isinstance(variants.get(key), dict) and variants[key].get("applied") is True
