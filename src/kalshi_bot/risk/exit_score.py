from __future__ import annotations

from dataclasses import dataclass

from kalshi_bot.risk.uncertainty import clamp


@dataclass(frozen=True, slots=True)
class ExitRiskConfig:
    boundary_threshold: float = 0.25
    disagreement_threshold: float = 0.85
    closeout_hours_before_event: float = 2.0
    max_spread_cents: float = 20.0
    boundary_weight: float = 0.35
    time_weight: float = 0.35
    disagreement_weight: float = 0.15
    spread_weight: float = 0.15
    risk_tightening: float = 0.50


@dataclass(frozen=True, slots=True)
class ExitRiskResult:
    risk_score: float
    boundary_risk: float
    time_risk: float
    disagreement_risk: float
    spread_risk: float
    effective_hold_buffer: float

    def to_dict(self) -> dict[str, float]:
        return {
            "risk_score": self.risk_score,
            "boundary_risk": self.boundary_risk,
            "time_risk": self.time_risk,
            "disagreement_risk": self.disagreement_risk,
            "spread_risk": self.spread_risk,
            "effective_hold_buffer": self.effective_hold_buffer,
        }


@dataclass(frozen=True, slots=True)
class ExitInputs:
    hours_to_event: float
    mark_to_market_pnl_cents: float
    current_p_bucket_yes: float
    entry_p_bucket_yes: float
    held_side: str
    ev_now: float
    take_profit_cents: float
    stop_loss_cents: float
    prob_drift_threshold: float
    ev_gone_threshold: float
    hold_buffer: float
    boundary_mass: float
    disagreement: float
    spread_cents: float


@dataclass(frozen=True, slots=True)
class ExitDecision:
    action: str
    reason: str
    risk: ExitRiskResult

    def to_dict(self) -> dict[str, object]:
        return {"action": self.action, "reason": self.reason, "risk": self.risk.to_dict()}


def score_exit_risk(
    *,
    boundary_mass: float,
    hours_to_event: float,
    disagreement: float,
    spread_cents: float,
    hold_buffer: float,
    config: ExitRiskConfig | None = None,
) -> ExitRiskResult:
    cfg = config or ExitRiskConfig()
    if cfg.boundary_threshold <= 0 or cfg.disagreement_threshold <= 0 or cfg.closeout_hours_before_event <= 0 or cfg.max_spread_cents <= 0:
        raise ValueError("exit risk thresholds must be positive")
    total_weight = cfg.boundary_weight + cfg.time_weight + cfg.disagreement_weight + cfg.spread_weight
    if total_weight <= 0:
        raise ValueError("exit risk weights must sum positive")

    boundary_risk = clamp(boundary_mass / cfg.boundary_threshold)
    time_risk = clamp(1.0 - (hours_to_event / cfg.closeout_hours_before_event))
    disagreement_risk = clamp(disagreement / cfg.disagreement_threshold)
    spread_risk = clamp(spread_cents / cfg.max_spread_cents)
    risk_score = (
        (cfg.boundary_weight * boundary_risk)
        + (cfg.time_weight * time_risk)
        + (cfg.disagreement_weight * disagreement_risk)
        + (cfg.spread_weight * spread_risk)
    ) / total_weight
    effective_hold_buffer = hold_buffer * (1.0 - cfg.risk_tightening * risk_score)
    return ExitRiskResult(
        risk_score=risk_score,
        boundary_risk=boundary_risk,
        time_risk=time_risk,
        disagreement_risk=disagreement_risk,
        spread_risk=spread_risk,
        effective_hold_buffer=max(0.0, effective_hold_buffer),
    )


def choose_exit_action(inputs: ExitInputs, *, config: ExitRiskConfig | None = None) -> ExitDecision:
    cfg = config or ExitRiskConfig()
    risk = score_exit_risk(
        boundary_mass=inputs.boundary_mass,
        hours_to_event=inputs.hours_to_event,
        disagreement=inputs.disagreement,
        spread_cents=inputs.spread_cents,
        hold_buffer=inputs.hold_buffer,
        config=cfg,
    )
    if inputs.hours_to_event <= cfg.closeout_hours_before_event:
        return ExitDecision(action="close", reason="closeout_window", risk=risk)
    if inputs.mark_to_market_pnl_cents <= -abs(inputs.stop_loss_cents):
        return ExitDecision(action="close", reason="stop_loss", risk=risk)
    held_side = inputs.held_side.strip().lower()
    prob_drift = inputs.current_p_bucket_yes - inputs.entry_p_bucket_yes
    if held_side == "yes" and prob_drift < -abs(inputs.prob_drift_threshold):
        return ExitDecision(action="close", reason="probability_drift", risk=risk)
    if held_side == "no" and prob_drift > abs(inputs.prob_drift_threshold):
        return ExitDecision(action="close", reason="probability_drift", risk=risk)
    if inputs.ev_now < inputs.ev_gone_threshold:
        return ExitDecision(action="close", reason="ev_gone", risk=risk)
    if inputs.mark_to_market_pnl_cents >= inputs.take_profit_cents - risk.effective_hold_buffer:
        return ExitDecision(action="close", reason="take_profit", risk=risk)
    return ExitDecision(action="hold", reason="no_exit_rule", risk=risk)
