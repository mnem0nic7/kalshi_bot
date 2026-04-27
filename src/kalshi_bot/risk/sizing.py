from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from kalshi_bot.risk.uncertainty import clamp

Side = Literal["yes", "no"]


@dataclass(frozen=True, slots=True)
class SizingConfig:
    kelly_fraction: float = 0.25
    ai_mult: float = 1.0
    spread_penalty: float = 1.0
    uncertainty_mult: float = 1.0
    health_size_mult: float = 1.0
    max_position_pct: float = 0.05
    max_position_usd: float = 100.0
    max_total_exposure_pct: float = 0.25


@dataclass(frozen=True, slots=True)
class SizingBreakdown:
    side: str
    p_win: float
    cost: float
    fee: float
    win_profit: float
    loss: float
    b: float
    full_kelly: float
    requested_size_dollars: float
    capped_size_dollars: float
    cap_bound: str | None
    reject_reason: str | None

    @property
    def accepted(self) -> bool:
        return self.reject_reason is None and self.capped_size_dollars > 0

    def to_dict(self) -> dict[str, float | str | bool | None]:
        return {
            "side": self.side,
            "p_win": self.p_win,
            "cost": self.cost,
            "fee": self.fee,
            "win_profit": self.win_profit,
            "loss": self.loss,
            "b": self.b,
            "full_kelly": self.full_kelly,
            "requested_size_dollars": self.requested_size_dollars,
            "capped_size_dollars": self.capped_size_dollars,
            "cap_bound": self.cap_bound,
            "reject_reason": self.reject_reason,
            "accepted": self.accepted,
        }


def side_probability(*, p_yes: float, side: str) -> float:
    p = clamp(p_yes)
    normalized = side.strip().lower()
    if normalized == "yes":
        return p
    if normalized == "no":
        return 1.0 - p
    raise ValueError("side must be yes or no")


def side_cost_from_yes_price(*, yes_price: float, side: str) -> float:
    price = clamp(yes_price)
    normalized = side.strip().lower()
    if normalized == "yes":
        return price
    if normalized == "no":
        return 1.0 - price
    raise ValueError("side must be yes or no")


def fee_aware_binary_kelly_fraction(
    *,
    p_win: float,
    cost: float,
    fee: float,
) -> float:
    p = clamp(p_win)
    c = clamp(cost)
    f = max(0.0, float(fee))
    win_profit = (1.0 - c) - f
    loss = c + f
    if c <= 0 or c >= 1 or win_profit <= 0 or loss <= 0:
        return 0.0
    b = win_profit / loss
    if b <= 0:
        return 0.0
    kelly = (p * (b + 1.0) - 1.0) / b
    return max(0.0, kelly)


def size_fee_aware_binary_trade(
    *,
    balance: float,
    p_yes: float,
    yes_price: float,
    side: Side,
    fee_per_contract: float,
    current_total_exposure_dollars: float = 0.0,
    config: SizingConfig | None = None,
) -> SizingBreakdown:
    cfg = config or SizingConfig()
    normalized_side = side.strip().lower()
    p_win = side_probability(p_yes=p_yes, side=normalized_side)
    cost = side_cost_from_yes_price(yes_price=yes_price, side=normalized_side)
    fee = max(0.0, float(fee_per_contract))
    win_profit = (1.0 - cost) - fee
    loss = cost + fee
    b = win_profit / loss if loss > 0 else 0.0
    full_kelly = fee_aware_binary_kelly_fraction(p_win=p_win, cost=cost, fee=fee)
    if balance <= 0:
        return _rejected(normalized_side, p_win, cost, fee, win_profit, loss, b, full_kelly, "non_positive_balance")
    if full_kelly <= 0:
        return _rejected(normalized_side, p_win, cost, fee, win_profit, loss, b, full_kelly, "kelly_non_positive")

    requested = (
        balance
        * full_kelly
        * max(0.0, cfg.kelly_fraction)
        * max(0.0, cfg.ai_mult)
        * clamp(cfg.spread_penalty)
        * clamp(cfg.uncertainty_mult)
        * clamp(cfg.health_size_mult)
    )
    caps = {
        "max_position_usd": max(0.0, cfg.max_position_usd),
        "max_position_pct": max(0.0, cfg.max_position_pct) * balance,
        "remaining_exposure_capacity": max(
            0.0,
            (max(0.0, cfg.max_total_exposure_pct) * balance) - max(0.0, current_total_exposure_dollars),
        ),
    }
    cap_bound, cap_value = min(caps.items(), key=lambda item: item[1])
    capped = min(requested, cap_value)
    return SizingBreakdown(
        side=normalized_side,
        p_win=p_win,
        cost=cost,
        fee=fee,
        win_profit=win_profit,
        loss=loss,
        b=b,
        full_kelly=full_kelly,
        requested_size_dollars=requested,
        capped_size_dollars=capped,
        cap_bound=cap_bound if capped < requested else None,
        reject_reason=None if capped > 0 else "cap_zero",
    )


def _rejected(
    side: str,
    p_win: float,
    cost: float,
    fee: float,
    win_profit: float,
    loss: float,
    b: float,
    full_kelly: float,
    reason: str,
) -> SizingBreakdown:
    return SizingBreakdown(
        side=side,
        p_win=p_win,
        cost=cost,
        fee=fee,
        win_profit=win_profit,
        loss=loss,
        b=b,
        full_kelly=full_kelly,
        requested_size_dollars=0.0,
        capped_size_dollars=0.0,
        cap_bound=None,
        reject_reason=reason,
    )
