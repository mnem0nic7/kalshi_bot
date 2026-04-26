from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_zero(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return None


def _quote_or_none(value: Any) -> Decimal | None:
    quote = _decimal_or_none(value)
    if quote is None:
        return None
    quote = quote.quantize(Decimal("0.0001"))
    return quote if quote > 0 else None


def _cents_to_dollars(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    amount = _decimal_or_none(value)
    if amount is None:
        return None
    raw = str(value).strip().lower()
    if "." not in raw and "e" not in raw:
        amount = amount / Decimal("100")
    return amount.quantize(Decimal("0.01"))


def _money_display(value: Decimal | None, *, signed: bool = False) -> str:
    if value is None:
        return "—"
    amount = value.quantize(Decimal("0.01"))
    if signed:
        if amount > 0:
            return f"+${amount}"
        if amount < 0:
            return f"-${abs(amount)}"
    return f"${amount}"


def _percent_display(value: Decimal | None, *, signed: bool = False) -> str | None:
    if value is None:
        return None
    amount = value.quantize(Decimal("0.01"))
    if signed and amount > 0:
        return f"+{amount}%"
    return f"{amount}%"


def _ratio_range_display(lower: float | None, upper: float | None) -> str:
    if lower is None or upper is None:
        return "—"
    return f"{lower:.0%}-{upper:.0%}"


def _price_display(value: Decimal | None) -> str:
    if value is None:
        return "—"
    return f"${value.quantize(Decimal('0.0001'))}"


def _pnl_tone(value: Decimal | None) -> str:
    if value is None:
        return "neutral"
    if value > 0:
        return "good"
    if value < 0:
        return "bad"
    return "neutral"


def _win_rate_display(win_rate_data: dict) -> str:
    total = win_rate_data.get("total_contracts", 0)
    if not total:
        return "—"
    won = win_rate_data.get("won_contracts", 0)
    pct = int(round(100 * won / total))
    return f"{pct}%"


def _win_loss_magnitude_display(win_rate_data: dict) -> dict[str, str]:
    """Format avg win / avg loss / stdev / Sharpe proxy for the win-rate card."""

    def _money(value: float | None, *, signed: bool) -> str:
        if value is None:
            return "—"
        return _money_display(Decimal(str(value)), signed=signed)

    sharpe = win_rate_data.get("sharpe_per_trade")
    sharpe_display = "—" if sharpe is None else f"{sharpe:+.2f}"
    return {
        "avg_win_display": _money(win_rate_data.get("avg_win_dollars"), signed=True),
        "avg_loss_display": _money(win_rate_data.get("avg_loss_dollars"), signed=True),
        "stdev_display": _money(win_rate_data.get("stdev_dollars"), signed=False),
        "sharpe_display": sharpe_display,
    }


def _broken_book_rate_display(data: dict) -> str:
    total = data.get("total_count", 0)
    if not total:
        return "—"
    broken = data.get("broken_count", 0)
    pct = int(round(100 * broken / total))
    return f"{pct}%"


def _percent_change(change: Decimal | None, baseline: Decimal | None) -> Decimal | None:
    if change is None or baseline is None or baseline <= 0:
        return None
    return ((change / baseline) * Decimal("100")).quantize(Decimal("0.01"))


def _daily_pnl_line_display(
    daily_pnl: Decimal | None, daily_pnl_percent: Decimal | None
) -> str:
    money_text = _money_display(daily_pnl, signed=True)
    if daily_pnl is None:
        return money_text
    percent_text = _percent_display(daily_pnl_percent)
    if percent_text is None:
        return f"{money_text} today (PT)"
    return f"{money_text} ({percent_text}) today (PT)"


def _capital_bucket_from_signal_payload(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return "risky"
    explicit = str(payload.get("capital_bucket") or "").strip().lower()
    if explicit in {"safe", "risky"}:
        return explicit
    trade_regime = str(payload.get("trade_regime") or "").strip().lower()
    if trade_regime in {"near_threshold", "longshot_yes", "longshot_no"}:
        return "risky"
    if trade_regime == "standard":
        return "safe"
    return "risky"
