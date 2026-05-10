from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Iterable

from kalshi_bot.core.schemas import AgentPack, AgentPackThresholds, AgentPackWeatherPolicy, AgentPackWeatherPolicyBook
from kalshi_bot.services.agent_packs import RuntimeThresholds


WEATHER_POLICY_LANES = {
    "entry_gate",
    "exit",
    "addon",
    "capital",
    "model",
    "execution",
    "universe",
    "watchlist",
    "grid",
    "data_quality",
    "strategy_discovery",
}
WEATHER_POLICY_MODES = {"live", "shadow_only", "paused", "research_only", "retired"}
WEATHER_POLICY_ACTIONS = {
    "keep_current",
    "tighten",
    "loosen",
    "pause",
    "shadow_only",
    "research_only",
    "rollback",
    "expand_grid",
    "reduce_grid",
}
WEATHER_CONFLICT_ORDER = (
    "hard_fuse",
    "manual_off",
    "data_quality",
    "capital",
    "model",
    "entry_gate",
    "execution",
    "watchlist",
)


@dataclass(frozen=True, slots=True)
class WeatherPolicyContext:
    strategy_code: str = "weather_baseline"
    cohort_id: str | None = None
    series_ticker: str | None = None
    side: str | None = None
    season: str | None = None
    month: int | str | None = None
    regime: str | None = None
    threshold_band: str | None = None
    lane: str = "entry_gate"


@dataclass(frozen=True, slots=True)
class ResolvedWeatherPolicy:
    policy_key: str
    fallback_policy_key_used: str
    mode: str
    action: str
    lane: str
    thresholds: RuntimeThresholds
    policy: AgentPackWeatherPolicy | None
    candidate_keys: tuple[str, ...]
    binding_policy_lane: str
    policy_disagreement: bool
    reason_codes: tuple[str, ...]
    deterministic_summary: str

    def provenance(self) -> dict[str, Any]:
        return {
            "policy_key": self.policy_key,
            "fallback_policy_key_used": self.fallback_policy_key_used,
            "lane": self.lane,
            "mode": self.mode,
            "action": self.action,
            "gate_thresholds_used": _runtime_threshold_payload(self.thresholds),
            "binding_policy_lane": self.binding_policy_lane,
            "policy_disagreement": self.policy_disagreement,
            "reason_codes": list(self.reason_codes),
            "deterministic_summary": self.deterministic_summary,
        }


class WeatherPolicyResolver:
    def resolve(
        self,
        *,
        pack: AgentPack,
        context: WeatherPolicyContext,
        fallback_thresholds: RuntimeThresholds,
    ) -> ResolvedWeatherPolicy:
        book = pack.weather_policy if pack.weather_policy is not None else AgentPackWeatherPolicyBook()
        candidates = tuple(policy_key_candidates(context, cohort_memberships=book.cohort_memberships))
        policies = book.policies or {}
        selected: AgentPackWeatherPolicy | None = None
        selected_key: str | None = None
        for key in candidates:
            policy = policies.get(key)
            if policy is None or _normalize_mode(policy.mode) == "retired":
                continue
            selected = sanitize_weather_policy(policy)
            selected_key = key
            break

        if selected is None:
            requested_key = candidates[0] if candidates else build_weather_policy_key(context)
            fallback_key = candidates[-1] if candidates else requested_key
            return ResolvedWeatherPolicy(
                policy_key=requested_key,
                fallback_policy_key_used=fallback_key,
                mode="live",
                action="keep_current",
                lane=_normalize_lane(context.lane),
                thresholds=fallback_thresholds,
                policy=None,
                candidate_keys=candidates,
                binding_policy_lane=_normalize_lane(context.lane),
                policy_disagreement=False,
                reason_codes=("fallback_flat_agent_pack_thresholds",),
                deterministic_summary=(
                    f"No scoped weather policy matched {requested_key}; using flat agent-pack thresholds via {fallback_key}."
                ),
            )

        thresholds = overlay_thresholds(fallback_thresholds, selected.thresholds)
        mode = _normalize_mode(selected.mode)
        action = _normalize_action(selected.action)
        lane = _normalize_lane(selected.lane)
        reason_codes = tuple(selected.reason_codes or ("scoped_weather_policy_matched",))
        summary = selected.deterministic_summary or (
            f"Matched weather policy {selected.policy_key} in {mode} mode with action {action}."
        )
        return ResolvedWeatherPolicy(
            policy_key=selected.policy_key,
            fallback_policy_key_used=selected_key or selected.policy_key,
            mode=mode,
            action=action,
            lane=lane,
            thresholds=thresholds,
            policy=selected,
            candidate_keys=candidates,
            binding_policy_lane=lane,
            policy_disagreement=False,
            reason_codes=reason_codes,
            deterministic_summary=summary,
        )


def build_weather_policy_key(context: WeatherPolicyContext) -> str:
    parts = (
        "weather",
        _normalize_part(context.strategy_code, default="weather_baseline"),
        _normalize_part(context.cohort_id, default="global"),
        _normalize_part(context.series_ticker),
        _normalize_part(context.side),
        _normalize_part(context.season or _season_from_month(context.month), default="all_season"),
        _normalize_month(context.month),
        _normalize_part(context.regime),
        _normalize_part(context.threshold_band),
        _normalize_lane(context.lane),
    )
    return "/".join(parts)


def policy_key_candidates(
    context: WeatherPolicyContext,
    *,
    cohort_memberships: dict[str, list[str]] | None = None,
) -> Iterable[str]:
    strategy = _normalize_part(context.strategy_code, default="weather_baseline")
    series = _normalize_part(context.series_ticker)
    side = _normalize_part(context.side)
    month = _normalize_month(context.month)
    season = _normalize_part(context.season or _season_from_month(context.month), default="all_season")
    regime = _normalize_part(context.regime)
    threshold_band = _normalize_part(context.threshold_band)
    lane = _normalize_lane(context.lane)
    cohorts = _cohort_candidates(series, context.cohort_id, cohort_memberships)
    keys: list[str] = []
    for cohort in cohorts:
        keys.extend(
            [
                _key(strategy, cohort, series, side, season, month, regime, threshold_band, lane),
                _key(strategy, cohort, series, side, season, "all_month", regime, threshold_band, lane),
                _key(strategy, cohort, series, side, season, "all_month", "any", threshold_band, lane),
                _key(strategy, cohort, series, side, season, "all_month", "any", "any", lane),
                _key(strategy, cohort, series, side, "all_season", "all_month", "any", "any", lane),
                _key(strategy, cohort, series, "any", "all_season", "all_month", "any", "any", lane),
            ]
        )
    keys.extend(
        [
            _key(strategy, "global", "any", side, season, month, regime, threshold_band, lane),
            _key(strategy, "global", "any", side, season, "all_month", "any", "any", lane),
            _key(strategy, "global", "any", "any", "all_season", "all_month", "any", "any", lane),
        ]
    )
    return tuple(dict.fromkeys(keys))


def sanitize_weather_policy(policy: AgentPackWeatherPolicy) -> AgentPackWeatherPolicy:
    lane = _normalize_lane(policy.lane)
    mode = _normalize_mode(policy.mode)
    action = _normalize_action(policy.action)
    key = policy.policy_key or build_weather_policy_key(
        WeatherPolicyContext(
            strategy_code=policy.strategy_code,
            cohort_id=policy.cohort_id,
            series_ticker=policy.series_ticker,
            side=policy.side,
            season=policy.season,
            month=policy.month,
            regime=policy.regime,
            threshold_band=policy.threshold_band,
            lane=lane,
        )
    )
    return policy.model_copy(update={"policy_key": key, "lane": lane, "mode": mode, "action": action})


def overlay_thresholds(base: RuntimeThresholds, overrides: AgentPackThresholds) -> RuntimeThresholds:
    payload = {field: getattr(base, field) for field in base.__dataclass_fields__}
    for field in AgentPackThresholds.model_fields:
        value = getattr(overrides, field, None)
        if value is not None and field in payload:
            payload[field] = value
    return RuntimeThresholds(**payload)


def weather_policy_context_from_market(
    *,
    market_ticker: str | None,
    strategy_code: str = "weather_baseline",
    side: str | None = None,
    local_market_day: str | None = None,
    trade_regime: str | None = None,
    threshold_band: str | None = None,
    lane: str = "entry_gate",
) -> WeatherPolicyContext:
    series = _series_from_ticker(market_ticker)
    month = _month_from_market_day(local_market_day or _market_day_from_ticker(market_ticker))
    return WeatherPolicyContext(
        strategy_code=strategy_code,
        series_ticker=series,
        side=side,
        season=_season_from_month(month),
        month=month,
        regime=trade_regime,
        threshold_band=threshold_band,
        lane=lane,
    )


def _key(
    strategy: str,
    cohort: str,
    series: str,
    side: str,
    season: str,
    month: str,
    regime: str,
    threshold_band: str,
    lane: str,
) -> str:
    return "/".join(("weather", strategy, cohort, series, side, season, month, regime, threshold_band, lane))


def _normalize_part(value: Any, *, default: str = "any") -> str:
    raw = str(value or default).strip()
    text = raw.lower()
    if text in {"", "*", "all", "none", "null"}:
        return default
    return raw.upper() if raw.upper().startswith("KX") else text


def _normalize_lane(value: Any) -> str:
    lane = str(value or "entry_gate").strip().lower()
    return lane if lane in WEATHER_POLICY_LANES else "entry_gate"


def _normalize_mode(value: Any) -> str:
    mode = str(value or "live").strip().lower()
    return mode if mode in WEATHER_POLICY_MODES else "shadow_only"


def _normalize_action(value: Any) -> str:
    action = str(value or "keep_current").strip().lower()
    return action if action in WEATHER_POLICY_ACTIONS else "keep_current"


def _normalize_month(value: Any) -> str:
    if value in (None, "", "any", "*", "all"):
        return "all_month"
    try:
        month = int(value)
    except (TypeError, ValueError):
        text = str(value).strip().lower()
        return text if text.startswith("m") or text == "all_month" else f"m{text}"
    return f"m{max(1, min(12, month)):02d}"


def _season_from_month(value: Any) -> str:
    try:
        month = int(str(value).removeprefix("m"))
    except (TypeError, ValueError):
        return "all_season"
    if month in {4, 5, 6, 7, 8, 9}:
        return "warm_season"
    if month in {10, 11, 12, 1, 2, 3}:
        return "cold_season"
    return "all_season"


def _cohort_candidates(
    series: str,
    cohort_id: str | None,
    cohort_memberships: dict[str, list[str]] | None,
) -> list[str]:
    direct = _normalize_part(cohort_id, default="global")
    cohorts: list[str] = []
    if direct != "global":
        cohorts.append(direct)
    for cohort, members in (cohort_memberships or {}).items():
        normalized_members = {_normalize_part(member) for member in members}
        if series in normalized_members:
            cohorts.append(_normalize_part(cohort, default="global"))
    cohorts.append("global")
    return list(dict.fromkeys(cohorts))


def _series_from_ticker(market_ticker: str | None) -> str | None:
    if not market_ticker:
        return None
    return str(market_ticker).split("-")[0]


def _market_day_from_ticker(market_ticker: str | None) -> str | None:
    if not market_ticker:
        return None
    parts = str(market_ticker).split("-")
    return parts[1] if len(parts) > 1 else None


def _month_from_market_day(value: str | None) -> int | None:
    if not value:
        return None
    text = str(value)
    try:
        if len(text) == 10 and text[4] == "-":
            return date.fromisoformat(text).month
        parsed = datetime.strptime(text[:5].upper(), "%y%b").replace(tzinfo=UTC)
        return parsed.month
    except (TypeError, ValueError):
        return None


def _runtime_threshold_payload(thresholds: RuntimeThresholds) -> dict[str, Any]:
    return {field: getattr(thresholds, field) for field in thresholds.__dataclass_fields__}
