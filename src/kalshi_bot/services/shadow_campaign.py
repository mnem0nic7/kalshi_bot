from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.core.schemas import ResearchDossier, ShadowCampaignRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.discovery import DiscoveryService, MarketDiscovery
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.shadow import ShadowRunResult, ShadowTrainingService


@dataclass(slots=True)
class ShadowCampaignCandidate:
    market_ticker: str
    city_bucket: str
    market_regime_bucket: str
    difficulty_bucket: str
    outcome_bucket: str
    settlement_urgency_bucket: str
    recent_count: int
    payload: dict[str, Any]


class ShadowCampaignService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        discovery_service: DiscoveryService,
        research_coordinator: ResearchCoordinator,
        shadow_training_service: ShadowTrainingService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.discovery_service = discovery_service
        self.research_coordinator = research_coordinator
        self.shadow_training_service = shadow_training_service

    async def run(self, request: ShadowCampaignRequest) -> list[ShadowRunResult]:
        discoveries = await self.discovery_service.discover_configured_markets()
        now = datetime.now(UTC)
        lookback_cutoff = now - timedelta(hours=self.settings.training_campaign_lookback_hours)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            campaigns = await repo.list_room_campaigns(limit=1000)
            failed_runs = await repo.list_research_runs(status="failed", limit=500)
            recent_dossiers = {
                record.market_ticker: record.payload
                for record in await repo.list_research_dossiers(limit=500)
            }
            active_rooms = {
                discovery.mapping.market_ticker: await repo.get_latest_active_room_for_market(discovery.mapping.market_ticker)
                for discovery in discoveries
            }
            await session.commit()

        recent_campaigns = [
            record
            for record in campaigns
            if self._normalize_dt(record.created_at) >= lookback_cutoff
        ]
        market_recent_count = Counter(record.payload.get("market_ticker") or "" for record in recent_campaigns)
        market_last_seen = {
            str(record.payload.get("market_ticker") or ""): self._normalize_dt(record.created_at)
            for record in recent_campaigns
            if record.payload.get("market_ticker")
        }
        failure_count = Counter(run.market_ticker for run in failed_runs)

        candidates: list[ShadowCampaignCandidate] = []
        for discovery in discoveries:
            mapping = discovery.mapping
            if mapping.market_type != "weather" or not mapping.supports_structured_weather:
                continue
            if discovery.status not in {"active", "open"}:
                continue
            if active_rooms.get(mapping.market_ticker) is not None:
                continue
            if failure_count[mapping.market_ticker] >= 3 and mapping.market_ticker not in recent_dossiers:
                continue
            dossier_payload = recent_dossiers.get(mapping.market_ticker)
            signal = None
            if dossier_payload is not None:
                try:
                    dossier_model = ResearchDossier.model_validate(dossier_payload)
                    quality = self.research_coordinator.training_quality_snapshot(dossier_model)
                    if not bool(quality.get("valid_dossier")):
                        continue
                    signal = self.research_coordinator.build_signal_from_dossier(dossier_model, discovery.raw)
                    if signal.resolution_state != WeatherResolutionState.UNRESOLVED:
                        continue
                except Exception:
                    signal = None
            last_seen = market_last_seen.get(mapping.market_ticker)
            if last_seen is not None and (now - last_seen).total_seconds() < self.settings.training_campaign_cooldown_seconds:
                continue
            if market_recent_count[mapping.market_ticker] >= self.settings.training_campaign_max_recent_per_market:
                continue
            candidates.append(
                self._candidate_from_discovery(
                    discovery,
                    dossier=dossier_payload,
                    recent_count=market_recent_count[mapping.market_ticker],
                    now=now,
                    signal=signal,
                )
            )

        selected = self._select_candidates(candidates, limit=request.limit)
        if not selected:
            return []

        campaign_id = f"weather-corpus-{now.strftime('%Y%m%d%H%M%S')}"
        results: list[ShadowRunResult] = []
        for candidate in selected:
            campaign_payload = {
                **candidate.payload,
                "campaign_id": campaign_id,
                "trigger_source": request.reason,
                "market_ticker": candidate.market_ticker,
                "city_bucket": candidate.city_bucket,
                "market_regime_bucket": candidate.market_regime_bucket,
                "difficulty_bucket": candidate.difficulty_bucket,
                "outcome_bucket": candidate.outcome_bucket,
                "settlement_urgency_bucket": candidate.settlement_urgency_bucket,
            }
            results.append(
                await self.shadow_training_service.run_shadow_room(
                    candidate.market_ticker,
                    reason=request.reason,
                    campaign=campaign_payload,
                )
            )
        return results

    def _select_candidates(
        self,
        candidates: list[ShadowCampaignCandidate],
        *,
        limit: int,
    ) -> list[ShadowCampaignCandidate]:
        selected: list[ShadowCampaignCandidate] = []
        selected_city = Counter()
        selected_regime = Counter()
        selected_outcome = Counter()
        remaining = list(candidates)
        while remaining and len(selected) < limit:
            remaining.sort(
                key=lambda item: (
                    item.recent_count,
                    self._urgency_rank(item.settlement_urgency_bucket),
                    selected_city[item.city_bucket],
                    selected_regime[item.market_regime_bucket],
                    selected_outcome[item.outcome_bucket],
                    self._difficulty_rank(item.difficulty_bucket),
                    item.market_ticker,
                )
            )
            choice = remaining.pop(0)
            selected.append(choice)
            selected_city[choice.city_bucket] += 1
            selected_regime[choice.market_regime_bucket] += 1
            selected_outcome[choice.outcome_bucket] += 1
        return selected

    def _candidate_from_discovery(
        self,
        discovery: MarketDiscovery,
        *,
        dossier: dict[str, Any] | None,
        recent_count: int,
        now: datetime,
        signal: Any = None,
    ) -> ShadowCampaignCandidate:
        mapping = discovery.mapping
        fair_yes = self._decimal_or_none(((dossier or {}).get("trader_context") or {}).get("fair_yes_dollars"))
        if signal is None and dossier is not None:
            try:
                signal = self.research_coordinator.build_signal_from_dossier(
                    ResearchDossier.model_validate(dossier),
                    discovery.raw,
                )
            except Exception:
                signal = None
        city_bucket = mapping.location_name or mapping.station_id or mapping.market_ticker
        market_regime_bucket = self._regime_bucket(discovery)
        difficulty_bucket = self._difficulty_bucket(mapping.threshold_f, dossier)
        outcome_bucket = self._outcome_bucket(discovery, fair_yes=fair_yes, dossier=dossier, signal=signal)
        settlement_urgency_bucket = self._settlement_urgency_bucket(discovery.close_ts, now=now)
        payload = {
            "market_label": mapping.label,
            "series_ticker": mapping.series_ticker,
            "station_id": mapping.station_id,
            "threshold_f": mapping.threshold_f,
            "operator": mapping.operator,
            "discovery_status": discovery.status,
            "can_trade": discovery.can_trade,
            "notes": discovery.notes,
            "recent_count": recent_count,
        }
        return ShadowCampaignCandidate(
            market_ticker=mapping.market_ticker,
            city_bucket=city_bucket,
            market_regime_bucket=market_regime_bucket,
            difficulty_bucket=difficulty_bucket,
            outcome_bucket=outcome_bucket,
            settlement_urgency_bucket=settlement_urgency_bucket,
            recent_count=recent_count,
            payload=payload,
        )

    @staticmethod
    def _regime_bucket(discovery: MarketDiscovery) -> str:
        if discovery.yes_ask_dollars is None or discovery.yes_bid_dollars is None:
            return "illiquid"
        spread = discovery.yes_ask_dollars - discovery.yes_bid_dollars
        if spread <= Decimal("0.0200"):
            return "tight"
        if spread <= Decimal("0.0500"):
            return "medium"
        return "wide"

    @staticmethod
    def _difficulty_bucket(threshold_f: float | None, dossier: dict[str, Any] | None) -> str:
        if threshold_f is None or dossier is None:
            return "unknown"
        facts = (dossier.get("summary") or {}).get("current_numeric_facts") or {}
        forecast_high = facts.get("forecast_high_f")
        if forecast_high not in (None, ""):
            delta = abs(float(forecast_high) - float(threshold_f))
            if delta < 3:
                return "near_threshold"
            if delta < 7:
                return "moderate"
            return "far_from_threshold"
        fair_yes = ((dossier.get("trader_context") or {}).get("fair_yes_dollars"))
        if fair_yes not in (None, ""):
            distance = abs(float(fair_yes) - 0.5)
            if distance < 0.08:
                return "near_threshold"
            if distance < 0.20:
                return "moderate"
            return "far_from_threshold"
        return "unknown"

    @staticmethod
    def _outcome_bucket(
        discovery: MarketDiscovery,
        *,
        fair_yes: Decimal | None,
        dossier: dict[str, Any] | None,
        signal: Any,
    ) -> str:
        gate = ((dossier or {}).get("gate") or {})
        if not gate.get("passed", False):
            return "no_trade"
        if signal is not None and getattr(signal, "recommended_side", None) is not None:
            return f"trade_{signal.recommended_side.value}"
        if fair_yes is not None and discovery.yes_ask_dollars is not None and fair_yes > discovery.yes_ask_dollars:
            return "trade_yes"
        if fair_yes is not None and discovery.no_ask_dollars is not None and (Decimal("1.0000") - fair_yes) > discovery.no_ask_dollars:
            return "trade_no"
        return "no_trade"

    @staticmethod
    def _difficulty_rank(bucket: str) -> int:
        return {
            "near_threshold": 0,
            "moderate": 1,
            "far_from_threshold": 2,
            "unknown": 3,
        }.get(bucket, 4)

    @staticmethod
    def _settlement_urgency_bucket(close_ts: int | None, *, now: datetime) -> str:
        if close_ts is None:
            return "later"
        close_at = datetime.fromtimestamp(close_ts, tz=UTC)
        delta_seconds = (close_at - now).total_seconds()
        if delta_seconds <= 6 * 60 * 60:
            return "closing_soon"
        if delta_seconds <= 24 * 60 * 60:
            return "closing_today"
        return "later"

    @staticmethod
    def _urgency_rank(bucket: str) -> int:
        return {
            "closing_soon": 0,
            "closing_today": 1,
            "later": 2,
        }.get(bucket, 3)

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value)).quantize(Decimal("0.0001"))

    @staticmethod
    def _normalize_dt(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
