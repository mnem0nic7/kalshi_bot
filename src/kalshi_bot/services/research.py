from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import async_sessionmaker
from pydantic import BaseModel

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, ContractSide, StandDownReason, StrategyMode, TradeAction, WeatherResolutionState
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.core.schemas import (
    ResearchClaim,
    ResearchDelta,
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchQualitySummary,
    ResearchSourceCard,
    ResearchSummary,
    ResearchTraderContext,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.historical_archive import append_weather_bundle_archive, weather_bundle_archive_metadata
from kalshi_bot.services.signal import (
    StrategySignal,
    WeatherSignalEngine,
    annotate_signal_quality,
    base_strategy_summary,
    capital_bucket_for_trade_regime,
    summarize_signal_action,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping
from kalshi_bot.weather.scoring import extract_current_temp_f, extract_forecast_high_f

try:
    from duckduckgo_search import DDGS
except Exception:  # pragma: no cover - optional dependency protection
    DDGS = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


PRIMARY_DOMAINS = {
    "kalshi.com",
    "api.weather.gov",
    "weather.gov",
    "noaa.gov",
    "nws.noaa.gov",
}

REPUTABLE_DOMAINS = {
    "apnews.com",
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "nytimes.com",
    "cnbc.com",
    "espn.com",
    "wikipedia.org",
}


def _publisher_from_url(url: str | None) -> str:
    if not url:
        return "Unknown"
    host = urlparse(url).netloc.lower()
    return host.removeprefix("www.") or "Unknown"


def _domain_trust_tier(url: str | None) -> str:
    if not url:
        return "weak"
    host = urlparse(url).netloc.lower().removeprefix("www.")
    if host.endswith(".gov") or host in PRIMARY_DOMAINS:
        return "primary"
    if host in REPUTABLE_DOMAINS or host.endswith(".edu") or host.endswith(".org"):
        return "reputable"
    return "weak"


def _source_key(source_class: str, title: str, url: str | None = None) -> str:
    seed = f"{source_class}|{url or title}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _coerce_market(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("market", payload)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _exception_payload(exc: Exception, *, market_ticker: str, trigger_reason: str) -> dict[str, Any]:
    error_type = str(getattr(exc, "error_type", "") or type(exc).__name__)
    endpoint = getattr(exc, "endpoint", None)
    status_code = getattr(exc, "status_code", None)
    message = str(exc).strip()
    if not message:
        message = f"{error_type} while refreshing {market_ticker}"
        if endpoint:
            message = f"{message} via {endpoint}"
    payload: dict[str, Any] = {
        "market_ticker": market_ticker,
        "trigger_reason": trigger_reason,
        "error": message,
        "error_type": error_type,
    }
    if endpoint:
        payload["endpoint"] = str(endpoint)
    if status_code is not None:
        payload["status_code"] = status_code
    return payload


def _checkpoint_age_seconds(checkpoint_payload: dict[str, Any], key: str, *, now: datetime) -> float | None:
    raw_value = checkpoint_payload.get(key)
    if not raw_value:
        return None
    try:
        recorded_at = datetime.fromisoformat(str(raw_value))
    except ValueError:
        return None
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=UTC)
    return (now - recorded_at.astimezone(UTC)).total_seconds()


class WebSynthesisPayload(BaseModel):
    narrative: str
    bullish_case: str
    bearish_case: str
    unresolved_uncertainties: list[str]
    fair_yes_dollars: Decimal | None = None
    confidence: float
    thesis: str


class ResearchCoordinator:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
        weather: NWSWeatherClient,
        weather_directory: WeatherMarketDirectory,
        providers: ProviderRouter,
        signal_engine: WeatherSignalEngine,
        agent_pack_service: AgentPackService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.weather = weather
        self.weather_directory = weather_directory
        self.providers = providers
        self.signal_engine = signal_engine
        self.agent_pack_service = agent_pack_service
        self._inflight_markets: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    async def get_latest_dossier(self, market_ticker: str) -> ResearchDossier | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            record = await repo.get_research_dossier(market_ticker)
            await session.commit()
        if record is None:
            return None
        return self._hydrate_runtime_fields(ResearchDossier.model_validate(record.payload))

    async def list_recent_runs(self, market_ticker: str, limit: int = 10, *, status: str | None = None) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            runs = await repo.list_research_runs(market_ticker=market_ticker, status=status, limit=limit)
            await session.commit()
        return [
            {
                "id": run.id,
                "market_ticker": run.market_ticker,
                "trigger_reason": run.trigger_reason,
                "status": run.status,
                "error_text": run.error_text,
                "started_at": _to_iso(run.started_at),
                "finished_at": _to_iso(run.finished_at),
                "payload": run.payload,
            }
            for run in runs
        ]

    async def list_failed_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            runs = await repo.list_research_runs(status="failed", limit=limit)
            await session.commit()
        return [
            {
                "id": run.id,
                "market_ticker": run.market_ticker,
                "trigger_reason": run.trigger_reason,
                "error_text": run.error_text,
                "started_at": _to_iso(run.started_at),
            }
            for run in runs
        ]

    async def ensure_fresh_dossier(self, market_ticker: str, *, reason: str) -> ResearchDossier:
        dossier = await self.get_latest_dossier(market_ticker)
        if dossier is not None and not dossier.freshness.stale:
            return dossier
        return await self.refresh_market_dossier(market_ticker, trigger_reason=reason)

    async def refresh_market_dossier(self, market_ticker: str, *, trigger_reason: str, force: bool = True) -> ResearchDossier:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.create_research_run(market_ticker=market_ticker, trigger_reason=trigger_reason)
            await session.commit()

            try:
                pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
                thresholds = self.agent_pack_service.runtime_thresholds(pack)
                market_response = await self.kalshi.get_market(market_ticker)
                market = _coerce_market(market_response)
                mapping = self.weather_directory.resolve_market(market_ticker, market)
                await repo.log_exchange_event("research", "market_snapshot", market_response, market_ticker=market_ticker)

                weather_bundle: dict[str, Any] | None = None
                weather_signal = None
                if mapping is not None and mapping.supports_structured_weather:
                    weather_bundle = await self.weather.build_market_snapshot(mapping)
                    await repo.log_weather_event(mapping.station_id, "research_weather_bundle", weather_bundle)
                    archive_record = append_weather_bundle_archive(
                        self.settings,
                        weather_bundle,
                        source_id=f"research:{run.id}",
                        archive_source="research_refresh",
                    )
                    archive_meta = weather_bundle_archive_metadata(weather_bundle)
                    if archive_meta is not None:
                        await repo.upsert_historical_weather_snapshot(
                            station_id=archive_meta["station_id"],
                            series_ticker=archive_meta["series_ticker"],
                            local_market_day=archive_meta["local_market_day"],
                            asof_ts=archive_meta["asof_ts"],
                            source_kind="archived_weather_bundle",
                            source_id=f"research:{run.id}",
                            source_hash=hashlib.sha1(
                                json.dumps(weather_bundle, sort_keys=True, default=str).encode("utf-8")
                            ).hexdigest()[:24],
                            observation_ts=archive_meta["observation_ts"],
                            forecast_updated_ts=archive_meta["forecast_updated_ts"],
                            forecast_high_f=archive_meta["forecast_high_f"],
                            current_temp_f=archive_meta["current_temp_f"],
                            payload={
                                **weather_bundle,
                                "_archive": {
                                    "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                                    "archive_source": "research_refresh",
                                    "source_id": f"research:{run.id}",
                                },
                            },
                        )
                    weather_signal = self.signal_engine.evaluate(
                        mapping,
                        market_response,
                        weather_bundle,
                        min_edge_bps=thresholds.risk_min_edge_bps,
                    )

                sources = self._kalshi_sources(mapping, market_ticker, market)
                claims = self._kalshi_claims(mapping, market_ticker, market, sources[0].source_key)

                if mapping is not None and mapping.supports_structured_weather and weather_bundle is not None and weather_signal is not None:
                    weather_sources, weather_claims = self._structured_weather_research(mapping, weather_bundle, weather_signal)
                    sources.extend(weather_sources)
                    claims.extend(weather_claims)

                use_web = mapping is None or not mapping.supports_structured_weather
                web_payload: dict[str, Any] | None = None
                if use_web:
                    web_sources = await self._web_sources(mapping, market, pack=pack)
                    sources.extend(web_sources)
                    claims.extend(self._web_claims(web_sources))
                    web_payload = await self._web_synthesis_payload(
                        mapping=mapping,
                        market=market,
                        sources=web_sources,
                        pack=pack,
                    )

                dossier = self._build_dossier(
                    market_ticker=market_ticker,
                    market=market,
                    mapping=mapping,
                    sources=self._dedupe_sources(sources),
                    claims=claims,
                    weather_signal=weather_signal,
                    web_payload=web_payload,
                    last_run_id=run.id,
                )
                source_records = await repo.save_research_sources(
                    run_id=run.id,
                    market_ticker=market_ticker,
                    sources=dossier.sources,
                )
                await repo.save_research_claims(
                    run_id=run.id,
                    market_ticker=market_ticker,
                    claims=dossier.claims,
                    source_records=source_records,
                )
                await repo.upsert_research_dossier(dossier)
                await repo.complete_research_run(
                    run.id,
                    status="completed",
                    payload={
                        "status": dossier.status,
                        "source_count": len(dossier.sources),
                        "settlement_covered": dossier.settlement_covered,
                        "gate_passed": dossier.gate.passed,
                    },
                )
                await repo.set_checkpoint(
                    f"research_refresh:{market_ticker}",
                    cursor=None,
                    payload={
                        "refreshed_at": dossier.freshness.refreshed_at.isoformat(),
                        "source_count": len(dossier.sources),
                        "fair_yes_dollars": str(dossier.trader_context.fair_yes_dollars)
                        if dossier.trader_context.fair_yes_dollars is not None
                        else None,
                        "agent_pack_version": pack.version,
                    },
                )
                await session.commit()
                return dossier
            except Exception as exc:
                error_payload = _exception_payload(
                    exc,
                    market_ticker=market_ticker,
                    trigger_reason=trigger_reason,
                )
                await repo.complete_research_run(
                    run.id,
                    status="failed",
                    payload={"error": error_payload},
                    error_text=error_payload["error"],
                )
                await repo.log_ops_event(
                    severity="error",
                    summary=f"Research refresh failed for {market_ticker}",
                    source="research",
                    payload=error_payload,
                )
                await repo.set_checkpoint(
                    f"research_refresh_failed:{self.settings.kalshi_env}:{market_ticker}",
                    cursor=None,
                    payload={
                        "failed_at": datetime.now(UTC).isoformat(),
                        **error_payload,
                    },
                )
                await session.commit()
                raise

    async def handle_market_update(self, market_ticker: str) -> None:
        if not self.weather_directory.supports_market_ticker(market_ticker):
            return
        if market_ticker in self._inflight_markets:
            return
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            if control.active_color != self.settings.app_color:
                await session.commit()
                return
            checkpoint = await repo.get_checkpoint(f"research_refresh:{market_ticker}")
            if checkpoint is not None:
                last_refresh = checkpoint.payload.get("refreshed_at")
                if last_refresh is not None:
                    refreshed_at = datetime.fromisoformat(last_refresh)
                    if datetime.now(UTC) - refreshed_at < timedelta(seconds=self.settings.research_refresh_cooldown_seconds):
                        await session.commit()
                        return
            failed_checkpoint = await repo.get_checkpoint(
                f"research_refresh_failed:{self.settings.kalshi_env}:{market_ticker}"
            )
            if failed_checkpoint is not None:
                age_seconds = _checkpoint_age_seconds(failed_checkpoint.payload, "failed_at", now=datetime.now(UTC))
                if (
                    age_seconds is not None
                    and age_seconds < self.settings.research_refresh_failed_cooldown_seconds
                ):
                    await session.commit()
                    return
            market_state = await repo.get_market_state(market_ticker)
            dossier_record = await repo.get_research_dossier(market_ticker)
            await session.commit()

        if market_state is None:
            return
        if dossier_record is not None:
            dossier = self._hydrate_runtime_fields(ResearchDossier.model_validate(dossier_record.payload))
            previous_bid = _to_float(dossier.summary.current_numeric_facts.get("yes_bid_dollars"))
            previous_ask = _to_float(dossier.summary.current_numeric_facts.get("yes_ask_dollars"))
            current_bid = _to_float(market_state.yes_bid_dollars)
            current_ask = _to_float(market_state.yes_ask_dollars)
            if previous_bid == current_bid and previous_ask == current_ask and not dossier.freshness.stale:
                return

        self._inflight_markets.add(market_ticker)
        task = asyncio.create_task(self._refresh_task(market_ticker))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def wait_for_tasks(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _refresh_task(self, market_ticker: str) -> None:
        try:
            await self.refresh_market_dossier(market_ticker, trigger_reason="market_event")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.info("Background research refresh failed for %s; failure was recorded", market_ticker)
        finally:
            self._inflight_markets.discard(market_ticker)

    def build_room_delta(
        self,
        *,
        dossier: ResearchDossier,
        market_response: dict[str, Any],
        weather_bundle: dict[str, Any] | None = None,
    ) -> ResearchDelta:
        market = _coerce_market(market_response)
        latest_facts = self._market_numeric_facts(market)
        changed_fields: list[str] = []
        updates: dict[str, Any] = {}
        for key, value in latest_facts.items():
            if dossier.summary.current_numeric_facts.get(key) != value:
                changed_fields.append(key)
                updates[key] = value
        if weather_bundle is not None:
            forecast_high = extract_forecast_high_f(weather_bundle.get("forecast", {}))
            current_temp = extract_current_temp_f(weather_bundle.get("observation", {}))
            if forecast_high is not None and dossier.summary.current_numeric_facts.get("forecast_high_f") != forecast_high:
                changed_fields.append("forecast_high_f")
                updates["forecast_high_f"] = forecast_high
            if current_temp is not None and dossier.summary.current_numeric_facts.get("current_temp_f") != current_temp:
                changed_fields.append("current_temp_f")
                updates["current_temp_f"] = current_temp
        summary = (
            "No material changes since the shared dossier refresh."
            if not changed_fields
            else f"Room delta observed changes in {', '.join(changed_fields)}."
        )
        return ResearchDelta(summary=summary, changed_fields=changed_fields, numeric_fact_updates=updates)

    def build_structured_dossier_from_snapshot(
        self,
        *,
        market_ticker: str,
        market_response: dict[str, Any],
        mapping: WeatherMarketMapping,
        weather_bundle: dict[str, Any],
        reference_time: datetime,
        last_run_id: str,
    ) -> ResearchDossier:
        market = _coerce_market(market_response)
        weather_signal = self.signal_engine.evaluate(
            mapping,
            market_response,
            weather_bundle,
            min_edge_bps=self.settings.risk_min_edge_bps,
        )
        sources = self._kalshi_sources(mapping, market_ticker, market, retrieved_at=reference_time)
        claims = self._kalshi_claims(mapping, market_ticker, market, sources[0].source_key)
        weather_sources, weather_claims = self._structured_weather_research(
            mapping,
            weather_bundle,
            weather_signal,
            retrieved_at=reference_time,
        )
        sources.extend(weather_sources)
        claims.extend(weather_claims)
        return self._build_dossier(
            market_ticker=market_ticker,
            market=market,
            mapping=mapping,
            sources=self._dedupe_sources(sources),
            claims=claims,
            weather_signal=weather_signal,
            web_payload=None,
            last_run_id=last_run_id,
            reference_time=reference_time,
        )

    def build_signal_from_dossier(
        self,
        dossier: ResearchDossier,
        market_response: dict[str, Any],
        *,
        min_edge_bps: int | None = None,
    ) -> StrategySignal:
        dossier = self._hydrate_runtime_fields(dossier)
        age_seconds = (datetime.now(UTC) - dossier.created_at).total_seconds()
        if age_seconds > self.settings.research_stale_seconds:
            return StrategySignal(
                fair_yes_dollars=Decimal("0.5000"),
                confidence=0.0,
                edge_bps=0,
                recommended_action=None,
                recommended_side=None,
                target_yes_price_dollars=None,
                summary=f"Stand down: dossier is {age_seconds:.0f}s old (max {self.settings.research_stale_seconds}s).",
                stand_down_reason=StandDownReason.DOSSIER_STALE,
            )
        market = _coerce_market(market_response)
        fair_yes = dossier.trader_context.fair_yes_dollars
        if fair_yes is None or fair_yes == Decimal("0.5000"):
            return StrategySignal(
                fair_yes_dollars=Decimal("0.5000"),
                confidence=0.0,
                edge_bps=0,
                recommended_action=None,
                recommended_side=None,
                target_yes_price_dollars=None,
                summary="Stand down: dossier fair value is unavailable or neutral sentinel.",
                stand_down_reason=StandDownReason.FORECAST_UNAVAILABLE,
            )
        ask_yes = quantize_price(market.get("yes_ask_dollars")) if market.get("yes_ask_dollars") is not None else None
        bid_yes = quantize_price(market.get("yes_bid_dollars")) if market.get("yes_bid_dollars") is not None else None
        ask_no = quantize_price(market.get("no_ask_dollars")) if market.get("no_ask_dollars") is not None else None
        effective_min_edge_bps = min_edge_bps if min_edge_bps is not None else self.settings.risk_min_edge_bps
        min_edge = Decimal(effective_min_edge_bps) / Decimal("10000")

        recommendation_action = None
        recommendation_side = None
        target_yes = None
        edge_bps = 0

        if ask_yes is not None and fair_yes - ask_yes >= min_edge:
            recommendation_action = TradeAction.BUY
            recommendation_side = ContractSide.YES
            target_yes = ask_yes
            edge_bps = int(((fair_yes - ask_yes) * Decimal("10000")).to_integral_value())
        elif ask_no is not None:
            fair_no = Decimal("1.0000") - fair_yes
            edge_no = fair_no - ask_no
            if edge_no >= min_edge:
                recommendation_action = TradeAction.BUY
                recommendation_side = ContractSide.NO
                target_yes = quantize_price(Decimal("1.0000") - ask_no)
                edge_bps = int((edge_no * Decimal("10000")).to_integral_value())
        elif bid_yes is not None and fair_yes - bid_yes >= min_edge:
            edge_bps = int(((fair_yes - bid_yes) * Decimal("10000")).to_integral_value())

        summary = summarize_signal_action(
            base_strategy_summary(dossier.trader_context.thesis),
            recommendation_action=recommendation_action,
            recommendation_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            edge_bps=edge_bps,
            market_snapshot=market_response,
            spread_limit_bps=self.settings.trigger_max_spread_bps,
        )
        signal = StrategySignal(
            fair_yes_dollars=fair_yes,
            confidence=dossier.trader_context.confidence,
            edge_bps=edge_bps,
            recommended_action=recommendation_action,
            recommended_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            weather=None,
            resolution_state=dossier.trader_context.resolution_state,
            strategy_mode=dossier.trader_context.strategy_mode,
            trade_regime=dossier.trade_regime or dossier.trader_context.trade_regime,
            capital_bucket=dossier.capital_bucket or dossier.trader_context.capital_bucket,
            forecast_delta_f=(
                dossier.forecast_delta_f
                if dossier.forecast_delta_f is not None
                else dossier.trader_context.forecast_delta_f
            ),
            confidence_band=dossier.confidence_band or dossier.trader_context.confidence_band,
        )
        return annotate_signal_quality(
            settings=self.settings,
            signal=signal,
            market_snapshot=market_response,
        )

    def _build_dossier(
        self,
        *,
        market_ticker: str,
        market: dict[str, Any],
        mapping: WeatherMarketMapping | None,
        sources: list[ResearchSourceCard],
        claims: list[ResearchClaim],
        weather_signal: Any,
        web_payload: dict[str, Any] | None,
        last_run_id: str,
        reference_time: datetime | None = None,
    ) -> ResearchDossier:
        now = (reference_time.astimezone(UTC) if reference_time is not None else datetime.now(UTC))
        narrative = ""
        bullish_case = ""
        bearish_case = ""
        unresolved = []
        fair_yes: Decimal | None = None
        confidence = 0.0
        thesis = ""
        numeric_facts = self._market_numeric_facts(market)
        structured_used = False
        web_used = bool(web_payload)

        if weather_signal is not None:
            weather_snapshot = weather_signal.weather
            structured_used = True
            narrative = weather_signal.summary
            bullish_case = f"Structured weather inputs imply fair yes near {weather_signal.fair_yes_dollars}."
            bearish_case = "Weather can still underperform the forecast and leave the market short of threshold."
            fair_yes = weather_signal.fair_yes_dollars
            confidence = float(weather_signal.confidence)
            thesis = weather_signal.summary
            numeric_facts.update(
                {
                    "forecast_high_f": weather_snapshot.forecast_high_f,
                    "current_temp_f": weather_snapshot.current_temp_f,
                    "forecast_delta_f": weather_snapshot.forecast_delta_f,
                    "threshold_f": mapping.threshold_f if mapping is not None else None,
                    "resolution_state": weather_snapshot.resolution_state.value,
                }
            )
        elif web_payload is not None:
            narrative = str(web_payload.get("narrative") or f"Web research dossier assembled for {market_ticker}.")
            bullish_case = str(web_payload.get("bullish_case") or "Available sources support a positive outcome.")
            bearish_case = str(web_payload.get("bearish_case") or "Available sources still leave meaningful downside.")
            unresolved = [str(item) for item in web_payload.get("unresolved_uncertainties", [])]
            fair_raw = web_payload.get("fair_yes_dollars")
            fair_yes = quantize_price(fair_raw) if fair_raw not in (None, "") else None
            confidence = float(web_payload.get("confidence") or 0.0)
            thesis = str(web_payload.get("thesis") or narrative)
        else:
            narrative = f"Research dossier assembled for {market_ticker} from market metadata."
            bullish_case = "Kalshi market structure provides a clear tradable instrument."
            bearish_case = "Research coverage is too thin to support an autonomous view."
            unresolved = ["No structured or web research source produced a fair-value estimate."]
            thesis = narrative

        settlement_covered = any(claim.settlement_critical for claim in claims) or bool(mapping and mapping.settlement_source)
        if not settlement_covered:
            unresolved.append("Settlement mechanics are not fully covered by current sources.")
        max_age = int(max((now - source.retrieved_at).total_seconds() for source in sources) if sources else 0)
        freshness = ResearchFreshness(
            refreshed_at=now,
            expires_at=now + timedelta(seconds=self.settings.research_stale_seconds),
            stale=False,
            max_source_age_seconds=max_age,
        )
        summary = ResearchSummary(
            narrative=narrative,
            bullish_case=bullish_case,
            bearish_case=bearish_case,
            unresolved_uncertainties=unresolved,
            settlement_mechanics=self._settlement_text(mapping, market),
            current_numeric_facts=numeric_facts,
            source_coverage=self._source_coverage_text(sources, structured_used=structured_used, web_used=web_used),
            research_confidence=confidence,
        )
        trader_context = ResearchTraderContext(
            fair_yes_dollars=fair_yes,
            confidence=confidence,
            thesis=thesis,
            source_keys=[source.source_key for source in sources],
            numeric_facts=numeric_facts,
            structured_source_used=structured_used,
            web_source_used=web_used,
            autonomous_ready=fair_yes is not None,
            resolution_state=(
                weather_signal.weather.resolution_state
                if weather_signal is not None and weather_signal.weather is not None
                else WeatherResolutionState.UNRESOLVED
            ),
            strategy_mode=(
                StrategyMode.RESOLVED_CLEANUP_CANDIDATE
                if weather_signal is not None
                and weather_signal.weather is not None
                and weather_signal.weather.resolution_state != WeatherResolutionState.UNRESOLVED
                else StrategyMode.DIRECTIONAL_UNRESOLVED
            ),
            trade_regime=weather_signal.trade_regime if weather_signal is not None else "standard",
            capital_bucket=(
                weather_signal.capital_bucket
                if weather_signal is not None
                else capital_bucket_for_trade_regime("standard")
            ),
            forecast_delta_f=(
                weather_signal.forecast_delta_f
                if weather_signal is not None
                else numeric_facts.get("forecast_delta_f")
            ),
            confidence_band=weather_signal.confidence_band if weather_signal is not None else "low",
            model_quality_status=weather_signal.model_quality_status if weather_signal is not None else "pass",
            model_quality_reasons=list(weather_signal.model_quality_reasons) if weather_signal is not None else [],
            recommended_size_cap_fp=(
                weather_signal.recommended_size_cap_fp
                if weather_signal is not None
                else None
            ),
            warn_only_blocked=weather_signal.warn_only_blocked if weather_signal is not None else False,
        )
        contradiction_count = sum(1 for claim in claims if claim.stance == "contradicts")
        unresolved_count = len(summary.unresolved_uncertainties)
        gate = self._gate_dossier(
            sources=sources,
            summary=summary,
            trader_context=trader_context,
            freshness=freshness,
            settlement_covered=settlement_covered,
        )
        quality = self._quality_summary(
            mapping=mapping,
            sources=sources,
            claims=claims,
            summary=summary,
            trader_context=trader_context,
            freshness=freshness,
            settlement_covered=settlement_covered,
            contradiction_count=contradiction_count,
        )
        status = "ready" if gate.passed else "blocked"
        mode = "mixed" if structured_used and web_used else "structured" if structured_used else "web" if web_used else "market_only"
        return ResearchDossier(
            market_ticker=market_ticker,
            status=status,
            mode=mode,
            summary=summary,
            freshness=freshness,
            quality=quality,
            trader_context=trader_context,
            gate=gate,
            sources=sources,
            claims=claims,
            contradiction_count=contradiction_count,
            unresolved_count=unresolved_count,
            settlement_covered=settlement_covered,
            trade_regime=trader_context.trade_regime,
            capital_bucket=trader_context.capital_bucket,
            forecast_delta_f=trader_context.forecast_delta_f,
            confidence_band=trader_context.confidence_band,
            model_quality_status=trader_context.model_quality_status,
            model_quality_reasons=trader_context.model_quality_reasons,
            recommended_size_cap_fp=trader_context.recommended_size_cap_fp,
            warn_only_blocked=trader_context.warn_only_blocked,
            created_at=now,
            last_run_id=last_run_id,
        )

    def _effective_freshness(self, freshness: ResearchFreshness, *, reference_time: datetime | None = None) -> ResearchFreshness:
        now = reference_time.astimezone(UTC) if reference_time is not None else datetime.now(UTC)
        refreshed_at = freshness.refreshed_at.astimezone(UTC)
        expires_at = freshness.expires_at.astimezone(UTC)
        stale = now >= expires_at
        grace_expires_at = expires_at + timedelta(
            seconds=self.settings.research_stale_seconds * (self.settings.research_stale_grace_factor - 1.0)
        )
        stale_grace = stale and now < grace_expires_at
        elapsed_since_refresh = max(0, int((now - refreshed_at).total_seconds()))
        return ResearchFreshness(
            refreshed_at=refreshed_at,
            expires_at=expires_at,
            stale=stale,
            stale_grace=stale_grace,
            max_source_age_seconds=max(freshness.max_source_age_seconds, elapsed_since_refresh),
        )

    def _hydrate_runtime_fields(self, dossier: ResearchDossier, *, reference_time: datetime | None = None) -> ResearchDossier:
        freshness = self._effective_freshness(dossier.freshness, reference_time=reference_time)
        gate = self._gate_dossier(
            sources=dossier.sources,
            summary=dossier.summary,
            trader_context=dossier.trader_context,
            freshness=freshness,
            settlement_covered=dossier.settlement_covered,
        )
        quality = self._quality_summary(
            mapping=None,
            sources=dossier.sources,
            claims=dossier.claims,
            summary=dossier.summary,
            trader_context=dossier.trader_context,
            freshness=freshness,
            settlement_covered=dossier.settlement_covered,
            contradiction_count=dossier.contradiction_count,
        )
        return dossier.model_copy(
            update={
                "freshness": freshness,
                "gate": gate,
                "quality": quality,
                "status": "ready" if gate.passed else "blocked",
            }
        )

    def training_quality_snapshot(self, dossier: ResearchDossier, *, reference_time: datetime | None = None) -> dict[str, Any]:
        dossier = self._hydrate_runtime_fields(dossier, reference_time=reference_time)
        structured_training_ready = dossier.mode in {"structured", "mixed"} and dossier.trader_context.structured_source_used
        good_for_training = (
            dossier.gate.passed
            and structured_training_ready
            and dossier.quality.overall_score >= self.settings.training_good_research_threshold
        )
        return {
            "market_ticker": dossier.market_ticker,
            "dossier_status": dossier.status,
            "gate_passed": dossier.gate.passed,
            "valid_dossier": dossier.status == "ready" and not dossier.freshness.stale,
            "good_for_training": good_for_training,
            "quality_score": dossier.quality.overall_score,
            "citation_coverage_score": dossier.quality.citation_coverage_score,
            "settlement_clarity_score": dossier.quality.settlement_clarity_score,
            "freshness_score": dossier.quality.freshness_score,
            "contradiction_count": dossier.contradiction_count,
            "structured_completeness_score": dossier.quality.structured_completeness_score,
            "fair_value_score": dossier.quality.fair_value_score,
            "payload": {
                "mode": dossier.mode,
                "quality": dossier.quality.model_dump(mode="json"),
                "gate": dossier.gate.model_dump(mode="json"),
                "freshness": dossier.freshness.model_dump(mode="json"),
                "effective_freshness": dossier.freshness.model_dump(mode="json"),
                "source_count": len(dossier.sources),
                "settlement_covered": dossier.settlement_covered,
                "contradiction_count": dossier.contradiction_count,
                "unresolved_count": dossier.unresolved_count,
                "structured_source_used": dossier.trader_context.structured_source_used,
                "web_source_used": dossier.trader_context.web_source_used,
                "resolution_state": dossier.trader_context.resolution_state.value,
                "strategy_mode": dossier.trader_context.strategy_mode.value,
            },
        }

    def _gate_dossier(
        self,
        *,
        sources: Sequence[ResearchSourceCard],
        summary: ResearchSummary,
        trader_context: ResearchTraderContext,
        freshness: ResearchFreshness,
        settlement_covered: bool,
    ) -> ResearchGateVerdict:
        reasons: list[str] = []
        cited = [source.source_key for source in sources]
        # Hard-block checks: stale beyond the grace window, or missing quality requirements
        if freshness.stale and not freshness.stale_grace:
            reasons.append("Research dossier is stale beyond grace window.")
        if not sources:
            reasons.append("Research dossier has no sources.")
        if not settlement_covered:
            reasons.append("Research dossier does not cover settlement mechanics.")
        if summary.unresolved_uncertainties:
            critical_unresolved = [item for item in summary.unresolved_uncertainties if "settlement" in item.lower()]
            if critical_unresolved:
                reasons.append("Research dossier has unresolved settlement-critical uncertainties.")
        if not any(source.trust_tier in {"primary", "reputable"} for source in sources):
            reasons.append("Research dossier does not cite primary or reputable sources.")
        if trader_context.fair_yes_dollars is None:
            reasons.append("Research dossier did not produce a fair-value estimate.")
        # Stale-within-grace: passes but flags stale_tolerance_active so the supervisor
        # can apply a reduced notional cap (research_stale_tolerance_notional_factor).
        stale_tolerance_active = not reasons and freshness.stale_grace
        if stale_tolerance_active:
            reasons = ["Research gate passed (stale tolerance active — reduced position cap applies)."]
        return ResearchGateVerdict(
            passed=not reasons or stale_tolerance_active,
            reasons=reasons or ["Research gate passed."],
            cited_source_keys=cited,
            stale_tolerance_active=stale_tolerance_active,
        )

    def _quality_summary(
        self,
        *,
        mapping: WeatherMarketMapping | None,
        sources: Sequence[ResearchSourceCard],
        claims: Sequence[ResearchClaim],
        summary: ResearchSummary,
        trader_context: ResearchTraderContext,
        freshness: ResearchFreshness,
        settlement_covered: bool,
        contradiction_count: int,
    ) -> ResearchQualitySummary:
        issues: list[str] = []
        citation_coverage_score = 0.0
        if claims:
            cited_claims = sum(1 for claim in claims if claim.citations)
            citation_coverage_score = round(cited_claims / len(claims), 4)
        elif sources:
            citation_coverage_score = 1.0

        has_settlement_unknown = any("settlement" in item.lower() for item in summary.unresolved_uncertainties)
        if settlement_covered and not has_settlement_unknown:
            settlement_clarity_score = 1.0
        elif settlement_covered:
            settlement_clarity_score = 0.5
        else:
            settlement_clarity_score = 0.0

        freshness_score = 0.0 if freshness.stale else max(
            0.0,
            round(1.0 - (freshness.max_source_age_seconds / max(1, self.settings.research_stale_seconds)), 4),
        )

        contradiction_score = 1.0
        if claims:
            contradiction_score = max(0.0, round(1.0 - (contradiction_count / len(claims)), 4))
        elif contradiction_count:
            contradiction_score = 0.0

        structured_completeness_score = 0.0
        if mapping is not None and mapping.supports_structured_weather:
            required_facts = [
                trader_context.numeric_facts.get("forecast_high_f"),
                trader_context.numeric_facts.get("current_temp_f"),
                mapping.threshold_f,
            ]
            structured_completeness_score = round(
                sum(1 for item in required_facts if item not in (None, "")) / len(required_facts),
                4,
            )
        elif trader_context.structured_source_used:
            structured_completeness_score = 1.0

        fair_value_score = 1.0 if trader_context.fair_yes_dollars is not None else 0.0

        overall_score = round(
            (
                citation_coverage_score * 0.20
                + settlement_clarity_score * 0.20
                + freshness_score * 0.20
                + contradiction_score * 0.15
                + structured_completeness_score * 0.15
                + fair_value_score * 0.10
            ),
            4,
        )

        if citation_coverage_score < 1.0:
            issues.append("Not every research claim is explicitly cited.")
        if settlement_clarity_score < 1.0:
            issues.append("Settlement mechanics are incomplete or still partially unresolved.")
        if freshness_score < 0.5:
            issues.append("Research sources are aging toward the stale threshold.")
        if contradiction_count > 0:
            issues.append("Research contains contradictory evidence that should be reviewed.")
        if mapping is not None and mapping.supports_structured_weather and structured_completeness_score < 1.0:
            issues.append("Structured weather facts are incomplete for this market.")
        if fair_value_score == 0.0:
            issues.append("No fair-value estimate is available for autonomous training.")
        if not any(source.trust_tier in {"primary", "reputable"} for source in sources):
            issues.append("Source trust is below the preferred training threshold.")

        return ResearchQualitySummary(
            citation_coverage_score=citation_coverage_score,
            settlement_clarity_score=settlement_clarity_score,
            freshness_score=freshness_score,
            contradiction_score=contradiction_score,
            structured_completeness_score=structured_completeness_score,
            fair_value_score=fair_value_score,
            overall_score=overall_score,
            issues=issues,
        )

    def _kalshi_sources(
        self,
        mapping: WeatherMarketMapping | None,
        market_ticker: str,
        market: dict[str, Any],
        *,
        retrieved_at: datetime | None = None,
    ) -> list[ResearchSourceCard]:
        title = str(market.get("title") or mapping.label if mapping is not None else market_ticker)
        settlement = self._settlement_text(mapping, market)
        snippet = (
            f"{title}. Yes bid {market.get('yes_bid_dollars')}, yes ask {market.get('yes_ask_dollars')}, "
            f"last price {market.get('last_price_dollars')}. Settlement: {settlement}"
        )
        return [
            ResearchSourceCard(
                source_key=_source_key("kalshi_market", market_ticker),
                source_class="kalshi_market",
                trust_tier="primary",
                publisher="Kalshi",
                title=title,
                url=None,
                snippet=snippet,
                retrieved_at=(retrieved_at.astimezone(UTC) if retrieved_at is not None else datetime.now(UTC)),
                content={"market": market},
            )
        ]

    def _kalshi_claims(
        self,
        mapping: WeatherMarketMapping | None,
        market_ticker: str,
        market: dict[str, Any],
        source_key: str,
    ) -> list[ResearchClaim]:
        claims = [
            ResearchClaim(
                source_key=source_key,
                claim=f"Kalshi quotes yes bid {market.get('yes_bid_dollars')} and yes ask {market.get('yes_ask_dollars')} for {market_ticker}.",
                stance="context",
                citations=[source_key],
            )
        ]
        settlement_text = self._settlement_text(mapping, market)
        if settlement_text:
            claims.append(
                ResearchClaim(
                    source_key=source_key,
                    claim=settlement_text,
                    stance="context",
                    settlement_critical=True,
                    citations=[source_key],
                )
            )
        return claims

    def _structured_weather_research(
        self,
        mapping: WeatherMarketMapping,
        weather_bundle: dict[str, Any],
        weather_signal: Any,
        *,
        retrieved_at: datetime | None = None,
    ) -> tuple[list[ResearchSourceCard], list[ResearchClaim]]:
        weather_snapshot = weather_signal.weather
        external_archive = dict(weather_bundle.get("_external_archive") or {})
        provider_label = str(external_archive.get("provider_label") or "NWS/NOAA")
        provider_url = str(external_archive.get("provider_url") or "https://api.weather.gov")
        provider_model = str(external_archive.get("model") or "").strip()
        source_class = "weather_archive_external" if external_archive else "weather_structured"
        trust_tier = "reputable" if external_archive else "primary"
        title = (
            f"Archived forecast run for {mapping.label}"
            if external_archive
            else f"Structured weather evidence for {mapping.label}"
        )
        source_key = _source_key("weather_structured", mapping.market_ticker)
        source = ResearchSourceCard(
            source_key=source_key,
            source_class=source_class,
            trust_tier=trust_tier,
            publisher=(f"{provider_label} {provider_model}".strip() if provider_model else provider_label),
            title=title,
            url=provider_url,
            snippet=weather_signal.summary,
            retrieved_at=(retrieved_at.astimezone(UTC) if retrieved_at is not None else datetime.now(UTC)),
            content={
                "forecast_updated_time": _to_iso(weather_snapshot.forecast_updated_time),
                "observation_time": _to_iso(weather_snapshot.observation_time),
                "archive_provider": external_archive.get("provider"),
                "archive_model": external_archive.get("model"),
                "archive_run_ts": external_archive.get("run_ts"),
            },
        )
        stance = "supports" if weather_signal.fair_yes_dollars >= Decimal("0.5000") else "contradicts"
        claims = [
            ResearchClaim(
                source_key=source_key,
                claim=weather_signal.summary,
                stance=stance,
                citations=[source_key],
            ),
            ResearchClaim(
                source_key=source_key,
                claim=f"Settlement source is {mapping.settlement_source}.",
                stance="context",
                settlement_critical=True,
                citations=[source_key],
            ),
        ]
        return [source], claims

    async def _web_sources(self, mapping: WeatherMarketMapping | None, market: dict[str, Any], *, pack) -> list[ResearchSourceCard]:
        queries = self._web_queries(mapping, market)
        urls = list(mapping.research_urls) if mapping is not None else []
        sources: list[ResearchSourceCard] = []
        max_results = pack.research.web_max_results or self.settings.research_web_max_results
        if urls:
            for url in urls[: max_results]:
                publisher = _publisher_from_url(url)
                sources.append(
                    ResearchSourceCard(
                        source_key=_source_key("web_seed", url=url, title=url),
                        source_class="web_seed",
                        trust_tier=_domain_trust_tier(url),
                        publisher=publisher,
                        title=f"Configured source from {publisher}",
                        url=url,
                        snippet=f"Configured research source for {market.get('ticker') or market.get('title')}.",
                    )
                )
        if DDGS is None:
            return sources
        max_queries = pack.research.web_max_queries or self.settings.research_web_max_queries
        results = await asyncio.to_thread(self._search_web, queries, max_queries=max_queries, max_results=max_results)
        for result in results:
            url = result.get("href") or result.get("url")
            title = str(result.get("title") or "Web research result")
            snippet = str(result.get("body") or result.get("snippet") or "")
            if not url:
                continue
            publisher = _publisher_from_url(url)
            sources.append(
                ResearchSourceCard(
                    source_key=_source_key("web_search", title=title, url=url),
                    source_class="web_search",
                    trust_tier=_domain_trust_tier(url),
                    publisher=publisher,
                    title=title,
                    url=url,
                    snippet=snippet[:800],
                )
            )
        return self._dedupe_sources(sources)[: max_results]

    def _search_web(self, queries: list[str], *, max_queries: int, max_results: int) -> list[dict[str, Any]]:
        if DDGS is None:
            return []
        results: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        with DDGS() as ddgs:
            for query in queries[: max_queries]:
                for item in ddgs.text(query, max_results=max_results):
                    url = item.get("href") or item.get("url")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    results.append(item)
        return results

    async def _web_synthesis_payload(
        self,
        *,
        mapping: WeatherMarketMapping | None,
        market: dict[str, Any],
        sources: list[ResearchSourceCard],
        pack,
    ) -> dict[str, Any]:
        market_title = str(market.get("title") or market.get("ticker"))
        fallback = {
            "narrative": f"Web research was gathered for {market_title}, but no model-backed fair value estimate is available.",
            "bullish_case": "Web sources may support the market thesis, but evidence remains incomplete.",
            "bearish_case": "Web-sourced evidence may be noisy or incomplete without a dedicated adapter.",
            "unresolved_uncertainties": ["No structured adapter is available for this market."],
            "fair_yes_dollars": None,
            "confidence": 0.0,
            "thesis": f"Research coverage for {market_title} remains incomplete.",
        }
        user_prompt = json.dumps(
            {
                "market": {
                    "ticker": market.get("ticker"),
                    "title": market.get("title"),
                    "subtitle": market.get("subtitle"),
                    "yes_bid_dollars": market.get("yes_bid_dollars"),
                    "yes_ask_dollars": market.get("yes_ask_dollars"),
                    "last_price_dollars": market.get("last_price_dollars"),
                },
                "mapping": mapping.model_dump(mode="json") if mapping is not None else None,
                "sources": [source.model_dump(mode="json") for source in sources],
            },
            indent=2,
        )
        role_config = pack.roles.get(AgentRole.RESEARCHER.value)
        return await self.providers.maybe_complete_json(
            role=AgentRole.RESEARCHER,
            fallback_payload=fallback,
            system_prompt=pack.research.synthesis_system_prompt,
            user_prompt=user_prompt,
            role_config=role_config,
            schema_model=WebSynthesisPayload,
        )

    def _web_claims(self, sources: Sequence[ResearchSourceCard]) -> list[ResearchClaim]:
        claims: list[ResearchClaim] = []
        for source in sources:
            if not source.snippet:
                continue
            claims.append(
                ResearchClaim(
                    source_key=source.source_key,
                    claim=source.snippet,
                    stance="context",
                    citations=[source.source_key],
                )
            )
        return claims

    def _web_queries(self, mapping: WeatherMarketMapping | None, market: dict[str, Any]) -> list[str]:
        if mapping is not None and mapping.research_queries:
            return mapping.research_queries
        title = str(market.get("title") or market.get("ticker") or "Kalshi market")
        subtitle = str(market.get("subtitle") or "")
        return [f"{title} {subtitle}".strip(), f"{title} settlement rules"]

    def _settlement_text(self, mapping: WeatherMarketMapping | None, market: dict[str, Any]) -> str:
        settlement_sources = market.get("settlement_sources")
        if isinstance(settlement_sources, list) and settlement_sources:
            return "; ".join(str(item) for item in settlement_sources[:3])
        if isinstance(settlement_sources, dict) and settlement_sources:
            return json.dumps(settlement_sources)
        for key in ("rules_primary_text", "rules_secondary_text", "subtitle", "result_explanation"):
            value = market.get(key)
            if value:
                return str(value)
        return mapping.settlement_source if mapping is not None else "Settlement details unavailable."

    def _source_coverage_text(self, sources: Sequence[ResearchSourceCard], *, structured_used: bool, web_used: bool) -> str:
        parts = [f"{len(sources)} total sources"]
        if structured_used:
            parts.append("structured adapter")
        if web_used:
            parts.append("web research")
        return ", ".join(parts)

    def _market_numeric_facts(self, market: dict[str, Any]) -> dict[str, Any]:
        return {
            "yes_bid_dollars": str(market.get("yes_bid_dollars")) if market.get("yes_bid_dollars") is not None else None,
            "yes_ask_dollars": str(market.get("yes_ask_dollars")) if market.get("yes_ask_dollars") is not None else None,
            "no_ask_dollars": str(market.get("no_ask_dollars")) if market.get("no_ask_dollars") is not None else None,
            "last_price_dollars": str(market.get("last_price_dollars")) if market.get("last_price_dollars") is not None else None,
            "volume": market.get("volume"),
        }

    def _dedupe_sources(self, sources: Sequence[ResearchSourceCard]) -> list[ResearchSourceCard]:
        deduped: dict[str, ResearchSourceCard] = {}
        for source in sources:
            deduped[source.source_key] = source
        return list(deduped.values())
