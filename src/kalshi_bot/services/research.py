from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, ContractSide, TradeAction
from kalshi_bot.core.fixed_point import as_decimal, quantize_price
from kalshi_bot.core.schemas import (
    ResearchClaim,
    ResearchDelta,
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchSourceCard,
    ResearchSummary,
    ResearchTraderContext,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.signal import StrategySignal, WeatherSignalEngine
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping

try:
    from duckduckgo_search import DDGS
except Exception:  # pragma: no cover - optional dependency protection
    DDGS = None  # type: ignore[assignment]


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
    return "reputable"


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
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.weather = weather
        self.weather_directory = weather_directory
        self.providers = providers
        self.signal_engine = signal_engine
        self._inflight_markets: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    async def get_latest_dossier(self, market_ticker: str) -> ResearchDossier | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            record = await repo.get_research_dossier(market_ticker)
            await session.commit()
        if record is None:
            return None
        return ResearchDossier.model_validate(record.payload)

    async def list_recent_runs(self, market_ticker: str, limit: int = 10, *, status: str | None = None) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
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
                market_response = await self.kalshi.get_market(market_ticker)
                market = _coerce_market(market_response)
                mapping = self.weather_directory.resolve_market(market_ticker, market)
                await repo.log_exchange_event("research", "market_snapshot", market_response, market_ticker=market_ticker)

                weather_bundle: dict[str, Any] | None = None
                weather_signal = None
                if mapping is not None and mapping.supports_structured_weather:
                    weather_bundle = await self.weather.build_market_snapshot(mapping)
                    await repo.log_weather_event(mapping.station_id, "research_weather_bundle", weather_bundle)
                    weather_signal = self.signal_engine.evaluate(mapping, market_response, weather_bundle)

                sources = self._kalshi_sources(mapping, market_ticker, market)
                claims = self._kalshi_claims(mapping, market_ticker, market, sources[0].source_key)

                if mapping is not None and mapping.supports_structured_weather and weather_bundle is not None and weather_signal is not None:
                    weather_sources, weather_claims = self._structured_weather_research(mapping, weather_bundle, weather_signal)
                    sources.extend(weather_sources)
                    claims.extend(weather_claims)

                use_web = mapping is None or not mapping.supports_structured_weather
                web_payload: dict[str, Any] | None = None
                if use_web:
                    web_sources = await self._web_sources(mapping, market)
                    sources.extend(web_sources)
                    claims.extend(self._web_claims(web_sources))
                    web_payload = await self._web_synthesis_payload(
                        mapping=mapping,
                        market=market,
                        sources=web_sources,
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
                    },
                )
                await session.commit()
                return dossier
            except Exception as exc:
                await repo.complete_research_run(run.id, status="failed", payload={}, error_text=str(exc))
                await repo.log_ops_event(
                    severity="error",
                    summary=f"Research refresh failed for {market_ticker}",
                    source="research",
                    payload={"market_ticker": market_ticker, "error": str(exc)},
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
            checkpoint = await repo.get_checkpoint(f"research_refresh:{market_ticker}")
            if checkpoint is not None:
                last_refresh = checkpoint.payload.get("refreshed_at")
                if last_refresh is not None:
                    refreshed_at = datetime.fromisoformat(last_refresh)
                    if datetime.now(UTC) - refreshed_at < timedelta(seconds=self.settings.research_refresh_cooldown_seconds):
                        await session.commit()
                        return
            market_state = await repo.get_market_state(market_ticker)
            dossier_record = await repo.get_research_dossier(market_ticker)
            await session.commit()

        if market_state is None:
            return
        if dossier_record is not None:
            dossier = ResearchDossier.model_validate(dossier_record.payload)
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
            forecast_high = weather_bundle.get("forecast", {}).get("properties", {}).get("periods", [{}])[0].get("temperature")
            current_temp = weather_bundle.get("observation", {}).get("properties", {}).get("temperature", {}).get("value")
            if forecast_high is not None and dossier.summary.current_numeric_facts.get("forecast_high_f") != forecast_high:
                changed_fields.append("forecast_high_f")
                updates["forecast_high_f"] = forecast_high
            if current_temp is not None and dossier.summary.current_numeric_facts.get("current_temp_c") != current_temp:
                changed_fields.append("current_temp_c")
                updates["current_temp_c"] = current_temp
        summary = (
            "No material changes since the shared dossier refresh."
            if not changed_fields
            else f"Room delta observed changes in {', '.join(changed_fields)}."
        )
        return ResearchDelta(summary=summary, changed_fields=changed_fields, numeric_fact_updates=updates)

    def build_signal_from_dossier(self, dossier: ResearchDossier, market_response: dict[str, Any]) -> StrategySignal:
        market = _coerce_market(market_response)
        fair_yes = dossier.trader_context.fair_yes_dollars or Decimal("0.5000")
        ask_yes = quantize_price(market.get("yes_ask_dollars")) if market.get("yes_ask_dollars") is not None else None
        bid_yes = quantize_price(market.get("yes_bid_dollars")) if market.get("yes_bid_dollars") is not None else None
        ask_no = quantize_price(market.get("no_ask_dollars")) if market.get("no_ask_dollars") is not None else None
        min_edge = Decimal(self.settings.risk_min_edge_bps) / Decimal("10000")

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

        summary = dossier.trader_context.thesis
        if recommendation_action is None or recommendation_side is None or target_yes is None:
            summary = f"{summary} No taker trade clears the configured edge threshold."
        else:
            summary = (
                f"{summary} Recommend {recommendation_action.value} {recommendation_side.value} at yes price {target_yes} "
                f"with edge {edge_bps} bps."
            )
        return StrategySignal(
            fair_yes_dollars=fair_yes,
            confidence=dossier.trader_context.confidence,
            edge_bps=edge_bps,
            recommended_action=recommendation_action,
            recommended_side=recommendation_side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            weather=None,
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
    ) -> ResearchDossier:
        now = datetime.now(UTC)
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
        status = "ready" if gate.passed else "blocked"
        mode = "mixed" if structured_used and web_used else "structured" if structured_used else "web" if web_used else "market_only"
        return ResearchDossier(
            market_ticker=market_ticker,
            status=status,
            mode=mode,
            summary=summary,
            freshness=freshness,
            trader_context=trader_context,
            gate=gate,
            sources=sources,
            claims=claims,
            contradiction_count=contradiction_count,
            unresolved_count=unresolved_count,
            settlement_covered=settlement_covered,
            created_at=now,
            last_run_id=last_run_id,
        )

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
        if freshness.stale:
            reasons.append("Research dossier is stale.")
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
        return ResearchGateVerdict(passed=not reasons, reasons=reasons or ["Research gate passed."], cited_source_keys=cited)

    def _kalshi_sources(self, mapping: WeatherMarketMapping | None, market_ticker: str, market: dict[str, Any]) -> list[ResearchSourceCard]:
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
    ) -> tuple[list[ResearchSourceCard], list[ResearchClaim]]:
        weather_snapshot = weather_signal.weather
        source_key = _source_key("weather_structured", mapping.market_ticker)
        source = ResearchSourceCard(
            source_key=source_key,
            source_class="weather_structured",
            trust_tier="primary",
            publisher="NWS/NOAA",
            title=f"Structured weather evidence for {mapping.label}",
            url="https://api.weather.gov",
            snippet=weather_signal.summary,
            content={
                "forecast_updated_time": _to_iso(weather_snapshot.forecast_updated_time),
                "observation_time": _to_iso(weather_snapshot.observation_time),
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

    async def _web_sources(self, mapping: WeatherMarketMapping | None, market: dict[str, Any]) -> list[ResearchSourceCard]:
        queries = self._web_queries(mapping, market)
        urls = list(mapping.research_urls) if mapping is not None else []
        sources: list[ResearchSourceCard] = []
        if urls:
            for url in urls[: self.settings.research_web_max_results]:
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
        results = await asyncio.to_thread(self._search_web, queries)
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
        return self._dedupe_sources(sources)[: self.settings.research_web_max_results]

    def _search_web(self, queries: list[str]) -> list[dict[str, Any]]:
        if DDGS is None:
            return []
        results: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        with DDGS() as ddgs:
            for query in queries[: self.settings.research_web_max_queries]:
                for item in ddgs.text(query, max_results=self.settings.research_web_max_results):
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
        return await self.providers.maybe_complete_json(
            role=AgentRole.RESEARCHER,
            fallback_payload=fallback,
            system_prompt=(
                "You are the research synthesis agent for a Kalshi trading system. "
                "Return JSON only. Estimate fair_yes_dollars only if the cited sources support a reasoned probability view. "
                "Do not fabricate citations. Keep unresolved_uncertainties concise."
            ),
            user_prompt=user_prompt,
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
