from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import io
import math
import re
from typing import Any, Iterable, Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi_leaderboard import (
    LEADERBOARD_NAMES,
    LEADERBOARD_TIME_WINDOWS,
    KalshiLeaderboardScraper,
    KalshiLeaderboardSnapshot,
    normalize_leaderboard_category,
    normalize_leaderboard_name,
    normalize_leaderboard_time_window,
)


DEFAULT_MIRROR_ANALYSIS_METRICS = ("projected_pnl", "num_markets_traded", "volume")
DEFAULT_MIRROR_ANALYSIS_WINDOWS = ("daily", "weekly", "monthly", "all_time")
DEFAULT_ACTIVE_CATEGORIES = ("Crypto", "Climate and Weather")

CRYPTO_TICKER_PREFIXES = (
    "KXBTC",
    "KXETH",
    "KXSOL",
    "KXXRP",
    "KXDOGE",
    "KXADA",
    "KXLTC",
    "KXBCH",
    "KXBNB",
    "KXAVAX",
    "KXLINK",
    "KXCRYPTO",
)
WEATHER_TICKER_PREFIXES = (
    "KXHIGH",
    "KXLOW",
    "KXRAIN",
    "KXSNOW",
    "KXTEMP",
    "KXWEATHER",
)

AGGREGATE_ONLY_CAUTION = (
    "Leaderboard evidence is aggregate by category; verify public trade activity and exact market "
    "overlap before mirroring."
)


@dataclass(slots=True)
class ActiveMarketExposure:
    market_ticker: str
    category: str | None
    sources: list[str] = field(default_factory=list)
    side: str | None = None
    count: str | None = None
    average_price_dollars: str | None = None
    updated_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_ticker": self.market_ticker,
            "category": self.category,
            "sources": self.sources,
            "side": self.side,
            "count": self.count,
            "average_price_dollars": self.average_price_dollars,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class LeaderboardEvidence:
    metric: str
    time_window: str
    category: str
    rank: int | None
    value: Any
    value_numeric: float | None
    source_url: str
    fetched_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "time_window": self.time_window,
            "category": self.category,
            "rank": self.rank,
            "value": self.value,
            "value_numeric": self.value_numeric,
            "source_url": self.source_url,
            "fetched_at": self.fetched_at,
        }


@dataclass(slots=True)
class LeaderboardMirrorCandidate:
    username: str
    display_name: str | None
    member_id: str | None
    profile_image_url: str | None
    score: float
    recommendation: str
    matched_categories: list[str]
    evidence: list[LeaderboardEvidence]
    reasons: list[str]
    cautions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "username": self.username,
            "display_name": self.display_name,
            "member_id": self.member_id,
            "profile_image_url": self.profile_image_url,
            "score": round(self.score, 2),
            "recommendation": self.recommendation,
            "matched_categories": self.matched_categories,
            "evidence": [item.to_dict() for item in self.evidence],
            "reasons": self.reasons,
            "cautions": self.cautions,
        }


@dataclass(slots=True)
class LeaderboardMirrorReport:
    generated_at: datetime
    kalshi_env: str
    active_markets: list[ActiveMarketExposure]
    categories: list[str]
    metrics: list[str]
    time_windows: list[str]
    candidates: list[LeaderboardMirrorCandidate]
    skipped_anonymous_entries: int
    fetch_errors: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "kalshi_env": self.kalshi_env,
            "active_markets": [item.to_dict() for item in self.active_markets],
            "categories": self.categories,
            "metrics": self.metrics,
            "time_windows": self.time_windows,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "skipped_anonymous_entries": self.skipped_anonymous_entries,
            "fetch_errors": self.fetch_errors,
            "warnings": self.warnings,
        }


class LeaderboardMirrorAnalysisService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession] | None,
        scraper: KalshiLeaderboardScraper,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.scraper = scraper

    async def analyze(
        self,
        *,
        kalshi_env: str | None = None,
        explicit_market_tickers: Sequence[str] = (),
        categories: Sequence[str] | None = None,
        metrics: Sequence[str] | None = None,
        time_windows: Sequence[str] | None = None,
        limit_per_board: int = 50,
        top: int = 20,
        source: str = "direct",
        require_auth: bool = True,
        include_all_category: bool = False,
        skip_database_active_markets: bool = False,
        position_limit: int = 200,
        active_room_limit: int = 100,
    ) -> LeaderboardMirrorReport:
        env = (kalshi_env or self.settings.kalshi_env or "demo").strip() or "demo"
        active_markets, active_warnings = await self.collect_active_markets(
            kalshi_env=env,
            explicit_market_tickers=explicit_market_tickers,
            position_limit=position_limit,
            active_room_limit=active_room_limit,
            include_database=not skip_database_active_markets,
        )
        resolved_categories = _resolve_categories(
            explicit_categories=categories,
            active_markets=active_markets,
            include_all_category=include_all_category,
        )
        resolved_metrics = _resolve_metrics(metrics)
        resolved_windows = _resolve_time_windows(time_windows)

        snapshots: list[KalshiLeaderboardSnapshot] = []
        fetch_errors: list[str] = []
        for category in resolved_categories:
            for metric in resolved_metrics:
                for time_window in resolved_windows:
                    try:
                        snapshots.append(
                            await self.scraper.fetch(
                                name=metric,
                                time_window=time_window,
                                category=category,
                                limit=limit_per_board,
                                source=source,
                                require_auth=require_auth,
                            )
                        )
                    except Exception as exc:
                        label = category or "(all)"
                        fetch_errors.append(f"{metric}/{time_window}/{label}: {exc}")

        if not snapshots and fetch_errors:
            raise RuntimeError("Unable to fetch any Kalshi leaderboard snapshots: " + "; ".join(fetch_errors[:3]))

        return build_leaderboard_mirror_report(
            kalshi_env=env,
            active_markets=active_markets,
            snapshots=snapshots,
            metrics=resolved_metrics,
            time_windows=resolved_windows,
            fetch_errors=fetch_errors,
            warnings=active_warnings,
            top=top,
        )

    async def collect_active_markets(
        self,
        *,
        kalshi_env: str,
        explicit_market_tickers: Sequence[str] = (),
        position_limit: int = 200,
        active_room_limit: int = 100,
        include_database: bool = True,
    ) -> tuple[list[ActiveMarketExposure], list[str]]:
        exposures: dict[str, ActiveMarketExposure] = {}
        warnings: list[str] = []

        for ticker in explicit_market_tickers:
            _merge_exposure(exposures, str(ticker), source="cli")

        if not include_database or self.session_factory is None:
            if not exposures:
                warnings.append("No database active-market scan was available; using default active categories.")
            return _sorted_exposures(exposures), warnings

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            positions = await repo.list_positions(limit=position_limit, kalshi_env=kalshi_env)
            for position in positions:
                exposure = _merge_exposure(exposures, position.market_ticker, source="position")
                exposure.side = position.side
                exposure.count = str(position.count_fp)
                exposure.average_price_dollars = str(position.average_price_dollars)
                exposure.updated_at = _iso_or_none(position.updated_at)

            active_rooms = await repo.list_active_rooms(kalshi_env=kalshi_env, limit=active_room_limit)
            for room in active_rooms:
                exposure = _merge_exposure(exposures, room.market_ticker, source="active_room")
                if exposure.updated_at is None:
                    exposure.updated_at = _iso_or_none(room.updated_at)

        if not exposures:
            warnings.append(
                "No open positions or active rooms were found; analysis falls back to default traded categories."
            )
        return _sorted_exposures(exposures), warnings


def build_leaderboard_mirror_report(
    *,
    kalshi_env: str,
    active_markets: Sequence[ActiveMarketExposure],
    snapshots: Sequence[KalshiLeaderboardSnapshot],
    metrics: Sequence[str] | None = None,
    time_windows: Sequence[str] | None = None,
    fetch_errors: Sequence[str] = (),
    warnings: Sequence[str] = (),
    top: int = 20,
) -> LeaderboardMirrorReport:
    active_categories = _active_categories(active_markets)
    candidate_map: dict[str, _CandidateAccumulator] = {}
    skipped_anonymous = 0

    for snapshot in snapshots:
        category = normalize_leaderboard_category(snapshot.category)
        for entry in snapshot.entries:
            if entry.is_anonymous is True:
                skipped_anonymous += 1
                continue
            username = _clean_username(entry.username or entry.display_name)
            if username is None:
                continue
            key = (entry.member_id or username).casefold()
            accumulator = candidate_map.get(key)
            if accumulator is None:
                accumulator = _CandidateAccumulator(
                    username=username,
                    display_name=entry.display_name,
                    member_id=entry.member_id,
                    profile_image_url=entry.profile_image_url,
                )
                candidate_map[key] = accumulator
            else:
                accumulator.display_name = accumulator.display_name or entry.display_name
                accumulator.member_id = accumulator.member_id or entry.member_id
                accumulator.profile_image_url = accumulator.profile_image_url or entry.profile_image_url
            accumulator.evidence.append(
                LeaderboardEvidence(
                    metric=snapshot.name,
                    time_window=snapshot.time_window,
                    category=category,
                    rank=entry.rank,
                    value=entry.value,
                    value_numeric=_numeric_value(entry.value),
                    source_url=snapshot.source_url,
                    fetched_at=snapshot.fetched_at.isoformat(),
                )
            )

    candidates = [
        _score_candidate(accumulator, active_categories=active_categories)
        for accumulator in candidate_map.values()
    ]
    candidates.sort(
        key=lambda candidate: (
            candidate.recommendation != "shadow_mirror_candidate",
            -candidate.score,
            candidate.username.casefold(),
        )
    )
    if top > 0:
        candidates = candidates[:top]

    report_warnings = list(warnings)
    report_warnings.append(AGGREGATE_ONLY_CAUTION)
    if fetch_errors:
        report_warnings.append("Some leaderboard boards failed to fetch; see fetch_errors for details.")

    return LeaderboardMirrorReport(
        generated_at=datetime.now(UTC),
        kalshi_env=kalshi_env,
        active_markets=list(active_markets),
        categories=_categories_from_snapshots_or_active(snapshots, active_categories),
        metrics=list(metrics or DEFAULT_MIRROR_ANALYSIS_METRICS),
        time_windows=list(time_windows or DEFAULT_MIRROR_ANALYSIS_WINDOWS),
        candidates=candidates,
        skipped_anonymous_entries=skipped_anonymous,
        fetch_errors=list(fetch_errors),
        warnings=_dedupe(report_warnings),
    )


def leaderboard_mirror_report_to_csv(report: LeaderboardMirrorReport) -> str:
    output = io.StringIO()
    columns = [
        "recommendation",
        "score",
        "username",
        "display_name",
        "member_id",
        "matched_categories",
        "best_projected_pnl_rank",
        "best_projected_pnl_value",
        "best_projected_pnl_time",
        "best_num_markets_traded_rank",
        "best_volume_rank",
        "reasons",
        "cautions",
    ]
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for candidate in report.candidates:
        best_pnl = _best_evidence(candidate.evidence, "projected_pnl")
        best_markets = _best_evidence(candidate.evidence, "num_markets_traded")
        best_volume = _best_evidence(candidate.evidence, "volume")
        writer.writerow(
            {
                "recommendation": candidate.recommendation,
                "score": round(candidate.score, 2),
                "username": candidate.username,
                "display_name": candidate.display_name or "",
                "member_id": candidate.member_id or "",
                "matched_categories": ";".join(candidate.matched_categories),
                "best_projected_pnl_rank": best_pnl.rank if best_pnl else "",
                "best_projected_pnl_value": best_pnl.value if best_pnl else "",
                "best_projected_pnl_time": best_pnl.time_window if best_pnl else "",
                "best_num_markets_traded_rank": best_markets.rank if best_markets else "",
                "best_volume_rank": best_volume.rank if best_volume else "",
                "reasons": " | ".join(candidate.reasons),
                "cautions": " | ".join(candidate.cautions),
            }
        )
    return output.getvalue()


def format_leaderboard_mirror_report(report: LeaderboardMirrorReport) -> str:
    lines = [
        f"Kalshi leaderboard mirror analysis ({report.kalshi_env})",
        f"Generated: {report.generated_at.isoformat()}",
        f"Categories: {', '.join(category or '(all)' for category in report.categories)}",
        "",
    ]
    if report.active_markets:
        lines.append("Active market basis:")
        for market in report.active_markets[:20]:
            category = market.category or "unknown"
            lines.append(f"- {market.market_ticker} [{category}] from {', '.join(market.sources)}")
        if len(report.active_markets) > 20:
            lines.append(f"- ... {len(report.active_markets) - 20} more")
        lines.append("")
    if report.warnings:
        lines.append("Warnings:")
        for warning in report.warnings:
            lines.append(f"- {warning}")
        lines.append("")
    lines.append("Candidates:")
    if not report.candidates:
        lines.append("- none")
    for candidate in report.candidates:
        lines.append(
            f"- {candidate.username}: {candidate.recommendation}, score={candidate.score:.1f}, "
            f"categories={', '.join(candidate.matched_categories) or 'unknown'}"
        )
        if candidate.reasons:
            lines.append(f"  reasons: {'; '.join(candidate.reasons[:3])}")
        if candidate.cautions:
            lines.append(f"  cautions: {'; '.join(candidate.cautions[:2])}")
    if report.fetch_errors:
        lines.append("")
        lines.append("Fetch errors:")
        for error in report.fetch_errors[:10]:
            lines.append(f"- {error}")
        if len(report.fetch_errors) > 10:
            lines.append(f"- ... {len(report.fetch_errors) - 10} more")
    return "\n".join(lines) + "\n"


def infer_leaderboard_category_for_market(market_ticker: str) -> str | None:
    ticker = str(market_ticker or "").upper().strip()
    if not ticker:
        return None
    if ticker.startswith(CRYPTO_TICKER_PREFIXES):
        return "Crypto"
    if ticker.startswith(WEATHER_TICKER_PREFIXES):
        return "Climate and Weather"
    return None


@dataclass(slots=True)
class _CandidateAccumulator:
    username: str
    display_name: str | None
    member_id: str | None
    profile_image_url: str | None
    evidence: list[LeaderboardEvidence] = field(default_factory=list)


def _score_candidate(
    accumulator: _CandidateAccumulator,
    *,
    active_categories: Sequence[str],
) -> LeaderboardMirrorCandidate:
    matched_categories = sorted(
        {
            evidence.category
            for evidence in accumulator.evidence
            if evidence.category and (not active_categories or evidence.category in active_categories)
        }
    )
    relevant = [
        evidence
        for evidence in accumulator.evidence
        if not active_categories or not evidence.category or evidence.category in active_categories
    ]
    pnl_evidence = [item for item in relevant if item.metric == "projected_pnl"]
    positive_pnl = [item for item in pnl_evidence if (item.value_numeric or 0.0) > 0.0]
    markets_evidence = [item for item in relevant if item.metric == "num_markets_traded"]
    volume_evidence = [item for item in relevant if item.metric == "volume"]

    score = 0.0
    best_pnl = _best_evidence(positive_pnl, "projected_pnl")
    if best_pnl is not None:
        score += _rank_points(best_pnl.rank, max_points=38.0, floor_rank=50)
        score += _value_points(best_pnl.value_numeric, max_points=16.0)

    best_markets = _best_evidence(markets_evidence, "num_markets_traded")
    if best_markets is not None:
        score += _rank_points(best_markets.rank, max_points=10.0, floor_rank=50)
        score += _value_points(best_markets.value_numeric, max_points=6.0, scale=3.0)

    best_volume = _best_evidence(volume_evidence, "volume")
    if best_volume is not None:
        score += _rank_points(best_volume.rank, max_points=8.0, floor_rank=50)
        score += _value_points(best_volume.value_numeric, max_points=5.0, scale=2.0)

    pnl_windows = {item.time_window for item in positive_pnl}
    if len(pnl_windows) >= 2:
        score += 10.0
    if pnl_windows & {"monthly", "yearly", "all_time"}:
        score += 10.0
    if matched_categories:
        score += 8.0
    if len({item.metric for item in relevant}) >= 2:
        score += 5.0

    reasons: list[str] = []
    if best_pnl is not None:
        rank = f"rank {best_pnl.rank}" if best_pnl.rank is not None else "rank unavailable"
        reasons.append(f"best projected PnL {rank} in {best_pnl.category or 'all categories'} ({best_pnl.time_window})")
    if len(pnl_windows) >= 2:
        reasons.append("positive projected PnL across multiple windows")
    if pnl_windows & {"monthly", "yearly", "all_time"}:
        reasons.append("has longer-window projected PnL evidence")
    if best_markets is not None:
        reasons.append("appears on markets-traded leaderboard")
    if best_volume is not None:
        reasons.append("appears on volume leaderboard")

    cautions = [AGGREGATE_ONLY_CAUTION]
    if accumulator.member_id is None:
        cautions.append("No stable social member id was present in the leaderboard payload.")
    if not positive_pnl:
        cautions.append("No positive projected PnL evidence was found in fetched boards.")
    if positive_pnl and not (pnl_windows & {"monthly", "yearly", "all_time"}):
        cautions.append("Evidence is short-window only.")
    if not matched_categories:
        cautions.append("No active traded category match was established.")

    score = min(100.0, max(0.0, score))
    if score >= 70 and best_pnl is not None and (pnl_windows & {"monthly", "yearly", "all_time"}) and best_markets is not None:
        recommendation = "shadow_mirror_candidate"
    elif score >= 50 and best_pnl is not None:
        recommendation = "watchlist"
    else:
        recommendation = "monitor_only"

    return LeaderboardMirrorCandidate(
        username=accumulator.username,
        display_name=accumulator.display_name,
        member_id=accumulator.member_id,
        profile_image_url=accumulator.profile_image_url,
        score=score,
        recommendation=recommendation,
        matched_categories=matched_categories,
        evidence=sorted(
            accumulator.evidence,
            key=lambda item: (
                item.category,
                LEADERBOARD_NAMES.index(item.metric) if item.metric in LEADERBOARD_NAMES else 99,
                LEADERBOARD_TIME_WINDOWS.index(item.time_window) if item.time_window in LEADERBOARD_TIME_WINDOWS else 99,
                item.rank if item.rank is not None else 9999,
            ),
        ),
        reasons=_dedupe(reasons),
        cautions=_dedupe(cautions),
    )


def _resolve_categories(
    *,
    explicit_categories: Sequence[str] | None,
    active_markets: Sequence[ActiveMarketExposure],
    include_all_category: bool,
) -> list[str]:
    categories = [normalize_leaderboard_category(item) for item in explicit_categories or ()]
    if not categories:
        categories = _active_categories(active_markets)
    if not categories:
        categories = list(DEFAULT_ACTIVE_CATEGORIES)
    if include_all_category:
        categories.insert(0, "")
    return _dedupe(categories)


def _resolve_metrics(metrics: Sequence[str] | None) -> list[str]:
    values = list(metrics or DEFAULT_MIRROR_ANALYSIS_METRICS)
    return _dedupe([normalize_leaderboard_name(value) for value in values])


def _resolve_time_windows(time_windows: Sequence[str] | None) -> list[str]:
    values = list(time_windows or DEFAULT_MIRROR_ANALYSIS_WINDOWS)
    return _dedupe([normalize_leaderboard_time_window(value) for value in values])


def _active_categories(active_markets: Sequence[ActiveMarketExposure]) -> list[str]:
    return _dedupe([item.category for item in active_markets if item.category])


def _categories_from_snapshots_or_active(
    snapshots: Sequence[KalshiLeaderboardSnapshot],
    active_categories: Sequence[str],
) -> list[str]:
    categories = _dedupe([snapshot.category for snapshot in snapshots if snapshot.category])
    if categories:
        return categories
    return list(active_categories or DEFAULT_ACTIVE_CATEGORIES)


def _merge_exposure(
    exposures: dict[str, ActiveMarketExposure],
    market_ticker: str,
    *,
    source: str,
) -> ActiveMarketExposure:
    ticker = str(market_ticker or "").strip()
    if not ticker:
        raise ValueError("market ticker cannot be blank")
    exposure = exposures.get(ticker)
    if exposure is None:
        exposure = ActiveMarketExposure(
            market_ticker=ticker,
            category=infer_leaderboard_category_for_market(ticker),
            sources=[],
        )
        exposures[ticker] = exposure
    if source not in exposure.sources:
        exposure.sources.append(source)
    return exposure


def _sorted_exposures(exposures: dict[str, ActiveMarketExposure]) -> list[ActiveMarketExposure]:
    return sorted(exposures.values(), key=lambda item: item.market_ticker)


def _best_evidence(
    evidence: Iterable[LeaderboardEvidence],
    metric: str,
) -> LeaderboardEvidence | None:
    candidates = [item for item in evidence if item.metric == metric]
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (
            item.rank if item.rank is not None else 9999,
            -(item.value_numeric or 0.0),
        ),
    )[0]


def _rank_points(rank: int | None, *, max_points: float, floor_rank: int) -> float:
    if rank is None or rank <= 0:
        return 0.0
    if rank > floor_rank:
        return 0.0
    return max_points * ((floor_rank - rank + 1) / floor_rank)


def _value_points(value: float | None, *, max_points: float, scale: float = 4.0) -> float:
    if value is None or value <= 0:
        return 0.0
    return min(max_points, math.log10(value + 1.0) * scale)


def _numeric_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").replace("%", "")
    text = re.sub(r"^\((.+)\)$", r"-\1", text)
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def _clean_username(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    result: list[Any] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
