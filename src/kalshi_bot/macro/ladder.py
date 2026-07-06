"""Kalshi CPI ladder parsing + read-only fetch helpers.

Parses `KXCPI-<MON>` (MoM) and `KXCPIYOY-<MON>` (YoY) markets into sorted strike ladders using
the correct fixed-point/dollar field names — verified live 2026-07-06:
`yes_bid_dollars` / `yes_ask_dollars` (quote), `volume_fp` / `open_interest_fp` (fixed-point
size), `floor_strike` + `strike_type="greater"` (the ladder is "> floor_strike"), `result`
(`"yes"` / `"no"` once `status="finalized"`). This mirrors the crypto module's field-name
gotcha (`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`).

GET-only: no order fields are read or written here. The fetch helpers hit Kalshi's public,
unauthenticated market-data endpoints (no API key needed for GET on public markets).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import httpx

logger = logging.getLogger(__name__)

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

Result = Literal["yes", "no"]


@dataclass(frozen=True, slots=True)
class LadderRung:
    """One strike of a Kalshi CPI ladder."""

    ticker: str
    floor_strike: float
    strike_type: str
    yes_bid: float
    yes_ask: float
    mid: float
    volume: float
    open_interest: float
    result: Result | None


def parse_market(raw: dict[str, Any]) -> LadderRung:
    """Parse one raw `/markets` row. Raises on missing/malformed required fields."""
    floor_strike = raw["floor_strike"]
    if floor_strike is None:
        raise ValueError(f"market {raw.get('ticker')!r} has no floor_strike")
    yes_bid = float(raw["yes_bid_dollars"])
    yes_ask = float(raw["yes_ask_dollars"])
    volume = float(raw.get("volume_fp") or 0.0)
    open_interest = float(raw.get("open_interest_fp") or 0.0)
    result_raw = raw.get("result") or None
    result: Result | None = result_raw if result_raw in ("yes", "no") else None
    return LadderRung(
        ticker=raw["ticker"],
        floor_strike=float(floor_strike),
        strike_type=raw.get("strike_type", ""),
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        mid=(yes_bid + yes_ask) / 2.0,
        volume=volume,
        open_interest=open_interest,
        result=result,
    )


def parse_ladder(raw_markets: Iterable[dict[str, Any]]) -> list[LadderRung]:
    """Parse a batch of raw market rows into a strike-sorted ladder.

    Skips malformed rows with a logged count rather than raising for the whole batch or
    silently dropping the ladder (per the spec's error-handling section).
    """
    rungs: list[LadderRung] = []
    skipped = 0
    for raw in raw_markets:
        try:
            rungs.append(parse_market(raw))
        except (KeyError, TypeError, ValueError):
            skipped += 1
            continue
    if skipped:
        logger.warning("macro.ladder: skipped %d malformed market row(s)", skipped)
    rungs.sort(key=lambda r: r.floor_strike)
    return rungs


def near_money_rungs(rungs: Sequence[LadderRung], *, low: float = 0.05, high: float = 0.95) -> list[LadderRung]:
    """Rungs whose mid price sits in the tradeable band (excludes near-certain far strikes)."""
    return [r for r in rungs if low <= r.mid <= high]


async def _get_with_retry(
    client: httpx.AsyncClient, url: str, *, params: dict[str, Any], max_attempts: int = 6
) -> httpx.Response:
    """GET with exponential backoff on 429 (the public API rate-limits aggressively).

    Empirically needed: a bare loop over ~75 markets' candlesticks hits 429s within the first
    dozen requests at default httpx pacing.
    """
    for attempt in range(max_attempts):
        response = await client.get(url, params=params)
        if response.status_code != 429:
            response.raise_for_status()
            return response
        if attempt == max_attempts - 1:
            response.raise_for_status()
        await asyncio.sleep(2**attempt)
    raise AssertionError("unreachable")  # pragma: no cover - loop always returns or raises


async def fetch_markets_for_event(client: httpx.AsyncClient, event_ticker: str) -> list[dict[str, Any]]:
    """GET all markets for a Kalshi event ticker (e.g. ``KXCPI-26MAY``)."""
    response = await _get_with_retry(
        client, f"{KALSHI_API_BASE}/markets", params={"event_ticker": event_ticker, "limit": 500}
    )
    return response.json().get("markets", [])


async def fetch_settled_events(client: httpx.AsyncClient, series_ticker: str, *, limit: int = 200) -> list[dict[str, Any]]:
    """GET settled events for a series ticker (e.g. ``KXCPI``)."""
    response = await _get_with_retry(
        client,
        f"{KALSHI_API_BASE}/events",
        params={"series_ticker": series_ticker, "status": "settled", "limit": limit},
    )
    return response.json().get("events", [])


async def fetch_candlesticks(
    client: httpx.AsyncClient,
    series_ticker: str,
    market_ticker: str,
    *,
    start_ts: int,
    end_ts: int,
    period_interval_minutes: int = 1440,
) -> list[dict[str, Any]]:
    """GET daily (or other interval) candlesticks for one market.

    Note: the public API's history retention is short (~2 months as of 2026-07-06 — confirmed
    empirically: `KXCPI-26MAR`/`KXCPI-26FEB` and older events return zero markets from
    `/markets?event_ticker=...` even though they still appear in the settled-events listing).
    Callers should expect empty results for events older than that window and must not treat an
    empty candlestick list as an error.
    """
    response = await _get_with_retry(
        client,
        f"{KALSHI_API_BASE}/series/{series_ticker}/markets/{market_ticker}/candlesticks",
        params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval_minutes},
    )
    return response.json().get("candlesticks", [])
