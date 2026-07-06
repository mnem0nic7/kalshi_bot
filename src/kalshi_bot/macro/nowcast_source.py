"""Cleveland Fed CPI/PCE nowcast ingestion.

**Verified data source (2026-07-01 feasibility probe, re-confirmed 2026-07-06 against the live
endpoint):** the primary endpoint is
``https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json``
(sibling ``nowcast_year.json`` carries the year-over-year series in the identical frame shape;
values are YoY % instead of MoM %). **UA-gated**: some clients get a 403 without a browser-like
``User-Agent`` header; this module always sends one explicitly (`config.macro_cpi_nowcast_user_agent`)
rather than relying on a client default. No auth / API key required.

**Structure (verified):** a JSON list of ~157 frames, one per target month from 2013-07 to the
current month. Each frame's ``categories[0].category`` is the daily x-axis of that month's
nowcast run-up (``MM/DD`` labels), interleaved with non-data marker entries such as ``CPI Feb`` /
``PCE Feb`` (calendar annotations for a *different* month's release, not this frame's own values —
excluded from the date/value alignment). ``dataset`` carries 8 series (``CPI Inflation``,
``Core CPI Inflation``, ``PCE Inflation``, ``Core PCE Inflation`` and their ``Actual ...``
counterparts); each series' ``data`` list is already filtered to the non-marker dates, so it
lines up index-for-index with the filtered date list. A value of ``"0.13"`` means 0.13
(percentage points), not a fraction.

**Point-in-time discipline (the crypto stale-spot lesson, `project_settlement_basis_finding`):**
every reading here is tagged with the ``as_of_date`` it was known on — the frame's x-axis *is*
the vintage archive. Callers must never use a reading whose ``as_of_date`` is after the backtest
date being evaluated; `latest_as_of` enforces this by construction (it only considers readings
with ``as_of_date <= as_of_date``).

**Data-gap caveat:** some months carry no ``Actual ...`` value yet (methodology notes for
missing-CPI months, e.g. a government-shutdown data gap). The parser tolerates this — a reading
is only emitted when a cell has a non-empty numeric value; a month with no actual simply has no
`is_actual=True` readings and `latest_as_of(..., actual=True)` returns `None` for it. It never
raises for this and never fabricates a stale/None value in its place.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Literal

import httpx

logger = logging.getLogger(__name__)

IndexKind = Literal["mom", "yoy"]
Series = Literal["cpi", "core_cpi", "pce", "core_pce"]

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

DEFAULT_URLS: dict[IndexKind, str] = {
    "mom": "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json",
    "yoy": "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_year.json",
}

_SERIES_NAME_MAP: dict[Series, str] = {
    "cpi": "CPI Inflation",
    "core_cpi": "Core CPI Inflation",
    "pce": "PCE Inflation",
    "core_pce": "Core PCE Inflation",
}
_NAME_TO_SERIES: dict[str, Series] = {v: k for k, v in _SERIES_NAME_MAP.items()}


@dataclass(frozen=True, slots=True)
class NowcastReading:
    """One point-in-time observation of the Cleveland Fed nowcast (or its eventual actual)."""

    as_of_date: date
    target_month: str  # "YYYY-MM"
    series: Series
    index_kind: IndexKind
    value_pct: float
    is_actual: bool
    source: str = "cleveland_fed"


class NowcastSourceError(RuntimeError):
    """Raised when no configured source could produce readings. Never fall back silently."""


class ClevelandFedNowcastSource:
    """Fetches + parses the Cleveland Fed CPI/PCE nowcast frame archive."""

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout_seconds: float = 30.0,
        urls: dict[IndexKind, str] | None = None,
    ) -> None:
        self._user_agent = user_agent
        self._timeout_seconds = timeout_seconds
        self._urls = urls or DEFAULT_URLS

    async def fetch(self, index_kind: IndexKind) -> list[NowcastReading]:
        url = self._urls[index_kind]
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(url, headers={"User-Agent": self._user_agent})
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise NowcastSourceError(f"cleveland_fed fetch failed for {index_kind}: {exc}") from exc
        return parse_frames(response.json(), index_kind)


def _parse_target_month(subcaption: str) -> str:
    """``'2024-6'`` -> ``'2024-06'``."""
    year_str, month_str = subcaption.split("-")
    return f"{int(year_str):04d}-{int(month_str):02d}"


def _is_marker_label(label: str) -> bool:
    return label.startswith("CPI ") or label.startswith("PCE ")


def _infer_dates(labels: Sequence[str], target_month: str) -> list[date]:
    """Assign a calendar year to each bare ``MM/DD`` label.

    Empirically (checked against all 157 real frames), a frame's non-marker window starts in
    its own target month in 156/157 cases; the sole exception is the very first (oldest, all-blank)
    frame in the archive. The window can run for several weeks past the release and cross a
    calendar-year boundary (e.g. a December target month's window runs into January) — detected
    by watching for the month number decreasing between consecutive labels.
    """
    target_year, _target_mon = (int(p) for p in target_month.split("-"))
    dates: list[date] = []
    year = target_year
    prev_month: int | None = None
    for label in labels:
        month_str, day_str = label.split("/")
        month, day = int(month_str), int(day_str)
        if prev_month is not None and month < prev_month:
            year += 1
        dates.append(date(year, month, day))
        prev_month = month
    return dates


def _series_for_dataset_name(name: str) -> tuple[Series | None, bool]:
    is_actual = name.startswith("Actual ")
    lookup_name = name[len("Actual ") :] if is_actual else name
    return _NAME_TO_SERIES.get(lookup_name), is_actual


def parse_frames(raw: object, index_kind: IndexKind) -> list[NowcastReading]:
    """Parse the full frame list (or a single frame dict) into point-in-time readings.

    Tolerant by design (per the spec's error-handling section): a frame with an unparseable
    ``subcaption``, mismatched category/value lengths, or blank cell values is skipped (or the
    cell is skipped) with a logged warning rather than raising — except that JSON structurally
    unrelated to this format (not a list/dict) is a caller bug, not a data gap.
    """
    frames = raw if isinstance(raw, list) else [raw]
    readings: list[NowcastReading] = []
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        chart = frame.get("chart") or {}
        subcaption = chart.get("subcaption")
        if not subcaption:
            continue
        try:
            target_month = _parse_target_month(subcaption)
        except (ValueError, AttributeError):
            logger.warning("macro.nowcast_source: unparseable subcaption %r; skipping frame", subcaption)
            continue

        categories = frame.get("categories") or []
        if not categories:
            continue
        labels = [c.get("label", "") for c in categories[0].get("category", [])]
        non_marker_labels = [lbl for lbl in labels if not _is_marker_label(lbl)]
        try:
            dates = _infer_dates(non_marker_labels, target_month)
        except (ValueError, IndexError):
            logger.warning(
                "macro.nowcast_source: unparseable date labels for target month %s; skipping frame",
                target_month,
            )
            continue

        for dataset in frame.get("dataset", []):
            name = dataset.get("seriesname", "")
            series, is_actual = _series_for_dataset_name(name)
            if series is None:
                continue
            values = dataset.get("data", [])
            if len(values) != len(dates):
                logger.warning(
                    "macro.nowcast_source: %s %s length mismatch (%d values vs %d dates); skipping series",
                    target_month,
                    name,
                    len(values),
                    len(dates),
                )
                continue
            for as_of, cell in zip(dates, values):
                raw_value = (cell or {}).get("value", "")
                if raw_value in ("", None):
                    continue
                try:
                    value_pct = float(raw_value)
                except (TypeError, ValueError):
                    continue
                readings.append(
                    NowcastReading(
                        as_of_date=as_of,
                        target_month=target_month,
                        series=series,
                        index_kind=index_kind,
                        value_pct=value_pct,
                        is_actual=is_actual,
                    )
                )
    return readings


def latest_as_of(
    readings: Iterable[NowcastReading],
    *,
    target_month: str,
    series: Series,
    index_kind: IndexKind,
    as_of_date: date,
    actual: bool = False,
) -> NowcastReading | None:
    """Point-in-time lookup: the latest reading known on or before ``as_of_date``.

    This is the lookahead guard: readings with ``as_of_date`` after the query date are never
    considered, so a backtest walking forward in time can never see the future.
    """
    candidates = [
        r
        for r in readings
        if r.target_month == target_month
        and r.series == series
        and r.index_kind == index_kind
        and r.is_actual == actual
        and r.as_of_date <= as_of_date
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda r: r.as_of_date)
