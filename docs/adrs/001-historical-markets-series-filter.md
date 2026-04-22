# ADR 001 — Pass `series_ticker` to `list_historical_markets`

**Date:** 2026-04-22
**Status:** Accepted
**Author:** Grant Funk / Claude Sonnet 4.6

## Context

`HistoricalTrainingService._list_historical_markets` crawls Kalshi's
`/historical/markets` endpoint to build the training corpus for a given
`WeatherSeriesTemplate`. The method was calling the endpoint without
`series_ticker`, causing the API to return results globally — newest-first
across all series.

Kalshi's `/historical/markets` is a global feed sorted by close time
descending. With `historical_import_max_pages=25` and
`historical_import_page_size=500`, the crawler sees at most 12,500
markets before the page budget is exhausted. For Tier 1 series (KXHIGHAUS,
KXHIGHNY, etc.) whose history begins December 2024, this budget was consumed
entirely by newer markets from other series before reaching the relevant
date range.

**Effect:** Tier 1 series appeared to have zero historical data despite
~470 station-days of settled markets being available on the exchange. The
DB-fit layer of the per-station σ calibration (§4.2) depended on this data
and would have produced no rows.

**Discovery:** Empirical probe via `historical-import weather` showed the
demo-env API returning mostly KXHIGHT* series results (Feb–Apr 2026) for
what should have been a KXHIGHAUS-specific crawl.

## Decision

Pass `series_ticker=template.series_ticker` to every call of
`list_historical_markets` inside `_list_historical_markets`. The Kalshi API
accepts this as a query parameter and applies server-side filtering, so the
page budget is spent entirely on the target series.

The client-side `startswith(prefix)` check at line 3299 is retained as
defence-in-depth against unexpected API behaviour, but it is not a substitute
for server-side filtering because it cannot recover budget already spent on
foreign pages.

## Consequences

- Tier 1 series history (Dec 2024 – present, ~470 station-days) is now
  retrievable within the configured page budget.
- Each `_list_historical_markets` call now issues one `series_ticker`-scoped
  request per page rather than a global one — no additional API calls, same
  rate limit footprint.
- Tier 2 and Tier 3 series remain shallow (~40 and ~10–20 station-days
  respectively); the σ calibration sample thresholds (100 and 200) correctly
  defer to YAML anchors or global fallback for those stations until data
  accumulates.

## Tests

`tests/unit/test_historical_training_unit.py`:
- `test_list_historical_markets_passes_series_ticker` — asserts `series_ticker`
  is present in every API call kwargs.
- `test_list_historical_markets_no_cross_series_contamination` — asserts
  markets from a different series are excluded from the returned list even
  if the API (incorrectly) returns them.
