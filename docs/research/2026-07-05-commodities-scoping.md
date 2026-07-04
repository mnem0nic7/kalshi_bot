# Commodities vertical scoping spike (2026-07-05)

**Status:** Research only. Measured liquidity/cadence/spread for every Kalshi series in the
Commodities category from the public API (no auth, host venv). **No implementation is authorized
by this document** — if a candidate below is pursued further, that is its own scoped design task.

**Script:** `scripts/commodities_scope.py` (public `GET /series?category=Commodities` +
`GET /markets?series_ticker=...&status=open|settled`, unauthenticated `api.elections.kalshi.com`).

## Method and adaptations made while running it

The task brief's script was ~95% runnable as written; two things needed adapting on contact with
the real API, both minimal and confined to the script:

1. **Field names.** The brief assumed bare `volume` / `open_interest` fields on market objects.
   The live API does not expose those — it returns `volume_fp` / `open_interest_fp` (decimal
   strings; `_fp` = fixed-point). `yes_bid_dollars` / `yes_ask_dollars` matched the brief as
   written. Fixed in the script with a comment at the call site.
2. **Aggressive rate limiting.** The unauthenticated endpoint 429'd on the large majority of
   requests during the first full pass at 0.3s/request (only ~15 of 116 series returned data
   without adaptation). Added exponential backoff retry (`get()`, up to 6 attempts, 2s→64s) and
   raised the inter-series pause to 1.0s. Second pass completed for 112/116 series before a
   600s harness timeout; the remaining 4 (`KXNATGASD`, `KXH200Q`, `KXAAAGASMINTX`, `KXWTIEU`) were
   fetched individually with the same retry logic and merged in. Final dataset: all 116 series.
3. A tangential taxonomy inconsistency, noted but not corrected: `GET /series/{ticker}` reports
   `category: "Economics"` for the AAA gas series (`KXAAAGASD`, `KXAAAGASW`) even though they
   appear in the `category=Commodities` series listing. Kalshi's own category filter and per-series
   category field disagree for at least this case; doesn't affect the analysis below.

**Metric caveats (read before trusting the ranking):**
- `settles/wk (est)` = `settled_recent_count / span_days × 7` over the last ≤200 settled markets
  returned. For laddered daily products (many strike markets settling the same day, mirroring the
  crypto 15m/1h bucket-ladder pattern) this counts *strike-settlements*, not distinct pricing
  events — e.g. natural gas's 700/wk reflects ~100 strikes settling per trading day, not 100
  independent daily prints. Treat it as a settlement-*volume* intensity proxy, not literal cadence;
  the series' own `frequency` field (daily/weekly/monthly/annual) is the ground truth for cadence.
- `total_volume_open` / `total_oi_open` are **sums across every currently open market (strike) in
  the series**, not a single instrument's volume. A series with more open strikes naturally sums
  higher even at equal per-strike liquidity. Used because the brief asked for volume as the primary
  ranking axis; the per-market thinness this can hide is called out explicitly for copper and nat
  gas below.
- 50 of 116 series returned `open_markets: 0` (delisted/dormant) at scan time; those are excluded
  from the ranked table (they cannot be traded today) but are listed under the ags/metals verdict
  below because that null result *is* the finding for that category.

## Ranked table (top 25 of 66 series with open markets, by settles/wk × volume, spread as tiebreak)

| Rank | Series | Title | Freq | Open mkts | Settled (window) | Settle span | settles/wk (est) | Median spread ($) | Σ volume (open) | Σ OI (open) |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | KXWTI | WTI oil on day | daily | 75 | 200 | 06-12..07-02 | 70.0 | 0.04 | 519,971 | 248,616 |
| 2 | KXAAAGASW | US gas price up (weekly) | weekly | 18 | 200 | 05-11..06-29 | 28.6 | 0.01 | 359,828 | 254,552 |
| 3 | KXAAAGASD | US gas price up (daily) | daily | 17 | 200 | 06-23..07-04 | 127.3 | 0.02 | 50,658 | 41,251 |
| 4 | KXNATGASD | Natural Gas Daily | daily | 100 | 200 | 06-30..07-02 | 700.0 | 0.30 | 5,588 | 4,063 |
| 5 | KXGOLDD | Gold Daily | daily | 40 | 200 | 06-25..07-02 | 200.0 | 0.03 | 16,201 | 13,520 |
| 6 | KXAAAGASMAX | US highest gas price yearly | annual | 13 | 2 | 04-30..05-04 | 3.5 | 0.01 | 903,931 | 514,571 |
| 7 | KXSILVERD | Silver Daily | daily | 40 | 200 | 06-24..07-02 | 175.0 | 0.02 | 11,549 | 9,631 |
| 8 | KXB200WS | B200 Weekly (GPU pricing, non-commodity-fuel) | weekly | 45 | 18 | 06-19..06-26 | 18.0 | 0.01 | 74,876 | 30,822 |
| 9 | **KXBRENTD** | **Brent Oil Daily** | daily | 20 | 200 | 06-17..07-02 | 93.3 | **0.01** | 11,944 | 9,096 |
| 10-11 | KXH200WS, KXRTX5090WS | (GPU pricing, non-commodity-fuel) | weekly | — | — | — | — | 0.01 | — | — |
| 12 | KXCOPPERD | Daily Copper | daily | 40 | 200 | 06-25..07-02 | 200.0 | **0.30** | 2,661 | 2,564 |
| 13 | KXAAAGASM | US gas price (monthly) | monthly | 42 | 115 | 04-30..06-30 | 13.2 | 0.01 | 19,536 | 15,987 |
| 14 | KXNATGASW | Natural Gas Weekly | weekly | 40 | 200 | 06-05..07-02 | 51.9 | 0.02 | 4,039 | 3,938 |
| 18 | KXWTIW | WTI oil weekly range | weekly | 15 | 135 | 05-01..06-26 | 16.9 | 0.01 | 5,878 | 3,602 |
| 22 | KXBRENTMON | Brent Monthly | monthly | 20 | 60 | 04-30..06-30 | 6.9 | 0.01 | 7,404 | 3,687 |
| — | KXCOPPERMON | Copper Monthly | monthly | 40 | 120 | 04-30..06-30 | 13.8 | 0.35 | 86 | 86 |

GPU-pricing series (H100/H200/B200/RTX/A100/GPT — priced under Kalshi's "Commodities" category
filter alongside oil/metals/ags) are out of scope for this spike (operator's ask was Brent, copper,
natgas, gas prices, ags) and are left in the table only to show they don't distort the ranking; not
otherwise assessed.

Gold Daily and Silver Daily are similarly excluded from the top-5 deep-dive — the operator's ask was
oil/gas (and ags/industrial metals); precious metals are a separate vertical and would need their own
scoping pass before any verdict.

**Note on `KXNGASW` vs `KXNATGASD`/`KXNATGASW`:** the brief's a-priori guess `KXNGASW` ("Natural gas
price max and min weekly") is a *dead* legacy series — 0 open markets, 0 settled markets ever
observed. The live, actively-listed nat-gas products are `KXNATGASD` (daily) and `KXNATGASW`
(weekly); ranked above. Ticker-guessing from the brief was wrong here and worth flagging as a
general lesson: verify tickers against the live series list before assuming a naming pattern.

## Ags and base metals: fully delisted, zero exceptions

Every agricultural and base-metal series returned by the Commodities category filter has **0 open
markets today** (2026-07-04) — wheat, corn, soybeans, cocoa, coffee, sugar, live cattle, nickel,
steel, lithium, cobalt (`KXWHEATW/MON`, `KXCORNW/MON`, `KXSOYBEANW/MON`, `KXCOCOAW/MON`,
`KXCOFFEEW/MON`, `KXSUGARW/MON`, `KXLCATTLEW/MON`, `KXNICKELW/MON`, `KXSTEELW/MON`,
`KXLITHIUMW/MON`, `KXCOBALTW/MON` — 22 series checked). Their settled-market history is not a
gradual wind-down: every one of them settled its *entire* observed history in a single batch on
2026-04-30 or 2026-05-01 (e.g. `KXWHEATW`: 34 markets, all closing 2026-05-01; `KXCOCOAMON`: 24
markets, all closing 2026-04-30), then never listed another market. That pattern is Kalshi
discontinuing the whole ags/base-metals product line around late April/early May 2026, not
seasonal thinness. There is nothing to measure liquidity *of* — no open book, no forward listing to
project into. This is a listing-availability finding, not a liquidity finding.

## Settlement source (top 5, verified via `GET /series/{ticker}`)

| Series | Settlement source (API `settlement_sources`) | Underlying instrument (from `rules_primary`/`custom_strike`) |
|---|---|---|
| KXBRENTD | **Pyth** (`pythdata.app`) | 1-minute candle close of the front-month Brent contract (e.g. `BRENTU6`) at 5:00pm ET, rolling 5 business days before expiry |
| KXWTI | **ICE** (`theice.com/products/213/WTI-Crude-Futures`) | ICE's own official *daily settlement price* for the front-month WTI contract, rolling 2 business days before expiry |
| KXCOPPERD | **Pyth** (`pythdata.app`) | 1-minute candle close of the front-month COMEX copper contract (e.g. `CCN6`) at 5:00pm ET |
| KXNATGASD | **Pyth** (`pythdata.app`) | 1-minute candle close of the front-month NYMEX Henry Hub natural gas contract (e.g. `NGDQ6`) at 5:00pm ET |
| KXAAAGASD / KXAAAGASW | **AAA** (`gasprices.aaa.com`) | AAA's published U.S. national average regular-gas retail price for the given calendar day |

This is a materially useful finding beyond what the brief asked for: three of the four
oil/metals/gas dailies (Brent, copper, nat gas) settle to **Pyth**, a third-party price oracle, not
directly to the raw exchange tape. Only WTI settles to ICE's own administratively-published daily
print.

## Fresh-underlying-feed assessment (top 5)

- **Brent (KXBRENTD) — Pyth.** Pyth publishes commodity price feeds (confirmed: WTI, Brent, natural
  gas, copper, corn, soybeans, wheat) via the public **Hermes** API, updated continuously
  (sub-second aggregation across venues), and this is the *same* price series Kalshi settles
  against — i.e. the feed and the settlement source are identical, structurally the same situation
  as crypto's spot-vs-`floor_strike` mechanism already built in `crypto/`. One near-term caveat:
  Pyth's public Hermes instance moves from open access to **required authentication on 2026-07-31**
  (per Pyth's developer docs, confirmed via web search 2026-07-04); a free/low-cost signup appears
  to be the mechanism but exact terms (free tier vs. paid "Pyth Pro") were not confirmed from public
  docs alone and should be verified with Pyth directly before any build commits to it.
- **WTI (KXWTI) — ICE.** ICE publishes one official daily settlement print per contract, computed
  administratively after close — there is **no continuously observable feed converging toward
  that number intraday** the way Pyth's 1-minute candle is observable up to the settlement instant.
  A live-tracking edge (the crypto pattern) doesn't transfer; a discrete "beat everyone to the
  published print" edge would need a paid low-latency ICE data feed or fast scraping of ICE's
  release, both unverified as free/cheap options here.
- **Copper (KXCOPPERD) — Pyth/COMEX.** Same Pyth mechanism as Brent (fresh feed exists), but see
  the liquidity verdict below — the feed being fresh doesn't help if the market itself won't fill
  at a workable spread.
- **Nat gas (KXNATGASD) — Pyth/Henry Hub.** Same Pyth mechanism as Brent; same liquidity caveat as
  copper.
- **AAA gas (KXAAAGASD/W) — AAA, daily-published only, as flagged in the task brief.** No intraday
  feed exists or would help — AAA publishes one national-average number per day, freely, at
  `gasprices.aaa.com`. Any edge here is a **nowcast of tomorrow's AAA print from wholesale/futures
  gasoline inputs** (RBOB futures, EIA weekly data, etc.), structurally the same shape as the
  in-flight CPI nowcast project (predict a slow official index ahead of its publication), not a
  live-tracking arb. That distinction matters for scoping a build — it would reuse the CPI
  nowcast pattern's shape, not the crypto pattern's.

## Verdict per series

| Series | Verdict | Reason |
|---|---|---|
| **KXBRENTD** (Brent Oil Daily) | **GO-candidate** | Tightest spread of any oil/metals/gas daily (1¢), real volume (~12k contracts, ~9k OI open), live cadence, and a verified fresh feed (Pyth) that *is* the settlement source — structurally reusable against the existing crypto spot-tracking/analytic-vol-fair-value architecture. |
| KXWTI (WTI oil on day) | NO (for now) | Highest raw volume/cadence score in the whole scan, but settles to ICE's single opaque daily print with no continuously observable free feed converging toward it — the crypto-style live-tracking approach doesn't transfer, and the spread (4¢) is 4x Brent's. Revisit only if a cheap ICE settlement feed is identified. |
| KXCOPPERD (Daily Copper) | NO | Fresh Pyth feed exists (COMEX front-month), but median spread ≈30¢ on effectively a $1-wide strike range — the *market*, not the feed, is the blocker; that's an order-of-magnitude wider than Brent despite 2x the open strikes, signaling near-zero organic two-sided flow. |
| KXNATGASD (Natural Gas Daily) | NO | Same story as copper: fresh Pyth/Henry Hub feed, but ≈30¢ spread despite having the *most* settlement volume of any series scanned (dense 100-strike ladder) — cadence and feed freshness don't compensate for a market nobody is quoting tightly. |
| **KXAAAGASD / KXAAAGASW** (AAA gas price) | **GO-candidate (different shape)** | Tightest spreads in the scan (1-2¢) with meaningful volume, and a free daily feed — but this is a next-day-nowcast opportunity, not a live-tracking one; recommend scoping only alongside/after the CPI nowcast infrastructure, not as a standalone build. |
| All ags (wheat, corn, soy, cocoa, coffee, sugar, cattle) | **NO** | Fully delisted — 0 open markets, entire history closed in one batch ~2026-04-30/05-01. Nothing to build against; not a spread/cadence problem. |
| All base metals except copper (nickel, steel, lithium, cobalt) | **NO** | Same as ags — fully delisted. |
| Brent/WTI/copper/nat-gas *monthly* variants (KXBRENTMON, KXCOPPERMON, KXWTIMONTHLY, KXNATGASMON) | NO | Thin relative to their daily counterparts (e.g. `KXCOPPERMON` sums to only 86 total volume/OI across 40 open strikes — essentially unquoted) and too slow-cadence to matter next to the daily products above. |

## Recommendation

Of ~40 commodity-labeled products actually checked in depth (and 116 series scanned overall), the
honest read is **one clear GO-candidate (Brent daily) plus one differently-shaped secondary
candidate (AAA gas nowcast), and NO everywhere else** — including the operator's other two named
targets, copper and nat gas, which both fail on market-level spread despite having a usable feed,
and WTI, which fails on feed access despite having the best raw volume. The ags/base-metals half of
the "commodities" universe is not a liquidity question at all — it's delisted and there is nothing
there today. If this is pursued further, the recommended next step is a narrowly scoped design for
**Brent daily only**, explicitly reusing the crypto pipeline's spot-tracking/analytic-vol-fair-value
shape against the Pyth feed (same settlement-source-is-the-feed structure that made crypto
tractable), with AAA-gas nowcasting evaluated opportunistically alongside the CPI nowcast track
rather than as its own vertical. **No implementation is authorized by this document** — any of the
above would be its own scoped design/build task with its own review.
