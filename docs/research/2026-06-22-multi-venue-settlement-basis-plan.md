# Multi-venue settlement-basis alignment — long-term edge plan (2026-06-22)

**Status:** STARTED 2026-06-22 (operator authorized "start backfill … long-term plan and goal").
**Owner goal it serves:** "get the rest of the market live ready" — but the research below shows
this is a multi-week DATA effort, not a config change, and BTC/HYPE are likely structurally
unfixable regardless.

## Why this plan exists (the root cause)

A full sweep + research (see [[project_settlement_basis_finding]] memory, and
`docs/research/2026-06-14-model-selection-diagnosis-and-plan.md`) established that at 15m the
market mid is near-efficient and **no model is both profitable and well-calibrated**. The
mechanism, quantified on our own data:

- **Kalshi settles crypto on the CF Benchmarks Real-Time Index** (BTC = BRTI/BRRNY; a
  *multi-exchange* aggregate), **averaged over a 60-second window at expiry**
  (sources: help.kalshi.com/en/articles/13823838-crypto-markets, cfbenchmarks.com,
  kalshibacktest.com/resources/kalshi-settlement-mechanics).
- **Our dominant feature** `spot_moneyness = close − target_price` (`crypto/services.py:10408`)
  uses **single-venue Coinbase, instantaneous close** — a double mismatch (venue + timing).
- On `crypto_settlement_benchmark_windows`, `(close≥strike)` and `(twap≥strike)` **disagree on
  13–19% of markets** (BNB 19.0, HYPE 16.1, ETH 15.1, XRP 14.3, SOL 14.0, BTC 13.6, DOGE 13.3),
  and Kalshi is on the TWAP-of-index side. So our feature is systematically noisier than the
  ground-truth label near the strike → the near-money "coin-flip zone" that makes mids look
  efficient and forces profitable models to be miscalibrated.

**The fix:** align our feature/fair-value spot basis to Kalshi's actual settlement basis — a
60s TWAP of a multi-venue aggregate approximating CF BRTI/BRRNY.

## The blocker this plan removes

`crypto_spot_ohlc` today: **Coinbase** 513k rows (full year) but **Kraken only ~2.8k rows /
18 days** (since 2026-06-04), no other venues. We cannot reconstruct a multi-exchange index
historically. This plan accumulates the data going forward.

## Stages

### Stage 0 — DONE 2026-06-22: confirm mechanism already multi-venue
`crypto_spot_service.backfill()` already pulls Coinbase + Kraken (+CoinGecko fallback;
`_collect_kraken_rows`), and the daemon collects forward each cycle. So forward multi-venue
accumulation is ALREADY running — it just started recently and is sparse. Ran a bounded
`crypto-spot backfill --frequency 15m --days 30` to maximize current Kraken depth (Kraken OHLC
API caps ~720 candles/request, so this is light — not a host stressor).

### Stage 1 — Accumulate + widen venues (weeks; the long pole)
- **Let forward collection run** so Kraken (and any added venues) densify to a usable history
  (target: ≥30–60 days of dense multi-venue coverage before training on it).
- **Add more CF-constituent venues** (code): Bitstamp, Gemini, LMAX, itBit/Paxos are CME CF
  constituents. Each is a new client in `integrations/crypto_spot.py` + wiring into `backfill()`
  / `collect_current()` like the existing Kraken path (`_collect_kraken_rows`). More venues →
  better BRTI approximation. **Respect host disk-I/O fragility** (see [[project_host_oom_guardrails]],
  [[project_live_trading_state]] compose-hang root cause): stagger collection, never run
  concurrently with a recreate/retrain.
  - **DONE 2026-06-22 — Gemini venue shipped** (`GeminiSpotClient`, `GEMINI_PAIRS`,
    `_parse_gemini_ohlc_payload`, `_gemini_time_frame` in `integrations/crypto_spot.py`;
    `_gemini_client`/`_collect_gemini_rows` wired into `backfill()`+`collect_current()` in
    `crypto/services.py`; `crypto_spot_gemini_enabled=True` in config; TDD tests in
    `tests/unit/test_crypto_spot_gemini.py`). Gemini is a US-accessible CF constituent → adds a
    **3rd venue for BTC/ETH/SOL/XRP/DOGE/ADA/BCH**. Like Kraken, **does NOT list BNB or HYPE**
    (GEMINI_PAIRS None) — so BNB still has no 2nd venue, HYPE still none. Takes effect on next
    app/daemon deploy (running containers need the new image). Accumulates forward; Gemini v2
    candles return a fixed recent window (no deep historical backfill).
  - Still open: Bitstamp/LMAX clients; a venue that lists BNB (only Binance does — not
    US-accessible → BNB likely stays single-venue).

### Stage 2 — Build the basis-aligned feature (code, TDD)
- Compute a **multi-venue volume/recency-weighted aggregate spot** (reuse `mm/data_spine.py`
  consolidation logic) and a **60s TWAP at the settlement window**.
- Add `spot_moneyness_index_twap` (and a target-distance/σ variant) **alongside** the existing
  close-based features — additive, so the trainer's feature selection + replay gate judge it;
  no risk to the currently-live DOGE/BNB.
- Re-fit the analytic vol fair value's σ to the **variance of the 60s-TWAP**, not instantaneous
  spot (lower terminal variance near expiry).

### Stage 3 — Retrain + let the gate decide
- Continuous trainer re-selects per asset; the replay gate (OOS profit + coverage) remains the
  arbiter. Expect the lever to help **illiquid alts** (DOGE/XRP/SOL/ETH) where the Kalshi mid
  lags the index — NOT BTC (its mid is already efficient on the index it settles to), and
  probably not HYPE (may lack a clean CF index).

## Honest expectations
- This is **weeks** to a usable signal, with **uncertain payoff**.
- **BTC stays unbeatable** (mid efficient on the settlement index). **HYPE** likely unfixable.
- Realistic ceiling: turn the oscillating alts (ETH/SOL/XRP) into *consistently* gate-passing,
  maybe +1–2 reliably-live assets. NOT "7/7".
- The safety frontier is unchanged: only profitable-OOS models deploy; we never deploy losers.

## Tracking
- Re-check venue depth: `SELECT provider, asset_symbol, count(*), min/max(start_ts) FROM
  crypto_spot_ohlc GROUP BY 1,2`.
- Re-run the basis-flip metric (light, safe): `crypto_settlement_benchmark_windows`,
  `avg((close≥strike)<>(twap≥strike))` per asset.
- Milestone to start Stage 2: ≥30 days of dense ≥2-venue coverage on the active alts.

## ⭐ STAGE 2 PLUMBING BUILT 2026-06-23 (operator "build now") — feature lands, validation gated on data
Built the cross-venue basis FEATURE (not just data accrual). Three model features now computed per
decision from the PRE-dedup multi-venue spot rows and registered in the feature schema (`crypto-rich-v12`):
- `spot_cross_venue_basis_pct` — signed (Coinbase − cross-venue mean)/mean ≈ our distance from the
  multi-venue index Kalshi settles to.
- `spot_cross_venue_spread_pct` — (max−min)/mean venue-disagreement magnitude.
- `spot_cross_venue_count` — # venues observed that period (1–3).
Key correctness point: the existing provider dedup (`_dedup_spot_rows_by_provider_preference`, keeps one
venue/period for momentum-feature safety) DISCARDS the cross-venue signal, so the basis is computed BEFORE
dedup (`_crypto_basis_index` in `_prepare_spot_context_series`, bisected per decision via
`_crypto_basis_for_decision`). Also added **gemini** to `CRYPTO_SPOT_PROVIDER_PREFERENCE` (was missing → its
rows were dropped). Helpers + extractor in `crypto/services.py`; tests `tests/unit/test_crypto_cross_venue_basis.py`.
**NOT deployed / NOT retrained** — schema bump to v12 triggers a full feature rebuild on next train, so deploy
via blue-green when ready; validation still gated on ≥30d dense ≥2-venue coverage (Stage 3). Old feature rows
(pre-multi-venue) default basis→0 safely. This removes the "unbuilt middle": when the data matures, retrain
immediately consumes the basis instead of starting feature work then.

### v12 DEPLOYED then v13 GRANULARITY FIX 2026-06-23
v12 deployed (active=blue, trainer chunked-rebuilt, 19.6k+ rows). Then "are we backfilling?" investigation
found the basis was near-empty: backfill loop is OFF and Kraken/Gemini can't be deep-backfilled (shallow APIs)
— overlap only grows forward. Worse, `collect_current` (15s) stores **Coinbase as instantaneous ticks
(interval=0)** but **Kraken/Gemini as 15-min candles (interval=900)**, so they never shared a basis period →
BTC 24h: 95% of periods single-venue → basis ≈ constant 0. **v13 fix:** `_crypto_basis_index` now uses
CANDLE rows only (interval>0), keyed by (epoch, interval), so venues align on the 15-min boundary (which is
also the 15m settlement reference). ~800 multi-venue periods/day/asset become usable. Schema bumped v12→v13.
NOT yet redeployed (v13 rebuild operator-gated). Optional further density: collect Kraken/Gemini TICKER at 15s
to align with Coinbase ticks (denser basis) — not yet built. Tests: tests/unit/test_crypto_cross_venue_basis.py.
