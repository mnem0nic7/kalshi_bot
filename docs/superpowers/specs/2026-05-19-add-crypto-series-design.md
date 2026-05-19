# Design: Add Support for Missing Crypto Series

**Date:** 2026-05-19  
**Status:** Approved

## Overview

Six Kalshi crypto series appear on the live API but are not fully supported by the bot:

| Ticker    | Frequency | Asset | Gap |
|-----------|-----------|-------|-----|
| KXADA15M  | 15m       | ADA   | New asset (Cardano) — no spot feed, no config |
| KXBCH15M  | 15m       | BCH   | New asset (Bitcoin Cash) — no spot feed, no config |
| KXSOL     | 1h        | SOL   | Already handled by dynamic discovery + parsing; no code change needed |
| KXRIPPLE  | 1h        | XRP   | Already handled via ASSET_SYMBOL_ALIASES `{"RIPPLE": "XRP"}`; no code change needed |
| KXBTCC    | 1h        | BTC   | Kalshi API returns `asset_symbol=BTC` directly; no alias needed |
| BTCI      | 1h        | BTC   | Ticker-derived name "BTCI" has no alias → treat as BTC via new alias |

Net new work: add **ADA** and **BCH** as full assets, add **BTCI → BTC** alias.

## Architecture

The crypto trading stack uses **dynamic asset discovery** via `CryptoMarketService.discover_series()`, which calls the Kalshi API and filters by frequency. No ticker is hardcoded in the main trading path. Adding a new asset requires only:

1. A spot price feed entry (Coinbase + CoinGecko IDs)
2. Inclusion in config-driven asset lists (nightly model, passive bid, add-on positions)
3. An alias in `ASSET_SYMBOL_ALIASES` if the ticker-derived name doesn't match the canonical symbol

KXSOL, KXRIPPLE, and KXBTCC are already handled by dynamic discovery + existing alias/API-field logic. Only ADA, BCH, and the BTCI alias require changes.

## Files Changed

### 1. `src/kalshi_bot/crypto/parsing.py`
Add `"BTCI": "BTC"` to `ASSET_SYMBOL_ALIASES`. This is a pure alias — `_frequency_from_ticker` already derives "1h" from the ticker correctly, and the Kalshi API field `asset_symbol` will be consulted first; the alias is a fallback for when the field is missing or empty.

### 2. `src/kalshi_bot/integrations/crypto_spot.py`
Add ADA and BCH to both lookup tables:
- `COINBASE_PRODUCT_IDS`: `"ADA": "ADA-USD"`, `"BCH": "BCH-USD"`
- `COINGECKO_IDS`: `"ADA": "cardano"`, `"BCH": "bitcoin-cash"`

### 3. `src/kalshi_bot/config.py`
Two string fields appended with ADA and BCH:
- `crypto_last_minute_passive_bid_by_asset` → `...,ADA:0.54,BCH:0.54`
- `crypto_model_nightly_assets` → `...,ADA,BCH`

Bid price 0.54 is conservative (matches ETH/XRP) — empirical bucket gate (min 20 samples) will prevent live orders until data accumulates.

### 4. `.env`
Three env vars updated (same additions as config.py defaults):
- `CRYPTO_MODEL_NIGHTLY_ASSETS` → `...,ADA,BCH`
- `CRYPTO_POSITION_ADD_ON_ASSETS` → `...,ADA,BCH`
- `CRYPTO_LAST_MINUTE_PASSIVE_BID_BY_ASSET` → `...,ADA:0.54,BCH:0.54`

### 5. `.env.example`
Same additions as `.env` to keep the template in sync.

### 6. `src/kalshi_bot/services/overnight_readiness.py`
Add `"KXADA15M"`, `"KXBCH15M"`, `"KXBTCC"`, and `"BTCI"` to `CRYPTO_MARKET_PREFIXES`. KXSOL and KXRIPPLE are already present.

### 7. `src/kalshi_bot/cli.py`
Add `"ADA"` and `"BCH"` to `CRYPTO_LIVE_PATH_DEFAULT_ASSETS`.

## What Does NOT Change

- No DB schema changes — new series rows are inserted dynamically by the history ingestor
- No changes to `CryptoMarketService.discover_series()` — dynamic discovery already handles new tickers
- `leaderboard_mirror_analysis.py` already includes `KXADA` and `KXBCH` prefixes — no change needed
- KXSOL, KXRIPPLE, KXBTCC receive no code changes — existing logic already handles them correctly

## Safety Properties

- **Empirical bucket gate** (`CRYPTO_EMPIRICAL_BUCKET_GATE_ENABLED=true`, min 20 samples): ADA and BCH will not trade live until candle history is accumulated. This is automatic.
- **Shadow mode** (`APP_SHADOW_MODE=true`): no real orders will be placed until the operator explicitly disables shadow mode.
- **Nightly model job**: ADA and BCH are added to `CRYPTO_MODEL_NIGHTLY_ASSETS` so the model regeneration job will train models for them once sufficient data is available.
- The BTCI alias addition is purely defensive — if the Kalshi API returns `asset_symbol=BTC` directly (as confirmed during API testing), the alias is never reached.

## Testing

- Existing integration test `tests/integration/test_daemon_service.py` should continue to pass with no modifications.
- The `check_kalshi_series.py` diagnostic script can be re-run post-deploy to confirm new series resolve correctly.
- After a nightly candle ingest cycle, verify ADA and BCH candle rows appear in `crypto_market_snapshots`.
