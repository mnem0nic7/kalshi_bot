# Add Missing Crypto Series Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add full support for ADA (KXADA15M), BCH (KXBCH15M), and BTCI (1h BTC) — the three series that require code changes; KXSOL, KXRIPPLE, and KXBTCC are already handled by existing dynamic discovery logic.

**Architecture:** Dynamic asset discovery already handles new tickers automatically via `CryptoMarketService.discover_series()`. The required changes are: (1) a BTCI→BTC alias in the parsing layer, (2) spot price feed entries for ADA and BCH, (3) config defaults and env vars so ADA and BCH participate in nightly model training and add-on position logic, and (4) overnight readiness prefix entries for the three genuinely new prefixes.

**Tech Stack:** Python, pytest, pydantic-settings. No DB migrations, no new files.

---

## File Map

| File | Change |
|------|--------|
| `src/kalshi_bot/crypto/parsing.py` | Add `"BTCI": "BTC"` to `ASSET_SYMBOL_ALIASES` |
| `src/kalshi_bot/integrations/crypto_spot.py` | Add ADA and BCH to `COINBASE_PRODUCT_IDS` and `COINGECKO_IDS` |
| `src/kalshi_bot/config.py` | Append ADA and BCH to `crypto_last_minute_passive_bid_by_asset` and `crypto_model_nightly_assets` defaults |
| `.env` | Append ADA and BCH to `CRYPTO_POSITION_ADD_ON_ASSETS` |
| `.env.example` | Append ADA and BCH to `CRYPTO_LAST_MINUTE_PASSIVE_BID_BY_ASSET`, `CRYPTO_MODEL_NIGHTLY_ASSETS`, and `CRYPTO_POSITION_ADD_ON_ASSETS` |
| `src/kalshi_bot/services/overnight_readiness.py` | Add `"KXADA15M"`, `"KXBCH15M"`, `"BTCI"` to `CRYPTO_MARKET_PREFIXES` (KXBTCC already covered by existing `"KXBTC"` prefix) |
| `src/kalshi_bot/cli.py` | Add `"ADA"`, `"BCH"` to `CRYPTO_LIVE_PATH_DEFAULT_ASSETS` |
| `tests/unit/test_crypto_parsing.py` | Add test for BTCI alias |
| `tests/unit/test_crypto_spot_features.py` | Add test for ADA/BCH dict entries |
| `tests/unit/test_overnight_readiness.py` | Add test for new CRYPTO_MARKET_PREFIXES |

---

### Task 1: BTCI alias — failing test

**Files:**
- Modify: `tests/unit/test_crypto_parsing.py`

- [ ] **Step 1: Add the failing test**

Append to `tests/unit/test_crypto_parsing.py`:

```python
def test_btci_ticker_resolves_to_btc_via_alias() -> None:
    assert asset_symbol_from_series({"ticker": "BTCI", "category": "Crypto", "frequency": "hourly"}) == "BTC"
```

- [ ] **Step 2: Run it to verify it fails**

```
pytest tests/unit/test_crypto_parsing.py::test_btci_ticker_resolves_to_btc_via_alias -v
```

Expected: **FAIL** — `assert 'BTCI' == 'BTC'`

---

### Task 2: BTCI alias — implementation

**Files:**
- Modify: `src/kalshi_bot/crypto/parsing.py:12-15`

- [ ] **Step 3: Add the alias**

Change `ASSET_SYMBOL_ALIASES` from:

```python
ASSET_SYMBOL_ALIASES = {
    "RIPPLE": "XRP",
    "SOLE": "SOL",
}
```

To:

```python
ASSET_SYMBOL_ALIASES = {
    "BTCI": "BTC",
    "RIPPLE": "XRP",
    "SOLE": "SOL",
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
pytest tests/unit/test_crypto_parsing.py -v
```

Expected: **PASS** (all tests including the new one)

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/crypto/parsing.py tests/unit/test_crypto_parsing.py
git commit -m "feat: add BTCI→BTC alias to resolve 1h BTC BTCI series"
```

---

### Task 3: ADA/BCH spot feeds — failing test

**Files:**
- Modify: `tests/unit/test_crypto_spot_features.py`

- [ ] **Step 6: Add the failing test**

At the end of `tests/unit/test_crypto_spot_features.py`, add:

```python
def test_ada_and_bch_have_spot_feed_entries() -> None:
    from kalshi_bot.integrations.crypto_spot import COINBASE_PRODUCT_IDS, COINGECKO_IDS

    for asset in ("ADA", "BCH"):
        assert asset in COINBASE_PRODUCT_IDS, f"{asset} missing from COINBASE_PRODUCT_IDS"
        assert asset in COINGECKO_IDS, f"{asset} missing from COINGECKO_IDS"
```

- [ ] **Step 7: Run it to verify it fails**

```
pytest tests/unit/test_crypto_spot_features.py::test_ada_and_bch_have_spot_feed_entries -v
```

Expected: **FAIL** — `ADA missing from COINBASE_PRODUCT_IDS`

---

### Task 4: ADA/BCH spot feeds — implementation

**Files:**
- Modify: `src/kalshi_bot/integrations/crypto_spot.py:19-37`

- [ ] **Step 8: Add ADA and BCH to both lookup tables**

Change the two dicts from:

```python
COINBASE_PRODUCT_IDS = {
    "BNB": "BNB-USD",
    "BTC": "BTC-USD",
    "DOGE": "DOGE-USD",
    "ETH": "ETH-USD",
    "HYPE": "HYPE-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}

COINGECKO_IDS = {
    "BNB": "binancecoin",
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "ETH": "ethereum",
    "HYPE": "hyperliquid",
    "SOL": "solana",
    "XRP": "ripple",
}
```

To:

```python
COINBASE_PRODUCT_IDS = {
    "ADA": "ADA-USD",
    "BCH": "BCH-USD",
    "BNB": "BNB-USD",
    "BTC": "BTC-USD",
    "DOGE": "DOGE-USD",
    "ETH": "ETH-USD",
    "HYPE": "HYPE-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}

COINGECKO_IDS = {
    "ADA": "cardano",
    "BCH": "bitcoin-cash",
    "BNB": "binancecoin",
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "ETH": "ethereum",
    "HYPE": "hyperliquid",
    "SOL": "solana",
    "XRP": "ripple",
}
```

- [ ] **Step 9: Run the test to verify it passes**

```
pytest tests/unit/test_crypto_spot_features.py -v
```

Expected: **PASS** (all tests)

- [ ] **Step 10: Commit**

```bash
git add src/kalshi_bot/integrations/crypto_spot.py tests/unit/test_crypto_spot_features.py
git commit -m "feat: add ADA and BCH to Coinbase/CoinGecko spot feed lookups"
```

---

### Task 5: Config defaults + env vars

No unit test — these are string defaults read at startup; behavior is covered by the existing integration test suite which creates Settings().

**Files:**
- Modify: `src/kalshi_bot/config.py:240,303`
- Modify: `.env:72`
- Modify: `.env.example:173,203,221`

- [ ] **Step 11: Update config.py default strings**

At line 240, change:

```python
    crypto_last_minute_passive_bid_by_asset: str = "BTC:0.55,ETH:0.54,XRP:0.54,SOL:0.63,DOGE:0.65,BNB:0.77,HYPE:0.84"
```

To:

```python
    crypto_last_minute_passive_bid_by_asset: str = "BTC:0.55,ETH:0.54,XRP:0.54,SOL:0.63,DOGE:0.65,BNB:0.77,HYPE:0.84,ADA:0.54,BCH:0.54"
```

At line 303, change:

```python
    crypto_model_nightly_assets: str = "BTC,ETH,SOL,XRP,BNB,DOGE,HYPE"
```

To:

```python
    crypto_model_nightly_assets: str = "BTC,ETH,SOL,XRP,BNB,DOGE,HYPE,ADA,BCH"
```

- [ ] **Step 12: Update .env**

At line 72, change:

```
CRYPTO_POSITION_ADD_ON_ASSETS=BNB,BTC,DOGE,ETH,HYPE,SOL,XRP
```

To:

```
CRYPTO_POSITION_ADD_ON_ASSETS=ADA,BCH,BNB,BTC,DOGE,ETH,HYPE,SOL,XRP
```

- [ ] **Step 13: Update .env.example**

At line 173, change:

```
CRYPTO_LAST_MINUTE_PASSIVE_BID_BY_ASSET=BTC:0.55,ETH:0.54,XRP:0.54,SOL:0.63,DOGE:0.65,BNB:0.77,HYPE:0.84
```

To:

```
CRYPTO_LAST_MINUTE_PASSIVE_BID_BY_ASSET=BTC:0.55,ETH:0.54,XRP:0.54,SOL:0.63,DOGE:0.65,BNB:0.77,HYPE:0.84,ADA:0.54,BCH:0.54
```

At line 203, change:

```
CRYPTO_POSITION_ADD_ON_ASSETS=live
```

To:

```
CRYPTO_POSITION_ADD_ON_ASSETS=ADA,BCH,BNB,BTC,DOGE,ETH,HYPE,SOL,XRP
```

At line 221, change:

```
CRYPTO_MODEL_NIGHTLY_ASSETS=BTC,ETH,SOL,XRP,BNB,DOGE,HYPE
```

To:

```
CRYPTO_MODEL_NIGHTLY_ASSETS=BTC,ETH,SOL,XRP,BNB,DOGE,HYPE,ADA,BCH
```

- [ ] **Step 14: Commit**

```bash
git add src/kalshi_bot/config.py .env .env.example
git commit -m "feat: add ADA and BCH to crypto asset config lists and env vars"
```

---

### Task 6: Overnight readiness prefixes — failing test

**Files:**
- Modify: `tests/unit/test_overnight_readiness.py`

- [ ] **Step 15: Add the failing test**

Add after the imports at the top of `tests/unit/test_overnight_readiness.py` (no new imports needed beyond what's already there — `CRYPTO_MARKET_PREFIXES` is a module-level name):

```python
from kalshi_bot.services.overnight_readiness import CRYPTO_MARKET_PREFIXES
```

Then at the end of the file, add:

```python
def test_new_series_tickers_recognized_as_crypto() -> None:
    for ticker in ("KXADA15M", "KXADA15M-26MAY01-B0.5", "KXBCH15M", "BTCI", "BTCI-26MAY01-T50000"):
        assert any(ticker.startswith(p) for p in CRYPTO_MARKET_PREFIXES), (
            f"{ticker!r} not recognized as crypto by CRYPTO_MARKET_PREFIXES"
        )
```

- [ ] **Step 16: Run it to verify it fails**

```
pytest tests/unit/test_overnight_readiness.py::test_new_series_tickers_recognized_as_crypto -v
```

Expected: **FAIL** — `'KXADA15M' not recognized as crypto by CRYPTO_MARKET_PREFIXES`

---

### Task 7: Overnight readiness prefixes — implementation

**Files:**
- Modify: `src/kalshi_bot/services/overnight_readiness.py:36-60`

- [ ] **Step 17: Add the three new prefixes**

Change `CRYPTO_MARKET_PREFIXES` from:

```python
CRYPTO_MARKET_PREFIXES = (
    "KXBTC15M",
    "KXETH15M",
    "KXSOL15M",
    "KXXRP15M",
    "KXDOGE15M",
    "KXBNB15M",
    "KXHYPE15M",
    "KXBTC",
    "KXBTCD",
    "KXETH",
    "KXETHD",
    "KXSOL",
    "KXSOLE",
    "KXSOLD",
    "KXXRP",
    "KXXRPD",
    "KXRIPPLE",
    "KXDOGE",
    "KXDOGED",
    "KXBNB",
    "KXBNBD",
    "KXHYPE",
    "KXHYPED",
)
```

To:

```python
CRYPTO_MARKET_PREFIXES = (
    "BTCI",
    "KXADA15M",
    "KXBCH15M",
    "KXBTC15M",
    "KXETH15M",
    "KXSOL15M",
    "KXXRP15M",
    "KXDOGE15M",
    "KXBNB15M",
    "KXHYPE15M",
    "KXBTC",
    "KXBTCD",
    "KXETH",
    "KXETHD",
    "KXSOL",
    "KXSOLE",
    "KXSOLD",
    "KXXRP",
    "KXXRPD",
    "KXRIPPLE",
    "KXDOGE",
    "KXDOGED",
    "KXBNB",
    "KXBNBD",
    "KXHYPE",
    "KXHYPED",
)
```

Note: `"KXBTCC"` does **not** need an entry — it already matches the existing `"KXBTC"` prefix.

- [ ] **Step 18: Run the test to verify it passes**

```
pytest tests/unit/test_overnight_readiness.py -v
```

Expected: **PASS** (all tests)

- [ ] **Step 19: Commit**

```bash
git add src/kalshi_bot/services/overnight_readiness.py tests/unit/test_overnight_readiness.py
git commit -m "feat: add KXADA15M, KXBCH15M, BTCI to CRYPTO_MARKET_PREFIXES"
```

---

### Task 8: CLI default assets

No TDD step — this is the static fallback list used when API discovery fails. The existing test at `tests/integration/test_cli_module_entrypoint.py:411` asserts `assets == list(cli_module.CRYPTO_LIVE_PATH_DEFAULT_ASSETS)`, which will automatically include ADA and BCH after the change.

**Files:**
- Modify: `src/kalshi_bot/cli.py:135`

- [ ] **Step 20: Add ADA and BCH to the fallback tuple**

Change line 135 from:

```python
CRYPTO_LIVE_PATH_DEFAULT_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE")
```

To:

```python
CRYPTO_LIVE_PATH_DEFAULT_ASSETS = ("ADA", "BCH", "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE")
```

- [ ] **Step 21: Run the full unit suite to catch any regressions**

```
pytest tests/unit/ -v
```

Expected: **PASS** (all unit tests)

- [ ] **Step 22: Commit**

```bash
git add src/kalshi_bot/cli.py
git commit -m "feat: add ADA and BCH to crypto live-path default asset fallback list"
```

---

### Task 9: Full test suite + integration verify

- [ ] **Step 23: Run the integration tests**

```
pytest tests/integration/ -v
```

Expected: **PASS** (all integration tests, including `test_daemon_service.py`)

- [ ] **Step 24: Quick smoke check — verify aliases resolve correctly**

```bash
python3 - <<'EOF'
import sys; sys.path.insert(0, "src")
from kalshi_bot.crypto.parsing import asset_symbol_from_series, ASSET_SYMBOL_ALIASES
from kalshi_bot.integrations.crypto_spot import COINBASE_PRODUCT_IDS, COINGECKO_IDS
from kalshi_bot.services.overnight_readiness import CRYPTO_MARKET_PREFIXES

print("Aliases:", ASSET_SYMBOL_ALIASES)
print("BTCI →", asset_symbol_from_series({"ticker": "BTCI"}))
print("ADA in Coinbase:", "ADA" in COINBASE_PRODUCT_IDS)
print("BCH in CoinGecko:", "BCH" in COINGECKO_IDS)
print("KXADA15M is crypto:", any("KXADA15M".startswith(p) for p in CRYPTO_MARKET_PREFIXES))
print("KXBCH15M is crypto:", any("KXBCH15M".startswith(p) for p in CRYPTO_MARKET_PREFIXES))
print("BTCI is crypto:   ", any("BTCI".startswith(p) for p in CRYPTO_MARKET_PREFIXES))
print("KXBTCC is crypto: ", any("KXBTCC".startswith(p) for p in CRYPTO_MARKET_PREFIXES))
EOF
```

Expected output:
```
Aliases: {'BTCI': 'BTC', 'RIPPLE': 'XRP', 'SOLE': 'SOL'}
BTCI → BTC
ADA in Coinbase: True
BCH in CoinGecko: True
KXADA15M is crypto: True
KXBCH15M is crypto: True
BTCI is crypto:    True
KXBTCC is crypto:  True
```
