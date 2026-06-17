# Kalshi 15-Minute Crypto — Statistical Market-Making Plan

A committed implementation spec. One strategy, built and validated in stages, with a hard
go/no-go gate before any capital is risked.

---

## 1. Thesis

Build a single strategy: a statistical market-maker that harvests transient mispricings in
the 15-minute crypto binaries.

Over a 15-minute horizon, BTC (and ETH/SOL) behave as a near-driftless martingale; the
weak-form-efficiency evidence says direction is not predictable from price history. The
contract price is therefore already a probability estimate. The only edge with a sound
mechanism is to estimate that probability more accurately than the market, in real time, and
provide liquidity when the market price diverges from it.

**Out of scope (predicted to lose to fees):** directional "fade the panic," momentum,
threshold rules — anything that bets on direction rather than on mispricing.

**Primary asset:** BTC (KXBTC15M, deepest book). ETH/SOL added later and modeled
independently (different settlement basis and liquidity).

---

## 2. The edge mechanism

An "up" contract pays $1 if the close `S_T > K`. Assuming zero drift over the remaining time
`τ`, the log-return `r = ln(S_T / S_t)` is approximately `N(0, σ²τ)`. The fair probability is:

```
fair_up = Φ( ln(S_t / K) / (σ · √τ) )
```

- `S_t` — current consolidated spot (your estimate of true BTC price right now)
- `K` — floor strike (the banded value the market opened against; use the market's own
  `floor_strike` field, never a generic spot index)
- `σ · √τ` — standard deviation of the log-return over the **remaining** window
- `Φ` — standard normal CDF (replace with Student-t / empirical to capture heavy tails)

The edge is the gap between `fair_up` and the market's quoted price. It is fed by two
independent sources: (1) a cleaner, fresher spot read than a momentarily stale Kalshi quote
reflects, and (2) a better `σ` estimate, especially as `τ → 0` near the close. You are not
racing co-located firms on latency — the edge is data quality and calibration.

---

## 3. Architecture

Five components, event-driven:

1. **Data spine** — multi-venue spot + Kalshi WS, normalized, timestamped, logged.
2. **Fair-value model** — calibrated `P(close > strike)` per market, per tick.
3. **Order-flow signal** — classifies divergences as flow-driven (fade) vs. information-driven (stand down).
4. **Execution** — maker-first order placement/cancellation via REST.
5. **Risk** — sizing, caps, kill switch.

Ingest over WebSocket; place orders over REST (Kalshi's WS is read-only). Develop entirely on
the demo environment first.

---

## 4. Component specs

### 4.1 Data spine
- **Spot:** consolidated mid from ≥2 venues (e.g., Coinbase + one other), volume/recency-weighted,
  with outlier rejection and per-venue staleness guards. Log per-second (or per-tick).
- **Kalshi:** subscribe to `orderbook_delta`, `ticker`, `trade` for the active 15-min markets.
  Maintain live book state from the deltas with gap detection.
- **Per market, record:** `floor_strike`, `expiration_value` (post-settlement), full book
  snapshots, synthetic mid, best bid/ask + sizes, signed trades, seconds-to-close.
- **Storage:** a time-series store (Parquet files or Postgres/Timescale). This dataset is the
  prerequisite for everything downstream. Log for several weeks spanning calm and volatile
  regimes before trusting any model.

### 4.2 Fair-value model
- **Volatility estimator:** standard deviation of log-returns over the *remaining* horizon.
  Start with realized vol from 1–5s returns over a trailing window, scaled to `τ_remaining`,
  with an intraday-seasonality adjustment (crypto vol clusters by time-of-day and around macro
  events). A short EWMA or HAR-style blend is a reasonable v1.
- **Probability:** `fair_up = Φ(ln(S_t/K)/(σ̂√τ))`; use a Student-t or empirical return
  distribution for tails. Handle the terminal regime explicitly — as `τ → 0` the probability
  snaps toward 0/1 and the closed form becomes unstable.
- **Calibration (mandatory):** do not trust the closed form. Bucket logged observations by
  `z = ln(S/K)/(σ̂√τ)` and seconds-to-close; compare predicted vs. realized settlement
  frequency; apply isotonic or Platt calibration. Track Brier score and a reliability curve.
  The model is only usable once it is calibrated against real settlements. (No LLM required —
  this is a classical, calibrated probabilistic model.)

### 4.3 Order-flow signal
- Compute order-book imbalance (size-weighted) and signed trade flow from the WS feed over
  short windows.
- When the market price diverges from `fair_up`, gate the trade on **both**: (a) the divergence
  is associated with an order-flow imbalance you expect to revert (aggressive flow tends to
  reverse at short horizons), and (b) consolidated spot does **not** corroborate the contract's
  move. If spot genuinely moved, the contract is correct and you would be the adverse-selected
  counterparty — stand down and update fair value instead.

### 4.4 Execution (maker-first)
- Rest limit orders at `fair_up ± edge_buffer`; cancel/replace as spot and `τ` evolve. Cross the
  spread (take) only when the divergence clearly exceeds the taker fee — rare.
- **Entry (up side, symmetric for down):** post a buy at price `p` when
  `fair_up − p > round_trip_cost + buffer` AND the 4.3 flow/spot conditions hold.
- Avoid the ~45–55¢ band (peak fee, maximum uncertainty) unless calibrated confidence is high.
  Skewed prices are cheaper on fees but risk more to make less, so scale the required edge
  accordingly.
- **Terminal handling:** in the final ~60–90s the book thins and jump-to-settlement risk rises.
  Flatten or stop quoting unless the model explicitly prices the terminal regime — this is where
  naive bots get picked off.

### 4.5 Sizing & risk
- **Fractional Kelly** (start at ¼-Kelly) on the *measured out-of-sample* per-trade edge.
  Binary Kelly on a YES bought at `price`: with true win probability `P` and net odds
  `b = (1 − price)/price`, the full-Kelly bankroll fraction is
  `f* = (b·P − (1 − P)) / b`; deploy `0.25 · f*`.
- Hard per-window cap, per-market cap, and a daily loss limit with an automatic kill switch.
  Respect Kalshi's ~$25k/market position limit and the API rate limits.
- **Model ruin explicitly:** high frequency × thin edge × heavy tails ⇒ real drawdown risk.
  Size for survival, not for the median path.

---

## 5. Validation protocol (the gate)

The most important section. Most apparent edge in naive backtests is fill fantasy plus ignored
adverse selection.

- **Realistic fills:** replay the logged order book. You fill a resting order only if its price
  was at/through the touch and you would be ahead in queue. No "assume I got filled at mid."
- **Real fees:** pull the *current* crypto fee multiplier from the live fee schedule / market
  metadata (crypto is a premium category — do not assume the 7% standard or a zero maker fee).
  Kalshi pays no maker rebate; do not model one.
- **Adverse selection:** condition fill probability on the subsequent price path. Your maker
  fills are disproportionately the trades where you were wrong; a backtest that ignores this
  will look profitable and lose live.
- **Out-of-sample only:** walk-forward. Report after-cost Sharpe, hit-rate vs. the fee-inflated
  breakeven (>~51.5% at 50¢), max drawdown, and edge decay across the window.
- **Kill criterion:** if the strategy does not show stable, positive after-cost expectancy
  out-of-sample across multiple regimes, do not deploy. Expect this to be the outcome for most
  parameterizations — the market is efficient and competitive. Finding that out cheaply is the
  deliverable.

---

## 6. Build sequence

1. **Data spine + logger** on demo. Multi-venue spot + Kalshi WS, anchored to `floor_strike`.
   Weeks of data across regimes. *(Mandatory — nothing else can be built or validated without it.)*
2. **Fair-value + calibration model**, offline on logged data. Reliability curve, Brier score.
3. **Realistic-fill backtester** with adverse-selection modeling. Run the divergence +
   flow-conditioned strategy. Apply the kill criterion.
4. **If it survives:** paper-trade live on demo; confirm fills track the backtest.
5. **Small live** (real fills reveal true adverse selection), then scale only if live tracks
   paper. Continuous calibration-drift and regime monitoring; re-fit the vol model on schedule.

---

## 7. Engineering & operational notes
- **API:** REST for order entry (~50–200ms latency), WebSocket read-only for data, FIX 4.4 only
  if a proven edge later justifies it. Rate limits make true HFT impractical — design for
  medium-frequency, event-driven operation; implement 429 backoff with jitter.
- **Auth:** API key + RSA-PSS signing; sign the path without query params; timestamp in
  milliseconds. Use fixed-point (`_dollars` / `_fp`) fields with Decimal arithmetic, never floats.
- **Security:** credentials in env vars or a secret manager; never commit keys (public Kalshi
  bot repos routinely leak live RSA keys — design against that). Use a dedicated subaccount for
  the bot.
- **Reference client:** `pbeets/kalshi-trade-rs` is a clean, full-coverage example if you want one.

---

## 8. What kills this strategy
- Stale or noisy spot read → your "edge" is just your own data lag.
- Uncalibrated vol/probability → systematically mispriced quotes and adverse-selected fills.
- Ignoring adverse selection in the backtest → false positive, live losses.
- Quoting near the close without terminal modeling → picked off on settlement jumps.
- Sizing on in-sample edge → ruin when the real edge turns out thinner.
