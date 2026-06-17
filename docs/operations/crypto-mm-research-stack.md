# Crypto market-making research stack (`crypto_mm_production`)

A self-contained, **NON-TRADING** research service implementing
`docs/research/kalshi_15m_market_making_plan.md` as a continuous loop. It logs
the data spine and periodically OOS-evaluates the analytic vol fair value. It
**never places orders** — execution stays in the live `ExecutionService`.

## What it is

- Code: `src/kalshi_bot/mm/` (`data_spine.py`, `fair_value.py`, `backtest.py`,
  `storage.py`, `loop.py`, `service.py`).
- Container: `crypto_mm_production` (CPU-only, `mem_limit 8g`, `oom_score_adj 500`
  — not trading-critical, first OOM victim, cgroup-capped). Command:
  `python -m kalshi_bot.cli crypto-mm run`.
- Storage: append-only per-UTC-day JSONL on the `mm_data` volume
  (`/app/data/mm`), with optional Parquet compaction.

## Safety (triple-guarded)

`CRYPTO_TRADING_ENABLED=false`, `CRYPTO_AUTONOMY_ENABLED=false`,
`APP_SHADOW_MODE=true`, `APP_ENABLE_KILL_SWITCH=true`. The loop only logs and
evaluates; there is no order-placement path in `mm/`.

## Pipeline (per plan §3)

1. **Data spine** (`data_spine.py`) — discovers active 15m markets (anchored to
   `floor_strike` = `target_price_dollars`), joins the latest consolidated spot,
   normalizes to a tick row, appends to the day's JSONL partition.
2. **Fair value** (`fair_value.py`) — `Φ(ln(S/K)/(σ√τ))` with a realized-vol σ̂.
   **σ is the lever** (`crypto-vol-eval` showed the analytic edge stands or falls
   on it); iterate the σ estimator (EWMA/HAR, intraday seasonality) here.
3. **Backtest** (`backtest.py`) — maker entry rule (avoid the 45–55¢ peak-fee
   band), realistic fill (only when the market trades to the resting limit),
   settlement P&L (no maker rebate) so adverse selection is reflected.
4. **Eval stage** — the loop periodically runs the proven
   `evaluate_vol_fair_value` OOS scoring (vol vs mid, after fees + shrinkage).

## Run / debug

```bash
# Continuous loop (the container command)
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps crypto_mm_production

# One-off passes for debugging (inside a container / where the DB is reachable)
kalshi-bot-cli crypto-mm collect-once   # one data-spine collection pass
kalshi-bot-cli crypto-mm eval-once      # one vol fair-value OOS eval pass
```

## Staging (plan §6) — what is and isn't built

- **v1 (built):** data spine reusing already-collected spot (verifiable without
  new WS infra), analytic fair value + σ̂, maker/backtest core, continuous loop,
  JSONL storage, periodic OOS eval. Pure logic is unit-tested
  (`tests/unit/test_mm_*.py`).
- **Later stages (not built):** live multi-venue WebSocket book reconstruction
  with gap detection, the §4.3 order-flow / adverse-selection stand-down gate,
  fractional-Kelly sizing, and any live execution (which would go through
  `ExecutionService`, not `mm/`).

## Config

`mm_*` settings in `config.py` (`mm_enabled`, `mm_data_dir`, `mm_frequency`,
`mm_eval_interval_seconds`, `mm_idle_seconds`). See
`docs/crypto-trading-dials-and-knobs.md`.
