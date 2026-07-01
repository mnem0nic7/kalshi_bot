# CPI Nowcast — Gate-0 signal-quality result (2026-07-01)

**Status:** Criterion (1) PASSED. Criterion (2) (beat the market) still open.
**Spec:** `docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`
**Data:** Cleveland Fed `nowcast_month.json` (153 months with nowcast+actual, 2013-08 .. 2026-05).
**Method:** offline spike `scratchpad/gate0_signal.py` (NOT the production parser — that is TDD in `macro/`).

## Data validation (before any metric)

- **Units:** percent MoM, seasonally adjusted. Verified against BLS ground truth: 2020-04 = −0.80%
  (COVID trough), 2022-06 = +1.32% (9.1%-YoY month, BLS MoM +1.3%), 2015-01 = −0.68%. Range −0.80..+1.32,
  mean +0.23 — a textbook CPI MoM distribution. These are exactly what Kalshi KXCPI resolves against.
- **Alignment:** per frame, subcaption = target month; `CPI Inflation` series = daily nowcast path for
  that month; `Actual CPI Inflation` = realized print; `CPI May`/`PCE May` labels are timeline
  annotations, not targets. Final pre-release nowcast = last non-empty nowcast value.

## Result: nowcast vs actual (153 months)

| horizon                | RMSE (pp) | MAE (pp) | bias (pp) |
|------------------------|-----------|----------|-----------|
| nowcast, first day (~40d out) | 0.241 | 0.170 | −0.026 |
| nowcast, mid-run-up    | 0.158 | 0.108 | −0.012 |
| **nowcast, final (pre-release)** | **0.147** | 0.100 | −0.011 |
| baseline: carry-forward (prev actual) | 0.289 | 0.220 | — |
| baseline: trailing-mean 12m | 0.277 | — | — |

**Head-to-head (same 151 months): nowcast final RMSE 0.147 vs carry-forward 0.289 — ~2× better.**
- Nowcast closer than baseline in **120/151 = 79%** of months.
- Nowcast direction-vs-last-month correct in **132/151 = 87%**.

## Ladder-discrimination σ (the number that matters for trading)

- Final-nowcast error **stdev = 0.147pp** all-sample; **0.104pp ex-2021/22 shock**.
- |error| median 0.067, p75 0.129, p90 0.239, max 0.684.
- Within 0.1pp: 66% · within 0.2pp: 84% · within 0.3pp: 94%.
- KXCPI strikes are 0.1%-spaced → the nowcast discriminates strikes ~1–1.5 widths from the money;
  genuinely uncertain right at the money. Error shrinks toward release, so the tradeable edge is
  strongest in the last ~week and for months where the nowcast sits clearly between strikes.

## Interpretation

Gate-0 **criterion (1) — signal beats naive baselines — PASSES decisively** (~2× carry-forward, 79%
win rate, 87% directional). Combined with the cited Cleveland Fed real-time assessment (nowcast beats
SPF + Blue Chip consensus, 1999–2022), the *signal is real*.

**This is necessary, not sufficient.** The decisive **criterion (2) — nowcast beats the KALSHI market
net of fees — remains open** and requires the tested ladder parser + Kalshi CPI price history (thin:
~2mo API cutoff + forward collection). The market and consensus both beat carry-forward, so the live
edge is whatever gap remains between the nowcast (σ≈0.10–0.15pp) and the market-implied ladder. That
gap is the whole ballgame and is measured in Sub-project A's ladder backtest / Sub-project B's shadow.

**Honest caveat:** σ≈0.1pp is comparable to strike spacing and to typical consensus error, so the edge
is thin and situational (between-strike / far-from-market months), not a blanket mispricing. Do not
over-extrapolate criterion (1) into a profit claim; the fee `rate·p·(1−p)` still has to clear.
