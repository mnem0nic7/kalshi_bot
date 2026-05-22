# Live Trading Deep Dive — May 21–22, 2026

**Scope:** 70 settled markets across 2 trading days (live since May 21 14:48 PDT)
**Net P&L:** $99.07 on $428.91 deployed (23.1% return on capital at risk)

---

## Overview

| Date   | Settled | W  | L  | Net P&L | Invested | Return |
|--------|---------|----|----|---------|----------|--------|
| May 21 | 44      | 30 | 14 | $49.72  | $179.44  | 27.7%  |
| May 22 | 26      | 17 | 9  | $49.35  | $249.47  | 19.8%  |
| Total  | 70      | 47 | 23 | $99.07  | $428.91  | 23.1%  |

Active assets: BTC (live), XRP (live), SOL (live), HYPE (shadow as of May 22 morning).

---

## 1. Asset Performance

| Asset | Settled | Win Rate | Profit Factor | Avg Win | Avg Loss | Break-even | Margin  |
|-------|---------|----------|---------------|---------|----------|------------|---------|
| BTC   | 26      | 73.1%    | 2.225         | $5.30   | $6.47    | 54.9%      | +18.2pp |
| XRP   | 6       | 100.0%   | ∞             | $4.89   | —        | —          | —       |
| HYPE  | 36      | 58.3%    | 1.137         | $5.98   | $7.37    | 55.2%      | +3.1pp  |
| SOL   | 2       | 50.0%    | 0.814         | $3.78   | $4.64    | 55.2%      | −5.2pp  |

**BTC is the engine.** At 73% win rate with a 2.22× profit factor it has substantial buffer above the 54.9% breakeven requirement.

**HYPE is on the knife's edge.** The 3.1pp margin above breakeven is thin enough that a 2–3 bad trades can flip it negative — exactly what happened on the morning of May 22 (5 consecutive losses). Shadow is correct until the next retrain.

**SOL** has only 2 settled markets; the sample is too small to draw conclusions.

### Win/Loss Dollar Asymmetry

The average win is consistently ~$1.20–1.40 smaller than the average loss for both BTC and HYPE (win/loss ratio ≈ 0.82). This is structural: we buy "yes" at prices near 50¢, so max win and max loss per contract are roughly symmetric — but losing positions tended to be larger due to the multi-fill race condition and compounding Kelly sizing. This creates a structural requirement for a win rate well above 50% to remain profitable.

---

## 2. Position Sizing — The Biggest Risk

Position sizes grew **4× in 18 hours**, driven by Kelly compounding on a growing account balance:

| Time (PDT)    | Avg contracts/market | $ at risk/trade |
|---------------|---------------------|-----------------|
| May 21 14–16h | 6–9                 | $3–5            |
| May 21 20–22h | 10–15               | $5–9            |
| May 22 01–04h | 15–20               | $8–12           |
| May 22 06–09h | 22–30               | $11–19          |

The largest single loss was `KXBTC15M-26MAY220630-30` at 30 contracts → **−$11.61**. Three of today's four worst HYPE losses were at 21–28 contracts each (−$11 to −$13). Yesterday's losses at the same model accuracy were −$3 to −$5 at 6–9 contracts.

The same model applied to 4× the position size produces 4× the loss magnitude. Kelly grows fast when winning — it will contract just as fast during a losing streak.

**Recommendation: Hard cap per-market at 20 contracts** (~$10 max at 50¢ entry). Kelly can size up to this cap but no further regardless of account balance.

---

## 3. Multi-Fill Race Condition (Fixed May 22)

7 markets had 3+ fills from the cancel-replace race condition in `_execute_limit()`. The damage was asymmetric:

| Asset | Fill type  | Outcome | Avg contracts | vs. single-fill |
|-------|------------|---------|--------------|-----------------|
| BTC   | 2+-fills   | loss    | 19.06        | +66%            |
| BTC   | 2+-fills   | win     | 10.12        | +2%             |
| HYPE  | 2+-fills   | loss    | 16.17        | +38%            |
| HYPE  | 2+-fills   | win     | 15.42        | +89%            |

BTC's multi-fill **losing** positions were at nearly double the intended size vs wins. The race condition disproportionately inflated losing positions — likely because cancel-replace triggered more often in illiquid conditions where maker fills arrived late, and those same conditions correlated with adverse market outcomes.

Worst case: `KXHYPE15M-26MAY220930-30` accumulated 41.42 contracts in 0.005 seconds (pure simultaneous fill) when ~20 was intended. It happened to win ($21.63), masking the structural risk.

**Fix deployed:** `_execute_limit()` now calls `get_fills(order_id=...)` after each cancel, accumulates already-filled quantity, and reduces the requote size accordingly. If the cancelled order was already fully covered by maker fills, the loop returns `status="filled"` without issuing a second order.

---

## 4. Taker vs Maker Fill Quality

BTC shows a stark divergence by fill type:

| Fill type | Count | Win rate | Avg entry |
|-----------|-------|----------|-----------|
| Maker     | 26    | 65.4%    | $0.467    |
| Taker     | 14    | **85.7%**| $0.491    |

Taker fills cross a wider spread (avg $0.491 vs $0.467) but win 20pp more often. This suggests the model generates two tiers of signals: high-conviction entries that fire immediately as takers, and passive-wait entries that rest as makers. HYPE shows no meaningful difference (57.5% maker vs 53.3% taker), consistent with HYPE having a weaker overall signal.

**Recommendation:** Track taker/maker win rates per asset weekly. If BTC taker quality holds at 80%+, consider a small sizing premium (e.g., 1.2× Kelly) for taker-qualified signals.

---

## 5. HYPE Entry Price Anomaly

HYPE wins at **higher** entry prices than losses:

| Outcome | Avg entry | P25   | P75   |
|---------|-----------|-------|-------|
| Win     | $0.5513   | $0.49 | $0.62 |
| Loss    | $0.5029   | $0.45 | $0.55 |

Buying "yes" at higher prices correlates with winning for HYPE. When the model is most confident (price already 60–70¢), it is also most accurate. The marginal entries at 37–44¢ are less reliable — these are long-shot positions where the market is pricing a low-probability event, and the model's edge is weakest.

**Recommendation:** Add a per-asset entry floor for HYPE of `min_yes_price = 0.45` to skip the low-confidence long-shot entries. Pair with the higher `min_fee_adjusted_edge_bps` (700–800 bps) when HYPE returns from shadow.

---

## 6. HYPE Over-Trading

| Asset | Trade tickets | Buy fills | Ticket→Fill rate |
|-------|--------------|-----------|-----------------|
| BTC   | 104          | 40        | 38.5%           |
| HYPE  | 120          | 70        | 58.3%           |
| XRP   | 26           | 6         | 23.1%           |
| SOL   | 18           | 18        | 100.0%          |

HYPE generates 15% more tickets than BTC but has a 58% ticket→fill rate vs BTC's 38%. The model is eager to enter HYPE and gets matched frequently. At 36 settled markets vs BTC's 26, HYPE is trading ~40% more markets despite having a substantially worse win rate. The permissive entry threshold (currently the same 500 bps global minimum as BTC) is the lever.

---

## 7. Time-of-Day Patterns

Sample is small (2 days) but notable:

| Hour (PDT) | Wins | Losses | Win rate |
|------------|------|--------|----------|
| 0–4h       | 10   | 3      | **76.9%** |
| 5–9h       | 5    | 8      | **38.5%** |
| 14–16h     | 8    | 3      | 72.7%    |
| 17–23h     | 21   | 8      | 72.4%    |

The 5–9am PDT window is notably weak (38.5%). This is pre-market / early US session for crypto. It may reflect reduced liquidity and noisier price action. With only 2 days of data this is suggestive, not conclusive — but worth monitoring weekly.

---

## 8. Data Quality Issues

### Stale unresolved fills

The fills table contains entries from May 17 with `settlement_result IS NULL` at suspicious prices ($0.04, $0.06, $0.84, $0.85). These appear to be from the shadow/replay period before live trading — all inserted in a millisecond batch at `2026-05-17 17:54:33`. The reconciliation service has not resolved them because those markets expired >7 days ago.

These don't affect live P&L (filtered by `settlement_result IS NOT NULL`) but create noise in the fills table and inflate apparent total exposure metrics.

**Recommendation:** Add a reconciliation sweep for fills where `settlement_result IS NULL AND created_at < NOW() - 48h`. These markets are certainly settled; results can be fetched from Kalshi's historical fills endpoint.

---

## 9. Priority Recommendations

| # | Recommendation | Why | How |
|---|----------------|-----|-----|
| 1 | **Hard per-trade cap: 20 contracts** | Sizing grew 4× in 18h; worst losses are $11–13; same model at 4× size = 4× drawdown | `RISK_MAX_CONTRACTS_PER_MARKET=20` + compose mapping |
| 2 | **HYPE edge floor when re-enabling: 700–800 bps** | 3.1pp win-rate buffer is insufficient; higher threshold targets 65%+ win rate | `kalshi-bot-cli crypto-policy set-asset-override --asset HYPE --min-edge-bps 750` |
| 3 | **HYPE min entry price ~0.45** | Entries below 0.45 have worse outcomes; the model is miscalibrated at long-shot prices | Asset-level entry override in policy |
| 4 | **Daily loss circuit breaker: $50–75** | Growing positions + bad hour = $46 losses in one morning session; needs a floor | `RISK_DAILY_LOSS_LIMIT_DOLLARS=60` + compose mapping |
| 5 | **Reconcile stale fills** | P&L reporting blind spot; ~10 fills with unknown settlement outcome | Backfill reconciliation job |
| 6 | **Track taker/maker win rates weekly** | BTC taker quality (85.7%) is a strong signal; warrants monitoring and possible sizing premium | Add to weekly training log |

---

## Appendix: Worst 10 Trades

| Asset | Market | Contracts | Entry | PnL | Fills | Taker |
|-------|--------|-----------|-------|-----|-------|-------|
| HYPE | KXHYPE15M-26MAY221045-45 | 22.66 | $0.58 | −$13.14 | 1 | yes |
| HYPE | KXHYPE15M-26MAY221000-00 | 27.83 | $0.44 | −$11.92 | 2 | yes |
| HYPE | KXHYPE15M-26MAY220900-00 | 27.79 | $0.44 | −$11.90 | 4 | yes |
| BTC  | KXBTC15M-26MAY220630-30  | 30.08 | $0.39 | −$11.61 | 2 | no  |
| HYPE | KXHYPE15M-26MAY221100-00 | 22.66 | $0.50 | −$11.34 | 4 | no  |
| HYPE | KXHYPE15M-26MAY220815-15 | 21.81 | $0.54 | −$11.05 | 2 | yes |
| BTC  | KXBTC15M-26MAY221230-30  | 18.89 | $0.55 | −$10.39 | 1 | yes |
| BTC  | KXBTC15M-26MAY212345-45  | 18.24 | $0.51 | −$9.21  | 2 | yes |
| HYPE | KXHYPE15M-26MAY212000-00 | 14.80 | $0.50 | −$7.20  | 2 | no  |
| HYPE | KXHYPE15M-26MAY220700-00 | 18.56 | $0.38 | −$6.96  | 2 | yes |

The top 6 losses all have ≥20 contracts. The largest single-fill loss (KXBTC15M-26MAY221230-30, 18.89 contracts) is today's most recent settled market — the sizing growth is ongoing.

## Appendix: Best 10 Trades

| Asset | Market | Contracts | Entry | PnL | Fills | Race fill? |
|-------|--------|-----------|-------|-----|-------|------------|
| HYPE | KXHYPE15M-26MAY220930-30 | 41.42 | $0.48 | +$21.64 | 3 | **yes** |
| HYPE | KXHYPE15M-26MAY220345-45 | 20.28 | $0.52 | +$9.73  | 2 | no |
| HYPE | KXHYPE15M-26MAY212000-00 | 17.14 | $0.45 | +$9.43  | 2 | no |
| HYPE | KXHYPE15M-26MAY220630-30 | 19.11 | $0.53 | +$9.01  | 5 | no |
| BTC  | KXBTC15M-26MAY220430-30  | 17.45 | $0.49 | +$8.90  | 1 | no |
| HYPE | KXHYPE15M-26MAY220015-15 | 21.88 | $0.59 | +$8.88  | 2 | no |
| BTC  | KXBTC15M-26MAY220345-45  | 16.98 | $0.46 | +$8.53  | 2 | no |
| XRP  | KXXRP15M-26MAY220700-00  | 16.00 | $0.47 | +$8.48  | 1 | no |
| HYPE | KXHYPE15M-26MAY220415-15 | 17.59 | $0.52 | +$8.44  | 1 | no |
| BTC  | KXBTC15M-26MAY220315-15  | 17.12 | $0.53 | +$8.05  | 1 | no |

The top winner (HYPE 41.42 contracts, +$21.64) was the race-condition double-fill. It happened to land on a winning market, but the same mechanism produced 6 of the 10 worst losses.
