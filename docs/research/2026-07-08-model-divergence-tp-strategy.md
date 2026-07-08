# Model-divergence entry + model-pegged take-profit: REFUTED (2026-07-08)

Operator-proposed strategy, backtested at their request before any build:

1. Model predicts P(yes); ENTER (taker) when the executable price is ≥10¢
   cheaper than the model (either side).
2. Rest a take-profit sell pegged to the CURRENT model prediction, re-pegged
   every snapshot as the prediction moves.
3. While the model is ≥80% confident, pull the sell and ride to settlement;
   reinstate below 80%.

Method (`scripts/model_divergence_tp_backtest.py`, READ-ONLY): model =
analytic vol_normal fair value on v15 fresh-spot materialized rows; 14d, all
7 assets, one trade per market, ttc 90–870s, entry taker fee 0.07·p·(1−p),
maker exit $0; TP fill = next snapshot's opposing best bid trades through the
pegged level. Arm A = full rule; arm B = identical entries HELD to
settlement (isolates the TP mechanics).

## Result @ th≥0.10 (net of fees)

| asset | n | A full rule avg/ct | B hold avg/ct | TP fired | TP cost vs holding |
|---|---|---|---|---|---|
| BNB | 597 | −8.2¢ | −6.7¢ | 60% | −$9 |
| HYPE | 593 | −4.6¢ | +0.7¢ | 70% | −$31 |
| XRP | 631 | −8.8¢ | −2.4¢ | 78% | −$41 |
| ETH | 567 | −7.5¢ | +2.9¢ | 85% | −$59 |
| DOGE | 578 | −10.4¢ | −4.0¢ | 82% | −$37 |
| BTC | 667 | −9.9¢ | −3.0¢ | 94% | −$46 |
| SOL | 849 | −10.2¢ | −1.9¢ | 87% | −$71 |

**Total: −$388 over ~4,500 trades (−8.7¢/ct); every asset negative.**

Two independent failures:

1. **The entry has no edge** — even held to settlement the entries average
   −2.1¢/ct. A 10¢ model-market gap is mostly the market being right (the
   measured edge-shrinkage β ≈ 0.125–0.2 made concrete on this exact rule).
2. **The model-pegged TP is strictly harmful on all 7 assets** (−$9…−$71
   each): it caps winners at (model fair − entry) while losers ride to zero,
   and re-pegging to a noisy model realizes losses on model wobble — the same
   pathology that led to the 07-06 CRYPTO_15M exit exemption, rebuilt in
   reverse.

DO NOT DEPLOY. Revisit only with a model that beats the mid (see
`2026-07-08-model-bar-push.md`); the order mechanics themselves already exist
in the champion path (edge gates) and prediction-scaled TP.
