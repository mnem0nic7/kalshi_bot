# Crypto Model Specification Audit

Date: 2026-05-12

## Prediction Target

The crypto model predicts the probability that a Kalshi 15-minute crypto market
settles YES. Training rows use `settlement_result` joined into `label_yes`.

## Feature Families

The model uses:

- Kalshi market mid/logit, bid/ask spread, volume, open interest, target price,
  time to close, and market age
- asset indicators
- candlestick momentum
- spot moneyness, spot momentum, spot returns, realized volatility, target
  distance in volatility units, spot staleness, and Kalshi-mid/spot gap
- quote-source flags and strict-trade eligibility
- recent per-asset settlement and mid-error features

`market_mid` is explicitly a feature and remains a diagnostic baseline.

## Gate Reform

Because market-mid is a model feature, requiring the trained model to beat
market-mid on Brier, log-loss, and ECE as hard promotion gates is misaligned
with the trading objective. Those metrics now remain diagnostics. Promotion
uses fee-adjusted out-of-sample simulated P/L on selected live-quality
candidates and requires the model to beat the market-mid baseline P/L.

## Baselines

Replay now reports market-mid, always-0.5, last-direction, naive momentum,
linear-on-returns, current heuristic, and calibrated model policy metrics.

