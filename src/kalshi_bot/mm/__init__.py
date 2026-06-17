"""Statistical market-making research stack (docs/research/kalshi_15m_market_making_plan.md).

A self-contained, non-trading research service that runs the plan's pipeline as a
continuous loop: data spine (multi-venue spot + Kalshi quotes, anchored to
floor_strike) → analytic fair value (Φ(ln(S/K)/(σ√τ)) + calibration) → realistic
-fill backtest. It never places orders — execution stays in the live
ExecutionService. Built in stages per plan §6; the data spine is the foundation.
"""
