"""CPI nowcast gate-0 research subsystem (sub-project A).

Self-contained, NON-TRADING library code that mirrors the structure of `crypto/` but is far
smaller: an offline Cleveland Fed nowcast ingester, a nowcast-error distribution, read-only
Kalshi CPI ladder parsing, and a gate-0 backtest. Nothing here is wired into `AppContainer`
and nothing here places orders. See
`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`.
"""
