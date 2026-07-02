# 2026-07-02 — Active daemon reconcile/heartbeat wedge after Postgres crash

## What happened

Postgres went through two crash-recovery cycles (15:57 and 16:21 UTC; backend
`exited with exit code 2` → `all server processes terminated; reinitializing`;
underlying backend-crash cause not yet diagnosed). The active-color
(green) daemon survived, but its reconcile loop and heartbeat follow-up path
went silent for ~7 hours: no rows landed in `fills` after 15:34 UTC even while
the stale-quote pilot was filling live orders, and no settlement
reconciliation ran. Everything *visible* stayed green — the container health
check passed and the market-history loop kept writing snapshots.

## Root cause

`_periodic_reconcile_loop` and `_periodic_heartbeat_loop` were the only
periodic loops in `services/daemon.py` with no exception handling and no
bound on iteration time. `DaemonService.run()` intends a death of either loop
to crash the daemon (Docker would restart it), but a mid-crash await on a dead
DB connection doesn't raise — it hangs forever. A hung loop never completes
`asyncio.wait(FIRST_COMPLETED)`, so the daemon neither recovered nor died.
Secondary bug: the heartbeat follow-up's error handler wrote an ops event to
the same dead DB, so the handler itself raised ("Task exception was never
retrieved") and the original error was only visible in container logs.

## Fix

- Both loops now wrap each iteration in `asyncio.timeout(daemon_loop_stall_timeout_seconds)`
  (new setting, default 300s) plus catch-log-continue, matching the pattern
  every other periodic loop already used. A DB blip now costs at most one
  iteration; a wedged await is cancelled and retried on the next interval.
- The follow-up runner's ops-event write is best-effort: if the DB is down,
  the failure is logged and swallowed instead of double-faulting the task.
- Tests: `tests/unit/test_daemon_loop_resilience.py` (raise, hang, and
  cancellation-propagation cases for both loops; double-fault case for the
  follow-up runner).

## Detection gap (open)

The daemon health check does not observe reconcile staleness directly; the
tell was `SELECT max(created_at) FROM fills` lagging live activity. If this
recurs, check that first, then `docker logs` for "reconcile loop error" /
"heartbeat loop error" (post-fix, the loops log instead of dying). The
Postgres backend crash itself (exit code 2, preceded by a storm of canceled
`crypto_spot_ohlc` statements) is an unexplained open item.
