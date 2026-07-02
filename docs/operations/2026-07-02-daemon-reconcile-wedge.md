# 2026-07-02 — Active daemon reconcile/heartbeat wedge after Postgres crash

## What happened

Postgres went through two crash-recovery cycles (15:57 and 16:21 UTC; backend
`exited with exit code 2` → `all server processes terminated; reinitializing`;
root cause diagnosed same day — see "Postgres crash root cause" below). The active-color
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
"heartbeat loop error" (post-fix, the loops log instead of dying).

## Postgres crash root cause (diagnosed + reproduced same day)

Postgres never actually crashed — **the postmaster deliberately entered
crash-recovery after reaping a process that wasn't a backend.**

Mechanism (reproduced in a throwaway `pgvector/pg16` container):
1. The container's postmaster runs as in-container **PID 1** (no init).
2. Under the morning's host memory-pressure storm (caused by the
   `daemon_production_crypto_1h_blue` leak cycling into its 8g cap — 236
   cgroup-OOM restarts since 06-30 — plus swap thrash; journald logged "Under
   memory pressure", and docker health checks timed out fleet-wide), dockerd
   failed to start/supervise the postgres container's own `pg_isready`
   healthcheck execs ("timed out starting health check" at 15:52–15:57 UTC;
   "Could not send KILL signal … process does not exist" at 16:19:40).
3. An abandoned healthcheck exec is **orphaned and reparents to PID 1 = the
   postmaster**. `pg_isready` exits **2** ("no response") when the loaded
   server can't answer within its timeout.
4. The postmaster reaps the unknown child, sees nonzero exit status, assumes
   a server process crashed → `server process (PID n) exited with exit code
   2` → kills all backends → crash recovery. The victim "backend" PIDs
   (443531, 445873) never logged anything because they were never backends.

Timing corroboration: last healthcheck-start failure 15:57:08 UTC → "crash"
15:57:14; exec kill failure 16:19:40 → "crash" 16:20:43.

Fix: `init: true` on both postgres services in `infra/docker-compose.yml`
(tini becomes PID 1 and reaps orphans; verified in the repro container that
the same orphaned `exit 2` no longer triggers recovery). **Takes effect only
when the postgres container is recreated** (`docker compose --env-file .env
-f infra/docker-compose.yml up -d postgres_production` — brief DB blip; the
2026-07-02 daemon loop fix makes the fleet tolerate it). Until then the
production DB remains exposed to a recurrence under memory pressure.

Residual disease: the crypto_1h daemon leak (still open, see CLAUDE.md
2026-06-30 note) is what created the pressure storm; its 8g cap contains the
blast radius but it still degrades the host every cycle.
