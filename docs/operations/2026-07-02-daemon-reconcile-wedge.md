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

## crypto_1h daemon "leak" diagnosed (2026-07-03)

Live observation of a fresh container (memory sampled every 30s alongside
logs): RSS ramps only on the ACTIVE color, in ~130MB/cycle ratchets whenever
an hourly strike ladder sits inside the 1h close-time discovery window, and
partially releases when the ladder exits the window. It is not a classic
unbounded leak — it is working-set churn + glibc arena fragmentation:

- The 1h universe legitimately includes the wide hourly ladder markets
  (~50 strikes/asset; the "hourly event listing" duration rule in
  `db/repositories.py` exists precisely to include them). During ladder
  hours the loops churn hundreds of MB of market/orderbook/snapshot
  payloads per cycle (measured: 130MB of snapshot payloads written per 30
  min, ~2,500 distinct markets touched).
- The autonomy loop ran every 5s (`crypto_autonomy_idle_interval_seconds`
  default, sized for the 15m live path) with `persist=True` discovery —
  re-persisting the entire ladder as "live" snapshots ≈1.1M rows/day of
  write amplification into the ~71GB `crypto_market_snapshots` table.
- glibc malloc arenas never return the churn to the OS → RSS ratchets to
  the 8g cap → cgroup OOM restart (236 restarts 06-30→07-02, always on the
  active color).

Containment shipped on both crypto_1h services (shadow-only, no live
impact): `MALLOC_ARENA_MAX=2` (kills the fragmentation ratchet) and
`CRYPTO_AUTONOMY_IDLE_INTERVAL_SECONDS=60` (12x less ladder churn), plus
`oom_score_adj: 500` so a recurrence is the kernel's first pick instead of
last.

### The real burst source (found 2026-07-03 via py-spy, fixed in code)

The containment above was NOT sufficient — the daemon still OOM'd at 8g on
a multi-GB single-burst allocation. A py-spy trap (temporary SYS_PTRACE on
the shadow container, dumps auto-captured above a 2.5G threshold) caught
the main thread inside `json.loads` decoding JSONB from asyncpg, with
concurrent reads of `raw_exchange_events` (5.7GB), room `signals`, room
`artifacts`, and `historical_replay_runs` — i.e. **training-corpus room-
bundle assembly**. Root cause: `_run_heartbeat_follow_up` gated only on
ACTIVE COLOR, not role — the crypto-only 1h daemon shares APP_COLOR with
the main daemon, so this "lean collector" ran the ENTIRE heavy follow-up
suite (historical intelligence, decision-corpus promotion, gate tuning,
strategy regression/codex/promotion jobs, shadow campaigns, canary sweeps)
every heartbeat from a second container. (The trainer sets
`APP_COLOR: trainer`, so it never matched the active color and only ran the
ungated rollout-monitor tail — the duplication was specifically the
crypto_1h daemon whose compose env shares the real blue/green colors.)
Besides the memory bursts, that was duplicate concurrent execution of
stateful jobs.

Fix: `_run_heartbeat_follow_up` now returns immediately unless
`_heartbeat_role == "daemon"` (the main daemon). Crypto-only and trainer
nodes keep their liveness heartbeats; only the main active-color daemon
runs the follow-up suite (`tests/unit/test_daemon_follow_up_role_gate.py`).
Backlog items kept for reference (likely unnecessary now): drop
`persist=True` from 1h autonomy discovery; `collect_open`'s 5000-row
summary read → COUNT.
