# Crypto 15m Touch20 Rules Runbook

Last updated: 2026-06-05

This runbook covers the independent non-model 15-minute Touch20 rules strategy.
BTC keeps the legacy strategy code `btc15m_touch20_rules`; other supported
assets use their own lanes, settings, gates, approvals, ledgers, and order
prefixes. The path is additive. It must not disable or retune the model-trained
crypto bot.

The same rules module can evaluate 1-hour Touch20 lanes with frequency-scoped
strategy codes, gates, approvals, ledgers, and order prefixes. The 1h touch
worker is disabled in the current production posture and should not be restarted
unless an operator explicitly re-enables the container, rules, and trading
switches.

## 1h Evidence Expansion Status

As of 2026-06-05, the 1h path is configured for continuous data-only evidence
collection across BTC, HYPE, ETH, BNB, SOL, DOGE, and XRP. The separate 1h
Touch20 worker is stopped and disabled.

Current production settings:

- `ENABLE_CRYPTO_CURRENT_1H_CONTAINER=true`
- `PRODUCTION_CRYPTO_CURRENT_1H_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP`
- `CRYPTO_1H_CURRENT_INTERVAL_SECONDS=0`
- `CRYPTO_1H_CURRENT_SETTLED_EVERY_CYCLES=20`
- `CRYPTO_1H_CURRENT_SETTLED_DAYS=2`
- `CRYPTO_1H_CURRENT_SETTLED_LABEL_PROPAGATION_ENABLED=true`
- `CRYPTO_1H_CURRENT_REPLAY_GATE_ENABLED=false`
- `CRYPTO_1H_CURRENT_REPLAY_GATE_EVERY_CYCLES=80`
- `CRYPTO_1H_CURRENT_REPLAY_GATE_DAYS=30`
- `CRYPTO_1H_CURRENT_REPLAY_GATE_LIMIT=50000`
- `CRYPTO_1H_CURRENT_REPLAY_JOINED_FALLBACK_ENABLED=false`
- `PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ENABLED=false`
- `PRODUCTION_CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED=false`
- `PRODUCTION_CRYPTO_1H_TOUCH20_ALLOWED_SIDES=yes,no`
- `PRODUCTION_CRYPTO_1H_TOUCH20_TAKE_PROFIT_PCT=0.15`
- `PRODUCTION_CRYPTO_1H_TOUCH20_MIN_SECONDS_TO_CLOSE=300`
- `PRODUCTION_CRYPTO_1H_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.10`
- `PRODUCTION_CRYPTO_1H_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.85`
- `PRODUCTION_CRYPTO_1H_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0`
- `PRODUCTION_CRYPTO_1H_TOUCH20_MIN_RULE_SCORE=0.25`
- `PRODUCTION_CRYPTO_1H_TOUCH20_BUCKET_PRICE_BAND_CENTS=40`
- `PRODUCTION_CRYPTO_1H_TOUCH20_BUCKET_SPREAD_BAND_CENTS=2`
- `PRODUCTION_CRYPTO_1H_TOUCH20_BUCKET_TIME_BAND_MINUTES=60`
- `PRODUCTION_CRYPTO_1H_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0`
- `PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP`
- `ENABLE_CRYPTO_1H_TOUCH20_CONTAINER=false`

The production 1h current collector is data-only: `CRYPTO_TRADING_ENABLED=false`
inside the container. It collects open-market quote evidence and fresh Coinbase
spot rows. The separate 1h Touch20 worker remains stopped; container-level
`CRYPTO_TRADING_ENABLED` is still mapped to
`PRODUCTION_CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED` for any future explicit
operator re-enable. Automatic settled-label refresh and replay/gate refresh
remain disabled in the current production posture.

The current 1h data collector and 1h Touch20 order service defaults both keep
automatic replay/gate refresh disabled while the production entry-qualified
quote-path access path is being indexed and stabilized. Manual replay and
optimize commands can include the joined settled-label fallback unless run with
`--skip-joined-fallback`, but those commands should only be run after confirming
no production index build or replay query is already active.

The 2026-06-04 15:08 UTC runtime check confirmed the 15m blue live service was
still running with BTC, HYPE, ETH, BNB, SOL, DOGE, and XRP configured in the
15m Touch20 lane. The 1h current collector was also running, but remained
data-only (`CRYPTO_TRADING_ENABLED=false` and
`CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED=false`), and the separate 1h order
service was not running. The 1h settled refresh used `min_close_ts` with
cutoff-bounded pagination and per-ticker settlement propagation; database
activity sampled as short per-ticker updates rather than the earlier long JSON
join update. The subsequent 15:10 UTC 1h gate refresh passed BTC with `88`
allowed samples, while HYPE, ETH, BNB, SOL, DOGE, and XRP remained blocked; no
1h live approval or order service start happened.

The same runtime audit found all seven 15m assets had zero-fill entry orders
rejected by Kalshi with `insufficient_balance`, while the strategy ledger still
marked those client order ids as `entry_submitted`. That made every approved
15m asset appear near its `$10` cap even though no order had filled. The live
container was rebuilt after `_entry_ledger_decision()` was changed to treat
`rejected*`/`failed`/`error` zero-fill entry statuses as terminal non-ledger
events, and the seven rejected zero-fill ledger reservations were removed. A
post-fix check showed all seven 15m ledgers at `0.0000` open/pending notional.
Fresh trades still require production buying power; otherwise future candidates
will continue to reject with `insufficient_balance`, but they should no longer
reserve phantom strategy cap.

The 2026-06-04 03:58 UTC collector cycle confirmed:

- open 1h quote evidence stored for all seven assets
- fresh Coinbase spot rows stored for all seven assets
- one-day settled backfill stored `29132` label snapshots across all seven
  assets with no missing asset and no API errors
- settlement propagation was still `0` because the newly collected 1h quote
  rows had not settled yet
- the replay blocker at that point was `missing_settled_label`

After the 04:00 UTC market settlement buffer, a manual 2026-06-04 04:05 UTC
one-day settled-label pass stored `28872` settled snapshots and propagated
`5256` settlement labels onto live/evidence quote rows. The direct replay index
then returned recent 1h rows for BTC, HYPE, ETH, BNB, and SOL. The first bounded
BTC replay still failed the gate because those rows were captured near close and
did not yet provide a full post-entry quote path; candidate reasons were
primarily `missing_real_bid_ask`.

After the 05:00 UTC market settlement buffer, a manual 2026-06-04 05:05 UTC
one-day settled-label pass again stored `28872` settled snapshots and
propagated `42644` settlement labels. BTC, HYPE, ETH, BNB, and SOL received
direct 05:00 UTC labels on live/evidence quote rows. DOGE and XRP had 05:00 UTC
live/evidence rows, but their matching 05:00 UTC settled backfill labels were
not visible yet, so they need a later delayed settlement pass.

The larger 1h replay slice (`--limit 20000`) still failed the gate for every
asset. BTC produced only `5` 1h trade candidates and `0` allowed candidates;
HYPE, ETH, BNB, and SOL produced `0` trade candidates. DOGE and XRP remained on
older direct rows because their 05:00 UTC labels had not landed. Dominant replay
blockers were sparse executable bid/ask paths (`missing_real_bid_ask`), plus
some price, spread, timing, momentum, and spot-availability filters. Leave the
collector running for more settled 1h hours before re-evaluating gates.

At 2026-06-04 05:20 UTC, the 1h evidence collector was rebuilt and recreated
with an additional market-duration guard. Kalshi's open endpoint was returning
active week-long/day-range crypto markets under the same hourly-frequency
series tickers; those rows must not be stored as 1h quote-path evidence. The
collector now rejects series-backed 1h/15m market rows whose open-close duration
does not match the requested frequency, and `collect-open` skips markets that
are not active at the observation timestamp. The replay repository also filters
1h quote-path rows to hour-long market durations, so previously stored
day/week-range rows cannot become 1h gate evidence if they settle later. The
first fixed collector cycle found no active duration-valid 1h markets to store.

At 2026-06-04 05:31 UTC, current market discovery was also updated to follow
Kalshi market cursors, matching the historical and settled collectors, so active
duration-valid 1h markets cannot be missed merely because they appear after the
first page. A one-off fixed `collect-open` still found `0` active
duration-valid 1h markets at that timestamp, so the remaining blocker is market
availability plus settled quote-path support rather than a first-page-only
collector bug.

At 2026-06-04 05:43 UTC, the 1h `collect-open` path was further narrowed to
query the current 1h close window with `min_close_ts`/`max_close_ts` and no
status filter. This avoids the misleading `status=open` day/week-range markets
while still allowing initialized/open hour-long markets to be seen once they are
inside the active close window. The fixed one-off pass again found `0` active
duration-valid 1h markets to store for BTC, HYPE, ETH, BNB, SOL, DOGE, and XRP.

At 2026-06-04 06:58 UTC, the current collector's recent-evidence diagnostic was
aligned with the same hour-duration guard used by replay and live selection, so
daily/week-range rows stamped as `frequency=1h` no longer appear as current 1h
readiness evidence. The 1h current collector also gained an evaluation-only
replay/gate refresh cadence; it can keep blocked/passed gate artifacts current
without starting the 1h order-submission daemon.

At 2026-06-04 08:20 UTC, the 1h replay path selected recent
entry-qualified markets instead of simply taking the newest quote rows. Latest
gates were still blocked for every asset. BTC had `10528` entry-window rows,
`5` candidates, and `0` allowed candidates. HYPE, ETH, BNB, SOL, DOGE, and XRP
had entry-window rows (`33242`, `29934`, `10725`, `33317`, `7952`, and `4200`)
but produced `0` candidates. Replay now includes side-level input diagnostics so
operators can distinguish no-data problems from rule-filter problems. The
current blockers are still sparse live-faithful executable bid/ask paths and
strict rule overlap, not an approved tradeable 1h edge.

A disabled-by-default research override, `max_spread_dollars`, was added so
offline sweeps can test wider spreads without changing the production default.
The default `0` keeps the existing tiered spread rule: 1 cent below a 20 cent
entry and 2 cents otherwise. A production sweep tested wider 5 cent and 10 cent
spread caps, lower 10%/15% take-profit profiles, broader price buckets, and
lower rule-score variants. None passed the 1h gate. BTC remained at `0` allowed
candidates with negative net P/L under wider profiles; ETH and SOL only produced
loss-making candidates; HYPE, BNB, DOGE, and XRP stayed at `0` candidates. Do
not carry the wider-spread override into live settings without a new passed gate
and explicit approval.

At 2026-06-04 08:37 UTC, all seven 1h replay/gate artifacts were refreshed with
a cumulative `side_filter_funnel` diagnostic. The current 1h config allows YES
only, so the funnel shows how many YES-side rows survive each rule in order:

```text
asset  candidates  allowed  entry_window  executable  price_band  spread_ok
BTC    5           0        10528         741         81          76
HYPE   0           0        33242         140         0           0
ETH    0           0        29934         1102        209         3
BNB    0           0        10725         0           0           0
SOL    0           0        33317         358         71          0
DOGE   0           0        7952          84          42          0
XRP    0           0        4200          54          23          0
```

At 2026-06-04 08:41 UTC, those artifacts were refreshed again with
market-level funnel counts, which align with the live-faithful replay rule that
enters at most once per market:

```text
asset  candidates  allowed  entry_markets  executable_markets  price_markets  spread_markets
BTC    5           0        376            33                  9              7
HYPE   0           0        292            4                   0              0
ETH    0           0        357            16                  8              1
BNB    0           0        300            0                   0              0
SOL    0           0        292            7                   4              0
DOGE   0           0        342            4                   2              0
XRP    0           0        150            2                   1              0
```

A non-persisting profile diagnostic then tested `yes,no`, wider open
`0.10-0.85` price bands, zero minimum momentum, lower rule scores, NO-only, and
10% take-profit variants. None passed. BTC reached at most `17` candidates with
negative net P/L (`-$0.26`) under the 10% take-profit open profile; ETH reached
at most `5` candidates with negative net P/L (`-$0.85`); HYPE, BNB, SOL, DOGE,
and XRP stayed at `0` candidates. This rules out a simple side/momentum/price
override as the path to live 1h approval.

The latest direct Kalshi probe at 2026-06-04 08:30 UTC also showed why
`collect-open` can legitimately report `checked_markets: 0` between active
hourly windows: visible BTC 1h rows were the next `09:00-10:00` markets with
`status=initialized` and no bid/ask. Those are useful discovery evidence, but
they are not tradeable quote-path rows and must not be stored as live executable
evidence before they open.

At 2026-06-04 09:01 UTC, replay quote-path filtering was made side-aware. The
settled replay repository and live-faithful replay evaluator now keep a row when
at least one configured side has a complete bid/ask pair, instead of requiring
YES bid, YES ask, NO bid, and NO ask all to be present. This matters for the
current YES-only 1h research gates because otherwise valid YES evidence can be
discarded when Kalshi does not expose a complete NO side. Production kept the
old all-four partial index `ix_crypto_market_snapshots_touch20_replay_direct`
and added a new valid side-aware partial index,
`ix_crypto_market_snapshots_touch20_replay_side_direct`. Production Alembic was
advanced to `20260604_0041`.

The post-index 2026-06-04 10:18-10:19 UTC replay/gate refresh still blocked
every 1h asset:

```text
asset  status   candidates  allowed  sample_count
BTC    blocked  5           0        19176
HYPE   blocked  0           0        50000
ETH    blocked  0           0        50000
BNB    blocked  0           0        17250
SOL    blocked  0           0        50000
DOGE   blocked  0           0        16617
XRP    blocked  0           0        6450
```

The side-aware data-access fix widened valid evidence handling, but it did not
create an approved 1h trading edge. Do not treat it as approval to trade.

At 2026-06-04 10:43-10:44 UTC, the 1h current collector was rebuilt and
recreated with the side-aware evaluator plus binary-complement quote inference:
when a direct side bid/ask is missing or non-executable, the evaluator can use
the opposite side's executable bid/ask pair to infer the equivalent binary price.
The refreshed production gates still blocked every 1h asset:

```text
asset  status   candidates  allowed  created_at
BTC    blocked  5           0        2026-06-04 10:43:21 UTC
HYPE   blocked  0           0        2026-06-04 10:43:33 UTC
ETH    blocked  0           0        2026-06-04 10:43:46 UTC
BNB    blocked  0           0        2026-06-04 10:43:53 UTC
SOL    blocked  0           0        2026-06-04 10:44:07 UTC
DOGE   blocked  0           0        2026-06-04 10:44:15 UTC
XRP    blocked  0           0        2026-06-04 10:44:21 UTC
```

The new `quote_source_rows` diagnostics equaled direct raw bid/ask rows in this
refresh, which means the remaining blocker is not missing complement handling.
It is sparse executable 1h liquidity and insufficient supported replay buckets.
The 1h optimizer fetch window now uses the loosest proposed profile entry window
(`300` seconds to close, not the stricter live `1200` seconds) when evaluating
candidate profiles. A corrected read-only 10k BTC optimizer run found a best
loose profile with `19` candidates, positive total P/L, and `0` allowed bucket
support; a corrected 10k XRP run found only `1` losing candidate. Neither is
eligible for live 1h approval.

At 2026-06-04 11:01-11:15 UTC, the read-only 1h optimizer gained 1h-only coarse
bucket research profiles (`bucket_time_band_minutes=60`,
`bucket_price_band_cents=40`) so it can test a single `0_60m` time bucket without
weakening the live gate criteria. The 10k all-asset production optimizer sweep
still found no passed 1h profile:

```text
asset  status             best_profile                              trades  allowed  net_pl
BTC    no_passed_profile  yes_no_take15_maxspread10_open_s25        19      0        0.58
HYPE   no_passed_profile  current                                   0       0        0.00
ETH    no_passed_profile  no_take15_stop50_open_s25                 1       0        0.11
BNB    no_passed_profile  current                                   0       0        0.00
SOL    no_passed_profile  yes_no_open_s30                           1       0        0.18
DOGE   no_passed_profile  current                                   0       0        0.00
XRP    no_passed_profile  yes_no_take10_maxspread10_open_s25        1       0       -0.12
```

For BTC, the new coarse `time60/price40` profile created broader buckets such as
`BTC|no|40_80c|gt_2c|0_60m`, but the largest useful bucket still had only `3`
samples, below the bucket-support floor of `5`. The blocker is therefore current
1h evidence support, not merely time-bucket fragmentation.

At 2026-06-04 11:22 UTC, the data-only 1h current collector cadence was lowered
from `60` seconds to `30` seconds. This increases quote-path density for future
settled replay without changing live gate criteria or enabling the
`crypto_non_model_1h_touch20_production` order-submission service.

At 2026-06-04 11:29 UTC, the 1h production Compose services were updated to
explicitly map `PRODUCTION_CRYPTO_1H_TOUCH20_*` settings into the
`CRYPTO_1H_TOUCH20_*` environment names read by the rules evaluator. Before this
fix, the data-only collector's replay/gate refresh saw defaults such as
`CRYPTO_1H_TOUCH20_ASSET_SETTINGS={}` even though production-scoped settings
were present. The production 1h evaluation profile was also set to the broad
research shape (`yes,no`, `300` seconds to close, `0.10-0.85` price range,
`40c` price buckets, `2c` spread buckets, `60m` time buckets) while keeping all
per-asset `trading_enabled=false`.

At 2026-06-04 11:49 UTC, a corrected-env 50k read-only optimizer sweep showed
BTC improving to `17-20` candidates and `6` allowed replay-bucket candidates
under `yes_no_take15_maxspread5_open_s25` / adjacent broad profiles. That still
blocks because the gate minimum is `50` allowed candidates, but it is real bucket
support rather than zero support. HYPE/BNB/DOGE remained at `0` candidates, ETH
had `2` candidates, SOL had `1`, and XRP had `1` losing candidate. Production
1h asset settings were updated to track the best known read-only evaluation
profiles for BTC, ETH, and XRP while keeping every asset `trading_enabled=false`.

After recreating the data-only collector with those per-asset settings, the
2026-06-04 11:50-11:51 UTC regular replay/gate refresh still blocked every 1h
asset:

```text
asset  status   candidates  allowed  allowed_net  blocker
BTC    blocked  17          6        0.15         allowed count below 50
HYPE   blocked  0           0        0.00         no candidates
ETH    blocked  2           0        0.00         no allowed bucket support
BNB    blocked  0           0        0.00         no candidates
SOL    blocked  1           0        0.00         no allowed bucket support
DOGE   blocked  0           0        0.00         no candidates
XRP    blocked  0           0        0.00         no candidates
```

The BTC allowed bucket is `BTC|no|0_40c|le_2c|0_60m` with `6` samples, `0.15`
net simulated P/L, `66.7%` touch rate, and `33.3%` stop-loss rate. It is
live-ineligible until more settled 1h quote-path evidence raises allowed support
to the gate minimum.

At 2026-06-04 12:00-12:02 UTC, a manual settled refresh and corrected-env
replay/gate sweep kept every 1h asset blocked:

| asset | status | candidates | allowed | allowed net | blocker |
| --- | --- | ---: | ---: | ---: | --- |
| BTC | blocked | 17 | 6 | 0.15 | allowed count below 50 |
| HYPE | blocked | 0 | 0 | 0.00 | no candidates |
| ETH | blocked | 2 | 0 | 0.00 | no allowed bucket support |
| BNB | blocked | 0 | 0 | 0.00 | no candidates |
| SOL | blocked | 1 | 0 | 0.00 | no allowed bucket support |
| DOGE | blocked | 0 | 0 | 0.00 | no candidates |
| XRP | blocked | 0 | 0 | 0.00 | no candidates |

A read-only optimizer probe with additional low-target 1h profiles found no
promotable profile, so those experimental profiles were not kept. The data-only
1h collector cadence was then lowered from `30` seconds to `15` seconds to
increase future quote-path density without changing replay pass thresholds or
starting the 1h order-submission service.

At 2026-06-04 12:26 UTC, after the 15-second collector recreate, the automatic
evaluation sweep completed with all 1h gates still blocked:

| asset | status | candidates | allowed | allowed net | net |
| --- | --- | ---: | ---: | ---: | ---: |
| BTC | blocked | 23 | 0 | 0.00 | -0.93 |
| HYPE | blocked | 0 | 0 | 0.00 | 0.00 |
| ETH | blocked | 3 | 0 | 0.00 | 0.26 |
| BNB | blocked | 0 | 0 | 0.00 | 0.00 |
| SOL | blocked | 1 | 0 | 0.00 | 0.18 |
| DOGE | blocked | 0 | 0 | 0.00 | 0.00 |
| XRP | blocked | 0 | 0 | 0.00 | 0.00 |

At 2026-06-04 12:40-12:41 UTC, the replay entry selector was tightened to ignore
terminal `0/1` quote rows that only have bid/ask fields present. A row now has
to contain at least one non-terminal, non-crossed executable side: YES bid/ask
inside `(0, 1)` with ask at or above bid, or the equivalent NO bid/ask pair.
This is a data-quality fix only; it does not lower replay pass thresholds,
loosen gates, enable 1h trading, or change order submission.

The strict selector made the available 1h evidence more realistic and improved
BTC from `23` candidates / `0` allowed to `101` candidates / `17` allowed after
`15860` settlement labels were propagated. BTC still failed because the gate
requires at least `50` allowed candidates. All other 1h assets also remained
blocked:

| asset | status | candidates | allowed | allowed net | net | allowed touch | allowed stop |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTC | blocked | 101 | 17 | 1.70 | -0.97 | 64.7% | 17.6% |
| HYPE | blocked | 0 | 0 | 0.00 | 0.00 | 0.0% | 0.0% |
| ETH | blocked | 6 | 0 | 0.00 | 0.01 | 0.0% | 0.0% |
| BNB | blocked | 0 | 0 | 0.00 | 0.00 | 0.0% | 0.0% |
| SOL | blocked | 1 | 0 | 0.00 | 0.18 | 0.0% | 0.0% |
| DOGE | blocked | 1 | 0 | 0.00 | -0.08 | 0.0% | 0.0% |
| XRP | blocked | 1 | 0 | 0.00 | -0.12 | 0.0% | 0.0% |

The strict-selector BTC optimizer probe found a diagnostic pass only by lowering
`replay_min_candidates` from `50` to `15`; that is not live-promotable and must
not be used for approval. The optimizer now reports that case as
`diagnostic_profile_found` / `diagnostic_passed` rather than
`passed_profile_found`. The best gate-preserving BTC profile was
`no_take15_stop50_price30_open_s25`, which remained blocked at `45` allowed
candidates versus the required `50` despite positive allowed replay P/L
(`1.93`) and `48.9%` allowed touch rate.

An attempted partial index for the strict replay selector was canceled because
it would require a large `crypto_market_snapshots` scan during live operations.
The canceled index was dropped, the 0042 migration was removed from the image,
and production Alembic remained at `20260604_0041`. At 2026-06-04 12:52 UTC the
data-only 1h collector was recreated from the clean image with
`CRYPTO_TRADING_ENABLED=false`,
`CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED=false`, and all seven 1h assets loaded.
The `crypto_non_model_1h_touch20_production` order-submission service remained
stopped.

At 2026-06-04 13:07-13:19 UTC, a strict-selector all-asset optimizer sweep and
manual replay/gate refresh moved the data-only 1h evaluation settings to the
best gate-preserving profiles found so far, while keeping every asset
`trading_enabled=false`. The first BTC candidate profile
(`no_take15_stop50_price30_open_s25`) was rejected after a persisted replay
showed `0` allowed buckets under newer evidence. A fresh optimizer pass then
selected the more stable `yes_no_take15_maxspread10_time60_price40_open_s25`
shape for BTC. That profile is still not live-promotable, but it leaves BTC
closest to the gate:

| asset | status | candidates | allowed | allowed net | net | allowed touch | allowed stop |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTC | blocked | 155 | 44 | 2.66 | -1.58 | 63.6% | 18.2% |
| HYPE | blocked | 0 | 0 | 0.00 | 0.00 | 0.0% | 0.0% |
| ETH | blocked | 7 | 5 | 0.18 | 0.35 | 80.0% | 20.0% |
| BNB | blocked | 1 | 0 | 0.00 | -0.12 | 0.0% | 0.0% |
| SOL | blocked | 1 | 0 | 0.00 | 0.18 | 0.0% | 0.0% |
| DOGE | blocked | 2 | 0 | 0.00 | -0.15 | 0.0% | 0.0% |
| XRP | blocked | 1 | 0 | 0.00 | -0.12 | 0.0% | 0.0% |

The blocker remains evidence support: BTC needs `6` more allowed replay-bucket
candidates to clear the configured `50` minimum, and every non-BTC lane is much
farther away. The 1h order-submission service is still stopped.

At 2026-06-04 23:00 UTC, the 1h entry-qualified joined fallback path was moved
behind an online partial index,
`ix_crypto_market_snapshots_touch20_entry_quote`, matching strict executable
entry quote predicates. The migration was made resilient to interrupted
`CREATE INDEX CONCURRENTLY` runs by dropping and rebuilding missing or invalid
index shells instead of trusting `IF NOT EXISTS`. The repository replay/update
paths that force index plans also set a local `45s` statement timeout so failed
or unindexed 1h evidence refreshes fail fast instead of lingering in production.

A fresh 1h status read at that time showed all seven assets configured with
`enabled=true` and `trading_enabled=false`. BTC had `49` candidates and `0`
allowed candidates; HYPE had `0`; ETH had `24` and `0`; BNB, SOL, and XRP each
had `2` and `0`; DOGE had `16` and `0`. Every 1h gate remained blocked and
there was no operator approval for any 1h asset.

To keep live loops from competing with the production index build or minting
new timestamped gate versions during normal entry/exit cycles, the 15m Touch20
service also defaults `CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0`.
Manual replay/gate refresh remains available and still requires fresh operator
approval for any new gate version before entries can resume under that gate.

Post-index 1h refresh sequence:

1. Confirm `ix_crypto_market_snapshots_touch20_entry_quote` is valid and no
   `pg_stat_progress_create_index` row remains for it.
2. Confirm no stale replay query is active in `pg_stat_activity`.
3. Run a bounded manual replay and gate for each 1h asset:

```bash
for asset in BTC HYPE ETH BNB SOL DOGE XRP; do
  kalshi-bot-cli crypto-non-model-touch20 replay \
    --kalshi-env production \
    --frequency 1h \
    --asset "$asset" \
    --days 30 \
    --limit 50000 \
    --json
  kalshi-bot-cli crypto-non-model-touch20 gate \
    --kalshi-env production \
    --frequency 1h \
    --asset "$asset" \
    --json
done
```

4. Inspect each 1h status. Only assets with a passed gate, valid simulator
   version, and exact operator approval for that gate version may move to
   `trading_enabled=true`.

Do not start `crypto_non_model_1h_touch20_production` for live order submission
until the direct label path produces replay rows, each asset's 1h gate passes,
and the operator approves that exact 1h gate/simulator version with max
notional.

## Core Trading Logic

The strategy buys a supported 15-minute contract only when deterministic rules
and replay-bucket evidence indicate the contract has a good chance to touch its
configured net executable profit target before close. BTC defaults to a stricter YES-only
setup based on current live-faithful replay evidence.

It does not try to predict settlement as its primary objective. The preferred
outcome is:

1. enter from the executable ask, directly or from the equivalent binary
   complement when the opposite side has an executable pair
2. wait for contract-price fluctuation
3. sell from the executable bid when net profit reaches the profile target after
   estimated entry and exit taker fees

Replay now mirrors live exit mechanics. It scans the future quote path for
take-profit, stop-loss, and profit-protection exits, then terminal-closes at
market close only when no executable exit occurs first. Live exits are
strategy-owned and trigger on the profile-specific net take-profit target,
profile-specific stop loss, or armed profit protection. BTC/HYPE/ETH/BNB use
+20%/-30% in their current profiles; SOL live-ready evidence uses +15%/-50%,
and DOGE/XRP live-ready evidence uses +10%/-50%.

The gate reports both the full first-eligible replay universe and the subset in
allowed replay buckets. Gate pass/fail decisions use the allowed-bucket subset
when a bucket matrix is present, because live entries are allowed only inside
those buckets. The full universe remains in the report for diagnostics and
profile tuning.

## Scope

Allowed:

- assets: BTC, ETH, SOL, XRP, BNB, DOGE, HYPE
- frequency: 15m for this live-approved runbook; 1h is a separate evidence-gated
  expansion and is not live-approved by this document
- market type: Kalshi crypto contracts
- sides: configured YES/NO buys; BTC default is YES only
- entry: executable ask
- exit: executable bid
- process: `crypto_non_model_btc15m_touch20_production`
- strategy code: `btc15m_touch20_rules` for BTC; `<asset>15m_touch20_rules` for
  non-BTC assets, such as `eth15m_touch20_rules`
- order ID prefix: `b15t20r` for BTC; `<asset>15t20r` for non-BTC assets, such
  as `eth15t20r`

Not allowed:

- assets outside BTC, ETH, SOL, XRP, BNB, DOGE, HYPE
- 1h crypto live order submission without a passed 1h gate and explicit 1h
  operator approval
- trained crypto model calls
- trained model artifact loading as entry authority
- model settlement replay gates
- global crypto 15m take-profit exits
- entries inside the final 5 minutes
- proxy-only spot evidence

## Environment Flags

All live flags are disabled by default.

| Variable | Default | Meaning |
|---|---:|---|
| `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED` | `false` | Enables the rules path to evaluate candidates. |
| `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED` | `false` | Allows the rules path to submit entry orders. |
| `CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES` | `yes` | Comma-separated allowed entry sides. BTC defaults to YES-only. |
| `CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT` | `0.20` | Net executable take-profit target. |
| `CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT` | `0.30` | Net executable stop-loss trigger for strategy-owned positions. |
| `CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE` | `600` | Minimum time to close for new entries. |
| `CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS` | `10` | Strategy-local open plus pending notional cap. |
| `CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS` | `10` | Strategy-local daily realized loss stop. |
| `CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS` | `5` | Minimum strategy entry notional after sizing. |
| `CRYPTO_BTC15M_TOUCH20_MAX_BUCKET_LIVE_LOSS_DOLLARS` | `1` | Live bucket loss threshold that blocks more entries in that bucket. |
| `CRYPTO_BTC15M_TOUCH20_MAX_BUCKET_CONSECUTIVE_LOSSES` | `2` | Consecutive stop/terminal losses that block a live bucket. |
| `CRYPTO_BTC15M_TOUCH20_MAX_REPLAY_STOP_LOSS_RATE` | `0.35` | Max replay stop-loss rate for the gate and bucket allowance. |
| `CRYPTO_BTC15M_TOUCH20_MAX_REPLAY_TERMINAL_LOSS_RATE` | `0.15` | Max replay terminal-loss rate for the gate and bucket allowance. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_THRESHOLD_PCT` | `0.10` | Profit level that arms profit protection. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_FLOOR_PCT` | `0.05` | Armed profit-protection floor. |
| `CRYPTO_BTC15M_TOUCH20_LOOP_INTERVAL_SECONDS` | `0` | Docker process loop sleep. `0` immediately starts the next pass. |
| `CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS` | `0.20` | Strategy-owned minimum entry ask. |
| `CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS` | `0.50` | Strategy-owned maximum entry ask. |
| `CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM` | `0.0005` | Minimum side-aligned spot momentum. |
| `CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE` | `0.458` | Minimum standalone rules score for entry. |
| `CRYPTO_BTC15M_TOUCH20_BUCKET_PRICE_BAND_CENTS` | `10` | Replay/live bucket price-band width. Supported values are `10`, `20`, `30`, and `40`; non-BTC lanes can override this in `CRYPTO_15M_TOUCH20_ASSET_SETTINGS`. |
| `CRYPTO_BTC15M_TOUCH20_BUCKET_SPREAD_BAND_CENTS` | `1` | Replay/live bucket spread-band width. Default keeps `le_1c` and `le_2c` separate; sparse non-BTC lanes can set `2` to merge both into `le_2c` while preserving side, price band, and time bucket. |
| `CRYPTO_BTC15M_TOUCH20_BUCKET_TIME_BAND_MINUTES` | `5` | Replay/live bucket time-band width. Default keeps `5_10m` and `10_15m` separate; sparse non-BTC lanes can set `10` to merge both into `5_15m` while preserving side, price band, and spread band. |
| `max_spread_dollars` asset setting | `0` | Optional per-asset research override in `CRYPTO_15M_TOUCH20_ASSET_SETTINGS` or `CRYPTO_1H_TOUCH20_ASSET_SETTINGS`. `0` keeps the tiered default spread rule; nonzero values require a fresh passed gate and approval before live use. |
| `CRYPTO_BTC15M_TOUCH20_QUOTE_FRESH_SECONDS` | `30` | Maximum age for live Kalshi quote snapshots. |
| `CRYPTO_BTC15M_TOUCH20_SPOT_FRESH_SECONDS` | `180` | Maximum age for live asset spot rows. |
| `CRYPTO_15M_TOUCH20_RULES_ASSETS` | `BTC` | Comma-separated assets for the Docker loop to evaluate. |
| `CRYPTO_15M_TOUCH20_ASSET_SETTINGS` | `{}` | JSON object with per-asset overrides for non-BTC lanes. |

Production uses the `PRODUCTION_` prefixed versions in `.env`, mapped into the
container as the runtime names above.

BTC uses the legacy `CRYPTO_BTC15M_TOUCH20_*` flags. Non-BTC lanes are disabled
by default and must be enabled through `CRYPTO_15M_TOUCH20_ASSET_SETTINGS`.
Example:

```json
{
  "ETH": {
    "rules_enabled": true,
    "trading_enabled": false,
    "allowed_sides": "yes,no",
    "max_open_notional_dollars": 10,
    "daily_loss_limit_dollars": 10,
    "take_profit_pct": 0.20,
    "stop_loss_pct": 0.30,
    "min_seconds_to_close": 300,
    "min_contract_price_dollars": 0.10,
    "max_contract_price_dollars": 0.85,
    "max_spread_dollars": 0.0,
    "min_aligned_momentum": 0.0,
    "min_rule_score": 0.30
  }
}
```

## Entry Checklist

An entry can be submitted only when all of the following are true:

1. The command scope is a supported 15m asset.
2. The asset lane has `rules_enabled=true`; BTC uses
   `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true`.
3. The running container color is the active deployment color.
4. The kill switch is off.
5. The separate rules replay gate has status `passed`.
6. Strategy daily realized P/L is not below the daily loss limit.
7. The latest market quote snapshot is fresh.
8. Market status is open or active.
9. The candidate side has an executable bid/ask pair.
10. Asset spot features are fresh and non-proxy.
11. Market age is at least 60 seconds.
12. Time to close is at least 600 seconds by default.
13. Candidate side is allowed by `CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES`; BTC
    defaults to YES-only.
14. Entry ask is at least the configured minimum contract price, default `$0.20`.
15. Entry ask is below the configured maximum contract price, default `$0.50`.
16. Side-aligned spot momentum is at least `0.0005` by default.
17. The +20% fee-aware target exit price is below `$1.00`.
18. Spread is within the tier limits, unless a nonzero per-asset
    `max_spread_dollars` override is explicitly gated and approved.
19. Standalone rule score clears the configured minimum.
20. Candidate replay bucket is allowed by the asset-owned gate, such as
    `btc15m_touch20_rules_gate:15m:BTC` or `eth15m_touch20_rules_gate:15m:ETH`.
21. Candidate bucket is not blocked by live bucket controls.
22. This strategy has no open or pending entry on the same Kalshi market.
23. The market is not in the one-cycle cooldown after a strategy stop/terminal
    loss.
24. Strategy-owned open plus pending notional remains within the `$10` cap.
25. Sized order notional is at least the configured minimum, default `$5`.
26. Operator approval checkpoint exists and references the latest passed gate
    version and replay simulator version.
27. The asset lane has `trading_enabled=true`; BTC uses
    `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true`.

If the final trading flag is false, the process can still produce
`trading_disabled` telemetry with the selected candidate and no order.

## Candidate Ranking

When multiple candidates pass filters, the strategy chooses one candidate per
entry cycle using this ranking:

1. higher replay bucket P/L per candidate
2. higher replay bucket touch rate
3. higher standalone rule score
4. tighter spread
5. more remaining time

The standalone rule score is a deterministic weighted score based on replay
touch rate, replay P/L per candidate, target gap, remaining time, realized spot
volatility, short-term spot momentum, and spread quality. It is not a trained
model prediction.

## Replay And Gate

Build the non-model replay artifact:

```bash
kalshi-bot-cli crypto-non-model-touch20 replay \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --days 30 \
  --json
```

Persist the separate live gate:

```bash
kalshi-bot-cli crypto-non-model-touch20 gate \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

Approve the exact passed gate version:

```bash
kalshi-bot-cli crypto-non-model-touch20 approve \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --approved-by <operator> \
  --json
```

Gate artifact:

```text
btc15m_touch20_rules_gate:15m:BTC
```

Non-BTC lanes use their own artifact type, for example:

```text
eth15m_touch20_rules_gate:15m:ETH
```

Gate requirements:

- at least 50 candidates in allowed replay buckets
- real settled quote-path evidence present
- no trained model usage
- replay simulator version `live_exit_v3`
- entry replay mode `first_eligible_per_market`
- net simulated P/L above `$0.00` after live-faithful exits
- P/L per candidate at least `$0.01` after fees
- touch rate at least 25%
- stop-loss rate at or below 35%
- terminal-loss rate at or below 15%
- hard-cap breaches equal 0
- at least one allowed bucket
- no allowed replay bucket with negative P/L, excessive stop losses, or
  excessive terminal losses

Approval checkpoint:

```text
btc15m_touch20_rules_approval:<kalshi_env>:BTC:15m
```

Non-BTC approvals are separate, for example:

```text
eth15m_touch20_rules_approval:<kalshi_env>:ETH:15m
```

A new gate version or simulator version invalidates old approval until the
operator approves again. The old Grantv approval for the touch-only simulator is
expected to fail closed after this remediation until renewed against the current gate.

## BTC Live Activation Record - 2026-06-03

This is the condensed record of how BTC 15m Touch20 reached live-approved
status.

### Activation Timeline

1. Built the standalone 15m Touch20 rules lane so BTC could trade from
   deterministic rules and live-faithful replay evidence without depending on
   the trained crypto model.
2. Ran production replay/gate evidence and found an initially profitable profile
   that leaned on the `BTC|yes|50_60c|le_1c|10_15m` bucket.
3. Rejected that profile for live use because the same bucket was already
   blocked by live bucket controls. Passing replay evidence is not enough when
   the live loop would skip the bucket that made replay look good.
4. Added executable-evidence reporting so the gate exposed total replay
   candidates, allowed replay candidates, live-executable candidates, and
   allowed buckets blocked by live risk controls.
5. Retuned the BTC profile to the 20-50c YES-only universe with stricter
   momentum and score filters, then reran replay until the gate passed with at
   least 50 live-executable candidates and positive net P/L.
6. Redeployed only the focused production services needed for BTC 15m Touch20,
   current crypto data, the app, web, Postgres, and Caddy.
7. Verified the new gate failed closed without fresh operator approval because
   the simulator/gate version had changed.
8. Recorded explicit operator approval for BTC 15m Touch20 with max notional
   `$10`, tied to the exact passed gate and simulator version.
9. Ran a post-approval status and `run-once` check. The strategy was enabled,
   trading-enabled, gate-passed, approval-valid, flat on positions, and armed;
   the first loop returned `no_candidate`, which is expected for this selective
   profile.

### What Made It Live

BTC was considered live only after all of these were simultaneously true:

- the production BTC lane had `rules_enabled=true` and `trading_enabled=true`
- replay used the live exit simulator, `live_exit_v3`
- the asset-owned gate `btc15m_touch20_rules_gate:15m:BTC` had status `passed`
- live-executable replay support stayed above the minimum sample threshold
- allowed replay buckets did not overlap live-blocked buckets
- the focused production container was running on the active deployment color
- operator approval matched the exact gate version and simulator version
- approval capped strategy notional at `$10`
- strategy-local open positions, pending notional, and daily realized P/L were
  clean at activation

### Starting Problem

The first replay-passing profile was profitable but depended on the live-blocked
bucket:

```text
BTC|yes|50_60c|le_1c|10_15m
```

That bucket had prior live losses and was blocked by live bucket controls. A
gate that passed by leaning on that bucket would look good in replay while the
live entry loop skipped part of the candidate universe.

### Code And Gate Fixes

Two commits made the evidence and runtime behavior line up:

- `e12b07f Clarify BTC touch gate executable evidence`
- `8bc906f Tune BTC touch profile away from live-blocked bucket`

The first commit added separate reporting for:

- total replay candidates
- candidates in allowed replay buckets
- candidates still executable after subtracting live-blocked buckets
- allowed replay buckets that are currently live-blocked

The second commit moved the BTC profile away from the blocked 50-60c band and
made bucket-level gate failures apply only to buckets that the gate allows live.
Unallowed buckets remain visible in `blocked_bucket_keys`, but they do not block
the gate simply because live would never trade them.

### Final BTC Profile

The final BTC production profile is:

```text
CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true
CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES=yes
CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT=0.20
CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30
CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600
CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20
CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50
CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005
CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458
CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS=10
CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS=10
CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS=5
```

### Replay Evidence

The non-persisting production replay audit for this profile returned:

```text
status pass
gate_reasons []
trade_candidate_count 54
allowed_trade_candidate_count 51
net_simulated_pl_dollars 2.657
pnl_per_candidate_dollars 0.0492037037037037
touch_rate 0.6111111111111112
stop_loss_rate 0.3333333333333333
terminal_loss_rate 0.0
allowed_bucket_keys [
  BTC|yes|20_30c|le_1c|10_15m,
  BTC|yes|30_40c|le_1c|10_15m,
  BTC|yes|40_50c|le_1c|10_15m
]
```

This proved the profile had at least 50 live-executable replay candidates and
positive net P/L without relying on the blocked `50_60c` bucket.

### Deployment Shape

The focused production redeploy kept unrelated strategy containers down:

```bash
ENABLE_DEMO_DAEMON=false \
ENABLE_PRODUCTION_DAEMON=false \
ENABLE_CRYPTO_1H_DAEMON=false \
ENABLE_CRYPTO_1H_CONTAINER=false \
ENABLE_CRYPTO_CURRENT_CONTAINER=true \
ENABLE_BTC15M_TOUCH20_CONTAINER=true \
ENABLE_WEB_STRATEGIES_CONTAINER=false \
scripts/blue_green_redeploy.sh --env production --yes
```

After the final redeploy, the running container set was:

```text
infra-crypto_non_model_btc15m_touch20_production-1
infra-crypto_current_production-1
infra-app_production_blue-1
infra-web_production-1
infra-postgres_production-1
infra-caddy-1
```

No demo daemon, generic production daemon, 1h crypto container/daemon, web
strategies container, or inactive app color was left running.

### Final Gate And Approval

The current live gate is:

```text
btc15m-touch20-rules-gate-15m-BTC-20260603174324-b68c8e1726b0
```

The older approval against `live_exit_v2` correctly failed closed after the
simulator changed to `live_exit_v3`. The user then explicitly approved:

```text
btc approved
```

Approval was recorded with:

```bash
kalshi-bot-cli crypto-non-model-touch20 approve \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --approved-by Grantv \
  --max-notional-dollars 10 \
  --note "User approved via Codex: btc approved" \
  --json
```

The approved checkpoint referenced:

```text
gate_version btc15m-touch20-rules-gate-15m-BTC-20260603174324-b68c8e1726b0
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:27:06.035727+00:00
```

Final status after approval:

```text
enabled true
trading_enabled true
approval_valid true
approval_reason operator_approval_valid
gate status passed
gate sample_count 51
live_executable_candidate_count 51
live_blocked_allowed_bucket_keys []
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

The first approved `run-once` returned `no_candidate`, not an order, because the
fresh market was too early or too late and older snapshots were stale. That is
expected: the strategy is live-approved and armed, but it remains selective.
Based on the 30-day replay evidence, expect roughly one to two qualifying
opportunities per day rather than trades in every 15-minute market.

## HYPE Live Activation Record - 2026-06-03

HYPE was added as a non-BTC 15m Touch20 lane after the BTC path was live. It is
configured with its own asset lane, replay gate, operator approval, and
per-asset trading switch.

### HYPE Profile

The production HYPE override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
HYPE.rules_enabled=true
HYPE.trading_enabled=true
HYPE.allowed_sides=yes
HYPE.take_profit_pct=0.20
HYPE.stop_loss_pct=0.30
HYPE.min_seconds_to_close=300
HYPE.min_contract_price_dollars=0.20
HYPE.max_contract_price_dollars=0.60
HYPE.min_aligned_momentum=0.0
HYPE.min_rule_score=0.40
HYPE.replay_min_candidates=50
HYPE.max_open_notional_dollars=10
HYPE.daily_loss_limit_dollars=10
HYPE.min_order_notional_dollars=5
```

### HYPE Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-HYPE-20260603174336-7c649fe5463b
gate_version btc15m-touch20-rules-gate-15m-HYPE-20260603174338-208cf001c20a
artifact_type hype15m_touch20_rules_gate:15m:HYPE
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 138
allowed_trade_candidate_count 51
allowed_net_simulated_pl_dollars 1.78
allowed_pnl_per_candidate_dollars 0.03490196078431373
allowed_touch_rate 0.5882352941176471
allowed_stop_loss_rate 0.29411764705882354
allowed_terminal_loss_rate 0.0
allowed_hard_cap_breaches 0
gate_reasons []
```

The full first-eligible universe was still negative:

```text
net_simulated_pl_dollars -2.359
pnl_per_candidate_dollars -0.017094202898550726
touch_rate 0.463768115942029
stop_loss_rate 0.42028985507246375
```

That negative total is not hidden. It is why HYPE needs allowed-bucket aggregate
gating: the live loop will reject unallowed buckets, so the live-readiness proof
must judge the subset HYPE can actually trade.

Allowed HYPE buckets:

```text
HYPE|yes|30_40c|le_2c|10_15m
HYPE|yes|20_30c|le_1c|10_15m
HYPE|yes|50_60c|le_1c|10_15m
HYPE|yes|30_40c|le_1c|5_10m
HYPE|yes|40_50c|le_1c|5_10m
HYPE|yes|40_50c|le_1c|10_15m
```

### HYPE Runtime State

After explicit approval and focused redeploy, the running production process had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 51
live_executable_candidate_count 51
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

The operator approval was:

```text
approve HYPE 15m touch with max notional 10
```

Approval was recorded with:

```bash
kalshi-bot-cli crypto-non-model-touch20 approve \
  --kalshi-env production \
  --frequency 15m \
  --asset HYPE \
  --approved-by Grantv \
  --max-notional-dollars 10 \
  --note "User approved via Codex: approve HYPE 15m touch with max notional 10" \
  --json
```

The approved checkpoint referenced:

```text
gate_version btc15m-touch20-rules-gate-15m-HYPE-20260603173025-eff6b361796d
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T17:36:42.058211+00:00
```

The daemon's automatic replay/gate refresh was then disabled with:

```text
CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0
```

This prevents the live order loop from minting a new timestamped gate version
and invalidating approval during normal entry/exit cycles. Manual replay and
gate refreshes are still available; any new gate version requires fresh operator
approval before HYPE can submit entries again.

After the stable-gate redeploy, approval was renewed against the current gate:

```text
gate_version btc15m-touch20-rules-gate-15m-HYPE-20260603174338-208cf001c20a
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T17:45:29.380308+00:00
```

After a later live-faithful gate refresh, approval was renewed again against the
current production HYPE gate:

```text
gate_version btc15m-touch20-rules-gate-15m-HYPE-20260603201939-964a7cda1e71
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T20:19:47.640430+00:00
```

The first approved daemon HYPE entry pass returned:

```text
status no_candidate
reason stale_quote_snapshot
```

That is expected. HYPE is live-approved and armed, but still selective. It may
return `no_candidate` until a fresh 15m market lands in an allowed replay bucket
with fresh spot, acceptable spread, enough time to close, and a rule score above
`0.40`.

## ETH Live Activation Record - 2026-06-03

ETH was added to the production 15m Touch20 loop after the non-BTC profile sweep
found one passing 30-day allowed-bucket gate. ETH is now live-approved after
explicit operator approval, with `trading_enabled=true` and max notional `10`.

### ETH Profile

The production ETH override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
ETH.rules_enabled=true
ETH.trading_enabled=true
ETH.allowed_sides=yes,no
ETH.take_profit_pct=0.20
ETH.stop_loss_pct=0.30
ETH.min_seconds_to_close=300
ETH.min_contract_price_dollars=0.10
ETH.max_contract_price_dollars=0.85
ETH.min_aligned_momentum=0.0
ETH.min_rule_score=0.30
ETH.replay_min_candidates=50
ETH.max_open_notional_dollars=10
ETH.daily_loss_limit_dollars=10
ETH.min_order_notional_dollars=5
```

### ETH Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-ETH-20260603182049-a9d8cd7fdd52
gate_version btc15m-touch20-rules-gate-15m-ETH-20260603182103-3b18872e9981
artifact_type eth15m_touch20_rules_gate:15m:ETH
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 453
allowed_trade_candidate_count 58
allowed_net_simulated_pl_dollars 1.969
allowed_pnl_per_candidate_dollars 0.033948275862068965
allowed_touch_rate 0.5344827586206896
allowed_stop_loss_rate 0.25862068965517243
allowed_terminal_loss_rate 0.0
allowed_hard_cap_breaches 0
gate_reasons []
```

The full first-eligible universe was negative:

```text
net_simulated_pl_dollars -14.527
pnl_per_candidate_dollars -0.03206843267108168
touch_rate 0.4260485651214128
stop_loss_rate 0.4304635761589404
```

Allowed ETH buckets:

```text
ETH|no|60_70c|le_2c|10_15m
ETH|no|60_70c|le_1c|10_15m
```

### ETH Runtime State

After explicit approval and focused redeploy to active color `blue`, the running production process had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 58
live_executable_candidate_count 58
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

Approval was recorded with:

```text
gate_version btc15m-touch20-rules-gate-15m-ETH-20260603182103-3b18872e9981
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:21:35.708299+00:00
```

ETH is live-approved and armed, but still selective. It may return
`no_candidate` until a fresh 15m market lands in an allowed replay bucket with
fresh spot, acceptable spread, enough time to close, and a rule score above
`0.30`.

## BNB Live Activation Record - 2026-06-03

BNB became live-ready after adding an asset-level 20c bucket price-band option.
The gate threshold was not relaxed: BNB still needed at least 50 allowed replay
candidates, positive net P/L, positive P/L per candidate, acceptable touch/stop
rates, no terminal-loss breach, and no hard-cap breaches. The wider price bucket
is captured in replay metrics and status as `bucket_price_band_cents=20`.

BNB is now live-approved after explicit operator approval, with
`trading_enabled=true` and max notional `10`.

### BNB Profile

The production BNB override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
BNB.rules_enabled=true
BNB.trading_enabled=true
BNB.allowed_sides=yes,no
BNB.take_profit_pct=0.20
BNB.stop_loss_pct=0.30
BNB.min_seconds_to_close=300
BNB.min_contract_price_dollars=0.60
BNB.max_contract_price_dollars=0.80
BNB.min_aligned_momentum=0.0
BNB.min_rule_score=0.25
BNB.bucket_price_band_cents=20
BNB.replay_min_candidates=50
BNB.max_open_notional_dollars=10
BNB.daily_loss_limit_dollars=10
BNB.min_order_notional_dollars=5
```

### BNB Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-BNB-20260603184228-2e251f39a1c5
gate_version btc15m-touch20-rules-gate-15m-BNB-20260603184238-bce9e6f424e7
artifact_type bnb15m_touch20_rules_gate:15m:BNB
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
bucket_price_band_cents 20
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 97
allowed_trade_candidate_count 57
allowed_net_simulated_pl_dollars 3.469
allowed_pnl_per_candidate_dollars 0.060859649122807016
allowed_touch_rate 0.6491228070175439
allowed_stop_loss_rate 0.15789473684210525
allowed_terminal_loss_rate 0.0
gate_reasons []
```

The full first-eligible universe was slightly negative:

```text
net_simulated_pl_dollars -0.166
pnl_per_candidate_dollars -0.001711340206185567
```

Allowed BNB buckets:

```text
BNB|yes|60_80c|le_2c|10_15m
BNB|no|60_80c|le_1c|10_15m
BNB|no|60_80c|le_2c|10_15m
```

The same profile with the default 10c bucket price bands did not pass: it had
45 allowed replay candidates. The 20c BNB bucket mode groups adjacent 60-70c
and 70-80c evidence while preserving side, spread, and time-to-close controls.

### BNB Runtime State

After explicit approval and focused redeploy to active color `blue`, the running production process had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 57
live_executable_candidate_count 57
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

Approval was recorded with:

```text
gate_version btc15m-touch20-rules-gate-15m-BNB-20260603184238-bce9e6f424e7
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:21:41.726095+00:00
```

BNB is live-approved and armed, but still selective. It may return
`no_candidate` until a fresh 15m market lands in an allowed replay bucket with
fresh spot, acceptable spread, enough time to close, and a rule score above
`0.25`.

## SOL Live Activation Record - 2026-06-03

SOL became live-ready after adding targeted non-BTC optimizer profiles for
profile-specific exit settings. The gate threshold was not relaxed: SOL still
needed at least 50 allowed replay candidates, positive net P/L, positive P/L per
candidate, acceptable touch/stop rates, no terminal-loss breach, and no hard-cap
breaches.

SOL is now live-approved after explicit operator approval, with
`trading_enabled=true` and max notional `10`.

### SOL Profile

The production SOL override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
SOL.rules_enabled=true
SOL.trading_enabled=true
SOL.allowed_sides=no
SOL.take_profit_pct=0.15
SOL.stop_loss_pct=0.50
SOL.min_seconds_to_close=300
SOL.min_contract_price_dollars=0.10
SOL.max_contract_price_dollars=0.85
SOL.min_aligned_momentum=0.0
SOL.min_rule_score=0.25
SOL.replay_min_candidates=50
SOL.max_open_notional_dollars=10
SOL.daily_loss_limit_dollars=10
SOL.min_order_notional_dollars=5
```

### SOL Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-SOL-20260603200028-9ae029fb579a
gate_version btc15m-touch20-rules-gate-15m-SOL-20260603200153-2023b2f7ac57
artifact_type sol15m_touch20_rules_gate:15m:SOL
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 498
allowed_trade_candidate_count 77
allowed_net_simulated_pl_dollars 2.32
allowed_pnl_per_candidate_dollars 0.03012987012987013
allowed_touch_rate 0.6493506493506493
allowed_stop_loss_rate 0.2727272727272727
allowed_terminal_loss_rate 0.0
gate_reasons []
```

Allowed SOL buckets:

```text
SOL|no|20_30c|le_1c|10_15m
SOL|no|30_40c|le_2c|10_15m
SOL|no|50_60c|le_1c|10_15m
```

The previous best SOL profile had only 19 allowed replay candidates. The passing
profile, `no_take15_stop50_open_s25`, stays inside the allowed-bucket gate and
uses NO-only entries with a +15% take-profit target and -50% stop.

### SOL Runtime State

After explicit approval and focused redeploy to active color `blue`, the running production process had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 77
live_executable_candidate_count 77
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

Approval was recorded with:

```text
gate_version btc15m-touch20-rules-gate-15m-SOL-20260603200153-2023b2f7ac57
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:21:47.788718+00:00
```

SOL is live-approved and armed, but still selective. It may return
`no_candidate` until a fresh 15m market lands in an allowed replay bucket with
fresh spot, acceptable spread, enough time to close, and a rule score above
`0.25`.

## DOGE Live Activation Record - 2026-06-03

DOGE became live-ready after adding an asset-level spread-bucket width option.
The gate threshold was not relaxed: DOGE still needed at least 50 allowed replay
candidates, positive net P/L, positive P/L per candidate, acceptable touch/stop
rates, no terminal-loss breach, and no hard-cap breaches. The wider spread
bucket is captured in replay metrics and status as `bucket_spread_band_cents=2`.

DOGE is now live-approved after explicit operator approval, with
`trading_enabled=true` and max notional `10`.

### DOGE Profile

The production DOGE override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
DOGE.rules_enabled=true
DOGE.trading_enabled=true
DOGE.allowed_sides=yes,no
DOGE.take_profit_pct=0.10
DOGE.stop_loss_pct=0.50
DOGE.min_seconds_to_close=300
DOGE.min_contract_price_dollars=0.10
DOGE.max_contract_price_dollars=0.85
DOGE.min_aligned_momentum=0.0
DOGE.min_rule_score=0.30
DOGE.bucket_spread_band_cents=2
DOGE.replay_min_candidates=50
DOGE.max_open_notional_dollars=10
DOGE.daily_loss_limit_dollars=10
DOGE.min_order_notional_dollars=5
```

### DOGE Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-DOGE-20260603205717-a4581451cf94
gate_version btc15m-touch20-rules-gate-15m-DOGE-20260603205725-75a4ddddff87
artifact_type doge15m_touch20_rules_gate:15m:DOGE
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
bucket_price_band_cents 10
bucket_spread_band_cents 2
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 209
allowed_trade_candidate_count 53
allowed_net_simulated_pl_dollars 3.689
allowed_pnl_per_candidate_dollars 0.06960377358490566
allowed_touch_rate 0.8679245283018868
allowed_stop_loss_rate 0.11320754716981132
allowed_terminal_loss_rate 0.0
allowed_hard_cap_breaches 0
gate_reasons []
```

The full first-eligible universe was negative:

```text
net_simulated_pl_dollars -13.482
pnl_per_candidate_dollars -0.06450717703349282
touch_rate 0.583732057416268
stop_loss_rate 0.3923444976076555
```

Allowed DOGE buckets:

```text
DOGE|no|60_70c|le_2c|5_10m
DOGE|yes|80_90c|le_2c|10_15m
DOGE|yes|60_70c|le_2c|10_15m
DOGE|yes|70_80c|le_2c|5_10m
DOGE|no|20_30c|le_2c|10_15m
DOGE|no|40_50c|le_2c|10_15m
DOGE|no|80_90c|le_2c|5_10m
```

### DOGE Runtime State

After explicit approval and focused redeploy to active color `blue`, the running production process had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 53
live_executable_candidate_count 53
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

Approval was recorded with:

```text
gate_version btc15m-touch20-rules-gate-15m-DOGE-20260603205725-75a4ddddff87
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:21:53.638529+00:00
```



DOGE is live-approved and armed, but still selective. It may return
`no_candidate` until a fresh 15m market lands in an allowed replay bucket with
fresh spot, acceptable spread, enough time to close, and a rule score above
`0.30`.

## XRP Live Activation Record - 2026-06-03

XRP became live-ready after adding an asset-level time-bucket width option. The
gate threshold was not relaxed: XRP still needed at least 50 allowed replay
candidates, positive net P/L, positive P/L per candidate, acceptable touch/stop
rates, no terminal-loss breach, and no hard-cap breaches. The wider time bucket
is captured in replay metrics and status as `bucket_time_band_minutes=10`.

XRP is now live-approved after explicit operator approval, with
`trading_enabled=true` and max notional `10`.

### XRP Profile

The production XRP override is:

```text
CRYPTO_15M_TOUCH20_RULES_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP
XRP.rules_enabled=true
XRP.trading_enabled=true
XRP.allowed_sides=yes,no
XRP.take_profit_pct=0.10
XRP.stop_loss_pct=0.50
XRP.min_seconds_to_close=300
XRP.min_contract_price_dollars=0.10
XRP.max_contract_price_dollars=0.85
XRP.min_aligned_momentum=0.0
XRP.min_rule_score=0.30
XRP.bucket_time_band_minutes=10
XRP.replay_min_candidates=50
XRP.max_open_notional_dollars=10
XRP.daily_loss_limit_dollars=10
XRP.min_order_notional_dollars=5
```

### XRP Gate Evidence

The production replay and gate were persisted with:

```text
backtest_version btc15m-touch20-rules-backtest-15m-XRP-20260603212944-1a1598e979e3
gate_version btc15m-touch20-rules-gate-15m-XRP-20260603212952-7131a364a992
artifact_type xrp15m_touch20_rules_gate:15m:XRP
gate status passed
gate_candidate_scope allowed_replay_buckets
simulator_version live_exit_v3
bucket_price_band_cents 10
bucket_spread_band_cents 1
bucket_time_band_minutes 10
```

Allowed-bucket replay evidence:

```text
trade_candidate_count 528
allowed_trade_candidate_count 57
allowed_net_simulated_pl_dollars 2.409
allowed_pnl_per_candidate_dollars 0.04226315789473684
allowed_touch_rate 0.8421052631578947
allowed_stop_loss_rate 0.12280701754385964
allowed_terminal_loss_rate 0.017543859649122806
allowed_hard_cap_breaches 0
gate_reasons []
```

The full first-eligible universe was negative:

```text
net_simulated_pl_dollars -33.024
pnl_per_candidate_dollars -0.06254545454545454
touch_rate 0.6003787878787878
stop_loss_rate 0.38446969696969696
```

Allowed XRP buckets:

```text
XRP|no|40_50c|le_1c|5_15m
XRP|yes|70_80c|le_2c|5_15m
XRP|no|80_90c|le_1c|5_15m
```

The same profile with default 5-minute time buckets had only 47 allowed replay
candidates. The 10-minute XRP time-bucket mode groups adjacent `5_10m` and
`10_15m` evidence into `5_15m` while preserving side, price band, and spread
controls.

### XRP Runtime State

After explicit approval and focused redeploy to active color `blue`, the running production process
had:

```text
enabled true
trading_enabled true
gate status passed
gate sample_count 57
live_executable_candidate_count 57
live_blocked_allowed_bucket_keys []
approval_valid true
approval_reason operator_approval_valid
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

Approval was recorded with:

```text
gate_version btc15m-touch20-rules-gate-15m-XRP-20260603212952-7131a364a992
simulator_version live_exit_v3
max_notional_dollars 10.0000
approved_at 2026-06-03T23:21:59.297928+00:00
```

XRP is live-approved and armed, but still selective. It may return
`no_candidate` until a fresh 15m market lands in an allowed replay bucket with
fresh spot, acceptable spread, enough time to close, and a rule score above
`0.30`.

## Dry Run

Enable evaluation but keep trading disabled:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=false
```

Run one entry evaluation:

```bash
kalshi-bot-cli crypto-non-model-touch20 run-once \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

Expected safe statuses include:

- `disabled`
- `inactive_color`
- `kill_switch_enabled`
- `gate_blocked`
- `approval_blocked`
- `daily_loss_limit_blocked`
- `no_candidate`
- `strategy_cap_blocked`
- `min_order_notional_blocked`
- `trading_disabled`

Only `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true` allows entry order
submission.

## Tiny-Live

Before tiny-live:

1. Confirm active color and kill switch.
2. Confirm production write credentials are present.
3. Confirm the asset's 15m quote collection is current.
4. Confirm the asset spot rows are fresh and non-proxy.
5. Confirm the asset-owned gate, such as `btc15m_touch20_rules_gate:15m:BTC`, is
   passed with simulator version `live_exit_v3`.
6. Confirm the selected dry-run candidate is in an allowed replay bucket.
7. Confirm `live_bucket_controls.blocked_bucket_keys` does not include the
   selected candidate bucket.
8. Confirm the strategy ledger has no stale pending notional and no duplicate
   strategy entry for the selected market.
9. Confirm max strategy notional remains `$10` and daily loss limit remains
   `$10`; do not override the daily-loss block as part of this remediation.
10. Confirm the existing model-trained crypto bot remains unchanged.
11. Approve the latest gate with `crypto-non-model-touch20 approve`.

Then enable the current BTC live profile:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30
PRODUCTION_CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES=yes
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS=10
PRODUCTION_CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS=10
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS=5
```

For non-BTC lanes, run the same replay, gate, approve, status, run-once, and
exit-once commands with that asset symbol. Do not set a non-BTC lane's
`trading_enabled` override to `true` until its own gate and approval are valid.

Start or recreate the process:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d \
  crypto_non_model_btc15m_touch20_production
```

## Exit Behavior

Run one exit pass:

```bash
kalshi-bot-cli crypto-non-model-touch20 exit-once \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

The exit loop evaluates only strategy-ledger positions with the `b15t20r:`
prefix for BTC, or the asset lane's own prefix for non-BTC. It exits on:

- `take_profit`: net executable profit is at least +20%
- `stop_loss`: net executable profit is at or below -30% for the current BTC
  live profile
- `profit_protection_floor`: armed profit falls to 5% or lower
- `profit_protection_adverse_momentum`: armed profit is declining and spot
  momentum is adverse

Profit protection arms only after net executable profit first reaches +10%.
The stop loss is not a resting exchange order; it is evaluated by the dedicated
exit loop from current executable quotes.

## Ledger

Checkpoint stream:

```text
btc15m_touch20_rules:<kalshi_env>:BTC:15m
```

Non-BTC lanes use separate streams, for example:

```text
eth15m_touch20_rules:<kalshi_env>:ETH:15m
```

The ledger records:

- entry client order ID
- Kalshi order ID when available
- side
- count
- entry price and notional
- target exit price
- replay bucket
- gate version
- profit-protection state
- exit client order ID
- realized P/L when closed

Manual trades and model-bot trades can overlap the same Kalshi market, but they
must not be counted as strategy-owned unless they are in that asset lane's
ledger under that lane's order prefix.

## Monitoring

Watch these first:

- candidate funnel: market seen, quote valid, spot fresh, spread pass,
  entry-window pass, replay-bucket pass, selected, submitted, filled
- gate health: candidate count, touch rate, net P/L, P/L per candidate,
  allowed bucket count, blocked bucket count, exit reason counts, stop-loss
  rate, terminal-loss rate, simulator version
- trading quality: entry spread, exit spread, slippage, fill latency, partial
  fills, rejected orders, stale quote skips
- P/L attribution: strategy-only realized/unrealized P/L, take-profit exits,
  profit-protection exits, settlement holds
- risk: strategy open notional, pending notional, daily loss, cap blocks, live
  bucket blocks, duplicate-market skips, cooldown skips, overlap with model-bot
  positions
- market regime: asset spot volatility, short-term momentum, distance to target,
  liquidity by price band, time-to-close bucket

Ops events are logged with source:

```text
crypto_non_model_btc15m_touch20
```

## Rollback

The fastest safe rollback is to disable entries:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=false
```

For non-BTC lanes, set that asset's `trading_enabled` override to `false`.

Keep the process running if it owns open positions, because the exit loop is the
strategy-specific take-profit, stop-loss, and profit-protection path. If the
process itself must be stopped, manually inspect and manage any open
strategy-prefixed positions first, such as `b15t20r:` for BTC.

For a hard stop of new evaluation:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=false
```

Global kill switch also blocks entries and still allows risk-reducing exits.

## Important Caveats

- This is a tiny-live path, not a broad 15m crypto replacement.
- The replay gate is necessary, not sufficient. Live spread and fill quality can
  differ from historical quote-path snapshots.
- The ledger is strategy-local. If an exchange fill happens after initial order
  submission, reconciliation must keep the strategy ledger accurate before
  sizing up.
- The stop loss only works when the process has a fresh quote and can submit a
  risk-reducing close; it is not a guaranteed exchange-side stop.
