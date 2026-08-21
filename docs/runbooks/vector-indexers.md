# Vector — indexers runbook

Owner: vector team · Source rules: [alerts/vector-indexers.yaml](../../alerts/vector-indexers.yaml)

The Vector indexers (`morpho-indexer`, `oracle-price-worker`) are the last
hop in the pipeline before customer-facing data lands in TimescaleDB. They
consume blocks from the watcher and derive product state (positions,
prices). A stall here directly translates to stale downstream reads.

---

## VectorMorphoIndexerStalled / VectorOracleIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

The labelled indexer on the labelled `chain` has processed zero blocks for
15 minutes. Downstream TimescaleDB state for Morpho positions / oracle
prices is going stale.

The morpho rule keys on `status="success"`, not on the total (VEC-218). The
morpho queue is FIFO: a block that fails blocks its message group, and every SQS
redelivery of that one poison message keeps `morpho_blocks_processed_total`
advancing with `status="error"` while nothing is persisted. At
visibility-timeout cadence that redelivery rate also stays under
`VectorMorphoIndexerErrorsHigh`'s 0.1/s, so a total-based rule could let the
whole chain's Morpho data freeze silently. Keying on successes closes that.
Consequence when it fires: **an all-error loop looks identical to a dead
consumer here** — check the logs before assuming the pod is wedged.

That success rate is zero-filled from the bare total
(`success-rate or total-rate * 0`). A pod restarting straight into the poison
pill emits only `status="error"` samples, so the `status="success"` series never
exists and a bare `== 0` would match nothing — the firing alert would RESOLVE on
every restart while the data stayed frozen. The total, which the redelivery loop
keeps advancing, supplies the 0 the success series is missing.

Residual: a live pod whose OTLP export dies lets **both** series
staleness-expire, so `== 0` matches nothing and this rule goes silent. morpho
has no kube-state-metrics `Down` companion yet (unlike maple / fluid-vault /
allocation-tracker); adding one is the follow-up that closes this.

### First checks (≤5 min)

1. **Pod status** — run the one matching the firing alert:
   - Morpho: `kubectl -n vector get pods -l app=morpho-indexer`
   - Oracle: `kubectl -n vector get pods -l 'app in (oracle-price-worker,avalanche-oracle-price-worker,base-oracle-price-worker)'`
     — one Deployment per chain, each with its own `app` label; the bare
     `app=oracle-price-worker` selector matches mainnet pods only.
2. **Recent logs** — look for decode panics, DB connection errors, or
   `context deadline exceeded` against the watcher's archive RPC.
3. **Upstream lag** — confirm the watcher is producing for this chain (if
   not, fix that first — that's `VectorWatcherNoBlocks`).
4. **TimescaleDB health** — connection pool exhaustion or replication lag
   can stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade → add the new
  ABI / decoder and redeploy.
- DB connection pool saturated → restart the pod; longer-term raise the
  pool limit.
- Watcher archive RPC slow → see `VectorMorphoIndexerRPCLatencyHigh`.

### Verify recovery

`rate(morpho_blocks_processed_total{status="success"}[5m]) > 0` /
`rate(oracle_blocks_processed_total[5m]) > 0` for the affected chain.

---

## VectorMorphoIndexerErrorsHigh / VectorOracleIndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

The labelled indexer is logging more than 0.1 errors/sec sustained for
15 minutes. Investigate before it escalates to a full stall.

### First checks

- Inspect recent logs for the dominant error class.
- Correlate with recent deploys (`kubectl rollout history`).
- Check for chain reorgs in the watcher logs — indexers may need to roll
  back state.

---

## VectorMorphoIndexerRPCLatencyHigh / VectorOracleIndexerRPCLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 RPC request latency from the indexer (to the watcher's archive RPC) is
over 3 seconds sustained for 15 minutes. The indexer is degraded; expect
downstream lag.

### First checks

- Watcher RPC pod health and CPU usage.
- Network latency between the indexer node group and the watcher service.
- Recent changes in batch size / pagination defaults.

---

## VectorOracleUnitStale

**Severity:** warning · **For:** 5m (on 30m staleness)

### What it means

The oracle-price-worker on the labelled `chain` is still consuming blocks,
but the single oracle unit `oracle_name` has not completed a successful
processing pass for over 30 minutes. Prices for that unit's tokens in
TimescaleDB are going stale while every whole-worker signal (Stalled,
ErrorsHigh) can look healthy.

The gauge `oracle_unit_last_success_timestamp_seconds` advances after every
successful per-unit pass **whether or not any row was written** (writes are
change-only), so this alert means "the worker stopped successfully
processing this unit", not "the upstream feed stopped updating". Slow
heartbeat feeds (daily NAV, e.g. JTRSY, JAAA, STAC, BUIDL-I) do not fire
this when they are merely quiet. The gauge is baselined per unit at worker
startup, so a unit that has never succeeded since the last deploy fires
roughly 30 minutes after startup, and a pod restart re-arms the alert
rather than resolving it. Note the alert does not cover SQS backlog lag: a
worker grinding through a deep backlog reads fresh while DB prices lag;
check queue depth for that.

### First checks (≤5 min)

1. **Per-unit errors in the logs**: a unit hard-erroring on every block is
   the most likely cause and is logged per pass. Pick the deployment
   matching the alert's `chain` label:
   - `chain="mainnet"`:
     `kubectl -n vector logs deploy/oracle-price-worker | grep "failed to process oracle"`
   - `chain="avalanche-c"`:
     `kubectl -n vector logs deploy/avalanche-oracle-price-worker | grep "failed to process oracle"`
   - `chain="base"`:
     `kubectl -n vector logs deploy/base-oracle-price-worker | grep "failed to process oracle"`

   The `oracle` field names the unit. **If the grep comes back empty**, grep
   the same logs for `"failed to process message"` instead: the failure is
   then before the per-unit loop (feed-decimals validation or block-timestamp
   resolution), which errors every block without reaching any unit, so the
   per-unit log line never appears. In that scenario all of the chain's units
   fire together, which is itself diagnostic.
2. **Registry state**: after a recent migration, confirm the unit's oracle
   row and its assets/feeds are still sane: enabled flags, feed addresses,
   decimals, oracle type. A misconfigured unit fails every pass with the
   same error.
3. **Upstream RPC**: if the errors are timeouts/429s, check
   `VectorRPCRetryRatioHigh` and `VectorOracleIndexerRPCLatencyHigh` for the
   same window; a degraded RPC can starve one expensive unit while cheaper
   units keep succeeding.
4. **Cross-check the fetched counter**
   (`rate(oracle_unit_prices_fetched_total{oracle_name="..."}[15m])`):
   - Fresh gauge + zero fetched: every feed in the unit is reverting
     (guard-skipped, not errored); this alert stays quiet by design and
     `VectorOracleUnitReadsFailing` (below) is the alert for that mode.
   - Stale gauge + nonzero fetched: fetch works but the pass fails after it
     (change detection or the DB upsert); check upsert errors in the logs.

### Common causes

- Unit hard-erroring every block after a contract / feed upgrade: fix the
  feed address or ABI handling and redeploy.
- Registry misconfig introduced by a migration (wrong feed address, wrong
  oracle type, disabled asset set): correct the registry rows; the worker
  reloads units on restart.
- Upstream RPC degradation making the unit's multicall persistently fail:
  see the RPC alerts above.

### Verify recovery

`time() - oracle_unit_last_success_timestamp_seconds{oracle_name="..."}`
drops back under a few minutes for the affected unit, and
`rate(oracle_unit_prices_fetched_total{oracle_name="..."}[15m]) > 0`.

---

## VectorOracleUnitReadsFailing

**Severity:** warning · **For:** 10m (on a 30m failed-read rate)

### What it means

The worker is healthy and the unit `oracle_name` completes every pass, but
at least one of the unit's configured price reads has produced no usable
price on nearly every block for over 30 minutes: a feed reverting on every
call (guard-skipped by design, so nothing errors) or the Aave oracle
answering a zero quote. Prices for the affected token(s) are frozen in
TimescaleDB while the worker, the unit's freshness gauge, and every
whole-worker signal look green.

The counter `oracle_unit_reads_failed_total` accumulates, per pass, the
pass's read count minus the usable prices it fetched. The alert fires when
the unit's failed-read rate exceeds 0.9x the chain's block rate, i.e. at
least one read failing on ~every block. For feed and Aave units it covers
both total loss (every read failing, fetched rate zero) and partial loss
(one dark feed inside an otherwise healthy multi-feed unit); erc4626 units
land here only on partial vault loss. `VectorOracleUnitStale` stays quiet
in all of these because the pass itself still succeeds. Losing ALL erc4626
vaults at once, and any failed sub-read of a curve LP unit, hard-error the
pass instead: those page `VectorOracleUnitStale`, never this alert.

### First checks (≤5 min)

1. **Identify the failing read in the logs**: pick the deployment for the
   alert's `chain` label exactly as in `VectorOracleUnitStale` step 1, then
   grep for `"feed call failed"` (warn per failed feed, carries `tokenID`,
   `feedAddress`, `block`). `"all feeds failed, check configuration"` at
   error level means total loss. For an Aave-type unit grep for
   `"skipping unpriceable asset"` instead (a zero quote, carries `tokenID`).
2. **Check the feed contract on-chain**: `cast call <feedAddress>
   "latestRoundData()" --rpc-url <chain rpc>` at head. A deprecated or
   migrated Chainlink aggregator reverts forever; that is the classic cause.
3. **Registry state**: after a recent migration, confirm the unit's feed
   address, decimals, and quote currency against the token it should price;
   a wrong feed address reads as a permanently reverting feed.
4. **Quantify the loss**: compare
   `rate(oracle_unit_reads_failed_total{oracle_name="..."}[30m])` with
   `rate(oracle_unit_prices_fetched_total{oracle_name="..."}[30m])`. Failed
   rate ≈ the chain's block rate means exactly one dark feed; fetched rate
   zero means every read of the unit is failing.

### Common causes

- Deprecated / migrated upstream feed (proxy repointed, aggregator turned
  off) reverting on every call: update the registry row to the successor
  feed and restart the worker.
- Registry misconfig introduced by a migration (wrong feed address, wrong
  decimals): correct the registry rows; the worker reloads units on
  restart.
- An Aave-oracle asset answering a zero quote (the `detectChanges` zero
  guard's safety-net case; a source-less asset with a zero fallback makes
  the AaveOracle revert the whole batch instead, which pages
  `VectorOracleUnitStale`): remove the asset from the unit or restore its
  price source.

### Verify recovery

`rate(oracle_unit_reads_failed_total{oracle_name="..."}[30m])` drops back
to ~0 for the affected unit and the fetched rate returns to its
pre-incident level.

---

## maple-graphql-indexer (VEC-320)

Unlike morpho/oracle, `maple-graphql-indexer` is **not** a block consumer. It
is a Temporal cron (default 10m, `MAPLE_SYNC_INTERVAL`) that snapshots the
public Maple GraphQL API (`api.maple.finance/v2/graphql`) into TimescaleDB:
pools, active open-term loans + collateral, Sky strategies, Syrup globals.
There are no blocks and no RPC — the upstream is the Maple API, and "stale"
means the borrower-risk snapshot tables stop advancing. Single replica;
mainnet-only (`chain="ethereum"`).

Phases run per cycle, each in its own transaction: `pools` → `loans` →
`sky_strategies` → `syrup_globals`. A failed `pools` phase skips `loans` and
`sky_strategies` (they depend on the pool registry).

---

## VectorMapleIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

No sync cycle has completed for >20m (two missed 10m intervals).
`maple_sync_cycles_total` (incremented on both success and error) is flat, so
the Maple snapshot tables are going stale.

### First checks (≤5 min)

1. **Is `VectorMapleIndexerDown` also firing?**
   - **Yes** → the pod is not running; this is a process death. Go to that
     runbook section — it's the root cause.
   - **No** → the pod is alive but not producing. Two likely causes:
     **(a)** the cron worker is wedged, or **(b)** the OTLP/metrics pipeline
     broke and the indexer is actually fine (this shape false-fired in
     VEC-360). Disambiguate with the next two steps.
2. **Pod + logs** — `kubectl -n vector logs -l app=maple-graphql-indexer --tail=100`.
   Healthy cycles log `pools synced` / `loans synced` / `syrup globals synced`
   every ~10m. If those lines are recent, the indexer is fine and the metrics
   pipeline is the problem (check the OTel collector in `observability`).
3. **Temporal** — confirm the schedule is firing: workflow IDs
   `scheduled-maple-graphql-indexer-<ts>` should appear each interval. If the
   worker lost its Temporal connection, restart the pod.
4. **Upstream** — `curl -s -XPOST https://api.maple.finance/v2/graphql` reachable?
   A hard Maple outage stalls every cycle (cycles still *run*, so this presents
   as cycle/phase errors too).

### Common causes

- OTLP export broken, indexer healthy (VEC-360) → fix the metrics pipeline, not
  the cron; logs are the source of truth.
- Temporal worker wedged / lost connection → restart the pod.
- Sustained Maple API outage → cycles fail; see `VectorMaplePhaseErrors`.

### Verify recovery

`rate(maple_sync_cycles_total[20m]) > 0`, and a fresh `synced_at` lands:
`SELECT max(synced_at) FROM maple_pool_state;` within the last interval.

---

## VectorMapleIndexerDown

**Severity:** critical · **For:** 10m

### What it means

The `maple-graphql-indexer` Deployment has <1 available replica for >10m
(kube-state-metrics, independent of the indexer's own OTLP export). The cron
worker is not running; no snapshots are taken. If `VectorMapleIndexerStalled`
is firing too, **this is the root cause.**

### First checks (≤5 min)

1. `kubectl -n vector get deploy,pods -l app=maple-graphql-indexer` — pod state
   and restart count.
2. **Not Running?** describe for the reason:
   `kubectl -n vector describe pod -l app=maple-graphql-indexer | sed -n '/Events:/,$p'`
   - `CreateContainerConfigError` / `CreateContainerError` → missing config or
     secret; check the `maple-graphql-indexer` ConfigMap and ExternalSecret.
   - `CrashLoopBackOff` → read logs; common: bad `DATABASE_URL`, unreachable
     TimescaleDB, or a startup panic.
   - `Pending` / `FailedScheduling` → no node capacity; check the node group.
3. **ExternalSecret synced?**
   `kubectl -n vector get externalsecret maple-graphql-indexer` — a failed sync
   leaves the pod unable to start.

### Common causes

- Missing/failed ExternalSecret (DB creds) → fix the secret, pod recovers.
- Crash on startup (DB unreachable, config) → fix the dependency.
- Node scaled away with no rescheduling capacity → check the node group /
  autoscaler.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="maple-graphql-indexer"} == 1`
and cycles resume (see Stalled recovery check).

---

## VectorMapleIndexerCycleErrors / VectorMaplePhaseErrors

**Severity:** warning · **For:** 0m (the 30m window debounces)

### What it means

Cycles are running but failing. `CycleErrors` is the rollup (also catches
pre-phase setup failures: protocol seed, DB connection); `PhaseErrors` names
the failing `phase` and is the actionable one. Per-phase transactions mean
surviving phases still commit — data is **partial, not absent**, so this warns
rather than pages.

### First checks

1. **Which phase** — `sum by (phase) (increase(maple_sync_phases_total{status="error"}[30m]))`.
2. **Logs** for that phase — `kubectl -n vector logs -l app=maple-graphql-indexer`.
   Errors name the owning entity (e.g. `pool 0x… : …`).
3. **Cascade check** — a failing `pools` phase deliberately skips `loans` and
   `sky_strategies`; fix `pools` first.

### Common causes

- Maple API returning Apollo `errors[]`, 429/5xx, or unstable pagination
  (duplicate IDs across pages) → usually transient; confirm it clears next cycle.
- A string-encoded integer field the client can't parse → the phase fails hard
  by design (never silently skips rows); needs a client fix.
- DB write failure (FK, constraint) → inspect the named entity.

---

## VectorMaplePoolWritesZero

**Severity:** warning · **For:** 15m

### What it means

Cycles are running but the `pools` phase wrote 0 rows over 20m. There are
always ~21 PoolV2 pools, so 0 means Maple's `poolV2S` query returned an empty
(but HTTP 200, no-error) collection — a silent upstream regression the error
alerts do **not** catch. (This is gated on "cycling AND zero" so it doesn't
just duplicate `Stalled`.)

### First checks

- Query the API directly for `poolV2S` and confirm it returns pools.
- Check `maple-graphql-client` logs for `fetched paginated collection
  collection=pools total_rows=0`.
- If Maple changed the query shape/field names, the client needs updating.

> Note: do **not** expect an equivalent for loans — the service legitimately
> records 0 loan rows when there are no active loans, so loan-zero is normal.

---

## VectorMapleFTLBookActive

**Severity:** info · **For:** 0m (1h window)

### What it means

The `fixed_term_loans` phase wrote > 0 rows to `maple_ftl_loan_state` in 1h.
The FTL book has been dormant (0 live fixed-term loans), so the steady state is
0 rows and there is intentionally no zero-rows alert (it would fire constantly).
A nonzero write is the inverse signal: Maple reactivated the fixed-term-loan
product and the indexer is now capturing it.

### First checks

- `SELECT COUNT(*), MAX(synced_at) FROM maple_ftl_loan_state;` — confirm rows
  are landing and current.
- Check `maple-graphql-indexer` logs for `fixed-term loans synced count=<n>`.
- Spot-check a row against the Maple API (`loans` query) for the same loan id:
  state, `interestRate` scale (6-decimal on live PoolV2), collateral/funds token
  resolution.

### Action

Confirm the FTL path end-to-end, then add the data-quality alerts that only
make sense once the book is live — most importantly an FTL silent-empty alert
analogous to `VectorMaplePoolWritesZero` (cycling AND zero), so a later silent
drop back to `[]` is caught. Until then this info alert is the only FTL
data-quality signal.

---

## VectorMapleSchemaDrift

**Severity:** warning · **For:** 0m (1h window debounces)

### What it means

A field Maple normally populates (`pool_monthly_apy`, `pool_spot_apy`,
`strategy_fee_rate`, `strategy_total_fees_collected`) was null-downgraded to
SQL NULL repeatedly (>5/1h). Known-nullable fields (`loan_acm_ratio`,
`pool_tvl`, `pool_collateral_value_usd`, `syrup_drips_yield_boost`) are
excluded. A sustained count signals a Maple GraphQL API schema change, not a
code bug.

### First checks

- `sum by (field) (increase(maple_sync_null_downgrades_total[1h]))` — which
  field(s).
- Cross-check the field against Maple's published SDL / docs.
- Inspect rows: `SELECT * FROM maple_pool_state WHERE <field> IS NULL ORDER BY
  synced_at DESC LIMIT 20;` — confirm the API, not our parsing, dropped it.

### Action

If Maple intentionally made the field nullable, move it to the excluded set and
re-baseline (ORB-145). If it's an upstream regression, raise with Maple and
decide whether downstream consumers tolerate the gap. The allowlist and the
`>5/1h` threshold are pre-baseline estimates — tune off a few days of data.

---

## VectorMapleCollateralUnpriceable

**Severity:** warning · **For:** 90m (20m lookback window)

### What it means

A loan's collateral USD price came back null in a non-pending state. Maple's
oracle layer had no fiat feed for the token at query time, so its API returned
HTTP 200 with a top-level `errors[]` entry `No fiat value for <TOKEN>` scoped to
the collateral node. The client tolerates that specific case: it keeps the rest
of the loan book and persists the offending `asset_value_usd` as SQL NULL
(metric `reason="unpriceable"`), rather than discarding the whole cycle's
snapshot. `reason="pending"` (collateral still `DepositPending`, no price yet)
is normal and is **not** alerted.

This is expected to self-heal — Maple's pricing layer restores the feed and the
next 10m cycle writes a real value. The alert detects **persistence, not
volume**: `increase[20m] > 0` must hold continuously for `90m`, per token.

The two durations compose rather than acting independently. The 20m lookback
keeps the expression true for 20m *after* the last bad cycle, so the recurrence
itself only needs to span `90m - 20m` — i.e. the alert fires when a token keeps
coming back unpriceable for **>70m**.

That does **not** mean every cycle must fail. Because the lookback is 20m and
cycles run every 10m, failing every *other* cycle already leaves no window empty
— so the practical bar is **≥4 bad cycles, none more than 20m apart**. One clean
cycle in between is tolerated; two in a row resets the clock. Shorter gaps, even
multi-cycle ones, self-heal and never fire. Anything that clears that bar is an
upstream Maple pricing problem, not our bug. The client no longer emits a
per-occurrence warn; the metric is the signal.

### First checks

- `sum by (token) (increase(maple_sync_null_downgrades_total{reason="unpriceable"}[1h]))`
  — which token(s), and whether it's a single blip or sustained.
- Inspect rows: `SELECT l.loan_address, c.asset, c.state, c.synced_at FROM
  maple_loan_collateral c JOIN maple_loan l ON l.id = c.maple_loan_id WHERE
  c.asset_value_usd IS NULL AND c.state <> 'DepositPending' ORDER BY c.synced_at
  DESC LIMIT 20;` — confirm the price, not the whole loan, is what dropped.

### Action

- **Fires once, then resolves within ~20m:** Maple's feed came back. Note that
  it still means the gap persisted >70m before firing, so record the token and
  duration, but no code action beyond the one-time task below.
- **Sustained (fires across many cycles):** upstream Maple pricing gap. Raise
  with Maple; decide whether downstream consumers tolerate the NULL. Not a code
  bug.

### Confirmed shape (baseline)

First observed live in staging, 2026-07, on open-term-loan collateral
(HYPE, PYUSD, USDG, cbBTC). The captured `errors[]` matched the client's
assumptions exactly:

- `message`: `No fiat value for <TOKEN>`
- `path`: `[openTermLoans collateral assetValueUsd]` (through a `collateral` segment)
- `extensions.code`: `INTERNAL_SERVER_ERROR`
- partial `data` present

So the classifier (`tolerableUnpriceableCollateral` / `pathThroughCollateral`)
classified it correctly; the temporary diagnostic warn has been removed and the
alert re-baselined from a raw `>0` to a persistence signal
(`increase[20m] > 0` for `90m`, per token).

> Known gap (follow-up): the metric is only recorded when `collateral` is
> non-null (service.go). If a "No fiat value" error nulls the **whole**
> `collateral` node (not just `assetValueUsd`), the loan is kept with no
> collateral row and no downgrade metric — so this alert cannot see it. Not
> observed live (the live path is field-level `...collateral assetValueUsd`), but
> track it separately if Maple's SDL ever makes `assetValueUsd` non-nullable.

If a future occurrence does **not** match this shape (path outside collateral,
or extensions reveal a different failure), the classifier needs tightening — fix
it in `stl-verify/internal/adapters/outbound/maple/client.go` so that shape
stays fatal rather than being swallowed as a price gap.

---

## VectorMaplePhaseLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

A phase p95 duration exceeded 30s over 15m. Normal p95 is seconds (loans ~4s,
others <1s) and is essentially Maple API response time — there is no RPC here.
30s is generous for ~150 rows/cycle; sustained latency this high means the
Maple API is degraded and a phase risks overrunning the 10m interval.

### First checks

- `histogram_quantile(0.95, sum by (phase, le) (rate(maple_sync_phase_duration_seconds_bucket[15m])))`
  — which phase.
- Maple API status / response times (curl a representative query).
- If it's the `loans` phase, pagination volume may have grown; check
  `total_rows` in the client logs.

---

## fluid-vault-indexer (VEC-438)

`fluid-vault-indexer` consumes Ethereum BlockEvents, reads end-of-block Fluid
(Instadapp) vault state from the VaultResolver via Multicall3, and appends
`fluid_vault_state` snapshots into TimescaleDB. Mainnet (chain 1) only.

**Metric coverage:** the service emits **no service-level counters/histograms of
its own** today. These alerts use the signals that genuinely exist:
`kube_deployment_status_replicas_available` (process liveness, independent of the
OTel pipeline), `multicall_batch_size_count{service_name="fluid-vault-indexer"}`
(advances on startup reconcile + every block touching a known vault), and the
shared `VectorArchiving*` rules (raw-SC-call archive health, keyed by
`service_name`). Error-rate, silent-empty (rows-written == 0), and RPC-latency
alerts are intentionally absent — no metric would make them fire honestly, so
they are omitted rather than shipped as rules that can never fire. Adding
`fluid_blocks_processed_total` / `fluid_errors_total` / a rows-written counter /
an RPC-latency histogram to the B2 service is a follow-up instrumentation task;
grow the rules + these sections when those land.

---

## VectorFluidVaultIndexerDown

**Severity:** critical · **For:** 10m

### What it means

The `fluid-vault-indexer` Deployment has <1 available replica for 10 minutes. No
pod is running, so no Fluid vault snapshots are written and the SQS backlog is
growing. This is the keystone freshness signal for a service with no internal
block-progress metric.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=fluid-vault-indexer`.
2. **Why it's not ready** — `kubectl -n vector describe deployment/fluid-vault-indexer`
   and `kubectl -n vector logs -l app=fluid-vault-indexer --previous` for a crash
   loop (missing queue URL, DB/Redis/RPC dial failure, bad ABI load).
3. **Secrets/config present** — the worker requires `AWS_SQS_QUEUE_URL`,
   `DATABASE_URL`, `ALCHEMY_API_KEY`, `REDIS_ADDR`, `S3_BUCKET`. A missing key
   from the `fluid-vault-indexer` ExternalSecret crashes it on startup. The queue
   URL is the `ethereum_sqs_fluid_vault_url` property of `stl-<env>-infra-config`
   (created by infra VEC-439).
4. **Node/scheduling** — pending pod => check node capacity / taints.

### Common causes

- ExternalSecret not yet synced (queue URL / DB URL missing) — the deploy ran
  before the infra apply. Verify VEC-439 applied; re-sync the ExternalSecret.
- Crash loop on a startup error (DB/Redis/RPC unreachable) — fix the dependency;
  the worker is fail-fast by design.
- OOMKilled — check memory limits.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="fluid-vault-indexer"} >= 1`
and the pod logs show `fluid vault indexer started, waiting for messages...`.

---

## VectorFluidVaultIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

The worker is **up** (≥1 replica) but has consumed **no block for 15 minutes**:
`rate(blocks_processed_total{service_name="fluid-vault-indexer"}[5m])` is zero —
or the series has **vanished entirely** (the expression zero-fills from
kube-state-metrics, so a dead OTLP export with a live pod still fires; other
OTel series from the pod will be flat/absent too). The worker consumes ~1
`BlockEvent` per mainnet block (~12s) and records one `blocks_processed_total`
sample per consumed block regardless of whether a vault was touched, so this is
**not** a quiet vault period — it means the SQS consume loop is wedged (process
alive but not processing) or the OTLP metric export broke (worker fine, metrics
stopped). Either way the counter is blind and possibly no Fluid vault state is
being written. This replaced the old multicall-activity expression, which could
not distinguish a genuinely quiet vault from a wedged loop.

### First checks (≤5 min)

1. **Distinguish the cases** — is `VectorFluidVaultIndexerDown` also firing? If
   so the process is down (treat as Down; this rule is replica-gated and should
   resolve). If not, the pod is alive — it is a wedged loop or a dead metrics
   export, both of which need action.
2. **Recent logs** — `kubectl -n vector logs -l app=fluid-vault-indexer --tail=200`.
   Look for a repeating error on one message, `context deadline exceeded` against
   the Alchemy RPC, or silence (poll loop stopped).
3. **SQS backlog** — check the `stl-<env>-ethereum-fluid_vault.fifo` queue depth.
   Growing `ApproximateNumberOfMessages` while the counter is flat confirms the
   loop is wedged (not draining).
4. **OTLP export** — if logs show blocks still being processed but the counter is
   flat, the metrics pipeline is the problem, not the worker; check the OTel
   collector and other series from the pod (they will be flat too).
5. **Upstream** — confirm the ethereum watcher is still producing blocks; if not,
   that's the root cause (`VectorWatcherNoBlocks`) and the queue is legitimately
   empty.

### Common causes

- Poison message wedging the poll loop — inspect the DLQ; redrive or purge the
  offending message.
- Alchemy RPC degraded / rate-limited — per-block reads time out; check logs and
  the Alchemy status.
- Broken OTLP export — worker is processing but metrics stopped flowing; restart
  the pod or fix the collector.

### Verify recovery

`rate(blocks_processed_total{service_name="fluid-vault-indexer"}[5m]) > 0`, or
confirm the SQS backlog is draining and recent logs show blocks being processed.

---

## VectorCexOrderbookPersistFailing

**Severity:** critical · **For:** 15m

### What it means

Every snapshot write for the labelled `exchange` has failed continuously for
>15 minutes. The WebSocket is probably still up (books are fresh in memory) but
nothing is reaching TimescaleDB — a silent data hole. Because the indexer drops
failed ticks by design, it will not recover on its own from a permanent cause.

### First checks (≤5 min)

1. **Pod status & logs** — `kubectl -n vector logs -l app=cex-orderbook-indexer-<exchange> --tail=200`.
   Look for the `failed to persist order book snapshots` error and its cause.
2. **Classify the cause from the error:**
   - `password authentication failed` / permission denied → DB credential or
     grant problem; fix the secret/role and restart.
   - `relation "cex_orderbook_snapshots" does not exist` → the migration did not
     run in this environment; run the migrate job.
   - `timeout` / `too many connections` / pool exhausted → DB under load or pool
     too small; check the Postgres dashboard.

### Verify recovery

`rate(orderbook_persist_failures_total{exchange="<exchange>"}[10m]) == 0` and
fresh rows: `SELECT max(persisted_at) FROM cex_orderbook_snapshots WHERE exchange = '<exchange>'`.

---

## VectorCexOrderbookStreamStalled

**Severity:** critical · **For:** 10m

### What it means

The oldest symbol on the labelled `exchange` has had no order book update for
>120s sustained over 10m. Snapshots are stale and stale symbols stop being
written, so the series flat-lines.

Since the 90s stale watchdog (VEC-542), a whole connection going silently dead
self-heals by reconnect before this alert's 120s threshold — that mode now
surfaces as `VectorCexOrderbookStaleFeedChurn` instead. If this alert fires
anyway, suspect the modes the watchdog cannot fix: a single symbol dead while
the rest of its connection keeps updating (the watchdog is per-connection), a
consumer wedged after the update is received (age gauge climbs while the
socket is fine), or the watchdog itself broken.

### First checks (≤5 min)

1. **Pod logs** — `kubectl -n vector logs -l app=cex-orderbook-indexer-<exchange> --tail=200`.
   Look for reconnect churn (`orderbook.reconnections.total`) or
   `skipping stale order books`.
2. **Exchange status** — check the venue's status page / API health; an outage
   or a symbol delisting stops updates.
3. **Symbol config** — a bad/renamed symbol can wedge the feed (e.g. Kraken
   `XBT`/`XDG` aliasing); confirm `SYMBOLS` matches the venue's current pairs.
4. **Network egress** — confirm the pod can reach the exchange WebSocket.

### Verify recovery

`max(orderbook_last_update_age_seconds{exchange="<exchange>"}) < 120` and
`rate(orderbook_updates_emitted_total{exchange="<exchange>"}[5m]) > 0`.

---

## VectorCexOrderbookDown

**Severity:** critical · **For:** 10m

### What it means

The labelled `deployment` has <1 available replica for >10m (kube-state-metrics,
independent of the pod's own OTLP export). The order book indexer is not
running, so no snapshots are taken. This is the availability companion to
`VectorCexOrderbookStreamStalled`: that one reads the pod's own
`orderbook_last_update_age_seconds` gauge, which vanishes on a pod/exporter outage — so
`Down` catches the total-outage case `StreamStalled` cannot. If both fire,
**this is the root cause.**

### First checks (≤5 min)

1. `kubectl -n vector get deploy,pods -l app=<deployment>` — pod state and
   restart count.
2. **Not Running?** describe for the reason:
   `kubectl -n vector describe pod -l app=<deployment> | sed -n '/Events:/,$p'`
   - `CreateContainerConfigError` → missing config/secret; check the
     `<deployment>-config` ConfigMap and its ExternalSecret (DB URL).
   - `CrashLoopBackOff` → read logs; common: bad `DATABASE_URL`, unreachable
     TimescaleDB, or a startup panic (e.g. unknown `EXCHANGE`).
   - `Pending` / `FailedScheduling` → no node capacity; check the node group.
3. **ExternalSecret synced?**
   `kubectl -n vector get externalsecret <deployment>` — a failed sync leaves
   the pod unable to start.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="<deployment>"} == 1` and
updates resume (see StreamStalled recovery check).

---

## VectorCexOrderbookPersistLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 latency of a snapshot batch write to TimescaleDB exceeded 1s over 10m. A
top-N JSONB insert is normally single-digit ms, so this means the DB or the
connection pool is degraded. The risk: a write slower than the snapshot interval
(default 5s) makes ticks pile up and drop — this is the precursor to
`VectorCexOrderbookPersistFailing`, not yet an outage.

### First checks (≤5 min)

1. **Which exchange/pod** — `histogram_quantile(0.99, sum by (exchange, le) (rate(orderbook_persist_duration_seconds_bucket[10m])))`.
2. **TimescaleDB health** — connection pool saturation, CPU, lock contention, or
   replication lag on the Postgres dashboard. Latency here is almost always
   downstream DB pressure, not the indexer.
3. **Correlate** — is another heavy writer (a backfill, another indexer) loading
   the same DB right now?

### Common causes

- DB pool saturated / under load → restart is a stopgap; longer-term raise the
  pool limit or the DB instance size.
- A slow/locking migration or compaction job running concurrently.

### Verify recovery

`histogram_quantile(0.99, sum by (exchange, le) (rate(orderbook_persist_duration_seconds_bucket[10m]))) < 1`.

---

## VectorCexOrderbookStaleFeedChurn

**Severity:** warning · **For:** 15m

### What it means

The stale-feed watchdog (VEC-542) on the labelled `exchange` keeps closing
connections that stay open but deliver no book updates for 90s, at a sustained
rate (>3 `reason="stale_feed"` reconnects per 10m, held 15m). One venue
restart-drain causes a single stale_feed close per connection and does not
fire; this firing means every reconnect lands on another dead feed. Because
the watchdog resets `orderbook_last_update_age_seconds` at ~90s,
`VectorCexOrderbookStreamStalled` (120s threshold) stays green through this —
the alerts are complementary, and data continuity is degrading on every
recycle even though nothing pages.

### First checks (≤5 min)

1. **Pod logs** — `kubectl -n vector logs -l app=cex-orderbook-indexer-<exchange> --tail=200`.
   Look for the repeating stale-feed close/reconnect cycle and whether any
   updates arrive at all between reconnects.
2. **Exchange status** — check the venue's status page; a degraded matching
   engine or WebSocket incident (e.g. a rolling restart drain that never
   completes) keeps sockets alive but silent.
3. **Symbol config** — a bad/renamed symbol can make subscriptions succeed but
   never produce data (e.g. Kraken `XBT`/`XDG` aliasing); confirm `SYMBOLS`
   matches the venue's current pairs.
4. **Scope** — is `orderbook_updates_emitted_total{exchange="<exchange>"}`
   zero (whole feed dead) or merely reduced (one connection of several
   recycling)?

### Common causes

- Venue-side incident: sockets accepted and kept alive (heartbeats/pongs) but
  no book data — wait it out or fail over; the watchdog is already retrying.
- Subscription no longer effective after the venue changed its protocol or
  renamed a pair — reconnects "succeed" but subscribe to nothing.

### Verify recovery

`sum by (exchange) (increase(orderbook_reconnections_total{reason="stale_feed", exchange="<exchange>"}[10m])) == 0`
and `rate(orderbook_updates_emitted_total{exchange="<exchange>"}[5m]) > 0`.

---

## VectorRPCRetryRatioHigh

**Severity:** warning · **For:** 15m

### What it means

For the labelled `service_name` and `server_address` (RPC host), more than 20%
of RPC attempts over the last 10m were retries, measured by the shared
`internal/pkg/rpchttp` retry transport (used by every `DialEthereum` caller:
oracle / morpho / sparklend / prime / psm3 indexers, dex bootstrap, and the
backfillers). The transport retries 429 / 5xx / network with capped exponential
backoff and masks the failures as added latency, so this is the leading
indicator of a throttle-driven latency tail — it typically precedes or
accompanies a `Vector*IndexerRPCLatencyHigh` warning on the same chain.

### First checks (≤5 min)

1. **Break down by reason** — the single most useful query:
   ```promql
   sum by (reason) (rate(rpc_http_retries_total{k8s_namespace_name="vector", service_name="<svc>"}[10m]))
   ```
   - `reason="429"` → upstream **rate-limiting** (Alchemy compute-unit
     throttling). The Alchemy key is shared across all workers and chains, so a
     burst from any worker can throttle the whole account.
   - `reason="5xx"` → transient provider **server errors**.
   - `reason="network"` → connection resets / DNS / TLS / dial failures.
2. **Confirm latency impact** — check the matching per-service RPC latency
   metric (e.g. `oracle_rpc_duration_seconds` p99) and whether a
   `Vector*IndexerRPCLatencyHigh` alert is firing on the same chain. A slow
   trace also carries inline `rpc.retry` span events with the same labels.
3. **Alchemy status / quota** — https://status.alchemy.com/ and the account's
   compute-unit usage.

### Common causes

- **Account-wide CU throttling on the shared Alchemy key** (`reason="429"`):
  cross-worker bursts exceed the per-second CU budget. The call itself is cheap;
  the seconds come from our backoff. This is the shape behind the avalanche-c
  oracle latency tail.
- **Transient provider degradation** (`reason="5xx"`): wait for recovery.
- **Network churn** (`reason="network"`): check pod egress / node connectivity
  if localized to one pod.

### What to do

- Transient (ratio falls back under 20% within an interval or two): no action —
  the transport is doing its job.
- Sustained 429s: raise the Alchemy CU/throughput limit, or reduce cross-worker
  burst pressure (stagger schedules / lower per-worker concurrency). As a
  freshness-over-completeness lever, a service can lower its dial timeout
  (`WithClientTimeout`) or retry budget so a throttled call fails fast and the
  block is reprocessed rather than blocking the worker.
- Sustained 5xx/network against one host only: consider failing that chain over
  to an alternate RPC provider if available.

---

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)

## curve-indexer (VEC-260)

## VectorCurveIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

`curve_blocks_processed_total` has not incremented for 15 minutes on the
labelled `chain`. The Curve pool state in TimescaleDB is going stale; no swap
or liquidity events are being recorded.

### First checks (<=5 min)

1. **Pod status**
   `kubectl -n vector get pods -l app=curve-indexer`
2. **Recent logs** — look for decode panics, DB connection errors,
   `context deadline exceeded`, or SQS poll failures:
   `kubectl -n vector logs -l app=curve-indexer --tail=100`
3. **Upstream lag** — confirm the watcher is producing blocks for this chain
   (if not, the root cause is upstream — see `VectorWatcherNoBlocks`).
4. **SQS queue depth** — check the curve-indexer SQS queue. A depth of 0 with
   no processing means the consumer lost its connection or the queue is empty.
5. **TimescaleDB health** — connection pool exhaustion or replication lag can
   stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade -> add the new
  ABI / decoder and redeploy.
- DB connection pool saturated -> restart the pod; longer-term raise the pool
  limit.
- SQS consumer lost connection -> pod restart reconnects.
- Block latency high enough that the worker is processing but not completing
  within the 5m rate window (see `VectorCurveIndexerBlockLatencyHigh`).

### Verify recovery

`rate(curve_blocks_processed_total[5m]) > 0` for the affected chain.

---

## VectorCurveIndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

`curve_errors_total` is above 0.1 errors/sec sustained for 15 minutes. Errors
are counted per operation (attribute `operation`); the indexer continues
processing but errors at this rate often precede a full stall.

### First checks

1. **Dominant error class** — `sum by (operation)(rate(curve_errors_total[10m]))`
   to see which operation is failing most.
2. **Pod logs** — `kubectl -n vector logs -l app=curve-indexer | grep "ERROR"`
3. **Recent deploys** — `kubectl rollout history deploy/curve-indexer -n vector`.
   A failed ABI decode after a contract change is a common trigger.
4. **Chain reorgs** — check watcher logs; a reorg delivers blocks the indexer
   may reject until the version advances.

### Common causes

- ABI decode failure after a Curve contract upgrade -> update the ABI/decoder.
- DB write error (FK constraint, duplicate key) -> inspect the failing pool and
  block number.
- Transient RPC timeout -> usually self-clears; investigate if sustained.

### Verify recovery

`rate(curve_errors_total[10m]) == 0` for the affected chain.

---

## VectorCurveIndexerBlockLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 block processing duration (`curve_block_duration_seconds`) exceeds 3
seconds sustained for 15 minutes. The indexer is degraded; expect downstream
lag in Curve pool state.

### First checks

1. **Multicall/RPC latency** — the Coordinator's `SnapshotState` calls issue
   batched multicalls to the archive RPC. High latency there dominates block
   duration. Check the archive RPC pod health and CPU.
2. **Pool count** — a large `snapshotSet` (many touched pools or sweep
   firing on all pools) multiplies multicall round-trips. Check
   `SweepBlocks` config and pool count.
3. **DB write latency** — confirm TimescaleDB is not under I/O pressure.
4. **Pod CPU/memory** — `kubectl top pod -n vector -l app=curve-indexer`.

### Common causes

- Archive RPC node degraded -> coordinate with infra; consider circuit-
  breaker or fallback RPC.
- Sweep interval too short for pool count -> raise `SWEEP_BLOCKS`.
- TimescaleDB I/O contention -> investigate concurrent write patterns.

### Verify recovery

`histogram_quantile(0.99, sum by (le)(rate(curve_block_duration_seconds_bucket[10m]))) < 3`
for the affected chain.

---

## VectorCurveIndexerNoStateWritten

**Severity:** warning · **For:** 10m
The 30m rate window must remain above the configured sweep interval (default 50 blocks, ~10min on mainnet).

### What it means

Blocks are advancing (`curve_blocks_processed_total{status="success"}` is
non-zero) but no pool-state snapshot rows have been written
(`curve_state_rows_written_total` is zero) for 30 minutes. The error path will
NOT catch this: a quietly-empty snapshot loop (e.g. `buildSnapshotSet` always
returns empty, sweep disabled and no touched pools, or all pools skipped)
produces no errors, just no state rows.

This is the data-quality / silent-empty check that `VectorCurveIndexerStalled`
cannot see.

Gating on blocks processed is sound for curve because the periodic sweep
(`SWEEP_BLOCKS`) re-snapshots every pool on a fixed block cadence, so a healthy
worker writes state rows even through a totally quiet market. The rule uses
`unless`, not `and … == 0`, so it also fires when `curve_state_rows_written_total`
is **absent** — a deploy that has never once written a state row is exactly the
case the old `and` form went blind on.

### First checks

1. **SweepBlocks config** — if `SWEEP_BLOCKS=0` (disabled) and no
   blocks contain Curve events, `buildSnapshotSet` legitimately returns empty.
   Confirm there is genuine pool activity on-chain or re-enable sweep. Note the
   overlays do **not** set `SWEEP_BLOCKS` — curve relies on the code default of
   50 blocks (`cmd/workers/internal/dexbootstrap/parseconfig.go`), ~10min on
   mainnet, so grepping k8s and finding nothing does not mean the sweep is off.
   With the sweep genuinely off, curve loses the cadence guarantee this alert
   assumes and will false-positive through quiet windows the way uniswap-v3 did
   before it was re-gated on `pools_touched` — prefer re-enabling the sweep over
   widening the window.
2. **Touched pools** — check whether any registered pool addresses match logs
   in the processed blocks. `kubectl logs -l app=curve-indexer` should show
   pool-touch debug entries (or absence thereof).
3. **Pool registry** — if the pool list is empty (LoadPools returned 0 rows),
   `buildSnapshotSet` can never produce entries. Confirm the DB has rows in
   `curve_pool`.
4. **snapshotSet size metric** is not separately emitted; use
   `curve_state_rows_written_total` as the proxy. A sudden drop to zero after
   previously non-zero is more urgent than a fresh deploy with no history.

### Common causes

- `SWEEP_BLOCKS=0` and no Curve events on this chain in 30m (legitimate
  low-activity window) -> confirm on-chain before escalating.
- Pool registry empty (migration not applied, wrong chain ID) -> verify
  `SELECT count(*) FROM curve_pool WHERE chain_id = <id>`.
- Contract address mismatch (new pool deployed at different address) -> update
  the pool registry.
- Sustained SQS replay / redrive, or a backfill re-run over an already-indexed
  range under one `build_id`: every message is a block already persisted at this
  build (same `build_id`, same `block_version`), so each state INSERT hits
  ON CONFLICT DO NOTHING (0 rows) and `curve_state_rows_written_total` does not
  advance even though processing succeeds. A redeploy (new `build_id`) or reorg
  (new `block_version`) inserts fresh rows and clears the alert. Check the queue
  for a redrive, and check whether a backfill is re-processing an already-indexed
  range, before assuming a logic stall.

### Verify recovery

`rate(curve_state_rows_written_total[30m]) > 0` for the affected chain, or
confirm on-chain that no Curve activity occurred (legitimate quiet window).

---

## uniswap-v3-indexer (VEC-261)

Runs via the unified `dex-indexer` binary (`DEX=uniswap-v3`), metric prefix
`uniswap_v3` (set by `uniswapV3Factory` in
`cmd/workers/dex-indexer/factories.go`). Unlike `curve-indexer` there is no
periodic sweep: `handleBlock` decodes events per block, derives the touched
pool set via `dexconsumer.DueSet`, and snapshots only those pools' state and
tick rows through one multicall before the transaction commit. A block with
no Uniswap V3 activity legitimately writes zero state rows.

## VectorUniswapV3IndexerStalled

**Severity:** critical · **For:** 15m

### What it means

`uniswap_v3_blocks_processed_total{status="success"}` has not incremented for
15 minutes on the labelled `chain`. Uniswap V3 pool state in TimescaleDB is
going stale; no swaps, liquidity events, or tick updates are being recorded.

### First checks (<=5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=uniswap-v3-indexer`
2. **Recent logs** — look for decode panics, DB connection errors,
   `context deadline exceeded`, or SQS poll failures:
   `kubectl -n vector logs -l app=uniswap-v3-indexer --tail=100`
3. **Upstream lag** — confirm the watcher is producing blocks for this chain
   (if not, the root cause is upstream — see `VectorWatcherNoBlocks`).
4. **SQS queue depth** — check the uniswap-v3-indexer SQS queue. A depth of 0
   with no processing means the consumer lost its connection or the queue is
   empty.
5. **TimescaleDB health** — connection pool exhaustion or replication lag can
   stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade -> add the new
  ABI / decoder and redeploy.
- DB connection pool saturated -> restart the pod; longer-term raise the pool
  limit.
- SQS consumer lost connection -> pod restart reconnects.
- Block latency high enough that the worker is processing but not completing
  within the 5m rate window (see `VectorUniswapV3IndexerBlockLatencyHigh`).

### Verify recovery

`rate(uniswap_v3_blocks_processed_total{status="success"}[5m]) > 0` for the
affected chain.

---

## VectorUniswapV3IndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

`uniswap_v3_errors_total` is above 0.1 errors/sec sustained for 15 minutes.
Errors are counted per operation (attribute `operation`, currently
`blockHandler` — recorded once at the `BlockHandler` boundary on any non-nil
error); the indexer continues processing but errors at this rate often
precede a full stall.

### First checks

1. **Dominant error class** — `sum by (operation)(rate(uniswap_v3_errors_total[10m]))`
   to see which operation is failing most.
2. **Pod logs** — `kubectl -n vector logs -l app=uniswap-v3-indexer | grep "ERROR"`
3. **Recent deploys** — `kubectl rollout history deploy/uniswap-v3-indexer -n vector`.
   A failed ABI decode after a contract change is a common trigger.
4. **Chain reorgs** — check watcher logs; a reorg delivers blocks the indexer
   may reject until the version advances.

### Common causes

- ABI decode failure after a Uniswap V3 contract upgrade -> update the
  ABI/decoder.
- DB write error (FK constraint, duplicate key) -> inspect the failing pool
  and block number.
- Transient RPC timeout on the DueSet multicall -> usually self-clears;
  investigate if sustained.

### Verify recovery

`rate(uniswap_v3_errors_total[10m]) == 0` for the affected chain.

---

## VectorUniswapV3IndexerBlockLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 block processing duration (`uniswap_v3_block_duration_seconds`) exceeds 3
seconds sustained for 15 minutes. The indexer is degraded; expect downstream
lag in Uniswap V3 pool state.

### First checks

1. **Multicall/RPC latency** — `snapshotDueSet` issues one batched multicall
   per block for all touched pools before opening the transaction. High
   latency there dominates block duration. Check the archive RPC pod health
   and CPU.
2. **Touched-pool count** — a block touching many pools (e.g. a heavy swap
   block across the registered pool set) multiplies multicall payload size.
   Check pool count for this chain.
3. **DB write latency** — confirm TimescaleDB is not under I/O pressure.
4. **Pod CPU/memory** — `kubectl top pod -n vector -l app=uniswap-v3-indexer`.

### Common causes

- Archive RPC node degraded -> coordinate with infra; consider circuit-
  breaker or fallback RPC.
- Large touched-pool set in a single block -> expected under high on-chain
  activity; confirm against block explorer before escalating.
- TimescaleDB I/O contention -> investigate concurrent write patterns.

### Verify recovery

`histogram_quantile(0.99, sum by (le)(rate(uniswap_v3_block_duration_seconds_bucket[10m]))) < 3`
for the affected chain.

---

## VectorUniswapV3IndexerNotWritingState

**Severity:** warning · **For:** 10m
Gated on `uniswap_v3_pools_touched_total`, not on blocks processed. uniswap-v3
runs `SnapshotTracker(0)` — no sweep — so it writes state rows only for pools an
event touched in that block, and a quiet market legitimately writes nothing. The
gate means a quiet window can no longer fire this alert: it fires only when pools
WERE touched and rows still did not come out.

This is **one half** of the silent-empty guard. It cannot fire when the touched
set is always empty (that zeroes its own left side) —
[`VectorUniswapV3IndexerNoPoolsTouched`](#vectoruniswapv3indexernopoolstouched)
is the other half and covers that class. Neither alone is the whole guard.

### What it means

Decoded events touched registered pools
(`uniswap_v3_pools_touched_total` is non-zero) but no state/tick snapshot rows
were written (`uniswap_v3_state_rows_written_total` is zero) for 30 minutes.
The error path will NOT catch this: a due-set that goes quietly empty (`DueSet`
returning nothing for a pool that WAS touched, or every `snapshotDueSet` call
silently no-opping) produces no errors, just no state rows.

Because pools are being touched, **a quiet market is already ruled out** — this
is not the "no Uniswap V3 activity" case. Something between decode and persist
is dropping the rows.

This is the data-quality / silent-empty check that `VectorUniswapV3IndexerStalled`
cannot see.

### First checks

1. **SQS replay / redrive** — the most common benign cause (see below). Check
   the queue for a redrive before assuming a logic stall.
2. **Due set** — `pools.touched` is recorded from `handleBlock`'s decode-stage
   `touchedIDs`, upstream of `DueSet`, so a non-zero gate with zero state rows
   points straight at `DueSet` returning empty for pools that were touched, or
   at `snapshotDueSet` no-opping.
3. **Pool registry** — a registry that is empty for this chain cannot produce
   touches either, so it would NOT fire this alert. If you suspect an empty
   registry, check `uniswap_v3_pools_touched_total` is flat at zero and confirm
   the Uniswap V3 pool registry row count for this chain directly.

### Common causes

- Sustained SQS replay / redrive over an already-indexed range under one
  `build_id`: every message is a block already persisted at this build (same
  `build_id`, same `block_version`), so each state INSERT hits `ON CONFLICT DO
  NOTHING` (0 rows) and `uniswap_v3_state_rows_written_total` does not advance
  even though processing succeeds — while `pools_touched` keeps advancing,
  since the blocks really do touch pools. A redeploy (new `build_id`) or reorg
  (new `block_version`) inserts fresh rows and clears the alert.
- `DueSet` silently empty despite touched pools -> a tracker/`SnapshotTracker`
  regression; this is the bug the alert exists to catch.
- Contract address mismatch (new pool deployed at a different address) ->
  update the pool registry. Note this suppresses touches too, so it shows up as
  a flat-zero `pools_touched`, not as this alert.

### Verify recovery

`rate(uniswap_v3_state_rows_written_total[30m]) > 0` for the affected chain.

A quiet market no longer needs ruling out — if no pools are being touched the
alert cannot fire. To sanity-check overall liveness during a lull, confirm
`rate(uniswap_v3_blocks_processed_total{status="success"}[5m]) > 0`
(`VectorUniswapV3IndexerStalled` covers this).

### History

Fired on 2026-07-13 (~22:36-22:51 UTC) against a **healthy** indexer, hours
after the VEC-329 rollout put uniswap-v3 into staging. The rule then gated on
blocks processed, which advance every 12s on mainnet, while the seeded registry
(18 wstETH/LST/LRT pools) only produces 3-28 state rows/hour — so any quiet
half-hour tripped it. The `pools_touched` gate replaced the blocks-processed
gate to close that false positive. Because that gate is blind to an always-empty
touched set, `VectorUniswapV3IndexerNoPoolsTouched` was added at the same time to
cover the class the old rule had covered by accident.

---

## VectorUniswapV3IndexerNoPoolsTouched

**Severity:** warning · **For:** 10m
The 6h rate window is the one that absorbs a quiet market. It is deliberately
much wider than the 30m used elsewhere in this group: 30m stretches with zero
touched pools are normal for this registry and are precisely what made the old
`NotWritingState` gate a false positive.

### What it means

Blocks are advancing (`uniswap_v3_blocks_processed_total{status="success"}` is
non-zero) but **not one** registered pool has been touched by a decoded event
(`uniswap_v3_pools_touched_total` is zero or absent) for 6 hours.

This is the other half of the silent-empty guard, and it covers what
[`VectorUniswapV3IndexerNotWritingState`](#vectoruniswapv3indexernotwritingstate)
structurally cannot: that rule gates on `pools_touched`, so a touched set that is
*always* empty makes its left side absent and the rule un-fireable. Nothing else
in the group would page — `blocks_processed` keeps advancing happily with a dead
registry, and no error is ever raised.

Measured cadence in staging is 3-28 state rows/hour, every hour, so 6h of zero
touches is roughly 30x the worst observed quiet gap. It is not a lull.

### First checks

1. **Pool registry** — the most likely cause. Confirm the Uniswap V3 pool
   registry actually has rows for this chain; a registry that loads zero pools
   means `poolsByAddr` is empty and no log can ever match.
2. **Address matching** — if the registry is populated, suspect
   `poolsTouchedByReceipt` / `poolsByAddr` (a checksum/casing regression, or
   pools seeded at the wrong addresses). Cross-check a known-active pool address
   against a block you can see swaps in on-chain.
3. **Genuinely dead pool set** — confirm on-chain that the seeded pools really
   have had no swap/mint/burn in 6h. For the wstETH/LST/LRT set this would be
   extraordinary, but a chain other than mainnet with a thin registry could
   legitimately go quiet this long; if so, the window needs widening for that
   chain rather than the alert silencing.

### Common causes

- Pool registry empty or not seeded for this chain (migration not applied, wrong
  chain ID) -> verify the registry row count for this chain.
- Address-match regression in `poolsByAddr` -> no log ever matches a registered
  pool, so `touchedIDs` is always empty.
- Contract address mismatch (pools seeded at addresses that were never deployed,
  or superseded by redeployed pools) -> update the pool registry.

### Verify recovery

`rate(uniswap_v3_pools_touched_total[6h]) > 0` for the affected chain.

---

## uniswap-v4-indexer (VEC-475)

> **Not deployed yet.** The k8s workload for this indexer — its Deployment and
> its SQS queue — lands in the stacked follow-up PR. Until that merges no
> `uniswap_v4_*` series exist, so none of the alerts below can fire and the
> `kubectl` commands in this section select nothing.

Runs via the unified `dex-indexer` binary/image (`DEX=uniswap-v4`), metric prefix
`uniswap_v4` — declared by `uniswapV4Factory.MetricPrefix()` in
`cmd/workers/dex-indexer/factories.go` and passed to `dextelemetry.NewTelemetry`
by `dexbootstrap.Bootstrap`
(`cmd/workers/internal/dexbootstrap/bootstrap.go`). Same shape as
`uniswap-v3-indexer`, with one structural difference that changes every triage
step below.

**V4 is a singleton.** There is no per-pool contract: every pool lives inside one
`PoolManager` (mainnet `0x000000000004444c5dc75cB358380D2e3dE08A90`, deployed at
block 21688329) and is identified by a 32-byte **PoolId**,
`keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))`. Log
matching is therefore two-stage: the log's *address* must be the PoolManager, and
its indexed `topics[1]` (the PoolId) must be a registered pool. An address match
alone tells you nothing about which pool — or whether it is one of ours.

Pool state is not read from the pool (there isn't one). It is read from the
**StateView** lens contract
(`0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227`) — `getSlot0`, `getLiquidity`,
`getFeeGrowthGlobals`, `getTickInfo`, `getTickBitmap` — pinned to the block hash
(`ExecuteAtHash`), never by block number.

Like `uniswap-v3-indexer` and unlike `curve-indexer` there is no periodic sweep
(`SnapshotTracker(0)`): for a **static-fee** pool V4 state can only change
through a PoolManager log keyed by PoolId, so `handleBlock` decodes events per
block, derives the touched pool set via `dexconsumer.DueSet`, and snapshots only
those pools before the transaction commit.

**Snapshots are a curated per-pool gate.** `uniswap_v4_pool.snapshot_supported`
decides whether a pool's state, ticks and positions are read at all; a `false`
pool is still decoded, and its swaps, liquidity events and pool events are still
indexed — only the `uniswap_v4_pool_state` / `uniswap_v4_tick` /
`uniswap_v4_position` half is dropped, and the worker issues no chain read for
it. The gate exists for the dynamic LP fee
(`PoolKey.fee == 0x800000`): `updateDynamicLPFee` rewrites `slot0.lpFee` and
emits nothing, so with no sweep to fall back on the snapshotted `lp_fee` would
silently go stale between touches. Such a pool is **not** refused at boot — the
fee is hashed into the PoolId, so no superseding registry row could ever repair
it and the worker would crash-loop forever on a legal pool. See VEC-573 for the
`lp_fee` refresh path that would let the gate be flipped back on.

Consequences worth remembering while triaging: an excluded pool contributes to
`uniswap_v4_pools_touched_total` but never to
`uniswap_v4_state_rows_written_total`, and it is deliberately not counted by
`uniswap_v4_pools_never_indexed`. To change a pool's gate, append a superseding
registry row with the new `snapshot_supported` value — the same append-only
correction as any other registry fix
([Fixing a bad registry row](#fixing-a-bad-registry-row)) — and restart the
worker so `LoadPools` re-reads it. `UPDATE` is revoked on the table; there is no
other way to flip it.

The reads are per pool, not per block: one `getSlot0` +
`getLiquidity` + `getFeeGrowthGlobals` multicall for each due pool, then that
pool's `getTickInfo` reads batched 500 positions per call (and, on a pool's
first-*ever* touch, a `getTickBitmap` baseline scan batched 500 words per call —
"ever" across restarts, since the already-baselined set is seeded at boot from
the persisted rows). A block with no Uniswap V4 activity legitimately writes
zero state rows.

**Tables:** `uniswap_v4_pool_state`, `uniswap_v4_swap`,
`uniswap_v4_liquidity_event`, `uniswap_v4_tick`, `uniswap_v4_pool_event`,
`uniswap_v4_position`.
**Registry:** `uniswap_v4_pool`, keyed by `chain_id` + the 32-byte `pool_id`,
plus `uniswap_v4_pool_manager`, which holds that chain's StateView address
directly and its **PoolManager address only through `protocol_id`** — the
address lives on the FK'd `protocol` row, so every query for it joins
`protocol`. There is no FK between the two registry tables: both are append-only
version histories matched on `chain_id`, and "current" always means the highest
`processing_version` per natural key — never the newest `id` or `build_id`.

Two invariants worth knowing before you debug anything here:

- **A `uniswap_v4_pool_state` row with `sqrt_price_x96 = 0` can never exist at
  `block_version = 0`.** `UniswapV4PoolState.Validate` rejects it. StateView
  returns all zeros — it does *not* revert — for a PoolId that was never
  initialized, so a zero price is the only signal that the registry is pointing
  at a PoolId that does not exist on chain. If you ever see that error in the
  logs, it is a registry bug, not an RPC blip. The one legal all-zero row is the
  reorg tombstone at `block_version > 0`: a pool whose `Initialize` an orphaned
  fork carried reads back all-zero on the new fork, and that row has to persist
  to supersede the orphan (`isOrphanedReRead`, and the table's own
  `CHECK (sqrt_price_x96 > 0 OR (sqrt_price_x96 = 0 AND block_version > 0))`).
- **A PoolId that disagrees with its key cannot reach production.** The worker
  recomputes `keccak256(abi.encode(PoolKey))` for every registry row at startup
  (`ValidatePoolKeys`) and refuses to boot on a mismatch. A pool whose *key* is
  right but whose *PoolId* is wrong is therefore impossible; a pool whose key is
  wrong (wrong currency, fee, tickSpacing, or hooks address) is self-consistent
  and boots fine — it just never matches a log. One such pool is caught by
  [`VectorUniswapV4IndexerPoolNeverIndexed`](#vectoruniswapv4indexerpoolneverindexed);
  a whole registry of them by
  [`VectorUniswapV4IndexerNoPoolsTouched`](#vectoruniswapv4indexernopoolstouched).

Handy commands, used by several sections below.

Registry, as the worker loads it:

```sql
-- Current registry for chain 1. uniswap_v4_pool is an append-only version
-- history keyed by (chain_id, pool_id), so the current row for a pool is its
-- highest processing_version -- hence DISTINCT ON, not a plain SELECT.
SELECT '0x' || encode(p.pool_id, 'hex')    AS pool_id,
       '0x' || encode(p.currency0, 'hex')  AS currency0,
       '0x' || encode(p.currency1, 'hex')  AS currency1,
       p.fee, p.tick_spacing,
       '0x' || encode(p.hooks, 'hex')      AS hooks,
       p.deploy_block, p.processing_version
FROM (
    SELECT DISTINCT ON (pool_id) *
    FROM uniswap_v4_pool
    WHERE chain_id = 1
    ORDER BY pool_id, processing_version DESC
) p
ORDER BY p.deploy_block;

-- uniswap_v4_pool_manager is versioned the same way, one current row per chain.
-- It has NO pool_manager_address column: the PoolManager address is the FK'd
-- protocol row's, so the address always comes from this join.
SELECT DISTINCT ON (m.chain_id)
       m.chain_id,
       '0x' || encode(pr.address, 'hex')             AS pool_manager,
       '0x' || encode(m.state_view_address, 'hex')   AS state_view,
       m.deploy_block, m.processing_version
FROM uniswap_v4_pool_manager m
JOIN protocol pr ON pr.id = m.protocol_id
ORDER BY m.chain_id, m.processing_version DESC;
```

The migration ships `uniswap_v4_pool_current` and
`uniswap_v4_pool_manager_current`, views that apply exactly those `DISTINCT ON`
picks, so `SELECT * FROM uniswap_v4_pool_current WHERE chain_id = 1` is the
short form. The manager view is the registry row only — it carries
`protocol_id`, not the PoolManager address, so keep the `protocol` join whenever
you need the address. Query the base tables without the wrapper to see a pool's
full correction history — that is how you check whether a bad row has already
been superseded.

Recompute a PoolId from its key (this is exactly what `ValidatePoolKeys` does;
the example is the seeded ETH/wstETH `fee=100, tickSpacing=1, hooks=0` pool):

```bash
cast keccak "$(cast abi-encode 'f(address,address,uint24,int24,address)' \
  0x0000000000000000000000000000000000000000 \
  0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0 \
  100 1 0x0000000000000000000000000000000000000000)"
# 0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76
```

Note `currency0 = address(0)` — that is native ETH in V4, not a missing value.

Read a pool's live state through StateView:

```bash
cast call 0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227 \
  'getSlot0(bytes32)(uint160,int24,uint24,uint24)' \
  0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76 \
  --rpc-url "$RPC_URL"
# -> sqrtPriceX96, tick, protocolFee, lpFee
```

An **all-zero return is not a revert** and not an RPC failure: it means StateView
has never heard of that PoolId. That is the fastest confirmation that a registry
row is wrong.

---

## VectorUniswapV4IndexerDown

**Severity:** critical · **For:** 10m

### What it means

The `uniswap-v4-indexer` Deployment has <1 available replica for 10 minutes. No
pod is running, so nothing is written to any `uniswap_v4_*` table and the SQS
backlog is growing.

This rule reads `kube_deployment_status_replicas_available` from
kube-state-metrics, which is independent of the OTel pipeline. That is the whole
point: **a worker that never boots emits no `uniswap_v4_*` series at all**, so
every other rule in the group has nothing to key on and stays silent. The V4
worker is deliberately fail-fast at startup — `ValidatePoolKeys`, the
single-PoolManager check, and the boot-time `PoolIDsEverSnapshotted` read each
refuse to start rather than index wrong or invisible data — which makes a boot
crash-loop a routine failure mode here, not an exotic one.

### First checks (<=5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=uniswap-v4-indexer`.
2. **Why it is not ready** — `kubectl -n vector describe deployment/uniswap-v4-indexer`
   and, for a crash loop, the *previous* container's logs:
   `kubectl -n vector logs -l app=uniswap-v4-indexer --previous --tail=100`.
3. **Registry refusal** — the fail-fast startup errors are self-identifying:
   `kubectl -n vector logs -l app=uniswap-v4-indexer --previous | grep -E "registry bug|PoolManager|StateView|no uniswap v4 pools|ever been indexed"`
   - `... : registry bug` — `ValidatePoolKeys` recomputed a PoolId that
     disagrees with the seeded one, or two rows share a PoolId.
   - `pools A and B have different PoolManager/StateView addresses` — the
     registry spans two deployments; one worker serves one.
   - `no uniswap v4 pools registered for chain N` — the chain has no
     current-version `uniswap_v4_pool` rows.
   - `reading which uniswap v4 pools have ever been indexed on chain N` — the
     boot read against TimescaleDB failed; this is a DB availability problem,
     not a registry one.

   Every one of these is fixed by appending a superseding registry row
   ([Fixing a bad registry row](#fixing-a-bad-registry-row)) or by restoring the
   database — never by editing the offending row in place.
4. **Secrets/config present** — a missing key from the `uniswap-v4-indexer`
   ExternalSecret (queue URL, `DATABASE_URL`, `ALCHEMY_API_KEY`, `REDIS_ADDR`)
   crashes the worker on startup by design.
5. **Node/scheduling** — a pending pod means node capacity or taints, not the
   worker.

### Common causes

- Registry refusal on the first deploy after a seeding migration — the worker is
  doing its job; correct the registry and restart.
- ExternalSecret not yet synced (queue URL / DB URL missing) — the deploy ran
  before the infra apply.
- TimescaleDB unreachable at boot — fail-fast by design; fix the dependency.
- OOMKilled — check memory limits.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="uniswap-v4-indexer"} >= 1`,
and the pod logs show the `uniswap-v4-indexer started` line with a non-zero
`pools` count.

---

## VectorUniswapV4IndexerStalled

**Severity:** critical · **For:** 15m

### What it means

The worker is **up** (>=1 replica) but has consumed **no block for 15 minutes**:
`rate(uniswap_v4_blocks_processed_total{status="success"}[5m])` is zero — or the
series has **vanished entirely**. The expression zero-fills from
kube-state-metrics, so a dead OTLP export with a live pod still fires; without
that zero-fill the counter would simply staleness-expire and the rule would
return no data and stay silent. It is replica-gated, so a dead process is
reported by [`VectorUniswapV4IndexerDown`](#vectoruniswapv4indexerdown) instead
of by both.

Uniswap V4 pool state in TimescaleDB is going stale; no swaps, liquidity events,
or tick updates are being recorded. The worker consumes one `BlockEvent` per
block (~12s on mainnet) and counts every one, touched pools or not, so this is
never a quiet market.

Because the zero-fill comes from kube-state-metrics, which knows nothing about
chains, this rule aggregates by `cluster` only — unlike the rest of the group it
carries no `chain` label.

### First checks (<=5 min)

1. **Distinguish the cases** — is
   [`VectorUniswapV4IndexerDown`](#vectoruniswapv4indexerdown) also firing? If so
   the process is down; triage there and this rule should resolve. If not, the
   pod is alive and this is a wedged consume loop or a dead metrics export.
2. **Recent logs** — look for decode panics, DB connection errors,
   `context deadline exceeded`, or SQS poll failures:
   `kubectl -n vector logs -l app=uniswap-v4-indexer --tail=100`
3. **OTLP export** — if the logs show blocks still being processed while the
   counter is flat, the metrics pipeline is the problem, not the worker; other
   OTel series from the pod will be flat or absent too.
4. **Upstream lag** — confirm the watcher is producing blocks for this chain
   (if not, the root cause is upstream — see `VectorWatcherNoBlocks`).
5. **SQS queue depth** — check the uniswap-v4-indexer SQS queue. A depth of 0
   with no processing means the consumer lost its connection or the queue is
   empty.
6. **TimescaleDB health** — connection pool exhaustion or replication lag can
   stall writes; check the Postgres dashboard.

### Common causes

- Poison message wedging the consume loop -> the same block fails and is
  redelivered forever; inspect the DLQ and the repeating error in the logs.
- Broken OTLP export -> the worker is processing but its metrics stopped
  flowing; restart the pod or fix the collector.
- StateView reads failing hard (`AllowFailure=false` everywhere, by design) ->
  every block errors out and nothing commits; see
  `VectorUniswapV4IndexerErrorsHigh`.
- DB connection pool saturated -> restart the pod; longer-term raise the pool
  limit.
- SQS consumer lost connection -> pod restart reconnects.
- Block latency high enough that the worker is processing but not completing
  within the 5m rate window (see `VectorUniswapV4IndexerBlockLatencyHigh`).

A startup registry refusal is **not** in this list: it kills the pod, so it fires
[`VectorUniswapV4IndexerDown`](#vectoruniswapv4indexerdown) and is triaged there.

### Verify recovery

`sum by (cluster) (rate(uniswap_v4_blocks_processed_total{status="success"}[5m])) > 0`
in the affected cluster.

---

## VectorUniswapV4IndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

`uniswap_v4_errors_total` is above 0.1 errors/sec sustained for 15 minutes.
Errors carry an `operation` attribute with one of three values:

- `fetchReceipts` — the shared `dexconsumer` block processor could not read the
  block's receipts from the cache/S3 payload (upstream, before any V4 code runs).
- `unmarshalReceipts` — the payload was fetched but would not decode.
- `blockHandler` — recorded once at the `BlockHandler` boundary for any non-nil
  error out of the V4 handler itself: event decode, `DueSet`, the StateView
  snapshot reads, or persistence.

The indexer continues processing, but errors at this rate often precede a full
stall.

### First checks

1. **Dominant error class** — `sum by (operation)(rate(uniswap_v4_errors_total[10m]))`
   to see which operation is failing most.
2. **Pod logs** — `kubectl -n vector logs -l app=uniswap-v4-indexer | grep "ERROR"`
3. **Zero sqrt price** — a `sqrt_price_x96` validation error names a registry
   bug, not a transient fault. StateView returns zeros (no revert) for a PoolId
   it has never seen, and `Validate` rejects that rather than persisting a fake
   price. Confirm with the `getSlot0` snippet in the service intro, then append
   a superseding registry row
   ([Fixing a bad registry row](#fixing-a-bad-registry-row)).
4. **Recent deploys** — `kubectl rollout history deploy/uniswap-v4-indexer -n vector`.
5. **Chain reorgs** — check watcher logs; a reorg delivers blocks the indexer
   may reject until the version advances.

### Common causes

- Registry row pointing at a PoolId that does not exist on chain -> `Validate`
  rejects `sqrt_price_x96 = 0` on every touched block for that pool.
- `deploy_block` seeded above the blocks being processed -> `dexconsumer.DueSet`
  returns a hard error (`pool N touched at block B but registry deploy block is
  D: registry bug`), so the block fails and is redelivered forever. This is the
  symptom of a bad `deploy_block`; it never shows up as
  `VectorUniswapV4IndexerNotWritingState`.
- ABI decode failure on a *known* PoolManager event -> `shared.DecodeLog`
  rejects a partial decode at the source, so a log whose `topics[0]` the ABI
  recognises but whose payload will not fully decode fails the block. An unknown
  `topics[0]` is **not** in this class — see the note below.
- DB write error (FK constraint, duplicate key) -> inspect the failing pool and
  block number.
- Transient RPC timeout on the StateView multicall -> usually self-clears;
  investigate if sustained.

### How to spot a decoder gap (no alert covers this)

An **unknown `topics[0]` raises no error and moves no counter.** `captureRaw`
mirrors the log into `protocol_event` under its topic0 hex as the event name and
returns nil, deliberately — the capture net exists so an unrecognised log is
never lost, and treating it as a failure would poison-stall the block on
something the indexer has no typed home for. So it will never show up here, in
`uniswap_v4_errors_total`, or in any other rule in this group. The PoolManager is
not upgradeable, so the net is expected to stay empty; the only way to find out
that it did not is to look.

Look periodically, and after any change to the decoder or the ABI:

```sql
-- Raw-captured logs carry their topic0 hex as event_name (a zero-topic log gets
-- the literal 'anonymous'); every decoded event carries its ABI name instead.
-- Anything this returns is a decoder gap. protocol is keyed by
-- (chain_id, address), so scope by the PoolManager address -- `name` is not
-- unique and the scalar subquery would error, not degrade, on a second row.
SELECT event_name,
       count(*)          AS logs,
       min(block_number) AS first_block,
       max(block_number) AS last_block
FROM protocol_event
WHERE chain_id = 1
  AND protocol_id = (SELECT id FROM protocol
                     WHERE chain_id = 1
                       AND address = '\x000000000004444c5dc75cB358380D2e3dE08A90'::bytea)
  AND (event_name LIKE '0x%' OR event_name = 'anonymous')
GROUP BY event_name
ORDER BY logs DESC;
```

For each row, read the mirrored log and identify the event:

```sql
SELECT block_number, log_index, event_data
FROM protocol_event
WHERE chain_id = 1
  AND protocol_id = (SELECT id FROM protocol
                     WHERE chain_id = 1
                       AND address = '\x000000000004444c5dc75cB358380D2e3dE08A90'::bytea)
  AND event_name = '<topic0 hex>'
ORDER BY block_number DESC
LIMIT 5;
```

`event_data` is `{"topics": [...], "data": "0x..."}` verbatim. Identify the
signature the topic0 hashes to, then either add the event to the V4 ABI and its
typed table, or record that it is deliberately uncovered. Nothing self-heals
here: the rows stay raw until the decoder learns the event and the range is
re-indexed under a new `build_id`.

### Fixing a bad registry row

`uniswap_v4_pool` and `uniswap_v4_pool_manager` are strictly append-only:
`UPDATE` and `DELETE` are revoked on both. A correction is a new migration that
`INSERT`s a superseding row for the same natural key — `(chain_id, pool_id)` for
a pool, `chain_id` for a manager — carrying a `build_id` different from the row
it supersedes. The table's `BEFORE INSERT` trigger then assigns the next
`processing_version`, `LoadPools` reads that version on the next boot, and every
fact row already written keeps pointing at the registry version that was in
force when it was written.

Two things this rules out: re-inserting under the *same* `build_id` is an
idempotent no-op (the trigger reuses the existing version), not a correction;
and `UPDATE`/`DELETE` on the bad row is never the answer, because it would
rewrite the history the fact tables reference. Deploying the migration is the
whole fix — restart the worker so `LoadPools` re-reads the registry.

### Verify recovery

`rate(uniswap_v4_errors_total[10m]) == 0` for the affected chain.

---

## VectorUniswapV4IndexerBlockLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 block processing duration (`uniswap_v4_block_duration_seconds`) exceeds 3
seconds sustained for 15 minutes. The indexer is degraded; expect downstream
lag in Uniswap V4 pool state.

### First checks

1. **StateView / RPC latency** — `snapshotDueSet` walks the due pools one at a
   time, so round-trips scale with the due-pool count and the tick count, not
   with the block: one `getSlot0` + `getLiquidity` + `getFeeGrowthGlobals`
   multicall **per due pool**, then that pool's `getTickInfo` reads chunked 500
   positions per call, plus `getTickBitmap` word scans (500 words per call) for
   a pool being baselined. All of it is pinned to the block hash, so it goes to
   an archive node. High latency there dominates block duration; check archive
   RPC pod health and CPU.
2. **Baseline blocks** — the first block that touches a pool baselines its tick
   set via `getTickBitmap` word scans (chunked), which is far heavier than a
   steady-state block. `baselineSeen` is seeded at boot from the pools that
   already have persisted rows (`PoolIDsEverSnapshotted`), so a **restart or a
   rollout does not re-baseline anything** — only a genuinely newly seeded pool
   pays this cost, once. A spike right after a deploy is therefore actionable:
   either a registry migration added pools in the same release (confirm, then
   expect it to clear within the first blocks that touch each new pool), or the
   boot seed is not working and every pool is re-enumerating.
3. **Touched-pool count** — because V4 is a singleton, one busy block can carry
   PoolManager logs for many registered pools at once, multiplying the multicall
   payload.
4. **DB write latency** — confirm TimescaleDB is not under I/O pressure. The
   append-on-change tick writer takes per-`(pool, tick)` advisory locks, so a
   block touching a wide tick range serializes more work.
5. **Pod CPU/memory** — `kubectl top pod -n vector -l app=uniswap-v4-indexer`.

### Common causes

- Archive RPC node degraded -> coordinate with infra; consider circuit-breaker
  or fallback RPC.
- Tick baselining of pools a registry migration just added -> clears after the
  first block that touches each new pool. A rollout alone does not cause this;
  if no pools were added, treat it as the boot seed failing instead.
- Large touched-pool set in a single block -> expected under high on-chain
  activity; confirm against a block explorer before escalating.
- TimescaleDB I/O contention -> investigate concurrent write patterns.

### Verify recovery

`histogram_quantile(0.99, sum by (le)(rate(uniswap_v4_block_duration_seconds_bucket[10m]))) < 3`
for the affected chain.

---

## VectorUniswapV4IndexerNotWritingState

**Severity:** warning · **For:** 10m
Gated on `uniswap_v4_pools_touched_total`, not on blocks processed. uniswap-v4
runs `SnapshotTracker(0)` — no sweep — so it writes state rows only for pools a
PoolManager log touched in that block, and a quiet market legitimately writes
nothing. The gate means a quiet window can no longer fire this alert: it fires
only when pools WERE touched and rows still did not come out.

This is **one half** of the silent-empty guard. It cannot fire when the touched
set is always empty (that zeroes its own left side) —
[`VectorUniswapV4IndexerNoPoolsTouched`](#vectoruniswapv4indexernopoolstouched)
is the other half and covers that class. Neither alone is the whole guard.

### What it means

Decoded PoolManager events touched registered PoolIds
(`uniswap_v4_pools_touched_total` is non-zero) but no pool-state snapshot rows
were written for 30 minutes. `uniswap_v4_state_rows_written_total` counts
`uniswap_v4_pool_state` inserts only — never `uniswap_v4_tick` rows — so this
alert is about the state snapshot, not the tick writer.

The error path will NOT catch this: `snapshotDueSet` silently no-opping, or
every state `INSERT` hitting `ON CONFLICT DO NOTHING`, produces no error — just
no rows. `DueSet` is *not* one of those silent paths: a touched pool it cannot
resolve in the registry, or one whose `deploy_block` is above the block being
processed, is a hard error that fails the block.

Because pools are being touched, **a quiet market is already ruled out** — this
is not the "no Uniswap V4 activity" case. Something between decode and persist
is dropping the rows.

This is the data-quality / silent-empty check that `VectorUniswapV4IndexerStalled`
cannot see.

### First checks

1. **SQS replay / redrive** — the most common benign cause (see below). Check
   the queue for a redrive before assuming a logic stall.
2. **Snapshot path** — `pools.touched` is recorded from `handleBlock`'s
   decode-stage `touchedIDs`, upstream of `DueSet`, so a non-zero gate with zero
   state rows points at everything after it: `snapshotDueSet` no-opping,
   `buildBlockWrites` dropping the states, or the insert conflicting away.
3. **Not the deploy gate** — a `deploy_block` seeded above the blocks being
   processed cannot cause this alert. `DueSet` hard-errors on that pool
   (`... touched at block B but registry deploy block is D: registry bug`), the
   block fails, and the symptom is
   [`VectorUniswapV4IndexerErrorsHigh`](#vectoruniswapv4indexererrorshigh) or
   [`VectorUniswapV4IndexerStalled`](#vectoruniswapv4indexerstalled). Rule it
   out there, not here.
4. **Latest state rows** — confirm directly whether anything is landing:
   `SELECT pool_id, max(block_number) FROM uniswap_v4_pool_state GROUP BY 1;`
5. **Not an empty registry** — a chain with no `uniswap_v4_pool` rows never
   boots (`uniswapV4Factory` errors with `no uniswap v4 pools registered for
   chain N`), and a registry that loads but matches nothing produces no touches,
   which zeroes this alert's own gate. Either way the symptom is `Stalled` or
   [`VectorUniswapV4IndexerNoPoolsTouched`](#vectoruniswapv4indexernopoolstouched),
   not this alert.

### Common causes

- Sustained SQS replay / redrive over an already-indexed range under one
  `build_id`: every message is a block already persisted at this build (same
  `build_id`, same `block_version`), so each state INSERT hits `ON CONFLICT DO
  NOTHING` (0 rows) and `uniswap_v4_state_rows_written_total` does not advance
  even though processing succeeds — while `pools_touched` keeps advancing,
  since the blocks really do touch pools. A redeploy (new `build_id`) or reorg
  (new `block_version`) inserts fresh rows and clears the alert.
- A regression between the due set and the insert -> `snapshotDueSet` returning
  no states for a non-empty due set, or `buildBlockWrites` dropping them. This
  is the bug class the alert exists to catch. `DueSet` itself is not a
  candidate: it either resolves every touched pool or returns a hard error.
- Mis-seeded pool key in the registry -> note this suppresses touches too, so it
  shows up as a flat-zero `pools_touched` and fires the *other* alert, not this
  one.

### Verify recovery

`rate(uniswap_v4_state_rows_written_total[30m]) > 0` for the affected chain.

A quiet market no longer needs ruling out — if no pools are being touched the
alert cannot fire. To sanity-check overall liveness during a lull, confirm
`rate(uniswap_v4_blocks_processed_total{status="success"}[5m]) > 0`
(`VectorUniswapV4IndexerStalled` covers this).

---

## VectorUniswapV4IndexerNoPoolsTouched

**Severity:** warning · **For:** 10m
The 6h rate window is set by the *failure class*, not by a measured touch rate.
Everything this rule catches — a mis-seeded pool key, a wrong PoolManager
address, a `poolsByID` matching regression — is permanent: once the touched set
is empty it stays empty until a registry row changes or a fix ships, so a wide
window costs detection speed and no detections at all. The pressure in the other
direction is false positives: 30m stretches with zero touched pools are normal
for a curated registry, which is exactly what made the equivalent uniswap-v3
`NotWritingState` gate a false positive on 2026-07-13. The seeded pools were
chosen for liquidity *depth*, not for swap frequency, and their quiet-period
length has never been measured — 6h is margin against that unknown, not a
measurement. If a chain's registry turns out to be quieter still, widen the
window for that chain rather than silencing the alert.

### What it means

Blocks are advancing (`uniswap_v4_blocks_processed_total{status="success"}` is
non-zero) but **not one** registered PoolId has been touched by a decoded
PoolManager event (`uniswap_v4_pools_touched_total` is zero or absent) for 6
hours.

This is the other half of the silent-empty guard, and it covers what
[`VectorUniswapV4IndexerNotWritingState`](#vectoruniswapv4indexernotwritingstate)
structurally cannot: that rule gates on `pools_touched`, so a touched set that is
*always* empty makes its left side absent and the rule un-fireable. Nothing else
in the group would page — `blocks_processed` keeps advancing happily while
nothing matches, and no error is ever raised.

Because V4 matching is two-stage (log address == PoolManager, then
`topics[1]` == a registered PoolId), there are two independent ways for the
touched set to be permanently empty, and they need different fixes.

### First checks

1. **PoolManager address** — check this first; it is the failure mode V3 does not
   have. The address is the FK'd `protocol` row's, not a column of
   `uniswap_v4_pool_manager`, so the wrong value can come from either side: a
   manager row pointing at the wrong `protocol_id`, or the right protocol row
   carrying the wrong `address`. If the resolved address is not the real
   singleton, *zero* logs are ever considered and no PoolId can match, however
   good the pool rows are:

   ```sql
   -- Versioned table: the current row per chain is the highest
   -- processing_version, so read it through DISTINCT ON, and join protocol for
   -- the PoolManager address.
   SELECT DISTINCT ON (m.chain_id)
          m.chain_id,
          pr.name,
          '0x' || encode(pr.address, 'hex')           AS pool_manager,
          '0x' || encode(m.state_view_address, 'hex') AS state_view
   FROM uniswap_v4_pool_manager m
   JOIN protocol pr ON pr.id = m.protocol_id
   ORDER BY m.chain_id, m.processing_version DESC;
   -- mainnet: UniswapV4
   --          0x000000000004444c5dc75cb358380d2e3de08a90
   --          0x7ffe42c4a5deea5b0fec41c94c136cf115597227
   ```

2. **Pool registry** — confirm `uniswap_v4_pool` actually has current-version
   rows for this chain's `chain_id` (query in the service intro; the two
   registry tables are matched on `chain_id`, not by an FK). A chain with zero
   rows would not have booted at all, so what you are looking for here is a
   registry that loads but describes the wrong pools.
3. **Mis-seeded pool key** — the worker recomputes every PoolId at startup, so
   the id and the key are guaranteed consistent with each other; what startup
   validation cannot catch is a key that is internally consistent but does not
   describe a real pool (a `tickSpacing` that isn't the real pool's — in V4 it
   is chosen freely per pool, not derived from the fee as in V3 — currencies
   swapped, or a hooks address of zero on a hooked pool). Recompute the id from
   the key you *believe* is right and compare:

   ```bash
   cast keccak "$(cast abi-encode 'f(address,address,uint24,int24,address)' \
     "$CURRENCY0" "$CURRENCY1" "$FEE" "$TICK_SPACING" "$HOOKS")"
   ```

   then ask StateView whether the seeded id exists at all:

   ```bash
   cast call 0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227 \
     'getSlot0(bytes32)(uint160,int24,uint24,uint24)' "$POOL_ID" --rpc-url "$RPC_URL"
   ```

   An all-zero return (no revert) means the PoolId was never initialized on this
   chain — the registry row is wrong. A non-zero `sqrtPriceX96` means the pool is
   real and the problem is in matching, not seeding.
4. **PoolId matching** — if the registry and the PoolManager address are both
   right, suspect the `topics[1]` -> `poolsByID` lookup (a byte-order or
   `common.Hash` conversion regression). Cross-check a PoolManager `Swap` log you
   can see on-chain for a seeded pool against what the decoder does with it.
5. **Genuinely dead pool set** — confirm on-chain that the seeded pools really
   have had no `Swap` / `ModifyLiquidity` / `Donate` in 6h. A chain with a thin
   registry can legitimately go quiet this long, in which case the window needs
   widening for that chain rather than the alert silencing.

### Common causes

- Wrong or missing `uniswap_v4_pool_manager` row for this chain (wrong
  `protocol_id`, or a `protocol.address` that is not the singleton, or the wrong
  `chain_id`) -> no PoolManager log is ever considered.
- Registry seeded under the wrong `chain_id` -> the worker loads a non-empty
  registry for its own chain only if rows exist there; with none it refuses to
  boot, so what fires this alert is rows that exist but describe other chains'
  pools.
- **Every** pool key mis-seeded -> self-consistent key/PoolId pairs that no
  on-chain pool has. Each passes `ValidatePoolKeys`, never matches a log, and
  StateView returns zeros for it. Correct them by appending superseding registry
  rows, never by editing the bad ones
  ([Fixing a bad registry row](#fixing-a-bad-registry-row)). A *single*
  mis-seeded pool cannot fire this alert — the rest of the registry keeps
  `pools_touched` non-zero — and is caught by
  [`VectorUniswapV4IndexerPoolNeverIndexed`](#vectoruniswapv4indexerpoolneverindexed)
  instead.
- PoolId-matching regression in `poolsByID` -> no log ever matches a registered
  pool, so `touchedIDs` is always empty.

### Verify recovery

`rate(uniswap_v4_pools_touched_total[6h]) > 0` for the affected chain.

---

## VectorUniswapV4IndexerPoolNeverIndexed

**Severity:** warning · **For:** 10m (plus a 24h `offset` comparison)

### What it means

At least one registered, snapshot-supported pool has produced **no**
`uniswap_v4_pool_state` and **no** `uniswap_v4_tick` row — not now, and not 24
hours ago either. `$value` is how many.

This is the only **per-pool** health signal in the group. Every other rule here
is registry-wide: `NoPoolsTouched` needs the *whole* registry to go quiet,
`NotWritingState` needs *all* state rows to stop. One bad pool in a registry of
twenty moves neither. That gap is real and cheap to hit — in V4 `tickSpacing` is
chosen freely per pool rather than derived from the fee, so a single wrong digit
yields a `PoolKey`/`PoolId` pair that is internally consistent, passes
`ValidatePoolKeys`, boots green, matches no log, and stays empty forever while
the rest of the registry keeps the group's metrics healthy.

**The gauge measures persisted history, not uptime.** At construction the
service reads `PoolIDsEverSnapshotted` — the pools on this chain with at least
one state or tick row, at any height — and counts the registered,
snapshot-supported pools missing from it. It decrements as first rows land, and
a restart or a rollout re-derives the same number from the database rather than
resetting it. Pools the registry excludes (`snapshot_supported = false`) are
never counted: they produce no rows by design.

**Why `offset 24h` and not `for: 24h`.** Both mean "true for a day", but `for`
restarts its timer on any gap in the series, so a routine rollout would keep
re-arming it. Comparing against the sample 24 hours ago tolerates those gaps —
and gives a newly seeded pool its grace period for free, since right after a
registry migration there is no day-old sample above zero to compare with.

**Why a full day and not six hours.** Every failure this catches is permanent,
so a wider window costs detection speed and no detections at all, while the
pressure the other way — a curated pool that is simply quiet — is unmeasured:
the seeded pools were picked for liquidity depth, not swap frequency. A pool
untouched for six hours is not yet evidence of anything; a pool untouched for a
full day, in a registry where everything else is indexing, is.

One accepted false positive: if one pool gets its first rows and a *different*
pool is seeded inside the same 24h, both ends of the comparison are non-zero and
this fires on a pool younger than the window. Step 1 resolves it in one look.

### First checks

1. **Which pool?** — the worker names them at boot. This is the fastest answer
   and tells you immediately whether the pool is new:

   ```bash
   kubectl -n vector logs -l app=uniswap-v4-indexer | \
     grep "have never produced a state or tick row"
   ```

   The line carries `count`, `poolRowIds` (the `uniswap_v4_pool.id` surrogates)
   and `poolIds` (the on-chain 32-byte PoolIds).

   Same answer straight from the database, which also works when the pod has
   been restarted past its log retention:

   ```sql
   -- The exact complement of the worker's PoolIDsEverSnapshotted read. Fact
   -- rows written before a registry correction carry the SUPERSEDED pool id, so
   -- the existence check has to run over every version of the natural key, not
   -- over cur.id alone -- otherwise a corrected pool reads as never indexed.
   SELECT cur.id,
          '0x' || encode(cur.pool_id, 'hex') AS pool_id,
          cur.tick_spacing, cur.fee,
          '0x' || encode(cur.hooks, 'hex')   AS hooks,
          cur.deploy_block, cur.created_at
   FROM uniswap_v4_pool_current cur
   WHERE cur.chain_id = 1
     AND cur.snapshot_supported
     AND NOT EXISTS (
         SELECT 1
         FROM uniswap_v4_pool p
         WHERE p.chain_id = cur.chain_id
           AND p.pool_id = cur.pool_id
           AND (EXISTS (SELECT 1 FROM uniswap_v4_pool_state s WHERE s.pool_id = p.id)
                OR EXISTS (SELECT 1 FROM uniswap_v4_tick t WHERE t.pool_id = p.id)))
   ORDER BY cur.created_at DESC;
   ```

   `created_at` is the registry row's own write time. A pool seeded in the last
   few hours with a real on-chain PoolId is the accepted false positive above —
   confirm with step 2 and wait rather than escalating.

2. **Does the PoolId exist on chain?** — StateView answers without a revert:

   ```bash
   cast call 0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227 \
     'getSlot0(bytes32)(uint160,int24,uint24,uint24)' "$POOL_ID" --rpc-url "$RPC_URL"
   ```

   An **all-zero return means the PoolId was never initialized** — the registry
   row is wrong, and the pool can never index. A non-zero `sqrtPriceX96` means
   the pool is real, so either it is genuinely idle or the matching is broken.

3. **Which field is wrong?** — recompute the id from the key you *believe* is
   right and compare against the seeded one. `tick_spacing` and `hooks` are the
   usual culprits; `fee` and the currency order are hashed in too:

   ```bash
   cast keccak "$(cast abi-encode 'f(address,address,uint24,int24,address)' \
     "$CURRENCY0" "$CURRENCY1" "$FEE" "$TICK_SPACING" "$HOOKS")"
   ```

   Cross-check against the pool's own `Initialize` log on a block explorer — that
   log carries the authoritative `PoolKey`.

4. **Real but idle?** — if the PoolId exists on chain, check whether it has had a
   `Swap` / `ModifyLiquidity` / `Donate` at all since its `deploy_block`. A
   curated deep-liquidity pool going a full day untouched is already unusual; a
   pool that has *never* traded since deployment should not be in the registry
   at all — supersede it out rather than widening the window.

5. **Not a worker-wide fault** — if `VectorUniswapV4IndexerNoPoolsTouched` or
   `VectorUniswapV4IndexerNotWritingState` is firing at the same time, the
   registry-wide fault is the root cause; triage there first and this will clear
   with it.

### Common causes

- Mis-seeded `tick_spacing` (or `hooks`, `fee`, currency order) -> a
  self-consistent key/PoolId pair no on-chain pool has. This is the case the
  alert exists for. Correct it by appending a superseding registry row
  ([Fixing a bad registry row](#fixing-a-bad-registry-row)), then restart the
  worker so `LoadPools` picks it up.
- A pool seeded on the wrong `chain_id` -> it loads into the wrong worker, whose
  PoolManager never emits its logs.
- A genuinely dead pool in the curated registry -> the fix is to stop carrying
  it, not to silence the alert. Removal is also an append: supersede the row with
  `snapshot_supported = false` so it stops being counted while its history stays
  queryable.
- Registry migration in the last 24h that added a pool -> the accepted false
  positive; confirm with `created_at` and step 2, then let it clear.

### Verify recovery

`uniswap_v4_pools_never_indexed` reaches 0 for the affected chain, or the
step-1 SQL returns no rows. Note the gauge only moves after a block actually
snapshots the pool, so a registry correction needs a worker restart *and* a
block that touches the corrected pool before it clears.

---

## VectorUniswapV4AppendOnChangeGrowthHigh

**Severity:** warning · **For:** 6h

**Nothing is broken.** This is a tripwire on a *design decision*, not a fault.
It fires when the append-on-change tables have grown fast enough, for long
enough, that the choice to keep them un-partitioned should be revisited. Treat it
as a planning ticket, not an incident: there is roughly a year of runway from the
moment it fires (derivation below).

### What it means

`uniswap_v4_tick` and `uniswap_v4_position` are deliberately **plain tables, not
hypertables** — the only two `uniswap_v4_*` fact tables that are. The reason is
their write path, not their size.

Both are *append-on-change*. Every write first reads the latest row per natural
key (`(pool_id, tick)` / `(pool_id, owner, tick_lower, tick_upper, salt)`) to
decide whether anything changed, and that read can only be bounded by
`block_number <= N`: there is no lower bound the planner could use, because the
previous observation of a position may be arbitrarily old. On a plain table that
is one bounded index descent. On a hypertable it becomes a descent *per chunk* —
with 1-day chunks and a year of retention, ~365 probes per key per block, and
worse once chunks compress (locate a segment, then decompress it). That is the
fan-out profile VEC-541 measured for the `processing_version` triggers (4,410 ms
vs 148 ms for one 721-row batch at ~2,000 chunks). Every other `uniswap_v4_*`
fact table is written once per touched block and read by time range, so
partitioning suits them; these two are not, and it does not.

The trade is only right while the tables stay small. This alert guards that
premise.

**Threshold derivation** (mirrored in the rule comment):

| | |
|---|---|
| Plain-table comfort ceiling | ~100M rows (index depth, autovacuum, bloat on rewrite) |
| Observed rate today | ~10–100 rows/day, both tables combined |
| Alert threshold | 250k rows/day sustained = **2.9 rows/s** (`250000 / 86400 = 2.894`) |
| Implied growth | ~90M rows/year — about a year of runway |

The threshold sits 3–4 orders of magnitude above today's rate, so it cannot fire
on ordinary growth. Reaching it means a registry expansion (many more pools) or a
traffic regime change — something that genuinely invalidates the premise.

The 6h rate window *and* `for: 6h` both exist because the signal is a sustained
regime change. A backfill, or a pool's first-touch baseline tick enumeration
(O(10³) ticks read in one block), spikes the instantaneous rate by design and
must not wake anyone.

> The counters count rows **offered** to the append-on-change writer, which then
> drops the unchanged ones. The alert therefore over-estimates real table growth
> — on purpose: firing early on a headroom warning is the safe direction. Expect
> the real row counts below to be *lower* than the metric implies.

### First checks

1. **Actual row counts.** The metric is an upper bound; these are the truth.

   ```sql
   SELECT 'uniswap_v4_tick' AS table_name, count(*) AS row_count FROM uniswap_v4_tick
   UNION ALL
   SELECT 'uniswap_v4_position', count(*) FROM uniswap_v4_position;
   ```

   On a large table `count(*)` is slow; the planner's estimate is enough to
   decide, and instant:

   ```sql
   SELECT relname, n_live_tup,
          pg_size_pretty(pg_total_relation_size(relid)) AS total_size
   FROM pg_stat_user_tables
   WHERE relname IN ('uniswap_v4_tick', 'uniswap_v4_position');
   ```

2. **Growth over time**, to tell a step change from a one-off:

   ```sql
   SELECT date_trunc('day', block_timestamp) AS day, count(*) AS row_count
   FROM uniswap_v4_position
   WHERE block_timestamp > now() - INTERVAL '30 days'
   GROUP BY 1 ORDER BY 1;
   ```

3. **What changed.** A jump almost always traces to more pools, not more traffic
   per pool:

   ```sql
   SELECT count(*) AS current_pools FROM uniswap_v4_pool_current;
   ```

   Then check whether a migration recently seeded a batch of pools, or whether a
   new chain's indexer was deployed.

### Common causes

- **Registry expansion** — a migration seeded many more pools. Legitimate; the
  only question is whether the new steady-state rate justifies converting.
- **A new chain** — another `uniswap-v4-indexer` instance now writes into the
  same two tables (they key on the `uniswap_v4_pool` surrogate, which spans
  chains — which is why this rule sums across `chain`).
- **A backfill running longer than 6h** — legitimate and transient. Confirm it is
  a backfill, let it finish, expect the alert to clear itself.
- **An append-on-change regression** — rows appended on every touch instead of
  only on change (a field dropped from `v4TickUnchanged` / `v4PositionUnchanged`,
  or a `block_version` that always differs). This one *is* a bug. Symptom:
  consecutive rows for one natural key carrying identical values.

  ```sql
  -- Keys with more versions than distinct values: should return nothing.
  SELECT pool_id, owner, tick_lower, tick_upper, salt,
         count(*) AS versions,
         count(DISTINCT (liquidity,
                         fee_growth_inside0_last_x128,
                         fee_growth_inside1_last_x128)) AS distinct_values
  FROM uniswap_v4_position
  GROUP BY 1, 2, 3, 4, 5
  HAVING count(*) > count(DISTINCT (liquidity,
                                    fee_growth_inside0_last_x128,
                                    fee_growth_inside1_last_x128))
  LIMIT 20;
  ```

### Remediation — converting to a hypertable

Only once the rate is confirmed to be the new normal. This is a **new** migration,
never an edit to the creating one.

TimescaleDB will not partition a table that already holds rows in place, so the
shape is create-new / copy / swap. The append-only rule does not block this: it
forbids `UPDATE`/`DELETE` on ingest paths, and this is a schema migration that
only INSERTs and renames.

```sql
-- 1. New table, same columns. block_timestamp must join the PK: TimescaleDB
--    requires the partition column in every unique index on a hypertable.
CREATE TABLE uniswap_v4_position_new
    (LIKE uniswap_v4_position INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE uniswap_v4_position_new
    DROP CONSTRAINT uniswap_v4_position_new_pkey,
    ADD PRIMARY KEY (pool_id, owner, tick_lower, tick_upper, salt,
                     block_timestamp, block_number, block_version, processing_version);
SELECT create_hypertable('uniswap_v4_position_new', 'block_timestamp',
                         chunk_time_interval => INTERVAL '30 days',
                         migrate_data => false);

-- 2. Copy. Batch by block_number on a large table so one transaction does not
--    hold a snapshot for hours.
INSERT INTO uniswap_v4_position_new SELECT * FROM uniswap_v4_position;

-- 3. Swap, then re-create the indexes, the processing_version trigger and the
--    REVOKEs against the new table: LIKE carries none of them, and grants
--    follow the object, not the name.
ALTER TABLE uniswap_v4_position RENAME TO uniswap_v4_position_old;
ALTER TABLE uniswap_v4_position_new RENAME TO uniswap_v4_position;
```

**Re-solve the fan-out before shipping it.** Partitioning is what this alert asks
you to consider; it is not free, and the append-on-change read is exactly the
query it penalises. The conversion is only finished when that read prunes chunks:

1. **A wide `chunk_time_interval`** (30 days, not 1 day) — cuts the chunk count
   ~30x at no correctness cost. It interacts with compression: a chunk only
   compresses once the *whole* chunk is older than the threshold, so widen the
   compression interval to match.
2. **Pin `block_timestamp` in the `processing_version` trigger's two lookups.**
   It is functionally determined by `(block_number, block_version)` — one height
   on one fork is one block — so the equality is exact, and it prunes the
   per-inserted-row lookup to a single chunk. This is the hottest path.
3. **A bounded lookback in the read-latest query** (`readLatestPositionsV4` /
   `readLatestTicksV4`). Safe but semantically weakening: a key untouched for
   longer than the window is re-appended with identical values rather than
   skipped. Not wrong data — readers take `ORDER BY block_number DESC … LIMIT 1`
   — but it turns "append-on-change" into "append-on-change within N days" and
   must be stated in the table COMMENT if adopted.

Do **not** pin `block_timestamp` by equality in `PositionsForPoolAtBlock` /
`TicksForPoolAtBlock`: a reorged block at the same height carries a *different*
timestamp, so equality would miss exactly the prior-version rows those queries
exist to find.

### Verify recovery

If the spike was a backfill, recovery is just the rate falling back:

```promql
sum(rate(uniswap_v4_tick_rows_written_total[6h]))
  + sum(rate(uniswap_v4_position_rows_written_total[6h])) <= 2.9
```

After an actual conversion the rule no longer describes reality for the converted
table — delete or re-scope it in the same PR rather than leaving it firing
(`alerts/AGENTS.md`, alert ownership).

---

## allocation-tracker (VEC-499)

`prime-allocation-indexer` (Deployment / pod `app` label `allocation-tracker`,
OTel `service_name` `prime-allocation-indexer`) consumes Ethereum BlockEvents,
extracts ERC-20 transfers to the ALM proxies, reads end-of-block token positions
and total supplies via Multicall3, and appends `allocation_position` /
`token_total_supply` snapshots into TimescaleDB. A periodic sweep (every
`SweepEveryNBlocks`, default 75) re-reads every tracked entry to catch
transfer-less balance changes (interest accrual, rebases). Mainnet today;
avalanche/base instances stack on top (VEC-499) and reuse one `service_name`,
differing only by the `chain` label — so every alert below covers all chains
without per-instance edits.

**Metric coverage (VEC-499):** the shared `telemetry.Metrics` recorder emits one
sample per consumed block — `blocks_processed_total{service_name="prime-allocation-indexer",
chain, status}` and the seconds-bucket histogram `processing_duration_seconds`
(exported as `processing_duration_seconds_bucket`). `blocks_processed_total`
advances on every block (~12s on mainnet) regardless of position activity, so it
is the honest per-block liveness signal. The silent data-quality hole the error
path cannot catch keeps its own alert (`VectorAllocationUnderlyingValueFailures`,
below).

**`<deployment>` / `app` label per chain:** each per-chain instance has its **own**
Deployment and `app` label equal to its deployment name — mainnet →
`allocation-tracker`, avalanche → `avalanche-allocation-tracker`, base →
`base-allocation-tracker`. The Down alert carries a `deployment` label (use
`{{ $labels.deployment }}` in its commands); the chain-scoped alerts below carry
only `chain`, so substitute the matching `<deployment>` from this mapping in their
`kubectl` selectors (`-l app=allocation-tracker` matches mainnet pods only).

---

## VectorAllocationTrackerDown

**Severity:** critical · **For:** 10m

### What it means

The allocation-tracker Deployment (`{{ $labels.deployment }}` —
`allocation-tracker`, or `avalanche-`/`base-allocation-tracker` for the per-chain
instances) has <1 available replica for 10 minutes. No pod is running, so no
allocation positions or supplies are written and the SQS backlog is growing. This
is process liveness from kube-state-metrics, independent of the OTel pipeline, so
it fires even when the metrics export is the thing that broke.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app={{ $labels.deployment }}`
   (each instance's `app` label equals its deployment name).
2. **Why it's not ready** — `kubectl -n vector describe deployment/{{ $labels.deployment }}`
   and `kubectl -n vector logs -l app={{ $labels.deployment }} --previous` for a
   crash loop (missing queue URL, DB/Redis/RPC dial failure, empty primes table,
   bad axis-synome contract load).
3. **Secrets/config present** — the worker requires `AWS_SQS_QUEUE_URL`,
   `DATABASE_URL`, `ALCHEMY_API_KEY`, `REDIS_ADDR`, `S3_BUCKET`, `DEPLOY_ENV`. A
   missing key from the `allocation-tracker` ExternalSecret crashes it on startup.
4. **Node/scheduling** — a pending pod means node capacity / taints.

### Common causes

- ExternalSecret not yet synced (queue URL / DB URL missing) — the deploy ran
  before the infra apply; re-sync the ExternalSecret.
- Crash loop on a startup error (DB/Redis/RPC unreachable, `no primes found in
  database`) — fix the dependency; the worker is fail-fast by design.
- OOMKilled — check memory limits (`kubectl -n vector describe pod ...`).

### Verify recovery

`kube_deployment_status_replicas_available{deployment="{{ $labels.deployment }}"} >= 1`
and the pod logs show `running` with a nonzero `entries` count.

---

## VectorAllocationTrackerStalled

**Severity:** critical · **For:** 15m

### What it means

The worker has processed **no block *successfully* for 15 minutes** on the
labelled `chain`:
`rate(blocks_processed_total{service_name="prime-allocation-indexer", status="success"}[5m])`
is zero. The alert keys on the `status="success"` series, **not** the total: the
worker records one sample per consumed block (~12s on mainnet), so an all-error
loop keeps the *total* advancing while persisting nothing — a total-based stall
would miss it. `success == 0` makes that loop critical. The zero-success states
are all page-worthy: a wedged consume loop, an all-error loop, or a dead OTLP
export. This is **not** a quiet-market period — a healthy worker records a success
every block.

**Residual gaps (see also `VectorAllocationTrackerErrorRatioHigh` and Down):**
1. *OTLP dead but pod alive* — if the export dies, the success series
   staleness-expires and `rate(...) == 0` returns *no data*, so this alert stays
   silent. The common cause of a vanished series, process death, is caught by
   `VectorAllocationTrackerDown`; the narrow alive-but-export-dead sliver is
   accepted (a bare `rate == 0` cannot fire on an absent series). If Down is NOT
   firing but other OTel series from the pod are also flat/absent, suspect a dead
   export.
2. *All-error from the very first block* — the success series is never created, so
   this rule cannot fire; `VectorAllocationTrackerErrorRatioHigh` (100% error
   ratio) covers that case.

### First checks (≤5 min)

1. **Distinguish the cases** — is `VectorAllocationTrackerDown` also firing? If so
   the process is down (treat as Down). If not, the pod is alive: a wedged loop or
   a dead metrics export.
2. **Recent logs** — `kubectl -n vector logs -l app=<deployment> --tail=200`
   (see the intro's chain→`<deployment>` mapping). Look for a repeating error on
   one message, `context deadline exceeded` against Alchemy, or silence (poll loop
   stopped).
3. **SQS backlog** — check the allocation-tracker SQS queue depth. A growing
   `ApproximateNumberOfMessages` while the counter is flat confirms a wedged loop.
4. **All-error loop** — if logs show blocks being consumed but *every* one
   erroring (so `status="success"` is flat while `status="error"` climbs),
   `VectorAllocationTrackerErrorRatioHigh` should also be firing; treat the error
   cause (see that runbook) as the root fix.
5. **OTLP export** — if logs show blocks processing successfully but the success
   counter is flat, the metrics pipeline is the problem; check the OTel collector
   and whether other series from the pod are flat too.
6. **Upstream** — confirm the watcher for this chain is still producing blocks; if
   not, that's the root cause (`VectorWatcherNoBlocks`) and the queue is
   legitimately empty.
7. **DB liveness (`db-query`)** —
   `SELECT max(block_number), max(created_at) FROM allocation_position WHERE chain_id = <id>;`
   a frozen max block number corroborates the stall.

### Common causes

- Poison message wedging the poll loop — inspect the DLQ; redrive or purge the
  offending message.
- Alchemy RPC degraded / rate-limited — per-block multicall reads time out.
- Broken OTLP export — worker processing but metrics stopped; restart the pod or
  fix the collector.
- Per-chain queue outage — the chain's SQS/SNS wiring broke, so no blocks arrive.

### Verify recovery

`rate(blocks_processed_total{service_name="prime-allocation-indexer", status="success"}[5m]) > 0`
for the affected chain, or confirm the SQS backlog is draining.

---

## VectorAllocationTrackerErrorRatioHigh

**Severity:** warning · **For:** 15m

### What it means

More than **50% of block-processing attempts are erroring** over 10 minutes on the
labelled `chain`, with meaningful throughput (`> 0.02` blocks/s — the
minimum-volume guard). Every block that fails is propagated and redelivered by SQS
(a partial failure stops the whole block by design), so a sustained majority-error
ratio means blocks are looping without persisting — it usually precedes a stall.

This is an **error ratio** (`error / total`), not an absolute error rate, on
purpose: block cadence varies across chains (mainnet ~12s → at most ~0.083 err/s,
base ~2s → ~0.5 err/s), so a fixed per-second threshold either misses a
slow-chain failure or false-fires on a fast one. The ratio is cadence-independent
and mirrors `VectorArchivingErrorRatioHigh`. It also closes the dead zone a
`status="success"`-based stall leaves at the *start* of a worker's life: a worker
erroring from its first block never creates a success series, so Stalled cannot
fire, but its error ratio is 100% and trips this alert.

### First checks

1. **Pod logs** — `kubectl -n vector logs -l app=<deployment> | grep -i error`
   (see the intro's chain→`<deployment>` mapping). Typical: `fetch observations
   for block`, `sweep block`, `handler:`, `parse receipts`.
2. **Recent deploys** — `kubectl -n vector rollout history deploy/<deployment>`.
   A source-registry or contract-regen change (a new token type, changed
   axis-synome entries) is a common trigger.
3. **RPC health** — sustained multicall failures point at Alchemy; check the
   Alchemy status and `multicall_batch_size_count{service_name="prime-allocation-indexer"}`.
4. **DB writes** — FK/constraint errors on `allocation_position` /
   `token_total_supply`; check the Postgres dashboard and pod logs.

### Common causes

- Contract regeneration / new axis-synome entry with a token type the source
  registry does not handle -> add the source or fix the entry, then redeploy.
- Alchemy RPC timeouts / rate limits -> usually self-clears; investigate if
  sustained.
- DB write error (constraint, pool exhaustion) -> inspect the failing block.
- Per-chain queue outage — the chain's SQS/SNS wiring broke; check upstream.

### Verify recovery

The error ratio drops back under 50%:
`sum by (chain) (rate(blocks_processed_total{service_name="prime-allocation-indexer", status="error"}[10m])) / sum by (chain) (rate(blocks_processed_total{service_name="prime-allocation-indexer"}[10m])) < 0.5`
for the affected chain (and `status="success"` is climbing again).

---

## VectorAllocationTrackerBlockLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 block processing duration (`processing_duration_seconds`) exceeds 3 seconds
sustained for 15 minutes on the labelled `chain`. The indexer is degraded; blocks
risk SQS visibility-timeout redelivery and downstream allocation state lags. The
histogram uses seconds buckets (`telemetry.SecondsDurationBuckets`), so the p99
resolves honestly instead of clamping at 4.95s the way the OTel ms-scale default
buckets would.

### First checks

1. **Multicall/RPC latency** — per-block position reads and the periodic sweep
   issue batched multicalls to Alchemy; high latency there dominates block
   duration. Check `multicall_batch_size_count{service_name="prime-allocation-indexer"}`
   and the Alchemy status.
2. **Sweep cadence** — the sweep (every `SweepEveryNBlocks`, default 75) reads
   *every* tracked entry in one multicall, so its blocks are the heaviest and sit
   at the top of the p99. A larger entry set or a shorter `SWEEP_BLOCKS`
   multiplies round-trips; check the config and the `entries` count in the startup
   log.
3. **DB write latency** — confirm TimescaleDB is not under I/O pressure (Postgres
   dashboard).
4. **Pod CPU/memory** — `kubectl top pod -n vector -l app=<deployment>` (see the
   intro's chain→`<deployment>` mapping).

### Common causes

- Alchemy RPC degraded -> coordinate with infra; consider a fallback RPC.
- Sweep interval too short for the entry count -> raise `SWEEP_BLOCKS`.
- TimescaleDB I/O contention -> investigate concurrent write patterns.

### Verify recovery

`histogram_quantile(0.99, sum by (chain, le) (rate(processing_duration_seconds_bucket{service_name="prime-allocation-indexer"}[10m]))) < 3`
for the affected chain.

---

## VectorAllocationUnderlyingValueFailures

**Severity:** warning · **For:** 30m

### What it means

The prime-allocation-indexer persisted `allocation_position` rows with
`underlying_value = NULL` for a token type that should produce one
(`erc4626` / `atoken` / `erc20`). Writes succeed, so no error alert fires;
USD exposure computed from these rows silently undercounts (VEC-307).

`reason` tells you where it broke:

- `convert_failed` -- the vault's `convertToAssets(shares)` reverted or
  returned undecodable data (known case: grove-bbqUSDC-V2). Check the
  contract on Etherscan at the alerting block; if the vault genuinely has no
  working `convertToAssets`, reclassify the entry's `token_type` in the
  axis-synome export instead of leaving a permanent warning.
- `missing_asset_address` -- the axis-synome entry for a vault/atoken has no
  `asset_address`. Fix the entry in the axis-synome export; the indexer
  cannot invent a denomination.
- `asset_metadata_missing` -- should not occur: metadata for every denomination
  address is prefetched, and a fetch failure hard-fails the batch before
  persistence. If this fires, a code path built a valuation for an address the
  handler did not prefetch -- treat as a bug, not as transient RPC trouble.

### First checks

1. `sum by (token, reason) (increase(allocation_underlying_value_failures_total[6h]))`
   -- which contracts, which reason.
2. Logs: `{app="allocation-tracker"} |= "underlying value not computable"`
   -- carries token, wallet, block, reason.
3. Rows stay NULL until the next successful sweep writes new rows (the table
   is append-only; nothing backfills automatically). Consumers fall back to
   balance-based pricing for NULL rows, so impact is undercounted yield, not
   zeroed exposure.

---

## morpho VaultV2 structured tracking (VEC-218)

A Morpho VaultV2 never touches Morpho Blue directly. It holds a set of **adapter**
contracts, one per downstream venue (`MorphoMarketV1AdapterV2` wraps a Blue
market, `MorphoVaultV1Adapter` wraps a nested MetaMorpho V1 vault), and the
`morpho-indexer` derives five structured tables from the vault's own events:

| Table | Written by | Trigger |
| --- | --- | --- |
| `morpho_adapter` | adapter **identity** only — `(vault, address)` and the asset, written once and never updated | first sight of the adapter on any path |
| `morpho_adapter_membership` | append-only **observations** of whether an adapter is in the vault's set (`is_member`, `adapter_type`, `observed_via`) | `AddAdapter` / `RemoveAdapter` (transitions), `Allocate` / discovery enumeration / bootstrap seed (assertions) |
| `morpho_adapter_state` | `realAssets()` snapshot | `Allocate` / `Deallocate` |
| `morpho_vault_cap` | `(absoluteCap, relativeCap)` snapshot | the 4 `*AbsoluteCap` / `*RelativeCap` events |
| `morpho_vault_fee` | full fee-config snapshot | the 4 `Set*Fee` / `Set*FeeRecipient` events |

There is no lifecycle column anywhere. **Query `morpho_adapter_current`** for the
set an adapter is in now (it is the latest membership row per adapter, filtered to
`is_member`); the block an adapter was added at is
`MIN(block_number) FILTER (WHERE is_member AND observed_via = 'add_adapter_event')`
over `morpho_adapter_membership`, and it is **NULL** for an adapter whose
`AddAdapter` we have never witnessed until its history is replayed.

**Signals** (all carry `chain` + `cluster`):

- `morpho_v2_adapter_registrations_total{adapter_type, observed_via}` — one
  sample per adapter-membership observation **appended** to
  `morpho_adapter_membership`; what it does and does not count is stated
  canonically on `RecordAdapterMembershipObservation` in
  `stl-verify/internal/services/morpho_indexer/telemetry.go`.
  `adapter_type` is the classification the observation carried (`market_v1` |
  `vault_v1` | `unknown` = probed and unclassifiable | `unprobed` = the
  observation carried no probe, which is what a de-registration looks like);
  `observed_via` is how the membership was observed and takes the same five
  values as the DB column (`add_adapter_event` | `remove_adapter_event` |
  `allocation_event` | `vault_discovery` | `bootstrap_seed`), so the same
  question can be asked in PromQL and in SQL. `bootstrap_seed` has no writer yet:
  the Temporal bootstrap that emits it lands in the stacked #640, so the label is
  absent from the metric until that merges.
- `morpho_v2_snapshots_written_total{snapshot_type}` — one sample per **committed**
  event-driven snapshot (`adapter_state` | `vault_cap` | `vault_fee`).
  Discovery-seeded `adapter_state` rows are deliberately excluded: this counter is
  the liveness signal for the *event-driven* write path.
- `morpho_events_processed_total{event_type}` — pre-existing; every V2 event name
  is a registered topic on it, which is why the ForceDeallocate rule needs no new
  instrument.

### Why there is no VaultV2 state-freshness alert

V2 vault state is **event-driven**: a dormant vault legitimately writes nothing
for days, and that is correct behaviour, not a gap. Chain-verified in the 2026-07
vault-631 / vault-630 investigation — the apparent "staleness" moved from one
vault to the other purely with on-chain activity, the indexer resumed on the
exact first event of each gap (block 25,583,119) field-for-field correct, and
vault 630 is chain-verified silent across its whole 6-day gap. A wall-clock
freshness rule would have false-positived on both. **Do not add one.**

The only honest freshness shape is "the vault emitted an event at block N but has
no state row at N" — an audit-log comparison, not something PromQL can express
from these counters. Use it as a manual triage query (`db-query`):

```sql
-- V2 vault events with no matching vault-state row at the same block.
-- Only the state-affecting events are expected to produce a vault_state row;
-- the governance/timelock surface is audit-log only, so filter to those four.
SELECT '0x' || encode(pe.contract_address, 'hex') AS vault,
       pe.block_number,
       pe.event_name
FROM protocol_event pe
JOIN morpho_vault mv
  ON mv.address = pe.contract_address AND mv.chain_id = pe.chain_id
LEFT JOIN morpho_vault_state mvs
  ON mvs.morpho_vault_id = mv.id AND mvs.block_number = pe.block_number
WHERE mv.vault_version = 3
  AND pe.event_name IN ('Deposit', 'Withdraw', 'Transfer', 'AccrueInterest')
  AND pe.block_number > <recent_block>
  AND mvs.morpho_vault_id IS NULL
ORDER BY pe.block_number DESC
LIMIT 50;
```

### Known requirement gap (not a pipeline defect)

VaultV2 `totalAssets()` is a **virtual accruing view**: it climbs with zero
events (measured +0.053% over 6 event-free days on vault 630).
`morpho_vault_state.total_assets` has last-event-snapshot semantics — correct as
of its block, but a dormant vault's *live* TVL drifts above the newest row.
Consumers that need live TVL need periodic sampling, which is a product decision
for the team, not something to patch into this pipeline. Flagged here so it is
not mistaken for a bug during triage.

---

## VectorMorphoV2UnknownAdapters

**Severity:** warning · **For:** 30m (on a 24h window)

### What it means

`morpho-indexer` recorded more than 25 VaultV2 adapters as `adapter_type=unknown`
in 24 hours on the labelled `chain`. The classifier probes two selectors on every
adapter — `morpho()` (`0xd8fbc833`) and `morphoVaultV1()` (`0xe4baaddf`) — and
records Unknown (DB `adapter_type = 99`) unless **exactly one** answers. There are
therefore **two** Unknown arms (`classifyAdapter`, `adapter_probe.go`), and they
mean different things:

- **Both revert** — the contract serves neither marker. A family the probe does
  not model; the fix is a new marker selector.
- **Both succeed** — the contract serves *both* markers, so the probe cannot
  choose. A hybrid adapter, or a proxy/fallback that answers any selector. A third
  selector does not help here: the fix is a tie-break rule (e.g. prefer the more
  specific marker, or discriminate on a shape only one family has).

Those adapters are still registered and their `realAssets()` still tracked, so this
is not data loss; what is lost is venue attribution, so nothing downstream can say
what backs the exposure.

**A non-zero Unknown count is normal.** The VEC-218 ticket originally proposed
"Unknown count stays 0" as the sentinel; live mainnet validation disproved it —
real adapters exist whose both getters revert, and one 8-minute discovery window
produced 7+ Unknown registrations. Only a sustained wave is actionable, which is
what the threshold encodes: the live registration path (`AddAdapter`) fired just
5 times in 7 days across *all* types on prod mainnet, so >25/day can only come
from a mass discovery burst or a genuinely new adapter family.

**Expect one firing during the initial VaultV2 bootstrap**, when every existing V2
vault is discovered at once. Acknowledge and curate.

### First checks

1. **Which provenance** — `sum by (observed_via) (increase(morpho_v2_adapter_registrations_total{adapter_type="unknown"}[24h]))`.
   All `vault_discovery` (or `bootstrap_seed`) means a bootstrap/backfill burst
   (benign, but still curate). Any meaningful `add_adapter_event` share means a
   new family is shipping live. Until the stacked #640 merges, `bootstrap_seed`
   never appears — its Temporal writer does not exist yet — so do not read its
   absence as evidence about where a burst came from.
2. **Identify the adapters** (`db-query`):

   ```sql
   SELECT '0x' || encode(c.address, 'hex') AS adapter,
          '0x' || encode(v.address, 'hex') AS vault,
          c.as_of_block,
          (SELECT MIN(m.block_number) FILTER (WHERE m.is_member AND m.observed_via = 'add_adapter_event')
           FROM morpho_adapter_membership m
           WHERE m.morpho_adapter_id = c.id) AS added_at_block
   FROM morpho_adapter_current c
   JOIN morpho_vault v ON v.id = c.morpho_vault_id
   WHERE c.adapter_type = 99
   ORDER BY c.as_of_block DESC
   LIMIT 50;
   ```

   `morpho_adapter_current` is already restricted to adapters currently in their
   vault's set, so no "not removed" predicate is needed. `added_at_block` is NULL
   for an adapter whose `AddAdapter` we never witnessed (discovered mid-life, or
   seeded by the bootstrap); that is expected, not a gap in this triage.

3. **Re-probe one on-chain** to confirm the classification is genuinely absent
   rather than an RPC artefact (a transport error propagates and never reaches
   Unknown, so it should not be, but confirm):

   ```bash
   cast call <adapter> "morpho()(address)"         --rpc-url https://ethereum-rpc.publicnode.com
   cast call <adapter> "morphoVaultV1()(address)"  --rpc-url https://ethereum-rpc.publicnode.com
   ```

   **Read which arm you are in.** Both reverting = an unmodelled family, so go to
   check 4. Both *answering* = the probe could not choose, which needs a tie-break
   rule rather than a third selector — expect a hybrid adapter or a
   proxy/fallback that answers any selector, and check the contract before
   assuming either family.
4. **Find the real type** — read the contract on Etherscan and look for the
   marker getter of the new family (e.g. a `compoundV3()` / `erc4626()` style
   accessor). That selector is the fix.
5. **Indexer logs** — `kubectl -n vector logs -l app=morpho-indexer | grep "unknown type"`
   carries vault, adapter and block for every Unknown registration.

### Common causes

- Morpho deployed a new adapter family the 2-selector probe does not model →
  extend `adapter_probe.go` with the new marker selector and its
  `entity.MorphoAdapterType`, then **replay / re-seed the affected vaults** so the
  extended probe APPENDS a corrected classification. `morpho_adapter_current` is
  latest-row-wins, so the new observation supersedes the type-99 one; there is no
  in-place fix available — UPDATE is revoked on `morpho_adapter_membership`. See
  the replay note under Verify recovery for
  [`VectorMorphoV2LazyAdapterRegistrations`](#vectormorphov2lazyadapterregistrations).
- A mass discovery burst (bootstrap, or a wave of new V2 vaults) surfacing the
  known long tail of unclassifiable adapters all at once → curate, no code change.

### Verify recovery

`increase(morpho_v2_adapter_registrations_total{adapter_type="unknown"}[24h]) <= 25`
for the affected chain, and the type-99 query above returns only adapters you
have consciously accepted.

---

## VectorMorphoV2LazyAdapterRegistrations

**Severity:** warning · **For:** 30m (on a 6h window)

### What it means

More than 3 adapter memberships were **inferred from an allocation** in 6 hours on
the labelled `chain`. An `Allocate` / `Deallocate` proves its adapter is in the
vault's set — the contract cannot allocate to an unregistered adapter — so when the
membership log has no answer at that position the indexer classifies the adapter
on-chain and records the membership the event implies, rather than hard-failing and
poisoning the FIFO queue.

A `RemoveAdapter` for an unknown adapter is **not** part of this path and does not
count here: it is recorded as one untyped `is_member = false` observation, which is
the truthful record and needs no classification.

The inference itself is correct. What it *signals* is a discovery gap: vault
discovery enumerates the vault's **current** adapter set (`adaptersLength()` /
`adapters(i)`, hash-pinned) and records every entry, so once a vault is discovered
the log already answers every allocation, nothing is appended, and this counter's
steady-state rate is **zero**.

**The one benign source is deterministic, not a race.** Discovery records its
enumeration at `log_index = EndOfBlockLogIndex` (MaxInt32) so it orders above every
log in the discovery block, while the membership read is position-scoped
(`(block_number, block_version, log_index) <= …`). A VaultV2 emits `AccrueInterest`
— the discovery trigger — first in the very transaction that allocates, so every
allocation in that same block reads strictly *below* the seed, finds no answer, and
appends. Expect exactly one append per adapter allocated in the discovery block,
every single time; nothing has to have changed between two reads. Its signature in
the query below is `blocks_after_discovery = 0`.

It also costs data quality — but not the way a mutable registry did. Nothing is
approximated: an adapter known only from an `Allocate` simply has **no**
`add_adapter_event` observation, so its add block is NULL until its history is
replayed. Current membership and classification are correct in the meantime.

### First checks

1. **Is it one new vault or many?** A mid-life discovery produces one append per
   adapter the vault allocates to in the discovery block — deterministically, per
   the mechanism above — so a wave of new vaults produces a small, one-off burst.
   Correlate with the discovery path:
   `sum by (observed_via) (increase(morpho_v2_adapter_registrations_total[6h]))`
   — `allocation_event` observations with **no** matching `vault_discovery`
   traffic in the same window are the suspicious case.
2. **Identify them** — the indexer logs one WARN per inference:
   `kubectl -n vector logs -l app=morpho-indexer | grep "membership inferred from an Allocate"`
   (carries vault, adapter, block).
3. **Was the vault genuinely new?** Compare the block the membership was inferred
   at against its vault's first-seen block (`db-query`):

   ```sql
   SELECT '0x' || encode(v.address, 'hex') AS vault,
          v.created_at_block AS vault_first_seen_block,
          '0x' || encode(a.address, 'hex') AS adapter,
          MIN(m.block_number) FILTER (WHERE m.observed_via = 'allocation_event') AS inferred_at_block,
          MIN(m.block_number) FILTER (WHERE m.is_member AND m.observed_via = 'add_adapter_event') AS added_at_block,
          MIN(m.block_number) FILTER (WHERE m.observed_via = 'allocation_event') - v.created_at_block
              AS blocks_after_discovery
   FROM morpho_adapter_membership m
   JOIN morpho_adapter a ON a.id = m.morpho_adapter_id
   JOIN morpho_vault v ON v.id = a.morpho_vault_id
   WHERE v.vault_version = 3
   GROUP BY v.address, v.created_at_block, a.address
   HAVING MIN(m.block_number) FILTER (WHERE m.observed_via = 'allocation_event') IS NOT NULL
   ORDER BY blocks_after_discovery DESC
   LIMIT 50;
   ```

   `blocks_after_discovery = 0` is the same-block signature above — benign, and the
   expected shape, not a coincidence. A large positive value on a long-known vault
   means enumeration missed the adapter, which is the bug. A NULL `added_at_block`
   is the replay backlog, not a second fault.
4. **Cross-check the chain** — for a suspect vault, ask the contract directly and
   compare with the registry:

   ```bash
   cast call <vault> "adaptersLength()(uint256)" --rpc-url https://ethereum-rpc.publicnode.com
   cast call <vault> "adapters(uint256)(address)" <i> --rpc-url https://ethereum-rpc.publicnode.com
   ```

### Common causes

- A wave of newly discovered V2 vaults (or the initial bootstrap) → benign and
  expected: each contributes one append per adapter allocated in its discovery
  block. Confirm via `blocks_after_discovery = 0` and let it clear.
- `readV2Adapters` enumeration regression (truncated list, wrong selector, a
  failed sub-read defaulting to empty) → adapters are missing from every newly
  discovered vault; this is the bug the alert exists to catch.
- Vault registry losing known vaults (e.g. repeated re-discovery after restarts)
  → adapters look absent on every restart.

### Verify recovery

`increase(morpho_v2_adapter_registrations_total{observed_via="allocation_event"}[6h]) <= 3`
for the affected chain. If the cause was an enumeration bug, also replay the
affected vaults: the replay appends each adapter's real `AddAdapter` observation
at its own block, which is what turns a NULL add block into the true one.

---

## VectorMorphoV2ForceDeallocateSurge

**Severity:** warning · **For:** 0m (on a 1h window)

### What it means

More than 20 VaultV2 `ForceDeallocate` events landed in one hour on the labelled
`chain`. `forceDeallocate()` is the **permissionless emergency exit**: anyone can
force liquidity out of an adapter, paying a penalty, when the vault's idle balance
cannot serve a withdrawal.

**This is a protocol-side signal, not an indexer fault.** Individual events are
routine — measured on prod mainnet over 7 days: 76 events total, peak 24 in a 24h
window, peak 7 in a 1h window. That is why there is no ">0 sentinel" (it would
fire ~11x/day and get muted). At >20/h the character changes: that is no longer
arbitrage trimming the edges, it is depositors paying the penalty *en masse*
because a vault cannot meet withdrawals from idle liquidity — a liquidity-run
shape worth handing to risk.

The rule is gated on a redelivery **loop** — errors present *and* zero successful
blocks over the same 1h window, with the success side zero-filled from the total
so a fresh pod that only ever errors still counts as zero — because
`RecordEventProcessed` increments **before** dispatch. On a redelivery loop every
log in the stuck block is re-counted, so one FIFO-blocked block holding two
`ForceDeallocate` logs crosses 20/h on redelivery alone (12/h at the 300s
visibility timeout) with no new on-chain activity at all.

The gate deliberately does **not** trip on a single error: a lone transient
429/5xx used to blind this alert for the rest of the hour (~2.3h blind per 7d).
The price is that a stall's *first* hour can still fire this, because the success
rate needs the full 1h window to fall to zero — so rule out a stall first.

### First checks

1. **Confirm the indexer is not stalled or redelivering.** Expect this alert
   *alongside* a stall, not instead of one — and if it is firing during an error
   loop, treat the count as unreliable and fix the stall first:
   `rate(morpho_blocks_processed_total{status="error"}[1h])` and
   `VectorMorphoIndexerStalled`. The gate suppresses a chain that is erroring and
   committing nothing, but a loop's first hour — and the tail of one that has just
   cleared — can still leave inflated counts inside the 1h window.
2. **Which vault and adapter** (`db-query`) — the events are audit-logged with
   their raw payload:

   ```sql
   SELECT '0x' || encode(contract_address, 'hex') AS vault,
          block_number,
          event_data
   FROM protocol_event
   WHERE event_name = 'ForceDeallocate'
     AND created_at > now() - interval '1 hour'
     AND block_number > <block_1h_ago>
   ORDER BY block_number DESC;
   ```

   The `created_at` bound is not redundant: on `protocol_event` `created_at` **is**
   the block timestamp and is the partition column, so it is what prunes chunks —
   `block_number` alone scans every one of them.

   `event_data` carries the raw topics/data; decode against
   `stl-verify/internal/pkg/blockchain/abis/vault_v2_events_abi.go` for
   `adapter`, `assets`, `onBehalf` and `penaltyAssets`.
3. **Is it one vault or many?** One vault = a vault-specific liquidity squeeze.
   Many = a market-wide event; check whether the underlying asset is depegging or
   a large Blue market is at full utilisation.
4. **Idle liquidity trend** — the accompanying `Deallocate` events snapshot each
   adapter's `realAssets()`, so the drain is visible in `morpho_adapter_state`:

   ```sql
   SELECT s.block_number, '0x' || encode(a.address, 'hex') AS adapter, s.real_assets
   FROM morpho_adapter_state s
   JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
   WHERE a.morpho_vault_id = <vault_id>
     AND s.timestamp > now() - interval '7 days'
   ORDER BY s.block_number DESC
   LIMIT 100;
   ```

   Bound on `timestamp`, **not** `created_at`. On the VaultV2 tables `timestamp` is
   the block time and the partition column; `created_at` is processing time (wall
   clock at insert) and never block time, so bounding on it prunes nothing and
   silently excludes backfilled rows. Widen the interval if the drain predates it.

5. **Indexer logs** — every event is WARNed with full context:
   `kubectl -n vector logs -l app=morpho-indexer | grep forceDeallocate`.

### Common causes

- A poison pill in the morpho FIFO queue → `RecordEventProcessed` fires before
  dispatch, so every redelivery re-counts every log in the stuck block. Two
  `ForceDeallocate` logs in one blocked block reach 20/h on redelivery alone. The
  rule's errors-and-no-successes gate suppresses this once the chain has committed
  nothing for a full hour, but the loop's first hour and the tail of one that has
  just cleared can still leave inflated counts inside the 1h window. Not a
  liquidity event — fix the stall.
- A large withdrawal against a vault whose liquidity is fully allocated into
  adapters → the exit path is working as designed; inform risk, no code action.
- An underlying Blue market at ~100% utilisation, so normal deallocation cannot
  source assets → protocol-side, monitor.
- A depeg / incident on the vault's underlying asset driving a coordinated exit.

**Nothing here is an indexer fix.** Do not silence by raising the threshold
without a new baseline measurement.

### Verify recovery

`increase(morpho_events_processed_total{event_type="ForceDeallocate"}[1h]) <= 20`
for the affected chain.

---

## VectorMorphoV2NoSnapshotsWritten

**Severity:** warning · **For:** 15m (on a 6h window)

### What it means

VaultV2 allocation / cap / fee events have been processed for 6 hours on the
labelled `chain`, but **zero** structured snapshots were committed
(`morpho_v2_snapshots_written_total` is zero or absent). The events are landing in
`protocol_event` as audit rows while `morpho_adapter_state` / `morpho_vault_cap` /
`morpho_vault_fee` quietly stop filling, and **no error is raised** — this is the
silent-data-hole guard that neither the Stalled nor the ErrorsHigh rule can see.

The classic mechanism is dispatch drift: `processMetaMorphoLog`'s `switch` ends in
`default: return nil`, so an event whose typed handler is removed or whose case
stops matching is audit-logged and reported as success.

**Activity-aware by construction.** The left side *is* the gate: no V2 events
means no expectation of rows and the rule stays silent, so a dormant vault
population cannot false-positive it. This is deliberately **not** a wall-clock
staleness rule — see "Why there is no VaultV2 state-freshness alert" above.

This is **half one** of a two-rule silent-empty guard.
[`VectorMorphoV2NoStructuredEvents`](#vectormorphov2nostructuredevents) is half
two and covers the class this rule structurally cannot: an event feed that is
*always* empty zeroes this rule's own left side and makes it un-fireable.

Scope: the counters are summed across snapshot types, and Allocate/Deallocate
traffic on mainnet is continuous (~25k events/7d), so this catches a **total**
write-path failure. The loss of one snapshot type alone (e.g. caps only, ~104
events/7d) will not fire it.

Both sides use the same 6h window, deliberately. Cap (~104/7d) and fee (~15/7d)
sparsity is what forces 6h on the left; a narrower right side would false-fire on
every ordinary stall, since a 30m-quiet indexer trivially has zero snapshots
against 6h of remembered events — and the Stalled rule already owns that
condition. The cost is detection latency: a deploy that breaks the write path is
not caught until the 6h rate window has aged out the last pre-deploy snapshot
sample, so **worst case ~6h15m** (6h window + 15m `for`).

### First checks

1. **Rollout ordering first.** The alert fires when the counter series is
   *absent*, so it fires in the window between the alert rules syncing and the
   morpho-indexer image that emits `morpho_v2_snapshots_written_total` actually
   serving. Confirm the running image carries VEC-218 before investigating
   anything else:
   `kubectl -n vector get deploy morpho-indexer -o jsonpath='{.spec.template.spec.containers[0].image}'`.
   If it predates the metric, this is expected and clears ~15m after the rollout.
2. **Which side is dead** —
   `sum by (event_type) (increase(morpho_events_processed_total{event_type=~"Allocate|Deallocate|Increase.*Cap|Decrease.*Cap|Set.*Fee.*"}[6h]))`
   versus
   `sum by (snapshot_type) (increase(morpho_v2_snapshots_written_total[6h]))`.
   Events non-zero with snapshots absent confirms the write path, not the feed.
3. **Confirm against the DB** (`db-query`) — metrics could be lying:

   ```sql
   SELECT max(block_number) AS last_adapter_state FROM morpho_adapter_state
    WHERE timestamp > now() - interval '7 days';
   SELECT max(block_number) AS last_cap FROM morpho_vault_cap
    WHERE timestamp > now() - interval '7 days';
   SELECT max(block_number) AS last_fee FROM morpho_vault_fee
    WHERE timestamp > now() - interval '7 days';
   ```

   `morpho_adapter_state` is a hypertable partitioned on `timestamp`, so that bound
   is what prunes chunks — an unbounded `max()` scans the whole table.
   `morpho_vault_cap` / `morpho_vault_fee` (and `morpho_adapter_membership`) are
   plain tables with nothing to prune; keep them bounded on `timestamp` or
   `block_number` anyway, for row count rather than pruning. Never bound any of
   these on `created_at` — it is processing time here, not block time. A `max()`
   that comes back NULL means nothing landed in the window, which is the answer
   this check is looking for.

   If these are advancing while the counter is flat, the problem is the OTLP
   export, not the pipeline.
4. **Dispatch drift** — diff `processMetaMorphoLog`'s `switch` against the
   registered topic list in `event_extractor.go`. A topic present in the
   extractor's typed-extraction list but missing a `case` in the handler switch
   is the bug.
5. **Vault version guard** — every V2 handler starts at `resolveV2Vault`, which
   errors on a vault recorded as V1/V1.1. That path *errors* (so it would show up
   as a stall, not here), but a vault mis-recorded as V2 that never reaches a
   handler would look like this. Check
   `SELECT vault_version, count(*) FROM morpho_vault GROUP BY 1;`.

### Common causes

- Alert rules synced ahead of the indexer rollout → expected, self-clears.
- A typed handler removed or its `case` dropped from `processMetaMorphoLog` →
  events audit-log without producing rows. This is the bug the alert exists for.
- Broken OTLP export while the pipeline is healthy → DB max block numbers
  advance while the counter is flat (check 3 above).

### Verify recovery

`rate(morpho_v2_snapshots_written_total[6h]) > 0` for the affected chain, and the
three `max(block_number)` queries above tracking the chain head.

---

## VectorMorphoV2NoStructuredEvents

**Severity:** warning · **For:** 15m (on a 6h window)

### What it means

Blocks are processing successfully (`morpho_blocks_processed_total{status="success"}`
is non-zero) but **not one** VaultV2 allocation / cap / fee event has been decoded
(`morpho_events_processed_total` for the V2 event set is zero or absent) for 6
hours on the labelled `chain`.

This is **half two** of the silent-empty guard, and it covers what
[`VectorMorphoV2NoSnapshotsWritten`](#vectormorphov2nosnapshotswritten)
structurally cannot: that rule gates on this same V2 event feed, so a feed that is
*always* empty makes its left side absent and the rule un-fireable. Nothing else
in the group would page — `morpho_blocks_processed_total` keeps advancing happily,
and no error is ever raised.

Mainnet prod runs ~25k `Allocate`/`Deallocate` per 7d (~150/h), so 6h of zero V2
events is orders of magnitude outside anything observed. It is not a lull.

### First checks

1. **Topic registration** — the most likely cause. Confirm the V2 topics are still
   in the extractor's registered set and still resolve to typed events:
   diff `event_extractor.go`'s V2 topic table against the `event_type` values the
   metric last carried
   (`count by (event_type) (morpho_events_processed_total)` over a wider window,
   e.g. `[30d]`, to see which names have gone missing).
2. **V2 vault population** — a registry holding no V2 vault means
   `IsVaultActivityEvent` never routes a log into the V2 handlers:

   ```sql
   SELECT vault_version, count(*) FROM morpho_vault GROUP BY 1;
   ```

   Zero rows at `vault_version = 3` is the answer.
3. **Genuinely no V2 activity on-chain** — confirm against the chain that the
   known V2 vaults really emitted nothing in 6h (cast, or the Morpho explorer).
   For the mainnet population this would be extraordinary; if a chain is added
   whose V2 population is legitimately empty, gate the rule on that chain rather
   than silencing it.
4. **Audit log cross-check** (`db-query`) — metrics could be lying:

   ```sql
   SELECT event_name, max(block_number)
     FROM protocol_event
    WHERE created_at > now() - interval '1 hour'
      AND event_name IN ('Allocate', 'Deallocate', 'SetPerformanceFee')
    GROUP BY 1;
   ```

   Rows advancing while the counter is flat means the OTLP export is broken, not
   the pipeline.

### Common causes

- V2 topic dropped from the extractor's registered set → logs are never decoded,
  so nothing reaches `processMetaMorphoLog`.
- ABI / signature change on the vault contracts → the registered topic hashes no
  longer match the emitted logs.
- Vault registry holds no V2 vault (migration not applied, discovery regression)
  → every V2 log is filtered out before dispatch.
- Broken OTLP export while the pipeline is healthy → `protocol_event` advances
  while the counter is flat (check 4 above).

### Verify recovery

`rate(morpho_events_processed_total{event_type=~"Allocate|Deallocate|Increase.*Cap|Decrease.*Cap|Set.*Fee.*"}[6h]) > 0`
for the affected chain.

---
