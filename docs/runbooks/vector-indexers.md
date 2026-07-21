# Vector — indexers runbook

Owner: vector team · Source rules: [alerts/vector-indexers.yaml](../../alerts/vector-indexers.yaml)

The Vector indexers (`stl-morpho-indexer`, `stl-oracle-indexer`) are the last
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

### First checks (≤5 min)

1. **Pod status** — run the one matching the firing alert:
   - Morpho: `kubectl -n vector get pods -l app=stl-morpho-indexer`
   - Oracle: `kubectl -n vector get pods -l app=stl-oracle-indexer`
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

`rate({morpho,oracle}_blocks_processed_total) > 0` for the affected chain.

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

**Severity:** warning · **For:** 0m (1h window debounces)

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
volume**: it fires only when a token's collateral stays unpriceable across
consecutive cycles for **>30m** (`increase[20m] > 0` held for `30m`, per token).
A lone gap self-heals within a cycle and never fires; a sustained gap is an
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

- **Transient (fires once, resolves within ~1h):** expected self-heal. No
  action beyond the one-time task below.
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
(`increase[20m] > 0` for `30m`, per token).

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
>120s sustained over 10m. The upstream feed has silently gone dead; snapshots
are stale and stale symbols stop being written, so the series flat-lines.

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

1. **Pod status** — `kubectl -n vector get pods -l app=allocation-tracker` (the
   `app` label is shared across all per-chain instances).
2. **Why it's not ready** — `kubectl -n vector describe deployment/{{ $labels.deployment }}`
   and `kubectl -n vector logs -l app=allocation-tracker --previous` for a crash
   loop (missing queue URL, DB/Redis/RPC dial failure, empty primes table, bad
   axis-synome contract load).
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

The worker is **up** but has consumed **no block for 15 minutes** on the labelled
`chain`: `rate(blocks_processed_total{service_name="prime-allocation-indexer"}[5m])`
is zero. The worker consumes ~1 BlockEvent per block (~12s on mainnet) and records
one sample per consumed block regardless of whether a position was touched, so
this is **not** a quiet-market period — the SQS consume loop is wedged (process
alive but not draining).

**Residual gap — OTLP dead but pod alive:** if the pod is alive but the OTLP
metric export dies, the `blocks_processed_total` series staleness-expires and
`rate(...) == 0` returns *no data*, so this alert stays silent rather than firing.
The common cause of a vanished series — process death — is caught by
`VectorAllocationTrackerDown` instead; the narrow alive-but-export-dead sliver is
an accepted gap (a bare `rate == 0` cannot fire on an absent series). If Down is
NOT firing but other OTel series from the pod are also flat/absent, suspect a dead
export.

### First checks (≤5 min)

1. **Distinguish the cases** — is `VectorAllocationTrackerDown` also firing? If so
   the process is down (treat as Down). If not, the pod is alive: a wedged loop or
   a dead metrics export.
2. **Recent logs** — `kubectl -n vector logs -l app=allocation-tracker --tail=200`.
   Look for a repeating error on one message, `context deadline exceeded` against
   Alchemy, or silence (poll loop stopped).
3. **SQS backlog** — check the allocation-tracker SQS queue depth. A growing
   `ApproximateNumberOfMessages` while the counter is flat confirms a wedged loop.
4. **OTLP export** — if logs show blocks still processing but the counter is flat,
   the metrics pipeline is the problem; check the OTel collector and whether other
   series from the pod are flat too.
5. **Upstream** — confirm the watcher for this chain is still producing blocks; if
   not, that's the root cause (`VectorWatcherNoBlocks`) and the queue is
   legitimately empty.
6. **DB liveness (`db-query`)** —
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

`rate(blocks_processed_total{service_name="prime-allocation-indexer"}[5m]) > 0`
for the affected chain, or confirm the SQS backlog is draining.

---

## VectorAllocationTrackerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

`blocks_processed_total{status="error"}` is above 0.1 errors/sec sustained for 15
minutes on the labelled `chain`. Every block that fails is propagated and
redelivered by SQS (a partial failure stops the whole block by design), so a
sustained error rate means blocks are looping without persisting — it usually
precedes a stall.

### First checks

1. **Pod logs** — `kubectl -n vector logs -l app=allocation-tracker | grep -i error`.
   Typical: `fetch observations for block`, `sweep block`, `handler:`,
   `parse receipts`.
2. **Recent deploys** — `kubectl -n vector rollout history deploy/{{ $labels.deployment }}`.
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

`rate(blocks_processed_total{service_name="prime-allocation-indexer", status="error"}[10m]) < 0.1`
for the affected chain.

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
4. **Pod CPU/memory** — `kubectl top pod -n vector -l app=allocation-tracker`.

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
