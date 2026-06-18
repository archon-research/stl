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

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)
