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

**Severity:** warning · **For:** 15m

### What it means

The worker is **up** (≥1 replica) but has executed **no VaultResolver multicalls
for 30 minutes**. A healthy worker on mainnet advances
`multicall_batch_size_count` regularly (startup reconcile + every block touching
a known vault), so a sustained zero means one of: a benign quiet period (no
in-scope sUSDS-debt vault touched, no restart), a wedged poll loop (process alive
but not processing), or a broken OTLP export (worker fine, metrics stopped — in
which case `VectorFluidVaultIndexerDown` is **not** firing and other OTel series
from the pod also go flat).

### First checks (≤5 min)

1. **Distinguish the cases** — is `VectorFluidVaultIndexerDown` also firing? If
   so the process is down (treat as Down). If not, the pod is alive — suspect a
   wedged loop or a metrics-pipeline issue.
2. **Recent logs** — `kubectl -n vector logs -l app=fluid-vault-indexer --tail=200`.
   Look for a repeating error on one message, `context deadline exceeded` against
   the Alchemy RPC, or silence (poll loop stopped).
3. **SQS backlog** — check the `stl-<env>-ethereum-fluid_vault.fifo` queue depth.
   Growing `ApproximateNumberOfMessages` + zero multicalls = wedged (not
   draining); near-empty queue + zero multicalls = genuine quiet period (benign).
4. **Upstream** — confirm the ethereum watcher is still producing blocks; if not,
   that's the root cause (`VectorWatcherNoBlocks`).

### Common causes

- Poison message wedging the poll loop — inspect the DLQ; redrive or purge the
  offending message.
- Alchemy RPC degraded / rate-limited — multicalls time out; check logs and the
  Alchemy status.
- Genuine quiet period on a low-activity target debt token — no action; clears
  once a vault is next touched.

### Verify recovery

`increase(multicall_batch_size_count{service_name="fluid-vault-indexer"}[30m]) > 0`,
or confirm the SQS backlog is draining and recent logs show snapshots written.

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

### First checks

1. **SweepBlocks config** — if `SWEEP_BLOCKS=0` (disabled) and no
   blocks contain Curve events, `buildSnapshotSet` legitimately returns empty.
   Confirm there is genuine pool activity on-chain or re-enable sweep.
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
The 30m rate window tolerates a genuinely quiet block with no Uniswap V3
events; there is no sweep to guarantee periodic writes the way curve's
`SWEEP_BLOCKS` does, so 30m of zero on an otherwise-active chain is the
actionable signal.

### What it means

Blocks are advancing (`uniswap_v3_blocks_processed_total{status="success"}` is
non-zero) but no state/tick snapshot rows have been written
(`uniswap_v3_state_rows_written_total` is zero) for 30 minutes. The error path
will NOT catch this: a quietly-empty touched-pool set (e.g. `DueSet` always
returns empty, or every `snapshotDueSet` call silently no-ops) produces no
errors, just no state rows.

This is the data-quality / silent-empty check that `VectorUniswapV3IndexerStalled`
cannot see.

### First checks

1. **On-chain activity** — confirm there is genuine Uniswap V3 swap/mint/burn
   activity on this chain in the window; a real quiet period is not a bug.
2. **Touched pools** — check whether any registered pool addresses match logs
   in the processed blocks. `kubectl logs -l app=uniswap-v3-indexer` should
   show pool-touch debug entries (or absence thereof).
3. **Pool registry** — if the pool list is empty (`LoadPools` returned 0
   rows), `DueSet` can never produce entries. Confirm the DB has rows in the
   Uniswap V3 pool registry for this chain.
4. **No separate DueSet-size metric** is emitted; use
   `uniswap_v3_state_rows_written_total` as the proxy. A sudden drop to zero
   after previously non-zero is more urgent than a fresh deploy with no
   history.

### Common causes

- No Uniswap V3 events on this chain in 30m (legitimate low-activity window)
  -> confirm on-chain before escalating.
- Pool registry empty (migration not applied, wrong chain ID) -> verify the
  Uniswap V3 pool registry row count for this chain.
- Contract address mismatch (new pool deployed at a different address) ->
  update the pool registry.
- Sustained SQS replay / redrive over an already-indexed range under one
  `build_id`: every message is a block already persisted at this build (same
  `build_id`, same `block_version`), so each state INSERT hits `ON CONFLICT DO
  NOTHING` (0 rows) and `uniswap_v3_state_rows_written_total` does not advance
  even though processing succeeds. A redeploy (new `build_id`) or reorg (new
  `block_version`) inserts fresh rows and clears the alert. Check the queue
  for a redrive before assuming a logic stall.

### Verify recovery

`rate(uniswap_v3_state_rows_written_total[30m]) > 0` for the affected chain,
or confirm on-chain that no Uniswap V3 activity occurred (legitimate quiet
window).

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
