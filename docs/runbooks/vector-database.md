# Vector — database runbook

Owner: vector team · Source rules: [alerts/vector-database.yaml](../../alerts/vector-database.yaml)

Every Vector **Go** service reaches Postgres (TimescaleDB on TigerData) through
one pool builder, `stl-verify/internal/adapters/outbound/postgres/db.go`. That
builder attaches a pgx tracer which counts every query, batch, copy and
connection it makes. Every other Vector counter is per-service and domain-shaped
(blocks, reorgs, backfill gaps), so this is the only per-service view of the
shared database dependency.

`python-api` runs in the same namespace but reaches the same database through
SQLAlchemy/asyncpg, so it emits none of these counters — and it is not a minor
omission: it produced 174 of the 202 `out of memory` log lines in the worst
morning hour of 2026-08-25. Absence of a Vector database alert does **not** mean
the database is healthy for the API.

**Why it exists:** on 2026-08-25 the staging database returned SQLSTATE 53200
(`out_of_memory`) across the fleet from 07:00 to 15:00 UTC — 444 log lines over
15 services, peaking at 211 lines across 14 services in the 14:00 hour, of which
arbitrum-watcher absorbed 90 — with no *per-service* signal. The instance alerts
fired (see below); nothing said which services were being refused, or on what.
Workers retried and recovered, so nothing looked broken while writes failed and
were re-driven all day. The failure recurred on 2026-08-27.

**This is not the only database alerting — but for every class-53 cause except
`53200` it is the only alerting there is.** `TigerDataMemoryPressure` /
`TigerDataMemoryPressureCritical` (>75% / >85%), `TigerDataMemoryMetricsMissing`,
the long-running-transaction rules and the WAL-archive rules live in the
infrastructure repo (`alerts/orbit/orbit-tigerdata.yaml`) with their own runbook,
[tigerdata-memory-exhaustion.md](https://github.com/archon-research/infrastructure/blob/main/docs/runbook/tigerdata-memory-exhaustion.md).
Every one of those watches memory, WAL or transaction age. **Nothing watches
connection count or disk** — Grafana carries no connection-count metric for the
instance at all, and `timescale_cloud_system_disk_usage_bytes` has no matching
`_total_bytes` to form a ratio against. So a `53300` or `53100` storm fires
nothing upstream and leaves a memory dashboard that looks fine. Only `53200` has
a companion alert, and only that code should send you to memory first.

**Even for `53200`, `…Critical` is not a reliable companion.** On 2026-08-27 the
failure recurred with memory at **76.7%** in the hour the errors landed (03:00–
04:00 UTC; the whole day peaked at 80.3%). The >75% warning fired for 18 minutes
of that day — and `…Critical` recorded **0 firing and 0 pending minutes**, it did
not fire at all. On 2026-08-25 it did: memory reached **87.6% at 14:27 UTC** and
87.1% just before 10:00, and `…Critical` fired in four episodes (08:08, 08:30,
09:52, 14:29 UTC) inside nine warning episodes spanning 07:12–14:37. Read a
silent `…Critical` as "the instance stayed under 85%", never as "the fleet was
served". Query these at 1m resolution rather than reading the TigerData console,
whose default range averages the spikes away.

**Read this first:** workers retry and recover from database errors, so the
pipeline looks healthy while writes fail and are re-driven. Absence of a visible
outage is not evidence that these alerts are benign.

## The metrics

| Metric | Labels | Use |
|---|---|---|
| `db_query_total` | — | Denominator: every traced operation, successful or not |
| `db_query_errors_total` | `error_class` | What the alert rules key on |
| `db_query_errors_by_sqlstate_total` | `sqlstate`, `error_class` | Triage breakdown |

`error_class` is a closed domain — `resources` (SQLSTATE class 53),
`retryable` (40001 / 40P01), `unknown` (no SQLSTATE: the error never reached the
server), `other` (everything else). All four series are seeded at 0 when a pool
is built, which is what lets `increase()` see a class's *first* error as a 0→1
transition and gives the ratio rule's numerator a series to divide. SQLSTATE
cannot be seeded, so alert on `error_class` and use
`db_query_errors_by_sqlstate_total` only to break an alert down — and break it
down with the raw counter, not `increase()`, which returns 0 for a `sqlstate`
series whose first sample is the error you are looking for.

`db_query_total` counts what pgx traces, so it includes the implicit
`BEGIN`/`COMMIT`/`ROLLBACK` of every transaction and the `SET` that
`WorkerDBConfig`'s AfterConnect hook issues on each new connection. On a
transaction-heavy repository that is roughly 3x the statements the calling code
wrote. Read the ratio as "share of traced operations", not "share of my queries".

Pool-exhaustion timeouts (`Acquire` giving up before a connection frees) are
returned by `pgxpool` before any tracer runs, so they are not counted anywhere
here. Connect failures — including `53300 too_many_connections` — are counted.

---

## VectorDatabaseResourceErrors

**Severity:** warning · **Window:** 15m · **Grouping:** per cluster, not per service

### What it means

Postgres returned a SQLSTATE class-53 error (`insufficient_resources`) to one or
more Vector Go services. The four members share a class and nothing else — a
different resource, a different first check, and a different amount of upstream
alerting:

| SQLSTATE | Meaning | Resource | Instance alert |
|---|---|---|---|
| `53200` | `out_of_memory` — a backend could not allocate | instance memory | `TigerDataMemoryPressure(Critical)`, unreliably (see above) |
| `53300` | `too_many_connections` | connection slots, i.e. pool sizing | none |
| `53100` | `disk_full` | disk headroom, retention, tiering | none |
| `53400` | configuration limit exceeded | a server limit (`max_locks_per_transaction`, prepared statements) | none |

The rule is fleet-level on purpose: this is a fault in the one dependency every
service shares, so it sends one notification per cluster rather than one per
affected service. It is a warning, not a page — for `53200` the page belongs to
`TigerDataMemoryPressureCritical`, and a second critical here paged twice for
one incident. That is the only code with an upstream page: for the other three
this rule is the whole signal, which is why the first check below is *get the
code*, not *open the memory dashboard*.

### First checks (≤5 min)

1. **Get the code, and who is affected** — the raw counter, graphed over the
   incident window (not `increase()`, which is blind to a `sqlstate` series'
   first sample):
   `sum by (service_name, sqlstate) (db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", error_class="resources"})`
   Everything after this branches on the code. Do not open the memory dashboard
   before you have it — three of the four codes have nothing to do with memory.
2. **Check python-api separately** — it emits none of these counters, whichever
   the code is. Loki, with the message text that matches (`out of memory`,
   `too many connections`, `No space left on device`):
   `{k8s_namespace_name="vector", service_name="python-api"} |= "out of memory"`.
   In the 08:00 hour on 2026-08-25 it was 86% of the fleet's failures.

#### `53200` — instance memory

3. **Instance headroom** — `100 * timescale_cloud_system_memory_usage_bytes /
   timescale_cloud_system_memory_total_bytes{service_id="…"}`, at 1m resolution
   rather than in the console, whose default range averages spikes away.
   Staging is `xd7na17213`, prod `ucpymqz73b`. A reading under 85% clears
   nothing: the 08-27 recurrence refused work at 76.7%.
4. **Do not hunt the query in the error message** — the failing statements are
   usually trivial indexed lookups (`get last block`, `get block by hash`).
   That is the signature of a server-side memory ceiling: the small queries are
   victims failing to allocate, not the cause. Go to Common causes below.

#### `53300` — connection slots

Pool sizing, not memory, and Grafana carries no connection-count metric for the
instance, so count them at the source over a read-only session (`stl_read_only`).

3. **What is actually connected** —
   `SELECT state, count(*) FROM pg_stat_activity GROUP BY 1` against
   `SHOW max_connections`. Idle-in-transaction sessions hold a slot as firmly
   as active ones and are the usual surprise.
4. **What the fleet asks for** — `MinConns` × replicas, summed over the
   services. `WorkerDBConfig`'s `MinConns` was cut to 1 fleet-wide in PR #585
   for exactly this failure; a service that overrides it, or a replica-count
   bump, undoes that. `MaxConns` bounds the burst, `MinConns` the floor every
   replica holds permanently.
5. **Correlate with a rollout** — a new worker or a scale-up lands its floor
   immediately, so `53300` starting at a deploy boundary points at the change,
   not at the database.

#### `53100` — disk

3. **Disk headroom** — `timescale_cloud_system_disk_usage_bytes{service_id="…"}`.
   There is no exported total, so compare it against the plan's allocated
   storage in the TigerData console; this is why no instance alert covers it.
4. **Are the policies running** — a stalled retention, compression or tiering
   job is the usual cause, and it fails silently:
   `SELECT job_id, last_run_status, last_successful_finish FROM timescaledb_information.job_stats WHERE last_run_status <> 'Success'`.
5. **What is growing** — order hypertables by size and check the largest have
   a retention or tiering policy at all, rather than assuming they do.

#### `53400` — a configured server limit

3. Read the error text: it names the limit. `max_locks_per_transaction` is the
   one this schema can reach (a statement touching many chunks takes a lock per
   chunk), which makes it the same over-chunking problem as `53200` — see
   VEC-663 below — rather than an independent fault.

### Common causes of `53200`

The other codes are diagnosed by their first checks above; only memory needs a
cause list, because the error never names it.

- **Per-query planner and executor memory on over-chunked hypertables.** This is
  the measured driver, not maintenance work. A single call of a routine API
  query against `allocation_position` (130 MB over 176 chunks) allocated 724 MB,
  holding one sort open per chunk under a Merge Append, and re-planned on every
  call. A handful of concurrent requests is enough to exhaust an 8 GB instance.
  Check chunk counts (`timescaledb_information.chunks`) for the tables in the
  plan, and `pg_stat_statements.plans` vs `calls` for re-planning. See VEC-663.
- **A single high-frequency query spilling repeatedly.** Death by a thousand
  cuts, not one big query: order `pg_stat_statements` by `temp_blks_written` and
  check `calls` — the top consumer has been one application query at ~10 MB per
  call across ~5,900 calls.

Concurrent TimescaleDB maintenance jobs are a plausible-looking cause that has
**not** held up: staging runs ~103 policy jobs that fire in tight clusters, but
capping `maintenance_work_mem` was measured to change neither compression time
nor the failure. Don't spend time there before the two causes above.

### Fixing it

For `53300`, cut the floor the fleet holds (`MinConns`) or the replica count —
raising `max_connections` moves the same memory problem to `53200`, since each
backend costs memory. For `53100`, restart the stalled policy and give the
growing hypertable a retention or tiering policy; adding disk buys weeks.

For `53200` the durable fixes are per-query — chunk intervals and the spilling
queries themselves — not server knobs. Postgres memory settings are role/database-level
(`ALTER ROLE … SET`, `ALTER DATABASE … SET`), which migrations may not do: they
need privileges the migration role does not hold and belong in the infra repo's
`bootstrap-db.sh`. See `db/migrations/AGENTS.md` ("Role admin vs object
grants"). Note `timescaledb.max_background_workers` is
postmaster-context and `autovacuum_max_workers` is sighup-context, so neither can
be set that way at all. Resizing the instance is the fallback, not the
first move.

### Verify recovery

The alert auto-resolves when
`increase(db_query_errors_total{error_class="resources"}[15m])` returns to 0.
Because workers retry, also confirm the pipeline caught up rather than assuming
it: the watchers' `backfill_watermark_lag` should be draining toward zero.

---

## VectorDatabaseErrorRatioHigh

**Severity:** warning · **For:** 15m

### What it means

A service is failing more than 5% of its traced database operations, sustained
over 15m, counting `error_class` `resources`, `other` and `unknown` — the
serialization/deadlock codes are covered by
VectorDatabaseSerializationErrorsUnretried.

This is the "one service is broken" shape: schema drift after a deploy, a
revoked grant, an adapter issuing invalid SQL. A resource storm is the opposite
shape — a small share of failures across a healthy fleet. On 2026-08-25 the
fleet's worst hour was 0.059 errors/sec against an instance committing 50–98
transactions/sec, so the ratio was well under 1% throughout (an estimate:
`db_query_total` did not exist yet). That storm is covered by
VectorDatabaseResourceErrors above. If the failures here *are* class-53, read
that section: which resource ran out depends on the SQLSTATE, and only `53200`
has an instance alert to corroborate it.

It is a ratio rather than an absolute rate because query volume across the fleet
spans orders of magnitude, from a watcher querying continuously to a cronjob
issuing a handful an hour. The second conjunct is a minimum-volume guard on the
*denominator* (>0.02 traced operations/sec), matching
`VectorAllocationTrackerErrorRatioHigh`: it stops a near-idle service firing on
a 100% ratio built from one failed query, without imposing the queries/sec floor
the ratio form exists to avoid.

### First checks (≤5 min)

1. **Classify by code** —
   `sum by (sqlstate) (rate(db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", service_name="<service>"}[10m]))`

   | SQLSTATE | Meaning | Usual cause |
   |---|---|---|
   | `42501` | `insufficient_privilege` | an ingest path attempted UPDATE/DELETE on an append-only table |
   | `42P01` / `42703` | undefined table / column | schema drift: a deploy landed ahead of its migration |
   | `55P03` | `lock_not_available` | `WorkerDBConfig`'s 10s `lock_timeout` fired — a lock convoy |
   | `57014` | `query_canceled` | a server-side cancel: a `statement_timeout` set outside this repo, or `pg_cancel_backend`. A client-side cancel lands in `unknown`, not here |
   | `23505` | unique violation | a replay writing duplicate rows — check `processing_version` |
   | `unknown` | never reached the server | connection reset, context cancellation |

2. **Correlate with the last deploy** — `42P01`/`42703` appearing at a rollout
   boundary is a migration-ordering problem, not a database fault.

### Common causes

- **`42501` after an append-only conversion.** A converted table has `UPDATE`
  and `DELETE` revoked; an adapter still issuing `ON CONFLICT … DO UPDATE` fails
  at executor start whether or not a conflict occurs. See
  `db/migrations/AGENTS.md`.
- **`55P03` on a latency-bounded worker.** `WorkerDBConfig` sets a 10s
  `lock_timeout` so a lock convoy surfaces as a fast error rather than a hang;
  a burst of these means something is holding the lock — usually an
  idle-in-transaction session or a TimescaleDB policy job on the same chunks.
- **A burst of `unknown` at a rollout.** In-flight queries cancelled by a
  graceful shutdown land in `unknown`. A deploy is seconds long and cannot
  sustain the 15m dwell; if `unknown` *is* sustained, look for connection resets
  between the pod and the database rather than at a deploy.

### Fixing it

Fix the query or the adapter — do not raise `work_mem` or add a
`statement_timeout` to silence it. Note that raising `work_mem` is particularly
counterproductive on these instances: it is per-sort-node, so it multiplies
across concurrent backends and moves the box closer to the class-53 failures
above.

### Verify recovery

The alert auto-resolves when the error ratio for the labelled service falls
below 5% (or its traced-operation rate below 0.02/s).

---

## VectorDatabaseSerializationErrorsUnretried

**Severity:** warning · **Window:** 1h

### What it means

A service that does **not** retry serialization failures (`40001`) or deadlocks
(`40P01`) hit six or more of them in an hour.

The only retry for these codes in the repo is `isRetryableTxError`
(`stl-verify/internal/adapters/outbound/postgres/blockstate_repository.go`),
which wraps `SaveBlock` and `HandleReorgAtomic` — so only the watchers retry.
(Note `isRetryableError` in the same file is a *different* predicate: it retries
any error that is not a context cancellation, and does not key on SQLSTATE.)
Nothing in the other ~30 repositories retries these codes, which is why the rule
excludes only the services that do retry (`service_name!~".*watcher"`) instead
of dropping the codes fleet-wide.

Everywhere else the abort rolls back the whole unit of work. The worker returns
an error, does not ack, and SQS redrives the message — so a one-off is already
handled and is not actionable. A *sustained* rate means two writers are
genuinely contending, and messages are heading for the DLQ.

### First checks (≤5 min)

1. **Serialization or deadlock** — the raw counter, graphed over the last hour
   (`increase()` is blind to a `sqlstate` series' first sample):
   `sum by (service_name, sqlstate) (db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", error_class="retryable"})`
2. **Who else is writing the same table** — a deadlock needs two writers. Check
   whether a backfiller or a bootstrap job is running against the same tables as
   the alerting worker; that is the usual pairing.
3. **Check the DLQ depth** for the alerting worker's queue in the SQS console.
   No alert covers this; it is a manual check, and a growing DLQ means the
   contention is already costing messages.

### Common causes

- **A backfiller racing a live indexer** over the same hypertable chunks.
  Stagger them, or let the backfiller finish before resuming the worker.
- **Serializable transactions over a wide row range.** A transaction that
  touches more rows than it needs widens the conflict window; narrow the
  statement's predicate.

### Fixing it

Reduce the contention rather than adding a blanket retry: a retry loop around a
non-idempotent write is a correctness problem, and the append-only tables make
"just re-run it" a new-version write, not a no-op. If a retry genuinely is the
right answer for a path, use `retry.Do` with `isRetryableTxError`, matching
`BlockStateRepository`, and make sure the retried unit of work is a whole
transaction.

### Verify recovery

The alert auto-resolves when
`increase(db_query_errors_total{error_class="retryable"}[1h])` for the labelled
service drops below 6. Confirm in the SQS console that the worker's DLQ stopped
growing.
