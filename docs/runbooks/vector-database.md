# Vector — database runbook

Owner: vector team · Source rules: [alerts/vector-database.yaml](../../alerts/vector-database.yaml)

Every Vector service reaches Postgres (TimescaleDB on TigerData) through one
pool builder, `stl-verify/internal/adapters/outbound/postgres/db.go`. That
builder attaches a pgx tracer which counts every query, batch and copy in the
fleet. Every other Vector counter is per-service and domain-shaped (blocks,
reorgs, backfill gaps), so this is the only per-service view of the shared
database dependency.

**Why it exists:** on 2026-08-25 the staging database returned SQLSTATE 53200
(`out_of_memory`) to the watcher fleet for six hours — arbitrum-watcher absorbed
13 in a single hour — with no per-service signal. Workers retried and recovered,
so nothing looked broken while writes failed and were re-driven all day. The
failure recurred on 2026-08-27.

**This is not the only database alerting, and is not the first thing to check.**
`TigerDataMemoryPressure` / `TigerDataMemoryPressureCritical` (>75% / >85%),
`TigerDataMemoryMetricsMissing`, and the WAL-archive rules live in the
infrastructure repo (`alerts/orbit/orbit-tigerdata.yaml`) with their own runbook,
[tigerdata-memory-exhaustion.md](https://github.com/archon-research/infrastructure/blob/main/docs/runbook/tigerdata-memory-exhaustion.md).
Those watch the *instance*; these rules watch *which services are being refused
and on what*. On a resource fault expect both to fire — start there for the
cause, come here for the blast radius.

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
is built, which is what lets `increase()` catch the *first* error of an
incident. SQLSTATE cannot be seeded, so alert on `error_class` and use
`db_query_errors_by_sqlstate_total` only to break an alert down.

Pool acquisition failures (dial errors, pool exhaustion) never reach the tracer
— pgx returns them before it runs — so they are not counted anywhere here.

---

## Resource errors (SQLSTATE class 53)

**No alert of its own.** Paging for this is owned by
`TigerDataMemoryPressureCritical` in the infrastructure repo, which watches the
instance directly — a second critical here paged twice for one incident. These
errors still surface per-service through VectorDatabaseErrorRatioHigh, and this
section is the diagnostic reference for them.

### What it means

Postgres returned a SQLSTATE class-53 error (`insufficient_resources`) to one or
more Vector services. The members that matter here:

| SQLSTATE | Meaning |
|---|---|
| `53200` | `out_of_memory` — a backend could not allocate |
| `53300` | `too_many_connections` |
| `53100` | `disk_full` |
| `53400` | configuration limit exceeded |

This is a fault in a shared dependency: on 2026-08-25 it hit six services at
once, so expect sibling alerts. The rule keeps `service_name` so a single
service can be silenced or routed on its own; collapsing the incident into one
page is Alertmanager's `group_by`, not the rule's job.

### First checks (≤5 min)

1. **Which code, and who is affected** — break the metric down:
   `sum by (sqlstate, service_name) (increase(db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", error_class="resources"}[15m]))`
2. **Confirm at the source** — the failing statements are usually trivial
   indexed lookups (`get last block`, `get block by hash`). That is the
   signature of a *server-side* memory ceiling, not an expensive query: the
   small queries are victims failing to allocate, not the cause. Do not go
   hunting the query in the error message.
3. **Instance headroom** — `100 * timescale_cloud_system_memory_usage_bytes /
   timescale_cloud_system_memory_total_bytes{service_id="…"}`. The TigerData
   metric exporter feeds these into Grafana, so query them at 1m resolution
   rather than reading the console, whose default range averages spikes away.
   Staging is `xd7na17213`, prod `ucpymqz73b`.

### Common causes

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
- **`53300` too_many_connections** — check pool sizing. `MinConns` was cut to 1
  in PR #585 for exactly this; a service that overrides it can undo that.
- **`53100` disk_full** — check retention and tiering policies are running.

Concurrent TimescaleDB maintenance jobs are a plausible-looking cause that has
**not** held up: staging runs ~103 policy jobs that fire in tight clusters, but
capping `maintenance_work_mem` was measured to change neither compression time
nor the failure. Don't spend time there before the two causes above.

### Fixing it

The durable fixes are per-query — chunk intervals and the spilling queries
themselves — not server knobs. Postgres memory settings are role/database-level
(`ALTER DATABASE … SET`), which migrations may not do: they require superuser and
belong in the infra repo's `bootstrap-db.sh`. See `db/migrations/AGENTS.md`
("Role admin vs object grants"). Note `timescaledb.max_background_workers` is
postmaster-context and `autovacuum_max_workers` is sighup-context, so neither can
be set that way at all. Resizing the instance is the fallback, not the
first move.

### Verify recovery

`increase(db_query_errors_total{error_class="resources"}[10m])` should return to
0. Because workers retry, also confirm the pipeline caught up rather than
assuming it: the watchers' `backfill_watermark_lag` should be draining toward
zero.

---

## VectorDatabaseErrorRatioHigh

**Severity:** warning · **For:** 15m

### What it means

A service is failing more than 5% of its database operations, sustained over
15m, counting `error_class` `resources`, `other` and `unknown` — the
serialization/deadlock codes are covered by
VectorDatabaseSerializationErrorsUnretried.

If the failures are class-53 (`resources`), the instance is out of memory,
connections or disk: see [Resource errors](#resource-errors-sqlstate-class-53)
above, and expect `TigerDataMemoryPressureCritical` to be the page that matters.

It is a ratio rather than an absolute rate because query volume across the fleet
spans several orders of magnitude, from a watcher issuing hundreds of queries a
second to a cronjob issuing a handful an hour. A second conjunct requires at
least ~0.01 errors/sec so a near-idle service cannot fire on a 100% ratio built
from a single failed query.

### First checks (≤5 min)

1. **Classify by code** —
   `sum by (sqlstate) (rate(db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", service_name="<service>"}[10m]))`

   | SQLSTATE | Meaning | Usual cause |
   |---|---|---|
   | `57014` | `query_canceled` | `statement_timeout` hit — a query regressed, or a plan flipped |
   | `42501` | `insufficient_privilege` | an ingest path attempted UPDATE/DELETE on an append-only table |
   | `42P01` / `42703` | undefined table / column | schema drift: a deploy landed ahead of its migration |
   | `23505` | unique violation | a replay writing duplicate rows — check `processing_version` |
   | `unknown` | never reached the server | connection reset, context cancellation |

2. **Correlate with the last deploy** — `42P01`/`42703` appearing at a rollout
   boundary is a migration-ordering problem, not a database fault.

### Common causes

- **`42501` after an append-only conversion.** A converted table has `UPDATE`
  and `DELETE` revoked; an adapter still issuing `ON CONFLICT … DO UPDATE` fails
  at executor start whether or not a conflict occurs. See
  `db/migrations/AGENTS.md`.
- **`57014` on a latency-bounded worker.** `WorkerDBConfig` sets a 10s
  `lock_timeout`; services that also set `StatementTimeout` will surface a
  regressed query here first.
- **A burst of `unknown` at a rollout.** In-flight queries cancelled by a
  graceful shutdown land in `unknown`. A deploy is seconds long and cannot
  sustain the 15m dwell; if `unknown` *is* sustained, look for connection resets
  between the pod and the database rather than at a deploy.

### Fixing it

Fix the query or the adapter — do not raise `work_mem` or `statement_timeout` to
silence it. Note that raising `work_mem` is particularly counterproductive on
these instances: it is per-sort-node, so it multiplies across concurrent
backends and moves the box closer to the class-53 failures above.

### Verify recovery

The alert auto-resolves when the error ratio for the labelled service falls
below 5% (or its absolute error rate below 0.01/s).

---

## VectorDatabaseSerializationErrorsUnretried

**Severity:** warning · **For:** 15m

### What it means

A service that does **not** retry serialization failures (`40001`) or deadlocks
(`40P01`) hit more than five of them in an hour.

The only retry for these codes in the repo is `isRetryableTxError`
(`stl-verify/internal/adapters/outbound/postgres/blockstate_repository.go`),
which wraps `SaveBlock` and `HandleReorgAtomic` — so only the watchers retry.
(Note `isRetryableError` in the same file is a *different* predicate: it retries
any error that is not a context cancellation, and does not key on SQLSTATE.)
Nothing in the other ~30 repositories retries these codes, which is why the rule
scopes its exclusion to `service_name=~".*watcher"` instead of dropping the
codes fleet-wide.

Everywhere else the abort rolls back the whole unit of work. The worker returns
an error, does not ack, and SQS redrives the message — so a one-off is already
handled and is not actionable. A *sustained* rate means two writers are
genuinely contending, and messages are heading for the DLQ.

### First checks (≤5 min)

1. **Serialization or deadlock** —
   `sum by (sqlstate, service_name) (increase(db_query_errors_by_sqlstate_total{k8s_namespace_name="vector", error_class="retryable"}[1h]))`
2. **Who else is writing the same table** — a deadlock needs two writers. Check
   whether a backfiller or a bootstrap job is running against the same tables as
   the alerting worker; that is the usual pairing.
3. **Check the DLQ depth** for the alerting worker's queue. If it is growing,
   the contention is already costing messages.

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
service drops back to 5 or fewer. Confirm the worker's DLQ stopped growing.
