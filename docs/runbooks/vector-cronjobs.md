# Vector — Temporal cronjobs runbook

Owner: vector team · Source rules: [alerts/vector-cronjobs.yaml](../../alerts/vector-cronjobs.yaml)

These alerts cover every Temporal cronjob built on the shared `cronjobWorkflow`
(`stl-verify/internal/adapters/outbound/temporal/`). Each cronjob runs on a
schedule in the `vector` Temporal namespace and snapshots an external source
into TimescaleDB (or validates stored data). Current cronjobs:

| Cronjob (`service_name`) | Deployment | Interval | Does |
|---|---|---|---|
| `anchorage-indexer` | `spark-anchorage-indexer` | 15m | Anchorage collateral / operations snapshots |
| `offchain-price-indexer` | `offchain-price-indexer` | 5m | CoinGecko token prices |
| `watcher-data-validator` | `watcher-data-validator` | 1h | Validates stored chain data vs Etherscan |
| `transform-worker` | `transform-worker` | 10m | Drains the transformed-layer change queues and refreshes the parity ledger |
| `db-statement-stats` | `db-statement-stats` | 1m | Exports per-table INSERT write cost from `pg_stat_statements` as OTel counters |
| `offchain-price-backfill` | `offchain-price-backfill` | **on demand** | Backfills CoinGecko price history for a range supplied at trigger time |

> `maple-graphql-indexer` is also a cronjob but has its own richer rules — see
> [vector-indexers.md](vector-indexers.md), not this runbook.

The shared activity records `cronjob_runs_total{status="success"|"error"}` and
`cronjob_run_duration_seconds` per run, labelled `service_name=<cronjob>`. New
cronjobs are covered automatically; only the availability rule needs the new
Deployment name added to its regex — `VectorCronjobWorkerDown` for a scheduled
cronjob, `VectorOnDemandWorkerDown` for an on-demand worker.

> `transform-worker` ships at `replicas: 0` and is enabled (scaled to 1) only after
> the one-off bootstrap has run. `VectorCronjobWorkerDown` is guarded on
> `kube_deployment_spec_replicas > 0`, so a deliberately scaled-to-zero deployment
> does not page (see that section).

General triage:

```bash
kubectl -n vector get pods -l app=<deployment>      # pod health
kubectl -n vector logs deploy/<deployment> --tail=200
```

Temporal UI (vector namespace) → Schedules → `<cronjob>` shows recent runs and
the failure stack for each.

---

## VectorCronjobRunFailing

**Severity:** warning · **For:** 0m

### What it means

The labelled cronjob recorded at least one failed run in the last 15m. Recorded
once per activity execution, so a transient failure the retry policy later
recovers also fires this — that is intentional (you want to see flapping). Data
may be partial or briefly stale.

A run interrupted by worker shutdown (a deploy rollout landing mid-run, or a
schedule cancel) is recorded as `status="canceled"`, not `status="error"`, and
does not fire this alert — Temporal retries that run on the new worker.

### First checks (≤5 min)

1. **Logs** — `kubectl -n vector logs deploy/<deployment> --tail=200`; find the
   error class (upstream API 4xx/5xx, DB error, decode/parse failure).
2. **Upstream API** — most cronjobs pull an external API (Anchorage,
   CoinGecko, Etherscan). Check for outage / rate limiting / auth failure.
3. **Temporal UI** — inspect the failed run's stack trace.

### Common causes

- External API rate limit or transient 5xx → usually self-heals on the next
  cycle; confirm `status="success"` returns.
- Credential expiry (API key rotated) → update the secret and restart.
- DB connection issue → check the Postgres dashboard.

### Verify recovery

`increase(cronjob_runs_total{status="error", service_name="<cronjob>"}[15m]) == 0`
and a fresh `status="success"` run.

---

## VectorCronjobAllRunsFailing

**Severity:** critical (pages) · **For:** 15m

### What it means

Zero successful runs **and** at least one failure over the last 1h for the
labelled cronjob. The worker is up and emitting metrics, but every run errors —
its TimescaleDB output is going stale. This is the sustained-failure escalation
of `VectorCronjobRunFailing`.

### First checks

1. Everything under `VectorCronjobRunFailing` — but the failure is now
   persistent, so look for a hard, non-transient cause.
2. **Recent deploys** — `kubectl -n vector rollout history deploy/<deployment>`;
   a bad release is the most common persistent cause. Roll back if so.
3. **Upstream contract / schema change** — a changed external API response or
   on-chain contract upgrade can break every run until the code is updated.
4. **Credentials / config** — a wrong or expired secret fails 100% of runs.

### Verify recovery

A `status="success"` run appears and the rule clears (errors-with-no-success
no longer holds over 1h).

---

## VectorCronjobWorkerDown

**Severity:** critical (pages) · **For:** 10m

### What it means

The labelled Deployment has <1 available replica for >10m. The cron worker is
not running, so its Temporal schedule fires with no worker to pick it up — no
snapshots/validations are produced. Sourced from kube-state-metrics, so it fires
even when the OTLP/metrics pipeline is the thing that broke (a dead worker emits
no `cronjob_runs_total`, so the metric-based alerts can't see it).

The alert is guarded on `kube_deployment_spec_replicas > 0`, so a deployment
deliberately scaled to zero does not page. This matters for triage: a
scaled-to-zero deployment (e.g. `transform-worker` before it is enabled) is
fully silent — no page and no metrics — which is expected, not an outage.
`transform-worker` must only be scaled to 1 after the one-off bootstrap has run;
if it is enabled first, the first tick runs the full parity verify inline and can
run long. Keep that rollout order explicit in the deploy notes.

### First checks

1. **Pod status** — `kubectl -n vector get pods -l app=<deployment>`. Look for
   `CrashLoopBackOff`, `OOMKilled`, `ImagePullBackOff`, `Pending`.
2. **Describe** — `kubectl -n vector describe pod <pod>` for the failure reason
   (scheduling, image, resource limits).
3. **Logs of the crashing container** —
   `kubectl -n vector logs <pod> --previous`.

### Common causes

- Crash on startup after a bad deploy → roll back.
- OOM → raise the memory limit.
- Bad image ref / registry auth → fix the tag / pull secret.
- Node pressure / unschedulable → check Karpenter and node capacity.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="<deployment>", namespace="vector"} >= 1`
and a fresh `status="success"` run in `cronjob_runs_total`.

## VectorOnDemandWorkerDown

### What it means

A `temporal.RunWorker` Deployment has had <1 available replica for >30m. These
workers carry **no schedule**, so unlike `VectorCronjobWorkerDown` nothing is
ticking into the void and no data is going stale. The only impact is that a new
run cannot be started until the pod is back. Warning severity for that reason.

Currently matches: `offchain-price-backfill`.

### First checks

1. `kubectl -n vector get pods -l app=offchain-price-backfill` — look for
   `CrashLoopBackOff`, `ImagePullBackOff` or `OOMKilled`.
2. `kubectl -n vector logs deploy/offchain-price-backfill --tail=100`.
3. If the worker is up but a *run* is failing, this is the wrong alert — see
   `VectorCronjobRunFailing`, which is the only run-failure signal for on-demand
   workers (`VectorCronjobAllRunsFailing` excludes them).

### Common causes

- **`ImagePullBackOff`** — much the most likely. `cmd/backfillers/` is not
  auto-discovered, so the release needs its explicit
  `_docker-release-offchain-price-backfill-internal` line in `docker-release-all`
  **and** its entry in `deploy.yaml`'s `CRONJOBS` promotion list. Missing either
  ships a tag nothing built.
- **Missing config/secret** — the pod fails at startup wiring; the log names the
  variable (e.g. `required env var COINGECKO_API_KEY is not set`).

### Verify recovery

`kube_deployment_status_replicas_available{deployment="offchain-price-backfill"} >= 1`,
then start the smoke workflow below and confirm it completes.

---

### Special case: `offchain-price-backfill` (on-demand, no schedule)

How to actually run a backfill:
[docs/backfilling-offchain-prices.md](../backfilling-offchain-prices.md).


This Deployment is an **on-demand** Temporal worker (`temporal.RunWorker`), not a
scheduled cronjob. Two things differ when it pages:

- **Nothing is missed while it is down.** It has no schedule, so there is no tick
  firing into the void and no data going stale. The impact is only that a backfill
  cannot be *started* until it is back. Triage it, but it is not a data-loss page.
- **It does emit `cronjob_runs_total`, but one record per *chunk*, not per run.**
  `RunWorker` instruments activities via an interceptor, so a 162-chunk backfill
  emits 162 records. `VectorCronjobRunFailing` (warning) therefore covers it, but
  it is deliberately **excluded from `VectorCronjobAllRunsFailing`** (critical):
  zero successes in an hour is this job's normal idle state, so that rule would
  page on any single failed manual trigger. Verify recovery by confirming the pod
  is available and that a test workflow completes:

  ```
  temporal workflow start --namespace vector \
    --task-queue offchain-price-backfill --type OffchainPriceBackfill \
    --workflow-id backfill-smoke-$(date +%s) \
    --input '{"assets":["weth"],"from":"2026-07-01T00:00:00Z","to":"2026-07-08T00:00:00Z"}'
  ```

  A one-week window is one chunk and completes in seconds. Safe to run, but not
  a no-op: the pod you just recovered is normally a new image, hence a new
  `build_id`, and `assign_processing_version_offchain_token_price` only reuses a
  version for the same build. The smoke run therefore appends a fresh
  `processing_version` generation for that one week. That is additive and read
  paths take the newest, so it is harmless — but prefer staging if you would
  rather not add a generation in prod.
- **A coverage failure fires no alert at all.** The interceptor records per
  activity, so a run whose chunks all succeeded but whose *workflow* then failed
  its `assertCoverage` check — an asset that returned nothing, or a gap after data
  began — emits only `status="success"` and never trips
  `VectorCronjobRunFailing`. This is the one failure mode metrics cannot see. It
  is tolerable only because the job is hand-triggered: the run goes red in the
  Temporal UI with the offending asset named in the error, in front of the person
  who started it. **Do not treat a green dashboard as evidence a backfill was
  complete** — read the workflow's own outcome, or the `progress` query, which is
  still readable after the run has closed.

`ImagePullBackOff` here most often means the image was never built — the binary
lives under `cmd/backfillers/`, which is **not** auto-discovered, so it needs its
explicit `_docker-release-offchain-price-backfill-internal` line in
`docker-release-all` and its entry in `deploy.yaml`'s `CRONJOBS` promotion list.

---

## VectorDbStatementStatsNoInsertActivity

**Severity:** warning · **For:** 30m

### What it means

The `db-statement-stats` worker is available but no INSERT calls have reached
`db_statements_insert_calls_total` in 15m — the counters are flat, or the series
is missing entirely.

This job is the one cronjob whose failure the generic rules cannot see. Its only
output is metrics, so "read `pg_stat_statements` and publish nothing" completes
without error and records `status="success"`; the per-table write-cost panels
simply go blank. The app database is written to continuously by the watcher and
the indexers, so zero observed INSERT activity means the measurement path is
broken, not that the database is idle.

Nothing is lost while this fires — no ingest depends on this job. The cost is
that per-table write-cost history has a hole for the duration.

### First checks (≤5 min)

1. **Is the series there at all?** In Grafana, query
   `db_statements_insert_calls_total{service_name="db-statement-stats"}`. No
   series at all points at export or startup; a flat series points at the
   database read.
2. **Logs** — `kubectl -n vector logs deploy/db-statement-stats --tail=100`.
   A failing read logs the wrapped `querying pg_stat_statements` error, and the
   startup line warns explicitly if `OTEL_EXPORTER_OTLP_ENDPOINT` is unset.
3. **Is the extension still there?** It is a prerequisite owned by the infra
   repo's `bootstrap-db.sh`, not by a migration (`CREATE EXTENSION
   pg_stat_statements` needs superuser, and migrations run as `stl_migrator`):

   ```sql
   SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
   SHOW shared_preload_libraries;   -- must list pg_stat_statements
   ```

   Both are required: the extension needs the library preloaded at server start,
   so a restart that lost the `shared_preload_libraries` setting breaks the read
   even though the extension row still exists.

### Common causes

- **`OTEL_EXPORTER_OTLP_ENDPOINT` unset** — the OTel providers install as silent
  no-ops, so the job runs perfectly and exports nothing. Confirm the Deployment
  still has `envFrom: configMapRef: otel-config`; this is exactly how
  `offchain-price-indexer` exported nothing for months.
- **Extension dropped or not preloaded** after a database restart, resize or
  restore → reapply the infra bootstrap.
- **`DATABASE_URL` pointing at the wrong database** — the read is scoped to
  `current_database()`, so a worker aimed elsewhere sees no INSERTs into our
  tables. Note this is legitimately quiet on a database nobody writes to.
- **Every fingerprint evicted each tick** — if `pg_stat_statements.max` is far
  too small for the workload, no fingerprint survives between two readings, so
  every sighting is a fresh baseline and every delta is zero. Check
  `SELECT count(*) FROM pg_stat_statements` against the configured max.

### Verify recovery

`sum(rate(db_statements_insert_calls_total{service_name="db-statement-stats"}[15m])) > 0`
and per-table series visible on the write-cost dashboard.

### Persistent tick failure: unparseable INSERT

A different shape of failure, which arrives via `VectorCronjobRunFailing` /
`VectorCronjobAllRunsFailing` rather than through this alert. The logs show:

```
cannot read INSERT target table from "INSERT INTO ..."
```

The job deliberately fails the whole tick when it cannot read a statement's target
table, rather than attributing that write cost to nothing or to a truncated name.
That is the right trade — a silent hole in write-cost data is worse than a loud stop
— but it means one unrecognised statement shape halts the export until the code
changes. It does not self-heal: the same statement is still in `pg_stat_statements`
next tick, so every tick fails identically.

The error quotes the offending statement. Two sites must change **together**, or the
job either keeps failing or starts under-reporting:

1. **The target parser** — `insertTarget` in
   `stl-verify/internal/services/db_statement_stats/service.go`. It matches the full
   identifier, quoted or bare, and requires a trailing delimiter so a name it cannot
   read in full fails instead of matching a prefix (this is what makes
   `INSERT INTO café` an error rather than a table called `caf`).
2. **The SQL filter** — the `s.query ~*` clause in
   `stl-verify/internal/adapters/outbound/postgres/statement_stats_repository.go`,
   which decides what reaches the parser at all.

Widening the filter without teaching the parser the new shape turns this into a
guaranteed tick failure. Narrowing the filter to dodge one statement silently drops
that table's cost — prefer teaching the parser.

The known gap is the CTE-prefixed write (`WITH … INSERT INTO …`): the anchored filter
never returns it, so it is under-reported rather than misattributed. Introducing one
into an ingest path means extending both sites.

---

## Adding a new cronjob

Failure + all-failing alerts are automatic (they group by `service_name`).
`VectorCronjobAllRunsFailing` excludes `maple-graphql-indexer` and
`offchain-price-backfill`; `VectorCronjobRunFailing` excludes only maple. Two
manual steps:

1. Add the new **Deployment name** to the `deployment=~"..."` regex in the
   availability rule that matches its lifecycle — `VectorCronjobWorkerDown` for
   a scheduled cronjob, `VectorOnDemandWorkerDown` for a `temporal.RunWorker`
   job. (The kube-state-metrics label is the Deployment name, which may differ
   from `service_name` — e.g. `spark-anchorage-indexer`.) An on-demand worker
   must ALSO be added to the `service_name!=` exclusions in
   `VectorCronjobAllRunsFailing`, or its idle state pages.
2. Add a row to the table at the top of this runbook.
