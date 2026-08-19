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

## VectorAnchorageNoSnapshotsStored

**Severity:** critical (pages) · **For:** 30m

### What it means

`anchorage-indexer` ran successfully over the last 2h (8 ticks of its 15m
schedule) but wrote **0 rows** to `anchorage_package_snapshot`. The table is the
only record of Spark's custodied collateral at Anchorage, so it is now frozen at
whatever it last saw while the real position keeps moving — the risk models keep
reading the stale value with no indication it is stale.

This is the gap the run-outcome rules cannot see. Fetching zero packages is not
an error: `FetchPackages` gets a 200, `Run` returns nil, the Temporal run is
`status="success"`, and `VectorCronjobRunFailing` / `VectorCronjobAllRunsFailing`
/ `VectorCronjobWorkerDown` all stay green. That is exactly what happened between
2026-06-16 and 2026-08-19, undetected.

### First checks (≤5 min)

1. **Is the API returning packages at all?**

   ```bash
   kubectl -n vector logs deploy/spark-anchorage-indexer --tail=100 | grep "fetched packages"
   ```

   - `count=0` → upstream. The API is answering 200 with an empty list.
   - `count>0` → the packages are being fetched but dropped by
     `filterActivePackages`; look for `skipping inactive anchorage package`
     warnings in the same logs. Every package went `active=false`.

2. **Credential.** An Anchorage key that has been rotated, revoked, or rescoped
   to a different prime returns an empty collection rather than a 401, so it
   fails exactly like this. Compare against
   `stl-<env>-anchorage-api-key` in Secrets Manager and confirm
   `ANCHORAGE_PRIME` still matches the key's prime — a mismatched pair is
   silently accepted (see the note in `cmd/cronjobs/anchorage-indexer/main.go`).

3. **Last good data.**

   ```sql
   SELECT max(snapshot_time), count(*) FROM anchorage_package_snapshot;
   ```

   Gives the date the feed went quiet, which dates the credential/contract change.

4. **Cross-check the live position** against Block Analitica's Spark dashboard.
   If the venue still shows exposure, the data is missing, not gone.

### Common causes

- API key rotated or rescoped upstream → empty list, no 401.
- Anchorage moved the packages to a different prime or endpoint.
- Every package legitimately closed (`active=false`) → the alert is correct and
  the venue is wound down.

### Verify recovery

`increase(anchorage_snapshots_stored_total[2h]) > 0`, and a fresh
`stored snapshots count=` log line. Confirm in the DB that `max(snapshot_time)`
is now current.

### If the venue is genuinely wound down

The alert will hold indefinitely — that is intended, not a tuning problem. Zero
collateral at Anchorage means the indexer has no job left, so the fix is to
retire it (drop the Deployment and this rule), not to widen the window.

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
