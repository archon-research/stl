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
| `reference-capital-indexer` | `reference-capital-indexer` | 15m | Sky Star-monitor reference risk capital; the only writer of forward reference history |
| `reference-capital-backfill` | `reference-capital-backfill` | **on demand** | Seeds the reference balance-sheet history predating the syncer's first run |
| `morpho-vault-backfill` | `morpho-vault-backfill` | **on demand** | Discovers Morpho vaults from the archived S3 receipts and replays their VaultV2 structured events, for a block range supplied at trigger time (VEC-218) |
| `morpho-v2-bootstrap` | `morpho-v2-bootstrap` | **manual only** | One-shot repair of Morpho VaultV2 vaults discovered before atomic discovery (VEC-218) |

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

> `morpho-v2-bootstrap` is **manual only**: its Temporal schedule is created paused
> and with no interval, so it produces nothing until an operator triggers it. Its
> worker idles ~100% of the time and no data goes stale while it is down, so it is
> classed with the on-demand workers: `VectorOnDemandWorkerDown` (warning) covers
> its availability, and it is excluded from `VectorCronjobAllRunsFailing` — a job
> that runs once on demand produces only errors and no success from a single failed
> run (up to one error per attempt, and it is allowed three), which would page
> critical for a run an operator is already watching. That failure still fires
> `VectorCronjobRunFailing` (warning), the right severity for it. If a trigger does
> not start, check the pod first
> (`kubectl -n vector get pods -l app=morpho-v2-bootstrap`).

> **A killed morpho-v2-bootstrap run resumes; a re-triggered one starts over.** The
> sweep records its position in the activity's Temporal heartbeat details after
> every completed block chunk, and the activity is allowed 3 attempts. A worker
> killed mid-run — any deploy rolls this Deployment — is retried by Temporal and
> the retry picks up at the next chunk, so an interrupted run costs minutes, not
> the hours of `eth_getLogs` it had already done. Heartbeat details belong to one
> workflow execution, so a run that goes red and is **re-triggered by hand starts
> from the factory deploy block again**: that is a fresh execution with no
> heartbeat history. It is safe (every write is idempotent), just slow. A run that
> is red after its attempts is the operator signal — the cause is deterministic and
> no further retry will clear it.

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

A trigger-only Deployment — a `temporal.RunWorker` job, or a `ManualOnly` cronjob
whose schedule is paused with no interval — has had <1 available replica for
>30m. These workers carry **no schedule**, so unlike `VectorCronjobWorkerDown`
nothing is ticking into the void and no data is going stale. The only impact is
that a new run cannot be started until the pod is back. Warning severity for that
reason.

Currently matches: `offchain-price-backfill`, `reference-capital-backfill`,
`morpho-vault-backfill`, `morpho-v2-bootstrap`.

### First checks

1. `kubectl -n vector get pods -l app=$DEPLOY` — look for
   `CrashLoopBackOff`, `ImagePullBackOff` or `OOMKilled`. `$DEPLOY` is the
   `deployment` label on the alert; more than one worker matches this rule.
2. `kubectl -n vector logs deploy/$DEPLOY --tail=100`.
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

### Special case: `reference-capital-backfill` (on-demand, no schedule)

Seeds the reference balance-sheet history that predates STL's own observation of
Sky's Star monitor. `reference-capital-indexer` can only accumulate **forward**
from its first run — the monitor publishes no history — so this is the only
source of anything earlier, and it reads Sky's balance-sheet feed instead.

Trigger it from the Temporal UI (Workflow Type `ReferenceCapitalBackfill`, Input
`{"daysAgo": 365}`) or:

```
temporal workflow start --namespace vector \
  --task-queue reference-capital-backfill --type ReferenceCapitalBackfill \
  --workflow-id reference-capital-backfill-$(date +%s) \
  --input '{"daysAgo": 365}'
```

- **One activity, all or nothing.** Unlike `offchain-price-backfill` it is not
  chunked. The service fetches every tracked prime in one request and refuses to
  write unless all of them came back, because the write is `ON CONFLICT DO
  NOTHING`: a prime missing from a one-shot seed leaves a permanent hole a re-run
  cannot repair. A failure therefore writes nothing — retry it rather than
  reaching for a partial repair.
- **Re-running is safe.** Rows are insert-only and conflict away within a build.
  A run under a new `build_id` appends a fresh `processing_version` generation
  rather than overwriting; read paths take the newest.
- **What it fills, and what it cannot.** It populates `assets_usd` (the figure
  Sky's dashboard labels PRIME COLLATERAL, and the only source of it) and
  `treasury_balance_usd`, which backs the total-capital series. It does **not**
  fill reference `exposure`: the feed's `allocated_assets` is a different
  measurement from the monitor's `total_exposure` (+32% for spark at the same
  instant), so the read path splices in `NULL` rather than stepping the series.
  Reference exposure has no history by design and accumulates forward only.
- **Verify a run landed** by row count rather than a green dashboard, since
  per-activity metrics cannot see a workflow-level failure:

  ```sql
  SELECT count(*), min(observed_at)::date, max(observed_at)::date
  FROM prime_reference_balance_sheet;
  ```

---

## VectorReferenceCapitalIndexerWritesZero

**What it means.** Cycles are succeeding but `prime_capital_stack` received no
rows for an hour. `reference-capital-indexer` is the only writer of forward reference
capital, and Sky's Star monitor publishes no history, so every cycle that
records nothing is a permanent hole — it cannot be backfilled afterwards.

**Why it is not caught by the generic rules.** The run returns no error, so
`VectorCronjobRunFailing` stays quiet. And because the read path gap-fills with
`locf`, `/v1/primes/{id}/total-capital?reference=true` keeps serving the last
observed value as if it were current, rather than going null. The stall is
invisible from both the error path and the API.

**Triage.**

1. Confirm the worker is cycling rather than wedged:
   `kubectl -n vector logs deploy/reference-capital-indexer --tail=100`. A healthy
   cycle logs `capital stack sync complete` with a non-zero `snapshots` count.
2. Check whether the monitor is answering at all:
   `curl -s "$SKY_RISK_CAPITAL_URL/primes/" | jq '.data.results | length'`.
   Zero or a missing `results` array is an upstream fault; the client rejects
   both, so this should have surfaced as an error — if it did not, the payload
   changed shape.
3. If the monitor is healthy and the worker is cycling, the primes it covers no
   longer match the ones STL tracks. `VectorReferenceCapitalIndexerPrimeUncovered`
   should also be firing; treat that as the primary signal.

**Resolution.** This is upstream coverage, not something to fix in the service.
Confirm which primes the monitor now reports and reconcile against the
axis-synome contract. The gap in the series stays — say so rather than
backfilling it from a different feed, which would splice a different
measurement.

---

## VectorReferenceCapitalIndexerPrimeUncovered

**What it means.** A prime STL tracks was absent from every upstream response
for an hour. Its reference series is frozen while the other primes keep
advancing.

**Why it is not caught by the generic rules.** The cycle succeeds and still
writes every covered prime, so neither the error rules nor
`VectorReferenceCapitalIndexerWritesZero` fire. `locf` then carries that prime's last
value forward indefinitely, so its chart looks current and flat rather than
absent.

**Triage.**

1. `{{ $labels.star }}` names the prime. Ask the monitor directly:
   `curl -s "$SKY_RISK_CAPITAL_URL/primes/" | jq -r '.data.results[].star'`.
2. If the prime is absent from that list, the monitor dropped it. If a
   similar-but-different name is present, the vocabulary drifted.
3. Compare against what STL tracks — the star keys of the axis-synome contract's
   ALM proxies, which is what `trackedStarsFromContract` reads. Note the `prime`
   table is **not** the tracked set: it still carries rows for primes STL has
   stopped tracking.

**Resolution.**

- *Monitor dropped the prime.* Nothing to fix in the service; the series is
  correctly frozen. Decide with the team whether the prime should still be
  tracked, and silence the alert while that is open.
- *Name drifted.* The contract and the monitor disagree on spelling. Fix it in
  the axis-synome contract, not by mapping the name in the syncer — the contract
  is the tracked set, and a local alias would hide the next drift.

Do not "fix" this by relaxing the syncer to accept partial coverage silently.
The alert exists precisely because a partially-covered cycle looks healthy.
## morpho-v2-bootstrap fails repeatedly on the same adapter
### Special case: `morpho-vault-backfill` (on-demand, no schedule)

Another **on-demand** Temporal worker (`temporal.RunWorker`). Everything said
about `offchain-price-backfill` above applies — nothing is missed while it is
down, it emits one `cronjob_runs_total` record per *activity*, and it is excluded
from `VectorCronjobAllRunsFailing` for the same reason.

**How to start a run.** Temporal UI (namespace **`vector`**) →
**Start Workflow**:

| Field | Value |
|---|---|
| Task Queue | `morpho-vault-backfill` |
| Workflow Type | `MorphoVaultBackfill` |
| Workflow ID | descriptive and unique, e.g. `morpho-vault-backfill-24765588-24786366` |
| Input | `{"from":24765588,"to":24786366}` |

`from`/`to` are inclusive block numbers. To cover the whole VaultV2 era instead,
supply `{"to":24786366,"fromV2Deploy":true}` — `from` then defaults to the
chain's VaultV2 factory deploy block. An explicit `from` always wins. The
equivalent CLI call:

```bash
temporal workflow start --namespace vector \
  --task-queue morpho-vault-backfill --type MorphoVaultBackfill \
  --workflow-id morpho-vault-backfill-24765588-24786366 \
  --input '{"from":24765588,"to":24786366}'
```

**What a run does, and how long it takes.** One discovery activity over the whole
range (scan S3 receipts → probe candidates on-chain → persist vaults), then one
activity per 1000-block S3 partition replaying that partition's VaultV2
structured events, sequentially and in ascending block order. Measured on
mainnet: ~11.5 s per partition for discovery and ~20 s per partition for replay,
so a whole-V2-era run is measured in hours. A range wider than 8000 partitions is
rejected up front — that catches a mistyped `from` or a millisecond timestamp
pasted into `to`, and keeps every accepted run inside Temporal's 51,200-event
history limit (~6 events per activity), so none is terminated mid-flight.

**Reading progress and re-running.** The `progress` query (UI → Query tab) shows
`partitionsDone` / `partitionsTotal` mid-run and survives a failed run, whose
Result panel Temporal discards. A failed partition stops the run there, so the
completed prefix is what the query reports. Re-running the same range is always
safe: every write is an idempotent append, so a repeat costs wall clock, not
correctness.

**Failure modes specific to this worker.**

- `AccessDenied` listing or reading the raw bucket → the EKS Pod Identity
  association granting this ServiceAccount S3 read on the per-chain raw bucket is
  missing. It lives in the infra repo, not here.
- `partition ... missing receipt block(s) ... (S3 gap)` → the archive is
  genuinely incomplete for that partition. Replay hard-stops rather than replay
  a thinned partition; repair S3 and re-run the same range.
- An adapter-classification failure — same cause and same recovery as the
  bootstrap's, below; the two share the VaultV2 replay path.

---

## morpho-v2-bootstrap fails on an adapter

**Nothing here needs rows reconciling by hand.** Adapter membership is an
append-only observation log, so a failed pass writes no lifecycle a later run has
to walk back, and re-running is always safe. Two things can stop a run:

**1. A chain or DB error.** `eth_getLogs` 401/429/5xx, an RPC timeout, a DB
outage. Temporal retries the activity (3 attempts) and each retry resumes from the
last fully replayed chunk. A wrong or expired RPC credential is the common
non-clearing case — it retries identically until the secret is fixed.

**2. `no adapter classification supplied to record an observation of membership`**
(`ErrAdapterUnclassified`), wrapped as `adapter <addr> was a member before the
transaction but is not at block <N> inside it, so no type was probed`. A replayed
`Allocate` skips the on-chain type probe when committed state already places its
adapter in the vault's set at that log's position; if the log stops answering that
way inside the transaction — a concurrent live-indexer write landing between the
two reads — the registry refuses to record membership with no classification
rather than defaulting a type. It clears on retry: the retry re-reads and probes.

**Not a failure:** a `RemoveAdapter` for an adapter the registry has never seen.
It records one untyped `is_member = false` observation, which is the truthful
record of learning about an adapter from its own de-registration.

**Recovery.** Let Temporal's retries run; if the run is still red, re-trigger from
the Temporal UI (a hand re-trigger starts from the factory deploy block — safe,
just slow). Escalate to the Vector team if the same non-transient error repeats
across triggers: that is a code defect, not an operational state.

This is a property of the shared VaultV2 replay path, not of the bootstrap alone
— `morpho-vault-backfill` replays through the same handlers and has the same
exposure.

---

## Adding a new cronjob

Failure + all-failing alerts are automatic (they group by `service_name`).
`VectorCronjobAllRunsFailing` excludes `maple-graphql-indexer`,
`offchain-price-backfill`, `morpho-vault-backfill` and `morpho-v2-bootstrap`;
`VectorCronjobRunFailing` excludes only maple. Two manual steps:

1. Add the new **Deployment name** to the `deployment=~"..."` regex in the
   availability rule that matches its lifecycle — `VectorCronjobWorkerDown` for
   a scheduled cronjob, `VectorOnDemandWorkerDown` for anything that only runs
   when a human triggers it (a `temporal.RunWorker` job, or a `ManualOnly`
   cronjob with a paused interval-less schedule like `morpho-v2-bootstrap`).
   (The kube-state-metrics label is the Deployment name, which may differ from
   `service_name` — e.g. `spark-anchorage-indexer`.) A trigger-only worker must
   ALSO be added to the `service_name!=` exclusions in
   `VectorCronjobAllRunsFailing`, or its idle state pages.
2. Add a row to the table at the top of this runbook.
