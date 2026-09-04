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
| `morpho-vault-backfill` | `morpho-vault-backfill` | **on demand** | Discovers Morpho vaults from the archived S3 receipts and replays their VaultV2 structured events, for a block range supplied at start time (VEC-218) |
| `morpho-v2-bootstrap` | `morpho-v2-bootstrap` | **on demand** | One-shot repair of Morpho VaultV2 vaults discovered before atomic discovery (VEC-218) |
| `block-republisher` | `block-republisher` | **on demand** | Re-publishes named block heights under the next `block_version` their raw archive leaves free, so every indexer appends the canonical block for a height whose only published version is a losing fork (ARCT-383) |
| `core-model-runner` | `core-model-runner` | 24h | CORE model CRR per market → `core_model_results` (Python harness; staging + prod; N_MC capped at 100 until the sizing in #804 settles) |

> `maple-graphql-indexer` is also a cronjob but has its own richer rules — see
> [vector-indexers.md](vector-indexers.md), not this runbook.

The shared activity records `cronjob_runs_total{status="success"|"error"|"canceled"}`
and `cronjob_run_duration_seconds` per run, labelled `service_name=<cronjob>`.
New cronjobs are covered automatically; only the availability rule needs the
new Deployment name added to its regex — `VectorCronjobWorkerDown` for a
scheduled cronjob, `VectorOnDemandWorkerDown` for an on-demand worker.

> `core-model-runner` is the first **Python** Temporal cronjob. Its harness
> (`stl-verify/python/app/adapters/temporal/`) now emits the same
> `cronjob_runs_total{status}` / `cronjob_run_duration_seconds` series from a
> single site every Python cronjob shares — a `RunMetricsInterceptor` wrapping
> every activity execution, mirroring the Go shared activity's `RecordRun`,
> including the `status="canceled"` split for a run interrupted by activity
> cancellation (worker shutdown during a deploy, or a schedule cancel) — so the
> metric-based rules above cover it like any other cronjob, with no per-job
> wiring ([VEC-638](https://linear.app/archontech/issue/VEC-638), closing the
> prod-blocker gap raised in the review of #705).
>
> `core-model-runner` is still excluded from `VectorCronjobAllRunsFailing` (see
> that rule and "Adding a new cronjob" below): it ticks every 24h with no retry,
> so a single failed tick would otherwise page critical and stay paged for up
> to 24h — the same shape `VectorCronjobAllRunsFailing` already avoids for the
> on-demand jobs, just reached by a long interval instead of no schedule.
> `VectorCronjobRunFailing` (warning) still covers a failed tick, and
> [`VectorCoreModelRunnerStale`](#vectorcoremodelrunnerstale) (critical) pages
> when no tick has *completed* (success or error) in 30h — the stall coverage
> the exclusion above would otherwise remove, and the only rule that catches a
> tick lost to a deploy-time cancel or a hang (neither records an error).
> The runner runs in staging and prod (#800), both at N_MC=100 until the
> sizing run in #804 settles — unrelated to this gap.

> `transform-worker` ships at `replicas: 0` and is enabled (scaled to 1) only after
> the one-off bootstrap has run. `VectorCronjobWorkerDown` is guarded on
> `kube_deployment_spec_replicas > 0`, so a deliberately scaled-to-zero deployment
> does not page (see that section).

> `morpho-v2-bootstrap` carries **no schedule**: it produces nothing until an
> operator starts a run on its task queue. Its worker idles ~100% of the time and
> no data goes stale while it is down, so it is classed with the other on-demand
> workers: `VectorOnDemandWorkerDown` (warning) covers its availability, and it is
> excluded from `VectorCronjobAllRunsFailing` — a job that runs once on demand
> produces only errors and no success from a single failed run (up to one error per
> attempt, and it is allowed three), which would page critical for a run an
> operator is already watching. That failure still fires `VectorCronjobRunFailing`
> (warning), the right severity for it. If a run does not start, check the pod
> first (`kubectl -n vector get pods -l app=morpho-v2-bootstrap`).

> **A killed morpho-v2-bootstrap run resumes; a newly started one starts over.** The
> sweep records its position in the activity's Temporal heartbeat details after
> every completed block chunk, and the activity is allowed 3 attempts. A worker
> killed mid-run — any deploy rolls this Deployment — is retried by Temporal and
> the retry picks up at the next chunk, so an interrupted run costs minutes, not
> the hours of `eth_getLogs` it had already done. Heartbeat details belong to one
> workflow execution, so a run that goes red and is **started again by hand starts
> from the factory deploy block**: that is a fresh execution with no heartbeat
> history. It is safe (every write is idempotent), just slow. A run that is red
> after its attempts is the operator signal — the cause is deterministic and no
> further retry will clear it.

General triage:

```bash
kubectl -n vector get pods -l app=<deployment>      # pod health
kubectl -n vector logs deploy/<deployment> --tail=200
```

Temporal UI (vector namespace) → Schedules → `<cronjob>` shows recent runs and
the failure stack for each. An on-demand job has no schedule; look under
Workflows instead, filtered by its Workflow Type.

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

### Special case: `watcher-data-validator` "Orphan-only heights"

The validator fails the run — and so this alert — when its **Orphan-only
heights** check reports `N height(s) have only an orphaned block: <numbers>`.
The count is exact; the message names the first 100 heights and appends
`(+K more)`, with the full list in the check's `orphan_only_heights` detail.

It means block N is stored only on a losing fork: the watcher saved fork A, the
canonical broadcast for N was dropped as `stale_fork` (a load-balanced RPC node
had not converged when the watcher verified it), and the reorg that block N+1
committed orphaned fork A without ever fetching the winner. Nothing at that
height is canonical — S3 holds only the orphaned fork and every indexer has its
events at `block_version` 0. `FindGaps` scans only above the backfill
watermark, so it re-fetches the height only while the watermark still sits
below it. This check scans the full block range, where **Chain Integrity**
only looks for missing heights up to the watermark — and a fresh occurrence
sits *above* the watermark, because the reorg that caused it rewound the
watermark to the common ancestor below, so this check is the only one that
names it. A hole older than that rewind sits below the watermark and fails
both.

`block_states` carries a 30-day retention policy
(`add_retention_policy('block_states', INTERVAL '30 days')`), so the check only
ever sees the retained window. **This alert clearing without a repair means the
hole aged out of the check, not that it healed** — the downstream
`block_version` 0 rows and the S3 objects are untouched by retention. The 14
staging mainnet heights named in ARCT-379 (25087888 … 25589752) are already
outside it.

Confirm:

1. `cast block <N> --field hash --rpc-url <chain rpc>` against the stored row —
   `SELECT hash, version, is_orphaned FROM block_states WHERE chain_id = <id>
   AND number = <N>;`. One row, orphaned, hash unknown to the chain, is the
   signature.
2. The watcher's Loki line `dropping stale-fork reorg broadcast … block=<N>`
   around the block's timestamp is the cause. Use it, not
   `chain_reorgs_dropped_total{reorg_dropped_reason="stale_fork"}`: that counter
   carries no block label by design (block numbers are unbounded cardinality),
   so it can only tell you drops happened, never at which height.

Repair:

- **New occurrences heal themselves.** A reorg commit rewinds
  `backfill_watermark` to the common ancestor, so the next gap pass re-fetches
  N and saves it as version 1 (ARCT-379). If the same height is still named two
  poll intervals later, the RPC is still serving the losing fork — re-check
  `cast block <N>`.
- **A hole no rewind covered — one predating ARCT-379, or one below every
  reorg since — is repaired by rewinding the watermark by hand,** as long as it
  is still inside the retained window: `UPDATE backfill_watermark SET watermark
  = <N-1>, rewind_count = rewind_count + 1 WHERE chain_id = <id>;`. Bump the
  rewind count too: a pass already scanning compares against the pair, and
  would otherwise advance straight back over the height you just re-opened. The
  next gap pass saves N as version 1 and publishes it, so the indexers append
  the correction. The orphaned version-0 rows stay put as history.
- **A hole outside the retained window has no safe executable repair yet.**
  `raw-block-bulk-downloader --bucket <raw bucket> --rpc-url <chain rpc>
  --start-block <N> --end-block <N>` looks like the repair and is not one.
  Every key it builds and every existence check it makes is pinned to a literal
  version `1`, and it never reads what the archive already holds. The losing
  fork sits at version **0**, not 1 — `assign_block_version()` is
  `COALESCE(MAX(version), -1) + 1`, so the first row at a height is version 0,
  and the raw-data-backup-worker builds the key from `event.Version`. So at an
  orphan-only height the tool finds no `<N>_1_*`, refetches the height **by
  number**, and writes the canonical block, receipts and traces at `<N>_1_*`.
  That is the right answer for the single-reorg shape by coincidence, not by
  design: at a height that already holds a `_1_` it skips (uploads go through
  `WriteFileIfNotExists`, so it never overwrites), and where the canonical
  block belongs at version 2 it cannot express that. Pointed at anything but a
  hole it is actively harmful — every healthy height is version 0, so it
  refetches and writes a duplicate `_1_` copy of block, receipts and traces for
  every block in the range, which is how deep history came to be `_1_`-only
  (`cmd/backfillers/morpho-vault-backfill/discovery.go`). There is no
  `--dry-run` flag either; the tool exits 2 on one. Doing this deliberately —
  next free version above the losing fork, dry run first — needs **#849**,
  which adds `--dry-run`, `--allow-unfinalized` and next-free-version writing.
  Even then it fixes S3 only: the indexers' rows stay at `block_version` 0
  until the republisher (**ARCT-383**, #850) re-emits the block event. The
  republisher derives that version the same way, per height, from the objects in
  the raw bucket, so the order is a rule, not a preference: **republisher first,
  bulk downloader after.** The other way round, the `_1_` objects the repair
  wrote push the event to version 2 and the archive ends up holding the same
  canonical block twice — and the republisher refuses the height outright,
  because the archive's top version is by then already canonical. The recovery
  for a height #849 has already repaired is `"archiveRepaired": true` (below),
  which republishes at the version those objects occupy instead of past them.

Before the prod deploy, run the pre-flight queries below against prod's
retained window — a hole found there is repairable while it is still inside
retention.

### Special case: `watcher-data-validator` "Chain Integrity" missing blocks

`chain integrity violation: canonical block(s) A to B missing between blocks X
and Y` (or `… missing after block X`, for a hole at the top of the checked
range) means the canonical chain has a hole at A..B that the backfill watermark
has already passed — so the gap filler is not working on it and
`VectorWatcherBackfillWatermarkLagHigh` will not fire for it. Only the lowest
hole is named, and that is the only one you need: one rewind to A re-opens
every hole above it too (`FindGaps` returns them all, the pass fills each, and
the advance parks below the first one it could not fill). Re-running the check
before the filler has caught up tells you nothing — it can never name a hole
above the watermark.

Below the watermark the check is strict: every height present and linked. Above
it, only the links are checked (`Parent-hash chain valid through backfill
watermark N; parent links valid through block M`), because a missing height up
there is the filler's live work — covered by `backfill_watermark_lag` and
`VectorWatcherBackfillWatermarkLagHigh` in
[vector-watcher.md](vector-watcher.md) — while a broken link or a duplicated
height never repairs itself. A named *missing* range is therefore always
**below** the watermark; a named link break can be either side of it.

Two causes, told apart by `SELECT number, hash, is_orphaned FROM block_states
WHERE chain_id = <id> AND number BETWEEN A AND B;`:

- **Rows exist but are all orphaned** → the orphan-only case above; repair it
  that way.
- **No rows at all** → the watermark passed a height that was never filled.
  Three ways in: a pre-#844 unconditional watermark write that overwrote a
  reorg's rewind; a watermark written by hand past an unfilled height; and a
  stale-chain recovery whose canonical refetch failed, which before #844
  orphaned the row without rewinding. `FindGaps` never scans below the
  watermark, so rewind it — `UPDATE backfill_watermark SET watermark = A - 1,
  rewind_count = rewind_count + 1 WHERE chain_id = <id>;`, the rewind count
  bump for the same reason as above. Then trigger a validator run by hand
  (Temporal UI → Schedules → `<SERVICE_NAME>` → Trigger, or `temporal schedule
  trigger --schedule-id <SERVICE_NAME>`) once the watermark has passed B; the
  repair is done when `backfill_watermark_lag` has drained past B, not when the
  check passes — right after the rewind it passes whether or not A..B was
  refetched, because the strict half stops at the watermark you just lowered.

### Pre-flight before deploying the ARCT-379 validator checks

Both special cases above rest on checks #844 adds or widens —
`FindOrphanOnlyHeights`, and the missing-height, duplicate-canonical and
broken-parent-link violations `VerifyChainIntegrity` now reports. They judge
state that is already in the database, so a hole that has been sitting there
since long before the rollout fails the very first hourly run:
`VectorCronjobRunFailing` immediately, `VectorCronjobAllRunsFailing`
(critical, pages) about 1h15m later. The repair is the one above — one
`UPDATE backfill_watermark SET watermark = <below the lowest hole>,
rewind_count = rewind_count + 1 WHERE chain_id = <id>;` per chain, which
re-opens every hole above it in the same rewind — but it is a repair you want
made before the pager goes off, not after.

`watcher-data-validator` runs on **three** chains in both staging and prod —
chain ids 1, 130 and 42161. All six per-chain deployments are listed in
`k8s/overlays/{staging,prod}/kustomization.yaml`, but `optimism-`, `base-` and
`avalanche-watcher-data-validator` are `replicas: 0` in
`k8s/base/<chain>-watcher-data-validator/deployment.yaml` with no overlay
patch, because the Etherscan cross-check is not covered for those chains yet —
so run the three queries once per **enabled** chain. All three are read-only,
and `block_states`' 30-day retention bounds them: only the retained window is
checkable, and only a hole inside it is repairable. Expected outcome on every
chain: **zero rows from queries 1 and 2, and `tail = want` from query 3 — or
repair first.**

**1. Orphan-only heights.** This is the query `FindOrphanOnlyHeights`
(`internal/adapters/outbound/postgres/blockstate_repository.go`) runs, minus
its range bound. Sub-second; served by `idx_block_states_orphaned`.

```sql
SELECT DISTINCT o.number
FROM block_states o
WHERE o.chain_id = <id> AND o.is_orphaned
  AND NOT EXISTS (SELECT 1 FROM block_states c
                  WHERE c.chain_id = o.chain_id AND c.number = o.number AND NOT c.is_orphaned)
ORDER BY o.number;
```

**2. Chain integrity** — missing heights below the watermark, plus duplicate
canonical rows and broken parent links over the **whole** retained window. The
validator checks two bands: `VerifyChainIntegrity` runs the strict form up to
`min(max canonical, watermark)`, and `VerifyParentLinks` runs the
links-and-duplicates form from there to the top canonical row, so a pre-flight
bounded at the watermark misses every violation above it. One pass does both —
only the missing-height arm carries the watermark bound. Measured on staging at
14 s on the largest chain (42161, ~10.8 M heights).

```sql
WITH ordered_blocks AS (
  SELECT number, hash, parent_hash,
         LAG(hash)   OVER (ORDER BY number, version DESC) AS prev_hash,
         LAG(number) OVER (ORDER BY number, version DESC) AS prev_number
  FROM block_states
  WHERE chain_id = <id> AND NOT is_orphaned)
SELECT prev_number, number,
       CASE WHEN prev_number = number     THEN 'duplicate canonical rows'
            WHEN prev_number < number - 1 THEN 'missing ' || (prev_number + 1) || '..' || (number - 1)
            ELSE 'broken parent link' END AS violation
FROM ordered_blocks
WHERE prev_number IS NOT NULL
  AND (prev_number = number
       OR (prev_number < number - 1 AND number <= <watermark>)
       OR (prev_number = number - 1 AND parent_hash != prev_hash))
ORDER BY number LIMIT 1;
```

**3. The tail.** The pair scan has no successor to flag a hole at the *top* of
the strict band against, so check that separately — but check the same thing
the validator checks. The validator fails when there is no canonical row at the
watermark height (`verifyRangeReachesEnd`), and it fails when the watermark
sits above the last canonical row altogether — the state a hand-written
watermark or a pre-#844 overwritten rewind leaves. Both collapse into one
comparison:

```sql
SELECT w.watermark,
       (SELECT max(number) FROM block_states
         WHERE chain_id = <id> AND NOT is_orphaned AND number <= w.watermark) AS tail
FROM backfill_watermark w WHERE w.chain_id = <id>;
```

`tail` must equal `w.watermark`. A lower `tail` is either a watermark parked on
an unfilled height or a watermark past the head, and the validator fails the
run on both. A chain with no
`backfill_watermark` row, or `watermark = 0`, is vacuous here — the validator
verifies the whole range and the tail check cannot fail.

Staging, as of 2026-09-01, on the three enabled chains: zero orphan-only
heights, zero integrity violations over the whole retained window, and
`tail = want` on each — nothing to repair there. Run the same three against
prod before the prod deploy.

If the pre-flight does turn something up, or you would rather not race the
first hourly run, propose in the PR — and get a human to approve it, per
[alerts/AGENTS.md](../../alerts/AGENTS.md) — that `VectorCronjobRunFailing` and
`VectorCronjobAllRunsFailing` be silenced for the deploy window on
`service_name=~"(unichain-|arbitrum-)?watcher-data-validator"`. `service_name`
is the pod's own `app` label, injected as `SERVICE_NAME` through the downward
API, so a matcher on the literal `watcher-data-validator` silences mainnet
only. Keep the rewind tracked, and silence the alert while that is open.

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
   persistent, so look for a hard, non-transient cause. For
   `watcher-data-validator` that includes an orphan-only height the gap filler
   cannot heal: it fails every run, so it escalates here about 1h15m after the
   first failure — see
   [Special case: `watcher-data-validator` "Orphan-only heights"](#special-case-watcher-data-validator-orphan-only-heights).
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

A start-on-demand Deployment — a `temporal.RunWorker` job — has had <1 available
replica for >30m. These workers carry **no schedule**, so unlike
`VectorCronjobWorkerDown` nothing is ticking into the void and no data is going
stale. The only impact is that a new run cannot be started until the pod is back.
Warning severity for that reason.

Currently matches: `offchain-price-backfill`, `reference-capital-backfill`,
`morpho-vault-backfill`, `morpho-v2-bootstrap`, `block-republisher`.

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
  **and** its `cronjob offchain-price-backfill ...` line in `k8s/image-roster.txt`
  (which drives promotion and the overlay `images:` entry, ORB-362). Missing
  either leaves the pod without a pullable image.
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
`docker-release-all` and its `cronjob` line in `k8s/image-roster.txt` (promotion
and the overlay `images:` entry are generated from it, ORB-362).

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

### Special case: `morpho-vault-backfill` (on-demand, no schedule)

Another **on-demand** Temporal worker (`temporal.RunWorker`). Everything said
about `offchain-price-backfill` above applies — nothing is missed while it is
down, it emits one `cronjob_runs_total` record per *activity*, and it is excluded
from `VectorCronjobAllRunsFailing` for the same reason.

**Reaching Temporal before you run any of the commands below.** The server is
in-cluster in every environment, in the `temporal` k8s namespace, at
`temporal-server.temporal:7233` in Temporal namespace `vector` — that is what
every worker is handed (`TEMPORAL_HOST_PORT` / `TEMPORAL_NAMESPACE` in
`k8s/overlays/{staging,prod}/configmaps.yaml`). In EKS the server itself is owned
by the infrastructure repo, not this one (`k8s/dev-infra/temporal.yaml` is local
dev only and says so). The workers dial it with no TLS and no API key
(`stl-verify/internal/adapters/outbound/temporal/temporal.go`), so access is a
matter of reaching the cluster, not of holding a Temporal credential.

The `temporal` CLI defaults to `127.0.0.1:7233`, so a bare invocation fails with
connection refused. Port-forward the service and point the CLI at it:

```bash
kubectl --context <staging-or-prod-context> -n temporal port-forward svc/temporal-server 7233:7233
```

Every snippet below then works as written. The `temporal` snippets in this
runbook's other sections omit `--address`; `export TEMPORAL_ADDRESS=127.0.0.1:7233`
once in the same shell and they work too. Locally, `make temporal-cli` does the
equivalent by exec-ing into the kind cluster's own server pod; it refuses any
non-kind context on purpose, so it cannot be used for staging or prod.

**Without CLI access**, use the Temporal UI (namespace `vector`) — starting a run,
listing executions, terminating one and reading history are all buttons there.
This repo does not carry the staging/prod UI URL; it is exposed by the
infrastructure repo, so get it from there or from the team rather than guessing at
a hostname.

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
temporal workflow start --address 127.0.0.1:7233 --namespace vector \
  --task-queue morpho-vault-backfill --type MorphoVaultBackfill \
  --workflow-id morpho-vault-backfill-24765588-24786366 \
  --input '{"from":24765588,"to":24786366}'
```

**What a run does, and how long it takes.** One discovery activity per sub-range
of a few dozen partitions (scan S3 receipts → probe candidates on-chain → persist
vaults), then one activity per 1000-block S3 partition replaying that partition's
VaultV2 structured events, both sequentially and in ascending block order. Every
completed activity is banked in the event history, so a rollout mid-run retries
the sub-range or partition it was interrupted in and leaves the completed ones
alone — the sub-range is sized to finish inside the gap between two deploys, which
is what lets a long run survive them. Measured on
mainnet: ~11.5 s per partition for discovery and ~20 s per partition for replay,
so a whole-V2-era run is measured in hours. A range wider than 8000 partitions is
rejected up front — that catches a mistyped `from` or a millisecond timestamp
pasted into `to`, and keeps every accepted run inside Temporal's 51,200-event
history limit (~6 events per activity), so none is terminated mid-flight.
Note on dead-worker detection: the replay activity heartbeats on a 60 s ticker,
but a typical partition finishes in 10-20 s, so in practice the heartbeat only
fires on pathologically slow partitions — for ordinary ones the 30-minute
StartToClose timeout is the real detector after a mid-partition rollout.

**Deploying this worker while a run is in flight.** A routine deploy is fine: the
run is replayed against the new code, reaches the same command sequence, and
carries on from where it was. What an in-flight run cannot survive is an image
that CHANGES that sequence — a different phase split (the sub-range width is one),
a changed activity input shape, or activities reordered. Ship one of those and you
must terminate every running `MorphoVaultBackfill` execution as part of the roll,
then start the ranges again afterwards; restarting costs only wall clock, since
every write is an idempotent append. A changed input shape is the one to be
strictest about: an activity SCHEDULED under the old image and still in flight
carries an old-shaped payload, which the new worker decodes with `encoding/json`
— unknown fields ignored, missing ones zero-filled — so it can scan an empty
range, probe at block 0, succeed, and be banked as a completed sub-range. That is
a hole in the data reported as success, which nothing downstream detects.

Why replay cannot absorb it: Temporal walks history and the scheduled commands in
lockstep, comparing each activity's id and the last dot-separated segment of its
type (`lastPartOfName`), positionally — the input is not among the fields
compared, so a re-planned run is not corrected by it. Only a side running out
short-circuits the walk (a missing or extra command); an inserted or reordered
activity fails inside that id/type comparison (`go.temporal.io/sdk v1.45.0`,
`internal/internal_task_handlers.go:1650-1662`). The failure is not a failed run:
the workflow TASK fails with `NON_DETERMINISTIC_ERROR` and retries forever while
the execution sits in Running, because the default `WorkflowPanicPolicy` is
`BlockWorkflow` (`internal/worker.go:448-454`) and this worker never overrides it,
and no run timeout is set. Look for
`temporal_workflow_task_execution_failed{failure_reason="NonDeterminismError"}`
(the SDK maps a history mismatch to that cause in
`internal/internal_task_pollers.go:835-836`) and, in the pod log, the messages
`Workflow panic` or `Failed to process workflow task.` — grep their structured
`Error` field, not the message, for `[TMPRL1100] nondeterministic workflow`.

```bash
temporal workflow list --address 127.0.0.1:7233 --namespace vector \
  --query 'WorkflowType="MorphoVaultBackfill" AND ExecutionStatus="Running"'
temporal workflow terminate --address 127.0.0.1:7233 --namespace vector \
  --workflow-id <id> --reason 'rolling morpho-vault-backfill'
```

**Reading progress and re-running.** The `progress` query (UI → Query tab) shows
`subRangesDone` / `subRangesTotal` for discovery and `partitionsDone` /
`partitionsTotal` for replay mid-run, and survives a failed run, whose Result
panel Temporal discards. A failed sub-range or partition stops the run there, so
the completed prefix is what the query reports.

To resume a FAILED execution, reset it rather than starting a new one — a fresh
start has empty history and rescans from the run's own `from`, discarding every
banked sub-range. Reset to a point BEFORE the failing activity was scheduled:
activity results are never reapplied, only re-executed, so a reset to the last
workflow task carries the recorded failure into the new execution and it stops at
the same place. Read the history, find the `WorkflowTaskCompleted` just before the
failing `ActivityTaskScheduled`, and reset to that event id. The reset point is
EXCLUSIVE: that workflow task is not replayed, so the commands it issued — the
failing activity among them — are re-issued, while every sub-range and partition
completed before it stays banked.

```bash
temporal workflow show --address 127.0.0.1:7233 --namespace vector --workflow-id <id>
temporal workflow reset --address 127.0.0.1:7233 --namespace vector --workflow-id <id> \
  --event-id <the WorkflowTaskCompleted before the failing ActivityTaskScheduled> \
  --reason 'resuming after a failed sub-range'
```

Two flag traps here. `--reason` is REQUIRED on `reset` (the command fails
client-side without it), unlike on `terminate` above where it is optional. And do
NOT add `--type` alongside `--event-id`: passing both raises no error, but
`--type` silently wins and your event id is discarded — `--type LastWorkflowTask`
gets you exactly the failure-carrying reset this paragraph is telling you to
avoid.

Check the new execution's `progress` query afterwards to confirm it resumed where
you intended rather than earlier. Starting the same range fresh is always safe
too, just slower: every write is an idempotent append, so a repeat costs wall
clock, not correctness.

`candidates` and `vaults` — in the query, the Result panel and the completion log
— are summed across the sub-ranges, so an address appearing in several is counted
once per sub-range: they measure scan volume, not distinct addresses or vaults.
`knownV2Vaults` is a registry count and is exact.

The query, the Result panel and each partition's `replayed partition` log line
also carry `rowsAppended`, split per table: `adapterStates`, `vaultCaps`,
`vaultFees`, `membershipObservations`. It is a different quantity from
`eventsReplayed` — events counts the logs driven through the handler path, rows
counts the versioned snapshots those logs appended — and the two diverge for
legitimate reasons as well as for bugs:

- `ForceDeallocate` appends no snapshot at all; its paired `Deallocate` in the
  same transaction carries the adapter-state row, so a partition holding only
  those logs reports events with no rows.
- `membershipObservations` counts appended observations only. An assertion that
  the log already answers at that block position appends nothing, whatever the
  build, so 0 is ordinary on a re-run.
- The per-event `protocol_event` audit row is written but NOT counted here.

What is worth investigating is `adapterStates` = 0 on a partition holding
allocation, cap or fee events when the range is fresh, or when the same range is
re-run from a NEW `build_id` (which must append a new `processing_version`). That
is the shape of the compressed-chunk drop VEC-218 fixed for `morpho_adapter_state`
— and the shape `protocol_event` still has, since its INSERT still leaves the
version to its trigger.

**Failure modes specific to this worker.**

- `AccessDenied` listing or reading the raw bucket → the EKS Pod Identity
  association granting this ServiceAccount S3 read on the per-chain raw bucket is
  missing. It lives in the infra repo, not here.
- `partition ... missing receipt block(s) ... (S3 gap)` → the archive is
  genuinely incomplete for that partition. Replay hard-stops rather than replay
  a thinned partition; repair S3 and re-run the same range.
- `block <N> (<partition>/<N>_<v>_receipts.json.gz): archived hash 0x… is an
  orphaned fork, canonical hash at that height is 0x…: fetching header for block
  0x…: not found` → the node does not know the archived hash but does know a
  different block at that height, so the block's highest-version receipts object
  is an orphaned fork: the watcher published it and never re-published the
  canonical block (a missed reorg), and the live indexers ingested it too.
  Structural — the run fails on the first attempt instead of burning the 2h retry
  envelope (87 attempts, 2026-08-29 staging, block 25395651). The error names the
  canonical hash itself, so `cast block <N> --field hash` is only a cross-check.
  Re-archive the canonical receipts at a higher version, re-run from that block,
  and audit the live tables at that block: the orphaned rows are the ones at the
  orphaned object's version — the `<v>` in the key the error names, which is
  `block_version` 0 only when that object is version 0 — and the append-only
  correction is a re-publish of the canonical block, not an edit.
- The same `fetching header for block 0x…: not found` **without** a canonical
  hash (it ends `reading the canonical header at that height: …`, or `yet it is
  the canonical hash at that height`) is left retryable on purpose: a
  load-balanced RPC replica a few blocks behind head also answers null for a hash
  it has not reached yet, and nothing is proven until a by-number read returns a
  different hash. If it keeps failing, the node — not S3 — is what to look at.
- An adapter-classification failure — same cause and same recovery as the
  bootstrap's, below; the two share the VaultV2 replay path.

---

### Special case: `morpho-v2-bootstrap` (on-demand, no schedule)

Both history jobs emit the same `morpho_v2_*` metrics as the live indexer (the
replay path is metered since VEC-218), so the V2 volume alerts in
`vector-indexers.yaml` can fire during a deliberate replay or bootstrap run —
expected, not an incident; the run is operator-initiated and visible here.
`VectorMorphoV2ForceDeallocateSurge` is the exception: it is scoped to the live
indexer's `service_name`, because replayed history is not a liquidity run.

A third **on-demand** Temporal worker (`temporal.RunWorker`). Everything said
about `offchain-price-backfill` above applies — nothing is missed while it is
down, and it is excluded from `VectorCronjobAllRunsFailing` for the same reason.

**How to start a run.** Temporal UI (namespace **`vector`**) →
**Start Workflow**:

| Field | Value |
|---|---|
| Task Queue | `morpho-v2-bootstrap` |
| Workflow Type | `MorphoV2Bootstrap` |
| Workflow ID | descriptive and unique, e.g. `morpho-v2-bootstrap-2026-08-20` |
| Input | leave empty |

There is nothing to supply: the run reads the chain from its ConfigMap, the V2
vault set from the database, and pins its own finalized head. The equivalent CLI
call, which is how the local `make run-cronjob-solo NAME=morpho-v2-bootstrap`
worker is driven too:

```bash
temporal workflow start --namespace vector \
  --task-queue morpho-v2-bootstrap --type MorphoV2Bootstrap \
  --workflow-id morpho-v2-bootstrap-2026-08-20
```

**What a run does, and how long it takes.** One activity for the whole job: sweep
`eth_getLogs` from the VaultV2 factory deploy block to a pinned finalized block
for the 10 VaultV2 governance events, replay each through the live handler path,
then enumerate every V2 vault's current adapter set and snapshot each adapter's
`realAssets()`. A full mainnet sweep measures in **minutes** on today's V2 era
(~15m end to end, measured 2026-08) — the 6h `StartToClose` / 12h `ScheduleToClose`
bounds (3 attempts, 60 s heartbeat) are headroom for era growth and provider
slowness, all compiled into the worker, so an operator supplies none of it. A run
still going after an hour is a stall signal, not normal.
Unlike the backfill, progress lives in the activity's heartbeat details rather
than in workflow history; see the resume note at the top of this runbook.

---

### Special case: `block-republisher` (on-demand, no schedule)

Another **on-demand** Temporal worker (`temporal.RunWorker`). Everything said
about `offchain-price-backfill` above applies — nothing is missed while it is
down, and it is excluded from `VectorCronjobAllRunsFailing` for the same reason.

**What it repairs.** A height whose only published version is a losing fork
(ARCT-379): the watcher dropped the canonical broadcast as a `stale_fork`, the
next block's reorg commit orphaned the height without replacing it, and nothing
re-fetched it. S3 then holds only that height's `_0_` objects and every indexer
holds its events at `block_version` 0. Nothing else repairs it — the watcher's
own retry only reaches non-orphaned `block_states` rows, which the 30-day
retention drops, and `raw-block-bulk-downloader` repairs S3 without telling the
indexers.

**How to start a run.** Temporal UI (namespace **`vector`**) → **Start
Workflow**:

| Field | Value |
|---|---|
| Task Queue | `block-republisher` |
| Workflow Type | `BlockRepublish` |
| Workflow ID | descriptive and unique, e.g. `block-republisher-2026-09-01` |
| Input | `{"blocks":[25395651,25087888]}` |

`blocks` are the heights, processed in the order given, at most **200** per run,
each named at most once, and with the optional `archiveRepaired` flag below they
are the whole input. Every height must sit at least 64 blocks below the chain
head. The chain comes from the Deployment's `CHAIN_ID`, and the version from the
archive (below) — neither is an input. An input carrying `version`, or any other
field, **fails the run non-retryably**: a run started from the old runbook line
must stop rather than quietly do something else. The equivalent CLI call:

```bash
temporal workflow start --namespace vector \
  --task-queue block-republisher --type BlockRepublish \
  --workflow-id block-republisher-2026-09-01 \
  --input '{"blocks":[25395651,25087888]}'
```

**A height that needs no repair is refused, not republished.** While deriving the
version the worker reads the block hash out of the first 8 KB of the top archived
version's `_block` object (falling back to its `_receipts`) and compares it with
the canonical hash the node reports. If they match, that height fails
`StructuralData` — "already canonical in the archive at version N" — before
anything is cached or published, so a healthy height **the archive holds** cannot
gain a permanent identical extra version. A version that names no block at all
(no `_block`, no `_receipts`, or only a data type this binary does not know) is
treated as a height to repair, and the run proceeds at the next slot. A height the
archive holds nothing for has no object to compare against at all: it is repaired
at version 1 on your word alone, and logs a warning saying so — list such a height
only once the dry run below or the indexers have proved the fork.

That makes the bulk downloader's dry run (#849) a cheap pre-check rather than a
safety net — useful for sizing a run before you start it:

```bash
raw-block-bulk-downloader --dry-run --bucket $RAW_BUCKET --rpc-url <chain rpc> \
  --start-block 25395651 --end-block 25395651
```

It reports each height as archived ≠ canonical or not, from the same read.

**The version comes from the archive, per height.** `block_version` is the reorg
counter, so a height that genuinely reorged once already has a real version 1,
and republishing into an occupied slot would append rows that every reader
(`ORDER BY block_version DESC, processing_version DESC`) prefers over the genuine
ones. So the worker lists the height's own prefix in the chain's raw bucket
(`S3_BUCKET`) and publishes at **one past the highest `<number>_<version>_*`
object there — 1 where there is none, never 0**, which holds the data being
corrected. An object at a version occupies it whatever data type it carries, so a
half-written correction still moves the next one up. Two heights in one run
routinely land at different versions; each block's own version is in the result,
in the `progress` query and in the worker's log line for it.

You can predict what a height will get, though nothing requires you to
(`$RAW_BUCKET` is the chain's `stl-sentinel<env>-<chain>-raw-<suffix>` bucket, as
on the backup worker):

```bash
aws s3 ls s3://$RAW_BUCKET/25395000-25395999/ | grep 25395651_
```

Only `_0_` present is the ARCT-379 shape, and that height will be republished at
1; a height that also shows `_1_` goes to 2.

**`archiveRepaired: true`, for a height #849 already fixed in S3.** The bulk
downloader repairs the archive and tells no indexer, so a height it has been
pointed at holds the canonical block in S3 while the indexers still hold the
fork — and the derivation above, which lands one past what the archive holds,
then refuses that height forever as already canonical. Adding the flag
(`{"blocks":[25395651],"archiveRepaired":true}`) publishes each named height
**at** the version the archive's top objects occupy instead of one past it, so
the backup worker's if-not-exists write is a no-op and every indexer appends
that version. It is not a force flag: the worker still reads the top version's
hash, and fails the height `StructuralData` if the archive holds nothing there,
if that version names no block, or if it is not the canonical one ("drop
archiveRepaired") — so a healthy height cannot gain a duplicate version with it
either. Version 0 is a legal answer here and only here: a height the archive
never held at all is repaired by #849 at `_0_`. The flag applies to every height
in the run, so keep such a run to the heights #849 actually repaired.

The archive is written by `raw-data-backup` from the event, so it trails a
healthy run by up to a minute — and stays behind for good if that worker
dead-lettered the block. If a height holds fewer versions than you expect, check
what the indexers have before starting a run:

`protocol_event` is not the only place a version lands — 37 public tables carry a
`block_version` today (`allocation_position`, `position_state`,
`token_total_supply`, `psm3_reserves`, `fluid_vault_state`, `onchain_token_price`
and the `morpho_*`, `curve_*`, `uniswap_v3_*` and `sparklend_reserve_data*`
families among them). Generate the check rather than maintaining that list here:

```sql
SELECT string_agg(
         format('SELECT %L AS source, max(block_version) AS block_version FROM %I WHERE block_number = %s',
                table_name, table_name, 25395651),
         E'\nUNION ALL ' ORDER BY table_name)
FROM information_schema.columns
WHERE table_schema = 'public' AND column_name = 'block_version';
```

Run what it prints. Only seven of those tables carry a `chain_id` of their own
(`protocol_event`, `allocation_position`, `allocation_position_current`,
`position_state`, `psm3_reserves`, `psm3_alm_shares`, `token_total_supply`); the
rest reach the chain through a registry FK, so on a multi-chain database read a
hit as "some chain has this version" and confirm before acting on it.

A version there that the archive does not hold means the repair would land on a
slot the indexers already have: fix the archive gap (the backup worker's DLQ)
first. This query is the only guard on that. A republish at a version an indexer
already holds is silent all the way down — the consumers insert `ON CONFLICT …
DO NOTHING`, the backup worker writes only where the object is absent, and this
worker reads no database at all — so the run reports success and the fork stays
the highest version anywhere it matters.

**Before the first run.** The ServiceAccount's EKS Pod Identity association needs
`sns:Publish` on the chain's blocks topic, `s3:ListBucket` on the chain's raw
bucket (which versions are taken) and `s3:GetObject` on it (the first kilobytes of
the top version's block object, to tell an already-canonical height from a losing
fork). Nothing is ever written to S3. `sns:Publish` and `s3:ListBucket` come from
archon-research/infrastructure#667 (merged); `s3:GetObject` from
archon-research/infrastructure#669, merged and applied on staging and prod on
2026-09-03. The worker proves both S3 grants at startup — one `ListObjectsV2`
under the `0-999/0_` prefix and one one-byte ranged `GetObject` of
`0-999/0_startup-probe`, a key that cannot exist, so a grant conditioned on a
prefix covers both — and a missing grant or a mistyped `S3_BUCKET` is therefore a
pod that will not start, logging `this pod needs s3:ListBucket` or `this pod
needs s3:GetObject on that bucket`, rather than a run that dies on its first
archived height. `ALCHEMY_HTTP_URL` is required too, with no mainnet default: a
deployment without it would refuse to start rather than fetch another chain's
blocks.

**What a run does.** Two activities per block. `DeriveVersion` lists the height's
archive prefix and settles the version, which Temporal records in the workflow's
history — that is what makes a retried republish reuse the slot instead of
stepping past the objects its own first attempt caused. `RepublishBlock` then
reads the chain head and refuses any height inside the 64-block reorg window,
reads the block by number (the canonical block at fetch time), fetches block +
receipts + traces pinned to that hash, re-reads the number and **refuses if the
canonical hash moved between the two reads**, writes the payload to
`stl:{chainId}:{number}:{version}:{dataType}` in Redis, and publishes a
`BlockEvent` at that version on the chain's SNS topic. A block takes seconds; a
200-block run takes minutes. The first failing block stops the run — the blocks
before it are already durable, so the retry is the remaining list.

**It does not write `block_states`.** The `assign_block_version` trigger
overwrites the supplied version with `MAX(version)+1` over the rows surviving at
that height, and after the 30-day retention there are none — it would hand back
0 and re-stamp the losing fork's own slot. Writing there would also hand the
watcher's gap filler an unpublished row to re-publish and pin its backfill
watermark behind the repaired height. The durable record of a republish is the
SNS event, the S3 objects and the indexers' rows.

**What to verify afterwards.** Take each height's version from the run's result
(or the `progress` query): they need not be the same, and the checks below are
per height. The example uses a height that landed at 1.

1. The archive holds the new version — `raw-data-backup` writes it from the same
   event, usually within a minute:
   ```bash
   aws s3 ls s3://$RAW_BUCKET/25395000-25395999/ | grep 25395651_1_
   ```
   Expect `_1_block`, `_1_receipts` and (Ethereum only) `_1_traces`. This is also
   what makes a re-run of the same height idempotent in the sense that matters:
   the next run reads these objects and would go to 2, so an accidental repeat
   appends a new version rather than overwriting one.
2. The indexers appended the corrected version:
   ```sql
   SELECT block_version, count(*) FROM protocol_event
   WHERE chain_id = 1 AND block_number = 25395651 GROUP BY 1 ORDER BY 1;
   ```
   Expect a `block_version = 1` group alongside the old `0`. Nothing is deleted:
   the correction is the newer version, and every reader takes the highest.
3. `morpho-vault-backfill` can now replay the partition the height sits in — its
   replay reads the archive by partition prefix, so it picks up the `_1_` objects
   the step above confirmed.

**Failure modes specific to this worker.**

- **`StructuralData`: the height is above the chain head or within 64 blocks of
  it** (non-retryable, run goes red immediately). The worker refuses to repair
  inside the reorg window: two reads seconds apart would both see the same fork,
  and the "correction" could itself be orphaned. A different node does not help —
  wait until the height is deep enough (64 blocks is about 13 minutes on
  mainnet), then start a new run.
- **The node answers null for the block or one of its payloads (retryable).**
  The head read already proved a synced node knows this height, so a null is a
  replica behind that head and the next attempt asks again. If every attempt
  inside the 30-minute envelope answers null, the RPC has no history this deep:
  point at one that does — an archive node — and start a new run.
- **`StructuralData`: the archive already holds the canonical block at the
  height's top version** (non-retryable). Nothing to repair: the archived hash
  and the chain's agree. The message names the version and the hash. Drop that
  height from the list — a re-run cannot change the answer, and forcing it would
  append an identical correction every reader then prefers. The one exception is
  a height #849 repaired ahead of the indexers, where `archiveRepaired: true`
  above is the answer.
- **`StructuralData`: an object under the height's archive prefix carries no
  version** (non-retryable). `DeriveVersion` cannot tell which slot is free while
  something it cannot read sits there. The error names the key: remove or rename
  it, then start a new run.
- **`StructuralData`: an archived object cannot be read** (non-retryable). The
  top version's `_block` or `_receipts` object is not a gzip stream, holds no
  bytes at all, or keeps its hash past the first 8 KB — so the already-canonical
  check cannot run, and no attempt reads it differently. The error names the
  key: repair or remove that object, then start a new run.
- **`InvalidParams` (non-retryable, nothing is republished)** — the input is not
  the object this workflow takes: an empty or oversized `blocks` list, a
  non-positive height, or a field it does not accept, `version` above all. Fix
  the input and start a new run; nothing was published, so the list is unchanged.
- **Canonical hash moved mid-republish (retryable)** — a reorg deeper than the
  64-block guard. The activity retries inside a 30-minute envelope, which is
  normally long enough for the chain to settle.
- **The archive listing fails (retryable)** — a throttled `ListObjectsV2` on the
  raw bucket leaves the height with no version to publish under, so
  `DeriveVersion` retries rather than guessing, and settles inside the 30-minute
  envelope. A missing `s3:ListBucket` grant cannot show up here: the startup
  probe would have kept the pod from starting.
- **A republished block poisons an indexer's queue.** SNS FIFO groups by chain,
  so one block an indexer cannot process head-of-line-blocks every later block on
  that chain's queue until SQS redrives it to the DLQ. The usual cause is a state
  read at a height the node has no archive state for. Watch the consumers' stall
  alerts (`VectorMorphoIndexerStalled`, `VectorOracleIndexerStalled`,
  `VectorFluidVaultIndexerStalled`, `VectorBackupWorkerStalled`) for the first
  few minutes of a run — republish a single block first and confirm they stay
  quiet before running a list.
- **A block is published twice.** An activity Temporal cancelled (a rolled pod)
  or timed out after its publish is retried at the version the run already
  settled on, so the repeat is the same event. The activity heartbeats every 10s
  against a 30s timeout, so a killed worker is noticed within 30s and the retry
  usually lands inside SNS FIFO's five-minute deduplication window, where the
  repeat never reaches the queues at all; outside it every append-only consumer
  re-derives identical rows anyway. The case to avoid is a second **run** over a height that
  already succeeded — the archive holds that version by then, so it lands one
  slot further along. Naming the same height twice in one run is refused outright.
- **Nothing arrives downstream** — check the worker actually published
  (`kubectl -n vector logs deploy/block-republisher | grep 'republished block'`)
  before suspecting the consumers; SNS FIFO silently drops a repeat of the same
  `{chainId}:{blockHash}:{version}` inside its five-minute deduplication window.

**Local runs.** The kind cluster's LocalStack topic is `stl-ethereum-blocks.fifo`,
which the worker's startup guard rejects — it requires the deployed
`stl-sentinel<env>-<chain>-blocks.fifo` naming for the configured `CHAIN_ID` and
`DEPLOY_ENV`. Run it against a real environment's topic, or rename the dev topic.
`S3_BUCKET` is guarded the same way, against `stl-sentinel<env>-<chain>-raw`.

---

## VectorCoreModelRunnerStale

**Severity:** critical (pages) · **For:** 30m · **Window:** 30h

### What it means

`core-model-runner` has not recorded a **completed** run — `status="success"`
or `status="error"` — in 30h. The schedule is 24h and a tick may legitimately
run up to 4h (`TICK_TIMEOUT`, in `app/services/core_model_runner/workflow.py`),
so two healthy completions are at most ~28h apart; 30h without one is a stall.
`core_model_results` has stopped advancing, and the API keeps serving its
newest row as the current CRR — there is no age check on the read side — so
the published number is going stale without anything else saying so.

A tick that **fails** is deliberately not a stall here: `run_markets` fails the
whole tick when any one market fails, so a single market with a known input
gap would otherwise keep this rule paging every day while the other markets
advance normally. A failed tick is `VectorCronjobRunFailing`'s (warning). The
gap that leaves — a tick failing every day, for every market, only ever warns —
is tracked in [VEC-665](https://linear.app/archontech/issue/VEC-665) (per-market
success signal); until then, treat a `VectorCronjobRunFailing` for
`core-model-runner` on two consecutive days as if it were this alert.

### Why the generic rules do not catch it

`core-model-runner` is excluded from `VectorCronjobAllRunsFailing` (24h
interval, `RetryPolicy.maximum_attempts=1`: one failed tick can never recover
inside that rule's 1h window). That leaves `VectorCronjobRunFailing`, which is
a warning and fires only when the activity **raises**. Four ways a tick is
lost record no error at all:

1. **A deploy landed mid-tick.** The worker cancels the running activity on
   shutdown; the harness classifies that `status="canceled"`, deliberately not
   `"error"`. With no retry, that day's tick is simply gone.
2. **The tick hung.** Temporal times the activity out at 4h, but the sync
   activity thread never learns of it (the runner does not heartbeat), so the
   metrics interceptor never returns and **no run is recorded at all**. The
   failed workflow is visible only in the Temporal UI.
3. **The pod was SIGKILLed** after `terminationGracePeriodSeconds` while deep
   in a numpy call (the injected cancel exception cannot land inside C code):
   nothing recorded, nothing flushed.
4. **Karpenter evicted the pod mid-tick** (VEC-681, first seen in prod on
   27 Aug 2026). Mechanically case 3, but the trigger is neither a deploy nor a
   fault: consolidation reclaims an underutilised node at any hour, so a healthy
   tick on a healthy pod is lost with the pod's own logs gone with it. The pod
   template now carries `karpenter.sh/do-not-disrupt: "true"` as a stopgap;
   until the tick is resumable that annotation is the only thing preventing a
   repeat, and it does **not** cover staging, where nodes are spot and reclaim
   is forceful.

`VectorCronjobWorkerDown` does not help either: cases 1-3 leave the Deployment
`1/1` available throughout, and case 4's replacement pod schedules well inside
that rule's 10m `for`.

**A single lost tick is not necessarily caught.** This rule's 30h window is
measured from the last completion, not from the schedule, so a lost **scheduled**
tick fires it ~6.5h late (once the previous day's completion ages out) and a lost
**hand-triggered** run never fires it at all — the next 00:00 tick completes
inside 30h. That is why the 27 Aug eviction was found by hand rather than by an
alert.

**What this rule cannot see.** A live pod whose OTLP export dies (collector
unreachable, exporter thread wedged) stops refreshing the series; once it
staleness-expires, `increase(...[30h])` has fewer than two samples, returns no
data, and this rule goes **silent** rather than firing. `VectorCronjobWorkerDown`
does not cover that either — the pod is `1/1`. The same residual is documented
on the sibling stall rules in `alerts/vector-indexers.yaml`. If the runner's
series vanish from Grafana while the pod is up, check the pod's
`cronjob run metrics initialized` / exporter warnings in its logs.

The rule is also gated on the success series having existed 29h ago, so a
freshly registered Deployment does not page on its first day (the seeded series
starts at 0 and the first tick is at the next 00:00 UTC). The first tick after
a cold start is therefore covered only by `VectorCronjobRunFailing`.

### First checks

1. **Temporal UI** — `vector` namespace → Schedules → `core-model-runner`.
   Look at the recent actions: a **Failed** workflow with a
   `StartToClose`/`ScheduleToClose` timeout is the hang case; a **Canceled** or
   failed one at a deploy time is case 1; **no** recent action means the
   schedule itself is paused or the worker is not polling.
2. **Pod logs** — `kubectl -n vector logs deploy/core-model-runner --tail=200`.
   A healthy tick logs `running market_key=…` for every market and ends with
   `result written to core_model_results market_key=…` for each. A market that
   raised logs `failed market_key=… -- continuing` and the tick ends in
   `one or more markets failed`. Common causes: a live reader refusing its
   input (missing price-history days, a dead oracle feed, no borrower rows —
   the message names the exact gap; see
   `stl-verify/python/app/risk_engine/core_model/DATA_GAPS.md`), or the
   database being unreachable.
3. **Did a deploy land during the tick?** —
   `kubectl -n vector rollout history deploy/core-model-runner`; compare with
   the tick's start (00:00 UTC by default). If so, nothing is broken — the day
   was lost to the rollout.
   **Or was the pod evicted?** — same symptom, different cause, and the
   rollout history is empty because the ReplicaSet never changed. Check
   `kubectl -n vector get events --field-selector involvedObject.name=<pod>`
   for `Evicted pod: Underutilized` (Karpenter consolidation) and confirm the
   pod name changed without a new ReplicaSet.
4. **Memory** — `kubectl -n vector describe pod -l app=core-model-runner` for
   `OOMKilled`. A raised `CORE_MODEL_N_MC` without a matching memory limit is
   the expected way to hit this (see the ConfigMap and Deployment comments).

### Resolution

Fix the cause, then start the schedule by hand from the Temporal UI (Schedules
→ `core-model-runner` → Trigger) rather than waiting for the next 00:00 tick.
A hand-triggered run is identical to a scheduled one (`market_key=all`), and
`core_model_results` is append-only, so a second run on the same day is a new
row, not a conflict.

### Verify recovery

A new completed run for `core-model-runner` in `cronjob_runs_total` clears the
rule on the next evaluation. Recovery of the *data* is a `status="success"`
sample and a fresh `computed_at` for every market in `core_model_results` — an
`error` completion clears this alert but leaves `VectorCronjobRunFailing` to
tell you which market is still broken.

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

This alert also fires if `reference_capital_sync_snapshots_written_total`
stops being emitted at all (a collector drop or a metric rename), not only
when it is present and reads zero — check that the series still exists at all
before chasing an upstream cause.

**Serving impact.** This table is also where the API reads *coverage* from, and
where `/v1/primes/{id}/risk-capital` reads its reference totals. A stall does
not lose coverage — the existing rows still answer it — so nothing starts
404ing; instead the reference figures freeze while `reference_synced_at` falls
further behind, and `/v1/provenance/available` keeps offering `reference` for
every prime that has ever been covered.

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

---

## VectorReferenceCapitalIndexerAllocationsZero

**What it means.** Cycles are succeeding but `prime_capital_stack_allocation`
received no rows for an hour. The per-allocation breakdown behind the
prime-level totals has stopped advancing, and like the prime-level series it
cannot be backfilled afterwards — the monitor publishes no history.

**Why it is not caught by the generic rules.** The run returns no error, so
`VectorCronjobRunFailing` stays quiet — and the service deliberately fails a
cycle whose covered primes report exposure with an empty breakdown, so a
successful cycle writing zero rows means every covered prime reported zero
exposure. That is either a market state worth confirming or an upstream fault
wearing its shape.

**Triage.**

1. Confirm the worker is cycling:
   `kubectl -n vector logs deploy/reference-capital-indexer --tail=100`.
2. Ask the breakdown route directly for a covered prime:
   `curl -s "$SKY_RISK_CAPITAL_URL/primes/spark/allocations/?limit=500" | jq '.data.results | length'`.
   Rows here with zero rows landing means the payload changed shape — the
   client should have errored, so check its logs for parse failures.
3. Cross-check the prime-level series: if
   `VectorReferenceCapitalIndexerWritesZero` is also firing, treat that as the
   primary signal — the whole feed stalled, not just the breakdown.

**Serving impact.** `/v1/primes/{id}/risk-capital?source=reference` reads its
`per_allocation` breakdown from this table, pinned to the same cycle its totals
came from so the two cannot be mixed. A totals row with no matching breakdown
and non-zero exposure — every cycle recorded before 2026-08-26, before this
table existed — is skipped rather than served: the reader falls back to that
prime's last complete cycle, or to a **404** (`both` degrading to `indexed`) if
it has none. It cannot 500 for this reason — the three reference tables land in
one transaction, so a cycle that wrote totals always wrote its breakdown too.

**Resolution.** Same posture as `WritesZero`: upstream coverage is upstream's.
Confirm what the monitor reports and reconcile; the gap in the series stays.

---

## VectorReferenceCapitalIndexerPositionsZero

**What it means.** Cycles are succeeding but
`reference_capital_sync_positions_written_total` recorded no increase for an
hour. This points at the counter, not the pipeline: see below for why.

**Why it is not caught by the generic rules, and why it is not a data gap.**
The positions client fails the whole cycle on an empty result for a covered
prime — the feed answers unknown primes with `200` and an empty list, so
emptiness is deliberately never persisted — and `Run` fails before persisting
anything if the cycle's snapshot set is empty. So a cycle that reports success
always covers at least one star and always wrote at least one position row.
Zero on this counter while cycles succeed can therefore only mean the counter
itself broke (collector drop, metric rename, a missed recording call), never
that positions stopped landing.

**A third failure mode this alert cannot catch.** A prime the Star monitor
covers but this positions feed does not carry makes every cycle fail loudly
instead — `VectorCronjobRunFailing` fires, not this alert. The escape hatch is
a deliberate team decision to gate positions on the positions feed's own
coverage — a code change, never a silent skip.

**Triage.**

1. Confirm the worker is cycling:
   `kubectl -n vector logs deploy/reference-capital-indexer --tail=100`.
2. Compare against the table:
   `SELECT max(synced_at) FROM prime_reference_position;` — if rows are
   landing at the expected cadence, this is telemetry-only: fix the counter
   (check the metric name/labels and that `RecordPositionsWritten` is on the
   success path), not the pipeline.
3. If rows are genuinely not landing despite cycles succeeding, that
   contradicts the invariant above — treat it as a code regression in the
   positions client's empty-result guard, not a routine data gap.

**Serving impact.** `/v1/primes/{id}/allocations?source=reference` reads this
table, taking the newest cycle that has rows, so a stall serves a frozen balance
sheet rather than an empty list — `reference_synced_at` on each row is what says
how old it is. A prime that has *never* had rows here answers `404` on that
endpoint, deliberately: an empty list would claim the prime holds nothing.
`/v1/provenance/available` may still offer `reference` for it, since coverage
there comes from `prime_capital_stack`.

**Resolution.** A telemetry fix ships as a normal PR; no upstream reconciliation
or accepted gap applies here, unlike `WritesZero`/`AllocationsZero`.

---

## VectorReferenceCapitalIndexerBalanceSheetPrimeUncovered

**What it means.** A prime STL tracks was absent from every day the
balance-sheet feed's fetch window held for an hour. That prime's balance
sheet is frozen while every other tracked prime keeps advancing, and because
the read path gap-fills with `locf`, its `/debt` and `/total-capital` series
keep serving the last observed value as if it were current.

**Why it is not caught by the generic rules.** The cycle succeeds and still
inserts rows for every other covered prime, so neither the error rules nor
`VectorReferenceCapitalIndexerBalanceSheetStalled` fire — this is the single
tracked-prime version of that gap, the balance-sheet analogue of
`VectorReferenceCapitalIndexerPrimeUncovered`.

**Triage.**

1. `{{ $labels.star }}` names the prime. Ask the feed directly for its recent
   days:
   `curl -s "$SKY_DATA_URL/primes/historic/?days_ago=3" | jq -r '.data[].star' | sort -u`.
   If the star is absent from that list, the feed dropped it; a
   similar-but-different name present instead means the vocabulary drifted.
2. Compare against the table for when the prime last landed a row:
   `SELECT max(observed_at) FROM prime_reference_balance_sheet WHERE prime_id =
   (SELECT id FROM prime WHERE name = '<star>');`
3. Compare against what STL tracks — the same axis-synome contract the
   snapshot-level `PrimeUncovered` triage uses. Note the `prime` table is
   **not** the tracked set.

**Resolution.**

- *Feed dropped the prime.* Nothing to fix in the service; the balance sheet
  is correctly frozen. Decide with the team whether the prime should still be
  tracked, and silence the alert while that is open.
- *Name drifted.* Fix it in the axis-synome contract, not by mapping the name
  in the indexer — a local alias would hide the next drift.

Do not "fix" this by relaxing the indexer to accept partial balance-sheet
coverage silently. The alert exists precisely because a partially-covered
cycle looks healthy.

---

## VectorReferenceCapitalIndexerBalanceSheetStalled

**What it means.** Cycles are succeeding but `prime_reference_balance_sheet`
has had zero newly-inserted rows for 36h. The daily balance-sheet write path
has stopped advancing for every tracked prime at once, and via `locf` every
prime's `/debt` and `/total-capital` series is now frozen on its last value.

**Why it is not caught by the generic rules, and why the window is 36h.** The
run returns no error, so `VectorCronjobRunFailing` stays quiet. Unlike
`WritesZero`/`AllocationsZero`/`PositionsZero`, which use a 1h window, this
feed publishes one day per prime per UTC day and the client deliberately drops
the current in-progress day — so on most cycles the insert count is
legitimately zero, and a 1h window would false-positive on every run that
happens to land between upstream publish times. `[36h]` on the `increase()`,
with a matching `for: 2h`, gives the daily cadence room to land at least once
before this fires. The underlying counter only counts a day's first-ever
insert (`processing_version=0`), not a build's correction to an
already-stored day, so a deploy replaying the lookback cannot silence it.

**Triage.**

1. Confirm the worker is cycling:
   `kubectl -n vector logs deploy/reference-capital-indexer --tail=100`. Each
   cycle logs `balance sheet advanced inserted=<n> new_days=<n> fetched=<n>`.
   `inserted` includes build corrections to already-stored days and goes
   non-zero on almost every cycle after a deploy — it is not the alert's
   signal. `new_days` is: it only goes non-zero roughly once per day per
   prime, when that prime's newly-completed day lands for the first time. If
   you never see a non-zero `new_days` across a full day, that corroborates
   the alert.
2. Ask the feed directly whether it has published recent days at all:
   `curl -s "$SKY_DATA_URL/primes/historic/?days_ago=3" | jq '.data | group_by(.star) | map({star: .[0].star, dates: map(.date)})'`.
3. Compare against the table:
   `SELECT prime_id, max(observed_at) FROM prime_reference_balance_sheet GROUP
   BY prime_id;` — if every prime's `max(observed_at)` is stuck more than a day
   or two in the past while the feed above shows fresh dates, the client is
   failing to parse or persist rather than the feed being empty; check the pod
   logs for parse failures.
4. If only one prime is affected rather than the whole feed, that is
   `VectorReferenceCapitalIndexerBalanceSheetPrimeUncovered` instead — treat
   that as the primary signal.

**Resolution.** If the feed itself has gone stale (step 2 shows no dates newer
than the last insert), this is upstream — confirm with the team and wait. If
the feed has fresh data but the client is not persisting it, this is a service
bug: fix the parse/insert path, not the alert's window. The gap in the series
stays regardless — say so rather than backfilling from a different feed, which
would splice a different measurement.

**First-deploy note.** The counter series starts younger than the 36h window
on a fresh rollout, so this can fire once before the first day lands even on a
healthy worker. Check the deployment timestamp before treating a very-recent
first firing as a real stall.

---

## morpho-v2-bootstrap run outcomes

**Nothing here needs rows reconciling by hand.** Adapter membership is an
append-only observation log, so a failed pass writes no lifecycle a later run has
to walk back, and re-running is always safe. Three things can stop a run:

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

**3. `N of M vaults could not be seeded`.** The seed pass deliberately does not
stop at the first bad vault — a vault-shaped contract it cannot probe fails
identically on every future run, so aborting there would leave every vault after
it unhealed forever. So everything healable in that run was already attempted,
and the joined error names each vault that was not. Work through those
individually; re-running unchanged produces the same set. The run stays red until
each one is fixed or explicitly written off, which is the point: a hole is
reported, never hidden.

**Not failures:**

- A `RemoveAdapter` for an adapter the registry has never seen. It records one
  untyped `is_member = false` observation, which is the truthful record of
  learning about an adapter from its own de-registration.
- A green run whose logs carry `deferredVaults=N`. A vault first SEEN above the
  run's pinned finalized head is DEFERRED, not skipped: nothing proves it had
  code at that head, and live indexing has owned it since its first event. Each
  one is named in its own WARN, and the next run pins a later head that includes
  it — so the scope heals itself once finality passes them, and there is nothing
  to do. Evidence is log-only; no metric counts deferrals.

  If EVERY known vault is deferred there is nothing left to work on, so the run
  fails instead, with `all N known VaultV2 vaults of chain <id> were first seen
  above the pinned head <block> — re-run once finality passes them`. That is the
  same benign state: re-run later rather than checking `CHAIN_ID` and
  `DATABASE_URL`, which this message deliberately does not blame.

**Recovery.** Let Temporal's retries run; if the run is still red, start another
one (see the section above — a fresh run starts from the factory deploy block,
safe but slow). Escalate to the Vector team if the same non-transient error
repeats across runs: that is a code defect, not an operational state.

This is a property of the shared VaultV2 replay path, not of the bootstrap alone
— `morpho-vault-backfill` replays through the same handlers and has the same
exposure.

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
`VectorCronjobAllRunsFailing` excludes `maple-graphql-indexer`, the five
on-demand jobs (`offchain-price-backfill`, `reference-capital-backfill`,
`morpho-vault-backfill`, `morpho-v2-bootstrap`, `block-republisher`), and
`core-model-runner`;
`VectorCronjobRunFailing` excludes only maple. Two manual steps:

1. Add the new **Deployment name** to the `deployment=~"..."` regex in the
   availability rule that matches its lifecycle — `VectorCronjobWorkerDown` for
   a scheduled cronjob, `VectorOnDemandWorkerDown` for anything that only runs
   when a human starts it (any `temporal.RunWorker` job, whether it takes
   parameters like `morpho-vault-backfill` or none like `morpho-v2-bootstrap`).
   (The kube-state-metrics label is the Deployment name, which may differ from
   `service_name` — e.g. `spark-anchorage-indexer`.) An on-demand worker must
   ALSO be added to the `service_name!=` exclusions in
   `VectorCronjobAllRunsFailing`, or its idle state pages.
2. Add a row to the table at the top of this runbook.

A **scheduled** cronjob that ticks too infrequently or retries too little for
`VectorCronjobAllRunsFailing`'s 1h window to span a likely recovery needs the
same `service_name!=` exclusion as an on-demand job — `core-model-runner`
(24h interval, `RetryPolicy.maximum_attempts=1`) is the first example; see the
alert's own comment in `alerts/vector-cronjobs.yaml` for the reasoning. Unlike
an on-demand job, though, its output *does* go stale, so the exclusion must be
paired with a per-job stall rule — `VectorCoreModelRunnerStale` is the
template: "no `status="success"` in (interval + max tick duration + slack)",
critical, with its own runbook section.
