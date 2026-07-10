# Vector — transformation-layer worker runbook

Owner: vector team · Source rules: [alerts/vector-cronjobs.yaml](../../alerts/vector-cronjobs.yaml)

The `transform-worker` cronjob (Temporal, `vector` namespace, default interval
`TRANSFORM_INTERVAL`=10m) materialises the `transformed.*` layer. Each raw table
has an `AFTER INSERT` trigger that appends the new row's primary key to a change
queue `transformed._pending_<source>`; each tick the worker calls
`transformed._run_<source>()`, which drains that queue in bounded batches and
upserts the canonical rows.

Generic run-failure and worker-liveness are covered by
[vector-cronjobs.md](vector-cronjobs.md) (`service_name="transform-worker"`).
These three alerts cover the silent cases that path can't: the worker is up and
its runs succeed, but a source's queue stops draining, or a row reaches neither
the transformed table nor its queue.

Signal: `transform_queue_oldest_age_seconds{table="<source>"}` — seconds since
the oldest un-drained queue row was enqueued (companion depth gauge
`transform_queue_pending`). Both are recorded each tick from
`transformed._queue_status`.

General triage:

```bash
kubectl -n vector logs deploy/transform-worker --tail=200          # errors / drain-cap warnings
```

```sql
-- backlog and age per source (this is exactly what the gauges read)
SELECT * FROM transformed._queue_status ORDER BY oldest_enqueued_at NULLS LAST;
-- is a run function wedged / holding its advisory lock?
SELECT pid, state, wait_event_type, query, now()-query_start AS running_for
FROM pg_stat_activity WHERE query LIKE '%transformed._run_%' ORDER BY query_start;
```

---

## VectorTransformQueueLagging

**Severity:** warning · **For:** 15m

### What it means
The oldest row in one source's change queue has been waiting >30m — the worker
is running but that source is not draining each tick. Usually a transient
backlog (a large backfill burst, or the worker briefly behind), or the drain
hitting its per-tick iteration cap.

### What to do
1. `SELECT * FROM transformed._queue_status` — confirm which `table` and whether
   `pending` is climbing or draining across a few minutes.
2. Check the worker logs for `transform drain hit iteration cap` (a big backlog
   being worked off over several ticks — expected to recover on its own) or
   per-table run errors.
3. If `pending` is falling, no action — it will clear. If it is climbing, treat
   as a pre-page for VectorTransformQueueStalled and continue below.

## VectorTransformQueueStalled

**Severity:** critical · **For:** 30m

### What it means
A source's queue has an un-drained row older than 2h and is not recovering:
`transformed.<table>` is materially behind raw. The worker is up (otherwise
`VectorCronjobWorkerDown` would fire), so the drain itself is failing or wedged.

### What to do
1. Worker error logs: `kubectl -n vector logs deploy/transform-worker --tail=300 | grep -i error`.
   A repeating `running transform "<table>"` error points at that source's
   `_run` function (e.g. a bad row, a lock timeout, or a schema drift).
2. Check for a wedged run holding the advisory lock (query above). A long-running
   `transformed._run_<table>()` blocks its own next tick; kill the stuck backend
   with `SELECT pg_terminate_backend(<pid>)` if it is hung, then let the next
   tick retry.
3. If the queue is pathologically large (e.g. after a long outage or a bulk
   backfill), the drain works it off in `drainBatch` (10k) chunks over successive
   ticks; confirm `pending` is decreasing tick-over-tick. If it is not, the
   inserts are outpacing the drain — scale the interval down (`TRANSFORM_INTERVAL`)
   or investigate the source's write rate.
4. If a specific row is poisoning the batch, inspect it via
   `transformed._pending_<table>` joined back to `public.<table>` on the queued
   PK; a genuinely bad raw row should be fixed at source (raw is append-only).

## VectorTransformParityDrift

**Severity:** warning · **For:** 30m

### What it means
`drift = raw - transformed - pending` for a source is nonzero. This is the
backstop for the failure the stall alert can't see: a raw row that reached
neither the transformed table nor its queue. In a consistent snapshot after
bootstrap the invariant is `raw = transformed + pending`, so drift is 0. Causes:

- The enqueue trigger did not fire for some raw writes (a trigger got dropped, or
  a new raw write path bypasses it) — positive drift, growing.
- The initial bootstrap never covered some history — positive drift, static. This
  is also the expected state *during* bootstrap, before it finishes.
- Negative drift: the transformed table has rows with no raw counterpart (orphans
  — a raw delete, or a bug). Should never happen.

Not detected by this check: an in-place `UPDATE` on a raw row changes neither the
row count nor the insert/delete activity the ledger keys on, so drift stays 0
while the transformed copy silently goes stale. This is a known limitation,
acceptable only because the raw tables are verified INSERT-only (corrections and
reorgs arrive as new rows with a new PK). A raw `DELETE` is different — it drops
the raw count, so it surfaces as negative drift above.

### What to do
1. `SELECT * FROM transformed._parity_status WHERE source = '<table>'` — read
   raw/transformed/pending/drift. Note the sign and whether it is static or
   growing.
2. If this is a fresh rollout and bootstrap is still running, this is expected —
   wait for bootstrap to finish and confirm drift returns to 0.
3. Growing positive drift: check the enqueue trigger exists and is enabled —
   `SELECT tgname, tgenabled FROM pg_trigger WHERE tgrelid = 'public.<table>'::regclass AND NOT tgisinternal`
   (expect `_transform_enqueue`, enabled). If missing/disabled, re-apply the
   migration or re-enable, then re-bootstrap the gap window.
4. Static positive drift after bootstrap: re-run `transform-bootstrap` for the
   affected source (`-source <table>`); the guarded upsert makes it idempotent.
5. Negative drift: investigate raw deletes / an errant transform; do not ignore.

Enabling: this rule is warning because it is also nonzero during the initial
bootstrap. Once the layer is bootstrapped and drift reads 0, it can be promoted
to critical.
