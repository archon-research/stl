# Runbook — position-materializer (VEC-402)

The cronjob calls one per-projection materializer function (`materialize_morpho_market`,
`materialize_aave_lending`, ...) per configured entry, passing `p_build_id` and `p_run_id` by name. Each wrapper runs its projection's own
pre-flight checks, then the shared `materialize_position_projection` validates the view against the
`position_state` column contract, evaluates it once into a temp table, runs its checks, and appends the
observations it has not already stored.

Configuration: `POSITION_PROJECTIONS` (comma-separated `materialize_<projection>` function names —
explicit, never discovery), `MATERIALIZE_INTERVAL` (default `1h`), `MATERIALIZE_SCHEDULE_OFFSET` (`40m` in
both overlays, off the hour the validators and indexers share), `DATABASE_URL`.

Two properties make almost every incident low-risk:

- **The append is idempotent.** A rerun re-derives the same observations and writes nothing. Recovery
  from most failures is simply the next scheduled run; there is no repair step and no partial state.
- **Nothing is ever overwritten.** `position_state` has no update channel, so a bad run cannot corrupt
  stored history. The worst outcome is missing rows, not wrong ones.

The shared cronjob alerts in `vector-cronjobs.yaml` read `cronjob_runs_total`, which the Temporal
adapter records for the whole run: `VectorCronjobRunFailing` warns on a failing run under
`service_name="position-materializer"`, and `VectorCronjobWorkerDown` fires on a worker with no available
replica. `VectorCronjobAllRunsFailing` excludes this service: one failing view fails the whole hourly run,
so it would page critical while the other views wrote. `ViewFailing` and `ViewNotCompleting` name the view
instead. No alert reads run duration or container restarts, so a crash-looping pod may surface as
`VectorCronjobWorkerDown` only while it stays unavailable. The six alerts below read this service's own
`position_materializer_*` metrics. A run cut short by a rollout is recorded as
`status="canceled"` on both counters, so neither the shared failure alerts nor `ViewFailing` fire on it;
`ViewNotCompleting` fires when a view is cancelled with no completed run for 3 hours. When a run is cut
short, every view it did not reach is recorded as cancelled too, so a view starved by a slow one before it
is named.

The two gauges, `positions_refused` and `cache_rows`, carry only what the latest tick read. A projection
that did not complete in that tick, or a read that failed, has no series until a later tick reports it,
and a restarted pod has none until its first run ends.

The alerts label a projection two ways: `VectorPositionMaterializerViewFailing` carries the function
(`materializer="materialize_morpho_market"`), `VectorPositionMaterializerWithholdingPositions` the view
(`projection="public.position_morpho_market"`).

## Before the first run

The deployment ships at `replicas: 0`, and each environment is switched on separately through the
`replicas:` entry at the end of its overlay's `kustomization.yaml`. Switch staging on first and let it soak
before prod. The first run appends the whole history into `position_state`, which has no
update or delete channel, so a wrong bootstrap cannot be rolled back without a superuser.

Before bumping an environment to 1:

1. Every entry in `POSITION_PROJECTIONS` exists in the target database. The worker checks this at
   startup and exits naming the ones it cannot call; `materialize_morpho_market` and
   `materialize_morpho_vault` are prerequisites, not yet deployed: they ship with #624 and #626, and both
   must be merged and migrated first.
2. Time one projection by hand and watch its transaction. The call holds a snapshot and a transaction
   id, and with them the vacuum horizon for every table in the database, for its whole duration:

   ```sql
   -- session A
   BEGIN; SELECT materialize_morpho_vault(0); ROLLBACK;
   -- session B, while A runs
   SELECT pid, backend_xid, backend_xmin, now() - xact_start AS age
     FROM pg_stat_activity WHERE backend_xid IS NOT NULL;
   ```

   Run it while the deployment is at 0: until session A ends it holds the per-view advisory lock and
   blocks any other call on that view. The duration is how long autovacuum cannot clean rows deleted
   after the call started; decide whether that is acceptable before the schedule makes it hourly.
3. Confirm no schedule exists yet (`temporal schedule describe --schedule-id position-materializer`).
   A schedule keeps the activity timeouts it was created with, and a redeploy updates only its
   interval, so changing the timeouts later means deleting the schedule and letting the worker recreate it.
4. Run the bootstrap by hand, with the deployment still at 0, so it is not bound by the schedule's
   activity timeouts:
   - Pick an off-peak window and tell the channel first: the call is one long transaction and trips the
     long-running-transaction alert.
   - Use a direct connection, not the pooler.
   - Snapshot the counts first:
     `SELECT projection, count(*) FROM position_state GROUP BY projection;` and
     `SELECT count(*) FROM position_projection_run;`.
   - Call each wrapper in `POSITION_PROJECTIONS` in turn, for example `SELECT materialize_morpho_market(0);`.
     Rows written this way carry `build_id` 0 and a NULL `run_id`, so they read as pre-tracking in the
     query under "Checking what a run actually did".
   - While it runs, watch `pg_locks` for ungranted locks and the Tiger logs for `still waiting for ShareLock`.
   - Afterwards, read the newest `position_projection_run` row per projection and any
     `position_projection_refusal` rows it wrote, and resolve what they show before bumping replicas.

While a run is longer than `MATERIALIZE_INTERVAL`, the ticks that fall inside it are skipped rather than
queued; the bootstrap skips several. That is expected, not a stall.

The activity heartbeat carries a Temporal cancellation to the running query while the pod is alive. The
pool sets no `client_connection_check_interval`: `DATABASE_URL` is the pooler, so the server would be checking
pgbouncer's connection, not the pod's. A pod
that is killed outright leaves its query running behind the pooler until it finishes, holding its locks
and transaction id; check `pg_stat_activity` before starting another run by hand.

Every tick re-reads each view's whole history. On staging (2026-09-17) the two view reads alone took
19.4 s for `morpho_vault_position` (9.9M rows, ~1.7 GB of temp spill) and 8.4 s for
`morpho_market_position` (1.4M rows); each call also holds its transaction id for at least that long.
The cost grows with the source tables. If a tick's read passes about 10 minutes, raise
`MATERIALIZE_INTERVAL` or bound the read (VEC-566).

---

## VectorPositionMaterializerSilentlyEmpty

**What it means.** Runs are succeeding, but no observation has been appended for any view in 6 hours.
`position_state` has stopped tracking positions while everything looks healthy.
It needs the counter to have existed 6 hours back, so a new pod after a hand bootstrap, whose first runs
correctly append nothing, does not fire it.

**Why it is not paging.** The data is stale, not wrong. Nothing is corrupted and no manual repair is
needed — once the cause is fixed, the next run appends the backlog, because the projection covers the
whole history every time.

**Triage, in order.**

1. Confirm the runs really are succeeding and empty:
   ```sql
   SELECT projection, max(block_timestamp) AS newest_observation, max(created_at) AS last_write
     FROM position_state GROUP BY projection ORDER BY projection;
   ```
   `last_write` far behind now, with `newest_observation` also stale, means nothing is arriving.

2. Check the upstream source for each configured view. This is the usual cause — the materializer is
   only as fresh as what it projects from:
   ```sql
   SELECT max(block_number) FROM morpho_vault_position;   -- or morpho_market_position, prime_debt
   ```
   If the source is stale, the incident belongs to that indexer, not here. The generic cronjob alerts
   for that indexer should also be firing.

3. If the sources are current but nothing lands, run one projection by hand and read the count. A
   manual call stamps `build_id = 0` and a NULL run on every row it appends, and the table is
   append-only, so that provenance cannot be corrected afterwards:
   ```sql
   SELECT materialize_morpho_vault(0);
   ```
   A return of `0` with fresh sources means the view is filtering everything out — inspect the view's
   joins. A raise names the exact problem; see the failure table below.

4. Check the projection list is what you expect. An operator setting `POSITION_PROJECTIONS` to a
   single entry, or to a view that legitimately has no new rows, produces exactly this signal:
   ```bash
   kubectl -n vector get configmap position-materializer -o yaml | grep -A2 POSITION_PROJECTIONS
   ```

**Resolution.** Fix the upstream indexer or the view, then let the next scheduled run catch up. No
backfill command exists or is needed — the full projection *is* the backfill.

---

## VectorPositionMaterializerWithholdingPositions

**What it means.** A projection is withholding positions rather than failing. Its run succeeds, the
other positions land, and these sit at whatever was last stored, which every downstream reader treats
as current. `position_projection_run.positions_refused` is the per-run count the alert reads, taken from
each projection's run in the latest tick under the running pod's writer run. It fires when every reading
over 3 hours is above zero, readings exist 90 minutes to 3 hours back, and one arrived in the last 65
minutes, so a restart does not reset it and a level first seen minutes ago does not fire it and a
projection that has stopped completing stops firing it (`ViewFailing` or `ViewNotCompleting` takes over).

**Which positions.** The refusal table holds one row per refused observation for the life of the
refusal, and the two classes need different questions asked of them.

Withheld by a block-against-instant inversion, so nothing of that position landed:

```sql
SELECT r.projection, encode(r.position_id, 'hex') AS position_id,
       min(r.block_number) AS from_block, min(r.created_at) AS first_refused_at, min(r.detail) AS detail
  FROM position_projection_refusal r
 WHERE r.reason = 'block_time_inverts_height'
   AND NOT EXISTS (SELECT 1 FROM position_state p
                    WHERE p.position_id = r.position_id AND p.block_number >= r.block_number)
 GROUP BY 1, 2
 ORDER BY first_refused_at;
```

A correction the spine declined, where the stored row is kept and so is present by definition. The
inversion filter above would hide every one of these:

```sql
SELECT r.projection, encode(r.position_id, 'hex') AS position_id, r.reason,
       r.block_number, r.created_at, r.detail
  FROM position_projection_refusal r
 WHERE r.reason IN ('observation_drift', 'deal_type_drift')
 ORDER BY r.created_at DESC
 LIMIT 50;
```

An inversion means the source gave a higher block an earlier instant and needs fixing upstream; the
position leaves the first result by storing an observation at or beyond `from_block`, so a source
correction clears it with no intervention here. A drift means the view re-emitted a stored key with
a different value, which a real correction expresses by bumping `block_version` or
`processing_version` instead. Both classes count towards `positions_refused`.

## VectorPositionMaterializerViewFailing

**What it means.** One named projection returned an error. The runner logs it and moves on to the next
projection, so every other view in that run did write; only this one did not. The alert names the view so
you do not have to find it in logs. Check `position_projection_refusal` and `positions_refused` in
`position_projection_run` too: a run can succeed while withholding individual positions.

A configured wrapper the worker cannot call does not reach this alert: it exits at startup with
`configured materializers not callable`, the pod crash-loops and `VectorCronjobWorkerDown` fires after
10m. Fix `POSITION_PROJECTIONS`, or deploy the wrapper's migration.

**The ways a run fails, and what each one means.**

| error | cause | fix |
| --- | --- | --- |
| `violates the position_state column contract: X (is Y / MISSING)` | the view lost a column or changed its type | fix the view; the contract is the ten columns in the migration header |
| `double-emits a logical observation key` | the view produces two rows for one `(position, block, block_version, processing_version)` | dedupe the view; usually a join fanning out |
| `emits position_ids owned by another projection` | two views claim the same position — their `instrument_key` forms disagree, or the fan-out overlaps | decide which view owns it; do **not** work around it, this is the guard doing its job |
| `p_view (oid N) does not name an existing relation` | a wrapper's own view was dropped | restore the view |

**A warning rather than an error** — `re-emits stored observations with a changed block_timestamp` or
`changed quantity` — is not a failure. The stored row is kept and nothing is rewritten. It means the
view's pick for a logical key is unstable; event-time sources must dedupe each key to a stable value
(for example `MIN(synced_at)`). A genuine correction should arrive as a new `block_version` or
`processing_version` row from the source instead. It is safe to leave until the view is fixed, because
nothing wrong is ever stored.

**Resolution.** Fix the view, then let the next run proceed. Because the append is idempotent, the
recovered run writes exactly the observations the failed runs missed.

---

## VectorPositionMaterializerViewNotCompleting

**What it means.** One projection was cancelled at least once in the last 3 hours and completed no run in
that time. A cancelled run is recorded as `status="canceled"`, which `ViewFailing` and the shared failure
alerts ignore, so without this rule a view that never finishes would fire nothing while appending nothing.

**Triage, in order.**

1. Check whether rollouts landed on every tick: a deploy stream faster than one run cancels each run in turn.
   It clears once deploys pause for longer than a run.
2. Check the run duration in the pod logs (`projection materialized` carries `duration`) and the Temporal UI.
   A run longer than the activity's StartToClose, or one that stops heartbeating, is cancelled by Temporal.
3. If a single view's read has grown, time it by hand as in "Before the first run" and raise
   `MATERIALIZE_INTERVAL` or bound the read (VEC-566).

---

## VectorPositionMaterializerLevelReadFailing

**What it means.** The read behind a gauge failed on at least two ticks in 3 hours: `read="refused"`
(`position_projection_run`, for `WithholdingPositions`) or `read="cache_rows"` (`approximate_row_count` on
the caches, for `CacheTableGrowthHigh`). The projections themselves still ran and their rows are committed,
but the gauge is absent, so its alert cannot fire.

**Triage.** The pod logs carry the error (`reading withheld positions failed` or
`reading cache row estimates failed`). The usual causes are a changed grant on `position_projection_run`
for the worker's role, or a renamed or dropped cache table. As that role, this simplified read (the worker
also filters on its run and the tick) confirms the grant:

```sql
SELECT DISTINCT ON (projection) projection, positions_refused
  FROM position_projection_run ORDER BY projection, created_at DESC;
```

**Resolution.** Restore the grant or the relation. The next tick republishes the level.

---

## VectorPositionMaterializerCacheTableGrowthHigh

**What it means.** A trigger-fed cache off `position_state` — today `position_current` — has passed 50M
estimated rows. It is a plain table, and this is the tripwire `db/migrations/AGENTS.md` charges for that
choice: nothing else notices a plain table growing. **Nothing is broken.** It says the table has outgrown
the size at which staying plain was the right trade.

The alert fires when every reading over 6 hours is above 50M, so a pod restart does not reset it.

**Confirm it.** The alert reads `approximate_row_count`, an estimate from planner statistics that moves
with autovacuum rather than continuously:

```sql
SELECT c.relname,
       approximate_row_count(c.oid) AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public' AND c.relname = 'position_current';
```

If the estimate looks stale, `ANALYZE position_current;` refreshes it. An exact `count(*)` is a full scan
of the table you are already worried about — reach for it only if the estimate is not believable.

**`position_current` cannot be converted to a hypertable.** Its primary key is `(position_id)` alone, and
`create_hypertable` refuses a unique index that omits the partition column —
`cannot create a unique index without the column "block_timestamp" (used in partitioning)`. Adding the
timestamp to that key would change the grain from one row per position to one row per position per
block, which is `position_state`'s job, not this cache's. So partitioning is not the answer here: one row
per position means the **position count** itself has exploded. Check for a projection minting identities
it should not (`VectorPositionMaterializerWithholdingPositions` and the refusal table are the place to
start) and fix that rather than the storage. If the count is legitimate, raise the threshold deliberately
and say why in the rule's comment.

**A cache that can be partitioned** — one whose PK includes a time column — converts in place per
`db/migrations/AGENTS.md`, in a new migration, with the compression and tiering policies in that same
migration. Because these caches upsert in place rather than appending, settle first: the decompression cap
(`SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0` on the trigger function and the rebuild
procedure), `enable_tiered_reads` on the rebuild (it computes newest-per-key over the whole table), and a
tiering horizon beyond the reprocess window or none. Confirm the hot reads prune chunks before shipping.
The alert's metric survives the conversion — `approximate_row_count` counts chunk rows, where
`pg_class.reltuples` would drop to zero and silently take the tripwire with it.

**Resolution.** Fix the upstream cause and let the table shrink, or convert where the table's key allows it. If the threshold itself is wrong once there is a real production write rate to judge by,
change it in `alerts/vector-cronjobs.yaml` and say so in the PR. It is half the ~100M plain-table budget
used by `VectorUniswapV4AppendOnChangeGrowthHigh`; there is no production write rate yet to derive a
runway in time from.

---

## Checking what a run actually did

`build_id` records which build wrote each row and `run_id` which process start, so a run is traceable after the fact:

```sql
SELECT build_id, run_id, projection, count(*) AS observations, min(created_at), max(created_at)
  FROM position_state
 GROUP BY build_id, run_id, projection
 ORDER BY max(created_at) DESC
 LIMIT 10;
```

A `run_id` of `NULL` means the row predates run tracking; the service refuses to start without an
open run, so a deployed sweep never writes one. A `build_id` of `0` means the row was written without
a resolved build (the reserved pre-tracking value) — for this service that indicates the build registry lookup was skipped, which should not
happen in a deployed environment.
