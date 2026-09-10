# Runbook — position-materializer (VEC-402)

The cronjob calls one per-projection materializer function (`materialize_morpho_market(build_id)`,
`materialize_aave_lending(build_id)`, ...) per configured entry. Each wrapper runs its projection's own
pre-flight checks, then the shared `materialize_position_projection` validates the view against the
`position_state` column contract, evaluates it once into a temp table, runs its checks, and appends the
observations it has not already stored.

Configuration: `POSITION_PROJECTIONS` (comma-separated `materialize_<projection>` function names —
explicit, never discovery), `MATERIALIZE_INTERVAL` (default `1h`), `DATABASE_URL`.

Two properties make almost every incident low-risk:

- **The append is idempotent.** A rerun re-derives the same observations and writes nothing. Recovery
  from most failures is simply the next scheduled run; there is no repair step and no partial state.
- **Nothing is ever overwritten.** `position_state` has no update channel, so a bad run cannot corrupt
  stored history. The worst outcome is missing rows, not wrong ones.

Generic run failures, restarts and duration are covered by the shared cronjob alerts in
`vector-cronjobs.yaml` under `service_name="position-materializer"`; only the two alerts below are
specific to this service.

---

## VectorPositionMaterializerSilentlyEmpty

**What it means.** Runs are succeeding, but no observation has been appended for any view in 6 hours.
`position_state` has stopped tracking positions while everything looks healthy.

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

3. If the sources are current but nothing lands, run one projection by hand and read the count:
   ```sql
   SELECT materialize_morpho_vault(0);
   ```
   A return of `0` with fresh sources means the view is filtering everything out — inspect the view's
   joins. A raise names the exact problem; see the failure table below.

4. Check the projection list is what you expect. An operator setting `POSITION_PROJECTIONS` to a
   single entry, or to a view that legitimately has no new rows, produces exactly this signal:
   ```bash
   kubectl -n vector get cronjob position-materializer -o yaml | grep -A2 POSITION_PROJECTIONS
   ```

**Resolution.** Fix the upstream indexer or the view, then let the next scheduled run catch up. No
backfill command exists or is needed — the full projection *is* the backfill.

---

## VectorPositionMaterializerWithholdingPositions

**What it means.** A projection is withholding positions rather than failing. Its run succeeds, the
other positions land, and these sit at whatever was last stored, which every downstream reader treats
as current. `position_projection_run.positions_refused` is the per-run count the alert reads.

**Which positions.** The refusal table holds one row per refused observation for the life of the
refusal, so filter it against what is stored rather than reading it alone:

```sql
SELECT r.projection, encode(r.position_id, 'hex') AS position_id, r.reason,
       min(r.block_number) AS from_block, min(r.created_at) AS first_refused_at, min(r.detail) AS detail
  FROM position_projection_refusal r
 WHERE NOT EXISTS (SELECT 1 FROM position_state p
                    WHERE p.position_id = r.position_id AND p.block_number >= r.block_number)
 GROUP BY 1, 2, 3
 ORDER BY first_refused_at;
```

A position leaves that result by storing an observation at or beyond `from_block`, so a source
correction clears it with no intervention here. `block_time_inverts_height` means the source gave a
higher block an earlier instant and needs fixing upstream; the drift reasons mean a stored key was
re-emitted with a different value, which a real correction expresses by bumping a version instead.

## VectorPositionMaterializerViewFailing

**What it means.** One named projection returned an error. The runner logs it and moves on to the next
projection, so every other view in that run did write; only this one did not. The alert names the view so
you do not have to find it in logs. Check `position_projection_refusal` and `positions_refused` in
`position_projection_run` too: a run can succeed while withholding individual positions.

**The four ways a run fails, and what each one means.**

| error | cause | fix |
| --- | --- | --- |
| `violates the position_state column contract: X (is Y / MISSING)` | the view lost a column or changed its type | fix the view; the contract is the ten columns in the migration header |
| `double-emits a logical observation key` | the view produces two rows for one `(position, block, block_version, processing_version)` | dedupe the view; usually a join fanning out |
| `emits position_ids owned by another projection` | two views claim the same position — their `instrument_key` forms disagree, or the fan-out overlaps | decide which view owns it; do **not** work around it, this is the guard doing its job |
| `p_view (oid N) does not name an existing relation` | a configured view was dropped, or the list names something that is not a relation | fix `POSITION_PROJECTIONS`, or restore the view |

**A warning rather than an error** — `re-emits stored observations with a changed block_timestamp` or
`changed quantity` — is not a failure. The stored row is kept and nothing is rewritten. It means the
view's pick for a logical key is unstable; event-time sources must dedupe each key to a stable value
(for example `MIN(synced_at)`). A genuine correction should arrive as a new `block_version` or
`processing_version` row from the source instead. It is safe to leave until the view is fixed, because
nothing wrong is ever stored.

**Resolution.** Fix the view, then let the next run proceed. Because the append is idempotent, the
recovered run writes exactly the observations the failed runs missed.

---

## Checking what a run actually did

`build_id` records which build wrote each row, so a run is traceable after the fact:

```sql
SELECT build_id, projection, count(*) AS observations, min(created_at), max(created_at)
  FROM position_state
 GROUP BY build_id, projection
 ORDER BY max(created_at) DESC
 LIMIT 10;
```

A `build_id` of `0` means the row was written without a resolved build (the reserved pre-tracking
value) — for this service that indicates the build registry lookup was skipped, which should not
happen in a deployed environment.
