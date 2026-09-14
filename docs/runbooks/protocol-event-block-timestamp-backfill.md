# protocol_event.block_timestamp backfill (VEC-711)

`protocol_event.block_timestamp` is added by
`20260911_120000_add_block_timestamp_to_protocol_event.sql` as a nullable column. Population runs
**out of band on staging, then prod** — a 14M-row `UPDATE` over compressed chunks does not belong in
the migrator's single transaction, the same reason VEC-491 kept `block_meta` DDL-only.

Every step below is an in-place `UPDATE` on an ingest table, which `stl-verify/db/migrations/AGENTS.md`
requires the team to sanction before it runs. It is a one-time operator repair of a column that
never had a value, not a write channel any writer gains: the ingest path only ever INSERTs, and
`block_timestamp` is never rewritten once set. Get that nod, and record it in that file, before
Step 1.

Run as a non-superuser role with write access, against the database directly rather than the pooler
(no 2-minute statement timeout). ADR-0005's reader switch and the catalogue axis flip to `event` are
VEC-735's, and must not run until Step 3 reports zero.

`protocol_event` carries a 1-year `add_tiering_policy` and `timescaledb.enable_tiered_reads`
defaults off, so a plain session cannot see — and Step 3 cannot count — a tiered chunk. Set it on
for every step below, or Step 3 reports zero while tiered history stays undated and VEC-735 flips
the axis on a series that is still part observation-time:

```sql
SET timescaledb.enable_tiered_reads = 'on';
```

## Where the two cohorts come from

Every indexer sets `created_at` to the block-header timestamp, and has since 2026-04-14 — the
watcher's SNS envelope carries the block time and all four writers (`aavelike_position_tracker`,
`morpho_indexer`, `dexconsumer`, and anything else reaching `EventRepository`) pass it through. Rows
written before that date were left to the column's `DEFAULT NOW()` and hold ingest time instead.

The two are told apart without a join: a block-header timestamp is whole-second, `NOW()` is not.
Ingest-time rows are the ones where `created_at <> date_trunc('second', created_at)`. Confirm the
boundary before relying on it, and use the date it reports (not the one above) in Step 1:

```sql
SELECT max(created_at) FROM protocol_event
WHERE created_at <> date_trunc('second', created_at);
```

## Step 1 — the rows created_at already dates (no external source needed)

For every row a writer set explicitly, `created_at` *is* the block timestamp, so the copy is local.
Idempotent, and restartable: re-running it skips what it already wrote.

Run it one chunk-window at a time — a single statement over the whole table decompresses every
chunk at once. Window on `created_at`, the partition key, and as a **literal**, never a bind
parameter, or the planner builds paths for every chunk.

The whole-second predicate is not redundant with the date window: it is what stops an operator who
mis-set the boundary from stamping ingest time as event time. Step 3 counts only NULLs, so a row
dated wrongly here is never surfaced again.

```sql
UPDATE protocol_event
SET block_timestamp = created_at
WHERE created_at >= '2026-04-15' AND created_at < '2026-05-01'   -- advance one month per run
  AND created_at = date_trunc('second', created_at)
  AND block_timestamp IS NULL;
```

Record wall-clock and chunk count per window; that is the cost the ticket asks for.

## Step 2 — the pre-boundary rows (needs `block_meta`)

These carry ingest time in `created_at`, so their event time has to come from
`block_meta (chain_id, block_number, block_version) -> block_timestamp` (VEC-491). `block_meta` is
loaded out of band from the block headers in the S3 raw-block archive; covering these blocks is that
loader's work and a prerequisite of this step, not part of it. `block_states` alone cannot serve: it
is a rolling ~1-month reorg window and holds none of these blocks.

Size the residual first — it is the distinct-block count the `block_meta` load has to cover:

```sql
SELECT chain_id, count(*) AS distinct_blocks
FROM (SELECT DISTINCT chain_id, block_number, block_version FROM protocol_event
      WHERE created_at < '2026-04-15'
        AND created_at <> date_trunc('second', created_at)) x
GROUP BY 1 ORDER BY 1;
```

Once `block_meta` covers them, date the rows from it, again one window at a time. `block_meta` is
append-only and versioned on `processing_version`, so a corrected header time is a second row for
the same block: take the highest one, or a known-bad time wins.

```sql
UPDATE protocol_event pe
SET block_timestamp = bm.block_timestamp
FROM (SELECT DISTINCT ON (chain_id, block_number, block_version)
             chain_id, block_number, block_version, block_timestamp
      FROM block_meta
      ORDER BY chain_id, block_number, block_version, processing_version DESC) bm
WHERE bm.chain_id = pe.chain_id AND bm.block_number = pe.block_number
  AND bm.block_version = pe.block_version
  AND pe.created_at >= '2026-02-01' AND pe.created_at < '2026-03-01'   -- advance one month per run
  AND pe.block_timestamp IS NULL;
```

## Step 3 — verify

Zero here is VEC-711's "every row carries `block_timestamp`", and VEC-735's go-ahead:

```sql
SELECT chain_id, count(*) AS undated
FROM protocol_event WHERE block_timestamp IS NULL
GROUP BY 1 ORDER BY 1;
```

Any row that survives every source above is undatable, not pending: record which blocks in the
ticket and leave it NULL, since readers filter `IS NOT NULL`.
