---
title: Backfilling protocol_event.block_timestamp - Operator Guide
audience: [developers, operators, ai-agents]
repo: stl
applies_to: stl-verify
ticket: VEC-711
related_docs:
  - docs/adr/0005-time-series-api-surface.md   # why the series needs an event-time column
retire: delete this file in the PR that records the completed run
---

# Backfilling `protocol_event.block_timestamp`

One-time repair. `protocol_event.block_timestamp` is added by
`20260911_120000_add_block_timestamp_to_protocol_event.sql` as a nullable column; every row that
predates the migration is NULL. Population runs **out of band on staging, then prod** — an
`UPDATE` of that size over compressed chunks does not belong in the migrator's single transaction,
the same reason VEC-491 kept `block_meta` DDL-only.

Every step below is an in-place `UPDATE` on an ingest table, which `stl-verify/db/migrations/AGENTS.md`
requires the team to sanction before it runs. It is a one-time operator repair of a column that
never had a value, not a write channel any writer gains: the ingest path only ever INSERTs, and
`block_timestamp` is never rewritten once set. Get that nod, and record it in that file, before
Step 1.

Run as a non-superuser role with write access, against the database directly rather than the pooler
(no 2-minute statement timeout). ADR-0005's reader switch and the catalogue axis flip to `event` are
VEC-735's, and must not run until Step 3 reports zero.

## Run it before the oldest chunks tier

`protocol_event` tiers to object storage after 1 year (`policy_movechunk_to_s3`), and a tiered
chunk is read-only: an `UPDATE` cannot reach its rows, so they stay undated until someone calls
`untier_chunk` on each one first. Check the headroom before planning the run — the oldest chunk's
`range_start` plus a year is the deadline, and at the time of writing the oldest was 2025-09-22
with nothing tiered yet:

```sql
SELECT count(*) AS chunks, min(range_start)::date AS oldest FROM timescaledb_information.chunks
WHERE hypertable_name = 'protocol_event';
SELECT count(*) AS tiered FROM timescaledb_osm.tiered_chunks WHERE hypertable_name = 'protocol_event';
```

`timescaledb.enable_tiered_reads` also defaults off, so a plain session cannot see — and Step 3
cannot count — a tiered chunk. Set it on for every step below, or Step 3 reports zero while tiered
history stays undated and VEC-735 flips the axis on a series that is still part observation-time:

```sql
SET timescaledb.enable_tiered_reads = 'on';
```

## Cost and headroom

Each window decompresses the chunks it touches, rewrites every row in them, and leaves them
uncompressed until `policy_compression` (`compress_after` 2 days) catches up. Two consequences to
size before starting: the table needs disk headroom for its decompressed form (2.1 GB compressed
across 358 chunks at the time of writing, several times that uncompressed), and the recompression
that follows is its own background load.

Decompression itself is not the bottleneck — a warm read of 135k rows including `event_data`
measured 285 ms. The write path is, and it cannot be measured without writing, so treat the first
window as the calibration run and extrapolate from what it reports rather than from a guess.

## Where the two cohorts come from

Every indexer passes the block-header timestamp into `entity.NewProtocolEvent`, which rejects a zero
value, so `created_at` on a row written today *is* event time. That has held since VEC-80 (#191,
2026-04-14) made the field an explicit constructor argument; before it, `created_at` was left to its
`DEFAULT NOW()` and holds ingest time.

The two are told apart without a join: a block-header timestamp is whole-second, `NOW()` is not.
Re-measure both cohorts before running anything — the numbers below are the prod measurement taken
when this doc was written, not an invariant:

```sql
SELECT count(*) FILTER (WHERE created_at =  date_trunc('second', created_at)) AS whole_sec,
       count(*) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS sub_sec,
       min(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS first_subsec,
       max(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS last_subsec
FROM protocol_event;
```

At the time of writing that reported 15.0M whole-second rows against 1.2M sub-second ones, the
latter confined to `2026-02-18 14:23:15.93Z .. 2026-04-14 12:02:18.12Z`. Rows older than that
window are whole-second too: they were written by backfillers that always supplied block time.

Spot-checked against mainnet before relying on the split:

| Row | Stored `created_at` | Block-header time | Verdict |
| --- | --- | --- | --- |
| block 24558876 (whole-second) | `2026-03-01 00:01:47Z` | `2026-03-01 00:01:47Z` | event time |
| block 24659163 (sub-second) | `2026-03-14 23:59:59.xx`+2.4s and +2.6s, two rows | `2026-03-14 23:59:59Z` | ingest time — one block cannot have two header times |

## Step 1 — the whole-second rows (no external source needed)

For every row a writer set explicitly, `created_at` *is* the block timestamp, so the copy is local.
Idempotent, and restartable: re-running it skips what it already wrote.

Run it one chunk-window at a time — a single statement over the whole table decompresses every
chunk at once. Window on `created_at`, the partition key, and as a **literal**, never a bind
parameter, or the planner builds paths for every chunk.

The whole-second predicate is what carries the claim; the date window is only the chunk batching.
A `NOW()` value that lands on an exact second passes it — at ~1e-6 of the sub-second cohort, under
one row table-wide — so a stray ingest-time row surviving here is possible and will not be surfaced
again: Step 3 counts only NULLs.

```sql
UPDATE protocol_event
SET block_timestamp = created_at
WHERE created_at >= '2025-09-01' AND created_at < '2025-10-01'   -- advance one month per run
  AND created_at = date_trunc('second', created_at)
  AND block_timestamp IS NULL;
```

Record wall-clock and chunk count per window; that is the cost the ticket asks for.

## Step 2 — the sub-second rows (needs `block_meta`)

These carry ingest time in `created_at`, so their event time has to come from
`block_meta (chain_id, block_number, block_version) -> block_timestamp` (VEC-491). `block_states`
alone cannot serve: it is a rolling ~1-month reorg window and holds none of these blocks.

`block_meta` is filled by `block-meta-loader`, an on-demand Temporal worker, one deployment per
chain, started by hand from the Temporal UI (`--type BlockMetaLoad`). It needs a run per chain this
step covers — mainnet and Avalanche — and that is a prerequisite of this step, not part of it.

Nothing has to hand it a block list: it enumerates what a chain references and `block_meta` lacks,
deriving the tables it scans from `schema_master.json`'s `block_meta` fills. `protocol_event` keeps
its fill entry for exactly that reason, even though the column is native now — drop the entry and
the loader stops enumerating this table's blocks, silently, and every value here resolves NULL. The
entry goes when Step 3 reports zero, in the PR that retires this file.

Size the residual first — it is the distinct-block count the `block_meta` load has to cover, and at
the time of writing it was ~242k mainnet and ~57k Avalanche blocks:

```sql
SELECT chain_id, count(*) AS distinct_blocks
FROM (SELECT DISTINCT chain_id, block_number, block_version FROM protocol_event
      WHERE created_at >= '2026-02-18' AND created_at < '2026-04-15'
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
  AND pe.created_at >= '2026-02-18' AND pe.created_at < '2026-03-01'   -- advance one month per run
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
