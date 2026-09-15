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

One-time repair. `20260911_120000_add_block_timestamp_to_protocol_event.sql` adds the column
nullable; every row predating it is NULL. Runs out of band, **staging then prod** — an `UPDATE` of
this size over compressed chunks does not belong in the migrator's single transaction, the same
reason VEC-491 kept `block_meta` DDL-only.

## Before you start

- **Get the team's sign-off and record it in `stl-verify/db/migrations/AGENTS.md`.** Every step here
  is an in-place `UPDATE` on an ingest table, which that file forbids by default. This is a one-time
  repair of a column that never had a value, not a write channel any writer gains.
- **Connect as a non-superuser with write access, direct rather than through the pooler** (no
  2-minute statement timeout).
- **`SET timescaledb.enable_tiered_reads = 'on'` in every session.** It defaults off, so a plain
  session cannot see — or count — a tiered chunk, and Step 3 would report a false zero.
- ADR-0005's reader switch and the catalogue axis flip to `event` are VEC-735's. They must not run
  until Step 3 reports zero.

### There is a deadline

`protocol_event` tiers to object storage after 1 year (`policy_movechunk_to_s3`, which runs hourly),
and **a tiered chunk is read-only** — an `UPDATE` cannot reach its rows without an `untier_chunk`
first. The oldest chunk's `range_start` plus a year is the deadline; on prod on 2026-09-15 that was
2025-09-22, with nothing tiered yet:

```sql
SELECT count(*) AS chunks, min(range_start)::date AS oldest FROM timescaledb_information.chunks
WHERE hypertable_name = 'protocol_event';
SELECT count(*) AS tiered FROM timescaledb_osm.tiered_chunks WHERE hypertable_name = 'protocol_event';
```

## What the run costs

Measured on prod, 2026-09-15 — a snapshot, not an invariant. The `created_at` month is the Step 1
window; chunks and size are what that window touches.

| Window | Rows | Step 1 (whole-second) | Step 2 (sub-second) | Chunks | Compressed |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2025-09 .. 2026-01 | 8,484 | 8,484 | — | 131 | 16 MB |
| 2026-02 | 148,179 | 13,935 | 134,244 | 28 | 19 MB |
| 2026-03 | 827,217 | 38,805 | 788,412 | 31 | 108 MB |
| 2026-04 | 747,348 | 460,103 | 287,245 | 30 | 102 MB |
| 2026-05 | 1,542,999 | 1,542,999 | — | 31 | 152 MB |
| 2026-06 | 2,564,606 | 2,564,606 | — | 30 | 224 MB |
| 2026-07 | 3,400,513 | 3,400,513 | — | 31 | 284 MB |
| 2026-08 | 3,842,213 | 3,842,213 | — | 31 | 310 MB |
| 2026-09 (partial) | 3,391,028 | 3,391,028 | — | 15 | 868 MB |
| **Total** | **16,471,952** | **15,262,051** | **1,209,901** | **358** | **2.0 GB** |

Two thirds of Step 1 is the last four months. The 2025-09 .. 2026-01 windows are rounding error,
and are worth running first only because they are the ones the tiering clock reaches.

Step 2's residual, once `block_meta` covers it: 242,328 mainnet and 57,233 Avalanche blocks.
`block_meta` was still empty on prod when this was written.

**Disk headroom.** An `UPDATE` decompresses each chunk it touches and leaves it that way until
`policy_compression` (`compress_after` 2 days, job every 12h) catches up. Prod's uncompressed chunks
give the factor: 216 MB average against 12 MB compressed, ~956 B/row against ~97 B, so **~10x**:

```sql
SELECT c.is_compressed, count(*) AS chunks, pg_size_pretty(sum(d.total_bytes)) AS total,
       pg_size_pretty((sum(d.total_bytes) / count(*))::bigint) AS avg_chunk
FROM timescaledb_information.chunks c
JOIN chunks_detailed_size('protocol_event') d ON d.chunk_name = c.chunk_name
WHERE c.hypertable_name = 'protocol_event' AND c.range_start >= date_trunc('month', now())
GROUP BY 1;
```

One month at a time is what bounds it — the heaviest window (310 MB) peaks near 3 GB decompressed,
against ~20 GB for the whole table in one statement. Confirm that headroom, and let recompression
catch up between windows.

**Wall clock is deliberately not estimated.** Decompression is not the bottleneck (136k rows read
warm in 149 ms); the write path is, and it cannot be measured without writing, which is what the
sign-off gates. Run the five tiny windows first for the per-chunk overhead that dominates them, then
2026-05 (1.5M rows, 31 chunks) for the per-row rate. Those two size the rest, and the ticket wants
them recorded anyway.

## Where the two cohorts come from

Three eras wrote this table, and only the middle one lost the block time:

| Era | Writer | Where `created_at` came from | Result |
| --- | --- | --- | --- |
| up to 2026-02-18 | backfillers | the block header, passed explicitly | whole-second, dated |
| 2026-02-18 .. 2026-04-14 | live indexers | nothing — the INSERT omitted the column, so `DEFAULT NOW()` fired | the sub-second rows |
| 2026-04-14 onward | live indexers | the block header, an explicit constructor argument | whole-second, dated |

The header sat unused in the watcher's SNS envelope: the table had no event-time column, so the
indexers had nowhere to put it. VEC-80 (#191) ended that era by making it an argument to
`entity.NewProtocolEvent`, whose `Validate` rejects a zero value; live ingestion starting is what
opened it, since a backfiller replaying months-old blocks must pass a timestamp of its own.

So the gap is in the row, not in the world — every such row still carries
`(chain_id, block_number, block_version)`, and the header is recoverable from the S3 archive via
`block_meta`. It is also frozen: no writer can insert an undated row today.

The two cohorts are told apart without a join, because a block-header timestamp is whole-second and
`NOW()` is not. Re-measure before running anything:

```sql
SELECT count(*) FILTER (WHERE created_at =  date_trunc('second', created_at)) AS whole_sec,
       count(*) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS sub_sec,
       min(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS first_subsec,
       max(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS last_subsec
FROM protocol_event;
```

The sub-second rows are confined to `2026-02-18 14:23:15.93Z .. 2026-04-14 12:02:18.12Z`.

### The whole second is not a truncated ingest time

A writer storing `date_trunc('second', now())` would be whole-second and still hold ingest time. It
never did, and the delta against the chain is what settles it: a truncated ingest time keeps the
ingest lag, 2.0-3.5s on the rows that demonstrably hold one, so it lands at +2 or +3 and never +0.

| Cohort | Chain | Samples | Delta vs block header |
| --- | --- | ---: | --- |
| whole-second | mainnet, 2025-09 .. 2026-09 | 21 | +0 on every one |
| whole-second | Avalanche | 3 | +0 on every one |
| sub-second | Avalanche | 2 | +2 — the ingest lag |
| sub-second | mainnet, block 24659163 | 2 rows, one block | +2.4s and +2.6s; one block cannot have two header times |

The mainnet samples span every era, including before the writer change and the whole-second rows
interleaved with sub-second ones in 2026-02..04. The code agrees: before VEC-80 the INSERT did not
name `created_at`, so no truncating branch ever existed on that path.

Re-sample before Step 1 if the run is much later than this doc, comparing each against
`eth_getBlockByNumber` on any archive RPC. **A non-zero delta means the cohort test is wrong for
that era, and Step 1 must not run on it.**

```sql
SELECT chain_id, block_number, extract(epoch FROM created_at)::bigint AS stored_epoch
FROM protocol_event
WHERE chain_id = 1 AND created_at >= '<window start>' AND created_at < '<window end>'
  AND created_at = date_trunc('second', created_at)
ORDER BY created_at LIMIT 5;
```

## Step 1 — the whole-second rows (no external source needed)

`created_at` *is* the block timestamp for these, so the copy is local. Idempotent and restartable:
re-running skips what it already wrote.

One chunk-window at a time — a single statement over the whole table decompresses every chunk at
once. Window on `created_at`, the partition key, and as a **literal**, never a bind parameter, or
the planner builds paths for every chunk.

The whole-second predicate carries the claim; the date window is only batching. A `NOW()` landing on
an exact second passes it — ~1e-6 of the sub-second cohort, under one row table-wide — and Step 3
counts only NULLs, so such a row is never surfaced again.

```sql
UPDATE protocol_event
SET block_timestamp = created_at
WHERE created_at >= '2025-09-01' AND created_at < '2025-10-01'   -- advance one month per run
  AND created_at = date_trunc('second', created_at)
  AND block_timestamp IS NULL;
```

Record wall-clock and chunk count per window; that is the cost the ticket asks for.

## Step 2 — the sub-second rows (needs `block_meta`)

These hold ingest time, so their event time comes from
`block_meta (chain_id, block_number, block_version) -> block_timestamp` (VEC-491). `block_states`
cannot serve: it is a rolling ~1-month reorg window and holds none of these blocks.

`block_meta` is filled by `block-meta-loader`, an on-demand Temporal worker, one deployment per
chain, started by hand from the Temporal UI (`--type BlockMetaLoad`). A run per chain this step
covers — mainnet and Avalanche — is a prerequisite, not part of this step.

Nothing hands it a block list: it enumerates what a chain references and `block_meta` lacks, taking
the tables it scans from `schema_master.json`'s `block_meta` fills. **`protocol_event` keeps its fill
entry for that reason**, native column notwithstanding — drop it and the loader silently stops
enumerating this table, leaving every value here NULL. The entry goes when Step 3 reports zero, in
the PR that retires this file.

Size the residual first (counts in [What the run costs](#what-the-run-costs)):

```sql
SELECT chain_id, count(*) AS distinct_blocks
FROM (SELECT DISTINCT chain_id, block_number, block_version FROM protocol_event
      WHERE created_at >= '2026-02-18' AND created_at < '2026-04-15'
        AND created_at <> date_trunc('second', created_at)) x
GROUP BY 1 ORDER BY 1;
```

Then date the rows one window at a time. `block_meta` is append-only and versioned on
`processing_version`, so a corrected header time is a second row for the same block: take the
highest, or a known-bad time wins.

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

A row surviving every source above is undatable, not pending: record which blocks in the ticket and
leave it NULL, since readers filter `IS NOT NULL`.
