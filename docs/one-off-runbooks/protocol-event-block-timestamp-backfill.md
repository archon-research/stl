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
`range_start` plus a year is the deadline. On prod on 2026-09-15 the oldest chunk was 2025-09-22
with nothing tiered yet, and `policy_movechunk_to_s3` runs hourly, so the first chunk tiers within
an hour of becoming eligible:

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

## What the run costs

Everything below was measured on prod on 2026-09-15 and is a snapshot, not an invariant. The
`created_at` month is the Step 1 window; chunks and compressed size are what that window touches.

| Window | Rows | Step 1 (whole-second) | Step 2 (sub-second) | Chunks | Compressed |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2025-09 | 162 | 162 | — | 8 | 848 kB |
| 2025-10 | 283 | 283 | — | 31 | 3.1 MB |
| 2025-11 | 303 | 303 | — | 30 | 3.0 MB |
| 2025-12 | 729 | 729 | — | 31 | 3.5 MB |
| 2026-01 | 7,007 | 7,007 | — | 31 | 5.2 MB |
| 2026-02 | 148,179 | 13,935 | 134,244 | 28 | 19 MB |
| 2026-03 | 827,217 | 38,805 | 788,412 | 31 | 108 MB |
| 2026-04 | 747,348 | 460,103 | 287,245 | 30 | 102 MB |
| 2026-05 | 1,542,999 | 1,542,999 | — | 31 | 152 MB |
| 2026-06 | 2,564,606 | 2,564,606 | — | 30 | 224 MB |
| 2026-07 | 3,400,513 | 3,400,513 | — | 31 | 284 MB |
| 2026-08 | 3,842,213 | 3,842,213 | — | 31 | 310 MB |
| 2026-09 (partial) | 3,391,028 | 3,391,028 | — | 15 | 868 MB |
| **Total** | **16,471,952** | **15,262,051** | **1,209,901** | **358** | **2.0 GB** |

Two thirds of Step 1 is the last four months. The first five windows are rounding error and worth
running first only because they are the ones the tiering clock reaches.

Step 2's residual, once `block_meta` covers it: 242,328 mainnet and 57,233 Avalanche blocks.

### Disk headroom

An `UPDATE` decompresses each chunk it touches and leaves it that way until `policy_compression`
(`compress_after` 2 days, job runs every 12h) catches up. Prod's current uncompressed chunks give
the expansion factor directly — the three newest chunks average 216 MB against 12 MB for the
compressed ones, and per row that is ~956 B against ~97 B, so call it **10x**:

```sql
SELECT c.is_compressed, count(*) AS chunks,
       pg_size_pretty(sum(d.total_bytes)) AS total,
       pg_size_pretty((sum(d.total_bytes) / count(*))::bigint) AS avg_chunk
FROM timescaledb_information.chunks c
JOIN chunks_detailed_size('protocol_event') d ON d.chunk_name = c.chunk_name
WHERE c.hypertable_name = 'protocol_event' AND c.range_start >= date_trunc('month', now())
GROUP BY 1;
```

One month at a time is what keeps that bounded: the heaviest window (2026-08, 310 MB) peaks around
3 GB decompressed, against ~20 GB if the whole table were done in one statement. Confirm the
instance has that headroom for the largest window before starting, and let recompression catch up
between windows rather than queueing every month back to back.

### Wall clock

Not estimated here, deliberately. Decompression is not the bottleneck — a warm read of 136k rows
including `event_data` measured 149 ms — and the write path that is cannot be measured without
writing, which is what the sign-off gates. Any number produced before the first window would be a
guess dressed as a figure.

Run the smallest windows first (2025-09 through 2026-01, ~8.5k rows over 131 chunks): they
establish the per-chunk overhead, which is what dominates them, while risking almost nothing. Then
one mid-size window (2026-05, 1.5M rows over 31 chunks) gives the per-row rate. Those two numbers
size the rest, and the ticket asks for them recorded either way.

## Where the two cohorts come from

Three eras wrote this table, and only the middle one lost the block time:

| Era | Writer | Where `created_at` came from | Result |
| --- | --- | --- | --- |
| up to 2026-02-18 | backfillers | the block header, passed explicitly | whole-second, dated |
| 2026-02-18 .. 2026-04-14 | live indexers | nothing — the INSERT omitted the column, so `DEFAULT NOW()` fired | the sub-second rows |
| 2026-04-14 onward | live indexers | the block header, an explicit constructor argument | whole-second, dated |

The table had no event-time column, and the live INSERT did not name `created_at`, so Postgres
filled it at insert. The header was in the watcher's SNS envelope the whole time — the indexers had
nowhere to put it and no reason to pass it. VEC-80 (#191) closed that by making it an argument to
`entity.NewProtocolEvent`, whose `Validate` rejects a zero value, which is why the era ends there.
It begins where live ingestion begins: a backfiller replaying months-old blocks has to pass a
timestamp, since `now()` would be visibly absurd for the row it is writing.

So the gap is in the row, not in the world. Every one of those rows still carries
`(chain_id, block_number, block_version)`, and the header is recoverable from the S3 raw-block
archive — which is what `block_meta` is for (VEC-491) and what Step 2 joins against. The gap is also
frozen: no writer can insert an undated row today, so the cohort cannot grow, and it measures the
same on staging and prod.

The two are told apart without a join: a block-header timestamp is whole-second, `NOW()` is not.
Re-measure the split before running anything; the sizes it reported on prod are in
[What the run costs](#what-the-run-costs):

```sql
SELECT count(*) FILTER (WHERE created_at =  date_trunc('second', created_at)) AS whole_sec,
       count(*) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS sub_sec,
       min(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS first_subsec,
       max(created_at) FILTER (WHERE created_at <> date_trunc('second', created_at)) AS last_subsec
FROM protocol_event;
```

The sub-second rows are confined to `2026-02-18 14:23:15.93Z .. 2026-04-14 12:02:18.12Z`. Rows
older than that window are whole-second too: they were written by backfillers that always supplied
block time.

### The whole second is not a truncated ingest time

The obvious objection to the test is that a writer could have stored `date_trunc('second', now())`,
which is whole-second and still ingest time. It did not, and the check that settles it is the delta
against the chain: a truncated ingest time carries the ingest lag, which is 2.0-3.5s on the rows
that demonstrably hold one, so it can only land at +2 or +3, never at +0.

21 whole-second mainnet rows sampled across every era — including before the writer change, and the
whole-second rows interleaved with sub-second ones in 2026-02..04 — matched their block header
exactly, delta +0 in all 21. Three Avalanche whole-second rows did too. The control is the other
cohort: two Avalanche sub-second rows came in at +2, which is the signature a truncating writer
would have left everywhere.

| Cohort | Chain | Samples | Delta vs block header |
| --- | --- | ---: | --- |
| whole-second | mainnet, 2025-09 .. 2026-09 | 21 | +0 on every one |
| whole-second | Avalanche | 3 | +0 on every one |
| sub-second | Avalanche | 2 | +2 — the ingest lag |
| sub-second | mainnet, block 24659163 | 2 rows, one block | +2.4s and +2.6s; one block cannot have two header times |

The code says the same: before VEC-80 the INSERT did not name `created_at` at all, so that era's
live rows fell to `DEFAULT NOW()` — which is why they are sub-second. No truncating branch ever
existed on that path, and the whole-second rows from that era came from backfillers passing their
own value.

Re-sample before Step 1 if the run is much later than this doc, with any archive RPC:

```sql
SELECT chain_id, block_number, extract(epoch FROM created_at)::bigint AS stored_epoch
FROM protocol_event
WHERE chain_id = 1 AND created_at >= '<window start>' AND created_at < '<window end>'
  AND created_at = date_trunc('second', created_at)
ORDER BY created_at LIMIT 5;
```

Compare each against `eth_getBlockByNumber`. A non-zero delta means the cohort test is wrong for
that era and Step 1 must not run on it.

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

Size the residual first — it is the distinct-block count the `block_meta` load has to cover
(counted in [What the run costs](#what-the-run-costs); `block_meta` was still empty on prod when
this was written):

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
