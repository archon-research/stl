# Hypertables → plain Postgres: research findings

Measured against **sentinelstaging** TigerData on 2026-09-16 (PostgreSQL 18.6,
TimescaleDB 2.29.2, timescaledb_osm 1.17.0, toolkit 1.26.0), read-only as
`stl_read_only` through the bastion tunnel. Instance: 8.59 GB RAM, 43 GB
database, 48.7 GB disk (Grafana `timescale_cloud_system_*`). Prod is the same
8.59 GB shape.

Every number below is from that session unless it cites a ticket.

---

## 1. Verdict

**Do not convert all of them, and do not keep all of them. The split is
lopsided and it falls on size.**

| | tables | chunks | share of chunks | on disk |
|---|---|---|---|---|
| ≥ 1 GB | 4 | 768 | 16.1% | 39 GB |
| < 1 GB | 57 | 4,001 | **83.9%** | 2,106 MB |

84% of the partitioning serves 8% of the data. That long tail is where the cost
is, and it buys nothing that a plain table with an index would not do better.

Recommended end state:

- **Keep 2 as hypertables**: `cex_orderbook_snapshots`, `protocol_event`.
- **Keep 1 for retention only**: `block_states` — but fix its shape, it is the
  single most expensive object on the instance.
- **Decide after VEC-800**: `morpho_vault_position`.
- **Convert the other 44 public tables to plain Postgres**, and **drop the 13
  empty `transformed` shadows** rather than converting them.

That removes roughly 4,000 of 4,769 chunks and ~30,000 of 36,002 internal
relations, at a storage cost of about 1–2 GB (§4.2), on an instance that is
currently spending **46% of all its CPU time planning queries**.

---

## 2. What we are actually running

61 hypertables: 48 in `public`, 13 in `transformed`.

**The 13 `transformed` hypertables are all empty** — 0 rows, 0 chunks, 24 kB
each. They are compressed, tiered and policy-managed shadows of nothing. This is
VEC-719's question answered: they are abandoned. Four `public` tables are empty
too, yet still carry chunks: `uniswap_v3_pool_event` (69 chunks),
`curve_parameter_event` (68), `anchorage_package_snapshot` (68),
`anchorage_operation` (13).

**117 background policy jobs**: 58 compression, 56 S3 tiering, 1 retention, plus
telemetry and job-history retention. The database runbook already notes these
"fire in tight clusters".

**We use almost none of what TimescaleDB is for:**

| feature | status |
|---|---|
| continuous aggregates | **zero, anywhere in the schema** |
| retention / `drop_chunks` | **one table** (`block_states`, 30 days, 27 total failures) |
| columnstore compression | 58 tables — real value on 2 of them (§4) |
| S3 tiering | 56 policies attached, **0 bytes actually tiered** (all `_osm_tables.osm_chunk_*` are empty) |
| `time_bucket` / `gapfill` / `locf` | used by the Python API — **but these are extension functions that work on plain tables** |

That last row matters for scope: converting a table to plain Postgres does not
break `time_bucket_gapfill`, `locf` or `last()`. Nothing in the API layer has to
change to de-partition a table.

---

## 3. The cost, measured

### 3.1 Planning, not execution, is where the instance's time goes

`pg_stat_statements`, whole instance:

```
plan_s   exec_s   pct_planning   calls
2582.9   2991.6   46.3           1,536,982
```

**46.3% of database CPU time is query planning.** On an unpartitioned Postgres
schema this is normally 2–5%. It is paid on every call because the transaction-mode
pooler runs with `db_statement_cache_size=0` (VEC-713), so nothing is ever
re-used.

Top consumers, all hypertable reads:

| calls | plan | exec | ratio | query |
|---:|---:|---:|---:|---|
| 129,045 | 574.5 s | 106.9 s | 5.4× | `UPDATE block_states SET block_published …` |
| 138,534 | 499.3 s | 1003.9 s | 0.5× | `SELECT … FROM block_states WHERE chain_id …` |
| 128,473 | 444.3 s | 28.7 s | **15.5×** | `SELECT version FROM block_states WHERE chain_id = $1 AND hash = $2` |
| 1,627 | 221.6 s | 0.9 s | **246×** | `SELECT DISTINCT ON (token_id) … FROM offchain_token_price` |
| 16 | 41.2 s | 10.9 s | 3.8× | sparklend liquidation params (2,572 ms mean planning) |
| 8 | 21.8 s | 2.5 s | 8.7× | morpho vault assets (2,721 ms mean planning) |

`block_states` alone: 2,147 s planning against 2,270 s execution across 755,045
calls.

### 3.2 A worked example: 12,866 rows, 1.3 GB of planner memory

`offchain_token_price` holds **12,866 rows** spread over **344 chunks** — 37 rows
per chunk. The hot query returns **4 rows**:

```sql
EXPLAIN (ANALYZE, BUFFERS, MEMORY)
SELECT DISTINCT ON (token_id) token_id, "timestamp", price_usd
FROM offchain_token_price WHERE token_id = ANY(ARRAY[1,2,3,13])
ORDER BY token_id, "timestamp" DESC;
```

```
Planning:
  Buffers: shared hit=160589          -- 1.25 GB of buffer accesses to PLAN
  Memory: used=24306kB
Planning Time: 224.538 ms
Execution Time: 34.911 ms
Allocated Memory: allocated_by_plan=1336987kB   -- 1.3 GB
(2775 rows of plan)
```

A 2,775-line plan, a Merge Append over 344 chunks each with its own
SkipScan → ColumnarScan → index scan, **1.3 GB allocated by the planner**, to
return four rows from a table that would fit in a single 8 kB page run.

On a plain table with `(token_id, "timestamp" DESC)` this is an index scan:
sub-millisecond planning, single-digit buffers.

The same shape on the live indexer path, `onchain_token_price.GetLatestPrices`
(`internal/adapters/outbound/postgres/onchain_price_repository.go:129`), called
per block:

```
Planning Time: 327.833 ms   Execution Time: 96.913 ms
Planning buffers: 197,520 (1.54 GB)
allocated_by_plan = 1504330kB  -- 1.47 GB
```

**1.47 GB of planner allocation per call, on an 8 GB instance.** This is the
direct, sufficient explanation for the `53200 out_of_memory` storms in
`docs/runbooks/vector-database.md`, and it corroborates VEC-663's independent
measurement on `allocation_position` (724 MB exec / 2.76 GB plan).

Even a single-row lookup by unique key pays:

```
SELECT version FROM block_states WHERE chain_id = 1 AND hash = …
Planning Time: 17.364 ms   Execution Time: 21.441 ms
allocated_by_plan = 60780kB   (169-line plan)
```

17 ms to plan a primary-key-shaped lookup, 128,473 times.

### 3.3 Why: 36,002 relations behind 61 tables

4,769 chunks. **36,002 relations** in the `_timescaledb*` schemas. Every plan
walks that catalogue; every backend caches its slice of it. That is the memory
and the planning time.

`block_states` is the worst offender on its own: 9 indexes × 128 chunks.

| table | total | heap | indexes | % index |
|---|---:|---:|---:|---:|
| `block_states` | **18 GB** | 5,179 MB | **13 GB** | **71.9%** |
| `prime_debt` | 21 MB | 3,008 kB | 12 MB | 54.1% |
| `prime_reference_position` | 65 MB | 31 MB | 33 MB | 50.6% |
| `offchain_token_price` | 49 MB | 11 MB | 15 MB | 31.1% |

`block_states` is 42% of the entire database and **72% of it is index**, because
every index is duplicated per chunk and chunk count is inflated 4× by a
`chain_id` space dimension (128 chunks = 32 days × 4 chains, for a 30-day
retention window).

`prime_debt` holds **630 rows** in 180 chunks and spends 12 MB on indexes.

### 3.4 Correctness and operational debt that exists only because of chunking

Every one of these is a hypertable-induced defect with an open ticket:

- **VEC-615** (~35 tables): on a compressed chunk `ON CONFLICT` resolves *before*
  row triggers, so a trigger-assigned `processing_version` reaches the arbiter as
  `DEFAULT 0` and the correction row is **silently discarded**. ADR-0002's whole
  corrections model stops working once a chunk compresses. The workaround is a
  hand-written `next_processing_version_<table>()` per table.
- **VEC-570**, confirmed live: 19 tables compress after **1 day**, not the 2 days
  their migration asks for — the `tsdb.hypertable` auto-policy that
  `add_compression_policy(…, if_not_exists => TRUE)` silently declines to
  replace. So VEC-615 bites a day earlier than anyone designed for.
- **VEC-541 / VEC-543**: `plan_cache_mode = 'force_custom_plan'` required on 36
  trigger functions, enforced by a catalogue test, because a generic plan fans
  out over every chunk (4,410 ms vs 148 ms per batch).
- **VEC-672**: a time window bound as a parameter instead of interpolated as a
  literal planned all 355 chunks of `onchain_token_price` — 21.8 MB / 754 ms vs
  100 kB / 0.4 ms. The Python API's `_time_window.py` still binds its windows,
  wrapped in `(:x IS NULL OR col >= :x)`, which cannot prune at plan time at all.
- **VEC-581**: chunk creation deadlocks against concurrent `protocol` upserts.
- **VEC-575**: 1.2 GB planner memory on one `/v1/primes/{id}/debt` call.
- The **tiering round-trip CHECK-constraint trap** (`x BETWEEN a AND b AND …`
  and `IN`-lists on `VARCHAR` fail to re-parse), which blocked every staging
  deploy on 2026-09-07 and now needs a dedicated migrator test.
- `timescaledb.enable_tiered_reads` is **off** on the server, so any chunk that
  reaches S3 becomes invisible to ordinary reads. Six migrations already have to
  `SET LOCAL` it. Nothing is tiered yet (all OSM chunks are 0 bytes) because the
  oldest data is ~8 months old against a 1-year horizon — this lands around
  **Jan–Feb 2027**. `core_model_results` has a **30-day** horizon.

None of this exists on a plain table.

### 3.5 What the read paths actually ask for

Scanning the Go and Python read paths, the dominant shape is "latest row per
entity", with **no predicate on the partition column**:

```sql
SELECT DISTINCT ON (token_id) token_id, price_usd FROM onchain_token_price
WHERE oracle_id = $1
ORDER BY token_id, block_number DESC, block_version DESC, processing_version DESC
```

Ordering is by `block_number`/`block_version`; partitioning is by `timestamp`.
Chunk exclusion cannot fire, so the planner opens a path per chunk and the
executor holds a sort per chunk under a Merge Append. The `*_current` cache
tables (VEC-409, VEC-577, VEC-660, VEC-659, VEC-753) are the team already
routing around this — every one of them is a plain table.

---

## 4. The benefit, measured

Compression is the only TimescaleDB feature currently earning its keep, and it
is concentrated in two tables.

### 4.1 Where compression pays

| table | before | after | ratio | saved |
|---|---:|---:|---:|---:|
| `cex_orderbook_snapshots` | 45.0 GB | 14.3 GB | 3.1× | **30.7 GB** |
| `protocol_event` | 15.2 GB | 1.3 GB | 11.9× | **13.9 GB** |
| `morpho_vault_position` | 3.1 GB | 2.05 GB | 1.5× | 1.05 GB |
| `onchain_token_price` | 2.44 GB | 0.11 GB | 22.6× | 2.33 GB |
| `allocation_position` | 1.54 GB | 0.09 GB | 17.7× | 1.45 GB |
| `borrower_collateral` | 1.32 GB | 0.17 GB | 7.6× | 1.15 GB |
| **all 58** | **71.6 GB** | **18.9 GB** | 3.8× | **52.7 GB** |

Two tables account for **44.6 GB of the 52.7 GB**. The other 56 compressed
hypertables save ~8 GB between them.

### 4.2 The long tail's "savings" are mostly chunk overhead

`before_compression_total_bytes` is the measured size of the *chunked*
uncompressed form — per-chunk heap padding and per-chunk index copies included.
It is not what a plain table would cost.

`offchain_token_price`: 12,866 rows, "saved 172 MB". A plain table holding 12,866
narrow rows with two indexes is a couple of MB. The 210 MB it was compressed
*from* is 344 chunks' worth of overhead, not data.

`prime_debt`: 630 rows, "saved 31 MB", heap 3 MB, indexes 12 MB, 180 chunks.

So converting the long tail does not cost the ~8 GB the compression stats imply.
Realistically it costs **1–2 GB**, and it returns ~4,000 chunks and ~30,000
catalogue relations.

### 4.3 Nothing in the tail is near the size where partitioning matters

The repo's own threshold, from `alerts/vector-indexers.yaml:3006`: *"a plain
table gets uncomfortable past ~100M rows."*

Largest hypertables by rows: `block_states` 25.7M, `protocol_event` 14.8M,
`cex_orderbook_snapshots` 12.7M, `morpho_vault_position` 9.8M,
`onchain_token_price` 9.0M. Everything else is under 3M. The tail is in the
hundreds-to-thousands.

`maple_syrup_global_state` holds **153 rows across 93 chunks**.
`uniswap_v3_liquidity_event` holds **106 rows across 69 chunks**.

---

## 5. Table-by-table

### Keep — earns it (2)

| table | rows | chunks | rows/chunk | why |
|---|---:|---:|---:|---|
| `cex_orderbook_snapshots` | 12.7M | 79 | 161,193 | Genuine high-rate append-only series; compression saves 30.7 GB (64% of all savings). Kept for **storage economics, not read performance** — its reads do not prune at plan time (§5.1). |
| `protocol_event` | 14.8M | 359 | 41,294 | 11.9× compression, 13.9 GB saved. **But 359 chunks is ~10× too many** — widen to 30 days (VEC-663) and fix VEC-581 and VEC-615 first; it is the table VEC-615 names as most exposed. |

### Keep for retention, but fix it first (1)

**`block_states`** — 25.7M rows, 18 GB, **72% index**, 128 chunks, no
compression, 30-day retention. It is the only user of `drop_chunks` in the
schema, which is the one thing a plain table genuinely cannot do cheaply. It is
also the most expensive object on the instance: 2,147 s of planning, 13 GB of
per-chunk index copies, 17 ms to plan a unique-key lookup.

Three fixes, in order, before any conversion question:

1. **Drop the `chain_id` space dimension.** It quadruples chunk count (4 × 32
   days = 128) for a table whose queries already filter `chain_id` through a
   btree. This alone removes 96 chunks and ~9 GB of duplicated index.
2. **Widen `chunk_time_interval` to 7 days.** Retention is 30 days; 4–5 chunks is
   plenty. Combined with (1): 128 chunks → ~5.
3. **Prune the 9 indexes.** At 5 chunks they cost 5× less, but several look
   redundant (`idx_block_states_chain_number_version` vs
   `unique_chain_block_version`).

Expected: 18 GB → low single-digit GB, and the largest single planning line item
on the instance mostly disappears.

### 5.1 Do the keeps actually prune chunks? Mostly no

The keeps above are justified by storage and retention. They are **not**
justified by query shape — measured on each one's real hot path:

| table | partition col | hot query keys on | pruning | plan time |
|---|---|---|---|---|
| `block_states` | `created_at` | `(chain_id, hash)`, `(chain_id, number)` | **none** — plain `Append`/`Merge Append` over all 32 chunks of the chain, no `Chunks excluded` line at all | 17.7 ms for a unique-key lookup, ×128,473 calls |
| `cex_orderbook_snapshots` | `persisted_at` | `(symbol, persisted_at)` | **runtime only** — `Chunks excluded during startup: 78` of 79 | 49–84 ms bound vs **3.7 ms literal** |
| `protocol_event` | `created_at` | DQ reads on `created_at` | plan-time, correctly | 7.1 ms |
| `protocol_event` | `created_at` | `(chain_id, block_number)` | **none** — `Append` over all 359 chunks | **1,957 ms** |
| `morpho_vault_position` | `timestamp` | latest-per-`(vault, user)` by `block_number` | **none** — `Merge Append` over all 202 chunks | **1,672 ms** |

Three findings fall out of this:

1. **`block_states` is partitioned on a column nothing queries.** Its hot path
   keys on `(chain_id, hash)` and `(chain_id, number)`; `created_at` appears in
   none of it. 755,045 calls each open every chunk of their chain. It is the
   single biggest planning line item on the instance (2,147 s) and gets zero
   pruning in return. The *only* thing keeping it a hypertable is `drop_chunks`
   for the 30-day retention.

2. **`cex_orderbook_snapshots` has the right query shape and throws it away.**
   `core_model_orderbook_reader.py:45` writes the window as
   `persisted_at > now() - CAST(:max_age AS interval)` — a bound parameter. The
   planner therefore builds paths for all 79 chunks and `ChunkAppend` excludes
   78 of them at *startup*. Execution is fine (0.9 ms); planning is 13–23×
   worse than the literal form, and with `db_statement_cache_size=0` under the
   transaction pooler that planning is paid on every call. This is exactly the
   VEC-672 trap `db/migrations/AGENTS.md` documents — fixed for
   `onchain_token_price`, never fixed for this table, same CORE feed-liveness
   family. **One-line fix, independent of any conversion.**
   Its other read, `SELECT MAX(event_time)` with no `WHERE` at all, touches all
   79 chunks for 1.5 s and cannot prune by construction.

3. **All four keeps are INSERT-dominated**, so read pruning was never really the
   argument:

   | table | INSERTs | SELECTs |
   |---|---:|---:|
   | `protocol_event` | 28,953 | 19 |
   | `morpho_vault_position` | 19,708 | ~18 |
   | `cex_orderbook_snapshots` | 418 | 11 |

   Nearly every SELECT against them is a data-quality check, not an application
   read. That is a genuine argument *for* keeping them partitioned — writes go
   to the newest chunk and compression pays on the cold ones — but it means the
   decision rests on storage and retention, not on query performance, and §5's
   "keep" list should be read that way.

### Decide after VEC-800 (1)

**`morpho_vault_position`** — 9.8M rows, 2.2 GB, 202 chunks. Compression only
1.5× because `segmentby = (morpho_vault_id, user_id)` yields 2.9 rows per batch
(VEC-800). Fix the segmentby to `morpho_vault_id` and widen the interval; if it
does not clear ~5×, convert it — 9.8M rows is unremarkable for a plain table.

### Convert to plain Postgres (44)

Everything else in `public`. Ordered worst-first by rows/chunk — the top of this
list is where partitioning is actively absurd:

| table | rows | chunks | rows/chunk |
|---|---:|---:|---:|
| `uniswap_v3_pool_event` | 0 | 69 | 0 |
| `curve_parameter_event` | 0 | 68 | 0 |
| `anchorage_package_snapshot` | 0 | 68 | 0 |
| `anchorage_operation` | 0 | 13 | 0 |
| `maple_syrup_global_state` | 153 | 93 | 1 |
| `uniswap_v3_liquidity_event` | 106 | 69 | 1 |
| `prime_debt` | 630 | 180 | 3 |
| `curve_liquidity_event` | 416 | 69 | 6 |
| `maple_sky_strategy_state` | 680 | 93 | 7 |
| `prime_capital_stack` | 208 | 28 | 7 |
| `uniswap_v3_swap` | 775 | 69 | 11 |
| `curve_lp_token_event` | 799 | 69 | 11 |
| `uniswap_v3_pool_state` | 759 | 69 | 11 |
| `core_model_results` | 51 | 4 | 12 |
| `curve_stableswap_state` | 1,659 | 69 | 24 |
| `offchain_token_price` | 12,866 | 344 | 37 |
| `maple_pool_state` | 3,696 | 93 | 39 |
| `uniswap_v4_liquidity_event` | 100 | 2 | 50 |
| `prime_reference_balance_sheet` | 814 | 13 | 62 |
| `curve_cryptoswap_state` | 4,826 | 71 | 67 |
| `curve_swap` | 6,475 | 70 | 92 |
| `asset_price` | 6,160 | 10 | 616 |
| `borrower` | 183,614 | 212 | 866 |
| `morpho_adapter_state` | 403,031 | 359 | 1,122 |
| `morpho_vault_state` | 592,706 | 202 | 2,934 |
| `psm3_alm_shares` | 14,963 | 4 | 3,740 |
| `psm3_reserves` | 47,476 | 12 | 3,956 |
| `sparklend_reserve_data` | 1,009,257 | 210 | 4,805 |
| `morpho_market_state` | 1,389,474 | 202 | 6,878 |
| `morpho_market_position` | 1,407,483 | 202 | 6,967 |
| `uniswap_v4_pool_state` | 28,504 | 3 | 9,501 |
| `maple_loan_collateral` | 824,352 | 83 | 9,931 |
| `maple_loan_state` | 847,502 | 83 | 10,210 |
| `uniswap_v4_swap` | 32,861 | 3 | 10,953 |
| `allocation_position` | 2,195,416 | 196 | 11,201 |
| `prime_capital_stack_allocation` | 34,850 | 3 | 11,616 |
| `borrower_collateral` | 2,825,611 | 212 | 13,328 |
| `token_total_supply` | 482,379 | 24 | 20,099 |
| `onchain_token_price` | 9,026,936 | 355 | 25,427 |
| `prime_reference_position` | 184,383 | 3 | 61,461 |
| `position_state`, `fluid_vault_state`, `maple_ftl_loan_state`, `uniswap_v4_pool_event` | 0 | 0 | — |

Two on this list deserve a second look rather than a reflex:

- **`onchain_token_price`** — 9M rows but only 194 MB on disk, because
  compression is 22.6×. Uncompressed it is 2.4 GB. It is the strongest
  *compression* case in the tail, and simultaneously the worst *planning* case
  (327 ms / 1.47 GB per call, 355 chunks, on the live indexer path). Widening to
  30-day chunks gets both: ~12 chunks, compression intact. Try that before
  converting.
- **`allocation_position`** and **`borrower_collateral`** — 17.7× and 7.6×,
  ~1.2–1.5 GB saved each, but 196/212 chunks and VEC-663's measured 724 MB/2.76 GB
  query. Same treatment: widen first, measure, then decide.

### Drop, don't convert (13)

All 13 `transformed.*` hypertables: 0 rows, 0 chunks, 24 kB, yet each carries a
compression policy and a tiering policy. That is 26 of the 117 background jobs
maintaining nothing. VEC-719 asks whether they are intentional; they are not
being written to. Removing them is the cheapest win here.

---

## 6. Recommended sequencing

The tail conversion is not urgent in the way the top three are. Ordered by
value per unit of risk:

0. **Fix the `cex_orderbook_snapshots` liveness window** —
   `core_model_orderbook_reader.py:45`, bound `CAST(:max_age AS interval)` →
   interpolated literal. One line, 84 ms → 3.7 ms of planning per call, no
   schema change, no conversion. The VEC-672 fix that never reached this table.
1. **Drop the 13 empty `transformed` hypertables** and the 4 empty `public` ones
   that still hold chunks. Zero risk, removes 26+ policy jobs and ~218 chunks.
2. **Land VEC-663 / PR #808** (chunk-interval widening). Already in review. This
   is the single highest-value change and it does not require converting
   anything.
3. **Fix `block_states`**: drop the space dimension, widen to 7 days, prune
   indexes. Biggest object, biggest planning line item, and its retention policy
   is the only reason it stays a hypertable at all.
4. **Fix `morpho_vault_position` segmentby** (VEC-800) and re-measure.
5. **Convert the tail**, in batches, worst rows/chunk first. The first batch
   (everything under 100 rows/chunk — 21 tables) is nearly free: they hold almost
   no data, so `create_hypertable`'s inverse is a fast table rewrite.
6. **Re-measure** `pg_stat_statements` planning share. Target: under 10%.

Steps 1–3 alone should move the needle on the OOM alerts without a single
conversion.

---

## 7. How to convert (the inverse of the create-plain rule)

There is no `drop_hypertable` that de-partitions in place. The recipe:

```sql
-- 1. Stop the policies first, or they will fight the rewrite.
SELECT remove_compression_policy('<table>', if_exists => true);
SELECT remove_tiering_policy('<table>', if_exists => true);   -- if attached

-- 2. Decompress every chunk (must happen before the copy; compressed chunks
--    cannot be read into a plain table without decompression anyway).
DO $$ DECLARE c regclass; BEGIN
  FOR c IN SELECT show_chunks('<table>') LOOP
    PERFORM decompress_chunk(c, if_compressed => true);
  END LOOP;
END $$;
ALTER TABLE <table> SET (timescaledb.compress = false);

-- 3. Rewrite into a plain table, then swap.
CREATE TABLE <table>_plain (LIKE <table> INCLUDING ALL EXCLUDING INDEXES);
INSERT INTO <table>_plain SELECT * FROM <table>;
-- recreate indexes WITHOUT the partition column forced into unique keys
-- drop the old, rename the new
```

Five things the conversion has to carry, or it ships a defect:

1. **The unique keys can shrink again.** A hypertable forces its partition column
   into every unique index. `block_timestamp`/`created_at` was added to these PKs
   purely to satisfy that, and `ON CONFLICT` arbiters name the widened key. The
   INSERT and the migration must change together — exactly the trap the
   *forward* conversion recipe documents in `docs/runbooks/vector-indexers.md`,
   run backwards.
2. **The VEC-615 `next_processing_version_<table>()` machinery becomes
   unnecessary** on converted tables (no compressed chunks, so the trigger fires
   normally again), but leave it in place — it is correct either way, and
   removing it is a separate change.
3. **`plan_cache_mode = 'force_custom_plan'`** (VEC-541) can come off converted
   tables, and should: on a plain table the generic plan is the good one, and
   re-planning per execution is now pure cost. `TestProcessingVersionTriggersForceCustomPlan`
   enumerates `pg_proc` and will need scoping to the remaining hypertables.
4. **Append-only grants survive** — `REVOKE UPDATE, DELETE` is table-level and
   does not depend on chunks. The converted-table list in
   `db/migrations/AGENTS.md` is unaffected.
5. **`COMMENT ON` tags**: `[Hypertable]` → `[Timeseries]`, and the comment should
   record that it was converted and what would convert it back — the mirror of
   the existing create-plain rule.

`db/migrations/AGENTS.md` needs the reverse rule written down alongside the
create-plain one, plus the permanently-plain list extended.

---

## 7a. Cost: leaving TimescaleDB entirely vs keeping compression

The question behind the tail conversion is whether to leave the TimescaleDB
service type altogether for a plain Postgres service on TigerData, which bills
compute at a lower rate. That trades compression (44.6 GB saved on two tables
today) against cheaper vCPU-hours.

### Rates

From the infrastructure repo's FinOps model (ADR-010 / ORB-248, PR #388),
calibrated to the April and May 2026 invoices to within 1.5%:

| item | rate | per month (730 h) |
|---|---|---|
| compute, TimescaleDB service | $0.3424 / vCPU-hr | **$250 / vCPU** |
| storage (billed on used GiB, primary *and* replica) | $0.0014544 / GiB-hr | **$1.06 / GiB** |
| prod pooler + VPC | — | $44.64 |
| **May 2026 invoice, all services** | | **$1,965.03** |

Fleet compute: prod primary + replica (4 vCPU) $1,000, staging (2 vCPU) $500,
one further non-prod service ≈ $290 — roughly **$1,750/month of the $1,965 is
compute**. Storage is about **$130/month** (prod 2 × 35 GiB, staging 49 GiB).
**Storage is ~7% of the bill.** Compute is what the decision is about.

The plain-Postgres service rate is not published on tigerdata.com, the docs, or
the AWS Marketplace listing — only the console calculator shows it. The
break-even below is therefore expressed as "how much cheaper per vCPU-hr the
plain service has to be".

### What the storage side actually costs

Both prod and staging are 2 vCPU / 8 GB, and neither is CPU-bound: prod averaged
**12%** CPU over the last 7 days, staging 23%. Prod memory averaged 42% and
peaked at 64% over 30 days. The instances are sized for planner memory, not
for work (§3) — which is itself an argument that de-chunking could allow a
smaller instance, on either service type.

Plain-PG footprint of staging **today**: ≈ 80 GB vs 43 GB now (compressed
tables at their measured uncompressed size, `block_states` de-bloated from 18 GB
to ~8 GB, tail at heap + sane indexes). **+$40/month.** Negligible.

The forward picture is dominated by one table. `cex_orderbook_snapshots` moved
to full-depth books every 60 s on staging (15 Sep 14:25 UTC) and on prod today
(VEC-740, `5099e66f`). Measured:

- a Coinbase BTC-USD book row is **612 kB** after TOAST (1.09 MB raw JSON,
  lz4 is already the server default — `default_toast_compression = lz4` —
  so plain Postgres has nothing further to squeeze);
- Coinbase alone writes ~1.8 GB/day, the table **~2.0 GB/day uncompressed**
  (16 Sep, half day: 1.16 GB; 15 Sep: 1.64 GB);
- old-regime chunks compress 3.3–5.3× (≈ 250–360 MB/day on disk). The first
  full-depth chunk compresses on ~17 Sep; the ratio on 600 kB JSONB blobs is
  **not yet known** and is the largest uncertainty here.

`protocol_event` adds 0.15 GB/day uncompressed (12× compressed). Everything
else is noise at this scale.

Twelve-month storage per service, by the one decision that matters — **whether
`cex_orderbook_snapshots` gets a retention policy** (today it has none):

| cex retention | plain Postgres | TimescaleDB (3.3×) | delta / service / month |
|---|---:|---:|---:|
| none | ~820 GB, **$873/mo** | ~250 GB, $266/mo | **$608** |
| 90 days | ~275 GB, $289/mo | ~85 GB, $89/mo | $201 |
| 30 days | ~155 GB, $162/mo | ~47 GB, $50/mo | $112 |

Prod pays that delta **twice** (replica storage is billed at the same rate).

### Break-even

Fleet compute is $1,750/month. Storage delta from losing compression, fleet-wide
(prod ×2 + staging):

| cex retention | storage delta / month | plain service must be cheaper by |
|---|---:|---:|
| none | ~$1,825 | >100% — **not viable** |
| 90 days | ~$600 | ≥ 35% per vCPU-hr |
| 30 days | ~$335 | ≥ 19% per vCPU-hr |
| 30 days, and prod drops to 1 vCPU / 4 GB after de-chunking | ~$335 vs ~$1,300 compute | ≥ 0% — cheaper regardless |

So:

1. **Without a retention policy on `cex_orderbook_snapshots`, leaving
   TimescaleDB does not pay at any plausible discount.** At full depth with no
   expiry it is ~730 GB/year uncompressed. That table is the entire case for
   keeping the columnstore.
2. **With a 30-day retention on `cex_orderbook_snapshots`** (native declarative
   partitioning + `DROP PARTITION`, which plain Postgres does fine — the same
   answer as for `block_states`), the storage penalty is ~$335/month fleet-wide
   and a **~19% cheaper vCPU-hour** on the plain service breaks even. Anything
   beyond that is saving.
3. **The real prize is not the service-type discount, it is instance size.**
   Prod runs at 12% CPU on 2 vCPU because 8 GB of RAM is needed to absorb 1.3–1.5
   GB planner allocations per call (§3.2). Remove the chunking and that pressure
   goes with it. Dropping prod primary + replica to 1 vCPU / 4 GB saves **$500/month
   on its own**, more than any storage delta in the table above — and that is
   available on *either* service type once the tail is converted and
   `block_states` is fixed.

**Recommendation on the cost question:** the decision to leave TimescaleDB
hinges entirely on retention for the orderbook table. Decide that first. If
30–90 days of full-depth books is enough for the CORE model (its reader only
asks for the latest snapshot per venue, §5.1), the plain service is cheaper as
soon as its vCPU rate is ≥ 20–35% lower — which is the claim to verify in the
console. If the books must be kept indefinitely, stay on TimescaleDB for that
one table's compression and still convert the tail.

## 8. Risks and things I have not established

- **Disk will grow.** Converting the tail costs ~1–2 GB (§4.2); converting
  `onchain_token_price`, `allocation_position` and `borrower_collateral` without
  widening first would cost ~5 GB. Keeping the two big tables compressed
  preserves 44.6 GB of the 52.7 GB saving.
- **The rewrite needs a window.** `INSERT … SELECT` over `morpho_vault_position`
  (9.8M rows) or `onchain_token_price` (9M) takes an exclusive lock for the
  duration. The small tables are seconds.
- **`block_states` retention is the one real dependency.** If it is ever
  converted, retention becomes a `DELETE` job and a vacuum problem. I would not
  convert it; I would fix its shape.
- **The full-depth compression ratio is unmeasured.** Every compressed
  `cex_orderbook_snapshots` chunk predates the 60 s / full-ladder change; the
  first one under the new regime compresses on ~17 Sep. If 600 kB JSONB rows
  compress worse than the 3.3× assumed in §7a, the TimescaleDB column of the
  storage table gets worse and the break-even moves in favour of the plain
  service. Re-run `chunk_compression_stats('cex_orderbook_snapshots')` after
  that.
- **The plain-Postgres vCPU rate is unverified.** It is not on any public
  TigerData page; §7a's break-even needs the figure from the console
  calculator.
- **I did not measure prod**, only staging. Prod is the same instance size and
  the same schema, but chunk counts and the tail's row counts will differ.
- **I did not benchmark a converted table side by side.** The plain-table numbers
  in §3.2 are inferred from the plan shape, not measured — creating a table
  requires write access I deliberately did not use. Worth doing on one table
  (`offchain_token_price` is the obvious candidate) before committing to the
  sweep.
- **The 46% planning figure is instance-wide**, so it includes non-hypertable
  work. The per-query breakdowns in §3.1–3.2 are what isolate the cause.

---

## 9. Reproducing this

Through the bastion tunnel (`make -C stl-verify tigerdata-tunnel`, then the
read-only DSN):

```sql
-- inventory
SELECT hypertable_schema, count(*) FROM timescaledb_information.hypertables GROUP BY 1;

-- the decision table: rows per chunk
WITH h AS (SELECT hypertable_schema s, hypertable_name t,
                  format('%I.%I',hypertable_schema,hypertable_name)::regclass rc
           FROM timescaledb_information.hypertables)
SELECT s||'.'||t, approximate_row_count(rc) AS rows,
       (SELECT count(*) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema=s AND c.hypertable_name=t) AS chunks,
       pg_size_pretty(hypertable_size(rc)) AS on_disk
FROM h ORDER BY 2 DESC;

-- where compression actually pays
SELECT h.hypertable_name,
       pg_size_pretty(sum(cs.before_compression_total_bytes)),
       pg_size_pretty(sum(cs.after_compression_total_bytes))
FROM timescaledb_information.hypertables h
CROSS JOIN LATERAL hypertable_compression_stats(
    format('%I.%I',h.hypertable_schema,h.hypertable_name)::regclass) cs
GROUP BY 1 ORDER BY sum(cs.before_compression_total_bytes)
                  - sum(cs.after_compression_total_bytes) DESC;

-- index vs heap
SELECT h.hypertable_name, pg_size_pretty(sum(ds.table_bytes)) heap,
       pg_size_pretty(sum(ds.index_bytes)) indexes
FROM timescaledb_information.hypertables h
CROSS JOIN LATERAL hypertable_detailed_size(
    format('%I.%I',h.hypertable_schema,h.hypertable_name)::regclass) ds
GROUP BY 1 ORDER BY sum(ds.index_bytes) DESC;

-- planning share
SELECT sum(total_plan_time)/1000 plan_s, sum(total_exec_time)/1000 exec_s,
       100*sum(total_plan_time)/(sum(total_plan_time)+sum(total_exec_time)) pct
FROM pg_stat_statements;

-- the smoking gun
EXPLAIN (ANALYZE, BUFFERS, MEMORY)
SELECT DISTINCT ON (token_id) token_id, "timestamp", price_usd
FROM offchain_token_price WHERE token_id = ANY(ARRAY[1,2,3,13])
ORDER BY token_id, "timestamp" DESC;
```

Note: the readonly role competes with the fleet for connection slots. This
session was blocked for ~50 minutes by
`remaining connection slots are reserved for roles with privileges of
"pg_use_reserved_connections"` before a slot freed.

## 10. Related tickets

VEC-663 (in review, PR #808) · VEC-800 · VEC-615 · VEC-570 · VEC-541 · VEC-543 ·
VEC-672 · VEC-581 · VEC-575 · VEC-719 · VEC-713 · VEC-509
