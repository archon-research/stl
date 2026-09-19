# Plan Critique & Revisions

**Date**: 2026-09-19
**Source**: Self-review + Fable 5.1 automated critique

---

## Critical Findings (Blockers)

### B1. `LAG IGNORE NULLS` is PG19, not PG18

The gapfill replacement strategy assumed PG18 supports `LAG(... IGNORE NULLS)`.
It does not — this feature was committed 2025-10-03 for **PostgreSQL 19** only.
PG18 docs explicitly state: "The SQL standard defines a RESPECT NULLS or IGNORE
NULLS option [...] This is not implemented in PostgreSQL."

**Fix**: Use the count-group LOCF construction instead:
```sql
SELECT bucket, entity,
       first_value(agg) OVER (PARTITION BY entity, grp ORDER BY bucket) AS value
FROM (
  SELECT gs.bucket, e.entity, d.agg,
         count(d.bucket) OVER (PARTITION BY e.entity ORDER BY gs.bucket) AS grp
  FROM generate_series(...) gs(bucket)
  CROSS JOIN (SELECT DISTINCT entity FROM d) e
  LEFT JOIN d ON d.bucket = gs.bucket AND d.entity = e.entity
) x
```

### B2. Baseline squash is superior to migrator pre-processor

The migrator pre-processor approach (stripping `WITH (tsdb.*)` storage params) is
permanent complexity for 162 files that will never change. The **baseline squash**
is cleaner:

1. Generate `00000000_000000_baseline.sql` from `pg_dump --schema-only` of a fully
   migrated TimescaleDB test DB (hypertable parents dump as ordinary CREATE TABLE)
2. Insert all 162 historical filenames into `migrations` with their checksums
3. Move 162 files to `db/migrations/archive/` (migrator reads only top-level dir)
4. Fresh DBs run baseline once; migrated prod carries its `migrations` table over

**Decision**: Adopt baseline squash. The migrator pre-processor (tsdb_compat.go) is
still useful as a transitional tool for development/testing, but the baseline is the
production path.

### B3. `_parity_*` functions read TimescaleDB catalog at runtime

`20260706_140000_create_transformed_bucket1.sql` defines `transformed._parity_refresh`
and `transformed._parity_verify_all` which query `timescaledb_information.dimensions`
and `.chunks` and `RAISE EXCEPTION 'no time dimension'` if empty. These are called by
the transform worker. Also `gen-transformed` (the code generator) emits `create_hypertable`
+ `SET (timescaledb.compress ...)`.

**Fix**: Add follow-up migration replacing both functions with plain-table versions.
Update `gen-transformed` to stop emitting hypertable DDL.

### B4. Tiered chunks are NOT in pg_dump

S3-tiered chunks are not local relations. Must `untier_chunk` before dump.
Requires S3 egress time and local disk headroom on TigerData.

**Fix**: Add untier step to Phase 5 with time estimate from sizing step.

### B5. `first()`/`last()` are TimescaleDB aggregates

Used at all 7 gapfill sites. Not in the original inventory.

**Fix**: Replace with `(array_agg(x ORDER BY ts DESC))[1]` or `DISTINCT ON`.

---

## High-Priority Findings

### H1. Retention on `block_states` needs replacement

The retention policy drops with no replacement. `block_states` grows unbounded.
Since it's hot cache and not append-only-converted, a scheduled DELETE produces bloat.

**Fix**: Partition `block_states` with native range partitioning. Retention becomes
`DROP PARTITION`.

### H2. Compression/tiering loss has no storage plan

Columnstore yields 5-15x compression; S3 tiering moves >1yr data off disk.
On plain PG the entire history sits uncompressed on gp3.

**Fix**: Add Phase 0 step: measure actual data sizes with
`hypertable_compression_stats()` and `timescaledb_osm.tiered_chunks` on prod.
This drives instance sizing, storage planning, and RDS-vs-Aurora decision.

### H3. Partition-column indexes will be lost

`create_hypertable` auto-creates `<table>_<partcol>_idx`. The baseline must
explicitly include these or every time-windowed query becomes a seq scan.

**Fix**: Include explicit `CREATE INDEX ... (partition_col DESC)` per table in
the baseline migration.

### H4. `locf(last(x))` carries NULL as a value

The gapfill replacement must distinguish "bucket absent" from "bucket present with
NULL". `IGNORE NULLS` would silently substitute an older non-null value — a behavior
change. The count-group construction (B1) handles this correctly.

### H5. date_bin origin alignment

TimescaleDB's default origin differs from `1970-01-01`. `gap_policy.py` uses
`_BUCKET_ORIGIN = 2000-01-01`. Must use the same origin constant in `date_bin`.

---

## Medium-Priority Findings

- **Alpine image collation**: Use `postgres:18` (Debian), not `-alpine` (musl), to
  match glibc collation behavior of RDS.
- **Sequences after data copy**: Must `setval` every sequence post-load.
- **Triggers during data load**: Disable triggers during bulk load; rebuild
  `*_current` caches afterwards.
- **Template format bump**: Removing `DisableScheduledJobs` changes `buildTemplate`,
  requires bumping `templateFormat`.
- **`block_meta_repository` windows**: Keep bounded windows but derive bounds from
  `min/max(partCol)` split into N ranges, not unbounded single query.
- **Autovacuum tuning**: Large append-only tables need
  `autovacuum_vacuum_insert_scale_factor` tuned low on RDS.

---

## Revised Strategy

### Migration path: Baseline Squash + Incremental Forward Migrations

1. **Phase 0**: Measure data sizes on prod (compression stats, tiered data, total)
2. **Phase 1**: Code changes that work on BOTH engines (block_meta, gapfill, _parity_*)
   - Land and soak on TigerData
3. **Phase 2**: Baseline squash + test image switch (ONE PR)
   - Generate baseline SQL from pg_dump of migrated TimescaleDB
   - Move 162 files to archive/
   - Switch test images to `postgres:18`
   - Remove compression/tiering integration tests
4. **Phase 3**: RDS provisioning (parallel with Phase 1-2)
5. **Phase 4**: Staging cutover (data copy, verify, flip secrets)
6. **Phase 5**: Prod cutover (after 48h staging soak)
7. **Phase 6**: Cleanup (after 2-week rollback window)

### Realistic timeline: ~5-7 weeks elapsed

| Phase | Duration | Notes |
|-------|----------|-------|
| 0 | 1 day | Sizing queries on prod |
| 1 | 1-2 weeks | Gapfill rewrite is the critical path |
| 2 | 3-5 days | Baseline generation + review |
| 3 | 1-2 days | Terraform, parallel with Phase 1 |
| 4 | 3-5 days | Staging rehearsal |
| 5 | 1 day | After 48h staging soak |
| 6 | 2-3 days | After 2-week rollback window |

### Data migration: parallel COPY with delta catch-up

Not pg_dump-and-load. Instead:
1. Schema from migrator (run baseline against RDS)
2. Data via parallel `COPY (SELECT * FROM <table>) TO STDOUT | COPY FROM STDIN`
3. Pre-copy history while live; at cutover copy only the delta
4. Downtime: minutes, not hours (SNS/SQS buffers blocks)
