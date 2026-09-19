# Critique: `docs/migrate/001-timescaledb-to-vanilla-postgres.md`

Reviewed against the repo as of `main` (37e56db0) and the `infrastructure` checkout at
`/Users/tore/workspace/infrastructure`. Every count below was re-derived from the tree, not taken
from the plan.

## 0. Verdict

The plan is a good inventory of *where* TimescaleDB is touched, but three of its load-bearing
decisions do not survive contact with the code, and a fourth is a correctness bug:

| # | Blocker | Why it fails | Fix (detail in the section cited) |
|---|---------|--------------|-----------------------------------|
| B1 | Phase 3 "compatibility shim" | 19 migration files create 38+ hypertables with `CREATE TABLE … WITH (tsdb.hypertable, …)` and 24 files run `ALTER TABLE … SET (timescaledb.compress, …)`. These are storage parameters, not function calls; vanilla PG rejects the `tsdb`/`timescaledb` namespace at parse time. No SQL-level shim can intercept them. | Squash to a **baseline migration** and add a `-- migrate: baseline` directive to the migrator (§2). |
| B2 | Phase 5 data migration via `pg_dump` | Tiered chunks live in S3 and are not local relations: `pg_dump` never sees them, and neither does any read without `timescaledb.enable_tiered_reads = on`. Compressed chunks dump as opaque bytea batches. 116 `add_tiering_policy` calls and 64 `add_compression_policy` calls say most history is in one of those two states. | Export per hypertable with `COPY (SELECT * FROM t) TO STDOUT` under `enable_tiered_reads = on`, into a schema the **migrator** already created on RDS. Never post-process a dump (§4). |
| B3 | Phase 3 migration run against TigerData | `DROP EXTENSION timescaledb` needs to own the extension; migrations run as `stl_migrator` (CREATEROLE only, `bootstrap-db.sh:33`). It also cannot drop an extension that still owns tiered chunks. And the in-place hypertable→plain copy of every table on the production instance is pure risk for a database that is about to be re-loaded elsewhere anyway. | Delete the in-place conversion. The baseline is generated in a throwaway container; prod never runs a conversion (§2.3, §3). |
| B4 | `LAG(... IGNORE NULLS)` | `IGNORE NULLS` / `RESPECT NULLS` for window functions was committed on 2025-10-03 for **PostgreSQL 19**, not 18. The recommended Option (a) does not parse on the target. `last()` is also a TimescaleDB aggregate and has no vanilla equivalent under that name. | Gaps-and-islands LOCF with `count() OVER` + `first_value()`, `array_agg(... ORDER BY)[1]` for `last()`, and `date_bin` with origin `2000-01-03` (§5). |

Secondary findings that materially change scope: the `transformed` schema (13 hypertables plus
PL/pgSQL parity functions that iterate `timescaledb_information.chunks` at runtime) is missing from
the plan entirely (§1.2); Phase 2 cannot precede Phase 3 as the dependency graph claims (§3); the
rollback story for Phase 5 ignores non-replayable tables (§7); and the 7–9 day total is roughly a
quarter of what the work actually is (§8).

---

## 1. Completeness

### 1.1 Inventory corrections

| Plan says | Tree says | Consequence |
|-----------|-----------|-------------|
| ~143 migration files | **162** `.sql` files in `stl-verify/db/migrations/` | Baseline size; nothing else. |
| "30+ migrations, ~50 hypertables" | 19 files use `WITH (tsdb.hypertable …)` (38 distinct tables by a multi-line parse; a few dimension tables in that list are regex over-reach, so treat it as ~35), 9 files use `create_hypertable()` (10 tables), plus **13 in `transformed.*`** from `20260706_140000_create_transformed_bucket1.sql`. | Get the authoritative list from `timescaledb_information.hypertables` on staging and pin it in the plan; the grep-derived list is the wrong source of truth. |
| `time_bucket_gapfill` + `locf`: 7 call sites | 6 `time_bucket_gapfill` statements (`prime_debt` 2, `prime_capital_stack` 1, `allocation_position` 3) wrapping **15 `locf(last(...))` expressions**, one with a `FILTER` clause (`allocation_position_repository.py:2092`). Plus `time_bucket()` via `_time_window.time_bucket_expr` in `protocol_event_repository.py`. | The unit of rewrite is the `locf(last())` expression, and the `FILTER` variant needs its own equivalent (§5.2). |
| `~15` compression test files | **18** `*_integration_test.go` files reference `compress_chunk` / `show_chunks` / `timescaledb_information` / `DisableScheduledJobs` / `CREATE EXTENSION`. | Full list: run `grep -rlE 'compress_chunk\|decompress_chunk\|show_chunks\|timescaledb_information\|DisableScheduledJobs\|untier_chunk\|tiered_chunks\|CREATE EXTENSION' --include='*_test.go' stl-verify`. |
| `add_*_policy`: ~80 calls | 116 `add_tiering_policy` + 64 `add_compression_policy` + 11 `remove_compression_policy` + 1 `add_retention_policy` + 7 `alter_job` + 1 `add_dimension` (`by_hash('chain_id', 4)` on `block_states`) + 1 `set_integer_now_func` + 1 `set_chunk_time_interval` | Matters only for judging the shim (§2.1); irrelevant once squashed. |
| Migrations reference catalog views: not listed | 8 `timescaledb_information.chunks`, 5 `.jobs`, 4 `.dimensions`, 2 `timescaledb_osm.tiered_chunks`, 1 `.chunk_columnstore_settings`, plus `compress_chunk`/`decompress_chunk`/`show_chunks`/`untier_chunk` **inside migrations** (`20260410_120000`, `20260410_140000`, `20260715_120000`, `20260706_140000`). | A function shim would also have to fake four catalog views. Another nail in B1. |
| Infra repo: 11 files listed | ~40 files match `tigerdata\|timescale` in `infrastructure/`: 4 GH workflows (`archon-{dev,staging,prod}-deploy.yml`, `ci.yml`, plus the drill), `alerts/orbit/orbit-tigerdata.yaml`, 5 runbooks, ADR-001/002/006/009/010/011/015, `docs/access-control-matrix.md`, `docs/security-hardening.md`, `authz/WRITE-PATH.md`, 4 diagrams, and the OpenMetadata runbooks. | Phase 6 needs a real sweep; §1.3 lists the consumers that are more than doc updates. |

### 1.2 Missing from the plan entirely

1. **`transformed` schema and its parity machinery.** `20260706_140000_create_transformed_bucket1.sql` (1,600+ lines, generated by `cmd/util/gen-transformed`) creates 13 hypertables and the functions `transformed._parity_refresh(_source)` / `_parity_verify_all(_source)`, whose bodies read `timescaledb_information.dimensions`, iterate `timescaledb_information.chunks` joined to `pg_stat_all_tables` to detect chunk activity, and probe `timescaledb_osm.tiered_chunks`. `CREATE FUNCTION` will succeed on vanilla PG (plpgsql does not resolve relations at definition time) and then **fail at first call** from `cmd/cronjobs/transform-worker` and `cmd/backfillers/transform-bootstrap/main.go:132`. The parity design (per-chunk activity baseline → re-count overlapping day-buckets) has no meaning without chunks and must be redesigned, not translated — e.g. a per-source `(day_bucket, n_tup_ins)` ledger keyed off `max(observed_at)` deltas, or drop chunk-activity gating and re-count the trailing N days each tick.
   - Also update the generator (`cmd/util/gen-transformed/emit.go:88-91` emits `create_hypertable`, `ALTER TABLE SET (timescaledb.compress…)`, `add_compression_policy`, `add_tiering_policy`; `consts.go:163-290` holds the parity bodies) or the next regenerated bucket reintroduces everything.
2. **`cmd/util/generate-er/main.go`** (39 lines) queries `timescaledb_information.hypertables`/`dimensions` behind a `pg_extension` check (line 316). Confirm the fallback path produces the ER output with no `[Hypertable]` annotations rather than an empty table set.
3. **`cmd/backfillers/transform-bootstrap/main.go:179-185`** treats an *unrecognized* `timescaledb.enable_tiered_reads` GUC as "no tiering, proceed". On vanilla PG a dotted unknown GUC is accepted as a placeholder, so that branch is dead and the `SET` silently succeeds. Harmless, but remove it and the comment block (lines 165-175) rather than leave misleading logic. Same for `block_meta_repository.go:226` (`SET LOCAL timescaledb.enable_tiered_reads = on`) and the 10 migrations whose `CREATE FUNCTION … SET timescaledb.enable_tiered_reads = 'on'` clauses will be carried into the baseline unless stripped.
4. **`block_states` 30-day retention** (`20260207_120000:59`, `add_retention_policy`). Nothing in the plan replaces it. A plain table that receives every block on six chains and is never pruned grows without bound; a cron `DELETE` on a large plain table means bloat and vacuum pressure, and it is a *delete channel* that `db/migrations/AGENTS.md` requires to be sanctioned explicitly. Recommendation: this is the one table with a genuine case for native range partitioning on `created_at` from day one (daily partitions, `pg_partman` — available on RDS — or a 20-line `DO` block in a Temporal cronjob) so retention is `DROP TABLE` of a partition. The `by_hash('chain_id', 4)` sub-dimension has no equivalent and no need for one.
5. **Compression → storage inflation.** TigerData "current usage" is the compressed, partially tiered footprint. Columnstore ratios on this kind of data are typically 5-15×. The RDS gp3 sizing and the "~100 GB: 2-4 hours" estimate are both unfounded until someone runs, on prod with `enable_tiered_reads = on`:
   `SELECT hypertable_name, hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)) FROM timescaledb_information.hypertables;` plus `SELECT * FROM hypertable_compression_stats(...)` (`before_compression_total_bytes`) and the tiered bytes from `timescaledb_osm.tiered_chunks`. Add index sizes on top (uncompressed indexes are typically 30-60% of heap). Put the numbers in the plan before choosing an instance class.
6. **Column `COMMENT`s.** `db/migrations/AGENTS.md` makes `[Hypertable]` a type tag in `COMMENT ON TABLE` and calls comments "the catalogue's source of truth". The baseline should rewrite every `[Hypertable]` tag to `[Timeseries]`, and `Partition` roles on columns should be reviewed. `data_quality/schemamaster/schema_master.json` and its conformance test should be regenerated from the baseline schema.
7. **Read replica.** `02_tigerdata.tf:172` provisions `timescale_service.readonly_replica`, and the PITR drill's `VERIFY_URL` targets it. Who else reads from it (OpenMetadata? Grafana datasource? the Python API)? The plan provisions one RDS instance. Decide: RDS read replica, or drop the replica and prove nothing depends on it.
8. **Connection pooling.** There is a `tigerdata-connection-saturation.md` runbook, so connection count is a known pressure point. RDS has no built-in pooler; decide on RDS Proxy vs. in-cluster pgbouncer before sizing `max_connections`.
9. **Database name.** TigerData's database is `tsdb` (`bootstrap-db.sh:43`, drill URLs `/tsdb?sslmode=require`); local is `stl_verify`. Choose the RDS name deliberately and grep every DSN, drill script and runbook for the literal.
10. **`GRANT SET ON PARAMETER temp_file_limit TO PUBLIC`** (`k8s/dev-infra/jobs/bootstrap-db.yaml:42`, `materialize_aave_lending`). On RDS you are `rds_superuser`, not superuser; verify that grant is permitted and that every superuser-only GUC pinned by a migration function still resolves.
11. **OpenMetadata and FinOps.** `docs/runbook/openmetadata-*.md` and ADR-010 (cost dashboard) integrate with TigerData. Both are consumers with credentials and connectors to re-point; neither is in Phase 6.

### 1.3 Correct but understated

- `blockstate_repository.go:113` — the comment says SERIALIZABLE was avoided because of chunk-level constraints. Once the table is plain, re-evaluate whether the isolation-level workaround is still needed; do not just reword the comment.
- `uniswap_v4_repository.go:294,331` and `core_model_positions_reader.py:103-118,301` explain query shapes chosen to avoid chunk fan-out or tiering misses. With plain tables some of those shapes (e.g. avoiding `DISTINCT ON` over history) may no longer be the fastest. Not a migration blocker, but list them as follow-up performance reviews.

---

## 2. The shim is infeasible; squash to a baseline instead

### 2.1 Why no shim works

- `CREATE TABLE … WITH (tsdb.hypertable, tsdb.partition_column = 'x', tsdb.chunk_interval = …)` (39 occurrences, 19 files) and `ALTER TABLE … SET (timescaledb.compress, timescaledb.compress_segmentby = …, timescaledb.compress_orderby = …)` (56/54/61 occurrences, 24 files) are reloptions. Vanilla PG's reloption parser accepts only the `toast` namespace for heap tables and errors with `unrecognized parameter namespace "tsdb"` before any user code runs. No function, event trigger, or extension-less SQL object can intercept it.
- Even the function surface is not a handful of no-ops: `create_hypertable` has two overloads (old positional form and `by_range()`/`by_hash()` returning `_timescaledb_internal.dimension_info`), and migrations *read* four catalog views (`timescaledb_information.chunks/jobs/dimensions/chunk_columnstore_settings`) and call `compress_chunk`/`show_chunks`/`untier_chunk`. `DO … EXCEPTION WHEN undefined_function` guards exist only around `add_tiering_policy`.
- Tests assert TimescaleDB behaviour, not just schema: the VEC-570 effective-policy-interval assertions, `TestHypertableCheckConstraintsSurviveTheTieringRoundTrip`, `compressed_chunk_versioning_integration_test.go`. A shim would leave them green while asserting nothing.
- A migrator-side regex that strips `WITH (tsdb…)` from file contents before execution is the only mechanical alternative, and it means executing SQL that differs from what the checksum covers, on 1,600-line PL/pgSQL bodies. Do not do this.

### 2.2 Baseline design

The migrator already supports this with one small change. `Migrator.getMigrationFiles` (`db/migrator/migrator.go:89`) lists only files on disk and `verifyChecksum` is only called for files on disk, so **rows in `migrations` for deleted files are inert**. Squashing is safe for every existing database.

1. **Add a `-- migrate: baseline` directive** next to `noTransactionDirective`: if the `migrations` table exists and is non-empty when this file is reached, `INSERT INTO migrations (filename, checksum)` for it and skip execution; otherwise execute normally. Ten lines in `applyMigration`, plus a unit test beside `split_statements_test.go`. This is what lets staging/prod (which have the full history) and fresh CI/dev/RDS (which have none) run the same file set.
2. **Generate `00000000_000000_baseline.sql`** (sorts first) from a throwaway timescale container, not by hand:
   - Start `timescale/timescaledb:2.29.2-pg18` as CI does (superuser, no OSM), run all 162 migrations.
   - `pg_dump --schema-only --no-owner --exclude-schema='_timescaledb_*'`. Hypertable parents dump as ordinary `CREATE TABLE` with their indexes, constraints, triggers, grants and comments; compression/tiering settings are catalog state and do not appear. Chunks are excluded by the schema filter.
   - Post-process, and make the post-processor a checked-in script so it is reproducible: drop `CREATE EXTENSION`/`COMMENT ON EXTENSION timescaledb`; drop every `ts_insert_blocker` trigger (`EXECUTE FUNCTION _timescaledb_functions.insert_blocker()`); drop `*_integer_now()` functions; strip `SET timescaledb.enable_tiered_reads` clauses from function definitions; replace the four chunk-reading function bodies (§1.2 item 1) with their redesigned versions; rewrite `[Hypertable]` comment tags; append the self-registration `INSERT`.
   - `pg_dump --data-only` for the seeded reference/dimension tables (`oracle_asset` has 66 seed inserts, `token` 23, plus the `ref_*` vocabularies and `entity_master`) and append it. Exclude `migrations`.
   - Grants: keep `--no-owner` but **not** `--no-privileges`; `TestConvertedTablesAreAppendOnly` and `TestTriggerOnlyCachesGrantTheAppRoleNoWrite` depend on the `REVOKE`s and `ALTER DEFAULT PRIVILEGES` surviving. Note that `ALTER DEFAULT PRIVILEGES` is per-role and pg_dump emits it only for the dumping role's view; verify it lands.
3. **Move the 162 old files to `db/migrations/archive/`** (the migrator skips directories) so history and blame survive; delete nothing from git.
4. **Verify equivalence**, twice: (a) apply the baseline on `postgres:18` and diff `pg_dump --schema-only` (normalised) against the throwaway's post-processed dump; (b) run the `data_quality/schemamaster` conformance test and the whole `db/migrator` suite.
5. **Audit the 37 `db/migrator/*_integration_test.go` files.** Many test a *specific migration's data transformation* by seeding rows and then applying it (`vec535_rekey`, `position_daily_materializer`, `utc_normalize_enrichment`, `oracle_asset_append_on_change`, `materialize_*`, `secstore_wave1`, `position_current_created_at`). After squashing those migrations no longer run as steps and the tests lose their subject. Each needs a decision: delete (behaviour is now baseline state), or convert into a test of the resulting function/procedure (`rebuild_position_current()` etc.). Budget this; it is not a grep-and-delete.
6. **Add a CI lint** (`ci/check-*.sh` alongside `check-ci-services.sh`) that fails any migration containing `tsdb\.|timescaledb|create_hypertable|_policy\(`. With no shim there is no silent success path, but the lint gives a clear error instead of a parse error on line 400 of a DO block.

### 2.3 What happens to the plan's Phase 3 in-place conversion

Delete it. It was needed only to run forward on TigerData environments, which (B3) it cannot do as `stl_migrator`, and which (§4) will not be the source of the production schema anyway. The only place hypertables need to become plain tables is inside the throwaway container in step 2 above, where `pg_dump --schema-only` does it for free.

Also update `db/migrations/AGENTS.md:23`: the "NEVER modify an applied migration" rule and the checksum exception remain true, but add the baseline rule ("history before `<date>` is squashed into `00000000_000000_baseline.sql`; the archive is read-only reference").

---

## 3. Ordering and dependency graph

The graph in §12 of the plan has Phase 2 (vanilla test image) before Phase 3 (migrations runnable on vanilla). That is inverted: the moment `images.go` and `conftest.py` point at `postgres:18`, every migration test fails at `20260122_143000_initial_schema.sql`'s first hypertable. Phase 2 and Phase 3 are one PR, and it is the biggest PR of the project.

Recommended order (each line is independently mergeable and leaves CI green):

1. **Inventory PR** (Phase 0, currently missing content): authoritative hypertable list, tiered/compressed/uncompressed byte counts per table from prod, list of non-replayable tables (§4.3), Redis payload TTL, target RDS class. No code.
2. **App code, still on TimescaleDB** (Phase 1): gapfill rewrites with the differential test in §5.5 running *on the timescale image* (the only place both old and new SQL can execute side by side); `block_meta_repository` rewrite; `transformed` parity redesign + generator; `generate-er`; remove `DisableScheduledJobs` callers; entity comments. Deploy to staging/prod — the plan's claim that new code runs on either schema must be *true* here, so the differential test is what proves it.
3. **Baseline + image flip + test audit** (Phases 2+3 merged): migrator directive, baseline file, archive move, `postgres:18` in `images.go`/`go-ci.yml`/`check-ci-services.sh`/`conftest.py`/`python-ci.yml`, delete the 18 TimescaleDB-specific tests, rework the migrator suite. Merging this to `main` deploys the migrator to staging: the baseline directive must correctly no-op there. That is the first production-adjacent test of the directive — do it on a kind cluster carrying a `pg_dump` of staging's `migrations` table first.
4. **Dev-infra** (Phase 4) can ride along with 3 or follow; note `k8s/dev-infra/timescaledb.yaml` is still on `2.25.1-pg17` while CI is on `pg18`, so `make dev-wipe` is mandatory regardless.
5. **RDS provisioning** (Phase 5.1) genuinely is parallel with 2-4 and should start first because it has the longest lead time (VPC/SG/KMS/secrets/pod identity, and the infra repo's PR cadence). Provision staging RDS empty, run the migrator (post-step 3) against it, and use it as the rehearsal target.
6. **Staging data rehearsal, twice** (Phase 5.2) measuring COPY throughput and validating row counts — this is where the "2-4 hours" number gets replaced by a measurement.
7. **Prod cutover** (Phase 5.2/5.3) using the block-height design in §4.3.
8. **Cleanup** (Phase 6) after the soak, with the full ~40-file infra sweep (§1.1).

---

## 4. Data migration

### 4.1 Why `pg_dump` cannot be the mechanism

- **Tiered chunks are not in PostgreSQL storage.** A tiering policy moves chunks to S3; they disappear from `timescaledb_information.chunks` and appear in `timescaledb_osm.tiered_chunks` (`block_meta_repository.go:148-152` documents exactly this). `pg_dump` walks `pg_class`; tiered chunks are not there. Any read of a hypertable without `timescaledb.enable_tiered_reads = on` silently excludes them (default off). Four of the six block_meta source tables carry a one-year tiering policy, and there are 116 `add_tiering_policy` calls in total.
- **Compressed chunks dump as compressed.** The internal `_timescaledb_internal.compress_hyper_*` tables hold bytea batches; restoring those into vanilla PG gives you tables full of blobs.
- "Process the dump: remove TimescaleDB extension commands, convert hypertable DDL" is therefore not a transformation of a working dump; there is no working dump to transform.
- **Logical replication is not a fallback.** Hypertable chunks are inheritance children, not declarative partitions, so a publication on the parent replicates nothing and `publish_via_partition_root` does not apply; compressed chunks do not decode to row changes. Strike Open Question 6.

### 4.2 Mechanism that works

1. Migrator creates the schema on RDS from the baseline (§2). No stripping, no dump post-processing in the production path at all.
2. Per table, on a bastion/migration pod with `SET timescaledb.enable_tiered_reads = on` and one `REPEATABLE READ` snapshot per table (or one for all chain tables if the cutover design below needs a consistent block height):
   `psql TIGER -c "\copy (SELECT * FROM t WHERE <bound>) TO STDOUT BINARY" | psql RDS -c "\copy t FROM STDIN BINARY"`.
   Reading through the hypertable parent decompresses and un-tiers transparently. Parallelise by table; for the few very large tables (`protocol_event`, `block_states`, `allocation_position`, `position_state`, the `transformed.*` set) split by partition-column range.
3. For the largest tables, consider dropping secondary indexes on RDS before load and recreating after; measure on the staging rehearsal whether it pays.
4. Exclude `migrations` (owned by the migrator on RDS) and the run-scratch `block_meta_worklist` (`UNLOGGED`, rebuilt per run).
5. `ANALYZE` everything; `*_current` caches were loaded as data, not via their triggers, so run each `rebuild_*` procedure (`rebuild_position_current()` and siblings) and compare to the copied rows as a consistency check.
6. Validation: per-table `count(*)` **with tiered reads on** on both sides; per-table `max(partition_col)`; a checksum query on a sample of identity keys; the smoke tests in the plan.

### 4.3 Cutover design that shrinks downtime

The plan's "stop all writers, dump, restore, resume" treats the system as an opaque OLTP store. It is a block indexer with backfillers and a `backfill_watermark` gap filler; chain-derived history is *replayable*. Use that:

- **Chain-derived tables** (partition column `block_timestamp`/`block_number`/`created_at`, ~30 tables): copy everything up to a per-chain block height `N` while the system keeps running. After cutover, seed `backfill_watermark` on RDS to `N` per chain and let the gap filler / `block-republisher` re-index `N..head`. Downtime for these is zero.
- **Non-replayable tables** (partition column `synced_at`/`timestamp`/`snapshot_time`/`observed_at`: `cex_orderbook_snapshots`, `anchorage_*`, `maple_*`, `offchain_price_*`, `core_model_results`, `prime_reference_*`, and the Temporal cronjob outputs generally): these are API snapshots that cannot be re-fetched for the past. They need either a short stop-copy-start window (they are small relative to chain data — measure) or a brief dual-write. This is the actual downtime, and it is minutes if the copies are pre-staged and only the delta is copied at cutover.
- **Redis TTL constraint**: workers read block payloads from Redis, not Alchemy (`stl-verify/AGENTS.md`, cache key convention). Any pause longer than the payload TTL means the queued SQS pointers reference evicted payloads; the plan's 2-4 hour stop may already exceed it. Either confirm the TTL covers the window, or rely on the backfill path for the gap and do not try to drain the paused queues.
- **Secret rollout**: Reloader (`k8s/AGENTS.md`) rolls every staging/prod Deployment when the ESO-synced secret changes, so "update secrets" *is* the redeploy, gated by ESO's refresh interval. Write the exact sequence (ESO refresh, pod rollout order: watchers last) into a cutover runbook and rehearse it on staging.

### 4.4 Row-count "match within tolerance"

Reject tolerance. Counts must match exactly when both sides are read at the same snapshot with tiered reads on. A tolerance hides precisely the tiered-chunk omission in §4.1.

---

## 5. Gapfill replacement correctness

### 5.1 `IGNORE NULLS` is PostgreSQL 19

Commit "Add IGNORE NULLS/RESPECT NULLS option to Window functions" landed 2025-10-03 for PG19 (see depesz, "Waiting for PostgreSQL 19"). CI runs `2.29.2-pg18`; RDS ships PG18 (18.1 GA, 18.6 as of Aug 2026). The plan's Option (a) as written is a syntax error on the target. Replace the LOCF with the standard gaps-and-islands form, which works on every supported PG:

```sql
WITH series AS (
    SELECT g AS bucket_start
    FROM generate_series(date_bin(:w, CAST(:from_timestamp AS timestamptz), :origin),
                         CAST(:to_timestamp AS timestamptz), :w) g
),
agg AS (
    SELECT date_bin(:w, pd.synced_at, :origin) AS bucket_start,
           (array_agg(pd.debt_wad ORDER BY pd.synced_at DESC, pd.block_number DESC,
                                            pd.block_version DESC, pd.processing_version DESC))[1] AS debt_wad
    FROM prime_debt pd
    WHERE pd.prime_id = :prime_id AND pd.synced_at >= :from_ts AND pd.synced_at <= :to_ts
    GROUP BY 1
),
joined AS (
    SELECT s.bucket_start, a.debt_wad,
           count(a.debt_wad) OVER (ORDER BY s.bucket_start) AS grp   -- increments on each non-NULL
    FROM series s LEFT JOIN agg a USING (bucket_start)
)
SELECT bucket_start,
       first_value(debt_wad) OVER (PARTITION BY grp ORDER BY bucket_start) AS debt_wad
FROM joined
ORDER BY bucket_start DESC
LIMIT :limit;
```

The LOCF must be computed over the full series *before* `LIMIT` (as above), otherwise the first returned bucket after a gap has nothing to carry.

### 5.2 `last()` is a TimescaleDB aggregate

`locf(last(x, t))` needs two replacements, and the plan's snippet (`last_value(col)`, a window function) is not one of them. Use `(array_agg(x ORDER BY t DESC, <tiebreaks>))[1]`. For the `FILTER` site (`allocation_position_repository.py:2092,2098`): `(array_agg(x ORDER BY t DESC) FILTER (WHERE x IS NOT NULL))[1]`.

Side effect worth reviewing on its own: the code comments at `allocation_position_repository.py:82-91, 927, 2064, 2331` and `prime_capital_stack_repository.py:111,168` describe `last()`'s undefined tie-break as a known hazard. `array_agg` with a full `ORDER BY` on the version tuple makes the pick deterministic. That is an improvement, but it is a behaviour change that the differential test (§5.5) will surface as diffs on tied rows; decide up front that the deterministic answer is the accepted one.

### 5.3 `date_bin` origin

`time_bucket()` on `timestamptz` aligns to `2000-01-03 00:00:00+00` (a Monday), not the epoch. The plan's `'1970-01-01'` origin (a Thursday) produces identical buckets only when the width divides 86,400 s. For any multi-day or weekly width, buckets shift. Use `'2000-01-03 00:00:00+00'::timestamptz` as the origin so existing API consumers see the same bucket boundaries. `date_bin` also rejects intervals with month/year parts; `make_interval(secs => …)` is fine.

### 5.4 Series semantics to pin down

`time_bucket_gapfill(w, col, start, finish)` emits buckets for the half-open range `[start, finish)` **per GROUP BY key**. Two of the six sites group by more than the bucket (per-position / per-feed series). The replacement must `CROSS JOIN` the series with the distinct group keys, and the `count() OVER` must be `PARTITION BY <group key> ORDER BY bucket_start`. `app/domain/gap_policy.py:69` already models "every bucket gapfill generates"; make the new SQL's `generate_series` bounds match that model exactly rather than the `required_time_window_clause` bounds (`<=` on the upper side), and fix whichever one is wrong.

### 5.5 Differential test, before the image flips

Add a temporary integration test that runs both the old (`time_bucket_gapfill`) and new (`generate_series`) query against the same fixture on the timescale image and asserts identical rows, for every one of the six sites, with fixtures that include: a leading gap (no data before the first bucket → NULL, not carried), an interior gap spanning ≥2 buckets, a `to_timestamp` exactly on a bucket boundary, ties on `synced_at`, the `FILTER` case, and a multi-key group. This test is deleted in the image-flip PR. It is the only moment in the project where both engines are available side by side; the plan's "integration tests compare old vs new output" is listed as a mitigation but has no phase in which it can actually run.

Also update the `_time_window.time_bucket_expr` docstring and the two domain modules' comments (`gap_policy.py`, `time_series.py:65`) in the same PR.

---

## 6. Performance without chunk exclusion

The plan's assumption ("with plain tables, the planner has one table") is right about *planning* cost and silent about *execution* cost and *storage*.

1. **Index audit.** Chunk exclusion prunes on the partition column with no index. On a plain table the same predicate needs a btree on the partition column (often composite with the identity key) or it is a seq scan. Before the flip, for every hypertable, list the hot predicates (`WHERE <partcol> > …` from repositories, the `*_current` trigger lookups, the `assign_processing_version_*` `MAX()` lookups) and confirm an index leads with the columns they filter on. Most tables already have identity-leading indexes for `ON CONFLICT`; the time-leading ones are the likely gaps.
2. **Storage and cache.** Decompressed data is 5-15× larger and so are its indexes. `effective_cache_size`/`shared_buffers` sizing follows from §1.2 item 5, and it likely pushes the instance class above the plan's `db.r7g.large`.
3. **`block_states`**: partition natively (§1.2 item 4). It is also the one table whose retention `DELETE` would otherwise be a continuous vacuum load on the hottest read path.
4. **`SET plan_cache_mode = 'force_custom_plan'`** on the `assign_processing_version_*` triggers (18 migration files, `TestProcessingVersionTriggersForceCustomPlan`). The reason (VEC-541: generic plans fan out over chunks) goes away, but the setting is harmless and removing it is a re-planning-cost trade in the other direction. Keep it in the baseline, delete the test, and revisit with `EXPLAIN` data later. Do not bundle a plan-cache change into the migration.
5. **`block_meta_repository` option (b) is not "unbounded query per arm".** The chunk windows bounded planning memory (`chunksPerWindow`, 6 MB per chunk) *and* transaction duration/lock scope per `INSERT … SELECT` into `block_meta_worklist`. Keep windowing, derive it from `min(partCol)`/`max(partCol)` split into fixed-width ranges (or keyset over the partition column), and drop the two catalog probes and the `SET LOCAL` GUC. Then `partCol` is static per arm as the plan says, but keep it as a struct literal rather than reading `schema_master.json` — the arms are already declared in Go.
6. **Native partitioning elsewhere: defer** (Open Question 4). Plain tables with the right indexes should hold at this data volume; add partitioning to a table only with an `EXPLAIN (ANALYZE, BUFFERS)` showing why, per the existing "measurement first" rule the plan is otherwise preserving.

---

## 7. Rollback realism

- **Phases 1-2 (code)**: fine as stated, *provided* the differential test in §5.5 proves the new SQL is correct on TimescaleDB, because that is where the new code first runs in prod.
- **Phase 3 (baseline)**: the plan says "revert the branch". After the baseline directive has inserted its row into `migrations` on staging/prod, a revert that restores the 162 old files sees them all as applied and the baseline row as an orphan — harmless. On CI/dev the revert simply re-runs history. So this rollback is real, but only because the directive never *executes* on populated databases (§2.2 step 1). Test that property explicitly.
- **Phase 5 (cutover)**: "point secrets back at TigerData" restores an instance that has been frozen since cutover. Everything indexed on RDS in between is either replayable (chain tables: re-run backfill on TigerData for the gap) or **lost** (the non-replayable API snapshot tables in §4.3). The plan must say this, define the rollback window during which that loss is acceptable (24 h? 72 h?), and after that window declare RDS the system of record with no rollback. Keep TigerData running read-only until then, then take the final snapshot and decommission.
- **TigerData's own protections**: before cutover, take a manual TigerData backup / note the PITR window, and confirm with the vendor how long a paused/deleted service's PITR data remains recoverable.
- **Phase 6**: "no rollback needed" is only true after the window above closes and the final snapshot is verified restorable somewhere. Add that as an explicit gate.

---

## 8. Timeline

The plan totals 7-9 engineering days. The re-scoped work:

| Work | Plan | Realistic |
|------|------|-----------|
| Phase 0 inventory (sizes, non-replayable list, TTLs, authoritative hypertable list) | not budgeted | 2-3 days |
| Phase 1 app code: 6 gapfill sites + differential test, `last()` semantics review, `block_meta` rewrite, `transformed` parity redesign + generator + `transform-bootstrap`, `generate-er`, `DisableScheduledJobs` callers, comments | 1-2 days | 2-3 weeks |
| Phases 2+3: migrator directive, baseline generation script, post-processor, equivalence checks, image flip, 18 test deletions, 37-file migrator test audit | 1.5 days | 1-1.5 weeks |
| Phase 4 dev-infra | 0.5 day | 0.5-1 day (fine) |
| Phase 5.1 RDS provisioning (infra repo PR cadence, SG/KMS/secrets/pod identity, pooler decision, replica decision) | part of 2-3 days | 1 week, started first |
| Phase 5.2 staging rehearsals ×2 with measurements, cutover runbook | part of 2-3 days | 1-1.5 weeks |
| Staging soak | 48 h | 2 weeks minimum (the plan's own Phase 6 gate says 2+ weeks, contradicting the 48 h in the matrix) |
| Prod cutover + rollback window | — | 1 day + the window |
| Phase 6 sweep (~40 infra files, new ADR superseding ADR-006, dashboards, alerts, 5 runbooks, OpenMetadata/FinOps re-pointing) | 1 day | 1 week |

Call it **6-8 weeks calendar for one engineer plus infra review**, with the differential test and the first staging rehearsal as the two checkpoints where the estimate gets revised.

---

## 9. Risk register additions

| Risk | Impact | Mitigation |
|------|--------|------------|
| Tiered chunks silently excluded from the export | Data loss, undetectable by tolerant counts | §4.2 mechanism; exact counts with `enable_tiered_reads = on` on both sides |
| `transformed` parity functions error at first cronjob tick after the flip | transform-worker down | §1.2 item 1 redesign, tested on the vanilla image |
| Storage 5-15× TigerData's reported size | Under-provisioned RDS, slow load | §1.2 item 5 measurement before sizing |
| Non-replayable snapshot tables during downtime / rollback | Permanent gaps in CEX/Anchorage/Maple/offchain-price series | §4.3 stop-copy-start or dual-write; rollback window defined |
| Redis payload TTL shorter than the pause | Workers cannot process queued pointers | Confirm TTL; plan on backfill covering the gap |
| Baseline directive executes on a populated DB | Attempts to re-create every table on staging | Unit test + kind rehearsal against a copy of staging's `migrations` table |
| `last()` tie-break becomes deterministic | API values change on tied rows | Decide and document before the differential test; review diffs |
| Weekly/multi-day bucket boundaries shift | Chart discontinuity for API consumers | `date_bin` origin `2000-01-03` |
| RDS lacks superuser | Migration functions pinning superuser GUCs fail | Verify `GRANT SET ON PARAMETER` and every `SET` clause on the staging RDS before the rehearsal |
| Connection saturation without a pooler | Known incident class recurs | RDS Proxy / pgbouncer decision in Phase 5.1 |

---

## 10. Open questions to add (and one to strike)

- Strike Q6 (logical replication): not viable for hypertables (§4.1).
- Add: which workloads read from `timescale_service.readonly_replica`, and is an RDS read replica required at cutover or later?
- Add: RDS Proxy vs pgbouncer vs none, with the `tigerdata-connection-saturation` runbook as input.
- Add: RDS database name (`tsdb` vs `stl_verify`) and the DSN literal sweep.
- Add: Redis block-payload TTL and its relation to any cutover pause.
- Add: rollback window length for Phase 5 and the acceptance of non-replayable loss inside it.
- Add: which migrator-suite tests survive the squash (§2.2 step 5) — decide per file, not as a class.
- Add: does OpenMetadata / the FinOps exporter / Grafana connect to TigerData directly, and who re-points them.
- Q1 (PG version): PG18 — CI already runs it and RDS ships it. Q2 (Aurora): Aurora PG18 is still preview per the Dec 2025 announcement; RDS PostgreSQL 18 is GA. Choose RDS unless the preview has since gone GA in the target region.

---

## 11. Suggested rewrite of the phase table

| Phase | What | Gate to exit |
|-------|------|--------------|
| 0 | Inventory: authoritative hypertable list, per-table bytes (local/compressed/tiered), non-replayable table list, Redis TTL, RDS class + pooler + replica decisions | Numbers in the doc |
| 1 | App code on TimescaleDB, incl. differential gapfill test, `transformed` redesign, generators | Differential test green on timescale image; deployed to prod |
| 2 | RDS provisioned empty (staging, then prod), started in parallel with 1 | Bastion `psql` works; `GRANT SET ON PARAMETER` verified |
| 3 | Baseline + migrator directive + image flip + test audit + dev-infra | CI green on `postgres:18`; directive no-ops against a copy of staging's `migrations`; migrator populates staging RDS |
| 4 | Staging rehearsal ×2: per-table COPY with tiered reads, exact counts, timing | Runbook with measured durations |
| 5 | Prod cutover per §4.3; rollback window; TigerData read-only | Window closed; final TigerData snapshot verified |
| 6 | Decommission + ~40-file infra sweep + ADR superseding ADR-006 + AGENTS.md rules | `terraform plan` clean; no `timescale` provider |

Sources for the external facts cited: [depesz — Waiting for PostgreSQL 19: IGNORE NULLS/RESPECT NULLS](https://www.depesz.com/2025/10/13/waiting-for-postgresql-19-add-ignore-nulls-respect-nulls-option-to-window-functions/), [pgsql-committers: Add IGNORE NULLS/RESPECT NULLS option to Window functions](https://www.postgresql.org/message-id/E1v4U9B-001BCJ-00@gemulon.postgresql.org), [Amazon RDS for PostgreSQL supports major version 18](https://www.amazonaws.cn/en/new/2025/amazon-rds-for-postgresql-supports-major-version-18/), [Amazon Aurora PostgreSQL 18.1 in preview](https://aws.amazon.com/about-aws/whats-new/2025/12/amazon-aurora-postgresql-18-1-rds-database-preview/), [Tiger Data docs — tiered storage and `enable_tiered_reads`](https://docs.timescale.com/use-timescale/latest/data-tiering/enabling-data-tiering/), [Tiger Data docs — untier_chunk](https://docs.timescale.com/use-timescale/latest/data-tiering/tour-data-tiering/).
