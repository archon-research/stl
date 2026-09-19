# Migration Plan: TimescaleDB/TigerData to Vanilla PostgreSQL

**Status**: REVISED — Phase 3 superseded by baseline squash (see critiques)
**Date**: 2026-09-19
**Scope**: Both repos (`stl` and `infrastructure`)

> **Post-critique revisions**: This plan was critiqued by Fable 5.1
> ([003-fable-critique.md](003-fable-critique.md)) and self-review
> ([002-plan-critique-and-revisions.md](002-plan-critique-and-revisions.md)).
> Key changes:
> - Phase 3 "compatibility shim" replaced by **baseline squash** (generate one
>   baseline.sql from pg_dump, archive 162 migration files)
> - `LAG IGNORE NULLS` is PG19 — gapfill uses count-group LOCF instead
> - `first()`/`last()` are TimescaleDB aggregates — replaced with `array_agg`
> - Tiered chunks not in pg_dump — data migration uses parallel COPY
> - `transformed._parity_*` functions need redesign (read TimescaleDB catalogs at runtime)
> - `block_states` needs native range partitioning for retention replacement
> - Realistic timeline: **5-7 weeks**, not 7 days

---

## 1. Current State Inventory

### 1.1 TimescaleDB features in active use

| Feature | Where | Count | Replacement complexity |
|---------|-------|-------|----------------------|
| Hypertables (`create_hypertable` / `WITH tsdb.hypertable`) | 30+ migrations | ~50 tables | High — need new migration to revert |
| `time_bucket()` | Python query layer (`_time_window.py`, 3 repos) | ~6 call sites | Medium — `date_trunc` or `date_bin` (PG14+) |
| `time_bucket_gapfill()` + `locf()` | Python repos (allocation, prime_debt, prime_capital_stack) | 7 call sites | High — no vanilla equivalent, need app-level gapfill |
| `timescaledb_information.dimensions` | `block_meta_repository.go` | 1 call site | Medium — replace with `pg_catalog` or config |
| `timescaledb_information.chunks` / `timescaledb_osm.tiered_chunks` | `block_meta_repository.go` | 2 queries | Medium — replace with table-scan or PG partitions |
| `timescaledb_information.jobs` + `alter_job()` | `testutil/templatedb.go` | 1 call site | Low — remove entirely |
| `compress_chunk` / `decompress_chunk` / `show_chunks` | Integration tests only | ~15 test files | Medium — remove compression tests |
| `add_compression_policy` / `add_tiering_policy` / `add_retention_policy` | Migrations | ~80 calls | High — need new migration to remove |
| `REVOKE` propagation to hypertable chunks | Append-only enforcement | ~20 tables | Low — REVOKE on plain tables still works |
| `SET plan_cache_mode = 'force_custom_plan'` on triggers | 5+ trigger functions | ~5 | Low — remove (no chunks = no fan-out) |
| TigerData (Timescale Cloud) managed service | infrastructure repo | 1 service + replica | High — full RDS migration |
| VPC peering (TigerData ↔ AWS) | infrastructure repo | 3 resources | High — remove, add RDS subnet group |
| TigerData Prometheus exporter | infrastructure repo | 1 resource | Medium — replace with RDS + PG exporter |
| TigerData PITR drill | infrastructure repo | 1 GH workflow + script | Medium — replace with RDS PITR |
| Grafana dashboards (TigerData metrics) | infrastructure repo | 2 dashboards | Medium — replace with RDS/PG dashboards |
| Alerts (TigerData-specific) | infrastructure repo | 1 alert file | Medium — replace with RDS alerts |

### 1.2 TimescaleDB features NOT in use
- Continuous aggregates (zero usage anywhere)
- `time_bucket` in Go code (zero usage)
- Custom TimescaleDB data types

### 1.3 Three permanently-plain tables
`uniswap_v3_tick`, `uniswap_v4_tick`, `uniswap_v4_position` — already plain, no change needed.

---

## 2. Migration Strategy

### Guiding principle
**New forward migration, not history rewrite.** We add new migration(s) that convert hypertables back to plain tables, drop the extension, and remove policies. Applied migrations are immutable (checksum-tracked). The code changes are coordinated so that the new code runs against either the old schema (hypertable) or the new schema (plain) during the rollout window.

### Phase overview

| Phase | What | Where | Duration estimate |
|-------|------|-------|-------------------|
| **0** | Plan + setup | docs/migrate/ | This document |
| **1** | App code: remove TimescaleDB API dependencies | stl repo | 1-2 days |
| **2** | Test infra: switch to vanilla Postgres image | stl repo | 0.5 day |
| **3** | SQL migration: revert hypertables to plain tables, drop extension | stl repo | 1 day |
| **4** | k8s/dev-infra: switch local dev to vanilla Postgres | stl repo | 0.5 day |
| **5** | Infrastructure: provision RDS, migrate data, cut over | infrastructure repo | 2-3 days |
| **6** | Cleanup: remove TigerData resources, update docs/alerts | both repos | 1 day |

---

## 3. Phase 1 — Remove TimescaleDB API dependencies from app code

### 3.1 Go: `block_meta_repository.go` — partition column discovery

**Current**: Queries `timescaledb_information.dimensions` to find a hypertable's partition column.

**Replacement**: The partition column is always one of a known set (`block_number` or `created_at`). Replace the live catalogue lookup with a static configuration derived from `schema_master.json` or a hardcoded map. The `workListArm.partCol` struct already holds it per arm.

**Current**: Queries `timescaledb_information.chunks` and `timescaledb_osm.tiered_chunks` to build bounded windows for worklist queries (chunk-aware query planning).

**Replacement**: Without hypertable chunks, the worklist builder can either:
- (a) Use native PG table partitioning with `pg_catalog.pg_inherits` + range-bound lookup, or
- (b) Simplify to a single unbounded query per arm (the table is no longer chunked, so there's no planning-cost concern that motivated the windowing).

Option (b) is simpler and correct: the chunk-window optimization existed to avoid planning 355 chunks; with plain tables, the planner has one table.

**Files to modify**:
- `stl-verify/internal/adapters/outbound/postgres/block_meta_repository.go`

**Verification**: Integration tests for block_meta_loader already exist (`block_meta_worklist_integration_test.go`). They will validate the new code path.

### 3.2 Go: `blockstate_repository.go` — hypertable comments

**Current**: Comments reference hypertable chunk-level constraints. Code itself uses standard SQL.

**Replacement**: Update comments only. No functional change needed.

**Files to modify**:
- `stl-verify/internal/adapters/outbound/postgres/blockstate_repository.go` (comments only)

### 3.3 Go: `testutil/templatedb.go` — `DisableScheduledJobs`

**Current**: Calls `alter_job(job_id, scheduled => false)` from `timescaledb_information.jobs` to prevent compression jobs from firing mid-test.

**Replacement**: Remove the function entirely. With no TimescaleDB extension, there are no scheduled policy jobs.

**Files to modify**:
- `stl-verify/internal/testutil/templatedb.go`
- All callers of `DisableScheduledJobs` (grep for call sites)

**Verification**: All integration tests must pass without this call.

### 3.4 Go: Domain entity comments

**Current**: Several entity structs have comments mentioning "hypertable partition column" or "hypertable dedup".

**Replacement**: Update comments to reference the column's actual role (e.g., "time-series key" or "partition column").

**Files to modify**:
- `stl-verify/internal/domain/entity/allocation_position.go`
- `stl-verify/internal/domain/entity/borrower.go`
- `stl-verify/internal/domain/entity/borrower_collateral.go`
- `stl-verify/internal/domain/entity/orderbook_snapshot.go`
- `stl-verify/internal/domain/entity/protocol_event.go`

### 3.5 Python: Replace `time_bucket()` with `date_bin()` (PG14+)

**Current**: `_time_window.py:time_bucket_expr()` returns `time_bucket(make_interval(secs => :bucket_seconds), column)`.

**Replacement**: PostgreSQL 14+ has `date_bin(interval, source, origin)`:
```python
def time_bucket_expr(column: str) -> str:
    return f"date_bin(make_interval(secs => :bucket_seconds), {column}, '1970-01-01'::timestamptz)"
```

`date_bin` aligns to an origin (epoch) the same way `time_bucket` does by default.

**Files to modify**:
- `stl-verify/python/app/adapters/postgres/_time_window.py`

**Verification**: Unit test `test_gap_policy.py` + integration tests for allocation, protocol_event APIs.

### 3.6 Python: Replace `time_bucket_gapfill()` + `locf()` with app-level gapfill

**Current**: 7 call sites across 3 repository files use `time_bucket_gapfill(interval, column)` with `locf(last(...))` for last-observation-carried-forward time-series queries.

**Replacement strategy**: Two options:

**(a) generate_series + LEFT JOIN (pure SQL, no app change):**
```sql
SELECT gs.bucket, COALESCE(d.value, lag(d.value) OVER (ORDER BY gs.bucket)) AS value
FROM generate_series(:from, :to, make_interval(secs => :bucket_seconds)) AS gs(bucket)
LEFT JOIN (
    SELECT date_bin(...) AS bucket, last_value(col) ...
    FROM table WHERE ...
    GROUP BY 1
) d ON d.bucket = gs.bucket
```
Plus a window function `COALESCE(value, LAG(value IGNORE NULLS) OVER (...))` — but PG doesn't have `IGNORE NULLS` in window functions until PG17 (we're on PG18, so this works!).

Actually, PG18 should support `IGNORE NULLS` for `LAG`/`LEAD`. Confirm target PG version. If PG17+, use:
```sql
COALESCE(agg_value, LAG(agg_value IGNORE NULLS) OVER (ORDER BY bucket))
```

**(b) App-level gapfill in Python**: Generate the bucket series in Python, fetch the data with a plain `GROUP BY date_bin(...)`, then forward-fill in Python using pandas or a simple loop. The `gap_policy.py` module already models the bucket grid.

**Recommendation**: Option (a) keeps logic in SQL, which aligns with current architecture. PG17+ `IGNORE NULLS` makes LOCF trivial.

**Files to modify**:
- `stl-verify/python/app/adapters/postgres/allocation_position_repository.py` (4 sites)
- `stl-verify/python/app/adapters/postgres/prime_debt_repository.py` (2 sites)
- `stl-verify/python/app/adapters/postgres/prime_capital_stack_repository.py` (1 site)
- `stl-verify/python/app/adapters/postgres/_time_window.py` (update/add helpers)
- `stl-verify/python/app/domain/gap_policy.py` (update comments)
- `stl-verify/python/app/domain/time_series.py` (update comments)

**Verification**: Integration tests exist for all three repositories. Run `make test-integration` in `stl-verify/python/`.

### 3.7 Go: Remove compression-related test code

**Current**: ~15 integration test files exercise `compress_chunk`, `decompress_chunk`, `show_chunks`.

**Replacement**: Remove or simplify these tests. Tests that verify behavior *through* compression (e.g., "does a query work on compressed data?") become plain-table tests. Tests that test compression *itself* are deleted.

**Files to modify** (test files only):
- `stl-verify/db/migrator/compression_integration_test.go`
- `stl-verify/db/migrator/compressed_chunk_versioning_integration_test.go`
- `stl-verify/db/migrator/check_constraint_round_trip_integration_test.go`
- `stl-verify/internal/adapters/outbound/postgres/allocation_compressed_chunk_integration_test.go`
- Various `*_integration_test.go` that call `compress_chunk` in setup

**Verification**: `make test-integration` passes.

---

## 4. Phase 2 — Switch test infrastructure to vanilla PostgreSQL

### 4.1 Go test image

**Current**: `testutil/images.go` → `timescale/timescaledb:2.29.2-pg18`

**Replacement**: `postgres:18-alpine` (or `postgres:18`)

**Files to modify**:
- `stl-verify/internal/testutil/images.go`
- `.github/workflows/go-ci.yml` (service image)
- `stl-verify/ci/check-ci-services.sh` (tag validation)

**Verification**: `make test-integration` passes locally and in CI.

### 4.2 Python test image

**Current**: `conftest.py` → `TIMESCALEDB_IMAGE = "timescale/timescaledb:2.29.2-pg18"`

**Replacement**: `POSTGRES_IMAGE = "postgres:18-alpine"`

**Files to modify**:
- `stl-verify/python/tests/integration/conftest.py`
- `.github/workflows/python-ci.yml` (image grep pattern)

**Verification**: `make test-integration` in `stl-verify/python/`.

### 4.3 Test helpers

**Current**: `testutil/db.go` references `StartTimescaleDBForMain`/`startTimescaleDBContainer`.

**Replacement**: Rename to `StartPostgresForMain`/`startPostgresContainer`. Remove the `CREATE EXTENSION IF NOT EXISTS timescaledb` from `db/migrator/migrator_integration_test.go`.

**Files to modify**:
- `stl-verify/internal/testutil/db.go`
- `stl-verify/internal/testutil/runshared.go`
- `stl-verify/db/migrator/migrator_integration_test.go`
- All `main_integration_test.go` files that reference `TimescaleDSN` (rename to `PostgresDSN`)

**Verification**: All integration tests pass.

---

## 5. Phase 3 — Make migrations run on vanilla PostgreSQL

### 5.1 The core problem

143 migration files are checksum-tracked and immutable. 26+ of them use TimescaleDB-specific
DDL that vanilla PostgreSQL rejects:

| Pattern | Count | Shimmable? |
|---------|-------|------------|
| `CREATE TABLE ... WITH (tsdb.hypertable, tsdb.partition_column, tsdb.chunk_interval)` | 39 occurrences in 26 files | **NO** — storage parameters, not function calls |
| `ALTER TABLE ... SET (timescaledb.compress, timescaledb.compress_segmentby, ...)` | ~60 occurrences | **NO** — storage parameters |
| `SET LOCAL timescaledb.enable_tiered_reads = 'on'` | ~6 occurrences | **NO** — unknown GUC |
| `SET timescaledb.max_tuples_decompressed_per_dml_transaction` | 1 | **NO** — unknown GUC |
| `create_hypertable(...)` function calls | 26 occurrences in 10 files | Yes — function shim |
| `add_compression_policy(...)` | 64 occurrences | Yes — function shim |
| `add_tiering_policy(...)` | 116 occurrences | Yes — function shim |
| `add_retention_policy(...)` | 1 | Yes — function shim |
| `set_integer_now_func(...)` | 2 | Yes — function shim |
| `alter_job(...)` | 7 | Yes — function shim |
| `compress_chunk(...)` / `decompress_chunk(...)` | 6 | Yes — function shim |
| `show_chunks(...)` | 1 | Yes — function shim |
| `timescaledb_information.*` catalog views | ~15 references | Need shim views |

A pure function-shim approach covers only ~60% of the surface. The storage parameters and GUCs
require a different mechanism.

### 5.2 Solution: Migrator SQL pre-processor

Add a **pre-processing step** to the Go migrator that strips TimescaleDB-specific syntax when
running against vanilla PostgreSQL. The migrator already detects the database engine; add:

```go
func stripTimescaleDB(sql string) string {
    // 1. Strip storage params from CREATE TABLE WITH (...):
    //    Remove tsdb.* and timescaledb.* entries from WITH clauses.
    //    If the WITH clause becomes empty, remove it entirely.
    // 2. Strip storage params from ALTER TABLE SET (...):
    //    Remove entire ALTER TABLE ... SET (timescaledb.*) statements.
    // 3. Strip GUC settings:
    //    Remove SET [LOCAL] timescaledb.* statements.
    // 4. Leave function calls untouched (handled by shim functions below).
}
```

The pre-processor is a regex/parser that:
1. Removes `tsdb.*` and `timescaledb.*` key-value pairs from `WITH (...)` clauses
2. Removes entire `ALTER TABLE x SET (timescaledb.*)` statements
3. Removes `SET [LOCAL] timescaledb.*` statements
4. Removes `timescaledb.enable_tiered_reads` from function `SET` clauses

**Detection**: The migrator checks `SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'`.
If present (TigerData/staging/prod during transition), run SQL as-is. If absent (vanilla PG
in CI/dev), run through the pre-processor first.

**Checksum handling**: The checksum is computed on the **original** SQL, not the stripped version.
The migration tracking table records the original file's hash. The pre-processor is transparent
to the checksum system.

### 5.3 Function shims (bootstrap-installed)

For the ~60% that ARE function calls, install no-op shim functions before the first migration.
This replaces the current `CREATE EXTENSION IF NOT EXISTS timescaledb` in the bootstrap:

```sql
-- Shim functions for vanilla PostgreSQL compatibility
-- Installed by bootstrap, silently ignored when TimescaleDB is present

CREATE OR REPLACE FUNCTION create_hypertable(
    relation REGCLASS, time_column_name TEXT,
    chunk_time_interval ANYELEMENT DEFAULT NULL,
    migrate_data BOOLEAN DEFAULT FALSE,
    if_not_exists BOOLEAN DEFAULT FALSE
) RETURNS VOID AS $$ BEGIN END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_compression_policy(
    hypertable REGCLASS, compress_after ANYELEMENT,
    if_not_exists BOOLEAN DEFAULT FALSE
) RETURNS INTEGER AS $$ BEGIN RETURN 0; END; $$ LANGUAGE plpgsql;

-- ... similar for add_tiering_policy, add_retention_policy,
-- set_integer_now_func, alter_job, compress_chunk, decompress_chunk,
-- show_chunks, set_chunk_time_interval
```

Also create empty shim views:
```sql
CREATE SCHEMA IF NOT EXISTS timescaledb_information;
CREATE OR REPLACE VIEW timescaledb_information.dimensions AS
    SELECT NULL::TEXT AS hypertable_name, NULL::TEXT AS column_name,
           NULL::INT AS dimension_number LIMIT 0;
CREATE OR REPLACE VIEW timescaledb_information.chunks AS
    SELECT NULL::TEXT AS hypertable_name, NULL::BIGINT AS range_start_integer,
           NULL::BIGINT AS range_end_integer, NULL::TIMESTAMPTZ AS range_start,
           NULL::TIMESTAMPTZ AS range_end LIMIT 0;
CREATE OR REPLACE VIEW timescaledb_information.jobs AS
    SELECT NULL::INT AS job_id LIMIT 0;
```

### 5.4 Final migration: drop extension on TigerData

A new migration for the production cutover (runs on TigerData where the extension exists):

`YYYYMMDD_120000_drop_timescaledb_to_vanilla.sql` (`-- migrate: no-transaction`):

1. Drop all tiering policies (wrapped in exception handler)
2. Drop all compression policies
3. Drop the retention policy on `block_states`
4. Decompress all compressed chunks (`SELECT decompress_chunk(c) FROM show_chunks(...) WHERE ...`)
5. For each hypertable: the extension's internal `_timescaledb_functions.hypertable_to_table()`
   or, if unavailable, the manual approach: `CREATE TABLE new AS SELECT * FROM old; DROP TABLE old; ALTER TABLE new RENAME TO old; recreate indexes/constraints/triggers`
6. Drop shim functions if present
7. `DROP EXTENSION IF EXISTS timescaledb CASCADE`

**On vanilla PG** (CI/dev): This migration is a no-op — all guards use `IF EXISTS`.

### 5.5 Verification

- `make test-integration` with vanilla PG image — all 143 migrations apply cleanly
- `TestConvertedTablesAreAppendOnly` passes (REVOKE on plain tables works identically)
- `TestProcessingVersionTriggersForceCustomPlan` — adapt to skip `plan_cache_mode` check
  on vanilla PG (the SET is harmless but the `pg_proc` assertion needs updating)
- `TestHypertableCheckConstraintsSurviveTheTieringRoundTrip` — delete (no tiering)
- Migrator integration tests run the full sequence on both vanilla PG and TimescaleDB
  images to verify the pre-processor doesn't break the TimescaleDB path during transition

---

## 6. Phase 4 — k8s/dev-infra: switch local dev to vanilla PostgreSQL

### 6.1 StatefulSet image

**Current**: `k8s/dev-infra/timescaledb.yaml` → `timescale/timescaledb:2.25.1-pg17`

**Replacement**: `postgres:18-alpine` (match CI). Rename the StatefulSet to `postgres` (breaking change for anyone with existing local data — document `make dev-wipe`).

**Files to modify**:
- `k8s/dev-infra/timescaledb.yaml` → rename to `k8s/dev-infra/postgres.yaml`
- `k8s/dev-infra/kind.yaml` (hostPath, port mapping — update names)
- `k8s/dev-infra/jobs/bootstrap-db.yaml` (remove `CREATE EXTENSION`, add shim functions)
- `k8s/dev-infra/jobs/migrate.yaml` (update service name from `timescaledb` to `postgres`)
- `k8s/overlays/dev/config.yaml` (update `DATABASE_URL` host from `timescaledb` to `postgres`)
- `stl-verify/Makefile` (update all references: `timescaledb-0`, `statefulset/timescaledb`, hostpath names, `tigerdata-tunnel` targets)
- `k8s/AGENTS.md`, `k8s/README.md` (update references)

**Verification**:
- `make dev-up` starts successfully
- `make dev-migrate` runs all migrations
- `make run-watcher` + workers function correctly

---

## 7. Phase 5 — Infrastructure: provision RDS, migrate data, cut over

### 7.1 Provision RDS PostgreSQL

Add a new Terraform resource for an RDS PostgreSQL instance, mirroring the existing `auth_db` pattern but sized for the workload:

```hcl
resource "aws_db_instance" "vector" {
  identifier     = "${local.prefix}-vector-db"
  engine         = "postgres"
  engine_version = "18"
  instance_class = var.vector_db_instance_class  # e.g., db.r7g.xlarge
  # ... storage, networking, encryption, monitoring
}
```

**Key decisions**:
- **Instance class**: Match current TigerData sizing (2 CPU / 8 GB → `db.r7g.large` or `db.m7g.large`; prod has HA → Multi-AZ)
- **Storage**: gp3, start with current usage + headroom, enable autoscaling
- **Networking**: Same VPC as EKS, private subnets, security group allowing EKS pods
- **Encryption**: KMS, same key policy as auth_db
- **Monitoring**: Enhanced monitoring, Performance Insights, CloudWatch log exports
- **Backup**: Automated backups, 7-day retention (match current TigerData PITR), manual snapshots before cutover
- **Parameter group**: Custom PG18 parameter group tuned for time-series workload (shared_buffers, work_mem, effective_cache_size, maintenance_work_mem)

**Files to modify** (infrastructure repo):
- `infra/02_vector_db.tf` (new — replaces `02_tigerdata.tf`)
- `infra/variables.tf` (new vector_db variables)
- `infra/environments/*.tfvars` (sizing per environment)
- `infra/outputs.tf` (new vector_db outputs)
- `infra/05_secrets.tf` (new secret entries)
- `infra/01_security_groups.tf` (SG for RDS)
- `infra/04_service_iam_policies.tf` (update pod identity attachments)
- `infra/12_vector_pod_identity.tf` (update secret ARNs)
- `infra/scripts/bootstrap-db.sh` (update for RDS connection, remove `CREATE EXTENSION timescaledb`, add shim functions)

**Verification**:
- `terraform plan` shows clean additions
- `terraform apply` on staging creates the RDS instance
- Bastion can connect to RDS via psql
- Migrations run successfully against the new RDS instance

### 7.2 Data migration

**Strategy**: For staging, a fresh start may be acceptable (re-backfill). For production:

1. Stop all writers (scale deployments to 0)
2. `pg_dump` from TigerData (using the bastion or a migration pod)
3. Process the dump: remove TimescaleDB extension commands, convert hypertable DDL to plain CREATE TABLE
4. `pg_restore` into the new RDS instance
5. Run the new "drop extension" migration (no-op since the dump was cleaned)
6. Verify row counts match
7. Update secrets to point to new RDS
8. Scale deployments back up
9. Monitor for errors

**Downtime estimate**: Depends on data size. For ~100 GB: 2-4 hours.

**Alternative for zero-downtime**: Use logical replication from TigerData to RDS during transition, then cut over. This requires TigerData to support logical replication (confirm with vendor).

**Verification**:
- Row counts match between TigerData and RDS for every table
- Sample queries return identical results
- All workers start successfully and process new blocks
- API responses are correct (spot-check key endpoints)
- Grafana dashboards show data continuity

### 7.3 Update Kubernetes secrets and config

**Files to modify** (stl repo):
- `k8s/overlays/staging/external-secrets.yaml` (point at new secret ARNs)
- `k8s/overlays/prod/external-secrets.yaml`
- `k8s/overlays/staging/configmaps.yaml` (update comments)
- `k8s/overlays/prod/configmaps.yaml`

**Verification**: Pods connect successfully after secret rotation.

---

## 8. Phase 6 — Cleanup

### 8.1 Remove TigerData infrastructure

**Files to remove/modify** (infrastructure repo):
- `infra/02_tigerdata.tf` → delete (after RDS is stable and TigerData decommissioned)
- `infra/06_eks_vpc.tf` (remove TigerData peering sections)
- `infra/05_bastion.tf` (remove TigerData egress rules)
- `infra/30_tigerdata_drill_oidc.tf` → delete
- `.github/workflows/tigerdata-pitr-drill.yml` → delete or replace with RDS PITR drill
- `scripts/tigerdata-pitr-drill.sh` → delete or replace
- `infra/main.tf` (remove `timescale` provider)
- `infra/.terraform.lock.hcl` (regenerate without timescale provider)
- `gitops/tooling/archon-*/grafana-operator/dashboards/tigerdata.yaml` → replace with PG/RDS dashboards
- `alerts/orbit/orbit-tigerdata.yaml` → replace with RDS alerts
- `docs/runbook/tigerdata-*.md` → replace with RDS runbooks

### 8.2 Update documentation (stl repo)

- `AGENTS.md` (root) — remove hypertable/TimescaleDB references from cross-cutting rules
- `stl-verify/AGENTS.md` — update "Environment" section, data flow diagram
- `stl-verify/db/migrations/AGENTS.md` — remove all hypertable-specific rules (chunk exclusion, compression traps, tiering traps, plan_cache_mode). Keep append-only rules (they apply to plain tables too)
- `CONTRIBUTING.md` — update database references
- `docs/adr/0005-time-series-api-surface.md` — update for vanilla PG approach
- `docs/entity_relation.md` — remove hypertable annotations
- `alerts/` — update alert comments
- `docs/runbooks/` — update database runbook sections

### 8.3 Update AGENTS.md rules

Key rules to update:
- Remove: "A time window on a hypertable is a SQL literal" — still good practice but the hypertable reason is gone
- Remove: Compression policy trap, tiering round-trip trap
- Remove: `block_states` no-compression-policy rule
- Remove: "Foreign keys from a hypertable" rules
- Remove: `SET plan_cache_mode = 'force_custom_plan'` trigger rule
- Update: "Create every table plain; convert to hypertable later" → "Tables are plain PostgreSQL; use native PG partitioning only when measurement calls for it"
- Keep: All append-only rules, REVOKE rules, version tuple rules

---

## 9. Progress Tracker

### Done (branch `toreluntang/vec-na/timescaledb-to-vanilla-postgres`, 10 commits)

| Item | Commit | What changed |
|------|--------|-------------|
| Migrator pre-processor | `999ef11`, `8ca2152` | `tsdb_compat.go` strips `WITH (tsdb.*)`, `ALTER TABLE SET (timescaledb.*)`, `SET timescaledb.*` GUCs from migration SQL on vanilla PG. Checksums stay on original content. All 162 migrations pass. |
| `block_meta_repository` | `999ef11` | Detects TimescaleDB at runtime; falls back to `information_schema` for partition column, skips chunk-window optimization and tiered-reads SET on vanilla PG. |
| `time_bucket` → `date_bin` | `999ef11` | `_time_window.py`: `date_bin(make_interval(...), col, '2000-01-01'::timestamptz)` anchored at Postgres epoch matching `gap_policy._BUCKET_ORIGIN`. |
| `time_bucket_gapfill` + `locf` → vanilla SQL | `39e1283` | 7 call sites across 3 Python repos replaced with `generate_series` + `LEFT JOIN` + count-group LOCF (`count(d.col) OVER ... AS grp` → `first_value(col) OVER (PARTITION BY grp ...)`). `last()` → `(array_agg(... ORDER BY ts DESC))[1]`. PG18-compatible (no `IGNORE NULLS`). |
| Identifier rename | `9736fe9` | 56 Go files: `ImageTimescaleDB` → `ImagePostgres`, `StartTimescaleDBForMain` → `StartPostgresForMain`, `TimescaleDSN` → `PostgresDSN`, `timescaleDB` field → `postgres`. |
| CI images | `6988622` | `go-ci.yml` service → `postgres:18`. `python-ci.yml` grep → `POSTGRES_IMAGE`. `conftest.py` → `postgres:18`. |
| k8s dev-infra | `b499484` | `timescaledb.yaml` → `postgres.yaml` (postgres:18). Bootstrap removes `CREATE EXTENSION`. Migrate job, kind.yaml, dev config, Makefile all updated. |
| `DisableScheduledJobs` | `9736fe9` | Now detects TimescaleDB; no-op on vanilla PG. `templateFormat` bumped to 3. |
| Skip guards | `a11c4c5`, `666c4fc`, `18451d3` | `SkipWithoutTimescaleDB` helper + 48 skip guards across 13 test files covering all compression/tiering/chunk-specific tests. |
| Plan + critique docs | `999ef11`, `5ee3141` | `docs/migrate/001-003`: plan, self-review, Fable 5.1 critique. |

### Remaining (separate PRs)

#### Next up — can start now

| Item | Effort | Description |
|------|--------|-------------|
| **Baseline squash** | 3-5 days | Generate `00000000_000000_baseline.sql` from `pg_dump --schema-only` of a migrated TimescaleDB container. Move 162 migration files to `archive/`. Add `-- migrate: baseline` directive to migrator. Audit 37 `db/migrator/*_integration_test.go` files (many test specific migration transformations that lose their subject after squash). This is the **production migration path** — the pre-processor is transitional only. |
| **`transformed._parity_*` redesign** | 2-3 days | `_parity_refresh` and `_parity_verify_all` iterate `timescaledb_information.chunks` at runtime. They `CREATE FUNCTION` fine on vanilla PG then fail on first call from `transform-worker`. Replace with a day-bucket approach. Also update `cmd/util/gen-transformed/emit.go` so regeneration doesn't reintroduce hypertable DDL. |
| **`block_states` partitioning** | 1-2 days | The 30-day `add_retention_policy` disappears. Without replacement, `block_states` grows unbounded. Native range partitioning (daily, via `pg_partman` on RDS or a manual migration) makes retention `DROP PARTITION`. |
| **Phase 0 sizing** | 0.5 day | Run `hypertable_compression_stats()` and `timescaledb_osm.tiered_chunks` sizes on prod to get the actual uncompressed data volume. Drives RDS instance class, storage, and downtime estimate. |
| **RDS provisioning** | 1-2 days | Terraform in the infrastructure repo: `aws_db_instance`, parameter group, security groups, secrets, bootstrap script. Mirrors the existing `auth_db` pattern. Can start in parallel with everything above. |

#### After baseline lands + RDS is provisioned

| Item | Effort | Description |
|------|--------|-------------|
| **Staging data copy + cutover rehearsal** | 3-5 days | Schema from migrator on RDS. Data via parallel `COPY (SELECT * FROM t) TO STDOUT` with `enable_tiered_reads=on`. Untier S3 chunks first. Pre-copy history while live; at cutover copy only the delta. Verify row counts + checksums. Flip secrets. |
| **Prod cutover** | 1 day | After 48h+ staging soak. Same procedure as staging. |

#### After 2-week prod rollback window

| Item | Effort | Description |
|------|--------|-------------|
| **Cleanup** | 2-3 days | Remove `02_tigerdata.tf`, VPC peering, `timescale` provider, PITR drill workflow, TigerData Grafana dashboards, TigerData alerts, runbooks. Update AGENTS.md rules (remove hypertable/compression/tiering guidance, keep append-only). ~40 files in the infrastructure repo. |

### Open decisions

1. **PG17 vs PG18** — verify `postgres:18` is GA for RDS Multi-AZ in your region
2. **Connection pooling** — RDS has no built-in pooler; need RDS Proxy or in-cluster pgbouncer
3. **Read replica** — TigerData has one; decide if RDS needs one
4. **Database name** — TigerData uses `tsdb`, local uses `stl_verify`; pick one for RDS
5. **TigerData contract** — when does the current term end? Coordinate decommission timing
6. **Autovacuum tuning** — large append-only tables need `autovacuum_vacuum_insert_scale_factor` tuned low on RDS

### Rollback plan

- **Code changes** (this branch): Revert. Old code still works against TigerData.
- **Staging/prod cutover**: Keep TigerData running in parallel. Rollback = flip secrets back. Off-chain tables (Anchorage, Maple, CEX orderbooks) are not replayable — document a reverse-delta copy and a max rollback window (72h).
- **Cleanup**: Only after extended soak. TigerData contract re-establishment would be needed.

### Realistic timeline: ~5-7 weeks elapsed

| Phase | Duration | Notes |
|-------|----------|-------|
| Baseline squash + parity redesign + partitioning | 1-2 weeks | Critical path |
| RDS provisioning | 1-2 days | Parallel with above |
| Staging rehearsal | 3-5 days | Copy + verify + soak |
| Prod cutover | 1 day | After 48h staging soak |
| Rollback window | 2 weeks | TigerData stays live |
| Cleanup | 2-3 days | After rollback window closes |
