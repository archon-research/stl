# Migration Plan: TimescaleDB/TigerData to Vanilla PostgreSQL

**Status**: DRAFT — awaiting critique and approval
**Date**: 2026-09-19
**Scope**: Both repos (`stl` and `infrastructure`)

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

## 9. Verification Matrix

### Per-phase verification

| Phase | Verification | Command | Pass criteria |
|-------|-------------|---------|---------------|
| 1 | Go unit tests | `make test` | All pass |
| 1 | Go integration tests | `make test-integration` | All pass |
| 1 | Python unit tests | `cd python && make test-unit` | All pass |
| 1 | Python integration tests | `cd python && make test-integration` | All pass |
| 2 | CI pipeline | Push branch, watch GH Actions | Green |
| 3 | Migration on vanilla PG | `make test-integration` (migrator tests) | All migrations apply cleanly |
| 3 | Append-only test | `TestConvertedTablesAreAppendOnly` | Pass |
| 4 | Local dev cluster | `make dev-up && make dev-migrate` | Clean startup |
| 4 | Workers function | `make run-watcher` + spot-check | Blocks indexed |
| 5 | Staging deploy | ArgoCD sync after secret update | All pods healthy |
| 5 | Staging smoke test | API queries, Grafana dashboards | Data present and correct |
| 5 | Staging row-count audit | Compare TigerData vs RDS counts | Match within tolerance |
| 5 | Prod deploy | After staging soak (48h+) | All pods healthy |
| 5 | Prod smoke test | Same as staging | Data present and correct |
| 6 | Terraform plan clean | `terraform plan` shows no drift | Clean |

### Rollback plan

- **Phase 1-4** (code changes): Revert the branch. Old code still works against TigerData.
- **Phase 5** (data migration): Keep TigerData running in parallel during the soak period. Rollback = update secrets to point back to TigerData, scale deployments.
- **Phase 6** (cleanup): Only execute after extended soak (2+ weeks). No rollback needed — TigerData contract would need to be re-established.

---

## 10. Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| `time_bucket_gapfill` + `locf` replacement produces different results | Data correctness | Medium | Integration tests compare old vs new output; manual spot-check on staging |
| Data migration takes longer than maintenance window | Extended downtime | Medium | Pre-test with staging data; have logical replication as fallback |
| Old migrations fail on vanilla PG (no shim coverage) | CI broken | Low | Comprehensive shim covering all TimescaleDB functions called in migrations |
| Performance regression without hypertable chunk exclusion | Slow queries | Medium | Add native PG range partitioning for largest tables if needed (Phase 5 follow-up) |
| RDS cost higher than TigerData | Budget | Low | Size comparison before provisioning; spot-check pricing |
| `block_meta_repository` window optimization loss | Slower block_meta loads | Low | Profile on staging; add PG partitioning if needed |

---

## 11. Open Questions

1. **Target PostgreSQL version**: PG18 (matches current TimescaleDB base)? Or PG17?
2. **RDS vs Aurora**: Aurora Serverless v2 auto-scales and may fit the bursty workload better. Cost comparison needed.
3. **Staging data migration strategy**: Fresh start (re-backfill) or dump-and-load?
4. **Native PG partitioning**: Should we add range partitioning for the largest tables (`block_states`, `allocation_position`, `protocol_event`) as part of this migration, or defer?
5. **TigerData contract/billing**: When does the current term end? Coordinate decommission timing.
6. **Logical replication**: Does TigerData support `pg_logical` for zero-downtime migration?

---

## 12. Implementation Order (Dependency Graph)

```
Phase 1.1-1.4 (Go code) ──┐
Phase 1.5-1.6 (Python)  ──┤
Phase 1.7 (test cleanup) ──┼── Phase 2 (test images) ── Phase 3 (SQL migration)
                           │                                      │
                           │                            Phase 4 (dev-infra)
                           │                                      │
                           └─────────── Phase 5 (RDS provision + data migration)
                                                                  │
                                                        Phase 6 (cleanup)
```

Phases 1.1-1.7 can be parallelized. Phase 2 depends on Phase 1 (code must not call TimescaleDB APIs). Phase 3 depends on Phase 2 (migrations must run on vanilla PG). Phase 5 can start in parallel with Phase 3 (RDS provisioning doesn't depend on code changes).
