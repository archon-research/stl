# ADR-0002: Data Auditability and Processing Versioning

**Status**: Accepted  
**Proposed**: @simonbojeoutzen  
**Date**: 2026-04-08  
**Deciders**: @vector, @infrastructure

## Context

We are building risk calculation capabilities on top of our indexed data. Every risk calculation
must be fully auditable: an auditor needs to trace from a risk result back to the exact data
points that were used, and from each data point back to the code version that produced it.

Our data pipeline currently handles one source of data mutation — **chain reorganizations** — via the
`block_version` column and the `assign_block_version` trigger on `block_states`. When the blockchain
reorganizes, we insert a new row at the same block height with an incremented `block_version`.

However, there is a second source of mutation we do not yet handle: **internal processing errors**.
If a bug in our indexing code (e.g. an incorrect rate calculation in the Morpho indexer) produces
wrong data, we need to fix the code, reprocess the affected block range, and store corrected data
alongside the original — without destroying the original for audit purposes.

Today this is impossible. Our inserts use `ON CONFLICT DO NOTHING` to ensure idempotent retries
(pod restarts, SQS re-deliveries) don't create duplicates — this is essential and must be
preserved. But it also means reprocessing a block range with fixed code silently discards the
corrected data, since the old rows already occupy the primary key.

We need two things:

1. **Internal data versioning** — the ability to store multiple processing versions of the same
   data point, orthogonal to blockchain reorg versioning.
2. **Code provenance** — every data point must record which build (git commit) of our code
   produced it.

Risk calculation lineage (tracing from a risk result back to the exact data points it consumed)
is a related but independent concern, deferred to a future ADR.

## Decision

### 1. Build Registry

A new regular (non-hypertable) table that records every deployed build of our services:

```sql
CREATE TABLE build_registry (
    id           SERIAL PRIMARY KEY,
    git_hash     TEXT NOT NULL UNIQUE,
    built_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    docker_sha   TEXT,   -- nullable: not all builds produce Docker images (local dev, tests)
    notes        TEXT
);

INSERT INTO build_registry (id, git_hash, built_at, notes)
VALUES (0, 'pre-tracking', NOW(), 'Data produced before provenance tracking was enabled');
SELECT setval(pg_get_serial_sequence('build_registry', 'id'), 1, false);
```

Each service binary must expose its git commit at runtime for provenance. `GitCommit` can be set
at build time via `-ldflags` and, when it is not provided, is populated from Go's embedded VCS
build info (`debug.ReadBuildInfo`). We use the full 40-character hash for unambiguous
auditability, so provenance does not depend on a specific build pipeline:

```go
var GitCommit string // optionally set by: go build -ldflags "-X main.GitCommit=${GIT_COMMIT}"
                     // otherwise populated from Go build info at runtime
```

Build resolution happens once, in shared infrastructure code, so individual workers and jobs
don't need to think about it. A shared `buildregistry` package registers the git hash on startup
and exposes the resolved `build_id`:

```go
// internal/adapters/outbound/postgres/buildregistry/registry.go
type Registry struct {
    buildID int
}

func New(ctx context.Context, db *pgxpool.Pool, gitHash string) (*Registry, error) {
    var id int
    err := db.QueryRow(ctx, `
        INSERT INTO build_registry (git_hash) VALUES ($1)
        ON CONFLICT (git_hash) DO NOTHING
        RETURNING id`, gitHash).Scan(&id)
    if err == pgx.ErrNoRows {
        // Already registered (pod restart, multiple replicas) — look up the existing id.
        err = db.QueryRow(ctx, `
            SELECT id FROM build_registry WHERE git_hash = $1`,
            gitHash).Scan(&id)
    }
    if err != nil {
        return nil, fmt.Errorf("resolving build_id: %w", err)
    }
    return &Registry{buildID: id}, nil
}

func (r *Registry) BuildID() int { return r.buildID }
```

Each service's `main.go` creates the registry once and passes it (or just the `int`) into
whatever services and repositories need it — the same way database pools and other shared
dependencies are wired today. Individual workers never interact with `build_registry` directly;
they just receive a `buildID int` and include it in their writes.

To prevent new services from accidentally omitting build registration, repository constructors
should require `buildID` as a parameter — making the omission a compile error rather than a
runtime bug. If a repository can be constructed without a `buildID`, it can silently write
`build_id = 0`, which is indistinguishable from pre-tracking data.

Note that `build_id` is per **row**, not per calculation or per service. If a risk calculation
consumes sparklend data indexed by build X and morpho data indexed by build Y, the input rows
carry their respective `build_id` values. This is correct and expected — it captures exactly
which code produced each data point, even when services are deployed independently.

### 2. Processing Version and Build ID on State Tables

Every state table gains two columns:

```sql
processing_version  INT NOT NULL DEFAULT 0
build_id            INT NOT NULL DEFAULT 0
```

`processing_version` is added to primary key and unique constraints. `build_id` is not — it is
metadata, not identity.

Example for `morpho_market_state`:

```
Before: PK (morpho_market_id, block_number, block_version, timestamp)
After:  PK (morpho_market_id, block_number, block_version, processing_version, timestamp)
```

Semantics:

- `block_version` — which version of the blockchain data (reorg handling, external)
- `processing_version` — which version of our processing (bug fix / correction, internal)
- `build_id` — pointer to `build_registry`, identifying which code produced this row

A single data point can have:

| block_version | processing_version | Meaning |
|---------------|-------------------|---------|
| 0 | 0 | Original block, original code |
| 0 | 1 | Original block, corrected code |
| 1 | 0 | Reorged block, original code |
| 1 | 1 | Reorged block, corrected code |

For tables without blockchain data (`anchorage_package_snapshot`, `anchorage_operation`,
`offchain_token_price`), `block_version` does not exist, but `processing_version` applies the
same way — any polled or off-chain data can have processing bugs that need correction.

No foreign key is defined from state tables to `build_registry`. Foreign keys are incompatible
with distributed hypertables (as documented in our existing `offchain_token_price` migration),
and the constraint is already enforced at the application level — each service resolves its
`build_id` once at startup.

### 3. Automatic Version Assignment via Per-Table Triggers

Each state table gets a dedicated trigger function that assigns `processing_version` automatically
on insert. The functions use hardcoded column names so PostgreSQL can cache the query plan,
following the same pattern as the existing `assign_block_version` trigger on `block_states`.

#### Retry vs. Reprocessing

The trigger must distinguish between two cases:

- **Idempotent retry** (pod restart, SQS re-delivery): same data, same code, same natural key —
  should be deduplicated, not create a new version.
- **Reprocessing** with fixed code: same natural key, different `build_id` — should insert as a
  new version.

The trigger uses `build_id` to distinguish these cases. If a row with the same natural key AND
same `build_id` already exists, it is a retry — the trigger reuses the existing
`processing_version`, and the subsequent `ON CONFLICT DO NOTHING` on the PK deduplicates the
insert. If the `build_id` is different (or no row exists), it assigns the next version.

#### Concurrency

Each trigger function acquires `pg_advisory_xact_lock` on a 64-bit hash of the table prefix +
full natural key columns before reading `MAX(processing_version)`. This serializes concurrent
inserts for the same natural key at the database level — every INSERT is protected regardless
of which client, code path, or language performs it.

The lock key is built with `format()` and pipe delimiters. The table prefix prevents
cross-table collisions. `format()` converts each argument independently, preventing ambiguity
between value boundaries.

```sql
PERFORM pg_advisory_xact_lock(hashtextextended(
    format('mms|%s|%s|%s|%s',
        NEW.morpho_market_id, NEW.block_number, NEW.block_version, NEW.timestamp),
    0));
```

The only requirement from callers: use `READ COMMITTED` isolation (PostgreSQL's default, and
explicitly configured in `OpenPool`). `SERIALIZABLE` would snapshot too early, preventing the
trigger from seeing committed rows from the lock holder.

**Deadlock prevention in batch inserts:** The trigger fires per row in VALUES clause order.
Two concurrent transactions inserting overlapping rows in different order would deadlock.
Batch repository methods must sort rows by natural key before building the INSERT to ensure
consistent lock acquisition order across transactions.

#### Trigger Examples

```sql
-- ============================================================================
-- Blockchain-derived tables (have block_number + block_version)
-- ============================================================================

-- morpho_market_state
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_market_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    -- Serialize concurrent inserts for the same natural key.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mms|%s|%s|%s|%s',
            NEW.morpho_market_id, NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    -- Check if this build already produced a version for this key (retry)
    SELECT processing_version INTO existing_ver
    FROM morpho_market_state
    WHERE morpho_market_id = NEW.morpho_market_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        -- Retry: reuse existing version -> ON CONFLICT DO NOTHING handles it
        NEW.processing_version := existing_ver;
    ELSE
        -- New processing: assign next version
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_market_state
        WHERE morpho_market_id = NEW.morpho_market_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_market_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_market_state();

-- onchain_token_price
CREATE OR REPLACE FUNCTION assign_processing_version_onchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM onchain_token_price
    WHERE token_id = NEW.token_id
      AND oracle_id = NEW.oracle_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM onchain_token_price
        WHERE token_id = NEW.token_id
          AND oracle_id = NEW.oracle_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON onchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_onchain_token_price();

-- Same pattern for: borrower, borrower_collateral, sparklend_reserve_data,
-- morpho_market_position, morpho_vault_state, morpho_vault_position,
-- prime_debt, allocation_position, protocol_event
-- (each with its own natural key columns in the WHERE clause)

-- ============================================================================
-- Off-chain / polled tables (no block_number or block_version)
-- ============================================================================

-- anchorage_package_snapshot
-- Natural key: (prime_id, package_id, asset_type, custody_type, snapshot_time)
CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_package_snapshot()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM anchorage_package_snapshot
    WHERE prime_id = NEW.prime_id
      AND package_id = NEW.package_id
      AND asset_type = NEW.asset_type
      AND custody_type = NEW.custody_type
      AND snapshot_time = NEW.snapshot_time
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM anchorage_package_snapshot
        WHERE prime_id = NEW.prime_id
          AND package_id = NEW.package_id
          AND asset_type = NEW.asset_type
          AND custody_type = NEW.custody_type
          AND snapshot_time = NEW.snapshot_time;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON anchorage_package_snapshot
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_anchorage_package_snapshot();

-- anchorage_operation
-- Natural key: (operation_id, created_at)
CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_operation()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM anchorage_operation
    WHERE operation_id = NEW.operation_id
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM anchorage_operation
        WHERE operation_id = NEW.operation_id
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON anchorage_operation
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_anchorage_operation();

-- offchain_token_price
-- Natural key: (token_id, source_id, timestamp)
CREATE OR REPLACE FUNCTION assign_processing_version_offchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('ofp|%s|%s|%s', NEW.token_id, NEW.source_id, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM offchain_token_price
    WHERE token_id = NEW.token_id
      AND source_id = NEW.source_id
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM offchain_token_price
        WHERE token_id = NEW.token_id
          AND source_id = NEW.source_id
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON offchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_offchain_token_price();
```

Each function is identical in structure — only the table name and WHERE clause columns differ.
This is intentionally repetitive: the boilerplate cost is low, and the performance benefit of
plan caching is significant on hot paths.

Insert statements do not pass `processing_version` — the trigger handles it. Existing
`ON CONFLICT DO NOTHING` clauses remain in application code, using the updated PK/unique
constraints that include `processing_version`. On retries (same `build_id`), the trigger reuses
the existing version and the conflict clause deduplicates. On reprocessing (different `build_id`),
the trigger assigns a new version and the insert succeeds.

#### Performance

Each trigger function uses static SQL with hardcoded column names, allowing PostgreSQL to cache
the query plan across invocations. The queries run as index-only scans on the existing PK/unique
index.

The trigger acquires an advisory lock (~2-5us uncontended), then performs up to two index
lookups: one to check for a retry (by `build_id`), and if not found, one to get
`MAX(processing_version)`. On the normal path (first insert, no existing row), all operations
return quickly.

Benchmarked at ~200us per INSERT total (advisory lock + trigger + INSERT + network roundtrip
to containerized PostgreSQL). The advisory lock itself is a small fraction of the total cost.
For normal block processing (~50-200 rows per 12-second block), total overhead is negligible
relative to network I/O.

### 4. Canonical Version: Latest-Wins

The canonical data point for any natural key is the one with the highest `processing_version`.
This is a convention, not a database constraint. Queries that need canonical data use:

```sql
SELECT DISTINCT ON (morpho_market_id, block_number, block_version, timestamp) *
FROM morpho_market_state
WHERE morpho_market_id = $1 AND block_number BETWEEN $2 AND $3
ORDER BY morpho_market_id, block_number, block_version, timestamp,
         processing_version DESC;
```

This works because reprocessing is a deliberate, forward-only correction — you never intentionally
produce a worse version. If a reprocessing introduces its own bug, the fix is to produce
`processing_version = 2`, not to roll back.

## Affected Tables

The following tables receive `processing_version` and `build_id` columns, with per-table
triggers:

### Blockchain-derived (have `block_number` + `block_version`)

| Table | Natural Key (WHERE clause) | Type |
|-------|---------------------------|------|
| `borrower` | `user_id, protocol_id, token_id, block_number, block_version, created_at` | Hypertable (columnstore), natural PK |
| `borrower_collateral` | `user_id, protocol_id, token_id, block_number, block_version, created_at` | Hypertable (columnstore), natural PK |
| `sparklend_reserve_data` | `protocol_id, token_id, block_number, block_version` | Hypertable, surrogate PK `(id, block_number)` |
| `onchain_token_price` | `token_id, oracle_id, block_number, block_version, timestamp` | Hypertable (compression) |
| `morpho_market_state` | `morpho_market_id, block_number, block_version, timestamp` | Hypertable (compression) |
| `morpho_market_position` | `user_id, morpho_market_id, block_number, block_version, timestamp` | Hypertable (compression) |
| `morpho_vault_state` | `morpho_vault_id, block_number, block_version, timestamp` | Hypertable (compression) |
| `morpho_vault_position` | `user_id, morpho_vault_id, block_number, block_version, timestamp` | Hypertable (compression) |
| `prime_debt` | `prime_id, block_number, block_version, synced_at` | Hypertable, UNIQUE only (no PK) |
| `allocation_position` | `chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, created_at` | Hypertable (columnstore), natural PK |
| `protocol_event` | `chain_id, block_number, block_version, tx_hash, log_index, created_at` | Hypertable (columnstore), natural PK |

### Off-chain / polled (no `block_number` or `block_version`)

| Table | Natural Key (WHERE clause) | Type |
|-------|---------------------------|------|
| `anchorage_package_snapshot` | `prime_id, package_id, asset_type, custody_type, snapshot_time` | Hypertable (compression) |
| `anchorage_operation` | `operation_id, created_at` | Hypertable (compression) |
| `offchain_token_price` | `token_id, source_id, timestamp` | Hypertable (compression) |

**Notes:**
- All state tables are now hypertables. Tables using the old compression API
  (`timescaledb.compress`) require `remove_compression_policy` + `decompress_chunk` before
  constraint alteration. Tables using the columnstore API (`timescaledb.enable_columnstore`)
  require pausing the job, decompressing, and disabling columnstore before alteration.
- `sparklend_reserve_data` has a surrogate PK `(id, block_number)` with a **separate** UNIQUE
  constraint on the natural key. `processing_version` is added to the UNIQUE constraint, not
  the surrogate PK. `prime_debt` has no PK — only a UNIQUE constraint.
- The trigger WHERE clause always matches against the natural key columns.

## Migration Strategy

Existing data is not backfilled. The migration is split into separate files so that each step
is an independent transaction. If decompression fails, no schema changes have occurred. If
constraint alteration fails, compression settings are unchanged.

Tiered (S3) chunks may need to be recalled before decompression. The total data volume should
be estimated to plan an appropriate maintenance window.

### File 1: `20260410_100000_create_build_registry.sql`

Create the `build_registry` table and seed the `pre-tracking` entry (id=0).

### File 2: `20260410_110000_add_auditability_columns.sql`

Add `processing_version INT NOT NULL DEFAULT 0` and `build_id INT NOT NULL DEFAULT 0` to all
14 state tables. Adding `NOT NULL` columns with `DEFAULT` is supported on compressed
hypertables since TimescaleDB 2.6 — no decompression needed.

### File 3: `20260410_120000_remove_policies_and_decompress.sql`

Remove compression/columnstore policies and decompress all chunks. Tables using the old
compression API (`timescaledb.compress`) use `remove_compression_policy` + `decompress_chunk`.
Tables using the columnstore API (`timescaledb.enable_columnstore`) pause their jobs,
decompress, and disable columnstore. Tables with no compression (`sparklend_reserve_data`,
`prime_debt`) are skipped.

### File 4: `20260410_130000_alter_constraints.sql`

Drop and recreate PK/UNIQUE constraints to include `processing_version`. For most tables this
is the PK. For `sparklend_reserve_data` (surrogate PK) and `prime_debt` (no PK), it is the
UNIQUE constraint. Uses a helper function for dynamic constraint name lookup.

### File 5: `20260410_140000_update_settings_and_recompress.sql`

Update compression/columnstore `orderby` settings to append `processing_version DESC`, re-add
policies, and recompress chunks older than 2 days.

### File 6: `20260410_150000_create_processing_version_triggers.sql`

Create the per-table `assign_processing_version_*` functions and triggers as described in
section 3.

Existing rows receive `processing_version = 0` and `build_id = 0` (pointing to the
`pre-tracking` build entry) via the column defaults.

## Alternatives Considered

**Generic reusable trigger function** — A single `assign_processing_version()` function that
accepts natural key column names as trigger arguments and builds the WHERE clause dynamically via
`EXECUTE format(...)`. Reduces boilerplate (one function for all tables) but prevents PostgreSQL
from caching the query plan, adding ~100-250us per insert (2-3x slower than a dedicated
function). Rejected because the boilerplate cost of per-table functions is low and the
performance benefit of plan caching matters on high-throughput tables.

**Application-level advisory locking** — Instead of acquiring the advisory lock in the trigger,
a Go-side helper function would be called by each repository before inserting. Rejected because
it duplicates the natural key definition (the trigger already has it in the WHERE clause),
relies on caller discipline across 14+ call sites, doesn't protect inserts from non-Go clients
(migrations, scripts, psql), and creates the same class of bug as untyped variadic parameters.

**Application-level version assignment** — Instead of a trigger, each repository's INSERT
statement would include a subquery to compute the next `processing_version`. Normal inserts would
hardcode `0`; reprocessing jobs would query `MAX + 1` at startup for their target block range and
pass it into all subsequent inserts. Lighter at runtime (no trigger overhead on the normal path,
one query per reprocessing run instead of per row), but splits the versioning invariant across
multiple code paths. Every new service or adapter that writes to a state table must remember to
implement the version logic correctly — a missed code path silently overwrites data at
`processing_version = 0`. Rejected in favour of the trigger, which enforces the invariant in the
database regardless of which code path inserts data.

**Foreign key from hypertables to build_registry** — Would provide database-level referential
integrity. Rejected because foreign keys are incompatible with distributed hypertables (as
documented in our existing `offchain_token_price` migration), and the constraint is already
enforced at the application level (services resolve `build_id` once at startup).

**`git_hash TEXT` directly on state tables instead of `build_id INT`** — Stores the git hash on
every row, making the build immediately visible without a join. Rejected because a TEXT column
(40+ bytes per row) compresses much less efficiently than an INT (4 bytes, and very few distinct
values per chunk — ideal for RLE/dictionary compression in TimescaleDB).

**Explicit canonical flag (`is_canonical BOOLEAN`)** — A mutable column marking which processing
version is the active one. Provides explicit control over rollback. Rejected in favour of the
simpler latest-wins convention, because reprocessing is a forward-only operation and the mutable
flag adds write amplification (updating old rows when new versions arrive).

**Canonical version registry table** — A separate table mapping each natural key to its canonical
`processing_version`. Most flexible, supports rollback. Rejected because it adds a join to every
canonical-data query and introduces a coordination point that must be kept in sync with inserts.

## Consequences

**Positive:**
- Full audit trail from data points back to the code that produced them
- Internal processing errors can be corrected without destroying original data
- Build provenance is recorded for every data point with minimal storage overhead
- The `processing_version` trigger follows an established pattern (`assign_block_version`),
  keeping the codebase consistent
- Existing data and insert paths require minimal changes (new columns have defaults, trigger
  handles version assignment)
- Retry-safe: SQS re-deliveries and pod restarts do not create phantom versions thanks to the
  build-aware trigger
- Applies uniformly to both blockchain-derived and off-chain polled data

**Negative / Trade-offs:**
- Every INSERT on state tables incurs ~200us total overhead (advisory lock + trigger + INSERT),
  benchmarked against containerized PostgreSQL
- Primary keys / unique constraints on all state tables grow by one column, slightly increasing
  index size
- The `DISTINCT ON ... ORDER BY processing_version DESC` query pattern for canonical data must
  be adopted consistently across all read paths
- Migration requires decompressing all compressed hypertable chunks, altering constraints,
  updating compression settings, and recompressing — this is a significant operation that needs
  a maintenance window
- Per-table trigger functions are boilerplate-heavy (~14 functions with near-identical structure)
- Batch insert methods must sort rows by natural key before INSERT to prevent deadlocks from
  the per-row advisory lock in the trigger
