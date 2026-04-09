# ADR-0002: Data Auditability and Processing Versioning

**Status**: Proposed
**Date**: 2026-04-08

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

Following the established pattern in `blockstate_repository.go`, the **application/repository
layer** is responsible for concurrency control. Inserts that need version assignment must be
wrapped in a `READ COMMITTED` transaction, and the caller must acquire an advisory lock before
executing the insert to serialize concurrent writers for the same natural key.

The trigger's responsibility is limited to reading existing rows and assigning the correct
version number inside that already-serialized transaction. The trigger does **not** acquire
advisory locks itself — keeping the serialization contract in one layer (the repository) rather
than splitting it across trigger and application code.

`SERIALIZABLE` isolation alone cannot detect version races on TimescaleDB hypertables due to
chunk-level constraints — it produces unique constraint violations (23505) instead of
serialization failures (40001). The advisory lock serializes inserts, and `READ COMMITTED`
allows the trigger to see committed changes from the prior lock-holder.

The advisory lock key should be derived from stable natural key components using a low-collision
strategy. Prefer `pg_advisory_xact_lock($1::int, $2::int)` when the natural key has stable
integer parts (e.g. `(chain_id, block_number)` as in `blockstate_repository.go`). If it cannot
be expressed as two integers, use a 64-bit derived key such as
`pg_advisory_xact_lock(hashtextextended(concatenated_key, 0)::bigint)`. Avoid plain
`hashtext()` — it is only 32-bit and can unexpectedly serialize unrelated natural keys.

Any new repository that writes to these tables must follow the same locking pattern.

#### Trigger Examples

The examples below assume the caller has already started a `READ COMMITTED` transaction and
acquired the advisory lock for the row's natural key.

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

The trigger performs up to two index lookups per insert: one to check for a retry (by
`build_id`), and if not found, one to get `MAX(processing_version)`. On the normal path (first
insert, no existing row), both return empty results quickly. The advisory lock is acquired by
the caller before the insert, not by the trigger.

Estimated per-insert trigger overhead: ~30-100us (plan lookup from cache + index scans).
The advisory lock adds ~5us uncontended at the repository level. For normal block processing
(~50-200 rows per 12-second block), total overhead is 2-21ms — negligible relative to
network I/O.

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
| `borrower` | `user_id, protocol_id, token_id, block_number, block_version` | Regular table, surrogate PK `(id)` |
| `borrower_collateral` | `user_id, protocol_id, token_id, block_number, block_version` | Regular table, surrogate PK `(id)` |
| `sparklend_reserve_data` | `protocol_id, token_id, block_number, block_version` | Hypertable, surrogate PK `(id, block_number)` |
| `onchain_token_price` | `token_id, oracle_id, block_number, block_version, timestamp` | Hypertable |
| `morpho_market_state` | `morpho_market_id, block_number, block_version, timestamp` | Hypertable |
| `morpho_market_position` | `user_id, morpho_market_id, block_number, block_version, timestamp` | Hypertable |
| `morpho_vault_state` | `morpho_vault_id, block_number, block_version, timestamp` | Hypertable |
| `morpho_vault_position` | `user_id, morpho_vault_id, block_number, block_version, timestamp` | Hypertable |
| `prime_debt` | `prime_id, block_number, block_version, synced_at` | Hypertable, UNIQUE only (no PK) |
| `allocation_position` | `chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction` | Regular table, surrogate PK `(id)` |
| `protocol_event` | `chain_id, block_number, block_version, tx_hash, log_index` | Regular table, surrogate PK `(id)` |

### Off-chain / polled (no `block_number` or `block_version`)

| Table | Natural Key (WHERE clause) | Type |
|-------|---------------------------|------|
| `anchorage_package_snapshot` | `prime_id, package_id, asset_type, custody_type, snapshot_time` | Hypertable |
| `anchorage_operation` | `operation_id, created_at` | Hypertable |
| `offchain_token_price` | `token_id, source_id, timestamp` | Hypertable |

**Notes:**
- Regular tables (`borrower`, `borrower_collateral`, `allocation_position`, `protocol_event`)
  have no compression/decompression concerns during migration.
- Several tables have surrogate PKs (`id` or `(id, block_number)`) with a **separate** UNIQUE
  constraint on the natural key: `borrower`, `borrower_collateral`, `sparklend_reserve_data`,
  `allocation_position`, `protocol_event`. For these tables, `processing_version` must be added
  to the **UNIQUE constraint**, not the surrogate PK. `prime_debt` has no PK at all — only a
  UNIQUE constraint that receives `processing_version`.
- The trigger WHERE clause always matches against the natural key columns (the UNIQUE constraint
  columns), regardless of whether the table uses a surrogate PK.

## Migration Strategy

Existing data is not backfilled. The migration must be executed in a specific order due to
TimescaleDB constraints around compressed chunks.

### Step 1: Create `build_registry` table

Create the table and seed the `pre-tracking` entry (id=0).

### Step 2: Add columns to all state tables

```sql
ALTER TABLE <table> ADD COLUMN processing_version INT NOT NULL DEFAULT 0;
ALTER TABLE <table> ADD COLUMN build_id INT NOT NULL DEFAULT 0;
```

Adding `NOT NULL` columns with `DEFAULT` is supported on compressed hypertables since
TimescaleDB 2.6 — no decompression needed for this step.

### Step 3: Decompress hypertable chunks

Dropping/recreating PK and UNIQUE constraints is NOT supported on compressed chunks. Every
compressed chunk across affected hypertables must be decompressed first:

```sql
-- For each affected hypertable:
SELECT remove_compression_policy('<table>', if_exists => true);
SELECT decompress_chunk(c, if_not_exists => true)
FROM show_chunks('<table>') c;
```

Tiered (S3) chunks may need to be recalled before decompression. This should be verified in
staging. The total data volume should be estimated to plan an appropriate maintenance window.

Regular tables (`borrower`, `borrower_collateral`, `allocation_position`, `protocol_event`) skip
this step entirely — no compression concerns.

### Step 4: Alter UNIQUE constraints

Drop and recreate the natural-key UNIQUE constraints to include `processing_version`. For tables
with surrogate PKs (`borrower`, `borrower_collateral`, `sparklend_reserve_data`,
`allocation_position`, `protocol_event`) and `prime_debt` (UNIQUE only, no PK), this means
the UNIQUE constraint — not the surrogate PK. For tables where the PK is the natural key
(e.g. `morpho_market_state`, `onchain_token_price`), it means the PK.

### Step 5: Update compression settings

```sql
-- Example for morpho_market_state (was: 'block_number DESC, block_version DESC')
ALTER TABLE morpho_market_state SET (
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- Example for anchorage_package_snapshot (was: 'snapshot_time DESC')
ALTER TABLE anchorage_package_snapshot SET (
    timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC'
);

-- Same pattern for all other hypertables — append processing_version DESC to existing orderby.
```

Without this, `processing_version` values would not benefit from RLE/dictionary compression.

### Step 6: Re-enable compression and recompress

```sql
SELECT add_compression_policy('<table>', INTERVAL '2 days', if_not_exists => true);
-- Recompress previously decompressed chunks:
SELECT compress_chunk(c, if_not_exists => true)
FROM show_chunks('<table>', older_than => INTERVAL '2 days') c;
```

### Step 7: Create trigger functions and triggers

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
- Every INSERT on state tables incurs ~30-100us trigger overhead plus ~5us for the
  repository-level advisory lock (~5-15% increase on insert latency)
- Primary keys / unique constraints on all state tables grow by one column, slightly increasing
  index size
- The `DISTINCT ON ... ORDER BY processing_version DESC` query pattern for canonical data must
  be adopted consistently across all read paths
- Migration requires decompressing all compressed hypertable chunks, altering constraints,
  updating compression settings, and recompressing — this is a significant operation that needs
  a maintenance window
- Per-table trigger functions are boilerplate-heavy (~15 functions with near-identical structure)
