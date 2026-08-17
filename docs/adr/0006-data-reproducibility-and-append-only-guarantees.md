# ADR-0006: Data Reproducibility and Append-Only Guarantees

**Status**: Proposed
**Proposed**: @simonbojeoutzen
**Date**: 2026-08-17
**Deciders**: @vector, @infrastructure
**Supersedes**: [ADR-0002](0002-data-auditability-and-processing-versioning.md) §3 (trigger-assigned `processing_version`); extends its §1, §2 and §4

## Context

The goal is stronger than "trace a row back to a git commit": **any third party
must be able to recreate every individual on-chain data point we store, and every calculation
that consumed our data points**, given our code, our S3 archives, an archive node and a copy of
the database. Making that *possible* is in scope now; making it *easy* (replay tooling) is not.

[ADR-0002](0002-data-auditability-and-processing-versioning.md) (April 2026) introduced `build_registry`, `processing_version` +
`build_id` on every state table, and a per-table `BEFORE INSERT` trigger that assigned
`processing_version` by inferring "retry vs. reprocess" from `build_id`. Four months of production
use (VEC-244 spike, 2026-08) showed:

- The trigger has never produced a deliberate correction. Every `processing_version > 0` row in
  prod (~880 across `sparklend_reserve_data`, `morpho_*`, `borrower*`, `offchain_token_price`) is a
  payload-identical duplicate written by the *next* build at a deploy boundary; in one case an
  older build's row became canonical over a newer build's (build 1685 → 1671).
- The mechanism cost VEC-185 (covering indexes), VEC-194 (advisory locks), VEC-541 (plan cache,
  30× regression), VEC-543, natural-key sort discipline in every batch writer, 36 trigger
  functions and their conformance tests.
- Reproducibility gaps sit elsewhere: read-time config (`oracle_asset.enabled`, classification)
  is overwritten in place (VEC-549), `created_at` is block time for half the rows and wall clock
  for the other half (VEC-551), no calculation is recorded (VEC-232), and "canonical" is decided
  at read time with nothing recorded about what a reader saw.
- Newer tables already diverged: `security_master`/`entity_master` use loader-assigned
  `processing_version` (VEC-411/419); `cex_orderbook_snapshots` skipped the pattern.

This ADR keeps what ADR-0002 got right — append-only tables, versions in the key, per-row build
provenance, latest-wins reads — and replaces the trigger with explicit assignment, adds the
guarantees reproducibility actually needs, and states the boundaries.

## Goal and Boundaries

| Artefact | Reproducibility claim | Inputs a third party needs |
|---|---|---|
| On-chain data point | Recreatable from source | Raw block/receipts and SC-call archive in S3 (keyed `chain / block / block_version / source`), the build (`build_id` → git hash), the config as of the run |
| Off-chain data point (CoinGecko, Anchorage, Maple GraphQL, CEX) | **None to source.** Terminal fact: what the source returned, when, which build stored it | — |
| Calculation | Recreatable from **our stored rows** | The exact visible row set (snapshot), calc build, request/params, config as of the run |

Off-chain sources cannot be replayed and we do not claim otherwise. Off-chain tables keep the
same columns and rules for uniformity (a transform bug on stored off-chain rows can still be
corrected as a new version), but no archive of raw responses is required.

## Decision

### 1. Data and config tables are append-only, and Postgres enforces it

Applies to every table classified `raw_pipeline`, `dimension` or `config` in
`data_quality/schemamaster/schema_master.json`. Operational tables (`block_states`, queues,
watermarks, transform queues) are exempt.

- **Privileges**: services connect as an application role with `SELECT, INSERT` only on these
  tables. Migrations run as the owner role. This is zero-cost and verifiable from
  `information_schema.role_table_grants` by anyone with the dump.
- **Guard trigger**: `BEFORE UPDATE OR DELETE OR TRUNCATE … FOR EACH STATEMENT` raising an
  exception, on every governed table. Statement-level, so it never fires in normal operation.
  A legitimate one-off data fix must `ALTER TABLE … DISABLE TRIGGER` inside a migration — explicit
  and auditable in git.
- **No retention**: governed tables may compress and tier to S3 (tiered chunks stay queryable and
  are kept indefinitely) but never get `add_retention_policy` / `drop_chunks`. `drop_chunks`
  bypasses triggers, so this is enforced by a conformance test over
  `timescaledb_information.jobs`, not by the trigger.
- **Conformance test** driven by `schema_master.json`: every governed table has the guard trigger,
  the role has no `UPDATE`/`DELETE` on it, and no retention job targets it.

The existing ~880 accidental `processing_version > 0` rows are left in place: they are
payload-identical, harmless under latest-wins, and deleting them would violate this section.

### 2. Build registry (unchanged from ADR-0002)

`build_registry(id, git_hash UNIQUE, built_at, docker_sha, notes)`, id 0 = `pre-tracking`.
Every binary resolves its `build_id` once at startup via `buildregistry.New`
(`ldflags` → `debug.ReadBuildInfo` → `BUILD_GIT_HASH`, hard error if none). Repository
constructors take `buildregistry.BuildID`. Later: populate `docker_sha` and build with
`-trimpath` so a third party can rebuild the image bit-for-bit; not required for possibility.

### 3. `processing_version` is caller-assigned, not trigger-inferred

`processing_version INT NOT NULL DEFAULT 0` stays in every PK/UNIQUE constraint. What changes is
assignment:

- **Live writers never set it.** Every worker, cronjob and backfiller writes at 0 with the
  existing `ON CONFLICT DO NOTHING`. Retries (pod restart, SQS redelivery, rolling deploy overlap,
  a different binary covering the same range) collide on `(natural key, 0)` and are dropped —
  first write wins, deterministically, regardless of build.
- **Corrections choose N once per run.** A correction (fixed code re-run over a range) allocates
  the next version for the table by inserting into the insert-only allocator/log

  ```sql
  CREATE TABLE processing_version_log (
      table_name          TEXT        NOT NULL,
      processing_version  INT         NOT NULL,
      ticket              TEXT        NOT NULL,
      reason              TEXT        NOT NULL,
      build_id            INT         NOT NULL,
      applied_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (table_name, processing_version)
  );
  ```

  (`N = max(processing_version) + 1` for that table, seeded once at migration time from the
  current per-table maxima), then writes every row of the run at N. Per-key gaps are expected
  (a key may have versions 0 and 2). A re-run of the same correction is a retry at N and dedupes.
- **Latest-wins is unchanged**: canonical row per natural key = highest `processing_version`,
  after `block_version DESC` for on-chain tables. `build_id` is never used for ordering.
- The 36 `assign_processing_version_*` triggers and functions, their advisory locks, the
  `plan_cache_mode` settings, the natural-key sort requirement on batch writers and the
  `TestProcessingVersion*` trigger/index tests are removed. The `…, processing_version DESC` read
  indexes and columnstore `orderby` stay.

Why not keep the trigger: `build_id` changes ~1.6×/day and corrections are rare and deliberate,
so inferring intent from it fires on the wrong events (deploy boundaries, worker vs. cronjob
builds) and cannot fire on the right one (same-build re-run after a config fix). The invariant it
"enforced regardless of code path" is exactly the behaviour live writers should have anyway
(write at 0); only the one correction tool needs version logic.

### 4. Config tables are append-on-change

Tables whose content decides which rows a reader uses (`oracle_asset`, protocol/contract
registries, `position_classification`, …) are converted from update-in-place to append-on-change,
using the `security_master` pattern (VEC-411): natural key + `processing_version` in the PK,
`valid_from`, and a `<table>_current` view. Toggling `oracle_asset.enabled` becomes a new row.
Reads go through the `_current` views. Identity-only registries (`token`, pool hubs) are converted
last. Once converted they fall under §1 and get the guard trigger.

This is what makes "config as of the run" recoverable: with config append-only, the same snapshot
mechanism (§5) that pins data rows also pins config rows — no separate config snapshot needed.

### 5. Snapshot-exact reads: `ingest_xid` and recorded snapshots

Wall-clock as-of is racy in two ways: a row's assignment time is its transaction *start*, but it
becomes visible at *commit* (a long batch is invisible to a calc yet included in a later replay);
and a multi-statement calc under `READ COMMITTED` sees a different snapshot per statement.
Postgres' own visibility primitive avoids both:

- Every governed table gets `ingest_xid xid8 DEFAULT pg_current_xact_id()` (added nullable with
  no default first — metadata-only on compressed hypertables — then `SET DEFAULT`; existing rows
  stay `NULL`, meaning "predates tracking" and always visible). Also `ingested_at TIMESTAMPTZ
  DEFAULT now()` — a human label only, never the audit key (VEC-551).
- A calculation runs all its reads in one `REPEATABLE READ` (or `SERIALIZABLE READ ONLY
  DEFERRABLE`) transaction and records `pg_current_snapshot()::text` in the same transaction.
- Replay = `WHERE ingest_xid IS NULL OR pg_visible_in_snapshot(ingest_xid, :snap)` then the
  ordinary latest-wins rule. `pg_visible_in_snapshot` is a pure function of stored values, so it
  is exact and evaluable on an export without our cluster.

### 6. Calculation record

Every calculation (dry-run or not) writes one insert-only row, minimally:
`id, calculation_type, build_id (calc code), request/params, snapshot (§5), output,
is_dry_run, created_at`, and returns `id` in the response. With §1, §4 and §5 in place this tuple
is a complete, race-free description of the inputs; no snapshot store or per-row input list is
needed. Final shape and API surface are VEC-232's; this ADR only fixes what the record must pin.

### 7. Canonical reads are structural, not conventional

Application reads of current state go through `<table>_current` views (or the VEC-563 read
models / `transformed.*`), which encode `block_version DESC, processing_version DESC` once. Raw
tables are for audit queries. Existing version-blind aggregates (`protocol_event` and
`allocation_position` time buckets, `prime_debt` list) are fixed to read through those. A
schemamaster check flags application SQL that orders a governed table without
`processing_version`.

## Migration Plan

Ordered by information lost per day of delay; 1–3 make reproducibility *possible*.

1. **Config append-on-change** (§4), starting with `oracle_asset` and `position_classification`
   — the only item where waiting destroys information.
2. **`ingest_xid` + `ingested_at`** on governed tables (§5); calculation record (§6).
3. **Append-only enforcement** (§1): app role, guard triggers, conformance test.
4. **Trigger removal** (§3): one migration drops the 36 functions/triggers, creates and seeds
   `processing_version_log`; delete the plan-cache/lock/sort tests and `db/migrations/AGENTS.md`
   rules; writers unchanged (they already omit `processing_version`).
5. **`_current` views / read-model routing** (§7) and the three version-blind read fixes.
6. Later: replay tooling (`replay row`, `replay calc`), reproducible builds, `docker_sha`.

Existing rows keep `processing_version`, `build_id` and `NULL ingest_xid`; no backfill.

## Alternatives Considered

**Keep the ADR-0002 trigger** — rejected: see §3; zero deliberate use, ~880 accidental versions, an
inverted canonical row, and a growing operational tax.

**Separate `correction_run` table with a per-row `run_id`** — richer provenance, but
`processing_version` + `build_id` already carry it; the metadata lives in
`processing_version_log` instead. Rejected as an extra column for no reproducibility gain.

**`ingest_xid` in the primary key instead of `processing_version`** — every retry is a new
transaction, so idempotency would need read-then-write + advisory locks again, and a late live
retry after a correction would win. Rejected.

**Wall-clock `ingested_at` as the as-of key** — the assignment/commit race and per-statement
snapshots make replay inexact (§5). Kept as a label only.

**Calculations enumerate every input row** — exact and race-free, but heavy per calculation; the
recorded snapshot is the compact form of the same thing.

**Archive raw off-chain responses** — not required; off-chain data points carry no reproduction
claim (Boundaries).

**Row-level immutability trigger / rules** — statement-level trigger has the same guarantee at
lower cost; `RULE … DO INSTEAD NOTHING` fails silently. Rejected.

## Consequences

**Positive**
- Every on-chain data point and every calculation is reproducible by a third party from S3, the
  code and a database export, with exact MVCC semantics and no wall-clock races.
- Append-only is a Postgres-enforced property, not a convention; corrections are deliberate,
  logged, and cannot happen by accident at a deploy boundary.
- Removes per-row trigger overhead, advisory locks, plan-cache tuning, sort discipline and 36
  functions; new versioned tables need only the columns and a `_current` view.
- Config history exists, so calculations survive `oracle_asset` toggles and reclassification.

**Negative / trade-offs**
- Config tables lose update-in-place: writers must append, and identity registries need an
  append-on-change pattern; that is real app-code work, done table by table.
- Role separation on TigerData and guard triggers add migration/ops surface; one-off data fixes
  become deliberate (`DISABLE TRIGGER` in a migration).
- Calculations must run reads in one transaction and persist a record; slightly more API work.
- Governed tables can never be retention-pruned; storage is bounded by compression + tiering only.

## Appendix: ADR-0002 §3 mechanism (2026-04-08, replaced by §3 above)

ADR-0002 assigned `processing_version` in a per-table `BEFORE INSERT` trigger: advisory-lock the
natural key, look up an existing row with the same key **and** `build_id` (retry → reuse its
version), else `MAX(processing_version)+1` (reprocess → new version); callers had to sort batch
inserts by natural key to avoid deadlocks, every function needed
`plan_cache_mode = force_custom_plan` on hypertables (generic plans cannot prune chunks; measured
4,410 ms vs 148 ms per 721-row batch at ~2,000 chunks, VEC-541), and VEC-185 added covering
indexes. The migration files (`20260410_*`, `20260424_120000`, `20260428_120000`,
`20260806_1[23]0000`) remain the authoritative record of what ran.
