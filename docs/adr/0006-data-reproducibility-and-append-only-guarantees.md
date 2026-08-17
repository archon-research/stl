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
| Calculation | Recreatable from its **manifest alone** (calc step), and each on-chain input from source | The calculation manifest (self-contained: request, code identities, every input row's identity, provenance and values), our code at the listed commits, S3, an archive node — **no database access** |

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

### 5. Snapshot-exact reads: `ingest_xid` and recorded snapshots (internal mechanism)

Wall-clock as-of is racy in two ways: a row's assignment time is its transaction *start*, but it
becomes visible at *commit* (a long batch is invisible to a calc yet included in a later replay);
and a multi-statement calc under `READ COMMITTED` sees a different snapshot per statement.
Postgres' own visibility primitive avoids both:

- Every governed table gets `ingest_xid xid8 DEFAULT pg_current_xact_id()` (added nullable with
  no default first — metadata-only on compressed hypertables — then `SET DEFAULT`; existing rows
  stay `NULL`, meaning "predates tracking" and always visible). Also `ingested_at TIMESTAMPTZ
  DEFAULT now()` — a human label only, never the audit key (VEC-551).
- A calculation runs all its reads in one `REPEATABLE READ` transaction on one connection,
  takes `pg_current_snapshot()::text` **first**, and writes its record (§6) in that same
  transaction. This is per calculation, not per API: each request holds its own pool connection
  for its own transaction, as today. Fan-out inside one calculation is still possible — the first
  connection calls `pg_export_snapshot()` and workers `SET TRANSACTION SNAPSHOT` to read the
  identical snapshot (same server; snapshots do not cross primary/replica). Read-only
  `REPEATABLE READ` takes no locks; only minutes-long snapshots (vacuum lag) need attention.
- Internal replay = `WHERE ingest_xid IS NULL OR pg_visible_in_snapshot(ingest_xid, :snap)` then
  the ordinary latest-wins rule. `pg_visible_in_snapshot` is a pure function of stored values.

The snapshot is **not** the third-party deliverable — it only resolves against our database. It
is what lets us generate the calculation manifest (§6) exactly, at any later time, off the
request path, because governed rows are never removed.

### 6. Calculation record and self-contained manifest

Reproducibility must not be gated on access to our database. Every calculation (dry-run or not)
therefore produces two artefacts:

**a. Record** (insert-only, governed): `id, calculation_type, build_id (calc code),
schema_version (last applied migration), request/params including the effective "as of" time,
snapshot (§5), output, manifest_key, manifest_hash, is_dry_run, created_at`; `id` is returned in
the response. Written in the same transaction as the reads. The Python API registers itself in
`build_registry` at startup like the Go binaries (it does not today), and calculation logic
never reads wall-clock time, environment/configmap values, caches, or external services — every
input is either a governed row visible in the snapshot or a field of the recorded request.

**b. Manifest** — one object in the archive bucket (`calc/<id>.jsonl.zst`, alongside the raw
block and SC-call archives), containing everything a third party needs and nothing that requires
our database:

- the record header (request, effective time, calc `git_hash`, `schema_version`);
- **every input row**: table, natural key, `block_number`/`block_version`/`processing_version`
  where present, `build_id` → `git_hash` of the writer, and the row's **values**;
- for on-chain rows, the raw-archive location (`chain / block / block_version / source`) from
  which the row can be re-derived by running the writer's build. Referencing is sufficient (the
  archives are immutable and kept indefinitely); the manifest job may additionally copy the
  referenced raw objects under `calc/<id>/raw/` when a fully self-contained per-calculation
  folder is wanted, at the cost of duplicated storage. Off-chain rows are terminal facts and their
  values are taken as given;
- the config rows used (they are governed rows too);
- the output, and `manifest_hash` for integrity.

With the manifest a third party can (i) rerun the calculation step from the manifest values and
our calc code at `git_hash`, and (ii) independently re-derive each on-chain input from S3 + the
writer's build. Only off-chain inputs are unverifiable to source, by the stated boundary.

**Generation.** The manifest is produced **asynchronously** by a job that re-selects the
calculation's inputs under `pg_visible_in_snapshot(ingest_xid, snapshot)` — exact by §5 — and
writes the object; the request path pays only for the record. Because governed rows are never
removed, a manifest can be (re)generated at any later time; the record's `snapshot` is the
fallback pointer, never the deliverable. `manifest_key`/`manifest_hash` are filled by the job
(insert-only: a second record row referencing the first, or a separate `calculation_manifest`
table). Final shape and API surface are VEC-232's; this ADR fixes what the two artefacts must pin.

### 7. Canonical reads are structural, not conventional

Application reads of current state go through `<table>_current` views (or the VEC-563 read
models / `transformed.*`), which encode `block_version DESC, processing_version DESC` once. Raw
tables are for audit queries. Existing version-blind aggregates (`protocol_event` and
`allocation_position` time buckets, `prime_debt` list) are fixed to read through those. A
schemamaster check flags application SQL that orders a governed table without
`processing_version`.

## Threats to Reproducibility

Ways a calculation can still become unreproducible once the above is implemented, and what
prevents each. These are part of the decision, not commentary.

| Threat | Prevention |
|---|---|
| A calculation reads a table that is not governed/append-only — an operational table such as `block_states.is_orphaned` (mutable, 30-day retention), a refreshed materialised view or continuous aggregate, an in-place "current state" read model | Calculation read paths may touch only governed tables (`raw_pipeline`/`dimension`/`config`) and governed, append-only read models carrying `ingest_xid`. Reorg fixes (VEC-553) are corrective rows, never a join to operational state. Schemamaster lint on calculation SQL. |
| Reads spread across several connections or transactions, or the record written best-effort/afterwards | §5/§6: one `REPEATABLE READ` transaction per calculation, snapshot taken first, record written in the same transaction; fan-out only via `pg_export_snapshot`. Without this the manifest job cannot know which rows the calculation actually saw. |
| Wall-clock, environment, cache or external-service inputs inside the calculation | §6: forbidden; the effective time is a field of the recorded request. |
| Calculation code without an identity (Python API today), or schema-resident logic (`_current` views, tie-break rules) not pinned | §6: Python registers a `build_id`; the record carries `schema_version`. A third party rebuilds schema at that migration and code at that commit. |
| A sanctioned in-place rewrite (`DISABLE TRIGGER` + `UPDATE`, as `20260306`, `20260410_125000`, `20260707` did) changes rows that earlier snapshots point at | Data fixes are new rows at a new `processing_version`. An in-place rewrite of a governed table is exceptional, requires an ADR-referenced migration, and is logged in `processing_version_log` with `reason` naming the calculations it invalidates. |
| Destructive schema migration on a governed table (drop/rename/retype) makes old code unrunnable against a later export | Governed tables are additive-only; deprecations keep the old column/view until no recorded calculation's `build_id` depends on it. |
| Cluster migration via dump/restore restarts `pg_current_xact_id()` low, so rows written afterwards look older than every stored snapshot | Prefer physical restore/fork (xids preserved). After any logical migration, `pg_resetwal -x` sets NextXID above the previous maximum before writes resume; the runbook records it. |
| A writer inserts `ingest_xid = NULL` explicitly and becomes "always visible" | No `INSERT` names `ingest_xid`; lint plus the conformance test. |
| Under-specified ordering with real ties (`DISTINCT ON`, `last()`, `locf`, cross-table "latest price ≤ block") returns arbitrary rows; float/parallel/hash-order nondeterminism in code | Every canonical selection has a total order (VEC-549 tie-break pattern); calculation code is deterministic given its inputs. |
| Retention or `drop_chunks` on a governed table; tiered data with a lifecycle rule | §1 conformance test; tiering means "kept indefinitely". |
| Manifest never generated (job failure/lag), or generated from a different snapshot than the calculation used | Manifest job is idempotent and retried; `manifest_hash` recorded; regeneration is always possible from the record's snapshot (§5); an alert on records older than N minutes without a manifest. |
| Manifest omits values for off-chain rows or config rows, forcing a third party back to our database | Manifest schema check: every input row carries identity **and** values; off-chain rows are terminal facts and must be complete in the manifest. |

Reproducing a wrong result exactly is the contract: corrections appear as new versions in later
snapshots and never change what an earlier calculation saw.

## Migration Plan

Ordered by information lost per day of delay; 1–3 make reproducibility *possible*.

1. **Config append-on-change** (§4), starting with `oracle_asset` and `position_classification`
   — the only item where waiting destroys information.
2. **`ingest_xid` + `ingested_at`** on governed tables (§5); calculation record + manifest job, Python `build_id`, `schema_version` (§6).
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

**Drop `processing_version` and use `ingest_xid` as the key discriminator** — rejected.
`processing_version` is kept: `ingest_xid` only answers "which rows were visible to a
calculation"; it cannot separate retries from corrections. Every retry is a new transaction, so
with `ingest_xid` in the key idempotency would need read-then-write + advisory locks again, and a
late live retry after a correction would get a higher xid and win.

**Wall-clock `ingested_at` as the as-of key** — the assignment/commit race and per-statement
snapshots make replay inexact (§5). Kept as a label only.

**Snapshot only, no manifest** — compact and exact, but reproducibility would be gated on access
to our database. Rejected; the snapshot is kept as the internal mechanism that generates the
manifest exactly and off the request path.

**Manifest built synchronously inside the request** — the "naive" shape from VEC-244; exact, but
read amplification lands on the request path. Rejected in favour of asynchronous generation from
the recorded snapshot, which is equally exact.

**Archive raw off-chain responses** — not required; off-chain data points carry no reproduction
claim (Boundaries).

**Row-level immutability trigger / rules** — statement-level trigger has the same guarantee at
lower cost; `RULE … DO INSTEAD NOTHING` fails silently. Rejected.

## Consequences

**Positive**
- Every on-chain data point and every calculation is reproducible by a third party from S3 (raw
  archives + calculation manifests), the code and an archive node — no database access — with
  exact MVCC semantics and no wall-clock races.
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
- Calculations must run reads in one transaction on one connection (or an exported snapshot) and persist a record; the API must not use wall-clock, env or cache inputs in calculation logic.
- Governed tables become additive-only; in-place data rewrites are exceptional and logged.
- Governed tables can never be retention-pruned; storage is bounded by compression + tiering only.
- Manifests carry input values, so S3 grows with calculation volume; a background job and its
  monitoring become part of the calculation path.

## Appendix: ADR-0002 §3 mechanism (2026-04-08, replaced by §3 above)

ADR-0002 assigned `processing_version` in a per-table `BEFORE INSERT` trigger: advisory-lock the
natural key, look up an existing row with the same key **and** `build_id` (retry → reuse its
version), else `MAX(processing_version)+1` (reprocess → new version); callers had to sort batch
inserts by natural key to avoid deadlocks, every function needed
`plan_cache_mode = force_custom_plan` on hypertables (generic plans cannot prune chunks; measured
4,410 ms vs 148 ms per 721-row batch at ~2,000 chunks, VEC-541), and VEC-185 added covering
indexes. The migration files (`20260410_*`, `20260424_120000`, `20260428_120000`,
`20260806_1[23]0000`) remain the authoritative record of what ran.
