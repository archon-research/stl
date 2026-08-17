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
| Calculation | Recreatable from its **manifest alone** (calc step); each on-chain input from source; the input *selection* verifiable from chain history up to the manifest's cutoff | The calculation manifest (self-contained: request, code identities, every input row's identity, provenance and values), our code at the listed commits, S3, an archive node — **no database access** |

Two units of reproduction are supported, and each must be self-contained for a third party
without database access: a **single data point** (via its recipe, §8) and a **whole calculation
with every data point in it** (via its manifest, §6, which is a list of recipes plus values).

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

### 2. Build registry and image retention

`build_registry(id, git_hash UNIQUE, built_at, docker_sha, notes)`, id 0 = `pre-tracking`.
Every binary resolves its `build_id` once at startup via `buildregistry.New`
(`ldflags` → `debug.ReadBuildInfo` → `BUILD_GIT_HASH`, hard error if none). Repository
constructors take `buildregistry.BuildID`. Unchanged from ADR-0002, plus:

- **Production images are kept indefinitely.** Every image that has run in production (any
  binary that writes governed rows or performs calculations) is retained in the registry with
  no lifecycle/expiry rule, keyed by digest, so an auditor can be handed the *original* image
  when rebuilding one that behaves identically is impossible or impractical (toolchain drift,
  dependency sources gone, non-reproducible base layers). Rebuilding from `git_hash` is the
  first path; the retained image is the guaranteed fallback.
- `build_registry.docker_sha` is populated at registration (from the running image's digest)
  so a `build_id` resolves to both a commit and a retained artefact.
- Later: `-trimpath`, pinned toolchain and base-image digests so rebuilds are bit-for-bit;
  useful, not required for possibility given the retained images.

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

This is what makes "config as of the run" recoverable: with config append-only, the same
`ingested_at` watermark (§5) that pins data rows also pins config rows — no separate config
snapshot needed.

### 5. Frozen prefix: `ingested_at` and a computed watermark (internal mechanism)

Because governed rows are never updated or removed (§1), the set of rows that were *committed*
by some instant never changes afterwards. A calculation therefore pins its inputs with one
timestamp — provided that timestamp is chosen so that no writer could still add a row "before"
it. Two things make a naive `created_at ≤ now()` unsafe: `created_at` means block time on half
the rows (VEC-551), and a row is stamped at its transaction *start* but becomes visible at
*commit*, so a long batch that started before T can appear after T. Both are removed as follows:

- Every governed table gets `ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()` — DB-assigned
  ingest time (`now()` = the writer's transaction start), never supplied by a writer (lint;
  no `INSERT` names the column). **Time zones are explicit:** the column is `timestamptz`
  (an absolute instant, stored as UTC), the cluster and every application session run with
  `TimeZone = 'UTC'`, and T is serialised everywhere — record, manifest, API — as RFC 3339 with
  microseconds and an explicit `Z` (`2026-08-17T09:03:19.444048Z`), never as a naive timestamp
  or a local time. A `timestamp without time zone` column, or a T rendered without an offset,
  is a defect: it would make the frozen prefix depend on whoever reads it. (`created_at` on
  existing tables is `timestamptz` too, but carries the VEC-551 semantic split; it is not used.) Added with a default on compressed hypertables is metadata-only;
  existing rows are backfilled `NULL` → treated as "predates tracking, always in the prefix", or
  set to a fixed cutover timestamp — either way they are older than any T a calculation records.
- At the start of a calculation, on the **primary**, compute the watermark

  ```sql
  SELECT LEAST(now(), COALESCE(min(xact_start), now())) AS t
  FROM pg_stat_activity
  WHERE xact_start IS NOT NULL AND pid <> pg_backend_pid();
  ```

  (include `pg_prepared_xacts` if two-phase commit is ever used). Every writer that started
  before T is already committed or aborted; every writer starting after the check stamps `> T`.
  So `ingested_at ≤ T` is a **frozen prefix**: exact, and it stays exact forever.
- Every read the calculation performs filters `ingested_at ≤ T` (including the latest-wins
  selection), and T is written into the record and manifest (§6). This is per calculation, not
  per API: each request computes its own T; reads may use any connection, replica or shard,
  because T is a plain timestamp — no session, transaction or xid state is carried.
- Internal replay = the same reads with the same T. A long-running writer transaction pushes T
  back for its duration (correct behaviour — the calculation then sees data up to that far
  behind); alert on writer transactions older than a few minutes so this stays visible.

The watermark is **not** the third-party deliverable — it is what lets us regenerate the
calculation manifest (§6) exactly, at any later time, off the request path, and what lets the
manifest state "all inputs are from the frozen prefix ≤ T".

### 6. Calculation record and self-contained manifest

Reproducibility must not be gated on access to our database. Every calculation (dry-run or not)
therefore produces two artefacts:

**a. Record** (insert-only, governed): `id, calculation_type, build_id (calc code),
schema_version (last applied migration), request/params including the effective "as of" time,
watermark T (§5), output, manifest_key, manifest_hash, is_dry_run, created_at`; `id` is returned in
the response. Written in the same transaction as the reads. The Python API registers itself in
`build_registry` at startup like the Go binaries (it does not today), and calculation logic
never reads wall-clock time, environment/configmap values, caches, or external services — every
input is either a governed row in the frozen prefix `ingested_at ≤ T` or a field of the recorded request.

**b. Manifest** — one object in the archive bucket (`calc/<id>.jsonl.zst`, alongside the raw
block and SC-call archives), containing everything a third party needs and nothing that requires
our database:

- the record header (request, effective time, watermark T as RFC 3339 UTC, calc `git_hash`,
  `schema_version`);
- **every input row** as its recipe (§8) plus the row's **values**: table, natural key,
  `block_number`/`block_version`/`processing_version` where present, `build_id` → `git_hash` of
  the writer;
- for on-chain rows, the raw-archive location (`chain / block / block_version / source`) from
  which the row can be re-derived by running the writer's build. Referencing is sufficient (the
  archives are immutable and kept indefinitely); the manifest job may additionally copy the
  referenced raw objects under `calc/<id>/raw/` when a fully self-contained per-calculation
  folder is wanted, at the cost of duplicated storage. Off-chain rows are terminal facts and their
  values are taken as given;
- the config rows used (they are governed rows too);
- a **selection statement**: which rule chose the input rows (identified by the calc `git_hash`
  + `schema_version`, with its parameters — protocol, asset, prime, …) and a **per-chain
  cutoff** (highest `block_number`/`block_version` our governed data was complete to at
  watermark T). Protocol-wide models such as gap-sweep read "latest row per user" over what we had
  indexed — a mixed-block set that is a fact about our database, not about the chain at one
  block — so the manifest must state how the set was chosen and up to where;
- the output, and `manifest_hash` for integrity.

With the manifest a third party can (i) rerun the calculation step from the manifest values and
our calc code at `git_hash`, (ii) independently re-derive each on-chain input from S3 + the
writer's build, and (iii) verify the **selection** — completeness of the set and freshness of each
row — by replaying the selection rule against chain history up to the stated cutoff (event scans
or our indexer code from genesis/baseline). (iii) needs no database access but is expensive; it
is the "possible now, easy later" item, and a future "as-of block B" calculation mode (all
positions evaluated at one block) would reduce it to a chain-state query. Only off-chain inputs
are unverifiable to source, by the stated boundary.

**Generation.** The manifest may be written inline when the calculation already holds its
input rows, or — the default for protocol-wide models whose SQL aggregates tens of thousands of
rows server-side — **asynchronously**, by a job that re-runs the calculation's input selection
with `ingested_at ≤ T` (exact by §5) and writes the object; the request path pays only for the
record. Because governed rows are never removed, a manifest can be (re)generated at any later
time; the record's T is the fallback pointer, never the deliverable. The job runs on commit of the
record (queue or poll on `manifest_key IS NULL`), is idempotent (same record → same key and
hash), and an alert fires on records older than N minutes without a manifest. `manifest_key`/`manifest_hash` are filled by the job
(insert-only: a second record row referencing the first, or a separate `calculation_manifest`
table). Final shape and API surface are VEC-232's; this ADR fixes what the two artefacts must pin.

### 7. Canonical reads are structural, not conventional

Application reads of current state go through `<table>_current` views (or the VEC-563 read
models / `transformed.*`), which encode `block_version DESC, processing_version DESC` once. Raw
tables are for audit queries. Existing version-blind aggregates (`protocol_event` and
`allocation_position` time buckets, `prime_debt` list) are fixed to read through those. A
schemamaster check flags application SQL that orders a governed table without
`processing_version`.

### 8. Data-point recipe: the unit a single data point reproduces from

Every governed row has a **recipe** — the minimal self-contained description a third party needs
to recreate that one data point without our database:

| Field | Source |
|---|---|
| `table`, natural key, `block_number`, `block_version`, `processing_version` | the row's identity |
| `git_hash` (and image digest once populated) | `build_registry[build_id]` — the writer's code |
| `source` and raw-archive locator (`chain / block / block_version / source`) | which binary and which S3 objects the row derives from; on-chain only |
| `chain_id`, contract addresses/log identity where the row has them | to re-fetch from any archive node instead of our S3 |
| value hash (or the values) | to compare a reproduction against what we stored |

Off-chain rows have a recipe too (identity, `git_hash`, `source`, fetched-at, values) but no
raw-archive locator; they are terminal facts.

The recipe is not a new table: it is a projection over columns that already exist plus the
`build_registry` join, and the manifest (§6) is a list of recipes with values. What this section
adds is a **delivery obligation**: any API response that returns a data point carries its recipe
(or a stable reference the third party can resolve to one, e.g. a `provenance` endpoint keyed by
row identity), so a single data point can be reproduced from the response alone — the API
returns `git_hash` and `source`, not our internal `build_id`. `CONTRIBUTING.md` already requires
`block_version` and `processing_version` on results; this extends it to the full recipe.

## Threats to Reproducibility

Ways a calculation can still become unreproducible once the above is implemented, and what
prevents each. These are part of the decision, not commentary.

| Threat | Prevention |
|---|---|
| A calculation reads a table that is not governed/append-only — an operational table such as `block_states.is_orphaned` (mutable, 30-day retention), a refreshed materialised view or continuous aggregate, an in-place "current state" read model | Calculation read paths may touch only governed tables (`raw_pipeline`/`dimension`/`config`) and governed, append-only read models carrying `ingested_at`. Reorg fixes (VEC-553) are corrective rows, never a join to operational state. Schemamaster lint on calculation SQL. |
| A calculation reads without the watermark filter, or computes T on a replica (which cannot see the primary's in-flight writers), or the record is written best-effort/afterwards | §5/§6: T computed on the primary before any read, every read filters `ingested_at ≤ T`, record written with T in the calculation's own transaction. Without T the manifest job cannot know which rows the calculation actually saw. |
| Wall-clock, environment, cache or external-service inputs inside the calculation | §6: forbidden; the effective time is a field of the recorded request. |
| Calculation code without an identity (Python API today), or schema-resident logic (`_current` views, tie-break rules) not pinned | §6: Python registers a `build_id`; the record carries `schema_version`. A third party rebuilds schema at that migration and code at that commit. |
| A sanctioned in-place rewrite (`DISABLE TRIGGER` + `UPDATE`, as `20260306`, `20260410_125000`, `20260707` did) changes rows that earlier snapshots point at | Data fixes are new rows at a new `processing_version`. An in-place rewrite of a governed table is exceptional, requires an ADR-referenced migration, and is logged in `processing_version_log` with `reason` naming the calculations it invalidates. |
| Destructive schema migration on a governed table (drop/rename/retype) makes old code unrunnable against a later export | Governed tables are additive-only; deprecations keep the old column/view until no recorded calculation's `build_id` depends on it. |
| A writer supplies or backdates `ingested_at` (explicit value, or a clock far behind the primary's), so a row lands inside an already-recorded frozen prefix | `ingested_at` is DB-assigned only (no `INSERT` names it — lint + conformance test); T is computed on the primary from the primary's clock and in-flight transactions; shards/replicas stamp with their own DB clock, and skew is bounded by NTP and visible in monitoring. |
| T or `ingested_at` is rendered or compared without an explicit zone (naive `timestamp`, local-time serialisation, session `TimeZone` other than UTC), so a third party filters a different prefix than the calculation used | `timestamptz` only; `TimeZone = 'UTC'` on the cluster and in every application session; T serialised as RFC 3339 UTC with microseconds; schemamaster check that no governed table has a `timestamp without time zone` column; API/manifest schema test on the T format. |
| A long writer transaction (backfill) holds T minutes in the past, so calculations silently see stale data | Correct by construction, but alert on writer transactions older than a few minutes; backfills commit per batch. |
| Under-specified ordering with real ties (`DISTINCT ON`, `last()`, `locf`, cross-table "latest price ≤ block") returns arbitrary rows; float/parallel/hash-order nondeterminism in code | Every canonical selection has a total order (VEC-549 tie-break pattern); calculation code is deterministic given its inputs. |
| Retention or `drop_chunks` on a governed table; tiered data with a lifecycle rule | §1 conformance test; tiering means "kept indefinitely". |
| The image that produced a row or calculation can no longer be rebuilt identically (toolchain/dependency drift, non-reproducible base layers) and the original was pruned from the registry | §2: production images retained indefinitely by digest, `docker_sha` recorded per build; a conformance check that every `build_registry.docker_sha` still resolves in the registry. |
| Manifest never generated (job failure/lag), or generated with a different T than the calculation used | Manifest job is idempotent and retried; `manifest_hash` recorded; regeneration is always possible from the record's T (§5); an alert on records older than N minutes without a manifest. |
| A data point is served without its recipe (API returns values but not `git_hash`/`source`/version identity), so a single-point reproduction needs our database | §8 delivery obligation; response-schema test that every data-point payload carries recipe fields or a resolvable provenance reference. |
| Manifest lists the selected rows but not the selection rule or chain cutoff, so completeness/freshness of the input set can only be checked against our database | §6 selection statement + per-chain cutoff in every manifest; manifest schema check. |
| Manifest omits values for off-chain rows or config rows, forcing a third party back to our database | Manifest schema check: every input row carries identity **and** values; off-chain rows are terminal facts and must be complete in the manifest. |

Reproducing a wrong result exactly is the contract: corrections appear as new versions in later
snapshots and never change what an earlier calculation saw.

## Migration Plan

Ordered by information lost per day of delay; 1–3 make reproducibility *possible*.

1. **Config append-on-change** (§4), starting with `oracle_asset` and `position_classification`
   — the only item where waiting destroys information.
2. **`ingested_at`** on governed tables + watermark helper (§5); calculation record + manifest job, Python `build_id`, `schema_version` (§6).
3. **Append-only enforcement** (§1): app role, guard triggers, conformance test.
4. **Trigger removal** (§3): one migration drops the 36 functions/triggers, creates and seeds
   `processing_version_log`; delete the plan-cache/lock/sort tests and `db/migrations/AGENTS.md`
   rules; writers unchanged (they already omit `processing_version`).
5. **`_current` views / read-model routing** (§7), the three version-blind read fixes, and recipe
   fields on data-point API responses (§8).
6. Registry retention policy removed for production repositories and `docker_sha` populated at
   registration (§2) — small, do early. Later: replay tooling (`replay row`, `replay calc`),
   reproducible builds.

Existing rows keep `processing_version` and `build_id`; `ingested_at` is `NULL`/cutover for them; no other backfill.

## Alternatives Considered

**Keep the ADR-0002 trigger** — rejected: see §3; zero deliberate use, ~880 accidental versions, an
inverted canonical row, and a growing operational tax.

**Separate `correction_run` table with a per-row `run_id`** — richer provenance, but
`processing_version` + `build_id` already carry it; the metadata lives in
`processing_version_log` instead. Rejected as an extra column for no reproducibility gain.

**Drop `processing_version` and use `ingested_at` (or a transaction id) as the key
discriminator** — rejected. Timestamps order rows but cannot carry the writer's *intent*: a live
retry is a new transaction with a new timestamp, so idempotency would need `WHERE NOT EXISTS` +
advisory locks and concurrent writers would still produce byte-identical "versions" (exactly the
~880 phantom rows ADR-0002 produced), a correction run could not be retried idempotently, and a
late live retry after a correction would be the newest row and win. `processing_version` (`0` =
first observation, deduped by construction; `N` = logged deliberate correction) is kept.

**MVCC snapshot instead of the watermark (`ingest_xid xid8 DEFAULT pg_current_xact_id()` on
rows; each calculation reads in one `REPEATABLE READ` transaction, records
`pg_current_snapshot()`, replay filters `pg_visible_in_snapshot(ingest_xid, snap)`)** — this was
the earlier draft of §5. It is exact by Postgres' own visibility rules with zero staleness and
supports cross-connection fan-out via `pg_export_snapshot`. Rejected in favour of the watermark
because it is cluster-local (xid spaces do not survive dump/restore or sharding; snapshots do
not cross primary/replica), Postgres-specific and opaque to a third party (a manifest or export
cannot be filtered without a live cluster and MVCC knowledge), and constrains every calculation
to one connection and one transaction. The watermark gives the same frozen-prefix guarantee as
a plain timestamp filter that any consumer can apply; where a zero-lag calculation ever needs
it, the snapshot variant can be added for that endpoint without changing the manifest format.

**Fixed safety margin (`T = now() − k minutes`) instead of the computed watermark** — simpler,
but trades exactness for a guess about maximum transaction duration and makes every calculation
k minutes stale. Rejected; `pg_stat_activity` gives the exact bound for free.

**Watermark only, no manifest** — compact and exact, but reproducibility would be gated on access
to our database. Rejected; the watermark is kept as the internal mechanism that generates the
manifest exactly and off the request path.

**Manifest always built synchronously inside the request** — the "naive" shape from VEC-244;
exact, but read amplification lands on the request path for protocol-wide models. Allowed where
the calculation already holds its rows; otherwise asynchronous generation from the recorded T,
which is equally exact.

**Archive raw off-chain responses** — not required; off-chain data points carry no reproduction
claim (Boundaries).

**Row-level immutability trigger / rules** — statement-level trigger has the same guarantee at
lower cost; `RULE … DO INSTEAD NOTHING` fails silently. Rejected.

## Consequences

**Positive**
- Every on-chain data point and every calculation is reproducible by a third party from S3 (raw
  archives + calculation manifests), the code and an archive node — no database access — and
  every calculation's inputs are a frozen, timestamp-bounded prefix that any consumer can filter.
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
- Calculations must compute T first, filter every read by `ingested_at ≤ T`, and persist a record; the API must not use wall-clock, env or cache inputs in calculation logic. Calculations see data up to the age of the oldest in-flight writer transaction (normally sub-second).
- Governed tables become additive-only; in-place data rewrites are exceptional and logged.
- Governed tables can never be retention-pruned; storage is bounded by compression + tiering only.
- Manifests carry input values, so S3 grows with calculation volume; a background job and its
  monitoring become part of the calculation path.
- Container-registry storage grows without bound (one image per production build, ~1.6/day
  today); no lifecycle rules on the production repositories.

## Appendix: ADR-0002 §3 mechanism (2026-04-08, replaced by §3 above)

ADR-0002 assigned `processing_version` in a per-table `BEFORE INSERT` trigger: advisory-lock the
natural key, look up an existing row with the same key **and** `build_id` (retry → reuse its
version), else `MAX(processing_version)+1` (reprocess → new version); callers had to sort batch
inserts by natural key to avoid deadlocks, every function needed
`plan_cache_mode = force_custom_plan` on hypertables (generic plans cannot prune chunks; measured
4,410 ms vs 148 ms per 721-row batch at ~2,000 chunks, VEC-541), and VEC-185 added covering
indexes. The migration files (`20260410_*`, `20260424_120000`, `20260428_120000`,
`20260806_1[23]0000`) remain the authoritative record of what ran.
