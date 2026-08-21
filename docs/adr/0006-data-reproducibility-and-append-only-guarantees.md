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
- Reproducibility gaps sit elsewhere: read-time reference data (`oracle_asset.enabled`, classification)
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
| On-chain data point | Recreatable from source | Raw block/receipts and SC-call archive in S3 (keyed `chain / block / block_version / source`), the build (`build_id` → git hash), the reference data as of the run |
| Off-chain data point (CoinGecko, Anchorage, Maple GraphQL, CEX) | **None to source.** Terminal fact: what the source returned, when, which build stored it | — |
| Calculation | Recreatable from its **manifest alone** (calc step); each on-chain input from source; the input *selection* verifiable from chain history up to the manifest's cutoff | The calculation manifest (self-contained: request, code identities, every input row's identity, provenance and values), our code at the listed commits, S3, an archive node — **no database access** |

Two units of reproduction are supported, and each must be self-contained for a third party
without database access: a **single data point** (via its recipe, §8) and a **whole calculation
with every data point in it** (via its manifest, §6, which is a list of recipes plus values).

Off-chain sources cannot be replayed and we do not claim otherwise. Off-chain tables keep the
same columns and rules for uniformity (a transform bug on stored off-chain rows can still be
corrected as a new version), but no archive of raw responses is required.

**Reproduction fidelity.** Bit-for-bit is the claim only when the reproduction runs the
retained image (§2). A rebuild from `git_hash` — different toolchain, dependency or base-image
state — must match within a documented numerical tolerance (value TBD with the model owners;
PRD RP-4.4). The tolerance acknowledges the float paths in the risk models; the retained image
is the exact path.

**Point-in-time boundary.** "Point in time" here means *any recorded calculation*: the snapshot
(§5) reproduces exactly what a calculation saw, at any later time. Arbitrary wall-clock as-of
queries over the whole database are not a goal of this ADR; if they ever become one, the
`ingested_at` watermark (Alternatives) is the designed upgrade path. PRD RP-4.1 is being
aligned to this wording.

**Cutover.** The guarantees in this ADR apply to rows and calculations from the cutover date
(TBD) onward. Pre-tracking data (`build_id = 0`, `NULL ingest_xid`, reference-data history overwritten
in place before §4, archive gaps) is explicitly out of scope and cannot be brought into scope
retroactively.

**Scope: reproducibility only.** Auditability controls are deliberately out of scope and are
covered by a separate ADR: actor identity and per-service credentials, tamper-evident
hashing/anchoring, storage-level WORM and retention, correction approvals, audit/access
logging, evidence export, and GDPR handling. This ADR leaves the seams that ADR attaches to:
`writer_run` is where a principal lands, `processing_version_log` is insert-only and extensible
with approval fields, and `manifest_hash` is the hook for signing/anchoring.

## Decision

### 1. Data and reference tables are append-only, and Postgres enforces it

Applies to every table classified `raw_pipeline`, `dimension` or `config` in
`data_quality/schemamaster/schema_master.json` (the latter two are reference tables, §4). Operational tables (`block_states`, queues,
watermarks, transform queues) are exempt.

Governance must be **complete** to mean anything: the 36 tables currently in schemamaster's
`ignore_tables` — the `curve_*` and `uniswap_v3_*` pipelines, the enrichment layer
(`entity_master`, `entity_ref_codes`, `security_master`, `security_instrument_bridge`,
`position_classification`, `position_entity_link`) and the `ref_*` vocabularies — are
classified and brought under the same rules. The `transformed.*` read models are governed too:
their refresh path today rewrites rows in place (`ON CONFLICT … DO UPDATE … WHERE IS DISTINCT
FROM`), which is incompatible with §5, so it is converted to append-only (new rows,
latest-wins) before any calculation may read `transformed.*`; until then calculations read the
raw governed tables only.

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

### 2. Code identity: artefacts, writer runs, and image retention

ADR-0002's `build_registry(id, git_hash UNIQUE, …)` identifies a *commit*, which is not enough:
one commit produces one image per service, and the same service can be rebuilt from the same
commit with a different digest. And a row's provenance is not only code — the writer's *reference data*
(which oracles/tokens/contracts it was told to poll) is loaded at process start and changes
without a deploy. Two small tables replace "one int per git hash":

```sql
-- what ran: an immutable deploy artefact
CREATE TABLE build_registry (               -- kept name; semantics widened
    id            SERIAL PRIMARY KEY,
    git_hash      TEXT NOT NULL,
    service       TEXT NOT NULL,             -- binary/deployment name, e.g. sparklend-indexer
    image_digest  TEXT NOT NULL,             -- immutable, the retained artefact
    built_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes         TEXT,
    UNIQUE (git_hash, service, image_digest)
);
-- one process start of a writer or calculator
CREATE TABLE writer_run (
    id               BIGSERIAL PRIMARY KEY,
    build_id         INT NOT NULL REFERENCES build_registry(id),
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reference_snapshot  TEXT NOT NULL,           -- pg_current_snapshot() taken when reference data was loaded
    reference_effective_at TIMESTAMPTZ NOT NULL  -- the effective-as-of the process used for valid_from
);
```

- Every binary registers its artefact and opens a **run** at startup (`buildregistry.New`
  resolves `git_hash` as today, `service` from the binary name, `image_digest` from the running
  container; hard error if any is missing) and takes `reference_snapshot` in the same transaction in
  which it loads its reference data. Governed rows carry **`run_id`** (`BIGINT`, `NULL` = pre-tracking);
  `build_id` on rows is retained for existing data and derivable through the run for new data.
  Repository constructors take the run id the way they take `BuildID` today.
- "Reference data as of the writer run" is then exact and cheap: the reference rows visible in
  the run's `reference_snapshot` (reference tables are append-only, §4) with
  `valid_from <= reference_effective_at`. A process that reloads its reference data opens a new run.
- **Production images are kept indefinitely.** Every `image_digest` in `build_registry` is
  retained in the container registry with no lifecycle/expiry rule, so an auditor can be handed
  the *original* image when rebuilding one that behaves identically is impossible or impractical
  (toolchain drift, dependency sources gone, non-reproducible base layers). Rebuilding from
  `git_hash` is the first path; the retained image is the guaranteed fallback. A conformance check
  verifies every `image_digest` still resolves.
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
      run_id              BIGINT      NOT NULL REFERENCES writer_run(id),
      applied_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (table_name, processing_version),
      UNIQUE (table_name, ticket)
  );
  ```

  **Allocation contract** (serialised and idempotent): in one transaction,
  `pg_advisory_xact_lock(hashtext('pvlog:' || table_name))` — one lock per correction *run*,
  not per row — then `SELECT processing_version WHERE (table_name, ticket)` and return it if
  present (a retry of the same correction), else `INSERT … (max(processing_version) + 1)` and
  return that. Two concurrent corrections of one table therefore serialise; a crashed/re-run
  correction recovers its N by ticket. Seeded once at migration time from the current per-table
  maxima. Then the run writes every row of the correction at N. Per-key gaps are expected (a key
  may have versions 0 and 2). A re-run of the same correction is a retry at N and dedupes.
- **Latest-wins is unchanged**: canonical row per natural key = highest `processing_version`,
  after `block_version DESC` for on-chain tables. `build_id` is never used for ordering.
- The 36 `assign_processing_version_*` triggers and functions, their advisory locks, the
  `plan_cache_mode` settings, the natural-key sort requirement on batch writers and the
  `TestProcessingVersion*` trigger/index tests are removed. The `…, processing_version DESC` read
  indexes and columnstore `orderby` stay.

Why not keep the trigger: `build_id` changes ~1.6×/day and corrections are rare and deliberate,
so inferring intent from it fires on the wrong events (deploy boundaries, worker vs. cronjob
builds) and cannot fire on the right one (same-build re-run after a reference-data fix). The invariant it
"enforced regardless of code path" is exactly the behaviour live writers should have anyway
(write at 0); only the one correction tool needs version logic.

### 4. Reference tables are append-on-change

Tables whose content decides which rows a reader uses (`oracle_asset`, protocol/contract
registries, `position_classification`, …) are converted from update-in-place to append-on-change,
using the `security_master` pattern (VEC-411): natural key + `processing_version` in the PK,
`valid_from`, and a `<table>_current` view. Toggling `oracle_asset.enabled` becomes a new row.
Identity-only registries (`token`, pool hubs) are converted last. Once converted they fall under
§1 and get the guard trigger.

*Terminology:* "reference table" is used here instead of the ambiguous "config table": it covers
schemamaster's `config` and `dimension` types plus the read-path enrichment tables brought under
governance in §1. The schemamaster type name `config` itself is unchanged.

Reference data is **bitemporal**, and both axes must be pinned explicitly:

- *Knowledge time* — which reference rows existed: the MVCC snapshot (§5) for a calculation,
  the run's `reference_snapshot` (§2) for a writer.
- *Effective time* — which of those rows applied: `valid_from <= :effective_at`, where
  `effective_at` is an **explicit, recorded parameter** (the calculation's request "as of", the
  writer run's `reference_effective_at`), never `now()`/`CURRENT_DATE`. The existing
  `security_master_current` view filters `valid_from <= (now() AT TIME ZONE 'utc')::date`; a
  future-dated reference row visible in the snapshot would therefore flip a later replay. So
  `_current` views are for operational reads only and are **banned from calculation and writer
  SQL**; those use `<table>_as_of(effective_at)` functions/views (or the inline predicate) with
  the recorded value. Schemamaster lint: no `_current` view and no `now()`/`CURRENT_DATE` in
  calculation SQL.

With both axes recorded, "reference data as of the run" — for the calculation *and* for the
writer of each input row — is recoverable exactly, with no separate reference-data snapshot store.

### 5. Snapshot-exact reads: `ingest_xid` and recorded MVCC snapshots (internal mechanism)

A calculation must be able to say, later and exactly, which rows it saw. Wall-clock "as of"
cannot: a row is stamped at its transaction *start* but becomes visible at *commit*, so a long
batch that started before T can appear after T; clocks can step; and a multi-statement calc under
`READ COMMITTED` sees a different snapshot per statement. Postgres' own visibility primitive has
none of these problems, so it is the mechanism:

- Every governed table gets `ingest_xid xid8 DEFAULT pg_current_xact_id()` (added nullable with
  no default first — metadata-only on compressed hypertables — then `SET DEFAULT`; existing rows
  stay `NULL`, meaning "predates tracking, always visible"). Never supplied by a writer (lint; no
  `INSERT` names the column).
- Every governed table also gets `ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()` — a
  **human label only** (freshness dashboards, "roughly when did we learn this", the manifest
  header), never the audit key. It is `timestamptz`, the cluster and every application session
  run `TimeZone = 'UTC'`, and it is serialised as RFC 3339 UTC with microseconds and an explicit
  `Z`; a naive `timestamp` column on a governed table is a defect (VEC-551 is what happens when
  time columns are ambiguous).
- A calculation runs all its reads in one `REPEATABLE READ` transaction on one connection,
  takes `pg_current_snapshot()::text` **first**, and writes its record (§6) in that same
  transaction. This is per calculation, not per API: each request holds its own pool connection
  for its own transaction, as today. Fan-out inside one calculation is still possible — the first
  connection calls `pg_export_snapshot()` and workers `SET TRANSACTION SNAPSHOT` to read the
  identical snapshot (same server; snapshots do not cross primary/replica). Read-only
  `REPEATABLE READ` takes no locks; only minutes-long snapshots (vacuum lag) need attention. A
  calculation may run entirely on a replica: the replica's snapshot is exact for that replica and
  every row in it is committed on the primary.
- Internal replay = `WHERE ingest_xid IS NULL OR pg_visible_in_snapshot(ingest_xid, :snap)` then
  the ordinary latest-wins rule. `pg_visible_in_snapshot` is a pure function of stored values;
  in-flight writers at snapshot time (a running backfill) are in the snapshot's in-progress list
  and are excluded on replay exactly as they were invisible to the calculation — no staleness,
  no watermark to push back. Replay works on **any node of the same cluster** — primary, an
  existing replica, or a replica created later — because physical replicas share the xid space
  and receive `ingest_xid` as data. It also needs no LSN bookkeeping: every input row was
  committed before the snapshot, and the snapshot before the record (same transaction, snapshot
  first), so **a node that can read the calculation record has already replayed every row the
  snapshot can see.**
- **End-to-end self-check:** the manifest job recomputes the calculation from the manifest it
  just wrote and compares with the recorded output; a mismatch (a read outside the transaction,
  a non-governed input, nondeterminism) raises an alert. This guards the whole class, not just
  the cases listed under Threats. A scheduled assurance job extends the same check backwards:
  it samples historical calculation records, regenerates each manifest from the recorded
  snapshot, re-runs the calculation at the recorded code identity, and alerts on drift
  (PRD RP-4.8) — reproducibility is verified continuously, not only at write time.
- **Replay is read-only by construction** (PRD RP-4.7): replay and manifest generation run
  under a read-only role on the production cluster or any physical replica. Where isolation
  from production is required, the environment is created by physical fork or physical
  restore, never logical dump (see Threats: a logical restore resets the xid space). The
  read-only replay role and the replica/fork runbook are Migration Plan items.

The snapshot is **not** the third-party deliverable — it only resolves against our database and
nobody outside ever sees it. It is what lets us generate the calculation manifest (§6) exactly,
at any later time, off the request path, because governed rows are never removed.

**Why the MVCC snapshot and not an `ingested_at` watermark.** Both were designed in full (the
watermark is in Alternatives). The comparison that decided it:

| | MVCC snapshot (`ingest_xid` + `pg_current_snapshot`) — **chosen** | `ingested_at` watermark (`T = LEAST(now()−ε, min(xact_start))`) |
|---|---|---|
| Exactness | By construction (commit-time visibility); zero staleness | Exact in practice, but needs ε for the clock-read→`pg_stat_activity` window, and relies on `pg_stat_activity` completeness on the primary |
| Clocks | Not involved | NTP steps, VM migration, failover skew can stamp rows "before" an issued T; UTC/`timestamptz` discipline required for the key |
| Long writer transaction (backfill) | Invisible to the calc, excluded on replay; calcs stay fresh | Pushes T back for its duration → every calc that stale; needs alert + cap |
| Two-phase commit | Handled by MVCC | Needs `pg_prepared_xacts` special-casing |
| Replica reads | Snapshot taken on the replica is exact for it | Needs `t_lsn` and a replay-LSN guard |
| Connections | One transaction per calc (or `pg_export_snapshot`) | Any connection; plain filter |
| Topology | Cluster-local: xid space does not survive dump/restore or sharding | Portable: plain timestamp on shards, restores, exports |
| Third-party visibility | Opaque — irrelevant, the manifest is the deliverable | Human-readable |
| Compression pruning | None for `pg_visible_in_snapshot` | None for `≤ T` on existing chunks |
| Extra columns | `ingest_xid` + `ingested_at` (label) | `ingested_at` only |

The decisive points: the snapshot removes the timing/clock failure modes entirely rather than
mitigating them, and never stalls calculations behind a backfill; the watermark's only decisive
advantage — portability across shards and logical restores — is not a current plan (TimescaleDB
multi-node was removed; TigerData scales with replicas, forks and tiering, which the snapshot
handles). If sharding or a logical migration ever becomes a plan, §5 can be switched to the
watermark without changing the manifest format, the record's meaning, or anything a third party
sees; the migration is "add the filter, stop recording the snapshot".

### 6. Calculation record and self-contained manifest

Reproducibility must not be gated on access to our database. Every calculation (dry-run or not)
therefore produces two artefacts:

**a. Record** (insert-only, governed): `id, calculation_type, run_id (calc artefact + reference
snapshot), schema_version (last applied migration), request/params including the effective
"as of" time (`effective_at`, also used for every reference-table `valid_from` predicate),
snapshot (§5), output, manifest_key, manifest_hash, is_dry_run, created_at`; `id` is returned in
the response. Written in the same transaction as the reads. The Python API registers its
artefact and opens a `writer_run` at startup like the Go binaries (it does not today), and
calculation logic
never reads wall-clock time, environment/configmap values, caches, or external services — every
input is either a governed row visible in the snapshot or a field of the recorded request.

**b. Manifest** — one object in the archive bucket (`calc/<id>.jsonl.zst`, alongside the raw
block and SC-call archives), containing everything a third party needs and nothing that requires
our database:

- the record header (request, effective time, `ingested_at` label of the newest input as RFC 3339
  UTC, calc `git_hash`, `schema_version`);
- **every input row** as its recipe (§8) plus the row's **values**: table, natural key,
  `block_number`/`block_version`/`processing_version` where present, `run_id` → writer artefact
  (`git_hash`, `service`, `image_digest`) and the run's `reference_snapshot`/`reference_effective_at`;
- for on-chain rows, the **full, immutable raw-archive object key(s)** from which the row can be
  re-derived by running the writer's build. The SC-call archive key is
  `raw-sc-calls/chain_id=…/block=…/{block}_{block_version}_{source}_{batch_hash}.jsonl.zst`; the
  batch hash is known to the writer at write time, so governed rows written from archived calls
  carry `archive_batch` (the 16-hex batch hash; constant across a batch, dictionary-compresses)
  and the key is fully derivable — `chain / block / block_version / source` alone is only a
  listing prefix. Referencing is sufficient (the archives are immutable and kept indefinitely);
  the manifest job may additionally copy the referenced raw objects under `calc/<id>/raw/` when a
  fully self-contained per-calculation folder is wanted, at the cost of duplicated storage.
  Off-chain rows are terminal facts and their values are taken as given;
- the reference rows used (they are governed rows too);
- a **selection statement**: which rule chose the input rows (identified by the calc `git_hash`
  + `schema_version`, with its parameters — protocol, asset, prime, …) and a **per-chain
  cutoff** (highest `block_number`/`block_version` our governed data was complete to at the
  snapshot). Protocol-wide models such as gap-sweep read "latest row per user" over what we had
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
under `pg_visible_in_snapshot(ingest_xid, snapshot)` (exact by §5) and writes the object; the
request path pays only for the record. Because governed rows are never removed, a manifest can be
(re)generated at any later time; the record's snapshot is the fallback pointer, never the
deliverable. The job runs on commit of the
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
`processing_version`. The `transformed.*` read models qualify as a canonical read surface for
calculations only once converted to append-only under §1.

### 8. Data-point recipe: the unit a single data point reproduces from

Every governed row has a **recipe** — the minimal self-contained description a third party needs
to recreate that one data point without our database:

| Field | Source |
|---|---|
| `table`, natural key, `block_number`, `block_version`, `processing_version` | the row's identity |
| `git_hash`, `service`, `image_digest` | `writer_run[run_id] → build_registry` — the writer's exact code artefact (rebuild or retained image) |
| writer reference data: `reference_snapshot`, `reference_effective_at` | `writer_run[run_id]` — the reference rows the writer had, resolvable against the append-only reference tables (§4) |
| `source` and the **full raw-archive object key(s)** (`…/{block}_{block_version}_{source}_{archive_batch}.jsonl.zst`) | which binary and exactly which S3 objects the row derives from; on-chain only |
| `chain_id`, contract addresses/log identity where the row has them | to re-fetch from any archive node instead of our S3 |
| value hash (or the values) | to compare a reproduction against what we stored |

Off-chain rows have a recipe too (identity, artefact, run reference data, `source`, fetched-at, values)
but no raw-archive key; they are terminal facts.

**Archiving is a verified invariant, not best-effort.** Today `archiving/multicaller.go` logs a
failed archive write as a "permanent, unretried loss" and the row is still written; a recipe that
names an object which does not exist is worthless. Decision: a data-quality check enumerates
governed rows with an `archive_batch` and verifies the object exists (`HEAD`), alerting on any
gap; failures at write time are retried with backoff and counted; if the gap rate is not zero in
practice, archiving becomes a prerequisite of the row write for the affected sources.

The recipe is not a new table: it is a projection over columns that already exist (`run_id`,
`archive_batch`) plus the `writer_run`/`build_registry` joins, and the manifest (§6) is a list of
recipes with values. What this section
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
| Reads spread across several connections or transactions, or the record written best-effort/afterwards | §5/§6: one `REPEATABLE READ` transaction per calculation, snapshot taken first, record written in the same transaction; fan-out only via `pg_export_snapshot`. Without this the manifest job cannot know which rows the calculation actually saw. |
| Wall-clock, environment, cache or external-service inputs inside the calculation | §6: forbidden; the effective time is a field of the recorded request. |
| Calculation code without an identity (Python API today), or schema-resident logic (`_as_of` functions, tie-break rules) not pinned | §6: Python registers an artefact + run; the record carries `schema_version`. A third party rebuilds schema at that migration and code at that commit (or takes the retained image). |
| A reference lookup uses `_current` (`valid_from <= now()`), so a future-dated reference row that is visible in the snapshot flips a later replay | §4: reference data is bitemporal; every calculation/writer reference read uses the recorded `effective_at`; `_current` views and `now()`/`CURRENT_DATE` are banned from calculation and writer SQL (schemamaster lint). |
| A row's recipe names the writer's code but not the reference data the writer ran with, so "why this row exists / which calls were made" cannot be re-derived from chain | §2: rows carry `run_id`; `writer_run` records `reference_snapshot` + `reference_effective_at`; reference tables are append-only, so the writer's reference data is exactly recoverable. |
| One `git_hash` maps to several service images or a rebuilt digest, so the retained-image fallback cannot name the image that wrote a row | §2: `build_registry` keyed by `(git_hash, service, image_digest)`; rows → `run_id` → artefact. |
| The recipe's archive locator is a listing prefix, not an object key, or the object was never written (best-effort archiver) | §6/§8: rows carry `archive_batch`, the key is fully derivable; archive existence is verified by a data-quality check with alerting, and archiving becomes a write prerequisite where gaps occur. |
| Two concurrent corrections allocate the same `processing_version`, or a retried correction cannot find its earlier allocation | §3: per-table advisory lock around allocation; `UNIQUE (table_name, ticket)`; allocate-or-return-existing by ticket. |
| A sanctioned in-place rewrite (`DISABLE TRIGGER` + `UPDATE`, as `20260306`, `20260410_125000`, `20260707` did) changes rows that earlier snapshots point at | Data fixes are new rows at a new `processing_version`. An in-place rewrite of a governed table is exceptional, requires an ADR-referenced migration, and is logged in `processing_version_log` with `reason` naming the calculations it invalidates. |
| Destructive schema migration on a governed table (drop/rename/retype) makes old code unrunnable against a later export | Governed tables are additive-only; deprecations keep the old column/view until no recorded calculation's `build_id` depends on it. |
| Cluster migration via dump/restore (logical) restarts `pg_current_xact_id()` low, so rows written afterwards look older than every stored snapshot | Prefer physical restore/fork (xids preserved — TigerData's backup/fork are physical). After any logical migration, `pg_resetwal -x` sets NextXID above the previous maximum before writes resume; the runbook records it. If sharding ever becomes a plan, switch §5 to the watermark alternative. |
| A writer inserts `ingest_xid = NULL` explicitly and becomes "always visible" | No `INSERT` names `ingest_xid`; lint plus the conformance test. |
| One of a calculation's queries runs outside the `REPEATABLE READ` transaction (another connection, autocommit) | Reads go through a helper bound to the calculation's transaction; lint; end-to-end self-check (§5) compares regenerated output to recorded output. |
| The manifest job runs on a replica that has not replayed the calculation's inputs | Structurally impossible on a physical replica: the job starts from the calculation record, and a node that can read the record has replayed everything the snapshot can see (§5). Only a logical replica or a different cluster (see dump/restore row) can differ. |
| A calculation holds its snapshot for many minutes (vacuum lag on hot tables) | Calculations are request-scoped; a bound on calculation transaction duration; alert on old read-only transactions. |
| `ingested_at`/other time columns rendered without an explicit zone (naive `timestamp`, session `TimeZone` other than UTC) confuse a human or a downstream consumer | `timestamptz` only on governed tables; `TimeZone = 'UTC'` everywhere; RFC 3339 UTC serialisation; schemamaster check for `timestamp without time zone`. |
| `pg_visible_in_snapshot(ingest_xid, …)` gets no pruning on compressed chunks, so heavy manifest regeneration is slow | Performance, not correctness: latest-wins indexes drive the read; the job is off the request path; measure protocol-wide calculations. |
| Under-specified ordering with real ties (`DISTINCT ON`, `last()`, `locf`, cross-table "latest price ≤ block") returns arbitrary rows; float/parallel/hash-order nondeterminism in code | Every canonical selection has a total order (VEC-549 tie-break pattern); calculation code is deterministic given its inputs. |
| Retention or `drop_chunks` on a governed table; tiered data with a lifecycle rule | §1 conformance test; tiering means "kept indefinitely". |
| The image that produced a row or calculation can no longer be rebuilt identically (toolchain/dependency drift, non-reproducible base layers) and the original was pruned from the registry | §2: production images retained indefinitely by digest, `docker_sha` recorded per build; a conformance check that every `build_registry.docker_sha` still resolves in the registry. |
| Manifest never generated (job failure/lag), or generated with a different T than the calculation used | Manifest job is idempotent and retried; `manifest_hash` recorded; regeneration is always possible from the record's T (§5); an alert on records older than N minutes without a manifest. |
| A data point is served without its recipe (API returns values but not `git_hash`/`source`/version identity), so a single-point reproduction needs our database | §8 delivery obligation; response-schema test that every data-point payload carries recipe fields or a resolvable provenance reference. |
| Manifest lists the selected rows but not the selection rule or chain cutoff, so completeness/freshness of the input set can only be checked against our database | §6 selection statement + per-chain cutoff in every manifest; manifest schema check. |
| Manifest omits values for off-chain rows or reference rows, forcing a third party back to our database | Manifest schema check: every input row carries identity **and** values; off-chain rows are terminal facts and must be complete in the manifest. |

Reproducing a wrong result exactly is the contract: corrections appear as new versions in later
snapshots and never change what an earlier calculation saw.

## Migration Plan

Ordered by information lost per day of delay; 1–3 make reproducibility *possible*.

1. **Reference-table append-on-change** (§4) with `_as_of(effective_at)` reads, starting with `oracle_asset`
   and `position_classification` — the only item where waiting destroys information.
2. **`ingest_xid` + `ingested_at`** on governed tables (§5); `build_registry` widened to `(git_hash, service, image_digest)`, `writer_run`, `run_id` and `archive_batch` on governed rows (§2/§8); calculation record + manifest job, Python artefact/run, `schema_version` (§6).
3. **Append-only enforcement** (§1): app role, guard triggers, conformance test.
4. **Trigger removal** (§3): one migration drops the 36 functions/triggers, creates and seeds
   `processing_version_log`; delete the plan-cache/lock/sort tests and `db/migrations/AGENTS.md`
   rules; writers unchanged (they already omit `processing_version`).
5. **`_current` views / read-model routing** (§7), the three version-blind read fixes, and recipe
   fields on data-point API responses (§8).
6. Registry retention policy removed for production repositories and `docker_sha` populated at
   registration (§2) — small, do early. Later: replay tooling (`replay row`, `replay calc`),
   reproducible builds.
7. **Governance completeness and replay access** (§1, §5): classify the `ignore_tables` set in
   `schema_master.json`, convert the `transformed.*` refresh path to append-only, create the
   read-only replay role, and document the physical-fork/restore (never logical dump) rule.

Existing rows keep `processing_version` and `build_id`, with `NULL ingest_xid` (always visible) and `NULL`/cutover `ingested_at`; no other backfill.

## PRD Traceability

This ADR implements the reproducibility half of the *STL Auditability & Reproducibility PRD*;
auditability requirements are deferred to a separate ADR. Per-requirement mapping:

| PRD requirement | Where in this ADR |
|---|---|
| AR-1.1 append-only stores | §1 |
| AR-1.4 retraction as new append | §3 (corrections are new versions) |
| PR-2.2 software version | §2 (`build_registry`: git hash, service, image digest) |
| PR-2.3 input lineage | §6 manifest, §8 recipe |
| PR-2.4 run/config identity | §2 (`writer_run`) |
| PR-2.6 provenance immutability | §1 (registry/log tables are governed, insert-only) |
| PR-2.7 UTC timestamps | §5 (`timestamptz`, `TimeZone = 'UTC'`, RFC 3339); clock sync itself: auditability ADR |
| CR-3.1/3.2 supersession by reference | §3 — identity is natural key + `block_version` + `processing_version`; the supersession chain is the version ordering plus `processing_version_log`; there are no surrogate record IDs |
| CR-3.3 correction reason | §3 (`ticket`, `reason`); approval workflow: auditability ADR |
| CR-3.4 correction history | §1/§7 (raw tables keep every version) |
| CR-3.5 restatement vs valid-time change | §4 (reference data); `block_version` (chain) vs `processing_version` (restatement) |
| CR-3.6 original vs corrected at a past time | §5 snapshot + version filter |
| RP-4.1 as-of queries | §5, bounded to recorded calculations (Point-in-time boundary) |
| RP-4.2 reproduction manifest | §6 |
| RP-4.3 artifact retention | §2 (production images retained indefinitely) |
| RP-4.4 re-execution fidelity | Goal and Boundaries (bit-for-bit via retained image; tolerance TBD otherwise) |
| RP-4.5 determinism | §6 rules + Threats (total ordering; no wall-clock/env/cache) |
| RP-4.6 output ↔ manifest link | §6 (record id in response; `manifest_key`/`manifest_hash`) |
| RP-4.7 isolated re-execution | §5 (read-only replay; fork-not-dump) |
| RP-4.8 periodic re-verification | §5 (self-check + historical sampling) |
| AR-1.2/1.3/1.5/1.6/1.7, PR-2.1/2.5, NFR-1..8, DP-1..10 | Auditability ADR (separate) |

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

**`ingested_at` frozen-prefix watermark instead of the MVCC snapshot** — the alternative
designed in full before deciding (pros/cons table in §5). Mechanism: `ingested_at TIMESTAMPTZ NOT
NULL DEFAULT now()` on governed tables, DB-assigned only; at calc start, on the primary,
`T = LEAST(now() − ε, min(xact_start) of other active backends)` (plus `pg_prepared_xacts` under
2PC) together with `pg_current_wal_lsn()` for replica-read validity; every read filters
`ingested_at ≤ T`; T is recorded and, because a writer that started before T is committed or
aborted, `≤ T` is a frozen prefix. Pros: a plain timestamp filter that any connection, replica,
shard, export or third party can apply; survives logical restore and sharding; human-readable;
no per-calculation transaction. Cons: needs ε for the clock-read→`pg_stat_activity` window; a
long writer transaction (backfill) pushes T back and makes every calculation that stale (needs
alert + cap); clock steps/failover skew can violate the prefix; 2PC and replica lag need
special-casing; strict UTC/`timestamptz` discipline on the key. Rejected because the snapshot
removes those failure modes rather than mitigating them, and the watermark's decisive advantage
(topology portability) is not a current need. Kept as the documented fallback: switching is
"add the filter, stop recording the snapshot", with no change to the manifest.

**Fixed safety margin (`T = now() − k minutes`) as the watermark** — simpler than the computed
T but trades exactness for a guess about maximum transaction duration and makes every
calculation k minutes stale. Rejected outright.

**Snapshot only, no manifest** — compact and exact, but reproducibility would be gated on access
to our database. Rejected; the snapshot is kept as the internal mechanism that generates the
manifest exactly and off the request path.

**Manifest always built synchronously inside the request** — the "naive" shape from VEC-244;
exact, but read amplification lands on the request path for protocol-wide models. Allowed where
the calculation already holds its rows; otherwise asynchronous generation from the recorded
snapshot, which is equally exact.

**Archive raw off-chain responses** — not required; off-chain data points carry no reproduction
claim (Boundaries).

**Row-level immutability trigger / rules** — statement-level trigger has the same guarantee at
lower cost; `RULE … DO INSTEAD NOTHING` fails silently. Rejected.

## Consequences

**Positive**
- Every on-chain data point and every calculation is reproducible by a third party from S3 (raw
  archives + calculation manifests), the code and an archive node — no database access — and
  every calculation's input set is pinned by an exact MVCC snapshot with zero staleness.
- Append-only is a Postgres-enforced property, not a convention; corrections are deliberate,
  logged, and cannot happen by accident at a deploy boundary.
- Removes per-row trigger overhead, advisory locks, plan-cache tuning, sort discipline and 36
  functions; new versioned tables need only the columns and a `_current` view.
- Reference-data history exists, so calculations survive `oracle_asset` toggles and reclassification.

**Negative / trade-offs**
- Reference tables lose update-in-place: writers must append, and identity registries need an
  append-on-change pattern; that is real app-code work, done table by table.
- Role separation on TigerData and guard triggers add migration/ops surface; one-off data fixes
  become deliberate (`DISABLE TRIGGER` in a migration).
- Calculations must run reads in one `REPEATABLE READ` transaction on one connection (or an exported snapshot) and persist a record in it; the API must not use wall-clock, env or cache inputs in calculation logic.
- Governed tables become additive-only; in-place data rewrites are exceptional and logged.
- Two more small columns on governed rows (`run_id`, `archive_batch`) and two small registry
  tables; every binary (including the Python API) opens a run at startup and snapshots its reference data.
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
