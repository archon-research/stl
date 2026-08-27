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
(§5) reproduces exactly what a calculation saw, at any later time. An arbitrary wall-clock
timestamp is served by lookup, not by database time travel: calculation records carry wall-clock
UTC `created_at`, so for any timestamp T the most recent record with `created_at <= T` (per
calculation type) is found and replayed exactly. Agreed as satisfying PRD RP-4.1 (2026-08-21).
Arbitrary as-of queries over the whole database remain a non-goal; if they ever become one, the
`ingested_at` watermark (Alternatives) is the designed upgrade path.

**Cutover.** The guarantees in this ADR apply to rows and calculations from the cutover date
(TBD) onward. Pre-tracking data (`build_id = 0`, `NULL ingest_xid`, archive gaps) is out of
scope for this ADR's guarantees. Information destroyed before §4 lands (reference-data history
overwritten in place) is unrecoverable. Everything else historical — such as best-effort
reproduction of older data points — is a separate, later task ("from now backwards",
2026-08-21), not a claim this ADR makes.

**Scope: reproducibility only.** Auditability controls are deliberately out of scope and are
covered by a separate ADR: actor identity and per-service credentials, tamper-evident
hashing/anchoring, storage-level WORM and retention, correction approvals, audit/access
logging, evidence export, and GDPR handling. This ADR leaves the seams that ADR attaches to:
`writer_run` is where a principal lands, `processing_version_log` is insert-only and extensible
with approval fields, and `manifest_hash` is the hook for signing/anchoring.

## Decision

### 1. Data and reference tables are append-only, and Postgres enforces it

Applies to every table classified `raw_pipeline`, `dimension` or `config` in
`data_quality/schemamaster/schema_master.json` (the latter two are reference tables, §4).
Operational tables (schemamaster type `infrastructure`: `block_states`, `backfill_watermark`,
`reorg_events`, queues and transform queues) are exempt — criteria and rationale below.

**Scope.** The disciplines in this ADR — append-only (§1), `run_id`/`ingest_xid` columns
(§2/§5), caller-assigned `processing_version` (§3), recipe delivery (§8) — apply to every
table whose content can affect a calculation output or a served data point, at write time or
at replay. By class:

- **Facts** (`raw_pipeline`): what happened on-chain, or what an off-chain source returned.
- **Reference tables** (`dimension`, `config`, plus the enrichment and `ref_*` vocabulary
  tables brought under governance below): content that decides which rows a reader uses (§4).
- **Read models** (`transformed.*`): derived, but a calculation input surface once converted
  to append-only (§1/§7) — derived data is governed whenever calculations read it instead of
  the raw tables.
- **Provenance** (`build_registry`, `writer_run`, `processing_version_log`, calculation
  records): insert-only by design and governed regardless of schemamaster classification
  (`build_registry` is typed `infrastructure` today) — a mutable provenance row would
  silently change what a recipe or manifest resolves to (PRD PR-2.6).

**Exemption criteria.** A table may be exempt from governance only if all three hold:

1. **No calculation or served data point ever reads it.** Enforced by the schemamaster lint
   on calculation SQL (Threats, first row), not by convention.
2. **Its content is coordination state, not evidence.** It records where the machinery is —
   a watermark, a queue entry, an orphan flag — and everything evidentiary that passes
   through it lands in a governed table as rows. A reorg, for example, becomes
   `block_version`'d corrective rows in the governed tables (VEC-553); no reader ever joins
   to `block_states.is_orphaned` to decide canonicality.
3. **Its state is derivable or disposable.** It could be reconstructed from governed data
   (a watermark is in essence `MAX(block)` over what was ingested) or is worthless once
   consumed (a queue entry), so retaining its history would add storage and write-path cost
   with zero reproducibility gain.

The third point is also *why* operational tables are exempt rather than governed defensively:
they must mutate to function (a watermark advances in place, a queue deletes processed
entries, `block_states.is_orphaned` flips on reorg and the table carries 30-day retention).
Making them append-only would buy nothing — nothing downstream reads them at replay — while
costing unbounded growth on the hottest small tables. The reproducibility claim never rests
on them: it rests on governed rows plus the calculation record (§5/§6).

Exemption is explicit, never a default: a table is exempt by being classified
`infrastructure` in `schema_master.json`. An unclassified table is a governance gap to be
closed (next paragraph), not an implicit exemption; and if a calculation path ever needs
something an operational table knows, the fix is to write that fact into a governed table,
never to read across the boundary.

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

This mechanism was stress-tested as candidate A1 against five alternatives in the
`stl-row-version` spike (Appendix B: problem statement; Appendix C: verified comparison). It is
the only candidate that passes every hard requirement and the per-table cutover gate without key
surgery. The spike also names two compositions this section must not omit: a **run-status flip**
on `processing_version_log` so a correction's rows across sibling tables become canonical in one
commit (Appendix C, aux 2), and a **payload-hash divergence check** after `ON CONFLICT DO NOTHING`
so two writers producing *different* payloads for one key outside a correction are surfaced,
never silently dropped (aux 5).

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
- **xid monotonicity guard.** Everything above assumes `pg_current_xact_id()` only ever moves
  forward for the life of the data. `ingest_xid` is `xid8` (64-bit, epoch-qualified), so the
  32-bit wraparound that autovacuum manages never touches it. NextXID and the epoch live in
  `pg_control` and travel with the data directory, which sorts operations into two kinds:
  - *Safe (physical, counter carried forward):* base backup + WAL replay, streaming-replica
    promotion, `pg_upgrade` (source-verified: copies `pg_xact`, then `pg_resetwal -x/-e` with
    the *old* values). On TigerData every documented lifecycle operation is one of these:
    compute resize (restart on the same volume, or HA switchover), storage autoscale (volume
    grow), HA failover (standby promotion), fork and PITR (pgBackRest physical restore),
    extension upgrade (`ALTER EXTENSION`).
  - *Unsafe (new installation, counter restarts low):* logical dump/restore, logical
    replication or live-migration into a fresh service, `pg_resetwal` without `-x`.
  - *Caveats:* the cloud docs do not state the mechanism for compute resize (blogs do) or for
    major PostgreSQL upgrades (the ~20-minute window fits `pg_upgrade`). A PITR fork is a *new
    installation* whose counter resumes from the recovery target; rows written on the abandoned
    original after that point carry xids the fork will reissue, so nothing may ever be copied
    logically from the original into the fork (see Threats).
  - *The check:* rather than rely on documentation and runbook alone, every governed writer and
    the calculation API assert on startup, and the assurance job asserts on each run, that

    ```sql
    pg_current_xact_id() > (SELECT max(pg_snapshot_xmax(snapshot::pg_snapshot)) FROM <§6 record table>)
    ```

    and likewise above `max(ingest_xid)` over governed tables where cheap to compute. On failure
    writers refuse to write governed rows and the API refuses to record calculations, alerting
    with the runbook step (`pg_resetwal -x <max+margin>`; on a managed service, restore from a
    physical backup instead).
  - *Failure mode it prevents:* a low counter does not damage existing records — it makes *new*
    rows carry xids below old snapshots' `xmin`, so old replays wrongly include them. The guard
    stops the first such write.
- **End-to-end self-check:** a scheduled assurance job samples calculation records — recent and
  historical, weighted towards fresh records so a regression is caught close to when it ships —
  generates each one's manifest from the recorded snapshot (§6, on demand), re-runs the
  calculation at the recorded code identity, and compares with the recorded output; a mismatch
  (a read outside the transaction, a non-governed input, nondeterminism) raises an alert
  (PRD RP-4.8). This guards the whole class, not just the cases listed under Threats —
  reproducibility is verified continuously.
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
therefore has two artefacts: a **record**, written at calculation time, and a **manifest**,
generated from the record on demand (see Generation):

**a. Record** (insert-only, governed): `id, calculation_type, run_id (calc artefact + reference
snapshot), schema_version (last applied migration), request/params including the effective
"as of" time (`effective_at`, also used for every reference-table `valid_from` predicate),
snapshot (§5), output, manifest_key, manifest_hash, is_dry_run, created_at`; `id` is returned in
the response. `created_at` is wall-clock UTC and indexed per `calculation_type`, so the nearest
prior record to an arbitrary timestamp is a lookup (Point-in-time boundary). Written in the same
transaction as the reads. The Python API registers its
artefact and opens a `writer_run` at startup like the Go binaries (it does not today), and
calculation logic
never reads wall-clock time, environment/configmap values, caches, or external services — every
input is either a governed row visible in the snapshot or a field of the recorded request.

**b. Manifest** — one object (`calc/<id>.jsonl.zst`; when persisted it lands in the archive
bucket alongside the raw block and SC-call archives), containing everything a third party needs
and nothing that requires our database:

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
  manifest generation may additionally copy the referenced raw objects under `calc/<id>/raw/` when a
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

**Generation.** The record is the durable artefact; the manifest is **generated on demand**
from it (decided 2026-08-21; standards research confirms eager persistence is not required).
Because governed rows are never removed, the same generation — re-running the calculation's
input selection under `pg_visible_in_snapshot(ingest_xid, snapshot)`, exact by §5 — yields the
identical manifest at any later time: for an auditor's request, for the assurance sampling job
(§5), or inline when the calculation already holds its input rows and wants to hand the
manifest back immediately. Generation is idempotent (same record → same key and hash); a
manifest that *is* written goes to the archive bucket and fills `manifest_key`/`manifest_hash`
insert-only (a second record row referencing the first, or a separate `calculation_manifest`
table). Should a persisted-manifest obligation ever appear, the eager shape is the same
generator triggered on commit of the record (queue or poll on `manifest_key IS NULL`) with an
alert on records older than N minutes without a manifest — a deployment change, not a design
change. Final shape and API surface are VEC-232's; this ADR fixes what the two artefacts must pin.

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
| Reads spread across several connections or transactions, or the record written best-effort/afterwards | §5/§6: one `REPEATABLE READ` transaction per calculation, snapshot taken first, record written in the same transaction; fan-out only via `pg_export_snapshot`. Without this, manifest generation cannot know which rows the calculation actually saw. |
| Wall-clock, environment, cache or external-service inputs inside the calculation | §6: forbidden; the effective time is a field of the recorded request. |
| Calculation code without an identity (Python API today), or schema-resident logic (`_as_of` functions, tie-break rules) not pinned | §6: Python registers an artefact + run; the record carries `schema_version`. A third party rebuilds schema at that migration and code at that commit (or takes the retained image). |
| A reference lookup uses `_current` (`valid_from <= now()`), so a future-dated reference row that is visible in the snapshot flips a later replay | §4: reference data is bitemporal; every calculation/writer reference read uses the recorded `effective_at`; `_current` views and `now()`/`CURRENT_DATE` are banned from calculation and writer SQL (schemamaster lint). |
| A row's recipe names the writer's code but not the reference data the writer ran with, so "why this row exists / which calls were made" cannot be re-derived from chain | §2: rows carry `run_id`; `writer_run` records `reference_snapshot` + `reference_effective_at`; reference tables are append-only, so the writer's reference data is exactly recoverable. |
| One `git_hash` maps to several service images or a rebuilt digest, so the retained-image fallback cannot name the image that wrote a row | §2: `build_registry` keyed by `(git_hash, service, image_digest)`; rows → `run_id` → artefact. |
| The recipe's archive locator is a listing prefix, not an object key, or the object was never written (best-effort archiver) | §6/§8: rows carry `archive_batch`, the key is fully derivable; archive existence is verified by a data-quality check with alerting, and archiving becomes a write prerequisite where gaps occur. |
| Two concurrent corrections allocate the same `processing_version`, or a retried correction cannot find its earlier allocation | §3: per-table advisory lock around allocation; `UNIQUE (table_name, ticket)`; allocate-or-return-existing by ticket. |
| A sanctioned in-place rewrite (`DISABLE TRIGGER` + `UPDATE`, as `20260306`, `20260410_125000`, `20260707` did) changes rows that earlier snapshots point at | Data fixes are new rows at a new `processing_version`. An in-place rewrite of a governed table is exceptional, requires an ADR-referenced migration, and is logged in `processing_version_log` with `reason` naming the calculations it invalidates. |
| Destructive schema migration on a governed table (drop/rename/retype) makes old code unrunnable against a later export | Governed tables are additive-only; deprecations keep the old column/view until no recorded calculation's `build_id` depends on it. |
| Cluster migration via dump/restore (logical), logical replication into a new service, or `pg_resetwal` without `-x` restarts `pg_current_xact_id()` low, so rows written afterwards look older than every stored snapshot | Prefer physical restore/fork (xids preserved — TigerData's backup/fork are pgBackRest physical restores; resizes, HA failover and `pg_upgrade` carry `pg_control` forward — §5 lists what is documented versus inferred). After any logical migration, `pg_resetwal -x` sets NextXID above the previous maximum before writes resume; the runbook records it. Enforced, not just documented: the §5 xid monotonicity guard blocks governed writes and calculation records while `pg_current_xact_id()` is below the maximum recorded snapshot. If sharding ever becomes a plan, switch §5 to the watermark alternative. |
| After a PITR fork/restore, rows or calculation records written on the abandoned original after the recovery target are salvaged into the fork (logical copy), colliding with xids the fork has since reissued | Never copy governed rows between installations; a fork is cut over whole or not at all. Salvage, if unavoidable, re-ingests from chain/archive as new rows (fresh `ingest_xid`, new `run_id`) and drops the original's post-target calculation records. |
| A writer inserts `ingest_xid = NULL` explicitly and becomes "always visible" | No `INSERT` names `ingest_xid`; lint plus the conformance test. |
| One of a calculation's queries runs outside the `REPEATABLE READ` transaction (another connection, autocommit) | Reads go through a helper bound to the calculation's transaction; lint; end-to-end self-check (§5) compares regenerated output to recorded output. |
| Manifest generation runs on a replica that has not replayed the calculation's inputs | Structurally impossible on a physical replica: generation starts from the calculation record, and a node that can read the record has replayed everything the snapshot can see (§5). Only a logical replica or a different cluster (see dump/restore row) can differ. |
| A calculation holds its snapshot for many minutes (vacuum lag on hot tables) | Calculations are request-scoped; a bound on calculation transaction duration; alert on old read-only transactions. |
| `ingested_at`/other time columns rendered without an explicit zone (naive `timestamp`, session `TimeZone` other than UTC) confuse a human or a downstream consumer | `timestamptz` only on governed tables; `TimeZone = 'UTC'` everywhere; RFC 3339 UTC serialisation; schemamaster check for `timestamp without time zone`. |
| `pg_visible_in_snapshot(ingest_xid, …)` gets no pruning on compressed chunks, so heavy manifest regeneration is slow | Performance, not correctness: latest-wins indexes drive the read; the job is off the request path; measure protocol-wide calculations. |
| Under-specified ordering with real ties (`DISTINCT ON`, `last()`, `locf`, cross-table "latest price ≤ block") returns arbitrary rows; float/parallel/hash-order nondeterminism in code | Every canonical selection has a total order (VEC-549 tie-break pattern); calculation code is deterministic given its inputs. |
| Retention or `drop_chunks` on a governed table; tiered data with a lifecycle rule | §1 conformance test; tiering means "kept indefinitely". |
| The image that produced a row or calculation can no longer be rebuilt identically (toolchain/dependency drift, non-reproducible base layers) and the original was pruned from the registry | §2: production images retained indefinitely by digest, `docker_sha` recorded per build; a conformance check that every `build_registry.docker_sha` still resolves in the registry. |
| On-demand manifest generation fails, drifts, or uses a different snapshot than the calculation did | Generation is idempotent and driven solely by the record's snapshot (§5), so it is always repeatable; `manifest_hash` recorded when persisted; the assurance sampling job continuously generates and recomputes, alerting on drift. |
| A data point is served without its recipe (API returns values but not `git_hash`/`source`/version identity), so a single-point reproduction needs our database | §8 delivery obligation; response-schema test that every data-point payload carries recipe fields or a resolvable provenance reference. |
| Manifest lists the selected rows but not the selection rule or chain cutoff, so completeness/freshness of the input set can only be checked against our database | §6 selection statement + per-chain cutoff in every manifest; manifest schema check. |
| Manifest omits values for off-chain rows or reference rows, forcing a third party back to our database | Manifest schema check: every input row carries identity **and** values; off-chain rows are terminal facts and must be complete in the manifest. |

Reproducing a wrong result exactly is the contract: corrections appear as new versions in later
snapshots and never change what an earlier calculation saw.

## Migration Plan

Ordered by information lost per day of delay; 1–3 make reproducibility *possible*.

1. **Reference-table append-on-change** (§4) with `_as_of(effective_at)` reads, starting with `oracle_asset`
   and `position_classification` — the only item where waiting destroys information.
2. **`ingest_xid` + `ingested_at`** on governed tables (§5) with the xid monotonicity guard in writer/API startup and the assurance job; `build_registry` widened to `(git_hash, service, image_digest)`, `writer_run`, `run_id` and `archive_batch` on governed rows (§2/§8); calculation record + on-demand manifest generation, Python artefact/run, `schema_version` (§6).
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
| RP-4.1 as-of queries | §5 + §6 record `created_at` lookup — nearest prior calculation to an arbitrary timestamp (agreed 2026-08-21; Point-in-time boundary) |
| RP-4.2 reproduction manifest | §6 (generated on demand from the record) |
| RP-4.3 artifact retention | §2 (production images retained indefinitely) |
| RP-4.4 re-execution fidelity | Goal and Boundaries (bit-for-bit via retained image; tolerance TBD otherwise) |
| RP-4.5 determinism | §6 rules + Threats (total ordering; no wall-clock/env/cache) |
| RP-4.6 output ↔ manifest link | §6 (record id in response; `manifest_key`/`manifest_hash`) |
| RP-4.7 isolated re-execution | §5 (read-only replay; fork-not-dump) |
| RP-4.8 periodic re-verification | §5 (assurance sampling job, recent + historical) |
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
the calculation already holds its rows; otherwise on-demand generation from the recorded
snapshot, which is equally exact.

**Archive raw off-chain responses** — not required; off-chain data points carry no reproduction
claim (Boundaries).

**Row-level immutability trigger / rules** — statement-level trigger has the same guarantee at
lower cost; `RULE … DO INSTEAD NOTHING` fails silently. Rejected.

**Other row-versioning mechanisms (spike candidates A2–A6)** — single-axis SCD2 everywhere,
an observation log with derived read models, range keys with overlap exclusion,
content-addressed idempotency, and no correction column at all. Assessed against verified
environment facts in Appendix C: A4 and A6 are dead on arrival (tiered chunks forbid the
range-closing `UPDATE`; a late live retry outranks a correction and commit timestamps are
vacuumed), A2 fits reference tables only, A5 survives as the payload-hash check, and A3 is the
one alternative worth a toy comparison.

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
- Manifests carry input values but are generated on demand, so archive storage grows with
  audit/assurance demand rather than calculation volume; the assurance sampling job and its
  monitoring become part of the calculation path.
- Container-registry storage grows without bound (one image per production build, ~1.6/day
  today); no lifecycle rules on the production repositories.

## Appendix A: ADR-0002 §3 mechanism (2026-04-08, replaced by §3 above)

ADR-0002 assigned `processing_version` in a per-table `BEFORE INSERT` trigger: advisory-lock the
natural key, look up an existing row with the same key **and** `build_id` (retry → reuse its
version), else `MAX(processing_version)+1` (reprocess → new version); callers had to sort batch
inserts by natural key to avoid deadlocks, every function needed
`plan_cache_mode = force_custom_plan` on hypertables (generic plans cannot prune chunks; measured
4,410 ms vs 148 ms per 721-row batch at ~2,000 chunks, VEC-541), and VEC-185 added covering
indexes. The migration files (`20260410_*`, `20260424_120000`, `20260428_120000`,
`20260806_1[23]0000`) remain the authoritative record of what ran.

## Appendix B: Spike — replacing `processing_version`, portable problem statement (2026-08-24)

The following two appendices are the `stl-row-version` spike, reproduced verbatim apart from
heading levels and cross-references. The spike deliberately reopened the row-versioning design
space with §3's mechanism as *one* candidate (A1) among six, stated the problem
solution-neutrally (requirements R1–R14, incumbent failure modes P1–P8, stress scenarios S/C/Q),
and researched each candidate against verified PostgreSQL 17 / TimescaleDB 2.29 / TigerData
behaviour. Outcome: A1 — caller-assigned versions plus the insert-only allocation log — is the
only candidate passing every hard requirement and every migration gate, provided it is composed
with the auxiliary mechanisms (run-status flip for sibling atomicity, payload-hash divergence
detection, stored `xid8` + recorded snapshot for replay — the last is §5). A3 (observation log +
derived read models) is the one architecture worth a toy comparison; A2/A4/A5/A6 are ruled out
with the fact that kills each. Two findings correct assumptions elsewhere: exclusion constraints
*are* supported on hypertables (partition column included), and partial unique indexes cannot be
added while chunks are compressed. Section numbers inside these appendices (§1–§8) refer to the
spike document, not to this ADR's Decision sections.

**Status**: Spike input, 2026-08-24
**Audience**: a new, empty repository. This document is self-contained: it assumes no access
to the stl codebase. Names, tables and domains in the toy may differ freely from stl's — what
must be preserved is the *pattern*, the invariants, and the failure modes, so that a candidate
mechanism proven in the toy transfers back.

### 1. Purpose

stl versions database rows with a `processing_version` column assigned by per-table
`BEFORE INSERT` triggers (ADR-0002, April 2026). Four months of production use showed the
write-side machinery is the dominant cost: escalating fixes (covering indexes, advisory locks,
plan-cache pinning), 36+ near-identical trigger functions rewritten wholesale twice, and — the
decisive datum — **the trigger never produced a deliberate correction**. Every
`processing_version > 0` row in production (~880) is a payload-identical duplicate minted at a
deploy boundary; in one case an *older* build's row became canonical over a newer build's.

A successor design exists as a proposed (not accepted) ADR-0006. **This spike deliberately
reopens the design space**: ADR-0006's row-versioning mechanism is one candidate among several
(candidate list below), not the presumed answer. The toy repo's job is to implement the problem below,
reproduce the incumbent's failure modes as baselines, and stress candidates against them.

The spike evaluates **destinations only**. Migration from the incumbent is out of scope, except
for the constraints in §8 that make some destinations unreachable.

### 2. The system being modeled

stl ingests state from an external system (a blockchain) and sells conclusions drawn from it
(risk calculations). Three properties shape everything:

1. **The source restates its own history.** A chain reorg rewrites recent blocks. This is an
   *external* mutation: the source changed, our code was fine.
2. **Our code has bugs.** A broken indexer writes wrong rows for a range; after a fix, the range
   is reprocessed. This is an *internal* mutation: the source was fine, our reading of it wasn't.
3. **Nothing may be destroyed.** Results are audited. "What did we believe at the time, and why"
   must remain answerable forever — a correction must never make the original unreachable.

So every time-varying value is stored append-only under a three-part version key:

- a **source position** (`block_number` — where in the source's own order),
- a **source restatement counter** (`block_version` — bumped when the source rewrites that position),
- a **correction counter** (`processing_version` — bumped when *we* rewrite our reading of it).

"Current" is never a column; it is the query
`ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1` per natural key.

Two table families share the column today:

- **Family A — pipeline snapshots** (~45 TimescaleDB hypertables): high-volume, block-keyed
  observations written by workers, cronjobs and backfillers. `processing_version` is
  trigger-assigned (the mechanism under replacement).
- **Family B — curated masters** (securities/entities reference data): low-volume, human/loader-
  curated. Rows carry `valid_from` (effective date) as the primary ordering axis;
  `processing_version` is loader-assigned and breaks same-day ties; `_current` and `_versions`
  views derive `valid_to` via `lead()`.

Around them sit **reference tables** (which oracles/tokens/contracts to poll, classifications,
enabled flags) whose content decides what writers write and readers read — today partly
update-in-place, which destroys history — and **calculations**, readers whose input set must be
reproducible later, exactly.

#### Writer population (fixed operational facts)

- Live **workers** run continuously; **cronjobs** run on schedules; **backfillers** re-cover
  historical ranges, sometimes while the live worker is running.
- Kubernetes rolling deploys mean two instances of the same writer briefly overlap.
- Pods crash and message queues redeliver: the same logical write arrives more than once.
- Deploys happen ~1.6×/day; deliberate corrections are **rare** (months apart) and always
  operator-initiated.
- **Concurrency assumption the spike may rely on**: at most one *correction run* per table at a
  time (operationally enforced). Live-path collisions — deploy overlap, retry storms, a
  backfiller running beside a worker — are normal and must be harmless. Do not design for
  concurrent correction runs; do not assume away live-path collisions.

### 3. The problem, solution-neutral

A candidate mechanism must deliver all of the following. Phrase is behavioral; any mechanism
that produces the behavior qualifies.

- **R1 — Append-only.** Originals are never destroyed or made unreachable. Corrections are
  additive. Enforced by the database (privileges/guards verifiable from the catalog), not by
  convention (**R13**).
- **R2 — Idempotent live writes.** Replaying the same logical write — crash retry, queue
  redelivery, deploy overlap, backfiller re-covering an already-ingested range — leaves the
  database byte-identical. No phantom versions, ever. If two *different* payloads collide on the
  same identity outside a correction, that is surfaced (error or alert), never silently resolved.
- **R3 — Deliberate corrections.** A correction run supersedes prior generations for the keys it
  covers; it is attributable (ticket, reason, code identity), idempotently re-runnable after a
  crash, and its rows win over all prior generations for those keys. Per-key gaps in the version
  sequence are acceptable.
- **R4 — Two mutation axes stay distinct.** Source restatement (external) and correction
  (internal) are separately recorded and separately queryable: "what did the source say at
  position N, before and after its restatement" and "what did our code conclude, before and
  after our fix" are both plain queries.
- **R5 — Current is structural and cheap.** One named object per table answers latest-per-key;
  the ordering/tie-break logic exists in exactly one place, not at every call site.
- **R6 — Reproducible reads.** A calculation can record what it read such that the identical row
  set (and thus output) is recoverable later, exactly — after corrections, backfills and
  restatements have landed, and without the recording having raced an in-flight backfill.
- **R7 — Reference data is historized and bitemporal.** Rows whose content steers reads/writes
  (enabled flags, classifications, rosters) keep history. Two time axes are pinned explicitly:
  *knowledge time* (which rows existed when the reader ran) and *effective time* (which rows
  applied, `valid_from <= :effective_at` with `effective_at` an explicit recorded parameter,
  never `now()`).
- **R8 — Cohort correctness is expressible.** Entities that silently stop being observed (no
  tombstones) must not pollute "current" aggregates forever. The mechanism must give readers a
  structural way to scope, or make tombstoning cheap.
- **R9 — Sibling consistency.** Tables written together for one key read back consistently:
  a reader never sees a torn key (one sibling at the old generation, another at the new).
- **R10 — New tables are cheap.** Adding a versioned table requires declarative artifacts only
  (DDL, maybe a view). No per-table procedural code, no hand-maintained conformance constants.
- **R11 — Humans can reason about it.** The standard questions (§6, Q3) are answerable with
  plain SQL against documented objects.
- **R12 — Insert cost is flat in table age.** Write cost must not grow with chunk/partition
  count or total history size.
- **R14 — Works compressed.** All of the above holds with columnstore compression enabled on
  old chunks, and nothing relies on features unavailable on TigerData (§7).

**Non-goals**: multi-node sharding; survival of logical dump/restore (physical fork/restore is
the assumed path — but a candidate that *also* survives logical restore should say so, it's
worth points); arbitrary wall-clock time travel over the whole database; tamper evidence;
concurrent correction runs per table.

**Open question the spike may answer**: is build/deploy identity a sound primitive at all?
The incumbent infers intent (retry vs. correction) from build identity and got it wrong both
ways — it fired on deploy boundaries (wrong) and cannot fire on a same-build re-run after a
reference-data fix (also wrong). Candidates may replace it with explicit operator intent,
content hashing, transaction identity, or anything else that satisfies R2/R3.

### 4. The incumbent, and its measured failure modes

The mechanism being replaced (reproduce this in the toy as the baseline):

Every versioned table has a `BEFORE INSERT` trigger that (1) takes
`pg_advisory_xact_lock(hash(natural key))`, (2) looks up an existing row with the same natural
key *and the same build identity* — if found, this is a retry: reuse its `processing_version`;
(3) else assigns `MAX(processing_version)+1` over the natural key. The insert then runs with
`ON CONFLICT DO NOTHING` on a PK that includes `processing_version`. Batch writers must sort
rows by natural key to avoid deadlocks. On TimescaleDB, every trigger function must carry
`SET plan_cache_mode = 'force_custom_plan'` or its `MAX()` lookup stops pruning chunks.

Measured failure modes — the toy must be able to reproduce P1–P3; P4–P8 inform the tests in §6:

| # | Failure mode | Measurement in stl |
|---|---|---|
| P1 | Insert cost grows with chunk count: plpgsql switches to a generic plan after ~5 calls; generic plans can't prune hypertable chunks, so the per-row `MAX()` fans out over all of them. Fires even for rows `ON CONFLICT` then discards. | 4,410 ms vs 148 ms per 721-row batch at ~2,000 chunks (30×); 143× in a clean repro; worst batch 464 s; leaked into workflow-engine activity timeouts. The fix (`SET plan_cache_mode`) is silently reset by any `CREATE OR REPLACE FUNCTION` and guarded only by a catalog test with a hand-bumped floor constant. |
| P2 | Phantom versions: every deploy boundary makes the next payload-identical write look like a correction. | ~880 phantom rows across 6+ tables in 4 months; zero deliberate corrections; one case where an older build's row became canonical over a newer build's. |
| P3 | Write-race version loss: without the advisory lock, two same-key writers with different builds both compute the same next version; `ON CONFLICT DO NOTHING` silently drops the loser. | A real production bug (VEC-194), now a regression test plus sort discipline in every batch writer. |
| P4 | Per-table procedural boilerplate: ~45 near-identical trigger functions, differing only in table name and key columns. | Rewritten wholesale twice (adding locks; plan-cache fix); each new table adds a function, a trigger, an index and a test-floor bump. |
| P5 | Read-side leakage: the ordering tuple is hand-written at every call site. | ~29 `DISTINCT ON` sites plus dozens of lateral joins across two languages; one file repeats the tuple ~14 times; a forgotten tie-breaker "gets a wrong answer silently". |
| P6 | Cohort trap: latest-per-key over an append-only snapshot keeps decommissioned entities current forever. | A $521M vs $310M aggregate discrepancy (closed packages' stale last rows, still flagged active); a second case where repaid loans linger in a `_current` view indefinitely. |
| P7 | Lockstep fragility: sibling tables version independently; readers join on version equality, guarded only by a comment saying the writers stay in lockstep. | Comment-guarded assumption, no enforcement. |
| P8 | Family B backdated-correction hazard: with `valid_from` as the primary ordering axis, a correction with a higher version but an earlier `valid_from` never becomes current in any view. | Documented invariant enforced only by loader convention. Also: 0-based and 1-based version numbering coexist under one column name. |

### 5. The toy model

Recreate the *pattern* with a small neutral domain. Suggested shape (rename freely) — a fleet of
metering devices reporting through a feed that occasionally restates its recent history:

**Tables**

- `meter_state(meter_id, seq, source_version, <correction axis>, payload…, writer identity…)` —
  Family A analogue. TimescaleDB hypertable partitioned on `seq` (or a timestamp derived from
  it), small chunk interval so hundreds of chunks are cheap to create. Natural key
  `(meter_id, seq, source_version)`.
- `meter_register(meter_id, seq, source_version, <correction axis>, …)` — sibling written in the
  same cycle as `meter_state` (exercises R9/P7).
- `meter_master(meter_id, <correction axis>, valid_from, change_reason, attrs…)` — Family B
  analogue: loader-curated, effective-dated, with derived current/versions objects (exercises P8).
- `poll_roster(meter_id, enabled, valid_from, …)` — reference table steering both the writer
  (which meters to poll) and readers (which meters count) (exercises R7).
- `billing_run(id, params incl. effective_at, <whatever the candidate records>, output, created_at)`
  — the calculation: reads current states and the roster, produces an aggregate, and must satisfy
  R6 (exercises reproducible reads).

**Mapping to stl** (for the reader coming from either side):

| stl | toy |
|---|---|
| `block_number` | `seq` |
| `block_version` (reorg counter) | `source_version` (feed restatement counter) |
| `processing_version` | the correction axis under design |
| `build_id` / `build_registry` | writer build identity (a counter bumped per simulated deploy) |
| worker / cronjob / backfiller | live writer / periodic writer / range re-writer |
| `oracle_asset.enabled` | `poll_roster.enabled` |
| risk calculation | `billing_run` |

**Workload generator** — a driver that emits these events, individually scriptable and
composable into the scenarios of §6:

- **E1 live tick**: a batch of new `(meter_id, seq)` observations for enabled meters.
- **E2 retry**: re-deliver the last batch unchanged (crash/redelivery).
- **E3 deploy overlap**: two writer instances with different build identities ingest the same
  batch concurrently.
- **E4 backfill re-cover**: re-ingest an old, already-ingested range with the *same* code.
- **E5 correction**: re-ingest a range with *fixed* code producing different payloads, as a
  deliberate, ticketed correction run; must be crash-resumable (kill it halfway, re-run).
- **E6 source restatement**: the feed rewrites positions `[a,b]`; re-ingest at `source_version+1`.
- **E7 silent decommission**: a meter stops appearing in the feed; no tombstone is emitted.
- **E8 roster toggle**: disable a meter in `poll_roster` (as a new effective-dated row).
- **E9 backdated master fix**: a `meter_master` correction with earlier `valid_from` than the
  row it supersedes.
- **E10 concurrent reader**: a loop polling "current" state and aggregates while other events run.
- **E11 calculation + replay**: run `billing_run`, persist what the candidate needs; later —
  after E4/E5/E6 have landed — reproduce its exact input row set and output.
- **E12 payload-divergent collision**: two writers with different builds write *different*
  payloads for the same natural key outside any correction (a genuine bug in one of them).
  The right behavior is an open design question, but silence is a wrong answer (R2).

### 6. Verification — what "the new solution works" means

#### Stress scenarios (run against both the incumbent baseline and each candidate)

- **S1 — flat inserts (P1/R12)**: grow the `meter_state` hypertable to 300+ chunks (compress the
  older ones), inserting fixed-size batches (~700 rows) throughout. Baseline must reproduce the
  slope (insert latency growing with chunk count). Candidate passes if batch latency at 300+
  chunks is within ~2× of its 10-chunk latency.
- **S2 — phantom-version storm (P2/R2)**: 50 simulated deploy boundaries interleaved with E1/E2/
  E3/E4 over a fixed range. Candidate passes if the versioned tables are byte-identical to a
  single clean ingest of the same data — zero new versions/generations — and E12 within the storm
  is detected, not absorbed.
- **S3 — correction under live read (R3/R9)**: E5 over ~10k keys (both siblings) while E10 polls.
  Candidate passes if every polled result shows, for every key, one whole generation across both
  siblings (no torn keys); the correction, killed at 50% and re-run, converges to the same final
  state; and the candidate *states and demonstrates* its cross-key atomicity guarantee (per-key
  flip at commit boundaries is acceptable if declared; a whole-run atomic flip earns points).

#### Correctness cases (pass/fail tests)

- **C1 — cohort trap (P6/R8)**: after E7, a naive latest-per-key aggregate over-counts. The
  candidate must offer a structural fix (scoping object, cheap tombstones, whatever) and the test
  asserts the correct aggregate through it.
- **C2 — sibling consistency (P7/R9)**: readers joining `meter_state` × `meter_register` never
  mix generations for a key, under E5 and under a partial-failure injection (correction writes
  one sibling, crashes, resumes).
- **C3 — backdated master fix (P8)**: after E9, the corrected row either *becomes current* or
  the write is *rejected loudly*. Silently-never-current is a fail.
- **C4 — replay exactness (R6)**: E11's replay returns the identical input row set and output
  after corrections, backfills and restatements have landed — including the case where a backfill
  was in flight (uncommitted) when the calculation ran.
- **C5 — bitemporal reference reads (R7)**: E8 after a `billing_run` does not change that run's
  replay; a *future-dated* roster row present at run time does not flip a later replay; and a
  new run with the same `effective_at` sees the pre-toggle roster.
- **C6 — restatement vs correction (R4)**: after E6 then E5 over overlapping ranges, all four
  readings are one query each: (source before/after restatement) × (our code before/after fix).

#### Quality metrics (the softer goals, made checkable)

- **Q1 — new-table cost (R10)**: add a fourth versioned table to the finished toy. Count
  artifacts. Pass: declarative DDL (+ optionally one view) only; no per-table functions,
  triggers-with-logic, or test constants. Record the line count; compare to the baseline's
  (~35-line function + trigger + index + test bump).
- **Q2 — single ordering definition (R5)**: `grep` the finished toy for the ordering/tie-break
  expression. Pass: it appears in exactly one place per table family (the view/function that
  defines it), and no application query restates it.
- **Q3 — human reasoning (R11)**: five questions, each answerable with a short documented SELECT:
  (1) what is the current value for key K? (2) what was it before correction T? (3) why does this
  row exist — which run/ticket produced it? (4) what did billing run B read? (5) which keys did
  correction T change?
- **Q4 — enforcement is inspectable (R13)**: a single catalog query (grants, triggers, jobs)
  proves append-only holds for every versioned table; the test fails when a new table is added
  without protection (i.e., protection must be structural or self-enumerating, not a
  hand-maintained list).

### 7. Environment

- **Toy runs**: TimescaleDB (current `timescaledb-ha` Docker image), single node,
  `TimeZone='UTC'`. Columnstore compression enabled and actually exercised: S1–S3 and C-cases
  run with older chunks compressed.
- **Destination is TigerData** (managed; formerly Timescale Cloud). Constraints the toy cannot
  fully simulate — record each candidate's answer as a checklist instead:
  - Tiered (S3) chunks: `SET NOT NULL` needs a validating scan that is blocked on tiered
    chunks — columns must be born with their constraints; plan for additive-only schema change.
  - `drop_chunks`/retention bypass row/statement triggers: any trigger-based guard needs a
    policy-level conformance check too; "no retention on versioned tables" must be verifiable.
  - Unique constraints on hypertables must include the partition column; anything the candidate
    needs in a PK/UNIQUE must be compatible with segment-by/order-by compression settings.
  - Exclusion constraints and some index types are unavailable on hypertables — candidates built
    on range-overlap exclusion must state their fallback.
  - Role separation (SELECT/INSERT-only app roles) and advisory locks are available; extensions
    beyond TimescaleDB's bundle are not guaranteed.
  - Physical fork/replica is the supported clone path; anything relying on xid stability must
    document behavior under logical dump/restore (see non-goals).

### 8. Migration constraints (destination gates only)

Not a migration plan — just the facts that make some destinations unreachable from stl:

Cutover must be possible **one table at a time**, with old-style and new-style tables coexisting;
readers are spread over ~29+ hand-written sites in two languages and cannot flip atomically, so
the read surface of a candidate must be introducible per table. Reshaping the PK of a large
compressed hypertable is prohibitively expensive — candidates that keep the existing key shape
(or only add columns/objects additively) are strongly preferred over ones requiring key surgery.
Existing history (including ~880 phantom version rows and pre-tracking rows with null
provenance) must remain in place and remain harmless under the new read rules; applied
migrations are immutable, so every change is a new forward migration.

### Candidate mechanisms A1–A6 (spike “Appendix A”, non-binding)

Each candidate is listed with the stress most likely to kill it. The toy should implement at
least the incumbent baseline plus two candidates, one of which should be A1 (it is the proposed
successor in stl and carries four months of analysis).

- **A1 — Caller-assigned versions + insert-only allocation log** (ADR-0006 §3 shape): live
  writers always write version 0 with first-write-wins dedup; a correction run allocates the next
  per-table version once, in an insert-only log keyed `(table, ticket)` under a per-table
  advisory lock, then writes every row at that version. *Killer stresses*: E12 — first-write-wins
  silently drops a divergent second payload unless the writer checks insert counts; C1 (the
  mechanism itself doesn't address cohorts); S3 cross-key atomicity.
- **A2 — Single-axis SCD2 everywhere** (generalize Family B): every versioned table gets
  `valid_from`/derived-`valid_to`; corrections are new effective rows. *Killer stresses*: C3
  (backdating is structural here); C6 (mapping two mutation axes onto one temporal axis); R2
  (what dedupes a retry?).
- **A3 — Observation log + derived read models**: one append-only log of raw observations
  (identity + payload + provenance); per-table "current" is a derived, rebuildable, append-only
  read model. *Killer stresses*: S1 on the refresh path; C4 (snapshot exactness of a derived
  model); R10 (does each table need bespoke refresh logic?).
- **A4 — Range/temporal keys**: system-period or `tstzrange`-style keys with overlap exclusion.
  *Killer stresses*: §7 (exclusion constraints on hypertables); R1 (closing a range without
  UPDATE); compression compatibility.
- **A5 — Content-addressed idempotency**: dedupe by payload hash instead of build identity or
  version counters; a "version" is any new distinct payload for a key. *Killer stresses*: R3
  (corrections aren't distinguishable from bugs — E12 vs E5 look identical); a correction that
  reproduces the original payload for some keys; float serialization stability.
- **A6 — No correction column**: corrections are just new rows; ordering by ingestion
  (transaction id or DB-assigned timestamp) decides current. *Killer stresses*: R2 (a late live
  retry after a correction becomes newest and wins — the exact objection that rejected this in
  stl's ADR-0006 Alternatives; the toy may re-test it rather than take it on faith); logical
  restore; clock behavior.

### Glossary (spike “Appendix B”)

- **Observation**: one append-only row: "at source position N (restatement V), our code
  (identity B) concluded X about key K".
- **Source restatement**: the external system rewriting its own history (stl: chain reorg;
  toy: feed restatement). Recorded on its own axis, never by overwriting.
- **Correction**: a deliberate, attributable re-derivation of a range after an internal bug fix.
  Recorded on its own axis. Rare, operator-initiated, at most one per table at a time.
- **Retry**: re-delivery of the same logical write (crash, redelivery, deploy overlap, backfill
  re-cover with unchanged code). Must be invisible (R2).
- **Phantom version**: a new version minted by a retry that the mechanism mistook for a
  correction. The incumbent's signature failure.
- **Generation**: all rows a single correction run wrote (one value on the correction axis).
- **Current / canonical**: latest generation per natural key under the documented ordering.
  Always a query or view, never a flag column.
- **Governed table**: a table under the append-only regime, database-enforced.
- **Reference table**: a table whose content steers writers/readers (rosters, flags,
  classifications). Must be historized (R7).
- **Effective time vs knowledge time**: when a fact applied, vs when the database knew it.
  Reference reads pin both explicitly.
- **Cohort**: the set of keys that should participate in an aggregate *now* — membership is
  time-varying and ends silently (E7).
- **Lockstep siblings**: tables written together per cycle whose rows for one key must be read
  at one generation together.

## Appendix C: Spike — candidate comparison and environment facts (2026-08-24)

**Status**: research notes for the spike, 2026-08-24
**Inputs**: Appendix B (problem statement). All R/P/S/C/Q/E numbers and
candidate ids (A1–A6) refer to that document.

**Method**: environment claims were verified against (a) primary documentation
(postgresql.org/docs, tigerdata.com/docs — the former docs.timescale.com), (b) the
timescale/timescaledb GitHub repo (source, changelog, release notes), and (c) **empirical tests
against a live instance**: `timescale/timescaledb:latest-pg17` Docker image, PostgreSQL 17.11,
TimescaleDB **2.29.2** (current as of this writing). Note the toy spec (§7) names the
`timescaledb-ha` image; the extension version is the same, but the empirical results below
should be re-run once on `timescaledb-ha` when the toy stands up. Claims are marked
**VERIFIED** (primary source read and/or reproduced live) or **UNVERIFIED/ASSUMED**.

---

### Part 1 — Environment facts

#### 1. Unique constraints / PKs on hypertables must include the partition column — VERIFIED

- Docs: "Unique indexes must include all columns that are partitioning dimensions."
  — [Hypertable limitations](https://www.tigerdata.com/docs/use-timescale/latest/limitations).
- Empirical (2.29.2): `create_hypertable` on a table whose PK omits the partition column, and
  `CREATE UNIQUE INDEX` omitting it later, both fail with:
  `ERROR: cannot create a unique index without the column "seq" (used in partitioning)`
  (HINT: ensure the partitioning column is part of the primary or composite key).
- Empirical bonus: **partial** unique indexes that include the partition column are accepted and
  enforced (`CREATE UNIQUE INDEX ... ON t(meter_id, seq) WHERE ver = 0` worked) — but see fact 3
  for a compressed-chunk caveat.
- Consequence: any dedup-by-constraint design must carry `seq` (the partition column) in the
  arbiter key. All A1/A5/A6 shapes do naturally (the natural key contains `seq`).

#### 2. Exclusion constraints on hypertables: SUPPORTED (with the same partition-column rule) — VERIFIED empirically; docs are silent

This contradicts §7's assumption ("Exclusion constraints … are unavailable on hypertables").

- Current docs do **not** list exclusion constraints as unsupported
  ([Hypertable limitations](https://www.tigerdata.com/docs/use-timescale/latest/limitations)
  mentions only unique indexes, NULLs in time dimensions, cross-partition UPDATE, hypertable→hypertable FKs).
- Source: [`src/indexing.c`](https://github.com/timescale/timescaledb/blob/main/src/indexing.c)
  — "A UNIQUE, PRIMARY KEY or EXCLUSION index on a chunk must cover all partitioning dimensions
  to guarantee uniqueness (or exclusion) across the entire hypertable";
  [`src/chunk_constraint.c`](https://github.com/timescale/timescaledb/blob/main/src/chunk_constraint.c)
  copies EXCLUSION constraints to every chunk. The
  [2.29.0 changelog](https://github.com/timescale/timescaledb/blob/main/CHANGELOG.md)
  (#10281 "Disable direct compress when the destination table has an exclusion constraint so the
  constraint is still enforced") confirms they are a supported, maintained case.
- Empirical (2.29.2, `btree_gist` loaded):
  - `EXCLUDE USING gist (meter_id WITH =, vr WITH &&)` **rejected** (same "cannot create a unique
    index without the column seq" error) — the partition column must appear.
  - `EXCLUDE USING gist (meter_id WITH =, seq WITH =, vr WITH &&)` **accepted and enforced**:
    an overlapping range for the same `(meter_id, seq)` raised
    `conflicting key value violates exclusion constraint "1_e2_meter_id_seq_vr_excl"`.
- Semantics caveat: enforcement is per-chunk (the constraint is copied to each chunk), so it is
  only globally correct when the partition column participates with `=` — which forces
  conflicting rows into the same chunk. A range **over the partition axis itself** (rows whose
  validity range spans chunks) cannot be enforced this way.
- `btree_gist` is available on Tiger Cloud (not enabled by default) —
  [Extensions list](https://www.tigerdata.com/docs/use-timescale/latest/extensions).
- Consequence: A4 is **not** killed by exclusion-constraint availability. It dies elsewhere (see
  Part 2).

#### 3. Columnstore compression: full DML incl. ON CONFLICT works on compressed chunks — VERIFIED

The problem statement's compression fears are largely stale; since TimescaleDB 2.11 (2023) the
situation is:

- [Release 2.11.0](https://github.com/timescale/timescaledb/releases/tag/2.11.0): "Support for
  DML operations on compressed chunks: UPDATE/DELETE support", "Support for unique constraints
  on compressed chunks", "Support for ON CONFLICT DO UPDATE", "Support for ON CONFLICT DO NOTHING".
- Docs: [Inserting or modifying data in the columnstore](https://docs.tigerdata.com/use-timescale/latest/compression/modify-compressed-data/)
  — on INSERT with unique constraints TimescaleDB "decompresses relevant data during the insert
  to check if the new data breaks unique checks"; UPDATE/DELETE "only attempts to decompress data
  where it is necessary". Performance improvements landed steadily
  ([PR #7108](https://github.com/timescale/timescaledb/pull/7108): conflict check without
  decompression when no ON CONFLICT clause and one unique constraint; 2.26/2.27 bloom filters for
  UPSERT/UPDATE/DELETE per the [CHANGELOG](https://github.com/timescale/timescaledb/blob/main/CHANGELOG.md)).
- Empirical (2.29.2), on a hypertable with PK `(meter_id, seq, ver)`,
  `segmentby='meter_id'`, `orderby='seq, ver'`, chunks compressed:
  - plain INSERT into a compressed chunk: **works**;
  - duplicate insert: **unique violation raised** (`duplicate key value violates unique
    constraint "2_c1_pkey"`) — dedup constraints are enforced on compressed chunks;
  - `ON CONFLICT DO NOTHING`: **works** (0 rows, no error);
  - `ON CONFLICT (cols) DO UPDATE`: **works**;
  - `ON CONFLICT ON CONSTRAINT <name> DO NOTHING`: **works** on 2.29.2 (the old
    [issue #1094](https://github.com/timescale/timescaledb/issues/1094) limitation was not
    reproducible; docs are silent — treat named-constraint arbiters as usable but prefer
    column-inference form);
  - UPDATE and DELETE against compressed chunks: **work**.
- segmentby/orderby vs unique constraints: not a hard rule, a warning. Declaring
  `segmentby=''`, `orderby='seq'` with PK `(meter_id, seq, ver)` produced
  `WARNING: column "meter_id" should be used for segmenting or ordering` (and same for `ver`)
  but succeeded. Keeping unique-key columns inside segmentby ∪ orderby is what keeps the
  conflict check cheap (sparse/bloom indexes over those columns).
- **Empirical caveat found**: adding a **partial** unique index to a hypertable *while chunks
  are compressed* failed with a spurious
  `ERROR: 23505 duplicate key value violates unique constraint` from TimescaleDB's
  `validate_index_constraints` (no actual duplicates; the same statement succeeded after
  `decompress_chunk`). A *full* unique index on the same data succeeded while compressed.
  No doc or issue found for this — treat as: **partial unique indexes must be born before
  compression** (worth filing upstream).
- Remaining hard limits: "UPDATE statements that move values between partitions (chunks) are not
  supported. This includes upserts (INSERT ... ON CONFLICT UPDATE)"
  ([limitations](https://www.tigerdata.com/docs/use-timescale/latest/limitations)) — irrelevant
  for append-only designs whose ON CONFLICT never changes the partition key.
- Consequence for R2/R14: **dedup via unique constraints works on compressed chunks**, at a
  decompression-probe cost on conflict-checking inserts. A1-style designs are viable compressed.

#### 4. plan_cache_mode / generic plans defeat chunk pruning (P1's mechanism) — VERIFIED

- [PREPARE docs](https://www.postgresql.org/docs/current/sql-prepare.html): "the first five
  executions are done with custom plans and the average estimated cost of those plans is
  calculated. Then a generic plan is created and its estimated cost is compared … Subsequent
  executions use the generic plan if its cost is not so much higher …". Overridable via
  `plan_cache_mode = force_generic_plan | force_custom_plan`.
- [PL/pgSQL implementation docs](https://www.postgresql.org/docs/current/plpgsql-implementation.html):
  PL/pgSQL statements are prepared statements under SPI and follow the same plan-caching rule —
  so a trigger-body `MAX()` flips to a generic plan after ~5 calls.
- Empirical (2.29.2, 51 chunks): `PREPARE q AS SELECT max(ver) FROM p1 WHERE meter_id=$1 AND
  seq=$2`; under `force_custom_plan` the plan touches **one** chunk index; under
  `force_generic_plan` it is a MergeAppend over **all 51 chunks** — no plan-time pruning, and no
  runtime exclusion for this shape. P1's mechanism is exactly as the incumbent describes; the
  per-function `SET plan_cache_mode = force_custom_plan` fix (and its silent reset on
  `CREATE OR REPLACE FUNCTION`) is real.
- Consequence: any candidate whose **hot write path** runs a parameterized per-row lookup over
  the hypertable inherits P1. Candidates that make the hot path a plain constraint-arbitered
  INSERT (values known at parse time → pruning by value) do not.

#### 5. drop_chunks / retention bypass row and statement triggers — VERIFIED empirically; docs describe the mechanism

- [About data retention](https://www.tigerdata.com/docs/use-timescale/latest/data-retention/about-data-retention):
  "Deleting data row-by-row … can be slow. But dropping data by the chunk is faster, because it
  deletes an entire file from disk." (i.e. DDL, not DML — no DELETE ever runs).
- Empirical (2.29.2): hypertable with BEFORE DELETE/UPDATE row trigger **and** BEFORE DELETE
  statement trigger; `drop_chunks()` removed a 99-row chunk with **zero trigger firings**.
- Consequence for R13/Q4: trigger-based append-only guards do not see chunk drops. Enforcement
  needs (a) grants (see fact 9) **and** (b) a policy-level conformance check that no retention
  policy / manual drop_chunks touches governed tables (`timescaledb_information.jobs` is
  queryable for that).

#### 6. Tiered (S3) chunks: columns must be born with their constraints — VERIFIED

- [About tiered storage](https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/about-data-tiering):
  disallowed on hypertables with tiered chunks: "adding a column with any default value
  (including NULL), renaming a column, changing the data type of a column, and adding a NOT NULL
  constraint to the column". Allowed: renaming the hypertable, **adding columns without
  defaults**, adding indexes, schema rename, CHECK constraints (untiered data only), deleting
  columns (which then cannot be re-added under the same name).
- Also: "You cannot insert data into, update, or delete a tiered chunk. These limitations take
  effect as soon as the chunk is scheduled for tiering."
- Consequence: (a) the §7 "born with their constraints" claim is confirmed and is actually
  broader (no defaults on added columns at all); (b) **any candidate that must touch old rows
  during a correction (A4's range-closing) is impossible once those rows are tiered**;
  append-only candidates that only add *new* rows in *new* chunks are unaffected.

#### 7. xid8 / pg_current_xact_id, wraparound, commit timestamps — VERIFIED

- [Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html):
  "The internal transaction ID type xid is 32 bits wide and wraps around every 4 billion
  transactions. A 32-bit epoch is incremented during each wraparound. There is also a 64-bit type
  xid8 which includes this epoch and therefore does not wrap around during the life of an
  installation."
- [System information functions](https://www.postgresql.org/docs/current/functions-info.html):
  `pg_current_xact_id() → xid8` (assigns one if none yet; returns top-level id in subxacts).
- Dump/restore vs fork: the docs scope xid8 uniqueness to "the life of an installation". A
  logical dump/restore is a **new installation**: stored xid8 *column values* survive (they are
  ordinary data, and comparisons **among stored values** stay valid), but comparisons against the
  new cluster's live xid counter are meaningless — the new counter restarts low, so new writes
  can sort *below* old data. A physical replica/fork preserves the counter; TigerData forks are
  physical ([Fork services](https://www.tigerdata.com/docs/use-timescale/latest/fork-services),
  [Replicas and forks with tiered data](https://www.tigerdata.com/docs/build/data-management/storage/tiered-data-replicas-forks)),
  so xid-based ordering survives the supported clone path and dies under logical restore
  (explicitly a non-goal, but must be documented per §7).
- Commit timestamps: `pg_xact_commit_timestamp(xid)` "only provide[s] useful data when the
  track_commit_timestamp configuration option is enabled, and only for transactions that were
  committed after it was enabled. **Commit timestamp information is routinely removed during
  vacuum.**" ([functions-info](https://www.postgresql.org/docs/current/functions-info.html)).
  That retention clause makes commit timestamps **unusable as a durable ordering axis** for A6
  regardless of whether Tiger Cloud exposes the GUC (whether it does is UNVERIFIED — the
  [advanced parameters](https://docs.tigerdata.com/use-timescale/latest/configuration/advanced-parameters/)
  page describes a searchable list but I could not confirm this specific GUC).

#### 8. Snapshots and reproducible reads (R6/C4 primitives) — VERIFIED

- [Snapshot synchronization functions](https://www.postgresql.org/docs/current/functions-admin.html):
  `pg_export_snapshot()` — "The snapshot is available for import only until the end of the
  transaction that exported it." So **snapshot export is a same-instant coordination tool, not a
  later-replay primitive**. Exact replay later via MVCC machinery is off the table (as the task
  brief anticipated).
- [Transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html):
  REPEATABLE READ "sees a snapshot as of the start of the first non-transaction-control
  statement in the transaction" and "never sees either uncommitted data or changes committed by
  concurrent transactions during the transaction's execution."
- `pg_current_snapshot() → pg_snapshot` returns the current snapshot as a **value**
  (`xmin:xmax:xip_list`), and `pg_visible_in_snapshot(xid8, pg_snapshot) → boolean` evaluates
  visibility of a *stored* xid8 against a *stored* snapshot
  ([functions-info](https://www.postgresql.org/docs/current/functions-info.html)).
- **The composable primitive that works** (evaluated against the C4 in-flight-backfill trap):
  stamp every governed row with `written_xid xid8 DEFAULT pg_current_xact_id()`; a calculation
  runs in REPEATABLE READ and records its `pg_current_snapshot()` text as data. Replay =
  `WHERE pg_visible_in_snapshot(written_xid, :recorded_snapshot)`. Because both sides are stored
  **data** (not tuple headers), vacuum/freezing cannot erase them; an in-flight backfill's xid
  was in the snapshot's `xip_list` at run time, so its later-committed rows are correctly
  excluded from replay; aborted writers never produced rows at all. Survives physical fork;
  survives logical restore for *historical* replays (stored-vs-stored comparisons), degrades
  only for snapshots recorded *after* a logical restore against pre-restore rows. Row `xmin`
  itself is 32-bit and not durable — only the explicit stored column works.
- Caveat vs fact 6: `DEFAULT pg_current_xact_id()` on a **new** column cannot be added to an
  already-tiered hypertable ("adding a column with any default value (including NULL)" is
  blocked). New tables: born with it. Existing stl tables: add the column without default before
  tiering, or treat NULL as "pre-tracking, always visible" (stl already has null-provenance
  rows; §8 requires they stay harmless anyway).

#### 9. Grants-based append-only enforcement coexists with compression — VERIFIED

- [Privileges](https://www.postgresql.org/docs/current/ddl-priv.html): UPDATE, DELETE and
  TRUNCATE are independently grantable/revocable table privileges; "these default privilege
  settings can be overridden using the ALTER DEFAULT PRIVILEGES command" (future objects).
- Empirical (2.29.2): role with only `SELECT, INSERT`:
  - INSERT works; UPDATE and DELETE fail with `permission denied for table` — catalog-inspectable
    via `information_schema.role_table_grants` (Q4);
  - `INSERT ... ON CONFLICT DO NOTHING` works **without** UPDATE privilege, including against a
    compressed chunk (DO NOTHING requires no UPDATE privilege; DO UPDATE would);
  - the table owner can still `compress_chunk()` while the app role is INSERT-only — compression
    is an owner-side chunk rewrite, orthogonal to app-role grants.
- Background jobs run with owner permissions: "Background workers for commands that start with
  add_job will run with the permissions of the table owner"
  ([issue #1662](https://github.com/timescale/timescaledb/issues/1662) /
  [PR #1709](https://github.com/timescale/timescaledb/pull/1709));
  `compress_chunk` requires ownership-level rights
  ([compress_chunk API](https://docs.timescale.com/api/latest/compression/compress_chunk/)).
  PARTIALLY VERIFIED (issue/PR text + empirical owner test; current docs phrase it thinly).
- Note: TimescaleDB ≥ 2.23 removed the legacy `insert_blocker` trigger
  ([CHANGELOG](https://github.com/timescale/timescaledb/blob/main/CHANGELOG.md) #8804) — one less
  hidden trigger interaction.

#### 10. TigerData managed-service model — VERIFIED (with gaps)

- **No superuser**: "Tiger Cloud does not provide superuser access. tsdbadmin is not a
  superuser. … you can use standard PostgreSQL means to create other roles or assign individual
  permissions." ([Manage data security](https://www.tigerdata.com/docs/use-timescale/latest/security/read-only-role)).
  Role separation for R1/R13 is therefore available.
- **Extensions**: fixed allowlist of ~60+ ("The following PostgreSQL extensions are available in
  every Tiger Cloud service") — includes `timescaledb`, `timescaledb_toolkit`, `postgres_fdw`
  (default-on) and `pgcrypto`, `btree_gist` (enable-able)
  ([Extensions](https://www.tigerdata.com/docs/use-timescale/latest/extensions)). Nothing beyond
  the list — candidates must not require exotic extensions.
- **Physical fork**: supported first-class ("fork an existing service into a new, independent
  copy"; data written after fork time not included)
  ([Fork services](https://www.tigerdata.com/docs/use-timescale/latest/fork-services)) —
  consistent with fact 7's xid-stability analysis.
- **Advisory locks**: core PostgreSQL, no extension required; no TigerData doc found restricting
  them. UNVERIFIED as an explicit TigerData statement; §7 asserts availability and nothing found
  contradicts it.
- **GUC surface**: many parameters settable per service via console
  ([Advanced parameters](https://docs.tigerdata.com/use-timescale/latest/configuration/advanced-parameters/));
  whether `track_commit_timestamp` specifically is exposed: UNVERIFIED (moot per fact 7).

---

### Part 2 — Candidate assessment

Scorecard legend: ✔ pass, ✘ fail, ◐ conditional (condition stated). "Aux" = requires one of the
cross-cutting auxiliary mechanisms (next section) — every candidate needs *some* of them; a ◐
that names an aux is not a demerit unless the aux fights the candidate's own mechanics.

#### Incumbent baseline (reference)

Trigger-assigned `MAX()+1` with build-identity retry detection, advisory per-key lock,
`ON CONFLICT DO NOTHING`. Failure modes P1–P8 as measured; P1's plan-cache mechanism and the
grants/trigger environment facts above are all confirmed. It fails R2 (P2 phantom storms), R10
(P4), R12 (P1), and R13 only holds by trigger discipline that drop_chunks bypasses (fact 5).
Reproduce as baseline; nothing new.

#### A1 — Caller-assigned versions + insert-only allocation log (ADR-0006 shape)

**Mechanism.** Live writers always write correction-axis `ver = 0`; the PK
`(meter_id, seq, source_version, ver)` (partition column included — fact 1) plus
`ON CONFLICT DO NOTHING` makes every replay a byte-level no-op. A correction run first INSERTs a
row into a global insert-only `correction_run(table_name, ticket, ver, reason, code_id, status,
created_at)` log — uniqueness on `(table_name, ticket)` makes allocation idempotent; a per-table
advisory lock serializes allocation (one correction per table, per §2) — then writes all its rows
at that `ver`. No triggers, no per-table functions.

**Scorecard.**

| Req | Verdict | Why |
|---|---|---|
| R1/R13 | ✔ | INSERT-only grants; ON CONFLICT DO NOTHING needs no UPDATE priv (fact 9); Q4 = grants query + "no retention job on governed tables" check (fact 5) |
| R2 | ◐ | identical replays: structurally byte-identical (PK dedup). Divergent payload (E12): silently dropped **unless** composed with the payload-hash check (aux 5) — the named killer, must be built in |
| R3 | ✔ | ticket-keyed allocation is attributable and idempotent; crash-resume = re-run same ticket, DO NOTHING skips done rows |
| R4 | ✔ | `source_version` and `ver` are separate columns; all four C6 readings are one predicate each |
| R5 | ✔ | one `_current` view per table owns the ORDER BY tuple |
| R6 | ◐ | aux 3 (xid8 + snapshot recording) — composes cleanly, additive column |
| R7 | ◐ | orthogonal; aux 4 (SCD2 reference tables) required, as for every candidate |
| R8 | ◐ | aux 1 (roster-scoped current views) |
| R9 | ◐ | aux 2: shared run id + status flip in the allocation log gives whole-run atomic flip (S3 bonus points) |
| R10 | ✔ | DDL + one view; allocation log is global, not per-table |
| R11 | ✔ | Q3's five questions are one SELECT each (run log answers "why does this row exist" / "which keys did T change" via `WHERE ver = :v`) |
| R12 | ✔ | hot path is a plain INSERT — values known at parse time, chunk pruning by value; no per-row MAX() (fact 4 avoided by construction) |
| R14 | ✔ | verified: unique enforcement + DO NOTHING on compressed chunks (fact 3); keep PK columns ⊆ segmentby ∪ orderby to keep the conflict probe cheap |

**Killer stresses.** E12: an insert that conflicts on identity but differs in payload vanishes
under DO NOTHING — the writer must compare inserted-count vs batch-size and, on shortfall, join
its candidate hashes against stored hashes; alert on mismatch (aux 5). C1: not addressed by the
mechanism — aux 1. S3 cross-key atomicity: rows at `ver = V` are written invisible (current view
requires the run's `status = 'complete'`); flipping status is one row UPDATE-free event (insert a
`correction_run_status` row or write status as a new log row) → whole-run atomic flip at one
commit, crash-safe. Other danger: none of S1 (no lookup on hot path).

**Environment verdict.** Everything it needs is verified available: PK-with-partition-column,
compressed-chunk conflict handling, INSERT-only grants, advisory locks (assumed per §7/fact 10).
Nothing conflicts with tiered chunks (never touches old rows).

**Migration gate (§8).** Strongest of all candidates: the PK shape `(natural key, ver)` is what
stl already has — no key surgery; cutover per table = drop trigger, revoke UPDATE/DELETE, add
`_current` view; existing phantom rows (`ver > 0`) remain valid history and remain canonical
where they already were; the allocation log is a new table.

**Q1 cost.** Table DDL + 1 view (+ grants inherited via `ALTER DEFAULT PRIVILEGES`). Zero
procedural artifacts. Estimated ~15 lines vs the baseline's ~35-line function + trigger + index +
test bump.

#### A2 — Single-axis SCD2 everywhere

**Mechanism.** Every table gets `valid_from` with derived `valid_to` (lead() views); a correction
is a new effective row. Family B already is this.

**Scorecard.** R1 ✔ (rows only added); **R2 ✘** — nothing dedups a retry: `valid_from` is
loader-assigned wall-clock, so a redelivered batch mints new effective rows (P2 reborn) unless
you add content hashing, at which point you've built A5-on-A2; R3 ◐ (a correction is just
another row; attribution needs an extra column, fine); **R4 ✘ structurally** — source
restatement and correction are forced onto one temporal axis; C6's four readings are not
expressible without re-adding a second axis, at which point it is no longer "single-axis";
R5 ✔ (views); R6/R7/R8 ◐ (same auxes); R9 ◐; R10 ✔; R11 ◐ (temporal-interval reasoning is
harder than generation reasoning); R12 ✔; R14 ✔ (nothing exotic). **C3/P8 is structural**: a
backdated correction (higher knowledge, earlier `valid_from`) silently never becomes current
unless the view orders by (valid_from, knowledge-axis) *and* the reader pins knowledge time —
i.e. correctness requires the bitemporal machinery everywhere, for tables that have no natural
effective-time axis at all.

**Environment verdict.** No conflicts — it needs nothing the environment lacks.

**Migration gate.** ✘ Family A tables would need `valid_from` added into the uniqueness/PK shape
of large compressed hypertables → exactly the key surgery §8 prohibits.

**Q1 cost.** DDL + 2 views. Cheap.

**Verdict.** Wrong shape for Family A (fails R2/R4 by design, gated by §8 anyway). But it *is*
the right shape for Family B and the reference tables — keep it there (see aux 4); every
candidate inherits it for R7 regardless.

#### A3 — Observation log + derived read models

**Mechanism.** One append-only hypertable per table-family stream (or one log with a
`stream` discriminator): `(stream, natural key, seq, source_version, payload, provenance,
payload_hash, run_id, written_xid)`, deduped by unique `(stream, key, seq, source_version,
payload_hash)`-style arbiter. Per-table "current" is a derived, rebuildable read model refreshed
transactionally by a generic procedure driven by catalog metadata (stream → target table + key
columns), or consumed directly through views.

**Scorecard.** R1 ✔ (log is INSERT-only; read models are caches — rewriting them destroys
nothing); R2 ◐ (hash-arbitered dedup is exact; E12 = same identity, new hash → new log row,
**visible** — detection is structural, better than A1's); R3 ✔ (run_id on correction rows);
R4 ✔; R5 ✔ (the read model *is* the single ordering definition); **R6 ✔/strong** — the log plus
aux 3 gives exact replay, and sibling read models derive from one log so C2 is easy; R7 ◐ (aux
4); R8 ◐ (aux 1); **R9 ✔** — siblings are one stream or two streams written in one transaction,
and the read-model refresh applies them atomically; **R10 ◐** — the killer: each new table needs
read-model DDL *plus registration in the refresh machinery*; if refresh is one generic
metadata-driven procedure this stays declarative-ish, if it drifts into per-table refresh logic
it re-creates P4; R11 ◐ (two-layer indirection: "why is current X" = log query + refresh
watermark); **R12/S1 ◐** — the log insert is flat, but the *refresh path* must be incremental
(track a log high-water mark) or it degrades with history size — S1 must be run against the
refresh, as the candidate list in Appendix B says; R14 ◐ (log compresses well; typed payloads per stream, or jsonb
with the storage/segmentby costs that implies).

**Environment verdict.** Nothing it needs is missing. Watch: read models rebuilt in place are
tables that get UPDATEd/rewritten — they must be exempt from the append-only grant regime
(they are derived caches, document that in Q4's conformance query), and they should stay
uncompressed/small (latest-only), which they naturally are.

**Migration gate.** ◐ Per-table cutover is possible (one stream per legacy table; readers move
to the read model view one table at a time — the read surface is introducible per table). But
writers must be repointed to the log, and the log is a genuinely new architecture: biggest lift
of the viable candidates. Existing history can be back-loaded into the log or left as a frozen
pre-log epoch under the read model — harmless either way.

**Q1 cost.** Read-model DDL + one registration row (+ shared generic refresh proc). ~20 lines if
the genericity holds — measuring exactly this is why it belongs in the toy.

#### A4 — Range/temporal keys with overlap exclusion

**Mechanism.** Rows carry a system-validity range (`sys_period tstzrange`/`int8range`);
superseding a row **closes** its range and inserts the successor; overlap exclusion
(`EXCLUDE (key WITH =, seq WITH =, sys_period WITH &&)`) guarantees at most one current row.

**Environment verdict — dies, but not where §7 expected.** Exclusion constraints are *available*
(fact 2, contra §7). What kills it:

1. **R1/R13 ✘ structurally**: closing a range is an UPDATE of the superseded row. Append-only
   INSERT-only grants are impossible; the writer role must hold UPDATE on all history — the
   exact privilege R13 exists to revoke. The audit property ("original unreachable? never")
   now depends on convention, which R1 forbids.
2. **Tiered chunks ✘ (verified fact 6)**: "You cannot insert data into, update, or delete a
   tiered chunk." A correction over a tiered range cannot close the old rows. Dead on TigerData
   at exactly the moment (old data) corrections target.
3. Compression friction: closing ranges UPDATEs compressed batches (works — fact 3 — but is a
   rewrite of old cold data on every correction, the worst-case DML pattern), and 2.29's #10281
   shows exclusion constraints already disable compression fast paths.
4. Migration gate ✘: the range column must join the key/constraint shape of large compressed
   hypertables — key surgery.

The "insert-only ranges" repair (never close; current = latest lower bound) discards the
exclusion constraint's value entirely and collapses into A6-with-extra-steps. **DOA.**

**Q1 cost** (moot): DDL + exclusion constraint + btree_gist; no procedural code.

#### A5 — Content-addressed idempotency

**Mechanism.** Identity = (natural key, payload_hash); unique arbiter on
`(key, seq, source_version, payload_hash)`; a "version" is any new distinct payload.

**Scorecard highlights.** R2 (retry) ✔ — replays are hash-identical, structurally deduped; but
**E12 ✘ silently absorbed**: a divergent payload is *by definition* a new version, so the bug
case and the correction case are indistinguishable (the named killer — it fails R2's "surfaced,
never silently resolved" and R3's attributability in one stroke). **R3 ✘** also on the
reproduced-original trap: a correction that re-derives the *original* payload for some keys
cannot insert (hash exists) — if a buggy intermediate payload is current, the key is **stuck
wrong forever**; and hash gives no order, so "current" needs an ingestion-order axis anyway →
inherits A6's problems. Float/serialization stability is a real operational hazard on ~45
tables of numeric payloads. R14 ✔ (hash column compresses; arbiter works compressed — fact 3).

**Environment verdict.** Nothing missing (`pgcrypto`/`sha256` available — fact 10; or use
`hashtextextended` built-ins). It dies on requirements, not environment.

**Migration gate.** ◐ hash column is additive, but the *arbiter* uniqueness including hash is a
new unique index on large compressed hypertables (full unique indexes on compressed chunks did
create in the live test — fact 3 caveat applies only to partial ones), so feasible-but-heavy.

**Verdict.** Not viable as the identity mechanism. **Adopt as the auxiliary** (aux 5): a
payload-hash column + count/hash divergence check is precisely what plugs A1's and A6's E12
hole and cheapens A3's dedup. This is where its value survives.

#### A6 — No correction column; ordering by ingestion

**Mechanism.** Rows carry `written_xid xid8 DEFAULT pg_current_xact_id()` (or a timestamp);
current = latest by ingestion order per key. No version column at all.

**Scorecard highlights.** **R2 ✘ structurally** — the dilemma: (a) if the PK is the bare natural
key `(key, seq, source_version)` + DO NOTHING, retries dedup but corrections *cannot insert at
all* (identity collides; DO UPDATE would violate R1); (b) if the PK includes the ordering column,
every retry is a distinct row → phantom versions worse than P2, and a late live retry (queue
redelivery hours later, after a correction) inserts with a *newer* xid and **wins over the
correction** — the exact objection in stl's ADR-0006 Alternatives; the toy can re-test it but
the structure is visible from here. R3 ✘ (no attributable generation; "which rows did T write"
requires joining on xid ranges); R4 ◐ (source axis survives; correction axis is smeared into
ingestion order). Clock variant: `now()` is not monotonic across writers, ties under deploy
overlap. Commit-timestamp variant: **dead by verified fact 7** — "Commit timestamp information
is routinely removed during vacuum". xid variant: verified durable per-installation and across
physical forks, dead across logical restore (documented non-goal).

**Environment verdict.** The primitives exist (fact 7); the design fails on R2/R3 regardless.
**DOA** as the primary mechanism. Its one durable idea — stored xid8 as a *knowledge-time
stamp* — survives as aux 3 (reproducible reads), where it is genuinely excellent.

**Q1 cost** (moot): DDL only. Cheapest — which is why it keeps getting re-proposed.

---

### Cross-cutting auxiliary mechanisms

These are the sub-problems Appendix B candidates don't individually solve. Every viable
composition = one primary candidate + auxes 1–5.

**Aux 1 — Cohort correctness (C1/R8).** Three options: (a) *scoping views* — `_current` joins
`poll_roster` at an explicit effective/knowledge time, so decommission = roster row, and E7
(silent disappearance, no one writes anything) is handled by (b) *recency scoping* — cohort =
keys observed within a window — which is a heuristic, not a fact; or (c) *cheap tombstones* —
operator/detector inserts an explicit end-of-cohort row. Judgment: roster-driven scoping is the
only structural answer consistent with R7 (the roster is already the thing that decides who is
polled; make it also decide who counts), with tombstones as the escape hatch for entities that
end outside the roster's knowledge. Composes identically with A1 and A3. The C1 test should
assert through the roster-scoped view.

**Aux 2 — Sibling consistency (C2/R9).** Options: (a) *shared generation/run id* — both siblings
write the same `ver`/`run_id`, readers join on it (still torn mid-write); (b) *transactional
flip* — rows at the new generation are invisible until one commit flips the run's status in the
allocation/run log, and `_current` views filter to completed runs. Judgment: (b), because it also
answers S3's crash-resume (a half-written run is invisible, the re-run completes it, one commit
flips both siblings) and earns S3's "whole-run atomic flip" points. A1's allocation log already
has the row to hang status on; A3 gets it for free (single log, refresh applies atomically).

**Aux 3 — Reproducible reads (R6/C4).** Evaluated options: (a) *max-visible-version watermarks
per table* — fails the C4 trap: an in-flight (uncommitted) backfill's rows carry values below the
recorded watermark and enter later replays; (b) *snapshot export* — verified unusable later
(fact 8: import only until exporting txn ends); (c) *logging exact row keys* — bulletproof,
expensive, and the fallback if (d) is judged too clever; (d) *stored xid8 + stored
pg_current_snapshot()* — verified sound (fact 8): vacuum-proof (both sides are data),
in-flight-backfill-proof (xip_list), fork-proof, and one additive column + one text column on
`billing_run`. Judgment: implement (d) in the toy, keep (c) as the comparison baseline in the C4
test. Tiered-chunk caveat: the xid8 column must be *born* with its DEFAULT (fact 6); NULL = pre-
tracking = always-visible.

**Aux 4 — Bitemporal reference data (C5/R7).** Largely orthogonal to A1–A6, and **every candidate
must add it**: reference tables (`poll_roster`, `meter_master`) become SCD2 (`valid_from`,
derived `valid_to`) **plus** a knowledge axis (aux 3's xid8 stamp, or an insert timestamp) so a
read pins both `effective_at` (explicit parameter, never now() — R7) and knowledge time (the
recorded snapshot). C5's three assertions all reduce to: replay filters roster rows by
`pg_visible_in_snapshot(written_xid, run_snapshot) AND valid_from <= run.effective_at`. C3/P8:
the `_current` view orders by `(valid_from, correction-axis)` *within* pinned knowledge — a
backdated fix becomes current for its effective window; additionally a loud guard (constraint or
loader check) should reject a correction whose `valid_from` regresses without an explicit
backdate flag, satisfying "becomes current or rejected loudly".

**Aux 5 — Payload-divergence detection (E12).** A `payload_hash` column (deterministic
serialization: fixed column order, canonical float encoding — test this, it is A5's named
hazard) + a statement-level check: after `INSERT ... ON CONFLICT DO NOTHING`, if inserted-count
< batch-count, join the shortfall's identities against stored hashes; identity-equal +
hash-different outside a correction → alert/error, never absorb. Composes with A1 (mandatory —
plugs its named killer), A3 (structural — divergence is already a visible new log row; the check
degenerates to a query), and A6 (moot). Hashing is a *check*, not identity — A5's failure modes
(reproduced-original, un-orderable versions) don't apply to it in this role.

---

### Comparison matrix

| Criterion | Incumbent | A1 +auxes | A2 | A3 +auxes | A4 | A5 | A6 |
|---|---|---|---|---|---|---|---|
| R1/R13 enforceable by grants | ✘ (trigger-guard; fact 5) | ✔ | ✔ | ✔ (log) | ✘ needs UPDATE | ✔ | ✔ |
| R2 identical retry | ◐ (P2!) | ✔ | ✘ | ✔ | ✔ | ✔ | ✘ |
| E12 surfaced | ✘ | ◐ aux 5 | ✘ | ✔ | ◐ | ✘ absorbed | ✘ |
| R3 corrections | ◐ (P2 both ways) | ✔ | ◐ | ✔ | ◐ | ✘ | ✘ |
| R4 two axes | ✔ | ✔ | ✘ | ✔ | ◐ | ◐ | ◐ |
| R5/Q2 one ordering | ✘ (P5) | ✔ | ✔ | ✔ | ✔ | ✘ | ✔ |
| R6/C4 (with aux 3) | ✘ | ✔ | ✔ | ✔✔ | ✔ | ✔ | ✔ |
| R9/S3 atomic flip | ✘ (P7) | ✔ aux 2 | ◐ | ✔ | ◐ | ◐ | ◐ |
| R10/Q1 new-table cost | ✘ (P4) | ✔ DDL+view | ✔ | ◐ +registration | ✔ | ✔ | ✔ |
| R12/S1 flat inserts | ✘ (P1, verified) | ✔ | ✔ | ◐ refresh path | ◐ | ✔ | ✔ |
| R14 compressed | ◐ | ✔ (fact 3) | ✔ | ◐ | ◐ | ✔ | ✔ |
| Tiered chunks (fact 6) | ✔ | ✔ | ✔ | ✔ | ✘ **fatal** | ✔ | ✔ |
| Migration gates §8 | — | ✔ same key shape | ✘ key surgery | ◐ big lift, per-table OK | ✘ key surgery | ◐ | ✔ |
| Overall | baseline | **strongest** | Family B only | **strong, costlier** | DOA | aux only | DOA |

---

### Recommendation

**Implement in the toy: A1 (mandated), A3, and A5-as-auxiliary folded into both.**

- **A1** is the only candidate that passes every hard requirement *and* every migration gate
  with verified environment facts behind each pass (facts 1, 3, 9). Its two named killers are
  both closed by cheap composition: E12 by aux 5, S3/R9 by aux 2's status-flip on the allocation
  log it already owns. The toy must prove the composed shape, not bare A1: PK dedup + allocation
  log + run-status-filtered views + payload-hash divergence check + xid8/snapshot replay.
- **A3** is the one genuinely different architecture worth the toy's time: it is structurally
  better than A1 exactly where A1 is conditional (E12 visible by construction, R9 free, R6
  strongest) and structurally worse exactly where the toy can measure it (R10 registration
  cost — Q1; S1 on the refresh path). If the generic metadata-driven refresh survives Q1 and S1,
  A3 is the better long-term destination; if it doesn't, the toy has demonstrated why A1 wins.
  That is a real experiment; A2/A4/A6 would not be.
- **A5** should not be implemented as a standalone candidate — implement its hash as aux 5
  inside both A1 and A3, plus the float-serialization stability test it drags in.
- **A2** for Family B/reference tables only: it is the existing Family B shape and aux 4's
  substrate; as a Family A candidate it is dead on R2/R4 and §8's key-surgery gate.

**Dead on arrival, with the verified fact that kills each:**

- **A4** — killed by *"You cannot insert data into, update, or delete a tiered chunk"*
  ([verified, About tiered storage](https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/about-data-tiering)):
  range-closing UPDATEs are impossible on exactly the old data corrections target — plus the
  structural R1/R13 conflict (closing ranges requires the UPDATE privilege append-only exists to
  revoke). Notably it is *not* killed by exclusion-constraint availability: fact 2 shows those
  now work on hypertables (§7 should be corrected).
- **A6** — killed twice: structurally by R2 (the PK dilemma: dedup-PK blocks corrections,
  ordering-PK re-mints phantoms and lets a late retry outrank a correction), and its
  commit-timestamp variant by the verified *"Commit timestamp information is routinely removed
  during vacuum"* ([functions-info](https://www.postgresql.org/docs/current/functions-info.html)).
  Its xid8 idea survives — demoted to aux 3, where it is the best available primitive for R6/C4.

**Corrections the toy should feed back into the problem statement (§7):** exclusion constraints
*are* supported on hypertables (partition column included, per-chunk semantics); compressed
chunks accept INSERT/UPDATE/DELETE/ON CONFLICT with enforced unique constraints since 2.11;
partial unique indexes cannot be added while chunks are compressed (empirical, 2.29.2 —
born-before-compression applies); drop_chunks bypassing triggers is empirically confirmed (zero
firings), so Q4 must pair grants with a no-retention-policy conformance query.
