# ADR-0009: Derived Read Models and Their Corrections

- **Status**: Proposed
- **Proposed**: @Yasanji
- **Date**: 2026-09-16
- **Deciders**: @vector
- **Relates to**: [ADR-0006](0006-data-reproducibility-and-append-only-guarantees.md) §1 (append-only), §3 (`processing_version` is caller-assigned), §7 (canonical reads); ARCT-470 (retraction rows); VEC-636 (`position_daily`, the first table built this way)

## Context

A derived read model answers a question its source table cannot answer cheaply: what a position held on a UTC date, what a market's state is now, what the latest row per key is. Its rows are computed from a governed table rather than ingested, and `schema_master.json` types them `derived`.

ADR-0006 governs the source tables. It says less about the tables downstream of them, and the little it says assumes they are shaped like their sources. Two of those assumptions are wrong in a way that loses data rather than merely reading oddly, and both were found by building one: `position_daily_observation` (VEC-636), whose rows carry the spine coordinate `(block_number, block_version, processing_version, block_timestamp)` copied verbatim from `position_state`.

The first assumption is that a derived table can allocate a `processing_version` for its own corrections. ADR-0006 §3 corrects a row by appending the same natural key at a higher `processing_version`, allocated from `processing_version_log`; ARCT-470 extends that to retraction rows. That works because a governed source owns the number. A derived table that copies it does not, and the collision is reachable through the sanctioned correction path:

1. A spine observation at `(block 100, pv 0)` crystallizes. The day reads 10.
2. A correction run withdraws the day, appending a tombstone at `(block 100, pv 1)` per ARCT-470.
3. The source genuinely reprocesses that same observation. `processing_version_log` hands out 1, the first allocation for the table, so `position_state` gains `(block 100, pv 1)` with the corrected value.
4. The writer computes that row as the day's winner and offers it with `ON CONFLICT DO NOTHING`. Its primary key is the tombstone's. The insert does nothing, the corrected value never arrives, the day stays withdrawn, and the procedure reports the zero it reports on a quiet run.

The second assumption is that a flag on a governed table means the same thing on a table derived from it. ARCT-470 says the `_current` caches "carry the column and their upsert copies it". Copying `is_retracted` downstream is not a narrower version of the upstream claim; it is a different claim, and acting on it withdraws data that is correct.

## Decision

**1. A derived table corrects itself on an axis it owns.** Where a derived table copies a version or coordinate from its source, that copy is read-only to it: the derived table never allocates a value in it. Corrections local to the derived table — including retractions — use a column the table owns, defaulting to 0 on every computed row, last in the primary key and last in the resolution ordering. Last, because it must break ties within one source coordinate and must never outrank a genuinely newer observation. `position_daily_observation.correction_seq` is the first instance.

A consequence worth stating plainly: a derived table needs no allocator. `processing_version_log` (ARCT-428) exists to serialise allocation in a namespace shared by concurrent correction runs of one table. A per-table correction axis is not shared with anything, so ARCT-428 does not block retraction on a derived table.

**2. A retraction is a claim about the rows of the table it is written on.** On a source table, `is_retracted` means *this observation never should have existed*. On a derived table it means *this derived key never should have existed*. These do not compose by copying. A derived writer therefore **filters** on its source's retraction flag — excluding retracted source rows from the computation, so the derived answer falls back to the next live source row — and never copies the flag into its own rows.

The two claims coincide only when every source row behind a derived key is retracted. Nothing derives that automatically: the writer has no new answer to append, so the stale derived row stands until someone decides to withdraw it. That decision is a retraction on the derived table, made by a person under a ticket.

**3. A derived read model is append-only, and its reads resolve rather than mutate.** The physical table is append-only under ADR-0006 §1; the answer is a view over a resolution function that picks the winning row per key. A correction appends and outranks. This is what makes the derived table reproducible: `<table>_as_of(T)` restricted to rows written by `T` returns the answer the model gave at `T`, which an in-place `_current` cache cannot do.

**4. The retraction filter sits outside the resolution, never inside it.** The read picks the winning row per key first, then drops the key if that winner is retracted. Filtering inside the resolution picks the newest *live* row instead, and answers with a reading the correction withdrew. The distinction is invisible in a table with one row per key and load-bearing in every table this ADR covers.

**5. Every retraction carries a ticket and a reason, enforced by the table.** A `CHECK` ties the pair to the flag in both directions: a tombstone without attribution is refused, and so is a computed row carrying attribution. Withdrawing data is a decision, and an undocumented one cannot be reviewed after the fact.

**6. A derived model publishes where it disagrees with its source, and why.** The ways a derived answer can legitimately differ from a recomputation of its source are finite, and each is either self-healing or a decision. They belong in a view with a reason per row rather than in a document. For `position_daily` the reasons are: the writer is behind and the next tick fixes it; a correction moved a source row to another key and stranded this one; a live row now outranks a tombstone; the source has no row for this key at all; the source row was re-stamped in place by a recovery path. Empty is the steady state, and a row that survives a refresh is the finding.

## Boundaries

What this ADR does **not** claim:

- **It does not make derived tables governed.** `governedTableTypes` in `run_id_coverage_integration_test.go` is `{raw_pipeline, dimension, config}`; `derived` is absent, so neither `position_state` nor any `_current` cache is checked for `run_id` today. Eight `*_current` caches have no `run_id` at all, measured by adding `derived` to that map. Closing it is a migration across those tables and belongs with ARCT-455/ARCT-439, not here.
- **It does not make the existing `_current` caches conform.** They are written `ON CONFLICT … DO UPDATE`, rewriting rows in place, which decisions 3 and 4 rule out. Whether they should be converted or exempted is a separate decision with a migration behind it. This ADR describes the shape new derived models take.
- **It does not claim the refresh is cheap.** Measured on a standalone rig, 20.4M source rows across 56,000 positions and 365 dates, yielding 20.4M derived keys: computing the winners takes 7.5s, and the full refresh takes 430s with `ON CONFLICT DO NOTHING` or 300s with an explicit anti-join, both writing nothing. The cost is the per-row comparison against the rows already stored, not the scan, and neither formulation avoids it. A model that re-offers every key on every tick is therefore bounded by its own size, and at volume the refresh has to be driven by what changed rather than by a full recomputation. That needs an arrival axis that is monotonic in commit order, which is ADR-0006 §5's `ingest_xid` (ARCT-438); `created_at` is transaction-start time and a row committed late but stamped early would be skipped permanently. Until then a derived model of this shape is sized for a full refresh, and `position_daily` is deployed at zero replicas against an empty spine.
- **It does not change ADR-0006 §3 for source tables.** Caller-assigned `processing_version` and `processing_version_log` stand exactly as written for tables that own their version. This ADR only says what a table that copies one must do instead.

## Consequences

- ARCT-470's rule needs amending in two places for derived tables: the retraction row's encoding (decision 1) and the `_current` caches copying the flag (decision 2). Both are stated there as prescriptions, so leaving them would put the written spec and the built code in conflict.
- A derived table's primary key gains a column. For `position_daily_observation` that is `correction_seq`, and the ordering it participates in is asserted by tests that fail when it is dropped or moved earlier.
- A reader of a derived model sees keys disappear. A retracted key is absent, not zero and not the previous value, and a consumer that treats absence as zero will be wrong in a way no error surfaces. This is the same contract the model already has for an unobserved key.
- The anomaly view of decision 6 scans the source, so it is a data-quality read rather than a hot path, and it needs a schedule or a dashboard to be worth anything. Nothing consumes `position_daily_anomaly` yet.

## Alternatives considered

**Retract at `processing_version + 1`, as ARCT-470 prescribes.** The status quo for source tables, and what `position_daily` did first. Rejected on the collision above, which is reachable through the sanctioned correction path and loses a corrected value with no error — the failing case is committed as `TestPositionDailyRetraction/does not squat on the spine's next version`.

**Give the derived table its own `processing_version_log` entry.** Would keep one column and one concept. Rejected: the column's value is copied from the source on every computed row, so a locally allocated value in the same column would mean two different things in one place, and the ordering could not tell them apart.

**Let the derived table delete or update the row it withdraws.** Simplest, and how the `_current` caches work today. Rejected under ADR-0006 §1: an answer already given must stay readable, which is the whole reason `<table>_as_of(T)` can reconstruct a past reading.

**Copy `is_retracted` down from the source and let the derived key disappear with it.** Rejected under decision 2: it withdraws a derived key whose other source rows are perfectly good, which is a data-loss bug dressed as consistency.

**Record the difference between a derived model and its source in the model's documentation.** The status quo before decision 6 — the cases were paragraphs in a pull request. Rejected because they cannot be queried, counted or alerted on, and because a case nobody can list is one nobody checks.
