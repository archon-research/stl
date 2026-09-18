# ADR-0009: Derived Read Models, Their Writers and Their Corrections

- **Status**: Proposed
- **Proposed**: @Yasanji
- **Date**: 2026-09-16
- **Deciders**: @vector
- **Relates to**: [ADR-0006](0006-data-reproducibility-and-append-only-guarantees.md) §1 (append-only), §3 (`processing_version` is caller-assigned), §5 (`ingest_xid`), §7 (canonical reads); ARCT-470 (retraction rows); ARCT-428 (`processing_version_log`); VEC-636 (`position_daily`, the first model built this way)

## Context

A derived read model answers a question its source cannot answer cheaply: what a position held on a UTC date, what the latest row per key is, what a market's state is now. Its rows are computed from a governed table rather than ingested, and `schema_master.json` types them `derived`.

ADR-0006 governs the source tables. It says little about the tables downstream of them, and what it does say assumes they are shaped like their sources. Two of those assumptions lose data rather than merely reading oddly. Both were found by building one — `position_daily_observation` (VEC-636), whose rows carry the coordinate `(block_number, block_version, processing_version, block_timestamp)` copied verbatim from `position_state`.

**The first assumption is that a derived table can allocate a `processing_version` for its own corrections.** ADR-0006 §3 corrects a row by appending the same natural key at a higher `processing_version` allocated from `processing_version_log`; ARCT-470 extends that to retraction rows. That works because a governed source owns the number. A derived table that copies it does not, and the collision is reachable through the sanctioned path:

1. A source observation at `(block 100, pv 0)` crystallizes. The day reads 10.
2. A correction run withdraws the day, appending a tombstone at `(block 100, pv 1)` per ARCT-470.
3. The source genuinely reprocesses that observation. `processing_version_log` hands out 1 — the table's first allocation — so `position_state` gains `(block 100, pv 1)` carrying the corrected value.
4. The writer computes that row as the day's winner and offers it with `ON CONFLICT DO NOTHING`. Its primary key is the tombstone's. The insert does nothing, the corrected value never arrives, the day stays withdrawn, and the procedure returns the same zero it returns on a quiet night.

**The second assumption is that a flag on a source means the same thing on a table derived from it.** ARCT-470 says the `_current` caches "carry the column and their upsert copies it". Copying `is_retracted` downstream is not a narrower version of the upstream claim; it is a different claim, and acting on it withdraws data that is correct.

A third thing is simply unrecorded: **how a derived model is refreshed.** `position_daily` is maintained by a scheduled writer, and the properties that make it safe to schedule — recompute rather than apply, idempotent per tick, settled periods only — are the reason a missed run, a double run and a crash are all survivable. That belongs in the record next to the shape it maintains.

## What this changes elsewhere

Stated up front, because it is the part a reader of ARCT-470 or ADR-0006 needs before anything else.

**ARCT-470 changes in two places, for derived tables only.** Its retraction encoding — the same natural key at a higher `processing_version` — is replaced by a correction axis the derived table owns (decision 1). Its rule that the `_current` caches "carry the column and their upsert copies it" is replaced by filtering on the source's flag rather than copying it (decision 2). Each has a measured failing case behind it, not a preference.

**One dependency it records is removed.** ARCT-470 is blocked by ARCT-428, the `processing_version_log` allocator. That block is real for a table that allocates in a shared version namespace, and decision 1 means a derived table does not, so retraction on a derived table is not waiting on ARCT-428.

**ADR-0006 §3 is untouched for source tables.** Caller-assigned `processing_version` and the allocator stand exactly as written wherever a table owns its own version. Nothing here narrows that.

## Decision

Each decision names what enforces it. A rule with no enforcement is a comment.

**1. A derived table corrects itself on an axis it owns.** Where a derived table copies a version or coordinate from its source, that copy is read-only to it: the derived table never allocates a value in it. Local corrections — retractions included — use a column the table owns, `NOT NULL DEFAULT 0` on every computed row, **last** in the primary key and **last** in the resolution ordering. Last, because it must break ties within one source coordinate and must never outrank a genuinely newer observation.

> *Enforced by*: `TestPositionDailyRetraction/does not squat on the spine's next version`, which fails on the pre-decision encoding; `TestPositionDailySchema` pins the key order; promoting the axis ahead of `block_number` is killed by the inert-retraction and losing-row cases.

A consequence worth stating plainly: **a derived table needs no allocator.** `processing_version_log` (ARCT-428) exists to serialise allocation in a namespace shared by concurrent correction runs of one table. A per-table correction axis is shared with nothing, so ARCT-428 does not block retraction on a derived table.

**2. A retraction is a claim about the rows of the table it is written on.** On a source table, `is_retracted` means *this observation never should have existed*. On a derived table it means *this derived key never should have existed*. These do not compose by copying. A derived writer therefore **filters** on its source's flag — excluding retracted source rows from the computation, so the answer falls back to the next live source row — and never copies the flag into its own rows.

The two claims coincide only when every source row behind a derived key is retracted. Nothing derives that automatically: the writer has no new answer to append, so the stale derived row stands until someone withdraws it.

> *Enforced by*: the column `COMMENT` states the day-level meaning; `position_daily_anomaly.orphaned_day` surfaces the coinciding case, tested by removing the source row and asserting only a retraction clears it.

**3. A derived read model is append-only, and its reads resolve rather than mutate.** The physical table is append-only under ADR-0006 §1 — `UPDATE`, `DELETE` and `TRUNCATE` revoked from the app role *and* from the owner. The answer is a view over a resolution function that picks the winning row per key; a correction appends and outranks. This is what makes the model reproducible: `<table>_as_of(T)` restricted to rows written by `T` returns the answer the model gave at `T`, which an in-place `_current` cache cannot do.

> *Enforced by*: `TestPositionDailyIsWrittenOnlyByItsOwnerUnderTheRealRole` reads the ACL rather than `has_table_privilege`, because the test owner is a superuser; `TestPositionDailyNeverTouchesAnAppendedRow` compares `ctid` and `xmin`, so a rewrite that preserved every value still fails.

**4. The retraction filter sits outside the resolution, never inside it.** The read picks the winning row per key first, then drops the key if that winner is retracted. Inside the resolution it picks the newest *live* row instead and answers with a reading the correction withdrew. The distinction is invisible in a table with one row per key and load-bearing in every table this ADR covers.

> *Enforced by*: moving the filter inside the resolution fails 19 cases; removing it fails 20. Both were re-measured after the read stopped being defined in two migrations — the later definition had been winning, so an earlier round of these numbers described a copy that no longer ran.

**5. A derived model is refreshed by a scheduled writer that recomputes and offers.** The writer never applies the change that just arrived. For each *settled* period it recomputes that period's winner over the whole source and offers it with `ON CONFLICT DO NOTHING`. Three properties follow, and they are why it is safe to put on a schedule at all: a missed run catches up, a double run writes nothing, and a crash rolls back and is redone. The writer reports the rows it wrote, so a run says what it did rather than that it ran; zero is the steady state.

A period is refreshed only once it has closed, plus a settling margin. The margin buys quiet, not correctness — a period crystallized early is repaired by the next run — so it is configuration, not a constant in a migration.

> *For `position_daily`*: `CALL crystallize_position_daily(settle_after)`, run by the `position-daily-crystallizer` cronjob — Temporal-scheduled like every other cronjob here, one tick is one statement, interval and settling window from the configmap per `stl-verify/AGENTS.md`.
> *Enforced by*: `TestPositionDailyCrystallizesOnceADayAndAppendsOnlyOnChange`, `TestPositionDailyCrystallizerAppendsOnlyWhatIsMissing`, `TestPositionDailyDoesNotCrystallizeTheCurrentDay`, `TestPositionDailyCrystallizerReportsWhatItWrote`, and `TestPositionDailyEqualsTheSpineArgmaxOverRandomHistories`, which drives randomised out-of-order histories through the materializer and compares every column the two tables share, taken from the catalogue rather than named.

**6. The correction writer is a separate, human-invoked procedure.** The schedule never retracts. Withdrawing a key is a decision, so it is its own procedure, called by a person under a ticket, idempotent on a re-run, and refusing a key the table does not hold rather than reporting a quiet zero.

> *For `position_daily`*: `CALL retract_position_daily(position_id, as_of_date, ticket, reason)`.
> *Enforced by*: `TestRetractPositionDaily` — seven cases covering the withdrawal, the idempotent re-run, the refusal of an absent key, and blank ticket/reason.

**7. Every retraction carries a ticket and a reason, enforced by the table.** A `CHECK` ties the pair to the flag in both directions: a tombstone without attribution is refused, and so is a computed row carrying attribution. The procedure is not the enforcement — a hand-written `INSERT` in a migration must fail the same way.

> *Enforced by*: `TestRetractPositionDaily/the table refuses unattributed or mis-attributed rows`, which drives the `INSERT` directly in both directions and asserts SQLSTATE 23514.

**8. A derived model publishes where it disagrees with its source, and why.** The ways a derived answer can legitimately differ from a recomputation of its source are finite, and each is either self-healing or a decision. They belong in a view with a reason per row, not in a document, because a case nobody can list is one nobody checks. Empty is the steady state; a row that survives a refresh is the finding.

> *Enforced by*: `TestPositionDailyAnomaly`, which asserts a healthy key reports nothing, and that each reason fires on its own cause and clears on its own remedy.

**9. Each object is defined once.** A migration that widens a table must not restate the reads over it, because the later definition silently wins and a mutation of the earlier one becomes inert — which is how an earlier round of this ADR's own enforcement figures came to describe code that no longer ran. Where the migrations land together, the column belongs in the `CREATE TABLE`; where they cannot, the redefinition is the only definition and the original is removed.

> *Enforced by*: removing the retraction filter from the superseded copy passed the entire suite, which is what surfaced it. Nothing yet fails on a re-definition itself; the protection is that there is one.

## The shape, worked

`position_daily` is the reference instantiation. Ordering legs are elided below where they repeat.

```sql
CREATE TABLE position_daily_observation (
    position_id        bytea NOT NULL,          -- the derived key
    as_of_date         date  NOT NULL,          -- the period
    ...                                         -- the answer's columns, copied
    block_number       bigint  NOT NULL,        -- the source coordinate: copied,
    block_version      integer NOT NULL,        -- never allocated here
    processing_version integer NOT NULL,
    block_timestamp    timestamptz NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),   -- the as-of axis
    is_retracted       boolean,                              -- decision 2, day-level
    correction_seq     integer NOT NULL DEFAULT 0,           -- decision 1, locally owned
    retraction_ticket  text, retraction_reason text,         -- decision 7
    PRIMARY KEY (position_id, as_of_date, block_number, block_version,
                 processing_version, block_timestamp, correction_seq),
    CHECK ((is_retracted IS TRUE) = (retraction_ticket IS NOT NULL)
       AND (retraction_ticket IS NULL) = (retraction_reason IS NULL))
);
```

The read resolves, then filters (decisions 3 and 4):

```sql
SELECT w.* FROM (
    SELECT DISTINCT ON (position_id, as_of_date, holder_id) *
      FROM position_daily_observation
     WHERE created_at <= position_daily_as_of_bound(seen_before)
     ORDER BY position_id, as_of_date, holder_id,
              block_number DESC, block_version DESC, processing_version DESC,
              block_timestamp DESC, correction_seq DESC
) w
WHERE w.is_retracted IS NOT TRUE;
```

`holder_id` joins the `DISTINCT ON` key for a reason worth recording, because it looks redundant: it is functionally determined by `position_id`, but a qual on a non-key column cannot be pushed below a `DISTINCT ON`. Measured on 4.32M rows against the read as it stands: a holder filter reaches `position_daily_observation_holder_idx` at 1.5ms with it, and post-filters the whole table at 4,905ms without.

The scheduled writer recomputes and offers (decision 5):

```sql
INSERT INTO position_daily_observation (...)
SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date) ...
  FROM position_state p
 WHERE (p.block_timestamp AT TIME ZONE 'utc')::date
       <= ((now() - settle_after) AT TIME ZONE 'utc')::date - 1
 ORDER BY ...
ON CONFLICT ON CONSTRAINT position_daily_observation_pkey DO NOTHING;
```

A correction appends at the same source coordinate and the next `correction_seq`, so it outranks what it withdraws and nothing else — and never occupies a key the source can reach.

## Threats

| Threat | What stops it |
| --- | --- |
| A derived correction occupies a primary key the source will later allocate, and the writer's `ON CONFLICT DO NOTHING` discards the real correction | Decision 1: the correction axis is local, so the source's coordinate space is untouched. Committed failing case |
| A retracted key answers from the row beneath it | Decision 4: the filter is outside the resolution |
| A source retraction withdraws a derived key whose other source rows are good | Decision 2: the derived writer filters, never copies |
| A withdrawal cannot be attributed after the fact | Decision 7: a `CHECK` on the table, not a convention in the procedure |
| The writer rewrites a superseded row, leaving counts and the current answer intact | `UPDATE`/`DELETE` revoked from owner and app role; the `ctid`/`xmin` comparison test |
| A scheduled run overlaps itself, is missed, or dies mid-write | Decision 5: recompute-and-offer is idempotent; one tick is one statement, so a crash rolls back |
| A derived answer silently diverges from its source | Decision 8: the anomaly view, with the divergence classified rather than merely counted |
| The model is refreshed from an arrival axis that is not monotonic in commit order, and a row is skipped permanently | Not solved here. `created_at` is transaction-start time; the safe axis is ADR-0006 §5's `ingest_xid` (ARCT-438). Until then the refresh is a full recomputation — see Boundaries |

## Adoption

New derived models take this shape. Existing ones do not, and this ADR does not convert them:

- **`position_daily`** (VEC-636) is the reference instantiation and ships at zero replicas against an empty spine.
- **The `*_current` caches** are written `ON CONFLICT … DO UPDATE`, which decisions 3 and 4 rule out. Whether they are converted or exempted is a separate decision with a migration behind it, and eight of them carry no `run_id` at all, so the governance question (ARCT-455/ARCT-439) has to be settled alongside.
- **ARCT-470 needs amending** in two places before it is built: the retraction encoding for derived tables, and the `_current` caches copying the flag. Both are prescriptions there, so leaving them puts the written spec and the built code in conflict.

## Boundaries

What this ADR does **not** claim:

- **It does not make derived tables governed.** `governedTableTypes` in `run_id_coverage_integration_test.go` is `{raw_pipeline, dimension, config}`; `derived` is absent, so neither `position_state` nor any `_current` cache is checked for `run_id` today. Measured by adding `derived` to that map: the two position tables pass, eight caches fail for want of the column.
- **It makes no claim about the refresh cost, because it has not been measured on real data.** `position_state` holds zero rows in production and `position_daily_observation` does not exist there, so there is nothing to measure. What is known from a real table is the shape a source has: `allocation_position` holds 2,211,448 rows over 88 distinct positions and 11,309 distinct (position, day) keys, and 455,821 of those rows (20.6%) carry `processing_version > 0`, so corrections are routine rather than exceptional. Timings taken on generated fixtures are omitted here: the first such fixture used 56,000 positions and 20.4M keys, roughly 1,800x the only real key count available, and the conclusion drawn from it did not survive contact with that number. Whether a full refresh stays cheap is a measurement to take against real data once the materializers land, not a property to assert now.
- **It does not change ADR-0006 §3 for source tables.** Caller-assigned `processing_version` and `processing_version_log` stand exactly as written for a table that owns its version. This ADR says only what a table that *copies* one must do instead.
- **It does not give the anomaly view a consumer.** It scans the source, so it is a data-quality read rather than a hot path, and it needs a schedule or a dashboard to be worth anything. Nothing reads `position_daily_anomaly` yet.

## Consequences

- A derived table's primary key gains a column, and the ordering it participates in becomes load-bearing rather than incidental.
- A reader sees keys disappear. A retracted key is absent — not zero, not the previous value — and a consumer that treats absence as zero is wrong in a way no error surfaces. It is the same contract the model already has for an unobserved period.
- Two writers instead of one: a schedule that never withdraws, and a human-invoked procedure that only withdraws. The separation is the point — an automated withdrawal is a data-loss mechanism with no one's name on it.
- A derived model now has an answer to "why does this not match the source", which is the question every such table eventually gets asked, and it is a query rather than an investigation.

## Alternatives considered

**Retract at `processing_version + 1`, as ARCT-470 prescribes.** The status quo for source tables, and what `position_daily` did first. Rejected: the tombstone occupies the key the source's next correction resolves to, so the correction is discarded with no error and a zero that reads like a quiet run. The failing case is committed.

**Give the derived table its own `processing_version_log` entry.** Keeps one column and one concept. Rejected: the column is copied from the source on every computed row, so a locally allocated value in it would mean two things in one place, and the resolution ordering could not tell them apart.

**Let the derived table delete or update the row it withdraws.** Simplest, and how the `_current` caches work today. Rejected under ADR-0006 §1: an answer already given must stay readable, which is what makes `<table>_as_of(T)` possible at all.

**Copy `is_retracted` down from the source.** What ARCT-470 prescribes for caches, and superficially the consistent choice. Rejected under decision 2: it withdraws a derived key whose other source rows are good. Withdrawing an observation should narrow the input set, not delete the answer.

**Refresh incrementally from `created_at` now, rather than waiting for ARCT-438.** Would bound the cost measured in Boundaries. Rejected: `created_at` is transaction-start time, so a row stamped before a watermark and committed after it is skipped permanently and silently — a correctness bug traded for a performance one, on a model whose source is empty.

**Leave the divergences in the model's documentation.** The status quo before decision 8. Rejected: prose cannot be queried, counted or alerted on, and it drifts from the code the moment either changes.
