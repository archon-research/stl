# NFAT Service Architecture: Custom State Machine Engine

## 1. Context and Problem Statement

NFAT (Non-Fungible Asset Token) is a compliance verification service that manages the lifecycle of structured finance facilities in DeFi. Before a facility can begin originating assets, it must pass through a multi-phase compliance process. **Phase 0** is the one-time facility setup: a 14-step directed acyclic graph involving six distinct actor groups, on-chain contract deployments, governance timelocks, and writes to multiple data stores (Synome NFAT, Synome Axis, on-chain state).

**Key requirements:**

- The Phase 0 process is DAG-shaped with three independent entry points, two merge points, and a hard 14-day timer gate (cBEAM timelock).
- Five step types: **Definition**, **Approval**, **Configuration**, **Review**, and **Submission**.
- Multiple actor groups participate: Halo Contributor Team, Vector, Risk Team, Core Council, GovOps, Star Agent, and Halo.
- Steps write to heterogeneous backends: Synome (NFAT), Synome Axis, on-chain contracts via beacons (lpha-nfat, lpha-attest, Council Beacon).
- Processes can be long-running. The cBEAM timelock alone introduces a mandatory 14-day wait. Human approvals and reviews may take days or weeks.
- Every state transition, decision, and timeout must be fully auditable (compliance context).
- The system must be resilient to crashes, restarts, and concurrent execution across multiple instances.

**This document describes the Custom Engine approach**: a purpose-built state machine engine backed entirely by PostgreSQL. There is no external workflow orchestrator (e.g., Temporal). The database is the single source of truth for process definitions, instance state, step execution, and audit history.

## 2. Architecture Overview

```
                                    +---------------------------+
                                    |      HTTP API Server      |
                                    |  (human actions:          |
                                    |   approvals, reviews,     |
                                    |   submissions, configs)   |
                                    +------------+--------------+
                                                 |
                                                 | calls
                                                 v
+-------------------+          +----------------------------------+
| SQS Consumer      |          |        NFAT Engine Service       |
| (FacilitySetup    |--------->|                                  |
|  events)          |          |  HandleTriggerEvent()            |
+-------------------+          |  SubmitStepAction()              |
                               |  AdvanceInstance()  <-- core     |
                               |  HandleTimerExpiry()             |
                               +--------+-----+--+---------------+
                                        |     |  |
                          +-------------+     |  +---------------+
                          |                   |                  |
                          v                   v                  v
               +-------------------+  +---------------+  +---------------+
               |    PostgreSQL     |  |  Beacon Layer |  |   SNS FIFO    |
               |                   |  |               |  |  (EventSink)  |
               | - definitions     |  | lpha-nfat     |  |               |
               | - instances       |  | lpha-attest   |  | PhaseComplete |
               | - step states     |  | Council       |  | StepCompleted |
               | - audit log       |  | Beacon        |  |               |
               +-------------------+  +-------+-------+  +---------------+
                          ^                   |
                          |                   v
               +-------------------+  +---------------+
               | Timeout Ticker    |  | Synome (NFAT) |
               | (goroutine,       |  | Synome Axis   |
               |  every 30s --     |  | On-chain      |
               |  cBEAM lock,      |  | contracts     |
               |  stale locks)     |  +---------------+
               +-------------------+
```

**Flow summary:**

1. An SQS message (e.g., `FacilitySetupRequested`) triggers `HandleTriggerEvent`, which creates an NFAT instance (a Book) pinned to the active Phase 0 definition version, and calls `AdvanceInstance`.
2. `AdvanceInstance` is the core loop. It loads the instance, evaluates which steps can progress, executes automated steps or parks human-action steps, and writes audit entries -- all within a single database transaction.
3. Human actions (approvals, reviews, submissions, configurations) arrive via HTTP POST. The handler validates the request, records the action, invokes the appropriate beacon for on-chain/Synome writes, and calls `AdvanceInstance` to continue.
4. A periodic goroutine (the timeout ticker) scans for timer gates (e.g., the 14-day cBEAM timelock on step 0.7) and stale locks. When a timer expires, it advances the instance.
5. On phase completion, an event is published to SNS FIFO via the `EventSink` port.

## 3. Domain Model

### ProcessDefinition

Top-level facility lifecycle process template. Identified by a slug (e.g., `nfat-phase-0`). Represents the canonical compliance process for facility setup.

### ProcessDefVersion

An immutable snapshot of a process definition at a point in time. Contains the full step graph (steps + edges) as structured data. Each modification creates a new version. Active instances are pinned to the version they started with.

### StepDefinition

A single node in the DAG. Attributes:

| Field | Description |
|-------|-------------|
| `id` | Unique identifier within the version (e.g., `0.1`, `0.2`, ..., `0.14`) |
| `name` | Human-readable label (e.g., "Halo Defines Buy Box Parameters") |
| `step_type` | `definition`, `approval`, `configuration`, `review`, `submission` |
| `actor` | The responsible actor group (e.g., `halo_contributor_team`, `vector`, `risk_team`, `core_council`, `govops`, `star_agent`, `halo`) |
| `config` | Type-specific configuration: beacon targets, Synome write specs, timer durations, required documents, on-chain contract parameters |
| `join_mode` | `all` (wait for all predecessors) or `any` (proceed on first). Default: `all` |

### Edge

A directed connection between two steps. Attributes:

| Field | Description |
|-------|-------------|
| `from_step_id` | Source step |
| `to_step_id` | Target step |
| `condition` | Optional expression evaluated against the source step's result. Null means unconditional. |

### Book

The central entity representing a facility under construction. A running instance of the Phase 0 process. The Book entity carries the facility's state machine (initial state: `CREATED`, advancing through later phases).

| Field | Description |
|-------|-------------|
| `id` | UUID primary key |
| `def_version_id` | Pinned process definition version |
| `status` | `running`, `completed`, `failed`, `cancelled` |
| `book_state` | Facility state machine value (e.g., `CREATED`) |
| `trigger_event_type` | Event that created this instance |
| `trigger_event_payload` | Full event body (JSONB) |
| `context_data` | Accumulated data from step results (JSONB): risk parameters, buy box params, attestor details, compliance docs, contract addresses |
| `dedup_key` | Idempotency key derived from trigger event |
| `created_at` | Timestamp |
| `updated_at` | Timestamp |
| `completed_at` | Nullable timestamp |

### StepState

Per-step execution state within an instance. Includes locking fields for concurrency control, timer fields for the cBEAM timelock, and an attempt counter for retry tracking.

| Field | Description |
|-------|-------------|
| `id` | UUID primary key |
| `book_id` | FK to Book |
| `step_id` | References step within the pinned definition (e.g., `0.7`) |
| `status` | `pending`, `running`, `waiting_action`, `waiting_timer`, `completed`, `failed`, `skipped` |
| `result_payload` | Output of step execution (JSONB) |
| `actor` | Actor group responsible for this step |
| `completed_by` | User who completed the step |
| `started_at` | Timestamp |
| `completed_at` | Timestamp |
| `deadline_at` | Timer expiry (e.g., cBEAM 14-day lock on step 0.7) |
| `attempts` | Number of execution attempts |
| `last_error` | Error message from most recent failed attempt |
| `locked_by` | Worker ID holding the lock (UUID or hostname) |
| `locked_at` | Timestamp when lock was acquired |

### AuditLogEntry

Immutable record of every state transition and action.

| Field | Description |
|-------|-------------|
| `id` | UUID primary key |
| `book_id` | FK to Book |
| `step_id` | Nullable (instance-level events have no step) |
| `action` | `step_started`, `step_completed`, `step_failed`, `action_submitted`, `timer_started`, `timer_expired`, `beacon_write`, `instance_completed`, etc. |
| `actor` | System identifier or user ID |
| `details` | Structured payload (JSONB): beacon target, Synome write details, on-chain tx hash, etc. |
| `created_at` | Timestamp (partition key) |

## 4. Database Schema

```sql
-- ============================================================
-- Process Definition Tables
-- ============================================================

CREATE TABLE nfat_process_definitions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    description     TEXT,
    owner           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE nfat_process_def_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    definition_id   UUID NOT NULL REFERENCES nfat_process_definitions(id),
    version         INT NOT NULL,
    steps           JSONB NOT NULL,       -- array of StepDefinition objects
    dag_edges       JSONB NOT NULL,       -- array of Edge objects
    is_active       BOOLEAN NOT NULL DEFAULT false,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (definition_id, version)
);

CREATE INDEX idx_def_versions_definition ON nfat_process_def_versions(definition_id);
CREATE INDEX idx_def_versions_active ON nfat_process_def_versions(definition_id)
    WHERE is_active = true;

-- ============================================================
-- Instance Tables
-- ============================================================

CREATE TABLE nfat_books (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    def_version_id          UUID NOT NULL REFERENCES nfat_process_def_versions(id),
    status                  TEXT NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    book_state              TEXT NOT NULL DEFAULT 'CREATED'
                            CHECK (book_state IN ('CREATED', 'ACTIVE', 'CLOSED')),
    trigger_event_type      TEXT NOT NULL,
    trigger_event_payload   JSONB NOT NULL,
    context_data            JSONB NOT NULL DEFAULT '{}',
    dedup_key               TEXT UNIQUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at            TIMESTAMPTZ
);

CREATE INDEX idx_books_status ON nfat_books(status);
CREATE INDEX idx_books_def_version ON nfat_books(def_version_id);
CREATE INDEX idx_books_book_state ON nfat_books(book_state);

CREATE TABLE nfat_step_states (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    book_id         UUID NOT NULL REFERENCES nfat_books(id),
    step_id         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'running', 'waiting_action',
                        'waiting_timer', 'completed', 'failed', 'skipped'
                    )),
    result_payload  JSONB,
    actor           TEXT,
    completed_by    TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    UNIQUE (book_id, step_id)
);

CREATE INDEX idx_step_states_book ON nfat_step_states(book_id);
CREATE INDEX idx_step_states_status ON nfat_step_states(status);
CREATE INDEX idx_step_states_deadline ON nfat_step_states(deadline_at)
    WHERE status = 'waiting_timer' AND deadline_at IS NOT NULL;
CREATE INDEX idx_step_states_waiting ON nfat_step_states(status, actor)
    WHERE status = 'waiting_action';
CREATE INDEX idx_step_states_locked ON nfat_step_states(locked_at)
    WHERE status = 'running' AND locked_by IS NOT NULL;

-- ============================================================
-- Audit Log (TimescaleDB hypertable with 7-day chunks)
-- ============================================================

CREATE TABLE nfat_audit_log (
    id          UUID NOT NULL DEFAULT gen_random_uuid(),
    book_id     UUID NOT NULL,
    step_id     TEXT,
    action      TEXT NOT NULL,
    actor       TEXT NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('nfat_audit_log', 'created_at',
    chunk_time_interval => INTERVAL '7 days');

CREATE INDEX idx_audit_log_book ON nfat_audit_log(book_id, created_at DESC);
CREATE INDEX idx_audit_log_step ON nfat_audit_log(book_id, step_id, created_at DESC);
```

## 5. Process Versioning: Pin-to-Version

Process definitions evolve over time as governance requirements change. The versioning strategy ensures that in-flight instances are never disrupted by definition changes.

**Rules:**

1. Each modification creates a new `nfat_process_def_versions` row with an incremented `version` number. The step graph and edge list are stored as immutable JSONB.
2. Only one version per definition can be marked `is_active = true` at a time. New instances use the active version.
3. When a Book is created, it stores the `def_version_id` of the currently active version. This is the **pinned version** for the lifetime of that instance.
4. In-flight instances always read their steps and edges from their pinned version, regardless of subsequent edits.
5. Old versions are retained indefinitely for auditability.

**Activation flow:**

1. Governance team proposes a process change (e.g., adding a new compliance check step between 0.10 and 0.13).
2. Backend validates the DAG (no cycles, all edges reference valid steps, at least one entry point, all merge points have correct join modes).
3. New version row is inserted with `is_active = false`.
4. After review, the version is explicitly activated. A transaction sets the old version to `is_active = false` and the new version to `is_active = true`.

## 6. Execution Strategy: AdvanceInstance Core Loop

`AdvanceInstance` is the central operation of the engine. It is called whenever something changes: a new instance is created, a step completes, a human submits an action, or a timer expires.

```
AdvanceInstance(bookID):
  1. BEGIN TRANSACTION (READ COMMITTED)

  2. SELECT * FROM nfat_books WHERE id = bookID FOR UPDATE
     -- Row-level lock on the Book prevents concurrent AdvanceInstance calls
     -- for the same instance from interleaving.

  3. Load all step states for this Book.

  4. Load pinned definition version (steps + edges).

  5. If no step states exist (fresh instance):
     a. Identify entry point steps (steps with no incoming edges).
        For Phase 0, these are: 0.1, 0.8, and 0.10.
     b. INSERT step states with status = 'pending' for all three entry points.
        This launches three parallel paths immediately.

  6. LOOP: collect all steps in 'pending' status.
     For each pending step:

     a. Check predecessor satisfaction (join_mode evaluation):
        - Load all incoming edges for this step.
        - If join_mode = 'all': verify ALL predecessor steps are terminal.
          Example: step 0.6 requires 0.3 + 0.4 + 0.5 all completed.
          Example: step 0.13 requires 0.10 + 0.11 both completed.
        - If join_mode = 'any': at least one predecessor is terminal.
        - If not satisfied, skip this step (leave as 'pending').

     b. TRY LOCK:
        UPDATE nfat_step_states
        SET locked_by = $workerID, locked_at = now(),
            status = 'running', started_at = now(),
            attempts = attempts + 1
        WHERE id = $stepStateID AND locked_by IS NULL
        -- If another worker locked it (0 rows updated), skip.

     c. EXECUTE by step type (see Section 7 for details):

        definition / review / submission:
          - Set status = 'waiting_action'.
          - Set actor from step config.
          - Release lock (set locked_by = NULL).
          - Step waits for human input via HTTP API.

        approval:
          - Set status = 'waiting_action'.
          - Set actor from step config.
          - Release lock.
          - Step waits for council/governance approval via HTTP API.

        configuration:
          - If step has a timer gate (e.g., step 0.7 with 14-day cBEAM lock):
            - Set status = 'waiting_timer'.
            - Set deadline_at = now() + configured duration.
            - Release lock.
            - Step waits for timeout ticker to detect expiry.
          - Otherwise:
            - Set status = 'waiting_action'.
            - Set actor from step config.
            - Release lock.

     d. Write audit log entry for the action taken.

     e. If step completed (from a re-entrant call after human action),
        evaluate outgoing edges:
        - For each edge from this step:
          - If edge has a condition, evaluate against step result_payload.
          - If condition passes (or no condition), check target step:
            - If target step state does not exist:
              INSERT step state with status = 'pending'.

     f. Clear lock on completed/failed steps (set locked_by = NULL).

  7. Repeat step 6 if any new 'pending' steps were created
     (cascade within the same transaction).

  8. Check termination:
     - If all 14 steps are in a terminal state (completed, failed, skipped):
       - If any required step failed: set Book status = 'failed'.
       - Otherwise: set Book status = 'completed', set completed_at.
         Step 0.14 (GovOps Enables Prime Access) is the final step;
         its completion means Phase 0 is done.
       - Write instance-level audit log entry.
       - Publish completion event via EventSink (enqueued for post-commit).

  9. COMMIT TRANSACTION
```

**Post-commit:** Publish any enqueued SNS events. If publishing fails, the instance state is already committed. A separate reconciliation process can detect completed instances with unpublished events.

**Concrete Phase 0 flow through AdvanceInstance:**

1. Fresh instance: entry points 0.1, 0.8, 0.10 all set to `pending`, then immediately to `waiting_action` for their respective actors.
2. Halo submits buy box parameters (0.1 completes) -> 0.2 becomes `pending` -> Vector initialises risk model.
3. Meanwhile, GovOps identifies attestor (0.8 completes) -> 0.9 becomes `pending`.
4. Meanwhile, Halo submits compliance docs (0.10 completes) -> waits for 0.11 at merge.
5. Risk path continues: 0.2 -> 0.3 -> 0.4 -> 0.5 -> 0.6 (merge, needs 0.3+0.4+0.5).
6. 0.6 completes (Council approves risk framework, beacon writes to Synome Axis) -> 0.7 set to `waiting_timer` with `deadline_at = now() + 14 days`.
7. After 14 days, timeout ticker detects 0.7 expiry -> sets 0.7 to `completed` -> 0.11 becomes `pending` (needs 0.6 + 0.7 elapsed).
8. 0.11 completes (facility registered) -> 0.12 becomes `pending`, and 0.13 becomes `pending` if 0.10 is also done (merge point).
9. 0.13 completes (Star Agent reviews) -> 0.14 becomes `pending` -> GovOps enables Prime access -> Phase 0 complete.

## 7. Step Type Execution

Each of the five step types has a distinct execution pattern within the engine.

### Definition Steps

Steps 0.1 (Buy Box Parameters), 0.2 (Risk Model Init), 0.3 (Financial Risk Params), 0.4 (Non-Financial Risk Params), 0.5 (Ecosystem Risk Params).

**Lifecycle:**

1. `AdvanceInstance` encounters a definition step. It sets `status = 'waiting_action'` and populates `actor` (e.g., `halo_contributor_team` for 0.1, `vector` for 0.2, `risk_team` for 0.3-0.5).
2. The step row sits in the database. No goroutine is blocked.
3. The responsible actor submits the definition data via HTTP POST.
4. The handler:
   a. Validates the actor has the required role.
   b. Validates the step is in `waiting_action` status.
   c. Stores the definition payload in `result_payload` (e.g., buy box parameters, risk model configuration, financial/non-financial/ecosystem risk parameters).
   d. Merges relevant data into the Book's `context_data`.
   e. Sets `status = 'completed'`, `completed_by`, `completed_at`.
   f. Writes an audit log entry.
   g. Calls `AdvanceInstance` to continue the process.

### Approval Steps

Step 0.6 (Core Council Approves Risk Framework), step 0.9 (GovOps Whitelists Attestor Company).

**Lifecycle:**

1. `AdvanceInstance` sets `status = 'waiting_action'` and `actor` (e.g., `core_council` for 0.6, `govops` for 0.9).
2. The approver reviews the accumulated context data and submits a decision via HTTP POST.
3. The handler:
   a. Validates the actor has approval authority.
   b. Records the decision in `result_payload`.
   c. On approval: sets `status = 'completed'`.
   d. On rejection: sets `status = 'failed'`.
   e. **Beacon integration** (critical for approval steps):
      - Step 0.6: Writes the approved risk framework to Synome Axis via Council Beacon.
      - Step 0.9: Writes attestor whitelist entry on-chain and to Synome (NFAT) via lpha-nfat beacon.
   f. Writes an audit log entry including beacon write confirmation (tx hash, Synome write ID).
   g. Calls `AdvanceInstance`.

### Configuration Steps

Step 0.7 (Core Council Grants cBEAM), step 0.11 (GovOps Registers NFAT Facility), step 0.12 (GovOps Determines Prime Type), step 0.14 (GovOps Enables Prime Access).

**Lifecycle varies by whether the step has a timer gate:**

**Step 0.7 -- cBEAM Grant with 14-day BEAMTimeLock:**

1. `AdvanceInstance` encounters step 0.7. It recognizes the timer gate in the step config.
2. Sets `status = 'waiting_timer'`, `deadline_at = now() + 14 days`.
3. The cBEAM grant itself is executed on-chain at this point (the token is granted, but the timelock period begins).
4. The step row sits in the database for 14 days. No resources consumed.
5. The timeout ticker (running every 30 seconds) periodically scans:
   ```sql
   SELECT id, book_id, step_id, deadline_at
   FROM nfat_step_states
   WHERE status = 'waiting_timer'
     AND deadline_at <= now();
   ```
6. When the 14-day lock expires, the ticker sets `status = 'completed'`, writes an audit log entry (`timer_expired`), and calls `AdvanceInstance` to unblock step 0.11.

**Steps 0.11, 0.12, 0.14 -- Human-initiated configuration:**

1. `AdvanceInstance` sets `status = 'waiting_action'` and `actor = 'govops'`.
2. GovOps submits the configuration action via HTTP POST.
3. The handler:
   a. Validates actor permissions.
   b. Executes beacon writes:
      - Step 0.11: Deploys on-chain contracts via lpha-nfat beacon. Creates Book template in Synome (NFAT) with `book_state = CREATED`.
      - Step 0.12: Writes Prime type determination to Synome (NFAT).
      - Step 0.14: Writes Prime access enablement on-chain and to Synome (NFAT) via lpha-nfat beacon.
   c. Stores results (contract addresses, Synome write confirmations) in `result_payload`.
   d. Sets `status = 'completed'`.
   e. Writes audit log entry.
   f. Calls `AdvanceInstance`.

### Review Steps

Step 0.8 (GovOps Identifies Attestor Company).

**Lifecycle:**

1. `AdvanceInstance` sets `status = 'waiting_action'` and `actor = 'govops'`.
2. GovOps performs due diligence on potential attestor companies and submits the selected attestor via HTTP POST.
3. The handler:
   a. Validates the actor and the attestor company data.
   b. Stores the attestor identification in `result_payload`.
   c. Sets `status = 'completed'`.
   d. Writes audit log entry.
   e. Calls `AdvanceInstance` to unblock step 0.9.

### Submission Steps

Step 0.10 (Halo Provides Risk and Compliance Inputs).

**Lifecycle:**

1. `AdvanceInstance` sets `status = 'waiting_action'` and `actor = 'halo'`.
2. Halo submits 6 sub-documents containing risk and compliance inputs via HTTP POST.
3. The handler:
   a. Validates all 6 required sub-documents are present and well-formed.
   b. Writes each sub-document to Synome (NFAT).
   c. Stores submission confirmation in `result_payload`.
   d. Merges compliance document references into the Book's `context_data`.
   e. Sets `status = 'completed'`.
   f. Writes audit log entry.
   g. Calls `AdvanceInstance`. Step 0.13 can now proceed if 0.11 is also complete.

### Step 0.13 -- Approval with Merge

Step 0.13 (Star Agent Administers Halo in Atlas) is an approval step at a merge point requiring both 0.10 and 0.11 to be complete.

1. `AdvanceInstance` checks join_mode = `all` for step 0.13. Both 0.10 (compliance docs) and 0.11 (facility registered) must be `completed`.
2. Once both predecessors are satisfied, step 0.13 becomes `pending` and then `waiting_action` for `star_agent`.
3. Star Agent reviews the compliance documents (submitted in 0.10) in the context of the registered facility (0.11).
4. On approval, writes to Synome (NFAT) and calls `AdvanceInstance` to unblock 0.14.

## 8. Beacon Integration

Steps that write to on-chain state or Synome databases do so through a beacon layer. In the Custom Engine approach, beacon interactions are handled by a `BeaconHandlerRegistry` -- analogous to the `CheckHandlerRegistry` but for external writes rather than automated checks.

```go
type BeaconHandler interface {
    Execute(ctx context.Context, beacon string, payload map[string]any) (BeaconResult, error)
}

type BeaconResult struct {
    TxHash       string         // On-chain transaction hash (if applicable)
    SynomeWriteID string        // Synome write confirmation ID (if applicable)
    Details      map[string]any // Additional beacon-specific results
}
```

**Beacon-to-step mapping:**

| Step | Beacon | Write Target | Payload |
|------|--------|-------------|---------|
| 0.6 | Council Beacon | Synome Axis | Approved risk framework |
| 0.7 | lpha-nfat | On-chain | cBEAM grant with timelock |
| 0.9 | lpha-nfat + lpha-attest | On-chain + Synome (NFAT) | Attestor whitelist entry |
| 0.10 | lpha-nfat | Synome (NFAT) | 6 compliance sub-documents |
| 0.11 | lpha-nfat | On-chain + Synome (NFAT) | Contract deployment, Book template (state: CREATED) |
| 0.12 | lpha-nfat | Synome (NFAT) | Prime type determination |
| 0.13 | lpha-nfat | Synome (NFAT) | Atlas administration record |
| 0.14 | lpha-nfat | On-chain + Synome (NFAT) | Prime access enablement |

**Beacon handlers are registered at startup:**

```go
registry := nfat.NewBeaconHandlerRegistry()
registry.Register("council_beacon", beacons.NewCouncilBeaconHandler(councilClient))
registry.Register("lpha_nfat", beacons.NewLphaNFATBeaconHandler(nfatClient))
registry.Register("lpha_attest", beacons.NewLphaAttestBeaconHandler(attestClient))
```

When a step's HTTP handler processes a human action, it:
1. Looks up the beacon handler(s) from the step config.
2. Calls `Execute` with the appropriate payload.
3. Stores the `BeaconResult` in the step's `result_payload`.
4. Writes an audit log entry with action `beacon_write` including the tx hash and Synome write ID.
5. Only marks the step as `completed` after beacon confirmation.

If a beacon write fails, the step remains in its current status. The human can retry the action.

## 9. Concurrency and Consistency

### Row-Level Locking

The `SELECT ... FOR UPDATE` on the `nfat_books` row at the start of `AdvanceInstance` serializes all concurrent calls for the same instance. Under PostgreSQL's default `READ COMMITTED` isolation, this prevents double-execution without requiring higher isolation levels.

Additionally, the `UPDATE WHERE locked_by IS NULL` pattern on individual step states provides a secondary guard. Even if two workers somehow enter the same instance concurrently, only one will succeed in locking any given step.

### Crash Recovery

If an engine instance crashes mid-execution, it may leave steps in `running` status with a stale `locked_by` value. A periodic recovery scan handles this:

```sql
SELECT id, book_id, step_id, attempts
FROM nfat_step_states
WHERE status = 'running'
  AND locked_at < now() - INTERVAL '5 minutes';
```

For each stale step:
- If `attempts < max_retries` (from step config, default 3): reset `status = 'pending'`, clear `locked_by` and `locked_at`.
- If `attempts >= max_retries`: set `status = 'failed'`, write audit log entry.
- Call `AdvanceInstance` for the affected Book.

### Transaction Boundaries

The entire `AdvanceInstance` call runs within a single database transaction. Step state changes, new step creation, and audit log entries are committed atomically. Either all changes for a given advance cycle persist, or none do.

**Important exception:** Beacon writes are external side effects. They occur before the transaction commits. If the transaction fails after a successful beacon write, the beacon write cannot be rolled back. To handle this:
- Beacon writes are idempotent where possible.
- Each beacon write records a unique operation ID. On retry, the handler checks whether the operation already succeeded.
- The audit log records beacon write attempts regardless of transaction outcome.

### Idempotent Event Handling

The `dedup_key` column on `nfat_books` (with a `UNIQUE` constraint) prevents duplicate instances from the same trigger event. If an SQS message is delivered twice, the second `INSERT` fails with a unique violation, which the consumer handles gracefully by acknowledging the message.

### Horizontal Scaling

Multiple engine instances can run safely:

- The `FOR UPDATE` row lock on `nfat_books` serializes `AdvanceInstance` calls per Book.
- The `locked_by` check on step states prevents double-execution at the step level.
- The timeout ticker is idempotent: multiple tickers scanning overlapping windows will attempt the same steps, but only one will succeed in updating each step due to the lock check.
- SQS provides at-least-once delivery with visibility timeout, so only one consumer processes a given message at a time.

## 10. The Phase 0 DAG (Concrete Example)

The following diagram shows all 14 steps of Phase 0 with their dependencies, parallel paths, merge points, and the timer gate.

```
PARALLEL PATH 1: Risk Framework
=================================

  [0.1] Halo Defines Buy Box Parameters
    |   Actor: Halo Contributor Team | Type: Definition
    v
  [0.2] Vector Initialises Risk Model
    |   Actor: Vector | Type: Definition
    v
  [0.3] Risk Team Collects Financial Risk Parameters
    |   Actor: Risk Team | Type: Definition
    v
  [0.4] Risk Team Defines Non-Financial Risk Parameters
    |   Actor: Risk Team | Type: Definition
    v
  [0.5] Risk Team Defines Ecosystem Risk Parameters
    |   Actor: Risk Team | Type: Definition
    v
  [0.6] Core Council Approves Risk Framework  <-- MERGE: requires 0.3 + 0.4 + 0.5
    |   Actor: Core Council | Type: Approval
    |   Beacon: Council Beacon -> Synome Axis
    v
  [0.7] Core Council Grants cBEAM to GovOps
        Actor: Core Council | Type: Configuration
        >>> 14-DAY BEAMTimeLock (waiting_timer) <<<
        |
        v  (after 14 days)


PARALLEL PATH 2: Attestor Onboarding
======================================

  [0.8] GovOps Identifies Attestor Company
    |   Actor: GovOps | Type: Review
    v
  [0.9] GovOps Whitelists Attestor Company
        Actor: GovOps | Type: Approval
        Beacon: lpha-nfat + lpha-attest -> On-chain + Synome (NFAT)


PARALLEL PATH 3: Compliance Documentation
===========================================

  [0.10] Halo Provides Risk and Compliance Inputs
         Actor: Halo | Type: Submission
         6 sub-documents -> Synome (NFAT)


CONVERGENCE: Facility Registration and Completion
===================================================

  [0.11] GovOps Registers NFAT Facility  <-- MERGE: requires 0.7 (timer elapsed) + 0.6
    |   Actor: GovOps | Type: Configuration
    |   Beacon: lpha-nfat -> On-chain + Synome (NFAT) [Book state: CREATED]
    |
    +-------v-----------+
    |                   |
    v                   |
  [0.12] GovOps         |
    Determines          |
    Prime Type          |
    Actor: GovOps       |
    Type: Config        |
    -> Synome (NFAT)    |
                        v
                [0.13] Star Agent Administers Halo in Atlas
                  |     <-- MERGE: requires 0.10 + 0.11
                  |     Actor: Star Agent | Type: Approval
                  |     -> Synome (NFAT)
                  v
                [0.14] GovOps Enables Prime Access
                        Actor: GovOps | Type: Configuration
                        Beacon: lpha-nfat -> On-chain + Synome (NFAT)
                        >>> PHASE 0 COMPLETE <<<
```

**Simplified dependency graph:**

```
        0.1 ─── 0.2 ─── 0.3 ─── 0.4 ─── 0.5
                          |       |       |
                          +───────+───────+
                                  |
                                 0.6 ──── 0.7 [14d timer]
                                  |               |
                                  +───────────────+
                                          |
  0.8 ── 0.9                             0.11 ──── 0.12
                                          |
                          0.10 ───────────+
                                          |
                                         0.13 ──── 0.14
```

**Entry points (no incoming edges):** 0.1, 0.8, 0.10

**Merge points:**
- 0.6: join_mode = `all`, requires 0.3 + 0.4 + 0.5
- 0.11: join_mode = `all`, requires 0.6 + 0.7 (timer elapsed)
- 0.13: join_mode = `all`, requires 0.10 + 0.11

**Timer gate:** 0.7 has a 14-day `deadline_at` enforced by the timeout ticker.

**Terminal step:** 0.14 (GovOps Enables Prime Access). When 0.14 completes, Phase 0 is done.

## 11. Hexagonal Architecture Mapping

### Domain Layer

```
stl-verify/internal/domain/entity/nfat.go
```

Contains all domain types: `ProcessDefinition`, `ProcessDefVersion`, `StepDefinition`, `Edge`, `Book`, `StepState` (including `LockedBy`, `LockedAt`, `Attempts`, `LastError`, `DeadlineAt` fields), `AuditLogEntry`, `StepType` (`definition`, `approval`, `configuration`, `review`, `submission`), `StepStatus`, `BookStatus`, `BookState`, `JoinMode`, `BeaconResult`.

### Inbound Ports

```
stl-verify/internal/ports/inbound/nfat_service.go
```

```go
type NFATService interface {
    HandleTriggerEvent(ctx context.Context, event TriggerEvent) (entity.Book, error)
    SubmitStepAction(ctx context.Context, req StepActionRequest) error
    AdvanceInstance(ctx context.Context, bookID uuid.UUID) error
    HandleTimerExpiry(ctx context.Context, stepStateID uuid.UUID) error

    // Definition management
    CreateDefinition(ctx context.Context, req CreateDefinitionRequest) (entity.ProcessDefinition, error)
    CreateVersion(ctx context.Context, req CreateVersionRequest) (entity.ProcessDefVersion, error)
    ActivateVersion(ctx context.Context, versionID uuid.UUID) error
    GetDefinition(ctx context.Context, slug string) (entity.ProcessDefinition, error)
    ListPendingActions(ctx context.Context, actor string) ([]entity.StepState, error)
}
```

### Outbound Ports

```
stl-verify/internal/ports/outbound/nfat_repository.go
```

```go
type NFATBookRepository interface {
    CreateBook(ctx context.Context, tx Tx, book entity.Book) error
    GetBookForUpdate(ctx context.Context, tx Tx, id uuid.UUID) (entity.Book, error)
    UpdateBook(ctx context.Context, tx Tx, book entity.Book) error
}

type NFATStepStateRepository interface {
    CreateStepState(ctx context.Context, tx Tx, state entity.StepState) error
    GetStepStatesForBook(ctx context.Context, tx Tx, bookID uuid.UUID) ([]entity.StepState, error)
    TryLockStep(ctx context.Context, tx Tx, stepStateID uuid.UUID, workerID string) (bool, error)
    UpdateStepState(ctx context.Context, tx Tx, state entity.StepState) error
    UnlockStep(ctx context.Context, tx Tx, stepStateID uuid.UUID) error
    GetExpiredTimers(ctx context.Context) ([]entity.StepState, error)
    GetStaleLockedSteps(ctx context.Context, staleDuration time.Duration) ([]entity.StepState, error)
}

type NFATDefinitionRepository interface {
    CreateDefinition(ctx context.Context, def entity.ProcessDefinition) error
    GetDefinitionBySlug(ctx context.Context, slug string) (entity.ProcessDefinition, error)
    CreateVersion(ctx context.Context, version entity.ProcessDefVersion) error
    GetVersion(ctx context.Context, id uuid.UUID) (entity.ProcessDefVersion, error)
    GetActiveVersion(ctx context.Context, definitionID uuid.UUID) (entity.ProcessDefVersion, error)
    ActivateVersion(ctx context.Context, tx Tx, definitionID uuid.UUID, versionID uuid.UUID) error
}

type NFATAuditLogger interface {
    Log(ctx context.Context, tx Tx, entry entity.AuditLogEntry) error
    GetAuditLog(ctx context.Context, bookID uuid.UUID) ([]entity.AuditLogEntry, error)
}

type BeaconWriter interface {
    Write(ctx context.Context, beacon string, payload map[string]any) (entity.BeaconResult, error)
}
```

### Service Layer

```
stl-verify/internal/services/nfat/engine.go
```

Core `AdvanceInstance` loop implementation. Depends on all outbound ports, the `CheckHandlerRegistry`, and the `BeaconHandlerRegistry`.

```
stl-verify/internal/services/nfat/registry.go
```

`CheckHandlerRegistry` and `BeaconHandlerRegistry` -- maps of handler names to implementations, populated at startup.

```go
type CheckHandler interface {
    Execute(ctx context.Context, stepConfig json.RawMessage, contextData map[string]any) (map[string]any, error)
}

type BeaconHandler interface {
    Execute(ctx context.Context, beacon string, payload map[string]any) (BeaconResult, error)
}
```

```
stl-verify/internal/services/nfat/transition_evaluator.go
```

Pure function that evaluates edge conditions against step result payloads and join_mode satisfaction. Testable in isolation.

```
stl-verify/internal/services/nfat/dag_validator.go
```

Validates DAG structure on version creation: cycle detection, edge reference validation, entry point existence, join_mode consistency.

### Inbound Adapters

```
stl-verify/internal/adapters/inbound/http/nfat_handler.go
```

REST endpoints for definition management, step action submission (approvals, reviews, definitions, submissions, configurations), and pending action listing by actor.

```
stl-verify/internal/adapters/inbound/sqs/nfat_consumer.go
```

SQS consumer that listens for trigger events (e.g., `FacilitySetupRequested`) and calls `HandleTriggerEvent`.

### Outbound Adapters

```
stl-verify/internal/adapters/outbound/postgres/nfat_book_repository.go
stl-verify/internal/adapters/outbound/postgres/nfat_step_state_repository.go
stl-verify/internal/adapters/outbound/postgres/nfat_definition_repository.go
stl-verify/internal/adapters/outbound/postgres/nfat_audit_logger.go
```

PostgreSQL implementations of all outbound repository ports. Use `pgx` for queries and row-level locking.

```
stl-verify/internal/adapters/outbound/beacons/council_beacon_handler.go
stl-verify/internal/adapters/outbound/beacons/lpha_nfat_beacon_handler.go
stl-verify/internal/adapters/outbound/beacons/lpha_attest_beacon_handler.go
```

Beacon handler implementations. Each wraps the appropriate client library and implements the `BeaconHandler` interface.

### Entry Point

```
stl-verify/cmd/nfat/main.go
```

Wires up all dependencies: database connection pool, repository implementations, check handler registry, beacon handler registry, engine service, HTTP server, SQS consumer, and timeout ticker goroutine.

## 12. Event Integration

### Inbound Events

The SQS consumer subscribes to a queue fed by an SNS FIFO topic. It deserializes incoming messages and routes them by event type:

- **`FacilitySetupRequested`**: Calls `HandleTriggerEvent`, which looks up the active Phase 0 definition, creates a Book pinned to that version with `book_state = CREATED`, and calls `AdvanceInstance` to activate the three entry points (0.1, 0.8, 0.10).

The consumer uses the SQS message deduplication ID as the `dedup_key` to prevent duplicate Book creation.

### Outbound Events

When an instance reaches a terminal state or significant milestones occur, the engine publishes via the `EventSink` outbound port (SNS FIFO):

| Event | Published When |
|-------|----------------|
| `BookCreated` | New Book instance created, Phase 0 started |
| `StepCompleted` | Any step completes (includes step_id, actor, beacon results) |
| `RiskFrameworkApproved` | Step 0.6 completes (Council approves risk framework) |
| `FacilityRegistered` | Step 0.11 completes (on-chain contracts deployed) |
| `Phase0Completed` | Step 0.14 completes, all steps passed |
| `Phase0Failed` | Instance fails due to a rejected approval or failed step |

Events include the `book_id`, `def_version_id`, `trigger_event_type`, `trigger_event_payload`, and relevant `result_payload` data (contract addresses, beacon confirmations) for downstream correlation.

## 13. File Structure

```
stl-verify/
  cmd/
    nfat/
      main.go                          # Entry point: wiring, HTTP server, SQS consumer, ticker
      .env                             # Local development config
  internal/
    domain/
      entity/
        nfat.go                        # All NFAT domain types (Book, StepState, etc.)
    ports/
      inbound/
        nfat_service.go                # NFATService interface
      outbound/
        nfat_repository.go             # Repository + audit logger + beacon writer interfaces
    services/
      nfat/
        engine.go                      # AdvanceInstance core loop
        engine_test.go                 # Unit tests (mocked repositories)
        registry.go                    # CheckHandlerRegistry + BeaconHandlerRegistry
        registry_test.go
        transition_evaluator.go        # Edge condition + join_mode evaluation
        transition_evaluator_test.go
        dag_validator.go               # DAG structure validation
        dag_validator_test.go
    adapters/
      inbound/
        http/
          nfat_handler.go              # REST API for step actions + definitions
          nfat_handler_test.go
        sqs/
          nfat_consumer.go             # SQS trigger event consumer
          nfat_consumer_test.go
      outbound/
        postgres/
          nfat_book_repository.go
          nfat_book_repository_test.go
          nfat_step_state_repository.go
          nfat_step_state_repository_test.go
          nfat_definition_repository.go
          nfat_definition_repository_test.go
          nfat_audit_logger.go
          nfat_audit_logger_test.go
        beacons/
          council_beacon_handler.go    # Council Beacon -> Synome Axis
          lpha_nfat_beacon_handler.go  # lpha-nfat -> On-chain + Synome (NFAT)
          lpha_attest_beacon_handler.go # lpha-attest -> Attestation writes
          beacon_handler_test.go
  db/
    migrations/
      NNNN_create_nfat_tables.up.sql
      NNNN_create_nfat_tables.down.sql
```

## 14. Pros and Cons

### Pros

- **Single source of truth.** PostgreSQL holds definitions, Book state, step states, and audit history. No state synchronization between systems.
- **Full control over execution and auditing.** Every transition, lock acquisition, retry, beacon write, and timer expiry is implemented explicitly and logged atomically.
- **Simplest deployment.** No external orchestrator to provision, monitor, version, or secure. One binary, one database, plus the beacon layer that is required regardless of architecture choice.
- **Atomic transactions.** Step transitions, new step creation, and audit log entries all commit in a single database transaction. No possibility of partial state updates (with the documented caveat for beacon writes).
- **Timer gates are natural.** The 14-day cBEAM timelock is modeled as a `deadline_at` column on the step state. The timeout ticker detects expiry and advances the instance. No need for external timer infrastructure.
- **Fits existing architecture patterns.** Follows the same hexagonal architecture, port/adapter conventions, and PostgreSQL-centric approach used throughout the codebase.
- **No vendor lock-in.** The engine is plain Go code with SQL queries. It can be tested, debugged, and profiled with standard tools.
- **Beacon integration is clean.** Beacon handlers implement a simple interface and are registered at startup, matching the existing `CheckHandler` pattern.

### Cons

- **You own the engine.** Retry logic, crash recovery, concurrency control, DAG evaluation, and timer management are all custom code that must be written, tested, and maintained. Bugs in the engine affect all facility lifecycle processes.
- **Polling-based timers.** The 30-second ticker introduces up to 30 seconds of latency for timer detection. This is acceptable for the 14-day cBEAM timelock and other multi-day workflows, but is architecturally less elegant than event-driven timers.
- **Beacon write atomicity gap.** Beacon writes (on-chain, Synome) cannot participate in the PostgreSQL transaction. If a beacon write succeeds but the transaction fails, the write cannot be rolled back. Idempotency keys and reconciliation logic are needed to handle this.
- **No built-in workflow visualization.** There is no "workflow dashboard" out of the box. Building a UI to visualize Book progress through the Phase 0 DAG requires additional frontend work, querying step states and rendering them against the definition graph.
- **Horizontal scaling requires careful locking.** While the row-level locking strategy is sound, it introduces contention under high concurrency for the same Book. This is unlikely to be a practical issue (facility setup processes are low-throughput, typically single-digit per day), but it is a consideration.
- **Complex DAG patterns require engine evolution.** Loops, sub-processes, dynamic step insertion, and other advanced workflow patterns are not supported in the initial design. Each new pattern requires engine changes. Later NFAT phases may introduce such requirements.
- **Operational burden.** Monitoring engine health, detecting stuck Books, and debugging execution flows require custom tooling and alerting. An external orchestrator typically provides these out of the box.

## 15. See Also

- `docs/architecture/nfat-architecture-hybrid-temporal.md` -- Temporal-based hybrid alternative where Temporal manages orchestration and PostgreSQL holds definitions and audit history.
- `docs/architecture/nfat-architecture-adr.md` -- Architecture Decision Record comparing the Custom Engine and Hybrid Temporal approaches, including evaluation criteria and the final decision.
