# NFAT Service Architecture: Hybrid (Temporal + PostgreSQL)

## 1. Context and Problem Statement

NFAT (Non-Fungible Asset Token) is a compliance verification service for the lifecycle
management of structured finance facilities in DeFi. When a new facility is onboarded, NFAT
orchestrates a multi-phase compliance process involving multiple actors, governance gates,
on-chain contract deployments, time-locked operations, and cross-system data writes.

**Phase 0** is the one-time facility setup phase. It consists of 14 steps organized as a
directed acyclic graph (DAG) with three independent parallel paths that merge at key
synchronization points. The steps involve six distinct actor groups (Halo Contributor Team,
Vector, Risk Team, Core Council, GovOps, Star Agent, Halo) and five step types (Definition,
Approval, Configuration, Review, Submission).

Phases 1 through 5 cover per-deal operations (origination, verification, settlement, etc.)
and are not yet defined. The architecture must support adding these phases as new process
definitions without code changes.

Key requirements:

- **Data-driven process definitions.** Process definitions (DAGs of steps, edges, actors,
  and step types) are stored as versioned data. New phases and modifications to existing
  phases do not require code deployments.
- **DAG-shaped execution.** Processes support parallel branches (e.g., attestor onboarding
  runs concurrently with risk framework approval), merge points (e.g., facility registration
  waits for both risk framework and cBEAM timelock), and hard time dependencies (14-day
  BEAMTimeLock).
- **Multi-actor human gates.** Different steps are owned by different actors (Core Council
  approvals, GovOps configurations, Star Agent reviews). The system must route work to the
  correct actor and wait indefinitely for their action.
- **Beacon integration.** Steps interact with the beacon layer (lpha-nfat, Council Beacon,
  lpha-attest) to perform on-chain transactions and writes to Synome databases (NFAT and
  Axis).
- **Full auditability.** Every state transition, approval, submission, beacon invocation, and
  timer event must be recorded in an immutable audit log (compliance context).
- **Operational simplicity.** The system already has Temporal deployed in Kubernetes
  (`temporal-worker`), so adopting it for NFAT avoids introducing a new orchestration layer.

The hybrid approach stores mutable process definitions, facility records, compliance
documents, and runtime state in PostgreSQL (Synome NFAT and Synome Axis) while delegating
durable execution, timers, signal-based human gates, and retry logic to Temporal.

---

## 2. Architecture Overview

```
                  +-------------------+
                  |    HTTP API       |
                  |   (nfat-api)      |
                  +---------+---------+
                            |
           +----------------+------------------+
           |                |                  |
   Definition CRUD   Approval/Review     Submission
   (read/write PG)   (SignalWorkflow)    (SignalWorkflow)
           |                |                  |
           v                v                  v
   +-------+--------+   +--+-------------------+--+
   | Synome (NFAT)  |   |    Temporal Server       |
   | PostgreSQL     |<->|                          |
   |                |   |  NFATInterpreterWorkflow |
   | - facilities   |   |  (generic DAG interpreter|
   | - books        |   |   for any phase)         |
   | - compliance   |   +----------+---------------+
   | - attestors    |              |
   | - step_states  |   +----------+---------------+
   | - audit_log    |   |    Temporal Worker        |
   +-------+--------+   |   (nfat-worker)           |
           ^             +----------+---------------+
           |                        |
   +-------+--------+     Activities call:
   | SQS Consumer   |     - LoadDefinitionSnapshot
   | (nfat-worker)  |     - NFATBeaconActivity
   +-------+--------+     - CouncilBeaconActivity
           ^               - AttestBeaconActivity
           |               - RecordStepResult
   FacilityRequested       - FinalizeRecord
   (from SQS)              - WaitForSignal (human gates)
           |                        |
           v                        v
   +-------+--------+   +----------+---------------+
   | Synome Axis    |   |     Beacon Layer          |
   | PostgreSQL     |   |                           |
   |                |   | lpha-nfat  --> Synome NFAT |
   | - risk         |   |               + on-chain  |
   |   frameworks   |   | Council    --> Synome Axis |
   +----------------+   |   Beacon      + on-chain  |
                        | lpha-attest--> attestation |
           +            |               + on-chain  |
           |            +--+-----------+------------+
           v               |           |
   +-------+--------+     v           v
   |  SNS FIFO      |  On-chain    On-chain
   |  (event bus)   |  contracts   contracts
   +----------------+  (PAU,       (attestor
     publishes:         queue,      whitelist)
     FacilityCreated,   redeem)
     RiskFrameworkApproved,
     BookCreated,
     PrimeAccessEnabled
```

**Flow summary:**

1. An SQS consumer receives a `FacilityRequested` event and calls
   `nfatService.StartFacilitySetup()`.
2. The service creates a `Facility` row, an `NFATRecord` pinned to the currently published
   Phase 0 definition version, and starts a Temporal workflow (`NFATInterpreterWorkflow`).
3. The generic workflow loads the immutable definition snapshot (all 14 steps and edges),
   builds an in-memory DAG, and identifies entry points (steps 0.1, 0.8, 0.10).
4. The workflow traverses the DAG, launching parallel branches. For each step, it dispatches
   to the appropriate execution strategy based on step type (Definition, Approval,
   Configuration, Review, Submission).
5. For human-gated steps (Approval, Review, Submission), the workflow blocks on a Temporal
   signal channel. The HTTP API receives the human action and calls
   `temporalClient.SignalWorkflow()`.
6. For timer gates (step 0.7, 14-day BEAMTimeLock), the workflow blocks on
   `workflow.NewTimer()`.
7. For beacon-mediated steps (Configuration, some Approvals), activities invoke the
   appropriate beacon (lpha-nfat, Council Beacon, lpha-attest) to perform on-chain
   transactions and Synome writes.
8. On completion of all 14 steps, the `FinalizeRecord` activity writes the terminal status,
   transitions the Book to its final Phase 0 state, and publishes a `PrimeAccessEnabled`
   event to SNS FIFO.

---

## 3. Domain Model

All entities reside in `stl-verify/internal/domain/entity/nfat.go`.

### Facility

The top-level entity representing a structured finance facility.

| Field            | Type      | Description                                       |
|------------------|-----------|---------------------------------------------------|
| ID               | uuid      | Primary key                                       |
| Name             | string    | Facility display name                             |
| ChainID          | int64     | Blockchain network identifier                     |
| Status           | string    | setup, active, suspended, closed                  |
| CreatedAt        | time.Time |                                                   |
| UpdatedAt        | time.Time |                                                   |

### Book

Central entity with state machine. Each facility has one Book, created during Phase 0.

| Field            | Type      | Description                                       |
|------------------|-----------|---------------------------------------------------|
| ID               | uuid      | Primary key                                       |
| FacilityID       | uuid      | FK to Facility                                    |
| State            | BookState | CREATED (end of Phase 0), further states in later phases |
| TemplateData     | JSONB     | Book template data written at creation            |
| CreatedAt        | time.Time |                                                   |
| UpdatedAt        | time.Time |                                                   |

`BookState` enum:

```go
type BookState string

const (
    BookStateCreated BookState = "CREATED"
    // Future phases will add: ACTIVE, SUSPENDED, CLOSED, etc.
)
```

### BuyBox

Parameters defining asset eligibility criteria for a facility.

| Field            | Type            | Description                               |
|------------------|-----------------|-------------------------------------------|
| ID               | uuid            | Primary key                               |
| FacilityID       | uuid            | FK to Facility                            |
| Parameters       | json.RawMessage | Buy box parameter definitions (JSONB)     |
| DefinedBy        | Actor           | HaloContributorTeam                       |
| CreatedAt        | time.Time       |                                           |

### ProcessDefinition

Mutable template. Represents a named compliance process (e.g., "Phase 0: Facility Setup").

| Field              | Type      | Description                                      |
|--------------------|-----------|--------------------------------------------------|
| ID                 | uuid      | Primary key                                      |
| Slug               | string    | URL-safe unique identifier (e.g., "phase-0")     |
| Name               | string    | Human-readable name                              |
| Description        | string    | Optional long description                        |
| IsActive           | bool      | Soft-disable toggle                              |
| CurrentVersionID   | *uuid     | Points to the currently published version        |
| CreatedAt          | time.Time |                                                  |
| UpdatedAt          | time.Time |                                                  |

### ProcessDefVersion

Immutable snapshot of a process definition at a point in time.

| Field          | Type              | Description                                      |
|----------------|-------------------|--------------------------------------------------|
| ID             | uuid              | Primary key                                      |
| DefinitionID   | uuid              | FK to ProcessDefinition                          |
| VersionNumber  | int               | Monotonically increasing per definition          |
| Steps          | []StepDefinition  | Stored as JSONB                                  |
| DAGEdges       | []Edge            | Stored as JSONB                                  |
| Status         | VersionStatus     | draft, published, archived                       |
| CreatedAt      | time.Time         |                                                  |
| PublishedAt    | *time.Time        | Set when status transitions to published         |

### StepDefinition

A node in the process DAG. Each step has a type that determines its execution strategy and
an actor who is responsible for performing the step.

| Field              | Type              | Description                                      |
|--------------------|-------------------|--------------------------------------------------|
| StepID             | string            | Unique within the version (e.g., "0.1", "0.14")  |
| Name               | string            | Display name (e.g., "Halo Defines Buy Box Parameters") |
| StepType           | StepType          | definition, approval, configuration, review, submission |
| Actor              | Actor             | Which actor group owns this step                 |
| Beacon             | *BeaconType       | Which beacon to invoke, if any                   |
| Config             | json.RawMessage   | Type-specific configuration (JSONB)              |
| TimerDuration      | *time.Duration    | Hard time dependency (e.g., 14-day BEAMTimeLock) |
| TimeoutDuration    | *time.Duration    | Max wait before escalation                       |
| EscalationPolicy   | *EscalationPolicy | What to do on timeout                            |

`StepType` enum:

```go
type StepType string

const (
    StepTypeDefinition     StepType = "definition"
    StepTypeApproval       StepType = "approval"
    StepTypeConfiguration  StepType = "configuration"
    StepTypeReview         StepType = "review"
    StepTypeSubmission     StepType = "submission"
)
```

`Actor` enum:

```go
type Actor string

const (
    ActorHaloContributorTeam Actor = "halo_contributor_team"
    ActorVector              Actor = "vector"
    ActorRiskTeam            Actor = "risk_team"
    ActorCoreCouncil         Actor = "core_council"
    ActorGovOps              Actor = "govops"
    ActorStarAgent           Actor = "star_agent"
    ActorHalo                Actor = "halo"
)
```

`BeaconType` enum:

```go
type BeaconType string

const (
    BeaconNFAT    BeaconType = "lpha_nfat"     // writes to Synome NFAT + on-chain
    BeaconCouncil BeaconType = "council"        // writes to Synome Axis + on-chain
    BeaconAttest  BeaconType = "lpha_attest"    // writes attestations + on-chain
)
```

### Edge

Directed transition between steps in the DAG.

| Field       | Type    | Description                                           |
|-------------|---------|-------------------------------------------------------|
| FromStepID  | string  | Source step (empty string for entry edges)             |
| ToStepID    | string  | Target step                                           |
| Condition   | *string | Optional expression evaluated against the source step result |

### NFATRecord

Runtime instance of a compliance process for a specific facility.

| Field                | Type            | Description                                  |
|----------------------|-----------------|----------------------------------------------|
| ID                   | uuid            | Primary key                                  |
| FacilityID           | uuid            | FK to Facility                               |
| DefVersionID         | uuid            | FK to ProcessDefVersion (pinned)             |
| Status               | RecordStatus    | pending, running, completed, failed, cancelled |
| TemporalWorkflowID  | string          | Temporal workflow ID                         |
| TemporalRunID        | string          | Temporal run ID                              |
| CreatedAt            | time.Time       |                                              |
| CompletedAt          | *time.Time      |                                              |

### StepState

Per-step execution state within a record.

| Field         | Type            | Description                                      |
|---------------|-----------------|--------------------------------------------------|
| ID            | uuid            | Primary key                                      |
| RecordID      | uuid            | FK to NFATRecord                                 |
| StepID        | string          | References StepDefinition.StepID                 |
| Status        | StepStatus      | pending, waiting_signal, waiting_timer, running, completed, failed, skipped, timed_out |
| ResultPayload | json.RawMessage | Output from the step (approval details, config results, etc.) |
| Actor         | Actor           | Actor responsible for the step                   |
| CompletedBy   | *string         | Identity of who completed the step               |
| StartedAt     | *time.Time      |                                                  |
| CompletedAt   | *time.Time      |                                                  |
| DeadlineAt    | *time.Time      | Computed from TimeoutDuration at step start      |
| TimerEndsAt   | *time.Time      | When a hard timer gate expires (e.g., cBEAM lock)|

### ComplianceFile

Documents submitted by external parties during Submission steps.

| Field         | Type            | Description                                      |
|---------------|-----------------|--------------------------------------------------|
| ID            | uuid            | Primary key                                      |
| FacilityID    | uuid            | FK to Facility                                   |
| RecordID      | uuid            | FK to NFATRecord                                 |
| StepID        | string          | References StepDefinition.StepID (e.g., "0.10")  |
| FileType      | string          | Document category (e.g., "risk_input", "compliance_cert") |
| S3Key         | string          | S3 object key for the stored document            |
| Metadata      | json.RawMessage | Additional file metadata                         |
| SubmittedBy   | string          | Identity of submitter                            |
| CreatedAt     | time.Time       |                                                  |

### AttestorRecord

Records for attestor companies identified and whitelisted during Phase 0.

| Field            | Type            | Description                                   |
|------------------|-----------------|-----------------------------------------------|
| ID               | uuid            | Primary key                                   |
| FacilityID       | uuid            | FK to Facility                                |
| CompanyName      | string          | Attestor company name                         |
| OnChainAddress   | string          | Whitelisted on-chain address                  |
| WhitelistedAt    | *time.Time      | When whitelisting was confirmed on-chain      |
| SynomeRecordID   | *string         | ID of the record in Synome NFAT               |
| Status           | string          | identified, whitelisted, revoked              |
| CreatedAt        | time.Time       |                                               |

### RiskFramework

Risk framework approved by Core Council, stored in Synome Axis.

| Field            | Type            | Description                                   |
|------------------|-----------------|-----------------------------------------------|
| ID               | uuid            | Primary key                                   |
| FacilityID       | uuid            | FK to Facility                                |
| FinancialParams  | json.RawMessage | Financial risk parameters (from step 0.3)     |
| NonFinancialParams | json.RawMessage | Non-financial risk parameters (from step 0.4) |
| EcosystemParams  | json.RawMessage | Ecosystem risk parameters (from step 0.5)     |
| ApprovedAt       | *time.Time      | When Core Council approved                    |
| SynomeAxisID     | *string         | ID of the record in Synome Axis               |
| CreatedAt        | time.Time       |                                               |

### AuditLogEntry

Immutable append-only record of every state transition.

| Field      | Type            | Description                                      |
|------------|-----------------|--------------------------------------------------|
| ID         | uuid            | Primary key                                      |
| RecordID   | uuid            | FK to NFATRecord                                 |
| FacilityID | uuid            | FK to Facility (denormalized for query efficiency)|
| StepID     | *string         | Null for record-level events                     |
| EventType  | string          | e.g., "step_started", "approval_submitted", "beacon_invoked", "timer_started", "timer_elapsed" |
| Actor      | string          | Actor identity or "system"                       |
| BeaconType | *string         | Which beacon was invoked, if applicable          |
| Detail     | json.RawMessage | Arbitrary context (on-chain tx hash, etc.)       |
| CreatedAt  | time.Time       |                                                  |

---

## 4. Database Schema

```sql
-- ============================================================
-- Synome (NFAT) Database
-- ============================================================

-- Facilities
CREATE TABLE nfat_facilities (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    chain_id        BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'setup'
                    CHECK (status IN ('setup', 'active', 'suspended', 'closed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Books (one per facility, created during Phase 0 step 0.11)
CREATE TABLE nfat_books (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id     UUID NOT NULL REFERENCES nfat_facilities(id),
    state           TEXT NOT NULL DEFAULT 'CREATED'
                    CHECK (state IN ('CREATED')),  -- extended in later phases
    template_data   JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (facility_id)
);

-- Buy box parameters (one per facility, created during Phase 0 step 0.1)
CREATE TABLE nfat_buy_boxes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id     UUID NOT NULL REFERENCES nfat_facilities(id),
    parameters      JSONB NOT NULL,
    defined_by      TEXT NOT NULL DEFAULT 'halo_contributor_team',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (facility_id)
);

-- Process definitions (mutable template)
CREATE TABLE nfat_process_definitions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    is_active       BOOLEAN NOT NULL DEFAULT true,
    current_version_id UUID,  -- FK added after versions table exists
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nfat_process_definitions_slug ON nfat_process_definitions (slug);

-- Process definition versions (immutable snapshots)
CREATE TABLE nfat_process_def_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    definition_id   UUID NOT NULL REFERENCES nfat_process_definitions(id),
    version_number  INTEGER NOT NULL,
    steps           JSONB NOT NULL,       -- []StepDefinition
    dag_edges       JSONB NOT NULL,       -- []Edge
    status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'published', 'archived')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,

    UNIQUE (definition_id, version_number)
);

CREATE INDEX idx_nfat_def_versions_definition ON nfat_process_def_versions (definition_id);
CREATE INDEX idx_nfat_def_versions_status ON nfat_process_def_versions (definition_id, status);

-- Add FK from definitions to versions (deferred to avoid circular dependency)
ALTER TABLE nfat_process_definitions
    ADD CONSTRAINT fk_current_version
    FOREIGN KEY (current_version_id) REFERENCES nfat_process_def_versions(id);

-- Runtime records (one per facility per phase)
CREATE TABLE nfat_records (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id             UUID NOT NULL REFERENCES nfat_facilities(id),
    def_version_id          UUID NOT NULL REFERENCES nfat_process_def_versions(id),
    status                  TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    temporal_workflow_id    TEXT NOT NULL,
    temporal_run_id         TEXT NOT NULL DEFAULT '',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at            TIMESTAMPTZ
);

CREATE INDEX idx_nfat_records_status ON nfat_records (status);
CREATE INDEX idx_nfat_records_facility ON nfat_records (facility_id);
CREATE INDEX idx_nfat_records_def_version ON nfat_records (def_version_id);
CREATE INDEX idx_nfat_records_temporal ON nfat_records (temporal_workflow_id);

-- Per-step execution state
CREATE TABLE nfat_step_states (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id       UUID NOT NULL REFERENCES nfat_records(id),
    step_id         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'waiting_signal', 'waiting_timer',
                                      'running', 'completed', 'failed', 'skipped', 'timed_out')),
    result_payload  JSONB NOT NULL DEFAULT '{}',
    actor           TEXT NOT NULL,
    completed_by    TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    timer_ends_at   TIMESTAMPTZ,

    UNIQUE (record_id, step_id)
);

CREATE INDEX idx_nfat_step_states_record ON nfat_step_states (record_id);
CREATE INDEX idx_nfat_step_states_status ON nfat_step_states (record_id, status);

-- Compliance files (documents submitted during Submission steps)
CREATE TABLE nfat_compliance_files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id     UUID NOT NULL REFERENCES nfat_facilities(id),
    record_id       UUID NOT NULL REFERENCES nfat_records(id),
    step_id         TEXT NOT NULL,
    file_type       TEXT NOT NULL,
    s3_key          TEXT NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    submitted_by    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nfat_compliance_files_facility ON nfat_compliance_files (facility_id);
CREATE INDEX idx_nfat_compliance_files_record ON nfat_compliance_files (record_id);

-- Attestor records
CREATE TABLE nfat_attestor_records (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id         UUID NOT NULL REFERENCES nfat_facilities(id),
    company_name        TEXT NOT NULL,
    on_chain_address    TEXT,
    whitelisted_at      TIMESTAMPTZ,
    synome_record_id    TEXT,
    status              TEXT NOT NULL DEFAULT 'identified'
                        CHECK (status IN ('identified', 'whitelisted', 'revoked')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nfat_attestor_records_facility ON nfat_attestor_records (facility_id);

-- Risk frameworks (canonical reference, stored in Synome Axis)
-- This table lives in the Synome Axis database, shown here for completeness
CREATE TABLE nfat_risk_frameworks (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id             UUID NOT NULL,
    financial_params        JSONB NOT NULL,
    non_financial_params    JSONB NOT NULL,
    ecosystem_params        JSONB NOT NULL,
    approved_at             TIMESTAMPTZ,
    synome_axis_id          TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nfat_risk_frameworks_facility ON nfat_risk_frameworks (facility_id);

-- Audit log (immutable, append-only)
-- TimescaleDB hypertable for time-series queries and retention policies
CREATE TABLE nfat_audit_log (
    id          UUID NOT NULL DEFAULT gen_random_uuid(),
    record_id   UUID NOT NULL,
    facility_id UUID NOT NULL,
    step_id     TEXT,
    event_type  TEXT NOT NULL,
    actor       TEXT NOT NULL,
    beacon_type TEXT,
    detail      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('nfat_audit_log', 'created_at');

CREATE INDEX idx_nfat_audit_log_record ON nfat_audit_log (record_id, created_at DESC);
CREATE INDEX idx_nfat_audit_log_facility ON nfat_audit_log (facility_id, created_at DESC);
CREATE INDEX idx_nfat_audit_log_event_type ON nfat_audit_log (event_type, created_at DESC);
```

Notes:

- The `nfat_audit_log` table is a TimescaleDB hypertable partitioned on `created_at`. This
  supports efficient time-range queries and automatic data retention policies. There is no
  foreign key from `nfat_audit_log.record_id` to `nfat_records.id` because hypertables on
  distributed setups do not support foreign keys to non-distributed tables.
- The `nfat_risk_frameworks` table logically belongs to the Synome Axis database. It is shown
  here for schema completeness. In production, the NFAT service writes to it via the Council
  Beacon.
- JSONB columns keep the schema flexible as step types and domain entities evolve.
- The `UNIQUE (definition_id, version_number)` constraint guarantees version ordering
  integrity within a process definition.
- The `UNIQUE (facility_id)` constraints on `nfat_books` and `nfat_buy_boxes` enforce
  one-to-one relationships with facilities.

---

## 5. Process Versioning: Pin-to-Version

The versioning model ensures that in-flight facility setups are never affected by
definition changes.

**Publishing a new version:**

1. An operator edits a process definition. Edits are saved to a `draft` version row.
2. When the operator publishes, the system:
   a. Validates the DAG (no cycles, all edges reference valid steps, at least one entry
      point, all steps have a valid actor and step type).
   b. Sets the draft version's status to `published` and records `published_at`.
   c. Archives the previously published version (old `published` -> `archived`).
   d. Updates `process_definitions.current_version_id` to point to the new version.
3. This is executed in a single database transaction.

**Pin-to-version semantics:**

- When a new `NFATRecord` is created for a facility, it stores `def_version_id` pointing to
  the currently published version at that moment.
- The `ProcessDefVersion` row is immutable once published. Its `steps` and `dag_edges` JSONB
  columns are never modified.
- In-flight facilities continue executing against their pinned version regardless of
  subsequent publishes. There is no migration of running facilities to new versions.

**Concrete example:** Suppose Core Council decides to add a new step between 0.5 and 0.6
requiring an additional ecosystem risk review. The operator creates a new version (v2) of
the Phase 0 definition with 15 steps and publishes it. Any facility already mid-setup
continues executing the original 14-step v1 definition. Only new facilities pick up v2.

**Temporal versioning is not needed.** The workflow code is a generic DAG interpreter that
reads its behavior from the database. Since the workflow logic itself does not change between
process definition versions, Temporal's `workflow.GetVersion()` and versioning machinery is
unnecessary. The workflow code is stable; only the data it interprets changes.

---

## 6. Execution Strategy: Generic Temporal Interpreter Workflow

The core design insight is that a single, stable Temporal workflow function interprets
any process definition -- Phase 0 today, Phases 1 through 5 in the future. The workflow
never contains business-specific branching logic.

### Workflow Function

```go
func NFATInterpreterWorkflow(ctx workflow.Context, input NFATWorkflowInput) (NFATWorkflowResult, error) {
    // input contains: RecordID, FacilityID, DefVersionID

    // Step 1: Load the immutable definition snapshot.
    var snapshot DefinitionSnapshot
    err := workflow.ExecuteActivity(ctx, LoadDefinitionSnapshot, input.DefVersionID).Get(ctx, &snapshot)
    if err != nil {
        return NFATWorkflowResult{}, fmt.Errorf("loading definition snapshot: %w", err)
    }

    // Step 2: Build in-memory DAG from steps and edges.
    dag := BuildDAG(snapshot.Steps, snapshot.DAGEdges)

    // Step 3: Execute the DAG via topological traversal.
    //
    // For Phase 0, this identifies three entry points (0.1, 0.8, 0.10),
    // launches them in parallel, handles merge points (0.6, 0.11, 0.13),
    // and respects the 14-day timer gate at 0.7.
    result, err := executeDAG(ctx, dag, input.RecordID, input.FacilityID)
    if err != nil {
        return NFATWorkflowResult{}, fmt.Errorf("executing DAG: %w", err)
    }

    // Step 4: Finalize the record (write terminal status, publish event).
    err = workflow.ExecuteActivity(ctx, FinalizeRecord, FinalizeInput{
        RecordID:       input.RecordID,
        FacilityID:     input.FacilityID,
        TerminalStatus: result.Status,
    }).Get(ctx, nil)
    if err != nil {
        return NFATWorkflowResult{}, fmt.Errorf("finalizing record: %w", err)
    }

    return result, nil
}
```

### DAG Execution

The `executeDAG` function performs a topological traversal:

1. **Identify entry steps** -- steps with no incoming edges. For Phase 0 these are:
   - 0.1 (Halo Defines Buy Box Parameters)
   - 0.8 (GovOps Identifies Attestor Company)
   - 0.10 (Halo Provides Risk and Compliance Inputs)
2. **Launch entry steps in parallel** via `workflow.Go()`.
3. **Each step, on completion**, evaluates outgoing edges. If an edge condition is met (or
   unconditional), the target step is notified via an internal channel.
4. **Merge points** track how many predecessors have completed. The step executes only when
   all incoming edges are satisfied:
   - Step 0.6 waits for steps 0.3, 0.4, and 0.5 (all three risk parameter categories).
   - Step 0.11 waits for step 0.7 (cBEAM timelock elapsed) and step 0.6 (risk framework
     approved in Synome Axis).
   - Step 0.13 waits for step 0.10 (compliance docs submitted) and step 0.11 (facility
     registered).
5. **Timer gates** are handled inline. When a step has a `TimerDuration` (step 0.7's 14-day
   cBEAM lock), the workflow calls `workflow.NewTimer()` after the step's primary action
   completes, then waits for the timer to fire before signaling downstream steps.
6. The traversal completes when all terminal steps (no outgoing edges) finish. For Phase 0,
   the single terminal step is 0.14 (GovOps Enables Prime Access).

### Step Dispatch

Each step is dispatched based on its `StepType`:

- `definition` -- calls a recording activity, may read from external sources.
- `approval` -- sends notification, then blocks on a signal channel for human approval.
- `configuration` -- calls a beacon activity for on-chain + Synome writes.
- `review` -- sends notification, then blocks on a signal channel for human review.
- `submission` -- blocks on a signal channel for external party document upload.

After each step completes, a `RecordStepResult` activity persists the `StepState` and
writes an `AuditLogEntry`.

---

## 7. Step Type Execution

### Definition Steps (0.1, 0.2, 0.3, 0.4, 0.5)

Definition steps record parameter definitions and model initializations. They involve an
actor providing structured data that is persisted to Synome (NFAT).

**Temporal execution:**

1. The workflow marks the step as `waiting_signal` and sends a notification to the
   responsible actor (e.g., Halo Contributor Team for 0.1, Vector for 0.2, Risk Team for
   0.3/0.4/0.5).
2. The workflow blocks on a named signal channel: `definition:{stepID}`.
3. The actor submits the definition data via the HTTP API.
4. The HTTP handler calls `temporalClient.SignalWorkflow()` with the definition payload.
5. The workflow receives the signal, executes a `RecordDefinitionActivity` that persists
   the data (e.g., buy box parameters to `nfat_buy_boxes`, risk parameters to the risk
   framework record).
6. The step is marked `completed` and downstream steps are notified.

```go
signalCh := workflow.GetSignalChannel(ctx, "definition:"+step.StepID)

var payload DefinitionPayload
signalCh.Receive(ctx, &payload)

err := workflow.ExecuteActivity(ctx, RecordDefinitionActivity, RecordDefinitionInput{
    RecordID:   recordID,
    FacilityID: facilityID,
    StepID:     step.StepID,
    Payload:    payload,
}).Get(ctx, nil)
```

### Approval Steps (0.6, 0.9, 0.13)

Approval steps are governance gates requiring explicit human sign-off. The workflow must
block for an unbounded duration without consuming resources.

**Temporal execution:**

1. The workflow marks the step as `waiting_signal` and sends a notification to the
   approving actor (Core Council for 0.6, GovOps for 0.9, Star Agent for 0.13).
2. The workflow blocks on a named signal channel: `approval:{stepID}`.
3. A `workflow.Selector` races the signal channel against an optional timeout timer.
4. The approver reviews the accumulated data and submits approval via the HTTP API.
5. The HTTP handler calls `temporalClient.SignalWorkflow()`.
6. On approval, the workflow executes the appropriate beacon activity if the step involves
   on-chain or Synome writes:
   - Step 0.6: `CouncilBeaconActivity` writes the risk framework to Synome Axis.
   - Step 0.9: `NFATBeaconActivity` + `AttestBeaconActivity` whitelist the attestor on-chain
     and in Synome NFAT.
   - Step 0.13: `NFATBeaconActivity` writes the Star Agent's administration record to
     Synome NFAT.

```go
signalCh := workflow.GetSignalChannel(ctx, "approval:"+step.StepID)

sel := workflow.NewSelector(ctx)

sel.AddReceive(signalCh, func(ch workflow.ReceiveChannel, more bool) {
    var signal ApprovalSignal
    ch.Receive(ctx, &signal)
    // Process approval or rejection
})

if step.TimeoutDuration != nil {
    timerFuture := workflow.NewTimer(ctx, *step.TimeoutDuration)
    sel.AddFuture(timerFuture, func(f workflow.Future) {
        // Handle timeout based on escalation policy
    })
}

sel.Select(ctx)
```

### Configuration Steps (0.7, 0.11, 0.12, 0.14)

Configuration steps perform on-chain transactions and system setup via beacons. These are
system-driven (not human-gated), though some require preconditions like timer expiry.

**Temporal execution:**

1. The workflow marks the step as `running`.
2. If the step has a `TimerDuration` (step 0.7: 14-day BEAMTimeLock):
   a. The `CouncilBeaconActivity` grants cBEAM to GovOps on-chain.
   b. The workflow marks the step as `waiting_timer` and calls `workflow.NewTimer(ctx,
      14 * 24 * time.Hour)`.
   c. The timer fires after 14 days. The step is marked `completed`.
3. If no timer, the workflow executes the beacon activity directly:
   - Step 0.11: `NFATBeaconActivity` deploys PAU, queue, and redeem contracts on-chain and
     creates a Book template in Synome NFAT with state `CREATED`.
   - Step 0.12: `NFATBeaconActivity` writes the prime type determination to Synome NFAT.
   - Step 0.14: `NFATBeaconActivity` enables prime access on-chain and writes the final
     configuration to Synome NFAT.

```go
// Step 0.7 example: configuration with timer gate
err := workflow.ExecuteActivity(ctx, CouncilBeaconActivity, CouncilBeaconInput{
    FacilityID: facilityID,
    Action:     "grant_cbeam",
    Payload:    step.Config,
}).Get(ctx, nil)
if err != nil {
    return fmt.Errorf("granting cBEAM: %w", err)
}

if step.TimerDuration != nil {
    // 14-day BEAMTimeLock
    _ = workflow.NewTimer(ctx, *step.TimerDuration).Get(ctx, nil)
}
```

### Review Steps (0.8)

Review steps involve an actor evaluating and selecting from options. The actor must complete
their review before the step can proceed.

**Temporal execution:**

1. The workflow marks the step as `waiting_signal` and optionally sends a notification to
   the reviewer (GovOps for 0.8).
2. The workflow blocks on a named signal channel: `review:{stepID}`.
3. The reviewer completes their evaluation (e.g., identifies an attestor company) and
   submits the result via the HTTP API.
4. The workflow receives the signal, executes a recording activity to persist the review
   outcome (e.g., creates an `nfat_attestor_records` row with status `identified`).

```go
signalCh := workflow.GetSignalChannel(ctx, "review:"+step.StepID)

var result ReviewResult
signalCh.Receive(ctx, &result)

err := workflow.ExecuteActivity(ctx, RecordReviewActivity, RecordReviewInput{
    RecordID:   recordID,
    FacilityID: facilityID,
    StepID:     step.StepID,
    Result:     result,
}).Get(ctx, nil)
```

### Submission Steps (0.10)

Submission steps wait for external parties to provide documents or data. The submitting
party uploads documents independently, and the step completes when all required documents
are received.

**Temporal execution:**

1. The workflow marks the step as `waiting_signal` and sends a notification to the
   submitting actor (Halo for 0.10).
2. The workflow blocks on a named signal channel: `submission:{stepID}`.
3. Step 0.10 requires 6 sub-documents. Each document upload triggers a signal with the
   document metadata. The workflow accumulates signals until all required documents are
   received.
4. Each document is persisted to S3 and recorded in `nfat_compliance_files`.
5. When all 6 documents are received, the step is marked `completed`.

```go
signalCh := workflow.GetSignalChannel(ctx, "submission:"+step.StepID)

requiredDocs := step.Config.RequiredDocumentCount  // 6 for step 0.10
receivedDocs := 0

for receivedDocs < requiredDocs {
    var submission SubmissionSignal
    signalCh.Receive(ctx, &submission)

    err := workflow.ExecuteActivity(ctx, RecordSubmissionActivity, RecordSubmissionInput{
        RecordID:   recordID,
        FacilityID: facilityID,
        StepID:     step.StepID,
        Document:   submission,
    }).Get(ctx, nil)
    if err != nil {
        return fmt.Errorf("recording submission: %w", err)
    }

    receivedDocs++
}
```

### Timer Gates

Timer gates are hard time dependencies embedded within Configuration steps. They use
Temporal's durable timer primitive, which survives worker restarts and server failures.

The only timer gate in Phase 0 is step 0.7's 14-day BEAMTimeLock. The timer starts after
cBEAM is granted on-chain and must elapse before step 0.11 (facility registration) can
proceed.

```go
// Durable 14-day timer -- survives worker restarts
timerDuration := 14 * 24 * time.Hour
err := workflow.NewTimer(ctx, timerDuration).Get(ctx, nil)
// Timer has elapsed; downstream steps can now proceed
```

---

## 8. Beacon Integration

Beacons are the bridge between the NFAT service and external systems (on-chain contracts
and Synome databases). Each beacon maps to a Temporal activity with its own retry policy.

### lpha-nfat Beacon -> NFATBeaconActivity

Writes to Synome (NFAT) database and performs on-chain transactions for NFAT-specific
operations.

| Step | Action                                                |
|------|-------------------------------------------------------|
| 0.9  | Whitelist attestor company on-chain + write to Synome NFAT |
| 0.10 | Write compliance file records to Synome NFAT          |
| 0.11 | Deploy PAU/queue/redeem contracts + create Book template in Synome NFAT |
| 0.12 | Write prime type determination to Synome NFAT         |
| 0.13 | Write Star Agent administration record to Synome NFAT |
| 0.14 | Enable prime access on-chain + write to Synome NFAT   |

```go
type NFATBeaconInput struct {
    FacilityID string          `json:"facilityId"`
    Action     string          `json:"action"`
    Payload    json.RawMessage `json:"payload"`
}

type NFATBeaconOutput struct {
    SynomeRecordID string          `json:"synomeRecordId,omitempty"`
    TxHash         string          `json:"txHash,omitempty"`
    Detail         json.RawMessage `json:"detail"`
}
```

### Council Beacon -> CouncilBeaconActivity

Writes to Synome Axis database (canonical risk reference) and performs on-chain governance
transactions.

| Step | Action                                                |
|------|-------------------------------------------------------|
| 0.6  | Write approved risk framework to Synome Axis          |
| 0.7  | Grant cBEAM to GovOps on-chain                        |

```go
type CouncilBeaconInput struct {
    FacilityID string          `json:"facilityId"`
    Action     string          `json:"action"`  // "write_risk_framework", "grant_cbeam"
    Payload    json.RawMessage `json:"payload"`
}

type CouncilBeaconOutput struct {
    SynomeAxisID string          `json:"synomeAxisId,omitempty"`
    TxHash       string          `json:"txHash,omitempty"`
    Detail       json.RawMessage `json:"detail"`
}
```

### lpha-attest Beacon -> AttestBeaconActivity

Writes attestation records, used for attestor whitelisting.

| Step | Action                                                |
|------|-------------------------------------------------------|
| 0.9  | Write attestor whitelist record on-chain              |

```go
type AttestBeaconInput struct {
    FacilityID     string `json:"facilityId"`
    AttestorID     string `json:"attestorId"`
    OnChainAddress string `json:"onChainAddress"`
}

type AttestBeaconOutput struct {
    TxHash string `json:"txHash"`
}
```

### Retry and Idempotency

All beacon activities must be idempotent. On-chain transactions use nonce management to
prevent duplicate submissions. Synome writes use upsert semantics keyed on facility ID and
step ID. Temporal's retry policy handles transient failures:

```go
beaconActivityOptions := workflow.ActivityOptions{
    StartToCloseTimeout: 2 * time.Minute,
    RetryPolicy: &temporal.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    30 * time.Second,
        MaximumAttempts:    5,
        NonRetryableErrorTypes: []string{"InvalidInputError", "AuthorizationError"},
    },
}
```

---

## 9. The Phase 0 DAG (Concrete Example)

The following ASCII diagram shows the complete Phase 0 facility setup process with all 14
steps, their dependencies, parallel paths, merge points, and the timer gate.

```
 RISK FRAMEWORK PATH            ATTESTOR PATH        COMPLIANCE DOCS PATH
 ==================              =============        ====================

 [0.1] Halo Defines             [0.8] GovOps         [0.10] Halo Provides
       Buy Box Params                 Identifies             Risk & Compliance
       (Definition,                   Attestor Co.           Inputs
        HaloContributorTeam)          (Review, GovOps)       (Submission, Halo)
       |                              |                      6 sub-documents
       v                              v                      |
 [0.2] Vector Initialises       [0.9] GovOps                |
       Risk Model                     Whitelists             |
       (Definition, Vector)           Attestor Co.           |
       |                              (Approval, GovOps)     |
       v                              on-chain +             |
 [0.3] Risk Team Collects            Synome (NFAT)          |
       Financial Risk Params          via lpha-nfat          |
       (Definition, RiskTeam)         + lpha-attest          |
       |                                                     |
       v                                                     |
 [0.4] Risk Team Defines                                     |
       Non-Financial Risk                                    |
       Params                                                |
       (Definition, RiskTeam)                                |
       |                                                     |
       v                                                     |
 [0.5] Risk Team Defines                                     |
       Ecosystem Risk Params                                 |
       (Definition, RiskTeam)                                |
       |                                                     |
       +--------+--------+                                   |
                |        |                                   |
       (0.3) --+--------+-- (0.4) -- (0.5)                  |
                |                                            |
                v [MERGE: all of 0.3, 0.4, 0.5]             |
 [0.6] Core Council Approves                                |
       Risk Framework                                        |
       (Approval, CoreCouncil)                               |
       Writes to Synome Axis                                 |
       via Council Beacon                                    |
       |                                                     |
       v                                                     |
 [0.7] Core Council Grants                                   |
       cBEAM to GovOps                                       |
       (Configuration,                                       |
        CoreCouncil)                                         |
       +----[ 14-DAY BEAMTIMELOCK ]----+                     |
                                       |                     |
       (0.6) -------------------------+|                     |
                                       ||                    |
                                       vv                    |
              [MERGE: 0.7 elapsed + 0.6 approved]           |
 [0.11] GovOps Registers                                     |
        NFAT Facility                                        |
        (Configuration, GovOps)                              |
        Deploys on-chain contracts                           |
        Creates Book (CREATED)                               |
        in Synome (NFAT)                                     |
        via lpha-nfat                                        |
        |                                                    |
        +--+                                                 |
           |                                                 |
           v                                                 |
 [0.12] GovOps Determines          (0.10) -----------------+|
        Prime Type                                          ||
        (Configuration, GovOps)                             ||
        Writes to Synome (NFAT)                             ||
                                                            vv
                               [MERGE: 0.10 + 0.11]
                    [0.13] Star Agent Administers
                           Halo in Atlas
                           (Approval, StarAgent)
                           Reviews compliance docs
                           Writes to Synome (NFAT)
                           via lpha-nfat
                           |
                           v
                    [0.14] GovOps Enables
                           Prime Access
                           (Configuration, GovOps)
                           On-chain + Synome (NFAT)
                           via lpha-nfat
                           |
                           v
                      [ PHASE 0 COMPLETE ]
```

**Step summary table:**

| Step | Name                                       | Type          | Actor                | Depends On         | Beacon        | Notes                     |
|------|--------------------------------------------|---------------|----------------------|--------------------|---------------|---------------------------|
| 0.1  | Halo Defines Buy Box Parameters            | Definition    | HaloContributorTeam  | (entry point)      | --            |                           |
| 0.2  | Vector Initialises Risk Model              | Definition    | Vector               | 0.1                | --            |                           |
| 0.3  | Risk Team Collects Financial Risk Params   | Definition    | RiskTeam             | 0.2                | --            |                           |
| 0.4  | Risk Team Defines Non-Financial Risk Params| Definition    | RiskTeam             | 0.3                | --            |                           |
| 0.5  | Risk Team Defines Ecosystem Risk Params    | Definition    | RiskTeam             | 0.4                | --            |                           |
| 0.6  | Core Council Approves Risk Framework       | Approval      | CoreCouncil          | 0.3, 0.4, 0.5     | Council       | Writes to Synome Axis     |
| 0.7  | Core Council Grants cBEAM to GovOps       | Configuration | CoreCouncil          | 0.6                | Council       | 14-day BEAMTimeLock       |
| 0.8  | GovOps Identifies Attestor Company         | Review        | GovOps               | (entry point)      | --            |                           |
| 0.9  | GovOps Whitelists Attestor Company         | Approval      | GovOps               | 0.8                | lpha-nfat, lpha-attest | On-chain + Synome NFAT |
| 0.10 | Halo Provides Risk and Compliance Inputs   | Submission    | Halo                 | (entry point)      | lpha-nfat     | 6 sub-documents           |
| 0.11 | GovOps Registers NFAT Facility             | Configuration | GovOps               | 0.7, 0.6           | lpha-nfat     | Deploys contracts, creates Book |
| 0.12 | GovOps Determines Prime Type               | Configuration | GovOps               | 0.11               | lpha-nfat     |                           |
| 0.13 | Star Agent Administers Halo in Atlas       | Approval      | StarAgent            | 0.10, 0.11         | lpha-nfat     | Reviews compliance docs   |
| 0.14 | GovOps Enables Prime Access                | Configuration | GovOps               | 0.13               | lpha-nfat     | Phase 0 complete          |

**Parallel paths:**

1. **Risk framework path:** 0.1 -> 0.2 -> 0.3 -> 0.4 -> 0.5 -> 0.6 -> 0.7
   (sequential chain through buy box definition, risk model, three risk parameter
   categories, council approval, and cBEAM grant with timelock)
2. **Attestor path:** 0.8 -> 0.9
   (independent; GovOps identifies and whitelists an attestor company)
3. **Compliance docs path:** 0.10
   (independent; Halo provides 6 sub-documents)

These paths merge at:
- **Step 0.6:** Waits for 0.3 + 0.4 + 0.5 (all three risk parameter definitions).
- **Step 0.11:** Waits for 0.7 (cBEAM timelock elapsed) + 0.6 (risk framework in Synome Axis).
- **Step 0.13:** Waits for 0.10 (compliance docs submitted) + 0.11 (facility registered).

---

## 10. Hexagonal Architecture Mapping

The NFAT service follows the same hexagonal architecture as the rest of the codebase.

### Domain Layer

| File | Contents |
|------|----------|
| `internal/domain/entity/nfat.go` | All entities: `Facility`, `Book`, `BookState`, `BuyBox`, `ProcessDefinition`, `ProcessDefVersion`, `StepDefinition`, `StepType`, `Actor`, `BeaconType`, `Edge`, `NFATRecord`, `StepState`, `ComplianceFile`, `AttestorRecord`, `RiskFramework`, `AuditLogEntry`, enums |
| `internal/domain/entity/nfat_dag.go` | `BuildDAG()`, `ValidateDAG()`, `FindEntryPoints()`, `FindMergePoints()` |

### Inbound Ports

| File | Interface |
|------|-----------|
| `internal/ports/inbound/nfat_service.go` | `NFATService` -- `StartFacilitySetup()`, `SubmitDefinition()`, `SubmitApproval()`, `SubmitReview()`, `SubmitDocument()`, `GetFacility()`, `GetRecord()` |
| `internal/ports/inbound/nfat_definition_service.go` | `NFATDefinitionService` -- `CreateDefinition()`, `UpdateDraft()`, `PublishVersion()`, `GetDefinition()`, `ListDefinitions()` |

### Outbound Ports

| File | Interface |
|------|-----------|
| `internal/ports/outbound/nfat_repository.go` | `FacilityReader`, `FacilityWriter`, `BookReader`, `BookWriter`, `ProcessDefinitionReader`, `ProcessDefinitionWriter`, `ProcessDefVersionReader`, `ProcessDefVersionWriter`, `NFATRecordReader`, `NFATRecordWriter`, `StepStateReader`, `StepStateWriter`, `ComplianceFileWriter`, `AttestorRecordReader`, `AttestorRecordWriter`, `AuditLogger` |
| `internal/ports/outbound/nfat_beacon.go` | `NFATBeacon` -- `Execute(ctx, NFATBeaconInput) (*NFATBeaconOutput, error)` |
| `internal/ports/outbound/council_beacon.go` | `CouncilBeacon` -- `Execute(ctx, CouncilBeaconInput) (*CouncilBeaconOutput, error)` |
| `internal/ports/outbound/attest_beacon.go` | `AttestBeacon` -- `Execute(ctx, AttestBeaconInput) (*AttestBeaconOutput, error)` |

The existing `EventSink` port (`internal/ports/outbound/eventsink.go`) is reused for
publishing NFAT events to SNS FIFO.

### Service Layer

| File | Contents |
|------|----------|
| `internal/services/nfat/service.go` | Implements `NFATService` -- orchestrates facility setup, signal routing, Temporal workflow start |
| `internal/services/nfat/definition_service.go` | Implements `NFATDefinitionService` -- manages definitions, versions, publishing |
| `internal/services/nfat/dag.go` | DAG construction and traversal utilities (used by workflow and validation) |

### Inbound Adapters

| File | Contents |
|------|----------|
| `internal/adapters/inbound/temporal/nfat_workflow.go` | `NFATInterpreterWorkflow` function -- generic DAG interpreter |
| `internal/adapters/inbound/temporal/nfat_activities.go` | Activities: `LoadDefinitionSnapshot`, `RecordDefinitionActivity`, `RecordReviewActivity`, `RecordSubmissionActivity`, `NFATBeaconActivity`, `CouncilBeaconActivity`, `AttestBeaconActivity`, `RecordStepResult`, `FinalizeRecord` |
| `internal/adapters/inbound/http/nfat_handler.go` | HTTP handlers: definition CRUD, signal submission endpoints (approval, review, document upload), facility and record queries |
| `internal/adapters/inbound/sqs/nfat_consumer.go` | SQS consumer: listens for `FacilityRequested`, calls `nfatService.StartFacilitySetup()` |

### Outbound Adapters

| File | Contents |
|------|----------|
| `internal/adapters/outbound/postgres/nfat_facility.go` | PostgreSQL implementation of facility, book, buy box repositories |
| `internal/adapters/outbound/postgres/nfat_definition.go` | PostgreSQL implementation of definition/version repositories |
| `internal/adapters/outbound/postgres/nfat_record.go` | PostgreSQL implementation of record/step_state repositories |
| `internal/adapters/outbound/postgres/nfat_compliance.go` | PostgreSQL implementation of compliance file repository |
| `internal/adapters/outbound/postgres/nfat_attestor.go` | PostgreSQL implementation of attestor record repository |
| `internal/adapters/outbound/postgres/nfat_audit.go` | PostgreSQL implementation of `AuditLogger` |
| `internal/adapters/outbound/beacon/nfat_beacon.go` | `NFATBeacon` implementation (lpha-nfat HTTP client) |
| `internal/adapters/outbound/beacon/council_beacon.go` | `CouncilBeacon` implementation (Council Beacon HTTP client) |
| `internal/adapters/outbound/beacon/attest_beacon.go` | `AttestBeacon` implementation (lpha-attest HTTP client) |

### Entry Points

| File | Contents |
|------|----------|
| `cmd/nfat-worker/main.go` | Temporal worker process: registers `NFATInterpreterWorkflow` and all activities, starts SQS consumer |
| `cmd/nfat-api/main.go` | HTTP API server: definition management, signal submission endpoints, facility and record queries |

---

## 11. Event Integration

### Inbound Events

The NFAT worker runs an SQS consumer that subscribes to the `FacilityRequested` event type.

```
FacilityRequested (SQS) --> nfat_consumer.go --> nfatService.StartFacilitySetup()
                                                    |
                                                    +--> INSERT nfat_facilities
                                                    +--> INSERT nfat_records (pinned to current version)
                                                    +--> temporalClient.ExecuteWorkflow()
```

The consumer uses the existing SQS adapter patterns from the codebase. Message visibility
timeout is set high enough to cover the `StartFacilitySetup` transaction.

### Outbound Events

Activities publish events via the existing `EventSink` port (SNS FIFO) at key milestones:

| Event                       | Published When                              | Message Group ID          |
|-----------------------------|---------------------------------------------|---------------------------|
| `FacilityCreated`           | Facility record created (StartFacilitySetup)| `nfat:{facilityID}`       |
| `BuyBoxDefined`             | Step 0.1 completes                          | `nfat:{facilityID}`       |
| `RiskModelInitialised`      | Step 0.2 completes                          | `nfat:{facilityID}`       |
| `RiskFrameworkApproved`     | Step 0.6 completes (risk framework in Axis) | `nfat:{facilityID}`       |
| `CBEAMGranted`              | Step 0.7 cBEAM granted (before timer)       | `nfat:{facilityID}`       |
| `CBEAMTimelockElapsed`      | Step 0.7 14-day timer fires                 | `nfat:{facilityID}`       |
| `AttestorWhitelisted`       | Step 0.9 completes                          | `nfat:{facilityID}`       |
| `ComplianceDocsSubmitted`   | Step 0.10 all 6 documents received          | `nfat:{facilityID}`       |
| `BookCreated`               | Step 0.11 completes (Book in CREATED state) | `nfat:{facilityID}`       |
| `FacilityRegistered`        | Step 0.11 completes (contracts deployed)    | `nfat:{facilityID}`       |
| `HaloAdministered`          | Step 0.13 completes                         | `nfat:{facilityID}`       |
| `PrimeAccessEnabled`        | Step 0.14 completes (Phase 0 complete)      | `nfat:{facilityID}`       |

Using the facility ID as the SNS FIFO message group ID ensures ordered delivery of events
for a single facility while allowing parallelism across facilities.

---

## 12. File Structure

New files to create under `stl-verify/`:

```
stl-verify/
├── cmd/
│   ├── nfat-api/
│   │   └── main.go
│   └── nfat-worker/
│       └── main.go
├── internal/
│   ├── domain/
│   │   └── entity/
│   │       ├── nfat.go
│   │       └── nfat_dag.go
│   ├── ports/
│   │   ├── inbound/
│   │   │   ├── nfat_service.go
│   │   │   └── nfat_definition_service.go
│   │   └── outbound/
│   │       ├── nfat_repository.go
│   │       ├── nfat_beacon.go
│   │       ├── council_beacon.go
│   │       └── attest_beacon.go
│   ├── services/
│   │   └── nfat/
│   │       ├── service.go
│   │       ├── service_test.go
│   │       ├── definition_service.go
│   │       ├── definition_service_test.go
│   │       ├── dag.go
│   │       └── dag_test.go
│   └── adapters/
│       ├── inbound/
│       │   ├── temporal/
│       │   │   ├── nfat_workflow.go
│       │   │   ├── nfat_workflow_test.go
│       │   │   ├── nfat_activities.go
│       │   │   └── nfat_activities_test.go
│       │   ├── http/
│       │   │   ├── nfat_handler.go
│       │   │   └── nfat_handler_test.go
│       │   └── sqs/
│       │       ├── nfat_consumer.go
│       │       └── nfat_consumer_test.go
│       └── outbound/
│           ├── postgres/
│           │   ├── nfat_facility.go
│           │   ├── nfat_facility_test.go
│           │   ├── nfat_definition.go
│           │   ├── nfat_definition_test.go
│           │   ├── nfat_record.go
│           │   ├── nfat_record_test.go
│           │   ├── nfat_compliance.go
│           │   ├── nfat_compliance_test.go
│           │   ├── nfat_attestor.go
│           │   ├── nfat_attestor_test.go
│           │   ├── nfat_audit.go
│           │   └── nfat_audit_test.go
│           └── beacon/
│               ├── nfat_beacon.go
│               ├── nfat_beacon_test.go
│               ├── council_beacon.go
│               ├── council_beacon_test.go
│               ├── attest_beacon.go
│               └── attest_beacon_test.go
├── db/
│   └── migrations/
│       ├── XXXXXX_create_nfat_facilities.up.sql
│       ├── XXXXXX_create_nfat_facilities.down.sql
│       ├── XXXXXX_create_nfat_process_definitions.up.sql
│       ├── XXXXXX_create_nfat_process_definitions.down.sql
│       ├── XXXXXX_create_nfat_records.up.sql
│       ├── XXXXXX_create_nfat_records.down.sql
│       ├── XXXXXX_create_nfat_compliance_files.up.sql
│       ├── XXXXXX_create_nfat_compliance_files.down.sql
│       ├── XXXXXX_create_nfat_attestor_records.up.sql
│       ├── XXXXXX_create_nfat_attestor_records.down.sql
│       ├── XXXXXX_create_nfat_audit_log.up.sql
│       └── XXXXXX_create_nfat_audit_log.down.sql
└── k8s/
    └── base/
        ├── nfat-api/
        │   ├── deployment.yaml
        │   └── service.yaml
        └── nfat-worker/
            ├── deployment.yaml
            └── serviceaccount.yaml
```

---

## 13. Pros and Cons

### Pros

- **Durable execution out of the box.** Temporal handles workflow persistence, recovery after
  crashes, and exactly-once activity execution. The 14-day BEAMTimeLock timer at step 0.7
  survives worker restarts, server failures, and deployments without custom persistence code.
- **Zero-cost human gate waits.** Steps waiting on Core Council approval (0.6), GovOps
  attestor whitelisting (0.9), Star Agent review (0.13), or Halo document submission (0.10)
  consume no worker threads or compute resources. Only a small amount of Temporal server
  storage is used.
- **No Temporal versioning complexity.** Because the workflow is a generic DAG interpreter,
  the Go code changes infrequently. Adding new phases (1-5) or modifying Phase 0 steps
  requires only data changes (new `ProcessDefVersion` rows), not code deployments. This
  avoids the single hardest operational aspect of Temporal.
- **Built-in workflow history.** Every beacon invocation, approval signal, timer event, and
  activity execution is recorded in Temporal's event history, providing a second audit trail
  alongside the application audit log. This is valuable for compliance.
- **Natural parallel execution.** The three independent Phase 0 paths (risk framework,
  attestor, compliance docs) run concurrently via `workflow.Go()` goroutines, with Temporal
  managing scheduling and the merge points at steps 0.6, 0.11, and 0.13.
- **Task queue isolation.** NFAT workflows run on a dedicated task queue (`nfat`), isolated
  from existing Temporal workloads.
- **Temporal retry and timeout primitives.** Beacon activities (on-chain transactions) get
  battle-tested retry policies with exponential backoff. Non-retryable errors (authorization
  failures, invalid input) are handled without custom retry logic.

### Cons

- **Dual state.** Runtime state exists in both PostgreSQL (`nfat_records`, `nfat_step_states`,
  `nfat_compliance_files`) and Temporal (workflow event history). These must be kept in sync.
  If a beacon activity fails after writing to Synome but before returning to Temporal, the
  activity retry will re-execute, so all beacon activities must be idempotent.
- **Debugging spans two systems.** Investigating a stuck facility setup requires checking both
  the PostgreSQL tables and the Temporal UI/CLI. For example, determining why step 0.13 has
  not started requires checking whether both step 0.10 (compliance docs) and step 0.11
  (facility registration) have completed in both systems.
- **Temporal is a critical dependency.** If the Temporal server is down, no new facility
  setups can start, the 14-day cBEAM timer cannot progress, and no human approval signals
  can be delivered. The HTTP API for definitions remains available (PostgreSQL only), but
  operational actions are blocked.
- **DAG interpreter complexity.** The generic interpreter must handle parallel branches,
  three merge points with different predecessor sets, the timer gate at step 0.7, and
  multi-document accumulation at step 0.10. This is non-trivial code that requires thorough
  testing with the Temporal test framework.
- **Beacon idempotency is hard.** On-chain transactions (contract deployment at 0.11,
  attestor whitelisting at 0.9, prime access at 0.14) must be idempotent across retries.
  This requires nonce management, transaction receipts checking, and careful error
  classification (retryable vs. non-retryable).
- **Temporal test framework required.** Unit testing workflows with timer gates (14-day
  BEAMTimeLock), multiple signal channels (6 step types across 14 steps), and merge point
  synchronization requires `go.temporal.io/sdk/testsuite` with time skipping, which has
  its own learning curve.
- **Operational overhead.** Temporal server (with its own PostgreSQL backend) must be
  monitored, upgraded, and maintained alongside the Synome NFAT and Synome Axis databases.

---

## 14. See Also

- `docs/architecture/nfat-architecture-custom-engine.md` -- Alternative approach using a
  custom state machine engine backed solely by PostgreSQL, without Temporal dependency.
- `docs/architecture/nfat-architecture-adr.md` -- Architecture Decision Record comparing
  the hybrid Temporal approach against the custom engine approach, with the final decision
  and rationale.
