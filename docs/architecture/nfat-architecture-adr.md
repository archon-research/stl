# ADR: NFAT Service State Machine Architecture

## Status

Proposed

## Date

2026-03-26

## Context

The NFAT (Non-Fungible Asset Token) service manages the facility lifecycle for compliance verification of DeFi structured finance facilities. The immediate scope is Phase 0 -- facility setup -- which involves 14 steps, 7 distinct actors, and a mix of on-chain and off-chain actions. Phases 1 through 5 will follow with per-deal workflows that build on the same facility infrastructure.

### Phase 0: Facility Setup

Phase 0 establishes a new facility before any deals can be originated against it. The process involves:

- **14 steps** spanning 5 step types: Definition, Approval, Configuration, Review, and Submission.
- **7 actors**: Halo Contributor Team, Vector, Risk Team, Core Council, GovOps, Star Agent, and Halo.
- **DAG-shaped flow** with 3 parallel paths that branch after initial definitions and merge at key synchronization points (e.g., all parallel configuration and approval paths must complete before the final submission steps).
- **On-chain actions**: contract deployments, cBEAM grant transactions, and whitelist transactions executed through beacons.
- **Off-chain state management**: writes to Synome NFAT and Synome Axis via beacons (lpha-nfat, lpha-attest, Council Beacon).
- **Hard time dependency**: a 14-day cBEAM lock period that must elapse before subsequent steps can proceed. This is a protocol-level constraint, not a soft timeout.

Beacons are the execution layer that bridges the state machine to the outside world. Each beacon encapsulates a specific capability -- lpha-nfat handles NFAT-related Synome writes and on-chain contract operations, lpha-attest handles attestation writes, and Council Beacon handles governance-related on-chain transactions. Activities in either architecture option invoke beacons rather than performing on-chain or Synome operations directly.

### Phases 1-5

After Phase 0 completes, the facility enters its operational lifecycle. Phases 1 through 5 define per-deal workflows (origination, funding, servicing, maturity, wind-down) that will reuse the same orchestration infrastructure. These phases introduce additional step types, actors, and time-gated constraints, making extensibility a first-order concern.

### Existing Infrastructure

The system already has Temporal deployed in the Kubernetes cluster for other workflows. PostgreSQL (TimescaleDB via TigerData) is the primary data store.

## Options Considered

### Option A: Hybrid (Temporal + PostgreSQL)

Process definitions (the Phase 0 DAG with its 14 steps, actor assignments, step types, and dependency edges) are stored as data in PostgreSQL. A single generic Temporal workflow loads a snapshot of the DAG definition and interprets it step-by-step:

- **Human gates** (approvals from Core Council, submissions from Star Agent, reviews from Risk Team) are implemented as Temporal signal channels. The workflow durably waits for the signal at zero cost -- no polling, no timers, no resource consumption during waits that may last days or weeks.
- **Timer gates** (the 14-day cBEAM lock period) use `workflow.NewTimer`, which is durable across workflow restarts, deployments, and infrastructure failures. The timer fires exactly when the period elapses.
- **Beacon operations** (contract deployments, cBEAM grants, whitelist transactions, Synome writes) are implemented as Temporal activities with configurable retry policies. Temporal provides at-least-once execution with activity heartbeating for long-running on-chain transactions.
- **Parallel paths** in the DAG are executed as concurrent goroutines within the workflow, synchronized at merge points using Temporal's deterministic execution model.
- **Audit trail** is maintained in PostgreSQL via idempotent activity writes at each step transition, supplemented by Temporal's built-in workflow event history.

Full details: `nfat-architecture-hybrid-temporal.md`

### Option B: Custom State Machine Engine (PostgreSQL Only)

Both definitions and execution state live entirely in PostgreSQL. A custom `AdvanceInstance` engine loop processes the Phase 0 DAG:

- **Human gates** are implemented as DB status flags. External actors (Core Council, Star Agent, Risk Team) update the step status via API calls, and the next `AdvanceInstance` invocation detects the status change and advances the DAG.
- **Timer gates** (the 14-day cBEAM lock period) are tracked as a timestamp column. A periodic ticker (e.g., every 30 seconds) checks whether the lock period has elapsed and advances the step if so.
- **Beacon operations** are dispatched through a check handler registry. Each step type maps to a handler function that invokes the appropriate beacon. Retry logic, idempotency, and failure recovery must be built into the handler layer.
- **Parallel paths** are handled by the topological traversal in `AdvanceInstance` -- all steps whose dependencies are satisfied are advanced in the same pass.
- **Audit trail** is maintained in PostgreSQL as the single source of truth. Every state transition is recorded in an audit log table within the same transaction as the state change.
- **Concurrency control** uses row-level locking (`SELECT ... FOR UPDATE`) to prevent concurrent `AdvanceInstance` invocations from producing inconsistent state.

Full details: `nfat-architecture-custom-engine.md`

## Decision Drivers

1. **Compliance auditability** -- every step in the facility lifecycle must be fully traceable. Regulators and auditors must be able to reconstruct the exact sequence of events, who performed each action, and when. This is non-negotiable for DeFi structured finance compliance.

2. **Multi-actor coordination** -- 7 different actors with different roles and permissions must interact with the process. Some steps require sequential handoffs (Halo Contributor Team defines, then Risk Team approves), while others run in parallel across independent actors.

3. **Timer gates** -- the 14-day cBEAM lock period is a hard protocol constraint. If the system fires early or fails to fire at all, the facility setup is invalid. The timer must survive infrastructure failures, deployments, and scaling events.

4. **On-chain and off-chain action reliability** -- beacon operations that deploy contracts, grant cBEAM, or write to Synome must succeed exactly once. Failed on-chain transactions are expensive (gas costs) and potentially dangerous (partial state). Retries must be safe and idempotent.

5. **Extensibility to Phases 1-5** -- the architecture must accommodate additional process types without significant rework. Each phase introduces new step types, actors, time constraints, and DAG topologies. A pattern that works only for Phase 0 is insufficient.

6. **Operational simplicity** -- fewer systems to monitor, debug, and maintain reduce the risk of operational failures in a compliance-critical path.

7. **Team expertise** -- the team already operates Temporal for other services and has established patterns for workflows, activities, and monitoring. Leveraging existing expertise reduces ramp-up time and operational risk.

## Comparison

| Aspect | Hybrid (Temporal + PostgreSQL) | Custom Engine (PostgreSQL Only) |
|---|---|---|
| **14-day cBEAM timer** | `workflow.NewTimer` -- durable, exact, survives restarts | Polling ticker -- 30s+ latency, must handle missed ticks |
| **Human approval waits** | Signal channels -- zero-cost durable wait for days/weeks | DB status flag + API callback -- works but requires polling |
| **Beacon retry/recovery** | Built-in activity retries with backoff and heartbeating | Must build: retry logic, dead-letter handling, idempotency |
| **Parallel path execution** | Deterministic goroutines with merge synchronization | Topological traversal in AdvanceInstance loop |
| **Audit trail** | Dual: PostgreSQL audit log + Temporal event history | Single: PostgreSQL audit log (single source of truth) |
| **State consistency** | Dual state (DB + Temporal) -- requires idempotent sync | Single source of truth (PostgreSQL only) |
| **Debugging** | Two systems: Temporal UI + PostgreSQL | One system: PostgreSQL + application logs |
| **On-chain tx reliability** | Activity heartbeating detects stuck transactions | Must build: timeout detection, tx status polling |
| **Phase 1-5 extensibility** | Same generic workflow interprets any new DAG definition | Same AdvanceInstance engine interprets any new DAG definition |
| **Infrastructure** | Temporal (already deployed) + PostgreSQL | PostgreSQL only |
| **Build effort** | Lower -- Temporal handles durability, retries, timers | Higher -- must build engine, locking, crash recovery, timers |
| **Operational overhead** | Higher -- two systems to monitor | Lower -- one system to monitor |
| **Workflow visibility** | Temporal UI shows live workflow state and full history | Must build DAG visualization or rely on log queries |
| **Testing** | Temporal test framework + unit tests | Pure Go unit tests (simpler to reason about) |
| **Failure modes** | Temporal server outage blocks all workflows | PostgreSQL outage blocks all workflows |

## Recommendation

**Option A: Hybrid (Temporal + PostgreSQL)** is recommended.

The recommendation is grounded in the specific characteristics of the NFAT facility lifecycle:

1. **The 14-day cBEAM lock period demands durable timers.** This is not a soft timeout or an SLA -- it is a hard protocol constraint. `workflow.NewTimer` guarantees the timer fires after exactly 14 days regardless of deployments, restarts, or scaling events. The custom engine alternative (a polling ticker checking a timestamp column) works in the common case but introduces failure modes: missed ticks during outages, clock skew across replicas, and the need to build catch-up logic. For a compliance-critical time gate, the durable timer is the safer choice.

2. **Human approval waits are inherently long-running and unpredictable.** Core Council approvals, Star Agent submissions, and Risk Team reviews may take hours, days, or weeks. Temporal signals provide zero-cost durable waits -- the workflow consumes no resources while waiting. The custom engine approach (polling DB status flags) is functionally equivalent but adds unnecessary load and latency proportional to the polling interval.

3. **Beacon operations benefit from Temporal's retry semantics.** On-chain transactions (contract deployments, cBEAM grants, whitelist updates) are expensive and must not be duplicated or lost. Temporal activities provide configurable retry policies with exponential backoff, activity heartbeating for long-running transactions, and deterministic replay that prevents duplicate execution after crashes. Building equivalent guarantees in the custom engine is possible but represents significant engineering effort with high stakes for correctness.

4. **The generic workflow pattern scales to Phases 1-5.** Since the workflow code is a generic DAG interpreter, adding Phase 1 (deal origination) or Phase 3 (servicing) requires only new process definitions in PostgreSQL -- no Temporal workflow versioning, no code deployments. This is the same extensibility model as the custom engine, but with the durability and retry guarantees included.

5. **The team already operates Temporal.** The infrastructure is deployed, monitoring is configured, and the team has established patterns for workflow development. The marginal cost of adding an NFAT task queue is near zero.

**When Option B would be the better choice:**

- If the team determines that dual-state consistency (keeping PostgreSQL and Temporal in sync) introduces unacceptable complexity for the compliance audit trail. A single source of truth in PostgreSQL is simpler to reason about and audit.
- If the NFAT processes remain simple enough that the DAG never requires complex patterns beyond linear sequences and basic parallelism.
- If minimizing external dependencies on the compliance-critical path is prioritized -- removing Temporal from the critical path means one fewer system that can cause a facility setup to stall.

## Consequences

### If Hybrid (Temporal + PostgreSQL) is chosen

- The team must maintain consistency between PostgreSQL (source of truth for definitions and audit) and Temporal (source of truth for execution state). All activities that write to PostgreSQL must be idempotent.
- Debugging facility setup issues requires checking both Temporal UI (workflow history, pending signals, timer state) and PostgreSQL (audit log, step statuses, beacon operation records).
- Temporal server availability becomes a dependency on the compliance-critical path. A Temporal outage will block all in-flight facility setups until the server recovers. Temporal's persistence layer must be backed by a durable store with appropriate backup policies.
- Beacon activities must be designed with heartbeating to detect stuck on-chain transactions and with idempotency keys to prevent duplicate submissions.
- Phase 1-5 workflows are added by inserting new process definitions into PostgreSQL. No workflow code changes or Temporal versioning is required unless the step type vocabulary expands (e.g., a new type beyond Definition, Approval, Configuration, Review, Submission).

### If Custom Engine (PostgreSQL Only) is chosen

- The team owns all durability, retry, and concurrency logic. Bugs in row-level locking, crash recovery, or retry handling are the team's responsibility to find and fix.
- The 14-day cBEAM lock timer precision is limited by the ticker polling interval. Edge cases around outages, clock drift, and missed ticks must be explicitly handled.
- Workflow visualization for operations and debugging must be built from scratch -- there is no equivalent to Temporal UI out of the box.
- The operational model is simpler: one system (PostgreSQL) to monitor, back up, and maintain. Failure modes are well-understood and localized.
- The audit trail is authoritative by construction -- there is no second system to reconcile.

## Related Documents

- [Hybrid Architecture Design](nfat-architecture-hybrid-temporal.md)
- [Custom Engine Architecture Design](nfat-architecture-custom-engine.md)
- [Vector System Architecture](vector-architecture.excalidraw)
