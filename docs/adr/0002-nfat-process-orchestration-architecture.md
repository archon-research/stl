# ADR-0002: NFAT Process Orchestration Architecture

**Status**: Proposed
**Date**: 2026-03-31

## Reviewers

| Area | People |
| --- | --- |
| **Risk** | Peter Simon, Katarina Janec |
| **NFAT process** | Derek Flossman |
| **Vector engineers** | Tore Christensen, Simon Boje Outzen, Vialli Kavoo |

## Decision Drivers

- **Dependency enforcement** — steps must only execute when all prerequisites are complete
- **Decoupling** — the orchestrator must not have direct knowledge of downstream systems (beacons, Synome, on-chain tooling)
- **Auditability** — every state transition must be traceable for compliance
- **Process evolution** — it must be possible to add steps to both future and in-flight process instances
- **Time-based gates** — some steps are blocked by wall-clock constraints (e.g., 14-day cBEAM lock)
- **Partial parallelism** — independent tracks (e.g., attestor flow vs. risk framework flow) must execute concurrently without explicit parallel/join constructs
- **Low operational complexity** — the team is small; the solution should minimise new infrastructure

## Context

The NFAT (Non-Fungible Allocation Token) lifecycle is a multi-phase compliance verification process for creating and managing structured finance facilities. Phase 0 alone contains 14 steps involving 7 distinct actors (Halo Contributor Team, Vector, Risk Team, Core Council, GovOps, Star Agent, Halo), each with explicit dependency chains, on-chain actions, and Synome database writes via beacons.

Key characteristics of the process:

- **Strict ordering with partial parallelism.** Some steps are independent (e.g., the attestor selection flow in Steps 0.8–0.9 runs in parallel with risk framework approval in Steps 0.2–0.6), while others have hard sequential dependencies (e.g., Step 0.11 requires both the cBEAM 14-day lock to elapse and the risk framework to be written to Synome Axis).
- **Time-based gates.** The cBEAM grant in Step 0.7 imposes a 14-day lock period before downstream steps can proceed.
- **Multiple actors and systems.** Steps involve on-chain transactions, off-chain governance decisions, beacon writes to Synome (NFAT) and Synome Axis, and document submissions.
- **Process evolution.** The NFAT process is expected to evolve — new compliance requirements, additional risk checks, or governance steps may need to be inserted into an already-defined process without invalidating in-flight facilities.
- **Auditability.** Every state transition must be traceable for compliance purposes.

We need an orchestration architecture that enforces dependency ordering, supports parallel execution where allowed, handles time-based gates, persists state durably, and allows the process definition itself to evolve over time.

## Decision

Use a **state machine** backed by a **persistent event bus** and a **relational database** as the source of truth for NFAT process orchestration.

### State Machine

Each NFAT process (per Facility in Phase 0, per Deal in Phases 1–5) is modelled as a directed acyclic graph (DAG) of steps. Each step has:

- **A unique identifier** (e.g., `0.1`, `0.7`, `0.11`)
- **A status**: `PENDING` | `IN_PROGRESS` | `COMPLETED` | `FAILED`
- **A list of dependency step IDs** that must all be `COMPLETED` before this step can transition to `IN_PROGRESS`
- **An actor** responsible for advancing the step
- **A step type**: `Definition` | `Configuration` | `Approval` | `Review` | `Submission`
- **Optional time-based gates** (e.g., "cBEAM lock must have elapsed")

The state machine enforces the invariant: **a step may only transition to `IN_PROGRESS` when all of its dependencies are `COMPLETED` and all time-based gates have passed.** This is the sole mechanism that governs step ordering — there is no implicit ordering beyond what the dependency graph encodes.

For Phase 0, the dependency graph is:

```
Step 0.1  (no deps)
  └→ Step 0.2
       └→ Step 0.3
            └→ Step 0.4
                 └→ Step 0.5
                      └→ Step 0.6
                           └→ Step 0.7 (starts 14-day lock)

Step 0.8  (no deps, parallel track)
  └→ Step 0.9
       └→ Step 0.10

Step 0.11 (deps: Step 0.7 + 14-day lock elapsed, Step 0.6)
  └→ Step 0.12
  └→ Step 0.13 (deps: Step 0.10, Step 0.11)
       └→ Step 0.14  → Phase 0 Complete
```

Process definitions are versioned. In-flight instances are isolated from future definition changes but can still have steps added — see [Dynamic Step Insertion](#dynamic-step-insertion).

### Event Bus

Every state transition emits an event to a durable event bus. The event bus serves primarily as a **decoupling boundary** between the state machine and the downstream systems that react to step transitions.

Step completions trigger actions across multiple independent systems — lpha-nfat writes to Synome (NFAT), Council Beacon writes risk frameworks to Synome Axis, lpha-attest manages attestor operations, and GovOps tooling deploys on-chain contracts. Without an event bus, the orchestration layer would need direct knowledge of every downstream system, calling each one explicitly on each transition. This creates tight coupling: adding a new beacon, notification, or audit consumer would require modifying the orchestrator.

With the event bus, the orchestration layer emits a "step X transitioned to Y" event and is done. Each downstream system subscribes to the events it cares about and acts independently. This means:

- **Beacons are independently deployable.** A new beacon can subscribe to relevant events without changing the orchestrator.
- **Failures are isolated.** If lpha-nfat fails to write to Synome, it retries independently without blocking Council Beacon or any other consumer.
- **The orchestrator stays simple.** It enforces the DAG, persists state, and emits events. It has no knowledge of what happens downstream.

Note: the event bus is justified by decoupling, not throughput. NFAT event volumes are very low (tens of transitions per facility, spread over weeks). Any of the technology options below can handle this volume trivially.

The event bus technology is an open decision — see [Event Bus Technology Options](#event-bus-technology-options) below for the candidates under evaluation.

### Database (Source of Truth)

PostgreSQL serves as the durable source of truth for all process state. The event bus is a transport mechanism — if the bus is lost, the database state is canonical and the event log in the database can be replayed.

Core tables:

- **`nfat_process`** — One row per process instance (facility or deal), including the process definition version.
- **`nfat_step`** — One row per step per process instance. Contains current status, dependencies (as a step ID array), actor, timestamps, and time-gate configuration.
- **`nfat_event`** — Append-only log of every state transition event. Mirrors what flows through the event bus but persisted durably for audit and replay.
- **`nfat_process_definition`** — Versioned process templates. Stores the step graph (steps, dependencies, actors, types, time gates) as a versioned record. When a process instance is created, it snapshots the current definition version.

### Dynamic Step Insertion

The architecture supports adding steps to a process definition and, under controlled conditions, to in-flight process instances:

**Definition-level insertion.** A new step can be added to `nfat_process_definition` by creating a new version. New process instances use the latest version. Existing in-flight instances are unaffected — they continue with their snapshotted definition.

**Instance-level insertion.** A new step can be inserted into an in-flight process instance if and only if:

1. All steps whose dependency lists would be modified are still in `PENDING` status.
2. The new step's dependencies reference existing steps in the instance.
3. The insertion is recorded as an event for auditability.

If any affected downstream step has already transitioned beyond `PENDING`, the insertion is rejected. This prevents retroactive modification of steps that are already executing or complete.

When a step is inserted:
- A new `nfat_step` row is created with status `PENDING`.
- Affected downstream steps have their dependency arrays updated to include the new step.
- A `STEP_INSERTED` event is emitted to the event bus.
- If the new step's own dependencies are already all `COMPLETED`, it immediately becomes eligible for `IN_PROGRESS`.

### How It Fits Together

```
┌──────────────────────────────────────────────┐
│              STATE MACHINE                    │
│                                               │
│  Step 0.1 ──→ Step 0.2 ──→ ... ──→ Step 0.14│
│  Step 0.8 ──→ Step 0.9 ──→ Step 0.10        │
│                                               │
│  Each step transition emits an event ────────────→  ┌────────────┐
│                                               │     │  EVENT BUS │
│  Workers read events, perform actions,       │     │            │
│  and request the next transition ←───────────────  │            │
└──────────────────────────────────────────────┘     └─────┬──────┘
                                                           │
                                                      persist
                                                           │
                                                    ┌──────▼──────┐
                                                    │  PostgreSQL  │
                                                    │  (source of  │
                                                    │    truth)    │
                                                    └─────────────┘
```

1. An actor (or automated worker) requests a step transition.
2. The state machine validates that all dependencies are `COMPLETED` and time gates have passed.
3. The step status is updated in PostgreSQL.
4. A transition event is written to both the `nfat_event` table and published to the event bus.
5. Downstream workers consume the event. If their step's dependencies are now all met, they begin work.
6. When work completes, the worker requests the next transition, and the cycle repeats.

## Event Bus Technology Options

Five technologies are under consideration for the event bus layer. All are evaluated against the same criteria: durability, ordering guarantees, operational complexity, fit with the existing stack, and suitability for NFAT event volumes (low — tens of events per facility, not thousands per second).

### Option A — Redis Streams

Redis Streams provide ordered, persistent (within Redis), consumer-group-based event delivery. Workers subscribe via `XREADGROUP` and acknowledge processing with `XACK`.

| Criterion | Assessment |
| --- | --- |
| **Already in stack** | Yes — used for caching in the live data pipeline |
| **Ordering** | Ordered within a stream |
| **Durability** | Persistent to disk (AOF/RDB), but not designed as a primary durable store. Data survives restarts if persistence is configured, but failover scenarios can lose recent writes |
| **Consumer groups** | Built-in. Multiple workers can process different step types concurrently. PEL (pending entry list) tracks unacknowledged messages |
| **Operational overhead** | Low incremental — Redis is already operated. No new infrastructure |
| **Dual-write concern** | PostgreSQL and Redis must be kept consistent. Write DB first, then publish to Redis. On Redis failure, a reconciliation worker replays from `nfat_event` |
| **Throughput ceiling** | Far exceeds NFAT requirements |

### Option B — AWS SNS/SQS (FIFO)

SNS FIFO for ordered fan-out, SQS FIFO queues for consumer delivery. Already used in the live data pipeline for block event distribution.

| Criterion | Assessment |
| --- | --- |
| **Already in stack** | Yes — SNS FIFO + SQS used for block data fan-out |
| **Ordering** | FIFO ordering guaranteed per message group ID |
| **Durability** | Fully managed, highly durable. AWS guarantees at-least-once delivery with no data loss |
| **Consumer groups** | Modelled via multiple SQS FIFO subscriptions to a single SNS FIFO topic. Each consumer type gets its own queue |
| **Operational overhead** | None — fully managed by AWS. No servers to operate |
| **Dual-write concern** | Same as Redis. DB write first, then SNS publish. Alternatively, use the transactional outbox pattern with a relay that reads from PostgreSQL and publishes to SNS |
| **Throughput ceiling** | 300 msg/s per message group (3,000 with batching). Far exceeds NFAT requirements |
| **Additional considerations** | Adds AWS coupling to the NFAT orchestration layer. SQS FIFO has a 14-day message retention limit. Exactly-once processing available within the deduplication window (5 min) |

### Option C — NATS JetStream

NATS is a lightweight, high-performance messaging system. JetStream adds persistent streaming with consumer groups, replay, and exactly-once semantics.

| Criterion | Assessment |
| --- | --- |
| **Already in stack** | No — would be new infrastructure |
| **Ordering** | Ordered per subject/stream |
| **Durability** | Persistent to disk with configurable replication. Designed as a durable streaming layer |
| **Consumer groups** | Built-in via durable consumers. Supports pull-based and push-based delivery, acknowledgement, and redelivery |
| **Operational overhead** | Medium — requires deploying and operating a NATS cluster (or using a managed offering). New operational surface for the team |
| **Dual-write concern** | Same as Redis and SNS/SQS |
| **Throughput ceiling** | Very high (millions of msg/s). Far exceeds NFAT requirements |
| **Additional considerations** | Lightweight binary, simple to deploy on Kubernetes. Embeddable in Go applications. Rich subject-based routing. If adopted, could serve as a general messaging backbone beyond NFAT, potentially replacing Redis Streams and SNS/SQS in future services |

### Option D — PostgreSQL as the Event Bus (SKIP LOCKED / pgmq)

Use PostgreSQL itself as the queue. Workers poll for events using `SELECT ... FOR UPDATE SKIP LOCKED`, optionally with `LISTEN/NOTIFY` for push-based wake-up signals. Alternatively, use [pgmq](https://github.com/tembo-io/pgmq) which wraps this pattern into a clean API.

| Criterion | Assessment |
| --- | --- |
| **Already in stack** | Yes — PostgreSQL is the source of truth |
| **Ordering** | Ordered by insertion (sequence/timestamp column) |
| **Durability** | Excellent — same transactional guarantees as the rest of the data |
| **Consumer groups** | Manual — modelled via status columns and `SKIP LOCKED`. pgmq provides a higher-level abstraction with visibility timeouts |
| **Operational overhead** | None — no new infrastructure. Adds load to the existing PostgreSQL instance |
| **Dual-write concern** | **None** — single persistence layer. Event insert and state update happen in the same transaction. This is the only option that eliminates the dual-write problem entirely |
| **Throughput ceiling** | Lower than dedicated messaging systems, but far exceeds NFAT requirements |
| **Additional considerations** | Polling latency (mitigated but not eliminated by `LISTEN/NOTIFY`). PostgreSQL becomes both data store and messaging transport, coupling these concerns. No native fan-out — multiple consumers require application-level routing |

### Option E — Apache Kafka

Industry-standard distributed event streaming platform. Provides durable, ordered, partitioned log-based messaging.

| Criterion | Assessment |
| --- | --- |
| **Already in stack** | No — would be new infrastructure |
| **Ordering** | Ordered per partition |
| **Durability** | Excellent — replicated, persistent log with configurable retention |
| **Consumer groups** | Built-in. Consumer groups with partition assignment, offset tracking, and rebalancing |
| **Operational overhead** | High — requires operating a Kafka cluster (brokers + KRaft/ZooKeeper). Significant operational surface for a small team |
| **Dual-write concern** | Same as Redis and SNS/SQS |
| **Throughput ceiling** | Very high (millions of msg/s). Massively over-provisioned for NFAT requirements |
| **Additional considerations** | Richest ecosystem for event streaming (Kafka Connect, Schema Registry, ksqlDB). However, the operational cost and complexity are disproportionate to NFAT event volumes. Better suited to high-throughput, multi-consumer event streaming at scale |

### Comparison

| | Redis Streams | AWS SNS/SQS FIFO | NATS JetStream | PostgreSQL (SKIP LOCKED) | Apache Kafka |
| --- | --- | --- | --- | --- | --- |
| Already in stack | Yes | Yes | No | Yes | No |
| Managed service | No (self-operated) | Yes | No (unless managed) | Yes (RDS) | No (unless managed) |
| New infra to operate | None | None | NATS cluster | None | Kafka cluster |
| Ordering | Per stream | Per message group | Per subject | Per sequence | Per partition |
| Durability | Good (with AOF) | Excellent | Excellent | Excellent | Excellent |
| Dual-write problem | Yes | Yes | Yes | **No** | Yes |
| Go ecosystem | Mature (go-redis) | Mature (AWS SDK) | Mature (nats.go) | Mature (pgx/sqlx) | Mature (confluent-kafka-go) |
| Broader future utility | Limited to caching + streams | Limited to AWS environments | High — general-purpose messaging | Limited to PostgreSQL workloads | High — event streaming platform |
| Operational complexity | Low | Low | Medium | Low | High |

## Consequences

**Positive:**
- Step dependencies are declarative data, not imperative code — the process graph is inspectable, versionable, and modifiable without code changes.
- Every state transition is persisted and auditable, which is essential for compliance.
- Parallel execution paths (e.g., attestor flow vs. risk framework flow) emerge naturally from the dependency graph without explicit parallel/join constructs.
- Dynamic step insertion is a first-class capability with clear safety invariants.
- The event bus allows horizontal scaling of workers per step type.
- PostgreSQL as source of truth means the system can recover from event bus failures by replaying from the database.
- The same architecture serves Phase 0 (one-time setup) and Phases 1–5 (per-deal), with different process definitions.

**Negative / Trade-offs:**
- Two persistence layers (PostgreSQL + event bus) must be kept consistent. The database is authoritative; the event bus is ephemeral transport. Dual-write failures must be handled (write DB first, then publish; on publish failure, a reconciliation worker replays from `nfat_event`).
- Time-based gates (14-day cBEAM lock) require either a polling mechanism or a scheduled job that checks and transitions when the gate opens.
- The state machine must be carefully designed so that the dependency graph for each phase is a true DAG — cycles would cause deadlocks.
- Workers must be idempotent, since all three event bus options provide at-least-once delivery.
- Whichever event bus is chosen, it adds a dependency beyond PostgreSQL that must be monitored and maintained.

## References

- [Capital Lifecycle process flow](https://www.notion.so/Capital-Lifecycle-process-flow-31bb87693a5a8196832cf4ee00d3c740) — full NFAT process specification (Notion)
- [State Machine + Event Bus simulation](docs/architecture/state-machine-event-bus.html) — interactive prototype demonstrating the DAG-based orchestration and dynamic step insertion
