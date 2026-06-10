# Multicall telemetry design

Date: 2026-06-10
Status: Approved for planning

## Context and goal

Raw smart contract (SC) call archiving (VEC-81) writes one S3 object per call
today. We are deciding whether to (a) keep the concurrency semaphore in the
archiving decorator and (b) later batch one S3 object per `Execute()`. Both
decisions need data we do not currently collect: how many multicalls and how
many individual SC calls happen per second across the fleet, and how large a
single `Execute()` batch gets.

This work adds telemetry on multicall usage so the load can be observed in
Grafana before those decisions are made. No archiving behaviour changes here.

Goal: answer, from Grafana,

- batched S3 PUTs/sec (= multicalls/sec),
- current per-call S3 PUTs/sec (= individual SC calls/sec),
- worst-case burst of concurrent calls a single `Execute()` can produce,
- actual archive write rate and success/error split where archiving is on.

## Metrics

### Layer 1: base multicall client

File: `internal/pkg/blockchain/multicall/client.go`

- `multicall.batch.size` (histogram, label `chain`)
  - Recorded once per `Execute()` with the number of calls in the batch.
  - `_count` rate = multicalls/sec = batched S3 PUTs/sec.
  - `_sum` rate = individual calls/sec = current per-call S3 PUTs/sec.
  - Distribution / p100 = largest single-Execute burst, the quantity the
    semaphore defends against.

Recorded for all multicall traffic regardless of whether archiving is enabled,
so fleet call volume is visible before archiving is turned on everywhere.

### Layer 2: archiving decorator

File: `internal/pkg/blockchain/archiving/multicaller.go`

- `archive.writes.total` (counter, labels `chain`, `source`, `status`)
  - `status` is `success` or `error`.
  - Incremented once per archive write attempt (per call under the current
    per-call design).
  - Rate = ground-truth S3 PUT rate where archiving is enabled.
  - The `status` split surfaces archive write failures that are currently only
    logged at error level.

### Explicitly out of scope (YAGNI)

- `multicall.execution.duration` (latency): does not serve the semaphore/load
  decision. Trivial to add later if wanted.
- Explicit `multicall.calls.total` / `multicall.executions.total` counters: the
  histogram `_sum` and `_count` already provide these, plus the distribution.

## Wiring

### Base client telemetry

A `multicall.Telemetry` struct mirrors `alchemy.Telemetry`:

- `NewTelemetry(chain string)` uses the global OTEL meter provider
  (`otel.GetMeterProvider()`), exactly like `alchemy.NewTelemetry`.
- The `multicall.Client` gains an optional `*Telemetry` field, attached via a
  functional option `multicall.WithTelemetry(t)`.
- `NewClient(ethClient, addr, opts ...Option)` keeps its existing two-arg form
  valid; callers opt in to telemetry explicitly.
- All recording is nil-safe: no telemetry attached = no-op. If a cmd has not
  initialised OTEL, the global meter is a no-op (no data, no crash).

Cmd binaries that construct a multicall client and want fleet metrics pass
`multicall.WithTelemetry(multicall.NewTelemetry(chain))`. Target callsites are
the watcher and worker entrypoints that already know their chain.

### Decorator telemetry

The archiving decorator is built in one place,
`archivingwire.NewS3WrapFromEnv`, so `archive.writes.total` wires there. The
decorator already carries `ChainID` and `Source` in its `Config`; the meter is
obtained from the global provider.

## Labels rationale

Labels cannot be retrofitted onto already-collected series, so they are fixed
now:

- `chain` on both metrics: per-chain attribution, consistent with existing
  Alchemy metrics.
- `source` on the archive counter: attributes S3 load to the originating
  indexer (morpho, oracle, prime-allocation, etc.).
- `status` on the archive counter: success/error split.

## Testing

- `multicall` package: unit test that `Execute()` records the histogram with the
  correct batch size and chain attribute via a test meter provider; nil-telemetry
  path is a no-op.
- `archiving` package: extend `multicaller_test.go` to assert `archive.writes.total`
  is incremented with the right labels on both success and error archive paths,
  using a test meter provider.
- No integration test changes required; behaviour is unchanged.

## Out of scope

- Batching one S3 object per `Execute()` (separate follow-up, informed by this
  data).
- Removing the semaphore (decision driven by the burst data this collects).
- Any change to archiving write behaviour or S3 key scheme.
