# PR #89 Review: SEN-172 — Sparklend Backfill Cmd

**Author:** toreluntang  
**Branch:** `toreluntang/historical-sparklend-data-puller` -> `main`  
**Stats:** +2467 / -92 across 17 files  
**CI Status:** Pending (Analyze go, Lint/Test/Build)

---

## Overview

This PR adds a standalone CLI tool for backfilling historical SparkLend position data. It reads transaction receipts from S3 (with RPC fallback for missing blocks), processes them through the existing position tracker, and persists results to PostgreSQL. The PR also makes the existing position tracker service compatible with backfill mode (nil SQS/Redis), refactors Postgres repositories for concurrent upsert safety, and adds comprehensive tests.

### Files Changed

| Category | Files |
|----------|-------|
| **New CLI** | `cmd/sparklend-backfill/main.go`, `main_test.go` |
| **New service** | `services/sparklend_backfill/service.go`, `service_test.go` |
| **New port** | `ports/outbound/block_receipts_reader.go` |
| **Modified tracker** | `services/sparklend_position_tracker/service.go`, `blockchain_service.go`, `blockchain_service_test.go`, `event_extractor.go`, `event_extractor_test.go` |
| **Modified repos** | `postgres/protocol_repository.go`, `token_repository.go`, `user_repository.go` |
| **New integration tests** | `postgres/protocol_repository_integration_test.go`, `token_repository_integration_test.go`, `user_repository_integration_test.go` |
| **Minor** | `adapters/outbound/alchemy/client.go` (compile-time interface assertion) |

---

## Strengths

### 1. Clean hexagonal architecture
The backfill service depends only on ports (`outbound.S3Reader`, `outbound.BlockReceiptsReader`) and a locally-defined `ReceiptProcessor` interface. No adapter imports leak into the service layer.

### 2. Well-designed concurrency model
- Worker pool using `sync.WaitGroup.Go()` (Go 1.25+) with a buffered channel for backpressure.
- Progress goroutine uses `context.Background()` to outlive parent cancellation, with explicit `wg.Wait() -> progressCancel() -> <-progressDone` sequencing to prevent goroutine leaks.
- Graceful shutdown via signal handling and context cancellation propagates to workers and the block enqueuer.

### 3. Correct Postgres upsert patterns for concurrency safety
- Protocol and token: `INSERT ... ON CONFLICT DO NOTHING RETURNING id` with fallback `SELECT` correctly handles the PostgreSQL behavior where `RETURNING` produces no rows on `DO NOTHING`.
- User: `INSERT ... ON CONFLICT DO UPDATE SET first_seen_block = LEAST(...) RETURNING id` always returns a row and is idempotent regardless of execution order.

### 4. Comprehensive test coverage
- 862 lines of service tests with 13 `TestRun` scenarios covering success, errors, cancellation, RPC fallback, and version selection.
- Concurrent race integration tests for all three repository upserts (10 workers each).
- Table-driven CLI flag validation tests.

### 5. Thoughtful S3 version discovery
`ScanVersions` + `BuildVersionMap` scan S3 prefixes upfront, choosing the highest receipt version per block before processing begins. This avoids per-block S3 listing overhead.

---

## Issues

### Critical

None found.

### High Severity



#### H1. `sync.WaitGroup.Go()` does not exist in standard Go
`service.go:147` — The code calls `wg.Go(func() { ... })` on a `sync.WaitGroup`. The standard library `sync.WaitGroup` in Go 1.25 does **not** have a `.Go()` method. This was flagged by Copilot as well. Unless this project uses a custom `sync.WaitGroup` wrapper or a build with an experimental patch, this will not compile.

**Expected pattern:**
```go
for range s.config.Concurrency {
    wg.Add(1)
    go func() {
        defer wg.Done()
        for blockNum := range blockCh {
            // ...
        }
    }()
}
```

EDIT: Please verify that this is the case for go 1.26

**File:** `stl-verify/internal/services/sparklend_backfill/service.go:147`

#### H2. Panic risk in `batchGetUserReserveData` — bare type assertions
`blockchain_service.go:321-332` uses bare type assertions (`unpacked[0].(*big.Int)`, `unpacked[8].(bool)`) on 8 of 9 fields, but only field index 7 gets a safe assertion with `, ok`. If the ABI returns unexpected types, these will panic at runtime. This code has no test coverage.

**File:** `stl-verify/internal/services/sparklend_position_tracker/blockchain_service.go:321-332`

### Medium Severity

#### M1. Signal handler goroutine leaks on normal completion
`main.go:122-128` — `signal.Stop(sigChan)` is never called. When `run()` returns normally, the goroutine at line 124 remains permanently blocked on `<-sigChan`. Adding `defer signal.Stop(sigChan)` after `signal.Notify` would fix this.

**File:** `stl-verify/cmd/sparklend-backfill/main.go:122-128`

#### M2. `protocolType` parameter is unused in `GetOrCreateProtocol`
`protocol_repository.go:64` hardcodes `"lending"` instead of using the `protocolType` parameter. This means the function signature accepts a value it ignores.

**File:** `stl-verify/internal/adapters/outbound/postgres/protocol_repository.go:53-82`

#### M3. `Service.mu` should be `sync.RWMutex` for read-heavy path
`service.go:118` — `getOrCreateBlockchainService` acquires an exclusive `sync.Mutex` lock just to check if a key exists in the map. Since this is the hot path (called for every receipt), using `sync.RWMutex` with `RLock()` for the first check would reduce contention.

**File:** `stl-verify/internal/services/sparklend_position_tracker/service.go:118`

#### M4. `io.ReadAll` in `processBlockFromS3` may spike memory under high concurrency
`service.go:337` loads the entire decompressed receipt JSON into memory. With high concurrency (e.g., `--concurrency 50`), this could cause significant memory pressure on blocks with large receipt sets. Consider using `json.NewDecoder(rc).Decode(&receipts)` for streaming decoding.

**File:** `stl-verify/internal/services/sparklend_backfill/service.go:337`

#### M5. `ScanVersions` is sequential across partitions
For large block ranges (e.g., 1M blocks = 1000 partitions), listing one S3 partition prefix at a time could make startup slow. Parallelizing `ListPrefix` calls with a bounded worker pool would improve this.

**File:** `stl-verify/internal/services/sparklend_backfill/service.go:277-301`

#### M6. Inconsistent `metadata` handling between `GetOrCreateUser` and `upsertUserBatch`
- `GetOrCreateUser` does NOT update `metadata` on conflict.
- `upsertUserBatch` DOES overwrite `metadata` on conflict.

If both code paths handle the same users, metadata behavior will differ depending on which path runs.

**File:** `stl-verify/internal/adapters/outbound/postgres/user_repository.go`

#### M7. Only the first worker error is returned from `Run`
`service.go` — `errCh` has capacity 1 with a non-blocking send. If multiple blocks fail, only the first error is propagated to the caller. The `failed` counter captures the total, but callers of `Run` see only one error. Consider documenting this clearly or returning an aggregated error.

**File:** `stl-verify/internal/services/sparklend_backfill/service.go`

### Low Severity

#### L1. Swallowed errors from `erc20ABI.Pack()`
`blockchain_service.go:358-360` discards errors from `Pack()` calls. While these are constant method names that should never fail, this violates the project's "Skip error handling" DO NOT rule.

**File:** `stl-verify/internal/services/sparklend_position_tracker/blockchain_service.go:358-360`

#### L2. `chainID` is never validated in `NewService`
The constructor validates logger, s3Reader, processor, rpcFallback, and bucket, but accepts any `chainID` including 0 or negative values without error.

**File:** `stl-verify/internal/services/sparklend_backfill/service.go`

#### L3. `BlockReceiptsReader` contract is ambiguous for empty/nonexistent blocks
The interface doesn't specify what to return for blocks with no transactions or blocks that don't exist. The alchemy client may return `nil` `json.RawMessage` for such cases. `json.Unmarshal(nil, &receipts)` produces a nil slice, and it's unclear if `ProcessReceipts` handles this correctly.

**File:** `stl-verify/internal/ports/outbound/block_receipts_reader.go`

#### L4. Default RPC mock in tests silently succeeds
In `service_test.go`, the default `mockBlockReceiptsReader` returns `"[]"` on any call. If the service erroneously called RPC for blocks that exist in S3, the test would still pass. A stricter default that fails on unexpected calls would catch this.

**File:** `stl-verify/internal/services/sparklend_backfill/service_test.go`

#### L5. No test for malformed JSON from S3 or RPC
The `json.Unmarshal` error paths in both `processBlockFromS3` and `processBlockFromRPC` have no test coverage.

**File:** `stl-verify/internal/services/sparklend_backfill/service_test.go`

#### L6. Context stored on `Service` struct
`service.go:125-126` stores `ctx` and `cancel` as struct fields. This is an anti-pattern per Go conventions (contexts should be passed as parameters). It's mitigated here since they're only used for `Start`/`Stop` lifecycle management.

**File:** `stl-verify/internal/services/sparklend_position_tracker/service.go:125-126`

#### L7. No test coverage for `batchGetUserReserveData`, `getUserReservesData`, or `getFullReserveData`
These methods in `blockchain_service.go` have no test coverage. `batchGetUserReserveData` in particular contains the panic-prone bare type assertions noted in H2.

**File:** `stl-verify/internal/services/sparklend_position_tracker/blockchain_service.go`

#### L8. `updated_at` not bumped when only `metadata` changes in batch user upsert
In `upsertUserBatch`, `metadata` is always overwritten on conflict, but `updated_at` is only bumped when `first_seen_block` decreases. A metadata-only change will not update `updated_at`.

**File:** `stl-verify/internal/adapters/outbound/postgres/user_repository.go:133-138`

---

## Test Coverage Assessment

| Component | Unit Tests | Integration Tests | Coverage Quality |
|-----------|-----------|-------------------|------------------|
| CLI flag parsing | Good (10 cases) | N/A | Strong |
| Backfill service | Good (13 Run cases, 8 BuildVersionMap, 4 ScanVersions, 6 validation) | None | Strong for happy path; gaps on malformed JSON |
| Protocol repo upsert | N/A | Good (create, idempotent, concurrent race) | Strong |
| Token repo upsert | N/A | Good (create, idempotent, empty symbol, concurrent race) | Strong |
| User repo upsert | N/A | Good (create, idempotent, LEAST semantics, concurrent race) | Strong |
| Blockchain service | Partial (ABI loading, parsing, concurrent metadata cache) | None | Gaps on `batchGetUserReserveData`, `getUserReservesData`, `getFullReserveData` |

---

## Previously Addressed Review Comments

The following issues from CodeRabbit were already fixed:
- Extra leading space in comments (fixed in commit `3fe733c`)
- Missing `defer rpcClient.Close()` (fixed in commit `3fe733c`)
- Doc comment concurrency default mismatch (fixed in commits `4f6cc2a` to `f027f2c`)
- Formatting issues in `blockchain_service.go` (fixed in commit `3fe733c`)

---

## Recommendations

1. **Fix H1** (`wg.Go()`) — this is a compile blocker if using standard Go.
2. **Fix H2** — add safe type assertions (or at minimum, add test coverage) for `batchGetUserReserveData`.
3. **Add `defer signal.Stop(sigChan)`** in main.go (M1).
4. **Consider `json.NewDecoder` streaming** for large receipt files (M4).
5. **Document the first-error-only behavior** of `Run` (M7), or switch to collecting errors.
6. **Add tests for malformed JSON** from both S3 and RPC paths (L5).
