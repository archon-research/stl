# Maple Indexer sqsutil Migration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the maple indexer's hand-rolled SQS polling loop with the shared `sqsutil.RunLoop` introduced in PR #77, and align `main.go` with the new `sqsadapter.NewConsumer` API.

**Architecture:** PR #77 extracted duplicated SQS boilerplate (polling loop, JSON parsing, delete-on-success) into `internal/common/sqsutil` and promoted `outbound.BlockEvent` as the canonical shared struct. The maple indexer was written before that PR merged and still carries its own `processLoop`, `processMessages`, local `blockEvent` struct, and `processMessage` — all of which are now dead weight. We delete all of that and wire `sqsutil.RunLoop` instead, exactly as `oracle_price_worker` and `sparklend_position_tracker` do.

**Tech Stack:** Go 1.25, `internal/common/sqsutil`, `internal/ports/outbound.BlockEvent`, `internal/adapters/outbound/sqs.NewConsumer`

---

## Task 1: Update `service.go` — remove duplicated SQS boilerplate

**Files:**
- Modify: `stl-verify/internal/services/maple_indexer/service.go`

The service currently has:
- `processLoop()` — ticker loop (lines 170–184)
- `processMessages()` — receive + fan-out (lines 186–217)
- `type blockEvent struct` — local copy of the canonical type (lines 219–227)
- `processMessage()` — JSON unmarshal + delegate (lines 229–236)
- `processBlock(ctx, blockEvent)` — the actual business logic (uses local type)

All of the above except `processBlock` must be deleted. `processBlock` must have its signature changed from `blockEvent` → `outbound.BlockEvent`. The `Start` method must call `sqsutil.RunLoop` instead of `go s.processLoop()`.

**Step 1: Open the file and confirm lines to remove**

Read `stl-verify/internal/services/maple_indexer/service.go`. Confirm:
- `processLoop` at ~line 170
- `processMessages` at ~line 186
- `type blockEvent struct` at ~line 219
- `processMessage` at ~line 229

**Step 2: Remove the four items and update Start + processBlock**

Replace `go s.processLoop()` in `Start` with:

```go
go sqsutil.RunLoop(s.ctx, sqsutil.Config{
    Consumer:     s.consumer,
    MaxMessages:  s.config.MaxMessages,
    PollInterval: s.config.PollInterval,
    Logger:       s.logger,
}, s.processBlock)
```

Delete methods `processLoop`, `processMessages`, `processMessage` and `type blockEvent struct`.

Change `processBlock` signature from:
```go
func (s *Service) processBlock(ctx context.Context, event blockEvent) error {
```
to:
```go
func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) error {
```

Add import for `sqsutil`:
```go
"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
```

Remove now-unused imports: `"encoding/json"`, `"errors"`.

**Step 3: Run tests to confirm nothing broken yet**

```bash
cd stl-verify && go build ./internal/services/maple_indexer/...
```
Expected: compiles cleanly.

**Step 4: Commit**

```bash
git add stl-verify/internal/services/maple_indexer/service.go
git commit -m "refactor(maple-indexer): replace hand-rolled SQS loop with sqsutil.RunLoop"
```

---

## Task 2: Update `service_test.go` — switch local `blockEvent` to `outbound.BlockEvent`

**Files:**
- Modify: `stl-verify/internal/services/maple_indexer/service_test.go`

The test file has:
- `makeMapleBlockEventJSON` (line ~205) which constructs `blockEvent{...}` (local type)
- All `TestProcessBlock` sub-tests call `svc.processBlock(ctx, blockEvent{...})` with the local type

After Task 1, the local type no longer exists. All `blockEvent{...}` literals must become `outbound.BlockEvent{...}`.

**Step 1: Update `makeMapleBlockEventJSON`**

Change:
```go
func makeMapleBlockEventJSON(blockNumber int64, blockTimestamp int64) string {
    event := blockEvent{
        ChainID:        1,
        BlockNumber:    blockNumber,
        Version:        1,
        BlockHash:      "0xabc123",
        BlockTimestamp: blockTimestamp,
    }
    data, _ := json.Marshal(event)
    return string(data)
}
```
To:
```go
func makeMapleBlockEventJSON(blockNumber int64, blockTimestamp int64) string {
    event := outbound.BlockEvent{
        ChainID:        1,
        BlockNumber:    blockNumber,
        Version:        1,
        BlockHash:      "0xabc123",
        BlockTimestamp: blockTimestamp,
    }
    data, _ := json.Marshal(event)
    return string(data)
}
```

**Step 2: Replace all `blockEvent{...}` literals in tests**

Every occurrence of `blockEvent{ChainID: ..., BlockNumber: ..., ...}` in `TestProcessBlock` and `TestProcessMessages` becomes `outbound.BlockEvent{ChainID: ..., BlockNumber: ..., ...}`.

Specifically update these test sub-tests (search for `blockEvent{` in the file):
- `TestProcessBlock/success: borrowers and collateral persisted` (~line 565)
- `TestProcessBlock/no loans found: no upserts` (~line 639)
- `TestProcessBlock/GetAllActiveLoansAtBlock error propagates` (~line 679)
- `TestProcessBlock/upsert borrowers error propagates` (~line 716)
- `TestProcessBlock/upsert borrower collateral error propagates` (~line 753)
- `TestProcessBlock/success: single transaction for users and positions` (~line 787)
- `TestProcessBlock/deduplication: same borrower across loans calls GetOrCreateUser once` (~line 861)
- `TestProcessBlock/user resolution error causes processBlock to fail` (~line 911)
- `TestProcessBlock/chain mismatch returns error` (~line 947)

**Step 3: Run the unit tests**

```bash
cd stl-verify && make test
```
Expected: all pass, no compile errors.

**Step 4: Commit**

```bash
git add stl-verify/internal/services/maple_indexer/service_test.go
git commit -m "test(maple-indexer): update tests to use outbound.BlockEvent"
```

---

## Task 3: Update `main.go` — use `NewConsumer` with `BaseEndpoint` in config

**Files:**
- Modify: `stl-verify/cmd/maple-indexer/main.go`

`main.go` currently (lines 111–123):
```go
var sqsOptFns []func(*sqs.Options)
if endpoint := env.Get("AWS_SQS_ENDPOINT", ""); endpoint != "" {
    sqsOptFns = append(sqsOptFns, func(o *sqs.Options) {
        o.BaseEndpoint = aws.String(endpoint)
    })
}

consumer, err := sqsadapter.NewConsumerWithOptions(awsCfg, sqsadapter.Config{
    QueueURL: cfg.queueURL,
}, logger, sqsOptFns...)
```

PR #77 added `BaseEndpoint` to `sqsadapter.Config` and `NewConsumer` handles it internally. Replace with:
```go
consumer, err := sqsadapter.NewConsumer(awsCfg, sqsadapter.Config{
    QueueURL:     cfg.queueURL,
    BaseEndpoint: env.Get("AWS_SQS_ENDPOINT", ""),
}, logger)
```

Remove now-unused imports:
- `"github.com/aws/aws-sdk-go-v2/aws"`
- `"github.com/aws/aws-sdk-go-v2/service/sqs"`

**Step 1: Apply the change**

Replace the `sqsOptFns` block + `NewConsumerWithOptions` call with the single `NewConsumer` call above.

Remove unused imports.

**Step 2: Build to confirm no compile errors**

```bash
cd stl-verify && go build ./cmd/maple-indexer/...
```
Expected: compiles cleanly.

**Step 3: Run all tests**

```bash
cd stl-verify && make test
```
Expected: all pass.

**Step 4: Commit**

```bash
git add stl-verify/cmd/maple-indexer/main.go
git commit -m "refactor(maple-indexer): use sqsadapter.NewConsumer with BaseEndpoint in config"
```

---

## Task 4: Final verification

**Step 1: Full CI check**

```bash
cd stl-verify && make ci
```
Expected: all checks pass (fmt, vet, lint, tests).

**Step 2: Verify deleted code is gone**

```bash
grep -rn "processLoop\|processMessages\|type blockEvent\|processMessage\|NewConsumerWithOptions" stl-verify/internal/services/maple_indexer/ stl-verify/cmd/maple-indexer/
```
Expected: no output.

**Step 3: Confirm sqsutil is used**

```bash
grep -rn "sqsutil.RunLoop" stl-verify/internal/services/maple_indexer/
```
Expected: one match in `service.go`.
