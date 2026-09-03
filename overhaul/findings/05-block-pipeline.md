Status: FINAL

# 05 — Core block pipeline (watcher → cache/SNS/SQS → workers, backfill/backup/validation)

## 1. Area map

The pipeline has one producer and two families of consumer.

```text
Alchemy WS ─► live_data.LiveService ─┐
                                     ├─► redis.BlockCache  (stl:{chain}:{num}:{ver}:{dataType})
Alchemy HTTP ─► backfill_gaps ───────┤   postgres.BlockStateRepository (block_states, reorg_events, backfill_watermark)
   (same process: cmd/base/watcher)  └─► sns.EventSink ── SNS FIFO ──┬─► SQS ─► 8 indexer workers  (sqsutil.RunLoop)
                                                                     └─► SQS ─► raw_data_backup   (own receive loop)
                                                                                    └─► s3.Writer (s3key: {part}/{num}_{ver}_{type}.json.gz)

out-of-band, same S3 layout, no DB: cmd/backfillers/raw-block-bulk-downloader, cmd/util/null-payload-refill
out-of-band, reads block_states only: data_validator (cmd/cronjobs/watcher-data-validator)
unrelated pipeline sharing the cmd/ neighbourhood: transform_worker / transform-bootstrap / gen-transformed
```

Wiring: `cmd/base/watcher/main.go` composes live + backfill over one pool/cache/sink.
`cmd/workers/raw-data-backup/main.go` composes the backup worker. Every indexer worker
composes `sqsutil.Config` + a `BlockEventHandler`. The block-event *contract* is
`outbound.BlockEvent` (a struct with json tags) plus the Redis key convention; nothing owns
either as a module.

`internal/common/sqsutil` is the consume-loop policy (poll cadence, chain filter, handler
budget, drain, settle metrics, release); `adapters/outbound/sqs` is the AWS translation.
`internal/common/` exists solely to hold `sqsutil` (`internal/pkg/` holds 25 other packages).

## 2. Metrics

| | |
|---|---|
| Packages in area | 25 (+ `adapters/outbound/postgres` partially) |
| Production files / lines | 49 / 13,940 (48/12,722 + `blockstate_repository.go` 1,218) |
| Test files / lines | 63 / 27,691 (**2.0× the production code**) |
| SQS consume-loop implementations | **2** (`sqsutil.RunLoop`, `raw_data_backup.Service.Run`) for 9 consumers |
| `BlockEvent` construct sites / decode sites | 3 / 2 |
| Ports implemented in area | `EventSink`, `SQSConsumer`, `DeadLetterPublisher`, `BlockCache`, `BlockCacheReader`, `S3Reader`, `S3RangeReader`, `S3Writer`, `S3Overwriter`, `CallArchiver`, `BlockVerifier`, `Repository`, `BlockStateRepository` |
| Ports with exactly one adapter | `SQSConsumer`, `DeadLetterPublisher`, `EventSink`(prod), `BlockCacheReader`, `BlockVerifier`(1 kind), `Repository`(0 callers) |
| Hand-rolled test doubles for `SQSConsumer` | **7** packages + `testutil.MockSQSConsumer` (used by 4 others) |
| Hand-rolled doubles for `BlockCache` | `memory.BlockCache` + `testutil.MockBlockCache` (incompatible signatures) + 2 in `raw_data_backup/service_test.go` |
| `lifecycle.Run` / `RunWithTimeoutGuard` / neither | 2 / 8 / 22 of 32 binaries |
| `lifecycle.SignalContext` vs raw `signal.NotifyContext` | 1 vs 21 |

Largest production files: `backfill_gaps_service.go` 1371 · `blockstate_repository.go` 1218 ·
`raw-block-bulk-downloader/main.go` 893 · `raw_data_backup/service.go` 852 ·
`data_validator/service.go` 657 · `memory/blockstate.go` 647 · `watcher/main.go` 561 ·
`null-payload-refill/main.go` 523 · `redis/blockcache.go` 469.

Largest functions (line count, incl. comments/blanks):

| Lines | Location |
|---|---|
| 163 | `transform_worker/service.go:70 RunOnce` |
| 146 | `null-payload-refill/main.go:197 Run` |
| 139 | `redis/blockcache.go:145 SetBlockData` |
| 126 | `backfill_gaps_service.go:265 findAndFillGaps` |
| 123 | `backfill_gaps_service.go:499 processBlockDataInner` |
| 111 | `cmd/workers/raw-data-backup/main.go:209 run` |
|  95 | `cmd/workers/raw-data-backup/main.go:99 parseConfig` |
|  89 | `backfill_gaps_service.go:969 cacheAndPublishBlockData` |
|  88 | `cmd/base/watcher/main.go:142 run` |
|  75 | `blockstate_repository.go:849 FindGaps` |
|  71 | `sns/eventsink.go:148 Publish` |
|  70 | `data_validator/service.go:544 spotCheckBlock`, `:447 validateSingleReorg` |
|  70 | `raw-block-bulk-downloader/main.go:811 reportProgress` |
|  66 | `backfill_gaps_service.go:1266 retryBlockPublish` |
|  65 | `backfill_gaps_service.go:872 recoverFromStaleChain` |
| (sibling) | `live_data_service.go:1046 cacheAndPublishBlockData` 111 |

`go build ./...` is clean.

---

## 3. Findings

### F05.1 — The block-event contract is a bare struct plus `json.Unmarshal`; one SNS misconfiguration silently deletes every block

**Strength**: Strong

**Files**
- `internal/ports/outbound/eventsink.go:42-107` (`BlockEvent`, `ParsedBlockHash`)
- `internal/common/sqsutil/process_loop.go:216-242` (`processMessage`, `discardForeignChainMessage`)
- `internal/services/raw_data_backup/service.go:525-534` (`parseEvent`, second decoder)
- construct sites: `live_data_service.go:1005-1015`, `backfill_gaps_service.go:1033-1042`, `null-payload-refill/refiller.go:304-312`
- `localstack-init/init-aws.sh:63,70,94`; `k8s/dev-infra/localstack.yaml` (8× `RawMessageDelivery=true`)

**Problem**
1. Decoding is `json.Unmarshal([]byte(msg.Body), &event)` into a plain struct. The contract
   therefore silently depends on the SNS subscription having `RawMessageDelivery=true`, an
   invariant that appears only in LocalStack scripts and test setup — never in Go, and never
   in a doc comment on `BlockEvent`. I verified that an SNS notification envelope
   (`{"Type":"Notification","Message":"…","Timestamp":"…"}`) unmarshals into `BlockEvent`
   with `err == nil` and **every field zero** (Go ignores unknown fields; no envelope key
   matches a tag or field name). `process_loop.go:225` then sees `event.ChainID (0) !=
   cfg.ChainID` and routes to `discardForeignChainMessage`, which **deletes the message**
   (`:241`). A subscription created without that flag would drain the whole block stream to
   nothing, with `ERROR chain ID mismatch, deleting message` as the only trace, no DLQ, and
   the loop reporting healthy. This is exactly the "poison pills get fixed or explicitly
   discarded, never silently skipped" rule inverted: an unparseable payload becomes an
   authoritative "wrong chain" verdict.
2. Unpacking the event is re-implemented per worker rather than owned by the contract:
   `event.ParsedBlockHash()` is called at **11** sites, each with its own error wrap
   (`aavelike:218`, `allocation_tracker:215,249`, `morpho_indexer:393`,
   `oracle_price_worker:443,512,538,560`, `prime_debt:212`, `psm3:222`);
   `time.Unix(event.BlockTimestamp, 0).UTC()` appears at **7** sites; and the
   read-receipts-for-this-event skeleton (get → `err != nil` → `== nil` → "receipts not found
   in cache for block %d (chain=%d, version=%d)") appears **6** times, with four different
   message wordings: `allocation_tracker:189-195`, `fluid_vault_indexer:332-338`,
   `morpho_indexer:385-391`, `aavelike:204-210`, `dexconsumer/block_processor.go:63-69`,
   `raw_data_backup:584-614`.
3. `shared.CacheKey` (`shared/utils.go:21`) hardcodes the `stl` prefix and is used for exactly
   one error message (`raw_data_backup/service.go:605`); the real key builder is
   `redis/blockcache.go:286`.

**Proposed change**
A `blockevent` module that owns the wire contract end to end:

```go
package blockevent
func Decode(body []byte) (Event, error)   // strict: rejects an SNS envelope, a zero chain ID,
                                          // a missing/malformed hash — the ParsedBlockHash
                                          // guard runs at the boundary, once
type Event struct{ ... }                  // accessors, not fields:
func (e Event) Chain() int64
func (e Event) Ref() blockref.Ref         // {ChainID, Number, Version, Hash common.Hash}
func (e Event) Timestamp() time.Time
```
plus `blockpayload.Reader` (a `BlockCacheReader` + `Ref`) exposing
`Receipts(ctx, ref) ([]shared.TransactionReceipt, error)` so the six copies of the
read-and-nil-check skeleton become one call. `sqsutil` calls `blockevent.Decode`; a decode
failure is a *parse* failure (leave for redelivery → DLQ via redrive), never a chain
mismatch. Chain-ID mismatch stays a delete but only for an event that decoded strictly.

**Benefits** Locality: one place knows how a block event is encoded, validated and turned into
a payload; the RawMessageDelivery invariant becomes a test in one package instead of an
undocumented infra fact. Leverage: a worker's handler starts from a validated `Ref`.
Tests: the envelope case, zero chain, bad hash and missing receipts get one table-driven test
instead of being untestable per worker.

**Risk / migration** Land `Decode` + the envelope rejection first (behaviour-preserving for
well-formed messages, S). Then `Ref`/`blockpayload` behind the existing accessors, migrating
workers one per PR.

**Size** L (2–4 PRs) — the strict decoder alone is S.

**Enables** F05.4, F05.12.

---

### F05.2 — `raw_data_backup` re-implements the consume loop and cannot use the shutdown seam

**Strength**: Strong

**Files**
- `internal/services/raw_data_backup/service.go:255-385` (`Run`, `fetchAndDispatch`, `stopFetching`, `holdForRelease`, `takeHeldMessages`, `releaseIfShuttingDown`, `dispatch`, `Stop`, `worker`)
- vs `internal/common/sqsutil/process_loop.go:122-285`
- `cmd/workers/raw-data-backup/main.go:70` (raw `signal.NotifyContext`), `:314` (`service.Run` direct)
- `internal/services/raw_data_backup/service.go:387-513` (`processAndSettle` … `deleteMessage`)

**Problem** It is the only one of nine consumers not on `sqsutil.RunLoop`, and it duplicates
the loop's whole settle policy: `handleResult:434` mirrors `settleMessage:251`,
`keepForRedelivery:453` mirrors `keepMessageForRedelivery:258`, `handlerTimeout()/drainTimeout()`
at `:397-409` duplicate `Config.handlerTimeout()/drainTimeout()` at `process_loop.go:73-90`,
and `parseEvent:525` is a second `BlockEvent` decoder. Its own comment concedes the cost:
"it is the only worker that settles outside ProcessMessages" (`:505-506`).

Consequences of the fork:
- `Service` exposes `Run(ctx) error` + `Stop()` (no error) instead of
  `Start(ctx) error`/`Stop() error`, so it **cannot satisfy `lifecycle.Service`** — hence the
  comment at `:256-257` ("runs Run directly rather than under lifecycle.Run") and the missing
  `lifecycle.ForceExitAfter` guard that all eight sibling workers arm
  (e.g. `cmd/workers/morpho-indexer/main.go:47`).
- `Stop()`/`stopCh` have **21 test call sites and zero production callers**: a test-only API
  keeping a production service off the shared seam.
- Two callers of `sqsutil.ValidateVisibilityTimeout` disagree on "in flight":
  `process_loop.go:63` passes `MaxMessages`, `service.go:243` passes
  `ceil(BatchSize/Workers)` (`inFlightPerReceive:250`). The weaker bound is justified in a
  comment, but the invariant now lives in two places.
- `cmd/workers/raw-data-backup/main.go:295-299` **swallows** a `telemetry.NewMetrics` failure
  ("Proceed without metrics"), where `fluid-vault-indexer/main.go:267` and
  `prime-allocation-indexer/main.go:294` return the error, and the watcher's own
  `openDependencies` (`cmd/base/watcher/main.go:383-388`) fails hard with the rationale
  "continuing here would mean running blind forever".

**Proposed change** Give `sqsutil` the one capability the backup worker actually needs — a
worker pool — as `sqsutil.Config.Concurrency` (default 1), and a handler result type carrying
an outcome label plus a `Permanent` classification so `ErrPermanent`/DLQ routing moves into
`settleMessage`. The backup service then becomes a `BlockEventHandler` and gets
`Start`/`Stop`, `lifecycle.RunWithTimeoutGuard`, and the force-exit guard for free.

**Benefits** Locality: one settle/drain/release policy for nine consumers instead of two.
Leverage: DLQ routing and per-outcome status labels become available to every worker.
Tests: `service_test.go` (4,082 lines) loses its loop/shutdown scenarios to
`process_loop_test.go`, which already covers them.

**Risk / migration** Shutdown semantics are the risk. Add `Concurrency` + `Permanent` to
`sqsutil` with the existing single-message behaviour unchanged (PR 1), port the backup worker
behind an integration test that asserts the current release-on-SIGTERM behaviour (PR 2).

**Size** M–L

---

### F05.3 — `cacheAndPublishBlockData` exists twice with divergent failure policy

**Strength**: Strong

**Files**
- `internal/services/live_data/live_data_service.go:1046-1156` (111 lines) + `publishBlockEvent:991-1042`
- `internal/services/backfill_gaps/backfill_gaps_service.go:969-1057` (89 lines)

**Problem** Both perform the identical seven steps: parse `header.Timestamp` via
`hexutil.ParseInt64`; check `BlockErr`/`ReceiptsErr`/`TracesErr` (if `EnableTraces`)/`BlobsErr`
(if `EnableBlobs`); `rpcutil.IsNullOrEmpty` the same four payloads; build
`outbound.BlockDataInput` conditionally on the same two flags; `cache.SetBlockData`; build
`outbound.BlockEvent`; `eventSink.Publish`; `stateRepo.MarkPublishComplete`. The backfill copy
even documents the coupling — "mirroring live_data's order" (`:983-984`) — which is a
maintenance instruction, not an abstraction.

They diverge on the step that matters:

| | live_data | backfill_gaps |
|---|---|---|
| `MarkPublishComplete` failure | `logger.Warn`, **returns nil** (`:1033-1035`) | `return fmt.Errorf(...)` (`:1052-1054`) |
| Blobs in cache input | only when `EnableBlobs` (`:1138-1140`) | always assigned (nil when disabled, `:1023`) |
| Spans | per-step child spans | none |

Both divergences are commented as deliberate, but they mean "did this block get published"
has two answers depending on which service handled it — and the retry loop that consumes
`block_published` is in the *backfill* service.

**Proposed change** One `blockpublisher` module owning the sequence:

```go
type Publisher struct{ cache, sink, state, cfg }
func (p Publisher) Publish(ctx, ref blockref.Ref, bd outbound.BlockData, origin Origin) error
```
`Origin` (`Live` / `Backfill` / `Refill`) selects the `IsReorg`/`IsBackfill` flags and the
`MarkPublishComplete` policy, so the divergence is one enumerated decision instead of two
code paths. Validation of `bd` moves behind F05.4's payload type.

**Benefits** Locality: the "cache everything, then publish, then mark" ordering invariant —
the thing that keeps the retry loop honest — lives once. Leverage: `null-payload-refill`
(`refiller.go:285-320`) becomes a third caller instead of a third partial copy.
Tests: the null-payload (VEC-242) matrix is tested once rather than in both
`live_data_service_test.go` and `backfill_gaps_service_test.go`.

**Risk / migration** Pick one `MarkPublishComplete` policy explicitly (a decision, not a
refactor) or keep both behind `Origin`. Land as a pure extraction with both services'
existing tests unchanged.

**Size** M

**Depends on** F05.4 · **Enables** F05.1

---

### F05.4 — `BlockData`'s four-payloads-plus-four-errors shape forces the same four-way unroll in seven places

**Strength**: Strong

**Files**
- `internal/ports/outbound/blockchain_client.go:9-21` (`BlockData`)
- `internal/services/raw_data_backup/service.go:539-567` (`fetchExpectedData`), `:676-688` (`expectedTypes`), `:745-779` (`fetchFromRPC`), `:806-828` (`backUpToS3`)
- `internal/services/backfill_gaps/backfill_gaps_service.go:985-1027`
- `internal/services/live_data/live_data_service.go:1068-1140`
- `cmd/util/null-payload-refill/refiller.go:369-381` (`pickField`)
- `cmd/backfillers/raw-block-bulk-downloader/dispatch.go:171-180` (`payloadFor`)
- `internal/adapters/outbound/cache/reader_with_fallback.go:69-92` (four one-line wrappers)
- `internal/adapters/outbound/redis/blockcache.go:308-386` (four setters, four getters)

**Problem** "A block has four data types; each has a payload and an error; some are optional
per chain/flag" is encoded seven times, three different ways: an unrolled `if` chain (backup,
backfill, live), a `switch` on a type tag (`pickField`, `payloadFor`), and method values
(`fetchExpectedData`, `getWithFallback`). Adding a fifth data type is a change in every one.
The type tag is also inconsistently typed: `raw_data_backup.expectedTypes` returns
`[]string{"block","receipts",...}` (`:677-687`) and converts to `s3key.DataType(dataType)` at
`:832`, while `redis.BlockCache.key` takes a bare `string` (`:286`) — so the `s3key.DataType`
enum is bypassed on the hottest path.

**Proposed change** Make the data-type set a first-class type and index the payloads by it:

```go
type DataType uint8            // Block, Receipts, Traces, Blobs — one enum, in domain
type Payloads map[DataType]json.RawMessage
func (b BlockData) Payload(dt DataType) (json.RawMessage, error)  // payload+err+null in one
func (b BlockData) Require(dts ...DataType) error                 // replaces every unrolled chain
```
`ChainExpectation` (`raw_data_backup/service.go:58-83`) becomes `[]DataType`; `s3key.DataType`
and the Redis `dataType` string both alias the one enum.

**Benefits** Leverage: `Require(Block, Receipts, Traces)` replaces 20–40 lines at each of five
sites. Locality: null/empty classification (the VEC-242 rule) is enforced by the accessor, not
by every caller remembering `rpcutil.IsNullOrEmpty`. Tests: one table over the enum.

**Risk / migration** `BlockData` is produced by the alchemy adapter and consumed widely; add
the accessors alongside the fields, migrate callers, then remove direct field reads.

**Size** M

**Enables** F05.3

---

### F05.5 — `BlockStateRepository` is a 25-method god port over six concerns, with a second, drifting adapter used only by tests

**Strength**: Strong

**Files**
- `internal/ports/outbound/blockstate.go:81-237` (25 methods, 157 lines — 90 of them doc comment)
- `internal/adapters/outbound/postgres/blockstate_repository.go` (1,218 lines, 44 functions)
- `internal/adapters/outbound/memory/blockstate.go` (647 lines) + `blockstate_test.go` (600 lines testing the double itself)
- consumers: `backfill_gaps_service.go:94`, `live_data_service.go:87`, `data_validator/service.go:56`

**Problem**
The port bundles six unrelated responsibilities, and each of the three consumers uses a
different subset:

| Concern | Repository lines | Used by |
|---|---|---|
| Block identity + version assignment | 56-340 | live, backfill, validator |
| Orphan / reorg commit | 341-740 | live, backfill |
| Backfill cursor / watermark | 761-844 | backfill, validator (read) |
| Gap detection | 740-760, 846-974 | backfill, validator |
| Chain-integrity **verification** | 975-1110 | validator only |
| Publish tracking | 1111-1218 | live, backfill |

This directly contradicts `stl-verify/AGENTS.md` ("Interface Segregation … Prefer multiple
small interfaces"). Concrete costs:
- `data_validator` must be handed the full 25-method port to use 7 methods, so
  `service_test.go` hand-rolls a 25-method `mockBlockStateRepository`.
- Dead port surface: `GetBlockVersionCount` (`:110`) has **zero** production callers;
  `GetBackfillWatermark` (`:158`) is called only by the postgres adapter's own `FindGaps`
  (`:855`); `GetReorgEvents` (`:683`, 27 lines) is not on the port at all and is called only
  by `cmd/base/watcher/main_integration_test.go:1228`.
- The `memory` adapter is a **second implementation of the same semantics**, and it has
  drifted:
  - `postgres.SaveBlock:62-64` rejects `BlockTimestamp == 0`; `memory.SaveBlock:46-60` accepts
    it. Unit tests can therefore build states production refuses.
  - `postgres.FindGaps:875-920` never reports a trailing gap above the highest canonical row
    (the `LAG` window plus a leading-gap fixup); `memory.FindGaps:430-433` **does**
    (`if gapStart >= 0 { append {gapStart, maxBlock} }`). Every `backfill_gaps` unit test runs
    against the more generous model.
  - `memory` has no `chain_id` scoping at all, while every postgres query filters on it.
- `blockstate_repository.go`'s own header comment is wrong: it advertises "upsert semantics
  (ON CONFLICT UPDATE)" (`:5`) where the code uses `ON CONFLICT DO NOTHING` (per
  `db/migrations/AGENTS.md`'s strict append-only rule).

**Proposed change** Split the port along the concerns above — `BlockStore` (identity+version),
`ReorgJournal`, `BackfillCursorStore`, `GapScanner`, `ChainIntegrityChecker`,
`PublishLedger` — implemented by the one postgres type (Go structural typing means no adapter
change), and split the 1,218-line file to match. Then delete `memory.BlockStateRepository`:
`backfill_gaps` already has 1,808 lines of integration tests against real Postgres, so the
unit tests either move there or take a narrow fake of the small port they need.

**Benefits** Locality: reorg/watermark invariants stop being spread across a file that also
holds gap SQL and publish flags. Leverage: `data_validator` depends on ~7 methods, so its mock
shrinks by ~18 methods. Tests: one implementation of `FindGaps` semantics, so a unit test
cannot pass against a model production disagrees with.

**Risk / migration** Deleting the memory adapter is the risky half; do the port split and the
`memory`-vs-`postgres` behaviour reconciliation first (fix `FindGaps` trailing gap and the
`BlockTimestamp` guard, PR 1), then retire it per test file.

**Size** L (2–4 PRs)

**Depends on** F05.7

---

### F05.6 — `backfill_gaps_service.go` (1,371 lines) is five concerns, and its top-level loops log-and-continue

**Strength**: Strong

**Files** `internal/services/backfill_gaps/backfill_gaps_service.go`

**Problem** Five separable concerns in one type:
1. two poll loops + tracing wrappers (`:166-262`, `:419-437`, `:1224-1231`);
2. gap fill (`findAndFillGaps:265-390` 126 lines, `fillGap`, `processBatch`,
   `processBlockDataInner:499-621` 123 lines);
3. orphan self-healing (`unorphanIfOnCanonicalChain:639`, `findCanonicalAnchorAbove`,
   `walkStoredChainDownTo:675`, `assertCanonicalRowExists:713`, `validateBlockLinkage:761`);
4. stale-chain / boundary recovery (`verifyBoundaryBlocks:803`, `recoverFromStaleChain:872`,
   `orphanEmptiedHeight`, `rewindWatermarkBelow`);
5. watermark advance (`advanceWatermark:1061` … `reportSkippedAdvance:1159`);
6. publish retry (`retryIncompletePublishes:1224`, `retryBlockPublish:1266`,
   `reconcileOrphanedRetry:1346`).

`findAndFillGaps` is a 126-line body with three comment-delimited sections and inline span
plumbing — the exact shape `AGENTS.md`'s function-composition rule names as a defect.
`processBlockDataInner` is a 123-line linear chain of eight guards.

Error handling contradicts the repo's own rule ("A partial failure stops the whole
event/block", "Never swallow a failure into partial success"). Five top-level loops
log-and-continue:
- `:300-304` boundary verification fails → `Warn` → "Continue anyway";
- `:369-380` a gap fails → `Warn` → "Continue with other gaps";
- `:385-387` watermark advance fails → `Warn`, pass returns nil;
- `:408-411` a batch fails → `Warn` → "Continue with next batch";
- `:456-460` a block fails → `Warn` → "Continue with other blocks";
- `:908-926` a canonical replacement fetch fails → `continue`.
A pass therefore returns `nil` having filled nothing, and the only signal is
`RecordWatermarkLag` (`:1094`). `assertCanonicalRowExists` (`:713-752`) exists precisely
because this pattern once hid a 26-day incident; the fix added an observer rather than closing
the swallow.

Also dead-ish: `findGapsFromBlock:1178-1203` opens with "For now, we'll use a simplified
approach", and its helper `getFirstBlockInRange:1206-1219` can only return `fromBlock` or
`toBlock+1` — i.e. it never finds "the first block in range", so `findContiguousTarget:1108`
re-derives the leading gap the repository already computes at
`blockstate_repository.go:912-920`.

**Proposed change** Split the file into `gapfill.go` / `healing.go` / `boundary.go` /
`watermark.go` / `retry.go` behind the existing `BackfillService`, and give each loop an
explicit outcome type (`passResult{filled, failed int; firstErr error}`) so a pass that filled
nothing returns an error instead of `nil`. Delete `findGapsFromBlock`/`getFirstBlockInRange`
in favour of the repository's own leading-gap handling.

**Benefits** Locality: reorg-healing rules stop sharing a file with poll cadence. Leverage: a
typed pass result lets the alert key on "pass made no progress" instead of on lag 1,000 blocks
later. Tests: `backfill_gaps_service_test.go` (2,950 lines) splits along the same seams.

**Risk / migration** File split first (no behaviour change, S). Then per-loop outcome types,
one loop per PR — each changes alerting behaviour, so pair with `alerts/`.

**Size** L

---

### F05.7 — `adapters/outbound/memory` is 1,599 lines of test-only code in the production adapter tree, and `Repository` is untouched scaffolding

**Strength**: Strong

**Files**
- `internal/adapters/outbound/memory/{blockstate.go 647, blockstate_test.go 600, blockcache.go 163, eventsink.go 153, repository.go 36}`
- `internal/ports/outbound/repository.go:9-15`

**Problem** Every import of `adapters/outbound/memory` is from a `_test.go` file — 7 files,
zero production. So the production adapter tree carries a test double that has its own
600-line test suite validating the double (not the port), and the double has drifted from the
real adapter (F05.5).

`memory/repository.go` + `outbound.Repository` are unmodified generator scaffolding:

```go
// internal/ports/outbound/repository.go:9-15
type Repository interface {
	// Add your repository methods here
	// Example:
	// Save(ctx context.Context, entity *entity.Verification) error
	HealthCheck(ctx context.Context) error
}
```
One adapter, zero callers of `NewRepository`, zero callers of `HealthCheck`. Deletion test:
complexity vanishes entirely.

`memory.BlockCache` also duplicates `testutil.MockBlockCache` for the same port with
*incompatible* signatures (`memory.SetReceipts(ctx, chain, num, ver, data) error` vs
`testutil.MockBlockCache.SetReceipts(chain, num, ver, data)` with no ctx and no error), so a
test switching doubles must rewrite its setup.

**Proposed change** Delete `memory/repository.go` and `outbound.Repository`. Move the
remaining three memory adapters to `internal/testutil/` (or `internal/testutil/fake/`) and
merge `memory.BlockCache` with `testutil.MockBlockCache` into one double with one signature.
Then `adapters/outbound/` contains only things that talk to real infrastructure — which is
what the hexagonal layout claims.

**Benefits** Locality: one in-memory `BlockCache` instead of two-plus-two. Leverage: a
production-tree grep for "who implements BlockCache" stops returning a fake. Tests: the
double's 600-line self-test either becomes a shared port contract test (run against both
postgres and the fake) or goes away.

**Risk / migration** Mechanical: move + rename imports in 7 test files. Deleting
`outbound.Repository` touches 2 files.

**Size** S (scaffolding) + M (move + double merge)

**Enables** F05.5

---

### F05.8 — `pkg/lifecycle` is a hypothetical seam: `SignalContext` has 1 of 22 possible users, `Run` has 2 of 32

**Strength**: Strong

**Files** `internal/pkg/lifecycle/lifecycle.go:31-43,107-139`; `cmd/base/watcher/main.go:73`

**Problem** `AGENTS.md` states "Every binary extracts a `run(ctx, args) error` … and runs
under one of three entry points". Actual counts across 32 `main.go` files:

| Entry point | Binaries |
|---|---|
| `lifecycle.Run` | 2 (`cmd/base/watcher`, `cmd/util/stress-test/mock-blockchain-server`) |
| `lifecycle.RunWithTimeoutGuard` | 8 (the indexer workers) |
| `temporal.RunCronjob` / `RunWorker` | 10 |
| none of the three | 12, including `cmd/workers/raw-data-backup` (`main.go:314` calls `service.Run` directly) |

`lifecycle.SignalContext` exists specifically to log *which* signal arrived
(`lifecycle.go:116-119`) — a stated operational need — and exactly one binary uses it; 21 use
raw `signal.NotifyContext` and get no such log. `Run` and `RunWithTimeoutGuard` differ only in
one nil-able callback (`:32` vs `:42`), i.e. `Run` is `RunWithTimeoutGuard(…, nil, …)`.

**Proposed change** Collapse `Run` into `RunWithTimeoutGuard` (keep one name), and either
adopt `SignalContext` in every non-Temporal `main` (it is a one-line swap) or delete it and
its 184-line test file. Pick one; a seam with one adapter and one caller is not a seam.

**Benefits** Locality: "how does this process shut down" has one answer. Leverage: the signal
log and the force-exit guard reach all 12 binaries that currently have neither.

**Risk / migration** Trivially incremental, one binary per commit.

**Size** S

---

### F05.9 — The Redis cache-key convention is spread across 6 composition roots, 2 of which cannot be namespaced

**Strength**: Strong

**Files**
- builder: `internal/adapters/outbound/redis/blockcache.go:286-288`, default `:64`
- reads `REDIS_KEY_PREFIX`: `cmd/workers/morpho-indexer/main.go:210`, `oracle-price-indexer/main.go:178`, `prime-allocation-indexer/main.go:227`, `sparklend-indexer/main.go:208`
- hardcodes `"stl"`: `cmd/base/watcher/main.go:317`, `cmd/workers/fluid-vault-indexer/main.go:222`, `cmd/workers/internal/dexbootstrap/bootstrap.go:165`, `cmd/workers/raw-data-backup/main.go:226`
- third encoding: `internal/services/shared/utils.go:21`

**Problem** The key convention is documented in `AGENTS.md` and in three doc comments, but
enforced nowhere. Four composition roots honour `REDIS_KEY_PREFIX`; four hardcode `"stl"`. The
AGENTS.md testing rule says an integration test that drives a binary "hands the binary a
namespace to build [keys] from: `REDIS_KEY_PREFIX` for the cache key" — which works for four
workers and is silently ignored by the watcher, fluid-vault-indexer, dexbootstrap and the
backup worker, so those tests share a keyspace with anything else on the same Redis. In
production the prefix cannot be changed at all without touching four files.

**Proposed change** One constructor, `rediscache.NewBlockCacheFromEnv(logger)` (mirroring the
existing `s3.NewReaderFromEnv`, whose doc comment records the identical bug: "Six worker
entrypoints previously inlined this same block; sparklend-indexer omitted it and its S3
fallback was silently unreachable in dev"). It owns `REDIS_ADDR`, `REDIS_PASSWORD`, `TTL` and
`REDIS_KEY_PREFIX`. Delete `shared.CacheKey` (replace its one use with the cache's own
`Key(...)` accessor, or just drop the key from the error message).

**Benefits** Locality: one place knows the key shape and the prefix source. Leverage: test
isolation works uniformly. Tests: `NewBlockCacheFromEnv` gets one test for prefix resolution
instead of four untested inline blocks.

**Risk / migration** Additive constructor, then one worker per commit.

**Size** S

---

### F05.10 — `transform_worker.RunOnce` is a 163-line function whose two drain passes are near-duplicates

**Strength**: Strong

**Files** `internal/services/transform_worker/service.go:70-232`

**Problem** One function holds: source listing and the empty-list guard (`:71-77`); budget and
deadline setup (`:88-90`); mutable accumulators (`:92-98`); a 29-line closure `drain` that
mutates four of them (`:103-131`); first pass (`:135-155`); second pass (`:159-185`); budget
warning (`:187-190`); per-source metric emission (`:196-212`); two backstop reads (`:218-223`);
and joined-error return. The two passes are the same loop written twice — `:141-151` vs
`:171-179`, differing only in `sources`/`backlog` and the `next` bookkeeping.

**Proposed change** Extract a `drainScheduler` type owning `deadline`, `states`, `minDrainSlice`
and the `drain` closure's state, exposing `Serve(sources []string) (backlog []string, err
error)`; `RunOnce` becomes `list → serve until budget spent → record → backstops`, and the two
passes become one `for` over `Serve`.

**Benefits** Locality: budget arithmetic and per-source accumulation stop being interleaved
with metric emission. Tests: the scheduler is testable without a `TransformRunner`.

**Risk / migration** Pure extraction; `service_test.go` (361 lines) already covers the budget
and backlog cases.

**Size** S

---

### F05.11 — `data_validator` is a `CheckResult` formatter over predicates that already live in the repository

**Strength**: Worth exploring

**Files**
- `internal/services/data_validator/service.go:250-412` (integrity + orphan checks)
- `internal/adapters/outbound/postgres/blockstate_repository.go:975-1110` (`VerifyChainIntegrity`, `VerifyParentLinks`, `verifyOrderedPairs`, `orderedPairScan`, `verifyRangeReachesEnd`), `:928-974` (`FindOrphanOnlyHeights`)
- `cmd/cronjobs/watcher-data-validator/main.go:89-108`

**Problem** Two of the four checks are already repository properties; the service only wraps
`error` in a `CheckResult`. `validateNoOrphanOnlyHeights:361-397` is 37 lines of which 30 are
three `CheckResult` literals around one repository call. `validateChainIntegrity:250-285` plus
its four helpers (`watermarkAboveDataFailure`, `verifyParentLinksOnly`, `verifyChainOver`,
`chainValidMessage`) are 100 lines of watermark-bound arithmetic that the repository is better
placed to express, since it already owns both the watermark and the ordered-pair scan.
Meanwhile the genuinely service-level part — comparing stored hashes to an independent
canonical source (`validateSingleReorg:447`, `spotCheckBlock:544`, 140 lines) — is where the
real logic is, and those two functions are 70 lines each of near-identical
fetch/nil-check/compare/format.

Also: `Report.Finalize()` is called twice — `service.go:137` and
`cmd/cronjobs/watcher-data-validator/main.go:94` — because the cronjob has no way to know the
service already finalised.

**Proposed change** Move the watermark-bounded integrity check into the repository as one
method returning a typed result (`IntegrityReport{ViolationKind, Height, ...}`) rather than an
`error` string; keep `data_validator` for the canonical-source comparison only, and factor
`validateSingleReorg`/`spotCheckBlock` into one `compareAgainstCanonical(ctx, height,
expectedHash, name)` helper. Make `Finalize` idempotent or private.

**Benefits** Locality: "is the stored chain whole" is answered by the thing that stores it.
Leverage: the same predicate becomes usable from the backfill service's post-pass assertion
(`assertCanonicalRowExists`) instead of a second hand-rolled check. Tests: hash comparison
gets one table instead of two 70-line functions.

**Risk / migration** The `CheckResult` message strings are quoted by runbooks/alerts, so
preserve wording. Land the `compareAgainstCanonical` extraction first (S).

**Size** M

**Depends on** F05.5

---

### F05.12 — Three independent notions of "the version at height N"; the bulk downloader derives it from S3 with a hand-rolled JSON scanner and never reads `block_states`

**Strength**: Worth exploring

**Files**
- authority: `postgres/blockstate_repository.go:111-165` (`saveBlockOnce`, trigger-assigned version)
- on the wire: `outbound.BlockEvent.Version` → `raw_data_backup/service.go:593`, `cache/reader_with_fallback.go:114` (`s3key.Build(blockNumber, version, dataType)`)
- re-derived from the archive: `cmd/backfillers/raw-block-bulk-downloader/plan.go:52-55` (`archiveState.Version`), `:113-134` (`indexPartition`), `:150-207` (`archivedBlockHash`, `hashFromArchivedObject`, `gunzipPrefix`), `:219-279` (`jsonStringField`, `scanJSONStringField` — 51 lines of hand-written JSON string scanning), `:382-397` (`PartitionCache.TopVersion`), `:411-446`
- `cmd/backfillers/raw-block-bulk-downloader/*.go` contains no `postgres`/`pgxpool`/`DATABASE_URL` reference

**Problem** The bulk downloader writes to the same bucket and the same `s3key` layout as the
backup worker, but chooses its version by listing the S3 partition for the highest existing
object version and comparing the archived block's hash (read from the first 8 KB of the gzip
prefix, via a bespoke scanner) against the RPC hash. The S3-fallback reader
(`cache/reader_with_fallback.go:113-141`) looks objects up by the **event** version, i.e. the
DB-trigger version. If those two numbering schemes ever disagree at a height, the fallback
returns `nil, nil` (a miss), and the consuming worker turns that into a hard error — which for
the six workers using the fallback is a FIFO head-of-line stall
(`dexconsumer/block_processor.go:68-69`).

`archivedBlockHash` also carries risk that a `block_states` read would remove entirely: the
hash is parsed out of a truncated gzip prefix with a hand-rolled scanner rather than
`encoding/json`.

**Proposed change** Give the downloader read-only access to `block_states` (the version and
hash it needs are two columns) behind a small port — the same `BlockStore` slice F05.5
proposes — and delete `archivedBlockHash`/`hashFromArchivedObject`/`gunzipPrefix`/
`jsonStringField`/`scanJSONStringField` (~150 lines) plus the hash-source registry. Keep
`PartitionCache` for "what objects already exist"; take the version from the DB.

**Benefits** Locality: one authority for a block's version. Leverage: ~150 lines of prefix
parsing and a 51-line JSON scanner go away. Tests: `plan_test.go` (375 lines) loses the
prefix-scanning cases.

**Risk / migration** Needs confirmation that the tool is always run where `block_states` is
reachable and populated for the target range (see Open questions). If not, the fallback path
stays but the version is cross-checked and a mismatch is a hard error rather than silent.

**Size** M–L

**Depends on** F05.5

---

### F05.13 — Hash handling: three helpers, and the backfill's canonical-vs-stored comparison is case-sensitive against its own comment

**Strength**: Strong

**Files**
- `internal/services/backfill_gaps/backfill_gaps_service.go:851-860` and `:790-795` (`truncateHash`)
- `internal/services/live_data/live_data_service.go:533-540` (`truncateHashLive`), `:1161-1163` (`normalizeHash`)
- `internal/services/data_validator/service.go:653-657` (`hashesMatch`)
- `internal/adapters/outbound/alchemy/types.go:52` (a fourth `truncateHash`)

**Problem** `verifyBoundaryBlocks` compares the RPC hash to the stored hash with `!=`:

```go
// Compare Hash: Canonical (RPC) vs Database
// Note: Case-insensitive comparison is safer for hex strings
if header.Hash != dbBlock.Hash {                       // :853 — case-SENSITIVE
```

The comment states the requirement and the code does the opposite. `live_data` has
`normalizeHash` for exactly this, `data_validator` has `hashesMatch` (which also trims `0x`),
and neither is used here — so the same domain question ("are these the same block?") has three
answers, and the backfill's is the one that would orphan a canonical block if a node ever
returned mixed-case hex. `truncateHash` exists in four packages with two different formats.

**Proposed change** A `blockhash` package (or extend `internal/pkg/hexutil`) with
`Equal(a, b string) bool` and `Short(h string) string`, and use `common.Hash` (already the
type `ParsedBlockHash` returns) wherever a hash crosses a module boundary — comparison then
cannot be wrong. Delete the four local helpers.

**Benefits** Locality: hash equality defined once. Leverage: eliminates a class of silent
orphaning. Tests: one table for equality/format.

**Risk / migration** `Equal` + call-site swap is a single small PR; the `common.Hash` typing is
a follow-on and pairs with F05.1's `Ref`.

**Size** S

---

### F05.14 — Dead and mis-documented port surface on `BlockCache` and `BlockStateRepository`

**Strength**: Strong

**Files**
- `internal/ports/outbound/blockcache.go:46-47` (`DeleteBlock`), `internal/ports/outbound/blockstate.go:110` (`GetBlockVersionCount`), `:158` (`GetBackfillWatermark`)
- `internal/adapters/outbound/redis/blockcache.go:308-386` (`SetBlock`/`SetReceipts`/`SetTraces`/`SetBlobs`), `:458` (`DeleteBlock`)

**Problem** `DeleteBlock`'s doc says "(used on reorg)". It has **zero** production callers:
two adapter implementations, four test doubles, and integration tests. The reorg path does not
delete the superseded version's cache entries at all — they age out on the 2 h TTL — so the
port documents an invariant the system does not have. `GetBlockVersionCount` has zero
production callers; `GetBackfillWatermark` is called only from inside the postgres adapter.
The four individual Redis setters have zero production callers (production uses only
`SetBlockData`) and exist purely so tests can wrap them
(e.g. `backfill_gaps_service_test.go:879-905`).

**Proposed change** Delete `DeleteBlock`, `GetBlockVersionCount` and `GetBackfillWatermark`
from the ports (keep the watermark read private to the adapter); make the four Redis setters
unexported or replace the tests' partial-failure wrapping with a `SetBlockData` fake that fails
a named data type.

**Benefits** Locality: the port stops asserting a reorg-eviction behaviour that does not
exist. Leverage: four test doubles shrink. Tests: `redis/blockcache_test.go` and the
`failingCache`/`mockFailingCache` doubles in two services get smaller.

**Risk / migration** Mechanical.

**Size** S

**Depends on** F05.7 (double consolidation)

---

### F05.15 — gzip is implemented three times for decompression and twice for compression, and one path double-decompresses

**Strength**: Worth exploring

**Files**
- `internal/pkg/gziputil/gziputil.go:13-28` (`Decompress`, `IsGzipped` — no `Compress`)
- `internal/adapters/outbound/redis/blockcache.go:292-306` (`compress`, `gzip.BestSpeed`)
- `internal/adapters/outbound/s3/writer.go:74-95` (`prepareBody`, default level)
- `internal/adapters/outbound/s3/reader.go:159-182` (gzip-on-`.gz` in `StreamFile`) + `:204-221` (`gzipReadCloser`)
- `cmd/backfillers/raw-block-bulk-downloader/plan.go:193-207` (`gunzipPrefix`)
- double-decompress: `internal/adapters/outbound/cache/reader_with_fallback.go:129-137`, `cmd/util/null-payload-refill/refiller.go:349-358`

**Problem** `gziputil` exists but owns only the read half, so both writers hand-roll
compression at *different levels* (BestSpeed for Redis, default for S3 — an undocumented
asymmetry). `StreamFile` already decompresses `.gz`, yet both of its callers run
`gziputil.Decompress` over the result again, each with a comment explaining the belt-and-braces
("Defensive: also pass through gziputil in case the bucket layout changes",
`refiller.go:353-354`) — i.e. neither caller trusts the adapter's contract.

**Proposed change** Give `gziputil` `Compress(data []byte, level int) ([]byte, error)` and use
it from both writers with the level named as a constant per store. Decide `StreamFile`'s
contract once — either it always returns plain bytes (then delete both defensive
decompressions) or it never decompresses (then the callers own it) — and state it in the port
doc rather than in caller comments.

**Benefits** Locality: one compression implementation, one documented decompression contract.
Leverage: `gunzipPrefix`'s truncated-stream handling becomes shareable.

**Size** S

---

### F05.16 — Seven hand-rolled `SQSConsumer` doubles alongside the shared one

**Strength**: Strong

**Files**
- shared: `internal/testutil/mock_sqs_consumer.go` (used by `aavelike_position_tracker`, `allocation_tracker`, `morpho_indexer` ×2)
- hand-rolled: `internal/common/sqsutil/testhelpers_test.go`, `internal/services/fluid_vault_indexer/testhelpers_test.go`, `internal/services/oracle_price_worker/{service_test.go,e2e_integration_test.go}`, `internal/services/prime_debt/service_test.go`, `internal/services/psm3/service_test.go`, `internal/services/raw_data_backup/service_test.go`

**Problem** A five-method port with one production adapter has eight test doubles. Each one
re-decides what `VisibilityTimeout()` returns (the shared one defaults to 300 s,
`mock_sqs_consumer.go:22`), whether `ChangeMessageVisibilityBatch` records handles, and how a
refusal is expressed — so the visibility/release contract that `sqsutil` enforces is asserted
against eight slightly different models. Test lines in this area are 2.0× production lines
(27,691 vs 13,940), and this is a visible part of why.

**Proposed change** One `testutil.FakeSQSQueue` implementing `SQSConsumer` *and* the
`DeadLetterPublisher` with real queue semantics (in-flight set, visibility clock, receive
count), plus a port contract test run against both it and the real adapter under LocalStack.
Delete the seven bespoke doubles.

**Benefits** Locality: one model of SQS behaviour. Leverage: the drain/release/DLQ scenarios
become writable in any worker's test. Tests: `raw_data_backup/service_test.go` (4,082 lines)
and `sqsutil/process_loop_test.go` (953) both shrink.

**Risk / migration** Introduce the fake, migrate one package per PR.

**Size** M

**Depends on** F05.2 (the backup worker's doubles disappear with its loop)

---

## 4. Cross-area observations

- **Dead inbound scaffolding.** `internal/adapters/inbound/http/` (`handler.go` 57,
  `health.go` 182, `health_test.go` 210), `internal/ports/inbound/services.go` (32, still
  "Add your use case methods here"), and `internal/services/verification_service.go` (30) are
  imported by **no binary**. `inbound.HealthChecker`'s doc claims "Implementations:
  LiveService" — nothing in the repo implements `IsReady()`/`IsHealthy()` outside
  `health_test.go`. ~510 lines and two ports to delete.
- **`block_states` is not append-only.** `MarkBlockOrphaned` (`blockstate_repository.go:342`),
  `ClearBlocksOrphaned` (`:392`) and `MarkPublishComplete` (`:1124`) are in-place `UPDATE`s of
  `is_orphaned` / `block_published` — lifecycle observations that
  `db/migrations/AGENTS.md` says must be appended. The table is not in the converted set, and
  the file's own header still advertises "upsert semantics (ON CONFLICT UPDATE)".
- **`BlockEvent.IsReorg` and `IsBackfill` are write-only.** Set at
  `live_data_service.go:1013-1014` and `backfill_gaps_service.go:1041`, read by nothing.
  Downstream workers learn about a reorg only implicitly, via the `Version` in the cache key.
  The reorg contract exists in prose (`AGENTS.md`, `blockstate.go:49-53`) but not in any type a
  worker consumes.
- **`live_data_service.go` (1,163 lines)** belongs to whoever owns the watcher's live path; it
  duplicates `backfill_gaps`' `truncateHash` and carries the reorg-classification logic
  (`detectReorg:630`, `classifyOutOfOrderArrival:737`, `handleReorg:778`,
  `verifyIncomingIsCanonical:946`) that F05.5's `ReorgJournal` split would touch.
- **Cache-miss fallback coverage is split by worker family.** Six indexer workers get
  Redis→S3 (`cache.NewReaderWithFallback`); `raw_data_backup` gets Redis→RPC-by-hash
  (`service.go:711-733`). Neither has both, so a payload that aged out of Redis *and* is
  absent from S3 is a permanent FIFO head-of-line stall for an indexer
  (`dexconsumer/block_processor.go:68-69`).
- **`internal/common/` holds exactly one package** (`sqsutil`) while `internal/pkg/` holds 25.
  Under the stated hexagonal layout `sqsutil` is an *inbound* adapter (it drives services from
  a queue); `internal/adapters/inbound/` currently holds only HTTP.
- **`internal/pkg/rawsckey` and `s3/call_archiver.go`** (raw SC-call archive) share only
  `partition` with the block pipeline; they belong to the multicall/archiving area.
- **`transform_worker` / `transform-bootstrap` / `gen-transformed`** are the transformation
  layer, not the block pipeline: no shared code with `raw_data_backup` or the bulk downloader
  beyond `postgres.PoolOpener`. They sit in this area's directory neighbourhood only.

## 5. Open questions

1. Is `RawMessageDelivery=true` asserted anywhere for the **production** SNS→SQS
   subscriptions? Only LocalStack scripts and tests set it in this repo; the Terraform lives
   elsewhere. If it is not asserted in CI, F05.1's strict decoder is the only guard.
2. Can `raw-block-bulk-downloader` reach `block_states` for the ranges it archives (F05.12), or
   is it deliberately DB-free so it can run against a bucket alone (e.g. from the Erigon box —
   `erigon/erigon-readonly.service`, `make deploy-bulk-download`)?
3. Has the archive's derived version ever diverged from `block_states.version` at a height? If
   so the S3 fallback silently misses and the mismatch is invisible.
4. Is the `MarkPublishComplete` divergence (F05.3: warn in live, error in backfill) an
   intentional policy split, or the residue of two independent fixes? Both carry rationale
   comments arguing the opposite conclusion.
5. `raw_data_backup` runs `Workers` (default 2) concurrent processors over one FIFO message
   group. Within a single receive SQS can return several messages of the same group, so blocks
   of one chain can be backed up out of order. Harmless for an idempotent per-(block, version,
   type) write, but is out-of-order settling within a receive intended?
6. `backfill_gaps` passes are expected to return `nil` after filling nothing (F05.6). Is the
   `backfill_watermark_lag` gauge considered sufficient detection, or is a "pass made no
   progress" signal wanted?
