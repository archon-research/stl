# 01 — Morpho indexing stack

Investigation area: the Morpho pipeline (largest single feature in the repo). Read-only;
every claim below is cited to `file:line` at commit `c4e0a8f2`. `go vet` over all five
packages is clean.

## 1. Area map

Morpho is indexed by three entry points that share one service package.

```
                Alchemy WS → watcher → Redis (receipts) + SNS FIFO
                                                 │
                                        SQS FIFO │ block pointer
                                                 ▼
cmd/workers/morpho-indexer/main.go  ──►  morpho_indexer.NewService (11 ports)
   (long-running, sqsutil.RunLoop)          │
                                            │ Service.processBlockEvent
                                            ▼
                          fetchAndProcessReceipts → processReceipt
                                            │
                    ┌───────────────────────┼────────────────────────┐
                    ▼                       ▼                        ▼
        processMorphoBlueLog      processMetaMorphoLog        tryDiscoverVault
        (10-arm type switch)      (18-arm type switch)        (vault_probe.go)
                    │                       │                        │
        morpho_blue_handler.go   metamorpho_handler.go        discovery.go
                                 vault_v2_handler.go
                    └───────────────────────┼────────────────────────┘
                                            ▼
                        blockchainService (18 multicall reads)
                        MorphoRepository / EventRepository / … (19 tx sites)

cmd/backfillers/morpho-vault-backfill  ──► morpho_indexer.NewReplayService (6 ports, 5 nil)
   (on-demand Temporal worker; S3 raw receipts)   └─► Service.ReplayMetaMorphoLog
                                                  └─► VaultProber (imported from the service)

cmd/cronjobs/morpho-v2-bootstrap ──► morpho_v2_bootstrap.NewService
   (hand-started Temporal worker; eth_getLogs)  └─► V2Replayer iface → the same Service
```

`morpho_indexer` is the only package with domain logic. The backfiller re-uses the
service's `VaultProber`, `MorphoBlueVaultCandidates` and `ReplayMetaMorphoLog`; the
bootstrap owns no handler logic at all (`morpho_v2_bootstrap/bootstrap.go:85-87`) and
drives the service through `V2Replayer` (`bootstrap.go:88`). Writes land in
`internal/adapters/outbound/postgres/morpho_repository.go` (15-method port) plus the
shared `protocol_event` audit log.

Domain vocabulary (`docs/morpho_spec.md`): Morpho Blue **market** / **position** /
**liquidation** vs MetaMorpho **vault** (V1 / V1.1 / V2) / **adapter** / **allocation** /
**cap** / **fee**, all persisted as versioned **snapshots** keyed on
`(block_number, block_version, processing_version)`.

## 2. Metrics

| Unit | src lines | test lines | src files | test files |
|---|---|---|---|---|
| `internal/services/morpho_indexer` | 6,828 | 13,016 | 13 | 15 |
| `cmd/backfillers/morpho-vault-backfill` | 2,649 | 4,142 | 7 | 9 |
| `internal/services/morpho_v2_bootstrap` | 749 | 2,013 | 3 | 4 |
| `internal/adapters/outbound/postgres/morpho_repository.go` | 691 | 3,183 | 1 | 1 |
| `internal/domain/entity/morpho*.go` | 989 | 1,490 | 11 | 11 |
| `cmd/workers/morpho-indexer` | 340 | 915 | 1 | 2 |
| `cmd/cronjobs/morpho-v2-bootstrap` | 295 | 311 | 1 | 3 |
| `internal/pkg/blockchain/abis/{morpho_blue,metamorpho,vault_v2_events}_abi.go` | 682 | — | 3 | — |
| `internal/ports/outbound/morpho_repository.go` | 126 | — | 1 | — |
| `internal/testutil/mock_morpho_repository.go` | — | 135 | — | 1 |
| **Total** | **~13,350** | **~25,200** | 41 | 46 |

Tests + fixtures are **65 %** of the 38,554-line footprint. Inside `morpho_indexer`
itself: 13,016 / 19,844 = **66 %**.

Largest src files:

| Lines | File |
|---|---|
| 1408 | `morpho_indexer/blockchain_service.go` |
| 1137 | `morpho_indexer/event_extractor.go` |
| 926 | `morpho_indexer/service.go` |
| 749 | `morpho-vault-backfill/discovery.go` |
| 691 | `postgres/morpho_repository.go` |
| 685 | `morpho_indexer/types.go` |
| 530 | `morpho_v2_bootstrap/bootstrap.go` |
| 526 | `morpho-vault-backfill/prober.go` |
| 521 | `morpho-vault-backfill/backfill.go` |
| 468 | `morpho_indexer/vault_v2_handler.go` |

Largest functions (top-level `func` scan, src only):

| Lines | Location | Function |
|---|---|---|
| 180 | `cmd/workers/morpho-indexer/main.go:161` | `run` |
| 137 | `morpho_indexer/service.go:477` | `(*Service).processReceipt` |
| 108 | `morpho_indexer/telemetry.go:98` | `NewTelemetryWithProviders` |
| 108 | `morpho-vault-backfill/prober.go:387` | `(*vaultProber).fetchVaultMetadata` |
| 98 | `morpho_indexer/blockchain_service.go:1236` | `getTokenPairMetadata` |
| 91 | `cmd/workers/morpho-indexer/main.go:69` | `parseConfig` |
| 80 | `morpho_indexer/service.go:686` | `(*Service).processMetaMorphoLog` |
| 78 | `morpho_indexer/event_extractor.go:248` | `ExtractMetaMorphoEvent` |
| 77 | `morpho-vault-backfill/discovery.go:673` | `persistVaults` |
| 77 | `morpho-vault-backfill/discovery.go:191` | `listAllBlockKeys` |
| 76 | `morpho_indexer/event_extractor.go:49` | `(*EventExtractor).loadABIs` |
| 74 | `morpho_indexer/discovery.go:374` | `discoverV1V11VaultsInReceipt` |

Other counts:

- **Ports consumed**: 10 (`SQSConsumer`, `BlockCacheReader`, `Multicaller`, `TxManager`,
  `UserRepository`, `ProtocolRepository`, `TokenRepository`, `MorphoRepository`,
  `EventRepository`, `ReceiptTokenRepository`). Ports **implemented**: 0 — `Service` is a
  concrete type consumed directly; the only in-area interfaces are
  `morpho_v2_bootstrap`'s `V2Replayer` / `ChainReader` / `ProgressStore`
  (`bootstrap.go:88-143`), each with exactly one production adapter.
- **Test doubles**: 10 shared mocks from `internal/testutil` (good — no per-package
  re-implementation), plus **4 hand-rolled fakes**, one of which is duplicated:
  `fakeChainReader` in both `morpho_v2_bootstrap/bootstrap_test.go:882` and
  `morpho-vault-backfill/replay_test.go:415` (same two-method surface),
  `fakeProgressStore` (`bootstrap_test.go:999`), `fakeReplayS3Reader`
  (`replay_test.go:230`).
- **Multicall reads**: 18 call sites (11 `ExecuteAtHash`, 7 `Execute`), 14 hand-written
  result-count checks, 31 `.Success` checks.
- **Transaction sites**: 19 `txManager.WithTransaction` calls in `morpho_indexer`.
- **Block-identity parameter lists**: 30 functions carry `blockVersion int` +
  `blockTimestamp time.Time` as loose positional params.
- No complexity linter is configured (`.golangci.yml` has no `funlen`/`gocognit`/
  `gocyclo`/`dupl`), so the backstop `stl-verify/AGENTS.md:196` calls "planned" does not
  exist yet.

## 3. Findings

Ranked by impact: **F01.14, F01.1, F01.2, F01.3, F01.4** are the top five; F01.5–F01.7
are strong but narrower; F01.8–F01.13 and F01.15 are smaller. (F01.14 and F01.15 were
established last, hence their numbering.)

### F01.1 — One VaultV2 event name is hand-maintained in six lists that must agree

**Strength**: Strong
**Files**:
- `morpho_indexer/event_extractor.go:268-271` (gate switch, 17 names)
- `morpho_indexer/event_extractor.go:285-322` (dispatch switch, 17 names again)
- `morpho_indexer/service.go:729-762` (handler dispatch, 18 type-switch arms)
- `morpho_indexer/replay.go:186-197` (`vaultV2StructuredEventNames`, 13 names)
- `morpho_indexer/replay.go:203-216` (`vaultV2ConfigEventNames`, 10 names)
- `internal/domain/entity/morpho_event_type.go:19-76` (const **and** `validMorphoEventTypes` map — every name twice)
- `morpho_indexer/types.go` (27 × struct + `Type()` + `ToJSON()`)
- `morpho_indexer/event_extractor.go:457-1139` (27 × `extractXxx`)

**Problem.** Tracing one event, `IncreaseAbsoluteCap`, gives **18 non-test occurrences
across 6 files** (plus 5 test files). The code says so itself:

```go
// morpho_indexer/replay.go:190-192
// … Keep in sync with the dispatch switch in processMetaMorphoLog.
var vaultV2StructuredEventNames = []string{
```

Nothing enforces that; `VaultV2StructuredEventTopics()` (`replay.go:219`) gates
`ReplayMetaMorphoLog` (`replay.go:73`) on a list a developer must remember to extend.
Miss it and the live path handles the event while replay rejects it as
`ErrUnreplayableLog` — a silent coverage hole in the backfiller only. Git confirms the
cost: `cd5508a5` added the V2 adapter/cap/fee surface across **58 files** (`service.go`
+176/−685), and the telemetry half of the same feature landed in three later commits
(`61e06365` +98, `0fb043ea` +31, `42ba6f4d` +6), each editing a per-type `switch` in
`telemetry.go:36 adapterTypeLabel`.

**Proposed change.** One registry table, one row per Morpho event, replacing all six
lists:

```go
type morphoEvent struct {
    Name        string                 // ABI event name
    Type        entity.MorphoEventType // protocol_event.event_type
    ABI         *abi.ABI
    Extract     func(map[string]any, string) (MetaMorphoEvent, error) // nil ⇒ audit-log only
    Handle      func(*Service, context.Context, MetaMorphoEvent, blockRef) error
    Replayable  bool // replaces vaultV2StructuredEventNames
    SweepConfig bool // replaces vaultV2ConfigEventNames
}
var morphoEvents = [...]morphoEvent{ … }
```

`loadABIs`, both switches in `ExtractMetaMorphoEvent`, the `processMetaMorphoLog`
dispatch, both topic-set helpers and `entity.validMorphoEventTypes` all derive from it.
A table-driven test asserts every registered ABI event has a row and every row's ABI
event exists — the drift class disappears rather than being reviewed for.

**Benefits.** Adding an event becomes: one table row + one `extract` + one handler
(3 edits in 2 files, down from 18 sites in 6). Locality: "what does the indexer do with
event X" is one greppable row. Leverage: the replay/sweep subsets become derived
predicates, so they cannot lag the live path.

**Risk / migration.** Land in three PRs: (1) introduce the table and derive
`loadABIs` + both topic-set helpers from it, keeping the switches; (2) derive
`ExtractMetaMorphoEvent` from `Extract`, deleting both switches; (3) derive
`processMetaMorphoLog` from `Handle`, deleting the type switch. Each step is
behaviour-preserving and covered by the existing 1,012-line `event_extractor_test.go`
and 2,016-line `vault_v2_handler_test.go`.

**Size**: L. **Enables**: F01.2, F01.6.

---

### F01.2 — `blockchain_service.go` hand-rolls 18 copies of a skeleton `shared.RunSnapshotReads` already exists for

**Strength**: Strong
**Files**: `morpho_indexer/blockchain_service.go:235-1382` (16 of the 18);
`vault_probe.go:185-205`, `vault_probe.go:359-372`, `adapter_probe.go:91-101`;
the unused seam: `internal/services/shared/snapshotread.go:22-68`

**Problem.** Every read repeats the same six steps — span, timing `defer` +
`RecordRPCCall`, `Pack` per call, `Execute*`, hand-written count check, positional
unpack. Verbatim, `getVaultState` (`blockchain_service.go:450-484`):

```go
	totalAssetsData, err := s.metaMorphoABI.Pack("totalAssets")
	if err != nil {
		return nil, fmt.Errorf("packing totalAssets call: %w", err)
	}
	totalSupplyData, err := s.metaMorphoABI.Pack("totalSupply")
	if err != nil {
		return nil, fmt.Errorf("packing totalSupply call: %w", err)
	}
	results, err := s.multicallClient.ExecuteAtHash(ctx, []outbound.Call{
		{Target: vaultAddress, AllowFailure: false, CallData: totalAssetsData},
		{Target: vaultAddress, AllowFailure: false, CallData: totalSupplyData},
	}, blockHash)
	if err != nil {
		return nil, fmt.Errorf("multicall vault state: %w", err)
	}
	if len(results) < 2 {
		return nil, fmt.Errorf("expected 2 results, got %d", len(results))
	}
	return s.unpackVaultState(results[0], results[1], vaultAddress)
```

The count check is written 14 times and **inconsistently**: `< 2`, `< 3`, `< 4`
(`:370, :426, :480, :523, :582`) vs `!= 2`, `!= 4`, `!= len(x)` (`:794, :862, :949,
:1169, :1308, :1362`). `<` silently tolerates a longer-than-expected result array.

Worse, six of these functions are a **cartesian product** of one read × user count:

| Read | 0 users | 1 user | 2 users |
|---|---|---|---|
| market | `getMarketState` :235 (38 L) | `getMarketAndPositionState` :340 (46 L) | `getMarketAndTwoPositionStates` :390 (57 L) |
| vault | `getVaultState` :450 (35 L) | `getVaultStateAndBalance` :489 (50 L) | `getVaultStateAndTwoBalances` :543 (60 L) |

286 of the file's 1,408 lines. A third user (or a third entity) means a seventh
function, not a call-site change.

Meanwhile `internal/services/shared/snapshotread.go` is exactly the missing seam, and its
docstring describes the very defect above — "no hand-maintained positional cursor shared
across separate pack/decode functions". It is a **real** seam (two adapters, five
callers: `curveindexer/cryptoswap_handler.go`, `curveindexer/stableswap_handler.go`,
`uniswapv3indexer/state.go`). `shared.UnpackUint` (`abidecode.go:20`) and
`shared.OptionalUintResult` (`abilog.go:187`) are likewise used by curve/uniswap and not
by morpho.

The probers get halfway there — `ProbeCalls`/`ParseProbeResults` + `NumProbeCalls()`
(`vault_probe.go:118, 207, 232`) — but hand-maintain the positional cursor the shared
executor owns, and the backfiller then re-derives the offsets itself
(`morpho-vault-backfill/prober.go:340-347`, and see the `// Keep in sync with
assetSymbolOffset / assetDecimalsOffset` comment at `prober.go:369`).

**Proposed change.** Express each read as a `shared.SnapshotRead[*morphoReadTarget]`
(pack + decode co-located), and let callers compose:

```go
reads := []shared.SnapshotRead[*marketSnapshot]{marketRead(mid), positionRead(mid, userA), positionRead(mid, userB)}
err := shared.RunSnapshotReads(ctx, s.multicallClient, snap, blockHash, reads)
```

The six-function product collapses to two reads plus a caller-built slice; the count
check, the offset arithmetic and the "expected N results" error all move into
`RunSnapshotReads`. Wrap the span/`RecordRPCCall` prologue once in a
`s.timedRead(ctx, name, fn)` decorator (repeated 16 times today).

**Benefits.** Removes ~450 lines from `blockchain_service.go` and the whole class of
"unpack read the wrong index". Leverage: batching two events' reads into one multicall
becomes a slice append instead of a new method. Tests: `blockchain_service_test.go`
(2,036 lines) currently asserts on positional result arrays via harness helpers
(`isVaultStateAndTwoBalancesMulticall`, `testhelpers_test.go:425`); per-read tests
replace them.

**Risk / migration.** Read-shape-preserving, so the existing pin assertions
(`assertMulticallPinnedViaHash`, `testhelpers_test.go:502`) keep guarding it. Migrate
family by family (market reads, vault reads, cap/fee reads, adapter enumeration), one PR
each. `RunSnapshotReads` is hash-pinned only, so the 7 number-pinned reads need
F01.3 first or a number-pinned sibling.

**Size**: L. **Depends on**: F01.3 (for the number-pinned reads).

---

### F01.3 — Block identity is 5 loose positional params through 30 functions, and hash-vs-number pinning is a convention, not a type

**Strength**: Strong
**Files**: 30 signatures — `service.go:477, 616, 686, 790`,
`morpho_blue_handler.go:22, 70, 93, 149, 258, 271`,
`metamorpho_handler.go:29, 90, 114, 141, 154`,
`vault_v2_handler.go:49, 163, 185, 222, 392, 438`,
`discovery.go:107, 213, 277, 311, 332, 374`, `replay.go:58, 99, 151`;
existing partial type at `internal/domain/entity/morpho_adapter.go:101`

**Problem.** `(chainID, blockNumber int64, blockHash common.Hash, blockVersion int,
blockTimestamp time.Time)` is carried by hand, plus `logIndex int32` in the V2 path. The
worst signatures: `metamorpho_handler.go:154 saveVaultPositionInTx` (12 positional
args, with `chainID int64` stranded **last**, after `txHash string`),
`morpho_blue_handler.go:70 handlePositionEvent` (10),
`metamorpho_handler.go:114 saveVaultEventSnapshot` (10). The 5-tuple appears at 20 call
sites verbatim and the 4-tuple at 29.

The cost is measured: `c92be237` (VEC-471) inserted **one** parameter, `blockHash`,
between `blockNumber` and `blockVersion` — 11 signatures in `service.go`, **59
repo-wide, 74 files**, including 10 consecutive `case` arms in `processMorphoBlueLog`
that each re-list the tuple. `cd5508a5` then appended three more params to
`discoverV1V11VaultsInReceipt`, a helper introduced two commits earlier.

`entity.BlockPosition{BlockNumber, BlockVersion, LogIndex}` **already exists**
(`morpho_adapter.go:101`) and is the right idea, but it (a) omits `ChainID`,
`BlockHash` and `Timestamp`, (b) lives in a Morpho-specific entity file despite being a
general chain concept, and (c) is used only as a query argument in 3 places
(`vault_v2_handler.go`, `replay.go:118`, `morpho_repository.go`).

Second half of the finding: **which pin a read uses is enforced only by prose.** 11
reads use `ExecuteAtHash` and 7 use `Execute`, and the difference between "versioned
state, must be hash-pinned" and "immutable identity, number is fine" is carried in
doc comments at `blockchain_service.go:232, 271, 635, 686, 831, 913, 1121` and
`adapter_probe.go:113`. Both live behind the same `outbound.Multicaller`, taking
different argument types. A new getter that grabs the wrong one is a silent
reorg-correctness bug — precisely the class VEC-471 (#520) was opened to fix.

**Proposed change.** One value type, threaded once:

```go
// internal/domain/entity (not the morpho file)
type BlockRef struct {
    ChainID     int64
    Number      int64
    Hash        common.Hash
    Version     int
    Timestamp   time.Time
}
func (b BlockRef) At(logIndex int32) BlockPosition
```

Built once in `fetchAndProcessReceipts` from `outbound.BlockEvent` (it already parses
the hash there, `service.go:388`) and passed as a single argument. Then make the pin a
type, not a comment: expose `StateReader` (hash-pinned only, takes `BlockRef`) and
`IdentityReader` (number-pinned) as two narrow seams over `Multicaller`, so a state read
*cannot* be number-pinned — the invariant moves from 18 doc comments to one signature.

**Benefits.** Locality: adding a field to block identity is one struct edit, not 59
signatures. Leverage: `BlockRef` carries its own `At(logIndex)` so the
`entity.EndOfBlockLogIndex` convention (`morpho_adapter.go:96`) is expressed in code.
Tests: handler tests stop threading 10-arg calls; a fixture factory
`testBlockRef(opts…)` replaces the positional soup in `testhelpers_test.go`.

**Risk / migration.** Mechanical and compiler-checked. Do it per file (7 files in
`morpho_indexer`), then the pin-typing as a separate PR. The pin split is the risky half:
it must not silently change any read's pin, so land it with the existing
`assertMulticallPinnedViaHash` assertions extended to every read.

**Size**: L (M for `BlockRef` alone). **Enables**: F01.2. Repo-wide sibling: the same
tuple is threaded in every other indexer, so this is worth doing once in `entity`/`shared`.

---

### F01.4 — Two transactions per log: the audit row commits separately from the structured row

**Strength**: Strong
**Files**: `service.go:660-668` + `service.go:802` (Morpho Blue),
`service.go:716` + `service.go:832` (MetaMorpho), then each handler's own
`WithTransaction` at `morpho_blue_handler.go:39, 76, 118, 161`,
`metamorpho_handler.go:74, 108, 126`, `vault_v2_handler.go:67, 191, 243, 409, 457`

**Problem.** `processMorphoBlueLog` calls `saveProtocolEvent` (its own transaction,
`service.go:832`) and *then* dispatches to a handler that opens **another**
transaction. Same for `processMetaMorphoLog` → `saveMetaMorphoProtocolEvent`
(`service.go:802`) → typed handler. So one log yields ≥2 commits, and a handler failure
leaves the `protocol_event` audit row committed with no matching structured row. There
are **19 separate `WithTransaction` sites** in the package; a block with N relevant logs
issues ~2N transactions.

`stl-verify/AGENTS.md` is explicit: "**A partial failure stops the whole event/block.**
Do not ack, commit, or persist a partially-processed event." Today the block is not
acked (the error propagates and SQS redelivers), and the writes are idempotent, so this
is not data corruption — but it *is* a persisted partial state, it doubles the commit
rate, and it means a reader of `protocol_event` cannot assume the structured tables
agree with it.

`fetchAndProcessReceipts` compounds it (`service.go:415-424`): it keeps processing every
remaining receipt after one fails, then `errors.Join`s. A poison receipt therefore does
2N-2 further commits before the block is refused, on every redelivery.

**Proposed change.** Move the transaction boundary up to **one transaction per log**
(or per receipt): `processMorphoBlueLog` / `processMetaMorphoLog` open the transaction,
and every handler takes `tx pgx.Tx` instead of opening its own — the file already has
the `…InTx` naming for this (`saveVaultStateSnapshotInTx`,
`saveVaultPositionInTx`, `saveAdapterSeedState`). Chain reads stay outside the
transaction, which the code already insists on ("Both chain reads run before the
transaction opens so a pooled DB connection never sits idle across a chain
round-trip", `vault_v2_handler.go:42`) — so the restructure is: read phase → single
transaction → telemetry. Then make `fetchAndProcessReceipts` return on first error.

**Benefits.** Audit row and structured row become atomic per log; commit rate halves;
`RecordV2Snapshot`'s `appended` flag (currently read *after* four separate commits,
`vault_v2_handler.go:87, 259, 417, 465`) becomes one post-commit sweep. Tests get
simpler: `failCommitAfterMembershipAppend` (`testhelpers_test.go:176`) exists precisely
to poke at this multi-commit shape.

**Risk / migration.** Real risk: a longer transaction holds a pooled connection and the
advisory lock (`morpho_repository.go:497 lockAdapterKey`) for the whole log, so lock
contention and pool pressure change. Land per handler family behind the existing
integration tests (`morpho_repository_integration_test.go`, 3,183 lines), and verify the
read-before-transaction split holds for each one first.

**Size**: L. **Depends on**: F01.3 helps (a single `BlockRef` makes the `…InTx`
signatures tolerable).

---

### F01.5 — `protocol_event` gets two incompatible `event_data` shapes from the same package

**Strength**: Strong
**Files**: `service.go:824-857` (`saveProtocolEvent` → `event.ToJSON()`),
`service.go:790-822` (`saveMetaMorphoProtocolEvent` → raw topics+data),
`types.go` (27 hand-written `ToJSON`)

**Problem.** Morpho Blue events are ABI-decoded into named JSON by 27 hand-written
`ToJSON()` methods (`types.go`, e.g. `LiquidateEvent.ToJSON` at `:209-223` listing 8
fields by hand). MetaMorpho events, written to the **same column**, are stored
undecoded, and the docstring says so deliberately:

```go
// service.go:783-787
// EventData is a JSON snapshot of the raw log: { eventType, vault, topics,
// data }. ABI decoding of args is intentionally skipped — operators can decode
// downstream from the canonical signatures in
// …/abis/vault_v2_events_abi.go if needed.
```

So a `protocol_event` consumer must branch on which writer produced the row. And the
decoded half is a third hand-maintained copy of each event's schema: the ABI declares
the fields, `extractXxx` pulls them out of `map[string]any` by string key
(`event_extractor.go:457-1139`), and `ToJSON` re-serialises them by string key. Any of
the three can drift from the others without a compile error.

There is already a shared decoder for exactly this. `internal/services/shared/abilog.go:28-30`:

```go
// DecodeLog extracts both indexed (from topics) and non-indexed (from data)
// fields of an ABI event log into a flat map, following the morpho_indexer
// parseTopics/parseData pattern.
```

It was extracted **from** morpho and morpho never adopted it: `parseTopics` /
`parseData` (`event_extractor.go:332-374`) and the six `getXxx(eventData, key)`
accessors (`:376-455`) are near-line-for-line the same code as
`shared.DecodeLog` + `GetAddrField` / `GetBigIntField` (`abilog.go:31-104`). Three
other indexers use the shared version (`curveindexer/{crypto,stable}swap_handler.go`,
`uniswapv3indexer/event_decode.go`), and `dexconsumer/captured.go:74` already has a
`MarshalDecodedParams` that JSON-encodes a `DecodeLog` map generically — which is
`ToJSON` for all 27 events.

**Proposed change.** (a) Delete `parseTopics`/`parseData` and the six accessors; call
`shared.DecodeLog` and `shared.Get*Field`. (b) Replace all 27 `ToJSON()` with one
generic marshaller over the decoded map (`dexconsumer.MarshalDecodedParams`, promoted to
`shared`), so `protocol_event.event_data` has **one** shape for every Morpho event,
decoded, derived from the ABI. Keep `Type()`, which is the registry key from F01.1.

**Benefits.** Removes ~350 lines from `types.go` and ~120 from `event_extractor.go`, and
removes the drift class entirely (fields come from the ABI, not from three string-keyed
copies). `protocol_event` becomes queryable uniformly. Leverage: the V2
"audit-log-only" surface (`SetCurator`, timelock/gate/metadata setters) gets decoded
args for free, which is what the current comment defers to the operator.

**Risk / migration.** `event_data` shape change is observable downstream — the JSON key
casing of the hand-written maps (`newAbsoluteCap`) differs from ABI param names
(`newAbsoluteCap` matches, but e.g. `marketId` vs `id`). Needs a schema decision with
whoever reads `protocol_event`, and probably a new `processing_version` rather than a
rewrite (append-only). Land the decoder swap (a) first — it is behaviour-preserving —
and treat (b) as its own PR.

**Size**: M for (a), M for (b). **Depends on**: F01.1 (registry) makes (b) natural.

---

### F01.6 — `NewReplayService` builds a `Service` with 5 nil ports and guards the resulting hole with a topic allowlist

**Strength**: Strong
**Files**: `service.go:154-181` (`NewReplayService`, `newService`),
`service.go:126-131` (`v2StructuredTopics` field), `replay.go:64-77`
(the guard), `service.go:858-926` (`validateDependencies` vs
`validateReplayDependencies`)

**Problem.** The backfiller and bootstrap need "the same handlers, no SQS, no Redis", so
the package gives `Service` a second constructor that passes `nil` for
`consumer, cache, userRepo, tokenRepo, receiptTokenRepo`:

```go
// service.go:180
	return newService(config, nil, nil, multicallClient, txManager, nil, protocolRepo, nil, morphoRepo, eventRepo, nil)
```

Safety then rests on a runtime allowlist, documented as such at `service.go:126-131`:

> `v2StructuredTopics` gates `ReplayMetaMorphoLog`: the replay constructor nils the
> user/token/cache/consumer/receipt-token ports, so only the VaultV2 structured
> governance/allocation/cap/fee events (which never touch them) are safe to replay. Any
> other MetaMorpho topic (e.g. a V1 Deposit) is rejected before it can nil-deref the
> share-accounting path.

That is a nil-pointer panic held off by a `map[common.Hash]struct{}` lookup
(`replay.go:71-75`) whose contents are one of the six hand-maintained lists from F01.1.
The two `validate*Dependencies` functions (44 + 26 lines of `if x == nil` at
`service.go:858, 902`) are the symptom: the type says 11 ports, the truth is two
different objects.

**Proposed change.** Split along the line the code already draws. Extract the
share-accounting handlers (`saveVaultEventSnapshot`, `handleVaultTransfer`,
`handleVaultAccrueInterest`, and the market/position handlers) into a
`positionIndexer` that owns `userRepo`/`tokenRepo`/`receiptTokenRepo`, and leave the
V2 governance/allocation/cap/fee handlers in a `vaultStateIndexer` that owns only
`morphoRepo`/`eventRepo`/`protocolRepo`/`txManager`/multicall. The live worker composes
both; replay constructs only the second. `ReplayMetaMorphoLog`'s allowlist and both
`validate*Dependencies` disappear — the compiler enforces what the map currently
enforces at runtime.

**Benefits.** Deletion test passes: removing the allowlist and the second validator
does not push complexity anywhere — the split makes the invariant structural.
Locality: "which ports does the replay path need" is a constructor signature.
Tests: the replay tests (`replay_test.go` 734 L,
`morpho_v2_replay_integration_test.go` 869 L) can construct the narrow service
directly instead of asserting on rejection messages.

**Risk / migration.** Both indexers write to `protocol_event` and share
`resolveV2Vault` / `vaultRegistry`, so the registry and the audit-log writer move to a
small shared collaborator first (its own PR), then the handler split. Behaviour-neutral
if done in that order.

**Size**: L. **Depends on**: F01.1 (the registry's `Handle` field is where the split
becomes visible).

---

### F01.7 — The same MetaMorpho-canonical invariant is written twice and disposes of failures differently

**Strength**: Strong
**Files**: `blockchain_service.go:1071-1081` vs
`cmd/backfillers/morpho-vault-backfill/prober.go:357-359`

**Problem.** `VaultProber.ParseProbeResults` (`vault_probe.go:232`) decides vault
flavour but **not** whether a MetaMorpho vault points at the canonical Morpho Blue
singleton. That check therefore lives outside it, twice. Live path
(`blockchain_service.go:1075`):

```go
	if probe.Version != entity.MorphoVaultV2 && probe.MorphoAddr != MorphoBlueAddress {
		return nil, &ErrNotVault{
			Err:         fmt.Errorf("MORPHO() returned %s, expected %s — not a MetaMorpho vault", …),
			VaultShaped: true, // MORPHO() returned an address — it's vault-shaped, just not ours.
		}
	}
```

Backfiller (`prober.go:357`):

```go
		if probeResult.Version != entity.MorphoVaultV2 && probeResult.MorphoAddr != morpho_indexer.MorphoBlueAddress {
			continue
		}
```

The live path raises the `VaultShaped` WARN whose whole purpose is to catch a future
Morpho V3 before it "sits invisible for ~225 days" (`service.go:583-586`). The
backfiller drops the identical address with no log line at all — a silent skip, which
`stl-verify/AGENTS.md` forbids ("**Poison pills get fixed or explicitly discarded, never
silently skipped**"). Because the backfiller is the recovery path, the one place you
would look after a missed vault flavour is the one place that says nothing.

**Proposed change.** Move the canonical-Morpho check **into**
`ParseProbeResults`, where the rest of the flavour decision tree lives
(`vault_probe.go:242-278`), returning `ErrNotVault{VaultShaped: true}`. Both callers
then get one verdict; the backfiller's `errors.As(err, &nv)` arm
(`prober.go:350-355`) already exists and only needs to log `nv.VaultShaped` at WARN.

**Benefits.** One statement of the invariant, one disposition. Deletion test: the
duplicate `if` in both callers vanishes with no complexity reappearing. Leverage: any
third prober caller inherits the check.

**Risk / migration.** Small and self-contained; `vault_probe_test.go` coverage exists
for `ParseProbeResults`. Watch that the live path's `getVaultMetadata` error message
(currently produced at `blockchain_service.go:1077`) is preserved for the log assertions
in `discovery_test.go`.

**Size**: S.

---

### F01.8 — `telemetry.go` is 108 lines of instrument boilerplate plus a per-type label switch

**Strength**: Worth exploring
**Files**: `morpho_indexer/telemetry.go:59-206` (11 instruments),
`telemetry.go:36-57` (`adapterTypeLabel`); unused sibling
`internal/pkg/telemetry/metrics.go:28-66`; 8 services with their own `telemetry.go`

**Problem.** `NewTelemetryWithProviders` is a 108-line straight line of
`t.x, err = meter.Int64Counter(name, desc); if err != nil { return nil, … }` × 11
(`:110-206`) — the single largest function in the package after `processReceipt`. Of the
7 commits to this file since 2026-03, **4 are "add an instrument"** (`0fb043ea` +31,
`61e06365` +98, `7abeb809` +22, `36c38646` +4) and one is "add three adapter families to
a `switch`" (`42ba6f4d` +6 in `adapterTypeLabel`), meaning a new adapter type is a
telemetry edit as well as a domain edit.

`internal/pkg/telemetry/metrics.go` has a shared `Metrics` type with
`RecordBlockProcessed` / `RecordProcessingLatency` — morpho re-declares both
(`telemetry.go:64, 74, 208`). Eight services each hand-roll a `telemetry.go`
(1,547 lines total), and `internal/services/shared/telemetry.go` (192 lines,
`ServiceTelemetry`) is used by exactly **one** caller, `cmd/base/watcher/main.go:411` —
a hypothetical seam by the brief's definition.

**Proposed change.** Declare instruments as data, not statements:

```go
var morphoInstruments = []telemetry.CounterSpec{
    {Field: &t.blocksProcessed, Name: "morpho_blocks_processed_total", Desc: "…"},
    …
}
```
plus one loop, in `internal/pkg/telemetry`. And derive `adapterTypeLabel` from the
`entity.MorphoAdapterType` enum (a `String()` method on the type, or the F01.1-style
table) so adding an adapter family is one edit, not two.

**Benefits.** ~80 lines out of this file, and the same helper removes similar
boilerplate from the other seven services. Locality: the adapter-family label lives
next to the adapter-family enum.

**Risk / migration.** Metric names and descriptions must not change — alerts in
`alerts/` and runbooks in `docs/runbooks/` key on them. Do it as a pure refactor with a
test that asserts the emitted instrument names, which
`telemetry_test.go` (291 L) + `telemetry_chain_test.go` (39 L) already partly do.

**Size**: M (S for morpho alone). **Cross-area**: the shared helper belongs to whoever
owns `internal/pkg/telemetry`.

---

### F01.9 — `processReceipt` is a 137-line function that inlines pre-walk, gating, dispatch and error bookkeeping

**Strength**: Worth exploring
**Files**: `service.go:477-613`; the near-duplicate gate at `service.go:421-475`
(`hasRelevantEvents`)

**Problem.** `processReceipt` is the busiest function in the package by churn — touched
in **8 of the 18** `service.go` commits since 2026-03. It contains: an early-out via
`hasRelevantEvents`, a span, a 20-line comment block explaining the pre-walk, the
V1/V1.1 discovery pre-walk, a `discoveryErrs map[common.Address]error` with a
"first-failure-wins, never delete on success" rule (VEC-188), a 4-arm `switch` over
`(address, isMorphoBlue, isMetaMorpho)`, a 30-line comment inside the `default` arm,
`ErrNotVault` classification with two log levels, and an `errors.Join` epilogue.
`stl-verify/AGENTS.md:145-150` calls exactly this shape a defect: "A single sprawling
handler that inlines decode + snapshot + persist is a defect, not a style preference…
comment-delimited 'sections' inside a function (each section becomes a named helper)".

`hasRelevantEvents` (`service.go:425-475`) duplicates the routing predicate:
`IsMorphoBlueEvent` / `IsMetaMorpho` / `IsKnownNotVault` / `IsKnownVault` /
`IsVaultActivityEvent`, with its own comment noting it "Mirrors the gate in
processReceipt's default branch." Two copies of one routing rule, kept in step by hand.

**Proposed change.** Introduce a `logRoute` classifier — one function, `shared.Log` +
registry + `vaultRegistry` in, one of `{skip, morphoBlue, knownVault, discoveryCandidate}`
out. `hasRelevantEvents` becomes `slices.ContainsFunc(receipt.Logs, routable)` over the
same classifier, and `processReceipt` becomes a five-line outline:
`discoverV1V11Vaults` → `routeLogs` → `dispatch` → `collectDiscoveryErrors`. Move the
VEC-188 first-failure bookkeeping into a small named type (`discoveryFailures`) with the
invariant in its doc comment instead of two inline comments.

**Benefits.** One routing rule instead of two. The 30-line rationale comments move to
the classifier's doc comment, where the brief's "state each why once, at its canonical
site" rule wants them. Given 8/18 commits land here, the locality win is the point.

**Risk / migration.** Behaviour-preserving refactor; `service_test.go` (1,032 L) covers
the gating and the VEC-188 rule. Low risk, but do it *after* F01.3 so the extracted
helpers do not inherit 8-arg signatures.

**Size**: M. **Depends on**: F01.3.

---

### F01.10 — Two hand-rolled channel pools, two bisect-retry loops and one duplicated fake across the backfiller and bootstrap

**Strength**: Worth exploring
**Files**: `morpho-vault-backfill/discovery.go:158-181` and `:191-267` (two pools);
`morpho-vault-backfill/prober.go:199-208` vs `morpho_v2_bootstrap/sweep.go:137-149`
(two bisects); `morpho_v2_bootstrap/sweep.go:56-61` vs
`morpho-vault-backfill/replay.go:385-390` (comparator);
`morpho-vault-backfill/backfill.go:131` vs `sweep.go:19` (`blockRange` declared twice);
`bootstrap_test.go:882` vs `replay_test.go:415` (`fakeChainReader` twice)

**Problem.** The genuinely-shared parts of Morpho discovery are already shared — the
backfiller imports the service's `VaultProber`, `MorphoBlueVaultCandidates` and
`ErrNotVault`, and the bootstrap owns no handler logic (verified: ~4 % of the
~4,090 lines across the three discovery paths is duplicated, not the wholesale copy one
might assume). What *is* duplicated is the plumbing:

- `discovery.go:158-181` and `:191-267` are two hand-rolled worker pools
  (`sync.WaitGroup` + `workCh`/`partCh`/`resultCh` + `atomic.Value` first-error +
  manual cancel), ~120 lines, in a repo where the prior art is
  `errgroup.SetLimit` — used by the same package 40 lines away
  (`replay.go:221`) and by four other binaries.
- The same "provider capped the result set → bisect the request and retry both halves"
  loop is written for a probe batch (`prober.go:199`) and for a block range
  (`sweep.go:137`). `internal/pkg/retry` (`Do[T]`, `DoVoid`) exists and is used by six
  other sites, but it retries the *same* request, so it genuinely does not cover this —
  the bisect pattern has no shared home.
- The `(blockNumber, logIndex)` log-order comparator, the
  `blocktime.TimestampAt → ReplayMetaMorphoLog` date-and-replay loop
  (`replay.go:121-133` vs `bootstrap.go:493-512`) and `blockRange` are each written
  twice.
- Two S3 gap checks inside the backfiller itself, with different dispositions:
  `logBlockGaps` WARNs (`discovery.go:442-487`), `requireCompletePartition` errors
  (`replay.go:255-287`).

**Proposed change.** Three small extractions: (1) replace both hand-rolled pools with
`errgroup` + `SetLimit`; (2) add `internal/pkg/retry.Bisect[T](work, split)` for the
provider-cap pattern and use it in both places; (3) move `blockRange`, the log
comparator and the date-and-replay loop into a `morpho_replay` helper (or `shared`) that
both drivers call. Then decide *one* disposition for an incomplete archive range and
delete the other check.

**Benefits.** ~150 lines out, and the concurrency/retry behaviour becomes reviewable in
one place instead of four. Tests: `errgroup` removes the need for the pool-specific
tests in `discovery_test.go`.

**Risk / migration.** (1) and (3) are mechanical. (2) needs care — the bisect's
"is this the provider's result cap?" predicate differs (`prober.go` inspects a batch
error, `sweep.go:87-89` documents why HTTP-level retry cannot help), so the shared
version must take the predicate as a parameter. The two gap-check dispositions are a
product decision, not a refactor.

**Size**: M.

---

### F01.11 — `GetOrCreateVault` / `GetOrCreateMarket` use `ON CONFLICT … DO UPDATE` with no recorded exception

**Strength**: Worth exploring
**Files**: `postgres/morpho_repository.go:211-227` (`morpho_vault`),
`:45-67` (`morpho_market`); policy at `db/migrations/AGENTS.md:10-11`

**Problem.** `db/migrations/AGENTS.md:11` says "**Append-only is the DEFAULT, for every
table — not just the converted set.** Adding ANY update channel — … an
`ON CONFLICT … DO UPDATE` arm … requires consulting the team FIRST… A sanctioned update
channel must be recorded in this file as an explicit exception citing the ticket; an
unconsulted or unrecorded one is a review finding regardless of scope."

`morpho_vault` and `morpho_market` are not in the converted set, so these are not
runtime errors, but neither is recorded as an exception:

```go
// morpho_repository.go:216-217
		 ON CONFLICT (chain_id, address) DO UPDATE SET
		     created_at_block = LEAST(morpho_vault.created_at_block, EXCLUDED.created_at_block)
```

```go
// morpho_repository.go:52-56
	// The no-op SET is required so that DO UPDATE ... RETURNING id works on conflict.
		 ON CONFLICT (chain_id, market_id) DO UPDATE SET protocol_id = EXCLUDED.protocol_id
```

The second is the exact no-op-`SET`-for-`RETURNING` idiom the policy names and forbids,
and the same file already demonstrates the sanctioned alternative — `DO NOTHING` +
follow-up `SELECT`, with the rationale written out at `morpho_repository.go:348-355`
(`ObserveAdapterMembership`) and `:392-412` (`adapterIdentityID`). So the correct
pattern exists 150 lines below the violating one.

`LEAST(created_at_block, …)` is a real converge, not a no-op: it rewrites a non-identity
column, which VEC-353 (`a9f221f1`, "prevent registry block_number clobbering") was about.

**Proposed change.** Rewrite both as `INSERT … ON CONFLICT DO NOTHING` + `SELECT id`,
matching `adapterIdentityID`. For `created_at_block`, either treat the first-seen block
as immutable identity (drop the `LEAST`) or make it an appended observation — a
question for whoever owns the schema.

**Benefits.** Removes the only `DO UPDATE` arms in the Morpho writers, unblocking
`morpho_vault`/`morpho_market` for the append-only conversion and the
`REVOKE UPDATE, DELETE` grant that makes the rule enforceable.

**Risk / migration.** `LEAST` semantics must be preserved or explicitly dropped; the
integration tests (`morpho_repository_integration_test.go`, 3,183 L) cover the
get-or-create races. Needs the DB owner's sign-off, so file it as a question, not a PR.

**Size**: S. **Cross-area**: belongs jointly to the DB/migrations area.

---

### F01.12 — `cmd/workers/morpho-indexer/main.go` `run()` is 180 lines, 5 of which are repeated verbatim in 3 sibling workers

**Strength**: Worth exploring
**Files**: `cmd/workers/morpho-indexer/main.go:161-340`; siblings
`cmd/workers/{sparklend,fluid-vault,prime-allocation}-indexer/main.go`

**Problem.** `run()` is the largest function in the whole area: OTEL init, AWS config,
SQS consumer, Redis block cache, S3 fallback reader, Ethereum dial, Postgres pool, build
registry, multicall + multicall telemetry, the archiving decorator, morpho telemetry,
then **seven** repository constructors, then an 11-argument `NewService`. Four of the
ten worker mains contain the identical five-step block (`redisAdapter.NewBlockCache` →
`cache.NewReaderWithFallback` → `multicall.NewClient` → `archivingwire.Bootstrap` →
`postgres.NewTxManager`); three more contain two or three of the steps.

**Proposed change.** A `blockworker.Deps` bootstrapper in `internal/pkg` (or
`cmd/internal`) that returns the assembled `{cacheReader, multicaller, pool, txManager,
buildRegistry, drain func()}` from one config struct, leaving each worker's `run()` to
build only its own repositories and service. This is a composition-root finding, so the
shape should be agreed with whoever owns the `cmd/` area.

**Benefits.** ~120 lines per worker main; one place to change when the archiving
decorator or the S3-fallback contract changes (`454b74a7` touched 38 files doing exactly
that).

**Risk / migration.** Low — `main_integration_test.go` (520 L) drives the binary
end to end. Ordering of `defer`s (pool close, archive drain, OTEL flush) is
load-bearing for the 60 s grace period and must be preserved exactly.

**Size**: M. **Cross-area**: composition roots.

---

### F01.13 — Duplicated `fakeChainReader` and a 1,045-line test harness with one `makeXxxLog` per event

**Strength**: Speculative
**Files**: `morpho_indexer/testhelpers_test.go:246-878` (16 `makeXxxLog` + 8 `packXxx`
helpers), `bootstrap_test.go:882` / `replay_test.go:415` (`fakeChainReader` ×2)

**Problem.** The mock story is otherwise healthy: all 10 outbound ports come from
`internal/testutil`, with no per-package re-implementation. But (a) `fakeChainReader` is
hand-rolled twice for the same `HeaderByNumber`/`HeaderByHash` surface, and (b)
`testhelpers_test.go` has one bespoke `make<Event>Log` per event type — so F01.1's
fan-out has a matching test-side fan-out. There is already a generic builder in the same
file, `makeV2VaultLog(event abi.Event, vaultAddr, indexed, nonIndexed…)`
(`:246`), used only by the V2 events; the 10 Morpho Blue helpers
(`makeSupplyLog` … `makeSetFeeLog`, `:564-771`) each rebuild topics by hand.

**Proposed change.** (a) Add a `testutil.MockChainReader` next to the other 10 mocks.
(b) Generalise `makeV2VaultLog` into `makeEventLog(abi, name, emitter, args…)` and
delete the 10 per-event builders — a fixture factory, which
`stl-verify/AGENTS.md:139` asks for explicitly.

**Benefits.** ~250 test lines out; a new event type needs no new test helper.

**Risk / migration.** Test-only. Do it alongside F01.1 so both fan-outs close together.

**Size**: S.

---

### F01.14 — The shared block-event runner already exists (`dexconsumer`); Morpho and 7 siblings hand-roll it

**Strength**: Strong
**Files**: `morpho_indexer/service.go:296-427` (hand-rolled);
the seam: `internal/services/dexconsumer/block_processor.go:31-87`,
`dexconsumer/captured.go:173-186`, `dexconsumer/deps.go:14-45`,
`internal/pkg/dextelemetry/telemetry.go`, `cmd/workers/internal/dexbootstrap/`

**Problem.** `dexconsumer` is a real seam — two adapters (`curveindexer`,
`uniswapv3indexer`), and its package doc states the exact intent this finding is about
(`block_processor.go:1-7`): "only the protocol-agnostic plumbing lives here… Extracting
it… removes the copy that would otherwise land once per worker." Morpho does not use it.
Neither do three other services: `outbound.BlockCacheReader.GetReceipts` has **5 call
sites** outside adapters/ports — `morpho_indexer/service.go:385`,
`fluid_vault_indexer/service.go:332`, `aavelike_position_tracker/service.go:204`,
`allocation_tracker/service.go:189`, and the one shared
`dexconsumer/block_processor.go:63`. Counting the whole handler surface: **8 hand-rolled
`sqsutil.BlockEventHandler` implementations vs 1 shared runner.**

The duplication is literal. The cache-miss error string is copied four times, and three
copies are **factually wrong**: every one of these workers is wired with the
S3-fallback reader (`cmd/workers/morpho-indexer/main.go:222`
`cache.NewReaderWithFallback`), so a nil payload means "missing from Redis *and* S3", as
the shared runner correctly says:

| Site | Message |
|---|---|
| `morpho_indexer/service.go:390` | `receipts not found in cache for block %d (chain=%d, version=%d)` |
| `fluid_vault_indexer/service.go:337` | *byte-identical* |
| `allocation_tracker/service.go:194` | *byte-identical* |
| `aavelike_position_tracker/service.go:209` | `receipts not found in cache: chainID=%d block=%d version=%d` |
| `dexconsumer/block_processor.go:72` | `receipts not found in cache **or S3** for block %d …` |

Morpho and fluid also record **no error metric** at the miss site, where the shared
runner records `RecordError(ctx, "fetchReceipts", err)` (`block_processor.go:73`).

Putting the three skeletons side by side turns up five more arbitrary divergences in
Morpho's copy:

| Behaviour | morpho | fluid | uniswapv3 (shared runner) |
|---|---|---|---|
| tx boundary | ≥2 commits per log (F01.4) | 1 per block, `service.go:499` | 1 per block, `captured.go:173` |
| partial failure | accumulate + `errors.Join`, `service.go:418-427, 609-611` | fail-fast, `service.go:304-317` | fail-fast, `service.go:143-170` |
| `ctx.Err()` mid-iteration | not checked | not checked | checked per receipt, `uniswapv3indexer/service.go:209` |
| `archiving` ctx stamp | version **and** number, `service.go:301-302` | version only, `service.go:302` | neither (no archived reads) |
| block-latency histogram | yes | **none** | yes, `block_processor.go:60` |

`aavelike_position_tracker` is Morpho's copy-paste twin — same two function names
(`processBlockEvent` `:192`, `fetchAndProcessReceipts` `:196`), same
`event.ParsedBlockHash()` call `:218`, same long-parameter style (6 full 5-tuples,
10 `blockHash`, 12 `blockVersion`). That is the evidence that these are propagating
copies, not per-protocol decisions.

**Proposed change.** Generalise `dexconsumer.BlockProcessor` out of the DEX namespace
into `internal/services/blockconsumer` (or `shared`) — it is already
protocol-agnostic — with `BlockHandler` as the one seam, and migrate the four
receipt-reading services onto it. `dextelemetry`'s `blocks.processed` /
`errors.total` / `block.duration_seconds` become the common core (see F01.8), leaving
each service only its domain instruments. `dexbootstrap` generalises the same way for
F01.12.

**Benefits.** Deletes 4 copies of cache-read + unmarshal + telemetry (~40 lines each),
fixes the wrong error message and the missing error metric in one place, and makes the
tx-boundary and partial-failure policies (F01.4, and rows 6-7 above) *one* decision
instead of eight. Leverage: a new indexer supplies a `BlockHandler` and nothing else.
Locality: "what happens between the SQS message and the handler" stops being eight
answers.

**Risk / migration.** The runner currently hands the handler the whole `[]receipts`,
whereas Morpho iterates receipts itself with a per-receipt span — that is compatible
(the handler owns the loop), but Morpho's per-receipt error accumulation must be
resolved first (F01.4) or it changes behaviour. Migrate one service per PR, easiest
first (`allocation_tracker`), Morpho last. `dexconsumer`'s doc comments assume DEX
vocabulary and need rewording as part of the move.

**Size**: L (XL if all 8 handlers migrate). **Depends on**: F01.4 (partial-failure
policy). **Enables**: F01.8, F01.12.

---

### F01.15 — `morpho.receipt.duration_seconds` is declared and never recorded

**Strength**: Strong
**Files**: `morpho_indexer/telemetry.go:74, 184-192`

**Problem.** The histogram has exactly 4 references in the entire repo, all of them its
own declaration:

```
telemetry.go:74:	receiptDuration metric.Float64Histogram
telemetry.go:184:	t.receiptDuration, err = meter.Float64Histogram(
telemetry.go:185:		"morpho.receipt.duration_seconds",
telemetry.go:191:		return nil, fmt.Errorf("creating receiptDuration histogram: %w", err)
```

There is no `RecordReceipt*` method and no call site. `processReceipt`
(`service.go:477`) times nothing. So a dashboard or alert keyed on it sees an
instrument that exists and never fires — the worst kind of metric.

**Proposed change.** Either record it (a `defer` in `processReceipt`, mirroring
`RecordBlockProcessed` at `service.go:375`) or delete the field, the registration and
the name. Check `alerts/` and `docs/runbooks/` for references before deleting.

**Benefits.** Removes a metric that looks healthy and is not. Small, but it is the kind
of thing 11 hand-declared instruments (F01.8) make easy to miss and a spec-driven
registration would catch.

**Risk / migration.** None if `alerts/` does not reference it.

**Size**: S. **Depends on**: nothing. Fold into F01.8.

## 4. Cross-area observations

- `internal/services/shared/telemetry.go` (192 L, `ServiceTelemetry`) has exactly one
  caller, `cmd/base/watcher/main.go:411` — a hypothetical seam in a package named for
  sharing.
- Eight services hand-roll a `telemetry.go` (1,547 L total) while
  `internal/pkg/telemetry/metrics.go` offers `RecordBlockProcessed` /
  `RecordProcessingLatency` that most of them re-declare.
- `.golangci.yml` enables no complexity linter, so the `funlen`/`gocognit` backstop
  `stl-verify/AGENTS.md:196` calls "planned" is not in CI; 180-, 137- and 108-line
  functions pass today.
- `entity.BlockPosition` (`internal/domain/entity/morpho_adapter.go:101`) is a general
  chain concept living in a Morpho-specific entity file.
- `shared.RunSnapshotReads`, `shared.DecodeLog`, `shared.UnpackUint` and
  `shared.OptionalUintResult` are used only by `curveindexer`, `uniswapv3indexer` and
  `dexconsumer`. The DEX indexers and the lending indexers have diverged into two
  dialects of the same job; picking one is probably the single biggest repo-wide win.
- The `blockHash`-threading commit `c92be237` (VEC-471, #520) changed **59 signatures
  across 74 files**. Every indexer carries the same 5-tuple by hand, so `BlockRef`
  (F01.3) is a repo-wide finding, not a Morpho one.
- Cross-cutting policy commits had to be applied per protocol:
  `5c8566bd` (SQS shutdown) touched 92 files, `ac662cd3` (data audibility) 96,
  `f39aeaf8` (Temporal history jobs) 84. `internal/services/{allocation_tracker,
  aavelike_position_tracker,oracle_price_worker,prime_debt}/service.go` co-change with
  `morpho_indexer/service.go` at nearly the same rate — the god-file shape is replicated
  per protocol.
- `internal/pkg/retry` has no "bisect the request on a provider result cap" primitive,
  and three places want one (`morpho-vault-backfill/prober.go:199`,
  `morpho_v2_bootstrap/sweep.go:137`, and the sweep's HTTP-retry note at
  `sweep.go:87-89`).
- No shared worker-pool helper exists in `internal/` or `pkg/`; the prior art is bare
  `errgroup.SetLimit` in five binaries, and two hand-rolled channel pools in
  `morpho-vault-backfill/discovery.go`.
- **`fluid_vault_indexer` reads state number-pinned, not hash-pinned**:
  `fluid_vault_indexer/blockchain_service.go:266` calls
  `Execute(ctx, calls, big.NewInt(blockNumber))` and the package never parses
  `event.BlockHash` at all. That is the reorg-correctness hole VEC-471 (#520) closed
  everywhere else — its snapshot can be answered from a post-reorg fork. Belongs to
  whoever owns Fluid; flagged here because it is the same bug class as F01.3 and matches
  the "curve/fluid reorg gap open on main" note in the pinned #520 diagnosis.
- `uniswapv3indexer/service.go:136-141` re-implements *half* of
  `outbound.BlockEvent.ParsedBlockHash()`: it rejects the empty hash but then calls
  `common.HexToHash`, so a **malformed** hash still zero-pads into "a plausible-looking
  hash that pins the read to a block that does not exist" — the second failure mode
  `ParsedBlockHash`'s own doc comment lists (`internal/ports/outbound/eventsink.go:85-93`,
  which calls it "the single guard that keeps VEC-471's reorg-correctness honest across
  every indexer"). Morpho uses the port helper (`service.go:393`); uniswapv3 should too.
- `internal/pkg/telemetry/metrics.go:57 RecordProcessingLatency` exists on the recorder
  `fluid_vault_indexer` holds and is never called there, so Fluid has no block-latency
  signal.
- `aavelike_position_tracker` is a near-copy of `morpho_indexer`'s block skeleton (same
  function names, same `ParsedBlockHash` call at `:218`, same 5-tuple threading). Fixing
  F01.3/F01.4/F01.14 in Morpho without fixing it there just moves the copy.
- `fluid_vault_indexer/service.go:187-203`: `Start`'s doc comment is split in two by an
  inserted `consumeLoop` method — the first half ends "then runs the SQS" and the second
  half begins "// processing loop until ctx is cancelled." Cosmetic, but a real edit
  artefact.

## 5. Open questions

- Who consumes `protocol_event.event_data`? F01.5 changes its shape for MetaMorpho rows,
  and whether that needs a new `processing_version` or is free depends on downstream
  readers I cannot see from Go alone.
- Is `morpho_vault.created_at_block`'s `LEAST(...)` converge (F01.11) a sanctioned
  exception with a ticket, or an unrecorded one? VEC-353 touched this area but the
  policy file lists no exception.
- Does anything read the `morpho_adapter_state` time series that F01.10's bootstrap
  deliberately does not reconstruct (`replay.go:203-216` says "a time series nothing
  consumes today")? If nothing does, the allocation-snapshot path may be over-built.
- What is the actual transaction/commit budget per block in production? F01.4's
  "~2N commits per block" claim is from the code; whether it is a measured problem needs
  the `morpho_blocks_processed` / DB metrics.
- `hasRelevantEvents` exists to avoid empty spans (`service.go:421-424`). Is that still
  a real cost with the current sampling config, or can the whole pre-gate go away
  (simplifying F01.9)?
